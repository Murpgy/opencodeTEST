import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { and, eq, sql } from "drizzle-orm"
import { Database } from "@opencode-ai/core/database/database"
import { ProjectDirectoryTable, ProjectTable } from "@opencode-ai/core/project/sql"
import { ProjectDirectories } from "@opencode-ai/core/project/directories"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { WorkspaceTable } from "@opencode-ai/core/control-plane/workspace.sql"
import { Flag } from "@opencode-ai/core/flag/flag"
import { GlobalBus } from "@/bus/global"
import { which } from "@opencode-ai/core/util/which"
import { Command } from "@/command"
import { InstanceState } from "@/effect/instance-state"
import { Effect, Cause, Exit, Layer, Scope, Context, Stream, Types, Schema } from "effect"
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { AppProcess } from "@opencode-ai/core/process"
import { ProjectV2 } from "@opencode-ai/core/project"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { serviceUse } from "@opencode-ai/core/effect/service-use"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { EventV2Bridge } from "@/event-v2-bridge"
import { EventV2 } from "@opencode-ai/core/event"
import { Project } from "@opencode-ai/schema/project"

export const Info = Project.Info
export type Info = Types.DeepMutable<Schema.Schema.Type<typeof Info>>

export const Event = {
  Updated: Project.Event.Updated,
}

// True when the stored project row already carries everything fromDirectory
// would persist (modulo time_updated, which nothing reads). Repeat boots for
// a known directory then skip the write transaction entirely, so a new
// terminal starts even while another process holds the database write lock
// (reads never block in WAL mode). Comparison errs toward writing: an
// uncertain match takes the slow path, which is always safe.
export const isProjectRowFresh = (
  existing: Pick<Info, "worktree" | "vcs" | "sandboxes">,
  computed: Pick<Info, "worktree" | "vcs" | "sandboxes">,
): boolean => {
  if (existing.worktree !== computed.worktree) return false
  if (JSON.stringify(existing.vcs ?? null) !== JSON.stringify(computed.vcs ?? null)) return false
  if (existing.sandboxes.length !== computed.sandboxes.length) return false
  const seen = new Set(existing.sandboxes)
  return computed.sandboxes.every((sandbox) => seen.has(sandbox))
}

// SQLite surfaces lock contention as "database is locked" (SQLITE_BUSY)
// wrapped through the drizzle layers; busy_timeout bounds each wait, so a
// failure here means someone holds a write transaction long past that.
export const isSqliteLockCause = (cause: Cause.Cause<unknown>): boolean =>
  /database is locked|SQLITE_BUSY|LockTimeout/i.test(Cause.pretty(cause))

// Boot writes vs a contended database: another process (archive, migration)
// may hold a write transaction for minutes on a large database, far past the
// 5s busy_timeout. A lock timeout here is transient by nature — SQLite
// releases locks when the holder exits — so wait it out instead of failing
// boot. Unbounded by design (a slow holder is indistinguishable from a wedged
// one, and dying helps nobody); every attempt is logged so the wait stays
// observable, and the sleep is interruptible so Ctrl+C still quits instantly.
// Non-lock failures pass through untouched (still die, as before).
export const withSqliteLockRetry = <A, E>(label: string, effect: Effect.Effect<A, E>): Effect.Effect<A, E> =>
  Effect.gen(function* () {
    let attempt = 0
    for (;;) {
      const exit: Exit.Exit<A, E> = yield* Effect.exit(effect)
      if (Exit.isFailure(exit) && !isSqliteLockCause(exit.cause)) return yield* Effect.failCause(exit.cause)
      if (Exit.isSuccess(exit)) return exit.value
      attempt += 1
      yield* Effect.logWarning(`${label}: database is locked by another process, retrying in 5s`, { attempt })
      yield* Effect.sleep("5 seconds")
    }
  })

type Row = typeof ProjectTable.$inferSelect

export function fromRow(row: Row): Info {
  const icon =
    row.icon_url || row.icon_url_override || row.icon_color
      ? {
          url: row.icon_url ?? undefined,
          override: row.icon_url_override ?? undefined,
          color: row.icon_color ?? undefined,
        }
      : undefined
  return {
    id: row.id,
    worktree: row.worktree,
    vcs: row.vcs ? Schema.decodeUnknownSync(Project.Vcs)(row.vcs) : undefined,
    name: row.name ?? undefined,
    icon,
    time: {
      created: row.time_created,
      updated: row.time_updated,
      initialized: row.time_initialized ?? undefined,
    },
    sandboxes: row.sandboxes,
    commands: row.commands ?? undefined,
  }
}

export const UpdateInput = Schema.Struct({
  projectID: ProjectV2.ID,
  name: Schema.optional(Schema.String),
  icon: Schema.optional(Project.Icon),
  commands: Schema.optional(Project.Commands),
})
export type UpdateInput = Types.DeepMutable<Schema.Schema.Type<typeof UpdateInput>>

export const UpdatePayload = Schema.Struct({
  name: Schema.optional(Schema.String),
  icon: Schema.optional(Project.Icon),
  commands: Schema.optional(Project.Commands),
}).annotate({ identifier: "ProjectUpdateInput" })
export type UpdatePayload = Types.DeepMutable<Schema.Schema.Type<typeof UpdatePayload>>

export class NotFoundError extends Schema.TaggedErrorClass<NotFoundError>()("Project.NotFoundError", {
  projectID: ProjectV2.ID,
}) {}

// ---------------------------------------------------------------------------
// Effect service
// ---------------------------------------------------------------------------

export interface Interface {
  /**
   * Per-instance setup. Subscribes to the `/init` slash command for the
   * current instance and stamps the project's initialized timestamp when it
   * fires. Subscription lifetime is tied to the per-instance state scope.
   */
  readonly init: () => Effect.Effect<void>
  readonly fromDirectory: (directory: string) => Effect.Effect<{ project: Info; sandbox: string }>
  readonly discover: (input: Info) => Effect.Effect<void>
  readonly list: () => Effect.Effect<Info[]>
  readonly get: (id: ProjectV2.ID) => Effect.Effect<Info | undefined>
  readonly update: (input: UpdateInput) => Effect.Effect<Info, NotFoundError>
  readonly initGit: (input: { directory: string; project: Info }) => Effect.Effect<Info>
  readonly setInitialized: (id: ProjectV2.ID) => Effect.Effect<void>
  readonly sandboxes: (id: ProjectV2.ID) => Effect.Effect<string[]>
  readonly addSandbox: (id: ProjectV2.ID, directory: string) => Effect.Effect<void>
  readonly removeSandbox: (id: ProjectV2.ID, directory: string) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Project") {}

type GitResult = { code: number; text: string; stderr: string }

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const fs = yield* FSUtil.Service
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
    const projectV2 = yield* ProjectV2.Service
    const projectDirectories = yield* ProjectDirectories.Service
    const events = yield* EventV2Bridge.Service
    const flags = yield* RuntimeFlags.Service
    const { db } = yield* Database.Service

    const git = Effect.fnUntraced(
      function* (args: string[], opts?: { cwd?: string }) {
        const handle = yield* spawner.spawn(
          ChildProcess.make("git", args, { cwd: opts?.cwd, extendEnv: true, stdin: "ignore" }),
        )
        const [text, stderr] = yield* Effect.all(
          [Stream.mkString(Stream.decodeText(handle.stdout)), Stream.mkString(Stream.decodeText(handle.stderr))],
          { concurrency: 2 },
        )
        const code = yield* handle.exitCode
        return { code, text, stderr } satisfies GitResult
      },
      Effect.scoped,
      Effect.catch(() => Effect.succeed({ code: 1, text: "", stderr: "" } satisfies GitResult)),
    )

    const emitUpdated = (data: Info) =>
      Effect.sync(() =>
        GlobalBus.emit("event", {
          directory: "global",
          project: data.id,
          payload: { type: Event.Updated.type, properties: data },
        }),
      )

    const fakeVcs = Schema.decodeUnknownSync(Schema.optional(Project.Vcs))(Flag.OPENCODE_FAKE_VCS)

    const scope = yield* Scope.Scope

    const migrateProjectId = Effect.fn("Project.migrateProjectId")(function* (
      oldID: ProjectV2.ID | undefined,
      newID: ProjectV2.ID,
    ) {
      if (!oldID) return
      if (oldID === ProjectV2.ID.global) return
      if (oldID === newID) return

      yield* db
        .transaction(
          (d) =>
            Effect.gen(function* () {
              const oldProject = yield* d.select().from(ProjectTable).where(eq(ProjectTable.id, oldID)).get()
              const newProject = yield* d.select().from(ProjectTable).where(eq(ProjectTable.id, newID)).get()
              if (oldProject && !newProject) {
                yield* d
                  .insert(ProjectTable)
                  .values({
                    ...oldProject,
                    id: newID,
                    time_updated: Date.now(),
                  })
                  .run()
              }

              // Project directories may be shared across distinct
              // checkouts which have diverged. Clear the directory
              // list and rely on it being re-populated to ensure
              // accuracy
              yield* d.delete(ProjectDirectoryTable).where(eq(ProjectDirectoryTable.project_id, oldID)).run()

              yield* d
                .update(SessionTable)
                .set({ project_id: newID, time_updated: sql`${SessionTable.time_updated}` })
                .where(eq(SessionTable.project_id, oldID))
                .run()
              yield* d
                .update(WorkspaceTable)
                .set({ project_id: newID })
                .where(eq(WorkspaceTable.project_id, oldID))
                .run()

              if (oldProject) yield* d.delete(ProjectTable).where(eq(ProjectTable.id, oldID)).run()
            }),
          { behavior: "immediate" },
        )
        .pipe(Effect.orDie)
    })

    const saveProjectDirectory = Effect.fn("Project.saveProjectDirectory")(function* (input: {
      projectID: ProjectV2.ID
      directory: string
    }) {
      if (input.projectID === ProjectV2.ID.global) return
      const opened = AbsolutePath.make(FSUtil.resolve(input.directory))
      yield* projectDirectories
        .create({
          directory: opened,
          projectID: input.projectID,
        })
        .pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("project directory persistence failed", { projectID: input.projectID, cause }),
          ),
        )
    })

    // Directories with a heal already in flight skip queueing another — every
    // boot under contention would otherwise stack a fiber.
    const healing = new Set<string>()

    // Deferred half of the read-only boot: re-check under current state and
    // persist only what is actually missing. Reads never block, so this stays
    // lock-safe; in the common case (concurrent boots, nothing changed) it
    // performs zero writes. Returns whether anything was persisted.
    const healProject = Effect.fn("Project.healProject")(function* (input: {
      result: Info
      projectID: ProjectV2.ID
      directory: string
    }) {
      const row = yield* db.select().from(ProjectTable).where(eq(ProjectTable.id, input.projectID)).get().pipe(Effect.orDie)
      const rowStale = !row || !isProjectRowFresh(fromRow(row), input.result)
      const backfillStale =
        input.projectID !== ProjectV2.ID.global &&
        ((yield* db
          .select({ one: sql`1` })
          .from(SessionTable)
          .where(and(eq(SessionTable.project_id, ProjectV2.ID.global), eq(SessionTable.directory, input.directory)))
          .get()
          .pipe(Effect.orDie)) !== undefined)
      const mappingStale =
        input.projectID !== ProjectV2.ID.global &&
        !(yield* projectDirectories.contains({
          projectID: input.projectID,
          directory: AbsolutePath.make(FSUtil.resolve(input.directory)),
        }))
      if (!rowStale && !backfillStale && !mappingStale) return false
      yield* withSqliteLockRetry("Project.persistHeal", persistProject({ result: input.result, projectID: input.projectID, directory: input.directory }))
      return true
    })

    const schedulePersistHeal = (projectID: ProjectV2.ID, result: Info, directory: string): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (healing.has(projectID)) return
        healing.add(projectID)
        yield* healProject({ result, projectID, directory }).pipe(
          Effect.tap((wrote) => (wrote ? Effect.logInfo("deferred project write landed", { project: projectID }) : Effect.void)),
          Effect.ensuring(Effect.sync(() => healing.delete(projectID))),
          Effect.forkIn(scope),
        )
      })

    // All boot-time project writes in one idempotent unit: upsert the row,
    // backfill legacy global sessions, persist the directory mapping. Shared
    // by the slow path (awaited before boot continues) and the heal (only
    // when its re-check finds something actually missing).
    const persistProject = Effect.fn("Project.persistProject")(function* (input: {
      result: Info
      projectID: ProjectV2.ID
      directory: string
    }) {
      yield* db
        .insert(ProjectTable)
        .values({
          id: input.result.id,
          worktree: AbsolutePath.make(input.result.worktree),
          vcs: input.result.vcs ?? null,
          name: input.result.name,
          icon_url: input.result.icon?.url,
          icon_url_override: input.result.icon?.override,
          icon_color: input.result.icon?.color,
          time_created: input.result.time.created,
          time_updated: input.result.time.updated,
          time_initialized: input.result.time.initialized,
          sandboxes: input.result.sandboxes.map((sandbox) => AbsolutePath.make(sandbox)),
          commands: input.result.commands,
        })
        .onConflictDoUpdate({
          target: ProjectTable.id,
          set: {
            worktree: AbsolutePath.make(input.result.worktree),
            vcs: input.result.vcs ?? null,
            name: input.result.name,
            icon_url: input.result.icon?.url,
            icon_url_override: input.result.icon?.override,
            icon_color: input.result.icon?.color,
            time_updated: input.result.time.updated,
            time_initialized: input.result.time.initialized,
            sandboxes: input.result.sandboxes.map((sandbox) => AbsolutePath.make(sandbox)),
            commands: input.result.commands,
          },
        })
        .run()
        .pipe(Effect.orDie)

      if (input.projectID !== ProjectV2.ID.global) {
        yield* db
          .update(SessionTable)
          .set({ project_id: input.projectID })
          .where(and(eq(SessionTable.project_id, ProjectV2.ID.global), eq(SessionTable.directory, input.directory)))
          .run()
          .pipe(Effect.orDie)
      }

      yield* saveProjectDirectory({
        projectID: input.projectID,
        directory: input.directory,
      })
    })

    const fromDirectory = Effect.fn("Project.fromDirectory")(function* (directory: string) {
      yield* Effect.logInfo("fromDirectory", { directory })

      const data = yield* projectV2.resolve(AbsolutePath.make(directory))
      const worktree = data.id === ProjectV2.ID.make("global") && !data.vcs ? "/" : data.directory

      // Phase 2: upsert. The writes below are lock-sensitive (another process
      // may hold a write transaction); the reads are not (WAL readers never
      // block). Known directories whose row is already fresh skip the writes
      // entirely and boot read-only, healing the cosmetic touch-ups in the
      // background once the lock clears.
      const projectID = ProjectV2.ID.make(data.id)
      yield* withSqliteLockRetry(
        "Project.migrateProjectId",
        migrateProjectId(data.previous ? ProjectV2.ID.make(data.previous) : undefined, projectID),
      )
      const row = yield* db.select().from(ProjectTable).where(eq(ProjectTable.id, projectID)).get().pipe(Effect.orDie)
      const existing = row
        ? fromRow(row)
        : {
            id: projectID,
            worktree,
            vcs: data.vcs?.type ?? fakeVcs,
            sandboxes: [] as string[],
            time: { created: Date.now(), updated: Date.now() },
          }

      if (flags.experimentalIconDiscovery) yield* discover(existing).pipe(Effect.ignore, Effect.forkIn(scope))

      const result: Info = {
        ...existing,
        worktree: projectID === ProjectV2.ID.global ? worktree : existing.worktree,
        vcs: data.vcs?.type ?? fakeVcs,
        time: { ...existing.time, updated: Date.now() },
      }
      // Local copy: pushing into result.sandboxes directly would mutate
      // existing.sandboxes through the spread-shared reference and make the
      // freshness check below compare the array against itself (always fresh).
      const sandboxes = [...existing.sandboxes]
      if (projectID !== ProjectV2.ID.global && data.directory !== result.worktree && !sandboxes.includes(data.directory))
        sandboxes.push(data.directory)
      result.sandboxes = yield* Effect.forEach(
        sandboxes,
        (s) =>
          fs.exists(s).pipe(
            Effect.orDie,
            Effect.map((exists) => (exists ? s : undefined)),
          ),
        { concurrency: "unbounded" },
      ).pipe(Effect.map((arr) => arr.filter((x): x is string => x !== undefined)))

      // Legacy sessions claimed synchronously, as before: callers (and tests)
      // rely on the backfill being visible by the time boot returns. Pending
      // backfill forces the slow path even when the row is otherwise fresh.
      // The check is a read, so it stays lock-safe.
      const backfillPending =
        projectID !== ProjectV2.ID.global &&
        ((yield* db
          .select({ one: sql`1` })
          .from(SessionTable)
          .where(and(eq(SessionTable.project_id, ProjectV2.ID.global), eq(SessionTable.directory, data.directory)))
          .get()
          .pipe(Effect.orDie)) !== undefined)
      // Same for the directory mapping: project id migrations (and torn
      // writes) can leave a fresh row with no mapping, and readers expect it
      // synchronously. The global project never has mappings (saved
      // unconditionally skipped), so it is exempt.
      const mappingPresent =
        projectID === ProjectV2.ID.global ||
        (yield* projectDirectories.contains({
          projectID,
          directory: AbsolutePath.make(FSUtil.resolve(data.directory)),
        }))
      if (row && isProjectRowFresh(existing, result) && !backfillPending && mappingPresent) {
        // Fast path: the stored row already matches and no legacy sessions
        // await claiming. Boot continues with zero writes; a torn directory
        // mapping (only possible after a crashed write) heals in the
        // background. Nothing is emitted or committed because nothing changed.
        // time_updated is deliberately left stale — nothing reads it, and
        // bumping it would turn every boot back into a write.
        yield* schedulePersistHeal(projectID, result, data.directory)
        return { project: result, sandbox: data.vcs ? data.directory : worktree }
      }

      yield* withSqliteLockRetry("Project.persistProject", persistProject({ result, projectID, directory: data.directory }))

      yield* emitUpdated(result)
      if (projectID !== ProjectV2.ID.global && data.vcs?.type === "git") {
        yield* projectV2.commit({ store: data.vcs.store, id: data.id })
      }
      return { project: result, sandbox: data.vcs ? data.directory : worktree }
    })

    const discover = Effect.fn("Project.discover")(function* (input: Info) {
      if (input.vcs !== "git") return
      if (input.icon?.override) return
      if (input.icon?.url) return

      const matches = yield* fs
        .glob("**/favicon.{ico,png,svg,jpg,jpeg,webp}", {
          cwd: input.worktree,
          absolute: true,
          include: "file",
        })
        .pipe(Effect.orDie)
      const shortest = matches.sort((a, b) => a.length - b.length)[0]
      if (!shortest) return

      const buffer = yield* fs.readFile(shortest).pipe(Effect.orDie)
      const base64 = Buffer.from(buffer).toString("base64")
      const mime = FSUtil.mimeType(shortest)
      const url = `data:${mime};base64,${base64}`
      yield* update({ projectID: input.id, icon: { url } }).pipe(
        Effect.catchTag("Project.NotFoundError", () => Effect.void),
      )
    })

    const list = Effect.fn("Project.list")(function* () {
      return (yield* db.select().from(ProjectTable).all().pipe(Effect.orDie)).map(fromRow)
    })

    const get = Effect.fn("Project.get")(function* (id: ProjectV2.ID) {
      const row = yield* db.select().from(ProjectTable).where(eq(ProjectTable.id, id)).get().pipe(Effect.orDie)
      return row ? fromRow(row) : undefined
    })

    const update = Effect.fn("Project.update")(function* (input: UpdateInput) {
      const result = yield* db
        .update(ProjectTable)
        .set({
          name: input.name,
          icon_url: input.icon?.url,
          icon_url_override: input.icon?.override,
          icon_color: input.icon?.color,
          commands: input.commands,
          time_updated: Date.now(),
        })
        .where(eq(ProjectTable.id, input.projectID))
        .returning()
        .get()
        .pipe(Effect.orDie)
      if (!result) return yield* new NotFoundError({ projectID: input.projectID })
      const data = fromRow(result)
      yield* emitUpdated(data)
      return data
    })

    const initGit = Effect.fn("Project.initGit")(function* (input: { directory: string; project: Info }) {
      if (input.project.vcs === "git") return input.project
      if (!(yield* Effect.sync(() => which("git")))) throw new Error("Git is not installed")
      const result = yield* git(["init", "--quiet"], { cwd: input.directory })
      if (result.code !== 0) {
        throw new Error(result.stderr.trim() || result.text.trim() || "Failed to initialize git repository")
      }
      const { project } = yield* fromDirectory(input.directory)
      return project
    })

    const setInitialized = Effect.fn("Project.setInitialized")(function* (id: ProjectV2.ID) {
      yield* db
        .update(ProjectTable)
        .set({ time_initialized: Date.now() })
        .where(eq(ProjectTable.id, id))
        .run()
        .pipe(Effect.orDie)
    })

    const initState = yield* InstanceState.make(
      Effect.fn("Project.initState")(function* (ctx) {
        const unsubscribe = yield* events.listen((event) => {
          if (event.type !== Command.Event.Executed.type || event.location?.directory !== ctx.directory)
            return Effect.void
          const data = event.data as EventV2.Data<typeof Command.Event.Executed>
          return data.name === Command.Default.INIT ? setInitialized(ctx.project.id) : Effect.void
        })
        yield* Effect.addFinalizer(() => unsubscribe)
      }),
    )

    const init = Effect.fn("Project.init")(function* () {
      yield* InstanceState.get(initState)
    })

    const sandboxes = Effect.fn("Project.sandboxes")(function* (id: ProjectV2.ID) {
      const row = yield* db.select().from(ProjectTable).where(eq(ProjectTable.id, id)).get().pipe(Effect.orDie)
      if (!row) return []
      const data = fromRow(row)
      return yield* Effect.forEach(
        data.sandboxes,
        (dir) =>
          fs.isDir(dir).pipe(
            Effect.orDie,
            Effect.map((ok) => (ok ? dir : undefined)),
          ),
        { concurrency: "unbounded" },
      ).pipe(Effect.map((arr) => arr.filter((x): x is string => x !== undefined)))
    })

    const addSandbox = Effect.fn("Project.addSandbox")(function* (id: ProjectV2.ID, directory: string) {
      const row = yield* db.select().from(ProjectTable).where(eq(ProjectTable.id, id)).get().pipe(Effect.orDie)
      if (!row) throw new Error(`Project not found: ${id}`)
      const sandbox = AbsolutePath.make(directory)
      const sboxes = [...row.sandboxes]
      if (!sboxes.includes(sandbox)) sboxes.push(sandbox)
      const result = yield* db
        .update(ProjectTable)
        .set({ sandboxes: sboxes, time_updated: Date.now() })
        .where(eq(ProjectTable.id, id))
        .returning()
        .get()
        .pipe(Effect.orDie)
      if (!result) throw new Error(`Project not found: ${id}`)
      yield* emitUpdated(fromRow(result))
    })

    const removeSandbox = Effect.fn("Project.removeSandbox")(function* (id: ProjectV2.ID, directory: string) {
      const row = yield* db.select().from(ProjectTable).where(eq(ProjectTable.id, id)).get().pipe(Effect.orDie)
      if (!row) throw new Error(`Project not found: ${id}`)
      const sandbox = AbsolutePath.make(directory)
      const sboxes = row.sandboxes.filter((s) => s !== sandbox)
      const result = yield* db
        .update(ProjectTable)
        .set({ sandboxes: sboxes, time_updated: Date.now() })
        .where(eq(ProjectTable.id, id))
        .returning()
        .get()
        .pipe(Effect.orDie)
      if (!result) throw new Error(`Project not found: ${id}`)
      yield* emitUpdated(fromRow(result))
    })

    return Service.of({
      init,
      fromDirectory,
      discover,
      list,
      get,
      update,
      initGit,
      setInitialized,
      sandboxes,
      addSandbox,
      removeSandbox,
    })
  }),
)

export const use = serviceUse(Service)

export const node = LayerNode.make({
  service: Service,
  layer: layer,
  deps: [
    FSUtil.node,
    AppProcess.node,
    CrossSpawnSpawner.node,
    ProjectV2.node,
    ProjectDirectories.node,
    EventV2Bridge.node,
    RuntimeFlags.node,
    Database.node,
  ],
})

export * as Project from "./project"
