import { describe, expect, test } from "bun:test"
import { Database as BunDatabase } from "bun:sqlite"
import { stat, unlink } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { Database } from "@opencode-ai/core/database/database"
import { ProjectDirectoryTable, ProjectTable } from "@opencode-ai/core/project/sql"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { Project } from "@/project/project"
import { ProjectV2 } from "@opencode-ai/core/project"
import { SessionID } from "@/session/schema"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Cause, Effect } from "effect"
import { and, eq } from "drizzle-orm"
import { tmpdirScoped } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"

describe("isProjectRowFresh", () => {
  const base = { worktree: "/w", vcs: "git" as const, sandboxes: ["/w"] }

  test("identical rows are fresh", () => {
    expect(Project.isProjectRowFresh(base, { ...base })).toBe(true)
  })

  test("worktree drift is stale", () => {
    expect(Project.isProjectRowFresh(base, { ...base, worktree: "/other" })).toBe(false)
  })

  test("vcs drift is stale", () => {
    expect(Project.isProjectRowFresh(base, { ...base, vcs: undefined })).toBe(false)
    expect(Project.isProjectRowFresh({ ...base, vcs: undefined }, { ...base, vcs: undefined })).toBe(true)
  })

  test("added or removed sandboxes are stale", () => {
    expect(Project.isProjectRowFresh(base, { ...base, sandboxes: ["/w", "/w2"] })).toBe(false)
    expect(Project.isProjectRowFresh({ ...base, sandboxes: ["/w", "/w2"] }, base)).toBe(false)
  })

  test("sandbox order does not matter", () => {
    expect(Project.isProjectRowFresh({ ...base, sandboxes: ["/a", "/b"] }, { ...base, sandboxes: ["/b", "/a"] })).toBe(
      true,
    )
  })
})

describe("isSqliteLockCause", () => {
  test("matches lock failures and defects", () => {
    expect(Project.isSqliteLockCause(Cause.fail(new Error("database is locked")))).toBe(true)
    expect(Project.isSqliteLockCause(Cause.die(new Error("SQLITE_BUSY: database is locked")))).toBe(true)
    expect(Project.isSqliteLockCause(Cause.fail(new Error("LockTimeoutError: statement timed out")))).toBe(true)
  })

  test("rejects anything else", () => {
    expect(Project.isSqliteLockCause(Cause.fail(new Error("no such table: foo")))).toBe(false)
    expect(Project.isSqliteLockCause(Cause.empty)).toBe(false)
  })
})

describe("withSqliteLockRetry", () => {
  test("retries lock failures every 5s until success", async () => {
    let attempts = 0
    const start = Date.now()
    const result = await Effect.runPromise(
      Project.withSqliteLockRetry(
        "test",
        Effect.sync(() => {
          attempts += 1
          if (attempts < 3) throw new Error("database is locked")
          return "ok"
        }),
      ),
    )
    expect(result).toBe("ok")
    expect(attempts).toBe(3)
    // Two 5s backoffs between three attempts.
    expect(Date.now() - start).toBeGreaterThan(9_000)
  }, 60_000)

  test("non-lock failures pass through immediately", async () => {
    const start = Date.now()
    const failure = await Effect.runPromise(
      Project.withSqliteLockRetry(
        "test",
        Effect.fail(new Error("no such table: foo")),
      ).pipe(Effect.flip),
    )
    expect(String(failure)).toMatch(/no such table/)
    expect(Date.now() - start).toBeLessThan(3_000)
  }, 30_000)
})

describe("sqlite lock mechanics", () => {
  test("RESERVED blocks writers but not readers (the fast-path invariant)", async () => {
    const file = join(tmpdir(), `opencode-bootlock-mech-${process.pid}.db`)
    await unlink(file).catch(() => {})
    try {
      const setup = new BunDatabase(file)
      setup.exec("CREATE TABLE t (id TEXT PRIMARY KEY, v INTEGER)")
      setup.exec("INSERT INTO t VALUES ('x', 1)")
      setup.exec("PRAGMA journal_mode = WAL")
      setup.close()

      const holder = new BunDatabase(file)
      holder.exec("BEGIN IMMEDIATE")
      try {
        // Reads proceed under a held write lock.
        const reader = new BunDatabase(file, { readonly: true })
        try {
          expect(reader.query("SELECT v AS v FROM t WHERE id = 'x'").get()).toEqual({ v: 1 })
        } finally {
          reader.close()
        }
        // Writes block, then fail loud (never silently).
        const writer = new BunDatabase(file)
        try {
          writer.exec("PRAGMA busy_timeout = 500")
          expect(() => writer.exec("UPDATE t SET v = 2 WHERE id = 'x'")).toThrow(/database is locked/)
        } finally {
          writer.close()
        }
      } finally {
        holder.exec("ROLLBACK")
        holder.close()
      }
    } finally {
      for (const suffix of ["", "-wal", "-shm", "-journal"]) {
        await unlink(file + suffix).catch(() => {})
      }
    }
  })
})

const it = testEffect(AppNodeBuilder.build(LayerNode.group([Project.node, Database.node, CrossSpawnSpawner.node])))

function directories(projectID: ProjectV2.ID) {
  return Database.Service.use(({ db }) =>
    db
      .select()
      .from(ProjectDirectoryTable)
      .where(eq(ProjectDirectoryTable.project_id, projectID))
      .all()
      .pipe(
        Effect.orDie,
        Effect.map((rows) =>
          rows
            .map((row) => ({ directory: row.directory, strategy: row.strategy ?? undefined }))
            .toSorted((a, b) => a.directory.localeCompare(b.directory)),
        ),
      ),
  )
}

describe("Project.fromDirectory read-only boot", () => {
  it.live("repeat boots for a known directory perform zero writes", () =>
    Effect.gen(function* () {
      const project = yield* Project.Service
      const tmp = yield* tmpdirScoped({ git: true })

      const first = yield* project.fromDirectory(tmp)
      const commitFile = join(tmp, ".git", "opencode")
      const snapshot = yield* Effect.gen(function* () {
        const info = yield* project.get(first.project.id)
        return {
          info: JSON.stringify(info),
          dirs: JSON.stringify(yield* directories(first.project.id)),
          commitMtime: yield* Effect.promise(() =>
            stat(commitFile)
              .then((s) => s.mtimeMs)
              .catch(() => -1),
          ),
        }
      })

      // Fast path: no sync write, and the background heal must no-op (row,
      // mapping and backfill are all fresh), so the grace window below would
      // catch any stray write deterministically.
      yield* project.fromDirectory(tmp)
      yield* Effect.sleep("3 seconds")

      const after = yield* Effect.gen(function* () {
        const info = yield* project.get(first.project.id)
        return {
          info: JSON.stringify(info),
          dirs: JSON.stringify(yield* directories(first.project.id)),
          commitMtime: yield* Effect.promise(() =>
            stat(commitFile)
              .then((s) => s.mtimeMs)
              .catch(() => -1),
          ),
        }
      })
      expect(after).toEqual(snapshot)
    }),
  )

  it.live("a missing row falls back to the write path and re-inserts", () =>
    Effect.gen(function* () {
      const project = yield* Project.Service
      const { db } = yield* Database.Service
      const tmp = yield* tmpdirScoped({ git: true })

      const first = yield* project.fromDirectory(tmp)
      yield* db.delete(ProjectTable).where(eq(ProjectTable.id, first.project.id)).run().pipe(Effect.orDie)

      const second = yield* project.fromDirectory(tmp)
      expect(second.project.id).toBe(first.project.id)
      const row = yield* db.select().from(ProjectTable).where(eq(ProjectTable.id, first.project.id)).get().pipe(Effect.orDie)
      expect(row).toBeDefined()
      expect(second.project.worktree).toBe(tmp)
    }),
  )

  it.live("the deferred heal restores a missing mapping and backfills sessions", () =>
    Effect.gen(function* () {
      const project = yield* Project.Service
      const { db } = yield* Database.Service
      // Plain directory first: the global project row must exist, or the
      // legacy session below violates its project_id foreign key.
      yield* project.fromDirectory(yield* tmpdirScoped())
      const tmp = yield* tmpdirScoped({ git: true })
      const first = yield* project.fromDirectory(tmp)

      const sessionID = SessionID.make("ses_heal_1")
      const now = Date.now()
      yield* db
        .insert(SessionTable)
        .values({
          id: sessionID,
          project_id: ProjectV2.ID.global,
          slug: sessionID,
          directory: tmp,
          title: "test",
          version: "0.0.0-test",
          time_created: now,
          time_updated: now,
        })
        .run()
        .pipe(Effect.orDie)
      yield* db.delete(ProjectDirectoryTable).where(eq(ProjectDirectoryTable.project_id, first.project.id)).run().pipe(Effect.orDie)

      // Row itself is fresh, so this fast-paths; the heal must still persist
      // the missing mapping and backfill the session in the background.
      yield* project.fromDirectory(tmp)
      const deadline = Date.now() + 15_000
      let healed = false
      while (!healed && Date.now() < deadline) {
        const dirs = yield* directories(first.project.id)
        const session = yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).get().pipe(Effect.orDie)
        if (dirs.length > 0 && session?.project_id === first.project.id) healed = true
        else yield* Effect.sleep("500 millis")
      }
      expect(healed).toBe(true)
    }),
  )
})
