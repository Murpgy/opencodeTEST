// v2 cold-storage commands: file-level converters between the live v1 layout
// and packed v2 archives. The v1 original is never opened writable here:
// live sources are snapshotted with VACUUM INTO (WAL-safe), offline files are
// byte-copied after refusing -wal/-shm sidecars, and publish is atomic behind
// a lockfile with a self-verify gate (0 byte-diffs or nothing ships).
import type { Argv } from "yargs"
import { Database } from "@opencode-ai/core/database/database"
import { SessionCold } from "@/session/cold"
import { SessionColdV2 } from "@/session/cold-v2"
import { SessionColdV2Progress } from "@/session/cold-v2-progress"
import type { SessionID } from "@/session/schema"
import { Effect } from "effect"
import { access } from "node:fs/promises"
import { join, dirname, resolve } from "node:path"
import { effectCmd, CliError, fail } from "../effect-cmd"

const toCliError = (cause: unknown): CliError => {
  const message = cause instanceof Error ? cause.message : String(cause)
  if (/lock held/.test(message)) return new CliError({ message, exitCode: 3 })
  if (cause instanceof SessionColdV2.ColdV2Error) return new CliError({ message, exitCode: 2 })
  return new CliError({ message: `unexpected failure: ${message.slice(0, 300)}`, exitCode: 1 })
}

const livePath = (): string => Database.path()

const originPath = (): string => Database.basePath()

const archivePath = (src: string): string => join(dirname(src), "opencode-cold-v2.db")

interface SessionRow {
  readonly id: string
  readonly parent_id: string | null
  readonly title: string
  readonly time_archived: number | null
  readonly time_updated: number
}

// Default selection mirrors the v1 policy (archived + idle) minus the
// anti-stranding rule, which only matters when deleting from live -- pack
// never deletes, it only copies sessions into the archive.
const defaultSelection = async (src: string, idleMinutes: number, active: Set<string>): Promise<string[]> => {
  const db = await SessionColdV2.openRawDb(src, "ro")
  try {
    const rows = db.all<SessionRow>(
      `SELECT id, parent_id, title, time_archived, time_updated FROM session ORDER BY time_updated DESC`,
    )
    const now = Date.now()
    const policy = { ...SessionCold.defaultPolicy(now), idleMs: idleMinutes * 60 * 1000 }
    return rows
      .filter((row) => {
        // Raw SQL rows carry plain strings; the brand is asserted at this
        // boundary (invalid ids fail downstream via missing-session/verify).
        const meta = {
          id: row.id as SessionID,
          parentID: (row.parent_id ?? undefined) as SessionID | undefined,
          title: row.title,
          timeArchived: row.time_archived ?? undefined,
          timeUpdated: row.time_updated,
        }
        return SessionCold.isColdCandidate(meta, policy) && SessionCold.isIdle(meta, policy, active)
      })
      .map((row) => row.id)
  } finally {
    db.close()
  }
}

const activeFrom = (active?: string[]): Set<string> => new Set((active ?? []).flatMap((value) => value.split(",")))

const exists = (path: string): Promise<boolean> => access(path).then(() => true, () => false)

// Progress tracker for a command: --no-progress silences the live bar
// (timings are still tracked for the result panel).
const commandProgress = (progress: boolean): SessionColdV2Progress.ProgressHandle =>
  SessionColdV2Progress.createProgress(SessionColdV2Progress.makeProgressSink(progress))

const formatSecs = (ms: number): string => `${(ms / 1000).toFixed(1)}s`

const timingRows = (phaseMs: Record<string, number>): (readonly [string, string])[] =>
  Object.entries(phaseMs).map(([phase, ms]) => [`  ${phase}`, formatSecs(ms)] as const)

export const DbColdV2PackCommand = effectCmd({
  command: "pack",
  describe: "convert a database file into a packed v2 archive (sources stay read-only; first run moves live traffic to opencode-live-v2.db)",
  instance: false,
  builder: (yargs: Argv) => {
    return yargs
      .option("src", { type: "string", describe: "v1 database file (default: live database)" })
      .option("dst", { type: "string", describe: "v2 archive to create (default: opencode-cold-v2.db next to src)" })
      .option("session", {
        type: "string",
        array: true,
        describe: "Session id(s) to archive, repeatable (default: archived+idle sessions)",
      })
      .option("all", { type: "boolean", default: false, describe: "Archive every session" })
      .option("min-bytes", { type: "number", default: 2048, describe: "Pack rows at or above this JSON size" })
      .option("idle-minutes", { type: "number", default: 30, describe: "Default selection skips sessions updated within this window" })
      .option("active", {
        type: "string",
        array: true,
        describe: "Session id(s) to skip, repeatable or comma-separated",
      })
      .option("verify", { type: "boolean", default: true, describe: "Self-verify (restore + byte-compare) before publish" })
      .option("force", { type: "boolean", default: false, describe: "Replace an existing dst archive" })
      .option("jobs", {
        type: "number",
        default: 0,
        describe: "Parallel pack workers (0=auto: match the system, 1=synchronous)",
      })
      .option("progress", { type: "boolean", default: true, describe: "Live progress bar (use --no-progress for plain logs)" })
      .option("wait", {
        type: "boolean",
        describe: "Pause on the result screen (default: only when interactive; --no-wait never pauses)",
      })
  },
  handler: Effect.fn("Cli.db.cold-v2.pack")(function* (args: {
    src?: string
    dst?: string
    session?: string[]
    all: boolean
    "min-bytes": number
    "idle-minutes": number
    active?: string[]
    verify: boolean
    force: boolean
    jobs: number
    progress: boolean
    wait?: boolean
  }) {
    const src = resolve(args.src ?? livePath())
    const dst = resolve(args.dst ?? archivePath(src))
    if (src === dst) return yield* fail("src and dst must differ")
    if ((yield* Effect.promise(() => exists(dst))) && !args.force) {
      return yield* fail(`dst exists (use --force to replace): ${dst}`)
    }
    const tracker = commandProgress(args.progress)
    // Migrated main-archive packs merge the slim live working set into a
    // restored full image first (packing slim directly would drop every
    // stub's payloads). Selection flags are ignored there: the main archive
    // is always full. Custom-dst packs keep the direct path.
    const mainArchive = resolve(archivePath(livePath()))
    const isMainArchivePack = resolve(dst) === mainArchive && resolve(src) === resolve(livePath())
    const liveExists = yield* Effect.promise(() => exists(resolve(livePath())))
    const useMerge = isMainArchivePack && liveExists && resolve(livePath()) !== resolve(originPath())
    if (useMerge && (args.session || !args.all)) {
      console.log("note: migrated main-archive pack is always full (merge); ignoring --session/--idle-minutes selection")
    }
    const done = yield* Effect.tryPromise({
      try: async () => {
        if (useMerge) {
          return SessionColdV2.packLiveToArchive({
            live: resolve(livePath()),
            archive: resolve(dst),
            minBytes: args["min-bytes"],
            verify: args.verify,
            jobs: args.jobs,
            progress: tracker,
          })
        }
        const active = activeFrom(args.active)
        const allow = args.all ? null : (args.session ?? (await defaultSelection(src, args["idle-minutes"], active)))
        const isLive = resolve(livePath()) === src
        return SessionColdV2.packArchiveFlow({
          src,
          dst,
          allow,
          minBytes: args["min-bytes"],
          verify: args.verify,
          treatAsLive: isLive,
          jobs: args.jobs,
          progress: tracker,
        })
      },
      catch: (cause) => toCliError(cause),
    })
    console.log(
      SessionColdV2Progress.formatResultPanel(`pack done: ${dst}`, [
        ["sessions", String(done.sessions)],
        ["parts", `${done.partPointers} pointers (+${done.partRawFallback} raw)`],
        ["events", `${done.eventSlims} slims (+${done.eventRawFallback} raw)`],
        ["blobs", String(done.blobs)],
        ["inline rows", String(done.inlineRows)],
        ["self-verify", args.verify ? "0 diffs" : "skipped (--no-verify)"],
        ["sha256", done.digest],
        ...timingRows(done.phaseMs),
      ]),
    )
    yield* Effect.promise(() => SessionColdV2Progress.maybeWaitForContinue({ wait: args.wait }))
  }),
})

export const DbColdV2UnpackCommand = effectCmd({
  command: "unpack",
  describe: "restore a v2 archive back to a live-layout v1 file (use --force to target the live database)",
  instance: false,
  builder: (yargs: Argv) => {
    return yargs
      .option("src", { type: "string", describe: "v2 archive (default: opencode-cold-v2.db next to live)" })
      .option("dst", { type: "string", demandOption: true, describe: "v1 file to create" })
      .option("force", { type: "boolean", default: false, describe: "Replace an existing dst file (required to target the live database)" })
      .option("progress", { type: "boolean", default: true, describe: "Live progress bar (use --no-progress for plain logs)" })
      .option("wait", {
        type: "boolean",
        describe: "Pause on the result screen (default: only when interactive; --no-wait never pauses)",
      })
  },
  handler: Effect.fn("Cli.db.cold-v2.unpack")(function* (args: { src?: string; dst: string; force: boolean; progress: boolean; wait?: boolean }) {
    const src = resolve(args.src ?? archivePath(livePath()))
    const dst = resolve(args.dst)
    if (src === dst) return yield* fail("src and dst must differ")
    const targetsLive = resolve(livePath()) === dst
    if (targetsLive && !args.force) {
      return yield* fail("refusing to overwrite the live database without --force; re-run with --force to restore live from the archive")
    }
    // Post-migration the v1 origin is frozen: it is only ever equal to dst
    // here when the caller names it explicitly (pre-migration livePath() is
    // the origin itself, so targetsLive already caught that world).
    if (!targetsLive && resolve(originPath()) === dst) {
      const frozen =
        (yield* Effect.promise(() => exists(resolve(livePath())))) ||
        (yield* Effect.promise(() => exists(resolve(archivePath(livePath())))))
      if (frozen) {
        return yield* fail(
          `refusing to overwrite the frozen v1 origin ${dst}; the live database is ${resolve(livePath())} (restore with --dst ${resolve(livePath())} --force)`,
        )
      }
    }
    if ((yield* Effect.promise(() => exists(dst))) && !args.force) {
      return yield* fail(`dst exists (use --force to replace): ${dst}`)
    }
    const room = yield* Effect.promise(() => SessionColdV2.diskRoom(src, dirname(dst), 2))
    if (room.free !== null && room.free < room.need) {
      return yield* fail(
        `disk space: ${(room.free / 1e9).toFixed(2)}GB free next to dst, need ~${(room.need / 1e9).toFixed(2)}GB (2x archive for work copy + VACUUM)`,
      )
    }
    const tracker = commandProgress(args.progress)
    const stats = yield* Effect.tryPromise({
      try: () =>
        SessionColdV2.withFileLock(`${dst}.lock`, async () => {
          const sidecar = await SessionColdV2.verifySidecar(src)
          if (sidecar === null) console.log(`warn: no ${src}.sha256 sidecar; skipping pre-check`)
          const stale = await SessionColdV2.cleanStaleTmps(dst)
          if (stale > 0) console.log(`removed ${stale} orphaned tmp file(s) from killed runs`)
          const tmp = `${dst}.tmp.${process.pid}`
          await SessionColdV2.removeIfExists(tmp)
          // A previous live file's WAL sidecars must not survive the rename:
          // they would replay against the restored image.
          for (const suffix of ["-wal", "-shm", "-journal"]) await SessionColdV2.removeIfExists(`${dst}${suffix}`)
          await SessionColdV2.copyBytes(src, tmp)
          const restored = await SessionColdV2.restoreFile(tmp, false, { progress: tracker })
          await SessionColdV2.atomicPublish(tmp, dst)
          for (const suffix of ["-wal", "-shm", "-journal"]) await SessionColdV2.removeIfExists(`${dst}${suffix}`)
          return { ...restored, phaseMs: tracker.timings() }
        }),
      catch: (cause) => toCliError(cause),
    })
    console.log(
      SessionColdV2Progress.formatResultPanel(`unpack done: ${dst}`, [
        ["parts", String(stats.parts)],
        ["events", String(stats.events)],
        ...timingRows(stats.phaseMs),
        ["next", targetsLive ? "live database restored; restart opencode" : "point OPENCODE_DB at the file or inspect with sqlite3"],
      ]),
    )
    yield* Effect.promise(() => SessionColdV2Progress.maybeWaitForContinue({ wait: args.wait }))
  }),
})

export const DbColdV2FetchCommand = effectCmd({
  command: "fetch",
  describe: "fault open sessions into live on demand (stub headers already browse; this warms payloads)",
  instance: false,
  builder: (yargs: Argv) => {
    return yargs
      .option("session", {
        type: "string",
        array: true,
        describe: "Session id(s) to fault in, repeatable (default: --all is required)",
      })
      .option("all", { type: "boolean", default: false, describe: "Fault in every stub session (live becomes full; prefer on-demand opens)" })
      .option("progress", { type: "boolean", default: true, describe: "Live progress bar (use --no-progress for plain logs)" })
      .option("wait", {
        type: "boolean",
        describe: "Pause on the result screen (default: only when interactive; --no-wait never pauses)",
      })
  },
  handler: Effect.fn("Cli.db.cold-v2.fetch")(function* (args: { session?: string[]; all: boolean; progress: boolean; wait?: boolean }) {
    const live = resolve(livePath())
    const archive = resolve(archivePath(live))
    if (!args.all && (!args.session || args.session.length === 0)) {
      return yield* fail("nothing to fetch: pass --session <id> (repeatable) or --all")
    }
    const tracker = commandProgress(args.progress)
    const done = yield* Effect.tryPromise({
      try: async () => {
        if (args.all) {
          const db = await SessionColdV2.openRawDb(live, "ro")
          let ids: string[]
          try {
            ids = db.all<{ id: string }>(`SELECT id FROM session ORDER BY id`).map((row) => row.id)
          } finally {
            db.close()
          }
          return SessionColdV2.faultInSessions(archive, live, ids)
        }
        return SessionColdV2.faultInSessions(archive, live, args.session ?? [])
      },
      catch: (cause) => toCliError(cause),
    })
    console.log(
      SessionColdV2Progress.formatResultPanel(`fetch done: ${live}`, [
        ["sessions", String(done.sessions)],
        ["parts", String(done.parts)],
        ["events", String(done.events)],
        ...timingRows(done.phaseMs),
      ]),
    )
    yield* Effect.promise(() => SessionColdV2Progress.maybeWaitForContinue({ wait: args.wait }))
  }),
})

export const DbColdV2EvictCommand = effectCmd({
  command: "evict",
  describe: "drop open sessions' payloads from live after proving they match the archive (headers stay for browsing)",
  instance: false,
  builder: (yargs: Argv) => {
    return yargs
      .option("session", {
        type: "string",
        array: true,
        describe: "Session id(s) to evict, repeatable",
      })
      .option("idle-minutes", { type: "number", default: 30, describe: "Without --session: evict sessions idle longer than this (archived+idle policy)" })
      .option("active", {
        type: "string",
        array: true,
        describe: "Session id(s) to skip, repeatable or comma-separated",
      })
      .option("dry-run", { type: "boolean", default: false, describe: "List eviction candidates without evicting" })
      .option("progress", { type: "boolean", default: true, describe: "Live progress bar (use --no-progress for plain logs)" })
      .option("wait", {
        type: "boolean",
        describe: "Pause on the result screen (default: only when interactive; --no-wait never pauses)",
      })
  },
  handler: Effect.fn("Cli.db.cold-v2.evict")(function* (args: {
    session?: string[]
    "idle-minutes": number
    active?: string[]
    "dry-run": boolean
    progress: boolean
    wait?: boolean
  }) {
    const live = resolve(livePath())
    const archive = resolve(archivePath(live))
    const ids = yield* Effect.tryPromise({
      try: async () => {
        if (args.session && args.session.length > 0) return args.session
        const active = activeFrom(args.active)
        const db = await SessionColdV2.openRawDb(live, "ro")
        try {
          const rows = db.all<{ id: string; parent_id: string | null; title: string; time_archived: number | null; time_updated: number }>(
            `SELECT id, parent_id, title, time_archived, time_updated FROM session ORDER BY time_updated DESC`,
          )
          const now = Date.now()
          const policy = { ...SessionCold.defaultPolicy(now), idleMs: args["idle-minutes"] * 60 * 1000 }
          const candidates = rows
            .filter((row) => {
              const meta = {
                id: row.id as SessionID,
                parentID: (row.parent_id ?? undefined) as SessionID | undefined,
                title: row.title,
                timeArchived: row.time_archived ?? undefined,
                timeUpdated: row.time_updated,
              }
              return SessionCold.isColdCandidate(meta, policy) && SessionCold.isIdle(meta, policy, active)
            })
            .map((row) => row.id)
          // Only sessions actually resident (heavy present) are worth evicting.
          return candidates.filter((id) => {
            try {
              return SessionColdV2.sessionIsResident(db, id)
            } catch {
              return false
            }
          })
        } finally {
          db.close()
        }
      },
      catch: (cause) => toCliError(cause),
    })
    if (args["dry-run"]) {
      console.log(`evict candidates (${ids.length}):`)
      for (const id of ids) console.log(`  ${id}`)
      return
    }
    if (ids.length === 0) return yield* fail("nothing to evict: no resident idle sessions matched")
    const tracker = commandProgress(args.progress)
    const done = yield* Effect.tryPromise({
      try: () => SessionColdV2.evictSessions(archive, live, ids),
      catch: (cause) => toCliError(cause),
    })
    void tracker
    console.log(
      SessionColdV2Progress.formatResultPanel(`evict done: ${live}`, [
        ["sessions", String(done.sessions)],
        ...timingRows(done.phaseMs),
      ]),
    )
    yield* Effect.promise(() => SessionColdV2Progress.maybeWaitForContinue({ wait: args.wait }))
  }),
})

export const DbColdV2VerifyCommand = effectCmd({
  command: "pack-verify",
  describe: "read-only integrity sweep of a v2 archive (manifest chain + every blob re-hashed)",
  instance: false,
  builder: (yargs: Argv) => {
    return yargs
      .option("src", { type: "string", describe: "v2 archive (default: opencode-cold-v2.db next to live)" })
      .option("progress", { type: "boolean", default: true, describe: "Live progress bar (use --no-progress for plain logs)" })
      .option("wait", {
        type: "boolean",
        describe: "Pause on the result screen (default: only when interactive; --no-wait never pauses)",
      })
  },
  handler: Effect.fn("Cli.db.cold-v2.pack-verify")(function* (args: { src?: string; progress: boolean; wait?: boolean }) {
    const src = resolve(args.src ?? archivePath(livePath()))
    const tracker = commandProgress(args.progress)
    const report = yield* Effect.tryPromise({
      try: async () => {
        const sidecar = await SessionColdV2.verifySidecar(src)
        if (sidecar === null) console.log(`warn: no ${src}.sha256 sidecar; skipping pre-check`)
        return SessionColdV2.verifyArchive(src, { progress: tracker })
      },
      catch: (cause) => toCliError(cause),
    })
    console.log(
      SessionColdV2Progress.formatResultPanel(`verify ok: ${src}`, [
        ["sessions", report.sessions],
        ["blobs", String(report.blobs)],
        ["pointers", String(report.pointers)],
        ["templates", report.templates],
        ["codecs", report.codecs.join(",")],
        ...timingRows(tracker.timings()),
      ]),
    )
    yield* Effect.promise(() => SessionColdV2Progress.maybeWaitForContinue({ wait: args.wait }))
  }),
})

export * as DbColdV2 from "./db-cold-v2"
