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
  describe: "convert a v1 database file into a packed v2 archive (v1 kept read-only)",
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
    const done = yield* Effect.tryPromise({
      try: async () => {
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
  describe: "restore a v2 archive back to a live-layout v1 file (never overwrites the live database)",
  instance: false,
  builder: (yargs: Argv) => {
    return yargs
      .option("src", { type: "string", describe: "v2 archive (default: opencode-cold-v2.db next to live)" })
      .option("dst", { type: "string", demandOption: true, describe: "v1 file to create" })
      .option("force", { type: "boolean", default: false, describe: "Replace an existing dst file" })
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
    if (resolve(livePath()) === dst) {
      return yield* fail("refusing to overwrite the live database; unpack to a file and point OPENCODE_DB at it")
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
          await SessionColdV2.copyBytes(src, tmp)
          const restored = await SessionColdV2.restoreFile(tmp, false, { progress: tracker })
          await SessionColdV2.atomicPublish(tmp, dst)
          return { ...restored, phaseMs: tracker.timings() }
        }),
      catch: (cause) => toCliError(cause),
    })
    console.log(
      SessionColdV2Progress.formatResultPanel(`unpack done: ${dst}`, [
        ["parts", String(stats.parts)],
        ["events", String(stats.events)],
        ...timingRows(stats.phaseMs),
        ["next", "point OPENCODE_DB at the file or inspect with sqlite3"],
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
