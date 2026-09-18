// Startup migration: frozen v1 origin <-> v2 live <-> packed v2 archive.
//
// Layout (all three sit next to each other in the data dir):
//   opencode.db           frozen v1 origin. Read-only source for the first
//                         migration; never opened again once the live file
//                         serves traffic or a complete archive exists.
//   opencode-live-v2.db   live database. Every Session read/write goes here
//                         after migration (Database.path() redirects to it).
//                         Deleting it re-materializes from the archive on boot.
//   opencode-cold-v2.db   packed v2 archive, the durable copy. `db pack`
//                         snapshots read-only and publishes here; a missing or
//                         empty live file restores from here under a lock.
//
// Controls: OPENCODE_COLD_V2_QUIET=1 suppresses the notes (scripts/CI),
// OPENCODE_COLD_V2_AUTO_MIGRATE=1 migrates without asking (headless).
// On an interactive TTY the user gets a yes/no prompt instead. Restores
// always run (data recovery), the quiet flag only silences their log.
//
// Auto-evict (on by default once migrated): every session open faults payloads
// in and then files idle sessions back to stubs, keeping live near the working
// set. Only resident sessions idle longer than the window go (byte-verified
// against the archive first; dirty or not-yet-archived sessions wait for the
// next pack). Never throws past the reader.
//   OPENCODE_COLD_V2_AUTO_EVICT=0 disables it.
//   OPENCODE_COLD_V2_EVICT_IDLE_MINUTES=N idle window (default 30).
//   OPENCODE_COLD_V2_EVICT_MAX=N cap per sweep (default 20).
//
// Known edge (preservation bias, documented): deleting EVERY session and then
// booting before the next pack re-indexes the archived headers as stubs —
// the archive is the durable copy and a 0-session live file is
// indistinguishable from a recreated one. Re-delete (or run db evict) and
// pack to converge; emptying the archive itself is always refused loud and
// requires deleting the archive file explicitly.
import { SessionColdV2 } from "@/session/cold-v2"
import { LlmActivity } from "@/session/llm-activity"
import { join, dirname } from "node:path"

export const archivePathFor = (origin: string): string => join(dirname(origin), "opencode-cold-v2.db")

// Mirror of Database.liveV2PathFor (kept import-free so status checks stay
// light; both must name the same file).
export const liveV2PathFor = (origin: string): string => join(dirname(origin), "opencode-live-v2.db")

const envOn = (key: string): boolean => {
  const value = process.env[key]?.toLowerCase()
  return value === "true" || value === "1"
}

const fileExists = (path: string): Promise<boolean> =>
  import("node:fs/promises").then(({ access }) => access(path).then(() => true, () => false))

export type V2ArchiveState = "missing" | "complete" | "incomplete" | "corrupt"

export interface V2MigrationStatus {
  readonly origin: string
  readonly live: string
  readonly archive: string
  readonly originExists: boolean
  readonly originSessions: number
  readonly originReadable: boolean
  readonly liveExists: boolean
  readonly liveSessions: number
  readonly liveReadable: boolean
  readonly archiveState: V2ArchiveState
  readonly archiveSessions: number
  // Archive format version ("4", "5", or "" when missing/corrupt). v4 keeps
  // serving (read-duality); the only difference is a one-line migrate nudge.
  readonly archiveVersion: string
  // Pack source: the live file when it serves traffic, else the frozen origin.
  readonly source: string
  readonly needsMigration: boolean
  // True when a complete archive holds sessions but the live file is missing,
  // empty or unreadable: startup should re-materialize live from the archive.
  readonly needsRestore: boolean
  // True once the live file exists: the origin is frozen from here on.
  readonly migrated: boolean
}

const readArchive = async (archive: string): Promise<{ state: V2ArchiveState; sessions: number; version: string }> => {
  if (!(await fileExists(archive))) return { state: "missing", sessions: 0, version: "" }
  // Read-only open: a status check must not touch the archive either.
  // The open itself is inside try: directories or garbage files must read
  // as "corrupt" (still needs migration), never throw past the warning.
  try {
    const db = await SessionColdV2.openRawDb(archive, "ro")
    try {
      const fields = SessionColdV2.readMeta(db)
      if (!SessionColdV2.READABLE_VERSIONS.has(fields["version"] ?? "")) return { state: "corrupt", sessions: 0, version: "" }
      if (fields["complete"] !== "1") return { state: "incomplete", sessions: 0, version: fields["version"] ?? "" }
      const count = Number(fields["count_session"] ?? "0")
      return { state: "complete", sessions: Number.isInteger(count) && count >= 0 ? count : 0, version: fields["version"] ?? "" }
    } catch {
      return { state: "corrupt", sessions: 0, version: "" }
    } finally {
      db.close()
    }
  } catch {
    return { state: "corrupt", sessions: 0, version: "" }
  }
}

const countSessions = async (file: string): Promise<{ sessions: number; readable: boolean }> => {
  // Read-only opens: status checks never write. Unopenable paths
  // (directories, garbage) read as unreadable, never throw.
  try {
    const db = await SessionColdV2.openRawDb(file, "ro")
    try {
      const tables = db.all<{ name: string }>(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'session'`)
      if (tables.length === 0) return { sessions: 0, readable: true }
      const row = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM session`)
      return { sessions: row?.n ?? 0, readable: true }
    } catch {
      return { sessions: 0, readable: false }
    } finally {
      db.close()
    }
  } catch {
    return { sessions: 0, readable: false }
  }
}

export const migrationStatus = async (origin: string, live: string, archive: string): Promise<V2MigrationStatus> => {
  const [originExists, liveExists] = await Promise.all([fileExists(origin), fileExists(live)])
  const { state: archiveState, sessions: archiveSessions, version: archiveVersion } = await readArchive(archive)
  const liveInfo = liveExists ? await countSessions(live) : { sessions: 0, readable: true }
  const liveActive = liveExists && liveInfo.readable && liveInfo.sessions > 0
  // Freeze: once live serves traffic or a complete archive exists, the origin
  // is never opened again. Otherwise (first boot, crashed pack) the origin is
  // still the world and must be counted.
  const frozen = liveActive || archiveState === "complete"
  const originInfo = !frozen && originExists ? await countSessions(origin) : { sessions: 0, readable: true }
  const base = {
    origin,
    live,
    archive,
    originExists,
    originSessions: originInfo.sessions,
    originReadable: originInfo.readable,
    liveExists,
    liveSessions: liveInfo.sessions,
    liveReadable: liveInfo.readable,
    archiveState,
    archiveSessions,
    archiveVersion,
  }
  if (archiveState === "complete") {
    if (liveActive) return { ...base, source: live, needsMigration: false, needsRestore: false, migrated: true }
    // An empty archive over an empty live is a genuine empty world, not a
    // recovery case: restoring would loop every boot with no data to save.
    if (archiveSessions > 0) return { ...base, source: live, needsMigration: false, needsRestore: true, migrated: true }
    return { ...base, source: live, needsMigration: false, needsRestore: false, migrated: liveExists }
  }
  const originActive = !frozen && originExists && (originInfo.sessions > 0 || !originInfo.readable)
  if (liveActive || originActive) {
    return { ...base, source: liveActive ? live : origin, needsMigration: true, needsRestore: false, migrated: liveExists }
  }
  return { ...base, source: liveExists ? live : origin, needsMigration: false, needsRestore: false, migrated: false }
}

export const formatMigrationWarning = (status: V2MigrationStatus): string => {
  const sourceSessions = status.source === status.live ? status.liveSessions : status.originSessions
  const sourceReadable = status.source === status.live ? status.liveReadable : status.originReadable
  const sizeNote = sourceReadable ? `${sourceSessions} sessions` : "unreadable"
  const stale =
    status.archiveState === "incomplete"
      ? `A previous migration left an incomplete archive (rebuild with: opencode db pack --all --force).${"\n"}`
      : status.archiveState === "corrupt"
        ? `The existing archive looks corrupt (rebuild with: opencode db pack --all --force).${"\n"}`
        : ""
  return (
    `[v2 storage] ${status.source} (${sizeNote}) has no packed v2 archive.${"\n"}` +
    stale +
    `Migration snapshots read-only and publishes ${status.archive}; afterwards live traffic moves to ${status.live} and ${status.origin} stays frozen.${"\n"}` +
    `Migrate now with: opencode db pack --all${"\n"}` +
    `Set OPENCODE_COLD_V2_AUTO_MIGRATE=1 to migrate on startup, OPENCODE_COLD_V2_QUIET=1 to silence this.`
  )
}

export const formatRestoreNote = (status: V2MigrationStatus): string => {
  const reason = !status.liveExists ? "missing" : status.liveReadable ? "empty (0 sessions)" : "unreadable"
  return (
    `[v2 storage] Live database ${status.live} is ${reason}; indexing ${status.archiveSessions} sessions from packed archive ${status.archive} (slim: headers only, payloads fault in on open).${"\n"}` +
    `The frozen origin ${status.origin} is not touched.`
  )
}

// On-demand fault-in for session reads. No-op unless migrated (live-v2 file
// exists) with a complete archive: pre-migration live is full, :memory: has no
// archive, and missing archives mean live-only data. Never resurrects deleted
// sessions (no live header → not found) and never throws past the caller:
// a fault-in failure surfaces as empty (caller retries or reports not found).
const resolveLivePaths = async (): Promise<{ live: string; archive: string } | null> => {
  const { Database } = await import("@opencode-ai/core/database/database")
  const origin = Database.basePath()
  if (origin === ":memory:") return null
  const live = Database.path()
  if (live === ":memory:" || live === origin) return null
  const { access } = await import("node:fs/promises")
  if (await access(live).then(() => false, () => true)) return null
  const archive = archivePathFor(origin)
  if (await access(archive).then(() => false, () => true)) return null
  return { live, archive }
}

// One indexed live check on the hot path: message presence + the partial
// marker in a single read-only open. Missing marker rows adopt by presence
// (ephemeral — nothing is written): pre-marker lives only ever held whole
// sessions, so presence means complete.
const readWindowState = async (live: string, sessionID: string): Promise<{ present: boolean; complete: boolean } | null> => {
  const db = await SessionColdV2.openRawDb(live, "ro").catch(() => null)
  if (!db) return null
  try {
    const present = (db.get<{ one: number }>(`SELECT 1 AS one FROM message WHERE session_id = ? LIMIT 1`, [sessionID])?.one ?? 0) === 1
    let complete: boolean
    try {
      const row = db.get<{ complete: number }>(`SELECT complete FROM fault_state WHERE session_id = ?`, [sessionID])
      complete = row ? row.complete === 1 : present
    } catch {
      complete = present
    }
    return { present, complete }
  } catch {
    return null
  } finally {
    db.close()
  }
}

export const ensureSessionsResident = async (sessionIDs: readonly string[]): Promise<void> => {
  if (sessionIDs.length === 0) return
  try {
    const paths = await resolveLivePaths()
    if (!paths) return
    const todo: string[] = []
    for (const id of sessionIDs) {
      const state = await readWindowState(paths.live, id)
      if (!state || state.complete) continue
      todo.push(id)
    }
    if (todo.length > 0) await SessionColdV2.faultInSessions(paths.archive, paths.live, todo)
    maybeAutoEvict(paths.archive, paths.live, sessionIDs)
  } catch {
    // Best-effort: reads proceed against live; a stub reads empty and the
    // next open retries. Fault-in errors are loud in the cold log already.
  }
}

// Bounded-read variant: faults only the newest window synchronously (what the
// caller displays) and completes the rest in the background. Tail callers
// (TUI open with limit:100, web first page) stop paying full-session latency.
export const ensureSessionTail = async (sessionIDs: readonly string[], tailMessages: number): Promise<void> => {
  if (sessionIDs.length === 0) return
  try {
    const paths = await resolveLivePaths()
    if (!paths) return
    const todo: string[] = []
    const incomplete: string[] = []
    for (const id of sessionIDs) {
      const state = await readWindowState(paths.live, id)
      if (!state) continue
      if (!state.complete) incomplete.push(id)
      // A present tail (complete or partial) already serves bounded reads.
      if (state.present) continue
      todo.push(id)
    }
    if (todo.length > 0) {
      await SessionColdV2.faultInSessions(paths.archive, paths.live, todo, { tailMessages })
      for (const id of todo) if (!incomplete.includes(id)) incomplete.push(id)
    }
    maybeAutoEvict(paths.archive, paths.live, sessionIDs)
    kickCompletion(paths.archive, paths.live, incomplete)
  } catch {
    // Best-effort: same contract as ensureSessionsResident.
  }
}

// Single-message variant for getPart / MessageV2.get: faults one message plus
// its parts instead of the whole session. The streaming hot loop reads the
// ACTIVE tool call's part per delta — a full fault there stalls mid-stream.
// The lock-free fast path lives inside faultInMessagePart (one indexed live
// SELECT, no archive touch when resident). Deliberately no auto-evict (a
// residency scan per tool delta is pure overhead; eviction happens on session
// opens) and no completion kick (the open's tail fault already kicked one; an
// unbounded read completes synchronously anyway). Best-effort like the rest:
// failures surface as not-found and the next read retries.
export const ensurePartResident = async (sessionID: string, messageID: string, partID?: string): Promise<void> => {
  try {
    const paths = await resolveLivePaths()
    if (!paths) return
    await SessionColdV2.faultInMessagePart(paths.archive, paths.live, sessionID, messageID, partID)
  } catch {
    // Best-effort: reads proceed against live; a missing part reads empty
    // and the next read retries.
  }
}

// Deferral for background cold work: while any session streams model output,
// the user is latency-sensitive, so completion and auto-evict wait for idle
// instead of contending for SQLite/CPU with the stream. User-visible faults
// (the tail/full reads above) are NEVER deferred — only the background extras.
//
// The timer is unref'd so a one-shot CLI (`opencode run`) can still exit with
// a deferral pending (the work is pure optimization; an unbounded read
// completes synchronously when actually needed). Tries are capped so a
// marathon session still converges eventually.
export interface DeferOpts {
  readonly deferMs?: number
  readonly maxDefers?: number
}

const DEFER_MS_DEFAULT = 15_000
const MAX_DEFERS_DEFAULT = 20

export const deferWhileBusy = (task: () => Promise<void>, opts: DeferOpts = {}): void => {
  const deferMs = opts.deferMs ?? DEFER_MS_DEFAULT
  const maxDefers = opts.maxDefers ?? MAX_DEFERS_DEFAULT
  const attempt = (triesLeft: number): void => {
    if (triesLeft <= 0 || !LlmActivity.anyLlmActive()) {
      // Promise.resolve().then: a synchronously-throwing task must still
      // surface as a rejection, never as a sync throw out of deferWhileBusy
      // (which would wedge callers' in-flight flags, e.g. autoEvictInFlight,
      // off for the process lifetime).
      void Promise.resolve()
        .then(task)
        .catch(() => {
          // Best-effort: callers already tolerate failure (completion is
          // retried by the next unbounded read; evict by the next open).
        })
      return
    }
    const timer = setTimeout(() => attempt(triesLeft - 1), deferMs)
    timer.unref?.()
  }
  attempt(maxDefers)
}

// Background completion for tail-faulted sessions: overlays the remaining
// rows without blocking the read that triggered it. Guarded against overlap
// (concurrent opens share one flight) and fully quiet on failure — an
// unbounded read completes synchronously instead, so nothing depends on this
// flight finishing. Must outlive the requesting read, so it floats outside
// the Effect scope rather than forking into it (a scoped fork would serialize
// the open it was meant to speed up).
const completionInFlight = new Set<string>()

export const kickCompletion = (
  archive: string,
  live: string,
  sessionIDs: readonly string[],
  opts: DeferOpts = {},
): void => {
  const fresh = sessionIDs.filter((id) => !completionInFlight.has(`${live}${id}`))
  if (fresh.length === 0) return
  for (const id of fresh) completionInFlight.add(`${live}${id}`)
  const settle = (): void => {
    for (const id of fresh) completionInFlight.delete(`${live}${id}`)
  }
  deferWhileBusy(() => SessionColdV2.faultInSessions(archive, live, fresh).then(settle, settle), opts)
}

// Live just grew by the fault-in above: file idle residents back to stubs so
// live tracks the working set instead of every session ever opened. Guarded
// against overlapping sweeps from concurrent opens (the file lock would
// serialize them anyway; the flag skips the pointless contention) and fully
// best-effort — an auto-evict failure must never break the read that caused it.
let autoEvictInFlight = false

export interface AutoEvictSettings {
  readonly enabled: boolean
  readonly idleMinutes: number
  readonly max: number
}

export const autoEvictSettings = (): AutoEvictSettings => {
  const raw = process.env.OPENCODE_COLD_V2_AUTO_EVICT?.toLowerCase()
  const enabled = raw === undefined || raw === "" ? true : raw !== "0" && raw !== "false" && raw !== "no" && raw !== "off"
  const idleMinutes = Number(process.env.OPENCODE_COLD_V2_EVICT_IDLE_MINUTES)
  const max = Number(process.env.OPENCODE_COLD_V2_EVICT_MAX)
  return {
    enabled,
    idleMinutes: Number.isInteger(idleMinutes) && idleMinutes > 0 ? idleMinutes : SessionColdV2.AUTO_EVICT_DEFAULT_IDLE_MINUTES,
    max: Number.isInteger(max) && max > 0 ? max : SessionColdV2.AUTO_EVICT_DEFAULT_MAX,
  }
}

const maybeAutoEvict = (archive: string, live: string, exclude: readonly string[], opts: DeferOpts = {}): void => {
  const settings = autoEvictSettings()
  if (!settings.enabled || autoEvictInFlight) return
  autoEvictInFlight = true
  deferWhileBusy(
    async () => {
      try {
        await SessionColdV2.autoEvictIdle(archive, live, { exclude, idleMinutes: settings.idleMinutes, max: settings.max })
      } catch {
        // Best-effort: the next open retries. autoEvictIdle already absorbs
        // per-session failures; this guards the unexpected.
      } finally {
        autoEvictInFlight = false
      }
    },
    opts,
  )
}

const askToMigrate = async (): Promise<boolean> => {
  try {
    const prompts = await import("@clack/prompts")
    const answer = await prompts.confirm({
      message: "Migrate to v2 storage now? (sources stay untouched; the live database moves to opencode-live-v2.db; large databases take a while)",
      initialValue: false,
    })
    if (prompts.isCancel(answer)) return false
    return answer === true
  } catch {
    // Non-interactive runtime or missing prompt support: warn only.
    return false
  }
}

export type StartupMigrationOutcome = "silent" | "warned" | "migrated" | "done" | "restored"

export interface StartupMigrationInput {
  readonly origin?: string
  readonly live?: string
  readonly archive?: string
}

const defaultPaths = async (): Promise<{ origin: string; live: string; archive: string }> => {
  const { Database } = await import("@opencode-ai/core/database/database")
  const origin = Database.basePath()
  return { origin, live: liveV2PathFor(origin), archive: archivePathFor(origin) }
}

// Another process won the live-lock race and is publishing the restore now.
// Wait for its file to appear with sessions instead of booting empty beside
// it. Returns true when a non-empty live file shows up in time.
const waitForLiveRestore = async (live: string, timeoutMs = 30_000): Promise<boolean> => {
  const start = Date.now()
  for (;;) {
    const { sessions, readable } = await countSessions(live).catch(() => ({ sessions: 0, readable: false }))
    if (readable && sessions > 0) return true
    if (Date.now() - start > timeoutMs) return false
    await new Promise((resolve) => setTimeout(resolve, 500))
  }
}

const restoreAndReport = async (live: string, archive: string, quiet: boolean): Promise<void> => {
  const done = await SessionColdV2.restoreLiveFromArchive({ archive, live })
  if (!quiet) process.stderr.write(`[v2 storage] Live ready at ${live} (${done.sessions} sessions indexed; payloads fault in on open).\n`)
}

export const maybeWarnColdV2Migration = async (input: StartupMigrationInput = {}): Promise<StartupMigrationOutcome> => {
  const quiet = envOn("OPENCODE_COLD_V2_QUIET")
  const { origin, live, archive } = input.origin
    ? { origin: input.origin, live: input.live ?? liveV2PathFor(input.origin), archive: input.archive ?? archivePathFor(input.origin) }
    : await defaultPaths()
  // :memory: databases and fresh installs (no file anywhere) have nothing to migrate.
  if (origin === ":memory:") return "silent"
  const status = await migrationStatus(origin, live, archive)
  // A missing/empty live file with a complete archive restores first (data
  // recovery beats the pack nudge). The quiet flag silences the note but
  // never skips the restore itself.
  if (status.needsRestore) {
    if (!quiet) process.stderr.write(formatRestoreNote(status) + "\n")
    try {
      await restoreAndReport(live, archive, quiet)
      return "restored"
    } catch (error) {
      // A second process restoring concurrently holds the live lock: poll for
      // its publish instead of booting empty beside it.
      if (/lock held/.test(error instanceof Error ? error.message : String(error))) {
        const restored = await waitForLiveRestore(live)
        if (restored) {
          if (!quiet) process.stderr.write(`[v2 storage] Live database appeared (restored by another process).\n`)
          return "restored"
        }
      }
      throw error
    }
  }
  if (quiet) return "silent"
  if (!status.needsMigration) {
    // v4 archives keep serving (read-duality): one line pointing at the
    // explicit, verified migration. Never block, never auto-migrate.
    if ((status.migrated || status.archiveState === "complete") && status.archiveVersion === "4" && status.archiveSessions > 0) {
      process.stderr.write(
        `[v2 storage] Archive ${archive} is format v4 (${status.archiveSessions} sessions, reads work as-is). ` +
          `Convert to v5 per-session bundles (~25-35% smaller) with: opencode db migrate-v5\n`,
      )
    }
    return status.migrated || status.archiveState === "complete" ? "done" : "silent"
  }
  process.stderr.write(formatMigrationWarning(status) + "\n")
  const auto = envOn("OPENCODE_COLD_V2_AUTO_MIGRATE")
  const interactive = Boolean(process.stdin.isTTY && process.stderr.isTTY) && !process.env.CI
  const go = auto || (interactive && (await askToMigrate()))
  if (!go) return "warned"
  // Full conversion, verified, published atomically. When live already serves
  // (slim working set + open sessions), the merge path folds live into a
  // restored full image first — packing the slim file directly would drop
  // every stub's payloads. Otherwise the origin packs directly.
  if (status.migrated) {
    await SessionColdV2.packLiveToArchive({ live, archive, minBytes: SessionColdV2.MIN_BYTES_DEFAULT, verify: true })
  } else {
    await SessionColdV2.packArchiveFlow({ src: status.source, dst: archive, allow: null, minBytes: SessionColdV2.MIN_BYTES_DEFAULT, verify: true, treatAsLive: true })
  }
  // The first migration must materialize the live file so traffic moves off
  // the origin from here on; later packs only refresh the archive.
  const after = await migrationStatus(origin, live, archive)
  if (after.needsRestore) {
    if (!quiet) process.stderr.write(formatRestoreNote(after) + "\n")
    await restoreAndReport(live, archive, quiet)
    if (!quiet) process.stderr.write(`[v2 storage] ${origin} is now frozen; live traffic serves from ${live} (session index only; payloads fault in on open).\n`)
  }
  return "migrated"
}

export * as DbColdV2Startup from "./db-cold-v2-startup"
