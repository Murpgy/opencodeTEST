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
import { SessionColdV2 } from "@/session/cold-v2"
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
  // Pack source: the live file when it serves traffic, else the frozen origin.
  readonly source: string
  readonly needsMigration: boolean
  // True when a complete archive holds sessions but the live file is missing,
  // empty or unreadable: startup should re-materialize live from the archive.
  readonly needsRestore: boolean
  // True once the live file exists: the origin is frozen from here on.
  readonly migrated: boolean
}

const readArchive = async (archive: string): Promise<{ state: V2ArchiveState; sessions: number }> => {
  if (!(await fileExists(archive))) return { state: "missing", sessions: 0 }
  // Read-only open: a status check must not touch the archive either.
  // The open itself is inside try: directories or garbage files must read
  // as "corrupt" (still needs migration), never throw past the warning.
  try {
    const db = await SessionColdV2.openRawDb(archive, "ro")
    try {
      const fields = SessionColdV2.readMeta(db)
      if (fields["version"] !== SessionColdV2.FORMAT_VERSION) return { state: "corrupt", sessions: 0 }
      if (fields["complete"] !== "1") return { state: "incomplete", sessions: 0 }
      const count = Number(fields["count_session"] ?? "0")
      return { state: "complete", sessions: Number.isInteger(count) && count >= 0 ? count : 0 }
    } catch {
      return { state: "corrupt", sessions: 0 }
    } finally {
      db.close()
    }
  } catch {
    return { state: "corrupt", sessions: 0 }
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
  const { state: archiveState, sessions: archiveSessions } = await readArchive(archive)
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
    `[v2 storage] Live database ${status.live} is ${reason}; restoring ${status.archiveSessions} sessions from packed archive ${status.archive}.${"\n"}` +
    `The frozen origin ${status.origin} is not touched.`
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
  if (!quiet) process.stderr.write(`[v2 storage] Live ready at ${live} (${done.parts} parts, ${done.events} events).\n`)
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
  if (!status.needsMigration) return status.migrated || status.archiveState === "complete" ? "done" : "silent"
  process.stderr.write(formatMigrationWarning(status) + "\n")
  const auto = envOn("OPENCODE_COLD_V2_AUTO_MIGRATE")
  const interactive = Boolean(process.stdin.isTTY && process.stderr.isTTY) && !process.env.CI
  const go = auto || (interactive && (await askToMigrate()))
  if (!go) return "warned"
  // Full conversion: every session, verified, published atomically. Sources
  // are only snapshotted inside packArchiveFlow, never written.
  await SessionColdV2.packArchiveFlow({ src: status.source, dst: archive, allow: null, minBytes: SessionColdV2.MIN_BYTES_DEFAULT, verify: true, treatAsLive: true })
  // The first migration must materialize the live file so traffic moves off
  // the origin from here on; later packs only refresh the archive.
  const after = await migrationStatus(origin, live, archive)
  if (after.needsRestore) {
    if (!quiet) process.stderr.write(formatRestoreNote(after) + "\n")
    await restoreAndReport(live, archive, quiet)
    if (!quiet) process.stderr.write(`[v2 storage] ${origin} is now frozen; live traffic serves from ${live}.\n`)
  }
  return "migrated"
}

export * as DbColdV2Startup from "./db-cold-v2-startup"
