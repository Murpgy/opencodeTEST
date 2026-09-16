// Startup migration nudge: v1 live database <-> packed v2 archive.
//
// V2-as-live semantics:
//   - The v2 archive is the durable source of truth. `db pack` snapshots the
//     live file read-only (VACUUM INTO for the live database, byte copy for
//     offline files) and publishes the archive atomically behind a lockfile.
//   - The live v1 file is a materialization. When it is missing or holds zero
//     sessions while a complete archive exists next to it (deleted after a
//     pack, fresh volume, ...), startup restores it from the archive under a
//     lock instead of booting empty. A live file that holds sessions is never
//     clobbered: new work since the last pack stays put.
//   - Controls: OPENCODE_COLD_V2_QUIET=1 suppresses the notes (scripts/CI),
//     OPENCODE_COLD_V2_AUTO_MIGRATE=1 migrates without asking (headless).
//     On an interactive TTY the user gets a yes/no prompt instead. Restores
//     always run (data recovery), the quiet flag only silences their log.
import { SessionColdV2 } from "@/session/cold-v2"
import { join, dirname } from "node:path"

export const archivePathFor = (live: string): string => join(dirname(live), "opencode-cold-v2.db")

const envOn = (key: string): boolean => {
  const value = process.env[key]?.toLowerCase()
  return value === "true" || value === "1"
}

const fileExists = (path: string): Promise<boolean> =>
  import("node:fs/promises").then(({ access }) => access(path).then(() => true, () => false))

export type V2ArchiveState = "missing" | "complete" | "incomplete" | "corrupt"

export interface V2MigrationStatus {
  readonly live: string
  readonly archive: string
  readonly liveExists: boolean
  readonly liveSessions: number
  readonly liveReadable: boolean
  readonly archiveState: V2ArchiveState
  readonly needsMigration: boolean
  // True when a complete archive exists but the live file is missing or holds
  // zero sessions: startup should re-materialize live from the archive.
  readonly needsRestore: boolean
}

const readArchiveState = async (archive: string): Promise<V2ArchiveState> => {
  if (!(await fileExists(archive))) return "missing"
  // Read-only open: a status check must not touch the archive either.
  // The open itself is inside try: directories or garbage files must read
  // as "corrupt" (still needs migration), never throw past the warning.
  try {
    const db = await SessionColdV2.openRawDb(archive, "ro")
    try {
      const fields = SessionColdV2.readMeta(db)
      if (fields["version"] !== SessionColdV2.FORMAT_VERSION) return "corrupt"
      return fields["complete"] === "1" ? "complete" : "incomplete"
    } catch {
      return "corrupt"
    } finally {
      db.close()
    }
  } catch {
    return "corrupt"
  }
}

const countLiveSessions = async (live: string): Promise<{ sessions: number; readable: boolean }> => {
  // Read-only open: the v1 original is sacred, even for counting.
  // Unopenable paths (directories, garbage) read as unreadable, never throw.
  try {
    const db = await SessionColdV2.openRawDb(live, "ro")
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

export const migrationStatus = async (live: string, archive: string): Promise<V2MigrationStatus> => {
  const liveExists = await fileExists(live)
  const archiveState = await readArchiveState(archive)
  if (archiveState === "complete") {
    // V2-as-live: a complete archive is the durable copy. When live is gone
    // or empty, flag a restore instead of booting empty. A live file that
    // holds sessions is left alone (second startup after a pack = done).
    // Counting costs one read-only open; reads never block in WAL mode.
    if (!liveExists) {
      return { live, archive, liveExists, liveSessions: 0, liveReadable: true, archiveState, needsMigration: false, needsRestore: true }
    }
    const { sessions, readable } = await countLiveSessions(live)
    if (readable && sessions === 0) {
      return { live, archive, liveExists, liveSessions: sessions, liveReadable: readable, archiveState, needsMigration: false, needsRestore: true }
    }
    if (!readable) {
      return { live, archive, liveExists, liveSessions: sessions, liveReadable: readable, archiveState, needsMigration: false, needsRestore: true }
    }
    return { live, archive, liveExists, liveSessions: sessions, liveReadable: readable, archiveState, needsMigration: false, needsRestore: false }
  }
  if (!liveExists) return { live, archive, liveExists, liveSessions: 0, liveReadable: true, archiveState, needsMigration: false, needsRestore: false }
  const { sessions, readable } = await countLiveSessions(live)
  // An unreadable live file still needs attention (the app itself will fail
  // on it); an empty one needs no migration.
  return { live, archive, liveExists, liveSessions: sessions, liveReadable: readable, archiveState, needsMigration: sessions > 0 || !readable, needsRestore: false }
}

export const formatMigrationWarning = (status: V2MigrationStatus): string => {
  const sizeNote = status.liveReadable ? `${status.liveSessions} sessions` : "unreadable"
  const stale =
    status.archiveState === "incomplete"
      ? `A previous migration left an incomplete archive (rebuild with: opencode db pack --all --force).${"\n"}`
      : status.archiveState === "corrupt"
        ? `The existing archive looks corrupt (rebuild with: opencode db pack --all --force).${"\n"}`
        : ""
  return (
    `[v2 storage] Live database ${status.live} (${sizeNote}) has no packed v2 archive.${"\n"}` +
    stale +
    `The v1 file stays untouched: migration snapshots it read-only and publishes ${status.archive}.${"\n"}` +
    `Migrate now with: opencode db pack --all${"\n"}` +
    `Set OPENCODE_COLD_V2_AUTO_MIGRATE=1 to migrate on startup, OPENCODE_COLD_V2_QUIET=1 to silence this.`
  )
}

export const formatRestoreNote = (status: V2MigrationStatus): string => {
  const reason = !status.liveExists ? "missing" : status.liveReadable ? "empty (0 sessions)" : "unreadable"
  return (
    `[v2 storage] Live database ${status.live} is ${reason}; restoring from packed archive ${status.archive}.${"\n"}` +
    `New sessions created since the last pack live only in the live file — a live file that holds sessions is never overwritten.`
  )
}

const askToMigrate = async (): Promise<boolean> => {
  try {
    const prompts = await import("@clack/prompts")
    const answer = await prompts.confirm({
      message: "Migrate the v1 database to a packed v2 archive now? (v1 stays untouched; large databases take a while)",
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
  readonly live?: string
  readonly archive?: string
}

const defaultPaths = async (): Promise<{ live: string; archive: string }> => {
  const { Database } = await import("@opencode-ai/core/database/database")
  const live = Database.path()
  return { live, archive: archivePathFor(live) }
}

// Another process won the live-lock race and is publishing the restore now.
// Wait for its file to appear with sessions instead of booting empty beside
// it. Returns true when a non-empty live file shows up in time.
const waitForLiveRestore = async (live: string, timeoutMs = 30_000): Promise<boolean> => {
  const start = Date.now()
  for (;;) {
    const { sessions, readable } = await countLiveSessions(live).catch(() => ({ sessions: 0, readable: false }))
    if (readable && sessions > 0) return true
    if (Date.now() - start > timeoutMs) return false
    await new Promise((resolve) => setTimeout(resolve, 500))
  }
}

export const maybeWarnColdV2Migration = async (input: StartupMigrationInput = {}): Promise<StartupMigrationOutcome> => {
  const quiet = envOn("OPENCODE_COLD_V2_QUIET")
  const { live, archive } = input.live ? { live: input.live, archive: input.archive ?? archivePathFor(input.live) } : await defaultPaths()
  // :memory: databases and fresh installs (no file yet) have nothing to migrate.
  if (live === ":memory:") return "silent"
  const status = await migrationStatus(live, archive)
  // V2-as-live: a missing/empty live file with a complete archive restores
  // first (data recovery beats the pack nudge). The quiet flag silences the
  // note but never skips the restore itself.
  if (status.needsRestore) {
    if (!quiet) process.stderr.write(formatRestoreNote(status) + "\n")
    try {
      const done = await SessionColdV2.restoreLiveFromArchive({ archive, live })
      if (!quiet) process.stderr.write(`[v2 storage] Restored ${done.parts} parts, ${done.events} events to ${live}.\n`)
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
  if (!status.needsMigration) return status.archiveState === "complete" ? "done" : "silent"
  process.stderr.write(formatMigrationWarning(status) + "\n")
  const auto = envOn("OPENCODE_COLD_V2_AUTO_MIGRATE")
  const interactive = Boolean(process.stdin.isTTY && process.stderr.isTTY) && !process.env.CI
  const go = auto || (interactive && (await askToMigrate()))
  if (!go) return "warned"
  // Full conversion: every session, verified, published atomically. The v1
  // source is only snapshotted inside packArchiveFlow, never written.
  await SessionColdV2.packArchiveFlow({ src: live, dst: archive, allow: null, minBytes: SessionColdV2.MIN_BYTES_DEFAULT, verify: true, treatAsLive: true })
  return "migrated"
}

export * as DbColdV2Startup from "./db-cold-v2-startup"
