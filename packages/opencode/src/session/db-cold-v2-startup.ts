// Startup migration nudge: v1 live database -> packed v2 archive.
//
// Semantics, read carefully:
//   - The v1 file is NEVER opened writable here. Status checks stat it or
//     open it read-only; migration snapshots it (VACUUM INTO for the live
//     database, byte copy for offline files) and packs the snapshot.
//   - Once a complete v2 archive exists next to the live database, the v1
//     file is IGNORED by this check: no warning, no action, every startup.
//     The live database keeps serving the app unchanged; the v2 archive is
//     cold storage, not a replacement live file (see unpack).
//   - Controls: OPENCODE_COLD_V2_QUIET=1 suppresses the warning (scripts/CI),
//     OPENCODE_COLD_V2_AUTO_MIGRATE=1 migrates without asking (headless).
//     On an interactive TTY the user gets a yes/no prompt instead.
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
    // Migrated: the v1 file is ignored from here on. Deliberately skip even
    // opening it, so post-migration startups pay two stats and nothing else.
    return { live, archive, liveExists, liveSessions: 0, liveReadable: true, archiveState, needsMigration: false }
  }
  if (!liveExists) return { live, archive, liveExists, liveSessions: 0, liveReadable: true, archiveState, needsMigration: false }
  const { sessions, readable } = await countLiveSessions(live)
  // An unreadable live file still needs attention (the app itself will fail
  // on it); an empty one needs no migration.
  return { live, archive, liveExists, liveSessions: sessions, liveReadable: readable, archiveState, needsMigration: sessions > 0 || !readable }
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

export type StartupMigrationOutcome = "silent" | "warned" | "migrated" | "done"

export interface StartupMigrationInput {
  readonly live?: string
  readonly archive?: string
}

const defaultPaths = async (): Promise<{ live: string; archive: string }> => {
  const { Database } = await import("@opencode-ai/core/database/database")
  const live = Database.path()
  return { live, archive: archivePathFor(live) }
}

export const maybeWarnColdV2Migration = async (input: StartupMigrationInput = {}): Promise<StartupMigrationOutcome> => {
  if (envOn("OPENCODE_COLD_V2_QUIET")) return "silent"
  const { live, archive } = input.live ? { live: input.live, archive: input.archive ?? archivePathFor(input.live) } : await defaultPaths()
  // :memory: databases and fresh installs (no file yet) have nothing to migrate.
  if (live === ":memory:") return "silent"
  const status = await migrationStatus(live, archive)
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
