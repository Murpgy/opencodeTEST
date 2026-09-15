import type { Argv } from "yargs"
import { Database } from "@opencode-ai/core/database/database"
import { SessionCold } from "@/session/cold"
import { Effect } from "effect"
import { effectCmd, fail } from "../effect-cmd"

// Offline cold-storage tools. Safe to run while the TUI is open: candidates
// are idle sessions only (see --idle-minutes/--active), work is chunked, and
// nothing is deleted from live without --force after a hash verify.

const policyFrom = (args: {
  includeForks?: boolean
  olderThanDays?: number
  idleMinutes?: number
}): SessionCold.ColdPolicy => ({
  includeArchived: true,
  includeForks: args.includeForks ?? false,
  olderThanMs: (args.olderThanDays ?? 30) * 24 * 3600 * 1000,
  idleMs: (args.idleMinutes ?? 30) * 60 * 1000,
  now: Date.now(),
})

const activeFrom = (args: { active?: string[] }) => new Set((args.active ?? []).flatMap((value) => value.split(",")))

export const DbColdArchiveCommand = effectCmd({
  command: "archive",
  describe: "copy cold sessions to opencode-cold.db (delete from live only with --force)",
  instance: false,
  builder: (yargs: Argv) => {
    return yargs
      .option("batch", { type: "number", default: 1000, describe: "Rows per copy/delete chunk" })
      .option("idle-minutes", { type: "number", default: 30, describe: "Skip sessions updated within this window" })
      .option("active", {
        type: "string",
        array: true,
        describe: "Session id(s) to skip, repeatable or comma-separated (e.g. the open TUI session)",
      })
      .option("dry-run", { type: "boolean", default: false, describe: "List candidates without copying" })
      .option("force", { type: "boolean", default: false, describe: "Delete from live after verified copy" })
      .option("include-forks", {
        type: "boolean",
        default: false,
        describe: "Also archive fork copies older than --older-than-days",
      })
      .option("older-than-days", { type: "number", default: 30, describe: "Fork age threshold" })
      .option("compact-events", {
        type: "boolean",
        default: true,
        describe: "Drop superseded part.updated events for archived sessions in cold storage",
      })
  },
  handler: Effect.fn("Cli.db.cold.archive")(function* (args: {
    batch: number
    dryRun: boolean
    force: boolean
    includeForks: boolean
    olderThanDays: number
    idleMinutes: number
    active?: string[]
    compactEvents: boolean
  }) {
    const { db } = yield* Database.Service
    const policy = policyFrom(args)
    const active = activeFrom(args)
    const candidates = yield* SessionCold.listCandidates(db, policy, active)
    console.log(`cold database: ${SessionCold.coldPath()}`)
    console.log(`candidates: ${candidates.length}`)
    for (const meta of candidates) console.log(`  ${meta.id}  ${meta.title.slice(0, 80)}`)
    if (args.dryRun) return
    for (const meta of candidates) {
      const result = yield* SessionCold.archiveSession({ sessionID: meta.id, batch: args.batch, force: args.force })
      if (result.status === "mismatch") return yield* fail(`hash mismatch for ${meta.id}, live left untouched`)
      if (args.compactEvents && (result.status === "archived" || result.status === "copied")) {
        const compacted = yield* SessionCold.compactColdSession({ sessionID: meta.id, batch: args.batch })
        console.log(`${result.status} ${meta.id} (${result.moved} rows, events -${compacted.removed})`)
        continue
      }
      console.log(`${result.status} ${meta.id} (${result.moved} rows)`)
    }
  }),
})

export const DbColdRestoreCommand = effectCmd({
  command: "restore <sessionID>",
  describe: "copy a session back from cold storage to live",
  instance: false,
  builder: (yargs: Argv) => {
    return yargs
      .positional("sessionID", { type: "string", demandOption: true })
      .option("batch", { type: "number", default: 1000, describe: "Rows per copy/delete chunk" })
      .option("force", { type: "boolean", default: false, describe: "Delete the cold copy after verified restore" })
  },
  handler: Effect.fn("Cli.db.cold.restore")(function* (args: {
    sessionID: string
    batch: number
    force: boolean
  }) {
    const result = yield* SessionCold.restoreSession({ sessionID: args.sessionID, batch: args.batch, force: args.force })
    if (result.status === "missing") return yield* fail(`session not in cold storage: ${args.sessionID}`)
    if (result.status === "mismatch") return yield* fail(`hash mismatch for ${args.sessionID}, live left untouched`)
    console.log(`${result.status} ${args.sessionID} (${result.moved} rows)`)
  }),
})

export const DbColdVerifyCommand = effectCmd({
  command: "verify [sessionID]",
  describe: "compare live vs cold fingerprints for a session, or list cold contents",
  instance: false,
  builder: (yargs: Argv) => {
    return yargs.positional("sessionID", { type: "string" })
  },
  handler: Effect.fn("Cli.db.cold.verify")(function* (args: { sessionID?: string }) {
    if (!args.sessionID) {
      console.log(`cold database: ${SessionCold.coldPath()}`)
      return
    }
    const result = yield* SessionCold.verifySession(args.sessionID)
    console.log(`${result.match ? "MATCH" : "MISMATCH"} ${args.sessionID}`)
    if (!result.match) return yield* fail("fingerprints differ")
  }),
})

export * as DbCold from "./db-cold"
