import { Database } from "@opencode-ai/core/database/database"
import { EventSequenceTable, EventTable } from "@opencode-ai/core/event/sql"
import {
  MessageTable,
  PartTable,
  SessionContextEpochTable,
  SessionInputTable,
  SessionMessageTable,
  SessionTable,
  TodoTable,
} from "@opencode-ai/core/session/sql"
import { asc, desc, eq, inArray } from "drizzle-orm"
import { Context, Effect, Layer } from "effect"
import { createHash } from "node:crypto"
import path from "path"
import type { SessionID } from "@/session/schema"

// Cold sessions live in a second SQLite file with the identical schema, so a
// stock build can read it (OPENCODE_DB=<cold file>) and restore is a plain
// copy back. Archive = copy subtree -> verify hashes -> delete from live.
// The live database keeps working untouched; only selected sessions move.

export interface ColdPolicy {
  readonly includeArchived: boolean
  readonly includeForks: boolean
  readonly olderThanMs: number
  readonly idleMs: number
  readonly now: number
}

export interface SessionMeta {
  // Branded session id: drizzle's eq() overloads only accept values whose
  // type matches the column, so plain strings fail to typecheck here.
  readonly id: SessionID
  readonly parentID: SessionID | undefined
  readonly title: string
  readonly timeArchived: number | undefined
  readonly timeUpdated: number
}

export const defaultPolicy = (now = Date.now()): ColdPolicy => ({
  includeArchived: true,
  includeForks: false,
  olderThanMs: 30 * 24 * 3600 * 1000,
  idleMs: 30 * 60 * 1000,
  now,
})

const forkTitle = /\(fork #\d+\)$/

export function isColdCandidate(meta: SessionMeta, policy: ColdPolicy): boolean {
  if (meta.timeArchived !== undefined) return true
  if (!policy.includeForks) return false
  if (meta.parentID === undefined && !forkTitle.test(meta.title)) return false
  if (policy.now - meta.timeUpdated <= policy.olderThanMs) return false
  return true
}

export function isIdle(meta: SessionMeta, policy: ColdPolicy, active: ReadonlySet<string>): boolean {
  if (active.has(meta.id)) return false
  return policy.now - meta.timeUpdated > policy.idleMs
}

export const chunked = <T>(rows: T[], size: number): T[][] => {
  if (rows.length === 0) return []
  const out: T[][] = []
  for (let i = 0; i < rows.length; i += size) out.push(rows.slice(i, i + size))
  return out
}

export const coldPath = () => path.join(path.dirname(Database.path()), "opencode-cold.db")

type ColdDb = Database.Interface["db"]

const withColdDb = <A, E, R>(fn: (db: ColdDb) => Effect.Effect<A, E, R>) =>
  Effect.scoped(
    Effect.gen(function* () {
      // Building the layer runs DatabaseMigration.apply on the file, so a
      // missing cold database is created with the current schema automatically.
      const context = yield* Layer.build(Database.layerFromPath(coldPath()))
      return yield* fn(Context.get(context, Database.Service).db)
    }),
  )

const metaFromRow = (row: typeof SessionTable.$inferSelect): SessionMeta => ({
  id: row.id,
  parentID: row.parent_id ?? undefined,
  title: row.title,
  timeArchived: row.time_archived ?? undefined,
  timeUpdated: row.time_updated,
})

export const listCandidates = Effect.fn("SessionCold.listCandidates")(function* (
  db: ColdDb,
  policy: ColdPolicy,
  active: ReadonlySet<string>,
) {
  const rows = yield* db.select().from(SessionTable).orderBy(desc(SessionTable.time_updated)).all().pipe(Effect.orDie)
  const metas = rows.map(metaFromRow)
  const byId = new Map(metas.map((meta) => [meta.id, meta]))
  const wanted = new Set(metas.filter((meta) => isColdCandidate(meta, policy)).map((meta) => meta.id))
  // Never strand a live child: a parent stays live while any of its children
  // stay live (child-away/parent-live is harmless and allowed).
  const stranded = new Set<string>()
  for (const meta of metas) {
    if (meta.parentID === undefined) continue
    if (!byId.has(meta.parentID)) continue
    if (wanted.has(meta.parentID) && !wanted.has(meta.id)) stranded.add(meta.parentID)
  }
  return [...wanted].flatMap((id) => {
    const meta = byId.get(id)
    if (!meta || stranded.has(id) || !isIdle(meta, policy, active)) return []
    return [meta]
  })
})

const insertChunked = <T>(rows: T[], batch: number, insert: (group: T[]) => Effect.Effect<void, unknown>) =>
  Effect.gen(function* () {
    for (const group of chunked(rows, batch)) yield* insert(group)
  })

const copyIdsChunked = Effect.fn("SessionCold.copyIdsChunked")(function* (
  from: ColdDb,
  to: ColdDb,
  sessionID: SessionID,
  batch: number,
) {
  let moved = 0
  const messageIds = yield* from
    .select({ id: MessageTable.id })
    .from(MessageTable)
    .where(eq(MessageTable.session_id, sessionID))
    .orderBy(asc(MessageTable.id))
    .all()
    .pipe(Effect.orDie)
  for (const group of chunked(
    messageIds.map((row) => row.id),
    batch,
  )) {
    const rows = yield* from.select().from(MessageTable).where(inArray(MessageTable.id, group)).all().pipe(Effect.orDie)
    yield* to.insert(MessageTable).values(rows).onConflictDoNothing().run().pipe(Effect.orDie)
    moved += rows.length
  }
  const partIds = yield* from
    .select({ id: PartTable.id })
    .from(PartTable)
    .where(eq(PartTable.session_id, sessionID))
    .orderBy(asc(PartTable.id))
    .all()
    .pipe(Effect.orDie)
  for (const group of chunked(
    partIds.map((row) => row.id),
    batch,
  )) {
    const rows = yield* from.select().from(PartTable).where(inArray(PartTable.id, group)).all().pipe(Effect.orDie)
    yield* to.insert(PartTable).values(rows).onConflictDoNothing().run().pipe(Effect.orDie)
    moved += rows.length
  }
  return moved
})

const copySessionRows = Effect.fn("SessionCold.copySessionRows")(function* (
  from: ColdDb,
  to: ColdDb,
  sessionID: SessionID,
  batch: number,
) {
  const session = yield* from
    .select()
    .from(SessionTable)
    .where(eq(SessionTable.id, sessionID))
    .get()
    .pipe(Effect.orDie)
  if (!session) return 0
  yield* to.insert(SessionTable).values(session).onConflictDoNothing().run().pipe(Effect.orDie)
  let moved = 1
  const small = yield* Effect.all([
    from.select().from(SessionMessageTable).where(eq(SessionMessageTable.session_id, sessionID)).all(),
    from.select().from(TodoTable).where(eq(TodoTable.session_id, sessionID)).all(),
    from.select().from(SessionInputTable).where(eq(SessionInputTable.session_id, sessionID)).all(),
    from.select().from(SessionContextEpochTable).where(eq(SessionContextEpochTable.session_id, sessionID)).all(),
  ]).pipe(Effect.orDie)
  const [messages, todos, inputs, epochs] = small
  yield* insertChunked(messages, batch, (group) =>
    to.insert(SessionMessageTable).values(group).onConflictDoNothing().run().pipe(Effect.orDie),
  )
  yield* insertChunked(todos, batch, (group) =>
    to.insert(TodoTable).values(group).onConflictDoNothing().run().pipe(Effect.orDie),
  )
  yield* insertChunked(inputs, batch, (group) =>
    to.insert(SessionInputTable).values(group).onConflictDoNothing().run().pipe(Effect.orDie),
  )
  yield* insertChunked(epochs, batch, (group) =>
    to.insert(SessionContextEpochTable).values(group).onConflictDoNothing().run().pipe(Effect.orDie),
  )
  moved += messages.length + todos.length + inputs.length + epochs.length
  moved += yield* copyIdsChunked(from, to, sessionID, batch)
  // Event log: full aggregate history moves with the session so replay stays
  // possible from cold storage. Compaction happens separately (compactEvents).
  const events = yield* from
    .select()
    .from(EventTable)
    .where(eq(EventTable.aggregate_id, sessionID))
    .orderBy(asc(EventTable.seq))
    .all()
    .pipe(Effect.orDie)
  yield* insertChunked(events, batch, (group) =>
    to.insert(EventTable).values(group).onConflictDoNothing().run().pipe(Effect.orDie),
  )
  const sequence = yield* from
    .select()
    .from(EventSequenceTable)
    .where(eq(EventSequenceTable.aggregate_id, sessionID))
    .get()
    .pipe(Effect.orDie)
  if (sequence) {
    yield* to.insert(EventSequenceTable).values(sequence).onConflictDoNothing().run().pipe(Effect.orDie)
  }
  moved += events.length + (sequence ? 1 : 0)
  return moved
})

const fingerprint = Effect.fn("SessionCold.fingerprint")(function* (db: ColdDb, sessionID: SessionID) {
  const hash = createHash("sha256")
  const feed = (label: string, rows: unknown[]) => {
    hash.update(label)
    hash.update(JSON.stringify(rows))
  }
  feed(
    "session",
    yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).all().pipe(Effect.orDie),
  )
  feed(
    "message",
    yield* db
      .select()
      .from(MessageTable)
      .where(eq(MessageTable.session_id, sessionID))
      .orderBy(asc(MessageTable.id))
      .all()
      .pipe(Effect.orDie),
  )
  feed(
    "part",
    yield* db
      .select()
      .from(PartTable)
      .where(eq(PartTable.session_id, sessionID))
      .orderBy(asc(PartTable.id))
      .all()
      .pipe(Effect.orDie),
  )
  feed(
    "session_message",
    yield* db
      .select()
      .from(SessionMessageTable)
      .where(eq(SessionMessageTable.session_id, sessionID))
      .orderBy(asc(SessionMessageTable.id))
      .all()
      .pipe(Effect.orDie),
  )
  feed(
    "todo",
    yield* db
      .select()
      .from(TodoTable)
      .where(eq(TodoTable.session_id, sessionID))
      .orderBy(asc(TodoTable.position))
      .all()
      .pipe(Effect.orDie),
  )
  feed(
    "session_input",
    yield* db
      .select()
      .from(SessionInputTable)
      .where(eq(SessionInputTable.session_id, sessionID))
      .orderBy(asc(SessionInputTable.id))
      .all()
      .pipe(Effect.orDie),
  )
  feed(
    "epoch",
    yield* db
      .select()
      .from(SessionContextEpochTable)
      .where(eq(SessionContextEpochTable.session_id, sessionID))
      .all()
      .pipe(Effect.orDie),
  )
  feed(
    "event",
    yield* db
      .select()
      .from(EventTable)
      .where(eq(EventTable.aggregate_id, sessionID))
      .orderBy(asc(EventTable.seq))
      .all()
      .pipe(Effect.orDie),
  )
  feed(
    "sequence",
    yield* db
      .select()
      .from(EventSequenceTable)
      .where(eq(EventSequenceTable.aggregate_id, sessionID))
      .all()
      .pipe(Effect.orDie),
  )
  return hash.digest("hex")
})

const deleteSession = Effect.fn("SessionCold.deleteSession")(function* (db: ColdDb, sessionID: SessionID) {
  // Explicit FK-safe order inside one transaction; session row last.
  yield* db
    .transaction((tx) =>
      Effect.gen(function* () {
        yield* tx.delete(SessionInputTable).where(eq(SessionInputTable.session_id, sessionID)).run()
        yield* tx.delete(SessionContextEpochTable).where(eq(SessionContextEpochTable.session_id, sessionID)).run()
        yield* tx.delete(SessionMessageTable).where(eq(SessionMessageTable.session_id, sessionID)).run()
        yield* tx.delete(TodoTable).where(eq(TodoTable.session_id, sessionID)).run()
        yield* tx.delete(PartTable).where(eq(PartTable.session_id, sessionID)).run()
        yield* tx.delete(MessageTable).where(eq(MessageTable.session_id, sessionID)).run()
        yield* tx.delete(EventTable).where(eq(EventTable.aggregate_id, sessionID)).run()
        yield* tx.delete(EventSequenceTable).where(eq(EventSequenceTable.aggregate_id, sessionID)).run()
        yield* tx.delete(SessionTable).where(eq(SessionTable.id, sessionID)).run()
      }),
    )
    .pipe(Effect.orDie)
})

export const archiveSession = Effect.fn("SessionCold.archiveSession")(function* (input: {
  sessionID: SessionID
  batch: number
  force: boolean
}) {
  const { db } = yield* Database.Service
  const moved = yield* withColdDb((cold) => copySessionRows(db, cold, input.sessionID, input.batch))
  if (moved === 0) return { sessionID: input.sessionID, status: "missing" as const, moved }
  const [live, cold] = yield* withColdDb((coldDb) =>
    Effect.all([fingerprint(db, input.sessionID), fingerprint(coldDb, input.sessionID)]),
  )
  if (live !== cold) {
    return { sessionID: input.sessionID, status: "mismatch" as const, moved }
  }
  if (input.force) yield* deleteSession(db, input.sessionID)
  return { sessionID: input.sessionID, status: input.force ? ("archived" as const) : ("copied" as const), moved }
})

export const restoreSession = Effect.fn("SessionCold.restoreSession")(function* (input: {
  sessionID: SessionID
  batch: number
  force: boolean
}) {
  const { db } = yield* Database.Service
  const moved = yield* withColdDb((cold) => copySessionRows(cold, db, input.sessionID, input.batch))
  if (moved === 0) return { sessionID: input.sessionID, status: "missing" as const, moved }
  const [live, cold] = yield* withColdDb((coldDb) =>
    Effect.all([fingerprint(db, input.sessionID), fingerprint(coldDb, input.sessionID)]),
  )
  if (live !== cold) {
    return { sessionID: input.sessionID, status: "mismatch" as const, moved }
  }
  yield* db
    .update(SessionTable)
    .set({ time_archived: null })
    .where(eq(SessionTable.id, input.sessionID))
    .run()
    .pipe(Effect.orDie)
  if (input.force) yield* withColdDb((coldDb) => deleteSession(coldDb, input.sessionID))
  return { sessionID: input.sessionID, status: "restored" as const, moved }
})

const partIdOf = (data: unknown): string | undefined => {
  if (typeof data !== "object" || data === null) return undefined
  const part = (data as Record<string, unknown>)["part"]
  if (typeof part !== "object" || part === null) return undefined
  const id = (part as Record<string, unknown>)["id"]
  return typeof id === "string" ? id : undefined
}

export const compactSessionEvents = Effect.fn("SessionCold.compactSessionEvents")(function* (input: {
  db: ColdDb
  sessionID: SessionID
  batch: number
}) {
  // Keep every non-part event plus the latest part.updated per part id.
  // Streaming deltas rewrite the full part each time, so superseded versions
  // are pure overhead once the projection has consumed them.
  const events = yield* input.db
    .select({ id: EventTable.id, type: EventTable.type, seq: EventTable.seq, data: EventTable.data })
    .from(EventTable)
    .where(eq(EventTable.aggregate_id, input.sessionID))
    .orderBy(asc(EventTable.seq))
    .all()
    .pipe(Effect.orDie)
  const latestByPart = new Map<string, { id: string; seq: number }>()
  for (const event of events) {
    if (event.type !== "message.part.updated.1") continue
    const partID = partIdOf(event.data)
    if (partID === undefined) continue
    const current = latestByPart.get(partID)
    if (!current || event.seq > current.seq) latestByPart.set(partID, { id: event.id, seq: event.seq })
  }
  const keep = new Set([...latestByPart.values()].map((entry) => entry.id))
  const stale = events.filter((event) => event.type === "message.part.updated.1" && !keep.has(event.id))
  for (const group of chunked(
    stale.map((event) => event.id),
    input.batch,
  )) {
    yield* input.db.delete(EventTable).where(inArray(EventTable.id, group)).run().pipe(Effect.orDie)
  }
  return { sessionID: input.sessionID, kept: events.length - stale.length, removed: stale.length }
})

export const compactColdSession = Effect.fn("SessionCold.compactColdSession")(function* (input: {
  sessionID: SessionID
  batch: number
}) {
  return yield* withColdDb((cold) =>
    compactSessionEvents({ db: cold, sessionID: input.sessionID, batch: input.batch }),
  )
})

export const verifySession = Effect.fn("SessionCold.verifySession")(function* (sessionID: SessionID) {
  const { db } = yield* Database.Service
  const [live, cold] = yield* withColdDb((coldDb) =>
    Effect.all([fingerprint(db, sessionID), fingerprint(coldDb, sessionID)]),
  )
  return { sessionID, match: live === cold, live, cold }
})

export * as SessionCold from "./cold"
