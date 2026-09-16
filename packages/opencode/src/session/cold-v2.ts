// Second-generation cold storage: content-addressed, compressed, self-verifying
// archives for idle sessions. Succeeds the v1 row-copy in `./cold`
// (`SessionCold`), which moves identical-schema rows to `opencode-cold.db`.
//
// v2 packs large `part`/`event` JSON payloads into a shared `blob` store
// (deduped by sha256, zstd-compressed) plus an order-template table (`tpl`)
// that reproduces the original key order byte-for-byte on restore. Small rows
// stay inline. A `meta` manifest with chained hashes makes any tampering loud.
//
// On-disk `meta.version` is `"4"`: the format is shared with the reference
// Python tooling (`repack4`/`restore4`), and archives are cross-readable with
// two known limits, both enforced loudly at restore time:
//   - Archives holding `zstd-9-dict` blobs need a zstd dictionary reader.
//     Node.js reads them; Bun's bundled zstd rejects cross-runtime dictionaries
//     ("Dictionary mismatch"), so Bun restores such archives with a clear
//     error. The TS packer writes plain `zstd-9` only (neither runtime exposes
//     dictionary training), keeping TS-written archives portable everywhere.
//   - Canonical JSON is `JSON.stringify` with explicit insertion order. The
//     live database is JS-written, so TS restores match the baseline by
//     construction. Python uses stdlib dumps, which formats some floats
//     differently (`1e-07` vs `1e-7`); the real corpus contains no such floats
//     in packed rows (3M-row exact proof), but float-torture payloads are only
//     guaranteed TS-to-TS.
//
// Conversion contract: the v1 source file is never opened writable by this
// module. `pack` snapshots it (live database via `VACUUM INTO`, which is
// WAL-safe; offline files via byte copy after refusing present `-wal`/`-shm`
// sidecars) and packs the snapshot. Publish is atomic (tmp + fsync + rename)
// and happens only after an in-process self-verify restores the archive and
// byte-compares it against the source: 0 diffs or nothing ships.

import { createHash } from "node:crypto"
import { zstdCompressSync, zstdDecompressSync } from "node:zlib"
import { Schema } from "effect"

export class ColdV2Error extends Schema.TaggedErrorClass<ColdV2Error>()("ColdV2Error", {
  message: Schema.String,
}) {}

const fail = (message: string): never => {
  throw new ColdV2Error({ message })
}

export const FORMAT_VERSION = "4"
export const MIN_BYTES_DEFAULT = 2048
export const IDS = ["id", "sessionID", "messageID"] as const
export const PU1 = "message.part.updated.1"
export const CODECS = ["zstd-9", "zstd-9-dict"] as const

type TableKey = readonly [table: string, key: string, sessionColumn: string]
export const TABLE_KEYS: readonly TableKey[] = [
  ["session", "id", "id"],
  ["message", "id", "session_id"],
  ["part", "id", "session_id"],
  ["event", "id", "aggregate_id"],
  ["session_message", "id", "session_id"],
  ["event_sequence", "aggregate_id", "aggregate_id"],
  ["todo", "rowid", "session_id"],
  ["session_input", "session_id", "session_id"],
  ["session_context_epoch", "session_id", "session_id"],
]

export type Json = string | number | boolean | null | Json[] | { [key: string]: Json }

const isObject = (value: unknown): value is { [key: string]: Json } =>
  typeof value === "object" && value !== null && !Array.isArray(value)

export const parseJson = (text: string): Json => JSON.parse(text) as Json

// Canonical dump: JS baseline bytes are `JSON.stringify` output, so the only
// safe canonical form is `JSON.stringify` itself with explicitly ordered keys.
// Never pretty-print, never sort floats, never touch escaping.
export const canonJson = (value: Json): string => JSON.stringify(value)

// Sort keys recursively; strip wrapper ids ONLY at the payload top level.
// Nested ids (e.g. attachment records carrying id/sessionID/messageID) are
// data and must survive, or distinct payloads collapse to one blob.
export const canonValue = (value: Json, depth = 0): Json => {
  if (Array.isArray(value)) return value.map((item) => canonValue(item, depth + 1))
  if (!isObject(value)) return value
  const out: { [key: string]: Json } = {}
  for (const key of Object.keys(value).sort()) {
    if (depth === 0 && (IDS as readonly string[]).includes(key)) continue
    const child = value[key]
    if (child === undefined) continue
    out[key] = canonValue(child, depth + 1)
  }
  return out
}

// ---------------------------------------------------------------- templates
export type Ctx = "P" | "E"
export interface TemplateOrder {
  readonly order: readonly string[]
  count: number
}
export interface TemplateEntry {
  readonly ctx: Ctx
  readonly type: string
  readonly tool: string
  readonly shape: readonly string[]
  readonly path: readonly string[]
  readonly orders: TemplateOrder[]
}
export type TemplateStore = Map<string, TemplateEntry>

export const templateKey = (ctx: Ctx, type: string, tool: string, shape: readonly string[], path: readonly string[]): string =>
  JSON.stringify([ctx, type, tool, [...shape].sort(), [...path]])

const idSet = new Set<string>(IDS as readonly string[])

// Exact order comparison (join-based compares collide on ["ab","c"] vs ["a","bc"]).
const sameOrder = (a: readonly string[], b: readonly string[]): boolean =>
  a.length === b.length && a.every((key, i) => key === b[i])

export const walkTemplates = (
  store: TemplateStore,
  ctx: Ctx,
  type: string,
  tool: string,
  shape: readonly string[],
  path: readonly string[],
  value: { [key: string]: Json },
): void => {
  const keys = path.length === 0 ? Object.keys(value).filter((key) => !idSet.has(key)) : Object.keys(value)
  const mapKey = templateKey(ctx, type, tool, shape, path)
  let entry = store.get(mapKey)
  if (!entry) {
    entry = { ctx, type, tool, shape: [...shape].sort(), path: [...path], orders: [] }
    store.set(mapKey, entry)
  }
  const seen = entry.orders.find((item) => item.order.length === keys.length && item.order.every((key, i) => key === keys[i]))
  if (seen) seen.count += 1
  else entry.orders.push({ order: [...keys], count: 1 })
  for (const [key, child] of Object.entries(value)) {
    if (isObject(child)) walkTemplates(store, ctx, type, tool, shape, [...path, key], child)
    else if (Array.isArray(child)) {
      for (const item of child) {
        if (isObject(item)) walkTemplates(store, ctx, type, tool, shape, [...path, key], item)
      }
    }
  }
}

const ordersFor = (store: TemplateStore, ctx: Ctx, type: string, tool: string, shape: readonly string[], path: readonly string[]) =>
  store.get(templateKey(ctx, type, tool, shape, path))?.orders

// Exactly one recorded order whose key SET equals the row's keys. Zero or
// contradictory orders mean raw-verbatim fallback, never guessing.
export const templateOrder = (
  store: TemplateStore,
  ctx: Ctx,
  type: string,
  tool: string,
  shape: readonly string[],
  path: readonly string[],
  keys: readonly string[],
): readonly string[] | undefined => {
  const orders = ordersFor(store, ctx, type, tool, shape, path)
  if (!orders) return undefined
  const matches = orders.filter(
    (item) => item.order.length === keys.length && item.order.every((key) => keys.includes(key)),
  )
  if (matches.length !== 1) return undefined
  return matches[0]?.order
}

// Every dict node must have an exact template order AND already be in that
// order. Minority-order variants go RAW so restore reproduces original bytes.
export const isReorderable = (
  store: TemplateStore,
  ctx: Ctx,
  value: { [key: string]: Json },
  type: string,
  tool: string,
  shape: readonly string[],
): boolean => {
  const stack: { path: readonly string[]; node: { [key: string]: Json } }[] = [{ path: [], node: value }]
  while (stack.length > 0) {
    const frame = stack.pop()
    if (!frame) break
    const keys = Object.keys(frame.node)
    if (keys.length > 0) {
      const expected = templateOrder(store, ctx, type, tool, shape, frame.path, keys)
      if (!expected) return false
      if (!expected.every((key, i) => key === keys[i])) return false
    }
    for (const [key, child] of Object.entries(frame.node)) {
      if (isObject(child)) stack.push({ path: [...frame.path, key], node: child })
      else if (Array.isArray(child)) {
        for (const item of child) {
          if (isObject(item)) stack.push({ path: [...frame.path, key], node: item })
        }
      }
    }
  }
  return true
}

// Payload orders are context-free: P-created blobs may be referenced from
// events and vice versa (shared sha), so try the own context then the other.
// Nested row ids are stripped in canonical blobs but present in templates, so
// also accept a template whose key set minus IDS equals the blob key set.
export const lookupOrder = (
  store: TemplateStore,
  ctx: Ctx,
  type: string,
  tool: string,
  shape: readonly string[],
  path: readonly string[],
  keys: readonly string[],
): { ctx: Ctx; order: readonly string[] } | undefined => {
  const other: Ctx = ctx === "P" ? "E" : "P"
  for (const candidate of [ctx, other] as const) {
    const order = templateOrder(store, candidate, type, tool, shape, path, keys)
    if (order) return { ctx: candidate, order }
  }
  for (const candidate of [ctx, other] as const) {
    const entry = store.get(templateKey(candidate, type, tool, shape, path))
    if (!entry) continue
    const matches = entry.orders.filter((item) => {
      const filtered = item.order.filter((key) => !idSet.has(key))
      return filtered.length === keys.length && filtered.every((key) => keys.includes(key))
    })
    if (matches.length === 1 && matches[0]) {
      return { ctx: candidate, order: matches[0].order.filter((key) => (keys as readonly string[]).includes(key)) }
    }
  }
  return undefined
}

const reorderChild = (
  store: TemplateStore,
  ctx: Ctx,
  type: string,
  tool: string,
  shape: readonly string[],
  path: readonly string[],
  value: Json,
  rowid: string,
): Json => {
  if (Array.isArray(value)) return value.map((item) => reorderChild(store, ctx, type, tool, shape, path, item, rowid))
  if (!isObject(value)) return value
  if (Object.keys(value).length === 0) return value
  const hit = lookupOrder(store, ctx, type, tool, shape, path, Object.keys(value))
  if (!hit) {
    fail(
      `no template for row ${rowid}: ctx=${ctx} type=${type} tool=${tool} shape=${[...shape].sort()} path=${path} keys=${Object.keys(value).sort()}`,
    )
  }
  const out: { [key: string]: Json } = {}
  for (const key of hit.order) {
    const child = value[key]
    if (child !== undefined) out[key] = reorderChild(store, hit.ctx, type, tool, shape, [...path, key], child, rowid)
  }
  for (const [key, child] of Object.entries(value)) {
    if (!(key in out)) out[key] = reorderChild(store, hit.ctx, type, tool, shape, [...path, key], child, rowid)
  }
  return out
}

export const reorderValue = (
  store: TemplateStore,
  ctx: Ctx,
  type: string,
  tool: string,
  shape: readonly string[],
  value: { [key: string]: Json },
  rowid = "?",
): { [key: string]: Json } => {
  const hit = lookupOrder(store, ctx, type, tool, shape, [], Object.keys(value))
  if (!hit) fail(`no top template for row ${rowid}: ctx=${ctx} type=${type} tool=${tool} shape=${[...shape].sort()}`)
  const out: { [key: string]: Json } = {}
  for (const key of hit.order) {
    const child = value[key]
    if (child !== undefined) out[key] = reorderChild(store, hit.ctx, type, tool, shape, [key], child, rowid)
  }
  for (const [key, child] of Object.entries(value)) {
    if (!(key in out)) out[key] = reorderChild(store, hit.ctx, type, tool, shape, [key], child, rowid)
  }
  return out
}

const orderOrThrow = (
  store: TemplateStore,
  ctx: Ctx,
  type: string,
  tool: string,
  shape: readonly string[],
  path: readonly string[],
  keys: readonly string[],
  rowid: string,
): readonly string[] => {
  const order = templateOrder(store, ctx, type, tool, shape, path, keys)
  if (!order) {
    fail(
      `no template for row ${rowid}: ctx=${ctx} type=${type} tool=${tool} shape=${[...shape].sort()} path=${path} keys=${[...keys].sort()}`,
    )
  }
  return order
}

// Reorder a payload to template order for canonical (dedupable) storage.
// Callers must check isReorderable first; anything else goes raw-verbatim.
export const emitCanonical = (
  store: TemplateStore,
  ctx: Ctx,
  type: string,
  tool: string,
  shape: readonly string[],
  value: { [key: string]: Json },
  rowid = "?",
): { [key: string]: Json } => {
  const emit = (node: { [key: string]: Json }, path: readonly string[]): { [key: string]: Json } => {
    const out: { [key: string]: Json } = {}
    for (const key of orderOrThrow(store, ctx, type, tool, shape, path, Object.keys(node), rowid)) {
      const child = node[key]
      if (child === undefined) continue
      if (isObject(child)) out[key] = emit(child, [...path, key])
      else if (Array.isArray(child)) {
        out[key] = child.map((item) => (isObject(item) ? emit(item, [...path, key]) : item))
      } else out[key] = child
    }
    return out
  }
  const out: { [key: string]: Json } = {}
  for (const key of orderOrThrow(store, ctx, type, tool, shape, [], Object.keys(value), rowid)) {
    const child = value[key]
    if (child === undefined) continue
    if (isObject(child)) out[key] = emit(child, [key])
    else if (Array.isArray(child)) {
      out[key] = child.map((item) => (isObject(item) ? emit(item, [key]) : item))
    } else out[key] = child
  }
  return out
}

// ------------------------------------------------------- template persistence
export interface TemplateRow {
  readonly ctx: Ctx
  readonly type: string
  readonly tool: string
  readonly shapeJson: string
  readonly pathJson: string
  readonly orderJson: string
  readonly count: number
}

export const storeTemplateRows = (store: TemplateStore): TemplateRow[] => {
  const rows: TemplateRow[] = []
  for (const entry of store.values()) {
    const shapeJson = JSON.stringify([...entry.shape].sort())
    const pathJson = JSON.stringify([...entry.path])
    for (const item of entry.orders) {
      rows.push({
        ctx: entry.ctx,
        type: entry.type,
        tool: entry.tool,
        shapeJson,
        pathJson,
        orderJson: JSON.stringify([...item.order]),
        count: item.count,
      })
    }
  }
  return rows
}

export const hashTemplateRows = (rows: readonly TemplateRow[]): string => {
  const hash = createHash("sha256")
  const lines = rows.map((row) => [row.ctx, row.type, row.tool, row.shapeJson, row.pathJson, row.orderJson, String(row.count)].join("\t"))
  for (const line of lines.sort()) hash.update(line + "\n", "utf8")
  return hash.digest("hex")
}

export const loadTemplateStore = (rows: readonly TemplateRow[]): TemplateStore => {
  const store: TemplateStore = new Map()
  const stringArray = (value: unknown): string[] | undefined => {
    if (!Array.isArray(value)) return undefined
    const out: string[] = []
    for (const item of value) {
      if (typeof item !== "string") return undefined
      out.push(item)
    }
    return out
  }
  for (const row of rows) {
    let shape: string[] | undefined
    let path: string[] | undefined
    let order: string[] | undefined
    try {
      shape = stringArray(JSON.parse(row.shapeJson))
      path = stringArray(JSON.parse(row.pathJson))
      order = stringArray(JSON.parse(row.orderJson))
    } catch {
      fail(`template table has corrupt JSON (ctx=${row.ctx} type=${row.type})`)
    }
    if (!shape || !path || !order) fail(`template table has corrupt shape/path/order (ctx=${row.ctx} type=${row.type})`)
    if ((row.ctx !== "P" && row.ctx !== "E") || typeof row.count !== "number") {
      fail(`template table has corrupt row (ctx=${row.ctx} type=${row.type})`)
    }
    const mapKey = templateKey(row.ctx, row.type, row.tool, shape, path)
    let entry = store.get(mapKey)
    if (!entry) {
      entry = { ctx: row.ctx, type: row.type, tool: row.tool, shape: [...shape].sort(), path: [...path], orders: [] }
      store.set(mapKey, entry)
    }
    entry.orders.push({ order: [...order], count: row.count })
  }
  return store
}

// ------------------------------------------------------------------ hashing
export const sha256Hex = (data: Uint8Array | string): string => createHash("sha256").update(data).digest("hex")

// The raw flag is bound into the digest: flipping raw 0<->1 changes the hash,
// so the flag cannot be altered without tripping restore verification.
export const blobDigest = (raw: boolean, plain: Uint8Array): string => {
  const hash = createHash("sha256")
  hash.update(Buffer.from([raw ? 1 : 0]))
  hash.update(plain)
  return hash.digest("hex")
}

export const ptrHashOf = (entries: readonly (readonly [t: string, id: string, sha: string])[]): string => {
  const hash = createHash("sha256")
  const lines = entries.map(([t, id, sha]) => `${t}|${id}|${sha}\n`)
  for (const line of lines.sort()) hash.update(line, "utf8")
  return hash.digest("hex")
}

const MANIFEST_SKIP = new Set(["manifest_hash", "complete", "published_utc", "verify_ok", "verify_rows"])

export const manifestHashOf = (meta: Readonly<Record<string, string>>): string => {
  const hash = createHash("sha256")
  for (const key of Object.keys(meta).sort()) {
    if (MANIFEST_SKIP.has(key)) continue
    hash.update(`${key}\t${meta[key]}\n`, "utf8")
  }
  return hash.digest("hex")
}

// ------------------------------------------------------------------ pointers
export const isPointerShape = (data: string): boolean => data.startsWith('{"_blob"')

export const parsePointer = (data: string, table: string, rowid: string): string => {
  let parsed: Json
  try {
    parsed = parseJson(data)
  } catch (error) {
    fail(`${table} row ${rowid}: pointer-shaped row is not valid JSON (${String(error).slice(0, 120)}); data=${data.slice(0, 80)}`)
  }
  if (!isObject(parsed) || Object.keys(parsed).length !== 1 || typeof parsed["_blob"] !== "string") {
    fail(`${table} row ${rowid}: pointer-shaped row has wrong shape; data=${data.slice(0, 80)}`)
  }
  const sha = parsed["_blob"] as string
  if (sha.length !== 64 || !/^[0-9a-f]{64}$/.test(sha)) fail(`${table} row ${rowid}: pointer sha malformed: ${sha.slice(0, 80)}`)
  return sha
}

export interface Slim {
  readonly sid: string
  readonly time: Json
  readonly pid: string
  readonly mid: string
  readonly blob: string
}

export const parseSlim = (data: string, rowid: string): Slim => {
  let parsed: Json
  try {
    parsed = parseJson(data)
  } catch (error) {
    fail(`event row ${rowid}: slim is not valid JSON (${String(error).slice(0, 120)})`)
  }
  if (!isObject(parsed) || parsed["_ev"] !== "pu1") fail(`event row ${rowid}: registered slim has wrong shape`)
  const slim = parsed as { [key: string]: Json }
  for (const field of ["sid", "time", "pid", "mid", "blob"] as const) {
    if (slim[field] === undefined) fail(`event row ${rowid}: slim missing field ${field}`)
  }
  if (typeof slim["sid"] !== "string" || typeof slim["pid"] !== "string" || typeof slim["mid"] !== "string" || typeof slim["blob"] !== "string") {
    fail(`event row ${rowid}: slim id fields malformed`)
  }
  return { sid: slim["sid"] as string, time: slim["time"] as Json, pid: slim["pid"] as string, mid: slim["mid"] as string, blob: slim["blob"] as string }
}

// ------------------------------------------------------------------ codec
// Plain zstd only on write. No runtime here exposes dictionary training, and
// trained dictionaries are not portable across zstd builds (Bun rejects
// dictionaries trained elsewhere), so portability wins over the ~10% dicts
// buy. Reads accept zstd-9-dict best-effort via node:zlib.
export const compressPlain = (plain: Uint8Array): Buffer => Buffer.from(zstdCompressSync(plain, { level: 9 }))

export const decompressBlob = (comp: Uint8Array, dict: Uint8Array | undefined, table: string, rowid: string, sha: string): Buffer => {
  try {
    return dict
      ? Buffer.from(zstdDecompressSync(comp, { dictionary: dict }))
      : Buffer.from(zstdDecompressSync(comp))
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    if (/dictionary/i.test(message)) {
      fail(
        `${table} row ${rowid}: blob ${sha.slice(0, 16)} needs a trained zstd dictionary this runtime cannot read (${message.slice(0, 80)}). ` +
          `Restore this archive with the Node.js runtime or the reference Python tooling.`,
      )
    }
    fail(`${table} row ${rowid}: blob ${sha.slice(0, 16)} decompress failed (${message.slice(0, 120)})`)
  }
}

// ------------------------------------------------------------------ raw db
// Minimal driver surface over bun:sqlite (primary: CLI and tests run on Bun)
// with a node:sqlite fallback for Node runtimes. Bulk ETL goes through
// prepared statements here instead of the Effect/Drizzle layer: millions of
// per-row updates do not need tracing spans, and explicit chunked
// transactions bound the journal like the reference pipeline.
export interface RawDb {
  all<T>(query: string, params?: unknown[]): T[]
  get<T>(query: string, params?: unknown[]): T | undefined
  run(query: string, params?: unknown[]): void
  exec(query: string): void
  close(): void
}

interface BunDatabase {
  query(query: string): { all(...params: unknown[]): unknown[]; get(...params: unknown[]): unknown; run(...params: unknown[]): unknown }
  exec(query: string): void
  close(): void
}

interface NodeDatabase {
  prepare(query: string): { all(...params: unknown[]): unknown[]; get(...params: unknown[]): unknown; run(...params: unknown[]): unknown }
  exec(query: string): void
  close(): void
}

const toBuffer = (value: unknown): Buffer => {
  if (Buffer.isBuffer(value)) return value
  if (value instanceof Uint8Array) return Buffer.from(value)
  fail(`expected BLOB bytes, got ${typeof value}`)
}

export const openRawDb = async (filename: string, mode: "ro" | "rw"): Promise<RawDb> => {
  let bunModule: { Database: new (file: string, opts?: Record<string, unknown>) => BunDatabase } | null = null
  try {
    bunModule = (await import("bun:sqlite")) as unknown as { Database: new (file: string, opts?: Record<string, unknown>) => BunDatabase }
  } catch {
    bunModule = null
  }
  if (bunModule) {
    const native = new bunModule.Database(filename, mode === "ro" ? { readonly: true } : { create: true })
    native.exec(`PRAGMA busy_timeout = 30000`)
    return {
      all: <T>(query: string, params: unknown[] = []): T[] => native.query(query).all(...params) as T[],
      get: <T>(query: string, params: unknown[] = []): T | undefined => (native.query(query).get(...params) ?? undefined) as T | undefined,
      run: (query: string, params: unknown[] = []): void => {
        native.query(query).run(...params)
      },
      exec: (query: string): void => {
        native.exec(query)
      },
      close: (): void => native.close(),
    }
  }
  const { DatabaseSync } = (await import("node:sqlite")) as unknown as {
    DatabaseSync: new (file: string, opts?: Record<string, unknown>) => NodeDatabase
  }
  const native = new DatabaseSync(filename, mode === "ro" ? { readOnly: true } : {})
  native.exec(`PRAGMA busy_timeout = 30000`)
  return {
    all: <T>(query: string, params: unknown[] = []): T[] => native.prepare(query).all(...params) as T[],
    get: <T>(query: string, params: unknown[] = []): T | undefined => (native.prepare(query).get(...params) ?? undefined) as T | undefined,
    run: (query: string, params: unknown[] = []): void => {
      native.prepare(query).run(...params)
    },
    exec: (query: string): void => {
      native.exec(query)
    },
    close: (): void => native.close(),
  }
}

export const toDbBuffer = toBuffer

// ------------------------------------------------------------------ engine
export interface RestoreResult {
  readonly parts: number
  readonly events: number
}

export interface Manifest {
  readonly fields: Record<string, string>
  readonly templates: TemplateStore
  readonly envelopeOrder: readonly string[]
  readonly wrapperOrder: readonly string[]
  readonly counts: Record<string, number>
  readonly pointers: number
}

const tableNames = (db: RawDb): Set<string> =>
  new Set(db.all<{ name: string }>(`SELECT name FROM sqlite_master WHERE type = 'table'`).map((row) => row.name))

export const assertLiveLayout = (db: RawDb, label: string): void => {
  const tables = tableNames(db)
  for (const need of ["session", "part", "event", "message"]) {
    if (!tables.has(need)) fail(`base ${label} lacks table ${need}`)
  }
  const packed = ["blob", "zdict", "tpl", "meta", "ptr"].filter((name) => tables.has(name))
  if (packed.length > 0) fail(`base ${label} looks already packed (has ${packed.join(",")}); refusing to repack output`)
  const pointers = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM part WHERE data LIKE '{"_blob%'`)?.n ?? 0
  const slims = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM event WHERE data LIKE '{"_ev%'`)?.n ?? 0
  if (pointers > 0 || slims > 0) {
    fail(`base ${label} contains ${pointers} pointer-shaped part rows and ${slims} slim-shaped event rows; refusing packed input`)
  }
}

export const filterSessions = (db: RawDb, allow: readonly string[] | null): Set<string> | null => {
  if (!allow) {
    const total = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM session`)?.n ?? 0
    console.log(`filter: keeping all ${total} sessions`)
    return null
  }
  const keep = [...allow].sort()
  // Empty allow-list archives nothing: `NOT IN ()` is a syntax error, so
  // delete unconditionally instead.
  if (keep.length === 0) {
    for (const table of ["session_input", "session_context_epoch", "session_message", "todo", "part", "message", "session", "event", "event_sequence"]) {
      db.run(`DELETE FROM "${table}"`)
    }
    console.log(`filter: keeping 0 sessions`)
    return new Set<string>()
  }
  const placeholders = keep.map(() => "?").join(",")
  const pairs: readonly (readonly [table: string, column: string])[] = [
    ["session_input", "session_id"],
    ["session_context_epoch", "session_id"],
    ["session_message", "session_id"],
    ["todo", "session_id"],
    ["part", "session_id"],
    ["message", "session_id"],
    ["session", "id"],
  ]
  for (const [table, column] of pairs) db.run(`DELETE FROM "${table}" WHERE "${column}" NOT IN (${placeholders})`, [...keep])
  db.run(`DELETE FROM event WHERE aggregate_id NOT IN (${placeholders})`, [...keep])
  db.run(`DELETE FROM event_sequence WHERE aggregate_id NOT IN (${placeholders})`, [...keep])
  const total = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM session`)?.n ?? 0
  console.log(`filter: keeping ${total} sessions`)
  return new Set(keep)
}

export interface Learned {
  readonly store: TemplateStore
  readonly envelopeOrder: readonly string[]
  readonly wrapperOrder: readonly string[]
}

export const learnTemplates = (db: RawDb): Learned => {
  const store: TemplateStore = new Map()
  let parts = 0
  let after: string | null = null
  for (;;) {
    const rows =
      after === null
        ? db.all<{ id: string; data: string }>(`SELECT id, data FROM part ORDER BY id LIMIT 20000`)
        : db.all<{ id: string; data: string }>(`SELECT id, data FROM part WHERE id > ? ORDER BY id LIMIT 20000`, [after])
    if (rows.length === 0) break
    for (const row of rows) {
      let parsed: Json
      try {
        parsed = parseJson(row.data)
      } catch (error) {
        fail(`template learn: part row unparseable (${String(error).slice(0, 100)})`)
      }
      if (!isObject(parsed)) fail("template learn: part row top-level not an object")
      const type = String(parsed["type"] ?? "?")
      const tool = String(parsed["tool"] ?? "")
      walkTemplates(store, "P", type, tool, Object.keys(parsed), [], parsed)
      parts += 1
      after = row.id
    }
    if (rows.length < 20000) break
  }
  console.log(`part templates: ${parts} rows`)
  let envelopeOrder: readonly string[] | null = null
  let wrapperOrder: readonly string[] | null = null
  let pu1 = 0
  let eventAfter = ""
  for (;;) {
    const rows = db.all<{ id: string; data: string }>(
      `SELECT id, data FROM event WHERE type = '${PU1}' AND id > ? ORDER BY id LIMIT 20000`,
      [eventAfter],
    )
    if (rows.length === 0) break
    for (const row of rows) {
      let parsed: Json
      try {
        parsed = parseJson(row.data)
      } catch (error) {
        fail(`template learn: event ${row.id} unparseable (${String(error).slice(0, 100)})`)
      }
      if (!isObject(parsed)) fail(`template learn: event ${row.id} is not an object`)
      const part = parsed["part"]
      if (!isObject(part)) fail(`template learn: event ${row.id} has no part object`)
      const keys = Object.keys(parsed)
      const here = Object.keys(part).filter((key) => idSet.has(key))
      if (!envelopeOrder || !wrapperOrder) {
        envelopeOrder = keys
        wrapperOrder = here
        if (here.length === 0) fail(`template learn: event ${row.id} part has no ids`)
      } else {
        if (!sameOrder(keys, envelopeOrder)) {
          fail(`template learn: event ${row.id} envelope order ${keys} != ${envelopeOrder}`)
        }
        const positions = Object.keys(part)
        if (!sameOrder(here, wrapperOrder) || !sameOrder(positions.slice(0, here.length), here)) {
          fail(`template learn: event ${row.id} wrapper order ${here} != ${wrapperOrder}`)
        }
      }
      const type = String(part["type"] ?? "?")
      const tool = String(part["tool"] ?? "")
      const payload = Object.fromEntries(Object.entries(part).filter(([key]) => !idSet.has(key)))
      walkTemplates(store, "E", type, tool, Object.keys(payload), [], payload)
      pu1 += 1
      eventAfter = row.id
    }
    if (rows.length < 20000) break
  }
  console.log(`event templates: ${pu1} pu1 rows; envelope=${envelopeOrder ?? []} wrapper=${wrapperOrder ?? []}`)
  return { store, envelopeOrder: envelopeOrder ?? [], wrapperOrder: wrapperOrder ?? [] }
}

const storeBlob = (db: RawDb, comp: Buffer, plain: Buffer, raw: boolean): { sha: string; isNew: boolean } => {
  const sha = blobDigest(raw, plain)
  const exists = db.get<{ one: number }>(`SELECT 1 AS one FROM blob WHERE sha256 = ?`, [sha])
  if (!exists) {
    db.run(`INSERT INTO blob (sha256, bytes, len, codec, raw, dict_id) VALUES (?, ?, ?, 'zstd-9', ?, NULL)`, [
      sha,
      comp,
      plain.length,
      raw ? 1 : 0,
    ])
    return { sha, isNew: true }
  }
  return { sha, isNew: false }
}

export const packParts = (db: RawDb, store: TemplateStore, minBytes: number): { pointers: number; rawFallback: number } => {
  let seen = 0
  let pointers = 0
  let rawFallback = 0
  let after = ""
  for (;;) {
    const rows = db.all<{ id: string; data: string }>(`SELECT id, data FROM part WHERE id > ? ORDER BY id LIMIT 10000`, [after])
    if (rows.length === 0) break
    const updates: string[][] = []
    const registry: string[][] = []
    for (const row of rows) {
      seen += 1
      after = row.id
      if (Buffer.byteLength(row.data, "utf8") < minBytes) continue
      let parsed: Json
      try {
        parsed = parseJson(row.data)
      } catch (error) {
        fail(`part ${row.id}: unparseable (${String(error).slice(0, 100)})`)
      }
      if (!isObject(parsed)) fail(`part ${row.id}: top-level JSON is not an object`)
      const intruders = Object.keys(parsed).filter((key) => idSet.has(key))
      if (intruders.length > 0) fail(`part ${row.id}: payload carries top-level wrapper ids ${intruders} (schema drift)`)
      const type = String(parsed["type"] ?? "?")
      const tool = String(parsed["tool"] ?? "")
      const shape = Object.keys(parsed)
      let sha: string
      if (isReorderable(store, "P", parsed, type, tool, shape)) {
        const canonical = Buffer.from(canonJson(canonValue(parsed)), "utf8")
        sha = storeBlob(db, compressPlain(canonical), canonical, false).sha
      } else {
        const verbatim = Buffer.from(row.data, "utf8")
        sha = storeBlob(db, compressPlain(verbatim), verbatim, true).sha
        rawFallback += 1
      }
      updates.push([JSON.stringify({ _blob: sha }), row.id])
      registry.push(["part", row.id, sha])
      pointers += 1
    }
    db.exec("BEGIN IMMEDIATE")
    try {
      for (const [data, id] of updates) db.run(`UPDATE part SET data = ? WHERE id = ?`, [data, id])
      for (const [t, id, sha] of registry) db.run(`INSERT INTO ptr (t, id, sha) VALUES (?, ?, ?)`, [t, id, sha])
      db.exec("COMMIT")
    } catch (error) {
      try {
        db.exec("ROLLBACK")
      } catch {
        // Best-effort; the tmp file is unpublished on failure.
      }
      if (error instanceof ColdV2Error) throw error
      fail(`part pack chunk failed: ${error instanceof Error ? error.message.slice(0, 160) : String(error).slice(0, 160)}`)
    }
    if (seen % 200000 < 10000) console.log(`  parts ...${seen} ptr=${pointers} rawfb=${rawFallback}`)
  }
  console.log(`parts: ${seen} rows, pointers=${pointers}, raw-fallback=${rawFallback}`)
  return { pointers, rawFallback }
}

export const packEvents = (
  db: RawDb,
  store: TemplateStore,
  envelopeOrder: readonly string[],
  wrapperOrder: readonly string[],
): { slims: number; rawFallback: number } => {
  let slims = 0
  let rawFallback = 0
  let after = ""
  for (;;) {
    const rows = db.all<{ id: string; data: string }>(
      `SELECT id, data FROM event WHERE type = '${PU1}' AND id > ? ORDER BY id LIMIT 2000`,
      [after],
    )
    if (rows.length === 0) break
    const updates: string[][] = []
    const registry: string[][] = []
    for (const row of rows) {
      after = row.id
      let parsed: Json
      try {
        parsed = parseJson(row.data)
      } catch (error) {
        fail(`event ${row.id}: unparseable (${String(error).slice(0, 100)})`)
      }
      if (!isObject(parsed)) fail(`event ${row.id}: pu1 row is not an object`)
      const part = parsed["part"]
      if (!isObject(part)) fail(`event ${row.id}: pu1 row has no part object`)
      if (!sameOrder(Object.keys(parsed), envelopeOrder)) {
        fail(`event ${row.id}: envelope order ${Object.keys(parsed)} != ${envelopeOrder} (schema drift)`)
      }
      const here = Object.keys(part).filter((key) => idSet.has(key))
      const positions = Object.keys(part)
      if (!sameOrder(here, [...wrapperOrder]) || !sameOrder(positions.slice(0, here.length), here)) {
        fail(`event ${row.id}: wrapper order ${here} != ${wrapperOrder} as prefix (schema drift)`)
      }
      const type = String(part["type"] ?? "?")
      const tool = String(part["tool"] ?? "")
      const payload = Object.fromEntries(Object.entries(part).filter(([key]) => !idSet.has(key)))
      const shape = Object.keys(payload)
      let sha: string
      if (isReorderable(store, "E", payload, type, tool, shape)) {
        const canonical = Buffer.from(canonJson(canonValue(payload)), "utf8")
        sha = storeBlob(db, compressPlain(canonical), canonical, false).sha
      } else {
        const verbatim = Buffer.from(canonJson(payload), "utf8")
        sha = storeBlob(db, compressPlain(verbatim), verbatim, true).sha
        rawFallback += 1
      }
      const slim: { [key: string]: Json } = {
        _ev: "pu1",
        sid: parsed["sessionID"] as Json,
        time: parsed["time"] as Json,
        pid: part["id"] as Json,
        mid: part["messageID"] as Json,
        blob: sha,
      }
      updates.push([JSON.stringify(slim), row.id])
      registry.push(["event", row.id, sha])
      slims += 1
    }
    db.exec("BEGIN IMMEDIATE")
    try {
      for (const [data, id] of updates) db.run(`UPDATE event SET data = ? WHERE id = ?`, [data, id])
      for (const [t, id, sha] of registry) db.run(`INSERT INTO ptr (t, id, sha) VALUES (?, ?, ?)`, [t, id, sha])
      db.exec("COMMIT")
    } catch (error) {
      try {
        db.exec("ROLLBACK")
      } catch {
        // Best-effort; the tmp file is unpublished on failure.
      }
      if (error instanceof ColdV2Error) throw error
      fail(`event pack chunk failed: ${error instanceof Error ? error.message.slice(0, 160) : String(error).slice(0, 160)}`)
    }
    if (slims % 200000 < 2000) console.log(`  events ...${slims} rawfb=${rawFallback}`)
  }
  console.log(`events: ${slims} slimmed, raw-fallback=${rawFallback}`)
  return { slims, rawFallback }
}

// ------------------------------------------------------------------ manifest
export const writeManifestBase = (
  db: RawDb,
  templateHash: string,
  templateRows: number,
  envelopeOrder: readonly string[],
  wrapperOrder: readonly string[],
  minBytes: number,
): void => {
  db.exec(`CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT)`)
  const meta: Record<string, string> = {
    version: FORMAT_VERSION,
    backend: "ts-json",
    created_utc: new Date().toISOString().replace(/\.\d+Z$/, "Z"),
    min_bytes: String(minBytes),
    template_hash: templateHash,
    tpl_rows: String(templateRows),
    envelope_order: JSON.stringify([...envelopeOrder]),
    wrapper_order: JSON.stringify([...wrapperOrder]),
    dicts: "{}",
  }
  for (const [key, value] of Object.entries(meta)) db.run(`INSERT OR REPLACE INTO meta (k, v) VALUES (?, ?)`, [key, value])
}

export const setMeta = (db: RawDb, key: string, value: string): void => {
  db.run(`INSERT OR REPLACE INTO meta (k, v) VALUES (?, ?)`, [key, value])
}

export const assertQuickCheck = (db: RawDb, label: string): void => {
  const check = db.get<Record<string, unknown>>(`PRAGMA quick_check`)
  const verdict = check ? Object.values(check)[0] : undefined
  if (verdict !== "ok") fail(`${label} quick_check failed: ${JSON.stringify(check).slice(0, 160)}`)
}

export const readMeta = (db: RawDb): Record<string, string> =>
  Object.fromEntries(db.all<{ k: string; v: string }>(`SELECT k, v FROM meta`).map((row) => [row.k, row.v]))

export const computePtrHash = (db: RawDb): string => {
  const hash = createHash("sha256")
  let afterT = ""
  let afterId = ""
  for (;;) {
    const rows = db.all<{ t: string; id: string; sha: string }>(
      `SELECT t, id, sha FROM ptr WHERE (t > ? OR (t = ? AND id > ?)) ORDER BY t, id LIMIT 50000`,
      [afterT, afterT, afterId],
    )
    if (rows.length === 0) break
    for (const row of rows) {
      hash.update(`${row.t}|${row.id}|${row.sha}\n`, "utf8")
      afterT = row.t
      afterId = row.id
    }
    if (rows.length < 50000) break
  }
  return hash.digest("hex")
}

export const computeInlineHash = (db: RawDb): { hash: string; count: number } => {
  const hash = createHash("sha256")
  let count = 0
  const parts = new Set(db.all<{ id: string }>(`SELECT id FROM ptr WHERE t = 'part'`).map((row) => row.id))
  let after = ""
  for (;;) {
    const rows = db.all<{ id: string; data: string }>(`SELECT id, data FROM part WHERE id > ? ORDER BY id LIMIT 20000`, [after])
    if (rows.length === 0) break
    for (const row of rows) {
      after = row.id
      if (parts.has(row.id)) continue
      hash.update(row.id, "utf8")
      hash.update("\0", "utf8")
      hash.update(row.data, "utf8")
      hash.update("\n", "utf8")
      count += 1
    }
    if (rows.length < 20000) break
  }
  const slims = new Set(db.all<{ id: string }>(`SELECT id FROM ptr WHERE t = 'event'`).map((row) => row.id))
  after = ""
  for (;;) {
    const rows = db.all<{ id: string; data: string }>(`SELECT id, data FROM event WHERE id > ? ORDER BY id LIMIT 20000`, [after])
    if (rows.length === 0) break
    for (const row of rows) {
      after = row.id
      if (slims.has(row.id)) continue
      hash.update(row.id, "utf8")
      hash.update("\0", "utf8")
      hash.update(row.data, "utf8")
      hash.update("\n", "utf8")
      count += 1
    }
    if (rows.length < 20000) break
  }
  console.log(`inline: ${count} rows hashed`)
  return { hash: hash.digest("hex"), count }
}

// ------------------------------------------------------------------ pack file
export interface PackFileStats {
  readonly sessions: number
  readonly partPointers: number
  readonly partRawFallback: number
  readonly eventSlims: number
  readonly eventRawFallback: number
  readonly blobs: number
  readonly inlineRows: number
}

// Packs the database file in place. The caller owns snapshotting and publish:
// open a writable copy, never the v1 original.
export const packFile = async (filename: string, allow: readonly string[] | null, minBytes: number): Promise<PackFileStats> => {
  const db = await openRawDb(filename, "rw")
  try {
    db.exec(`PRAGMA journal_mode = DELETE`)
    db.exec(`PRAGMA synchronous = FULL`)
    db.exec(`PRAGMA busy_timeout = 30000`)
    assertLiveLayout(db, filename)
    filterSessions(db, allow)
    const sessions = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM session`)?.n ?? 0
    const { store, envelopeOrder, wrapperOrder } = learnTemplates(db)
    const rows = storeTemplateRows(store)
    db.exec(`CREATE TABLE tpl (ctx TEXT, type TEXT, tool TEXT, shape_json TEXT, path_json TEXT, order_json TEXT, cnt INTEGER)`)
    for (let i = 0; i < rows.length; i += 5000) {
      db.exec("BEGIN IMMEDIATE")
      try {
        for (const row of rows.slice(i, i + 5000)) {
          db.run(`INSERT INTO tpl VALUES (?, ?, ?, ?, ?, ?, ?)`, [
            row.ctx,
            row.type,
            row.tool,
            row.shapeJson,
            row.pathJson,
            row.orderJson,
            row.count,
          ])
        }
        db.exec("COMMIT")
      } catch (error) {
        try {
          db.exec("ROLLBACK")
        } catch {
          // Best-effort; tmp is unpublished on failure.
        }
        fail(`template store failed: ${error instanceof Error ? error.message.slice(0, 160) : String(error).slice(0, 160)}`)
      }
    }
    const templateHash = hashTemplateRows(rows)
    writeManifestBase(db, templateHash, rows.length, envelopeOrder, wrapperOrder, minBytes)
    db.exec(`CREATE TABLE ptr (t TEXT, id TEXT, sha TEXT, PRIMARY KEY (t, id))`)
    db.exec(`CREATE TABLE blob (sha256 TEXT PRIMARY KEY, bytes BLOB, len INTEGER, codec TEXT, raw INTEGER, dict_id TEXT)`)
    const parts = packParts(db, store, minBytes)
    const events = packEvents(db, store, envelopeOrder, wrapperOrder)
    setMeta(db, "ptr_hash", computePtrHash(db))
    const inline = computeInlineHash(db)
    setMeta(db, "inline_hash", inline.hash)
    setMeta(db, "inline_count", String(inline.count))
    setMeta(db, "blob_count", String(db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM blob`)?.n ?? 0))
    // No dictionary training on this runtime (node:zlib exposes no trainer,
    // and trained dicts are not portable across zstd builds). The table
    // exists for format compatibility with reference-tooling archives.
    db.exec(`CREATE TABLE IF NOT EXISTS zdict (id TEXT PRIMARY KEY, project TEXT, bytes BLOB)`)
    for (const [table] of TABLE_KEYS) {
      try {
        const count = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM "${table}"`)?.n
        if (count !== undefined) setMeta(db, `count_${table}`, String(count))
      } catch {
        // Tables absent from older layouts simply get no count row.
      }
    }
    console.log("VACUUM ...")
    db.exec(`VACUUM`)
    setMeta(db, "manifest_hash", manifestHashOf(readMeta(db)))
    assertQuickCheck(db, "packed tmp")
    const blobs = Number(readMeta(db)["blob_count"] ?? "0")
    console.log(`packed: blob=${blobs} dicts=0 inline=${inline.count}`)
    return {
      sessions,
      partPointers: parts.pointers,
      partRawFallback: parts.rawFallback,
      eventSlims: events.slims,
      eventRawFallback: events.rawFallback,
      blobs,
      inlineRows: inline.count,
    }
  } finally {
    db.close()
  }
}

// ------------------------------------------------------------------ restore
export const loadManifest = (db: RawDb, filename: string, allowIncomplete: boolean): Manifest => {
  const tables = tableNames(db)
  for (const need of ["meta", "tpl", "blob", "zdict", "ptr"]) {
    if (!tables.has(need)) fail(`${filename} is not a completed v2 archive (missing table ${need})`)
  }
  const fields = readMeta(db)
  if (fields["version"] !== FORMAT_VERSION) fail(`unsupported archive version ${fields["version"]} (this tool reads v${FORMAT_VERSION})`)
  if (fields["complete"] !== "1" && !allowIncomplete) {
    fail(`${filename} is not a completed archive (no manifest complete marker; partial or failed build?)`)
  }
  const rows = db.all<TemplateRow>(`SELECT ctx, type, tool, shape_json AS shapeJson, path_json AS pathJson, order_json AS orderJson, cnt AS "count" FROM tpl`)
  const templates = loadTemplateStore(rows)
  if (hashTemplateRows(storeTemplateRows(templates)) !== fields["template_hash"]) {
    fail(`template table fails manifest hash (archive tampered or corrupt)`)
  }
  if (!fields["manifest_hash"]) fail("archive manifest has no manifest_hash (not a v2 build?)")
  if (manifestHashOf(fields) !== fields["manifest_hash"]) fail("manifest hash mismatch -- manifest rows tampered")
  const hash = createHash("sha256")
  let afterT = ""
  let afterId = ""
  for (;;) {
    const batch = db.all<{ t: string; id: string; sha: string }>(
      `SELECT t, id, sha FROM ptr WHERE (t > ? OR (t = ? AND id > ?)) ORDER BY t, id LIMIT 50000`,
      [afterT, afterT, afterId],
    )
    if (batch.length === 0) break
    for (const row of batch) {
      hash.update(`${row.t}|${row.id}|${row.sha}\n`, "utf8")
      afterT = row.t
      afterId = row.id
    }
    if (batch.length < 50000) break
  }
  if (hash.digest("hex") !== fields["ptr_hash"]) fail("pointer registry hash mismatch -- ptr table tampered")
  const codecs = new Set(db.all<{ codec: string }>(`SELECT DISTINCT codec FROM blob`).map((row) => row.codec))
  for (const codec of codecs) {
    if (!(CODECS as readonly string[]).includes(codec)) fail(`archive uses unknown codec ${codec} (allow-list ${CODECS.join(",")})`)
  }
  let envelopeOrder: readonly string[]
  let wrapperOrder: readonly string[]
  try {
    envelopeOrder = JSON.parse(fields["envelope_order"] ?? "[]") as string[]
    wrapperOrder = JSON.parse(fields["wrapper_order"] ?? "[]") as string[]
  } catch {
    fail("archive manifest has corrupt envelope/wrapper orders")
  }
  if (!Array.isArray(envelopeOrder) || !Array.isArray(wrapperOrder) || wrapperOrder.length === 0) {
    fail("archive manifest has corrupt envelope/wrapper orders")
  }
  const counts: Record<string, number> = {}
  for (const [key, value] of Object.entries(fields)) {
    if (key.startsWith("count_")) counts[key.slice("count_".length)] = Number(value)
  }
  const pointers = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM ptr`)?.n ?? 0
  return { fields, templates, envelopeOrder, wrapperOrder, counts, pointers }
}

const readBlob = (
  db: RawDb,
  dictCache: Map<string, Buffer>,
  table: string,
  rowid: string,
  sha: string,
): { plain: Buffer; raw: boolean } => {
  const row = db.get<{ bytes: unknown; codec: string; dict_id: string | null; len: number; raw: number }>(
    `SELECT bytes, codec, dict_id, len, raw FROM blob WHERE sha256 = ?`,
    [sha],
  )
  if (!row) fail(`${table} row ${rowid}: pointer sha ${sha} has no blob row`)
  if (!(CODECS as readonly string[]).includes(row.codec)) fail(`${table} row ${rowid}: blob ${sha.slice(0, 16)} uses unknown codec ${row.codec}`)
  const comp = toDbBuffer(row.bytes)
  const raw = row.raw === 1
  let plain: Buffer
  if (row.codec === "zstd-9") {
    if (row.dict_id !== null && row.dict_id !== undefined) {
      fail(`${table} row ${rowid}: blob ${sha.slice(0, 16)} codec=zstd-9 but dict_id=${row.dict_id}`)
    }
    plain = decompressBlob(comp, undefined, table, rowid, sha)
  } else {
    if (!row.dict_id) fail(`${table} row ${rowid}: blob ${sha.slice(0, 16)} codec=zstd-9-dict but dict_id is NULL`)
    let dict = dictCache.get(row.dict_id)
    if (!dict) {
      const dictRow = db.get<{ bytes: unknown }>(`SELECT bytes FROM zdict WHERE id = ?`, [row.dict_id])
      if (!dictRow) fail(`${table} row ${rowid}: blob ${sha.slice(0, 16)} needs missing dict ${row.dict_id}`)
      dict = toDbBuffer(dictRow.bytes)
      dictCache.set(row.dict_id, dict)
    }
    plain = decompressBlob(comp, dict, table, rowid, sha)
  }
  if (blobDigest(raw, plain) !== sha) {
    fail(`${table} row ${rowid}: blob sha mismatch (stored=${sha.slice(0, 16)}; bytes or raw-flag tampered)`)
  }
  if (plain.length !== row.len) fail(`${table} row ${rowid}: blob len mismatch (stored=${row.len} actual=${plain.length})`)
  return { plain, raw }
}

const resolvePartPayload = (templates: TemplateStore, rowid: string, plain: Buffer): string => {
  let parsed: Json
  try {
    parsed = parseJson(plain.toString("utf8"))
  } catch (error) {
    fail(`part row ${rowid}: blob payload is not valid JSON (${String(error).slice(0, 100)})`)
  }
  if (!isObject(parsed)) fail(`part row ${rowid}: blob payload is not an object`)
  const type = String(parsed["type"] ?? "?")
  const tool = String(parsed["tool"] ?? "")
  return canonJson(reorderValue(templates, "P", type, tool, Object.keys(parsed), parsed, rowid))
}

const resolveEventPayload = (
  templates: TemplateStore,
  envelopeOrder: readonly string[],
  wrapperOrder: readonly string[],
  slim: Slim,
  plain: Buffer,
  rowid: string,
): string => {
  let parsed: Json
  try {
    parsed = parseJson(plain.toString("utf8"))
  } catch (error) {
    fail(`event row ${rowid}: blob payload is not valid JSON (${String(error).slice(0, 100)})`)
  }
  if (!isObject(parsed)) fail(`event row ${rowid}: blob payload is not an object`)
  const type = String(parsed["type"] ?? "?")
  const tool = String(parsed["tool"] ?? "")
  const payload = reorderValue(templates, "E", type, tool, Object.keys(parsed), parsed, rowid)
  const ids: Record<string, Json> = { id: slim.pid, sessionID: slim.sid, messageID: slim.mid }
  const part: { [key: string]: Json } = {}
  for (const key of wrapperOrder) {
    if (ids[key] === undefined) fail(`event row ${rowid}: wrapper order references ${key}`)
    part[key] = ids[key] as Json
  }
  for (const [key, value] of Object.entries(payload)) {
    if (!(key in part)) part[key] = value
  }
  const vals: Record<string, Json> = { sessionID: slim.sid, part, time: slim.time }
  if (Object.keys(vals).sort().join("") !== [...envelopeOrder].sort().join("")) {
    fail(`event row ${rowid}: envelope fields ${Object.keys(vals).sort()} do not match archived order ${envelopeOrder}`)
  }
  const out: { [key: string]: Json } = {}
  for (const key of envelopeOrder) out[key] = vals[key] as Json
  return canonJson(out)
}

// Restores the archive file in place to live layout. The caller owns copies:
// work on a duplicate, never the published archive.
export const restoreFile = async (filename: string, allowIncomplete: boolean): Promise<RestoreResult> => {
  const db = await openRawDb(filename, "rw")
  try {
    const manifest = loadManifest(db, filename, allowIncomplete)
    console.log(
      `manifest: ${manifest.fields["count_session"]} sessions, blob=${manifest.fields["blob_count"]} ptr=${manifest.pointers} tpl=${manifest.fields["tpl_rows"]}`,
    )
    const dictCache = new Map<string, Buffer>()
    const partIds = db.all<{ id: string }>(`SELECT id FROM ptr WHERE t = 'part' ORDER BY id`).map((row) => row.id)
    const partSet = new Set(partIds)
    let parts = 0
    let after = ""
    for (;;) {
      const rows = db.all<{ id: string; data: string }>(`SELECT id, data FROM part WHERE id > ? ORDER BY id LIMIT 10000`, [after])
      if (rows.length === 0) break
      const updates: string[][] = []
      for (const row of rows) {
        after = row.id
        if (!partSet.has(row.id)) continue
        const sha = parsePointer(row.data, "part", row.id)
        const { plain, raw } = readBlob(db, dictCache, "part", row.id, sha)
        updates.push([raw ? plain.toString("utf8") : resolvePartPayload(manifest.templates, row.id, plain), row.id])
        parts += 1
      }
      if (updates.length > 0) {
        db.exec("BEGIN IMMEDIATE")
        try {
          for (const [data, id] of updates) db.run(`UPDATE part SET data = ? WHERE id = ?`, [data, id])
          db.exec("COMMIT")
        } catch (error) {
          try {
            db.exec("ROLLBACK")
          } catch {
            // Best-effort; output is unpublished on failure.
          }
          if (error instanceof ColdV2Error) throw error
          fail(`part restore chunk failed: ${error instanceof Error ? error.message.slice(0, 160) : String(error).slice(0, 160)}`)
        }
      }
      if (rows.length < 10000) break
    }
    if (parts !== partIds.length) fail(`part pointer registry has ${partIds.length} ids but ${parts} resolved`)
    console.log(`parts resolved: ${parts}`)
    const slimIds = db.all<{ id: string }>(`SELECT id FROM ptr WHERE t = 'event' ORDER BY id`).map((row) => row.id)
    const slimSet = new Set(slimIds)
    let events = 0
    after = ""
    for (;;) {
      const rows = db.all<{ id: string; data: string }>(`SELECT id, data FROM event WHERE id > ? ORDER BY id LIMIT 2000`, [after])
      if (rows.length === 0) break
      const updates: string[][] = []
      for (const row of rows) {
        after = row.id
        if (!slimSet.has(row.id)) continue
        const slim = parseSlim(row.data, row.id)
        const { plain, raw } = readBlob(db, dictCache, "event", row.id, slim.blob)
        if (raw) {
          let payload: Json
          try {
            payload = parseJson(plain.toString("utf8"))
          } catch (error) {
            fail(`event row ${row.id}: raw blob payload invalid (${String(error).slice(0, 100)})`)
          }
          if (!isObject(payload)) fail(`event row ${row.id}: raw blob payload not an object`)
          const ids: Record<string, Json> = { id: slim.pid, sessionID: slim.sid, messageID: slim.mid }
          const part: { [key: string]: Json } = {}
          for (const key of manifest.wrapperOrder) {
            if (ids[key] === undefined) fail(`event row ${row.id}: wrapper order references ${key}`)
            part[key] = ids[key] as Json
          }
          for (const [key, value] of Object.entries(payload)) {
            if (!(key in part)) part[key] = value
          }
          const vals: Record<string, Json> = { sessionID: slim.sid, part, time: slim.time }
          const out: { [key: string]: Json } = {}
          for (const key of manifest.envelopeOrder) out[key] = vals[key] as Json
          updates.push([canonJson(out), row.id])
        } else {
          updates.push([resolveEventPayload(manifest.templates, manifest.envelopeOrder, manifest.wrapperOrder, slim, plain, row.id), row.id])
        }
        events += 1
      }
      if (updates.length > 0) {
        db.exec("BEGIN IMMEDIATE")
        try {
          for (const [data, id] of updates) db.run(`UPDATE event SET data = ? WHERE id = ?`, [data, id])
          db.exec("COMMIT")
        } catch (error) {
          try {
            db.exec("ROLLBACK")
          } catch {
            // Best-effort; output is unpublished on failure.
          }
          if (error instanceof ColdV2Error) throw error
          fail(`event restore chunk failed: ${error instanceof Error ? error.message.slice(0, 160) : String(error).slice(0, 160)}`)
        }
      }
      if (events % 200000 < 2000 && events > 0) console.log(`  events ...${events}`)
      if (rows.length < 2000) break
    }
    if (events !== slimIds.length) fail(`event slim registry has ${slimIds.length} ids but ${events} resolved`)
    console.log(`events resolved: ${events}`)
    for (const [table] of TABLE_KEYS) {
      try {
        const count = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM "${table}"`)?.n
        const want = manifest.counts[table]
        if (count !== undefined && want !== undefined && count !== want) {
          fail(`restored table ${table} has ${count} rows, manifest says ${want}`)
        }
      } catch (error) {
        if (error instanceof ColdV2Error) throw error
        // Absent tables simply skip the count check.
      }
    }
    const inline = computeInlineHash(db)
    if (inline.hash !== manifest.fields["inline_hash"]) {
      fail(`inline hash mismatch (archive tampered or corrupt; ${inline.count} inline rows)`)
    }
    if (inline.count !== Number(manifest.fields["inline_count"] ?? "-1")) {
      fail(`inline row count ${inline.count} != manifest ${manifest.fields["inline_count"]}`)
    }
    console.log(`inline hash ok: ${inline.count} rows`)
    for (const table of ["ptr", "blob", "zdict", "tpl", "meta"]) db.exec(`DROP TABLE IF EXISTS "${table}"`)
    console.log("VACUUM ...")
    db.exec(`VACUUM`)
    assertQuickCheck(db, "restored tmp")
    return { parts, events }
  } finally {
    db.close()
  }
}

// ------------------------------------------------------------------ verify
export interface VerifyReport {
  readonly sessions: string
  readonly blobs: number
  readonly pointers: number
  readonly templates: string
  readonly codecs: string[]
}

// Read-only integrity sweep: manifest chain plus every blob decompressed,
// re-hashed and length-checked, plus ptr<->blob referential checks. Inline
// rows are covered by the inline hash only after a restore (see restoreFile),
// which the pack self-verify already performs before publish.
export const verifyArchive = async (filename: string): Promise<VerifyReport> => {
  const db = await openRawDb(filename, "ro")
  try {
    const manifest = loadManifest(db, filename, false)
    const dictCache = new Map<string, Buffer>()
    let checked = 0
    let after = ""
    for (;;) {
      const rows = db.all<{ sha256: string }>(`SELECT sha256 FROM blob WHERE sha256 > ? ORDER BY sha256 LIMIT 5000`, [after])
      if (rows.length === 0) break
      for (const row of rows) {
        after = row.sha256
        readBlob(db, dictCache, "blob", row.sha256.slice(0, 16), row.sha256)
        checked += 1
      }
      if (rows.length < 5000) break
    }
    const orphanPtr = db.get<{ n: number }>(
      `SELECT COUNT(*) AS n FROM ptr WHERE sha NOT IN (SELECT sha256 FROM blob)`,
    )?.n ?? 0
    if (orphanPtr > 0) fail(`verify: ${orphanPtr} ptr rows reference missing blobs`)
    const codecs = [...new Set(db.all<{ codec: string }>(`SELECT DISTINCT codec FROM blob`).map((row) => row.codec))]
    console.log(`verify ok: ${checked} blobs re-hashed, ${manifest.pointers} pointers, 0 orphans`)
    return {
      sessions: manifest.fields["count_session"] ?? "?",
      blobs: checked,
      pointers: manifest.pointers,
      templates: manifest.fields["tpl_rows"] ?? "?",
      codecs,
    }
  } finally {
    db.close()
  }
}

// ------------------------------------------------------------------ compare
export interface CompareResult {
  readonly total: number
  readonly diffs: number
  readonly firsts: string[]
}

// Streaming id-ordered merge join per table, O(1) memory. allow=null compares
// everything; otherwise the base side is filtered to the archived sessions
// (restored files only ever contain archived sessions).
export const compareFiles = async (baseFile: string, restoredFile: string, allow: readonly string[] | null): Promise<CompareResult> => {
  const base = await openRawDb(baseFile, "ro")
  const restored = await openRawDb(restoredFile, "ro")
  let total = 0
  let diffs = 0
  const firsts: string[] = []
  const note = (text: string): void => {
    if (firsts.length < 5) firsts.push(text)
  }
  try {
    for (const [table, key, sessionColumn] of TABLE_KEYS) {
      let baseColumns: string[]
      try {
        baseColumns = base.all<{ name: string }>(`SELECT name FROM pragma_table_info('${table}')`).map((row) => row.name)
      } catch {
        continue
      }
      if (baseColumns.length === 0) continue
      const restoredColumns = restored.all<{ name: string }>(`SELECT name FROM pragma_table_info('${table}')`).map((row) => row.name)
      if (restoredColumns.join("") !== baseColumns.join("")) {
        diffs += 1
        note(`${table}: schema differs base=${baseColumns} restored=${restoredColumns}`)
        continue
      }
      const select = baseColumns.map((column) => `"${column}"`).join(", ")
      const params: string[] = allow ? [...allow] : []
      const placeholders = params.map(() => "?").join(",")
      // Only todo is rowid-keyed: its anchor must bind as a number, since
      // `rowid > '5'` (text) is false for every row in SQLite type order.
      const numericAnchor = table === "todo"
      let checked = 0
      let mismatched = 0
      // Keyset windows keep memory O(1); the allow filter rides every page.
      let baseDone = false
      let restoredDone = false
      let baseAfter: string | number | null = null
      let restoredAfter: string | number | null = null
      const pageBase = (anchor: string | number | null): Record<string, unknown>[] => {
        const bounds = anchor === null ? "" : ` AND "${key}" > ?`
        const args = anchor === null ? [...params] : [...params, anchor]
        if (allow) {
          return base.all<Record<string, unknown>>(
            `SELECT "${key}", ${select} FROM "${table}" WHERE "${sessionColumn}" IN (${placeholders})${bounds} ORDER BY "${key}" LIMIT 2000`,
            args,
          )
        }
        return base.all<Record<string, unknown>>(
          `SELECT "${key}", ${select} FROM "${table}" WHERE 1 = 1${bounds} ORDER BY "${key}" LIMIT 2000`,
          anchor === null ? [] : [anchor],
        )
      }
      const pageRestored = (anchor: string | number | null): Record<string, unknown>[] =>
        restored.all<Record<string, unknown>>(
          anchor === null
            ? `SELECT "${key}", ${select} FROM "${table}" ORDER BY "${key}" LIMIT 2000`
            : `SELECT "${key}", ${select} FROM "${table}" WHERE "${key}" > ? ORDER BY "${key}" LIMIT 2000`,
          anchor === null ? [] : [anchor],
        )
      const nextAnchor = (rows: Record<string, unknown>[]): string | number => {
        const last = rows[rows.length - 1]?.[key]
        if (numericAnchor) return Number(last ?? 0)
        return String(last ?? "")
      }
      let baseRows = pageBase(null)
      let restoredRows = pageRestored(null)
      let bi = 0
      let ri = 0
      for (;;) {
        if (bi >= baseRows.length && !baseDone) {
          if (baseRows.length < 2000) baseDone = true
          else {
            baseAfter = nextAnchor(baseRows)
            baseRows = pageBase(baseAfter)
            bi = 0
            if (baseRows.length === 0) baseDone = true
          }
        }
        if (ri >= restoredRows.length && !restoredDone) {
          if (restoredRows.length < 2000) restoredDone = true
          else {
            restoredAfter = nextAnchor(restoredRows)
            restoredRows = pageRestored(restoredAfter)
            ri = 0
            if (restoredRows.length === 0) restoredDone = true
          }
        }
        const arow = bi < baseRows.length ? baseRows[bi] : undefined
        const rrow = ri < restoredRows.length ? restoredRows[ri] : undefined
        if (!arow && !rrow) break
        if (rrow && (!arow || String(rrow[key]) < String(arow[key]))) {
          mismatched += 1
          note(`${table}: ${rrow[key]} present in restored, not in base`)
          ri += 1
        } else if (arow && (!rrow || String(arow[key]) < String(rrow[key]))) {
          mismatched += 1
          note(`${table}: ${arow[key]} dropped from restored`)
          bi += 1
        } else if (arow && rrow) {
          checked += 1
          const atext = baseColumns.map((column) => String(arow[column]))
          const rtext = baseColumns.map((column) => String(rrow[column]))
          if (atext.join("") !== rtext.join("")) {
            mismatched += 1
            for (let i = 0; i < baseColumns.length; i += 1) {
              if (atext[i] !== rtext[i]) {
                note(`${table}: ${arow[key]} col ${baseColumns[i]} baselen=${atext[i]?.length ?? 0} restlen=${rtext[i]?.length ?? 0}`)
                break
              }
            }
          }
          bi += 1
          ri += 1
        }
      }
      total += checked
      diffs += mismatched
      console.log(`compare ${table}: shared=${checked} diffs=${mismatched}`)
    }
    return { total, diffs, firsts }
  } finally {
    base.close()
    restored.close()
  }
}

// ------------------------------------------------------------------ files
// The v1 original is sacred: pack/restore/verify only ever byte-copy it and
// work on the copy. Snapshots of the LIVE database go through VACUUM INTO,
// which is WAL-safe (a raw cp of a live WAL database without its -wal file is
// a torn snapshot). Offline files are byte-copied after refusing present
// -wal/-shm sidecars, then quick_checked before packing.
export const refuseWalSidecars = async (src: string): Promise<void> => {
  const { access } = await import("node:fs/promises")
  // NOTE: the existence probe must not share a try block with fail(): fail
  // throws, and a shared catch would swallow it as "absent".
  for (const suffix of ["-wal", "-shm", "-journal"]) {
    const present = await access(src + suffix).then(
      () => true,
      () => false,
    )
    if (present) {
      fail(
        `source ${src} has a ${suffix} sidecar: the database is live or uncleanly closed; ` +
          `point --src at the running instance (default) or checkpoint it first`,
      )
    }
  }
}

export const copyBytes = async (src: string, dst: string): Promise<void> => {
  const { copyFile, mkdir } = await import("node:fs/promises")
  const { dirname } = await import("node:path")
  await mkdir(dirname(dst), { recursive: true })
  await copyFile(src, dst)
}

export const removeIfExists = async (path: string): Promise<void> => {
  const { unlink } = await import("node:fs/promises")
  try {
    await unlink(path)
  } catch {
    // Already gone.
  }
}

// Exclusive, non-blocking lock via O_EXCL create. Stale locks (dead pid) are
// reaped once; a live holder fails loud (callers map to exit 3).
export const withFileLock = async <T>(lockPath: string, fn: () => Promise<T>): Promise<T> => {
  const { open, unlink } = await import("node:fs/promises")
  const acquire = async (): Promise<void> => {
    try {
      const handle = await open(lockPath, "wx", 0o644)
      await handle.writeFile(String(process.pid), "utf8")
      await handle.close()
      return
    } catch (error) {
      if ((error as { code?: string }).code !== "EEXIST") {
        fail(`cannot create lock ${lockPath}: ${error instanceof Error ? error.message : String(error)}`)
      }
    }
    const { readFile } = await import("node:fs/promises")
    const pid = Number((await readFile(lockPath, "utf8")).trim())
    let alive = true
    if (Number.isInteger(pid) && pid > 0) {
      try {
        process.kill(pid, 0)
      } catch {
        alive = false
      }
    }
    if (alive) fail(`lock held by another process: ${lockPath}`)
    await unlink(lockPath)
    const retry = await open(lockPath, "wx", 0o644)
    await retry.writeFile(String(process.pid), "utf8")
    await retry.close()
  }
  await acquire()
  try {
    return await fn()
  } finally {
    await removeIfExists(lockPath)
  }
}

export const atomicPublish = async (tmp: string, dst: string): Promise<void> => {
  const { open } = await import("node:fs/promises")
  const { rename } = await import("node:fs/promises")
  const { dirname } = await import("node:path")
  const handle = await open(tmp, "r")
  try {
    await handle.sync()
  } finally {
    await handle.close()
  }
  await rename(tmp, dst)
  const dir = await open(dirname(dst), "r")
  try {
    await dir.sync()
  } finally {
    await dir.close()
  }
}

export const markComplete = async (filename: string): Promise<void> => {
  const db = await openRawDb(filename, "rw")
  try {
    setMeta(db, "complete", "1")
    setMeta(db, "published_utc", new Date().toISOString().replace(/\.\d+Z$/, "Z"))
  } finally {
    db.close()
  }
}

// Consistent snapshot of the live database through its own driver handle.
// VACUUM INTO only reads the source (safe beside a running TUI in WAL mode)
// and writes a compacted copy; the source header and WAL are untouched.
export const snapshotLiveFile = async (live: string, tmp: string): Promise<void> => {
  await removeIfExists(tmp)
  const db = await openRawDb(live, "rw")
  try {
    db.exec(`VACUUM INTO '${tmp.replace(/'/g, "''")}'`)
  } finally {
    db.close()
  }
}

export * as SessionColdV2 from "./cold-v2"
