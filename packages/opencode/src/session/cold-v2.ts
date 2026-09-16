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
import { constants as zlibConstants, zstdCompressSync, zstdDecompressSync } from "node:zlib"
import { Schema } from "effect"
import { createProgress, nullSink, type ProgressHandle } from "./cold-v2-progress"
import {
  createPackPool,
  resolveJobs,
  verifyPoolEquivalence,
  wantsWorkers,
  type PackPool,
  type PackRowFns,
} from "./cold-v2-workers"

export class ColdV2Error extends Schema.TaggedErrorClass<ColdV2Error>()("ColdV2Error", {
  message: Schema.String,
}) {}

// Function declaration (not an arrow const): only this form narrows
// callers for definite-assignment and undefined checks under tsgo.
function fail(message: string): never {
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

export const isObject = (value: unknown): value is { [key: string]: Json } =>
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
// Exported: the worker pool serializes the pack row functions, which close
// over these — single source of truth, no duplicated logic.
export const sameOrder = (a: readonly string[], b: readonly string[]): boolean =>
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

export const ordersFor = (store: TemplateStore, ctx: Ctx, type: string, tool: string, shape: readonly string[], path: readonly string[]) =>
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
// Level rides in `params`, not a top-level `level` key: node:zlib silently
// ignores `{ level: 9 }` (the codec label below would then be a lie), while
// `params` is honored on both runtimes. Decompression needs no level.
export const compressPlain = (plain: Uint8Array): Buffer =>
  Buffer.from(zstdCompressSync(plain, { params: { [zlibConstants.ZSTD_c_compressionLevel]: 9 } }))

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

// Structured progress logging. Default output is the same human-readable
// line as before; with OPENCODE_COLD_JSON_LOG=1 every event becomes one JSON
// object per line ({ts, event, msg, ...fields}) for cron/k8s pipelines.
export const coldLog = (event: string, message: string, fields: Record<string, unknown> = {}): void => {
  if (process.env.OPENCODE_COLD_JSON_LOG === "1") {
    console.log(JSON.stringify({ ts: new Date().toISOString(), event, msg: message, ...fields }))
  } else {
    console.log(message)
  }
}

export const IN_CHUNK = 400

export const chunked = <T>(rows: T[], size: number): T[][] => {
  if (rows.length === 0) return []
  const out: T[][] = []
  for (let i = 0; i < rows.length; i += size) out.push(rows.slice(i, i + size))
  return out
}

// Staging table for large id sets. Interpolating one placeholder per id
// breaks past SQLite's variable limit (~999 on old builds); a temp table
// keeps every statement fixed-shape no matter how large the allow-list is.
const stageKeepIds = (db: RawDb, keep: readonly string[]): void => {
  db.exec(`CREATE TEMP TABLE _keep (id TEXT PRIMARY KEY)`)
  db.exec("BEGIN IMMEDIATE")
  try {
    for (const group of chunked([...keep], IN_CHUNK)) {
      db.run(`INSERT OR IGNORE INTO _keep VALUES ${group.map(() => "(?)").join(",")}`, [...group])
    }
    db.exec("COMMIT")
  } catch (error) {
    try {
      db.exec("ROLLBACK")
    } catch {
      // Best-effort; the tmp file is unpublished on failure.
    }
    throw error
  }
}

const dropKeepIds = (db: RawDb): void => {
  db.exec(`DROP TABLE IF EXISTS _keep`)
}

export const filterSessions = (db: RawDb, allow: readonly string[] | null): Set<string> | null => {
  if (!allow) {
    const total = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM session`)?.n ?? 0
    coldLog("filter", `filter: keeping all ${total} sessions`, { sessions: total })
    return null
  }
  const keep = [...allow].sort()
  // Empty allow-list archives nothing: `NOT IN ()` is a syntax error, so
  // delete unconditionally instead.
  if (keep.length === 0) {
    for (const table of ["session_input", "session_context_epoch", "session_message", "todo", "part", "message", "session", "event", "event_sequence"]) {
      db.run(`DELETE FROM "${table}"`)
    }
    coldLog("filter", `filter: keeping 0 sessions`, { sessions: 0 })
    return new Set<string>()
  }
  const pairs: readonly (readonly [table: string, column: string])[] = [
    ["session_input", "session_id"],
    ["session_context_epoch", "session_id"],
    ["session_message", "session_id"],
    ["todo", "session_id"],
    ["part", "session_id"],
    ["message", "session_id"],
    ["session", "id"],
  ]
  stageKeepIds(db, keep)
  try {
    for (const [table, column] of pairs) db.run(`DELETE FROM "${table}" WHERE "${column}" NOT IN (SELECT id FROM _keep)`)
    db.run(`DELETE FROM event WHERE aggregate_id NOT IN (SELECT id FROM _keep)`)
    db.run(`DELETE FROM event_sequence WHERE aggregate_id NOT IN (SELECT id FROM _keep)`)
  } finally {
    dropKeepIds(db)
  }
  const total = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM session`)?.n ?? 0
  coldLog("filter", `filter: keeping ${total} sessions`, { sessions: total })
  return new Set(keep)
}

export interface Learned {
  readonly store: TemplateStore
  readonly envelopeOrder: readonly string[]
  readonly wrapperOrder: readonly string[]
}

export const learnTemplates = (db: RawDb, onTick?: (rows: number) => void): Learned => {
  const store: TemplateStore = new Map()
  let parts = 0
  let after: string | null = null
  for (;;) {
    const rows: { id: string; data: string }[] =
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
    onTick?.(rows.length)
    if (rows.length < 20000) break
  }
  coldLog("templates", `part templates: ${parts} rows`, { parts })
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
    onTick?.(rows.length)
    if (rows.length < 20000) break
  }
  coldLog("templates", `event templates: ${pu1} pu1 rows; envelope=${envelopeOrder ?? []} wrapper=${wrapperOrder ?? []}`, { pu1 })
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

// ------------------------------------------------------------------ pack rows (pure)
// Per-row pack computation, DB-free so it runs identically on the main thread
// and in worker threads (`./cold-v2-workers` serializes these exact exported
// functions — single source of truth, no duplicated logic). Errors return as
// values, never throw: the caller maps them to fail() so the sync and
// parallel paths fail with byte-identical messages. "packed" covers pointer
// rows (parts) and slim rows (events); the caller builds the pointer/slim
// JSON from the assigned sha.
export type PackRowKind = "skip" | "packed" | "error"

export interface PackRowOut {
  readonly kind: PackRowKind
  readonly plain: Uint8Array
  readonly raw: boolean
  readonly message: string
  /** Slim id fields for event rows (null for parts); avoids a re-parse. */
  readonly slim: { sid: string; time: Json; pid: string; mid: string } | null
}

export const skippedRow = (): PackRowOut => ({ kind: "skip", plain: new Uint8Array(0), raw: false, message: "", slim: null })

export const errorRow = (message: string): PackRowOut => ({ kind: "error", plain: new Uint8Array(0), raw: false, message, slim: null })

export const packPartRow = (store: TemplateStore, minBytes: number, id: string, data: string): PackRowOut => {
  if (Buffer.byteLength(data, "utf8") < minBytes) return skippedRow()
  let parsed: Json
  try {
    parsed = parseJson(data)
  } catch (error) {
    return errorRow(`part ${id}: unparseable (${String(error).slice(0, 100)})`)
  }
  if (!isObject(parsed)) return errorRow(`part ${id}: top-level JSON is not an object`)
  const intruders = Object.keys(parsed).filter((key) => idSet.has(key))
  if (intruders.length > 0) return errorRow(`part ${id}: payload carries top-level wrapper ids ${intruders} (schema drift)`)
  const type = String(parsed["type"] ?? "?")
  const tool = String(parsed["tool"] ?? "")
  const shape = Object.keys(parsed)
  if (isReorderable(store, "P", parsed, type, tool, shape)) {
    return { kind: "packed", plain: Buffer.from(canonJson(canonValue(parsed)), "utf8"), raw: false, message: "", slim: null }
  }
  return { kind: "packed", plain: Buffer.from(data, "utf8"), raw: true, message: "", slim: null }
}

export const packEventRow = (
  store: TemplateStore,
  envelopeOrder: readonly string[],
  wrapperOrder: readonly string[],
  id: string,
  data: string,
): PackRowOut => {
  let parsed: Json
  try {
    parsed = parseJson(data)
  } catch (error) {
    return errorRow(`event ${id}: unparseable (${String(error).slice(0, 100)})`)
  }
  if (!isObject(parsed)) return errorRow(`event ${id}: pu1 row is not an object`)
  const part = parsed["part"]
  if (!isObject(part)) return errorRow(`event ${id}: pu1 row has no part object`)
  if (!sameOrder(Object.keys(parsed), envelopeOrder)) {
    return errorRow(`event ${id}: envelope order ${Object.keys(parsed)} != ${envelopeOrder} (schema drift)`)
  }
  const here = Object.keys(part).filter((key) => idSet.has(key))
  const positions = Object.keys(part)
  if (!sameOrder(here, [...wrapperOrder]) || !sameOrder(positions.slice(0, here.length), here)) {
    return errorRow(`event ${id}: wrapper order ${here} != ${wrapperOrder} as prefix (schema drift)`)
  }
  const type = String(part["type"] ?? "?")
  const tool = String(part["tool"] ?? "")
  const payload = Object.fromEntries(Object.entries(part).filter(([key]) => !idSet.has(key)))
  const shape = Object.keys(payload)
  const slim = { sid: parsed["sessionID"] as string, time: parsed["time"] as Json, pid: part["id"] as string, mid: part["messageID"] as string }
  if (isReorderable(store, "E", payload, type, tool, shape)) {
    return { kind: "packed", plain: Buffer.from(canonJson(canonValue(payload)), "utf8"), raw: false, message: "", slim }
  }
  return { kind: "packed", plain: Buffer.from(canonJson(payload), "utf8"), raw: true, message: "", slim }
}

// The exact function set the worker pool serializes. Keys must cover the
// FN_ORDER list in ./cold-v2-workers (pool creation throws loud if not).
export const packRowFns = (): PackRowFns => ({
  parseJson,
  canonJson,
  isObject,
  canonValue,
  skippedRow,
  errorRow,
  templateKey,
  ordersFor,
  sameOrder,
  templateOrder,
  isReorderable,
  compressPlain,
  blobDigest,
  packPartRow,
  packEventRow,
})

// Trust-but-verify blob insert for the computed path: the bytes were hashed
// by the same process (fail-loud on any error), so re-hashing here would
// double the digest cost for zero gain. INSERT OR IGNORE keeps cross-page
// duplicate payloads safe when pages complete out of order. Integrity is not
// weakened: the end-of-flow self-verify restores and byte-compares every row,
// and verifyArchive re-hashes every blob before anything is trusted.
const insertBlobRaw = (db: RawDb, sha: string, comp: Uint8Array, plainLen: number, raw: boolean): void => {
  db.run(`INSERT OR IGNORE INTO blob (sha256, bytes, len, codec, raw, dict_id) VALUES (?, ?, ?, 'zstd-9', ?, NULL)`, [
    sha,
    Buffer.from(comp.buffer, comp.byteOffset, comp.byteLength),
    plainLen,
    raw ? 1 : 0,
  ])
}

export interface ComputedRow {
  readonly id: string
  readonly sha: string
  readonly comp: Buffer
  readonly plainLen: number
  readonly raw: boolean
  readonly slim: { sid: string; time: Json; pid: string; mid: string } | null
}

export interface PackStepOpts {
  readonly pool?: PackPool | null
  readonly progress?: ProgressHandle
}

// Below this size a page is computed inline: the IPC round-trip would cost
// more than the rows themselves.
const PARALLEL_MIN_ROWS = 500

const toComputed = (id: string, out: PackRowOut): ComputedRow | null => {
  if (out.kind === "error") fail(out.message)
  if (out.kind === "skip") return null
  const plain = Buffer.isBuffer(out.plain) ? out.plain : Buffer.from(out.plain)
  const sha = blobDigest(out.raw, plain)
  return { id, sha, comp: compressPlain(plain), plainLen: plain.length, raw: out.raw, slim: out.slim }
}

// One page of part rows → compressed blobs. Pool results are
// order-preserving, and pages commit sequentially, so output bytes are
// identical with any worker count (the determinism test pins this).
const computePartRows = async (
  store: TemplateStore,
  minBytes: number,
  pool: PackPool | null | undefined,
  rows: readonly { id: string; data: string }[],
): Promise<ComputedRow[]> => {
  if (!pool || rows.length < PARALLEL_MIN_ROWS) {
    const out: ComputedRow[] = []
    for (const row of rows) {
      const computed = toComputed(row.id, packPartRow(store, minBytes, row.id, row.data))
      if (computed) out.push(computed)
    }
    return out
  }
  const results = await pool.run(
    "part",
    rows.map((row) => ({ id: row.id, data: row.data })),
    minBytes,
  )
  return results.flatMap((result) => {
    if (result.error) fail(result.error)
    if (result.skipped || !result.comp) return []
    return [
      {
        id: result.id,
        sha: result.sha,
        comp: Buffer.from(result.comp.buffer, result.comp.byteOffset, result.comp.byteLength),
        plainLen: result.plainLen,
        raw: result.raw,
        slim: null,
      } satisfies ComputedRow,
    ]
  })
}

const computeEventRows = async (
  store: TemplateStore,
  envelopeOrder: readonly string[],
  wrapperOrder: readonly string[],
  pool: PackPool | null | undefined,
  rows: readonly { id: string; data: string }[],
): Promise<ComputedRow[]> => {
  if (!pool || rows.length < PARALLEL_MIN_ROWS) {
    const out: ComputedRow[] = []
    for (const row of rows) {
      const computed = toComputed(row.id, packEventRow(store, envelopeOrder, wrapperOrder, row.id, row.data))
      if (computed) out.push(computed)
    }
    return out
  }
  const results = await pool.run(
    "event",
    rows.map((row) => ({ id: row.id, data: row.data })),
    0,
  )
  return results.flatMap((result) => {
    if (result.error) fail(result.error)
    if (result.skipped || !result.comp || !result.slim) fail(`event row ${result.id}: worker returned no slim (internal error)`)
    return [
      {
        id: result.id,
        sha: result.sha,
        comp: Buffer.from(result.comp.buffer, result.comp.byteOffset, result.comp.byteLength),
        plainLen: result.plainLen,
        raw: result.raw,
        slim: result.slim,
      } satisfies ComputedRow,
    ]
  })
}

export const packParts = async (
  db: RawDb,
  store: TemplateStore,
  minBytes: number,
  opts: PackStepOpts = {},
): Promise<{ pointers: number; rawFallback: number }> => {
  const progress = opts.progress ?? createProgress(nullSink())
  let seen = 0
  let pointers = 0
  let rawFallback = 0
  let after = ""
  for (;;) {
    const rows = db.all<{ id: string; data: string }>(`SELECT id, data FROM part WHERE id > ? ORDER BY id LIMIT 10000`, [after])
    if (rows.length === 0) break
    // Cheap pre-filter on the main thread: inline rows never need IPC.
    const packable = rows.filter((row) => {
      seen += 1
      after = row.id
      return Buffer.byteLength(row.data, "utf8") >= minBytes
    })
    const computed = await computePartRows(store, minBytes, opts.pool, packable)
    const updates: string[][] = []
    const registry: string[][] = []
    // One transaction per page, covering blob inserts too: under
    // synchronous=FULL each commit fsyncs, so per-row autocommit would cost
    // a journal+fsync per blob (measured ~100x slower on 60k rows). A crash
    // rolls back the partial page; the tmp file is unpublished on failure.
    db.exec("BEGIN IMMEDIATE")
    try {
      for (const item of computed) {
        insertBlobRaw(db, item.sha, item.comp, item.plainLen, item.raw)
        if (item.raw) rawFallback += 1
        updates.push([JSON.stringify({ _blob: item.sha }), item.id])
        registry.push(["part", item.id, item.sha])
        pointers += 1
      }
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
    progress.tick("pack-parts", rows.length)
    if (seen % 200000 < 10000) coldLog("progress", `  parts ...${seen} ptr=${pointers} rawfb=${rawFallback}`, { seen, pointers, rawFallback })
  }
  coldLog("pack-parts", `parts: ${seen} rows, pointers=${pointers}, raw-fallback=${rawFallback}`, { seen, pointers, rawFallback })
  return { pointers, rawFallback }
}

export const packEvents = async (
  db: RawDb,
  store: TemplateStore,
  envelopeOrder: readonly string[],
  wrapperOrder: readonly string[],
  opts: PackStepOpts = {},
): Promise<{ slims: number; rawFallback: number }> => {
  const progress = opts.progress ?? createProgress(nullSink())
  let slims = 0
  let rawFallback = 0
  let after = ""
  for (;;) {
    const rows = db.all<{ id: string; data: string }>(
      `SELECT id, data FROM event WHERE type = '${PU1}' AND id > ? ORDER BY id LIMIT 2000`,
      [after],
    )
    if (rows.length === 0) break
    for (const row of rows) after = row.id
    const computed = await computeEventRows(store, envelopeOrder, wrapperOrder, opts.pool, rows)
    const updates: string[][] = []
    const registry: string[][] = []
    // Same page-sized transaction as packParts: blob inserts ride the chunk.
    db.exec("BEGIN IMMEDIATE")
    try {
      for (const item of computed) {
        insertBlobRaw(db, item.sha, item.comp, item.plainLen, item.raw)
        if (item.raw) rawFallback += 1
        if (!item.slim) fail(`event row ${item.id}: missing slim ids (internal error)`)
        const slim: { [key: string]: Json } = {
          _ev: "pu1",
          sid: item.slim.sid,
          time: item.slim.time,
          pid: item.slim.pid,
          mid: item.slim.mid,
          blob: item.sha,
        }
        updates.push([JSON.stringify(slim), item.id])
        registry.push(["event", item.id, item.sha])
        slims += 1
      }
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
    progress.tick("pack-events", rows.length)
    if (slims % 200000 < 2000) coldLog("progress", `  events ...${slims} rawfb=${rawFallback}`, { slims, rawFallback })
  }
  coldLog("pack-events", `events: ${slims} slimmed, raw-fallback=${rawFallback}`, { slims, rawFallback })
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
  coldLog("inline", `inline: ${count} rows hashed`, { count })
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
//
// jobs: undefined = synchronous (today's path; also the background-migration
// default — no CPU spike beside a running TUI), 0 = auto (match the system),
// 1 = synchronous, >1 = worker pool. Pool startup or self-test trouble warns
// and falls back to sync; row-level trouble still fails loud.
export interface PackFileOpts {
  readonly jobs?: number
  readonly progress?: ProgressHandle
}

export const packFile = async (
  filename: string,
  allow: readonly string[] | null,
  minBytes: number,
  opts: PackFileOpts = {},
): Promise<PackFileStats> => {
  const progress = opts.progress ?? createProgress(nullSink())
  const db = await openRawDb(filename, "rw")
  try {
    db.exec(`PRAGMA journal_mode = DELETE`)
    db.exec(`PRAGMA synchronous = FULL`)
    db.exec(`PRAGMA busy_timeout = 30000`)
    assertLiveLayout(db, filename)
    filterSessions(db, allow)
    const sessions = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM session`)?.n ?? 0
    // An explicit non-empty allow-list that matches nothing is always a
    // mistake (typo'd ids); failing loud beats publishing an empty archive
    // that reads as a successful migration.
    if (allow !== null && allow.length > 0 && sessions === 0) {
      fail(`selection matched 0 sessions (${allow.length} requested); refusing to publish an empty archive`)
    }
    const partTotal = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM part`)?.n ?? 0
    const eventTotal = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM event WHERE type = '${PU1}'`)?.n ?? 0
    progress.start("learn-templates", "learn templates", partTotal + eventTotal)
    const { store, envelopeOrder, wrapperOrder } = learnTemplates(db, (n) => progress.tick("learn-templates", n))
    progress.end("learn-templates")
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
    let pool: PackPool | null = null
    let inline: { hash: string; count: number }
    const packResult: { parts?: { pointers: number; rawFallback: number }; events?: { slims: number; rawFallback: number } } = {}
    if (wantsWorkers(opts.jobs)) {
      const size = resolveJobs(opts.jobs)
      pool = await createPackPool({
        size,
        fns: packRowFns(),
        ids: IDS,
        store,
        envelopeOrder: [...envelopeOrder],
        wrapperOrder: [...wrapperOrder],
        onWarn: (message) => coldLog("workers", message),
      })
      if (pool) {
        try {
          await verifyPoolEquivalence(pool, packRowFns(), store, envelopeOrder, wrapperOrder, minBytes)
          coldLog("workers", `pack workers: ${pool.size} threads (requested ${opts.jobs === 0 ? "auto" : opts.jobs})`)
        } catch (error) {
          coldLog("workers", `pack workers failed self-test (${error instanceof Error ? error.message : String(error)}); packing synchronously`)
          await pool.close()
          pool = null
        }
      }
    }
    try {
      progress.start("pack-parts", "pack parts", partTotal)
      packResult.parts = await packParts(db, store, minBytes, { pool, progress })
      progress.end("pack-parts")
      progress.start("pack-events", "pack events", eventTotal)
      packResult.events = await packEvents(db, store, envelopeOrder, wrapperOrder, { pool, progress })
      progress.end("pack-events")
      progress.start("hashes", "manifest hashes", null)
      setMeta(db, "ptr_hash", computePtrHash(db))
      inline = computeInlineHash(db)
      progress.end("hashes")
      setMeta(db, "inline_hash", inline.hash)
    } finally {
      // Release worker threads before the long single-threaded tail (VACUUM,
      // self-verify): no point holding cores we no longer feed.
      if (pool) await pool.close()
    }
    setMeta(db, "inline_count", String(inline.count))
    setMeta(db, "blob_count", String(db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM blob`)?.n ?? 0))
    // No dictionary training on this runtime (node:zlib exposes no trainer,
    // and trained dicts are not portable across zstd builds). The table
    // exists for format compatibility with reference-tooling archives.
    db.exec(`CREATE TABLE IF NOT EXISTS zdict (id TEXT PRIMARY KEY, project TEXT, bytes BLOB)`)
    for (const [table] of TABLE_KEYS) {
      let count: number | undefined
      try {
        count = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM "${table}"`)?.n
      } catch (error) {
        // Missing tables on older layouts are the only benign case; anything
        // else must stay loud, or a failed count silently disables its
        // restore-side check (verify_counts treats absent as "nothing to check").
        const message = error instanceof Error ? error.message : String(error)
        if (!/no such table/i.test(message)) throw error
      }
      if (count !== undefined) setMeta(db, `count_${table}`, String(count))
    }
    coldLog("vacuum", "VACUUM ...")
    progress.start("vacuum", "vacuum", null)
    db.exec(`VACUUM`)
    progress.end("vacuum")
    setMeta(db, "manifest_hash", manifestHashOf(readMeta(db)))
    assertQuickCheck(db, "packed tmp")
    const blobs = Number(readMeta(db)["blob_count"] ?? "0")
    coldLog("packed", `packed: blob=${blobs} dicts=0 inline=${inline.count}`, { blobs, inline: inline.count })
    const parts = packResult.parts
    const events = packResult.events
    if (!parts || !events) fail("pack steps did not complete (internal error)")
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
  if (!Array.isArray(envelopeOrder) || !Array.isArray(wrapperOrder)) {
    fail("archive manifest has corrupt envelope/wrapper orders")
  }
  // Orders are empty exactly when the archive holds no pu1 slims (learned
  // from zero pu1 rows); otherwise both are non-empty. Enforcing the coupling
  // rejects both corruption and smuggled-empty-order tampering.
  const eventPtrs = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM ptr WHERE t = 'event'`)?.n ?? 0
  if (eventPtrs === 0) {
    if (envelopeOrder.length !== 0 || wrapperOrder.length !== 0) {
      fail("archive manifest orders non-empty but archive holds no event slims")
    }
  } else if (envelopeOrder.length === 0 || wrapperOrder.length === 0) {
    fail("archive manifest has corrupt envelope/wrapper orders")
  }
  const counts: Record<string, number> = {}
  for (const [key, value] of Object.entries(fields)) {
    if (key.startsWith("count_")) counts[key.slice("count_".length)] = Number(value)
  }
  const pointers = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM ptr`)?.n ?? 0
  return { fields, templates, envelopeOrder, wrapperOrder, counts, pointers }
}

// Light manifest for the per-session hot path (fault-in). Verifies everything
// loadManifest verifies EXCEPT the ptr_hash full-table scan: that scan is
// O(pointers) and would make every session open cost O(archive). Per-session
// integrity still holds — each faulted row's pointer/slim sha is JOIN-checked
// against its ptr entry and every blob is digest+len re-verified — so tampering
// with the faulted session is caught; tampering elsewhere surfaces on
// pack-verify or when that session faults in. Never use for publish gates.
export const loadManifestLight = (db: RawDb, filename: string, allowIncomplete: boolean): Manifest => {
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
  if (!Array.isArray(envelopeOrder) || !Array.isArray(wrapperOrder)) {
    fail("archive manifest has corrupt envelope/wrapper orders")
  }
  const eventPtrs = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM ptr WHERE t = 'event'`)?.n ?? 0
  if (eventPtrs === 0) {
    if (envelopeOrder.length !== 0 || wrapperOrder.length !== 0) {
      fail("archive manifest orders non-empty but archive holds no event slims")
    }
  } else if (envelopeOrder.length === 0 || wrapperOrder.length === 0) {
    fail("archive manifest has corrupt envelope/wrapper orders")
  }
  const counts: Record<string, number> = {}
  for (const [key, value] of Object.entries(fields)) {
    if (key.startsWith("count_")) counts[key.slice("count_".length)] = Number(value)
  }
  const pointers = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM ptr`)?.n ?? 0
  return { fields, templates, envelopeOrder, wrapperOrder, counts, pointers }
}

interface BlobRow {
  readonly bytes: unknown
  readonly codec: string
  readonly dict_id: string | null
  readonly len: number
  readonly raw: number
}

// Batched blob fetch: one chunked IN query per IN_CHUNK shas instead of one
// SELECT per pointer (2M pointers = 2M round-trips otherwise). Chunks keep
// every statement under SQLite's variable limit.
export const fetchBlobBatch = (db: RawDb, shas: readonly string[]): Map<string, BlobRow> => {
  const out = new Map<string, BlobRow>()
  const distinct = [...new Set(shas)]
  for (const group of chunked(distinct, IN_CHUNK)) {
    const rows = db.all<{ sha256: string } & BlobRow>(
      `SELECT sha256, bytes, codec, dict_id, len, raw FROM blob WHERE sha256 IN (${group.map(() => "?").join(",")})`,
      [...group],
    )
    for (const row of rows) {
      out.set(row.sha256, { bytes: row.bytes, codec: row.codec, dict_id: row.dict_id, len: row.len, raw: row.raw })
    }
  }
  return out
}

const readBlobFromRow = (
  db: RawDb,
  dictCache: Map<string, Buffer>,
  table: string,
  rowid: string,
  sha: string,
  row: BlobRow | undefined,
): { plain: Buffer; raw: boolean } => {
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

const readBlob = (
  db: RawDb,
  dictCache: Map<string, Buffer>,
  table: string,
  rowid: string,
  sha: string,
): { plain: Buffer; raw: boolean } => {
  const row = db.get<BlobRow>(`SELECT bytes, codec, dict_id, len, raw FROM blob WHERE sha256 = ?`, [sha])
  return readBlobFromRow(db, dictCache, table, rowid, sha, row)
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

// Fail-fast registry↔row presence checks (both directions, both tables).
// Per-row sha equality is checked in the JOIN loops of restoreFile (A) and
// verifyArchive (F); these COUNTs give the missing/stray cases loud,
// specific errors before any heavy work.
export const assertRegistryCounts = (db: RawDb, context: string, onTick?: (rows: number) => void): void => {
  const missingParts =
    db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM ptr LEFT JOIN part ON part.id = ptr.id WHERE ptr.t = 'part' AND part.id IS NULL`)?.n ?? 0
  onTick?.(1)
  if (missingParts > 0) {
    fail(`${context}: part pointer registry has ${missingParts} ids with no part row (row↔registry mismatch; deleted or tampered rows)`)
  }
  const strayParts =
    db.get<{ n: number }>(
      `SELECT COUNT(*) AS n FROM part LEFT JOIN ptr ON ptr.t = 'part' AND ptr.id = part.id WHERE part.data LIKE '{"_blob%' AND ptr.id IS NULL`,
    )?.n ?? 0
  onTick?.(1)
  if (strayParts > 0) {
    fail(`${context}: ${strayParts} pointer-shaped part rows have no registry entry (row↔registry mismatch; swapped or forged pointers)`)
  }
  const missingEvents =
    db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM ptr LEFT JOIN event ON event.id = ptr.id WHERE ptr.t = 'event' AND event.id IS NULL`)?.n ?? 0
  onTick?.(1)
  if (missingEvents > 0) {
    fail(`${context}: event slim registry has ${missingEvents} ids with no event row (row↔registry mismatch; deleted or tampered rows)`)
  }
  const strayEvents =
    db.get<{ n: number }>(
      `SELECT COUNT(*) AS n FROM event LEFT JOIN ptr ON ptr.t = 'event' AND ptr.id = event.id WHERE event.data LIKE '{"_ev%' AND ptr.id IS NULL`,
    )?.n ?? 0
  onTick?.(1)
  if (strayEvents > 0) {
    fail(`${context}: ${strayEvents} slim-shaped event rows have no registry entry (row↔registry mismatch; swapped or forged slims)`)
  }
}

// Full row↔registry sha equality, parse-only (no decompression). restoreFile
// runs this BEFORE resolving anything: a swapped slim used to fail only
// after the part loop had already rewritten rows in place, leaving a
// half-mutated tmp. Validate-then-execute keeps failures mutation-free.
export const assertRegistryLinks = (db: RawDb, context: string, onTick?: (rows: number) => void): void => {
  let partAfter = ""
  for (;;) {
    const rows = db.all<{ id: string; data: string; reg: string }>(
      `SELECT p.id AS id, p.data AS data, r.sha AS reg FROM part p JOIN ptr r ON r.t = 'part' AND r.id = p.id WHERE p.id > ? ORDER BY p.id LIMIT 5000`,
      [partAfter],
    )
    if (rows.length === 0) break
    for (const row of rows) {
      partAfter = row.id
      const sha = parsePointer(row.data, "part", row.id)
      if (sha !== row.reg) {
        fail(`${context}: part row ${row.id}: pointer sha ${sha.slice(0, 16)} != registry ${row.reg.slice(0, 16)} (row↔registry mismatch; swapped or tampered pointer)`)
      }
    }
    onTick?.(rows.length)
    if (rows.length < 5000) break
  }
  let eventAfter = ""
  for (;;) {
    const rows = db.all<{ id: string; data: string; reg: string }>(
      `SELECT e.id AS id, e.data AS data, r.sha AS reg FROM event e JOIN ptr r ON r.t = 'event' AND r.id = e.id WHERE e.id > ? ORDER BY e.id LIMIT 2000`,
      [eventAfter],
    )
    if (rows.length === 0) break
    for (const row of rows) {
      eventAfter = row.id
      const slim = parseSlim(row.data, row.id)
      if (slim.blob !== row.reg) {
        fail(`${context}: event row ${row.id}: slim blob ${slim.blob.slice(0, 16)} != registry ${row.reg.slice(0, 16)} (row↔registry mismatch; swapped or tampered slim)`)
      }
    }
    onTick?.(rows.length)
    if (rows.length < 2000) break
  }
}

// Restores the archive file in place to live layout. The caller owns copies:
// work on a duplicate, never the published archive.
export interface RestoreFileOpts {
  readonly progress?: ProgressHandle
}

export const restoreFile = async (filename: string, allowIncomplete: boolean, opts: RestoreFileOpts = {}): Promise<RestoreResult> => {
  const progress = opts.progress ?? createProgress(nullSink())
  const db = await openRawDb(filename, "rw")
  try {
    const manifest = loadManifest(db, filename, allowIncomplete)
    coldLog(
      "manifest",
      `manifest: ${manifest.fields["count_session"]} sessions, blob=${manifest.fields["blob_count"]} ptr=${manifest.pointers} tpl=${manifest.fields["tpl_rows"]}`,
      { sessions: manifest.fields["count_session"], blobs: manifest.fields["blob_count"], pointers: manifest.pointers },
    )
    const dictCache = new Map<string, Buffer>()
    progress.start("registry-links", "registry links", manifest.pointers)
    assertRegistryCounts(db, "restore", (n) => progress.tick("registry-links", n))
    assertRegistryLinks(db, "restore", (n) => progress.tick("registry-links", n))
    progress.end("registry-links")
    const expectedParts = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM ptr WHERE t = 'part'`)?.n ?? 0
    let parts = 0
    let after = ""
    progress.start("restore-parts", "restore parts", expectedParts)
    for (;;) {
      // JOIN the registry: the row's pointer sha must equal the registered
      // sha (fix A). A swapped pointer (valid sha, wrong row) trips here even
      // though the blob itself is intact — the old code followed row.data
      // blindly and only the byte-compare caught it.
      const rows = db.all<{ id: string; data: string; reg: string }>(
        `SELECT p.id AS id, p.data AS data, r.sha AS reg FROM part p JOIN ptr r ON r.t = 'part' AND r.id = p.id WHERE p.id > ? ORDER BY p.id LIMIT 10000`,
        [after],
      )
      if (rows.length === 0) break
      // Prefetch the page's distinct blobs in chunked IN queries, then
      // resolve pointers from memory instead of one SELECT per row.
      const wanted = new Map<string, string>()
      for (const row of rows) {
        after = row.id
        const sha = parsePointer(row.data, "part", row.id)
        if (sha !== row.reg) {
          fail(`part row ${row.id}: pointer sha ${sha.slice(0, 16)} != registry ${row.reg.slice(0, 16)} (row↔registry mismatch; swapped or tampered pointer)`)
        }
        wanted.set(row.id, sha)
      }
      const blobs = fetchBlobBatch(db, [...wanted.values()])
      const updates: string[][] = []
      for (const row of rows) {
        const sha = wanted.get(row.id)
        if (!sha) continue
        const { plain, raw } = readBlobFromRow(db, dictCache, "part", row.id, sha, blobs.get(sha))
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
      progress.tick("restore-parts", rows.length)
      if (rows.length < 10000) break
    }
    if (parts !== expectedParts) fail(`part pointer registry has ${expectedParts} ids but ${parts} resolved`)
    progress.end("restore-parts")
    coldLog("restore-parts", `parts resolved: ${parts}`, { parts })
    const expectedEvents = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM ptr WHERE t = 'event'`)?.n ?? 0
    let events = 0
    after = ""
    progress.start("restore-events", "restore events", expectedEvents)
    for (;;) {
      const rows = db.all<{ id: string; data: string; reg: string }>(
        `SELECT e.id AS id, e.data AS data, r.sha AS reg FROM event e JOIN ptr r ON r.t = 'event' AND r.id = e.id WHERE e.id > ? ORDER BY e.id LIMIT 2000`,
        [after],
      )
      if (rows.length === 0) break
      const slims = new Map<string, Slim>()
      for (const row of rows) {
        after = row.id
        const slim = parseSlim(row.data, row.id)
        if (slim.blob !== row.reg) {
          fail(`event row ${row.id}: slim blob ${slim.blob.slice(0, 16)} != registry ${row.reg.slice(0, 16)} (row↔registry mismatch; swapped or tampered slim)`)
        }
        slims.set(row.id, slim)
      }
      const blobs = fetchBlobBatch(
        db,
        [...slims.values()].map((slim) => slim.blob),
      )
      const updates: string[][] = []
      for (const row of rows) {
        const slim = slims.get(row.id)
        if (!slim) continue
        const { plain, raw } = readBlobFromRow(db, dictCache, "event", row.id, slim.blob, blobs.get(slim.blob))
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
      if (events % 200000 < 2000 && events > 0) coldLog("progress", `  events ...${events}`, { events })
      progress.tick("restore-events", rows.length)
      if (rows.length < 2000) break
    }
    if (events !== expectedEvents) fail(`event slim registry has ${expectedEvents} ids but ${events} resolved`)
    progress.end("restore-events")
    coldLog("restore-events", `events resolved: ${events}`, { events })
    for (const [table] of TABLE_KEYS) {
      let count: number | undefined
      try {
        count = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM "${table}"`)?.n
      } catch (error) {
        // Same narrowing as packFile: only a missing table skips the check.
        const message = error instanceof Error ? error.message : String(error)
        if (!/no such table/i.test(message)) throw error
        continue
      }
      const want = manifest.counts[table]
      if (count !== undefined && want !== undefined && count !== want) {
        fail(`restored table ${table} has ${count} rows, manifest says ${want}`)
      }
    }
    progress.start("inline-verify", "inline verify", null)
    const inline = computeInlineHash(db)
    progress.end("inline-verify")
    if (inline.hash !== manifest.fields["inline_hash"]) {
      fail(`inline hash mismatch (archive tampered or corrupt; ${inline.count} inline rows)`)
    }
    if (inline.count !== Number(manifest.fields["inline_count"] ?? "-1")) {
      fail(`inline row count ${inline.count} != manifest ${manifest.fields["inline_count"]}`)
    }
    coldLog("inline-verify", `inline hash ok: ${inline.count} rows`, { count: inline.count })
    for (const table of ["ptr", "blob", "zdict", "tpl", "meta"]) db.exec(`DROP TABLE IF EXISTS "${table}"`)
    coldLog("vacuum", "VACUUM ...")
    progress.start("vacuum", "vacuum", null)
    db.exec(`VACUUM`)
    progress.end("vacuum")
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
// re-hashed and length-checked, plus ptr<->blob referential checks, plus the
// row↔registry sweep (F): every pointer/slim row's sha must equal its ptr
// entry, both directions. Inline rows are covered by the inline hash only
// after a restore (see restoreFile), which the pack self-verify already
// performs before publish.
export const verifyArchive = async (filename: string, opts: RestoreFileOpts = {}): Promise<VerifyReport> => {
  const progress = opts.progress ?? createProgress(nullSink())
  const db = await openRawDb(filename, "ro")
  try {
    const manifest = loadManifest(db, filename, false)
    assertRegistryCounts(db, "verify")
    progress.start("verify-registry", "verify registry", manifest.pointers)
    // F: JOIN-compare every registered row's sha without resolving blobs.
    // Catches swapped pointers (valid sha, wrong row) with no restore.
    let regChecked = 0
    let partAfter = ""
    for (;;) {
      const rows = db.all<{ id: string; data: string; reg: string }>(
        `SELECT p.id AS id, p.data AS data, r.sha AS reg FROM part p JOIN ptr r ON r.t = 'part' AND r.id = p.id WHERE p.id > ? ORDER BY p.id LIMIT 5000`,
        [partAfter],
      )
      if (rows.length === 0) break
      for (const row of rows) {
        partAfter = row.id
        const sha = parsePointer(row.data, "part", row.id)
        if (sha !== row.reg) {
          fail(`verify: part row ${row.id}: pointer sha ${sha.slice(0, 16)} != registry ${row.reg.slice(0, 16)} (row↔registry mismatch)`)
        }
        regChecked += 1
      }
      progress.tick("verify-registry", rows.length)
      if (rows.length < 5000) break
    }
    let eventAfter = ""
    for (;;) {
      const rows = db.all<{ id: string; data: string; reg: string }>(
        `SELECT e.id AS id, e.data AS data, r.sha AS reg FROM event e JOIN ptr r ON r.t = 'event' AND r.id = e.id WHERE e.id > ? ORDER BY e.id LIMIT 2000`,
        [eventAfter],
      )
      if (rows.length === 0) break
      for (const row of rows) {
        eventAfter = row.id
        const slim = parseSlim(row.data, row.id)
        if (slim.blob !== row.reg) {
          fail(`verify: event row ${row.id}: slim blob ${slim.blob.slice(0, 16)} != registry ${row.reg.slice(0, 16)} (row↔registry mismatch)`)
        }
        regChecked += 1
      }
      progress.tick("verify-registry", rows.length)
      if (rows.length < 2000) break
    }
    coldLog("verify-registry", `verify: ${regChecked} row↔registry links checked`, { links: regChecked })
    progress.end("verify-registry")
    const dictCache = new Map<string, Buffer>()
    let checked = 0
    let after = ""
    const blobTotal = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM blob`)?.n ?? 0
    progress.start("verify-blobs", "verify blobs", blobTotal)
    for (;;) {
      const rows = db.all<{ sha256: string }>(`SELECT sha256 FROM blob WHERE sha256 > ? ORDER BY sha256 LIMIT 5000`, [after])
      if (rows.length === 0) break
      const batch = fetchBlobBatch(
        db,
        rows.map((row) => row.sha256),
      )
      for (const row of rows) {
        after = row.sha256
        readBlobFromRow(db, dictCache, "blob", row.sha256.slice(0, 16), row.sha256, batch.get(row.sha256))
        checked += 1
      }
      progress.tick("verify-blobs", rows.length)
      if (rows.length < 5000) break
    }
    progress.end("verify-blobs")
    const orphanPtr = db.get<{ n: number }>(
      `SELECT COUNT(*) AS n FROM ptr WHERE sha NOT IN (SELECT sha256 FROM blob)`,
    )?.n ?? 0
    if (orphanPtr > 0) fail(`verify: ${orphanPtr} ptr rows reference missing blobs`)
    const codecs = [...new Set(db.all<{ codec: string }>(`SELECT DISTINCT codec FROM blob`).map((row) => row.codec))]
    coldLog("verify", `verify ok: ${checked} blobs re-hashed, ${manifest.pointers} pointers, 0 orphans`, { blobs: checked, pointers: manifest.pointers })
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
export const compareFiles = async (
  baseFile: string,
  restoredFile: string,
  allow: readonly string[] | null,
  opts: RestoreFileOpts = {},
): Promise<CompareResult> => {
  const progress = opts.progress ?? createProgress(nullSink())
  const base = await openRawDb(baseFile, "ro")
  const restored = await openRawDb(restoredFile, "ro")
  let total = 0
  let diffs = 0
  const firsts: string[] = []
  const note = (text: string): void => {
    if (firsts.length < 5) firsts.push(text)
  }
  try {
    progress.start("compare", "compare tables", TABLE_KEYS.length)
    for (const [table, key, sessionColumn] of TABLE_KEYS) {
      progress.tick("compare", 1)
      let baseColumns: string[]
      try {
        baseColumns = base.all<{ name: string }>(`SELECT name FROM pragma_table_info('${table}')`).map((row) => row.name)
      } catch {
        continue
      }
      if (baseColumns.length === 0) continue
      const restoredColumns = restored.all<{ name: string }>(`SELECT name FROM pragma_table_info('${table}')`).map((row) => row.name)
      // Element-wise, not join-based: ["ab","c"] vs ["a","bc"] join equal.
      if (restoredColumns.length !== baseColumns.length || restoredColumns.some((column, i) => column !== baseColumns[i])) {
        diffs += 1
        note(`${table}: schema differs base=${baseColumns} restored=${restoredColumns}`)
        continue
      }
      const select = baseColumns.map((column) => `"${column}"`).join(", ")
      // The allow filter is applied in JS, not SQL: interpolating one
      // placeholder per session breaks past SQLite's variable limit, and the
      // base handle is read-only (no temp staging table). Filtering the
      // ordered stream preserves order with O(1) memory.
      const allowSet = allow ? new Set(allow) : null
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
      const pageBase = (anchor: string | number | null): { rows: Record<string, unknown>[]; rawExhausted: boolean; rawAnchor: string | number | null } => {
        const bounds = anchor === null ? "" : ` AND "${key}" > ?`
        const args = anchor === null ? [] : [anchor]
        const raw = base.all<Record<string, unknown>>(
          `SELECT "${key}", ${select} FROM "${table}" WHERE 1 = 1${bounds} ORDER BY "${key}" LIMIT 2000`,
          args,
        )
        const rawExhausted = raw.length < 2000
        const rawAnchor = raw.length > 0 ? (raw[raw.length - 1]?.[key] as string | number) : anchor
        if (!allowSet) return { rows: raw, rawExhausted, rawAnchor }
        return { rows: raw.filter((row) => allowSet.has(String(row[sessionColumn]))), rawExhausted, rawAnchor }
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
      // Native key ordering: integer keys (todo rowid) must compare
      // numerically — `"10" < "9"` lexically but not numerically, which used
      // to misattribute missing rows past digit boundaries (diagnostics only;
      // the go/no-go count was unaffected, but wrong messages waste hours).
      const keyLess = (a: unknown, b: unknown): boolean => {
        if (typeof a === "number" && typeof b === "number") return a < b
        if (typeof a === "bigint" && typeof b === "bigint") return a < b
        return String(a) < String(b)
      }
      const sameColumns = (a: string[], b: string[]): boolean => a.length === b.length && a.every((column, i) => column === b[i])
      let basePage = pageBase(null)
      let baseRows = basePage.rows
      baseAfter = basePage.rawAnchor
      if (basePage.rawExhausted) baseDone = true
      let restoredRows = pageRestored(null)
      let bi = 0
      let ri = 0
      for (;;) {
        // Refill filtered-empty pages: the raw stream may hold more rows.
        while (bi >= baseRows.length && !baseDone) {
          const next = pageBase(baseAfter)
          baseAfter = next.rawAnchor
          baseRows = next.rows
          bi = 0
          if (next.rawExhausted) baseDone = true
          if (baseRows.length > 0 || baseDone) break
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
        if ((!arow && rrow) || (arow && rrow && keyLess(rrow[key], arow[key]))) {
          mismatched += 1
          note(`${table}: ${rrow?.[key]} present in restored, not in base`)
          ri += 1
        } else if ((!rrow && arow) || (arow && rrow && keyLess(arow[key], rrow[key]))) {
          mismatched += 1
          note(`${table}: ${arow?.[key]} dropped from restored`)
          bi += 1
        } else if (arow && rrow) {
          checked += 1
          const atext = baseColumns.map((column) => String(arow[column]))
          const rtext = baseColumns.map((column) => String(rrow[column]))
          if (!sameColumns(atext, rtext)) {
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
      coldLog("compare", `compare ${table}: shared=${checked} diffs=${mismatched}`, { table, shared: checked, diffs: mismatched })
    }
    progress.end("compare")
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

// Pre-flight disk check: a pack transiently needs ~3x the source size next
// to dst (tmp copy + VACUUM rewrite headroom + verify copy). A full disk
// mid-VACUUM strands a corrupt tmp and wastes the whole build, so abort loud
// before touching anything. Returns {free, need} bytes; callers fail when
// free < need. statfs is unavailable on some runtimes — then free is null and
// the caller logs a warning instead of blocking.
export const diskRoom = async (src: string, dir: string, multiplier = 3): Promise<{ free: number | null; need: number }> => {
  const { stat, statfs } = await import("node:fs/promises")
  const size = (await stat(src)).size
  const need = size * multiplier
  let free: number | null = null
  try {
    const info = await statfs(dir)
    free = Number(info.bfree) * Number(info.bsize)
  } catch {
    free = null
  }
  return { free, need }
}

export const removeIfExists = async (path: string): Promise<void> => {
  const { unlink } = await import("node:fs/promises")
  try {
    await unlink(path)
  } catch {
    // Already gone.
  }
}

// Orphaned work files from killed runs (`<dst>.tmp.<pid>`, plus the verify
// and base snapshots derived from it). MUST run inside the destination
// lockfile: the prefix also matches a live process's tmp, and the lock is
// what proves no such process exists.
export const cleanStaleTmps = async (dst: string): Promise<number> => {
  const { readdir, unlink } = await import("node:fs/promises")
  const { dirname, basename } = await import("node:path")
  const dir = dirname(dst)
  const prefix = `${basename(dst)}.tmp.`
  let entries: string[]
  try {
    entries = await readdir(dir)
  } catch {
    return 0
  }
  let removed = 0
  for (const entry of entries) {
    if (!entry.startsWith(prefix)) continue
    try {
      await unlink(`${dir}/${entry}`)
      removed += 1
    } catch {
      // Raced or locked; harmless to leave.
    }
  }
  return removed
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

// External sidecar checksum (`<archive>.sha256`): detects bit-rot months or
// years later without invoking the sqlite/zstd toolchain. Written on publish,
// checked (when present) by unpack and pack-verify before any heavy work.
// Absent sidecars warn but don't block, so pre-sidecar archives keep working.
export const sidecarPath = (archive: string): string => `${archive}.sha256`

export const hashFile = async (path: string): Promise<string> => {
  const { createReadStream } = await import("node:fs")
  const hash = createHash("sha256")
  await new Promise<void>((resolve, reject) => {
    const stream = createReadStream(path)
    stream.on("data", (chunk) => hash.update(chunk as Buffer))
    stream.on("end", () => resolve())
    stream.on("error", (error) => reject(error))
  })
  return hash.digest("hex")
}

export const writeSidecar = async (archive: string): Promise<string> => {
  const { writeFile } = await import("node:fs/promises")
  const digest = await hashFile(archive)
  await writeFile(sidecarPath(archive), `${digest}  ${archive.split("/").pop()}\n`, "utf8")
  return digest
}

// Returns the recorded digest, or null when no sidecar exists (warn, proceed).
export const verifySidecar = async (archive: string): Promise<string | null> => {
  const { readFile } = await import("node:fs/promises")
  let recorded: string
  try {
    recorded = (await readFile(sidecarPath(archive), "utf8")).split(/\s+/, 1)[0] ?? ""
  } catch {
    return null
  }
  if (!/^[0-9a-f]{64}$/.test(recorded)) fail(`sidecar ${sidecarPath(archive)} is malformed`)
  const actual = await hashFile(archive)
  if (actual !== recorded) {
    fail(`sidecar mismatch for ${archive}: file changed since publish (bit-rot or tamper); recorded=${recorded.slice(0, 16)} actual=${actual.slice(0, 16)}`)
  }
  return recorded
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

// ------------------------------------------------------------------ live restore
// V2-as-live, on-demand: the packed archive is the durable source of truth;
// opencode-live-v2.db holds the full session index (every session header +
// projects, for instant browsing) but heavy payloads (messages/parts/events)
// only for open sessions. Closed sessions are stubs: header present, heavy
// absent, faulted back in from the archive on first read.
//
// Boot (missing/empty live) materializes a SLIM live (headers only, tiny)
// instead of a full decompress: browsing works immediately, sessions fault in
// one at a time. A full decompress only happens on explicit recovery
// (`unpack --dst <live> --force`).
export interface RestoreLiveInput {
  readonly archive: string
  readonly live: string
  readonly progress?: ProgressHandle
}

export interface RestoreLiveDone {
  readonly sessions: number
  readonly phaseMs: Record<string, number>
}

export interface FaultInDone {
  readonly sessions: number
  readonly parts: number
  readonly events: number
  readonly phaseMs: Record<string, number>
}

export interface EvictDone {
  readonly sessions: number
  readonly phaseMs: Record<string, number>
}

// Session-subtree tables. `session` is the index (always kept in live for
// browsing); the rest are heavy payloads, present in live only for open
// sessions. Delete/insert order is FK-safe (children before parents on
// delete, parents before children on insert).
const HEAVY_BY_SESSION: readonly (readonly [table: string, column: string])[] = [
  ["session_input", "session_id"],
  ["session_context_epoch", "session_id"],
  ["session_message", "session_id"],
  ["todo", "session_id"],
  ["part", "session_id"],
  ["message", "session_id"],
] as const
const HEAVY_BY_AGGREGATE: readonly (readonly [table: string, column: string])[] = [
  ["event", "aggregate_id"],
  ["event_sequence", "aggregate_id"],
] as const
const PACKED_TABLES = ["blob", "ptr", "tpl", "meta", "zdict"] as const

const removeLiveSidecars = async (live: string): Promise<void> => {
  for (const suffix of ["-wal", "-shm", "-journal"]) await removeIfExists(`${live}${suffix}`)
}

// Retry wrapper for the live file lock on the fault-in hot path: two TUIs
// opening different sessions at once serialize briefly instead of one failing
// loud. Checks residency between retries so a waiter returns as soon as the
// winner publishes.
const withLiveLockRetry = async <T>(live: string, fn: () => Promise<T>, isDone: () => Promise<boolean>): Promise<T | null> => {
  const start = Date.now()
  for (;;) {
    try {
      return await withFileLock(`${live}.lock`, fn)
    } catch (error) {
      const locked = /lock held/.test(error instanceof Error ? error.message : String(error))
      if (!locked) throw error
      // Winner is publishing: if our sessions are resident now, there is
      // nothing left to do — return null and let the caller re-check.
      if (await isDone().catch(() => false)) return null
      if (Date.now() - start > 30_000) throw error
      await new Promise((resolve) => setTimeout(resolve, 500))
    }
  }
}

const tableColumns = (db: RawDb, table: string): string[] => {
  // Attached databases need the two-argument pragma form:
  // pragma_table_info('src.t') parses as a table literally named "src.t" and
  // returns no rows (verified against bun:sqlite).
  const dot = table.indexOf(".")
  if (dot !== -1) {
    const schema = table.slice(0, dot)
    const name = table.slice(dot + 1)
    return db
      .all<{ name: string }>(`SELECT name FROM pragma_table_info(?, ?)`, [name, schema])
      .map((row) => row.name)
  }
  return db.all<{ name: string }>(`SELECT name FROM pragma_table_info('${table.replace(/'/g, "''")}')`).map((row) => row.name)
}

// Bulk insert without its own transaction: callers (fault-in, evict) wrap the
// whole per-session mutation (re-check + delete + insert) in ONE outer
// BEGIN IMMEDIATE so a crash or concurrent writer can never leave a
// half-materialized session behind.
const insertRowsRaw = (db: RawDb, table: string, rows: readonly Record<string, unknown>[]): void => {
  if (rows.length === 0) return
  const columns = tableColumns(db, table)
  if (columns.length === 0) fail(`fault-in: live table ${table} missing (schema drift?)`)
  const placeholders = columns.map(() => "?").join(",")
  const quoted = columns.map((column) => `"${column}"`).join(",")
  for (const row of rows) {
    db.run(`INSERT OR REPLACE INTO "${table}" (${quoted}) VALUES (${placeholders})`, columns.map((column) => (row as Record<string, unknown>)[column] ?? null))
  }
}

const deleteSessionHeavy = (db: RawDb, sessionID: string): void => {
  for (const [table, column] of HEAVY_BY_SESSION) {
    try {
      db.run(`DELETE FROM "${table}" WHERE "${column}" = ?`, [sessionID])
    } catch {
      // Missing table on older layouts: nothing to delete.
    }
  }
  for (const [table, column] of HEAVY_BY_AGGREGATE) {
    try {
      db.run(`DELETE FROM "${table}" WHERE "${column}" = ?`, [sessionID])
    } catch {
      // Missing table on older layouts: nothing to delete.
    }
  }
}

// Exported for the evict CLI candidate filter (same residency definition as
// the hot path: any heavy row present means resident).
export const sessionHeavyCounts = (db: RawDb, sessionID: string): { messages: number; parts: number; events: number; others: number } => {
  const countWhere = (table: string, column: string): number => {
    try {
      return db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM "${table}" WHERE "${column}" = ?`, [sessionID])?.n ?? 0
    } catch {
      // Missing table on older layouts: nothing to count.
      return 0
    }
  }
  const messages = countWhere("message", "session_id")
  const parts = countWhere("part", "session_id")
  const events = countWhere("event", "aggregate_id")
  let others = 0
  for (const [table, column] of [...HEAVY_BY_SESSION, ...HEAVY_BY_AGGREGATE] as const) {
    if (table === "message" || table === "part" || table === "event") continue
    others += countWhere(table, column)
  }
  return { messages, parts, events, others }
}

export const sessionIsResident = (db: RawDb, sessionID: string): boolean => {
  const counts = sessionHeavyCounts(db, sessionID)
  return counts.messages > 0 || counts.parts > 0 || counts.events > 0 || counts.others > 0
}

const hasSessionHeader = (db: RawDb, sessionID: string): boolean =>
  (db.get<{ one: number }>(`SELECT 1 AS one FROM session WHERE id = ?`, [sessionID])?.one ?? 0) === 1

interface SessionHeavy {
  readonly messages: Record<string, unknown>[]
  readonly parts: { row: Record<string, unknown>; data: string }[]
  readonly events: { row: Record<string, unknown>; data: string }[]
  readonly todos: Record<string, unknown>[]
  readonly sessionMessages: Record<string, unknown>[]
  readonly sessionInputs: Record<string, unknown>[]
  readonly sessionEpochs: Record<string, unknown>[]
  readonly eventSequences: Record<string, unknown>[]
}

// Reads one session's subtree from the PACKED archive, resolving pointers and
// slims to plain live-layout payloads. Verifies per-row registry linkage and
// per-blob digest+length (same guarantees as restoreFile, scoped to the
// session). Uses the light manifest: no O(archive) ptr_hash scan on the hot path.
const readSessionHeavyFromArchive = (
  archiveDb: RawDb,
  manifest: Manifest,
  dictCache: Map<string, Buffer>,
  sessionID: string,
): SessionHeavy => {
  const messages = archiveDb.all<Record<string, unknown>>(`SELECT * FROM message WHERE session_id = ? ORDER BY id`, [sessionID])
  const partRows = archiveDb.all<{ id: string; data: string }>(`SELECT id, data FROM part WHERE session_id = ? ORDER BY id`, [sessionID])
  const partFull = archiveDb.all<Record<string, unknown>>(`SELECT * FROM part WHERE session_id = ? ORDER BY id`, [sessionID])
  const byPartId = new Map(partFull.map((row) => [String(row["id"]), row]))
  const eventRows = archiveDb.all<{ id: string; data: string }>(`SELECT id, data FROM event WHERE aggregate_id = ? ORDER BY id`, [sessionID])
  const eventFull = archiveDb.all<Record<string, unknown>>(`SELECT * FROM event WHERE aggregate_id = ? ORDER BY id`, [sessionID])
  const byEventId = new Map(eventFull.map((row) => [String(row["id"]), row]))
  // Registry membership for this session's rows, fetched once (not per row).
  // Both directions are enforced below: pointer-shaped rows must be registered
  // (stray pointers fail), and plain rows must NOT be registered (a pointer
  // swapped for plain data fails instead of silently restoring wrong bytes).
  const regParts = new Set(
    archiveDb.all<{ id: string }>(`SELECT r.id AS id FROM ptr r JOIN part p ON p.id = r.id WHERE r.t = 'part' AND p.session_id = ?`, [sessionID]).map((row) => row.id),
  )
  const regPartSha = new Map<string, string>(
    archiveDb.all<{ id: string; sha: string }>(`SELECT r.id AS id, r.sha AS sha FROM ptr r JOIN part p ON p.id = r.id WHERE r.t = 'part' AND p.session_id = ?`, [sessionID]).map((row) => [row.id, row.sha] as const),
  )
  const regEvents = new Set(
    archiveDb.all<{ id: string }>(`SELECT r.id AS id FROM ptr r JOIN event e ON e.id = r.id WHERE r.t = 'event' AND e.aggregate_id = ?`, [sessionID]).map((row) => row.id),
  )
  const regEventSha = new Map<string, string>(
    archiveDb.all<{ id: string; sha: string }>(`SELECT r.id AS id, r.sha AS sha FROM ptr r JOIN event e ON e.id = r.id WHERE r.t = 'event' AND e.aggregate_id = ?`, [sessionID]).map((row) => [row.id, row.sha] as const),
  )
  // Resolve part pointers in one batched blob fetch.
  const partShas = new Map<string, string>()
  for (const row of partRows) {
    if (!isPointerShape(row.data)) {
      if (regParts.has(row.id)) fail(`fault-in ${sessionID}: part ${row.id} is plain data but registered as a pointer (swapped or tampered row)`)
      continue
    }
    const sha = parsePointer(row.data, "part", row.id)
    const reg = regPartSha.get(row.id)
    if (reg === undefined) fail(`fault-in ${sessionID}: pointer-shaped part ${row.id} has no registry entry`)
    if (sha !== reg) fail(`fault-in ${sessionID}: part ${row.id} pointer sha != registry (swapped or tampered)`)
    partShas.set(row.id, sha)
  }
  const partBlobs = fetchBlobBatch(archiveDb, [...partShas.values()])
  const parts: SessionHeavy["parts"] = []
  for (const row of partRows) {
    const full = byPartId.get(row.id)
    if (!full) continue
    const sha = partShas.get(row.id)
    if (sha === undefined) {
      parts.push({ row: full, data: row.data })
      continue
    }
    const { plain, raw } = readBlobFromRow(archiveDb, dictCache, "part", row.id, sha, partBlobs.get(sha))
    parts.push({ row: full, data: raw ? plain.toString("utf8") : resolvePartPayload(manifest.templates, row.id, plain) })
  }
  // Resolve event slims the same way. A slim that fails to parse is an inline
  // (non-pu1) row — unless it is registered, in which case a slim was swapped
  // for plain data and must fail loud, not restore silently wrong.
  const eventShas = new Map<string, Slim>()
  for (const row of eventRows) {
    let slim: Slim
    try {
      slim = parseSlim(row.data, row.id)
    } catch {
      if (regEvents.has(row.id)) fail(`fault-in ${sessionID}: event ${row.id} is plain data but registered as a slim (swapped or tampered row)`)
      continue // Inline (non-pu1) event row: copied as-is below.
    }
    const reg = regEventSha.get(row.id)
    if (reg === undefined) fail(`fault-in ${sessionID}: slim event ${row.id} has no registry entry`)
    if (slim.blob !== reg) fail(`fault-in ${sessionID}: event ${row.id} slim blob != registry (swapped or tampered)`)
    eventShas.set(row.id, slim)
  }
  const eventBlobs = fetchBlobBatch(archiveDb, [...eventShas.values()].map((slim) => slim.blob))
  const events: SessionHeavy["events"] = []
  for (const row of eventRows) {
    const full = byEventId.get(row.id)
    if (!full) continue
    const slim = eventShas.get(row.id)
    if (!slim) {
      events.push({ row: full, data: row.data })
      continue
    }
    const { plain, raw } = readBlobFromRow(archiveDb, dictCache, "event", row.id, slim.blob, eventBlobs.get(slim.blob))
    if (raw) {
      let payload: Json
      try {
        payload = parseJson(plain.toString("utf8"))
      } catch (error) {
        fail(`fault-in ${sessionID}: event ${row.id} raw blob invalid (${String(error).slice(0, 100)})`)
      }
      if (!isObject(payload)) fail(`fault-in ${sessionID}: event ${row.id} raw blob not an object`)
      const ids: Record<string, Json> = { id: slim.pid, sessionID: slim.sid, messageID: slim.mid }
      const part: { [key: string]: Json } = {}
      for (const key of manifest.wrapperOrder) {
        if (ids[key] === undefined) fail(`fault-in ${sessionID}: event ${row.id} wrapper order references ${key}`)
        part[key] = ids[key] as Json
      }
      for (const [key, value] of Object.entries(payload)) {
        if (!(key in part)) part[key] = value
      }
      const vals: Record<string, Json> = { sessionID: slim.sid, part, time: slim.time }
      const out: { [key: string]: Json } = {}
      for (const key of manifest.envelopeOrder) out[key] = vals[key] as Json
      events.push({ row: full, data: canonJson(out) })
    } else {
      events.push({ row: full, data: resolveEventPayload(manifest.templates, manifest.envelopeOrder, manifest.wrapperOrder, slim, plain, row.id) })
    }
  }
  const todos = archiveDb.all<Record<string, unknown>>(`SELECT * FROM todo WHERE session_id = ? ORDER BY rowid`, [sessionID])
  const sessionMessages = archiveDb.all<Record<string, unknown>>(`SELECT * FROM session_message WHERE session_id = ? ORDER BY rowid`, [sessionID])
  const sessionInputs = archiveDb.all<Record<string, unknown>>(`SELECT * FROM session_input WHERE session_id = ? ORDER BY rowid`, [sessionID])
  const sessionEpochs = archiveDb.all<Record<string, unknown>>(`SELECT * FROM session_context_epoch WHERE session_id = ? ORDER BY rowid`, [sessionID])
  const eventSequences = archiveDb.all<Record<string, unknown>>(`SELECT * FROM event_sequence WHERE aggregate_id = ? ORDER BY rowid`, [sessionID])
  return { messages, parts, events, todos, sessionMessages, sessionInputs, sessionEpochs, eventSequences }
}

const writeSessionHeavyToLive = (liveDb: RawDb, heavy: SessionHeavy): { parts: number; events: number } => {
  // Session header is deliberately NOT written: the live header wins (a stub
  // whose title changed while cold keeps its newer metadata; heavy still matches).
  insertRowsRaw(liveDb, "message", heavy.messages)
  insertRowsRaw(
    liveDb,
    "part",
    heavy.parts.map(({ row, data }) => ({ ...row, data })),
  )
  insertRowsRaw(
    liveDb,
    "event",
    heavy.events.map(({ row, data }) => ({ ...row, data })),
  )
  insertRowsRaw(liveDb, "todo", heavy.todos)
  insertRowsRaw(liveDb, "session_message", heavy.sessionMessages)
  insertRowsRaw(liveDb, "session_input", heavy.sessionInputs)
  insertRowsRaw(liveDb, "session_context_epoch", heavy.sessionEpochs)
  insertRowsRaw(liveDb, "event_sequence", heavy.eventSequences)
  return { parts: heavy.parts.length, events: heavy.events.length }
}

// On-demand fault-in: materialize the given sessions' heavy payloads from the
// packed archive into live. Sessions already resident (any heavy rows) are
// skipped without touching the archive; sessions with no live header are
// skipped (deleted stays deleted — fault-in never resurrects); sessions absent
// from the archive are skipped (live-only new or genuinely empty). Returns the
// sessions actually faulted in.
export const faultInSessions = async (archive: string, live: string, sessionIDs: readonly string[]): Promise<FaultInDone> => {
  const ids = [...new Set(sessionIDs)]
  if (ids.length === 0) return { sessions: 0, parts: 0, events: 0, phaseMs: {} }
  if (archive === live) fail("archive and live must differ")
  const started = Date.now()
  const { access } = await import("node:fs/promises")
  if (await access(archive).then(() => false, () => true)) fail(`archive not found: ${archive}`)
  if (await access(live).then(() => false, () => true)) {
    fail(`live database not found: ${live} (restart opencode once to re-materialize it from the archive, or restore explicitly with unpack --dst ${live} --force)`)
  }
  let done = { sessions: 0, parts: 0, events: 0 }
  const liveResident = async (): Promise<boolean> => {
    const liveDb = await openRawDb(live, "ro").catch(() => null)
    if (!liveDb) return false
    try {
      return ids.every((id) => {
        if (!hasSessionHeader(liveDb, id)) return true
        return sessionIsResident(liveDb, id)
      })
    } finally {
      liveDb.close()
    }
  }
  await withLiveLockRetry(
    live,
    async () => {
      const liveDb = await openRawDb(live, "rw")
      try {
        const need: string[] = []
        for (const id of ids) {
          if (!hasSessionHeader(liveDb, id)) continue
          if (sessionIsResident(liveDb, id)) continue
          need.push(id)
        }
        if (need.length === 0) return
        // Read + decompress BEFORE the write transaction: the live lock is a
        // file lock, so app writers are not blocked during CPU work — only
        // during the short commit below. The archive file itself is immutable
        // once published (packs replace it via atomic rename; an old handle
        // stays a consistent snapshot).
        const archiveDb = await openRawDb(archive, "ro")
        const heavies = new Map<string, SessionHeavy>()
        try {
          const manifest = loadManifestLight(archiveDb, archive, false)
          const dictCache = new Map<string, Buffer>()
          for (const id of need) {
            if (!hasSessionHeader(archiveDb, id)) continue
            const heavy = readSessionHeavyFromArchive(archiveDb, manifest, dictCache, id)
            const total = heavy.messages.length + heavy.parts.length + heavy.events.length + heavy.todos.length + heavy.sessionMessages.length + heavy.sessionInputs.length + heavy.sessionEpochs.length + heavy.eventSequences.length
            if (total === 0) continue // Genuinely empty on both sides.
            heavies.set(id, heavy)
          }
        } finally {
          archiveDb.close()
        }
        if (heavies.size === 0) return
        // One transaction per session: re-check residency INSIDE the write
        // lock, then delete + insert atomically. A concurrent writer either
        // committed before BEGIN (re-check sees its rows → skip, its data
        // wins) or blocks until COMMIT (its rows land on complete data).
        // A crash can only leave a stub (re-faulted next open), never a
        // half-materialized session that the residency check would accept.
        for (const [id, heavy] of heavies) {
          liveDb.exec("BEGIN IMMEDIATE")
          try {
            if (!hasSessionHeader(liveDb, id)) {
              liveDb.exec("ROLLBACK")
              continue // Deleted while we read the archive: stays deleted.
            }
            if (sessionIsResident(liveDb, id)) {
              liveDb.exec("ROLLBACK")
              continue // Materialized concurrently: nothing to do.
            }
            deleteSessionHeavy(liveDb, id)
            const wrote = writeSessionHeavyToLive(liveDb, heavy)
            liveDb.exec("COMMIT")
            coldLog("fault-in", `fault-in: session ${id} (${heavy.messages.length} messages, ${wrote.parts} parts, ${wrote.events} events)`, { session: id })
            done = { sessions: done.sessions + 1, parts: done.parts + wrote.parts, events: done.events + wrote.events }
          } catch (error) {
            try {
              liveDb.exec("ROLLBACK")
            } catch {
              // Best-effort; the session stays a stub and retries next open.
            }
            throw error
          }
        }
      } finally {
        liveDb.close()
      }
    },
    async () => liveResident(),
  )
  return { ...done, phaseMs: { totalMs: Date.now() - started } }
}

// Slim materialization: live keeps the full session index (every header) plus
// non-session tables (projects, etc.) for instant browsing, but no heavy
// payloads and no packed blob store. Tiny (headers only), fast to build, and
// the only boot path — full decompress is manual recovery only.
export const materializeLiveSlim = async (input: RestoreLiveInput): Promise<RestoreLiveDone> => {
  const { archive, live } = input
  if (archive === live) fail("archive and live must differ")
  const progress = input.progress ?? createProgress(nullSink())
  const { dirname } = await import("node:path")
  const { access } = await import("node:fs/promises")
  if (await access(archive).then(() => false, () => true)) fail(`archive not found: ${archive}`)
  const sidecar = await verifySidecar(archive)
  if (sidecar === null) coldLog("warn", `warn: no ${archive}.sha256 sidecar; skipping pre-check`)
  // Slim needs room for one archive-sized work copy only (no verify copy:
  // integrity comes from the manifest + sidecar, and heavy is faulted later).
  const room = await diskRoom(archive, dirname(live), 2)
  if (room.free !== null && room.free < room.need) {
    fail(
      `disk space: ${(room.free / 1e9).toFixed(2)}GB free next to live, need ~${(room.need / 1e9).toFixed(2)}GB (2x archive for work copy)`,
    )
  }
  return withFileLock(`${live}.lock`, async () => {
    const stale = await cleanStaleTmps(live)
    if (stale > 0) coldLog("restore", `restore: removed ${stale} orphaned tmp file(s) from killed runs`)
    const tmp = `${live}.tmp.${process.pid}`
    await removeIfExists(tmp)
    await removeLiveSidecars(live)
    coldLog("restore", `restore (slim): ${archive} -> ${live} (session index only; payloads fault in on open)`, { archive, live })
    progress.start("restore-copy", "restore copy", null)
    await copyBytes(archive, tmp)
    progress.end("restore-copy")
    progress.start("slim", "slim live", null)
    const db = await openRawDb(tmp, "rw")
    let sessions = 0
    try {
      loadManifest(db, tmp, false)
      for (const table of PACKED_TABLES) db.exec(`DROP TABLE IF EXISTS "${table}"`)
      db.exec("BEGIN IMMEDIATE")
      try {
        for (const [table] of HEAVY_BY_SESSION) db.run(`DELETE FROM "${table}"`)
        for (const [table] of HEAVY_BY_AGGREGATE) db.run(`DELETE FROM "${table}"`)
        db.exec("COMMIT")
      } catch (error) {
        try {
          db.exec("ROLLBACK")
        } catch {
          // Best-effort; tmp is unpublished on failure.
        }
        throw error
      }
      sessions = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM session`)?.n ?? 0
      db.exec(`VACUUM`)
      assertQuickCheck(db, "slim live")
    } finally {
      db.close()
    }
    progress.end("slim")
    progress.start("publish", "publish live", null)
    await atomicPublish(tmp, live)
    await removeLiveSidecars(live)
    progress.end("publish")
    coldLog("done", `DONE ${live} (slim: ${sessions} sessions indexed, 0 payloads)`, { live, sessions })
    return { sessions, phaseMs: progress.timings() }
  })
}

// Full decompress, kept for explicit recovery only (`unpack --dst <live>
// --force`). Boot never calls this: use materializeLiveSlim.
export const restoreLiveFullFromArchive = async (input: RestoreLiveInput): Promise<RestoreLiveDone & RestoreResult> => {
  const { archive, live } = input
  if (archive === live) fail("archive and live must differ")
  const progress = input.progress ?? createProgress(nullSink())
  const { dirname } = await import("node:path")
  const { access } = await import("node:fs/promises")
  if (await access(archive).then(() => false, () => true)) fail(`archive not found: ${archive}`)
  const sidecar = await verifySidecar(archive)
  if (sidecar === null) coldLog("warn", `warn: no ${archive}.sha256 sidecar; skipping pre-check`)
  // Honest pre-flight: work copy + restored image + VACUUM headroom. Payloads
  // dominate on large corpora, where a file-size multiple would strand the
  // restore on ENOSPC.
  const { file, blobs } = await archiveRestoreBytes(archive)
  const need = 2 * file + 2 * blobs
  const free = await diskRoomBytes(dirname(live))
  if (free !== null && free < need) {
    fail(
      `disk space: ${fmtGB(free)} free next to live, need ~${fmtGB(need)} (work copy + restored image + VACUUM headroom)`,
    )
  }
  if (free === null) coldLog("disk", `disk check: statfs unavailable, skipping pre-flight (need ~${fmtGB(need)} free)`)
  return withFileLock(`${live}.lock`, async () => {
    const stale = await cleanStaleTmps(live)
    if (stale > 0) coldLog("restore", `restore: removed ${stale} orphaned tmp file(s) from killed runs`)
    const tmp = `${live}.tmp.${process.pid}`
    await removeIfExists(tmp)
    await removeLiveSidecars(live)
    coldLog("restore (full)", "explicit recovery: full decompress (boot uses slim)", { archive, live })
    progress.start("restore-copy", "restore copy", null)
    await copyBytes(archive, tmp)
    progress.end("restore-copy")
    const restored = await restoreFile(tmp, false, { progress })
    coldLog("restore", `restore: resolved ${restored.parts} parts, ${restored.events} events`, { ...restored })
    const sessions = (await openRawDb(tmp, "ro").then(async (db) => {
      try {
        return db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM session`)?.n ?? 0
      } finally {
        db.close()
      }
    }).catch(() => 0))
    progress.start("publish", "publish live", null)
    await atomicPublish(tmp, live)
    await removeLiveSidecars(live)
    progress.end("publish")
    coldLog("done", `DONE ${live} (full restore from archive)`, { live })
    return { ...restored, sessions, phaseMs: progress.timings() }
  })
}

// Backwards-compatible boot entry: now slim. Kept under the old name so the
// startup path and existing callers need no changes.
export const restoreLiveFromArchive = materializeLiveSlim

// Evict: drop a session's heavy payloads from live after proving they match
// the archive exactly (byte compare of the resolved subtree). The header stays
// for browsing; the next open faults back in. Dirty sessions (live differs)
// refuse loud with "pack first" instead of losing writes.
//
// Atomicity: the compare + delete for each session runs inside one live write
// transaction, so a concurrent prompt either lands before the compare (evict
// then refuses dirty) or blocks until after the delete (its rows survive on a
// clean stub). The whole evict additionally holds the archive lock: a merge
// pack publishing mid-evict could otherwise drop the just-evicted payloads
// from the new archive while live no longer has them either.
export const evictSessions = async (archive: string, live: string, sessionIDs: readonly string[]): Promise<EvictDone> => {
  const ids = [...new Set(sessionIDs)]
  if (ids.length === 0) return { sessions: 0, phaseMs: {} }
  if (archive === live) fail("archive and live must differ")
  const started = Date.now()
  const { access } = await import("node:fs/promises")
  if (await access(archive).then(() => false, () => true)) fail(`archive not found: ${archive}`)
  if (await access(live).then(() => false, () => true)) {
    fail(`live database not found: ${live} (restart opencode once to re-materialize it from the archive, or restore explicitly with unpack --dst ${live} --force)`)
  }
  let evicted = 0
  await withFileLock(`${live}.lock`, async () => {
    await withFileLock(`${archive}.lock`, async () => {
      const archiveDb = await openRawDb(archive, "ro")
      try {
        const manifest = loadManifestLight(archiveDb, archive, false)
        const dictCache = new Map<string, Buffer>()
        const liveDb = await openRawDb(live, "rw")
        try {
          for (const id of ids) {
            if (!hasSessionHeader(liveDb, id)) continue
            if (!sessionIsResident(liveDb, id)) continue // Already a stub.
            if (!hasSessionHeader(archiveDb, id)) {
              fail(`evict ${id}: not in archive (pack first: opencode db pack --all)`)
            }
            const archived = readSessionHeavyFromArchive(archiveDb, manifest, dictCache, id)
            liveDb.exec("BEGIN IMMEDIATE")
            try {
              compareSessionHeavy(liveDb, id, archived)
              deleteSessionHeavy(liveDb, id)
              liveDb.exec("COMMIT")
            } catch (error) {
              try {
                liveDb.exec("ROLLBACK")
              } catch {
                // Best-effort; live keeps its rows on compare failure.
              }
              throw error
            }
            coldLog("evict", `evict: session ${id} now a stub (header kept, payloads dropped)`, { session: id })
            evicted += 1
          }
        } finally {
          liveDb.close()
        }
      } finally {
        archiveDb.close()
      }
    })
  })
  return { sessions: evicted, phaseMs: { totalMs: Date.now() - started } }
}

// Byte-compare one session's live heavy against the archive-resolved subtree
// in id order. Any difference (new messages, edited rows) means live is dirty:
// throws with a pack-first message instead of losing writes.
const compareSessionHeavy = (liveDb: RawDb, sessionID: string, archived: SessionHeavy): void => {
  const id = sessionID
  const liveMessages = liveDb.all<Record<string, unknown>>(`SELECT * FROM message WHERE session_id = ? ORDER BY id`, [id])
  if (liveMessages.length !== archived.messages.length) {
    fail(`evict ${id}: live has ${liveMessages.length} messages, archive has ${archived.messages.length} (pack first: opencode db pack --all)`)
  }
  for (let i = 0; i < liveMessages.length; i++) {
    if (JSON.stringify(liveMessages[i]) !== JSON.stringify(archived.messages[i])) {
      fail(`evict ${id}: message ${String(liveMessages[i]?.["id"] ?? i)} differs from archive (pack first)`)
    }
  }
  const liveParts = liveDb.all<{ id: string; data: string }>(`SELECT id, data FROM part WHERE session_id = ? ORDER BY id`, [id])
  if (liveParts.length !== archived.parts.length) {
    fail(`evict ${id}: live has ${liveParts.length} parts, archive has ${archived.parts.length} (pack first)`)
  }
  for (let i = 0; i < liveParts.length; i++) {
    const liveRow = liveParts[i]
    const archivedRow = archived.parts[i]
    if (!liveRow || !archivedRow || liveRow.id !== archivedRow.row["id"] || liveRow.data !== archivedRow.data) {
      fail(`evict ${id}: part ${String(liveRow?.id ?? i)} differs from archive (pack first)`)
    }
  }
  const liveEvents = liveDb.all<{ id: string; data: string }>(`SELECT id, data FROM event WHERE aggregate_id = ? ORDER BY id`, [id])
  if (liveEvents.length !== archived.events.length) {
    fail(`evict ${id}: live has ${liveEvents.length} events, archive has ${archived.events.length} (pack first)`)
  }
  for (let i = 0; i < liveEvents.length; i++) {
    const liveRow = liveEvents[i]
    const archivedRow = archived.events[i]
    if (!liveRow || !archivedRow || liveRow.id !== archivedRow.row["id"] || liveRow.data !== archivedRow.data) {
      fail(`evict ${id}: event ${String(liveRow?.id ?? i)} differs from archive (pack first)`)
    }
  }
  for (const [table, column, archivedRows] of [
    ["todo", "session_id", archived.todos],
    ["session_message", "session_id", archived.sessionMessages],
    ["session_input", "session_id", archived.sessionInputs],
    ["session_context_epoch", "session_id", archived.sessionEpochs],
    ["event_sequence", "aggregate_id", archived.eventSequences],
  ] as const) {
    let liveRows: Record<string, unknown>[]
    try {
      liveRows = liveDb.all<Record<string, unknown>>(`SELECT * FROM "${table}" WHERE "${column}" = ? ORDER BY rowid`, [id])
    } catch {
      if (archivedRows.length === 0) continue
      fail(`evict ${id}: live table ${table} missing but archive has ${archivedRows.length} rows (pack first)`)
    }
    if (liveRows.length !== archivedRows.length) {
      fail(`evict ${id}: table ${table} differs (live ${liveRows.length}, archive ${archivedRows.length}; pack first)`)
    }
    for (let i = 0; i < liveRows.length; i++) {
      if (JSON.stringify(liveRows[i]) !== JSON.stringify(archivedRows[i])) {
        fail(`evict ${id}: table ${table} row ${i} differs from archive (pack first)`)
      }
    }
  }
}

// ------------------------------------------------------------------ pack flow
// End-to-end v1 -> v2 conversion. Shared by `db pack` and the startup
// migration nudge so both paths snapshot, verify and publish identically.
// The v1 source is never opened writable: live sources are snapshotted with
// VACUUM INTO (WAL-safe), offline sources are byte-copied after refusing
// -wal/-shm sidecars. Publish is atomic (tmp + fsync + rename) behind a
// lockfile and happens only after a 0-diff self-verify gate.
//
// On-demand note: once migrated, live is SLIM (headers only + open working
// set). Packing the slim live directly would publish an archive missing every
// stub's payloads. Live-source packs to the MAIN archive therefore go through
// packLiveToArchive (merge-repack below): the archive is restored full, live's
// open/new/deleted sessions merge in, and the merged full file packs. Packs to
// a CUSTOM dst keep the direct path (explicit partial/inspection archives).
export interface PackFlowInput {
  readonly src: string
  readonly dst: string
  readonly allow: readonly string[] | null
  readonly minBytes: number
  readonly verify: boolean
  readonly treatAsLive: boolean
  /** undefined = synchronous pack (background-migration default: no CPU spike
   * beside a running TUI). 0 = auto (match the system), >1 = worker pool. */
  readonly jobs?: number
  /** When provided, every phase reports here and timings() feeds the panel. */
  readonly progress?: ProgressHandle
}

export interface PackFlowDone extends PackFileStats {
  readonly digest: string
  readonly phaseMs: Record<string, number>
}

export const packArchiveFlow = async (input: PackFlowInput): Promise<PackFlowDone> => {
  const { src, dst, allow, minBytes, verify, treatAsLive } = input
  if (src === dst) fail("src and dst must differ")
  const progress = input.progress ?? createProgress(nullSink())
  const { dirname } = await import("node:path")
  const room = await diskRoom(src, dirname(dst))
  if (room.free !== null && room.free < room.need) {
    fail(
      `disk space: ${(room.free / 1e9).toFixed(2)}GB free next to dst, need ~${(room.need / 1e9).toFixed(2)}GB (3x source); free space or shrink selection`,
    )
  }
  if (room.free === null) coldLog("disk", `disk check: statfs unavailable, skipping pre-flight (need ~${(room.need / 1e9).toFixed(2)}GB free)`)
  return withFileLock(`${dst}.lock`, async () => {
    coldLog("pack", `pack: ${src} -> ${dst} (${allow === null ? "all sessions" : `${allow.length} sessions`})`, {
      src,
      dst,
      sessions: allow === null ? "all" : allow.length,
    })
    const stale = await cleanStaleTmps(dst)
    if (stale > 0) coldLog("pack", `pack: removed ${stale} orphaned tmp file(s) from killed runs`)
    const tmp = `${dst}.tmp.${process.pid}`
    const verifyWork = `${tmp}.verify`
    const baseSnap = `${tmp}.base`
    await removeIfExists(tmp)
    await removeIfExists(verifyWork)
    await removeIfExists(baseSnap)
    progress.start("snapshot", "snapshot source", null)
    if (treatAsLive) {
      await snapshotLiveFile(src, tmp)
      coldLog("snapshot", `snapshot: VACUUM INTO tmp (WAL-safe)`)
    } else {
      await refuseWalSidecars(src)
      await copyBytes(src, tmp)
      coldLog("snapshot", `snapshot: byte copy (quiescent file)`)
    }
    progress.end("snapshot")
    const stats = await packFile(tmp, allow, minBytes, { jobs: input.jobs, progress })
    coldLog(
      "packed",
      `packed: ${stats.sessions} sessions, ${stats.partPointers} part ptr (+${stats.partRawFallback} raw), ` +
        `${stats.eventSlims} event slims (+${stats.eventRawFallback} raw), ${stats.blobs} blobs`,
      { ...stats },
    )
    if (verify) {
      coldLog("self-verify", `self-verify: restoring tmp + byte-compare vs source ...`)
      progress.start("self-verify-restore", "self-verify restore", null)
      await copyBytes(tmp, verifyWork)
      await restoreFile(verifyWork, true, { progress })
      progress.end("self-verify-restore")
      // Live sources move under us; compare against a fresh snapshot so
      // only the archived sessions are judged. Offline sources are
      // immutable: compare against the file itself.
      const baseFile = treatAsLive ? baseSnap : src
      if (treatAsLive) await snapshotLiveFile(src, baseSnap)
      progress.start("self-verify-compare", "self-verify compare", null)
      const { total, diffs, firsts } = await compareFiles(baseFile, verifyWork, allow, { progress })
      progress.end("self-verify-compare")
      coldLog("self-verify", `self-verify: ${total} rows, ${diffs} diffs`, { total, diffs })
      for (const line of firsts) coldLog("self-verify", `  ${line}`)
      if (diffs > 0) {
        fail(`self-verify FAILED: ${diffs} diffs (tmp kept: ${tmp}, verify kept: ${verifyWork})`)
      }
    } else {
      coldLog("self-verify", `self-verify SKIPPED (--no-verify)`)
    }
    // Anti-wipe (all paths): never publish an empty image over a full
    // archive. Packing an empty source is legitimate for fresh or custom
    // destinations; emptying the durable copy requires deleting the archive
    // file explicitly. (The merge path has its own earlier, more specific
    // refusal; this is the backstop for direct packs.)
    if (stats.sessions === 0) {
      let dstComplete = false
      let dstSessions = 0
      try {
        const dstDb = await openRawDb(dst, "ro")
        try {
          const fields = readMeta(dstDb)
          dstComplete = fields["version"] === FORMAT_VERSION && fields["complete"] === "1"
          const count = Number(fields["count_session"] ?? "0")
          dstSessions = Number.isInteger(count) && count > 0 ? count : 0
        } finally {
          dstDb.close()
        }
      } catch {
        // Missing/unreadable dst: fresh destination, proceed.
      }
      if (dstComplete && dstSessions > 0) {
        fail(
          `packed 0 sessions from ${src} but ${dst} holds ${dstSessions}; refusing to publish an empty archive over it ` +
            `(delete the archive file explicitly if that is really what you want)`,
        )
      }
    }
    await markComplete(tmp)
    progress.start("publish", "publish archive", null)
    await atomicPublish(tmp, dst)
    const digest = await writeSidecar(dst)
    progress.end("publish")
    await removeIfExists(verifyWork)
    await removeIfExists(baseSnap)
    coldLog("done", `DONE ${dst} (sha256=${digest.slice(0, 16)}...)`, { dst, digest })
    return { ...stats, digest, phaseMs: progress.timings() }
  })
}

// ------------------------------------------------------------------ merge pack
// Merges the slim live working set into a restored full image, then packs the
// result. Both tmp files are unpacked v1 layout (no pointers), so the merge is
// plain SQL — no blob work. Semantics per session:
//   - live has heavy rows (open/new/dirty): replace the full subtree.
//   - live has header only (stub): keep archived heavy, refresh the header.
//   - archived session missing in live: user deleted → drop from the image.
// Non-session tables (project, workspace, credentials, ...) merge as a
// superset via INSERT OR REPLACE (no deletes: tiny, and deletes there are
// out of scope for session storage).
const SESSION_SUBTREE = new Set(["session", "message", "part", "event", "event_sequence", "todo", "session_message", "session_input", "session_context_epoch"])

export const mergeLiveIntoFull = async (tmpLive: string, tmpFull: string): Promise<{ updated: number; deleted: number; keptStubs: number }> => {
  const db = await openRawDb(tmpFull, "rw")
  try {
    db.exec(`ATTACH DATABASE '${tmpLive.replace(/'/g, "''")}' AS src`)
    try {
      const srcIds = new Set(db.all<{ id: string }>(`SELECT id FROM src.session`).map((row) => row.id))
      const mainIds = new Set(db.all<{ id: string }>(`SELECT id FROM main.session`).map((row) => row.id))
      // Older/minimal layouts may lack some subtree tables: only touch tables
      // present on BOTH sides (pack tolerates the same drift).
      const srcTables = new Set(db.all<{ name: string }>(`SELECT name FROM src.sqlite_master WHERE type = 'table'`).map((row) => row.name))
      const mainTables = new Set(db.all<{ name: string }>(`SELECT name FROM main.sqlite_master WHERE type = 'table'`).map((row) => row.name))
      const shared = (table: string): boolean => srcTables.has(table) && mainTables.has(table)
      // Non-session tables first (no FK interplay with the subtree).
      for (const table of srcTables) {
        if (SESSION_SUBTREE.has(table)) continue
        if (table.startsWith("_keep")) continue
        if (table.startsWith("sqlite_")) continue
        if (!mainTables.has(table)) continue
        const srcCols = tableColumns(db, `src.${table}`)
        const mainCols = tableColumns(db, table)
        if (srcCols.length === 0 || srcCols.length !== mainCols.length || srcCols.some((col, i) => col !== mainCols[i])) {
          fail(`merge: table ${table} schema differs between live and archive image (live drift?)`)
        }
        db.run(`INSERT OR REPLACE INTO main."${table}" SELECT * FROM src."${table}"`)
      }
      const heavySessionTables = (["message", "part", "todo", "session_message", "session_input", "session_context_epoch"] as const).filter(shared)
      const heavyAggregateTables = (["event", "event_sequence"] as const).filter(shared)
      const countWhere = (qualified: string, column: string, id: string): number =>
        db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM ${qualified} WHERE "${column}" = ?`, [id])?.n ?? 0
      let updated = 0
      let keptStubs = 0
      db.exec("BEGIN IMMEDIATE")
      try {
        for (const id of srcIds) {
          const srcHeavy =
            (shared("message") ? countWhere("src.message", "session_id", id) : 0) +
            (shared("part") ? countWhere("src.part", "session_id", id) : 0) +
            (shared("event") ? countWhere("src.event", "aggregate_id", id) : 0)
          if (srcHeavy > 0 || !mainIds.has(id)) {
            // Open/new session: replace the whole subtree from live.
            for (const [table, column] of HEAVY_BY_SESSION) {
              if (shared(table)) db.run(`DELETE FROM main."${table}" WHERE "${column}" = ?`, [id])
            }
            for (const [table, column] of HEAVY_BY_AGGREGATE) {
              if (shared(table)) db.run(`DELETE FROM main."${table}" WHERE "${column}" = ?`, [id])
            }
            db.run(`DELETE FROM main.session WHERE id = ?`, [id])
            db.run(`INSERT INTO main.session SELECT * FROM src.session WHERE id = ?`, [id])
            for (const table of heavySessionTables) {
              db.run(`INSERT INTO main."${table}" SELECT * FROM src."${table}" WHERE "session_id" = ?`, [id])
            }
            for (const [table, column] of HEAVY_BY_AGGREGATE) {
              if (shared(table)) db.run(`INSERT INTO main."${table}" SELECT * FROM src."${table}" WHERE "${column}" = ?`, [id])
            }
            updated += 1
          } else {
            // Stub: keep archived heavy, refresh the header (title/metadata
            // edits while cold must survive the next pack).
            const cols = tableColumns(db, "session").filter((col) => col !== "id")
            if (cols.length > 0) {
              const set = cols.map((col) => `"${col}" = (SELECT "${col}" FROM src.session WHERE id = ?)`).join(", ")
              db.run(`UPDATE main.session SET ${set} WHERE id = ?`, [...cols.map(() => id), id])
            }
            keptStubs += 1
          }
        }
        for (const id of mainIds) {
          if (srcIds.has(id)) continue
          for (const [table, column] of HEAVY_BY_SESSION) {
            if (shared(table)) db.run(`DELETE FROM main."${table}" WHERE "${column}" = ?`, [id])
          }
          for (const [table, column] of HEAVY_BY_AGGREGATE) {
            if (shared(table)) db.run(`DELETE FROM main."${table}" WHERE "${column}" = ?`, [id])
          }
          db.run(`DELETE FROM main.session WHERE id = ?`, [id])
        }
        const deleted = [...mainIds].filter((id) => !srcIds.has(id)).length
        db.exec("COMMIT")
        return { updated, deleted, keptStubs }
      } catch (error) {
        try {
          db.exec("ROLLBACK")
        } catch {
          // Best-effort; tmp is unpublished on failure.
        }
        throw error
      }
    } finally {
      db.exec(`DETACH DATABASE src`)
    }
  } finally {
    db.close()
  }
}

export interface PackLiveInput {
  readonly live: string
  readonly archive: string
  readonly minBytes: number
  readonly verify: boolean
  readonly jobs?: number
  readonly progress?: ProgressHandle
}

// Restored-size estimate for disk pre-flights: packed bytes on disk plus the
// unpacked blob payloads a restore will materialize. Restores and merges
// transiently hold ~2x the restored size (work copy + VACUUM headroom), so
// callers add their own multipliers on top of file + blobs.
export const archiveRestoreBytes = async (archive: string): Promise<{ file: number; blobs: number }> => {
  const { stat } = await import("node:fs/promises")
  const file = (await stat(archive)).size
  const db = await openRawDb(archive, "ro")
  try {
    const blobs = db.get<{ n: number }>(`SELECT COALESCE(SUM(len), 0) AS n FROM blob`)?.n ?? 0
    return { file, blobs }
  } finally {
    db.close()
  }
}

export const diskRoomBytes = async (dir: string): Promise<number | null> => {
  const { statfs } = await import("node:fs/promises")
  try {
    const info = await statfs(dir)
    return Number(info.bfree) * Number(info.bsize)
  } catch {
    return null
  }
}

const fmtGB = (bytes: number): string => `${(bytes / 1e9).toFixed(2)}GB`

// Full merge-repack for the migrated world: restore the archive full, fold the
// live working set in, pack the result. O(archive) — packs are infrequent
// (manual / migration); session opens stay O(session) via fault-in. Returns
// the pack result plus merge counts.
export const packLiveToArchive = async (input: PackLiveInput): Promise<PackFlowDone & { mergedUpdated: number; mergedDeleted: number; mergedStubs: number }> => {
  const { live, archive, minBytes, verify } = input
  if (live === archive) fail("live and archive must differ")
  const progress = input.progress ?? createProgress(nullSink())
  const { dirname } = await import("node:path")
  const { access, stat } = await import("node:fs/promises")
  if (await access(live).then(() => false, () => true)) fail(`live database not found: ${live}`)
  // No complete archive yet: direct pack, no merge (first migration).
  const archivePresent = await access(archive).then(() => true, () => false)
  let archiveComplete = false
  let archiveSessions = 0
  if (archivePresent) {
    try {
      const db = await openRawDb(archive, "ro")
      try {
        const fields = readMeta(db)
        archiveComplete = fields["version"] === FORMAT_VERSION && fields["complete"] === "1"
        const count = Number(fields["count_session"] ?? "0")
        archiveSessions = Number.isInteger(count) && count > 0 ? count : 0
      } finally {
        db.close()
      }
    } catch {
      archiveComplete = false
    }
  }
  if (!archiveComplete) {
    coldLog("pack", "pack: no complete archive; packing live directly (first migration)")
    return { ...(await packArchiveFlow({ src: live, dst: archive, allow: null, minBytes, verify, treatAsLive: true, jobs: input.jobs, progress })), mergedUpdated: 0, mergedDeleted: 0, mergedStubs: 0 }
  }
  // Anti-wipe: an empty live file must never publish an empty archive over a
  // full one. This is exactly the world `db pack` sees when the live file was
  // wiped/recreated and the boot restore has not run (db commands skip the
  // startup middleware): refusing loud beats destroying the durable copy.
  const liveDb = await openRawDb(live, "ro")
  let liveSessions = 0
  try {
    liveSessions = liveDb.get<{ n: number }>(`SELECT COUNT(*) AS n FROM session`)?.n ?? 0
  } finally {
    liveDb.close()
  }
  if (liveSessions === 0 && archiveSessions > 0) {
    fail(
      `live ${live} holds 0 sessions but the archive holds ${archiveSessions}; refusing to publish an empty archive over it. ` +
        `If the live file was wiped, restart opencode once (boot re-indexes from the archive), then pack again.`,
    )
  }
  // Honest pre-flight: work copy (file) + restored image (file + blobs) +
  // live snapshot + pack tmp (file + blobs) + verify copy (file) + VACUUM
  // headroom (~restored size). Blobs dominate on large corpora, where the old
  // 4x-file estimate would strand a half-built merge on ENOSPC.
  const { file, blobs } = await archiveRestoreBytes(archive)
  const liveBytes = (await stat(live).catch(() => ({ size: 0 }))).size ?? 0
  const need = 4 * file + 2 * blobs + liveBytes
  const free = await diskRoomBytes(dirname(archive))
  if (free !== null && free < need) {
    fail(`disk space: ${fmtGB(free)} free next to archive, need ~${fmtGB(need)} (work copy + restored image + live snapshot + pack + verify + VACUUM headroom)`)
  }
  if (free === null) coldLog("disk", `disk check: statfs unavailable, skipping pre-flight (need ~${fmtGB(need)} free)`)
  coldLog("pack", `pack (merge): live working set -> full image -> ${archive} (archive ${fmtGB(file)} packed + ${fmtGB(blobs)} payloads; O(archive), infrequent)`)
  return withFileLock(`${archive}.lock`, async () => {
    const stale = await cleanStaleTmps(archive)
    if (stale > 0) coldLog("pack", `pack: removed ${stale} orphaned tmp file(s) from killed runs`)
    const base = `${archive}.tmp.${process.pid}`
    const tmpFull = `${base}.full`
    const tmpLive = `${base}.livesnap`
    const tmpPack = `${base}.pack`
    for (const file of [tmpFull, tmpLive, tmpPack, `${tmpPack}.verify`, `${tmpPack}.base`]) await removeIfExists(file)
    try {
      progress.start("merge-restore", "merge restore archive", null)
      await copyBytes(archive, tmpFull)
      await restoreFile(tmpFull, false, { progress })
      progress.end("merge-restore")
      progress.start("merge-snapshot", "merge snapshot live", null)
      await snapshotLiveFile(live, tmpLive)
      progress.end("merge-snapshot")
      progress.start("merge", "merge live into full", null)
      const merged = await mergeLiveIntoFull(tmpLive, tmpFull)
      progress.end("merge")
      coldLog("merge", `merge: ${merged.updated} live sessions folded in, ${merged.deleted} deleted, ${merged.keptStubs} stubs kept`, { ...merged })
      progress.start("pack-merged", "pack merged image", null)
      await refuseWalSidecars(tmpFull)
      await copyBytes(tmpFull, tmpPack)
      const stats = await packFile(tmpPack, null, minBytes, { jobs: input.jobs, progress })
      progress.end("pack-merged")
      if (verify) {
        coldLog("self-verify", `self-verify: restoring tmp + byte-compare vs merged image ...`)
        const verifyWork = `${tmpPack}.verify`
        await copyBytes(tmpPack, verifyWork)
        await restoreFile(verifyWork, true, { progress })
        const { total, diffs, firsts } = await compareFiles(tmpFull, verifyWork, null, { progress })
        coldLog("self-verify", `self-verify: ${total} rows, ${diffs} diffs`, { total, diffs })
        for (const line of firsts) coldLog("self-verify", `  ${line}`)
        if (diffs > 0) fail(`self-verify FAILED: ${diffs} diffs (merged image kept: ${tmpFull})`)
        await removeIfExists(verifyWork)
      }
      await markComplete(tmpPack)
      progress.start("publish", "publish archive", null)
      await atomicPublish(tmpPack, archive)
      const digest = await writeSidecar(archive)
      progress.end("publish")
      coldLog("done", `DONE ${archive} (merge pack, sha256=${digest.slice(0, 16)}...)`, { dst: archive, digest })
      return { ...stats, digest, phaseMs: progress.timings(), mergedUpdated: merged.updated, mergedDeleted: merged.deleted, mergedStubs: merged.keptStubs }
    } finally {
      for (const file of [tmpFull, tmpLive, tmpPack, `${tmpPack}.verify`, `${tmpPack}.base`]) await removeIfExists(file)
    }
  })
}

export * as SessionColdV2 from "./cold-v2"
