// Parallel CPU pool for v2 cold-storage packing.
//
// Design (see the threading brainstorm): the main thread owns every SQLite
// handle, every file and all console output. Workers are pure functions —
// JSON bytes in, compressed blobs out. They never touch the database, the
// filesystem, or the progress sink. That single rule removes the entire
// SQLite-threading hazard class: the worst a worker can do is return wrong
// bytes, and the pack self-verify gate (restore + 0-diff byte-compare before
// publish) catches that before anything ships.
//
// The worker script is assembled from the ACTUAL exported pure functions in
// `./cold-v2` via `fn.toString()`, emitted under their runtime `fn.name`
// (stable under bundler minification). No logic is duplicated, so the two
// sides cannot drift — and a startup self-test (worker vs main on torture
// rows, byte-compared) trips loudly to synchronous fallback if any runtime
// ever breaks the serialization assumption. Unavailable workers (old
// runtimes, spawn failure, self-test mismatch) degrade to sync, never fail.
import type { Ctx, Json, PackRowOut, TemplateOrder, TemplateStore } from "./cold-v2"
// Every pure function the worker needs, passed in by cold-v2 so this module
// has no runtime import of cold-v2 (import cycle would break the bundle).
export interface PackRowFns {
  readonly parseJson: (text: string) => Json
  readonly canonJson: (value: Json) => string
  readonly isObject: (value: unknown) => value is { [key: string]: Json }
  readonly canonValue: (value: Json, depth?: number) => Json
  readonly skippedRow: () => PackRowOut
  readonly errorRow: (message: string) => PackRowOut
  readonly templateKey: (ctx: Ctx, type: string, tool: string, shape: readonly string[], path: readonly string[]) => string
  readonly ordersFor: (
    store: TemplateStore,
    ctx: Ctx,
    type: string,
    tool: string,
    shape: readonly string[],
    path: readonly string[],
  ) => TemplateOrder[] | undefined
  readonly sameOrder: (a: readonly string[], b: readonly string[]) => boolean
  readonly templateOrder: (
    store: TemplateStore,
    ctx: Ctx,
    type: string,
    tool: string,
    shape: readonly string[],
    path: readonly string[],
    keys: readonly string[],
  ) => readonly string[] | undefined
  readonly isReorderable: (
    store: TemplateStore,
    ctx: Ctx,
    value: { [key: string]: Json },
    type: string,
    tool: string,
    shape: readonly string[],
  ) => boolean
  readonly compressPlain: (plain: Uint8Array) => Uint8Array
  readonly blobDigest: (raw: boolean, plain: Uint8Array) => string
  readonly packPartRow: (store: TemplateStore, minBytes: number, id: string, data: string) => PackRowOut
  readonly packEventRow: (
    store: TemplateStore,
    envelopeOrder: readonly string[],
    wrapperOrder: readonly string[],
    id: string,
    data: string,
  ) => PackRowOut
}

export interface PackTaskRow {
  readonly id: string
  readonly data: string
}

export interface PackResultRow {
  readonly id: string
  /** Compressed bytes (transferred, not copied). Absent when skipped/error. */
  readonly comp: Uint8Array | null
  readonly sha: string
  readonly plainLen: number
  readonly raw: boolean
  readonly skipped: boolean
  readonly error: string | null
  /** Slim id fields for event rows (null for parts). Plain JSON, cloned. */
  readonly slim: { sid: string; time: Json; pid: string; mid: string } | null
}

export interface PackPool {
  readonly size: number
  /** Order-preserving: results[i] corresponds to rows[i]. */
  readonly run: (kind: "part" | "event", rows: readonly PackTaskRow[], minBytes: number) => Promise<PackResultRow[]>
  readonly close: () => Promise<void>
}

// ------------------------------------------------------------------ job count
// Default matches the system: every core gets work, minus one left for the
// main thread (SQLite IO + orchestration) and the OS. Capped at 8 — past that
// the single-writer page commit serializes anyway and extra workers only burn
// memory on in-flight pages. 0/undefined = auto, 1 = synchronous (no workers).
export const MAX_AUTO_JOBS = 8
export const MAX_JOBS = 16

const systemParallelism = (): number => {
  try {
    const os = require("node:os") as typeof import("node:os")
    const n = os.availableParallelism()
    if (Number.isInteger(n) && n > 0) return n
  } catch {
    // Fall through to cpus().
  }
  try {
    const os = require("node:os") as typeof import("node:os")
    const n = os.cpus().length
    if (Number.isInteger(n) && n > 0) return n
  } catch {
    // Last resort.
  }
  return 4
}

export const resolveJobs = (requested?: number): number => {
  if (requested !== undefined && requested !== null && Number.isFinite(requested) && requested >= 1) {
    return Math.min(Math.floor(requested), MAX_JOBS)
  }
  // Auto (0/undefined/NaN/anything else): match the system.
  return Math.max(1, Math.min(systemParallelism() - 1, MAX_AUTO_JOBS))
}

// True when the caller should spawn workers at all. jobs<=1 runs the exact
// same row functions synchronously on the main thread (also the unit-test and
// background-migration path: no CPU spike, no IPC overhead on tiny inputs).
export const wantsWorkers = (requested?: number): boolean => (requested ?? 0) > 1

// ------------------------------------------------------------ worker source
// Fixed emission order: const TDZ inside the eval scope means a function must
// be defined before any serialized caller references it at CALL time —
// definition order only needs every name bound before the first task runs,
// which holds as long as all emissions precede the protocol handler. The
// explicit order below is belt-and-braces readability, not load-bearing.
const FN_ORDER = [
  "parseJson",
  "canonJson",
  "isObject",
  "canonValue",
  "skippedRow",
  "errorRow",
  "templateKey",
  "ordersFor",
  "sameOrder",
  "templateOrder",
  "isReorderable",
  "compressPlain",
  "blobDigest",
  "packPartRow",
  "packEventRow",
] as const

type FnValue = (...args: never[]) => unknown

export const buildWorkerSource = (fns: PackRowFns, ids: readonly string[]): string => {
  const byName = new Map<string, FnValue>()
  for (const value of Object.values(fns)) {
    const fn = value as FnValue
    if (typeof fn !== "function" || !fn.name) throw new Error(`pack worker: refusing to serialize anonymous/none-function`)
    byName.set(fn.name, fn)
  }
  const parts: string[] = [
    `const { parentPort } = require("node:worker_threads");`,
    `const { Buffer } = require("node:buffer");`,
    `const { createHash } = require("node:crypto");`,
    `const { constants: zlibConstants, zstdCompressSync } = require("node:zlib");`,
    `const IDS = ${JSON.stringify([...ids])};`,
    `const idSet = new Set(IDS);`,
  ]
  for (const key of FN_ORDER) {
    const fn = byName.get(key)
    if (!fn) throw new Error(`pack worker: missing required function ${key}`)
    // Parenthesized: valid for arrow and function-expression sources alike,
    // including minified output. Emitted under fn.name, which the bundler
    // renames consistently with the references inside the other bodies.
    parts.push(`const ${fn.name} = (${fn.toString()});`)
  }
  parts.push(`
let STORE = null, ENVELOPE = [], WRAPPER = [];
parentPort.on("message", (msg) => {
  if (msg.type === "init") {
    STORE = msg.store;
    ENVELOPE = msg.envelopeOrder;
    WRAPPER = msg.wrapperOrder;
    parentPort.postMessage({ type: "ready" });
    return;
  }
  try {
    const out = [];
    const transfer = [];
    for (const row of msg.rows) {
      const r = msg.kind === "part"
        ? packPartRow(STORE, msg.minBytes, row.id, row.data)
        : packEventRow(STORE, ENVELOPE, WRAPPER, row.id, row.data);
      if (r.kind === "error") { out.push({ id: row.id, comp: null, sha: "", plainLen: 0, raw: false, skipped: false, error: r.message, slim: null }); continue; }
      if (r.kind === "skip") { out.push({ id: row.id, comp: null, sha: "", plainLen: 0, raw: false, skipped: true, error: null, slim: null }); continue; }
      const comp = compressPlain(r.plain);
      const u8 = new Uint8Array(comp);
      transfer.push(u8.buffer);
      out.push({ id: row.id, comp: u8, sha: blobDigest(r.raw, r.plain), plainLen: r.plain.length, raw: r.raw, skipped: false, error: null, slim: r.slim });
    }
    parentPort.postMessage({ type: "rows", seq: msg.seq, results: out }, transfer);
  } catch (error) {
    parentPort.postMessage({ type: "rows", seq: msg.seq, error: String((error && error.message) || error) });
  }
});
`)
  return parts.join("\n")
}

// ------------------------------------------------------------------ pool
interface WorkerHandle {
  readonly worker: {
    postMessage(value: unknown, transfer?: ArrayBuffer[]): void
    on(event: string, listener: (msg: unknown) => void): void
    terminate(): Promise<number>
  }
  ready: Promise<void>
}

const TASK_TIMEOUT_MS = 10 * 60 * 1000

export interface CreatePoolInput {
  readonly size: number
  readonly fns: PackRowFns
  readonly ids: readonly string[]
  readonly store: TemplateStore
  readonly envelopeOrder: readonly string[]
  readonly wrapperOrder: readonly string[]
  readonly onWarn: (message: string) => void
}

// Returns null when workers are unavailable for any reason (no
// worker_threads, spawn failure, init timeout): the caller falls back to the
// synchronous path. Never throws for infrastructure reasons.
export const createPackPool = async (input: CreatePoolInput): Promise<PackPool | null> => {
  let WorkerCtor: new (source: string, opts: { eval: true }) => WorkerHandle["worker"]
  try {
    const mod = (await import("node:worker_threads")) as typeof import("node:worker_threads")
    WorkerCtor = mod.Worker as unknown as typeof WorkerCtor
  } catch (error) {
    input.onWarn(`pack workers unavailable (no worker_threads: ${error instanceof Error ? error.message : String(error)}); packing synchronously`)
    return null
  }
  let source: string
  try {
    source = buildWorkerSource(input.fns, input.ids)
  } catch (error) {
    input.onWarn(`pack workers disabled (source build: ${error instanceof Error ? error.message : String(error)}); packing synchronously`)
    return null
  }
  const handles: WorkerHandle[] = []
  try {
    for (let i = 0; i < input.size; i += 1) {
      const worker = new WorkerCtor(source, { eval: true })
      const ready = new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`worker ${i} init timeout`)), 10_000)
        worker.on("message", (msg) => {
          const m = msg as { type?: string }
          if (m && m.type === "ready") {
            clearTimeout(timer)
            resolve()
          }
        })
        worker.on("error", (msg) => {
          clearTimeout(timer)
          reject(msg instanceof Error ? msg : new Error(String(msg)))
        })
      })
      handles.push({ worker, ready })
    }
    // Broadcast the template store once per worker (structured-cloned Map).
    // Sent before awaiting readiness: the worker replies ready after init.
    for (const handle of handles) {
      handle.worker.postMessage({
        type: "init",
        store: input.store,
        envelopeOrder: [...input.envelopeOrder],
        wrapperOrder: [...input.wrapperOrder],
      })
    }
    await Promise.all(handles.map((handle) => handle.ready))
  } catch (error) {
    for (const handle of handles) {
      try {
        await handle.worker.terminate()
      } catch {
        // Best-effort.
      }
    }
    input.onWarn(`pack workers disabled (spawn/init: ${error instanceof Error ? error.message : String(error)}); packing synchronously`)
    return null
  }

  let seq = 0
  let cursor = 0
  let closed = false
  const pending = new Map<number, (msg: { results?: PackResultRow[]; error?: string }) => void>()
  for (const handle of handles) {
    handle.worker.on("message", (raw) => {
      const msg = raw as { type?: string; seq?: number; results?: PackResultRow[]; error?: string }
      if (!msg || msg.type !== "rows" || typeof msg.seq !== "number") return
      const resolve = pending.get(msg.seq)
      if (!resolve) return
      pending.delete(msg.seq)
      resolve(msg)
    })
    handle.worker.on("error", (raw) => {
      const error = raw instanceof Error ? raw : new Error(String(raw))
      for (const [, resolve] of pending) resolve({ error: `pack worker crashed: ${error.message}` })
      pending.clear()
    })
  }

  return {
    size: handles.length,
    run: (kind, rows, minBytes) => {
      if (closed) return Promise.reject(new Error("pack pool is closed"))
      // Contiguous slices per worker preserve row order trivially on gather.
      const chunks: { worker: number; rows: PackTaskRow[] }[] = []
      const per = Math.max(1, Math.ceil(rows.length / handles.length))
      for (let i = 0; i < rows.length; i += per) {
        chunks.push({ worker: cursor % handles.length, rows: rows.slice(i, i + per) as PackTaskRow[] })
        cursor += 1
      }
      return Promise.all(
        chunks.map(
          (chunk) =>
            new Promise<PackResultRow[]>((resolve, reject) => {
              const id = seq
              seq += 1
              const timer = setTimeout(() => {
                pending.delete(id)
                reject(new Error(`pack worker task timed out after ${TASK_TIMEOUT_MS / 60000}min (${chunk.rows.length} rows); failing loud, tmp unpublished`))
              }, TASK_TIMEOUT_MS)
              pending.set(id, (msg) => {
                clearTimeout(timer)
                if (msg.error) reject(new Error(msg.error))
                else resolve(msg.results ?? [])
              })
              try {
                handles[chunk.worker]?.worker.postMessage({ type: "rows", seq: id, kind, rows: chunk.rows, minBytes })
              } catch (error) {
                pending.delete(id)
                clearTimeout(timer)
                reject(error instanceof Error ? error : new Error(String(error)))
              }
            }),
        ),
      ).then((lists) => lists.flat())
    },
    close: async () => {
      closed = true
      for (const [, resolve] of pending) resolve({ error: "pack pool closed" })
      pending.clear()
      for (const handle of handles) {
        try {
          await handle.worker.terminate()
        } catch {
          // Best-effort.
        }
      }
    },
  }
}

// Startup equivalence gate: runs torture rows through worker[0] and the local
// functions and byte-compares everything (kind, text inputs aside, sha,
// raw flag, lengths AND compressed bytes — zstd is deterministic for fixed
// input/level). Throws on any divergence; the caller degrades to sync.
export const verifyPoolEquivalence = async (
  pool: PackPool,
  fns: PackRowFns,
  store: TemplateStore,
  envelopeOrder: readonly string[],
  wrapperOrder: readonly string[],
  minBytes: number,
): Promise<void> => {
  const big = `EQ-${"q".repeat(3000)}`
  const tasks: { kind: "part" | "event"; id: string; data: string }[] = [
    { kind: "part", id: "eq-small", data: JSON.stringify({ type: "text", text: "hi" }) },
    { kind: "part", id: "eq-big", data: JSON.stringify({ type: "text", text: big, time: { start: 1, end: 2 } }) },
    { kind: "part", id: "eq-raw", data: JSON.stringify({ b: big, a: big, type: "t" }) },
  ]
  const local = tasks.map((task) =>
    task.kind === "part"
      ? { task, out: fns.packPartRow(store, minBytes, task.id, task.data) }
      : { task, out: fns.packEventRow(store, envelopeOrder, wrapperOrder, task.id, task.data) },
  )
  const remote = await pool.run(
    "part",
    tasks.map((task) => ({ id: task.id, data: task.data })),
    minBytes,
  )
  if (remote.length !== local.length) throw new Error(`pool self-test: got ${remote.length} results for ${local.length} rows`)
  for (let i = 0; i < local.length; i += 1) {
    const want = local[i]?.out
    const got = remote[i]
    if (!want || !got) throw new Error(`pool self-test: missing row ${i}`)
    if (want.kind !== (got.error ? "error" : got.skipped ? "skip" : "packed")) {
      throw new Error(`pool self-test: kind diverged on ${tasks[i]?.id} (local=${want.kind})`)
    }
    if (want.kind !== "packed" || !got.comp) continue
    const wantComp = fns.compressPlain(want.plain)
    const wantSha = fns.blobDigest(want.raw, want.plain)
    if (got.sha !== wantSha || got.raw !== want.raw || got.plainLen !== want.plain.length) {
      throw new Error(`pool self-test: digest diverged on ${tasks[i]?.id}`)
    }
    if (got.comp.length !== wantComp.length || !got.comp.every((byte, j) => byte === wantComp[j])) {
      throw new Error(`pool self-test: compressed bytes diverged on ${tasks[i]?.id}`)
    }
  }
}

export * as SessionColdV2Workers from "./cold-v2-workers"
