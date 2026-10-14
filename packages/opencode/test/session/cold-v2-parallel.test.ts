import { describe, expect, test } from "bun:test"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { rm, copyFile, mkdir, readFile, stat, writeFile } from "node:fs/promises"
import { createHash } from "node:crypto"
import { SessionColdV2 } from "@/session/cold-v2"
import { SessionColdV2Progress } from "@/session/cold-v2-progress"
import { SessionColdV2Workers } from "@/session/cold-v2-workers"
import { archivePathFor, autoEvictSettings, formatMigrationWarning, liveV2PathFor, maybeWarnColdV2Migration, migrationStatus } from "@/session/db-cold-v2-startup"
import type { MessageID } from "@/session/schema"

describe("parallel pack, progress and result screens", () => {
  const scratch = async (): Promise<{ dir: string; cleanup: () => Promise<void> }> => {
    const dir = join(tmpdir(), `opencode-cold-v2-par-${process.pid}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`)
    await mkdir(dir, { recursive: true })
    return { dir, cleanup: () => rm(dir, { recursive: true, force: true }) }
  }

  // Big enough to engage workers (PARALLEL_MIN_ROWS=500): 1200 parts + 600
  // pu1 events, mixing canonical rows, raw-fallback pairs, unicode and floats.
  const buildBigLive = async (dir: string, name: string): Promise<string> => {
    const file = join(dir, name)
    const db = await SessionColdV2.openRawDb(file, "rw")
    try {
      db.exec(`CREATE TABLE session (id TEXT PRIMARY KEY, project_id TEXT)`)
      db.exec(`CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT)`)
      db.exec(`CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, time_updated INTEGER, data TEXT)`)
      db.exec(`CREATE TABLE event (id TEXT PRIMARY KEY, aggregate_id TEXT, seq INTEGER, type TEXT, data TEXT)`)
      db.exec(`CREATE TABLE event_sequence (aggregate_id TEXT PRIMARY KEY, seq INTEGER)`)
      db.exec(`CREATE TABLE todo (session_id TEXT, content TEXT)`)
      db.exec(`CREATE TABLE session_message (id TEXT PRIMARY KEY, session_id TEXT)`)
      db.exec(`CREATE TABLE session_input (session_id TEXT)`)
      db.exec(`CREATE TABLE session_context_epoch (session_id TEXT)`)
      const J = (value: unknown): string => JSON.stringify(value)
      db.run(`INSERT INTO session VALUES (?, ?)`, ["s1", "proj-a"])
      db.run(`INSERT INTO message VALUES (?, ?)`, ["m1", "s1"])
      db.run(`INSERT INTO event_sequence VALUES (?, ?)`, ["s1", 601])
      const pad = (n: number): string => String(n).padStart(5, "0")
      for (let i = 0; i < 1200; i += 1) {
        const big = `TXT-${i}-${"x".repeat(2500)}`
        const data =
          i % 10 === 0
            ? J({ type: "t", b: `B-${big}`, a: `A-${big}` })
            : i % 10 === 1
              ? J({ type: "t", a: `A-${big}`, b: `B-${big}` })
              : J({ type: "text", text: big, time: { start: i, end: i + 1 }, score: i % 3 === 0 ? 3.14159 : i })
        db.run(`INSERT INTO part VALUES (?, ?, ?, ?, ?, ?)`, [`p-${pad(i)}`, "m1", "s1", i, i, data])
      }
      const env = (n: number, pid: string, part: unknown, time: number): string =>
        J({ sessionID: "s1", part: { id: pid, sessionID: "s1", messageID: "m1", ...(part as Record<string, unknown>) }, time })
      for (let i = 0; i < 600; i += 1) {
        db.run(`INSERT INTO event VALUES (?, ?, ?, ?, ?)`, [
          `e-${pad(i)}`,
          "s1",
          i + 1,
          "message.part.updated.1",
          env(i, `p-${pad(i)}`, { type: "text", text: `EV-${i}-${"e".repeat(2500)}`, time: { start: i, end: i + 1 } }, 1000 + i),
        ])
      }
    } finally {
      db.close()
    }
    return file
  }

  const dumpPacked = async (file: string): Promise<{ blob: string[]; ptr: string[]; tpl: string[] }> => {
    const db = await SessionColdV2.openRawDb(file, "ro")
    try {
      const blob = db
        .all<{ sha256: string; bytes: unknown; len: number; codec: string; raw: number }>(
          `SELECT sha256, bytes, len, codec, raw FROM blob ORDER BY sha256`,
        )
        .map((row) => {
          const bytes = Buffer.isBuffer(row.bytes) ? row.bytes : Buffer.from(row.bytes as Uint8Array)
          return JSON.stringify({ sha: row.sha256, hex: bytes.toString("hex"), len: row.len, codec: row.codec, raw: row.raw })
        })
      const ptr = db
        .all<{ t: string; id: string; sha: string }>(`SELECT t, id, sha FROM ptr ORDER BY t, id`)
        .map((row) => JSON.stringify(row))
      const tpl = db
        .all<Record<string, unknown>>(
          `SELECT ctx, type, tool, shape_json, path_json, order_json, cnt FROM tpl ORDER BY ctx, type, tool, shape_json, path_json, order_json`,
        )
        .map((row) => JSON.stringify(row))
      return { blob, ptr, tpl }
    } finally {
      db.close()
    }
  }

  test("resolveJobs matches the system and clamps explicit asks", () => {
    expect(SessionColdV2Workers.resolveJobs(undefined)).toBeGreaterThanOrEqual(1)
    expect(SessionColdV2Workers.resolveJobs(undefined)).toBeLessThanOrEqual(SessionColdV2Workers.MAX_AUTO_JOBS)
    expect(SessionColdV2Workers.resolveJobs(0)).toEqual(SessionColdV2Workers.resolveJobs(undefined))
    expect(SessionColdV2Workers.resolveJobs(1)).toBe(1)
    expect(SessionColdV2Workers.resolveJobs(4)).toBe(4)
    expect(SessionColdV2Workers.resolveJobs(1000)).toBe(SessionColdV2Workers.MAX_JOBS)
    expect(SessionColdV2Workers.wantsWorkers(undefined)).toBe(false)
    expect(SessionColdV2Workers.wantsWorkers(0)).toBe(true)
    expect(SessionColdV2Workers.wantsWorkers(1)).toBe(false)
    expect(SessionColdV2Workers.wantsWorkers(2)).toBe(true)
  })

  test("worker source builds from the real functions (no drift)", () => {
    const source = SessionColdV2Workers.buildWorkerSource(SessionColdV2.packRowFns(), SessionColdV2.IDS)
    for (const name of ["packPartRow", "packEventRow", "isReorderable", "canonValue", "zstdCompressSync", "parentPort"]) {
      expect(source).toContain(name)
    }
  })

  test("pool self-test passes on a learned store (or sync fallback is silent-safe)", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildBigLive(dir, "live.db")
      const tmp = join(dir, "tmp.db")
      await copyFile(live, tmp)
      const db = await SessionColdV2.openRawDb(tmp, "rw")
      try {
        const { store, envelopeOrder, wrapperOrder } = SessionColdV2.learnTemplates(db)
        const pool = await SessionColdV2Workers.createPackPool({
          size: 2,
          fns: SessionColdV2.packRowFns(),
          ids: SessionColdV2.IDS,
          store,
          envelopeOrder: [...envelopeOrder],
          wrapperOrder: [...wrapperOrder],
          onWarn: () => {},
        })
        if (pool === null) {
          // No worker_threads on this runtime: pack must still succeed via fallback.
          const stats = await SessionColdV2.packFile(tmp, null, 200, { jobs: 4 })
          expect(stats.partPointers).toBeGreaterThan(500)
          return
        }
        try {
          await SessionColdV2Workers.verifyPoolEquivalence(pool, SessionColdV2.packRowFns(), store, envelopeOrder, wrapperOrder, 200)
        } finally {
          await pool.close()
        }
      } finally {
        db.close()
      }
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("bundle worker source builds from the real functions (no drift)", () => {
    const source = SessionColdV2Workers.buildBundleWorkerSource({
      compressPlain: SessionColdV2.compressPlain,
      compressBundleChunk: SessionColdV2.compressBundleChunk,
    })
    for (const name of ["compressBundleChunk", "compressPlain", "zstdCompressSync", "parentPort"]) {
      expect(source).toContain(name)
    }
  })

  test("bundle pool self-test passes (or sync fallback stays correct)", async () => {
    const pool = await SessionColdV2Workers.createBundlePool({
      size: 2,
      fns: { compressPlain: SessionColdV2.compressPlain, compressBundleChunk: SessionColdV2.compressBundleChunk },
      onWarn: () => {},
    })
    try {
      if (pool === null) return // No worker_threads: sync path is the product.
      await SessionColdV2Workers.verifyBundlePoolEquivalence(pool, {
        compressPlain: SessionColdV2.compressPlain,
        compressBundleChunk: SessionColdV2.compressBundleChunk,
      })
      // Spot byte-compare through the pool on realistic chunk sizes.
      const plains = [new Uint8Array(Buffer.from(`POOL-${"q".repeat(200_000)}`, "utf8"))]
      const remote = await pool.compress(plains)
      const local = SessionColdV2.compressBundleChunk(plains[0]!)
      expect(remote).toHaveLength(1)
      expect(remote[0]!.codec).toBe(local.codec)
      expect(Buffer.from(remote[0]!.bytes).equals(local.bytes)).toBe(true)
    } finally {
      await pool?.close()
    }
  })

  test("pack with jobs=4 is byte-identical to jobs=1 and restores EXACT", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildBigLive(dir, "live.db")
      const syncFile = join(dir, "sync.db")
      const parFile = join(dir, "par.db")
      await copyFile(live, syncFile)
      await copyFile(live, parFile)
      const syncStats = await SessionColdV2.packFile(syncFile, null, 200, { jobs: 1 })
      const parStats = await SessionColdV2.packFile(parFile, null, 200, { jobs: 4 })
      expect(parStats).toEqual(syncStats)
      expect(parStats.partPointers).toBeGreaterThan(500)
      expect(parStats.partRawFallback).toBeGreaterThan(0)
      const [syncDump, parDump] = await Promise.all([dumpPacked(syncFile), dumpPacked(parFile)])
      expect(parDump).toEqual(syncDump)
      // The parallel archive is healthy end to end.
      await SessionColdV2.markComplete(parFile)
      const report = await SessionColdV2.verifyArchive(parFile)
      expect(report.blobs).toBe(parStats.blobs)
      const rest = join(dir, "rest.db")
      await copyFile(parFile, rest)
      await SessionColdV2.restoreFile(rest, false)
      const { total, diffs, firsts } = await SessionColdV2.compareFiles(live, rest, null)
      expect(firsts).toEqual([])
      expect(diffs).toBe(0)
      expect(total).toBeGreaterThan(1800)
    } finally {
      await cleanup()
    }
  }, 300_000)

  test("corrupt rows fail loud with identical messages on both paths", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildBigLive(dir, "live.db")
      const bad = join(dir, "bad.db")
      await copyFile(live, bad)
      const db = await SessionColdV2.openRawDb(bad, "rw")
      try {
        db.run(`UPDATE part SET data = ? WHERE id = ?`, [`{"broken":${"z".repeat(3000)}`, "p-00600"])
      } finally {
        db.close()
      }
      // Template learning parses every part row first, so corruption trips
      // there — identically on both paths (no worker divergence possible).
      const syncFile = join(dir, "bad-sync.db")
      const parFile = join(dir, "bad-par.db")
      await copyFile(bad, syncFile)
      await copyFile(bad, parFile)
      const syncErr = await SessionColdV2.packFile(syncFile, null, 200, { jobs: 1 }).then(
        () => "NO-ERROR",
        (error: unknown) => String((error as Error).message ?? error),
      )
      const parErr = await SessionColdV2.packFile(parFile, null, 200, { jobs: 4 }).then(
        () => "NO-ERROR",
        (error: unknown) => String((error as Error).message ?? error),
      )
      expect(syncErr).toMatch(/template learn: part row unparseable/)
      expect(parErr).toBe(syncErr)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("worker row errors propagate with row context (no silent drops)", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildBigLive(dir, "live.db")
      const db = await SessionColdV2.openRawDb(live, "rw")
      const { store, envelopeOrder, wrapperOrder } = SessionColdV2.learnTemplates(db)
      db.close()
      const pool = await SessionColdV2Workers.createPackPool({
        size: 2,
        fns: SessionColdV2.packRowFns(),
        ids: SessionColdV2.IDS,
        store,
        envelopeOrder: [...envelopeOrder],
        wrapperOrder: [...wrapperOrder],
        onWarn: () => {},
      })
      if (pool === null) return
      try {
        const [partErr] = await pool.run("part", [{ id: "p-bad", data: `{"broken":${"z".repeat(3000)}` }], 200)
        expect(partErr?.error).toMatch(/p-bad.*unparseable/)
        const [eventErr] = await pool.run("event", [{ id: "e-bad", data: `{"nope":1}` }], 0)
        expect(eventErr?.error).toMatch(/e-bad.*no part object/)
      } finally {
        await pool.close()
      }
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("progress sink sees every phase start and finish with totals", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildBigLive(dir, "live.db")
      const packed = join(dir, "cold.db")
      await copyFile(live, packed)
      const seen: SessionColdV2Progress.PhaseProgress[] = []
      const progress = SessionColdV2Progress.createProgress((event) => seen.push({ ...event }))
      await SessionColdV2.packFile(packed, null, 200, { jobs: 1, progress })
      const finished = new Map(seen.filter((event) => event.finished).map((event) => [event.phase, event]))
      for (const phase of ["learn-templates", "pack-parts", "pack-events", "hashes", "vacuum"]) {
        expect(finished.has(phase)).toBe(true)
      }
      expect(finished.get("pack-parts")?.done ?? -1).toBe(finished.get("pack-parts")?.total ?? -2)
      expect(finished.get("pack-parts")?.total ?? -1).toBe(1200)
      expect(finished.get("pack-events")?.done ?? -1).toBe(finished.get("pack-events")?.total ?? -2)
      expect(finished.get("pack-events")?.total ?? -1).toBe(600)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("packArchiveFlow reports phase timings for the result panel", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildBigLive(dir, "live.db")
      const dst = join(dir, "cold-v2.db")
      const done = await SessionColdV2.packArchiveFlow({ src: live, dst, allow: null, minBytes: 200, verify: false, treatAsLive: false })
      expect(done.digest).toMatch(/^[0-9a-f]{64}$/)
      for (const phase of ["snapshot", "pack-parts", "pack-events", "publish"]) {
        expect(typeof done.phaseMs[phase]).toBe("number")
      }
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("result panel renders and the continue guard never blocks headless", async () => {
    const panel = SessionColdV2Progress.formatResultPanel("pack done: cold.db", [
      ["sessions", "3"],
      ["sha256", "abc"],
    ])
    expect(panel).toContain("pack done: cold.db")
    expect(panel).toContain("sessions: 3")
    expect(panel).toContain("sha256: abc")
    expect(panel.startsWith("+")).toBe(true)
    await expect(SessionColdV2Progress.maybeWaitForContinue()).resolves.toBe(false)
    await expect(SessionColdV2Progress.maybeWaitForContinue({ wait: false })).resolves.toBe(false)
  })
})
