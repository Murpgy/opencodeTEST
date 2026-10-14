import { describe, expect, test } from "bun:test"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { rm, mkdir, writeFile } from "node:fs/promises"
import { SessionColdV2 } from "@/session/cold-v2"
import { ensureSessionTail } from "@/session/db-cold-v2-startup"

// Exercises the REAL tail router (ensureSessionTail), not faultInSessions
// directly: a prior revision skipped the fault on boolean presence, serving a
// 1-row single-part residue as the whole window on the exact path TUI opens
// take. Also pins the time_updated freshness bump the evict defense depends
// on (EXACT suites normalize clocks, so a dead bump would otherwise stay
// green forever).
describe("tail router (ensureSessionTail)", () => {
  const scratch = async (): Promise<{ dir: string; cleanup: () => Promise<void> }> => {
    const dir = join(tmpdir(), `opencode-cold-v2-router-${process.pid}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`)
    await mkdir(dir, { recursive: true })
    return { dir, cleanup: () => rm(dir, { recursive: true, force: true }) }
  }

  const buildLive = async (dir: string): Promise<string> => {
    const file = join(dir, "live.db")
    const db = await SessionColdV2.openRawDb(file, "rw")
    const J = (value: unknown): string => JSON.stringify(value)
    try {
      db.exec(`CREATE TABLE session (id TEXT PRIMARY KEY, parent_id TEXT, title TEXT, time_archived INTEGER, time_updated INTEGER, project_id TEXT)`)
      db.exec(`CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER)`)
      db.exec(`CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, data TEXT)`)
      db.exec(`CREATE TABLE event (id TEXT PRIMARY KEY, aggregate_id TEXT, seq INTEGER, type TEXT, data TEXT)`)
      db.exec(`CREATE TABLE event_sequence (aggregate_id TEXT PRIMARY KEY, seq INTEGER)`)
      db.exec(`CREATE TABLE todo (session_id TEXT, content TEXT)`)
      db.exec(`CREATE TABLE session_message (id TEXT PRIMARY KEY, session_id TEXT)`)
      db.exec(`CREATE TABLE session_input (session_id TEXT)`)
      db.exec(`CREATE TABLE session_context_epoch (session_id TEXT)`)
      db.run(`INSERT INTO session VALUES (?, ?, ?, ?, ?, ?)`, ["s-r", null, "router", null, Date.now() - 2 * 3600 * 1000, "proj-a"])
      const pad = (n: number): string => String(n).padStart(2, "0")
      for (let i = 0; i < 16; i += 1) {
        const mid = `m-${pad(i)}`
        db.run(`INSERT INTO message VALUES (?, ?, ?)`, [mid, "s-r", 1000 + i])
        db.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, [`p-${pad(i)}`, mid, "s-r", J({ type: "text", text: `R-${i}-${"x".repeat(2500)}` })])
      }
    } finally {
      db.close()
    }
    return file
  }

  const timeUpdated = async (live: string): Promise<number> => {
    const db = await SessionColdV2.openRawDb(live, "ro")
    try {
      return db.get<{ time_updated: number }>(`SELECT time_updated FROM session WHERE id = 's-r'`)?.time_updated ?? 0
    } finally {
      db.close()
    }
  }

  test("router serves the window after a single-part fault (no truncation)", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildLive(dir)
      const archive = join(dir, "archive.db")
      await SessionColdV2.packArchiveFlow({ src: live, dst: archive, allow: null, minBytes: 200, verify: false, treatAsLive: false })
      const stub = await SessionColdV2.openRawDb(live, "rw")
      try {
        for (const table of ["message", "part", "event", "todo", "session_message", "session_input", "session_context_epoch", "event_sequence"]) {
          stub.exec(`DELETE FROM "${table}"`)
        }
      } finally {
        stub.close()
      }
      const before = await timeUpdated(live)
      // Streaming tool-delta faults one part: 1 message resident, partial.
      expect((await SessionColdV2.faultInMessagePart(archive, live, "s-r", "m-00", "p-00")).faulted).toBe(true)
      // The freshness bump fired (the evict-mid-read defense depends on it).
      expect(await timeUpdated(live)).toBeGreaterThan(before)
      // The ROUTER (not faultInSessions directly) must serve the window.
      await ensureSessionTail(["s-r"], 5, { live, archive })
      const db = await SessionColdV2.openRawDb(live, "ro")
      try {
        const n = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM message WHERE session_id = 's-r'`)?.n ?? 0
        expect(n).toBeGreaterThanOrEqual(5 + SessionColdV2.TAIL_OVERLAP)
        const ids = db.all<{ id: string }>(`SELECT id FROM message WHERE session_id = 's-r' ORDER BY id`).map((row) => row.id)
        expect(ids).toContain("m-15")
        expect(db.get<{ complete: number }>(`SELECT complete FROM fault_state WHERE session_id = 's-r'`)?.complete).toBe(0)
      } finally {
        db.close()
      }
    } finally {
      await cleanup()
    }
  }, 300_000)

  test("router skips a covered window without faulting", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildLive(dir)
      const archive = join(dir, "archive.db")
      await SessionColdV2.packArchiveFlow({ src: live, dst: archive, allow: null, minBytes: 200, verify: false, treatAsLive: false })
      // Live is full (never stubbed): the router must not fault at all.
      await ensureSessionTail(["s-r"], 5, { live, archive })
      const db = await SessionColdV2.openRawDb(live, "ro")
      try {
        expect(db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM message WHERE session_id = 's-r'`)?.n).toBe(16)
      } finally {
        db.close()
      }
    } finally {
      await cleanup()
    }
  }, 300_000)

  test("mutating global gate waits on restore/slim, proceeds on read-only tags", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = join(dir, "live.db")
      await writeFile(live, "x")
      await SessionColdV2.gateLiveMutatingOp(live, 1000)
      const proc = Bun.spawn(["true"])
      await proc.exited
      await writeFile(`${live}.lock`, `${proc.pid}:nonce:slim`)
      const t0 = Date.now()
      await SessionColdV2.gateLiveMutatingOp(live, 5000)
      expect(Date.now() - t0).toBeLessThan(4000)
      await writeFile(`${live}.lock`, `${process.pid}:nonce:pack`)
      await SessionColdV2.gateLiveMutatingOp(live, 1000)
      // CLI unpack targeting live tags "unpack" (mutating); unpack to a side
      // file tags "unpack-dst" (snapshot semantics — no wait).
      await writeFile(`${live}.lock`, `${process.pid}:nonce:unpack-dst`)
      await SessionColdV2.gateLiveMutatingOp(live, 1000)
    } finally {
      await cleanup()
    }
  }, 120_000)
})
