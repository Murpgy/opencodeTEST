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

describe("file locks", () => {
  const scratch = async (): Promise<{ dir: string; cleanup: () => Promise<void> }> => {
    const dir = join(tmpdir(), `opencode-cold-v2-lock-${process.pid}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`)
    await mkdir(dir, { recursive: true })
    return { dir, cleanup: () => rm(dir, { recursive: true, force: true }) }
  }

  test("empty and garbage lock files are reaped (kill between create and pid-write)", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const target = join(dir, "probe.txt")
      // 0-byte lock: the production wedge that blanked every cold open.
      await writeFile(`${target}.lock`, "")
      expect(await SessionColdV2.withFileLock(`${target}.lock`, async () => "ran")).toBe("ran")
      expect(await Bun.file(`${target}.lock`).exists()).toBe(false)
      // Garbage content reaps the same way.
      await writeFile(`${target}.lock`, "not-a-pid\n")
      expect(await SessionColdV2.withFileLock(`${target}.lock`, async () => "ran")).toBe("ran")
      expect(await Bun.file(`${target}.lock`).exists()).toBe(false)
    } finally {
      await cleanup()
    }
  }, 120_000)

  test("dead-pid locks reap, live holders fail loud and keep their lock", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const target = join(dir, "probe.txt")
      // Guaranteed-dead pid: spawned and reaped, not a magic constant that
      // could theoretically be recycled on a busy host.
      const proc = Bun.spawn(["true"])
      await proc.exited
      await writeFile(`${target}.lock`, String(proc.pid))
      expect(await SessionColdV2.withFileLock(`${target}.lock`, async () => "ran")).toBe("ran")
      expect(await Bun.file(`${target}.lock`).exists()).toBe(false)
      // Own pid = live holder: loud failure, lock file untouched.
      await writeFile(`${target}.lock`, String(process.pid))
      await expect(SessionColdV2.withFileLock(`${target}.lock`, async () => "ran")).rejects.toThrow(/lock held/)
      expect(await Bun.file(`${target}.lock`).text()).toBe(String(process.pid))
    } finally {
      await cleanup()
    }
  }, 120_000)

  // Two sessions with pointer-backed parts (large enough to exceed minBytes):
  // exercises the phase-3 re-checks (concurrent full landing before a tail,
  // tail landing before a full) deterministically, without timing races.
  const buildLockLive = async (dir: string): Promise<string> => {
    const file = join(dir, "live.db")
    const db = await SessionColdV2.openRawDb(file, "rw")
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
      const J = (value: unknown): string => JSON.stringify(value)
      for (const sid of ["s-a", "s-b"]) {
        db.run(`INSERT INTO session VALUES (?, ?, ?, ?, ?, ?)`, [sid, null, sid, null, Date.now(), "proj-a"])
        for (let i = 0; i < 4; i += 1) {
          const mid = `m-${sid}-${i}`
          db.run(`INSERT INTO message VALUES (?, ?, ?)`, [mid, sid, 1000 + i])
          db.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, [`p-${sid}-${i}`, mid, sid, J({ type: "text", text: `${sid}-${i}-${"x".repeat(2500)}` })])
        }
      }
    } finally {
      db.close()
    }
    return file
  }

  const stubLive = async (live: string): Promise<void> => {
    const db = await SessionColdV2.openRawDb(live, "rw")
    try {
      for (const table of ["message", "part", "event", "todo", "session_message", "session_input", "session_context_epoch", "event_sequence"]) {
        db.exec(`DELETE FROM "${table}"`)
      }
    } finally {
      db.close()
    }
  }

  test("tail fault after a concurrent full fault is a no-op (still complete)", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildLockLive(dir)
      const archive = join(dir, "archive.db")
      await SessionColdV2.packArchiveFlow({ src: live, dst: archive, allow: null, minBytes: 200, verify: false, treatAsLive: false })
      await stubLive(live)
      // "Concurrent" full lands first; the tail's phase-3 re-check must see
      // residency and skip instead of regressing complete -> partial.
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-a"])).sessions).toBe(1)
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-a"], { tailMessages: 2 })).sessions).toBe(0)
      const db = await SessionColdV2.openRawDb(live, "ro")
      try {
        expect(db.get<{ complete: number }>(`SELECT complete FROM fault_state WHERE session_id = ?`, ["s-a"])?.complete).toBe(1)
      } finally {
        db.close()
      }
      const { diffs } = await SessionColdV2.compareFiles(live, live, null)
      expect(diffs).toBe(0)
    } finally {
      await cleanup()
    }
  }, 300_000)

  test("full fault after a tail fault completes EXACT", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildWideLive(dir)
      const archive = join(dir, "archive.db")
      await SessionColdV2.packArchiveFlow({ src: live, dst: archive, allow: null, minBytes: 200, verify: false, treatAsLive: false })
      const origin = join(dir, "origin.db")
      await copyFile(live, origin)
      await stubLive(live)
      // 16 messages, window 2 (+10 overlap = 12 < 16): a genuine partial.
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-w"], { tailMessages: 2 })).sessions).toBe(1)
      expect(await liveCounts(live, "s-w")).toMatchObject({ messages: 12, complete: 0 })
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-w"])).sessions).toBe(1)
      await syncSessionTimes(origin, live)
      const { diffs, firsts } = await SessionColdV2.compareFiles(origin, live, null)
      expect(firsts).toEqual([])
      expect(diffs).toBe(0)
    } finally {
      await cleanup()
    }
  }, 300_000)
  // 16-message session with large parts (packs to pointers, faults through
  // every resolution branch): the truncation + promotion + markerless tests
  // need a session comfortably larger than any tail window under test.
  const buildWideLive = async (dir: string): Promise<string> => {
    const file = join(dir, "wide.db")
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
      db.run(`INSERT INTO session VALUES (?, ?, ?, ?, ?, ?)`, ["s-w", null, "wide", null, Date.now(), "proj-a"])
      const pad = (n: number): string => String(n).padStart(2, "0")
      for (let i = 0; i < 16; i += 1) {
        const mid = `m-${pad(i)}`
        db.run(`INSERT INTO message VALUES (?, ?, ?)`, [mid, "s-w", 1000 + i])
        db.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, [`p-${pad(i)}-a`, mid, "s-w", J({ type: "text", text: `W-${i}-A-${"x".repeat(2500)}` })])
        db.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, [`p-${pad(i)}-b`, mid, "s-w", J({ type: "text", text: `W-${i}-B-${"y".repeat(2500)}` })])
      }
    } finally {
      db.close()
    }
    return file
  }

  const liveCounts = async (live: string, sid: string): Promise<{ messages: number; complete: number | null }> => {
    const db = await SessionColdV2.openRawDb(live, "ro")
    try {
      const messages = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM message WHERE session_id = ?`, [sid])?.n ?? 0
      let complete: number | null = null
      try {
        complete = db.get<{ complete: number }>(`SELECT complete FROM fault_state WHERE session_id = ?`, [sid])?.complete ?? null
      } catch {
        complete = null
      }
      return { messages, complete }
    } finally {
      db.close()
    }
  }

  // Faults bump session.time_updated (idle-clock freshness against the
  // auto-evict sweep), so payload-EXACT compares normalize the activity clock
  // first: time_updated is MEANT to move on read, unlike every other column.
  const syncSessionTimes = async (origin: string, live: string): Promise<void> => {
    const liveDb = await SessionColdV2.openRawDb(live, "ro")
    try {
      const rows = liveDb.all<{ id: string; time_updated: number }>(`SELECT id, time_updated FROM session`)
      const originDb = await SessionColdV2.openRawDb(origin, "rw")
      try {
        for (const row of rows) originDb.run(`UPDATE session SET time_updated = ? WHERE id = ?`, [row.time_updated, row.id])
      } finally {
        originDb.close()
      }
    } finally {
      liveDb.close()
    }
  }

  test("single-part fault then tail open serves the full window (no truncation)", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildWideLive(dir)
      const archive = join(dir, "archive.db")
      await SessionColdV2.packArchiveFlow({ src: live, dst: archive, allow: null, minBytes: 200, verify: false, treatAsLive: false })
      await stubLive(live)
      // Streaming tool-delta faults one part: 1 message resident, partial.
      const one = await SessionColdV2.faultInMessagePart(archive, live, "s-w", "m-00", "p-00-a")
      expect(one.faulted).toBe(true)
      expect(await liveCounts(live, "s-w")).toMatchObject({ messages: 1, complete: 0 })
      // The next tail open must serve the requested window — boolean presence
      // used to serve that 1 row as the whole window (silent truncation).
      const tail = await SessionColdV2.faultInSessions(archive, live, ["s-w"], { tailMessages: 5 })
      expect(tail.sessions).toBe(1)
      const after = await liveCounts(live, "s-w")
      // Window served (the stale m-00 from the part fault survives alongside:
      // fault writes never delete, and the superset still serves every bounded
      // read — completion reconciles exactly later).
      expect(after.messages).toBeGreaterThanOrEqual(5 + SessionColdV2.TAIL_OVERLAP)
      expect(after.complete).toBe(0)
      // And the window holds the NEWEST messages (m-00's presence is the
      // harmless stale residue above, not the served window).
      const db = await SessionColdV2.openRawDb(live, "ro")
      try {
        const ids = db.all<{ id: string }>(`SELECT id FROM message WHERE session_id = ? ORDER BY id`, ["s-w"]).map((row) => row.id)
        expect(ids).toContain("m-15")
        expect(ids).toContain("m-11")
      } finally {
        db.close()
      }
      // Full completion afterwards is EXACT.
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-w"])).sessions).toBe(1)
      const origin = join(dir, "origin.db")
      await copyFile(archive, origin)
      await SessionColdV2.restoreFile(origin, false)
      await syncSessionTimes(origin, live)
      const { diffs, firsts } = await SessionColdV2.compareFiles(origin, live, null)
      expect(firsts).toEqual([])
      expect(diffs).toBe(0)
    } finally {
      await cleanup()
    }
  }, 300_000)

  test("tail fault of a small session marks complete (no re-fault loop)", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildLockLive(dir)
      const archive = join(dir, "archive.db")
      await SessionColdV2.packArchiveFlow({ src: live, dst: archive, allow: null, minBytes: 200, verify: false, treatAsLive: false })
      await stubLive(live)
      // s-a holds 4 messages; a window of 5+overlap covers the archive, so the
      // tail IS the whole session: must mark complete, or every open faults.
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-a"], { tailMessages: 5 })).sessions).toBe(1)
      expect(await liveCounts(live, "s-a")).toMatchObject({ messages: 4, complete: 1 })
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-a"], { tailMessages: 5 })).sessions).toBe(0)
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-a"])).sessions).toBe(0)
    } finally {
      await cleanup()
    }
  }, 300_000)

  test("markerless truncation heals on full fault (overlay + mark, never adopt)", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildWideLive(dir)
      const archive = join(dir, "archive.db")
      await SessionColdV2.packArchiveFlow({ src: live, dst: archive, allow: null, minBytes: 200, verify: false, treatAsLive: false })
      const origin = join(dir, "origin.db")
      await copyFile(live, origin)
      await stubLive(live)
      // Tail, then lose the marker (manual DELETE / backup without the table).
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-w"], { tailMessages: 2 })).sessions).toBe(1)
      const mut = await SessionColdV2.openRawDb(live, "rw")
      try {
        mut.exec(`DELETE FROM fault_state WHERE session_id = 's-w'`)
      } finally {
        mut.close()
      }
      // A full open must heal (overlay the missing rows + mark complete),
      // not adopt the truncated tail as whole.
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-w"])).sessions).toBe(1)
      expect(await liveCounts(live, "s-w")).toMatchObject({ messages: 16, complete: 1 })
      await syncSessionTimes(origin, live)
      const { diffs, firsts } = await SessionColdV2.compareFiles(origin, live, null)
      expect(firsts).toEqual([])
      expect(diffs).toBe(0)
    } finally {
      await cleanup()
    }
  }, 300_000)

  test("merge overlays markerless sessions (never replaces the base)", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildWideLive(dir)
      const archive = join(dir, "archive.db")
      await SessionColdV2.packArchiveFlow({ src: live, dst: archive, allow: null, minBytes: 200, verify: false, treatAsLive: false })
      await stubLive(live)
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-w"])).sessions).toBe(1)
      // Simulate a truncated tail that lost its marker: drop the oldest half
      // of messages/parts plus the marker row.
      const mut = await SessionColdV2.openRawDb(live, "rw")
      try {
        mut.exec(`DELETE FROM message WHERE session_id = 's-w' AND id < 'm-08'`)
        mut.exec(`DELETE FROM part WHERE session_id = 's-w' AND message_id < 'm-08'`)
        mut.exec(`DELETE FROM fault_state WHERE session_id = 's-w'`)
      } finally {
        mut.close()
      }
      // Merge into a restored full image: the archived base must survive.
      const full = join(dir, "full.db")
      await copyFile(archive, full)
      await SessionColdV2.restoreFile(full, false)
      const merged = await SessionColdV2.mergeLiveIntoFull(live, full)
      expect(merged.updated).toBe(1)
      const db = await SessionColdV2.openRawDb(full, "ro")
      try {
        const n = db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM message WHERE session_id = 's-w'`)?.n ?? 0
        expect(n).toBe(16)
      } finally {
        db.close()
      }
    } finally {
      await cleanup()
    }
  }, 300_000)

  test("fault and evict on the same session converge EXACT", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildLockLive(dir)
      const archive = join(dir, "archive.db")
      await SessionColdV2.packArchiveFlow({ src: live, dst: archive, allow: null, minBytes: 200, verify: false, treatAsLive: false })
      const origin = join(dir, "origin.db")
      await copyFile(live, origin)
      await stubLive(live)
      // Fault, evict, fault again on one session: every step converges, and
      // the end state equals the origin.
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-a"])).sessions).toBe(1)
      expect((await SessionColdV2.evictSessions(archive, live, ["s-a"], { quiet: true })).sessions).toBe(1)
      expect(await liveCounts(live, "s-a")).toMatchObject({ messages: 0, complete: null })
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-a"])).sessions).toBe(1)
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-b"])).sessions).toBe(1)
      await syncSessionTimes(origin, live)
      const { diffs, firsts } = await SessionColdV2.compareFiles(origin, live, null)
      expect(firsts).toEqual([])
      expect(diffs).toBe(0)
    } finally {
      await cleanup()
    }
  }, 300_000)

  test("stale mutating global op does not block session work", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = join(dir, "live.db")
      await writeFile(live, "x")
      // No lock: immediate return.
      await SessionColdV2.gateLiveMutatingOp(live, 1000)
      // Dead-pid mutating lock: stale, no wait.
      const proc = Bun.spawn(["true"])
      await proc.exited
      await writeFile(`${live}.lock`, `${proc.pid}:nonce:slim`)
      const t0 = Date.now()
      await SessionColdV2.gateLiveMutatingOp(live, 5000)
      expect(Date.now() - t0).toBeLessThan(4000)
      // Read-only global op with a LIVE holder: proceeds (pack snapshots).
      await writeFile(`${live}.lock`, `${process.pid}:nonce:pack`)
      await SessionColdV2.gateLiveMutatingOp(live, 1000)
    } finally {
      await cleanup()
    }
  }, 120_000)
})
