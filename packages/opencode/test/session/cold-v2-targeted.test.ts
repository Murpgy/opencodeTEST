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

describe("targeted part fault-in", () => {
  const scratch = async (): Promise<{ dir: string; cleanup: () => Promise<void> }> => {
    const dir = join(tmpdir(), `opencode-cold-v2-target-${process.pid}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`)
    await mkdir(dir, { recursive: true })
    return { dir, cleanup: () => rm(dir, { recursive: true, force: true }) }
  }

  const withEnv = async <T>(vars: Record<string, string | undefined>, fn: () => Promise<T>): Promise<T> => {
    const saved: Record<string, string | undefined> = {}
    for (const [key, value] of Object.entries(vars)) {
      saved[key] = process.env[key]
      if (value === undefined) delete process.env[key]
      else process.env[key] = value
    }
    try {
      return await fn()
    } finally {
      for (const [key, value] of Object.entries(saved)) {
        if (value === undefined) delete process.env[key]
        else process.env[key] = value
      }
    }
  }

  // One session per resolution branch: s-t bundles (16 messages x 2 big
  // parts, all session-unique), s-sh1/s-sh2 share one identical big part
  // (stays a global pointer), s-small stays classic inline (floor miss).
  const buildLiveTargeted = async (dir: string, name: string): Promise<string> => {
    const file = join(dir, name)
    const now = Date.now()
    const old = now - 2 * 3600 * 1000
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
      const big = (tag: string): string => J({ type: "text", text: `${tag}-${"x".repeat(2500)}`, time: { start: 1, end: 2 } })
      const seed = (id: string): void => {
        db.run(`INSERT INTO session VALUES (?, ?, ?, ?, ?, ?)`, [id, null, `${id} title`, null, old, "proj-a"])
      }
      seed("s-t")
      const pad = (n: number): string => String(n).padStart(2, "0")
      for (let i = 0; i < 16; i += 1) {
        const mid = `m-${pad(i)}`
        db.run(`INSERT INTO message VALUES (?, ?, ?)`, [mid, "s-t", 1000 + i])
        db.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, [`p-${pad(i)}-a`, mid, "s-t", big(`T-${i}-A`)])
        db.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, [`p-${pad(i)}-b`, mid, "s-t", big(`T-${i}-B`)])
      }
      const shared = J({ type: "text", text: `SHARED-${"y".repeat(2500)}` })
      for (const sid of ["s-sh1", "s-sh2"]) {
        seed(sid)
        const mid = `m-${sid}`
        db.run(`INSERT INTO message VALUES (?, ?, ?)`, [mid, sid, old])
        db.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, [`p-${sid}-shared`, mid, sid, shared])
        db.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, [`p-${sid}-u`, mid, sid, J({ type: "text", text: `unique ${sid}` })])
      }
      seed("s-small")
      db.run(`INSERT INTO message VALUES (?, ?, ?)`, ["m-sm", "s-small", old])
      db.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, ["p-sm", "m-sm", "s-small", J({ type: "text", text: "hello" })])
    } finally {
      db.close()
    }
    return file
  }

  const migrateTargeted = async (dir: string): Promise<{ origin: string; live: string; archive: string }> => {
    const origin = await buildLiveTargeted(dir, "opencode.db")
    const live = liveV2PathFor(origin)
    const archive = archivePathFor(origin)
    await withEnv({ OPENCODE_COLD_V2_QUIET: undefined, OPENCODE_COLD_V2_AUTO_MIGRATE: "1", CI: "1" }, async () => {
      expect(await maybeWarnColdV2Migration({ origin, live, archive })).toBe("migrated")
    })
    return { origin, live, archive }
  }

  const liveData = async (live: string, partID: string): Promise<string | null> => {
    const db = await SessionColdV2.openRawDb(live, "ro")
    try {
      return db.get<{ data: string }>(`SELECT data FROM part WHERE id = ?`, [partID])?.data ?? null
    } finally {
      db.close()
    }
  }

  const liveCounts = async (live: string, sid: string): Promise<{ messages: number; parts: number }> => {
    const db = await SessionColdV2.openRawDb(live, "ro")
    try {
      return {
        messages: db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM message WHERE session_id = ?`, [sid])?.n ?? 0,
        parts: db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM part WHERE session_id = ?`, [sid])?.n ?? 0,
      }
    } finally {
      db.close()
    }
  }

  const markerOf = async (live: string, sid: string): Promise<number | null> => {
    const db = await SessionColdV2.openRawDb(live, "ro")
    try {
      return db.get<{ complete: number }>(`SELECT complete FROM fault_state WHERE session_id = ?`, [sid])?.complete ?? null
    } catch {
      return null
    } finally {
      db.close()
    }
  }

  const originData = async (origin: string, partID: string): Promise<string> => {
    const db = await SessionColdV2.openRawDb(origin, "ro")
    try {
      const row = db.get<{ data: string }>(`SELECT data FROM part WHERE id = ?`, [partID])
      expect(row).toBeDefined()
      return row?.data ?? ""
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

  test("single old part faults without faulting the session", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const { origin, live, archive } = await migrateTargeted(dir)
      // Fixture actually exercises the bundle branch: s-t publishes chunks.
      const packed = await SessionColdV2.openRawDb(archive, "ro")
      try {
        expect(packed.get<{ n: number }>(`SELECT COUNT(*) AS n FROM bundle WHERE session_id = 's-t'`)?.n ?? 0).toBeGreaterThan(0)
        expect(packed.get<{ data: string }>(`SELECT data FROM part WHERE id = 'p-s-sh1-shared'`)?.data.startsWith('{"_blob"')).toBe(true)
        expect(packed.get<{ data: string }>(`SELECT data FROM part WHERE id = 'p-sm'`)?.data.startsWith('{"_bd"')).toBe(false)
      } finally {
        packed.close()
      }
      const done = await SessionColdV2.faultInMessagePart(archive, live, "s-t", "m-00", "p-00-a")
      expect(done.faulted).toBe(true)
      expect(done.parts).toBe(1)
      // Exactly one message + one part landed; the other 15 messages did not.
      expect(await liveCounts(live, "s-t")).toEqual({ messages: 1, parts: 1 })
      expect(await liveData(live, "p-00-a")).toBe(await originData(origin, "p-00-a"))
      expect(await markerOf(live, "s-t")).toBe(0)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("repeat is a no-op; full completion is EXACT", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const { origin, live, archive } = await migrateTargeted(dir)
      expect((await SessionColdV2.faultInMessagePart(archive, live, "s-t", "m-00", "p-00-a")).faulted).toBe(true)
      expect((await SessionColdV2.faultInMessagePart(archive, live, "s-t", "m-00", "p-00-a")).faulted).toBe(false)
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-t"])).sessions).toBe(1)
      expect(await markerOf(live, "s-t")).toBe(1)
      // Other sessions are still stubs: fault them fully, then the whole
      // live file must equal the origin byte-for-byte.
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-sh1", "s-sh2", "s-small"])).sessions).toBe(3)
      await syncSessionTimes(origin, live)
      const { diffs, firsts } = await SessionColdV2.compareFiles(origin, live, null)
      expect(firsts).toEqual([])
      expect(diffs).toBe(0)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("message scope faults all its parts", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const { origin, live, archive } = await migrateTargeted(dir)
      const done = await SessionColdV2.faultInMessagePart(archive, live, "s-t", "m-01")
      expect(done.faulted).toBe(true)
      expect(done.parts).toBe(2)
      expect(await liveCounts(live, "s-t")).toEqual({ messages: 1, parts: 2 })
      expect(await liveData(live, "p-01-a")).toBe(await originData(origin, "p-01-a"))
      expect(await liveData(live, "p-01-b")).toBe(await originData(origin, "p-01-b"))
      expect(await markerOf(live, "s-t")).toBe(0)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("shared-pointer and inline branches resolve", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const { origin, live, archive } = await migrateTargeted(dir)
      expect((await SessionColdV2.faultInMessagePart(archive, live, "s-sh1", "m-s-sh1", "p-s-sh1-shared")).faulted).toBe(true)
      expect(await liveData(live, "p-s-sh1-shared")).toBe(await originData(origin, "p-s-sh1-shared"))
      expect(await liveCounts(live, "s-sh1")).toEqual({ messages: 1, parts: 1 })
      expect((await SessionColdV2.faultInMessagePart(archive, live, "s-small", "m-sm", "p-sm")).faulted).toBe(true)
      expect(await liveData(live, "p-sm")).toBe(await originData(origin, "p-sm"))
      expect(await markerOf(live, "s-small")).toBe(0)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("missing part and deleted session return false", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const { live, archive } = await migrateTargeted(dir)
      expect((await SessionColdV2.faultInMessagePart(archive, live, "s-t", "m-00", "p-nope")).faulted).toBe(false)
      expect((await SessionColdV2.faultInMessagePart(archive, live, "s-t", "m-nope")).faulted).toBe(false)
      const del = await SessionColdV2.openRawDb(live, "rw")
      try {
        del.run(`DELETE FROM session WHERE id = ?`, ["s-small"])
      } finally {
        del.close()
      }
      expect((await SessionColdV2.faultInMessagePart(archive, live, "s-small", "m-sm", "p-sm")).faulted).toBe(false)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("tail plus targeted old part composes, completion EXACT", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const { origin, live, archive } = await migrateTargeted(dir)
      // Tail window (2 + overlap) covers the newest 12 of 16; m-00 is outside it.
      await SessionColdV2.faultInSessions(archive, live, ["s-t"], { tailMessages: 2 })
      expect((await liveCounts(live, "s-t")).messages).toBe(2 + SessionColdV2.TAIL_OVERLAP)
      expect(await liveData(live, "p-00-a")).toBeNull()
      expect((await SessionColdV2.faultInMessagePart(archive, live, "s-t", "m-00", "p-00-a")).faulted).toBe(true)
      expect(await liveData(live, "p-00-a")).toBe(await originData(origin, "p-00-a"))
      expect(await markerOf(live, "s-t")).toBe(0)
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-t"])).sessions).toBe(1)
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-sh1", "s-sh2", "s-small"])).sessions).toBe(3)
      await syncSessionTimes(origin, live)
      const { diffs, firsts } = await SessionColdV2.compareFiles(origin, live, null)
      expect(firsts).toEqual([])
      expect(diffs).toBe(0)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("message scope on a partial message fills only the missing parts", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const { origin, live, archive } = await migrateTargeted(dir)
      // Single-part fault leaves m-05 with 1 of its 2 parts (marker stays 0).
      expect((await SessionColdV2.faultInMessagePart(archive, live, "s-t", "m-05", "p-05-a")).faulted).toBe(true)
      expect(await liveCounts(live, "s-t")).toEqual({ messages: 1, parts: 1 })
      // Message scope must NOT treat "message present" as complete: it fills
      // exactly the missing part.
      const done = await SessionColdV2.faultInMessagePart(archive, live, "s-t", "m-05")
      expect(done.faulted).toBe(true)
      expect(done.parts).toBe(1)
      expect(await liveCounts(live, "s-t")).toEqual({ messages: 1, parts: 2 })
      expect(await liveData(live, "p-05-b")).toBe(await originData(origin, "p-05-b"))
      expect(await markerOf(live, "s-t")).toBe(0)
      // Now the message is whole: a repeat is a no-op without archive writes.
      expect((await SessionColdV2.faultInMessagePart(archive, live, "s-t", "m-05")).faulted).toBe(false)
    } finally {
      await cleanup()
    }
  }, 180_000)
})
