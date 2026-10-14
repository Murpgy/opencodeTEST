import { describe, expect, test } from "bun:test"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { rm, mkdir } from "node:fs/promises"
import { SessionColdV2 } from "@/session/cold-v2"

describe("live health scan (db check / db movelive)", () => {
  const scratch = async (): Promise<{ dir: string; cleanup: () => Promise<void> }> => {
    const dir = join(tmpdir(), `opencode-cold-v2-doctor-${process.pid}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`)
    await mkdir(dir, { recursive: true })
    return { dir, cleanup: () => rm(dir, { recursive: true, force: true }) }
  }

  // Three archived sessions: s-blank (stubbed live, payloads in cold),
  // s-full (resident live), s-drift (live gains a row post-pack).
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
      for (const sid of ["s-blank", "s-full", "s-drift"]) {
        db.run(`INSERT INTO session VALUES (?, ?, ?, ?, ?, ?)`, [sid, null, `${sid} title`, null, Date.now(), "proj-a"])
        db.run(`INSERT INTO message VALUES (?, ?, ?)`, [`m-${sid}`, sid, 1000])
        db.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, [`p-${sid}`, `m-${sid}`, sid, J({ type: "text", text: `hello ${sid}` })])
      }
    } finally {
      db.close()
    }
    return file
  }

  const arrange = async (dir: string): Promise<{ live: string; archive: string }> => {
    const live = await buildLive(dir)
    // s-gone rows land BEFORE the pack so the archive holds them; the live
    // header is deleted after, simulating a deletion pending merge.
    const pre = await SessionColdV2.openRawDb(live, "rw")
    try {
      pre.run(`INSERT INTO session VALUES (?, ?, ?, ?, ?, ?)`, ["s-gone", null, "s-gone title", null, Date.now(), "proj-a"])
      pre.run(`INSERT INTO message VALUES (?, ?, ?)`, ["m-s-gone", "s-gone", 1000])
    } finally {
      pre.close()
    }
    const archive = join(dir, "archive.db")
    await SessionColdV2.packArchiveFlow({ src: live, dst: archive, allow: null, minBytes: 200, verify: false, treatAsLive: false })
    const db = await SessionColdV2.openRawDb(live, "rw")
    try {
      // s-blank: stub (header kept, payloads dropped) — the blank to find.
      db.exec(`DELETE FROM message WHERE session_id = 's-blank'`)
      db.exec(`DELETE FROM part WHERE session_id = 's-blank'`)
      // s-drift: live gains a row the archive lacks.
      db.run(`INSERT INTO message VALUES (?, ?, ?)`, ["m-s-drift-2", "s-drift", 2000])
      // s-gone: header deleted live, still archived — merge-pack would read
      // this as "deleted" and drop it from the next archive.
      db.exec(`DELETE FROM message WHERE session_id = 's-gone'`)
      db.exec(`DELETE FROM session WHERE id = 's-gone'`)
      // s-new: live-only, never packed — no durable copy.
      db.run(`INSERT INTO session VALUES (?, ?, ?, ?, ?, ?)`, ["s-new", null, "s-new title", null, Date.now(), "proj-a"])
      db.run(`INSERT INTO message VALUES (?, ?, ?)`, ["m-s-new", "s-new", 1000])
    } finally {
      db.close()
    }
    return { live, archive }
  }

  test("scan finds blanks, live-only, drifted and index gaps", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const { live, archive } = await arrange(dir)
      const report = await SessionColdV2.scanLiveHealth(live, archive)
      expect(report.liveSessions).toBe(4) // s-blank, s-full, s-drift, s-new
      expect(report.archiveSessions).toBe(4) // s-blank, s-full, s-drift, s-gone
      expect(report.blanks.map((row) => row.id)).toEqual(["s-blank"])
      expect(report.blanks[0]).toMatchObject({ archivedMessages: 1 })
      expect(report.liveOnly.map((row) => row.id)).toEqual(["s-new"])
      expect(report.indexGaps.map((row) => row.id)).toEqual(["s-gone"])
      expect(report.drifted.map((row) => row.id)).toEqual(["s-drift"])
      expect(report.drifted[0]).toMatchObject({ liveMessages: 2, archivedMessages: 1 })
      // s-full: resident and converged — reported nowhere.
      expect([...report.blanks, ...report.liveOnly, ...report.drifted].some((row) => row.id === "s-full")).toBe(false)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("repair faults blanks back; rescan is clean", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const { live, archive } = await arrange(dir)
      const before = await SessionColdV2.scanLiveHealth(live, archive)
      expect(before.blanks).toHaveLength(1)
      const done = await SessionColdV2.faultInSessions(archive, live, before.blanks.map((row) => row.id))
      expect(done.sessions).toBe(1)
      const after = await SessionColdV2.scanLiveHealth(live, archive)
      expect(after.blanks).toHaveLength(0)
      const db = await SessionColdV2.openRawDb(live, "ro")
      try {
        expect(db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM message WHERE session_id = 's-blank'`)?.n).toBe(1)
      } finally {
        db.close()
      }
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("movelive apply converges live-only and drifted into the archive", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const { live, archive } = await arrange(dir)
      const done = await SessionColdV2.packLiveToArchive({ live, archive, minBytes: 200, verify: true })
      expect(done.mergedUpdated).toBeGreaterThanOrEqual(2) // s-new + s-drift (+ s-full/s-blank stubs)
      const after = await SessionColdV2.scanLiveHealth(live, archive)
      expect(after.liveOnly).toHaveLength(0)
      expect(after.drifted).toHaveLength(0)
      // s-blank is a live stub by construction — merge-pack keeps the stub
      // (it merges headers, not payloads into live). Fault it, then clean.
      expect(after.blanks.map((row) => row.id)).toEqual(["s-blank"])
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-blank"])).sessions).toBe(1)
      expect((await SessionColdV2.scanLiveHealth(live, archive)).blanks).toHaveLength(0)
      // The archive now durably holds the new session's payload.
      const db = await SessionColdV2.openRawDb(archive, "ro")
      try {
        expect(db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM message WHERE session_id = 's-new'`)?.n).toBe(1)
      } finally {
        db.close()
      }
    } finally {
      await cleanup()
    }
  }, 300_000)
})
