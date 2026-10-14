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

describe("file round-trip", () => {
  const buildLive = async (dir: string): Promise<string> => {
    const file = join(dir, "live.db")
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
      db.run(`INSERT INTO session VALUES (?, ?)`, ["s2", "proj-b"])
      db.run(`INSERT INTO message VALUES (?, ?)`, ["m1", "s1"])
      db.run(`INSERT INTO event_sequence VALUES (?, ?)`, ["s1", 3])
      db.run(`INSERT INTO event_sequence VALUES (?, ?)`, ["s2", 1])
      db.run(`INSERT INTO todo VALUES (?, ?)`, ["s1", "todo-1"])
      // Small inline row (stays inline at minBytes 200).
      db.run(`INSERT INTO part VALUES (?, ?, ?, ?, ?, ?)`, ["p-small", "m1", "s1", 1, 1, J({ type: "text", text: "hi" })])
      // Large canonical row.
      db.run(`INSERT INTO part VALUES (?, ?, ?, ?, ?, ?)`, [
        "p-big",
        "m1",
        "s1",
        2,
        2,
        J({ type: "text", text: `LARGE-${"x".repeat(500)}`, time: { start: 1, end: 2 } }),
      ])
      // Minority order -> raw fallback (sibling below defines template order).
      db.run(`INSERT INTO part VALUES (?, ?, ?, ?, ?, ?)`, [
        "p-raw",
        "m1",
        "s1",
        3,
        3,
        J({ type: "weird", b: `Y-${"y".repeat(300)}`, a: `X-${"x".repeat(300)}` }),
      ])
      db.run(`INSERT INTO part VALUES (?, ?, ?, ?, ?, ?)`, [
        "p-raw2",
        "m1",
        "s1",
        4,
        4,
        J({ type: "weird", a: `X-${"x".repeat(300)}`, b: `Y-${"y".repeat(300)}` }),
      ])
      // Unicode + floats.
      db.run(`INSERT INTO part VALUES (?, ?, ?, ?, ?, ?)`, [
        "p-uni",
        "m1",
        "s1",
        5,
        5,
        J({ type: "text", text: `héllo ✓ ${"ü".repeat(300)}`, score: 3.14159 }),
      ])
      const env = (pid: string, part: unknown, time: number): string =>
        J({ sessionID: "s1", part: { id: pid, sessionID: "s1", messageID: "m1", ...(part as Record<string, unknown>) }, time })
      db.run(`INSERT INTO event VALUES (?, ?, ?, ?, ?)`, [
        "e1",
        "s1",
        1,
        "message.part.updated.1",
        env("p-big", { type: "text", text: `EV-${"e".repeat(500)}`, time: { start: 5, end: 6 } }, 11),
      ])
      db.run(`INSERT INTO event VALUES (?, ?, ?, ?, ?)`, [
        "e1b",
        "s1",
        2,
        "message.part.updated.1",
        env("p-big", { type: "text", text: `EV2-${"f".repeat(500)}`, time: { start: 7, end: 8 } }, 12),
      ])
      db.run(`INSERT INTO event VALUES (?, ?, ?, ?, ?)`, ["e2", "s1", 3, "message.updated.1", J({ note: "non-pu1" })])
      db.run(`INSERT INTO event VALUES (?, ?, ?, ?, ?)`, ["e3", "s2", 1, "message.updated.1", J({ note: "other session" })])
    } finally {
      db.close()
    }
    return file
  }

  const scratch = async (): Promise<{ dir: string; cleanup: () => Promise<void> }> => {
    const dir = join(tmpdir(), `opencode-cold-v2-test-${process.pid}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`)
    await mkdir(dir, { recursive: true })
    return { dir, cleanup: () => rm(dir, { recursive: true, force: true }) }
  }

  test("pack -> verify -> unpack round-trips byte-exact", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildLive(dir)
      const packed = join(dir, "cold.db")
      await copyFile(live, packed)
      const stats = await SessionColdV2.packFile(packed, null, 200)
      expect(stats.partPointers).toBeGreaterThan(0)
      expect(stats.partRawFallback).toBeGreaterThan(0)
      await SessionColdV2.markComplete(packed)
      const report = await SessionColdV2.verifyArchive(packed)
      expect(report.blobs).toBeGreaterThan(0)
      const unpacked = join(dir, "restored.db")
      await copyFile(packed, unpacked)
      const restored = await SessionColdV2.restoreFile(unpacked, false)
      expect(restored.parts).toBe(stats.partPointers)
      expect(restored.events).toBe(stats.eventSlims)
      const { total, diffs, firsts } = await SessionColdV2.compareFiles(live, unpacked, null)
      expect(firsts).toEqual([])
      expect(diffs).toBe(0)
      expect(total).toBeGreaterThan(0)
    } finally {
      await cleanup()
    }
  }, 120_000)

  test("incomplete archives are refused without the testing override", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildLive(dir)
      const packed = join(dir, "cold.db")
      await copyFile(live, packed)
      await SessionColdV2.packFile(packed, null, 200)
      await expect(SessionColdV2.restoreFile(packed, false)).rejects.toThrow(/not a completed archive/)
      const copy = join(dir, "copy.db")
      await copyFile(packed, copy)
      await expect(SessionColdV2.restoreFile(copy, true)).resolves.toBeDefined()
    } finally {
      await cleanup()
    }
  }, 120_000)

  test("tampered blobs, flags, lengths and missing rows are loud (F1-F4)", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildLive(dir)
      const packed = join(dir, "cold.db")
      await copyFile(live, packed)
      await SessionColdV2.packFile(packed, null, 200)
      await SessionColdV2.markComplete(packed)
      const mutate = async (name: string, sqlText: string): Promise<string> => {
        const file = join(dir, name)
        await copyFile(packed, file)
        const db = await SessionColdV2.openRawDb(file, "rw")
        try {
          db.exec(sqlText)
        } finally {
          db.close()
        }
        return file
      }
      await expect(SessionColdV2.restoreFile(await mutate("f2.db", `UPDATE blob SET raw = 1 - raw WHERE raw = 0`), true)).rejects.toThrow(
        /sha mismatch/,
      )
      await expect(SessionColdV2.restoreFile(await mutate("f3.db", `UPDATE blob SET len = -1`), true)).rejects.toThrow(/len mismatch/)
      await expect(
        SessionColdV2.restoreFile(await mutate("f4.db", `DELETE FROM blob WHERE sha256 = (SELECT sha FROM ptr LIMIT 1)`), true),
      ).rejects.toThrow(/no blob row/)
      await expect(SessionColdV2.verifyArchive(await mutate("f5.db", `UPDATE meta SET v = '0' WHERE k = 'count_part'`))).rejects.toThrow(
        /manifest/,
      )
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("swapped row pointer (valid sha, wrong row) is refused at restore and verify", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildLive(dir)
      const packed = join(dir, "cold.db")
      await copyFile(live, packed)
      await SessionColdV2.packFile(packed, null, 200)
      await SessionColdV2.markComplete(packed)
      // Swap one row's pointer to another valid sha; leave ptr untouched so
      // the ptr_hash still verifies — the row↔registry check must refuse.
      const swapped = join(dir, "swap.db")
      await copyFile(packed, swapped)
      const db = await SessionColdV2.openRawDb(swapped, "rw")
      try {
        const rows = db.all<{ id: string; sha: string }>(`SELECT id, sha FROM ptr WHERE t = 'part' ORDER BY id LIMIT 2`)
        expect(rows.length).toBeGreaterThanOrEqual(2)
        const [a, b] = rows as [{ id: string; sha: string }, { id: string; sha: string }]
        db.run(`UPDATE part SET data = ? WHERE id = ?`, [JSON.stringify({ _blob: b.sha }), a.id])
      } finally {
        db.close()
      }
      await expect(SessionColdV2.restoreFile(swapped, true)).rejects.toThrow(/registry/)
      await expect(SessionColdV2.verifyArchive(swapped)).rejects.toThrow(/registry/)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("swapped event slim blob is refused at restore and verify", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildLive(dir)
      const packed = join(dir, "cold.db")
      await copyFile(live, packed)
      await SessionColdV2.packFile(packed, null, 200)
      await SessionColdV2.markComplete(packed)
      const slimRows = async (file: string): Promise<{ id: string; sha: string }[]> => {
        const db = await SessionColdV2.openRawDb(file, "ro")
        try {
          return db.all<{ id: string; sha: string }>(`SELECT id, sha FROM ptr WHERE t = 'event' ORDER BY id LIMIT 2`)
        } finally {
          db.close()
        }
      }
      const rows = await slimRows(packed)
      expect(rows.length).toBeGreaterThanOrEqual(2)
      const swapped = join(dir, "swap-ev.db")
      await copyFile(packed, swapped)
      const db = await SessionColdV2.openRawDb(swapped, "rw")
      try {
        const [a, b] = rows as [{ id: string; sha: string }, { id: string; sha: string }]
        const slim = JSON.parse(db.get<{ data: string }>(`SELECT data AS data FROM event WHERE id = ?`, [a.id])?.data ?? "{}") as Record<
          string,
          unknown
        >
        slim["blob"] = b.sha
        db.run(`UPDATE event SET data = ? WHERE id = ?`, [JSON.stringify(slim), a.id])
      } finally {
        db.close()
      }
      await expect(SessionColdV2.restoreFile(swapped, true)).rejects.toThrow(/registry/)
      await expect(SessionColdV2.verifyArchive(swapped)).rejects.toThrow(/registry/)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("pointer-shaped row without registry entry is refused", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildLive(dir)
      const packed = join(dir, "cold.db")
      await copyFile(live, packed)
      await SessionColdV2.packFile(packed, null, 200)
      await SessionColdV2.markComplete(packed)
      const stray = join(dir, "stray.db")
      await copyFile(packed, stray)
      const db = await SessionColdV2.openRawDb(stray, "rw")
      try {
        const sha = db.get<{ sha256: string }>(`SELECT sha256 FROM blob LIMIT 1`)?.sha256
        expect(sha).toBeDefined()
        const inline = db.get<{ id: string }>(
          `SELECT p.id AS id FROM part p LEFT JOIN ptr r ON r.t = 'part' AND r.id = p.id WHERE r.id IS NULL LIMIT 1`,
        )
        expect(inline?.id).toBeDefined()
        db.run(`UPDATE part SET data = ? WHERE id = ?`, [JSON.stringify({ _blob: sha }), inline?.id ?? ""])
      } finally {
        db.close()
      }
      await expect(SessionColdV2.restoreFile(stray, true)).rejects.toThrow(/registry/)
      await expect(SessionColdV2.verifyArchive(stray)).rejects.toThrow(/registry/)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("filterSessions stages allow-lists past the SQLite variable limit", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const file = join(dir, "many.db")
      const db = await SessionColdV2.openRawDb(file, "rw")
      const ids: string[] = []
      try {
        db.exec(`CREATE TABLE session (id TEXT PRIMARY KEY, project_id TEXT)`)
        db.exec(`CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT)`)
        db.exec(`CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, data TEXT)`)
        db.exec(`CREATE TABLE event (id TEXT PRIMARY KEY, aggregate_id TEXT, type TEXT, data TEXT)`)
        db.exec(`CREATE TABLE event_sequence (aggregate_id TEXT PRIMARY KEY, seq INTEGER)`)
        db.exec(`CREATE TABLE todo (session_id TEXT, content TEXT)`)
        db.exec(`CREATE TABLE session_message (id TEXT PRIMARY KEY, session_id TEXT)`)
        db.exec(`CREATE TABLE session_input (session_id TEXT)`)
        db.exec(`CREATE TABLE session_context_epoch (session_id TEXT)`)
        db.exec("BEGIN IMMEDIATE")
        for (let i = 0; i < 1200; i += 1) {
          const id = `ses_keep_${String(i).padStart(5, "0")}`
          ids.push(id)
          db.run(`INSERT INTO session VALUES (?, ?)`, [id, "proj"])
        }
        db.exec("COMMIT")
      } finally {
        db.close()
      }
      // Keep-all with 1200 ids: a single NOT IN (?,?...) would need 1200
      // placeholders (past the 999 limit on old SQLite builds).
      const db2 = await SessionColdV2.openRawDb(file, "rw")
      try {
        const keep = SessionColdV2.filterSessions(db2, ids)
        expect(keep?.size).toBe(1200)
        expect(db2.get<{ n: number }>(`SELECT COUNT(*) AS n FROM session`)?.n).toBe(1200)
        const half = SessionColdV2.filterSessions(db2, ids.slice(0, 600))
        expect(half?.size).toBe(600)
        expect(db2.get<{ n: number }>(`SELECT COUNT(*) AS n FROM session`)?.n).toBe(600)
      } finally {
        db2.close()
      }
    } finally {
      await cleanup()
    }
  }, 120_000)

  test("compare attributes missing todo rows correctly past digit boundaries", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const base = join(dir, "base.db")
      const db = await SessionColdV2.openRawDb(base, "rw")
      try {
        db.exec(`CREATE TABLE session (id TEXT PRIMARY KEY, project_id TEXT)`)
        db.exec(`CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT)`)
        db.exec(`CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, data TEXT)`)
        db.exec(`CREATE TABLE event (id TEXT PRIMARY KEY, aggregate_id TEXT, type TEXT, data TEXT)`)
        db.exec(`CREATE TABLE event_sequence (aggregate_id TEXT PRIMARY KEY, seq INTEGER)`)
        db.exec(`CREATE TABLE todo (session_id TEXT, content TEXT, position INTEGER)`)
        db.exec(`CREATE TABLE session_message (id TEXT PRIMARY KEY, session_id TEXT)`)
        db.exec(`CREATE TABLE session_input (session_id TEXT)`)
        db.exec(`CREATE TABLE session_context_epoch (session_id TEXT)`)
        db.run(`INSERT INTO session VALUES (?, ?)`, ["s1", "proj"])
        for (let i = 0; i < 15; i += 1) db.run(`INSERT INTO todo VALUES (?, ?, ?)`, ["s1", `todo-${i}`, i])
      } finally {
        db.close()
      }
      const modified = join(dir, "modified.db")
      await copyFile(base, modified)
      const mdb = await SessionColdV2.openRawDb(modified, "rw")
      try {
        mdb.exec(`DELETE FROM todo WHERE rowid = 9`)
      } finally {
        mdb.close()
      }
      const { diffs, firsts } = await SessionColdV2.compareFiles(base, modified, null)
      // String ordering ("10" < "9") used to cascade into many misattributed
      // diffs here; native ordering reports exactly the one dropped row.
      expect(diffs).toBe(1)
      expect(firsts.length).toBe(1)
      expect(firsts[0]).toMatch(/dropped/)
      const { diffs: same } = await SessionColdV2.compareFiles(base, base, null)
      expect(same).toBe(0)
    } finally {
      await cleanup()
    }
  }, 120_000)

  test("float torture payloads round-trip byte-exact", async () => {
    // The canonical form is JSON.stringify output, full stop. These values
    // pin that discipline: a normalizing serializer (orjson-style 1e-07→1e-7)
    // would break them, and the packed file round-trip proves end to end that
    // dumps preserve them bit for bit.
    const values = [1e-7, 1e21, -0.0, 0.30000000000000004, 1.5e-5, 123456789.123456789, 3.14159]
    for (const value of values) {
      const once = JSON.stringify({ f: value })
      expect(JSON.stringify(JSON.parse(once))).toBe(once)
    }
    const { dir, cleanup } = await scratch()
    try {
      const live = join(dir, "floats.db")
      const db = await SessionColdV2.openRawDb(live, "rw")
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
        db.run(`INSERT INTO session VALUES (?, ?)`, ["s1", "proj"])
        db.run(`INSERT INTO message VALUES (?, ?)`, ["m1", "s1"])
        db.run(`INSERT INTO part VALUES (?, ?, ?, ?, ?, ?)`, [
          "p-float",
          "m1",
          "s1",
          1,
          1,
          JSON.stringify({ type: "text", text: "pad-to-pack-" + "x".repeat(600), scores: values, tiny: 1e-7 }),
        ])
      } finally {
        db.close()
      }
      const packed = join(dir, "floats-cold.db")
      await copyFile(live, packed)
      await SessionColdV2.packFile(packed, null, 200)
      await SessionColdV2.markComplete(packed)
      const unpacked = join(dir, "floats-rest.db")
      await copyFile(packed, unpacked)
      await SessionColdV2.restoreFile(unpacked, false)
      const { diffs, firsts } = await SessionColdV2.compareFiles(live, unpacked, null)
      expect(firsts).toEqual([])
      expect(diffs).toBe(0)
    } finally {
      await cleanup()
    }
  }, 120_000)

  test("sidecar detects bit-rot without the sqlite toolchain", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildLive(dir)
      const packed = join(dir, "cold.db")
      await copyFile(live, packed)
      await SessionColdV2.packFile(packed, null, 200)
      await SessionColdV2.markComplete(packed)
      const digest = await SessionColdV2.writeSidecar(packed)
      expect(digest).toMatch(/^[0-9a-f]{64}$/)
      await expect(SessionColdV2.verifySidecar(packed)).resolves.toBe(digest)
      // Flip one byte in the middle of the file (bit-rot simulation).
      const db = await SessionColdV2.openRawDb(packed, "rw")
      try {
        const row = db.get<{ id: string }>(`SELECT id FROM part LIMIT 1`)
        if (row) db.run(`UPDATE part SET data = data || ' ' WHERE id = ?`, [row.id])
      } finally {
        db.close()
      }
      await expect(SessionColdV2.verifySidecar(packed)).rejects.toThrow(/sidecar mismatch/)
    } finally {
      await cleanup()
    }
  }, 120_000)

  test("kill mid-pack never publishes a partial archive", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = join(dir, "big-live.db")
      const db = await SessionColdV2.openRawDb(live, "rw")
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
        db.run(`INSERT INTO session VALUES (?, ?)`, ["sbig", "proj"])
        db.run(`INSERT INTO message VALUES (?, ?)`, ["m", "sbig"])
        db.exec("BEGIN IMMEDIATE")
        try {
          for (let i = 0; i < 8000; i += 1) {
            db.run(`INSERT INTO part VALUES (?, ?, ?, ?, ?, ?)`, [
              `p${String(i).padStart(5, "0")}`,
              "m",
              "sbig",
              i,
              i,
              JSON.stringify({ type: "text", text: `row-${i}-` + "v".repeat(900), time: { start: i, end: i } }),
            ])
          }
          db.exec("COMMIT")
        } catch (error) {
          try {
            db.exec("ROLLBACK")
          } catch {
            // Best-effort test setup.
          }
          throw error
        }
      } finally {
        db.close()
      }
      // Absolute import: the child runs from a tmp dir with no tsconfig paths.
      const engine = new URL("../../src/session/cold-v2.ts", import.meta.url).pathname
      const child = join(dir, "pack-child.ts")
      await writeFile(
        child,
        `import { SessionColdV2 } from ${JSON.stringify(engine)}\n` +
          `const [live, tmp, dst] = Bun.argv.slice(2)\n` +
          `await SessionColdV2.copyBytes(live, tmp)\n` +
          `await SessionColdV2.packFile(tmp, null, 500)\n` +
          `await SessionColdV2.markComplete(tmp)\n` +
          `await SessionColdV2.atomicPublish(tmp, dst)\n` +
          `console.log("PACK-DONE")\n`,
      )
      const dst = join(dir, "kill-dst.db")
      const tmp = `${dst}.tmp.child`
      const proc = Bun.spawn([process.execPath, child, live, tmp, dst], { stdout: "ignore", stderr: "ignore" })
      await new Promise((resolve) => setTimeout(resolve, 400))
      try {
        proc.kill("SIGKILL")
      } catch {
        // Already exited (fast host): the branch below still verifies EXACT.
      }
      await proc.exited
      const gone = await Bun.file(dst)
        .exists()
        .catch(() => false)
      if (gone) {
        // Kill landed after publish: the output must already verify EXACT.
        const rest = join(dir, "kill-rest.db")
        await copyFile(dst, rest)
        await SessionColdV2.restoreFile(rest, false)
        const { diffs, firsts } = await SessionColdV2.compareFiles(live, rest, null)
        expect(firsts).toEqual([])
        expect(diffs).toBe(0)
      } else {
        // Kill landed mid-build: nothing published, and a rerun heals EXACT.
        const heal = join(dir, "heal.db")
        await copyFile(live, heal)
        await SessionColdV2.packFile(heal, null, 500)
        await SessionColdV2.markComplete(heal)
        const rest = join(dir, "heal-rest.db")
        await copyFile(heal, rest)
        await SessionColdV2.restoreFile(rest, false)
        const { total, diffs, firsts } = await SessionColdV2.compareFiles(live, rest, null)
        expect(firsts).toEqual([])
        expect(diffs).toBe(0)
        expect(total).toBeGreaterThan(8000)
      }
    } finally {
      await cleanup()
    }
  }, 180_000)
})
