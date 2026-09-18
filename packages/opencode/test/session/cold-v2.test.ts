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

const obj = (value: SessionColdV2.Json): { [key: string]: SessionColdV2.Json } => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error("not an object")
  return value as { [key: string]: SessionColdV2.Json }
}

describe("canonValue", () => {
  test("strips wrapper ids only at the top level and sorts keys", () => {
    const out = obj(
      SessionColdV2.canonValue({ type: "tool", id: "prt_1", sessionID: "ses_1", messageID: "msg_1", tool: "read" }),
    )
    expect(Object.keys(out)).toEqual(["tool", "type"])
    expect(out["id"]).toBeUndefined()
  })

  test("keeps nested ids (attachments carry them as data)", () => {
    const out = obj(
      SessionColdV2.canonValue({ state: { attachments: [{ id: "att_1", sessionID: "ses_1", url: "x" }] }, type: "t" }),
    )
    const attachments = ((out["state"] as { [key: string]: SessionColdV2.Json })["attachments"] as SessionColdV2.Json[]) as {
      [key: string]: SessionColdV2.Json
    }[]
    expect(Object.keys(attachments[0] ?? {}).sort()).toEqual(["id", "sessionID", "url"])
  })

  test("leaves arrays and scalars alone", () => {
    expect(SessionColdV2.canonValue([3, 1, 2])).toEqual([3, 1, 2])
    expect(SessionColdV2.canonValue("s")).toBe("s")
  })
})

describe("templates", () => {
  const learn = (rows: { [key: string]: SessionColdV2.Json }[]): SessionColdV2.TemplateStore => {
    const store: SessionColdV2.TemplateStore = new Map()
    for (const row of rows) {
      SessionColdV2.walkTemplates(store, "P", String(row["type"] ?? "?"), String(row["tool"] ?? ""), Object.keys(row), [], row)
    }
    return store
  }

  test("single order resolves, contradictory orders fall back to raw", () => {
    const a = { b: 1, type: "t", tool: "" }
    const b = { type: "t", tool: "", b: 2 }
    const store = learn([a, b])
    expect(SessionColdV2.templateOrder(store, "P", "t", "", ["b", "tool", "type"], [], ["b", "type", "tool"])).toBeUndefined()
    const single = learn([a])
    expect(SessionColdV2.templateOrder(single, "P", "t", "", ["b", "tool", "type"], [], ["b", "type", "tool"])).toEqual([
      "b",
      "type",
      "tool",
    ])
  })

  test("reorderable requires the row to already be in template order", () => {
    const store = learn([{ type: "t", tool: "read", callID: "c" }])
    expect(SessionColdV2.isReorderable(store, "P", { type: "t", tool: "read", callID: "c" }, "t", "read", ["type", "tool", "callID"])).toBe(
      true,
    )
    expect(SessionColdV2.isReorderable(store, "P", { callID: "c", type: "t", tool: "read" }, "t", "read", ["type", "tool", "callID"])).toBe(
      false,
    )
  })

  test("reorder restores template order and throws loud on unknown shapes", () => {
    const store = learn([{ type: "t", state: { input: { filePath: "/a" } } }])
    const shuffled = { state: { input: { filePath: "/a" } }, type: "t" }
    const out = SessionColdV2.reorderValue(store, "P", "t", "", ["type", "state"], shuffled, "row_1")
    expect(Object.keys(out)).toEqual(["type", "state"])
    expect(() => SessionColdV2.reorderValue(store, "P", "nope", "", ["zzz"], { zzz: 1 }, "row_2")).toThrow(/no top template for row row_2/)
  })

  test("template rows round-trip with a stable hash that detects tampering", () => {
    const store = learn([
      { type: "t", tool: "read", callID: "c" },
      { type: "t", tool: "read", callID: "d" },
    ])
    const rows = SessionColdV2.storeTemplateRows(store)
    const hash = SessionColdV2.hashTemplateRows(rows)
    const loaded = SessionColdV2.loadTemplateStore(rows)
    expect(SessionColdV2.hashTemplateRows(SessionColdV2.storeTemplateRows(loaded))).toBe(hash)
    const tampered = rows.map((row, i) => (i === 0 ? { ...row, count: row.count + 1 } : row))
    expect(SessionColdV2.hashTemplateRows(tampered)).not.toBe(hash)
  })
})

describe("digests", () => {
  test("raw flag is bound into the blob digest (flag flips trip verification)", () => {
    const plain = Buffer.from('{"type":"t"}', "utf8")
    expect(SessionColdV2.blobDigest(false, plain)).not.toBe(SessionColdV2.blobDigest(true, plain))
    expect(SessionColdV2.blobDigest(false, plain)).toBe(SessionColdV2.blobDigest(false, plain))
  })

  test("manifest hash covers build rows but not publish marks", () => {
    const base = { version: "4", count_part: "10", template_hash: "abc" }
    const h1 = SessionColdV2.manifestHashOf(base)
    expect(SessionColdV2.manifestHashOf({ ...base, complete: "1", published_utc: "x", verify_ok: "1", verify_rows: "5" })).toBe(h1)
    expect(SessionColdV2.manifestHashOf({ ...base, count_part: "11" })).not.toBe(h1)
  })

  test("pointer hash is order-independent and detects swaps", () => {
    const a: (readonly [string, string, string])[] = [
      ["part", "p2", "s2"],
      ["part", "p1", "s1"],
    ]
    const b: (readonly [string, string, string])[] = [
      ["part", "p1", "s1"],
      ["part", "p2", "s2"],
    ]
    expect(SessionColdV2.ptrHashOf(a)).toBe(SessionColdV2.ptrHashOf(b))
    expect(SessionColdV2.ptrHashOf([["part", "p1", "s2"], ["part", "p2", "s1"]])).not.toBe(SessionColdV2.ptrHashOf(a))
  })
})

describe("pointer and slim validation", () => {
  test("valid pointers parse; garbage is loud", () => {
    const sha = "a".repeat(64)
    expect(SessionColdV2.parsePointer(JSON.stringify({ _blob: sha }), "part", "p1")).toBe(sha)
    expect(() => SessionColdV2.parsePointer('{"_blob": "zz', "part", "p1")).toThrow(/p1/)
    expect(() => SessionColdV2.parsePointer(JSON.stringify({ _blob: sha, extra: 1 }), "part", "p1")).toThrow(/wrong shape/)
    expect(() => SessionColdV2.parsePointer(JSON.stringify({ _blob: "zz" }), "part", "p1")).toThrow(/malformed/)
  })

  test("slims require the pu1 shape with all id fields", () => {
    const slim = { _ev: "pu1", sid: "s", time: 1, pid: "p", mid: "m", blob: "b" }
    expect(SessionColdV2.parseSlim(JSON.stringify(slim), "e1").pid).toBe("p")
    expect(() => SessionColdV2.parseSlim(JSON.stringify({ ...slim, _ev: "other" }), "e1")).toThrow(/wrong shape/)
    expect(() => SessionColdV2.parseSlim(JSON.stringify({ ...slim, pid: undefined }), "e1")).toThrow(/missing field pid/)
  })
})

describe("canonical JSON", () => {
  test("stringify preserves insertion order (the byte-exactness mechanism)", () => {
    expect(JSON.stringify({ b: 1, a: 2 })).toBe('{"b":1,"a":2}')
  })

  test("documents the JS float format the format relies on", () => {
    expect(JSON.stringify({ f: 1e-7 })).toBe('{"f":1e-7}')
  })
})

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

describe("startup migration nudge", () => {
  const scratch = async (): Promise<{ dir: string; cleanup: () => Promise<void> }> => {
    const dir = join(tmpdir(), `opencode-cold-v2-startup-${process.pid}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`)
    await mkdir(dir, { recursive: true })
    return { dir, cleanup: () => rm(dir, { recursive: true, force: true }) }
  }

  const buildLive = async (dir: string, name: string): Promise<string> => {
    const file = join(dir, name)
    const db = await SessionColdV2.openRawDb(file, "rw")
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
      db.run(`INSERT INTO session VALUES (?, ?)`, ["s1", "proj-a"])
      db.run(`INSERT INTO message VALUES (?, ?)`, ["m1", "s1"])
      db.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, ["p1", "m1", "s1", JSON.stringify({ type: "text", text: "hi" })])
      // Large row so the flow actually packs a blob (minBytes 200 in tests).
      db.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, [
        "p-big",
        "m1",
        "s1",
        JSON.stringify({ type: "text", text: `LARGE-${"x".repeat(600)}`, time: { start: 1, end: 2 } }),
      ])
    } finally {
      db.close()
    }
    return file
  }

  const fingerprint = async (file: string): Promise<string> => createHash("sha256").update(await readFile(file)).digest("hex")

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

  test("archive and live-v2 paths sit next to the origin file", () => {
    expect(archivePathFor("/data/x/opencode.db")).toBe("/data/x/opencode-cold-v2.db")
    expect(liveV2PathFor("/data/x/opencode.db")).toBe("/data/x/opencode-live-v2.db")
  })

  test("fresh install with no files needs nothing", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const status = await migrationStatus(join(dir, "opencode.db"), join(dir, "opencode-live-v2.db"), join(dir, "opencode-cold-v2.db"))
      expect(status.originExists).toBe(false)
      expect(status.liveExists).toBe(false)
      expect(status.needsMigration).toBe(false)
      expect(status.needsRestore).toBe(false)
      expect(status.migrated).toBe(false)
      expect(status.archiveState).toBe("missing")
    } finally {
      await cleanup()
    }
  })

  test("origin without live or archive needs migration from the origin", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const origin = await buildLive(dir, "opencode.db")
      const live = liveV2PathFor(origin)
      const archive = archivePathFor(origin)
      const status = await migrationStatus(origin, live, archive)
      expect(status.originExists).toBe(true)
      expect(status.originSessions).toBe(1)
      expect(status.liveExists).toBe(false)
      expect(status.migrated).toBe(false)
      expect(status.source).toBe(origin)
      expect(status.archiveState).toBe("missing")
      expect(status.needsMigration).toBe(true)
      expect(status.needsRestore).toBe(false)
      expect(formatMigrationWarning(status)).toMatch(/opencode db pack --all/)
      expect(formatMigrationWarning(status)).toMatch(/opencode-live-v2\.db/)
    } finally {
      await cleanup()
    }
  })

  test("auto-migrate packs the origin, materializes slim live and freezes the origin", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const origin = await buildLive(dir, "opencode.db")
      const live = liveV2PathFor(origin)
      const archive = archivePathFor(origin)
      const before = await fingerprint(origin)
      const beforeStat = await stat(origin)
      await withEnv({ OPENCODE_COLD_V2_QUIET: undefined, OPENCODE_COLD_V2_AUTO_MIGRATE: "1", CI: "1" }, async () => {
        expect(await maybeWarnColdV2Migration({ origin, live, archive })).toBe("migrated")
      })
      // Origin byte-identical (content and mtime): frozen from here on.
      expect(await fingerprint(origin)).toBe(before)
      expect((await stat(origin)).mtimeMs).toBe(beforeStat.mtimeMs)
      const status = await migrationStatus(origin, live, archive)
      expect(status.archiveState).toBe("complete")
      expect(status.archiveSessions).toBe(1)
      expect(status.migrated).toBe(true)
      expect(status.needsMigration).toBe(false)
      expect(status.needsRestore).toBe(false)
      expect(status.liveSessions).toBe(1)
      // On-demand: live holds the session index for browsing, but no payloads.
      const slim = await SessionColdV2.openRawDb(live, "ro")
      try {
        expect(slim.get<{ n: number }>(`SELECT COUNT(*) AS n FROM session`)?.n).toBe(1)
        expect(slim.get<{ n: number }>(`SELECT COUNT(*) AS n FROM message`)?.n).toBe(0)
        expect(slim.get<{ n: number }>(`SELECT COUNT(*) AS n FROM part`)?.n).toBe(0)
      } finally {
        slim.close()
      }
      // Second startup serves from live: done, no warning, no work.
      await withEnv({ OPENCODE_COLD_V2_QUIET: undefined, OPENCODE_COLD_V2_AUTO_MIGRATE: "1", CI: "1" }, async () => {
        expect(await maybeWarnColdV2Migration({ origin, live, archive })).toBe("done")
      })
      expect(await fingerprint(origin)).toBe(before)
      // The reported requirement: deleting the original changes nothing.
      await rm(origin, { force: true })
      await withEnv({ OPENCODE_COLD_V2_QUIET: undefined, OPENCODE_COLD_V2_AUTO_MIGRATE: "1", CI: "1" }, async () => {
        expect(await maybeWarnColdV2Migration({ origin, live, archive })).toBe("done")
      })
      expect((await migrationStatus(origin, live, archive)).liveSessions).toBe(1)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("deleted live file restores slim, fault-in is EXACT, origin stays out of it", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const origin = await buildLive(dir, "opencode.db")
      const live = liveV2PathFor(origin)
      const archive = archivePathFor(origin)
      await withEnv({ OPENCODE_COLD_V2_QUIET: undefined, OPENCODE_COLD_V2_AUTO_MIGRATE: "1", CI: "1" }, async () => {
        expect(await maybeWarnColdV2Migration({ origin, live, archive })).toBe("migrated")
      })
      const originBefore = await fingerprint(origin)
      await rm(live, { force: true })
      const status = await migrationStatus(origin, live, archive)
      expect(status.archiveState).toBe("complete")
      expect(status.liveExists).toBe(false)
      expect(status.needsRestore).toBe(true)
      expect(status.needsMigration).toBe(false)
      await withEnv({ OPENCODE_COLD_V2_QUIET: "1", CI: "1" }, async () => {
        expect(await maybeWarnColdV2Migration({ origin, live, archive })).toBe("restored")
      })
      const after = await migrationStatus(origin, live, archive)
      expect(after.needsRestore).toBe(false)
      expect(after.liveSessions).toBe(1)
      // Slim: headers for browsing, no payloads until opened.
      const slim = await SessionColdV2.openRawDb(live, "ro")
      try {
        expect(slim.get<{ n: number }>(`SELECT COUNT(*) AS n FROM session`)?.n).toBe(1)
        expect(slim.get<{ n: number }>(`SELECT COUNT(*) AS n FROM message`)?.n).toBe(0)
      } finally {
        slim.close()
      }
      // Origin untouched by the restore; fault-in equals a fresh full unpack.
      expect(await fingerprint(origin)).toBe(originBefore)
      const faulted = await SessionColdV2.faultInSessions(archive, live, ["s1"])
      expect(faulted.sessions).toBe(1)
      const fresh = join(dir, "fresh.db")
      await copyFile(archive, fresh)
      await SessionColdV2.restoreFile(fresh, false)
      const { diffs, firsts } = await SessionColdV2.compareFiles(fresh, live, null)
      expect(firsts).toEqual([])
      expect(diffs).toBe(0)
      // Idempotent: second fault-in is a no-op.
      expect((await SessionColdV2.faultInSessions(archive, live, ["s1"])).sessions).toBe(0)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("evict round-trips: clean evicts to stub, dirty refuses, fault-in restores", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const origin = await buildLive(dir, "opencode.db")
      const live = liveV2PathFor(origin)
      const archive = archivePathFor(origin)
      await withEnv({ OPENCODE_COLD_V2_QUIET: undefined, OPENCODE_COLD_V2_AUTO_MIGRATE: "1", CI: "1" }, async () => {
        expect(await maybeWarnColdV2Migration({ origin, live, archive })).toBe("migrated")
      })
      await SessionColdV2.faultInSessions(archive, live, ["s1"])
      // Clean evict: heavy drops, header stays for browsing.
      expect((await SessionColdV2.evictSessions(archive, live, ["s1"])).sessions).toBe(1)
      const stub = await SessionColdV2.openRawDb(live, "ro")
      try {
        expect(stub.get<{ n: number }>(`SELECT COUNT(*) AS n FROM session`)?.n).toBe(1)
        expect(stub.get<{ n: number }>(`SELECT COUNT(*) AS n FROM message`)?.n).toBe(0)
        expect(stub.get<{ n: number }>(`SELECT COUNT(*) AS n FROM part`)?.n).toBe(0)
      } finally {
        stub.close()
      }
      // Fault-in again is EXACT.
      expect((await SessionColdV2.faultInSessions(archive, live, ["s1"])).sessions).toBe(1)
      // Dirty live refuses eviction instead of losing writes.
      const dirty = await SessionColdV2.openRawDb(live, "rw")
      try {
        dirty.run(`INSERT INTO message VALUES (?, ?)`, ["m-dirty", "s1"])
      } finally {
        dirty.close()
      }
      await expect(SessionColdV2.evictSessions(archive, live, ["s1"])).rejects.toThrow(/pack first/)
      // Fault-in never resurrects deletes.
      const del = await SessionColdV2.openRawDb(live, "rw")
      try {
        for (const table of ["session_input", "session_context_epoch", "session_message", "todo", "part", "message", "event", "event_sequence"]) {
          try {
            del.exec(`DELETE FROM "${table}" WHERE session_id = 's1' OR aggregate_id = 's1'`)
          } catch {
            // Tables keyed the other way throw; the other statement covers them.
          }
        }
        del.run(`DELETE FROM session WHERE id = ?`, ["s1"])
      } finally {
        del.close()
      }
      expect((await SessionColdV2.faultInSessions(archive, live, ["s1"])).sessions).toBe(0)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("merge pack persists new live sessions without losing archived payloads", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const origin = await buildLive(dir, "opencode.db")
      const live = liveV2PathFor(origin)
      const archive = archivePathFor(origin)
      await withEnv({ OPENCODE_COLD_V2_QUIET: undefined, OPENCODE_COLD_V2_AUTO_MIGRATE: "1", CI: "1" }, async () => {
        expect(await maybeWarnColdV2Migration({ origin, live, archive })).toBe("migrated")
      })
      // New session created post-migration lives only in live (slim + open).
      const writer = await SessionColdV2.openRawDb(live, "rw")
      try {
        writer.run(`INSERT INTO session VALUES (?, ?)`, ["s2", "proj-a"])
        writer.run(`INSERT INTO message VALUES (?, ?)`, ["m2", "s2"])
        writer.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, ["p2", "m2", "s2", JSON.stringify({ type: "text", text: "new session here" })])
      } finally {
        writer.close()
      }
      const packed = await SessionColdV2.packLiveToArchive({ live, archive, minBytes: 200, verify: true })
      expect(packed.mergedUpdated).toBeGreaterThanOrEqual(1)
      expect((await migrationStatus(origin, live, archive)).archiveSessions).toBe(2)
      // Archived s1 still faults EXACT after the merge.
      await SessionColdV2.evictSessions(archive, live, ["s1"]).catch(() => undefined)
      expect((await SessionColdV2.faultInSessions(archive, live, ["s1"])).sessions).toBe(1)
      const fresh = join(dir, "fresh-merge.db")
      await copyFile(archive, fresh)
      await SessionColdV2.restoreFile(fresh, false)
      const check = await SessionColdV2.openRawDb(fresh, "ro")
      try {
        expect(check.get<{ n: number }>(`SELECT COUNT(*) AS n FROM session`)?.n).toBe(2)
        expect(check.get<{ n: number }>(`SELECT COUNT(*) AS n FROM message WHERE session_id = 's2'`)?.n).toBe(1)
      } finally {
        check.close()
      }
    } finally {
      await cleanup()
    }
  }, 300_000)

  test("empty live file restores without clobbering new work", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const origin = await buildLive(dir, "opencode.db")
      const live = liveV2PathFor(origin)
      const archive = archivePathFor(origin)
      await withEnv({ OPENCODE_COLD_V2_QUIET: undefined, OPENCODE_COLD_V2_AUTO_MIGRATE: "1", CI: "1" }, async () => {
        expect(await maybeWarnColdV2Migration({ origin, live, archive })).toBe("migrated")
      })
      // Simulate the reported bug against the new file: live removed, the app
      // recreates an empty live file on next boot, sessions vanish from the UI.
      await rm(live, { force: true })
      const empty = await SessionColdV2.openRawDb(live, "rw")
      try {
        empty.exec(`CREATE TABLE session (id TEXT PRIMARY KEY, project_id TEXT)`)
      } finally {
        empty.close()
      }
      const status = await migrationStatus(origin, live, archive)
      expect(status.archiveState).toBe("complete")
      expect(status.liveSessions).toBe(0)
      expect(status.needsRestore).toBe(true)
      await withEnv({ OPENCODE_COLD_V2_QUIET: "1", CI: "1" }, async () => {
        expect(await maybeWarnColdV2Migration({ origin, live, archive })).toBe("restored")
      })
      expect((await migrationStatus(origin, live, archive)).liveSessions).toBe(1)
      // A live file that already holds sessions is never overwritten: add a
      // post-pack session, then boot again and confirm it survives.
      const db = await SessionColdV2.openRawDb(live, "rw")
      try {
        db.run(`INSERT INTO session VALUES (?, ?)`, ["s-new", "proj-a"])
      } finally {
        db.close()
      }
      await withEnv({ OPENCODE_COLD_V2_QUIET: undefined, OPENCODE_COLD_V2_AUTO_MIGRATE: "1", CI: "1" }, async () => {
        expect(await maybeWarnColdV2Migration({ origin, live, archive })).toBe("done")
      })
      const kept = await SessionColdV2.openRawDb(live, "ro")
      try {
        expect(kept.get<{ n: number }>(`SELECT COUNT(*) AS n FROM session`)?.n).toBe(2)
      } finally {
        kept.close()
      }
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("empty archive over an empty live never loops a restore", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const origin = await buildLive(dir, "opencode.db")
      const emptied = await SessionColdV2.openRawDb(origin, "rw")
      try {
        for (const table of ["part", "event", "message", "session"]) emptied.exec(`DELETE FROM "${table}"`)
      } finally {
        emptied.close()
      }
      const live = liveV2PathFor(origin)
      const archive = archivePathFor(origin)
      await SessionColdV2.packArchiveFlow({ src: origin, dst: archive, allow: null, minBytes: 200, verify: true, treatAsLive: false })
      expect((await migrationStatus(origin, live, archive)).archiveState).toBe("complete")
      // Nothing anywhere to recover: done, never "restored".
      const status = await migrationStatus(origin, live, archive)
      expect(status.archiveSessions).toBe(0)
      expect(status.needsRestore).toBe(false)
      await withEnv({ OPENCODE_COLD_V2_QUIET: undefined, OPENCODE_COLD_V2_AUTO_MIGRATE: "1", CI: "1" }, async () => {
        expect(await maybeWarnColdV2Migration({ origin, live, archive })).toBe("done")
      })
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("incomplete and corrupt archives still need migration from the origin", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const origin = await buildLive(dir, "opencode.db")
      const live = liveV2PathFor(origin)
      const archive = archivePathFor(origin)
      await copyFile(origin, archive)
      await SessionColdV2.packFile(archive, null, 200)
      expect((await migrationStatus(origin, live, archive)).archiveState).toBe("incomplete")
      expect((await migrationStatus(origin, live, archive)).needsMigration).toBe(true)
      expect((await migrationStatus(origin, live, archive)).source).toBe(origin)
      await writeFile(archive, "garbage-bytes")
      expect((await migrationStatus(origin, live, archive)).archiveState).toBe("corrupt")
      expect((await migrationStatus(origin, live, archive)).needsMigration).toBe(true)
    } finally {
      await cleanup()
    }
  })

  test("packArchiveFlow converts offline v1 without touching it", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const origin = await buildLive(dir, "opencode.db")
      const before = await fingerprint(origin)
      const beforeStat = await stat(origin)
      const dst = join(dir, "opencode-cold-v2.db")
      const done = await SessionColdV2.packArchiveFlow({ src: origin, dst, allow: null, minBytes: 200, verify: true, treatAsLive: false })
      expect(done.blobs).toBeGreaterThan(0)
      expect(done.digest).toMatch(/^[0-9a-f]{64}$/)
      // v1 original byte-identical (content and mtime).
      expect(await fingerprint(origin)).toBe(before)
      expect((await stat(origin)).mtimeMs).toBe(beforeStat.mtimeMs)
      // Archive is complete and restores EXACT.
      const live = liveV2PathFor(origin)
      const status = await migrationStatus(origin, live, dst)
      expect(status.archiveState).toBe("complete")
      expect(status.needsMigration).toBe(false)
      expect(status.needsRestore).toBe(true)
      const rest = join(dir, "restored.db")
      await copyFile(dst, rest)
      await SessionColdV2.restoreFile(rest, false)
      const { diffs, firsts } = await SessionColdV2.compareFiles(origin, rest, null)
      expect(firsts).toEqual([])
      expect(diffs).toBe(0)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("quiet env stays silent, headless warns without prompting", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const origin = await buildLive(dir, "opencode.db")
      const live = liveV2PathFor(origin)
      const archive = archivePathFor(origin)
      await withEnv({ OPENCODE_COLD_V2_QUIET: "1", OPENCODE_COLD_V2_AUTO_MIGRATE: undefined, CI: "1" }, async () => {
        expect(await maybeWarnColdV2Migration({ origin, live, archive })).toBe("silent")
      })
      await withEnv({ OPENCODE_COLD_V2_QUIET: undefined, OPENCODE_COLD_V2_AUTO_MIGRATE: undefined, CI: "1" }, async () => {
        expect(await maybeWarnColdV2Migration({ origin, live, archive })).toBe("warned")
      })
    } finally {
      await cleanup()
    }
  }, 120_000)

  test("quiet still restores a missing live file", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const origin = await buildLive(dir, "opencode.db")
      const live = liveV2PathFor(origin)
      const archive = archivePathFor(origin)
      await withEnv({ OPENCODE_COLD_V2_QUIET: undefined, OPENCODE_COLD_V2_AUTO_MIGRATE: "1", CI: "1" }, async () => {
        expect(await maybeWarnColdV2Migration({ origin, live, archive })).toBe("migrated")
      })
      await rm(live, { force: true })
      // Recovery beats quiet: the restore runs, only its log is silenced.
      await withEnv({ OPENCODE_COLD_V2_QUIET: "1", CI: "1" }, async () => {
        expect(await maybeWarnColdV2Migration({ origin, live, archive })).toBe("restored")
      })
      expect((await migrationStatus(origin, live, archive)).liveSessions).toBe(1)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("merge pack refuses to publish an empty live over a full archive", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const origin = await buildLive(dir, "opencode.db")
      const live = liveV2PathFor(origin)
      const archive = archivePathFor(origin)
      await withEnv({ OPENCODE_COLD_V2_QUIET: undefined, OPENCODE_COLD_V2_AUTO_MIGRATE: "1", CI: "1" }, async () => {
        expect(await maybeWarnColdV2Migration({ origin, live, archive })).toBe("migrated")
      })
      // Simulate a wiped/recreated live file with db commands (which skip the
      // startup restore): packing must refuse, not destroy the durable copy.
      await rm(live, { force: true })
      const empty = await SessionColdV2.openRawDb(live, "rw")
      try {
        empty.exec(`CREATE TABLE session (id TEXT PRIMARY KEY, project_id TEXT)`)
      } finally {
        empty.close()
      }
      await expect(SessionColdV2.packLiveToArchive({ live, archive, minBytes: 200, verify: true })).rejects.toThrow(
        /refusing to publish an empty archive/,
      )
      // The archive is untouched and still restores.
      expect((await migrationStatus(origin, live, archive)).archiveSessions).toBe(1)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("full restore API decompresses EXACT for explicit recovery", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const origin = await buildLive(dir, "opencode.db")
      const live = liveV2PathFor(origin)
      const archive = archivePathFor(origin)
      await withEnv({ OPENCODE_COLD_V2_QUIET: undefined, OPENCODE_COLD_V2_AUTO_MIGRATE: "1", CI: "1" }, async () => {
        expect(await maybeWarnColdV2Migration({ origin, live, archive })).toBe("migrated")
      })
      await rm(live, { force: true })
      const done = await SessionColdV2.restoreLiveFullFromArchive({ archive, live })
      expect(done.sessions).toBe(1)
      const fresh = join(dir, "fresh-full.db")
      await copyFile(archive, fresh)
      await SessionColdV2.restoreFile(fresh, false)
      const { diffs, firsts } = await SessionColdV2.compareFiles(fresh, live, null)
      expect(firsts).toEqual([])
      expect(diffs).toBe(0)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("fault-in refuses swapped rows both directions", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const origin = await buildLive(dir, "opencode.db")
      // A row large enough to pack as a real pointer under MIN_BYTES_DEFAULT:
      // the buildLive rows are all inline, giving tampering nothing to target.
      const big = await SessionColdV2.openRawDb(origin, "rw")
      try {
        big.run(`INSERT INTO message VALUES (?, ?)`, ["m-big", "s1"])
        big.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, [
          "p-huge",
          "m-big",
          "s1",
          JSON.stringify({ type: "text", text: `HUGE-${"y".repeat(5000)}` }),
        ])
      } finally {
        big.close()
      }
      const live = liveV2PathFor(origin)
      const archive = archivePathFor(origin)
      await withEnv({ OPENCODE_COLD_V2_QUIET: undefined, OPENCODE_COLD_V2_AUTO_MIGRATE: "1", CI: "1" }, async () => {
        expect(await maybeWarnColdV2Migration({ origin, live, archive })).toBe("migrated")
      })
      // Direction 1: pointer replaced with plain data, registry intact.
      const swap = await SessionColdV2.openRawDb(archive, "rw")
      try {
        swap.run(`UPDATE part SET data = ? WHERE id = ?`, [JSON.stringify({ type: "text", text: "forged" }), "p-huge"])
      } finally {
        swap.close()
      }
      await expect(SessionColdV2.faultInSessions(archive, live, ["s1"])).rejects.toThrow(/plain data but registered as a pointer/)
      // Direction 2: pointer-shaped row with its registry entry deleted.
      const restore = await SessionColdV2.openRawDb(archive, "rw")
      try {
        restore.run(`UPDATE part SET data = ? WHERE id = ?`, [JSON.stringify({ _blob: "0".repeat(64) }), "p-huge"])
        restore.run(`DELETE FROM ptr WHERE t = 'part' AND id = ?`, ["p-huge"])
      } finally {
        restore.close()
      }
      await expect(SessionColdV2.faultInSessions(archive, live, ["s1"])).rejects.toThrow(/no registry entry/)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("merge handles attached schemas: non-session tables merge, stubs keep heavy", async () => {
    const { dir, cleanup } = await scratch()
    try {
      // Regression: pragma_table_info('src.t') returns no rows; the merge must
      // use the two-argument form or production schemas (project table) fail.
      const mk = async (name: string): Promise<string> => {
        const file = join(dir, name)
        const db = await SessionColdV2.openRawDb(file, "rw")
        try {
          db.exec(`CREATE TABLE session (id TEXT PRIMARY KEY, title TEXT)`)
          db.exec(`CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT)`)
          db.exec(`CREATE TABLE part (id TEXT PRIMARY KEY, session_id TEXT, data TEXT)`)
          db.exec(`CREATE TABLE project (id TEXT PRIMARY KEY, name TEXT)`)
        } finally {
          db.close()
        }
        return file
      }
      const tmpLive = await mk("merge-live.db")
      const tmpFull = await mk("merge-full.db")
      const liveDb = await SessionColdV2.openRawDb(tmpLive, "rw")
      try {
        liveDb.run(`INSERT INTO session VALUES (?, ?)`, ["s-stub", "stub title live"])
        liveDb.run(`INSERT INTO session VALUES (?, ?)`, ["s-open", "open live"])
        liveDb.run(`INSERT INTO message VALUES (?, ?)`, ["m-open", "s-open"])
        liveDb.run(`INSERT INTO project VALUES (?, ?)`, ["p-live", "from live"])
      } finally {
        liveDb.close()
      }
      const fullDb = await SessionColdV2.openRawDb(tmpFull, "rw")
      try {
        fullDb.run(`INSERT INTO session VALUES (?, ?)`, ["s-stub", "stub title archive"])
        fullDb.run(`INSERT INTO message VALUES (?, ?)`, ["m-stub", "s-stub"])
        fullDb.run(`INSERT INTO session VALUES (?, ?)`, ["s-gone", "deleted in live"])
        fullDb.run(`INSERT INTO message VALUES (?, ?)`, ["m-gone", "s-gone"])
        fullDb.run(`INSERT INTO project VALUES (?, ?)`, ["p-arch", "from archive"])
      } finally {
        fullDb.close()
      }
      const merged = await SessionColdV2.mergeLiveIntoFull(tmpLive, tmpFull)
      expect(merged.updated).toBe(1)
      expect(merged.keptStubs).toBe(1)
      const check = await SessionColdV2.openRawDb(tmpFull, "ro")
      try {
        // Stub: archived heavy kept, live header wins.
        expect(check.get<{ n: number }>(`SELECT COUNT(*) AS n FROM message WHERE session_id = 's-stub'`)?.n).toBe(1)
        expect(check.get<{ title: string }>(`SELECT title FROM session WHERE id = 's-stub'`)?.title).toBe("stub title live")
        // Open: subtree replaced from live.
        expect(check.get<{ n: number }>(`SELECT COUNT(*) AS n FROM message WHERE session_id = 's-open'`)?.n).toBe(1)
        // Deleted in live: dropped from the image.
        expect(check.get<{ n: number }>(`SELECT COUNT(*) AS n FROM session WHERE id = 's-gone'`)?.n).toBe(0)
        // Non-session tables merge as a superset from both sides.
        expect(check.get<{ n: number }>(`SELECT COUNT(*) AS n FROM project`)?.n).toBe(2)
      } finally {
        check.close()
      }
    } finally {
      await cleanup()
    }
  })

  test("merge never folds packed-archive tables into the image", async () => {
    const { dir, cleanup } = await scratch()
    try {
      // bundle/bptr/fault_state can never occur in a real live file
      // (slim/restore drop them), but if one ever did, merging its rows
      // into the image would corrupt the next pack — skip defensively.
      // Both sides carry the tables here: without the skip, the shared()
      // gate would pass and live rows would land in the image.
      const mk = async (name: string, packed: boolean): Promise<string> => {
        const file = join(dir, name)
        const db = await SessionColdV2.openRawDb(file, "rw")
        try {
          db.exec(`CREATE TABLE session (id TEXT PRIMARY KEY, title TEXT)`)
          db.exec(`CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT)`)
          db.exec(`CREATE TABLE bundle (session_id TEXT, chunk INTEGER)`)
          db.exec(`CREATE TABLE bptr (t TEXT, id TEXT, session_id TEXT, chunk INTEGER, off INTEGER, ln INTEGER)`)
          db.exec(`CREATE TABLE fault_state (session_id TEXT PRIMARY KEY, complete INTEGER NOT NULL)`)
          if (packed) {
            db.run(`INSERT INTO bundle VALUES (?, ?)`, ["s-open", 0])
            db.run(`INSERT INTO bptr VALUES (?, ?, ?, ?, ?, ?)`, ["part", "p-x", "s-open", 0, 0, 10])
            db.run(`INSERT INTO fault_state VALUES (?, ?)`, ["s-open", 0])
          }
        } finally {
          db.close()
        }
        return file
      }
      const tmpLive = await mk("merge-live.db", true)
      const tmpFull = await mk("merge-full.db", false)
      const liveDb = await SessionColdV2.openRawDb(tmpLive, "rw")
      try {
        liveDb.run(`INSERT INTO session VALUES (?, ?)`, ["s-open", "open live"])
        liveDb.run(`INSERT INTO message VALUES (?, ?)`, ["m-open", "s-open"])
      } finally {
        liveDb.close()
      }
      await SessionColdV2.mergeLiveIntoFull(tmpLive, tmpFull)
      const check = await SessionColdV2.openRawDb(tmpFull, "ro")
      try {
        // Packed tables present on both sides yet untouched: no live rows
        // folded in (the full side stays empty).
        expect(check.get<{ n: number }>(`SELECT COUNT(*) AS n FROM bundle`)?.n).toBe(0)
        expect(check.get<{ n: number }>(`SELECT COUNT(*) AS n FROM bptr`)?.n).toBe(0)
        expect(check.get<{ n: number }>(`SELECT COUNT(*) AS n FROM fault_state`)?.n).toBe(0)
        // The session itself still merged.
        expect(check.get<{ n: number }>(`SELECT COUNT(*) AS n FROM message WHERE session_id = 's-open'`)?.n).toBe(1)
      } finally {
        check.close()
      }
    } finally {
      await cleanup()
    }
  })

  // Policy-column fixture for auto-evict: the shared buildLive above has a
  // minimal session table (no time_updated), on which auto-evict must fail
  // closed. Slim restore copies the archive file, so extra header columns
  // survive migration untouched.
  const buildLivePolicy = async (dir: string, name: string): Promise<string> => {
    const file = join(dir, name)
    const now = Date.now()
    const old = now - 2 * 3600 * 1000
    const db = await SessionColdV2.openRawDb(file, "rw")
    try {
      db.exec(`CREATE TABLE session (id TEXT PRIMARY KEY, parent_id TEXT, title TEXT, time_archived INTEGER, time_updated INTEGER, project_id TEXT)`)
      db.exec(`CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT)`)
      db.exec(`CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, data TEXT)`)
      db.exec(`CREATE TABLE event (id TEXT PRIMARY KEY, aggregate_id TEXT, type TEXT, data TEXT)`)
      db.exec(`CREATE TABLE event_sequence (aggregate_id TEXT PRIMARY KEY, seq INTEGER)`)
      db.exec(`CREATE TABLE todo (session_id TEXT, content TEXT)`)
      db.exec(`CREATE TABLE session_message (id TEXT PRIMARY KEY, session_id TEXT)`)
      db.exec(`CREATE TABLE session_input (session_id TEXT)`)
      db.exec(`CREATE TABLE session_context_epoch (session_id TEXT)`)
      const seed = (id: string, updated: number): void => {
        db.run(`INSERT INTO session VALUES (?, ?, ?, ?, ?, ?)`, [id, null, `${id} title`, null, updated, "proj-a"])
        db.run(`INSERT INTO message VALUES (?, ?)`, [`m-${id}`, id])
        db.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, [`p-${id}`, `m-${id}`, id, JSON.stringify({ type: "text", text: `hello ${id}` })])
      }
      seed("s-old", old)
      seed("s-fresh", now)
      seed("s-dirty", old)
      seed("s-spared", old)
    } finally {
      db.close()
    }
    return file
  }

  const liveCounts = async (live: string, id: string): Promise<{ headers: number; messages: number }> => {
    const db = await SessionColdV2.openRawDb(live, "ro")
    try {
      return {
        headers: db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM session WHERE id = ?`, [id])?.n ?? 0,
        messages: db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM message WHERE session_id = ?`, [id])?.n ?? 0,
      }
    } finally {
      db.close()
    }
  }

  test("auto-evict files idle residents, skips fresh/dirty/excluded", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const origin = await buildLivePolicy(dir, "opencode.db")
      const live = liveV2PathFor(origin)
      const archive = archivePathFor(origin)
      await withEnv({ OPENCODE_COLD_V2_QUIET: undefined, OPENCODE_COLD_V2_AUTO_MIGRATE: "1", CI: "1" }, async () => {
        expect(await maybeWarnColdV2Migration({ origin, live, archive })).toBe("migrated")
      })
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-old", "s-fresh", "s-dirty", "s-spared"])).sessions).toBe(4)
      // s-dirty diverges after the pack: eviction must skip it, not lose it.
      const dirty = await SessionColdV2.openRawDb(live, "rw")
      try {
        dirty.run(`INSERT INTO message VALUES (?, ?)`, ["m-dirty-extra", "s-dirty"])
      } finally {
        dirty.close()
      }
      const done = await SessionColdV2.autoEvictIdle(archive, live, { exclude: ["s-spared"], idleMinutes: 30, max: 10 })
      expect(done.evicted).toBe(1)
      expect(done.skipped).toBe(1)
      // Filed: header kept for browsing, payloads dropped.
      expect(await liveCounts(live, "s-old")).toEqual({ headers: 1, messages: 0 })
      // Kept: fresh (inside the idle window), dirty (unpacked writes),
      // spared (explicitly excluded) all stay resident.
      expect((await liveCounts(live, "s-fresh")).messages).toBe(1)
      expect((await liveCounts(live, "s-dirty")).messages).toBe(2)
      expect((await liveCounts(live, "s-spared")).messages).toBe(1)
      // The filed session still faults back EXACT.
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-old"])).sessions).toBe(1)
      expect((await liveCounts(live, "s-old")).messages).toBe(1)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("auto-evict fails closed without time_updated and caps the sweep", async () => {
    const { dir, cleanup } = await scratch()
    try {
      // Minimal layout (no time_updated): the idle policy cannot be applied,
      // so nothing is evicted rather than everything.
      const origin = await buildLive(dir, "opencode.db")
      const live = liveV2PathFor(origin)
      const archive = archivePathFor(origin)
      await withEnv({ OPENCODE_COLD_V2_QUIET: undefined, OPENCODE_COLD_V2_AUTO_MIGRATE: "1", CI: "1" }, async () => {
        expect(await maybeWarnColdV2Migration({ origin, live, archive })).toBe("migrated")
      })
      await SessionColdV2.faultInSessions(archive, live, ["s1"])
      expect(await SessionColdV2.autoEvictIdle(archive, live, {})).toEqual({
        evicted: 0,
        skipped: 0,
        phaseMs: expect.anything(),
      })
      const kept = await SessionColdV2.openRawDb(live, "ro")
      try {
        expect(kept.get<{ n: number }>(`SELECT COUNT(*) AS n FROM message`)?.n).toBeGreaterThan(0)
      } finally {
        kept.close()
      }
      // max: 0 disables the sweep even where candidates exist. Separate
      // subdirectory: live/archive paths derive from the directory.
      const dir2 = join(dir, "second")
      await mkdir(dir2, { recursive: true })
      const origin2 = await buildLivePolicy(dir2, "second.db")
      const live2 = liveV2PathFor(origin2)
      const archive2 = archivePathFor(origin2)
      await withEnv({ OPENCODE_COLD_V2_QUIET: undefined, OPENCODE_COLD_V2_AUTO_MIGRATE: "1", CI: "1" }, async () => {
        expect(await maybeWarnColdV2Migration({ origin: origin2, live: live2, archive: archive2 })).toBe("migrated")
      })
      await SessionColdV2.faultInSessions(archive2, live2, ["s-old", "s-fresh", "s-dirty", "s-spared"])
      expect((await SessionColdV2.autoEvictIdle(archive2, live2, { max: 0 })).evicted).toBe(0)
      expect((await SessionColdV2.autoEvictIdle(archive2, live2, { max: 1, idleMinutes: 30 })).evicted).toBe(1)
    } finally {
      await cleanup()
    }
  }, 300_000)

  test("auto-evict settings default on and parse env", async () => {
    await withEnv(
      { OPENCODE_COLD_V2_AUTO_EVICT: undefined, OPENCODE_COLD_V2_EVICT_IDLE_MINUTES: undefined, OPENCODE_COLD_V2_EVICT_MAX: undefined },
      async () => {
        expect(autoEvictSettings()).toEqual({ enabled: true, idleMinutes: 30, max: 20 })
      },
    )
    await withEnv({ OPENCODE_COLD_V2_AUTO_EVICT: "0" }, async () => {
      expect(autoEvictSettings().enabled).toBe(false)
    })
    await withEnv({ OPENCODE_COLD_V2_AUTO_EVICT: "false" }, async () => {
      expect(autoEvictSettings().enabled).toBe(false)
    })
    await withEnv({ OPENCODE_COLD_V2_EVICT_IDLE_MINUTES: "5", OPENCODE_COLD_V2_EVICT_MAX: "3" }, async () => {
      expect(autoEvictSettings()).toEqual({ enabled: true, idleMinutes: 5, max: 3 })
    })
    await withEnv({ OPENCODE_COLD_V2_EVICT_IDLE_MINUTES: "abc", OPENCODE_COLD_V2_EVICT_MAX: "-2" }, async () => {
      expect(autoEvictSettings()).toEqual({ enabled: true, idleMinutes: 30, max: 20 })
    })
  })

  // Tail-first fixture: one idle session with enough history to stay partial
  // under a small tail window. Production-shaped columns (time_updated for the
  // idle policy, time_created for newest-first windows); payloads exceed the
  // 2048 migration pack threshold so parts become pointers and pu1 events
  // become slims.
  const TAIL_N = 40
  const buildLiveTail = async (dir: string, name: string): Promise<string> => {
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
      db.run(`INSERT INTO session VALUES (?, ?, ?, ?, ?, ?)`, ["s-tail", null, "tail title", null, old, "proj-a"])
      db.run(`INSERT INTO event_sequence VALUES (?, ?)`, ["s-tail", TAIL_N])
      const pad = (n: number): string => String(n).padStart(2, "0")
      for (let i = 0; i < TAIL_N; i += 1) {
        const mid = `m-${pad(i)}`
        db.run(`INSERT INTO message VALUES (?, ?, ?)`, [mid, "s-tail", 1000 + i])
        db.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, [
          `p-${pad(i)}`,
          mid,
          "s-tail",
          J({ type: "text", text: `TAIL-${i}-${"x".repeat(2500)}`, time: { start: i, end: i + 1 } }),
        ])
        db.run(`INSERT INTO event VALUES (?, ?, ?, ?, ?)`, [
          `e-${pad(i)}`,
          "s-tail",
          i + 1,
          "message.part.updated.1",
          J({
            sessionID: "s-tail",
            part: { id: `p-${pad(i)}`, sessionID: "s-tail", messageID: mid, type: "text", text: `EV-${i}-${"e".repeat(2500)}` },
            time: i + 1,
          }),
        ])
      }
    } finally {
      db.close()
    }
    return file
  }

  const tailCounts = async (live: string): Promise<{ messages: number; parts: number; events: number; marker: number | null }> => {
    const db = await SessionColdV2.openRawDb(live, "ro")
    try {
      const n = (sql: string): number => db.get<{ n: number }>(sql)?.n ?? 0
      let marker: number | null = null
      try {
        marker = db.get<{ complete: number }>(`SELECT complete FROM fault_state WHERE session_id = 's-tail'`)?.complete ?? null
      } catch {
        marker = null
      }
      return {
        messages: n(`SELECT COUNT(*) AS n FROM message WHERE session_id = 's-tail'`),
        parts: n(`SELECT COUNT(*) AS n FROM part WHERE session_id = 's-tail'`),
        events: n(`SELECT COUNT(*) AS n FROM event WHERE aggregate_id = 's-tail'`),
        marker,
      }
    } finally {
      db.close()
    }
  }

  const migrateTail = async (dir: string): Promise<{ origin: string; live: string; archive: string }> => {
    const origin = await buildLiveTail(dir, "opencode.db")
    const live = liveV2PathFor(origin)
    const archive = archivePathFor(origin)
    await withEnv({ OPENCODE_COLD_V2_QUIET: undefined, OPENCODE_COLD_V2_AUTO_MIGRATE: "1", CI: "1" }, async () => {
      expect(await maybeWarnColdV2Migration({ origin, live, archive })).toBe("migrated")
    })
    return { origin, live, archive }
  }

  test("tail faults the newest window, marks partial, completes EXACT", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const { origin, live, archive } = await migrateTail(dir)
      const window = 5 + SessionColdV2.TAIL_OVERLAP
      const tail = await SessionColdV2.faultInSessions(archive, live, ["s-tail"], { tailMessages: 5 })
      expect(tail.sessions).toBe(1)
      expect(tail.parts).toBe(window)
      expect(tail.events).toBe(0)
      const counts = await tailCounts(live)
      expect(counts).toEqual({ messages: window, parts: window, events: 0, marker: 0 })
      // Newest slice only: m-25..m-39 for a 40-message session under window 15.
      const db = await SessionColdV2.openRawDb(live, "ro")
      try {
        expect(db.get<{ id: string }>(`SELECT id FROM message WHERE session_id = 's-tail' ORDER BY id ASC LIMIT 1`)?.id).toBe("m-25")
      } finally {
        db.close()
      }
      // A second tail call is a no-op (the tail is present); the full call
      // completes and the result equals a fresh full unpack byte-for-byte.
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-tail"], { tailMessages: 5 })).sessions).toBe(0)
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-tail"])).sessions).toBe(1)
      expect((await tailCounts(live)).marker).toBe(1)
      const { diffs, firsts } = await SessionColdV2.compareFiles(origin, live, null)
      expect(firsts).toEqual([])
      expect(diffs).toBe(0)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("partial merge overlays without losing either side", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const { live, archive } = await migrateTail(dir)
      await SessionColdV2.faultInSessions(archive, live, ["s-tail"], { tailMessages: 5 })
      // Live-only write lands on the partial session (a prompt during the
      // background window).
      const writer = await SessionColdV2.openRawDb(live, "rw")
      try {
        writer.run(`INSERT INTO message VALUES (?, ?, ?)`, ["m-new", "s-tail", 9999])
        writer.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, ["p-new", "m-new", "s-tail", JSON.stringify({ type: "text", text: "live-only" })])
      } finally {
        writer.close()
      }
      const packed = await SessionColdV2.packLiveToArchive({ live, archive, minBytes: 2048, verify: true })
      expect(packed.mergedUpdated).toBeGreaterThanOrEqual(1)
      // The archive must not smuggle live bookkeeping.
      const check = await SessionColdV2.openRawDb(archive, "ro")
      try {
        expect(check.all(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'fault_state'`).length).toBe(0)
      } finally {
        check.close()
      }
      // Evict proves the merge preserved the overlay exactly: it completes
      // the partial session inside its transaction and byte-compares.
      expect((await SessionColdV2.evictSessions(archive, live, ["s-tail"])).sessions).toBe(1)
      expect((await tailCounts(live)).messages).toBe(0)
      // ... and the live-only row survives the round-trip.
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-tail"])).sessions).toBe(1)
      expect((await tailCounts(live)).messages).toBe(TAIL_N + 1)
    } finally {
      await cleanup()
    }
  }, 300_000)

  test("partial evict completes inside its transaction", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const { live, archive } = await migrateTail(dir)
      await SessionColdV2.faultInSessions(archive, live, ["s-tail"], { tailMessages: 5 })
      expect((await tailCounts(live)).marker).toBe(0)
      expect((await SessionColdV2.evictSessions(archive, live, ["s-tail"])).sessions).toBe(1)
      expect(await tailCounts(live)).toEqual({ messages: 0, parts: 0, events: 0, marker: null })
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-tail"])).sessions).toBe(1)
      expect((await tailCounts(live)).messages).toBe(TAIL_N)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("legacy sessions without markers adopt as complete", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const { live, archive } = await migrateTail(dir)
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-tail"])).sessions).toBe(1)
      // Simulate a pre-marker live file: rows present, no marker rows.
      const wipe = await SessionColdV2.openRawDb(live, "rw")
      try {
        wipe.exec(`DELETE FROM fault_state`)
      } finally {
        wipe.close()
      }
      // Adoption: residency implies whole, so no re-fault.
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-tail"])).sessions).toBe(0)
      expect((await tailCounts(live)).messages).toBe(TAIL_N)
      // Residency-based paths (auto-evict) still see the adopted session.
      expect((await SessionColdV2.autoEvictIdle(archive, live, {})).evicted).toBe(1)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("dirty-partial completion preserves live writes", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const { live, archive } = await migrateTail(dir)
      await SessionColdV2.faultInSessions(archive, live, ["s-tail"], { tailMessages: 5 })
      const writer = await SessionColdV2.openRawDb(live, "rw")
      try {
        writer.run(`INSERT INTO message VALUES (?, ?, ?)`, ["m-extra", "s-tail", 9998])
      } finally {
        writer.close()
      }
      // Full fault overlays (OR IGNORE): the live row wins, gaps fill.
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-tail"])).sessions).toBe(1)
      const counts = await tailCounts(live)
      expect(counts.messages).toBe(TAIL_N + 1)
      expect(counts.marker).toBe(1)
      const db = await SessionColdV2.openRawDb(live, "ro")
      try {
        expect(db.get<{ one: number }>(`SELECT 1 AS one FROM message WHERE id = 'm-extra'`)?.one).toBe(1)
      } finally {
        db.close()
      }
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("auto-evict leaves partial sessions for the next sweep", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const { live, archive } = await migrateTail(dir)
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-tail"])).sessions).toBe(1)
      expect((await SessionColdV2.evictSessions(archive, live, ["s-tail"])).sessions).toBe(1)
      await SessionColdV2.faultInSessions(archive, live, ["s-tail"], { tailMessages: 5 })
      // Idle and resident, but partial: skipped explicitly, not miscounted.
      const done = await SessionColdV2.autoEvictIdle(archive, live, {})
      expect(done.evicted).toBe(0)
      expect(done.skipped).toBe(0)
      expect((await tailCounts(live)).messages).toBe(5 + SessionColdV2.TAIL_OVERLAP)
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("batch inserts stay exact past the variable limit", async () => {
    const { dir, cleanup } = await scratch()
    try {
      // 800 parts x 4 columns = 3200 variables: forces multi-VALUES chunks
      // (chunk cap rows*cols <= 3000) on the fault write path.
      const file = join(dir, "opencode.db")
      const db = await SessionColdV2.openRawDb(file, "rw")
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
        db.run(`INSERT INTO session VALUES (?, ?)`, ["s-wide", "proj-a"])
        db.run(`INSERT INTO message VALUES (?, ?)`, ["m-wide", "s-wide"])
        db.exec("BEGIN IMMEDIATE")
        try {
          for (let i = 0; i < 800; i += 1) {
            db.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, [`p-${i}`, "m-wide", "s-wide", JSON.stringify({ type: "text", text: `row ${i}` })])
          }
          db.exec("COMMIT")
        } catch (error) {
          try {
            db.exec("ROLLBACK")
          } catch {}
          throw error
        }
      } finally {
        db.close()
      }
      const live = liveV2PathFor(file)
      const archive = archivePathFor(file)
      await withEnv({ OPENCODE_COLD_V2_QUIET: undefined, OPENCODE_COLD_V2_AUTO_MIGRATE: "1", CI: "1" }, async () => {
        expect(await maybeWarnColdV2Migration({ origin: file, live, archive })).toBe("migrated")
      })
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-wide"])).sessions).toBe(1)
      const { diffs, firsts } = await SessionColdV2.compareFiles(file, live, null)
      expect(firsts).toEqual([])
      expect(diffs).toBe(0)
    } finally {
      await cleanup()
    }
  }, 180_000)
})

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

describe("archive hygiene", () => {
  const scratch = async (): Promise<{ dir: string; cleanup: () => Promise<void> }> => {
    const dir = join(tmpdir(), `opencode-cold-v2-hyg-${process.pid}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`)
    await mkdir(dir, { recursive: true })
    return { dir, cleanup: () => rm(dir, { recursive: true, force: true }) }
  }

  const buildLive = async (dir: string): Promise<string> => {
    const file = join(dir, "live.db")
    const db = await SessionColdV2.openRawDb(file, "rw")
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
      db.run(`INSERT INTO session VALUES (?, ?)`, ["s1", "proj-a"])
    } finally {
      db.close()
    }
    return file
  }

  test("explicit selection matching nothing fails loud instead of empty archive", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildLive(dir)
      const dst = join(dir, "cold.db")
      await expect(
        SessionColdV2.packArchiveFlow({ src: live, dst, allow: ["ses_nope"], minBytes: 200, verify: false, treatAsLive: false }),
      ).rejects.toThrow(/matched 0 sessions/)
      // Nothing published.
      expect(await Bun.file(dst).exists()).toBe(false)
    } finally {
      await cleanup()
    }
  }, 120_000)

  test("stale tmps from killed runs are swept inside the lock", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const live = await buildLive(dir)
      const dst = join(dir, "cold.db")
      await SessionColdV2.packArchiveFlow({ src: live, dst, allow: null, minBytes: 200, verify: false, treatAsLive: false })
      // Plant orphans with foreign pids plus a live-looking tmp, plus a
      // bystander file the sweep must not touch.
      await writeFile(`${dst}.tmp.99999999`, "orphan")
      await writeFile(`${dst}.tmp.99999999.verify`, "orphan")
      await writeFile(`${dst}.tmp.${process.pid}`, "orphan")
      await writeFile(join(dir, "cold.db-journal"), "bystander")
      const removed = await SessionColdV2.cleanStaleTmps(dst)
      expect(removed).toBe(3)
      expect(await Bun.file(`${dst}.tmp.99999999`).exists()).toBe(false)
      expect(await Bun.file(`${dst}.tmp.99999999.verify`).exists()).toBe(false)
      expect(await Bun.file(join(dir, "cold.db-journal")).exists()).toBe(true)
      // And the wired path: a fresh pack cleans before building.
      await writeFile(`${dst}.tmp.424242`, "orphan")
      await SessionColdV2.packArchiveFlow({ src: live, dst, allow: null, minBytes: 200, verify: false, treatAsLive: false, jobs: 1 })
      expect(await Bun.file(`${dst}.tmp.424242`).exists()).toBe(false)
    } finally {
      await cleanup()
    }
  }, 180_000)
})

describe("forkfar point resolution", () => {
  const mid = (n: number): MessageID => `msg_${String(n).padStart(4, "0")}` as MessageID
  // Interleaved user/assistant history: u1 a1 u2 a2 u3 a3.
  const all = [1, 2, 3, 4, 5, 6].map((n) => ({ info: { id: mid(n) } }))
  const users = [1, 3, 5].map((n) => ({ info: { id: mid(n) } }))

  test("ordinal resolves to the inclusive start plus the next-message cutoff", async () => {
    const { resolveForkPoint } = await import("@/cli/cmd/session")
    expect(resolveForkPoint(users, all, "1")).toEqual({ ordinal: 1, id: mid(1), cutoff: mid(2) })
    expect(resolveForkPoint(users, all, "2")).toEqual({ ordinal: 2, id: mid(3), cutoff: mid(4) })
    // Last user message is not the last overall: cutoff is the trailing assistant message.
    expect(resolveForkPoint(users, all, "3")).toEqual({ ordinal: 3, id: mid(5), cutoff: mid(6) })
  })

  test("last message overall forks whole (no cutoff)", async () => {
    const { resolveForkPoint } = await import("@/cli/cmd/session")
    const tail = [...all, { info: { id: mid(7) } }]
    const tailUsers = [...users, { info: { id: mid(7) } }]
    expect(resolveForkPoint(tailUsers, tail, "4")).toEqual({ ordinal: 4, id: mid(7), cutoff: undefined })
    expect(resolveForkPoint(tailUsers, tail, mid(7))).toEqual({ ordinal: 4, id: mid(7), cutoff: undefined })
  })

  test("message IDs resolve with the same inclusive rule", async () => {
    const { resolveForkPoint } = await import("@/cli/cmd/session")
    expect(resolveForkPoint(users, all, mid(3))).toEqual({ ordinal: 2, id: mid(3), cutoff: mid(4) })
    // Non-user IDs are addressable too (ordinal 0 = not a user message).
    expect(resolveForkPoint(users, all, mid(2))).toEqual({ ordinal: 0, id: mid(2), cutoff: mid(3) })
  })

  test("garbage resolves to undefined", async () => {
    const { resolveForkPoint } = await import("@/cli/cmd/session")
    expect(resolveForkPoint(users, all, "0")).toBeUndefined()
    expect(resolveForkPoint(users, all, "4")).toBeUndefined()
    expect(resolveForkPoint(users, all, "99")).toBeUndefined()
    expect(resolveForkPoint(users, all, "abc")).toBeUndefined()
    expect(resolveForkPoint(users, all, "ses_f5b269f30ffeBoAvQI1obuBnJ5")).toBeUndefined()
    expect(resolveForkPoint(users, all, "msg_missing")).toBeUndefined()
  })
})

describe("v5 bundles", () => {
  const scratch = async (): Promise<{ dir: string; cleanup: () => Promise<void> }> => {
    const dir = join(tmpdir(), `opencode-cold-v5-${process.pid}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`)
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

  // Deterministic incompressible bytes (LCG): the random session must fail
  // the 4x bar on merit, not on luck.
  const lcg = (seed: number, n: number): Buffer => {
    const out = Buffer.alloc(n)
    let state = seed >>> 0
    for (let i = 0; i < n; i += 1) {
      state = (Math.imul(state, 1664525) + 1013904223) >>> 0
      out[i] = (state >>> 24) & 0xff
    }
    return out
  }

  // Mixed corpus: s-big bundles, s-rand fails the bar (incompressible),
  // s-tiny fails the floors, s-sha/s-shb share one blob (stays global) while
  // bundling their unique parts.
  const buildV5 = async (dir: string, name: string): Promise<string> => {
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
      const seedSession = (id: string): void => {
        db.run(`INSERT INTO session VALUES (?, ?, ?, ?, ?, ?)`, [id, null, `${id} title`, null, old, "proj-a"])
        db.run(`INSERT INTO message VALUES (?, ?, ?)`, [`m-${id}`, id, old])
      }
      const bigPart = (tag: string): string => J({ type: "text", text: `${tag}-${"x".repeat(2900)}`, time: { start: 1, end: 2 } })
      const bigEvent = (sid: string, pid: string, mid: string, tag: string, seq: number): string =>
        J({ sessionID: sid, part: { id: pid, sessionID: sid, messageID: mid, type: "text", text: `${tag}-${"e".repeat(1900)}` }, time: seq })
      // s-big: ~450 messages/parts + 450 events, all repetitive (~2.2MB unique).
      seedSession("s-big")
      db.run(`INSERT INTO event_sequence VALUES (?, ?)`, ["s-big", 450])
      db.exec("BEGIN IMMEDIATE")
      try {
        for (let i = 0; i < 450; i += 1) {
          const pad = String(i).padStart(4, "0")
          db.run(`INSERT INTO message VALUES (?, ?, ?)`, [`mb-${pad}`, "s-big", 1000 + i])
          db.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, [`pb-${pad}`, `mb-${pad}`, "s-big", bigPart(`BIG-${i}`)])
          db.run(`INSERT INTO event VALUES (?, ?, ?, ?, ?)`, [`eb-${pad}`, "s-big", i + 1, "message.part.updated.1", bigEvent("s-big", `pb-${pad}`, `mb-${pad}`, `EV-${i}`, i + 1)])
        }
        db.exec("COMMIT")
      } catch (error) {
        try {
          db.exec("ROLLBACK")
        } catch {}
        throw error
      }
      // s-rand: 20 incompressible parts (bar fails deterministically).
      seedSession("s-rand")
      for (let i = 0; i < 20; i += 1) {
        const pad = String(i).padStart(3, "0")
        db.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, [`pr-${pad}`, "m-s-rand", "s-rand", J({ type: "text", text: lcg(1234 + i, 2500).toString("base64") })])
      }
      // s-tiny: single small part (floor fails).
      seedSession("s-tiny")
      db.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, ["pt-1", "m-s-tiny", "s-tiny", J({ type: "text", text: "hi" })])
      // s-sha / s-shb: one shared identical part + unique parts each.
      const shared = bigPart("SHARED-IDENTICAL")
      for (const sid of ["s-sha", "s-shb"]) {
        seedSession(sid)
        db.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, [`p-${sid}-shared`, `m-${sid}`, sid, shared])
        for (let i = 0; i < 30; i += 1) {
          db.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, [`p-${sid}-${i}`, `m-${sid}`, sid, bigPart(`U-${sid}-${i}`)])
        }
      }
    } finally {
      db.close()
    }
    return file
  }

  const migrateV5 = async (dir: string, name = "opencode.db"): Promise<{ origin: string; live: string; archive: string }> => {
    const origin = await buildV5(dir, name)
    const live = liveV2PathFor(origin)
    const archive = archivePathFor(origin)
    await withEnv({ OPENCODE_COLD_V2_QUIET: undefined, OPENCODE_COLD_V2_AUTO_MIGRATE: "1", CI: "1" }, async () => {
      expect(await maybeWarnColdV2Migration({ origin, live, archive })).toBe("migrated")
    })
    return { origin, live, archive }
  }

  const metaOf = async (file: string): Promise<Record<string, string>> => {
    const db = await SessionColdV2.openRawDb(file, "ro")
    try {
      return SessionColdV2.readMeta(db)
    } finally {
      db.close()
    }
  }

  test("v5 round-trips EXACT over a mixed corpus", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const { origin, live, archive } = await migrateV5(dir)
      const meta = await metaOf(archive)
      expect(meta["version"]).toBe("5")
      expect(Number(meta["count_bundle"])).toBeGreaterThan(0)
      expect(Number(meta["count_bptr"])).toBeGreaterThan(0)
      expect(meta["bundle_hash"]).toMatch(/^[0-9a-f]{64}$/)
      // Bundled sessions publish refs; classic sessions keep classic shapes.
      const packed = await SessionColdV2.openRawDb(archive, "ro")
      try {
        const bundled = packed.get<{ n: number }>(`SELECT COUNT(DISTINCT session_id) AS n FROM bundle`)?.n ?? 0
        expect(bundled).toBeGreaterThanOrEqual(3) // s-big, s-sha, s-shb
        // Shared blob survived in the global store (not un-shared).
        expect(packed.get<{ n: number }>(`SELECT COUNT(*) AS n FROM blob`)?.n ?? 0).toBeGreaterThan(0)
      } finally {
        packed.close()
      }
      // Fault everything in and byte-compare against the origin.
      const ids = ["s-big", "s-rand", "s-tiny", "s-sha", "s-shb"]
      expect((await SessionColdV2.faultInSessions(archive, live, ids)).sessions).toBe(5)
      const { diffs, firsts } = await SessionColdV2.compareFiles(origin, live, null)
      expect(firsts).toEqual([])
      expect(diffs).toBe(0)
      // Full verify passes and reports bundles.
      const report = await SessionColdV2.verifyArchive(archive)
      expect(report.bundles).toBeGreaterThan(0)
      expect(report.codecs).toContain("zstd-9-ldm")
    } finally {
      await cleanup()
    }
  }, 300_000)

  test("random-only archive stays version 4", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const file = join(dir, "opencode.db")
      const db = await SessionColdV2.openRawDb(file, "rw")
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
        db.run(`INSERT INTO session VALUES (?, ?)`, ["s-r", "proj-a"])
        db.run(`INSERT INTO message VALUES (?, ?)`, ["m-r", "s-r"])
        for (let i = 0; i < 10; i += 1) {
          db.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, [`p-${i}`, "m-r", "s-r", JSON.stringify({ type: "text", text: lcg(99 + i, 1500).toString("base64") })])
        }
      } finally {
        db.close()
      }
      const dst = join(dir, "cold.db")
      await SessionColdV2.packArchiveFlow({ src: file, dst, allow: null, minBytes: 200, verify: false, treatAsLive: false })
      expect(await metaOf(dst)).toMatchObject({ version: "4" })
      const check = await SessionColdV2.openRawDb(dst, "ro")
      try {
        // No bundle tables: byte-shape v4, old readers fine.
        expect(check.all(`SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('bundle', 'bptr')`).length).toBe(0)
      } finally {
        check.close()
      }
    } finally {
      await cleanup()
    }
  }, 180_000)

  test("multi-chunk tail faults the window and completes EXACT", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const { origin, live, archive } = await withEnv({ OPENCODE_COLD_V2_BUNDLE_CHUNK_MB: "1" }, () => migrateV5(dir))
      const chunks = await SessionColdV2.openRawDb(archive, "ro").then(async (db) => {
        try {
          return db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM bundle WHERE session_id = 's-big'`)?.n ?? 0
        } finally {
          db.close()
        }
      })
      expect(chunks).toBeGreaterThanOrEqual(2)
      const window = 5 + SessionColdV2.TAIL_OVERLAP
      const tail = await SessionColdV2.faultInSessions(archive, live, ["s-big"], { tailMessages: 5 })
      expect(tail.sessions).toBe(1)
      expect(tail.events).toBe(0)
      const db = await SessionColdV2.openRawDb(live, "ro")
      try {
        expect(db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM message WHERE session_id = 's-big'`)?.n).toBe(window)
        expect(db.get<{ complete: number }>(`SELECT complete FROM fault_state WHERE session_id = 's-big'`)?.complete).toBe(0)
      } finally {
        db.close()
      }
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-big"])).sessions).toBe(1)
      // Other sessions are still stubs: fault them fully, then the whole
      // live file must equal the origin byte-for-byte.
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-rand", "s-tiny", "s-sha", "s-shb"])).sessions).toBe(4)
      const { diffs, firsts } = await SessionColdV2.compareFiles(origin, live, null)
      expect(firsts).toEqual([])
      expect(diffs).toBe(0)
    } finally {
      await cleanup()
    }
  }, 300_000)

  test("chunk-mb 0 packs one chunk per session; settings parse", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const { archive } = await withEnv({ OPENCODE_COLD_V2_BUNDLE_CHUNK_MB: "0" }, () => migrateV5(dir))
      const db = await SessionColdV2.openRawDb(archive, "ro")
      try {
        const perSession = db.all<{ n: number }>(`SELECT COUNT(*) AS n FROM bundle GROUP BY session_id`)
        expect(perSession.length).toBeGreaterThan(0)
        for (const row of perSession) expect(row.n).toBe(1)
      } finally {
        db.close()
      }
    } finally {
      await cleanup()
    }
    await withEnv({ OPENCODE_COLD_V2_BUNDLE_CHUNK_MB: undefined, OPENCODE_COLD_V2_NO_BUNDLE: undefined }, async () => {
      expect(SessionColdV2.bundleSettings()).toEqual({ chunkBytes: 8 * 1024 * 1024, disabled: false })
    })
    await withEnv({ OPENCODE_COLD_V2_BUNDLE_CHUNK_MB: "2" }, async () => {
      expect(SessionColdV2.bundleSettings()).toEqual({ chunkBytes: 2 * 1024 * 1024, disabled: false })
    })
    await withEnv({ OPENCODE_COLD_V2_BUNDLE_CHUNK_MB: "0" }, async () => {
      expect(SessionColdV2.bundleSettings().chunkBytes).toBe(Number.POSITIVE_INFINITY)
    })
    await withEnv({ OPENCODE_COLD_V2_BUNDLE_CHUNK_MB: "abc" }, async () => {
      expect(SessionColdV2.bundleSettings().chunkBytes).toBe(8 * 1024 * 1024)
    })
    await withEnv({ OPENCODE_COLD_V2_NO_BUNDLE: "1" }, async () => {
      expect(SessionColdV2.bundleSettings().disabled).toBe(true)
    })
  }, 300_000)

  test("bundle tampering is loud in every direction", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const { live, archive } = await migrateV5(dir)
      const mutate = async (name: string, fn: (file: string) => Promise<void>): Promise<string> => {
        const file = join(dir, name)
        await copyFile(archive, file)
        await fn(file)
        return file
      }
      const mutateLive = async (name: string, fn: (db: SessionColdV2.RawDb) => void): Promise<string> =>
        mutate(name, async (file) => {
          const db = await SessionColdV2.openRawDb(file, "rw")
          try {
            fn(db)
          } finally {
            db.close()
          }
        })
      // Victim triple scoped to s-big: every fault below reads s-big only,
      // so the mutation must land in a chunk s-big actually touches.
      const victim = await SessionColdV2.openRawDb(archive, "ro").then(async (db) => {
        try {
          return db.get<{ id: string; chunk: number }>(
            `SELECT b.id AS id, b.chunk AS chunk FROM bptr b WHERE b.t = 'part' AND b.session_id = 's-big' LIMIT 1`,
          )
        } finally {
          db.close()
        }
      })
      expect(victim?.id).toBeTruthy()
      const victimPart = victim?.id ?? ""
      const victimChunk = victim?.chunk ?? 0
      // Flipped bundle byte trips the chunk digest on fault-in.
      await expect(
        SessionColdV2.faultInSessions(
          await mutate("f-byte.db", async (file) => {
            const db = await SessionColdV2.openRawDb(file, "rw")
            try {
              const row = db.get<{ bytes: unknown }>(`SELECT bytes FROM bundle WHERE session_id = 's-big' AND chunk = ?`, [victimChunk])
              const buf = Buffer.from(row?.bytes as Uint8Array)
              buf[10] = (buf[10] ?? 0) ^ 0xff
              db.run(`UPDATE bundle SET bytes = ? WHERE session_id = 's-big' AND chunk = ?`, [buf, victimChunk])
            } finally {
              db.close()
            }
          }),
          live,
          ["s-big"],
        ),
      ).rejects.toThrow(/digest mismatch/)
      // Corrupt index trips the index hash.
      await expect(
        SessionColdV2.faultInSessions(
          await mutateLive("f-index.db", (db) => db.run(`UPDATE bundle SET rows_json = '[]' WHERE session_id = 's-big' AND chunk = ?`, [victimChunk])),
          live,
          ["s-big"],
        ),
      ).rejects.toThrow(/index hash mismatch/)
      // Rebound registry sha trips the index-vs-registry check.
      await expect(
        SessionColdV2.faultInSessions(
          await mutateLive("f-ptr.db", (db) => db.run(`UPDATE ptr SET sha = ? WHERE id = ?`, ["0".repeat(64), victimPart])),
          live,
          ["s-big"],
        ),
      ).rejects.toThrow(/index sha != registry/)
      // Missing registry entry trips the stray check.
      await expect(
        SessionColdV2.faultInSessions(await mutateLive("f-noptr.db", (db) => db.run(`DELETE FROM ptr WHERE id = ?`, [victimPart])), live, ["s-big"]),
      ).rejects.toThrow(/no registry entry/)
      // Deleted bundle row orphans the location (verify's job).
      await expect(SessionColdV2.verifyArchive(await mutateLive("f-dangling.db", (db) => db.exec(`DELETE FROM bundle`)))).rejects.toThrow(
        /missing chunks|no bundle table|bundle hash/,
      )
      // Deleted location trips the location audit (verify's job; reads stay
      // correct via ref+index+registry, which still agree).
      await expect(SessionColdV2.verifyArchive(await mutateLive("f-noloc.db", (db) => db.run(`DELETE FROM bptr WHERE id = ?`, [victimPart])))).rejects.toThrow(
        /no bundle location/,
      )
    } finally {
      await cleanup()
    }
  }, 300_000)

  test("migrate-v5 converts EXACT with backup and no-ops after", async () => {
    const { dir, cleanup } = await scratch()
    try {
      // v4-shaped source: kill-switch forces fully classic output.
      const origin = await buildV5(dir, "opencode.db")
      const v4 = join(dir, "v4.db")
      await withEnv({ OPENCODE_COLD_V2_NO_BUNDLE: "1" }, async () => {
        await SessionColdV2.packArchiveFlow({ src: origin, dst: v4, allow: null, minBytes: 200, verify: false, treatAsLive: false })
      })
      expect(await metaOf(v4)).toMatchObject({ version: "4" })
      // Read-duality: v5 code faults and verifies the v4 archive as-is.
      // Empty the FULL heavy set (event_sequence/todo/etc. count toward
      // residency too — a true stub has none of them, as slim produces).
      const liveProbe = join(dir, "probe.db")
      await copyFile(origin, liveProbe)
      const slimProbe = await SessionColdV2.openRawDb(liveProbe, "rw")
      try {
        for (const table of ["message", "part", "event", "todo", "session_message", "session_input", "session_context_epoch", "event_sequence"]) {
          slimProbe.exec(`DELETE FROM "${table}"`)
        }
      } finally {
        slimProbe.close()
      }
      expect((await SessionColdV2.faultInSessions(v4, liveProbe, ["s-big"])).sessions).toBe(1)
      expect((await SessionColdV2.verifyArchive(v4)).bundles).toBe(0)
      // Migrate: EXACT (proven inside the engine), backup, version flip.
      const done = await SessionColdV2.migrateArchiveToV5({ archive: v4 })
      expect(done.migrated).toBe(true)
      expect(done.version).toBe("5")
      expect(done.bundledSessions).toBeGreaterThan(0)
      expect(await metaOf(v4)).toMatchObject({ version: "5" })
      expect(await metaOf(`${v4}.prev-v4`)).toMatchObject({ version: "4" })
      expect(done.backup).toBe(`${v4}.prev-v4`)
      // Restored live from the migrated archive equals the origin.
      const check = join(dir, "check.db")
      await copyFile(v4, check)
      await SessionColdV2.restoreFile(check, false)
      const { diffs, firsts } = await SessionColdV2.compareFiles(origin, check, null)
      expect(firsts).toEqual([])
      expect(diffs).toBe(0)
      // Second run no-ops; existing backup refuses a forced re-run.
      expect((await SessionColdV2.migrateArchiveToV5({ archive: v4 })).migrated).toBe(false)
      await expect(SessionColdV2.migrateArchiveToV5({ archive: v4, force: true })).rejects.toThrow(/backup exists/)
    } finally {
      await cleanup()
    }
  }, 300_000)

  test("fault_state never ships in packs, slims, or restores", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const { live, archive } = await migrateV5(dir)
      // Plant a marker row, then pack: the archive must not carry it.
      const plant = await SessionColdV2.openRawDb(live, "rw")
      try {
        plant.exec(`CREATE TABLE IF NOT EXISTS fault_state (session_id TEXT PRIMARY KEY, complete INTEGER NOT NULL)`)
        plant.run(`INSERT OR REPLACE INTO fault_state VALUES (?, ?)`, ["s-big", 0])
      } finally {
        plant.close()
      }
      const repacked = join(dir, "repacked.db")
      await SessionColdV2.packArchiveFlow({ src: live, dst: repacked, allow: null, minBytes: 200, verify: false, treatAsLive: false })
      const names = async (file: string): Promise<Set<string>> => {
        const db = await SessionColdV2.openRawDb(file, "ro")
        try {
          return new Set(db.all<{ name: string }>(`SELECT name FROM sqlite_master WHERE type = 'table'`).map((row) => row.name))
        } finally {
          db.close()
        }
      }
      expect(await names(repacked)).not.toContain("fault_state")
      // Slim restore drops it too.
      const slim = join(dir, "slim.db")
      await SessionColdV2.materializeLiveSlim({ archive: repacked, live: slim })
      expect(await names(slim)).not.toContain("fault_state")
      // Full restore drops bundles + markers alike.
      const full = join(dir, "full.db")
      await copyFile(repacked, full)
      await SessionColdV2.restoreFile(full, false)
      const fullNames = await names(full)
      expect(fullNames).not.toContain("fault_state")
      expect(fullNames).not.toContain("bundle")
      expect(fullNames).not.toContain("bptr")
    } finally {
      await cleanup()
    }
  }, 300_000)
})

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

describe("llm-busy deferral", () => {
  test("deferWhileBusy runs immediately when idle", async () => {
    const { deferWhileBusy } = await import("@/session/db-cold-v2-startup")
    const { LlmActivity } = await import("@/session/llm-activity")
    LlmActivity.llmStreamEnd("ses_defer_test")
    let ran = 0
    deferWhileBusy(async () => {
      ran += 1
    })
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(ran).toBe(1)
    expect(LlmActivity.anyLlmActive()).toBe(false)
  })

  test("deferWhileBusy waits while busy, then runs", async () => {
    const { deferWhileBusy } = await import("@/session/db-cold-v2-startup")
    const { LlmActivity } = await import("@/session/llm-activity")
    LlmActivity.llmStreamBegin("ses_defer_busy")
    try {
      let ran = 0
      deferWhileBusy(
        async () => {
          ran += 1
        },
        { deferMs: 10, maxDefers: 50 },
      )
      await new Promise((resolve) => setTimeout(resolve, 60))
      expect(ran).toBe(0)
      LlmActivity.llmStreamEnd("ses_defer_busy")
      await new Promise((resolve) => setTimeout(resolve, 60))
      expect(ran).toBe(1)
    } finally {
      LlmActivity.llmStreamEnd("ses_defer_busy")
    }
  })

  test("deferWhileBusy gives up after maxDefers and runs anyway", async () => {
    const { deferWhileBusy } = await import("@/session/db-cold-v2-startup")
    const { LlmActivity } = await import("@/session/llm-activity")
    LlmActivity.llmStreamBegin("ses_defer_marathon")
    try {
      let ran = 0
      deferWhileBusy(
        async () => {
          ran += 1
        },
        { deferMs: 10, maxDefers: 3 },
      )
      await new Promise((resolve) => setTimeout(resolve, 150))
      expect(ran).toBe(1)
    } finally {
      LlmActivity.llmStreamEnd("ses_defer_marathon")
    }
  })

  test("activity flag scopes to streams and clears", async () => {
    const { LlmActivity } = await import("@/session/llm-activity")
    expect(LlmActivity.anyLlmActive()).toBe(false)
    LlmActivity.llmStreamBegin("ses_a")
    LlmActivity.llmStreamBegin("ses_b")
    expect(LlmActivity.anyLlmActive()).toBe(true)
    LlmActivity.llmStreamEnd("ses_a")
    expect(LlmActivity.anyLlmActive()).toBe(true)
    LlmActivity.llmStreamEnd("ses_b")
    expect(LlmActivity.anyLlmActive()).toBe(false)
  })
})
