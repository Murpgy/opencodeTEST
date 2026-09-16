import { describe, expect, test } from "bun:test"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { rm, copyFile, mkdir, readFile, stat, writeFile } from "node:fs/promises"
import { createHash } from "node:crypto"
import { SessionColdV2 } from "@/session/cold-v2"
import { SessionColdV2Progress } from "@/session/cold-v2-progress"
import { SessionColdV2Workers } from "@/session/cold-v2-workers"
import { archivePathFor, formatMigrationWarning, liveV2PathFor, maybeWarnColdV2Migration, migrationStatus } from "@/session/db-cold-v2-startup"

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

  test("auto-migrate packs the origin, materializes live and freezes the origin", async () => {
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

  test("deleted live file restores EXACT from the archive, origin stays out of it", async () => {
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
      // Origin untouched by the restore, live equals a fresh unpack.
      expect(await fingerprint(origin)).toBe(originBefore)
      const fresh = join(dir, "fresh.db")
      await copyFile(archive, fresh)
      await SessionColdV2.restoreFile(fresh, false)
      const { diffs, firsts } = await SessionColdV2.compareFiles(fresh, live, null)
      expect(firsts).toEqual([])
      expect(diffs).toBe(0)
    } finally {
      await cleanup()
    }
  }, 180_000)

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
