import { describe, expect, test } from "bun:test"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { rm, copyFile, mkdir } from "node:fs/promises"
import { SessionColdV2 } from "@/session/cold-v2"

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
      db.run(`INSERT INTO event VALUES (?, ?, ?, ?, ?)`, ["e2", "s1", 2, "message.updated.1", J({ note: "non-pu1" })])
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
})
