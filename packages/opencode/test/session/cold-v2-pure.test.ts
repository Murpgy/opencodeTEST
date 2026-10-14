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
