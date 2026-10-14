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
      await syncSessionTimes(origin, live)
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
      await syncSessionTimes(origin, live)
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

  test("migrate-v5 with workers stays EXACT and agrees with the sync path", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const origin = await buildV5(dir, "opencode.db")
      const mkv4 = async (name: string): Promise<string> => {
        const v4 = join(dir, name)
        await withEnv({ OPENCODE_COLD_V2_NO_BUNDLE: "1" }, async () => {
          await SessionColdV2.packArchiveFlow({ src: origin, dst: v4, allow: null, minBytes: 200, verify: false, treatAsLive: false })
        })
        return v4
      }
      const vSync = await mkv4("v-sync.db")
      const vPar = await mkv4("v-par.db")
      const syncDone = await SessionColdV2.migrateArchiveToV5({ archive: vSync, jobs: 1 })
      const parDone = await SessionColdV2.migrateArchiveToV5({ archive: vPar, jobs: 2 })
      expect(syncDone.migrated).toBe(true)
      expect(parDone.migrated).toBe(true)
      expect(parDone.bundledSessions).toBe(syncDone.bundledSessions)
      expect(parDone.bundleChunks).toBe(syncDone.bundleChunks)
      expect(await metaOf(vPar)).toMatchObject({ version: "5" })
      // The two outputs resolve to identical finals (worker overlap changes
      // scheduling, never bytes).
      const { diffs, firsts } = await SessionColdV2.compareArchivesFinal(vSync, vPar)
      expect(firsts).toEqual([])
      expect(diffs).toBe(0)
      // And the worker-built archive is healthy end to end.
      expect((await SessionColdV2.verifyArchive(vPar)).bundles).toBeGreaterThan(0)
      const check = join(dir, "check.db")
      await copyFile(vPar, check)
      await SessionColdV2.restoreFile(check, false)
      const back = await SessionColdV2.compareFiles(origin, check, null)
      expect(back.firsts).toEqual([])
      expect(back.diffs).toBe(0)
    } finally {
      await cleanup()
    }
  }, 300_000)

  test("migrate-v5 with nothing to bundle is a no-op (no backup, stays v4)", async () => {
    const { dir, cleanup } = await scratch()
    try {
      // Tiny-only corpus: every row misses the floors, so packBundles finds
      // zero sessions and the archive must stay v4 — published as "v5" would
      // be a version lie, and spending the backup on it would be worse.
      const origin = join(dir, "opencode.db")
      const db = await SessionColdV2.openRawDb(origin, "rw")
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
        db.run(`INSERT INTO session VALUES (?, ?, ?, ?, ?, ?)`, ["s-tiny", null, "tiny", null, Date.now(), "proj-a"])
        db.run(`INSERT INTO message VALUES (?, ?, ?)`, ["m-tiny", "s-tiny", Date.now()])
        db.run(`INSERT INTO part VALUES (?, ?, ?, ?)`, ["p-tiny", "m-tiny", "s-tiny", JSON.stringify({ type: "text", text: "hi" })])
      } finally {
        db.close()
      }
      const v4 = join(dir, "v4.db")
      await withEnv({ OPENCODE_COLD_V2_NO_BUNDLE: "1" }, async () => {
        await SessionColdV2.packArchiveFlow({ src: origin, dst: v4, allow: null, minBytes: 200, verify: false, treatAsLive: false })
      })
      expect(await metaOf(v4)).toMatchObject({ version: "4" })
      const done = await SessionColdV2.migrateArchiveToV5({ archive: v4 })
      expect(done.migrated).toBe(false)
      expect(done.version).toBe("4")
      expect(done.backup).toBeNull()
      expect(await metaOf(v4)).toMatchObject({ version: "4" })
      const { access } = await import("node:fs/promises")
      await expect(access(`${v4}.prev-v4`)).rejects.toThrow()
    } finally {
      await cleanup()
    }
  }, 300_000)

  test("migrate-v5 --force re-chunks a v5 archive EXACT via the fallback path", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const origin = await buildV5(dir, "opencode.db")
      const v4 = join(dir, "v4.db")
      await withEnv({ OPENCODE_COLD_V2_NO_BUNDLE: "1" }, async () => {
        await SessionColdV2.packArchiveFlow({ src: origin, dst: v4, allow: null, minBytes: 200, verify: false, treatAsLive: false })
      })
      const first = await SessionColdV2.migrateArchiveToV5({ archive: v4 })
      expect(first.migrated).toBe(true)
      expect(await metaOf(v4)).toMatchObject({ version: "5" })
      // Move the safety net aside so the forced re-run is allowed, then
      // re-bundle whole-session chunks: exercises the restore+repack fallback
      // (in-place bundling refuses already-bundled input by design).
      const { rename } = await import("node:fs/promises")
      await rename(`${v4}.prev-v4`, `${v4}.prev-v4.kept`)
      const second = await withEnv({ OPENCODE_COLD_V2_BUNDLE_CHUNK_MB: "0" }, async () => {
        return SessionColdV2.migrateArchiveToV5({ archive: v4, force: true })
      })
      expect(second.migrated).toBe(true)
      expect(second.version).toBe("5")
      expect(await metaOf(v4)).toMatchObject({ version: "5" })
      // Still EXACT against the origin after the re-chunk.
      const check = join(dir, "check.db")
      await copyFile(v4, check)
      await SessionColdV2.restoreFile(check, false)
      const { diffs, firsts } = await SessionColdV2.compareFiles(origin, check, null)
      expect(firsts).toEqual([])
      expect(diffs).toBe(0)
    } finally {
      await cleanup()
    }
  }, 300_000)

  test("migrate-v5 compare covers duplicate session_input rows and part linkage", async () => {
    const { dir, cleanup } = await scratch()
    try {
      const origin = await buildV5(dir, "opencode.db")
      // session_input/session_context_epoch key on session_id (non-unique,
      // nullable): seed duplicates + NULLs so a keyset-paginated compare
      // would silently skip them. part linkage columns (message_id) must also
      // survive bundling byte-identical.
      const seed = await SessionColdV2.openRawDb(origin, "rw")
      try {
        for (let i = 0; i < 5; i += 1) seed.run(`INSERT INTO session_input VALUES (?)`, ["s-big"])
        seed.run(`INSERT INTO session_input VALUES (?)`, [null])
        for (let i = 0; i < 3; i += 1) seed.run(`INSERT INTO session_context_epoch VALUES (?)`, ["s-big"])
      } finally {
        seed.close()
      }
      const v4 = join(dir, "v4.db")
      await withEnv({ OPENCODE_COLD_V2_NO_BUNDLE: "1" }, async () => {
        await SessionColdV2.packArchiveFlow({ src: origin, dst: v4, allow: null, minBytes: 200, verify: false, treatAsLive: false })
      })
      const inputRows = async (file: string, table: string): Promise<number> => {
        const db = await SessionColdV2.openRawDb(file, "ro")
        try {
          return db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM "${table}"`)?.n ?? 0
        } finally {
          db.close()
        }
      }
      expect(await inputRows(v4, "session_input")).toBe(6)
      const done = await SessionColdV2.migrateArchiveToV5({ archive: v4 })
      expect(done.migrated).toBe(true)
      // The archive-to-archive gate itself sees every row (0 diffs proves the
      // duplicates compared, not skipped).
      const { diffs } = await SessionColdV2.compareArchivesFinal(`${v4}.prev-v4`, v4)
      expect(diffs).toBe(0)
      expect(await inputRows(v4, "session_input")).toBe(6)
      // Mutation check: flip one part linkage column in the migrated archive;
      // the gate must go loud (proves linkage columns are compared, not just
      // data finals).
      const tampered = join(dir, "tampered.db")
      await copyFile(v4, tampered)
      const mut = await SessionColdV2.openRawDb(tampered, "rw")
      try {
        mut.run(`UPDATE part SET message_id = ? WHERE id = ?`, ["mb-0000-typo", "pb-0000"])
      } finally {
        mut.close()
      }
      const bad = await SessionColdV2.compareArchivesFinal(`${v4}.prev-v4`, tampered)
      expect(bad.diffs).toBeGreaterThan(0)
      expect(bad.firsts.join("\n")).toMatch(/part/)
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
