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
      // s-old is backdated to idle: fault-in bumps time_updated (freshness
      // against cross-process evict-mid-read), so without this every session
      // would read as just-touched and nothing would be evictable.
      const dirty = await SessionColdV2.openRawDb(live, "rw")
      try {
        dirty.run(`INSERT INTO message VALUES (?, ?)`, ["m-dirty-extra", "s-dirty"])
        dirty.run(`UPDATE session SET time_updated = ? WHERE id IN (?, ?)`, [Date.now() - 2 * 3600 * 1000, "s-old", "s-dirty"])
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
      // Fault-in bumps time_updated (freshness vs evict-mid-read): backdate
      // one victim so the capped sweep has an idle candidate.
      const backdate = await SessionColdV2.openRawDb(live2, "rw")
      try {
        backdate.run(`UPDATE session SET time_updated = ? WHERE id = ?`, [Date.now() - 2 * 3600 * 1000, "s-old"])
      } finally {
        backdate.close()
      }
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
      await syncSessionTimes(origin, live)
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
      // Reconcile, not blind adopt: a markerless session may be a truncated
      // tail that lost its marker, so the full open overlays the archive
      // (idempotent here — the session is genuinely whole) and re-marks
      // complete instead of serving residency as whole forever.
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-tail"])).sessions).toBe(1)
      expect((await tailCounts(live)).messages).toBe(TAIL_N)
      expect((await tailCounts(live)).marker).toBe(1)
      // A repeat open is now a no-op: the healed marker holds.
      expect((await SessionColdV2.faultInSessions(archive, live, ["s-tail"])).sessions).toBe(0)
      // Residency-based paths (auto-evict) still see the adopted session.
      // Backdate past the fault's freshness bump so the idle policy applies.
      const backdate = await SessionColdV2.openRawDb(live, "rw")
      try {
        backdate.run(`UPDATE session SET time_updated = ? WHERE id = ?`, [Date.now() - 2 * 3600 * 1000, "s-tail"])
      } finally {
        backdate.close()
      }
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
