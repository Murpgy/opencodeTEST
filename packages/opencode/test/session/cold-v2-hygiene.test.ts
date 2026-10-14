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
