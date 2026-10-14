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
