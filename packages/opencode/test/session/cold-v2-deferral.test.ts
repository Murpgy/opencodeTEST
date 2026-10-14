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

  test("concurrent streams on one session refcount", async () => {
    const { LlmActivity } = await import("@/session/llm-activity")
    LlmActivity.llmStreamBegin("ses_dup")
    LlmActivity.llmStreamBegin("ses_dup")
    LlmActivity.llmStreamEnd("ses_dup")
    expect(LlmActivity.anyLlmActive()).toBe(true)
    LlmActivity.llmStreamEnd("ses_dup")
    expect(LlmActivity.anyLlmActive()).toBe(false)
  })
})
