import { expect, test } from "bun:test"
import { SessionCompaction } from "@opencode-ai/core/session/compaction"

test("compaction prompt preserves detailed work state and relevant files", () => {
  const prompt = SessionCompaction.buildPrompt({ context: ["conversation history"] })

  expect(prompt).toContain("## Work State\n### Completed")
  expect(prompt).toContain("### Active")
  expect(prompt).toContain("### Blocked")
  expect(prompt).toContain("## Relevant Files")
})

test("toggle defaults to hybrid", () => {
  const saved = process.env["OPENCODE_COMPACTION_UPSTREAM"]
  delete process.env["OPENCODE_COMPACTION_UPSTREAM"]
  try {
    expect(SessionCompaction.getCompactionMode()).toBe("hybrid")
    expect(SessionCompaction.isUpstreamCompaction()).toBe(false)
  } finally {
    if (saved !== undefined) process.env["OPENCODE_COMPACTION_UPSTREAM"] = saved
  }
})

test("explicit legacy opt-out", () => {
  const saved = process.env["OPENCODE_COMPACTION_UPSTREAM"]
  try {
    for (const value of ["0", "false", "legacy"]) {
      process.env["OPENCODE_COMPACTION_UPSTREAM"] = value
      expect(SessionCompaction.getCompactionMode()).toBe("legacy")
      expect(SessionCompaction.isUpstreamCompaction()).toBe(false)
    }
  } finally {
    if (saved !== undefined) process.env["OPENCODE_COMPACTION_UPSTREAM"] = saved
    else delete process.env["OPENCODE_COMPACTION_UPSTREAM"]
  }
})

test("upstream toggle accepts 1 and true", () => {
  const saved = process.env["OPENCODE_COMPACTION_UPSTREAM"]
  try {
    process.env["OPENCODE_COMPACTION_UPSTREAM"] = "1"
    expect(SessionCompaction.isUpstreamCompaction()).toBe(true)
    expect(SessionCompaction.getCompactionMode()).toBe("upstream")
    process.env["OPENCODE_COMPACTION_UPSTREAM"] = "true"
    expect(SessionCompaction.isUpstreamCompaction()).toBe(true)
    process.env["OPENCODE_COMPACTION_UPSTREAM"] = "0"
    expect(SessionCompaction.isUpstreamCompaction()).toBe(false)
  } finally {
    if (saved !== undefined) process.env["OPENCODE_COMPACTION_UPSTREAM"] = saved
    else delete process.env["OPENCODE_COMPACTION_UPSTREAM"]
  }
})

test("hybrid mode parses but keeps legacy core semantics", () => {
  const saved = process.env["OPENCODE_COMPACTION_UPSTREAM"]
  try {
    for (const value of ["2", "hybrid"]) {
      process.env["OPENCODE_COMPACTION_UPSTREAM"] = value
      expect(SessionCompaction.getCompactionMode()).toBe("hybrid")
      // Core auto-compact path is single-message either way: hybrid behaves
      // as legacy (no upstream prompt/select).
      expect(SessionCompaction.isUpstreamCompaction()).toBe(false)
      expect(SessionCompaction.buildPrompt({ context: ["x"] })).not.toContain("<conversation>")
    }
  } finally {
    if (saved !== undefined) process.env["OPENCODE_COMPACTION_UPSTREAM"] = saved
    else delete process.env["OPENCODE_COMPACTION_UPSTREAM"]
  }
})

test("upstream buildPrompt tags the conversation for small models", () => {
  const saved = process.env["OPENCODE_COMPACTION_UPSTREAM"]
  process.env["OPENCODE_COMPACTION_UPSTREAM"] = "1"
  try {
    const fresh = SessionCompaction.buildPrompt({ context: ["conversation history"] })
    expect(fresh).toStartWith("Here is the conversation so far:\n\n<conversation>\nconversation history\n</conversation>")
    expect(fresh.indexOf("</conversation>")).toBeLessThan(fresh.indexOf("Create a new anchored summary"))
    expect(fresh).toContain("conversation history in the <conversation> tags above")

    const update = SessionCompaction.buildPrompt({ context: ["new conversation"], previousSummary: "existing summary" })
    expect(update.indexOf("<conversation>")).toBeLessThan(update.indexOf("<prior-summary>"))
    expect(update.indexOf("</prior-summary>")).toBeLessThan(update.indexOf("The <prior-summary> summarizes"))
    expect(update).toContain(
      "Carry forward objectives, constraints, user directives, decisions, and parallel workstreams from the <prior-summary>",
    )
    expect(update).toContain('Move completed work from "Active" to "Completed".')
    expect(update).toContain('Update "Objective" and "Next Move" to reflect the current work state.')
  } finally {
    if (saved !== undefined) process.env["OPENCODE_COMPACTION_UPSTREAM"] = saved
    else delete process.env["OPENCODE_COMPACTION_UPSTREAM"]
  }
})

test("legacy buildPrompt is unchanged in legacy and hybrid modes", () => {
  const saved = process.env["OPENCODE_COMPACTION_UPSTREAM"]
  try {
    for (const value of [undefined, "0", "2"]) {
      if (value === undefined) delete process.env["OPENCODE_COMPACTION_UPSTREAM"]
      else process.env["OPENCODE_COMPACTION_UPSTREAM"] = value
      const prompt = SessionCompaction.buildPrompt({ context: ["conversation history"] })
      expect(prompt).toContain("Create a new anchored summary from the conversation history.")
      expect(prompt).not.toContain("<conversation>")
      const update = SessionCompaction.buildPrompt({ context: ["x"], previousSummary: "old" })
      expect(update).toContain("<previous-summary>")
      expect(update).not.toContain("<prior-summary>")
    }
  } finally {
    if (saved !== undefined) process.env["OPENCODE_COMPACTION_UPSTREAM"] = saved
    else delete process.env["OPENCODE_COMPACTION_UPSTREAM"]
  }
})

test("compaction describes tool media without embedding base64", () => {
  const base64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAAB"
  const serialized = SessionCompaction.serializeToolContent([
    { type: "text", text: "Image read successfully" },
    {
      type: "file",
      uri: `data:image/png;base64,${base64}`,
      mime: "image/png",
      name: "pixel.png",
    },
  ])

  expect(serialized).toBe("Image read successfully\n[Attached image/png: pixel.png]")
  expect(serialized).not.toContain(base64)
})
