import { describe, expect, test } from "bun:test"
import { SessionCold } from "@/session/cold"

const meta = {
  id: "ses_test",
  parentID: undefined,
  title: "some work",
  timeArchived: undefined,
  timeUpdated: 1000,
}

describe("isColdCandidate", () => {
  test("archived sessions are always candidates", () => {
    const policy = SessionCold.defaultPolicy(2000)
    expect(SessionCold.isColdCandidate({ ...meta, timeArchived: 1500 }, policy)).toBe(true)
  })

  test("live sessions are skipped by default", () => {
    const policy = SessionCold.defaultPolicy(2000)
    expect(SessionCold.isColdCandidate(meta, policy)).toBe(false)
  })

  test("old forks qualify only with includeForks", () => {
    const old = Date.now() - 60 * 24 * 3600 * 1000
    const fork = { ...meta, parentID: "ses_parent", title: "work (fork #2)", timeUpdated: old }
    expect(SessionCold.isColdCandidate(fork, SessionCold.defaultPolicy(Date.now()))).toBe(false)
    expect(
      SessionCold.isColdCandidate(fork, { ...SessionCold.defaultPolicy(Date.now()), includeForks: true }),
    ).toBe(true)
  })

  test("recent forks do not qualify", () => {
    const policy = { ...SessionCold.defaultPolicy(Date.now()), includeForks: true }
    expect(SessionCold.isColdCandidate({ ...meta, parentID: "ses_parent", title: "work (fork #1)" }, policy)).toBe(
      false,
    )
  })

  test("fork-titled sessions without parent link qualify", () => {
    const old = Date.now() - 60 * 24 * 3600 * 1000
    const policy = { ...SessionCold.defaultPolicy(Date.now()), includeForks: true }
    expect(SessionCold.isColdCandidate({ ...meta, title: "work (fork #1)", timeUpdated: old }, policy)).toBe(true)
  })
})

describe("isIdle", () => {
  test("active sessions are never idle", () => {
    const policy = SessionCold.defaultPolicy(10_000)
    expect(SessionCold.isIdle({ ...meta, timeUpdated: 100 }, policy, new Set(["ses_test"]))).toBe(false)
  })

  test("recently updated sessions are not idle", () => {
    const policy = SessionCold.defaultPolicy(2000)
    expect(SessionCold.isIdle({ ...meta, timeUpdated: 1500 }, policy, new Set())).toBe(false)
  })

  test("stale sessions are idle", () => {
    const policy = { ...SessionCold.defaultPolicy(100_000), idleMs: 1000 }
    expect(SessionCold.isIdle(meta, policy, new Set())).toBe(true)
  })
})

describe("chunked", () => {
  test("splits evenly and keeps remainders", () => {
    expect(SessionCold.chunked([1, 2, 3, 4, 5], 2)).toEqual([[1, 2], [3, 4], [5]])
    expect(SessionCold.chunked([], 10)).toEqual([])
  })
})
