import { expect, test } from "bun:test"
import { copyCommands, copyWithFallback, osascriptArgs } from "../src/clipboard"

test("prefers Wayland clipboard when available", () => {
  expect(copyCommands("linux", true, (name) => name === "wl-copy")).toEqual([["wl-copy"]])
})

test("uses osascript on macOS", () => {
  expect(copyCommands("darwin", false, (name) => name === "osascript")).toEqual([["osascript"]])
  expect(copyCommands("darwin", false, () => false)).toEqual([])
})

test("falls back through X11 clipboard commands", () => {
  expect(copyCommands("linux", true, (name) => name === "xclip")).toEqual([["xclip", "-selection", "clipboard"]])
  expect(copyCommands("linux", false, (name) => name === "xsel")).toEqual([["xsel", "--clipboard", "--input"]])
})

test("returns no candidates when native clipboard is unavailable", () => {
  expect(copyCommands("linux", false, () => false)).toEqual([])
})

test("prefers interop clip.exe on WSL ahead of X11 tools", () => {
  const has = (name: string) => name === "clip.exe" || name === "xclip" || name === "xsel"
  expect(copyCommands("linux", false, has, true)).toEqual([
    ["clip.exe"],
    ["xclip", "-selection", "clipboard"],
    ["xsel", "--clipboard", "--input"],
  ])
})

test("keeps wl-copy first on WSL with a Wayland stack", () => {
  const has = (name: string) => name === "wl-copy" || name === "clip.exe"
  expect(copyCommands("linux", true, has, true)).toEqual([["wl-copy"], ["clip.exe"]])
})

test("ignores clip.exe outside WSL", () => {
  expect(copyCommands("linux", false, (name) => name === "clip.exe", false)).toEqual([])
})

test("fallthrough tries every backend until one succeeds", async () => {
  const calls: string[] = []
  const ok = await copyWithFallback("hi", [["broken"], ["clip.exe"]], async (cmd) => {
    calls.push(cmd[0] as string)
    if (cmd[0] === "broken") throw new Error("no server")
  })
  expect(ok).toBe(true)
  expect(calls).toEqual(["broken", "clip.exe"])
})

test("fallthrough reports false when every backend fails", async () => {
  const ok = await copyWithFallback(
    "hi",
    [["broken-a"], ["broken-b"]],
    async () => {
      throw new Error("nope")
    },
  )
  expect(ok).toBe(false)
})

test("osascript escapes backslashes and quotes", () => {
  expect(osascriptArgs('say "hi" \\ bye')).toEqual(["-e", 'set the clipboard to "say \\"hi\\" \\\\ bye"'])
})

test("fallthrough passes the raw text to each backend", async () => {
  const seen: string[] = []
  const ok = await copyWithFallback("hi", [["broken"], ["clip.exe"]], async (cmd, input) => {
    seen.push(`${cmd[0]}:${input}`)
    if (cmd[0] === "broken") throw new Error("no server")
  })
  expect(ok).toBe(true)
  expect(seen).toEqual(["broken:hi", "clip.exe:hi"])
})
