import { expect, test } from "bun:test"
import { copyCommand } from "../src/clipboard"

test("prefers Wayland clipboard when available", () => {
  expect(copyCommand("linux", true, (name) => name === "wl-copy")).toEqual(["wl-copy"])
})

test("uses osascript on macOS", () => {
  expect(copyCommand("darwin", false, (name) => name === "osascript")).toEqual(["osascript"])
})

test("falls back through X11 clipboard commands", () => {
  expect(copyCommand("linux", true, (name) => name === "xclip")).toEqual(["xclip", "-selection", "clipboard"])
  expect(copyCommand("linux", false, (name) => name === "xsel")).toEqual(["xsel", "--clipboard", "--input"])
})

test("returns undefined when native clipboard is unavailable", () => {
  expect(copyCommand("linux", false, () => false)).toBeUndefined()
})

test("uses clip.exe on WSL without X11 tools", () => {
  expect(copyCommand("linux", false, (name) => name === "clip.exe", true)).toEqual(["clip.exe"])
})

test("prefers X11 tools over clip.exe on WSL with a display stack", () => {
  expect(
    copyCommand("linux", false, (name) => name === "xclip" || name === "clip.exe", true),
  ).toEqual(["xclip", "-selection", "clipboard"])
})

test("ignores clip.exe outside WSL", () => {
  expect(copyCommand("linux", false, (name) => name === "clip.exe", false)).toBeUndefined()
})
