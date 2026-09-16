import { execFile, spawn } from "node:child_process"
import { readFile, rm } from "node:fs/promises"
import { platform, release, tmpdir } from "node:os"
import path from "node:path"
import { promisify } from "node:util"

const exec = promisify(execFile)

function command(command: string, args: string[] = [], input?: string) {
  return new Promise<Buffer>((resolve, reject) => {
    const child = spawn(command, args, { stdio: [input === undefined ? "ignore" : "pipe", "pipe", "ignore"] })
    const output: Buffer[] = []
    child.on("error", reject)
    child.stdout?.on("data", (chunk: Buffer) => output.push(chunk))
    child.on("close", (code) => {
      if (code === 0) return resolve(Buffer.concat(output))
      reject(new Error(`${command} exited with code ${code}`))
    })
    if (input !== undefined) child.stdin?.end(input)
  })
}

function writeOsc52(text: string) {
  if (!process.stdout.isTTY) return
  const sequence = `\x1b]52;c;${Buffer.from(text).toString("base64")}\x07`
  process.stdout.write(process.env.TMUX || process.env.STY ? `\x1bPtmux;\x1b${sequence}\x1b\\` : sequence)
}

export async function read() {
  if (platform() === "darwin") {
    const file = path.join(tmpdir(), "opencode-clipboard.png")
    try {
      await exec("osascript", [
        "-e",
        'set imageData to the clipboard as "PNGf"',
        "-e",
        `set fileRef to open for access POSIX file "${file}" with write permission`,
        "-e",
        "set eof fileRef to 0",
        "-e",
        "write imageData to fileRef",
        "-e",
        "close access fileRef",
      ])
      return { data: (await readFile(file)).toString("base64"), mime: "image/png" }
    } catch {
      // Fall through to text clipboard.
    } finally {
      await rm(file, { force: true }).catch(() => {})
    }
  }

  if (platform() === "win32" || release().includes("WSL")) {
    const script =
      "Add-Type -AssemblyName System.Windows.Forms; $img = [System.Windows.Forms.Clipboard]::GetImage(); if ($img) { $ms = New-Object System.IO.MemoryStream; $img.Save($ms, [System.Drawing.Imaging.ImageFormat]::Png); [System.Convert]::ToBase64String($ms.ToArray()) }"
    const image = await command("powershell.exe", ["-NonInteractive", "-NoProfile", "-command", script]).catch(() =>
      Buffer.alloc(0),
    )
    if (image.length) return { data: image.toString().trim(), mime: "image/png" }
  }

  if (platform() === "linux") {
    const wayland = await command("wl-paste", ["-t", "image/png"]).catch(() => Buffer.alloc(0))
    if (wayland.length) return { data: wayland.toString("base64"), mime: "image/png" }
    const x11 = await command("xclip", ["-selection", "clipboard", "-t", "image/png", "-o"]).catch(() =>
      Buffer.alloc(0),
    )
    if (x11.length) return { data: x11.toString("base64"), mime: "image/png" }
  }

  // WSL text paste via interop, after the display-server paths so healthy
  // WSLg setups keep their fast path. Reaches the Windows clipboard even
  // when no X/Wayland user-space tools exist. powershell appends one
  // trailing CRLF, which the prompt paste path already normalizes.
  if (release().includes("WSL")) {
    const text = await command("powershell.exe", [
      "-NonInteractive",
      "-NoProfile",
      "-command",
      "Get-Clipboard -Raw",
    ]).catch(() => Buffer.alloc(0))
    if (text.length) return { data: text.toString().replace(/\r?\n$/, ""), mime: "text/plain" }
  }

  const { default: clipboardy } = await import("clipboardy")
  const text = await clipboardy.read().catch(() => undefined)
  if (text) return { data: text, mime: "text/plain" }
}

// stdin → clipboard with explicit UTF-8. Serves native win32 and WSL
// interop alike. clip.exe is deliberately NOT used: it decodes stdin per the
// console codepage and silently mangles non-ASCII while exiting 0, which no
// fallthrough can detect — silent corruption with a success toast.
const powershellSetClipboard: string[] = [
  "powershell.exe",
  "-NonInteractive",
  "-NoProfile",
  "-Command",
  "[Console]::InputEncoding = [System.Text.Encoding]::UTF8; Set-Clipboard -Value ([Console]::In.ReadToEnd())",
]
// Ordered copy backends, most preferred first. Existence (`has`) is only a
// fast-path filter: a present binary can still be broken (no Wayland socket,
// no X server, blocked interop), so the writer tries each candidate at
// runtime and falls through on failure instead of trusting `has`. Healthy
// native tools come first (instant when working, millisecond-fail when not);
// the interop backstop is always correct, so it sits last.
export function copyCommands(
  os: NodeJS.Platform,
  wayland: boolean,
  has: (name: string) => boolean,
  wsl = release().includes("WSL"),
): string[][] {
  if (os === "darwin") return has("osascript") ? [["osascript"]] : []
  const cmds: string[][] = []
  if (os === "linux") {
    if (wayland && has("wl-copy")) cmds.push(["wl-copy"])
    if (has("xclip")) cmds.push(["xclip", "-selection", "clipboard"])
    if (has("xsel")) cmds.push(["xsel", "--clipboard", "--input"])
    if (wsl && has("powershell.exe")) cmds.push(powershellSetClipboard)
    return cmds
  }
  if (os === "win32" && has("powershell.exe")) {
    return [powershellSetClipboard]
  }
  return cmds
}

export type CopyRunner = (cmd: readonly string[], input: string) => Promise<unknown>

export const osascriptArgs = (text: string): string[] => [
  "-e",
  `set the clipboard to "${text.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`,
]

// Tries each backend in order until one accepts the write. Returns true on
// the first success, false when every backend failed. The injectable runner
// exists for tests; production passes the real spawn below.
export async function copyWithFallback(
  text: string,
  candidates: readonly (readonly string[])[],
  run: CopyRunner = (cmd, input) =>
    cmd[0] === "osascript" ? command("osascript", osascriptArgs(input)) : command(cmd[0] ?? "", cmd.slice(1), input),
): Promise<boolean> {
  for (const cmd of candidates) {
    try {
      await run(cmd, text)
      return true
    } catch {
      // Backend present but broken — next one instead of failing silent.
    }
  }
  return false
}

let copyMethod: Promise<(text: string) => Promise<boolean>> | undefined

function getCopyMethod() {
  return (copyMethod ??= (async () => {
    const { which } = await import("@opencode-ai/core/util/which")
    const candidates = copyCommands(platform(), Boolean(process.env.WAYLAND_DISPLAY), (name) => Boolean(which(name)))
    return async (text: string) => {
      if (await copyWithFallback(text, candidates)) return true
      const { default: clipboardy } = await import("clipboardy")
      return clipboardy.write(text).then(() => true, () => false)
    }
  })())
}

export async function write(text: string): Promise<boolean> {
  writeOsc52(text)
  const method = await getCopyMethod()
  return method(text)
}
