import type { Argv } from "yargs"
import { Effect } from "effect"
import { cmd } from "./cmd"
import { effectCmd, fail } from "../effect-cmd"
import { Session } from "@/session/session"
import { MessageID, SessionID } from "../../session/schema"
import type { SessionV1 } from "@opencode-ai/core/v1/session"
import { UI } from "../ui"
import { Locale } from "@/util/locale"
import { Flag } from "@opencode-ai/core/flag/flag"
import { Filesystem } from "@/util/filesystem"
import { Process } from "@/util/process"
import { NotFoundError } from "@/storage/storage"
import { EOL } from "os"
import path from "path"
import { which } from "@opencode-ai/core/util/which"

function pagerCmd(): string[] {
  const lessOptions = ["-R", "-S"]
  if (process.platform !== "win32") {
    return ["less", ...lessOptions]
  }

  // user could have less installed via other options
  const lessOnPath = which("less")
  if (lessOnPath) {
    if (Filesystem.stat(lessOnPath)?.size) return [lessOnPath, ...lessOptions]
  }

  if (Flag.OPENCODE_GIT_BASH_PATH) {
    const less = path.join(Flag.OPENCODE_GIT_BASH_PATH, "..", "..", "usr", "bin", "less.exe")
    if (Filesystem.stat(less)?.size) return [less, ...lessOptions]
  }

  const git = which("git")
  if (git) {
    const less = path.join(git, "..", "..", "usr", "bin", "less.exe")
    if (Filesystem.stat(less)?.size) return [less, ...lessOptions]
  }

  // Fall back to Windows built-in more (via cmd.exe)
  return ["cmd", "/c", "more"]
}

export const SessionCommand = cmd({
  command: "session",
  describe: "manage sessions",
  builder: (yargs: Argv) => yargs.command(SessionListCommand).command(SessionDeleteCommand).command(SessionForkCommand).demandCommand(),
  async handler() {},
})

export const SessionDeleteCommand = effectCmd({
  command: "delete <sessionID>",
  describe: "delete a session",
  builder: (yargs) =>
    yargs.positional("sessionID", {
      describe: "session ID to delete",
      type: "string",
      demandOption: true,
    }),
  handler: Effect.fn("Cli.session.delete")(function* (args) {
    const svc = yield* Session.Service
    const sessionID = SessionID.make(args.sessionID)
    yield* svc
      .remove(sessionID)
      .pipe(Effect.catchIf(NotFoundError.isInstance, () => fail(`Session not found: ${args.sessionID}`)))
    UI.println(UI.Style.TEXT_SUCCESS_BOLD + `Session ${args.sessionID} deleted` + UI.Style.TEXT_NORMAL)
  }),
})

export const SessionListCommand = effectCmd({
  command: "list",
  describe: "list sessions",
  builder: (yargs) =>
    yargs
      .option("max-count", {
        alias: "n",
        describe: "limit to N most recent sessions",
        type: "number",
      })
      .option("format", {
        describe: "output format",
        type: "string",
        choices: ["table", "json"],
        default: "table",
      }),
  handler: Effect.fn("Cli.session.list")(function* (args) {
    const sessions = yield* Session.Service.use((svc) => svc.list({ roots: true, limit: args.maxCount }))

    if (sessions.length === 0) return

    const output = args.format === "json" ? formatSessionJSON(sessions) : formatSessionTable(sessions)

    const shouldPaginate = process.stdout.isTTY && !args.maxCount && args.format === "table"

    if (shouldPaginate) {
      yield* Effect.promise(async () => {
        const proc = Process.spawn(pagerCmd(), {
          stdin: "pipe",
          stdout: "inherit",
          stderr: "inherit",
        })

        if (!proc.stdin) {
          console.log(output)
          return
        }

        proc.stdin.write(output)
        proc.stdin.end()
        await proc.exited
      })
    } else {
      console.log(output)
    }
  }),
})

function formatSessionTable(sessions: Session.Info[]): string {
  const lines: string[] = []

  const maxIdWidth = Math.max(20, ...sessions.map((s) => s.id.length))
  const maxTitleWidth = Math.max(25, ...sessions.map((s) => s.title.length))

  const header = `Session ID${" ".repeat(maxIdWidth - 10)}  Title${" ".repeat(maxTitleWidth - 5)}  Updated`
  lines.push(header)
  lines.push("─".repeat(header.length))
  for (const session of sessions) {
    const truncatedTitle = Locale.truncate(session.title, maxTitleWidth)
    const timeStr = Locale.todayTimeOrDateTime(session.time.updated)
    const line = `${session.id.padEnd(maxIdWidth)}  ${truncatedTitle.padEnd(maxTitleWidth)}  ${timeStr}`
    lines.push(line)
  }

  return lines.join(EOL)
}

function formatSessionJSON(sessions: Session.Info[]): string {
  const jsonData = sessions.map((session) => ({
    id: session.id,
    title: session.title,
    updated: session.time.updated,
    created: session.time.created,
    projectId: session.projectID,
    directory: session.directory,
  }))
  return JSON.stringify(jsonData, null, 2)
}

// ForkFar: fork from any point in the FULL history. The TUI fork picker only
// lists the visible window (last ~100 messages), so far-back fork points are
// unreachable there. This command faults the whole session in, numbers every
// user message oldest-first (--list to browse), and forks starting at the
// chosen one (--from), inclusive: the new session opens with that message.
export const SessionForkCommand = effectCmd({
  command: "fork <sessionID>",
  describe: "fork a session, optionally starting at any user message in its full history (see --list)",
  builder: (yargs) =>
    yargs
      .positional("sessionID", {
        describe: "session ID to fork",
        type: "string",
        demandOption: true,
      })
      .option("from", {
        describe: "user-message number from --list (1-based, fork starts here, inclusive) or message ID",
        type: "string",
      })
      .option("list", {
        describe: "list user messages across the full history with fork numbers instead of forking",
        type: "boolean",
        default: false,
      })
      .option("limit", {
        describe: "with --list: show only the newest N user messages (numbers stay global)",
        type: "number",
      }),
  handler: Effect.fn("Cli.session.fork")(function* (args: { sessionID: string; from?: string; list: boolean; limit?: number }) {
    const svc = yield* Session.Service
    const sessionID = SessionID.make(args.sessionID)
    const all = yield* svc.messages({ sessionID }).pipe(Effect.catchTag("NotFoundError", () => fail(`Session not found: ${args.sessionID}`)))
    const users = all.filter((msg) => msg.info.role === "user")
    if (args.list) {
      const shown = args.limit === undefined ? users : users.slice(-args.limit)
      const offset = users.length - shown.length
      const lines = [`User messages in ${args.sessionID} (${users.length} total, oldest first):`]
      shown.forEach((msg, i) => {
        lines.push(`${offset + i + 1}. [${Locale.todayTimeOrDateTime(msg.info.time.created)}] ${msg.info.id}: ${messagePreview(msg)}`)
      })
      const output = lines.join(EOL)
      if (process.stdout.isTTY && args.limit === undefined) {
        yield* Effect.promise(async () => {
          const proc = Process.spawn(pagerCmd(), { stdin: "pipe", stdout: "inherit", stderr: "inherit" })
          if (!proc.stdin) {
            console.log(output)
            return
          }
          proc.stdin.write(output)
          proc.stdin.end()
          await proc.exited
        })
      } else {
        console.log(output)
      }
      return
    }
    let messageID: MessageID | undefined
    let label = "full session"
    if (args.from !== undefined) {
      const point = resolveForkPoint(users, all, args.from)
      if (!point) return yield* fail(`Cannot fork from "${args.from}": ${users.length} user message(s) in ${args.sessionID} (--list to browse)`)
      messageID = point.cutoff
      label = `from user message ${point.ordinal} (${point.id})`
    }
    const forked = yield* svc.fork({ sessionID, messageID }).pipe(Effect.catchTag("NotFoundError", () => fail(`Session not found: ${args.sessionID}`)))
    UI.println(UI.Style.TEXT_SUCCESS_BOLD + `Forked ${forked.id} "${forked.title}" (${label})` + UI.Style.TEXT_NORMAL)
  }),
})

export interface ForkPoint {
  readonly ordinal: number
  readonly id: MessageID
  // Exclusive API cutoff: the message AFTER the inclusive start point (or
  // undefined when the start point is the last message, i.e. a full fork).
  readonly cutoff: MessageID | undefined
}

// Resolve a --from value (1-based user-message ordinal or message ID) to the
// inclusive start point plus the exclusive cutoff fork() needs. Pure:
// unit-tested without a session backend.
export const resolveForkPoint = (
  users: readonly { info: { id: MessageID } }[],
  all: readonly { info: { id: MessageID } }[],
  from: string,
): ForkPoint | undefined => {
  let index: number
  if (/^\d+$/.test(from)) {
    const ordinal = Number(from)
    if (!Number.isSafeInteger(ordinal) || ordinal < 1 || ordinal > users.length) return undefined
    index = all.findIndex((msg) => msg.info.id === users[ordinal - 1]?.info.id)
    if (index === -1) return undefined
    const id = users[ordinal - 1]?.info.id
    if (!id) return undefined
    return { ordinal, id, cutoff: messageIdAt(all, index + 1) }
  }
  if (!from.startsWith("msg")) return undefined
  index = all.findIndex((msg) => msg.info.id === from)
  if (index === -1) return undefined
  const ordinal = users.findIndex((msg) => msg.info.id === from) + 1
  return { ordinal, id: from as MessageID, cutoff: messageIdAt(all, index + 1) }
}

const messageIdAt = (all: readonly { info: { id: MessageID } }[], index: number): MessageID | undefined =>
  index < all.length ? all[index]?.info.id : undefined

const messagePreview = (msg: SessionV1.WithParts): string => {
  for (const part of msg.parts) {
    if (part.type === "text" && typeof (part as { text?: unknown }).text === "string") {
      const text = ((part as { text: string }).text).replace(/\s+/g, " ").trim()
      return text.length > 80 ? `${text.slice(0, 80)}…` : text
    }
  }
  const first = msg.parts[0]
  return first ? `<${first.type} part>` : "<no parts>"
}
