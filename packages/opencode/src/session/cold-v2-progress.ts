// Structured progress, ETA and result screens for v2 cold-storage flows.
//
// The engine (`./cold-v2`) stays renderer-agnostic: it reports phase
// start/tick/end through the small `Progress` handle below, and this module
// decides how that surfaces — one auto-selected sink:
//
//   - OPENCODE_COLD_JSON_LOG=1 → one JSON object per event (pipelines)
//   - interactive TTY          → single-line live bar + persistent result panel
//   - otherwise (CI, pipes)    → throttled log lines (10% steps + boundaries)
//
// Workers (see `./cold-v2-workers`) never touch this module: they report
// per-chunk counts back to the main thread, which ticks the tracker. That
// keeps all console output on one thread (no interleaved garbage) and all
// rate math in one place.

export interface PhaseProgress {
  readonly phase: string
  readonly label: string
  readonly done: number
  readonly total: number | null
  readonly ratePerSec: number | null
  readonly etaSec: number | null
  readonly finished: boolean
}

export type ProgressSink = (progress: PhaseProgress) => void

export interface ProgressHandle {
  readonly start: (phase: string, label: string, total: number | null) => void
  readonly tick: (phase: string, count?: number) => void
  readonly end: (phase: string) => void
  /** Elapsed ms per finished phase, for the result panel. */
  readonly timings: () => Record<string, number>
}

const jsonLog = (): boolean => process.env["OPENCODE_COLD_JSON_LOG"] === "1"

const ciEnv = (): boolean => process.env["GITHUB_ACTIONS"] === "true" || process.env["CI"] === "true"

export const isInteractiveTerminal = (): boolean => {
  if (jsonLog() || ciEnv()) return false
  return !!process.stdout.isTTY && !!process.stdin.isTTY
}

const formatEta = (etaSec: number | null): string => {
  if (etaSec === null || !Number.isFinite(etaSec)) return "--:--"
  const total = Math.max(0, Math.round(etaSec))
  const minutes = Math.floor(total / 60)
  const seconds = total % 60
  return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`
}

const BAR_WIDTH = 24

const bar = (done: number, total: number | null): string => {
  if (total === null || total <= 0) return `[${"~".repeat(BAR_WIDTH)}]`
  const ratio = Math.min(1, Math.max(0, done / total))
  const filled = Math.round(ratio * BAR_WIDTH)
  return `[${"#".repeat(filled)}${"-".repeat(BAR_WIDTH - filled)}]`
}

// Null sink: progress disabled (e.g. --no-progress). The engine still tracks
// timings, so the result panel keeps its per-phase table.
export const nullSink = (): ProgressSink => () => {}

// Default sink: JSON lines when asked, live TTY bar when interactive,
// throttled log lines otherwise. Exactly one line style per environment.
export const makeProgressSink = (enabled = true): ProgressSink => {
  if (!enabled) return nullSink()
  if (jsonLog()) {
    return (progress) => {
      console.log(
        JSON.stringify({
          ts: new Date().toISOString(),
          event: "progress",
          phase: progress.phase,
          label: progress.label,
          done: progress.done,
          total: progress.total,
          ratePerSec: progress.ratePerSec,
          etaSec: progress.etaSec,
          finished: progress.finished,
        }),
      )
    }
  }
  if (isInteractiveTerminal()) {
    let lastDraw = 0
    return (progress) => {
      const now = Date.now()
      // Redraw at most 4Hz while running; always draw the finish line.
      if (!progress.finished && now - lastDraw < 250) return
      lastDraw = now
      const pct = progress.total ? ` ${Math.min(100, Math.round((progress.done / progress.total) * 100))}%` : ""
      const rate = progress.ratePerSec !== null ? ` ${Math.round(progress.ratePerSec)}/s` : ""
      const eta = progress.total && !progress.finished ? ` ETA ${formatEta(progress.etaSec)}` : ""
      const total = progress.total !== null ? `/${progress.total}` : ""
      const line = `${bar(progress.done, progress.total)} ${progress.label} ${progress.done}${total}${pct}${rate}${eta}`
      if (progress.finished) {
        process.stdout.write(`\r\x1b[2K${line} done\n`)
      } else {
        process.stdout.write(`\r\x1b[2K${line}`)
      }
    }
  }
  // Pipes and CI: one line per 10% step plus open/close, never a rewrite.
  const lastPct = new Map<string, number>()
  return (progress) => {
    if (progress.finished) {
      console.log(`${progress.label}: done (${progress.done}${progress.total !== null ? `/${progress.total}` : ""})`)
      lastPct.delete(progress.phase)
      return
    }
    if (progress.total === null || progress.total <= 0) return
    const pct = Math.floor((progress.done / progress.total) * 100)
    const prev = lastPct.get(progress.phase)
    if (prev !== undefined && pct < prev + 10) return
    lastPct.set(progress.phase, pct)
    console.log(
      `${progress.label}: ${progress.done}/${progress.total} (${pct}%)${progress.ratePerSec !== null ? ` ${Math.round(progress.ratePerSec)}/s` : ""}${progress.etaSec !== null ? ` ETA ${formatEta(progress.etaSec)}` : ""}`,
    )
  }
}

interface PhaseState {
  label: string
  total: number | null
  done: number
  startMs: number
  lastMs: number
  rate: number | null
  endedMs: number | null
}

// Engine-facing tracker. Cheap by design: tick() is integer math plus one
// sink call, so page loops can tick per page with no measurable overhead.
// Rate is an EWMA over inter-tick samples; ETA stays null until 3s of data
// so early estimates (ramp-up, tiny pages) never print nonsense.
export const createProgress = (sink: ProgressSink = makeProgressSink()): ProgressHandle => {
  const phases = new Map<string, PhaseState>()

  const emit = (phase: string, state: PhaseState, finished: boolean): void => {
    sink({
      phase,
      label: state.label,
      done: state.done,
      total: state.total,
      ratePerSec: state.rate,
      etaSec: state.total !== null && state.rate !== null && state.rate > 0 && !finished ? (state.total - state.done) / state.rate : null,
      finished,
    })
  }

  return {
    start: (phase, label, total) => {
      const now = Date.now()
      phases.set(phase, { label, total, done: 0, startMs: now, lastMs: now, rate: null, endedMs: null })
      emit(phase, phases.get(phase) as PhaseState, false)
    },
    tick: (phase, count = 1) => {
      const state = phases.get(phase)
      if (!state || state.endedMs !== null) return
      const now = Date.now()
      const dt = (now - state.lastMs) / 1000
      if (dt > 0.005) {
        const sample = count / dt
        state.rate = state.rate === null ? sample : state.rate * 0.7 + sample * 0.3
        state.lastMs = now
      }
      state.done += count
      // Suppress ETA during ramp-up: report rate only after 3s of samples.
      if (now - state.startMs < 3000) {
        const held = state.rate
        state.rate = null
        emit(phase, state, false)
        state.rate = held
        return
      }
      emit(phase, state, false)
    },
    end: (phase) => {
      const state = phases.get(phase)
      if (!state || state.endedMs !== null) return
      state.endedMs = Date.now()
      emit(phase, state, true)
    },
    timings: () => {
      const out: Record<string, number> = {}
      for (const [phase, state] of phases) {
        out[phase] = (state.endedMs ?? Date.now()) - state.startMs
      }
      return out
    },
  }
}

// ------------------------------------------------------------------ results
// Box-drawing panel, pure string (trivially testable, no TTY needed).
export const formatResultPanel = (title: string, rows: readonly (readonly [key: string, value: string])[]): string => {
  const width = Math.max(title.length, ...rows.map(([key, value]) => key.length + value.length + 3))
  const rule = `+${"-".repeat(width + 2)}+`
  const line = (text: string): string => `| ${text.padEnd(width)} |`
  return [rule, line(title), rule, ...rows.map(([key, value]) => line(`${key}: ${value}`)), rule].join("\n")
}

export interface WaitOptions {
  /** true forces the pause, false forbids it, undefined pauses only when interactive. */
  readonly wait?: boolean
}

// "Continue screen": pause on the result panel so a human running a
// multi-minute pack actually sees it. Never pauses unless BOTH stdio ends are
// TTYs (plus JSON/CI guards) — scripts, pipes and CI always flow through.
// --wait forces it (still TTY-gated: refusing to hang a pipe matters more
// than obeying the flag); --no-wait (yargs boolean negation) forbids it.
export const maybeWaitForContinue = async (opts: WaitOptions = {}): Promise<boolean> => {
  if (opts.wait === false) return false
  if (!isInteractiveTerminal()) {
    if (opts.wait === true) console.log("(not pausing: stdout is not an interactive terminal)")
    return false
  }
  const readline = await import("node:readline")
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout })
  try {
    await new Promise<void>((resolve) => {
      rl.question("Press Enter to continue…", () => resolve())
    })
    return true
  } finally {
    rl.close()
  }
}

export * as SessionColdV2Progress from "./cold-v2-progress"
