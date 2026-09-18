// In-process LLM activity tracker so background cold-storage work (archive
// completion, auto-evict) can defer while the user is waiting on model
// output. Deliberately a leaf module with zero imports: both the Effect
// world (llm.ts marks begin/end around stream consumption) and the plain
// promise world (db-cold-v2-startup reads anyActive) share it without
// import cycles. Keys are session id strings; entries live only for the
// duration of a stream (acquireRelease in the stream scope), so a crash or
// interrupt clears via scope finalization and nothing can wedge busy forever.
const active = new Set<string>()

export const llmStreamBegin = (sessionID: string): void => {
  active.add(sessionID)
}

export const llmStreamEnd = (sessionID: string): void => {
  active.delete(sessionID)
}

export const anyLlmActive = (): boolean => active.size > 0

export * as LlmActivity from "./llm-activity"
