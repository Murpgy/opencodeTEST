// In-process LLM activity tracker so background cold-storage work (archive
// completion, auto-evict) can defer while the user is waiting on model
// output. Deliberately a leaf module with zero imports: both the Effect
// world (llm.ts marks begin/end around stream consumption) and the plain
// promise world (db-cold-v2-startup reads anyActive) share it without
// import cycles. Refcounted per session: concurrent streams (main + title +
// compaction) each hold one count, so the first finisher can't clear while a
// sibling still streams. Entries live only for the duration of a stream
// (acquireRelease in the stream scope), so a crash or interrupt clears via
// scope finalization and nothing can wedge busy forever.
const active = new Map<string, number>()

export const llmStreamBegin = (sessionID: string): void => {
  active.set(sessionID, (active.get(sessionID) ?? 0) + 1)
}

export const llmStreamEnd = (sessionID: string): void => {
  const left = (active.get(sessionID) ?? 1) - 1
  if (left <= 0) active.delete(sessionID)
  else active.set(sessionID, left)
}

export const anyLlmActive = (): boolean => active.size > 0

export * as LlmActivity from "./llm-activity"
