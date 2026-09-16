import { describe, expect } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { Effect } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { RelativePath } from "@opencode-ai/core/schema"
import { tmpdir } from "./fixture/tmpdir"
import { testEffect } from "./lib/effect"

const it = testEffect(LayerNode.compile(Ripgrep.node))

// 120s budget: the first test in each file cold-bootstraps the rg binary on
// machines without a system install (download + extract on Windows CI takes
// well over bun's 5s default on loaded runners; the kill used to surface as
// a SIGTERM-mid-extract Expand-Archive failure). A genuine hang still fails,
// just after the bootstrap window instead of inside it.
const COLD_BOOTSTRAP_TIMEOUT = 120_000

describe("Ripgrep", () => {
  it.live(
    "keeps ignored files out of catch-all find results",
    () =>
      Effect.acquireUseRelease(
        Effect.promise(() => tmpdir()),
        (tmp) =>
          Effect.gen(function* () {
            yield* Effect.promise(() => fs.mkdir(path.join(tmp.path, "node_modules", "pkg"), { recursive: true }))
            yield* Effect.promise(() => fs.mkdir(path.join(tmp.path, "src")))
            yield* Effect.promise(() => Bun.$`git init -q ${tmp.path}`)
            yield* Effect.promise(() => fs.writeFile(path.join(tmp.path, ".gitignore"), "node_modules/\n"))
            yield* Effect.promise(() => fs.writeFile(path.join(tmp.path, "node_modules", "pkg", "index.js"), "ignored\n"))
            yield* Effect.promise(() => fs.writeFile(path.join(tmp.path, "src", "index.js"), "included\n"))

            const files = yield* (yield* Ripgrep.Service).find({ cwd: tmp.path, pattern: "*", limit: 10 })
            expect(files.map((item) => item.path)).toContain(RelativePath.make("src/index.js"))
            expect(files.map((item) => item.path)).not.toContain(RelativePath.make("node_modules/pkg/index.js"))
          }),
        (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
      ),
    COLD_BOOTSTRAP_TIMEOUT,
  )

  it.live(
    "never includes git metadata",
    () =>
      Effect.acquireUseRelease(
        Effect.promise(() => tmpdir()),
        (tmp) =>
          Effect.gen(function* () {
            yield* Effect.promise(() => fs.mkdir(path.join(tmp.path, ".opencode")))
            yield* Effect.promise(() => fs.writeFile(path.join(tmp.path, ".opencode", "config"), "needle\n"))
            yield* Effect.promise(() => fs.mkdir(path.join(tmp.path, ".git")))
            yield* Effect.promise(() => fs.writeFile(path.join(tmp.path, ".git", "config"), "needle\n"))
            const ripgrep = yield* Ripgrep.Service

            const files = yield* ripgrep.find({ cwd: tmp.path, pattern: "**/*", limit: 10 })
            expect(files.map((item) => item.path)).toContain(RelativePath.make(".opencode/config"))
            expect(files.map((item) => item.path)).not.toContain(RelativePath.make(".git/config"))

            const observed: string[] = []
            const limited = yield* ripgrep.find({
              cwd: tmp.path,
              pattern: "**/*",
              limit: 1,
              onEntry: (entry) => Effect.sync(() => observed.push(entry.path)),
            })
            expect(observed).toEqual(limited.map((item) => item.path))

            const matches = yield* ripgrep.grep({ cwd: tmp.path, pattern: "needle", include: "config", limit: 10 })
            expect(matches.map((item) => item.entry.path)).toContain(RelativePath.make(".opencode/config"))
            expect(matches.map((item) => item.entry.path)).not.toContain(RelativePath.make(".git/config"))
          }),
        (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
      ),
    COLD_BOOTSTRAP_TIMEOUT,
  )
})
