export * as Database from "./database"

import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { layer as sqliteLayer } from "#sqlite"
import { Context, Effect, Layer } from "effect"
import { Global } from "../global"
import { Flag } from "../flag/flag"
import { dirname, isAbsolute, join } from "path"
import { existsSync } from "node:fs"
import { DatabaseMigration } from "./migration"
import { InstallationChannel } from "../installation/version"
import { makeGlobalNode } from "../effect/app-node"

const makeDatabase = EffectDrizzleSqlite.makeWithDefaults()
type DatabaseShape = Effect.Success<typeof makeDatabase>

export interface Interface {
  db: DatabaseShape
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/storage/Database") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const db = yield* makeDatabase

    yield* db.run("PRAGMA journal_mode = WAL")
    yield* db.run("PRAGMA synchronous = NORMAL")
    yield* db.run("PRAGMA busy_timeout = 5000")
    yield* db.run("PRAGMA cache_size = -64000")
    yield* db.run("PRAGMA foreign_keys = ON")
    yield* db.run("PRAGMA wal_checkpoint(PASSIVE)")
    yield* DatabaseMigration.apply(db)

    return { db }
  }).pipe(Effect.orDie),
)

export function layerFromPath(filename: string) {
  return layer.pipe(Layer.provide(sqliteLayer({ filename })))
}

// After the v1 -> v2 migration the original file is frozen and live traffic
// moves to opencode-live-v2.db next to it. The frozen original is never
// opened again once the live file exists (see path()).
export const LIVE_V2_FILENAME = "opencode-live-v2.db"

export function liveV2PathFor(base: string): string {
  return join(dirname(base), LIVE_V2_FILENAME)
}

export function basePath() {
  if (Flag.OPENCODE_DB) {
    if (Flag.OPENCODE_DB === ":memory:" || isAbsolute(Flag.OPENCODE_DB)) return Flag.OPENCODE_DB
    return join(Global.Path.data, Flag.OPENCODE_DB)
  }
  if (
    ["latest", "beta", "prod"].includes(InstallationChannel) ||
    process.env.OPENCODE_DISABLE_CHANNEL_DB === "1" ||
    process.env.OPENCODE_DISABLE_CHANNEL_DB === "true"
  )
    return join(Global.Path.data, "opencode.db")
  return join(Global.Path.data, `opencode-${InstallationChannel.replace(/[^a-zA-Z0-9._-]/g, "-")}.db`)
}

export function path() {
  const base = basePath()
  if (base === ":memory:") return base
  try {
    if (existsSync(liveV2PathFor(base))) return liveV2PathFor(base)
  } catch {
    // Stat failure (permissions, exotic FS): serve from the base file.
  }
  return base
}

// The filename resolves at layer-build time (first use), not at import time,
// so a migration that materializes the live file during startup still
// redirects this same process: the middleware runs before any service builds.
export const node = makeGlobalNode({ service: Service, layer: Layer.suspend(() => layerFromPath(path())), deps: [] })
