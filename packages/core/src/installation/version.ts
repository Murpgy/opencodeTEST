declare global {
  const OPENCODE_VERSION: string
  const OPENCODE_CHANNEL: string
}

// Escape hatch for forks pinned to an older base: Console gates some
// server-side features (e.g. free tier requires >= 1.18.0) on the version
// token of the `User-Agent: opencode/<version>` request header, which is
// built from InstallationVersion below. Setting OPENCODE_VERSION_OVERRIDE
// (e.g. `OPENCODE_VERSION_OVERRIDE=1.18.31`) makes this build report that
// version on the wire and in `--version` without touching the build.
// Unsupported: the running code is still whatever is checked out here.
const override = process.env["OPENCODE_VERSION_OVERRIDE"]?.trim() || undefined

export const InstallationVersion =
  override ?? (typeof OPENCODE_VERSION === "string" ? OPENCODE_VERSION : "local")
export const InstallationChannel = typeof OPENCODE_CHANNEL === "string" ? OPENCODE_CHANNEL : "local"
export const InstallationLocal = InstallationChannel === "local"
