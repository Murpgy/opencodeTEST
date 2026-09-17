# Cold storage v5: per-session solid bundles

v4 packs every large payload as an independent zstd-9 blob (462k blobs on the
reference corpus, 3.13x). The corpus is dominated by near-duplicate streaming
deltas (1.5M `message.part.updated.1` events embedding near-full part payloads)
plus 0.79GB of fully uncompressed inline JSON. Measured on the real archive:

- per-part-chain solid: −35% vs individual blobs (covers only ~27% of rows)
- per-session solid: −51% (zstd-9), −57% (zstd-9+LDM)
- chunk sweep (400MB sample): 0.5MB→67MB, 2MB→61MB, 8MB→57MB, 32MB→56MB, whole→56/49MB

v5 keeps the v4 layout and adds one structure: session-unique payloads move
from N independent blobs into per-session solid zstd streams (8MB plaintext
chunks, LDM on). Cross-session-shared blobs stay in the exact-dedup global
store (blind per-session bundling would un-share them: 4.03GB attributed vs
1.41GB stored). Projected serving size 3.03GB → ~2.2GB (~27%), zero semantic
change. Vault export of dormant copies is out of scope.

## Layout (additive over v4)

- `bundle(session_id TEXT, chunk INTEGER, bytes BLOB, codec TEXT, len INTEGER,
  rows INTEGER, rows_json TEXT, rows_hash TEXT, digest TEXT,
  PRIMARY KEY(session_id, chunk))`. One row per ≤8MB plaintext chunk.
  `rows_json` = canonical JSON array of `{t, id, o, l, sha}` (chunk-local
  offsets). `rows_hash` = sha256(rows_json), `digest` = sha256(bytes).
  `codec` is `"zstd-9-ldm"` (LDM frames decompress with plain zstd;
  the name is honesty + allow-list gating, no special decode path).
- `bptr(t TEXT, id TEXT, session_id TEXT, chunk INTEGER, off INTEGER, ln INTEGER,
  PRIMARY KEY(t, id))`. Location only; identity stays in `ptr`.
- Row refs are uniform `{"_bd":["<session>",chunk,off,len]}` for parts AND
  events (replaces `{"_blob":sha}` / `{"_ev":...}` for bundled rows).
- Member bytes are FINAL live-layout bytes, resolved at pack with the same
  resolvers restore uses (template / raw-envelope / verbatim as appropriate).
  Resolution is therefore slice + sha-verify + verbatim everywhere — no
  template, slim, or envelope code in any bundle path, and the existing
  self-verify gate (restore + byte-compare, 0 diffs or nothing ships) proves
  byte-exactness across the simplification.
- `ptr` rows are written for EVERY bundled member with sha = sha256(final).
  (Ex-blob members get their sha REBOUND from the canonical digest — same
  linkage structure, different bytes.) Consequences, all intended:
  - `computeInlineHash` excludes them with zero changes (it skips ptr ids).
  - registry JOINs, bidirectional swap checks, and counts treat them uniformly.
  - `assertRegistryCounts` gains two shape checks: `_bd` rows without `ptr`,
    `_bd` rows without `bptr` (both fail loud). Missing `bptr` table (v4)
    skips the second via the existing no-such-table tolerance.
- `meta` gains `bundle_hash`, `count_bundle`, `count_bptr`. `manifestHashOf`
  covers all non-skipped keys, so the chain extends with no code change.
  `bundleHashOf` = sha256 over sorted
  `session_id \t chunk \t rows_hash \t digest \t len \t rows` lines
  (mirrors `ptrHashOf` order-independence).
- `PACKED_TABLES` gains `bundle`, `bptr`: slim/restore drop them, unpack drops
  them, `assertLiveLayout` refuses them as packed input. `fault_state`
  (live-only markers) is dropped explicitly in pack/slim/restore (it must
  never ship; pre-fix archives are healed by migrate/unpack).

## Version policy

- Write path emits `"5"` iff ≥1 bundle was written, else `"4"` (fully classic
  output stays old-reader compatible). `no-bundle` kill-switch forces classic.
- Read path accepts `"4"` and `"5"` (`READABLE_VERSIONS`); anything else fails
  loud. v4 archives read with an empty bundle set — every v4 code path works
  unchanged (fault-in, evict, verify, restore, merge-pack source).
- All `FORMAT_VERSION` gates (loadManifest, loadManifestLight, readArchive,
  packArchiveFlow dst check, packLiveToArchive complete check) accept both.
  The packLiveToArchive gate is load-bearing: a v4 archive must NOT read as
  "no complete archive", or a merge pack would direct-pack the slim live file
  and drop every stub payload (data loss class).

## Bundle build (packFile, after classic pack, before hashes/counts)

1. Sharing probe, per session (O(session), never O(archive)): the session's
   candidate shas are checked with a chunked GROUP BY — a sha bundles only
   when no OTHER session references it. Sessions with no other-session
   overlap therefore bundle at the cost of one indexed query, not an
   archive-wide refcount map.
2. Per session members, in id order (parts by id, then events by id; id order
   ≈ time order per the fork-cutoff precedent, so tails cluster in last chunks):
   - unique-blob members: ptr rows whose sha is referenced by this session only.
     Plaintext via batched blob fetch + decompress, then RESOLVED to final
     live bytes with the same resolvers restore uses (template / raw-envelope
     / verbatim as appropriate — corrupt input fails loud here, tmp unpublished).
   - inline members: session part/event rows absent from `ptr` (ALL of them —
     bundles absorb the uncompressed tail), used verbatim.
   - `ptr` sha is rebound to sha256(final) for bundled members (UPDATE for
     ex-blob, INSERT for ex-inline): same linkage structure, new bytes.
   - small tables (message/todo/…) are NEVER bundled (tiny, plain rows stay).
3. Floors: unique plaintext ≥ 8KB AND ≥ 2 members, else classic.
4. Chunk at 8MB plaintext (`OPENCODE_COLD_V2_BUNDLE_CHUNK_MB`, MB int,
   `0` = whole session). Compress each chunk zstd-9+LDM; LDM-reject fallback
   is plain zstd-9 with codec `"zstd-9"` (correctness first, recorded per row).
   Pack-side memory is O(largest session): one session's member plaintexts are
   held while its chunks compress (chunk frames themselves are O(chunk)).
5. Gate (the 20% rule, one compression pass): total `bundle_bytes ≤
   ⌊plaintext/4⌋`, i.e. must beat 4x (population individual average is 3.13x,
   measured session bundles ~7x; the bar rarely binds but fails closed).
   On fail: classic rows stand as packed, no cleanup needed.
6. On pass: INSERT bundle/bptr rows, INSERT `ptr` rows for inline members,
   UPDATE member data to `_bd` refs, DELETE orphaned blob rows by exact sha
   set (unique-only by construction; shared blobs untouched).
7. Hashes/counts/VACUUM/manifest run after, covering the final state.
   `OPENCODE_COLD_V2_NO_BUNDLE=1` skips the phase (writes v4 content).

## Resolution (restore, fault-in, evict, verify)

- Shape dispatch gains one branch (`_bd`); `_blob`/`_ev`/inline paths are
  byte-identical. Bundle fetch is chunk-batched per session
  (`WHERE session_id=? AND chunk IN (...)`), then slice + sha256 vs `ptr`
  (+ vs index entry, + ref-vs-index offset agreement). Every fetched chunk
  verifies digest + rows_hash first, closing the tamper loop for unread rows.
- Tail fault: refs already carry their chunk, so only intersecting (normally
  trailing) chunks are fetched. Full fault/completion: all chunks.
  `plainCache` stays scoped to fetched chunks.
- verify: per bundle row — digest, rows_hash, row count, full slice+sha sweep
  (offline cost ≈ blob re-hash); `bundle_hash` recompute + compare; orphan
  bptr / ptr-with-neither-blob-nor-bptr fail loud (bptr-table-missing = v4,
  blob-only invariant).
- Merge-pack: operates on live-layout images, untouched. Partial sessions
  overlay as before. Bundle/bptr never enter images (restore drops them).

## Migration v4 → v5 (`db migrate-v5`)

Archive-only, live untouched: lock archive → copy to tmp → `restoreFile`
(v4 read) → heal (drop `fault_state`) → `packFile` (v5 bundle opts) →
`markComplete` → `verifyArchive` → `compareFiles(v4image, v5image)` 0 diffs
(cross-format EXACT proof) → copy archive to `<archive>.prev-v4` (refuse if
it exists; crash-safe — a kill mid-migration leaves the old archive, never
a missing one) → atomic publish → rewrite sidecar.
Already-v5 is a no-op success. `lock held` surfaces as exit 3 (pack in
flight — retry after). Disk pre-flight mirrors merge-pack accounting.
Next successful merge-pack unlinks a stale `.prev-v4` (documented rotation).
Startup: complete v4 archives keep serving (read-duality) with a QUIET-aware
one-line note pointing at `migrate-v5`; never block, never auto-migrate
(multi-minute, disk-heavy — explicit command only).

## Tests (thorough)

- v5 round-trip EXACT over a mixed corpus: bundling session, random-bytes
  session (stays classic — incompressible fails the 4x bar deterministically),
  tiny session (floor), two sessions sharing one big blob (shared row survives
  in `blob`, both sessions still bundle their unique parts).
- bar/floor/version: single-random-session archive writes version `"4"`.
- multi-chunk: >8MB unique session → ≥2 chunks, LDM codec recorded; tail
  faults the window and marks partial; completion is EXACT vs source.
- tamper (each fails loud): flipped bundle byte, corrupt rows_json,
  bptr→missing chunk, deleted bundle row, ptr/bptr sha mismatch, `_bd`
  without ptr, `_bd` without bptr.
- migration: v4 fixture → migrate → 0 diffs v4image-vs-v5image, backup
  present, version 5, second run no-ops, backup-exists refuses, v5 code
  faults-in and verifies the pre-migration v4 archive.
- hygiene: no `fault_state`/`bundle`/`bptr` in restored images or slim lives;
  no `fault_state` in v5 archives.
- settings parsing (chunk MB, kill-switch) as unit tests.
