# Prisma's SlateDB fork

This is [Prisma Streams](https://github.com/prisma/streams)' fork of
[slatedb/slatedb](https://github.com/slatedb/slatedb). It exists so Streams can ship
fixes it depends on before upstream releases them. Every change here is meant to go
upstream; nothing in this fork is Prisma-only code.

## Branches and tags

| Ref | What it is |
|---|---|
| `main` | upstream `main` plus exactly one commit: this file and `fork/sync.sh` |
| `streams` | `main` plus the patches below, one commit per patch, each naming its upstream PR or issue |
| `streams-YYYYMMDD-<sha>` tags | every `streams` revision Streams has pinned (Streams' `Cargo.toml` pins one of these revs) |

Upstream PR creation is limited to collaborators, so new patches are proposed upstream as
issues that link to the commit on `streams`. Existing PRs stay open and link here.

## Patches carried

| # | Patch | Upstream | Why Streams needs it |
|---|---|---|---|
| 1 | Collect the last compacted L0 SST on a database with no live L0s | [PR #2190](https://github.com/slatedb/slatedb/pull/2190) | The compacted-SST GC never deletes the L0 consumed by the last compaction while a tree has no live L0s, so quiet shard logs (and the queue collector's passes) leave an unreferenced SST in object storage indefinitely. |
| 2 | Reject a submitted compaction that shares a source or destination with a live one | [PR #2191](https://github.com/slatedb/slatedb/pull/2191) | Streams' queue collector submits compactions through `Admin::submit_compaction`; one landing beside the scheduler's own job into the same sorted run panicked the compaction worker and crash-looped the shard. Streams also carries an equivalent guard in its own scheduler wrapper (`compaction_guard.rs`), which can go once this is merged. |
| 3 | Fail fencing init after bounded consecutive CAS conflicts | [PR #2010](https://github.com/slatedb/slatedb/pull/2010), issue [#1970](https://github.com/slatedb/slatedb/issues/1970) | An opener racing a live incumbent's write cadence loses every fencing round forever; Streams needs a typed failure to apply a holdoff instead of burning the whole timeout. |
| 4 | Fetch a range's uncached blocks in one GET | issue [#2211](https://github.com/slatedb/slatedb/issues/2211) (PR creation is limited to collaborators) | With a cache under pressure, one read-ahead window split into a GET per gap between cached blocks (measured 2/38/94 GETs for three identical scans; 1 each with the patch). Streams' telemetry scans hit this after the move to upstream `main`. |
| 5 | `Settings::point_lookup_parallelism` bounds the point-lookup lookahead | issue [#2213](https://github.com/slatedb/slatedb/issues/2213) (PR creation is limited to collaborators) | Since #2039 a point lookup initializes up to four older sources at once and drops them on a hit, but their reads finish as detached cache loads with no counted miss. Streams' telemetry DBs (many small L0s, recent keys) went from 1.00 to 1.39 GETs per miss, +11% cost per small tenant; Streams sets the setting to 1 for them. |
| 6 | An explicit WAL flush writes only the WALs frozen by its call | issue [#2215](https://github.com/slatedb/slatedb/issues/2215) (PR creation is limited to collaborators) | Since #2084 `flush()` freezes the current WAL and the flush task froze it again, so a write appended in between cost its own WAL object. Streams' group-commit pump measured 21-24 WAL PUTs per 20 flushes under CPU load (exactly 20 on v0.15). |

Merged upstream and therefore no longer carried: [#2193](https://github.com/slatedb/slatedb/pull/2193) (zstd 0.14), [#2208](https://github.com/slatedb/slatedb/pull/2208) (flush retention boundary), [#1964](https://github.com/slatedb/slatedb/pull/1964) (yield points).

## Bringing in upstream changes

Run `fork/sync.sh` (from `main`). It fetches upstream, rebases this branch's one docs commit
and then each `streams` patch onto upstream `main`, stops at the first conflict, and runs
SlateDB's lib tests. A patch that upstream merged is dropped automatically (its diff is empty
after the rebase); remove its row above in the same sync.

**Never move Streams' pin on SlateDB's tests alone.** Before Streams pins a new `streams`
revision, Streams measures it against the current pin on the same day, same machine:

1. the Streams commit gate (`scripts/gate.sh`) and every formal receipt re-recorded;
2. PACK-S-MUX-0200 and PACK-T-1000 (`bench/k2cost`), and the queue benchmark at 1x and 10x
   beside a co-tenant and at 100x alone (`bench/queues-cost`);
3. the new pin is adopted only if no figure is worse than the current pin's (cost per
   tenant-month, cost per million messages), outside run-to-run noise. A regression is
   attributed and fixed (here or in Streams) before the pin moves.

The 2026-10 move from the v0.15 pin to upstream `main` is the example: SlateDB's tests passed,
and Streams measured +12% cost per small tenant from telemetry cache behaviour on `main`; the
pin did not move until that was understood.
