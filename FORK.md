# Prisma's SlateDB fork

This is [Prisma Streams](https://github.com/prisma/streams)' fork of
[slatedb/slatedb](https://github.com/slatedb/slatedb). It exists so Streams can ship
fixes it depends on before upstream releases them. Every change here is meant to go
upstream; nothing in this fork is Prisma-only code.

## Branches and tags

| Ref | What it is |
|---|---|
| `main` | upstream `main` plus the fork's docs commits: this file and `fork/sync.sh` |
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
| 7 | A clone can hold initial rows as its own L0 table from the moment it is initialized (`CloneBuilder::with_initial_rows`) | issue [#2239](https://github.com/slatedb/slatedb/issues/2239) (PR creation is limited to collaborators) | Streams splits a shard by projected clones of a checkpoint and seeds each child with its own bookkeeping rows at initialization, instead of opening each child as a writer inside the split's pause (about 24 sequential round trips of a ~50-round-trip pause). |
| 8 | A fresh clone writes its final checkpoint on its source through the manifest it already loaded | issue [#2240](https://github.com/slatedb/slatedb/issues/2240) (PR creation is limited to collaborators) | About three sequential round trips per clone inside a split's pause, and more in a merge, whose union clone writes a final checkpoint per source. |

Merged upstream and therefore no longer carried: [#2193](https://github.com/slatedb/slatedb/pull/2193) (zstd 0.14), [#2208](https://github.com/slatedb/slatedb/pull/2208) (flush retention boundary), [#1964](https://github.com/slatedb/slatedb/pull/1964) (yield points).

## Known issues (not patched)

Behaviour Streams works around rather than patches. Upstream issues cannot be opened from this fork
(issues are disabled here); each entry names the Streams test that reproduces it, so a pin that
changes the behaviour shows up as that test failing.

| # | Issue | Reproduced by | Streams' workaround |
|---|---|---|---|
| K1 | `create_checkpoint(CheckpointScope::All)` taken while writes are still buffered in the WAL writer leaves a data WAL above the checkpoint's replay point, so every projected (or union) clone of that checkpoint is refused (`InvalidCloneSourceWithWal`) until newer writes are flushed through a memtable. `begin_batch_writer_flush(true)` issues the WAL flush, then freezes the memtable stamped with `wal_observer.status().last_flushed_wal_id` from before that flush completes; the buffered writes land in the next WAL file while the memtable that holds them goes to L0. Openers are unaffected (replay skips entries at or below `last_committed_seq`), so this is checkpoint coverage, not durability. A later memtable flush plus a `Durable` checkpoint does not help (the memtable is empty). | Streams `keyspace::projection_tests::a_checkpoint_of_everything_with_wal_writes_buffered_is_refused_for_projection` (`src/keyspace_projection_tests.rs`): writes left buffered (no background flush, default write options), `checkpoint(All)`, a projected clone is refused; with `flush()` first, the same clone succeeds. | Streams' split and merge call `flush()` (a WAL flush) immediately before `create_checkpoint(All)` (S4-DESIGN rev 2.1 §1, §6.2). A fix would stamp the frozen memtable after the WAL flush `begin_batch_writer_flush(true)` issues completes. |

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
