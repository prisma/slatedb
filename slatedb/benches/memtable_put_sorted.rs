// our microbenchmarks use pprof, but it doesn't work on windows
#![cfg(not(windows))]

//! Compares the memtable's insert paths for a write batch: `put` for each row (each search
//! starts at the skip list's head) and `put_sorted` (each search starts where the previous
//! insert ended). Two more arms separate the gains and measure the cost for one row:
//!
//! - `put_pinned`, the control: `put` for each row under one outer epoch pin per group. Each
//!   row's own pin is then a re-pin, which only increments a counter. `put_sorted` also pins
//!   once per group, so this arm shows how much of its gain comes from the pin.
//! - `one_row_put` and `one_row_put_sorted`: each row is its own batch with its own sequence
//!   number, as `db.put` writes it. Here `put_sorted` pays to set up the finger and gains
//!   nothing.
//!
//! The batches come from a synthetic generator shaped like S2, slate-temporal's knee workload. A
//! commit group holds 24 jobs. A job is one Workflow Task completion of a random Workflow Id: 12
//! keys (three history events, the run, pending and current records, two ready-index keys and
//! four timer keys). A group's keys are sorted and inserted with one sequence number, as
//! `WriteBatch` hands them over. The memtable starts again when it holds `entries` rows, as a
//! flush does. Only the inserts are timed.
//!
//! Two key layouts:
//!
//! - `bucket_first`: every key of a Workflow Id leads with one of 256 buckets, so a group's keys
//!   land in up to 256 places in the memtable.
//! - `clustered`: the old layout. Keys lead with their key space, and the timer and ready-index
//!   keys are time-ordered, so they land at a few warm tails.
//!
//! Run with: cargo bench -p slatedb --features bench-internal --bench memtable_put_sorted
//!
//! Results on 2026-11-14, Apple M4 Pro (14 cores), macOS 26.6, the bench profile (LTO), on a
//! quiet host, in ns per row: criterion's estimate of the time per group divided by the mean
//! group (287 rows). The 95% intervals are within ±1%.
//!
//! | layout | memtable entries | `put` | `put_pinned` | `put_sorted` | `put_sorted` change |
//! |---|---:|---:|---:|---:|---:|
//! | bucket-first | 50,000 | 219.1 | 220.7 | 175.1 | -44.0 (-20.1%) |
//! | bucket-first | 200,000 | 318.3 | 315.7 | 274.9 | -43.4 (-13.6%) |
//! | clustered | 50,000 | 213.1 | 212.3 | 171.7 | -41.4 (-19.4%) |
//! | clustered | 200,000 | 275.5 | 274.6 | 234.6 | -41.0 (-14.9%) |
//!
//! One-row batches, in ns per row:
//!
//! | layout | memtable entries | `one_row_put` | `one_row_put_sorted` | change |
//! |---|---:|---:|---:|---:|
//! | bucket-first | 50,000 | 224.2 | 234.3 | +10.1 (+4.5%) |
//! | bucket-first | 200,000 | 315.3 | 327.9 | +12.6 (+4.0%) |
//! | clustered | 50,000 | 219.8 | 227.9 | +8.1 (+3.7%) |
//! | clustered | 200,000 | 276.9 | 286.1 | +9.2 (+3.3%) |
//!
//! The control arm is within 3 ns of `put`, so the gain comes from the finger, not from the pin.
//! The gain is a roughly constant cost per row, 41 to 44 ns, in both layouts and at both sizes.
//! It does not close the bucket-first gap: that gap is +6 and +43 ns with `put`, and +3 and +40
//! ns with `put_sorted`. The gap is most likely the cache misses near each spread key, which a
//! search from the previous key still meets. A one-row batch costs 8 to 13 ns more with
//! `put_sorted`.

use std::cell::RefCell;

#[allow(clippy::disallowed_types)]
use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pprof::criterion::{Output, PProfProfiler};
use slatedb::memtable_benches::Memtable;

/// Jobs in a commit group.
const GROUP: usize = 24;
const BUCKETS: u64 = 256;
/// Memtable sizes at which the memtable starts again.
const ENTRIES: [usize; 2] = [50_000, 200_000];
/// Groups generated per layout; the replay cycles through them.
const GROUPS: usize = 1_500;

#[derive(Clone, Copy)]
enum Layout {
    Clustered,
    BucketFirst,
}

impl Layout {
    fn name(self) -> &'static str {
        match self {
            Layout::Clustered => "clustered",
            Layout::BucketFirst => "bucket_first",
        }
    }
}

/// A small deterministic generator (xorshift64*).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

fn string(key: &mut Vec<u8>, s: &[u8]) {
    key.extend_from_slice(&(s.len() as u32).to_be_bytes());
    key.extend_from_slice(s);
}

/// A key of key space `old` (clustered) or `new` (bucket-first) for Workflow Id `wf`, whose tail
/// leads with `time` when it is time-ordered.
fn key(layout: Layout, old: u8, new: u8, wf: u64, time: Option<u64>, rest: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(96);
    match layout {
        Layout::Clustered => key.extend_from_slice(&[1, old]),
        Layout::BucketFirst => key.extend_from_slice(&[2, 0, (wf % BUCKETS) as u8, new]),
    }
    if let Some(time) = time {
        key.extend_from_slice(&time.to_be_bytes());
    }
    string(&mut key, b"bench-project");
    string(&mut key, b"bench-namespace");
    string(&mut key, format!("wf-{wf:016x}").as_bytes());
    key.extend_from_slice(rest);
    key
}

/// One job's keys: a Workflow Task's completion of Workflow Id `wf` at `now` ms.
fn job(layout: Layout, wf: u64, event: u64, now: u64) -> Vec<Vec<u8>> {
    let mut keys = Vec::with_capacity(12);
    for n in 0..3 {
        keys.push(key(
            layout,
            0x05,
            0x30,
            wf,
            None,
            &(event + n).to_be_bytes(),
        ));
    }
    keys.push(key(layout, 0x04, 0x21, wf, None, b"run"));
    keys.push(key(layout, 0x06, 0x22, wf, None, b"pending"));
    keys.push(key(layout, 0x03, 0x20, wf, None, b""));
    // The ready index, ordered by Task Queue then time.
    let ready = |time: u64| {
        let mut key = match layout {
            Layout::Clustered => vec![1, 0x09],
            Layout::BucketFirst => vec![2, 0, (wf % BUCKETS) as u8, 0x10],
        };
        string(&mut key, b"bench-queue");
        key.extend_from_slice(&time.to_be_bytes());
        key.extend_from_slice(format!("wf-{wf:016x}").as_bytes());
        key
    };
    keys.push(ready(now));
    keys.push(ready(now.saturating_sub(250)));
    // Timers, time-ordered under the key space (clustered) or under the bucket.
    for (offset, n) in [(10_000, 1u8), (5_000, 2), (0, 3), (250, 4)] {
        keys.push(key(layout, 0x0A, 0x11, wf, Some(now + offset), &[n]));
    }
    keys
}

/// `GROUPS` commit groups, each sorted and without duplicates.
fn groups(layout: Layout) -> Vec<Vec<(Bytes, Bytes)>> {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let value = Bytes::from_static(&[0u8; 64]);
    let (mut now, mut event) = (1_700_000_000_000u64, 0u64);
    (0..GROUPS)
        .map(|_| {
            let mut keys: Vec<Vec<u8>> = (0..GROUP)
                .flat_map(|_| job(layout, rng.next() % 100_000, event, now))
                .collect();
            event += 3;
            now += 2;
            keys.sort();
            keys.dedup();
            keys.into_iter()
                .map(|key| (Bytes::from(key), value.clone()))
                .collect()
        })
        .collect()
}

/// An insert path under test.
#[derive(Clone, Copy)]
enum Arm {
    /// `put` for each row of a group: one epoch pin and one head search per row.
    Put,
    /// The control: `put` for each row under one outer epoch pin per group, so each row's pin
    /// is a cheap re-pin. It separates the gain of one pin per group from the finger's.
    PutPinned,
    /// `put_sorted` for a group: one pin, and each search starts from the previous insert.
    PutSorted,
    /// One-row batches, as `db.put` writes them: `put` for one row.
    OneRowPut,
    /// One-row batches: `put_sorted` for one row, which also sets up the finger.
    OneRowPutSorted,
}

impl Arm {
    fn name(self) -> &'static str {
        match self {
            Arm::Put => "put",
            Arm::PutPinned => "put_pinned",
            Arm::PutSorted => "put_sorted",
            Arm::OneRowPut => "one_row_put",
            Arm::OneRowPutSorted => "one_row_put_sorted",
        }
    }

    fn one_row(self) -> bool {
        matches!(self, Arm::OneRowPut | Arm::OneRowPutSorted)
    }
}

/// The replay's state across criterion's calls: the memtable and the next group (or row).
struct Replay {
    groups: Vec<Vec<(Bytes, Bytes)>>,
    /// Every group's rows in turn, for the one-row arms.
    rows: Vec<(Bytes, Bytes)>,
    entries: usize,
    table: Memtable,
    next: usize,
    seq: u64,
}

impl Replay {
    fn new(groups: &[Vec<(Bytes, Bytes)>], entries: usize) -> Self {
        Replay {
            groups: groups.to_vec(),
            rows: groups.iter().flatten().cloned().collect(),
            entries,
            table: Memtable::default(),
            next: 0,
            seq: 0,
        }
    }

    /// Inserts the next `iters` groups (or rows, for a one-row arm), and returns the time the
    /// inserts took.
    fn run(&mut self, iters: u64, arm: Arm) -> Duration {
        if arm.one_row() {
            return self.run_rows(iters, arm);
        }
        let mut took = Duration::ZERO;
        for _ in 0..iters {
            let rows = self.groups[self.next].clone();
            self.next = (self.next + 1) % self.groups.len();
            self.seq += 1;
            #[allow(clippy::disallowed_types)]
            let start = Instant::now();
            match arm {
                Arm::Put => {
                    for (key, value) in rows {
                        self.table.put(key, value, self.seq);
                    }
                }
                Arm::PutPinned => {
                    let _guard = crossbeam_epoch::pin();
                    for (key, value) in rows {
                        self.table.put(key, value, self.seq);
                    }
                }
                Arm::PutSorted => self.table.put_sorted(rows, self.seq),
                Arm::OneRowPut | Arm::OneRowPutSorted => unreachable!(),
            }
            took += start.elapsed();
            if self.table.len() >= self.entries {
                self.table = Memtable::default();
            }
        }
        took
    }

    /// Inserts the next `iters` rows, each as its own batch with its own sequence number. The
    /// rows up to the next restart of the memtable are timed together, since one row is too
    /// short to time alone.
    fn run_rows(&mut self, iters: u64, arm: Arm) -> Duration {
        let mut took = Duration::ZERO;
        let mut left = iters as usize;
        while left > 0 {
            let chunk = left.min(self.entries.saturating_sub(self.table.len()).max(1));
            let rows: Vec<(Bytes, Bytes)> = (0..chunk)
                .map(|n| self.rows[(self.next + n) % self.rows.len()].clone())
                .collect();
            self.next = (self.next + chunk) % self.rows.len();
            let seq = self.seq;
            self.seq += chunk as u64;
            #[allow(clippy::disallowed_types)]
            let start = Instant::now();
            match arm {
                Arm::OneRowPut => {
                    for (n, (key, value)) in rows.into_iter().enumerate() {
                        self.table.put(key, value, seq + 1 + n as u64);
                    }
                }
                Arm::OneRowPutSorted => {
                    for (n, row) in rows.into_iter().enumerate() {
                        self.table
                            .put_sorted(std::iter::once(row), seq + 1 + n as u64);
                    }
                }
                Arm::Put | Arm::PutPinned | Arm::PutSorted => unreachable!(),
            }
            took += start.elapsed();
            left -= chunk;
            if self.table.len() >= self.entries {
                self.table = Memtable::default();
            }
        }
        took
    }
}

fn bench_put_sorted(c: &mut Criterion) {
    for layout in [Layout::BucketFirst, Layout::Clustered] {
        let groups = groups(layout);
        let rows: usize = groups.iter().map(Vec::len).sum();
        let mut group = c.benchmark_group(format!("memtable_put_sorted/{}", layout.name()));
        for entries in ENTRIES {
            for arm in [
                Arm::Put,
                Arm::PutPinned,
                Arm::PutSorted,
                Arm::OneRowPut,
                Arm::OneRowPutSorted,
            ] {
                // Rows per iteration (a group's on average, or one), so criterion reports rows
                // per second.
                group.throughput(Throughput::Elements(if arm.one_row() {
                    1
                } else {
                    (rows / groups.len()) as u64
                }));
                let replay = RefCell::new(Replay::new(&groups, entries));
                group.bench_function(BenchmarkId::new(arm.name(), entries), |b| {
                    b.iter_custom(|iters| replay.borrow_mut().run(iters, arm));
                });
            }
        }
        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        // This only runs when `--profile-time <num_seconds>` is set
        .with_profiler(PProfProfiler::new(100, Output::Protobuf));
    targets = bench_put_sorted
}

criterion_main!(benches);
