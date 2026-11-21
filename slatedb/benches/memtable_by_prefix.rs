//! Measures a memtable prototype with one skip list for each 2-byte key prefix. The question is
//! whether it removes the extra insert cost of a bucket-first key layout.
//!
//! The replay is slate-temporal's P2-11 memtable replay (its step 0b, corrected), copied here.
//! A commit group holds 24 jobs. A job is one Workflow Task completion of one Workflow Id: 12
//! keys (three history events, the run, pending and current records, two ready-index keys and
//! four timer keys). The Workflow Ids are in sequence, as in S2, slate-temporal's knee workload.
//! Each job takes its Workflow Id from S2's live window: the newest 625 Workflow Executions,
//! with 6 new ones started in each group. A group's keys are sorted and inserted one at a time
//! into a `SkipMap<Vec<u8>, ()>`. The memtable starts again when it holds `entries` rows, as a
//! flush does. Only the inserts are timed.
//!
//! Every variant uses plain `insert`, not the finger insert (`put_sorted`). This bench does not
//! measure the prototype with a finger.
//!
//! Three variants:
//!
//! - `v5`: the clustered layout. Keys lead with `01 <key space>`, and the timer and ready-index
//!   keys are time-ordered, so a group's keys land at a few warm tails. One skip list.
//! - `v6-256`: the bucket-first layout. Every key of a Workflow Id leads with `02 <bucket>`, the
//!   bucket two bytes big-endian (the top 8 bits of the Workflow Id's hash, so 256 buckets).
//!   One skip list.
//! - `v6-256 by prefix`: the same keys in the prototype, [`ByPrefix`]. It has one skip list for
//!   each value of the two bytes after a lead byte `02`, one list for the keys before them and
//!   one for the keys after them. Iteration is the concatenation of the lists in key order.
//!
//! The bench also times a full iteration and a seek on a memtable full of `v6-256` keys, in one
//! skip list and in the prototype.
//!
//! The prototype is only in this bench. It does not change the library.
//!
//! Run with: cargo bench -p slatedb --bench memtable_by_prefix
//! Run it on a quiet host. It prints its figures and has no criterion report.
//!
//! Results on 2026-11-21 (23:17Z), Apple M4 Pro (14 cores), macOS 26.6.2, the bench profile (LTO),
//! on a quiet host. Each figure is the mean of six replays of 20,000 commit groups, in ns per
//! insert, with the spread (most less least) of the six. The excess is over `v5`'s mean.
//!
//! | memtable entries | `v5` | `v6-256` | `v6-256` by prefix |
//! |---:|---:|---:|---:|
//! | 50,000 | 198.3 (spread 4.8) | 226.5 (spread 6.4), +28.2 | 135.4 (spread 4.6), -62.9 |
//! | 200,000 | 217.6 (spread 3.3) | 273.1 (spread 2.6), +55.5 | 186.7 (spread 1.0), -31.0 |
//!
//! A full iteration and a seek, `v6-256` keys, one list against the prototype (means of six):
//!
//! | memtable entries | iteration, ns per entry | seek, ns |
//! |---:|---:|---:|
//! | 50,133 | 7.00 against 7.07 (+1.0%) | 213.8 against 149.9 (-29.9%) |
//! | 200,130 | 10.28 against 10.41 (+1.3%) | 283.7 against 206.8 (-27.1%) |
//!
//! The prototype's inserts are faster than `v5`'s in one list. Its lists are short: a full
//! memtable holds about 195 or 780 entries in each of its 256 lists. This bench does not measure
//! `v5` keys in lists by key space.

use std::hint::black_box;
use std::ops::Bound;
#[allow(clippy::disallowed_types)]
use std::time::{Duration, Instant};

use crossbeam_skiplist::SkipMap;

/// Jobs in a commit group.
const GROUP: usize = 24;
/// S2's live Workflow Executions: 500 start each second, and each stays open about 1.25 s.
const S2_LIVE: u64 = 625;
/// Workflow Executions that S2 starts in the time of one commit group.
const S2_STARTS_PER_GROUP: u64 = 6;
const BUCKETS: u64 = 256;
/// Memtable sizes at which the memtable starts again.
const ENTRIES: [usize; 2] = [50_000, 200_000];
/// Measured replays for each variant, in alternation.
const REPLAYS: usize = 6;
/// Commit groups in one measured replay, and in one warm-up.
const GROUPS: usize = 20_000;
const WARM_UP_GROUPS: usize = 2_000;
/// Seek targets for one timed seek pass.
const SEEKS: usize = 20_000;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Layout {
    V5,
    V6,
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

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

fn fnv1a64_from(hash: u64, bytes: &[u8]) -> u64 {
    bytes.iter().fold(hash, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// splitmix64's finalizer.
fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// slate-temporal's Workflow Id hash: `mix64(fnv1a64(01 03 <project> <namespace> <id>))`,
/// each string with a `u32` big-endian length.
fn workflow_id_hash(project: &str, namespace: &str, id: &str) -> u64 {
    let mut hash = fnv1a64_from(FNV_OFFSET, &[0x01, 0x03]);
    for part in [project, namespace, id] {
        hash = fnv1a64_from(
            fnv1a64_from(hash, &(part.len() as u32).to_be_bytes()),
            part.as_bytes(),
        );
    }
    mix64(hash)
}

/// The bucket of S2's Workflow Id `n`: the top 8 bits of its hash.
fn bucket(n: u64) -> u8 {
    let id = format!("wf-{n:016x}");
    (workflow_id_hash("bench-project", "bench-namespace", &id) >> (64 - BUCKETS.trailing_zeros()))
        as u8
}

/// A key of key space `v5` or `v6` for Workflow Id `wf`, whose tail leads with `time` when the
/// key space is time-ordered.
fn key(layout: Layout, v5: u8, v6: u8, wf: u64, time: Option<u64>, rest: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(96);
    match layout {
        Layout::V5 => key.extend_from_slice(&[1, v5]),
        Layout::V6 => key.extend_from_slice(&[2, 0, bucket(wf), v6]),
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
            Layout::V5 => vec![1, 0x09],
            Layout::V6 => vec![2, 0, bucket(wf), 0x10],
        };
        string(&mut key, b"bench-queue");
        key.extend_from_slice(&time.to_be_bytes());
        key.extend_from_slice(format!("wf-{wf:016x}").as_bytes());
        key
    };
    keys.push(ready(now));
    keys.push(ready(now.saturating_sub(250)));
    // Timers, time-ordered under the key space (v5) or under the bucket (v6).
    for (offset, n) in [(10_000, 1u8), (5_000, 2), (0, 3), (250, 4)] {
        keys.push(key(layout, 0x0A, 0x11, wf, Some(now + offset), &[n]));
    }
    keys
}

/// The commit groups of a replay, each sorted and without duplicates. Every replay of a layout
/// gets the same groups.
struct Groups {
    rng: Rng,
    layout: Layout,
    now: u64,
    event: u64,
    newest: u64,
}

impl Groups {
    fn new(layout: Layout) -> Self {
        Groups {
            rng: Rng(0x9E37_79B9_7F4A_7C15),
            layout,
            now: 1_700_000_000_000,
            event: 0,
            newest: S2_LIVE,
        }
    }

    fn next_group(&mut self) -> Vec<Vec<u8>> {
        let mut batch: Vec<Vec<u8>> = (0..GROUP)
            .flat_map(|_| {
                let wf = self.newest - self.rng.next() % S2_LIVE;
                job(self.layout, wf, self.event, self.now)
            })
            .collect();
        self.event += 3;
        self.now += 2;
        self.newest += S2_STARTS_PER_GROUP;
        batch.sort();
        batch.dedup();
        batch
    }
}

/// A memtable under test.
trait Table: Default {
    fn insert(&mut self, key: Vec<u8>);
    fn len(&self) -> usize;
    /// Visits every key in order, and returns the number of keys.
    fn iterate(&self) -> usize;
    /// The first key at or after `target`.
    fn seek(&self, target: &[u8]) -> Option<usize>;
}

/// One skip list, as the memtable is today.
#[derive(Default)]
struct OneList(SkipMap<Vec<u8>, ()>);

impl Table for OneList {
    fn insert(&mut self, key: Vec<u8>) {
        self.0.insert(key, ());
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn iterate(&self) -> usize {
        let mut n = 0;
        for entry in self.0.iter() {
            black_box(entry.key());
            n += 1;
        }
        n
    }

    fn seek(&self, target: &[u8]) -> Option<usize> {
        self.0
            .lower_bound(Bound::Included(target))
            .map(|entry| entry.key().len())
    }
}

/// One skip list of the prototype.
type List = SkipMap<Vec<u8>, ()>;

/// The number of partitions: one before the `02` keys, one for each 2-byte prefix after `02`,
/// one after the `02` keys.
const PARTITIONS: usize = 1 + 65_536 + 1;

/// The prototype: one skip list for each 2-byte prefix after a lead byte `02`, one for the keys
/// that sort before all `02` keys and one for the keys that sort after them. The partition of a
/// key goes up with the key, so the concatenation of the lists in partition order is in key
/// order. A list is made at its first insert. A real memtable must make it safely for
/// concurrent writers, or make all lists before the first write.
struct ByPrefix {
    lists: Vec<Option<Box<List>>>,
    /// The partitions that have a list, in ascending order.
    used: Vec<usize>,
}

impl Default for ByPrefix {
    fn default() -> Self {
        ByPrefix {
            lists: (0..PARTITIONS).map(|_| None).collect(),
            used: Vec::new(),
        }
    }
}

/// The partition of `key`. A key `02` or `02 xx`, shorter than its prefix, goes to the lowest
/// partition that agrees with its bytes. It sorts before every other key there.
fn partition(key: &[u8]) -> usize {
    match key.first() {
        None => 0,
        Some(lead) if *lead < 2 => 0,
        Some(2) => {
            let hi = key.get(1).copied().unwrap_or(0) as usize;
            let lo = key.get(2).copied().unwrap_or(0) as usize;
            1 + (hi << 8 | lo)
        }
        Some(_) => PARTITIONS - 1,
    }
}

impl Table for ByPrefix {
    fn insert(&mut self, key: Vec<u8>) {
        let at = partition(&key);
        let list = match &mut self.lists[at] {
            Some(list) => list,
            slot @ None => {
                let place = self.used.partition_point(|used| *used < at);
                self.used.insert(place, at);
                slot.insert(Box::default())
            }
        };
        list.insert(key, ());
    }

    fn len(&self) -> usize {
        self.used
            .iter()
            .map(|at| self.lists[*at].as_ref().map_or(0, |list| list.len()))
            .sum()
    }

    fn iterate(&self) -> usize {
        let mut n = 0;
        for at in &self.used {
            if let Some(list) = &self.lists[*at] {
                for entry in list.iter() {
                    black_box(entry.key());
                    n += 1;
                }
            }
        }
        n
    }

    fn seek(&self, target: &[u8]) -> Option<usize> {
        let at = partition(target);
        if let Some(list) = &self.lists[at] {
            if let Some(entry) = list.lower_bound(Bound::Included(target)) {
                return Some(entry.key().len());
            }
        }
        let next = self.used.partition_point(|used| *used <= at);
        self.used[next..].iter().find_map(|at| {
            self.lists[*at]
                .as_ref()
                .and_then(|list| list.front())
                .map(|entry| entry.key().len())
        })
    }
}

/// One replay of `groups` commit groups into a memtable of up to `entries` entries. Returns ns
/// per insert.
fn replay<T: Table>(layout: Layout, entries: usize, groups: usize) -> f64 {
    let mut source = Groups::new(layout);
    let mut table = T::default();
    let mut inserts = 0u64;
    let mut took = Duration::ZERO;
    for _ in 0..groups {
        let batch = source.next_group();
        #[allow(clippy::disallowed_types)]
        let started = Instant::now();
        for key in batch {
            table.insert(key);
            inserts += 1;
        }
        took += started.elapsed();
        if table.len() >= entries {
            table = T::default();
        }
    }
    took.as_nanos() as f64 / inserts as f64
}

fn mean(xs: &[f64]) -> f64 {
    xs.iter().sum::<f64>() / xs.len() as f64
}

/// The least and the most of `xs`, and the spread between them.
fn span(xs: &[f64]) -> String {
    let lo = xs.iter().copied().fold(f64::INFINITY, f64::min);
    let hi = xs.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    format!("{lo:.1}-{hi:.1}, spread {:.1}", hi - lo)
}

/// A memtable filled with the replay's `v6-256` groups up to `entries` entries.
fn filled<T: Table>(entries: usize) -> T {
    let mut source = Groups::new(Layout::V6);
    let mut table = T::default();
    while table.len() < entries {
        for key in source.next_group() {
            table.insert(key);
        }
    }
    table
}

/// Seek targets: keys of the groups after the ones that filled the memtable, so some are in it
/// and some are not, in a random order.
fn targets(entries: usize) -> Vec<Vec<u8>> {
    let mut source = Groups::new(Layout::V6);
    let mut table = OneList::default();
    while table.len() < entries {
        for key in source.next_group() {
            table.insert(key);
        }
    }
    let mut keys = Vec::with_capacity(SEEKS);
    while keys.len() < SEEKS {
        keys.extend(source.next_group());
    }
    keys.truncate(SEEKS);
    let mut rng = Rng(0xD1B5_4A32_D192_ED03);
    for at in (1..keys.len()).rev() {
        keys.swap(at, (rng.next() % (at as u64 + 1)) as usize);
    }
    keys
}

/// ns per entry of a full iteration.
fn time_iterate<T: Table>(table: &T) -> f64 {
    #[allow(clippy::disallowed_types)]
    let started = Instant::now();
    let n = black_box(table.iterate());
    started.elapsed().as_nanos() as f64 / n as f64
}

/// ns per seek.
fn time_seek<T: Table>(table: &T, targets: &[Vec<u8>]) -> f64 {
    #[allow(clippy::disallowed_types)]
    let started = Instant::now();
    for target in targets {
        black_box(table.seek(black_box(target)));
    }
    started.elapsed().as_nanos() as f64 / targets.len() as f64
}

/// Checks the copied hash on slate-temporal's independent vector (`p`/`ns`/`a`: slot 35,033,
/// hash `88d9cadafe521592`), and the prototype's order and seek on a small memtable.
fn check() {
    assert_eq!(workflow_id_hash("p", "ns", "a"), 0x88d9_cada_fe52_1592);
    let mut one = OneList::default();
    let mut by_prefix = ByPrefix::default();
    let mut source = Groups::new(Layout::V6);
    for _ in 0..200 {
        for key in source.next_group() {
            one.insert(key.clone());
            by_prefix.insert(key);
        }
    }
    for key in [
        vec![0u8],
        vec![1, 9],
        vec![2],
        vec![2, 0],
        vec![2, 0, 7],
        vec![3],
    ] {
        one.insert(key.clone());
        by_prefix.insert(key);
    }
    let a: Vec<Vec<u8>> = one.0.iter().map(|entry| entry.key().clone()).collect();
    let mut b = Vec::new();
    for at in &by_prefix.used {
        if let Some(list) = &by_prefix.lists[*at] {
            b.extend(list.iter().map(|entry| entry.key().clone()));
        }
    }
    assert_eq!(a, b, "the prototype iterates in key order");
    assert_eq!(one.len(), by_prefix.len());
    for target in targets(2_000).iter().take(2_000).chain(&a) {
        assert_eq!(one.seek(target), by_prefix.seek(target));
    }
    assert_eq!(by_prefix.seek(&[4]), None);
}

// The bench prints its figures; it has no logger.
#[allow(clippy::disallowed_macros)]
fn main() {
    check();
    for entries in ENTRIES {
        replay::<OneList>(Layout::V5, entries, WARM_UP_GROUPS);
        replay::<OneList>(Layout::V6, entries, WARM_UP_GROUPS);
        replay::<ByPrefix>(Layout::V6, entries, WARM_UP_GROUPS);
        let (mut v5, mut v6, mut proto) = (Vec::new(), Vec::new(), Vec::new());
        for _ in 0..REPLAYS {
            v5.push(replay::<OneList>(Layout::V5, entries, GROUPS));
            v6.push(replay::<OneList>(Layout::V6, entries, GROUPS));
            proto.push(replay::<ByPrefix>(Layout::V6, entries, GROUPS));
        }
        let base = mean(&v5);
        let (v6_excess, proto_excess) = (mean(&v6) - base, mean(&proto) - base);
        println!(
            "S2 ids, memtable of {entries} entries: ns per insert v5 {base:.1} ({}), \
             v6-256 {:.1} ({}) ({v6_excess:+.1}), v6-256 by prefix {:.1} ({}) \
             ({proto_excess:+.1}); share of the excess removed: {:.2}; \
             valid (excess at 256 >= 10 ns): {}",
            span(&v5),
            mean(&v6),
            span(&v6),
            mean(&proto),
            span(&proto),
            1.0 - proto_excess / v6_excess,
            v6_excess >= 10.0
        );
    }
    for entries in ENTRIES {
        let one: OneList = filled(entries);
        let by_prefix: ByPrefix = filled(entries);
        let targets = targets(entries);
        time_iterate(&one);
        time_iterate(&by_prefix);
        time_seek(&one, &targets);
        time_seek(&by_prefix, &targets);
        let (mut iter_one, mut iter_proto, mut seek_one, mut seek_proto) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for _ in 0..REPLAYS {
            iter_one.push(time_iterate(&one));
            iter_proto.push(time_iterate(&by_prefix));
            seek_one.push(time_seek(&one, &targets));
            seek_proto.push(time_seek(&by_prefix, &targets));
        }
        println!(
            "v6-256 keys, memtable of {} entries: full iteration ns per entry one list {:.2} ({}), \
             by prefix {:.2} ({}) ({:+.1}%); seek ns one list {:.1} ({}), by prefix {:.1} ({}) \
             ({:+.1}%)",
            one.len(),
            mean(&iter_one),
            span(&iter_one),
            mean(&iter_proto),
            span(&iter_proto),
            100.0 * (mean(&iter_proto) / mean(&iter_one) - 1.0),
            mean(&seek_one),
            span(&seek_one),
            mean(&seek_proto),
            span(&seek_proto),
            100.0 * (mean(&seek_proto) / mean(&seek_one) - 1.0),
        );
    }
}
