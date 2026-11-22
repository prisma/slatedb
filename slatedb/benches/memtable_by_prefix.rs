//! Measures a memtable prototype with one skip list for each 2-byte key prefix. The question is
//! whether it removes the extra insert cost of a bucket-first key layout, and whether lists by
//! key space make the clustered layout's inserts cheaper.
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
//! Eight variants: four memtables, each with two insert paths.
//!
//! - `v5`: the clustered layout. Keys lead with `01 <key space>`, and the timer and ready-index
//!   keys are time-ordered, so a group's keys land at a few warm tails. One skip list.
//! - `v6-256`: the bucket-first layout. Every key of a Workflow Id leads with `02 <bucket>`, the
//!   bucket two bytes big-endian (the top 8 bits of the Workflow Id's hash, so 256 buckets).
//!   One skip list.
//! - `v6-256 by bucket`: the same keys in the prototype, [`ByPrefix`] with [`Bucket`]. It has one
//!   skip list for each value of the two bytes after a lead byte `02`, one list for the keys
//!   before them and one for the keys after them. Iteration is the concatenation of the lists in
//!   key order.
//! - `v5 by key space`: `v5`'s keys in the prototype with [`KeySpace`]: one skip list for each
//!   value of the byte after a lead byte `01`, plus the two lists before and after.
//!
//! The insert paths:
//!
//! - plain: `SkipMap::insert` for each key. Each search starts at the list's head.
//! - finger: patch 11's finger (`SkipMap::with_finger`), as `KVTable::put_sorted` uses it. One
//!   finger for each list a group touches, and one epoch pin for the group.
//!
//! The bench also times a full iteration and a seek, in one skip list and in the prototype: on a
//! memtable full of `v6-256` keys (by bucket), and on one full of `v5` keys (by key space).
//!
//! The prototype is only in this bench. It does not change the library.
//!
//! Run with: cargo bench -p slatedb --bench memtable_by_prefix
//! Run it on a quiet host. It prints its figures and has no criterion report. Each replay runs in
//! its own process (the bench runs its own executable again), after its own warm-up. With all
//! variants in one process, plain `v5` read up to 40 ns slower in most replays. In separate
//! processes it did not.
//!
//! M-1, on 2026-11-21 (23:17Z), Apple M4 Pro (14 cores), macOS 26.6.2, the bench profile (LTO),
//! on a quiet host, with plain inserts and the first three memtables only. Each figure is the
//! mean of six replays of 20,000 commit groups, in ns per insert, with the spread (most less
//! least) of the six. The excess is over `v5`'s mean.
//!
//! | memtable entries | `v5` | `v6-256` | `v6-256` by bucket |
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
//! M-2, on 2026-11-22 (01:53Z), the same host, each replay in its own process. ns per insert, the
//! mean of six replays (spread), and the excess over `v5`'s mean with the same insert path:
//!
//! | memtable entries | insert | `v5` | `v6-256` | `v6-256` by bucket | `v5` by key space |
//! |---:|---|---:|---:|---:|---:|
//! | 50,000 | plain | 204.8 (7.7) | 232.2 (6.8), +27.4 | 137.5 (3.9), -67.4 | 183.6 (4.0), -21.2 |
//! | 50,000 | finger | 170.0 (4.4) | 203.2 (3.3), +33.2 | 130.1 (1.3), -39.8 | 147.7 (3.6), -22.2 |
//! | 200,000 | plain | 225.3 (1.8) | 283.6 (3.2), +58.3 | 189.9 (2.3), -35.4 | 201.3 (3.0), -24.0 |
//! | 200,000 | finger | 188.8 (2.1) | 270.0 (5.3), +81.2 | 191.5 (3.0), +2.6 | 166.7 (2.2), -22.1 |
//!
//! A full iteration and a seek, one list against the prototype (means of six):
//!
//! | keys | memtable entries | iteration | seek |
//! |---|---:|---:|---:|
//! | `v6-256`, by bucket | 50,000 | 7.50 against 7.27 (-3.0%) | 213.4 against 160.3 (-24.9%) |
//! | `v6-256`, by bucket | 200,000 | 11.41 against 11.55 (+1.2%) | 287.9 against 225.2 (-21.8%) |
//! | `v5`, by key space | 50,000 | 7.30 against 7.06 (-3.4%) | 132.0 against 115.7 (-12.4%) |
//! | `v5`, by key space | 200,000 | 8.20 against 7.80 (-5.0%) | 137.9 against 124.8 (-9.5%) |
//!
//! The finger gap between `v5` and `v6-256` here (+33.2 and +81.2 ns) is larger than in
//! `memtable_put_sorted` (+3 and +40 ns). The benches differ: that one inserts `KVTable` rows
//! with values and sequence numbers, with random Workflow Ids.

use std::hint::black_box;
use std::marker::PhantomData;
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
    /// Inserts a sorted group with patch 11's finger (`SkipMap::with_finger`), as
    /// `KVTable::put_sorted` does: one finger for each list, each search from the previous insert.
    fn insert_sorted(&mut self, batch: Vec<Vec<u8>>);
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

    fn insert_sorted(&mut self, batch: Vec<Vec<u8>>) {
        self.0.with_finger(|finger| {
            for key in batch {
                finger.insert(key, ());
            }
        });
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

/// How the prototype puts keys in lists. The partition of a key must go up with the key.
trait Scheme {
    const PARTITIONS: usize;
    fn of(key: &[u8]) -> usize;
}

/// v6's buckets: one list for each value of the two bytes after a lead byte `02`, one for the
/// keys before them and one for the keys after them.
struct Bucket;

impl Scheme for Bucket {
    const PARTITIONS: usize = 1 + 65_536 + 1;

    /// A key `02` or `02 xx`, shorter than its prefix, goes to the lowest partition that agrees
    /// with its bytes. It sorts before every other key there.
    fn of(key: &[u8]) -> usize {
        match key.first() {
            None => 0,
            Some(lead) if *lead < 2 => 0,
            Some(2) => {
                let hi = key.get(1).copied().unwrap_or(0) as usize;
                let lo = key.get(2).copied().unwrap_or(0) as usize;
                1 + (hi << 8 | lo)
            }
            Some(_) => Self::PARTITIONS - 1,
        }
    }
}

/// v5's key spaces: one list for each value of the byte after a lead byte `01` (a v5 key is
/// `01 <key space> ...`), one for the keys before them and one for the keys after them.
struct KeySpace;

impl Scheme for KeySpace {
    const PARTITIONS: usize = 1 + 256 + 1;

    /// A key `01`, shorter than its prefix, goes to the lowest partition, as for [`Bucket`].
    fn of(key: &[u8]) -> usize {
        match key.first() {
            None => 0,
            Some(0) => 0,
            Some(1) => 1 + key.get(1).copied().unwrap_or(0) as usize,
            Some(_) => Self::PARTITIONS - 1,
        }
    }
}

/// The prototype: one skip list for each partition of a [`Scheme`]. The concatenation of the
/// lists in partition order is in key order. A list is made at its first insert. A real memtable
/// must make it safely for concurrent writers, or make all lists before the first write.
struct ByPrefix<S> {
    lists: Vec<Option<Box<List>>>,
    /// The partitions that have a list, in ascending order.
    used: Vec<usize>,
    scheme: PhantomData<S>,
}

impl<S: Scheme> Default for ByPrefix<S> {
    fn default() -> Self {
        ByPrefix {
            lists: (0..S::PARTITIONS).map(|_| None).collect(),
            used: Vec::new(),
            scheme: PhantomData,
        }
    }
}

impl<S: Scheme> ByPrefix<S> {
    /// The list of partition `at`, made if it has none.
    fn list(&mut self, at: usize) -> &List {
        match &mut self.lists[at] {
            Some(list) => list,
            slot @ None => {
                let place = self.used.partition_point(|used| *used < at);
                self.used.insert(place, at);
                slot.insert(Box::default())
            }
        }
    }
}

impl<S: Scheme> Table for ByPrefix<S> {
    fn insert(&mut self, key: Vec<u8>) {
        let at = S::of(&key);
        self.list(at).insert(key, ());
    }

    fn insert_sorted(&mut self, batch: Vec<Vec<u8>>) {
        // One pin for the group, as one list has, so each list's own pin is a re-pin.
        let _guard = crossbeam_epoch::pin();
        // The group is sorted, so the keys of one list are next to each other.
        let mut keys = batch.into_iter().peekable();
        while let Some(first) = keys.next() {
            let at = S::of(&first);
            self.list(at).with_finger(|finger| {
                finger.insert(first, ());
                while let Some(key) = keys.next_if(|key| S::of(key) == at) {
                    finger.insert(key, ());
                }
            });
        }
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
        let at = S::of(target);
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

/// How a replay inserts a group's keys.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Insert {
    /// `SkipMap::insert` for each key: each search starts at the list's head.
    Plain,
    /// Patch 11's finger: [`Table::insert_sorted`].
    Finger,
}

/// One replay of `groups` commit groups into a memtable of up to `entries` entries. Returns ns
/// per insert.
fn replay<T: Table>(layout: Layout, insert: Insert, entries: usize, groups: usize) -> f64 {
    let mut source = Groups::new(layout);
    let mut table = T::default();
    let mut inserts = 0u64;
    let mut took = Duration::ZERO;
    for _ in 0..groups {
        let batch = source.next_group();
        match insert {
            Insert::Plain => {
                #[allow(clippy::disallowed_types)]
                let started = Instant::now();
                for key in batch {
                    table.insert(key);
                    inserts += 1;
                }
                took += started.elapsed();
            }
            Insert::Finger => {
                inserts += batch.len() as u64;
                #[allow(clippy::disallowed_types)]
                let started = Instant::now();
                table.insert_sorted(batch);
                took += started.elapsed();
            }
        }
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

/// A memtable filled with the replay's groups of `layout` up to `entries` entries.
fn filled<T: Table>(layout: Layout, entries: usize) -> T {
    let mut source = Groups::new(layout);
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
fn targets(layout: Layout, entries: usize) -> Vec<Vec<u8>> {
    let mut source = Groups::new(layout);
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

/// The keys of `table` in iteration order.
fn keys<S: Scheme>(table: &ByPrefix<S>) -> Vec<Vec<u8>> {
    let mut keys = Vec::new();
    for at in &table.used {
        if let Some(list) = &table.lists[*at] {
            keys.extend(list.iter().map(|entry| entry.key().clone()));
        }
    }
    keys
}

/// Checks one scheme against one list on `layout`'s keys and a few short keys: the same keys in
/// the same order, the same seeks, and the same keys by plain insert and by finger.
fn check_scheme<S: Scheme>(layout: Layout, short: &[Vec<u8>]) {
    let mut one = OneList::default();
    let mut by_prefix = ByPrefix::<S>::default();
    let mut fingered = ByPrefix::<S>::default();
    let mut one_fingered = OneList::default();
    let mut source = Groups::new(layout);
    for _ in 0..200 {
        let group = source.next_group();
        for key in &group {
            one.insert(key.clone());
            by_prefix.insert(key.clone());
        }
        fingered.insert_sorted(group.clone());
        one_fingered.insert_sorted(group);
    }
    for key in short {
        one.insert(key.clone());
        by_prefix.insert(key.clone());
        fingered.insert_sorted(vec![key.clone()]);
        one_fingered.insert_sorted(vec![key.clone()]);
    }
    let a: Vec<Vec<u8>> = one.0.iter().map(|entry| entry.key().clone()).collect();
    let b: Vec<Vec<u8>> = one_fingered
        .0
        .iter()
        .map(|entry| entry.key().clone())
        .collect();
    assert_eq!(a, keys(&by_prefix), "the prototype iterates in key order");
    assert_eq!(a, keys(&fingered), "the finger inserts the same keys");
    assert_eq!(a, b, "the finger inserts the same keys in one list");
    assert_eq!(one.len(), by_prefix.len());
    for target in targets(layout, 2_000).iter().take(2_000).chain(&a) {
        assert_eq!(one.seek(target), by_prefix.seek(target));
    }
    assert_eq!(by_prefix.seek(&[4]), None);
}

/// Checks the copied hash on slate-temporal's independent vector (`p`/`ns`/`a`: slot 35,033,
/// hash `88d9cadafe521592`), and both schemes.
fn check() {
    assert_eq!(workflow_id_hash("p", "ns", "a"), 0x88d9_cada_fe52_1592);
    let short = [
        vec![0u8],
        vec![1],
        vec![1, 9],
        vec![2],
        vec![2, 0],
        vec![2, 0, 7],
        vec![3],
    ];
    check_scheme::<Bucket>(Layout::V6, &short);
    check_scheme::<KeySpace>(Layout::V5, &short);
}

/// A variant of the replay.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Variant {
    /// v5's keys in one list.
    V5,
    /// v6's keys at 256 buckets in one list.
    V6,
    /// v6's keys at 256 buckets, one list for each bucket.
    V6ByBucket,
    /// v5's keys, one list for each key space.
    V5ByKeySpace,
}

impl Variant {
    const ALL: [Variant; 4] = [
        Variant::V5,
        Variant::V6,
        Variant::V6ByBucket,
        Variant::V5ByKeySpace,
    ];

    fn name(self) -> &'static str {
        match self {
            Variant::V5 => "v5",
            Variant::V6 => "v6-256",
            Variant::V6ByBucket => "v6-256-by-bucket",
            Variant::V5ByKeySpace => "v5-by-key-space",
        }
    }

    fn replay(self, insert: Insert, entries: usize, groups: usize) -> f64 {
        match self {
            Variant::V5 => replay::<OneList>(Layout::V5, insert, entries, groups),
            Variant::V6 => replay::<OneList>(Layout::V6, insert, entries, groups),
            Variant::V6ByBucket => replay::<ByPrefix<Bucket>>(Layout::V6, insert, entries, groups),
            Variant::V5ByKeySpace => {
                replay::<ByPrefix<KeySpace>>(Layout::V5, insert, entries, groups)
            }
        }
    }
}

impl Insert {
    fn name(self) -> &'static str {
        match self {
            Insert::Plain => "plain",
            Insert::Finger => "finger",
        }
    }
}

/// Times a full iteration and a seek on `layout`'s keys, one list against the prototype, six
/// times each in alternation. Returns the four series: iteration in one list and in the
/// prototype, then seek in one list and in the prototype.
fn iterate_and_seek<S: Scheme>(layout: Layout, entries: usize) -> [Vec<f64>; 4] {
    let one: OneList = filled(layout, entries);
    let by_prefix: ByPrefix<S> = filled(layout, entries);
    let targets = targets(layout, entries);
    time_iterate(&one);
    time_iterate(&by_prefix);
    time_seek(&one, &targets);
    time_seek(&by_prefix, &targets);
    let mut series: [Vec<f64>; 4] = Default::default();
    for _ in 0..REPLAYS {
        series[0].push(time_iterate(&one));
        series[1].push(time_iterate(&by_prefix));
        series[2].push(time_seek(&one, &targets));
        series[3].push(time_seek(&by_prefix, &targets));
    }
    series
}

/// Runs this bench's executable again with `args`, and returns what it prints.
fn child(args: &[String]) -> String {
    let exe = std::env::current_exe().expect("the bench's executable");
    let out = std::process::Command::new(exe)
        .args(args)
        .output()
        .expect("the child runs");
    assert!(out.status.success(), "the child {args:?} failed: {out:?}");
    String::from_utf8(out.stdout).expect("the child prints text")
}

fn values(text: &str) -> Vec<f64> {
    text.split_whitespace()
        .map(|value| value.parse().expect("a number"))
        .collect()
}

fn list(xs: &[f64]) -> String {
    let xs: Vec<String> = xs.iter().map(|x| format!("{x:.1}")).collect();
    xs.join(" ")
}

/// The driver. Each replay runs in its own process, after its own warm-up, so that no variant
/// runs on the heap that another variant left. The eight variants alternate in each of six
/// rounds. Each iteration and seek reading runs in its own process for each layout and size.
#[allow(clippy::disallowed_macros)]
fn drive() {
    check();
    let runs: Vec<(Variant, Insert)> = [Insert::Plain, Insert::Finger]
        .into_iter()
        .flat_map(|insert| Variant::ALL.map(|variant| (variant, insert)))
        .collect();
    for entries in ENTRIES {
        let mut readings: Vec<Vec<f64>> = vec![Vec::new(); runs.len()];
        for _ in 0..REPLAYS {
            for (at, (variant, insert)) in runs.iter().enumerate() {
                let args = [
                    "replay".to_string(),
                    variant.name().to_string(),
                    insert.name().to_string(),
                    entries.to_string(),
                ];
                readings[at].extend(values(&child(&args)));
            }
        }
        for insert in [Insert::Plain, Insert::Finger] {
            let of = |variant: Variant| {
                let at = runs
                    .iter()
                    .position(|run| *run == (variant, insert))
                    .expect("a run of each variant");
                &readings[at]
            };
            let base = mean(of(Variant::V5));
            for variant in Variant::ALL {
                let xs = of(variant);
                println!(
                    "memtable of {entries} entries, {} insert, {}: ns per insert {:.1} ({}) \
                     ({:+.1}); replays {}",
                    insert.name(),
                    variant.name(),
                    mean(xs),
                    span(xs),
                    mean(xs) - base,
                    list(xs)
                );
            }
            let v6_excess = mean(of(Variant::V6)) - base;
            let bucket_excess = mean(of(Variant::V6ByBucket)) - base;
            println!(
                "memtable of {entries} entries, {} insert: share of v6-256's excess removed by \
                 bucket {:.2}; valid (excess at 256 >= 10 ns): {}",
                insert.name(),
                1.0 - bucket_excess / v6_excess,
                v6_excess >= 10.0
            );
        }
    }
    for layout in ["v6", "v5"] {
        for entries in ENTRIES {
            let args = [
                "iterate".to_string(),
                layout.to_string(),
                entries.to_string(),
            ];
            let text = child(&args);
            let series: Vec<Vec<f64>> = text.lines().map(values).collect();
            let change = |a: &[f64], b: &[f64]| 100.0 * (mean(b) / mean(a) - 1.0);
            println!(
                "{} keys, memtable of {entries} entries: full iteration ns per entry one list \
                 {:.2} ({}), prototype {:.2} ({}) ({:+.1}%); seek ns one list {:.1} ({}), \
                 prototype {:.1} ({}) ({:+.1}%)",
                match layout {
                    "v6" => "v6-256, by bucket",
                    _ => "v5, by key space",
                },
                mean(&series[0]),
                span(&series[0]),
                mean(&series[1]),
                span(&series[1]),
                change(&series[0], &series[1]),
                mean(&series[2]),
                span(&series[2]),
                mean(&series[3]),
                span(&series[3]),
                change(&series[2], &series[3]),
            );
        }
    }
}

// The bench prints its figures; it has no logger.
#[allow(clippy::disallowed_macros)]
fn main() {
    // `cargo bench` passes `--bench`.
    let args: Vec<String> = std::env::args()
        .skip(1)
        .filter(|arg| arg != "--bench")
        .collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    match args.as_slice() {
        [] => drive(),
        ["replay", variant, insert, entries] => {
            let variant = Variant::ALL
                .into_iter()
                .find(|v| v.name() == *variant)
                .expect("a variant");
            let insert = match *insert {
                "plain" => Insert::Plain,
                "finger" => Insert::Finger,
                other => panic!("no insert path {other}"),
            };
            let entries: usize = entries.parse().expect("a number of entries");
            variant.replay(insert, entries, WARM_UP_GROUPS);
            println!("{}", variant.replay(insert, entries, GROUPS));
        }
        ["iterate", layout, entries] => {
            let entries: usize = entries.parse().expect("a number of entries");
            let series = match *layout {
                "v6" => iterate_and_seek::<Bucket>(Layout::V6, entries),
                "v5" => iterate_and_seek::<KeySpace>(Layout::V5, entries),
                other => panic!("no layout {other}"),
            };
            for xs in series {
                println!("{}", list_exact(&xs));
            }
        }
        other => panic!(
            "usage: [replay <variant> <insert> <entries> | iterate <v5|v6> <entries>]: {other:?}"
        ),
    }
}

/// `xs` with every digit, for the driver to read back.
fn list_exact(xs: &[f64]) -> String {
    let xs: Vec<String> = xs.iter().map(|x| x.to_string()).collect();
    xs.join(" ")
}
