//! Tests of `SkipMap::with_finger` (SlateDB fork).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crossbeam_skiplist::map::FingerStats;
use crossbeam_skiplist::SkipMap;
use crossbeam_utils::thread;

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

fn contents<V: Clone>(map: &SkipMap<u64, V>) -> Vec<(u64, V)> {
    map.iter().map(|e| (*e.key(), e.value().clone())).collect()
}

fn insert_sorted(map: &SkipMap<u64, u64>, keys: &[u64]) -> FingerStats {
    map.with_finger(|finger| {
        for &key in keys {
            finger.insert(key, key * 10);
        }
        finger.stats()
    })
}

#[test]
fn empty_list() {
    let map = SkipMap::new();
    let stats = insert_sorted(&map, &[3, 5, 8]);
    assert_eq!(
        stats,
        FingerStats {
            from_finger: 2,
            empty: 1,
            out_of_order: 0,
            stale: 0,
        }
    );
    assert_eq!(contents(&map), vec![(3, 30), (5, 50), (8, 80)]);

    // A finger that inserts nothing leaves the map as it is.
    let stats = map.with_finger(|finger| finger.stats());
    assert_eq!(stats, FingerStats::default());
    assert_eq!(map.len(), 3);
}

#[test]
fn ascending_batches_spread_over_the_list() {
    let map = SkipMap::new();
    let mut expected = BTreeMap::new();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    for _ in 0..200 {
        let mut batch: Vec<u64> = (0..50).map(|_| rng.next() % 1_000_000).collect();
        batch.sort_unstable();
        batch.dedup();
        let stats = insert_sorted(&map, &batch);
        assert_eq!(stats.empty, 1);
        assert_eq!(stats.from_finger, batch.len() as u64 - 1);
        assert_eq!(stats.out_of_order + stats.stale, 0);
        for key in batch {
            expected.insert(key, key * 10);
        }
    }
    assert_eq!(map.len(), expected.len());
    assert_eq!(contents(&map), expected.into_iter().collect::<Vec<_>>());
}

#[test]
fn first_key_below_the_finger() {
    let map = SkipMap::new();
    insert_sorted(&map, &[10, 20, 30]);
    let stats = map.with_finger(|finger| {
        finger.insert(25, 250);
        // Below the finger (now at 25): a search from the head.
        finger.insert(5, 50);
        // Above the finger (now at 5).
        finger.insert(15, 150);
        finger.insert(40, 400);
        finger.stats()
    });
    assert_eq!(
        stats,
        FingerStats {
            from_finger: 2,
            empty: 1,
            out_of_order: 1,
            stale: 0,
        }
    );
    assert_eq!(
        contents(&map),
        vec![
            (5, 50),
            (10, 100),
            (15, 150),
            (20, 200),
            (25, 250),
            (30, 300),
            (40, 400)
        ]
    );
}

#[test]
fn equal_keys() {
    let map = SkipMap::new();
    let stats = map.with_finger(|finger| {
        finger.insert(1, 10);
        finger.insert(2, 20);
        // Equal to the finger's key: replaced, through a search from the head.
        finger.insert(2, 21);
        // Equal to a key behind the finger: also from the head.
        finger.insert(1, 11);
        // `compare_fn` returning false keeps the entry there.
        finger.compare_insert(1, 12, |_| false);
        finger.insert(3, 30);
        finger.stats()
    });
    assert_eq!(stats.out_of_order, 3);
    assert_eq!(stats.stale, 0);
    assert_eq!(contents(&map), vec![(1, 11), (2, 21), (3, 30)]);
    assert_eq!(map.len(), 3);
}

#[test]
fn many_levels() {
    // 2^19 keys in all make towers of about 19 levels: one ascending batch of 2^18 keys, then a
    // second one of 2^18 keys interleaved with the first.
    let map = SkipMap::new();
    let first: Vec<u64> = (0..1 << 18).map(|k| k * 2).collect();
    let stats = insert_sorted(&map, &first);
    assert_eq!(stats.from_finger, first.len() as u64 - 1);
    let second: Vec<u64> = (0..1 << 18).map(|k| k * 2 + 1).collect();
    let stats = insert_sorted(&map, &second);
    assert_eq!(stats.from_finger, second.len() as u64 - 1);
    assert_eq!(map.len(), 1 << 19);
    for (at, entry) in map.iter().enumerate() {
        assert_eq!(*entry.key(), at as u64);
        assert_eq!(*entry.value(), at as u64 * 10);
    }
}

#[test]
fn unsorted_batch_is_correct() {
    let map = SkipMap::new();
    let mut expected = BTreeMap::new();
    let mut rng = Rng(7);
    for _ in 0..100 {
        let batch: Vec<u64> = (0..64).map(|_| rng.next() % 5_000).collect();
        let stats = insert_sorted(&map, &batch);
        assert_eq!(
            stats.from_finger + stats.empty + stats.out_of_order,
            batch.len() as u64
        );
        for key in batch {
            expected.insert(key, key * 10);
        }
    }
    assert_eq!(contents(&map), expected.into_iter().collect::<Vec<_>>());
}

#[test]
fn removed_finger_node_falls_back_to_the_head() {
    let map = SkipMap::new();
    let stats = map.with_finger(|finger| {
        finger.insert(10, 100);
        finger.insert(20, 200);
        // The node the finger holds is removed under it.
        assert!(map.remove(&20).is_some());
        finger.insert(30, 300);
        finger.insert(40, 400);
        finger.stats()
    });
    assert_eq!(
        stats,
        FingerStats {
            from_finger: 2,
            empty: 1,
            out_of_order: 0,
            stale: 1,
        }
    );
    assert_eq!(contents(&map), vec![(10, 100), (30, 300), (40, 400)]);
}

#[test]
fn removed_predecessors_fall_back_or_are_unlinked() {
    // Every node the finger inserted is removed under it; the inserts after stay correct. Key 1
    // stays, so the list is not empty (a search of an empty list reads no finger node).
    let map = SkipMap::new();
    map.insert(1, 0);
    let keys: Vec<u64> = (0..1_000).map(|k| k * 2).collect();
    let stats = map.with_finger(|finger| {
        for &key in &keys {
            finger.insert(key, key * 10);
        }
        for &key in &keys {
            assert!(map.remove(&key).is_some());
        }
        for key in 0..1_000 {
            finger.insert(2_000 + key, key);
        }
        finger.stats()
    });
    assert_eq!(stats.stale, 1);
    assert_eq!(map.len(), 1_001);
    let keys: Vec<u64> = map.iter().map(|e| *e.key()).collect();
    assert_eq!(
        keys,
        [1].into_iter().chain(2_000..3_000).collect::<Vec<_>>()
    );
}

#[test]
fn concurrent_fingers_removals_and_readers() {
    // Keys 3k start in the map and are removed by one thread. Keys 3k + 1 are inserted by a
    // finger in ascending batches, and keys 3k + 2 by a second finger, while readers scan.
    const N: u64 = 200_000;
    let map = SkipMap::new();
    for k in 0..N {
        map.insert(3 * k, 0);
    }
    // Writers that have finished; the readers stop when all three have.
    let done = AtomicUsize::new(0);
    thread::scope(|s| {
        for residue in [1, 2] {
            let (map, done) = (&map, &done);
            s.spawn(move |_| {
                for batch in 0..N / 100 {
                    map.with_finger(|finger| {
                        for k in batch * 100..(batch + 1) * 100 {
                            finger.insert(3 * k + residue, 3 * k + residue);
                        }
                    });
                }
                done.fetch_add(1, Ordering::Release);
            });
        }
        s.spawn(|_| {
            for k in 0..N {
                map.remove(&(3 * k));
            }
            done.fetch_add(1, Ordering::Release);
        });
        for _ in 0..2 {
            s.spawn(|_| {
                while done.load(Ordering::Acquire) < 3 {
                    let mut previous = None;
                    for entry in map.iter() {
                        let key = *entry.key();
                        assert!(previous < Some(key), "a scan out of order");
                        previous = Some(key);
                    }
                }
            });
        }
    })
    .unwrap();
    let keys: Vec<u64> = map.iter().map(|e| *e.key()).collect();
    let expected: Vec<u64> = (0..3 * N).filter(|k| k % 3 != 0).collect();
    assert_eq!(keys, expected);
    for entry in map.iter() {
        assert_eq!(entry.key(), entry.value());
    }
}

#[test]
fn concurrent_removals_of_the_fingers_own_nodes() {
    // One finger inserts ascending keys. A second thread removes the newest node, which is most
    // often the node the finger holds, and on alternate passes the node below it. Readers scan
    // all the time. A round counts only if a search from the finger met a removed node.
    const N: u64 = 50_000;
    // Stops the other threads when the finger is done, also if it panics.
    struct Done<'a>(&'a AtomicBool);
    impl Drop for Done<'_> {
        fn drop(&mut self) {
            self.0.store(false, Ordering::Release);
        }
    }
    for round in 0..50 {
        let map = SkipMap::new();
        map.insert(0u64, 0u64);
        let inserting = AtomicBool::new(true);
        let (stats, removed) = std::thread::scope(|s| {
            let remover = s.spawn(|| {
                let mut removed = Vec::new();
                let mut below = false;
                while inserting.load(Ordering::Acquire) {
                    if let Some(newest) = map.back() {
                        let entry = if below { newest.prev() } else { Some(newest) };
                        below = !below;
                        if let Some(entry) = entry {
                            let key = *entry.key();
                            if key != 0 && entry.remove() {
                                removed.push(key);
                            }
                        }
                    }
                }
                removed
            });
            for _ in 0..2 {
                s.spawn(|| {
                    while inserting.load(Ordering::Acquire) {
                        let mut previous = None;
                        for entry in map.iter() {
                            let key = *entry.key();
                            assert!(previous < Some(key), "a scan out of order");
                            previous = Some(key);
                        }
                    }
                });
            }
            let stats = {
                let _done = Done(&inserting);
                map.with_finger(|finger| {
                    for key in 1..=N {
                        finger.insert(key, key);
                    }
                    finger.stats()
                })
            };
            (stats, remover.join().unwrap())
        });
        // The first insert finds the finger empty; every later key is above the previous one.
        assert_eq!((stats.empty, stats.out_of_order), (1, 0));
        assert_eq!(stats.from_finger + stats.stale, N - 1);
        let removed_set: std::collections::BTreeSet<u64> = removed.iter().copied().collect();
        assert_eq!(removed_set.len(), removed.len(), "a key removed twice");
        let keys: Vec<u64> = map.iter().map(|e| *e.key()).collect();
        let expected: Vec<u64> = (0..=N).filter(|k| !removed_set.contains(k)).collect();
        assert_eq!(keys, expected);
        for entry in map.iter() {
            assert_eq!(entry.key(), entry.value());
        }
        if stats.stale > 0 {
            return;
        }
        assert!(
            round < 49,
            "no round's search from the finger met a removed node"
        );
    }
}
