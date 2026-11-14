# SlateDB fork note

This directory is `crossbeam-skiplist` 0.1.3 from crates.io (crossbeam git commit
`3aa2598e3943c791e68d668d9f92e84e748bab65`, see `.cargo_vcs_info.json`), with one change: a
finger insert for batches of keys in ascending order. SlateDB's memtable uses it for a write
batch (`KVTable::put_sorted`).

The change, all marked "SlateDB fork" in the source:

- `src/base.rs`: `Finger`, `FingerStats`, `SkipList::finger`, `SkipList::search_position_from`,
  and a `finger` argument to `insert_internal` (the first search starts from the finger, and the
  node linked is recorded in it).
- `src/map.rs`: `SkipMap::with_finger` and `map::Finger`.
- `tests/finger.rs`: tests of the finger.
- `src/base.rs`, `IntoIter`: two explicit `&` for the `dangerous_implicit_autorefs` lint, which is
  deny by default since Rust 1.89. Cargo does not cap lints for a path dependency.

The upstream `benches` directory (nightly only) and `Cargo.toml.orig` are left out. The licenses
are unchanged. To see the full change against upstream:

```sh
cargo download crossbeam-skiplist==0.1.3   # or unpack ~/.cargo/registry/src/*/crossbeam-skiplist-0.1.3
diff -ru crossbeam-skiplist-0.1.3 vendor/crossbeam-skiplist
```

The fork versions the crate `0.1.3+slatedb.1`. It is outside SlateDB's workspace (`exclude` in the
root `Cargo.toml`). Run its tests with
`cargo test --release --manifest-path vendor/crossbeam-skiplist/Cargo.toml`; `fork/sync.sh` runs
them too. SlateDB's lib suite has copies of the stale-finger tests
(`mem_table::skiplist_finger_tests`), so the workspace's test runs cover them.

---

# Crossbeam Skiplist

[![Build Status](https://github.com/crossbeam-rs/crossbeam/workflows/CI/badge.svg)](
https://github.com/crossbeam-rs/crossbeam/actions)
[![License](https://img.shields.io/badge/license-MIT_OR_Apache--2.0-blue.svg)](
https://github.com/crossbeam-rs/crossbeam/tree/master/crossbeam-skiplist#license)
[![Cargo](https://img.shields.io/crates/v/crossbeam-skiplist.svg)](
https://crates.io/crates/crossbeam-skiplist)
[![Documentation](https://docs.rs/crossbeam-skiplist/badge.svg)](
https://docs.rs/crossbeam-skiplist)
[![Rust 1.61+](https://img.shields.io/badge/rust-1.61+-lightgray.svg)](
https://www.rust-lang.org)
[![chat](https://img.shields.io/discord/569610676205781012.svg?logo=discord)](https://discord.com/invite/JXYwgWZ)

This crate provides the types [`SkipMap`] and [`SkipSet`].
These data structures provide an interface similar to `BTreeMap` and `BTreeSet`,
respectively, except they support safe concurrent access across multiple threads.

This crate can be used in `no_std` environments that implement `alloc`. The `alloc` feature of this crate needs to be enabled in `no_std` environments.

[`SkipMap`]: https://docs.rs/crossbeam-skiplist/latest/crossbeam_skiplist/struct.SkipMap.html
[`SkipSet`]: https://docs.rs/crossbeam-skiplist/latest/crossbeam_skiplist/struct.SkipSet.html

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
crossbeam-skiplist = "0.1"
```

## Compatibility

Crossbeam Skiplist supports stable Rust releases going back at least six months,
and every time the minimum supported Rust version is increased, a new minor
version is released. Currently, the minimum supported Rust version is 1.61.

## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

#### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
