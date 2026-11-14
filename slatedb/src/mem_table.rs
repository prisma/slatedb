use std::cell::Cell;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
#[cfg(test)]
use crossbeam_skiplist::map::FingerStats;
use crossbeam_skiplist::map::Range;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering::SeqCst;

use chrono::{DateTime, Utc};
use parking_lot::Mutex;

use crate::db_common::extract_segment_prefix;
use crate::error::SlateDBError;
use crate::iter::{IterationOrder, RowEntryIterator};
use crate::prefix_extractor::PrefixExtractor;
use crate::read_trace::ReadTrace;
use crate::seq_tracker::{SequenceTracker, TrackedSeq};
use crate::types::RowEntry;
use crate::utils::{WatchableOnceCell, WatchableOnceCellReader};

/// Memtable may contains multiple versions of a single user key, with a monotonically increasing sequence number.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SequencedKey {
    pub(crate) user_key: Bytes,
    pub(crate) seq: u64,
}

impl SequencedKey {
    pub(crate) fn new(user_key: Bytes, seq: u64) -> Self {
        Self { user_key, seq }
    }
}

impl Ord for SequencedKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.user_key
            .cmp(&other.user_key)
            .then(self.seq.cmp(&other.seq).reverse())
    }
}

impl PartialOrd for SequencedKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct KVTableInternalKeyRange {
    start_bound: Bound<SequencedKey>,
    end_bound: Bound<SequencedKey>,
}

impl RangeBounds<SequencedKey> for KVTableInternalKeyRange {
    fn start_bound(&self) -> Bound<&SequencedKey> {
        self.start_bound.as_ref()
    }

    fn end_bound(&self) -> Bound<&SequencedKey> {
        self.end_bound.as_ref()
    }
}

/// Convert a user key range to a memtable internal key range. The internal key range should contain all the sequence
/// numbers for the given user key in the range. This is used for iterating over the memtable in [`KVTable::range`].
///
/// Please note that the sequence number is ordered in reverse, given a user key range (`key001`..=`key001`), the first
/// sequence number in this range is u64::MAX, and the last sequence number is 0. The output range should be
/// `(key001, u64::MAX) ..= (key001, 0)`.
impl<T: RangeBounds<Bytes>> From<T> for KVTableInternalKeyRange {
    fn from(range: T) -> Self {
        let start_bound = match range.start_bound() {
            Bound::Included(key) => Bound::Included(SequencedKey::new(key.clone(), u64::MAX)),
            Bound::Excluded(key) => Bound::Excluded(SequencedKey::new(key.clone(), 0)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end_bound = match range.end_bound() {
            Bound::Included(key) => Bound::Included(SequencedKey::new(key.clone(), 0)),
            Bound::Excluded(key) => Bound::Excluded(SequencedKey::new(key.clone(), u64::MAX)),
            Bound::Unbounded => Bound::Unbounded,
        };
        Self {
            start_bound,
            end_bound,
        }
    }
}

pub(crate) struct KVTable {
    map: Arc<SkipMap<SequencedKey, RowEntry>>,
    durable: WatchableOnceCell<Result<(), SlateDBError>>,
    entries_size_in_bytes: AtomicUsize,
    /// this corresponds to the timestamp of the most recent
    /// modifying operation on this KVTable (insertion or deletion)
    last_tick: AtomicI64,
    /// the sequence number of the most recent operation on this KVTable
    last_seq: AtomicU64,
    /// the sequence number of the oldest entry in this KVTable
    first_seq: AtomicU64,
    /// A sequence tracker that correlates sequence numbers with system clock ticks.
    /// The tracker is limited to 8192 entries and downsamples data when it gets full.
    sequence_tracker: Mutex<SequenceTracker>,
    /// RFC-0024: distinct segment prefixes touched by writes that have
    /// landed in this table. Populated by the write and WAL-replay
    /// paths after the antichain check. Empty when no extractor is
    /// configured.
    touched_segments: Mutex<std::collections::BTreeSet<Bytes>>,
    /// Debug builds: the `put` and `put_sorted` calls in progress, which must be at most one (see
    /// [`KVTable::put_sorted`]).
    #[cfg(debug_assertions)]
    writers: AtomicUsize,
    /// Tests: how this table's inserts went (see [`InsertCounts`]).
    #[cfg(test)]
    insert_counts: Mutex<InsertCounts>,
}

/// Tests: how a table's inserts went.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct InsertCounts {
    /// Inserts that replaced an entry with an equal (key, seq), which removes the old node.
    pub(crate) replaced: usize,
    /// The finger statistics of every `put_sorted`, summed.
    pub(crate) finger: FingerStats,
}

/// Held by a `put` or `put_sorted` while it inserts. Debug builds check that a table has at most
/// one at a time.
struct WriterToken<'a> {
    #[cfg(debug_assertions)]
    writers: &'a AtomicUsize,
    #[cfg(not(debug_assertions))]
    _table: std::marker::PhantomData<&'a ()>,
}

impl Drop for WriterToken<'_> {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        self.writers.fetch_sub(1, SeqCst);
    }
}

pub(crate) struct KVTableMetadata {
    pub(crate) entry_num: usize,
    pub(crate) entries_size_in_bytes: usize,
    /// this corresponds to the timestamp of the most recent
    /// modifying operation on this KVTable (insertion or deletion)
    #[allow(dead_code)]
    pub(crate) last_tick: i64,
    /// the sequence number of the most recent operation on this KVTable
    pub(crate) last_seq: u64,
    /// the sequence number of the oldest entry in this KVTable
    pub(crate) first_seq: u64,
}

pub(crate) struct WritableKVTable {
    table: Arc<KVTable>,
}

impl WritableKVTable {
    pub(crate) fn new() -> Self {
        Self {
            table: Arc::new(KVTable::new()),
        }
    }

    pub(crate) fn table(&self) -> &Arc<KVTable> {
        &self.table
    }

    pub(crate) fn put(&self, row: RowEntry) {
        self.table.put(row);
    }

    /// Delegate to [`KVTable::put_sorted`].
    pub(crate) fn put_sorted(&self, rows: impl IntoIterator<Item = RowEntry>) {
        self.table.put_sorted(rows);
    }

    pub(crate) fn metadata(&self) -> KVTableMetadata {
        self.table.metadata()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    pub(crate) fn record_sequence(&self, seq: u64, ts: DateTime<Utc>) {
        self.table.record_sequence(seq, ts);
    }

    /// Delegate to [`KVTable::record_touched_segments`].
    pub(crate) fn record_touched_segments(&self, prefixes: std::collections::BTreeSet<Bytes>) {
        self.table.record_touched_segments(prefixes);
    }
}

pub(crate) struct ImmutableMemtable {
    /// A WAL ID for which it is guaranteed that all writes in the contained WAL object are present
    /// in this memtable. This is used to determine a safe replay point when this IMM is flushed to
    /// a new L0. On a restart, only WALs after this id may contain data that is not present in the
    /// tree.
    ///
    /// Please note that this WAL ID might not exactly match the last WAL ID that produced this
    /// IMM, so we still need to take the last l0's `last_seq` to filter out the entries that are
    /// already contained in the last L0 SST.
    replay_after_wal_id: u64,
    table: Arc<KVTable>,
    /// Notified when the memtable's SST has been uploaded to object storage.
    /// Used to release backpressure on writers when unflushed bytes are too high.
    uploaded: WatchableOnceCell<Result<(), SlateDBError>>,
    /// A snapshot of the sequence tracker taken when this immutable memtable was created.
    /// This avoids needing to access the sequence tracker through a mutex on the underlying table.
    sequence_tracker: SequenceTracker,
}

#[self_referencing]
pub(crate) struct MemTableIteratorInner<T: RangeBounds<SequencedKey>> {
    map: Arc<SkipMap<SequencedKey, RowEntry>>,
    /// `inner` is the Iterator impl of SkipMap, which is the underlying data structure of MemTable.
    #[borrows(map)]
    #[not_covariant]
    inner: Range<'this, SequencedKey, T, SequencedKey, RowEntry>,
    ordering: IterationOrder,
    item: Option<RowEntry>,
    /// Stack used in descending mode to re-order entries within a key group.
    /// When iterating descending, `next_back()` yields entries for the same key
    /// in seq-ascending order. Pushing them onto this stack and popping gives
    /// seq-descending order, which is what the merge iterator needs for dedup.
    descending_stack: Vec<RowEntry>,
    read_trace: ReadTrace,
    /// End bound of the original range, kept for `seek` because `Range` does not expose it.
    end_bound: Bound<SequencedKey>,
}
pub(crate) type MemTableIterator = MemTableIteratorInner<KVTableInternalKeyRange>;

#[async_trait]
impl RowEntryIterator for MemTableIterator {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        Ok(self.next_sync())
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        // DbIterator::seek rejects descending scans before it reaches the memtable.
        match self.borrow_ordering() {
            IterationOrder::Ascending => {
                self.seek_ascending(next_key);
                // Keep in-memory seeking cooperative.
                tokio::task::coop::consume_budget().await;
                Ok(())
            }
            IterationOrder::Descending => Err(SlateDBError::SeekNotSupportedForDescendingScan),
        }
    }
}

impl MemTableIterator {
    /// Moves to the first entry with a key at or after `next_key` in O(log n).
    ///
    /// A crossbeam `Range` searches the skiplist only on its first `next()`, so this
    /// replaces `inner` with a new range starting at `next_key`.
    fn seek_ascending(&mut self, next_key: &[u8]) {
        // Do not move backwards or revive an exhausted iterator.
        if self
            .borrow_item()
            .as_ref()
            .is_none_or(|record| record.key >= next_key)
        {
            return;
        }
        // u64::MAX starts at the newest version; see `KVTableInternalKeyRange::from`.
        let range = KVTableInternalKeyRange {
            start_bound: Bound::Included(SequencedKey::new(
                Bytes::copy_from_slice(next_key),
                u64::MAX,
            )),
            end_bound: self.borrow_end_bound().clone(),
        };
        self.with_mut(|fields| {
            *fields.inner = fields.map.range(range);
            *fields.item = None;
        });
        self.next_sync();
    }

    pub(crate) fn next_sync(&mut self) -> Option<RowEntry> {
        let span = self.borrow_read_trace().new_memtable_span();
        let _guard = span.enter();
        match self.borrow_ordering() {
            IterationOrder::Ascending => self.next_ascending(),
            IterationOrder::Descending => self.next_descending(),
        }
    }

    fn next_ascending(&mut self) -> Option<RowEntry> {
        let ans = self.borrow_item().clone();
        let next_entry = self.with_inner_mut(|inner| inner.next());
        let cloned_next = next_entry.map(|entry| entry.value().clone());
        self.with_item_mut(|item| *item = cloned_next);
        ans
    }

    fn next_descending(&mut self) -> Option<RowEntry> {
        // If the stack has entries, pop from it (LIFO gives seq-descending).
        let top = self.with_descending_stack_mut(|stack| stack.pop());
        if top.is_some() {
            return top;
        }

        // Stack is empty. The current `item` (if any) is the start of a new
        // key group. Collect all entries for this key from the SkipMap.
        let first = self.borrow_item().clone();
        match first {
            Some(first) => {
                let current_key = first.key.clone();

                // Collect remaining entries with the same key. next_back()
                // yields them in seq-ascending order. Pushing onto the stack
                // and popping gives seq-descending order.
                self.with_descending_stack_mut(|stack| stack.push(first));
                self.fill_descending_stack(&current_key);

                self.with_descending_stack_mut(|stack| stack.pop())
            }
            None => {
                // item is None — either iterator is exhausted, or this is the
                // priming call. Advance inner to populate item for the next call.
                let next = self
                    .with_inner_mut(|inner| inner.next_back().map(|entry| entry.value().clone()));
                self.with_item_mut(|item| *item = next);
                None
            }
        }
    }

    fn fill_descending_stack(&mut self, current_key: &Bytes) {
        loop {
            let next = self.with_inner_mut(|inner| {
                inner
                    .next_back()
                    .map(|entry| (entry.key().user_key.clone(), entry.value().clone()))
            });
            match next {
                Some((key, row)) if key == *current_key => {
                    self.with_descending_stack_mut(|stack| stack.push(row));
                }
                other => {
                    self.with_item_mut(|item| *item = other.map(|(_, row)| row));
                    break;
                }
            }
        }
    }
}

impl ImmutableMemtable {
    pub(crate) fn new(table: WritableKVTable, replay_after_wal_id: u64) -> Self {
        let sequence_tracker = table.table.sequence_tracker_snapshot();
        Self {
            table: table.table,
            replay_after_wal_id,
            uploaded: WatchableOnceCell::new(),
            sequence_tracker,
        }
    }

    /// Segment prefixes touched by writes in this imm.
    /// Delegate to [`KVTable::touched_segments`].
    pub(crate) fn touched_segments(&self) -> std::collections::BTreeSet<Bytes> {
        self.table.touched_segments()
    }

    pub(crate) fn table(&self) -> Arc<KVTable> {
        self.table.clone()
    }

    pub(crate) fn recent_flushed_wal_id(&self) -> u64 {
        self.replay_after_wal_id
    }

    pub(crate) async fn await_uploaded(&self) -> Result<(), SlateDBError> {
        self.uploaded.reader().await_value().await
    }

    pub(crate) fn notify_uploaded(&self, result: Result<(), SlateDBError>) {
        self.uploaded.write(result);
    }

    pub(crate) fn sequence_tracker(&self) -> &SequenceTracker {
        &self.sequence_tracker
    }

    /// Returns a new [`ImmutableMemtable`] that only contains entries with sequence
    /// number greater than the given `seq`. [`ImmutableMemtable::recent_flushed_wal_id`]
    /// remains the same.
    ///
    /// When `segment_extractor` is `Some`, the surviving entries are run
    /// through it to rebuild the touched-segment set (RFC-0024) from
    /// scratch — the same derivation the writer applies in
    /// [`WriteBatch::extract_entries`], via the shared
    /// [`extract_segment_prefix`]. Prefixes whose every row was filtered
    /// out are therefore dropped, and no row's prefix can survive without
    /// its row. An empty/absent prefix is an `EmptySegmentPrefix` error,
    /// matching the write and replay paths. When `segment_extractor` is
    /// `None`, the new table records no segments.
    pub(crate) fn filter_after_seq(
        &self,
        seq: u64,
        segment_extractor: Option<&dyn PrefixExtractor>,
    ) -> Result<Self, SlateDBError> {
        let new_table = WritableKVTable::new();
        let mut surviving_segments = std::collections::BTreeSet::new();
        let mut table_iter = self.table.iter();
        while let Some(entry) = table_iter.next_sync() {
            if entry.seq > seq {
                if let Some(extractor) = segment_extractor {
                    surviving_segments.insert(extract_segment_prefix(extractor, &entry.key)?);
                }
                new_table.put(entry);
            }
        }
        new_table.record_touched_segments(surviving_segments);
        Ok(Self::new(new_table, self.replay_after_wal_id))
    }
}

impl KVTable {
    pub(crate) fn new() -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            entries_size_in_bytes: AtomicUsize::new(0),
            durable: WatchableOnceCell::new(),
            last_tick: AtomicI64::new(i64::MIN),
            last_seq: AtomicU64::new(0),
            first_seq: AtomicU64::new(u64::MAX),
            sequence_tracker: Mutex::new(SequenceTracker::new()),
            touched_segments: Mutex::new(std::collections::BTreeSet::new()),
            #[cfg(debug_assertions)]
            writers: AtomicUsize::new(0),
            #[cfg(test)]
            insert_counts: Mutex::new(InsertCounts::default()),
        }
    }

    /// Merge a batch's touched-segment prefixes into this table's
    /// running set. The caller's set is moved in, so the first batch
    /// against an empty table is a direct adoption rather than an
    /// element-wise extend.
    pub(crate) fn record_touched_segments(&self, prefixes: std::collections::BTreeSet<Bytes>) {
        if prefixes.is_empty() {
            return;
        }
        let mut guard = self.touched_segments.lock();
        if guard.is_empty() {
            *guard = prefixes;
        } else {
            guard.extend(prefixes);
        }
    }

    /// Snapshot of the segment prefixes touched by writes in this
    /// table. Cheap clone (cardinality is bounded by the number of
    /// distinct active segments — typically one).
    pub(crate) fn touched_segments(&self) -> std::collections::BTreeSet<Bytes> {
        self.touched_segments.lock().clone()
    }

    /// True iff this table's touched-segment set is empty *or* contains
    /// `prefix`. Used by per-prefix reservation accounting on the
    /// dispatch tracker, where an empty set is treated conservatively
    /// as "may target any prefix" (because we can't rule it out).
    /// Avoids cloning the full set when a single membership check
    /// suffices.
    pub(crate) fn touched_segments_empty_or_contains(&self, prefix: &Bytes) -> bool {
        let touched = self.touched_segments.lock();
        touched.is_empty() || touched.contains(prefix)
    }

    /// Validate `prefix` against this table's touched-segment set
    /// (RFC-0024 antichain). Returns `Ok(true)` if `prefix` is already
    /// an exact member — it was validated when first inserted, so the
    /// caller can skip checking it against older sources. Returns
    /// `Ok(false)` if `prefix` is disjoint from every recorded
    /// prefix; the caller must continue validating against
    /// downstream sources.
    ///
    /// Only the immediate sort-order neighbors in `touched_segments`
    /// can nest with `prefix`: the predecessor (largest `<= prefix`)
    /// is the only candidate ancestor, and the successor (smallest
    /// `> prefix`) is the only candidate descendant. Every other
    /// existing prefix would itself nest with one of those neighbors,
    /// which is impossible because `touched_segments` is already an
    /// antichain by induction on prior writes.
    pub(crate) fn ensure_valid_segment(&self, prefix: &Bytes) -> Result<bool, SlateDBError> {
        let touched = self.touched_segments.lock();
        if touched.is_empty() {
            return Ok(false);
        }
        if let Some(pred) = touched.range::<Bytes, _>(..=prefix).next_back() {
            if pred == prefix {
                return Ok(true);
            }
            if prefix.starts_with(pred.as_ref()) {
                return Err(SlateDBError::InvalidSegmentPrefix {
                    prefix: prefix.clone(),
                    conflict: pred.clone(),
                });
            }
        }
        if let Some(succ) = touched
            .range::<Bytes, _>((Bound::Excluded(prefix), Bound::Unbounded))
            .next()
        {
            if succ.starts_with(prefix.as_ref()) {
                return Err(SlateDBError::InvalidSegmentPrefix {
                    prefix: prefix.clone(),
                    conflict: succ.clone(),
                });
            }
        }
        Ok(false)
    }

    pub(crate) fn metadata(&self) -> KVTableMetadata {
        let entry_num = self.map.len();
        let entries_size_in_bytes = self.entries_size_in_bytes.load(Ordering::Relaxed);
        let last_tick = self.last_tick.load(SeqCst);
        let last_seq = self.last_seq().unwrap_or(0);
        let first_seq = self.first_seq().unwrap_or(0);
        KVTableMetadata {
            entry_num,
            entries_size_in_bytes,
            last_tick,
            last_seq,
            first_seq,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub(crate) fn last_tick(&self) -> i64 {
        self.last_tick.load(SeqCst)
    }

    pub(crate) fn last_seq(&self) -> Option<u64> {
        if self.is_empty() {
            None
        } else {
            let last_seq = self.last_seq.load(SeqCst);
            Some(last_seq)
        }
    }

    pub(crate) fn first_seq(&self) -> Option<u64> {
        if self.is_empty() {
            None
        } else {
            let first_seq = self.first_seq.load(SeqCst);
            Some(first_seq)
        }
    }

    pub(crate) fn iter(&self) -> MemTableIterator {
        self.range_ascending(..)
    }

    pub(crate) fn range_ascending<T: RangeBounds<Bytes>>(&self, range: T) -> MemTableIterator {
        self.range(range, IterationOrder::Ascending, ReadTrace::none())
    }

    pub(crate) fn range<T: RangeBounds<Bytes>>(
        &self,
        range: T,
        ordering: IterationOrder,
        read_trace: ReadTrace,
    ) -> MemTableIterator {
        let internal_range = KVTableInternalKeyRange::from(range);
        let end_bound = internal_range.end_bound.clone();
        let mut iterator = MemTableIteratorInnerBuilder {
            map: self.map.clone(),
            inner_builder: |map| map.range(internal_range),
            ordering,
            item: None,
            descending_stack: Vec::new(),
            read_trace,
            end_bound,
        }
        .build();
        iterator.next_sync();
        iterator
    }

    pub(crate) fn put(&self, row: RowEntry) {
        let _writer = self.enter_writer();
        let (internal_key, row_size) = self.record_row(&row);
        let previous_size = Cell::new(None);
        self.map.compare_insert(internal_key, row, |previous_row| {
            // Optimistically calculate the size of the previous value.
            // `compare_fn` might be called multiple times in case of concurrent
            // writes to the same key, so we use `Cell` to avoid subtracting
            // the size multiple times. The last call will set the correct size.
            previous_size.set(Some(previous_row.estimated_size()));
            true
        });
        self.account_insert(row_size, previous_size.take());
    }

    /// Inserts `rows` as [`put`](Self::put) does, one at a time, but starts the skip-list search
    /// for each row where the previous row's insert linked its node (a finger), not at the head.
    /// The write path calls it with a write batch, whose rows are in ascending key order with one
    /// sequence number. The search for each row then starts near the previous row's position.
    ///
    /// Rows need not be sorted. A row at or below the previous row in `SequencedKey` order
    /// (out of order, or an equal key and sequence number) is searched for from the head, and is
    /// inserted exactly as `put` inserts it.
    ///
    /// # Why the finger is never stale here
    ///
    /// The skip list's finger is correct with any concurrent readers, writers and removals (see
    /// `crossbeam_skiplist::base::Finger`): a removed node it holds sends the insert back to the
    /// head. This table also keeps the finger from ever needing that fallback:
    ///
    /// - One writer. Every row written to a mutable memtable comes from the batch writer task
    ///   (`write_entries_to_memtable`), under the state read lock, so a freeze (write lock)
    ///   cannot happen while a batch is inserted. WAL replay and `filter_after_seq` write to a
    ///   new table before any reader or writer can see it. Debug builds check that a table has at
    ///   most one `put` or `put_sorted` in progress.
    /// - No removals. Nothing removes from the map: the only mutation is `compare_insert`. Its
    ///   replace path removes the old node only for an equal (key, seq). A write batch has unique
    ///   keys and one sequence number, and every batch has a new, higher sequence number. If an
    ///   equal (key, seq) still came, the replace runs on a head search, which unlinks the old
    ///   node before the insert returns.
    /// - Readers unlink only nodes that are marked as removed, so with no removals they change
    ///   nothing.
    /// - The finger lives for one call, under one pinned epoch guard, so the nodes it holds
    ///   cannot be freed while it is used.
    ///
    /// A stale finger is handled in two places. The skip list always falls back to a head search,
    /// because it must stay correct for any caller. Here, debug builds also assert that no insert
    /// fell back for that reason: in this table it can only mean a broken invariant (a second
    /// writer or a removal), which tests must catch. Release builds keep the correct fallback.
    pub(crate) fn put_sorted(&self, rows: impl IntoIterator<Item = RowEntry>) {
        let _writer = self.enter_writer();
        let stats = self.map.with_finger(|finger| {
            for row in rows {
                let (internal_key, row_size) = self.record_row(&row);
                let previous_size = Cell::new(None);
                finger.compare_insert(internal_key, row, |previous_row| {
                    // As in `put`: the last call sets the size of the row replaced.
                    previous_size.set(Some(previous_row.estimated_size()));
                    true
                });
                self.account_insert(row_size, previous_size.take());
            }
            finger.stats()
        });
        debug_assert_eq!(
            stats.stale, 0,
            "a memtable insert met a removed node: the memtable has one writer and no removals"
        );
        #[cfg(test)]
        {
            let mut counts = self.insert_counts.lock();
            counts.finger.from_finger += stats.from_finger;
            counts.finger.empty += stats.empty;
            counts.finger.out_of_order += stats.out_of_order;
            counts.finger.stale += stats.stale;
        }
    }

    /// Tests: how this table's inserts went.
    #[cfg(test)]
    pub(crate) fn insert_counts(&self) -> InsertCounts {
        *self.insert_counts.lock()
    }

    /// Marks a `put` or `put_sorted` in progress until the token is dropped. Debug builds panic if
    /// another one is in progress: a memtable has one writer.
    #[cfg(debug_assertions)]
    fn enter_writer(&self) -> WriterToken<'_> {
        let others = self.writers.fetch_add(1, SeqCst);
        // The token exists before the assert, so a panic here still undoes the `fetch_add`.
        let token = WriterToken {
            writers: &self.writers,
        };
        assert_eq!(
            others, 0,
            "a second writer entered a memtable while another was writing to it"
        );
        token
    }

    #[cfg(not(debug_assertions))]
    fn enter_writer(&self) -> WriterToken<'_> {
        WriterToken {
            _table: std::marker::PhantomData,
        }
    }

    /// Updates the table's ticks and sequence numbers for `row`, and returns its internal key
    /// and estimated size.
    fn record_row(&self, row: &RowEntry) -> (SequencedKey, usize) {
        // it is safe to use fetch_max here to update the last tick
        // because the monotonicity is enforced when generating the clock tick
        // (see [crate::utils::MonotonicClock::now])
        if let Some(create_ts) = row.create_ts {
            self.last_tick.fetch_max(create_ts, SeqCst);
        }
        // update the last seq number if it is greater than the current last seq
        self.last_seq.fetch_max(row.seq, SeqCst);
        // update the first seq number if it is smaller than the current first seq
        self.first_seq.fetch_min(row.seq, SeqCst);
        (
            SequencedKey::new(row.key.clone(), row.seq),
            row.estimated_size(),
        )
    }

    /// Adds an inserted row's size, less the size of the row it replaced, if any.
    fn account_insert(&self, row_size: usize, replaced_size: Option<usize>) {
        if let Some(size) = replaced_size {
            self.entries_size_in_bytes
                .fetch_sub(size, Ordering::Relaxed);
            #[cfg(test)]
            {
                self.insert_counts.lock().replaced += 1;
            }
        }
        self.entries_size_in_bytes
            .fetch_add(row_size, Ordering::Relaxed);
    }

    pub(crate) fn durable_watcher(&self) -> WatchableOnceCellReader<Result<(), SlateDBError>> {
        self.durable.reader()
    }

    pub(crate) fn notify_durable(&self, result: Result<(), SlateDBError>) {
        self.durable.write(result);
    }

    pub(crate) fn record_sequence(&self, seq: u64, ts: DateTime<Utc>) {
        let mut tracker = self.sequence_tracker.lock();
        tracker.insert(TrackedSeq { seq, ts });
    }

    pub(crate) fn sequence_tracker_snapshot(&self) -> SequenceTracker {
        self.sequence_tracker.lock().clone()
    }
}

#[cfg(feature = "bench-internal")]
pub mod benches {
    //! A memtable for `benches/memtable_put_sorted.rs`.

    use super::KVTable;
    use crate::types::{RowEntry, ValueDeletable};
    use bytes::Bytes;

    /// A memtable (`KVTable`) with its two insert paths.
    pub struct Memtable(KVTable);

    impl Default for Memtable {
        fn default() -> Self {
            Self(KVTable::new())
        }
    }

    impl Memtable {
        /// Inserts one row, searching from the skip list's head (`KVTable::put`).
        pub fn put(&self, key: Bytes, value: Bytes, seq: u64) {
            self.0.put(row(key, value, seq));
        }

        /// Inserts a batch's rows, each search starting from the previous insert
        /// (`KVTable::put_sorted`).
        pub fn put_sorted(&self, rows: impl IntoIterator<Item = (Bytes, Bytes)>, seq: u64) {
            self.0
                .put_sorted(rows.into_iter().map(|(key, value)| row(key, value, seq)));
        }

        /// The number of entries.
        pub fn len(&self) -> usize {
            self.0.metadata().entry_num
        }

        /// True if the memtable has no entries.
        pub fn is_empty(&self) -> bool {
            self.0.is_empty()
        }
    }

    fn row(key: Bytes, value: Bytes, seq: u64) -> RowEntry {
        RowEntry::new(key, ValueDeletable::Value(value), seq, None, None)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;
    use crate::bytes_range::BytesRange;
    use crate::merge_iterator::MergeIterator;
    use crate::prefix_extractor::PrefixTarget;
    use crate::proptest_util::{arbitrary, sample};
    use crate::test_utils::assert_iterator;
    use crate::{proptest_util, test_utils};
    use rstest::rstest;
    use tokio::runtime::Runtime;

    #[tokio::test]
    async fn test_memtable_iter() {
        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"abc333", b"value3", 1));
        table.put(RowEntry::new_value(b"abc111", b"value1", 2));
        table.put(RowEntry::new_value(b"abc555", b"value5", 3));
        table.put(RowEntry::new_value(b"abc444", b"value4", 4));
        table.put(RowEntry::new_value(b"abc222", b"value2", 5));
        assert_eq!(table.table().last_seq(), Some(5));
        assert_eq!(table.table().first_seq(), Some(1));

        let mut iter = table.table().iter();
        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(b"abc111", b"value1", 2),
                RowEntry::new_value(b"abc222", b"value2", 5),
                RowEntry::new_value(b"abc333", b"value3", 1),
                RowEntry::new_value(b"abc444", b"value4", 4),
                RowEntry::new_value(b"abc555", b"value5", 3),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_memtable_iter_entry_attrs() {
        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"abc333", b"value3", 1));
        table.put(RowEntry::new_value(b"abc111", b"value1", 2));

        let mut iter = table.table().iter();
        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(b"abc111", b"value1", 2),
                RowEntry::new_value(b"abc333", b"value3", 1),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_memtable_range_from_existing_key() {
        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"abc333", b"value3", 1));
        table.put(RowEntry::new_value(b"abc111", b"value1", 2));
        table.put(RowEntry::new_value(b"abc555", b"value5", 3));
        table.put(RowEntry::new_value(b"abc444", b"value4", 4));
        table.put(RowEntry::new_value(b"abc222", b"value2", 5));

        let mut iter = table
            .table()
            .range_ascending(BytesRange::from(Bytes::from_static(b"abc333")..));
        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(b"abc333", b"value3", 1),
                RowEntry::new_value(b"abc444", b"value4", 4),
                RowEntry::new_value(b"abc555", b"value5", 3),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_memtable_range_from_nonexisting_key() {
        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"abc333", b"value3", 1));
        table.put(RowEntry::new_value(b"abc111", b"value1", 2));
        table.put(RowEntry::new_value(b"abc555", b"value5", 3));
        table.put(RowEntry::new_value(b"abc444", b"value4", 4));
        table.put(RowEntry::new_value(b"abc222", b"value2", 5));

        let mut iter = table
            .table()
            .range_ascending(BytesRange::from(Bytes::from_static(b"abc334")..));
        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(b"abc444", b"value4", 4),
                RowEntry::new_value(b"abc555", b"value5", 3),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_memtable_iter_delete() {
        let table = WritableKVTable::new();
        table.put(RowEntry::new_tombstone(b"abc333", 2));
        table.put(RowEntry::new_value(b"abc333", b"value3", 1));
        table.put(RowEntry::new_value(b"abc444", b"value4", 4));

        // in merge iterator, it should only return one entry
        let iter = table.table().iter();
        let mut merge_iter = MergeIterator::new(VecDeque::from(vec![iter])).unwrap();
        assert_iterator(
            &mut merge_iter,
            vec![
                RowEntry::new_tombstone(b"abc333", 2),
                RowEntry::new_value(b"abc444", b"value4", 4),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_memtable_track_sz_and_num() {
        let table = WritableKVTable::new();
        let mut metadata = table.table().metadata();

        assert_eq!(metadata.entry_num, 0);
        assert_eq!(metadata.entries_size_in_bytes, 0);
        table.put(RowEntry::new_value(b"first", b"foo", 1));
        metadata = table.table().metadata();
        assert_eq!(metadata.entry_num, 1);
        assert_eq!(metadata.entries_size_in_bytes, 16);

        table.put(RowEntry::new_tombstone(b"first", 2));
        metadata = table.table().metadata();
        assert_eq!(metadata.entry_num, 2);
        assert_eq!(metadata.entries_size_in_bytes, 29);

        table.put(RowEntry::new_value(b"abc333", b"val1", 1));
        metadata = table.table().metadata();
        assert_eq!(metadata.entry_num, 3);
        assert_eq!(metadata.entries_size_in_bytes, 47);

        table.put(RowEntry::new_value(b"def456", b"blablabla", 2));
        metadata = table.table().metadata();
        assert_eq!(metadata.entry_num, 4);
        assert_eq!(metadata.entries_size_in_bytes, 70);

        table.put(RowEntry::new_value(b"def456", b"blabla", 3));
        metadata = table.table().metadata();
        assert_eq!(metadata.entry_num, 5);
        assert_eq!(metadata.entries_size_in_bytes, 90);

        table.put(RowEntry::new_tombstone(b"abc333", 4));
        metadata = table.table().metadata();
        assert_eq!(metadata.entry_num, 6);
        assert_eq!(metadata.entries_size_in_bytes, 104);
    }

    #[tokio::test]
    async fn test_memtable_track_seqs() {
        let table = WritableKVTable::new();
        let mut metadata = table.table().metadata();

        assert_eq!(metadata.last_seq, 0);
        assert_eq!(metadata.first_seq, 0);
        table.put(RowEntry::new_value(b"first", b"foo", 1));
        metadata = table.table().metadata();
        assert_eq!(metadata.last_seq, 1);
        assert_eq!(metadata.first_seq, 1);

        table.put(RowEntry::new_tombstone(b"first", 2));
        metadata = table.table().metadata();
        assert_eq!(metadata.last_seq, 2);
        assert_eq!(metadata.first_seq, 1);

        table.put(RowEntry::new_value(b"abc333", b"val1", 1));
        metadata = table.table().metadata();
        assert_eq!(metadata.last_seq, 2);
        assert_eq!(metadata.first_seq, 1);

        table.put(RowEntry::new_value(b"def456", b"blablabla", 2));
        metadata = table.table().metadata();
        assert_eq!(metadata.last_seq, 2);
        assert_eq!(metadata.first_seq, 1);

        table.put(RowEntry::new_value(b"def456", b"blabla", 3));
        metadata = table.table().metadata();
        assert_eq!(metadata.last_seq, 3);
        assert_eq!(metadata.first_seq, 1);

        table.put(RowEntry::new_tombstone(b"abc333", 4));
        metadata = table.table().metadata();
        assert_eq!(metadata.last_seq, 4);
        assert_eq!(metadata.first_seq, 1);
    }

    #[rstest]
    #[case(
        BytesRange::from(..),
        KVTableInternalKeyRange {
            start_bound: Bound::Unbounded,
            end_bound: Bound::Unbounded,
        },
        vec![SequencedKey::new(Bytes::from_static(b"abc111"), 1)],
        vec![]
    )]
    #[case(
        BytesRange::from(Bytes::from_static(b"abc111")..=Bytes::from_static(b"abc333")),
        KVTableInternalKeyRange {
            start_bound: Bound::Included(SequencedKey::new(Bytes::from_static(b"abc111"), u64::MAX)),
            end_bound: Bound::Included(SequencedKey::new(Bytes::from_static(b"abc333"), 0)),
        },
        vec![
            SequencedKey::new(Bytes::from_static(b"abc111"), 1),
            SequencedKey::new(Bytes::from_static(b"abc222"), 2),
            SequencedKey::new(Bytes::from_static(b"abc333"), 3),
            SequencedKey::new(Bytes::from_static(b"abc333"), 0),
            SequencedKey::new(Bytes::from_static(b"abc333"), u64::MAX),
        ],
        vec![SequencedKey::new(Bytes::from_static(b"abc444"), 4)]
    )]
    #[case(
        BytesRange::from(Bytes::from_static(b"abc222")..Bytes::from_static(b"abc444")),
        KVTableInternalKeyRange {
            start_bound: Bound::Included(SequencedKey::new(Bytes::from_static(b"abc222"), u64::MAX)),
            end_bound: Bound::Excluded(SequencedKey::new(Bytes::from_static(b"abc444"), u64::MAX)),
        },
        vec![
            SequencedKey::new(Bytes::from_static(b"abc222"), 1),
            SequencedKey::new(Bytes::from_static(b"abc333"), 2),
        ],
        vec![
            SequencedKey::new(Bytes::from_static(b"abc444"), 0),
            SequencedKey::new(Bytes::from_static(b"abc444"), u64::MAX),
            SequencedKey::new(Bytes::from_static(b"abc555"), u64::MAX),
        ]
    )]
    #[case(
        BytesRange::from(..=Bytes::from_static(b"abc333")),
        KVTableInternalKeyRange {
            start_bound: Bound::Unbounded,
            end_bound: Bound::Included(SequencedKey::new(Bytes::from_static(b"abc333"), 0)),
        },
        vec![
            SequencedKey::new(Bytes::from_static(b"abc111"), 1),
            SequencedKey::new(Bytes::from_static(b"abc222"), 2),
            SequencedKey::new(Bytes::from_static(b"abc333"), 3),
            SequencedKey::new(Bytes::from_static(b"abc333"), u64::MAX),
        ],
        vec![SequencedKey::new(Bytes::from_static(b"abc444"), 4)]
    )]
    fn test_from_internal_key_range(
        #[case] range: BytesRange,
        #[case] expected: KVTableInternalKeyRange,
        #[case] should_contains: Vec<SequencedKey>,
        #[case] should_not_contains: Vec<SequencedKey>,
    ) {
        let range = KVTableInternalKeyRange::from(range);
        assert_eq!(range, expected);
        for key in should_contains {
            assert!(range.contains(&key));
        }
        for key in should_not_contains {
            assert!(!range.contains(&key));
        }
    }

    #[test]
    fn should_iterate_arbitrary_range() {
        let mut runner = proptest_util::runner::new(file!(), None);
        let runtime = Runtime::new().unwrap();
        let sample_table = sample::table(runner.rng(), 500, 10);

        let kv_table = WritableKVTable::new();
        let mut seq = 1;
        for (key, value) in &sample_table {
            let row_entry = RowEntry::new_value(key, value, seq);
            kv_table.put(row_entry);
            seq += 1;
        }

        runner
            .run(
                &(arbitrary::nonempty_range(10), arbitrary::iteration_order()),
                |(range, ordering)| {
                    let mut kv_iter =
                        kv_table
                            .table
                            .range(range.clone(), ordering, ReadTrace::none());

                    runtime.block_on(test_utils::assert_ranged_kv_scan(
                        &sample_table,
                        &range,
                        ordering,
                        &mut kv_iter,
                    ));
                    Ok(())
                },
            )
            .unwrap();
    }

    fn touched(prefixes: &[&[u8]]) -> std::collections::BTreeSet<Bytes> {
        prefixes.iter().map(|p| Bytes::copy_from_slice(p)).collect()
    }

    /// Extracts a single-byte segment prefix, matching the 1-byte
    /// prefixes used by the `filter_after_seq` tests.
    #[derive(Debug)]
    struct SingleByteExtractor;

    impl PrefixExtractor for SingleByteExtractor {
        fn name(&self) -> &str {
            "single-byte-test-only"
        }
        fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
            let len = match target {
                PrefixTarget::Point(b) | PrefixTarget::Prefix(b) => b.len(),
            };
            (len >= 1).then_some(1)
        }
    }

    fn assert_invalid_segment_prefix(err: SlateDBError, prefix: &[u8], conflict: &[u8]) {
        match err {
            SlateDBError::InvalidSegmentPrefix {
                prefix: p,
                conflict: c,
            } => {
                assert_eq!(p.as_ref(), prefix, "unexpected prefix in error");
                assert_eq!(c.as_ref(), conflict, "unexpected conflict in error");
            }
            other => panic!("expected InvalidSegmentPrefix, got {other:?}"),
        }
    }

    #[test]
    fn ensure_valid_segment_returns_false_when_table_has_no_touched_segments() {
        let table = KVTable::new();
        assert!(!table
            .ensure_valid_segment(&Bytes::from_static(b"abc"))
            .unwrap());
    }

    #[test]
    fn ensure_valid_segment_returns_true_on_exact_match() {
        let table = KVTable::new();
        table.record_touched_segments(touched(&[b"abc"]));
        assert!(table
            .ensure_valid_segment(&Bytes::from_static(b"abc"))
            .unwrap());
    }

    #[test]
    fn ensure_valid_segment_returns_false_when_disjoint() {
        let table = KVTable::new();
        table.record_touched_segments(touched(&[b"abc", b"xyz"]));
        // "def" sorts between but does not nest with either neighbor.
        assert!(!table
            .ensure_valid_segment(&Bytes::from_static(b"def"))
            .unwrap());
    }

    #[test]
    fn ensure_valid_segment_rejects_when_predecessor_is_proper_prefix() {
        let table = KVTable::new();
        table.record_touched_segments(touched(&[b"abc"]));
        let err = table
            .ensure_valid_segment(&Bytes::from_static(b"abcd"))
            .unwrap_err();
        assert_invalid_segment_prefix(err, b"abcd", b"abc");
    }

    #[test]
    fn ensure_valid_segment_rejects_when_candidate_is_proper_prefix_of_successor() {
        let table = KVTable::new();
        table.record_touched_segments(touched(&[b"abc"]));
        let err = table
            .ensure_valid_segment(&Bytes::from_static(b"ab"))
            .unwrap_err();
        assert_invalid_segment_prefix(err, b"ab", b"abc");
    }

    #[test]
    fn ensure_valid_segment_only_inspects_immediate_neighbors() {
        // Predecessor "aa" doesn't nest with "ac"; successor "az" doesn't
        // either. The far-away "x" must not be consulted.
        let table = KVTable::new();
        table.record_touched_segments(touched(&[b"aa", b"az", b"x"]));
        assert!(!table
            .ensure_valid_segment(&Bytes::from_static(b"ac"))
            .unwrap());
    }

    #[test]
    fn ensure_valid_segment_handles_candidate_smaller_than_all() {
        let table = KVTable::new();
        table.record_touched_segments(touched(&[b"mmm", b"ppp"]));
        // "aaa" has no predecessor; "mmm" doesn't start with "aaa".
        assert!(!table
            .ensure_valid_segment(&Bytes::from_static(b"aaa"))
            .unwrap());
    }

    #[test]
    fn ensure_valid_segment_handles_candidate_larger_than_all() {
        let table = KVTable::new();
        table.record_touched_segments(touched(&[b"aaa", b"bbb"]));
        // "zzz" has no successor; predecessor "bbb" is not a prefix of "zzz".
        assert!(!table
            .ensure_valid_segment(&Bytes::from_static(b"zzz"))
            .unwrap());
    }

    fn surviving_keys(imm: &ImmutableMemtable) -> Vec<Bytes> {
        let mut iter = imm.table().iter();
        let mut keys = Vec::new();
        while let Some(entry) = iter.next_sync() {
            keys.push(entry.key);
        }
        keys
    }

    #[test]
    fn should_drop_prefix_with_no_surviving_rows() {
        // given
        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"a1", b"v", 1));
        table.put(RowEntry::new_value(b"a2", b"v", 2));
        table.put(RowEntry::new_value(b"b1", b"v", 3));
        table.put(RowEntry::new_value(b"b2", b"v", 4));
        table.record_touched_segments(touched(&[b"a", b"b"]));
        let imm = ImmutableMemtable::new(table, 0);

        // when
        let filtered = imm.filter_after_seq(2, Some(&SingleByteExtractor)).unwrap();

        // then
        assert_eq!(filtered.table().touched_segments(), touched(&[b"b"]));
        assert_eq!(
            surviving_keys(&filtered),
            vec![Bytes::from_static(b"b1"), Bytes::from_static(b"b2")]
        );
    }

    #[test]
    fn should_keep_prefixes_with_surviving_rows() {
        // given
        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"a1", b"v", 1));
        table.put(RowEntry::new_value(b"b1", b"v", 2));
        table.record_touched_segments(touched(&[b"a", b"b"]));
        let imm = ImmutableMemtable::new(table, 0);

        // when
        let filtered = imm.filter_after_seq(0, Some(&SingleByteExtractor)).unwrap();

        // then
        assert_eq!(filtered.table().touched_segments(), touched(&[b"a", b"b"]));
    }

    #[test]
    fn should_empty_touched_segments_when_no_rows_survive() {
        // given
        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"a1", b"v", 1));
        table.put(RowEntry::new_value(b"b1", b"v", 2));
        table.record_touched_segments(touched(&[b"a", b"b"]));
        let imm = ImmutableMemtable::new(table, 0);

        // when
        let filtered = imm
            .filter_after_seq(10, Some(&SingleByteExtractor))
            .unwrap();

        // then
        assert!(filtered.table().touched_segments().is_empty());
        assert!(surviving_keys(&filtered).is_empty());
    }

    #[test]
    fn should_leave_touched_segments_empty_for_non_segmented() {
        // given
        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"a1", b"v", 1));
        table.put(RowEntry::new_value(b"b1", b"v", 2));
        let imm = ImmutableMemtable::new(table, 0);

        // when
        let filtered = imm.filter_after_seq(1, None).unwrap();

        // then
        assert!(filtered.table().touched_segments().is_empty());
        assert_eq!(surviving_keys(&filtered), vec![Bytes::from_static(b"b1")]);
    }

    #[tokio::test]
    async fn test_memtable_descending_returns_highest_seq_first_for_same_key() {
        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"aaaa", b"v1", 0));
        table.put(RowEntry::new_value(b"bbbb", b"old", 1));
        table.put(RowEntry::new_value(b"bbbb", b"new", 2));
        table.put(RowEntry::new_value(b"cccc", b"v3", 3));

        let mut iter = table
            .table()
            .range(.., IterationOrder::Descending, ReadTrace::none());

        // In descending order, for key "bbbb" the newest version (seq 2) must
        // come before the older version (seq 1) so that dedup works correctly.
        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(b"cccc", b"v3", 3),
                RowEntry::new_value(b"bbbb", b"new", 2),
                RowEntry::new_value(b"bbbb", b"old", 1),
                RowEntry::new_value(b"aaaa", b"v1", 0),
            ],
        )
        .await;
    }

    fn seek_test_table() -> WritableKVTable {
        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"abc111", b"value1", 1));
        table.put(RowEntry::new_value(b"abc222", b"value2", 2));
        table.put(RowEntry::new_value(b"abc333", b"old", 3));
        table.put(RowEntry::new_value(b"abc333", b"new", 4));
        table.put(RowEntry::new_value(b"abc444", b"value4", 5));
        table.put(RowEntry::new_value(b"abc555", b"value5", 6));
        table
    }

    #[rstest]
    #[case::existing_key(b"abc333".as_slice())]
    #[case::between_keys(b"abc332".as_slice())]
    #[tokio::test]
    async fn test_memtable_seek_lands_on_highest_seq(#[case] next_key: &[u8]) {
        let table = seek_test_table();
        let mut iter = table.table().iter();

        iter.seek(next_key).await.unwrap();

        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(b"abc333", b"new", 4),
                RowEntry::new_value(b"abc333", b"old", 3),
                RowEntry::new_value(b"abc444", b"value4", 5),
                RowEntry::new_value(b"abc555", b"value5", 6),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_memtable_seek_to_current_key_keeps_remaining_versions() {
        let table = seek_test_table();
        let mut iter = table.table().iter();
        iter.seek(b"abc333").await.unwrap();
        assert_eq!(
            iter.next().await.unwrap(),
            Some(RowEntry::new_value(b"abc333", b"new", 4))
        );

        // The front entry is the older version of the same key, so seek does nothing.
        iter.seek(b"abc333").await.unwrap();

        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(b"abc333", b"old", 3),
                RowEntry::new_value(b"abc444", b"value4", 5),
                RowEntry::new_value(b"abc555", b"value5", 6),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_memtable_seek_before_current_position_is_noop() {
        let table = seek_test_table();
        let mut iter = table.table().iter();
        iter.seek(b"abc444").await.unwrap();

        iter.seek(b"abc222").await.unwrap();

        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(b"abc444", b"value4", 5),
                RowEntry::new_value(b"abc555", b"value5", 6),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_memtable_seek_past_range_end_exhausts() {
        let table = seek_test_table();
        let mut iter = table
            .table()
            .range_ascending(BytesRange::from(..Bytes::from_static(b"abc444")));

        iter.seek(b"abc999").await.unwrap();

        assert_eq!(iter.next().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_memtable_seek_after_exhausted_is_noop() {
        let table = seek_test_table();
        let mut iter = table
            .table()
            .range_ascending(BytesRange::from(..Bytes::from_static(b"abc333")));
        iter.seek(b"abc333").await.unwrap();
        assert_eq!(iter.next().await.unwrap(), None);

        // A rebuilt range must not bring back entries that the iterator skipped.
        iter.seek(b"abc000").await.unwrap();

        assert_eq!(iter.next().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_memtable_seek_keeps_end_bound() {
        let table = seek_test_table();
        let mut iter = table
            .table()
            .range_ascending(BytesRange::from(..=Bytes::from_static(b"abc333")));

        iter.seek(b"abc222").await.unwrap();

        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(b"abc222", b"value2", 2),
                RowEntry::new_value(b"abc333", b"new", 4),
                RowEntry::new_value(b"abc333", b"old", 3),
            ],
        )
        .await;
    }

    /// A small deterministic generator (xorshift64*), for the `put_sorted` tests.
    struct XorShift(u64);

    impl XorShift {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    fn all_rows(table: &KVTable) -> Vec<RowEntry> {
        let mut iter = table.iter();
        let mut rows = Vec::new();
        while let Some(row) = iter.next_sync() {
            rows.push(row);
        }
        rows
    }

    fn scan(table: &KVTable, range: BytesRange, ordering: IterationOrder) -> Vec<RowEntry> {
        let mut iter = table.range(range, ordering, ReadTrace::none());
        let mut rows = Vec::new();
        while let Some(row) = iter.next_sync() {
            rows.push(row);
        }
        rows
    }

    #[derive(Clone, Copy, Debug)]
    enum BatchShape {
        /// Keys spread over 256 buckets, sorted, as a write batch hands them over.
        Spread,
        /// Keys at a time-ordered tail, sorted.
        Clustered,
        /// Spread keys, sorted, with some rows repeated with the same (key, seq).
        EqualKeys,
        /// Spread keys in random order.
        Unsorted,
    }

    /// `count` batches of the given shapes. Batch `b` has sequence number `b + 1`. Keys repeat
    /// across batches (with different sequence numbers).
    fn random_batches(
        rng: &mut XorShift,
        count: usize,
        shapes: &[BatchShape],
    ) -> Vec<Vec<RowEntry>> {
        let mut tail = 0u64;
        (0..count)
            .map(|b| {
                let seq = b as u64 + 1;
                let shape = shapes[b % shapes.len()];
                let len = 1 + rng.below(48) as usize;
                let mut keys: Vec<Vec<u8>> = (0..len)
                    .map(|_| match shape {
                        BatchShape::Clustered => {
                            tail += 1 + rng.below(3);
                            let mut key = vec![1u8];
                            key.extend_from_slice(&tail.to_be_bytes());
                            key
                        }
                        _ => {
                            let mut key = vec![2u8, rng.below(256) as u8];
                            key.extend_from_slice(&rng.below(400).to_be_bytes());
                            key
                        }
                    })
                    .collect();
                keys.sort();
                keys.dedup();
                let mut rows: Vec<RowEntry> = keys
                    .iter()
                    .enumerate()
                    .map(|(at, key)| RowEntry::new_value(key, format!("b{b}-{at}").as_bytes(), seq))
                    .collect();
                match shape {
                    BatchShape::EqualKeys => {
                        // Repeat about a quarter of the rows right after themselves.
                        let mut with_equal = Vec::new();
                        for row in rows {
                            let repeat = rng.below(4) == 0;
                            with_equal.push(row.clone());
                            if repeat {
                                with_equal.push(RowEntry::new_value(&row.key, b"again", seq));
                            }
                        }
                        rows = with_equal;
                    }
                    BatchShape::Unsorted => {
                        for at in (1..rows.len()).rev() {
                            rows.swap(at, rng.below(at as u64 + 1) as usize);
                        }
                    }
                    BatchShape::Spread | BatchShape::Clustered => {}
                }
                rows
            })
            .collect()
    }

    #[test]
    fn should_put_sorted_the_same_table_as_put() {
        // given
        let rows = vec![
            RowEntry::new_value(b"k1", b"a", 3),
            RowEntry::new_value(b"k2", b"b", 3),
            RowEntry::new_value(b"k2", b"c", 3),
            RowEntry::new_tombstone(b"k3", 3),
            RowEntry::new_value(b"k0", b"d", 3),
            RowEntry::new_merge(b"k4", b"e", 3),
        ];
        let by_put = KVTable::new();
        let by_put_sorted = KVTable::new();

        // when
        for row in rows.clone() {
            by_put.put(row);
        }
        by_put_sorted.put_sorted(rows);

        // then
        assert_eq!(all_rows(&by_put_sorted), all_rows(&by_put));
        let (a, b) = (by_put.metadata(), by_put_sorted.metadata());
        assert_eq!(
            (
                a.entry_num,
                a.entries_size_in_bytes,
                a.last_seq,
                a.first_seq
            ),
            (
                b.entry_num,
                b.entries_size_in_bytes,
                b.last_seq,
                b.first_seq
            )
        );
        // The equal (k2, 3) replaced one row on both paths; it and k0 searched from the head.
        assert_eq!(by_put.insert_counts().replaced, 1);
        assert_eq!(
            by_put_sorted.insert_counts(),
            InsertCounts {
                replaced: 1,
                finger: FingerStats {
                    from_finger: 3,
                    empty: 1,
                    out_of_order: 2,
                    stale: 0,
                },
            }
        );
    }

    #[test]
    fn should_put_sorted_into_an_empty_table_and_after_existing_rows() {
        let table = KVTable::new();
        table.put_sorted(Vec::new());
        assert!(table.is_empty());

        table.put_sorted(
            (0..1_000u32).map(|i| RowEntry::new_value(&(i * 2).to_be_bytes(), b"even", 1)),
        );
        table.put_sorted(
            (0..1_000u32).map(|i| RowEntry::new_value(&(i * 2 + 1).to_be_bytes(), b"odd", 2)),
        );

        let rows = all_rows(&table);
        assert_eq!(rows.len(), 2_000);
        for (at, row) in rows.iter().enumerate() {
            assert_eq!(row.key, Bytes::copy_from_slice(&(at as u32).to_be_bytes()));
        }
        let counts = table.insert_counts();
        assert_eq!(counts.finger.from_finger, 1_998);
        assert_eq!(counts.finger.empty, 2);
        assert_eq!(counts.finger.out_of_order + counts.finger.stale, 0);
    }

    /// The single-writer invariant is checked: a second `put` while a `put_sorted` is inserting
    /// panics in debug builds.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "a second writer entered a memtable")]
    fn should_panic_when_a_second_writer_enters_during_put_sorted() {
        let table = KVTable::new();
        let rows = (0..4u8).map(|i| {
            if i == 2 {
                table.put(RowEntry::new_value(b"other", b"v", 9));
            }
            RowEntry::new_value(&[i], b"v", 1)
        });
        table.put_sorted(rows);
    }

    /// `put_sorted` with readers scanning and reading the table while it inserts gives the same
    /// table and the same scans as `put` one row at a time. Readers check, while the batches go
    /// in, that every scan is in order and holds every row of each batch already inserted. The
    /// writer waits for a reader's scan every `PACE` batches, so scans overlap the inserts.
    #[test]
    fn should_put_sorted_with_concurrent_readers_like_put() {
        const BATCHES: usize = 1_000;
        const READERS: usize = 4;
        const PACE: usize = 10;
        let shape_sets: [&[BatchShape]; 3] = [
            &[BatchShape::Spread],
            &[BatchShape::Clustered, BatchShape::Spread],
            &[
                BatchShape::Spread,
                BatchShape::EqualKeys,
                BatchShape::Unsorted,
                BatchShape::Clustered,
            ],
        ];
        for (case, shapes) in shape_sets.into_iter().enumerate() {
            let mut rng = XorShift(0x9E37_79B9_7F4A_7C15 + case as u64);
            let batches = random_batches(&mut rng, BATCHES, shapes);

            // The (key, seq) pairs of batches 0..b, for b = 0..=BATCHES.
            let mut distinct = std::collections::BTreeSet::new();
            let mut present_after = vec![0usize];
            for batch in &batches {
                for row in batch {
                    distinct.insert((row.key.clone(), row.seq));
                }
                present_after.push(distinct.len());
            }

            let by_put = KVTable::new();
            for batch in &batches {
                for row in batch.clone() {
                    by_put.put(row);
                }
            }

            let by_put_sorted = KVTable::new();
            let inserted = AtomicUsize::new(0);
            // Scans that started with 0 < done < BATCHES.
            let scans_during_inserts = AtomicUsize::new(0);
            // The `done` of the last scan that started during the inserts.
            let last_scan_at = AtomicUsize::new(0);
            // Readers still running. The writer stops waiting for scans when one has failed.
            let readers_running = AtomicUsize::new(READERS);
            let start = std::sync::Barrier::new(READERS + 1);
            std::thread::scope(|scope| {
                for reader in 0..READERS {
                    let (table, inserted, batches) = (&by_put_sorted, &inserted, &batches);
                    let (present_after, start) = (&present_after, &start);
                    let (scans_during_inserts, last_scan_at) =
                        (&scans_during_inserts, &last_scan_at);
                    let readers_running = &readers_running;
                    scope.spawn(move || {
                        struct Running<'a>(&'a AtomicUsize);
                        impl Drop for Running<'_> {
                            fn drop(&mut self) {
                                self.0.fetch_sub(1, SeqCst);
                            }
                        }
                        let _running = Running(readers_running);
                        let mut rng = XorShift(reader as u64 + 1);
                        start.wait();
                        loop {
                            let done = inserted.load(SeqCst);
                            if 0 < done && done < BATCHES {
                                scans_during_inserts.fetch_add(1, SeqCst);
                                last_scan_at.store(done, SeqCst);
                            }
                            // A full scan: in order, and every row of batches 0..done is there.
                            let rows = all_rows(table);
                            for pair in rows.windows(2) {
                                let a = SequencedKey::new(pair[0].key.clone(), pair[0].seq);
                                let b = SequencedKey::new(pair[1].key.clone(), pair[1].seq);
                                assert!(a < b, "a scan out of order: {a:?} then {b:?}");
                            }
                            let old = rows.iter().filter(|row| row.seq <= done as u64).count();
                            assert_eq!(old, present_after[done], "a scan missed a row");
                            // Point reads of rows already inserted.
                            if done > 0 {
                                for _ in 0..8 {
                                    let batch = &batches[rng.below(done as u64) as usize];
                                    let row = &batch[rng.below(batch.len() as u64) as usize];
                                    let found = scan(
                                        table,
                                        BytesRange::from(row.key.clone()..=row.key.clone()),
                                        IterationOrder::Ascending,
                                    );
                                    assert!(
                                        found.iter().any(|r| r.seq == row.seq),
                                        "a read missed a row"
                                    );
                                }
                            }
                            if done == BATCHES {
                                break;
                            }
                        }
                    });
                }
                start.wait();
                for (b, batch) in batches.iter().enumerate() {
                    // Every PACE batches, wait until a scan has started at this point.
                    if b > 0 && b % PACE == 0 {
                        while last_scan_at.load(SeqCst) != b
                            && readers_running.load(SeqCst) == READERS
                        {
                            std::thread::yield_now();
                        }
                    }
                    by_put_sorted.put_sorted(batch.clone());
                    inserted.fetch_add(1, SeqCst);
                }
            });
            // A scan started at each of the BATCHES / PACE - 1 pauses at least.
            assert!(
                scans_during_inserts.load(SeqCst) >= BATCHES / PACE - 1,
                "case {case}: {} scans during the inserts",
                scans_during_inserts.load(SeqCst)
            );

            // The same rows, sizes and sequence numbers.
            assert_eq!(all_rows(&by_put_sorted), all_rows(&by_put), "case {case}");
            let (a, b) = (by_put.metadata(), by_put_sorted.metadata());
            assert_eq!(
                (
                    a.entry_num,
                    a.entries_size_in_bytes,
                    a.last_seq,
                    a.first_seq
                ),
                (
                    b.entry_num,
                    b.entries_size_in_bytes,
                    b.last_seq,
                    b.first_seq
                ),
                "case {case}"
            );
            // The same scans over random ranges, both ways.
            for _ in 0..100 {
                let mut bound = || {
                    let mut key = vec![1 + rng.below(2) as u8, rng.below(256) as u8];
                    key.extend_from_slice(&rng.below(400).to_be_bytes());
                    Bytes::from(key)
                };
                let (x, y) = (bound(), bound());
                let range = BytesRange::from(x.clone().min(y.clone())..=x.max(y));
                for ordering in [IterationOrder::Ascending, IterationOrder::Descending] {
                    assert_eq!(
                        scan(&by_put_sorted, range.clone(), ordering),
                        scan(&by_put, range.clone(), ordering),
                        "case {case}"
                    );
                }
            }
            let counts = by_put_sorted.insert_counts();
            assert_eq!(
                counts.replaced,
                by_put.insert_counts().replaced,
                "case {case}"
            );
            assert_eq!(counts.finger.stale, 0, "case {case}");
            assert!(counts.finger.from_finger > 0, "case {case}");
        }
    }

    #[tokio::test]
    async fn test_memtable_descending_seek_is_not_supported() {
        let table = seek_test_table();
        let mut iter = table
            .table()
            .range(.., IterationOrder::Descending, ReadTrace::new(None));

        let result = iter.seek(b"abc333").await;

        assert!(matches!(
            result,
            Err(SlateDBError::SeekNotSupportedForDescendingScan)
        ));
    }
}

/// The stale-finger tests of the vendored `crossbeam-skiplist` (`tests/finger.rs`), copied here
/// so that the workspace's test runs cover them. The vendored crate is outside the workspace.
#[cfg(test)]
mod skiplist_finger_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crossbeam_skiplist::map::FingerStats;
    use crossbeam_skiplist::SkipMap;

    fn contents(map: &SkipMap<u64, u64>) -> Vec<(u64, u64)> {
        map.iter().map(|e| (*e.key(), *e.value())).collect()
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
        // Every node the finger inserted is removed under it; the inserts after stay correct.
        // Key 1 stays, so the list is not empty (a search of an empty list reads no finger node).
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
        std::thread::scope(|s| {
            for residue in [1, 2] {
                let (map, done) = (&map, &done);
                s.spawn(move || {
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
            s.spawn(|| {
                for k in 0..N {
                    map.remove(&(3 * k));
                }
                done.fetch_add(1, Ordering::Release);
            });
            for _ in 0..2 {
                s.spawn(|| {
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
        });
        let keys: Vec<u64> = map.iter().map(|e| *e.key()).collect();
        let expected: Vec<u64> = (0..3 * N).filter(|k| k % 3 != 0).collect();
        assert_eq!(keys, expected);
        for entry in map.iter() {
            assert_eq!(entry.key(), entry.value());
        }
    }
}
