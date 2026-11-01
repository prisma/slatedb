#!/bin/bash
# fork/sync.sh: bring upstream slatedb/slatedb main into this fork.
# Rebases `main` (one docs commit) and `streams` (one commit per carried patch) onto
# upstream main, stops at the first conflict, and runs SlateDB's lib tests. It never
# pushes: review, then push `main`, `streams` and a `streams-YYYYMMDD-<sha>` tag yourself.
# Streams must measure the new `streams` rev against its current pin before adopting it
# (see FORK.md, "Bringing in upstream changes").
set -euo pipefail
UPSTREAM=${UPSTREAM:-https://github.com/slatedb/slatedb.git}
git remote get-url upstream >/dev/null 2>&1 || git remote add upstream "$UPSTREAM"
git fetch -q upstream main
git fetch -q origin main streams
old_main=$(git rev-parse origin/main)
git checkout -q -B main origin/main
git rebase -q upstream/main
git checkout -q -B streams origin/streams
# Replay only the patch commits (those on streams that are not on the old fork main).
git rebase -q --onto main "$old_main" streams
echo "patches now carried (empty ones were dropped by the rebase):"
git log --oneline main..streams
cargo test -p slatedb --lib --features zstd,wal_disable
echo "SYNC_OK: push main and streams, tag streams-$(date -u +%Y%m%d)-$(git rev-parse --short streams), then measure in Streams before moving its pin."
