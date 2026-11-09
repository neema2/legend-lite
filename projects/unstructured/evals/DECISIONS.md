# Answer-key decisions

## Entry 0, 2026-11-09

pre-freeze correction, C14, expected computed by the query, reviewer re-review requested

## Entry 1, 2026-11-09

answer key frozen

answer_key.yaml sha256: 7a95b43d2dc41f21a4eb6a3cbe5396298f1408ad13e8a5b4b2936266bc175e79

That is the file after the review commit b261590af and entry 0's correction.

Resolved: 49. Unresolved: 1 (C14: the review set it unresolved, and the re-review entry 0 requested is outstanding). An
unresolved case is never counted.

No label in answer_key.yaml changes without a dated entry here, recording the file's new sha256.

The judge is not yet built.

## Entry 2, 2026-11-09

answer key frozen; this entry's sha256 replaces entry 1's

answer_key.yaml sha256: a4dcdc340a4aa10911a123ff15cb81e28c93ffb36252bebd1dba8a59c0945e7e

Three changes to the key since entry 1:

- C14's status is resolved, as the re-review set it (commit bdf5b9e8a, REVIEW_2026_11_09.md).
- Line 2 reads "Frozen 2026-11-09 (DECISIONS.md entry 2)".
- The 40 cases outside the review carry reviewer: "not sampled (10 of 50 reviewed at random, seed 20261109;
  REVIEW_2026_11_09.md)".

Resolved: 50. Unresolved: 0.

From here, no label in answer_key.yaml changes without a dated entry here, recording the file's new sha256.

## Entry 3, 2026-11-09

the judge's baseline, before any tuning

answer_key.yaml sha256: a4dcdc340a4aa10911a123ff15cb81e28c93ffb36252bebd1dba8a59c0945e7e

ledger.tsv sha256: 934b8ba576b882988b1665ba7a82daf98bd55eca4a17fd1422a0066af39df297

The key did not change. Three fresh runs of the whole judge (load, embed, index, judge) wrote byte-identical ledgers.

**A rule changed, not a label.** The cut is the top max(5, number of accepted pointers) of each case, so that a match: all
case can pass in principle. C04 accepts 6 pointers and C08 accepts 12; a top 5 could hold neither. Effect on the ledger as
first shown: C08 went from DROPPED to PASS on all four arms that ran, C04 stayed DROPPED, and no other row moved.

**The owner's rulings on what the judge had decided for itself:**

- Hybrid's relevance is the fused score normalised by its theoretical maximum, 2/61.
- Postgres's terms are ORed, to match DuckDB's any-term BM25.
- Search covers all of CO_PASSAGE, not the case's corpus: cross-corpus noise is part of the test.
- STALE_HIT outranks DROPPED.
- A hash held at several dates takes its most recent date for recency. Approved for the baseline and recorded as finding
  F-U6; scoring per location and keeping the best is a candidate tuning entry, not a baseline change.
- Questions carry the embedding model card's retrieval instruction.

**Negatives.** All four text negatives are FALSE_HIT on every arm that ran, with no score threshold. That is the honest
baseline; a threshold is a tuning change, with a before/after entry, later.

**One change was made after a ledger had been seen, and it is determinism, not tuning.** `keyword_duckdb.sql` rounds the
BM25 score to 9 places before ranking. `match_bm25` sums in parallel, so mathematically equal scores differed by up to
8.9e-16 from one call to the next and ties broke at random: two full runs before the fix differed on 8 ledger rows,
including C06 and C76's verdicts on keyword DuckDB. Rounding lets a tie be a tie and break on the hash.

From here, a change that turns any row from PASS to anything else fails the gate, even if the counts improve.
