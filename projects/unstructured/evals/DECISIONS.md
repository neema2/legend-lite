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

## Entry 4, 2026-11-09

the Postgres vector and hybrid statements, run; this entry's ledger replaces entry 3's

answer_key.yaml sha256: a4dcdc340a4aa10911a123ff15cb81e28c93ffb36252bebd1dba8a59c0945e7e

ledger.tsv sha256: 45b0ec94317ea40e132e98759196323acbfe7b1f43419c4ffd9abf29cf6d6efe

No verdict changed, and the gate against entry 3's ledger passed. The only rows that moved are the 88 REFUSED rows of the
vector and hybrid arms on Postgres, whose detail now carries the exact error from running the statement on every case:
UndefinedTable 42P01, relation "co_passage_embedding" does not exist. Until this entry the judge read the catalogue and did
not run them.

The repository's embedded Postgres has no pgvector and nothing was installed into it. Both statements parse; the server
stops at name resolution, because the table's column needs the type pgvector provides, and creating that table answers
UndefinedObject 42704, type "vector" does not exist. So neither statement has returned a row, and the `<=>` operator has
not been exercised. A failure of these statements other than an undefined table, type or function is an ERROR, not a refusal.

Three fresh runs wrote byte-identical ledgers.

## Entry 5, 2026-11-09

a score floor for the negatives, the one tuning entry before the PR; this entry's ledger replaces entry 4's

answer_key.yaml sha256: a4dcdc340a4aa10911a123ff15cb81e28c93ffb36252bebd1dba8a59c0945e7e

ledger.tsv sha256: 20afd1c988442254e0786d11d780eb1def0bd44e65bf2ec4bde3bfdd15c3decf

The key did not change. A floor is a minimum on a result set's best score: below it the set is empty. It is written in each
arm's SQL as a constant. Hybrid's is on the fused relevance, before the recency blend, and hybrid fuses the arms' lists
without the keyword floors.

**The result.** Three fresh runs wrote byte-identical ledgers. The gate against entry 4's ledger passed: 0 regressions in 288
rows. Six rows moved, all FALSE_HIT to EMPTY_OK, and the structured route did not move.

| Arm, backend | Floor | PASS | DROPPED | STALE_HIT | EMPTY_OK | FALSE_HIT |
|---|---|---|---|---|---|---|
| keyword, DuckDB (BM25) | 4.5 | 15 → 15 | 20 → 20 | 5 → 5 | 0 → 2 | 4 → 2 |
| keyword, Postgres (`ts_rank`) | 0.026 | 16 → 16 | 19 → 19 | 5 → 5 | 0 → 2 | 4 → 2 |
| vector, DuckDB (cosine) | none | 12 → 12 | 24 → 24 | 4 → 4 | 0 → 0 | 4 → 4 |
| hybrid, DuckDB (fused) | 0.89 | 16 → 16 | 22 → 22 | 2 → 2 | 0 → 2 | 4 → 2 |
| vector and hybrid, Postgres | none, 0.89 | REFUSED 44 each, unchanged, details included | | | | |

**The negatives,** each with its best score on that arm, and what it became:

| | C40 | C60 | C78 | C80 | Lowest PASS's best score |
|---|---|---|---|---|---|
| keyword, DuckDB | 5.2531 FALSE_HIT | 5.3770 FALSE_HIT | 4.1325 EMPTY_OK | 4.0385 EMPTY_OK | 4.9283 (C27) |
| keyword, Postgres | 0.0405 FALSE_HIT | 0.0434 FALSE_HIT | 0.0240 EMPTY_OK | 0.0187 EMPTY_OK | 0.0274 (C77) |
| vector, DuckDB | 0.7488 FALSE_HIT | 0.7317 FALSE_HIT | 0.7122 FALSE_HIT | 0.6537 FALSE_HIT | 0.6314 (C32) |
| hybrid, DuckDB | 0.8780 EMPTY_OK | 1.0000 FALSE_HIT | 0.9236 FALSE_HIT | 0.8245 EMPTY_OK | 0.9067 (C21) |

**What it cost.** No PASS and no STALE_HIT moved. Ten DROPPED rows now return nothing: keyword DuckDB C04, C20, C29, C75;
keyword Postgres C09, C51, C75; hybrid DuckDB C04, C16, C20. Nine of them had found none of their accepted passages, so their
rows are unchanged. One lost something: C04 on keyword DuckDB found 1 of its 6 and now finds 0, still DROPPED.

**How the values were chosen.** From the unfloored statements' scores on every text case, against PASS rows and negatives.
Each arm's distributions overlap, so no value separates all four negatives from every PASS. The rule: the midpoint of the
gap between the lowest PASS's best score and the highest negative beneath it, rounded to stay inside the gap (4.53 to 4.5,
0.0257 to 0.026, 0.8924 to 0.89). An arm with no negative beneath its lowest PASS has no gap and gets no floor; that is the
vector arm. Each value was set once and no second value was run through the judge. The table of effects below is read from
those scores; the chosen row was then confirmed by the judge.

| Arm | Value | Negatives emptied | PASS lost | Why not |
|---|---|---|---|---|
| keyword DuckDB | 4.0 | none | none | under every negative |
| | 4.2 | C78, C80 | none | same negatives as 4.5, 0.07 above C78 |
| | **4.5** | C78, C80 | none | **chosen** |
| | 4.9 | C78, C80 | none | 0.03 under C27; empties 8 DROPPED, not 4, and C70's 1 of 2 |
| | 5.3 | C40, C78, C80 | C27 | the gate |
| | 5.4 | all four | C27 | the gate |
| keyword Postgres | 0.020 | C80 | none | leaves C78 for no gain |
| | 0.025 | C78, C80 | none | same negatives as 0.026, 0.001 above C78 |
| | **0.026** | C78, C80 | none | **chosen** |
| | 0.027 | C78, C80 | none | 0.0004 under C77; empties 4 DROPPED, not 3 |
| | 0.041 | C40, C78, C80 | C06, C48, C55, C76, C77 | the gate |
| | 0.044 | all four | those and C08, C73 | the gate |
| vector DuckDB | 0.62, 0.63 | none | none | empties 5 DROPPED and turns no negative; not set |
| | 0.655 | C80 | C32 | the gate |
| | 0.72 | C78, C80 | C10, C32, C42 | the gate |
| | 0.75 | all four | C07, C10, C29, C32, C42, C73, C77 | the gate |
| hybrid DuckDB | 0.85 | C80 | none | leaves C40 for no gain |
| | 0.88 | C40, C80 | none | same negatives as 0.89, 0.002 above C40 |
| | **0.89** | C40, C80 | none | **chosen** |
| | 0.905 | C40, C80 | none | 0.0017 under C21 |
| | 0.93 | C40, C78, C80 | C21 | the gate |
| | over 1.0 | all four | all 16 | the only value that reaches C60 |

**Alternatives considered.**

- A floor per row, not per set. At these values it loses C77 on keyword Postgres and C21, C25 and C38 on hybrid. Hybrid's
  lowest passage needed by a PASS is 0.7506 (C21), under every negative's best score, so per row it could not turn one negative.
- Hybrid's floor on the blended score. A time-aware case's blend adds recency, which says nothing about whether anything answers.
- The keyword floors applied inside hybrid. They would change the fused ranks; hybrid has its own floor.
- Floors per corpus. Four negatives over three corpora is a floor fitted to single cases.
- One floor across backends. BM25 and `ts_rank` are not on one scale.

**What this does not show.** The floors were read from the same 50 cases they are judged on, with four negatives among them.
Any value at or under the lowest PASS's best score passes the gate by construction, so the gate passing here is not independent
evidence. BM25 depends on the corpus's term statistics and cosine on the embedder, so each floor is tied to this corpus and these
pins. `hybrid_postgres.sql` carries DuckDB's 0.89 and has never returned a row. A fused score is rank agreement and not relevance:
C60's best passage is first in both lists, so its score is 1.0 and no fused floor can reach it. Two of four negatives are still
FALSE_HIT on keyword and hybrid, and all four on vector.

**What would reverse it.** Each of these, by a dated entry:

- A new resolved case, added by a dated entry, that is PASS without its arm's floor and empty with it.
- On a new set of questions labelled by hand before they are run, a floor that empties as many answerable questions as
  unanswerable ones, or more.
- A change of corpus, passage boundaries or embedder that puts any PASS's best score under its arm's floor: the floor is then
  re-read, not kept.
- A real pgvector run where a PASS's best fused relevance is under 0.89 on Postgres.

## Entry 6, 2026-11-09

the independent review's findings; no label and no verdict changed

answer_key.yaml sha256: a4dcdc340a4aa10911a123ff15cb81e28c93ffb36252bebd1dba8a59c0945e7e

ledger.tsv sha256: 20afd1c988442254e0786d11d780eb1def0bd44e65bf2ec4bde3bfdd15c3decf

**The evidence is committed.** `judge.py --write` now also writes `tools/unstructured-oracle/evidence/ranks.tsv` (sha256 adca41d8ce8872b687690f0918bd0b67f0a15aa1fd99ac004e249c6661dc79d3), each arm's
floor-free best score and each pointer's rank and score in its whole list, and `evidence/passages.tsv` (sha256 51117dce369928f672c7e2920132fad8bf2fb5919a902dc8e11c5c03043b9664). They are what entry 5's
best-score cells and the candidates below are read from. Three fresh runs wrote byte-identical ledgers, ranks and passages, and the ledger is
byte-for-byte this entry's, so the run changed nothing the gate reads. Anyone with the corpus reproduces it: `run.sh --write`, then
`git diff --exit-code` on `ledger.tsv` and `evidence/`. A run's identity is a measurement and has no other artifact.

**The cut rule's effect is now on file.** `evidence/ledger_before_cut_rule.tsv` (sha256 a63c8a3c052b0725233a91545fa35192fb6b4952cf3e6a5e557c5b83a7e3860f) is the ledger before entry 3's rule. Between it and entry 3's
ledger, C08 goes DROPPED to PASS on the four arms that ran, and C04's four rows change only in their detail, "top 5" to "top 6". The rule's commit, `d39699192`, carries no ledger, and the first one committed, `e42d8177a`, was
already made under the rule, so this file was in no commit.

**Two figures corrected, from the evidence.** In the candidates below, the median rank of the pointers beyond the cut is 28, not 30, and EDGAR's accepted
passages on PASS rows have a median of 747 characters, not 772: both had been taken as the upper of the two middle values of an even list. The study also
said "37 missed accepted pointers"; there are 37 accepted pointers on hybrid's DROPPED rows, of which 32 are missed and 5 found inside the cut.

**The freeze is mechanical.** `answer_key.sha256` holds the frozen hash, and `//projects/unstructured:freeze_test`, in the `checks` lane with no corpus, holds the
key to it. A test may not read this file (guard G17: prose must not change a verdict), which is why the record is a data file; `judge.py` refuses a run unless the key,
that file and this file's last entry agree.

## Entry 7, 2026-11-11

the Postgres driver: psycopg, LGPL-3.0-only, replaced by pg8000, BSD-3-Clause; no label, verdict, rank or score changed

answer_key.yaml sha256: a4dcdc340a4aa10911a123ff15cb81e28c93ffb36252bebd1dba8a59c0945e7e

ledger.tsv sha256: aa930c058c19855bd8ca9cd89dee47a82107c0c5d65b080d8848b9e5872a44a2

**Why.** The study's Python dependencies are to be Apache-2.0, MIT or BSD wherever a choice exists (`tools/unstructured-oracle/README.md`,
"Licences"). psycopg and psycopg-binary 3.3.8 declare LGPL-3.0-only. pg8000 1.31.6 declares BSD-3-Clause, and it is pinned in `pins.env`
in their place. Every statement to Postgres now goes through `tools/unstructured-corpus/postgres.py`.

**What moved.** Three fresh runs wrote byte-identical files, and against entry 6's ledger `judge.py --compare` finds 0 regressions over 288
rows. The ledger's body differs in one column of 88 rows, the REFUSED rows of vector and hybrid on Postgres. Their detail named the driver's
exception class, `UndefinedTable 42P01: relation "co_passage_embedding" does not exist`, and now carries what the server sent, its SQLSTATE
and message: `42P01: relation "co_passage_embedding" does not exist`. The class name was psycopg's spelling, not the database's. The
headers of `ledger.tsv` and `evidence/ranks.tsv` name pg8000's pin in place of psycopg's two. `evidence/ranks.tsv` (sha256 44eb98144d819b75629f24c2e9b7eb35cc6072e6347c609cc0533889fe1aae99)
differs from entry 6's only in that header; `evidence/passages.tsv` (sha256 51117dce369928f672c7e2920132fad8bf2fb5919a902dc8e11c5c03043b9664) and `summary.tsv` are unchanged.

Entry 4 quotes the store step as `UndefinedObject 42704`: that is psycopg's spelling at the time, and it stays as recorded.

## Not tuned, candidates

None of these has been run. Counts are from the ledger; ranks and scores are from `tools/unstructured-oracle/evidence/ranks.tsv`, the
statements' whole lists before the floors.

- **Per-location recency (F-U6).** The ledger's support is thin. No accepted or superseded pointer of the 9 time-aware text
  cases sits in a hash held at more than one date. Such hashes do hold places in hybrid's cut on 3 of those 9: C63 (4 of its 5,
  hybrid DROPPED), C43 (1, STALE_HIT) and C64 (1, DROPPED). C63 is where to start; that they are the cause is not measured.
- **Smaller EDGAR passages.** EDGAR's median passage is 717 characters, LENDING's 152, SALES's 58, and EDGAR is 7 PASS, 7
  DROPPED on hybrid. The evidence is mixed and the weakest here: on hybrid, EDGAR's accepted passages have a median of 747
  characters on PASS rows and 752 on DROPPED ones, and SALES, the shortest passages, has the most DROPPED, 9 of 15.
- **A larger embedder.** Vector has the fewest PASS (12, against 15 and 16 for keyword and 16 for hybrid) and the most DROPPED
  (24), and 1 PASS of the 15 SALES cases. The negatives' best cosines, 0.6537 to 0.7488, sit inside the PASS rows' range, 0.6314 to
  0.8449, which is why the vector arm has no floor.
- **k per arm** (each arm's 150 candidates, and fusion's k = 60). Of the 37 accepted pointers on hybrid's 22 DROPPED rows, 8 are
  outside both top-150 lists. Six of those 8 share no term with their question on DuckDB, so a deeper keyword list cannot reach
  them. A deeper vector list would reach 5 of the 8 by rank 500 (C04 twice, C16, C24, C59); the other 3 are at rank 1,460 or deeper.
  Depth is not the first limit: keyword's deepest listed miss is rank 69 of 150 on DuckDB.
- **A reranker outside the engine.** Of those 37 pointers, 24 are in the fused list but beyond the cut (median rank 28, deepest
  141), which is what a reranker reorders; 5 are inside it, on rows missing others. C60 is the other motive: first in both
  lists and answering nothing, beyond any fused floor. It stays outside the engine (the boundary in the oracle's README).
