# C3i. M3i's census re-run: the product's statement road against the test road, frames planned (2026-11-01)

M3 (`docs/TEST_IS_THE_PRODUCT_PLAN_2026_10_31.md` §6 B12) ends with the census re-run. Two probes ran together on the
code of bb9de6ce (M3a-M3h), never committed as code:

- M2's front-half census (`probes/M2Census.java.txt`, its hooks `probes/m0-hooks.diff.txt`): each value statement and
  host side planned the product's way (`ProductStatements`, now with the program's frames) and compared with the test
  road's SQL, a difference attributed to the lowering's configuration factors or to the front half.
- M3e's effect and seed comparison (`probes/M3eCensus.java.txt`).

Their combined hooks for the driver, the body compiler and the corpus runner are `probes/m3i-hooks.diff.txt`. Runs:
both corpora (both passes) and the five channel B suites. Channel B 5/5 and H2 6/6 green. DuckDB 4/6: its verdict
target green (every test's outcome matches its roster), shards 3 and 4 of the engine-order register moved -- the
probe's known artefact, recorded at M0 and M2 (its extra renders count as scan-order firings); the chain without
the probe re-checks them.

## 1. The front half: where both roads plan

| per pass | chB | DuckDB database | DuckDB host | H2 database | H2 host |
|---|---|---|---|---|---|
| db-side planned | 4,316 | 7,911 | -- | 8,055 | -- |
| ... same | 3,327 | 4,483 (M2 3,540) | -- | 4,411 (M2 3,493) | -- |
| ... numbering-only | 622 | 198 | -- | 195 | -- |
| ... diff lowering (the known factors) | 42 | 475 | -- | 347 | -- |
| ... constant-vs-literal | 325 | 451 | -- | 597 | -- |
| ... diff front | **0** | 2,166 (M2 0) | -- | 2,107 (M2 0) | -- |
| ... product refuses | 0 | **136** (M2 3,632) | -- | **396** (M2 3,768) | -- |
| host-side planned | 4,222 | 1,368 | 7,742 | 1,352 | 7,516 |
| ... same | 3,233 | 146 | 6,003 (M2 2,460) | 144 | 5,782 (M2 2,390) |
| ... diff lowering | 42 | 259 | 776 | 257 | 641 |
| ... constant-vs-literal | 325 | 203 | 602 | 203 | 589 |
| ... diff front | **0** | 744 | 40 (M2 2) | 732 | 40 (M2 2) |
| ... product refuses | 0 | 11 | **120** (M2 4,381) | 11 | **267** (M2 4,330) |

(Channel B's columns are one run of its five suites; the corpus columns one pass each, as in M2.)

**Reading.**
- **The frames plan.** What M2's product refused -- statements reading an `execute(...)` result -- the product now
  plans: on the host pass, where the walk pastes a frame's chain as the product does, 6,003 statements (DuckDB) plan
  identically (M2: 2,460), the refusals falling from 4,381 to 120.
- **diff front, database pass (2,166 / 2,107)**: every one is a statement reading a frame (the census tags each with
  the plan rows and frame facts that differ). The examples are MV9: the walk's database pass reads a frame by
  reference to its CTE (`FROM frame_result AS frame_result_t0`) where the census's product side, planning each
  statement alone, pastes its chain -- B10's per-statement rule (M3g) lands this with the body classes (M5+).
- **diff front, host pass (40 per corpus)**: statements reading frames, where the census cannot isolate the
  lowering's factors (the statements' plan rows differ). Every example is one of three known kinds, none a different
  answer: the walk's host fold of an activity SQL's string operations (`'select ...'`) against the product leaving
  them to the database (`replace(replace('select ...'))`); the product's exists-join form (`WHERE EXISTS (...)`)
  against the test road's join (`LEFT OUTER JOIN (SELECT DISTINCT ...)`); the product's streaming graph root (one
  object per row) against the test road's array.
- **The remaining refusals** (DuckDB database 136): a class query under a map (57, the resolver's wall, both roads);
  the test-data generator and its census carrier (38, the test's kinds); `TableAlias` at the lowering boundary (18,
  a wall both roads share); 18 statements that read an activity's comment, which the census's product side lowers
  without the stamp slots (the census's own artefact: the program road binds them, `TestDoorTest`); a statement of
  per-key regions asked as text (4); an `execute(...)` run where it stands (1, M9). H2's database pass adds its
  dialect's walls (JSON navigation, list lambdas, `encodeBaseN`).

## 2. Effects and seeds (M3d, M3e)

The second probe's dump files collided with the first's (both named by mode, process and millisecond), so its counts
survive for one pass: DuckDB's database pass -- 616 effect bodies planned whole, every script identical to the walk's
(3,622 scripts, 114,679 statements), the same numbers as M3e's own census, which covered all four passes on M3e's
code. One refusal is new there: a raw read bound by a let and read after a later effect (STAGED_GRID, T12, M3f's
named refusal).
