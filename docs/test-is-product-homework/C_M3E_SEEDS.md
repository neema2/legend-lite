# C3e. M3e: a test's seeds planned as `Setup` steps on the test door (the test-is-the-product plan's step M3e, 2026-10-31)

M3's brief (`docs/TEST_IS_THE_PRODUCT_PLAN_2026_10_31.md` §6 B12, item 7; D35) plans a test's seeds as `Setup` steps
of the test door's program, where the walk establishes a statement's contexts before it runs. The brief's
measurement: per statement, against the walk's seed texts, keys and skips.

## What was built

- `testplan/Seeds`, the one owner of a statement's seeds for the walk and the test door (moved from the body driver's
  `establishContexts`, `SeedSources` and `declaredSeed`): their **sources** (each `from()`'s inline test data and
  named runtime, the program's runtime; records compared by value, never the model's identity), the key the
  session's established state is checked by; their **steps** (`CsvSeed.planned`, the one owner of their form, which
  the query door's target takes too); and their **name in a plan** (`Seeds.key`: a hash of the steps' texts, so a
  plan stays one value and equal seeds share a name). The walk's `establishContexts` delegates to it.
- `testplan/TestKinds` places a `Setup` step before every statement whose sources name test data, unless the program
  placed the same sources since its last write (the walk's established state, within the program); an effect
  statement is a write, after which they are placed again. Across programs the runner skips a key the session holds
  clean (M3b). The steps are generated once per sources value. A step placed while a script is open closes it first:
  the program's order is the effects before the statement, then its seeds (the walk sends a statement's seeds before
  the open script; M3a measured that it never names test data there).
- `QueryDoor.Scan.kinds` reports every kind a statement holds, so a statement whose seeds the library places is still
  refused for any other kind it holds (an assert, M6).
- The test door's target carries no setup (R17 C6): its runtime's seeds are `Setup` steps of its program -- the same
  steps the query door's target carries (`TestDoorTest` pins the equality).

## The census

A research probe (`probes/M3eCensus.java.txt`: M3d's collector extended; hooks `probes/m3e-hooks.diff.txt`; never
committed as code) planned EVERY body the walk runs through `TestDoor.plan`. For each body the test door planned, it
compared the plan's `Setup` steps, in order, with the walk's establishments that have steps -- deduplicated within the
body since its last write, the program's own rule -- and checked that every seed the walk actually sent (the
session's state not holding it) is one of the planned steps. It also re-ran M3d's script comparison with M3d's
handle fold in.

| | core (28 packages) | DuckDB host | DuckDB database | H2 host | H2 database |
|---|---|---|---|---|---|
| bodies the walk ran | 320 | 3,187 | 3,187 | 3,187 | 3,187 |
| planned whole by the test door | 145 | 622 | 622 | 622 | 622 |
| ... naming seeds | **9** | 0 | 0 | 0 | 0 |
| ... `Setup` steps identical to the walk's establishments | **9** | -- | -- | -- | -- |
| ... every seed the walk sent a planned step | **9 of 9** | -- | -- | -- | -- |
| refused bodies naming seeds (they hold an assert: M6) | 60 | 12 | 12 | 12 | 12 |
| effect bodies planned (M3d re-run) | 9 | 616 | 616 | 616 | 616 |
| ... scripts identical | 8 (+1 whose walk failed: its scripts a prefix of the planned) | 616 | 616 | 616 | 616 |
| ... scripts / statements compared | 9 / 14 | 3,622 / 114,679 | 3,622 / 114,679 | 3,622 / 114,679 | 3,622 / 114,679 |

All runs green with the probe on: core 28/28, both corpora 6/6.

## Reading

- Where the test door plans a body that names seeds, its `Setup` steps are the walk's, statement for statement, and
  nothing the walk sends is missing from the plan. The corpus exercises no seed in the class the test door plans at
  M3e: its 12 seed-naming bodies per pass hold asserts, planned from M6; core's tests exercise 9. The comparison is
  re-run as each class moves (M5, M6).
- M3d's fold of handle-ended programs moved the 19 corpus setups that return their runtime from refused to planned:
  616 effect bodies per pass, every script identical.
- The core packages' sent statements are unchanged by M3e (the M1 method, the plan's §9).
