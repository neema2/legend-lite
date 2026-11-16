# PR1's corpus database registers, DE5 groups 1 and 2 (records, 2026-11-15 and 2026-11-16)

These two entries were first written into `docs/GATES.md` on PR1's branch. They move here because no PR edits a shared
document (the user's rule of 2026-11-15). `docs/GATES.md` is restored to main's version.

### 2026-11-15 — E: DE5's first group taught: a nested quantified assert gated by its own rule (the corpus's database registers)

The user's ruling DE5 (a), 2026-11-15 ("Def teach the judge the remaining"): the database judge plans the corpus bodies it
refused before any lane switches to it alone. Group 1: the twelve testDataGeneration bodies, whose inlined
`createTableRowIdentifiers` asserts every column of every row identifier exists -- `$identifiers->map(i |
$i.columnValuePairs->map(cv | assert(...)))`, a quantified assert whose elements are quantified asserts. The database
judge's static gate unrolled the outer level and handed each element's root, itself a `map`, to `gateOf`, which dispatches
on an assert's function and answered UNPLANNED. `AssertVerdicts.databaseGate` now gates such an element by the quantified
rule itself, recursively, as `planQuantified` already planned it.

REGISTERS (both lanes, DuckDB and H2):
- `<lane>-database-accepted-register.txt`: testConstant, testSimpleSingleTable, testSimpleSingleTableWithNoDataToInsert
  and testSimpleTwoTable move from `judge-parity-gap:database-planner-refuses(P5)` to the host roster's own entry,
  `by-design:test-data-generator-fetch-text(S193)`: the database judge now reaches the host's verdict, the same
  engine-text failure the host lane accepts.
- Four `alloy::` bodies' P5 entries are removed (testConstant_Alloy, testSimpleSingleTable_Alloy,
  testSimpleSingleTableWithNoDataToInsert_Alloy, testSimpleTwoTable_Alloy): they PASS under the database judge.
- testQualifier and the three milestoning `alloy::` bodies keep their P5 entries: past the map, their next refusal is the
  `assertEqualsH2Compatible` helper the gate does not reduce (DE5's next group, with testModelConnectionJoin's let that
  raises at planning).
- `<lane>-database-engine-order-register.txt`: the bodies that now run under the database judge, re-blessed by
  `bazel run //spec:update_rcorpus_<lane>`.

### 2026-11-16 — E: DE5's second group taught, pulled forward for PR1 (the corpus's database registers)

The orchestrator's call under the user's delegation (2026-11-15): BOTH is reachable through the public API
(`ExecuteOptions.withJudgeMode`), so the five corpus bodies DE4's deletion of the second road had stopped planning under
BOTH are carried before PR1 lands. Each was refused by the DATABASE judge's static gate:

- testQualifier and the three milestoning `alloy::` bodies end in `assertEqualsH2Compatible`, the dual-golden root.
  - Its text arm declines these shapes. `planHost` plans the root as its own meaning on our H2,
    `assertEquals(upgraded, actual)`.
  - But `AssertVerdicts.gateOf` looked for a verdict native on the helper's own name and answered UNPLANNED.
  - It now gates the rewritten equality.
- testModelConnectionJoin's `let res = executionPlan(...)` raises a model error at planning (a class its mapping does
  not map).
  - The host judge's planner fails the body with it.
  - The database half's static pass now raises it too (a LegendCompileException), instead of refusing the body.

REGISTERS (both lanes, DuckDB and H2):
- `<lane>-database-accepted-register.txt`: testQualifier moves from `judge-parity-gap:database-planner-refuses(P5)` to
  the host roster's own entry, `by-design:test-data-generator-fetch-text(S193)`: the database judge now reaches the
  host's verdict, the same engine-text failure the host lane accepts.
- The three milestoning `alloy::` bodies' P5 entries are removed: they PASS under the database judge.
- testModelConnectionJoin is in neither register: it fails with the host's own MappingResolutionException under every
  judge (both fail rosters already list it).

Pinned by `DatabaseHalfDe5Test`, each test red with its fix reverted.
