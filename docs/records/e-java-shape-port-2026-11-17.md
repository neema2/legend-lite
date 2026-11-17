# Shape port: what only PCT and the corpus exercise, into core (step 3 of the judge model, 2026-11-17)

**Why.** The user's judge model, ruled 2026-11-17 ("Love it"; the plan of record
`docs/records/total-wall-edge-program-2026-11-16.md` on `claude/total-wall-orch`, §12, 01:30 and 02:30):
- compatibility (PCT, the corpus) is judged by the HOST alone;
- CORE is the blocking check that both judges, isolated, agree on verdict and message under lite's rules.

So every shape the product supports must be exercised by core's own tests under both judges. This record lists the
assert × side-shape combinations that PCT or the corpus plan and core does not. Each one is ported as a lite test in
core, highest count first. A row closes when its core test lands and runs under isolated BOTH. Value types follow, one
per value kind, once every row here is closed.

**How it was measured.** The temporary shape census on the measurement-only branch `claude/total-wall-e-measure`
(`HostSides.plan`, `-Dlegend.measure.shapes`) recorded each planned assert's side shapes under the host judge on:
- core's 13 judged packages (`//core:core_tests_judges`);
- PCT channel B DuckDB's five suites;
- both corpora.

The side shape is `JudgePlans.Kind`, then `SideShape.kind`, then one value or many. In total: 67 distinct
assert × side-shape combinations in core, 21 in PCT, 69 in the corpus. Below are the 36 that are not in core, covering
1,017 planned asserts. With value types, 571 combinations are PCT- or corpus-only, against core's 148.

| # | Assert | Side shapes | PCT asserts | Corpus asserts | Core test | State |
|---|---|---|---|---|---|---|
| 1 | `quantified-assert` | VALUES/one, VALUES/one | 0 | 274 | | to port |
| 2 | `assertFalse` | VALUES/one | 161 | 38 | | to port |
| 3 | `assertSameElements` | VALUES/one, VALUES/many | 0 | 196 | | to port |
| 4 | `assertEquals` | CONSTANT/many, VALUES/one | 0 | 92 | | to port |
| 5 | `assertEmpty` | GRAPH/many | 0 | 28 | | to port |
| 6 | `assertSameElements` | CONSTANT/many, VALUES/one | 0 | 26 | | to port |
| 7 | `assertJsonStringsEqual` | CONSTANT/one, VALUES/one | 0 | 25 | | to port |
| 8 | `assertSize` | CONSTANT/one, VALUES/one | 0 | 23 | | to port |
| 9 | `assert` | VALUES/one, CONSTANT/one | 0 | 18 | | to port |
| 10 | `assertEquals` | VALUES/many, VALUES/one | 0 | 16 | | to port |
| 11 | `equal` | CONSTANT/one, CONSTANT/one, CONSTANT/one, CONSTANT/one | 0 | 14 | | to port |
| 12 | `assertSameElements` | CONSTANT/one, VALUES/one | 0 | 12 | | to port |
| 13 | `assertSameElements` | VALUES/one, TABULAR/many | 0 | 12 | | to port |
| 14 | `assertIs` | VALUES/one | 0 | 8 | | to port |
| 15 | `assertSameElements` | VALUES/one, VALUES/one | 0 | 8 | | to port |
| 16 | `equal` | CONSTANT/one, CONSTANT/one, CONSTANT/one, CONSTANT/one, CONSTANT/one, CONSTANT/one | 0 | 6 | | to port |
| 17 | `assert` | CONSTANT/many, VALUES/many, VALUES/one | 0 | 6 | | to port |
| 18 | `assertTdsEquivalent` | TABULAR/many, TABULAR/many, CONSTANT/one, CONSTANT/one | 2 | 4 | | to port |
| 19 | `assertSameElements` | CONSTANT/many, VALUES/many | 0 | 4 | | to port |
| 20 | `assertConversion` | (none) | 0 | 4 | | to port |
| 21 | `quantified-assert` | VALUES/one, VALUES/one, VALUES/one, VALUES/one, VALUES/one, VALUES/one | 0 | 4 | | to port |
| 22 | `quantified-assert` | CONSTANT/one, CONSTANT/one | 0 | 4 | | to port |
| 23 | `assertEqualsH2Compatible` | (none) | 0 | 4 | | to port |
| 24 | `equal` | CONSTANT/one, CONSTANT/one | 0 | 4 | | to port |
| 25 | `assertNotEmpty` | VALUES/one | 3 | 0 | | to port |
| 26 | `assertError` | VALUES/many | 2 | 0 | | to port |
| 27 | `assertEqWithinTolerance` | VALUES/many, TABULAR/many | 0 | 2 | | to port |
| 28 | `assertSameElements` | VALUES/many, CONSTANT/many | 0 | 2 | | to port |
| 29 | `assertEmpty` | CONSTANT/many, CONSTANT/one | 0 | 2 | | to port |
| 30 | `quantified-assert` | CONSTANT/one, CONSTANT/one, CONSTANT/one, CONSTANT/one | 0 | 2 | | to port |
| 31 | `assertNotEmpty` | GRAPH/many, CONSTANT/one | 0 | 2 | | to port |
| 32 | `assertJsonStringsEqual` | VALUES/one, VALUES/one | 0 | 2 | | to port |
| 33 | `forAll` | CONSTANT/one, CONSTANT/one, CONSTANT/one, CONSTANT/one | 0 | 2 | | to port |
| 34 | `assertTdsEquivalent` | TABULAR/many, TABULAR/many, CONSTANT/one | 2 | 0 | | to port |
| 35 | `assertEmpty` | VALUES/one | 2 | 0 | | to port |
| 36 | `assertNotEmpty` | VALUES/many | 1 | 0 | | to port |
