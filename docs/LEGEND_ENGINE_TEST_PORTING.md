# Legend-engine test estate — what to run, what to port

Survey of the REAL legend-engine (and legend-pure) test suites, judged by
whether legend-lite can consume them. Context: PCT is function-level only
(near-zero class/mapping coverage); this doc is the plan for everything
else. Companion scoreboards: docs/SCOREBOARD.md (engine-lite corpus),
docs/PCT_BURNDOWN.md (PCT).

## The crown jewel: the `core_relational` Pure corpus (RUN-as-data)

`legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/`
`legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/`
`src/main/resources/core_relational/relational/`

~524 `.pure` files, **~2,538 `test.Test` functions**, ~2,270 `execute()`
calls, ~3,400 `assertEquals`, **~502 `assertSameSQL` golden SQL strings**.
Each test is DATA: a Pure query + a mapping + a runtime + expected result
rows + (often) the expected SQL, with `createTablesAndFillDb()` seed
helpers, all in `.pure` source. Runs against H2 in the engine's harness —
legend-lite consumes the *inputs and expected outputs* directly with its
own runner and DuckDB/H2.

This corpus IS the class/mapping spec PCT lacks:

| feature family | tests |
|---|---|
| TDS ops (`relational/tds/`) | 281 |
| milestoning (`relational/milestoning/`) | 254 |
| graphFetch incl. cross-store (`relational/graphFetch/`) | 165 |
| union / operation mappings (`tests/mapping/union/`) | 127 |
| sqlFunction-in-mapping | 75 |
| embedded mappings | 70 |
| inheritance | 51 |
| classMappingFilter + innerJoin | 32 |
| extends (mapping inheritance) | 31 |
| join | 29 |
| aggregation-aware (`relational/aggregationAware/`) | 28 |
| enumeration | 27 |
| relation-function mappings | 25 |
| association | 24 |
| distinct 18 · groupBy 11 · tree 13 · multigrain 6 · dynaJoin 6 · propertyfunc 6 · selfJoin 3 · inClause 4 · include 1 · merge 1 | ~70 |
| semi-structured (`tests/semistructured/`, 13 files) | — |

Plus golden-SQL-only suites: `pureToSQLQuery/tests/`,
`sqlQueryToString/` (~14), `sqlDialectTranslation/` (~21), per-dialect
generation tests.

**Plan (the next burn-down after PCT):**
1. Build a `relational-corpus` test module: a runner that parses a
   `.pure` test file, materializes the seed tables, executes each
   `test.Test` body through core, and diffs rows (and, where present,
   SQL — dialect-translated goldens are H2-flavored; row equality is the
   primary assertion, `assertSameSQL` a secondary advisory diff at first).
2. Start with families we claim to support (association, join, embedded,
   enumeration, distinct, groupBy, tds) — expect a burn-down table like
   the last two.
3. Families we don't support yet (milestoning, union, extends,
   aggregation-aware, multigrain) become the MAPPING feature roadmap with
   the corpus as ready-made acceptance tests — no test authoring needed.

## Second: self-contained data-driven fixtures (RUN-as-data)

- Mapping `testSuites` blocks + `MappingTestRunner` fixtures
  (`legend-engine-core-testable/legend-engine-test-runner-mapping/`).
- Service tests (`legend-engine-xts-service/legend-engine-test-runner-service/
  src/test/resources/`, 32 files): grammar or protocol-JSON files carrying
  model + mapping + runtime + CSV/SQL seed data + expected JSON rows in ONE
  file. Dozens, not thousands — the ideal end-to-end smoke layer, zero
  Pure-helper dependencies.

## Third: grammar round-trips (PORT-by-hand, scrape the strings)

~268 core `@Test` (TestMappingGrammarRoundtrip 21, Domain/Runtime/Lambda/
Connection) + ~90 relational (`TestRelationalGrammarRoundtrip` 26,
`TestRelationalMappingGrammarRoundtrip` 11, parser negatives 10, connection
20) + per-dialect connection grammars (~15 dialects). Java-embedded strings
— the mechanism (parse → compose → assert-equals-input) is trivial to
replicate once legend-lite has a composer; the strings scrape cleanly.
Also validates the 18 @Disabled grammar gaps (scope, extends, XStore...).

## Fourth: compilation diagnostics (PORT-by-hand, selective)

`TestDomainCompilationFromGrammar` (131), `TestMappingCompilationFromGrammar`
(56), `TestRelationalCompilationFromGrammar` + embedded/mapper/accessor
variants (~59 files engine-wide). The NEGATIVE cases (expected error
message + line/column) are the unique value — they pin diagnostics no other
suite covers.

## Skip

- `legend-engine-xt-relationalStore-executionPlan/src/test/` (55 classes):
  plan-executor internals over engine-internal JSON plans — not a
  clean-room spec.
- `legend-pure-store-relational`: effectively empty (5 files, 0 tests) —
  all relational testing migrated to legend-engine.
- M2M `core/pure` suites (~493 tests): revisit when M2M scope grows beyond
  the current H5 slices.

## Order of battle

1. PCT burn-down to green (in flight — docs/PCT_BURNDOWN.md).
2. `relational-corpus` runner + first families (association/join/embedded/
   enum/tds) — the new primary scoreboard.
3. Mapping roadmap = the unsupported corpus families, in corpus-count
   order: milestoning (254), union (127), extends (31), aggregation-aware
   (28), multigrain (6).
4. Service-test fixtures as the end-to-end smoke gate.
5. Grammar round-trip scrape + compilation negatives.
6. Real-world model compiles (fibo/cdm/gs-quant checkouts) as the
   backwards-compat gate.
