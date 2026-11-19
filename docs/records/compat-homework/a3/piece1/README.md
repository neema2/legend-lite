# A3, piece 1: the islands and the ###Relational dynaFunctions say SQL's SUBSTRING and POSITION by name (2026-11-19)

**The code:** local branch `claude/compat-a3-frontends`, commit `2a05b904` on `origin/main` `b75d6c73`. It is
**not pushed**: this session pushes only to `claude/compat-translator-spike` without the user's word. The same
commit is `0001-a3-piece1-frontends.patch` here (`git am` applies it).

**The change:**
- Two lite-internal natives, each the database's function verbatim:
  - `meta::legend::lite::sqlSubstring(s, start[, length])` lowers to `SqlFn.SUBSTRING`;
  - `meta::legend::lite::sqlPosition(s, t)` lowers to `SqlFn.STRPOS`.
- These are the calls the plain `substring` and String `indexOf` rules emit today.
- The 47 island call heads are retargeted: `CypherToPure` 21, `PostgresFunctions` 10, `IqFunctions` 14,
  `IqAggregates` 2.
- So are the 3 dynaFunction sites in `RelOpTranslator`: `indexOf`, `substring`, `position`.
- The islands name the natives by text, through `SqlToPure.SQL_SUBSTRING` and `SQL_POSITION`, because the protocol
  layer cannot import the catalog, as with `quotient`.
- `native-claims.tsv` is regenerated.

## Measured (`run-targets.txt`, `postgres-as-ubuntu.txt`)

| target | result |
|---|---|
| `//spec:corpus_duckdb`, `//spec:corpus_h2` | **green: no corpus move** (the dynaFunction split is byte-identical) |
| `//core:iq_parity_test`, `//core:sql_parity_test` | **green: IQ's showcase SQL unchanged** |
| `//core:cypher_parity_test` | 3 of 4 green; the 4th is the showcase pin, whose `hard/pure.txt` moved and whose SQL files did not |
| the rest of `//gates:local` (`--lockfile_mode=error`) | green except the guards below |
| the 12 Postgres and DataCube tests that refuse root | green as `ubuntu` |
| `//datacube:cube_page_shell_test` | a flake: red once (a menu assertion), green on a re-run |

## The guard moves it needs, for the user's word (none made)

| guard | the move | why |
|---|---|---|
| `NativeCatalogGovernanceTest.theVerifiedSetsOnlyShrink` | `INTERNAL_DESUGAR` ≤ 38 → ≤ 40 | the two natives |
| `PlatformNamesGuardrailTest.functionFqnLiteralsOutsideTheCatalogsOnlyShrink` | `FUNCTION_FQN_LITERALS_MAX` 346 → 347 | the two names' text in `SqlToPure`; the count is 347 |
| `spec/src/test/resources/com/legend/generators/ratchets.tsv` (read by `ImplementationTableTest`; `//spec:update_ratchets`) | `implementation.kinds.Intrinsic` 735 → 738 | the three overloads |
| `CatalogUpstreamDiffTest.census`, `NOT_UPSTREAM_MAX` | → 76 | the three overloads upstream does not declare |
| **9 text goldens** | re-blessed | each compares the text an island writes, `substring(` becoming `meta::legend::lite::sqlSubstring(` and `indexOf(` becoming `meta::legend::lite::sqlPosition(`; no answer moved |

The 9 goldens:
- `CypherToPureTest`:
  - `aMarkedDateTimeIsANanosecondTimeOfDayOrDateTime`;
  - `aPatternComprehensionIsANavigationFromTheBoundNode`;
  - `aStoredZonedValueIsItsInstantWithItsOffsetSlotAsItsZone`;
  - `keysOfANodeAreTheNamesOfItsPropertiesThatHoldAValue`.
- `IqFunctionsTest.substringReadsAnsiVirtualPositions`.
- `IqToPureTest.theAggregatesTypesAndChecks`.
- `PostgresToPureTest`:
  - `theLongTailOfTextAndNumbersIsPostgresDefinitionStepByStep` (`:547`);
  - `theReviewsFindingsOnTheLongTailStayFixed` (`:621`, split_part's `->indexOf('…') + 1)`).
- `CypherParityTest.theShowcasePlansItsPinnedSql`: `core/src/test/resources/cypher-parity/showcase/hard/pure.txt`, and
  `docs/CYPHER_SHOWCASE.md` as its update message says.

The shape note predicted 7 goldens and 1 guard. The real count is 9 goldens and 4 guards. The 2 extra goldens are the
IQ aggregates test and the Postgres split_part test. The 3 extra guards are the FQN-literal count, the
implementation-kind ratchet and the catalog's not-upstream count, which every new lite-internal native moves.

**The dated note for each:** *"2026-11-19, A3 of the compatibility program, stage A ratified by the user 2026-11-19:
sqlSubstring and sqlPosition, SQL's SUBSTRING and POSITION by name, which the islands and the ###Relational
dynaFunctions write so that Pure's substring and indexOf can take Pure's meaning (register S130, S134); no SQL
moved."*
