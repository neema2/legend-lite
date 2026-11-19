# H14's real slice: a relation's `toString()` computed by the database in the product (shape note, 2026-11-19)

**For the product track. A shape, not a plan to land: no PR, no landing.** Measured on the scratch branch
`claude/compat-table-text-db-spike` at `c604cd72` (V2 + D1, the ruled variant); the evidence is
`H14-table-text-in-db.md` (§0 to §10) and `H14-data/`.

- **The ruling (the user, ~18:05, "Agree with all four table printing recommendations"):**
  1. lite's own route prints a relation's `toString` in the database, replacing B17's refusal;
  2. a whole Float prints `2.0` (S311 changes);
  3. a date with a time prints lite's standard `...000000Z` (D1, not D2);
  4. it lands with stage B's B2, so there is no two-form switch.
- **The edge's half (the user, ~18:12, "Yes record it"; the rules of record on `claude/compat-program`):** where a
  relation's `toString` is the program's last step, the legend-pure route takes it over: the database returns typed
  rows and Java prints legend-pure's text. Where the text is used further in the query, the database computes lite's
  own text and the difference is recorded. No SQL is kept for legend-pure's spelling.

## 1. What the slice is, in one paragraph

The `Lowerer`'s `relation::toString` arm calls ONE new product lowering, unconditionally, under every plan style. That
lowering writes legend-pure's `#TDS` layout as one scalar subquery: a `STRING_AGG` in the relation's own order, each
cell the scalar `toString` rule's node for its type. The product's Float text keeps one fraction digit for a whole
value. An abstract `Date` the database holds as a timestamp prints as a DateTime. `RelationTexts` loses its
`relationToString` member. The edge deletes its `toString` SQL, because B2's Java presenter prints the edge's
last-step tables from typed rows. `toCSV` and a TDS's `csv` are not touched: they stay the edge's and the product's
refusals, until B2 takes them.

## 2. Files

### The product (this slice)

| File | Change | Lines | In the spike |
|---|---|---|---|
| `lowering/ProductRelationText.java` | **new**: `of(Styled, TypedNativeCall, Function<TypedSpec, SqlSelect>, String)`; the layout, the cells (`null` empty, `'null'` under a Variant, a Variant or a brace/bracket String quoted with `\` and `'` escaped, else the scalar rule: DateTime `styled.dateTimes()`, Float `Scalars.floatRepr`, the rest `Scalars.pureToString`), the order by `Render.liftOrder`, five refusals by name | +150 (spike 149, 118 code) | yes, as is but its javadoc's SPIKE words |
| `lowering/Lowerer.java` | the `relation::toString` arm: `ProductRelationText.of(styled(), tc, this::relation, nextAlias())` in place of `relationTexts.apply(() -> TextRefusals.relation("toString")).relationToString(...)` | −2 / +1 | yes |
| `lowering/RelationTexts.java` | the `relationToString` member and its javadoc line deleted; the class javadoc names `toCSV` and `csv` only | −3 | **no** (the spike left the facet whole) |
| `lowering/TextRefusals.java` | `relation(what)`'s javadoc drops `toString` (the call site is gone) | ±0 | no |
| `lowering/Scalars.java` | D1 in `pureToString`, before the Boolean arm: an abstract `Date` whose slot is `TIMESTAMP`/`TIMESTAMPTZ` takes `styled.dateTimes().dateTimeText(x, null)` | +5 | yes (`9bcbd165`) |
| `sql/dialect/AnsiSqlRenderer.java` | V2 in `trimmedSix` and `trimmedSixStandard`: `'.000000'` marked `'.#'` before the trailing-zero trim, the mark a `0` after; the value still read once | ±0 (javadoc amended) | yes (`487f3e40`) |
| `sql/dialect/Snowflake.java`, `MySql.java`, `MongoDb.java` | **the whole value's `.0` in the three forms V2 did not reach:** Snowflake's arm from 1e32 and MySQL's from 1e60 (the leading 21 digits and zeros) append `.0`. MongoDB's whole form (its `$rtrim` arm and its spelled-digits arm from 1e34) marks and restores as the helpers do | about +10 | **no**: not built, not measured. Without it S311's "one text on every database" breaks on those three |

### The edge (in the same landing, because the slice lands with B2)

| File | Change | Lines |
|---|---|---|
| `edge/RelationTextLowering.java` | `relationToString`, `lowerToString`, `tdsString`, `tdsCell`, `dateOnly` and `dateTimeTextTds` deleted: the edge's `toString` SQL ("No SQL is kept for legend-pure's spelling"). `toCsv`, `csvProperty` and their helpers stay: `perSlot`/`kindType`, `printedCell`, `withSortOutputs`, `hasDynamicPivot`, `dynamicPivotText`, the date texts the CSV cell reads | about −240 of 791 |
| B2's presenter (B2's own files) | **prerequisite:** it claims every last-step `toString` of a relation on the edge's routes before lowering. That covers an assert side's root (H11's `RelationTextArm`: PCT 431 of 431) and a query whose answer is the text: pure/v1's execute, and `FloatTextTest`'s edge query. H11 proved the root door rewrite on a test, not on a route (H11 §6 and its open row "A root's door rewrite on the real routes") | B2's |

**Not carried from the spike:** `H14FloatTextProbeTest`, `H14DateCellSizeTest`, the `_CORE_MANUAL_PROBES` entry, and
the two PCT harness edits (`ChannelB`'s `-Dpct.channelb.unclipped`; `PctBackend.connect` opening the product's database
under `-Dpct.channelb.rules=LITE`). The second one is what any non-DuckDB LITE lane needs, so it belongs to the H2
track's step 0, not here.

## 3. Tests and ledgers that move (measured at V2 + D1, `H14-table-text-in-db.md` §10; every row in `H14-data/`)

| Where | Rows | What the slice does |
|---|---|---|
| Core: V2's Float pins | `FloatTextTest.aFloatsTextIsSixDecimalsTrimmed`, `TypeInferenceIntegrationTest.testFloatToStringWithNegativeExponent`, `AuditRound3Test.floatReprLargeBand`, `BigQueryDialectTest.aFloatsTextIsSixDecimalsTrimmed`, `SnowflakeDialectTest.aFloatsPrintFormsAreSpelledHere_theValueReadOnce` | re-pinned to `2.0`, `0.0`, `100000000000000000000.0` and the marked SQL; dated, naming the ruling |
| Core: the product's refusal | `RenderCsvIntegrationTest.theProductRefusesARelationsTextByName` | its `toString` entry leaves (toCSV and csv stay). **New:** a product test of the text on each lane (the layout, a whole Float, a DateTime, an abstract Date with a time, an empty cell, the order) |
| Core: a raw grid | `TaggedListTest.anUntypedGridsCellsAreReadOnTheVariantLane` | pins the new refusal's words ("a relation's toString over a raw grid (executeInDb): its columns exist only when the plan runs") |
| Core: the edge style | `FloatTextTest.anEdgeRelationsTextPrintsAFloatCellAsLegendPure`, `DynamicPivotTextRefusalTest.aDynamicPivotsTextIsRefusedByNameAtLowering` (its `toString` half) | both are a last step, so they are B2's: the first passes again once B2 prints it (`0.00001`, `100000000000000000000.0`); the second becomes B2's answer from the pivot's rows, or B2's refusal. **New:** a product test of the unnamed pivot's refusal |
| Channel A, lite's rules | `Test_LegendLite_LiteRules_EssentialFunctions_PCT`: `testFloatToStringWithExcessTrailingZeros`, `testFloatToStringWithPositiveExponent` leave; `testFloatToStringWithNegativeExponent`'s text `'0'` becomes `'0.0'` | 3 rows |
| Channel B, `duckdb-lite` (`channelb/ledger.tsv`) | 392 leave (Relation 335, Standard 55, Essential 2); 34 change their text (33 abstract-Date tables: the refusal becomes `...000000Z` against `.000+0000`; 1 Float `0` to `0.0`) | 426 rows: every one of the 423 refusal rows, and 3 Float rows |
| **Must not move (run on the slice, not run here with B2):** | the compatibility lanes: PCT channel A COMPAT (`pct_duckdb`, `pct_sqlite`, `pct_postgres`, `pct_h2`), `pct_channel_b_host_*`, the corpus on both lanes and both judges (`//spec:corpus_duckdb`, `//spec:corpus_h2`) | B2 takes every root, so none should move. The spike measured `pct_channel_b_host_relation` 469/469 and `_standard` 205/205 unchanged without B2 (§9). The corpus was not run with the change |

**PCT channel B under lite's rules (DuckDB, of 1,249): 1,179 at V2 + D1, the base 787.**

## 4. Guard rows

**No guard row is needed for the product half.** At V1, V2 and V2 + D1, `core_tests_architecture` (`ArchitectureTest`),
`core_tests_root` (`JavaEvalLedgerTest`, `HookRulingTest`, `HookLambdasTest`, `EdgeSeparationTest`,
`OpenClassMembersTest`, `PlanStyleSurfaceTest`) and `core_tests_testplan` all passed with the new lowering in place.
`lowering/` is not one of `JavaEvalLedgerTest`'s registered packages. What moves comes from the deletions, each with a
dated justification comment naming this ruling:

| Guard | Row | Move |
|---|---|---|
| `OpenClassMembersTest` | `com.legend.lowering.RelationTexts`'s member set | `public abstract ... relationToString(...)` leaves (shrink) |
| `JavaEvalLedgerTest` | the edge register's `RelationTextLowering.java` | its comment's "a relation's toString" leaves; the file stays (toCSV, csv) |
| `EdgeOnlyNodesCensusTest` | the `RELATION_TEXT` comment line | comment only |
| `HookRulingTest` | `PlanStyleFacets.TWO_FORMS`; the facets map's `RelationTexts`; `RelationTexts <- RelationTextLowering` | **unchanged**: no two-form kind (ruling 4); the facet stays for `toCsv`/`csvProperty` |
| `EdgeSeparationTest` | the four B17 HOOK-RULED rows | unchanged while the facet stays |
| `ArchitectureTest.PLANNER_ENTRIES` | `RelationTexts#pureToString`, `#kindType` | unchanged (the CSV cell and `PureListTexts` still read them) |
| `HookLambdasTest` | `PlanEnv#configure`'s two facet reaches | unchanged |
| `planner/PlantedHostDoors.RelationText` | abstract, implements the facet | compiles unchanged |

When B2 also takes `toCSV` and `csv`, the facet itself goes: that is B2's guard change (the B17 rows, the facet map's
row, `TWO_FORMS` untouched), not this slice's.

## 5. The register rows (in `docs/records/d-register-rows-2026-11-16.md`, which move into the register when D lands)

**S311, amended (the product's Float text):**
- **lite:** "six decimals with the trailing zeros trimmed" becomes "six decimals with the trailing zeros trimmed,
  **a whole value keeping one** (`1.5`, `2.0`, `0.3`, `0.333333`, `1234567.89`, `100000000000000000000.0`; below
  0.0000005, and a negative value that rounds to zero, `0.0`)".
- **The dialect line:** each format call's text marks a whole value's `.000000` before the trailing trim and restores
  one `0` after (`AnsiSqlRenderer.trimmedSix`, `trimmedSixStandard`). The large-magnitude forms append `.0`
  (Snowflake from 1e32, MySQL from 1e60, MongoDB from 1e34).
- **Why:** add the user's ruling of 2026-11-19 ~18:05 (the second recommendation: "Is there any cheap/lean sql way to
  get whole floats to be what people expect 2.0?", ~16:45, answered: one more `REPLACE`, the value read once).
- **Evidence:** add H14's scratch probe on DuckDB, H2, SQLite, Postgres, MySQL, MariaDB, ClickHouse and Trino (H14
  §6), plus PCT's three `testFloatToString*` tests. The slice re-measures with the row's own
  `//core:product_raise_probe`, its values given whole ones (`2.0`, `-2.0`, `0.0`, `1e20`, `1e33`, `1e61`, `1e300`), on
  those databases and MongoDB; Snowflake, Databricks and BigQuery on their lanes. The divergence from legend-pure that stays: six decimals
  against the shortest round-trip digits (`0.00001` is `0.00001`; `0.30000000000000004` is `0.3`; `1e-7` is `0.0`).

**S312, amended (the product's DateTime text):**
- **The "Not yet" sentence goes.** It read: "a relation's `toString`/`toCSV` (the TDS text, D1.3) and the writers'
  cells (D1.4) keep Pure's form".
- **In its place:** "a relation's `toString` writes each DateTime cell in this text (S316 (b)). An ABSTRACT `Date`
  the database holds as a timestamp (a value with a time; legend-pure's `hasHour` is a precision fact) takes this
  text too, in a scalar `toString()` and in a table's cell: `2026-01-07T00:00:00.000000Z`. Until then each
  database's own VARCHAR cast answered: `2026-01-07 00:00:00` on DuckDB, H2 and Postgres, `2026-01-07 00:00:00.000000`
  on SQLite, a product parity gap (G2) now closed."
- **legend-pure's side:** a relation's text prints such a cell `2026-01-07T00:00:00.000+0000`. Its TDS reads the
  value as a DateTime at three subsecond digits; the edge re-spells it from the typed value (rule 3).
- **Why:** add the user's ruling of 2026-11-19 ~18:05 (the third recommendation: lite's standard, D1, not D2's
  legend-pure form). A `DATE` slot keeps its day (StrictDate), unchanged. `toCSV` stays refused in the product.

**S316 (b), amended (the refusal this replaces; the rows S311 and S312 alone do not cover it):**
- **The product's half:** "a relation's `toString`, `toCSV` and a TDS's `csv` are refused by name" becomes "`toCSV`
  and a TDS's `csv` are refused by name. A relation's `toString` is lite's own text, computed by the database. The
  layout is legend-pure's `#TDS`: `#TDS`, the names line, one `   `-prefixed line per row in the relation's order,
  `#`; a name quoted unless `^[a-zA-Z0-9_]+$`; an empty cell `null`, `'null'` under a Variant; a Variant's text and a
  String holding a brace or a bracket quoted. Each cell is the product's text of its value (S311, S312, the scalar
  `toString` rule)."
- **Five refusals by name, none met in PCT:**
  - `toString(rel, true)` (types and multiplicities);
  - a raw grid (`executeInDb`);
  - a pivot whose columns nothing names;
  - a mixed (tagged) column;
  - a DISTINCT or star select ordered by a key that is no column.
- **The edge's half:** "a relation's `toString` that is the program's last step is legend-pure's text, printed in
  Java from lite's typed rows (B2). One used further in the query is lite's own text, computed by the database (the
  user, ~18:12, 'Yes record it')." Measured: 0 such further uses among PCT's and the corpus's judged sides (H11
  §13.1).
- **Evidence:** the product test of §3, the two ledgers' moves, and PCT channel B at 1,179 of 1,249.

**One note for the user's batch:** the layout's empty-cell token is legend-pure's `null`, while S316 (a) makes a
TDS cell's own `toString` the empty value. These are two functions with two texts. H14 kept the layout exactly
legend-pure's (390 of PCT's 423 pass byte for byte that way), so that is not a divergence from upstream. Inside the
product, though, the empty value prints as `null` in one place and as nothing in the other; the user may want to
rule on it.

## 6. Size

| | Lines |
|---|---|
| Product, added | about +165: the new lowering 150, `Scalars` 5, the three large forms about 10, the `Lowerer` and the helpers net 0 |
| Product, removed | about −5: the facet member, the refusal's call |
| Edge, removed (with B2) | about −240 of `RelationTextLowering`'s 791 |
| **Net** | **about −80** |
| Tests | 9 core cases re-pinned or moved to B2, 2 new product tests; ledgers 3 + 426 rows |

**SQL** (DuckDB, PCT's table tests, the host judge's statements):
- The relation suite's 365: **660,748** characters, against 696,964 with `main`'s edge builder (−5%).
- The standard suite's 58: **99,037**, against 194,885 (−49%).
- V2 + D1 adds 942 and 1,443 characters to V1's.
- Per cell: a Float +33 characters (+21 with the standard trim); an abstract Date with a time 18 → 36 on DuckDB,
  15 → 166 on SQLite, 18 → 53 on Postgres, 18 → 76 on H2.
- The edge's last-step tables, printed in Java by B2, send only their rows: 187,864 for the relation suite (H11
  §4.6).

## 7. Checked against the rules of record (`docs/records/compat-rules-of-record-2026-11-19.md`)

| Rule or tenet | Holds? | How |
|---|---|---|
| Rule 1, the database calculates everything, `toString` included | yes | the whole text is one scalar subquery the database computes. Java writes only the names line at planning, from the plan's typed columns: a fold of facts fixed by the types ("a query's column layout worked out from constants") |
| Rule 2, product presentation is thin | yes | the text is a VARCHAR value, and the writers serialize it unchanged |
| Rule 3, compatibility re-spells in Java from typed output only | yes | the edge's last-step tables are B2's Java from typed rows; the edge's `toString` SQL is deleted |
| Rule 4, judges | yes | unchanged: both of the product's judges compare text the database computed. At V1 the `duckdb-lite` outcomes were the same under `BOTH` and under the host judge alone |
| Tenet 1, the database computes | yes | as rule 1 |
| Tenet 2, Java decides nothing about a value | yes | name quoting is a fact of a column's name, decided at planning; no cell is touched in Java |
| Tenet 3, compatibility re-spells, never re-computes | yes | B2 prints from typed values; the 33 Date tables differ from PCT by spelling alone (H14 §8: one text on every database) |
| Tenet 4, upstream is the spec, never the design | yes | the layout is what the answer is; the computation is the product's own nodes (`STRING_AGG` over `liftOrder`, the scalar rule's cells); cells are lite's texts, not legend-pure's |
| Tenet 5, two judges in the product | yes | unchanged |
| Tenet 6, no modes in the core | yes | the arm is unconditional: no facet member, no `TWO_FORMS` row, no switch. The cells read the planner's `styled` exactly as the scalar `toString` does, with no read of the style of their own |
| AGENTS.md invariants 2, 3, 3a and 4 | yes | the lowering emits typed MIR only (`StringLit`, `Call` of `SqlFn`, `SqlAgg.Reducer` `STRING_AGG`, `Case`); no SQL text or function name; no fallback: each shape it cannot write is refused by name, and an impossible state throws |

**One reading to confirm (not measured; nothing in PCT or the corpus reaches it).** The ~18:12 ruling says a text used
further at the edge is "lite's own text". The cells follow the scalar rule through the planner's `styled`. So until
B2 retires the edge's two-form value texts (`DateTimeTexts`' edge form, the edge's Float print), a cell of such a
table under the edge style would take the edge's value spelling inside lite's layout. If the ruling means exactly the
product's text even before that, `ProductRelationText` passes `Styled.PRODUCT` in place of the planner's: one
argument, no branch.

## 8. Known product defects this does not change

- **H11's P1 is hidden again:** a window computed after a list operation reads it in a plain product query. Building
  the text wraps the relation as a subselect, so `testExtendJoinStringOnNull` passes on SQLite and Postgres; the
  product defect itself is unchanged.
- **H2:** the text needs list forms H2 lacks. The 4 `LIST_GET` tests (H11 §4.2) and H2's 35 capability refusals stay
  H2's, for the H2 track. Lite's own PCT lanes run DuckDB alone today.
