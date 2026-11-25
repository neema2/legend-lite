# R2: the SQL-difference inventory for the editions (2026-11-25, night)

*For the user's call on how far `engine` (and `pure`) should go toward full compatibility: "if we are going to be doing
some rewriting anyway, maybe worth going as full compatibility as possible so users dont freak out - but we should do
inventory and see what is cheap vs expensive in the rewrites and make a call". Read-only research on `/home/user/s2` at
`da231244`, plus one light probe outside every checkout (DuckDB 1.3.0 and 1.5.6, H2 2.1.214 and 2.5.252 run from the
cached driver jars, §6.2). Nothing was edited, built or committed.*

**Path roots used below.**
- `L/` = `/home/user/s2/core/src/main/java/com/legend/`
- `ENG/` = the pinned legend-engine source, `/root/.cache/bazel/_bazel_root/a6663fbd60fad5804a563aa83422814f/external/+http_archive+legend_engine_src/`
- `ENGR/` = `ENG/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/`
- `P2S` = `ENGR/pureToSQLQuery/pureToSQLQuery.pure`
- `DEF` = `ENGR/sqlQueryToString/extensionDefaults.pure`
- `H2X` = `ENGR/sqlQueryToString/dbSpecific/h2/h2Extension2_1_214.pure`
- `PGM` = `ENGR/sqlDialectTranslation/toPostgresModel.pure` (legend-engine's H2 route: see §6.1)
- `DUCKX` = `ENG/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-dbExtension/legend-engine-xt-relationalStore-duckdb/legend-engine-xt-relationalStore-duckdb-pure/src/main/resources/core_relational_duckdb/relational/sqlQueryToString/duckdbExtension.pure`
- `MD/` and `MH/` = legend-engine's PCT manifests `.../pct-manifests/relational-duckdb/` and `.../relational-h2/`
- `PV1/` = `/home/user/s2/tools/engine-runner/src/test/resources/pure_v1/` (legend-engine's own pure/v1 answers); `EDGE/` = `/home/user/s2/core/src/test/resources/edge/` (lite's pure/v1 net)
- `R1/` = `/home/user/s1/docs/records/edition-homework/r1/`
- Row ids: **T-n**, **P-n**, **S-n** are this inventory's. `S383`-style ids (no hyphen) are register rows of `docs/SEMANTICS_REGISTER.md`.

---

## 0. The answer on one page

**Counts.** 14 typing rows (T), 16 presentation rows (P) and **22 genuine SQL rows (S)**, of which 18 apply to
`engine`, 6 to `pure` (S-14 to S-16 apply to both).

**The headline.** Under rule zero, **`pure` needs almost no SQL of its own**: lite's product already answers Pure's way
(M6: every Pure-mode failure on DuckDB is a ledgered database limit). The five `pure` S rows are all either EXPENSIVE
(written precision, Decimal scale) or recorded already. `pure` = the product's SQL + the T answers + the presenters. What
`pure` needs is mostly *removal*: today PCT's legend-pure door gets legend-engine's meanings through the shared
`UPSTREAM` style (plain parses, `TDSNull`, the DECIMAL literal, H2's settings, `isNumeric`, `parseDecimal(5,2)`, lenient
`parseJSON`), census hard case 1.

**`engine` is where the cost is, and most of it is already built.** Of the 18 `engine` S rows:
- **10 are CHEAP because the SQL form exists in lite today** (at pure/v1's door, or at every edge door): the unchecked
  raises, the plain parses, `substring`/`indexOf`/`toOne` at pure/v1, `isNumeric` (DuckDB), `parseJSON`, H2's
  settings, the null-unsafe flag, `TDSNull` in a computed text, a Float's text used further.
- **3 are CHEAP but new** (keyed counts not folded, a DateTime's text used further as the database's cast, `parseDecimal`
  per database).
- **2 are MEDIUM** (extending `substring`/`indexOf` exactly to function bodies, mapping code and the corpus needs the
  placement pass the second review asked for; run-time-kind comparisons per ruling 7, much of which may be moot because
  legend-engine refuses `Any` lists).
- **3 are recommended K by earlier rulings** (the DECIMAL literal and engine's DECIMAL division; legend-engine's golden
  defects; database versions).

**The raises (§3).** Option (b), plain everywhere under `engine`, is already built for pure/v1 and PCT's engine lane and
moves 0 corpus verdicts (M3). Option (a), lite's checks everywhere, deletes ~50 lines and moves 4 of the 7 `raise.*`
pure/v1 goldens on H2 and 6 PCT engine-lane tests on DuckDB (to passing). The real user-facing difference: under (a) one
row with a zero divisor fails the whole query where legend-engine gives that cell `null` (DuckDB `rem`, H2 `sqrt`) or `[]`
(DuckDB `slice`, `range`). **Recommendation: (b) for the database part under `engine`, checks kept in `lite` and
`pure`.** Two loose ends either way: three of lite's four `toOne` guards ignore the style today, and DuckDB's
`x / 0` is `Infinity` on both DuckDB versions. legend-engine's `null` there is its JSON writer's, so it is presentation.

**legend-engine's known wrong answers (§4).** DuckDB has 47 in scope (22+5+1+14+5; the brief's 42 leaves out
Unclassified's 5):
- 28 are presentation or type;
- 17 are genuine translation bugs;
- 2 are typing.

Reproducing all 47 means 16 families: 6 CHEAP (11 tests), 7 MEDIUM (33 tests), 2 EXPENSIVE (2 tests), and 1 already
reproduced. Most of the MEDIUM ones would reverse a ruling (S399's instant equality, lite's Decimal typing).
**Recommendation: reproduce none**, except where the same SQL is already wanted for a deliberate reason: the 4
`substring`/`indexOf` ones, if the user keeps today's pure/v1 behaviour (S-3, S-4). H2 has 54 in scope (+1 Variant):
26 genuine, 26 presentation or type, 2 typing.

**New facts the user should know (§6):**
1. legend-engine 4.145.0 sends every H2 SELECT through its Postgres-model translation (`relationalMappingExecution.pure:439-442`), where `isNumeric` is a regex full match (`PGM:367`), not `lower = upper`. Register S370 may be stale for H2.
2. The edge's `parseDecimal` writes H2's `DECIMAL(5,2)` on every database. legend-engine's DuckDB writes `cast(x as decimal)`, which DuckDB types `DECIMAL(18,3)` (probed: `'123.450021'` gives `123.450`).
3. DuckDB's cast accepts `'1.5'` (answers 2), hex, `'1e3'` and whitespace for `parseInteger` (probed). So legend-engine's plain parse is a different *answer*, not only a different error.
4. Three of lite's four `toOne` guards ignore the plain-raise style (`L/lowering/Coercions.java:63-67`, `:73-90`; `L/lowering/ToOneGuard.java:68-72`).
5. The edge writes legend-pure's DateTime print inside SQL at legend-engine's door too. legend-engine's own is the database's cast (`2014-01-01 00:00:00`, `DUCKX:284`, `PGM:404`; its PCT manifests' wrong values).
6. The edge's `keyedCounts` folds a keyed count to 1 at every door (`L/edge/EdgeLoweringForms.java:103-106`), though S147 says legend-engine counts what the data holds.

---

## 1. Every difference, classified

The sources: the census (`edition-census-2026-11-25.md`), the first review's §3 and the second review's M1 to M11,
v2's §5 rows 5.1 to 5.20, B2's homework (`e3-design.md`, `engine-m2m-xstore-equality-2026-11-25.md`, M1 as summarized in
the amendments), `docs/SEMANTICS_REGISTER.md`, `docs/DIFFERENCES_FROM_LEGEND_ENGINE.md`, M3 to M7, and the corpus's
accepted rosters. Each difference is in exactly one bucket. Where a v2 §5 row splits (a last-step text is P, the same
text used further is S), the split is named and each part counted once.

### 1.1 (T) Typing: lite refuses the program (at compile or at planning), the mode answers. No lite SQL to change.

| # | Difference | lite (where it refuses) | How `engine` answers | How `pure` answers | Today / cost |
|---|---|---|---|---|---|
| T-1 | Comparing two known, different types (`==`, `!=`, orderings, `in`, `contains`, `indexOf`, the asserts, `max`/`min`). v2 5.1; S127 | the typer marks `TypedCrossTypeComparison`; the planner refuses it (step 5a) | the comparison the typer typed, unwrapped: the database's promotion (`L/edge/EngineComparisons.java:28-37`). legend-engine's in-memory M2M and XStore also answer `true` (B2's `engine-m2m-xstore-equality-2026-11-25.md`), so no placement is needed for numbers | Pure's: equality `false`, membership `false`, `indexOf` `-1`, an ordering as the database orders it, and a refusal where both sides may be empty (`L/edge/PureComparisons.java:44-74`) | built for the query; E2 extends it to bodies and mapping code. CHEAP per row, MEDIUM for the E2 reach |
| T-2 | An enumeration against a String, or against another enumeration's value (static). S169; PCT `testEqEnum`/`testEqualEnum` | marked as T-1. But `LiteralUnroll.neverEqual` folds a spelled pair to `false` at G½ on every door (`L/compiler/spec/LiteralUnroll.java:389-438`) | the database compares the stored value or name. legend-engine's PCT answers `TestEnum1.FIRST == TestEnum2.FIRST` true, and lists it as a failure (`MD/GrammarFunctions_manifest.json`, `testEqEnum`). lite answers `false` today even at the engine door, because the fold runs after the door | `false` | the fold half is S-11 |
| T-3 | Upstream text as a value (`planToString`, `toSQLString`, `toDDL`, `executionPlan`, `executeLegendQuery`). v2 5.19 | refused by name (ruling 2) | harness only (the referee's input); refused in a pure/v1 program under ruling 2 (a capability loss the first review named) | harness only | built (test tooling) |
| T-4 | legend-engine's feature flags in a program. v2 5.20 | refused by name (ruled; `FeatureRules.requireConsumed`, `L/lowering/FeatureRules.java:110-117`) | the flag's SQL: S-3 (corrected substring), S-12 (null-unsafe `=`) | not applicable | built |
| T-5 | legend-pure-only texts: `format`'s `%0wd` and `%r`, an instance's `@id`, `toRepresentation`. S314; v2 5.6 | refused by name at planning | legend-engine's text where it has one, else refused | legend-pure's (`EdgePureOnlyTexts`); at a last step a presenter could print it (P-1 family) | built at the edge |
| T-6 | `assertSize`/`assertEmpty`/`assertNotEmpty` over a relation. S137 | refused at planning | harness: the relation counts as one value (`L/edge/UpstreamTestBodies.java:53-57`) | the same (legend-pure's own meaning) | built (harness) |
| T-7 | An order-dependent `assertEquals` over rows with no defined order. S131 | refused (`compiler.spec.OrderedAsserts`) | harness: rewritten to `assertSameElements` (`UpstreamTestBodies.rowsOrderFree`) | the same | built (harness) |
| T-8 | Two whole Variants compared. S401 | refused by name | the database's `=` on the JSON: CHEAP (stop refusing) | identity: `false` for two documents built apart is CHEAP; a Variant compared with itself (Pure `true`) needs identity in SQL, EXPENSIVE | not built for either mode |
| T-9 | A date-time literal with more fraction digits than the database keeps. S181 | refused, never cut | legend-engine sends it and the database rounds | not applicable | K (ruled 2026-11-10) |
| T-10 | Decimal arithmetic on SQLite | refused by name (no exact decimal) | REAL arithmetic (`L/edge/PureText.java:425-430`) | the same today (shared `UPSTREAM`) | built at the edge; SQLite only |
| T-11 | An unmapped OPTIONAL property in a query. S393 | lite refuses at planning | legend-engine answers empty (`builder.pure:392`, per S393) | not applicable | not built. MEDIUM (the resolver writes a null column); a follow-up per S393 |
| T-12 | A model legend-engine compiles with a warning (duplicated property or enumeration value, S395); a `date()` Decimal second past an int (S394) | refused | compiled | refused (legend-pure refuses S395) | K (ruled 2026-11-21) |
| T-13 | `pure`'s equality where both sides may be empty | `PureComparisons.refused` (`L/edge/PureComparisons.java:71-74`) | not applicable | Pure's `true` for `[] == []`: needs a run-time emptiness test, MEDIUM | refused today; PCT has 0 crossings (probe-crossings-results) |
| T-14 | Mixed Integer/Float arithmetic's static type (`1 * 1.5`: legend-engine `Number`, lite `Float`). S25 | lite types it Float (it answers; this is the reverse case) | a result column typed `Number` in the envelope: P-11 | not applicable | P only |

Not a difference to act on: lite answers where legend-engine refuses at planning (S385; ruling 8, "lite can answer where
engine refuses - yes"), the 3-argument `indexOf` among them.

### 1.2 (P) Presentation: Java at the edge can write it from the typed value

"Can": the value reaches the edge typed, at the program's last step. For a relation the lift exists
(`L/edge/PrintedRelations.java`). For a scalar's `toString`/`format` at a last step or an assert's side, the lift is
B2b's S6a, **built and held** for the user's ruling (`PrintedRelations.java` header, "a last step's Float print at
legend-pure's door, lifted by B2b S6a's legend-pure door, is held"). Without it, the six PCT text cases move (B2's P-lite
probe: 5 DateTime and 1 Float, per database and channel; `e3-design.md` §4).

| # | Difference | Today (where it is written) | Java at the edge? | Exceptions (where the text is computed with inside SQL) |
|---|---|---|---|---|
| P-1 | A Float's text (`toString`, `format` `%f`/`%s`, Pair and mixed-list elements). S311; v2 5.2 | SQL. lite: six decimals (`L/lowering/Scalars.java:2444`, `:2937`, `:3525`). The edge: the database's cast, a literal printed by the literal processor (`L/edge/PurePrintForms.java:110-120`, every edge dialect's `floatToText`, `L/edge/PureText.java:288-534`) | yes, from the double. `engine`'s text is the database's own (DuckDB `1.3421e-08`, H2 `1.3421E-8`, probed), so Java would have to re-spell each database's double-to-text: MEDIUM for exactness. `pure`'s is legend-pure's `DecimalFormat` print, which Java has | **S-15**: a text used further (`toString()->length()`, a message joined, a harness `makeString`). Also a literal vs a computed operand: legend-engine prints a literal through its literal processor (legend-pure's text) and a computed value through the database. The presenter must know which, and the plan does |
| P-2 | A DateTime's text. S312, S118; v2 5.3 | SQL. lite: ISO nanoseconds with `Z`. The edge: legend-pure's print (`L/edge/EdgeDateTimeTexts.java:24-27`, `PureDateTimePrint`) at **both** edge doors | yes, but the written precision of a COMPUTED value is not in the database (only a literal's is known at planning). legend-engine's own text is the database's cast (`DUCKX:284`, `PGM:404`; probed `2014-01-01 00:00:00`), which differs from what the edge writes today | **S-16** (used further) |
| P-3 | A list's or Map's text (`[a, b]` vs JSON). S316c, S363; v2 5.4 | SQL (`ListTexts` two-form) | yes | used further: lite's text, recorded |
| P-4 | An empty cell's text (`TDSNull`) at a last step. S313, S316a; v2 5.5 | SQL pass (`PurePrintForms.TdsNullCells`, `:66-75`) | yes | **S-14** (joined or used further; 112 DuckDB / 107 H2 corpus tests) |
| P-5 | A relation's `toString`, `toCSV`, a TDS's `.csv` at a last step. S316, S402, S405, S420 | Java: `PrintedRelations` lifts it (built) | yes (built) | used further: lite's text, recorded (S316 as amended) |
| P-6 | `executeValues`' JsonBuilder rule (one object bare). v2 5.14 | SQL (`L/edge/EdgeLoweringForms.java:69-95`) | yes at the last step | a serialized value used inside the program |
| P-7 | Graph fetch key texts (`+0000`, a quoted String). v2 5.16 | SQL (`PlanStyle.graphKeyDate`/`graphKeyString`, `L/edge/PureText.java:146-156`) | yes (the writer re-keys) | none known |
| P-8 | The object reference's connection JSON. S362 | SQL literal (`EdgeLoweringForms.java:52-63`) | yes | none |
| P-9 | The `executionTraceID` comment. S304 | the style's trace cell (`PureText.java:159-161`) | yes (harness) | none |
| P-10 | Envelopes, error wording, assert words. S389, S390, S391, S398 | Java (stage B's B1) | yes | **an operand only the database held** prints `<computed>` (S389): the record lacks it, so legend-pure's exact sentence needs the value in the raise text (SQL). PCT's `assertError` reads words inside the body (census hard case 9) |
| P-11 | Wire texts and types: a Decimal's wire text (S104), a Variant's (S115), pivot names (S315/S107), Arrow types (S28/S125), `Number` columns (T-14) | Java writers | yes | none |
| P-12 | A non-finite Float at pure/v1: DuckDB `x / 0.0` is `Infinity` and H2 `sqrt(-2)` is `NaN` (both probed); legend-engine writes JSON `null` (`PV1/engine_duckdb_answers.tsv:4`; `PV1/engine_h2_answers.tsv:7`) | lite's pure/v1 writes `null` for H2's NaN (`EDGE/pure_v1_answers.tsv:6`); DuckDB not in lite's net | yes: a writer rule | none |
| P-13 | MongoDB's date-time format | the edge dialect (`L/edge/PureText.java:518`) | yes | used further |
| P-14 | A Boolean's text (H2's cast `TRUE`) | not written by lite | yes | appendix only (H2 `testBooleanToString`) |
| P-15 | A number's kind or scale in its print (`2` vs `2.0`, `8D` vs `8.0D`, `0.000D` vs `0.0D`) | the typed value's own | yes for spelling. legend-engine's values here are a different TYPE (its JDBC reader), which is §4's "type" class | appendix only |
| P-16 | A homogeneous list's `makeString` elements: the database's cast on every door, not S311's text (B2's `e3-design.md` §7) | SQL (`PureSql.elementText`) | yes at a last step | a product inconsistency with no register row (B2 noted it) |

### 1.3 (S) Genuine SQL: the mode's answer differs in rows, in whether an error happens, or in a value the query keeps computing with

| # | Construct | Applies to | Kind of legend-engine's answer | Detail |
|---|---|---|---|---|
| S-1 | The ten unchecked raise kinds (divide/rem by zero, `sqrt` of a negative, shifts past 62, `toOne` size, inverted slice, zero range step, impossible cast, three parses). v2 5.8 | `engine` | shortcut (no check; the database decides) | §2, §3 |
| S-2 | The parses' accepted text and their error (F31). v2 5.7 | `engine` (and a leak into `pure` today) | shortcut, plus a database cast that accepts more | §2 |
| S-3 | `substring` (and `substr`, the corrected `substring`): in-range indexing and out of range. S130, S383 | `engine` | deliberate (kept behind a flag for old queries) and listed as a failure in its own manifest | §2 |
| S-4 | String `indexOf`: 1-based, 0 on a miss. S134 | `engine` | deliberate-ish (a TODO in `DUCKX:237`) and listed as a failure | §2 |
| S-5 | `toOne` of an empty value. S384 | `engine` | shortcut (`processNoOp`) | §2, §3 |
| S-6 | `isNumeric` (a `###Relational` dynaFunction; not in legend-pure). S370 | `engine` | deliberate (its dynaFunction) | §2 |
| S-7 | `parseDecimal`'s one-argument precision. S365 | `engine` | deliberate per database | §2 |
| S-8 | `parseJSON`'s leniency. S366 | `engine` | deliberate (its parser) | §2 |
| S-9 | H2's `MODE=LEGACY` and `DEFAULT_NULL_ORDERING=HIGH`. S368; v2 5.13 | `engine` (H2) | deliberate (its connection) | §2 |
| S-10 | DECIMAL arithmetic: the bare Float literal (S20, ruled deleted) and Integer division written `(1.0 * x) / y` (NUMERIC on H2) | `engine` | deliberate | §2 |
| S-11 | Comparisons whose kinds are known only at run time (`Any`, `Number`, a mixed list), the tie order of mixed kinds (Q4), and `LiteralUnroll`'s literal cross-kind folds. Second review M9; ruling 7 | `engine` | delegation to the database | §2 |
| S-12 | `LEGACY_SQL_NULL_UNSAFE_EQUALS` | `engine` | deliberate (a flag) | §2 |
| S-13 | A count over a declared key folded to 1. S147 | `engine` | legend-engine counts the data | §2 |
| S-14 | `TDSNull` inside a text the program computes with | `engine`, the corpus harness | the TDS type's own value (ruling 6) | §2 |
| S-15 | A Float's text used further | `engine`, `pure` | `engine`: the database's cast; `pure`: legend-pure's print | §2 |
| S-16 | A DateTime's text used further | `engine`, `pure` | `engine`: the database's cast; `pure`: legend-pure's print | §2 |
| S-17 | DateTime equality across written precision. S399 | `pure` | Pure's own definition | §2 |
| S-18 | Decimal equality by scale. S139 | `pure` | Pure's own definition | §2 |
| S-19 | `average([])` raises. S19 | `pure` | Pure's own definition | §2 |
| S-20 | legend-engine's answers lite rules defects: `joinStrings`' separator (S386), `firstDayOfWeek` on H2 (S387), a count of an empty association (S22), an unfetched qualified property (S388), and the corpus's accepted `engine-golden-defect:*` classes | `engine` | translation bugs, already ruled | §2 |
| S-21 | Database versions: legend-engine's DuckDB 1.3.0 and patched H2 2.1.214 (S141); lite's DuckDB 1.5.6 and H2 2.5.252 (`tools/deps/jars_table.bzl:27,43`) | `engine` | not a choice | §2 |
| S-22 | Lite checks outside the ten that legend-engine does not make: `substring` out of range (`SUBSTRING_OUT_OF_RANGE`), NaN or infinity refused (`INVERSE_TRIG_NAN`, `NOT_FINITE_NOT_HELD`, `PARSED_NAN_NOT_HELD`) | `engine` | shortcut | §3 |

### 1.4 Not a difference, or not product SQL

- Answer-neutral SQL shape: `existsJoin`, `whereZones` (S367). Per CP-4, SQL text is never promised.
- Identical in both forms (dead per the census): `view`, `wireCast`, `relations`, `keyedCounts` as coded (but see S-13),
  `computedPruning`, `unionFrame` (`L/edge/EdgeLoweringForms.java:42-50`, `:97-113`, `:149-152`).
- Harness only:
  - the corpus's DuckDB scan order (`StableScanOrder`, S14);
  - `raw()`'s translation of H2 text (`PureText.java:271-276`);
  - the referee;
  - `ServiceTestRunner`'s JSON compare (v2 5.18; a judge, never the edition);
  - S144's json-simple grammar, applied at the door (`UpstreamTestBodies.jsonGoldensCleaned`).
- Product gaps that look like differences but are lite's defects in every mode:
  - a connection's `timezone` (S183, a known gap);
  - H2's locale-dependent week (DIFFERENCES "Gaps");
  - the H2 functions lite does not write yet.

---

## 2. The S rows in detail

Cost scale:
- **CHEAP:** the SQL form exists in lite today (product or edge), or it is one dialect arm or one rule, with no placement
  and no new engine answers.
- **MEDIUM:** a new typed form with an arm per dialect, a judge or fold change, or placement that A3's machinery already
  computes; days.
- **EXPENSIVE:** needs information the database does not carry (written precision, scale, identity), a placement pass
  not yet designed, an unexplained engine bug reproduced, or a per-database answer campaign; weeks or open-ended.

### S-1. The ten unchecked raise kinds
See §3: per kind, per database, with K and F. **Recommendation: F (plain) for `engine`'s database part; checks kept in
`lite` and `pure`.**

### S-2. The parses (`parseInteger`, `parseFloat`; `parseDate` is plain already on DuckDB and H2)

- **lite's product:** a grammar guard, else `TEXT_NOT_INTEGER`/`TEXT_NOT_FLOAT` by name:
  - `guardedParseInt`, `L/sql/dialect/AnsiSqlRenderer.java:949-960`, and `guardedParseFloat`, `:902-912`, with grammar `:921-922`;
  - DuckDB `L/sql/dialect/DuckDb.java:582`, `:1355-1364`;
  - `parseDate` is a plain `CAST AS TIMESTAMP` on DuckDB and H2 (`DuckDb.java:615`, `H2.java:697`).
- **Pure:** Java's `Long.parseLong` and `Double.parseDouble`; a bad text raises (`parseInteger.pure:43-50`, `parseFloat.pure:43-57`, per the pure/v1 census).
- **legend-engine:**
  - DuckDB: `cast(x as BIGINT)` and `cast(x as float)` (`DUCKX:257-258`);
  - H2 (via the translation): `cast(x as integer)`, 32-bit (`PGM:402`), and `cast(x as float)` (`H2X:248`).
  - Probed, DuckDB 1.3.0 = 1.5.6 (§6.2):
    - `'abc'` raises "Could not convert string 'abc' to INT64";
    - **`' 1 '` → 1, `'1.5'` → 2, `'+1'` → 1, `'0x10'` → 16, `'1e3'` → 1000**;
    - `cast('1.1' as float)` is a 4-byte REAL, read as a double **1.100000023841858**.
  - Probed, H2 2.1.214 = 2.5.252:
    - `'abc'` and `'1.5'` raise "Data conversion error";
    - `' 1 '` and `'+1'` → 1;
    - **`'9999999999999992'` raises as an INTEGER** (also `MH/EssentialFunctions_manifest.json:598-599`).
  - Golden: `PV1/engine_duckdb_answers.tsv:8`, `PV1/engine_h2_answers.tsv:8`.
- **Kind:** shortcut (no check). The extra accepted texts are the database cast's. H2's 32-bit cast is a translation bug (its manifest lists it).
- **K (lite's guard):** an engine user sees Pure's error where legend-engine raised the database's (the same request fails either way, different words), and **an error where DuckDB answered** for `' 1 '`, `'1.5'`, hex or `'1e3'`.
- **F:** today's F31 pass (`L/edge/PurePrintForms.java:47-64`):
  - the plain cast, to BIGINT and DOUBLE on every database;
  - exact for DuckDB's `parseInteger`;
  - per database for H2's 32-bit INTEGER (reproducing the bug) and for DuckDB's REAL `parseFloat` (reproducing a precision loss, unverified at legend-engine's reader);
  - no placement, no new form.
- **Cost:** CHEAP (exists). Exact per database: CHEAP but reproduces two bugs.
- **Tests:**
  - pure/v1 `raise.parseInteger` on H2 (`EDGE/pure_v1_answers.tsv:7`, plain today; the words differ only by H2's INTEGER vs BIGINT);
  - corpus 0 (M7: no parse in the harness; M3: none planned);
  - PCT engine lane H2 `testParseInteger` (engine DATABASE_ERROR, lite passes with BIGINT: `pct/engine_lane/h2_essential.tsv`).
- **Leak today:** F31 is in `PurePrintForms.passes()`, so PCT's legend-pure route also gets plain casts (census hard case 1). Under `pure` it goes (B2 measured 0 PCT moves).
- **Recommendation:** F with BIGINT and DOUBLE (today's) under `engine`. Do not copy H2's INTEGER or DuckDB's REAL.

### S-3. `substring` (2 and 3 arguments), `substr`, the corrected `substring`

- **lite's product:** Pure's, 0-based and end-exclusive, raising `SUBSTRING_OUT_OF_RANGE` (`L/lowering/Scalars.java:1492-1494`, `pureSubstring` `:3111-3143`, `FeatureRules.PURE_SUBSTRING` `L/lowering/FeatureRules.java:84-95`).
- **Pure:** 0-based; `StringIndexOutOfBoundsException` out of range (S383: compiled `CompiledSupport.java:1546-1556`).
- **legend-engine:**
  - `processSubstring` writes the database's `substring(s, a, b)` over the arguments as given, unless `CORRECT_SQL_SUBSTRING_INDEXING` (`P2S:4660-4666`: "Correcting substring unconditionally would change the results of existing queries, hence the flag"). `substr` is always corrected: `start + 1`, `end - start` (`P2S:4654-4690`).
  - DuckDB `DUCKX:277`; H2 `PGM:418`.
  - Probed, both databases: `substring('abcdef', 0, 3)` → DuckDB `'ab'`, H2 `'abc'`; `substring('ab', 1, 5)` → `'ab'` (clamped); `substring('abcdef', -2, 3)` → `'ef'` (a negative start counts from the end).
  - PCT: `testStart`/`testStartEnd` wrong values (`MD/EssentialFunctions_manifest.json`; M4 bucket 3).
  - A host-side `let` is computed in Java with Pure's meaning (H16).
- **Kind:** deliberate. A known bug kept behind a flag for backward compatibility; its own manifest lists it.
- **K (Pure's everywhere):** an engine user's `$p.name->substring(1, 3)` changes from `'abc'` to `'bc'`, and out of range raises where legend-engine clamped. Silent different rows: the "users freak out" case.
- **F:**
  - **At pure/v1 today it is built:** A3 rewrites each store-bound call to `substringAsGiven`/`substrAsGiven` (`L/edge/EngineSubstitutes.java:52-80`, the lowering `L/lowering/Scalars.java:1508-1524`), exactly by placement (a host-side `let` keeps Pure's) on H2 and DuckDB only (`L/edge/PureText.java:31-43`), refusing mapping code, graph-fetch properties and other databases.
  - **For the edition** (the corpus, PCT's engine lane, function bodies, mapping code):
    - (i) "the database's everywhere in `engine`" (review option d) is CHEAP: a rule substitutes the same natives. It loses only a host-side `let` (rare) and an out-of-range raise legend-engine makes in Java.
    - (ii) Exact by placement is MEDIUM to EXPENSIVE: the post-inlining placement pass the second review's M1 asks for, plus chained M2M per query (`chain.pure:104-116`) and checked constraints (Java upstream).
  - Per database: the answer is the database's own (H2 and DuckDB differ at start 0); no new dialect form.
  - Engine answers needed for other databases.
- **Cost:** CHEAP for (i) and today's pure/v1; MEDIUM to EXPENSIVE for (ii).
- **Tests:**
  - PCT engine lane, DuckDB and H2: `testStart`, `testStartEnd` move from LITE_FIXES to bucket 2 (lite reproduces the wrong value);
  - pure/v1: A3's tests (`PureMeaningEngineTest`);
  - corpus: `testWithFunction.pure:102` inside `execute()` (roster not verified; first review §1);
  - M7: `substring` in 4 harness tests (the harness is Pure, so unaffected).
- **Conflict to rule:** the amendments' proposed reading (3) ("where its translation is wrong ... lite writes the correct translation") says K. The 11-20 ruling ("engine only mode for engine for everything") says F, and it is what pure/v1 does today.
- **Recommendation:** F (i): `substringAsGiven` everywhere in `engine`'s database part. Keep A3 at pure/v1 until (ii) exists, if exactness for host-side `let`s matters.

### S-4. String `indexOf` (2 arguments)

- **lite's product:** `STRPOS - 1`, 0-based, -1 on a miss (`L/lowering/Scalars.java:1595-1600`). The 3-argument form is Pure's (`:1568-1593`).
- **Pure:** 0-based, -1 on a miss (`indexOf.pure:39`).
- **legend-engine:** DuckDB `instr(s, x)` (`DUCKX:237`, whose own TODO reads "pure uses 0-based indexing ... many user tests need to be fixed"); H2 `strpos` → `locate` (`PGM:381`). Probed: `instr('abc','b')` = 2, a miss is 0; the same on H2. PCT wrong values: `indexOf::testSimple`, `indexof::testIndexOfOneElement` (both manifests).
- **Kind:** a translation bug kept knowingly.
- **K:** an engine user's `indexOf` shifts by one, and a miss reads -1 instead of 0.
- **F:** A3's `sqlPosition` substitute (`L/edge/EngineSubstitutes.java:59-62`, `L/lowering/Scalars.java:1502-1504`), built at pure/v1. The same placement story as S-3.
- **Cost:** CHEAP (i) / MEDIUM to EXPENSIVE (ii).
- **Tests:**
  - PCT engine lane: the two tests above per database (from LITE_FIXES to bucket 2);
  - M7: 7 corpus tests use `indexOf` in the harness (Pure, unaffected).
- **Recommendation:** F, with S-3.

### S-5. `toOne` of an empty value

- **lite's product:** raises `SIZE_NOT_MULTIPLICITY` (`L/lowering/Coercions.java:63-67`, `:73-90`, `:101-107`; `L/lowering/ToOneGuard.java:68-72`).
- **Pure:** "Cannot cast a collection of size 0 to multiplicity [1]".
- **legend-engine:** `processNoOp` writes the operand alone (`P2S:3133-3137`, registered `:10793`), so the database answers null. Golden `raise.toOneOfEmpty` 200 null on both databases (`PV1/engine_duckdb_answers.tsv:9`, `PV1/engine_h2_answers.tsv:9`). A literal list of two or more is refused at planning (`raise.toOneOverMany`).
- **Kind:** shortcut.
- **K:** an error where legend-engine answers null.
- **F:**
  - always-empty: A3's `trustOne` at pure/v1 (`L/edge/EngineSubstitutes.java:83-88`; S384);
  - the [0..1] value-lane guard: the plain style (`Coercions.java:101-107`);
  - **not reached by any style:** the static-empty raise (`:63-67`), the two `CheckedOne` arms (`:73-90`) and `ToOneGuard` (`ToOneGuard.java:68-72`), which raise on every door;
  - **a value empty at run time but not always empty:** legend-engine's answer not asked (S384).
- **Cost:** CHEAP for what exists; MEDIUM to route the other three arms through `LoweringForms.raise`.
- **Tests:**
  - pure/v1 `raise.toOneOfEmpty` (H2);
  - M3: 8 DuckDB and 7 H2 corpus tests plan a `toOne` raise, 0 verdicts move;
  - M7: 337 corpus tests reach a verdict through a harness `toOne` (Pure's raise; the harness is Pure, unaffected);
  - PCT `testRemError` on the engine lane fails on lite's unstyled `toOne` after a plain `rem` (M4 bucket 4 #15).
- **Recommendation:** F, and route all four arms (MEDIUM), so `engine` is consistent.

### S-6. `isNumeric` (a `###Relational` dynaFunction; legend-pure has no such function, checked: no `.pure` file in the pinned legend-pure mentions it)

- **lite's product:** a real numeric check, `SqlFn.IS_NUMERIC` (`L/lowering/ProductLoweringForms.java:88-92`; S370).
- **Pure:** none (so `pure` has no row).
- **legend-engine:**
  - DuckDB: `(lower(s) = upper(s))` (`DUCKX:238`). Probed: `' 1 '`, `''`, `'-'` true; `'1e3'` false.
  - **H2: the translation writes `regexp_full_match(s, '[-]?[0-9]+(\.[0-9]+)?')` (`PGM:367`)**, because every H2 SELECT is translated (`relationalMappingExecution.pure:439-442`). Probed: `' 1 '` false, `'1.5'` true. `H2X:232`'s `lower = upper` appears unused for SELECTs. **S370's H2 claim needs the engine runner.**
- **Kind:** deliberate (its dynaFunction).
- **K:** a mapping column `isNumeric(...)` answers differently for text with spaces, an empty text, a lone sign, or an exponent.
- **F:** `EdgeLoweringForms.isNumeric` (`L/edge/EdgeLoweringForms.java:140-145`) writes `lower = upper` on every database. Exact for DuckDB; per database for H2 (a regex form: a new arm).
- **Cost:** CHEAP (DuckDB exists; H2 one arm after the runner answers).
- **Tests:** corpus 0 (DIFFERENCES §6); PCT none.
- **Recommendation:** F per database.

### S-7. `parseDecimal`, one argument

- **lite's product:** an exact decimal at the product's precision (`L/lowering/ProductLoweringForms.java:68-73`).
- **Pure:** `new BigDecimal(s)`, exact (S365).
- **legend-engine:**
  - H2: `cast(x as decimal(5, 2))` (`H2X:474-479`; `PGM:401`). Probed: `'123.450021'` → 123.45; `'1234.5'` raises "Value too long".
  - DuckDB: the default `cast(x as decimal)` (`DEF:721-727`, via `DUCKX:256`). Probed: **`DECIMAL(18,3)`, `'123.450021'` → 123.450**.
- **Kind:** deliberate per database.
- **K:** an engine user's value keeps every digit where legend-engine cut it (H2 2 places, DuckDB 3); a value over 999.99 answers on lite where H2 raised.
- **F:**
  - today's edge writes `DECIMAL(5,2)` on **every** database (`L/edge/EdgeLoweringForms.java:116-120`), which is wrong for DuckDB;
  - exact F is a per-database form: H2 (5,2), DuckDB the bare `DECIMAL`, other databases the default `decimal`;
  - no placement.
- **Cost:** CHEAP (a per-dialect arm).
- **Tests:**
  - corpus: 2 per database (DIFFERENCES §6, H5 task 0);
  - PCT: legend-engine's `testParseDecimalWithPrecisionScale` (H2 a genuine wrong value 123.12D) and `testParseZero` use the 3-argument or literal forms (M4 bucket 3).
- **Recommendation:** F per database. Fix the edge's DuckDB arm in any case.

### S-8. `parseJSON` leniency

- **lite's product:** strict JSON (`L/lowering/ProductLoweringForms.java:75-79`).
- **Pure:** not legend-pure's function (a legend-engine one).
- **legend-engine:** `Json.parseOne` lenient: `'}{'` reads as `'},{'` (S366). In SQL: DuckDB `cast(x as JSON)` (`DUCKX:259`).
- **Kind:** deliberate.
- **K:** a malformed array of objects is refused where legend-engine read it.
- **F:** exists (`L/edge/EdgeLoweringForms.java:124-130`). Per database: the regex form is portable; no placement.
- **Cost:** CHEAP (exists).
- **Tests:** corpus 1 lost on DuckDB, 1 gained on H2 (DIFFERENCES §6).
- **Recommendation:** F.

### S-9. H2's `MODE=LEGACY` and `DEFAULT_NULL_ORDERING=HIGH`

- **lite's product:** H2's defaults, `MODE=REGULAR` and `DEFAULT_NULL_ORDERING=LOW` (`L/sql/dialect/ProductDatabaseSettings.java:10-18`). Every relation sort's null placement is written explicitly (`L/lowering/Fold.java:446-458`), except an aggregate's own ordering (`AnsiSqlRenderer.aggOrderNullPlacement`, per `Fold.java:443-444`).
- **Pure:** not applicable (no H2).
- **legend-engine:** its H2 runs `MODE=LEGACY;DEFAULT_NULL_ORDERING=HIGH` (S368). Probed (H2 2.1.214 unpatched vs 2.5.252):
  - a BOOLEAN column `= 1` answers under LEGACY and **raises "not comparable"** under REGULAR;
  - a bare `ORDER BY v`: nulls last under HIGH, first under LOW;
  - `array_agg(v ORDER BY v)`: `[1, 2, NULL]` vs `[NULL, 1, 2]`.
- **Kind:** deliberate (its connection).
- **K:** a mapping filter `active = 1` on a BIT column fails; an aggregate's bare ordering puts nulls first.
- **F:** exists (`L/edge/EngineDatabaseSettings.java:16-29`, the H2 edge dialect `L/edge/PureText.java:300-304`). H2 only; no placement; a session setting, not an expression (census hard case 4).
- **Cost:** CHEAP (exists). M2 (B2) is still measuring.
- **Tests:** 1 corpus test (DIFFERENCES §7, H8 (h)); PCT H2 relation lane: aggregates' orders (unmeasured).
- **Recommendation:** F under `engine` on H2, pending M2.

### S-10. DECIMAL arithmetic: the bare Float literal, and Integer division

- **lite's product:** a Float is a double. A literal is a DOUBLE (S20, `L/edge/PureText.java:262,309,332,369,480` override it at the edge). Integer / Integer is a DOUBLE division (`L/lowering/DecimalKindRules.java:40-66`; `EDGE/pure_v1_answers.tsv:3`, `CAST(x AS DOUBLE) / CAST(0 AS DOUBLE)`).
- **Pure:** IEEE.
- **legend-engine:**
  - a Float literal bare: DuckDB `0.1 + 0.2` is `DECIMAL(3,1)` 0.3 (probed; M1);
  - `divide` is `((1.0 * x) / y)` (`DEF:218`): on DuckDB a DOUBLE (probed, `1/3` = 0.3333333333333333), **on H2 a NUMERIC with 21 places** (probed `0.333333333333333333333`).
- **Kind:** deliberate (it lets the database type a literal).
- **K:** Float literal arithmetic and H2 integer division differ in the last digits.
- **F:** keep the edge's DECIMAL literal on 5 databases (exists) and write `1.0 *` on H2.
- **Cost:** CHEAP. But the user ruled the literal deleted ("yes delete the float decimal thing"), and division is the same family.
- **Tests:**
  - M1: corpus H2 +2 PASS on deletion, PCT 2 answers toward Pure, `EqualityCompatEdgeTest`'s `0.1 + 0.2`;
  - the accepted rosters' `engine-store-arithmetic:h2-decfloat-*` (DuckDB 9, H2 8; `spec/src/test/resources/rcorpus/*-accepted-roster.txt`).
- **Recommendation:** K (ruled).

### S-11. Comparisons known only at run time; Q4's ties; `LiteralUnroll`'s literal folds

- **lite's product:**
  - a mixed (tagged) value compares by kind, Pure's meaning (`L/lowering/Scalars.java:3821-3870`, `TaggedLists.sameElement` `L/lowering/TaggedLists.java:381-470`);
  - equal magnitudes order Integer < Float < Decimal and StrictDate < DateTime (Q4, ruled 11-24; the implementing code was not located, unverified);
  - `LiteralUnroll` folds a literal-only `in`/`contains`/`==` across kinds to `false`, and an enumeration's value against another kind's (`L/compiler/spec/LiteralUnroll.java:389-438`), on every door, after the door.
- **Pure:** the same as lite.
- **legend-engine:** the database promotes. Probed DuckDB: `1 = 1.0` true, `1 in (1.0, 2)` true, `list_contains([1, 2.0], 1.0)` true. **But legend-engine refuses `Any` lists at planning** ("Any is not managed yet!", `MD/EssentialFunctions_manifest.json`, `testConcatenateMixedType`), so most mixed-list cases are refusals, where ruling 8 lets lite answer.
- **Kind:** delegation to the database (ruling 7: "comparison in engine database yes good to match", including Any and Number).
- **K:** the remaining cases (a `Number`-typed value; a literal-only `1->in([1.0])`) answer Pure's `false` where the database would answer `true`.
- **F:**
  - a promotion form for every `Any`/`Number` comparison (new typed forms: MEDIUM);
  - the folds told not to answer under `engine` (needs the edition at G½: MEDIUM, and the second review's M4 warns it is not byte-identical, and S169 regresses if done before E2 marks bodies);
  - no placement; no per-database variation.
- **Cost:** MEDIUM.
- **Tests:** unmeasured; PCT engine `testEqEnum`/`testEqualEnum` would move to bucket 2 (reproducing legend-engine's manifest failure).
- **Ask first:** which of these legend-engine refuses.
- **Recommendation:** F for the folds of numbers (ruled). Ask the runner before building the `Any` forms. Leave enumeration-vs-enumeration as Pure's (legend-engine's own manifest calls it a failure).

### S-12. `LEGACY_SQL_NULL_UNSAFE_EQUALS`

- **lite:** null-safe equality, as legend-engine's default (`P2S:8415-8416`, `:8465`); the flag selects plain `=` (`L/lowering/Lowerer.java:215-235`, sites `:2277`, `:2301`, `:3521`, `:4345`, `:4352`, `:4500`).
- **Pure:** not applicable.
- **legend-engine:** with the flag, plain `=` (nulls never equal).
- **Kind:** deliberate (a flag).
- **K:** refusing the flag (DIFFERENCES §8 plans that for stage B).
- **F:** exists in the product's lowering today. In the edition it moves to an `engine` row (E6).
- **Cost:** CHEAP.
- **Tests:** 7 corpus tests set a flag, 1 needs this one and checks SQL text only (DIFFERENCES §8, H8 (a.3)).
- **Recommendation:** F under `engine` (it is legend-engine's documented switch).

### S-13. A count over a declared key folded to 1

- **lite:** `KeyedCounts` folds `COUNT(*) OVER` to 1 where the partition covers a declared key (S147). The edge's form folds too (`L/edge/EdgeLoweringForms.java:103-106`, despite its comment "counts a keyed relation's rows as written").
- **Pure:** not applicable.
- **legend-engine:** counts the rows the data holds.
- **Kind:** deliberate (it trusts no key).
- **K:** on data that breaks a declared key (keys the database does not enforce), lite answers 1 where legend-engine counts the duplicates.
- **F:** `keyedCounts` returns the query unfolded under `engine` (the form exists). Lean-SQL growth at legend-engine's door.
- **Cost:** CHEAP.
- **Tests:** none known (the corpus's data keeps its keys); unmeasured.
- **Recommendation:** F (cheap; it is the "users don't freak out" case on dirty data).

### S-14. `TDSNull` inside a computed text

- **lite:** a join skips an empty element (`SqlFn.CELL_TEXT`, S313, `L/lowering/Scalars.java:1020-1025`).
- **Pure / legend-engine:** a legacy TDS's empty cell is `TDSNull`, the TDS type's own value (ruling 6), so `$r.values->makeString(',')` over `2, null` is `'2,TDSNull'`.
- **Today:** the edge's SQL pass (`L/edge/PurePrintForms.java:66-75`, `:125-162`).
- **Kind:** legend-engine's type, not a choice.
- **K:** rule zero strictly applied (Java at the edge only) fails these: the corpus harness joins the rows in Pure code that lite plans into SQL (second review M7).
- **F:** keep the SQL form (exists), later as an explicit typed call (v2's R4 precondition). Per database: the same.
- **Cost:** CHEAP (exists).
- **Tests:** 112 DuckDB / 107 H2 corpus tests (B2's S6: 76 a row's cells joined, 10 a cell's `toString`, 8 both, 16 others on DuckDB).
- **Recommendation:** F, for both `engine` and the corpus harness (`pure` under the part model).

### S-15. A Float's text used further

- **lite:** six decimals (S311).
- **Pure:** legend-pure's print (`0.000000013421`).
- **legend-engine:**
  - the database's cast (`DUCKX:284`, `PGM:404`): DuckDB `1.3421e-08`, H2 `1.3421E-8`, `1000000.0`;
  - a literal operand is printed by its literal processor (`L/edge/PurePrintForms.java:79-108` documents it; H2 `CAST(%s AS FLOAT)`, `H2X:152`).
- **Kind:** `engine` deliberate (the database's text).
- **K:** lite's six decimals inside the query; the difference recorded (rule zero's corner).
- **F:**
  - `engine`: the database's cast, which exists (`PurePrintForms.floatOf`, the dialect overrides `L/edge/PureText.java:288-534`): CHEAP, and not Java's algorithm in SQL;
  - `pure`: legend-pure's print in SQL is `FLOAT_SHORTEST_TEXT`, more than 490 characters per value (B2's `e3-design.md` §6): EXPENSIVE in size, and exactly what rule zero forbids.
- **Tests:**
  - corpus: 2 per lane (the validation showcase, `1000000` vs `1000000.0`);
  - H2 engine lane: the two exponent cases (legend-engine's own H2 manifest failures `testFloatToStringWith{Negative,Positive}Exponent`);
  - PCT `pure`: 1 (`testFloatToStringWithNegativeExponent`) if no scalar lift (P-1).
- **Recommendation:** F for `engine` (exists), K for `pure` (recorded).

### S-16. A DateTime's text used further

- **lite:** ISO nanoseconds with `Z` (S312).
- **Pure:** legend-pure's print at the written precision with `+0000`.
- **legend-engine:** the database's cast: `2014-01-01 00:00:00` and `2014-01-02 01:54:27.352` on both databases (probed; its PCT wrong values `testDateTimeToString`, `testSimpleDateToString`). **The edge writes legend-pure's print at legend-engine's door today** (`L/edge/EdgeDateTimeTexts.java:24-27`, `PurePrintForms.java:168-171`): not legend-engine's.
- **Kind:** deliberate (the database's text).
- **K:** lite's text.
- **F:**
  - `engine`: a plain `CAST(x AS VARCHAR)` (an existing node), CHEAP;
  - `pure`: legend-pure's print composed in SQL, which exists (`PureDateTimePrint`), MEDIUM in size;
  - a computed value's written precision is not in the database either way.
- **Tests:**
  - PCT engine lane: 3 DuckDB cases move from bucket 3 to bucket 2 (lite would reproduce legend-engine's "wrong" text);
  - PCT `pure`: 5 move under P-lite unless the scalar lift lands (B2's `e3-design.md` §4);
  - corpus: in M7's 77 text tests (DateTime share not split out).
- **Recommendation:** F for `engine` (CHEAP), K for `pure` with the scalar lift for last steps.

### S-17. DateTime equality across written precision (`pure` only)

- **lite:** two date-times at one instant are equal (S399, the one rulebook since 2026-11-24).
- **Pure:** not equal: the written precision is part of the value (`AbstractPureDate.equals`, legend-pure-m4 `.../AbstractPureDate.java:30-59`, per S399).
- **legend-engine:** reads every timestamp at nine digits, so a literal to the second fails against its column (73 expected failures across its manifests, DIFFERENCES §15).
- **Kind:** Pure's definition.
- **K:** `pure` says equal where legend-pure says not.
- **F:** carry a precision tag beside every DateTime in SQL. A computed value has none.
- **Cost:** EXPENSIVE.
- **Tests:** "no compatibility test's verdict moved" (DIFFERENCES §15).
- **Recommendation:** K.

### S-18. Decimal equality by scale (`pure` only)

- **lite:** by value (S139).
- **Pure:** by value and scale (`0.3D != 0.30D`).
- **F:** SQL's DECIMAL scale is static per type, so a literal against a column folds, but Pure's BigDecimal arithmetic scales differ from SQL's (S400).
- **Cost:** EXPENSIVE.
- **Tests:** S139 measured 0 verdicts on PCT channel B and the corpus.
- **Recommendation:** K.

### S-19. `average([])` (`pure` only)

- **lite / legend-engine:** NULL.
- **Pure:** raises (S19).
- **F:** a guard on an empty aggregate.
- **Cost:** CHEAP to MEDIUM.
- **Tests:** unmeasured (M6 shows no failure in Standard on DuckDB).
- **Recommendation:** K (lite = legend-engine already; `pure` would need its own SQL for one rare case).

### S-20. legend-engine's ruled defects lite does not copy

- **What:**
  - `joinStrings`' separator concatenated as an element (S386);
  - `firstDayOfWeek` by the H2 JVM's locale (S387);
  - a count over an empty association is 1 (S22);
  - an unfetched qualified property is `null` or fails mid-stream (S388);
  - the accepted rosters' `engine-golden-defect:*` classes: DuckDB 12 rows (`joinStrings-literal-collection` 2, `empty-string-as-null` 2, `digest-joinStrings` 2, `unfiltered-root-projection`, `isDistinct-in-checked-constraint`, `h2-literal-coercion`, `firstDayOfWeek-h2-week-start`, `filter-lambda-binds-outer-row`, `adjust-strictdate-render`); H2 8 rows (`spec/src/test/resources/rcorpus/duckdb-accepted-roster.txt`, `h2-accepted-roster.txt`).
- **Kind:** translation bugs, ruled 2026-09-05/08 and 2026-11-19.
- **F:** each its own form; S387 also needs a locale.
- **Cost:** MEDIUM each.
- **Recommendation:** K (ruled).

### S-21. Database versions

- **What:** legend-engine's goldens come from DuckDB 1.3.0 (`PV1/engine_duckdb_answers.tsv:1`) and its patched H2 2.1.214 (S141). lite runs DuckDB 1.5.6 and H2 2.5.252 (`tools/deps/jars_table.bzl:27,43`; the referee's H2 is 2.1.214, `:52`).
- **The probe:** 60 DuckDB statements gave identical answers on 1.3.0 and 1.5.6, and the 45 H2 statements identical on 2.1.214 and 2.5.252 (except the two settings, S-9). So no answer difference was found among the constructs above.
- **Recommendation:** K (note it in the differences page).

### S-22. Lite's other checks that legend-engine does not make
See §3.4.

---

## 3. The raises question

### 3.1 What lite does today

| Door | Raises |
|---|---|
| lite's product | every check written (`ProductLoweringForms.raise`, `L/lowering/ProductLoweringForms.java:24-28`) |
| pure/v1, PCT's engine lane | the ten `ENGINE_UNCHECKED` kinds plain (`L/edge/PureText.java:169-175`, `EdgeLoweringForms.PURE_V1` `L/edge/EdgeLoweringForms.java:30`) |
| PCT's legend-pure route, the corpus | checked (`EdgeLoweringForms.PCT`, `:27`) |

**But the hook covers less than the set says:**
- the parse kinds are made plain by F31 on **every** edge door, PCT's legend-pure route included (`L/edge/PurePrintForms.java:36-64`). `TEXT_NOT_DATE` is plain in the product already on DuckDB and H2 (`DuckDb.java:615`, `H2.java:697`);
- three of the four `toOne` arms raise on every door (S-5).

M3: the corpus with plain raises moves **0** verdicts on either lane. 12 DuckDB and 11 H2 tests plan an unchecked kind (SIZE_NOT_MULTIPLICITY, DIVIDE_BY_ZERO, SQRT_OF_NEGATIVE only), and no input reaches the raising case.

### 3.2 What legend-engine returns without the check

All database answers below were probed with legend-engine's SQL text on DuckDB 1.3.0 (its version) and 1.5.6 (lite's),
and on H2 2.1.214 under `MODE=LEGACY;DEFAULT_NULL_ORDERING=HIGH` (unpatched; legend-engine's is patched, S141) and on
H2 2.5.252. Both versions of each database agreed on every row.

| Kind | lite's check (file:line) | legend-engine's SQL | DuckDB, no check | H2, no check | Pure | What legend-engine's user sees |
|---|---|---|---|---|---|---|
| DIVIDE_BY_ZERO, Integer `/` | `L/lowering/DecimalKindRules.java:51-62` (Integer over Integer only; a Float or Decimal divisor is never guarded, even in the product) | `((1.0 * x) / y)` (`DEF:218`; H2 `PGM:434`) | **`Infinity`** (a DOUBLE; probed) | error "Division by zero" (probed; `MH/GrammarFunctions_manifest.json` line 6 per the pure/v1 census) | "Cannot divide 0 by zero" (compiled `CompiledSupport.java:1483-1485`) | DuckDB: `null` in pure/v1's JSON (`PV1/engine_duckdb_answers.tsv:4`), so its writer turns Infinity into null (P-12, inferred). H2: a 500 with H2's text (`PV1/engine_h2_answers.tsv:4`). lite plain on H2: the same error (`EDGE/pure_v1_answers.tsv:3`) |
| DIVIDE_BY_ZERO, `rem` | `L/lowering/Scalars.java:466-480` | `mod(x, y)` (`DEF:267`; H2 `PGM:426`) | **NULL** (probed; `MD/EssentialFunctions_manifest.json:304-305` "No error was thrown") | error "Division by zero" (probed; `MH/EssentialFunctions_manifest.json:490-491` wrapper) | "Cannot divide 5 by zero" (`rem.pure:94`) | DuckDB a null cell; H2 the query fails |
| DIVIDE_BY_ZERO, lite's quotient | `L/lowering/Scalars.java:2597-2602` | n/a (lite-internal) | DuckDB `//` NULL (comment there) | — | — | — |
| SQRT_OF_NEGATIVE | `L/lowering/Scalars.java:1974-1982` | `sqrt(x)` (`DEF:287`; `PGM:328`) | error "Out of Range Error: cannot take square root of a negative number" (probed; `PV1/engine_duckdb_answers.tsv:7`) | **NaN** (probed), written `null` (`PV1/engine_h2_answers.tsv:7`; `MH/EssentialFunctions_manifest.json:505-506`) | "Unable to compute sqrt of -1.0" | DuckDB the query fails; H2 a null cell. lite plain: the same on both (`EDGE/pure_v1_answers.tsv:6`; M4 bucket 4 #16) |
| SHIFT_OUT_OF_RANGE | `L/lowering/Scalars.java:426-451` | DuckDB `(cast(a as BIGINT) << cast(b as BIGINT))`, `>>` (`DUCKX:199-200`) | left past 62: **error** "Overflow in left shift (1 << 63)"; right: **0** (`-1` for a negative; probed; `MD/StandardFunctions_manifest.json:97-102`) | "No error was thrown" for both (`MH/StandardFunctions_manifest.json:157-166`), and wrong within range (`:161-162`, `70368744177664`) | "Unsupported number of bits to shift - max bits allowed is 62" | DuckDB left fails, right 0. H2: lite writes no shifts at all (a gap) |
| SIZE_NOT_MULTIPLICITY (`toOne`) | S-5's four arms | `processNoOp` (`P2S:3133-3137`, `:10793`) | NULL (`PV1/engine_duckdb_answers.tsv:9`) | NULL (`PV1/engine_h2_answers.tsv:9`) | "Cannot cast a collection of size 0 to multiplicity [1]" | a null cell. A literal list of 2+: refused at planning in Pure's words (`PV1/*:6`) |
| SLICE_BOUNDS_INVERTED | `L/lowering/ListEncodings.java:258-268` | `list_slice` | **`[]`** (probed; `MD/EssentialFunctions_manifest.json:112-113`) | refused at planning ("Couldn't find DynaFunction to Postgres model translation for toVariantList()", `MH/EssentialFunctions_manifest.json:220-221`) | "The low bound (3) can't be higher than the high bound (2) in a slice operation" | DuckDB an empty list |
| RANGE_STEP_ZERO | `L/lowering/ListRules.java:23-29` | `range(a, b, 0)` | **`[]`** (probed; `MD/GrammarFunctions_manifest.json:77-78`) | refused at planning (`MH/GrammarFunctions_manifest.json:109-110`) | "range step must not be 0" | DuckDB an empty list |
| CAST_IMPOSSIBLE | `L/lowering/CastPolicy.java:183` | `processCast` writes nothing for a scalar cast (`P2S:3160-3183`) | the value flows unconverted (unverified what the user then sees; no PCT test) | the same | "Cast exception: ..." | unverified; ask the runner |
| TEXT_NOT_INTEGER | S-2 | `cast(x as BIGINT)` / H2 `cast(x as integer)` | error for `'abc'`; **answers for `' 1 '`, `'1.5'`→2, `'+1'`, `'0x10'`, `'1e3'`** | error for `'abc'`, `'1.5'`, any value over 2^31; answers `' 1 '`, `'+1'` | Java's `parseLong` | DuckDB answers texts Pure refuses |
| TEXT_NOT_FLOAT | S-2 | `cast(x as float)` | error for `'abc'`; `' 2.5 '` → 2.5; **a 4-byte REAL** (`1.1` reads back `1.100000023841858`) | error for `'abc'`; a DOUBLE | Java's `parseDouble` | DuckDB a precision loss (unverified at legend-engine's reader) |
| TEXT_NOT_DATE | product's plain `CAST AS TIMESTAMP` on DuckDB and H2 already | the database's parse (census class A) | error (probed: "invalid timestamp field format") | error (per the pure/v1 census) | — | **no difference on DuckDB and H2 today** |

The brief's three extra constructs:
- **`substring` out of range** (`SUBSTRING_OUT_OF_RANGE`, `L/lowering/Scalars.java:3111-3143`; S383): legend-engine passes the indexes unchecked. Probed on both databases: a start of 0 is DuckDB `'ab'` and H2 `'abc'` for `substring('abcdef',0,3)`; past the end it clamps; a negative start reads from the end. Pure raises. A3 writes it plain at pure/v1 (S-3).
- **`indexOf` "out of range":** the 2-argument form has no range. The 3-argument form is refused by legend-engine (S385), so lite answers Pure's. A positional `at` (`INDEX_OUT_OF_BOUNDS`) is refused at planning by legend-engine ("->at(...) function is supported only after direct access", `PV1/*:5`), so lite answering is ruling 8.
- **`toOne`:** above.

### 3.3 The two choices, costed

**(a) Keep lite's checks everywhere (`engine` too).**
- Code: delete `ENGINE_UNCHECKED`, the plain set in `EdgeLoweringForms.PURE_V1` and F31's `UnguardedParses` (about 50 lines). Decide whether A3's `trustOne` (ruled 11-20, "toOne yes needs to be fixed for engine") also goes.
- Moves:
  - pure/v1 (H2 net, 7 `raise.*` cases): `raise.divideByZero` (words: legend-pure's sentence instead of H2's), `raise.sqrtOfNegative` (200 null to 500, so `PV1/engine_h2_differences.tsv:6` "same" becomes "differs"), `raise.parseInteger` (words), `raise.toOneOfEmpty` (200 null to 500, if `trustOne` goes). The other 3 (`atOutOfBounds`, `toOneOverMany`, `datePartAbsent`) are planning refusals and do not move.
  - PCT engine lane, DuckDB: 6 tests from BOTH_FAIL to LITE_FIXES (`testSliceError`, `testRangeStepError`, `testRemError`, `testSquareRootError`, `testBitShiftLeft_MoreThan62Bits`, `testBitShiftRight_MoreThan62Bits`; they pass on the checked legend-pure route, M6). H2: a subset, unmeasured (lite writes no shifts on H2).
  - The engine lane's pins regenerate (`pct/engine_lane/*.tsv`). The corpus moves 0 (M3).
  - pure/v1's SQL grows by the guards (`EDGE/pure_v1_sql_text.tsv`).
- What an engine user sees: Pure's error where legend-engine gave a null cell (DuckDB `rem`, `toOne`; H2 `sqrt`), an empty list (DuckDB `slice`, `range`), `0` (DuckDB right shift) or `Infinity`→`null` (DuckDB divide). **One bad row fails the whole query** where legend-engine returned a partial answer. Where legend-engine already fails (H2 divide and `rem`, DuckDB `sqrt` and left shift), only the words change.
- Rule fit: contradicts the user's earlier "we also should NOT try to correct in java things that engine does NOT do" (these are SQL checks, not Java, but the spirit applies); matches tonight's "keep lite's checks maybe even in engine".
- Cost: CHEAP. One register row ("`engine` raises Pure's error where legend-engine answers null, [] or 0").

**(b) Full compatibility: plain everywhere under `engine`.**
- Code:
  - corpus: `UPSTREAM`'s `LoweringForms` arm answers the plain set (M3's probe, a few lines, once the edition separates PCT's legend-pure route from the corpus);
  - pure/v1 and PCT's engine lane: already plain;
  - to be consistent: route `toOne`'s three other arms through `LoweringForms.raise` (MEDIUM; S-5), and write a non-finite Float as JSON `null` at pure/v1 (P-12, CHEAP);
  - not needed: matching DuckDB divide's null in SQL (it is the writer's).
- Moves: corpus 0 (M3); PCT engine lane 0 (already plain); pure/v1 0.
- Lean SQL: the corpus's SQL shrinks by the guards it plans (12 DuckDB / 11 H2 tests).
- What an engine user sees: legend-engine's answers, cell for cell, on DuckDB and H2. **Not matched without more work:** H2's 32-bit `parseInteger` (lite casts BIGINT; reproducing it is a bug copy) and DuckDB's REAL `parseFloat` (the same).
- Cost: CHEAP (plus MEDIUM for the `toOne` arms).

**Recommendation:** (b) under `engine`'s database part. Keep the checks in `lite` and in `pure` (Pure raises: M6 needs them). Route the `toOne` arms. Write non-finite Floats as null at pure/v1. Keep BIGINT and DOUBLE for the parses. The user's own test for this ("so users dont freak out") favours (b): a legend-engine query that returned rows keeps returning them. If the user prefers (a), it is equally cheap; it is the more opinionated choice, and it needs a register row and the DIFFERENCES page's §3 extended to legend-engine's door.

### 3.4 Lite checks outside the ten that legend-engine does not make (S-22)

| Lite's check | Where | legend-engine | Note |
|---|---|---|---|
| `SUBSTRING_OUT_OF_RANGE` | `L/lowering/Scalars.java:3111-3143` | unchecked (database clamps) | handled with S-3 (plain at pure/v1 by A3) |
| `INVERSE_TRIG_NAN` (acos/asin of \|x\| > 1), `NOT_FINITE_NOT_HELD`, `PARSED_NAN_NOT_HELD` | dialect guards on SQLite and MariaDB (`L/sql/dialect/SQLite.java:1901`, `MariaDb.java:1728`, `:2117`); on DuckDB and H2 the run refuses a NaN Float ("the database answered NaN for a Float, which has no NaN in Pure", M4 bucket 4 #17-18; the source line of that message was not located) | DuckDB raises for acos(2) (its manifest's wrapper, `MD/EssentialFunctions_manifest.json` `testArcCosineError`); H2 answers NaN, written null | under (b), the NaN refusal is lite's reader's, not a SQL check; matching legend-engine is P-12 (write null). CHEAP |
| `INDEX_OUT_OF_BOUNDS`, `DATE_PART_ABSENT`, `DATE_COMPONENT_INVALID` | lite raises at run | refused at planning (census class B) | ruling 8: lite answers; no change |

---

## 4. Appendix: legend-engine's known wrong answers

### 4.1 DuckDB: 47 wrong-value rows in lite's five suites

M4's count: Essential 22, Grammar 5, Relation 1, Standard 14, Unclassified 5 (`pct/engine_lane/summary_duckdb.tsv`,
`engine.DEFECT.WRONG_ANSWER`). The brief's "42" is these without Unclassified. Variant adds 2 more out of scope.
46 are M4's bucket 3 (lite passes) and 1 bucket 2 (lite already the same).

Grouped by the mechanism a reproduction would need. "P/type" = presentation or the value's type; "G" = a genuine
translation bug; "T" = typing.

| Family | Tests | Class | What legend-engine does (evidence) | To reproduce | Cost |
|---|---|---|---|---|---|
| a. DateTime precision in an assert | 11: `testAdjustByMicrosecondsBigNumber`, `testDateFromSecond`, `testDateFromSubSecond`; Standard `greatest`/`least`/`max`/`min` `_DateTime`, `_Single`, `_DateTimeArray` (8) | P/type | reads timestamps back at nine digits (or at milliseconds), so legend-pure's precision-sensitive equality fails (DIFFERENCES §15) | make the compatibility judge compare precision with the database's reading; reverses S399's one-rulebook ruling | MEDIUM |
| b. A number's type | 10: `testRemWithDecimals`; Grammar `testDecimalMinus`/`Plus`/`Times`; Standard `greatest`/`least`/`max`/`min` `_Number(s)`/`_NumbersArray` (6) | P/type | its JDBC reader types a DOUBLE result Float where Pure says Decimal or Integer (`0.14D` vs `0.14`, `2` vs `2.0`) | compute those in DOUBLE, or re-type at the edge (Java changing a value's type: against tenet 3) | MEDIUM |
| c. A Decimal's scale | 3: `testIntToDecimal`, `testParseDecimalWithPrecisionScale`, `testParseZero` | P/type | its reader loses the scale (`8.0D` for `8D`; DuckDB's `DECIMAL(18,3)`) | per-database casts plus scale-sensitive equality (S139 reversed) | MEDIUM |
| d. A DateTime's `toString` | 3: `testDateTimeToString`, `testDateTimeWithTimezoneToString`, `testSimpleDateToString` | P | the database's cast `2014-01-01 00:00:00` (probed) | S-16's F (CHEAP); a presenter at a last step | CHEAP |
| e. An instance's `toString` | 1: `testPersonToString` | P | unknown text (not `@_` or `Anonymous_`) | unknown; ask the runner | EXPENSIVE (unknown) |
| f. 1-based `indexOf` | 2: `indexof::testIndexOfOneElement` (`'c'->indexOf('c')`), `indexOf::testSimple` | G | `instr` (`DUCKX:237`) | S-4's F (`sqlPosition`, exists) | CHEAP |
| g. 1-based `substring` | 2: `testStart`, `testStartEnd` | G | `substring` uncorrected (`P2S:4660-4666`) | S-3's F (`substringAsGiven`, exists) | CHEAP |
| h. `round` half away from zero | 2: `testNegativeFloatRoundHalfEvenUp`, `testPositiveFloatRoundHalfEvenDown` | G | `cast(round(x, 0) as BIGINT)` (`DUCKX:865-870`); probed 16.5 → 17, -16.5 → -17 | one dialect arm (lite has `ROUND_HALF_UP`) | CHEAP |
| i. `dateDiff` in weeks | 1: `testDateDiffWeeks` | G | `datediff('week', …)` (`DUCKX:213`); probed Sat→Sun = 0 (Pure 1) | one arm per database (H2 answers differently: -1 for Sun→Sat) | CHEAP |
| j. `hour`/`minute` of an hour-precision literal | 2: `testHour`, `testMinute` | G | answers 0 for `%2015-04-15T17->hour()`; mechanism not read (likely its literal print at hour precision) | reproduce its literal processing for partial date-times (unverified mechanism) | MEDIUM |
| k. `pow` through a lambda | 1: `testNumberPow` (`{a, b | pow($a,$b)}->eval(3.0, 2)` → 27) | G | binds the lambda's parameters wrongly (inferred: 3^3) | reproduce a lambda-application bug | EXPENSIVE |
| l. `adjust` by many minutes | 1: `testAdjustByMinutesBigNumber` (year -21457 vs 21459) | G | an interval overflow in its SQL form (not read) | its exact interval form | MEDIUM |
| m. `regexpLike` multiline flags | 4: `testRegexpLike_Multiline`, `_NonNewlineSensitive`, `_CaseInsensitive_Multiline`, `_CaseInsensitive_Multiline_NonNewlineSensitive` | G | `transformRegexpLike`'s flag mapping (`DUCKX:267`) | copy its flag mapping as a form | MEDIUM |
| n. `splitPart` with an empty token | 1: `testSplitPartEmptyToken` | G | plain `split_part` (`DUCKX:276`); probed `split_part('Hello World','',1)` = `'H'` | drop lite's guard under `engine` | CHEAP |
| o. a Variant array's null joined | 1: `testVariantArrayColumn_joinStrings` | G | `'null'` joined as `null` | **lite already the same** (bucket 2) | 0 |
| p. an enumeration against another enumeration's value | 2: `testEqEnum`, `testEqualEnum` | T | compares the names (true) | S-11's fold change | MEDIUM |

**What "reproduce the bugs" costs on DuckDB:**

| | Families | Tests |
|---|---:|---:|
| CHEAP | 6 (d, f, g, h, i, n) | 11 |
| MEDIUM | 7 (a, b, c, j, l, m, p) | 33 |
| EXPENSIVE | 2 (e, k) | 2 |
| already | 1 (o) | 1 |
| **total** | **16** | **47** |

Of the 33 MEDIUM tests, 24 (families a, b, c) would reverse a standing ruling: S399's instant equality, lite's
Decimal typing, S139's value equality. Every family adds an `engine`-only SQL form or judge rule whose only purpose is a
wrong answer, and each needs per-database variants: H2 differs (i, below). The 4 in f and g come free if S-3 and S-4 are
F; d comes free with S-16's F.

### 4.2 H2: 54 wrong-value rows in scope (+1 Variant)

Classified the same way from `MH/*_manifest.json` (script `/tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad/r2/wv.py`,
output `h2wv.txt` beside this file). Essential 25, Grammar 5, Relation 1, Standard 22, Unclassified 1; Variant 1 out of
lite's scope.

| Class | Count | Sample |
|---|---:|---|
| G (genuine) | 26 | `average`/`median` of `[1..?]` give 5.0 for 3.8 and 3.0 (6 tests); `testMode_Number`; `testBitShiftLeft_UpTo62Bits` (`70368744177664` for 0); `testBinFloorNegative`; `testModWithNegativeNumbers` (-2 for 3); `testRemWithMixedIntegersAndFloats` (3 for 2.5); `testDecimalToDecimal`/`testDoubleToDecimal` (4D for 3.8D); `testParseDecimalWithPrecisionScale` (123.12D: `DECIMAL(5,2)` truncation); `testDateDiffWeeks` (-1 for 0); `testHour`/`testMinute`; `testNumberPow`; round half-even (2); `indexOf` (2); `substring` (2); `testEqualAll_EmptyRelation`; `testAsciiEmptyChar` |
| P/type | 26 | DateTime precision (8 Standard + `testAdjustByMicrosecondsBigNumber`); Number type (6 Standard + 3 Grammar + `testRemWithDecimals`); `testParseZero` (scale); texts: `testBooleanToString` (`TRUE`), `testDateTimeToString` (2), `testSimpleDateToString`, `testFloatToStringWith{Negative,Positive}Exponent` (`1.3421E-8`, `1.3421E8`) |
| T | 2 | `testEqEnum`, `testEqualEnum` |

H2 adds 6 genuine families DuckDB does not have: averages and medians, mode, shifts within range, `binFloor`, `mod` with
negatives, `rem` over mixed kinds, `toDecimal` rounding, and `parseDecimal`'s truncation. Reproducing H2's would cost at
least as much as DuckDB's, mostly MEDIUM.

---

## 5. The verdict lists

### 5.1 (S) rows CHEAP for full compatibility

- **Form exists today:**
  - S-1 (plain raises; the `toOne` arms are MEDIUM);
  - S-2 (plain parses, BIGINT and DOUBLE);
  - S-3 and S-4 (at pure/v1; and option (i) "the database's everywhere" for the edition);
  - S-5 (always-empty);
  - S-6 (DuckDB);
  - S-8, S-9, S-12, S-14, S-15 (`engine`).
- **One new arm or rule:**
  - S-6 (H2, after the runner);
  - S-7 (per database);
  - S-10 (exists, but ruled K);
  - S-13;
  - S-16 (`engine`);
  - S-19 (`pure`, recommended K).

### 5.2 (S) rows MEDIUM or EXPENSIVE

- **MEDIUM:**
  - S-3 and S-4 exact by placement (the post-inlining placement pass; M1 of the second review);
  - S-5's three unstyled arms;
  - S-11 (run-time-kind promotion forms, the folds at G½);
  - S-16 for `pure` in SQL;
  - S-20 (each defect its own form; ruled K).
- **EXPENSIVE:**
  - S-15 for `pure` (`FLOAT_SHORTEST_TEXT`, more than 490 characters per value);
  - S-17 (written precision);
  - S-18 (Decimal scale through arithmetic);
  - T-8's identity half.

### 5.3 SQL differences each choice leaves, per mode

Counted as rows of §1.3 where the mode's SQL differs from the product's, plus the T rows the mode answers where the
product refuses. P rows are Java, not SQL, except where noted.

| Choice | `engine`: S rows differing from product SQL | `pure`: S rows | T rows each mode answers |
|---|---:|---:|---|
| **All K** (the mode writes the product's SQL; T answered; P in Java) | 0 | 0 | `engine` 6 (T-1, T-2 via T-1, T-4, T-7, T-8 if built, T-10, T-11 if built). `pure` 5 (T-1, T-5, T-6, T-7, T-13) |
| **The recommendation** (§2, line "Recommendation") | 15: S-1, S-2, S-3, S-4, S-5, S-6, S-7, S-8, S-9, S-11 (numbers' folds), S-12, S-13, S-14, S-15, S-16 | 1: S-14 (the legacy TDS's `TDSNull`, for the corpus harness) | as above |
| **All F** (every S row reproduced) | 18 (+ S-22), and §4's 16 families if bugs are reproduced too | 6: S-14, S-15, S-16, S-17, S-18, S-19 | as above |

- **What already exists:** of the recommendation's 15 `engine` rows, 11 have their SQL in lite today (at pure/v1, at
  every edge door, or in the product's lowering): S-1 (part), S-2, S-3, S-4, S-5 (part), S-6 (DuckDB), S-8, S-9, S-12,
  S-14, S-15. The new work is S-7 (per database), S-11, S-13, S-16, the `toOne` arms, and extending S-3/S-4 from
  pure/v1 to the edition.
- **The P rows that are SQL today and move to Java under rule zero:** P-1, P-2 and P-4 at a last step (needs the held
  scalar lift), P-6, P-7, P-8.

### 5.4 Questions only the engine runner can answer

1. **H2 `isNumeric`:** with 4.145.0 translating every H2 SELECT (`relationalMappingExecution.pure:439-442`), is the answer `PGM:367`'s regex full match or `H2X:232`'s `lower = upper`? S370 rests on the latter.
2. **Non-finite Floats at pure/v1:** does legend-engine's JSON writer turn Infinity (DuckDB `x / 0`) and NaN (H2 `sqrt(-2)`) into `null`, as the goldens suggest (`PV1/engine_duckdb_answers.tsv:4`, `PV1/engine_h2_answers.tsv:7`)? For every non-finite value, in TDS and in graph fetch?
3. **`toOne` of a value empty at run time but not always empty** (S384 says not yet asked).
4. **`CAST_IMPOSSIBLE`:** what the user sees when `processCast` writes nothing and the value is the wrong type.
5. **DuckDB `parseFloat`:** `cast(x as float)` is a 4-byte REAL. Does legend-engine's reader return `1.100000023841858` for `'1.1'`?
6. **`substring`, `indexOf`, `toOne`, the raises on Postgres, Snowflake and the other databases** A3 refuses today.
7. **Chained M2M with a TDS result** (`chain.pure:104-116`): `substring`, `==` across kinds and `toString` there.
8. **legend-engine's Java part (a host-side `let`, an in-memory M2M):** an enumeration against a String, a Float's and a DateTime's `toString`, `5->rem(0)`, `sqrt(-1)` (B2 asked numbers' `==` only).
9. **Which run-time-kind comparisons legend-engine refuses** (`Any` lists: "Any is not managed yet!") and which it sends to the database (a `Number`-typed parameter or column), to size S-11.
10. **`testPersonToString`:** legend-engine's text of an instance.
11. **`testHour`/`testMinute`, `testNumberPow`, `testAdjustByMinutesBigNumber`:** the SQL legend-engine writes (only if the user wants §4's bugs reproduced).
12. **M2's H2 settings result** (B2, running): which corpus and PCT answers the two settings carry.

---

## 6. Method notes and facts found on the way

### 6.1 Facts that contradict or refine the standing documents

1. **legend-engine's H2 route is its Postgres-model translation**, not `h2Extension2_1_214.pure`'s dynaFunction table, for every SELECT (`relationalMappingExecution.pure:439-442`, `useDialectTranslation`, already cited by S387). Rows citing `H2X` (S365, S370) should be re-read against `PGM`. The two agree for `parseDecimal`, `parseInteger`, `substring`, `toString`, `rem` and `sqrt`. They **disagree for `isNumeric`**.
2. **The edge's `parseDecimal` is H2's `DECIMAL(5,2)` on every database** (`L/edge/EdgeLoweringForms.java:116-120`); legend-engine's DuckDB writes the bare `decimal`, `DECIMAL(18,3)`.
3. **The edge's DateTime text at legend-engine's door is legend-pure's print**, not legend-engine's: the database's cast (S-16). B2's `e3-design.md` §5 said legend-engine's was "not read or asked". It is read: `DUCKX:284`, `PGM:404`, and the PCT manifests' wrong values.
4. **DuckDB's `parseInteger` cast answers `'1.5'` as 2, hex as a number, `'1e3'` as 1000** (probed), so plain parses change answers, not only error words.
5. **Three `toOne` arms ignore the plain-raise style** (S-5): `engine`'s plain `toOne` is partial today (M4 bucket 4 #15, `testRemError`, is one consequence).
6. **The edge's `keyedCounts` folds** (S-13), against its own comment.
7. **legend-engine's DuckDB `x / 0` is `Infinity`** on both 1.3.0 and 1.5.6 (probed); the `null` in its pure/v1 answer is its JSON writer's. This reconciles DIFFERENCES §3's two statements ("DuckDB ... null" and "DuckDB 1.5.6 answers ... Infinity").
8. **The DuckDB wrong-value count is 47 in scope** (M4); "42" leaves out Unclassified's 5.

### 6.2 The probe (outside every checkout)

- **What:** each statement is legend-engine's SQL text for a construct, read from `DUCKX`, `DEF`, `PGM` and `H2X`, run directly on the databases with no legend-engine Java in between. So the probe answers "what does the database return for legend-engine's SQL", not "what does legend-engine's response say".
- **Jars:** copied from the Bazel cache into `/tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad/r2/jars/` (`duckdb_jdbc-1.3.0.0.jar`, `duckdb_jdbc-1.5.6.0.jar`, `h2-2.1.214.jar`, `h2-2.5.252.jar`).
- **Runner:** `java -cp <jar> /tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad/r2/probe/Q.java <url> <file>`.
- **Inputs:** `/tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad/r2/probe/duck.sql` and `.../probe/h2.sql`.
- **Outputs:** `.../probe/duck-1.3.0.0.out`, `duck-1.5.6.0.out`, `h2-engine.out` (2.1.214 with `MODE=LEGACY;DEFAULT_NULL_ORDERING=HIGH`), `h2-lite.out` (2.5.252, defaults).
- **Caveat:** legend-engine's H2 is patched (S141); the probe used the unpatched 2.1.214.

### 6.3 Not verified

- Q4's implementation site (the ruling is recorded; the code was not found by name).
- The source line of lite's "the database answered NaN for a Float" refusal.
- legend-engine's reader for DuckDB's REAL `parseFloat`.
- legend-engine's mechanisms for §4's families j, k, l, e.
- `testWithFunction.pure:102`'s roster status.
- H2 engine-lane moves under raise option (a).
- PCT H2 relation-lane moves from the null-ordering setting.
- The corpus's DateTime share of M7's 77 text tests.

---

## Errata (reported by the inventory's author after the disk filled; added by B3, 2026-11-25)

- String `indexOf` is at `lowering/Scalars.java:1595-1602`.
- `EngineSubstitutes`: `indexOf` at `:56-59`, `toOne` at `:77-80`.
- `FeatureRules.requireConsumed` at `:117-124`.
- `FLOAT_GRAMMAR` at `AnsiSqlRenderer.java:922-923`.
- `EdgeLoweringForms` relations through computedPruning at `:96-113`.
- `PureText`'s `floatToText` overrides at `:292-534`.
- Not re-verified after the disk filled: `duckdbExtension.pure:865-870` and the `Coercions` / `ToOneGuard` ranges (from earlier reads).
