# B2's progress record (from 2026-11-24)

The one session building all of stage B's B2 (B2a, then B2b), on the user's instruction of 2026-11-24 ("one extra
session for all of b2 ... at extra high effort ... make sure that session is doing interim audits and reviews at check
points"), relayed by the item 3 / B3 session. Branch `claude/compat-b2-nufdlk`. The plan: the plan of record's B2
(`docs/records/compat-stage-b-plan-2026-11-20.md` on `claude/compat-program`) and its B2a/B2b split
(`docs/records/compat-stage-b-plan-2026-11-24.md`). Every slice is measured first, independently reviewed before it is
pushed (a subagent that did not write the code, given the code and the measurements, not the conclusion), gated
(`//gates:local`, plus the lanes it touches), and recorded here.

## The rulings this work builds on

| Ruling | The user's words | What it decides |
|---|---|---|
| 2026-11-19 ~18:05 | "Agree with all four table printing recommendations" | lite's route prints a relation's `toString` in the database; a whole Float `2.0`; a date with a time lite's standard; it lands with B2 (no two-form switch) |
| 2026-11-19 ~18:12 | "Yes record it" | a last-step table text is legend-pure's, printed in Java from typed rows; a text used further is lite's, the difference recorded |
| 2026-11-19 ~18:36 | "Null is good" | a printed table's empty cell reads `null` |
| 2026-11-22 | "Yes always pretty print to iso nanos" | a DateTime's text has nine fraction digits (S312 amended) |
| 2026-11-23 | "I think we do it as 2.0" | Q10: trailing zeros trimmed down to one decimal place (S311 amended) |
| 2026-11-24 | (relayed) slice 2 held until B2b, option C | lite's route's table `toString` lands with B2b, with no temporary two-form facet |
| 2026-11-24 (relayed ~12:57) | "Agree on reword catalog - keep errors close to pure when cheap/fast so don't have to reword everything on compatibility" | the product's assert-failure catalog takes legend-pure's sentences where cheap (a B3 step): B2b builds NO edge-side rewording of assert words; S398's retirement mostly goes away, a few tweaks at most, named by the B3 session |
| 2026-11-24 (relayed ~13:43) | "Q6 fine", "Q7 fine" | Q6: the checked graph's violation sentence follows the catalog rule (legend-pure has none, so lite's own words; legend-engine's at the edge only where a compatibility test checks it). Q7: the defects cell dropped |
| 2026-11-24 (relayed ~13:43) | "I don't think we should keep adding random flags to plan? I thought we already have mechanism for this that substring/indexof use" | no new plan fact for Q8 or the string entry: the door (the A3 walker over the typed query, edge/PureMeaningRewrite, B3's UpstreamTestBodies) LIFTS an assert whose side is a relation's own toString/toCSV out of the body; the harness checks it afterwards through lite's public door with B2b's printer as the receiver (B3 builds the lift; B2b's S4 hands it the printer). On pure/v1 the string entry is the door's output receiver |
| 2026-11-24 (relayed ~13:43) | (the B3 session's direction) | F31, legend-engine's plain parse casts, moves into legend-engine's door rewrite (A3's pattern, EngineSubstitutes) when PurePrintForms goes, not to B4; the calendar's 41 toCSV-then-edit cases wait for the user's ruling after B3 step 1b (lite's text assumed; nothing depending on it lands) |
| 2026-11-24 (relayed ~13:51) | "I think toString and toCsV with more string functions like replace should also be all considered part of the toString rewrite if that's a good idea / not crazy - that might also fix these lost tests?" | in a compatibility test body, a side starting with a relation's own toString/toCSV followed by further string functions is lifted too: the harness prints the rows with B2b's printer and substitutes the text for the call (amends ~18:12 for compatibility test bodies only) |
| 2026-11-24 (relayed ~13:53) | "No I'm saying also do the replace in java too it's really just more string functions on the CSV" | a print followed by `replace` calls with literal arguments is one printing step in Java (the corpus's 41 calendar cases are all `toCSV(...)->replace('\n', ',')`): the printer's entry takes typed rows and a list of literal (target, replacement) pairs; anything else after a print falls back to the substituted text run as lite |
| 2026-11-24 ~06:20 | "Why are you asking questions like should I build oracle and raise limits? Could have been done ... if just kept building" | the direct, measured cost of a ruling already made is done and reported; only a new difference from upstream or a new semantics question goes to the orchestrator first |

## B2a

### Slice 1: the product bug B2's prototype exposed (verified, no code)

The window-inside-a-list lowering defect (H11 §8 P1): fixed on `main` before this work (`e201769df`, `bf2e9c4c3`, merged
in #128); `SelectMergeQueriedOperandsTest` green, PCT SQLite's `testExtendJoinStringOnNull` and
`testOLAPAggStringWithPartitionAndUnboundedWindow` pass and are in no ledger (2026-11-24).

### Slice 3: a whole Float keeps one fraction digit (`2.0`)

**The rule** (S311 amended, the row beside S311 in `docs/records/d-register-rows-2026-11-16.md`): six decimals, the
trailing zeros trimmed, a whole value keeping one (`2.0`, `0.0`, `100000000000000000000.0`). The database computes it:
each dialect's six-digit text has a whole value's `.000000` marked `.#` before the trailing trim and written `0` after
(`AnsiSqlRenderer.trimmedSix`, `trimmedSixStandard`); Snowflake from 1e32 and MySQL from 1e60 append `.0`; MongoDB
trims only a text with a point and ends an integer text in `.0`. A Float the store holds as an integer takes the same
text (`LiteralSpelling.floatPrint`); the edge's text of it stays legend-engine's cast.

**Measured (2026-11-24):**

| Where | What moved |
|---|---|
| core (`//core:core_tests`, the SQLite and H2 lanes) | 5 cases re-pinned: `FloatTextTest.aFloatsTextIsSixDecimalsTrimmed` (DuckDB, H2, SQLite), `TypeInferenceIntegrationTest.testFloatToStringWithNegativeExponent` (each lane), `AuditRound3Test.floatReprLargeBand`, `BigQueryDialectTest`, `SnowflakeDialectTest`; `MongoDbPlanTest`'s two product texts; a new case, `RelationalMappingIntegrationTest.testFloatHeldAsIntegerPrintsAFloatsText` |
| PCT under lite's rules | `testFloatToStringWithExcessTrailingZeros`, `testFloatToStringWithPositiveExponent` pass and leave the LiteRules Essential ledger and channel B's duckdb-lite rows; `testFloatToStringWithNegativeExponent`'s text `0` becomes `0.0` |
| compatibility lanes | unchanged: `pct_duckdb` (5 suites), `pct_sqlite` (5), `pct_h2`, channel B (5), `//spec:corpus_duckdb`, `//spec:corpus_h2` |
| `//core:product_raise_probe` | every Float text right on DuckDB, H2, SQLite, Postgres, MariaDB, MySQL, ClickHouse, Trino, MongoDB; the arithmetic cases still failing on ClickHouse, Trino and Postgres fail identically on the base |
| guards, UP on the user's word of 2026-11-24 ~06:20 (the direct cost of the 2026-11-23 ruling) | the lean-SQL baseline's 24 rows (`toString`, `format`, every database; +33 a value, +21 under the standard trim); channel B's lite Essential judge pins, +264 characters and plan JSON |

**Gated (2026-11-24, at b694378e2 and d3b10b173):** `//gates:local` green (its 14 Postgres-backed targets refuse root
in a cloud container and pass run unprivileged, `tools/postgres/as_unprivileged.sh`; the parser-equivalence floor
3344 -> 3345 for the new test's model), `//core:core_tests_lanes`, `//pct:pct_channel_b_lite_essential`, `//pct:edge_net`
(40 of 40), `//tools/guards:shrink_only_check`; before them (08e37643b) both corpora, every PCT lane named above,
`//core:mongodb_plan_test` and the product probe on MongoDB.

**A wrong value fixed on the way** (on the base too): MongoDB printed 1e33 as `1` (Decimal128's 34 digits: `$round`
keeps no fraction there, and the trailing-zero trim ate the integer's zeros).

### Slice 4: an abstract Date held as a timestamp prints a DateTime's text

(filled in below as it lands)

## Audits

| When | Scope | Findings | Fixed |
|---|---|---|---|
| 2026-11-24 | slice 3's diff (`96e7c1fc4..396961c73`), an independent subagent, read-only, given the code and the requirement | 5 must-fix: MongoDB printed `NaN.0`/`Infinity.0` (an unconditional `.0`); channel B's ledger row read `got "00.0"` (a slicing slip); channel B's lite Essential judge pins not re-pinned (+264); `MongoDbPlanTest`'s two product texts stale; a Float held as an integer printed `2` beside a computed `2.0` (one text per type broken). 1 should-fix: the probe's whole-value check too loose (a lost sign or a wrong digit count passed). Nits: stale javadocs, long lines. No defect in the shared helpers' SQL (simulated on every six-digit text kind, the non-finite texts included); the edge untouched | all, in `08e37643b` and the slice's last commit; the probe pins -2.0 and 1e20 exactly, and NaN and the infinities exactly |
