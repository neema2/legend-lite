# pure/v1 against legend-engine: the raise-kind census (2026-11-17)

The user's ruling (2026-11-17, relayed by the orchestrator): **the pure/v1 route matches legend-engine.** Where
legend-engine writes no check of its own, lite writes none on that route either, and the database's own error passes
through. PCT keeps legend-pure's words.

This record holds the census that sets the scope of that build, so the scope can be checked later.

## Sources
- legend-engine at tag `legend-engine-4.145.0` = `230c159196d6512486fd382556c5e9e4fb128ebb`.
- legend-pure at tag `legend-pure-5.99.0` = `7fbc7d6e8d52e2488bdae67280e5f5dfed448d68`.
- Both are public clones, verified against `git ls-remote`. Never the bazel cache.
- All 77 relational PCT manifests were read (7 scopes x 11 databases), plus legend-pure's `.pure` tests and the cited Java
  files (`git show <tag>:<path>`).
- Lite's side: `core/src/main/java/com/legend/edge/PureMessages.java` (`sentence`, `wordsOrNull`).
- `M(db, Scope:line)` stands for
  `legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-dbExtension/legend-engine-xt-relationalStore-<db>/legend-engine-xt-relationalStore-<db>-PCT/src/main/resources/pct-manifests/relational-<db>/<Scope>_manifest.json:<line>`.
- Reading a manifest entry:
  - no entry means the test passed upstream;
  - "No error was thrown" means the database returned a value;
  - "Unexpected error executing function with params [Anonymous_Lambda]" is the engine's wrapper, with the database's text
    not recorded.
  - Matching is a substring test (`PureTestBuilderCompiled.java:166`).

## The classes and the decision

| Class | What legend-engine does on pure/v1 (relational) | Kinds | Decision |
|---|---|---|---|
| **A** | No check in the query: the database decides | DIVIDE_BY_ZERO (both forms), SQRT_OF_NEGATIVE, SHIFT_OUT_OF_RANGE, SIZE_NOT_MULTIPLICITY (toOne → `processNoOp`), SLICE_BOUNDS_INVERTED, RANGE_STEP_ZERO, CAST_IMPOSSIBLE, TEXT_NOT_INTEGER / TEXT_NOT_FLOAT / TEXT_NOT_DATE | **Built now:** a plain operation on the pure/v1 route only; PCT keeps legend-pure's words |
| **B** | Refuses at planning, in its own words | DATE_PART_ABSENT, INDEX_OUT_OF_BOUNDS (`at`), DATE_COMPONENT_INVALID, likely USER_FAIL | Follow-up **F43** |
| **C** | Raises the same sentence (a Pure assert at planning) | WINDOW_FRAME_INVERTED | Kept: already matches |
| **D** | Its text matches lite's | CONSTRAINT_UNEVALUABLE | Kept: already matches |
| **E** | No upstream counterpart (lite's own) | INSERT_OUT_OF_BOUNDS, INSTANCE_NOT_OF, WINDOW_FRAME_OFFSET_INVALID, LIST_LENGTHS_DIFFER, MAP_KEY_*, VALUE_NOT_HELD, FRACTION_NOT_KEPT, CSV_TOO_WIDE | Follow-up **F44** (ask the engine runner) |

Defects found on the PCT side:
- **F45, MATCH_FAILURE:** lite writes "… matches no branch". legend-pure writes "Match failure: <value> instanceOf <Type>"
  (`CompiledSupport.java:1588`; `match.pure:240`).
- **F46, DIVIDE_BY_ZERO:** the javadoc cites `CompiledSupport:1485` for rem, but that line is divide and prints the divisor.
  rem is `CoreHelper.java:544` / `Rem.java:64`, which print the dividend. The operand-less "Division by zero" is not
  legend-pure's compiled answer, which is "Cannot divide 0 by zero".

## The table, per kind: (a) lite's edge words | (b) legend-engine 4.145.0 on pure/v1 | (c) legend-pure / engine PCT

| RaiseKind | (a) lite | (b) engine on pure/v1 | (c) PCT expectation |
|---|---|---|---|
| DIVIDE_BY_ZERO, rem / quotient | `Cannot divide ⟨dividend⟩ by zero` | No guard: H2 `mod` (toPostgresModel.pure:426). `testRemError`: H2 wrapper M(h2,Essential:490-491); DuckDB "No error was thrown" M(duckdb,Essential:304-305); Postgres wrapper M(postgres,Essential:478-479); Snowflake wrapper M(snowflake,Essential:292-293) | `rem.pure:94` 'Cannot divide 5 by zero'; `CoreHelper.java:544`, `Rem.java:64` |
| DIVIDE_BY_ZERO, Integer / Integer | `Division by zero` | No guard (`processDynaFunction`, pureToSQLQuery.pure:10674; H2 `/`, toPostgresModel.pure:434). H2's own text, M(h2,Grammar:6): "org.h2.jdbc.JdbcSQLDataException: Division by zero: \"12.0\"; SQL statement:\nselect false and ((1.0 * 12) / 0) > 0" | No PCT test; compiled `CompiledSupport.java:1483-1485` 'Cannot divide <divisor> by zero' |
| SQRT_OF_NEGATIVE | `Unable to compute sqrt of ⟨x⟩` | No guard (H2 `sqrt`, toPostgresModel.pure:328). `testSquareRootError`: H2 "No error was thrown" M(h2,Essential:505-506); DuckDB / Postgres wrapper; Snowflake mismatch M(snowflake,Essential:304-305) | `sqrt.pure:54` 'Unable to compute sqrt of -1.0'; `CoreHelper.java:520` |
| SHIFT_OUT_OF_RANGE | `Unsupported number of bits to shift - max bits allowed is 62` | No guard: H2 "No error was thrown" M(h2,Standard:157-158); DuckDB wrapper; Postgres unsupported-api; Snowflake wrapper | engine `bitShiftLeft.pure:41`, `bitShiftRight.pure:41` |
| SIZE_NOT_MULTIPLICITY | `Cannot cast a collection of size ⟨n⟩ to multiplicity ⟨b⟩` | `toOne` → `processNoOp` (pureToSQLQuery.pure:10793-10794): no check in the query. The 38 manifest entries carrying the sentence come from the planner's own `toOne` | not PCT (`toOne.pure:83-84`) |
| SLICE_BOUNDS_INVERTED | `The low bound (⟨l⟩) can't be higher than the high bound (⟨h⟩) in a slice operation` | DuckDB / Snowflake "No error was thrown"; H2 / Postgres refuse at planning (M(h2,Essential:220-221), M(postgres,Essential:217-218)) | `slice.pure:75` |
| RANGE_STEP_ZERO | `range step must not be 0` | DuckDB "No error was thrown"; H2 / Postgres refuse at planning (M(h2,Grammar:109-110), M(postgres,Grammar:76-77)); Snowflake wrapper | `range.pure:131` |
| CAST_IMPOSSIBLE | `Cast exception: ⟨type⟩ cannot be cast to ⟨target⟩` | `processCast` (pureToSQLQuery.pure:10550): no run-time guard | not PCT (`cast.pure:144`; `CompiledSupport.java:2157`) |
| TEXT_NOT_INTEGER | `Could not convert string '⟨text⟩' to INT64` | Plain cast; the database raises. H2 `cast(… as integer)` (toPostgresModel.pure:402); M(h2,Essential:598-599) "…Data conversion error converting \"9999999999999992\"; SQL statement:\nselect cast('9999999999999992' as integer) [22018-214]" | no bad-text test (`parseInteger.pure:43-50`) |
| TEXT_NOT_FLOAT | `Could not convert string '⟨text⟩' to DOUBLE` | Plain `cast(… as float)` on all four | no bad-text test (`parseFloat.pure:43-57`) |
| TEXT_NOT_DATE | `Could not parse '⟨text⟩' as a date` | The database's own parse error (H2 M(h2,Essential:586-587), Postgres M(postgres,Essential:571-572), Snowflake M(snowflake,Essential:367-368)); DuckDB unsupported-api | no bad-text test |
| DATE_PART_ABSENT | `Cannot get ⟨part⟩ for ⟨date⟩` | Refused at planning: "Ensure the target system understands Year or Year-month semantic." (H2 / Postgres / Snowflake); DuckDB "DuckDB doesn't support YEAR and YEAR-MONTH" | `dayOfMonth.pure:53` and siblings |
| INDEX_OUT_OF_BOUNDS | `The system is trying to get an element at offset ⟨o⟩ where the collection is of size ⟨s⟩` | Refused at planning on all four: "->at(...) function is supported only after direct access of 1->MANY properties" | `at.pure:72` |
| DATE_COMPONENT_INVALID | `Invalid ⟨component⟩: ⟨value⟩` | Refused at planning on all four: "No SQL translation exists for the PURE function 'date_Integer_1__Integer_1__Date_1_'" | `date.pure:132-134` |
| WINDOW_FRAME_INVERTED | `Invalid window frame boundary - lower bound of window frame cannot be greater than the upper bound!` | Same sentence, a Pure assert at planning (`rows.pure:29`, `range.pure:33`); passes on all four | `over.pure:957,984`, `reduce.pure:887,914` |
| CONSTRAINT_UNEVALUABLE | `Unable to evaluate constraint [⟨id⟩]: data not available - check your mappings` | Matches `constraints.pure:171` | no PCT test |
| MATCH_FAILURE | `Match failure: ⟨type⟩ matches no branch` | Unknown for a query; the manifests' entries are planner-internal | `match.pure:240` (`Match failure: 42 … instanceOf Integer`); `CompiledSupport.java:1588` |
| INSERT_OUT_OF_BOUNDS, INSTANCE_NOT_OF, WINDOW_FRAME_OFFSET_INVALID, LIST_LENGTHS_DIFFER, MAP_KEY_*, VALUE_NOT_HELD, FRACTION_NOT_KEPT, CSV_TOO_WIDE | lite's own words | Unknown; needs the engine runner | no test |
| USER_FAIL | the user's message | `fail` has no SQL entry; likely refused at planning (unverified) | no PCT test |
