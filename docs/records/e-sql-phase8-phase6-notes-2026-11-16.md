# E-SQL design notes: the rest of Phase 8, and Phase 6 (2026-11-16)

Two short notes for the orchestrator's file-overlap check, before Phases 2, 3, 5, 6 and 8 run in parallel. Measured on
`claude/total-wall-carriers` (cut from d-stage c41a2705, Phase 8's parse family on top).

## (a) The rest of Phase 8: exact carriers on every database, the emulations deleted once unreached

### 1. What changes, measured

- **Done on this branch:**
  - SQLite's JSON carrier of a computed Float (15 to 17 digits).
  - The parse family: SQLite (exact grammar), MariaDB/MySQL (REGEXP) and DuckDB (`regexp_full_match`).
- **Left:**
  1. **The shared Float carrier.** It is one function per database: "this double as text that reads back exactly". It is
     used where a value is carried as text (a list or JSON carrying a double), and by D's Phase 2 for a raise's Float
     operand (SQRT_OF_NEGATIVE).
     - Measured so far: DuckDB's and SQLite 3.53's `CAST(double AS text)` read back (2,011/2,011). SQLite 3.47's does
       not (15 digits; `printf('%!.17g')` does).
     - Not yet measured: Snowflake (its own cast writes 10 significant digits, the 2026-10-21 lane note), BigQuery,
       MariaDB, MySQL, ClickHouse, Trino, Databricks.
     - First step: `FloatCarrierExactTest` and `ParseFloatHeldTest`'s `[parse-table]` already run on every lane, so one
       lane round gives the table.
  2. **The shortest-text emulations, deleted once no judge and no carrier reaches them** (the plan's row 20):
     - Snowflake: `floatText` and `shortestDigits`, about 150 lines;
     - SQLite: the 15/16/17-digit `printf` ladder;
     - MariaDB: `floatLayout`;
     - BigQuery: its `FORMAT('%.17g')` form;
     - ClickHouse: its `.0` regexp.
     Today they are reached by FLOAT_PRINT and FLOAT_CANON (E-Java's W10a and W10b delete those consumers) and by any
     cast of a double to text.

### 2. Files

- `core/src/main/java/com/legend/sql/dialect/`: `Snowflake.java`, `SQLite.java`, `MariaDb.java`, `BigQuery.java`,
  `ClickHouse.java` (the emulations and their carriers), `AnsiSqlRenderer.java` (the carrier hook's default),
  `FloatPrintText.java` (deleted with the last emulation).
- `core/src/test/java/com/legend/integration/FloatCarrierExactTest.java` and `ParseFloatHeldTest.java`.
- The size pins: `core/src/test/resources/lean-sql/baseline.tsv`, `pct/.../channelb/ratchets.tsv` and the corpus
  baselines (re-pinned, dated).

### 3. Contracts

- **D's Phase 2** (plan format 56, raises): the Float carrier is the function Phase 2's raise operand calls. It is one
  dialect hook (say `exactFloatText(Piece)` on `AnsiSqlRenderer`), agreed with D before either builds it. Phase 8 adds no
  plan field and no format bump.
- **Phase 3** (the presenter): no overlap. The presenter prints typed values in Java; a carrier never prints.
- **E-Java's Phase 4** (W10a/W10b): the emulations are deleted only after W10a and W10b remove FLOAT_PRINT and
  FLOAT_CANON. Phase 8 deletes no arm a judge still reaches.

### 4. Risks, and what is measured before building

- Snowflake may have no single exact cast. Its format models cap at 16 digits, and `TO_JSON` of a double is unmeasured.
  Measure on the lane first. If nothing is exact, carry the typed value: a VARIANT holding the double itself.
- Each emulation's deletion is gated on a census that it is unreached (a grep for its callers, plus the net and PCT
  green).
- A carrier change moves sizes; each pin moves with a dated reason.

## (b) Phase 6: legend-engine's plan text at the edge (W9's planner half)

### 1. What changes, measured

The planner still holds legend-engine's plan text in four places. W9 step 1 (`UpstreamPlans`, D's d4bab2df6) moved
`planToString`, `toSqlString`, the deprecated `toDDL` text, the plan node model and an execute's activity SQL behind the
plan style's facet. Left:

1. **`planner/PlanAllocations.java` (487 lines):**
   - the plan's `processingTemplateFunctions` (freemarker, line 69);
   - the engine-style render of a frame's own chain (149 to 178);
   - `engines()` (208).

   They become `UpstreamPlans` methods, implemented in `com.legend.edge` and refused by name in the product's style.
2. **`planner/ProductStatements.java` (460 lines):** the engine-text routines (`UpstreamPlans.routines`, 149) and the
   CTE naming that mirrors legend-engine's `cteExtractionPostProcessor` (345). The product's own statements stay. The
   engine's spellings move to the facet.
3. **`sql/dialect/RawSqlBoundary.java` (339 lines):** the engine's H2 text that `executeInDb` hands a session,
   translated per database. It is compatibility, so it moves to the edge (`com.legend.edge`), and the product refuses raw
   engine text by name.
4. **`plan/PlanNode.java`'s `EXEC_TRACE_COMMENT`** (`-- "executionTraceID" : "${execID}"`): legend-engine's trace
   comment, written into the plan. It moves to the edge's plan text and leaves the product's plan record.

### 2. Files

- `core/src/main/java/com/legend/planner/`: `PlanAllocations.java`, `ProductStatements.java`, `UpstreamPlans.java`
  (new methods).
- `core/src/main/java/com/legend/plan/PlanNode.java`.
- `core/src/main/java/com/legend/sql/dialect/RawSqlBoundary.java` (moved to `com/legend/edge/`).
- `core/src/main/java/com/legend/edge/`: the facet's implementation (D's `EnginePlans` or its successor).
- The product style's refusals (`PlanStyle`), and `testplan/PlanStyleFacets.KINDS` if a kind is added.
- The guards: `CompatSpellingGuardTest` (its engine-plan-text rows shrink: about 37 today) and `ArchitectureTest` (the
  edge's direction).

### 3. Contracts

- **D's Phase 2** (plan format 56): removing `EXEC_TRACE_COMMENT` from the product's plan changes the plan's text. It
  lands with or after format 56, never bumping the version separately. D owns the number.
- **Phase 3** (the presenter): no overlap. Phase 6 is planning-side text; the presenter is result-side.
- **D's Phase 5** (in-query items as legend-engine's SQL) and **Phase 7** (separation by the build): Phase 6 only moves
  text to `com.legend.edge`, and Phase 7 then enforces the direction. Phase 6 must not make the product depend on
  `edge`.
- **E-Java's Phases 4 and 5:** the judges read plans through the test planner's style. A facet method the judges call
  stays on the style they use. Check `testplan` callers of each moved method first.

### 4. Risks, and what is measured before building

- PCT and the corpora read plan text (`planToString` tests and the corpus's plan assertions). Run the edge golden net and
  the corpus lanes before and after each move: zero edge drift is the bar.
- `RawSqlBoundary` is reached by `executeInDb` on every lane. Census its callers first, and keep H2's pass-through
  behaviour.
- The guard pins shrink only, each with a dated reason.
- File overlap with D's Phase 2: `PlanNode.java` (format 56). Agree the order with D before touching it.
