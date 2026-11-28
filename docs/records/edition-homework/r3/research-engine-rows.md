# legend-engine's rows for the edition table: an implementation sheet (2026-11-25, late night)

Read-only research. Nothing was edited, built or run.

**Read this first: the checkout moved while I worked.** The branch `claude/modest-galileo-h6swr4` went from `c06ebbab` to
`33d85c92` (14 edit-ahead commits). Those commits include `e20b54cb` (the table: `lowering/EditionTable.java`,
`edge/EngineTable.java`, the stamp `TypedNativeCall.outsideStore`, the consult at `Scalars.lower`) and `3ca2afd2` (E0c:
the dead two-form choices `keyedCounts`, `relations`, `view`, `wireCast`, `computedPruning`, `unionFrame` deleted from
`LoweringForms`). **Every lite line number below is at `33d85c92`.** Scalars.java lines after 2818 moved by +6 relative to
the r3 trace.

**Paths.**
- `L/` = `core/src/main/java/com/legend/`.
- `ENG/` = `/root/.cache/bazel/_bazel_root/e8817d619d2dbe75028d8b0114f08e2c/external/+http_archive+legend_engine_src/`.
- `CORE/` = `ENG/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/`.
- **Files:**
  - `DX` = `ENG/.../legend-engine-xt-relationalStore-duckdb-pure/.../duckdbExtension.pure`
  - `ED` = `CORE/sqlQueryToString/extensionDefaults.pure`
  - `P2S` = `CORE/pureToSQLQuery/pureToSQLQuery.pure`
  - `PGM` = `CORE/sqlDialectTranslation/toPostgresModel.pure`
  - `RME` = `CORE/relationalMappingExecution.pure`
  - `H2D` = `ENG/.../legend-engine-xt-relationalStore-h2-sqlDialectTranslation-pure/.../h2SqlDialect.pure`
  - `SDD` = `ENG/.../legend-engine-xt-relationalStore-sqlDialectTranslation-pure/.../defaults/sqlDialectDefaults.pure`
  - `LH2` = `ENG/.../legend-engine-xt-relationalStore-executionPlan-connection/src/main/java/org/finos/legend/engine/plan/execution/stores/relational/LegendH2Extensions.java`
  - `MD/`, `MH/` = the `relational-duckdb/` and `relational-h2/` PCT manifests.
  - `LANE/` = `pct/engine_lane/`.
- **The H2 route (verified).** legend-engine 4.145.0 sends every H2 SELECT through the Postgres-model translation:
  `RME:440-443` (`useDialectTranslation`: `$dbType->in([DatabaseType.H2])`). The H2 text is therefore `PGM` → `H2D`,
  never `h2Extension2_1_214.pure`. DuckDB's dynaFunction table is `ED`'s defaults overlaid by `DX`'s
  (`DX:25-26`, `getDynaFunctionToSqlDefault(...)->putAll(getDynaFunctionToSqlForDuckDB())`).

## 0. The mechanism as it stands at 33d85c92

- **The consult.** `L/lowering/Scalars.java:2818-2829`: `styled.edition().lowersAs(key, call.outsideStore(), features)`
  replaces `key` before `FeatureRules.select` and `RULES.get`.
- **The tables.**
  - `L/lowering/EditionTable.java`: the interface. `LITE` has no row.
  - `L/edge/EngineTable.java`: `substring`→`substringAsGiven` (`substrAsGiven` under `CORRECT_SQL_SUBSTRING_INDEXING`),
    `substr`→`substrAsGiven`, `INDEX_OF__STRING_1__STRING_1`→`sqlPosition`, none when `outsideStore`.
  - Today it is handed out only under `-Dlegend.probe.stamp` (`L/edge/PureText.java:174-177`, `:238-242`). `UPSTREAM`
    hands `LITE` (`:138-142`).
- **The raise choice is still not in the table.** It is `LoweringForms.raise` (`L/lowering/LoweringForms.java:46`):
  - the product's: `L/lowering/ProductLoweringForms.java:26`;
  - the edge's: `L/edge/EdgeLoweringForms.java:37`;
  - `PURE_V1` = `PureText.ENGINE_UNCHECKED` (`L/edge/PureText.java:179-184`);
  - PCT and the corpus: `PCT`, none plain (`EdgeLoweringForms.java:27`).
- **Three structural gaps the rows below keep hitting:**
  - **G1, no database in the table.** `lowersAs(key, outsideStore, features)` has no database argument, but at least
    six rows differ by database:
    - `isNumeric`, `parseDecimal`, `parseJSON` and `splitPart` take different forms;
    - `round` and `dateDiff` apply only where the dialect writes the database's own function.

    Today a per-database choice can only come from the style's dialect subclass, `PlanStyle.dialect(type)`.

    **Proposal:** add the target `DatabaseType` to `lowersAs`. `PlanEnv` already derives it from the runtime
    (`PlanEnv.floatLiteralKind`, `L/planner/PlanEnv.java:85-89`). It is null when the plan names no runtime, and then the
    table must refuse by name rather than guess (invariant 4).
  - **G2, the stamp is on calls only.**
    - `TypedNativeCall.outsideStore` exists (`L/compiler/spec/typed/TypedNativeCall.java:26-38`).
    - The raise sites that are not inside a native rule have no call to read it from: `CastPolicy` (a `TypedCast`),
      `ListEncodings.slice` (`Lowerer.java:4130-4140`), and `ToOneGuard` and `requiredOneEgress` (statement roots).
    - `Scalars.guarded(cond, msg, value, styled)` (`Scalars.java:3154-3156`) has no call argument either. Its callers
      hold one (`n`), so the change is mechanical.
  - **G3, statement-level choices have no slot.** The keyed count (`Lowerer.java:534`) and the null-unsafe flag
    (`Lowerer.java:219-233`) are not native calls. `EditionTable` would need one boolean method each, or they stay
    outside it.

---

## Row 1. The unchecked operations (S-1, S-22)

**`PureText.ENGINE_UNCHECKED`** (`L/edge/PureText.java:179-184`) holds DIVIDE_BY_ZERO, SQRT_OF_NEGATIVE,
SHIFT_OUT_OF_RANGE, SIZE_NOT_MULTIPLICITY, SLICE_BOUNDS_INVERTED, RANGE_STEP_ZERO, CAST_IMPOSSIBLE, TEXT_NOT_INTEGER,
TEXT_NOT_FLOAT and TEXT_NOT_DATE.

### 1a. Sites that ask `forms().raise` (the table's raise map works here with no other change)

| Kind | Site | Checked form | Plain operation |
|---|---|---|---|
| SHIFT_OUT_OF_RANGE | `Scalars.java:437-451` (shift rule, `:428-452`) | bound 0..62, literal or CASE | `(CAST(a AS BIGINT) << b)`. DuckDB arm `DuckDb.java:1113-1114`; H2 `bitOp` refuses every bit operation (`H2.java:716` → `AnsiSqlRenderer.java:1623-1625`) |
| DIVIDE_BY_ZERO (rem) | `Scalars.java:466-482` → `guarded(..., styled)` `:477` | `CASE WHEN y = 0 THEN error` | `SqlFn.REM` → `MOD(x, y)` (`AnsiSqlRenderer.java:1464-1466`, `DuckDb.java:576`, `H2.java:657`) |
| DIVIDE_BY_ZERO (Integer/Integer `/`) | `DecimalKindRules.java:44-64` → `Scalars.guarded(..., styled)` `:57` | guard | `SqlFn.DIVIDE` |
| DIVIDE_BY_ZERO (lite's quotient) | `Scalars.java:2597-2602` | guard | `INT_DIVIDE` |
| SQRT_OF_NEGATIVE | `Scalars.java:1974-1983` | `x < 0` guard | `SQRT(x)` |
| RANGE_STEP_ZERO | `ListRules.java:24-29` | guard | `RANGE_FN` |
| SIZE_NOT_MULTIPLICITY ([0..1] value lane) | `Coercions.java:101-107` | `IS NULL` guard | the operand |
| SLICE_BOUNDS_INVERTED | `ListEncodings.java:258-270` (asks `forms.raise` `:265`) | `lo > hi` guard | `LIST_SLICE` |
| CAST_IMPOSSIBLE (TypedCast) | `CastPolicy.java:182-184`, called at `:71`, `:192`; raise built `:234-237` | ERROR | the value (legend-engine's `processCast`) |
| SUBSTRING_OUT_OF_RANGE (S-22) | `Scalars.java:3117-3149` → `guarded(..., styled)` `:3147` | guard | `CORRECTED_SUBSTRING` |

The last row is moot under `engine`: the table replaces `substring` before this rule runs.

### 1b. Sites of the same kinds that do NOT ask (unrouted)

1. **SIZE_NOT_MULTIPLICITY, a toOne of a statically empty value**, `Coercions.java:63-67`.
   - Writes `ERROR(placed SIZE_NOT_MULTIPLICITY "[1]")` unconditionally.
   - Route: `styled.forms().raise(SIZE_NOT_MULTIPLICITY, () -> ERROR..., args.get(0))`. The plain operation is the
     lowered empty operand, which is what `trustOne` returns (`Coercions.java:118-120`).
   - **This is the site pure/v1's `raise.toOneOfEmpty` reaches** (`[]->cast(@Integer)->toOne()`,
     `core/src/test/resources/edge/pure_v1_requests.tsv:14`). Today A3's `trustOne` answers it. Under the stamp probe
     (A3 skipped) it would move from 200 null to 500 unless this site is routed. **Route it before deleting A3.**
2. **SIZE_NOT_MULTIPLICITY, `CheckedOne` arms**, `Coercions.java:72-78` (value-lane many) and `:82-87` (list-producing
   call).
   - The node is rendered as a guard by `AnsiSqlRenderer.checkedOne` (`AnsiSqlRenderer.java:816-850`, dispatched at
     `:1062`; MongoDb `:313`).
   - legend-engine's `processNoOp` (`P2S:3133-3137`, registered `:10793`) writes the operand alone. But lite's operand
     here is a **list carrier**, and the operand alone would put a LIST where a scalar is typed.
   - **No faithful plain form exists.** legend-engine refuses or never reaches these shapes (`raise.toOneOverMany` is a
     planning refusal in its goldens).
   - Recommendation: keep these checked. Record that legend-engine has no counterpart. Do not invent a "first element"
     form.
3. **SIZE_NOT_MULTIPLICITY, toOneMany**, `Coercions.java:174-179` (static empty, routable as item 1) and `:182-187`
   (`CheckedOne`, as item 2).
4. **SIZE_NOT_MULTIPLICITY at the root of a relation**, `Lowerer.java:835` → `ToOneGuard.exactlyOne`
   (`ToOneGuard.java:38-73`, the raise at `:69-70`).
   - It is not a call rule. `forms().raise` takes `Supplier<SqlExpr>`, so routing it needs a generic
     `<T> T raise(RaiseKind, Supplier<T>, T)` (or a `SqlQuery` overload), and the plain form is `rows`.
   - **Caution:** `Lowerer.java:1041-1048` says legend-engine's Java executor checks the finished result's row count
     against the declared multiplicity (`resultSizeRange`). I did not verify that claim in legend-engine's executor. If
     it holds, the root guards are legend-engine's own behaviour and should stay checked under `engine`. Ask the runner.
5. **SIZE_NOT_MULTIPLICITY at a scalar root**, `Lowerer.java:1060-1083` (`requiredOneEgress` → `new
   SqlExpr.CheckedOne(op)` `:1082`). This is the same story as item 4.
6. **SIZE_NOT_MULTIPLICITY in the renderer**, `AnsiSqlRenderer.java:827`, `:837`.
   - The renderer only spells a `CheckedOne` it was handed and has no style. Route at the creation sites above, never
     here.
   - Its javadoc ("engine-TEXT subclasses override to the verbatim inner value") is stale: those subclasses went with
     A2.
7. **CAST_IMPOSSIBLE in the resolver**, `resolver/Substitution.java:2323-2326`.
   - `ChainDispatch.typeRaise` (`resolver/ChainDispatch.java:231`) builds a `Lite.TYPE_RAISE` native inside a `TypedIf`
     (a test of the row's class, then the leaf, else the raise). It is lowered by `Coercions.java:126-144`, which has no
     `forms().raise`.
   - Routing it is two changes:
     - (i) the TYPE_RAISE rule asks `forms().raise(kind, ...)`;
     - (ii) the plain operation must be the leaf. That is the `TypedIf`'s then-branch, which the rule cannot see.
   - So either the resolver stops wrapping (it knows nothing of editions: guard 2), or the pass reduces `if(test, leaf,
     typeRaise)` to `leaf`.
   - This is phase H, not lowering. **Leave checked; record it.** No PCT test reaches it (r2 §3.2: "unverified; ask the
     runner").
8. **TEXT_NOT_INTEGER and TEXT_NOT_FLOAT are written at RENDER.**
   - The shared helpers are `AnsiSqlRenderer.java:906-913` (`guardedParseFloat`) and `:952-960` (`guardedParseInt`).
   - **DuckDB:** PARSE_INT `DuckDb.java:582-584`; the parseFloat guard inline at `DuckDb.java:1359-1368`.
   - **H2:** PARSE_INT `H2.java:663`, `:1318-1322`; parseFloat `H2.java:1367-1375`.
   - **Others:** MariaDb `:1731`, `:2287`; SQLite `:1421`, `:2027`; MongoDb `:650`, `:1069`.
   - The edge strips them with the render pass `UnguardedParses` (`L/edge/PurePrintForms.java:47-61`), which rewrites
     `PARSE_INT` and `Cast(VARCHAR→DOUBLE, USER)` to `Cast(..., Reason.DIALECT)`.
   - The table cannot use the raise map for these. Make them function rows (Row 3) and delete the pass. The kinds'
     membership in `ENGINE_UNCHECKED` is dead for `forms().raise` (r3 trace).
9. **TEXT_NOT_DATE.** MariaDb `:2798` and SQLite `:2541` only. DuckDB and H2 already write a plain cast (`DuckDb.java:615`,
   `H2.java:697`). Nothing to do on the two databases.

### LEGEND-ENGINE (each operation, unguarded)

| Operation | DuckDB | H2 |
|---|---|---|
| shifts | `(cast(%s as BIGINT) << cast(%s as BIGINT))`, `>>` (`DX:199-200`) | `lshift`/`rshift` (`PGM:311-312` → `H2D:660-676`) |
| `rem` | `mod(%s,%s)` (`ED:267`) | `mod` (`PGM:426`) |
| `/` | `((1.0 * %s) / %s)` (`ED:218`) | the Postgres model's divide |
| `sqrt` | `sqrt(%s)` (`ED:287`) | `PGM:328`, `H2D:321` |
| `toOne` | `processNoOp` (`P2S:3133-3137`, `:10793`) | the same |
| slice | `DX:183` | no translation (refused) |
| range | `range(%s, %s, %s)` (`DX:191`) | no translation (refused) |
| `acos`/`asin` | `acos(%s)` (`ED:190`), `asin(%s)` (`ED:195`) | `PGM:329-330`, `H2D:322-323` |

### THE ROW TO ADD

- Move the kind→plain decision into the table, stamp-aware, for example `boolean plain(RaiseKind kind, boolean
  outsideStore)` on `EditionTable`. Have every `forms().raise` caller pass the stamp of the call it lowers:
  - `Scalars.guarded`'s styled overload gains `TypedNativeCall n`;
  - `CastPolicy` and `slice` need a stamp on `TypedCast` and the slice node, or the row ignores the stamp for those two
    kinds (G2).
- Route items 1 and 3 (static empties) now: two lines each.
- Leave items 2, 4, 5, 6 and 7 checked, with a register row (item 4/5 pending the runner).
- No new `SqlFn` or dialect arm.

**S-22 kinds.**
- `SUBSTRING_OUT_OF_RANGE` is moot under the table's substring row.
- `INVERSE_TRIG_NAN`, `NOT_FINITE_NOT_HELD` and `PARSED_NAN_NOT_HELD` are dialect guards on SQLite and MariaDB only
  (`SQLite.java:1414`, `:1904`; `MariaDb.java:1728`, `:2119`). They are never asked on DuckDB or H2.
- See Row 16 for `acos`/`asin`.

**PLACEMENT.** Ruling 20.1 (amendments item 20.1) says lite's checks stay in the Java part. So the raise map must read the
stamp: plain only when `!outsideStore`. Whether legend-engine's Java raises for `5->rem(0)` and `sqrt(-1)`: not asked
(r2 §5.4 Q8).

### THE JUDGES

- **pure/v1 (H2), `core/src/test/resources/edge/pure_v1_answers.tsv`:**
  - `raise.divideByZero` 500 H2 "Division by zero" (line 3);
  - `raise.sqrtOfNegative` 200 `[null]` (line 6);
  - `raise.parseInteger` 500 "Data conversion error converting \"abc\"" (line 7);
  - `raise.toOneOfEmpty` 200 `[null]` (line 8);
  - all must stay. Tests: `PureV1EdgeGoldenTest`, `PureV1SqlTextTest`.
- **PCT engine lane, DuckDB (`LANE/duckdb_essential.tsv`, `duckdb_grammar.tsv`, `duckdb_standard.tsv`).** All
  BOTH_FAIL today; legend-engine's expected text in `MD/EssentialFunctions_manifest.json:304-317`,
  `MD/StandardFunctions_manifest.json:97-102`:
  - `rem::testRemError` (engine "No error was thrown"; lite fails on the unrouted toOne after a plain rem, r2 M4
    bucket 4 #15);
  - `slice::testSliceError`;
  - `range::testRangeStepError`;
  - `testSquareRootError`;
  - `testBitShiftLeft_MoreThan62Bits`;
  - `testBitShiftRight_MoreThan62Bits`.
- **PCT engine lane, H2 (`LANE/h2_essential.tsv`, `h2_standard.tsv`):** `testRemError`, `testSquareRootError` and the
  shifts are BOTH_FAIL. `testBitShiftLeft_UpTo62Bits` is BOTH_FAIL (lite REFUSED, no H2 bit operations).
  `testBitShiftRight_UpTo62Bits` is **LITE_BEHIND** today (engine PASS, lite REFUSED). That is a bucket-5 row
  independent of the table: H2 needs a `bitOp` arm.
- **Corpus:** M3, 0 verdicts move (12 DuckDB / 11 H2 tests plan an unchecked kind).
- **Lane commands:** `//pct:engine_lane_duckdb_test`, `//pct:engine_lane_h2_test` (pins refreshed by
  `bazel run //pct:update_engine_lane`; `pct/BUILD.bazel:1009-1110`).

### RISK

- G2: stamps on non-call raise sites.
- The `CheckedOne` shapes have no legend-engine form.
- The root guards may be legend-engine's own executor check.
- The parse kinds are written by dialects, not by the lowering.
- H2 has no bit operations at all.

---

## Row 2. `toOne` of an empty value (S-5)

**LITE TODAY.**
- Native ids: `Pure.AT_MULTIPLICITY_TO_ONE` (`L/builtin/Pure.java:2990`: `TO_ONE__T_MANY`, `TO_ONE__T_MANY__STRING_1`).
- The rule is `Coercions.java:48-116`. Its arms:
  - aggStrip: the scalar subquery;
  - static empty: an unrouted ERROR, `:63-67`;
  - `CheckedOne`: `:72-78`, `:82-87`;
  - [0..1] value lane: routed, `:101-107`;
  - otherwise the operand flows, `:115`.
- `trustOne` is `Pure.AT_LEGEND_LITE_TRUST_ONE` (`Pure.java:2707`), whose rule is the identity (`Coercions.java:118-120`).
- A3 substitutes it for an always-empty operand: `L/edge/EngineSubstitutes.java` `toOne`; register S384.

**LEGEND-ENGINE.** `processNoOp`, "Ignores the function and only processes the first parameter" (`P2S:3133-3137`; `toOne`
registered `P2S:10793`, `toOneMany` `:10794`). On H2 the Postgres model's `toOne` is `$p->toOne()` (`PGM:470`), and
`ED:295` writes `'%s'`. DuckDB and H2 both answer NULL (golden `raise.toOneOfEmpty`, r2 S-5).

**THE ROW TO ADD.**
- **Not** a function row `toOne → trustOne` for every call: for the `CheckedOne` arms the identity yields a list where a
  scalar is typed.
- Route through the raise map instead, so that SIZE_NOT_MULTIPLICITY is plain when `!outsideStore`:
  - **static empty** `:63-67`: plain = `args.get(0)`;
  - **[0..1]**: done;
  - **`CheckedOne` arms and roots**: stay checked (Row 1, items 2, 4, 5).

  This replaces A3's `trustOne` substitution exactly for the case A3 covers (an always-empty operand), plus the [0..1]
  arm.
- No new native, `SqlFn` or arm.

**PLACEMENT.** A host-side `let x = []->toOne()`: legend-engine's Java raises Pure's error (inferred; S384 not asked for
the run-time empty). Under the stamp, `outsideStore` keeps the check.

**THE JUDGES.**
- pure/v1 `raise.toOneOfEmpty` 200 `{"values":[null]}` (`pure_v1_answers.tsv:8`).
- A3's tests: `core/src/test/java/com/legend/edge/PureMeaningRewriteTest.java`, `PureMeaningEngineTest.java`,
  `EngineSubstitutesTest.java`, and the new `EngineTableShadowTest.java`, which must cover a `toOne` case once routed.
- PCT DuckDB `testRemError` (above) moves only if its toOne is the static-empty or [0..1] shape (not verified which).
- Corpus: M3 0 moves.

**RISK.** The site is routable in two lines. The root guards are open (Row 1, item 4).

---

## Row 3. The parses (S-2, S-7, S-8)

### 3a. `parseInteger`

- **LITE:**
  - id: `Pure.AT_STRING_PARSE_INTEGER` (`Pure.java:3074`);
  - rule: `Scalars.java:2470-2473` → `SqlFn.PARSE_INT`;
  - DuckDB: `DuckDb.java:582-584`, `regexp_full_match(s, '[+-]?[0-9]+')` guard then `CAST(s AS BIGINT)`;
  - H2: `H2.java:663`, `:1318-1322`, `REGEXP_LIKE` guard;
  - edge today: `UnguardedParses` → `Cast(s, BIGINT, Reason.DIALECT)` (`PurePrintForms.java:50-52`).
- **LEGEND-ENGINE:**
  - DuckDB `cast(%s as BIGINT)` (`DX:258`);
  - H2 `castExpression('integer')` (`PGM:402`), 32-bit: `MH/EssentialFunctions_manifest.json:598-599` "Data conversion
    error converting \"9999999999999992\" ... cast('9999999999999992' as integer)".
- **ROW:** a new product native, e.g. `meta::legend::lite::castToInteger(String[1]):Integer[1]`.
  - It is named by what it is: the database's own cast of a text to a 64-bit integer.
  - Its rule: `new SqlExpr.Cast(args.get(0), SqlType.Scalar.BIGINT, SqlExpr.Reason.DIALECT)`, the node
    `UnguardedParses` builds today.
  - Row: `PARSE_INTEGER__STRING_1 → castToInteger` on both databases.
  - Keep BIGINT on H2 (r2 S-2 recommendation: not H2's INTEGER bug).
  - No new `SqlFn`. Cast DIALECT has arms everywhere. Then delete `UnguardedParses`' PARSE_INT half.

### 3b. `parseFloat`

- **LITE:**
  - id: `Pure.AT_STRING_PARSE_FLOAT` (`:3073`);
  - rule: `castFamily(FLOAT, ...)` `Scalars.java:2474`, `:2757-2760` → `Cast(s, DOUBLE, USER)`. It is shared with
    `toFloat` (`:2475`).
  - The guard is the renderer's, keyed on `Cast(VARCHAR→DOUBLE, USER)` (`AnsiSqlRenderer.java:942-946`; DuckDB
    `:1359-1368`; H2 `:1367-1375`).
- **LEGEND-ENGINE:** DuckDB `cast(%s as float)` (`DX:257`), a 4-byte REAL (r2 probe); H2 `castExpression('float')`
  (`PGM:400`).
- **ROW:** a new native `meta::legend::lite::castToFloat(String[1]):Float[1]`, rule `Cast(s, DOUBLE, Reason.DIALECT)`
  (DOUBLE, not REAL). Row: `PARSE_FLOAT__STRING_1 → castToFloat`.
  - This resolves the trace's open question (whether a USER VARCHAR→DOUBLE cast has other sources): the row replaces
    the parseFloat call only, so `toFloat` and `CastPolicy` are untouched.
  - Delete the Cast half of `UnguardedParses`. Check that no corpus query relied on the pass catching a non-parseFloat
    USER cast (r3 "Not verified").

### 3c. `parseDate`

- **LITE:** `Pure.AT_STRING_PARSE_DATE` (`:3071`), rule `Scalars.java:2514-2550` → `Cast(PARSE_DATE(in), TIMESTAMP|DATE,
  USER)`. DuckDB `castAs TIMESTAMP` (`DuckDb.java:615`), H2 the same (`H2.java:697`).
- **LEGEND-ENGINE:**
  - `processParseDate` renames the call to `toTimestamp` with the format `'YYYY-MM-DD HH24:MI:SS'` (`P2S:3465-3470`).
  - **DuckDB has no `toTimestamp` (`DX:294` commented out):** refused, "[unsupported-api] The function 'toTimestamp' ...
    is not supported yet" (`MD/EssentialFunctions_manifest.json:382-392`).
  - **H2:** `cast(parsedatetime(s, 'yyyy-MM-dd HH:mm:ss') as timestamp)` (`PGM:413` → `H2D:482-493`). It fails on every
    PCT input with a `T` or a fraction (`MH/EssentialFunctions_manifest.json:577-587`).
- **ROW:** none. DuckDB is a refusal, so lite answers (ruling 8). H2's fixed-format parse is a different answer, not a
  delegation the user ruled. **Report it to the user** as a new difference (H2 `parseDate` of ISO text: legend-engine
  raises, lite answers). Lite today on H2: LITE_FIXES on `testParseDate`, `testParseDateTypes`, `testParseDateWithZ`;
  BOTH_FAIL on `testParseDateWithTimezone` (`LANE/h2_essential.tsv`).

### 3d. `parseBoolean`

- **LITE:** `castFamily(BOOLEAN, AT_STRING_PARSE_BOOLEAN)` (`Scalars.java:2511`).
- **LEGEND-ENGINE:** DuckDB has no translation (refused: `MD/...:376-381`, LITE_ANSWERS in the lane); H2
  `castExpression('boolean')` (`PGM:403`), which lite already writes (a USER cast). The H2 lane shows SAME.
- **ROW:** none.

### 3e. `parseDecimal`, one argument (S-7)

- **LITE:**
  - ids: `Pure.AT_STRING_PARSE_DECIMAL` (`:3072`, `PARSE_DECIMAL__STRING_1` and the 3-argument overload);
  - rule: `Scalars.java:2484-2510`. The literal branch writes `DECIMAL(38, scale of the literal)` (`:2496-2500`). The
    column branch asks `styled.forms().parsedDecimal(RTRIM(s,'dD'))` (`:2504-2509`).
  - product: `Cast(text, PureSql.type(DECIMAL)=Decimal(38,18), USER)` (`ProductLoweringForms.java:47-51`);
  - edge, every door: `Decimal(5,2)` (`EdgeLoweringForms.java:91-94`).
- **LEGEND-ENGINE:**
  - DuckDB: `transformParseDecimal` → `cast(%s as decimal)` for one argument (`DX:256` → `ED:721-727`), DuckDB's
    `DECIMAL(18,3)`. Witness: `MD/EssentialFunctions_manifest.json:397-398` "Could not convert string \"3.14159d\" to
    DECIMAL(18,3) ... select cast('3.14159d' as decimal)".
  - H2: `castExpression('decimal(5,2)')` (`PGM:401`). Witness: `MH/...:592-593` "select cast('3.14159d' as
    decimal(5,2))".
  - legend-engine never strips the `d` suffix. Lite does: an answer where legend-engine raises.
- **ROW, per database (G1):**
  - **H2:** `PARSE_DECIMAL__STRING_1 →` a new native `meta::legend::lite::parseDecimalFivePointTwo(String[1]):Decimal[1]`
    (or a better name for "a text cast to DECIMAL(5,2)"). Rule: `Cast(RTRIM(s,'dD'), Decimal(5,2), USER)`, the edge
    form's node.
  - **DuckDB:** `→` a new native `meta::legend::lite::castToDefaultDecimal(String[1]):Decimal[1]`, "the database's
    DECIMAL with no precision written". This **needs a new node**: lite has no bare-DECIMAL type
    (`SqlType.Decimal(p,s)` always writes both; `L/sql/SqlType.java`). Two options:
    - (a) a new `SqlFn.DEFAULT_DECIMAL` with one operand. It needs a render arm in every dialect's call switch: DuckDB
      `CAST(x AS DECIMAL)`, H2 `CAST(x AS DECIMAL)`, and a `DialectCapability` refusal in the other dialects until
      measured. It also needs `SqlTyping` to type it (DuckDB (18,3); H2 unmeasured on 2.5.252). **Recommended.**
    - (b) a new `SqlType.Scalar.DECIMAL_DEFAULT`. Every type-name switch (`castTypeName` in each dialect) gets an arm.
      More arms.

    The same node serves Row 8 (`toDecimal`).
  - Map only the one-argument id: the 3-argument form on H2 is a ruled machinery bug (`PGM:401` drops p and s).
  - **The row replaces the literal branch too** (legend-engine casts a literal the same way). Today's edge form reaches
    the column branch only, so PCT's literal cases will move (below).
- **PLACEMENT:** not asked.
- **JUDGES:**
  - PCT DuckDB `parseDecimal::testParseDecimal` (LITE_FIXES; lite strips `d` and passes, so it stays) and `testParseZero`
    (LITE_FIXES; lite `0.000` passes by value, S139).
  - PCT H2 `testParseDecimal`: lite would now write `CAST('3.14159' AS DECIMAL(5,2))` = 3.14, failing. LITE_FIXES →
    BOTH_FAIL. `testParseZero` may stay.
  - `testParseDecimalWithPrecisionScale` (3-argument) stays LITE_FIXES on both databases.
  - Corpus: 2 per database (DIFFERENCES §6, H5 task 0).
  - `EdgeAuditOpenTest` (pins the edge's form).

### 3f. `parseJSON` leniency (S-8)

- **LITE:** `Pure.AT_JSON_PARSE_JSON` (`Pure.java:2646`), rule `JsonLane.java:33-35` → `forms().parsedJson`. Product
  `Cast(x, JSON, CARRIER)` (`ProductLoweringForms.java:54-57`). Edge, every door, on every database:
  `Cast(REGEXP_REPLACE(x,'\}\s*\{','},{','g'), JSON)` (`EdgeLoweringForms.java:99-104`).
- **LEGEND-ENGINE (corrects r2 S-8):**
  - DuckDB writes the **strict** `cast(%s as JSON)` (`DX:259`; `P2S:4705` `processDynaFunction('parseJson', ...)`).
  - H2 writes `legend_h2_extension_json_parse(x)` (`PGM:471` → `H2D:422`). That is a Java UDF which reads the text
    with Jackson **as a Map** (`LH2:116-135`): strict JSON, an object only (an array raises "Unable to parse json as a
    Map").
  - **Neither database part has the `'}{'` tolerance.** S366's tolerance (`Json.parseOne` lenient) must come from
    legend-engine's Pure-level JSON parser: the harness or Java part. Where exactly: not verified.
- **ROW:**
  - DuckDB: none. The product's strict cast is legend-engine's.
  - H2: a native for "parse a JSON object, refusing anything else" would be exact, but it is a new form for a rare case.
  - The regex form belongs to wherever S366's corpus test actually parses (the harness, i.e. `pure`, or the Java part).
- **Recommendation:** remove `parsedJson` from the database part of `engine`. Re-read S366's corpus test
  (`testSubTypeAtRootLevelWithInheritanceMapping`) to see which part parses. Bring the correction to the user.
- **JUDGES:** corpus "1 lost on DuckDB, 1 gained on H2" (DIFFERENCES §6). There is no PCT test.
- **RISK:** an S row was misread as a delegation.

---

## Row 4. `isNumeric` (S-6)

**LITE TODAY.**
- id: `Pure.AT_LEGEND_LITE_IS_NUMERIC` (`Pure.java:2676`), the `###Relational` dynaFunction's product native.
- rule: `Scalars.java:2614-2617` → `styled.forms().isNumeric`.
- Product: `SqlFn.IS_NUMERIC` (`ProductLoweringForms.java:66-70`), rendered as the dialect's full match of
  `SqlFn.NUMERIC_TEXT` (`AnsiSqlRenderer.java:1165-1168`; DuckDB `:535`, H2 `:584`).
- Edge, every door: `LOWER(s) = UPPER(s)` (`EdgeLoweringForms.java:113-119`).

**LEGEND-ENGINE.**
- **DuckDB:** `(lower(%s) = upper(%s))` (`DX:238`).
- **H2 (4.145.0): a regular expression**, not lower=upper. The Postgres model writes
  `regexp_full_match(s, '[-]?[0-9]+(\.[0-9]+)?')` (`PGM:367`). `H2D:398` renders `RegexpFullMatch` with the defaults'
  processor (`SDD:967-971`), as `s ~ '^(?:[-]?[0-9]+(\.[0-9]+)?)$'` (`SDD:894-897`, operator `'~'`).
- **S370's H2 claim (`lower = upper`) is stale for 4.145.0.**
- Not verified: that H2 (legend-engine's patched 2.1.214) accepts the `~` operator. If it does not, legend-engine's H2
  `isNumeric` raises, which would make the row a refusal. Ask the engine runner (r2 §5.4 Q1).

**THE ROW TO ADD (per database, G1).** Two product natives, each built from existing nodes:
- **DuckDB:** `meta::legend::lite::hasNoLetterCase(String[0..1]):Boolean[0..1]`, rule `EQUAL(LOWER(s), UPPER(s))`. These
  are the edge form's nodes, with arms in every dialect (`DuckDb.java:648-649`, `H2.java:727-728`).
- **H2:** `meta::legend::lite::isPlainDecimalText(String[0..1]):Boolean[0..1]`, rule `REGEXP_FULL_MATCH(s,
  '[-]?[0-9]+([.][0-9]+)?')`. Write `[.]` for `\.`: `SqlFn.NUMERIC_TEXT`'s javadoc (`SqlFn.java:229-231`) forbids
  backslashes in such literals. `H2.java:634` already spells `REGEXP_FULL_MATCH`.
- No new `SqlFn`. Then delete `LoweringForms.isNumeric` and its two implementations.

**PLACEMENT.** Always inside a store query (a `###Relational` dynaFunction in mapping code): no Java column.

**THE JUDGES.**
- `core/src/test/java/com/legend/planner/IsNumericProbeTest.java` is the product's. It pins the RULE answers for "1",
  "-1.5", "1e3", " 1 ", "abc" and "" on every database, and must not change.
- An edge test of the two engine forms is needed. None exists (r3 R8 lists none beyond the probe).
- Corpus: 0 (DIFFERENCES §6). PCT: none.

**RISK.** H2's `~`. Lite's H2 2.5.252 regex dialect is Java's: `[.]` behaves the same.

---

## Row 5. `round` (half away from zero)

**LITE TODAY.**
- ids: `Pure.AT_MATH_ROUND` (`Pure.java:2958`: `ROUND__DECIMAL_1__INTEGER_1`, `ROUND__FLOAT_1__INTEGER_1`,
  `ROUND__NUMBER_1`).
- rule: `Scalars.java:1364-1366` → `DecimalKindRules.round` (`DecimalKindRules.java:24-31`):
  - one argument: `Cast(Call(SqlFn.ROUND, x), BIGINT, OPERATOR)`;
  - two arguments: `ROUND(x, n)`, a Decimal re-cast by `decimalScaleCast`.
- `SqlFn.ROUND` is half-even. DuckDB `roundHalfEven` (`DuckDb.java:636`, `:1082-1100+`); H2 `roundHalfEven`
  (`H2.java:715`, `:1247-1256`).

**LEGEND-ENGINE.**
- **DuckDB:** `transformRoundForDuckDB` (`DX:270`, `:864-870`): `cast(round(%s, 0) as BIGINT)` for one argument,
  `round(%s, %s)` for two.
- **H2:** `PGM:410`: one argument `castExpression(functionCall('round', $p), 'bigint')`, else `round(x, n)` (`H2D:336`).
- Both databases round half away from zero (r2 probe 16.5→17, -16.5→-17; `MD/...:310-314`, `MH/...:499-503`).

**THE ROW TO ADD.**
- `SqlFn.ROUND_HALF_UP` already means "the database's ROUND" on DuckDB (`DuckDb.java:617`, `writer.function("ROUND")`)
  and H2 (`H2.java:699`). Elsewhere it is composed: Postgres `:910`, `:1130-1141`; BigQuery, ClickHouse, MariaDb,
  SQLite, Snowflake, Trino, Mongo. Databricks is `ROUND` (`:1105`).
- New natives `meta::legend::lite::roundHalfUp` with the three overloads of `round`, in the same order (`EngineTable`
  pairs overloads by position). Rule: `DecimalKindRules.round` with `SqlFn.ROUND_HALF_UP` in place of `ROUND`. The
  `alreadyWhole` check covers it (`AnsiSqlRenderer.java:930-940`).
- No new `SqlFn` or arm.
- **The row only on DuckDB and H2 (G1).** On Postgres, legend-engine's own `round` on a double is half-even (third
  review, addendum B). Elsewhere it was never read: refuse, as A3 does.

**PLACEMENT.** legend-engine's Java `round` of a `let`: not asked (third review A). Under the stamp, `outsideStore` → no
row.

**THE JUDGES.**
- PCT engine lane, DuckDB and H2: `round::testNegativeFloatRoundHalfEvenUp` and `round::testPositiveFloatRoundHalfEvenDown`
  move LITE_FIXES → BOTH_FAIL (lite answers -17/17, legend-engine's).
- `testNegativeFloatRoundHalfEvenDown` and `testPositiveFloatRoundHalfEvenUp` stay SAME (`LANE/duckdb_essential.tsv`,
  `h2_essential.tsv`).
- Corpus: not measured.

**RISK.** Low. The only per-database fact is "DuckDB and H2 only".

---

## Row 6. `dateDiff` in weeks

**LITE TODAY.**
- id: `Pure.AT_DATE_DATE_DIFF` (`Pure.java:2851`; the row maps `DATE_DIFF__DATE_1__DATE_1__DURATION_UNIT_1` only).
- rule: `Scalars.java:787-791` → `dateDiffExpr(diffPart(unit), d1, d2)` (`:3368-3403`, `diffPart` `:3326-3341`).
  - WEEK is Pure's Sunday-index composition (`:3370-3381`).
  - HOUR, MINUTE and SECOND are elapsed time (`:3382-3390`).
  - Everything else is the bare `Call(SqlFn.DATE_DIFF, [DatePartLit(part), d1, d2])` (`:3398-3401`).
- DuckDB: `date_diff('week', a, b)` (`DuckDb.java:703-704`; `DuckDbSpellings.java:72`). H2: `DATEDIFF(WEEK, a, b)`
  (`H2.java:587`, `:864-869`, `:873-890`).

**LEGEND-ENGINE.**
- **DuckDB:** `datediff('%s',%s,%s)` with `'week'` (`DX:213`, `:478-483`). Sat→Sun = 0 (`MD/...:181-182`, "expected: 1
  actual: 0").
- **H2:** `datediff(<unit>, a, b)`, the unit spelled `week` (`PGM:397`, `:734-740` → `H2D:410`, `:549-553`; unit map
  SDD-side `sqlDialectExtensionDefaults.pure:392-405`). Sun→Sat = −1 (`MH/...:358-359`, "expected: 0 actual: -1").

**THE ROW TO ADD.**
- The database's own `date_diff` is the existing default arm. A new native `meta::legend::lite::sqlDateDiff(Date[1],
  Date[1], DurationUnit[1]):Integer[1]`, following the `sqlSubstring`/`sqlPosition` precedent (`Pure.java:489`,
  `:1227-1230`). Rule: `new SqlExpr.Call(SqlFn.DATE_DIFF, [DatePartLit(diffPart(unit)), d1, d2])`.
- No new `SqlFn` or arm.
- **Decision for the user:** legend-engine writes the database's `datediff` for **every** unit. Its HOURS, MINUTES and
  SECONDS count boundaries where lite counts elapsed time; that answer was not ruled. The PCT `testDateDiffHours`,
  `testDateDiffMinutes` and `testDateDiffSeconds` pass on both lite and legend-engine today. Either:
  - the native is the database's `date_diff` for every unit (legend-engine exactly, beyond the ruling); or
  - its rule takes WEEK bare and every other unit through `dateDiffExpr` (within the ruling, but a hybrid native).
- The table is keyed by function id, not by the unit's value, so the choice is inside the native's rule.

**PLACEMENT.** Not asked.

**THE JUDGES.**
- PCT DuckDB and H2 `testDateDiffWeeks`: LITE_FIXES → BOTH_FAIL (lite 0 on DuckDB, −1 on H2).
- The other `testDateDiff*` tests must stay SAME (`LANE/*_essential.tsv`).

**RISK.**
- H2's week rule on 2.5.252 versus legend-engine's patched 2.1.214 is not measured.
- The unit-scope decision above.

---

## Row 7. `splitPart` with an empty token

**LITE TODAY.**
- id: `Pure.AT_STRING_SPLIT_PART` (`Pure.java:3088`).
- rule: `Scalars.java:2216-2228`:
  - an empty literal token answers the whole text at index 0, else NULL;
  - otherwise `SqlFn.PURE_SPLIT_PART(s, t, plusOne(i))`.
- DuckDB `splitPartCall`: `list_extract(list_filter(string_split(s, t), x -> x <> ''), p)` (`DuckDb.java:638`,
  `:1385-1393`).
- H2 `splitPartCall`: `REGEXP_SUBSTR(s, '[^<chars>]+', 1, p)`, refusing an empty or non-literal separator (`H2.java:717`,
  `:1536-1547`).

**LEGEND-ENGINE.**
- **DuckDB:** `split_part(%s, %s, %s)` (`DX:276`). The index is converted on purpose ("sql is 1 index based, pure is 0
  index based", `P2S:3197-3204`, `zeroToOneBasedIndex` `:3206-3215`). `'Hello World'->splitPart('', 0)` = `'H'`
  (`MD/UnclassifiedFunctions_manifest.json:25-26`).
- **H2:** `legend_h2_extension_split_part` (`H2D:388`), a Java UDF on `StringUtils.split(string, token)` (`LH2:201-217`):
  - the token's characters are separators and empty parts are dropped, which is Pure's `split` (`splitPart.pure:17-22`
    in `ENG/legend-engine-core/.../core_functions_unclassified/string/split/splitPart.pure`);
  - an empty token splits nothing, so H2 gives Pure's answer. H2 needs no row.

**THE ROW TO ADD (DuckDB only, G1).**
- A new native `meta::legend::lite::sqlSplitPart(String[0..1], String[1], Integer[1]):String[0..1]`. Rule:
  `Call(SqlFn.SPLIT_PART, s, t, plusOne(i))`. `SqlFn.SPLIT_PART` already renders as `split_part` on DuckDB
  (`DuckDb.java:690`).
- **This is more than the empty token.** DuckDB's `split_part` takes the token as one string and keeps empty parts.
  Pure's takes each character as a separator and drops empties. So `'a,,b'->splitPart(',', 1)` is `''` on legend-engine
  and `'b'` in Pure. The classification's "the remaining difference is DuckDB's empty delimiter"
  (`wrong-answers-classified.md` §1 n) is incomplete. **Post it to the user** before landing: the ruling named the empty
  token only.
- Lite's out-of-range NULL already equals the database's NULL. Pure raises (`at`): third review E, "dropping the guard
  also drops Pure's out-of-range raise".

**PLACEMENT.** Not asked.

**THE JUDGES.**
- PCT DuckDB `splitPart::testSplitPartEmptyToken`: LITE_FIXES → BOTH_FAIL (`LANE/duckdb_unclassified.tsv`).
- `testSplitPart`, `testSplitPartEmptyString`, `testSplitPartTypicalToken` and `testSplitPartWithNoSplit` must stay SAME.
  The typical token `', '` gives the same answer both ways on its inputs (`splitPart.pure:50-57`).
- The H2 lane has no Unclassified suite (`LANE/` lists `h2_essential`, `h2_relation`, `h2_standard` only).

**RISK.** The semantics are wider than ruled. The row is DuckDB-only.

---

## Row 8. `toDecimal`

**LITE TODAY.**
- id: `Pure.AT_MATH_TO_DECIMAL` (`Pure.java:2970`, `TO_DECIMAL__NUMBER_1`).
- rule: `Scalars.java:1201-1204` → `DecimalKindRules.toDecimal` (`DecimalKindRules.java:79-92`):
  - an integer literal or column → `CAST AS DECIMAL(38,0)`;
  - a Decimal literal as is;
  - a Float literal → DecimalLit;
  - otherwise `CAST AS DECIMAL(38,18)`.

**LEGEND-ENGINE.**
- DuckDB: `cast(%s as decimal)` (`DX:282`), (18,3).
- H2: `castExpression('decimal')` (`PGM:406`). Bare DECIMAL on 2.1.214 has scale 0: `3.8` → `4`
  (`MH/...:508-512`; `wrong-answers-classified.md` §7 probe). Ruled match (amendments item 22).

**THE ROW TO ADD.** A new native `meta::legend::lite::toDefaultDecimal(Number[1]):Decimal[1]` over the Row 3e node
`SqlFn.DEFAULT_DECIMAL`. That is one node for "the database's DECIMAL with no precision written", with arms DuckDB
`CAST(x AS DECIMAL)`, H2 `CAST(x AS DECIMAL)`, and a refusal elsewhere. Row on DuckDB and H2.

**PLACEMENT.** Not asked.

**THE JUDGES.**
- PCT DuckDB `toDecimal::testIntToDecimal` (LITE_FIXES).
  - Lite's DuckDB `8.000` against Pure's `8D`: lite's judge compares Decimals by value (S139), so lite may still PASS,
    and the verdict would stay LITE_FIXES.
  - Not verified: the lite judge's Decimal comparison on this lane.
- PCT DuckDB `testDecimalToDecimal` and `testDoubleToDecimal`: SAME (3.800 = 3.8 by value).
- PCT H2 `testDecimalToDecimal` and `testDoubleToDecimal`: LITE_FIXES → BOTH_FAIL (4 vs 3.8D). H2 `testIntToDecimal`
  stays SAME.

**RISK.**
- **Lite runs H2 2.5.252.** Bare DECIMAL's scale there is not measured: the third review E says "probed on 2.1.214
  only". If 2.5.252 keeps 3.8, lite cannot reproduce legend-engine's `4` with the database's own cast. It would need
  `DECIMAL(p,0)`, which is no longer "the database's default". Measure before writing the H2 arm.
- `SqlTyping` needs a type for the new node.

---

## Row 9. `rem` and `mod`

**LITE TODAY.**
- `rem` (`Pure.AT_MATH_REM`, `Pure.java:2957`): `Scalars.java:466-482`, `SqlFn.REM` → `modTruncated`
  `MOD(x, y)` (`AnsiSqlRenderer.java:1464-1466`; DuckDB `:576`, H2 `:657`), guarded for a zero divisor.
- `mod` (`AT_MATH_MOD`, `:2950`): `family(SqlFn.MOD, ...)` `Scalars.java:464` → `modFloored`
  `MOD(MOD(a,b)+b,b)` (`AnsiSqlRenderer.java:1458-1461`; DuckDB `:575`, H2 `:656`).

**LEGEND-ENGINE.**
- `rem`: DuckDB `mod(%s,%s)` (`ED:267`), H2 `mod` (`PGM:426`).
- `mod`: DuckDB `cast(fmod(%s,%s) as integer)` (`DX:249`); H2 plain `mod` (`PGM:314`), which truncates. H2's negative
  `mod` is ruled a machinery bug, not reproduced.

**THE ROW TO ADD. None.**
- Lite already writes `MOD(x, y)` for `rem` on both databases. The only difference is the zero-divisor guard, which is
  Row 1's.
- H2's `mod(7.5, 5) = 3` comes from H2 typing MOD as its divisor's type. Lite's H2 `testRemWithMixedIntegersAndFloats` is
  **already BOTH_FAIL with WRONG_ANSWER** (`LANE/h2_essential.tsv`): lite already gives H2's answer, or a different
  wrong one (not verified which).
- This confirms the third review E: "lite already writes plain `MOD` (`H2.java:657`) ... unverified".

**THE JUDGES.**
- DuckDB `rem::testRemWithMixedIntegersAndFloats` SAME; `mod::testModWithNegativeNumbers` SAME;
  `binFloor::testBinFloorNegative` SAME.
- H2 `testModWithNegativeNumbers` LITE_FIXES (stays, ruled).

**RISK.** None for the table.

---

## Row 10. A DateTime's and a Boolean's `toString` used further (S-16, P-14)

**LITE TODAY.**
- ids: `Pure.AT_STRING_TO_STRING` (`Pure.java:3095`) and `AT_RELATION_TO_STRING`. Rule: `Scalars.java:2420-2446`.
- **DateTime:** `dateTimeOf(styled, ...)` (`:3698-3700`) → `styled.dateTimes().dateTimeText`.
  - The product writes `DATETIME_TO_TEXT` (`Styled.java:33-34`), ISO with Z.
  - The edge writes legend-pure's print at legend-engine's door too: the literal half `EdgeDateTimeTexts.java:24-27`,
    the computed half the pass `PurePrintForms.java:168-171`.
- **Boolean:** `pureToString` → `SqlFn.BOOL_TO_TEXT` (`Scalars.java:3047`).
  - DuckDB `CAST AS VARCHAR` = `'true'` (`DuckDb.java:620`).
  - H2 `CASE WHEN x THEN 'true' WHEN NOT x THEN 'false' END` (`H2.java:629-630`).
  - Any H2 `Cast(boolean → VARCHAR)` is rewritten to that CASE whatever its reason (`H2.java:1404-1411`).

**LEGEND-ENGINE.**
- DuckDB `cast(%s as varchar)` (`DX:284`): `'2014-01-01 00:00:00'`, `'2014-01-02 01:54:27.352'` (`MD/...:415-440`).
- H2 `castExpression('varchar')` (`PGM:404`): the same DateTime texts, and `'TRUE'` (`MH/...:607-608`, `:616-620`,
  `:643-644`).

**THE ROW TO ADD.**
- **A new node is needed for H2's Boolean**, because H2 rewrites every boolean→VARCHAR cast. Proposal: one native for
  "a value's text as the database's own cast to text writes it", `meta::legend::lite::castToText(Any[1]):String[1]`.
- Its rule is a new `SqlFn.CAST_TEXT` with one operand. The dialect arms are the plain `CAST(x AS VARCHAR)`, with no
  Boolean rewrite and no literal re-spelling:
  - DuckDB `castAs(x, "VARCHAR")`;
  - H2 `castAs(x, "VARCHAR")`;
  - every other dialect: its own cast, or a `DialectCapability` refusal where legend-engine's answer was never read.
- `SqlFn.FLOAT_FULL_TEXT` (`SqlFn.java:176-186`; DuckDB `:624`, H2 `:703`) is already "a Float's text as the database
  itself writes it". The new node generalizes it, or the Float case keeps `FLOAT_FULL_TEXT` (Row 11).
- **Keying:** one `toString` id serves every type, so the table maps `TO_STRING__ANY_1 → castToText` and the native's
  rule picks by the operand's static type (invariant 2 allows reading the annotation):
  - DateTime and Boolean → `CAST_TEXT`;
  - Float → Row 11;
  - String, Integer and StrictDate → the product's own (legend-engine's cast gives the same text: not verified per
    type);
  - enumerations, classes, lists and Variants → the product's rule, since legend-engine refuses or does them in Java
    (`MD/...:409-433`).

  The table cannot express "only for DateTime", so the rule must delegate to `pureToString` for the other types.

**PLACEMENT.**
- At a last step, rule zero says presentation; the SQL form gives the same text, so it is harmless but not lean.
- In legend-engine's Java part a DateTime's text is Pure's print: not asked (B2's question, design v3 §10.2).
- The stamp handles it: `outsideStore` → no row.

**THE JUDGES.**
- PCT DuckDB `toString::testDateTimeToString`, `testDateTimeWithTimezoneToString` and `testSimpleDateToString`:
  LITE_FIXES → BOTH_FAIL.
- PCT H2: the same three, plus `testBooleanToString` LITE_FIXES → BOTH_FAIL.
- DuckDB `testBooleanToString` stays SAME.
- Corpus: within M7's 77 text tests (the DateTime share is not split out).
- `FloatTextTest` and `JoinedTextTest` (r3 R8) pin answers under UPSTREAM and should not move under `engine`'s
  database part.

**RISK.** One new `SqlFn`, with an arm in each of the 11 dialects. The per-type delegation inside the rule. The
`ProductRelationText` forced-product cells (r3 §C) are not affected, because they never reach `Scalars.lower`'s table.

---

## Row 11. A Float's `toString` used further (S-15)

**LITE TODAY.** `Scalars.java:2442-2444` and `:2945-2947` → `floatRepr` (`:3531-3533`) → `LiteralSpelling.floatPrint` →
`SqlFn.FLOAT_TO_TEXT`. The product's text has six decimals. The edge pass re-spells it to `CAST(x AS VARCHAR)`; a
FloatLit becomes a DecimalLit, and on H2 `CAST(<lit> AS DOUBLE)` (`PurePrintForms.java:77-120`, `:165-167`).

**LEGEND-ENGINE.**
- `cast(%s as varchar)` (`DX:284`, `PGM:404`).
- A literal operand is printed by the literal processor first. On DuckDB that is the default processor (`DX:23`; Float
  not overridden in `DX:133-141`), so the literal is a DECIMAL and the cast prints it positionally. So DuckDB **passes**
  `testFloatToStringWith{Negative,Positive}Exponent` (`LANE/duckdb_essential.tsv`: SAME). H2 prints `1.3421E-8`
  (`MH/...:628-631`; lane: BOTH_FAIL).

**THE ROW TO ADD.**
- Inside the Row 10 native: a Float operand → `SqlFn.FLOAT_FULL_TEXT` (exists; arms on every dialect, `SqlFn.java:176-186`).
- **A FloatLit operand needs the literal-processor form** to match DuckDB: `CAST(<decimal literal> AS VARCHAR)`. That is
  a dialect choice (DuckDB's literal is a DECIMAL), so it belongs in the DuckDB `FLOAT_FULL_TEXT` or `CAST_TEXT` arm when
  its operand is a `FloatLit`. It cannot be decided at lowering: invariant 2 forbids the lowering to know the dialect.
- Without it the risk is bucket 5: the third review D says DuckDB passes because of the DECIMAL literal, and with the
  literal deleted (ruling 11) lite would print `1.3421e-08`.

**PLACEMENT.** Not asked (B2's question).

**THE JUDGES.**
- PCT DuckDB `testFloatToString`, `testFloatToStringWithExcessTrailingZeros`, `WithNegativeExponent`, `WithNoLeadingZero`
  and `WithPositiveExponent` must stay SAME.
- PCT H2 `WithNegativeExponent` and `WithPositiveExponent` stay BOTH_FAIL (lite then gives H2's own `1.3421E-8`).
- Corpus: 2 per lane (the validation showcase `1000000` vs `1000000.0`).
- `FloatTextTest`.

**RISK.** The literal case per dialect, and the interaction with step 1's deleted DECIMAL literal. **Measure on the
DuckDB lane before landing.**

---

## Row 12. `substring`, `substr`, `indexOf` (S-3, S-4): built, confirmed

**LITE.**
- Natives:
  - `Lite.SUBSTRING_AS_GIVEN` (`Pure.java:473`, signatures `:1036-1037`, ids `AT_LEGEND_LITE_SUBSTRING_AS_GIVEN` `:2703`);
  - `Lite.SUBSTR_AS_GIVEN` (`:484`, `:1042-1043`, `:2702`);
  - `Lite.SQL_POSITION` (`:489`, `:1230`, `:2699`).
- Rules:
  - `substringAsGiven`: `Scalars.java:1508-1510` → `SqlFn.SUBSTRING_AS_GIVEN`;
  - `substrAsGiven`: `:1516-1526` → `SUBSTRING_AS_GIVEN(s, start+1[, end-start])`;
  - `sqlPosition`: `:1502-1504` → `SqlFn.STRPOS`.
- Arms:
  - DuckDB `substr(...)` (`DuckDb.java:677`), `strpos` (`:678`);
  - H2 `substr(...)` (`H2.java:758`), `LOCATE(sub, s)` (`:614`).
- The table rows exist: `L/edge/EngineTable.java`, with the flag choosing `substrAsGiven`.
- Only the two-argument String `indexOf` is mapped, which is right. Pure's `indexOf` rule (`Scalars.java:1535`) is shared
  with `AT_COLLECTION_INDEX_OF`, and legend-engine refuses the three-argument form
  (`MD/EssentialFunctions_manifest.json:367-368`).

**LEGEND-ENGINE.**
- DuckDB `substring%s` over the arguments as given (`DX:277`); `instr(%s, %s)` (`DX:237`).
- H2 `substring` (`PGM:418`), `strpos` → `locate` (`PGM:381`, `H2D:383`).
- `processSubstring` and `processSubstr` (`P2S:4652-4666`, `correctSubstrIndices` `:4676-4693`).

**Confirmed equivalent:**
- DuckDB `substr` ≡ `substring` and `strpos` ≡ `instr` (aliases);
- H2 `SUBSTR` ≡ `SUBSTRING`, `LOCATE(sub, s)` = legend-engine's `locate`.

**THE JUDGES.**
- PCT DuckDB and H2 `substring::testStart`, `testStartEnd`, `indexOf::testSimple` and `indexof::testIndexOfOneElement`:
  LITE_FIXES → BOTH_FAIL once PCT's engine route hands `EngineTable`.
- `EngineTableShadowTest` against `PureMeaningRewriteTest`, `PureMeaningEngineTest`, `PureMeaningCheckTest` and
  `PureMeaningPostgresTest`.
- pure/v1 goldens (R1/R2).

**RISK.** The flag key is already handled in `EngineTable.lowersAs`. Postgres and the other databases: A3 refuses them
(`PureMeaningRewrite.java:101`). `EngineTable` has no database (G1), so it would apply everywhere.

---

## Row 13. An enumeration compared with another enumeration's value (by name)

**LITE TODAY.**
- **Not a `Scalars.lower` row.**
- The typer marks it `TypedCrossTypeComparison`. The planner refuses the marker.
- At legend-engine's door, `EngineComparisons.asTheDatabases` (`L/edge/EngineComparisons.java:22-31`) unwraps it, also
  on PCT's engine route (r3 trace, `PctPlanRoute.java:146`).
- Then G½'s `LiteralUnroll.equalityFold` → `neverEqual` (`L/compiler/spec/LiteralUnroll.java:389-438`) folds two spelled
  values of different kinds, one an enumeration, to `false` on every door, after the door (third review 5a).

**LEGEND-ENGINE (mechanism now read, not inferred).** `processEqualsForEnum`: where both sides are enumeration values,
`^DynaFunction(name = 'equal', parameters = [...]->map(v|^Literal(value=$v.processedParam->cast(@Literal).value->toString())))`.
That is the two **names** compared as literals (`P2S:8529-8534`, chosen at `P2S:8410-8414`). PCT: `testEqEnum`,
`testEqualEnum` "Assert failed" (`MD/GrammarFunctions_manifest.json:13`, `:33`; `MH/GrammarFunctions_manifest.json:21-22`,
`:41-42`).

**THE ROW TO ADD.**
- No table row. Once the marker is unwrapped and the fold is told not to fold a cross-kind pair, the lowering compares
  the two enumeration values as lowered.
- Whether a spelled enumeration value lowers as its name StringLit: inferred from `EnumSourceValues` (cited in
  `EngineComparisons.java:12-14`), not verified.
- The work is amendment 25's step-5 change, "a fold never answers a comparison across types", applied to `neverEqual`.

**PLACEMENT.** S169: legend-engine's SQL and its Java disagree (design v3 §9 M10). The Java part keeps Pure's `false`. It
needs the stamp on the comparison, or the fold kept for `outsideStore` calls.

**THE JUDGES.**
- PCT DuckDB `eq::testEqEnum` and `equal::testEqualEnum`: LITE_FIXES → BOTH_FAIL (`LANE/duckdb_grammar.tsv`). The H2
  lane has no Grammar suite.
- `PureComparisonsTest`; the `LiteralUnrollLedgerTest` noted by the third review.

**RISK.**
- MEDIUM.
- S169 for the product (third review 5b, decision D3).
- `TypedCrossTypeComparison` is not a native call, so the stamp must reach it.

---

## Row 14. A count over a declared key (S-13)

**LITE TODAY.**
- `KeyedCounts.fold` runs unconditionally at `Lowerer.java:534`: `SubselectPrune.prune(KeyedCounts.fold(q, tableKeys),
  true)`. Class: `L/lowering/KeyedCounts.java`.
- It folds `COUNT(*) OVER (PARTITION BY cols)` to 1 where the partition covers a declared key.
- **The edge's two-form choice was deleted at `3ca2afd2` (E0c).** It folded too, so it was dead. There is now no
  carrier at all.

**LEGEND-ENGINE.** Counts the rows the data holds (S147). It trusts no key.

**THE ROW TO ADD.**
- Not a function row: it is a fold over the SQL tree (G3).
- Options:
  - (a) an `EditionTable` method `boolean trustsDeclaredKeys()` that `Lowerer.java:534` asks;
  - (b) the user's decision 4 of the third review: is a declared key "a fact fixed by the model"? If yes, the row is
    recorded, not built.

**THE JUDGES.** None known: the corpus's data keeps its keys (r2 S-13). The lean-SQL pins
(`core/src/test/resources/lean-sql/baseline.tsv`, `LeanSqlBaselineTest`) would grow at legend-engine's door only.

**RISK.** It needs a carrier that was just deleted. It is cheap once one exists.

---

## Row 15. `LEGACY_SQL_NULL_UNSAFE_EQUALS` (S-12)

**LITE TODAY.**
- `Lowerer.withFeatures` sets `legacyNullUnsafeEquals` (`Lowerer.java:229-235`).
- `NullSemantics.verbatim` turns `NULL_SAFE_EQUAL` into `EQUAL` (`NullSemantics.java:101-105`) at the six wrappers
  `Lowerer.java:2270`, `:2294`, `:3480`, `:4303-4304`, `:4311`, `:4459`.
- It applies on every door, `lite` included.

**LEGEND-ENGINE.** The flag is read at `P2S:368`. `equal` becomes `legacyNullUnsafeEqualsOperation` (`P2S:8414-8416`,
`:8465-8478`): plain `=`, with FreeMarker selectors for optional parameters. Without the flag it is `nullSafeEqual`
(`P2S:8460-8462`).

**THE ROW TO ADD.**
- **Option A (as v3 says):** keep the mechanism and refuse the flag by name in `lite` (T-4). That is a product move.
- **Option B (table):** `EngineTable.lowersAs` already receives `features`. Under the flag, map `equal`, `eq` and `==` to
  a new native `meta::legend::lite::equalAnsi`, the twin of the existing `notEqualAnsi` (`Pure.java:580`, `:2122`; rule
  `Scalars.java:1208-1211`), whose rule is `SqlFn.EQUAL`. Then `legacyNullUnsafeEquals` leaves the Lowerer.
- **Caveat:** `NullSemantics.verbatim` rewrites any rule's `NULL_SAFE_EQUAL` output, not only `equal`'s. Whether other
  rules emit `NULL_SAFE_EQUAL` (for example `in` or `!=`) was not traced. Option B is exact only if `equal` is the sole
  emitter.

**THE JUDGES.** 7 corpus tests set a flag; 1 needs this one and checks SQL text only (DIFFERENCES §8, H8 (a.3)). No test
names the flag (r3 trace).

**RISK.** Low. Untraced emitters for option B.

---

## Row 16. `acos`/`asin` out of range, `sqrt` of a negative, shifts

**LITE TODAY.**
- **`acos`/`asin`:**
  - `Scalars.java:561-578` maps `AT_MATH_ACOS` and `AT_MATH_ASIN` (`Pure.java:2918-2919`) to `SqlFn.ACOS` and
    `SqlFn.ASIN`.
  - DuckDB `domainGuarded`: `CASE WHEN x BETWEEN -1 AND 1 THEN acos(x) ELSE 'NaN'::DOUBLE END` (`DuckDb.java:542-543`,
    `:732-736`).
  - H2 the same, clamped (`H2.java:743-744`, `:1435-1446`).
  - Ruled to change at step 1 / §6.1 item 7: raise as Pure does, and the DuckDB NaN guard goes. Not yet landed at
    `33d85c92`.
- **`sqrt`:** Row 1.
- **Shifts:** Row 1. H2 writes none.

**LEGEND-ENGINE.**
- `acos`/`asin`: DuckDB `acos(%s)` and `asin(%s)` (`ED:190`, `:195`) raise (`MD/...:322-326`, "Unexpected error
  executing function"). H2 answers NaN, "No error was thrown" (`MH/...:514-518`).
- `sqrt`: DuckDB raises; H2 NaN, written null.
- Shifts: DuckDB left past 62 raises, right gives 0; H2 "No error was thrown".

**THE ROW TO ADD.**
- When step 1's `acos`/`asin` raise lands, make it a `Scalars.guarded(..., styled)` over a bare `SqlFn.ACOS`/`ASIN` with
  a raise kind. Either reuse `INVERSE_TRIG_NAN` (today SQLite's and MariaDB's "cannot hold NaN" wording) or add a new
  `INVERSE_TRIG_OUT_OF_DOMAIN`.
- Delete the DuckDB and H2 `domainGuarded` arms so that `ACOS` is the bare function.
- Put the kind in the table's plain set (database part only).
- Under `engine` the plain form is then the database's own: DuckDB raises (legend-engine's answer); H2, see the risk
  below.
- No new `SqlFn`.

**RISK.**
- **H2 2.5.252 raises on `acos(2.0)` where legend-engine's 2.1.214 answers NaN.** `H2.java:1435-1441`: "H2 folds a
  constant inside a CASE's untaken branch too, and raises on acos(2.0) there (measured on 2.5.252)". So the plain form
  on lite's H2 may raise where legend-engine gave null: S-21, a database version. Measure a column operand. If 2.5.252
  raises there too, record it.
- Pure/v1 on H2 then raises where legend-engine answers `null` (third review 8, item 7), unless the kind joins the
  plain set.

**THE JUDGES.**
- PCT DuckDB and H2 `trigonometry::testArcCosineError` and `testArcSineError`: BOTH_FAIL today. They become PASS for
  `lite` and `pure` (Pure raises); under `engine` they stay BOTH_FAIL (the database's own).
- Shifts and `sqrt`: as in Row 1.

---

## Recommended order (cheapest and most certain first)

1. **Route the static-empty `toOne` arms** (`Coercions.java:63-67`, `:174-179`) through `forms().raise`. This is a
   precondition for deleting A3 without moving `raise.toOneOfEmpty`.
2. **Put the database into `lowersAs` (G1), and the stamp into the raise choice (G2's call half)**: `EditionTable.plain
   (kind, outsideStore)`, read by `Scalars.guarded` and the shift rule. Move `ENGINE_UNCHECKED` from
   `EdgeLoweringForms` into `EngineTable`.
3. **`round` → `roundHalfUp`** (DuckDB and H2). Existing `SqlFn`, three signatures.
4. **The parses:** `castToInteger` and `castToFloat` (existing Cast DIALECT), and delete `UnguardedParses`.
5. **`isNumeric` per database**: `hasNoLetterCase` (DuckDB) and `isPlainDecimalText` (H2), existing nodes. Delete
   `LoweringForms.isNumeric` and its two implementations. Bring the H2 `~` question to the runner first.
6. **`dateDiff` → `sqlDateDiff`**, after the user's unit-scope answer.
7. **`substring`/`indexOf`**: hand `EngineTable` to PCT's engine route and the corpus's `execute()` (built; amendment 24).
8. **`SqlFn.DEFAULT_DECIMAL`** (new, DuckDB and H2 arms plus refusals): `castToDefaultDecimal` (parseDecimal on DuckDB)
   and `toDefaultDecimal` (toDecimal). `parseDecimalFivePointTwo` on H2. **Measure H2 2.5.252's bare DECIMAL first.**
9. **`SqlFn.CAST_TEXT`** (new, 11 arms) with `castToText`: DateTime and Boolean, a Float through `FLOAT_FULL_TEXT` with
   DuckDB's literal case. Measure the Float pair on DuckDB.
10. **`splitPart` → `sqlSplitPart`** (DuckDB), after posting the wider semantics.
11. **Enumeration by name**: the fold change (step 5 of the plan), with the stamp for S169.
12. **`acos`/`asin`** with step 1's raise, and the kind in the plain set.
13. **Statement-level:** the keyed count (a carrier or a ruling) and the null-unsafe flag (option A or B).
14. **`parseJSON`**: remove it from `engine`'s database part after re-reading S366's test.

### Not rows

- `rem` (already the database's `MOD`; only Row 1).
- `parseBoolean` and `parseDate` (refusals or unruled).
- `ascii('')` on H2: lite's plain `ascii` already gives H2's NULL (`H2.java:767`; `Scalars.java:598`). Pure answers 0,
  which is a product gap to record.

## What the whole set needs

- **New product natives** (names proposed by what they are; each with the overloads of the function it stands for, in
  `Pure.java`'s order):
  - `roundHalfUp` (3 overloads);
  - `castToInteger`;
  - `castToFloat`;
  - `hasNoLetterCase`;
  - `isPlainDecimalText`;
  - `sqlDateDiff`;
  - `castToDefaultDecimal`;
  - `toDefaultDecimal`;
  - `parseDecimalFivePointTwo` (name to be ruled);
  - `castToText`;
  - `sqlSplitPart`;
  - optionally `equalAnsi`.

  Each must be registered as an implementation in the platform tables, as `substringAsGiven` is (`Pure.java:683`, `:697`,
  `:705`, `:1052-1057`), and listed in the guard 3 allowlist (design v3 §7).
- **New `SqlFn` values:**
  - `DEFAULT_DECIMAL` (or a `SqlType.Scalar` constant);
  - `CAST_TEXT`.
- **New dialect arms:** for each new `SqlFn`, one arm in every dialect's call switch (no `default ->`):
  - `DuckDb.java`: `CAST(x AS DECIMAL)`, `CAST(x AS VARCHAR)`;
  - `H2.java`: the same, with **no** Boolean CASE;
  - `Postgres`, `SQLite`, `MariaDb`/`MySql`, `Trino`, `ClickHouse`, `Snowflake`, `Databricks`, `BigQuery`, `MongoDb`:
    a `DialectCapability` refusal until legend-engine's answer is read there.

  `SqlTyping` needs a type for both. `FLOAT_FULL_TEXT`'s DuckDB arm (or `CAST_TEXT`'s) needs a `FloatLit` case for
  DuckDB's literal processor.
- **Changes to existing code:**
  - `EditionTable.lowersAs` gains the target `DatabaseType`;
  - `EditionTable` gains `plain(RaiseKind, boolean outsideStore)`, and optionally `trustsDeclaredKeys()` and a
    null-unsafe hook;
  - `Scalars.guarded`'s styled overload takes the call;
  - `forms().raise` becomes generic for the root guards (if they are routed);
  - delete `UnguardedParses`, `LoweringForms.isNumeric`, `parsedDecimal` and `parsedJson`, with their two
    implementations each;
  - the DuckDB and H2 `domainGuarded` arms go with step 1's `acos`/`asin`.

## Not verified

- That H2 (legend-engine's patched 2.1.214) accepts `s ~ '^(?:...)$'` from `SDD:894-897`. If not, legend-engine's H2
  `isNumeric` raises.
- That legend-engine's Java executor checks `resultSizeRange` at the root (`Lowerer.java:1041-1048` says so). This decides
  whether `ToOneGuard` and `requiredOneEgress` stay checked under `engine`.
- H2 2.5.252's bare `DECIMAL` scale (`toDecimal(3.8)`) and its `DATEDIFF(WEEK)` rule, compared with legend-engine's
  2.1.214.
- H2 2.5.252's `acos(x)` for a column `x` outside [-1, 1] (raises or NaN).
- Where S366's `'}{'` tolerance lives in legend-engine. Its SQL is strict on DuckDB (`DX:259`) and Jackson-Map strict on
  H2 (`LH2:116-135`).
- That a spelled enumeration value lowers as its name (`EnumSourceValues`), so that lifting the fold gives
  legend-engine's by-name `true`.
- Lite's PCT judge's Decimal comparison for `testIntToDecimal` (`8.000` vs `8D`), which decides whether that test moves.
- Whether rules other than `equal`'s emit `NULL_SAFE_EQUAL` (Row 15, option B).
- Whether `testRemError` on DuckDB reaches the static-empty or [0..1] `toOne` arm or a `CheckedOne`.
- What lite's H2 currently answers for `testRemWithMixedIntegersAndFloats` (BOTH_FAIL, WRONG_ANSWER: possibly already
  legend-engine's `3`).
- legend-engine's Java-part answers for every row (r2 §5.4 Q8): the stamp's `outsideStore` column assumes Pure's
  meaning everywhere.
- The checkout moved during this research (`c06ebbab` → `33d85c92`). Line numbers are at `33d85c92`. Files edited by
  later commits may shift again.
