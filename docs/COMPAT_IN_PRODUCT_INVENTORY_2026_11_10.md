# Compatibility hidden in the product: the D0 inventory (2026-11-10)

> **Status: RESEARCH, nothing ruled, no code changed.** This is step D0 of `docs/DATA_DELIVERY_DESIGN_2026_11_10.md`
> §6 (branch `origin/claude/data-delivery-design`): every place the PRODUCT path stringifies, formats or guards so as
> to reproduce legend-pure's or legend-engine's TEXT, with the typed behaviour the product would have instead and what
> the edge (`com.legend.edge`, G5) would do to stay compatible. It feeds D1 (the move) and D2 (the guard).
>
> Branch `claude/p2-compat-inventory`, made from `origin/claude/total-wall` at `886eadde`; every `file:line` is that
> commit's, under `core/src/main/java/com/legend/` unless it says otherwise. Upstream's answers were READ from the
> pinned sources (legend-pure 5.99.0, legend-engine 4.145.0, shallow clones of the tags; the reads are
> `docs/compat-inventory-homework/upstream-reads.txt`), never recalled. P3's sites
> (`docs/LEAN_PRODUCT_P3_2026_11_10.md` on `origin/claude/p3-lean-product`) are cross-referenced, not redone (§9).

## 0. The answer in one table

| kind | what it is | sites | path | product change if moved | ruling needed |
|---|---|---|---|---|---|
| **K1** | a guard raised IN SQL whose message is legend-pure's runtime text | 23 raise sites, 12 texts (§1) | both: every product query that calls the function | the message a product caller of `Execution` reads; the `pure/v1` answer stays (the edge spells it) | **Q1** (reopens S138), Q2 |
| **K2** | a value spelled Pure's way inside such a message | 1 (`sqrt`'s operand through `FLOAT_PRINT`) (§2) | both | none for the PCT value (`-1.0`); upstream itself spells it Java's way | Q3 |
| **K3** | messages that copy the REFERENCE DATABASE's (DuckDB's) words on another database | 4 families (§3) | both | the message text on Postgres, MariaDB/MySQL, SQLite | Q4 |
| **K4** | engine SQL spellings in product dialects (quoting, NULLS order, upper-casing, a lexicon, a flag-gated scan order) | 9 (§4) | both | none on any answer: SQL text only | Q5 |
| **K5** | engine-TEXT printers reached from the product planner (`planToString`, `toSQLString`, `toDDL`, the plan-text allocations) | 1 seam, ~12 call sites (§5) | both | none (move only) | none (P2 already names it) |
| **K6** | the engine-compatible writers and the engine vocabulary inside `result/CellText` | 3 (§6) | product: the server's `pure/v1` route; test: PCT, service tests | none (move only) | none (P2, Clause 7 A1) |
| **K7** | exec: the engine's per-statement trace comment, the legacy executor's decode rules | 2 (§7) | both: every query | none (D3, P3) | none here (D3, P3) |
| **K8** | compile-time and server messages in legend-engine's words (`Can't find ...`, the `pure/v1` error JSON, `Missing external parameter(s)`) | ~15 (§8) | both | the message a product caller reads | Q6 |
| **K9** (added) | Pure's EXACT print forms as a function's answer (`toString`, `format`, `makeString`, a relation's text, `toCSV`, lite's CSV/JSON/Arrow tokens): compatibility by the user's ruling of 2026-11-10 (§13) | 14 (§13) | both | yes, for users of lite's own outputs: §13.4 | Q10-Q15 |
| **not compat** | the database judge's verdict canon (test path) | §10 | | | |

Firing was **not measured by a counter**: this container has no Bazel and a cold setup (the LLVM toolchain, the
distdir, a core build) is well over the hour the brief allows (`docs/CLOUD_SESSIONS.md`). The static proxy used
instead: how many upstream PCT `assertError`s expect each text (`upstream-reads.txt`, last section) and how many
lite test files pin it. Where P3 measured a site, its numbers are quoted.

## 1. K1: SQL guards that raise legend-pure's text

**How it works today.** The lowering builds `CASE WHEN <bad> THEN ERROR(<message>) ELSE <value> END`
(`lowering/Scalars.java:2926-2937`, `PureSql.raise` at `lowering/PureSql.java:27`); every dialect wraps the message
in the U+001F envelope (`sql/dialect/AnsiSqlRenderer.java:910-924`, per-dialect forms: H2 `SIGNAL`, Trino `fail`,
Postgres and Snowflake a failing cast, SQLite a bad JSON path, MariaDB a session variable read back by
`exec/MariaDbConnector.java:45-51`); `exec/RaisedErrors.unwrap` cuts the text back out and `error/DataError` carries
it as an untyped `RuntimeException` message. The judge compares it by `String.equals`
(`testexec/AssertErrorRuns.java:73`).

**What upstream does.** These texts are legend-PURE's interpreter/compiled runtime's (`upstream-reads.txt` §1).
legend-engine's relational SQL raises none of them: its `extensionDefaults.pure:218,267,287` writes `((1.0 * %s) /
%s)`, `mod(%s,%s)`, `sqrt(%s)` with no guard, so on engine the database's own behaviour is the answer. The guards
exist so that the PCT's `assertError`s, which carry Pure's text, pass on SQL.

**The existing ruling.** S138 (the user, 2026-11-05): "a guard that implements a Pure error (an index out of bounds,
a divide by zero, a bad date part) carries upstream's exact text". D0 asks to move that text to the edge, so Q1
reopens S138 for the user.

| # | where | text produced (when) | PCT asserts / lite pins |
|---|---|---|---|
| 1.1 | `lowering/Scalars.java:449` (rem), `:2444` (`legend_lite::quotient`) | `Cannot divide <a> by zero`; `<a>` is the database's `CAST(a AS VARCHAR)` (a zero divisor) | 1 / 1 file |
| 1.2 | `lowering/DecimalKindRules.java:53` | `Division by zero` (Integer ÷ Integer, zero divisor): this is the text of Java's `ArithmeticException` that legend-pure's INTERPRETER lets through (its `NumericAccumulator` divides integers as `BigDecimal`), not a written Pure message; compiled Pure raises `Cannot divide <divisor> by zero` (`CompiledSupport.java:1485`) | 0 / 3 files |
| 1.3 | `lowering/Scalars.java:412` | `Unsupported number of bits to shift - max bits allowed is 62` (a shift outside 0..62; folded when the shift is a literal) | 2 (engine's `bitShiftLeft/Right.pure`) / 0 |
| 1.4 | `lowering/Scalars.java:1210` | `Cannot get <part> for <date>` (a literal date too coarse for hour/minute/second/day of month) | 4 / 3 files |
| 1.5 | `lowering/Scalars.java:1507`, `lowering/TaggedLists.java:200` | `The system is trying to get an element at offset <i> where the collection is of size <n>` (`at` out of bounds) | 7 / 2 files |
| 1.6 | `lowering/Scalars.java:1552` | `add(set, index, value): index out of bounds` (lite's own wording, not Pure's: listed for completeness) | 0 / 2 files |
| 1.7 | `lowering/Scalars.java:1842` | `Unable to compute sqrt of <x>` (x < 0); `<x>` see K2 | 1 / 0 |
| 1.8 | `lowering/ToOneGuard.java:67`, `lowering/Coercions.java:65,101,136`, `sql/dialect/AnsiSqlRenderer.java:603-621` (`checkedOne`) | `Cannot cast a collection of size <n> to multiplicity [1]` / `[1..*]` (toOne, toOneMany on the wrong size; also the relation-level toOne) | 5 / 7 files (the corpus among them) |
| 1.9 | `lowering/ListEncodings.java:265` | `The low bound (<lo>) can't be higher than the high bound (<hi>) in a slice operation` | 1 / 0 |
| 1.10 | `lowering/DateCtorRule.java:47,53,66,81` | `Invalid day: <y>-<m>-<d>`, `Invalid <component>: <v>` (date(...) with a component out of range; folded for literals, a CASE otherwise) | 1 / 4 files |
| 1.11 | `lowering/CastPolicy.java:221` | `Cast exception: <src> cannot be cast to <target>` (a cast no lane can succeed) | 1 / 4 files |
| 1.12 | `lowering/ListRules.java:27` | `range step must not be 0` | 1 / 0 |
| 1.13 | `lowering/CheckedEnvelope.java:84` | `Unable to evaluate constraint [<id>]: data not available - check your mappings` (a checked constraint over a NULL) | engine's milestoning/constraint text; corpus |
| (own) | `lowering/Scalars.java:2054`, `lowering/CanonicalRenderSql.java:657`, `lowering/Scalars.java:249` (`fail`) | lite's own sentence, or the user's own message: not compat | |

**The proposed product behaviour, typed.** Every K1 guard keeps its OUTCOME (an error, as legend-pure raises; no
null, no NaN: each of these functions raises upstream, so a null or NaN would be a new divergence, not a typed form of
today's). What changes is the error's shape:

- the plan's raise names a **kind** from one closed enum (say `RaiseKind`: `DIVIDE_BY_ZERO`, `BIT_SHIFT_RANGE`,
  `DATE_PART_ABSENT`, `INDEX_OUT_OF_BOUNDS`, `INSERT_OUT_OF_BOUNDS`, `NEGATIVE_SQRT`, `SIZE_NOT_IN_MULTIPLICITY`,
  `SLICE_BOUNDS_INVERTED`, `DATE_COMPONENT_INVALID`, `CAST_IMPOSSIBLE`, `RANGE_STEP_ZERO`, `CONSTRAINT_UNEVALUABLE`,
  `USER_FAIL`), plus its **operands** as the database's own text of each value (`CAST(x AS VARCHAR)`, as today's
  messages already do for all but K2);
- the envelope carries kind and operands (a new separator inside the U+001F pair, as U+001E already carries the
  position), and `exec/RaisedErrors` makes a typed `DataError` (kind, operands, position) instead of a message;
- the product's own message is the kind's sentence, written once, with no Pure wording.

**The edge's replacement.** One class in `com.legend.edge` (say `PureMessages`) maps `(kind, operands)` to legend-pure's
text, each row citing its upstream line (`upstream-reads.txt`). Its users: the test road's `assertError` judgment
(`testexec/AssertErrorRuns`, which then compares Pure's text to Pure's text), and the server's `pure/v1` error answer
(`server/PureV1Api.java:1022-1077`), which is the engine-compatible surface. The product depends on neither (D2's
rule). What depends on it: the 24 PCT asserts above and the lite test files that pin the texts.

**Whether the product's observable answer changes.** Through `pure/v1` and on PCT: no, the edge spells the same
text. Through `Execution.run` (a Java caller reading `DataError.getMessage()`): yes, e.g. `rem(5, 0)` today
`Cannot divide 5 by zero`, after `DIVIDE_BY_ZERO: 5` (the exact sentence is the user's to pick). Q1.

## 2. K2: a value spelled Pure's way inside a message

| where | what | upstream | proposal |
|---|---|---|---|
| `lowering/Scalars.java:1844` | `sqrt`'s operand is spelled through `floatRepr` = `LiteralSpelling.floatPrint` (`lowering/LiteralSpelling.java:478-491`) = `SqlFn.FLOAT_PRINT`, Pure's `toString` of a Float, rendered in SQL by every dialect (`FloatPrintText`, Snowflake's own) | `CoreHelper.java:520`: `"Unable to compute sqrt of " + input`, a Java `double` concatenation, i.e. `Double.toString`: `-1.0`, but `-1.0E-5` where lite writes `-0.00001` | the product's operand is the database's text of the value (as every other K1 operand); the edge, holding a typed operand, spells it as upstream does (`Double.toString`: G6 allows small Java formatting at the edge). The PCT's `-1.0` is the same both ways; values with an exponent change, toward upstream |

This is the user's example ("they spell numbers the way Pure prints them"). It is the only message operand spelled
through a Pure print form; every other operand (`rem`'s, `at`'s, the date parts', the slice bounds') is a plain
`CAST(... AS VARCHAR)`, the database's text. Firing: 1 PCT assert (`testSquareRootError`).

## 3. K3: the reference database's words on another database

These are not upstream's text at all: they make Postgres, MariaDB/MySQL or SQLite raise what DuckDB raises, so one
expected text serves every lane.

| where | text | proposal |
|---|---|---|
| `sql/dialect/Postgres.java:357-358,406-408,412-415` | `The map key list does not align with the map value list.`, `Map keys can not be NULL.`, `Map keys must be unique.` (DuckDB's map errors) | a typed kind (`MAP_KEYS_MISALIGNED`, ...); the product's sentence; no edge row unless a PCT expects DuckDB's words (none read) |
| `sql/dialect/MariaDb.java:2218-2219`, `sql/dialect/SQLite.java:1794-1796` | `Could not convert string '<s>' to INT64` (DuckDB's cast error) | the same: `TEXT_NOT_INTEGER` with the text as operand |
| `sql/dialect/MariaDb.java:2729`, `sql/dialect/SQLite.java:2295-2296` | `Could not parse '<s>' as a date` | `TEXT_NOT_DATE` |
| (already lite's own, by S138) `MariaDb.java:2049-2052`, `SQLite.java:1672-1675`, `SQLite.java:414-415`, `ClickHouse.java:1855-1856`, `MongoDb.java:664,835,1065,1068` | "a value this database cannot hold" sentences | already the product's own words; they become typed kinds with the rest |

Observable change: the message on those databases (Q4). DuckDB itself keeps raising its own native error, which is
not a platform raise and passes through `RaisedErrors` untouched.

## 4. K4: engine SQL spellings in product dialects

These shape the SQL text, never an answer. They exist so that lite's SQL reads like the engine's (the corpus pins
some SQL text, the activity rows show it).

| where | what | why it is there |
|---|---|---|
| `sql/dialect/AnsiSqlRenderer.java:442-458` | a bare sort key gets `NULLS LAST/FIRST` as the engine's `processSortItem` | engine ORDER semantics (an answer: null order) — keep, it is semantics |
| `sql/dialect/AnsiSqlRenderer.java:706-713` | a Float literal written bare, as the engine's literal processor | text |
| `sql/dialect/AnsiSqlRenderer.java:1766-1770, 1811-1818`, `H2.java:128,222` | schema-qualified names and aliases always quoted (`as "root"`) | corpus SQL goldens |
| `sql/dialect/AnsiSqlRenderer.java:1851, 2148` | DDL details (pk list, explicit `NULL`) | engine DDL text |
| `sql/dialect/DuckDb.java:712` | unquoted identifiers folded to upper case | the engine corpus |
| `sql/dialect/DuckDb.java:685-702`, `StableScanOrder.java` | a stable scan order added only under the system property `legend.exec.engineScanOrder` | ENGINE-CORPUS-COMPAT, flag-gated |
| `sql/dialect/H2.java:85-112` | `ENGINE_LEXICON` (engine's h2Extension reserved words), the product lexicon derived from it | quoting |
| `sql/dialect/InformationSchemaCatalog.java:16-17,42,57` | `upper(...)` identifiers and legend-pure's JDBC type names in catalog reads | engine spelling |
| `sql/dialect/SqlDialect.java:18-21` | `renderInline`: "the form legend-engine prints a statement it reports" | the test-data generator |

Proposal: the NULLS order is semantics and stays. The rest are text with no answer behind them: the flag-gated scan
order and the lexicon's engine half move to the edge (a dialect decorator the edge supplies), and the quoting rules
either stay as the product's one spelling or move, as the user prefers (Q5). Nothing observable changes except SQL
text in activity rows and plan dumps.

## 5. K5: engine-TEXT printers reached from the product planner

`planner/EngineText.java:41-43,74-75` is the only builder of `EngineStyleH2` (and so DB2, Composite); it is called by
`planner/EnginePlans.java:81,127,367-390,463,623-690`, `planner/PlanAllocations.java:140,280`,
`planner/FramePlanner.java:147`, and staged into every product body by `planner/ProductStatements.java:143` and
`planner/BodyPlanner.java:249` (`EnginePlans.nativeRoutines`: the `planToString`, `toSQLString` and `toDDL` natives),
`planner/BodyPlanner.java:348,365` (`ddlText`). The lowering's engine-text mode is `Lowerer.withEngineText()`
(`lowering/Lowerer.java:230`, used by `EnginePlans.java:179`; it reaches `CastPolicy.java:49-50` and
`Lowerer.java:1021`). No executed statement uses it. `sql/dialect/RawSqlBoundary` translates only text written for
the engine's H2 (`AnsiSqlRenderer.java:1312-1314`); its `Recorder` is used only by `edge/CompatEdge.java`, which main
never constructs.

These natives' ANSWER is the engine's text by definition (JAVA_EVICTION_PLAN §1: engine-exact text is a render target).
Proposal (P2 already lists it): `EngineText`, `EnginePlans`' text half, the three `EngineStyle*` classes, the engine
lexicon and `RawSqlBoundary` move to the edge; the product planner reaches the three natives through the typed edge
extension point, and a run without the edge refuses them by name. No answer changes.

## 6. K6: the engine-compatible writers

- `result/PureV1Writer.java` (the engine's TDS JSON "byte for byte"), built only for `Output.Engine`
  (`result/Output.java:46-83`), which only `server/PureV1Api.java:429,459` and the PCT route build.
- `result/TdsObjectsWriter.java` (engine `PURE_TDSOBJECT`), built only by `test/ServiceTestRunner.java:513`.
- `result/CellText.java` holds BOTH vocabularies: `pure` (`:35-78, 199-232, 273-354`, lite's outputs: product) and
  `engine` (`:149-166, 236-254, 292-305, 362`: `Double.toString`, nine-digit `+0000` timestamps, NaN as `null`,
  `engineDay`). The pivot-name quoting `presentedName` (`:259-261`, engine's `mayQuotePivotColNames`) is applied by
  every writer, the lean ones included.

Proposal: `PureV1Writer`, `TdsObjectsWriter` and `CellText`'s engine half move to the edge (Clause 7 A1); the edge
registers `Output.Engine` with the result library. One product change to rule: `presentedName` quotes a pivot column
`'a__|__b'` in lite's own CSV/JSON too; moved to the edge, lite's own writers would write `a__|__b` (Q8). P3 site 7
(the dynamic-pivot envelope) lives in the same writer.

## 7. K7: exec

- `exec/JdbcStatements.java:98-125` (`stamped`), `exec/ExecutionTrace.java`, `executionplan/ExecutionPlan.java:136-137`:
  the engine's `-- "executionTraceID" : "<uuid>"` line before every query, product queries included. This is D3,
  already ruled (a planned comment plus exec's own trace) and scheduled for the total wall's M5; recorded, not redone.
- `exec/JdbcDecode.java:37-43, 318-336`: the legacy executor's decode rules (a String kept under an integral label; a
  timestamp read at nine digits as the engine does). P3's territory (its sites 1-6); recorded, not redone.
- `exec/MariaDbConnector.java:45-51`: the raise read-back, D4.

## 8. K8: compile-time and server messages in legend-engine's words

| where | text |
|---|---|
| `compiler/spec/TableReferenceChecker.java:113-115` | `Can't find database '<path>'` (engine's `HelperRelationalBuilder.getDatabase`) |
| `compiler/element/ModelIntegrity.java:130,158,164,184,191,199,511` | `Can't find connection/stereotype/tag/the profile/mapping/runtime/database ...` |
| `compiler/element/ModelIntegrity.java:265-282` | `Invalid supertype: '<n>' cannot extend '<s>' ...` (engine's `ClassCompilerExtension`) |
| `compiler/element/TypeClassifier.java:207-210` | `Wrong type arguments count (N) for type: ...` (engine's `CompileContext.newGenericType`) |
| `normalizer/MappingNormalizer.java:1417`, `ImplicitInheritance.java:136`, `MappingValidation.java:83`, `MappingClosures.java:59,428` | the same `Can't find ...` family |
| `compiler/spec/TdsDesugars.java:787` | Pure's runtime `The system is trying to get an element at offset ...`, thrown in Java at COMPILE time as an `IllegalStateException` |
| `server/PureV1Api.java:791, 800-809` | `Missing external parameter(s): name:Type[m], ...` (engine's wording, measured 2026-09-30) |
| `server/PureV1Api.java:1022-1077` | the engine's error JSON `{"code":-1,"errorType":...,"message":...,"status":"error"}`, `SimpleClassName: message` |

These are already TYPED by the phase (`LegendCompileException.Phase`), so the kind exists; only the sentence is the
engine's. The server's shape belongs to the engine API it serves and is the edge by nature. Proposal: the compile
messages are the product's own sentences, keyed by an error kind, and the `pure/v1` route (an edge) spells the
engine's. Observable change: the message a product (non-`pure/v1`) caller reads, the LSP's diagnostics among them
(Q6). `TdsDesugars.java:787` should be a typed compile error whatever the ruling (it is an `IllegalStateException`
standing for a user error).

## 9. P3's sites, cross-referenced

P3 (`docs/LEAN_PRODUCT_P3_2026_11_10.md` §0) holds the Java value computations in decode and the writers: the REAL
re-parse, BigQuery's scale padding, the TIMESTAMPTZ shift, the BC-year repair, kinds from a cell's text, the positional
read, the dynamic-pivot envelope. None is repeated here. The overlaps: P3 site 7 is inside K6's `PureV1Writer`; P3
sites 1-6 are inside K7's `JdbcDecode`/`RowDecode`. A D1 move of K6 carries P3 site 7 with it.

## 10. Not compatibility: where the line is drawn

> **Superseded in part (2026-11-10, the user's ruling relayed by the orchestrator):** "for toString and format make
> string etc we should pick something very very simple and easy for product to do on all databases nothing
> complex". Pure's exact print forms are therefore COMPATIBILITY (K9, §13), and Q7 below is answered by that ruling.
> What stays below as not compatibility is the database judge's verdict canon, which is the test road's own.

Pure's print forms where they ARE the function's answer stay in the product: they are Pure's language semantics,
which lite implements (upstream-fidelity tenet), not a copy of upstream's output text.

- `toString`, `format` (`%s` of a Float, a date), `makeString`, the mixed-list `[a, b]` print:
  `lowering/Scalars.java:2287, 2762, 3314`, `TaggedLists.print`, the dialects' `FLOAT_PRINT` arms, H2's/MariaDB's/
  SQLite's `true`/`false`, MongoDb's date text.
- lite's own CSV/JSON/Arrow writers' Pure tokens (`CellText.pure`, `pureJson`).
- the database judge's verdict canon (`LiteralSpelling.leaf/literal`, `floatCanon`, `temporalCanon`, used only by
  `lowering/VerdictSql` and `lowering/CanonicalRenderSql`): test path, the judge's own lattice.
- `lowering/Render.java` (`toCSV`): engine's `meta::relational::tests::csv::toCSV` text, a function a user calls, its
  answer that text; it is borderline (a tests namespace) and listed in Q7.

One site inside a print form IS engine compatibility: `lowering/LiteralSpelling.java:479-488`. A Float property whose
store column is an integer prints the integer (`1000000`, not Pure's `1000000.0`) "as legend-engine's cast of the
column does" (the corpus's `Quantity not in range: 1000000`). In the product, Pure's `toString` of a Float would be
`1000000.0`. Q9.

## 11. Questions for the user (each is a real behaviour change; nothing here is decided)

- **Q1 (reopens S138).** Should the K1 guards raise a typed kind with operands, with Pure's text spelled only by the edge
  (for PCT's `assertError` and the `pure/v1` answer)? Before: `Execution.run` of `|5->rem(0)` fails with
  `DataError("Cannot divide 5 by zero")`. After: `DataError(DIVIDE_BY_ZERO, ["5"])`, whose message is the product's own
  sentence; PCT and `pure/v1` unchanged. Also asked: the product's sentence for each kind (lite's wording).
- **Q2.** `Integer / Integer` by zero raises `Division by zero` (the interpreter's `BigDecimal` exception text,
  `lowering/DecimalKindRules.java:53`); legend-pure's compiled `divide` raises `Cannot divide <divisor> by zero` for any
  zero divisor (`CompiledSupport.java:1485`), and lite's Float lane yields Infinity instead of raising. No PCT asserts
  either. Same kind as rem (`DIVIDE_BY_ZERO`), with which operand, and does the Float lane raise too? (This is an
  outcome divergence found on the way, not text: it needs a register row either way.)
- **Q3.** `sqrt`'s operand: the database's text in the product, upstream's `Double.toString` at the edge? Before (any
  path): `sqrt(-0.00001)` raises `Unable to compute sqrt of -0.00001`. After at the edge: `-1.0E-5` (upstream's
  text); in the product: the database's text of the double. PCT's `-1.0` unchanged.
- **Q4.** The DuckDB-worded messages on Postgres, MariaDB/MySQL and SQLite (K3): typed kinds with the product's own
  sentence? Before on Postgres: `The map key list does not align with the map value list.`; after: the kind's sentence.
- **Q5.** The engine SQL text conventions in product dialects (K4, all but NULLS order): keep as the product's one
  spelling, or move the engine-only half (the flag-gated scan order, the engine lexicon's surplus, upper-casing) to an
  edge decorator? No answer changes; SQL text in activity rows and plan dumps does.
- **Q6.** Compile-time messages in legend-engine's words (K8): the product's own sentences keyed by a kind, engine's
  at `pure/v1` only? Before (LSP, `Compiler.compileErrors`): `Can't find database 'x::Db'`; after: lite's sentence.
- **Q7.** Confirm the line of §10: Pure's print forms as a function's answer (`toString`, `format`, `makeString`,
  lite's CSV/JSON tokens) are product semantics and stay; and say whether `toCSV` (engine's text, a tests-namespace
  function) is product or edge.
- **Q8.** The pivot column name quoting (`'a__|__b'`, `result/CellText.java:259-261`) in lite's own CSV/JSON writers:
  keep (it is legend-engine's naming) or write the bare name once `CellText`'s engine half moves?
- **Q9.** A Float over an integral store column prints the integer (`1000000`) where Pure's `toString` prints
  `1000000.0` (`lowering/LiteralSpelling.java:479-488`): the product says `1000000.0` and the edge (the corpus) keeps
  `1000000`, or the product keeps engine's answer?

## 12. The homework

`docs/compat-inventory-homework/`:
- `upstream-reads.txt`: every upstream line quoted above, and the PCT assert counts;
- `census.sh` and its output `census-output.txt`: the mechanical half (every SQL raise and its literal, every engine
  dialect builder, the comments naming upstream's text), re-runnable on any commit;
- `print-probe.py` and `print-probe-output.md`: the databases' own text per type (DuckDB, SQLite, Postgres; §13.2);
- not done: a firing counter on `pct_duckdb` and `//spec:corpus_duckdb` (no Bazel here; P3's patch
  `docs/lean-product-homework/p3count.patch.txt` is the pattern to reuse, with one arm per K1 kind in
  `RaisedErrors.unwrap`).

## 13. K9: the product's print forms (added 2026-11-10, after the user's ruling on print forms)

The ruling (relayed by the orchestrator): the product's `toString`, `format`, `makeString` and the like are "something
very very simple and easy for product to do on all databases nothing complex"; Pure's exact print forms are
compatibility and go to the edge. This section lists every place the product prints a value as text, what it does
today, a simple per-type form, and the answers that change.

### 13.1 Every print site, and whether it emulates Pure today

| # | where | what it prints today | emulates Pure? |
|---|---|---|---|
| P1 | `lowering/Scalars.java:2264-2290` (`toString`) | a date LITERAL: its static text from `toEngineString` plus `+0000` for a DateTime (`dateLiteralPrint`, `:3461-3467`); a DateTime value: `LiteralSpelling.dateTimeText` (`lowering/LiteralSpelling.java:596-610`: ISO with `T`, the fraction's nine digits with trailing zeros cut, `+0000`); a Float: `FLOAT_PRINT`; everything else `pureToString` | yes |
| P2 | `lowering/Scalars.java:2751-2862` (`pureToString`) | Float through `FLOAT_PRINT`; Boolean through `BOOL_TO_TEXT` (`true`/`false`); Variant as canonical compact JSON; a user-class instance as `@_<id>`; an `Any` slot by JSON extraction, a nested list as `[a, b]`; a type value as its simple name; a mixed list as Pure's `[e1, e2]` (`TaggedLists.print`), a mixed Decimal with its `D`; Integer, Decimal, String, StrictDate the plain `CAST(... AS VARCHAR)` | yes for Float, Boolean, the list and instance forms; no for Integer/Decimal/StrictDate |
| P3 | `sql/dialect/FloatPrintText.java:96-177` and the twelve dialects' `FLOAT_PRINT` arms (DuckDb:548, H2:511, Postgres:751, SQLite:1493, Trino:890, ClickHouse:1653, Databricks:1036, BigQuery:1490, MongoDb:1172, MariaDb:1932; Snowflake's own at `:1357, 1825-1840`) | Pure's Float text in SQL: shortest digits, never an exponent, a `.0` on whole values, Java's `NaN`/`Infinity`; the portable form reads the value's text about nine times (a `1.5 + 2.25` statement on Snowflake was 48 KB before Snowflake got its own arm, `LiteralSpelling.java:473-477`) | yes: the heaviest single print form |
| P4 | `lowering/LiteralSpelling.java:479-488` | a Float over an integral store column: the integer, as legend-engine (Q9) | engine, not Pure |
| P5 | `lowering/Scalars.java:2205-2240, 3279-3372` (`format`) | `%f` → `FLOAT_PRINT`; `%s` of a number → `pureToString`; `%s`/`%t` of a date → Pure's date text (`datePrintOf`); `%t{pattern}` → `STRFTIME` with Java's date pattern translated, a `[Zone]` prefix shifted in SQL with a computed `+hhmm`; `%r` → `Repr.of` (a string quoted with `\`-escapes, a date with its `%` prefix: `lowering/Repr.java:48-52`); `%0<w>d` → Pure's sign-after-padding (`signedZeroPad`) | yes |
| P6 | `lowering/Scalars.java:960-998` (`makeString`) | each element `PureSql.elementText` = the database's `CAST(... AS VARCHAR)` (JSON-unquoted for variant-carried elements), a NULL element `TDSNull` (`sql/SqlExpr.java:36`), except `''` for an optional scalar | the element text is the database's (simple already); the `TDSNull` is legend-engine's TDS null cell |
| P7 | `lowering/Scalars.java:1009+` (`joinStrings`) | Strings only (Pure's signature): no printing | no |
| P8 | `lowering/Scalars.java:282-297` (`plus` over Strings) | Strings only: Pure has no implicit number-to-text `+`; a number must go through `toString` (P1) | no |
| P9 | `lowering/Render.java:364-650` (`toString` of a relation, `tdsString`, `tdsCell`) | Pure's TDS text of a relation: header, `'`-quoted strings containing `{` or `[`, dates at day, DateTimes with at least three fraction digits (`LiteralSpelling.dateTimeTextTds`, `:623+`, legend-engine's own test), Floats by Pure's text | yes |
| P10 | `lowering/Render.java:52-300, 750-830` (`toCSV`) | legend-engine's `meta::relational::tests::csv::toCSV` text: `yyyy-MM-dd`, `yyyy-MM-dd HH:mm:ss`, RFC 4180 escaping, `TDSNull` under `renderTdsNull` | engine |
| P11 | `result/CellText.java:35-78, 273-354` (`pure`), `:199-232` (`pureJson`): lite's CSV (`CsvWriter.java:50`), JSON rows (`JsonRowsWriter.java:72`), Arrow's text slots (`ArrowWriter.java:151, 205, 214, 422-423`) | Float by Pure's text (`pureFloat`: `1.0E-4` → `0.0001`), a Decimal under a Float column the same, dates by written precision with `+0000` from the minute, lists `[a, b]`, pairs `<a, b>`, maps `{k=v}`, instances `@_<id>` | yes (Java, at the boundary) |
| P12 | `result/CellText.java:259-261` (`presentedName`) | pivot names `'a__|__b'` in every writer | engine (Q8) |
| P13 | `sql/dialect/MongoDb.java:650-652, 699-720, 761-763, 847, 1349-1356, 1408-1412, 1505-1510` | MongoDB's pipeline spells Pure's Float, date (`%Y-%m-%dT%H:%M:%S.%L+0000`) and `format` texts itself | yes |
| P14 | `sql/dialect/H2.java:445-448, 1095-1132`, `MariaDb.java:1774-1797`, `SQLite.java:823-824, 1179-1180, 1353-1354`, `ClickHouse.java:1556-1562, 1970`, `Snowflake.java:1310-1313, 1796-1815`, `BigQuery.java:1418, 1447-1451` | each dialect's repairs so its own text matches Pure's or DuckDB's: `true`/`false` where the database prints `TRUE` or `1`, a double's text "as DuckDB prints it", a timestamp's trailing zeros dropped, a DECFLOAT in plain digits | yes (Pure, or DuckDB as the reference) |

### 13.2 What the databases print by themselves (measured on three of the twelve)

`docs/compat-inventory-homework/print-probe.py`, output `print-probe-output.md` (DuckDB 1.4.4, SQLite 3.53.1, Postgres
16, each `CAST(<value> AS VARCHAR/TEXT)`, run 2026-11-10 in this container). The other nine databases (H2, MariaDB,
MySQL, Trino, ClickHouse, Snowflake, Databricks, BigQuery, MongoDB) are not available here: what is said of them below
is read from today's dialect code, not measured.

| value | DuckDB | SQLite | Postgres |
|---|---|---|---|
| Float 1.0 | `1.0` | `1.0` | `1` |
| Float 0.1+0.2 | `0.30000000000000004` | `0.30000000000000004` | `0.30000000000000004` |
| Float 1e-5 | `1e-05` | `1.0e-05` | `1e-05` |
| Float 1e20 | `1e+20` | `1.0e+20` | `1e+20` |
| Float -0.0 | `-0.0` | `0.0` | `-0` |
| Float NaN / Infinity | `nan` / `inf` | (no NaN) / `Inf` | `NaN` / `Infinity` |
| Decimal(10,2) 1.50 | `1.50` | `1.5` (no decimal type) | `1.50` |
| Boolean true | `true` | `1` | `true` |
| StrictDate | `2024-01-02` | `2024-01-02` | `2024-01-02` |
| DateTime | `2024-01-02 10:30:00.123` | the same | the same |
| DateTime through `strftime`/`to_char` with `T` | `2024-01-02T10:30:00.123` | the same | the same |

The finding: the three agree on digits (shortest round trip) but not on the Float's TEXT: a whole value (`1.0` against
`1`), the exponent's spelling (`1e-05`, `1.0e-05`), negative zero and the non-finite names. "The database's own cast"
is simple, but it does not give one answer across databases for Float.

### 13.3 A simple product form per Pure type

| Pure type | proposed product form | how, on every database | where it is not simple | the edge keeps Pure's exact form by |
|---|---|---|---|---|
| Integer | plain digits | `CAST(x AS VARCHAR)` | nowhere | nothing to do (same as Pure) |
| Decimal | plain digits at its declared scale (`1.50`) | `CAST(x AS VARCHAR)` | SQLite (no decimal type: `1.5`), H2's DECFLOAT (exponent form, today repaired by `H2.java:1127-1132`) | Pure prints a Decimal without its scale's trailing zeros as written (`1.50` for `1.50D`): the edge re-spells from the typed value |
| Boolean | `true` / `false` | `CASE WHEN x THEN 'true' ELSE 'false' END` (one portable form; no database-specific arm) | nowhere | nothing (same as Pure) |
| StrictDate | `YYYY-MM-DD` | `CAST(x AS VARCHAR)`, or `strftime`/`to_char` `%Y-%m-%d` | nowhere measured | nothing (same as Pure) |
| DateTime | `YYYY-MM-DDTHH:MM:SS` plus `.fff` when the value has a fraction, no zone suffix (the platform's DateTimes are UTC) | the database's ISO format function (`strftime`, `to_char`, `date_format`, `formatDateTime`, `$dateToString`) | the fraction: a fixed three digits is one call everywhere; "the digits the value has" needs a trim, which is what today's `dateTimeText` does (an `RTRIM` of nine digits). ClickHouse prints a DateTime64's column scale | Pure writes `+0000` from the minute and the precision as written (S118, S121): the edge appends and re-spells |
| Float | Option A: the database's own `CAST(x AS VARCHAR)`, ruled per database (Postgres `1`, DuckDB `1.0`, SQLite `1.0e-05`). Option B: one portable rule, scientific always (`1.0E0`-style) via a format function, which some databases lack. Option C: shortest digits with a forced `.0`, today's `FLOAT_PRINT` (not simple) | A is one cast | A is not one answer across databases (§13.2); B has no common function; C is the heavy form the ruling retires | the edge re-spells the decoded double with legend-pure's formatter (`DecimalFormat("0.0")`, the text `upstream-reads.txt` cites from `docs/result-boundary-homework/README.md` §7) |
| String | as is | nothing | nowhere | nothing |
| Enum | the value's name | the carrier is the name | nowhere | nothing |
| List, Pair, Map, instance, Variant, a relation | no product text form: a structure is an answer (rows, JSON), not a string. `toString` of these is refused by name in the product, or prints the JSON the database builds (Variant) | `to_json` where the database has it | MongoDB, SQLite build JSON differently | the edge's Pure forms (`[a, b]`, `<a, b>`, `{k=v}`, `@_<id>`, the TDS text) |
| `format` | `%s` takes the product form above; `%d` and `%f` the database's own number formatting (`printf`/`format`/`to_char`); `%t{pattern}` the database's date format function over a translated pattern; `%r` and Pure's sign-after-padding `%0<w>d` are Pure-only | `printf`/`format`/`to_char` | `%r` and Pure's padding rule are not simple anywhere | the edge implements Pure's directive set |
| `makeString` | each element's product form, joined; a NULL element as `''` | `string_agg`/`listagg` over the product form | nowhere | the edge prints `TDSNull` for a TDS cell |

Recommendation: Integer, Decimal, Boolean, StrictDate, String and Enum as above (they are Pure's text already, except
Decimal on two databases). For DateTime, the simplest single call is a fixed three-digit fraction always
(`2024-01-02T10:30:00.000`); printing the fraction only when it is non-zero is already a `CASE`. For Float the user
picks between A (simplest; the text differs per database) and a fixed portable rule; the probe says no one cast agrees.

### 13.4 Answers that change for users of lite's own outputs (before → after, for one sitting)

Assuming the recommended forms (Float option A, DateTime with a fixed three-digit fraction and no zone):

| expression | today (every database) | after, DuckDB | after, Postgres | after, SQLite |
|---|---|---|---|---|
| `1.0->toString()` | `1.0` | `1.0` | `1` | `1.0` |
| `0.00001->toString()` | `0.00001` | `1e-05` | `1e-05` | `1.0e-05` |
| `1e20->toString()` | Pure's `100000000000000000000.0` | `1e+20` | `1e+20` | `1.0e+20` |
| `-0.0->toString()` | Pure's `0.0` (S117: lite's own outputs keep the sign) | `-0.0` | `-0` | `0.0` |
| `%2024-01-02T10:30:00->toString()` | `2024-01-02T10:30:00+0000` | `2024-01-02T10:30:00.000` | the same | the same |
| `%2024-01-02T10:30:00.5->toString()` | `2024-01-02T10:30:00.5+0000` | `2024-01-02T10:30:00.500` | the same | the same |
| `format('%s', [%2024-01-02T10:30:00])` | `2024-01-02T10:30:00+0000` | as `toString` | | |
| `format('%r', ['a'])` | `'a'` | refused in the product (edge only), or `a` | | |
| `[1, 'a']->toString()` (a mixed list) | `[1, a]` | refused by name in the product (a structure), edge only | | |
| `$person->toString()` (no declared toString) | `@_<id>` | refused by name in the product, edge only | | |
| a relation's `toString()` | Pure's TDS text | refused by name in the product (an answer is rows), edge only | | |
| `[1, []]->makeString(',')` with a NULL cell | `1,TDSNull` | `1,` | | |
| lite's CSV/JSON cell of a Float `1.0E-4` | `0.0001` | `0.0001` | `0.0001` | `0.0001` |
| `1e15->toString()` | `1000000000000000.0` | `1000000000000000.0` | `1e+15` | `1000000000000000.0` |
| `1e16->toString()` | `10000000000000000.0` | `1e+16` | `1e+16` | `10000000000000000.0` |
| lite's CSV/JSON cell of a DateTime | `2024-01-02T10:30:00+0000` | `2024-01-02T10:30:00.000` | | |
| Integer, Decimal at scale, Boolean, StrictDate, String | unchanged | unchanged | Decimal unchanged; Boolean unchanged | Decimal `1.5` for `1.50` |

PCT, the corpus and `pure/v1` keep today's text through the edge, which then owns `FloatPrintText`,
`FloatCanonText`'s print role, `LiteralSpelling`'s print forms, `Repr`, `TaggedLists.print`, `Render`'s TDS text and
`toCSV`, `CellText.pure`'s Pure forms and the dialects' P14 repairs. What D1 must decide (Q15): whether the edge's
Pure form is spelled in SQL by the edge's own rule (the database still computes) or in Java at the edge's boundary
(G6 allows small Java formatting at the edge only).

### 13.5 Questions on print forms

Q10 and Q11 are ruled (below); the rest wait for the user (the design's §6.2). The eleven-database measurement is
`docs/data-delivery-homework/PRINT_FORM_PROBE_2026_11_10.md` on `claude/data-delivery-design`. Errors are ruled strict
and typed, with safe versions (§6.1), which answers the direction of Q1 and §14.


- **Q10. RULED (the user, 2026-11-10; `docs/DATA_DELIVERY_DESIGN_2026_11_10.md` §6.1 on `claude/data-delivery-design` (`50c6550b`)):** Float is 6 decimals with trailing zeros (and a trailing `.`) trimmed; Decimal at its declared scale for exact amounts. The original question follows. Float's product text: A (each database's own cast, ruled per database, answers differ as §13.4), or one
  portable rule? Name the rule if portable.
- **Q11. RULED (the user, 2026-11-10; `docs/DATA_DELIVERY_DESIGN_2026_11_10.md` §6.1 on `claude/data-delivery-design` (`50c6550b`)):** DateTime is ISO 8601 UTC, six fraction digits and `Z`, nanoseconds only on the databases that store them. The original question follows. DateTime's product text: `YYYY-MM-DDTHH:MM:SS.fff` with a fixed three-digit fraction (one call everywhere,
  sub-millisecond digits dropped from the TEXT, not the value), or the fraction's own digits trimmed (a `CASE`); and
  the zone: none, `Z`, or `+0000`?
- **Q12.** Structures (lists, pairs, maps, instances, a relation) have no product `toString`: refused by name in the
  product and printed only at the edge, or a JSON text from the database?
- **Q13.** `format`: the product keeps `%s %d %f %t{}`, and `%r` and Pure's `%0<w>d` sign rule are edge only?
- **Q14.** `makeString`'s NULL element: `''` in the product, `TDSNull` at the edge?
- **Q15.** At the edge, is Pure's exact text spelled in SQL (the database computes, today's machinery moved) or in
  Java over the decoded value (simpler, allowed at the edge by G6)?

## 14. Safe variants (the user's idea of 2026-11-10: a typed error by default, explicit safe variants as a product feature)

Each raises by default, as legend-pure does (K1); the safe variant returns the empty value instead, typed `[0..1]`.
These would be lite's own functions (a new name, not a change to a Pure function), so each is a lite addition for the
register, not a divergence. The list is of functions whose K1 guard exists today:

| function (raises today) | safe variant (a name for the user to choose) | typing |
|---|---|---|
| `divide(Number, Number)` (and the Integer pair, Q2) | `divideOrEmpty` | `Float[0..1]` / `Decimal[0..1]` by the operands' kinds (as `divide`) |
| `rem(Number, Number)`, `mod`, `legend_lite::quotient` | `remOrEmpty`, `quotientOrEmpty` | the operands' kind `[0..1]` |
| `sqrt(Number)` | `sqrtOrEmpty` | `Float[0..1]` |
| `at(T[*], Integer)` | `atOrEmpty` | `T[0..1]` |
| `toOne(T[*])` | already has one: `first(T[*]):T[0..1]` (Pure's own); none needed | |
| `toOneMany(T[*])` | none needed: the empty list is the safe answer of the plain list | |
| `date(y, m, d, ...)` with components out of range | `dateOrEmpty` | `Date[0..1]` (the constructor's kind) |
| `cast(@T)` where no lane succeeds | none: this is a compile-time-known impossibility; it should be a compile error, not a run guard | |
| `hour`/`minute`/`second`/`dayOfMonth` of a date without the part | `hourOrEmpty` (and the others) | `Integer[0..1]` |
| `range(start, stop, step)` with step 0 | none needed: the caller passes a step | |
| `slice` with inverted bounds | none needed (an empty list is not Pure's answer; the guard should stay a raise) | |
| `bitShiftLeft/Right` beyond 62 | none needed | |
| the parse family (`parseInteger`, `parseFloat`, `parseDecimal`, `parseDate`, raised by the databases, K3) | `parseIntegerOrEmpty` and siblings | `Integer[0..1]`, etc. |

**Q16.** Which of these does the user want, under which names (`OrEmpty`, `try...`, `safe...`), and should they be in
the platform namespace (`meta::pure::functions::...`, which is legend-pure's) or lite's (`legend_lite::...`)? The
timeseries session's needs come to the orchestrator separately.

## 15. Refresh at phase D's start (2026-11-12, `claude/total-wall-d` from `3a20560f9`)

The rows above were re-read at `3a20560f9`, each searched by its text, not its line. The orchestrator's rulings on
the open points follow the table. From here, the **live register is `core/src/test/java/com/legend/CompatSpellingGuardTest.java`**
(D2's guard). It holds every hit by file and spelling, with the D step that removes it, and it is shrink-only.

| row | state at 3a20560f9 |
|---|---|
| K1 1.1-1.12, own | SAME, lines drifted (e.g. rem `Scalars.java:449`, sqrt `:1842`, `checkedOne` `AnsiSqlRenderer.java:767-784`) |
| K1 1.13 | SAME text, but NOT a SQL raise: `CheckedEnvelope.java:84` rides a defect row (`defect()` :109). Ruled: it becomes the kind `CONSTRAINT_UNEVALUABLE`, spelled by the edge |
| K3 | SAME; BigQuery's map raises are still outside the envelope (`BigQuery.java:2285, 2320, 2322`), a defect D1.1 fixes; the "cannot hold" family remains only in `MariaDb.java:2107`, `SQLite.java:1731` |
| K4 | SAME, all nine. The NULLS order stays as product semantics (ruled 2026-11-12) |
| K5 | SAME (`EngineText`, `EnginePlans`, `PlanAllocations`, `FramePlanner`, `ProductStatements`, `BodyPlanner.ddlText`, `Lowerer.withEngineText`, `RawSqlBoundary` and its `Recorder`, `EngineTextDialect`) |
| K6 | SAME; `Output` is still sealed and permits `Engine`; the writers are made per execution (`Output.java:61-99`) |
| K7 | `JdbcStatements.stamped` and `ExecutionTrace` GONE (C1, 5592d2c). The trace text is `ExecutionPlan.traceComment` :136 and `Sql.stamp` :690, bound per execution at `PlanRunner.java:795`, and read by `PureV1Writer:141` and `testplan/ActivityComments:23` (D1.4). MariaDB's read-back is the plan's `RaiseReadBack` (C2): `MariaDbConnector` holds no raise |
| K8 | SAME, except "Missing external parameter(s)", which moved to `exec/PlanParameters.java:91`; TypeClassifier builds "Wrong <kind> arguments count" (:294) and "variables count" (:245) |
| K9 P1-P14 | SAME in shape, lines drifted |

**New since 04eed79e:**
- raise sentences of the product's own:
  - `DateCtorRule:165`;
  - `ClickHouse:1959, 1963`;
  - `SQLite:468, 1682`;
  - `MariaDb:2058`;
  - MongoDB's `raiseText` at :692, :863, :1093, :1096;
  - `AnsiSqlRenderer`'s fraction and CSV-width raises.

  All of them become kinds in D1.1.
- `testdatagen/TestDataPlanner`'s `Printing {CLEAN, COMPAT}` (S306), chosen in `testplan/TestPlanner:45`: a product class
  branching on a compatibility mode. Ruled: the product writes CLEAN only, and COMPAT's spelling is the edge's
  decorator through the hook (D1.2). Its `representation` (:943) and `seedJson` (:880) are classified the same way.
- `server/TestDataRoutes` (upstream's four test-data routes): to the edge with pure/v1's upstream routes (D1.8).
- H2's `NON_KEYWORDS`, `MODE=LEGACY` and `DEFAULT_NULL_ORDERING=HIGH` as plan data (`H2.java:34-53`). Ruled: measure
  first, after D1.0. That means a run of core's H2 lane and PCT H2 channel A with the product's targets declaring no
  settings; the orchestrator rules per cause.
- `setup/RowLoad` and `setup/CsvSeed`: reading input data (S302). They stay product.
- pure/v1: the upstream routes (`PureV1Api.route` :156-176, the error JSON :1236) and the query store's messages go to
  the edge (D1.8). Lite's own routes stay product: `/api/lite/v1` (:177-180), and `jsonPlan`, `arrowPlan`,
  `jsonAnswer`, `arrowAnswer`.

**The guards (step 1):**
- `ArchitectureTest.noProductClassReachesTheEdge` (G5) already held. It gained a planted violator,
  `theEdgeRuleBitesAPlantedProductClass` (`planner.PlantedEdgeReach`).
- `CompatSpellingGuardTest` registers 178 rows (289 hits): D1.1 30, D1.2 44, D1.3 16, D1.4 37, D1.5 5, D1.6 19, D1.7 111,
  D1.8 27, JUDGE 28, STAYS 1. Its planted violator is `theScanBitesAPlantedSource`.
- D2 ends with no row naming a D step.
