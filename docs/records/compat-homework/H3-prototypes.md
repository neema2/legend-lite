# H3: does the compatibility architecture work end to end? Three prototypes (2026-11-19)

The compatibility homework's H3 (the brief: `docs/records/compat-homework-2026-11-19.md` on
`origin/claude/refactor-code-quality-review-9fqhug`, the user's go of 2026-11-19 "start the homework spike"; its §1 is
the architecture under test). Branch `claude/compat-homework-h3`, made from `origin/main` at `b75d6c73a`. A scratch
prototype: it never merges as it is, and no pin, ceiling, ledger, roster or allowlist was moved (the guards it trips
are listed in §5, not edited). Evidence, probe sources and every input and output are in `h3/` beside this file; the
core change is `h3/core-prototype.diff.txt`.

## 0. The answer

**Yes: the pattern works end to end for all three.** Each prototype reproduced upstream's own answer, measured against
upstream itself rather than recalled. Each also found something the architecture must absorb before the presentation
program is re-cut on it (§4).

| | prototype | checked against | result | took |
|---|---|---|---|---|
| (c) | legend-engine's TDS envelope, written by a presenter AFTER a product run (no style, no mode) | the pure/v1 edge golden (`core/src/test/resources/edge/pure_v1_answers.tsv`) and legend-engine 4.145.0's own answers (the slice-R oracle) | **tds byte-identical**. 9 of 14 golden lines byte-identical; 11 of 14 with the SQL text masked. The other 3 are run-time raises a presenter cannot write | 0 product lines; a 125-line probe over the edge's existing writer |
| (a) | `substring` / `indexOf` as an ENGINE input rewrite over lite's Pure meaning (ASSUMED by the prototype: whether LITE follows Pure or legend-engine is the user's open ruling R4; §3.1 says what moves under each choice) | legend-engine 4.145.0 run on the same requests, H2 and DuckDB, 25 requests | **H2 25 of 25, DuckDB 24 of 25** (the miss: a negative length). It needs a TARGET-AWARE rewrite: target-blind, DuckDB scores 17 of 25 | core +21/-26 lines (Scalars); about 30 Pure lines per target; a JSON rename |
| (b) | `toString` of a Float inside a filter, as a PURE input rewrite calling ONE named core capability | legend-pure 5.99.0's own `CompiledSupport.primitiveToString`, called from its jar, on 26 Floats (PCT's literals among them) | **H2 26 of 26, DuckDB 25 of 26** rows kept, where legend-pure keeps 26 and the product alone keeps 8. The miss: the smallest subnormal | core +5 (Pure.java) +66 (Scalars), 0 dialect lines; 2 Pure functions |

The four things the architecture must absorb, ranked (§4):

1. Upstream's meaning is often **its database's**, so an input rewrite must know the target. Some of it has no sane
   rewrite and is refused by name (a DuckDB constant-folding quirk, a negative length).
2. The ENGINE audience is **whole models**, not only request lambdas. The corpus's 15 newly failing tests need the
   adapter to rewrite a parsed model.
3. A capability must be **one MIR node a dialect writes once**. Built as a lowering tree, it cost 350 times the SQL.
4. The presenter reads **the plan too**. Some envelope fields are functions of upstream's SQL: a scalar's column
   label, a raise's message as the database's error text over upstream's statement. These need a class of their own:
   declared not promised, or refused.
5. **The product leans on today's meaning in two places.** If R4 rules that LITE follows Pure, these must be separated
   first:
   - lite's own Cypher translator writes Pure in legend-engine's meaning;
   - a `###Relational` dynaFunction `substring` shares the Pure function's rule.

## 1. Setup

- **Upstream's answers.**
  - legend-engine: 4.145.0 through the engine oracle of slice R (`claude/presentation-r`, `planoracle.GoldenRunMain`):
    planned by its PlanGenerator, the plan's JSON executed by its PlanExecutor. On H2 it runs legend-engine's patched
    2.1.214 from the goldens' LocalH2 seed. On DuckDB it runs 1.3.0, against a file seeded from that seed
    (`planoracle.SeedDuckDbMain`).
  - legend-pure: 5.99.0's `CompiledSupport.primitiveToString(double)`, called reflectively from its jar
    (`h3/probes/PurePrint.java.txt`).
  - PCT's expected values: lite's own PCT suite (`//pct:pct_duckdb_essential`).
  - Sources read where the spec is the code: the pinned `@legend_engine_src` and `@legend_pure_src`.
- **Lite.** H2 2.5.252, DuckDB 1.5.6. The probes are scratch Java compiled with `javac` against the jars `bazel build`
  makes (the `//core:edge_pure_v1` closure). They run the PRODUCT door,
  `Execution.runOnProjectDatabases(typed, null, Output.Rows(Collector))`: no plan style, no feature flag. No Bazel
  target, no `EDGE_TEST_SRCS` entry, no `//core:edge` visibility and no jars-table user was added; the probes' sources
  are in `h3/probes/` as `.txt`.
- **The input adapter** in (a) and (b) is the shape the architecture asks for:
  - a rewrite of the request's protocol JSON (pure/v1's own lambda form, made by lite's grammar-to-JSON), each call of
    upstream's name renamed to its compatibility twin (`RowsProbe.rename`);
  - plus a compatibility Pure source appended to the model.

  The core is never told which audience it serves.

## 2. (c) legend-engine's TDS envelope from a presenter after results

**What.** Each pure/v1 golden request is typed once (`Compiler.query`), planned and run on the product door, and its
typed rows collected (`result.Collector`). Only then is the result presented as legend-engine's envelope by the edge's
existing writer (`EngineAnswers.engine` over `HostAnswers.facts(typed)`), wrapped in legend-engine's error envelope
(`EngineEnvelope.answer` with `PureMessages`), as `PureV1Routes.execute` wraps it. The writer is fed `execution`,
`planned(plan)`, `columns`, one batch and `end`. Each answer is written as the net writes it (compact JSON, ids masked,
newlines escaped) and compared byte for byte with the golden (`h3/probes/PresentProbe.java.txt`;
`h3/c/run-cells.out`, `h3/c/run-raises.out`).

| request | against the golden | why |
|---|---|---|
| `tds` | **byte-identical** | the brief's target |
| `value` (500), `cell.dateTimeCoarse`, `cell.dateTimeSubMicro`, `cell.emptyIf`, `cell.strictDate`, `cell.empty` (500), `raise.atOutOfBounds` (500), `raise.toOneOverMany` (500) | byte-identical | 8 more |
| `cell.float`, `cell.floatWhole` | differ in `activities[].sql` only | the shipped route plans in the PURE_V1 style, which spells the SQL its way: `CAST(1.0 AS DOUBLE)` where the product writes `DOUBLE PRECISION`; `CAST(2.0 * 3.0 ...)` where it writes `CAST(2.0 ...) * CAST(3.0 ...)` |
| `raise.divideByZero`, `raise.parseInteger` | differ | the golden's message is H2's own error text, holding the statement: `Division by zero: "CAST(2023.0 AS DOUBLE PRECISION)"; SQL statement: SELECT ... [22012-252]`. The style removed the product's guard and let H2 fail. The product raises a kind (`Raised: DIVIDE_BY_ZERO`) |
| `raise.sqrtOfNegative` | differs | the golden is 200 with a null cell: the style lets H2 compute `sqrt(-2.0)`, as legend-engine does. The product raises `SQRT_OF_NEGATIVE`. This is a meaning difference |

Totals: 9 of 14 byte-identical; 11 of 14 with the SQL masked.

**Against legend-engine itself** (the slice-R oracle's pinned answers, `tools/engine-runner/src/test/resources/pure_v1/engine_h2_answers.tsv`
on `claude/presentation-r`, SQL masked): 4 of the 9 non-raise lines are identical. The other 5 are slice R's known
answer-level differences, unchanged by moving presentation after the run:
- `meta::pure::precisePrimitives::Int`/`Varchar` column types;
- `VARCHAR(8192)` for a computed String;
- the scalar `value` (legend-engine 200 `dataTypeBuilder`; lite refuses);
- `cell.float` 0.33333333 (legend-engine's DECFLOAT on H2 2.x);
- `cell.empty`'s words (both refuse).

**What it took.** No product or edge line changed. The presenter already exists: `EngineAnswers` 134 lines,
`PureV1Writer` 307, `HostAnswers` 433. The TDS path needs no SQL-built envelope (`EngineResultEnvelopes` is the
style's facet for class and serialized roots only).

**What did not fit.**

1. The presenter's input is **the typed result, the plan and the typed query's facts**, not the typed result alone:
   - `activities[].sql` is the plan's statement;
   - `builder.columns[].relationalType` comes from the query's result type.

   That fits the architecture if the plan is data. The architecture's §1.3 should say so.
2. **The scalar envelope carries a fact of upstream's SQL.** legend-engine answers `size()` with
   `"columns":["COUNT(*)"]`, H2's JDBC label of legend-engine's own `select count(*)`. A presenter over typed results
   can only write it by knowing legend-engine's SQL shape, which rule 4 says is never promised. Today lite refuses
   ("a SCALAR result's serialization is unprobed").
3. **A raise's words are, for the ENGINE audience on H2, the database's error text over upstream's statement**. That is
   not reproducible without promising SQL text. The product's raise carries its kind and site
   (`exec.RaisedErrors.Raised`: `kind()`, `site()`), which a presenter can word in legend-pure's sentence (the edge's
   `RaisedWords` does, today at planning). legend-engine's sentence for these is the database's.
4. **Three of the golden's lines are the style changing the PLAN, not the presentation:**
   - SQL spellings (2 lines; rule 4 deletes them, and the golden moves on `activities[].sql` only);
   - a removed guard (`sqrt` of a negative: a meaning rewrite, the input side's);
   - the database's own failure text (item 3).

## 3. (a) `substring` and `indexOf`: an ENGINE input rewrite over lite's Pure meaning

**What the prototype ASSUMED, not a decision.** Whether LITE's own meaning of `substring` and `indexOf` follows
legend-pure or legend-engine is the user's open ruling R4 (as the orchestrator relayed it, 2026-11-19). The prototype
assumed Pure's, because the brief asks for an ENGINE rewrite over it. §3.1 says what moves under each choice.

**The core's meaning under that assumption, Pure's** (registers S130, S134; `h3/core-prototype.diff.txt`, Scalars
+21/-26):
- `substring` is always the corrected rule (0-based, end-exclusive; `FeatureRules.CORRECTED_SUBSTRING`), whatever the
  flag says;
- string `indexOf(s, t)` is `STRPOS - 1` (0-based, -1 on a miss: exact for both);
- `indexOf(s, t, from)` is Java's `indexOf(t, from)` (from below 0 searches from 0, -1 on a miss).

**The adapter.**
- `substring` and `indexOf` are renamed in the request JSON to `meta::legend::compat::engine::*`.
- The compatibility source (`h3/a/compat-engine-h2.pure`, `compat-engine-duckdb.pure`) writes legend-engine's meaning
  as lite programs.
- legend-engine without `CORRECT_SQL_SUBSTRING_INDEXING` sends Pure's arguments to the database's
  `SUBSTRING(s, start, length)` unshifted (`pureToSQLQuery.pure` processSubstring).
- Its `indexOf` is H2's `LOCATE(t, s)` / DuckDB's `instr(s, t)`: 1-based, 0 on a miss
  (`h2Extension2_1_214.pure:231`, `duckdbExtension.pure:237`).
- Its 3-argument `indexOf` has no SQL translation.
- The compiler chose by overload: the String overload for strings, the generic `T[*]` overload (the core's list
  `indexOf`) for `[1, 2, 3]->indexOf(2)`.

**Checked against legend-engine itself.** 25 requests over the trades seed's four desks (`Credit`, `Equity`, `FX`,
`Rates`): literal and computed starts and ends, starts of 0, negative starts, starts before the first character, an end
past the string, an end before the start, a negative length, 2- and 3-argument forms, the list form, nested calls
(`h3/a/battery.tsv`, `battery2.tsv`). Each was run by legend-engine (`h3/a/engine*_{h2,duckdb}.jsonl`) and by lite in
each variant (`h3/a/lite_*`); compared by `h3/probes/compare.py.txt`.

| lite variant | H2: agree of 25 | DuckDB: agree of 25 |
|---|---|---|
| today's main (the verbatim rule; DuckDB's start clamp) | 24 (the miss: 3-argument `indexOf`, which lite answers and legend-engine refuses) | 16 |
| Pure meaning, no adapter (first 18 requests) | 6 of 18 | 4 of 18 |
| Pure meaning + a target-blind rewrite (`compat-engine.pure`) | 25 | 17 |
| **Pure meaning + the target-aware rewrite** | **25** (23 rows, 2 joint refusals) | **24** (23 rows, 1 joint refusal; the miss `substring(2, -1)`) |

Today's main is already wrong against legend-engine on DuckDB in 9 of 25. Every one is a start of 0 or below, or the
3-argument form; the clamp in item 4 below causes most of them.

**Why the rewrite must know the target** (`h3/a/grid/`, a 312-cell grid of `SUBSTRING(s, start, length)`, start -8..3,
length -1..10, and the 2-argument form, on `Credit` and `FX`):
- legend-engine's answer is the database's SUBSTRING, and the databases differ:
  - H2 2.1.214 and H2 2.5.252 agree on all 312 cells; DuckDB 1.3.0 and 1.5.6 agree on all 312.
  - H2 and DuckDB differ on 58 of the cells for a table column.
- At **start 0** H2 starts at 1 and DuckDB at 0: `substring(0, 2)` is `Cr` on H2, `C` on DuckDB.
- A **negative length** is empty on H2; DuckDB counts it backwards (`substring(2, -1)` is `C`).
- On DuckDB a **negative start before the first character** keeps its length from position 1 for a column value
  (`substring(desk, -7, 2)` is `Cr`). Its constant folder keeps the window for a literal (`substring('Credit', -7, 2)`
  is `C`), on both DuckDB versions: one database, two answers.

The rewrite in `compat-engine-<target>.pure` writes each target's window as Pure's 0-based, end-exclusive substring of
valid bounds, so lite's own clamps never see it. It reproduces every cell but the negative length, and DuckDB's folder
case for a literal string, which no request here hit. Those two are for the adapter to refuse by name.

**What broke** (the core change, measured on the scratch branch):

- PCT essential on DuckDB: **342 of 345**. The 3 are S134's `indexOf` known failures (`testSimple`, `testFromIndex`,
  `testIndexOfOneElement`), which now PASS against PCT's expected values. The runner reports them as "expected an error
  but the test succeeded". Their ledger rows would move; not moved.
- The relational corpus on H2 (`//spec:corpus_h2`): **15 tests newly fail**, and one unordered-register line moves.
  - These are legend-engine's own programs, run without the flag (S130), expecting its uncorrected substring and
    1-based indexOf:
    - `testFilterUsingSubstringFunction` (`substring(1, 5) == 'John'`);
    - `testFilterUsingParseDecimalFunction`;
    - the six `propertyfunc` tests (a mapped class's property function);
    - `testSubstringIndexingCorrectedByFeatureFlag`;
    - `testModelConnectionAgg`, `testModelConnectionMultipleAgg`;
    - `sqlFunction::indexOf::testProject`;
    - `testFilterUsingParseIntegerFunction`;
    - two `toSQLString` tests.
  - Some assert legend-engine's SQL text, which rule 4 does not promise.
  - The corpus's programs are whole MODELS (library functions, mapping property functions, test bodies), so the
    ENGINE adapter would have to rewrite a parsed model, not a request's lambda. Not prototyped (time box).
- core's behaviour suite (`//core:core_tests`, (a) and (b) together): **12 tests fail in 5 of 28 targets**.
  - 10 pin the old meaning:
    - `TypeInferenceIntegrationTest`'s `testSubstringStart`, `testStringIndexOf`, `testStringIndexOfSimple`,
      `testStringIndexOfFromIndex`, `testIndexOfOneElement` and `testSortWithKeyFunction` (a substring sort key);
    - `ExtendCheckerTest`'s `indexOf()`;
    - `ValueSortComparatorTest`'s "1-based substring keys";
    - `SubstrIndexingTest`, which pins "substring only under the flag" itself;
    - the DynaFunction property-mapping test "substring() over a joined column".
  - 1 is lite's own Cypher translator (`CypherIslandIntegrationTest.aMarkedListOfTemporalsReadsItsElementsBackOrIsReadWhole`).
  - 1 is (b)'s catalog guard (§4).
  - Two of these are findings in their own right (items 5 and 6 below).

**What did not fit.**

1. The target-blind rewrite is wrong on DuckDB. The adapter reads the target's database type from the request's
   runtime, which the request carries; that is knowledge, not a mode.
2. The JSON rename is untyped. It is safe only because the compatibility functions overload every upstream overload.
   The 3-argument `indexOf` came back as "no overload of 'meta::legend::compat::engine::indexOf' accepts 3
   argument(s)": a refusal, but not by name. A real adapter refuses it in upstream's terms ("no SQL translation exists
   for indexOf_String_1__String_1__Integer_1__Integer_1_").
3. A request that sets `CORRECT_SQL_SUBSTRING_INDEXING` would select no substring rewrite. The flag becomes the
   adapter's input, never the core's. Not exercised.
4. **Pre-existing, the product's parity (G2):** the product's DuckDB `SubstringClamp` lifts a negative start to 1
   while H2 counts from the end. The same lite program answers differently on the two databases (`substring(-1, 2)`:
   `t` on H2, `Cr` on DuckDB). Under Pure's meaning a negative start is invalid: legend-pure raises
   (`String.substring`); lite raises on neither. A meaning question for the core.
5. **The product itself writes Pure in legend-engine's meaning.** Lite's Cypher translator emits `substring` calls with
   a 1-based start and a length (`protocol/cypher/CypherToPure.java:11657` says so). Under Pure's meaning in the core,
   that translated program reads the wrong characters, and DuckDB fails on the text it reads ("invalid timestamp field
   format"). A frontend that writes Pure must write Pure's meaning; it is an input adapter of its own, inside the
   product.
6. **A `###Relational` dynaFunction shares the Pure function's rule.** The mapping DSL's `substring(col, ...)` is SQL's
   function in every audience (upstream's DSL meaning, 1-based). Lite lowers it through the same rule as Pure's
   `substring`, so giving the Pure function Pure's meaning moved the dynaFunction too. The two need separate rules
   before the core's meaning can change.

### 3.1 What moves under each choice of R4

Either way, S130's flag leaves the core and becomes an adapter's rewrite: the architecture has no mode.

| | LITE follows legend-pure (the prototype) | LITE follows legend-engine (today's main) |
|---|---|---|
| the core | Scalars +21/-26: substring always corrected, indexOf 0-based | no change |
| who gets a rewrite | the ENGINE audience (pure/v1, the corpus): §3's target-aware compat functions | the PURE audience (PCT, legend-pure's words): `substring(s, a, b)` becomes legend-engine's `substring(s, a + 1, b - a)`, and `indexOf` becomes legend-engine's minus 1. By algebra this is exact for Pure-valid arguments; not measured here |
| PCT | 3 known failures (S134) pass in the core itself | PCT runs through the PURE rewrite; S134's 3 would pass there (not measured) |
| the corpus | 15 tests need the ENGINE rewrite at the MODEL level (§3, "What broke") | unchanged |
| core's tests | 10 pin the old meaning and move; lite's Cypher translator and the `###Relational` dynaFunction must first be split from the Pure function's rule (items 5, 6) | unchanged |
| legend-engine on DuckDB | the target-aware rewrite (24 of 25) | still needed: today's main is 16 of 25 on DuckDB. "legend-engine's meaning" is per database (start 0, a negative length, DuckDB's folder vs its runtime), so the product cannot simply follow legend-engine; the DuckDB clamp (item 4) answers neither database's way |

## 4. (b) a Float's `toString` inside a filter, through one named capability

**The capability** (`h3/core-prototype.diff.txt`, Pure.java +5, Scalars +66, no dialect line):
- `native function meta::legend::lite::pureFloatText(f: Float[1]): String[1]`, a Float's text as legend-pure prints it.
- That print, read from the pinned source: `CompiledSupport.primitiveToString(double)`, which is `DecimalFormat("0.0")`
  with up to 340 fraction digits, and `0.0` for zero of either sign.
- Lowered as lite's shortest round-trip text (`SqlFn.FLOAT_SHORTEST_TEXT`, which every dialect already spells; plain
  inside its windows). Outside them the database's exponent form (`1.0E-20`, `1e-20`, `1.7976931348623157e+308`) is
  expanded in SQL to plain digits, and zero is `0.0`.
- Only the PURE adapter's rewrite calls it: no mode, no hook.

**The adapter.** `toString` is renamed to `meta::legend::compat::pure::toString`, overloaded on `Float[1]` (the
capability) and `Any[1]` (the core's own toString, legend-pure's already) (`h3/b/compat-pure.pure`).

**Checked against legend-pure's own print.** 26 Floats (`h3/b/pure_print.tsv`, each beside legend-pure's text from its
jar): PCT's `toString` literals (3.14, 3.14159, 17.0, 0.01, 134.21e6, 1.3421e-8), 2.5E7 (the brief's example; legend-pure
prints `25000000.0`), 0.1, 1/3, -1.5, 0.0, -0.0, 1e15 to 1e22, 1e-5 to 1e-20, 2^53+1, MAX and MIN. Each was seeded as a
row with its legend-pure text, and the filter `filter(r|$r.f->toString() == $r.expected)` was run
(`h3/b/requests.tsv`).

| | H2 | DuckDB |
|---|---|---|
| the product alone (six decimals, trimmed, the D_BRIEF ruling) | 8 of 26 kept | 8 of 26 |
| **with the PURE rewrite and the capability** | **26 of 26** | **25 of 26** (MIN, 4.9E-324: DuckDB's shortest digits are `5e-324`, Java's `4.9E-324`) |
| a literal inside the filter (`2.5E7->toString() == '25000000.0'`, `1.3421e-8 ... '0.000000013421'`) | kept, kept | kept, kept |
| an Integer's toString through the `Any` overload | unchanged | unchanged |

**What did not fit.**

1. **SQL size (G3):** one `toString` grew the answer's SQL from 282 to 99,560 characters on H2, and from 213 to
   94,154 on DuckDB. The plain expansion is a lowering tree that repeats the shortest-text subtree, which each dialect
   spells long, and nesting multiplies it. The capability must be ONE MIR node the dialect writes once (as
   `FLOAT_SHORTEST_TEXT` itself is), not a tree in the lowering.
2. **H2 folds every branch of a constant CASE.** An unguarded cast in a branch it never takes raised for a literal
   ("Data conversion error converting 25000000.0"). Fixed in the prototype by guarding the exponent's text; a dialect
   spelling (item 1) would own it.
3. **The catalog's guard:** a new lite native outside the verified sets fails
   `NativeCatalogGovernanceTest.everyLiteInternalNativeIsNamedAndVerified`. Accounting for it moves a ceiling
   (`LITE_SURFACE <= 9` or `INTERNAL_DESUGAR <= 38`), which is the user's word. Not moved.
4. **The ENGINE audience's toString of a Float** is the database's cast (`cast(%s as varchar)`, both extensions), a
   second capability ("the database's own text of a double") for the ENGINE adapter. Not prototyped; the brief asks
   for the PURE one.

## 5. The guards and pins the prototypes trip (none moved)

| guard | moved by | count |
|---|---|---|
| PCT's known-failure ledger (S134's indexOf rows) | (a) | 3 tests now pass |
| the corpus's H2 fail roster, unordered register | (a) | 15 newly failing, 1 line |
| `NativeCatalogGovernanceTest` | (b) | 1 unaccounted native |
| core's behaviour suite (`//core:core_tests`) | (a) and (b) | 12 tests in 5 of 28 targets (§3) |
| the pure/v1 edge golden, under rule 4 (the presenter without the style) | (c) | 2 lines (`activities[].sql`); 3 raise lines move with the input side |

## 6. Files and lines

| | file | lines |
|---|---|---|
| core | `core/src/main/java/com/legend/lowering/Scalars.java` | +87 / -26 ((a) +21/-26, (b) +66) |
| core | `core/src/main/java/com/legend/builtin/Pure.java` | +5 ((b)) |
| adapter, (a) | `h3/a/compat-engine-h2.pure`, `compat-engine-duckdb.pure` (and the target-blind `compat-engine.pure`) | 32, 33 (26) |
| adapter, (b) | `h3/b/compat-pure.pure` | 15 |
| adapter, the rename | `RowsProbe.rename` | 15 |
| probes | `h3/probes/PresentProbe.java.txt` (c), `RowsProbe.java.txt` (a, b), `compare.py.txt`, `Grid.java.txt`, `GridCol.java.txt`, `Q.java.txt`, `PurePrint.java.txt`, `Seed.java.txt` | 125, 90, 30, 22, 22, 14, 11, 6 |

## 7. Not done (time box)

- The model-level ENGINE adapter, and the corpus run through it. The corpus on DuckDB.
- PCT's other suites and lanes with the core change.
- (b) on the other databases. The ENGINE audience's Float text (§4 item 4).

## 8. To rerun

- Lite's jars: `bazel build //core:edge_pure_v1` (its params file is the class path; take external jars from the output
  base, since the execroot's links are pruned per build).
- The engine oracle: slice R's runner (`claude/presentation-r`, `tools/engine-runner`):
  `planoracle.GoldenRunMain --requests=<tsv> --model=<pure> --out=<jsonl> [--duckdb=<file>]`.
- Then:
  - `java -cp <probes>:<lite> h3.PresentProbe <model> <requests> <golden> <engine answers>` for (c);
  - `h3.RowsProbe <model> <requests> none|engine|pure [<compat.pure>] [<duckdb file>]` for (a) and (b);
  - `compare.py <engine jsonl> <lite tsv>`.
