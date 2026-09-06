# Compiler architecture audit — 2026-09-06

> **Scope.** The compilation pipeline as architecture, not as correctness: layering,
> concept ownership, duplication, fail-loud discipline, overfitting, and the
> sustainability of the corpus burn-down. Correctness findings appear only where
> a design failure produces one.
>
> **Baseline.** `66bee2de` (Batch 110 / L5). Corpus 2,463 pass / 2,575 runnable.
> Production 190,881 LOC in 630 files; test 116,701 LOC in 266 files.
>
> **Method.** Eleven adversarial reviewers, each briefed as a hostile expert Java /
> SQL / Legend-Pure developer, one per stage plus five cross-cutting dimensions.
> **Documentation was banned as evidence throughout** — only code and git history
> count, and an elaborate justifying comment was treated as a smell to investigate,
> not a defence. Every headline claim below was re-verified by the orchestrator
> against the source before being recorded here; claims that failed that check were
> dropped and are listed in §9.

---

## 1. Verdict

The hostile thesis — *"a clean-room rewrite of Legend Engine inevitably becomes an
unmaintainable pile of special cases"* — is **half proven, and the half that failed
matters as much as the half that held.**

**What failed to hold.** The code is not rotting into a decision tree over test
shapes. Measured across three weeks and +29% LOC: `if`-density *fell* (53.1 → 52.1
per KLOC), conjunctive-condition density rose only 8.8% (2.73 → 2.97 per KLOC), and
in the resolver — the worst-rated package — it *fell* (4.62 → 4.30). There are
**zero** corpus model, table, or column literals in any production guard. Only 11
methods in the whole tree take ≥3 boolean parameters. Loud walls grew 462 → 570.
Textual duplication is **0.2%**. The throw:catch ratio is **1,317 : 171**.

**What held.** The marginal cost of a corpus test has risen ~15×, from **6 LOC/test**
at batches 21-40 to **89-115 LOC/test** at batches 81-110, with files-touched-per-test
going 0.5 → 8.8. The cause is not code rot; it is that **the generalization ratchet
was switched off exactly when it was most needed** (§3).

**The single most useful sentence in this audit** is the concept-ownership reviewer's:
*this project can build owners, but it cannot finish migrations to them.* Eight types
carry a "THE ONE OWNER / lives HERE and nowhere else" claim in their own javadoc that
the code contradicts. That is the disease, and it is treatable.

**The finding that outranks everything above is structural: the scored path and the
product path are different code.** The corpus's platform lane runs
`Compiler.resolveQuery → executeResolved → StatementExecutor`; the HTTP surface runs
`Compiler.executeWire`, which contains **zero references to `StatementExecutor`** and
also skips `resolveQuery`'s three desugar passes. So production executes a *shorter*
pipeline than the 2,450 platform-scored tests do, and the difference is not covered by
either. Compounding it, the corpus runner sets `legend.exec.engineScanOrder`, adding a
SQL rewriter the shipping default does not enable. See §8a C1-C3.

**Context that reframes the burn-down, and which an earlier draft of this audit got
wrong:** `WholeTestFlip` is not test scaffolding — it is the harness-deletion migration
itself, on by default, and **2,450 of 2,573 tests (95%) already score from the platform's
own assert verdicts**, with 123 counted fallbacks remaining. The migration ratchet moves
in lockstep with the corpus ratchet, so the harness-deletion program and the corpus
burn-down are one program. That is a genuine and substantial architectural achievement,
and it materially softens the §3 cost-curve reading: the recent batches are not only
buying corpus passes, they are retiring the harness.

---

## 2. The one verified wrong-rows defect

Three implementations of the milestone window predicate; two different answers.

| Site | `inclusive` / `thruIsInclusive` = true |
|---|---|
| `resolver/TemporalFrame.java:865-881` | `from <  d AND thru >= d` |
| `testdatagen/TestDataGenerator.java:1791-1794` | `from <= d AND thru >= d` |
| `testdatagen/TestDataGenerator.java:684-691` | `from <= d AND thru >= d` (a *second* emitter in the same class) |

On the boundary date `d == from` the resolver **excludes** the row and the test-data
generator **includes** it. Sites 2 and 3 additionally differ from each other in date
literal spacing (`DATE'…'` vs `DATE '…'`) and in whether identifiers pass through
`q()`. Both emit raw SQL text from `testdatagen/`, violating the one-render-entry-point
rule.

**Fix:** delete both `testdatagen` emitters; route through `TemporalFrame`. This also
removes the 11 type-spelling divergences in §4.

---

## 3. Sustainability: the ratchet, not the architecture

Consolidation cadence, whole repo (orchestrator's measurement; the overfitting
reviewer's narrower query gives 8.8% / 9.3% / 2.5% — same collapse):

| Month | Commits | Consolidation | Share |
|---|---|---|---|
| 2026-07 | 996 | 208 | **20%** |
| 2026-08 | 1,094 | 324 | **29%** |
| 2026-09 (the 110-batch burn) | 180 | 11 | **6%** |

Within the batch window: 106 batch commits, **one** dedicated cleanup — a
consolidation:patch ratio of **1:106**. July and August's refactor months are what
made batches 26-40 cost 5 LOC/test; one of them (`dd1023dd`) flipped **76 tests for
22 production LOC**, another deleted 411 lines and flipped 36.

The ratchet still works when used. Batch 91 introduced a literal guard
`pa.property().equals("csv")`; Batch 100 — the single cleanup — replaced it with
`PlatformTypes.TDS_CSV_PROPERTY` and hoisted five ad-hoc rules to single owners across
16 files for +225/−136.

Consolidation rate by hot file shows where the effort is *not* going:

| File | Commits | Consolidating |
|---|---|---|
| `lowering/Scalars.java` | 208 | 33% |
| `lowering/Lowerer.java` | 270 | 31% |
| `compiler/spec/Typer.java` | 177 | 28% |
| `normalizer/MappingNormalizer.java` | 159 | 23% |
| `resolver/StoreResolver.java` | 281 | 21% |
| `resolver/Substitution.java` | 138 | 18% |
| `normalizer/UnionSynthesis.java` | 54 | 18% |
| `resolver/GraphEmission.java` | 93 | **11%** |

`MappingNormalizer` got its consolidating pass and has held a ±60-line band for eight
weeks. `UnionSynthesis` never had one: 1,137 → 2,970 lines, largest lifetime shrink
**12 lines**, steepest growth in the last three weeks.

**Projected cost of the remaining 124 tests: 12,800-25,000 production LOC over 3-8
days** (+7% to +13% of the codebase). Affordable — but the curve has no upper bound in
the data without the ratchet.

---

## 4. Concept ownership — the primary structural failure

Eight types assert exclusive ownership in javadoc and are contradicted by code:
`Temporal`, `TemporalContext`, `SyntheticHeads.JoinIdentity`, `ClassMapping`,
`LiteralSpelling`, `Ddl.dataTypeToSqlText`, `DateFormats`, `SqlDialect`.
`Temporal.isGeneratedDateProperty` — whose javadoc says the spellings live there once —
has **exactly one caller**; nine other sites reimplement it inline.

**Divergent duplicate authority (the dangerous class):**

| Concept | Divergence |
|---|---|
| Milestone window predicate | 3 impls, 2 answers (§2) |
| `RelationalDataType` → SQL type name | `exec/Ddl.java` vs `TestDataGenerator.duckType`: **11 disagreements**. `Varchar(n)` keeps size in one, drops it in the other; `NUMERIC` vs `DECIMAL`; **4 types `Ddl` throws on ("no DDL spelling") the other maps silently to `VARCHAR`**. Both carry the identical comment *"EXPLICIT per variant so a new variant is a compile error, never a silent VARCHAR."* |
| "nullable" | **4 definitions**. `[0..*]` is optional in `Render`/`ScalarValueReads`/`MixedEncoding`, not optional in `NullSemantics`; a `Var` multiplicity is nullable in `PureSql`, not in `NullSemantics` |
| Date-format vocabulary | `lowering/DateFormats.java`: `hh` is 12-hour at `:38`, 24-hour at `:99` — in one file, under a javadoc claiming *"parses ONCE, here"* |
| Schema qualification | **25 `"default".equals` sites in 14 files**, ≥3 different answers. `ScanRelations:1388` guards `schema == null`; `StatementExecutor:2806` does not, and emits the literal `"null.T"` |
| FQN splitting | `Protocol.java:2922` claims in bold *"the only place in legend-lite that splits an FQN"*; **56 raw `lastIndexOf("::")` sites in 40 files** disagree, quote-unawarely |
| `Multiplicity` | Two records, different constructor invariants (`protocol` permits `[2..1]`, `compiler/element/type` rejects it). Guard `requireValidBounds` is called at **3 sites, all in `checkClass`**; `checkFunction` omits it, so `function m::f(x: String[2..1])` still throws a bare `IllegalArgumentException` with no phase and no position |
| "carrier" | **542 occurrences, ≥4 unrelated senses, ~3,730 LOC**. The "FIFTH carrier" in `ebc7356f` is *test-suite source carriage*, unrelated to `CarrierStrategies` |

**A dead guess-loop:** `TemporalFrame.java:503,646,713,898` probe `subProp + "_"` then
`subProp + "_nav_"`. `_nav_` has **zero producers anywhere in main** — half of every
loop is dead code that turns a resolver bug into a silent `null`.

**Error provenance dies at phase F.** `SourceInfo` occurrences: resolver 0, sql 0,
exec 0, plan 0, model 0, normalizer 0, lowering 2, compiler 3. `lowering/` throws
**96 `IllegalStateException` against 4 phase-carrying exceptions**.

---

## 5. Fail-loud discipline

Invariant 4 is the most-cited rule in the codebase and is `[CONVENTION]` — nothing
enforces it. Overall discipline is genuinely good (1,317 throws : 171 catches; 3 bare
`IllegalStateException("unreachable")`), but the backend leaks:

- **129 `default →` arms** across `lowering`/`normalizer`/`sql`; only **25 (19%) throw**.
- **15 are `default → { }` block arms and none throws** — invisible to
  `ErrorShapeGuardrailTest`'s `default -> "` regex.
- **114 `default →` arms sit in switches over sealed types.** 23 throw (fine); 23 are
  legitimate traversal/delegation defaults (`mapChildren`, `super.call`); 20 are blocks
  needing review; and **48 answer a sealed switch with a fabricated data value**, which
  defeats javac exhaustiveness for no benefit — over a sealed type there are no
  unmatched kinds.

**The greppable heuristic worth institutionalising:** a switch or ternary containing
**both a `throw` arm and a `default →`/`getOrDefault`/`orElse` arm**. The throw arm is
the project's own proof the question deserves an error. Specimen —
`normalizer/MappingNormalizer.java:2619-2622`:

```java
String slot = targetIfMapped != null
        ? pipeline.navSlotByProp.getOrDefault(j.propertyName(), j.propertyName())  // guesses
        : JoinChainEmission.slotFor(pipeline, j.joins());                          // throws
```

`slotFor` throws *"join chain … was never emitted on this pipeline"*, and its javadoc
names the bug the sibling reinstates: *"the old flattened-name fallback let the terminal
read silently bind through ANOTHER chain's slot (audit 18 finding 2)."* A coarse
proximity scan finds **177 candidate sites**, concentrated in `StoreResolver` (8),
`ProtocolEmitter` (7), `ScanRelations` (7), `InferenceKernel` (7), `TemporalFrame` (6).

Three more of the same shape, each contradicted by sibling code:
`sql/SqlExpr.java:366-373` (typeless column on a schema miss; `Fold.java:864` throws),
`lowering/Lowerer.java:1416-1421` (element type defaults to `String` on **both**
branches), `lowering/Render.java:459-461` (`String.valueOf(null)` → the literal `"null"`
fed to the type mapper; `PctTdsWrap.java:78` forbids exactly this by name).

`PctTdsWrap.java:201-210` contains the sharpest self-observation in the tree: they found
a banned default **disguised as a ternary** — *"this was `: \"VARCHAR\"` … invisible to
the fallback guard."* The two findings above are that same disguised shape, unfixed.

---

## 6. The guards are being satisfied, not obeyed

`CodeShapeGuardrailTest` sets `METHOD_LIMIT = 250` and `FILE_LIMIT = 3500`, **never
raised since introduction**, with an **empty** method allowlist and a single dated file
exemption. That is real discipline. It is also now shaping the code rather than the
design:

- File-size distribution: **nine files in 3400-3499, then nothing until 3000-3099.**
  `MappingNormalizer` 3509 (exempted at 3510 — one line of headroom), `Scalars` 3499,
  `Lowerer` 3497, `SpecParser` 3496.
- `Typer.java` hit **exactly 3500**, had 84 lines extracted in Batch 102 → 3424, regrew
  to 3432.
- `StaticFold.evalCall` is **exactly 250 lines**.
- `Lowerer`'s scalar dispatch is one ~800-line switch split into 220/246/58/246-line
  methods that `default →` into each other — satisfying the ceiling while defeating
  javac exhaustiveness at every seam.
- `UnionSynthesis` and `JoinChainEmission` were split out of `MappingNormalizer`
  *purely to fit the cap*; `MappingNormalizer.java:126-128` says so: *"split out purely
  because of its size, **not because it is a different stage**."*

**Guard blind spots:**

| Guard | Blind to |
|---|---|
| `noStaticMutableState` (`static (?!final )`) | **20 `static final ThreadLocal` fields** — the javadoc says "banned outright — NO allowlist" |
| mutable-field regex `[\w.<>\[\], ?]+` (no `@`) | any `@Nullable`-annotated field (3 of `Lowerer`'s 7) |
| `staticCollectionStateIsImmutableOrRegistered` (filters `Map`/`Collection`) | every `ThreadLocal` |
| `ObservabilityGuardrailTest` `STRING_DISPATCH_SITES=87`, saturated | `equalsIgnoreCase`, `SET.contains(x.name())`, flipped-operand `"lit".equals(x.name())` — **52 further sites** |
| `ErrorShapeGuardrailTest` (`default -> "`) | block arms, ternaries, and `default -> false/true` over **sealed** types (correctly exempted for open predicates; the sealed case has no unmatched kinds) |
| `ArchitectureTest` (34 rules) | no rule forbids `resolver → lowering`; 19 such references exist. ArchUnit reads bytecode, so this is a **missing rule**, not a bypass |

---

## 7. Verification that does not verify

- **`testAggregationAware()`** (`RelationalMappingIntegrationTest.java:3331-3333`) is an
  **entirely empty method body** — one comment, no statements — carrying `@Test`, with
  the `@Disabled` removed on the justification *"the audit flagged this GAP stale
  (aggregationAware family scores 13/13)"*. It passes vacuously forever. **Zero** tests
  anywhere reference `AggregationAwareRouting` or `chooseSet`.
  Two more enabled empty bodies: `ElementParserTest.missingSemicolonAfterPropertyFails()`
  and `functionBodyParsesNestedLambdaBraces()` — both named for assertions they do not
  make. (12 further empty bodies are correctly `@Disabled`; the pattern is narrow, not
  systemic — 3 of 2,129 scanned.)
- **Native signatures are not verified against upstream.**
  `NativeFunctionTest.catalogMatchesTheGoldenFile` compares `Pure.all()` against a golden
  its own comment says you regenerate *from* `Pure.all()` — a change-detector, not a
  correctness check. `PreludeGeneratorTest` *does* walk the real checkouts, but only for
  the Prelude's class/enum shapes. This is why an earlier round could measure 183/721
  (25.4%) signature divergence while the suite stayed green.
- **`CrossStoreGuard.check` has exactly one call site** — `StatementExecutor.java:437`.
  `Compiler.plan()`, which AGENTS.md labels *"the production seam"*, never reaches it, so
  a cross-store query compiles and returns SQL naming both databases. Compounding it,
  `MappingFromProtocol.java:337-347` aliases `OP_ROUTER_UNION` to `OP_STORE_UNION`.
- **An optimization that is a hard compile failure.** `AggregationAwareRouting.java:247,330`
  end `default -> throw new NotImplementedException(... " pending")`, and the routing runs
  on any class carrying an aggregate view — so a date literal, enum value or `if()` in a
  filter aborts the compile. Adding an aggregate view breaks queries that previously
  worked. Real Legend's `generateProjectPath` is total and declines to rewrite.

---

## 8. Backend findings

- **Zero SQL bind parameters exist in production.** No `setString`/`setObject`/`setInt`
  anywhere; `Executor` calls `prepareStatement(sql)` with literals already inlined (the
  comment says `PreparedStatement` is a DuckDB *error-message* workaround). Of 37 sampled
  generated statements, 25 carry inlined literals and 0 carry `?`. Consequences: no
  plan-cache reuse on text-keyed databases, and one escaping function
  (`AnsiSqlRenderer.stringLit:965`) as the only guard across **≥7 identifier-quoting
  owners, of which one escapes correctly** (`H2.java:90`).
  `exec/Ddl.java:400-402` pastes cells **unescaped** when the *declared column type* is
  numeric. Reachable from a connection's `testDataSetupCsv` property — model-authoring
  trust, not end-user query input, so bounded but real. The user-reachable
  `loadCsvToDbTable` path escapes correctly.
- **`AnsiSqlRenderer` is a DuckDB renderer with an ANSI name, and `H2` inherits it.**
  `Spellings.java:14-16` says so. Their own backend probes measured **six silent
  wrong-answer classes** on MariaDB (`||` as logical OR, `"id"` parsed as a string
  literal, case-insensitive and PAD-SPACE collation changing `=`/`IN`/`LIKE`/`GROUP BY`).
  13 DuckDB-only base arms remain unoverridden by `H2`.
- **Semantically unsound SQL, open in their own ledger.**
  `docs/BURNDOWN_EXPLANATIONS.md:132`: *"Scalar subselect on an unproven to-one —
  Lowerer.java:2782-2790 (gate on provable uniqueness)."* DuckDB raises; a backend that
  returns the first row yields a wrong answer with no error.
- **Two clause writers over one MIR.** `EngineStyleH2.select()` never calls
  `super.select()`; it re-implements WITH and UNION and carries six mutable instance
  fields, four cleared at `render()` entry. `EngineStyleH2.expr` (245 lines) and
  `H2.call` (134) are `instanceof` chains, not switch expressions, so they evade the
  no-`default →` rule entirely.
- **The capability model is 2/3 dead.** `CarrierStrategies.Caps(nativeLists,
  correlatedExplode, jsonCarrier)` — the latter two have **zero readers**. And
  `AnsiSqlRenderer:99-101` hands every non-overriding dialect `Caps.H2`, so **SQLite gets
  H2's capabilities** while the javadoc four lines away says otherwise.
- **Rendering is not a pure function of the MIR.** 20 static `ThreadLocal`s;
  `EngineTextBoundary.enter()` wraps the *Lowerer* (`StatementExecutor:650`), so MIR
  construction depends on ambient state.

---

## 8a. Compensation: where the compiler and harness do work that is not theirs

This section answers a specific question — *where do the harness or the compiler still
compensate for missing platform features, by re-parsing, re-walking, re-deriving or
transforming?* The root causes, ranked by how many symptoms each removes.

**First, the migration context this section originally missed.**
`core/src/test/java/com/legend/harness/WholeTestFlip.java` is **not** a test-side legacy
artifact — it is *"HARNESS-DELETION item 1, slice 3 — the SCORING FLIP … the migration
itself, not an instrument"* (`WholeTestFlip.java:18-25`), the program moving scoring from
the harness's statement walk onto the platform. It is **on by default**
(`:149` disables only via `-Dll.wholetest.flip.score.off`), and the migration ratchet in
`RelationalCorpusRunner.java:2272-2277` currently pins:

```java
assertEquals(123L,  WholeTestFlip.fallbackCount(),  "…ratchet moved: fallbacks");
assertEquals(2450L, WholeTestFlip.flippedCount(),   "…ratchet moved: flipped");
```

**2,450 of 2,573 tests (95%) now score from the platform's own assert verdicts; 123 still
fall back to the legacy walk, each with a counted reason.** Those two counters moved in
exact lockstep with the corpus ratchet at Batch 111 (`124/2449 → 123/2450`), so the
harness-deletion migration and the corpus burn-down are now **the same program**: a test
joins the platform lane when it passes. `docs/WHOLETEST_COMPILATION_CHARTER.md`'s
"417 flipped / 2,156 fallbacks" is a stale 2026-08-31 snapshot; do not read it as current.
*(Verified: the lockstep move and both pinned values. Not verified: that every one of the
123 fallbacks fails — only that the counts coincide at two consecutive batches.)*

**C1 — The product path and the scored path are still different code, and production runs
*less* of the pipeline.** The platform lane executes
`Compiler.resolveQuery` (`WholeTestFlip.java:269`) → `Compiler.executeResolved` (`:340`)
→ `StatementExecutor`. The HTTP server calls only `queryService.executeWireJson` and
`executeSql` (`LegendHttpServer.java:160,234`), both routing to `Compiler.executeWire`
(`Compiler.java:483`), which calls `lowerQuery` + `Executor` and contains **zero
references to `StatementExecutor`**. So the 2,450 platform-scored tests and the shipping
HTTP surface exercise different back halves. This remains the most consequential
structural fact in the audit — but the correct framing is that **the corpus exercises more
of the pipeline than production does**, not that it exercises dead code.

**C2 — Two front doors; production takes the shorter one.** `Compiler.resolveQuery`
(`Compiler.java:672-692`) is `ValidateDesugar` + `LiteralMapUnroll` +
`DriverPkOption.set(fired)` + `NameResolver.resolveQuery`. Its **only caller repo-wide is
`WholeTestFlip.java:269`** — i.e. the platform lane, 2,450 tests. Every production entry
— `execute` (`:737`), `plan` (`:422`), `executeWire` (`:929`) — calls
`NameResolver.resolveQuery` **directly**, skipping all three passes. Consequences:
`ValidateDesugar` ("feature #45") and `LiteralMapUnroll` never run in production, and
`DriverPkOption` is **never set** on any production path while
`StatementExecutor.java:55` reads it on every execution. The fix is one line — have
`Compiler.execute`/`plan`/`executeWire` call `Compiler.resolveQuery`.

**C3 — The scored SQL pipeline is not the shipped one.**
`core/src/test/java/com/legend/rcorpus/RelationalCorpusRunner.java:70` sets
`legend.exec.engineScanOrder`, read by production `sql/dialect/DuckDb.java:110` to add a
`StableScanOrder` rewriter to the pass list. The scoreboard was produced with a rewriter
the shipping default does not enable.

**C4 — The Pure assert family is deleted rather than compiled.**
`compiler/element/type/PlatformTypes.java:433-447` declares 13 assert FQNs
platform-owned; `FunctionCompiler.java:70-78` drops the corpus's own bodies; a
2,258-line Java adjudicator (`AssertVerdicts`) decides every verdict — *"the
interpreter's per-element behavior, minus the interpreter"*. The concrete cost:
`equal()` now has **three implementations that disagree** —
`resolver/LiteralFolds.java:80-86` (`BigDecimal.compareTo`, so `1 == 1.0` is **true**),
the SQL lowering (**true**), and `exec/PureAsserts.java:271-282` (*"there is NO
cross-kind numeric equality"*, **false**). **No corpus test can catch this, because the
disagreeing path is the judge.**

**C5 — Modes travel as ambient thread state because phase signatures have no parameter
for them.** `PostProcessBoundary`'s own javadoc names the cause: *"the toSQLString
surface … has no runtime argument."* 20 static `ThreadLocal`s in 15 files. Eight would
be deleted by adding an options argument to `lower()` and the `toSQLString`/
`planToString` surfaces. A live bug: `PostProcessBoundary`'s four facts are recorded
only inside `if (ec.args().size() >= 3)` (`StatementExecutor.java:1579-1585`), so a
shorter execute **inherits the previous statement's table renames and timezone**.

**C6 — Identity encoded in strings and re-parsed downstream.** `DurationUnit` →
`"to_years"` → three dialect demanglers (`H2:181`, `EngineStyleH2:1586`,
`AnsiSqlRenderer:719`); the pivot column name `<value>__|__<template>` split apart at 7
sites; `SignatureMangle` regex-demangling invoked **inside the Typer**
(`Typer.java:2440,2572`). 33 text-dispatch sites in `sql/dialect/` alone.

**C7 — Late-bound schema has three owners.** `Typer.java:1507` stamps `lateBound()` →
`resolver/RawGridSchema` fires a **compile-time `LIMIT 0` JDBC probe** → `Executor.java:845`
adopts ResultSet headers when the probe never ran. Sibling: `Lowerer.deferredTds` plants a
`DeferredTdsString` that **throws if it reaches a renderer**
(`AnsiSqlRenderer.java:474`), and `StatementExecutor.java:2226` patches the plan afterwards.

**C8 — Type re-derived by content sniffing at egress.** `exec/Executor.java:600-618`
guesses via `Long.valueOf` → `Double.valueOf` → raw string, twelve lines below its own
comment stating *"the engine's rule is DECLARED TYPE DECIDES … the '4'-as-Long witness
showed sniffing mis-types raw text."* Root cause: `ExecutionResult.value` is `Object`.
One sealed `PureValue` deletes four compensations.

**C9 — Speculative execution as a predicate.** `lowering/SeedableLets.java:37-50`
constructs a fresh `Lowerer` and runs the compiler back-half to keep one throw/no-throw
bit, catching `RuntimeException` "BROAD BY DESIGN" — and the probe `Lowerer` is built
**without** `withEngineExistsJoinForm`/`withDbTimeZone`/`withInstanceIds`, so it answers
a question about a differently configured compiler than the one that runs. ~16 further
run-a-phase-in-try/catch sites exist.

**C10 — The product's own entry point regex-parses Pure source.**
`server/LegendHttpServer.java:35` — `Pattern.compile("Runtime\\s+([\\w:]+)\\s*\\{")` —
used to split model from query by locating a closing brace, while `Compiler.parseModel`
is called two files away.

**Harness softening, corrected.** An earlier draft of this audit credited the
`0-asserts` column as honest. That is **wrong for 27 of the 29**:
`EngineTestExecutor.java:1060-1071` splices `mayExecuteAlloyTest`'s `{|true}` fallback,
discarding the leg that holds the assertions, and `Runner.java:1173-1178` then scores the
resulting empty body PASS. The repo's own `docs/CORPUS_STUDY_2026_08.md:236-239` says so.
A further ~11-12 tests were reported as carrying no soft flag at all —
`WholeTestFlip.java:365-385` catches the platform lane's `AssertFailed`, records
`"platform-fail: …"`, and re-runs through the legacy walk, whose looser verdict stands.
**Treat that one as UNVERIFIED and re-check it**: the `platform-fail` bucket is a
*designed, counted* fallback route of the migration (charter: *"the REAL-divergence burn
list"*), and since the flip-fallback count equals the corpus failure count at two
consecutive batches, it is not obvious that any such test is scored as a pass. The
`0-asserts` finding above is independent of this and stands. The genuinely honest columns are `rescued` (a *stronger* verdict — the
engine's own golden SQL executed on real H2 and row-compared) and `SHAPE` (labelled, not
folded into pass).

To the project's credit, the scoreboard prints a `SOFT-PASS RECONCILIATION` line whose
comment cites the audit that forced it — *"the scoreboard cannot see its own softness"* —
and corpus exclusion is by stereotype only, read from the parsed model. The problem is
the columns' contents, not their disclosure. But four counters meant to keep them honest
(`H2Verify.M1_VERIFIED/M1_DIVERGED/M1_UNVERIFIABLE`, `VERDICT_ROSTER`) are **write-only**,
and the `UNVERIFIABLE_CENSUS` registry their javadoc says asserts against them **does not
exist**.

**Native signatures, measured.** Against the real checkouts, **182 of 644 registered
`meta::pure::functions::*` overloads (28.3%) exactly match a real `native function`
declaration**; **0** FQNs are fabricated; **351 (54.5%)** exist upstream as Pure-bodied
*programs* rather than natives — including `assertEquals`, `head`, and `if/2` — which
contradicts the World-Map tenet's "PROGRAMS … never ported to Java". Concrete errors
confirmed by hand: `math::plus` is five binary overloads where Pure has four variadic
reducers; `math::plus(String,String)` exists at no Pure FQN; `collection::add` returns
`T[*]` where Pure declares `T[1..*]`. *(The auditor's separate "111 divergent" figure is
a known-inflated upper bound — its comparator matched only `native` declarations, missing
Pure's bodied overloads — and should not be quoted.)*

---

## 9. Claims investigated and dropped

Recorded so the next reader does not re-derive them.

| Claim | Outcome |
|---|---|
| "A test-only flag changes production SQL rendering" (`TextGoldens.ACTIVE`) | **False.** Entered from the production `toSQLString`/`planToString` seam. An ambient-ThreadLocal design problem, not test code in production. |
| "`DriverPkOption` is written only by tests" | **Re-confirmed as originally stated — my mid-audit "correction" was itself wrong.** `Compiler.java:688` does set it, but line 688 sits *inside* `Compiler.resolveQuery` (672-692), whose only caller is `WholeTestFlip.java:269`. No production entry calls it, so production never sets the flag while `StatementExecutor.java:55` reads it every execution. |
| "Fully-qualified refs bypass ArchUnit" | **False.** ArchUnit reads bytecode. The resolver→lowering edge passes because **no rule forbids it**. |
| "The compiler branches on test identity" | **False.** 312 references to 237 test names, **all in comments**; zero fixture literals in guards. |
| "14.6k LOC of hand-rolled JSON is a maintenance bomb" | **False.** One escaping owner (`grep 'replace(' protocol/` → 0), 611 `str()` calls with zero raw interpolation, 364/364 `sourceInformation` via one helper, 99.6% of record components reach the wire, 2.3% duplication. |
| "`CanonicalRenderSql` is a second SQL renderer" | **False.** It builds MIR nodes and returns a `SqlQuery`; zero string-building. |
| "~96 enabled tests have no assertions" | **Withdrawn.** Heuristic error (one-line `@Test` bodies; helper-based assertions). Only the 3 empty bodies stand. |
| "Textual duplication is the problem" | **False.** 0.2% at 12-line normalized windows over 75,013 code lines. |
| "The `0-asserts` column is honest accounting" | **Wrong for 27 of 29** — see §8a. Corrected after the compensation sweep. |
| "`QueryService.execute` has zero callers" | **Imprecise.** It has callers, but only from tests. The correct statement is that no *production* path reaches it, and the HTTP surface bypasses `StatementExecutor` entirely. |
| "The corpus branches on fixture names throughout" | **False.** Three hardcoded fixture FQNs exist (`ConnectionFlags.java:42,57`; `InProtocol.java:128`); **zero** corpus model class names (`Firm`, `Person`, `Trade`) appear in `src/main`. |

---

## 10. What is good, and load-bearing

Stated because a review that finds nothing good is not credible, and because these are
the assets any remediation should build on.

1. **The traversal contract.** `TypedSpec.children()`/`withChildren()` are **mandatory,
   not defaulted**, so a new variant forces both decisions at definition time, and
   `TypedSpecChildrenTest` checks it reflectively. (The gap is usage: 1,132
   `instanceof Typed*` against 231 `children()` calls.)
2. **`SqlFn` as a 172-entry enum.** No stringly-typed `FunctionCall` catch-all exists in
   MIR. The `sql/` dependency wall is airtight and mechanically enforced; the `Cast` node
   is *better* than AGENTS.md claims.
3. **`InferenceKernel` is a real algorithm** — unification with rigid/covariant binding,
   a schema algebra, a lattice join *and* meet, contravariant parameter frames, and an
   explicit documented invariant that scoring and unification must agree.
   `Multiplicity` and `Type` pass the noun test cleanly.
4. **The dialect-extensibility claim survives git.** Four dialect-adding commits touched
   **1-3 main files each with zero MIR changes**. `Spellings`/`TypeNames`/`Lexicon`
   turned 77 hardcoded arms into data, and **SQLite's dialect class was deleted into a
   `Lexicon` row** — the project's own proof of the right pattern.
5. **The parser-equivalence oracle.** Lite's emitted bytes vs a **live**
   `PureGrammarParser` over 8,891 corpus sources: **6,489 byte-identical, 0 diffs, 0
   wrongly-refused**. Leniency is a named, bounded, regenerated ledger, not a claim.
6. **Bugs made unrepresentable.** `SqlSource.Join` throws if a CROSS join carries `ON`
   or a non-CROSS lacks one — which is why there are zero comma-joins in any output.
   `RelationType` enforces column-name uniqueness in its compact constructor.
7. **Honest instrumentation of its own softenings.** 25 named decline sites with
   per-row `test :: form :: reason` attribution; `NavArmCensus` maintains a live,
   per-test-attributed inventory of the pipeline's own non-general branches;
   `LL_TOL_COUNT` measures how often float tolerance is load-bearing. Most of this
   audit's sharpest citations came from the codebase's own comments.

---

## 11. Recommendations, ranked by leverage

0. **Put the product on the path the corpus scores.** Concretely, and in this order:
   (a) make `Compiler.execute`, `plan` and `executeWire` call `Compiler.resolveQuery`
   instead of `NameResolver.resolveQuery` — a one-line change each that ends the
   `ValidateDesugar` / `LiteralMapUnroll` / `DriverPkOption` divergence; (b) route
   `LegendHttpServer` through the same back half the platform lane uses, or accept
   `executeWire` as a distinct wire path and give it its own coverage; (c) delete
   `legend.exec.engineScanOrder` or make it the production default. This is item zero
   because the 95%-complete harness-deletion migration has moved the corpus onto a
   pipeline the shipping surface still does not run.
1. **Restore the ratchet at 1 consolidation per 10 patch batches.** Batch 100 is the
   existence proof. The 6% September figure is the number to move; it alone explains the
   15× cost curve.
2. **Delete `testdatagen`'s two milestone emitters and its `duckType` table.** Removes a
   wrong-rows contradiction and 11 type divergences in one change.
3. **Introduce `MilestoningFacts`, `NavPath`/`JoinIdentity`, `TableName`, and `Fqn` as
   real types.** These four cover the majority of §4. The string-keyed path model is
   already owed (`NavPath` leg, user ruling recorded in Batch 106).
4. **Work the 48-site sealed-switch queue and the mixed-adjudication sites**, and add two
   guards: no `default` arm in a switch over a sealed type; and no enabled `@Test` whose
   body contains no assertion.
5. **Fix the guard blind spots** — the ThreadLocal loophole, the `@`-blind mutable-field
   regex, and the string-dispatch regex that misses 52 sites. A saturated pin that cannot
   see the growth is worse than no pin.
6. **Move `CrossStoreGuard` to `Compiler.plan()`**, and make `AggregationAwareRouting`
   total (decline the rewrite instead of throwing).
7. **Give `LegendCompileException` a source position and thread `SourceInfo` past phase
   F.** It is the prerequisite that makes every other fix debuggable.
8. **Give `equal()` one implementation.** `LiteralFolds`, the SQL lowering and
   `PureAsserts` disagree on `1 == 1.0` (true / true / false), and the disagreeing path
   is the judge — no corpus test can catch it. Escalate independently of any refactor,
   along with `PostProcessBoundary` not clearing on <3-arg executes.
9. **Point `PreludeGeneratorTest`'s machinery at `native function` declarations.** The
   verification harness that walks the real checkouts already exists; it was built for
   classes and never aimed at signatures. Until then the "verbatim" claim in
   `builtin/Pure.java:13-16` and `AGENTS.md:44-45` should be withdrawn — measured exact
   match is 28.3%.
10. **Decide the text-parity question explicitly.** `docs/RELATIONAL_CORPUS.md` says *"row
   equality is the contract, golden SQL is advisory"*, yet the parity lane
   (`EngineStyle*` + `CanonicalDivergence` + `TextGoldens`, ~5,200 LOC) produces the
   shadowed-`"root"` and pass-through-wrapper SQL, and `sql/dialect` is the only package
   whose complexity density is rising. 21 `GOLDEN_TEXT_ONLY` + 27
   `TEST_ASSERTS_ENGINE_INTERNALS` verdicts in the adjudication ledger are advisory work
   being paid for in production code.
