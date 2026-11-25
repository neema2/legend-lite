# The edition program: start here

*For a session, or an account, with no context at all. Written 2026-11-25 by the B3 orchestrator session.*

## 0. READ THIS FIRST: the current state (2026-11-25, late night)

*This section is current. §§1-9 below are the history of v1 and its first review; read them only for background. §10 (how to work), §11 (traps) and §12 (file index) still hold.*

### 0.1 The program in one paragraph

legend-lite reimplements legend-pure and legend-engine: it compiles Pure to SQL, and the database computes. The **edition program** (the reshaped Stage B of the compatibility program) moves every audience difference out of lite's product:
- lite's own product meaning stays in the product;
- **legend-pure's meaning** (`pure`: compiled legend-pure) is used for PCT's reference run;
- **legend-engine's meaning** (`engine`: the database's meaning for the part legend-engine sends to the database, and legend-engine's own Java, about Pure, outside it) is used for pure/v1, PCT's engine run and the query inside the corpus's `execute()`.

A mode may change the SQL **only** where the answer itself differs and Java can't fix it afterwards (**rule zero**). Everything else is presentation in Java at the edge (**tenet 1 as amended**). The product stays lean. PMCD (both directions) and SQL plan import/export follow this program, at the edge.

### 0.2 The design of record: v3

**`edition-design-v3-2026-11-25.md` (v3) is the design**, a DRAFT under the third independent review. It merges v2, every ruling since (`edition-homework/v2-amendments-ruled-2026-11-25.md`, items 1-22, with the user's words) and all the research. Where v3 and an amendment's quoted ruling disagree, the ruling wins; report it.

In brief:
- **Editions:**
  - `lite` is the product;
  - `pure` is compiled legend-pure;
  - `engine` is the database's meaning in the part legend-engine sends to the database, and legend-engine's own Java, about Pure, outside it.
- **Rule zero; tenet 1 amended.**
- **One place for meaning:** a pass at the store resolver's exit, calling a rule set the door hands in per query. **One place for format:** the door's presenter.
- **Folds cut back** to constant Strings and Booleans and facts fixed by the types, with an allowlist guard.
- **Under `engine`:**
  - no lite checks in the database part;
  - `substring`/`indexOf` as legend-engine has them;
  - comparisons as the database's;
  - legend-engine's **delegations** matched (round, `dateDiff` weeks, casts' text, `splitPart`, `toDecimal`, `rem`, enum by name);
  - its **machinery bugs** not reproduced;
  - lite may answer where it refuses.
- **The gates:** PCT Pure mode as close to compiled Pure as the database allows; PCT engine mode on DuckDB with nothing legend-engine passes failing in lite; the corpus with harness `pure` and `execute()` `engine`, with the referee; the product lean; a guard comparing each mode's SQL with the product's.

### 0.3 What remains before building

| # | Item | State |
|---|---|---|
| 1 | The third independent review of v3 | running; report to `edition-homework/third-review-v3-2026-11-25.md` when it lands |
| 2 | Fix its must-fixes in v3 | after 1 |
| 3 | The user's go to build R2 onward | after 2 |
| 4 | The engine runner's answers (B2's three, the inventory's twelve, DuckDB's LIKE escaping) | B2, queued; they refine rows, they don't block R2 |

**Landing in parallel** (ruled or approved; each after a full round with `//gates:local` and an independent review; the user's standing word, amendment 19):
- **B3: the DECIMAL Float literal deletion.** The code is uncommitted in `/home/user/s1` while its round runs. Then the dead code, then the fold cut-back (probe first), then v3 §6.1's fixes.
- **B2: `toCSV` Floats exact,** as each database's own full-precision text (Snowflake through DECFLOAT), then S7a (A).

All the research is in `edition-homework/`:
- `r1/`: M3-M7;
- `r2/`: the SQL-difference inventory, the PCT sort, the fold audit, the wrong-answers classification, and the probes;
- B2's M1, M2, E3 and M2M records.

### 0.4 Sessions, branches, roles

- **B3** (this program's orchestrator; Claude session `session_01XmdJ7iCK1RL3QNeGq9zrLv`): branch `claude/compat-b3-nufdlk`.
  - It carries every record of the program and, since `ec762fad`, B2's code too (merged at B2's `418553e9`).
  - The worktrees: `/home/user/s1` (B3's edit and build tree), `/home/user/s2` (detached, used for measurements).
- **B2** (session `session_01XmdJ7iUHw3CS3J3T5aEFGr`): branch `claude/compat-b2-nufdlk`. It does the format and text work, and asks the engine runner. B3 relays the user's rulings to it with `send_message`.
- **Landing to `main`, or to `claude/compat-program`:** only with the user's explicit word, through a PR, with CI on every OS.

### 0.5 Reading order for a fresh session

1. This §0.
2. **`edition-design-v3-2026-11-25.md`:** the design, all in one place.
3. `edition-homework/v2-amendments-ruled-2026-11-25.md`: every ruling, quoted, in order (1-22).
4. The reviews:
   - the third, on v3, when it lands;
   - `edition-homework/second-review-v2-2026-11-25.md`;
   - `edition-homework/independent-review-2026-11-25.md`.
5. The research:
   - `edition-homework/r2/` (`sql-inventory.md`, `pct-sort.md`, `fold-audit.md`, `wrong-answers-classified.md`);
   - `edition-homework/r1/` (M3-M7);
   - B2's `m1-float-literal.md`, `m2-h2-settings.md`, `e3-design.md`, `engine-m2m-xstore-equality-2026-11-25.md`.
6. The rules: `AGENTS.md` (tenet 1 amended) and `compat-rules-of-record-2026-11-19.md`.
7. History only: v2 (`edition-design-v2-2026-11-25.md`), v1, the census, and §§1-9 below.
8. §10 below for how to run every check.

## 1. The whole thing on one page (HISTORY: v1, superseded by §0)

- **legend-lite** reimplements legend-pure and legend-engine. It compiles the Pure language to SQL, and the database computes every value.
- **It has its own product meaning** (lite's own route), plus **compatibility doors** that make lite answer like legend-engine (its API `pure/v1`, and its relational test corpus) or like legend-pure (its PCT test suite).
- **The problem.** In a few places the three disagree about what Pure code means. The clearest is comparing two different types:
  - legend-pure: `1 == 1.0` is `false`;
  - legend-engine: `true`, because SQL promotes the integer;
  - lite: a compile error (the user's strict-typing rule).

  Today the doors rewrite the typed query after typing, so the right answer comes out. But function bodies, derived properties and mapping code are typed later, on demand, inside the planner, where the doors can't reach. Every attempt to reach them needed another workaround (§4).
- **The fix, ruled by the user: the edition.** Every piece of code carries an edition: `lite`, `pure` (legend-pure's meaning) or `engine` (legend-engine's meaning). Only the typer reads it, in one package. It turns each construct whose meaning differs into an ordinary typed call that means the same in every edition. Nothing after the typer (planner, lowering, SQL, runtime, judges) can see the edition.
  - **Meaning follows the code. Format follows the door.**
  - It is a mode, honestly named and amended into the rules: one switch, read in one place, enforced by the build.
- **The wider cleanup.** The census found many other places that do something different for an audience. Each gets one fate:
  - meaning becomes an edition rule;
  - format goes to the door's presenters;
  - test-harness code moves to test tooling (the biggest piece);
  - product options stay;
  - dead code is deleted.

  `PlanStyle` and legend-engine's feature flags in the core end up deleted. The phases are E0c to E7 (§9).

---

## 2. Background you need

### 2.1 Where to read

| What | Where |
|---|---|
| The repository's rules: pipeline, invariants, tenets | `AGENTS.md` (always read first) |
| Goals of record G1-G11 | `docs/GOALS_OF_RECORD_2026_11_07.md` |
| The compatibility program's rules: the six tenets, the switch rule | `docs/records/compat-rules-of-record-2026-11-19.md` |
| Stage B's plan | detailed: `docs/records/compat-stage-b-plan-2026-11-20.md` on branch `claude/compat-program` (557 lines); the 2026-11-24 order and the one equality: `docs/records/compat-stage-b-plan-2026-11-24.md` |
| Every recorded difference from legend-pure / legend-engine | `docs/SEMANTICS_REGISTER.md` (rows like S403); older rows in `docs/records/d-register-rows-2026-11-16.md`; user-facing summary `docs/DIFFERENCES_FROM_LEGEND_ENGINE.md` |
| B3's record (equality, test harness): every slice, ruling, measurement | `docs/records/compat-b3-progress.md` |
| B2's record (formats, text) | `docs/records/compat-b2-progress.md` |
| The gate chain; what "green" means | `docs/GATES.md` |
| Bazel in a cloud container (proxy, distdir, disk) | `docs/CLOUD_SESSIONS.md` |
| Today's homework: probes, measurements, the step 6 and 5b/5c designs, the withdrawn draft | `docs/records/edition-homework/` |

### 2.2 The pipeline, in one breath

Text → lexer (A) → parser (B, C) → name resolution (D, eager over all bodies) → normalizer (E, which turns mapping DSL into synthesized functions) → model (F) → **typer (G: the query typed once)** → G½: user functions inlined, each body typed once per query on demand (`SpecCompiler.compile`, memoized) → **H: the store resolver demands mapping code** → I: lowering to SQL records → J: the dialect renders SQL → K: run. The doors act after G, on the typed query. Mapping code and function bodies are typed at G½ and H, which is the root of the problem.

### 2.3 The rules this work must keep

- **The six tenets:**
  1. the database computes every value;
  2. Java orchestrates and decides nothing about a value;
  3. compatibility re-spells, never re-computes;
  4. upstream is the spec of the answer, never the design;
  5. two judges in the product, one in compatibility;
  6. no modes in the core (amended for the edition, §6).
- **Upstream fidelity.** Lite follows Pure. legend-engine's formats are matched, and its answers where they agree with Pure. Any departure is posted to the user before it lands and recorded as a register row. A divergence that isn't a row is a defect.
- **The switch rule (11-19), as amended 11-25:**
  - switches are read at the door;
  - the one exception is the edition, read only by the typer;
  - one closed registry, a ceiling of 5;
  - guarded by architecture tests.
- **Demand-driven compilation is a tenet the user will not give up:** never type a body the planner wouldn't type (`docs/TENETS.md`).
- **No fallbacks** (AGENTS.md invariant 4): fail loudly, never default.
- **Guard files are load-bearing:** a pin, ceiling or allowlist moves only with a dated note naming the reason.

### 2.4 The one equality: what's ruled and built before the edition

- **Ruled 2026-11-24 to 25:**
  - strict typing: a comparison across known types is a compile error (`==`, `!=`, orderings, `in`, `contains`, `indexOf`, the asserts);
  - option A: any mix, no exceptions;
  - Q1: an assert across types is a compile error too;
  - Q4: at run time, equal magnitudes order Integer < Float < Decimal, and StrictDate < DateTime;
  - Q5: DuckDB reads a date-time written past the microsecond as TIMESTAMP_NS.
- **Built (step 5a, b0df6ea8), plus its client fix (96f0d33c):**
  - the typer marks a crossing in the query as `TypedCrossTypeComparison`;
  - the product's planner and lowering refuse it by name;
  - legend-engine's door unwraps it to the database's comparison (`edge/EngineComparisons`);
  - legend-pure's door answers Pure's way (`edge/PureComparisons`: equality false, membership false, `indexOf` -1, an ordering compared as written);
  - islands (`#SQL`, IQ, Cypher, TS) are typed unmarked, since they compare by their own language's rule.

  The client fix:
  - a TS island lifted ahead of its statement was typed marked; fixed with `Typer.islandOutput`;
  - DataCube's presets now compare in one type;
  - hand-written test literals fixed;
  - IQ parity cases with settings now run as islands, their settings written as IQ `SET` statements.
- **Ruling 1 (11-25):** the relational mapping language (joins, filters, column expressions, enumeration matches) compares by SQL's rule, like islands.

---

## 3. The rulings of record for this program, verbatim

All by the user on 2026-11-25.

| Ruling | Words |
|---|---|
| Adopt the edition | "Edition seems best of all options" |
| Per project, not per run | "Can start per project" |
| Names | `pure`, `engine`, `lite` |
| M2M, `~func`, derived properties in mappings and graph fetch: strict in `lite`, upstream's meaning elsewhere, by the typer | "didn't we already rule that we can do - not like previous door way" |
| Doors set editions at first; project settings later | "Doors only editions first yes" |
| The amendments (switch rule, tenet 6, the 11-20 ruling's reach, B5's goal) | "Yes to all the amendments" |
| legend-engine's feature flags in a `lite` program refused by name; upstream's text as a value test-harness only | "Yes to 1&2" |
| `ServiceTestRunner` kept | "service testrunner we will need right" |
| `.csv` in the product means `toCSV()` | "csv in product, yes same as toCSV" |
| CSV Q-C (B) and Q-D (A) | "Okay to both csv ones" |
| Building paused until the independent review and the user's read | "did you start executing on the plan already without us looking over final plan and reviewing it" → "Yes, start the independent review now" |
| The door-rewrite path for function bodies withdrawn | "We're doing way too many hacky things now, I don't think this path is going to work" |
| A compile-time-only check rejected | "I don't understand how this possibly builds coherent system if the compiler and runtime don't match" |
| The edition is honestly a mode | "it really is just a mode right? It's okay if it is, but we should be honest with ourselves" |

Earlier rulings this builds on:
- **11-19 ~18:12:** a last-step table text is legend-pure's, printed in Java; a text used further is lite's, recorded.
- **11-19 ~20:35:** the switch rule.
- **11-20 ~07:55:** "Delete the settings, make engine only mode for engine for everything".
- **11-22:** the core follows Pure; compatibility at the edges.
- **11-24:** strict typing, "Love it".

---

## 4. How we got here: every approach tried, and why it was rejected

**Do not retry these.** Each was built or argued in detail on 2026-11-25. The user's verdicts are quoted.

| # | Approach | What happened | Verdict |
|---|---|---|---|
| a | The door rewrites the typed query after G (step 5a) | Works for the query. Can't reach function bodies or mapping code, typed later | kept for the query until the edition replaces it |
| b | An "edition" as a run setting (first pass) | Argued as a run-time mode | rejected then as a mode; later adopted in the per-code form (h) |
| c | A lazy view of the program with a door callback the planner calls | — | "Oh no, this breaks your whole 'doors at public interface' tenet? You're now adding a door inside the planner?" |
| d | Wrapper natives at admission for every comparison | — | "extremely heavyweight" |
| e | Move the door to after H (the store resolver) | Would make the planner's private, mid-level representation a public contract; meaning would become tied to execution choices | the user preferred not to open planner internals |
| f | Bodies as data, the door guessing which bodies the planner will need | Over-approximated reach | "kinda hacky fake demand driven" |
| g | The `substring` model: the door inlines the functions a statement calls, then rewrites | Built and measured three times (below) | "way too many hacky things" |
| h | A compile-time-only check (the model check lists crossings in bodies; run time uses the database's comparison) | — | "compiler and runtime don't match" |
| i | **The edition, per code, read only by the typer** | — | **ruled** |

**What (g) taught, measured on both corpus lanes and PCT:**
1. **Walking every callee's body broke demand-driven typing.** It typed `relationalExtensions()`, an `execute` argument the planner never types, and 41 corpus tests failed with "unknown function". Lesson: never type more than the planner would.
2. **Inlining the test statement at the door, using the planner's own inliner, changed how the test planner plans.** 7 of legend-engine's `toPostgresModel` tests failed (unroll budget exceeded and similar), and 37 had verdicts renamed (`assertConversion` became `assertEquals`). Lesson: a door must not reshape test bodies.
3. **Handing the planner corrected bodies, found by running the inliner first, left 1 failure.** `testConvertSubqueryPredicates` failed on DuckDB: the inliner typed deeper than DuckDB's planning route, which hit a lite typer gap that H2 already hits. Lesson: no door-side step can know a lane's exact demand.

The withdrawn draft is in `docs/records/edition-homework/withdrawn-function-body-door-draft.patch.txt`. Two pieces of it are worth reusing in E2: lite's model check that lists crossings in written bodies (`Compiler.modelCheck`, `SpecCompiler.crossingsOf`), and its tests.

---

## 5. Measurements to build on

Each is in `docs/records/edition-homework/` with its probe.

**Comparisons across types outside the query** (probe `probe-crossings-outside-the-query.patch.txt`, results `probe-crossings-results.txt`):

| Where | Places | Test bodies reaching them |
|---|---|---|
| PCT | 0 | 0 |
| Corpus: relational mapping code | 13 | 42 per lane |
| Corpus: derived properties (`Money.usdRate`, a date-time `==` a date; `Entitlement.prop3`, a Decimal `==` a Float) | 2 | 8 per lane |
| Corpus: an in-memory cross-store association | 1 | 1 per lane |
| Corpus: legend-engine's own library, `toPostgresModel`'s converter map, `$p->last() == true` | 1 | 44 per lane |
| Lite's tests: the M2M `SalaryBandMapping`, `$src.salary < 50000` | 1 | 4 |

The library comparison is, as far as we can tell, a real bug in legend-engine: an `Expression` object is never `true`, so its variance conversion always picks `var_pop`.

**The print lift's SQL** (probe `probe-lift-sql.patch.txt`; details in `b3-step6-design.md` §7): legend-engine's door prints a relation by reading it before the body. That adds 25% SQL across the DuckDB corpus (2,307 → 2,875 statements). Step 6 should fix it.

**B2's S6 measurement** (in B2's record):
- the edge's `TDSNull` SQL pass carries 112 DuckDB and 107 H2 corpus tests;
- Float text carries the validation showcase's 2 tests per lane;
- legend-engine's H2 prints two PCT Float cases with an exponent; legend-engine's own H2 manifest lists them as expected failures;
- F31 (unguarded parses) carries one pure/v1 golden.

**The census** (`edition-census-2026-11-25.md`):

| | Meaning | Format | Test harness | Product option | Dead | Pass-through or mixed |
|---|---|---|---|---|---|---|
| 85 edge classes | 13 | 20 | 48 | 0 | 3 | 1 |
| 44 core classes | 9 | 0 | 15 | 1 | 6 | 13 |

Its 12 hard cases are answered in the design's §10b-2.

---

## 6. The design, in brief

The full text is in `docs/records/edition-design-2026-11-25.md`.

- **Editions:** `lite` (lite's projects, the default), `pure` (legend-pure's meaning), `engine` (legend-engine's meaning).
- **Where it comes from:** per project. In the first version, doors only:

  | Door | Edition |
  |---|---|
  | lite's API, IDE and clients (DataCube, Python) | `lite` |
  | `pure/v1` and the relational corpus | `engine` |
  | PCT's legend-pure route | `pure` |
  | PCT's legend-engine route | `engine` |

  It is a field of the compiled model (`ModelContext`), set where each door compiles its model. A per-package edition, for mixing a lite project with an upstream library, comes later.
- **Where it's read:**
  - **The one read:** `EditionRules.of(ctx.edition())`, once, when the typer is made (`SpecCompiler`).
  - **The one hook:** the native-call site in `compiler/spec/Overloads`, where every resolved native call is minted (`CallNodes.mint`) and where step 5a's crossing check sits today. There the typer calls `rules.apply(call)`.
  - **The rows:** one class per row of the design's §3 table, each with an exhaustive `switch` over the three editions and no `default`.
  - **Inside islands and mapping code:** a fixed "SQL rules" object. The normalizer records each mapping function's language: `relational`, or `pure`, plus whether legend-engine runs it in memory.
- **The guards:**
  - only `compiler.spec.edition` and the model may reference `Edition` (an ArchitectureTest);
  - a ratchet on the row count;
  - a matrix test: every row in every edition;
  - no edition field in the plan.
- **What stays at the doors:**
  - input: upstream's protocol, the edition for undeclared code, H2's session settings as connection data, the B6 gate on unchecked databases;
  - output: envelopes, error wording, last-step prints in Java.
- **The rules amended:** the switch rule, tenet 6, the 11-20 ruling's reach, and B5's goal ("the switch counts read one"). They are amended in AGENTS.md and `compat-rules-of-record-2026-11-19.md`, as `[CONVENTION until E1's guards]`.

---

## 7. Open decisions (the user's)

1. **The independent review's findings** (§8): the design is amended for each must-fix, then the user's explicit go after reading the amended design.
2. **`substring` (E4).** Today's rewrite (A3: `edge/PureMeaningCheck`, `PureMeaningRewrite`, `EngineSubstitutes`) is exact for the query: the database's `substring` inside store-reading code, Pure's elsewhere, as legend-engine does. It refuses `substring` reached through mappings. An edition rule can't see store-readiness inside a shared function body.
   - Candidate fix, which the review evaluates: an explicit "legend-engine `substring`" call whose definition the planner resolves per use, the database's function inside a store query and Pure's elsewhere. The planner already knows where each call ends up, and never reads the edition.
   - Until ruled, A3 stays as it is. **Nothing may make `substring` less exact than today without the user's word.**
3. **Comparisons in the `engine` edition.** legend-engine compares in Java, with Pure's meaning, in code that reads no store. Today's door and the design both use the database's comparison everywhere: an existing gap that isn't stated as a trade-off yet. The same candidate fix as `substring` may apply. **The review (§8.3) found this premise conflicts with S127's measurement** (legend-engine's generated-Java executor answers `1 == 1.0` true): ask the engine runner before ruling.
4. **B2's questions, posted, not yet ruled:**
   - S7a: a checked graph's defect record as a plain literal (recommended A);
   - S7b: single values on `pure/v1` execute (recommended A, keep refusing for now);
   - `.csv` of a `#TDS` literal: `toCSV()` (recommended) or Pure's source text;
   - a raw grid's last-step CSV at the doors: A now, C after the edition;
   - `toCSV()`'s Float digits: keep six decimals, or exact (recommended exact).
5. **What is `pure`?** legend-pure's meaning (Pure's comparisons, Pure's texts, the S137/S144 rewrites kept), or lite's rules with Pure's comparisons (the design as written)?
6. **One `engine` edition or two?** pure/v1 and the corpus differ today (§8.1).
7. **`substring`:** exact (§8.3 alternatives a to c) or simpler (d, with recorded losses)? This replaces item 2's candidate.
8. **H2's settings and the DECIMAL Float literals:** keep them as legend-engine's meaning (the design), or delete them (B4's plan)?
9. **May lite's own projects choose `engine`** (§11.3 of the design)? That would make cross-type equality and legend-engine's other departures a product mode; G3 and the fidelity tenet aren't amended for it.
10. **pure/v1 losing `planToString` and `executionPlan` inside programs** (ruling 2's consequence).
11. **Ruling 3, re-asked** on the corrected fact: legend-engine's JSON compare is exact.

---

## 8. The independent review

The full report, with every file and line reference: `docs/records/edition-homework/independent-review-2026-11-25.md`. A reviewer with no stake in the design read the design, the census, the code at 7c2ac303 and the project's rules. It was told to find what gets worse, for whom, and which decisions were glossed over.

**Verdict:** "Sound with must-fixes for E0c to E2. Not ready to approve for E3/E4 or the §11 amendments until the must-fixes are done."

### 8.1 The design's claims the review found wrong

Treat these sentences in the design as **wrong until it is amended**:

| The design says | The truth (with the review's evidence) |
|---|---|
| A3 (today's `substring` rewrite) has "the same blind spot as comparisons: a helper that calls them doesn't get the rewrite" | False. A3 already follows user calls and rewrites per call site (`PureMeaningRewrite.java:107-154`; `PureMeaningRewriteTest.aFunctionCalledHostSideAndStoreSideIsRewrittenAtTheStoreSideUseAlone`). Its real gaps: mapping code, graph fetch properties, databases other than H2 and DuckDB. |
| Every edition-dependent construct passes one site in `Overloads` | False. Float literals aren't calls; casts and slices have their own checkers; `StaticFold` folds `==` and Float text before the site; `LiteralUnroll` folds literal-only `in`/`contains`/`==` to Pure's `false` after it, at G½; generic code is known only after inlining; H2's settings aren't expressions. |
| §3's table is the entire list | False. Missing: DateTime text (S312), list text (S316c), legend-pure-only texts (S314), the DECIMAL Float literal (S20), `isNumeric` (S370), `parseDecimal(5,2)`, lenient `parseJSON`, the ten plain raise kinds, H2's settings, `executeValues`' bare single object. |
| `pure` is legend-pure's meaning | It is lite's rules plus Pure's comparisons. |
| One `engine` edition fits pure/v1 and the corpus | They differ today: pure/v1 has A3 and plain raises; the corpus has neither, and has the test-body rewrites. The corpus's own test code runs as compiled Pure upstream, with only `execute()` sent to the database. |
| M2M's "in memory" can be recorded by the normalizer | It is a property of the query: `chain.pure:104-116` pushes chained M2M into the store query for TDS results; graph fetch runs in Java. |
| "Today each door admits one origin's code"; per project means the same answers through either door | One `HeldModels` serves lite's API and pure/v1 for the same text (`HeldModels.java:44-49`, :74-76). The per-project declaration is in no phase. |
| The switch count stays at one | The typer would also read feature flags (written in programs, or attached by doors). That is a second exception. Flags scoped to a `from(...)` subtree can't reach cached function bodies. |
| H2's `MODE=LEGACY` / null ordering are door input | They change answers (null order): a door-chosen meaning. Keeping them reverses B4's plan to remove them. |
| Ruling 3: legend-engine's JSON compare allows 2 ULP | False since 2026-11-24: exact (`JsonNodeComparator.java:153-157`; `EqualToJson.java:88-89`, whose header javadoc is stale). And a Java comparator is a judge, which never sees the edition. **Ruling 3 must be re-asked.** |
| E3: "Expected moves: none" | PCT moves: Float text (further from Pure's), DateTime text, `TDSNull` cells, parse errors. pure/v1's Float text from `format`, lists and generic bodies can't be caught by a typer rule. |
| `pure` should refuse `assertSize` over a relation and be strict for `assertJsonStringsEqual` | Those rewrites are legend-pure's own meaning (S137, S144); PCT applies them today. The design moves PCT away from Pure. |
| E0c: `FeatureRules` is dead | False. `requireConsumed` (`FeatureRules.java:117`) is the live refusal of two flags. Keep it, or replace it with a guard first. |
| "M2M/XStore in memory gets Pure's `false`, legend-engine's real behaviour" | S127's measured evidence says legend-engine's generated-Java executor answers `1 == 1.0` **true**. Not verified for XStore or M2M: ask the engine runner. |

There are also two phases named E5 (per-package editions in §10, formats to presenters in §10b).

### 8.2 What gets worse, by phase and audience

| Phase | Lite's product | legend-engine's door (pure/v1, corpus) | legend-pure's door (PCT) |
|---|---|---|---|
| E0c | Nothing, if `FeatureRules`' refusal is kept | same | same |
| E1 | Nothing, unless held models are shared | The tab's pure/v1 uses the host's held model: if it's `lite`, `generatePlan` with a crossing is refused; if it's keyed by edition, the trading model compiles twice (5 to 6 s) | nothing |
| E2 | Crossings in function bodies and mapping code become compile errors (unmarked today); the prelude and normalized platform bodies are typed `lite`, unverified | Possibly a wrong answer for in-memory XStore/M2M (S127, unverified) | A crossing in a function body moves from the database's comparison to `false`, and where both sides may be empty, to a **new refusal** (`PureComparisons.java:58`) |
| E3 | nothing | Float text from `format`, lists, generic bodies is missed by a typer rule | Moves: Float text, DateTime text, `TDSNull`, parse errors |
| E4 | nothing | `substring` (§8.3): less exact than today at pure/v1; better for graph fetch properties and `~func`; **wrong where today refuses** (chained M2M with a TDS result) | `assertSize` and `assertJsonStringsEqual` move away from Pure |
| E5 to E7 | — | One `engine` edition merges plain raises (pure/v1) with checked raises (corpus): corpus error tests may move. `planToString` and `executionPlan` inside a pure/v1 program are answered today and **refused** under ruling 2 | H2's settings and the DECIMAL Float literals leave PCT: null order and arithmetic move |

Lean SQL: `pure`'s literal `false` and a guard-free `substring` make SQL smaller; PCT's six-decimal Float text adds about 33 characters per value (S311). The lean-SQL pins will move.

### 8.3 Why `substring` can't stay exact as designed

legend-engine's answer depends on **where the call runs**: the database's SUBSTRING in code sent to the database, Pure's in code it runs in Java (the H16 probe). Today's A3 sees that, per call site, after typing. An edition rule runs inside the typer, which types each function body **once per query** (`SpecCompiler.java:48`, :81-95), before any call site is known. So the rule must choose one answer per function, and the design chose "the database's everywhere". That loses three exact cases (a host-side `let`; an out-of-bounds raise; a function called from both sides), and gives a wrong answer for chained M2M with a TDS result and for checked graph fetch constraints.

The alternatives:
- **(a) Placement-resolved instruction (the review's recommendation to evaluate).** Under `engine` the typer writes an explicit `engineSubstring(...)` (likewise `indexOf`, `toOne` of an empty value). It means "the database's SUBSTRING where legend-engine sends the code to the database; Pure's otherwise", in every edition. One planner pass stamps each occurrence with its placement (reusing `StatementEffects`, before G½ folds host-side `let`s), and the store resolver supplies chained M2M and graph fetch. Exact where A3 is, plus mapping code. Cost: legend-engine's placement rule lives in the core, as a native's documented meaning (tension with G5).
- **(b) Per-placement typed bodies.** Exact, but more complex, and breaks the one-site claim.
- **(c) Hybrid.** Keep A3 at the door for the query and its helpers; an `engine` row only for resolver-demanded code. Strictly better than today, but keeps one door rewrite.
- **(d) The design's choice.** The database's everywhere, each lost case a register row ruled by the user.

The same placement question applies to comparisons in the `engine` edition (§7 item 3), but §7's premise ("legend-engine compares in Java with Pure's meaning") conflicts with S127's measurement and must be checked first.

### 8.4 Must-fixes before approval

1. Correct the A3 claim; rewrite the `substring` row with a chosen alternative, listing every case that changes.
2. Make §3 the real list, or name the phase that handles each missing item.
3. Correct "one read site": name each gap (literals, casts, slices, folds, generic code) and how it is handled.
4. Restate E3's moves per audience; add a DateTime row or admit the move; handle Float text from `format`, lists and generic bodies.
5. E1: specify how `HeldModels`, `LiteV1Api.Models` and layered libraries serve two doors.
6. Correct ruling 3's premise and re-ask it.
7. Check the M2M/XStore claims against S127 and `chain.pure` by asking the engine runner.
8. Amend the switch rule honestly for the flags the typer reads, or move the flags elsewhere.

**Should-fix:** keep `FeatureRules`' refusal in E0c; reword the guard so doors may set the edition; settle the duplicate E5; keep the migration report (every comparison legend-engine's door loosened, ruled 2026-11-24); decide the prelude's and normalized bodies' edition; fix §10c's PCT-runner slip (it is the testable runner, `TestExecutionContextHelper.java:46`); widen the E4 hook beyond single calls (the order-free `assertEquals` rewrite reads goldens across statements); amend G5, G3 and the fidelity tenet, or bound §11.3.

**Not verified:** whether lite's corpus runs `sortCsv`; PCT move counts for E2 to E4; whether the prelude and normalized platform bodies contain crossings; legend-engine's XStore/M2M answer for `1 == 1.0`.

### 8.5 The decisions the review found the user was never asked

These are in §7 as items 5 to 11.

---

## 9. The execution plan, phase by phase

Every phase follows the same loop:
1. Measure first.
2. Build on a branch.
3. The fast checks.
4. The full round with `//gates:local` (§10).
5. An independent review of the diff.
6. Fix it.
7. Push to the branch.
8. Tell the user what moved.

Every moved test result is shown to the user. Every divergence gets a register row ruled by the user. Never `main` without the user's word.

### E0c: delete dead code (no behaviour change)

**What:** the census's "deletable now" list.
- `LoweringForms`' six forms that are identical in both implementations: `view`, `wireCast`, `relations`, `keyedCounts`, `computedPruning`, `unionFrame`. Their call sites in `lowering/Lowerer` and `lowering/CastPolicy` take the shared behaviour directly, and `unionFrame`'s always-false branches go.
- `PlanMaker.sql`'s unused `style` parameter (both overloads) and its direct callers: `ProgramState`, `JudgePlans`, the test `ZonedDateTimeParameterTest`.
- `JudgePlans.shapeOf`'s unused `style`.
- `testexecutionplan/JsonGrammar` (one form, `JudgeJson.STRICT`).
- `DatabaseAsserts`' dead `appealOn` branch (:310-313).
- ~~`lowering/FeatureRules`~~ **Not dead (the review, §8.1): keep it.** `requireConsumed` (`FeatureRules.java:117`) is the live refusal of `USE_DB_NATIVE_IMPLICIT_NULL_ORDERING` and `VARIANT_TYPE_AS_INPUT`, and what lets `CORRECT_SQL_SUBSTRING_INDEXING` through. The no-op substring flag's registry (`RegistryKeys.featureOverrides`, `ImplementationTable`:80-88, the `Implementation` / `Registrations` / `PlatformRegistrations` entries, the select in `Scalars`) may go only if the refusal is kept. Keep the `Feature` enum: the edge still reads the flag.

**Guards to update:** `ProductFormsUseTheirArgumentsTest.RULED` (drop the `relations` and `view` entries, with a dated note), `LoweringForms`' doc, and any hook-list pins (`HookLambdasTest`, `HookRulingTest`) the deletions touch.

**Done when:** every lane is byte-identical, and the census's dead-code count reads 0.

A partial version was written and parked on 2026-11-25. Redo it from this list rather than looking for it.

### E1: the plumbing (no behaviour change)

1. **New package `compiler/spec/edition`:**
   - `enum Edition { LITE, PURE, ENGINE }`;
   - `sealed interface EditionRule` (`governs(TypedNativeCall)`, `meaning(Edition, TypedNativeCall)`);
   - `EditionRules`: the closed row list, empty in E1, with `of(Edition)` and `apply(call)`.
2. **The model carries its edition:** `ModelContext.edition()`, default `LITE`, set where each door compiles:
   - `spec/.../MinimalCorpus.buildModule` (:295): `ENGINE`;
   - `pct/.../PctPlanRoute` (:96-97, :134): the legend-pure route `PURE`, the legend-engine route `ENGINE`;
   - `edge/ModelInput` (:34, :56), `edge/HostAnswers` (:419), `edge/EdgeServer`: `ENGINE`;
   - `server/LiteV1Api` (:103): `LITE`.

   Check every other `Compiler.compileModel` / `buildModule` caller, and any cache of compiled models (`HeldModels`, plan caches): a model cached under one edition must never serve another.
3. **The one read and the one hook:**
   - `SpecCompiler`'s constructor reads `EditionRules.of(ctx.edition())` and hands it to the typer;
   - `Overloads` calls `rules.apply(call)` where the call is minted;
   - with no rows yet, nothing changes.
4. **Guards:**
   - an ArchitectureTest: only `compiler.spec.edition` and the model reference `Edition`;
   - a ratchet pinning the row count at 0;
   - a test that the plan JSON has no edition field;
   - registry docs: the edition is the switch registry's one permanent entry.
5. **Done when:** byte-identical on every lane, and the guards are green.

### E2: comparisons become edition rows

1. **Row `CrossTypeComparison`:**
   - `LITE`: the marker, refused by the planner and listed by the model check;
   - `PURE`: Pure's answer (equality `false` when a side is never empty, membership `false`, `indexOf` `-1`, an ordering compared as written, as `PureComparisons.answer` does today);
   - `ENGINE`: the plain comparison.

   The decision: `EqualityClasses.crossing(...)`, at the Overloads hook, for the query and for function bodies alike.
2. **Delete** `edge/EngineComparisons` and `edge/PureComparisons`, and their calls in `edge/UpstreamTestBodies` and `edge/PureV1Routes`.
3. **Islands and mapping code:**
   - the typer uses SQL's rules there (today's `marking` switch, generalized);
   - the normalizer records each synthesized mapping function's language: `relational`, or `pure` plus whether legend-engine runs it in memory. Record it on `FunctionDefinition.Synthesized`, which already holds the hat.
   - In `ENGINE`, Pure code that legend-engine runs in memory (M2M, in-memory XStore) gets Pure's answer.
4. **Lite's model check:** reuse `Compiler.modelCheck` and `SpecCompiler.crossingsOf` from the withdrawn draft, so the IDE lists every crossing in written bodies. legend-engine's and legend-pure's compile routes keep `compileErrors`.
5. **Fix lite's own models:** `SalaryBandMapping` in `M2MIntegrationTest` and `M2M2RTabularTest` (`50000` → `50000D`), and anything the round finds.
6. **Measure first:** rerun the crossing probe, counting graph fetch, mapping-level properties and each mapping kind separately.

   Expected moves:
   - none at the doors, except the corpus's one in-memory XStore test, which gets Pure's answer;
   - the PCT/corpus split (census hard case 1) assigns each rule to `pure` or `engine`: list every moved test for the user.

### E3: B2's held S6 work becomes edition rows (built by B2)

1. **`ENGINE`'s Float text:** the database's own cast, a typed native each dialect renders.
2. **`ENGINE`'s empty cell text `TDSNull`.** Precondition: the typer must produce an explicit call for "a row's values read as text" (today put together in the lowering: `Lowerer` ~:4265, `Scalars` ~:1020), so the hook can give it the empty text as an argument.
3. **`ENGINE`'s plain parses (F31).**
4. **Then delete the edge's SQL passes:** `edge/PurePrintForms`' arms, the `TDSNull` join pass, `UnguardedParses`, and `PureText`'s dialect overrides for them.
5. **Expected moves:** none (the same SQL, now produced at typing). The 2 validation tests and the H2 exponent cases keep matching legend-engine; no S404.

### E4: the remaining door meaning rewrites, one at a time, each measured

- `substring` / `indexOf` / `substr` / `toOne` (A3): see §7 item 2.
- S137 (`assertSize` / `assertEmpty` over a relation count it as one value).
- S144 (`assertJsonStringsEqual`'s lenient grammar).
- The order-free `assertEquals` for rows with no defined order (`UpstreamTestBodies.rowsOrderFree` and `joinsOrderFree`).

Each becomes an edition row, which fixes the helper-function blind spot for that row.

### E5: formats behind the plan; `PlanStyle` deleted

Per the census's table 1:
- `traceCell`, the graph fetch key text, `objectReferenceConnection` and the JsonBuilder rule go to the presenters at the last step;
- the test facets (`EngineTestText`, `UpstreamPlans`, `ResultEnvelopes`, `judgesEngineText`) go to test tooling, with step 6 (`edition-homework/b3-step6-design.md`);
- the meaning parts of `dialect(...)` are already edition rows (E3);
- then the 13 pass-through classes drop their `style` parameters, and `PlanStyle` is deleted.

### E6: `platform.Feature` leaves the lowering

- `LEGACY_SQL_NULL_UNSAFE_EQUALS` becomes an input to the `ENGINE` comparison row;
- `CORRECT_SQL_SUBSTRING_INDEXING` becomes an input to the `ENGINE` `substring` row;
- `PUSH_DOWN_ENUM_TRANSFORM` goes to legend-engine's plan presenter;
- the other two stay refused;
- in `LITE`, any flag is refused by name;
- runner defaults (legend-engine's PCT runner switches the `substring` fix on) are attached by the door to the code it admits.

### E7: the closing guards

- the census reads zero outside the four allowed kinds;
- the ArchitectureTests from E1, plus: nothing in the compiler, planner, lowering, SQL, runtime or test planner references `PlanStyle` (deleted) or `platform.Feature`.

### Alongside

- **`ServiceTestRunner`:** move it out of `edge` into the product. Its comparator (`EqualToJson`: a null equals a missing field, arrays in any order, Floats within 2 ULP) becomes an edition row: `ENGINE` keeps legend-engine's rules, `LITE` uses lite's typed comparison. A product route stays future work under `docs/DEFERRED_TEST_EXECUTION.md`.

### Who builds what

- **B3** (the equality and test-harness session, branch `claude/compat-b3-nufdlk`): E0c, E1, E2, E4, step 6.
- **B2** (the formats session, branch `claude/compat-b2-nufdlk`): E3, plus format work that needs no ruling.
- **E5 to E7:** assigned as they are reached.

A fresh session taking over should rebase onto the latest of both branches. B2's head was 7c2ac303 when this was written, moving to 7a1002edb. B3's head is the commit that adds this file.

---

## 10. How to work

### Branches and landing

- Work on a feature branch. Never push to `main`, merge a PR, or force-push `main` without the user's explicit word in that session.
- A landing of more than one change goes through a PR: CI covers Windows, arm, macOS, the corpus lanes and the product build, which the local gate does not.
- Do not create a PR unless the user asks.

### Bazel in the cloud container

See `docs/CLOUD_SESSIONS.md`: `tools/cloud-distdir.sh` and `tools/cloud-maven-distdir.sh` seed refused downloads; watch the disk allowance.
- **One Bazel run at a time.** Even `bazel info` waits on the running command's lock.
- **Never edit a tree while Bazel builds it.** Keep an edit tree and a build tree, and sync between them.

### The full round

Each phase runs it, from `edition-homework/round-template.sh.txt`:

```
bazel build --keep_going //core:core_edge_tests_lib //core:core_tests_lib //pct:all //spec:all
bazel test --keep_going --jobs=3 --local_test_jobs=2 \
  //pct:pct_duckdb_{essential,grammar,relation,standard,unclassified} \
  //pct:pct_duckdb_lite_{...} //pct:pct_channel_b_lite_{...} //pct:pct_channel_b_{...} //pct:pct_channel_b_sqlite_{...} \
  //pct:pct_channel_b_h2_relation //core:guardrails //core:guardrails_edge //core:census //core:census_edge \
  //core:core_edge_tests //core:core_tests_{sql,testexec,testexec_judge_database,testexec_judge_both,testplan,
  testplan_judge_database,testplan_judge_both,test,root,integration,resolver,compiler,testdatagen,ladder,planner,exec} \
  //core:query_capture_test //core:lean_sql_baseline_test //gates:local //gates:checks //gates:edge_net \
  //gates:corpus_duckdb //gates:corpus_h2 //spec:lean_sql_corpus_duckdb //spec:lean_sql_corpus_h2
bazel test --lockfile_mode=error //gates:local
```

**`//gates:local` is in every round.** Step 5a's round once skipped it, and 8 client targets went red: DataCube, which runs lite as WebAssembly in a browser, and Python.

### Reading the results

- **Running as root:**
  - About 14 targets fail because initdb refuses root: the Postgres-backed tests, DataCube's `verify_app_test` and `is_numeric_probe_test`.
  - They pass when rerun unprivileged; use `tools/postgres/as_unprivileged.sh` as the test's `--run_under`.
  - Compare against the base commit before calling one of them a regression.
- **The corpus:**
  - Each corpus lane is a build action, `judge_host_<lane>`. Its log is `bazel-bin/spec/judge_host_<lane>/host.log`, not in `bazel-testlogs`.
  - Its rosters and ledgers are diff-tested against `spec/src/test/resources/rcorpus/` and `.../edge/`.
  - A roster change needs a written reason in `docs/GATES.md`, regenerated with `bazel run //spec:update_rcorpus_<lane>_<n>`.
- **Lean SQL:**
  - `//spec:lean_sql_corpus_<lane>` plans each corpus body without running it, and without the door's print lift.
  - Growth needs the user's word.
- **Standing word:** "parser bumps are always okay" (the parser-equivalence floor in `PeRatchets`).

### The user's way of working

- Plain English.
- Honest trade-offs: say what gets worse, not only what gets better.
- No hacks.
- Measure before building.
- Present every moved result and every divergence for a ruling before it lands.
- An independent review before building, and after each slice.
- Ask before executing anything the user hasn't explicitly said go to: the go for a design is a separate word from the rulings on its questions.

---

## 11. Traps already hit (don't repeat them)

1. **Typing more than the planner would** breaks demand-driven typing and real tests (`relationalExtensions()`).
2. **A door must not reshape a test body.** Inlining a helper assert changes the test planner's path and renames verdicts.
3. **A door-side discovery step can't know a lane's exact demand.** DuckDB's and H2's routes type to different depths.
4. **A TS island lifted ahead of its statement was typed outside the island's rule.** Every island path must use the island's rule (`Typer.islandOutput`).
5. **The IQ parity harness's settings cases re-read a printed translation as ordinary Pure.** Settings reach no island yet, so those cases run as islands, their settings written as IQ `SET` statements. The translator must not be changed to suit a test.
6. **DataCube runs lite in WebAssembly in a browser,** so a probe's stderr is invisible. Put diagnostics in the refusal message.
7. **The edge's `UPSTREAM` style serves both PCT (legend-pure) and the corpus (legend-engine)** (census hard case 1). E2 and E3 must assign each rule to one edition, and will move some results.
8. **A corpus body that reads a printed relation is lifted by the door before it runs** (`UpstreamPrints`). The lean-SQL harness doesn't see that.
9. **The corpus pass's log is a build output,** `bazel-bin/spec/judge_host_<lane>/host.log`.
10. **Every guard pin moves only with a dated note.** A refused count that grows needs the user's word.

---

## 12. Index of files

- **This program:**
  - `docs/records/edition-program-START-HERE-2026-11-25.md` (this file);
  - `docs/records/edition-design-2026-11-25.md`;
  - `docs/records/edition-census-2026-11-25.md`;
  - `docs/records/edition-homework/`:
    - `b3-step6-design.md`: step 6, the SQL-text fallback into test tooling;
    - `b3-step5bc-design.md`: the earlier 5b/5c equality slices, partly superseded by E2;
    - the probes and their results;
    - the withdrawn draft;
    - the round template.
- **Code most touched:**
  - **typer:** `compiler/spec/Typer.java`, `Overloads.java`, `EqualityClasses.java`, `CrossTypeComparisons.java`, `SpecCompiler.java`, `UserCallInliner.java`, `TsIslands.java`, `SqlIslands.java`;
  - **typed nodes:** `compiler/spec/typed/TypedCrossTypeComparison.java`;
  - **front:** `Compiler.java`, `TypedQuery.java`;
  - **normalizer:** `normalizer/*` (`SynthHat`, `FunctionDefinition.Synthesized`);
  - **planner and lowering:** `planner/PlanMaker.java`, `lowering/Lowerer.java`, `lowering/Scalars.java`, `lowering/LoweringForms.java`, `lowering/ProductLoweringForms.java`;
  - **switches:** `database/PlanStyle.java`, `testplan/PlanStyleFacets.java`, `platform/Feature.java`;
  - **test runner:** `test/PureTestRunner.java`, `test/Audience.java`;
  - **the doors:** `edge/EngineComparisons.java`, `PureComparisons.java`, `UpstreamTestBodies.java`, `PureMeaningCheck.java`, `PureMeaningRewrite.java`, `EngineSubstitutes.java`, `PureText.java`, `PurePrintForms.java`, `EdgeLoweringForms.java`, `PrintedRelations.java`, `UpstreamPrints.java`, `PureV1Routes.java`, `ServiceTestRunner.java`;
  - **harnesses:** `spec/.../rcorpus/MinimalCorpus.java`, `pct/.../PctPlanRoute.java`.

  All paths are under `core/src/main/java/com/legend/` unless they start with `spec/` or `pct/`.
