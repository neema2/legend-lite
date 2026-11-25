# The editions, reshaped Stage B: design v3

*Written 2026-11-25 (night) by the B3 orchestrator session. **DRAFT for the third independent review.** It replaces v2 (`edition-design-v2-2026-11-25.md`) and the 21 amendments that followed it (`edition-homework/v2-amendments-ruled-2026-11-25.md`), merged into one text. Where this text and an amendment disagree, the amendment's quoted ruling is authoritative; report the conflict.*

*For a reader with no context: start at `edition-program-START-HERE-2026-11-25.md` §0, then read this file. Every claim cites the record it rests on. The records are under `docs/records/edition-homework/`: `r1/` holds the measurements M3-M7, `r2/` the inventory, the PCT sort and the fold audit, and B2's records are `m1-float-literal.md`, `m2-h2-settings.md`, `e3-design.md` and `engine-m2m-xstore-equality-2026-11-25.md`.*

---

## 0. In one page

- **What lite is.** Lite follows Pure. Toward legend-engine, lite is a drop-in replacement for what crosses its boundary: the answers users get, and later PMCD in both directions and the structure of SQL plans in both directions. It never reproduces legend-engine's implementation. legend-engine's own tests are lite's evidence. Their answers are compared directly; their SQL-text asserts are refereed by running both SQLs and comparing rows (M5: 1,594 of 1,658 DuckDB SQL-text asserts decided by matching rows).
- **Three editions,** which are upstream's two modes plus lite's own:

  | Edition | Meaning | Who gets it |
  |---|---|---|
  | `lite` | Pure's meaning, plus lite's ruled departures (strict typing, lean text forms, its checks) | lite's product |
  | `pure` | compiled legend-pure, everywhere | PCT's reference run; the corpus's test harness |
  | `engine` | the database's meaning in the part legend-engine sends to the database; legend-engine's own Java (Pure's meaning, except a measured list) outside it | pure/v1; PCT's engine run (DuckDB first); the query inside the corpus's `execute()` |

- **Rule zero.** A mode changes the SQL only where the answer itself differs (rows, whether an error happens, a value the query keeps computing with) and Java can't fix it afterwards. Everything else is presentation, in Java, at the edge.
- **Tenet 1, as amended.** In the product, the database computes every value, `toString()` included. In compatibility, how a value is written is presentation at the edge wherever it can be.
- **One place for meaning, one place for format.**
  - **Meaning:** a single *meaning pass* at the store resolver's exit, after inlining and store resolution and before lowering. It calls a rule set the door hands in for that query. The product never names an edition.
  - **Format:** the door's presenter, in Java, chosen by the door.
- **Folds go back to their rule:** constant Strings and Booleans (concatenation included) and facts fixed by the types, guarded by an allowlist. Folds never decide a meaning that differs by edition.
- **"Done" is measured against upstream's own numbers:**
  - PCT's Pure run is as close to compiled Pure as the database allows;
  - PCT's engine run on DuckDB: nothing legend-engine passes may fail in lite;
  - the corpus passes in its harness-`pure` / `execute()`-`engine` split, with the referee;
  - the product's SQL never grows for compatibility;
  - a guard shows each mode's SQL equals the product's except at listed rows.

---

## 1. The rulings this rests on

All are the user's, 2026-11-25, quoted in full in the amendments file (item numbers in brackets).

1. Editions; per project later; the names `pure`, `engine`, `lite` (v2 §1).
2. legend-engine is a user-facing product plus platform machinery; lite emulates the first, uses the second's tests as evidence, keeps the referee, and does PMCD and plans after this program (v2 §1).
3. Everything compatibility at the edge, and a lean product (v2 §1).
4. One place for meaning and one for format, "an okay place to land" [1]. Placement as parts [2].
5. "pure should be compiled legend-pure agreed" [4]. `engine` = "engine for database, pure for outside", with "outside" refined [3].
6. **Rule zero**: "these modes should not be changing sql ... except where they absolutely cannot - we dont want to do old way were we had a pure plan that had javas double formatting algo rewritten in sql" [9].
7. **Tenet 1 amended** [10]; written into AGENTS.md and the rules of record (`5c9aaa44`).
8. The DECIMAL Float literal deleted [11]. `TDSNull` is the TDS type's own, at the edge [6]. Comparisons under `engine` follow the database [7].
9. PCT's two runs: Pure mode as close to Pure as possible, "all the things that do not pass are database limitations that we just have to live with"; engine mode keeps "the spirit of what engine does (in engine sql and engine java) ... without trying reproducing the bugs" [12].
10. "As full compatibility as possible so users dont freak out", decided per row by cost [13], then [20]:
    - under `engine`, the database part behaves like legend-engine (no lite checks; the database decides);
    - `substring`/`indexOf` keep legend-engine's behaviour, off-by-one included;
    - an enumeration compared with another enumeration's value compares by name;
    - lite's 64-bit widening is kept;
    - `acos`/`asin` raise, as compiled Pure does;
    - `1 == 1D` follows compiled Pure.
11. legend-engine's other known wrong answers: classify each as delegation, presentation or bug before deciding [20; §5.4 below].
12. Folds restored to the old rule, with a guard [20].
13. H2's legacy session settings kept for the `engine` door [18]. ServiceTestRunner stays at the edge (a lite one later) [15].
14. S7a = A [17]; S7b keeps refusing; the `#TDS` literal's csv is Pure's source text at legend-pure's door; a raw grid's CSV matches legend-engine at the doors through the presenter; `toCSV` Floats are exact, as each database's own full-precision text [15, 21].
15. The standing word on landing: "if you think makes sense to land code, land it" [19].

---

## 2. The model

### 2.1 Three places code runs under legend-engine, and the parts the pass tracks

| Part | legend-engine runs it | Its meaning | Evidence |
|---|---|---|---|
| **Database part** | in SQL | the database's | substring clamps on H2; `1 == 1.0` is true |
| **legend-engine's Java part** (statements reading no store; in-memory M2M under graph fetch; checked constraints; constants) | in its generated Java | Pure's, except a measured list: so far, `==` across number kinds is true | B2's `engine-m2m-xstore-equality-2026-11-25.md`; the H16 probe |
| **Harness part** (the corpus's test code around `execute()`) | in compiled legend-pure | Pure's | S127 |

- The PCT engine run is all database part.
- pure/v1 is a database part plus a Java part.
- The corpus is a harness part around a database part.
- M2M feeding a TDS result runs in SQL (`chain.pure:104-116`).

### 2.2 `pure` needs almost no SQL of its own

The inventory (r2 §0) finds lite's product already answers Pure's way. M6: every Pure-mode failure on DuckDB is a ledgered database limit, or one of the fixes in §6.1. `pure` = the product's SQL, plus the answers for programs lite refuses at typing (T rows), plus the presenters. What `pure` mainly needs is **removal**: today PCT's legend-pure door inherits legend-engine's meanings through the shared `UPSTREAM` style. Those are the plain parses, `TDSNull`, H2's settings, `isNumeric`, `parseDecimal(5,2)` and lenient `parseJSON` (census hard case 1).

### 2.3 Contracts versus implementation

| Lite matches | Lite never reproduces |
|---|---|
| answers: values, rows, a raised error's record | legend-engine's SQL text (CP-4) |
| formats at the doors: envelopes, error wording, printed values | its plan's Java nodes (G4) |
| legend-engine's **delegations** to the database (§5.4) | its machinery bugs (§5.4) |
| — | its refusals: lite may answer where legend-engine refuses |

---

## 3. The mechanism

### 3.1 Rule sets: edge code, handed in per query

- **The product holds the interface and its own empty set:** `MeaningRules` and `LiteRules`. The edge holds `PureRules`, `EngineRules` and the corpus's composite (harness `PureRules`, database part `EngineRules`).
- **The rule set is an input to each query, not part of the compiled model.** Compiling a model (A-F) types no function body (verified, second review; `SpecCompiler.java:30-33`), so one held model serves lite's API and pure/v1 together (`HeldModels`).
- **The door supplies the rule set wherever a query is planned:** `Compiler.query`, `TestExecution`, the compile-errors route, `lowerResolved`, `hasStatementEffects`. `SpecCompiler` is constructed in those four places. Any plan cache keys on the rule set's identity.
- **The plan carries no rule identity.**
- **`EngineRules` is built by the door from the query's legend-engine feature flags,** read from the typed tree (`ContextReading.java:354-419`). The product reads none. A flag in a `lite` program is refused by name.

### 3.2 The one place for meaning: the pass at the store resolver's exit

- **The seam.** Every resolve overload funnels into one `StoreResolver.resolve(...)`, whose exit already walks every resolved statement (`StoreResolver.java:244-251`, invariant 7). The resolver is constructed in one place (`BodyPlanner.resolver`, `BodyPlanner.java:68`). Its callers:
  - `ProductStatements:150`, the product's query;
  - `BodyPlanner:213`, a test body's values and an `execute()` query;
  - `PlanAllocations:204`, test tooling;
  - `edge/EnginePlans:162`, the edge's SQL-text facet.

  Each tells the pass the body's **role**: the program's query, an `execute()` query, or a harness value.
- **What the pass sees:** the inlined, store-resolved tree. Every user function is inlined (G½), and mapping code, derived properties and constraints have been brought in by the resolver. Governed calls are still Pure calls with Pure types. Function boundaries are gone, which the rules don't need.
- **How it knows the part:**
  - the role, from the caller;
  - whether a statement reads a store (`StatementEffects`, product facts);
  - the resolver's **provenance labels** on what it brings in: relational mapping code, M2M under graph fetch or under a TDS result, checked constraints, graph-fetch-pushed properties.

  The labels are a new, neutral product fact. The rule set decides what a part means.
- **What it does:** for each governed call, it asks the rule set for the explicit form for that part (§3.4), or leaves the call unchanged. Under `LiteRules`, it changes nothing.
- **Why here, not in the typer:** the second review's M1. Lite types a call from its callee's signature (`SpecCompiler.java:30-33`), but legend-engine decides placement after inlining. This seam is where lite has the same view.
- **To verify in R2:** that no lowered statement skips the resolver. Constant plans (`ProductStatements.java:217-235`) answer a String or Boolean fold with no SQL, which the fold rule allows.

### 3.3 The one place for format: the door's presenter

Writing a value for an audience is Java at the edge, from the typed value: Float, DateTime and list text, `TDSNull`, envelopes, error wording, graph-fetch keys, CSV and `executeValues`' shape. The door chooses its presenter; the edition never does.

The exception is a text the program keeps computing with inside SQL, such as `toString()->length()` in a filter. There the database writes lite's text, or for `engine` the database's own cast (inventory S-15, S-16), and the difference is recorded.

Formatting still found in the product, to move in R4 (second review, amendment 2):
- `result/CellText.Forms`;
- `ArrowWriter`'s form;
- `PlanStyle`'s graph-key and trace-comment spellings;
- `objectReferenceConnection`;
- `executeValues`.

### 3.4 Forms in the product, choices at the edge

The dialect is the one place SQL is written (invariant 3), so every form's SQL lives in the product's dialects, named by what it is: `substringAsGiven`, `sqlPosition`, a plain (unchecked) operation, the database's own full-precision text, an enumeration compared by name, and so on. Most already exist (inventory r2 §0: 10 of `engine`'s cheap rows have their SQL in lite today).

A guard proves the product's own planner never mints a non-lite form. Only rule sets do.

### 3.5 Folds: the restored rule

**The rule** (the rules of record, row 1; restored by the user [20]): the only computation before the database is folding **constant String and Boolean expressions**, concatenation included, and **facts fixed by the types**, such as a relation's column names and types. Everything else goes to the database: numbers, number and date text, list operations over values, `indexOf`, `contains` over values, dates.

The fold audit (r2) found about 70 fold sites and nine disagreements with compiled Pure. Among them:
- integer literals past 64 bits rewritten;
- `'abc'->contains('b')` folding to `false`;
- Java's Float text;
- `Integer == 'Integer'`;
- `[] == [1,1]`;
- a crashing `sortBy`;
- `in` matching a date against its text;
- counts truncated with `intValue()`.

It also found one product answer against S399: date-time equality folded by written text.

**Proposed allowlist** (from the fold audit's proposal): column names and types, `elementToPath`, String `+`, `pair`, `zip`, `map`, `filter`, `isEmpty`, `if`, and `==`/`in`/`contains` over Strings, types and enumerations. `ConstBounds` (LIMIT/OFFSET arithmetic) is a named exception.

A guard pins the allowlist. Folds then never decide a meaning that differs by edition, so the pass needs nothing from them.

### 3.6 Connection data per door

legend-engine's H2 session settings (`MODE=LEGACY`, `DEFAULT_NULL_ORDERING=HIGH`) are the `engine` door's connection data [18]. They change no SQL. They carry one corpus test (M2: `active = 1` on a BOOLEAN), and they are faithful for users' `#SQL` islands. PCT uses the product's settings (0 moves).

### 3.7 Today's `substring` rewrite (A3)

Under ruling 10, `engine`'s database part gives legend-engine's `substring`/`indexOf`, off-by-one included, and its Java part gives Pure's. The pass decides this from the part. A3 (`edge/PureMeaningCheck`, `PureMeaningRewrite`, `EngineSubstitutes`, about 900 lines) is run in shadow beside the pass on every test (pure/v1's goldens, A3's tests, the corpus), and deleted when they agree. Disagreements go to the user case by case.

---

## 4. The gates

| Run | Judged by | Lite answers with | Target |
|---|---|---|---|
| PCT, Pure mode, every backend lite has | upstream's runner (channel A, **on compiled legend-pure**, §6.1) and lite's own (channel B) | `pure` | every failure a database limit or a ledgered, ruled difference; channel B on DuckDB is the bar |
| PCT, engine mode, DuckDB | upstream's runner | `engine` | against legend-engine's manifest (261): bucket 5 (upstream passes, lite fails) empty; lite may answer where legend-engine refuses or crashes; delegations matched (§5.4) |
| Corpus, DuckDB and H2 | lite's judges | harness `pure`, `execute()` query `engine` | upstream's expected answers; SQL-text asserts by the referee |
| Product | core's tests under the host, database and both judges | `lite` | its own pins; lean SQL only goes down |
| Every mode | the SQL-difference guard | — | each mode's SQL equals the product's for the same query, except at the listed S rows |

---

## 5. Every difference and its fate

The full classification is the inventory (`r2/sql-inventory.md` §1): 14 typing rows (T), 16 presentation rows (P), 22 genuine SQL rows (S). The decisions:

### 5.1 T rows (lite refuses; the mode answers)

The pass resolves the typing markers: `TypedCrossTypeComparison` and its kin.
- `pure` answers compiled Pure's way (`1 == 1.0` false, `1 == 1D` true).
- `engine` answers the database's way (true), as does an enumeration against another enumeration's value (by name).
- `lite` refuses.

Inventory §1.1 lists the rows.

### 5.2 P rows (presentation, Java at the edge)

All 16 (inventory §1.2) move to the presenters in R4. In the database's SQL, lite's own text remains only where the program computes with it further.

### 5.3 S rows (genuine SQL), decided

| Row | Under `engine` | Under `pure` | Cost | Phase |
|---|---|---|---|---|
| S-1 the ten unchecked raise kinds; S-2 plain parses; S-22 lite's other checks | plain in the database part (the database decides), lite's checks in the Java part | lite's checks | cheap (built for pure/v1); the `toOne` guards (`Coercions.java:63-90`, `ToOneGuard.java:68-72`) to route: medium; non-finite Floats written as JSON `null` at the edge | R3 |
| S-3 `substring`, S-4 `indexOf` | legend-engine's in the database part (`substringAsGiven`, `sqlPosition`), Pure's in the Java part | Pure's | cheap (built), A3 replaced by the pass | R3 |
| S-5 `toOne` of empty | as S-1 | checked | with S-1 | R3 |
| S-6 `isNumeric` | the database's form legend-engine writes per database (check H2: the Postgres model's regex, S370 may be stale) | Pure's | cheap | R3 |
| S-7 `parseDecimal` | per database as legend-engine writes it (DuckDB `DECIMAL(18,3)`, not H2's `(5,2)` everywhere) | Pure's | cheap, new | R3 |
| S-8 `parseJSON` leniency | legend-engine's | Pure's | cheap (built) | R3 |
| S-9 H2 settings | connection data (§3.6) | product's | built | — |
| S-10 the DECIMAL literal | deleted (ruled); recorded difference on DuckDB | — | landing now | L |
| S-11 run-time-kind comparisons | the database's promotion (ruled) | Pure's | medium; legend-engine refuses many `Any` lists ("Any is not managed yet!") | R3 |
| S-12 `LEGACY_SQL_NULL_UNSAFE_EQUALS` | as the flag says | refused by name in `lite` | built | R3 |
| S-13 a keyed count folded to 1 | the count the data holds (an edge bug against S147) | same | cheap | L |
| S-14 `TDSNull` in a computed text | legend-engine's | the TDS type's (`TDSNull`, ruled [6]) | built | R4 |
| S-15 / S-16 a Float's / DateTime's text used further | the database's cast | lite's text, recorded | cheap | R3 |
| S-17 written precision, S-18 Decimal scale, S-19 `average([])` (`pure` only) | — | recorded (expensive or ruled) | — | — |
| S-20 legend-engine's ruled defects; S-21 database versions | recorded | — | — | — |

### 5.4 legend-engine's known wrong answers: classified and ruled (amendment 22)

Every row in legend-engine's own PCT manifests was classified (`r2/wrong-answers-classified.md`, with per-test evidence):

| | DuckDB (47) | H2 (54) |
|---|---:|---:|
| **DELEGATION** (legend-engine deliberately uses the database's own function, like `substring`/`indexOf`): **match under `engine`** | 15 | 19 |
| **PRESENTATION / TYPE**: no new work (pure/v1's JSON already matches; the rest reduce to S-7, S-10, S-11) | 23 | 20 |
| **MACHINERY BUG**: **don't reproduce** | 9 | 15 |

**Matched (delegations, all cheap, R3):**
- `substring` and `indexOf` (ruled);
- `round` half away from zero (both databases);
- `dateDiff` in weeks, per database;
- DateTime and Boolean `toString` as the database's cast;
- `splitPart` with an empty token (DuckDB);
- `toDecimal` as the bare `decimal` (DuckDB `DECIMAL(18,3)`; H2 `toDecimal(3.8)` = 4, ruled);
- `rem` as plain `mod` (H2);
- `ascii('')` (H2, probably already matched);
- an enumeration against another enumeration by name (ruled).

**Not reproduced (bugs):**
- an hour-precision literal's `hour`/`minute`;
- `pow` through a lambda;
- a date adjusted into BC;
- `regexpLike` MULTILINE;
- H2's aggregates over literal lists;
- H2's `lshift` past 31 bits;
- H2's `parseDecimal(p, s)`;
- H2's `equalAll` over an empty relation;
- H2's negative `mod` and `binFloor` (ruled: a regression in legend-engine's newer H2 path).

**Open:** DuckDB's LIKE escaping in `startsWith`/`endsWith`/`contains` with `_` or `%` (`testPersonToString`). One query settles it. Not reproduced either way; recorded if confirmed.

---

## 6. What lands before the phases, and the phases

### 6.1 Landing now (ruled or plain fixes, independent of the mechanism)

Each lands after a full round with `//gates:local` and an independent review.
1. The DECIMAL Float literal deleted (B3, round running).
2. `toCSV` Floats exact, as each database's own full-precision text; S7a (A) (B2).
3. Dead code: the six identical `LoweringForms`, `PlanMaker.sql`'s unused `style`, `JudgePlans.shapeOf`'s `style`, `JsonGrammar`, `DatabaseAsserts`' dead `appealOn`, the feature registry's no-op entries. Keep `FeatureRules`' live refusal.
4. **The folds cut back to the rule** (§3.5): measure first (a probe), then land with the allowlist guard. This fixes the nine Pure disagreements and the S399 contradiction.
5. The edge's keyed count (S-13).
6. Channel A and the engine lane run on compiled legend-pure, per ruling 5. This removes `testBigFloatAbs`, a harness artifact (r2 pct-sort C).
7. `acos`/`asin` out of range raise (S119 reopened for them), and the DuckDB NaN guard (`DuckDb.java:730-735`) goes.
8. Pure-mode gaps (r2 pct-sort D): H2's lateral as an inner join (4 tests); SQLite's `chunk` with a literal pattern (10 tests).

### 6.2 The phases

| Phase | What | Owner | Done when |
|---|---|---|---|
| R2 | The plumbing: `MeaningRules` and `LiteRules`; rule sets per query (§3.1); the pass at the resolver's exit, empty; roles from the callers; the resolver's provenance labels; guards 1-6 (§7). No behaviour change. | B3 | byte-identical on every lane; guards green |
| R3 | `engine`'s meanings in the pass: the S rows of §5.3 marked R3, the T rows, the delegations of §5.4 (round, dateDiff weeks, the casts' text, splitPart, toDecimal, rem, enum by name); the corpus composite; A3 in shadow, then deleted; step 5a's door rewrites deleted | B3 | the gates measured; every move listed and ruled |
| R4 | Presentation to the presenters: the P rows, the presentation families of §5.4, the formatting still in the product (§3.3); `PurePrintForms`' passes deleted; `pure`'s removal of legend-engine's meanings from PCT | B2 | as above |
| R5 | Exact placement in function bodies and mapping code where the shadow comparison or the corpus shows a need (medium) | B3 | as above |
| R6 | The test-body rewrites (S137, S144, S131) as test tooling | B2/B3 | as above |
| R7 | Delete `PlanStyle`, `Features` in the lowering, the two-form `LoweringForms`, the edge's dialect subclasses; move the harness code (the census's T bucket) out of the product | B3 + B2 | the census test at 0 |
| R8 | Final guards; full PCT (both runs) and corpus rounds; an independent audit; the user's go to land, through a PR | B3 | the gates in §4 |

After R8: PMCD at 100% and two-way SQL plans, at the edge.

---

## 7. The guards

1. **Direction:** the edge depends on the product, never the reverse (exists).
2. **No product class depends on a rule-set implementation other than `LiteRules`** (ArchUnit, by type, not by name; the second review found guard-by-name unworkable).
3. **Non-lite forms are minted only by rule sets:** an allowlist of form natives, plus a run-time witness.
4. **No rule identity in the plan** (a test on the plan JSON).
5. **The matrix test:** every rule, in every rule set, over a column and a constant, in each part.
6. **The fold allowlist** (§3.5).
7. **The SQL-difference guard:** for every PCT and corpus query, each mode's SQL equals the product's except at the listed S rows.
8. **The product stays lean:** its lean-SQL pins only go down.
9. **The census test:** audience logic in product classes reads 0 after R7.

---

## 8. The rules this amends

1. **Tenet 1:** amended (done, `5c9aaa44`). Add the fold rule's carve-out to its text: "except the compiler's folding of constant String and Boolean expressions and of facts fixed by the types".
2. **Tenet 3** ("compatibility re-spells, never re-computes"): under `engine`, lite computes legend-engine's values, through the database, with different SQL, chosen by a rule set. Java still re-computes nothing. Amend the text to say so.
3. **Tenet 6** ("no modes in the core"): amended honestly. The product runs a door-chosen rule set at one pass. It is a mode, contained: the product names no mode and branches on none, and with `LiteRules` nothing changes.
4. **The switch rule:** the door chooses the rule set; the product calls whatever it's given.
5. **The fidelity tenet:** at legend-engine's door, lite answers legend-engine's way for its delegations and its database part, recording each departure from Pure; it doesn't reproduce legend-engine's machinery bugs.

---

## 9. The second review's must-fixes, answered

| Must-fix | Answer |
|---|---|
| M1 placement can't be decided at the typer | the pass at the resolver's exit (§3.2) |
| M2 the corpus region can't be found at typing | the caller's role at the resolver (§3.2): the test planner plans `execute()`'s query itself |
| M3 constructs that never reach the call site (casts, slices, `toOne` guards, raise kinds, post-typer calls) | casts and slices are nodes the pass sees; raises are a plain form chosen by the pass, and the `toOne` guards are routed in R3; calls created after the typer (`AssertMessages`, `VerdictQueries`) are the judges', not the program's |
| M4 folds decide more | the folds cut back to the rule (§3.5, §6.1 item 4) |
| M5 sites wrong for code legend-engine runs in Java | the resolver's provenance labels (§3.2); checked constraints and M2M under graph fetch are the Java part |
| M6 ruling 1 (islands and the relational DSL) has no mechanism | a provenance label for relational mapping code; islands stay outside the pass (SQL by definition) |
| M7 `TDSNull` is the type's | ruled [6] |
| M8 flags read before typing | the door builds `EngineRules` from the typed tree's flags (§3.1) |
| M9 run-time-kind comparisons | S-11, ruled: the database's promotion |
| M10 comparisons need no placement was measured for numbers only | enumeration against String: S169 (legend-engine's SQL and its Java disagree) is decided by the part; texts, raises and parses in the Java part go to the engine runner (§10) |
| M11 §8 honesty | §8 above |

---

## 10. Open

1. (Done: §5.4 classified and ruled.) DuckDB's LIKE escaping, one query.
2. **The engine runner's answers:**
   - B2's three: legend-engine's Java-part Float and DateTime text; each raise in the Java part; `testBigFloatAbs` (answered by reading: a harness artifact);
   - the inventory's twelve (r2 §5.4), starting with H2's `isNumeric` and how non-finite values are written.
3. **The fold cut-back's measured cost:** refusals, moves, SQL growth. If any construct needs more than the allowlist, it goes to the user.
4. **To verify in R2:** that no lowered statement skips the resolver; that the resolver can label provenance as §3.2 needs.

## 11. Risks

1. **The pass's part analysis must match legend-engine's router** for pure/v1's Java part. A3's shadow comparison is the check. Cases where they disagree are listed for the user, never guessed.
2. **Cutting the folds back may expose loud refusals** in schema computations (the fold audit's M4.1). Measure first.
3. **Moving the harness code (R7) is the largest phase:** mechanical, but long.
4. **Reading an S row as presentation when it is really meaning,** or the reverse. The SQL-difference guard and the matrix test catch drift.
