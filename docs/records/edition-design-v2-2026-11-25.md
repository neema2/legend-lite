# The editions, reshaped Stage B: design v2

*Written 2026-11-25 by the B3 orchestrator session, from the user's rulings of that day. **DRAFT for the second independent review (R0).** Nothing here is built. It replaces `edition-design-2026-11-25.md` (v1), which the first review found wrong in several places (`edition-homework/independent-review-2026-11-25.md`). Start at `edition-program-START-HERE-2026-11-25.md` if you have no context.*

Numbers marked **[R1]** come from measurements still running when this draft was written. Each is filled in before the review.

---

## 0. In one page

- **What lite is.** legend-lite follows Pure: a Pure program means what legend-pure says. Toward legend-engine, lite is a **drop-in replacement for what crosses legend-engine's boundary**:
  - the answers its users get;
  - later, PMCD in both directions and the structure of SQL execution plans in both directions.

  Lite does **not** reproduce legend-engine's implementation (its router, its SQL text, its plans' Java). legend-engine's own tests are lite's **evidence**: their answers are compared directly, and their SQL-text asserts are judged by the **referee**, which runs both SQLs and compares rows.
- **Upstream has two modes, and so do we.** legend-engine runs PCT in **Pure mode** (legend-pure, the reference: 3 known failures, compiled) and **Engine mode** against each backend (DuckDB: 261 known failures, H2: 494, Postgres: 501, each with its exact error text). The relational corpus runs as Pure, with `execute()` toggled between the two modes. Lite's **editions** are these two modes, plus lite's own:

  | Edition | Meaning | Who gets it |
  |---|---|---|
  | `lite` | Pure's meaning, plus lite's ruled departures: strict typing, lean text forms | lite's product |
  | `pure` | Pure's meaning, exactly (the reference) | PCT in Pure mode; the corpus's test harness |
  | `engine` | what legend-engine answers its users | pure/v1; PCT in Engine mode (DuckDB first); the query inside the corpus's `execute()` |

- **The mechanism: decisions at the edge, forms in the product.**
  - Each edition is a **rule set**, which is edge code.
  - The door hands its rule set to the compiler **as an input to each query**, not as part of the compiled model.
  - The product's typer calls the rule set at a few named places. Each rule turns a construct whose meaning differs into an explicit typed call that means the same thing wherever it appears.
  - Nothing after the typer can tell which rule set was used. The product never names `pure` or `engine`, and lite's own rule set changes nothing.
  - The SQL **forms** the rules choose between (a Float's six-decimal text, its shortest round-trip text, legend-engine's `isNumeric`) are product vocabulary, written by the product's dialects. The **choice** of form belongs to the edge.
- **Everything else that differs by audience leaves the product:**
  - formats go to the doors' presenters;
  - test-harness code goes to test tooling or the edge;
  - dead code is deleted.

  `PlanStyle`, legend-engine's feature flags in the core, and the lowering's two-form switch end up deleted.
- **"Done" is measured against upstream's own published numbers:**
  - PCT in Pure mode matches the reference on every backend;
  - PCT in Engine mode on DuckDB matches legend-engine's manifest, with no test that upstream passes and lite fails;
  - the corpus passes in its harness-Pure / `execute()`-engine split, with the referee;
  - the product stays lean.

---

## 1. The rulings this design rests on (the user, 2026-11-25, verbatim where quoted)

1. The edition approach: "Edition seems best of all options"; "Can start per project"; names `pure`, `engine`, `lite`.
2. Mapping code, `~func`, derived properties and graph fetch are reached, "not like previous door way".
3. The model of legend-engine as **user-facing product versus platform machinery**. Lite emulates the user-facing part, refined by the next two rulings.
4. "We need to be able to emulate 100% engine PMCD parser input/output and 100% engine Plans that are SQL (the sql does not have to be the same but the Plan structure does so we can import/export plans)". **Sequenced after this program**: "lets not put PMCD and Plans first, lets get the total separation of editions to the edge first".
5. "I do really want to be able to run the SQL assert 'referee' to make sure that we do get back same rows with different sql in corpus tests and backwards compatibility mode". The referee stays.
6. Priority: "get everything done at the edge ... lean product ... as much of PCT and corpus done right with everything at edge".
7. The Float literal: "measure and then hopefully delete the float thing". Always-DECIMAL was considered and is not proposed (§5.12).
8. PCT's two modes: "Engine mode can start on Duckdb only"; "lite can answer where engine refuses - yes".
9. The go for R0 and R1: "lets go".

Earlier rulings still in force:
- ruling 1 (the relational mapping DSL compares by SQL's rule);
- ruling 2 (upstream's text as a value only in the test harness);
- `ServiceTestRunner` kept;
- legend-engine's feature flags in a `lite` program refused by name;
- the step 5a strict typing in the product.

Re-asked and not yet ruled:
- ruling 3 (where `ServiceTestRunner`'s comparison lives): §11, item 4;
- B2's open questions: §11, item 6.

---

## 2. The model

### 2.1 legend-engine is two things

- **(A) The user-facing product.** Projects (classes, stores, mappings, connections, runtimes, services) and the APIs that run queries (Studio, Query, DataCube, services). The database computes the answers, except for a short list of places legend-engine computes itself, in Java:
  - in-memory model-to-model mappings;
  - graph fetch's object assembly and checked constraints;
  - constants and `let`s the router keeps out of SQL;
  - the result formats.
- **(B) The platform machinery.** Pure code running on legend-pure: the router, `pureToSQLQuery`, plan generation, the SQL and plan text, and legend-engine's own test suites (the relational corpus, the PCT adapters).

Lite reproduces (A)'s answers and contracts, and uses (B)'s tests as evidence.

### 2.2 Placement: where legend-engine computes a value

legend-engine's answer for the same call can depend on where it runs:

| Where | Whose answer | Measured |
|---|---|---|
| In SQL | the database's | `substring` clamps on H2; `1 == 1.0` is `true` |
| In legend-engine's Java (in-memory M2M, constants) | legend-engine's generated Java | `1 == 1.0` is `true` in an M2M transform and an XStore join (B2, `edition-homework/engine-m2m-xstore-equality-2026-11-25.md`); `substring` out of range raises (H16) |
| In a legend-engine test's own body | compiled legend-pure | `1 == 1.0` is `false` (S127) |

Consequences:
- **Comparisons need no placement under `engine`.** SQL and legend-engine's Java agree.
- **The corpus's harness gets Pure's meaning.** The split is visible in the source text (the argument of `execute()`), so the typer handles it (§4.5).
- **The calls that do need placement are a short list:** `substring`, `indexOf`, `toOne` of an empty value. Inside a pure/v1 query, they are handled by today's exact edge rewrite (§4.6).

### 2.3 Contracts versus implementation

| Lite matches exactly | Lite never reproduces |
|---|---|
| answers: values, rows, the record of a raised error | legend-engine's SQL text (CP-4) |
| formats: result envelopes, error wording, printed values, at the doors | its plan's Java nodes (G4: a plan is SQL and data) |
| PMCD, both directions (later) | its router's internals |
| SQL plans' structure, both directions (later) | its refusals: lite may answer where legend-engine refuses (ruling 8) |

---

## 3. The gates

| Run | Who judges the asserts | Lite answers with | Compared against | What it proves |
|---|---|---|---|---|
| **PCT, Pure mode**, every backend lite has | upstream's runner (channel A) | `pure` | legend-pure's reference manifest (3 known failures) | lite gives Pure's answer on every database |
| **PCT, Engine mode**, DuckDB | upstream's runner (channel A) | `engine` | legend-engine's DuckDB manifest (261) | drop-in replacement |
| **PCT, lite's platform** | lite's own judges (channel B) | `pure` | the Pure-mode run | lite's judges reach upstream's verdicts |
| **Corpus**, DuckDB and H2 | lite's judges | harness `pure`, `execute()` `engine` | upstream's expected answers; SQL-text asserts by the referee | drop-in on real tests |
| **Product** | core's tests, under the host, database and both judges | `lite` | its own pins | the product is lean and unchanged |

Engine-mode parity puts each test in one bucket:

| Bucket | Upstream | Lite | Target |
|---|---|---|---|
| 1 | passes | passes | — |
| 2 | wrong value | the same wrong value (the same failure text) | every upstream wrong value reproduced |
| 3 | refuses, or a database error | passes | allowed (ruling 8), recorded |
| 4 | fails | fails differently | each one explained |
| 5 | passes | fails | **must be empty** |

Today's numbers: **[R1: M4]** for engine mode, **[R1: M6]** for Pure mode.

---

## 4. The mechanism

### 4.1 The rule set

```java
// product: compiler/spec (the only product types)
interface MeaningRules {                       // a door's rule set; the product never names one
    TypedSpec atCall(TypedNativeCall call, Site site);   // the call site (4.2); identity by default
    boolean governs(TypedNativeCall call);               // folds ask before folding (4.3)
    MeaningRules forArgument(TypedNativeCall call, int arg); // a lambda argument's rules (4.5); this by default
}
enum Site { QUERY, FUNCTION_BODY, STORE_DEMANDED }      // where the typer is (4.4): placement facts, not audiences
final class LiteRules implements MeaningRules { ... }   // the product's own: identity everywhere
```

- **The edge holds the rule sets.** `PureRules`, `EngineRules`, and the corpus's composite (harness `PureRules`, `execute()` argument `EngineRules`).
- **Each rule is one class per difference** (§5), with an answer for each edition, kept in a closed list at the edge, and tested in every edition by a matrix test.
- **Each rule's result is an explicit typed call** with one fixed meaning (for example `floatText(x, SHORTEST_ROUND_TRIP)`), or the call unchanged.

### 4.2 Where the product calls the rules

| Place | Why | Notes |
|---|---|---|
| **1. The native-call site** (`compiler/spec/Overloads`, where every resolved native call is minted; step 5a's crossing check sits here today) | every call-shaped difference: comparisons, texts, parses, raises, `isNumeric`, ... | keyed on the call, not the operand's type, so the prelude's generic bodies and `format`'s operands are reached (B2's e3-design §2) |
| **2. Folds** (`StaticFold`, before place 1, inside normalized bodies; `LiteralUnroll`, at G½) | a fold must not decide a meaning that differs by edition | folds ask `governs(call)` and leave governed calls alone. `StaticFold`'s Java `String.valueOf` of a constant Float and its Java `equals` across number kinds stop being folds (tenet 1 already forbids Java computing them) |
| **3. A lambda argument's rules** (where the typer types a call's lambda argument) | the corpus: the harness is Pure, `execute()`'s query is legend-engine's | identity for every rule set except the corpus's composite |

There is **no fourth place for Float literals** if M1 lets us delete the DECIMAL spelling (§5.12). If M1 shows it can't go, the fallback is an explicit "bare literal" form at place 1's literal twin. That would be the one addition, and it's not proposed.

Everything in this table is pinned by a guard (§7).

### 4.3 Folds

`StaticFold` and `LiteralUnroll` fold only what is fixed by the types and constants, and never a call a rule governs. The test: the matrix test runs each rule over a constant operand as well as a column. That catches a fold that runs first.

### 4.4 Sites: function bodies, mapping code, graph fetch

- **Query:** the query the door received.
- **Function body:** a user function's body, typed once per query and memoized (`SpecCompiler`, a new instance per query; B2 confirmed nothing typed outlives a query).
- **Store-demanded:** code the store resolver demands, which runs in SQL by definition:
  - mapping code (property mappings, `~func`, chained M2M sources);
  - qualified properties a graph fetch pushes into SQL.

  The resolver already passes its `SpecCompiler`. It asks for these bodies as `STORE_DEMANDED`.

The memo of typed bodies is keyed by (function, rule set, site), so one body typed twice under different rules is never confused.

### 4.5 The corpus's two rule sets

A corpus test is Pure code (the harness: asserts, helpers such as `sortCsv`) around a user query (the lambda given to `execute()`):
- The door hands the composite rule set.
- When the typer types `execute()`'s query argument, it asks `forArgument` and gets `EngineRules`. Everything else is typed with `PureRules`.
- A helper called from both is typed twice (the memo key in §4.4).

### 4.6 Placement inside a pure/v1 query

`substring`, `indexOf` and `toOne` of an empty value mean the database's answer in SQL, and Pure's in legend-engine's Java. Within a pure/v1 query the typer can't know which, because the planner decides placement later. Today's edge rewrite (`edge/PureMeaningCheck`, `PureMeaningRewrite`, `EngineSubstitutes`: A3, ~900 lines) already does this exactly:
- it finds the statements that read a store;
- it follows user calls per call site;
- it rewrites only the store-side uses.

It stays, as the edge's second mechanism, for these calls alone. Division of labour:

| Where `substring` appears under `engine` | Who decides | Answer |
|---|---|---|
| store-demanded code (mapping code, graph fetch properties) | the rule at place 1 (site `STORE_DEMANDED`) | the database's: **new** (refused today) |
| the query and the helpers it calls | the A3 rewrite, by placement | the database's in store statements, Pure's elsewhere: as today |
| a function body otherwise | the rule at place 1 | Pure's, which A3 may then correct by placement |

What A3 still refuses by name, and is listed as residue:
- a lambda `let` shared between a host-side use and a store-side use;
- a function value it can't follow;
- a database it hasn't been checked on.

The alternative, one mechanism, would decide placement in the product's planner. That puts legend-engine's routing knowledge in the product, against the priority of ruling 6. Rejected; recorded.

### 4.7 Rules per query, not per model

- **Compiling the model** (steps A–F) doesn't type function bodies, so it doesn't need the rules. One held model can serve lite's API and pure/v1 for the same text. This removes the first review's must-fix 5.
- **The door supplies the rules** wherever a query is typed: `Compiler.query(ctx, rules, lambda)`, `TestExecution`, the compile-errors route (`compileErrors` / `compileAllBodies`: pure/v1's compile route uses `EngineRules`, so it reports no strict-typing errors), `lowerResolved`, `hasStatementEffects`. `SpecCompiler` is constructed in exactly these four places today.
- **Any cache of plans** keys on the rule set's identity. The plan itself carries no rule identity.

### 4.8 Feature flags

legend-engine's feature flags (`LEGACY_SQL_NULL_UNSAFE_EQUALS`, `CORRECT_SQL_SUBSTRING_INDEXING`) are legend-engine behaviour:
- **`engine`:** the door reads the query's flags and builds the `EngineRules` instance with them. The rules, not the product, act on the flags.
- **`lite`:** a flag in a lite program is refused by name (ruled).
- **Scope:** a flag scoped to one `from(...)` subtree, different from another subtree's, is refused by name until a test needs it.

The core's flag readers (`ContextReading`, `ExecutionContext`, `Scalars`' null-unsafe equality, `FeatureRules`) leave the product in R7. Until then, `FeatureRules`' refusal stays (the first review: it isn't dead).

### 4.9 Forms in the product, choices at the edge

The dialect is the one place SQL is written (invariant 3). So the SQL for every form lives in the product's dialects, named by what it is, not who wants it:
- `SIX_DECIMALS_TRIMMED`, `SHORTEST_ROUND_TRIP`, `DATABASE_CAST` for a Float's text;
- `ISO_NANOS_Z` and legend-pure's DateTime print;
- `IS_NUMERIC_FULL_MATCH` and `IS_NUMERIC_LOWER_EQUALS_UPPER`;
- and so on.

Precedent: `SqlFn.FLOAT_SHORTEST_TEXT` and `meta::legend::lite::isNumeric` already exist this way.

A guard (§7) proves the product's own planner never chooses a non-lite form: only the rule sets mint those calls. The edge's dialect subclasses (`PureText.dialect`, which override forms per door today) are deleted in R7, so the plan's renderer no longer depends on the door.

---

## 5. Every difference, and its fate

The full list, from:
- the census (`edition-census-2026-11-25.md`);
- the first review's §3 item 2;
- B2's homework (`edition-homework/e3-design.md`, on B2's branch at `da231244b`).

Mechanisms:
- **R** = a rule at place 1;
- **A3** = the edge's placement rewrite;
- **H** = test harness (edge or test tooling);
- **P** = a presenter at the door;
- **C** = the connection, data the door gives;
- **D** = delete.

| # | Difference | `pure` | `engine` | `lite` | Mech. | Phase | Evidence |
|---|---|---|---|---|---|---|---|
| 5.1 | Comparing two different types (`==`, `!=`, `in`, `contains`, the order comparisons, `max`/`min`) | Pure's (`false`; `PureComparisons`) | the database's (`true`; also legend-engine's Java) | compile error | R | R3 | S127; step 5a; B2 m2m |
| 5.2 | A Float's text (`toString`, `format` `%f` `%s` `%r`, Pair and mixed-list elements, a relation's print) | legend-pure's (`SHORTEST_ROUND_TRIP`; 0 PCT moves) | legend-engine's (today's edge text) | six decimals trimmed (S311) | R | R4 | B2 e3 §4 |
| 5.3 | A DateTime's text (`toString`, `format` `%s` `%t`, a relation's print) | legend-pure's (`...000+0000`) | legend-engine's | ISO, nine digits, `Z` (S312) | R | R4 | B2 e3 §5 |
| 5.4 | A list's text (S316c) | `[a, b]` | legend-engine's | JSON array | R | R4 | census |
| 5.5 | An empty cell's text | legend-pure's | `TDSNull` (112 DuckDB / 107 H2 corpus tests) | lite's | R, through an explicit "row's values as text" call (`cellText` exists) | R4 | B2 e3 |
| 5.6 | legend-pure-only texts (`%0wd`, `%r`, an instance's `@id`, `toRepresentation`; S314) | legend-pure's | refused by name, or legend-engine's where it has one | refused by name | R | R4 | census |
| 5.7 | Parses of bad text (F31: `parseInteger`, `parseFloat`, `parseDate`) | Pure's checked raise | the database's cast error (1 pure/v1 golden) | lite's checked raise | R | R4 | B2 S6c |
| 5.8 | Ten raise kinds legend-engine leaves to the database (`ENGINE_UNCHECKED`: divide by zero, sqrt, shifts, `toOne` size, slice, range, cast, the parses) | checked, Pure's message | plain (the database's error or value) | checked | R | R3 | **[R1: M3]** decides whether the corpus joins pure/v1 |
| 5.9 | `isNumeric` (S370) | Pure's | `lower = upper` | full numeric match | R | R4 | S370 |
| 5.10 | `parseDecimal` precision; lenient `parseJSON` | Pure's | legend-engine's (`DECIMAL(5,2)`, lenient) | lite's | R | R4 | census |
| 5.11 | `substring`, `indexOf`, `toOne` of an empty value (S383, S130) | Pure's (raises) | by placement (§4.6) | Pure's (raises) | R + A3 | R5 | §4.6 |
| 5.12 | A Float literal: legend-engine writes it bare, a DECIMAL on DuckDB, H2, Postgres, Databricks and Trino (S20) | IEEE | **delete** if M1 allows: IEEE, the difference recorded | IEEE | D (or C-like form) | R6 | **[R1: M1]** |
| 5.13 | H2's `MODE=LEGACY`, `DEFAULT_NULL_ORDERING=HIGH` | not applied | per M2: kept as connection data the door gives, or deleted with null placement explicit | not applied | C or D | R6 | **[R1: M2]** |
| 5.14 | `executeValues`' bare single object | legend-pure's | legend-engine's JsonBuilder | lite's | R inside a program; P at the last step | R4 | census hard case 7 |
| 5.15 | `assertSize` / `assertEmpty` / `assertNotEmpty` over a relation (S137); `assertJsonStringsEqual`'s grammar (S144); the order-free `assertEquals` rewrites (S131) | legend-pure's own meaning (kept) | harness = Pure: the same | refused / strict | H (the test-body rewrite reads goldens across statements, so not a call rule) | R6 | first review §3 item 15 |
| 5.16 | Graph fetch keys (`+0000`, a quoted String) written into SQL | — | legend-engine's text | lite's | P (the writer re-keys at the door) | R4 | census hard case 7 |
| 5.17 | A raw grid's / dynamic pivot's last-step CSV | — | lite's (recorded, S420) | lite's | P | — | B2 |
| 5.18 | `ServiceTestRunner`'s JSON comparison | — | legend-engine's (null ≡ missing, unordered arrays, exact numbers) | — | H at the edge, no edition (§11, item 4) | — | first review §3 item 10 |
| 5.19 | Upstream's text as a value (`planToString`, `toSQLString`, `toDDL`, `executionPlan`, `executeLegendQuery`) | harness only | harness only (the referee's input); refused in a pure/v1 program | refused by name | H | R7 | ruling 2 |
| 5.20 | Feature flags | — | the door builds `EngineRules` from them | refused by name | §4.8 | R3 | census |

**Dialect overrides at the edge** (`PureText.dialect`), each with a fate in R7:
- the DECIMAL literal (5.12);
- the Float text (5.2);
- MongoDB's date format;
- SQLite's refusal of decimal arithmetic;
- the H2 settings (5.13);
- `raw()` (translating H2 text for the referee: H);
- DuckDB's scan order.

Each moves to a rule, a form, the referee, or deletion. **R2 classifies the last three** (MongoDB, SQLite, scan order), which this draft hasn't settled.

**Formats** that stay at the doors (P): envelopes, error wording, last-step prints, the H2 connection JSON (`objectReferenceConnection`).

**Test tooling** (H): the census's T bucket in the product:
- `ActivityTexts`, `BodyPlanner`'s `toDDL`, `PlanAllocations`, `RoutedQueries`, `UpstreamPlans`, `PureTestRunner`, `TestObserver`;
- `EngineTestText`, `EngineTextGates`, `EngineTextPlanning` (the referee's front end: kept, moved);
- `Seeds`' and `TestPlanner`'s audience branches, `FramePlanner`, `PlanTextRoutines`, `SeedSqlForms`, `Audience`.

**Dead code** (D):
- the six identical `LoweringForms`;
- `PlanMaker.sql`'s unused `style`;
- `JudgePlans.shapeOf`'s `style`;
- `JsonGrammar`;
- `DatabaseAsserts`' dead `appealOn`;
- the feature registry's no-op entries, but **not** `FeatureRules`' live refusal.

---

## 6. What leaves the product, and how we know

The census at 7c2ac303:
- **44 core classes** with audience logic: 9 meaning, 15 test harness, 13 carriers or mixed, 6 dead, 1 product option;
- **85 edge classes**.

**Done** means:
- the product's classes hold **zero** audience logic: no `PlanStyle`, no `Features` read in the lowering, no two-form switch, no door-chosen dialect;
- the carriers carry a `MeaningRules` and nothing else;
- a census test pins the count at zero.

---

## 7. The guards

1. **Direction:** the edge depends on the product, never the reverse (existing).
2. **No edition in the product:** no product class references a rule set other than `LiteRules`, and no product class names `pure` or `engine` (ArchitectureTest).
3. **Forms are chosen only by rules:** the product's own planner never mints a non-lite form; only rule sets do (a test over the native catalog's "forms" list).
4. **No rule identity in the plan:** the plan JSON has no rule-set field (a test).
5. **The matrix test:** every rule, in every rule set, over a column and a constant operand, in a query, a function body and store-demanded code.
6. **Ratchets:** the rule count; the number of places the product calls the rules (3, §4.2); the A3 residue list.
7. **The product stays lean:** its lean-SQL pins only go down; the edge's pins move only with each phase's measured, ruled moves.
8. **The census test:** audience logic in product classes reads 0 after R7.

---

## 8. The rules this amends

1. **The switch rule (11-19):** "Switches are read at the door." It stays as written for the product. The door chooses a rule set, the product calls whatever rule set it's given, and the product never reads which one it is. The registry's first entry is "the rule set", owned by the compatibility program, read only at the doors, permanent.
2. **Tenet 6** ("no modes in the core"): **amended**, honestly. The product calls a door-chosen rule set at three named places, so it is a mode, contained: the product names no mode, branches on none, and with lite's rule set nothing changes. Per-project editions later make it a property of code.
3. **B5's goal** ("the switch counts read zero"): the product reads zero switches. The doors read one: the rule set.
4. **G5** ("compatibility at the edge, one direction"): kept. Decisions are edge code; the product holds forms (§4.9).
5. **The fidelity tenet:** unchanged. `pure` is Pure's meaning; `engine`'s departures from Pure are register rows; lite's product departures are register rows.

---

## 9. Phases (reshaped Stage B)

Every phase:
1. measure first;
2. build on a branch;
3. the fast checks;
4. the full round with `//gates:local`;
5. an independent review of the diff;
6. fix;
7. push to the branch;
8. tell the user every moved result.

No push to `main` without the user's word. A landing of more than one change goes through a PR.

| Phase | What | Owner | Done when |
|---|---|---|---|
| R0 | This design; the second independent review; the user's go | B3 | the user's go |
| R1 | M1 Float literal; M2 H2 settings; M3 corpus with plain raises; M4 engine parity on DuckDB; M5 referee coverage; M6 Pure parity | B2 (M1, M2), B3 (M3–M6) | numbers in §3 and §5 |
| R2 | The interface, the three places, `LiteRules`, rules per query (§4.7), the memo key, the guards; the dead code (§5 D); classify the open dialect overrides | B3 | byte-identical on every lane; guards green |
| R3 | 5.1 comparisons, 5.8 raises, 5.20 flags as rules; the corpus composite (§4.5); step 5a's door rewrites deleted | B3 | the gates in §3 measured; moves ruled |
| R4 | 5.2–5.7, 5.9, 5.10, 5.14, 5.16 as rules and forms; `PurePrintForms`' passes deleted | B2 | as above |
| R5 | 5.11: the store-demanded site, A3 kept for the query | B3 | as above |
| R6 | 5.12, 5.13 per M1/M2; 5.15 as harness | B2/B3 | as above |
| R7 | Delete `PlanStyle`, `Features`, `LoweringForms`, the edge dialect subclasses; move the harness code (§5 H) out of the product | B3 + B2 | census test at 0 |
| R8 | Final guards; full PCT (both modes) and corpus rounds; an independent audit; the user's go to land | B3 | the gates in §3 |

After R8: PMCD at 100% and two-way SQL plans, at the edge (ruling 4).

---

## 10. The first review's must-fixes, answered

| Must-fix | Answer here |
|---|---|
| 1. A3 "blind spot" wrong; substring row | §4.6: A3 kept, exact; the rule adds store-demanded code |
| 2. §3 not the real list | §5 (20 rows, plus dialect overrides, formats, harness, dead code) |
| 3. "One read site" false | §4.2: three named places, each with its reason; folds ask; literals per M1 |
| 4. E3 moves | §5 with B2's measurement: `pure` = Pure's texts moves 0 PCT cases |
| 5. Held models | §4.7: rules per query; the model is rule-free |
| 6. Ruling 3's premise | §11, item 4, re-asked on the corrected fact |
| 7. M2M / XStore | measured: `true` (B2); §2.2 |
| 8. Flags as a second switch | §4.8: the door builds `EngineRules` from the flags; the product reads none |

---

## 11. Open, for the user or the measurements

1. **[R1]** M1 (Float literal): delete, or keep as a form?
2. **[R1]** M2 (H2 settings): delete, or keep as connection data for `engine`?
3. **[R1]** M3: does the corpus take plain raises (one `engine` rule set), or does it keep checked raises, with the corpus composite's engine half differing from pure/v1's?
4. **Ruling 3, re-asked:** `ServiceTestRunner` stays at the edge, with legend-engine's exact JSON comparison and no edition (recommended), rather than moving into the product.
5. **The corpus mode question for the Legend team:** does upstream's corpus pass in both modes against the same expected answers? If yes, the corpus barely distinguishes the modes.
6. **B2's open questions:**
   - S7a: a plain literal (recommended);
   - S7b: keep refusing (recommended);
   - S405's `#TDS` csv: legend-pure's door answers Pure's source text;
   - the raw grid's CSV: keep, recorded;
   - `toCSV`'s Float digits: exact (recommended).

---

## 12. Risks

1. **Fold reordering touches two core passes.** The matrix test's constant operands are the check.
2. **The corpus's argument switch** depends on how the test planner types `execute()` (to confirm in R2).
3. **A3 stays as a second mechanism.** Its residue is listed and ratcheted; a future reviewer may still prefer one mechanism.
4. **Moving the harness code (R7) is the largest phase**; mechanical, but long.
5. **Engine-mode parity may show bucket-5 tests** that need new rules. **[R1: M4]** sizes this.
