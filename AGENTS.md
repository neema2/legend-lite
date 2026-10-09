# Legend Lite — Architectural Invariants

> **This file is read by AI coding assistants. Follow these rules strictly.**
>
> **North-star tenet:** *Total Knowledge, Demand-Driven Work* — be eager and
> total about Knowledge (parse/index/manifest), lazy about Work (type-check
> bodies, lower, SQL, execute). See `docs/TENETS.md` before any "lazy vs
> eager?" decision.
>
> **Guard files are load-bearing.** The ArchUnit rules, ratchet
> constants, allowlists, and ledgers under `core/src/test` (and
> `PctDisciplineTest`) are the mechanical form of this file's rules. A
> ceiling, pin, or allowlist entry moves ONLY with a dated justification
> comment naming the task/incident — never as an edit-in-passing to make
> a build green (docs/FOUNDATIONS_PLAN.md §0.4).
>
> **Execution tenet — an INVARIANT, not an aspiration (eviction program
> closed 2026-08-18; claim recalibrated by
> docs/ADVERSARIAL_TENET_AUDIT_2026_08_18.md):** *Java orchestrates, the
> DATABASE executes.* The honest, measurable form: NO host interpreter
> remains; the QUERY COMPILER executes no values; the egress boundary is
> a small set of irreducible carriage sites plus a NAMED, SHRINKING
> residue (the register in `JavaEvalLedgerTest`). Enforcement is
> mechanical, with known limits (guards catch drift, not adversaries —
> the audit's §3): `JavaEvalLedgerTest` (shrink-only pins on the
> registered residue), `ArchitectureTest.javaSqlIsFunnelledToTheCharteredSeam`
> (JDBC only at the chartered seams) and
> `ArchitectureTest.theInterpreterPerformsNoJdbc` (the metamodel channel
> cannot reach a connection — grid chains COMPILE to SQL at the exec
> seam, `GridReads.tryLower`, scheduled for deletion by the
> relation-typed `fetchDb` leg). The adjudication authority is `docs/TENET_CHARTER.md`
> (clauses C1-C5) plus `docs/JAVA_EVICTION_PLAN.md` §1's decision rule
> (query-time values → the DB; build-time model text → typed constants;
> engine-exact text → a render TARGET of the one compiler). Consult them
> before any "may Java compute this?" decision.
>
> **Reference-checkout tenet — an INVARIANT (V7 correction 2026-08-28,
> user catch):** legend-lite exists to REPLACE legend-pure and
> legend-engine. The reference checkouts (`-Dlegend.engine.root` /
> `-Dlegend.pure.root`) are SPEC and TEST INPUT only — corpus test
> sources, PCT trees, parity fixtures, signature verification. They are
> NEVER runtime components: no platform behavior (resolution, typing,
> stdlib bodies) may depend on files from those checkouts. The platform
> stdlib (`meta::pure::functions::*`) is OURS — registry natives with
> signatures verified verbatim against the real `.pure` sources
> (spec by VERIFICATION, never by LOADING), platform-owned so parsed
> twins suppress. The line to draw: does the checkout feed the thing
> UNDER TEST (fixture — fine) or the thing DOING THE JUDGING/RUNNING
> (violation)? Mechanical guard: `Runner.registerLibrarySource` refuses
> `meta::pure::functions::` elements
> (`LibraryPlatformNamespaceGuardTest`).
>
> **World-map tenet — ratified 2026-09-03 (`docs/WORLD_MAP.md`,
> `docs/TENET_CHARTER.md` Clause 6):** Pure code is NATIVES (one Java SQL
> lowering rule each), PLATFORM SEMANTICS (Java, designed from the engine's
> Pure as spec — the deletion test decides), or PROGRAMS (input to the
> compiler, whoever wrote them — never ported to Java). The unroll COMPARES
> spelled tokens and rearranges shapes; it never COMPUTES a value — the
> database does, through residual `CASE`s. The platform ships declarations
> (shapes with keys, native signatures) and views over its own system
> tables; no other Pure bodies. Residue is named, never ported.
>
> **Authoritative spec:** `core/README.md` — folder layout, per-package
> contracts, open decisions. This file is the short form.
>
> Layer design rationale: `docs/pipeline-architecture.md` (backend, HIR → MIR
> → SQL) and `docs/frontend-architecture.md` (frontend, text → HIR). **Both
> were written against the frozen `engine/` tree** — their reasoning governs,
> their type names do not. Each carries a name-mapping banner. Take names from
> here and from `core/README.md`.

## Read this first: ONE tree — the engine module is deleted (2026-08-11)

`core/` is the whole product. The legacy `engine/` module (`com.gs.legend`)
was deleted after everything real moved in:

- **Server shell** (HTTP, LSP, diagrams, query service, JSON lib,
  serializers) → `core com.legend.server`.
- **Behavioral test suite** (checker/integration tests, ~4,000 tests) →
  `core/src/test/.../com/legend/integration`.
- **Relational corpus scoreboard** → `com.legend.rcorpus` (gates 4/5:
  `//spec:corpus_duckdb`, `//spec:corpus_h2`).
- **Stress benchmarks** → ported onto `Compiler.compileModel`/`lowerResolved`.
- Hosted services and the mapping-testSuites runner were engine-lite
  inventions and were deleted, not ported —
  `docs/DEFERRED_TEST_EXECUTION.md` is the re-implementation charter.

Recover legacy sources via git history (last present at tag-commit
`4e3f0552`'s parent chain). Do not resurrect them.

## Pipeline — 11 steps, one driver

`com.legend.Compiler` (the planner, `//core:planner`) owns step ordering through J;
`com.legend.Execution` (the execution front door) runs a planned query on a
session (K). The planner's library has no execution dependency — planning never
touches a database (C2a, docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md);
a plan-only consumer depends on `//core:plan_side`. Every step is the same method
its own unit tests exercise; there is no orchestrator-only code path.

```
text                                                             [FRONTEND]
  A  lexer/            Lexer.tokenize                 text → TokenStream
  B  parser/           ElementParser.parse            tokens → ParsedModel
  C  parser/           SpecParser.parse               tokens → ValueSpecification
  D  compiler/         NameResolver.resolve           simple name → FQN
  E  normalizer/       ModelNormalizer.normalize      ParsedModel → NormalizedModel
  F  compiler/element/ PureModelContext.from          → TypedElement + ModelContext
  G  compiler/spec/    SpecCompiler                   spec + model → TypedSpec
  G½ compiler/spec/    UserCallInliner.inlineBody     TypedSpec → β-inlined TypedSpec
                                                                 [MIDEND]
  H  resolver/         StoreResolver.resolve          logical → physical TypedSpec
  I  lowering/         Lowerer.lower                  TypedSpec → sql.SqlQuery
                                                                 [BACKEND]
  J  sql/dialect/      SqlDialect.render              SqlQuery → SQL string
                                                                 [RUNTIME]
  K  exec/             Executor.execute               SQL + JDBC → ExecutionResult
```

**G½ runs on every execution path** — do not omit it when reasoning about the
pipeline.

**A second, user-facing phase vocabulary exists** and does not use letters:
`error/LegendCompileException.Phase` = `PARSE, RESOLVE, NORMALIZE, MODEL,
TYPE, MAPPING, LOWER, EXECUTE`. Note there is **no `RENDER`**. Every
user-visible error carries one of these eight.

### Entry points

| Method | Does | On error |
|---|---|---|
| `Compiler.compileModel(String)` | A→F | **STRICT** — first error aborts |
| `Compiler.buildModule(ParsedModel)` | A→F | **TOLERANT** — poison-don't-drop, returns a wall map |
| `Compiler.compileAllBodies(ctx)` | eager G over all bodies | **never throws**, returns walls |
| `Compiler.query(ctx, query)` | names + G, typed once → `TypedQuery` | STRICT |
| `TypedQuery.plan(runtime)` | G½ → J (also `.lower`, `.resultType`, `.expression`, `.target`) | STRICT — **the production seam** |
| `Execution.execute(...)` | A→K | STRICT — **the production seam** |
| `Execution.executeResolved(...)` | G½→K, the one back-half sequence | STRICT |

`compileModel` and `buildModule` differ by **one argument**:
`NameResolver.resolve(parsed)` vs `resolve(parsed, walls)`. If you want every
error rather than the first, you want `buildModule`.

`StatementExecutor` is **package-private by design** — reachable only through
`Execution.executeResolved`. The driver never re-implements a step; the
executor never decides pipeline order.

## Layer ownership (the contract)

| Layer | Owns | Forbidden |
|---|---|---|
| **Lexer** | text → tokens | anything else; it is JDK-only and stays that way |
| **ElementParser / SpecParser** | tokens → `model.PackageableElement`, `protocol.spec.ValueSpecification` | type info, semantic decisions |
| **NameResolver** | simple name → FQN over the parsed model | consulting the compiled model; type checking |
| **ModelNormalizer** | legacy mapping DSL → synthesized `FunctionDefinition`s | mutating the model; running after the typer |
| **PureModelContext** (phase F) | definitions → `TypedElement` in a `ModelContext` | type-checking bodies (that is G, on demand) |
| **SpecCompiler / Typer** | `ValueSpecification` + model → `TypedSpec`; resolves overloads; computes types and multiplicities | mutating `ModelContext`; emitting MIR |
| **StoreResolver** | logical `TypedSpec` → physical | re-running type checks |
| **Lowerer** | `TypedSpec` → `SqlQuery` | naming SQL functions; SQL syntax; importing a dialect; any `String` field encoding a SQL operation |
| **SqlDialect** | `SqlQuery` → SQL string | inferring types; rewriting HIR; consulting the model |
| **Executor** | SQL + JDBC → `ExecutionResult` | everything above |

## Invariants

> **These numbers are load-bearing.** 31 javadoc sites under
> `core/src/main/java` cite them *by number* — "AGENTS.md invariant 4" appears
> 15 times alone. **Do not renumber.** Append new rules at the end.
>
> **Enforcement is marked honestly.** `[ENFORCED]` means a test fails if you
> break it. `[CONVENTION]` means the rule is real and expected but **nothing
> checks it** — breaking it will not turn anything red, so it is on you.

### 1. The frontend does ALL AST walking and typing `[CONVENTION]`

Two cooperating compilers:

- **Element compiler** (phase F — `compiler/element/PureModelContext.from`,
  plus `ModelBuilder` and `element/{ClassCompiler,FunctionCompiler,StoreCompiler}`):
  builds the typed model from parsed definitions.
- **Expression compiler** (phase G — `compiler/spec/SpecCompiler`, `Typer`,
  `InferenceKernel` and the per-construct checkers): type-checks
  `ValueSpecification` against the model and produces typed HIR.

Together they are the **single source of truth for types**:

- Every expression and every call MUST get a type.
- Every overload MUST resolve to a concrete signature.
- If a type is missing downstream, **the frontend has a bug — fix the
  frontend**, usually `Typer` or the relevant checker.

### 2. The Lowerer does no type inference `[CONVENTION]`

The Lowerer reads **annotated HIR** — structure from typed nodes, types from
the annotations.

It MUST NOT: infer or resolve types (no model lookups, no compatibility
checks); validate correctness; inspect HIR for **type** dispatch
(`instanceof CInteger` to pick a MIR shape); parse function names to guess
types; contain SQL syntax or SQL function names; import a dialect.

It MAY: read HIR structure (function names, parameters, lambda bodies, ColSpec
names, nesting); read type annotations; pattern-match typed HIR for
**structural** dispatch; use binding tables to map a resolved signature to a
typed MIR variant.

The Lowerer emits typed MIR records — never raw SQL, never SQL function names.

### 3. The dialect owns ALL SQL rendering `[CONVENTION]`

Type-name mapping, keyword spellings, function names, syntactic quirks, and
dialect-specific decompositions all live **here**, not in lowering.

**Core's render entry points are on one interface** (`core/src/main/java/com/legend/sql/dialect/SqlDialect.java`):

```java
String render(SqlQuery query);                       // a query as text: no bound parameters
RenderedStatement renderStatement(SqlQuery query);   // a query to execute with bound values: text + its ? parameters
String render(SqlDdl ddl);                           // a table's or schema's DDL
String render(SqlDml dml);                           // rows in, rows out
```

Base impl `AnsiSqlRenderer`. **A dialect writes SQL into ONE `SqlWriter`** (2026-10-08,
docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §10, "E"): text in order, and each bound parameter at the place its `?` is
written, so a statement's parameters are in placeholder order by construction. A render method writes and returns the
writer (so its dispatching switch stays an expression javac checks, below) — never a rendered string that a caller
pastes (a piece pasted twice would carry its parameter once); a method that only spells a name, a literal or a type
returns its text; a helper that wraps SQL it does not build takes or returns a `SqlWriter.Piece`, never that SQL's
text. E is complete (2026-10-09): every dialect writes all of a query, and DML's rows, into the writer; DDL spells only
names, types and keywords, so it is text. Never edit rendered SQL after it is written. The legacy engine-text printer
(`EngineStyleH2`, the backwards-compatibility mode) writes legend-engine's own spellings directly — lowercase keywords and
function names through the `keyword` and `aggregateName` hooks, `listagg ... within group` — and its one text step is
legend-engine's own: a rendered column quoted into a FreeMarker template argument (each `'` as `\'`).

> **If you have read otherwise:** `SQLDialect`, `SqlAggregate`, `SqlRelation`
> and legend-engine's three-render-method contract on that `SQLDialect` are **engine-only**. They do not exist
> in core. `WindowAggregate` exists in neither tree.

Dialects: `AnsiSqlRenderer` → `DuckDb`; `H2` → `H2Modern`; `EngineStyleH2` →
`EngineStyleDB2` → `EngineStyleComposite`. SQLite is not a class — it is
`Lexicon.SQLITE` passed to `AnsiSqlRenderer`.

Render methods are switch **expressions** with **no `default ->` arm**, so
javac enforces exhaustiveness. When a dialect genuinely cannot express a
variant, the arm **throws** `UnsupportedOperationException` — that is still an
arm. `default ->` is not an acceptable substitute.

### 3a. The MIR is closed and pure data `[CONVENTION — dependency half ENFORCED]`

Sealed roots in `com.legend.sql` (15 files, ~1,743 LOC):

| Root | Variants |
|---|---|
| `SqlQuery` | `permits SqlSelect, SqlUnion` |
| `SqlSource` | 8 — `Pivot, SourceUrl, Table, VarSetPlaceholder, Dual, Subselect, Values, Join` |
| `SqlExpr` | 32 — incl. `Lambda` (**not** `LambdaExpr`), `Call`, `Cast`, `WindowCall`, `SqlAgg.Reducer` |
| `SqlAgg` | carries `enum Fn` (~35) + `Reducer` |
| `SqlType` | `Scalar` enum, `Decimal(p,s)`, `Array`, `Map` |
| `DateFmt` | — |

- **No method on a MIR type returns SQL.** No `toSql()`, no `render()`.
- **No MIR type references a dialect.**
- **No MIR record has a `String` field encoding a SQL operation.** The single
  carve-out is `SqlExpr.Cast(expr, pureTypeName)` — a *Pure* type name mapped
  by the dialect. Pure type names are not SQL.
- **No `FunctionCall(String name, args)` catch-all in MIR.** Every operation is
  its own typed record. New native = new MIR variant + new render arm.
- **Lambdas live in MIR as data** — `SqlExpr.Lambda`. MIR never holds a Pure
  AST node.
- **New dialect = one class implementing `render(SqlQuery)`.** MIR does not change.

`ArchitectureTest`'s `sqlLayerIsStandalone` and `sqlLayerIsFullyStandalone` enforce that `sql/` depends on nothing else.
The "no `toSql()` / no SQL-encoding `String` field" half has **no test**.

> **A record named `FunctionCall` does exist** at
> `model/RelationalOperation.java:189` — the parsed `###Relational` dynaFunc
> node. That is not MIR and is fine.

**Stop signs** — if you are writing one of these, re-read this section:

- `record Foo(...) implements SqlExpr { String toSql(...) {...} }`
- a `FunctionCall("someFunc", args)` in a lowering
- `private static String mapXxxName(String pureName)` in a lowering
- `default ->` in a render method (add a real arm; throw if unsupported)
- `sealed interface ...` with no `permits` clause

### 4. NO FALLBACKS. NO DEFAULTING. `[CONVENTION]`

*The most-cited invariant in the codebase — 15 javadoc sites.*

- The **whole point** of the compiler is to catch mistakes early.
- If a type is unknown, **fail**. Do not guess, default, or fall back.
- If a binding is missing for a resolved overload, **throw** — never fall
  through to a stringly-typed catch-all.
- Every defaulting branch is a bug hiding behind a safety net.

### 5. Lazy loading of user packageable elements `[CONVENTION in core]`

Cross-project dependencies must not force-load the transitive graph.

- **Platform types** (everything in `builtin/Pure.java`) are always loaded and
  safe to classify eagerly.
- **User types** are referenced by FQN. `Type.ClassType("my::app::Person")` is
  an FQN in a typed wrapper; it does **not** imply the class is loaded.
- Structural access (`findProperty`, `isSubtype`, superclass walks) MUST go
  through `ModelContext.findClass` / `findEnum` / `findFunction` — the sole
  layer that owns load triggering.
- Long-lived fields hold **FQN strings**, never resolved element objects.
  `TypedClass.superClassFqns: List<String>` is the canonical example.
- Never walk a dependency graph by field access; go through `ModelContext`
  every step.

> **The two automated guards — `NoEagerTypeReferencesTest` and
> `NoEagerUserClassLoadsTest` — died with the engine module.** Core has
> **no lazy-loading enforcement at all**. If
> you break this in core, nothing turns red.

### 6. F must not trigger G `[CONVENTION]`

Function bodies stay as `ValueSpecification` inside `TypedFunction` and are
type-checked on demand. Compiling elements must not compile specs.

### 7. Store-only nodes must not escape their phase `[ENFORCED — runtime]`

`TypedGetAll` and `TypedUserCall` MUST NOT survive phase H.
`StoreResolver.assertNoStoreOnlyEscapees:220` walks every resolved statement
and throws, naming the construct; `StoreResolverTest` pins it. Likewise no
store-only node reaches the Lowerer — those are "resolver bug" walls.

### 8. The wall `[ENFORCED — ArchitectureTest, rule "THE WALL"]`

No `com.legend..` → `com.gs.legend..` dependency, ever. Also: no `util/`
package anywhere (`ArchitectureTest`, the `..util..` rule).

`ArchitectureTest` is mostly dependency-direction rules (37 tests as of
2026-09-29, including `staticFieldsAreFinal`); it asserts no sealedness,
record-ness, or exhaustiveness (plan W1.11 adds an exhaustiveness check).
Line numbers drift: search by rule name. It uses its own
numbering ("6g", "7a-c") that does **not** map to this list or to
`core/README.md`'s. Do not merge the numbering schemes.

> `core/README.md` has its **own** 12-invariant list, also cited by number from
> code (`TypedElement.java:11`, `Type.java:16` cite "core/README invariant 11").
> The two lists are separate. Do not merge or renumber either.

## Standing documents

**Current work: the build rebuild and the self-contained upgrade bump. Start at
`docs/build-inventory/program/START_HERE.md`** (the goal, the state, the rules, how to run
every check, one brief per remaining phase); the plan is `docs/REBUILD_PROGRAM_2026_10_06.md`
(its §0 says what the whole program is for). `docs/IN_FLIGHT.md` says who is working.
**The compiler rebuild (`docs/EXECUTION_PLAN_2026_09_26.md`) is parked since 2026-10-04**
(`docs/IN_FLIGHT.md`): do not follow its §0 checklist unless the user resumes it. Every deliberate difference from legend-pure/legend-engine is
a row of `docs/SEMANTICS_REGISTER.md`. Older plans and queues (PROGRAM_MAP,
ONE_PLATFORM_PLAN, END_TO_END_PLAN, OPEN_REGISTER, ENGINEERING_LOG's queue,
REAL_PLAN_2026_09_25) are history, not instructions.

The live process layer. None of it is reachable from any always-loaded file
otherwise, which is how it drifted:

| Doc | What it is |
|---|---|
| `docs/build-inventory/program/START_HERE.md` | **Start here**: the build rebuild and the self-contained bump (the plan: `docs/REBUILD_PROGRAM_2026_10_06.md`) |
| `docs/EXECUTION_PLAN_2026_09_26.md` | The compiler rebuild's plan (rev H4): **parked since 2026-10-04**, reference until the user resumes it |
| `docs/SEMANTICS_REGISTER.md` | Every deliberate difference from the reference, with evidence |
| `docs/GATES.md` | The gate chain. **Read before claiming anything is green.** |
| `docs/ENGINEERING_LOG.md` | Standing tenets (its active queue is history; the plan above replaced it) |
| `docs/TENETS.md` | Eager-Knowledge / lazy-Work, the north star |
| `docs/WORLD_MAP.md` | Java vs Pure vs input: the three kinds of Pure code, the prelude, compare-not-compute, the decision procedure |
| `docs/AUDITS.md` | Audit index and reading order |
| `docs/CORPUS_BURNDOWN_HANDOFF.md` | History: the corpus burn-down program (paused; its ledger is Maven-era) |
| `docs/RELATIONAL_CORPUS.md` | History: the Maven-era corpus scoreboard (not regenerated by the Bazel chain; the roster files are the gate) |
| `docs/OUTSTANDING.md` | History: the Maven-era non-passing ledger (not regenerated) |

## Pushing to main

Sessions may push to `main` directly (decision D17 of `docs/BAZEL_FIRST_CLASS_WORKPLAN_2026_10_03.md`, the
fast loop), on three conditions. `main`'s ruleset (applied with P0-14) blocks direct pushes from everyone
except the repository owner, and sessions run as the owner, so these conditions are kept by convention, not
by GitHub:

1. **The local gate is green.** Rebase onto the latest `origin/main`, then
   `bazel test --lockfile_mode=error //gates:local` (CI's lock check: a `MODULE.bazel` edit without its lock
   fails here, not in CI); push only if it passes. Say so in the commit message ("local gate: //gates:local
   green"). It is lighter than CI: the heavy lanes and `bazel build //...` run there only.
2. **Windows-sensitive changes go through a PR: SUSPENDED until the Bazel program ends** (USER, 2026-10-04:
   most of its items touch `.github/CODEOWNERS` paths, and the rule would put each one behind a CI wait;
   restored by P8-01). Until then a change to those paths may be pushed like any other, and `main`'s CI on
   Windows plus rule 3 are the Windows guard. A change large enough to need an independent review still
   goes through a PR (`.github/pull_request_template.md`).
3. **Revert on red.** After a push, watch `main`'s CI. If it goes red, revert your own commit at once, then fix
   it in a PR. Tell the user either way.

**The build rebuild program lands without PRs** (the user, 2026-10-06): an audit, the local gate, one full CI run on
the branch, then that exact commit pushed to `main` (`docs/build-inventory/program/START_HERE.md` §4). Whether other
work keeps rule 2's PR path is open (`docs/build-inventory/program/PHASE_8.md` OD-9).

A documentation-only PR gets no gate run (`gate.yml`'s `paths-ignore`), so `gates green` never reports on it:
the owner merges it by bypass.

## Common mistakes (don't repeat)

1. **`(int) longValue`** — boxed `Long` cannot raw-cast to `int`. Use `.intValue()`.
2. **Hardcoding SQL in lowering** — emit a typed MIR variant; the dialect renders it.
3. **Naming SQL functions in lowering** — no `FunctionCall("name", args)`.
4. **`String mapXxxName(String pureName)` helpers in lowering** — the smoking
   gun of stringly-typed dispatch leaking SQL into the wrong layer.
5. **Adding a normalization instead of fixing the root cause.**
6. **Adding fallbacks or defaults** — fail loudly; fix the compiler.
7. **"Fixing" a fallback by changing what it defaults to** — if a default
   branch is being hit: (a) make it throw, (b) find why the upstream layer
   produced nothing, (c) fix that layer. The fallback existing is the bug.
8. **`default ->` in a render method** — add the missing arm.
9. **Type inference in the Lowerer** — it reads types, never infers them.
10. **Making the compiler lenient on missing model elements** — it MUST throw
    if a referenced class, property or type is absent. If a test fails because
    a class is not found, **fix the test's model setup**. Never degrade silently.
11. **(Retired with Maven, 2026-09-22 — kept so the numbering holds.)** Under
    Maven, `mvn -pl <module> test` resolved `legend-lite-core` from `~/.m2`, not
    the reactor, and silently tested a stale jar. Bazel builds every dependency
    from source on every `bazel test`; there is no installed jar to go stale. The
    lesson that remains: a gate result is only about the target that produced it
    — `bazel test //...` for the whole chain (docs/GATES.md).