# `core/` — Legend Lite Compiler (Strangler Fig)

> **Read this before adding any file under `core/`.**

Clean reimplementation of the Legend Lite compiler pipeline (text → SQL
→ results). Lives alongside `engine/` during a
[Strangler Fig](https://martinfowler.com/bliki/StranglerFigApplication.html)
migration. When `core/` reaches parity with `engine/`, `engine/` deletes.

`core/` is **self-sufficient by construction**: it includes everything
needed to parse, compile, optimize, render, AND execute a Pure query —
including services, runtimes, executor, JDBC plumbing. Tests run
end-to-end inside this module.

## The wall — non-negotiable

**Nothing under `com.legend.*` may import anything under `com.gs.legend.*`**
(other than JDK + JUnit + JDBC drivers + ArchUnit in tests).

Two layers enforce this:

1. **Maven**: `core/pom.xml` does not declare `legend-lite-engine` as a
   dependency. Engine classes are not on `core/`'s compile classpath.
2. **ArchUnit**: `ArchitectureTest` rejects any `com.legend..` →
   `com.gs.legend..` dependency at CI time. Belt-and-suspenders for if
   the dep ever gets added by mistake.

The wall has no exceptions, no `@Suppress`, no shared utility classes,
no generated bridges. **If `core/` needs something `engine/` has,
reimplement it in `core/`.** This is the cost we accept for getting it
right.

## Pipeline (1 entry point, 11 steps)

```
text                                                            [FRONTEND]
  ──▶  lexer/        Lexer.tokenize                              text → tokens
  ──▶  parser/       ElementParser, SpecParser                   tokens → parsed syntax
  ──▶  parser/       NameResolver                                simple → FQN
  ──▶  normalizer/   MappingNormalizer                           Decl → Decl (mappings → fns)
  ──▶  compiler/     ElementCompiler                             Decl → Def     (compiled model)
  ──▶  compiler/     SpecCompiler                                Spec + model → TypedSpec
                                                                [MIDEND]
  ──▶  resolver/     MappingResolver                             logical → physical TypedSpec
  ──▶  sql/build/    SqlBuilder                                  TypedSpec → SQL relation tree
                                                                [BACKEND]
  ──▶  sql/dialect/  Dialect.render                              SQL relation tree → SQL string
                                                                [RUNTIME]
  ──▶  executor/     PlanExecutor                                SQL string + JDBC → results
```

| # | Step | Package | Driver | Output |
|---|---|---|---|---|
| 0 | bootstrap | `builtin/` | (static registry) | platform types + native sigs |
| A | lex | `lexer/` | `Lexer` | `Token` stream |
| B | parse elements | `parser/` | `ElementParser` | `parser.element.PackageableElement` (`ClassDefinition`, `MappingDefinition`, …) wrapped in `ParsedModel` |
| C | parse specs | `parser/` | `SpecParser` | `parser.spec.ValueSpecification` (`LambdaFunction`, `AppliedFunction`, `Variable`, `CString`, …) |
| D | resolve names | `parser/` | `NameResolver` | (same shapes, FQN-rewritten) |
| E | normalize model | `normalizer/` | `MappingNormalizer` | `PackageableElement` (mappings desugared into `FunctionDefinition`s) |
| F | compile elements | `compiler/` | `ElementCompiler` | `compiler.element.TypedElement` (`TypedClass`, `TypedMapping`, …) + `ModelContext` |
| G | compile specs | `compiler/` | `SpecCompiler` | `compiler.spec.TypedSpec` (`TypedFilter`, `TypedProject`, …) + `Dependencies` |
| H | resolve mapping | `resolver/` | `MappingResolver` | `TypedSpec` (physical stamps applied) |
| I | build SQL | `sql/build/` | `SqlBuilder` | `sql.Rel`, `sql.ScalarOp` (dialect-free) |
| J | render SQL | `sql/dialect/` | `Dialect` | SQL string |
| K | execute | `executor/` | `PlanExecutor` | result rows |

Wired by `com.legend.Compiler.compile(...)`.

## Folder layout

Folder = package, 1:1. Single prefix `com.legend.`.

```
core/src/main/java/com/legend/
│
├── Compiler.java                      ← com.legend.Compiler — entry point
├── package-info.java                  ← module invariants
│
├── builtin/                           phase 0: platform types + native function sigs
│   ├── BuiltinRegistry.java
│   └── BuiltinClassRegistry.java
│
├── lexer/                             A. text → tokens
│   ├── Lexer.java                           static entry: `tokenize(String) → TokenStream`
│   ├── TokenStream.java                     immutable result; int[] internals + Token materialization API
│   ├── Token.java                           materialized record (TokenType, text, start, end)
│   └── TokenType.java
│
├── parser/                            B,C,D. tokens → resolved syntax
│   ├── ElementParser.java                   B driver
│   ├── SpecParser.java                      C driver
│   ├── NameResolver.java                    D driver
│   ├── ImportScope.java
│   ├── SourceLocation.java
│   ├── ParsedModel.java                     B output wrapper: (List<PackageableElement>, ImportScope)
│   ├── element/                             B output: parsed packageable elements (engine names verbatim)
│   │   ├── PackageableElement.java              sealed root
│   │   ├── ClassDefinition.java
│   │   ├── AssociationDefinition.java
│   │   ├── AssociationMappingDefinition.java
│   │   ├── EnumDefinition.java
│   │   ├── ProfileDefinition.java
│   │   ├── FunctionDefinition.java
│   │   ├── MappingDefinition.java               (desugared into FunctionDefinition in step E)
│   │   ├── MappingInclude.java
│   │   ├── DatabaseDefinition.java
│   │   ├── JoinChainElement.java
│   │   ├── RelationalOperation.java
│   │   ├── PropertyMappingValue.java
│   │   ├── ConnectionDefinition.java
│   │   ├── ConnectionSpecification.java         sealed (JsonModelConnection, RelationalConnection, …)
│   │   ├── JsonModelConnection.java
│   │   ├── AuthenticationSpec.java              sealed
│   │   ├── RuntimeDefinition.java
│   │   ├── ServiceDefinition.java
│   │   ├── StereotypeApplication.java
│   │   └── TaggedValue.java
│   └── spec/                                C output: parsed value specifications (engine names verbatim)
│       ├── ValueSpecification.java              sealed root
│       ├── LambdaFunction.java
│       ├── AppliedFunction.java
│       ├── AppliedProperty.java
│       ├── Variable.java
│       ├── NewInstance.java
│       ├── EnumValue.java
│       ├── PackageableElementPtr.java
│       ├── PureCollection.java
│       ├── TdsLiteral.java
│       ├── UnitInstance.java
│       ├── GraphFetchTree.java
│       ├── ColumnInstance.java
│       ├── ColSpec.java
│       ├── ColSpecArray.java
│       ├── TypeAnnotation.java                  sealed
│       ├── CBoolean.java, CString.java, CInteger.java, CFloat.java, CDecimal.java,
│       └── CByteArray.java, CDateTime.java, CStrictDate.java, CStrictTime.java, CLatestDate.java
│
├── normalizer/                        E. mapping desugar
│   └── MappingNormalizer.java               PackageableElement → PackageableElement
│                                              (MappingDefinition → synth FunctionDefinition)
│
├── compiler/                          F,G. parsed → typed
│   ├── ElementCompiler.java                 F driver
│   ├── SpecCompiler.java                    G driver (was TypeChecker in engine)
│   ├── Dependencies.java                    G secondary output: used FQNs/properties
│   ├── ModelContext.java                    read-only model view
│   ├── SymbolTable.java
│   ├── element/                             F output: compiled elements (Typed* uniform)
│   │   ├── TypedElement.java                    sealed root
│   │   ├── TypedClass.java
│   │   ├── TypedAssociation.java
│   │   ├── TypedAssociationMapping.java
│   │   ├── TypedEnum.java
│   │   ├── TypedProfile.java
│   │   ├── TypedFunction.java                   (compiled MappingDefinition desugars into this in E)
│   │   ├── TypedMapping.java                    (slated for removal post-step-E; kept for staging)
│   │   ├── TypedDatabase.java
│   │   ├── TypedConnection.java
│   │   ├── TypedRuntime.java
│   │   ├── TypedService.java
│   │   ├── store/                               relational store metamodel
│   │   │   ├── Table.java
│   │   │   ├── Column.java
│   │   │   └── Join.java
│   │   └── type/                                M3 type system
│   │       ├── Type.java                            sealed
│   │       ├── Primitive.java
│   │       ├── ClassType.java
│   │       ├── RelationType.java
│   │       ├── FunctionType.java
│   │       └── Multiplicity.java
│   ├── spec/                                G output: typed value specs (symmetric with parser/spec/)
│   │   ├── TypedSpec.java                       sealed root (engine name verbatim)
│   │   ├── (relation-shaped variants — flat)
│   │   │   TypedGetAll, TypedFilter, TypedProject, TypedExtend,
│   │   │   TypedSort, TypedSlice, TypedGroupBy, TypedDistinct,
│   │   │   TypedJoin, TypedFlatten, TypedFrom, TypedSerialize, ...
│   │   └── (scalar-shaped variants — flat)
│   │       TypedVar, TypedLit, TypedLambda, TypedCall,
│   │       TypedPropertyAccess, TypedNewInstance,
│   │       TypedUserCall, TypedIf, TypedLet, TypedCast, ...
│   └── checker/                             per-native dispatch (G internals)
│       ├── NativeChecker.java                   sealed
│       ├── FilterChecker.java
│       ├── ProjectChecker.java
│       ├── ExtendChecker.java
│       ├── SerializeChecker.java
│       └── ...
│
├── resolver/                          H. logical → physical TypedSpec
│   ├── MappingResolver.java                 driver
│   └── rule/
│       ├── BindRule.java                        sealed
│       ├── InlineUserCall.java                  β-reduce TypedUserCall
│       ├── InlineClassFetch.java                rule 1: GetAll → mapping body splice
│       ├── BindPhysicalColumn.java              rule 2: stamp physical column
│       ├── AssociationToJoin.java               rule 3: nav path → Join chain
│       └── ImplicitSerialize.java               rule 4: graph-fetch envelope
│
├── sql/                               I,J. SQL backend (one umbrella)
│   ├── Rel.java                             sealed relational root
│   ├── ScalarOp.java                        sealed scalar root
│   ├── (Rel variants — flat: Scan, Filter, Project, Join, Aggregate, ...)
│   ├── (ScalarOp variants — flat: one record per native; no FunctionCall(String,args))
│   ├── build/                               I driver + per-op build rules
│   │   ├── SqlBuilder.java
│   │   └── rule/
│   │       ├── FilterBuild.java
│   │       ├── ProjectBuild.java
│   │       └── ...
│   └── dialect/                             J. per-dialect rendering
│       ├── Dialect.java                         sealed
│       ├── DuckDbDialect.java
│       ├── H2Dialect.java
│       └── SqliteDialect.java
│
├── plan/                              final compile output
│   ├── ExecutionPlan.java                   matches engine's name; what Compiler.compile returns
│   ├── ResultFormat.java                    TDS / Graph / Scalar
│   └── ExecutionMode.java                   SNAPSHOT / STREAMING
│
└── executor/                          K. SQL string + JDBC → result rows
    ├── PlanExecutor.java
    ├── ConnectionResolver.java
    ├── Result.java
    ├── Row.java
    └── Column.java                          (result-side column; distinct from element/store/Column)
```

## Naming conventions

| Convention | Example | Rationale |
|---|---|---|
| **Single top prefix** `com.legend.*` | `com.legend.lexer.Lexer` | Distinct from `com.gs.legend.*` (engine); makes the wall unambiguous |
| **Folder = package**, 1:1 | folder `lexer/` ↔ package `com.legend.lexer` | One name per concept |
| **Noun packages** | `lexer/`, `parser/`, `compiler/`, `resolver/`, `executor/` | Matches engine convention; matches Java idioms |
| **Element / Spec symmetry, everywhere** | `parser/element/` + `parser/spec/`; `compiler/element/` + `compiler/spec/`; `ElementParser`+`SpecParser`; `ElementCompiler`+`SpecCompiler` | "Element" = packageable element. "Spec" = value specification. Same pair end-to-end, no ad-hoc `Model`/`Query` mixing |
| **Pure data records** for every IR node | `record TypedFilter(...)` | No identity, free equality, free serialization |
| **Sealed roots** for every variant family | `sealed interface TypedSpec permits ...` | Compile-time exhaustiveness on every dispatch site |
| **Parser records = engine class names verbatim** | `ClassDefinition`, `LambdaFunction`, `AppliedFunction`, `CString` | Maximizes test portability against the engine corpus |
| **`Typed*` prefix** for everything `compiler/` produces | `TypedClass`, `TypedMapping`, `TypedFilter`, `TypedProject` | One uniform prefix for the typed world. Elements and specs both follow the rule — sealed roots `TypedElement` + `TypedSpec` |
| **No `FunctionCall(String, args)`** anywhere | each native is its own typed record | Forces enumeration; kills stringly-typed dispatch |
| **No `default ->`** in any sealed switch | every arm explicit, throw on unsupported | javac enforces variant coverage |
| **No `util/` package** | helpers live with the code that needs them | `util/` is a code smell; ArchUnit blocks it |
| **FQN strings, not live refs**, in long-lived fields | `superClassFqn: String`, not `superClass: TypedClass` | Lazy loading; no transitive force-load |

## Strong invariants (enforced by structure + tests)

1. **The wall.** No `com.legend..` → `com.gs.legend..` dependency. ArchUnit.
2. **No `util/` package.** ArchUnit.
3. **Sealed everywhere a hierarchy exists.** `PackageableElement`, `ValueSpecification`, `TypedElement`, `TypedSpec`, `Type`, `Rel`, `ScalarOp`, `Dialect`, `NativeChecker`, `BindRule`, `ConnectionSpecification`, `AuthenticationSpec`. ArchUnit (sealed-or-final assertion on listed packages).
4. **Records for all data carriers** under `parser/element/`, `parser/spec/`, `compiler/element/`, `compiler/spec/`, `sql/`, `plan/`. ArchUnit.
5. **No `default ->` arms** in `sql/build/`, `sql/dialect/`, `resolver/rule/`, `compiler/checker/`. Use explicit `throw new UnsupportedOperationException(...)` arms instead. javac's exhaustiveness check enforces this when sealed roots have explicit `permits`.
6. **No `FunctionCall(String, args)`** type. Grep test asserts no such record shape.
7. **`F` (compile elements) MUST NOT trigger `G` (compile specs).** Function bodies stay as `ValueSpecification` inside `TypedFunction`; type-check on demand. (Engine violates this in `buildPureFunctions`; we don't carry the violation forward.)
8. **No mutable sidecar state across passes.** Each step takes input, returns output. No `IdentityHashMap<TypedSpec, ?>` threaded across phase boundaries. Pass-local caches are fine, labelled and confined.
9. **`TypedGetAll` and `TypedUserCall` MUST NOT survive `H` (resolver/).** Post-resolve walk asserts neither variant occurs. A post-condition test in `resolver/` enforces it.
10. **`TypedPropertyAccess.physicalColumn` MUST be present post-resolve.** Post-condition test.
11. **`compiler/element/Typed*` reference other elements by FQN string, not live ref.** Lazy loading: `superClassFqn: String`, not `superClass: TypedClass`. Inheriting AGENTS.md §5 from engine.
12. **`sql/` is closed and pure data.** No `toSql()` method, no `Dialect` import, no `String` field encoding a SQL operation. Inheriting AGENTS.md §3a from engine.

## How to add a file under `core/`

1. Read this README.
2. Find the package in the layout above. **If your file doesn't fit any listed package, stop and discuss before inventing a new one.**
3. Records first; classes only for services (`Lexer`, `ElementParser`, `SpecCompiler`, `MappingResolver`, ...) and per-step drivers.
4. Sealed root for any new variant family.
5. Run `mvn -pl core test` — `ArchitectureTest` must stay green.

## Testing strategy

- **Unit tests** per step in `core/src/test/java/com/legend/<step>/`.
- **Pipeline tests** end-to-end through `Compiler.compile(...)`, asserting SQL string output for golden Pure inputs.
- **Execution tests** end-to-end through `PlanExecutor.execute(...)`, asserting result rows against an in-memory DuckDB / SQLite.
- **Parity harness** (lives in a separate test-only Maven module that depends on BOTH `core/` and `engine/`): re-runs the existing engine + PCT suites against both back-ends. As `core/` grows, the V2 column climbs from 0% green toward parity. The harness depends on `core/`; `core/` itself never depends on the harness or on `engine/`.

## Open decisions to revisit

Things we deferred deliberately. Each entry: **what's deferred**, **why now**, **when to revisit**, **options on the table**.

### D-1. Body capture format for derived properties, constraints, function bodies

- **What.** `ClassDefinition.DerivedPropertyDefinition.expression`, `ClassDefinition.ConstraintDefinition.expression`, `FunctionDefinition.body`, and `ServiceDefinition.functionBody` are currently captured as raw `String` (source-text slice via `reconstructText`).
- **Why now.** Phase C (`SpecParser` for `ValueSpecification`) doesn't exist yet, so we can't parse the bodies. Text capture is the smallest placeholder.
- **Risk.** Lazy *compilation* of bodies is required by AGENTS.md invariant 5 (no force-loading transitive graphs). Lazy *parsing* of bodies is **not** required — parsing is structurally local and cannot trigger cycles. Choosing raw text was a convenience, not an architectural necessity. Engine conflates the two; core/ should not.
- **When to revisit.** Just before / during Phase C lands `SpecParser`.
- **Options.**
  1. **Keep raw text.** Re-lex + re-parse on demand. Simplest, matches engine. Loses early syntax-error detection on bodies.
  2. **Eager parse into typed AST.** Replace `String expression` with `ValueSpecification expression`. Catches body syntax errors at parse time. ElementParser depends on SpecParser (clean one-way).
  3. **`TokenSpan(TokenStream, int startToken, int endTokenExclusive)`** (preferred). Phase B emits spans; Phase C walks each span with SpecParser, producing a refined `ParsedModel`. No re-lexing; bodies independently parseable; `ParsedModel` carries a reference to its source `TokenStream`.

  **Tentative leaning: Option 3.** Records reshape in one focused commit when `SpecParser` has a concrete shape.

### D-2. Strict unknown-key handling in Runtime / Connection / Service (closed parity fix)

- **What.** Unknown top-level keys in `Runtime`/`RelationalDatabaseConnection`/`Service` bodies throw `ParseException` naming the offending key.
- **What `legend-lite/engine` did.** Silently dropped unknown keys via `skipToSemicolon()` (see `@/Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/parser/PureModelParser.java:2524-2527`) — a leniency that masks malformed Pure source.
- **What FINOS `legend-engine` does.** Grammar-driven (ANTLR4): unknown keys are syntax errors at the grammar level. The walker calls `validateAndExtractRequiredField`/`validateAndExtractOptionalField` over named contexts (see `@/Users/neema/legend/legend-engine/legend-engine-core/legend-engine-core-base/legend-engine-core-language-pure/legend-engine-language-pure-grammar/src/main/java/org/finos/legend/engine/language/pure/grammar/from/runtime/RuntimeParseTreeWalker.java:99`); the term `skipToSemicolon` appears nowhere in `legend-engine`.
- **Status.** Closed. `legend-lite/core`'s strictness matches FINOS engine. The risk I flagged earlier ("engine accepts more, we reject") was actually risk of breaking the GS port's loose parses — engine would reject those too. No real engine-parity gap.

### D-6. View filter reference shape (closed parity fix, full grammar coverage)

- **What.** `DatabaseDefinition.ViewDefinition.filter` is a nullable sealed `FilterMapping` with two variants:
  - `Direct(FilterPointer filter)` &mdash; the simple form (`~filter F` or `~filter [DB] F`)
  - `JoinMediated(String sourceDb, List<JoinChainElement> joins, FilterPointer filter)` &mdash; the join-mediated form (`~filter [DB1] @J1 > @J2 | [DB2]? F`)

  `FilterPointer` itself is sealed: `Local(name)` for ambient resolution (search enclosing db + includes), `Cross(db, name)` for explicit cross-database lookup. **No nullable fields anywhere in the type.**
- **What `legend-lite/engine` did.** Encoded just the bare `~filter F` form as a magic `RelationalOperation.Literal.string("~filter:" + name)` (see `@/Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/parser/PureModelParser.java:1376`) — missed both the `[DB]` qualifier and the join-mediated form entirely.
- **What FINOS `legend-engine` does.** Wraps a flat `FilterPointer { String db; String name; }` in a flat `FilterMapping { FilterPointer filter; List<JoinPointer> joins; }`. The four grammar forms are encoded as field combinations: `joins.isEmpty()` distinguishes simple from join-mediated; `db == null` distinguishes ambient from cross-database.
- **Status.** Closed. `legend-lite/core` supports all four grammar forms with a structurally honest sealed encoding. Two two-variant sealed types (`FilterMapping` and `FilterPointer`) carve the problem along the axes where consumers actually dispatch: "how do we reach the filter" (Direct / JoinMediated) and "where do we look it up" (Local / Cross). Pattern matching at consumer sites is compiler-checked exhaustive.
- **Why sealed instead of nullable, per design discussion.** Engine's `FilterPointer.db == null` carries semantic meaning ("search enclosing db + includes", not just "absence of value") different from `db != null` ("look only here"). That's a structural difference, not a presence/absence one — the case where sealed types pay for themselves. Pinned by tests `viewFilterDirectLocal`, `viewFilterDirectCross`, `viewFilterJoinMediatedLocalTarget`, `viewFilterJoinMediatedCrossTarget`, `joinMediatedFilterRequiresSourceDbQualifier`, `filterMappingJoinMediatedRejectsEmptyJoins`.

### D-7. Bare identifiers rejected in Database-context expressions (closed parity fix)

- **What.** A bare identifier inside a Database-context expression (Filter / Join / MultiGrainFilter / view filter), e.g. `Filter ActiveFilter(IS_ACTIVE = 1)`, throws a `ParseException` with the message `"Missing table or alias for column 'IS_ACTIVE'"`.
- **Engine parity.** This matches FINOS `legend-engine`'s behavior exactly. `RelationalParseTreeWalker.generateTableAlias` throws the same `EngineException` message when it encounters a column reference whose surrounding scope didn't supply a table alias.
- **What B.4a originally did.** Two earlier mistakes, both now closed:
  1. **Initial port** copied `legend-lite/engine`'s sentinel hack: `ColumnRef(null, "IS_ACTIVE", "IS_ACTIVE")` (table == column). That was a `legend-lite/engine` invention, *not* engine's behavior.
  2. **First "fix"** introduced a nullable `table` so we could capture `ColumnRef(null, null, "IS_ACTIVE")` and defer resolution to Phase D. That was the right intuition but the wrong layer: engine resolves bare-in-mapping-context at parse time using ScopeInfo and rejects bare-in-database-context outright. We now match that.
- **Net result.** `ColumnRef.table` is non-nullable. The parser AST never carries implicit-table column refs in either Database or Mapping context (B.4b will resolve mapping-context bare identifiers eagerly using the class mapping's main table, also matching engine).
- **Pinned by test** `filterRejectsBareIdentifierMatchingEngine`.

### D-3. Service test-suites parsed lazily

- **What.** `ServiceDefinition.testSuitesSource` holds the raw text inside a `testSuites { ... }` (or `testSuites [ ... ]`) block. Engine drops this data entirely; core/ preserves it as a `String` to be parsed once `MappingDefinition.TestSuiteDefinition` lands.
- **Why now.** Parsing test suites into typed records requires `MappingDefinition.TestSuiteDefinition` shape, which lands with B.4 (`Mapping`). Holding the raw text keeps the data intact without committing to a shape.
- **When to revisit.** When B.4 lands `MappingDefinition`. The `testSuitesSource` field on `ServiceDefinition` is a forcing function — it's in everyone's face and `null`-checked at every read site.
- **Options at revisit time.** (a) Replace `testSuitesSource: String` with `testSuites: List<TestSuiteDefinition>` and parse during B.4. (b) Keep the raw-text field and add a side-table `Map<ServiceDefinition, List<TestSuiteDefinition>>` produced by a later pass. **Preferred: (a)**, in one focused commit.

## Status

- [x] Module skeleton + Maven wiring
- [x] Architecture wall test (`com.legend.* ⇏ com.gs.legend.*`)
- [ ] Phase 0: builtins
- [x] Phase A: lexer (`Lexer` + `TokenStream` + `Token` + `TokenType`; 25 unit tests)
- [~] Phase B: parser/element + ElementParser
  - [x] B.1: scaffolding + `Class` (imports, properties, type params, extends, native, stereotypes, tagged values; 29 unit tests)
  - [x] B.2: derived properties + constraints + `Association` + `Enum` + `Profile` (42 unit tests; lazy body text capture)
  - [x] B.3: `function` + `Service` + `Runtime` + `RelationalDatabaseConnection` (60 unit tests, 87 total; strict unknown-key handling — D-2; testSuites raw-text capture — D-3; FunctionDefinition deliberately omits engine's compiler-cache fields)
  - [~] B.4: `Database` + `Mapping`
    - [x] B.4a: `Database` (21 tests, 108 total) — full relational expression sub-AST: ColumnRef, TargetColumnRef, Literal, FunctionCall, Comparison, BooleanOp, IsNull, IsNotNull, Group, ArrayLiteral, JoinNavigation; full view-filter sub-AST: sealed `FilterMapping` (Direct/JoinMediated) and `FilterPointer` (Local/Cross); sliced eagerly because the relational sub-grammar is small and bounded (≠ Pure value expressions, which still defer per D-1); audit-driven cleanups closed all engine-parity gaps: D-2, D-6, D-7.
    - [ ] B.4b: `Mapping` shell + Relational class mappings
    - [ ] B.4c: Association mappings
    - [ ] B.4d: Enumeration mappings
    - [ ] B.4e: M2M class mappings
    - [ ] B.4f: Mapping test suites (parse the `testSuitesSource` captured in B.3 — closes D-3)
- [ ] Phase C: parser/spec + SpecParser
- [ ] Phase D: NameResolver
- [ ] Phase E: MappingNormalizer
- [ ] Phase F: ElementCompiler + compiler/element (TypedElement family)
- [ ] Phase G: SpecCompiler + compiler/spec (TypedSpec family) + compiler/checker
- [ ] Phase H: MappingResolver + resolver/rule
- [ ] Phase I: SqlBuilder + sql/ data records
- [ ] Phase J: Dialect + sql/dialect
- [ ] Phase K: PlanExecutor
- [ ] Parity harness (separate test module)
