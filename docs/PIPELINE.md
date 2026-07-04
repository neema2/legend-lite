# The Pipeline — every pass, as built

The canonical map of legend-lite/core's compile pipeline. Phase letters match
`package-info.java` and the phase docs; each stage names its class, its
input → output, and what dies there. `PipelineStageFailureTest` drives one bad
input per stage through the FULL pipe and pins that it fails AT that stage.

```
MODEL TEXT ─lex/parse→ ParsedModel ─resolve→ ParsedModel(FQN) ─normalize→ NormalizedModel ─F→ PureModelContext
                                                                                                   │
QUERY TEXT ─parse→ ValueSpecification ─resolveQuery→ ValueSpecification(FQN) ─G→ TypedSpec HIR    │
                                                                                                   │
              TypedSpec ─[H: StoreResolver, NOT BUILT]→ ─I: Lowerer+Fold→ SqlQuery ─J: DuckDb→ SQL ─K→ rows
```

## Model side — `Compiler.compileModel(text)`

1. **Lex + parse-element** — `ElementParser.parse` → `ParsedModel`. All element
   grammars; function bodies parsed by `SpecParser` inline. Dies: grammar
   violations (`ParseException`).
2. **Name resolution (D)** — `NameResolver.resolve` → FQN-rewritten AST.
   Scope = file imports + platform prelude (from `Pure`'s indexed catalog).
   Pure AST→AST. Dies: ambiguous simple names (loud, lists candidates).
3. **Normalize (E)** — `ModelNormalizer.normalize` → `NormalizedModel`.
   "Everything becomes a function": derived properties/constraints lift into
   synthesized functions (`<owner>$prop$<name>`), legacy mappings desugar into
   canonical binding tables + lifted realizing functions, services externalize.
4. **Element compile (F)** — `PureModelContext.from(normalized)`:
   - `ModelBuilder` ingests elements into FQN indexes.
   - `TypeClassifier` (kind kernel) + per-kind compilers (`FunctionCompiler`,
     `ClassCompiler`, `StoreCompiler`) wire up.
   - **`ModelIntegrity.check` — THE eager reference-safety pass (F.a + F.b,
     one walk)**: every property/signature type classifies; every realizer
     function exists (constraints also shape-checked `Boolean[1]`); association
     ends resolve; mapping bindings name real classes/associations with
     correctly-shaped realizers. An invalid model NEVER becomes a context.
   - Then demand-driven, memoized materialization: `findType` (cheap kind
     manifest), `findClass`/`findFunction` (lazy `Typed*`), `findTable`
     (SQL→Pure column lattice), `findProperty` (inheritance walk).
     Work is lazy; knowledge and safety are eager.

## Query side — `Compiler.compileQuery(model, query)`

5. **Parse-spec** — `SpecParser.parse` → `ValueSpecification`.
6. **Query resolution** — `NameResolver.resolveQuery`: real legend's
   SECTIONLESS-lambda scope (`CompileContext.META_IMPORTS`) — prelude names
   bare (`JoinKind.INNER`), user elements FQN. Unresolved bare names pass
   through to fail in G with the "fully qualified" hint.
7. **Type check (G)** — `SpecCompiler.typeExpression` → **`TypedSpec` HIR**.
   Four layers: `Typer` (forms + ONE generic application rule + exhaustive
   `CoreFn` dispatch) → ~27 per-construct `Checkers` → `InferenceKernel`
   (unification, schema algebra, overloads, lattice). Every node carries its
   schema (`info()`) and traversal spine (`children()`). User-function calls
   compile via the whole-function memo (`compileReachable`). Dies: unknown
   tables/columns/properties, type mismatches — the bulk of all negatives.

## Back half

8. **Lowering (I)** — `Lowerer.lower(TypedSpec)` → `SqlQuery` IR
   (PHASE_HIJ_LOWERING.md governs). Exhaustive per-node dispatch; three
   single-authority components:
   - **`Fold`** — every fold-vs-isolate decision (filter → WHERE/HAVING/
     QUALIFY; commutation rules; `resolveInto` substitution through plain
     projections and stars). The lean-SQL tenet lives here.
   - **`Scalars`** — natives dispatched on the RESOLVED overload's identity
     (`TypedFunction.definition()`); catalog-driven families; loud errors.
   - **`Aggregates`** — reduce-overload → SQL reducer.
   Output: one `SqlSelect` per foldable run; `SqlUnion` for concatenate.
   Dies loudly: unimplemented constructs, unregistered overloads.
9. **Render (J)** — `DuckDb.render` → SQL text. The ONLY place SQL text
   exists: quote-only-when-needed, minimal parens, clause-per-line, semantic
   names → spellings (float-forcing divide, positive mod). Unknown name throws.
10. **Execute (K)** — NOT built in core. Tests use raw JDBC on in-memory
    DuckDB. Missing: `ResultShape` at the plan root (TDS/GRAPH/SCALAR/
    COLLECTION envelopes), typed `ExecutionResult`, runtime wiring — what
    makes `Compiler.compile(model, query, runtime)` real.

## The hole in the middle

**Phase H — StoreResolver — not built.** Between G and I for CLASS queries:
`Person.all()` → mapped table; association navigation → LEFT JOIN / EXISTS;
`navigate` lowering; `from(mapping, runtime)` binding; join ELISION
(mapping-implied joins only — user-written joins are never elided). Until H,
the pipeline is end-to-end for RELATION queries (`#>{db.TABLE}#`, TDS
literals) only — deliberate sequencing: lowering landed on stable inputs
before the resolver arc begins.
