# Phase B — Compiled Elements (Design & Strategy)

**Status:** Proposed, ready for implementation
**Companion to:** `BAZEL_DEPENDENCY_PROPOSAL.md` (why + what), `BAZEL_IMPLEMENTATION_PLAN.md` §3 (Phase B rollout checkpoints)
**Supersedes:** strategy drafts v1-v7 in `.windsurf/plans/` (archived)
**Implements:** `BAZEL_DEPENDENCY_PROPOSAL.md` §6 "Back-Reference Fragments (Three-Tier)" in terms of `Compiled*` records

This doc is the authoritative design spec for the Phase B engine refactor (element serialization + compile-all pipeline). The Bazel proposal explains *why* we serialize compiled form; the Bazel implementation plan gives rollout checkpoints. **This doc specifies the Java API, the `Compiled*` record hierarchy, the `.legend` file format, and the back-reference tiering in terms the engine code will actually be written against.**

---

## TL;DR

Every packageable element held by `PureModelBuilder` as a parsed **def record** (`ClassDefinition`, `MappingDefinition`, …) produces a **`CompiledElement`** record via `PureModelBuilder.compileAll()`. The builder keeps three layers distinct: retained def records as the source layer, the semantic model (`PureClass`, `PureAssociation`, …), and compiled dependency artifacts loaded from disk. All executable bodies — mapping source specs, function bodies, derived/qualified properties, service queries, constraint predicates — are represented as **`CompiledExpression`**, a resolved `ValueSpecification` AST plus per-node type info and first-class dependency data.

`compileAll()` is not a new pipeline — it is the **same `TypeChecker` queries use**, driven by exhaustive iteration over the builder's retained def records. The public API stays two-method: `check(ValueSpecification)` returns `CompiledExpression`; `check(PackageableElement)` (overloaded per kind) returns the right `CompiledElement` subtype. Build-time compilation returns **`CompilationArtifacts`** (elements + back-reference fragments + manifest) wrapped in `CompilationResult` alongside errors.

`CompiledElement` records are serialized as **`.legend`** files using the project's zero-dependency `Json` util (no Jackson; GraalVM-native-image-friendly; format-neutral extension so we can migrate to binary later without renaming).

Back-references remain **separate immutable artifacts** per the three-tier design in `BAZEL_DEPENDENCY_PROPOSAL.md` §6. `CompiledAssociation` models association semantics (ends + qualified properties); Tier 1 fragments model the target-side projected properties visible at lookup time. `CompiledClass` is never mutated to inject them. Monomorphization at non-recursive user-function call sites is preserved on top of standalone-compiled bodies; recursive user functions remain unsupported under the current macro-expansion strategy.

---

## Conversation Arc — How We Got Here

Chronological condensed record of the debate that led to v7. Each item is a real turn in the design that moved the decision.

1. **Initial JSON schema debate** — settled on Option C (structured multiplicity, flat string type) aligned with `BAZEL_IMPLEMENTATION_PLAN.md`.
2. **"Serialize from where in the pipeline?"** — decided: serialize from the compiled/runtime form, not the def-layer record.

---

## Core Concepts (Glossary)

These words have specific meanings in this doc. Avoid using them loosely.

| Term | Meaning |
|------|---------|
| **def record** | Parser output; structural description of source (e.g. `ClassDefinition`, `MappingDefinition`). Before any type-checking. |
| **retained def records** | `PureModelBuilder` keeps the parsed `PackageableElement` defs (`ClassDefinition`, `MappingDefinition`, etc.) alive after `addSource()` rather than stripping them. This is the source layer for `compileAll()` and for accessing local bodies. Separate from both the semantic model and loaded compiled deps. |
| **semantic model** | Metamodel-layer runtime objects (e.g. `PureClass`, `PureAssociation`). What `TypeChecker` / `PlanGenerator` operate on. Kept separate from def-record ownership. |
| **sourceSpec** | `ValueSpecification` AST representing a mapping's source expression. Produced by `MappingNormalizer`. E.g. relational mappings become `relation(Table)->filter(...)->extend([...])`. |
| **`TypeInfo`** | **Compiler-internal** per-AST-node type information stamped by `TypeChecker.compileExpr`. Internal working maps may key it by `ValueSpecification` identity while compiling, but compiled output does not expose identity-keyed state. |
| **`CompiledElement`** | Sealed interface; root of the `Compiled*` hierarchy. One permitted subtype per `PackageableElement` kind (`CompiledClass`, `CompiledMapping`, `CompiledFunction`, `CompiledService`, `CompiledAssociation`, `CompiledDatabase`, `CompiledEnum`, `CompiledProfile`, `CompiledConnection`, `CompiledRuntime`). Each is a pure-data record (no cycles, no live refs) ready for serialization. |
| **`CompiledExpression`** | The reusable primitive for compiled bodies. Holds a resolved `ValueSpecification` AST + per-node type information + dependency data. Returned by `check(ValueSpec)`; stored in `CompiledFunction.body`, `CompiledService.queryBody`, `CompiledDerivedProperty.body`, `CompiledConstraint.body`, `CompiledMappedClass.sourceSpec`. **Replaces `TypeCheckResult`** as the public typed-body contract. |
| **`CompiledDependencies`** | Member-level dependency data emitted by `TypeChecker` alongside a compiled body — class property accesses and association navigations, both keyed by class FQN. Lives on `CompiledExpression`. The element-level used-set (authoritative input to Bazel `unused_inputs_list`) is computed at the `CompilationArtifacts` level by aggregating across all compiled bodies — not duplicated per `CompiledElement`. |
| **Private `compile*` methods** | Per-element-kind compilation logic on `TypeChecker` (e.g. `compileClass`, `compileMapping`, `compileFunction`). Called ONLY by `check(PackageableElement)` overloads. Not visible to outside code — all compilation routes through the public `check()` API. |
| **Public `check()` methods** | Two public entry points on `TypeChecker`: `check(ValueSpecification) → CompiledExpression`, and `check(PackageableElement) → CompiledElement` (with narrow-return overloads per kind). Memoized — repeat calls return cached result. |
| **`CompilationArtifacts`** | Aggregate compiled output for one build unit: `elements`, `backReferenceFragments`, and `manifest`. This is the transport boundary between compiler, writer, reader, and builder load APIs. |
| **`compileAll()`** | Build-time entry point on `PureModelBuilder` that iterates the builder's retained def records and calls `tc.check(e)` per element. Returns `CompilationResult`, which wraps `CompilationArtifacts` plus an error list. |
| **`.legend` file** | Serialized `CompiledElement` on disk. Format-neutral extension (JSON payload for v1, migratable to binary). Payload wrapped as `{legendVersion, kind, element}`. Written/read via the project's `com.gs.legend.util.Json` utility with explicit per-kind codecs. |
| **`com.gs.legend.compiled` package** | New package housing the `Compiled*` record hierarchy. Separates compiler *machinery* (`com.gs.legend.compiler`) from compiler *output* (`com.gs.legend.compiled`). Mirrors `parser` vs `ast` split. |
| **`CompiledBackReference`** | Sealed hierarchy of back-reference records: `CompiledPropertyFromAssociation`, `CompiledQualifiedPropertyFromAssociation` (v1), future `CompiledSpecialization`. Each describes ONE injected contribution to a target class. Mirrors Pure's `BackReference` sealed interface. |
| **`CompiledBackRefFragment`** | File-level artifact containing `List<CompiledBackReference>` from a single contributor project to a single target class. On disk: `<targetPackage>/<TargetSimple>.backrefs.legend` under the contributor's output dir. Merged at lookup time in `findProperty`; NEVER mutates `CompiledClass`. |
| **`CompiledProjectManifest`** | Per-project Tier 2 manifest holding `List<CompiledExtensionRef>`. On disk: `project_manifest.legend` at the project's output root. Scaffolded in v7 with empty `extensions` list; populated when the Extension element lands. |
| **Tier 1 / Tier 2 / Tier 3** | Three-tier back-reference design from `BAZEL_DEPENDENCY_PROPOSAL.md` §6. Tier 1 = target-side visible-on-load (associations) = `CompiledBackRefFragment`. Tier 2 = caller-scoped (Extensions) = `CompiledProjectManifest`. Tier 3 = IDE indexer, deferred. |
| **Standalone compilation** | Compiling a function body against its declared signature (not against any particular call site). Catches "body doesn't match declared signature" errors at build time. |
| **Monomorphization** | Query-time compilation of a user-function body with actual call-site arg types substituted for declared param types. Gives call-site type precision. Transient, not serialized. |
| **One-hop compilation** | Type-checking an element requires only its own body plus the *signatures* of directly referenced elements, not their bodies. Keeps complexity linear. |
| **Pass-2 (associations)** | `TypeChecker.compileNeededAssociationTargets` — after the main AST walk, compiles target-class sourceSpecs for associations the AST navigated. Redundant under `compileAll()` (which compiles all mappings directly) but kept for the query path. |

---

## What We Learned

### Learning 1 — There is no "query pruning" to disable

Searches for "build everything mode" or "skip query optimization" are misleading framings. The query path has several mechanisms but none is a "pruning" in the sense of "skip work that wouldn't affect the result":

| Mechanism | What it actually is |
|---|---|
| Query AST as iteration root (`check(vs)`) | Lazy iteration — the query IS the work scope |
| `GetAllChecker` triggering sourceSpec compile | Lazy initialization |
| Pass-2 `compileNeededAssociationTargets` | Follow-up iteration driven by pass-1 side effects |
| `compiledSourceSpecs` dedup set | Memoization within a `TypeChecker` instance |
| `ExtendChecker` skipping embedded extends | Permanent feature skip (handled by `MappingResolver`) |

The query path does **lazy compilation driven by the query AST**. `compileAll()` does **exhaustive compilation driven by the model**. Same checkers, different iteration root.

### Learning 2 — Divergence risk is real but confined

Before the refactor, mapping sourceSpec compilation logic is *inside* `GetAllChecker`; user function body compilation is *inside* `inlineUserFunction`; derived property and constraint type-checking **does not exist today at all**.

If we added `compileAll()` as pure external orchestration (the "reach into checker internals" approach I originally sketched), we'd create two parallel code paths that could drift. Specifically:

- If someone adds a new semantic to `GetAllChecker`, `compileAll()` might miss it.
- If the `$this` context setup for derived properties ever gets a new field, two places need updating.
- If a new `PackageableElement` subtype is added (e.g. `PipelineDefinition`), external orchestration silently skips it.

The refactor below makes all of these impossible.

### Learning 3 — "Compiled state" is not a cache

The word "cache" implies optional, invalidatable, stale-friendly. The compiled state is none of those:

- **Produced** only by compilation (or by deserialization, which is just loading the output of a prior compilation).
- **Consumed** by `PlanGenerator` today and the Phase B serializer tomorrow.
- **Required** for query execution — no path queries raw source directly.
- **Invalidation** is element-scoped (if a source file changes, the element's compiled state is replaced, not "invalidated and recomputed from scratch on next access").

The correct mental model: compiled state is to Legend what `.class` bytecode is to Java. Source → compile → compiled state → (serialize or execute).

### Learning 4 — Monomorphization is worth keeping

User-function call-site type narrowing is how Pure currently works:

```pure
function util::identity<T>(x: T[1]): T[1] { $x }

identity(42)       // Integer[1]
identity('hello')  // String[1]
identity(person)   // Person[1]
```

Giving this up would cascade into imprecise types throughout query ASTs and block optimizations at `PlanGenerator`. The refactor preserves monomorphization — it just runs on top of standalone-compiled bodies rather than recompiling bodies from scratch at each call site.

### Learning 5 — Spike numbers show feasibility

Kitchen-sink 10K (from `PhaseBEagerCompileSpike`):

| Phase | Time |
|---|---|
| Generate source | ~50 ms |
| Parse + build | ~450 ms |
| Normalize | negligible |
| Eager type-check ALL elements (external orchestration) | ~133 ms |
| Heap delta | small (tens of MB) |

At realistic project sizes (tens of thousands of elements max, not hundreds of thousands) this is a non-issue. The spike confirms we can afford to type-check everything at build time.

---

## Decisions

### Decision 1 — Add `compileAll()` as a new build-time entry point, not a flag

Do not introduce a "build everything" flag on `TypeChecker` or `PureModelBuilder`. Instead, add a new method that iterates exhaustively. Lazy query-path behavior is preserved unchanged; `compileAll()` is additive.

### Decision 2 — Public API: two `check()` methods; private per-kind `compile*` methods

`TypeChecker` exposes exactly two public entry points. All per-kind compilation logic is private.

```java
public final class TypeChecker {
    // ====== PUBLIC API — exactly two entry points ======

    /** Compile an ad-hoc query/expression. Returns CompiledExpression (replaces TypeCheckResult). */
    public CompiledExpression check(ValueSpecification vs);

    /**
     * Compile one packageable element. Memoized — repeat calls return cached result.
     * Throws IllegalArgumentException on null or on an unsupported PackageableElement subtype
     * (guarded by the exhaustive sealed switch — compile-time-enforced for known subtypes).
     */
    public CompiledElement check(PackageableElement e);

    // Ergonomic overloads — same dispatch, narrower return types
    public CompiledClass       check(ClassDefinition cd);
    public CompiledMapping     check(MappingDefinition md);
    public CompiledFunction    check(FunctionDefinition fd);
    public CompiledService     check(ServiceDefinition sd);
    public CompiledAssociation check(AssociationDefinition ad);
    public CompiledDatabase    check(DatabaseDefinition dd);
    public CompiledEnum        check(EnumDefinition ed);
    public CompiledProfile     check(ProfileDefinition pd);
    public CompiledConnection  check(ConnectionDefinition cd);
    public CompiledRuntime     check(RuntimeDefinition rd);

    // ====== PRIVATE IMPLEMENTATION ======

    private CompiledClass       compileClass(ClassDefinition cd);
    private CompiledMapping     compileMapping(MappingDefinition md);
    private CompiledFunction    compileFunction(FunctionDefinition fd);
    private CompiledService     compileService(ServiceDefinition sd);
    // ...one per element kind...

    // Internal TypeInfo → CompiledExpression translation
    private CompiledExpression toCompiledExpression(ValueSpecification ast);
    private TypeRef typeRefOf(ExpressionType t);
    private Multiplicity multiplicityOf(ExpressionType t);

    // Memoization — keyed by element FQN
    private final Map<String, CompiledElement> compiled = new HashMap<>();
}
```

**Query-path integration — through public `check(e)`, never through private `compile*`:**

- `GetAllChecker` on hitting `getAll(Person)` looks up the mapping def and calls `env.check(mappingDef)`. Uses the returned `CompiledMapping.mappedClasses[Person].sourceSpec` (a `CompiledExpression`).
- `inlineUserFunction` on a user-function call calls `env.check(functionDef)` — returns `CompiledFunction` containing standalone-compiled body. Then monomorphizes actual arg types on top of that body.
- `compileProperty` on hitting a derived property calls `env.check(ownerClassDef)` — returns `CompiledClass` containing all `CompiledDerivedProperty` entries. Picks the one by name.

**The single public compilation entry point means memoization is impossible to bypass.** Whether called from the build path (`compileAll()` iterating and calling `check(e)` per element) or from the query path (`GetAllChecker` discovering a mapping), there is ONE code path that populates compilation results.

### Decision 3 — `compileAll()` on `PureModelBuilder` iterates retained def records and calls `check(e)`

`compileAll()` belongs on `PureModelBuilder` (which owns retained def records, the semantic model, and loaded compiled artifacts), not on `TypeChecker` (which knows how to compile one thing). The iteration root is the builder's **retained def records** — never semantic-model objects and never already-loaded dependency artifacts. Since `tc.check(e)` already does exhaustive switching internally (per Decision 2), `compileAll()` is a straightforward loop:

```java
public final class PureModelBuilder {
    public CompilationResult compileAll() {
        var tc = new TypeChecker(modelContext);
        var elements = new ArrayList<CompiledElement>();
        var errors   = new ArrayList<CompilationError>();

        for (PackageableElement e : sourceDefs.values()) {
            try {
                elements.add(tc.check(e));
            } catch (PureCompileException ex) {
                errors.add(CompilationError.of(e, ex));
            }
        }

        return new CompilationResult(
            new CompilationArtifacts(List.copyOf(elements), deriveBackRefFragments(elements), emptyProjectManifest()),
            List.copyOf(errors)
        );
    }

    /** Load compiled dependency artifacts without recompiling. */
    public void loadCompilationArtifacts(CompilationArtifacts artifacts) {
        for (var e : artifacts.elements()) compiledStore.put(e.qualifiedName(), e);
        loadBackRefFragments(artifacts.backReferenceFragments());
        loadProjectManifest(artifacts.manifest());
    }
}
```

**Safety:** the exhaustive switch on `PackageableElement` lives inside `TypeChecker.check(PackageableElement)`. A new element subtype forces a compile error there until it's handled, which guarantees `compileAll()` picks it up automatically without any change to `PureModelBuilder`.

**Precedence:** if the same FQN exists both locally and in loaded dependency artifacts, local def records and locally compiled state win. Local compilation reads retained defs; dependency loading reads compiled artifacts. The layers stay cleanly separated.

### Decision 4 — Sealed `CompiledElement` hierarchy is the compiled state

Replace ad-hoc map-of-maps with a proper sealed hierarchy of pure-data records. Each record is serialization-ready (no cycles, no live refs, no identity-keyed maps). The hierarchy lives in a new `com.gs.legend.compiled` package, separate from the `compiler` machinery.

```java
public sealed interface CompiledElement
        permits CompiledClass, CompiledAssociation, CompiledMapping,
                CompiledFunction, CompiledService, CompiledEnum,
                CompiledProfile, CompiledDatabase, CompiledConnection,
                CompiledRuntime {
    String qualifiedName();
    SourceLocation sourceLocation();
}

public record CompiledClass(
    String qualifiedName,
    List<String> superClassFqns,
    List<CompiledProperty> properties,
    List<CompiledDerivedProperty> derivedProperties,
    List<CompiledConstraint> constraints,
    List<StereotypeRef> stereotypes,
    Map<String, String> taggedValues,
    SourceLocation sourceLocation
) implements CompiledElement {}

public record CompiledMapping(
    String qualifiedName,
    List<CompiledMappedClass> mappedClasses,
    SourceLocation sourceLocation
) implements CompiledElement {}

public record CompiledMappedClass(
    String classFqn,
    MappingKind kind,                  // RELATIONAL or M2M
    CompiledExpression sourceSpec,     // the compiled relation/M2M expression
    Optional<String> rootTableFqn,     // relational only
    Optional<String> sourceClassFqn    // M2M only
) {}

public record CompiledFunction(
    String qualifiedName,
    List<CompiledParameter> parameters,
    TypeRef returnTypeRef,
    Multiplicity returnMultiplicity,
    CompiledExpression body,           // standalone-compiled against declared signature
    SourceLocation sourceLocation
) implements CompiledElement {}

public record CompiledService(
    String qualifiedName,
    List<CompiledParameter> parameters,
    CompiledExpression queryBody,
    SourceLocation sourceLocation
) implements CompiledElement {}

public record CompiledAssociation(
    String qualifiedName,
    List<CompiledAssociationEnd> ends,               // exactly 2
    List<CompiledDerivedProperty> qualifiedProperties,
    SourceLocation sourceLocation
) implements CompiledElement {}

public record CompiledEnum(
    String qualifiedName,
    List<CompiledEnumValue> values,
    List<StereotypeRef> stereotypes,
    Map<String, String> taggedValues,
    SourceLocation sourceLocation
) implements CompiledElement {}

public record CompiledEnumValue(
    String name,
    List<StereotypeRef> stereotypes,
    Map<String, String> taggedValues
) {}

public record CompiledProfile(
    String qualifiedName,
    List<String> stereotypes,
    List<String> tags,
    SourceLocation sourceLocation
) implements CompiledElement {}

public record CompiledDatabase(
    String qualifiedName,
    List<String> includedDatabaseFqns,
    List<CompiledSchema> schemas,          // each contains tables, views, joins, filters
    SourceLocation sourceLocation
) implements CompiledElement {}

public record CompiledConnection(
    String qualifiedName,
    String storeFqn,                        // references a CompiledDatabase FQN
    ConnectionKind kind,                    // RELATIONAL or MODEL_CHAIN
    Map<String, String> properties,         // connection-specific settings
    SourceLocation sourceLocation
) implements CompiledElement {}

public record CompiledRuntime(
    String qualifiedName,
    List<String> mappingFqns,
    List<CompiledRuntimeConnection> connections,
    SourceLocation sourceLocation
) implements CompiledElement {}

public record CompiledRuntimeConnection(
    String storeFqn,
    String connectionFqn
) {}

public record CompiledAssociationEnd(
    String name, TypeRef targetTypeRef, Multiplicity multiplicity
) {}

public record CompiledProperty(
    String name, TypeRef typeRef, Multiplicity multiplicity
) {}

public record CompiledDerivedProperty(
    String name, List<CompiledParameter> parameters,
    TypeRef returnTypeRef, Multiplicity returnMultiplicity,
    CompiledExpression body
) {}

public record CompiledConstraint(
    String name, CompiledExpression body
) {}

public record CompiledParameter(
    String name, TypeRef typeRef, Multiplicity multiplicity
) {}

public record TypeRef(Kind kind, String fqn) {
    public enum Kind { PRIMITIVE, CLASS, ENUMERATION }
}

public record Multiplicity(int lowerBound, Integer upperBound) {
    // upperBound null = unbounded
}

public enum MappingKind { RELATIONAL, M2M }

public record CompiledDependencies(
    Map<String, Set<String>> classPropertyAccesses,
    Map<String, Set<String>> associationNavigations
) {
    /** Derived: element FQNs statically referenced by this body (union of the two map key-sets). */
    public Set<String> elementFqns() {
        var s = new LinkedHashSet<String>(classPropertyAccesses.keySet());
        s.addAll(associationNavigations.keySet());
        return Set.copyOf(s);
    }
}

public record CompiledExpression(
    ValueSpecification ast,
    Map<ValueSpecification, TypeInfo> types,   // identity-keyed in memory; see Phase 7 note on persistence
    CompiledDependencies dependencies
) {
    /** Retains the accessor PlanGenerator uses today. */
    public TypeInfo typeInfoFor(ValueSpecification node) { ... }
}

**Node-ID note:** in memory, `types` is an `IdentityHashMap<ValueSpecification, TypeInfo>` — mechanically identical to today's `TypeCheckResult`. That shape is what Phase 1a lands. Persistence (Phase 7) introduces a parallel stable-node-ID encoding: either assigned during AST construction (every `ValueSpecification` gets a `long id` field) or via an on-the-side `Map<ValueSpecification, Long>` produced at serialize-time. Choice deferred to Phase 7 — doesn't affect the in-memory Phase 1a/1b/2 work.

**`CompiledExpression` is deeply immutable.** The `ast` is the already-normalized resolved `ValueSpecification` produced by `MappingNormalizer` / parser; it is shared-reference semantics but never mutated after construction. The `types` map and dependency collections are populated once (during `check`) and wrapped `Map.copyOf(...)` before the record is returned.

**Package placement:**

```
com.gs.legend.compiler       — TypeChecker, checkers, TypeInfo (internal machinery)
com.gs.legend.compiled       — CompiledElement hierarchy, CompiledExpression (output records)
com.gs.legend.compiled.codec — JSON codecs for .legend files (Phase 7)
```

Separation: `compiler` is machinery; `compiled` is its output. Mirrors `parser` vs `ast`.

**What happens to `TypeCheckResult`:** deleted. Its public replacement is `CompiledExpression`, in `com.gs.legend.compiled`. `PlanGenerator` and tests update to import the new name. The `typeInfoFor(node)` accessor is preserved with unchanged semantics — per `AGENTS.md`, `PlanGenerator` still reads TypeInfo from the sidecar.

**Immutability invariant (CRITICAL).** Every `Compiled*` record is **fully immutable after construction**:

- `CompiledClass` is never mutated to inject association-backed properties, contributed properties from Extensions, or any other back-referenced structure. These are modeled as **separate artifacts** (see Decision 10).
- `CompiledExpression` is never mutated to add TypeInfos after `check` returns.
- Collections exposed on records are `List.copyOf(...)` / `Map.copyOf(...)` wrapped at construction time.

This guarantees: (a) trivial serialization (no consistency-at-what-moment problem), (b) safe sharing across threads, (c) clean invalidation semantics (a `.legend` file rewrite is atomic content replacement, never structural surgery).

### Decision 5 — Compile-on-demand is preserved (memoization, not precompile-required)

Query path calls `env.check(e)`; `check(e)` checks the memoization map; on hit, returns cached `CompiledElement`; on miss, calls the private `compile*`, stores result, returns. Queries **do not require `compileAll()` to have run**. This keeps:

- Dev loop / REPL responsive (don't need to pay full build validation on every source edit).
- Partial builds workable (can run queries against a model where only some elements were validated).
- Compiled-artifact load path straightforward (`loadCompilationArtifacts(artifacts)` populates compiled state + back-ref state; queries hit it; no special mode).

### Decision 6 — Monomorphization stays at non-recursive call sites

- **Build time:** `compileAll()` calls `tc.check(functionDef)` → private `compileFunction` validates the body against the declared signature and stores a standalone `CompiledFunction` body.
- **Query time:** when `inlineUserFunction` hits a user-function call, it calls `env.check(functionDef)` → returns `CompiledFunction` containing the pre-validated body. Then it monomorphizes (substitutes actual arg types, runs propagation through the already-validated body) to produce call-site-specific TypeInfos. Monomorphization results are **transient** — not stored in compiled state, not serialized.
- **Recursion:** recursive and mutually recursive user functions remain unsupported under the current macro-expansion / inlining strategy. `compileAll()` and query-time compilation both surface them as compile errors rather than trying to encode partial recursive bodies.

### Decision 7 — Compilation errors are collected, not thrown

`compileAll()` returns a `CompilationResult` with an error list. Callers decide what to do:

- **Bazel build action** — any error fails the build. Report all errors for best developer feedback.
- **LSP** — attach errors to model but keep the model queryable for unaffected elements.
- **CI / test harness** — fail fast or collect, depending on test style.

Errors carry element identity + source location (once the def layer has `SourceLocation` attached — tracked separately but aligned with this plan).

### Decision 8 — Kitchen-sink contract test pins coverage

A new test `CompileAllCoverageTest` uses a fixture model that exercises every `PackageableElement` subtype × every non-trivial feature (join chains, DynaFunctions, filters, views, M2M, embedded extends, derived properties, constraints, user functions with deep non-recursive call chains, services). Asserts:

1. `compileAll()` returns zero errors.
2. Every element produces a `CompiledElement` of the expected subtype.
3. `CompiledClass` entries have populated `derivedProperties` and `constraints`.
4. `CompiledMapping` entries have populated `mappedClasses` with non-null `sourceSpec`.
5. `CompiledFunction` entries have non-null `body`.
6. Dep info aggregated across all `CompiledExpression`s (classPropertyAccesses / associationNavigations) contains expected closure.
7. A snapshot assertion over the full `List<CompiledElement>` shape (guards against silent drift).

### Decision 9 — Serialization uses the project's `Json` util, not Jackson

`com.gs.legend.util.Json` is the canonical JSON primitive: zero-dep, GraalVM-native-image-friendly, streaming `Writer`, deterministic `LinkedHashMap`-backed object output. No Jackson; no reflection; explicit codecs aligned with `AGENTS.md`'s "no reflection, fail loud" ethos.

**Codec pattern — one pair per `Compiled*` record:**

```java
// In each Compiled* record
public void toJson(Json.Writer w) {
    w.beginObject();
    w.field("kind", "class");
    w.field("qualifiedName", qualifiedName);
    w.fieldStringArray("superClassFqns", superClassFqns);
    w.name("properties").beginArray();
    for (var p : properties) p.toJson(w);
    w.endArray();
    // ...
    w.endObject();
}

public static CompiledClass fromJson(Json.Obj o) {
    return new CompiledClass(
        o.getString("qualifiedName"),
        o.getStringArray("superClassFqns"),
        o.getArr("properties").items().stream()
            .map(n -> CompiledProperty.fromJson((Json.Obj) n))
            .toList(),
        // ...
    );
}
```

**Dispatcher for the sealed hierarchy:**

```java
public final class CompiledElementCodec {
    public static void toJson(CompiledElement e, Json.Writer w) {
        switch (e) {
            case CompiledClass cc       -> cc.toJson(w);
            case CompiledMapping cm     -> cm.toJson(w);
            case CompiledFunction cf    -> cf.toJson(w);
            case CompiledService cs     -> cs.toJson(w);
            case CompiledAssociation ca -> ca.toJson(w);
            case CompiledDatabase cd    -> cd.toJson(w);
            case CompiledEnum ce        -> ce.toJson(w);
            case CompiledProfile cp     -> cp.toJson(w);
            case CompiledConnection cn  -> cn.toJson(w);
            case CompiledRuntime cr     -> cr.toJson(w);
        }
    }

    public static CompiledElement fromJson(Json.Obj o) {
        return switch (o.getString("kind")) {
            case "class"       -> CompiledClass.fromJson(o);
            case "mapping"     -> CompiledMapping.fromJson(o);
            case "function"    -> CompiledFunction.fromJson(o);
            case "service"     -> CompiledService.fromJson(o);
            case "association" -> CompiledAssociation.fromJson(o);
            case "database"    -> CompiledDatabase.fromJson(o);
            case "enum"        -> CompiledEnum.fromJson(o);
            case "profile"     -> CompiledProfile.fromJson(o);
            case "connection"  -> CompiledConnection.fromJson(o);
            case "runtime"     -> CompiledRuntime.fromJson(o);
            default -> throw new IllegalArgumentException("Unknown kind: " + o.getString("kind"));
        };
    }
}
```

**Estimated codec LOC:** ~40-80 per `Compiled*` record. ~500 LOC total across the full sealed hierarchy. More than Jackson auto-mapping, but aligned with project principles (no reflection, fail loud on unknowns, streaming-friendly output).

### Decision 10 — Back-references modeled as separate immutable artifacts (Tier 1/2/3)

`CompiledClass` is **immutable**. Association-injected properties, qualified association properties, future subclass enumerations, and future Extensions are **NOT** embedded in `CompiledClass`. They are captured in separate artifact types and merged at lookup time. This implements the three-tier back-reference design from `BAZEL_DEPENDENCY_PROPOSAL.md` §6 ("Back-Reference Fragments (Three-Tier)") in terms of `Compiled*` records.

**Why not embed in `CompiledClass`:**

- **Invalidation:** a change to `products::PersonAccount` would invalidate `refdata/Person.legend` for every consumer, even those that never touch `.accounts`. Fragments isolate the blast radius.
- **Scope:** Tier 2 Extensions are caller-scoped (only visible to callers that import the contributing project). Target-side embedding leaks them to every downstream.
- **Mutability:** embedding would force `CompiledClass` to be built in two phases (own properties, then merge back-refs) — violating the pure-data-record invariant.

#### Tier 1 — target-side fragments (v1, REQUIRED)

```java
// Sealed hierarchy — mirrors Pure's BackReference records
public sealed interface CompiledBackReference
        permits CompiledPropertyFromAssociation,
                CompiledQualifiedPropertyFromAssociation {
    String targetClassFqn();   // the class being contributed TO
    String name();             // the injected property/qualified-property name
}

public record CompiledPropertyFromAssociation(
    String targetClassFqn,     // e.g. refdata::Person
    String name,               // e.g. "accounts"
    String associationFqn,     // declaring association, e.g. products::PersonAccount
    String peerClassFqn,       // the OTHER end, e.g. products::Account
    Multiplicity multiplicity
) implements CompiledBackReference {}

public record CompiledQualifiedPropertyFromAssociation(
    String targetClassFqn,
    String name,
    String associationFqn,
    CompiledExpression body,   // qualified properties carry bodies
    TypeRef returnTypeRef,
    Multiplicity returnMultiplicity
) implements CompiledBackReference {}

// Future: CompiledSpecialization (for SuperClass.all() polymorphic queries)

// File-level artifact — one per (contributor, target-class) pair
public record CompiledBackRefFragment(
    int legendVersion,
    String targetFqn,              // e.g. refdata::Person
    String contributorProject,     // e.g. products
    List<CompiledBackReference> backReferences
) {}
```

**File layout — fragment filename mirrors target's, co-located with contributor's own output:**

```
bazel-bin/products/elements/
├── products/PersonAccount.legend           ← CompiledAssociation (authoritative)
├── refdata/Person.backrefs.legend          ← CompiledBackRefFragment, target=refdata::Person, contributor=products
├── products/Account.backrefs.legend        ← reverse end (if both classes external this would also be here)
└── project_manifest.legend                 ← CompiledProjectManifest (Tier 2)
```

Filename convention: `<targetPackage>/<TargetSimpleName>.backrefs.legend` — parallel to `<targetPackage>/<TargetSimpleName>.legend`. Multiple contributors produce same-named files in different output directories; the loader walks every registered dep dir and merges.

**Payload format (`.backrefs.legend`):**

```json
{
  "legendVersion": 1,
  "kind": "backrefs",
  "targetFqn": "refdata::Person",
  "contributorProject": "products",
  "backReferences": [
    {
      "kind": "propertyFromAssociation",
      "associationFqn": "products::PersonAccount",
      "name": "accounts",
      "peerClassFqn": "products::Account",
      "multiplicity": {"lowerBound": 0, "upperBound": null}
    }
  ]
}
```

#### Tier 2 — per-project manifest (scaffolded in v7, populated when Extensions land)

```java
public record CompiledProjectManifest(
    int legendVersion,
    String projectId,
    List<CompiledExtensionRef> extensions   // empty list allowed
) {}

public record CompiledExtensionRef(
    String extensionFqn,
    String targetFqn,
    boolean exported,
    ExtensionFlavor flavor
) {}

public enum ExtensionFlavor { DERIVED_ONLY, SHADOW_MAPPED }
```

File: `<projectRoot>/project_manifest.legend`. Always emitted by `compileAll()` (empty `extensions` list until the Extension element lands per `CROSS_PROJECT_JOINS.md` §8). Scaffolding lets Extensions activate later with zero build-graph churn.

#### Tier 3 — IDE indexer (deferred, out of scope)

Not a compiled-artifact type. No `Compiled*` record here. Future work — a separate indexer walks `.legend` files and builds its own reverse-ref store (Swift `SourceKit`-style). Not emitted by `compileAll()`.

#### Lookup-time merge (in `PureModelBuilder.findProperty`) — NOT load-time mutation

```java
public Optional<Property> findProperty(String classFqn, String name) {
    var cls = findClass(classFqn);
    if (cls.isEmpty()) return Optional.empty();

    // 1. Own properties + own derived properties (from CompiledClass)
    var local = cls.get().findOwnProperty(name);
    if (local.isPresent()) return local;

    // 2. Superclass chain
    for (var superFqn : cls.get().superClassFqns()) {
        var fromSuper = findProperty(superFqn, name);
        if (fromSuper.isPresent()) return fromSuper;
    }

    // 3. Tier 1 back-refs: walk all fragments targeting this class
    //    backRefStore is populated as fragment files are lazy-loaded; merge is cheap.
    for (var br : backRefStore.forTarget(classFqn)) {
        if (br.name().equals(name)) return Optional.of(synthesizeProperty(br));
    }

    // 4. Tier 2 caller-scoped extensions (when Extensions land)
    // ...

    return Optional.empty();
}
```

The critical invariant: **`CompiledClass` is never mutated**. All back-ref composition happens in `findProperty` against the `backRefStore` (a `Map<String, List<CompiledBackReference>>` populated on fragment load). This preserves record immutability, invalidation isolation, and Tier 2 caller-scope semantics.

#### Emission in `compileAll()`

```java
public CompilationResult compileAll() {
    // ... existing compile loop produces List<CompiledElement>

    // Derive Tier 1 fragments from compiled associations
    var fragments = new ArrayList<CompiledBackRefFragment>();
    var byTarget = new LinkedHashMap<String, List<CompiledBackReference>>();
    for (var e : compiled) {
        if (e instanceof CompiledAssociation ca) {
            for (var br : deriveBackReferences(ca)) {
                byTarget.computeIfAbsent(br.targetClassFqn(), k -> new ArrayList<>()).add(br);
            }
        }
    }
    for (var entry : byTarget.entrySet()) {
        fragments.add(new CompiledBackRefFragment(1, entry.getKey(), thisProjectId, entry.getValue()));
    }

    // Tier 2 manifest (scaffolded, empty for v1)
    var manifest = new CompiledProjectManifest(1, thisProjectId, List.of());

    return new CompilationResult(
        new CompilationArtifacts(List.copyOf(compiled), List.copyOf(fragments), manifest),
        List.copyOf(errors)
    );
}
```

`CompilationArtifacts` is the transport boundary. `CompilationResult` wraps that artifact bundle together with errors. Artifact writers emit `.legend`, `.backrefs.legend`, and `project_manifest.legend` from the bundle.

**Scope impact:**
- Phase 1a: +4 record types (`CompiledBackReference` sealed + 2 subtypes + `CompiledBackRefFragment`), +3 Tier 2 records (`CompiledProjectManifest`, `CompiledExtensionRef`, `ExtensionFlavor`). ~100 LOC.
- Phase 2: `compileAll()` derives fragments from `CompiledAssociation`s; emits empty manifest. `CompilationArtifacts` + `CompilationResult` land together.
- Phase 7: per-kind codecs add entries for `.backrefs.legend` + `project_manifest.legend`. `CompilationArtifactsReader` populates the `backRefStore`. ~150 additional LOC.
- `findProperty` lookup order updated in `PureModelBuilder` (~30 LOC).

References `BAZEL_DEPENDENCY_PROPOSAL.md` §6 ("Back-Reference Fragments (Three-Tier)") for the full rationale and comparison to Pure's `BackReference`.

### Decision 11 — `.legend` file format + layout

**Extension:** `.legend` (not `.json`). Format-neutral — we can switch to binary (protobuf, CBOR, custom) later without renaming artifacts. Like Java's `.class`: the spec defines the format, the extension just says "loadable compiled artifact."

**File contents (JSON payload, v1):**

```json
{
  "legendVersion": 1,
  "kind": "class",
  "element": {
    "qualifiedName": "model::Person",
    "superClassFqns": ["model::Entity"],
    "properties": [...],
    "derivedProperties": [...],
    "constraints": [...]
  }
}
```

- `legendVersion` — format version. Enables evolution; older loaders refuse unknown versions.
- `kind` — discriminator for sealed `CompiledElement` dispatch (`class`, `mapping`, `function`, ...).
- `element` — the actual `Compiled*` payload (via per-kind codec).

**File layout — hierarchical, mirroring package structure (like Java `.class` files):**

```
bazel-bin/my_model/
├── model/
│   ├── Person.legend
│   ├── Firm.legend
│   └── FirmMapping.legend
├── services/
│   └── getAllPeople.legend
└── functions/
    └── greet.legend
```

Filename: `<simpleName>.legend`. Package → directory mapping via `::` → `/`. Clean globs, matches IDE expectations, filesystem-native.

**Tooling:** register `.legend` as JSON syntax highlighting in project `.gitattributes` / IDE config while v1 is JSON.

---

## Architecture

### Data flow

```
┌─────────────┐     parse      ┌────────────────┐
│  .pure src  │ ─────────────> │  def records   │
└─────────────┘                │  (AST)         │
                               └────────┬───────┘
                                        │ PureModelBuilder.addSource
                                        ▼
                               ┌────────────────┐
                               │ semantic model │
                               │ PureClass etc. │
                               └────────┬───────┘
                                        │ MappingNormalizer
                                        ▼
                               ┌────────────────┐
                               │  sourceSpec    │
                               │  ValueSpecs    │
                               └────────┬───────┘
                                        │
         ┌──────────────────────────────┼──────────────────────────────┐
         │                              │                              │
         ▼                              ▼                              ▼
  compileAll()                 Query.check()              CompilationArtifactsReader
  (iterate retained             (check(e) on-             (load .legend
   defs + check(e))              demand at call            files)
         │                        sites)                        │
         │                              │                       │
         └──────────────────────────────┼───────────────────────┘
                                        ▼
                               ┌─────────────────────┐
                               │  List<CompiledElem> │
                               │  (sealed hierarchy, │
                               │   pure-data records)│
                               └──────────┬──────────┘
                                          │
                  ┌───────────────────────┼────────────────────────┐
                  ▼                       ▼                        ▼
           PlanGenerator           CompilationArtifactsWriter  dep extraction
           (reads Compiled-        (.legend files         (aggregated from
            Expression)             via Json util)         CompiledDependencies)
```

### API surface — final shape

```java
// === Entry points ===

public final class PureModelBuilder {
    public PureModelBuilder addSource(String pureSource);
    public CompilationResult compileAll();                      // build-time producer
    public Optional<CompiledElement> compiled(String fqn);      // accessor
    public void loadCompilationArtifacts(CompilationArtifacts a);  // Phase B load
}

public final class TypeChecker {
    // Two public methods only
    public CompiledExpression check(ValueSpecification vs);
    public CompiledElement    check(PackageableElement e);

    // Narrow-return overloads (same dispatch)
    public CompiledClass       check(ClassDefinition cd);
    public CompiledMapping     check(MappingDefinition md);
    public CompiledFunction    check(FunctionDefinition fd);
    public CompiledService     check(ServiceDefinition sd);
    public CompiledAssociation check(AssociationDefinition ad);
    public CompiledDatabase    check(DatabaseDefinition dd);
    public CompiledEnum        check(EnumDefinition ed);
    public CompiledProfile     check(ProfileDefinition pd);
    public CompiledConnection  check(ConnectionDefinition cd);
    public CompiledRuntime     check(RuntimeDefinition rd);

    // Private per-kind implementations (Decision 2)
}

public record CompilationArtifacts(
    List<CompiledElement> elements,
    List<CompiledBackRefFragment> backReferenceFragments,
    CompiledProjectManifest manifest
) {
    public Set<String> aggregatedElementDependencies();
    public Map<String, Set<String>> aggregatedClassPropertyAccesses();
    public Map<String, Set<String>> aggregatedAssociationNavigations();
}

public record CompilationResult(
    CompilationArtifacts artifacts,
    List<CompilationError> errors
) {
    public boolean success() { return errors.isEmpty(); }
}

public record CompilationError(
    String elementFqn,
    SourceLocation location,   // may be null until SourceLocation sidecar lands
    String message,
    Throwable cause
) {}

// === Phase B serialization (Phase 7) ===

public final class CompilationArtifactsWriter {
    public void writeAll(Path outDir, CompilationArtifacts artifacts);
}

public final class CompilationArtifactsReader {
    public CompilationArtifacts readAll(Path inDir);
}
```

### Invariants after `compileAll()` success

1. `result.artifacts().elements()` has one `CompiledElement` per `PackageableElement` in the model.
2. `CompiledClass` entries have complete `derivedProperties` and `constraints` lists (possibly empty).
3. `CompiledMapping` entries have complete `mappedClasses` with non-null `sourceSpec` per class.
4. `CompiledFunction` entries have non-null `body`.
5. `CompiledService` entries have non-null `queryBody`.
6. Dep info aggregated across compiled output reflects the full closure of static references. `CompilationArtifacts.aggregatedElementDependencies()` is the authoritative per-element used-set for Bazel; member-level dependency views remain available for finer-grained analysis.

### Invariants after Phase B deserialization

Identical to post-`compileAll()` invariants. Load path is: `CompilationArtifactsReader.readAll(dir)` → `CompilationArtifacts` → `builder.loadCompilationArtifacts(artifacts)` → queries work immediately without recompilation.

---

## Implementation Plan

### Phase 0 — Measurement spike [DONE]

- Built `PhaseBEagerCompileSpike` with 4 topologies (hub-spoke, linear chain, chaotic-lite, kitchen-sink) at 10K.
- Confirmed: eager type-check of all elements is ~133ms for kitchen-sink 10K. Feasible.
- Spike code stays in repo but is marked `@Tag("spike")` and skipped from default runs.

### Phase 1a — Define `Compiled*` sealed hierarchy + rename `TypeCheckResult` [NEXT]

**Goal:** land the type vocabulary before refactoring logic. Low-risk, high-value — unblocks everything downstream.

**Files created (in new `com.gs.legend.compiled` package):**
- `CompiledElement.java` (sealed interface)
- `CompiledClass.java`, `CompiledMapping.java`, `CompiledFunction.java`, `CompiledService.java`, `CompiledAssociation.java`, `CompiledDatabase.java`, `CompiledEnum.java`, `CompiledProfile.java`, `CompiledConnection.java`, `CompiledRuntime.java`
- `CompiledExpression.java` (body primitive — replaces `TypeCheckResult`)
- `CompiledProperty.java`, `CompiledDerivedProperty.java`, `CompiledConstraint.java`, `CompiledParameter.java`, `CompiledMappedClass.java`
- `CompiledEnumValue.java`, `CompiledRuntimeConnection.java`, `CompiledSchema.java` (stub)
- `TypeRef.java`, `Multiplicity.java`, `MappingKind.java`, `ConnectionKind.java`, `StereotypeRef.java` (supporting types)
- `CompiledBackReference.java` (sealed), `CompiledPropertyFromAssociation.java`, `CompiledQualifiedPropertyFromAssociation.java`, `CompiledBackRefFragment.java` (Tier 1, Decision 10)
- `CompiledProjectManifest.java`, `CompiledExtensionRef.java`, `ExtensionFlavor.java` (Tier 2, Decision 10)
- `SourceLocation.java` (if not already present — stub for now; populate when sidecar lands)

**Files modified:**
- `engine/src/main/java/com/gs/legend/compiler/TypeCheckResult.java` → delete (its name moves to `CompiledExpression` in new package)
- `engine/src/main/java/com/gs/legend/compiler/TypeChecker.java` → `check(ValueSpecification)` now returns `CompiledExpression`
- All `PlanGenerator*` files and tests that import `TypeCheckResult` → update import to `com.gs.legend.compiled.CompiledExpression`

**Test work:**
- Existing tests compile-pass with rename (pure mechanical change, no behavior change).
- Add `CompiledElementSealedDispatchTest` — verify exhaustive switch compiles across all subtypes (contract test).

**Estimated LOC:** ~300 new (Compiled* records), ~50 touched (rename). Pure mechanical work — no semantic changes.

### Phase 1b — Refactor `TypeChecker` to produce `Compiled*` records via private `compile*` methods

**Goal:** implement the private per-kind compilation logic. Route query-path code through public `check(e)`.

**Files touched:**
- `engine/src/main/java/com/gs/legend/compiler/TypeChecker.java`
- `engine/src/main/java/com/gs/legend/compiler/checkers/GetAllChecker.java`
- Wherever `inlineUserFunction` lives (user-function call-site inlining)
- Wherever `compileProperty` lives (derived-property resolution)

**Tasks:**

1. Add public `check(PackageableElement)` + narrow overloads. Exhaustive switch → private `compile*`.
2. Implement `compileClass`, `compileMapping`, `compileFunction`, `compileService`, `compileAssociation`, `compileDatabase`, `compileEnum`, `compileProfile`, `compileConnection`, `compileRuntime`.
   - Each produces a populated `Compiled*` record.
   - Each populates the memoization map keyed by element FQN.
3. Internal translation helpers: `toCompiledExpression(ast)`, `typeRefOf(ExpressionType)`, `multiplicityOf(ExpressionType)`.
4. Refactor `GetAllChecker`: on `getAll(ClassFqn)`, look up mapping def, call `env.check(mappingDef)`, use resulting `CompiledMapping.mappedClasses[fqn].sourceSpec`.
5. Refactor `inlineUserFunction`: call `env.check(functionDef)` → `CompiledFunction` with standalone body; monomorphize on top.
6. Refactor `compileProperty`: on derived property access, call `env.check(ownerClassDef)`, pick the `CompiledDerivedProperty` by name.
7. Memoization: `check(e)` returns cached `CompiledElement` if present; else calls `compile*`, stores, returns.

**Test work:**
- All existing integration tests run before merge. Primitives are net behavioral no-op for query path (logic moved, not changed).
- New: `TypeCheckerElementDispatchTest` — verify `check(PackageableElement)` returns correct `Compiled*` subtype per element kind.
- New: `TypeCheckerMemoizationTest` — verify repeat `check(e)` calls return same instance and compile logic runs once.
- Regression: golden query suite runs, produces same plans.

**Estimated LOC:** ~400-600 touched / ~200-300 net added. Most is code motion from `GetAllChecker` / `inlineUserFunction` into private `compile*` methods.

### Phase 2 — Implement `PureModelBuilder.compileAll()`

**Files touched:**
- `engine/src/main/java/com/gs/legend/model/PureModelBuilder.java`
- `engine/src/main/java/com/gs/legend/compiled/CompilationArtifacts.java` (new)
- `engine/src/main/java/com/gs/legend/compiled/CompilationResult.java` (new)
- `engine/src/main/java/com/gs/legend/compiled/CompilationError.java` (new)

**Tasks:**

1. Add `CompilationArtifacts`, `CompilationResult`, and `CompilationError` records (in `com.gs.legend.compiled`, next to `CompiledElement`). `CompilationArtifacts` owns `elements`, `backReferenceFragments`, and `manifest`; `CompilationResult` wraps artifacts plus errors.
2. Implement `compileAll()`: iterate the builder's retained def records (`sourceDefs.values()`), call `tc.check(e)` per element, collect `CompiledElement`s + errors.
3. Derive Tier 1 fragments from `CompiledAssociation`s: walk each association's two ends, emit one `CompiledPropertyFromAssociation` per injected property, group by target class, wrap into `CompiledBackRefFragment` per target.
4. Emit empty Tier 2 `CompiledProjectManifest` (extensions list empty in v1 — scaffolded per Decision 10).
5. Ensure normalization has run before iteration (call `ensureNormalized()` internally).
6. Use ONE `TypeChecker` instance shared across all element iterations (memoization shared across element calls).
7. Add `PureModelBuilder.compiled(fqn)` accessor returning `Optional<CompiledElement>`.
8. Add `PureModelBuilder.loadCompilationArtifacts(artifacts)` and `loadBackRefFragments(list)` for the compiled-artifact load path.
9. Update `PureModelBuilder.findProperty` to consult the `backRefStore` after local + superclass checks (Decision 10 lookup algorithm).
10. Error aggregation: on `PureCompileException`, record `CompilationError`, continue with next element (don't short-circuit).

**Test work:**
- `CompileAllSmokeTest` — trivial model (one class, one mapping, one function) → zero errors, 3 `CompiledElement`s of right subtypes.
- `CompileAllErrorTest` — deliberately broken function body → error reported with correct element FQN; other elements still compiled.
- `CompileAllOrderingTest` — model with forward references between mappings/functions → all resolve correctly regardless of iteration order (one-hop compilation handles it).
- `CompileAllMemoizationTest` — `tc.check(sameElement)` returns same instance across iteration + explicit later call.
- `CompileAllBackRefFragmentTest` — model with one association `products::PersonAccount` between `refdata::Person` and `products::Account` → `CompilationResult.artifacts().backReferenceFragments()` contains two fragments (one per target); each lists one `CompiledPropertyFromAssociation` with correct peer/multiplicity.
- `FindPropertyBackRefLookupTest` — after loading back-ref fragments, `findProperty("refdata::Person", "accounts")` returns synthesized `Property` via merge; `CompiledClass` for Person is observed unchanged (reference equality before/after lookup).

**Estimated LOC:** ~250 new (was ~150 pre-Decision-10; +100 for fragment derivation and find-property update).

### Phase 3 — Re-run spike with `compileAll()`

- Update `PhaseBEagerCompileSpike` to replace external Phase 3a/3b/3c/3d orchestration with a single `builder.compileAll()` call.
- Measure: should match or beat the current 133 ms (likely beat because memoization eliminates the re-parse for derived properties).
- Update `phase-b-measurement-spike-10k-c0954a.md` with final numbers.

### Phase 4 — Kitchen-sink contract test

**Files touched:**
- `engine/src/test/java/com/gs/legend/test/CompileAllCoverageTest.java` (new)
- `engine/src/test/resources/fixtures/compileAll/kitchen-sink.pure` (new)

**Tasks:**
1. Build fixture Pure model covering every element kind × feature combination:
   - Class with derived properties, constraints, supertypes, associations.
   - Class with M2M mapping using embedded extends, traverse columns, filters.
   - Class with relational mapping using join chains, DynaFunctions, embedded, groupBy, distinct.
   - View with filter + distinct + column derivation.
   - Association with bidirectional navigation.
   - Enumeration with mapping from multiple values.
   - Service with query body using user functions.
   - User function with deep non-recursive call chains, generic parameters, lambdas.
   - Database with table definitions, views, joins.
   - Runtime + connection definitions.
2. Assertions:
   - `compileAll()` returns zero errors.
   - Compiled state has expected entries for every element.
   - `classPropertyAccesses` / `associationNavigations` contain expected closure.
   - Golden snapshot of `CompilationResult` structure (not exact TypeInfos — those are too noisy — but counts + keys).

### Phase 5 — PhaseBProgressTest integration

- Add `compileAll` coverage matrix column to `PhaseBProgressTest`.
- Track which element kinds are covered by `compileAll` and which remain def-only.
- Gate Phase B serialization release on all element kinds being green.

### Phase 6 — Bazel integration

- Bazel compile action becomes:
  ```java
  var result = builder.compileAll();
  if (!result.success()) failBuild(result.errors());
  emitUnusedInputsList(result.artifacts().aggregatedElementDependencies());
  // Later (Phase 7): persist to .legend files
  new CompilationArtifactsWriter().writeAll(outDir, result.artifacts());
  ```
- Update `BAZEL_DEPENDENCY_PROPOSAL.md`:
  - Remove separate `validateElement(fqn)` section.
  - Remove separate `ProjectDependencyAnalyzer` section.
  - Replace both with a single "Validation and dependency analysis via `compileAll()`" section that references this plan.

### Phase 7 — Phase B serialization (follow-on, separate PR)

**Files created (in new `com.gs.legend.compiled.codec` package):**
- `CompiledElementCodec.java` — sealed dispatcher for `CompiledElement.toJson` / `fromJson` (see Decision 9)
- `CompiledExpressionCodec.java` — body primitive encoding (AST + types map + dep info); introduces stable-node-ID encoding per Decision 4 node-ID note
- `CompiledBackReferenceCodec.java` — sealed dispatcher for `CompiledBackReference` + `CompiledBackRefFragment` file I/O (Decision 10 Tier 1)
- `CompiledProjectManifestCodec.java` — Tier 2 manifest I/O (Decision 10 Tier 2)
- `ValueSpecificationCodec.java` — AST serialization, reused across all body encodings
- `TypeInfoCodec.java` — internal `TypeInfo` serialization for use inside `CompiledExpression`
- `CompilationArtifactsWriter.java` — top-level: iterates elements, writes `.legend`, `.backrefs.legend`, `project_manifest.legend` to hierarchical layout
- `CompilationArtifactsReader.java` — top-level: reads all three file kinds, returns `CompilationArtifacts`-shaped result (`elements`, `fragments`, `manifest`)

**Per-`Compiled*`-record codec methods:**
- Each `Compiled*` record gets `toJson(Json.Writer)` instance method + `fromJson(Json.Obj)` static factory.
- Pattern per Decision 9: explicit field-by-field write/read using the `Json.Writer` / `Json.Obj` API.
- No reflection. Unknown fields → loud failure.

**File layout convention (Decision 11):**

```
outDir/
├── <package>/<SimpleName>.legend   (hierarchical, :: → /)
└── ...
```

Each file contains `{legendVersion, kind, element}`.

**Round-trip tests:**
- `PhaseBRoundTripTest` — build model → `compileAll()` → `CompilationArtifactsWriter.writeAll()` → `CompilationArtifactsReader.readAll()` in fresh JVM → `builder.loadCompilationArtifacts()` → run query → compare results byte-for-byte against direct-parsed model.
- `PhaseBGoldenTest` — snapshot of `.legend` files for a fixture model. Detects format drift.

**Direct-load tests:**
- `PhaseBDirectLoadTest` — skip `.pure` parsing entirely. Load `.legend` files directly. Run queries. Verify behavior.

**Performance tests:**
- Measure load time for kitchen-sink 10K `.legend` files. Target: ≤ 50ms (should be much faster than the 133ms `compileAll()` path since no type-checking, just parsing + record construction).

**Estimated LOC:** ~500 codecs + ~200 Writer/Reader + ~100 tests = ~800 total. Follow-on PR, not blocking v7 success criteria.

### Phase 8 — Monomorphization-on-compiled-state fast path (optional polish)

After Phase 1-2 land, `inlineUserFunction` already uses compiled state. Measure: does monomorphization at query time have a perceptible cost for large query corpora? If yes, consider per-query monomorphization memoization (keyed by actual-arg-type-tuple). Not blocking for v7.

---

## Risks & Mitigations

### Risk 1 — Refactoring `GetAllChecker` / `inlineUserFunction` breaks existing query behavior

**Mitigation:** All existing integration tests run before merge. Primitives are net behavioral no-op for query path (logic moved, not changed).

**Detection:** Any golden test delta is an immediate red flag.

### Risk 2 — Monomorphization on top of standalone is subtly different from today's inline-from-scratch

**Mitigation:** Existing tests (`UserFunctionIntegrationTest` etc.) pin the behavior. If a specific monomorphization case regresses, we either fix the mono path or document + accept the behavior change.

**Detection:** Integration tests.

### Risk 3 — `compileAll()` takes too long on very large models

**Mitigation:** Spike already shows 10K kitchen-sink at 133ms. 100K would be ~1.3s (linear). At 1M elements we'd be at ~13s — that's a plausible ceiling but well beyond any realistic model.

**Detection:** Large-model stress tests with published numbers.

### Risk 4 — Error messages degrade during transition

**Mitigation:** `CompilationError` carries element FQN + source location (once `SourceLocation` sidecar lands) + underlying throwable. Error reporting tested explicitly in Phase 2 unit tests.

**Detection:** `CompileAllErrorTest` asserts error content.

### Risk 5 — Contract test drifts or becomes hard to maintain

**Mitigation:** Snapshot-based. When `CompiledElement` hierarchy or any `Compiled*` record shape changes, the snapshot must be deliberately regenerated. No silent drift.

**Detection:** CI diff on the snapshot file.

### Risk 6 — Embedded extends remain unchecked

**Context:** `ExtendChecker` today skips embedded extends (handled by `MappingResolver`). `compileAll()` inherits this.

**Decision:** Out of scope for v7. If we want embedded extend type-checking later, it's a separate extension to `ExtendChecker` that benefits both paths.

**Documentation:** Called out explicitly in the Bazel doc under "coverage gaps."

---

## Resolved Questions

### R1 — Should `compileAll()` run during `addSource()` automatically, or be opt-in?

`compileAll()` remains **opt-in**. `addSource()` retains def records + populates the semantic model; callers decide when to validate everything. Build actions call `compileAll()` explicitly. Dev-loop and LSP flows keep that choice separate.

### R2 — Where do derived-property and constraint bodies live post-build?

Bodies remain owned by the builder's **retained def records** (`ClassDefinition`, `MappingDefinition`, etc.), not by `PureClass`. `PureModelBuilder` stops stripping def records after `addSource()`. The semantic model stays semantic; source-defined bodies live on the parsed def records; compiled dependencies load from `CompilationArtifacts` without any def records attached. `compileAll()` reads retained defs, not stripped semantic objects.

### R3 — How does `SourceLocation` thread through `CompilationError`?

`CompilationError` keeps an optional `SourceLocation` field. Populate `null` until the sidecar lands; wire concrete locations through once available.

### R4 — Does monomorphization need its own compiled-state-like structure?

No. Monomorphization remains transient query-time work layered on top of standalone compiled bodies. It is not serialized and does not become part of `CompilationArtifacts`.

### R5 — Should `TypeChecker`'s memoization map be mutable or rebuilt immutably?

Mutable internally, immutable externally. `TypeChecker` memoizes lazily inside a mutable map. `CompilationArtifacts` exposes immutable snapshots.

### R6 — Do we support recursive user functions?

No. Under the current AST-level macro-expansion strategy, recursive and mutually recursive user functions are unsupported. The existing inline-depth guard is enforcing a real language/runtime limitation, not a temporary implementation gap. Phase B preserves that behavior and surfaces it at build time via `compileAll()`.

---

## Success Criteria

V7 is "done" when:

1. ✅ **All existing engine tests pass** — no behavioral regression on query path.
2. ✅ **`Compiled*` hierarchy lands** (Phase 1a) — sealed types defined, `TypeCheckResult` renamed to `CompiledExpression`, `PlanGenerator` imports updated.
3. ✅ **`TypeChecker.check(PackageableElement)` works** (Phase 1b) — all element kinds produce correct `Compiled*` subtypes; query path routes through public `check(e)`; memoization verified.
4. ✅ **`compileAll()` smoke test passes** (Phase 2) — trivial model validates cleanly, `CompilationResult.artifacts().elements()` populated with one `CompiledElement` per element.
5. ✅ **Kitchen-sink contract test passes** (Phase 4) — every element kind × feature compiles, sealed hierarchy snapshot pinned.
6. ✅ **10K spike re-run** (Phase 3) — `compileAll()` time is ≤ 150ms (parity or better with external-orchestration baseline).
7. ✅ **Bazel doc updated** (Phase 6) — `validateElement` + `ProjectDependencyAnalyzer` sections collapsed into single `compileAll()` section.
8. ✅ **PhaseBProgressTest coverage matrix** (Phase 5) includes `compileAll` column, all kinds green.

Phase B serialization (Phase 7) and Phase 8 polish can ship in follow-on PRs once the above is in.

---

## What v7 Obsoletes / Updates

| Document | Action |
|---|---|
| `phase-b-serialization-strategy-v5-c0954a.md` | Superseded (archive, link v7 from top) |
| `phase-b-measurement-spike-10k-c0954a.md` | Update Phase 3 section with `compileAll()` numbers after Phase 3 of impl plan |
| `BAZEL_DEPENDENCY_PROPOSAL.md` | Collapse `validateElement(fqn)` + `ProjectDependencyAnalyzer` sections → single "Validation and dependency analysis via `compileAll()`" section referencing v7 |
| `BAZEL_IMPLEMENTATION_PLAN.md` | Update JSON schema references to match `Compiled*` record field names; `.json` → `.legend` extension; keep schema principles (Option C, always-emit empty lists, `superClassFqns`). |
| `com.gs.legend.compiler.TypeCheckResult` | Deleted (renamed to `CompiledExpression`, moved to `com.gs.legend.compiled`). Breaking import change for `PlanGenerator` and tests — mechanical fix. |

---

## Summary in One Paragraph

Every packageable element held by `PureModelBuilder` as a parsed def record produces a `CompiledElement` record via `PureModelBuilder.compileAll()` at build time. Bodies are unified as `CompiledExpression` — a resolved AST plus per-node type info (identity-keyed in memory; stable IDs introduced at persistence in Phase 7) and first-class dependency data — replacing today's `TypeCheckResult` as the public compiled-body contract. `TypeChecker` exposes exactly two public methods: `check(ValueSpecification)` and `check(PackageableElement)`; the per-kind `compile*` methods are private, reachable only through `check(e)`, which makes memoization structurally unbypassable. Query path and build path converge on the same API, but retained def records, the semantic model, and loaded compiled deps remain distinct layers. The transport boundary is `CompilationArtifacts` (elements + back-reference fragments + manifest), persisted via `CompilationArtifactsWriter` / `CompilationArtifactsReader` to `.legend` files using the project's `Json` util. `CompiledAssociation` models association-owned semantics; back-reference fragments model target-side projection without mutating `CompiledClass`. Monomorphization of non-recursive user-function call sites sits on top of standalone-compiled `CompiledFunction.body`; recursive user functions remain unsupported. Spike confirms 10K kitchen-sink at ~133ms. Exhaustive switch on sealed `PackageableElement` inside `TypeChecker.check` + kitchen-sink contract test + compiled-shape snapshot assertion guard against future drift. Ship in 8 phases: 1a (records), 1b (refactor), 2 (compileAll), 3 (re-spike), 4 (contract test), 5 (progress matrix), 6 (Bazel), 7 (Phase B serialization — follow-on), 8 (mono polish — optional).
