package com.gs.legend.compiler;

import com.gs.legend.ast.*;
import com.gs.legend.compiled.CompiledAssociation;
import com.gs.legend.compiled.CompiledClass;
import com.gs.legend.compiled.CompiledConnection;
import com.gs.legend.compiled.CompiledDatabase;
import com.gs.legend.compiled.CompiledDependencies;
import com.gs.legend.compiled.CompiledElement;
import com.gs.legend.compiled.CompiledEnum;
import com.gs.legend.compiled.CompiledExpression;
import com.gs.legend.compiled.CompiledFunction;
import com.gs.legend.compiled.CompiledMappedClass;
import com.gs.legend.compiled.CompiledMapping;
import com.gs.legend.compiled.CompiledProfile;
import com.gs.legend.compiled.CompiledRuntime;
import com.gs.legend.compiled.CompiledService;
import com.gs.legend.compiled.MappingKind;
import com.gs.legend.compiled.SourceLocation;
import com.gs.legend.model.ModelContext;
import com.gs.legend.model.SymbolTable;
import com.gs.legend.model.def.AssociationDefinition;
import com.gs.legend.model.def.ClassDefinition;
import com.gs.legend.model.def.ConnectionDefinition;
import com.gs.legend.model.def.DatabaseDefinition;
import com.gs.legend.model.def.EnumDefinition;
import com.gs.legend.model.def.FunctionDefinition;
import com.gs.legend.model.def.MappingDefinition;
import com.gs.legend.model.def.PackageableElement;
import com.gs.legend.model.def.ProfileDefinition;
import com.gs.legend.model.def.RuntimeDefinition;
import com.gs.legend.model.def.ServiceDefinition;
import com.gs.legend.model.m3.Primitive;
import com.gs.legend.model.m3.Type;

import java.util.*;

/**
 * Clean compiler for Pure expressions.
 *
 * <p>
 * Takes untyped {@link ValueSpecification} AST (from
 * {@link ValueSpecificationBuilder})
 * and produces typed {@link TypeInfo} with full type checking via
 * {@link RelationType}.
 *
 * <p>
 * Responsibilities:
 * <ul>
 * <li>Name resolution — resolve table/class names to tables via store
 * metadata</li>
 * <li>Property→column mapping — resolve $p.firstName → FIRST_NAME via
 * MappingRegistry</li>
 * <li>Type checking — validate column/property existence</li>
 * <li>Type inference — derive result types for projections and computed
 * columns</li>
 * <li>Type.Schema propagation — track columns through the
 * pipeline</li>
 * </ul>
 */
public class TypeChecker implements TypeCheckEnv {

    /**
     * Built-in function registry — validates function existence, no more
     * passthrough.
     */
    private static final BuiltinRegistry builtinRegistry = BuiltinRegistry.instance();

    private final ModelContext modelContext;
    /** Class property accesses observed during compilation (className → property names). */
    private final Map<String, Set<String>> classPropertyAccesses = new HashMap<>();
    /** Association navigations observed during compilation (className → association property names). */
    private final Map<String, Set<String>> associationNavigations = new HashMap<>();
    /**
     * Compiled mapping functions keyed by class FQN. Populated as a side-effect
     * of {@link #compileMappingFunctionFor(String)} — covers both the query-root
     * fetch (via {@code GetAllChecker}) and pass-2 association-target fan-out
     * (via {@link #compileNeededAssociationTargets()}). Exposed to downstream
     * passes via {@code CompiledDependencies.mappingFunctions}, so
     * {@code MappingResolver} can look up a class's compiled mapping body
     * without holding a {@code TypeChecker} reference.
     */
    private final Map<String, CompiledFunction> mappingFunctions = new HashMap<>();
    /**
     * Memoization of compiled element results, keyed by element FQN.
     *
     * <p><strong>Correctness depends on a lifecycle invariant</strong>: a single
     * {@code TypeChecker} instance operates over a stable {@link ModelContext}
     * snapshot — no one mutates def records mid-compile. Given that, FQN
     * uniquely identifies one def-record content instance for the TypeChecker's
     * lifetime, so FQN-keyed memoization is safe without content hashing.
     *
     * <p>This matches current usage: every compilation cycle constructs a
     * fresh TypeChecker. {@code compileAll()} (Phase 2) will share one
     * TypeChecker across a build; the same invariant still holds because
     * {@code PureModelBuilder} takes a def-record snapshot at the start of
     * the call.
     *
     * <p><strong>Do not reuse a {@code TypeChecker} across compilation cycles
     * whose underlying model may have changed</strong> — e.g., LSP hot-reload
     * where a file edit replaces a def record for the same FQN. In that case
     * construct a new TypeChecker.
     */
    private final Map<String, CompiledElement> compiledElements = new HashMap<>();

    /**
     * Overload-aware memoization for {@link CompiledFunction}, keyed by
     * {@link com.gs.legend.model.m3.PureFunction} identity. Functions differ from other
     * {@link PackageableElement}s in that overloads share an FQN, so FQN-keyed caching
     * (as in {@link #compiledElements}) would collide across overloads.
     *
     * <p>{@link com.gs.legend.model.m3.PureFunction} is built once per overload during
     * {@link com.gs.legend.model.PureModelBuilder#assemble()}; each instance is immutable
     * and unique to its (FQN + signature) combo, making identity-keyed caching
     * content-correct without a separate content hash.
     */
    private final Map<com.gs.legend.model.m3.PureFunction, CompiledFunction> compiledFunctions =
            new IdentityHashMap<>();

    /**
     * Per-compile monotonic counter backing {@link #freshSymbol(String)}.
     * Instance-scoped — each {@code TypeChecker} starts at zero so gensym'd
     * names are deterministic within a single compilation and reset between
     * compilations.
     */
    private final java.util.concurrent.atomic.AtomicInteger gensymCounter =
            new java.util.concurrent.atomic.AtomicInteger();

    public TypeChecker(ModelContext modelContext) {
        this.modelContext = Objects.requireNonNull(modelContext, "ModelContext must not be null");
    }

    public ModelContext modelContext() {
        return modelContext;
    }

    /**
     * Strict variant — compile a class's synthetic mapping function or throw.
     *
     * <p>Used by the build path ({@link #compileMapping(MappingDefinition)}),
     * where the input is a declared {@link MappingDefinition} and a missing
     * function FQN means {@code MappingNormalizer} produced inconsistent state.
     *
     * <p>Query paths ({@code GetAllChecker}, pass-2 association fan-out) call
     * {@link #tryCompileMappingFunctionFor(String)} instead — for those callers
     * a missing mapping is a back-end / link-time error, not a type error.
     *
     * <p>Resolves the function FQN via {@link ModelContext#findMappingFunctionFqn}
     * (overlay from {@code MappingNormalizer}), then compiles via
     * {@link #check(com.gs.legend.model.m3.PureFunction)} — identity-keyed
     * memoization in {@code check(pf)} short-circuits repeat calls.
     */
    private CompiledFunction compileMappingFunctionFor(String classFqn) {
        return tryCompileMappingFunctionFor(classFqn)
                .orElseThrow(() -> new PureCompileException(
                        "No mapping in active scope for class '" + classFqn
                                + "' — cannot compile mapping function"));
    }

    private java.util.Optional<CompiledFunction> tryCompileMappingFunctionFor(String classFqn) {
        CompiledFunction cached = mappingFunctions.get(classFqn);
        if (cached != null) return java.util.Optional.of(cached);
        java.util.Optional<String> fnFqnOpt = modelContext.findMappingFunctionFqn(classFqn);
        if (fnFqnOpt.isEmpty()) return java.util.Optional.empty();
        String fnFqn = fnFqnOpt.get();
        List<com.gs.legend.model.m3.PureFunction> candidates = modelContext.findFunction(fnFqn);
        if (candidates.isEmpty()) {
            // FQN registered but body missing — that's a real model-context
            // inconsistency, not a "no mapping" probe miss. Fail loudly.
            throw new PureCompileException(
                    "Mapping function '" + fnFqn + "' not found in model context");
        }
        CompiledFunction compiled = check(candidates.get(0));
        // Expose on CompiledDependencies so MappingResolver can look the
        // function up by class FQN without holding a TypeChecker reference.
        mappingFunctions.put(classFqn, compiled);
        return java.util.Optional.of(compiled);
    }

    /**
     * Top-level compile: returns a {@link CompiledExpression} bundling the typed
     * HIR root and member-level dependency data.
     */
    public CompiledExpression check(ValueSpecification vs) {
        com.gs.legend.compiler.typed.TypedSpec root = compileExpr(vs, new CompilationContext());

        if (root == null) {
            throw new PureCompileException(
                    "TypeChecker: no TypedSpec produced for root " + vs.getClass().getSimpleName());
        }
        if (root.info() == null) {
            throw new PureCompileException(
                    "TypeChecker: ExpressionType not set on root " + vs.getClass().getSimpleName());
        }

        compileTouchedMappings();
        return new CompiledExpression(
                root,
                new CompiledDependencies(classPropertyAccesses, associationNavigations, mappingFunctions));
    }

    // ============================================================
    // check(PackageableElement) — Phase 1b API surface.
    //
    // Exactly one public method per kind, with an exhaustive sealed
    // switch on PackageableElement. Adding a new PackageableElement
    // subtype forces a compile error here — query path and build path
    // both converge on this method, so nothing silently drops elements.
    //
    // Each private compile*() builds the corresponding Compiled* record
    // and installs it in `compiledElements` before returning. Results
    // are memoized by FQN — repeat calls return the same instance.
    //
    // Phase 1b chunk 1 lands the API shape only: the per-kind compile*
    // methods throw UnsupportedOperationException until their owning
    // chunk (GetAllChecker / inlineUserFunction / compileProperty
    // refactor) wires them up. Nothing currently calls check(e), so the
    // stubs never execute in production — query path and tests are
    // unaffected.
    // ============================================================

    /**
     * Compile one packageable element to its {@link CompiledElement}
     * representation. Memoized — repeat calls on the same element return
     * the same cached instance.
     *
     * <p>This is the single public compile entry point for the sealed
     * {@link PackageableElement} hierarchy. Both the build path
     * ({@code PureModelBuilder.compileAll()} iterating) and the query
     * path ({@code GetAllChecker} / {@code inlineUserFunction} /
     * {@code compileProperty}) converge here, which makes it
     * structurally impossible to bypass memoization.
     */
    public CompiledElement check(PackageableElement e) {
        Objects.requireNonNull(e, "PackageableElement must not be null");
        var cached = compiledElements.get(e.qualifiedName());
        if (cached != null) return cached;
        CompiledElement compiled = switch (e) {
            case ClassDefinition cd       -> compileClass(cd);
            case MappingDefinition md     -> compileMapping(md);
            case FunctionDefinition fd    -> throw new PureCompileException(
                    "FunctionDefinition is a parse-layer element and cannot be compiled "
                            + "directly. Resolve it to a PureFunction via "
                            + "ModelContext.findFunction(fqn) and call check(PureFunction) instead. "
                            + "Offending FQN: " + fd.qualifiedName());
            case ServiceDefinition sd     -> compileService(sd);
            case AssociationDefinition ad -> compileAssociation(ad);
            case DatabaseDefinition dd    -> compileDatabase(dd);
            case EnumDefinition ed        -> compileEnum(ed);
            case ProfileDefinition pd     -> compileProfile(pd);
            case ConnectionDefinition cd  -> compileConnection(cd);
            case RuntimeDefinition rd     -> compileRuntime(rd);
        };
        compiledElements.put(e.qualifiedName(), compiled);
        return compiled;
    }

    /**
     * Compiles a {@link com.gs.legend.model.m3.PureFunction} (the typed downstream
     * metamodel form) to a {@link CompiledFunction}. This is the canonical entry point
     * for function compilation — the compiler does not consume the parse-layer
     * {@link FunctionDefinition}; that is the {@link com.gs.legend.model.PureModelBuilder}'s
     * input.
     *
     * <p>Memoized via {@link #compiledFunctions}, keyed by PureFunction identity so
     * overloads (which share an FQN) get distinct cache entries.
     */
    public CompiledFunction check(com.gs.legend.model.m3.PureFunction pf) {
        Objects.requireNonNull(pf, "PureFunction must not be null");
        return compileFunction(pf);
    }

    /** Narrow-return overload — same dispatch, more precise return type. */
    public CompiledClass       check(ClassDefinition cd)       { return (CompiledClass)       check((PackageableElement) cd); }
    /** Narrow-return overload. */
    public CompiledMapping     check(MappingDefinition md)     { return (CompiledMapping)     check((PackageableElement) md); }
    /** Narrow-return overload. */
    public CompiledService     check(ServiceDefinition sd)     { return (CompiledService)     check((PackageableElement) sd); }
    /** Narrow-return overload. */
    public CompiledAssociation check(AssociationDefinition ad) { return (CompiledAssociation) check((PackageableElement) ad); }
    /** Narrow-return overload. */
    public CompiledDatabase    check(DatabaseDefinition dd)    { return (CompiledDatabase)    check((PackageableElement) dd); }
    /** Narrow-return overload. */
    public CompiledEnum        check(EnumDefinition ed)        { return (CompiledEnum)        check((PackageableElement) ed); }
    /** Narrow-return overload. */
    public CompiledProfile     check(ProfileDefinition pd)     { return (CompiledProfile)     check((PackageableElement) pd); }
    /** Narrow-return overload. */
    public CompiledConnection  check(ConnectionDefinition cd)  { return (CompiledConnection)  check((PackageableElement) cd); }
    /** Narrow-return overload. */
    public CompiledRuntime     check(RuntimeDefinition rd)     { return (CompiledRuntime)     check((PackageableElement) rd); }

    // --- Private per-kind compilation (Phase 1b chunks 2+ implement these) ---

    private CompiledClass compileClass(ClassDefinition cd) {
        throw new UnsupportedOperationException(
                "Phase 1b: compileClass not yet implemented for " + cd.qualifiedName());
    }

    /**
     * Compiles a {@link MappingDefinition} into a {@link CompiledMapping}.
     *
     * <p>For each class the mapping covers, pulls the already-normalized
     * sourceSpec from {@code modelContext.findMappingExpression(classFqn)}
     * (produced earlier by {@code MappingNormalizer}), type-checks it via
     * {@link #compileExpr(com.gs.legend.ast.ValueSpecification, CompilationContext)},
     * and wraps it as a {@link CompiledMappedClass}.
     *
     * <p>Idempotent with the query path: per-class sourceSpec compilation is
     * guarded by {@code compiledSourceSpecs}, so a subsequent
     * {@code GetAllChecker} hit on the same class is a no-op.
     *
     * <p>{@code rootTableFqn} / {@code sourceClassFqn} on the resulting
     * mapped-class records carry what the def-record exposes directly; a
     * later chunk resolves {@code rootTableFqn} to a fully-qualified
     * database FQN (today the def record only carries the short table name).
     */
    private CompiledMapping compileMapping(MappingDefinition md) {
        var mappedClasses = new ArrayList<CompiledMappedClass>();
        for (var cm : md.classMappings()) {
            String classFqn = cm.className();
            // Single primitive — compiles the synthetic mapping function via
            // the same path used by GetAllChecker and pass-2 association fan-out.
            // Memoization on PureFunction identity inside check(pf) makes this
            // idempotent with prior query-path compilation.
            CompiledFunction mappingFunction = compileMappingFunctionFor(classFqn);

            MappingKind kind = cm.isM2M() ? MappingKind.M2M : MappingKind.RELATIONAL;

            mappedClasses.add(new CompiledMappedClass(
                    classFqn, kind, mappingFunction, cm.sourceName()));
        }
        return new CompiledMapping(md.qualifiedName(), List.copyOf(mappedClasses), SourceLocation.UNKNOWN);
    }

    /**
     * Detects if a {@link Type} represents a {@code Relation<(col:Type,...)>} and, if so,
     * builds its row {@link Type.Schema}. Accepts either the pre-classified
     * {@link Type.Relation} form or the parsed {@link Type.GenericType} form. Returns
     * {@code null} for any non-Relation type.
     */
    private Type.Schema asRelationSchema(Type t) {
        if (t instanceof Type.Relation rel) return rel.schema();
        if (t instanceof Type.GenericType p
                && isRelationRawType(p.rawType())
                && !p.typeArgs().isEmpty()
                && p.typeArgs().get(0) instanceof Type.RelationTypeVar rtv) {
            var columns = new java.util.LinkedHashMap<String, Type>();
            for (var col : rtv.columns()) {
                columns.put(col.name(), classifyUserType(col.type()));
            }
            return Type.Schema.withoutPivot(columns);
        }
        return null;
    }

    /**
     * Compiles a typed {@link com.gs.legend.model.m3.PureFunction} to a
     * {@link CompiledFunction}.
     *
     * <p>Flow:
     * <ol>
     *   <li>Consult {@link #compiledFunctions} cache (keyed by PureFunction identity
     *       for overload awareness) — return cached if present.</li>
     *   <li>Bind each parameter into a fresh {@link CompilationContext}.</li>
     *   <li>Compile the body via the shared {@link #compileBodyInContext} primitive,
     *       validating declared return type + multiplicity.</li>
     *   <li>Project typed parameters to {@link CompiledParameter}, wrap the compiled
     *       body in {@link CompiledExpression}, and cache the resulting
     *       {@link CompiledFunction}.</li>
     * </ol>
     */
    private CompiledFunction compileFunction(com.gs.legend.model.m3.PureFunction pureFn) {
        CompiledFunction cached = compiledFunctions.get(pureFn);
        if (cached != null) return cached;

        // Recursion guard lives at ingest: PureModelBuilder.detectCallGraphCycles rejects
        // cyclic user-function call graphs, so any PureFunction reaching this point is
        // part of a DAG and transitive compilation cannot loop back on itself.
        //
        // Bind declared parameters into a fresh context. Relation params get schema
        // binding so $rel.COL resolves via {@link CompilationContext#getRelationType};
        // everything else (Primitive, ClassType/NameRef, FunctionType, etc.) goes
        // through the generic lambda-param channel.
        //
        // Synthetic mapping function detection: when this PureFunction is a
        // <class>::mappingFunction (per the canonical class↔fn binding maintained
        // by NormalizedMapping), stamp the materialized class FQN on the context
        // so binding-aware checkers (ExtendChecker) can validate per-ColSpec
        // property bindings against the model. Inverse-map lookup, no FQN parsing.
        CompilationContext ctx = new CompilationContext();
        var mappingTargetClass = modelContext.findClassForMappingFunction(pureFn.qualifiedName());
        if (mappingTargetClass.isPresent()) {
            ctx = ctx.withMappingTarget(mappingTargetClass.get());
        }
        for (var p : pureFn.parameters()) {
            Type.Schema relationSchema = asRelationSchema(p.type());
            if (relationSchema != null) {
                ctx = ctx.withRelationType(p.name(), relationSchema);
            } else {
                ctx = ctx.withLambdaParam(p.name(), p.type());
            }
        }

        // Compile body + validate declared return against actual body result —
        // the last statement's TypedSpec becomes the function's HIR root.
        com.gs.legend.compiler.typed.TypedSpec bodyHir = compileBodyInContext(
                pureFn.body(), ctx,
                pureFn.returnType(), pureFn.returnMultiplicity(),
                "Function '" + pureFn.qualifiedName() + "'");

        // Per-function body carries the same TypeChecker-level aggregated maps.
        // Only the root CompiledExpression populates mappingFunctions meaningfully;
        // individual function bodies pass an empty map because they are not the
        // consumption point for MappingResolver lookups.
        CompiledExpression body = new CompiledExpression(
                bodyHir,
                new CompiledDependencies(classPropertyAccesses, associationNavigations, Map.of()));

        List<com.gs.legend.compiled.CompiledParameter> compiledParams = pureFn.parameters().stream()
                .map(p -> new com.gs.legend.compiled.CompiledParameter(p.name(), p.type(), p.multiplicity()))
                .toList();

        CompiledFunction result = new CompiledFunction(
                pureFn.qualifiedName(),
                compiledParams,
                pureFn.returnType(),
                pureFn.returnMultiplicity(),
                body,
                SourceLocation.UNKNOWN);
        compiledFunctions.put(pureFn, result);
        return result;
    }

    private CompiledService compileService(ServiceDefinition sd) {
        throw new UnsupportedOperationException(
                "Phase 1b: compileService not yet implemented for " + sd.qualifiedName());
    }

    private CompiledAssociation compileAssociation(AssociationDefinition ad) {
        throw new UnsupportedOperationException(
                "Phase 1b: compileAssociation not yet implemented for " + ad.qualifiedName());
    }

    private CompiledDatabase compileDatabase(DatabaseDefinition dd) {
        throw new UnsupportedOperationException(
                "Phase 1b: compileDatabase not yet implemented for " + dd.qualifiedName());
    }

    private CompiledEnum compileEnum(EnumDefinition ed) {
        throw new UnsupportedOperationException(
                "Phase 1b: compileEnum not yet implemented for " + ed.qualifiedName());
    }

    private CompiledProfile compileProfile(ProfileDefinition pd) {
        throw new UnsupportedOperationException(
                "Phase 1b: compileProfile not yet implemented for " + pd.qualifiedName());
    }

    private CompiledConnection compileConnection(ConnectionDefinition cd) {
        throw new UnsupportedOperationException(
                "Phase 1b: compileConnection not yet implemented for " + cd.qualifiedName());
    }

    private CompiledRuntime compileRuntime(RuntimeDefinition rd) {
        throw new UnsupportedOperationException(
                "Phase 1b: compileRuntime not yet implemented for " + rd.qualifiedName());
    }

    /**
     * Pass 2: compiles the mapping function for every class statically
     * referenced by the body — the touched-class closure.
     *
     * <p>The touched-class set is just {@link #classPropertyAccesses}'s
     * keyset. Pass-1 populates it from three identical write-sites, all
     * inside TypeChecker:
     * <ul>
     *   <li>Direct property access ({@code $x.prop}) — registers the
     *       owning class.</li>
     *   <li>Association navigation ({@code $x.assocProp}) — registers
     *       both the owning class (with the assoc prop) and the target
     *       class (with no prop). Same call site as direct access; assoc
     *       targets are not "special".</li>
     *   <li>{@code getAll(Class)} dispatch — registers the root class.
     *       This one is at the dispatch site (not inside a property
     *       resolver) because there is no property being accessed; bare
     *       {@code Class.all()} otherwise leaves the maps empty.</li>
     * </ul>
     *
     * <p>Demand-driven worklist. Each iteration drains the frontier
     * (keyset minus already-attempted) and probe-compiles each class.
     * Compiling a mapping body recursively type-checks it, which calls
     * {@code compileProperty} for every property/assoc inside it, which
     * grows {@link #classPropertyAccesses} — the next iteration picks up
     * the new entries. Fixed point when no new entries arrive.
     *
     * <p>Termination: {@code attempted} grows monotonically; the class
     * universe is finite. The probe variant
     * ({@link #tryCompileMappingFunctionFor}) returns empty for classes
     * without a mapping in scope — those become a back-end / link-time
     * error at the use site ({@code SourceLowering}), not a type-check
     * failure.
     */
    private void compileTouchedMappings() {
        Set<String> attempted = new HashSet<>();
        while (true) {
            Set<String> needed = new HashSet<>(classPropertyAccesses.keySet());
            needed.removeAll(attempted);
            if (needed.isEmpty()) return;
            for (String target : needed) {
                attempted.add(target);
                tryCompileMappingFunctionFor(target);
            }
        }
    }

    /**
     * Internal: compiles a ValueSpecification to a typed result.
     * Called recursively for sub-expressions.
     */
    @Override
    public com.gs.legend.compiler.typed.TypedSpec compileExpr(ValueSpecification vs, CompilationContext ctx) {
        return switch (vs) {
            case AppliedFunction af -> compileFunction(af, ctx);
            case com.gs.legend.ast.ColumnInstance ci -> throw new PureCompileException(
                    "Unexpected ColumnInstance " + ci.getClass().getSimpleName()
                            + " at expression position — must be a function argument, not a top-level expression");
            case com.gs.legend.ast.NewInstance id -> throw new PureCompileException(
                    "Unexpected NewInstance '" + id.className()
                            + "' at expression position — must be the second argument to new(), not a top-level expression");
            case LambdaFunction lf -> compileLambda(lf, ctx);
            case Variable v -> compileVariable(v, ctx);
            case AppliedProperty ap -> compileProperty(ap, ctx);
            case PackageableElementPtr pe -> resolvePackageableElement(pe);
            // TypeAnnotation is a type-reference argument only; callers (cast, etc.)
            // inspect the VS directly. A passthrough typed wrapper would be pure
            // noise — surface the misuse loudly.
            case TypeAnnotation ta -> throw new PureCompileException(
                    "TypeAnnotation '" + ta + "' cannot be a standalone expression — "
                            + "it is only valid as the type argument of cast/@-annotation");
            case PureCollection coll -> compileCollection(coll, ctx);
            // Literals — one typed variant per literal kind
            case CInteger i -> new com.gs.legend.compiler.typed.TypedCInteger(
                    i.value(), ExpressionType.one(classifyInteger(i)));
            case CFloat f -> new com.gs.legend.compiler.typed.TypedCFloat(
                    f.value(), ExpressionType.one(Primitive.FLOAT));
            case CDecimal d -> new com.gs.legend.compiler.typed.TypedCDecimal(
                    d.value(), ExpressionType.one(classifyDecimal(d)));
            case CString s -> new com.gs.legend.compiler.typed.TypedCString(
                    s.value(), ExpressionType.one(Primitive.STRING));
            case CBoolean b -> new com.gs.legend.compiler.typed.TypedCBoolean(
                    b.value(), ExpressionType.one(Primitive.BOOLEAN));
            case CDateTime dt -> new com.gs.legend.compiler.typed.TypedCDateTime(
                    dt.value(), ExpressionType.one(Primitive.DATE_TIME));
            case CStrictDate sd -> new com.gs.legend.compiler.typed.TypedCStrictDate(
                    sd.value(), ExpressionType.one(Primitive.STRICT_DATE));
            case CStrictTime st -> new com.gs.legend.compiler.typed.TypedCStrictTime(
                    st.value(), ExpressionType.one(Primitive.STRICT_TIME));
            case CLatestDate ld -> new com.gs.legend.compiler.typed.TypedCLatestDate(
                    ExpressionType.one(Primitive.DATE_TIME));
            case CByteArray ba -> new com.gs.legend.compiler.typed.TypedCByteArray(
                    ba.value(), ExpressionType.one(Primitive.STRING));
            case EnumValue ev -> new com.gs.legend.compiler.typed.TypedEnumValue(
                    ev.fullPath(), ev.value(), ExpressionType.one(new Type.EnumType(ev.fullPath())));
            // UnitInstance — carries a numeric value with a unit; model as ANY for
            // now. Refine when the unit subsystem is typed.
            case UnitInstance ui -> {
                // Touch the field so the variable isn't flagged unused.
                @SuppressWarnings("unused") var unused = ui.unitType();
                yield new com.gs.legend.compiler.typed.TypedCFloat(
                        0.0d, ExpressionType.one(Primitive.FLOAT));
            }
        };
    }

    // ========== PackageableElement Resolution ==========

    /**
     * Resolves a PackageableElementPtr to the correct Type by looking up
     * the name in all available registries — analogous to legend-engine's
     * {@code CompileContext.resolvePackageableElement()}.
     *
     * <p>Resolution order:
     * <ol>
     *   <li>Function registries (builtin + user) → {@link Type.FunctionReference}</li>
     *   <li>Class registry → {@link Type.ClassType}</li>
     *   <li>Mapping registry → {@link Type.ClassType} (named element)</li>
     *   <li>Enum registry → {@link Type.EnumType}</li>
     *   <li>Unresolved → {@link Type.ClassType} with full path
     *       (runtimes, stores — named elements not yet in registries)</li>
     * </ol>
     *
     * <p>Function signature-encoded names (e.g.,
     * {@code eq_Any_1__Any_1__Boolean_1_}) are decoded by extracting the base
     * name before the first {@code _} when the name contains {@code __}
     * (the Pure parameter-group separator).
     */
    private com.gs.legend.compiler.typed.TypedPackageableRef resolvePackageableElement(PackageableElementPtr pe) {
        String path = pe.fullPath();
        String name = simpleName(path);

        // 1. Check function registries
        //    Direct match first (e.g., "removeDuplicates")
        if (builtinRegistry.isRegistered(name) || !modelContext.findFunction(path).isEmpty()
                || !modelContext.findFunction(name).isEmpty()) {
            return new com.gs.legend.compiler.typed.TypedPackageableRef(
                    path, ExpressionType.one(new Type.FunctionReference(path)));
        }
        //    Signature-encoded name (e.g., "eq_Any_1__Any_1__Boolean_1_"):
        //    Pure encodes function signatures as name_Type_mult__Type_mult__RetType_mult_
        //    The double-underscore __ separates parameter groups.
        if (name.contains("__")) {
            int firstUnderscore = name.indexOf('_');
            if (firstUnderscore > 0) {
                String baseName = name.substring(0, firstUnderscore);
                if (builtinRegistry.isRegistered(baseName)) {
                    return new com.gs.legend.compiler.typed.TypedPackageableRef(
                            path, ExpressionType.one(new Type.FunctionReference(path)));
                }
            }
        }

        // 2. Check model registries — resolve to FQN for consistent downstream keys
        var classOpt = modelContext.findClass(path);
        if (classOpt.isEmpty()) classOpt = modelContext.findClass(name);
        if (classOpt.isPresent()) {
            String fqn = classOpt.get().qualifiedName();
            return new com.gs.legend.compiler.typed.TypedPackageableRef(
                    fqn, ExpressionType.one(new Type.ClassType(fqn)));
        }
        if (modelContext.findEnum(path).isPresent() || modelContext.findEnum(name).isPresent()) {
            return new com.gs.legend.compiler.typed.TypedPackageableRef(
                    path, ExpressionType.one(new Type.EnumType(path)));
        }

        // 3. Unresolved — named element reference (runtimes, stores, etc.)
        //    These are valid Pure elements not yet in our registries.
        //    Type as ClassType (a named reference) rather than STRING.
        //    TODO: Add findRuntime/findStore to ModelContext for full resolution.
        return new com.gs.legend.compiler.typed.TypedPackageableRef(
                path, ExpressionType.one(new Type.ClassType(path)));
    }

    // ========== Function Dispatch ==========

    /**
     * Dispatches function calls to the appropriate checker.
     *
     * <p>
     * Pattern: pre-compile source &rarr; switch dispatch &rarr; post-stamp.
     * Source-less functions (getAll, match, if, eval, let) simply ignore
     * the pre-compiled source and access af.parameters() directly.
     */
    private com.gs.legend.compiler.typed.TypedSpec compileFunction(AppliedFunction af, CompilationContext ctx) {
        // User function — FQN-first lookup, before built-in switch
        var userFuncs = modelContext.findFunction(af.function());
        if (!userFuncs.isEmpty()) {
            return compileUserCall(af, userFuncs, ctx);
        }

        String funcName = simpleName(af.function());

        // Compile first arg (source) for most functions.
        // eval is excluded: its source is an "applicable" (colSpec, funcRef, lambda),
        // not a value — EvalChecker handles its own source compilation.
        com.gs.legend.compiler.typed.TypedSpec source = !af.parameters().isEmpty() && !"eval".equals(funcName)
                ? compileExpr(af.parameters().get(0), ctx)
                : null;

        return switch (funcName) {
            // --- Relation Sources ---
            case "getAll" -> {
                var typedGetAll = new com.gs.legend.compiler.checkers.GetAllChecker(this).check(af, source, ctx);
                // Record the root class in classPropertyAccesses so it appears in
                // the touched-class set used by Pass-2 mapping-function compilation
                // and CompiledDependencies.elementFqns(). Covers bare Class.all()
                // with no subsequent property access — without this, the property-
                // access-driven path in compileProperty() never sees the class.
                classPropertyAccesses.computeIfAbsent(typedGetAll.className(), k -> new HashSet<>());
                yield typedGetAll;
            }
            case "tableReference" -> new com.gs.legend.compiler.checkers.TableReferenceChecker(this).check(af, source, ctx);
            case "sourceUrl" -> new com.gs.legend.compiler.checkers.SourceUrlChecker(this).check(af, source, ctx);
            case "tds" -> new com.gs.legend.compiler.checkers.TdsChecker(this).check(af, source, ctx);
            // --- Object Construction ---
            case "new" -> compileNew(af, ctx);
            // --- Shape-preserving ---
            case "sort", "sortBy", "sortByReversed" ->
                new com.gs.legend.compiler.checkers.SortChecker(this).check(af, source, ctx);
            case "filter" -> new com.gs.legend.compiler.checkers.FilterChecker(this).check(af, source, ctx);
            case "limit", "take", "drop", "slice" ->
                new com.gs.legend.compiler.checkers.SlicingChecker(this).check(af, source, ctx);
            // --- Column operations ---
            case "rename" -> new com.gs.legend.compiler.checkers.RenameChecker(this).check(af, source, ctx);
            case "select", "newTDSRelationAccessor" -> new com.gs.legend.compiler.checkers.SelectChecker(this).check(af, source, ctx);
            case "distinct" -> new com.gs.legend.compiler.checkers.DistinctChecker(this).check(af, source, ctx);
            // --- Shape-changing ---
            case "concatenate" -> new com.gs.legend.compiler.checkers.ConcatenateChecker(this).check(af, source, ctx);
            case "project" -> new com.gs.legend.compiler.checkers.ProjectChecker(this).check(af, source, ctx);
            case "groupBy" -> new com.gs.legend.compiler.checkers.GroupByChecker(this).check(af, source, ctx);
            case "aggregate" -> new com.gs.legend.compiler.checkers.AggregateChecker(this).check(af, source, ctx);
            case "extend" -> new com.gs.legend.compiler.checkers.ExtendChecker(this).check(af, source, ctx);
            case "join" -> new com.gs.legend.compiler.checkers.JoinChecker(this).check(af, source, ctx);
            case "asOfJoin" -> new com.gs.legend.compiler.checkers.AsOfJoinChecker(this).check(af, source, ctx);
            case "pivot" -> new com.gs.legend.compiler.checkers.PivotChecker(this).check(af, source, ctx);
            case "flatten" -> new com.gs.legend.compiler.checkers.FlattenChecker(this).check(af, source, ctx);
            case "from" -> new com.gs.legend.compiler.checkers.FromChecker(this).check(af, source, ctx);
            // --- Scalar collection functions with lambdas ---
            case "map" -> new com.gs.legend.compiler.checkers.MapChecker(this).check(af, source, ctx);
            case "fold" -> new com.gs.legend.compiler.checkers.FoldChecker(this).check(af, source, ctx);
            case "zip" -> new com.gs.legend.compiler.checkers.ZipChecker(this).check(af, source, ctx);
            case "if" -> new com.gs.legend.compiler.checkers.IfChecker(this).check(af, source, ctx);
            case "letFunction" -> new com.gs.legend.compiler.checkers.LetChecker(this).check(af, source, ctx);
            // --- Type functions ---
            case "cast" -> new com.gs.legend.compiler.checkers.CastChecker(this).check(af, source, ctx);
            case "toMany", "toOne", "toVariant", "to" ->
                new com.gs.legend.compiler.checkers.TypeConversionChecker(this).check(af, source, ctx);
            // --- Variant access ---
            case "get" -> new com.gs.legend.compiler.checkers.GetChecker(this).check(af, source, ctx);
            case "write" -> new com.gs.legend.compiler.checkers.WriteChecker(this).check(af, source, ctx);
            // --- GraphFetch / Serialize ---
            case "graphFetch" -> new com.gs.legend.compiler.checkers.GraphFetchChecker(this).check(af, source, ctx);
            case "serialize" -> new com.gs.legend.compiler.checkers.SerializeChecker(this).check(af, source, ctx);
            case "eval" -> new com.gs.legend.compiler.checkers.EvalChecker(this).check(af, source, ctx);
            case "match" -> new com.gs.legend.compiler.checkers.MatchChecker(this).check(af, source, ctx);
            // --- Unknown: builtin → error ---
            default -> {
                if (builtinRegistry.isRegistered(funcName))
                    yield new com.gs.legend.compiler.checkers.ScalarChecker(this).check(af, source, ctx);
                throw new PureCompileException(
                        "Unknown function '" + funcName + "'. "
                                + "Function is not registered and no user-defined function found. "
                                + "Available functions: " + builtinRegistry.functionCount() + " registered.");
            }
        };
    }

    /**
     * Black-box call to a user-defined function. No AST substitution, no body
     * inlining — the compiled body lives once on the callee's {@link CompiledFunction}
     * and downstream consumers resolve it by FQN.
     *
     * <p>Steps:
     * <ol>
     *   <li>Arity filter over overloads.</li>
     *   <li>Compile each arg — bidirectional for {@link Type.FunctionType} params
     *       so lambda arg bodies see expected types.</li>
     *   <li>Overload resolution on compiled arg types.</li>
     *   <li>{@code check(pureFn)} to materialize the canonical compiled body once.</li>
     *   <li>Validate call-site arg types + multiplicity against compiled signature.</li>
     *   <li>Emit a {@link com.gs.legend.compiler.typed.TypedUserCall} carrying the
     *       callee FQN, typed args, and return {@code info} from the compiled signature.</li>
     * </ol>
     */
    private com.gs.legend.compiler.typed.TypedUserCall compileUserCall(AppliedFunction af,
            List<com.gs.legend.model.m3.PureFunction> candidates, CompilationContext ctx) {
        int argCount = af.parameters().size();

        // 1. Arity filter.
        var arityMatches = candidates.stream()
                .filter(f -> f.parameters().size() == argCount)
                .toList();
        if (arityMatches.isEmpty()) {
            throw new PureCompileException(
                    "No overload of '" + af.function() + "' accepts " + argCount + " argument(s). "
                            + "Available arities: " + candidates.stream()
                                    .map(f -> String.valueOf(f.parameters().size()))
                                    .distinct().sorted().toList());
        }

        // 2. Compile args (bidirectional for Function-typed params).
        var firstMatch = arityMatches.get(0);
        List<com.gs.legend.compiler.typed.TypedSpec> argSpecs = new ArrayList<>(argCount);
        for (int i = 0; i < argCount; i++) {
            var arg = af.parameters().get(i);
            var param = firstMatch.parameters().get(i);
            if (param.type() instanceof Type.FunctionType ft && arg instanceof LambdaFunction lambda) {
                int expectedArity = ft.params().size();
                if (lambda.parameters().size() != expectedArity) {
                    throw new PureCompileException(
                            "Function '" + af.function() + "' parameter '" + param.name()
                                    + "' expects a lambda with " + expectedArity + " param(s), "
                                    + "but got " + lambda.parameters().size());
                }
                argSpecs.add(compileLambdaWithExpectedType(lambda, ft, ctx));
            } else {
                argSpecs.add(compileExpr(arg, ctx));
            }
        }

        // 3. Resolve overload based on compiled arg types.
        var pureFn = resolveOverload(af.function(), arityMatches, argSpecs);

        // 4. Materialize the canonical compiled body once (memoized by PureFunction identity).
        CompiledFunction compiled = check(pureFn);

        // 5. Call-site validation against the canonical compiled signature.
        validateCallSiteArgTypes(af, compiled, argSpecs);
        validateCallSiteArgMultiplicity(af, compiled, argSpecs);

        // 6. Return ExpressionType: the user function's declared return type
        //    (classified). At compile time, a function declared {@code :Any[*]}
        //    returns {@code Any[*]} — the contract is the contract; we do not
        //    eagerly narrow to the body's inferred type. Classification
        //    converts any structural {@code GenericType(RELATION,
        //    [RelationTypeVar])} signature into a {@link Type.Relation}
        //    with a resolved {@link Type.Schema} (the form downstream
        //    {@code instanceof Type.Relation} sites expect).
        //
        //    When the runtime/execution layer needs the *effective* shape
        //    (e.g., Tabular schema for an {@code :Any[*]}-declared function
        //    whose body returns a Relation), it recurses into
        //    {@code callee().body()} at plan-generation time — see
        //    {@link com.gs.legend.plan.ResultFormat#from} and
        //    {@link com.gs.legend.plan.PlanGenerator} effective-type lookup.
        return new com.gs.legend.compiler.typed.TypedUserCall(
                pureFn.qualifiedName(),
                List.copyOf(argSpecs),
                compiled,
                new ExpressionType(classifyUserType(pureFn.returnType()), pureFn.returnMultiplicity()));
    }

    /** Call-site arg-type validation against the canonical compiled signature. */
    private void validateCallSiteArgTypes(
            AppliedFunction af, CompiledFunction compiled,
            List<com.gs.legend.compiler.typed.TypedSpec> argSpecs) {
        for (int i = 0; i < compiled.parameters().size(); i++) {
            var param = compiled.parameters().get(i);
            var argSpec = argSpecs.get(i);
            if (param.type() instanceof Type.FunctionType) continue; // bidirectional already checked

            if (argSpec.type() == null) continue;
            Type declaredType = param.type();
            if (declaredType == Primitive.ANY) continue;

            if (declaredType instanceof Type.GenericType p
                    && isRelationRawType(p.rawType())
                    && !p.typeArgs().isEmpty()
                    && p.typeArgs().get(0) instanceof Type.RelationTypeVar) {
                Type declaredGeneric = classifyUserType(declaredType);
                checkRelationSchemaCompatibility(
                        af.function(), param.name(), argSpec.type(), declaredGeneric);
                continue;
            }

            if (!isSubtype(argSpec.type(), declaredType)) {
                throw new PureCompileException(
                        "Function '" + af.function() + "' parameter '" + param.name()
                                + "' expects " + declaredType.typeName()
                                + " but got " + argSpec.type().typeName());
            }
        }
    }

    /** Call-site arg-multiplicity validation against the canonical compiled signature. */
    private void validateCallSiteArgMultiplicity(
            AppliedFunction af, CompiledFunction compiled,
            List<com.gs.legend.compiler.typed.TypedSpec> argSpecs) {
        for (int i = 0; i < compiled.parameters().size(); i++) {
            var param = compiled.parameters().get(i);
            var argSpec = argSpecs.get(i);
            if (param.type() instanceof Type.FunctionType) continue;

            var actualMult = argSpec.multiplicity();
            var declMult = param.multiplicity();
            int declLower = declMult.lowerBound();
            Integer declUpper = declMult.upperBound();

            boolean lowerOk = actualMult.lowerBound() >= declLower;
            boolean upperOk = declUpper == null
                    || (actualMult.upperBound() != null && actualMult.upperBound() <= declUpper);

            if (!lowerOk || !upperOk) {
                throw new PureCompileException(
                        "Function '" + af.function() + "' parameter '" + param.name()
                                + "' expects multiplicity " + param.type().typeName() + declMult
                                + " but got " + actualMult);
            }
        }
    }

    /**
     * Resolves which overload to use when multiple functions match by arity.
     * If only one match, returns it. If multiple, uses compiled arg types to disambiguate.
     */
    private com.gs.legend.model.m3.PureFunction resolveOverload(
            String funcName,
            List<com.gs.legend.model.m3.PureFunction> arityMatches,
            List<com.gs.legend.compiler.typed.TypedSpec> argSpecs) {
        if (arityMatches.size() == 1) return arityMatches.get(0);

        // Score each candidate by how many arg types match declared param types
        var scored = new ArrayList<Map.Entry<com.gs.legend.model.m3.PureFunction, Integer>>();
        for (var candidate : arityMatches) {
            int matches = 0;
            for (int i = 0; i < candidate.parameters().size(); i++) {
                Type declaredType = candidate.parameters().get(i).type();
                if (declaredType == Primitive.ANY) {
                    matches++; // Any matches everything
                } else if (argSpecs.get(i).type() != null
                        && isSubtype(argSpecs.get(i).type(), declaredType)) {
                    matches++;
                }
            }
            scored.add(Map.entry(candidate, matches));
        }

        // Sort by match count descending
        scored.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));

        if (scored.size() >= 2 && scored.get(0).getValue().equals(scored.get(1).getValue())) {
            throw new PureCompileException(
                    "Ambiguous overload for '" + funcName + "': multiple candidates match with "
                            + scored.get(0).getValue() + " matching params");
        }

        return scored.get(0).getKey();
    }

    /**
     * Unified subtype check for all {@link Type}s.
     * <ul>
     *   <li>Primitive hierarchy — delegated to polymorphic {@link Primitive#isSubtypeOf}.</li>
     *   <li>Class hierarchy — delegated to {@link ModelContext#isClassSubtype}.</li>
     *   <li>Other Type variants — equality only (handled by Type's default isSubtypeOf).</li>
     * </ul>
     */
    private boolean isSubtype(Type actual, Type declared) {
        if (actual.isSubtypeOf(declared)) return true;
        if (actual instanceof Type.ClassType ac && declared instanceof Type.ClassType dc && modelContext != null) {
            return modelContext.isClassSubtype(ac.qualifiedName(), dc.qualifiedName());
        }
        // User-type NameRef on declared side: PureFunction keeps user types as NameRef to preserve
        // lazy loading (AGENTS.md §5). Compare by FQN instead of forcing findClass/findEnum.
        if (actual instanceof Type.ClassType ac && declared instanceof Type.NameRef nr) {
            if (ac.qualifiedName().equals(nr.qualifiedName())) return true;
            if (modelContext != null) {
                return modelContext.isClassSubtype(ac.qualifiedName(), nr.qualifiedName());
            }
        }
        if (actual instanceof Type.EnumType ae && declared instanceof Type.NameRef nr) {
            return ae.qualifiedName().equals(nr.qualifiedName());
        }
        return false;
    }

    /**
     * Validates that an actual Relation argument matches a declared Relation schema.
     * Checks each declared column: must exist in actual with a compatible type.
     * Superset is OK — actual may have extra columns. Throws with precise diagnostics.
     */
    private void checkRelationSchemaCompatibility(
            String funcName, String paramName, Type actualType, Type declaredType) {
        if (!(declaredType instanceof Type.Relation declaredRel)) {
            throw new PureCompileException(
                    "Function '" + funcName + "' parameter '" + paramName
                            + "': internal error — expected Relation declared type but got " + declaredType.typeName());
        }
        if (!(actualType instanceof Type.Relation actualRel)) {
            throw new PureCompileException(
                    "Function '" + funcName + "' parameter '" + paramName
                            + "' expects Relation but got " + actualType.typeName());
        }
        for (var entry : declaredRel.schema().columns().entrySet()) {
            String colName = entry.getKey();
            Type declaredColType = entry.getValue();
            Type actualColType = actualRel.schema().columns().get(colName);
            if (actualColType == null) {
                throw new PureCompileException(
                        "Function '" + funcName + "' parameter '" + paramName
                                + "': schema mismatch — missing column '" + colName
                                + "'. Available columns: " + actualRel.schema().columns().keySet());
            }
            if (!isSubtype(actualColType, declaredColType)) {
                String hint = primitiveMismatchHint(declaredColType, actualColType);
                throw new PureCompileException(
                        "Function '" + funcName + "' parameter '" + paramName
                                + "': schema mismatch — column '" + colName + "' expects "
                                + declaredColType.typeName() + " but got " + actualColType.typeName()
                                + hint);
            }
        }
    }

    /**
     * Suggests a fix for primitive-type schema mismatches commonly seen when
     * a property's declared Pure type doesn't match its mapped column's
     * inferred Pure type. Returns {@code ""} when no specific guidance applies.
     *
     * <p>Most frequent case: a class declares {@code Float} for a column
     * stored as {@code DECIMAL} (or vice versa). Pure's {@code Float} and
     * {@code Decimal} are sibling subtypes of {@code Number}, not subtypes
     * of each other — this is a real type bug, not a system limitation.
     * The hint enumerates the three honest fixes.
     */
    private String primitiveMismatchHint(Type declared, Type actual) {
        boolean declaredPrim = declared instanceof com.gs.legend.model.m3.Primitive
                || declared instanceof Type.PrecisionDecimal;
        boolean actualPrim = actual instanceof com.gs.legend.model.m3.Primitive
                || actual instanceof Type.PrecisionDecimal;
        if (declaredPrim && actualPrim) {
            return ". The class property and the mapped column declare different Pure types — change the property type to match the column, change the column type to match the property, or apply an explicit conversion (e.g. ->toFloat() / ->cast(@Decimal)) in the property mapping body.";
        }
        return "";
    }

    // ========== Body Compilation ==========

    /**
     * The single body-compile primitive. All four body-compile sites (top-level user
     * function, call-site inline specialization, lambda with pushed-in expected FunctionType,
     * lambda with inferred types) route through here for the actual body walk + optional
     * return validation.
     *
     * <p><strong>Caller responsibilities:</strong>
     * <ul>
     *   <li>Build the {@link CompilationContext} with whatever lambda/function parameter
     *       bindings are appropriate for the site.</li>
     *   <li>Decide whether to validate return — pass {@code expectedReturnType == null}
     *       <em>and</em> {@code expectedReturnMult == null} to skip (lambda inference case).</li>
     *   <li>Stamp the outer AST node ({@code LambdaFunction}, {@code AppliedFunction} call
     *       site) based on the returned {@link TypeInfo}. This primitive does not touch
     *       the outer node.</li>
     * </ul>
     *
     * <p><strong>Primitive responsibilities:</strong>
     * <ul>
     *   <li>Compile body statements with let-chaining via {@link #compileBodyStatements}.</li>
     *   <li>If {@code expectedReturnType != null}: validate the last statement's type fits
     *       (covariant for {@code Relation<(...)>}, subtype otherwise, {@link Primitive#ANY}
     *       bypasses).</li>
     *   <li>If {@code expectedReturnMult != null}: validate the last statement's
     *       multiplicity fits (lower/upper bound check).</li>
     *   <li>Fail loudly on mismatch — no defaulting, per AGENTS.md rule #4.</li>
     * </ul>
     */
    private com.gs.legend.compiler.typed.TypedSpec compileBodyInContext(
            List<ValueSpecification> body,
            CompilationContext ctx,
            Type expectedReturnType,
            com.gs.legend.model.m3.Multiplicity expectedReturnMult,
            String errorContext) {
        if (body.isEmpty()) {
            throw new PureCompileException(errorContext + ": empty body");
        }

        com.gs.legend.compiler.typed.TypedSpec bodyResult = compileBodyStatements(body, ctx);

        // Return type validation — Relation gets structural compare, everything else is subtype.
        if (expectedReturnType != null
                && bodyResult.type() != null
                && expectedReturnType != Primitive.ANY) {
            if (expectedReturnType instanceof Type.GenericType p
                    && isRelationRawType(p.rawType())
                    && !p.typeArgs().isEmpty()
                    && p.typeArgs().get(0) instanceof Type.RelationTypeVar) {
                Type resolvedDeclared = classifyUserType(expectedReturnType);
                checkRelationSchemaCompatibility(
                        errorContext, "<return>", bodyResult.type(), resolvedDeclared);
            } else if (expectedReturnType instanceof Type.Relation) {
                // Pre-built Type.Relation (e.g. synthetic mapping function
                // declares Relation<{...}>[1]) — schema-compatibility uses
                // the same superset-OK rule as the GenericType form.
                checkRelationSchemaCompatibility(
                        errorContext, "<return>", bodyResult.type(), expectedReturnType);
            } else if (!isSubtype(bodyResult.type(), expectedReturnType)) {
                throw new PureCompileException(
                        errorContext + " declares return type " + expectedReturnType.typeName()
                                + " but body returns " + bodyResult.type().typeName());
            }
        }

        // Multiplicity validation.
        if (expectedReturnMult != null) {
            var actualMult = bodyResult.multiplicity();
            if (!com.gs.legend.model.m3.Multiplicity.fits(actualMult, expectedReturnMult)) {
                throw new PureCompileException(
                        errorContext + ": body multiplicity " + actualMult
                                + " does not fit declared " + expectedReturnMult);
            }
        }

        return bodyResult;
    }

    /**
     * Compiles a list of body statements with let-chaining. The terminal statement's
     * {@link com.gs.legend.compiler.typed.TypedSpec} is the body's result; if there
     * are intermediate let-statements, the whole body is wrapped in a
     * {@link com.gs.legend.compiler.typed.TypedBlock}. Single-statement bodies return
     * the bare terminal node (no block wrapper).
     */
    private com.gs.legend.compiler.typed.TypedSpec compileBodyStatements(
            List<ValueSpecification> stmts, CompilationContext ctx) {
        if (stmts.size() == 1) {
            return compileExpr(stmts.get(0), ctx);
        }

        CompilationContext bodyCtx = ctx;
        List<com.gs.legend.compiler.typed.TypedSpec> typedStmts = new ArrayList<>(stmts.size());
        for (int i = 0; i < stmts.size() - 1; i++) {
            var stmt = stmts.get(i);
            if (stmt instanceof AppliedFunction letAf
                    && simpleName(letAf.function()).equals("letFunction")
                    && letAf.parameters().size() >= 2
                    && letAf.parameters().get(0) instanceof CString(String letName)) {
                ValueSpecification valueExpr = letAf.parameters().get(1);
                com.gs.legend.compiler.typed.TypedSpec typedValue = compileExpr(valueExpr, bodyCtx);
                bodyCtx = bodyCtx.withLetBinding(letName, typedValue);
                typedStmts.add(new com.gs.legend.compiler.typed.TypedLet(
                        letName, typedValue, typedValue.info()));
            } else {
                typedStmts.add(compileExpr(stmt, bodyCtx));
            }
        }
        com.gs.legend.compiler.typed.TypedSpec terminal = compileExpr(stmts.getLast(), bodyCtx);
        typedStmts.add(terminal);
        return new com.gs.legend.compiler.typed.TypedBlock(typedStmts, terminal.info());
    }

    /**
     * {@inheritDoc}
     *
     * <p>Delegates to {@link #compileBodyStatements} so built-in lambda arguments
     * (dispatched through {@code AbstractChecker.compileLambdaArg}) and user-function
     * bodies walk the statement list identically: intermediate {@code letFunction}
     * statements bind into {@code letBindings} and emit {@link com.gs.legend.compiler.typed.TypedLet},
     * multi-statement bodies wrap in {@link com.gs.legend.compiler.typed.TypedBlock},
     * and single-statement bodies return the bare terminal node.
     */
    @Override
    public com.gs.legend.compiler.typed.TypedSpec compileLambdaBody(
            LambdaFunction lambda, CompilationContext ctx) {
        if (lambda.body().isEmpty()) {
            throw new PureCompileException(
                    "Lambda body is empty — every lambda must contain at least one expression");
        }
        return compileBodyStatements(lambda.body(), ctx);
    }

    @Override
    public String freshSymbol(String prefix) {
        return prefix + gensymCounter.getAndIncrement();
    }

    // ========== Bidirectional Lambda Typing ==========

    /**
     * Compiles a lambda argument using expected types from a declared FunctionType.
     * This is bidirectional type checking: the expected parameter types from the
     * function signature are pushed into the lambda params before compiling the body.
     *
     * <p>Same pattern as {@code AbstractChecker.compileLambdaArg} for built-in functions.
     */
    private com.gs.legend.compiler.typed.TypedLambda compileLambdaWithExpectedType(
            LambdaFunction lambda, Type.FunctionType expectedFT, CompilationContext ctx) {
        // 1. Bind lambda params from expected FunctionType. Strict arity:
        //    silent Math.min truncation would mask real call-site bugs.
        if (lambda.parameters().size() != expectedFT.params().size()) {
            throw new PureCompileException(
                    "Lambda has " + lambda.parameters().size()
                            + " parameter(s) but expected function type requires "
                            + expectedFT.params().size());
        }
        CompilationContext lambdaCtx = ctx;
        int paramCount = expectedFT.params().size();
        List<com.gs.legend.compiler.typed.TypedParam> typedParams = new ArrayList<>(paramCount);
        for (int i = 0; i < paramCount; i++) {
            String paramName = lambda.parameters().get(i).name();
            Type paramType = classifyUserType(expectedFT.params().get(i).type());
            lambdaCtx = lambdaCtx.withLambdaParam(paramName, paramType);
            typedParams.add(new com.gs.legend.compiler.typed.TypedParam(
                    paramName, paramType, expectedFT.params().get(i).multiplicity()));
        }

        // 2. Compile body + validate return via the shared body-compile primitive.
        // Only validate Primitive / EnumType returns — other kinds (ClassType, Relation,
        // TypeVar) can appear in native signatures and aren't checkable here without
        // a Bindings accumulator.
        Type expectedReturn = expectedFT.returnType();
        Type validateReturn = (expectedReturn instanceof Primitive || expectedReturn instanceof Type.EnumType)
                ? expectedReturn : null;
        com.gs.legend.compiler.typed.TypedSpec bodyResult = compileBodyInContext(
                lambda.body(), lambdaCtx, validateReturn, null, "Lambda");

        return new com.gs.legend.compiler.typed.TypedLambda(
                typedParams, List.of(bodyResult), bodyResult.info());
    }

    /**
     * Resolves a parsed user-signature {@link Type} to a fully classified compile-time
     * {@link Type}. {@link Type.NameRef} leaves get looked up via {@link ModelContext#findType}
     * (FQN-only — NameResolver guarantees the FQN form before we reach this point).
     * Already-resolved leaves (Primitive, ClassType, EnumType, PrecisionDecimal) are
     * idempotent. The {@code Relation<(...columns...)>} structural shape is classified
     * to {@link Type.Relation}.
     *
     * <p>Type variables (generics) aren't supported in user function signatures — they'd
     * require a unification framework the user-signature compiler doesn't have yet.
     * Native-signature-only variants (SchemaAlgebra, FunctionReference, RelationTypeVar
     * outside a Relation&lt;...&gt;, etc.) also throw.
     */
    private Type classifyUserType(Type parsed) {
        return switch (parsed) {
            case Type.NameRef nr -> Type.resolve(nr.qualifiedName(), modelContext);
            case Primitive p -> p;
            case Type.ClassType c -> c;
            case com.gs.legend.model.m3.LClass lc -> lc;
            case Type.EnumType e -> e;
            case Type.PrecisionDecimal pd -> pd;
            case Type.GenericType p when isRelationRawType(p.rawType())
                    && !p.typeArgs().isEmpty()
                    && p.typeArgs().get(0) instanceof Type.RelationTypeVar rtv -> {
                // Relation<(col1:Type1, col2:Type2)> → Type.Relation(Schema)
                var columns = new java.util.LinkedHashMap<String, Type>();
                for (var col : rtv.columns()) {
                    columns.put(col.name(), classifyUserType(col.type()));
                }
                yield new Type.Relation(Type.Schema.withoutPivot(columns));
            }
            case Type.GenericType p -> throw new PureCompileException(
                    "Generic types other than Relation<(...)> are not supported in user function signatures: " + p);
            case Type.TypeVar tv -> throw new PureCompileException(
                    "Type variables (generics) are not supported in user function signatures: " + tv.name());
            case Type.FunctionType ft -> throw new PureCompileException(
                    "Function types are not supported in user function signatures: " + ft);
            case Type.Relation r -> throw new PureCompileException(
                    "Pre-built Type.Relation should not appear in user-parsed signatures: " + r);
            case Type.Tuple t -> throw new PureCompileException(
                    "Pre-built Type.Tuple should not appear in user-parsed signatures: " + t);
            case Type.SchemaAlgebra sa -> throw new PureCompileException(
                    "Schema algebra is a native-signature construct, not valid in user signatures: " + sa);
            case Type.RelationTypeVar rtv -> throw new PureCompileException(
                    "Bare RelationTypeVar must appear inside a Relation<...>, not as a standalone type: " + rtv);
            case Type.FunctionReference fr -> throw new PureCompileException(
                    "FunctionReference is an expression-level type, not valid in user signatures: " + fr);
        };
    }

    /**
     * Whether the rawType of a parsed generic references the platform {@code Relation} class.
     * After {@link NameResolver} runs, platform-class NameRefs are promoted directly to their
     * {@link com.gs.legend.model.m3.LClass} variant — identity compare suffices.
     */
    private static boolean isRelationRawType(Type rawType) {
        return rawType == com.gs.legend.model.m3.LClass.RELATION;
    }

    /**
     * Thin wrapper: delegates to NewChecker. The to-many fixup now lives inside
     * {@link com.gs.legend.compiler.checkers.NewChecker} — it wraps single-value
     * literals in {@link com.gs.legend.compiler.typed.TypedCollection} when the
     * target property is declared {@code [*]}.
     */
    private com.gs.legend.compiler.typed.TypedNewInstance compileNew(
            AppliedFunction af, CompilationContext ctx) {
        return new com.gs.legend.compiler.checkers.NewChecker(this).check(af, null, ctx);
    }

    // compileRelationAccessor, compileTdsLiteral, compileInstanceLiteral
    // moved to TableReferenceChecker, TdsChecker, NewChecker respectively

    // ========== Extraction Utilities ==========


    static String simpleName(String qualifiedName) {
        return SymbolTable.extractSimpleName(qualifiedName);
    }

    // ========== Other AST Nodes ==========

    private com.gs.legend.compiler.typed.TypedLambda compileLambda(LambdaFunction lf, CompilationContext ctx) {
        // Scope lambda params with their declared types
        CompilationContext lambdaCtx = ctx;
        List<com.gs.legend.compiler.typed.TypedParam> typedParams =
                new ArrayList<>(lf.parameters().size());
        for (var p : lf.parameters()) {
            Type paramType = p.typeName() == null ? null : Type.resolve(p.typeName(), modelContext);
            lambdaCtx = lambdaCtx.withLambdaParam(p.name(), paramType);
            typedParams.add(new com.gs.legend.compiler.typed.TypedParam(
                    p.name(), paramType, p.multiplicity()));
        }
        if (lf.body().isEmpty()) {
            throw new PureCompileException("Unresolved type for lambda");
        }

        // Multi-statement body: process let bindings, compile final expression.
        // Matches legend-pure's M3 model: lambda body IS the statement list.
        // Inference mode — no expected return type (caller will stamp the outer lambda).
        com.gs.legend.compiler.typed.TypedSpec bodyResult =
                compileBodyInContext(lf.body(), lambdaCtx, null, null, "Lambda");
        return new com.gs.legend.compiler.typed.TypedLambda(
                typedParams, List.of(bodyResult), bodyResult.info());
    }

    private com.gs.legend.compiler.typed.TypedSpec compileVariable(Variable v, CompilationContext ctx) {
        // Relation-typed bindings (lambda source with schema): produce a typed var
        // carrying the relation schema.
        Type.Schema varType = ctx.getRelationType(v.name());
        if (varType != null) {
            return new com.gs.legend.compiler.typed.TypedVariable(
                    v.name(),
                    com.gs.legend.compiler.typed.Role.LAMBDA_PARAM,
                    ExpressionType.one(new Type.Relation(varType)));
        }
        // Let binding → emit a TypedVariable with role LET_BINDING carrying
        // the bound value's type. The reference resolves at LOWERING time via
        // the binder's installed bindVar/bindRel (see ControlFlowLowering for
        // TypedBlock/TypedLet). We deliberately do NOT splice the bound HIR
        // into the use site — splicing at type-check time captures any
        // same-named outer bindings inside the RHS and conflates type-checking
        // with optimisation.
        //
        // letBindings is still the right ctx field to consult here (it tracks
        // names introduced by {@link #compileBodyStatements} and the binder
        // checkers below); we just use it to look up the type rather than to
        // inline the value.
        com.gs.legend.compiler.typed.TypedSpec letValue = ctx.getLetBinding(v.name());
        if (letValue != null) {
            return new com.gs.legend.compiler.typed.TypedVariable(
                    v.name(),
                    com.gs.legend.compiler.typed.Role.LET_BINDING,
                    letValue.info());
        }
        // Lambda parameter with a known declared type.
        Type lambdaType = ctx.getLambdaParamType(v.name());
        if (lambdaType != null) {
            return new com.gs.legend.compiler.typed.TypedVariable(
                    v.name(),
                    com.gs.legend.compiler.typed.Role.LAMBDA_PARAM,
                    ExpressionType.one(lambdaType));
        }
        if (ctx.isLambdaParam(v.name())) {
            throw new PureCompileException(
                    "Lambda param '" + v.name() + "' has no resolved type — "
                            + "upstream must infer type from function signature or annotation");
        }
        throw new PureCompileException("Unresolved type for variable: " + v.name());
    }

    /**
     * Extracts a class FQN from a {@link Type} that represents a user class reference —
     * either fully classified ({@link Type.ClassType}) or still lazy ({@link Type.NameRef}).
     * Returns {@code null} for types that aren't class references (primitives, enums,
     * relations, tuples, etc.). Per AGENTS.md §5, NameRef stays lazy until a genuine
     * use site; property access is exactly such a site.
     */
    private static String classFqnFor(Type t) {
        if (t instanceof Type.ClassType(String qn)) return qn;
        if (t instanceof Type.NameRef nr) return nr.qualifiedName();
        return null;
    }

    private com.gs.legend.compiler.typed.TypedSpec compileProperty(
            AppliedProperty ap, CompilationContext ctx) {
        if (ap.parameters().isEmpty()) {
            throw new PureCompileException(
                    "AppliedProperty '" + ap.property() + "' has no receiver");
        }
        ValueSpecification receiver = ap.parameters().get(0);

        // Fast path: direct $var.prop — relation column, tuple column, or class field
        // resolved without materializing the variable first.
        if (receiver instanceof Variable v) {
            // 1. Relation-typed variable: column access.
            Type.Schema relType = ctx.getRelationType(v.name());
            if (relType != null) {
                relType.requireColumn(ap.property());
                Type colType = relType.columns().get(ap.property());
                if (colType != null) {
                    var typedSource = new com.gs.legend.compiler.typed.TypedVariable(
                            v.name(),
                            com.gs.legend.compiler.typed.Role.LAMBDA_PARAM,
                            ExpressionType.one(new Type.Relation(relType)));
                    return new com.gs.legend.compiler.typed.TypedPropertyAccess(
                            typedSource, ap.property(), java.util.Optional.empty(),
                            ExpressionType.one(colType));
                }
            }
            // 2. Lambda param owning a Tuple (row schema): tuple-column access.
            Type paramType = ctx.getLambdaParamType(v.name());
            if (paramType instanceof Type.Tuple t) {
                t.schema().requireColumn(ap.property());
                Type colType = t.schema().columns().get(ap.property());
                if (colType != null) {
                    var typedSource = new com.gs.legend.compiler.typed.TypedVariable(
                            v.name(),
                            com.gs.legend.compiler.typed.Role.LAMBDA_PARAM,
                            ExpressionType.one(paramType));
                    return new com.gs.legend.compiler.typed.TypedPropertyAccess(
                            typedSource, ap.property(), java.util.Optional.empty(),
                            ExpressionType.one(colType));
                }
            }
            // 3. Lambda param owning a user class: field access via modelContext.
            String classFqn = classFqnFor(paramType);
            if (classFqn != null && modelContext != null) {
                var typedSource = new com.gs.legend.compiler.typed.TypedVariable(
                        v.name(),
                        com.gs.legend.compiler.typed.Role.LAMBDA_PARAM,
                        ExpressionType.one(paramType));
                var resolved = resolvePropertyOnClass(classFqn, ap.property(), typedSource);
                if (resolved != null) return resolved;
            }
            // 4. Let-bound variable → compileExpr returns the bound TypedSpec (structural
            // inlining). If that's a TypedNewInstance we drill in via struct-extract.
            com.gs.legend.compiler.typed.TypedSpec vTyped = compileExpr(v, ctx);
            if (vTyped instanceof com.gs.legend.compiler.typed.TypedNewInstance tni) {
                return structExtractFor(tni, ap.property());
            }
        }

        // Otherwise: recurse on the receiver, then dispatch on its shape.
        com.gs.legend.compiler.typed.TypedSpec ownerTyped = compileExpr(receiver, ctx);

        // 1. ^Class instance literal: direct structural extraction.
        if (ownerTyped instanceof com.gs.legend.compiler.typed.TypedNewInstance tni) {
            return structExtractFor(tni, ap.property());
        }

        // 2. Multi-valued receiver: desugar xs.prop → xs->map({x | x.prop}).
        //    Build the AST and re-compile — the recursive compileExpr call produces a
        //    TypedMap with the correct shape.
        if (ownerTyped.isMany() && receiver instanceof AppliedFunction ownerFn) {
            var propVar = new Variable("_prop_x");
            var propAccess = new AppliedProperty(ap.property(), List.of(propVar));
            var lambda = new LambdaFunction(List.of(propVar), propAccess);
            var mapNode = new AppliedFunction("map", List.of(ownerFn, lambda));
            return compileExpr(mapNode, ctx);
        }

        // 3. Tuple result (e.g., nth/lead/lag): direct tuple-column access.
        if (ownerTyped.type() instanceof Type.Tuple rt) {
            Type colType = rt.schema().getColumnType(ap.property());
            if (colType != null) {
                return new com.gs.legend.compiler.typed.TypedPropertyAccess(
                        ownerTyped, ap.property(), java.util.Optional.empty(),
                        ExpressionType.one(colType));
            }
        }

        // 4. Relation result: desugar filter(xs).col → xs->project(~col|$r.col).
        if (ownerTyped.isRelation() && receiver instanceof AppliedFunction ownerFn) {
            var propVar = new Variable("_rel_x");
            var propAccess = new AppliedProperty(ap.property(), List.of(propVar));
            var colSpec = new ColSpec(
                    ap.property(), new LambdaFunction(List.of(propVar), propAccess), null);
            var projectNode = new AppliedFunction("project", List.of(ownerFn, colSpec));
            return compileExpr(projectNode, ctx);
        }

        // 5. Multi-hop association chain: $p.dept.org.name.
        //    Must run BEFORE the struct-extract fallback below: when an
        //    AppliedProperty receiver lands here with a ClassType result
        //    (e.g., $p.dept typed as Dept), this branch resolves the next
        //    hop's property against that class. Falling through to the
        //    generic struct-extract would erase the class type to ANY and
        //    break further hops.
        //
        //    The recursive {@code compileExpr} already produced a flat TPA
        //    for the receiver (its {@code associationPath} carries every
        //    hop walked so far). We simply append this level's property to
        //    that path — reusing the inner source — so the result stays
        //    flat (a single {@code TypedPropertyAccess} rooted at the
        //    underlying variable, never a nested chain of TPAs).
        if (ownerTyped instanceof com.gs.legend.compiler.typed.TypedPropertyAccess inner
                && inner.associationPath().isPresent()) {
            String className = classFqnFor(ownerTyped.type());
            var resolved = resolvePropertyOnClass(className, ap.property(), inner.source());
            if (resolved instanceof com.gs.legend.compiler.typed.TypedPropertyAccess tpa) {
                var path = new ArrayList<>(inner.associationPath().get());
                path.add(ap.property());
                return new com.gs.legend.compiler.typed.TypedPropertyAccess(
                        inner.source(), tpa.property(),
                        java.util.Optional.of(List.copyOf(path)), tpa.info());
            }
            if (resolved != null) return resolved;
        }

        // 6. ClassType result without instance literal (e.g., at(1) returning a single
        //    struct): synthesize a struct-extract on the typed owner.
        if (ownerTyped.type() instanceof Type.ClassType) {
            return new com.gs.legend.compiler.typed.TypedStructExtract(
                    ownerTyped, ap.property(), ExpressionType.one(Primitive.ANY));
        }

        throw new PureCompileException("Unresolved type for property: " + ap.property());
    }

    /**
     * Resolve {@code property} on class {@code classFqn} against {@code typedSource}.
     * Handles both direct class fields and association-injected properties. Returns
     * {@code null} when neither resolves (caller falls through).
     */
    private com.gs.legend.compiler.typed.TypedSpec resolvePropertyOnClass(
            String classFqn, String property,
            com.gs.legend.compiler.typed.TypedSpec typedSource) {
        var classOpt = modelContext.findClass(classFqn);
        if (classOpt.isPresent()) {
            var propOpt = classOpt.get().findProperty(property, modelContext);
            if (propOpt.isPresent()) {
                classPropertyAccesses.computeIfAbsent(classFqn, k -> new HashSet<>()).add(property);
                Type fieldType = propOpt.get().type();
                return new com.gs.legend.compiler.typed.TypedPropertyAccess(
                        typedSource, property,
                        java.util.Optional.of(List.of(property)),
                        ExpressionType.one(fieldType));
            }
        }
        var assocNav = modelContext.findAssociationByProperty(classFqn, property);
        if (assocNav.isPresent()) {
            var nav = assocNav.get();
            associationNavigations.computeIfAbsent(classFqn, k -> new HashSet<>()).add(property);
            // Register the target class as referenced — same shape as direct
            // property access above. Empty entry is innocuous to consumers
            // (MappingResolver uses getOrDefault(., Set.of())) and means Pass-2
            // sees the touched-class closure complete in classPropertyAccesses.
            // Without this, an assoc nav whose target's scalar properties are
            // never accessed (e.g. $p.children->isEmpty()) would miss the
            // target's mapping body.
            classPropertyAccesses.computeIfAbsent(nav.targetClassName(), k -> new HashSet<>());
            Type targetType = new Type.ClassType(nav.targetClassName());
            ExpressionType et = nav.isToMany()
                    ? ExpressionType.many(targetType)
                    : ExpressionType.one(targetType);
            return new com.gs.legend.compiler.typed.TypedPropertyAccess(
                    typedSource, property,
                    java.util.Optional.of(List.of(property)), et);
        }
        return null;
    }

    /**
     * Build a {@link com.gs.legend.compiler.typed.TypedStructExtract} for pulling
     * {@code field} out of a {@link com.gs.legend.compiler.typed.TypedNewInstance}.
     * Field type resolves via modelContext; defaults to {@link Primitive#ANY} when
     * the class isn't loadable or the field isn't declared.
     */
    private com.gs.legend.compiler.typed.TypedStructExtract structExtractFor(
            com.gs.legend.compiler.typed.TypedNewInstance tni, String field) {
        Type fieldType = Primitive.ANY;
        if (modelContext != null) {
            var classOpt = modelContext.findClass(tni.className());
            if (classOpt.isPresent()) {
                var propOpt = classOpt.get().findProperty(field, modelContext);
                if (propOpt.isPresent()) fieldType = propOpt.get().type();
            }
        }
        return new com.gs.legend.compiler.typed.TypedStructExtract(
                tni, field, ExpressionType.one(fieldType));
    }

    private com.gs.legend.compiler.typed.TypedCollection compileCollection(
            PureCollection coll, CompilationContext ctx) {
        List<com.gs.legend.compiler.typed.TypedSpec> typedValues = new ArrayList<>(coll.values().size());
        for (var v : coll.values()) {
            typedValues.add(compileExpr(v, ctx));
        }
        Type elementType = unifyElementTypeFromTyped(typedValues);
        return new com.gs.legend.compiler.typed.TypedCollection(
                typedValues, ExpressionType.many(elementType));
    }

    /**
     * Finds the common supertype over a list of typed children.
     * All numeric → NUMBER, all temporal → DATE, all same ClassType → that ClassType,
     * all ClassTypes with common supertype → LCA ClassType, mixed → ANY.
     */
    private Type unifyElementTypeFromTyped(List<com.gs.legend.compiler.typed.TypedSpec> typedValues) {
        if (typedValues.isEmpty()) return Primitive.ANY;
        var elementTypes = typedValues.stream()
                .map(com.gs.legend.compiler.typed.TypedSpec::type)
                .map(t -> t == null ? Primitive.ANY : t)
                .distinct()
                .toList();
        if (elementTypes.size() == 1) return elementTypes.getFirst();
        if (elementTypes.stream().allMatch(Type::isNumeric)) return Primitive.NUMBER;
        if (elementTypes.stream().allMatch(Type::isTemporal)) return Primitive.DATE;
        if (elementTypes.stream().allMatch(t -> t instanceof Type.ClassType) && modelContext != null) {
            var classTypes = elementTypes.stream()
                    .map(t -> ((Type.ClassType) t).qualifiedName()).toList();
            String current = classTypes.get(0);
            for (int i = 1; i < classTypes.size(); i++) {
                var lcaOpt = modelContext.findLowestCommonAncestor(current, classTypes.get(i));
                if (lcaOpt.isEmpty()) return Primitive.ANY;
                current = lcaOpt.get().qualifiedName();
            }
            return new Type.ClassType(current);
        }
        return Primitive.ANY;
    }

    // ========== Compilation Context ==========

    /**
     * Context for compilation — tracks variable → Type.Schema and
     * variable →
     * Mapping bindings.
     *
     * <p>{@link #mappingTarget} is populated only when compiling the body
     * of a synthetic mapping function — names the class the function
     * materializes, so binding-aware checkers (today: {@code ExtendChecker})
     * can validate per-ColSpec property bindings against the model. Hint,
     * not contract — see {@link MappingTarget}.
     */
    public record CompilationContext(
            Map<String, Type.Schema> relationTypes,
            Map<String, Type> lambdaParams,
            Map<String, com.gs.legend.compiler.typed.TypedSpec> letBindings,
            java.util.Optional<MappingTarget> mappingTarget) {

        public CompilationContext() {
            this(Map.of(), Map.of(), Map.of(), java.util.Optional.empty());
        }

        public CompilationContext withRelationType(String paramName, Type.Schema type) {
            var newTypes = new HashMap<>(relationTypes);
            newTypes.put(paramName, type);
            return new CompilationContext(Map.copyOf(newTypes), lambdaParams, letBindings, mappingTarget);
        }

        public CompilationContext withLambdaParam(String name, Type type) {
            var m = new HashMap<>(lambdaParams);
            m.put(name, type); // type may be null for untyped params (e.g., forAll(e|...))
            return new CompilationContext(relationTypes, Collections.unmodifiableMap(m), letBindings, mappingTarget);
        }

        public CompilationContext withLetBinding(String name, com.gs.legend.compiler.typed.TypedSpec value) {
            var m = new HashMap<>(letBindings);
            m.put(name, value);
            return new CompilationContext(relationTypes, lambdaParams, Map.copyOf(m), mappingTarget);
        }

        /**
         * Returns a context bound to the given materialized class FQN, used
         * by {@link TypeChecker#compileFunction} when entering a synthetic
         * mapping function body. See {@link MappingTarget}.
         */
        public CompilationContext withMappingTarget(String classFqn) {
            return new CompilationContext(relationTypes, lambdaParams, letBindings,
                    java.util.Optional.of(new MappingTarget(classFqn)));
        }

        public boolean isLambdaParam(String name) {
            return lambdaParams.containsKey(name);
        }

        public Type getLambdaParamType(String name) {
            return lambdaParams.get(name);
        }

        public com.gs.legend.compiler.typed.TypedSpec getLetBinding(String name) {
            return letBindings.get(name);
        }

        public Type.Schema getRelationType(String name) {
            return relationTypes.get(name);
        }

    }

    // ========== Literal Type Classification ==========

    /**
     * Classifies a CInteger to the appropriate precision type.
     * INTEGER for values in [-2^31, 2^31-1] (INT32 range).
     * INT64 for values that fit in 64-bit but exceed INT32 range.
     * INT128 for BigInteger values exceeding INT64 range.
     */
    private static Type classifyInteger(CInteger ci) {
        if (ci.value() instanceof java.math.BigInteger) {
            return Primitive.INT128;
        }
        long v = ci.value().longValue();
        if (v > Integer.MAX_VALUE || v < Integer.MIN_VALUE) {
            return Primitive.INT128;
        }
        return Primitive.INTEGER;
    }

    /**
     * Classifies a CDecimal to the appropriate precision type.
     * PrecisionDecimal(18,0) for integer-valued decimals (no fractional part)
     * to preserve DECIMAL type in SQL instead of being coerced to INTEGER.
     * DECIMAL for decimals with fractional parts.
     */
    private static Type classifyDecimal(CDecimal d) {
        java.math.BigDecimal v = d.value();
        int scale = v.scale();
        if (scale <= 0) {
            // Integer-valued decimal (e.g., 17774D) — preserve DECIMAL type
            return new Type.PrecisionDecimal(38, 0);
        }
        // Use actual scale from the literal (e.g., 19.905D → scale=3)
        return new Type.PrecisionDecimal(38, scale);
    }


}
