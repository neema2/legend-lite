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
    /** Classes whose source relations have been compiled (prevents double-compilation in pass 2). */
    private final Set<String> compiledSourceSpecs = new HashSet<>();
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

    public TypeChecker(ModelContext modelContext) {
        this.modelContext = Objects.requireNonNull(modelContext, "ModelContext must not be null");
    }

    public ModelContext modelContext() {
        return modelContext;
    }

    /**
     * Compile a class's normalized sourceSpec (relational or M2M) once, idempotently.
     *
     * <p>Single primitive behind every place that needs "type-check this class's
     * sourceSpec":
     * <ul>
     *   <li>Query path — {@code GetAllChecker} on a class reference.</li>
     *   <li>Query path, pass-2 — association-target fan-out in
     *       {@link #compileNeededAssociationTargets()}.</li>
     *   <li>Build path — {@link #compileMapping(MappingDefinition)} fan-out.</li>
     * </ul>
     *
     * <p>Guarded by {@link #compiledSourceSpecs}: a second call for the same
     * {@code classFqn} within this {@code TypeChecker}'s lifetime is a no-op.
     */
    @Override
    public void compileSourceSpecFor(String classFqn) {
        if (compiledSourceSpecs.contains(classFqn)) return;
        modelContext.findSourceSpec(classFqn).ifPresent(spec -> {
            compiledSourceSpecs.add(classFqn);
            compileExpr(spec, new CompilationContext());
        });
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

        compileNeededAssociationTargets();
        return new CompiledExpression(
                root,
                new CompiledDependencies(classPropertyAccesses, associationNavigations));
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

            // Primitive — handles both relational and M2M, idempotent with the
            // query path's prior source-spec compilation for the same class.
            compileSourceSpecFor(classFqn);

            ValueSpecification spec = modelContext.findSourceSpec(classFqn).orElse(null);

            MappingKind kind = cm.isM2M() ? MappingKind.M2M : MappingKind.RELATIONAL;

            CompiledExpression compiledSourceSpec = spec == null ? null
                    : new CompiledExpression(
                            compileExpr(spec, new CompilationContext()),
                            new CompiledDependencies(classPropertyAccesses, associationNavigations));

            mappedClasses.add(new CompiledMappedClass(
                    classFqn, kind, compiledSourceSpec, cm.sourceName()));
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
                columns.put(col.name(), resolveUserSignatureType(col.type()));
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
        CompilationContext ctx = new CompilationContext();
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

        CompiledExpression body = new CompiledExpression(
                bodyHir,
                new CompiledDependencies(classPropertyAccesses, associationNavigations));

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
     * Pass 2: compile source relations for classes whose associations the query navigates.
     * After pass 1 (compileExpr), {@code associationNavigations} contains every
     * className→propName pair the query touches. For each entry:
     * 1. Compile the source class's source relation (if not already done) — stamps TypeInfo
     *    on its traverse conditions.
     * 2. Compile each target class's source relation — stamps TypeInfo on any join-chain
     *    traverses the target class has (e.g., Firm with a @FirmCountry join chain).
     */
    private void compileNeededAssociationTargets() {
        if (modelContext == null) return;
        // Iterate a snapshot — associationNavigations is not modified by compileExpr on source relations.
        for (var entry : new ArrayList<>(associationNavigations.entrySet())) {
            String className = entry.getKey();
            compileSourceSpecFor(className);
            for (String propName : entry.getValue()) {
                modelContext.findAssociationByProperty(className, propName)
                        .ifPresent(nav -> compileSourceSpecFor(nav.targetClassName()));
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
                    ev.fullPath(), ev.name(), ExpressionType.one(new Type.EnumType(ev.fullPath())));
            // UnitInstance — carries a numeric value with a unit; for now model as
            // a scalar float literal. Refine when the unit subsystem is typed.
            case UnitInstance ui -> new com.gs.legend.compiler.typed.TypedCFloat(
                    java.math.BigDecimal.valueOf(ui.value()),
                    ExpressionType.one(Primitive.FLOAT));
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
    private TypeInfo resolvePackageableElement(PackageableElementPtr pe) {
        String path = pe.fullPath();
        String name = simpleName(path);

        // 1. Check function registries
        //    Direct match first (e.g., "removeDuplicates")
        if (builtinRegistry.isRegistered(name) || !modelContext.findFunction(path).isEmpty()
                || !modelContext.findFunction(name).isEmpty()) {
            return scalarTyped(pe, new Type.FunctionReference(path));
        }
        //    Signature-encoded name (e.g., "eq_Any_1__Any_1__Boolean_1_"):
        //    Pure encodes function signatures as name_Type_mult__Type_mult__RetType_mult_
        //    The double-underscore __ separates parameter groups.
        if (name.contains("__")) {
            int firstUnderscore = name.indexOf('_');
            if (firstUnderscore > 0) {
                String baseName = name.substring(0, firstUnderscore);
                if (builtinRegistry.isRegistered(baseName)) {
                    return scalarTyped(pe, new Type.FunctionReference(path));
                }
            }
        }

        // 2. Check model registries — resolve to FQN for consistent downstream keys
        var classOpt = modelContext.findClass(path);
        if (classOpt.isEmpty()) classOpt = modelContext.findClass(name);
        if (classOpt.isPresent()) {
            return scalarTyped(pe, new Type.ClassType(classOpt.get().qualifiedName()));
        }
        if (modelContext.findEnum(path).isPresent() || modelContext.findEnum(name).isPresent()) {
            return scalarTyped(pe, new Type.EnumType(path));
        }

        // 3. Unresolved — named element reference (runtimes, stores, etc.)
        //    These are valid Pure elements not yet in our registries.
        //    Type as ClassType (a named reference) rather than STRING.
        //    TODO: Add findRuntime/findStore to ModelContext for full resolution.
        return scalarTyped(pe, new Type.ClassType(path));
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
    private TypeInfo compileFunction(AppliedFunction af, CompilationContext ctx) {
        // User function — FQN-first lookup, before built-in switch
        var userFuncs = modelContext.findFunction(af.function());
        if (!userFuncs.isEmpty()) {
            TypeInfo info = inlineUserFunction(af, userFuncs, ctx);
            types.put(af, info);
            return info;
        }

        String funcName = simpleName(af.function());

        // Compile first arg (source) for most functions.
        // eval is excluded: its source is an "applicable" (colSpec, funcRef, lambda),
        // not a value — EvalChecker handles its own source compilation.
        TypeInfo source = !af.parameters().isEmpty() && !"eval".equals(funcName)
                ? compileExpr(af.parameters().get(0), ctx)
                : null;

        TypeInfo info = switch (funcName) {
            // --- Relation Sources ---
            case "getAll" -> new com.gs.legend.compiler.checkers.GetAllChecker(this).check(af, source, ctx);
            case "tableReference" -> new com.gs.legend.compiler.checkers.TableReferenceChecker(this).check(af, source, ctx);
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

        // Common: stamp TypeInfo
        types.put(af, info);
        return info;
    }

    /**
     * Inlines a user-defined Pure function via AST-level parameter substitution.
     *
     * <p>Steps:
     * <ol>
     *   <li>Recursion guard — increment depth, throw if > 32</li>
     *   <li>Overload resolution — filter by arity, then by compiled arg types</li>
     *   <li>Parse + resolve body (pre-resolved at model-build time)</li>
     *   <li>Build bindings: paramName → argument AST node</li>
     *   <li>Substitute params in each statement (capture-avoiding)</li>
     *   <li>Compile body with let-chaining</li>
     *   <li>Stamp TypeInfo with inlinedBody for PlanGenerator</li>
     * </ol>
     */
    private TypeInfo inlineUserFunction(AppliedFunction af,
            List<com.gs.legend.model.m3.PureFunction> candidates, CompilationContext ctx) {
        // Recursion is rejected at ingest (PureModelBuilder.detectCallGraphCycles); any
        // call chain reaching here is part of a DAG, bounded by the user's call-graph depth.
        CompilationContext inlineCtx = ctx;

        int argCount = af.parameters().size();

        // 2. Arity filter.
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

        // 3. Compile args (bidirectional for Function-typed params — push expected
        //    lambda signatures in before walking their bodies).
        var firstMatch = arityMatches.get(0);
        List<TypeInfo> argTypes = new ArrayList<>(argCount);
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
                argTypes.add(compileLambdaWithExpectedType(lambda, ft, inlineCtx));
            } else {
                argTypes.add(compileExpr(arg, inlineCtx));
            }
        }

        // 4. Resolve overload based on compiled arg types.
        var pureFn = resolveOverload(af.function(), arityMatches, argTypes);

        // 5. Ensure the function is compiled. This is the unified pipeline:
        //    check(pureFn) compiles the declared body ONCE per (TypeChecker, PureFunction),
        //    validating signature + return types. Cached in compiledFunctions.
        //    Declaration-level bugs (body doesn't satisfy declared return, etc.) surface here,
        //    independent of the specific call-site arg types.
        CompiledFunction compiled = check(pureFn);

        // 6. Call-site signature validation against the canonical CompiledFunction.parameters.
        validateCallSiteArgTypes(af, compiled, argTypes);
        validateCallSiteArgMultiplicity(af, compiled, argTypes);

        // 7. Specialization — substitute arg ASTs into a body clone.
        var bindings = new HashMap<String, ValueSpecification>();
        for (int i = 0; i < pureFn.parameters().size(); i++) {
            bindings.put(pureFn.parameters().get(i).name(), af.parameters().get(i));
        }
        List<ValueSpecification> substituted = pureFn.body().stream()
                .map(stmt -> substituteParams(stmt, bindings))
                .toList();

        // 8. Re-stamp the specialized body with narrowed (actual) arg types.
        //    No return validation — that already happened in check(pureFn) against declared types;
        //    specialization narrows, so it can only refine, not violate.
        TypeInfo bodyResult = compileBodyInContext(
                substituted, inlineCtx, null, null, "Function '" + af.function() + "' call");
        ValueSpecification resultNode = substituted.getLast();

        // 9. Stamp TypeInfo with inlinedBody for PlanGenerator.
        TypeInfo result = TypeInfo.from(bodyResult).inlinedBody(resultNode).build();
        types.put(af, result);
        return result;
    }

    /** Call-site arg-type validation against the canonical compiled signature. */
    private void validateCallSiteArgTypes(
            AppliedFunction af, CompiledFunction compiled, List<TypeInfo> argTypes) {
        for (int i = 0; i < compiled.parameters().size(); i++) {
            var param = compiled.parameters().get(i);
            var argInfo = argTypes.get(i);
            if (param.type() instanceof Type.FunctionType) continue; // bidirectional already checked

            if (argInfo.type() == null) continue;
            Type declaredType = param.type();
            if (declaredType == Primitive.ANY) continue;

            if (declaredType instanceof Type.GenericType p
                    && isRelationRawType(p.rawType())
                    && !p.typeArgs().isEmpty()
                    && p.typeArgs().get(0) instanceof Type.RelationTypeVar) {
                Type declaredGeneric = resolveUserSignatureType(declaredType);
                checkRelationSchemaCompatibility(
                        af.function(), param.name(), argInfo.type(), declaredGeneric);
                continue;
            }

            if (!isSubtype(argInfo.type(), declaredType)) {
                throw new PureCompileException(
                        "Function '" + af.function() + "' parameter '" + param.name()
                                + "' expects " + declaredType.typeName()
                                + " but got " + argInfo.type().typeName());
            }
        }
    }

    /** Call-site arg-multiplicity validation against the canonical compiled signature. */
    private void validateCallSiteArgMultiplicity(
            AppliedFunction af, CompiledFunction compiled, List<TypeInfo> argTypes) {
        for (int i = 0; i < compiled.parameters().size(); i++) {
            var param = compiled.parameters().get(i);
            var argInfo = argTypes.get(i);
            if (param.type() instanceof Type.FunctionType) continue;

            var actualMult = argInfo.expressionType().multiplicity();
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
            List<TypeInfo> argTypes) {
        if (arityMatches.size() == 1) return arityMatches.get(0);

        // Score each candidate by how many arg types match declared param types
        var scored = new ArrayList<Map.Entry<com.gs.legend.model.m3.PureFunction, Integer>>();
        for (var candidate : arityMatches) {
            int matches = 0;
            for (int i = 0; i < candidate.parameters().size(); i++) {
                Type declaredType = candidate.parameters().get(i).type();
                if (declaredType == Primitive.ANY) {
                    matches++; // Any matches everything
                } else if (argTypes.get(i).type() != null
                        && isSubtype(argTypes.get(i).type(), declaredType)) {
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
                throw new PureCompileException(
                        "Function '" + funcName + "' parameter '" + paramName
                                + "': schema mismatch — column '" + colName + "' expects "
                                + declaredColType.typeName() + " but got " + actualColType.typeName());
            }
        }
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
    private TypeInfo compileBodyInContext(
            List<ValueSpecification> body,
            CompilationContext ctx,
            Type expectedReturnType,
            com.gs.legend.model.m3.Multiplicity expectedReturnMult,
            String errorContext) {
        if (body.isEmpty()) {
            throw new PureCompileException(errorContext + ": empty body");
        }

        TypeInfo bodyResult = compileBodyStatements(body, ctx);

        // Return type validation — Relation gets structural compare, everything else is subtype.
        if (expectedReturnType != null
                && bodyResult.type() != null
                && expectedReturnType != Primitive.ANY) {
            if (expectedReturnType instanceof Type.GenericType p
                    && isRelationRawType(p.rawType())
                    && !p.typeArgs().isEmpty()
                    && p.typeArgs().get(0) instanceof Type.RelationTypeVar) {
                Type resolvedDeclared = resolveUserSignatureType(expectedReturnType);
                checkRelationSchemaCompatibility(
                        errorContext, "<return>", bodyResult.type(), resolvedDeclared);
            } else if (!isSubtype(bodyResult.type(), expectedReturnType)) {
                throw new PureCompileException(
                        errorContext + " declares return type " + expectedReturnType.typeName()
                                + " but body returns " + bodyResult.type().typeName());
            }
        }

        // Multiplicity validation.
        if (expectedReturnMult != null) {
            var actualMult = bodyResult.expressionType().multiplicity();
            int declLower = expectedReturnMult.lowerBound();
            Integer declUpper = expectedReturnMult.upperBound();
            boolean lowerOk = actualMult.lowerBound() >= declLower;
            boolean upperOk = declUpper == null
                    || (actualMult.upperBound() != null && actualMult.upperBound() <= declUpper);
            if (!lowerOk || !upperOk) {
                throw new PureCompileException(
                        errorContext + ": body multiplicity " + actualMult
                                + " does not fit declared " + expectedReturnMult);
            }
        }

        return bodyResult;
    }

    /**
     * Compiles a list of body statements with let-chaining.
     * Intermediate let-statements enrich the context; the final statement is the result.
     * Used by inlineUserFunction, compileLambda, and compileLambdaWithExpectedType.
     */
    private TypeInfo compileBodyStatements(List<ValueSpecification> stmts, CompilationContext ctx) {
        CompilationContext bodyCtx = ctx;
        for (int i = 0; i < stmts.size() - 1; i++) {
            var stmt = stmts.get(i);
            if (stmt instanceof AppliedFunction letAf
                    && simpleName(letAf.function()).equals("letFunction")
                    && letAf.parameters().size() >= 2
                    && letAf.parameters().get(0) instanceof CString(String letName)) {
                ValueSpecification valueExpr = letAf.parameters().get(1);
                compileExpr(valueExpr, bodyCtx);
                bodyCtx = bodyCtx.withLetBinding(letName, valueExpr);
            } else {
                compileExpr(stmt, bodyCtx);
            }
        }
        return compileExpr(stmts.getLast(), bodyCtx);
    }

    // ========== Bidirectional Lambda Typing ==========

    /**
     * Compiles a lambda argument using expected types from a declared FunctionType.
     * This is bidirectional type checking: the expected parameter types from the
     * function signature are pushed into the lambda params before compiling the body.
     *
     * <p>Same pattern as {@code AbstractChecker.compileLambdaArg} for built-in functions.
     */
    private TypeInfo compileLambdaWithExpectedType(
            LambdaFunction lambda, Type.FunctionType expectedFT, CompilationContext ctx) {
        // 1. Bind lambda params from expected FunctionType
        CompilationContext lambdaCtx = ctx;
        int paramCount = Math.min(lambda.parameters().size(), expectedFT.params().size());
        for (int i = 0; i < paramCount; i++) {
            String paramName = lambda.parameters().get(i).name();
            Type paramType = resolveUserSignatureType(expectedFT.params().get(i).type());
            lambdaCtx = lambdaCtx.withLambdaParam(paramName, paramType);
        }

        // 2. Compile body + validate return via the shared body-compile primitive.
        // Only validate Primitive / EnumType returns — other kinds (ClassType, Relation,
        // TypeVar) can appear in native signatures and aren't checkable here without
        // a Bindings accumulator.
        Type expectedReturn = expectedFT.returnType();
        Type validateReturn = (expectedReturn instanceof Primitive || expectedReturn instanceof Type.EnumType)
                ? expectedReturn : null;
        TypeInfo bodyResult = compileBodyInContext(
                lambda.body(), lambdaCtx, validateReturn, null, "Lambda");

        // 3. Stamp the lambda itself with the body result type.
        if (bodyResult.isScalar() && bodyResult.type() != null) {
            return scalarTyped(lambda, bodyResult.type());
        }
        return bodyResult;
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
    private Type resolveUserSignatureType(Type parsed) {
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
                    columns.put(col.name(), resolveUserSignatureType(col.type()));
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

    // ========== AST-level Parameter Substitution ==========

    /**
     * Capture-avoiding substitution: replaces Variable nodes matching binding names
     * with the corresponding AST subtrees. Lambda parameters shadow outer bindings.
     *
     * <p>Only 5 of 17 ValueSpecification subtypes can contain nested Variables:
     * Variable, AppliedFunction, AppliedProperty, LambdaFunction, PureCollection.
     * All others (13 literal/terminal types) are returned as-is.
     */
    private ValueSpecification substituteParams(ValueSpecification node,
            Map<String, ValueSpecification> bindings) {
        return switch (node) {
            case Variable v -> bindings.getOrDefault(v.name(), v);
            case AppliedFunction af -> new AppliedFunction(af.function(),
                    af.parameters().stream().map(p -> substituteParams(p, bindings)).toList(),
                    af.hasReceiver(), af.sourceText(), af.argTexts());
            case AppliedProperty ap -> new AppliedProperty(ap.property(),
                    ap.parameters().stream().map(p -> substituteParams(p, bindings)).toList());
            case LambdaFunction lf -> {
                var inner = new HashMap<>(bindings);
                lf.parameters().forEach(p -> inner.remove(p.name())); // capture-avoiding
                yield new LambdaFunction(lf.parameters(),
                        lf.body().stream().map(e -> substituteParams(e, inner)).toList());
            }
            case PureCollection c -> new PureCollection(
                    c.values().stream().map(v -> substituteParams(v, bindings)).toList());
            default -> node; // CInteger, CFloat, CDecimal, CString, CBoolean, CDateTime,
                             // CStrictDate, CStrictTime, CLatestDate, CByteArray, EnumValue,
                             // UnitInstance, PackageableElementPtr, TypeAnnotation,
                             // ColumnInstance, NewInstance — no Variable children
        };
    }

    /**
     * Thin wrapper: delegates to NewChecker, then applies to-many property override.
     * TODO: Remove override once compiler coerces single→collection for [*] properties (model-driven).
     */
    private TypeInfo compileNew(AppliedFunction af, CompilationContext ctx) {
        TypeInfo info = new com.gs.legend.compiler.checkers.NewChecker(this).check(af, null, ctx);
        types.put(af, info);

        // To-many override: if model says [*] but user wrote a single value,
        // tag the value's TypeInfo as many(propType) so PlanGenerator wraps it in [].
        // This is a workaround — the correct fix is compiler-driven single→collection coercion.
        var data = (NewInstance) af.parameters().get(1);
        if (info.type() instanceof Type.ClassType(String qn) && modelContext != null) {
            var pureClass = modelContext.findClass(qn).orElse(null);
            if (pureClass != null) {
                for (var entry : data.properties().entrySet()) {
                    var propOpt = pureClass.findProperty(entry.getKey(), modelContext);
                    if (propOpt.isPresent() && propOpt.get().isCollection()
                            && !(entry.getValue() instanceof PureCollection)) {
                        Type propType = propOpt.get().type();
                        var valInfo = types.get(entry.getValue());
                        if (valInfo != null) {
                            types.put(entry.getValue(),
                                    TypeInfo.from(valInfo).expressionType(ExpressionType.many(propType)).build());
                        }
                    }
                }
            }
        }

        return info;
    }

    // compileRelationAccessor, compileTdsLiteral, compileInstanceLiteral
    // moved to TableReferenceChecker, TdsChecker, NewChecker respectively

    // ========== Extraction Utilities ==========


    static String simpleName(String qualifiedName) {
        return SymbolTable.extractSimpleName(qualifiedName);
    }

    // ========== Other AST Nodes ==========

    private TypeInfo compileLambda(LambdaFunction lf, CompilationContext ctx) {
        // Scope lambda params with their declared types
        CompilationContext lambdaCtx = ctx;
        for (var p : lf.parameters()) {
            Type paramType = p.typeName() == null ? null : Type.resolve(p.typeName(), modelContext);
            lambdaCtx = lambdaCtx.withLambdaParam(p.name(), paramType);
        }
        if (lf.body().isEmpty()) {
            throw new PureCompileException("Unresolved type for lambda");
        }

        // Multi-statement body: process let bindings, compile final expression.
        // Matches legend-pure's M3 model: lambda body IS the statement list.
        // Inference mode — no expected return type (caller will stamp the outer lambda).
        TypeInfo bodyInfo = compileBodyInContext(lf.body(), lambdaCtx, null, null, "Lambda");
        if (bodyInfo.isScalar() && bodyInfo.type() != null) {
            // Propagate multiplicity from body — if body is MANY (list-producing),
            // the lambda must also be MANY so UNNEST is applied at the root.
            if (bodyInfo.isMany()) {
                return scalarTypedMany(lf, bodyInfo.type());
            }
            return scalarTyped(lf, bodyInfo.type());
        }
        // Mutations (e.g. write()) set relationType for PlanGenerator routing but
        // returnType as scalar (Integer). Propagate that returnType so the root
        // stamping logic doesn't overwrite it with Relation.
        if (bodyInfo.expressionType() != null && !bodyInfo.expressionType().isRelation()) {
            var info = TypeInfo.builder()
                    .expressionType(bodyInfo.expressionType())
                    .build();
            types.put(lf, info);
            return info;
        }
        return typed(lf, bodyInfo.schema());
    }

    private TypeInfo compileVariable(Variable v, CompilationContext ctx) {
        Type.Schema varType = ctx.getRelationType(v.name());
        if (varType != null) {
            return typed(v, varType);
        }
        // Let binding → inline the bound expression via inlinedBody
        // PlanGenerator already handles inlinedBody at both relational and scalar
        // levels
        ValueSpecification letValue = ctx.getLetBinding(v.name());
        if (letValue != null) {
            TypeInfo letInfo = compileExpr(letValue, ctx);
            // Create a TypeInfo with the compiled type info AND inlinedBody pointing to the
            // bound value
            TypeInfo inlined = TypeInfo.from(letInfo).inlinedBody(letValue).build();
            types.put(v, inlined);
            return inlined;
        }
        // Lambda parameter — mark in side table with declared type
        Type lambdaType = ctx.getLambdaParamType(v.name());
        if (lambdaType != null) {
            var info = TypeInfo.builder()
                    .expressionType(ExpressionType.one(lambdaType)).lambdaParam(true).build();
            types.put(v, info);
            return info;
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

    private TypeInfo compileProperty(AppliedProperty ap, CompilationContext ctx) {
        if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof Variable v) {
            Type.Schema relType = ctx.getRelationType(v.name());
            if (relType != null) {
                relType.requireColumn(ap.property());
                // Resolve the column's type from the RelationType
                Type colType = relType.columns().get(ap.property());
                if (colType != null) {
                    return scalarTyped(ap, colType);
                }
            }
            // Lambda param with Tuple: resolve column type from Schema
            // Tuple = T in Relation<T> = row schema type
            Type paramType = ctx.getLambdaParamType(v.name());
            if (paramType instanceof Type.Tuple t) {
                t.schema().requireColumn(ap.property());
                Type colType = t.schema().columns().get(ap.property());
                if (colType != null) {
                    return scalarTyped(ap, colType);
                }
            }
            // Lambda param owning a user class (ClassType or NameRef): resolve field type via
            // modelContext. NameRef stays lazy until this use site per AGENTS.md §5 — property
            // access is precisely where "we genuinely need the class's structure".
            String classFqn = classFqnFor(paramType);
            if (classFqn != null && modelContext != null) {
                var classOpt = modelContext.findClass(classFqn);
                if (classOpt.isPresent()) {
                    var propOpt = classOpt.get().findProperty(ap.property(), modelContext);
                    if (propOpt.isPresent()) {
                        classPropertyAccesses.computeIfAbsent(classFqn, k -> new HashSet<>()).add(ap.property());
                        Type fieldType = propOpt.get().type();
                        var info = TypeInfo.builder()
                                .expressionType(ExpressionType.one(fieldType))
                                .associationPath(List.of(ap.property()))
                                .build();
                        types.put(ap, info);
                        return info;
                    }
                }
                // Association-injected properties (e.g., $p.addresses from Association
                // Person_Address)
                // These are first-class properties in Pure, just stored on the Association
                // rather than the Class.
                var assocNav = modelContext.findAssociationByProperty(classFqn, ap.property());
                if (assocNav.isPresent()) {
                    var nav = assocNav.get();
                    associationNavigations.computeIfAbsent(classFqn, k -> new HashSet<>()).add(ap.property());
                    Type targetType = new Type.ClassType(nav.targetClassName());
                    var info = TypeInfo.builder()
                            .expressionType(nav.isToMany()
                                    ? ExpressionType.many(targetType)
                                    : ExpressionType.one(targetType))
                            .associationPath(List.of(ap.property()))
                            .build();
                    types.put(ap, info);
                    return info;
                }
            }
            // Let-bound variable → resolve inlinedBody to its NewInstance
            TypeInfo vInfo = types.get(v);
            if (vInfo == null) {
                vInfo = compileExpr(v, ctx);
            }
            if (vInfo != null && vInfo.instanceLiteral()) {
                return inlineStructExtract(ap, vInfo.inlinedBody(), ctx);
            }
        }
        // .prop on a function result (includes ^Class instance literals, list-producing fns, etc.)
        if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof AppliedFunction ownerFn) {
            TypeInfo ownerInfo = compileExpr(ownerFn, ctx);
            // ^Class instance literal: ^Person(firstName='John').firstName
            if (ownerInfo.instanceLiteral()) {
                return inlineStructExtract(ap, ownerFn, ctx);
            }
            if (ownerInfo != null && ownerInfo.isMany()) {
                var propVar = new Variable("_prop_x");
                var propAccess = new AppliedProperty(ap.property(), List.of(propVar));
                var lambda = new LambdaFunction(List.of(propVar), propAccess);
                var mapNode = new AppliedFunction("map", List.of(ownerFn, lambda));
                TypeInfo mapInfo = compileExpr(mapNode, ctx);
                // Point original property node → synthetic map via inlinedBody
                var info = TypeInfo.from(mapInfo).inlinedBody(mapNode).build();
                types.put(ap, info);
                return info;
            }
            // .prop on a Tuple (row from offset functions like lead/lag/nth/first):
            // Resolve column type directly — no project() desugaring needed.
            if (ownerInfo != null && ownerInfo.type() instanceof Type.Tuple rt) {
                Type colType = rt.schema().getColumnType(ap.property());
                if (colType != null) {
                    return scalarTyped(ap, colType);
                }
            }
            // .prop on a relational result (e.g., filter(...).legalName)
            // → desugar to single-column project so PlanGenerator builds proper FROM clause
            // Pure return type is the column type as a collection (e.g., String[*])
            if (ownerInfo != null && ownerInfo.isRelational()) {
                var propVar = new Variable("_rel_x");
                var propAccess = new AppliedProperty(ap.property(), List.of(propVar));
                var colSpec = new ColSpec(ap.property(), new LambdaFunction(List.of(propVar), propAccess), null);
                var projectNode = new AppliedFunction("project", List.of(ownerFn, colSpec));
                TypeInfo projectInfo = compileExpr(projectNode, ctx);
                // Extract the column's Type for the return type
                Type colType = ownerInfo.schema().getColumnType(ap.property());
                ExpressionType propExprType = colType != null
                        ? ExpressionType.many(colType) // String[*], Integer[*], etc.
                        : null;
                var info = TypeInfo.from(projectInfo)
                        .inlinedBody(projectNode)
                        .expressionType(propExprType != null ? propExprType : projectInfo.expressionType())
                        .build();
                types.put(ap, info);
                return info;
            }
            // .prop on a ClassType result (e.g., at(1) returning a single struct)
            // → synthesize structExtract(ownerFn, 'prop')
            if (ownerInfo != null && ownerInfo.type() instanceof Type.ClassType) {
                var extractNode = new AppliedFunction("structExtract",
                        List.of(ownerFn, new CString(ap.property())));
                var info = TypeInfo.builder()
                        .inlinedBody(extractNode)
                        .expressionType(ExpressionType.one(Primitive.ANY)).build();
                types.put(ap, info);
                return info;
            }
        }
        // .prop on an AppliedProperty that returns a ClassType (multi-hop association
        // path)
        // e.g., $p.addresses.city → addresses returns ClassType("Address"), resolve
        // city from Address
        if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof AppliedProperty ownerAp) {
            TypeInfo ownerInfo = compileExpr(ownerAp, ctx);
            if (ownerInfo != null && modelContext != null) {
                // Extract the class FQN (ClassType or NameRef — both carry an FQN; NameRef stays
                // lazy until this use site per AGENTS.md §5).
                String className = classFqnFor(ownerInfo.type());
                if (className != null) {
                    var classOpt = modelContext.findClass(className);
                    if (classOpt.isPresent()) {
                        var propOpt = classOpt.get().findProperty(ap.property(), modelContext);
                        if (propOpt.isPresent()) {
                            Type fieldType = propOpt.get().type();
                            List<String> assocPath = collectPropertyChain(ap);
                            var info = TypeInfo.builder()
                                    .expressionType(ExpressionType.one(fieldType))
                                    .associationPath(assocPath)
                                    .build();
                            types.put(ap, info);
                            return info;
                        }
                    }
                    // Association-injected properties (same pattern as first-hop at line 472)
                    var assocNav = modelContext.findAssociationByProperty(className, ap.property());
                    if (assocNav.isPresent()) {
                        var nav = assocNav.get();
                        associationNavigations.computeIfAbsent(className, k -> new HashSet<>()).add(ap.property());
                        Type targetType = new Type.ClassType(nav.targetClassName());
                        List<String> assocPath = collectPropertyChain(ap);
                        var info = TypeInfo.builder()
                                .expressionType(nav.isToMany()
                                        ? ExpressionType.many(targetType)
                                        : ExpressionType.one(targetType))
                                .associationPath(assocPath)
                                .build();
                        types.put(ap, info);
                        return info;
                    }
                }
            }
        }
        throw new PureCompileException("Unresolved type for property: " + ap.property());
    }

    /**
     * Collects the full property chain from a multi-hop AppliedProperty.
     * E.g., $e.firm.legalName → ["firm", "legalName"].
     */
    private static List<String> collectPropertyChain(AppliedProperty ap) {
        var path = new java.util.ArrayList<String>();
        path.add(ap.property());
        ValueSpecification current = ap.parameters().isEmpty() ? null : ap.parameters().get(0);
        while (current instanceof AppliedProperty ownerAp) {
            path.addFirst(ownerAp.property());
            current = ownerAp.parameters().isEmpty() ? null : ownerAp.parameters().get(0);
        }
        return List.copyOf(path);
    }

    private TypeInfo inlineStructExtract(AppliedProperty ap, ValueSpecification structSource,
            CompilationContext ctx) {
        var extractNode = new AppliedFunction("structExtract",
                List.of(structSource, new CString(ap.property())));
        // Resolve type from model — the struct source has ClassType
        TypeInfo srcInfo = types.get(structSource);
        Type fieldType = Primitive.ANY;
        if (srcInfo != null && srcInfo.type() instanceof Type.ClassType(String qn) && modelContext != null) {
            var classOpt = modelContext.findClass(qn);
            if (classOpt.isPresent()) {
                var propOpt = classOpt.get().findProperty(ap.property(), modelContext);
                if (propOpt.isPresent()) {
                    fieldType = propOpt.get().type();
                }
            }
        }
        var info = TypeInfo.builder()
                .inlinedBody(extractNode)
                .expressionType(ExpressionType.one(fieldType)).build();
        types.put(ap, info);
        return info;
    }

    private TypeInfo compileCollection(PureCollection coll, CompilationContext ctx) {
        // Compile each element so they're in the side table
        for (var v : coll.values()) {
            compileExpr(v, ctx);
        }
        Type elementType = unifyElementType(coll.values());
        // For struct collections, propagate instanceLiteral from first element
        if (!coll.values().isEmpty()) {
            TypeInfo firstElem = types.get(coll.values().get(0));
            if (firstElem != null && firstElem.instanceLiteral()) {
                var info = TypeInfo.builder()
                        .instanceLiteral(true)
                        .expressionType(ExpressionType.many(elementType))
                        .build();
                types.put(coll, info);
                return info;
            }
        }
        return scalarTypedMany(coll, elementType);
    }

    /**
     * Infers Type from a ValueSpecification AST node.
     * Used for type unification in collections.
     */
    private Type typeOf(ValueSpecification vs) {
        // Check side table first (for compiled sub-expressions)
        TypeInfo info = types.get(vs);
        if (info != null && info.type() != null)
            return info.type();
        // Fall back to pattern matching on literal types
        return switch (vs) {
            case CInteger i -> Primitive.INTEGER;
            case CFloat f -> Primitive.FLOAT;
            case CDecimal d -> Type.DEFAULT_DECIMAL;
            case CString s -> Primitive.STRING;
            case CBoolean b -> Primitive.BOOLEAN;
            case CDateTime dt -> Primitive.DATE_TIME;
            case CStrictDate sd -> Primitive.STRICT_DATE;
            case CStrictTime st -> Primitive.STRICT_TIME;
            case CLatestDate ld -> Primitive.DATE_TIME;
            case PureCollection c -> unifyElementType(c.values());
            default -> Primitive.ANY;
        };
    }

    /**
     * Finds the common supertype for a list of expressions.
     * All numeric → NUMBER, all temporal → DATE, all same ClassType → that
     * ClassType,
     * all ClassTypes with common supertype → LCA ClassType, mixed → ANY.
     */
    private Type unifyElementType(java.util.List<ValueSpecification> values) {
        if (values.isEmpty())
            return Primitive.ANY;
        var elementTypes = values.stream().map(this::typeOf).distinct().toList();
        if (elementTypes.size() == 1)
            return elementTypes.getFirst();
        if (elementTypes.stream().allMatch(Type::isNumeric))
            return Primitive.NUMBER;
        if (elementTypes.stream().allMatch(Type::isTemporal))
            return Primitive.DATE;
        // All ClassTypes: try to find common supertype
        if (elementTypes.stream().allMatch(t -> t instanceof Type.ClassType) && modelContext != null) {
            var classTypes = elementTypes.stream()
                    .map(t -> ((Type.ClassType) t).qualifiedName()).toList();
            // Pairwise LCA reduction
            String current = classTypes.get(0);
            for (int i = 1; i < classTypes.size(); i++) {
                var lcaOpt = modelContext.findLowestCommonAncestor(current, classTypes.get(i));
                if (lcaOpt.isEmpty())
                    return Primitive.ANY;
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
     */
    public record CompilationContext(
            Map<String, Type.Schema> relationTypes,
            Map<String, Type> lambdaParams,
            Map<String, ValueSpecification> letBindings) {

        public CompilationContext() {
            this(Map.of(), Map.of(), Map.of());
        }

        public CompilationContext withRelationType(String paramName, Type.Schema type) {
            var newTypes = new HashMap<>(relationTypes);
            newTypes.put(paramName, type);
            return new CompilationContext(Map.copyOf(newTypes), lambdaParams, letBindings);
        }

        public CompilationContext withLambdaParam(String name, Type type) {
            var m = new HashMap<>(lambdaParams);
            m.put(name, type); // type may be null for untyped params (e.g., forAll(e|...))
            return new CompilationContext(relationTypes, Collections.unmodifiableMap(m), letBindings);
        }

        public CompilationContext withLetBinding(String name, ValueSpecification value) {
            var m = new HashMap<>(letBindings);
            m.put(name, value);
            return new CompilationContext(relationTypes, lambdaParams, Map.copyOf(m));
        }

        public boolean isLambdaParam(String name) {
            return lambdaParams.containsKey(name);
        }

        public Type getLambdaParamType(String name) {
            return lambdaParams.get(name);
        }

        public ValueSpecification getLetBinding(String name) {
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


    // ========== Type Registration Utilities ==========

    /** Registers a scalar TypeInfo with a known Type (multiplicity ONE). */
    private TypeInfo scalarTyped(ValueSpecification ast, Type type) {
        var info = TypeInfo.builder().expressionType(ExpressionType.one(type)).build();
        types.put(ast, info);
        return info;
    }

    /**
     * Registers a scalar TypeInfo with Multiplicity.MANY — produces N independent
     * values.
     */
    private TypeInfo scalarTypedMany(ValueSpecification ast, Type type) {
        var info = TypeInfo.builder().expressionType(ExpressionType.many(type)).build();
        types.put(ast, info);
        return info;
    }

    /**
     * Registers a relational TypeInfo in the side table and returns it.
     * All relational type registration in TypeChecker goes through this method.
     */
    private TypeInfo typed(ValueSpecification ast, Type.Schema relationType) {
        var info = TypeInfo.builder()
                .expressionType(ExpressionType.one(new Type.Relation(relationType))).build();
        types.put(ast, info);
        return info;
    }
}
