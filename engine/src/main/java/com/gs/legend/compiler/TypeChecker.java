package com.gs.legend.compiler;

import com.gs.legend.antlr.ValueSpecificationBuilder;
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
import com.gs.legend.compiled.CompiledMapping;
import com.gs.legend.compiled.CompiledProfile;
import com.gs.legend.compiled.CompiledRuntime;
import com.gs.legend.compiled.CompiledService;
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
import com.gs.legend.parser.PureParser;
import com.gs.legend.plan.GenericType;

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
 * <li>GenericType.Relation.Schema propagation — track columns through the
 * pipeline</li>
 * </ul>
 */
public class TypeChecker implements TypeCheckEnv {

    /**
     * Built-in function registry — validates function existence, no more
     * passthrough.
     */
    private static final BuiltinFunctionRegistry builtinRegistry = BuiltinFunctionRegistry.instance();

    private final ModelContext modelContext;
    /** Per-node type info, consumed by PlanGenerator. */
    private final IdentityHashMap<ValueSpecification, TypeInfo> types = new IdentityHashMap<>();
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

    public TypeChecker(ModelContext modelContext) {
        this.modelContext = Objects.requireNonNull(modelContext, "ModelContext must not be null");
    }

    public ModelContext modelContext() {
        return modelContext;
    }

    @Override
    public TypeInfo lookupCompiled(ValueSpecification vs) {
        return types.get(vs);
    }

    @Override
    public void markSourceSpecCompiled(String className) {
        compiledSourceSpecs.add(className);
    }

    /**
     * Top-level compile: returns a {@link CompiledExpression} bundling the typed
     * AST, per-node type side table, and dependency data.
     */
    public CompiledExpression check(ValueSpecification vs) {
        TypeInfo rootInfo = compileExpr(vs, new CompilationContext());

        if (rootInfo == null) {
            throw new PureCompileException(
                    "TypeChecker: no TypeInfo stamped for root " + vs.getClass().getSimpleName());
        }
        if (rootInfo.expressionType() == null) {
            throw new PureCompileException(
                    "TypeChecker: expressionType not stamped for root " + vs.getClass().getSimpleName());
        }

        compileNeededAssociationTargets();
        return new CompiledExpression(
                vs,
                types,
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
            case FunctionDefinition fd    -> compileFunction(fd);
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

    /** Narrow-return overload — same dispatch, more precise return type. */
    public CompiledClass       check(ClassDefinition cd)       { return (CompiledClass)       check((PackageableElement) cd); }
    /** Narrow-return overload. */
    public CompiledMapping     check(MappingDefinition md)     { return (CompiledMapping)     check((PackageableElement) md); }
    /** Narrow-return overload. */
    public CompiledFunction    check(FunctionDefinition fd)    { return (CompiledFunction)    check((PackageableElement) fd); }
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

    private CompiledMapping compileMapping(MappingDefinition md) {
        throw new UnsupportedOperationException(
                "Phase 1b: compileMapping not yet implemented for " + md.qualifiedName());
    }

    private CompiledFunction compileFunction(FunctionDefinition fd) {
        throw new UnsupportedOperationException(
                "Phase 1b: compileFunction not yet implemented for " + fd.qualifiedName());
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
        // Iterate a snapshot — associationNavigations is not modified by compileExpr on source relations
        for (var entry : new ArrayList<>(associationNavigations.entrySet())) {
            String className = entry.getKey();
            // Compile this class's source relation if not already done
            compileSourceSpecIfNeeded(className);
            // Compile each target class's source relation
            for (String propName : entry.getValue()) {
                modelContext.findAssociationByProperty(className, propName).ifPresent(nav -> {
                    compileSourceSpecIfNeeded(nav.targetClassName());
                });
            }
        }
    }

    private void compileSourceSpecIfNeeded(String className) {
        if (compiledSourceSpecs.contains(className)) return;
        modelContext.findMappingExpression(className).ifPresent(mapExpr -> {
            if (mapExpr instanceof ModelContext.MappingExpression.Relational rel) {
                compiledSourceSpecs.add(className);
                compileExpr(rel.sourceSpec(), new CompilationContext());
            }
        });
    }

    /**
     * Internal: compiles a ValueSpecification to a typed result.
     * Called recursively for sub-expressions.
     */
    @Override
    public TypeInfo compileExpr(ValueSpecification vs, CompilationContext ctx) {
        return switch (vs) {
            case AppliedFunction af -> compileFunction(af, ctx);
            case ClassInstance ci -> throw new PureCompileException(
                    "Unexpected ClassInstance '" + ci.type() + "' in compileExpr — should be desugared by parser");
            case LambdaFunction lf -> compileLambda(lf, ctx);
            case Variable v -> compileVariable(v, ctx);
            case AppliedProperty ap -> compileProperty(ap, ctx);
            case PackageableElementPtr pe -> resolvePackageableElement(pe);
            case GenericTypeInstance gti -> scalarTyped(gti, GenericType.Primitive.STRING);
            case PureCollection coll -> compileCollection(coll, ctx);
            // Literals — scalar with known type
            case CInteger i -> scalarTyped(i, classifyInteger(i));
            case CFloat f -> scalarTyped(f, GenericType.Primitive.FLOAT);
            case CDecimal d -> scalarTyped(d, classifyDecimal(d));
            case CString s -> scalarTyped(s, GenericType.Primitive.STRING);
            case CBoolean b -> scalarTyped(b, GenericType.Primitive.BOOLEAN);
            case CDateTime dt -> scalarTyped(dt, GenericType.Primitive.DATE_TIME);
            case CStrictDate sd -> scalarTyped(sd, GenericType.Primitive.STRICT_DATE);
            case CStrictTime st -> scalarTyped(st, GenericType.Primitive.STRICT_TIME);
            case CLatestDate ld -> scalarTyped(ld, GenericType.Primitive.DATE_TIME);
            case CByteArray ba -> scalarTyped(ba, GenericType.Primitive.STRING);
            case EnumValue ev -> scalarTyped(ev, new GenericType.EnumType(ev.fullPath()));
            case UnitInstance ui -> scalarTyped(ui, GenericType.Primitive.FLOAT);
        };
    }

    // ========== PackageableElement Resolution ==========

    /**
     * Resolves a PackageableElementPtr to the correct GenericType by looking up
     * the name in all available registries — analogous to legend-engine's
     * {@code CompileContext.resolvePackageableElement()}.
     *
     * <p>Resolution order:
     * <ol>
     *   <li>Function registries (builtin + user) → {@link GenericType.FunctionReference}</li>
     *   <li>Class registry → {@link GenericType.ClassType}</li>
     *   <li>Mapping registry → {@link GenericType.ClassType} (named element)</li>
     *   <li>Enum registry → {@link GenericType.EnumType}</li>
     *   <li>Unresolved → {@link GenericType.ClassType} with full path
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
            return scalarTyped(pe, new GenericType.FunctionReference(path));
        }
        //    Signature-encoded name (e.g., "eq_Any_1__Any_1__Boolean_1_"):
        //    Pure encodes function signatures as name_Type_mult__Type_mult__RetType_mult_
        //    The double-underscore __ separates parameter groups.
        if (name.contains("__")) {
            int firstUnderscore = name.indexOf('_');
            if (firstUnderscore > 0) {
                String baseName = name.substring(0, firstUnderscore);
                if (builtinRegistry.isRegistered(baseName)) {
                    return scalarTyped(pe, new GenericType.FunctionReference(path));
                }
            }
        }

        // 2. Check model registries — resolve to FQN for consistent downstream keys
        var classOpt = modelContext.findClass(path);
        if (classOpt.isEmpty()) classOpt = modelContext.findClass(name);
        if (classOpt.isPresent()) {
            return scalarTyped(pe, new GenericType.ClassType(classOpt.get().qualifiedName()));
        }
        if (modelContext.findEnum(path).isPresent() || modelContext.findEnum(name).isPresent()) {
            return scalarTyped(pe, new GenericType.EnumType(path));
        }

        // 3. Unresolved — named element reference (runtimes, stores, etc.)
        //    These are valid Pure elements not yet in our registries.
        //    Type as ClassType (a named reference) rather than STRING.
        //    TODO: Add findRuntime/findStore to ModelContext for full resolution.
        return scalarTyped(pe, new GenericType.ClassType(path));
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
            List<com.gs.legend.model.def.FunctionDefinition> candidates, CompilationContext ctx) {
        // 1. Recursion guard
        CompilationContext inlineCtx = ctx.withIncrementedDepth();

        int argCount = af.parameters().size();

        // 2. Arity filter
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

        // 3. Compile args and resolve overload by type.
        // Bidirectional: for function-typed params, push expected types into lambda params.
        var firstMatch = arityMatches.get(0);
        List<TypeInfo> argTypes = new ArrayList<>(argCount);
        for (int i = 0; i < argCount; i++) {
            var arg = af.parameters().get(i);
            var paramDef = firstMatch.parameters().get(i);
            if (paramDef.functionType() != null && arg instanceof LambdaFunction lambda) {
                // Arity check before bidirectional compilation
                int expectedArity = paramDef.functionType().paramTypes().size();
                if (lambda.parameters().size() != expectedArity) {
                    throw new PureCompileException(
                            "Function '" + af.function() + "' parameter '" + paramDef.name()
                                    + "' expects a lambda with " + expectedArity + " param(s), "
                                    + "but got " + lambda.parameters().size());
                }
                argTypes.add(compileLambdaWithExpectedType(lambda, paramDef.functionType(), inlineCtx));
            } else {
                argTypes.add(compileExpr(arg, inlineCtx));
            }
        }
        var funcDef = resolveOverload(af.function(), arityMatches, argTypes);

        // 4. Call-site type check: validate scalar arg types against declared param types.
        // Function-typed params are already validated in step 3 (arity + bidirectional body check).
        for (int i = 0; i < funcDef.parameters().size(); i++) {
            var paramDef = funcDef.parameters().get(i);
            var argInfo = argTypes.get(i);

            if (paramDef.functionType() != null) continue; // already checked in step 3

            // Scalar param: compare compiled arg type vs declared type
            if (argInfo.type() != null) {
                String declaredType = SymbolTable.extractSimpleName(paramDef.type());
                if ("Any".equals(declaredType)) continue;

                // Schema-aware check for Relation<(col:Type)> params
                if (paramDef.parsedType() instanceof PType.Parameterized p
                        && "Relation".equals(p.rawType())
                        && !p.typeArgs().isEmpty()
                        && p.typeArgs().get(0) instanceof PType.RelationTypeVar) {
                    GenericType declaredGeneric = resolvePTypeToGenericType(paramDef.parsedType());
                    checkRelationSchemaCompatibility(
                            af.function(), paramDef.name(), argInfo.type(), declaredGeneric);
                    continue;
                }

                if (!isSubtype(argInfo.type(), GenericType.fromTypeName(declaredType))) {
                    throw new PureCompileException(
                            "Function '" + af.function() + "' parameter '" + paramDef.name()
                                    + "' expects " + declaredType + " but got " + argInfo.type().typeName());
                }
            }
        }

        // 4b. Multiplicity check: actual range must fit within declared range.
        // [1] accepts only [1]; [0..1] accepts [1] or [0..1]; [*] accepts anything.
        for (int i = 0; i < funcDef.parameters().size(); i++) {
            var paramDef = funcDef.parameters().get(i);
            var argInfo = argTypes.get(i);
            if (paramDef.functionType() != null) continue;

            var actualMult = argInfo.expressionType().multiplicity();
            int declLower = paramDef.lowerBound();
            Integer declUpper = paramDef.upperBound(); // null = unbounded

            // actual.lower must be ≥ declared.lower (actual guarantees enough)
            boolean lowerOk = actualMult.lowerBound() >= declLower;
            // actual.upper must be ≤ declared.upper (actual doesn't exceed max)
            boolean upperOk = declUpper == null // declared [*] or [1..*] → no upper limit
                    || (actualMult.upperBound() != null && actualMult.upperBound() <= declUpper);

            if (!lowerOk || !upperOk) {
                throw new PureCompileException(
                        "Function '" + af.function() + "' parameter '" + paramDef.name()
                                + "' expects multiplicity " + paramDef.typeWithMultiplicity()
                                + " but got " + actualMult);
            }
        }

        // 5. Get pre-resolved body
        List<ValueSpecification> bodyStmts = funcDef.resolvedBody();
        if (bodyStmts == null) {
            bodyStmts = PureParser.parseCodeBlock(funcDef.body());
        }

        // 6. Build bindings: paramName → argument AST
        var bindings = new HashMap<String, ValueSpecification>();
        for (int i = 0; i < funcDef.parameters().size(); i++) {
            bindings.put(funcDef.parameters().get(i).name(), af.parameters().get(i));
        }

        // 7. Substitute params in each body statement
        List<ValueSpecification> substituted = bodyStmts.stream()
                .map(stmt -> substituteParams(stmt, bindings))
                .toList();

        // 8. Compile body with let-chaining
        TypeInfo bodyResult = compileBodyStatements(substituted, inlineCtx);
        ValueSpecification resultNode = substituted.getLast();

        // 9. Return type validation
        if (bodyResult.type() != null) {
            String declaredReturn = SymbolTable.extractSimpleName(funcDef.returnType());
            if (!"Any".equals(declaredReturn)) {
                // Schema-aware return check for Relation<(col:Type)> return types (covariant)
                if (funcDef.parsedReturnType() instanceof PType.Parameterized p
                        && "Relation".equals(p.rawType())
                        && !p.typeArgs().isEmpty()
                        && p.typeArgs().get(0) instanceof PType.RelationTypeVar) {
                    GenericType declaredGeneric = resolvePTypeToGenericType(funcDef.parsedReturnType());
                    // Covariant: body's actual schema must be ⊇ declared return schema
                    checkRelationSchemaCompatibility(
                            af.function(), "<return>", bodyResult.type(), declaredGeneric);
                } else if (!isSubtype(bodyResult.type(), GenericType.fromTypeName(declaredReturn))) {
                    throw new PureCompileException(
                            "Function '" + af.function() + "' declares return type " + declaredReturn
                                    + " but body returns " + bodyResult.type().typeName());
                }
            }
        }

        // 10. Stamp TypeInfo with inlinedBody for PlanGenerator
        TypeInfo result = TypeInfo.from(bodyResult).inlinedBody(resultNode).build();
        types.put(af, result);
        return result;
    }

    /**
     * Resolves which overload to use when multiple functions match by arity.
     * If only one match, returns it. If multiple, uses compiled arg types to disambiguate.
     */
    private com.gs.legend.model.def.FunctionDefinition resolveOverload(
            String funcName,
            List<com.gs.legend.model.def.FunctionDefinition> arityMatches,
            List<TypeInfo> argTypes) {
        if (arityMatches.size() == 1) return arityMatches.get(0);

        // Score each candidate by how many arg types match declared param types
        var scored = new ArrayList<Map.Entry<com.gs.legend.model.def.FunctionDefinition, Integer>>();
        for (var candidate : arityMatches) {
            int matches = 0;
            for (int i = 0; i < candidate.parameters().size(); i++) {
                String declaredType = SymbolTable.extractSimpleName(candidate.parameters().get(i).type());
                if ("Any".equals(declaredType)) {
                    matches++; // Any matches everything
                } else if (argTypes.get(i).type() != null
                        && isSubtype(argTypes.get(i).type(), GenericType.fromTypeName(declaredType))) {
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
     * Unified subtype check for all GenericTypes.
     * Primitive hierarchy via Primitive.isSubtypeOf.
     * Class hierarchy via model superclass walk.
     */
    private boolean isSubtype(GenericType actual, GenericType declared) {
        if (actual.typeName().equals(declared.typeName())) return true;
        if (declared == GenericType.Primitive.ANY) return true;
        if (actual.isPrimitive() && declared.isPrimitive()) {
            return actual.asPrimitive().isSubtypeOf(declared.asPrimitive());
        }
        if (actual instanceof GenericType.ClassType ct && modelContext != null) {
            var cls = modelContext.findClass(ct.qualifiedName());
            if (cls.isPresent()) {
                for (String superFqn : cls.get().superClassFqns()) {
                    if (isSubtype(new GenericType.ClassType(superFqn), declared)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Validates that an actual Relation argument matches a declared Relation schema.
     * Checks each declared column: must exist in actual with a compatible type.
     * Superset is OK — actual may have extra columns. Throws with precise diagnostics.
     */
    private void checkRelationSchemaCompatibility(
            String funcName, String paramName, GenericType actualType, GenericType declaredType) {
        if (!(declaredType instanceof GenericType.Relation declaredRel)) {
            throw new PureCompileException(
                    "Function '" + funcName + "' parameter '" + paramName
                            + "': internal error — expected Relation declared type but got " + declaredType.typeName());
        }
        if (!(actualType instanceof GenericType.Relation actualRel)) {
            throw new PureCompileException(
                    "Function '" + funcName + "' parameter '" + paramName
                            + "' expects Relation but got " + actualType.typeName());
        }
        for (var entry : declaredRel.schema().columns().entrySet()) {
            String colName = entry.getKey();
            GenericType declaredColType = entry.getValue();
            GenericType actualColType = actualRel.schema().columns().get(colName);
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
            LambdaFunction lambda, PType.FunctionType expectedFT, CompilationContext ctx) {
        // 1. Bind lambda params from expected FunctionType
        CompilationContext lambdaCtx = ctx;
        int paramCount = Math.min(lambda.parameters().size(), expectedFT.paramTypes().size());
        for (int i = 0; i < paramCount; i++) {
            String paramName = lambda.parameters().get(i).name();
            GenericType paramType = resolvePTypeToGenericType(expectedFT.paramTypes().get(i).type());
            lambdaCtx = lambdaCtx.withLambdaParam(paramName, paramType);
        }

        // 2. Compile body with let-chaining
        if (lambda.body().isEmpty()) {
            throw new PureCompileException("Empty lambda body");
        }
        TypeInfo bodyResult = compileBodyStatements(lambda.body(), lambdaCtx);

        // 3. Validate return type against expected
        if (bodyResult.type() != null && expectedFT.returnType() instanceof PType.Concrete c) {
            GenericType expectedReturn = c.toGenericType();
            if (expectedReturn != null) {
                String expectedName = expectedReturn.typeName();
                if (!"Any".equals(expectedName) && !isSubtype(bodyResult.type(), GenericType.fromTypeName(expectedName))) {
                    throw new PureCompileException(
                            "Lambda return type mismatch: expected " + expectedName
                                    + " but body returns " + bodyResult.type().typeName());
                }
            }
        }

        // Stamp the lambda itself with the body result type
        if (bodyResult.isScalar() && bodyResult.type() != null) {
            return scalarTyped(lambda, bodyResult.type());
        }
        return bodyResult;
    }

    /**
     * Converts a PType (parse-time type) to GenericType (compile-time type).
     * Only concrete types are allowed in user function signatures — type variables
     * (generics) require a unification framework we don't have.
     */
    private GenericType resolvePTypeToGenericType(PType ptype) {
        return switch (ptype) {
            case PType.Concrete c -> {
                GenericType gt = c.toGenericType();
                if (gt != null) yield gt;
                // Non-primitive: try as class name
                yield GenericType.fromTypeName(c.name());
            }
            case PType.Parameterized p when "Relation".equals(p.rawType())
                    && !p.typeArgs().isEmpty()
                    && p.typeArgs().get(0) instanceof PType.RelationTypeVar rtv -> {
                // Relation<(col1:Type1, col2:Type2)> → GenericType.Relation(Schema)
                var columns = new java.util.LinkedHashMap<String, GenericType>();
                for (var col : rtv.columns()) {
                    columns.put(col.name(), resolvePTypeToGenericType(col.type()));
                }
                yield new GenericType.Relation(GenericType.Relation.Schema.withoutPivot(columns));
            }
            case PType.TypeVar tv -> throw new PureCompileException(
                    "Type variables (generics) are not supported in user function signatures: " + tv.name());
            default -> throw new PureCompileException(
                    "Unsupported type in user function signature: " + ptype);
        };
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
                             // UnitInstance, PackageableElementPtr, GenericTypeInstance,
                             // ClassInstance — no Variable children
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
        var ci = (ClassInstance) af.parameters().get(1);
        var data = (ValueSpecificationBuilder.InstanceData) ci.value();
        if (info.type() instanceof GenericType.ClassType(String qn) && modelContext != null) {
            var pureClass = modelContext.findClass(qn).orElse(null);
            if (pureClass != null) {
                for (var entry : data.properties().entrySet()) {
                    var propOpt = pureClass.findProperty(entry.getKey(), modelContext);
                    if (propOpt.isPresent() && propOpt.get().isCollection()
                            && !(entry.getValue() instanceof PureCollection)) {
                        GenericType propType = GenericType.fromTypeRef(propOpt.get().typeRef());
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
            GenericType paramType = null;
            if (p.typeName() != null) {
                try {
                    paramType = GenericType.Primitive.fromTypeName(p.typeName());
                } catch (IllegalArgumentException e) {
                    // Non-primitive type (class, enum) — resolve to FQN for consistent keys
                    String resolvedName = modelContext.findClass(p.typeName())
                            .map(c -> c.qualifiedName()).orElse(p.typeName());
                    paramType = new GenericType.ClassType(resolvedName);
                }
            }
            lambdaCtx = lambdaCtx.withLambdaParam(p.name(), paramType);
        }
        if (lf.body().isEmpty()) {
            throw new PureCompileException("Unresolved type for lambda");
        }

        // Multi-statement body: process let bindings, compile final expression
        // Matches legend-pure's M3 model: lambda body IS the statement list
        TypeInfo bodyInfo = compileBodyStatements(lf.body(), lambdaCtx);
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
        GenericType.Relation.Schema varType = ctx.getRelationType(v.name());
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
        GenericType lambdaType = ctx.getLambdaParamType(v.name());
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

    private TypeInfo compileProperty(AppliedProperty ap, CompilationContext ctx) {
        if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof Variable v) {
            GenericType.Relation.Schema relType = ctx.getRelationType(v.name());
            if (relType != null) {
                relType.requireColumn(ap.property());
                // Resolve the column's type from the RelationType
                GenericType colType = relType.columns().get(ap.property());
                if (colType != null) {
                    return scalarTyped(ap, colType);
                }
            }
            // Lambda param with Tuple: resolve column type from Schema
            // Tuple = T in Relation<T> = row schema type
            GenericType paramType = ctx.getLambdaParamType(v.name());
            if (paramType instanceof GenericType.Tuple t) {
                t.schema().requireColumn(ap.property());
                GenericType colType = t.schema().columns().get(ap.property());
                if (colType != null) {
                    return scalarTyped(ap, colType);
                }
            }
            // Lambda param with ClassType: resolve field type from model context
            if (paramType instanceof GenericType.ClassType(String qualifiedName) && modelContext != null) {
                var classOpt = modelContext.findClass(qualifiedName);
                if (classOpt.isPresent()) {
                    var propOpt = classOpt.get().findProperty(ap.property(), modelContext);
                    if (propOpt.isPresent()) {
                        classPropertyAccesses.computeIfAbsent(qualifiedName, k -> new HashSet<>()).add(ap.property());
                        GenericType fieldType = GenericType.fromTypeRef(propOpt.get().typeRef());
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
                var assocNav = modelContext.findAssociationByProperty(qualifiedName, ap.property());
                if (assocNav.isPresent()) {
                    var nav = assocNav.get();
                    associationNavigations.computeIfAbsent(qualifiedName, k -> new HashSet<>()).add(ap.property());
                    GenericType targetType = new GenericType.ClassType(nav.targetClassName());
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
            // Let-bound variable → resolve inlinedBody to instance ClassInstance
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
            if (ownerInfo != null && ownerInfo.type() instanceof GenericType.Tuple rt) {
                GenericType colType = rt.schema().getColumnType(ap.property());
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
                var colSpecCI = new ClassInstance("colSpec", colSpec);
                var projectNode = new AppliedFunction("project", List.of(ownerFn, colSpecCI));
                TypeInfo projectInfo = compileExpr(projectNode, ctx);
                // Extract the column's GenericType for the return type
                GenericType colType = ownerInfo.schema().getColumnType(ap.property());
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
            if (ownerInfo != null && ownerInfo.type() instanceof GenericType.ClassType) {
                var extractNode = new AppliedFunction("structExtract",
                        List.of(ownerFn, new CString(ap.property())));
                var info = TypeInfo.builder()
                        .inlinedBody(extractNode)
                        .expressionType(ExpressionType.one(GenericType.Primitive.ANY)).build();
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
                // Extract the ClassType — type is now directly Address, not List<Address>
                GenericType ownerType = ownerInfo.type();
                String className = null;
                if (ownerType instanceof GenericType.ClassType(String qn)) {
                    className = qn;
                }
                if (className != null) {
                    var classOpt = modelContext.findClass(className);
                    if (classOpt.isPresent()) {
                        var propOpt = classOpt.get().findProperty(ap.property(), modelContext);
                        if (propOpt.isPresent()) {
                            GenericType fieldType = GenericType.fromTypeRef(propOpt.get().typeRef());
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
                        GenericType targetType = new GenericType.ClassType(nav.targetClassName());
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
        GenericType fieldType = GenericType.Primitive.ANY;
        if (srcInfo != null && srcInfo.type() instanceof GenericType.ClassType(String qn) && modelContext != null) {
            var classOpt = modelContext.findClass(qn);
            if (classOpt.isPresent()) {
                var propOpt = classOpt.get().findProperty(ap.property(), modelContext);
                if (propOpt.isPresent()) {
                    fieldType = GenericType.fromTypeRef(propOpt.get().typeRef());
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
        GenericType elementType = unifyElementType(coll.values());
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
     * Infers GenericType from a ValueSpecification AST node.
     * Used for type unification in collections.
     */
    private GenericType typeOf(ValueSpecification vs) {
        // Check side table first (for compiled sub-expressions)
        TypeInfo info = types.get(vs);
        if (info != null && info.type() != null)
            return info.type();
        // Fall back to pattern matching on literal types
        return switch (vs) {
            case CInteger i -> GenericType.Primitive.INTEGER;
            case CFloat f -> GenericType.Primitive.FLOAT;
            case CDecimal d -> GenericType.DEFAULT_DECIMAL;
            case CString s -> GenericType.Primitive.STRING;
            case CBoolean b -> GenericType.Primitive.BOOLEAN;
            case CDateTime dt -> GenericType.Primitive.DATE_TIME;
            case CStrictDate sd -> GenericType.Primitive.STRICT_DATE;
            case CStrictTime st -> GenericType.Primitive.STRICT_TIME;
            case CLatestDate ld -> GenericType.Primitive.DATE_TIME;
            case PureCollection c -> unifyElementType(c.values());
            default -> GenericType.Primitive.ANY;
        };
    }

    /**
     * Finds the common supertype for a list of expressions.
     * All numeric → NUMBER, all temporal → DATE, all same ClassType → that
     * ClassType,
     * all ClassTypes with common supertype → LCA ClassType, mixed → ANY.
     */
    private GenericType unifyElementType(java.util.List<ValueSpecification> values) {
        if (values.isEmpty())
            return GenericType.Primitive.ANY;
        var elementTypes = values.stream().map(this::typeOf).distinct().toList();
        if (elementTypes.size() == 1)
            return elementTypes.getFirst();
        if (elementTypes.stream().allMatch(GenericType::isNumeric))
            return GenericType.Primitive.NUMBER;
        if (elementTypes.stream().allMatch(GenericType::isTemporal))
            return GenericType.Primitive.DATE;
        // All ClassTypes: try to find common supertype
        if (elementTypes.stream().allMatch(t -> t instanceof GenericType.ClassType) && modelContext != null) {
            var classTypes = elementTypes.stream()
                    .map(t -> ((GenericType.ClassType) t).qualifiedName()).toList();
            // Pairwise LCA reduction
            String current = classTypes.get(0);
            for (int i = 1; i < classTypes.size(); i++) {
                var lcaOpt = modelContext.findLowestCommonAncestor(current, classTypes.get(i));
                if (lcaOpt.isEmpty())
                    return GenericType.Primitive.ANY;
                current = lcaOpt.get().qualifiedName();
            }
            return new GenericType.ClassType(current);
        }
        return GenericType.Primitive.ANY;
    }

    // ========== Compilation Context ==========

    /**
     * Context for compilation — tracks variable → GenericType.Relation.Schema and
     * variable →
     * Mapping bindings.
     */
    public record CompilationContext(
            Map<String, GenericType.Relation.Schema> relationTypes,
            Map<String, GenericType> lambdaParams,
            Map<String, ValueSpecification> letBindings,
            int inlineDepth) {

        private static final int MAX_INLINE_DEPTH = 32;

        public CompilationContext() {
            this(Map.of(), Map.of(), Map.of(), 0);
        }

        public CompilationContext withRelationType(String paramName, GenericType.Relation.Schema type) {
            var newTypes = new HashMap<>(relationTypes);
            newTypes.put(paramName, type);
            return new CompilationContext(Map.copyOf(newTypes), lambdaParams, letBindings, inlineDepth);
        }

        public CompilationContext withLambdaParam(String name, GenericType type) {
            var m = new HashMap<>(lambdaParams);
            m.put(name, type); // type may be null for untyped params (e.g., forAll(e|...))
            return new CompilationContext(relationTypes, Collections.unmodifiableMap(m), letBindings, inlineDepth);
        }

        public CompilationContext withLetBinding(String name, ValueSpecification value) {
            var m = new HashMap<>(letBindings);
            m.put(name, value);
            return new CompilationContext(relationTypes, lambdaParams, Map.copyOf(m), inlineDepth);
        }

        public CompilationContext withIncrementedDepth() {
            if (inlineDepth >= MAX_INLINE_DEPTH) {
                throw new PureCompileException(
                        "Maximum function inline depth (" + MAX_INLINE_DEPTH + ") exceeded. "
                                + "This usually indicates a recursive function, which is not supported.");
            }
            return new CompilationContext(relationTypes, lambdaParams, letBindings, inlineDepth + 1);
        }

        public boolean isLambdaParam(String name) {
            return lambdaParams.containsKey(name);
        }

        public GenericType getLambdaParamType(String name) {
            return lambdaParams.get(name);
        }

        public ValueSpecification getLetBinding(String name) {
            return letBindings.get(name);
        }

        public GenericType.Relation.Schema getRelationType(String name) {
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
    private static GenericType classifyInteger(CInteger ci) {
        if (ci.value() instanceof java.math.BigInteger) {
            return GenericType.Primitive.INT128;
        }
        long v = ci.value().longValue();
        if (v > Integer.MAX_VALUE || v < Integer.MIN_VALUE) {
            return GenericType.Primitive.INT128;
        }
        return GenericType.Primitive.INTEGER;
    }

    /**
     * Classifies a CDecimal to the appropriate precision type.
     * PrecisionDecimal(18,0) for integer-valued decimals (no fractional part)
     * to preserve DECIMAL type in SQL instead of being coerced to INTEGER.
     * DECIMAL for decimals with fractional parts.
     */
    private static GenericType classifyDecimal(CDecimal d) {
        java.math.BigDecimal v = d.value();
        int scale = v.scale();
        if (scale <= 0) {
            // Integer-valued decimal (e.g., 17774D) — preserve DECIMAL type
            return new GenericType.PrecisionDecimal(38, 0);
        }
        // Use actual scale from the literal (e.g., 19.905D → scale=3)
        return new GenericType.PrecisionDecimal(38, scale);
    }


    // ========== Type Registration Utilities ==========

    /** Registers a scalar TypeInfo with a known GenericType (multiplicity ONE). */
    private TypeInfo scalarTyped(ValueSpecification ast, GenericType type) {
        var info = TypeInfo.builder().expressionType(ExpressionType.one(type)).build();
        types.put(ast, info);
        return info;
    }

    /**
     * Registers a scalar TypeInfo with Multiplicity.MANY — produces N independent
     * values.
     */
    private TypeInfo scalarTypedMany(ValueSpecification ast, GenericType type) {
        var info = TypeInfo.builder().expressionType(ExpressionType.many(type)).build();
        types.put(ast, info);
        return info;
    }

    /**
     * Registers a relational TypeInfo in the side table and returns it.
     * All relational type registration in TypeChecker goes through this method.
     */
    private TypeInfo typed(ValueSpecification ast, GenericType.Relation.Schema relationType) {
        var info = TypeInfo.builder()
                .expressionType(ExpressionType.one(new GenericType.Relation(relationType))).build();
        types.put(ast, info);
        return info;
    }
}
