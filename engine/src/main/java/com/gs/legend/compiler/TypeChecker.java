package com.gs.legend.compiler;

import com.gs.legend.antlr.ValueSpecificationBuilder;
import com.gs.legend.ast.*;
import com.gs.legend.model.ModelContext;
import com.gs.legend.model.PureFunctionRegistry;

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
    private static final PureFunctionRegistry functionRegistry = PureFunctionRegistry.withBuiltins();

    private final ModelContext modelContext;
    /** Per-node type info, consumed by PlanGenerator. */
    private final IdentityHashMap<ValueSpecification, TypeInfo> types = new IdentityHashMap<>();

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

    /**
     * Top-level compile: returns a {@link TypeCheckResult} bundling the typed
     * result and per-node side table.
     */
    public TypeCheckResult check(ValueSpecification vs) {
        TypeInfo rootInfo = compileExpr(vs, new CompilationContext());

        if (rootInfo == null) {
            throw new PureCompileException(
                    "TypeChecker: no TypeInfo stamped for root " + vs.getClass().getSimpleName());
        }
        if (rootInfo.expressionType() == null) {
            throw new PureCompileException(
                    "TypeChecker: expressionType not stamped for root " + vs.getClass().getSimpleName());
        }

        return new TypeCheckResult(vs, types);
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
        if (builtinRegistry.isRegistered(name) || functionRegistry.hasFunction(path)
                || functionRegistry.hasFunction(name)) {
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

        // 2. Check model registries
        if (modelContext.findClass(path).isPresent() || modelContext.findClass(name).isPresent()) {
            return scalarTyped(pe, new GenericType.ClassType(path));
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
            // --- Unknown: user function → builtin → error ---
            default -> {
                String qualifiedName = af.function();
                var fn = functionRegistry.getFunction(qualifiedName)
                        .or(() -> functionRegistry.getFunction(funcName));
                if (fn.isPresent())
                    yield inlineUserFunction(af, fn.get(), ctx);
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
     * Inlines a user-defined Pure function by:
     * 1. Substituting $param with argument source text
     * 2. Re-parsing the expanded body into new AST
     * 3. Compiling the inlined AST recursively
     */
    private TypeInfo inlineUserFunction(AppliedFunction af,
            PureFunctionRegistry.FunctionEntry entry, CompilationContext ctx) {
        String body = entry.bodySource();
        List<String> paramNames = entry.paramNames();

        // Substitute parameters with argument source text
        if (af.hasReceiver() && af.sourceText() != null) {
            // Arrow call: $param0 = sourceText, $param1.. = argTexts
            if (!paramNames.isEmpty()) {
                body = body.replace("$" + paramNames.get(0), af.sourceText());
            }
            for (int i = 1; i < paramNames.size() && i <= af.argTexts().size(); i++) {
                String argText = af.argTexts().get(i - 1);
                if (argText != null && !argText.isEmpty()) {
                    body = body.replace("$" + paramNames.get(i), argText);
                }
            }
        } else {
            // Standalone call: all params from argTexts
            for (int i = 0; i < paramNames.size() && i < af.argTexts().size(); i++) {
                String argText = af.argTexts().get(i);
                if (argText != null && !argText.isEmpty()) {
                    body = body.replace("$" + paramNames.get(i), argText);
                }
            }
        }

        // Re-parse inlined body into new AST and compile
        ValueSpecification inlinedNode = PureParser.parseQuery(body);
        TypeInfo bodyResult = compileExpr(inlinedNode, ctx);
        // Store inlined body in TypeInfo — PlanGenerator processes it instead of the
        // original call
        TypeInfo result = TypeInfo.from(bodyResult).inlinedBody(inlinedNode).build();
        types.put(af, result);
        return result;
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
            var pureClass = modelContext.findClass(simpleName(qn)).orElse(null);
            if (pureClass != null) {
                for (var entry : data.properties().entrySet()) {
                    var propOpt = pureClass.findProperty(entry.getKey());
                    if (propOpt.isPresent() && propOpt.get().isCollection()
                            && !(entry.getValue() instanceof PureCollection)) {
                        GenericType propType = GenericType.fromType(propOpt.get().genericType());
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
        return TypeInfo.simpleName(qualifiedName);
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
                    // Non-primitive type (class, enum) — store as ClassType
                    paramType = new GenericType.ClassType(p.typeName());
                }
            }
            lambdaCtx = lambdaCtx.withLambdaParam(p.name(), paramType);
        }
        if (lf.body().isEmpty()) {
            throw new PureCompileException("Unresolved type for lambda");
        }

        // Multi-statement body: process let bindings, compile final expression
        // Matches legend-pure's M3 model: lambda body IS the statement list
        List<ValueSpecification> body = lf.body();
        for (int i = 0; i < body.size() - 1; i++) {
            var stmt = body.get(i);
            if (stmt instanceof AppliedFunction letAf
                    && simpleName(letAf.function()).equals("letFunction")
                    && letAf.parameters().size() >= 2
                    && letAf.parameters().get(0) instanceof CString(String value)) {
                // let x = valueExpr → compile value, store binding in context
                ValueSpecification valueExpr = letAf.parameters().get(1);
                compileExpr(valueExpr, lambdaCtx);
                lambdaCtx = lambdaCtx.withLetBinding(value, valueExpr);
            } else {
                compileExpr(stmt, lambdaCtx);
            }
        }

        // Compile the final (result) expression with enriched context
        TypeInfo bodyInfo = compileExpr(body.getLast(), lambdaCtx);
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
                    var propOpt = classOpt.get().findProperty(ap.property());
                    if (propOpt.isPresent()) {
                        GenericType fieldType = GenericType.fromType(propOpt.get().genericType());
                        return scalarTyped(ap, fieldType);
                    }
                }
                // Association-injected properties (e.g., $p.addresses from Association
                // Person_Address)
                // These are first-class properties in Pure, just stored on the Association
                // rather than the Class.
                var assocNav = modelContext.findAssociationByProperty(qualifiedName, ap.property());
                if (assocNav.isPresent()) {
                    var nav = assocNav.get();
                    GenericType targetType = new GenericType.ClassType(nav.targetClassName());
                    var info = TypeInfo.builder()
                            .expressionType(nav.isToMany()
                                    ? ExpressionType.many(targetType)
                                    : ExpressionType.one(targetType))
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
                    String simpleClassName = simpleName(className);
                    var classOpt = modelContext.findClass(simpleClassName);
                    if (classOpt.isPresent()) {
                        var propOpt = classOpt.get().findProperty(ap.property());
                        if (propOpt.isPresent()) {
                            GenericType fieldType = GenericType.fromType(propOpt.get().genericType());
                            return scalarTyped(ap, fieldType);
                        }
                    }
                    // Association-injected properties (same pattern as first-hop at line 472)
                    var assocNav = modelContext.findAssociationByProperty(simpleClassName, ap.property());
                    if (assocNav.isPresent()) {
                        var nav = assocNav.get();
                        GenericType targetType = new GenericType.ClassType(nav.targetClassName());
                        var info = TypeInfo.builder()
                                .expressionType(nav.isToMany()
                                        ? ExpressionType.many(targetType)
                                        : ExpressionType.one(targetType))
                                .build();
                        types.put(ap, info);
                        return info;
                    }
                }
            }
        }
        throw new PureCompileException("Unresolved type for property: " + ap.property());
    }

    private TypeInfo inlineStructExtract(AppliedProperty ap, ValueSpecification structSource,
            CompilationContext ctx) {
        var extractNode = new AppliedFunction("structExtract",
                List.of(structSource, new CString(ap.property())));
        // Resolve type from model — the struct source has ClassType
        TypeInfo srcInfo = types.get(structSource);
        GenericType fieldType = GenericType.Primitive.ANY;
        if (srcInfo != null && srcInfo.type() instanceof GenericType.ClassType(String qn) && modelContext != null) {
            String className = simpleName(qn);
            var classOpt = modelContext.findClass(className);
            if (classOpt.isPresent()) {
                var propOpt = classOpt.get().findProperty(ap.property());
                if (propOpt.isPresent()) {
                    fieldType = GenericType.fromType(propOpt.get().genericType());
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
            Map<String, ValueSpecification> letBindings) {

        public CompilationContext() {
            this(Map.of(), Map.of(), Map.of());
        }

        public CompilationContext withRelationType(String paramName, GenericType.Relation.Schema type) {
            var newTypes = new HashMap<>(relationTypes);
            newTypes.put(paramName, type);
            return new CompilationContext(Map.copyOf(newTypes), lambdaParams, letBindings);
        }

        public CompilationContext withLambdaParam(String name, GenericType type) {
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
