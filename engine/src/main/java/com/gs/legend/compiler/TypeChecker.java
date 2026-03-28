package com.gs.legend.compiler;

import com.gs.legend.antlr.ValueSpecificationBuilder;
import com.gs.legend.ast.*;
import com.gs.legend.model.ModelContext;
import com.gs.legend.model.PureFunctionRegistry;

import com.gs.legend.model.mapping.ClassMapping;
import com.gs.legend.model.mapping.MappingRegistry;
import com.gs.legend.model.mapping.RelationalMapping;
import com.gs.legend.model.store.Table;
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
    public java.util.Map<String, TypeInfo.AssociationTarget> resolveAssociations(
            List<ValueSpecification> body, ClassMapping mapping) {
        return resolveAssociationsInBody(body, mapping);
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
            case ClassInstance ci -> compileClassInstance(ci, ctx);
            case LambdaFunction lf -> compileLambda(lf, ctx);
            case Variable v -> compileVariable(v, ctx);
            case AppliedProperty ap -> compileProperty(ap, ctx);
            case PackageableElementPtr pe -> scalarTyped(pe, GenericType.Primitive.STRING);
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

        // Common: compile first arg (source) — harmless for source-less functions
        TypeInfo source = !af.parameters().isEmpty()
                ? compileExpr(af.parameters().get(0), ctx)
                : null;

        TypeInfo info = switch (funcName) {
            // --- Relation Sources ---
            case "getAll" -> new com.gs.legend.compiler.checkers.GetAllChecker(this).check(af, source, ctx);
            // --- Shape-preserving ---
            case "sort", "sortBy", "sortByReversed" ->
                new com.gs.legend.compiler.checkers.SortChecker(this).check(af, source, ctx);
            case "filter" -> new com.gs.legend.compiler.checkers.FilterChecker(this).check(af, source, ctx);
            case "limit", "take", "drop", "slice", "first", "last" ->
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

    // ========== ClassInstance (DSL containers) ==========

    private TypeInfo compileClassInstance(ClassInstance ci, CompilationContext ctx) {
        return switch (ci.type()) {
            case "relation" -> compileRelationAccessor(ci, ctx);
            case "tdsLiteral" -> compileTdsLiteral(ci, ctx);
            case "instance" -> compileInstanceLiteral(ci, ctx);
            default -> throw new PureCompileException("Unknown ClassInstance type: " + ci.type());
        };
    }

    /**
     * Compiles a struct literal ^ClassName(prop=val, ...) with proper type info.
     * Builds a GenericType.Relation.Schema where each property becomes a typed
     * column,
     * so PlanGenerator can distinguish arrays (need UNNEST) from scalars.
     */
    private TypeInfo compileInstanceLiteral(ClassInstance ci, CompilationContext ctx) {
        var data = (ValueSpecificationBuilder.InstanceData) ci.value();
        var columns = new LinkedHashMap<String, GenericType>();

        // Built-in Pure standard library types (no model context needed)
        String simpleName = data.className().contains("::")
                ? data.className().substring(data.className().lastIndexOf("::") + 2)
                : data.className();

        com.gs.legend.model.m3.PureClass pureClass = null;

        if ("Pair".equals(simpleName) && data.typeArguments().size() == 2) {
            // Pair<A, B> → built-in with properties first:A, second:B
            var firstType = GenericType.fromTypeName(data.typeArguments().get(0));
            var secondType = GenericType.fromTypeName(data.typeArguments().get(1));
            pureClass = new com.gs.legend.model.m3.PureClass(
                    data.className().contains("::")
                            ? data.className().substring(0, data.className().lastIndexOf("::"))
                            : "",
                    "Pair", java.util.List.of(
                            new com.gs.legend.model.m3.Property("first",
                                    com.gs.legend.model.m3.PrimitiveType.fromName(firstType.typeName()),
                                    new com.gs.legend.model.m3.Multiplicity(1, 1)),
                            new com.gs.legend.model.m3.Property("second",
                                    com.gs.legend.model.m3.PrimitiveType.fromName(secondType.typeName()),
                                    new com.gs.legend.model.m3.Multiplicity(1, 1))));
        }

        // Fall back to model context for user-defined classes
        if (pureClass == null && modelContext != null) {
            pureClass = modelContext.findClass(data.className()).orElse(null);
        }
        if (pureClass == null) {
            throw new PureCompileException(
                    "Struct literal: class '" + data.className() + "' not found in model context");
        }

        for (var entry : data.properties().entrySet()) {
            String propName = entry.getKey();
            var propOpt = pureClass.findProperty(propName);
            if (propOpt.isEmpty()) {
                throw new PureCompileException(
                        "Struct literal: property '" + propName + "' not found in class '"
                                + data.className() + "'");
            }
            var prop = propOpt.get();
            GenericType propType = GenericType.fromType(prop.genericType());
            // Wrap in List if multiplicity is [*]
            if (prop.isCollection()) {
                propType = GenericType.listOf(propType);
            }
            columns.put(propName, propType);
            // Compile the property value expression so Variables etc. are in the side table
            compileExpr(entry.getValue(), ctx);
            // If property is to-many [*] but value is a single element (not a Collection),
            // tag the value's TypeInfo with list scalarType so PlanGenerator wraps it in
            // [].
            if (prop.isCollection() && !(entry.getValue() instanceof PureCollection)) {
                var valInfo = types.get(entry.getValue());
                if (valInfo != null) {
                    types.put(entry.getValue(),
                            TypeInfo.from(valInfo).expressionType(ExpressionType.one(propType)).build());
                }
            }
        }

        var rt = GenericType.Relation.Schema.withoutPivot(columns);

        // Build identity mapping — scalar primitives get identity PropertyMappings
        var identityMapping = RelationalMapping.identity(pureClass);

        // Synthesize associations for to-many class-typed properties
        // These get AssociationTarget(null, null, true) — no Join signals UNNEST
        var associations = new java.util.LinkedHashMap<String, TypeInfo.AssociationTarget>();
        for (var prop : pureClass.allProperties()) {
            if (prop.isCollection() && prop.genericType() instanceof com.gs.legend.model.m3.PureClass elementClass) {
                // Resolve FULL class from modelContext — property genericType may be a
                // forward-reference stub with no properties
                var resolvedClass = modelContext != null
                        ? modelContext.findClass(elementClass.qualifiedName()).orElse(elementClass)
                        : elementClass;
                // Build identity mapping for element class so compileProject can resolve leaf
                // properties
                var targetMapping = RelationalMapping.identity(resolvedClass);
                associations.put(prop.name(), new TypeInfo.AssociationTarget(targetMapping, null, true));
            }
        }

        var info = TypeInfo.builder()
                .mapping(identityMapping)
                .associations(associations.isEmpty() ? java.util.Map.of() : java.util.Map.copyOf(associations))
                .expressionType(ExpressionType.many(new GenericType.Relation(rt)))
                .build();
        types.put(ci, info);
        return info;
    }

    private TypeInfo compileRelationAccessor(ClassInstance ci, CompilationContext ctx) {
        String tableRef = (String) ci.value();
        Table table = resolveTable(tableRef);
        return typed(ci, tableToRelationType(table), null);
    }

    private TypeInfo compileTdsLiteral(ClassInstance ci, CompilationContext ctx) {
        String raw = (String) ci.value();
        com.gs.legend.ast.TdsLiteral tds = com.gs.legend.ast.TdsLiteral.parse(raw);
        // Build GenericType.Relation.Schema from parsed column names and types
        Map<String, GenericType> columns = new LinkedHashMap<>();
        for (int i = 0; i < tds.columns().size(); i++) {
            var col = tds.columns().get(i);
            GenericType colType = mapTdsColumnType(col.type());
            // If no type annotation, infer from first non-null data value
            if (colType == null && !tds.rows().isEmpty()) {
                for (var row : tds.rows()) {
                    if (i < row.size() && row.get(i) != null) {
                        Object val = row.get(i);
                        if (val instanceof Long)
                            colType = GenericType.Primitive.INTEGER;
                        else if (val instanceof Double)
                            colType = GenericType.Primitive.FLOAT;
                        else if (val instanceof Boolean)
                            colType = GenericType.Primitive.BOOLEAN;
                        else
                            colType = GenericType.Primitive.STRING;
                        break;
                    }
                }
            }
            columns.put(col.name(), colType != null ? colType : GenericType.Primitive.STRING);
        }
        // Register type on the ORIGINAL ci so callers can look it up
        var info = typed(ci, GenericType.Relation.Schema.withoutPivot(columns), null);
        // Also register a parsed-TDS ClassInstance for PlanGenerator
        types.put(new ClassInstance("tdsLiteral", tds), info);
        return info;
    }

    /** Maps a TDS type annotation string to a GenericType. */
    private static GenericType mapTdsColumnType(String typeStr) {
        if (typeStr == null)
            return null;
        return switch (typeStr) {
            case "Integer" -> GenericType.Primitive.INTEGER;
            case "Float", "Number" -> GenericType.Primitive.FLOAT;
            case "Decimal" -> GenericType.DEFAULT_DECIMAL;
            case "Boolean" -> GenericType.Primitive.BOOLEAN;
            case "String" -> GenericType.Primitive.STRING;
            case "Date", "StrictDate" -> GenericType.Primitive.STRICT_DATE;
            case "DateTime" -> GenericType.Primitive.DATE_TIME;
            default -> GenericType.Primitive.STRING;
        };
    }

    // ========== Table Resolution ==========

    /**
     * Resolves a table reference string to a physical Table.
     */
    private Table resolveTable(String tableRef) {
        int dotIdx = tableRef.lastIndexOf('.');
        String simpleDbName = tableRef;
        String tableName = tableRef;

        if (dotIdx > 0) {
            String dbRef = tableRef.substring(0, dotIdx);
            tableName = tableRef.substring(dotIdx + 1);
            simpleDbName = dbRef.contains("::")
                    ? dbRef.substring(dbRef.lastIndexOf("::") + 2)
                    : dbRef;
        }

        String tableKey = simpleDbName + "." + tableName;

        if (modelContext != null) {
            var tableOpt = modelContext.findTable(tableKey);
            if (tableOpt.isPresent())
                return tableOpt.get();
            tableOpt = modelContext.findTable(tableName);
            if (tableOpt.isPresent())
                return tableOpt.get();
        }

        var mappingOpt = mappingRegistry().findByTableName(tableKey);
        if (mappingOpt.isPresent())
            return mappingOpt.get().table();
        mappingOpt = mappingRegistry().findByTableName(tableName);
        if (mappingOpt.isPresent())
            return mappingOpt.get().table();

        throw new PureCompileException("Table not found: " + tableRef);
    }

    /** Converts a physical Table to a RelationType. */
    private GenericType.Relation.Schema tableToRelationType(Table table) {
        Map<String, GenericType> columns = new LinkedHashMap<>();
        for (var col : table.columns()) {
            columns.put(col.name(), col.dataType().toGenericType());
        }
        return GenericType.Relation.Schema.withoutPivot(columns);
    }

    // ========== Extraction Utilities ==========

    static String simpleName(String qualifiedName) {
        return TypeInfo.simpleName(qualifiedName);
    }

    /**
     * Scans a filter lambda body for multi-hop property paths and resolves
     * associations. Called during compileFilter().
     *
     * @param body    The lambda body expressions
     * @param mapping The source mapping
     * @return Map of association property name → pre-resolved target
     */
    private java.util.Map<String, TypeInfo.AssociationTarget> resolveAssociationsInBody(
            List<ValueSpecification> body, ClassMapping mapping) {
        if (modelContext == null || mapping == null) {
            return java.util.Map.of();
        }
        var result = new java.util.LinkedHashMap<String, TypeInfo.AssociationTarget>();
        for (var expr : body) {
            scanForAssociationPaths(expr, mapping, result);
        }
        return result.isEmpty() ? java.util.Map.of() : java.util.Map.copyOf(result);
    }

    /**
     * Recursively scans an expression tree for multi-hop AppliedProperty chains.
     * When found, resolves the association and stores in the result map.
     */
    private void scanForAssociationPaths(ValueSpecification vs, ClassMapping mapping,
            java.util.Map<String, TypeInfo.AssociationTarget> result) {
        if (vs instanceof AppliedProperty ap) {
            // Check if this is a multi-hop chain like $p.items.name
            List<String> chain = extractPropertyChain(ap);
            if (chain.size() > 1) {
                String assocProp = chain.get(0);
                if (!result.containsKey(assocProp)) {
                    resolveAndStore(assocProp, mapping, result);
                }
            }
            // Also scan parameters recursively
            for (var param : ap.parameters()) {
                scanForAssociationPaths(param, mapping, result);
            }
        } else if (vs instanceof AppliedFunction af) {
            // Detect single-hop association properties used with exists/forAll/map:
            // $p.addresses->exists(a|$a.city == 'NY')
            // AST: AppliedFunction("exists", [AppliedProperty("addresses", [$p]), Lambda])
            if (("exists".equals(af.function()) || "forAll".equals(af.function()) || "map".equals(af.function()))
                    && !af.parameters().isEmpty()
                    && af.parameters().get(0) instanceof AppliedProperty assocProp) {
                String propName = assocProp.property();
                if (!result.containsKey(propName)) {
                    resolveAndStore(propName, mapping, result);
                }
            }
            for (var param : af.parameters()) {
                scanForAssociationPaths(param, mapping, result);
            }
        } else if (vs instanceof LambdaFunction lf) {
            for (var expr : lf.body()) {
                scanForAssociationPaths(expr, mapping, result);
            }
        }
    }

    /**
     * Resolves a single association property and stores in the result map.
     */
    private void resolveAndStore(String assocProp, ClassMapping mapping,
            java.util.Map<String, TypeInfo.AssociationTarget> result) {
        String sourceClassName = mapping.targetClass().name();
        var assocNav = modelContext.findAssociationByProperty(sourceClassName, assocProp);
        if (assocNav.isPresent()) {
            var nav = assocNav.get();
            String targetClassName = nav.targetClassName();
            var targetMapping = modelContext.findMapping(targetClassName);
            if (targetMapping.isPresent()) {
                result.put(assocProp, new TypeInfo.AssociationTarget(
                        targetMapping.get(), nav.join(), nav.isToMany()));
            }
        }
    }

    /**
     * Recursively extracts property chain from nested AppliedProperty.
     * e.g., $p.items.name → ["items", "name"]
     */
    private List<String> extractPropertyChain(AppliedProperty ap) {
        if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof AppliedProperty parent) {
            var result = new java.util.ArrayList<>(extractPropertyChain(parent));
            result.add(ap.property());
            return result;
        }
        return new java.util.ArrayList<>(List.of(ap.property()));
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
                    .mapping(bodyInfo.mapping())
                    .expressionType(bodyInfo.expressionType())
                    .build();
            types.put(lf, info);
            return info;
        }
        return typed(lf, bodyInfo.schema(), bodyInfo.mapping());
    }

    private TypeInfo compileVariable(Variable v, CompilationContext ctx) {
        GenericType.Relation.Schema varType = ctx.getRelationType(v.name());
        if (varType != null) {
            return typed(v, varType, null);
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
                    if (nav.isToMany()) {
                        // To-many: return List<TargetClass> — caller handles exists/map desugaring
                        targetType = GenericType.listOf(targetType);
                    }
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
            if (vInfo != null && vInfo.inlinedBody() instanceof ClassInstance ci
                    && "instance".equals(ci.type())) {
                return inlineStructExtract(ap, ci, ctx);
            }
        }
        // .prop directly on instance literal: ^Person(firstName='John').firstName
        if (!ap.parameters().isEmpty()
                && ap.parameters().get(0) instanceof ClassInstance ci
                && "instance".equals(ci.type())) {
            compileExpr(ci, ctx);
            return inlineStructExtract(ap, ci, ctx);
        }
        // .prop on a list-producing function is sugar for ->map(_x | _x.prop)
        // Desugar by building a synthetic map node and pointing via inlinedBody
        if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof AppliedFunction ownerFn) {
            TypeInfo ownerInfo = compileExpr(ownerFn, ctx);
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
                        ? ExpressionType.many(GenericType.listOf(colType)) // String[*], Integer[*], etc.
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
                // Extract the inner ClassType (unwrap List if to-many)
                GenericType ownerType = ownerInfo.type();
                String className = null;
                if (ownerType instanceof GenericType.ClassType(String qn)) {
                    className = qn;
                } else if (ownerType.isList() && ownerType.elementType() instanceof GenericType.ClassType(String qn)) {
                    className = qn;
                }
                if (className != null) {
                    var classOpt = modelContext.findClass(className);
                    if (classOpt.isPresent()) {
                        var propOpt = classOpt.get().findProperty(ap.property());
                        if (propOpt.isPresent()) {
                            GenericType fieldType = GenericType.fromType(propOpt.get().genericType());
                            return scalarTyped(ap, fieldType);
                        }
                    }
                }
            }
        }
        throw new PureCompileException("Unresolved type for property: " + ap.property());
    }

    /**
     * Synthesizes structExtract(instance, 'field') for .property on instance
     * literals.
     * PlanGenerator handles structExtract → STRUCT_EXTRACT(struct, 'field').
     */
    private TypeInfo inlineStructExtract(AppliedProperty ap, ClassInstance ci,
            CompilationContext ctx) {
        var extractNode = new AppliedFunction("structExtract",
                List.of(ci, new CString(ap.property())));
        var info = TypeInfo.builder()
                .inlinedBody(extractNode)
                .expressionType(ExpressionType.one(GenericType.Primitive.ANY)).build();
        types.put(ap, info);
        return info;
    }

    private TypeInfo compileCollection(PureCollection coll, CompilationContext ctx) {
        // Compile each element so they're in the side table
        for (var v : coll.values()) {
            compileExpr(v, ctx);
        }
        GenericType elementType = unifyElementType(coll.values());
        // For struct collections, carry both list type AND struct column info
        // so isList() works (for contains/head/find dispatch) AND project() can resolve
        // columns
        if (!coll.values().isEmpty()) {
            TypeInfo firstElem = types.get(coll.values().get(0));
            if (firstElem != null && firstElem.mapping() != null && firstElem.schema() != null) {
                var info = TypeInfo.builder()
                        .mapping(firstElem.mapping())
                        .associations(firstElem.associations())
                        .expressionType(ExpressionType.many(GenericType.listOf(elementType)))
                        .build();
                types.put(coll, info);
                return info;
            }
        }
        return scalarTypedMany(coll, GenericType.listOf(elementType));
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
            case PureCollection c -> GenericType.listOf(unifyElementType(c.values()));
            case ClassInstance ci when "instance".equals(ci.type())
                    && ci.value() instanceof ValueSpecificationBuilder.InstanceData data ->
                new GenericType.ClassType(data.className());
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
            Map<String, ClassMapping> mappings,
            Map<String, GenericType> lambdaParams,
            Map<String, ValueSpecification> letBindings) {

        public CompilationContext() {
            this(Map.of(), Map.of(), Map.of(), Map.of());
        }

        public CompilationContext withRelationType(String paramName, GenericType.Relation.Schema type) {
            var newTypes = new HashMap<>(relationTypes);
            newTypes.put(paramName, type);
            return new CompilationContext(Map.copyOf(newTypes), mappings, lambdaParams, letBindings);
        }

        public CompilationContext withMapping(String paramName, ClassMapping mapping) {
            var newMappings = new HashMap<>(mappings);
            newMappings.put(paramName, mapping);
            return new CompilationContext(relationTypes, Map.copyOf(newMappings), lambdaParams, letBindings);
        }

        public CompilationContext withLambdaParam(String name, GenericType type) {
            var m = new HashMap<>(lambdaParams);
            m.put(name, type); // type may be null for untyped params (e.g., forAll(e|...))
            return new CompilationContext(relationTypes, mappings, Collections.unmodifiableMap(m), letBindings);
        }

        public CompilationContext withLetBinding(String name, ValueSpecification value) {
            var m = new HashMap<>(letBindings);
            m.put(name, value);
            return new CompilationContext(relationTypes, mappings, lambdaParams, Map.copyOf(m));
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

        public ClassMapping getMapping(String name) {
            return mappings.get(name);
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
        String plain = d.value().toPlainString();
        if (!plain.contains(".")) {
            return new GenericType.PrecisionDecimal(38, 0);
        }
        return GenericType.DEFAULT_DECIMAL;
    }

    /**
     * Convenience accessor — never null (ModelContext guarantees non-null empty
     * registry).
     */
    private MappingRegistry mappingRegistry() {
        return modelContext.getMappingRegistry();
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
    private TypeInfo typed(ValueSpecification ast, GenericType.Relation.Schema relationType,
            ClassMapping mapping) {
        return typed(ast, relationType, mapping, java.util.Map.of());
    }

    /**
     * Registers relational TypeInfo with pre-resolved associations in the side
     * table.
     */
    private TypeInfo typed(ValueSpecification ast, GenericType.Relation.Schema relationType,
            ClassMapping mapping, java.util.Map<String, TypeInfo.AssociationTarget> associations) {
        var info = TypeInfo.builder()
                .mapping(mapping).associations(associations)
                .expressionType(ExpressionType.many(new GenericType.Relation(relationType))).build();
        types.put(ast, info);
        return info;
    }
}
