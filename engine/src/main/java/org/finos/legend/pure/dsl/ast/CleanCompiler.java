package org.finos.legend.pure.dsl.ast;

import org.finos.legend.engine.plan.GenericType;
import org.finos.legend.engine.plan.RelationType;
import org.finos.legend.engine.store.MappingRegistry;
import org.finos.legend.engine.store.RelationalMapping;
import org.finos.legend.engine.store.Table;
import org.finos.legend.pure.dsl.ModelContext;
import org.finos.legend.pure.dsl.PureCompileException;

import java.util.*;

/**
 * Clean compiler for Pure expressions.
 *
 * <p>
 * Takes untyped {@link ValueSpecification} AST (from {@link CleanAstBuilder})
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
 * <li>RelationType propagation — track columns through the pipeline</li>
 * </ul>
 */
public class CleanCompiler {

    private final MappingRegistry mappingRegistry;
    private final ModelContext modelContext;
    /** Per-node type info, consumed by PlanGenerator. */
    private final java.util.IdentityHashMap<ValueSpecification, TypeInfo> types = new java.util.IdentityHashMap<>();

    public CleanCompiler(ModelContext modelContext) {
        this.modelContext = modelContext;
        this.mappingRegistry = modelContext != null ? modelContext.getMappingRegistry() : null;
    }

    /** Returns the per-node type side table. */
    public java.util.IdentityHashMap<ValueSpecification, TypeInfo> types() {
        return types;
    }

    /**
     * Top-level compile: returns a {@link CompilationUnit} bundling the typed
     * result and per-node side table.
     */
    public CompilationUnit compile(ValueSpecification vs) {
        compileExpr(vs, new CompilationContext());
        return new CompilationUnit(vs, types);
    }

    /**
     * Internal: compiles a ValueSpecification to a typed result.
     * Called recursively for sub-expressions.
     */
    TypeInfo compileExpr(ValueSpecification vs, CompilationContext ctx) {
        return switch (vs) {
            case AppliedFunction af -> compileFunction(af, ctx);
            case ClassInstance ci -> compileClassInstance(ci, ctx);
            case LambdaFunction lf -> compileLambda(lf, ctx);
            case Variable v -> compileVariable(v, ctx);
            case AppliedProperty ap -> compileProperty(ap, ctx);
            case PackageableElementPtr pe -> scalar(pe);
            case GenericTypeInstance gti -> scalar(gti);
            case Collection coll -> compileCollection(coll, ctx);
            // Literals — scalar, no relation type
            case CInteger i -> scalar(i);
            case CFloat f -> scalar(f);
            case CDecimal d -> scalar(d);
            case CString s -> scalar(s);
            case CBoolean b -> scalar(b);
            case CDateTime dt -> scalar(dt);
            case CStrictDate sd -> scalar(sd);
            case CStrictTime st -> scalar(st);
            case CLatestDate ld -> scalar(ld);
            case CByteArray ba -> scalar(ba);
            case EnumValue ev -> scalar(ev);
            case UnitInstance ui -> scalar(ui);
        };
    }

    private TypeInfo scalar(ValueSpecification ast) {
        return typed(ast, RelationType.empty(), null);
    }

    /**
     * Registers TypeInfo in the side table and returns it.
     * All type registration in CleanCompiler goes through this method.
     */
    private TypeInfo typed(ValueSpecification ast, RelationType relationType,
            RelationalMapping mapping) {
        return typed(ast, relationType, mapping, java.util.Map.of());
    }

    /**
     * Registers TypeInfo with pre-resolved associations in the side table.
     */
    private TypeInfo typed(ValueSpecification ast, RelationType relationType,
            RelationalMapping mapping, java.util.Map<String, TypeInfo.AssociationTarget> associations) {
        var info = TypeInfo.of(relationType, mapping, associations);
        types.put(ast, info);
        return info;
    }

    // ========== Function Dispatch ==========

    private TypeInfo compileFunction(AppliedFunction af, CompilationContext ctx) {
        String funcName = simpleName(af.function());

        return switch (funcName) {
            // --- Relation Sources ---
            case "table", "class" -> compileTableAccess(af, ctx);
            case "getAll" -> compileGetAll(af, ctx);
            // --- Shape-preserving ---
            case "filter" -> compileFilter(af, ctx);
            case "sort", "sortBy" -> compileSort(af, ctx);
            case "limit", "take" -> compileLimit(af, ctx);
            case "drop" -> compileDrop(af, ctx);
            case "slice" -> compileSlice(af, ctx);
            case "first" -> compileFirst(af, ctx);
            case "distinct" -> compileDistinct(af, ctx);
            // --- Shape-changing ---
            case "rename" -> compileRename(af, ctx);
            case "concatenate" -> compileConcatenate(af, ctx);
            case "project" -> compileProject(af, ctx);
            case "select" -> compileSelect(af, ctx);
            case "groupBy" -> compileGroupBy(af, ctx);
            case "aggregate" -> compileAggregate(af, ctx);
            case "extend" -> compileExtend(af, ctx);
            case "join" -> compileJoin(af, ctx);
            case "asOfJoin" -> compileAsOfJoin(af, ctx);
            case "pivot" -> compilePivot(af, ctx);
            case "from" -> compileFrom(af, ctx);
            // --- Scalar (pass-through — PlanGenerator handles SQL) ---
            default -> compilePassThrough(af, ctx);
        };
    }

    // ========== Relation Sources ==========

    /**
     * Compiles table('store::db.TABLE') or class('model::MyClass').
     * Resolves to physical Table and builds RelationType.
     */
    private TypeInfo compileTableAccess(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.isEmpty()) {
            throw new PureCompileException("table/class() requires an argument");
        }

        String tableRef = extractStringValue(params.get(0));

        // For class('model::Person'), resolve via class name
        if ("class".equals(simpleName(af.function()))) {
            String className = tableRef.contains("::") ? tableRef.substring(tableRef.lastIndexOf("::") + 2) : tableRef;
            if (mappingRegistry != null) {
                var mappingOpt = mappingRegistry.findByClassName(className);
                if (mappingOpt.isPresent()) {
                    RelationalMapping mapping = mappingOpt.get();
                    RelationType relType = tableToRelationType(mapping.table());
                    return typed(af, relType, mapping);
                }
            }
        }

        Table table = resolveTable(tableRef);
        RelationType relType = tableToRelationType(table);
        return typed(af, relType, null);
    }

    /**
     * Compiles Person.all() → getAll(PackageableElementPtr("Person")).
     * Resolves class → mapping → table → RelationType.
     * Stores the mapping in context for property→column resolution in
     * project/filter.
     */
    private TypeInfo compileGetAll(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.isEmpty()) {
            throw new PureCompileException("getAll() requires a class argument");
        }

        String className;
        if (params.get(0) instanceof PackageableElementPtr pe) {
            className = pe.fullPath().contains("::") ? pe.fullPath().substring(pe.fullPath().lastIndexOf("::") + 2)
                    : pe.fullPath();
        } else {
            return scalar(af);
        }

        if (mappingRegistry != null) {
            var mappingOpt = mappingRegistry.findByClassName(className);
            if (mappingOpt.isPresent()) {
                RelationalMapping mapping = mappingOpt.get();
                RelationType relType = tableToRelationType(mapping.table());
                return typed(af, relType, mapping);
            }
        }

        return scalar(af);
    }

    // ========== Shape-Preserving Operations ==========

    /**
     * Compiles filter(source, lambda).
     * Binds lambda param to source's type, validates column refs.
     */
    private TypeInfo compileFilter(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 2) {
            throw new PureCompileException("filter() requires 2 parameters");
        }

        TypeInfo source = compileExpr(params.get(0), ctx);
        RelationType sourceType = source.relationType();

        // Bind lambda parameter to source type
        LambdaFunction lambda = extractLambda(params.get(1));
        CompilationContext lambdaCtx = ctx;
        if (!lambda.parameters().isEmpty()) {
            String paramName = lambda.parameters().get(0).name();
            lambdaCtx = ctx.withRelationType(paramName, sourceType);
            // Propagate mapping for property→column resolution
            if (source.mapping() != null) {
                lambdaCtx = lambdaCtx.withMapping(paramName, source.mapping());
            }
        }

        // Type-check lambda body
        if (!lambda.body().isEmpty()) {
            typeCheckExpression(lambda.body().get(0), lambdaCtx);
        }

        // Pre-resolve associations for multi-hop property paths in the filter lambda
        var associations = resolveAssociationsInBody(
                lambda.body().isEmpty() ? List.of() : lambda.body(),
                source.mapping());

        return typed(af, sourceType, source.mapping(), associations);
    }

    /** Compiles sort(source, sortSpecs). */
    private TypeInfo compileSort(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 2) {
            throw new PureCompileException("sort() requires source and sort specs");
        }

        TypeInfo source = compileExpr(params.get(0), ctx);
        RelationType sourceType = source.relationType();

        // Pre-resolve sort specs into normalized (column, direction) pairs
        List<TypeInfo.SortSpec> sortSpecs = resolveSortSpecs(params.get(1), sourceType);

        var info = new TypeInfo(sourceType, source.mapping(), Map.of(), sortSpecs, List.of(), List.of(), false, null,
                null);
        types.put(af, info);
        return info;
    }

    /**
     * Resolves sort specifications from various AST patterns into normalized
     * SortSpecs.
     * Handles: asc(~col), desc(~col), sortInfo(~col), bare ~col, legacy CString.
     */
    private List<TypeInfo.SortSpec> resolveSortSpecs(ValueSpecification vs, RelationType sourceType) {
        List<TypeInfo.SortSpec> result = new ArrayList<>();
        List<ValueSpecification> specs = (vs instanceof Collection coll) ? coll.values() : List.of(vs);
        for (var spec : specs) {
            result.add(resolveSingleSortSpec(spec, sourceType));
        }
        return result;
    }

    private TypeInfo.SortSpec resolveSingleSortSpec(ValueSpecification vs, RelationType sourceType) {
        if (vs instanceof ClassInstance ci && "sortInfo".equals(ci.type())) {
            if (ci.value() instanceof ColSpec cs) {
                if (!sourceType.columns().isEmpty())
                    sourceType.requireColumn(cs.name());
                return new TypeInfo.SortSpec(cs.name(), TypeInfo.SortDirection.ASC);
            }
        }
        if (vs instanceof AppliedFunction af) {
            String funcName = simpleName(af.function());
            if ("asc".equals(funcName) || "ascending".equals(funcName)
                    || "desc".equals(funcName) || "descending".equals(funcName)) {
                String col = extractColumnName(af.parameters().get(0));
                if (!sourceType.columns().isEmpty())
                    sourceType.requireColumn(col);
                TypeInfo.SortDirection dir = ("asc".equals(funcName) || "ascending".equals(funcName))
                        ? TypeInfo.SortDirection.ASC
                        : TypeInfo.SortDirection.DESC;
                return new TypeInfo.SortSpec(col, dir);
            }
        }
        if (vs instanceof CString s) {
            return new TypeInfo.SortSpec(s.value(), TypeInfo.SortDirection.ASC);
        }
        // Old-style lambda sort: sortBy({e | $e.sal}) or sort({x,y | $y->compare($x)})
        if (vs instanceof LambdaFunction) {
            String col = extractColumnName(vs);
            return new TypeInfo.SortSpec(col, TypeInfo.SortDirection.ASC);
        }
        // Generic fallback: try to extract column name
        return new TypeInfo.SortSpec(extractColumnName(vs), TypeInfo.SortDirection.ASC);
    }

    /** Compiles limit/take(source, count). */
    private TypeInfo compileLimit(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 2) {
            throw new PureCompileException("limit/take() requires source and count");
        }

        TypeInfo source = compileExpr(params.get(0), ctx);
        return typed(af, source.relationType(), source.mapping());
    }

    /** Compiles drop(source, count). */
    private TypeInfo compileDrop(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 2) {
            throw new PureCompileException("drop() requires source and count");
        }

        TypeInfo source = compileExpr(params.get(0), ctx);
        return typed(af, source.relationType(), source.mapping());
    }

    /** Compiles slice(source, start, end). */
    private TypeInfo compileSlice(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 3) {
            throw new PureCompileException("slice() requires source, start, and end");
        }

        TypeInfo source = compileExpr(params.get(0), ctx);
        return typed(af, source.relationType(), source.mapping());
    }

    /** Compiles first(source). */
    private TypeInfo compileFirst(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.isEmpty()) {
            throw new PureCompileException("first() requires a source");
        }
        TypeInfo source = compileExpr(params.get(0), ctx);
        return typed(af, source.relationType(), source.mapping());
    }

    /** Compiles distinct(source, columns?). */
    private TypeInfo compileDistinct(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.isEmpty()) {
            throw new PureCompileException("distinct() requires a source");
        }

        TypeInfo source = compileExpr(params.get(0), ctx);

        if (params.size() > 1) {
            List<String> cols = extractColumnNames(params.get(1));
            return typed(af, source.relationType().onlyColumns(cols), source.mapping());
        }

        return typed(af, source.relationType(), source.mapping());
    }

    // ========== Shape-Changing Operations ==========

    /** Compiles rename(source, ~oldName, ~newName). */
    private TypeInfo compileRename(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 3) {
            throw new PureCompileException("rename() requires source, old name, and new name");
        }

        TypeInfo source = compileExpr(params.get(0), ctx);
        String oldName = extractColumnName(params.get(1));
        String newName = extractColumnName(params.get(2));

        RelationType newType = source.relationType().renameColumn(oldName, newName);
        List<TypeInfo.ColumnSpec> colSpecs = List.of(TypeInfo.ColumnSpec.renamed(oldName, newName));
        var info = new TypeInfo(newType, source.mapping(), Map.of(), List.of(), List.of(), colSpecs, false, null, null);
        types.put(af, info);
        return info;
    }

    /** Compiles concatenate(left, right). */
    private TypeInfo compileConcatenate(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 2) {
            throw new PureCompileException("concatenate() requires two sources");
        }

        TypeInfo left = compileExpr(params.get(0), ctx);
        compileExpr(params.get(1), ctx);

        return typed(af, left.relationType(), left.mapping());
    }

    /**
     * Compiles project(source, projectionSpecs...).
     *
     * <p>
     * Handles two AST patterns:
     * <ol>
     * <li>project(source, [lambdas], ['aliases']) — Collection of lambdas +
     * aliases</li>
     * <li>project(source, lambda1, lambda2, ...) — bare lambdas as params</li>
     * </ol>
     *
     * Resolves property names to column names via mapping.
     */
    private TypeInfo compileProject(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 2) {
            throw new PureCompileException("project() requires source and projection specs");
        }

        TypeInfo source = compileExpr(params.get(0), ctx);
        RelationType sourceType = source.relationType();
        RelationalMapping mapping = source.mapping();

        // Determine projection specs and aliases based on AST pattern
        List<ValueSpecification> lambdaSpecs;
        List<String> aliases;

        if (params.get(1) instanceof Collection coll) {
            // Pattern 1: project(source, [lambdas], ['aliases'])
            lambdaSpecs = coll.values();
            aliases = params.size() > 2 ? extractStringList(params.get(2)) : List.of();
        } else {
            // Pattern 2: project(source, lambda1, lambda2, ...)
            // All params after source are lambda specs, no explicit aliases
            lambdaSpecs = params.subList(1, params.size());
            aliases = List.of();
        }

        // Pre-resolve associations for multi-hop property paths
        var associations = resolveAssociations(lambdaSpecs, mapping);

        // Build projected RelationType + ProjectionSpecs
        Map<String, GenericType> projectedColumns = new LinkedHashMap<>();
        List<TypeInfo.ProjectionSpec> projections = new ArrayList<>();
        for (int i = 0; i < lambdaSpecs.size(); i++) {
            ValueSpecification lambdaSpec = lambdaSpecs.get(i);

            // Extract full property path from lambda
            List<String> propertyPath = extractPropertyPathFromLambda(lambdaSpec);
            String propertyName = propertyPath.isEmpty() ? extractPropertyFromLambda(lambdaSpec)
                    : propertyPath.getLast();
            String alias = i < aliases.size() ? aliases.get(i) : propertyName;

            // Resolve column via mapping
            String resolvedColumn = null;
            GenericType colType = GenericType.Primitive.STRING; // default
            if (mapping != null && propertyName != null) {
                var columnOpt = mapping.getColumnForProperty(propertyName);
                if (columnOpt.isPresent()) {
                    resolvedColumn = columnOpt.get();
                    colType = sourceType.columns().getOrDefault(resolvedColumn, colType);
                }
            } else if (propertyName != null && sourceType.columns().containsKey(propertyName)) {
                resolvedColumn = propertyName;
                colType = sourceType.columns().get(propertyName);
            }

            projectedColumns.put(alias, colType);

            // Use full path if multi-hop, otherwise single property
            List<String> specPath = propertyPath.isEmpty() ? List.of(propertyName) : propertyPath;
            projections.add(new TypeInfo.ProjectionSpec(specPath, resolvedColumn, alias));
        }

        RelationType resultType = new RelationType(projectedColumns);
        // Propagate struct flag from source
        if (source.isStructSource()) {
            var info = new TypeInfo(resultType, null, associations, List.of(), projections, List.of(), true, null,
                    null);
            types.put(af, info);
            return info;
        }
        var info = new TypeInfo(resultType, mapping, associations, List.of(), projections, List.of(), false, null,
                null);
        types.put(af, info);
        return info;
    }

    /**
     * Compiles select(source, ~col1, ~col2, ...).
     * Selects specific columns from a relation by name.
     */
    private TypeInfo compileSelect(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 2) {
            // select() with no columns = select all, pass through
            TypeInfo source = compileExpr(params.get(0), ctx);
            types.put(af, source);
            return source;
        }

        TypeInfo source = compileExpr(params.get(0), ctx);
        List<String> columnNames = new ArrayList<>();
        for (int i = 1; i < params.size(); i++) {
            columnNames.addAll(extractColumnNames(params.get(i)));
        }

        Map<String, GenericType> selectedColumns = new LinkedHashMap<>();
        List<TypeInfo.ColumnSpec> colSpecs = new ArrayList<>();
        for (String col : columnNames) {
            GenericType type = source.relationType().columns().containsKey(col)
                    ? source.relationType().columns().get(col)
                    : GenericType.Primitive.STRING;
            selectedColumns.put(col, type);
            colSpecs.add(TypeInfo.ColumnSpec.col(col));
        }

        var info = new TypeInfo(new RelationType(selectedColumns), source.mapping(),
                Map.of(), List.of(), List.of(), colSpecs, false, null, null);
        types.put(af, info);
        return info;
    }

    /**
     * Compiles groupBy(source, groupCols, aggSpecs).
     * Output RelationType has group columns + aggregate columns.
     */
    private TypeInfo compileGroupBy(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 2) {
            throw new PureCompileException("groupBy() requires source and group specs");
        }

        TypeInfo source = compileExpr(params.get(0), ctx);
        RelationType sourceType = source.relationType();

        // Build output columns: group columns + aggregate columns
        Map<String, GenericType> resultColumns = new LinkedHashMap<>();
        List<TypeInfo.ColumnSpec> colSpecs = new ArrayList<>();

        // Group columns (param 1): ~col or [~col1, ~col2]
        List<String> groupColNames = extractColumnNames(params.get(1));
        for (String col : groupColNames) {
            GenericType type = sourceType.columns().getOrDefault(col, GenericType.Primitive.STRING);
            resultColumns.put(col, type);
            colSpecs.add(TypeInfo.ColumnSpec.col(col));
        }

        // Aggregate columns (params 2+): ~alias:x|$x.prop:y|$y->sum()
        for (int i = 2; i < params.size(); i++) {
            var aggInfo = extractAggSpec(params.get(i));
            if (aggInfo != null) {
                resultColumns.put(aggInfo.alias(), GenericType.Primitive.NUMBER);
                colSpecs.add(aggInfo);
            }
        }

        // Fallback: if no columns resolved, use source type so it's never empty
        if (resultColumns.isEmpty()) {
            return typed(af, sourceType, source.mapping());
        }

        var info = new TypeInfo(new RelationType(resultColumns), source.mapping(),
                Map.of(), List.of(), List.of(), colSpecs, false, null, null);
        types.put(af, info);
        return info;
    }

    /**
     * Compiles aggregate(source, aggSpecs).
     * Full-table aggregation (no group columns).
     */
    private TypeInfo compileAggregate(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.isEmpty()) {
            throw new PureCompileException("aggregate() requires a source");
        }

        TypeInfo source = compileExpr(params.get(0), ctx);

        // Build output columns from aggregate specs
        Map<String, GenericType> resultColumns = new LinkedHashMap<>();
        for (int i = 1; i < params.size(); i++) {
            String aggCol = extractNewColumnName(params.get(i));
            if (aggCol != null) {
                resultColumns.put(aggCol, GenericType.Primitive.NUMBER);
            }
        }

        if (resultColumns.isEmpty()) {
            return typed(af, source.relationType(), source.mapping());
        }

        return typed(af, new RelationType(resultColumns), source.mapping());
    }

    /**
     * Compiles extend(source, ~newCol : lambda).
     * Adds computed column to the source's RelationType.
     * Pre-resolves window function specification into the sidecar.
     */
    private TypeInfo compileExtend(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 2) {
            throw new PureCompileException("extend() requires source and column specs");
        }

        TypeInfo source = compileExpr(params.get(0), ctx);
        RelationType sourceType = source.relationType();

        // Build new RelationType = source columns + new columns
        Map<String, GenericType> newColumns = new LinkedHashMap<>(sourceType.columns());
        for (int i = 1; i < params.size(); i++) {
            String colName = extractNewColumnName(params.get(i));
            if (colName != null) {
                newColumns.put(colName, GenericType.Primitive.STRING);
            }
        }

        // Pre-resolve window function spec from ColSpec + over() params
        AppliedFunction overSpec = null;
        ColSpec windowColSpec = null;
        for (int i = 1; i < params.size(); i++) {
            var p = params.get(i);
            if (p instanceof AppliedFunction paf && "over".equals(simpleName(paf.function()))) {
                overSpec = paf;
            } else if (p instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
                windowColSpec = cs;
            }
        }

        TypeInfo.WindowFunctionSpec windowSpec = null;
        if (windowColSpec != null) {
            // Resolve partition/order/frame from over() if present
            List<String> partitionBy = new ArrayList<>();
            List<TypeInfo.SortSpec> orderBy = new ArrayList<>();
            TypeInfo.FrameSpec frame = null;
            if (overSpec != null) {
                var overResult = resolveOverClause(overSpec);
                partitionBy = overResult.partitionBy;
                orderBy = overResult.orderBy;
                frame = overResult.frame;
            }

            String alias = windowColSpec.name();
            windowSpec = resolveWindowFunc(windowColSpec, partitionBy, orderBy, frame, alias);
        }

        var info = new TypeInfo(new RelationType(newColumns), source.mapping(),
                Map.of(), List.of(), List.of(), List.of(), false, null, windowSpec);
        types.put(af, info);
        return info;
    }

    /**
     * Resolves window function from a ColSpec's function1/function2 lambdas.
     * Stores Pure function names only — no SQL mapping here.
     */
    private TypeInfo.WindowFunctionSpec resolveWindowFunc(ColSpec cs,
            List<String> partitionBy, List<TypeInfo.SortSpec> orderBy, TypeInfo.FrameSpec frame,
            String alias) {

        // Pattern 1: Aggregate window with function2 = aggregate lambda
        // ~alias:x|$x.prop:y|$y->plus()
        if (cs.function2() != null) {
            String column = extractPropertyNameFromLambda(cs.function1());
            String aggFunc = extractPureFuncName(cs.function2());
            if (column != null && aggFunc != null) {
                return TypeInfo.WindowFunctionSpec.aggregate(aggFunc, column, alias,
                        partitionBy, orderBy, frame);
            }
        }

        // Pattern 2: Function in function1 lambda body
        if (cs.function1() != null) {
            var body = cs.function1().body();
            if (!body.isEmpty() && body.get(0) instanceof AppliedFunction af) {
                String funcName = simpleName(af.function());

                // Post-processor wrapping: round(cumulativeDistribution($w,$r), 2)
                if (isWrapperFunc(funcName) && !af.parameters().isEmpty()
                        && af.parameters().get(0) instanceof AppliedFunction innerAf) {
                    String innerFuncName = simpleName(innerAf.function());
                    if (isRankingFunc(innerFuncName)) {
                        List<String> extraArgs = new ArrayList<>();
                        for (int i = 1; i < af.parameters().size(); i++) {
                            extraArgs.add(extractLiteralValue(af.parameters().get(i)));
                        }
                        return TypeInfo.WindowFunctionSpec.wrapped(innerFuncName,
                                funcName, extraArgs, alias, partitionBy, orderBy, frame);
                    }
                }

                // Zero-arg ranking functions: rowNumber, rank, denseRank, etc.
                if (isRankingFunc(funcName)) {
                    return TypeInfo.WindowFunctionSpec.ranking(funcName, alias,
                            partitionBy, orderBy, frame);
                }

                // NTILE
                if ("ntile".equals(funcName)) {
                    int buckets = af.parameters().size() > 1
                            ? (int) extractIntLiteral(af.parameters().get(1))
                            : 1;
                    return TypeInfo.WindowFunctionSpec.ntile(buckets, alias,
                            partitionBy, orderBy, frame);
                }

                // Aggregate/value functions with arguments: sum($w.salary), lag($w.col)
                String sourceCol = af.parameters().size() > 1
                        ? extractColumnName(af.parameters().get(1))
                        : null;
                // Store the Pure function name as-is
                return TypeInfo.WindowFunctionSpec.aggregate(funcName, sourceCol, alias,
                        partitionBy, orderBy, frame);
            }

            // Property access pattern: {p,w,r|$p->avg($w,$r).salary}
            if (!body.isEmpty() && body.get(0) instanceof AppliedProperty ap) {
                String propName = ap.property();
                if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof AppliedFunction innerAf) {
                    String innerFunc = simpleName(innerAf.function());
                    return TypeInfo.WindowFunctionSpec.aggregate(innerFunc, propName, alias,
                            partitionBy, orderBy, frame);
                }
            }
        }

        // Fallback: no window function resolved
        return null;
    }

    /** Resolves partition, order, and frame from over() parameters. */
    private record OverClauseResult(List<String> partitionBy, List<TypeInfo.SortSpec> orderBy,
            TypeInfo.FrameSpec frame) {
    }

    private OverClauseResult resolveOverClause(AppliedFunction overSpec) {
        List<String> partitionBy = new ArrayList<>();
        List<TypeInfo.SortSpec> orderBy = new ArrayList<>();
        TypeInfo.FrameSpec frame = null;

        for (var p : overSpec.parameters()) {
            if (p instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
                partitionBy.add(cs.name());
            } else if (p instanceof AppliedFunction paf) {
                String funcName = simpleName(paf.function());
                if ("asc".equals(funcName) || "ascending".equals(funcName)) {
                    String col = extractColumnName(paf.parameters().get(0));
                    orderBy.add(new TypeInfo.SortSpec(col, TypeInfo.SortDirection.ASC));
                } else if ("desc".equals(funcName) || "descending".equals(funcName)) {
                    String col = extractColumnName(paf.parameters().get(0));
                    orderBy.add(new TypeInfo.SortSpec(col, TypeInfo.SortDirection.DESC));
                } else if ("rows".equals(funcName) || "range".equals(funcName) || "_range".equals(funcName)) {
                    String frameType = funcName.startsWith("_") ? funcName.substring(1) : funcName;
                    TypeInfo.FrameBound start = resolveFrameBoundPure(paf.parameters(), true);
                    TypeInfo.FrameBound end = resolveFrameBoundPure(paf.parameters(), false);
                    frame = new TypeInfo.FrameSpec(frameType, start, end);
                }
            }
        }

        return new OverClauseResult(partitionBy, orderBy, frame);
    }

    /** Resolves a frame bound into a structured FrameBound (no SQL text). */
    private TypeInfo.FrameBound resolveFrameBoundPure(List<ValueSpecification> params, boolean isStart) {
        int idx = isStart ? 0 : 1;
        if (idx >= params.size())
            return isStart ? TypeInfo.FrameBound.unbounded() : TypeInfo.FrameBound.currentRow();
        var param = params.get(idx);
        if (param instanceof AppliedFunction af && "unbounded".equals(simpleName(af.function()))) {
            return TypeInfo.FrameBound.unbounded();
        }
        if (param instanceof AppliedFunction af && "minus".equals(simpleName(af.function()))) {
            if (!af.parameters().isEmpty()) {
                long v = extractIntLiteral(af.parameters().get(af.parameters().size() - 1));
                return TypeInfo.FrameBound.offset(-v); // negative = preceding
            }
        }
        long v = extractIntLiteral(param);
        if (v == 0)
            return TypeInfo.FrameBound.currentRow();
        return TypeInfo.FrameBound.offset(v); // positive = following, negative = preceding
    }

    /**
     * Extracts the Pure function name from an aggregate lambda like {y|$y->plus()}.
     */
    private String extractPureFuncName(LambdaFunction lf) {
        if (lf == null || lf.body().isEmpty())
            return null;
        var body = lf.body().get(0);
        if (body instanceof AppliedFunction af) {
            return simpleName(af.function());
        }
        return null;
    }

    /** Returns true for zero-arg ranking Pure functions. */
    private boolean isRankingFunc(String funcName) {
        return switch (funcName) {
            case "rowNumber", "rank", "denseRank", "percentRank", "cumulativeDistribution" -> true;
            default -> false;
        };
    }

    /** Returns true for Pure math functions that wrap a window function. */
    private boolean isWrapperFunc(String funcName) {
        return switch (funcName) {
            case "round", "abs", "ceil", "floor", "truncate" -> true;
            default -> false;
        };
    }

    /** Extracts a literal value as string from a ValueSpecification. */
    private String extractLiteralValue(ValueSpecification vs) {
        if (vs instanceof CInteger ci)
            return String.valueOf(ci.value());
        if (vs instanceof CFloat cf)
            return String.valueOf(cf.value());
        if (vs instanceof CString cs)
            return cs.value();
        return "0";
    }

    /** Extracts an integer literal value. */
    private long extractIntLiteral(ValueSpecification vs) {
        if (vs instanceof CInteger ci)
            return ci.value().longValue();
        if (vs instanceof CFloat cf)
            return (long) cf.value();
        return 0;
    }

    /**
     * Compiles join(left, right, joinType, condition).
     * Pre-resolves joinType from EnumValue/CString/AppliedProperty.
     */
    private TypeInfo compileJoin(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 3) {
            throw new PureCompileException("join() requires left source, right source, and condition");
        }

        TypeInfo left = compileExpr(params.get(0), ctx);
        TypeInfo right = compileExpr(params.get(1), ctx);

        // Merge column types from both sides
        Map<String, GenericType> mergedColumns = new LinkedHashMap<>(left.relationType().columns());
        mergedColumns.putAll(right.relationType().columns());

        // Pre-resolve join type from params
        String joinType = "INNER"; // default
        if (params.size() >= 4) {
            joinType = extractJoinTypeName(params.get(2));
        } else if (params.get(2) instanceof EnumValue) {
            joinType = extractJoinTypeName(params.get(2));
        }

        var info = new TypeInfo(new RelationType(mergedColumns), left.mapping(),
                Map.of(), List.of(), List.of(), List.of(), false, joinType, null);
        types.put(af, info);
        return info;
    }

    /** Extracts join type name from EnumValue, CString, or AppliedProperty. */
    private String extractJoinTypeName(ValueSpecification vs) {
        String typeName = switch (vs) {
            case EnumValue ev -> ev.value();
            case CString cs -> cs.value();
            case AppliedProperty ap -> ap.property();
            default -> "INNER";
        };
        return typeName.toUpperCase().replace(" ", "_");
    }

    /** Compiles asOfJoin. */
    private TypeInfo compileAsOfJoin(AppliedFunction af, CompilationContext ctx) {
        return compileJoin(af, ctx); // Same type logic
    }

    /**
     * Compiles pivot — pass through source type (columns not known until runtime).
     */
    private TypeInfo compilePivot(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.isEmpty()) {
            throw new PureCompileException("pivot() requires a source");
        }

        TypeInfo source = compileExpr(params.get(0), ctx);
        return typed(af, source.relationType(), source.mapping());
    }

    /**
     * Compiles from(source, runtimeRef).
     * Passes through the source's type — from() is a runtime binding, not a type
     * change.
     */
    private TypeInfo compileFrom(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.isEmpty()) {
            throw new PureCompileException("from() requires a source");
        }

        TypeInfo source = compileExpr(params.get(0), ctx);
        return typed(af, source.relationType(), source.mapping());
    }

    /**
     * Handles unknown/scalar functions — compiles the source (if it's a relation)
     * and propagates its type.
     */
    private TypeInfo compilePassThrough(AppliedFunction af, CompilationContext ctx) {
        // Try to compile the first param as a relation source
        if (!af.parameters().isEmpty()) {
            try {
                TypeInfo source = compileExpr(af.parameters().get(0), ctx);
                if (source.relationType() != null && !source.relationType().columns().isEmpty()) {
                    return typed(af, source.relationType(), source.mapping());
                }
            } catch (PureCompileException ignored) {
                // Not a relation source — treat as scalar
            }
        }
        return scalar(af);
    }

    // ========== ClassInstance (DSL containers) ==========

    private TypeInfo compileClassInstance(ClassInstance ci, CompilationContext ctx) {
        return switch (ci.type()) {
            case "relation" -> compileRelationAccessor(ci, ctx);
            case "tdsLiteral" -> compileTdsLiteral(ci, ctx);
            case "instance" -> compileInstanceLiteral(ci);
            default -> scalar(ci);
        };
    }

    /**
     * Compiles a struct literal ^ClassName(prop=val, ...) with proper type info.
     * Builds a RelationType where each property becomes a typed column,
     * so PlanGenerator can distinguish arrays (need UNNEST) from scalars.
     */
    private TypeInfo compileInstanceLiteral(ClassInstance ci) {
        var data = (CleanAstBuilder.InstanceData) ci.value();
        var columns = new LinkedHashMap<String, GenericType>();
        for (var entry : data.properties().entrySet()) {
            columns.put(entry.getKey(), inferStructPropertyType(entry.getValue()));
        }
        var rt = new RelationType(columns);
        // Tag as struct source so PlanGenerator knows without AST walking
        var info = TypeInfo.structOf(rt);
        types.put(ci, info);
        return info;
    }

    /**
     * Infers the GenericType of a struct property value from its AST node.
     */
    private GenericType inferStructPropertyType(ValueSpecification vs) {
        return switch (vs) {
            case CString ignored -> GenericType.Primitive.STRING;
            case CInteger ignored -> GenericType.Primitive.INTEGER;
            case CFloat ignored -> GenericType.Primitive.FLOAT;
            case CDecimal ignored -> GenericType.Primitive.DECIMAL;
            case CBoolean ignored -> GenericType.Primitive.BOOLEAN;
            case CDateTime ignored -> GenericType.Primitive.DATE_TIME;
            case CStrictDate ignored -> GenericType.Primitive.STRICT_DATE;
            case Collection coll -> {
                // Array property — infer element type from first element
                GenericType elemType = coll.values().isEmpty()
                        ? GenericType.Primitive.ANY
                        : inferStructPropertyType(coll.values().get(0));
                yield GenericType.listOf(elemType);
            }
            case ClassInstance nested when "instance".equals(nested.type()) -> {
                var nestedData = (CleanAstBuilder.InstanceData) nested.value();
                yield new GenericType.ClassType(nestedData.className());
            }
            default -> GenericType.Primitive.ANY;
        };
    }

    private TypeInfo compileRelationAccessor(ClassInstance ci, CompilationContext ctx) {
        String tableRef = (String) ci.value();
        Table table = resolveTable(tableRef);
        return typed(ci, tableToRelationType(table), null);
    }

    private TypeInfo compileTdsLiteral(ClassInstance ci, CompilationContext ctx) {
        String raw = (String) ci.value();
        org.finos.legend.pure.dsl.TdsLiteral tds = org.finos.legend.pure.dsl.TdsLiteral.parse(raw);
        // Build RelationType from parsed column names and types
        Map<String, GenericType> columns = new LinkedHashMap<>();
        for (var col : tds.columns()) {
            columns.put(col.name(), GenericType.Primitive.STRING);
        }
        // Register type on the ORIGINAL ci so callers can look it up
        var info = typed(ci, new RelationType(columns), null);
        // Also register a parsed-TDS ClassInstance for PlanGenerator
        types.put(new ClassInstance("tdsLiteral", tds), info);
        return info;
    }

    /**
     * Type-checks a scalar expression, validating column references.
     * When a mapping is bound (class-based queries), resolves property
     * names through the mapping before checking column existence.
     */
    private void typeCheckExpression(ValueSpecification vs, CompilationContext ctx) {
        switch (vs) {
            case AppliedProperty ap -> {
                if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof Variable v) {
                    RelationType relType = ctx.getRelationType(v.name());
                    if (relType != null && !relType.columns().isEmpty()) {
                        // Mapping exists — verify the property name is a valid column
                        // (projected columns use property names, not physical column names)
                        RelationalMapping mapping = ctx.getMapping(v.name());
                        if (mapping != null) {
                            var columnOpt = mapping.getColumnForProperty(ap.property());
                            if (columnOpt.isPresent()) {
                                // Property is in the mapping — check the property name (alias)
                                // against projected columns, not the physical column name
                                if (relType.columns().containsKey(ap.property())) {
                                    // OK — property name matches a projected column
                                } else {
                                    // Try physical column name as fallback (direct table access)
                                    relType.requireColumn(columnOpt.get());
                                }
                            }
                            // If property not in mapping, allow it through
                            // (could be a computed field or direct column access)
                        } else {
                            // No mapping — check column name directly
                            relType.requireColumn(ap.property());
                        }
                    }
                }
            }
            case AppliedFunction af -> {
                for (var param : af.parameters()) {
                    typeCheckExpression(param, ctx);
                }
            }
            case LambdaFunction lf -> {
                for (var body : lf.body()) {
                    typeCheckExpression(body, ctx);
                }
            }
            default -> {
            }
        }
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

        if (mappingRegistry != null) {
            var mappingOpt = mappingRegistry.findByTableName(tableKey);
            if (mappingOpt.isPresent())
                return mappingOpt.get().table();
            mappingOpt = mappingRegistry.findByTableName(tableName);
            if (mappingOpt.isPresent())
                return mappingOpt.get().table();
        }

        throw new PureCompileException("Table not found: " + tableRef);
    }

    /** Converts a physical Table to a RelationType. */
    private RelationType tableToRelationType(Table table) {
        Map<String, GenericType> columns = new LinkedHashMap<>();
        for (var col : table.columns()) {
            columns.put(col.name(), col.dataType().toGenericType());
        }
        return new RelationType(columns);
    }

    // ========== Extraction Utilities ==========

    static String simpleName(String qualifiedName) {
        return TypeInfo.simpleName(qualifiedName);
    }

    private LambdaFunction extractLambda(ValueSpecification vs) {
        if (vs instanceof LambdaFunction lf)
            return lf;
        throw new PureCompileException("Expected lambda, got: " + vs.getClass().getSimpleName());
    }

    private String extractStringValue(ValueSpecification vs) {
        if (vs instanceof CString s)
            return s.value();
        throw new PureCompileException("Expected string, got: " + vs.getClass().getSimpleName());
    }

    private String extractColumnName(ValueSpecification vs) {
        if (vs instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
            return cs.name();
        }
        if (vs instanceof ClassInstance ci && ci.value() instanceof ColSpecArray csa) {
            // ColSpecArray in single-column context — use first ColSpec name
            if (!csa.colSpecs().isEmpty())
                return csa.colSpecs().get(0).name();
        }
        if (vs instanceof CString s)
            return s.value();
        // Column index (old-style groupBy): groupBy([0, 1], ...)
        if (vs instanceof CInteger ci)
            return "col" + ci.value();
        // Old-style lambda: {r | $r.colName} → extract property name
        if (vs instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            var body = lf.body().get(0);
            if (body instanceof AppliedProperty ap) {
                return ap.property();
            }
            if (body instanceof AppliedFunction af && !af.parameters().isEmpty()) {
                if (af.parameters().get(0) instanceof AppliedProperty ap) {
                    return ap.property();
                }
                // Receiver-style calls like $x.name->toLower() — recurse into first param
                String nested = extractPropertyName(af.parameters().get(0));
                if (nested != null)
                    return nested;
            }
            // Use the function name itself as last resort for lambdas
            if (body instanceof AppliedFunction af2) {
                return simpleName(af2.function());
            }
            // Lambda body we can't extract a property name from — throw
            throw new PureCompileException(
                    "extractColumnName: unrecognized lambda body: " + body.getClass().getSimpleName() + " → " + body);
        }
        // Direct property reference: $r.col
        if (vs instanceof AppliedProperty ap)
            return ap.property();
        // Function application: $r.col->sum()
        if (vs instanceof AppliedFunction af) {
            if (!af.parameters().isEmpty() && af.parameters().get(0) instanceof AppliedProperty ap) {
                return ap.property();
            }
            return simpleName(af.function());
        }
        // Variable reference: $x
        if (vs instanceof Variable v)
            return v.name();
        // Type annotation: GenericTypeInstance(Integer) in project specs
        if (vs instanceof GenericTypeInstance gti)
            return gti.fullPath();
        // No fallback — throw so we can fix the root cause
        throw new PureCompileException(
                "extractColumnName: unrecognized VS type: " + vs.getClass().getSimpleName() + " → " + vs);
    }

    private List<String> extractColumnNames(ValueSpecification vs) {
        if (vs instanceof ClassInstance ci && ci.value() instanceof ColSpecArray csa) {
            return csa.colSpecs().stream().map(ColSpec::name).toList();
        }
        if (vs instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
            return List.of(cs.name());
        }
        if (vs instanceof Collection coll) {
            return coll.values().stream().map(this::extractColumnName).toList();
        }
        return List.of(extractColumnName(vs));
    }

    /** Extracts a list of strings from a Collection. */
    private List<String> extractStringList(ValueSpecification vs) {
        if (vs instanceof Collection coll) {
            return coll.values().stream()
                    .filter(v -> v instanceof CString)
                    .map(v -> ((CString) v).value())
                    .toList();
        }
        if (vs instanceof CString s) {
            return List.of(s.value());
        }
        return List.of();
    }

    // ========== Association Pre-Resolution ==========

    /**
     * Scans projection lambdas for multi-hop property paths and resolves
     * the association targets. Called during compileProject().
     *
     * @param lambdaSpecs The projection lambda specifications
     * @param mapping     The source mapping (for looking up the source class)
     * @return Map of association property name → pre-resolved target
     */
    private java.util.Map<String, TypeInfo.AssociationTarget> resolveAssociations(
            List<ValueSpecification> lambdaSpecs, RelationalMapping mapping) {
        if (modelContext == null || mapping == null) {
            return java.util.Map.of();
        }
        var result = new java.util.LinkedHashMap<String, TypeInfo.AssociationTarget>();
        for (var lambdaSpec : lambdaSpecs) {
            List<String> path = extractPropertyPathFromLambda(lambdaSpec);
            if (path.size() > 1) {
                String assocProp = path.get(0);
                if (!result.containsKey(assocProp)) {
                    resolveAndStore(assocProp, mapping, result);
                }
            }
        }
        return result.isEmpty() ? java.util.Map.of() : java.util.Map.copyOf(result);
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
            List<ValueSpecification> body, RelationalMapping mapping) {
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
    private void scanForAssociationPaths(ValueSpecification vs, RelationalMapping mapping,
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
    private void resolveAndStore(String assocProp, RelationalMapping mapping,
            java.util.Map<String, TypeInfo.AssociationTarget> result) {
        String sourceClassName = mapping.pureClass().name();
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
     * Extracts the full property path from a lambda, e.g., ["items", "name"].
     * Returns a single-element list for simple properties.
     */
    private List<String> extractPropertyPathFromLambda(ValueSpecification vs) {
        if (vs instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            ValueSpecification body = lf.body().get(0);
            if (body instanceof AppliedProperty ap) {
                return extractPropertyChain(ap);
            }
        }
        // ColSpec
        if (vs instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
            return List.of(cs.name());
        }
        // Fallback: single property
        String prop = extractPropertyFromLambda(vs);
        return prop != null ? List.of(prop) : List.of();
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

    /**
     * Extracts the property name from a lambda like {p|$p.firstName}.
     */
    private String extractPropertyFromLambda(ValueSpecification vs) {
        if (vs instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            String name = extractPropertyName(lf.body().get(0));
            if (name != null)
                return name;
        }
        // Fallback to extractColumnName which always returns something
        return extractColumnName(vs);
    }

    private String extractPropertyName(ValueSpecification vs) {
        if (vs instanceof AppliedProperty ap) {
            return ap.property();
        }
        if (vs instanceof AppliedFunction af) {
            // For chained property access like $p.date->year(),
            // extract bottommost property
            if (!af.parameters().isEmpty()) {
                return extractPropertyName(af.parameters().get(0));
            }
            // Zero-arg function like now(), today() — use the function name
            return simpleName(af.function());
        }
        return null;
    }

    /**
     * Extracts the new column name from an extend spec.
     * Handles: ~newCol : lambda, ColSpec, ClassInstance.
     */
    private String extractNewColumnName(ValueSpecification vs) {
        if (vs instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
            return cs.name();
        }
        if (vs instanceof ClassInstance ci && ci.value() instanceof ColSpecArray csa) {
            // Multiple columns in extend — return first
            return csa.colSpecs().isEmpty() ? null : csa.colSpecs().get(0).name();
        }
        // For AppliedFunction (e.g., over() window spec), try to extract from first
        // param
        if (vs instanceof AppliedFunction af && !af.parameters().isEmpty()) {
            return extractNewColumnName(af.parameters().get(0));
        }
        return null;
    }

    /**
     * Extracts a full aggregate spec from a groupBy aggregate parameter.
     * Handles both ColSpec (new relation API) and legacy lambda patterns.
     * Returns a ColumnSpec with sourceCol, alias, and Pure aggregate function name.
     */
    private TypeInfo.ColumnSpec extractAggSpec(ValueSpecification vs) {
        if (vs instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
            String alias = cs.name();
            String sourceCol = alias; // default: column name is the alias
            String aggFunc = "plus"; // default Pure agg function

            // Extract source column from function1 lambda: x|$x.salary
            if (cs.function1() != null) {
                sourceCol = extractPropertyNameFromLambda(cs.function1());
                if (sourceCol == null)
                    sourceCol = alias;
            }

            // Extract aggregate function from function2 lambda: y|$y->sum()
            if (cs.function2() != null && !cs.function2().body().isEmpty()) {
                var body = cs.function2().body().get(0);
                if (body instanceof AppliedFunction bodyAf) {
                    aggFunc = simpleName(bodyAf.function());
                }
            }

            return TypeInfo.ColumnSpec.agg(sourceCol, alias, aggFunc);
        }
        // Fallback: extract column name as best we can
        String colName = extractNewColumnName(vs);
        if (colName != null) {
            return TypeInfo.ColumnSpec.agg(colName, colName + "_agg", "plus");
        }
        return null;
    }

    /**
     * Extracts property name from a lambda body.
     * E.g., from {x|$x.salary} extracts "salary".
     */
    private String extractPropertyNameFromLambda(ValueSpecification vs) {
        if (vs instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            ValueSpecification body = lf.body().get(0);
            if (body instanceof AppliedProperty ap)
                return ap.property();
        }
        return null;
    }

    // ========== Other AST Nodes ==========

    private TypeInfo compileLambda(LambdaFunction lf, CompilationContext ctx) {
        if (!lf.body().isEmpty()) {
            TypeInfo bodyInfo = compileExpr(lf.body().get(0), ctx);
            // Register the lambda node itself with the body's type info,
            // so root-level lambdas (|'hello'->endsWith('lo')) are in the types map
            return typed(lf, bodyInfo.relationType(), bodyInfo.mapping());
        }
        return scalar(lf);
    }

    private TypeInfo compileVariable(Variable v, CompilationContext ctx) {
        RelationType varType = ctx.getRelationType(v.name());
        if (varType != null) {
            return typed(v, varType, null);
        }
        return scalar(v);
    }

    private TypeInfo compileProperty(AppliedProperty ap, CompilationContext ctx) {
        if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof Variable v) {
            RelationType relType = ctx.getRelationType(v.name());
            if (relType != null) {
                relType.requireColumn(ap.property());
            }
        }
        return scalar(ap);
    }

    private TypeInfo compileCollection(Collection coll, CompilationContext ctx) {
        return scalar(coll);
    }

    // ========== Compilation Context ==========

    /**
     * Context for compilation — tracks variable → RelationType and variable →
     * Mapping bindings.
     */
    public record CompilationContext(
            Map<String, RelationType> relationTypes,
            Map<String, RelationalMapping> mappings) {

        public CompilationContext() {
            this(Map.of(), Map.of());
        }

        public CompilationContext withRelationType(String paramName, RelationType type) {
            var newTypes = new HashMap<>(relationTypes);
            newTypes.put(paramName, type);
            return new CompilationContext(Map.copyOf(newTypes), mappings);
        }

        public CompilationContext withMapping(String paramName, RelationalMapping mapping) {
            var newMappings = new HashMap<>(mappings);
            newMappings.put(paramName, mapping);
            return new CompilationContext(relationTypes, Map.copyOf(newMappings));
        }

        public RelationType getRelationType(String name) {
            return relationTypes.get(name);
        }

        public RelationalMapping getMapping(String name) {
            return mappings.get(name);
        }
    }
}
