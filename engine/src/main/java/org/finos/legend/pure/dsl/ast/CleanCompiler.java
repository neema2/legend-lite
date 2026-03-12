package org.finos.legend.pure.dsl.ast;

import org.finos.legend.engine.plan.GenericType;
import org.finos.legend.engine.plan.RelationType;
import org.finos.legend.engine.store.MappingRegistry;
import org.finos.legend.engine.store.RelationalMapping;
import org.finos.legend.engine.store.Table;
import org.finos.legend.pure.dsl.ModelContext;
import org.finos.legend.pure.dsl.PureCompileException;
import org.finos.legend.pure.dsl.PureFunctionRegistry;
import org.finos.legend.pure.dsl.PureParser;

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
            // Literals — scalar with known type
            case CInteger i -> scalarTyped(i, GenericType.Primitive.INTEGER);
            case CFloat f -> scalarTyped(f, GenericType.Primitive.FLOAT);
            case CDecimal d -> scalarTyped(d, GenericType.Primitive.DECIMAL);
            case CString s -> scalarTyped(s, GenericType.Primitive.STRING);
            case CBoolean b -> scalarTyped(b, GenericType.Primitive.BOOLEAN);
            case CDateTime dt -> scalarTyped(dt, GenericType.Primitive.DATE_TIME);
            case CStrictDate sd -> scalarTyped(sd, GenericType.Primitive.STRICT_DATE);
            case CStrictTime st -> scalarTyped(st, GenericType.Primitive.STRICT_TIME);
            case CLatestDate ld -> scalarTyped(ld, GenericType.Primitive.DATE_TIME);
            case CByteArray ba -> scalar(ba);
            case EnumValue ev -> scalar(ev);
            case UnitInstance ui -> scalar(ui);
        };
    }

    private TypeInfo scalar(ValueSpecification ast) {
        return typed(ast, RelationType.empty(), null);
    }

    /** Registers a scalar TypeInfo with a known GenericType. */
    private TypeInfo scalarTyped(ValueSpecification ast, GenericType type) {
        var info = TypeInfo.scalarOf(type);
        types.put(ast, info);
        return info;
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
            // --- Scalar collection functions with lambdas ---
            case "fold" -> compileFold(af, ctx);
            case "map" -> compileMap(af, ctx);
            case "find" -> compileFind(af, ctx);
            case "zip" -> compileZip(af, ctx);
            case "forAll", "exists" -> compileCollectionPredicate(af, ctx);
            case "block" -> compileBlock(af, ctx);
            case "letFunction" -> compileLet(af, ctx);
            // --- Type functions (cast, toMany, toOne, toVariant, to) ---
            case "cast", "toMany", "toOne", "toVariant", "to" -> compileTypeFunction(af, ctx);
            // --- Size (returns Integer scalar for both relations and lists) ---
            case "size" -> compileSize(af, ctx);
            // --- Collection / scalar functions (type-propagating) ---
            case "range" -> compileRange(af, ctx);
            // List-producing functions: always return a list
            case "tail", "init", "reverse", "removeDuplicates", "add" -> compileListProducing(af, ctx);
            case "at", "head",
                 "in", "length", "indexOf",
                 "last", "isEmpty", "isNotEmpty",
                 "contains", "startsWith", "endsWith",
                 "toLower", "toUpper", "trim", "replace",
                 "parseInteger", "parseFloat", "parseDecimal",
                 "parseBoolean", "parseDate",
                 "toString", "substring",
                 // --- Math ---
                 "abs", "ceiling", "floor", "round", "sign",
                 "sqrt", "pow", "exp", "log", "log10",
                 "mod", "rem", "divide",
                 "bitAnd", "bitOr", "bitXor", "bitShiftLeft", "bitShiftRight",
                 "plus", "minus", "times",
                 "toDecimal", "toDegrees", "toRadians", "pi",
                 // --- String ---
                 "ascii", "char", "lpad", "rpad", "splitPart",
                 "reverseString", "format", "joinStrings",
                 "encodeBase64", "decodeBase64",
                 "hash", "levenshteinDistance",
                 // --- Date/Time ---
                 "date", "dateDiff", "datePart", "adjust", "timeBucket",
                 "year", "monthNumber", "dayOfMonth",
                 "hour", "hasHour", "hasMinute", "hasMonth",
                 // --- Comparison / Boolean ---
                 "equal", "greaterThan", "lessThan", "between",
                 "compare", "not", "and", "or", "xor",
                 "greatest", "least", "coalesce",
                 // --- Aggregation ---
                 "sum", "average", "mean", "median", "mode",
                 "min", "max", "minBy", "maxBy",
                 "stdDevSample", "variance", "varianceSample",
                 "variancePopulation", "covarPopulation",
                 "percentile", "percentileCont", "corr",
                 // --- Misc ---
                 "pair", "list", "eval",
                 "type", "generateGuid" -> compileTypePropagating(af, ctx);
            case "match" -> compileMatch(af, ctx);
            // --- Conditional ---
            case "if" -> compileIf(af, ctx);
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
            if (sourceType != null) {
                // Relational filter: bind param to relation columns
                lambdaCtx = ctx.withRelationType(paramName, sourceType);
                if (source.mapping() != null) {
                    lambdaCtx = lambdaCtx.withMapping(paramName, source.mapping());
                }
            } else {
                // Collection filter: bind param as lambda variable with element type
                GenericType elemType = source.scalarType() != null && source.scalarType().isList()
                        ? source.scalarType().elementType() : source.scalarType();
                lambdaCtx = ctx.withLambdaParam(paramName, elemType);
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

        // List sort with lambdas: detect compare lambda direction
        if (sourceType == null || sourceType.columns().isEmpty()) {
            for (int i = 1; i < params.size(); i++) {
                compileExpr(params.get(i), ctx);
            }
            // Detect direction from compare lambda: {x,y|$y->compare($x)} = DESC
            TypeInfo.SortDirection direction = TypeInfo.SortDirection.ASC;
            if (params.size() > 1) {
                // The compare lambda is always the last lambda with 2 params
                var lastParam = params.get(params.size() - 1);
                if (lastParam instanceof LambdaFunction compLf
                        && compLf.parameters().size() == 2 && !compLf.body().isEmpty()) {
                    var body = compLf.body().get(0);
                    if (body instanceof AppliedFunction af2
                            && simpleName(af2.function()).equals("compare")
                            && !af2.parameters().isEmpty()) {
                        String secondParam = compLf.parameters().get(1).name();
                        if (af2.parameters().get(0) instanceof Variable v
                                && v.name().equals(secondParam)) {
                            direction = TypeInfo.SortDirection.DESC;
                        }
                    }
                }
            }
            // Store direction as a SortSpec with null column (list sort, not relational)
            var info = TypeInfo.builder()
                    .sortSpecs(List.of(new TypeInfo.SortSpec(null, direction))).build();
            types.put(af, info);
            return info;
        }

        // Relational sort: resolve sort specs
        List<TypeInfo.SortSpec> sortSpecs = resolveSortSpecs(params.get(1), sourceType);

        // Handle direction override in 3rd param: sort('col', 'DESC') or sortBy({e | $e.col}, 'desc')
        if (params.size() > 2 && params.get(2) instanceof CString dirStr) {
            String dir = dirStr.value().toUpperCase();
            if ("DESC".equals(dir) || "DESCENDING".equals(dir)) {
                sortSpecs = sortSpecs.stream()
                        .map(s -> new TypeInfo.SortSpec(s.column(), TypeInfo.SortDirection.DESC))
                        .toList();
            }
        }

        var info = TypeInfo.builder().relationType(sourceType).mapping(source.mapping())
                .sortSpecs(sortSpecs).build();
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
        if (vs instanceof LambdaFunction lf) {
            // 2-param compare lambda: detect direction
            if (lf.parameters().size() == 2 && !lf.body().isEmpty()) {
                var body = lf.body().get(0);
                if (body instanceof AppliedFunction af2
                        && simpleName(af2.function()).equals("compare")
                        && !af2.parameters().isEmpty()) {
                    String secondParam = lf.parameters().get(1).name();
                    TypeInfo.SortDirection dir = TypeInfo.SortDirection.ASC;
                    if (af2.parameters().get(0) instanceof Variable v
                            && v.name().equals(secondParam)) {
                        dir = TypeInfo.SortDirection.DESC;
                    }
                    String col = extractColumnName(lf);
                    return new TypeInfo.SortSpec(col, dir);
                }
            }
            // 1-param key function: sortBy({e | $e.sal})
            String col = extractColumnName(lf);
            return new TypeInfo.SortSpec(col, TypeInfo.SortDirection.ASC);
        }
        throw new PureCompileException("Unsupported sort spec type: " + vs.getClass().getSimpleName());
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
            List<TypeInfo.ColumnSpec> colSpecs = new ArrayList<>();
            for (String c : cols) {
                colSpecs.add(TypeInfo.ColumnSpec.col(c));
            }
            var info = TypeInfo.builder().relationType(source.relationType().onlyColumns(cols))
                    .mapping(source.mapping()).columnSpecs(colSpecs).build();
            types.put(af, info);
            return info;
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
        var info = TypeInfo.builder().relationType(newType).mapping(source.mapping())
                .columnSpecs(colSpecs).build();
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

            // Detect function-wrapped lambda bodies: e.g., $e.date->monthNumber()
            // The lambda body is an AppliedFunction wrapping property access
            ValueSpecification computedExpr = null;
            if (lambdaSpec instanceof LambdaFunction lf && !lf.body().isEmpty()) {
                ValueSpecification body = lf.body().get(0);
                if (body instanceof AppliedFunction) {
                    // Function wrapping a property — store body for scalar compilation
                    computedExpr = body;
                }
            }

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
            projections.add(new TypeInfo.ProjectionSpec(specPath, resolvedColumn, alias, computedExpr));
        }

        RelationType resultType = new RelationType(projectedColumns);
        // Propagate struct flag from source
        if (source.isStructSource()) {
            var info = TypeInfo.builder().relationType(resultType).associations(associations)
                    .projections(projections).structSource(true).build();
            types.put(af, info);
            return info;
        }
        var info = TypeInfo.builder().relationType(resultType).mapping(mapping)
                .associations(associations).projections(projections).build();
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

        var info = TypeInfo.builder().relationType(new RelationType(selectedColumns))
                .mapping(source.mapping()).columnSpecs(colSpecs).build();
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

        // Group columns (param 1): ~col or [~col1, ~col2] or [{r | $r.col}]
        List<String> groupColNames = extractColumnNames(params.get(1));
        for (String col : groupColNames) {
            GenericType type = sourceType.columns().getOrDefault(col, GenericType.Primitive.STRING);
            resultColumns.put(col, type);
            colSpecs.add(TypeInfo.ColumnSpec.col(col));
        }

        // Aggregate columns: handle three patterns
        // Pattern 1 (legacy): params[2] is Collection[LambdaFunction]
        // Pattern 2 (new API, array): params[2] is ClassInstance(ColSpecArray)
        // Pattern 3 (new API, single): params[2+] are individual ColSpec instances
        if (params.size() > 2 && params.get(2) instanceof Collection aggColl) {
            // Legacy pattern: unwrap Collection of agg lambdas
            // Alias names come from params[3] (CString collection): ['dept', 'medianSal']
            // First N aliases are for group cols, rest for agg cols
            List<String> aliasNames = new ArrayList<>();
            if (params.size() > 3 && params.get(3) instanceof Collection aliasColl) {
                for (var v : aliasColl.values()) {
                    if (v instanceof CString cs) aliasNames.add(cs.value());
                }
            }
            for (int i = 0; i < aggColl.values().size(); i++) {
                var aggInfo = extractAggSpec(aggColl.values().get(i));
                if (aggInfo != null) {
                    // Override alias from params[3] if available
                    int aliasIdx = groupColNames.size() + i;
                    if (aliasIdx < aliasNames.size()) {
                        aggInfo = new TypeInfo.ColumnSpec(
                                aggInfo.columnName(), aliasNames.get(aliasIdx),
                                aggInfo.aggFunction(), aggInfo.extraArgs());
                    }
                    resultColumns.put(aggInfo.alias(), GenericType.Primitive.NUMBER);
                    colSpecs.add(aggInfo);
                }
            }
        } else if (params.size() > 2
                && params.get(2) instanceof ClassInstance ci
                && ci.value() instanceof ColSpecArray csa) {
            // Relation API array: ~[total:x|$x.id, count:x|$x.id:y|$y->count()]
            for (var cs : csa.colSpecs()) {
                var aggInfo = extractAggSpec(new ClassInstance("colSpec", cs));
                if (aggInfo != null) {
                    resultColumns.put(aggInfo.alias(), GenericType.Primitive.NUMBER);
                    colSpecs.add(aggInfo);
                }
            }
        } else {
            // New API single: params[2+] are individual ColSpec instances
            for (int i = 2; i < params.size(); i++) {
                var aggInfo = extractAggSpec(params.get(i));
                if (aggInfo != null) {
                    resultColumns.put(aggInfo.alias(), GenericType.Primitive.NUMBER);
                    colSpecs.add(aggInfo);
                }
            }
        }

        // Fallback: if no columns resolved, use source type so it's never empty
        if (resultColumns.isEmpty()) {
            return typed(af, sourceType, source.mapping());
        }

        var info = TypeInfo.builder().relationType(new RelationType(resultColumns))
                .mapping(source.mapping()).columnSpecs(colSpecs).build();
        types.put(af, info);
        return info;
    }

    /**
     * Compiles aggregate(source, aggSpecs).
     * Full-table aggregation (no group columns).
     * Populates columnSpecs in the sidecar so PlanGenerator can build aggregate SQL.
     */
    private TypeInfo compileAggregate(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.isEmpty()) {
            throw new PureCompileException("aggregate() requires a source");
        }

        TypeInfo source = compileExpr(params.get(0), ctx);

        // Build output columns from aggregate specs (same pattern as groupBy, no group cols)
        Map<String, GenericType> resultColumns = new LinkedHashMap<>();
        List<TypeInfo.ColumnSpec> colSpecs = new ArrayList<>();

        // Handle three patterns: Collection, ColSpecArray, or individual params
        if (params.size() > 1 && params.get(1) instanceof Collection aggColl) {
            for (var v : aggColl.values()) {
                var aggInfo = extractAggSpec(v);
                if (aggInfo != null) {
                    resultColumns.put(aggInfo.alias(), GenericType.Primitive.NUMBER);
                    colSpecs.add(aggInfo);
                }
            }
        } else if (params.size() > 1
                && params.get(1) instanceof ClassInstance ci
                && ci.value() instanceof ColSpecArray csa) {
            for (var cs : csa.colSpecs()) {
                var aggInfo = extractAggSpec(new ClassInstance("colSpec", cs));
                if (aggInfo != null) {
                    resultColumns.put(aggInfo.alias(), GenericType.Primitive.NUMBER);
                    colSpecs.add(aggInfo);
                }
            }
        } else {
            for (int i = 1; i < params.size(); i++) {
                var aggInfo = extractAggSpec(params.get(i));
                if (aggInfo != null) {
                    resultColumns.put(aggInfo.alias(), GenericType.Primitive.NUMBER);
                    colSpecs.add(aggInfo);
                }
            }
        }

        if (resultColumns.isEmpty()) {
            return typed(af, source.relationType(), source.mapping());
        }

        var info = TypeInfo.builder().relationType(new RelationType(resultColumns))
                .mapping(source.mapping()).columnSpecs(colSpecs).build();
        types.put(af, info);
        return info;
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

        // Structural fact: scalar extend has no over() and no function2 (aggregate lambda).
        // Window extend always has either over() (partition/order spec) or function2 (aggregate).
        boolean isScalarExtend = windowColSpec != null
                && overSpec == null
                && windowColSpec.function2() == null;

        TypeInfo.WindowFunctionSpec windowSpec = null;
        if (windowColSpec != null && !isScalarExtend) {
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

        // For scalar extends, type-check the lambda body so property accesses
        // get typed from the source RelationType (needed for string concat detection etc.)
        if (isScalarExtend && windowColSpec.function1() != null && sourceType != null) {
            LambdaFunction lambda = windowColSpec.function1();
            if (!lambda.parameters().isEmpty() && !lambda.body().isEmpty()) {
                String paramName = lambda.parameters().get(0).name();
                CompilationContext lambdaCtx = ctx.withRelationType(paramName, sourceType);
                typeCheckExpression(lambda.body().get(0), lambdaCtx);
            }
        }

        var info = TypeInfo.builder().relationType(new RelationType(newColumns))
                .mapping(source.mapping())
                .windowSpecs(windowSpec != null ? List.of(windowSpec) : List.of()).build();
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
                // Special handling for percentile: boolean args control function name
                if ("percentile".equals(aggFunc) || "percentileCont".equals(aggFunc)
                        || "percentileDisc".equals(aggFunc)) {
                    var percentileResult = resolvePercentileArgs(cs.function2(), aggFunc);
                    return TypeInfo.WindowFunctionSpec.aggregateMulti(percentileResult.funcName,
                            column, alias, partitionBy, orderBy, frame,
                            List.of(String.valueOf(percentileResult.value)));
                }
                // General: extract non-boolean extra args from function2
                List<String> fn2ExtraArgs = extractFuncExtraArgs(cs.function2());
                if (!fn2ExtraArgs.isEmpty()) {
                    return TypeInfo.WindowFunctionSpec.aggregateMulti(aggFunc, column, alias,
                            partitionBy, orderBy, frame, fn2ExtraArgs);
                }
                return TypeInfo.WindowFunctionSpec.aggregate(aggFunc, column, alias,
                        partitionBy, orderBy, frame);
            }
            // rowMapper pattern: {p,w,r|rowMapper($r.valA, $r.valB)}:y|$y->corr()
            if (column == null && aggFunc != null && cs.function1() != null) {
                var body = cs.function1().body();
                if (!body.isEmpty() && body.get(0) instanceof AppliedFunction rmAf) {
                    String rmFunc = simpleName(rmAf.function());
                    if (rmFunc.endsWith("rowMapper") || "rowMapper".equals(rmFunc)) {
                        // Extract two columns from rowMapper params
                        String col1 = null, col2 = null;
                        if (rmAf.parameters().size() >= 1) {
                            col1 = extractColumnName(rmAf.parameters().get(0));
                        }
                        if (rmAf.parameters().size() >= 2) {
                            col2 = extractColumnName(rmAf.parameters().get(1));
                        }
                        if (col1 != null) {
                            List<String> extra = col2 != null ? List.of(col2) : List.of();
                            return TypeInfo.WindowFunctionSpec.aggregateMulti(aggFunc, col1, alias,
                                    partitionBy, orderBy, frame, extra);
                        }
                    }
                    // Non-rowMapper function in function1 body — extract source column
                    String sourceCol = rmAf.parameters().size() > 0
                            ? extractColumnName(rmAf.parameters().get(0))
                            : null;
                    if (sourceCol != null) {
                        return TypeInfo.WindowFunctionSpec.aggregate(aggFunc, sourceCol, alias,
                                partitionBy, orderBy, frame);
                    }
                }
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

                // NTILE: bucket arg may be at various positions
                if ("ntile".equals(funcName)) {
                    int buckets = 1;
                    for (int pi = 0; pi < af.parameters().size(); pi++) {
                        var p = af.parameters().get(pi);
                        if (p instanceof CInteger ci) {
                            buckets = ci.value().intValue();
                            break;
                        }
                    }
                    return TypeInfo.WindowFunctionSpec.ntile(buckets, alias,
                            partitionBy, orderBy, frame);
                }

                // LAG/LEAD: always pass offset 1
                if ("lag".equals(funcName) || "lead".equals(funcName)) {
                    String sourceCol = extractColumnNameDeep(af);
                    return TypeInfo.WindowFunctionSpec.aggregateMulti(funcName, sourceCol, alias,
                            partitionBy, orderBy, frame, List.of("1"));
                }

                // COUNT: use * instead of column name
                if ("count".equals(funcName) || "size".equals(funcName)) {
                    return TypeInfo.WindowFunctionSpec.aggregate("count", "*", alias,
                            partitionBy, orderBy, frame);
                }

                // NTH_VALUE: extract offset arg
                if ("nth".equals(funcName) || "nthValue".equals(funcName)) {
                    String sourceCol = extractColumnNameDeep(af);
                    int offset = 1;
                    for (int pi = 0; pi < af.parameters().size(); pi++) {
                        var p = af.parameters().get(pi);
                        if (p instanceof CInteger ci) {
                            offset = ci.value().intValue();
                            break;
                        }
                    }
                    return TypeInfo.WindowFunctionSpec.aggregateMulti(funcName, sourceCol, alias,
                            partitionBy, orderBy, frame, List.of(String.valueOf(offset)));
                }

                // Aggregate/value functions with arguments: sum($w.salary)
                String sourceCol = af.parameters().size() > 1
                        ? extractColumnName(af.parameters().get(1))
                        : null;
                return TypeInfo.WindowFunctionSpec.aggregate(funcName, sourceCol, alias,
                        partitionBy, orderBy, frame);
            }

            // Property access pattern: {p,w,r|$p->avg($w,$r).salary}
            if (!body.isEmpty() && body.get(0) instanceof AppliedProperty ap) {
                String propName = ap.property();
                if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof AppliedFunction innerAf) {
                    String innerFunc = simpleName(innerAf.function());
                    // Extract extra literal args (e.g., nth offset 2, joinStrings separator)
                    List<String> innerExtras = new ArrayList<>();
                    for (int ei = 1; ei < innerAf.parameters().size(); ei++) {
                        var px = innerAf.parameters().get(ei);
                        if (px instanceof CInteger ci) {
                            innerExtras.add(String.valueOf(ci.value()));
                        } else if (px instanceof CFloat cf) {
                            innerExtras.add(String.valueOf(cf.value()));
                        } else if (px instanceof CString cstr) {
                            innerExtras.add("'" + cstr.value() + "'");
                        }
                        // Skip variable references ($p, $w, $r)
                    }
                    if (!innerExtras.isEmpty()) {
                        return TypeInfo.WindowFunctionSpec.aggregateMulti(innerFunc, propName, alias,
                                partitionBy, orderBy, frame, innerExtras);
                    }
                    // LAG/LEAD always need offset=1
                    if ("lag".equals(innerFunc) || "lead".equals(innerFunc)) {
                        return TypeInfo.WindowFunctionSpec.aggregateMulti(innerFunc, propName, alias,
                                partitionBy, orderBy, frame, List.of("1"));
                    }
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
            } else if (p instanceof Collection coll) {
                // Collection of sort specs: [~o->ascending(), ~i->ascending()]
                for (var elem : coll.values()) {
                    var sortSpec = tryResolveSortSpec(elem);
                    if (sortSpec != null)
                        orderBy.add(sortSpec);
                }
            } else if (p instanceof AppliedFunction paf) {
                String funcName = simpleName(paf.function());
                var sortSpec = tryResolveSortSpec(p);
                if (sortSpec != null) {
                    orderBy.add(sortSpec);
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

    /** Try to resolve a single ascending/descending sort spec from an AST node. */
    private TypeInfo.SortSpec tryResolveSortSpec(ValueSpecification vs) {
        if (vs instanceof AppliedFunction af) {
            String funcName = simpleName(af.function());
            if ("asc".equals(funcName) || "ascending".equals(funcName)) {
                String col = extractColumnName(af.parameters().get(0));
                return new TypeInfo.SortSpec(col, TypeInfo.SortDirection.ASC);
            } else if ("desc".equals(funcName) || "descending".equals(funcName)) {
                String col = extractColumnName(af.parameters().get(0));
                return new TypeInfo.SortSpec(col, TypeInfo.SortDirection.DESC);
            }
        }
        return null;
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

    /**
     * Extracts extra literal/column args from aggregate lambda body.
     * E.g., {y|$y->percentile(0.6, true, true)} → ["0.6", "true", "true"]
     * {y|$y->joinStrings('')} → ["''"]
     */
    private List<String> extractFuncExtraArgs(LambdaFunction lf) {
        List<String> extras = new ArrayList<>();
        if (lf == null || lf.body().isEmpty())
            return extras;
        var body = lf.body().get(0);
        if (body instanceof AppliedFunction af) {
            // Params: [0] = $y (variable), [1+] = extra args
            for (int i = 1; i < af.parameters().size(); i++) {
                var p = af.parameters().get(i);
                if (p instanceof CInteger ci) {
                    extras.add(String.valueOf(ci.value()));
                } else if (p instanceof CFloat cf) {
                    extras.add(String.valueOf(cf.value()));
                } else if (p instanceof CDecimal cd) {
                    extras.add(cd.value().toPlainString());
                } else if (p instanceof CString cstr2) {
                    extras.add("'" + cstr2.value() + "'");
                } else if (p instanceof AppliedProperty ap) {
                    extras.add(ap.property());
                }
            }
        }
        return extras;
    }

    /**
     * Result of resolving percentile boolean args to function name + numeric value.
     */
    private record PercentileResult(String funcName, double value) {
    }

    /**
     * Resolves percentile function args: percentile(0.6, ascending, continuous).
     * Boolean arg2: ascending — if false, value becomes 1.0 - value.
     * Boolean arg3: continuous — if false, function is DISC; if true, CONT.
     * Matches old pipeline AstAdapter lines 1126-1143.
     */
    private PercentileResult resolvePercentileArgs(LambdaFunction lf, String baseFuncName) {
        String funcName = "percentileCont"; // default: continuous
        double value = 0.5;

        if (lf != null && !lf.body().isEmpty() && lf.body().get(0) instanceof AppliedFunction af) {
            // Extract percentile value (arg1, after $y variable at index 0)
            if (af.parameters().size() > 1) {
                var valParam = af.parameters().get(1);
                if (valParam instanceof CFloat cf)
                    value = cf.value();
                else if (valParam instanceof CDecimal cd)
                    value = cd.value().doubleValue();
                else if (valParam instanceof CInteger ci)
                    value = ci.value().doubleValue();
            }
            // arg2: ascending (index 2)
            if (af.parameters().size() > 2 && af.parameters().get(2) instanceof CBoolean ascBool) {
                if (!ascBool.value()) {
                    value = 1.0 - value;
                }
            }
            // arg3: continuous (index 3)
            if (af.parameters().size() > 3 && af.parameters().get(3) instanceof CBoolean contBool) {
                if (!contBool.value()) {
                    funcName = "percentileDisc";
                }
            }
        }
        // If base function already specifies disc
        if ("percentileDisc".equals(baseFuncName)) {
            funcName = "percentileDisc";
        }
        return new PercentileResult(funcName, value);
    }

    /**
     * Extracts source column from a window function call like lag($p, $w, $r).
     * Scans params for AppliedProperty, skipping Variable refs ($p, $w, $r).
     */
    private String extractColumnNameDeep(AppliedFunction af) {
        for (var p : af.parameters()) {
            if (p instanceof AppliedProperty ap)
                return ap.property();
        }
        // Fallback to first non-variable param
        for (var p : af.parameters()) {
            String col = extractColumnName(p);
            if (col != null)
                return col;
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

        // Walk the condition lambda and tag each AppliedProperty with its join-side alias
        int conditionIdx = params.size() >= 4 ? 3 : 2;
        if (conditionIdx < params.size() && params.get(conditionIdx) instanceof LambdaFunction lf) {
            String leftParam = lf.parameters().size() > 0 ? lf.parameters().get(0).name() : "l";
            String rightParam = lf.parameters().size() > 1 ? lf.parameters().get(1).name() : "r";
            if (!lf.body().isEmpty()) {
                tagJoinConditionProperties(lf.body().get(0), leftParam, rightParam);
            }
        }

        var info = TypeInfo.builder().relationType(new RelationType(mergedColumns))
                .mapping(left.mapping()).joinType(joinType).build();
        types.put(af, info);
        return info;
    }

    /**
     * Recursively tags each AppliedProperty in a join condition with its join-side alias.
     * PlanGenerator reads columnAlias from the side table instead of AST-walking.
     */
    private void tagJoinConditionProperties(ValueSpecification vs, String leftParam, String rightParam) {
        switch (vs) {
            case AppliedProperty ap -> {
                if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof Variable v) {
                    if (v.name().equals(leftParam)) {
                        types.put(ap, TypeInfo.withAlias("left_src"));
                    } else if (v.name().equals(rightParam)) {
                        types.put(ap, TypeInfo.withAlias("right_src"));
                    }
                }
            }
            case AppliedFunction afn -> {
                for (var p : afn.parameters()) {
                    tagJoinConditionProperties(p, leftParam, rightParam);
                }
            }
            case LambdaFunction lf -> {
                for (var body : lf.body()) {
                    tagJoinConditionProperties(body, leftParam, rightParam);
                }
            }
            default -> { /* literals, variables — no tagging needed */ }
        }
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
     * Compiles pivot — extracts pivot columns and aggregate specs into TypeInfo.PivotSpec.
     * Pure: relation->pivot(~[pivotCols], ~[aggName : x | $x.col : y | $y->sum()])
     */
    private TypeInfo compilePivot(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.isEmpty()) {
            throw new PureCompileException("pivot() requires a source");
        }

        TypeInfo source = compileExpr(params.get(0), ctx);

        // Extract pivot columns from params[1]: ClassInstance(ColSpecArray)
        List<String> pivotColumns = new java.util.ArrayList<>();
        if (params.size() > 1 && params.get(1) instanceof ClassInstance ci) {
            if (ci.value() instanceof ColSpecArray csa) {
                for (ColSpec cs : csa.colSpecs()) {
                    pivotColumns.add(cs.name());
                }
            } else if (ci.value() instanceof ColSpec cs) {
                pivotColumns.add(cs.name());
            }
        }

        // Extract aggregate specs from params[2]: ClassInstance(ColSpecArray)
        List<TypeInfo.PivotAggSpec> aggregates = new java.util.ArrayList<>();
        if (params.size() > 2 && params.get(2) instanceof ClassInstance ci) {
            List<ColSpec> aggSpecs;
            if (ci.value() instanceof ColSpecArray csa) {
                aggSpecs = csa.colSpecs();
            } else if (ci.value() instanceof ColSpec cs) {
                aggSpecs = List.of(cs);
            } else {
                throw new PureCompileException("pivot(): unsupported aggregate spec: " + ci.value());
            }

            for (ColSpec cs : aggSpecs) {
                String alias = cs.name();
                String aggFunction = "SUM";
                String valueColumn = null;
                ValueSpecification valueExpr = null;

                // function2 = aggregate function: y | $y->sum()
                if (cs.function2() != null && !cs.function2().body().isEmpty()) {
                    ValueSpecification aggBody = cs.function2().body().get(0);
                    if (aggBody instanceof AppliedFunction aggFn) {
                        aggFunction = switch (simpleName(aggFn.function())) {
                            case "plus", "sum" -> "SUM";
                            case "count" -> "COUNT";
                            case "average", "mean" -> "AVG";
                            case "min" -> "MIN";
                            case "max" -> "MAX";
                            default -> simpleName(aggFn.function()).toUpperCase();
                        };
                    }
                }

                // function1 = value extraction: x | $x.col or x | $x.a * $x.b
                if (cs.function1() != null && !cs.function1().body().isEmpty()) {
                    ValueSpecification body = cs.function1().body().get(0);
                    if (body instanceof AppliedProperty ap) {
                        valueColumn = ap.property();
                    } else {
                        // Complex expression — store AST for PlanGenerator to compile
                        valueExpr = body;
                    }
                }

                aggregates.add(new TypeInfo.PivotAggSpec(alias, aggFunction, valueColumn, valueExpr));
            }
        }

        var pivotSpec = new TypeInfo.PivotSpec(pivotColumns, aggregates);
        var info = TypeInfo.builder().relationType(source.relationType())
                .mapping(source.mapping()).pivotSpec(pivotSpec).build();
        types.put(af, info);
        return info;
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
     *
     * Also checks the PureFunctionRegistry for user-defined functions.
     * If found, inlines the function body by substituting parameters with
     * argument source text, re-parsing, and compiling the result.
     */
    private static final PureFunctionRegistry functionRegistry = PureFunctionRegistry.withBuiltins();

    // ========== Scalar Collection Functions with Lambdas ==========

    /** Compiles fold(list, {x,y|body}, init). */
    private TypeInfo compileFold(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        // Compile all params — lambda bodies get walked via compileLambda
        for (var p : params) {
            compileExpr(p, ctx);
        }
        // Detect fold+add pattern: fold(source, {val,acc|$acc->add($val)}, init)
        // Desugar to concatenate(init, source) so PlanGenerator emits LIST_CONCAT
        if (params.size() >= 3 && params.get(1) instanceof LambdaFunction lf) {
            if (isFoldAddPattern(lf)) {
                // Build synthetic: concatenate(init, source)
                var concat = new AppliedFunction("concatenate",
                        List.of(params.get(2), params.get(0)));
                compileExpr(concat, ctx);
                // Point fold node to the synthetic concatenate via inlinedBody
                var info = TypeInfo.from(types.get(concat)).inlinedBody(concat).build();
                types.put(af, info);
                return info;
            }
        }
        return scalar(af);
    }

    /** Checks if a fold lambda body is the add pattern: $acc->add($val) */
    private static boolean isFoldAddPattern(LambdaFunction lf) {
        if (lf.parameters().size() < 2 || lf.body().isEmpty()) return false;
        String elemParam = lf.parameters().get(0).name();
        String accParam = lf.parameters().get(1).name();
        // Body must be: add(acc, val) — an AppliedFunction named "add"
        if (lf.body().get(0) instanceof AppliedFunction bodyAf
                && TypeInfo.simpleName(bodyAf.function()).equals("add")
                && bodyAf.parameters().size() == 2) {
            // First param of add is the accumulator, second is the element
            var addSource = bodyAf.parameters().get(0);
            var addElem = bodyAf.parameters().get(1);
            return addSource instanceof Variable accVar && accVar.name().equals(accParam)
                    && addElem instanceof Variable elemVar && elemVar.name().equals(elemParam);
        }
        return false;
    }

    /** Compiles match(input, [branches], extraParams...) — static type dispatch. */
    private TypeInfo compileMatch(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        // Compile all params first
        for (var p : params) {
            try { compileExpr(p, ctx); }
            catch (PureCompileException ignored) { }
        }
        if (params.size() < 2) return scalar(af);

        // Determine input type name
        String inputTypeName = inferTypeName(params.get(0));
        if (inputTypeName == null) return scalar(af);

        // Extract branches from Collection (params[1])
        List<LambdaFunction> branches;
        if (params.get(1) instanceof Collection coll) {
            branches = coll.values().stream()
                    .filter(v -> v instanceof LambdaFunction)
                    .map(v -> (LambdaFunction) v)
                    .toList();
        } else if (params.get(1) instanceof LambdaFunction lf) {
            branches = List.of(lf);
        } else {
            return scalar(af);
        }

        // Determine if input is a collection (affects multiplicity matching)
        boolean inputIsMany = (params.get(0) instanceof Collection coll && coll.values().size() != 1)
                || (types.get(params.get(0)) != null && types.get(params.get(0)).isList());

        // Find matching branch by type + multiplicity
        for (var branch : branches) {
            if (branch.parameters().isEmpty()) continue;
            Variable branchParam = branch.parameters().get(0);
            if (branchParam.typeName() == null) continue;
            String branchType = TypeInfo.simpleName(branchParam.typeName());
            if (branchType.equals(inputTypeName)
                    || branchType.equals("Any")
                    || inputTypeName.equals("Any")) {
                // Check multiplicity compatibility
                String mult = branchParam.multiplicity();
                boolean branchAcceptsMany = mult == null || mult.equals("*")
                        || mult.equals("0") || mult.contains("..");
                if (inputIsMany && !branchAcceptsMany) continue;
                // Match found — compile the body with params bound as let bindings
                // so that variable references get substituted with actual values
                CompilationContext matchCtx = ctx;
                // Bind match param → input value as let binding
                matchCtx = matchCtx.withLetBinding(branchParam.name(), params.get(0));
                // If there's an extra param (params[2]), bind it similarly
                if (branch.parameters().size() > 1 && params.size() > 2) {
                    Variable extraParam = branch.parameters().get(1);
                    matchCtx = matchCtx.withLetBinding(extraParam.name(), params.get(2));
                }
                if (!branch.body().isEmpty()) {
                    ValueSpecification body = branch.body().get(0);
                    TypeInfo bodyInfo = compileExpr(body, matchCtx);
                    // Set inlinedBody so PlanGenerator follows the resolved expression
                    var info = TypeInfo.from(bodyInfo).inlinedBody(body).build();
                    types.put(af, info);
                    return info;
                }
            }
        }
        return scalar(af);
    }

    /** Infers the simple type name from an AST node. */
    private String inferTypeName(ValueSpecification vs) {
        if (vs instanceof CString) return "String";
        if (vs instanceof CInteger) return "Integer";
        if (vs instanceof CFloat) return "Float";
        if (vs instanceof CDecimal) return "Decimal";
        if (vs instanceof CBoolean) return "Boolean";
        if (vs instanceof CStrictDate) return "StrictDate";
        if (vs instanceof CDateTime) return "DateTime";
        if (vs instanceof Collection coll) {
            // Infer from first element, or fall through to side table
            if (!coll.values().isEmpty()) {
                return inferTypeName(coll.values().get(0));
            }
        }
        // Check side table
        TypeInfo info = types.get(vs);
        if (info != null && info.scalarType() != null) {
            GenericType st = info.scalarType();
            // For list types (e.g. from cast), use the element type
            if (st.isList() && st.elementType() != null) st = st.elementType();
            if (st == GenericType.Primitive.STRING) return "String";
            if (st == GenericType.Primitive.INTEGER) return "Integer";
            if (st == GenericType.Primitive.FLOAT) return "Float";
            if (st == GenericType.Primitive.BOOLEAN) return "Boolean";
        }
        return null;
    }



    /** Compiles list-producing functions (tail, init, reverse, etc.) — always returns a list. */
    private TypeInfo compileListProducing(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        for (var p : params) {
            compileExpr(p, ctx);
        }
        // Determine element type from source
        GenericType elemType = GenericType.Primitive.ANY;
        if (!params.isEmpty()) {
            TypeInfo sourceInfo = types.get(params.get(0));
            if (sourceInfo != null && sourceInfo.scalarType() != null) {
                // If source is already a list, use its element type
                if (sourceInfo.scalarType().isList() && sourceInfo.scalarType().elementType() != null) {
                    elemType = sourceInfo.scalarType().elementType();
                } else {
                    // Source is a single value — treat as element type
                    elemType = sourceInfo.scalarType();
                }
            }
        }
        return scalarTyped(af, GenericType.listOf(elemType));
    }

    /** Compiles range(n) — produces a List&lt;Integer&gt;. */
    private TypeInfo compileRange(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        for (var p : params) {
            compileExpr(p, ctx);
        }
        return scalarTyped(af, GenericType.listOf(GenericType.Primitive.INTEGER));
    }

    /** Compiles map(source, {x|body}). */
    private TypeInfo compileMap(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        for (var p : params) {
            compileExpr(p, ctx);
        }
        // map(list, lambda) produces a list — propagate list type from source
        if (!params.isEmpty()) {
            TypeInfo sourceInfo = types.get(params.get(0));
            if (sourceInfo != null && sourceInfo.isList()) {
                // Infer element type from lambda body if possible
                GenericType elemType = GenericType.Primitive.ANY;
                if (params.size() > 1 && params.get(1) instanceof LambdaFunction lf
                        && !lf.body().isEmpty()) {
                    TypeInfo bodyInfo = types.get(lf.body().get(0));
                    if (bodyInfo != null && bodyInfo.scalarType() != null) {
                        elemType = bodyInfo.scalarType();
                    }
                }
                return scalarTyped(af, GenericType.listOf(elemType));
            }
        }
        return scalar(af);
    }

    /** Compiles find(source, {x|predicate}). */
    private TypeInfo compileFind(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        for (var p : params) {
            compileExpr(p, ctx);
        }
        return scalar(af);
    }

    /** Compiles zip(list1, list2). */
    private TypeInfo compileZip(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        for (var p : params) {
            compileExpr(p, ctx);
        }
        return scalar(af);
    }

    /** Compiles forAll(list, {e|predicate}) and exists(list, {e|predicate}). */
    private TypeInfo compileCollectionPredicate(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        for (var p : params) {
            compileExpr(p, ctx);
        }
        return scalar(af);
    }

    /** Compiles size(source) — always returns Integer scalar, for both relations and lists. */
    private TypeInfo compileSize(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        // Compile source so its TypeInfo is available to PlanGenerator
        for (var p : params) {
            try { compileExpr(p, ctx); }
            catch (PureCompileException ignored) { }
        }
        TypeInfo info = TypeInfo.scalarOf(GenericType.Primitive.INTEGER);
        types.put(af, info);
        return info;
    }

    /** Compiles type conversion functions: cast, toMany, toOne, toVariant, to. */
    private TypeInfo compileTypeFunction(AppliedFunction af, CompilationContext ctx) {
        String func = simpleName(af.function());
        List<ValueSpecification> params = af.parameters();

        // Compile all params (source + @Type arg)
        for (var p : params) {
            compileExpr(p, ctx);
        }

        // Resolve target type from @Type argument (GenericTypeInstance)
        GenericType targetType = null;
        for (var p : params) {
            if (p instanceof GenericTypeInstance gti) {
                targetType = GenericType.fromTypeName(simpleName(gti.fullPath()));
                break;
            }
        }

        return switch (func) {
            case "toMany" -> {
                // toMany always produces a list of the target type
                GenericType listType = targetType != null
                        ? GenericType.listOf(targetType)
                        : GenericType.listOf(GenericType.Primitive.ANY);
                TypeInfo info = TypeInfo.scalarOf(listType);
                types.put(af, info);
                yield info;
            }
            case "cast" -> {
                // cast preserves source shape: if source is relational, propagate relation type
                TypeInfo sourceInfo = !params.isEmpty() ? types.get(params.get(0)) : null;
                if (sourceInfo != null && sourceInfo.relationType() != null) {
                    // Relational cast: propagate source relation type through
                    types.put(af, sourceInfo);
                    yield sourceInfo;
                }
                // Scalar cast: if source is list, result is list of target type
                if (sourceInfo != null && sourceInfo.isList() && targetType != null) {
                    TypeInfo info = TypeInfo.scalarOf(GenericType.listOf(targetType));
                    types.put(af, info);
                    yield info;
                }
                if (targetType != null) {
                    TypeInfo info = TypeInfo.scalarOf(targetType);
                    types.put(af, info);
                    yield info;
                }
                yield scalar(af);
            }
            case "toOne" -> {
                // toOne extracts single value — if source has list type, return element type
                TypeInfo sourceInfo = !params.isEmpty() ? types.get(params.get(0)) : null;
                if (sourceInfo != null && sourceInfo.scalarType() != null
                        && sourceInfo.scalarType().isList()
                        && sourceInfo.scalarType().elementType() != null) {
                    TypeInfo info = TypeInfo.scalarOf(sourceInfo.scalarType().elementType());
                    types.put(af, info);
                    yield info;
                }
                yield scalar(af);
            }
            case "to" -> {
                // to(@Type) — variant conversion, produces target type
                if (targetType != null) {
                    TypeInfo info = TypeInfo.scalarOf(targetType);
                    types.put(af, info);
                    yield info;
                }
                yield scalar(af);
            }
            case "toVariant" -> {
                // toVariant produces a variant (pass through source type for list detection)
                TypeInfo sourceInfo = !params.isEmpty() ? types.get(params.get(0)) : null;
                if (sourceInfo != null && sourceInfo.scalarType() != null) {
                    types.put(af, sourceInfo);
                    yield sourceInfo;
                }
                yield scalar(af);
            }
            default -> scalar(af);
        };
    }

    /** Compiles functions that propagate type from their first param. */
    private TypeInfo compileTypePropagating(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        for (var p : params) {
            try { compileExpr(p, ctx); }
            catch (PureCompileException ignored) { }
        }
        // Check if the function has a known return type that differs from its source
        GenericType returnType = knownReturnType(simpleName(af.function()));
        if (returnType != null) {
            return scalarTyped(af, returnType);
        }
        // Propagate scalarType from source (first param)
        if (!params.isEmpty()) {
            TypeInfo sourceInfo = types.get(params.get(0));
            if (sourceInfo != null && sourceInfo.scalarType() != null) {
                types.put(af, TypeInfo.scalarOf(sourceInfo.scalarType()));
                return TypeInfo.scalarOf(sourceInfo.scalarType());
            }
        }
        return scalar(af);
    }

    /** Return type for functions whose result type differs from their source type. */
    private static GenericType knownReturnType(String funcName) {
        return switch (funcName) {
            // Integer-producing functions
            case "length", "indexOf", "size", "count", "ascii",
                 "parseInteger", "toInteger",
                 "ceiling", "floor", "round", "mod", "rem", "sign",
                 "levenshteinDistance" -> GenericType.Primitive.INTEGER;
            // Float-producing functions
            case "toFloat", "parseFloat" -> GenericType.Primitive.FLOAT;
            // Decimal-producing functions
            case "toDecimal", "parseDecimal" -> GenericType.Primitive.DECIMAL;
            // String-producing functions
            case "toString", "toLower", "toUpper", "trim", "format",
                 "joinStrings", "replace", "lpad", "rpad", "splitPart",
                 "reverseString", "encodeBase64", "decodeBase64", "char",
                 "hash" -> GenericType.Primitive.STRING;
            // Boolean-producing functions
            case "contains", "in", "startsWith", "endsWith",
                 "isEmpty", "isNotEmpty", "parseBoolean" -> GenericType.Primitive.BOOLEAN;
            // Number-producing (propagate numeric but don't narrow)
            case "toDegrees", "toRadians" -> GenericType.Primitive.FLOAT;
            default -> null; // propagate source type
        };
    }

    /** Compiles if(condition, then-lambda, else-lambda). */
    private TypeInfo compileIf(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        for (var p : params) {
            try { compileExpr(p, ctx); }
            catch (PureCompileException ignored) { }
        }
        // Result type from then-branch (2nd param)
        if (params.size() >= 2) {
            TypeInfo thenInfo = types.get(params.get(1));
            if (thenInfo != null && thenInfo.scalarType() != null) {
                types.put(af, thenInfo);
                return thenInfo;
            }
        }
        return scalar(af);
    }

    /** Compiles block(stmt1, stmt2, ..., stmtN) — let binding scope. */
    private TypeInfo compileBlock(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> stmts = af.parameters();
        if (stmts.isEmpty()) return scalar(af);

        CompilationContext blockCtx = ctx;
        // Process all statements; for let stmts, enrich context with binding
        for (int i = 0; i < stmts.size() - 1; i++) {
            var stmt = stmts.get(i);
            if (stmt instanceof AppliedFunction letAf
                    && (simpleName(letAf.function()).equals("letFunction")
                            || simpleName(letAf.function()).equals("letAsLastStatement"))
                    && letAf.parameters().size() >= 2
                    && letAf.parameters().get(0) instanceof CString varName) {
                // let x = valueExpr → store binding in context
                ValueSpecification valueExpr = letAf.parameters().get(1);
                compileExpr(valueExpr, blockCtx); // compile value for type info
                blockCtx = blockCtx.withLetBinding(varName.value(), valueExpr);
            } else {
                compileExpr(stmt, blockCtx);
            }
        }
        // Compile the final (result) expression with enriched context
        ValueSpecification lastStmt = stmts.getLast();
        TypeInfo result = compileExpr(lastStmt, blockCtx);
        // Set inlinedBody on the block node → PlanGenerator transparently skips
        // through block to the result expression (same pattern as user function inlining)
        TypeInfo blockInfo = TypeInfo.from(result).inlinedBody(lastStmt).build();
        types.put(af, blockInfo);
        return blockInfo;
    }

    /** Compiles letFunction('x', valueExpr) — standalone let. */
    private TypeInfo compileLet(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() >= 2) {
            return compileExpr(params.get(1), ctx);
        }
        if (!params.isEmpty()) {
            return compileExpr(params.get(0), ctx);
        }
        return scalar(af);
    }

    private TypeInfo compilePassThrough(AppliedFunction af, CompilationContext ctx) {
        String funcName = af.function();
        String simple = simpleName(funcName);

        // === User-defined function inlining ===
        var fn = functionRegistry.getFunction(funcName)
                .or(() -> functionRegistry.getFunction(simple));
        if (fn.isPresent()) {
            return inlineUserFunction(af, fn.get(), ctx);
        }

        // Try to compile the first param as a relation source
        if (!af.parameters().isEmpty()) {
            try {
                TypeInfo source = compileExpr(af.parameters().get(0), ctx);
                // Compile remaining params so lambda variables get scoped
                for (int i = 1; i < af.parameters().size(); i++) {
                    try { compileExpr(af.parameters().get(i), ctx); }
                    catch (PureCompileException ignored) { }
                }
                if (source.relationType() != null && !source.relationType().columns().isEmpty()) {
                    return typed(af, source.relationType(), source.mapping());
                }
                // Propagate scalar type from source even for unknown functions
                if (source.scalarType() != null) {
                    System.err.println("[COMPILER] Unhandled function '" + simple
                            + "' — propagating source type (add to dispatch!)");
                    types.put(af, TypeInfo.scalarOf(source.scalarType()));
                    return TypeInfo.scalarOf(source.scalarType());
                }
            } catch (PureCompileException ignored) {
                // Not a relation source — still compile remaining params
                for (int i = 1; i < af.parameters().size(); i++) {
                    try { compileExpr(af.parameters().get(i), ctx); }
                    catch (PureCompileException ignored2) { }
                }
            }
        }
        System.err.println("[COMPILER] Unhandled function '" + simple
                + "' — returning scalar (add to dispatch!)");
        return scalar(af);
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
        ValueSpecification inlinedNode = PureParser.parseClean(body);
        TypeInfo bodyResult = compileExpr(inlinedNode, ctx);
        // Store inlined body in TypeInfo — PlanGenerator processes it instead of the
        // original call
        TypeInfo result = TypeInfo.from(bodyResult).inlinedBody(inlinedNode).lambdaParam(false).build();
        types.put(af, result);
        return result;
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
        for (int i = 0; i < tds.columns().size(); i++) {
            var col = tds.columns().get(i);
            GenericType colType = mapTdsColumnType(col.type());
            // If no type annotation, infer from first non-null data value
            if (colType == null && !tds.rows().isEmpty()) {
                for (var row : tds.rows()) {
                    if (i < row.size() && row.get(i) != null) {
                        Object val = row.get(i);
                        if (val instanceof Long) colType = GenericType.Primitive.INTEGER;
                        else if (val instanceof Double) colType = GenericType.Primitive.FLOAT;
                        else if (val instanceof Boolean) colType = GenericType.Primitive.BOOLEAN;
                        else colType = GenericType.Primitive.STRING;
                        break;
                    }
                }
            }
            columns.put(col.name(), colType != null ? colType : GenericType.Primitive.STRING);
        }
        // Register type on the ORIGINAL ci so callers can look it up
        var info = typed(ci, new RelationType(columns), null);
        // Also register a parsed-TDS ClassInstance for PlanGenerator
        types.put(new ClassInstance("tdsLiteral", tds), info);
        return info;
    }

    /** Maps a TDS type annotation string to a GenericType. */
    private static GenericType mapTdsColumnType(String typeStr) {
        if (typeStr == null) return null;
        return switch (typeStr) {
            case "Integer" -> GenericType.Primitive.INTEGER;
            case "Float", "Number" -> GenericType.Primitive.FLOAT;
            case "Decimal" -> GenericType.Primitive.DECIMAL;
            case "Boolean" -> GenericType.Primitive.BOOLEAN;
            case "String" -> GenericType.Primitive.STRING;
            case "Date", "StrictDate" -> GenericType.Primitive.STRICT_DATE;
            case "DateTime" -> GenericType.Primitive.DATE_TIME;
            default -> GenericType.Primitive.STRING;
        };
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
                        // Resolve column type and store in side table
                        GenericType colType = relType.columns().get(ap.property());
                        if (colType != null) {
                            types.put(ap, TypeInfo.scalarOf(colType));
                        }
                    }
                }
            }
            case AppliedFunction af -> {
                // Check for user-defined function inlining
                String funcName = af.function();
                String simple = simpleName(funcName);
                var fn = functionRegistry.getFunction(funcName)
                        .or(() -> functionRegistry.getFunction(simple));
                if (fn.isPresent()) {
                    inlineUserFunction(af, fn.get(), ctx);
                }
                for (var param : af.parameters()) {
                    typeCheckExpression(param, ctx);
                }
                // Propagate inner type through pass-through functions (toOne, toMany, etc.)
                if (("toOne".equals(simple) || "toMany".equals(simple)) && !af.parameters().isEmpty()) {
                    TypeInfo innerType = types.get(af.parameters().get(0));
                    if (innerType != null && innerType.scalarType() != null) {
                        types.put(af, TypeInfo.scalarOf(innerType.scalarType()));
                    }
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
     * Handles ColSpec (new relation API), LambdaFunction (legacy), and agg()
     * wrappers.
     * Returns a ColumnSpec with sourceCol, alias, and Pure aggregate function name.
     */
    private TypeInfo.ColumnSpec extractAggSpec(ValueSpecification vs) {
        if (vs instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
            String alias = cs.name();
            String sourceCol = null;
            String aggFunc = null;

            // Extract source column(s) from function1 lambda
            List<String> extraArgsFromFunc1 = new ArrayList<>();
            if (cs.function1() != null && !cs.function1().body().isEmpty()) {
                var f1Body = cs.function1().body().get(0);

                if (f1Body instanceof AppliedFunction f1Af) {
                    String f1Func = simpleName(f1Af.function());

                    if (f1Func.endsWith("rowMapper") || "rowMapper".equals(f1Func)) {
                        // rowMapper($x.col1, $x.col2) — extract two columns
                        for (var p : f1Af.parameters()) {
                            if (p instanceof AppliedProperty ap) {
                                if (sourceCol == null) {
                                    sourceCol = ap.property();
                                } else {
                                    extraArgsFromFunc1.add(ap.property());
                                }
                            }
                        }
                    } else if (cs.function2() == null) {
                        // Single-function aggregate: x|$x.name->joinStrings(',')
                        // The outermost function IS the aggregate
                        aggFunc = f1Func;
                        if (!f1Af.parameters().isEmpty()
                                && f1Af.parameters().get(0) instanceof AppliedProperty ap) {
                            sourceCol = ap.property();
                        }
                        // Extract extra args (separator for joinStrings, etc.)
                        for (int k = 1; k < f1Af.parameters().size(); k++) {
                            var extra = f1Af.parameters().get(k);
                            if (extra instanceof CString cs2) {
                                extraArgsFromFunc1.add("'" + cs2.value() + "'");
                            } else if (extra instanceof CInteger ci2) {
                                extraArgsFromFunc1.add(String.valueOf(ci2.value()));
                            } else if (extra instanceof CFloat cf) {
                                extraArgsFromFunc1.add(String.valueOf(cf.value()));
                            }
                        }
                    } else {
                        // Has function2 — function1 is just value extraction
                        sourceCol = extractPropertyNameFromLambda(cs.function1());
                    }
                } else if (f1Body instanceof AppliedProperty ap) {
                    // Simple property: x|$x.salary
                    sourceCol = ap.property();
                }
            }

            // If sourceCol still null, fall back to alias (e.g., ~[total:x|$x.id] with no explicit column)
            if (sourceCol == null) sourceCol = alias;

            // Extract aggregate function from function2 lambda: y|$y->sum()
            List<String> allExtraArgs = new ArrayList<>(extraArgsFromFunc1);
            if (cs.function2() != null && !cs.function2().body().isEmpty()) {
                var body = cs.function2().body().get(0);
                if (body instanceof AppliedFunction bodyAf) {
                    aggFunc = simpleName(bodyAf.function());

                    // Special handling for percentile(value, ascending, continuous)
                    if ("percentile".equals(aggFunc)) {
                        boolean ascending = true;
                        boolean continuous = true;
                        double pValue = 0.5;
                        var pParams = bodyAf.parameters();
                        if (pParams.size() > 1) {
                            if (pParams.get(1) instanceof CFloat cf) pValue = cf.value();
                            else if (pParams.get(1) instanceof CDecimal cd) pValue = cd.value().doubleValue();
                        }
                        if (pParams.size() > 2 && pParams.get(2) instanceof CBoolean cb) ascending = cb.value();
                        if (pParams.size() > 3 && pParams.get(3) instanceof CBoolean cb) continuous = cb.value();
                        aggFunc = continuous ? "percentileCont" : "percentileDisc";
                        double effectiveValue = ascending ? pValue : (1.0 - pValue);
                        String valStr = effectiveValue == (long) effectiveValue
                                ? String.valueOf((long) effectiveValue)
                                : String.valueOf(effectiveValue);
                        allExtraArgs.add(valStr);
                    } else {
                        // Extract extra params (separator for joinStrings, etc.)
                        for (int k = 1; k < bodyAf.parameters().size(); k++) {
                            var extra = bodyAf.parameters().get(k);
                            if (extra instanceof AppliedProperty ap) {
                                allExtraArgs.add(ap.property());
                            } else if (extra instanceof CInteger ci2) {
                                allExtraArgs.add(String.valueOf(ci2.value()));
                            } else if (extra instanceof CFloat cf) {
                                allExtraArgs.add(String.valueOf(cf.value()));
                            } else if (extra instanceof CDecimal cd) {
                                allExtraArgs.add(cd.value().toPlainString());
                            } else if (extra instanceof CString cs2) {
                                allExtraArgs.add("'" + cs2.value() + "'");
                            }
                        }
                    }
                }
            }

            // aggFunc must be set by now — either from function1 or function2
            if (aggFunc == null) aggFunc = "plus"; // only for simple ~[total:x|$x.id] with no agg function

            if (!allExtraArgs.isEmpty()) {
                return TypeInfo.ColumnSpec.aggMulti(sourceCol, alias, aggFunc, allExtraArgs);
            }
            return TypeInfo.ColumnSpec.agg(sourceCol, alias, aggFunc);
        }

        // Legacy lambda pattern: {r | $r.sal->stdDevSample()} or {r |
        // $r.sal->covarSample($r.years)}
        if (vs instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            var body = lf.body().get(0);
            if (body instanceof AppliedFunction bodyAf) {
                String aggFunc = simpleName(bodyAf.function());
                String sourceCol = null;
                List<String> extraArgs = new ArrayList<>();

                // First param is the source property: $r.sal
                if (!bodyAf.parameters().isEmpty()) {
                    var inner = bodyAf.parameters().get(0);
                    if (inner instanceof AppliedProperty ap) {
                        sourceCol = ap.property();
                    }
                }
                // Extra params: column refs or literals (e.g., $r.years or 0.5)
                for (int k = 1; k < bodyAf.parameters().size(); k++) {
                    var extra = bodyAf.parameters().get(k);
                    if (extra instanceof AppliedProperty ap) {
                        extraArgs.add(ap.property()); // column ref
                    } else if (extra instanceof CInteger ci) {
                        extraArgs.add(String.valueOf(ci.value()));
                    } else if (extra instanceof CFloat cf) {
                        extraArgs.add(String.valueOf(cf.value()));
                    } else if (extra instanceof CDecimal cd) {
                        extraArgs.add(cd.value().toPlainString());
                    } else if (extra instanceof CString cs) {
                        extraArgs.add("'" + cs.value() + "'");
                    }
                }
                if (sourceCol != null) {
                    return TypeInfo.ColumnSpec.aggMulti(sourceCol, sourceCol + "_agg", aggFunc, extraArgs);
                }
            } else if (body instanceof AppliedProperty ap) {
                // Simple property: {r | $r.sal} → default SUM
                return TypeInfo.ColumnSpec.agg(ap.property(), ap.property() + "_agg", "plus");
            }
        }

        // agg() wrapper: agg({r | $r.sal}, {y | $y->sum()})
        if (vs instanceof AppliedFunction af && "agg".equals(simpleName(af.function()))) {
            List<ValueSpecification> aggParams = af.parameters();
            String sourceCol = null;
            String aggFunc = "plus";
            if (aggParams.size() > 0) {
                sourceCol = extractPropertyNameFromLambda(aggParams.get(0));
            }
            if (aggParams.size() > 1 && aggParams.get(1) instanceof LambdaFunction lf2
                    && !lf2.body().isEmpty()) {
                var body2 = lf2.body().get(0);
                if (body2 instanceof AppliedFunction af2) {
                    aggFunc = simpleName(af2.function());
                }
            }
            if (sourceCol != null) {
                return TypeInfo.ColumnSpec.agg(sourceCol, sourceCol + "_agg", aggFunc);
            }
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
        if (!lf.body().isEmpty()) {
            TypeInfo bodyInfo = compileExpr(lf.body().get(0), lambdaCtx);
            return typed(lf, bodyInfo.relationType(), bodyInfo.mapping());
        }
        return scalar(lf);
    }

    private TypeInfo compileVariable(Variable v, CompilationContext ctx) {
        RelationType varType = ctx.getRelationType(v.name());
        if (varType != null) {
            return typed(v, varType, null);
        }
        // Let binding → inline the bound expression via inlinedBody
        // PlanGenerator already handles inlinedBody at both relational and scalar levels
        ValueSpecification letValue = ctx.getLetBinding(v.name());
        if (letValue != null) {
            TypeInfo letInfo = compileExpr(letValue, ctx);
            // Create a TypeInfo with the compiled type info AND inlinedBody pointing to the bound value
            TypeInfo inlined = TypeInfo.from(letInfo).inlinedBody(letValue).build();
            types.put(v, inlined);
            return inlined;
        }
        // Lambda parameter — mark in side table with declared type
        GenericType lambdaType = ctx.getLambdaParamType(v.name());
        if (lambdaType != null) {
            var info = TypeInfo.lambdaParamOf(lambdaType);
            types.put(v, info);
            return info;
        }
        if (ctx.isLambdaParam(v.name())) {
            var info = TypeInfo.lambdaParamMarker();
            types.put(v, info);
            return info;
        }
        return scalar(v);
    }

    private TypeInfo compileProperty(AppliedProperty ap, CompilationContext ctx) {
        if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof Variable v) {
            RelationType relType = ctx.getRelationType(v.name());
            if (relType != null) {
                relType.requireColumn(ap.property());
                // Resolve the column's type from the RelationType
                GenericType colType = relType.columns().get(ap.property());
                if (colType != null) {
                    return scalarTyped(ap, colType);
                }
            }
        }
        return scalar(ap);
    }

    private TypeInfo compileCollection(Collection coll, CompilationContext ctx) {
        // Compile each element so they're in the side table
        for (var v : coll.values()) {
            compileExpr(v, ctx);
        }
        GenericType elementType = unifyElementType(coll.values());
        return scalarTyped(coll, GenericType.listOf(elementType));
    }

    /**
     * Infers GenericType from a ValueSpecification AST node.
     * Used for type unification in collections.
     */
    private GenericType typeOf(ValueSpecification vs) {
        // Check side table first (for compiled sub-expressions)
        TypeInfo info = types.get(vs);
        if (info != null && info.scalarType() != null)
            return info.scalarType();
        // Fall back to pattern matching on literal types
        return switch (vs) {
            case CInteger i -> GenericType.Primitive.INTEGER;
            case CFloat f -> GenericType.Primitive.FLOAT;
            case CDecimal d -> GenericType.Primitive.DECIMAL;
            case CString s -> GenericType.Primitive.STRING;
            case CBoolean b -> GenericType.Primitive.BOOLEAN;
            case CDateTime dt -> GenericType.Primitive.DATE_TIME;
            case CStrictDate sd -> GenericType.Primitive.STRICT_DATE;
            case CStrictTime st -> GenericType.Primitive.STRICT_TIME;
            case CLatestDate ld -> GenericType.Primitive.DATE_TIME;
            case Collection c -> GenericType.listOf(unifyElementType(c.values()));
            default -> GenericType.Primitive.ANY;
        };
    }

    /**
     * Finds the common supertype for a list of expressions.
     * All numeric → NUMBER, all temporal → DATE, mixed → ANY.
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
        return GenericType.Primitive.ANY;
    }

    // ========== Compilation Context ==========

    /**
     * Context for compilation — tracks variable → RelationType and variable →
     * Mapping bindings.
     */
    public record CompilationContext(
            Map<String, RelationType> relationTypes,
            Map<String, RelationalMapping> mappings,
            Map<String, GenericType> lambdaParams,
            Map<String, ValueSpecification> letBindings) {

        public CompilationContext() {
            this(Map.of(), Map.of(), Map.of(), Map.of());
        }

        public CompilationContext withRelationType(String paramName, RelationType type) {
            var newTypes = new HashMap<>(relationTypes);
            newTypes.put(paramName, type);
            return new CompilationContext(Map.copyOf(newTypes), mappings, lambdaParams, letBindings);
        }

        public CompilationContext withMapping(String paramName, RelationalMapping mapping) {
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

        public RelationType getRelationType(String name) {
            return relationTypes.get(name);
        }

        public RelationalMapping getMapping(String name) {
            return mappings.get(name);
        }
    }
}
