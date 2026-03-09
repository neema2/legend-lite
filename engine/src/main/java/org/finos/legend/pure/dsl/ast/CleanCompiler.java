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
 * and produces typed {@link TypedValueSpec} with full type checking via
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

    public CleanCompiler(MappingRegistry mappingRegistry, ModelContext modelContext) {
        this.mappingRegistry = mappingRegistry;
        this.modelContext = modelContext;
    }

    public CleanCompiler(MappingRegistry mappingRegistry) {
        this(mappingRegistry, null);
    }

    public CleanCompiler() {
        this(null, null);
    }

    /**
     * Compiles a ValueSpecification to a typed result.
     */
    public TypedValueSpec compile(ValueSpecification vs, CompilationContext ctx) {
        return switch (vs) {
            case AppliedFunction af -> compileFunction(af, ctx);
            case ClassInstance ci -> compileClassInstance(ci, ctx);
            case LambdaFunction lf -> compileLambda(lf, ctx);
            case Variable v -> compileVariable(v, ctx);
            case AppliedProperty ap -> compileProperty(ap, ctx);
            case PackageableElementPtr pe -> TypedValueSpec.scalar(pe);
            case GenericTypeInstance gti -> TypedValueSpec.scalar(gti);
            case Collection coll -> compileCollection(coll, ctx);
            // Literals — scalar, no relation type
            case CInteger i -> TypedValueSpec.scalar(i);
            case CFloat f -> TypedValueSpec.scalar(f);
            case CDecimal d -> TypedValueSpec.scalar(d);
            case CString s -> TypedValueSpec.scalar(s);
            case CBoolean b -> TypedValueSpec.scalar(b);
            case CDateTime dt -> TypedValueSpec.scalar(dt);
            case CStrictDate sd -> TypedValueSpec.scalar(sd);
            case CStrictTime st -> TypedValueSpec.scalar(st);
            case CLatestDate ld -> TypedValueSpec.scalar(ld);
            case CByteArray ba -> TypedValueSpec.scalar(ba);
            case EnumValue ev -> TypedValueSpec.scalar(ev);
            case UnitInstance ui -> TypedValueSpec.scalar(ui);
        };
    }

    // ========== Function Dispatch ==========

    private TypedValueSpec compileFunction(AppliedFunction af, CompilationContext ctx) {
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
    private TypedValueSpec compileTableAccess(AppliedFunction af, CompilationContext ctx) {
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
                    return new TypedValueSpec(af, relType, mapping);
                }
            }
        }

        Table table = resolveTable(tableRef);
        RelationType relType = tableToRelationType(table);
        return new TypedValueSpec(af, relType);
    }

    /**
     * Compiles Person.all() → getAll(PackageableElementPtr("Person")).
     * Resolves class → mapping → table → RelationType.
     * Stores the mapping in context for property→column resolution in
     * project/filter.
     */
    private TypedValueSpec compileGetAll(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.isEmpty()) {
            throw new PureCompileException("getAll() requires a class argument");
        }

        String className;
        if (params.get(0) instanceof PackageableElementPtr pe) {
            className = pe.fullPath().contains("::") ? pe.fullPath().substring(pe.fullPath().lastIndexOf("::") + 2)
                    : pe.fullPath();
        } else {
            return TypedValueSpec.scalar(af);
        }

        if (mappingRegistry != null) {
            var mappingOpt = mappingRegistry.findByClassName(className);
            if (mappingOpt.isPresent()) {
                RelationalMapping mapping = mappingOpt.get();
                RelationType relType = tableToRelationType(mapping.table());
                return new TypedValueSpec(af, relType, mapping);
            }
        }

        return TypedValueSpec.scalar(af);
    }

    // ========== Shape-Preserving Operations ==========

    /**
     * Compiles filter(source, lambda).
     * Binds lambda param to source's type, validates column refs.
     */
    private TypedValueSpec compileFilter(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 2) {
            throw new PureCompileException("filter() requires 2 parameters");
        }

        TypedValueSpec source = compile(params.get(0), ctx);
        RelationType sourceType = source.resultType();

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

        return new TypedValueSpec(af, sourceType, source.mapping());
    }

    /** Compiles sort(source, sortSpecs). */
    private TypedValueSpec compileSort(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 2) {
            throw new PureCompileException("sort() requires source and sort specs");
        }

        TypedValueSpec source = compile(params.get(0), ctx);
        RelationType sourceType = source.resultType();

        // Validate sort column names exist in source
        validateSortColumns(params.get(1), sourceType);

        return new TypedValueSpec(af, sourceType, source.mapping());
    }

    private void validateSortColumns(ValueSpecification vs, RelationType sourceType) {
        if (sourceType.columns().isEmpty())
            return;

        if (vs instanceof Collection coll) {
            coll.values().forEach(v -> validateSortColumns(v, sourceType));
        } else if (vs instanceof AppliedFunction af) {
            String funcName = simpleName(af.function());
            if ("asc".equals(funcName) || "desc".equals(funcName)) {
                String col = extractColumnName(af.parameters().get(0));
                sourceType.requireColumn(col);
            }
        } else if (vs instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
            sourceType.requireColumn(cs.name());
        }
    }

    /** Compiles limit/take(source, count). */
    private TypedValueSpec compileLimit(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 2) {
            throw new PureCompileException("limit/take() requires source and count");
        }

        TypedValueSpec source = compile(params.get(0), ctx);
        return new TypedValueSpec(af, source.resultType(), source.mapping());
    }

    /** Compiles drop(source, count). */
    private TypedValueSpec compileDrop(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 2) {
            throw new PureCompileException("drop() requires source and count");
        }

        TypedValueSpec source = compile(params.get(0), ctx);
        return new TypedValueSpec(af, source.resultType(), source.mapping());
    }

    /** Compiles slice(source, start, end). */
    private TypedValueSpec compileSlice(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 3) {
            throw new PureCompileException("slice() requires source, start, and end");
        }

        TypedValueSpec source = compile(params.get(0), ctx);
        return new TypedValueSpec(af, source.resultType(), source.mapping());
    }

    /** Compiles first(source). */
    private TypedValueSpec compileFirst(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.isEmpty()) {
            throw new PureCompileException("first() requires a source");
        }
        TypedValueSpec source = compile(params.get(0), ctx);
        return new TypedValueSpec(af, source.resultType(), source.mapping());
    }

    /** Compiles distinct(source, columns?). */
    private TypedValueSpec compileDistinct(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.isEmpty()) {
            throw new PureCompileException("distinct() requires a source");
        }

        TypedValueSpec source = compile(params.get(0), ctx);

        if (params.size() > 1) {
            List<String> cols = extractColumnNames(params.get(1));
            return new TypedValueSpec(af, source.resultType().onlyColumns(cols), source.mapping());
        }

        return new TypedValueSpec(af, source.resultType(), source.mapping());
    }

    // ========== Shape-Changing Operations ==========

    /** Compiles rename(source, ~oldName, ~newName). */
    private TypedValueSpec compileRename(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 3) {
            throw new PureCompileException("rename() requires source, old name, and new name");
        }

        TypedValueSpec source = compile(params.get(0), ctx);
        String oldName = extractColumnName(params.get(1));
        String newName = extractColumnName(params.get(2));

        RelationType newType = source.resultType().renameColumn(oldName, newName);
        return new TypedValueSpec(af, newType, source.mapping());
    }

    /** Compiles concatenate(left, right). */
    private TypedValueSpec compileConcatenate(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 2) {
            throw new PureCompileException("concatenate() requires two sources");
        }

        TypedValueSpec left = compile(params.get(0), ctx);
        compile(params.get(1), ctx);

        return new TypedValueSpec(af, left.resultType(), left.mapping());
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
    private TypedValueSpec compileProject(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 2) {
            throw new PureCompileException("project() requires source and projection specs");
        }

        TypedValueSpec source = compile(params.get(0), ctx);
        RelationType sourceType = source.resultType();
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

        // Build projected RelationType
        Map<String, GenericType> projectedColumns = new LinkedHashMap<>();
        for (int i = 0; i < lambdaSpecs.size(); i++) {
            ValueSpecification lambdaSpec = lambdaSpecs.get(i);

            String propertyName = extractPropertyFromLambda(lambdaSpec);
            String alias = i < aliases.size() ? aliases.get(i) : propertyName;

            // Resolve column type via mapping or direct lookup
            GenericType colType = GenericType.Primitive.STRING; // default
            if (mapping != null && propertyName != null) {
                var columnOpt = mapping.getColumnForProperty(propertyName);
                if (columnOpt.isPresent()) {
                    String columnName = columnOpt.get();
                    colType = sourceType.columns().getOrDefault(columnName, colType);
                }
            } else if (propertyName != null && sourceType.columns().containsKey(propertyName)) {
                colType = sourceType.columns().get(propertyName);
            }

            projectedColumns.put(alias, colType);
        }

        RelationType resultType = new RelationType(projectedColumns);
        return new TypedValueSpec(af, resultType, mapping);
    }

    /**
     * Compiles select(source, ~col1, ~col2, ...).
     * Selects specific columns from a relation by name.
     */
    private TypedValueSpec compileSelect(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 2) {
            throw new PureCompileException("select() requires source and column specs");
        }

        TypedValueSpec source = compile(params.get(0), ctx);
        List<String> columnNames = new ArrayList<>();
        for (int i = 1; i < params.size(); i++) {
            columnNames.addAll(extractColumnNames(params.get(i)));
        }

        Map<String, GenericType> selectedColumns = new LinkedHashMap<>();
        for (String col : columnNames) {
            GenericType type = source.resultType().columns().containsKey(col)
                    ? source.resultType().columns().get(col)
                    : GenericType.Primitive.STRING;
            selectedColumns.put(col, type);
        }

        return new TypedValueSpec(af, new RelationType(selectedColumns), source.mapping());
    }

    /**
     * Compiles groupBy(source, groupCols, aggSpecs).
     * Output RelationType has group columns + aggregate columns.
     */
    private TypedValueSpec compileGroupBy(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 2) {
            throw new PureCompileException("groupBy() requires source and group specs");
        }

        TypedValueSpec source = compile(params.get(0), ctx);

        // GroupBy produces a new RelationType — for now, pass through with empty
        // (PlanGenerator will figure out the SQL from the AST)
        return new TypedValueSpec(af, RelationType.empty(), source.mapping());
    }

    /**
     * Compiles aggregate(source, aggSpecs).
     * Full-table aggregation (no group columns).
     */
    private TypedValueSpec compileAggregate(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.isEmpty()) {
            throw new PureCompileException("aggregate() requires a source");
        }

        TypedValueSpec source = compile(params.get(0), ctx);
        return new TypedValueSpec(af, RelationType.empty(), source.mapping());
    }

    /**
     * Compiles extend(source, ~newCol : lambda).
     * Adds computed column to the source's RelationType.
     */
    private TypedValueSpec compileExtend(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 2) {
            throw new PureCompileException("extend() requires source and column specs");
        }

        TypedValueSpec source = compile(params.get(0), ctx);
        RelationType sourceType = source.resultType();

        // Build new RelationType = source columns + new columns
        Map<String, GenericType> newColumns = new LinkedHashMap<>(sourceType.columns());
        // Extract new column names from extend specs
        for (int i = 1; i < params.size(); i++) {
            String colName = extractNewColumnName(params.get(i));
            if (colName != null) {
                newColumns.put(colName, GenericType.Primitive.STRING); // default type
            }
        }

        return new TypedValueSpec(af, new RelationType(newColumns), source.mapping());
    }

    /**
     * Compiles join(left, right, joinType, condition).
     */
    private TypedValueSpec compileJoin(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 3) {
            throw new PureCompileException("join() requires left source, right source, and condition");
        }

        TypedValueSpec left = compile(params.get(0), ctx);
        TypedValueSpec right = compile(params.get(1), ctx);

        // Merge column types from both sides
        Map<String, GenericType> mergedColumns = new LinkedHashMap<>(left.resultType().columns());
        mergedColumns.putAll(right.resultType().columns());

        return new TypedValueSpec(af, new RelationType(mergedColumns), left.mapping());
    }

    /** Compiles asOfJoin. */
    private TypedValueSpec compileAsOfJoin(AppliedFunction af, CompilationContext ctx) {
        return compileJoin(af, ctx); // Same type logic
    }

    /** Compiles pivot. */
    private TypedValueSpec compilePivot(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.isEmpty()) {
            throw new PureCompileException("pivot() requires a source");
        }

        TypedValueSpec source = compile(params.get(0), ctx);
        return new TypedValueSpec(af, RelationType.empty(), source.mapping());
    }

    /**
     * Compiles from(source, runtimeRef).
     * Passes through the source's type — from() is a runtime binding, not a type
     * change.
     */
    private TypedValueSpec compileFrom(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.isEmpty()) {
            throw new PureCompileException("from() requires a source");
        }

        TypedValueSpec source = compile(params.get(0), ctx);
        return new TypedValueSpec(af, source.resultType(), source.mapping());
    }

    /**
     * Handles unknown/scalar functions — compiles the source (if it's a relation)
     * and propagates its type.
     */
    private TypedValueSpec compilePassThrough(AppliedFunction af, CompilationContext ctx) {
        // Try to compile the first param as a relation source
        if (!af.parameters().isEmpty()) {
            try {
                TypedValueSpec source = compile(af.parameters().get(0), ctx);
                if (source.resultType() != null && !source.resultType().columns().isEmpty()) {
                    return new TypedValueSpec(af, source.resultType(), source.mapping());
                }
            } catch (Exception ignored) {
                // Not a relation source — treat as scalar
            }
        }
        return TypedValueSpec.scalar(af);
    }

    // ========== ClassInstance (DSL containers) ==========

    private TypedValueSpec compileClassInstance(ClassInstance ci, CompilationContext ctx) {
        return switch (ci.type()) {
            case "relation" -> compileRelationAccessor(ci, ctx);
            case "tdsLiteral" -> compileTdsLiteral(ci, ctx);
            default -> TypedValueSpec.scalar(ci);
        };
    }

    private TypedValueSpec compileRelationAccessor(ClassInstance ci, CompilationContext ctx) {
        String tableRef = (String) ci.value();
        Table table = resolveTable(tableRef);
        return new TypedValueSpec(ci, tableToRelationType(table));
    }

    private TypedValueSpec compileTdsLiteral(ClassInstance ci, CompilationContext ctx) {
        return new TypedValueSpec(ci, RelationType.empty());
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
        if (qualifiedName.contains("::")) {
            return qualifiedName.substring(qualifiedName.lastIndexOf("::") + 2);
        }
        return qualifiedName;
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
        if (vs instanceof CString s)
            return s.value();
        throw new PureCompileException(
                "Expected column name, got: " + vs.getClass().getSimpleName());
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

    /** Extracts items from a Collection or wraps a single value. */
    private List<ValueSpecification> extractList(ValueSpecification vs) {
        if (vs instanceof Collection coll) {
            return coll.values();
        }
        return List.of(vs);
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

    /**
     * Extracts the property name from a lambda like {p|$p.firstName}.
     */
    private String extractPropertyFromLambda(ValueSpecification vs) {
        if (vs instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            return extractPropertyName(lf.body().get(0));
        }
        return null;
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

    // ========== Other AST Nodes ==========

    private TypedValueSpec compileLambda(LambdaFunction lf, CompilationContext ctx) {
        if (!lf.body().isEmpty()) {
            return compile(lf.body().get(0), ctx);
        }
        return TypedValueSpec.scalar(lf);
    }

    private TypedValueSpec compileVariable(Variable v, CompilationContext ctx) {
        RelationType varType = ctx.getRelationType(v.name());
        if (varType != null) {
            return new TypedValueSpec(v, varType);
        }
        return TypedValueSpec.scalar(v);
    }

    private TypedValueSpec compileProperty(AppliedProperty ap, CompilationContext ctx) {
        if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof Variable v) {
            RelationType relType = ctx.getRelationType(v.name());
            if (relType != null) {
                relType.requireColumn(ap.property());
            }
        }
        return TypedValueSpec.scalar(ap);
    }

    private TypedValueSpec compileCollection(Collection coll, CompilationContext ctx) {
        return TypedValueSpec.scalar(coll);
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
