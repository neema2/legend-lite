package com.gs.legend.plan;

import com.gs.legend.antlr.ValueSpecificationBuilder;
import com.gs.legend.ast.*;
import com.gs.legend.compiler.PureCompileException;
import com.gs.legend.compiler.TypeCheckResult;
import com.gs.legend.compiler.TypeChecker;
import com.gs.legend.compiler.StoreResolution;
import com.gs.legend.compiler.TypeInfo;
import com.gs.legend.sqlgen.SQLDialect;
import com.gs.legend.sqlgen.SqlBuilder;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Compiles Pure AST to structured SQL via {@link SqlBuilder}.
 *
 * <p>
 * Replaces {@link PlanGenerator}: instead of building SQL strings by
 * concatenation, builds a structured {@link SqlBuilder} (SQL AST) that
 * gets serialized later via {@link SqlBuilder#toSql(SQLDialect)}.
 *
 * <p>
 * Aligned with legend-engine's {@code toSQLQuery()} — walks the Pure
 * expression tree once, building a mutable SQL object. Type information
 * is looked up from the compilation side table when needed.
 *
 * <p>
 * Design principle: <strong>minimize subquery wrapping</strong>. Only wrap
 * in subquery when structurally necessary (e.g., ORDER BY on a GROUP BY,
 * or window function on a filtered source).
 */
public class PlanGenerator {

    private final TypeCheckResult unit;
    private final SQLDialect dialect;
    private final java.util.IdentityHashMap<com.gs.legend.ast.ValueSpecification, com.gs.legend.compiler.StoreResolution> storeResolutions;
    private int tableAliasCounter = 0;

    public PlanGenerator(TypeCheckResult unit, SQLDialect dialect) {
        this(unit, dialect, new java.util.IdentityHashMap<>());
    }

    public PlanGenerator(TypeCheckResult unit, SQLDialect dialect,
            java.util.IdentityHashMap<com.gs.legend.ast.ValueSpecification, com.gs.legend.compiler.StoreResolution> storeResolutions) {
        this.unit = unit;
        this.dialect = dialect;
        this.storeResolutions = storeResolutions;
    }

    /**
     * One-shot from pre-built model: compile → generate plan.
     * Use when you already have a PureModelBuilder (e.g., QueryService reuses
     * the model it built for connection resolution).
     *
     * @param model       Pre-built PureModelBuilder
     * @param query       The Pure query string
     * @param runtimeName The runtime name to resolve dialect from
     * @return A SingleExecutionPlan containing the generated SQL
     */
    public static SingleExecutionPlan generate(
            com.gs.legend.model.PureModelBuilder model,
            String query, String runtimeName) {
        SQLDialect dialect = model.resolveDialect(runtimeName);
        var mappingNames = model.resolveMappingNames(runtimeName);
        var normalizer = new com.gs.legend.compiler.MappingNormalizer(model, mappingNames);

        var vs = model.resolveQuery(query);
        var unit = new TypeChecker(normalizer.modelContext()).check(vs);

        var storeResolutions = new com.gs.legend.compiler.MappingResolver(
                unit, normalizer.normalizedMapping(), model).resolve();
        return new PlanGenerator(unit, dialect, storeResolutions).generate();
    }

    /**
     * One-shot from source: parse source → compile → generate plan.
     * Convenience for callers that don't need the model for anything else.
     */
    public static SingleExecutionPlan generate(String pureSource, String query, String runtimeName) {
        var model = new com.gs.legend.model.PureModelBuilder().addSource(pureSource);
        return generate(model, query, runtimeName);
    }

    public SingleExecutionPlan generate() {
        ValueSpecification vs = unit.root();
        TypeInfo info = unit.types().get(vs);

        // If root was inlined (user function), process the expanded body
        if (info != null && info.inlinedBody() != null) {
            vs = info.inlinedBody();
            info = unit.types().get(vs);
        }

        SqlBuilder builder;
        if (info.isScalar() && storeFor(vs) == null) {
            builder = generateScalarQuery(vs);
        } else {
            builder = generateRelation(vs);
        }

        // JSON wrapping now handled inside generateGraphFetch — no top-level intercept needed.

        // expressionType MUST be stamped by TypeChecker — no silent fallback.
        var expressionType = info.expressionType();
        if (expressionType == null) {
            throw new PureCompileException(
                    "PlanGenerator: expressionType not stamped by compiler for root expression "
                            + vs.getClass().getSimpleName()
                            + " — this is a compiler bug, not a query error");
        }

        String sql = builder.toSql(dialect);
        var sqlNode = new SQLExecutionNode(
                sql,
                info != null ? info.schema() : null,
                null // connectionRef resolved by QueryService at execution time
        );
        return new SingleExecutionPlan(sqlNode, expressionType);
    }

    /**
     * Wraps a tabular SQL builder in JSON output.
     *
     * <p>Generates:
     * <pre>
     * SELECT json_group_array(json_object('p1', _sub."p1", ...)) AS result
     * FROM (tabular_sql) AS _sub
     * </pre>
     *
     * Uses GraphFetchSpec properties as JSON keys.
     * The inner tabular query must have SELECT columns matching the spec's property names.
     */
    private SqlBuilder wrapJsonOutput(SqlBuilder tabular,
            com.gs.legend.plan.GraphFetchSpec spec, TypeInfo info,
            java.util.Map<String, SqlExpr> nestedSubqueries) {
        // Build json_object key-value pairs: 'key', _sub."key", ...
        var kvPairs = new java.util.ArrayList<SqlExpr>();
        for (var prop : spec.properties()) {
            kvPairs.add(new SqlExpr.StringLiteral(prop.name()));
            if (prop.isNested() && nestedSubqueries.containsKey(prop.name())) {
                // Nested property: correlated subquery expression
                kvPairs.add(nestedSubqueries.get(prop.name()));
            } else {
                // ColumnRef.toSql() calls dialect.quoteIdentifier() — pass raw name
                kvPairs.add(new SqlExpr.ColumnRef(prop.name()));
            }
        }

        // Dialect-delegated JSON expressions (no DuckDB names in PlanGenerator)
        var jsonObjectExpr = new SqlExpr.JsonObject(kvPairs);
        var jsonArrayExpr = new SqlExpr.JsonArrayAgg(jsonObjectExpr);

        // Outer query: SELECT json_array_agg(json_object(...)) AS "result" FROM (inner) AS "_sub"
        var outer = new SqlBuilder();
        outer.addSelect(jsonArrayExpr, dialect.quoteIdentifier("result"));
        outer.fromSubquery(tabular, dialect.quoteIdentifier("_sub"));
        return outer;
    }

    /**
     * Compiles a standalone scalar query → SELECT scalarExpr AS "value".
     * Used for queries like |42, |'hello', |%2024-01-15->adjust(...), etc.
     */
    private SqlBuilder generateScalarQuery(ValueSpecification ast) {
        ValueSpecification body = ast;
        if (ast instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            body = lf.body().getLast();
        }
        SqlExpr scalar = generateScalar(body, null, storeFor(ast));

        // Collection results: UNNEST to produce N scalar rows instead of 1 row with a LIST.
        // Only applied when the compiler explicitly marked this expression with Multiplicity.MANY,
        // meaning it produces N independent values. Single list values (e.g., fold result) are left as-is.
        TypeInfo info = unit.types().get(ast);
        if (info != null && info.isMany()) {
            scalar = new SqlExpr.FunctionCall("unnest", List.of(scalar));
        }

        return new SqlBuilder().addSelect(scalar, dialect.quoteIdentifier("value"));
    }


    // ========== Relation Compilation ==========

    private SqlBuilder generateRelation(ValueSpecification vs) {
        // Compiler-driven inlining: follow inlinedBody pointers (let bindings, blocks, user functions)
        TypeInfo info = unit.types().get(vs);
        if (info != null && info.inlinedBody() != null) {
            return generateRelation(info.inlinedBody());
        }
        return switch (vs) {
            case AppliedFunction af -> generateFunction(af);
            case LambdaFunction lf -> {
                if (lf.body().isEmpty())
                    throw new PureCompileException("PlanGenerator: empty lambda body");

                yield generateRelation(lf.body().getLast());
            }
            case ClassInstance ci -> throw new PureCompileException(
                    "Unexpected ClassInstance '" + ci.type() + "' in generateRelation — should be desugared by parser");
            case PureCollection coll -> generateStructCollection(coll);
            default -> throw new PureCompileException(
                    "PlanGenerator: cannot compile: " + vs.getClass().getSimpleName());
        };

    }

    // generateClassInstance removed — parser desugars relation/tdsLiteral/instance to AppliedFunction

    /**
     * Compiles a struct literal ^ClassName(field=val, ...) to flat VALUES.
     * Each scalar property becomes its own column; array properties stay as array literals.
     * This allows the standard relational path (filter, project, etc.) to work uniformly.
     */
    private SqlBuilder generateStructLiteral(ValueSpecificationBuilder.InstanceData data) {
        return generateFlatValues(java.util.List.of(data));
    }

    /**
     * Compiles a collection of struct literals [^Class(...), ^Class(...)] to multi-row flat VALUES.
     */
    private SqlBuilder generateStructCollection(PureCollection coll) {
        if (coll.values().isEmpty()) {
            throw new PureCompileException("PlanGenerator: empty struct collection");
        }
        var rows = new java.util.ArrayList<ValueSpecificationBuilder.InstanceData>();
        for (var v : coll.values()) {
            if (v instanceof AppliedFunction af && "new".equals(simpleName(af.function()))
                    && af.parameters().size() >= 2
                    && af.parameters().get(1) instanceof ClassInstance ci
                    && "instance".equals(ci.type())) {
                rows.add((ValueSpecificationBuilder.InstanceData) ci.value());
            } else {
                throw new PureCompileException(
                        "PlanGenerator: struct collection contains non-instance: " + v.getClass().getSimpleName());
            }
        }
        return generateFlatValues(rows);
    }

    /**
     * Generates flat multi-row VALUES from a list of InstanceData.
     * Each property becomes its own column:
     *   VALUES ('f', [{...}]), ('f2', [{...}]) AS t("legalName", "employees")
     */
    private SqlBuilder generateFlatValues(java.util.List<ValueSpecificationBuilder.InstanceData> dataRows) {
        var firstData = dataRows.get(0);
        java.util.List<String> columnNames = firstData.properties().keySet().stream()
                .map(dialect::quoteIdentifier).toList();

        var rows = new java.util.ArrayList<java.util.List<SqlExpr>>();
        for (var data : dataRows) {
            var row = new java.util.ArrayList<SqlExpr>();
            for (var entry : data.properties().entrySet()) {
                SqlExpr value = renderStructValue(entry.getValue());
                // If compiler tagged as many (to-many [*] property) but AST is a single element, wrap in array
                TypeInfo valInfo = unit.types().get(entry.getValue());
                if (valInfo != null && valInfo.isMany()
                        && !(entry.getValue() instanceof PureCollection)) {
                    value = new SqlExpr.ArrayLiteral(java.util.List.of(value));
                }
                row.add(value);
            }
            rows.add(row);
        }

        return new SqlBuilder()
                .selectStar()
                .fromValues(rows, "t", columnNames);
    }

    /**
     * Recursively renders a ValueSpecification to a SqlExpr struct/value literal.
     * All syntax goes through dialect — no DuckDB-specific code here.
     */
    private SqlExpr renderStructValue(ValueSpecification vs) {
        return switch (vs) {
            case PureCollection coll -> {
                java.util.List<SqlExpr> elements = coll.values().stream()
                        .map(this::renderStructValue)
                        .toList();
                yield new SqlExpr.ArrayLiteral(elements);
            }
            case AppliedFunction af when "new".equals(simpleName(af.function()))
                    && af.parameters().size() >= 2
                    && af.parameters().get(1) instanceof ClassInstance ci
                    && "instance".equals(ci.type()) -> {
                var data = (ValueSpecificationBuilder.InstanceData) ci.value();
                var fields = new java.util.LinkedHashMap<String, SqlExpr>();
                for (var entry : data.properties().entrySet()) {
                    SqlExpr rendered = renderStructValue(entry.getValue());
                    TypeInfo valInfo = unit.types().get(entry.getValue());
                    if (valInfo != null && valInfo.isMany()
                            && !(entry.getValue() instanceof PureCollection)) {
                        rendered = new SqlExpr.ArrayLiteral(java.util.List.of(rendered));
                    }
                    fields.put(entry.getKey(), rendered);
                }
                yield new SqlExpr.StructLiteral(fields);
            }
            // All other values (literals, variables, function calls) → delegate to generateScalar
            default -> generateScalar(vs, null, null);
        };
    }

    /**
     * Compiles a parsed TdsLiteral to VALUES SQL.
     * Receives structured data from TypeChecker — no string parsing here.
     */
    private SqlBuilder generateTdsLiteral(com.gs.legend.ast.TdsLiteral tds) {
        List<String> quotedCols = tds.columns().stream()
                .map(c -> dialect.quoteIdentifier(c.name())).toList();

        var columns = tds.columns();

        // Empty TDS: generate a single NULL row with WHERE 1=0 to produce
        // zero rows while preserving the column schema (VALUES () is invalid SQL)
        if (tds.rows().isEmpty()) {
            List<SqlExpr> nullRow = columns.stream()
                    .map(c -> (SqlExpr) new SqlExpr.NullLiteral())
                    .collect(java.util.stream.Collectors.toList());
            return new SqlBuilder()
                    .selectStar()
                    .fromValues(List.of(nullRow), "_tds", quotedCols)
                    .addWhere(new SqlExpr.Binary(
                            new SqlExpr.NumericLiteral(1), "=", new SqlExpr.NumericLiteral(0)));
        }

        List<List<SqlExpr>> rows = tds.rows().stream()
                .map(row -> {
                    List<SqlExpr> cells = new java.util.ArrayList<>();
                    for (int i = 0; i < row.size(); i++) {
                        Object val = row.get(i);
                        String colType = i < columns.size() ? columns.get(i).type() : null;
                        SqlExpr cell;
                        // Date/DateTime columns: strip % prefix and emit DATE/TIMESTAMP literal
                        if (val instanceof String sv && colType != null
                                && ("StrictDate".equals(colType) || "Date".equals(colType))) {
                            String dateStr = sv.startsWith("%") ? sv.substring(1) : sv;
                            cell = new SqlExpr.DateLiteral(dateStr);
                        } else if (val instanceof String sv && colType != null
                                && "DateTime".equals(colType)) {
                            String tsStr = sv.startsWith("%") ? sv.substring(1) : sv;
                            cell = new SqlExpr.TimestampLiteral(tsStr);
                        } else {
                            cell = formatTdsValue(val);
                        }
                        // Variant columns need dialect-specific variant literal wrapping
                        if (i < columns.size() && columns.get(i).isVariant()) {
                            cell = new SqlExpr.VariantLiteral(cell);
                        }
                        cells.add(cell);
                    }
                    return cells;
                })
                .toList();

        return new SqlBuilder()
                .selectStar()
                .fromValues(rows, "_tds", quotedCols);
    }

    /** Formats a typed TDS cell value as SqlExpr. */
    private SqlExpr formatTdsValue(Object val) {
        if (val == null)
            return new SqlExpr.NullLiteral();
        if (val instanceof Boolean b)
            return new SqlExpr.BoolLiteral(b);
        if (val instanceof Number)
            return new SqlExpr.NumericLiteral((Number) val);
        return new SqlExpr.StringLiteral(val.toString());
    }

    private SqlBuilder generateFunction(AppliedFunction af) {
        String funcName = simpleName(af.function());
        return switch (funcName) {
            case "getAll" -> generateGetAll(af);
            case "table", "tableReference" -> generateTableAccess(af);
            case "tds" -> generateTdsFunction(af);
            case "new" -> generateNewFunction(af);
            case "filter" -> generateFilter(af);
            case "project" -> generateProject(af);
            case "sort", "sortBy", "sortByReversed" -> generateSort(af);
            case "limit", "take" -> generateLimit(af);
            case "drop" -> generateDrop(af);
            case "slice" -> generateSlice(af);
            case "first", "head" -> generateFirst(af);
            case "find" -> generateFind(af);
            case "distinct" -> generateDistinct(af);
            case "select" -> generateSelect(af);
            case "rename" -> generateRename(af);
            case "concatenate", "union" -> generateConcatenate(af);
            case "groupBy" -> generateGroupBy(af);
            case "aggregate" -> generateAggregate(af);
            case "extend" -> generateExtend(af);
            case "join" -> generateJoin(af);
            case "from" -> generateFrom(af);
            case "pivot" -> generatePivot(af);
            case "asOfJoin" -> generateAsOfJoin(af);
            case "flatten" -> generateFlatten(af);
            case "write" -> generateWrite(af);
            case "graphFetch" -> generateGraphFetch(af);
            case "serialize" -> generateRelation(af.parameters().get(0)); // walk to graphFetch source
            // --- Pass-through: source-preserving relational functions ---
            case "toString", "toVariant",
                 "eq", "cast", "size",
                 "newTDSRelationAccessor",
                 "greaterThan", "lessThan", "greaterThanEqual", "lessThanEqual" ->
                generateRelation(af.parameters().get(0));
            default -> throw new PureCompileException("PlanGenerator: unsupported function '" + funcName + "'");
        };
    }

    // ========== getAll / table ==========

    private SqlBuilder generateGetAll(AppliedFunction af) {
        var store = storeFor(af);
        if (store == null || store.tableName() == null) {
            throw new PureCompileException("getAll: StoreResolution not resolved for " + af.function());
        }

        // External data source: subquery FROM rendered by dialect (takes priority)
        if (store.sourceUrl() != null) {
            String alias = nextTableAlias();
            return new SqlBuilder()
                    .selectStar()
                    .fromSourceExpr(new SqlExpr.SourceUrl(store.sourceUrl()),
                            dialect.quoteIdentifier(alias));
        }

        // sourceSpec path: Relation ValueSpec chain (tableRef + filter + distinct + joins)
        if (store.sourceSpec() != null) {
            return generateRelation(store.sourceSpec());
        }

        // Fallback: simple SELECT * FROM table (for identity mappings)
        String tableName = store.tableName();
        String alias = nextTableAlias();
        return new SqlBuilder()
                .selectStar()
                .from(dialect.quoteIdentifier(tableName), dialect.quoteIdentifier(alias));
    }

    /**
     * Generates SQL for graphFetch: projects mapped properties from the source,
     * then wraps with JSON output based on the GraphFetchSpec.
     *
     * <p>Like generateProject, resolves mapped property expressions via
     * resolveColumnExpr. The projected columns are then wrapped with
     * json_group_array(json_object(...)).
     *
     * <p>Currently handles flat fetch (all scalar properties).
     * Phase 3: extend to handle nested properties via correlated subqueries.
     */
    private SqlBuilder generateGraphFetch(AppliedFunction af) {
        TypeInfo info = unit.typeInfoFor(af);
        var spec = info.graphFetchSpec();
        if (spec == null) {
            throw new PureCompileException(
                    "generateGraphFetch: graphFetchSpec not found in TypeInfo — compiler bug");
        }

        // Step 1: Build source SQL (e.g., Person.all() → SELECT * FROM T_RAW_PERSON)
        SqlBuilder source = generateRelation(af.parameters().get(0));

        // Step 2: Get table alias and store resolution
        var store = storeFor(af);
        String tableAlias;
        if (source.hasJoins()) {
            // Source has JOINs (from traverse extends) — wrap in subquery so
            // traverse-computed column aliases are accessible on the wrapper alias.
            // Same pattern as generateProject's canInline=false wrapping.
            tableAlias = "gf_src";
            source = new SqlBuilder().selectStar().fromSubquery(source, tableAlias);
        } else {
            tableAlias = unquote(source.getFromAlias());
        }

        // Step 3: Project mapped properties into source builder
        // Also track parent join columns needed for nested correlated subqueries
        source.clearSelect();
        var nestedSubqueries = new java.util.LinkedHashMap<String, SqlExpr>();
        for (var prop : spec.properties()) {
            if (prop.isNested()) {
                // Resolve join for this nested property
                var joinRes = store != null && store.hasJoins() ? store.joins().get(prop.name()) : null;
                if (joinRes == null) {
                    throw new PureCompileException(
                            "Nested property '" + prop.name() + "' has no resolved join in StoreResolution");
                }

                // Ensure parent join columns are projected (may be multiple for compound conditions)
                if (joinRes.joinCondition() != null) {
                    for (String col : joinRes.sourceColumns()) {
                        source.addSelect(new SqlExpr.Column(tableAlias, col), dialect.quoteIdentifier(col));
                    }
                }

                // Build correlated subquery for nested object
                String childTable = joinRes.targetTable();
                String childAlias = "c_" + prop.name();

                // Build json_object for child properties
                var childKvPairs = new java.util.ArrayList<SqlExpr>();
                for (var childProp : prop.nested().properties()) {
                    childKvPairs.add(new SqlExpr.StringLiteral(childProp.name()));
                    SqlExpr childColExpr = resolveColumnExpr(childProp.name(), joinRes.targetResolution(), childAlias);
                    childKvPairs.add(childColExpr);
                }
                var childJsonObj = new SqlExpr.JsonObject(childKvPairs);

                // Correlated WHERE: full join condition with _sub as source, childAlias as target
                SqlExpr correlation = (joinRes.joinCondition() != null)
                        ? generateScalar(joinRes.joinCondition(), null, null, null,
                            Map.of(joinRes.sourceParam(), "_sub", joinRes.targetParam(), childAlias))
                        : new SqlExpr.BoolLiteral(true);

                SqlBuilder childQuery = new SqlBuilder();
                if (joinRes.isToMany()) {
                    // 1-to-many: SELECT json_group_array(json_object(...))
                    childQuery.addSelect(new SqlExpr.JsonArrayAgg(childJsonObj),
                            dialect.quoteIdentifier("_arr"));
                } else {
                    // 1-to-1: scalar subquery — enforces [0..1] by throwing if >1 row
                    childQuery.addSelect(childJsonObj, dialect.quoteIdentifier("_obj"));
                }
                childQuery.from(dialect.quoteIdentifier(childTable), dialect.quoteIdentifier(childAlias));
                childQuery.addWhere(correlation);

                nestedSubqueries.put(prop.name(), new SqlExpr.Subquery(childQuery));
            } else {
                SqlExpr colExpr = resolveColumnExpr(prop.name(), store, tableAlias);
                source.addSelect(colExpr, dialect.quoteIdentifier(prop.name()));
            }
        }

        // Step 4: Wrap projected query in JSON output (with nested subqueries)
        return wrapJsonOutput(source, spec, info, nestedSubqueries);
    }

    private SqlBuilder generateTableAccess(AppliedFunction af) {
        var store = storeFor(af);
        String tableName = store != null ? store.tableName() : null;
        // Read resolved table name from TypeInfo (stamped by TableReferenceChecker)
        if (tableName == null) {
            TypeInfo tableInfo = unit.typeInfoFor(af);
            if (tableInfo != null && tableInfo.resolvedTableName() != null) {
                tableName = tableInfo.resolvedTableName();
            }
        }
        if (tableName == null) {
            throw new PureCompileException("tableAccess: tableName not resolved for " + af.function());
        }
        String alias = nextTableAlias();
        return new SqlBuilder()
                .selectStar()
                .from(dialect.quoteIdentifier(tableName), dialect.quoteIdentifier(alias));
    }

    /**
     * Generates VALUES SQL for a tds() function call.
     * Reads parsed TdsLiteral from TypeInfo sidecar — no string parsing here.
     */
    private SqlBuilder generateTdsFunction(AppliedFunction af) {
        TypeInfo info = unit.typeInfoFor(af);
        if (info == null || info.tdsLiteral() == null) {
            throw new PureCompileException("tds: parsed TdsLiteral not found in TypeInfo — compiler bug");
        }
        return generateTdsLiteral(info.tdsLiteral());
    }

    /**
     * Generates flat VALUES SQL for a new() function call (struct literal).
     * Extracts InstanceData from param[1] (structural AST access).
     * Type validation already done by TypeChecker.
     */
    private SqlBuilder generateNewFunction(AppliedFunction af) {
        var ci = (ClassInstance) af.parameters().get(1);
        var data = (ValueSpecificationBuilder.InstanceData) ci.value();
        return generateStructLiteral(data);
    }

    // ========== flatten ==========

    /**
     * Generates UNNEST for flatten(~col).
     * Reads the column name from compiler's TypeInfo (columnSpecs).
     * Produces: SELECT * EXCLUDE("col"), UNNEST(CAST("col" AS JSON[])) AS "col" FROM (source) AS t
     */

    // ========== write ==========

    private SqlBuilder generateWrite(AppliedFunction af) {
        SqlBuilder source = generateRelation(af.parameters().get(0));
        return new SqlBuilder()
                .addSelect(new SqlExpr.FunctionCall("COUNT",
                        List.of(new SqlExpr.Star())), dialect.quoteIdentifier("value"))
                .fromSubquery(source, "src");
    }

    /**
     * Generates UNNEST for flatten(~col).
     * Reads the column name from compiler's TypeInfo (columnSpecs).
     * Produces: SELECT * EXCLUDE("col"), UNNEST(CAST("col" AS JSON[])) AS "col" FROM (source) AS t
     */
    private SqlBuilder generateFlatten(AppliedFunction af) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = generateRelation(params.get(0));

        // Read column name from compiler's TypeInfo — not raw AST
        TypeInfo info = unit.typeInfoFor(af);
        if (info == null || info.columnSpecs().isEmpty()) {
            // No flatten column resolved — pass through
            return source;
        }
        String colName = info.columnSpecs().get(0).columnName();
        String quotedCol = dialect.quoteIdentifier(colName);

        // Build UNNEST layer: SELECT * EXCLUDE("col"), UNNEST(CAST("col" AS JSON[])) AS "col"
        //                      FROM (source) AS t
        SqlBuilder unnestLayer = new SqlBuilder()
                .selectStar()
                .addStarExcept(quotedCol)
                .addSelect(
                    new SqlExpr.Unnest(new SqlExpr.VariantArrayCast(
                        new SqlExpr.ColumnRef(colName), "JSON")),
                    quotedCol)
                .fromSubquery(source, "t");

        // Wrap in opaque subquery so downstream extend creates a separate layer
        return new SqlBuilder()
                .selectStar()
                .fromSubquery(unnestLayer, "window_src");
    }

    // ========== filter ==========

    private SqlBuilder generateFilter(AppliedFunction af) {
        var store = storeFor(af);
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = generateRelation(params.get(0));
        LambdaFunction lambda = (LambdaFunction) params.get(1);
        String paramName = lambda.parameters().isEmpty() ? "x" : lambda.parameters().get(0).name();

        // Minimize wrapping: if source is a bare table scan (SELECT * FROM table),
        // we can inline the WHERE clause directly. This is safe because all column
        // references resolve against physical table columns.
        // When source has explicit SELECT columns (projections, computed expressions,
        // M2M expressions, aliases) we MUST wrap in subquery — those aliases don't
        // exist as physical columns on the FROM table.
        // ORDER BY is fine to coexist with WHERE — SQL evaluates WHERE before ORDER BY.
        if (!source.hasSelectColumns()
                && !source.hasGroupBy()
                && source.getFromSubquery() == null
                && source.getFromTable() != null) {
            // Use table-alias-prefixed scalar so all column refs get t0. prefix
            if (source.getFromAlias() == null)
                throw new PureCompileException("PlanGenerator: source has no FROM alias for filter inlining");
            String tableAlias = unquote(source.getFromAlias());
            SqlExpr whereClause = generateScalar(lambda.body().get(0), paramName, store, tableAlias);
            // Resolve association paths → EXISTS subqueries
            if (store != null && store.hasJoins()) {
                whereClause = resolveAssociationRefs(whereClause, store, tableAlias);
            }
            source.addWhere(whereClause);
            return source;
        }
        // Merge consecutive filters: if source is SELECT * FROM subquery with
        // existing WHERE but no GROUP BY/ORDER BY, just AND the new condition
        if (source.isSelectStar() && source.hasWhere()
                && !source.hasGroupBy() && !source.hasOrderBy()
                && !source.hasWindowColumns()) {
            String mergeAlias = source.getFromAlias() != null ? unquote(source.getFromAlias()) : null;
            SqlExpr whereClause = generateScalar(lambda.body().get(0), paramName, store);
            // Resolve association paths → EXISTS subqueries
            if (store != null && mergeAlias != null) {
                whereClause = resolveAssociationRefs(whereClause, store, mergeAlias);
            }
            source.addWhere(whereClause);
            return source;
        }
        // Subquery wrapping: determine store usage from source type.
        // ClassType source (pre-project): filter on class properties → need store for M2M expansion.
        // Relation source (post-project): filter on projected aliases → store=null.
        String filterAlias = source.hasGroupBy() ? "grp" : "ext";
        TypeInfo sourceInfo = unit.types().get(params.get(0));
        boolean isClassSource = sourceInfo != null
                && sourceInfo.type() instanceof GenericType.ClassType;
        SqlExpr whereClause = generateScalar(lambda.body().get(0), paramName,
                isClassSource ? store : null, filterAlias);
        if (store != null) {
            whereClause = resolveAssociationRefs(whereClause, store, filterAlias);
        }
        return new SqlBuilder()
                .selectStar()
                .fromSubquery(source, filterAlias)
                .addWhere(whereClause);
    }

    // ========== project ==========

    private SqlBuilder generateProject(AppliedFunction af) {
        List<ValueSpecification> params = af.parameters();

        var store = storeFor(af);

        // Step 1: Compile the source (getAll, filter, struct collection, etc.)
        SqlBuilder source = generateRelation(params.get(0));

        // Step 2: Determine table alias from source builder
        String tableAlias;
        if (source.getFromAlias() == null) {
            // TDS literal / VALUES source — wrap as subquery
            String subAlias = nextTableAlias();
            source = new SqlBuilder().selectStar().fromSubquery(source,
                    dialect.quoteIdentifier(subAlias));
            tableAlias = subAlias;
        } else {
            tableAlias = unquote(source.getFromAlias());
        }

        // Step 3: Can we inline columns into source, or do we need a subquery?
        // We can inline as long as source is a simple SELECT * (no GROUP BY, no existing WHERE, no JOINs etc.)
        // JOIN sources must be wrapped: columns from left and right sides need a single alias.
        boolean canInline = source.isSelectStar()
                && !source.hasGroupBy()
                && !source.hasJoins();
        SqlBuilder builder;
        if (canInline) {
            // Replace SELECT * with explicit columns
            source.clearSelect();
            builder = source;
        } else {
            // Wrap source as subquery
            tableAlias = "proj_src";
            builder = new SqlBuilder().fromSubquery(source, tableAlias);
        }

        // Track association joins: chainKey → cached alias + resolution
        // chainKey is dotted path: "dept", "dept.org" for multi-hop
        record AssocJoinInfo(String alias, String parentAlias,
                             StoreResolution.JoinResolution joinRes) {
        }
        java.util.Map<String, AssocJoinInfo> assocJoins = new java.util.LinkedHashMap<>();
        // Track which association hops actually need their JOINs emitted.
        // For Otherwise mappings, a hop is only needed if a non-EmbeddedColumn leaf was resolved.
        java.util.Set<String> neededJoins = new java.util.HashSet<>();

        // Step 4: Build SELECT columns from projections (sidecar) + lambda bodies (AST)
        TypeInfo info = unit.types().get(af);
        List<ColSpec> colSpecs = (params.get(1) instanceof ClassInstance ci
                && ci.value() instanceof ColSpecArray(List<ColSpec> specs))
                ? specs : List.of();

        for (int i = 0; i < info.projections().size(); i++) {
            var proj = info.projections().get(i);

            if (proj.isAssociation()) {
                // Association navigation: e.g., associationPath=["dept","org","name"]
                // Walk intermediate hops, adding JOINs for each
                List<String> path = proj.associationPath();
                String leafProp = path.getLast();
                StoreResolution curStore = store;
                String curAlias = tableAlias;

                // Walk all hops except the leaf property
                for (int hop = 0; hop < path.size() - 1; hop++) {
                    String hopProp = path.get(hop);
                    String chainKey = String.join(".", path.subList(0, hop + 1));

                    AssocJoinInfo joinInfo = assocJoins.get(chainKey);
                    if (joinInfo == null) {
                        var joinRes = curStore != null && curStore.hasJoins()
                                ? curStore.joins().get(hopProp) : null;
                        if (joinRes == null) joinRes = findJoinResolution(hopProp);
                        if (joinRes == null) {
                            throw new PureCompileException(
                                    "No join resolution for association '" + hopProp + "' in project");
                        }
                        if (joinRes.embedded()) {
                            // Embedded: no JOIN, sub-properties on parent table
                            assocJoins.put(chainKey, new AssocJoinInfo(curAlias, curAlias, joinRes));
                            curStore = joinRes.targetResolution();
                        } else {
                            String hopAlias = "j" + (assocJoins.size() + 1);
                            assocJoins.put(chainKey, new AssocJoinInfo(hopAlias, curAlias, joinRes));
                            curAlias = hopAlias;
                            curStore = joinRes.targetResolution();
                        }
                    } else {
                        curAlias = joinInfo.alias();
                        curStore = joinInfo.joinRes().targetResolution();
                    }
                }

                SqlExpr colExpr;
                var lastJoin = assocJoins.get(String.join(".", path.subList(0, path.size() - 1)));
                if (lastJoin != null && !lastJoin.joinRes().embedded()
                        && lastJoin.joinRes().joinCondition() == null) {
                    // UNNEST association (struct array): resolve from unnested element
                    String elemAlias = curAlias + "_elem";
                    colExpr = resolveColumnExpr(leafProp, curStore, elemAlias);
                } else if (lastJoin != null && !lastJoin.joinRes().embedded()
                        && curStore != null && curStore.resolveProperty(leafProp)
                            instanceof StoreResolution.PropertyResolution.EmbeddedColumn emb) {
                    // Otherwise: embedded sub-property resolves from parent table, not join
                    colExpr = new SqlExpr.Column(lastJoin.parentAlias(), emb.columnName());
                } else {
                    // Leaf-join check: if the leaf property itself requires a join
                    // (traverse-extend on the target class), add it before resolving
                    var leafJoinRes = curStore != null && curStore.hasJoins()
                            ? curStore.joins().get(leafProp) : null;
                    if (leafJoinRes != null && !leafJoinRes.embedded()
                            && leafJoinRes.joinCondition() != null) {
                        String leafChainKey = String.join(".", path);
                        AssocJoinInfo leafJoinInfo = assocJoins.get(leafChainKey);
                        if (leafJoinInfo == null) {
                            String leafAlias = "j" + (assocJoins.size() + 1);
                            assocJoins.put(leafChainKey, new AssocJoinInfo(leafAlias, curAlias, leafJoinRes));
                            neededJoins.add(leafChainKey);
                            curAlias = leafAlias;
                            curStore = leafJoinRes.targetResolution();
                        } else {
                            curAlias = leafJoinInfo.alias();
                            curStore = leafJoinRes.targetResolution();
                        }
                    }
                    colExpr = resolveColumnExpr(leafProp, curStore, curAlias);
                    // Non-embedded leaf accessed — mark all hops in chain as needed
                    for (int hop = 0; hop < path.size() - 1; hop++) {
                        neededJoins.add(String.join(".", path.subList(0, hop + 1)));
                    }
                }
                builder.addSelect(colExpr, dialect.quoteIdentifier(proj.alias()));
            } else {
                // Non-association: walk the ColSpec lambda body directly
                ColSpec cs = colSpecs.get(i);
                LambdaFunction lambda = cs.function1();
                if (lambda != null && !lambda.body().isEmpty()) {
                    String paramName = lambda.parameters().isEmpty() ? "x"
                            : lambda.parameters().get(0).name();
                    SqlExpr colExpr = generateScalar(lambda.body().get(0), paramName, store, tableAlias);

                    // Resolve any AssociationRef markers into LEFT JOINs on the outer query.
                    // This handles computed expressions like $o.customer.name + $o.product.name
                    // where multiple association paths appear in a single scalar expression.
                    SqlExpr.AssociationRef ref;
                    while ((ref = findAssociationRef(colExpr)) != null && store != null) {
                        var hops = ref.hops();
                        StoreResolution curStore = store;
                        String curAlias = tableAlias;

                        // Walk hops, adding LEFT JOINs via assocJoins (same as association branch)
                        for (String hop : hops) {
                            String chainKey = "scalar_" + hop + "_" + curAlias;
                            AssocJoinInfo joinInfo = assocJoins.get(chainKey);
                            if (joinInfo == null) {
                                var joinRes = curStore.hasJoins()
                                        ? curStore.joins().get(hop) : null;
                                if (joinRes == null) joinRes = findJoinResolution(hop);
                                if (joinRes == null) break;
                                if (joinRes.embedded()) {
                                    assocJoins.put(chainKey, new AssocJoinInfo(curAlias, curAlias, joinRes));
                                    curStore = joinRes.targetResolution();
                                } else {
                                    String hopAlias = "j" + (assocJoins.size() + 1);
                                    assocJoins.put(chainKey, new AssocJoinInfo(hopAlias, curAlias, joinRes));
                                    neededJoins.add(chainKey);
                                    curAlias = hopAlias;
                                    curStore = joinRes.targetResolution();
                                }
                            } else {
                                curAlias = joinInfo.alias();
                                curStore = joinInfo.joinRes().targetResolution();
                            }
                        }

                        // Leaf-join check on the target column
                        var leafJoinRes = curStore != null && curStore.hasJoins()
                                ? curStore.joins().get(ref.targetCol()) : null;
                        if (leafJoinRes != null && !leafJoinRes.embedded()
                                && leafJoinRes.joinCondition() != null) {
                            String leafKey = "scalar_leaf_" + ref.targetCol() + "_" + curAlias;
                            AssocJoinInfo leafInfo = assocJoins.get(leafKey);
                            if (leafInfo == null) {
                                String leafAlias = "j" + (assocJoins.size() + 1);
                                assocJoins.put(leafKey, new AssocJoinInfo(leafAlias, curAlias, leafJoinRes));
                                neededJoins.add(leafKey);
                                curAlias = leafAlias;
                                curStore = leafJoinRes.targetResolution();
                            } else {
                                curAlias = leafInfo.alias();
                                curStore = leafJoinRes.targetResolution();
                            }
                        }

                        String physCol = curStore != null ? curStore.columnFor(ref.targetCol()) : null;
                        if (physCol == null) physCol = ref.targetCol();
                        colExpr = replaceAssociationRef(colExpr, ref, curAlias, physCol);
                    }

                    builder.addSelect(colExpr, dialect.quoteIdentifier(proj.alias()));
                } else {
                    // Bare ColSpec (~prop) — treat as column reference
                    SqlExpr colExpr = resolveColumnExpr(proj.alias(), store, tableAlias);
                    builder.addSelect(colExpr, dialect.quoteIdentifier(proj.alias()));
                }
            }
        }

        // Step 5: Add JOINs for association projections
        for (var entry : assocJoins.entrySet()) {
            AssocJoinInfo joinInfo = entry.getValue();
            var joinRes = joinInfo.joinRes();
            if (joinRes.embedded()) {
                // Embedded: no JOIN needed — sub-properties resolve from parent alias
                continue;
            } else if (joinRes.joinCondition() != null) {
                if (!neededJoins.contains(entry.getKey())) {
                    // Otherwise: all leaves for this hop resolved to EmbeddedColumn — skip JOIN
                    continue;
                }
                // Regular FK join — full condition rendered via generateScalar + varAliases
                String targetTableName = joinRes.targetTable();
                SqlExpr onCondition = generateScalar(joinRes.joinCondition(), null, null, null,
                        Map.of(joinRes.sourceParam(), joinInfo.parentAlias(), joinRes.targetParam(), joinInfo.alias()));
                builder.addJoin(SqlBuilder.JoinType.LEFT,
                        dialect.quoteIdentifier(targetTableName),
                        dialect.quoteIdentifier(joinInfo.alias()), onCondition);
            } else {
                // UNNEST for inline array properties (struct literal arrays)
                String arrayProp = entry.getKey();
                SqlExpr arrayRef = resolveColumnExpr(arrayProp, store, tableAlias);
                String elemAlias = joinInfo.alias() + "_elem";

                SqlBuilder unnestSubquery = new SqlBuilder()
                        .addSelect(new SqlExpr.Unnest(arrayRef), null);
                String compoundAlias = joinInfo.alias() + "(" + elemAlias + ")";
                builder.addJoin(new SqlBuilder.JoinClause(
                        SqlBuilder.JoinType.LEFT_LATERAL, null, compoundAlias,
                        unnestSubquery, new SqlExpr.BoolLiteral(true), null));
            }
        }

        return builder;
    }



    /**
     * Resolves a property to its column expression via StoreResolution.
     * Pattern-matches on PropertyResolution — no ClassMapping dispatch.
     */
    private SqlExpr resolveColumnExpr(String propertyName, StoreResolution store, String alias) {
        if (store == null || propertyName == null) {
            // No store (TDS literal, bare relation) — use property name as column directly
            return alias != null ? new SqlExpr.Column(alias, propertyName) : new SqlExpr.ColumnRef(propertyName);
        }
        var resolution = store.resolveProperty(propertyName);
        if (resolution == null) {
            // Property not in store — use propertyToColumn or name directly
            String col = store.columnFor(propertyName);
            String name = col != null ? col : propertyName;
            return alias != null ? new SqlExpr.Column(alias, name) : new SqlExpr.ColumnRef(name);
        }
        return switch (resolution) {
            case StoreResolution.PropertyResolution.Column col ->
                    alias != null ? new SqlExpr.Column(alias, col.columnName())
                            : new SqlExpr.ColumnRef(col.columnName());
            case StoreResolution.PropertyResolution.DynaFunction dyna ->
                    generateScalar(dyna.expression(), null, store, alias);
            case StoreResolution.PropertyResolution.EmbeddedColumn emb ->
                    alias != null ? new SqlExpr.Column(alias, emb.columnName())
                            : new SqlExpr.ColumnRef(emb.columnName());
        };
    }

    /**
     * Resolves AssociationRef markers in an expression tree into EXISTS subqueries.
     * Called by generateFilter after scalar compilation.
     *
     * Walks the SqlExpr tree. For any expression that contains an AssociationRef:
     * 1. Replaces AssociationRef with Column(subAlias, targetCol)
     * 2. Wraps the containing expression in EXISTS with join correlation
     */
    private SqlExpr resolveAssociationRefs(SqlExpr expr, StoreResolution store, String tableAlias) {
        // Check if this expression tree contains an AssociationRef
        SqlExpr.AssociationRef ref = findAssociationRef(expr);
        if (ref == null) {
            // Recurse into AND/OR to handle mixed predicates
            if (expr instanceof SqlExpr.And(List<SqlExpr> conditions)) {
                var resolved = conditions.stream()
                        .map(c -> resolveAssociationRefs(c, store, tableAlias))
                        .toList();
                return new SqlExpr.And(resolved);
            }
            if (expr instanceof SqlExpr.Or(List<SqlExpr> conditions)) {
                var resolved = conditions.stream()
                        .map(c -> resolveAssociationRefs(c, store, tableAlias))
                        .toList();
                return new SqlExpr.Or(resolved);
            }
            if (expr instanceof SqlExpr.Not(SqlExpr inner)) {
                return new SqlExpr.Not(resolveAssociationRefs(inner, store, tableAlias));
            }
            if (expr instanceof SqlExpr.Grouped(SqlExpr inner)) {
                return new SqlExpr.Grouped(resolveAssociationRefs(inner, store, tableAlias));
            }
            return expr;
        }

        // Found AssociationRef — resolve based on join type
        var hops = ref.hops();
        StoreResolution currentStore = store;
        String currentAlias = tableAlias;

        // Resolve first hop
        var firstJoinRes = currentStore.hasJoins() ? currentStore.joins().get(hops.get(0)) : null;
        if (firstJoinRes == null) firstJoinRes = findJoinResolution(hops.get(0));
        if (firstJoinRes == null) {
            return expr; // Can't resolve — pass through
        }

        // Embedded: resolve directly to Column(tableAlias, physicalCol) — no EXISTS
        if (firstJoinRes.embedded()) {
            StoreResolution embeddedStore = firstJoinRes.targetResolution();
            String physicalCol = embeddedStore != null ? embeddedStore.columnFor(ref.targetCol()) : null;
            if (physicalCol == null) physicalCol = ref.targetCol();
            return replaceAssociationRef(expr, ref, tableAlias, physicalCol);
        }

        // Otherwise: if targetCol resolves to EmbeddedColumn, use parent alias directly
        if (firstJoinRes.targetResolution() != null) {
            var propRes = firstJoinRes.targetResolution().resolveProperty(ref.targetCol());
            if (propRes instanceof StoreResolution.PropertyResolution.EmbeddedColumn emb) {
                return replaceAssociationRef(expr, ref, tableAlias, emb.columnName());
            }
        }

        if (firstJoinRes.joinCondition() == null) {
            return expr; // Can't resolve — pass through
        }

        String firstSubAlias = "sub" + (tableAliasCounter++);
        String firstTargetTable = firstJoinRes.targetTable();

        // Correlation: first hop — join condition rendered via generateScalar + varAliases
        SqlExpr correlation = generateScalar(firstJoinRes.joinCondition(), null, null, null,
                Map.of(firstJoinRes.sourceParam(), currentAlias, firstJoinRes.targetParam(), firstSubAlias));

        SqlBuilder subquery = new SqlBuilder()
                .addSelect(new SqlExpr.NumericLiteral(1), null)
                .from(dialect.quoteIdentifier(firstTargetTable), dialect.quoteIdentifier(firstSubAlias));

        // Add intermediate JOINs for hops beyond the first
        String leafAlias = firstSubAlias;
        StoreResolution hopStore = firstJoinRes.targetResolution();
        for (int i = 1; i < hops.size(); i++) {
            String hop = hops.get(i);
            var hopJoinRes = hopStore != null && hopStore.hasJoins()
                    ? hopStore.joins().get(hop) : null;
            if (hopJoinRes == null || hopJoinRes.joinCondition() == null) break;

            String hopAlias = "sub" + (tableAliasCounter++);
            String hopTable = hopJoinRes.targetTable();

            // JOIN intermediate table — join condition via generateScalar + varAliases
            SqlExpr joinCond = generateScalar(hopJoinRes.joinCondition(), null, null, null,
                    Map.of(hopJoinRes.sourceParam(), leafAlias, hopJoinRes.targetParam(), hopAlias));
            subquery.addJoin(SqlBuilder.JoinType.INNER, dialect.quoteIdentifier(hopTable),
                    dialect.quoteIdentifier(hopAlias), joinCond);

            leafAlias = hopAlias;
            hopStore = hopJoinRes.targetResolution();
        }

        // Leaf-join check: if the target column itself requires a join
        // (traverse-extend on the target class), add INNER JOIN before resolving
        var leafJoinRes = hopStore != null && hopStore.hasJoins()
                ? hopStore.joins().get(ref.targetCol()) : null;
        if (leafJoinRes != null && !leafJoinRes.embedded()
                && leafJoinRes.joinCondition() != null) {
            String ljAlias = "sub" + (tableAliasCounter++);
            SqlExpr ljCond = generateScalar(leafJoinRes.joinCondition(), null, null, null,
                    Map.of(leafJoinRes.sourceParam(), leafAlias, leafJoinRes.targetParam(), ljAlias));
            subquery.addJoin(SqlBuilder.JoinType.INNER, dialect.quoteIdentifier(leafJoinRes.targetTable()),
                    dialect.quoteIdentifier(ljAlias), ljCond);
            leafAlias = ljAlias;
            hopStore = leafJoinRes.targetResolution();
        }

        // Resolve Pure property name → physical column via leaf store
        String physicalCol = hopStore != null ? hopStore.columnFor(ref.targetCol()) : null;
        if (physicalCol == null) physicalCol = ref.targetCol();

        // Replace AssociationRef → Column(leafAlias, physicalCol) in the expression
        SqlExpr resolved = replaceAssociationRef(expr, ref, leafAlias, physicalCol);

        subquery.addWhere(new SqlExpr.And(List.of(correlation, resolved)));
        return new SqlExpr.Exists(subquery);
    }

    /** Finds the first AssociationRef in an expression tree, or null. */
    private SqlExpr.AssociationRef findAssociationRef(SqlExpr expr) {
        if (expr instanceof SqlExpr.AssociationRef ref) return ref;
        if (expr instanceof SqlExpr.Binary(SqlExpr l, String op, SqlExpr r)) {
            var found = findAssociationRef(l);
            return found != null ? found : findAssociationRef(r);
        }
        if (expr instanceof SqlExpr.FunctionCall(String name, List<SqlExpr> args)) {
            for (var arg : args) {
                var found = findAssociationRef(arg);
                if (found != null) return found;
            }
        }
        if (expr instanceof SqlExpr.Grouped(SqlExpr inner)) return findAssociationRef(inner);
        if (expr instanceof SqlExpr.Not(SqlExpr inner)) return findAssociationRef(inner);
        if (expr instanceof SqlExpr.IsNull(SqlExpr inner)) return findAssociationRef(inner);
        if (expr instanceof SqlExpr.IsNotNull(SqlExpr inner)) return findAssociationRef(inner);
        if (expr instanceof SqlExpr.StartsWith(SqlExpr s, SqlExpr p)) {
            var found = findAssociationRef(s);
            return found != null ? found : findAssociationRef(p);
        }
        if (expr instanceof SqlExpr.EndsWith(SqlExpr s, SqlExpr p)) {
            var found = findAssociationRef(s);
            return found != null ? found : findAssociationRef(p);
        }
        return null;
    }

    /** Replaces an AssociationRef with Column(subAlias, physicalCol) in an expression tree. */
    private SqlExpr replaceAssociationRef(SqlExpr expr, SqlExpr.AssociationRef ref,
                                          String subAlias, String physicalCol) {
        if (expr instanceof SqlExpr.AssociationRef r && r.equals(ref)) {
            return new SqlExpr.Column(subAlias, physicalCol);
        }
        if (expr instanceof SqlExpr.Binary(SqlExpr l, String op, SqlExpr r)) {
            return new SqlExpr.Binary(replaceAssociationRef(l, ref, subAlias, physicalCol), op,
                    replaceAssociationRef(r, ref, subAlias, physicalCol));
        }
        if (expr instanceof SqlExpr.FunctionCall(String name, List<SqlExpr> args)) {
            var replaced = args.stream()
                    .map(a -> replaceAssociationRef(a, ref, subAlias, physicalCol)).toList();
            return new SqlExpr.FunctionCall(name, replaced);
        }
        if (expr instanceof SqlExpr.Grouped(SqlExpr inner)) {
            return new SqlExpr.Grouped(replaceAssociationRef(inner, ref, subAlias, physicalCol));
        }
        if (expr instanceof SqlExpr.Not(SqlExpr inner)) {
            return new SqlExpr.Not(replaceAssociationRef(inner, ref, subAlias, physicalCol));
        }
        if (expr instanceof SqlExpr.StartsWith(SqlExpr s, SqlExpr p)) {
            return new SqlExpr.StartsWith(replaceAssociationRef(s, ref, subAlias, physicalCol),
                    replaceAssociationRef(p, ref, subAlias, physicalCol));
        }
        if (expr instanceof SqlExpr.EndsWith(SqlExpr s, SqlExpr p)) {
            return new SqlExpr.EndsWith(replaceAssociationRef(s, ref, subAlias, physicalCol),
                    replaceAssociationRef(p, ref, subAlias, physicalCol));
        }
        return expr;
    }

    /**
     * Resolves the RelationalMapping for a relation expression by finding the
     * getAll class.
     */

    // ========== sort ==========

    private SqlBuilder generateSort(AppliedFunction af) {
        SqlBuilder source = generateRelation(af.parameters().get(0));

        // Read pre-resolved sort specs from sidecar
        TypeInfo info = unit.types().get(af);
        List<TypeInfo.SortSpec> sortSpecs = info.sortSpecs();
        var store = storeFor(af);

        // Resolve table alias for generateScalar (must be unquoted)
        String tableAlias = unquote(source.getFromAlias());

        // Minimize wrapping: if source has no ORDER BY, add directly.
        // CRITICAL: UNION ALL and PIVOT must be wrapped — they render with
        // completely different syntax that can't have ORDER BY appended.
        SqlBuilder target = source;
        if (source.hasOrderBy() || source.hasSetOperation() || source.hasPivot()) {
            target = new SqlBuilder()
                    .selectStar()
                    .fromSubquery(source, "sort_src");
            tableAlias = "sort_src";
        }

        for (var spec : sortSpecs) {
            SqlBuilder.SortDirection dir = spec.direction() == TypeInfo.SortDirection.ASC
                    ? SqlBuilder.SortDirection.ASC
                    : SqlBuilder.SortDirection.DESC;
            SqlExpr colExpr;
            if (spec.column() == null) {
                // Collection sort: key lambda lives in the AST, not SortSpec.
                // Extract from params[1] — same pattern as filter/project/extend.
                colExpr = extractKeyExprFromAST(af, store, tableAlias);
            } else {
                // Relation sort: column name is already the SQL column name
                colExpr = new SqlExpr.ColumnRef(spec.column());
            }
            target.addOrderBy(colExpr, dir, SqlBuilder.NullsPosition.DEFAULT);
        }

        return target;
    }

    /**
     * Extracts and compiles the key lambda expression from a sort/sortBy/sortByReversed AST.
     * Reads the lambda body from params[1], compiles it through mapping — same pattern
     * as generateFilter reads its predicate from the AST.
     *
     * <p>Falls back to a no-op (first column) if no key lambda is present (natural sort).
     */
    private SqlExpr extractKeyExprFromAST(AppliedFunction af, StoreResolution store, String tableAlias) {
        List<ValueSpecification> params = af.parameters();
        String funcName = simpleName(af.function());

        // Find the key lambda: for sort(col, key, comp) it's the 1-param lambda
        // For sortBy/sortByReversed it's always params[1]
        LambdaFunction keyLambda = null;
        if ("sortBy".equals(funcName) || "sortByReversed".equals(funcName)) {
            if (params.size() > 1 && params.get(1) instanceof LambdaFunction lf) {
                keyLambda = lf;
            }
        } else {
            // sort(col, key?, comp?) — key is the 1-param lambda
            for (int i = 1; i < params.size(); i++) {
                if (params.get(i) instanceof LambdaFunction lf && lf.parameters().size() == 1) {
                    keyLambda = lf;
                    break;
                }
            }
        }

        if (keyLambda == null || keyLambda.body().isEmpty()) {
            // Natural sort — no key function, pass-through
            return new SqlExpr.NumericLiteral(1); // ORDER BY 1 (position)
        }

        String paramName = keyLambda.parameters().get(0).name();
        SqlExpr colExpr = generateScalar(keyLambda.body().get(0), paramName, store, tableAlias);

        // Resolve AssociationRef → correlated scalar subquery
        // Filter uses EXISTS (boolean), sort needs the actual value
        SqlExpr.AssociationRef ref = findAssociationRef(colExpr);
        if (ref != null && store != null) {
            var hops = ref.hops();
            // Resolve first hop
            var firstJoinRes = findJoinResolution(hops.get(0));
            if (firstJoinRes != null && firstJoinRes.joinCondition() != null) {
                String firstSubAlias = "sort_j" + (tableAliasCounter++);
                String firstTargetTable = firstJoinRes.targetTable();

                // Correlation: join condition via generateScalar + varAliases
                SqlExpr correlation = generateScalar(firstJoinRes.joinCondition(), null, null, null,
                        Map.of(firstJoinRes.sourceParam(), tableAlias, firstJoinRes.targetParam(), firstSubAlias));
                SqlBuilder subquery = new SqlBuilder()
                        .from(dialect.quoteIdentifier(firstTargetTable),
                                dialect.quoteIdentifier(firstSubAlias))
                        .addWhere(correlation);

                // Add intermediate JOINs for multi-hop
                String leafAlias = firstSubAlias;
                StoreResolution hopStore = firstJoinRes.targetResolution();
                for (int i = 1; i < hops.size(); i++) {
                    String hop = hops.get(i);
                    var hopJoinRes = hopStore != null && hopStore.hasJoins()
                            ? hopStore.joins().get(hop) : null;
                    if (hopJoinRes == null || hopJoinRes.joinCondition() == null) break;

                    String hopAlias = "sort_j" + (tableAliasCounter++);
                    SqlExpr joinCond = generateScalar(hopJoinRes.joinCondition(), null, null, null,
                            Map.of(hopJoinRes.sourceParam(), leafAlias, hopJoinRes.targetParam(), hopAlias));
                    subquery.addJoin(SqlBuilder.JoinType.INNER, dialect.quoteIdentifier(hopJoinRes.targetTable()),
                            dialect.quoteIdentifier(hopAlias), joinCond);
                    leafAlias = hopAlias;
                    hopStore = hopJoinRes.targetResolution();
                }

                // Leaf-join check: if the target column itself requires a join
                var leafJoinRes = hopStore != null && hopStore.hasJoins()
                        ? hopStore.joins().get(ref.targetCol()) : null;
                if (leafJoinRes != null && !leafJoinRes.embedded()
                        && leafJoinRes.joinCondition() != null) {
                    String ljAlias = "sort_j" + (tableAliasCounter++);
                    SqlExpr ljCond = generateScalar(leafJoinRes.joinCondition(), null, null, null,
                            Map.of(leafJoinRes.sourceParam(), leafAlias, leafJoinRes.targetParam(), ljAlias));
                    subquery.addJoin(SqlBuilder.JoinType.INNER, dialect.quoteIdentifier(leafJoinRes.targetTable()),
                            dialect.quoteIdentifier(ljAlias), ljCond);
                    leafAlias = ljAlias;
                    hopStore = leafJoinRes.targetResolution();
                }

                // Resolve Pure property name → physical column via leaf store
                String sortPhysicalCol = hopStore != null ? hopStore.columnFor(ref.targetCol()) : null;
                if (sortPhysicalCol == null) sortPhysicalCol = ref.targetCol();
                subquery.addSelect(new SqlExpr.Column(leafAlias, sortPhysicalCol), null);
                colExpr = new SqlExpr.Subquery(subquery);
            }
        }
        return colExpr;
    }

    // ========== limit / drop / slice / first ==========

    private SqlBuilder generateLimit(AppliedFunction af) {
        SqlBuilder source = generateRelation(af.parameters().get(0));
        long count = extractIntValue(af.parameters().get(1));

        // Minimize wrapping: if source has no limit, just add it
        if (!source.hasLimit()) {
            source.limit((int) count);
            return source;
        }
        return new SqlBuilder()
                .selectStar()
                .fromSubquery(source, "limit_src")
                .limit((int) count);
    }

    private SqlBuilder generateDrop(AppliedFunction af) {
        SqlBuilder source = generateRelation(af.parameters().get(0));
        long count = extractIntValue(af.parameters().get(1));

        if (!source.hasOffset()) {
            source.offset((int) count);
            return source;
        }
        return new SqlBuilder()
                .selectStar()
                .fromSubquery(source, "limit_src")
                .offset((int) count);
    }

    private SqlBuilder generateSlice(AppliedFunction af) {
        SqlBuilder source = generateRelation(af.parameters().get(0));
        long start = extractIntValue(af.parameters().get(1));
        long end = extractIntValue(af.parameters().get(2));

        if (!source.hasLimit() && !source.hasOffset()) {
            source.limit((int) (end - start));
            if (start > 0)
                source.offset((int) start);
            return source;
        }
        SqlBuilder wrapper = new SqlBuilder()
                .selectStar()
                .fromSubquery(source, "limit_src")
                .limit((int) (end - start));
        if (start > 0)
            wrapper.offset((int) start);
        return wrapper;
    }

    private SqlBuilder generateFirst(AppliedFunction af) {
        SqlBuilder source = generateRelation(af.parameters().get(0));
        if (!source.hasLimit()) {
            source.limit(1);
            return source;
        }
        return new SqlBuilder()
                .selectStar()
                .fromSubquery(source, "limit_src")
                .limit(1);
    }

    /**
     * find(set, pred) → filter(set, pred)->first()
     * Returns the first element matching the predicate.
     */
    private SqlBuilder generateFind(AppliedFunction af) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = generateRelation(params.get(0));
        LambdaFunction lambda = (LambdaFunction) params.get(1);
        String paramName = lambda.parameters().isEmpty() ? "x" : lambda.parameters().get(0).name();

        // Apply filter predicate as WHERE clause
        SqlExpr whereClause = generateScalar(lambda.body().get(0), paramName, null);
        SqlBuilder filtered = new SqlBuilder()
                .selectStar()
                .fromSubquery(source, "find_src")
                .addWhere(whereClause)
                .limit(1);
        return filtered;
    }

    // ========== distinct ==========

    private SqlBuilder generateDistinct(AppliedFunction af) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = generateRelation(params.get(0));

        // Check if distinct specifies particular columns
        TypeInfo info = unit.types().get(af);
        if (info != null && !info.columnSpecs().isEmpty()) {
            // Project specific columns: SELECT DISTINCT "col1", "col2" FROM (source) AS distinct_src
            SqlBuilder builder = new SqlBuilder()
                    .distinct()
                    .fromSubquery(source, "distinct_src");
            for (var cs : info.columnSpecs()) {
                builder.addSelect(new SqlExpr.ColumnRef(cs.columnName()), null);
            }
            return builder;
        }

        // No explicit columns — inline DISTINCT into source when possible
        if (!source.hasGroupBy() && !source.hasOrderBy() && !source.hasLimit()) {
            source.distinct();
            return source;
        }
        return new SqlBuilder()
                .selectStar()
                .distinct()
                .fromSubquery(source, "distinct_src");
    }

    // ========== select ==========

    private SqlBuilder generateSelect(AppliedFunction af) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = generateRelation(params.get(0));

        TypeInfo info = unit.types().get(af);
        // select() with no column specs = select all, pass through source directly
        if (info.columnSpecs().isEmpty()) {
            return source;
        }
        // If selecting all columns from source, also pass through
        TypeInfo sourceInfo = unit.types().get(params.get(0));
        if (sourceInfo != null && sourceInfo.schema() != null
                && info.columnSpecs().size() == sourceInfo.schema().columns().size()) {
            boolean allMatch = info.columnSpecs().stream()
                    .allMatch(cs -> sourceInfo.schema().columns().containsKey(cs.columnName()));
            if (allMatch) {
                return source;
            }
        }

        // If source is a simple star-select (no GROUP/ORDER/LIMIT/window),
        // inline column projections directly instead of wrapping in a subquery.
        // WHERE is safe — SELECT cols FROM table WHERE cond is valid SQL.
        if (source.isSelectStar() && !source.hasSelectColumns()
                && !source.hasGroupBy()
                && !source.hasOrderBy() && !source.hasLimit()
                && !source.hasWindowColumns()) {
            source.clearSelect();
            for (var cs : info.columnSpecs()) {
                source.addSelect(new SqlExpr.ColumnRef(cs.columnName()), dialect.quoteIdentifier(cs.columnName()));
            }
            return source;
        }

        SqlBuilder builder = new SqlBuilder()
                .fromSubquery(source, "subq");

        for (var cs : info.columnSpecs()) {
            builder.addSelect(new SqlExpr.ColumnRef(cs.columnName()), dialect.quoteIdentifier(cs.columnName()));
        }
        return builder;
    }

    // ========== rename ==========

    private SqlBuilder generateRename(AppliedFunction af) {
        SqlBuilder source = generateRelation(af.parameters().get(0));

        TypeInfo info = unit.types().get(af);

        // Build SELECT using EXCLUDE + rename pattern for each rename pair
        SqlBuilder builder = new SqlBuilder().fromSubquery(source, "rename_src");
        builder.selectStar();
        for (var renameSpec : info.columnSpecs()) {
            String oldName = renameSpec.columnName();
            String newName = renameSpec.alias();
            builder.addStarExcept(dialect.quoteIdentifier(oldName));
            builder.addSelect(new SqlExpr.ColumnRef(oldName), dialect.quoteIdentifier(newName));
        }
        return builder;
    }

    // ========== concatenate / union ==========

    private SqlBuilder generateConcatenate(AppliedFunction af) {
        SqlBuilder left = generateRelation(af.parameters().get(0));
        SqlBuilder right = generateRelation(af.parameters().get(1));
        left.unionWrapped(right, true);
        return left;
    }

    // ========== groupBy ==========

    private SqlBuilder generateGroupBy(AppliedFunction af) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = generateRelation(params.get(0));
        TypeInfo info = unit.types().get(af);

        boolean classSource = info.projections() != null && !info.projections().isEmpty();
        StoreResolution store = classSource ? storeFor(af) : null;
        String tableAlias;
        SqlBuilder builder;

        // Class source: inline into source table if possible; otherwise subquery-wrap
        if (classSource && source.isSelectStar() && !source.hasGroupBy() && !source.hasJoins()) {
            tableAlias = unquote(source.getFromAlias());
            source.clearSelect();
            builder = source;
        } else {
            tableAlias = "groupby_src";
            builder = new SqlBuilder().fromSubquery(source, tableAlias);
        }

        // Track association joins for class-source (same pattern as generateProject)
        record AssocJoinInfo(String alias, String parentAlias,
                             StoreResolution.JoinResolution joinRes) {}
        java.util.Map<String, AssocJoinInfo> assocJoins = new java.util.LinkedHashMap<>();

        // Key columns — from projections (class source) or columnSpecs (Relation)
        if (classSource) {
            for (var proj : info.projections()) {
                if (proj.isAssociation()) {
                    // Association navigation: walk hops and add JOINs (same as generateProject)
                    List<String> path = proj.associationPath();
                    String leafProp = path.getLast();
                    StoreResolution curStore = store;
                    String curAlias = tableAlias;

                    for (int hop = 0; hop < path.size() - 1; hop++) {
                        String hopProp = path.get(hop);
                        String chainKey = String.join(".", path.subList(0, hop + 1));
                        AssocJoinInfo joinInfo = assocJoins.get(chainKey);
                        if (joinInfo == null) {
                            var joinRes = curStore != null && curStore.hasJoins()
                                    ? curStore.joins().get(hopProp) : null;
                            if (joinRes == null) joinRes = findJoinResolution(hopProp);
                            if (joinRes == null) {
                                throw new PureCompileException(
                                        "No join resolution for association '" + hopProp + "' in groupBy");
                            }
                            if (joinRes.embedded()) {
                                assocJoins.put(chainKey, new AssocJoinInfo(curAlias, curAlias, joinRes));
                                curStore = joinRes.targetResolution();
                            } else {
                                String hopAlias = "j" + (assocJoins.size() + 1);
                                assocJoins.put(chainKey, new AssocJoinInfo(hopAlias, curAlias, joinRes));
                                curAlias = hopAlias;
                                curStore = joinRes.targetResolution();
                            }
                        } else {
                            curAlias = joinInfo.alias();
                            curStore = joinInfo.joinRes().targetResolution();
                        }
                    }
                    SqlExpr colExpr = resolveColumnExpr(leafProp, curStore, curAlias);
                    builder.addSelect(colExpr, dialect.quoteIdentifier(proj.alias()));
                    builder.addGroupBy(colExpr);
                } else {
                    SqlExpr colExpr = resolveColumnExpr(
                            proj.associationPath().getLast(), store, tableAlias);
                    builder.addSelect(colExpr, dialect.quoteIdentifier(proj.alias()));
                    builder.addGroupBy(colExpr);
                }
            }
        } else {
            for (var cs : info.columnSpecs()) {
                SqlExpr colRef = new SqlExpr.ColumnRef(cs.columnName());
                builder.addSelect(colRef, null);
                builder.addGroupBy(colRef);
            }
        }

        // Emit LEFT JOINs for association hops (same pattern as generateProject Step 5)
        for (var entry : assocJoins.entrySet()) {
            var ji = entry.getValue();
            if (ji.joinRes().embedded()) {
                continue;
            }
            if (ji.joinRes().joinCondition() != null) {
                String targetTableName = ji.joinRes().targetTable();
                SqlExpr onCondition = generateScalar(ji.joinRes().joinCondition(), null, null, null,
                        Map.of(ji.joinRes().sourceParam(), ji.parentAlias(),
                               ji.joinRes().targetParam(), ji.alias()));
                builder.addJoin(SqlBuilder.JoinType.LEFT,
                        dialect.quoteIdentifier(targetTableName),
                        dialect.quoteIdentifier(ji.alias()), onCondition);
            }
        }

        // Agg columns — unified (store is null for Relation path, non-null for class)
        List<ColSpec> astSpecs = com.gs.legend.compiler.checkers.GroupByChecker
                .extractAggColSpecs(params.get(2));
        for (int i = 0; i < info.aggColumnSpecs().size(); i++) {
            var acs = info.aggColumnSpecs().get(i);
            SqlExpr aggExpr = generateAggFromAst(acs, astSpecs.get(i), store, tableAlias);
            if (acs.castType() != null)
                aggExpr = new SqlExpr.Cast(aggExpr, acs.castType().typeName());
            builder.addSelect(aggExpr, dialect.quoteIdentifier(acs.alias()));
        }

        return builder;
    }

    private SqlBuilder generateAggregate(AppliedFunction af) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = generateRelation(params.get(0));

        SqlBuilder builder = new SqlBuilder()
                .fromSubquery(source, "agg_src");

        TypeInfo info = unit.types().get(af);
        List<ColSpec> astSpecs = com.gs.legend.compiler.checkers.GroupByChecker
                .extractAggColSpecs(params.get(1));
        for (int i = 0; i < info.aggColumnSpecs().size(); i++) {
            var acs = info.aggColumnSpecs().get(i);
            ColSpec ast = astSpecs.get(i);

            SqlExpr aggExpr = generateAggFromAst(acs, ast, null, null);
            if (acs.castType() != null)
                aggExpr = new SqlExpr.Cast(aggExpr, acs.castType().typeName());
            builder.addSelect(aggExpr, dialect.quoteIdentifier(acs.alias()));
        }

        return builder;
    }

    // ========== Aggregate SQL from AST + resolved function ==========

    /**
     * Generates SQL for an aggregate column by reading fn1/fn2 from the AST.
     * Dispatches on resolved function identity (not strings).
     */
    private SqlExpr generateAggFromAst(TypeInfo.AggColumnSpec acs, ColSpec ast,
                                        StoreResolution store, String tableAlias) {
        var registry = com.gs.legend.compiler.BuiltinFunctionRegistry.instance();
        var fn1Body = ast.function1().body().get(0);
        // For window context, fn1 has 3 params {p,w,r|...}: use last param (row variable)
        // For groupBy/aggregate, fn1 has 1 param {c|...}: use first (only) param
        var fn1Params = ast.function1().parameters();
        String fn1Param = fn1Params.isEmpty() ? null
                : fn1Params.get(fn1Params.size() - 1).name();

        // Detect rowMapper pattern in fn1: rowMapper($x.col1, $x.col2)
        // Used by wavg, corr, covarSample, covarPopulation, maxBy, minBy
        if (fn1Body instanceof AppliedFunction fn1Af
                && isRowMapperFunc(fn1Af, registry)) {
            SqlExpr col1 = generateScalar(fn1Af.parameters().get(0), fn1Param, store, tableAlias);
            SqlExpr col2 = generateScalar(fn1Af.parameters().get(1), fn1Param, store, tableAlias);
            return generateRowMapperAgg(acs, col1, col2, registry);
        }

        // Standard: compile fn1 body as scalar expression
        SqlExpr mapExpr = generateScalar(fn1Body, fn1Param, store, tableAlias);

        // Special: hashCode → HASH(LIST(col))
        if (acs.resolvedFunc() == registry.hashCodeAgg()) {
            return new SqlExpr.FunctionCall("HASH",
                    List.of(new SqlExpr.FunctionCall("LIST", List.of(mapExpr))));
        }

        // Get SQL function name
        String sqlName = mapPureFuncToSql(acs.resolvedFunc().name());
        if (sqlName == null) sqlName = acs.resolvedFunc().name().toUpperCase();

        // Standard: FUNC(mapExpr, extraArgs from fn2...)
        List<SqlExpr> args = new ArrayList<>();
        args.add(mapExpr);
        // Extra args from fn2 AST params (skip param 0 = the variable)
        var fn2Body = ast.function2().body().get(0);
        if (fn2Body instanceof AppliedFunction fn2Af) {
            // Unwrap cast() wrapper if present
            if ("cast".equals(simpleName(fn2Af.function()))
                    && !fn2Af.parameters().isEmpty()
                    && fn2Af.parameters().get(0) instanceof AppliedFunction innerAf) {
                fn2Af = innerAf;
            }
            // Percentile: interpret ascending/continuous flags from fn2 args
            // Pure: percentile(values, p, ascending=true, continuous=true)
            // DuckDB: QUANTILE_CONT(col, p) or QUANTILE_DISC(col, p) — only 2 args
            String fn2Name = simpleName(fn2Af.function());
            if ("percentile".equals(fn2Name) || "percentileCont".equals(fn2Name)
                    || "percentileDisc".equals(fn2Name)) {
                // param[0]=variable, param[1]=p, param[2]=ascending, param[3]=continuous
                SqlExpr p = fn2Af.parameters().size() > 1
                        ? generateScalar(fn2Af.parameters().get(1), null, null, null)
                        : new SqlExpr.NumericLiteral(0.5);
                boolean ascending = true;
                boolean continuous = "percentileCont".equals(fn2Name)
                        || "percentile".equals(fn2Name);  // default is continuous
                if ("percentileDisc".equals(fn2Name)) continuous = false;
                if (fn2Af.parameters().size() > 2
                        && fn2Af.parameters().get(2) instanceof CBoolean(boolean v)) ascending = v;
                if (fn2Af.parameters().size() > 3
                        && fn2Af.parameters().get(3) instanceof CBoolean(boolean v)) continuous = v;
                if (!ascending) {
                    p = new SqlExpr.Binary(new SqlExpr.NumericLiteral(1), "-", p);
                }
                sqlName = continuous ? "percentileCont" : "percentileDisc";
                return new SqlExpr.FunctionCall(sqlName, List.of(mapExpr, p));
            }
            for (int i = 1; i < fn2Af.parameters().size(); i++) {
                args.add(generateScalar(fn2Af.parameters().get(i), null, null, null));
            }
        }
        return new SqlExpr.FunctionCall(sqlName, args);
    }

    /**
     * Handles rowMapper-based aggregates: wavg, corr, covarSample, etc.
     * rowMapper bundles two columns from fn1 into a paired expression.
     */
    private SqlExpr generateRowMapperAgg(TypeInfo.AggColumnSpec acs,
                                          SqlExpr col1, SqlExpr col2,
                                          com.gs.legend.compiler.BuiltinFunctionRegistry registry) {
        String funcName = acs.resolvedFunc().name();
        // wavg: SUM(col1 * col2) / SUM(col2)
        if ("wavg".equals(funcName)) {
            SqlExpr product = new SqlExpr.Binary(col1, "*", col2);
            SqlExpr sumProduct = new SqlExpr.FunctionCall("SUM", List.of(product));
            SqlExpr sumWeight = new SqlExpr.FunctionCall("SUM", List.of(col2));
            return new SqlExpr.Binary(sumProduct, "/", sumWeight);
        }
        // corr: CORR(col1, col2)
        if ("corr".equals(funcName)) {
            return new SqlExpr.FunctionCall("CORR", List.of(col1, col2));
        }
        // covarSample: COVAR_SAMP(col1, col2)
        if ("covarSample".equals(funcName)) {
            return new SqlExpr.FunctionCall("COVAR_SAMP", List.of(col1, col2));
        }
        // covarPopulation: COVAR_POP(col1, col2)
        if ("covarPopulation".equals(funcName)) {
            return new SqlExpr.FunctionCall("COVAR_POP", List.of(col1, col2));
        }
        // maxBy/minBy: ARG_MAX/ARG_MIN(col1, col2)
        if ("maxBy".equals(funcName)) {
            return new SqlExpr.FunctionCall("ARG_MAX", List.of(col1, col2));
        }
        if ("minBy".equals(funcName)) {
            return new SqlExpr.FunctionCall("ARG_MIN", List.of(col1, col2));
        }
        throw new PureCompileException(
                "PlanGenerator: unknown rowMapper aggregate: " + funcName);
    }

    /**
     * Checks if an AppliedFunction is a rowMapper call by comparing
     * the resolved function against the registry's cached rowMapper def.
     */
    private boolean isRowMapperFunc(AppliedFunction af,
                                     com.gs.legend.compiler.BuiltinFunctionRegistry registry) {
        String name = simpleName(af.function());
        var defs = registry.resolve(name);
        return !defs.isEmpty() && defs.get(0) == registry.rowMapper();
    }

    /**
     * Recursively wraps aggregate FunctionCall nodes inside a composite expression
     * with OVER(window), so expressions like SUM(x*y) / SUM(y) become
     * SUM(x*y) OVER(...) / SUM(y) OVER(...).
     * Leaves non-aggregate nodes (column refs, literals, etc.) untouched.
     */
    private SqlExpr windowifyAggExpr(SqlExpr expr, SqlExpr.WindowSpec window) {
        if (expr instanceof SqlExpr.FunctionCall fc) {
            return new SqlExpr.WindowFunction(fc, window);
        }
        if (expr instanceof SqlExpr.Binary bin) {
            return new SqlExpr.Binary(
                    windowifyAggExpr(bin.left(), window), bin.op(),
                    windowifyAggExpr(bin.right(), window));
        }
        if (expr instanceof SqlExpr.Cast cast) {
            return new SqlExpr.Cast(
                    windowifyAggExpr(cast.expr(), window), cast.pureTypeName());
        }
        return expr;
    }

    // ========== extend (window functions) ==========

    private SqlBuilder generateExtend(AppliedFunction af) {
        List<ValueSpecification> params = af.parameters();

        // --- Class-source extend: passthrough (stays in object space) ---
        // extend(C[*]) is an intermediate step — downstream project/graphFetch generates SQL.
        TypeInfo extendInfo = unit.types().get(af);
        if (extendInfo != null && extendInfo.type() instanceof GenericType.ClassType) {
            return generateRelation(params.get(0));
        }

        // --- Cancellation check (from StoreResolution sidecar) ---
        var store = storeFor(af);
        var extendOverride = store != null ? store.extendOverride() : null;
        if (extendOverride != null && extendOverride.isFullyCancelled()) {
            return generateRelation(params.get(0)); // skip entire node including JOINs
        }

        SqlBuilder source = generateRelation(params.get(0));

        // Read pre-resolved window specs from sidecar
        TypeInfo info = unit.types().get(af);
        if (info != null && !info.windowSpecs().isEmpty()) {
            // --- Window extend: WindowSpec (sidecar) + ColSpec (AST) ---
            SqlBuilder b = new SqlBuilder().selectStar().fromSubquery(source, "window_src");
            List<ColSpec> astSpecs = com.gs.legend.compiler.checkers.ExtendChecker
                    .extractAllColSpecs(params);

            for (int i = 0; i < info.windowSpecs().size(); i++) {
                var ws = info.windowSpecs().get(i);
                ColSpec ast = astSpecs.get(i);
                String quotedAlias = dialect.quoteIdentifier(ws.alias());

                // Build SQL OVER(...) from compiled OverSpec
                SqlExpr.WindowSpec sqlWindow = buildSqlWindowSpec(ws.over());

                // Build function call from AST (like generateAggFromAst)
                // generateWindowFuncFromAst always returns a complete expression
                // with OVER() already applied — caller just adds it as a column.
                SqlExpr funcExpr = generateWindowFuncFromAst(ws, ast, sqlWindow);

                if (ws.castType() != null) {
                    funcExpr = new SqlExpr.Cast(funcExpr,
                            ws.castType().typeName());
                }
                b.addWindowColumn(funcExpr, null, quotedAlias);
            }
            return b;
        }

        // --- Traverse extend: flat LEFT JOINs + colSpec from terminal table ---
        if (info != null && info.traversalSpecs() != null) {
            var specs = info.traversalSpecs();
            String sourceAlias = unquote(source.getFromAlias());
            String prevAlias = sourceAlias;

            // Determine if source is table-ref based (physical column names → flat JOINs safe)
            // or not (class-based project / scalar extend → aliased columns → must wrap).
            TypeInfo sourceInfo = unit.types().get(params.get(0));
            boolean sourceIsTableBased = sourceInfo != null
                    && (sourceInfo.resolvedTableName() != null || sourceInfo.traversalSpecs() != null);

            if (!sourceIsTableBased) {
                // Source has aliased columns (class-based project or scalar extend).
                // Wrap in subquery so traverse ON clause references output column names.
                String wrapAlias = "trav_src";
                source = new SqlBuilder()
                        .selectQualifiedStar(dialect.quoteIdentifier(wrapAlias))
                        .fromSubquery(source, wrapAlias);
                sourceAlias = wrapAlias;
                prevAlias = wrapAlias;
            } else if (!source.hasJoins()) {
                // Clean table-ref source: qualified star to avoid column ambiguity.
                source.selectQualifiedStar(dialect.quoteIdentifier(sourceAlias));
            }
            // else: table-ref based source with JOINs (prior traverse) — keep flat

            // Add flat LEFT JOINs for each TraversalSpec, tracking each terminal alias
            var terminalAliases = new java.util.ArrayList<String>();
            for (var spec : specs) {
                prevAlias = sourceAlias;
                for (var hop : spec.hops()) {
                    String hopAlias = nextTableAlias();
                    var aliases = Map.of(hop.prevParam(), prevAlias, hop.hopParam(), hopAlias);
                    SqlExpr on = generateScalar(hop.conditionBody(), null, null, null, aliases);
                    source.addJoin(SqlBuilder.JoinType.LEFT,
                            dialect.quoteIdentifier(hop.tableName()),
                            dialect.quoteIdentifier(hopAlias), on);
                    prevAlias = hopAlias;
                }
                terminalAliases.add(prevAlias);
            }

            // Compile colSpecs — map lambda params to terminal aliases
            List<ColSpec> traverseColSpecs = new java.util.ArrayList<>();
            for (int i = 1; i < params.size(); i++) {
                if (params.get(i) instanceof ClassInstance ci) {
                    if (ci.value() instanceof ColSpec cs) traverseColSpecs.add(cs);
                    else if (ci.value() instanceof ColSpecArray(List<ColSpec> specs2))
                        traverseColSpecs.addAll(specs2);
                }
            }
            for (ColSpec cs : traverseColSpecs) {
                if (extendOverride != null && !extendOverride.isActive(cs.name())) continue;
                if (cs.function1() != null) {
                    var lambdaParams = cs.function1().parameters();
                    Map<String, String> varAliases;
                    if (specs.size() > 1 && lambdaParams.size() > 2) {
                        // Multi-traverse: {src, t1, t2, ...}
                        var aliasMap = new java.util.HashMap<String, String>();
                        aliasMap.put(lambdaParams.get(0).name(), sourceAlias);
                        for (int ti = 0; ti < terminalAliases.size() && (ti + 1) < lambdaParams.size(); ti++) {
                            aliasMap.put(lambdaParams.get(ti + 1).name(), terminalAliases.get(ti));
                        }
                        varAliases = aliasMap;
                    } else if (lambdaParams.size() >= 2) {
                        // 2-param lambda {src, tgt | ...}: src → source, tgt → terminal
                        varAliases = Map.of(
                                lambdaParams.get(0).name(), sourceAlias,
                                lambdaParams.get(1).name(), terminalAliases.get(terminalAliases.size() - 1));
                    } else if (lambdaParams.size() == 1) {
                        varAliases = Map.of(lambdaParams.get(0).name(), terminalAliases.get(terminalAliases.size() - 1));
                    } else {
                        varAliases = null;
                    }
                    SqlExpr computed = generateScalar(
                            cs.function1().body().get(0), null, null, null, varAliases);
                    source.addSelect(computed, dialect.quoteIdentifier(cs.name()));
                }
            }
            return source;
        }

        // --- Scalar extend: read AST directly (unchanged) ---
        List<ColSpec> colSpecs = new java.util.ArrayList<>();
        for (int i = 1; i < params.size(); i++) {
            if (params.get(i) instanceof ClassInstance ci) {
                if (ci.value() instanceof ColSpec cs) {
                    colSpecs.add(cs);
                } else if (ci.value() instanceof ColSpecArray(List<ColSpec> specs)) {
                    colSpecs.addAll(specs);
                }
            }
        }
        if (source.hasPivot() || source.hasWindowColumns()) {
            source = new SqlBuilder().selectStar().fromSubquery(source, "extend_src");
        }
        for (ColSpec cs : colSpecs) {
            if (extendOverride != null && !extendOverride.isActive(cs.name())) continue;
            if (cs.function1() != null) {
                String lambdaParam = cs.function1().parameters().isEmpty() ? null
                        : cs.function1().parameters().get(0).name();
                SqlExpr computed = generateScalar(
                        cs.function1().body().get(0), lambdaParam, null, null);
                source.addSelect(computed, dialect.quoteIdentifier(cs.name()));
            }
        }
        return source;
    }

    /** Builds SQL OVER(...) from compiled OverSpec — pure data, no string sniffing. */
    private SqlExpr.WindowSpec buildSqlWindowSpec(TypeInfo.OverSpec over) {
        List<SqlExpr> partCols = over.partitionBy().stream()
                .map(c -> (SqlExpr) new SqlExpr.ColumnRef(c)).toList();
        List<SqlExpr> orderParts = over.orderBy().stream()
                .map(s -> {
                    String dir = s.direction() == TypeInfo.SortDirection.ASC ? "ASC" : "DESC";
                    String nullOrder = "DESC".equals(dir) ? "NULLS FIRST" : "NULLS LAST";
                    return (SqlExpr) new SqlExpr.OrderByTerm(
                            new SqlExpr.ColumnRef(s.column()), dir, nullOrder);
                }).toList();
        String frame = over.frame() != null ? formatFrameSpec(over.frame()) : null;
        return new SqlExpr.WindowSpec(partCols, orderParts, frame);
    }

    /**
     * Generates a complete windowed SQL expression from AST.
     * Always returns a fully-formed expression with OVER() already applied.
     * Caller should NOT add another OVER().
     */
    private SqlExpr generateWindowFuncFromAst(TypeInfo.WindowSpec ws, ColSpec ast,
                                               SqlExpr.WindowSpec sqlWindow) {
        String sqlFunc = mapPureFuncToSql(ws.resolvedFunc().name());
        if (sqlFunc == null) sqlFunc = ws.resolvedFunc().name().toUpperCase();

        // --- Aggregate window (fn1 + fn2): reuse generateAggFromAst ---
        // Composite expressions like wavg's SUM(x*y)/SUM(y) become
        // SUM(x*y) OVER(...) / SUM(y) OVER(...) — already fully windowed.
        if (ast.function2() != null) {
            var acs = new TypeInfo.AggColumnSpec(ws.alias(), ws.resolvedFunc(),
                    ws.returnType(), ws.castType());
            SqlExpr aggExpr = generateAggFromAst(acs, ast, null, null);
            return windowifyAggExpr(aggExpr, sqlWindow);
        }

        // --- fn1-only: read function structure from AST body ---
        var body = ast.function1().body().get(0);

        // Pattern: $p->func($w,$r).property → FUNC(property) OVER(...)
        // Used by: lag, lead, first, last
        if (body instanceof AppliedProperty ap && !ap.parameters().isEmpty()
                && ap.parameters().get(0) instanceof AppliedFunction innerAf) {
            String innerSql = mapPureFuncToSql(simpleName(innerAf.function()));
            if (innerSql == null) innerSql = simpleName(innerAf.function()).toUpperCase();
            List<SqlExpr> args = new ArrayList<>();
            args.add(new SqlExpr.ColumnRef(ap.property()));
            for (int i = 1; i < innerAf.parameters().size(); i++) {
                var param = innerAf.parameters().get(i);
                if (!(param instanceof Variable)) {
                    args.add(generateScalar(param, null, null, null));
                }
            }
            SqlExpr func = new SqlExpr.FunctionCall(innerSql, args);
            return new SqlExpr.WindowFunction(func, sqlWindow);
        }

        // Pattern: wrapper(innerFunc($w,$r), args) → WRAPPER(INNER() OVER(...), args)
        // Used by: round(cumulativeDistribution($w,$r), 2)
        if (body instanceof AppliedFunction af && !af.parameters().isEmpty()
                && af.parameters().get(0) instanceof AppliedFunction innerAf) {
            String innerFuncName = simpleName(innerAf.function());
            String innerSql = mapPureFuncToSql(innerFuncName);
            if (innerSql == null) innerSql = innerFuncName.toUpperCase();
            var innerDefs = com.gs.legend.compiler.BuiltinFunctionRegistry.instance()
                    .resolve(innerFuncName);
            if (!innerDefs.isEmpty()) {
                SqlExpr innerFunc = new SqlExpr.FunctionCall(innerSql, List.of());
                SqlExpr windowed = new SqlExpr.WindowFunction(innerFunc, sqlWindow);
                List<SqlExpr> wrapperArgs = new ArrayList<>();
                wrapperArgs.add(windowed);
                for (int i = 1; i < af.parameters().size(); i++) {
                    var param = af.parameters().get(i);
                    if (!(param instanceof Variable)) {
                        wrapperArgs.add(generateScalar(param, null, null, null));
                    }
                }
                String outerSql = mapPureFuncToSql(simpleName(af.function()));
                if (outerSql == null) outerSql = simpleName(af.function()).toUpperCase();
                return new SqlExpr.FunctionCall(outerSql, wrapperArgs);
            }
        }

        // Pattern: func($r), func($w,$r), func($r,2) → FUNC(args) OVER(...)
        // Used by: rowNumber, rank, denseRank, percentRank, cumulativeDistribution, ntile, count
        if (body instanceof AppliedFunction af) {
            List<SqlExpr> args = new ArrayList<>();
            for (var p : af.parameters()) {
                if (p instanceof CInteger ci) {
                    args.add(new SqlExpr.NumericLiteral(ci.value()));
                } else if (p instanceof CFloat cf) {
                    args.add(new SqlExpr.NumericLiteral(cf.value()));
                } else if (p instanceof CString cs) {
                    args.add(new SqlExpr.StringLiteral(cs.value()));
                }
            }
            SqlExpr func = new SqlExpr.FunctionCall(sqlFunc, args);
            return new SqlExpr.WindowFunction(func, sqlWindow);
        }

        // Fallback: compile fn1 body as scalar expression, wrap with OVER
        String fn1Param = ast.function1().parameters().isEmpty() ? null
                : ast.function1().parameters().get(0).name();
        SqlExpr scalar = generateScalar(body, fn1Param, null, null);
        return new SqlExpr.WindowFunction(scalar, sqlWindow);
    }

    /** Maps a Pure function name to a semantic function name for SQL generation.
     *  Dialect layer takes care of rendering semantic names to SQL syntax. */
    private String mapPureFuncToSql(String pureFuncName) {
        return switch (pureFuncName) {
            // Aggregates
            case "plus", "sum" -> "sum";
            case "average", "avg", "mean" -> "avg";
            case "count", "size" -> "count";
            case "min" -> "min";
            case "max" -> "max";
            case "stdDev", "stddev" -> "stdDev";
            case "stdDevSample" -> "stdDevSample";
            case "stdDevPopulation" -> "stdDevPopulation";
            case "variance" -> "variance";
            case "varianceSample" -> "varianceSample";
            case "variancePopulation" -> "variancePopulation";
            case "covarSample" -> "covarSample";
            case "covarPopulation" -> "covarPopulation";
            case "median" -> "median";
            case "percentile", "percentileCont" -> "percentileCont";
            case "percentileDisc" -> "percentileDisc";
            case "joinStrings" -> "joinStrings";
            case "mode" -> "mode";
            case "corr" -> "corr";
            case "maxBy" -> "maxBy";
            case "minBy" -> "minBy";
            case "wavg" -> "wavg";
            case "hashCode" -> "hashCode";
            // Ranking
            case "rowNumber" -> "rowNumber";
            case "rank" -> "rank";
            case "denseRank" -> "denseRank";
            case "percentRank" -> "percentRank";
            case "cumulativeDistribution" -> "cumulativeDistribution";
            // Value functions
            case "first", "firstValue" -> "firstValue";
            case "last", "lastValue" -> "lastValue";
            case "lag" -> "lag";
            case "lead" -> "lead";
            case "ntile" -> "ntile";
            case "nthValue", "nth" -> "nthValue";
            // Math wrappers
            case "round" -> "round";
            case "abs" -> "abs";
            case "ceil" -> "ceil";
            case "floor" -> "floor";
            case "truncate" -> "truncate";
            // cast: pass-through (identity) in aggregate context
            case "cast" -> "cast";
            default -> null;
        };
    }

    /** Formats a structured FrameSpec into SQL frame clause text. */
    private String formatFrameSpec(TypeInfo.FrameSpec frame) {
        String type = frame.frameType().toUpperCase();
        return type + " BETWEEN " + formatFrameBound(frame.start(), true)
                + " AND " + formatFrameBound(frame.end(), false);
    }

    /** Formats a FrameBound into SQL text. */
    private String formatFrameBound(TypeInfo.FrameBound bound, boolean isStart) {
        return switch (bound.type()) {
            case UNBOUNDED -> isStart ? "UNBOUNDED PRECEDING" : "UNBOUNDED FOLLOWING";
            case CURRENT_ROW -> "CURRENT ROW";
            case OFFSET -> {
                double v = bound.offset();
                if (v < 0)
                    yield formatOffset(Math.abs(v)) + " PRECEDING";
                if (v > 0)
                    yield formatOffset(v) + " FOLLOWING";
                yield "CURRENT ROW";
            }
        };
    }

    /** Formats a frame offset: integer if whole, decimal otherwise. */
    private static String formatOffset(double v) {
        if (v == Math.floor(v) && !Double.isInfinite(v))
            return String.valueOf((long) v);
        return java.math.BigDecimal.valueOf(v).stripTrailingZeros().toPlainString();
    }

    // ========== join ==========

    private SqlBuilder generateJoin(AppliedFunction af) {
        List<ValueSpecification> params = af.parameters();
        // join(left, right, joinType, condition)
        if (params.size() < 3) {
            throw new PureCompileException("PlanGenerator: join() requires left, right, and condition");
        }

        SqlBuilder left = generateRelation(params.get(0));
        SqlBuilder right = generateRelation(params.get(1));

        // Read pre-resolved join type from sidecar
        TypeInfo info = unit.types().get(af);
        SqlBuilder.JoinType joinType = switch (info.joinType()) {
            case "LEFT", "LEFT_OUTER" -> SqlBuilder.JoinType.LEFT;
            case "RIGHT", "RIGHT_OUTER" -> SqlBuilder.JoinType.RIGHT;
            case "FULL", "FULL_OUTER" -> SqlBuilder.JoinType.FULL;
            case "CROSS" -> SqlBuilder.JoinType.CROSS;
            default -> SqlBuilder.JoinType.INNER;
        };

        // Determine condition index
        int conditionIdx = params.size() >= 4 ? 3 : 2;
        if (conditionIdx == 2 && params.get(2) instanceof EnumValue) {
            conditionIdx = -1; // cross join, no condition
        }

        // Build ON condition from lambda — derive param→alias mapping from AST
        SqlExpr onCondition = null;
        if (conditionIdx >= 0 && conditionIdx < params.size()) {
            var condSpec = params.get(conditionIdx);
            if (condSpec instanceof LambdaFunction lf) {
                Map<String, String> aliases = buildJoinAliases(lf);
                onCondition = generateScalar(lf.body().get(0), null, null, null, aliases);
            }
        }
        String leftAlias = dialect.quoteIdentifier("left_src");
        String rightAlias = dialect.quoteIdentifier("right_src");

        // When renames exist, enumerate columns explicitly to apply prefixed aliases
        Map<String, String> renames = info.joinColumnRenames();
        SqlBuilder result = new SqlBuilder();
        if (renames != null && !renames.isEmpty()) {
            // Left: all columns as "left_src".*
            result.addSelect(new SqlExpr.QualifiedStar(leftAlias), null);
            // Right: enumerate each column, applying rename for conflicts
            TypeInfo rightInfo = unit.types().get(params.get(1));
            if (rightInfo != null && rightInfo.schema() != null) {
                for (String colName : rightInfo.schema().columns().keySet()) {
                    String renamed = renames.get(colName);
                    if (renamed != null) {
                        // Conflicting column → right_src.col AS prefixed_name
                        result.addSelect(
                                new SqlExpr.Column("right_src", colName),
                                dialect.quoteIdentifier(renamed));
                    } else {
                        // Non-conflicting → right_src.col AS col
                        result.addSelect(
                                new SqlExpr.Column("right_src", colName),
                                dialect.quoteIdentifier(colName));
                    }
                }
            }
        } else {
            result.selectStar();
        }
        result.fromSubquery(left, leftAlias);

        result.addJoin(new SqlBuilder.JoinClause(
                joinType,
                null, // no table name — using subquery
                rightAlias,
                right, // right as subquery
                onCondition,
                null)); // no USING

        return result;
    }

    private SqlBuilder generateAsOfJoin(AppliedFunction af) {
        List<ValueSpecification> params = af.parameters();
        // asOfJoin(left, right, matchCondition [, keyCondition])
        if (params.size() < 3) {
            throw new PureCompileException("PlanGenerator: asOfJoin() requires left, right, and match condition");
        }

        SqlBuilder left = generateRelation(params.get(0));
        SqlBuilder right = generateRelation(params.get(1));

        // Match condition: {t, q | $t.time > $q.time}
        SqlExpr matchCondition = null;
        if (params.get(2) instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            Map<String, String> aliases = buildJoinAliases(lf);
            matchCondition = generateScalar(lf.body().get(0), null, null, null, aliases);
        }

        // Optional key condition: {t, q | $t.symbol == $q.symbol}
        SqlExpr keyCondition = null;
        if (params.size() >= 4 && params.get(3) instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            Map<String, String> aliases = buildJoinAliases(lf);
            keyCondition = generateScalar(lf.body().get(0), null, null, null, aliases);
        }

        // Build ON clause: key AND match (key first if present)
        SqlExpr onCondition;
        if (keyCondition != null && matchCondition != null) {
            onCondition = new SqlExpr.And(List.of(keyCondition, matchCondition));
        } else if (matchCondition != null) {
            onCondition = matchCondition;
        } else {
            throw new PureCompileException("asOfJoin: match condition required");
        }

        String leftAlias = dialect.quoteIdentifier("left_src");
        String rightAlias = dialect.quoteIdentifier("right_src");

        TypeInfo info = unit.typeInfoFor(af);
        Map<String, String> renames = info != null ? info.joinColumnRenames() : null;
        SqlBuilder result = new SqlBuilder();
        if (renames != null && !renames.isEmpty()) {
            // Left: all columns as "left_src".*
            result.addSelect(new SqlExpr.QualifiedStar(leftAlias), null);
            // Right: enumerate each column, applying rename for conflicts
            TypeInfo rightInfo = unit.types().get(params.get(1));
            if (rightInfo != null && rightInfo.schema() != null) {
                for (String colName : rightInfo.schema().columns().keySet()) {
                    String renamed = renames.get(colName);
                    if (renamed != null) {
                        result.addSelect(
                                new SqlExpr.Column("right_src", colName),
                                dialect.quoteIdentifier(renamed));
                    } else {
                        result.addSelect(
                                new SqlExpr.Column("right_src", colName),
                                dialect.quoteIdentifier(colName));
                    }
                }
            }
        } else {
            result.selectStar();
        }
        result.fromSubquery(left, leftAlias);

        result.addJoin(new SqlBuilder.JoinClause(
                SqlBuilder.JoinType.ASOF_LEFT,
                null,
                rightAlias,
                right,
                onCondition,
                null));

        return result;
    }

    /**
     * Builds lambda param → table alias mapping for join conditions.
     * First lambda param → "left_src", second → "right_src".
     */
    private static Map<String, String> buildJoinAliases(LambdaFunction lf) {
        var params = lf.parameters();
        if (params.size() >= 2) {
            return Map.of(params.get(0).name(), "left_src",
                          params.get(1).name(), "right_src");
        } else if (params.size() == 1) {
            return Map.of(params.get(0).name(), "left_src");
        }
        return Map.of();
    }

    private String unquote(String s) {
        if (s != null && s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    // ========== from ==========

    private SqlBuilder generateFrom(AppliedFunction af) {
        // from() is a runtime binding — pass through source
        return generateRelation(af.parameters().get(0));
    }

    // ========== pivot ==========
    /**
     * Generates PIVOT SQL from TypeInfo sidecar — same pattern as groupBy.
     * Reads pivot column names from {@code columnSpecs} and aggregates from
     * {@code aggColumnSpecs} + AST fn1/fn2 via {@link #generateAggFromAst}.
     *
     * SQL: PIVOT (source) ON pivotCol USING AGG(valueCol) AS "_|__alias"
     */
    private SqlBuilder generatePivot(AppliedFunction af) {
        SqlBuilder source = generateRelation(af.parameters().get(0));

        TypeInfo info = unit.typeInfoFor(af);
        if (info == null || info.columnSpecs().isEmpty()) {
            throw new PureCompileException("pivot(): missing pivot column specs in TypeInfo sidecar");
        }

        // Pivot column names from columnSpecs (set by PivotChecker)
        List<String> pivotColumns = info.columnSpecs().stream()
                .map(TypeInfo.ColumnSpec::columnName)
                .toList();

        // Aggregate columns — same pattern as generateGroupBy/generateAggregate
        int aggParamIdx = af.parameters().size() - 1;
        List<ColSpec> astSpecs = com.gs.legend.compiler.checkers.GroupByChecker
                .extractAggColSpecs(af.parameters().get(aggParamIdx));
        List<SqlBuilder.PivotAggregate> aggregates = new java.util.ArrayList<>();
        for (int i = 0; i < info.aggColumnSpecs().size(); i++) {
            var acs = info.aggColumnSpecs().get(i);
            ColSpec ast = astSpecs.get(i);

            SqlExpr aggExpr = generateAggFromAst(acs, ast, null, null);
            if (acs.castType() != null)
                aggExpr = new SqlExpr.Cast(aggExpr, acs.castType().typeName());
            String exprSql = aggExpr.toSql(dialect);
            aggregates.add(new SqlBuilder.PivotAggregate(exprSql, acs.alias()));
        }

        // Build pivot on a fresh builder — source becomes fromSubquery so
        // renderPivot serializes the full source chain (WHERE, SELECT, etc.)
        SqlBuilder pivotBuilder = new SqlBuilder().fromSubquery(source, null);
        pivotBuilder.pivot(new SqlBuilder.PivotClause(pivotColumns, aggregates));
        return pivotBuilder;
    }

    // ========== Scalar Expression Compilation ==========

    /**
     * Compiles a scalar expression to SqlExpr. Delegates to 5-arg with null
     * tableAlias and varAliases.
     */
    SqlExpr generateScalar(ValueSpecification vs, String rowParam, StoreResolution store) {
        return generateScalar(vs, rowParam, store, null, null);
    }

    /**
     * Compiles scalar with optional table alias prefix. Delegates to 5-arg.
     */
    SqlExpr generateScalar(ValueSpecification vs, String rowParam, StoreResolution store, String tableAlias) {
        return generateScalar(vs, rowParam, store, tableAlias, null);
    }

    /**
     * Canonical 5-arg version: compiles scalar with optional table alias prefix
     * and optional variable→alias mapping for join condition lambda params.
     *
     * @param varAliases Maps lambda param names to table aliases (e.g., "l" → "left_src").
     *                   Used by join/asOfJoin to qualify column references without
     *                   compiler-side tagging. Null outside join conditions.
     */
    SqlExpr generateScalar(ValueSpecification vs, String rowParam, StoreResolution store,
                           String tableAlias, Map<String, String> varAliases) {
        // Compiler-driven inlining: follow inlinedBody pointers (let bindings, blocks, user functions)
        TypeInfo vsInfo = unit.types().get(vs);
        if (vsInfo != null && vsInfo.inlinedBody() != null) {
            return generateScalar(vsInfo.inlinedBody(), rowParam, store, tableAlias, varAliases);
        }
        return switch (vs) {
            case AppliedProperty ap -> {
                // Join condition: resolve variable→alias from lambda param mapping
                if (varAliases != null && !ap.parameters().isEmpty()
                        && ap.parameters().get(0) instanceof Variable v
                        && varAliases.containsKey(v.name())) {
                    yield new SqlExpr.Column(varAliases.get(v.name()), ap.property());
                }
                // Multi-hop association path: read from TypeInfo (stamped by Compiler)
                TypeInfo apInfo = unit.types().get(ap);
                if (apInfo != null && apInfo.associationPath() != null
                        && apInfo.associationPath().size() > 1) {
                    var path = apInfo.associationPath();
                    // hops = all but last, leaf = last
                    yield new SqlExpr.AssociationRef(
                            path.subList(0, path.size() - 1), path.getLast());
                }
                // Struct field access in lambda: $f.legalName → f.legalName
                // Only when owner is NOT the relational row param (relational row accesses → column ref)
                if (tableAlias == null && !ap.parameters().isEmpty()
                        && ap.parameters().get(0) instanceof Variable owner
                        && (!owner.name().equals(rowParam))) {
                    yield new SqlExpr.FieldAccess(new SqlExpr.Identifier(owner.name()), ap.property());
                }
                // Delegate to resolveColumnExpr — handles column, expression, enum, M2M uniformly
                yield resolveColumnExpr(ap.property(), store, tableAlias);
            }
            case AppliedFunction af -> {
                // Check for inlined user function — process expanded body
                TypeInfo afInfo = unit.types().get(af);
                if (afInfo != null && afInfo.inlinedBody() != null) {
                    TypeInfo bodyInfo = unit.types().get(afInfo.inlinedBody());
                    if (bodyInfo != null && !bodyInfo.isScalar()) {
                        throw new PureCompileException(
                                "Inlined function '" + af.function() + "' returns a relation in scalar context");
                    }
                    yield generateScalar(afInfo.inlinedBody(), rowParam, store, tableAlias, varAliases);
                }
                yield generateScalarFunction(af, rowParam, store, tableAlias, varAliases);
            }
            case CInteger i -> {
                // Check compiler type annotation for precision
                TypeInfo tiInt = unit.typeInfoFor(i);
                if (tiInt != null && tiInt.type() == GenericType.Primitive.INT128) {
                    yield new SqlExpr.Cast(new SqlExpr.NumericLiteral(i.value()), "Int128");
                }
                yield new SqlExpr.NumericLiteral(i.value());
            }
            case CFloat f -> {
                yield new SqlExpr.DecimalLiteral(java.math.BigDecimal.valueOf(f.value()));
            }
            case CDecimal d -> {
                // Check compiler type annotation for precision
                TypeInfo tiDec = unit.typeInfoFor(d);
                if (tiDec != null && tiDec.type() instanceof GenericType.PrecisionDecimal(
                        int precision, int scale
                )) {
                    yield new SqlExpr.Cast(
                            new SqlExpr.DecimalLiteral(d.value()),
                            "Decimal(" + precision + "," + scale + ")");
                }
                yield new SqlExpr.DecimalLiteral(d.value());
            }
            case CString s -> new SqlExpr.StringLiteral(s.value());
            case CBoolean b -> new SqlExpr.BoolLiteral(b.value());
            case CDateTime dt -> new SqlExpr.TimestampLiteral(dt.value());
            case CStrictDate sd -> new SqlExpr.DateLiteral(sd.value());
            case CStrictTime st -> new SqlExpr.TimeLiteral(st.value());
            case CLatestDate ld -> new SqlExpr.CurrentDate();
            case Variable v -> {
                if (v.name().equals(rowParam))
                    yield new SqlExpr.ColumnRef("");
                // Lambda param → raw unquoted identifier (from compiler side-table)
                TypeInfo vti = unit.types().get(v);
                if (vti != null && vti.lambdaParam())
                    yield new SqlExpr.Identifier(v.name());
                yield new SqlExpr.ColumnRef(v.name());
            }
            case ClassInstance ci -> {
                if (ci.value() instanceof ColSpec cs)
                    yield new SqlExpr.ColumnRef(cs.name());
                throw new PureCompileException(
                        "PlanGenerator: unsupported ClassInstance in scalar: " + ci.type());
            }
            case EnumValue ev -> new SqlExpr.StringLiteral(ev.value());
            case PureCollection coll -> {
                var exprs = coll.values().stream()
                        .map(v -> generateScalar(v, rowParam, store, tableAlias))
                        .collect(Collectors.toList());
                // Heterogeneous lists (List<Number>, List<Date>) wrap each element with
                // ::VARIANT to preserve original types through DuckDB operations.
                // Integer literals must be upcast to Integer (→ BIGINT) before VARIANT
                // so that VARIANT preserves 64-bit semantics (Pure Integer = Java Long).
                TypeInfo collInfo = unit.typeInfoFor(coll);
                if (collInfo != null && collInfo.isHeterogeneousList()
                        && !coll.values().isEmpty()
                        && coll.values().stream().noneMatch(v -> v instanceof ClassInstance)) {
                    var values = coll.values();
                    for (int idx = 0; idx < exprs.size(); idx++) {
                        SqlExpr e = exprs.get(idx);
                        // Upcast integer literals to BIGINT before VARIANT
                        if (values.get(idx) instanceof CInteger) {
                            e = new SqlExpr.Cast(e, "Integer");
                        }
                        exprs.set(idx, new SqlExpr.VariantCast(e));
                    }
                }
                yield new SqlExpr.ArrayLiteral(exprs);
            }
            case LambdaFunction lf -> {
                // Nested lambda in scalar context (predicate arg to find/exists)
                if (!lf.body().isEmpty()) {
                    yield generateScalar(lf.body().get(0), rowParam, store, tableAlias);
                }
                throw new PureCompileException(
                        "PlanGenerator: empty lambda body in scalar context");
            }
            case PackageableElementPtr ptr -> {
                // Function/class reference in scalar context — render as string
                yield new SqlExpr.StringLiteral(ptr.fullPath());
            }
            default -> throw new PureCompileException(
                    "PlanGenerator: unsupported scalar: " + vs.getClass().getSimpleName());
        };
    }

    /**
     * Unified scalar function compilation. Handles all Pure functions → SqlExpr.
     * When tableAlias is non-null, property accesses are prefixed with the alias
     * and comparisons may produce EXISTS subqueries for association paths.
     */
    private SqlExpr generateScalarFunction(AppliedFunction af, String rowParam, StoreResolution store,
            String tableAlias, Map<String, String> varAliases) {
        String funcName = simpleName(af.function());
        List<ValueSpecification> params = af.parameters();

        // Helper: recursively compile with same context
        java.util.function.Function<ValueSpecification, SqlExpr> c = v -> generateScalar(v, rowParam, store,
                tableAlias, varAliases);

        // Helper: check if first param is a list via TypeInfo side table
        boolean firstArgIsList = !params.isEmpty() && isListArg(params.get(0));

        return switch (funcName) {
            // --- Struct field access (synthesized by Compiler for .property on instance literals) ---
            case "structExtract" -> {
                SqlExpr struct = generateScalar(params.get(0), rowParam, store, tableAlias);
                String field = ((CString) params.get(1)).value();
                yield new SqlExpr.FieldAccess(struct, field);
            }
            // --- Comparison (may produce EXISTS for association paths when tableAlias set)
            // ---
            case "equal", "greaterThan", "greaterThanEqual",
                    "lessThan", "lessThanEqual", "notEqual", "notEqualAnsi" -> {
                String op = switch (funcName) {
                    case "equal" -> "=";
                    case "greaterThan" -> ">";
                    case "greaterThanEqual" -> ">=";
                    case "lessThan" -> "<";
                    case "lessThanEqual" -> "<=";
                    case "notEqual", "notEqualAnsi" -> "<>";
                    default -> "=";
                };
                // Partial date comparisons: render as string comparison
                if (hasPartialDate(params)) {
                    SqlExpr left = renderForDateComparison(params.get(0), c);
                    SqlExpr right = renderForDateComparison(params.get(1), c);
                    yield new SqlExpr.Binary(left, op, right);
                }
                yield buildComparison(params, op, c, store, tableAlias);
            }

            // --- Logical ---
            case "and" -> {
                if (params.size() == 1 && firstArgIsList) {
                    yield new SqlExpr.FunctionCall("listBoolAnd", List.of(c.apply(params.get(0))));
                }
                if (params.size() == 1) {
                    yield c.apply(params.get(0));
                }
                yield new SqlExpr.And(List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            }
            case "or" -> {
                if (params.size() == 1 && firstArgIsList) {
                    yield new SqlExpr.FunctionCall("listBoolOr", List.of(c.apply(params.get(0))));
                }
                if (params.size() == 1) {
                    yield c.apply(params.get(0));
                }
                yield new SqlExpr.Or(List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            }
            case "not" -> new SqlExpr.Not(c.apply(params.get(0)));

            // --- Arithmetic ---
            case "plus" -> {
                if (params.size() == 1)
                    yield c.apply(params.get(0)); // unary +
                // String concat: use || operator
                // Check literal strings, or typed lambda params (from compiler side-table)
                boolean isStringConcat = params.get(0) instanceof CString || params.get(1) instanceof CString;
                if (!isStringConcat) {
                    // Check TypeInfo for string type (e.g., lambda param x: String[1])
                    for (var p : params) {
                        TypeInfo pti = unit.types().get(p);
                        if (pti != null && pti.type() == GenericType.Primitive.STRING) {
                            isStringConcat = true;
                            break;
                        }
                    }
                }
                if (isStringConcat) {
                    yield new SqlExpr.Binary(c.apply(params.get(0)), "||", c.apply(params.get(1)));
                }
                // Numeric addition: only valid when compiler confirms numeric types
                boolean isNumeric = false;
                for (var p : params) {
                    TypeInfo pti = unit.types().get(p);
                    if (pti != null && pti.type().isNumeric()) {
                        isNumeric = true;
                        break;
                    }
                    // Literal numeric types are always arithmetic
                    if (p instanceof CInteger || p instanceof CFloat || p instanceof CDecimal) {
                        isNumeric = true;
                        break;
                    }
                }
                if (!isNumeric) {
                    throw new PureCompileException(
                            "plus(): cannot determine type — compiler must tag operands. "
                            + "Params: " + params.stream()
                                .map(p -> p.getClass().getSimpleName() + ":" + unit.types().get(p))
                                .toList());
                }
                yield new SqlExpr.Binary(c.apply(params.get(0)), "+", c.apply(params.get(1)));
            }
            case "minus", "sub" -> {
                if (params.size() == 1) {
                    // Unary minus: (-1 * x) to match old pipeline
                    yield new SqlExpr.Binary(new SqlExpr.NumericLiteral(-1), "*", c.apply(params.get(0)));
                }
                yield new SqlExpr.Binary(c.apply(params.get(0)), "-", c.apply(params.get(1)));
            }
            case "times" -> new SqlExpr.Binary(c.apply(params.get(0)), "*", c.apply(params.get(1)));
            case "divide" -> {
                if (params.size() > 2) {
                    // divide(a, b, scale) → ROUND_EVEN((a / b), scale)
                    yield new SqlExpr.FunctionCall("roundHalfEven",
                            List.of(new SqlExpr.Binary(c.apply(params.get(0)), "/", c.apply(params.get(1))),
                                    c.apply(params.get(2))));
                }
                yield new SqlExpr.Binary(c.apply(params.get(0)), "/", c.apply(params.get(1)));
            }
            case "rem" -> new SqlExpr.FunctionCall("MOD", List.of(c.apply(params.get(0)), c.apply(params.get(1))));

            // --- String ---
            case "contains" -> {
                if (firstArgIsList) {
                    // Type-mismatch short-circuit: struct list can never contain a primitive
                    // (e.g., [^Firm(...), ^Firm(...)]->contains(3) → FALSE)
                    // DuckDB rejects LIST_CONTAINS with mismatched types, so we must constant-fold.
                    TypeInfo listInfo = unit.typeInfoFor(params.get(0));
                    TypeInfo searchInfo = unit.typeInfoFor(params.get(1));
                    if (listInfo != null && searchInfo != null
                            && listInfo.type() != null && searchInfo.type() != null) {
                        GenericType listElemType = listInfo.type();
                        GenericType searchType = searchInfo.type();
                        if (listElemType instanceof GenericType.ClassType
                                && searchType.isPrimitive()) {
                            yield new SqlExpr.BoolLiteral(false);
                        }
                        if (listElemType != null && listElemType.isPrimitive()
                                && searchType instanceof GenericType.ClassType) {
                            yield new SqlExpr.BoolLiteral(false);
                        }
                    }

                    // Check if list has mixed types (List<ANY>) — needs TO_JSON wrapping
                    if (listInfo != null && listInfo.isMixedList()
                            && params.get(0) instanceof PureCollection(List<ValueSpecification> values)
                            && values.stream().noneMatch(v -> v instanceof ClassInstance)) {
                        // Mixed-type list: wrap elements in toJson for comparable representation
                        // "toJson" is a semantic name — dialect maps it (DuckDB: TO_JSON)
                        var wrappedElems = values.stream()
                                .map(v -> (SqlExpr) new SqlExpr.FunctionCall("toJson",
                                        List.of(c.apply(v))))
                                .collect(Collectors.toList());
                        SqlExpr wrappedList = new SqlExpr.ArrayLiteral(wrappedElems);
                        SqlExpr wrappedSearch = new SqlExpr.FunctionCall("toJson",
                                List.of(c.apply(params.get(1))));
                        yield new SqlExpr.ListContains(wrappedList, wrappedSearch);
                    }
                    yield new SqlExpr.ListContains(c.apply(params.get(0)), c.apply(params.get(1)));
                }
                // String contains: strPos(str, substr) > 0
                yield new SqlExpr.Binary(
                        new SqlExpr.FunctionCall("strPos", List.of(c.apply(params.get(0)), c.apply(params.get(1)))),
                        ">", new SqlExpr.NumericLiteral(0));
            }
            case "startsWith" -> new SqlExpr.StartsWith(c.apply(params.get(0)), c.apply(params.get(1)));
            case "endsWith" -> new SqlExpr.EndsWith(c.apply(params.get(0)), c.apply(params.get(1)));
            case "concat" -> new SqlExpr.FunctionCall("concat", params.stream().map(c::apply).toList());
            case "toLower" -> new SqlExpr.FunctionCall("LOWER", List.of(c.apply(params.get(0))));
            case "toUpper" -> new SqlExpr.FunctionCall("UPPER", List.of(c.apply(params.get(0))));
            case "length" -> new SqlExpr.FunctionCall("LENGTH", List.of(c.apply(params.get(0))));
            case "trim" -> new SqlExpr.FunctionCall("TRIM", List.of(c.apply(params.get(0))));
            case "toString" -> {
                // DateTime/StrictDate toString: return the literal string directly
                if (params.get(0) instanceof CDateTime(String value)) {
                    yield new SqlExpr.StringLiteral(value);
                } else if (params.get(0) instanceof CStrictDate(String value)) {
                    yield new SqlExpr.StringLiteral(value);
                } else if (params.get(0) instanceof PackageableElementPtr(String fullPath)) {
                    // Class toString: return simplified name
                    yield new SqlExpr.StringLiteral(simpleName(fullPath));
                }
                yield new SqlExpr.Cast(c.apply(params.get(0)), "String");
            }
            case "substring" -> {
                // Pure is 0-based, SQL is 1-based
                SqlExpr start = c.apply(params.get(1));
                SqlExpr offset = new SqlExpr.Binary(start, "+", new SqlExpr.NumericLiteral(1));
                if (params.size() > 2) {
                    // Pure substring(str, start, end) → SQL SUBSTRING(str, start+1, end-start)
                    SqlExpr end = c.apply(params.get(2));
                    SqlExpr length = new SqlExpr.Binary(end, "-", start);
                    yield new SqlExpr.FunctionCall("SUBSTRING",
                            List.of(c.apply(params.get(0)), offset, length));
                }
                yield new SqlExpr.FunctionCall("SUBSTRING",
                        List.of(c.apply(params.get(0)), offset));
            }
            case "indexOf" -> {
                if (firstArgIsList) {
                    // Pure is 0-based, LIST_POSITION is 1-based → subtract 1
                    yield new SqlExpr.Binary(
                            new SqlExpr.FunctionCall("indexOf",
                                    List.of(c.apply(params.get(0)), c.apply(params.get(1)))),
                            "-", new SqlExpr.NumericLiteral(1));
                }
                // String indexOf with optional fromIndex
                if (params.size() > 2) {
                    // indexOf(str, search, fromIndex) →
                    // ((fromIndex + INSTR(SUBSTRING(str, fromIndex+1), search)) - 1)
                    SqlExpr str = c.apply(params.get(0));
                    SqlExpr search = c.apply(params.get(1));
                    SqlExpr fromIdx = c.apply(params.get(2));
                    yield new SqlExpr.FunctionCall("indexOfFrom",
                            List.of(str, search, fromIdx));
                }
                // Simple string indexOf: (instr(str, substr) - 1)
                yield new SqlExpr.Binary(
                        new SqlExpr.FunctionCall("instr", List.of(c.apply(params.get(0)), c.apply(params.get(1)))),
                        "-", new SqlExpr.NumericLiteral(1));
            }
            case "replace" -> new SqlExpr.FunctionCall("REPLACE",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1)), c.apply(params.get(2))));

            // --- SQL literal DynaFunctions ---
            case "sqlNull" -> new SqlExpr.NullLiteral();
            case "sqlTrue" -> new SqlExpr.BoolLiteral(true);
            case "sqlFalse" -> new SqlExpr.BoolLiteral(false);
            case "isNull" -> new SqlExpr.IsNull(c.apply(params.get(0)));
            case "isNotNull" -> new SqlExpr.IsNotNull(c.apply(params.get(0)));
            case "group" -> c.apply(params.get(0));
            case "isDistinct" -> new SqlExpr.FunctionCall("isDistinctFrom",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            case "currentUserId" -> new SqlExpr.FunctionCall("currentUserId", List.of());
            case "divideRound" -> new SqlExpr.FunctionCall("roundHalfEven",
                    List.of(new SqlExpr.Binary(c.apply(params.get(0)), "/", c.apply(params.get(1))),
                            c.apply(params.get(2))));
            case "md5" -> new SqlExpr.FunctionCall("MD5", List.of(c.apply(params.get(0))));
            case "sha1" -> new SqlExpr.FunctionCall("SHA1", List.of(c.apply(params.get(0))));
            case "sha256" -> new SqlExpr.FunctionCall("SHA256", List.of(c.apply(params.get(0))));
            case "objectReferenceIn" -> new SqlExpr.In(c.apply(params.get(0)),
                    params.subList(1, params.size()).stream().map(c).toList());
            case "averageRank" -> new SqlExpr.FunctionCall("PERCENT_RANK", List.of());
            case "variantTo" -> new SqlExpr.Cast(c.apply(params.get(0)),
                    params.size() > 1 && params.get(1) instanceof CString(String typeName) ? typeName : "Any");

            // --- Null checks ---
            case "isEmpty" -> {
                if (firstArgIsList) {
                    yield new SqlExpr.Binary(
                            new SqlExpr.FunctionCall("listLength", List.of(c.apply(params.get(0)))),
                            "=", new SqlExpr.NumericLiteral(0));
                }
                yield new SqlExpr.IsNull(c.apply(params.get(0)));
            }
            case "isNotEmpty" -> {
                if (firstArgIsList) {
                    yield new SqlExpr.Binary(
                            new SqlExpr.FunctionCall("listLength", List.of(c.apply(params.get(0)))),
                            ">", new SqlExpr.NumericLiteral(0));
                }
                yield new SqlExpr.IsNotNull(c.apply(params.get(0)));
            }

            // --- Math ---
            case "abs" -> new SqlExpr.FunctionCall("ABS", List.of(c.apply(params.get(0))));
            case "ceiling", "ceil" -> new SqlExpr.Cast(
                    new SqlExpr.FunctionCall("CEIL", List.of(c.apply(params.get(0)))), "Integer");
            case "floor" -> new SqlExpr.Cast(
                    new SqlExpr.FunctionCall("FLOOR", List.of(c.apply(params.get(0)))), "Integer");
            case "round" -> {
                if (params.size() > 1) {
                    // Pure round() uses banker's rounding (round half to even)
                    yield new SqlExpr.FunctionCall("roundHalfEven",
                            List.of(c.apply(params.get(0)), c.apply(params.get(1))));
                }
                yield new SqlExpr.Cast(
                        new SqlExpr.FunctionCall("roundHalfEven",
                                List.of(c.apply(params.get(0)), new SqlExpr.NumericLiteral(0))),
                        "Integer");
            }
            case "sqrt" -> new SqlExpr.FunctionCall("SQRT", List.of(c.apply(params.get(0))));
            case "pow", "power" -> new SqlExpr.FunctionCall("POW",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            case "log" -> new SqlExpr.FunctionCall(params.size() > 1 ? "LOG" : "LN",
                    params.stream().map(c).collect(Collectors.toList()));
            case "exp" -> new SqlExpr.FunctionCall("EXP", List.of(c.apply(params.get(0))));
            case "roundHalfEven" -> {
                if (params.size() > 1) {
                    yield new SqlExpr.FunctionCall("roundHalfEven",
                            List.of(c.apply(params.get(0)), c.apply(params.get(1))));
                }
                yield new SqlExpr.FunctionCall("roundHalfEven", List.of(c.apply(params.get(0))));
            }

            // --- Cast ---
            case "toInteger", "parseInteger", "toFloat", "parseFloat", "toDecimal" -> {
                // Read target type from compiler's TypeInfo — compiler is the source of truth
                TypeInfo castInfo = unit.types().get(af);
                String castType = (castInfo != null && castInfo.isScalar())
                        ? castInfo.type().typeName() : switch (funcName) {
                            case "toInteger", "parseInteger" -> "Integer";
                            case "toFloat", "parseFloat" -> "Float";
                            default -> "Decimal";
                        };
                yield new SqlExpr.Cast(c.apply(params.get(0)), castType);
            }
            case "parseDecimal" -> {
                // parseDecimal: strip d/D suffix then CAST — read type from TypeInfo
                // Must CAST input to VARCHAR first for REGEXP_REPLACE compatibility
                TypeInfo castInfo = unit.types().get(af);
                String castType = (castInfo != null && castInfo.isScalar())
                        ? castInfo.type().typeName() : "Decimal(38,18)";
                SqlExpr input = new SqlExpr.Cast(c.apply(params.get(0)), "String");
                yield new SqlExpr.Cast(
                        new SqlExpr.FunctionCall("regexpReplace",
                                List.of(input, new SqlExpr.StringLiteral("[dD]$"),
                                        new SqlExpr.StringLiteral(""))),
                        castType);
            }

            // --- If/Case ---
            case "if" -> {
                if (params.size() >= 3) {
                    // Standard if(cond, then, else)
                    SqlExpr cond = c.apply(params.get(0));
                    LambdaFunction thenLambda = (LambdaFunction) params.get(1);
                    LambdaFunction elseLambda = (LambdaFunction) params.get(2);
                    SqlExpr thenVal = generateScalar(thenLambda.body().get(0), rowParam, store, tableAlias);
                    SqlExpr elseVal = generateScalar(elseLambda.body().get(0), rowParam, store, tableAlias);
                    yield new SqlExpr.CaseWhen(cond, thenVal, elseVal);
                }
                // Multi-if: [pair(cond, val), ...] -> if(default) — compile to CASE WHEN
                // params[0] is Collection of pairs, params[1] is else lambda
                if (params.size() >= 2 && params.get(0) instanceof PureCollection(List<ValueSpecification> values)) {
                    List<SqlExpr.SearchedCase.WhenBranch> branches = new ArrayList<>();
                    for (var pairExpr : values) {
                        if (pairExpr instanceof AppliedFunction pairAf
                                && pairAf.parameters().size() >= 2) {
                            // pair(|condition, |value) — both are lambdas
                            var condParam = pairAf.parameters().get(0);
                            var valParam = pairAf.parameters().get(1);
                            SqlExpr condExpr;
                            if (condParam instanceof LambdaFunction condLf && !condLf.body().isEmpty()) {
                                condExpr = generateScalar(condLf.body().get(0), rowParam, store, tableAlias);
                            } else {
                                condExpr = c.apply(condParam);
                            }
                            SqlExpr valExpr;
                            if (valParam instanceof LambdaFunction valLf && !valLf.body().isEmpty()) {
                                valExpr = generateScalar(valLf.body().get(0), rowParam, store, tableAlias);
                            } else {
                                valExpr = c.apply(valParam);
                            }
                            branches.add(new SqlExpr.SearchedCase.WhenBranch(condExpr, valExpr));
                        }
                    }
                    // else value from params[1] (lambda)
                    SqlExpr elseExpr;
                    if (params.get(1) instanceof LambdaFunction elseLf && !elseLf.body().isEmpty()) {
                        elseExpr = generateScalar(elseLf.body().get(0), rowParam, store, tableAlias);
                    } else {
                        elseExpr = c.apply(params.get(1));
                    }
                    yield new SqlExpr.SearchedCase(branches, elseExpr);
                }
                yield c.apply(params.get(0));
            }

            // --- In ---
            case "in" -> {
                SqlExpr left = c.apply(params.get(0));
                if (params.get(1) instanceof PureCollection(List<ValueSpecification> values)) {
                    // Check if this is a mixed-type list — needs TO_JSON wrapping
                    TypeInfo listInfo = unit.typeInfoFor(params.get(1));
                    if (listInfo != null && listInfo.isMixedList()
                            && values.stream().noneMatch(v -> v instanceof ClassInstance)) {
                        var wrappedElems = values.stream()
                                .map(v -> (SqlExpr) new SqlExpr.FunctionCall("toJson",
                                        List.of(c.apply(v))))
                                .collect(Collectors.toList());
                        SqlExpr wrappedList = new SqlExpr.ArrayLiteral(wrappedElems);
                        SqlExpr wrappedSearch = new SqlExpr.FunctionCall("toJson", List.of(left));
                        yield new SqlExpr.ListContains(wrappedList, wrappedSearch);
                    }
                    var vals = values.stream().map(c).collect(Collectors.toList());
                    yield new SqlExpr.In(left, vals);
                }
                yield new SqlExpr.In(left, List.of(c.apply(params.get(1))));
            }

            // --- Coalesce ---
            case "coalesce" -> {
                // Map empty list [] to NULL for COALESCE semantics
                java.util.List<SqlExpr> coalArgs = new java.util.ArrayList<>();
                for (var p : params) {
                    if (p instanceof PureCollection(List<ValueSpecification> values) && values.isEmpty()) {
                        coalArgs.add(new SqlExpr.NullLiteral());
                    } else {
                        coalArgs.add(c.apply(p));
                    }
                }
                yield new SqlExpr.FunctionCall("COALESCE", coalArgs);
            }

            // --- Date adjust ---
            case "adjust" -> {
                if (params.size() < 2) {
                    throw new PureCompileException(
                            "PlanGenerator: adjust() requires at least 2 parameters, got " + params.size());
                }
                SqlExpr dateExpr = c.apply(params.get(0));
                SqlExpr amount = c.apply(params.get(1));
                String unit = "DAY";
                if (params.size() > 2 && params.get(2) instanceof EnumValue ev) {
                    unit = switch (ev.value()) {
                        case "DAYS" -> "DAY";
                        case "HOURS" -> "HOUR";
                        case "MINUTES" -> "MINUTE";
                        case "SECONDS" -> "SECOND";
                        case "MILLISECONDS" -> "MILLISECOND";
                        case "MICROSECONDS" -> "MICROSECOND";
                        case "MONTHS" -> "MONTH";
                        case "YEARS" -> "YEAR";
                        case "WEEKS" -> "WEEK";
                        default -> ev.value();
                    };
                }
                SqlExpr adjusted = new SqlExpr.DateAdd(dateExpr, amount, unit);
                // Check input date precision for proper output wrapping
                if (params.get(0) instanceof CStrictDate(String value)) {
                    if (value.matches("\\d{4}")) {
                        // Year-only: wrap in strftime('%Y', CAST(adjusted AS DATE))
                        yield new SqlExpr.FunctionCall("strftime", List.of(
                                new SqlExpr.Cast(adjusted, "Date"),
                                new SqlExpr.StringLiteral("%Y")));
                    } else if (value.matches("\\d{4}-\\d{2}")) {
                        // Year-month: wrap in strftime('%Y-%m', CAST(adjusted AS DATE))
                        yield new SqlExpr.FunctionCall("strftime", List.of(
                                new SqlExpr.Cast(adjusted, "Date"),
                                new SqlExpr.StringLiteral("%Y-%m")));
                    }
                    // Full date: keep CAST to Date
                    yield new SqlExpr.Cast(adjusted, "Date");
                }
                // CDateTime (timestamp): no CAST wrapping
                if (params.get(0) instanceof CDateTime) {
                    yield adjusted;
                }
                // Column ref or other non-literal: keep CAST to Date
                yield new SqlExpr.Cast(adjusted, "Date");
            }

            // --- Trig ---
            case "sin" -> new SqlExpr.FunctionCall("SIN", List.of(c.apply(params.get(0))));
            case "cos" -> new SqlExpr.FunctionCall("COS", List.of(c.apply(params.get(0))));
            case "tan" -> new SqlExpr.FunctionCall("TAN", List.of(c.apply(params.get(0))));
            case "asin" -> new SqlExpr.FunctionCall("ASIN", List.of(c.apply(params.get(0))));
            case "acos" -> new SqlExpr.FunctionCall("ACOS", List.of(c.apply(params.get(0))));
            case "atan" -> new SqlExpr.FunctionCall("ATAN", List.of(c.apply(params.get(0))));
            case "atan2" -> new SqlExpr.FunctionCall("ATAN2",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            case "toRadians" -> new SqlExpr.FunctionCall("RADIANS", List.of(c.apply(params.get(0))));
            case "toDegrees" -> new SqlExpr.FunctionCall("DEGREES", List.of(c.apply(params.get(0))));
            case "pi" -> new SqlExpr.FunctionCall("PI", List.of());
            case "sinh" -> new SqlExpr.FunctionCall("sinh", List.of(c.apply(params.get(0))));
            case "cosh" -> new SqlExpr.FunctionCall("cosh", List.of(c.apply(params.get(0))));
            case "tanh" -> new SqlExpr.FunctionCall("tanh", List.of(c.apply(params.get(0))));
            case "cot" -> new SqlExpr.FunctionCall("cot", List.of(c.apply(params.get(0))));
            case "cbrt" -> new SqlExpr.FunctionCall("cbrt", List.of(c.apply(params.get(0))));

            // --- More math ---
            case "mod" -> {
                // Pure mod always returns non-negative: MOD((MOD(x, n) + n), n)
                SqlExpr x = c.apply(params.get(0));
                SqlExpr n = c.apply(params.get(1));
                SqlExpr innerMod = new SqlExpr.FunctionCall("MOD", List.of(x, n));
                SqlExpr adjusted = new SqlExpr.Binary(innerMod, "+", n);
                yield new SqlExpr.FunctionCall("MOD", List.of(adjusted, n));
            }
            case "log10" -> new SqlExpr.FunctionCall("LOG10", List.of(c.apply(params.get(0))));
            case "sign" -> new SqlExpr.Cast(
                    new SqlExpr.FunctionCall("SIGN", List.of(c.apply(params.get(0)))), "Integer");

            // --- More string ---
            case "reverseString" -> new SqlExpr.FunctionCall("reverseString", List.of(c.apply(params.get(0))));
            case "repeatString" -> new SqlExpr.FunctionCall("REPEAT",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            case "left" -> new SqlExpr.FunctionCall("left",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            case "right" -> new SqlExpr.FunctionCall("right",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            case "ltrim" -> new SqlExpr.FunctionCall("ltrim", List.of(c.apply(params.get(0))));
            case "rtrim" -> new SqlExpr.FunctionCall("rtrim", List.of(c.apply(params.get(0))));
            case "split" -> new SqlExpr.FunctionCall("split",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            case "matches" -> new SqlExpr.FunctionCall("matches",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            case "toUpperFirstCharacter" -> new SqlExpr.FunctionCall("toUpperFirstCharacter",
                    List.of(c.apply(params.get(0))));
            case "toLowerFirstCharacter" -> new SqlExpr.FunctionCall("toLowerFirstCharacter",
                    List.of(c.apply(params.get(0))));
            case "jaroWinklerSimilarity" -> new SqlExpr.FunctionCall("jaroWinklerSimilarity",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            case "hashCode" -> new SqlExpr.FunctionCall("hashCode", List.of(c.apply(params.get(0))));
            case "splitPart" -> {
                SqlExpr str = c.apply(params.get(0));
                SqlExpr delim = c.apply(params.get(1));
                // Pure 0-based index -> SQL 1-based: offset + 1
                SqlExpr idx = new SqlExpr.Binary(c.apply(params.get(2)), "+", new SqlExpr.NumericLiteral(1));
                // Empty input -> NULL
                if (params.get(0) instanceof PureCollection(List<ValueSpecification> values) && values.isEmpty()) {
                    yield new SqlExpr.NullLiteral();
                }
                yield new SqlExpr.CaseWhen(
                        new SqlExpr.Binary(delim, "=", new SqlExpr.StringLiteral("")),
                        str,
                        new SqlExpr.FunctionCall("splitPart", List.of(str, delim, idx)));
            }
            case "joinStrings" -> {
                if (params.size() == 4) {
                    // joinStrings(list, prefix, separator, suffix)
                    yield new SqlExpr.FunctionCall("joinStringsWithPrefixSuffix",
                            List.of(c.apply(params.get(0)), c.apply(params.get(1)),
                                    c.apply(params.get(2)), c.apply(params.get(3))));
                }
                if (params.size() > 1) {
                    SqlExpr listArg = c.apply(params.get(0));
                    // If first arg is not a list, wrap in list brackets
                    if (!firstArgIsList) {
                        listArg = new SqlExpr.FunctionCall("wrapList", List.of(listArg));
                    }
                    yield new SqlExpr.FunctionCall("arrayToString",
                            List.of(listArg, c.apply(params.get(1))));
                }
                yield new SqlExpr.FunctionCall("CONCAT", List.of(c.apply(params.get(0))));
            }
            case "find" -> {
                if (firstArgIsList && params.size() > 1 && params.get(1) instanceof LambdaFunction(
                        List<Variable> parameters, List<ValueSpecification> body
                )) {
                    // Collection find: LIST_EXTRACT(list_filter(list, (s) -> predicate), 1)
                    String paramName = parameters.isEmpty() ? "s" : parameters.get(0).name();
                    SqlExpr lambdaBody = !body.isEmpty()
                            ? c.apply(body.getLast()) : new SqlExpr.NullLiteral();
                    SqlExpr lambda = new SqlExpr.LambdaExpr(List.of(paramName), lambdaBody);
                    yield new SqlExpr.FunctionCall("listFind",
                            List.of(c.apply(params.get(0)), lambda));
                }
                yield new SqlExpr.StrPosition(c.apply(params.get(1)), c.apply(params.get(0)));
            }
            case "levenshteinDistance" -> new SqlExpr.FunctionCall("levenshteinDistance",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            case "ascii" -> new SqlExpr.FunctionCall("ASCII", List.of(c.apply(params.get(0))));
            case "char" -> new SqlExpr.FunctionCall("CHR", List.of(c.apply(params.get(0))));
            case "hash" -> {
                // Dispatch on algorithm: may arrive as EnumValue, CString, or PackageableElementPtr
                if (params.size() > 1 && params.get(1) instanceof EnumValue ev) {
                    String hashAlgo = ev.value().toUpperCase();
                    yield new SqlExpr.FunctionCall(hashAlgo, List.of(c.apply(params.get(0))));
                }
                if (params.size() > 1 && params.get(1) instanceof CString(String algoStr)) {
                    String hashAlgo = algoStr.toUpperCase();
                    yield new SqlExpr.FunctionCall(hashAlgo, List.of(c.apply(params.get(0))));
                }
                if (params.size() > 1 && params.get(1) instanceof PackageableElementPtr(String fullPath)) {
                    if (fullPath.contains("SHA256"))
                        yield new SqlExpr.FunctionCall("SHA256", List.of(c.apply(params.get(0))));
                    if (fullPath.contains("MD5"))
                        yield new SqlExpr.FunctionCall("MD5", List.of(c.apply(params.get(0))));
                }
                yield new SqlExpr.FunctionCall("hash", List.of(c.apply(params.get(0))));
            }
            case "lpad" -> {
                SqlExpr str = c.apply(params.get(0));
                SqlExpr len = c.apply(params.get(1));
                SqlExpr fill = params.size() > 2 ? c.apply(params.get(2)) : new SqlExpr.StringLiteral(" ");
                yield new SqlExpr.FunctionCall("lpadSafe",
                        List.of(str, len, fill));
            }
            case "rpad" -> {
                SqlExpr str = c.apply(params.get(0));
                SqlExpr len = c.apply(params.get(1));
                SqlExpr fill = params.size() > 2 ? c.apply(params.get(2)) : new SqlExpr.StringLiteral(" ");
                yield new SqlExpr.FunctionCall("rpadSafe",
                        List.of(str, len, fill));
            }
            case "encodeBase64" -> new SqlExpr.FunctionCall("encodeBase64",
                    List.of(c.apply(params.get(0))));
            case "decodeBase64" -> new SqlExpr.FunctionCall("decodeBase64",
                    List.of(c.apply(params.get(0))));
            case "parseDate" -> new SqlExpr.Cast(c.apply(params.get(0)), "TimestampTZ");
            case "parseBoolean" -> new SqlExpr.Cast(c.apply(params.get(0)), "Boolean");

            // --- Date/time extraction: context-aware ---
            // Literal dates → function form (YEAR(x)), column refs → EXTRACT form
            case "year" -> {
                var arg = params.get(0);
                boolean isLiteral = unit.typeInfoFor(arg) != null && unit.typeInfoFor(arg).isDateType();
                yield new SqlExpr.FunctionCall(
                        isLiteral ? "year" : "extractYear", List.of(c.apply(arg)));
            }
            case "monthNumber", "month" -> {
                var arg = params.get(0);
                boolean isLiteral = unit.typeInfoFor(arg) != null && unit.typeInfoFor(arg).isDateType();
                yield new SqlExpr.FunctionCall(
                        isLiteral ? "month" : "extractMonth", List.of(c.apply(arg)));
            }
            case "dayOfMonth", "day" -> {
                var arg = params.get(0);
                boolean isLiteral = unit.typeInfoFor(arg) != null && unit.typeInfoFor(arg).isDateType();
                yield new SqlExpr.FunctionCall(
                        isLiteral ? "dayOfMonth" : "extractDay", List.of(c.apply(arg)));
            }
            case "hour" -> {
                var arg = params.get(0);
                boolean isLiteral = unit.typeInfoFor(arg) != null && unit.typeInfoFor(arg).isDateType();
                yield new SqlExpr.FunctionCall(
                        isLiteral ? "hour" : "extractHour", List.of(c.apply(arg)));
            }
            case "minute" -> new SqlExpr.FunctionCall("minute", List.of(c.apply(params.get(0))));
            case "second" -> new SqlExpr.FunctionCall("second", List.of(c.apply(params.get(0))));
            case "quarter" -> new SqlExpr.FunctionCall("quarter", List.of(c.apply(params.get(0))));
            case "quarterNumber" -> new SqlExpr.FunctionCall("quarterNumber",
                    List.of(c.apply(params.get(0))));
            case "dayOfWeek" -> new SqlExpr.FunctionCall("dayOfWeek",
                    List.of(c.apply(params.get(0))));
            case "dayOfWeekNumber" -> new SqlExpr.FunctionCall("dayOfWeekNumber",
                    List.of(c.apply(params.get(0))));
            case "dayOfYear" -> new SqlExpr.FunctionCall("dayOfYear",
                    List.of(c.apply(params.get(0))));
            case "weekOfYear" -> new SqlExpr.FunctionCall("weekOfYear",
                    List.of(c.apply(params.get(0))));

            // --- Date truncation ---
            case "firstDayOfMonth" -> new SqlExpr.FunctionCall("firstDayOfMonth",
                    List.of(c.apply(params.get(0))));
            case "firstDayOfYear" -> new SqlExpr.FunctionCall("firstDayOfYear",
                    List.of(c.apply(params.get(0))));
            case "firstDayOfQuarter" -> new SqlExpr.FunctionCall("firstDayOfQuarter",
                    List.of(c.apply(params.get(0))));
            case "firstHourOfDay" -> new SqlExpr.FunctionCall("firstHourOfDay",
                    List.of(c.apply(params.get(0))));
            case "firstMillisecondOfSecond" -> new SqlExpr.FunctionCall("firstMillisecondOfSecond",
                    List.of(c.apply(params.get(0))));

            // --- Epoch conversion ---
            case "fromEpochValue" -> new SqlExpr.FunctionCall("fromEpochValue",
                    List.of(c.apply(params.get(0))));
            case "toEpochValue" -> new SqlExpr.FunctionCall("toEpochValue",
                    List.of(c.apply(params.get(0))));
            case "datePart" -> {
                // datePart on year/year-month uses STRFTIME; on full date/timestamp uses
                // DATE_TRUNC
                if (params.get(0) instanceof CStrictDate(String value)) {
                    if (value.matches("\\d{4}")) {
                        yield new SqlExpr.FunctionCall("strftime",
                                List.of(c.apply(params.get(0)), new SqlExpr.StringLiteral("%Y")));
                    } else if (value.matches("\\d{4}-\\d{2}")) {
                        yield new SqlExpr.FunctionCall("strftime",
                                List.of(c.apply(params.get(0)), new SqlExpr.StringLiteral("%Y-%m")));
                    }
                }
                yield new SqlExpr.FunctionCall("dateTruncDay",
                        List.of(c.apply(params.get(0))));
            }
            case "dateDiff" -> {
                SqlExpr start = c.apply(params.get(0));
                SqlExpr end = c.apply(params.get(1));
                String dunit = "DAY";
                if (params.size() > 2 && params.get(2) instanceof EnumValue ev) {
                    dunit = switch (ev.value()) {
                        case "DAYS" -> "DAY";
                        case "HOURS" -> "HOUR";
                        case "MINUTES" -> "MINUTE";
                        case "SECONDS" -> "SECOND";
                        case "MONTHS" -> "MONTH";
                        case "YEARS" -> "YEAR";
                        case "WEEKS" -> "WEEK";
                        default -> ev.value();
                    };
                }
                yield new SqlExpr.FunctionCall("dateDiff",
                        List.of(new SqlExpr.IntervalLiteral(dunit), start, end));
            }
            case "date" -> {
                SqlExpr year = c.apply(params.get(0));
                if (params.size() == 1) {
                    // Year only: strftime(makeDate(year,1,1), '%Y')
                    yield new SqlExpr.FunctionCall("strftime", List.of(
                            new SqlExpr.FunctionCall("makeDate", List.of(year,
                                    new SqlExpr.NumericLiteral(1), new SqlExpr.NumericLiteral(1))),
                            new SqlExpr.StringLiteral("%Y")));
                } else if (params.size() == 2) {
                    // Year-month: strftime(makeDate(year,month,1), '%Y-%m')
                    yield new SqlExpr.FunctionCall("strftime", List.of(
                            new SqlExpr.FunctionCall("makeDate", List.of(year,
                                    c.apply(params.get(1)), new SqlExpr.NumericLiteral(1))),
                            new SqlExpr.StringLiteral("%Y-%m")));
                } else if (params.size() == 3) {
                    // Full date: makeDate(year,month,day)
                    yield new SqlExpr.FunctionCall("makeDate",
                            List.of(year, c.apply(params.get(1)), c.apply(params.get(2))));
                } else if (params.size() == 4) {
                    // To hour: strftime(makeTimestamp(y,m,d,h,0,0), '%Y-%m-%dT%H')
                    yield new SqlExpr.FunctionCall("strftime", List.of(
                            new SqlExpr.FunctionCall("makeTimestamp", List.of(
                                    year, c.apply(params.get(1)), c.apply(params.get(2)),
                                    c.apply(params.get(3)), new SqlExpr.NumericLiteral(0), new SqlExpr.NumericLiteral(0))),
                            new SqlExpr.StringLiteral("%Y-%m-%dT%H")));
                } else if (params.size() == 5) {
                    // To minute: strftime(makeTimestamp(y,m,d,h,min,0), '%Y-%m-%dT%H:%M')
                    yield new SqlExpr.FunctionCall("strftime", List.of(
                            new SqlExpr.FunctionCall("makeTimestamp", List.of(
                                    year, c.apply(params.get(1)), c.apply(params.get(2)),
                                    c.apply(params.get(3)), c.apply(params.get(4)), new SqlExpr.NumericLiteral(0))),
                            new SqlExpr.StringLiteral("%Y-%m-%dT%H:%M")));
                } else {
                    // To second: regexpReplace(strftime(makeTimestamp(y,m,d,h,min,sec), ...), '0{1,5}$', '')
                    SqlExpr makeTs = new SqlExpr.FunctionCall("makeTimestamp", List.of(
                            year, c.apply(params.get(1)), c.apply(params.get(2)),
                            c.apply(params.get(3)), c.apply(params.get(4)), c.apply(params.get(5))));
                    yield new SqlExpr.FunctionCall("regexpReplace", List.of(
                            new SqlExpr.FunctionCall("strftime", List.of(
                                    makeTs, new SqlExpr.StringLiteral("%Y-%m-%dT%H:%M:%S.%f"))),
                            new SqlExpr.StringLiteral("0{1,5}$"),
                            new SqlExpr.StringLiteral("")));
                }
            }
            case "timeBucket" -> {
                // timeBucket(date, quantity, DurationUnit)
                // Context-aware: literal dates use TO_DAYS+origin+CAST, column refs use INTERVAL
                var dateParam = params.get(0);
                SqlExpr dateExpr = c.apply(dateParam);
                SqlExpr quantityExpr = c.apply(params.get(1));
                String tbUnit = "days";
                if (params.size() > 2 && params.get(2) instanceof EnumValue ev) {
                    tbUnit = ev.value().toLowerCase();
                }
                TypeInfo dateTypeInfo = unit.typeInfoFor(dateParam);
                if (dateTypeInfo != null && dateTypeInfo.isDateType()) {
                    // Literal date path: TO_DAYS/TO_WEEKS + origin + CAST
                    // Pass type info so dialect can render correctly
                    String castType = (dateTypeInfo.type() == GenericType.Primitive.DATE_TIME)
                            ? "TimestampNS" : "Date";
                    yield new SqlExpr.FunctionCall("timeBucketScalar",
                            List.of(quantityExpr, new SqlExpr.StringLiteral(tbUnit),
                                    dateExpr, new SqlExpr.StringLiteral(castType)));
                }
                // Column ref path: INTERVAL syntax
                yield new SqlExpr.FunctionCall("timeBucket",
                        List.of(quantityExpr, new SqlExpr.StringLiteral(tbUnit), dateExpr));
            }
            case "hasHour" -> {
                // Check date precision: hasHour is true for DateTime, false for StrictDate
                if (params.get(0) instanceof CDateTime) {
                    yield new SqlExpr.BoolLiteral(true);
                } else if (params.get(0) instanceof CStrictDate) {
                    yield new SqlExpr.BoolLiteral(false);
                }
                yield new SqlExpr.FunctionCall("HOUR", List.of(c.apply(params.get(0))));
            }
            case "hasMinute" -> {
                // Check date precision: hasMinute is true only for DateTime with minute
                // component
                if (params.get(0) instanceof CDateTime(String value)) {
                    // Dates like %2015-04-15T17 have hour but no minute
                    boolean hasMin = value.matches(".*T\\d{2}:\\d{2}.*");
                    yield new SqlExpr.BoolLiteral(hasMin);
                } else if (params.get(0) instanceof CStrictDate) {
                    yield new SqlExpr.BoolLiteral(false);
                }
                yield new SqlExpr.FunctionCall("MINUTE", List.of(c.apply(params.get(0))));
            }
            case "hasMonth" -> {
                // Pure dates: year-only has no month, everything else has month
                if (params.get(0) instanceof CStrictDate || params.get(0) instanceof CDateTime) {
                    yield new SqlExpr.BoolLiteral(true);
                }
                yield new SqlExpr.FunctionCall("MONTH", List.of(c.apply(params.get(0))));
            }
            case "hasDay" -> {
                // hasDay: true for full dates (YYYY-MM-DD), false for year-only/year-month
                if (params.get(0) instanceof CDateTime) {
                    yield new SqlExpr.BoolLiteral(true);
                } else if (params.get(0) instanceof CStrictDate(String value)) {
                    boolean hasDay = value.matches("\\d{4}-\\d{2}-\\d{2}.*");
                    yield new SqlExpr.BoolLiteral(hasDay);
                }
                yield new SqlExpr.FunctionCall("DAY", List.of(c.apply(params.get(0))));
            }
            case "hasSecond" -> {
                // hasSecond: true for DateTime with seconds component
                if (params.get(0) instanceof CDateTime(String value)) {
                    boolean hasSec = value.matches(".*T\\d{2}:\\d{2}:\\d{2}.*");
                    yield new SqlExpr.BoolLiteral(hasSec);
                } else if (params.get(0) instanceof CStrictDate) {
                    yield new SqlExpr.BoolLiteral(false);
                }
                yield new SqlExpr.FunctionCall("SECOND", List.of(c.apply(params.get(0))));
            }
            case "hasSubsecond" -> {
                // hasSubsecond: true for DateTime with fractional seconds (.NNN)
                if (params.get(0) instanceof CDateTime(String value)) {
                    boolean hasFrac = value.matches(".*\\.\\d+.*");
                    yield new SqlExpr.BoolLiteral(hasFrac);
                } else if (params.get(0) instanceof CStrictDate) {
                    yield new SqlExpr.BoolLiteral(false);
                }
                yield new SqlExpr.BoolLiteral(false);
            }
            case "hasSubsecondWithAtLeastPrecision" -> {
                // hasSubsecondWithAtLeastPrecision(date, n): check fractional digit count >= n
                if (params.get(0) instanceof CDateTime(String value)) {
                    int fracDigits = 0;
                    int dotIdx = value.indexOf('.');
                    if (dotIdx >= 0) {
                        // Count digits after the dot until non-digit
                        for (int fi = dotIdx + 1; fi < value.length() && Character.isDigit(value.charAt(fi)); fi++) {
                            fracDigits++;
                        }
                    }
                    long required = 1;
                    if (params.size() > 1 && params.get(1) instanceof CInteger(Number precValue)) {
                        required = precValue.longValue();
                    }
                    yield new SqlExpr.BoolLiteral(fracDigits >= required);
                } else if (params.get(0) instanceof CStrictDate) {
                    yield new SqlExpr.BoolLiteral(false);
                }
                yield new SqlExpr.BoolLiteral(false);
            }

            // --- Date constants (SQL keywords, not functions — no parens) ---
            case "now" -> new SqlExpr.CurrentTimestamp();
            case "today" -> new SqlExpr.CurrentDate();

            // --- Date comparisons: wrap both sides in DATE_TRUNC('day', ...) ---
            case "isOnDay", "isAfterDay", "isBeforeDay", "isOnOrAfterDay", "isOnOrBeforeDay" -> {
                String dateOp = switch (funcName) {
                    case "isOnDay" -> "=";
                    case "isAfterDay" -> ">";
                    case "isBeforeDay" -> "<";
                    case "isOnOrAfterDay" -> ">=";
                    case "isOnOrBeforeDay" -> "<=";
                    default -> "=";
                };
                SqlExpr left = new SqlExpr.FunctionCall("dateTruncDay", List.of(c.apply(params.get(0))));
                SqlExpr right = new SqlExpr.FunctionCall("dateTruncDay", List.of(c.apply(params.get(1))));
                yield new SqlExpr.Grouped(new SqlExpr.Binary(left, dateOp, right));
            }

            // --- Bitwise ---
            case "bitAnd" -> new SqlExpr.Binary(
                    c.apply(params.get(0)), "&",
                    c.apply(params.get(1)));
            case "bitOr" -> new SqlExpr.Binary(
                    c.apply(params.get(0)), "|",
                    c.apply(params.get(1)));
            case "bitXor" -> new SqlExpr.FunctionCall("bitXor",
                    List.of(c.apply(params.get(0)),
                            c.apply(params.get(1))));
            case "bitNot" -> new SqlExpr.Unary("~",
                    c.apply(params.get(0)));
            case "bitShiftLeft" -> new SqlExpr.Binary(
                    new SqlExpr.Cast(c.apply(params.get(0)), "Integer"), "<<",
                    c.apply(params.get(1)));
            case "bitShiftRight" -> {
                // Old pipeline: CASE WHEN shift > 62 THEN CAST(1 AS BIGINT) << shift
                //               ELSE CAST(val AS BIGINT) >> shift END
                SqlExpr val = new SqlExpr.Cast(c.apply(params.get(0)), "Integer");
                SqlExpr shift = c.apply(params.get(1));
                yield new SqlExpr.FunctionCall("bitShiftRightSafe",
                        List.of(val, shift));
            }
            case "xor" -> {
                // Boolean xor: (A AND NOT B) OR (NOT A AND B)
                SqlExpr a = c.apply(params.get(0));
                SqlExpr b = c.apply(params.get(1));
                yield new SqlExpr.Or(List.of(
                        new SqlExpr.And(List.of(a, new SqlExpr.Not(b))),
                        new SqlExpr.And(List.of(new SqlExpr.Not(a), b))));
            }

            // --- write: Relation → Integer (row count) ---
            case "write" -> {
                // write(source, target) returns row count — compile source as subquery
                SqlBuilder source = generateRelation(params.get(0));
                SqlBuilder countQuery = new SqlBuilder()
                        .addSelect(new SqlExpr.FunctionCall("COUNT",
                                List.of(new SqlExpr.StringLiteral("*"))), null)
                        .fromSubquery(source, "write_src");
                yield new SqlExpr.Subquery(countQuery);
            }

            // --- Collection/list ---
            case "size", "count" -> {
                // Dispatch based on source type
                if (!params.isEmpty()) {
                    TypeInfo sourceInfo = unit.typeInfoFor(params.get(0));
                    if (sourceInfo.isRelational()) {
                        // Relational size: (SELECT COUNT('*') FROM (subquery) AS subq)
                        SqlBuilder source = generateRelation(params.get(0));
                        SqlBuilder countQuery = new SqlBuilder()
                                .addSelect(new SqlExpr.FunctionCall("COUNT",
                                        List.of(new SqlExpr.StringLiteral("*"))), null)
                                .fromSubquery(source, "subq");
                        yield new SqlExpr.Subquery(countQuery);
                    }
                    // Collection/list size: use LEN (not aggregate COUNT)
                    // isMany(): multiplicity [*] expressions (PureCollections, match vars, etc.)
                    // lambdaParam(): fold accumulator — V[1] in Pure but LIST in SQL
                    if (sourceInfo.isMany() || sourceInfo.lambdaParam()) {
                        yield new SqlExpr.FunctionCall("listLength",
                                List.of(c.apply(params.get(0))));
                    }
                }
                // No fallback — compiler must type the source. Fix TypeChecker if this fires.
                throw new PureCompileException(
                        "PlanGenerator: size() source has no TypeInfo. Compiler bug — fix TypeChecker.");
            }
            case "at" -> {
                // at(0) on a scalar (e.g., struct field p.lastName) is identity — skip LIST_EXTRACT
                if (!firstArgIsList) {
                    yield c.apply(params.get(0));
                }
                yield new SqlExpr.FunctionCall("listExtract",
                    List.of(c.apply(params.get(0)),
                            new SqlExpr.Binary(c.apply(params.get(1)), "+",
                                    new SqlExpr.NumericLiteral(1))));
            }
            case "head", "first" -> new SqlExpr.FunctionCall("listExtract",
                    List.of(c.apply(params.get(0)), new SqlExpr.NumericLiteral(1)));
            case "last" -> {
                SqlExpr arg = c.apply(params.get(0));
                yield new SqlExpr.FunctionCall("listExtract",
                        List.of(arg,
                                new SqlExpr.FunctionCall("listLength", List.of(arg))));
            }
            case "tail" -> {
                SqlExpr listArg = firstArgIsList ? c.apply(params.get(0))
                        : new SqlExpr.FunctionCall("wrapList", List.of(c.apply(params.get(0))));
                yield new SqlExpr.FunctionCall("listSlice",
                        List.of(listArg, new SqlExpr.NumericLiteral(2),
                                new SqlExpr.FunctionCall("listLength", List.of(listArg))));
            }
            case "init" -> {
                SqlExpr listArg = firstArgIsList ? c.apply(params.get(0))
                        : new SqlExpr.FunctionCall("wrapList", List.of(c.apply(params.get(0))));
                yield new SqlExpr.FunctionCall("listSlice",
                        List.of(listArg, new SqlExpr.NumericLiteral(1),
                                new SqlExpr.Binary(
                                        new SqlExpr.FunctionCall("listLength", List.of(listArg)),
                                        "-", new SqlExpr.NumericLiteral(1))));
            }
            case "add" -> {
                if (params.size() > 2) {
                    // add(list, offset, value) — splice: insert value at offset position
                    SqlExpr list = c.apply(params.get(0));
                    SqlExpr offset = c.apply(params.get(1));
                    SqlExpr value = c.apply(params.get(2));
                    // LIST_CONCAT(LIST_CONCAT(LIST_SLICE(list, 1, offset), LIST_VALUE(value)),
                    //             LIST_SLICE(list, offset+1, LENGTH(list)))
                    SqlExpr leftSlice = new SqlExpr.FunctionCall("listSlice",
                            List.of(list, new SqlExpr.NumericLiteral(1), offset));
                    SqlExpr wrapped = new SqlExpr.FunctionCall("LIST_VALUE", List.of(value));
                    SqlExpr leftConcat = new SqlExpr.FunctionCall("listConcat",
                            List.of(leftSlice, wrapped));
                    SqlExpr rightStart = new SqlExpr.Binary(offset, "+", new SqlExpr.NumericLiteral(1));
                    SqlExpr listLen = new SqlExpr.FunctionCall("LENGTH", List.of(list));
                    SqlExpr rightSlice = new SqlExpr.FunctionCall("listSlice",
                            List.of(list, rightStart, listLen));
                    yield new SqlExpr.FunctionCall("listConcat",
                            List.of(leftConcat, rightSlice));
                }
                yield new SqlExpr.FunctionCall("listAppend",
                        List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            }
            case "concatenate" -> {
                // LIST_CONCAT requires list arguments — wrap scalars
                SqlExpr left = c.apply(params.get(0));
                SqlExpr right = c.apply(params.get(1));
                if (!firstArgIsList) {
                    left = new SqlExpr.FunctionCall("wrapList", List.of(left));
                }
                // Check if second arg is a scalar (not a list/collection)
                if (!(params.get(1) instanceof PureCollection)) {
                    TypeInfo rightInfo = unit.types().get(params.get(1));
                    boolean rightIsList = params.get(1) instanceof PureCollection
                            || (rightInfo != null && rightInfo.isMany());
                    if (!rightIsList) {
                        right = new SqlExpr.FunctionCall("wrapList", List.of(right));
                    }
                }

                // Cross-list mixed-type: [1,2,3].concatenate(['a','b'])
                // Each list is homogeneous individually but they have different element types
                // Wrap individual *elements* in ::VARIANT so LIST_CONCAT gets VARIANT[] on both sides.
                // VARIANT preserves original types through UNNEST (getObject returns native Java types),
                // unlike TO_JSON which returns JsonNode objects.
                // Only works for Collection nodes (literal arrays) — non-Collection args (e.g.
                // cast() expressions from fold identity) are left as-is to avoid wrapping the
                // entire array which would break LIST_CONCAT (needs arrays, not scalars)
                TypeInfo leftInfo = unit.typeInfoFor(params.get(0));
                TypeInfo rightInfo2 = unit.typeInfoFor(params.get(1));
                boolean leftEmpty = params.get(0) instanceof PureCollection(List<ValueSpecification> values)
                        && values.isEmpty();
                boolean rightEmpty = params.get(1) instanceof PureCollection(List<ValueSpecification> values)
                        && values.isEmpty();
                if (!leftEmpty && !rightEmpty
                        && leftInfo != null && rightInfo2 != null
                        && leftInfo.type() != null && rightInfo2.type() != null
                        && !leftInfo.type().equals(rightInfo2.type())
                        && params.get(0) instanceof PureCollection
                        && params.get(1) instanceof PureCollection) {
                    // Both are literal collections with different element types — wrap elements in VARIANT
                    left = wrapCollectionElementsInVariant(
                            (PureCollection) params.get(0), c);
                    right = wrapCollectionElementsInVariant(
                            (PureCollection) params.get(1), c);
                }

                yield new SqlExpr.FunctionCall("listConcat", List.of(left, right));
            }
            case "take" -> new SqlExpr.FunctionCall("listSlice",
                    List.of(c.apply(params.get(0)), new SqlExpr.NumericLiteral(1),
                            new SqlExpr.FunctionCall("greatest", List.of(
                                    c.apply(params.get(1)),
                                    new SqlExpr.NumericLiteral(0)))));
            case "drop" -> new SqlExpr.FunctionCall("listSlice",
                    List.of(c.apply(params.get(0)),
                            new SqlExpr.FunctionCall("greatest", List.of(
                                    new SqlExpr.Binary(c.apply(params.get(1)), "+",
                                            new SqlExpr.NumericLiteral(1)),
                                    new SqlExpr.NumericLiteral(1))),
                            new SqlExpr.FunctionCall("listLength",
                                    List.of(c.apply(params.get(0))))));
            case "slice" -> {
                SqlExpr list = c.apply(params.get(0));
                SqlExpr start = c.apply(params.get(1));
                SqlExpr end = c.apply(params.get(2));
                // Pure slice is 0-based, DuckDB LIST_SLICE is 1-based
                // Pre-compute for literal starts; negative→clamp to 1
                if (params.get(1) instanceof CInteger(Number value)) {
                    long startVal = value.longValue();
                    long offset = Math.max(startVal + 1, 1);
                    yield new SqlExpr.FunctionCall("listSlice",
                            List.of(list, new SqlExpr.NumericLiteral(offset), end));
                }
                // Dynamic: max(start + 1, 1) — clamp to 1 for negative starts
                SqlExpr startPlus1 = new SqlExpr.Binary(start, "+", new SqlExpr.NumericLiteral(1));
                SqlExpr clampedStart = new SqlExpr.FunctionCall("GREATEST",
                        List.of(startPlus1, new SqlExpr.NumericLiteral(1)));
                yield new SqlExpr.FunctionCall("listSlice",
                        List.of(list, clampedStart, end));
            }

            // --- Aggregates (list-context aware) ---
            case "sum" -> {
                if (firstArgIsList)
                    yield new SqlExpr.FunctionCall("listSum", List.of(c.apply(params.get(0))));
                yield c.apply(params.get(0)); // scalar identity
            }
            case "average", "mean" -> {
                if (firstArgIsList)
                    yield new SqlExpr.FunctionCall("listAvg", List.of(c.apply(params.get(0))));
                yield new SqlExpr.Cast(c.apply(params.get(0)), "Double"); // scalar cast
            }
            case "min" -> {
                if (params.size() > 1) {
                    // Two-arg min(a, b): when types differ, use VARIANT subquery
                    TypeInfo p0Info = unit.typeInfoFor(params.get(0));
                    TypeInfo p1Info = unit.typeInfoFor(params.get(1));
                    if (p0Info != null && p1Info != null
                            && !p0Info.type().equals(p1Info.type())
                            && (p0Info.type().isNumeric() && p1Info.type().isNumeric())) {
                        yield buildVariantMinMaxSubquery(
                                params.stream().map(c).collect(Collectors.toList()), false, false);
                    }
                    yield new SqlExpr.FunctionCall("LEAST",
                            params.stream().map(c).collect(Collectors.toList()));
                }
                if (firstArgIsList) {
                    TypeInfo listInfo = unit.typeInfoFor(params.get(0));
                    if (listInfo != null && listInfo.isHeterogeneousList()) {
                        yield buildVariantMinMaxSubquery(
                                List.of(c.apply(params.get(0))), false, listInfo.isDateList());
                    }
                    yield new SqlExpr.FunctionCall("listMin", List.of(c.apply(params.get(0))));
                }
                yield c.apply(params.get(0)); // scalar identity
            }
            case "max" -> {
                if (params.size() > 1) {
                    // Two-arg max(a, b): when types differ, use VARIANT subquery
                    TypeInfo p0Info = unit.typeInfoFor(params.get(0));
                    TypeInfo p1Info = unit.typeInfoFor(params.get(1));
                    if (p0Info != null && p1Info != null
                            && !p0Info.type().equals(p1Info.type())
                            && (p0Info.type().isNumeric() && p1Info.type().isNumeric())) {
                        yield buildVariantMinMaxSubquery(
                                params.stream().map(c).collect(Collectors.toList()), true, false);
                    }
                    yield new SqlExpr.FunctionCall("GREATEST",
                            params.stream().map(c).collect(Collectors.toList()));
                }
                if (firstArgIsList) {
                    TypeInfo listInfo = unit.typeInfoFor(params.get(0));
                    if (listInfo != null && listInfo.isHeterogeneousList()) {
                        yield buildVariantMinMaxSubquery(
                                List.of(c.apply(params.get(0))), true, listInfo.isDateList());
                    }
                    yield new SqlExpr.FunctionCall("listMax", List.of(c.apply(params.get(0))));
                }
                yield c.apply(params.get(0)); // scalar identity
            }
            case "greatest" -> {
                if (firstArgIsList) {
                    TypeInfo listInfo = unit.typeInfoFor(params.get(0));
                    if (listInfo != null && listInfo.isHeterogeneousList()) {
                        yield buildVariantMinMaxSubquery(
                                List.of(c.apply(params.get(0))), true, listInfo.isDateList());
                    }
                    yield new SqlExpr.FunctionCall("listMax",
                            params.stream().map(c).collect(Collectors.toList()));
                }
                // Single scalar: greatest(x) = x (identity)
                if (params.size() == 1) yield c.apply(params.get(0));
                yield new SqlExpr.FunctionCall("GREATEST",
                        params.stream().map(c).collect(Collectors.toList()));
            }
            case "least" -> {
                if (firstArgIsList) {
                    TypeInfo listInfo = unit.typeInfoFor(params.get(0));
                    if (listInfo != null && listInfo.isHeterogeneousList()) {
                        yield buildVariantMinMaxSubquery(
                                List.of(c.apply(params.get(0))), false, listInfo.isDateList());
                    }
                    yield new SqlExpr.FunctionCall("listMin",
                            params.stream().map(c).collect(Collectors.toList()));
                }
                // Single scalar: least(x) = x (identity)
                if (params.size() == 1) yield c.apply(params.get(0));
                yield new SqlExpr.FunctionCall("LEAST",
                        params.stream().map(c).collect(Collectors.toList()));
            }
            case "median" -> new SqlExpr.FunctionCall(
                    firstArgIsList ? "listMedian" : "median",
                    params.stream().map(c).collect(Collectors.toList()));
            case "mode" -> new SqlExpr.FunctionCall(
                    firstArgIsList ? "listMode" : "mode",
                    params.stream().map(c).collect(Collectors.toList()));

            // --- Statistical aggregates (list-context aware) ---
            case "stdDev", "stdDevSample" -> new SqlExpr.FunctionCall(
                    firstArgIsList ? "listStdDevSample" : "stdDevSample",
                    params.stream().map(c).collect(Collectors.toList()));
            case "stdDevPopulation" -> new SqlExpr.FunctionCall(
                    firstArgIsList ? "listStdDevPopulation" : "stdDevPopulation",
                    params.stream().map(c).collect(Collectors.toList()));
            case "variance", "varianceSample" -> {
                // variance(list, isSample) — isSample=true→var_samp, isSample=false→var_pop
                if (params.size() > 1 && params.get(1) instanceof CBoolean(boolean value) && !value) {
                    yield new SqlExpr.FunctionCall(
                            firstArgIsList ? "listVariancePopulation" : "variancePopulation",
                            List.of(c.apply(params.get(0))));
                }
                yield new SqlExpr.FunctionCall(
                        firstArgIsList ? "listVarianceSample" : "varianceSample",
                        List.of(c.apply(params.get(0))));
            }
            case "variancePopulation" -> new SqlExpr.FunctionCall(
                    firstArgIsList ? "listVariancePopulation" : "variancePopulation",
                    List.of(c.apply(params.get(0))));
            case "corr" -> {
                // Bivariate: always needs paired sequences via UNNEST
                SqlExpr arg0 = c.apply(params.get(0));
                if (!firstArgIsList) arg0 = new SqlExpr.ArrayLiteral(List.of(arg0));
                yield new SqlExpr.FunctionCall("listCorr", List.of(arg0, c.apply(params.get(1))));
            }
            case "covarSample" -> {
                SqlExpr arg0 = c.apply(params.get(0));
                if (!firstArgIsList) arg0 = new SqlExpr.ArrayLiteral(List.of(arg0));
                yield new SqlExpr.FunctionCall("listCovarSample", List.of(arg0, c.apply(params.get(1))));
            }
            case "covarPopulation" -> {
                SqlExpr arg0 = c.apply(params.get(0));
                if (!firstArgIsList) arg0 = new SqlExpr.ArrayLiteral(List.of(arg0));
                yield new SqlExpr.FunctionCall("listCovarPopulation", List.of(arg0, c.apply(params.get(1))));
            }
            case "percentile", "percentileCont" -> {
                SqlExpr list = c.apply(params.get(0));
                SqlExpr p = c.apply(params.get(1));
                // percentile(value, ascending, continuous)
                // param[2]: ascending — when false, invert p to (1 - p)
                boolean ascending = true;
                boolean continuous = true;
                if (params.size() > 2 && params.get(2) instanceof CBoolean(boolean value)) {
                    ascending = value;
                }
                // param[3]: continuous — when false, use percentileDisc
                if (params.size() > 3 && params.get(3) instanceof CBoolean(boolean value)) {
                    continuous = value;
                }
                if (!ascending) {
                    p = new SqlExpr.Binary(new SqlExpr.NumericLiteral(1), "-", p);
                }
                String pctFunc = continuous
                        ? (firstArgIsList ? "listPercentileCont" : "percentileCont")
                        : (firstArgIsList ? "listPercentileDisc" : "percentileDisc");
                yield new SqlExpr.FunctionCall(pctFunc, List.of(list, p));
            }
            case "percentileDisc" -> {
                SqlExpr list = c.apply(params.get(0));
                SqlExpr p = c.apply(params.get(1));
                // param[2]: ascending — when false, invert p
                if (params.size() > 2 && params.get(2) instanceof CBoolean(boolean value) && !value) {
                    p = new SqlExpr.Binary(new SqlExpr.NumericLiteral(1), "-", p);
                }
                yield new SqlExpr.FunctionCall(
                        firstArgIsList ? "listPercentileDisc" : "percentileDisc",
                        List.of(list, p));
            }

            // --- Analytical helpers: minBy/maxBy → UNNEST subquery decomposition ---
            case "maxBy", "minBy" -> {
                String aggFunc = funcName.equals("maxBy") ? "maxBy" : "minBy";
                SqlExpr list1 = c.apply(params.get(0));
                SqlExpr list2 = c.apply(params.get(1));

                if (params.size() >= 3) {
                    // TopK form: (SELECT LIST(sub.a) FROM (SELECT a FROM
                    //   (SELECT UNNEST(l1) AS a, UNNEST(l2) AS b,
                    //    UNNEST(generate_series(0, len(l1)-1)) AS rn)
                    //   ORDER BY b ASC/DESC, rn ASC LIMIT k) sub)
                    SqlExpr limit = c.apply(params.get(2));
                    String orderDir = funcName.equals("minBy") ? "ASC" : "DESC";
                    // Build as raw SQL since SqlBuilder doesn't model nested subqueries easily
                    yield new SqlExpr.FunctionCall("listMinMaxByTopK",
                            List.of(list1, list2, limit, new SqlExpr.Identifier(orderDir)));
                } else {
                    // Simple form: (SELECT ARG_MIN/MAX(a, b) FROM (SELECT UNNEST(l1) AS a, UNNEST(l2) AS b))
                    yield new SqlExpr.FunctionCall("listMinMaxBy",
                            List.of(list1, list2, new SqlExpr.Identifier(aggFunc)));
                }
            }

            // --- Variant access (read key literal directly from AST) ---
            // get() always returns untyped variant — type conversion via to()/toMany()
            case "get" -> {
                SqlExpr source = c.apply(params.get(0));
                ValueSpecification key = params.get(1);

                if (key instanceof CInteger(Number v)) {
                    yield new SqlExpr.VariantIndex(source, v.intValue());
                }
                if (key instanceof CString(String k)) {
                    yield new SqlExpr.VariantAccess(source, k);
                }
                // Dynamic key — compile as expression
                SqlExpr keyExpr = c.apply(key);
                yield new SqlExpr.VariantAccess(source, keyExpr.toSql(dialect));
            }

            // --- List/array operations ---
            case "reverse" -> new SqlExpr.FunctionCall("listReverse",
                    List.of(c.apply(params.get(0))));
            case "removeDuplicates" -> {
                SqlExpr listArg = firstArgIsList ? c.apply(params.get(0))
                        : new SqlExpr.FunctionCall("wrapList", List.of(c.apply(params.get(0))));
                yield new SqlExpr.FunctionCall("removeDuplicates", List.of(listArg));
            }
            case "removeDuplicatesBy" -> {
                // removeDuplicatesBy(list, keyFn) — distinct by key function
                SqlExpr listArg = firstArgIsList ? c.apply(params.get(0))
                        : new SqlExpr.FunctionCall("wrapList", List.of(c.apply(params.get(0))));
                if (params.size() >= 2 && params.get(1) instanceof LambdaFunction(
                        List<Variable> parameters, List<ValueSpecification> body
                )) {
                    String elemParam = parameters.isEmpty() ? "x"
                            : parameters.get(0).name();
                    SqlExpr lambdaBody = !body.isEmpty()
                            ? c.apply(body.getLast()) : new SqlExpr.NullLiteral();
                    SqlExpr lambda = new SqlExpr.LambdaExpr(List.of(elemParam), lambdaBody);
                    // list_transform then distinct
                    SqlExpr transformed = new SqlExpr.FunctionCall("listTransform",
                            List.of(listArg, lambda));
                    yield new SqlExpr.FunctionCall("removeDuplicates", List.of(transformed));
                }
                yield new SqlExpr.FunctionCall("removeDuplicates", List.of(listArg));
            }

            // --- Misc ---
            case "generateGuid" -> new SqlExpr.FunctionCall("generateGuid", List.of());
            case "between" -> new SqlExpr.And(List.of(
                    new SqlExpr.Binary(c.apply(params.get(0)), ">=", c.apply(params.get(1))),
                    new SqlExpr.Binary(c.apply(params.get(0)), "<=", c.apply(params.get(2)))));
            case "eq" -> buildComparison(params, "=", c, store, tableAlias);
            case "type" -> new SqlExpr.FunctionCall("typeOf", List.of(c.apply(params.get(0))));

            // --- forAll / exists ---
            case "forAll" -> {
                // forAll(list, {e|pred}) → COALESCE(listBoolAnd(listTransform(list, e -> pred)), TRUE)
                if (params.size() >= 2 && params.get(1) instanceof LambdaFunction(
                        List<Variable> parameters, List<ValueSpecification> body
                )) {
                    SqlExpr list = c.apply(params.get(0));
                    String elemParam = parameters.get(0).name();
                    SqlExpr predBody = !body.isEmpty()
                            ? c.apply(body.getLast()) : new SqlExpr.BoolLiteral(true);
                    SqlExpr lambda = new SqlExpr.LambdaExpr(List.of(elemParam), predBody);
                    SqlExpr transformed = new SqlExpr.FunctionCall("listTransform", List.of(list, lambda));
                    SqlExpr boolAnd = new SqlExpr.FunctionCall("listBoolAnd", List.of(transformed));
                    yield new SqlExpr.FunctionCall("COALESCE", List.of(boolAnd, new SqlExpr.BoolLiteral(true)));
                }
                if (!params.isEmpty()) yield c.apply(params.get(0));
                throw new PureCompileException("forAll: requires list and predicate lambda");
            }
            case "exists" -> {
                // Relational association EXISTS: $p.addresses->exists(a|$a.city == 'NY')
                // → EXISTS (SELECT 1 FROM T_ADDRESS AS _ex WHERE _ex.PERSON_ID = t0.ID AND pred)
                if (params.size() >= 2
                        && params.get(0) instanceof AppliedProperty assocProp
                        && params.get(1) instanceof LambdaFunction(
                                List<Variable> parameters, List<ValueSpecification> body)) {
                    String propName = assocProp.property();
                    var joinRes = findJoinResolution(propName);
                    if (joinRes != null && joinRes.joinCondition() != null) {
                        String targetTable = joinRes.targetTable();
                        String existsAlias = "_ex";

                        // Build predicate from lambda body using target resolution
                        String elemParam = parameters.get(0).name();
                        SqlExpr predBody = !body.isEmpty()
                                ? generateScalar(body.getLast(), elemParam, joinRes.targetResolution(), existsAlias)
                                : new SqlExpr.BoolLiteral(true);

                        // Build JOIN condition via generateScalar + varAliases
                        SqlExpr joinCond = generateScalar(joinRes.joinCondition(), null, null, null,
                                Map.of(joinRes.sourceParam(), tableAlias, joinRes.targetParam(), existsAlias));

                        // Combine: JOIN condition AND predicate
                        SqlExpr whereCond = new SqlExpr.Binary(joinCond, "AND", predBody);

                        // Build: EXISTS (SELECT 1 FROM target_table AS _ex WHERE ...)
                        SqlBuilder subquery = new SqlBuilder()
                                .addSelect(new SqlExpr.NumericLiteral(1), null)
                                .from(dialect.quoteIdentifier(targetTable),
                                      dialect.quoteIdentifier(existsAlias))
                                .addWhere(whereCond);
                        yield new SqlExpr.Exists(subquery);
                    }
                }
                // Struct array exists: exists(list, {f|pred}) → listLength(listFilter(list, f -> pred)) > 0
                if (params.size() >= 2 && params.get(1) instanceof LambdaFunction(
                        List<Variable> parameters, List<ValueSpecification> body
                )) {
                    SqlExpr list = c.apply(params.get(0));
                    String elemParam = parameters.get(0).name();
                    SqlExpr predBody = !body.isEmpty()
                            ? c.apply(body.getLast()) : new SqlExpr.BoolLiteral(true);
                    SqlExpr lambda = new SqlExpr.LambdaExpr(List.of(elemParam), predBody);
                    SqlExpr filtered = new SqlExpr.FunctionCall("listFilter", List.of(list, lambda));
                    SqlExpr len = new SqlExpr.FunctionCall("listLength", List.of(filtered));
                    yield new SqlExpr.Binary(len, ">", new SqlExpr.NumericLiteral(0));
                }
                if (!params.isEmpty()) yield c.apply(params.get(0));
                throw new PureCompileException("exists: requires list and predicate lambda");
            }

            // --- Scalar list operations (used in variant array chains) ---
            case "filter" -> {
                // filter(list, {t|pred}) → list_filter(list, t -> pred)
                if (params.size() >= 2 && params.get(1) instanceof LambdaFunction(
                        List<Variable> parameters, List<ValueSpecification> body
                )) {
                    SqlExpr list = c.apply(params.get(0));
                    String elemParam = parameters.get(0).name();
                    SqlExpr predBody = !body.isEmpty()
                            ? c.apply(body.getLast()) : new SqlExpr.BoolLiteral(true);
                    SqlExpr lambda = new SqlExpr.LambdaExpr(List.of(elemParam), predBody);
                    yield new SqlExpr.FunctionCall("listFilter", List.of(list, lambda));
                }
                // Relational filter pass-through (no lambda — handled in relational path)
                if (!params.isEmpty()) yield c.apply(params.get(0));
                throw new PureCompileException("filter: requires list and predicate lambda");
            }

            // --- Variant conversion functions ---
            case "toMany" -> {
                SqlExpr source = c.apply(params.get(0));
                TypeInfo info = unit.types().get(af);
                String elemType = resolveVariantSqlType(info);
                // For toMany(@Variant), type is JSON — resolveVariantSqlType returns null,
                // but we still need CAST(source AS JSON[]) to unnest the JSON array
                if (elemType == null && info != null
                        && info.type() == GenericType.Primitive.JSON) {
                    elemType = "JSON";
                }
                yield elemType != null ? new SqlExpr.VariantArrayCast(source, elemType) : source;
            }
            case "toVariant" -> {
                yield new SqlExpr.ToVariant(c.apply(params.get(0)));
            }
            case "to" -> {
                SqlExpr source = c.apply(params.get(0));
                TypeInfo info = unit.types().get(af);
                String sqlType = resolveVariantSqlType(info);
                if (sqlType != null) {
                    // Convert VariantAccess (->) to VariantTextAccess (->>) before CAST
                    // so JSON string quotes are stripped before type conversion
                    if (source instanceof SqlExpr.VariantAccess va) {
                        source = new SqlExpr.VariantTextAccess(va.expr(), va.key());
                    }
                    yield new SqlExpr.VariantScalarCast(source, sqlType);
                }
                yield source;
            }

            // --- Pass-through for non-SQL functions ---
            case "toOne", "eval",
                    "list", "pair", "match",
                    "letWithParam",
                    "groupBy", "select",
                    "compare", "comparator" -> {
                // These are Pure-level functions that should pass through the first arg
                if (!params.isEmpty()) {
                    yield c.apply(params.get(0));
                }
                throw new PureCompileException(
                        "PlanGenerator: pass-through function '" + funcName + "' has no parameters");
            }
            case "cast" -> {
                if (!params.isEmpty()) {
                    SqlExpr source = c.apply(params.get(0));
                    // Check compiler type info for typed list cast: cast(@Integer) on a list
                    TypeInfo castInfo = unit.typeInfoFor(af);
                    if (castInfo != null && castInfo.isMany()) {
                        GenericType elemType = castInfo.type();
                        if (elemType != null && elemType != GenericType.Primitive.ANY) {
                            String sqlType = dialect.sqlTypeName(elemType.typeName());
                            yield new SqlExpr.VariantArrayCast(source, sqlType);
                        }
                    }
                    yield source;
                }
                throw new PureCompileException(
                        "PlanGenerator: pass-through function 'cast' has no parameters");
            }
            case "range" -> {
                // range(n) → RANGE(n) — produces a list of integers [0..n-1]
                yield new SqlExpr.FunctionCall("RANGE",
                        List.of(c.apply(params.get(0))));
            }
            case "map" -> {
                // map(list, {x|body}) → list_transform(list, x -> body)
                if (params.size() >= 2 && params.get(1) instanceof LambdaFunction(
                        List<Variable> parameters, List<ValueSpecification> body
                )) {
                    SqlExpr list = c.apply(params.get(0));
                    // If source is a variant (JSON), wrap in CAST(AS JSON[]) for array iteration
                    TypeInfo sourceInfo = unit.types().get(params.get(0));
                    if (sourceInfo != null && sourceInfo.type() == GenericType.Primitive.JSON) {
                        list = new SqlExpr.VariantArrayCast(list, dialect.sqlTypeName("JSON"));
                    }
                    String elemParam = parameters.isEmpty() ? "x"
                            : parameters.get(0).name();
                    SqlExpr lambdaBody = !body.isEmpty()
                            ? c.apply(body.getLast()) : new SqlExpr.NullLiteral();
                    SqlExpr lambda = new SqlExpr.LambdaExpr(List.of(elemParam), lambdaBody);
                    yield new SqlExpr.FunctionCall("listTransform", List.of(list, lambda));
                }
                // Non-lambda map: pass through source
                if (!params.isEmpty()) {
                    yield c.apply(params.get(0));
                }
                throw new PureCompileException("map: no parameters");
            }
            case "fold" -> {
                // fold(source, {elem,acc|body}, init)
                // Exhaustive switch on FoldSpec — stamped by FoldChecker.
                if (params.size() >= 3
                        && params.get(1) instanceof LambdaFunction(
                        List<Variable> parameters, List<ValueSpecification> body
                )) {
                    String elemParam = parameters.get(0).name();
                    String accParam = parameters.get(1).name();

                    SqlExpr source = c.apply(params.get(0));
                    if (!firstArgIsList)
                        source = new SqlExpr.FunctionCall("wrapList", List.of(source));
                    SqlExpr init = c.apply(params.get(2));

                    TypeInfo foldInfo = unit.types().get(af);
                    TypeInfo.FoldSpec spec = foldInfo != null ? foldInfo.foldSpec() : null;
                    System.out.println("[FOLD DEBUG] foldInfo=" + foldInfo);
                    System.out.println("[FOLD DEBUG] spec=" + spec);

                    if (spec == null)
                        throw new PureCompileException("fold: no FoldSpec stamped by FoldChecker");

                    yield switch (spec) {
                        // Path 1: fold+add → listConcat(init, source)
                        case TypeInfo.FoldSpec.Concatenation() ->
                            new SqlExpr.FunctionCall("listConcat", List.of(init, source));

                        // Path 2: T == V → listReduce(source, lambda, init)
                        case TypeInfo.FoldSpec.SameType() -> {
                            SqlExpr lambdaBody = c.apply(body.getLast());
                            SqlExpr lambda = new SqlExpr.LambdaExpr(
                                    List.of(accParam, elemParam), lambdaBody);
                            // Cast init to match list element type — DuckDB list_reduce
                            // requires init type == list child type (e.g., INTEGER 0 vs BIGINT[])
                            SqlExpr castInit = init;
                            if (foldInfo != null && foldInfo.type() != null) {
                                String sqlType = dialect.sqlTypeName(foldInfo.type().typeName());
                                if (sqlType != null) {
                                    castInit = new SqlExpr.Cast(init, foldInfo.type().typeName());
                                }
                            }
                            yield new SqlExpr.FunctionCall("listReduce",
                                    List.of(source, lambda, castInit));
                        }

                        // Path 3: T ≠ V, decomposable → listTransform + listReduce
                        case TypeInfo.FoldSpec.MapReduce(var transform, var reducerBody,
                                var mrAccParam, var mrFreshParam) -> {
                            // listTransform(source, elem -> transform)
                            SqlExpr transformBody = c.apply(transform);
                            SqlExpr transformLambda = new SqlExpr.LambdaExpr(
                                    List.of(elemParam), transformBody);
                            SqlExpr mapped = new SqlExpr.FunctionCall("listTransform",
                                    List.of(source, transformLambda));
                            // listReduce(mapped, (acc, __mr_x) -> reducerBody, init)
                            SqlExpr compiledReducer = c.apply(reducerBody);
                            SqlExpr reducerLambda = new SqlExpr.LambdaExpr(
                                    List.of(mrAccParam, mrFreshParam), compiledReducer);
                            yield new SqlExpr.FunctionCall("listReduce",
                                    List.of(mapped, reducerLambda, init));
                        }

                        // Path 4: V = List<T>, non-decomposable → wrap + unwrap + listReduce
                        case TypeInfo.FoldSpec.CollectionBuild() -> {
                            // Wrap each source element: listTransform(source, elem -> [elem])
                            SqlExpr wrapBody = new SqlExpr.FunctionCall("wrapList",
                                    List.of(new SqlExpr.ColumnRef(elemParam)));
                            SqlExpr wrapLambda = new SqlExpr.LambdaExpr(
                                    List.of(elemParam), wrapBody);
                            SqlExpr wrappedSource = new SqlExpr.FunctionCall("listTransform",
                                    List.of(source, wrapLambda));
                            // Compile body as-is, then SQL-level unwrap:
                            // replace ColumnRef(elemParam) → listExtract(ColumnRef(elemParam), 1)
                            SqlExpr lambdaBody = c.apply(body.getLast());
                            SqlExpr unwrapped = unwrapElemRefs(lambdaBody, elemParam);
                            SqlExpr lambda = new SqlExpr.LambdaExpr(
                                    List.of(accParam, elemParam), unwrapped);
                            yield new SqlExpr.FunctionCall("listReduce",
                                    List.of(wrappedSource, lambda, init));
                        }
                    };
                }
                throw new PureCompileException("fold: requires 3 parameters with lambda");
            }
            case "zip" -> {
                // zip(list1, list2) → list of pairs
                if (params.size() >= 2) {
                    yield new SqlExpr.FunctionCall("listZip",
                            List.of(c.apply(params.get(0)), c.apply(params.get(1))));
                }
                if (!params.isEmpty()) {
                    yield c.apply(params.get(0));
                }
                throw new PureCompileException("zip: no parameters");
            }
            case "sort", "sortBy", "sortByReversed" -> {
                if (firstArgIsList) {
                    // Derive direction from function name or sidecar
                    String direction;
                    if ("sortByReversed".equals(funcName)) {
                        direction = "DESC";
                    } else if ("sortBy".equals(funcName)) {
                        direction = "ASC";
                    } else {
                        // sort: read from sidecar (TypeChecker detected compare lambda)
                        TypeInfo sortInfo = unit.types().get(af);
                        direction = (sortInfo != null && sortInfo.hasSortSpecs()
                                && sortInfo.sortSpecs().get(0).direction() == TypeInfo.SortDirection.DESC)
                                ? "DESC" : "ASC";
                    }
                    // Check for key function: sort(keyFn, compareFn) or sortBy/sortByReversed(keyFn)
                    LambdaFunction keyLf = null;
                    if ("sortBy".equals(funcName) || "sortByReversed".equals(funcName)) {
                        // sortBy/sortByReversed: key lambda is always params[1]
                        if (params.size() > 1 && params.get(1) instanceof LambdaFunction lf) {
                            keyLf = lf;
                        }
                    } else if (params.size() > 2
                            || (params.size() == 2 && params.get(1) instanceof LambdaFunction kf
                                    && kf.parameters().size() == 1)) {
                        keyLf = (LambdaFunction) params.get(1);
                    }

                    if (keyLf != null) {
                        String keyParam = keyLf.parameters().get(0).name();
                        SqlExpr keyBody = !keyLf.body().isEmpty()
                                ? c.apply(keyLf.body().get(0)) : new SqlExpr.NullLiteral();
                        yield new SqlExpr.FunctionCall("listSortWithKey",
                                List.of(c.apply(params.get(0)),
                                        new SqlExpr.StringLiteral(direction),
                                        new SqlExpr.Identifier(keyParam),
                                        keyBody));
                    }
                    if ("DESC".equals(direction)) {
                        yield new SqlExpr.FunctionCall("listSort",
                                List.of(c.apply(params.get(0)),
                                        new SqlExpr.StringLiteral(direction)));
                    }
                    yield new SqlExpr.FunctionCall("listSort",
                            List.of(c.apply(params.get(0))));
                }
                // Relation sort — pass through (handled by generateSort)
                if (!params.isEmpty()) {
                    yield c.apply(params.get(0));
                }
                throw new PureCompileException("sort: no parameters");
            }

            // --- Let binding ---
            case "letFunction" -> {
                // Let bindings: compile the value expression
                if (params.size() >= 2) {
                    yield c.apply(params.get(1));
                }
                if (!params.isEmpty()) {
                    yield c.apply(params.get(0));
                }
                throw new PureCompileException(
                        "PlanGenerator: unsupported scalar function '" + funcName + "'");
            }



            // --- String format ---
            case "format" -> {
                // Flatten Collection args (Pure passes format args as a list)
                // Emit semantic FunctionCall — dialect handles %t/%f/%r conversion
                SqlExpr fmtString = c.apply(params.get(0));
                java.util.List<SqlExpr> fmtArgs = new java.util.ArrayList<>();
                fmtArgs.add(fmtString);
                for (int fi = 1; fi < params.size(); fi++) {
                    var fp = params.get(fi);
                    if (fp instanceof PureCollection(List<ValueSpecification> values)) {
                        for (var elem : values) {
                            fmtArgs.add(c.apply(elem));
                        }
                    } else {
                        fmtArgs.add(c.apply(fp));
                    }
                }
                yield new SqlExpr.FunctionCall("format", fmtArgs);
            }

            // --- Struct literal: new(PE(class), ClassInstance("instance", data)) → StructLiteral ---
            case "new" -> {
                yield renderStructValue(af);
            }

            default -> throw new PureCompileException(
                    "PlanGenerator: unsupported scalar function '" + funcName + "'");
        };
    }

    /** Checks if any param is an actual partial date (year-only or year-month, not full YYYY-MM-DD). */
    private boolean hasPartialDate(List<ValueSpecification> params) {
        for (var p : params) {
            if (p instanceof CStrictDate(String value) && isPartialDate(value)) return true;
        }
        return false;
    }

    /** True if the date string is partial (year-only or year-month, not full YYYY-MM-DD). */
    private static boolean isPartialDate(String dateValue) {
        // Full date: YYYY-MM-DD (exactly 2 dashes)
        // Partial: YYYY (0 dashes) or YYYY-MM (1 dash)
        long dashes = dateValue.chars().filter(ch -> ch == '-').count();
        return dashes < 2;
    }

    /**
     * Resolves the SQL type name for a variant-typed expression.
     * Returns the dialect-specific SQL type (e.g., "BIGINT" for Integer) if the
     * compiler resolved a concrete target type, or null if untyped (Any/JSON).
     */
    private String resolveVariantSqlType(TypeInfo info) {
        if (info == null) return null;
        GenericType t = info.type();
        if (t == GenericType.Primitive.ANY || t == GenericType.Primitive.JSON) return null;
        return dialect.sqlTypeName(t.typeName());
    }

    /**
     * Renders a param for precision-aware date comparison.
     * Called ONLY when hasPartialDate() is true (some side is partial).
     * In that context, ALL CStrictDate values must become string literals
     * so the comparison is string-vs-string (preserving Pure date precision).
     * Full-date-vs-full-date comparisons skip this path entirely.
     */
    private SqlExpr renderForDateComparison(ValueSpecification vs,
            java.util.function.Function<ValueSpecification, SqlExpr> c) {
        if (vs instanceof CStrictDate(String value)) {
            return new SqlExpr.StringLiteral(value);
        }
        return c.apply(vs);
    }

    /**
     * Builds a comparison expression.
     */
    private SqlExpr buildComparison(List<ValueSpecification> params, String op,
            java.util.function.Function<ValueSpecification, SqlExpr> c,
            StoreResolution store, String tableAlias) {
        SqlExpr left = c.apply(params.get(0));
        SqlExpr right = c.apply(params.get(1));
        return new SqlExpr.Binary(left, op, right);
    }

    /** Checks if an AST node represents a many-valued expression via TypeInfo side table. */
    private boolean isListArg(ValueSpecification vs) {
        TypeInfo info = unit.typeInfoFor(vs);
        return info.expressionType().isMany();
    }

    /**
     * SQL-level post-processor for CollectionBuild fold.
     * Replaces ColumnRef(elemParam) → listExtract(ColumnRef(elemParam), 1)
     * in the compiled SQL tree so wrapped elements get unwrapped in the body.
     */
    private static SqlExpr unwrapElemRefs(SqlExpr expr, String elemParam) {
        if (expr instanceof SqlExpr.ColumnRef cr && cr.name().equals(elemParam)) {
            return new SqlExpr.FunctionCall("listExtract",
                    List.of(expr, new SqlExpr.NumericLiteral(1)));
        }
        // Lambda variables compile to Identifier, not ColumnRef
        if (expr instanceof SqlExpr.Identifier id && id.name().equals(elemParam)) {
            return new SqlExpr.FunctionCall("listExtract",
                    List.of(expr, new SqlExpr.NumericLiteral(1)));
        }
        if (expr instanceof SqlExpr.FunctionCall fc) {
            var newArgs = fc.args().stream()
                    .map(a -> unwrapElemRefs(a, elemParam))
                    .toList();
            return new SqlExpr.FunctionCall(fc.name(), newArgs);
        }
        if (expr instanceof SqlExpr.Binary b) {
            return new SqlExpr.Binary(
                    unwrapElemRefs(b.left(), elemParam), b.op(),
                    unwrapElemRefs(b.right(), elemParam));
        }
        if (expr instanceof SqlExpr.LambdaExpr le) {
            // Don't unwrap inside nested lambdas that shadow the variable
            if (le.params().contains(elemParam)) return expr;
            return new SqlExpr.LambdaExpr(le.params(),
                    unwrapElemRefs(le.body(), elemParam));
        }
        if (expr instanceof SqlExpr.CaseWhen cw) {
            return new SqlExpr.CaseWhen(
                    unwrapElemRefs(cw.condition(), elemParam),
                    unwrapElemRefs(cw.thenExpr(), elemParam),
                    unwrapElemRefs(cw.elseExpr(), elemParam));
        }
        if (expr instanceof SqlExpr.SearchedCase sc) {
            var newBranches = sc.branches().stream()
                    .map(b -> new SqlExpr.SearchedCase.WhenBranch(
                            unwrapElemRefs(b.condition(), elemParam),
                            unwrapElemRefs(b.result(), elemParam)))
                    .toList();
            SqlExpr newElse = sc.elseExpr() != null
                    ? unwrapElemRefs(sc.elseExpr(), elemParam) : null;
            return new SqlExpr.SearchedCase(newBranches, newElse);
        }
        // Cast, Subquery, literals, etc. — no ColumnRef to unwrap
        return expr;
    }

    // ========== Utility Methods ==========

    private String nextTableAlias() {
        return "t" + tableAliasCounter++;
    }

    /** Looks up the StoreResolution for an AST node (null if not resolved by MappingResolver). */
    private StoreResolution storeFor(ValueSpecification vs) {
        return storeResolutions.get(vs);
    }

    /** Searches all StoreResolutions for a JoinResolution matching the association property. */
    private StoreResolution.JoinResolution findJoinResolution(String assocProp) {
        for (var store : storeResolutions.values()) {
            if (store != null && store.hasJoins()) {
                var join = store.joins().get(assocProp);
                if (join != null) return join;
            }
        }
        return null;
    }

    // renderJoinCondition / renderJoinOn DELETED — replaced by generateScalar + varAliases

    // binaryOp DELETED — was thin wrapper, inlined into generateScalarFunction

    private long extractIntValue(ValueSpecification vs) {
        if (vs instanceof CInteger(Number value))
            return value.longValue();
        if (vs instanceof CFloat(double value))
            return (long) value;
        throw new PureCompileException("Expected integer, got: " + vs.getClass().getSimpleName());
    }


    private static String simpleName(String qualifiedName) {
        return com.gs.legend.model.SymbolTable.extractSimpleName(qualifiedName);
    }


    /**
     * Wraps individual elements of a Collection in ::VARIANT for cross-type LIST_CONCAT.
     * VARIANT preserves original types through UNNEST — getObject() returns native Java types.
     * Integer literals are upcast to Integer (→ BIGINT) before VARIANT so that
     * DuckDB preserves 64-bit semantics (Pure Integer = Java Long).
     * Only handles literal Collections — non-Collection args should be left as-is
     * (wrapping entire arrays would break LIST_CONCAT which needs array args).
     */
    private SqlExpr wrapCollectionElementsInVariant(PureCollection coll,
                                                    java.util.function.Function<ValueSpecification, SqlExpr> c) {
        var wrapped = new java.util.ArrayList<SqlExpr>();
        for (var v : coll.values()) {
            SqlExpr e = c.apply(v);
            if (v instanceof CInteger) {
                e = new SqlExpr.Cast(e, "Integer");
            }
            wrapped.add(new SqlExpr.VariantCast(e));
        }
        return new SqlExpr.ArrayLiteral(wrapped);
    }

    /**
     * Builds a structural subquery for type-preserving min/max on VARIANT lists.
     * The generated SQL pattern:
     *   (SELECT "_v" FROM (SELECT UNNEST(array) AS "_v") AS "_vt"
     *    ORDER BY CAST("_v" AS DOUBLE|TIMESTAMP) DESC|ASC LIMIT 1)
     *
     * Each element in the array should already be ::VARIANT cast (done by Collection rendering).
     * This preserves the winning element's original type through JDBC.
     *
     * @param elements  List of SqlExpr — either a single array expr, or multiple scalar values
     * @param isMax     true for greatest/max (DESC), false for least/min (ASC)
     * @param isDate    true if comparison should use TIMESTAMP cast, false for DOUBLE
     */
    private SqlExpr buildVariantMinMaxSubquery(List<SqlExpr> elements, boolean isMax, boolean isDate) {
        // If multiple scalar elements (two-arg max/min), wrap each in VARIANT and build array
        SqlExpr arrayExpr;
        if (elements.size() > 1) {
            var variantElements = elements.stream()
                    .map(e -> (SqlExpr) new SqlExpr.VariantCast(e))
                    .collect(Collectors.toList());
            arrayExpr = new SqlExpr.ArrayLiteral(variantElements);
        } else {
            arrayExpr = elements.get(0);
        }

        // Inner: SELECT UNNEST(array) AS "_v"
        SqlBuilder inner = new SqlBuilder();
        inner.addSelect(new SqlExpr.Unnest(arrayExpr), "\"_v\"");

        // Outer: SELECT "_v" FROM (inner) ORDER BY CAST("_v" AS DOUBLE|TIMESTAMP) LIMIT 1
        String castType = isDate ? "DateTime" : "Double";
        SqlBuilder outer = new SqlBuilder();
        outer.addSelect(new SqlExpr.ColumnRef("_v"), null);
        outer.fromSubquery(inner, "\"_vt\"");
        outer.addOrderBy(
                new SqlExpr.Cast(new SqlExpr.ColumnRef("_v"), castType),
                isMax ? SqlBuilder.SortDirection.DESC : SqlBuilder.SortDirection.ASC);
        outer.limit(1);

        return new SqlExpr.Subquery(outer);
    }
}
