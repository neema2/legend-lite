package org.finos.legend.pure.dsl.ast;

import org.finos.legend.engine.plan.RelationType;
import org.finos.legend.engine.store.RelationalMapping;
import org.finos.legend.engine.transpiler.SQLDialect;
import org.finos.legend.pure.dsl.PureCompileException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Generates SQL directly from CleanCompiler's typed output.
 *
 * <p>
 * Replaces both {@code PureCompiler.compileToSqlExpression} and
 * {@code SQLGenerator} with a single clean pass: TypedValueSpec → SQL.
 *
 * <p>
 * Uses {@link SQLDialect} for multi-backend support (DuckDB, SQLite, etc.).
 */
public class PlanGenerator {

    private final SQLDialect dialect;

    public PlanGenerator(SQLDialect dialect) {
        this.dialect = dialect;
    }

    /**
     * Generates SQL from a TypedValueSpec.
     */
    public String generateSql(TypedValueSpec tvs) {
        return generateRelation(tvs.ast(), tvs.resultType(), tvs.mapping());
    }

    // ========== Relation Operations → SQL ==========

    private String generateRelation(ValueSpecification vs, RelationType resultType, RelationalMapping mapping) {
        return switch (vs) {
            case AppliedFunction af -> generateFunction(af, resultType, mapping);
            case ClassInstance ci -> generateClassInstance(ci, resultType);
            case LambdaFunction lf -> {
                if (!lf.body().isEmpty()) {
                    yield generateRelation(lf.body().get(0), resultType, mapping);
                }
                throw new PureCompileException("PlanGenerator: empty lambda body");
            }
            default -> throw new PureCompileException(
                    "PlanGenerator: cannot generate SQL for: " + vs.getClass().getSimpleName());
        };
    }

    private String generateFunction(AppliedFunction af, RelationType resultType, RelationalMapping mapping) {
        String funcName = CleanCompiler.simpleName(af.function());
        return switch (funcName) {
            // --- Relation Sources ---
            case "table", "class" -> generateTableAccess(af, mapping);
            case "getAll" -> generateGetAll(af, mapping);
            // --- Shape-preserving ---
            case "filter" -> generateFilter(af, mapping);
            case "sort", "sortBy" -> generateSort(af, mapping);
            case "limit", "take" -> generateLimit(af, mapping);
            case "drop" -> generateDrop(af, mapping);
            case "slice" -> generateSlice(af, mapping);
            case "first" -> generateFirst(af, mapping);
            case "distinct" -> generateDistinct(af, mapping);
            // --- Shape-changing ---
            case "rename" -> generateRename(af, mapping);
            case "concatenate" -> generateConcatenate(af, mapping);
            case "project" -> generateProject(af, mapping);
            case "select" -> generateSelect(af, mapping);
            case "groupBy" -> generateGroupBy(af, mapping);
            case "aggregate" -> generateAggregate(af, mapping);
            case "extend" -> generateExtend(af, mapping);
            case "join" -> generateJoin(af, mapping);
            case "from" -> generateFrom(af, mapping);
            default -> throw new PureCompileException(
                    "PlanGenerator: unsupported function '" + funcName + "'");
        };
    }

    // ========== Relation Sources ==========

    private String generateTableAccess(AppliedFunction af, RelationalMapping mapping) {
        String content = extractString(af.parameters().get(0));
        String tableName = extractTableName(content);

        // If mapping is available, use mapping's table name
        if (mapping != null) {
            tableName = mapping.table().name();
        }

        return "SELECT * FROM " + dialect.quoteIdentifier(tableName)
                + " AS " + dialect.quoteIdentifier("t0");
    }

    /**
     * Generates SQL for Person.all() → getAll(PackageableElementPtr("Person")).
     * Uses mapping to resolve class → table.
     */
    private String generateGetAll(AppliedFunction af, RelationalMapping mapping) {
        if (mapping != null) {
            return "SELECT * FROM " + dialect.quoteIdentifier(mapping.table().name())
                    + " AS " + dialect.quoteIdentifier("t0");
        }

        // Fallback: try to use class name as table name
        String className = "";
        if (af.parameters().get(0) instanceof PackageableElementPtr pe) {
            className = pe.fullPath().contains("::")
                    ? pe.fullPath().substring(pe.fullPath().lastIndexOf("::") + 2)
                    : pe.fullPath();
        }
        return "SELECT * FROM " + dialect.quoteIdentifier(className)
                + " AS " + dialect.quoteIdentifier("t0");
    }

    // ========== Filter ==========

    private String generateFilter(AppliedFunction af, RelationalMapping mapping) {
        List<ValueSpecification> params = af.parameters();
        String sourceSql = generateRelation(params.get(0), RelationType.empty(), mapping);
        LambdaFunction lambda = (LambdaFunction) params.get(1);
        String paramName = lambda.parameters().isEmpty() ? "x" : lambda.parameters().get(0).name();
        String whereClause = generateScalar(lambda.body().get(0), paramName, mapping);
        return "SELECT * FROM (" + sourceSql + ") AS filter_src WHERE " + whereClause;
    }

    // ========== Sort ==========

    private String generateSort(AppliedFunction af, RelationalMapping mapping) {
        List<ValueSpecification> params = af.parameters();
        String sourceSql = generateRelation(params.get(0), RelationType.empty(), mapping);

        String orderBy;
        // Pattern 1: sort('colName', 'asc'/'desc') — string params
        if (params.size() >= 2 && params.get(1) instanceof CString colStr) {
            String col = dialect.quoteIdentifier(colStr.value());
            String dir = "ASC";
            if (params.size() >= 3 && params.get(2) instanceof CString dirStr) {
                dir = dirStr.value().toUpperCase();
            }
            orderBy = col + " " + dir;
        } else {
            // Pattern 2/3: sort(~col->asc()) or sort([~c1->asc(), ~c2->desc()])
            orderBy = generateSortSpecs(params.get(1), mapping);
        }
        return "SELECT * FROM (" + sourceSql + ") AS sort_src ORDER BY " + orderBy;
    }

    private String generateSortSpecs(ValueSpecification vs, RelationalMapping mapping) {
        if (vs instanceof Collection coll) {
            return coll.values().stream()
                    .map(v -> generateSingleSortSpec(v, mapping))
                    .collect(Collectors.joining(", "));
        }
        return generateSingleSortSpec(vs, mapping);
    }

    private String generateSingleSortSpec(ValueSpecification vs, RelationalMapping mapping) {
        if (vs instanceof ClassInstance ci && "sortInfo".equals(ci.type())) {
            if (ci.value() instanceof ColSpec cs) {
                // Sort operates on projected relation — use alias name directly
                return dialect.quoteIdentifier(cs.name()) + " ASC";
            }
        }
        if (vs instanceof AppliedFunction af) {
            String funcName = CleanCompiler.simpleName(af.function());
            if ("asc".equals(funcName) || "ascending".equals(funcName)
                    || "desc".equals(funcName) || "descending".equals(funcName)) {
                // Sort operates on projected relation — use alias name directly, not DB column
                String col = extractColumnNameFromParam(af.parameters().get(0));
                String dir = ("asc".equals(funcName) || "ascending".equals(funcName)) ? "ASC" : "DESC";
                return dialect.quoteIdentifier(col) + " " + dir;
            }
        }
        if (vs instanceof CString s) {
            return dialect.quoteIdentifier(s.value());
        }
        throw new PureCompileException("PlanGenerator: unsupported sort spec: " + vs);
    }

    // ========== Limit/Take/Drop/Slice/First ==========

    private String generateLimit(AppliedFunction af, RelationalMapping mapping) {
        String sourceSql = generateRelation(af.parameters().get(0), RelationType.empty(), mapping);
        long count = extractIntValue(af.parameters().get(1));
        return "SELECT * FROM (" + sourceSql + ") AS limit_src LIMIT " + count;
    }

    private String generateDrop(AppliedFunction af, RelationalMapping mapping) {
        String sourceSql = generateRelation(af.parameters().get(0), RelationType.empty(), mapping);
        long count = extractIntValue(af.parameters().get(1));
        return "SELECT * FROM (" + sourceSql + ") AS limit_src OFFSET " + count;
    }

    private String generateSlice(AppliedFunction af, RelationalMapping mapping) {
        String sourceSql = generateRelation(af.parameters().get(0), RelationType.empty(), mapping);
        long start = extractIntValue(af.parameters().get(1));
        long end = extractIntValue(af.parameters().get(2));
        String sql = "SELECT * FROM (" + sourceSql + ") AS limit_src LIMIT " + (end - start);
        if (start > 0) {
            sql += " OFFSET " + start;
        }
        return sql;
    }

    private String generateFirst(AppliedFunction af, RelationalMapping mapping) {
        String sourceSql = generateRelation(af.parameters().get(0), RelationType.empty(), mapping);
        return "SELECT * FROM (" + sourceSql + ") AS limit_src LIMIT 1";
    }

    // ========== Distinct ==========

    private String generateDistinct(AppliedFunction af, RelationalMapping mapping) {
        String sourceSql = generateRelation(af.parameters().get(0), RelationType.empty(), mapping);
        return "SELECT DISTINCT * FROM (" + sourceSql + ") AS distinct_src";
    }

    // ========== Rename ==========

    private String generateRename(AppliedFunction af, RelationalMapping mapping) {
        String sourceSql = generateRelation(af.parameters().get(0), RelationType.empty(), mapping);
        String oldCol = extractColumnNameFromParam(af.parameters().get(1));
        String newCol = extractColumnNameFromParam(af.parameters().get(2));
        return "SELECT * EXCLUDE(" + dialect.quoteIdentifier(oldCol) + "), "
                + dialect.quoteIdentifier(oldCol) + " AS " + dialect.quoteIdentifier(newCol)
                + " FROM (" + sourceSql + ") AS rename_src";
    }

    // ========== Concatenate ==========

    private String generateConcatenate(AppliedFunction af, RelationalMapping mapping) {
        String leftSql = generateRelation(af.parameters().get(0), RelationType.empty(), mapping);
        String rightSql = generateRelation(af.parameters().get(1), RelationType.empty(), mapping);
        return "(" + leftSql + ") UNION ALL (" + rightSql + ")";
    }

    /**
     * Generates SQL for project(source, projectionSpecs...).
     *
     * For class-based queries (getAll()->project()), generates flat SQL:
     * SELECT "t0"."NAME" AS "name", "t0"."SALARY" AS "salary" FROM "T_EMPLOYEE" AS
     * "t0"
     *
     * The table alias "t0" is consistent with the old pipeline.
     */
    private String generateProject(AppliedFunction af, RelationalMapping mapping) {
        List<ValueSpecification> params = af.parameters();

        // Extract projection lambdas and aliases (handle both AST patterns)
        List<ValueSpecification> lambdaSpecs;
        List<String> aliases;

        if (params.get(1) instanceof Collection coll) {
            lambdaSpecs = coll.values();
            aliases = params.size() > 2 ? extractStringList(params.get(2)) : List.of();
        } else {
            lambdaSpecs = params.subList(1, params.size());
            aliases = List.of();
        }

        // Determine table alias and table name from the source (getAll)
        String tableAlias = "t0";
        String tableName = resolveTableNameFromSource(params.get(0), mapping);

        // Build column projections
        List<String> projections = new ArrayList<>();
        for (int i = 0; i < lambdaSpecs.size(); i++) {
            String propertyName = extractPropertyNameFromLambda(lambdaSpecs.get(i));
            String alias = i < aliases.size() ? aliases.get(i) : propertyName;

            // Resolve property → column name via mapping
            String columnName = propertyName;
            if (mapping != null && propertyName != null) {
                var columnOpt = mapping.getColumnForProperty(propertyName);
                if (columnOpt.isPresent()) {
                    columnName = columnOpt.get();
                }
            }

            // Generate: "t0"."COLUMN" AS "alias"
            String colExpr = dialect.quoteIdentifier(tableAlias) + "."
                    + dialect.quoteIdentifier(columnName);
            projections.add(colExpr + " AS " + dialect.quoteIdentifier(alias));
        }

        // Generate flat FROM clause: FROM "TABLE" AS "t0"
        String fromClause = dialect.quoteIdentifier(tableName) + " AS " + dialect.quoteIdentifier(tableAlias);

        // Check if there's a filter between getAll and project
        String whereClause = extractFilterClause(params.get(0), tableAlias, mapping);

        String sql = "SELECT " + String.join(", ", projections) + " FROM " + fromClause;
        if (whereClause != null) {
            sql += " WHERE " + whereClause;
        }
        return sql;
    }

    /**
     * Resolves the table name from the source expression (typically getAll).
     * Walks through filters etc. to find the underlying getAll.
     */
    private String resolveTableNameFromSource(ValueSpecification source, RelationalMapping mapping) {
        if (mapping != null) {
            return mapping.table().name();
        }
        // Fallback: walk the AST to find table/getAll
        if (source instanceof AppliedFunction af) {
            String funcName = simpleName(af.function());
            if ("getAll".equals(funcName) && !af.parameters().isEmpty()) {
                // getAll(className) — use mapping table name, or className as fallback
                return extractClassName(af.parameters().get(0));
            } else if (!af.parameters().isEmpty()) {
                // filter/sort/etc — recurse into source
                return resolveTableNameFromSource(af.parameters().get(0), mapping);
            }
        }
        return "UNKNOWN";
    }

    /**
     * Extracts a WHERE clause if the source of project is a filter.
     */
    private String extractFilterClause(ValueSpecification source, String tableAlias, RelationalMapping mapping) {
        if (source instanceof AppliedFunction af) {
            String funcName = simpleName(af.function());
            if ("filter".equals(funcName) && af.parameters().size() >= 2) {
                LambdaFunction lambda = (LambdaFunction) af.parameters().get(1);
                String paramName = lambda.parameters().isEmpty() ? "x" : lambda.parameters().get(0).name();
                return generateScalar(lambda.body().get(0), paramName, mapping, tableAlias);
            }
        }
        return null;
    }

    /**
     * Generates a projection column expression from a lambda.
     * {p|$p.firstName} → "FIRST_NAME"
     * {p|$p.age + 1} → "AGE_VAL" + 1
     */
    private String generateProjectionColumn(ValueSpecification vs, RelationalMapping mapping) {
        if (vs instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            String paramName = lf.parameters().isEmpty() ? "x" : lf.parameters().get(0).name();
            return generateScalar(lf.body().get(0), paramName, mapping);
        }
        // ColSpec: ~name syntax
        if (vs instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
            String col = resolveColumnName(cs.name(), mapping);
            return dialect.quoteIdentifier(col);
        }
        throw new PureCompileException("PlanGenerator: unsupported projection: " + vs.getClass().getSimpleName());
    }

    // ========== Select ==========

    private String generateSelect(AppliedFunction af, RelationalMapping mapping) {
        List<ValueSpecification> params = af.parameters();
        String sourceSql = generateRelation(params.get(0), RelationType.empty(), mapping);

        List<String> columns = new ArrayList<>();
        for (int i = 1; i < params.size(); i++) {
            for (String col : extractColumnNames(params.get(i))) {
                columns.add(dialect.quoteIdentifier(col));
            }
        }

        return "SELECT " + String.join(", ", columns) + " FROM (" + sourceSql + ") AS select_src";
    }

    // ========== GroupBy ==========

    private String generateGroupBy(AppliedFunction af, RelationalMapping mapping) {
        List<ValueSpecification> params = af.parameters();
        String sourceSql = generateRelation(params.get(0), RelationType.empty(), mapping);

        List<String> groupCols = new ArrayList<>();
        List<String> aggExprs = new ArrayList<>();

        if (params.size() >= 3) {
            // Extract group columns
            List<ValueSpecification> groupSpecs = extractList(params.get(1));
            for (var spec : groupSpecs) {
                String colName = extractPropertyOrColumnName(spec);
                if (colName != null) {
                    groupCols.add(dialect.quoteIdentifier(colName));
                }
            }

            // Extract aggregate specs
            List<ValueSpecification> aggSpecs = extractList(params.get(2));
            List<String> aliases = params.size() > 3 ? extractStringList(params.get(3)) : List.of();
            for (int i = 0; i < aggSpecs.size(); i++) {
                var aggSpec = aggSpecs.get(i);

                String sourceCol;
                String aggFunc = "SUM";
                String alias;

                if (aggSpec instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
                    // Relation syntax: ~totalSal:x|$x.salary:y|$y->sum()
                    alias = cs.name(); // output alias
                    // Extract source column from map lambda (function1)
                    sourceCol = cs.function1() != null
                            ? extractPropertyNameFromLambda(cs.function1())
                            : cs.name();
                    // Extract agg function from reduce lambda (function2)
                    if (cs.function2() != null) {
                        aggFunc = extractAggFunction(cs.function2());
                    }
                } else {
                    // Legacy lambda syntax: {r|$r.sal} with separate aliases
                    sourceCol = extractPropertyOrColumnName(aggSpec);
                    int aliasIndex = groupCols.size() + i;
                    alias = aliasIndex < aliases.size() ? aliases.get(aliasIndex) : sourceCol + "_agg";
                }

                String expr = aggFunc + "(" + dialect.quoteIdentifier(sourceCol) + ")";
                aggExprs.add(expr + " AS " + dialect.quoteIdentifier(alias));
            }
        }

        String selectCols = String.join(", ", groupCols);
        if (!aggExprs.isEmpty()) {
            selectCols += ", " + String.join(", ", aggExprs);
        }
        String sql = "SELECT " + selectCols + " FROM (" + sourceSql + ") AS groupby_src";
        if (!groupCols.isEmpty()) {
            sql += " GROUP BY " + String.join(", ", groupCols);
        }
        return sql;
    }

    /**
     * Extracts aggregate function name from a reduce lambda body: $y->sum() →
     * "SUM".
     */
    private String extractAggFunction(LambdaFunction lambda) {
        if (lambda.body().isEmpty())
            return "SUM";
        var body = lambda.body().get(0);
        if (body instanceof AppliedFunction af) {
            String funcName = CleanCompiler.simpleName(af.function());
            return switch (funcName) {
                case "sum", "plus" -> "SUM";
                case "count" -> "COUNT";
                case "average", "mean" -> "AVG";
                case "min" -> "MIN";
                case "max" -> "MAX";
                case "stdDev", "stdDevSample" -> "STDDEV_SAMP";
                case "stdDevPopulation" -> "STDDEV_POP";
                case "variance", "varianceSample" -> "VAR_SAMP";
                case "variancePopulation" -> "VAR_POP";
                case "joinStrings" -> "STRING_AGG";
                default -> "SUM";
            };
        }
        return "SUM";
    }

    /** Extracts property name from lambda {r|$r.dept} or column spec ~dept. */
    private String extractPropertyOrColumnName(ValueSpecification vs) {
        // Lambda: {r|$r.dept}
        String propName = extractPropertyNameFromLambda(vs);
        if (propName != null)
            return propName;
        // ColSpec: ~dept
        if (vs instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
            return cs.name();
        }
        if (vs instanceof CString s)
            return s.value();
        return null;
    }

    // ========== Aggregate ==========

    private String generateAggregate(AppliedFunction af, RelationalMapping mapping) {
        String sourceSql = generateRelation(af.parameters().get(0), RelationType.empty(), mapping);
        return "SELECT * FROM (" + sourceSql + ") AS agg_src";
    }

    // ========== Extend ==========

    private String generateExtend(AppliedFunction af, RelationalMapping mapping) {
        List<ValueSpecification> params = af.parameters();
        String sourceSql = generateRelation(params.get(0), RelationType.empty(), mapping);

        // Look for window function pattern: extend(source, over(...), ~colSpec)
        AppliedFunction overSpec = null;
        ColSpec windowColSpec = null;

        for (int i = 1; i < params.size(); i++) {
            var p = params.get(i);
            if (p instanceof AppliedFunction paf && "over".equals(CleanCompiler.simpleName(paf.function()))) {
                overSpec = paf;
            } else if (p instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
                windowColSpec = cs;
            }
        }

        if (overSpec != null && windowColSpec != null) {
            // Window function: SELECT *, FUNC() OVER (...) AS "alias" FROM (...) AS
            // window_src
            String windowFunc = extractWindowFunction(windowColSpec);
            String overClause = generateOverClause(overSpec);
            String alias = windowColSpec.name();
            return "SELECT *, " + windowFunc + " OVER (" + overClause + ") AS "
                    + dialect.quoteIdentifier(alias) + " FROM (" + sourceSql + ") AS window_src";
        }

        // Simple extend (computed column, no window)
        if (windowColSpec != null) {
            // TODO: handle simple computed columns
            return "SELECT * FROM (" + sourceSql + ") AS extend_src";
        }

        return "SELECT * FROM (" + sourceSql + ") AS extend_src";
    }

    /**
     * Extracts window function name from ColSpec lambda: {p,w,r|$p->rowNumber($r)}
     * → "ROW_NUMBER()".
     */
    private String extractWindowFunction(ColSpec cs) {
        if (cs.function1() != null) {
            var body = cs.function1().body();
            if (!body.isEmpty() && body.get(0) instanceof AppliedFunction af) {
                String funcName = CleanCompiler.simpleName(af.function());
                return switch (funcName) {
                    case "rowNumber" -> "ROW_NUMBER()";
                    case "rank" -> "RANK()";
                    case "denseRank" -> "DENSE_RANK()";
                    case "ntile" -> {
                        // ntile has a parameter
                        if (af.parameters().size() > 1) {
                            yield "NTILE(" + extractIntValue(af.parameters().get(1)) + ")";
                        }
                        yield "NTILE(1)";
                    }
                    case "sum" -> "SUM(" + extractWindowFuncArg(af) + ")";
                    case "average", "avg" -> "AVG(" + extractWindowFuncArg(af) + ")";
                    case "count" -> "COUNT(" + extractWindowFuncArg(af) + ")";
                    case "min" -> "MIN(" + extractWindowFuncArg(af) + ")";
                    case "max" -> "MAX(" + extractWindowFuncArg(af) + ")";
                    default -> funcName.toUpperCase() + "()";
                };
            }
        }
        return "ROW_NUMBER()";
    }

    private String extractWindowFuncArg(AppliedFunction af) {
        // The first parameter is usually the row variable; look for column refs
        if (af.parameters().size() > 1) {
            return dialect.quoteIdentifier(extractColumnNameFromParam(af.parameters().get(1)));
        }
        return "*";
    }

    /**
     * Generates OVER clause: PARTITION BY "col" ORDER BY "col" DESC NULLS FIRST.
     */
    private String generateOverClause(AppliedFunction overSpec) {
        List<ValueSpecification> overParams = overSpec.parameters();
        StringBuilder sb = new StringBuilder();

        // First param(s) are partition-by columns
        // Last param may be a sort spec (ascending/descending)
        List<String> partitionCols = new ArrayList<>();
        List<String> orderParts = new ArrayList<>();

        for (var p : overParams) {
            if (p instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
                // Simple column → partition by
                partitionCols.add(dialect.quoteIdentifier(cs.name()));
            } else if (p instanceof AppliedFunction paf) {
                String funcName = CleanCompiler.simpleName(paf.function());
                if ("asc".equals(funcName) || "ascending".equals(funcName)
                        || "desc".equals(funcName) || "descending".equals(funcName)) {
                    String col = extractColumnNameFromParam(paf.parameters().get(0));
                    String dir = ("asc".equals(funcName) || "ascending".equals(funcName)) ? "ASC" : "DESC";
                    orderParts.add(dialect.quoteIdentifier(col) + " " + dir + " NULLS FIRST");
                }
            }
        }

        if (!partitionCols.isEmpty()) {
            sb.append("PARTITION BY ").append(String.join(", ", partitionCols));
        }
        if (!orderParts.isEmpty()) {
            if (!sb.isEmpty())
                sb.append(" ");
            sb.append("ORDER BY ").append(String.join(", ", orderParts));
        }
        return sb.toString();
    }

    // ========== Join ==========

    private String generateJoin(AppliedFunction af, RelationalMapping mapping) {
        List<ValueSpecification> params = af.parameters();
        String leftSql = generateRelation(params.get(0), RelationType.empty(), mapping);
        String rightSql = generateRelation(params.get(1), RelationType.empty(), null);
        // TODO: implement join SQL gen with condition
        return "SELECT * FROM (" + leftSql + ") AS left_src CROSS JOIN (" + rightSql + ") AS right_src";
    }

    // ========== From ==========

    private String generateFrom(AppliedFunction af, RelationalMapping mapping) {
        // from() just binds a runtime — pass through the source SQL
        return generateRelation(af.parameters().get(0), RelationType.empty(), mapping);
    }

    // ========== ClassInstance ==========

    private String generateClassInstance(ClassInstance ci, RelationType resultType) {
        return switch (ci.type()) {
            case "relation" -> {
                String content = (String) ci.value();
                String tableName = extractTableName(content);
                yield "SELECT * FROM " + dialect.quoteIdentifier(tableName)
                        + " AS " + dialect.quoteIdentifier("t0");
            }
            default -> throw new PureCompileException(
                    "PlanGenerator: unsupported ClassInstance type: " + ci.type());
        };
    }

    // ========== Scalar Expression → SQL ==========

    /**
     * Compiles a scalar ValueSpecification to SQL.
     * Uses mapping for property→column resolution when available.
     */
    String generateScalar(ValueSpecification vs, String rowParam, RelationalMapping mapping) {
        return switch (vs) {
            case AppliedProperty ap -> {
                // Resolve property to column name via mapping
                String colName = ap.property();
                if (mapping != null) {
                    var columnOpt = mapping.getColumnForProperty(colName);
                    if (columnOpt.isPresent()) {
                        colName = columnOpt.get();
                    }
                }
                yield dialect.quoteIdentifier(colName);
            }
            case AppliedFunction af -> generateScalarFunction(af, rowParam, mapping);
            case CInteger i -> String.valueOf(i.value());
            case CFloat f -> String.valueOf(f.value());
            case CDecimal d -> String.valueOf(d.value());
            case CString s -> dialect.quoteStringLiteral(s.value());
            case CBoolean b -> dialect.formatBoolean(b.value());
            case CDateTime dt -> dialect.formatTimestamp(dt.value());
            case CStrictDate sd -> dialect.formatDate(sd.value());
            case CStrictTime st -> dialect.formatTime(st.value());
            case CLatestDate ld -> "CURRENT_DATE";
            case Variable v -> {
                if (v.name().equals(rowParam)) {
                    yield "";
                }
                yield dialect.quoteIdentifier(v.name());
            }
            case ClassInstance ci -> {
                if (ci.value() instanceof ColSpec cs) {
                    yield dialect.quoteIdentifier(cs.name());
                }
                throw new PureCompileException("PlanGenerator: unsupported ClassInstance in scalar: " + ci.type());
            }
            case EnumValue ev -> dialect.quoteStringLiteral(ev.value());
            case Collection coll -> coll.values().stream()
                    .map(v -> generateScalar(v, rowParam, mapping))
                    .collect(Collectors.joining(", "));
            default -> throw new PureCompileException(
                    "PlanGenerator: unsupported scalar: " + vs.getClass().getSimpleName());
        };
    }

    /** Legacy 2-arg version for backward compat. */
    String generateScalar(ValueSpecification vs, String rowParam) {
        return generateScalar(vs, rowParam, null);
    }

    /**
     * 4-arg version: generates scalar with table alias prefix.
     * e.g., $e.salary > 80000 → "t0"."SALARY" > 80000
     */
    String generateScalar(ValueSpecification vs, String rowParam, RelationalMapping mapping, String tableAlias) {
        return switch (vs) {
            case AppliedProperty ap -> {
                String colName = ap.property();
                if (mapping != null) {
                    var columnOpt = mapping.getColumnForProperty(colName);
                    if (columnOpt.isPresent()) {
                        colName = columnOpt.get();
                    }
                }
                yield dialect.quoteIdentifier(tableAlias) + "." + dialect.quoteIdentifier(colName);
            }
            case AppliedFunction af -> {
                // For binary ops, recurse with table alias
                String funcName = CleanCompiler.simpleName(af.function());
                var params = af.parameters();
                yield switch (funcName) {
                    case "greaterThan" -> generateScalar(params.get(0), rowParam, mapping, tableAlias)
                            + " > " + generateScalar(params.get(1), rowParam, mapping, tableAlias);
                    case "lessThan" -> generateScalar(params.get(0), rowParam, mapping, tableAlias)
                            + " < " + generateScalar(params.get(1), rowParam, mapping, tableAlias);
                    case "greaterThanEqual" -> generateScalar(params.get(0), rowParam, mapping, tableAlias)
                            + " >= " + generateScalar(params.get(1), rowParam, mapping, tableAlias);
                    case "lessThanEqual" -> generateScalar(params.get(0), rowParam, mapping, tableAlias)
                            + " <= " + generateScalar(params.get(1), rowParam, mapping, tableAlias);
                    case "equal" -> generateScalar(params.get(0), rowParam, mapping, tableAlias)
                            + " = " + generateScalar(params.get(1), rowParam, mapping, tableAlias);
                    case "notEqual" -> generateScalar(params.get(0), rowParam, mapping, tableAlias)
                            + " <> " + generateScalar(params.get(1), rowParam, mapping, tableAlias);
                    case "and" -> generateScalar(params.get(0), rowParam, mapping, tableAlias)
                            + " AND " + generateScalar(params.get(1), rowParam, mapping, tableAlias);
                    case "or" -> generateScalar(params.get(0), rowParam, mapping, tableAlias)
                            + " OR " + generateScalar(params.get(1), rowParam, mapping, tableAlias);
                    case "not" -> "NOT " + generateScalar(params.get(0), rowParam, mapping, tableAlias);
                    default -> generateScalarFunction(af, rowParam, mapping);
                };
            }
            // For literals, delegate to non-prefixed version
            default -> generateScalar(vs, rowParam, mapping);
        };
    }

    private String generateScalarFunction(AppliedFunction af, String rowParam, RelationalMapping mapping) {
        String funcName = CleanCompiler.simpleName(af.function());
        List<ValueSpecification> params = af.parameters();

        return switch (funcName) {
            // --- Comparison ---
            case "equal" -> binaryOp(params, "=", rowParam, mapping);
            case "greaterThan" -> binaryOp(params, ">", rowParam, mapping);
            case "greaterThanEqual" -> binaryOp(params, ">=", rowParam, mapping);
            case "lessThan" -> binaryOp(params, "<", rowParam, mapping);
            case "lessThanEqual" -> binaryOp(params, "<=", rowParam, mapping);

            // --- Logical ---
            case "and" -> "(" + binaryOp(params, "AND", rowParam, mapping) + ")";
            case "or" -> "(" + binaryOp(params, "OR", rowParam, mapping) + ")";
            case "not" -> "NOT (" + generateScalar(params.get(0), rowParam, mapping) + ")";

            // --- Arithmetic ---
            case "plus" -> binaryOp(params, "+", rowParam, mapping);
            case "minus" -> binaryOp(params, "-", rowParam, mapping);
            case "times" -> binaryOp(params, "*", rowParam, mapping);
            case "divide" -> binaryOp(params, "/", rowParam, mapping);
            case "rem" -> binaryOp(params, "%", rowParam, mapping);

            // --- String ---
            case "contains" -> generateScalar(params.get(0), rowParam, mapping)
                    + " LIKE '%' || " + generateScalar(params.get(1), rowParam, mapping) + " || '%'";
            case "startsWith" -> generateScalar(params.get(0), rowParam, mapping)
                    + " LIKE " + generateScalar(params.get(1), rowParam, mapping) + " || '%'";
            case "endsWith" -> generateScalar(params.get(0), rowParam, mapping)
                    + " LIKE '%' || " + generateScalar(params.get(1), rowParam, mapping);
            case "toLower" -> "LOWER(" + generateScalar(params.get(0), rowParam, mapping) + ")";
            case "toUpper" -> "UPPER(" + generateScalar(params.get(0), rowParam, mapping) + ")";
            case "length" -> "LENGTH(" + generateScalar(params.get(0), rowParam, mapping) + ")";
            case "trim" -> "TRIM(" + generateScalar(params.get(0), rowParam, mapping) + ")";
            case "toString" -> "CAST(" + generateScalar(params.get(0), rowParam, mapping) + " AS VARCHAR)";
            case "substring" -> {
                String str = generateScalar(params.get(0), rowParam, mapping);
                String start = generateScalar(params.get(1), rowParam, mapping);
                if (params.size() > 2) {
                    yield "SUBSTRING(" + str + ", " + start + ", "
                            + generateScalar(params.get(2), rowParam, mapping) + ")";
                }
                yield "SUBSTRING(" + str + ", " + start + ")";
            }
            case "indexOf" -> "POSITION(" + generateScalar(params.get(1), rowParam, mapping)
                    + " IN " + generateScalar(params.get(0), rowParam, mapping) + ")";
            case "replace" -> "REPLACE(" + generateScalar(params.get(0), rowParam, mapping) + ", "
                    + generateScalar(params.get(1), rowParam, mapping) + ", "
                    + generateScalar(params.get(2), rowParam, mapping) + ")";

            // --- Null checks ---
            case "isEmpty" -> generateScalar(params.get(0), rowParam, mapping) + " IS NULL";
            case "isNotEmpty" -> generateScalar(params.get(0), rowParam, mapping) + " IS NOT NULL";

            // --- Math ---
            case "abs" -> "ABS(" + generateScalar(params.get(0), rowParam, mapping) + ")";
            case "ceiling", "ceil" -> "CEIL(" + generateScalar(params.get(0), rowParam, mapping) + ")";
            case "floor" -> "FLOOR(" + generateScalar(params.get(0), rowParam, mapping) + ")";
            case "round" -> {
                if (params.size() > 1) {
                    yield "ROUND(" + generateScalar(params.get(0), rowParam, mapping) + ", "
                            + generateScalar(params.get(1), rowParam, mapping) + ")";
                }
                yield "ROUND(" + generateScalar(params.get(0), rowParam, mapping) + ")";
            }
            case "sqrt" -> "SQRT(" + generateScalar(params.get(0), rowParam, mapping) + ")";
            case "pow", "power" -> "POWER(" + generateScalar(params.get(0), rowParam, mapping) + ", "
                    + generateScalar(params.get(1), rowParam, mapping) + ")";
            case "log" -> "LOG(" + generateScalar(params.get(0), rowParam, mapping) + ")";
            case "exp" -> "EXP(" + generateScalar(params.get(0), rowParam, mapping) + ")";

            // --- Cast ---
            case "toInteger", "parseInteger" -> "CAST("
                    + generateScalar(params.get(0), rowParam, mapping) + " AS INTEGER)";
            case "toFloat", "parseFloat" -> "CAST("
                    + generateScalar(params.get(0), rowParam, mapping) + " AS DOUBLE)";
            case "toDecimal", "parseDecimal" -> "CAST("
                    + generateScalar(params.get(0), rowParam, mapping) + " AS DECIMAL)";

            // --- If/Case ---
            case "if" -> {
                String cond = generateScalar(params.get(0), rowParam, mapping);
                LambdaFunction thenLambda = (LambdaFunction) params.get(1);
                LambdaFunction elseLambda = (LambdaFunction) params.get(2);
                String thenVal = generateScalar(thenLambda.body().get(0), rowParam, mapping);
                String elseVal = generateScalar(elseLambda.body().get(0), rowParam, mapping);
                yield "CASE WHEN " + cond + " THEN " + thenVal + " ELSE " + elseVal + " END";
            }

            // --- In ---
            case "in" -> {
                String left = generateScalar(params.get(0), rowParam, mapping);
                if (params.get(1) instanceof Collection coll) {
                    String vals = coll.values().stream()
                            .map(v -> generateScalar(v, rowParam, mapping))
                            .collect(Collectors.joining(", "));
                    yield left + " IN (" + vals + ")";
                }
                yield left + " IN (" + generateScalar(params.get(1), rowParam, mapping) + ")";
            }

            // --- Coalesce ---
            case "coalesce" -> "COALESCE(" + params.stream()
                    .map(v -> generateScalar(v, rowParam, mapping))
                    .collect(Collectors.joining(", ")) + ")";

            default -> throw new PureCompileException(
                    "PlanGenerator: unsupported scalar function '" + funcName + "'");
        };
    }

    // ========== Utilities ==========

    private String binaryOp(List<ValueSpecification> params, String op, String rowParam,
            RelationalMapping mapping) {
        return generateScalar(params.get(0), rowParam, mapping) + " " + op + " "
                + generateScalar(params.get(1), rowParam, mapping);
    }

    private long extractIntValue(ValueSpecification vs) {
        if (vs instanceof CInteger i)
            return i.value().longValue();
        if (vs instanceof CFloat f)
            return (long) f.value();
        throw new PureCompileException("Expected integer, got: " + vs.getClass().getSimpleName());
    }

    private String extractString(ValueSpecification vs) {
        if (vs instanceof CString s)
            return s.value();
        throw new PureCompileException("Expected string, got: " + vs.getClass().getSimpleName());
    }

    private String extractColumnNameFromParam(ValueSpecification vs) {
        if (vs instanceof ClassInstance ci && ci.value() instanceof ColSpec cs)
            return cs.name();
        if (vs instanceof CString s)
            return s.value();
        throw new PureCompileException("Expected column name, got: " + vs.getClass().getSimpleName());
    }

    private List<String> extractColumnNames(ValueSpecification vs) {
        if (vs instanceof ClassInstance ci && ci.value() instanceof ColSpecArray csa) {
            return csa.colSpecs().stream().map(ColSpec::name).toList();
        }
        if (vs instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
            return List.of(cs.name());
        }
        return List.of(extractColumnNameFromParam(vs));
    }

    private List<ValueSpecification> extractList(ValueSpecification vs) {
        if (vs instanceof Collection coll)
            return coll.values();
        return List.of(vs);
    }

    private List<String> extractStringList(ValueSpecification vs) {
        if (vs instanceof Collection coll) {
            return coll.values().stream()
                    .filter(v -> v instanceof CString)
                    .map(v -> ((CString) v).value())
                    .toList();
        }
        if (vs instanceof CString s)
            return List.of(s.value());
        return List.of();
    }

    /** Extracts table name from "store::DB.TABLE" → "TABLE". */
    private String extractTableName(String content) {
        int dotIdx = content.lastIndexOf('.');
        return dotIdx > 0 ? content.substring(dotIdx + 1) : content;
    }

    /**
     * Resolves a property/column name to the actual SQL column name.
     * Uses mapping if available (property → column), otherwise returns as-is.
     */
    private String resolveColumnName(String name, RelationalMapping mapping) {
        if (mapping != null) {
            var opt = mapping.getColumnForProperty(name);
            if (opt.isPresent()) {
                return opt.get();
            }
        }
        return name;
    }

    /** Extracts property name from a lambda: {e|$e.name} → "name". */
    private String extractPropertyNameFromLambda(ValueSpecification vs) {
        if (vs instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            ValueSpecification body = lf.body().get(0);
            if (body instanceof AppliedProperty ap) {
                return ap.property();
            }
        }
        return null;
    }

    /** Extracts class name from PackageableElementPtr. */
    private String extractClassName(ValueSpecification vs) {
        if (vs instanceof PackageableElementPtr pep) {
            String fqn = pep.fullPath();
            int idx = fqn.lastIndexOf("::");
            return idx > 0 ? fqn.substring(idx + 2) : fqn;
        }
        return "UNKNOWN";
    }

    private static String simpleName(String qualifiedName) {
        return CleanCompiler.simpleName(qualifiedName);
    }
}
