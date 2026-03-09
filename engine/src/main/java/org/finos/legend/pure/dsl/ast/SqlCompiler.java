package org.finos.legend.pure.dsl.ast;

import org.finos.legend.engine.store.Join;
import org.finos.legend.engine.store.RelationalMapping;
import org.finos.legend.engine.transpiler.SQLDialect;
import org.finos.legend.pure.dsl.ModelContext;
import org.finos.legend.pure.dsl.PureCompileException;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
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
public class SqlCompiler {

    private final IdentityHashMap<ValueSpecification, TypeInfo> types;
    private final SQLDialect dialect;
    private final ModelContext modelContext;
    private int tableAliasCounter = 0;

    public SqlCompiler(IdentityHashMap<ValueSpecification, TypeInfo> types, SQLDialect dialect) {
        this(types, dialect, null);
    }

    public SqlCompiler(IdentityHashMap<ValueSpecification, TypeInfo> types, SQLDialect dialect,
            ModelContext modelContext) {
        this.types = types;
        this.dialect = dialect;
        this.modelContext = modelContext;
    }

    private String nextTableAlias() {
        return "t" + tableAliasCounter++;
    }

    /**
     * Compiles a TypedValueSpec to a SqlBuilder.
     * Uses resultType from CleanCompiler to route: scalar queries produce
     * SELECT expr AS "value", relation queries produce full SELECT.
     */
    public SqlBuilder compile(TypedValueSpec tvs) {
        if (tvs.resultType().columns().isEmpty() && isScalarAst(tvs.ast())) {
            return compileScalarQuery(tvs.ast(), tvs.mapping());
        }
        return compileRelation(tvs.ast(), tvs.mapping());
    }

    /** Backward-compatible entry point. */
    public SqlBuilder compile(ValueSpecification ast, RelationalMapping mapping) {
        return compileRelation(ast, mapping);
    }

    /**
     * Compiles a standalone scalar query → SELECT scalarExpr AS "value".
     * Used for queries like |42, |'hello', |%2024-01-15->adjust(...), etc.
     */
    private SqlBuilder compileScalarQuery(ValueSpecification ast, RelationalMapping mapping) {
        ValueSpecification body = ast;
        if (ast instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            body = lf.body().getLast();
        }
        String scalar = compileScalar(body, null, mapping);
        return new SqlBuilder().addSelect(scalar, dialect.quoteIdentifier("value"));
    }

    /**
     * Returns true if the AST represents a scalar expression (not a relation
     * source).
     */
    private boolean isScalarAst(ValueSpecification vs) {
        if (vs instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            vs = lf.body().getLast();
        }
        // Literals are always scalar
        if (vs instanceof CInteger || vs instanceof CFloat || vs instanceof CDecimal
                || vs instanceof CString || vs instanceof CBoolean
                || vs instanceof CDateTime || vs instanceof CStrictDate
                || vs instanceof CStrictTime || vs instanceof CLatestDate) {
            return true;
        }
        // A function not in the relation set is scalar (adjust, plus, etc.)
        if (vs instanceof AppliedFunction af) {
            return !(vs instanceof ClassInstance);
        }
        return false;
    }

    // ========== Relation Compilation ==========

    private SqlBuilder compileRelation(ValueSpecification vs, RelationalMapping mapping) {
        return switch (vs) {
            case AppliedFunction af -> compileFunction(af, mapping);
            case LambdaFunction lf -> {
                if (lf.body().isEmpty())
                    throw new PureCompileException("SqlCompiler: empty lambda body");
                yield compileRelation(lf.body().getLast(), mapping);
            }
            case ClassInstance ci -> compileClassInstance(ci, mapping);
            default -> throw new PureCompileException(
                    "SqlCompiler: cannot compile: " + vs.getClass().getSimpleName());
        };
    }

    private SqlBuilder compileClassInstance(ClassInstance ci, RelationalMapping mapping) {
        return switch (ci.type()) {
            case "relation" -> {
                // #>{DB.TABLE}# → SELECT * FROM "TABLE" AS "t0"
                String tableRef = (String) ci.value();
                String tableName = tableRef.contains(".")
                        ? tableRef.substring(tableRef.lastIndexOf('.') + 1)
                        : tableRef;
                String alias = nextTableAlias();
                yield new SqlBuilder()
                        .selectStar()
                        .from(dialect.quoteIdentifier(tableName), dialect.quoteIdentifier(alias));
            }
            case "tdsLiteral" -> {
                // CleanCompiler stores parsed TdsLiteral; nested ASTs may still have raw String
                org.finos.legend.pure.dsl.TdsLiteral tds = ci.value() instanceof org.finos.legend.pure.dsl.TdsLiteral t
                        ? t
                        : org.finos.legend.pure.dsl.TdsLiteral.parse((String) ci.value());
                yield compileTdsLiteral(tds);
            }
            default -> throw new PureCompileException(
                    "SqlCompiler: unsupported ClassInstance type: " + ci.type());
        };
    }

    /**
     * Compiles a parsed TdsLiteral to VALUES SQL.
     * Receives structured data from CleanCompiler — no string parsing here.
     */
    private SqlBuilder compileTdsLiteral(org.finos.legend.pure.dsl.TdsLiteral tds) {
        List<String> quotedCols = tds.columns().stream()
                .map(c -> dialect.quoteIdentifier(c.name())).toList();

        List<List<String>> rows = tds.rows().stream()
                .map(row -> row.stream().map(this::formatTdsValue).toList())
                .toList();

        return new SqlBuilder()
                .selectStar()
                .fromValues(rows, dialect.quoteIdentifier("_tds"), quotedCols);
    }

    /** Formats a typed TDS cell value for SQL. */
    private String formatTdsValue(Object val) {
        if (val == null)
            return "NULL";
        if (val instanceof Boolean b)
            return b ? "TRUE" : "FALSE";
        if (val instanceof Number)
            return val.toString();
        return dialect.quoteStringLiteral(val.toString());
    }

    private SqlBuilder compileFunction(AppliedFunction af, RelationalMapping mapping) {
        String funcName = simpleName(af.function());
        return switch (funcName) {
            case "getAll" -> compileGetAll(af, mapping);
            case "table", "tableReference" -> compileTableAccess(af, mapping);
            case "filter" -> compileFilter(af, mapping);
            case "project" -> compileProject(af, mapping);
            case "sort" -> compileSort(af, mapping);
            case "limit", "take" -> compileLimit(af, mapping);
            case "drop" -> compileDrop(af, mapping);
            case "slice" -> compileSlice(af, mapping);
            case "first" -> compileFirst(af, mapping);
            case "distinct" -> compileDistinct(af, mapping);
            case "select" -> compileSelect(af, mapping);
            case "rename" -> compileRename(af, mapping);
            case "concatenate", "union" -> compileConcatenate(af, mapping);
            case "groupBy" -> compileGroupBy(af, mapping);
            case "aggregate" -> compileAggregate(af, mapping);
            case "extend" -> compileExtend(af, mapping);
            case "join" -> compileJoin(af, mapping);
            case "from" -> compileFrom(af, mapping);
            default -> throw new PureCompileException(
                    "SqlCompiler: unsupported function '" + funcName + "'");
        };
    }

    // ========== getAll / table ==========

    private SqlBuilder compileGetAll(AppliedFunction af, RelationalMapping mapping) {
        String tableName;
        if (mapping != null) {
            tableName = mapping.table().name();
        } else {
            tableName = extractClassName(af.parameters().get(0));
        }
        String alias = nextTableAlias();
        return new SqlBuilder()
                .selectStar()
                .from(dialect.quoteIdentifier(tableName), dialect.quoteIdentifier(alias));
    }

    private SqlBuilder compileTableAccess(AppliedFunction af, RelationalMapping mapping) {
        String content = extractString(af.parameters().get(0));
        String tableName = extractTableName(content);
        if (mapping != null) {
            tableName = mapping.table().name();
        }
        return new SqlBuilder()
                .selectStar()
                .from(dialect.quoteIdentifier(tableName), dialect.quoteIdentifier("t0"));
    }

    // ========== filter ==========

    private SqlBuilder compileFilter(AppliedFunction af, RelationalMapping mapping) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = compileRelation(params.get(0), mapping);
        LambdaFunction lambda = (LambdaFunction) params.get(1);
        String paramName = lambda.parameters().isEmpty() ? "x" : lambda.parameters().get(0).name();

        // Minimize wrapping: if source has a direct FROM table (not a subquery),
        // we can inline the WHERE clause regardless of whether it's SELECT * or
        // explicit columns
        if (!source.hasWhere() && !source.hasGroupBy()
                && !source.hasOrderBy() && source.getFromSubquery() == null
                && source.getFromTable() != null) {
            // Use table-alias-prefixed scalar so all column refs get t0. prefix
            String tableAlias = source.getFromAlias() != null ? unquote(source.getFromAlias()) : "t0";
            String whereClause = compileScalar(lambda.body().get(0), paramName, mapping, tableAlias);
            source.addWhere(whereClause);
            return source;
        }
        // Subquery wrapping — columns resolve by name, no prefix needed
        String whereClause = compileScalar(lambda.body().get(0), paramName, mapping);
        return new SqlBuilder()
                .selectStar()
                .fromSubquery(source, "filter_src")
                .addWhere(whereClause);
    }

    // ========== project ==========

    private SqlBuilder compileProject(AppliedFunction af, RelationalMapping mapping) {
        List<ValueSpecification> params = af.parameters();

        // Extract projection lambdas and aliases
        List<ValueSpecification> lambdaSpecs;
        List<String> aliases;
        if (params.get(1) instanceof Collection coll) {
            lambdaSpecs = coll.values();
            aliases = params.size() > 2 ? extractStringList(params.get(2)) : List.of();
        } else {
            lambdaSpecs = params.subList(1, params.size());
            aliases = List.of();
        }

        String tableAlias = nextTableAlias();
        String tableName = resolveTableNameFromSource(params.get(0), mapping);

        SqlBuilder builder = new SqlBuilder();

        // Track association joins: assocProperty -> cached info
        record AssocJoinInfo(String alias, RelationalMapping mapping, Join join) {
        }
        java.util.Map<String, AssocJoinInfo> assocJoins = new java.util.LinkedHashMap<>();

        // Build column projections
        for (int i = 0; i < lambdaSpecs.size(); i++) {
            List<String> propPath = extractPropertyPath(lambdaSpecs.get(i));
            String alias = i < aliases.size() ? aliases.get(i) : propPath.getLast();

            if (propPath.size() > 1 && modelContext != null) {
                // Association navigation: e.g., ["items", "productName"]
                String assocProp = propPath.get(0);
                String leafProp = propPath.getLast();

                AssocJoinInfo joinInfo = assocJoins.get(assocProp);
                String targetAlias;
                RelationalMapping targetMapping;

                if (joinInfo == null) {
                    // Discover target via ModelContext association lookup
                    String sourceClassName = mapping.pureClass().name();
                    var assocNav = modelContext.findAssociationByProperty(sourceClassName, assocProp);
                    if (assocNav.isEmpty()) {
                        throw new PureCompileException(
                                "SqlCompiler: no association found for property '" + assocProp
                                        + "' on class '" + sourceClassName + "'");
                    }
                    String targetClassName = assocNav.get().targetClassName();
                    var targetMappingOpt = modelContext.findMapping(targetClassName);
                    if (targetMappingOpt.isEmpty()) {
                        throw new PureCompileException(
                                "SqlCompiler: no mapping found for association target class '"
                                        + targetClassName + "'");
                    }
                    targetMapping = targetMappingOpt.get();
                    targetAlias = "j" + (assocJoins.size() + 1);
                    Join assocJoin = assocNav.get().join();
                    assocJoins.put(assocProp, new AssocJoinInfo(targetAlias, targetMapping, assocJoin));
                } else {
                    targetAlias = joinInfo.alias();
                    targetMapping = joinInfo.mapping();
                }

                // Resolve column from target mapping
                String colExpr = resolveColumnExpr(leafProp, targetMapping, targetAlias);
                builder.addSelect(colExpr, dialect.quoteIdentifier(alias));
            } else {
                // Simple single-hop property
                String propertyName = propPath.getLast();
                String colExpr = resolveColumnExpr(propertyName, mapping, tableAlias);
                builder.addSelect(colExpr, dialect.quoteIdentifier(alias));
            }
        }

        builder.from(dialect.quoteIdentifier(tableName), dialect.quoteIdentifier(tableAlias));

        // Add LEFT OUTER JOINs for association projections
        for (var entry : assocJoins.entrySet()) {
            AssocJoinInfo info = entry.getValue();
            Join join = info.join();
            if (join != null && mapping != null) {
                String targetTableName = info.mapping().table().name();
                String leftCol = join.getColumnForTable(mapping.table().name());
                String rightCol = join.getColumnForTable(targetTableName);
                String onCondition = dialect.quoteIdentifier(tableAlias) + "."
                        + dialect.quoteIdentifier(leftCol) + " = "
                        + dialect.quoteIdentifier(info.alias()) + "."
                        + dialect.quoteIdentifier(rightCol);
                builder.addJoin(SqlBuilder.JoinType.LEFT,
                        dialect.quoteIdentifier(targetTableName),
                        dialect.quoteIdentifier(info.alias()), onCondition);
            }
        }

        // Extract filter clause if source is a filter (flatten into WHERE)
        String filterClause = extractFilterClause(params.get(0), tableAlias, mapping);
        if (filterClause != null) {
            builder.addWhere(filterClause);
        }

        return builder;
    }

    /**
     * Extracts the full property path from a lambda, e.g., ["addresses", "street"].
     */
    private List<String> extractPropertyPath(ValueSpecification vs) {
        if (vs instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            return extractPropertyChain(lf.body().get(0));
        }
        throw new PureCompileException(
                "SqlCompiler: cannot extract property path from " + vs.getClass().getSimpleName());
    }

    /** Recursively extracts property chain from nested AppliedProperty. */
    private List<String> extractPropertyChain(ValueSpecification vs) {
        if (vs instanceof AppliedProperty ap) {
            if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof AppliedProperty) {
                List<String> parent = extractPropertyChain(ap.parameters().get(0));
                var result = new ArrayList<>(parent);
                result.add(ap.property());
                return result;
            }
            return List.of(ap.property());
        }
        throw new PureCompileException(
                "SqlCompiler: cannot extract property chain from " + vs.getClass().getSimpleName());
    }

    /**
     * Resolves a property to its column expression, handling expression mappings.
     */
    private String resolveColumnExpr(String propertyName, RelationalMapping mapping, String alias) {
        if (mapping != null && propertyName != null) {
            var pmOpt = mapping.getPropertyMapping(propertyName);
            if (pmOpt.isPresent() && pmOpt.get().hasExpression()) {
                return translateExpressionToSql(pmOpt.get().expressionString(),
                        pmOpt.get().columnName(), alias);
            }
            if (pmOpt.isPresent()) {
                return dialect.quoteIdentifier(alias) + "."
                        + dialect.quoteIdentifier(pmOpt.get().columnName());
            }
        }
        return dialect.quoteIdentifier(alias) + "." + dialect.quoteIdentifier(propertyName);
    }

    /**
     * Translates a Pure expression mapping string to SQL.
     * Examples:
     * [OrderDB] T_ORDERS.DATA->get('customerName', @String)
     * → CAST(("t0"."DATA")->>'customerName' AS VARCHAR)
     * [OrderDB] T_ORDERS.DATA->get('price', @Integer)
     * → CAST(("t0"."DATA")->>'price' AS BIGINT)
     */
    private String translateExpressionToSql(String expression, String columnName, String alias) {
        // Pattern: ->get('key', @Type) or ->get('key')
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("->get\\('(\\w+)'(?:,\\s*@(\\w+))?\\)")
                .matcher(expression);

        if (m.find()) {
            String key = m.group(1);
            String pureType = m.group(2); // may be null

            String qualifiedCol = "(" + dialect.quoteIdentifier(alias) + "."
                    + dialect.quoteIdentifier(columnName) + ")";
            String jsonAccess = qualifiedCol + "->>" + dialect.quoteStringLiteral(key);

            if (pureType != null) {
                String sqlType = switch (pureType) {
                    case "String" -> "VARCHAR";
                    case "Integer" -> "BIGINT";
                    case "Float", "Decimal" -> "DOUBLE";
                    case "Boolean" -> "BOOLEAN";
                    case "Date", "StrictDate" -> "DATE";
                    case "DateTime" -> "TIMESTAMP";
                    default -> "VARCHAR";
                };
                return "CAST((" + jsonAccess + ") AS " + sqlType + ")";
            }
            return jsonAccess;
        }

        // Fallback: use expression as-is with qualified column
        String qualifiedCol = dialect.quoteIdentifier(alias) + "." + dialect.quoteIdentifier(columnName);
        return expression.replaceAll("\\[\\w+\\]\\s+\\w+\\.\\w+", qualifiedCol);
    }

    /**
     * Resolves an association property to its target RelationalMapping and Join.
     * Uses ModelContext.findAssociationByProperty for proper resolution.
     *
     * @return target mapping and join, or throws if not found
     */
    private AssociationResult resolveAssociationTarget(String assocProp,
            RelationalMapping sourceMapping) {
        if (modelContext == null || sourceMapping == null) {
            throw new PureCompileException(
                    "SqlCompiler: cannot resolve association '" + assocProp + "' — no model context");
        }

        String sourceClassName = sourceMapping.pureClass().name();
        var assocNav = modelContext.findAssociationByProperty(sourceClassName, assocProp);
        if (assocNav.isEmpty()) {
            throw new PureCompileException(
                    "SqlCompiler: no association found for property '" + assocProp
                            + "' on class '" + sourceClassName + "'");
        }

        String targetClassName = assocNav.get().targetClassName();
        var targetMapping = modelContext.findMapping(targetClassName);
        if (targetMapping.isEmpty()) {
            throw new PureCompileException(
                    "SqlCompiler: no mapping found for association target class '" + targetClassName + "'");
        }

        return new AssociationResult(targetMapping.get(), assocNav.get().join());
    }

    /**
     * Result of resolving an association: the target mapping and the join to use.
     */
    private record AssociationResult(RelationalMapping mapping, Join join) {
    }

    /**
     * Resolves the RelationalMapping for a relation expression by finding the
     * getAll class.
     */
    private RelationalMapping resolveRelationMapping(ValueSpecification vs, RelationalMapping defaultMapping) {
        if (modelContext == null)
            return defaultMapping;
        if (vs instanceof AppliedFunction af) {
            String funcName = simpleName(af.function());
            if ("getAll".equals(funcName) && !af.parameters().isEmpty()) {
                String className = extractClassName(af.parameters().get(0));
                var opt = modelContext.findMapping(className);
                if (opt.isPresent())
                    return opt.get();
            }
            if (!af.parameters().isEmpty()) {
                return resolveRelationMapping(af.parameters().get(0), defaultMapping);
            }
        }
        return defaultMapping;
    }

    // ========== sort ==========

    private SqlBuilder compileSort(AppliedFunction af, RelationalMapping mapping) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = compileRelation(params.get(0), mapping);

        // Parse sort specs
        List<SqlBuilder.OrderByColumn> sortCols;
        if (params.size() >= 2 && params.get(1) instanceof CString colStr) {
            // Legacy: sort('colName', 'asc'/'desc')
            String dir = "ASC";
            if (params.size() >= 3 && params.get(2) instanceof CString dirStr) {
                dir = dirStr.value().toUpperCase();
            }
            sortCols = List.of(new SqlBuilder.OrderByColumn(
                    dialect.quoteIdentifier(colStr.value()),
                    "ASC".equals(dir) ? SqlBuilder.SortDirection.ASC : SqlBuilder.SortDirection.DESC,
                    SqlBuilder.NullsPosition.DEFAULT));
        } else {
            sortCols = parseSortSpecs(params.get(1), mapping);
        }

        // Minimize wrapping: if source has no ORDER BY, add directly
        if (!source.hasOrderBy()) {
            for (var sc : sortCols) {
                source.addOrderBy(sc.expression(), sc.direction(), sc.nulls());
            }
            return source;
        }

        // Source already has ORDER BY — wrap in subquery
        SqlBuilder wrapper = new SqlBuilder()
                .selectStar()
                .fromSubquery(source, "sort_src");
        for (var sc : sortCols) {
            wrapper.addOrderBy(sc.expression(), sc.direction(), sc.nulls());
        }
        return wrapper;
    }

    private List<SqlBuilder.OrderByColumn> parseSortSpecs(ValueSpecification vs, RelationalMapping mapping) {
        List<SqlBuilder.OrderByColumn> result = new ArrayList<>();
        List<ValueSpecification> specs = (vs instanceof Collection coll) ? coll.values() : List.of(vs);
        for (var spec : specs) {
            result.add(parseSingleSortSpec(spec, mapping));
        }
        return result;
    }

    private SqlBuilder.OrderByColumn parseSingleSortSpec(ValueSpecification vs, RelationalMapping mapping) {
        if (vs instanceof ClassInstance ci && "sortInfo".equals(ci.type())) {
            if (ci.value() instanceof ColSpec cs) {
                return new SqlBuilder.OrderByColumn(
                        dialect.quoteIdentifier(cs.name()),
                        SqlBuilder.SortDirection.ASC,
                        SqlBuilder.NullsPosition.DEFAULT);
            }
        }
        if (vs instanceof AppliedFunction af) {
            String funcName = simpleName(af.function());
            if ("asc".equals(funcName) || "ascending".equals(funcName)
                    || "desc".equals(funcName) || "descending".equals(funcName)) {
                String col = extractColumnNameFromParam(af.parameters().get(0));
                SqlBuilder.SortDirection dir = ("asc".equals(funcName) || "ascending".equals(funcName))
                        ? SqlBuilder.SortDirection.ASC
                        : SqlBuilder.SortDirection.DESC;
                return new SqlBuilder.OrderByColumn(
                        dialect.quoteIdentifier(col), dir, SqlBuilder.NullsPosition.DEFAULT);
            }
        }
        if (vs instanceof CString s) {
            return new SqlBuilder.OrderByColumn(
                    dialect.quoteIdentifier(s.value()),
                    SqlBuilder.SortDirection.ASC,
                    SqlBuilder.NullsPosition.DEFAULT);
        }
        throw new PureCompileException("SqlCompiler: unsupported sort spec: " + vs);
    }

    // ========== limit / drop / slice / first ==========

    private SqlBuilder compileLimit(AppliedFunction af, RelationalMapping mapping) {
        SqlBuilder source = compileRelation(af.parameters().get(0), mapping);
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

    private SqlBuilder compileDrop(AppliedFunction af, RelationalMapping mapping) {
        SqlBuilder source = compileRelation(af.parameters().get(0), mapping);
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

    private SqlBuilder compileSlice(AppliedFunction af, RelationalMapping mapping) {
        SqlBuilder source = compileRelation(af.parameters().get(0), mapping);
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

    private SqlBuilder compileFirst(AppliedFunction af, RelationalMapping mapping) {
        SqlBuilder source = compileRelation(af.parameters().get(0), mapping);
        if (!source.hasLimit()) {
            source.limit(1);
            return source;
        }
        return new SqlBuilder()
                .selectStar()
                .fromSubquery(source, "limit_src")
                .limit(1);
    }

    // ========== distinct ==========

    private SqlBuilder compileDistinct(AppliedFunction af, RelationalMapping mapping) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = compileRelation(params.get(0), mapping);
        // Inline DISTINCT into source when no conflicting clauses
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

    private SqlBuilder compileSelect(AppliedFunction af, RelationalMapping mapping) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = compileRelation(params.get(0), mapping);

        SqlBuilder builder = new SqlBuilder()
                .fromSubquery(source, "subq");

        for (int i = 1; i < params.size(); i++) {
            for (String col : extractColumnNames(params.get(i))) {
                builder.addSelect(dialect.quoteIdentifier(col), dialect.quoteIdentifier(col));
            }
        }
        return builder;
    }

    // ========== rename ==========

    private SqlBuilder compileRename(AppliedFunction af, RelationalMapping mapping) {
        SqlBuilder source = compileRelation(af.parameters().get(0), mapping);
        List<ValueSpecification> params = af.parameters();

        String oldName = extractColumnNameFromParam(params.get(1));
        String newName = extractColumnNameFromParam(params.get(2));

        // Build SELECT using DuckDB EXCLUDE + rename pattern
        SqlBuilder builder = new SqlBuilder().fromSubquery(source, "rename_src");
        builder.selectStar();
        builder.addStarExcept(dialect.quoteIdentifier(oldName));
        builder.addSelect(dialect.quoteIdentifier(oldName), dialect.quoteIdentifier(newName));
        return builder;
    }

    // ========== concatenate / union ==========

    private SqlBuilder compileConcatenate(AppliedFunction af, RelationalMapping mapping) {
        SqlBuilder left = compileRelation(af.parameters().get(0), mapping);
        SqlBuilder right = compileRelation(af.parameters().get(1), mapping);
        left.unionWrapped(right, true);
        return left;
    }

    // ========== groupBy ==========

    private SqlBuilder compileGroupBy(AppliedFunction af, RelationalMapping mapping) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = compileRelation(params.get(0), mapping);

        SqlBuilder builder = new SqlBuilder()
                .fromSubquery(source, "groupby_src");

        if (params.size() >= 3) {
            // Group columns
            List<ValueSpecification> groupSpecs = extractList(params.get(1));
            for (var spec : groupSpecs) {
                String colName = extractPropertyOrColumnName(spec);
                if (colName != null) {
                    String quoted = dialect.quoteIdentifier(colName);
                    builder.addSelect(quoted, null);
                    builder.addGroupBy(quoted);
                }
            }

            // Aggregate specs
            List<ValueSpecification> aggSpecs = extractList(params.get(2));
            List<String> aliases = params.size() > 3 ? extractStringList(params.get(3)) : List.of();
            List<String> groupColNames = groupSpecs.stream()
                    .map(this::extractPropertyOrColumnName)
                    .toList();

            for (int i = 0; i < aggSpecs.size(); i++) {
                var aggSpec = aggSpecs.get(i);
                String sourceCol;
                String aggFunc = "SUM";
                String alias;

                if (aggSpec instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
                    alias = cs.name();
                    sourceCol = cs.function1() != null
                            ? extractPropertyNameFromLambda(cs.function1())
                            : cs.name();
                    if (cs.function2() != null) {
                        aggFunc = extractAggFunction(cs.function2());
                    }
                } else {
                    sourceCol = extractPropertyOrColumnName(aggSpec);
                    int aliasIndex = groupColNames.size() + i;
                    alias = aliasIndex < aliases.size() ? aliases.get(aliasIndex) : sourceCol + "_agg";
                }

                String expr = aggFunc + "(" + dialect.quoteIdentifier(sourceCol) + ")";
                builder.addSelect(expr, dialect.quoteIdentifier(alias));
            }
        }

        return builder;
    }

    // ========== aggregate ==========

    private SqlBuilder compileAggregate(AppliedFunction af, RelationalMapping mapping) {
        // Same as groupBy but with no group columns
        return compileGroupBy(af, mapping);
    }

    // ========== extend (window functions) ==========

    private SqlBuilder compileExtend(AppliedFunction af, RelationalMapping mapping) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = compileRelation(params.get(0), mapping);

        // Look for window function pattern: extend(source, over(...), ~colSpec)
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

        if (overSpec != null && windowColSpec != null) {
            String windowFunc = extractWindowFunction(windowColSpec);
            String overClause = generateOverClause(overSpec);
            String alias = windowColSpec.name();

            if (windowFunc.startsWith("WRAPPED:")) {
                // Post-processor wrapping a window function
                // e.g., WRAPPED:ROUND(CUME_DIST(), 2) → ROUND(CUME_DIST() OVER(...), 2)
                String wrappedExpr = windowFunc.substring("WRAPPED:".length());
                // Insert OVER clause after the inner window function's closing parens
                // Find the inner window function (first "()" pattern) and insert OVER after it
                int innerClose = wrappedExpr.indexOf("()");
                if (innerClose >= 0) {
                    String fullExpr = wrappedExpr.substring(0, innerClose + 2)
                            + " OVER (" + overClause + ")"
                            + wrappedExpr.substring(innerClose + 2);
                    // Use addSelect with *, func expression as additional column
                    SqlBuilder b = new SqlBuilder().selectStar().fromSubquery(source, "window_src");
                    b.addWindowColumn(fullExpr, null, dialect.quoteIdentifier(alias));
                    return b;
                }
            }

            return new SqlBuilder()
                    .selectStar()
                    .addWindowColumn(windowFunc, overClause, dialect.quoteIdentifier(alias))
                    .fromSubquery(source, "window_src");
        }

        // Aggregate extend without OVER — e.g.,
        // extend(~totalSalary:x|$x.salary:y|$y->plus())
        // Generates: SUM("salary") OVER ()
        if (windowColSpec != null && windowColSpec.function2() != null) {
            String windowFunc = extractWindowFunction(windowColSpec);
            String alias = windowColSpec.name();
            return new SqlBuilder()
                    .selectStar()
                    .addWindowColumn(windowFunc, "", dialect.quoteIdentifier(alias))
                    .fromSubquery(source, "window_src");
        }

        // Simple extend (computed column) — placeholder
        return new SqlBuilder()
                .selectStar()
                .fromSubquery(source, "extend_src");
    }

    // ========== join ==========

    private SqlBuilder compileJoin(AppliedFunction af, RelationalMapping mapping) {
        List<ValueSpecification> params = af.parameters();
        // join(left, right, joinType, condition)
        if (params.size() < 3) {
            throw new PureCompileException("SqlCompiler: join() requires left, right, and condition");
        }

        SqlBuilder left = compileRelation(params.get(0), mapping);
        // Resolve right-side mapping independently (e.g., Address.all() needs Address
        // mapping)
        RelationalMapping rightMapping = resolveRelationMapping(params.get(1), mapping);
        SqlBuilder right = compileRelation(params.get(1), rightMapping);

        // Determine join type
        SqlBuilder.JoinType joinType = SqlBuilder.JoinType.INNER;
        int conditionIdx = 2; // default: join(left, right, condition)
        if (params.size() >= 4) {
            // join(left, right, JoinType, condition)
            joinType = extractJoinType(params.get(2));
            conditionIdx = 3;
        } else if (params.get(2) instanceof EnumValue) {
            // join(left, right, JoinType) — no condition ??
            joinType = extractJoinType(params.get(2));
            conditionIdx = -1; // cross join, no condition
        }

        // Build ON condition from lambda
        String onCondition = null;
        if (conditionIdx >= 0 && conditionIdx < params.size()) {
            var condSpec = params.get(conditionIdx);
            if (condSpec instanceof LambdaFunction lf) {
                // Lambda params: {l, r | condition}
                String leftParam = lf.parameters().size() > 0 ? lf.parameters().get(0).name() : "l";
                String rightParam = lf.parameters().size() > 1 ? lf.parameters().get(1).name() : "r";
                onCondition = compileJoinCondition(lf.body().get(0), leftParam, rightParam,
                        left, right);
            }
        }

        // Wrap left in a base builder, add right as a JOIN subquery
        SqlBuilder result = new SqlBuilder()
                .selectStar()
                .fromSubquery(left, "\"left_src\"");

        result.addJoin(new SqlBuilder.JoinClause(
                joinType,
                null, // no table name — using subquery
                "\"right_src\"", // right alias
                right, // right as subquery
                onCondition,
                null)); // no USING

        return result;
    }

    private SqlBuilder.JoinType extractJoinType(ValueSpecification vs) {
        String typeName;
        if (vs instanceof EnumValue ev) {
            typeName = ev.value();
        } else if (vs instanceof CString cs) {
            typeName = cs.value();
        } else if (vs instanceof AppliedProperty ap) {
            typeName = ap.property();
        } else {
            typeName = vs.toString();
        }

        return switch (typeName.toUpperCase().replace(" ", "_")) {
            case "INNER" -> SqlBuilder.JoinType.INNER;
            case "LEFT", "LEFT_OUTER" -> SqlBuilder.JoinType.LEFT;
            case "RIGHT", "RIGHT_OUTER" -> SqlBuilder.JoinType.RIGHT;
            case "FULL", "FULL_OUTER" -> SqlBuilder.JoinType.FULL;
            case "CROSS" -> SqlBuilder.JoinType.CROSS;
            default -> SqlBuilder.JoinType.INNER;
        };
    }

    /**
     * Compiles a join condition lambda where $l refers to left (t0) and $r refers
     * to right (t1).
     */
    private String compileJoinCondition(ValueSpecification vs, String leftParam, String rightParam,
            SqlBuilder left, SqlBuilder right) {
        return switch (vs) {
            case AppliedProperty ap -> {
                // Property access — determine if from left or right
                String owner = "";
                if (ap.parameters() != null && !ap.parameters().isEmpty()
                        && ap.parameters().get(0) instanceof Variable v) {
                    owner = v.name();
                }
                String colName = ap.property();
                // Look up column alias from the source builder's select columns
                String resolved = resolveJoinColumn(colName, owner, leftParam, rightParam, left, right);
                yield resolved;
            }
            case AppliedFunction af -> {
                String funcName = simpleName(af.function());
                var params = af.parameters();
                yield switch (funcName) {
                    case "equal" -> compileJoinCondition(params.get(0), leftParam, rightParam, left, right)
                            + " = " + compileJoinCondition(params.get(1), leftParam, rightParam, left, right);
                    case "greaterThan" -> compileJoinCondition(params.get(0), leftParam, rightParam, left, right)
                            + " > " + compileJoinCondition(params.get(1), leftParam, rightParam, left, right);
                    case "lessThan" -> compileJoinCondition(params.get(0), leftParam, rightParam, left, right)
                            + " < " + compileJoinCondition(params.get(1), leftParam, rightParam, left, right);
                    case "and" -> "(" + compileJoinCondition(params.get(0), leftParam, rightParam, left, right)
                            + " AND " + compileJoinCondition(params.get(1), leftParam, rightParam, left, right) + ")";
                    case "or" -> "(" + compileJoinCondition(params.get(0), leftParam, rightParam, left, right)
                            + " OR " + compileJoinCondition(params.get(1), leftParam, rightParam, left, right) + ")";
                    case "not" -> "NOT " + compileJoinCondition(params.get(0), leftParam, rightParam, left, right);
                    default -> compileScalarFunction(af, leftParam, null);
                };
            }
            case Variable v -> {
                // Skip — the variable itself carries no SQL
                yield "";
            }
            case CString s -> dialect.quoteStringLiteral(s.value());
            case CInteger i -> String.valueOf(i.value());
            default -> compileScalar(vs, leftParam, null);
        };
    }

    /**
     * Resolves a column reference in a join condition.
     * Maps property name to the correct alias (t0 for left, t1 for right).
     */
    private String resolveJoinColumn(String colName, String owner, String leftParam, String rightParam,
            SqlBuilder left, SqlBuilder right) {
        // Determine which side this belongs to
        String tableAlias;
        SqlBuilder source;
        if (owner.equals(leftParam)) {
            tableAlias = "left_src";
            source = left;
        } else if (owner.equals(rightParam)) {
            tableAlias = "right_src";
            source = right;
        } else {
            // Default to quoted column name
            return dialect.quoteIdentifier(colName);
        }

        // Look up the column name in the source's select columns
        // If the source has explicit SELECT columns, the alias is what matters
        if (source.hasSelectColumns()) {
            for (var sc : source.getSelectColumns()) {
                String alias = sc.alias() != null ? unquote(sc.alias()) : null;
                if (colName.equals(alias)) {
                    return dialect.quoteIdentifier(tableAlias) + "." + dialect.quoteIdentifier(colName);
                }
            }
        }

        return dialect.quoteIdentifier(tableAlias) + "." + dialect.quoteIdentifier(colName);
    }

    private String unquote(String s) {
        if (s != null && s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    // ========== from ==========

    private SqlBuilder compileFrom(AppliedFunction af, RelationalMapping mapping) {
        // from() is a runtime binding — pass through source
        return compileRelation(af.parameters().get(0), mapping);
    }

    // ========== Scalar Expression Compilation ==========

    String compileScalar(ValueSpecification vs, String rowParam, RelationalMapping mapping) {
        return switch (vs) {
            case AppliedProperty ap -> {
                String colName = ap.property();
                if (mapping != null) {
                    var columnOpt = mapping.getColumnForProperty(colName);
                    if (columnOpt.isPresent())
                        colName = columnOpt.get();
                }
                yield dialect.quoteIdentifier(colName);
            }
            case AppliedFunction af -> compileScalarFunction(af, rowParam, mapping);
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
                if (v.name().equals(rowParam))
                    yield "";
                yield dialect.quoteIdentifier(v.name());
            }
            case ClassInstance ci -> {
                if (ci.value() instanceof ColSpec cs)
                    yield dialect.quoteIdentifier(cs.name());
                throw new PureCompileException("SqlCompiler: unsupported ClassInstance: " + ci.type());
            }
            case EnumValue ev -> dialect.quoteStringLiteral(ev.value());
            case Collection coll -> coll.values().stream()
                    .map(v -> compileScalar(v, rowParam, mapping))
                    .collect(Collectors.joining(", "));
            default -> throw new PureCompileException(
                    "SqlCompiler: unsupported scalar: " + vs.getClass().getSimpleName());
        };
    }

    /** 4-arg version: generates scalar with table alias prefix. */
    String compileScalar(ValueSpecification vs, String rowParam, RelationalMapping mapping, String tableAlias) {
        return switch (vs) {
            case AppliedProperty ap -> {
                // Check for association path: $p.addresses.street
                if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof AppliedProperty) {
                    // Multi-hop: nested AppliedProperty = association navigation
                    List<String> path = extractPropertyChain(ap);
                    if (path.size() > 1 && modelContext != null) {
                        String assocProp = path.get(0);
                        String leafProp = path.getLast();
                        var assocResult = resolveAssociationTarget(assocProp, mapping);
                        String targetCol = leafProp;
                        var pmOpt = assocResult.mapping().getPropertyMapping(leafProp);
                        if (pmOpt.isPresent())
                            targetCol = pmOpt.get().columnName();
                        // Return the association-qualified column (EXISTS will be built by caller)
                        yield "ASSOC:" + assocProp + ":" + targetCol;
                    }
                }
                String colName = ap.property();
                if (mapping != null) {
                    var columnOpt = mapping.getColumnForProperty(colName);
                    if (columnOpt.isPresent())
                        colName = columnOpt.get();
                }
                yield dialect.quoteIdentifier(tableAlias) + "." + dialect.quoteIdentifier(colName);
            }
            case AppliedFunction af -> compileScalarFunctionWithAlias(af, rowParam, mapping, tableAlias);
            case Variable v -> {
                if (v.name().equals(rowParam))
                    yield "";
                yield dialect.quoteIdentifier(v.name());
            }
            default -> compileScalar(vs, rowParam, mapping);
        };
    }

    /**
     * Compiles any scalar function with table alias prefixed on all property
     * accesses.
     */
    private String compileScalarFunctionWithAlias(AppliedFunction af, String rowParam, RelationalMapping mapping,
            String tableAlias) {
        String funcName = simpleName(af.function());
        List<ValueSpecification> params = af.parameters();

        // Helper: recursively compile with alias
        java.util.function.Function<ValueSpecification, String> c = v -> compileScalar(v, rowParam, mapping,
                tableAlias);

        return switch (funcName) {
            // --- Comparison (may produce EXISTS for association paths) ---
            case "equal" -> buildComparison(params, "=", c, mapping, tableAlias);
            case "greaterThan" -> buildComparison(params, ">", c, mapping, tableAlias);
            case "greaterThanEqual" -> buildComparison(params, ">=", c, mapping, tableAlias);
            case "lessThan" -> buildComparison(params, "<", c, mapping, tableAlias);
            case "lessThanEqual" -> buildComparison(params, "<=", c, mapping, tableAlias);
            case "notEqual" -> buildComparison(params, "<>", c, mapping, tableAlias);
            // --- Logical ---
            case "and" -> "(" + c.apply(params.get(0)) + " AND " + c.apply(params.get(1)) + ")";
            case "or" -> "(" + c.apply(params.get(0)) + " OR " + c.apply(params.get(1)) + ")";
            case "not" -> "NOT(" + c.apply(params.get(0)) + ")";
            // --- Arithmetic ---
            case "plus" -> c.apply(params.get(0)) + " + " + c.apply(params.get(1));
            case "minus" -> c.apply(params.get(0)) + " - " + c.apply(params.get(1));
            case "times" -> c.apply(params.get(0)) + " * " + c.apply(params.get(1));
            case "divide" -> c.apply(params.get(0)) + " / " + c.apply(params.get(1));
            case "rem" -> c.apply(params.get(0)) + " % " + c.apply(params.get(1));
            // --- String ---
            case "contains" -> "LIST_CONTAINS(" + c.apply(params.get(0))
                    + ", " + c.apply(params.get(1)) + ")";
            case "startsWith" -> "STARTS_WITH(" + c.apply(params.get(0))
                    + ", " + c.apply(params.get(1)) + ")";
            case "endsWith" -> c.apply(params.get(0))
                    + " LIKE '%' || " + c.apply(params.get(1));
            case "toLower" -> "LOWER(" + c.apply(params.get(0)) + ")";
            case "toUpper" -> "UPPER(" + c.apply(params.get(0)) + ")";
            case "length" -> "LENGTH(" + c.apply(params.get(0)) + ")";
            case "trim" -> "TRIM(" + c.apply(params.get(0)) + ")";
            case "toString" -> "CAST(" + c.apply(params.get(0)) + " AS VARCHAR)";
            case "substring" -> {
                String str = c.apply(params.get(0));
                String start = c.apply(params.get(1));
                if (params.size() > 2) {
                    yield "SUBSTRING(" + str + ", " + start + ", " + c.apply(params.get(2)) + ")";
                }
                yield "SUBSTRING(" + str + ", " + start + ")";
            }
            case "indexOf" -> "POSITION(" + c.apply(params.get(1)) + " IN " + c.apply(params.get(0)) + ")";
            case "replace" -> "REPLACE(" + c.apply(params.get(0)) + ", " + c.apply(params.get(1)) + ", "
                    + c.apply(params.get(2)) + ")";
            // --- Null checks ---
            case "isEmpty" -> c.apply(params.get(0)) + " IS NULL";
            case "isNotEmpty" -> c.apply(params.get(0)) + " IS NOT NULL";
            // --- Math ---
            case "abs" -> "ABS(" + c.apply(params.get(0)) + ")";
            case "ceiling", "ceil" -> "CEIL(" + c.apply(params.get(0)) + ")";
            case "floor" -> "FLOOR(" + c.apply(params.get(0)) + ")";
            case "round" -> {
                if (params.size() > 1) {
                    yield "ROUND(" + c.apply(params.get(0)) + ", " + c.apply(params.get(1)) + ")";
                }
                yield "ROUND(" + c.apply(params.get(0)) + ")";
            }
            case "sqrt" -> "SQRT(" + c.apply(params.get(0)) + ")";
            case "pow", "power" -> "POWER(" + c.apply(params.get(0)) + ", " + c.apply(params.get(1)) + ")";
            case "log" -> "LOG(" + c.apply(params.get(0)) + ")";
            case "exp" -> "EXP(" + c.apply(params.get(0)) + ")";
            // --- Cast ---
            case "toInteger", "parseInteger" -> "CAST(" + c.apply(params.get(0)) + " AS INTEGER)";
            case "toFloat", "parseFloat" -> "CAST(" + c.apply(params.get(0)) + " AS DOUBLE)";
            case "toDecimal", "parseDecimal" -> "CAST(" + c.apply(params.get(0)) + " AS DECIMAL)";
            // --- If/Case ---
            case "if" -> {
                String cond = c.apply(params.get(0));
                LambdaFunction thenLambda = (LambdaFunction) params.get(1);
                LambdaFunction elseLambda = (LambdaFunction) params.get(2);
                String thenVal = compileScalar(thenLambda.body().get(0), rowParam, mapping, tableAlias);
                String elseVal = compileScalar(elseLambda.body().get(0), rowParam, mapping, tableAlias);
                yield "CASE WHEN " + cond + " THEN " + thenVal + " ELSE " + elseVal + " END";
            }
            // --- In ---
            case "in" -> {
                String left = c.apply(params.get(0));
                if (params.get(1) instanceof Collection coll) {
                    String vals = coll.values().stream().map(c).collect(Collectors.joining(", "));
                    yield "LIST_CONTAINS([" + vals + "], " + left + ")";
                }
                yield left + " IN (" + c.apply(params.get(1)) + ")";
            }
            // --- Coalesce ---
            case "coalesce" -> "COALESCE(" + params.stream().map(c).collect(Collectors.joining(", ")) + ")";
            default -> throw new PureCompileException(
                    "SqlCompiler: unsupported scalar function '" + funcName + "'");
        };
    }

    /**
     * Builds a comparison, generating EXISTS subquery for association paths.
     */
    private String buildComparison(List<ValueSpecification> params, String op,
            java.util.function.Function<ValueSpecification, String> c,
            RelationalMapping mapping, String tableAlias) {
        String left = c.apply(params.get(0));
        String right = c.apply(params.get(1));

        if (left.startsWith("ASSOC:") && modelContext != null && mapping != null) {
            String[] parts = left.split(":", 3);
            String assocProp = parts[1];
            String targetCol = parts[2];

            var assocResult = resolveAssociationTarget(assocProp, mapping);
            String subAlias = "sub" + (tableAliasCounter++);
            String targetTableName = assocResult.mapping().table().name();
            Join join = assocResult.join();

            if (join != null) {
                String leftJoinCol = join.getColumnForTable(mapping.table().name());
                String rightJoinCol = join.getColumnForTable(targetTableName);

                String correlation = dialect.quoteIdentifier(subAlias) + "."
                        + dialect.quoteIdentifier(rightJoinCol) + " = "
                        + dialect.quoteIdentifier(tableAlias) + "."
                        + dialect.quoteIdentifier(leftJoinCol);
                String filter = dialect.quoteIdentifier(subAlias) + "."
                        + dialect.quoteIdentifier(targetCol) + " " + op + " " + right;
                return "EXISTS (SELECT 1 FROM " + dialect.quoteIdentifier(targetTableName)
                        + " AS " + dialect.quoteIdentifier(subAlias)
                        + " WHERE (" + correlation + " AND " + filter + "))";
            }
        }

        return left + " " + op + " " + right;
    }

    private String compileScalarFunction(AppliedFunction af, String rowParam, RelationalMapping mapping) {
        String funcName = simpleName(af.function());
        List<ValueSpecification> params = af.parameters();

        return switch (funcName) {
            // --- Comparison ---
            case "equal" -> binaryOp(params, "=", rowParam, mapping);
            case "greaterThan" -> binaryOp(params, ">", rowParam, mapping);
            case "greaterThanEqual" -> binaryOp(params, ">=", rowParam, mapping);
            case "lessThan" -> binaryOp(params, "<", rowParam, mapping);
            case "lessThanEqual" -> binaryOp(params, "<=", rowParam, mapping);
            case "notEqual" -> binaryOp(params, "<>", rowParam, mapping);

            // --- Logical ---
            case "and" -> "(" + binaryOp(params, "AND", rowParam, mapping) + ")";
            case "or" -> "(" + binaryOp(params, "OR", rowParam, mapping) + ")";
            case "not" -> "NOT (" + compileScalar(params.get(0), rowParam, mapping) + ")";

            // --- Arithmetic ---
            case "plus" -> binaryOp(params, "+", rowParam, mapping);
            case "minus" -> binaryOp(params, "-", rowParam, mapping);
            case "times" -> binaryOp(params, "*", rowParam, mapping);
            case "divide" -> binaryOp(params, "/", rowParam, mapping);
            case "rem" -> binaryOp(params, "%", rowParam, mapping);

            // --- String ---
            case "contains" -> "LIST_CONTAINS(" + compileScalar(params.get(0), rowParam, mapping)
                    + ", " + compileScalar(params.get(1), rowParam, mapping) + ")";
            case "startsWith" -> "STARTS_WITH(" + compileScalar(params.get(0), rowParam, mapping)
                    + ", " + compileScalar(params.get(1), rowParam, mapping) + ")";
            case "endsWith" -> compileScalar(params.get(0), rowParam, mapping)
                    + " LIKE '%' || " + compileScalar(params.get(1), rowParam, mapping);
            case "toLower" -> "LOWER(" + compileScalar(params.get(0), rowParam, mapping) + ")";
            case "toUpper" -> "UPPER(" + compileScalar(params.get(0), rowParam, mapping) + ")";
            case "length" -> "LENGTH(" + compileScalar(params.get(0), rowParam, mapping) + ")";
            case "trim" -> "TRIM(" + compileScalar(params.get(0), rowParam, mapping) + ")";
            case "toString" -> "CAST(" + compileScalar(params.get(0), rowParam, mapping) + " AS VARCHAR)";
            case "substring" -> {
                String str = compileScalar(params.get(0), rowParam, mapping);
                String start = compileScalar(params.get(1), rowParam, mapping);
                if (params.size() > 2) {
                    yield "SUBSTRING(" + str + ", " + start + ", "
                            + compileScalar(params.get(2), rowParam, mapping) + ")";
                }
                yield "SUBSTRING(" + str + ", " + start + ")";
            }
            case "indexOf" -> "POSITION(" + compileScalar(params.get(1), rowParam, mapping)
                    + " IN " + compileScalar(params.get(0), rowParam, mapping) + ")";
            case "replace" -> "REPLACE(" + compileScalar(params.get(0), rowParam, mapping) + ", "
                    + compileScalar(params.get(1), rowParam, mapping) + ", "
                    + compileScalar(params.get(2), rowParam, mapping) + ")";

            // --- Null checks ---
            case "isEmpty" -> compileScalar(params.get(0), rowParam, mapping) + " IS NULL";
            case "isNotEmpty" -> compileScalar(params.get(0), rowParam, mapping) + " IS NOT NULL";

            // --- Math ---
            case "abs" -> "ABS(" + compileScalar(params.get(0), rowParam, mapping) + ")";
            case "ceiling", "ceil" -> "CEIL(" + compileScalar(params.get(0), rowParam, mapping) + ")";
            case "floor" -> "FLOOR(" + compileScalar(params.get(0), rowParam, mapping) + ")";
            case "round" -> {
                if (params.size() > 1) {
                    yield "ROUND(" + compileScalar(params.get(0), rowParam, mapping) + ", "
                            + compileScalar(params.get(1), rowParam, mapping) + ")";
                }
                yield "ROUND(" + compileScalar(params.get(0), rowParam, mapping) + ")";
            }
            case "sqrt" -> "SQRT(" + compileScalar(params.get(0), rowParam, mapping) + ")";
            case "pow", "power" -> "POWER(" + compileScalar(params.get(0), rowParam, mapping) + ", "
                    + compileScalar(params.get(1), rowParam, mapping) + ")";
            case "log" -> "LOG(" + compileScalar(params.get(0), rowParam, mapping) + ")";
            case "exp" -> "EXP(" + compileScalar(params.get(0), rowParam, mapping) + ")";

            // --- Cast ---
            case "toInteger", "parseInteger" -> "CAST("
                    + compileScalar(params.get(0), rowParam, mapping) + " AS INTEGER)";
            case "toFloat", "parseFloat" -> "CAST("
                    + compileScalar(params.get(0), rowParam, mapping) + " AS DOUBLE)";
            case "toDecimal", "parseDecimal" -> "CAST("
                    + compileScalar(params.get(0), rowParam, mapping) + " AS DECIMAL)";

            // --- If/Case ---
            case "if" -> {
                String cond = compileScalar(params.get(0), rowParam, mapping);
                LambdaFunction thenLambda = (LambdaFunction) params.get(1);
                LambdaFunction elseLambda = (LambdaFunction) params.get(2);
                String thenVal = compileScalar(thenLambda.body().get(0), rowParam, mapping);
                String elseVal = compileScalar(elseLambda.body().get(0), rowParam, mapping);
                yield "CASE WHEN " + cond + " THEN " + thenVal + " ELSE " + elseVal + " END";
            }

            // --- In ---
            case "in" -> {
                String left = compileScalar(params.get(0), rowParam, mapping);
                if (params.get(1) instanceof Collection coll) {
                    String vals = coll.values().stream()
                            .map(v -> compileScalar(v, rowParam, mapping))
                            .collect(Collectors.joining(", "));
                    yield left + " IN (" + vals + ")";
                }
                yield left + " IN (" + compileScalar(params.get(1), rowParam, mapping) + ")";
            }

            // --- Coalesce ---
            case "coalesce" -> "COALESCE(" + params.stream()
                    .map(v -> compileScalar(v, rowParam, mapping))
                    .collect(Collectors.joining(", ")) + ")";

            // --- Date adjust ---
            case "adjust" -> {
                // adjust(date, amount, DurationUnit.DAYS) → date + (INTERVAL '1' DAY * amount)
                String dateExpr = compileScalar(params.get(0), rowParam, mapping);
                String amount = compileScalar(params.get(1), rowParam, mapping);
                // Third param is an EnumValue like DurationUnit.DAYS
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
                yield "(" + dateExpr + " + (INTERVAL '1' " + unit + " * " + amount + "))";
            }

            default -> throw new PureCompileException(
                    "SqlCompiler: unsupported scalar function '" + funcName + "'");
        };
    }

    // ========== Window Function Helpers ==========

    private String extractWindowFunction(ColSpec cs) {
        // Pattern 1: Aggregate window with function2 = aggregate lambda
        // e.g., ~runningSum:{p,w,r|$r.salary}:y|$y->plus()
        // function1 = {p,w,r|$r.salary} → column selector → extracts "salary"
        // function2 = {y|$y->plus()} → aggregate → maps plus→SUM
        if (cs.function2() != null) {
            String column = extractColumnFromWindowMapper(cs.function1());
            String aggFunc = extractAggFuncFromLambda(cs.function2());
            if (column != null && aggFunc != null) {
                return aggFunc + "(" + dialect.quoteIdentifier(column) + ")";
            }
        }

        // Pattern 2: Ranking/value function in function1
        // e.g., ~cumeDist:{p,w,r|$p->cumulativeDistribution($w,$r)}
        if (cs.function1() != null) {
            var body = cs.function1().body();
            if (!body.isEmpty() && body.get(0) instanceof AppliedFunction af) {
                String funcName = simpleName(af.function());

                // Check for aggregate body WITH property access:
                // {p,w,r|$p->sum($w,$r).salary}
                if (body.get(0) instanceof AppliedProperty ap) {
                    // Property access on a function call result
                    String propName = ap.property();
                    if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof AppliedFunction innerAf) {
                        String innerFunc = simpleName(innerAf.function());
                        String sqlAgg = mapAggregateFunction(innerFunc);
                        if (sqlAgg != null) {
                            return sqlAgg + "(" + dialect.quoteIdentifier(propName) + ")";
                        }
                    }
                }

                // Check for post-processor wrapping a window function:
                // e.g., round(cumulativeDistribution($w,$r), 2) → ROUND(CUME_DIST() OVER(...),
                // 2)
                if (isPostProcessor(funcName) && !af.parameters().isEmpty()
                        && af.parameters().get(0) instanceof AppliedFunction innerAf) {
                    String innerFuncName = simpleName(innerAf.function());
                    String innerWindowFunc = switch (innerFuncName) {
                        case "cumulativeDistribution" -> "CUME_DIST()";
                        case "percentRank" -> "PERCENT_RANK()";
                        case "rank" -> "RANK()";
                        case "denseRank" -> "DENSE_RANK()";
                        case "rowNumber" -> "ROW_NUMBER()";
                        default -> null;
                    };
                    if (innerWindowFunc != null) {
                        // Build the wrapper: ROUND(CUME_DIST() OVER(...), 2)
                        // Return with WRAPPED: prefix so caller can split
                        String args = "";
                        for (int i = 1; i < af.parameters().size(); i++) {
                            args += ", " + compileScalar(af.parameters().get(i), "", null);
                        }
                        return "WRAPPED:" + funcName.toUpperCase() + "(" + innerWindowFunc + args + ")";
                    }
                }

                return switch (funcName) {
                    case "rowNumber" -> "ROW_NUMBER()";
                    case "rank" -> "RANK()";
                    case "denseRank" -> "DENSE_RANK()";
                    case "cumulativeDistribution" -> "CUME_DIST()";
                    case "percentRank" -> "PERCENT_RANK()";
                    case "first", "firstValue" -> "FIRST_VALUE(" + extractWindowFuncArg(af) + ")";
                    case "last", "lastValue" -> "LAST_VALUE(" + extractWindowFuncArg(af) + ")";
                    case "ntile" -> {
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
                    case "lag" -> "LAG(" + extractWindowFuncArg(af) + ")";
                    case "lead" -> "LEAD(" + extractWindowFuncArg(af) + ")";
                    default -> funcName.toUpperCase() + "()";
                };
            }

            // Check for property access pattern: {p,w,r|$p->avg($w,$r).salary}
            // or {p,w,r|$p->first($w,$r).salary}
            if (!body.isEmpty() && body.get(0) instanceof AppliedProperty ap) {
                String propName = ap.property();
                if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof AppliedFunction innerAf) {
                    String innerFunc = simpleName(innerAf.function());
                    // Check if it's an aggregate
                    String sqlAgg = mapAggregateFunction(innerFunc);
                    if (sqlAgg != null) {
                        return sqlAgg + "(" + dialect.quoteIdentifier(propName) + ")";
                    }
                    // Check if it's a value function (first, last, lag, lead)
                    return switch (innerFunc) {
                        case "first", "firstValue" -> "FIRST_VALUE(" + dialect.quoteIdentifier(propName) + ")";
                        case "last", "lastValue" -> "LAST_VALUE(" + dialect.quoteIdentifier(propName) + ")";
                        case "lag" -> "LAG(" + dialect.quoteIdentifier(propName) + ")";
                        case "lead" -> "LEAD(" + dialect.quoteIdentifier(propName) + ")";
                        default -> "ROW_NUMBER()";
                    };
                }
            }
        }
        return "ROW_NUMBER()";
    }

    /**
     * Returns true if the function is a post-processor that can wrap a window
     * function.
     */
    private boolean isPostProcessor(String funcName) {
        return switch (funcName) {
            case "round", "abs", "ceiling", "ceil", "floor", "toString" -> true;
            default -> false;
        };
    }

    /** Maps Pure aggregate function name to SQL aggregate function. */
    private String mapAggregateFunction(String funcName) {
        return switch (funcName) {
            case "plus", "sum" -> "SUM";
            case "average", "avg" -> "AVG";
            case "count", "size" -> "COUNT";
            case "min" -> "MIN";
            case "max" -> "MAX";
            case "stdDev", "stddev" -> "STDDEV";
            case "variance" -> "VARIANCE";
            default -> null;
        };
    }

    /** Extracts column name from a window mapper lambda like {p,w,r|$r.salary}. */
    private String extractColumnFromWindowMapper(LambdaFunction lf) {
        if (lf == null || lf.body().isEmpty())
            return null;
        var body = lf.body().get(0);
        if (body instanceof AppliedProperty ap) {
            return ap.property();
        }
        return null;
    }

    /** Extracts SQL aggregate function name from a lambda like {y|$y->plus()}. */
    private String extractAggFuncFromLambda(LambdaFunction lf) {
        if (lf == null || lf.body().isEmpty())
            return null;
        var body = lf.body().get(0);
        if (body instanceof AppliedFunction af) {
            return mapAggregateFunction(simpleName(af.function()));
        }
        return null;
    }

    private String extractWindowFuncArg(AppliedFunction af) {
        if (af.parameters().size() > 1) {
            return dialect.quoteIdentifier(extractColumnNameFromParam(af.parameters().get(1)));
        }
        return "*";
    }

    private String generateOverClause(AppliedFunction overSpec) {
        List<String> partitionCols = new ArrayList<>();
        List<String> orderParts = new ArrayList<>();
        String frameClause = null;

        for (var p : overSpec.parameters()) {
            if (p instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
                partitionCols.add(dialect.quoteIdentifier(cs.name()));
            } else if (p instanceof AppliedFunction paf) {
                String funcName = simpleName(paf.function());
                if ("asc".equals(funcName) || "ascending".equals(funcName)
                        || "desc".equals(funcName) || "descending".equals(funcName)) {
                    String col = extractColumnNameFromParam(paf.parameters().get(0));
                    String dir = ("asc".equals(funcName) || "ascending".equals(funcName)) ? "ASC" : "DESC";
                    // Match old pipeline: DESC → NULLS FIRST, ASC → NULLS LAST
                    String nullOrder = "DESC".equals(dir) ? "NULLS FIRST" : "NULLS LAST";
                    orderParts.add(dialect.quoteIdentifier(col) + " " + dir + " " + nullOrder);
                } else if ("rows".equals(funcName)) {
                    frameClause = "ROWS BETWEEN " + formatFrameBound(paf.parameters(), true)
                            + " AND " + formatFrameBound(paf.parameters(), false);
                } else if ("range".equals(funcName) || "_range".equals(funcName)) {
                    frameClause = "RANGE BETWEEN " + formatFrameBound(paf.parameters(), true)
                            + " AND " + formatFrameBound(paf.parameters(), false);
                }
            }
        }

        StringBuilder sb = new StringBuilder();
        if (!partitionCols.isEmpty()) {
            sb.append("PARTITION BY ").append(String.join(", ", partitionCols));
        }
        if (!orderParts.isEmpty()) {
            if (!sb.isEmpty())
                sb.append(" ");
            sb.append("ORDER BY ").append(String.join(", ", orderParts));
        }
        if (frameClause != null) {
            if (!sb.isEmpty())
                sb.append(" ");
            sb.append(frameClause);
        }
        return sb.toString();
    }

    /** Formats a frame bound from the rows()/range() parameters. */
    private String formatFrameBound(List<ValueSpecification> params, boolean isStart) {
        int idx = isStart ? 0 : 1;
        if (idx >= params.size())
            return isStart ? "UNBOUNDED PRECEDING" : "CURRENT ROW";
        var param = params.get(idx);
        if (param instanceof AppliedFunction af && "unbounded".equals(simpleName(af.function()))) {
            return isStart ? "UNBOUNDED PRECEDING" : "UNBOUNDED FOLLOWING";
        }
        // Handle negative numbers: AppliedFunction("minus", [CInteger(n)]) → n
        // PRECEDING
        if (param instanceof AppliedFunction af && "minus".equals(simpleName(af.function()))) {
            if (!af.parameters().isEmpty()) {
                try {
                    long v = extractIntValue(af.parameters().get(af.parameters().size() - 1));
                    return v + " PRECEDING";
                } catch (PureCompileException e) {
                    /* fall through */ }
            }
        }
        // Try extracting as integer value (handles CInteger, CFloat, etc.)
        try {
            long v = extractIntValue(param);
            if (v == 0)
                return "CURRENT ROW";
            if (v < 0)
                return Math.abs(v) + " PRECEDING";
            return v + " FOLLOWING";
        } catch (PureCompileException e) {
            // Not a simple integer, fall through
        }
        return isStart ? "CURRENT ROW" : "CURRENT ROW";
    }

    // ========== Aggregate Helpers ==========

    private String extractAggFunction(LambdaFunction lambda) {
        if (lambda.body().isEmpty())
            return "SUM";
        ValueSpecification body = lambda.body().get(0);
        if (body instanceof AppliedFunction af) {
            String funcName = simpleName(af.function());
            return switch (funcName) {
                case "sum", "plus" -> "SUM";
                case "count" -> "COUNT";
                case "average", "avg", "mean" -> "AVG";
                case "min" -> "MIN";
                case "max" -> "MAX";
                default -> funcName.toUpperCase();
            };
        }
        return "SUM";
    }

    private String extractPropertyOrColumnName(ValueSpecification vs) {
        if (vs instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            ValueSpecification body = lf.body().get(0);
            if (body instanceof AppliedProperty ap)
                return ap.property();
        }
        if (vs instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
            return cs.name();
        }
        if (vs instanceof AppliedFunction af) {
            if (!af.parameters().isEmpty()) {
                return extractPropertyOrColumnName(af.parameters().get(0));
            }
        }
        return null;
    }

    // ========== Source Helpers ==========

    private String resolveTableNameFromSource(ValueSpecification source, RelationalMapping mapping) {
        if (mapping != null)
            return mapping.table().name();
        if (source instanceof AppliedFunction af) {
            String funcName = simpleName(af.function());
            if ("getAll".equals(funcName) && !af.parameters().isEmpty()) {
                return extractClassName(af.parameters().get(0));
            } else if (!af.parameters().isEmpty()) {
                return resolveTableNameFromSource(af.parameters().get(0), mapping);
            }
        }
        return "UNKNOWN";
    }

    private String extractFilterClause(ValueSpecification source, String tableAlias, RelationalMapping mapping) {
        if (source instanceof AppliedFunction af) {
            String funcName = simpleName(af.function());
            if ("filter".equals(funcName) && af.parameters().size() >= 2) {
                LambdaFunction lambda = (LambdaFunction) af.parameters().get(1);
                String paramName = lambda.parameters().isEmpty() ? "x" : lambda.parameters().get(0).name();
                return compileScalar(lambda.body().get(0), paramName, mapping, tableAlias);
            }
        }
        return null;
    }

    // ========== Utility Methods ==========

    private String binaryOp(List<ValueSpecification> params, String op, String rowParam,
            RelationalMapping mapping) {
        return compileScalar(params.get(0), rowParam, mapping) + " " + op + " "
                + compileScalar(params.get(1), rowParam, mapping);
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

    private String extractTableName(String content) {
        int dotIdx = content.lastIndexOf('.');
        return dotIdx > 0 ? content.substring(dotIdx + 1) : content;
    }

    private String extractPropertyNameFromLambda(ValueSpecification vs) {
        if (vs instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            ValueSpecification body = lf.body().get(0);
            if (body instanceof AppliedProperty ap)
                return ap.property();
        }
        return null;
    }

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
