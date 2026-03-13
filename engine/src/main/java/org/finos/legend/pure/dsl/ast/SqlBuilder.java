package org.finos.legend.pure.dsl.ast;

import org.finos.legend.engine.transpiler.SQLDialect;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Structured SQL AST — models a SELECT statement as data, not strings.
 *
 * <p>
 * Inspired by legend-engine's {@code SelectSQLQuery}: a mutable SQL object
 * that gets built up during compilation, then serialized via
 * {@link #toSql(SQLDialect)}.
 *
 * <p>
 * Supports the union of all SELECT clause features across DuckDB, Snowflake,
 * Databricks, BigQuery, and Postgres.
 *
 * <h3>Design principle: minimize subquery wrapping</h3>
 * Only wrap in subquery when structurally necessary (e.g., applying ORDER BY
 * to a projection that has a WHERE clause already built into it).
 */
public class SqlBuilder {

    // ──── WITH (CTE) ────
    private final List<CTE> ctes = new ArrayList<>();
    private boolean recursive = false;

    // ──── SELECT ────
    private final List<SelectColumn> selectColumns = new ArrayList<>();
    private boolean selectStar = false;
    private final List<String> starExcept = new ArrayList<>(); // EXCEPT(col)
    private final List<SelectColumn> starReplace = new ArrayList<>(); // REPLACE(expr AS col)
    private boolean distinct = false;
    private final List<String> distinctOn = new ArrayList<>(); // DISTINCT ON

    // ──── FROM ────
    private String fromTable;
    private String fromAlias;
    private String fromSchema;
    private SqlBuilder fromSubquery;
    private ValuesClause fromValues;

    // ──── JOIN ────
    private final List<JoinClause> joins = new ArrayList<>();

    // ──── WHERE ────
    private final List<SqlExpr> whereConditions = new ArrayList<>();

    // ──── GROUP BY ────
    private final List<SqlExpr> groupByColumns = new ArrayList<>();
    private boolean groupByAll = false;

    // ──── HAVING ────
    private final List<SqlExpr> havingConditions = new ArrayList<>();

    // ──── WINDOW ────
    private final List<WindowColumn> windowColumns = new ArrayList<>();
    private final List<NamedWindow> namedWindows = new ArrayList<>();

    // ──── QUALIFY ────
    private SqlExpr qualifyCondition;

    // ──── ORDER BY ────
    private final List<OrderByColumn> orderByColumns = new ArrayList<>();

    // ──── LIMIT / OFFSET ────
    private Integer limit;
    private Integer offset;

    // ──── SET OPERATIONS ────
    private SetOperation setOperation;

    // ──── PIVOT ────
    private PivotClause pivotClause;

    // ========== Builder Methods ==========

    // --- CTE ---
    public SqlBuilder withCte(String name, SqlBuilder query) {
        ctes.add(new CTE(name, query));
        return this;
    }

    public SqlBuilder withRecursive(boolean recursive) {
        this.recursive = recursive;
        return this;
    }

    // --- SELECT ---
    public SqlBuilder addSelect(SqlExpr expression, String alias) {
        selectColumns.add(new SelectColumn(expression, alias));
        return this;
    }

    public SqlBuilder selectStar() {
        this.selectStar = true;
        return this;
    }

    /** Clears SELECT * so explicit columns can be added. */
    public SqlBuilder clearSelect() {
        this.selectStar = false;
        this.selectColumns.clear();
        return this;
    }

    public SqlBuilder addStarExcept(String column) {
        starExcept.add(column);
        return this;
    }

    public SqlBuilder addStarReplace(SqlExpr expression, String alias) {
        starReplace.add(new SelectColumn(expression, alias));
        return this;
    }

    public SqlBuilder distinct() {
        this.distinct = true;
        return this;
    }

    public SqlBuilder addDistinctOn(String column) {
        distinctOn.add(column);
        return this;
    }

    // --- FROM ---
    public SqlBuilder from(String table, String alias) {
        this.fromTable = table;
        this.fromAlias = alias;
        return this;
    }

    public SqlBuilder fromSchema(String schema) {
        this.fromSchema = schema;
        return this;
    }

    public SqlBuilder fromSubquery(SqlBuilder subquery, String alias) {
        this.fromSubquery = subquery;
        this.fromAlias = alias;
        return this;
    }

    /** FROM (VALUES (...), (...)) AS alias(col1, col2) */
    public SqlBuilder fromValues(List<List<SqlExpr>> rows, String alias, List<String> columnNames) {
        this.fromValues = new ValuesClause(rows, alias, columnNames);
        return this;
    }

    // --- JOIN ---
    public SqlBuilder addJoin(JoinClause join) {
        joins.add(join);
        return this;
    }

    public SqlBuilder addJoin(JoinType type, String table, String alias, SqlExpr onCondition) {
        joins.add(new JoinClause(type, table, alias, null, onCondition, null));
        return this;
    }

    // --- WHERE ---
    public SqlBuilder addWhere(SqlExpr condition) {
        whereConditions.add(condition);
        return this;
    }

    // --- GROUP BY ---
    public SqlBuilder addGroupBy(SqlExpr column) {
        groupByColumns.add(column);
        return this;
    }

    public SqlBuilder groupByAll() {
        this.groupByAll = true;
        return this;
    }

    // --- HAVING ---
    public SqlBuilder addHaving(SqlExpr condition) {
        havingConditions.add(condition);
        return this;
    }

    // --- WINDOW ---
    public SqlBuilder addWindowColumn(SqlExpr function, SqlExpr overClause, String alias) {
        windowColumns.add(new WindowColumn(function, overClause, alias));
        return this;
    }

    public SqlBuilder addNamedWindow(String name, SqlExpr spec) {
        namedWindows.add(new NamedWindow(name, spec));
        return this;
    }

    // --- QUALIFY ---
    public SqlBuilder qualify(SqlExpr condition) {
        this.qualifyCondition = condition;
        return this;
    }

    // --- ORDER BY ---
    public SqlBuilder addOrderBy(SqlExpr expression, SortDirection direction) {
        orderByColumns.add(new OrderByColumn(expression, direction, NullsPosition.DEFAULT));
        return this;
    }

    public SqlBuilder addOrderBy(SqlExpr expression, SortDirection direction, NullsPosition nulls) {
        orderByColumns.add(new OrderByColumn(expression, direction, nulls));
        return this;
    }

    // --- LIMIT / OFFSET ---
    public SqlBuilder limit(int limit) {
        this.limit = limit;
        return this;
    }

    public SqlBuilder offset(int offset) {
        this.offset = offset;
        return this;
    }

    // --- SET OPERATIONS ---
    public SqlBuilder union(SqlBuilder other, boolean all) {
        this.setOperation = new SetOperation(SetOpType.UNION, all, other, false);
        return this;
    }

    public SqlBuilder unionWrapped(SqlBuilder other, boolean all) {
        this.setOperation = new SetOperation(SetOpType.UNION, all, other, true);
        return this;
    }

    public SqlBuilder intersect(SqlBuilder other, boolean all) {
        this.setOperation = new SetOperation(SetOpType.INTERSECT, all, other, false);
        return this;
    }

    public SqlBuilder except(SqlBuilder other, boolean all) {
        this.setOperation = new SetOperation(SetOpType.EXCEPT, all, other, false);
        return this;
    }

    // --- PIVOT ---
    public SqlBuilder pivot(PivotClause clause) {
        this.pivotClause = clause;
        return this;
    }

    // ========== Query Accessors (for PlanGenerator to inspect) ==========

    public boolean hasSelectColumns() {
        return !selectColumns.isEmpty();
    }

    public boolean hasWhere() {
        return !whereConditions.isEmpty();
    }

    public boolean hasGroupBy() {
        return !groupByColumns.isEmpty() || groupByAll;
    }

    public boolean hasOrderBy() {
        return !orderByColumns.isEmpty();
    }

    public boolean hasLimit() {
        return limit != null;
    }

    public boolean hasOffset() {
        return offset != null;
    }

    public boolean hasWindowColumns() {
        return !windowColumns.isEmpty();
    }

    public boolean hasPivot() {
        return pivotClause != null;
    }

    public boolean isSelectStar() {
        return selectStar;
    }

    public String getFromTable() {
        return fromTable;
    }

    public String getFromAlias() {
        return fromAlias;
    }

    public SqlBuilder getFromSubquery() {
        return fromSubquery;
    }

    public List<SelectColumn> getSelectColumns() {
        return selectColumns;
    }

    // ========== SQL Serialization ==========

    /**
     * Serializes this SQL AST to a SQL string using the given dialect.
     *
     * <p>
     * Clause order follows the standard: WITH → SELECT → FROM → JOIN → WHERE →
     * GROUP BY → HAVING → WINDOW → QUALIFY → ORDER BY → LIMIT → OFFSET
     */
    public String toSql(SQLDialect dialect) {
        // PIVOT mode — completely different syntax from SELECT
        if (pivotClause != null) {
            return renderPivot(dialect);
        }
        StringBuilder sql = new StringBuilder();

        // WITH
        if (!ctes.isEmpty()) {
            sql.append("WITH ");
            if (recursive)
                sql.append("RECURSIVE ");
            sql.append(ctes.stream()
                    .map(cte -> cte.name() + " AS (" + cte.query().toSql(dialect) + ")")
                    .collect(Collectors.joining(", ")));
            sql.append(" ");
        }

        // SELECT
        sql.append("SELECT ");
        if (distinct) {
            sql.append("DISTINCT ");
            if (!distinctOn.isEmpty()) {
                sql.append("ON (")
                        .append(distinctOn.stream().collect(Collectors.joining(", ")))
                        .append(") ");
            }
        }

        if (selectStar && !windowColumns.isEmpty()) {
            // SELECT *, window_func() OVER (...) AS alias
            sql.append("*");
            for (var wc : windowColumns) {
                sql.append(", ").append(wc.function().toSql(dialect));
                if (wc.overClause() != null) {
                    sql.append(" OVER (").append(wc.overClause().toSql(dialect)).append(")");
                }
                sql.append(" AS ").append(wc.alias());
            }
        } else if (selectStar) {
            sql.append("*");
            if (!starExcept.isEmpty()) {
                sql.append(dialect.renderStarExcept(starExcept));
            }
            if (!starReplace.isEmpty()) {
                sql.append(" REPLACE (")
                        .append(starReplace.stream()
                                .map(sc -> sc.expression().toSql(dialect) + " AS " + sc.alias())
                                .collect(Collectors.joining(", ")))
                        .append(")");
            }
            // Additional SELECT columns after star (e.g., rename: * EXCLUDE(x), x AS y)
            if (!selectColumns.isEmpty()) {
                sql.append(", ");
                sql.append(selectColumns.stream()
                        .map(sc -> sc.alias() != null
                                ? sc.expression().toSql(dialect) + " AS " + sc.alias()
                                : sc.expression().toSql(dialect))
                        .collect(Collectors.joining(", ")));
            }
        } else if (!selectColumns.isEmpty()) {
            sql.append(selectColumns.stream()
                    .map(sc -> sc.alias() != null
                            ? sc.expression().toSql(dialect) + " AS " + sc.alias()
                            : sc.expression().toSql(dialect))
                    .collect(Collectors.joining(", ")));
        } else {
            sql.append("*");
        }

        // FROM
        if (fromValues != null) {
            sql.append(" FROM (VALUES ");
            sql.append(fromValues.rows().stream()
                    .map(row -> "(" + row.stream().map(e -> e.toSql(dialect)).collect(Collectors.joining(", ")) + ")")
                    .collect(Collectors.joining(", ")));
            sql.append(") AS ").append(fromValues.alias());
            if (!fromValues.columnNames().isEmpty()) {
                sql.append("(");
                sql.append(String.join(", ", fromValues.columnNames()));
                sql.append(")");
            }
        } else if (fromSubquery != null) {
            sql.append(" FROM (").append(fromSubquery.toSql(dialect)).append(")");
            if (fromAlias != null) {
                sql.append(" AS ").append(fromAlias);
            }
        } else if (fromTable != null) {
            sql.append(" FROM ");
            if (fromSchema != null) {
                sql.append(fromSchema).append(".");
            }
            sql.append(fromTable);
            if (fromAlias != null) {
                sql.append(" AS ").append(fromAlias);
            }
        }

        // JOIN
        for (var join : joins) {
            sql.append(" ").append(join.type().toSql()).append(" ");
            if (join.subquery() != null) {
                sql.append("(").append(join.subquery().toSql(dialect)).append(")");
            } else {
                sql.append(join.table());
            }
            if (join.alias() != null) {
                sql.append(" AS ").append(join.alias());
            }
            if (join.onCondition() != null) {
                sql.append(" ON ").append(join.onCondition().toSql(dialect));
            }
            if (join.usingColumns() != null && !join.usingColumns().isEmpty()) {
                sql.append(" USING (")
                        .append(String.join(", ", join.usingColumns()))
                        .append(")");
            }
        }

        // WHERE
        if (!whereConditions.isEmpty()) {
            sql.append(" WHERE ").append(whereConditions.stream()
                    .map(c -> c.toSql(dialect)).collect(Collectors.joining(" AND ")));
        }

        // GROUP BY
        if (groupByAll) {
            sql.append(" GROUP BY ALL");
        } else if (!groupByColumns.isEmpty()) {
            sql.append(" GROUP BY ").append(groupByColumns.stream()
                    .map(c -> c.toSql(dialect)).collect(Collectors.joining(", ")));
        }

        // HAVING
        if (!havingConditions.isEmpty()) {
            sql.append(" HAVING ").append(havingConditions.stream()
                    .map(c -> c.toSql(dialect)).collect(Collectors.joining(" AND ")));
        }

        // WINDOW (named window specs)
        if (!namedWindows.isEmpty()) {
            sql.append(" WINDOW ")
                    .append(namedWindows.stream()
                            .map(w -> w.name() + " AS (" + w.spec().toSql(dialect) + ")")
                            .collect(Collectors.joining(", ")));
        }

        // QUALIFY
        if (qualifyCondition != null) {
            sql.append(" QUALIFY ").append(qualifyCondition.toSql(dialect));
        }

        // ORDER BY
        if (!orderByColumns.isEmpty()) {
            sql.append(" ORDER BY ")
                    .append(orderByColumns.stream()
                            .map(ob -> {
                                StringBuilder obs = new StringBuilder(ob.expression().toSql(dialect));
                                if (ob.direction() != null) {
                                    obs.append(" ").append(ob.direction().name());
                                }
                                if (ob.nulls() != NullsPosition.DEFAULT) {
                                    obs.append(" NULLS ").append(ob.nulls().name());
                                }
                                return obs.toString();
                            })
                            .collect(Collectors.joining(", ")));
        }

        // LIMIT
        if (limit != null) {
            sql.append(" LIMIT ").append(limit);
        }

        // OFFSET
        if (offset != null) {
            sql.append(" OFFSET ").append(offset);
        }

        // SET OPERATIONS
        if (setOperation != null) {
            if (setOperation.wrapped()) {
                // Wrap: (left) UNION ALL (right)
                String leftSql = sql.toString();
                sql.setLength(0);
                sql.append("(").append(leftSql).append(")");
            }
            sql.append(" ").append(setOperation.type().name());
            if (setOperation.all())
                sql.append(" ALL");
            if (setOperation.wrapped()) {
                sql.append(" (").append(setOperation.other().toSql(dialect)).append(")");
            } else {
                sql.append(" ").append(setOperation.other().toSql(dialect));
            }
        }

        return sql.toString();
    }

    // ========== Supporting Types ==========

    /** SELECT column: expression AS alias */
    public record SelectColumn(SqlExpr expression, String alias) {
    }

    /** Common Table Expression */
    public record CTE(String name, SqlBuilder query) {
    }

    /** JOIN clause */
    public record JoinClause(
            JoinType type,
            String table,
            String alias,
            SqlBuilder subquery,
            SqlExpr onCondition,
            List<String> usingColumns) {
    }

    /** JOIN types — union of all dialects */
    public enum JoinType {
        INNER("INNER JOIN"),
        LEFT("LEFT OUTER JOIN"),
        RIGHT("RIGHT OUTER JOIN"),
        FULL("FULL OUTER JOIN"),
        CROSS("CROSS JOIN"),
        LEFT_SEMI("LEFT SEMI JOIN"),
        LEFT_ANTI("LEFT ANTI JOIN"),
        ASOF("ASOF JOIN"),
        ASOF_LEFT("ASOF LEFT JOIN"),
        NATURAL("NATURAL JOIN"),
        LEFT_LATERAL("LEFT JOIN LATERAL");

        private final String sql;

        JoinType(String sql) {
            this.sql = sql;
        }

        public String toSql() {
            return sql;
        }
    }

    /** ORDER BY column */
    public record OrderByColumn(SqlExpr expression, SortDirection direction, NullsPosition nulls) {
    }

    public enum SortDirection {
        ASC, DESC
    }

    public enum NullsPosition {
        FIRST, LAST, DEFAULT
    }

    /** Window function column: FUNC() OVER (...) AS alias */
    public record WindowColumn(SqlExpr function, SqlExpr overClause, String alias) {
    }

    /** Named WINDOW clause */
    public record NamedWindow(String name, SqlExpr spec) {
    }

    /** VALUES clause: FROM (VALUES (...), (...)) AS alias(col1, col2) */
    public record ValuesClause(List<List<SqlExpr>> rows, String alias, List<String> columnNames) {
    }

    /** SET operation (UNION, INTERSECT, EXCEPT) */
    public record SetOperation(SetOpType type, boolean all, SqlBuilder other, boolean wrapped) {
    }

    public enum SetOpType {
        UNION, INTERSECT, EXCEPT
    }

    /** PIVOT clause: PIVOT (source) ON col USING AGG(val) AS alias */
    public record PivotClause(
            List<String> pivotColumns,
            List<PivotAggregate> aggregates
    ) {}

    /** Aggregate spec for PIVOT: AGG(expr) AS alias */
    public record PivotAggregate(
            String aggFunction,    // e.g., "SUM"
            String valueColumn,    // column name, or null if expression
            String valueExpression, // raw SQL expression, or null if column
            String alias           // output alias suffix
    ) {}

    /**
     * Renders PIVOT SQL: PIVOT (source) ON col USING AGG(val) AS alias
     */
    private String renderPivot(SQLDialect dialect) {
        StringBuilder sb = new StringBuilder();

        // Render the source — pivot source is always the fromSubquery or fromValues
        String sourceSql;
        if (fromSubquery != null) {
            sourceSql = fromSubquery.toSql(dialect);
        } else if (fromValues != null) {
            // Inline VALUES source
            StringBuilder vs = new StringBuilder("SELECT * FROM (VALUES ");
            vs.append(fromValues.rows().stream()
                    .map(row -> "(" + row.stream().map(e -> e.toSql(dialect)).collect(Collectors.joining(", ")) + ")")
                    .collect(Collectors.joining(", ")));
            vs.append(") AS ").append(fromValues.alias());
            if (!fromValues.columnNames().isEmpty()) {
                vs.append("(").append(String.join(", ", fromValues.columnNames())).append(")");
            }
            sourceSql = vs.toString();
        } else if (fromTable != null) {
            sourceSql = "SELECT * FROM " + fromTable;
        } else {
            throw new IllegalStateException("PIVOT requires a source");
        }

        List<String> pivotCols = pivotClause.pivotColumns();
        if (pivotCols.size() > 1) {
            // Multi-column pivot: concatenate columns with '__|__'
            String excludeList = pivotCols.stream()
                    .map(dialect::quoteIdentifier)
                    .collect(Collectors.joining(", "));
            String concatExpr = pivotCols.stream()
                    .map(dialect::quoteIdentifier)
                    .reduce((a, b) -> a + " || '__|__' || " + b)
                    .orElse("");
            sourceSql = "SELECT * EXCLUDE(" + excludeList + "), " + concatExpr
                    + " AS \"_pivot_key\" FROM (" + sourceSql + ") AS _pivot_src";
            sb.append("PIVOT (").append(sourceSql).append(") ON \"_pivot_key\"");
        } else {
            sb.append("PIVOT (").append(sourceSql).append(") ON ");
            sb.append(dialect.quoteIdentifier(pivotCols.get(0)));
        }

        // USING clause
        sb.append(" USING ");
        boolean first = true;
        for (var agg : pivotClause.aggregates()) {
            if (!first) sb.append(", ");
            first = false;
            sb.append(agg.aggFunction().toUpperCase());
            sb.append("(");
            if (agg.valueColumn() != null) {
                sb.append(dialect.quoteIdentifier(agg.valueColumn()));
            } else if (agg.valueExpression() != null) {
                sb.append(agg.valueExpression());
            } else {
                sb.append("1");
            }
            sb.append(") AS ");
            sb.append(dialect.quoteIdentifier("_|__" + agg.alias()));
        }

        return sb.toString();
    }
}
