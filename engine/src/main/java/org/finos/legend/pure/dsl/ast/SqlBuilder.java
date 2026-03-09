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

    // ──── JOIN ────
    private final List<JoinClause> joins = new ArrayList<>();

    // ──── WHERE ────
    private final List<String> whereConditions = new ArrayList<>();

    // ──── GROUP BY ────
    private final List<String> groupByColumns = new ArrayList<>();
    private boolean groupByAll = false;

    // ──── HAVING ────
    private final List<String> havingConditions = new ArrayList<>();

    // ──── WINDOW ────
    private final List<WindowColumn> windowColumns = new ArrayList<>();
    private final List<NamedWindow> namedWindows = new ArrayList<>();

    // ──── QUALIFY ────
    private String qualifyCondition;

    // ──── ORDER BY ────
    private final List<OrderByColumn> orderByColumns = new ArrayList<>();

    // ──── LIMIT / OFFSET ────
    private Integer limit;
    private Integer offset;

    // ──── SET OPERATIONS ────
    private SetOperation setOperation;

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
    public SqlBuilder addSelect(String expression, String alias) {
        selectColumns.add(new SelectColumn(expression, alias));
        return this;
    }

    public SqlBuilder selectStar() {
        this.selectStar = true;
        return this;
    }

    public SqlBuilder addStarExcept(String column) {
        starExcept.add(column);
        return this;
    }

    public SqlBuilder addStarReplace(String expression, String alias) {
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

    // --- JOIN ---
    public SqlBuilder addJoin(JoinClause join) {
        joins.add(join);
        return this;
    }

    public SqlBuilder addJoin(JoinType type, String table, String alias, String onCondition) {
        joins.add(new JoinClause(type, table, alias, null, onCondition, null));
        return this;
    }

    // --- WHERE ---
    public SqlBuilder addWhere(String condition) {
        whereConditions.add(condition);
        return this;
    }

    // --- GROUP BY ---
    public SqlBuilder addGroupBy(String column) {
        groupByColumns.add(column);
        return this;
    }

    public SqlBuilder groupByAll() {
        this.groupByAll = true;
        return this;
    }

    // --- HAVING ---
    public SqlBuilder addHaving(String condition) {
        havingConditions.add(condition);
        return this;
    }

    // --- WINDOW ---
    public SqlBuilder addWindowColumn(String function, String overClause, String alias) {
        windowColumns.add(new WindowColumn(function, overClause, alias));
        return this;
    }

    public SqlBuilder addNamedWindow(String name, String spec) {
        namedWindows.add(new NamedWindow(name, spec));
        return this;
    }

    // --- QUALIFY ---
    public SqlBuilder qualify(String condition) {
        this.qualifyCondition = condition;
        return this;
    }

    // --- ORDER BY ---
    public SqlBuilder addOrderBy(String expression, SortDirection direction) {
        orderByColumns.add(new OrderByColumn(expression, direction, NullsPosition.DEFAULT));
        return this;
    }

    public SqlBuilder addOrderBy(String expression, SortDirection direction, NullsPosition nulls) {
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

    // ========== Query Accessors (for SqlCompiler to inspect) ==========

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
                sql.append(", ").append(wc.function());
                if (wc.overClause() != null) {
                    sql.append(" OVER (").append(wc.overClause()).append(")");
                }
                sql.append(" AS ").append(wc.alias());
            }
        } else if (selectStar) {
            sql.append("*");
            if (!starExcept.isEmpty()) {
                sql.append(" EXCLUDE(")
                        .append(String.join(", ", starExcept))
                        .append(")");
            }
            if (!starReplace.isEmpty()) {
                sql.append(" REPLACE (")
                        .append(starReplace.stream()
                                .map(sc -> sc.expression() + " AS " + sc.alias())
                                .collect(Collectors.joining(", ")))
                        .append(")");
            }
            // Additional SELECT columns after star (e.g., rename: * EXCLUDE(x), x AS y)
            if (!selectColumns.isEmpty()) {
                sql.append(", ");
                sql.append(selectColumns.stream()
                        .map(sc -> sc.alias() != null
                                ? sc.expression() + " AS " + sc.alias()
                                : sc.expression())
                        .collect(Collectors.joining(", ")));
            }
        } else if (!selectColumns.isEmpty()) {
            sql.append(selectColumns.stream()
                    .map(sc -> sc.alias() != null
                            ? sc.expression() + " AS " + sc.alias()
                            : sc.expression())
                    .collect(Collectors.joining(", ")));
        } else {
            sql.append("*");
        }

        // FROM
        if (fromSubquery != null) {
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
                sql.append(" ON ").append(join.onCondition());
            }
            if (join.usingColumns() != null && !join.usingColumns().isEmpty()) {
                sql.append(" USING (")
                        .append(String.join(", ", join.usingColumns()))
                        .append(")");
            }
        }

        // WHERE
        if (!whereConditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", whereConditions));
        }

        // GROUP BY
        if (groupByAll) {
            sql.append(" GROUP BY ALL");
        } else if (!groupByColumns.isEmpty()) {
            sql.append(" GROUP BY ").append(String.join(", ", groupByColumns));
        }

        // HAVING
        if (!havingConditions.isEmpty()) {
            sql.append(" HAVING ").append(String.join(" AND ", havingConditions));
        }

        // WINDOW (named window specs)
        if (!namedWindows.isEmpty()) {
            sql.append(" WINDOW ")
                    .append(namedWindows.stream()
                            .map(w -> w.name() + " AS (" + w.spec() + ")")
                            .collect(Collectors.joining(", ")));
        }

        // QUALIFY
        if (qualifyCondition != null) {
            sql.append(" QUALIFY ").append(qualifyCondition);
        }

        // ORDER BY
        if (!orderByColumns.isEmpty()) {
            sql.append(" ORDER BY ")
                    .append(orderByColumns.stream()
                            .map(ob -> {
                                StringBuilder obs = new StringBuilder(ob.expression());
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
    public record SelectColumn(String expression, String alias) {
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
            String onCondition,
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
        NATURAL("NATURAL JOIN");

        private final String sql;

        JoinType(String sql) {
            this.sql = sql;
        }

        public String toSql() {
            return sql;
        }
    }

    /** ORDER BY column */
    public record OrderByColumn(String expression, SortDirection direction, NullsPosition nulls) {
    }

    public enum SortDirection {
        ASC, DESC
    }

    public enum NullsPosition {
        FIRST, LAST, DEFAULT
    }

    /** Window function column: FUNC() OVER (...) AS alias */
    public record WindowColumn(String function, String overClause, String alias) {
    }

    /** Named WINDOW clause */
    public record NamedWindow(String name, String spec) {
    }

    /** SET operation (UNION, INTERSECT, EXCEPT) */
    public record SetOperation(SetOpType type, boolean all, SqlBuilder other, boolean wrapped) {
    }

    public enum SetOpType {
        UNION, INTERSECT, EXCEPT
    }
}
