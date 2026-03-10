package org.finos.legend.pure.dsl.ast;

import org.finos.legend.engine.transpiler.SQLDialect;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Structural SQL expression metamodel.
 *
 * <p>
 * Every SQL expression in the system is represented as a {@code SqlExpr} node.
 * No raw SQL strings — all rendering happens through {@link #toSql(SQLDialect)}
 * at serialization time, giving the dialect full control over syntax.
 *
 * <p>
 * Mirrors legend-engine's {@code RelationalOperationElement} hierarchy,
 * simplified for legend-lite's needs.
 */
public sealed interface SqlExpr {

    /**
     * Renders this expression to SQL using the given dialect.
     * This is the ONLY place SQL text is produced from expressions.
     */
    String toSql(SQLDialect dialect);

    // ==================== Column References ====================

    /** Qualified column: table.column */
    record Column(String table, String column) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.quoteIdentifier(table) + "." + dialect.quoteIdentifier(column);
        }
    }

    /** Unqualified column reference (used in ORDER BY, GROUP BY on aliases). */
    record ColumnRef(String name) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.quoteIdentifier(name);
        }
    }

    // ==================== Literals ====================

    /** Numeric literal (rendered as-is). */
    record Literal(String value) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return value;
        }
    }

    /** NULL literal. */
    record NullLiteral() implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return "NULL";
        }
    }

    /** CURRENT_DATE. */
    record CurrentDate() implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return "CURRENT_DATE";
        }
    }

    /** String literal (dialect-quoted). */
    record StringLiteral(String value) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.quoteStringLiteral(value);
        }
    }

    /** Boolean literal (dialect-formatted). */
    record BoolLiteral(boolean value) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.formatBoolean(value);
        }
    }

    /** Timestamp literal (dialect-formatted). */
    record TimestampLiteral(String value) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.formatTimestamp(value);
        }
    }

    /** Date literal (dialect-formatted). */
    record DateLiteral(String value) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.formatDate(value);
        }
    }

    /** Time literal (dialect-formatted). */
    record TimeLiteral(String value) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.formatTime(value);
        }
    }

    // ==================== Operators ====================

    /** Binary operator: left op right */
    record Binary(SqlExpr left, String op, SqlExpr right) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return left.toSql(dialect) + " " + op + " " + right.toSql(dialect);
        }
    }

    /** Unary prefix operator: op expr (e.g., NOT, -) */
    record Unary(String op, SqlExpr operand) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return op + " " + operand.toSql(dialect);
        }
    }

    /** AND of multiple conditions. */
    record And(List<SqlExpr> conditions) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return conditions.stream()
                    .map(c -> c.toSql(dialect))
                    .collect(Collectors.joining(" AND ", "(", ")"));
        }
    }

    /** OR of multiple conditions. */
    record Or(List<SqlExpr> conditions) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return conditions.stream()
                    .map(c -> c.toSql(dialect))
                    .collect(Collectors.joining(" OR ", "(", ")"));
        }
    }

    /** NOT expr */
    record Not(SqlExpr expr) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return "NOT " + expr.toSql(dialect);
        }
    }

    // ==================== Functions ====================

    /** SQL function call: name(arg1, arg2, ...) */
    record FunctionCall(String name, List<SqlExpr> args) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            var renderedArgs = args.stream()
                    .map(a -> a.toSql(dialect))
                    .collect(Collectors.toList());
            return dialect.renderFunction(name, renderedArgs);
        }
    }

    /** CAST(expr AS typeName) — type name resolved through dialect. */
    record Cast(SqlExpr expr, String pureTypeName) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return "CAST(" + expr.toSql(dialect) + " AS " + dialect.sqlTypeName(pureTypeName) + ")";
        }
    }

    // ==================== Predicates ====================

    /** expr IS NULL */
    record IsNull(SqlExpr expr) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return expr.toSql(dialect) + " IS NULL";
        }
    }

    /** expr IS NOT NULL */
    record IsNotNull(SqlExpr expr) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return expr.toSql(dialect) + " IS NOT NULL";
        }
    }

    /** expr IN (val1, val2, ...) */
    record In(SqlExpr expr, List<SqlExpr> values) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            String vals = values.stream()
                    .map(v -> v.toSql(dialect))
                    .collect(Collectors.joining(", "));
            return expr.toSql(dialect) + " IN (" + vals + ")";
        }
    }

    /** expr BETWEEN low AND high */
    record Between(SqlExpr expr, SqlExpr low, SqlExpr high) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return expr.toSql(dialect) + " BETWEEN " + low.toSql(dialect) + " AND " + high.toSql(dialect);
        }
    }

    /** EXISTS (subquery) */
    record Exists(SqlBuilder subquery) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return "EXISTS (" + subquery.toSql(dialect) + ")";
        }
    }

    // ==================== CASE ====================

    /** CASE WHEN condition THEN thenExpr ELSE elseExpr END */
    record CaseWhen(SqlExpr condition, SqlExpr thenExpr, SqlExpr elseExpr) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return "CASE WHEN " + condition.toSql(dialect) + " THEN " + thenExpr.toSql(dialect)
                    + " ELSE " + elseExpr.toSql(dialect) + " END";
        }
    }

    // ==================== Dialect-specific (delegated) ====================

    /** Dialect-rendered list containment check. */
    record ListContains(SqlExpr list, SqlExpr element) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.renderListContains(list.toSql(dialect), element.toSql(dialect));
        }
    }

    /** Dialect-rendered startsWith. */
    record StartsWith(SqlExpr str, SqlExpr prefix) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.renderStartsWith(str.toSql(dialect), prefix.toSql(dialect));
        }
    }

    /** Dialect-rendered endsWith. */
    record EndsWith(SqlExpr str, SqlExpr suffix) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.renderEndsWith(str.toSql(dialect), suffix.toSql(dialect));
        }
    }

    /** Dialect-rendered date arithmetic. */
    record DateAdd(SqlExpr date, SqlExpr amount, String unit) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.renderDateAdd(date.toSql(dialect), amount.toSql(dialect), unit);
        }
    }

    /** Dialect-rendered JSON text access. */
    record JsonAccess(SqlExpr expr, String key) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            var jsonDialect = dialect.getJsonDialect();
            if (jsonDialect != null) {
                return jsonDialect.variantGet(expr.toSql(dialect), key);
            }
            return expr.toSql(dialect) + "->>" + dialect.quoteStringLiteral(key);
        }
    }

    /**
     * Struct field access: base.field (for struct column paths like t.struct.prop).
     */
    record FieldAccess(SqlExpr base, String field) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return base.toSql(dialect) + "." + field;
        }
    }

    /** Dialect-rendered UNNEST(array). */
    record Unnest(SqlExpr array) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.renderUnnestExpression(array.toSql(dialect));
        }
    }

    /** POSITION(substring IN string) — string index lookup. */
    record StrPosition(SqlExpr substring, SqlExpr string) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return "POSITION(" + substring.toSql(dialect) + " IN " + string.toSql(dialect) + ")";
        }
    }

    // ==================== Window Functions ====================

    /** Window function: func OVER (overClause) */
    record WindowFunction(SqlExpr function, SqlExpr overClause) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return function.toSql(dialect) + " OVER (" + overClause.toSql(dialect) + ")";
        }
    }

    /** Window specification: PARTITION BY ... ORDER BY ... ROWS/RANGE ... */
    record WindowSpec(List<SqlExpr> partitionBy, List<SqlExpr> orderBy, String frame) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            StringBuilder sb = new StringBuilder();
            if (!partitionBy.isEmpty()) {
                sb.append("PARTITION BY ");
                sb.append(partitionBy.stream().map(e -> e.toSql(dialect)).collect(Collectors.joining(", ")));
            }
            if (!orderBy.isEmpty()) {
                if (!sb.isEmpty())
                    sb.append(" ");
                sb.append("ORDER BY ");
                sb.append(orderBy.stream().map(e -> e.toSql(dialect)).collect(Collectors.joining(", ")));
            }
            if (frame != null && !frame.isEmpty()) {
                if (!sb.isEmpty())
                    sb.append(" ");
                sb.append(frame);
            }
            return sb.toString();
        }
    }

    // ==================== Aggregation ====================

    /** SELECT * */
    record Star() implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return "*";
        }
    }

    // ==================== Compile-time Markers ====================

    /**
     * Association property reference — compile-time only.
     * Created during scalar compilation when an association path (e.g.,
     * $p.addresses.street)
     * is detected. Consumed by buildComparison to generate EXISTS subqueries.
     * Must never reach toSql() — throws if it does.
     */
    record AssociationRef(String assocProp, String targetCol) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            throw new IllegalStateException(
                    "AssociationRef should be resolved before SQL generation: " + assocProp + "." + targetCol);
        }
    }

    /**
     * Wrapped window function — compile-time only.
     * Represents a post-processor wrapping a window function,
     * e.g., ROUND(CUME_DIST() OVER(...), 2).
     * Created by extractWindowFunction, consumed by compileExtend which
     * inserts the OVER clause between the inner function and extra args.
     */
    record WrappedWindowFunction(String wrapperFunc, SqlExpr innerWindowFunc,
            List<SqlExpr> extraArgs) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            throw new IllegalStateException(
                    "WrappedWindowFunction should be resolved before SQL generation: " + wrapperFunc);
        }
    }

    // ==================== Struct / Array (dialect-delegated) ====================

    /** Dialect-rendered struct literal. */
    record StructLiteral(java.util.LinkedHashMap<String, SqlExpr> fields) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            var rendered = new java.util.LinkedHashMap<String, String>();
            fields.forEach((k, v) -> rendered.put(k, v.toSql(dialect)));
            return dialect.renderStructLiteral(rendered);
        }
    }

    /** Dialect-rendered array literal. */
    record ArrayLiteral(List<SqlExpr> elements) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.renderArrayLiteral(
                    elements.stream().map(e -> e.toSql(dialect)).collect(Collectors.toList()));
        }
    }
}
