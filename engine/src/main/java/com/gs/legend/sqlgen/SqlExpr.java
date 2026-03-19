package com.gs.legend.sqlgen;

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

    /** Raw unquoted identifier — used for lambda parameter references. */
    record Identifier(String name) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return name;
        }
    }

    /** First-class lambda expression: ((p1, p2) -> body). */
    record LambdaExpr(List<String> params, SqlExpr body) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            String bodyStr = body.toSql(dialect);
            if (params.size() == 1) {
                // Single-param: s -> body (DuckDB list_filter/list_transform style)
                return params.get(0) + " -> " + bodyStr;
            }
            // Multi-param: ((y, x) -> body) (DuckDB list_reduce style)
            String paramList = String.join(", ", params);
            return "((" + paramList + ") -> " + bodyStr + ")";
        }
    }

    // ==================== Literals ====================

    /** Numeric literal — takes an actual Number, renders via toString(). */
    record NumericLiteral(Number value) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return value.toString();
        }
    }

    /** Decimal literal — renders BigDecimal with toPlainString() (no scientific notation). */
    record DecimalLiteral(java.math.BigDecimal value) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return value.toPlainString();
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

    /** CURRENT_TIMESTAMP. */
    record CurrentTimestamp() implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return "CURRENT_TIMESTAMP";
        }
    }

    /** Interval unit literal — dialect-routed (e.g., 'DAY' in DuckDB). */
    record IntervalLiteral(String unit) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.renderIntervalUnit(unit);
        }
    }

    /** ORDER BY term: column + direction + null ordering. Used in window specs. */
    record OrderByTerm(SqlExpr column, String direction, String nullOrder) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return column.toSql(dialect) + " " + direction + " " + nullOrder;
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
            String sql = left.toSql(dialect) + " " + op + " " + right.toSql(dialect);
            // Arithmetic and string concat ops get parenthesized; comparison/logical ops
            // don't
            return switch (op) {
                case "+", "-", "*", "/", "||", "//", "<<", ">>", "&", "|", "^" ->
                    "(" + sql + ")";
                default -> sql;
            };
        }
    }

    /** Explicit parenthesization wrapper */
    record Grouped(SqlExpr inner) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return "(" + inner.toSql(dialect) + ")";
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
            return "NOT (" + expr.toSql(dialect) + ")";
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


    /** Integer division: left // right — no outer parens around result. */
    record IntegerDivide(SqlExpr left, SqlExpr right) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return left.toSql(dialect) + " // " + right.toSql(dialect);
        }
    }

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

    /** Scalar subquery: (SELECT ... ) — used when a full query appears as an expression. */
    record Subquery(SqlBuilder query) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return "(" + query.toSql(dialect) + ")";
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

    /** Multi-branch searched CASE: CASE WHEN c1 THEN r1 WHEN c2 THEN r2 ... ELSE elseExpr END */
    record SearchedCase(List<WhenBranch> branches, SqlExpr elseExpr) implements SqlExpr {
        public record WhenBranch(SqlExpr condition, SqlExpr result) {}
        @Override
        public String toSql(SQLDialect dialect) {
            StringBuilder sb = new StringBuilder("CASE");
            for (var branch : branches) {
                sb.append(" WHEN ").append(branch.condition.toSql(dialect))
                  .append(" THEN ").append(branch.result.toSql(dialect));
            }
            if (elseExpr != null) {
                sb.append(" ELSE ").append(elseExpr.toSql(dialect));
            }
            sb.append(" END");
            return sb.toString();
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

    /** Dialect-rendered Variant text extraction (returns string value, not Variant). */
    record VariantTextExtract(SqlExpr expr, String key) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.renderVariantTextAccess(expr.toSql(dialect), key);
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

    /** Qualified star: table.* — used in joins to select all columns from one side. */
    record QualifiedStar(String table) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return table + ".*";
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

    // ==================== JSON (dialect-delegated) ====================

    /** Dialect-rendered JSON object: json_object(k1, v1, k2, v2, ...) */
    record JsonObject(List<SqlExpr> keyValuePairs) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            var rendered = keyValuePairs.stream()
                    .map(e -> e.toSql(dialect))
                    .collect(Collectors.toList());
            return dialect.renderJsonObject(rendered);
        }
    }

    /** Dialect-rendered JSON array aggregation: json_group_array(expr) */
    record JsonArrayAgg(SqlExpr expr) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.renderJsonArrayAgg(expr.toSql(dialect));
        }
    }

    // ==================== Variant Expressions ====================
    // PlanGenerator emits these using Variant semantics.
    // Each dialect decides the physical representation (DuckDB: JSON, Snowflake: VARIANT).

    /** Mark a literal value as Variant type. DuckDB: expr::JSON */
    record VariantLiteral(SqlExpr expr) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.renderVariantLiteral(expr.toSql(dialect));
        }
    }

    /** Access a Variant field by key (returns Variant). DuckDB: (expr)->'key' */
    record VariantAccess(SqlExpr expr, String key) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.renderVariantAccess(expr.toSql(dialect), key);
        }
    }

    /** Access a Variant array element by index (returns Variant). DuckDB: (expr)->index */
    record VariantIndex(SqlExpr expr, int index) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.renderVariantIndex(expr.toSql(dialect), index);
        }
    }

    /** Extract text value from Variant by key (returns string). DuckDB: (expr)->>'key' */
    record VariantTextAccess(SqlExpr expr, String key) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.renderVariantTextAccess(expr.toSql(dialect), key);
        }
    }

    /** Convert a value to Variant type. DuckDB: CAST(expr AS JSON) */
    record ToVariant(SqlExpr expr) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.renderToVariant(expr.toSql(dialect));
        }
    }

    /** Cast Variant to a typed array. DuckDB: CAST(expr AS BIGINT[]) */
    record VariantArrayCast(SqlExpr expr, String sqlType) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.renderVariantArrayCast(expr.toSql(dialect), sqlType);
        }
    }

    /** Cast Variant to a scalar type. DuckDB: CAST(expr AS BIGINT) */
    record VariantScalarCast(SqlExpr expr, String sqlType) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.renderVariantScalarCast(expr.toSql(dialect), sqlType);
        }
    }

    /** Cast a value to VARIANT type for type preservation. Dialect-delegated. */
    record VariantCast(SqlExpr expr) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.renderVariantCast(expr.toSql(dialect));
        }
    }
}
