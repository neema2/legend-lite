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

    /**
     * Typed arithmetic operator. Replaces stringly-typed {@link Binary} for
     * arithmetic. The renderer decides operator spelling; nodes carry
     * semantic ops resolved by the checker via overload resolution.
     */
    enum ArithOp {
        PLUS("+"), MINUS("-"), TIMES("*"), DIVIDE("/"),
        MOD("%"), POWER("^"), REM("%");
        private final String sql;
        ArithOp(String sql) { this.sql = sql; }
        public String sql() { return sql; }
    }

    /** Typed binary arithmetic: left op right. Always parenthesised. */
    record BinaryArith(ArithOp op, SqlExpr left, SqlExpr right) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return "(" + left.toSql(dialect) + " " + op.sql() + " " + right.toSql(dialect) + ")";
        }
    }

    /** Typed string concatenation: left || right. Always parenthesised. */
    record StringConcat(SqlExpr left, SqlExpr right) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return "(" + left.toSql(dialect) + " || " + right.toSql(dialect) + ")";
        }
    }

    /** Typed unary negation: (-expr). */
    record Negate(SqlExpr expr) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return "(- " + expr.toSql(dialect) + ")";
        }
    }

    /**
     * Typed comparison operator. Replaces stringly-typed {@link Binary} for
     * comparisons. Comparison ops are not parenthesised (they sit inside
     * larger boolean expressions which manage their own grouping).
     */
    enum CompareOp {
        EQ("="), NE("<>"), LT("<"), LE("<="), GT(">"), GE(">=");
        private final String sql;
        CompareOp(String sql) { this.sql = sql; }
        public String sql() { return sql; }
    }

    /** Typed binary comparison: left op right. */
    record BinaryCompare(CompareOp op, SqlExpr left, SqlExpr right) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return left.toSql(dialect) + " " + op.sql() + " " + right.toSql(dialect);
        }
    }

    // ==================== Lists ====================

    /**
     * Typed list element access by 1-based index. The bindings convert
     * Pure 0-based indexing to SQL 1-based indexing at lowering time.
     * Replaces stringly-typed {@code FunctionCall("listExtract", ...)}.
     */
    record ListExtract(SqlExpr list, SqlExpr index) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.renderListExtract(list.toSql(dialect), index.toSql(dialect));
        }
    }

    /**
     * Typed list slice: 1-based inclusive {@code [from, to]} bounds.
     * Replaces stringly-typed {@code FunctionCall("listSlice", ...)}.
     */
    record ListSlice(SqlExpr list, SqlExpr from, SqlExpr to) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.renderListSlice(list.toSql(dialect), from.toSql(dialect), to.toSql(dialect));
        }
    }

    /**
     * Typed list length. Replaces stringly-typed
     * {@code FunctionCall("listLength", ...)}.
     */
    record ListLength(SqlExpr list) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.renderListLength(list.toSql(dialect));
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

    // NOTE: Legacy {@code Exists(SqlBuilder)} and {@code Subquery(SqlBuilder)} records
    // were removed in the c0954a port because {@code SqlBuilder} has been retired in
    // favour of the {@link com.gs.legend.plan.sql.SqlRelation} MIR. When EXISTS /
    // scalar-subquery support is re-introduced (Stage 3+), the new records will hold
    // a {@link com.gs.legend.plan.sql.SqlRelation} and render via
    // {@link com.gs.legend.plan.printing.SqlRelationPrinter}. The dependency is moved
    // into a small bridge so {@code sqlgen} does not need to import {@code plan.**}.

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
     * $p.addresses.street or $e.dept.company.country)
     * is detected. Consumed by resolveAssociationRefs to generate EXISTS subqueries.
     * Must never reach toSql() — throws if it does.
     *
     * @param hops      Ordered list of association property names to traverse.
     *                  Single-hop: ["addresses"]. Multi-hop: ["dept", "company"].
     * @param targetCol The resolved column name on the final target table.
     */
    record AssociationRef(java.util.List<String> hops, String targetCol) implements SqlExpr {
        /** Convenience: first hop property name (used for single-hop lookups). */
        public String assocProp() { return hops.get(0); }

        @Override
        public String toSql(SQLDialect dialect) {
            throw new IllegalStateException(
                    "AssociationRef should be resolved before SQL generation: " + hops + "." + targetCol);
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

    // ==================== External Data Source ====================

    /** External data source rendered as a subquery. Dialect renders the URL scheme. */
    record SourceUrl(String url) implements SqlExpr {
        @Override
        public String toSql(SQLDialect dialect) {
            return dialect.renderSourceUrl(url);
        }
    }

    /**
     * Windowed function call:
     * {@code FUNC(args) OVER (PARTITION BY … ORDER BY … [ROWS|RANGE BETWEEN … AND …])}.
     * Function name is routed through {@link SQLDialect#renderFunction} so that
     * dialects can remap {@code rowNumber}→{@code ROW_NUMBER}, {@code rank}→
     * {@code RANK}, etc. Partition and order-by expressions are pre-rendered
     * sub-expressions; the frame clause, when present, is rendered inline.
     */
    record WindowCall(String name,
                      List<SqlExpr> args,
                      List<SqlExpr> partitionBy,
                      List<OrderByTerm> orderBy,
                      java.util.Optional<WindowFrame> frame) implements SqlExpr {

        @Override
        public String toSql(SQLDialect dialect) {
            var renderedArgs = args.stream()
                    .map(a -> a.toSql(dialect))
                    .collect(Collectors.toList());
            String call = dialect.renderFunction(name, renderedArgs);
            StringBuilder over = new StringBuilder(" OVER (");
            boolean need = false;
            if (!partitionBy.isEmpty()) {
                over.append("PARTITION BY ");
                for (int i = 0; i < partitionBy.size(); i++) {
                    if (i > 0) over.append(", ");
                    over.append(partitionBy.get(i).toSql(dialect));
                }
                need = true;
            }
            if (!orderBy.isEmpty()) {
                if (need) over.append(" ");
                over.append("ORDER BY ");
                for (int i = 0; i < orderBy.size(); i++) {
                    if (i > 0) over.append(", ");
                    over.append(orderBy.get(i).toSql(dialect));
                }
                need = true;
            }
            if (frame.isPresent()) {
                if (need) over.append(" ");
                over.append(frame.get().toSql());
            }
            over.append(")");
            return call + over;
        }
    }

    /**
     * Window frame clause: {@code ROWS|RANGE BETWEEN <start> AND <end>}.
     * Dialect-agnostic — standard SQL syntax is universal across DuckDB,
     * Postgres, Snowflake, etc.
     */
    record WindowFrame(FrameType type, FrameBound start, FrameBound end) {
        public String toSql() {
            return type.name() + " BETWEEN "
                    + start.toSql(/*isStart=*/true)
                    + " AND "
                    + end.toSql(/*isStart=*/false);
        }
    }

    enum FrameType { ROWS, RANGE }

    /** Window frame bound. {@code isStart} disambiguates UNBOUNDED's direction. */
    sealed interface FrameBound {
        String toSql(boolean isStart);
    }

    /** Unbounded — {@code UNBOUNDED PRECEDING} at start, {@code UNBOUNDED FOLLOWING} at end. */
    record UnboundedFrameBound() implements FrameBound {
        @Override public String toSql(boolean isStart) {
            return isStart ? "UNBOUNDED PRECEDING" : "UNBOUNDED FOLLOWING";
        }
    }

    /** {@code CURRENT ROW}. */
    record CurrentRowFrameBound() implements FrameBound {
        @Override public String toSql(boolean isStart) { return "CURRENT ROW"; }
    }

    /**
     * Signed offset frame bound. Negative → {@code n PRECEDING}, positive →
     * {@code n FOLLOWING}, zero → {@code CURRENT ROW} (normalized).
     *
     * <p>{@code double} to support fractional RANGE bounds (e.g. {@code 0.5
     * FOLLOWING}); integral values (ROWS offsets) round-trip exactly and are
     * rendered without a decimal point.
     */
    record OffsetFrameBound(double offset) implements FrameBound {
        @Override public String toSql(boolean isStart) {
            if (offset == 0) return "CURRENT ROW";
            double mag = Math.abs(offset);
            String lit = (mag == Math.floor(mag) && !Double.isInfinite(mag))
                    ? String.valueOf((long) mag)
                    : java.math.BigDecimal.valueOf(mag).stripTrailingZeros().toPlainString();
            return lit + (offset < 0 ? " PRECEDING" : " FOLLOWING");
        }
    }
}
