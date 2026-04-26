package com.gs.legend.sqlgen;

import java.util.List;

/**
 * Structural SQL expression IR. Pure data — no SQL emission methods, no
 * {@link SQLDialect} dependency.
 *
 * <p>Per AGENTS.md invariant 3a: codegen is owned by the dialect's
 * {@link SQLDialect#render(SqlExpr)} pattern match. Records here carry
 * only the structural fields needed to reconstruct the SQL, never the
 * SQL itself. New variants get a new arm in {@code render}; never a
 * method on the record.
 *
 * <p>Mirrors legend-engine's {@code RelationalOperationElement} hierarchy,
 * simplified for legend-lite's needs.
 */
public sealed interface SqlExpr permits
        SqlExpr.And,
        SqlExpr.ArrayLiteral,
        SqlExpr.AssociationRef,
        SqlExpr.Between,
        SqlExpr.Binary,
        SqlExpr.BinaryArith,
        SqlExpr.BinaryCompare,
        SqlExpr.BoolLiteral,
        SqlExpr.CaseWhen,
        SqlExpr.Cast,
        SqlExpr.Column,
        SqlExpr.ColumnRef,
        SqlExpr.CurrentDate,
        SqlExpr.CurrentTimestamp,
        SqlExpr.DateAdd,
        SqlExpr.DateLiteral,
        SqlExpr.DecimalLiteral,
        SqlExpr.EndsWith,
        SqlExpr.FieldAccess,
        SqlExpr.FunctionCall,
        SqlExpr.Grouped,
        SqlExpr.Identifier,
        SqlExpr.In,
        SqlExpr.IntegerDivide,
        SqlExpr.IntervalLiteral,
        SqlExpr.IsNotNull,
        SqlExpr.IsNull,
        SqlExpr.JsonArrayAgg,
        SqlExpr.JsonObject,
        SqlExpr.LambdaExpr,
        SqlExpr.ListContains,
        SqlExpr.ListExtract,
        SqlExpr.ListLength,
        SqlExpr.ListSlice,
        SqlExpr.Negate,
        SqlExpr.Not,
        SqlExpr.NullLiteral,
        SqlExpr.NumericLiteral,
        SqlExpr.Or,
        SqlExpr.OrderByTerm,
        SqlExpr.QualifiedStar,
        SqlExpr.SearchedCase,
        SqlExpr.SourceUrl,
        SqlExpr.Star,
        SqlExpr.StartsWith,
        SqlExpr.StrPosition,
        SqlExpr.StringConcat,
        SqlExpr.StringLiteral,
        SqlExpr.StructLiteral,
        SqlExpr.TimeLiteral,
        SqlExpr.TimestampLiteral,
        SqlExpr.ToVariant,
        SqlExpr.Unary,
        SqlExpr.Unnest,
        SqlExpr.VariantAccess,
        SqlExpr.VariantArrayCast,
        SqlExpr.VariantCast,
        SqlExpr.VariantIndex,
        SqlExpr.VariantLiteral,
        SqlExpr.VariantScalarCast,
        SqlExpr.VariantTextAccess,
        SqlExpr.VariantTextExtract,
        SqlExpr.WindowCall,
        SqlExpr.WindowSpec {

    // ==================== Column References ====================

    /** Qualified column: {@code table.column}. */
    record Column(String table, String column) implements SqlExpr {}

    /** Unqualified column reference (used in ORDER BY, GROUP BY on aliases). */
    record ColumnRef(String name) implements SqlExpr {}

    /** Raw unquoted identifier — used for lambda parameter references. */
    record Identifier(String name) implements SqlExpr {}

    /** First-class lambda expression: {@code ((p1, p2) -> body)}. */
    record LambdaExpr(List<String> params, SqlExpr body) implements SqlExpr {}

    // ==================== Literals ====================

    /** Numeric literal — carries an actual {@link Number}. */
    record NumericLiteral(Number value) implements SqlExpr {}

    /** Decimal literal — carries a {@link java.math.BigDecimal} (rendered without scientific notation). */
    record DecimalLiteral(java.math.BigDecimal value) implements SqlExpr {}

    /** {@code NULL}. */
    record NullLiteral() implements SqlExpr {}

    /** {@code CURRENT_DATE}. */
    record CurrentDate() implements SqlExpr {}

    /** {@code CURRENT_TIMESTAMP}. */
    record CurrentTimestamp() implements SqlExpr {}

    /** Interval unit literal (e.g. {@code 'DAY'}) — dialect-routed. */
    record IntervalLiteral(String unit) implements SqlExpr {}

    /** ORDER BY term: column + direction + null ordering. Used in window specs. */
    record OrderByTerm(SqlExpr column, String direction, String nullOrder) implements SqlExpr {}

    /** String literal (dialect-quoted). */
    record StringLiteral(String value) implements SqlExpr {}

    /** Boolean literal (dialect-formatted). */
    record BoolLiteral(boolean value) implements SqlExpr {}

    /** Timestamp literal (dialect-formatted). */
    record TimestampLiteral(String value) implements SqlExpr {}

    /** Date literal (dialect-formatted). */
    record DateLiteral(String value) implements SqlExpr {}

    /** Time literal (dialect-formatted). */
    record TimeLiteral(String value) implements SqlExpr {}

    // ==================== Operators ====================

    /**
     * Stringly-typed binary operator (legacy). Kept until Phase 3.3
     * finishes the migration to {@link BinaryArith} / {@link BinaryCompare}.
     */
    record Binary(SqlExpr left, String op, SqlExpr right) implements SqlExpr {}

    /** Explicit parenthesization wrapper. */
    record Grouped(SqlExpr inner) implements SqlExpr {}

    /** Unary prefix operator: {@code op expr} (e.g. {@code NOT}, {@code -}). Legacy. */
    record Unary(String op, SqlExpr operand) implements SqlExpr {}

    /**
     * Typed arithmetic operator. Replaces stringly-typed {@link Binary} for
     * arithmetic. Nodes carry semantic ops resolved by the checker via
     * overload resolution; spelling decisions belong to the dialect.
     */
    enum ArithOp {
        PLUS("+"), MINUS("-"), TIMES("*"), DIVIDE("/"),
        MOD("%"), POWER("^"), REM("%");
        private final String sql;
        ArithOp(String sql) { this.sql = sql; }
        public String sql() { return sql; }
    }

    /** Typed binary arithmetic: {@code left op right}. Always parenthesised. */
    record BinaryArith(ArithOp op, SqlExpr left, SqlExpr right) implements SqlExpr {}

    /** Typed string concatenation: {@code left || right}. Always parenthesised. */
    record StringConcat(SqlExpr left, SqlExpr right) implements SqlExpr {}

    /** Typed unary negation: {@code (-expr)}. */
    record Negate(SqlExpr expr) implements SqlExpr {}

    /**
     * Typed comparison operator. Replaces stringly-typed {@link Binary} for
     * comparisons.
     */
    enum CompareOp {
        EQ("="), NE("<>"), LT("<"), LE("<="), GT(">"), GE(">=");
        private final String sql;
        CompareOp(String sql) { this.sql = sql; }
        public String sql() { return sql; }
    }

    /** Typed binary comparison: {@code left op right}. Not parenthesised. */
    record BinaryCompare(CompareOp op, SqlExpr left, SqlExpr right) implements SqlExpr {}

    // ==================== Lists ====================

    /**
     * Typed list element access by 1-based index. The bindings convert
     * Pure 0-based indexing to SQL 1-based indexing at lowering time.
     * Replaces stringly-typed {@code FunctionCall("listExtract", ...)}.
     */
    record ListExtract(SqlExpr list, SqlExpr index) implements SqlExpr {}

    /**
     * Typed list slice: 1-based inclusive {@code [from, to]} bounds.
     * Replaces stringly-typed {@code FunctionCall("listSlice", ...)}.
     */
    record ListSlice(SqlExpr list, SqlExpr from, SqlExpr to) implements SqlExpr {}

    /**
     * Typed list length. Replaces stringly-typed
     * {@code FunctionCall("listLength", ...)}.
     */
    record ListLength(SqlExpr list) implements SqlExpr {}

    /** AND of multiple conditions. */
    record And(List<SqlExpr> conditions) implements SqlExpr {}

    /** OR of multiple conditions. */
    record Or(List<SqlExpr> conditions) implements SqlExpr {}

    /** {@code NOT expr}. */
    record Not(SqlExpr expr) implements SqlExpr {}

    // ==================== Functions ====================

    /** SQL function call: {@code name(arg1, arg2, ...)}. */
    record FunctionCall(String name, List<SqlExpr> args) implements SqlExpr {}

    /** {@code CAST(expr AS typeName)} — type name resolved through dialect. */
    record Cast(SqlExpr expr, String pureTypeName) implements SqlExpr {}

    /** Integer division: {@code left // right}. */
    record IntegerDivide(SqlExpr left, SqlExpr right) implements SqlExpr {}

    /** {@code expr IS NULL}. */
    record IsNull(SqlExpr expr) implements SqlExpr {}

    /** {@code expr IS NOT NULL}. */
    record IsNotNull(SqlExpr expr) implements SqlExpr {}

    /** {@code expr IN (val1, val2, ...)}. */
    record In(SqlExpr expr, List<SqlExpr> values) implements SqlExpr {}

    /** {@code expr BETWEEN low AND high}. */
    record Between(SqlExpr expr, SqlExpr low, SqlExpr high) implements SqlExpr {}

    // NOTE: Legacy {@code Exists(SqlBuilder)} and {@code Subquery(SqlBuilder)} records
    // were removed in the c0954a port because {@code SqlBuilder} has been retired in
    // favour of the {@link com.gs.legend.plan.sql.SqlRelation} MIR. When EXISTS /
    // scalar-subquery support is re-introduced (Stage 3+), the new records will hold
    // a {@link com.gs.legend.plan.sql.SqlRelation} and the dialect's
    // {@link SQLDialect#render(com.gs.legend.plan.sql.SqlRelation)} arm
    // emits the subquery SQL.

    // ==================== CASE ====================

    /** {@code CASE WHEN condition THEN thenExpr ELSE elseExpr END}. */
    record CaseWhen(SqlExpr condition, SqlExpr thenExpr, SqlExpr elseExpr) implements SqlExpr {}

    /** Multi-branch searched CASE. */
    record SearchedCase(List<WhenBranch> branches, SqlExpr elseExpr) implements SqlExpr {
        public record WhenBranch(SqlExpr condition, SqlExpr result) {}
    }

    // ==================== Dialect-specific (delegated by codegen) ====================

    /** List containment check. Dialect renders. */
    record ListContains(SqlExpr list, SqlExpr element) implements SqlExpr {}

    /** {@code startsWith}. Dialect renders. */
    record StartsWith(SqlExpr str, SqlExpr prefix) implements SqlExpr {}

    /** {@code endsWith}. Dialect renders. */
    record EndsWith(SqlExpr str, SqlExpr suffix) implements SqlExpr {}

    /** Date arithmetic. Dialect renders. */
    record DateAdd(SqlExpr date, SqlExpr amount, String unit) implements SqlExpr {}

    /** Variant text extraction (returns string value, not Variant). Dialect renders. */
    record VariantTextExtract(SqlExpr expr, String key) implements SqlExpr {}

    /** Struct field access: {@code base.field} (for struct column paths like {@code t.struct.prop}). */
    record FieldAccess(SqlExpr base, String field) implements SqlExpr {}

    /** {@code UNNEST(array)}. Dialect renders. */
    record Unnest(SqlExpr array) implements SqlExpr {}

    /** {@code POSITION(substring IN string)} — string index lookup. */
    record StrPosition(SqlExpr substring, SqlExpr string) implements SqlExpr {}

    // ==================== Window Functions ====================

    /** Window specification: {@code PARTITION BY ... ORDER BY ... ROWS/RANGE ...}. */
    record WindowSpec(List<SqlExpr> partitionBy, List<SqlExpr> orderBy, String frame) implements SqlExpr {}

    // ==================== Aggregation / wildcards ====================

    /** {@code SELECT *}. */
    record Star() implements SqlExpr {}

    /** Qualified star: {@code table.*} — used in joins to select all columns from one side. */
    record QualifiedStar(String table) implements SqlExpr {}

    // ==================== Compile-time Markers ====================

    /**
     * Association property reference — compile-time only. Created during
     * scalar compilation when an association path (e.g. {@code $p.addresses.street}
     * or {@code $e.dept.company.country}) is detected. Consumed by
     * {@code resolveAssociationRefs} to generate EXISTS subqueries. Must
     * never reach codegen — the dialect throws if it does.
     *
     * @param hops      Ordered list of association property names to traverse.
     *                  Single-hop: {@code ["addresses"]}. Multi-hop: {@code ["dept", "company"]}.
     * @param targetCol The resolved column name on the final target table.
     */
    record AssociationRef(List<String> hops, String targetCol) implements SqlExpr {
        /** Convenience: first hop property name (used for single-hop lookups). */
        public String assocProp() { return hops.get(0); }
    }

    // ==================== Struct / Array (dialect-delegated) ====================

    /** Struct literal. Dialect renders. */
    record StructLiteral(java.util.LinkedHashMap<String, SqlExpr> fields) implements SqlExpr {}

    /** Array literal. Dialect renders. */
    record ArrayLiteral(List<SqlExpr> elements) implements SqlExpr {}

    // ==================== JSON (dialect-delegated) ====================

    /** {@code json_object(k1, v1, k2, v2, ...)}. Dialect renders. */
    record JsonObject(List<SqlExpr> keyValuePairs) implements SqlExpr {}

    /** {@code json_group_array(expr)}. Dialect renders. */
    record JsonArrayAgg(SqlExpr expr) implements SqlExpr {}

    // ==================== Variant Expressions ====================
    // PlanGenerator emits these using Variant semantics. Each dialect
    // decides the physical representation (DuckDB: JSON, Snowflake: VARIANT).

    /** Mark a literal value as Variant type. */
    record VariantLiteral(SqlExpr expr) implements SqlExpr {}

    /** Access a Variant field by key (returns Variant). */
    record VariantAccess(SqlExpr expr, String key) implements SqlExpr {}

    /** Access a Variant array element by index (returns Variant). */
    record VariantIndex(SqlExpr expr, int index) implements SqlExpr {}

    /** Extract text value from Variant by key (returns string). */
    record VariantTextAccess(SqlExpr expr, String key) implements SqlExpr {}

    /** Convert a value to Variant type. */
    record ToVariant(SqlExpr expr) implements SqlExpr {}

    /** Cast Variant to a typed array. */
    record VariantArrayCast(SqlExpr expr, String sqlType) implements SqlExpr {}

    /** Cast Variant to a scalar type. */
    record VariantScalarCast(SqlExpr expr, String sqlType) implements SqlExpr {}

    /** Cast a value to VARIANT type for type preservation. */
    record VariantCast(SqlExpr expr) implements SqlExpr {}

    // ==================== External Data Source ====================

    /** External data source rendered as a subquery. Dialect renders the URL scheme. */
    record SourceUrl(String url) implements SqlExpr {}

    /**
     * Windowed function call:
     * {@code FUNC(args) OVER (PARTITION BY … ORDER BY … [ROWS|RANGE BETWEEN … AND …])}.
     * The {@code fn} field is a typed {@link com.gs.legend.plan.sql.SqlAggregate}
     * variant — any sub-category ({@code Reducer}, {@code RankingFn},
     * {@code ValueFn}). Reducers render identically here and in agg context;
     * ranking and value functions only appear here. Dispatch happens via
     * {@link SQLDialect#render(com.gs.legend.plan.sql.SqlAggregate)}.
     */
    record WindowCall(com.gs.legend.plan.sql.SqlAggregate fn,
                      List<SqlExpr> partitionBy,
                      List<OrderByTerm> orderBy,
                      java.util.Optional<WindowFrame> frame) implements SqlExpr {}

    /**
     * Window frame clause: {@code ROWS|RANGE BETWEEN <start> AND <end>}.
     * Pure data; rendered by {@link SQLDialect#renderWindowFrame}.
     */
    record WindowFrame(FrameType type, FrameBound start, FrameBound end) {}

    enum FrameType { ROWS, RANGE }

    /**
     * Window frame bound. {@code isStart} disambiguates {@code UNBOUNDED}'s
     * direction at codegen time.
     */
    sealed interface FrameBound permits
            UnboundedFrameBound,
            CurrentRowFrameBound,
            OffsetFrameBound {}

    /** Unbounded — {@code UNBOUNDED PRECEDING} at start, {@code UNBOUNDED FOLLOWING} at end. */
    record UnboundedFrameBound() implements FrameBound {}

    /** {@code CURRENT ROW}. */
    record CurrentRowFrameBound() implements FrameBound {}

    /**
     * Signed offset frame bound. Negative → {@code n PRECEDING}, positive →
     * {@code n FOLLOWING}, zero → {@code CURRENT ROW} (normalised at
     * render time).
     *
     * <p>{@code double} to support fractional RANGE bounds (e.g.
     * {@code 0.5 FOLLOWING}); integral values (ROWS offsets) round-trip
     * exactly and are rendered without a decimal point.
     */
    record OffsetFrameBound(double offset) implements FrameBound {}
}
