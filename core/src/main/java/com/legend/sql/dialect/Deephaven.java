// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlType;
import com.legend.sql.TypeFact;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The Deephaven target (docs/DEEPHAVEN_PLAN_2026_10_22.md, DH1, DH2, DH5, DH7): the one query compiler's render target
 * whose text is a batch of Deephaven's table operations (its protocol's {@code BatchTableRequest}, as JSON) rather than
 * SQL: Deephaven's own SQL front end refused 6 of DataCube's 14 query shapes, its table operations none. It extends the
 * base renderer as every target does, and overrides the writing of a query ({@link DeephavenBatch}: the operations) and
 * of an expression (here: the formulas the operations carry).
 *
 * <p>What this dialect decides, in its own tables:
 * <ul>
 *   <li>SQL's three-valued logic over Deephaven's null sentinels (DH7): a condition is written in its two-valued
 *       forms, "is true" ({@link #truth}) and "is false" ({@link #falsity}), each guarding an operand that may be
 *       null; a boolean value is the two read back into a nullable {@code Boolean}. Where no operand may be null the
 *       plain formula is the answer;</li>
 *   <li>every function ({@link #call}) and every aggregate ({@link DeephavenBatch}): a spelling, or a refusal by name
 *       -- never a fallback;</li>
 *   <li>a plan parameter as a column of the one-row parameters table the connector uploads ({@code __p<i>}, the
 *       statement's i-th placeholder), joined into the operation that reads it: no value is written into formula
 *       text (DH5);</li>
 *   <li>a string literal in the formula language's backticks; a string it cannot carry as written (a backtick, a
 *       backslash, a control character) refused by name until measured.</li>
 * </ul>
 * Nothing from Deephaven's documentation or code is copied: the formulas are written from the language's documented
 * operators and functions, the operations from the protocol's field names.
 */
public final class Deephaven extends AnsiSqlRenderer {

    /** No quoting: a Deephaven name is spelled bare, and a name it cannot hold is refused ({@link DeephavenJson#name}). */
    public static final Lexicon LEXICON = new Lexicon('`', Set.of());

    /** No native list, no correlated explosion, no JSON carrier, no full join: each refused by name where met. */
    public static final CarrierStrategies.Caps CAPS = new CarrierStrategies.Caps(false, false, false, false);

    /** A parameter's column in the parameters table, by its place among the statement's placeholders. */
    static final String PARAMETER = "__p";

    public Deephaven() {
        super(LEXICON);
    }

    // ==================================================================
    // Scope: how an expression reads its columns
    // ==================================================================

    /** One select's columns: each source column's Deephaven name by its alias and name; after a group, its keys' and
     *  aggregates' columns by expression; the select's outputs by name (an ORDER BY's read of an output). */
    static final class Scope {
        final Map<String, Map<String, String>> sources = new LinkedHashMap<>();
        final Map<SqlExpr, String> computed = new LinkedHashMap<>();
        final Map<String, String> outputs = new LinkedHashMap<>();
        /** Each aggregate of the select's group: the column it is computed into and the column of its argument. */
        final Map<SqlAgg.Reducer, String[]> aggregates = new LinkedHashMap<>();
        /** Whether the select's rows are grouped: a source column is then read only as a group key. */
        boolean grouped;
    }

    private final Deque<Scope> scopes = new ArrayDeque<>();

    void pushScope(Scope s) {
        scopes.push(s);
    }

    void popScope() {
        scopes.pop();
    }

    private Scope scope() {
        Scope s = scopes.peek();
        if (s == null) {
            throw new IllegalStateException("an expression reached the Deephaven writer outside a select");
        }
        return s;
    }

    // ==================================================================
    // The query: a batch of table operations
    // ==================================================================

    @Override
    protected SqlWriter query(SqlWriter writer, SqlQuery q, int depth) {
        if (depth != 0) {
            throw new DialectCapability("a nested query reached Deephaven as a subquery: only a relation's own"
                    + " operations are written");
        }
        scopes.clear();
        return new DeephavenBatch(this, writer).batch(q);
    }

    @Override
    protected java.util.List<com.legend.sql.SqlRewriter> passes() {
        return List.of(new CarrierStrategies(CAPS));
    }

    /** {@inheritDoc} The engine's H2 text has no translation into table operations. */
    @Override
    public String fromEngineText(String statement) {
        throw new DialectCapability("the engine's H2 text reached Deephaven, which runs no SQL text");
    }

    // ==================================================================
    // Formulas: an expression's value
    // ==================================================================

    @Override
    protected SqlWriter expr(SqlWriter writer, SqlExpr e, int parentPrec) {
        String computed = scope().computed.get(e);
        if (computed != null) {
            return writer.append(computed);
        }
        if (isCondition(e)) {
            return booleanValue(writer, e);
        }
        return switch (e) {
            case SqlExpr.Group g -> writer.append("(").expr(g.inner(), 0).append(")");
            case SqlExpr.PlanParam p -> placeholder(writer, p);
            case SqlExpr.Column c -> writer.append(column(c));
            case SqlExpr.StringLit s -> writer.append(stringLiteral(s.value()));
            case SqlExpr.IntLit i -> writer.append(intLiteral(i));
            case SqlExpr.FloatLit f -> writer.append(doubleLiteral(f.value()));
            case SqlExpr.BoolLit b -> writer.append(b.value() ? "true" : "false");
            case SqlExpr.NullLit n -> writer.append(typedNull(n.type(), "a null"));
            case SqlExpr.DateLit d -> writer.append("parseLocalDate(").append(stringLiteral(d.iso())).append(")");
            case SqlExpr.Call c -> call(writer, c, parentPrec);
            case SqlExpr.Case c -> caseExpr(writer, c);
            case SqlExpr.Cast c -> cast(writer, c);
            case SqlAgg.Reducer r -> reducer(writer, r);
            case SqlExpr.DecimalLit d -> throw new DialectCapability("a decimal literal (" + d.value()
                    + ") reached Deephaven: its BigDecimal arithmetic is not shaped yet");
            case SqlExpr.TimestampLit t -> throw new DialectCapability("a date-time literal (" + t.iso()
                    + ") reached Deephaven: not shaped yet");
            case SqlExpr.TimeLit t -> throw new DialectCapability("a time-of-day literal (" + t.iso()
                    + ") reached Deephaven: not shaped yet");
            case SqlExpr.Star s -> throw new IllegalStateException("a star reached the formula writer: the"
                    + " projection writes the columns it names");
            case SqlExpr.StarExcept se -> throw new DialectCapability("a star with excluded columns reached"
                    + " Deephaven: no shape yet");
            case SqlExpr.FormatLit fl -> throw new DialectCapability("a date format reached Deephaven: no shape yet");
            case SqlExpr.DatePartLit p -> throw new DialectCapability("a date part (" + p.part()
                    + ") reached Deephaven: no shape yet");
            case SqlExpr.JsonKindLit k -> throw new DialectCapability("a JSON kind reached Deephaven, which holds no"
                    + " JSON");
            case SqlExpr.TypeNameLit t -> throw new DialectCapability("a type name reached Deephaven: no shape yet");
            case SqlExpr.ArrayLit a -> throw new DialectCapability("a list literal reached Deephaven: no shape yet");
            case SqlExpr.OrderedListAgg ola -> throw new DialectCapability("an ordered list aggregate reached"
                    + " Deephaven: no shape yet");
            case SqlExpr.JsonArray j -> throw new DialectCapability("a JSON array reached Deephaven, which holds no"
                    + " JSON");
            case SqlExpr.StructLit s -> throw new DialectCapability("a struct reached Deephaven: no shape yet");
            case SqlExpr.StructGet g -> throw new DialectCapability("a struct field's read reached Deephaven: no"
                    + " shape yet");
            case SqlExpr.Exists ex -> throw new DialectCapability("an EXISTS subquery reached Deephaven: no shape"
                    + " yet");
            case SqlExpr.ScalarSubquery sq -> throw new DialectCapability("a scalar subquery reached Deephaven: no"
                    + " shape yet");
            case SqlExpr.CheckedOne co -> throw new DialectCapability("a checked single value reached Deephaven: no"
                    + " shape yet");
            case SqlExpr.CheckedDefects cd -> throw new DialectCapability("checked defects reached Deephaven: no"
                    + " shape yet");
            case SqlExpr.CheckedChildValue cv -> throw new DialectCapability("a checked child's value reached"
                    + " Deephaven: no shape yet");
            case SqlExpr.InSubquery i -> throw new DialectCapability("an IN subquery reached Deephaven: no shape yet");
            case SqlExpr.Quantified q -> throw new DialectCapability("a quantified comparison reached Deephaven: no"
                    + " shape yet");
            case SqlExpr.CompactList cl -> throw new DialectCapability("a compacted list reached Deephaven: no shape"
                    + " yet");
            case SqlExpr.DeferredTdsString d -> throw new IllegalStateException("deferred relation-toString reached"
                    + " the renderer: the execution boundary resolves it first (id " + d.id() + ")");
            case SqlExpr.WindowCall w -> throw new DialectCapability("a window function (" + w.fn().fn()
                    + ") reached Deephaven: its updateBy shape is not built yet");
            case SqlExpr.Lambda l -> throw new DialectCapability("a lambda reached Deephaven: no list functions"
                    + " yet");
            case SqlExpr.FoldCall f -> throw new DialectCapability("a fold reached Deephaven: no shape yet");
            case SqlExpr.JsonObject j -> throw new DialectCapability("a JSON object reached Deephaven, which holds no"
                    + " JSON");
            case SqlExpr.JsonArrayAgg j -> throw new DialectCapability("a JSON array aggregate reached Deephaven,"
                    + " which holds no JSON");
            case SqlExpr.KeyCell k -> throw new DialectCapability("a dynamic pivot's probe plan reached Deephaven:"
                    + " no shape yet");
            case SqlExpr.RowOrder r -> throw new DialectCapability("a physical row order reached Deephaven: no shape"
                    + " yet");
            case SqlExpr.ReduceCollection rc -> throw new DialectCapability("a collection reduction reached"
                    + " Deephaven: no shape yet");
            case SqlExpr.Membership m -> throw new DialectCapability("a list membership reached Deephaven: no shape"
                    + " yet");
            case SqlExpr.TempTableInSplice t -> throw new IllegalStateException("temp-table IN splice '"
                    + t.tempTableName() + "' reached an executable dialect: plan-text vocabulary only");
            case SqlExpr.StoredRead r -> throw new IllegalStateException("a stored read reached Deephaven, which"
                    + " reads none");
        };
    }

    /** A column's Deephaven name in the current select: a source column by its alias (or by its name, when one source
     *  has it), an output by its name. */
    private String column(SqlExpr.Column c) {
        Scope s = scope();
        if (c.table() != null) {
            Map<String, String> source = s.sources.get(c.table());
            String name = source == null ? null : source.get(c.name());
            if (name == null) {
                throw new IllegalStateException("column " + c.table() + "." + c.name() + " is not in the select's"
                        + " sources " + s.sources.keySet());
            }
            return name;
        }
        String found = null;
        for (Map<String, String> source : s.sources.values()) {
            String name = source.get(c.name());
            if (name != null) {
                if (found != null) {
                    throw new IllegalStateException("column " + c.name() + " is in more than one source");
                }
                found = name;
            }
        }
        if (found == null) {
            found = s.outputs.get(c.name());
        }
        if (found == null) {
            throw new IllegalStateException("column " + c.name() + " is not in the select's sources or outputs");
        }
        return found;
    }

    /** A plan parameter: its column in the parameters table, by its place among the statement's placeholders. */
    @Override
    protected SqlWriter placeholder(SqlWriter writer, SqlExpr.PlanParam p) {
        if (p.type() instanceof TypeFact.Typed t && t.type() instanceof SqlType.Array) {
            throw new DialectCapability("list parameter '" + p.name() + "' reached Deephaven: a list is bound as a"
                    + " column of the parameters table only as one value, not shaped yet");
        }
        return writer.bind(PARAMETER + writer.bound(), scalarBind(p));
    }

    /** A string in the formula language's backticks; what it cannot carry as written refused by name. */
    static String stringLiteral(String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '`' || c == '\\' || c == '"' || c < 0x20) {
                throw new DialectCapability("a string holding a backtick, a backslash, a double quote or a control"
                        + " character reached Deephaven: how its formula language carries one is not measured yet");
            }
        }
        return "`" + value + "`";
    }

    private static String intLiteral(SqlExpr.IntLit i) {
        return i.type() instanceof TypeFact.Typed t && t.type() == SqlType.Scalar.INTEGER
                ? Long.toString(i.value()) : i.value() + "L";
    }

    private static String doubleLiteral(double v) {
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            throw new DialectCapability("a non-finite float literal reached Deephaven: no shape yet");
        }
        return Double.toString(v);
    }

    /** The null of {@code fact}'s type: a primitive column's sentinel, an object column's typed {@code null}. */
    String typedNull(TypeFact fact, String what) {
        if (!(fact instanceof TypeFact.Typed t) || !(t.type() instanceof SqlType.Scalar s)) {
            throw new DialectCapability(what + " of no known type reached Deephaven, whose nulls are typed");
        }
        String type = scalarTypeName(s);
        return switch (type) {
            case "int" -> "NULL_INT";
            case "long" -> "NULL_LONG";
            case "double" -> "NULL_DOUBLE";
            case "Boolean" -> "NULL_BOOLEAN";
            default -> "(" + type + ") null";
        };
    }

    /** Whether {@code e} may be SQL NULL; uncertainty is the safe side ({@link TypeFact.Typed#nullable}). */
    static boolean nullable(SqlExpr e) {
        return !(e.type() instanceof TypeFact.Typed t) || t.nullable();
    }

    // ==================================================================
    // Conditions: SQL's three values in two-valued formulas
    // ==================================================================

    /** Whether {@code e} is a condition: a boolean composition, a comparison, a null test or a membership. */
    private static boolean isCondition(SqlExpr e) {
        return e instanceof SqlExpr.Call c && switch (c.fn()) {
            case AND, OR, NOT, EQUAL, NOT_EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL, IS_NULL, IS_NOT_NULL, IN,
                 STARTS_WITH, ENDS_WITH, NULL_SAFE_EQUAL, NULL_SAFE_NOT_EQUAL -> true;
            // a COALESCE of conditions (the lowering's null-safe membership: COALESCE(x IN (...), false))
            case COALESCE -> c.args().stream().allMatch(x -> isCondition(x) || x instanceof SqlExpr.BoolLit);
            default -> false;
        };
    }

    /** A condition as a nullable {@code Boolean} value: true when it is true, false when it is false, else null. Where
     *  it cannot be unknown, its "is true" form is the value. */
    private SqlWriter booleanValue(SqlWriter writer, SqlExpr e) {
        if (!mayBeUnknown(e)) {
            return truth(writer, e);
        }
        writer.append("(");
        truth(writer, e).append(" ? true : (");
        return falsity(writer, e).append(" ? false : NULL_BOOLEAN))");
    }

    /** Whether a condition can be neither true nor false. */
    private static boolean mayBeUnknown(SqlExpr e) {
        if (e instanceof SqlExpr.Group g) {
            return mayBeUnknown(g.inner());
        }
        if (e instanceof SqlExpr.Call c) {
            return switch (c.fn()) {
                case IS_NULL, IS_NOT_NULL, NULL_SAFE_EQUAL, NULL_SAFE_NOT_EQUAL -> false;
                case AND, OR, NOT -> c.args().stream().anyMatch(Deephaven::mayBeUnknown);
                case COALESCE -> c.args().stream().allMatch(Deephaven::mayBeUnknown);
                default -> c.args().stream().anyMatch(Deephaven::nullable);
            };
        }
        return nullable(e);
    }

    /** {@code e} is TRUE, as a two-valued formula: what a filter keeps and a CASE branch takes. */
    SqlWriter truth(SqlWriter writer, SqlExpr e) {
        return condition(writer, e, true);
    }

    /** {@code e} is FALSE, as a two-valued formula. */
    SqlWriter falsity(SqlWriter writer, SqlExpr e) {
        return condition(writer, e, false);
    }

    private SqlWriter condition(SqlWriter writer, SqlExpr e, boolean holds) {
        String computed = scope().computed.get(e);
        if (computed != null) {
            return booleanColumn(writer, computed, nullable(e), holds);
        }
        return switch (e) {
            case SqlExpr.Group g -> condition(writer, g.inner(), holds);
            case SqlExpr.BoolLit b -> writer.append(b.value() == holds ? "true" : "false");
            case SqlExpr.NullLit n -> writer.append("false");
            case SqlExpr.Column c -> booleanColumn(writer, column(c), nullable(c), holds);
            case SqlExpr.Call c -> conditionCall(writer, c, holds);
            default -> throw new DialectCapability("a condition of shape " + e.getClass().getSimpleName()
                    + " reached Deephaven: no shape yet");
        };
    }

    private static SqlWriter booleanColumn(SqlWriter writer, String name, boolean nullable, boolean holds) {
        if (!nullable) {
            return writer.append(holds ? name : "!" + name);
        }
        return writer.append("(!isNull(" + name + ") && " + (holds ? "" : "!") + name + ")");
    }

    private SqlWriter conditionCall(SqlWriter writer, SqlExpr.Call c, boolean holds) {
        List<SqlExpr> a = c.args();
        return switch (c.fn()) {
            // TRUE(x AND y) = TRUE(x) && TRUE(y); FALSE(x AND y) = FALSE(x) || FALSE(y); OR dually
            case AND -> junction(writer, a, holds, holds ? " && " : " || ");
            case OR -> junction(writer, a, holds, holds ? " || " : " && ");
            case NOT -> condition(writer, a.get(0), !holds);
            case IS_NULL -> writer.append(holds ? "isNull(" : "!isNull(").expr(a.get(0), 0).append(")");
            case IS_NOT_NULL -> writer.append(holds ? "!isNull(" : "isNull(").expr(a.get(0), 0).append(")");
            case EQUAL -> comparison(writer, a, holds ? " == " : " != ");
            case NOT_EQUAL -> comparison(writer, a, holds ? " != " : " == ");
            case LESS -> comparison(writer, a, holds ? " < " : " >= ");
            case LESS_EQUAL -> comparison(writer, a, holds ? " <= " : " > ");
            case GREATER -> comparison(writer, a, holds ? " > " : " <= ");
            case GREATER_EQUAL -> comparison(writer, a, holds ? " >= " : " < ");
            case IN -> membership(writer, a, holds);
            // a null is equal to a null and to nothing else: two-valued, never unknown
            case NULL_SAFE_EQUAL -> nullSafeEqual(writer, a, holds);
            case NULL_SAFE_NOT_EQUAL -> nullSafeEqual(writer, a, !holds);
            case COALESCE -> coalesceCondition(writer, a, 0, holds);
            case STARTS_WITH -> stringTest(writer, a, "startsWith", holds);
            case ENDS_WITH -> stringTest(writer, a, "endsWith", holds);
            default -> throw new DialectCapability("a condition " + c.fn() + " reached Deephaven: no shape yet");
        };
    }

    /** COALESCE over conditions: it holds as its first argument holds, or, that argument being unknown (neither true nor
     *  false), as the rest's coalesce holds. */
    private SqlWriter coalesceCondition(SqlWriter writer, List<SqlExpr> a, int from, boolean holds) {
        SqlExpr first = a.get(from);
        if (from == a.size() - 1 || !mayBeUnknown(first)) {
            return condition(writer, first, holds);
        }
        if (from == a.size() - 2 && a.get(from + 1) instanceof SqlExpr.BoolLit last) {
            // COALESCE(c, b): c's own answer, an unknown c answering b -- so TRUE is TRUE(c) when b is false and
            // "not FALSE(c)" when b is true; FALSE dually
            if (last.value() == holds) {
                writer.append("!");
                return condition(writer, first, !holds);
            }
            return condition(writer, first, holds);
        }
        writer.append("(");
        condition(writer, first, holds).append(" || (!");
        condition(writer, first, true).append(" && !");
        condition(writer, first, false).append(" && ");
        return coalesceCondition(writer, a, from + 1, holds).append("))");
    }

    private SqlWriter junction(SqlWriter writer, List<SqlExpr> a, boolean holds, String op) {
        writer.append("(");
        for (int i = 0; i < a.size(); i++) {
            if (i > 0) {
                writer.append(op);
            }
            condition(writer, a.get(i), holds);
        }
        return writer.append(")");
    }

    /** The operands' null guard, then the comparison: a comparison with a null operand is neither true nor false. */
    private SqlWriter comparison(SqlWriter writer, List<SqlExpr> a, String op) {
        writer.append("(");
        guard(writer, a);
        return writer.expr(a.get(0), 0).append(op).expr(a.get(1), 0).append(")");
    }

    private SqlWriter guard(SqlWriter writer, List<SqlExpr> operands) {
        for (SqlExpr x : operands) {
            if (nullable(x)) {
                writer.append("!isNull(").expr(x, 0).append(") && ");
            }
        }
        return writer;
    }

    /** {@code x IN (v1, ...)} over non-null values: true when x equals one, false when it equals none; a null x
     *  neither. A null among the values is refused (its unknown is not shaped). */
    private SqlWriter membership(SqlWriter writer, List<SqlExpr> a, boolean holds) {
        SqlExpr x = a.get(0);
        List<SqlExpr> values = a.subList(1, a.size());
        for (SqlExpr v : values) {
            if (nullable(v)) {
                throw new DialectCapability("an IN list with a value that may be null reached Deephaven: no shape"
                        + " yet");
            }
        }
        writer.append("(");
        guard(writer, List.of(x));
        writer.append("(");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                writer.append(holds ? " || " : " && ");
            }
            writer.expr(x, 0).append(holds ? " == " : " != ").expr(values.get(i), 0);
        }
        return writer.append("))");
    }

    /** {@code a IS NOT DISTINCT FROM b} ({@code equal}) or its negation: both null, or neither null and equal. An
     *  operand that cannot be null drops its half of the test. */
    private SqlWriter nullSafeEqual(SqlWriter writer, List<SqlExpr> a, boolean equal) {
        SqlExpr x = a.get(0);
        SqlExpr y = a.get(1);
        if (!nullable(x) && !nullable(y)) {
            return writer.append("(").expr(x, 0).append(equal ? " == " : " != ").expr(y, 0).append(")");
        }
        if (!nullable(x) || !nullable(y)) {
            // one side is never null: equal is "the other is not null and equal"; unequal is "null or unequal"
            SqlExpr maybe = nullable(x) ? x : y;
            writer.append(equal ? "(!isNull(" : "(isNull(").expr(maybe, 0).append(equal ? ") && " : ") || ");
            return writer.expr(x, 0).append(equal ? " == " : " != ").expr(y, 0).append(")");
        }
        writer.append(equal ? "((isNull(" : "!((isNull(").expr(x, 0).append(") && isNull(").expr(y, 0)
                .append(")) || (!isNull(").expr(x, 0).append(") && !isNull(").expr(y, 0).append(") && ");
        return writer.expr(x, 0).append(" == ").expr(y, 0).append("))");
    }

    private SqlWriter stringTest(SqlWriter writer, List<SqlExpr> a, String method, boolean holds) {
        writer.append("(");
        guard(writer, a);
        writer.append(holds ? "" : "!");
        return writer.expr(a.get(0), 0).append("." + method + "(").expr(a.get(1), 0).append("))");
    }

    // ==================================================================
    // Filters: what the server reads without compiling (DH25)
    // ==================================================================

    /**
     * A filter's condition as the filter list Deephaven ANDs (docs/DEEPHAVEN_PLAN_2026_10_22.md DH25): its conjunction
     * split, one entry a conjunct, and each comparison of a column with a literal written as the server's filter parser
     * reads it with no compiling -- {@code col == v}, {@code col != v}, {@code col < v} (and its kin), {@code col in v1,
     * v2} and {@code col not in v1, v2}, bare, as a match or range filter. Any other entry is a formula, which the server
     * compiles to a Java class once for each distinct text: measured on Deephaven 42.7 (2026-10-23), a new literal in a
     * formula cost a filter 240 ms of compiling on the server, written bare 0 -- every value a person filters on, every
     * group a tree opens, was a compile. A comparison's null guard is its own entry, a formula that names only its
     * column, so it is compiled once a column.
     */
    SqlWriter filters(SqlWriter writer, SqlExpr condition) {
        List<Map.Entry<SqlExpr, Boolean>> conjuncts = new java.util.ArrayList<>();
        conjuncts(condition, true, conjuncts);
        // each column's guard written once: the lowering states some itself ("x is not null and x > v")
        java.util.Set<String> guarded = new java.util.LinkedHashSet<>();
        boolean first = true;
        for (Map.Entry<SqlExpr, Boolean> c : conjuncts) {
            String notNull = notNull(c.getKey(), c.getValue());
            Bare bare = notNull == null ? bare(c.getKey(), c.getValue()) : null;
            String guard = notNull != null ? notNull : bare != null && bare.guard() ? bare.column() : null;
            if (guard != null && guarded.add(guard)) {
                writer.append(first ? "\"!isNull(" : ", \"!isNull(").append(guard).append(")\"");
                first = false;
            }
            if (notNull != null) {
                continue;
            }
            writer.append(first ? "\"" : ", \"");
            first = false;
            if (bare != null) {
                writer.append(bare.column() + bare.op() + bare.values());
            } else {
                condition(writer, c.getKey(), c.getValue());
            }
            writer.append("\"");
        }
        return writer;
    }

    /** The column "{@code e} holds as {@code holds}" says is not null ({@code x IS NOT NULL}, or FALSE of
     *  {@code x IS NULL}), by its name; else null. */
    private @com.legend.base.Nullable String notNull(SqlExpr e, boolean holds) {
        if (e instanceof SqlExpr.Group g) {
            return notNull(g.inner(), holds);
        }
        return e instanceof SqlExpr.Call c && scope().computed.get(e) == null
                && (c.fn() == SqlFn.IS_NOT_NULL && holds || c.fn() == SqlFn.IS_NULL && !holds)
                && c.args().get(0) instanceof SqlExpr.Column x && scope().computed.get(x) == null ? column(x) : null;
    }

    /** The conjuncts of "{@code e} holds as {@code holds}": TRUE(a AND b) is TRUE(a) and TRUE(b), FALSE(a OR b) is
     *  FALSE(a) and FALSE(b), and a NOT turns which one. */
    private void conjuncts(SqlExpr e, boolean holds, List<Map.Entry<SqlExpr, Boolean>> out) {
        if (scope().computed.get(e) == null) {
            if (e instanceof SqlExpr.Group g) {
                conjuncts(g.inner(), holds, out);
                return;
            }
            if (e instanceof SqlExpr.Call c && (c.fn() == SqlFn.AND && holds || c.fn() == SqlFn.OR && !holds)) {
                for (SqlExpr a : c.args()) {
                    conjuncts(a, holds, out);
                }
                return;
            }
            if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.NOT) {
                conjuncts(c.args().get(0), !holds, out);
                return;
            }
        }
        out.add(Map.entry(e, holds));
    }

    /** A filter entry the server reads with no compiling: {@code column + op + values}, after a null guard on the
     *  column when {@code guard}. */
    private record Bare(String column, String op, String values, boolean guard) {
    }

    /** "{@code e} holds as {@code holds}" as a bare filter entry, or null when it has no bare form. */
    private @com.legend.base.Nullable Bare bare(SqlExpr e, boolean holds) {
        if (e instanceof SqlExpr.Group g) {
            return bare(g.inner(), holds);
        }
        if (!(e instanceof SqlExpr.Call c) || scope().computed.get(e) != null) {
            return null;
        }
        List<SqlExpr> a = c.args();
        return switch (c.fn()) {
            // a match filter keeps no null: TRUE(x == v) needs no guard; an inverted one keeps every null: TRUE(x != v)
            // and FALSE(x == v) need one, and the null-safe forms, whose null is unequal to v, none
            case EQUAL -> compared(a, holds ? " == " : " != ", !holds);
            case NOT_EQUAL -> compared(a, holds ? " != " : " == ", holds);
            case NULL_SAFE_EQUAL -> compared(a, holds ? " == " : " != ", false);
            case NULL_SAFE_NOT_EQUAL -> compared(a, holds ? " != " : " == ", false);
            // a range filter's nulls are guarded off either way
            case LESS -> compared(a, holds ? " < " : " >= ", true);
            case LESS_EQUAL -> compared(a, holds ? " <= " : " > ", true);
            case GREATER -> compared(a, holds ? " > " : " <= ", true);
            case GREATER_EQUAL -> compared(a, holds ? " >= " : " < ", true);
            // FALSE(x IN vs): not null and none of them
            case IN -> listed(a, holds ? " in " : " not in ", !holds);
            case COALESCE -> a.size() == 2 && a.get(1) instanceof SqlExpr.BoolLit b && !b.value()
                    ? orFalse(a.get(0), holds) : null;
            default -> null;
        };
    }

    /** COALESCE(c, false), the lowering's null-safe membership (nested, at times): TRUE is c's TRUE; FALSE is c's
     *  FALSE where c is never unknown, and where c is {@code x IN vs}, "a null x or none of them" -- the inverted
     *  match, which keeps every null. */
    private @com.legend.base.Nullable Bare orFalse(SqlExpr c, boolean holds) {
        if (holds || !mayBeUnknown(c)) {
            return bare(c, holds);
        }
        SqlExpr inner = c instanceof SqlExpr.Group g ? g.inner() : c;
        return inner instanceof SqlExpr.Call in && in.fn() == SqlFn.IN && scope().computed.get(in) == null
                ? listed(in.args(), " not in ", false) : null;
    }

    /** A column and a literal compared, either way round (a literal first is mirrored); {@code guard}: the column's
     *  nulls are filtered off first, when it may hold one. */
    private @com.legend.base.Nullable Bare compared(List<SqlExpr> a, String op, boolean guard) {
        for (int side = 0; side < 2; side++) {
            SqlExpr x = a.get(side);
            String column = bareColumn(x);
            String value = column == null ? null : bareLiteral(a.get(1 - side), x);
            if (column != null && value != null) {
                return new Bare(column, side == 0 ? op : mirrored(op), value, guard && nullable(x));
            }
        }
        return null;
    }

    private static String mirrored(String op) {
        return switch (op) {
            case " < " -> " > ";
            case " <= " -> " >= ";
            case " > " -> " < ";
            case " >= " -> " <= ";
            default -> op;
        };
    }

    /** {@code x in v1, v2}: every value a literal of the column's kind. The server splits the list at its commas, so
     *  a string holding one has no bare form. */
    private @com.legend.base.Nullable Bare listed(List<SqlExpr> a, String op, boolean guard) {
        String column = bareColumn(a.get(0));
        if (column == null) {
            return null;
        }
        StringBuilder values = new StringBuilder();
        for (SqlExpr v : a.subList(1, a.size())) {
            String value = bareLiteral(v, a.get(0));
            if (value == null || v instanceof SqlExpr.StringLit s && s.value().indexOf(',') >= 0) {
                return null;
            }
            values.append(values.isEmpty() ? "" : ", ").append(value);
        }
        return new Bare(column, op, values.toString(), guard && nullable(a.get(0)));
    }

    /** A column of a kind the server's filter parser compares with a literal, by its name; else null. */
    private @com.legend.base.Nullable String bareColumn(SqlExpr e) {
        // a name the server keeps for a row's place (i, ii, k), which its filter parser would not read as a column, is
        // never a column's: DeephavenJson refuses it
        return e instanceof SqlExpr.Column c && scope().computed.get(e) == null && bareKind(e) != null ? column(c) : null;
    }

    /** A literal as the server's filter parser reads one of {@code column}'s kind: an integer's digits (no
     *  {@code L}), a float's plain decimal with its point, a string in backticks, a boolean; else null. */
    private static @com.legend.base.Nullable String bareLiteral(SqlExpr e, SqlExpr column) {
        SqlType.Scalar kind = bareKind(column);
        if (e instanceof SqlExpr.IntLit i && (kind == SqlType.Scalar.INTEGER || kind == SqlType.Scalar.BIGINT)) {
            return Long.toString(i.value());
        }
        if (e instanceof SqlExpr.FloatLit f && kind == SqlType.Scalar.DOUBLE && Double.isFinite(f.value())) {
            String plain = java.math.BigDecimal.valueOf(f.value()).toPlainString();
            return plain.indexOf('.') >= 0 ? plain : plain + ".0";
        }
        if (e instanceof SqlExpr.StringLit s && kind == SqlType.Scalar.VARCHAR) {
            return stringLiteral(s.value());
        }
        if (e instanceof SqlExpr.BoolLit b && kind == SqlType.Scalar.BOOLEAN) {
            return b.value() ? "true" : "false";
        }
        return null;
    }

    /** The kinds the bare forms compare: whole numbers, floats, strings, booleans. */
    private static @com.legend.base.Nullable SqlType.Scalar bareKind(SqlExpr e) {
        if (e.type() instanceof TypeFact.Typed t && t.type() instanceof SqlType.Scalar s) {
            return switch (s) {
                case INTEGER, BIGINT, DOUBLE, VARCHAR, BOOLEAN -> s;
                default -> null;
            };
        }
        return null;
    }

    // ==================================================================
    // Functions
    // ==================================================================

    @Override
    protected SqlWriter call(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        List<SqlExpr> a = c.args();
        return switch (c.fn()) {
            case AND, OR, NOT, EQUAL, NOT_EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL, IS_NULL, IS_NOT_NULL, IN,
                 STARTS_WITH, ENDS_WITH -> booleanValue(writer, c);
            // the formula language's arithmetic is null-propagating over the primitive columns' sentinels (to
            // measure on the lanes, DH7); a division of integers answers a double, as DIVIDE asks
            case PLUS -> infixArithmetic(writer, a, " + ");
            case MINUS -> infixArithmetic(writer, a, " - ");
            case TIMES -> infixArithmetic(writer, a, " * ");
            case DIVIDE -> infixArithmetic(writer, a, " / ");
            case NEGATE -> writer.append("(-").expr(a.get(0), 0).append(")");
            case ABS -> function(writer, "abs", a);
            case SQRT -> function(writer, "sqrt", a);
            case EXP -> function(writer, "exp", a);
            case LN -> function(writer, "log", a);
            case LOG10 -> function(writer, "log10", a);
            case POW -> function(writer, "pow", a);
            case FLOOR -> function(writer, "floor", a);
            case CEILING -> function(writer, "ceil", a);
            case SIGN -> function(writer, "signum", a);
            case SIN -> function(writer, "sin", a);
            case COS -> function(writer, "cos", a);
            case TAN -> function(writer, "tan", a);
            case ASIN -> function(writer, "asin", a);
            case ACOS -> function(writer, "acos", a);
            case ATAN -> function(writer, "atan", a);
            case COALESCE -> coalesce(writer, a, 0);
            case CONCAT -> concat(writer, c);
            case UPPER -> stringMethod(writer, c, "toUpperCase");
            case LOWER -> stringMethod(writer, c, "toLowerCase");
            case LENGTH -> stringMethod(writer, c, "length");
            case MOD, REM, PARSE_INT, GREATEST, LEAST, CONCAT_JOIN, LIST_FILTER, LIST_TRANSFORM, LIST_CONCAT,
                 LIST_GET, LIST_POSITION, LIST_EXISTS, LIST_FOR_ALL, STRUCT_INSERT, UNNEST, LIST_FLATTEN,
                 JSON_MERGE_PATCH, CBRT, PI, ATAN2, SINH, COSH, TANH, ROUND, XOR, BIT_AND, BIT_OR, BIT_XOR,
                 BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT, BIT_NOT, REGEXP_EXTRACT, REGEXP_EXTRACT_ALL, REGEXP_REPLACE,
                 REGEXP_FULL_MATCH, MAP_FROM_LISTS, MAP_FROM_ENTRIES, MAP_EMPTY, MAP_EXTRACT, MAP_KEYS, MAP_VALUES,
                 MAP_CONCAT, SUBSTRING, STRPOS, MATCHES, LEFT, RIGHT, LPAD, RPAD, TRIM, LTRIM, RTRIM, REPLACE,
                 SPLIT, SPLIT_PART, TRANSLATE, PURE_SPLIT_PART, REVERSE_STRING, ASCII_CODE, CHR, UC_FIRST, LC_FIRST,
                 ENCODE_BASE64, LEVENSHTEIN, GUID, FORMAT, HASH, MD5, SHA1, SHA256, RANDOM, EXTRACT, TODAY, NOW,
                 DATE_TRUNC_DAY, MAKE_DATE, MAKE_TIMESTAMP, DATE_TRUNC, ADD_INTERVAL, DATE_DIFF, TIME_BUCKET,
                 TIMEZONE, DATE_AT_TIME, EPOCH_NS, FROM_EPOCH_NS, ADD_INTERVAL_TEMPORAL, EPOCH_SECONDS, EPOCH_MS,
                 FROM_EPOCH_SECONDS, FROM_EPOCH_MS, DAYNAME, MONTHNAME, COT, RADIANS, DEGREES, INT_DIVIDE,
                 REPEAT_STR, JARO_WINKLER, DECODE_BASE64, CURRENT_USER_FN, LIST_LENGTH, LIST_ZIP, LIST_DISTINCT,
                 LIST_APPEND, LIST_SUM, LIST_MIN, LIST_MAX, LIST_AVG, LIST_MEDIAN, LIST_MODE, LIST_SORT,
                 LIST_SORT_DESC, LIST_TAIL, LIST_INIT, RANGE_FN, LIST_PRODUCT, LIST_REDUCE, LIST_SLICE, REPEAT_VALUE,
                 LIST_BOOL_AND, LIST_BOOL_OR, IS_DISTINCT_FROM, ALL_DISTINCT, NULL_SAFE_EQUAL, NULL_SAFE_NOT_EQUAL,
                 STRFTIME, STRPTIME, PARSE_DATE, LIST_REVERSE, TYPEOF, ROUND_HALF_UP, ERROR, FLOOR_RAW, TO_VARIANT,
                 BOOL_TO_TEXT, FLOAT_CANON, FLOAT_PRINT, FLOAT_PRINT_ENGINE, VARIANT_ELEMENTS, VARIANT_GET,
                 JSON_TYPE, JSON_ARRAY_LENGTH, JSON_PRETTY ->
                    throw new DialectCapability("function " + c.fn() + " reached Deephaven: no shape yet");
        };
    }

    private SqlWriter infixArithmetic(SqlWriter writer, List<SqlExpr> a, String op) {
        writer.append("(");
        for (int i = 0; i < a.size(); i++) {
            if (i > 0) {
                writer.append(op);
            }
            writer.expr(a.get(i), 0);
        }
        return writer.append(")");
    }

    private SqlWriter function(SqlWriter writer, String name, List<SqlExpr> a) {
        writer.append(name).append("(");
        for (int i = 0; i < a.size(); i++) {
            if (i > 0) {
                writer.append(", ");
            }
            writer.expr(a.get(i), 0);
        }
        return writer.append(")");
    }

    private SqlWriter coalesce(SqlWriter writer, List<SqlExpr> a, int from) {
        if (from == a.size() - 1) {
            return writer.expr(a.get(from), 0);
        }
        writer.append("(!isNull(").expr(a.get(from), 0).append(") ? ").expr(a.get(from), 0).append(" : ");
        return coalesce(writer, a, from + 1).append(")");
    }

    /** {@code a + b} over strings, null when one is: the language's {@code +} would write {@code "null"}. */
    private SqlWriter concat(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        boolean guarded = a.stream().anyMatch(Deephaven::nullable);
        writer.append("(");
        if (guarded) {
            writer.append("(");
            boolean first = true;
            for (SqlExpr x : a) {
                if (nullable(x)) {
                    writer.append(first ? "" : " || ").append("isNull(").expr(x, 0).append(")");
                    first = false;
                }
            }
            writer.append(") ? ").append(typedNull(c.type(), "a concatenation")).append(" : ");
        }
        writer.append("(");
        for (int i = 0; i < a.size(); i++) {
            if (i > 0) {
                writer.append(" + ");
            }
            writer.expr(a.get(i), 0);
        }
        return writer.append("))");
    }

    /** A string's method, null when the string is: a method of a null string fails the operation. */
    private SqlWriter stringMethod(SqlWriter writer, SqlExpr.Call c, String method) {
        SqlExpr s = c.args().get(0);
        if (nullable(s)) {
            writer.append("(isNull(").expr(s, 0).append(") ? ").append(typedNull(c.type(), "a string's " + method))
                    .append(" : ");
            return writer.expr(s, 0).append("." + method + "())");
        }
        return writer.expr(s, 0).append("." + method + "()");
    }

    // ==================================================================
    // CASE and casts
    // ==================================================================

    /** {@code CASE WHEN c THEN x ... ELSE y END} as a chain of ternaries: a branch is taken when its condition is
     *  TRUE, as SQL takes it; no ELSE is the CASE's typed null. */
    @Override
    protected SqlWriter caseExpr(SqlWriter writer, SqlExpr.Case c) {
        writer.append("(");
        for (SqlExpr.Case.When w : c.whens()) {
            truth(writer, w.condition()).append(" ? ");
            branch(writer, w.then(), c).append(" : ");
        }
        if (c.otherwise() == null) {
            writer.append(typedNull(c.type(), "a CASE with no ELSE"));
        } else {
            branch(writer, c.otherwise(), c);
        }
        return writer.append(")");
    }

    /** A CASE's branch: a null of no type of its own is the CASE's typed null (a missing ELSE, {@code []}). */
    private SqlWriter branch(SqlWriter writer, SqlExpr e, SqlExpr.Case c) {
        if (e instanceof SqlExpr.NullLit n && !(n.type() instanceof TypeFact.Typed)) {
            return writer.append(typedNull(c.type(), "a CASE's null"));
        }
        return writer.expr(e, 0);
    }

    /** A numeric widening cast, a null kept null (a primitive cast of a null sentinel would answer a number). */
    private SqlWriter cast(SqlWriter writer, SqlExpr.Cast c) {
        if (!(c.target() instanceof SqlType.Scalar target)) {
            throw new DialectCapability("a cast to " + c.target() + " reached Deephaven: no shape yet");
        }
        String primitive = switch (target) {
            case DOUBLE -> "double";
            case BIGINT -> "long";
            default -> throw new DialectCapability("a cast to " + target + " reached Deephaven: no shape yet");
        };
        if (!(c.value().type() instanceof TypeFact.Typed from) || !(from.type() instanceof SqlType.Scalar f)
                || !(f == SqlType.Scalar.INTEGER || f == SqlType.Scalar.BIGINT || f == SqlType.Scalar.DOUBLE)) {
            throw new DialectCapability("a cast of a non-numeric value to " + target + " reached Deephaven: no shape"
                    + " yet");
        }
        if (nullable(c.value())) {
            writer.append("(isNull(").expr(c.value(), 0).append(") ? ").append(typedNull(c.type(), "a cast"))
                    .append(" : ");
            return writer.append("(" + primitive + ") ").expr(c.value(), 0).append(")");
        }
        return writer.append("((" + primitive + ") ").expr(c.value(), 0).append(")");
    }

    // ==================================================================
    // Aggregates: read back from the group's columns
    // ==================================================================

    /**
     * An aggregate's Deephaven aggregation, written into the group's {@code aggregate} operation
     * ({@link DeephavenBatch}): computed into its column from its argument's ({@link Scope#aggregates}). Read in a
     * formula, an aggregate is its column ({@link Scope#computed}), so this is reached only by the group. SQL's
     * {@code COUNT(x)} counts the rows where x is not null; {@code COUNT(*)} every row.
     */
    @Override
    protected SqlWriter reducer(SqlWriter writer, SqlAgg.Reducer r) {
        String[] names = scope().aggregates.get(r);
        if (names == null) {
            throw new IllegalStateException("aggregate " + r.fn() + " reached Deephaven outside its group");
        }
        if (!r.orderBy().isEmpty()) {
            throw new DialectCapability("an ordered aggregate (" + r.fn() + ") reached Deephaven: no shape yet");
        }
        String out = names[0];
        String pair = DeephavenJson.strings(List.of(out + "=" + names[1]));
        String aggregation = switch (r.fn()) {
            case COUNT -> count(r, out, names[1], pair);
            case SUM -> columns(r, "{\"sum\": {}}", pair);
            case AVG -> columns(r, "{\"avg\": {}}", pair);
            case MIN -> columns(r, "{\"min\": {}}", pair);
            case MAX -> columns(r, "{\"max\": {}}", pair);
            case STDDEV_SAMP, STDDEV -> columns(r, "{\"std\": {}}", pair);
            case VAR_SAMP, VARIANCE -> columns(r, "{\"var\": {}}", pair);
            case MEDIAN -> columns(r, "{\"median\": {\"averageEvenlyDivided\": true}}", pair);
            case ANY_VALUE -> columns(r, "{\"first\": {}}", pair);
            case STDDEV_POP, VAR_POP, MODE, STRING_AGG, LIST, QUANTILE_CONT, QUANTILE_DISC, CORR, COVAR_SAMP,
                 COVAR_POP, ARG_MAX, ARG_MIN, BOOL_AND, BOOL_OR -> throw new DialectCapability("aggregate " + r.fn()
                    + " reached Deephaven: no shape yet");
            case WAVG, HASH_LIST, IS_DISTINCT_MARK, UNIQUE_VALUE_ONLY -> throw loweringMarker(r);
            case ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE,
                 NTH_VALUE -> throw windowFunction(r);
        };
        return writer.append(aggregation);
    }

    /** SQL's COUNT: every row ({@code COUNT(*)}, or a value never null), the rows whose value is not null, or the
     *  distinct values that are not null. */
    private static String count(SqlAgg.Reducer r, String out, String arg, String pair) {
        if (r.distinct()) {
            return "{\"columns\": {\"spec\": {\"countDistinct\": {\"countNulls\": false}}, \"matchPairs\": "
                    + pair + "}}";
        }
        if (r.args().isEmpty() || !nullable(r.args().get(0))) {
            return "{\"count\": {\"columnName\": \"" + out + "\"}}";
        }
        return "{\"countWhere\": {\"columnName\": \"" + out + "\", \"filters\": "
                + DeephavenJson.strings(List.of("!isNull(" + arg + ")")) + "}}";
    }

    /** An aggregation over one column; a distinct one refused (only COUNT's is shaped). */
    private static String columns(SqlAgg.Reducer r, String spec, String pair) {
        if (r.distinct()) {
            throw new DialectCapability("a distinct " + r.fn() + " reached Deephaven: no shape yet");
        }
        return "{\"columns\": {\"spec\": " + spec + ", \"matchPairs\": " + pair + "}}";
    }

    // ==================================================================
    // Names, types and the statements Deephaven has no SQL for
    // ==================================================================

    @Override
    public String physicalName(String name) {
        return DeephavenJson.name(name, "a name");
    }

    @Override
    public String storedName(String name, boolean declaredQuoted) {
        return DeephavenJson.name(name, "a column");
    }

    @Override
    public String insertColumnName(String name, boolean declaredQuoted) {
        return DeephavenJson.name(name, "a column");
    }

    /** {@inheritDoc} A table is created as an input table (DH15): the plan's DDL writes {@link DeephavenBatch#ddl}. */
    @Override
    public String render(com.legend.sql.SqlDdl ddl) {
        return DeephavenBatch.ddl(ddl);
    }

    /** {@inheritDoc} Rows are written to an input table by the write step (§6), not built yet. */
    @Override
    public String render(com.legend.sql.SqlDml dml) {
        throw new DialectCapability("a row-writing statement reached Deephaven: its input tables' writes are the"
                + " write step's (docs/DEEPHAVEN_PLAN_2026_10_22.md §6), not built yet");
    }

    @Override
    protected String jsonKindName(com.legend.sql.JsonKind kind) {
        throw new DialectCapability("a JSON kind reached Deephaven, which holds no JSON");
    }

    @Override
    protected String typeofName(SqlType.Scalar type) {
        throw new DialectCapability("a type name reached Deephaven: no shape yet");
    }

    /** {@inheritDoc} The column type a Deephaven column of this scalar has (a primitive's, or a Java class's). */
    @Override
    protected String scalarTypeName(SqlType.Scalar s) {
        return switch (s) {
            case BOOLEAN -> "Boolean";
            case INTEGER -> "int";
            case BIGINT -> "long";
            case DOUBLE -> "double";
            case VARCHAR -> "String";
            case DATE -> "java.time.LocalDate";
            case TIMESTAMP -> "java.time.Instant";
            case TIME -> "java.time.LocalTime";
            case HUGEINT, TIMESTAMPTZ, JSON, LITERAL, TEMPORAL_TEXT, DECIMAL_TEXT -> throw new DialectCapability(
                    "a value of type " + s + " reached Deephaven: no column holds it yet");
        };
    }

    @Override
    protected String structTypeName(SqlType.Struct st) {
        throw new DialectCapability("a struct type reached Deephaven: no shape yet");
    }

    @Override
    protected String formatText(SqlExpr.FormatLit fl) {
        throw new DialectCapability("a date format reached Deephaven: no shape yet");
    }

    /** {@inheritDoc} A column's type is the typed null its input table starts from, written whole by
     *  {@link DeephavenBatch#ddl}: no SQL type name is written. */
    @Override
    protected String ddlType(com.legend.sql.SqlDdl.ColumnType t) {
        throw new IllegalStateException("a SQL column type was asked of Deephaven, whose DDL is DeephavenBatch's");
    }

    /** {@inheritDoc} A Deephaven server has no session-only table: a plan's metamodel rows are its own, inline. */
    @Override
    public boolean temporaryTables() {
        return false;
    }
}
