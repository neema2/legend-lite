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
    /** No lists, no JSON, no FULL join; its own as-of join (aj and raj, K7). */
    public static final CarrierStrategies.Caps CAPS = new CarrierStrategies.Caps(false, false, false, false, true);

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
        /**
         * The source columns the view being written has copied already, each to its copy's name ({@code d1 = t8__d1}):
         * a later formula of that view reads the copy, so its text names the column as the query does, not by its
         * source's place in the batch -- the same formula in every plan that computes it, which the server compiles
         * once (2026-10-26, the trading desk's options book: its chain's Black and Scholes, under other aliases than the
         * chain's own plan, compiled again, seconds a formula group).
         */
        final Map<String, String> copies = new LinkedHashMap<>();
    }

    private final Deque<Scope> scopes = new ArrayDeque<>();

    void pushScope(Scope s) {
        scopes.push(s);
    }

    void popScope() {
        scopes.pop();
    }

    /** The current select's columns computed by expression ({@link Scope#computed}), or null outside a select. */
    java.util.@com.legend.base.Nullable Map<SqlExpr, String> computedOrNull() {
        Scope s = scopes.peek();
        return s == null ? null : s.computed;
    }

    /** The current select's columns its view has copied so far ({@link Scope#copies}), or null outside a select. */
    java.util.@com.legend.base.Nullable Map<String, String> copiesOrNull() {
        Scope s = scopes.peek();
        return s == null ? null : s.copies;
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
            // a date-time literal: the instant its UTC wall-clock names (DH29), parsed by the server
            case SqlExpr.TimestampLit t -> writer.append("parseInstant(").append(stringLiteral(isoInstant(t.iso())))
                    .append(")");
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

    /** {@code e}'s Deephaven name when it is a column the select reads as it is, nothing computed in its place; else
     *  null. */
    @com.legend.base.Nullable String plainColumn(SqlExpr e) {
        return e instanceof SqlExpr.Column c && scope().computed.get(e) == null ? column(c) : null;
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
            return s.copies.getOrDefault(name, name);
        }
        String found = null;
        for (Map<String, String> source : s.sources.values()) {
            String name = source.get(c.name());
            if (name != null) {
                if (found != null) {
                    throw new IllegalStateException("column " + c.name() + " is in more than one source");
                }
                found = s.copies.getOrDefault(name, name);
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

    static String doubleLiteral(double v) {
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

    /** The library function reading the first or the last value ({@code fn}) of a group of {@code fact}'s type, the
     *  type's null when the group is empty: {@code first} of a primitive column's vector, {@code firstObj} of an object
     *  column's (DH33: a vector's own methods are not the validator's). */
    String ofGroup(TypeFact fact, String fn, String what) {
        if (!(fact instanceof TypeFact.Typed t) || !(t.type() instanceof SqlType.Scalar s)) {
            throw new DialectCapability(what + " of no known type reached Deephaven, whose groups are typed");
        }
        return switch (scalarTypeName(s)) {
            case "int", "long", "double" -> fn;
            default -> fn + "Obj";
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
            case SqlExpr.Case c -> caseCondition(writer, c, holds);
            default -> throw new DialectCapability("a condition of shape " + e.getClass().getSimpleName()
                    + " reached Deephaven: no shape yet");
        };
    }

    /** A boolean CASE as a condition (Pure's {@code if} of two conditions): TRUE when the branch it takes is TRUE,
     *  FALSE likewise; no ELSE takes a null, which is neither. */
    private SqlWriter caseCondition(SqlWriter writer, SqlExpr.Case c, boolean holds) {
        writer.append("(");
        for (SqlExpr.Case.When w : c.whens()) {
            truth(writer, w.condition()).append(" ? ");
            condition(writer, w.then(), holds).append(" : ");
        }
        if (c.otherwise() == null) {
            writer.append("false");
        } else {
            condition(writer, c.otherwise(), holds);
        }
        return writer.append(")");
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
            // a guard's error where a condition stands: the condition is never decided
            case ERROR -> raise(writer, c, SqlType.Scalar.BOOLEAN);
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
    private SqlWriter comparison(SqlWriter writer, List<SqlExpr> given, String op) {
        List<SqlExpr> a = asBooleans(given);
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
                writer.append(first ? "\"" : ", \"").append(notNullFilter(guard)).append("\"");
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

    /**
     * A PIVOT CELL'S ROWS (docs/DEEPHAVEN_PLAN_2026_10_22.md P1): {@code condition} as the columns it holds equal to a
     * literal -- DataCube's pivot cell, each pivot column equal to one of its values -- each value as the server's
     * filter parser reads it (a match filter: no compiling), by column; null when it is any other condition, or names
     * a column twice. A match filter keeps no null, as the cell's condition takes none: a null guard on a column it
     * matches is what the match keeps already.
     */
    java.util.@com.legend.base.Nullable SortedMap<String, String> pivotCell(SqlExpr condition) {
        List<Map.Entry<SqlExpr, Boolean>> conjuncts = new java.util.ArrayList<>();
        conjuncts(condition, true, conjuncts);
        java.util.SortedMap<String, String> cell = new java.util.TreeMap<>();
        java.util.Set<String> notNull = new java.util.HashSet<>();
        for (Map.Entry<SqlExpr, Boolean> c : conjuncts) {
            // a column's own null guard (the lowering states some itself): what its match keeps already
            String guarded = notNull(c.getKey(), c.getValue());
            if (guarded != null) {
                notNull.add(guarded);
                continue;
            }
            Bare bare = bare(c.getKey(), c.getValue());
            if (bare == null || !bare.op().equals(" == ") || bare.guard() || cell.put(bare.column(), bare.values()) != null) {
                return null;
            }
        }
        return cell.isEmpty() || !cell.keySet().containsAll(notNull) ? null : cell;
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

    /**
     * A filter entry keeping the rows where {@code column} is not null: an inverted match filter on null, which the server
     * reads with no compiling (P1: written {@code !isNull(column)}, a formula, the server compiled it once for each
     * column a filter guarded -- a first filter, a first join, a first count of a column, each 150-400 ms).
     */
    static String notNullFilter(String column) {
        return column + " != null";
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
    /**
     * Two compared operands, a boolean's 0 or 1 read as false or true: SQL compares a BIT column with a number (a store
     * filter's {@code IS_INTERPOLATED = 0}), where the server's Boolean equals no number (the stress corpus on Deephaven,
     * task 15: a curve's quoted pillars matched none).
     */
    private static List<SqlExpr> asBooleans(List<SqlExpr> a) {
        if (a.size() != 2) {
            return a;
        }
        List<SqlExpr> out = new java.util.ArrayList<>(a);
        for (int side = 0; side < 2; side++) {
            if (a.get(side).type() instanceof TypeFact.Typed t && t.type() == SqlType.Scalar.BOOLEAN
                    && a.get(1 - side) instanceof SqlExpr.IntLit n && (n.value() == 0 || n.value() == 1)) {
                out.set(1 - side, new SqlExpr.BoolLit(n.value() == 1));
            }
        }
        return out;
    }

    private @com.legend.base.Nullable Bare compared(List<SqlExpr> given, String op, boolean guard) {
        List<SqlExpr> a = asBooleans(given);
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
        if (e instanceof SqlExpr.WindowCall && bareKind(e) != null) {
            // a window is the column it was computed into (K6: a top-N's row number, filtered with no compiling)
            return scope().computed.get(e);
        }
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
        // a date-time and a date in quotes, as the server's filter parser reads them (K1: no compiling, DH25)
        if (e instanceof SqlExpr.TimestampLit t && kind == SqlType.Scalar.TIMESTAMP) {
            return "'" + isoInstant(t.iso()) + "'";
        }
        if (e instanceof SqlExpr.DateLit d && kind == SqlType.Scalar.DATE) {
            return "'" + d.iso() + "'";
        }
        return null;
    }

    /** The kinds the bare forms compare: whole numbers, floats, strings, booleans, date-times and dates. */
    private static @com.legend.base.Nullable SqlType.Scalar bareKind(SqlExpr e) {
        if (e.type() instanceof TypeFact.Typed t && t.type() instanceof SqlType.Scalar s) {
            return switch (s) {
                case INTEGER, BIGINT, DOUBLE, VARCHAR, BOOLEAN, TIMESTAMP, DATE -> s;
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
            case PLUS -> infixArithmetic(writer, c, " + ");
            case MINUS -> infixArithmetic(writer, c, " - ");
            case TIMES -> infixArithmetic(writer, c, " * ");
            case DIVIDE -> infixArithmetic(writer, c, " / ");
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
            // in doubles when it is a double's, or when its values are a BigDecimal and a double (bigDecimal says why)
            case COALESCE -> a.stream().anyMatch(Deephaven::bigDecimal)
                    && (c.type() instanceof TypeFact.Typed t && t.type() == SqlType.Scalar.DOUBLE
                        || !a.stream().allMatch(Deephaven::bigDecimal))
                    ? coalesceInDoubles(writer, a, 0) : coalesce(writer, a, 0);
            case CONCAT -> concat(writer, c);
            // the scalars a screen's computed column calls (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md §2, the stress
            // corpus's TF_TradeScalars): Pure's round is half-even (SqlFn's contract), as Java's rint is; mod is the
            // remainder with the divisor's sign, rem with the dividend's (Java's %); substring and strpos SQL's,
            // one-based, read through String's own methods (DH33)
            case ROUND -> round(writer, c);
            case MOD -> writer.append("(((").expr(a.get(0), 0).append(" % ").expr(a.get(1), 0).append(") + ")
                    .expr(a.get(1), 0).append(") % ").expr(a.get(1), 0).append(")");
            case REM -> writer.append("(").expr(a.get(0), 0).append(" % ").expr(a.get(1), 0).append(")");
            // K5: a whole-number quotient cut toward zero, and the platform's runtime error
            case INT_DIVIDE -> intDivide(writer, c);
            case FLOAT_PRINT, FLOAT_PRINT_ENGINE -> floatPrint(writer, c);
            case ERROR -> raise(writer, c, c.type(), "a runtime error");
            case SUBSTRING -> substring(writer, c);
            case STRPOS -> strpos(writer, c);
            case UPPER -> stringMethod(writer, c, "toUpperCase");
            case LOWER -> stringMethod(writer, c, "toLowerCase");
            case LENGTH -> stringMethod(writer, c, "length");
            // dates and times (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md DH29): each read in UTC
            case TIME_BUCKET -> timeBucket(writer, a);
            case EXTRACT -> extract(writer, a);
            case DATE_TRUNC_DAY -> asDate(writer, a.get(0), "datePart");
            case DATE_TRUNC -> dateTrunc(writer, a);
            case ADD_INTERVAL -> addInterval(writer, a);
            // K5's: dateDiff, and toEpochValue (a dateDiff from the epoch) -- its calendar parts SQL's crossings, its
            // hours, minutes and seconds the epoch milliseconds' whole-number quotient
            case DATE_DIFF -> dateDiff(writer, c);
            case EPOCH_MS -> epochMillis(writer, a.get(0));
            case NOW, TODAY -> throw new DialectCapability("the time now (" + c.fn() + ") reached Deephaven: a live"
                    + " view's rows are filtered when they change, not again as time passes, so a filter on the time"
                    + " now would keep a row it has aged out; not shaped until a live window is designed"
                    + " (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md DH30)");
            case PARSE_INT, GREATEST, LEAST, CONCAT_JOIN, LIST_FILTER, LIST_TRANSFORM, LIST_CONCAT,
                 LIST_GET, LIST_POSITION, LIST_EXISTS, LIST_FOR_ALL, STRUCT_INSERT, UNNEST, LIST_FLATTEN,
                 JSON_MERGE_PATCH, CBRT, PI, ATAN2, SINH, COSH, TANH, XOR, BIT_AND, BIT_OR, BIT_XOR,
                 BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT, BIT_NOT, REGEXP_EXTRACT, REGEXP_EXTRACT_ALL, REGEXP_REPLACE,
                 REGEXP_FULL_MATCH, MAP_FROM_LISTS, MAP_FROM_ENTRIES, MAP_EMPTY, MAP_EXTRACT, MAP_KEYS, MAP_VALUES,
                 MAP_CONCAT, MATCHES, LEFT, RIGHT, LPAD, RPAD, TRIM, LTRIM, RTRIM, REPLACE,
                 SPLIT, SPLIT_PART, TRANSLATE, PURE_SPLIT_PART, REVERSE_STRING, ASCII_CODE, CHR, UC_FIRST, LC_FIRST,
                 ENCODE_BASE64, LEVENSHTEIN, GUID, FORMAT, HASH, MD5, SHA1, SHA256, RANDOM,
                 MAKE_DATE, MAKE_TIMESTAMP,
                 TIMEZONE, DATE_AT_TIME, EPOCH_NS, FROM_EPOCH_NS, ADD_INTERVAL_TEMPORAL, EPOCH_SECONDS,
                 FROM_EPOCH_SECONDS, FROM_EPOCH_MS, DAYNAME, MONTHNAME, COT, RADIANS, DEGREES,
                 REPEAT_STR, JARO_WINKLER, DECODE_BASE64, CURRENT_USER_FN, LIST_LENGTH, LIST_ZIP, LIST_DISTINCT,
                 LIST_APPEND, LIST_SUM, LIST_MIN, LIST_MAX, LIST_AVG, LIST_MEDIAN, LIST_MODE, LIST_SORT,
                 LIST_SORT_DESC, LIST_TAIL, LIST_INIT, RANGE_FN, LIST_PRODUCT, LIST_REDUCE, LIST_SLICE, REPEAT_VALUE,
                 LIST_BOOL_AND, LIST_BOOL_OR, IS_DISTINCT_FROM, ALL_DISTINCT, NULL_SAFE_EQUAL, NULL_SAFE_NOT_EQUAL,
                 STRFTIME, STRPTIME, PARSE_DATE, LIST_REVERSE, TYPEOF, ROUND_HALF_UP, FLOOR_RAW, TO_VARIANT,
                 BOOL_TO_TEXT, FLOAT_CANON, VARIANT_ELEMENTS, VARIANT_GET,
                 JSON_TYPE, JSON_ARRAY_LENGTH, JSON_PRETTY ->
                    throw new DialectCapability("function " + c.fn() + " reached Deephaven: no shape yet");
        };
    }

    /**
     * Arithmetic in the formula language. A double's arithmetic over a DECIMAL column (a BigDecimal: DeephavenBatch's
     * DDL) takes the decimal as its own {@code doubleValue} first, as SQL does: a decimal over an integer is a DOUBLE
     * (DuckDB's {@code /}), computed in doubles -- Deephaven's own BigDecimal division keeps fewer digits (the stress
     * corpus on Deephaven, task 15: 1666666.66666667 where SQL answers 1666666.6666666667).
     */
    private SqlWriter infixArithmetic(SqlWriter writer, SqlExpr.Call c, String op) {
        List<SqlExpr> a = c.args();
        boolean inDoubles = c.type() instanceof TypeFact.Typed t && t.type() == SqlType.Scalar.DOUBLE;
        writer.append("(");
        for (int i = 0; i < a.size(); i++) {
            if (i > 0) {
                writer.append(op);
            }
            if (inDoubles) {
                asDouble(writer, a.get(i), nullable(a.get(i)));
            } else {
                writer.expr(a.get(i), 0);
            }
        }
        return writer.append(")");
    }

    /**
     * Whether {@code e} is WRITTEN as a BigDecimal: a DECIMAL column (the DDL's), arithmetic over one, a COALESCE whose
     * every value is one. A decimal the formula computes from no decimal column is a double (its literals are written
     * as doubles), and so is a COALESCE of a BigDecimal and a double -- the server has no condition of the two
     * ("Incompatible types in condition operation"), so it is written in doubles ({@link #coalesceInDoubles}), and
     * arithmetic over it is a double's.
     */
    static boolean bigDecimal(SqlExpr e) {
        if (!(e.type() instanceof TypeFact.Typed t) || !(t.type() instanceof SqlType.Decimal)) {
            return false;
        }
        if (e instanceof SqlExpr.Call c) {
            return switch (c.fn()) {
                case COALESCE -> c.args().stream().allMatch(Deephaven::bigDecimal);
                case PLUS, MINUS, TIMES, DIVIDE, NEGATE -> c.args().stream().anyMatch(Deephaven::bigDecimal);
                default -> readsDecimalColumn(e);
            };
        }
        return readsDecimalColumn(e);
    }

    /** {@code e} as a double: a BigDecimal by its own {@code doubleValue} (a null kept null when it {@code mayBeNull}),
     *  anything else as it is. */
    private SqlWriter asDouble(SqlWriter writer, SqlExpr e, boolean mayBeNull) {
        if (!bigDecimal(e)) {
            return writer.expr(e, 0);
        }
        if (mayBeNull) {
            return writer.append("(isNull(").expr(e, 0).append(") ? NULL_DOUBLE : ").expr(e, 0).append(".doubleValue())");
        }
        return writer.append("(").expr(e, 0).append(".doubleValue())");
    }

    /** COALESCE in doubles: each argument a double first -- a condition of a BigDecimal and a double is refused by the
     *  server ("Incompatible types in condition operation": the stress corpus on Deephaven, task 15). */
    private SqlWriter coalesceInDoubles(SqlWriter writer, List<SqlExpr> a, int from) {
        if (from == a.size() - 1) {
            return asDouble(writer, a.get(from), nullable(a.get(from)));
        }
        writer.append("(!isNull(").expr(a.get(from), 0).append(") ? ");
        asDouble(writer, a.get(from), false).append(" : ");
        return coalesceInDoubles(writer, a, from + 1).append(")");
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
    /** Half-even, as SqlFn.ROUND is: {@code rint} of the value, or of the value scaled by a literal power of ten and
     *  scaled back (SQL's ROUND_EVEN(x, s) is the same two steps); the library's {@code rint} keeps a null a null. */
    private SqlWriter round(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (a.size() == 1) {
            return writer.append("rint(").expr(a.get(0), 0).append(")");
        }
        if (!(a.get(1) instanceof SqlExpr.IntLit scale) || scale.value() < 0 || scale.value() > 15) {
            throw new DialectCapability("a round to a computed or negative scale reached Deephaven: no shape yet");
        }
        String power = java.math.BigDecimal.TEN.pow((int) scale.value()).toPlainString() + ".0";
        return writer.append("(rint(").expr(a.get(0), 0).append(" * " + power + ") / " + power + ")");
    }

    /** The platform's SUBSTRING(s, from[, length]) over a literal start and length: one-based, a start below 1 held
     *  at 1 (H2's at its runtime, DuckDB's by SubstringClamp), each index held to the string's length as SQL holds it
     *  (a start past the end is the empty string) -- String's own substring. */
    private SqlWriter substring(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        SqlExpr s = a.get(0);
        if (!(a.get(1) instanceof SqlExpr.IntLit from)
                || a.size() > 2 && !(a.get(2) instanceof SqlExpr.IntLit length && length.value() >= 0)) {
            throw new DialectCapability("a substring at a computed place reached Deephaven: no shape yet");
        }
        long begin = Math.max(from.value(), 1) - 1;
        SqlWriter.Piece len = w -> w.expr(s, 0).append(".length()");
        SqlWriter.Piece held = w -> w.append("(").piece(len).append(" < " + begin + " ? ").piece(len)
                .append(" : " + begin + ")");
        if (nullable(s)) {
            writer.append("(isNull(").expr(s, 0).append(") ? ").append(typedNull(c.type(), "a substring")).append(" : ");
        }
        writer.expr(s, 0).append(".substring(").piece(held);
        if (a.size() > 2) {
            long end = begin + ((SqlExpr.IntLit) a.get(2)).value();
            writer.append(", (").piece(len).append(" < " + end + " ? ").piece(len).append(" : " + end + ")");
        }
        writer.append(")");
        return nullable(s) ? writer.append(")") : writer;
    }

    /** SQL's STRPOS(s, sub): the one-based place of the first {@code sub} in {@code s}, 0 where there is none. */
    private SqlWriter strpos(SqlWriter writer, SqlExpr.Call c) {
        SqlExpr s = c.args().get(0);
        SqlExpr sub = c.args().get(1);
        boolean guard = nullable(s) || nullable(sub);
        if (guard) {
            writer.append("(isNull(").expr(s, 0).append(") || isNull(").expr(sub, 0).append(") ? ")
                    .append(typedNull(c.type(), "a string's place")).append(" : ");
        }
        writer.append("(").expr(s, 0).append(".indexOf(").expr(sub, 0).append(") + 1)");
        return guard ? writer.append(")") : writer;
    }

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
    // Dates and times (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md DH29)
    // ==================================================================

    /** The zone a calendar function reads an instant in: Pure's DateTime is UTC wall-clock, as on every SQL lane. */
    private static final String UTC = "timeZone(`UTC`)";

    /** Where a week starts: Monday 1969-12-29, three days before the epoch (the platform's origin, an ISO week). */
    private static final long WEEK_OFFSET = -259_200_000_000_000L;

    /** A fixed unit's length in nanoseconds, what an instant is binned and moved by; null for a calendar unit. */
    private static @com.legend.base.Nullable Long nanos(com.legend.sql.DatePart p) {
        return switch (p) {
            case NANOSECOND -> 1L;
            case MICROSECOND -> 1_000L;
            case MILLISECOND -> 1_000_000L;
            case SECOND -> 1_000_000_000L;
            case MINUTE -> 60_000_000_000L;
            case HOUR -> 3_600_000_000_000L;
            case DAY -> 86_400_000_000_000L;
            case WEEK -> 604_800_000_000_000L;
            case YEAR, QUARTER, MONTH, DAY_OF_YEAR, ISO_DAY_OF_WEEK -> null;
        };
    }

    /** A date-time literal's text ({@code 2026-10-24T09:30:00.000}, or with a space) as the ISO instant the server
     *  parses, in UTC ({@code ...Z}); a text of another form refused by name. */
    static String isoInstant(String iso) {
        String t = iso.replace(' ', 'T');
        if (!t.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}(:\\d{2}(\\.\\d{1,9})?)?")) {
            throw new DialectCapability("a date-time literal (" + iso + ") reached Deephaven in a form not shaped yet");
        }
        return (t.length() == 16 ? t + ":00" : t) + "Z";
    }

    /** Whether {@code e} is a date (a {@code LocalDate}) rather than a date-time (an {@code Instant}): what a
     *  date-time function's spelling turns on. A value of any other type refused by name. */
    private static boolean isDate(SqlExpr e, String what) {
        // a time bucket is an instant whatever it buckets (timeBucket: the server's lowerBin), as on every dialect; the
        // IR leaves its type unknown
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.TIME_BUCKET) {
            return false;
        }
        if (!(e.type() instanceof TypeFact.Typed t) || !(t.type() instanceof SqlType.Scalar s)) {
            throw new DialectCapability(what + " of a value of no known type reached Deephaven: no shape yet");
        }
        return switch (s) {
            case DATE -> true;
            case TIMESTAMP -> false;
            case BOOLEAN, INTEGER, BIGINT, HUGEINT, DOUBLE, VARCHAR, TIMESTAMPTZ, TIME, JSON, LITERAL, TEMPORAL_TEXT,
                 DECIMAL_TEXT -> throw new DialectCapability(what + " of a " + s + " reached Deephaven: no shape yet");
        };
    }

    /** {@code e} as an instant: a date-time as it is, a date at its UTC midnight. */
    private SqlWriter asInstant(SqlWriter writer, SqlExpr e, String what) {
        return isDate(e, what) ? writer.append("toInstant(atMidnight(").expr(e, 0).append(", " + UTC + "))")
                : writer.expr(e, 0);
    }

    /** {@code e} as a date: a date as it is, a date-time's UTC date. */
    private SqlWriter asDate(SqlWriter writer, SqlExpr e, String what) {
        return isDate(e, what) ? writer.expr(e, 0) : writer.append("toLocalDate(").expr(e, 0).append(", " + UTC + ")");
    }

    /** DATE_DIFF(part, from, to): SQL's count of the part's boundaries crossed from {@code from} to {@code to}
     *  (DuckDB's {@code date_diff}, measured 1.4.4): a day's, a month's or a year's on the UTC calendar, a
     *  millisecond's floored (half a millisecond before the epoch to the epoch is 1). A null either side is null. */
    private SqlWriter dateDiff(SqlWriter writer, SqlExpr.Call c) {
        com.legend.sql.DatePart p = part(c.args().get(0));
        SqlExpr from = c.args().get(1);
        SqlExpr to = c.args().get(2);
        String what = "a date difference";
        writer.append("(");
        List<SqlExpr> guarded = java.util.stream.Stream.of(from, to).filter(Deephaven::nullable).toList();
        if (!guarded.isEmpty()) {
            writer.append("(");
            for (int i = 0; i < guarded.size(); i++) {
                writer.append(i > 0 ? " || isNull(" : "isNull(").expr(guarded.get(i), 0).append(")");
            }
            // the library's calendar parts read a null's sentinel, but a date's own toEpochDay would fail on it
            writer.append(") ? NULL_LONG : ");
        }
        // an expression switch, so javac holds it to every part
        SqlWriter done = switch (p) {
            case DAY -> {
                asDate(writer, to, what).append(".toEpochDay() - ");
                yield asDate(writer, from, what).append(".toEpochDay()");
            }
            case MONTH -> {
                writer.append("12L * (");
                calendar(writer, "year", to).append(" - ");
                calendar(writer, "year", from).append(") + (");
                calendar(writer, "monthOfYear", to).append(" - ");
                yield calendar(writer, "monthOfYear", from).append(")");
            }
            case YEAR -> {
                writer.append("(long) (");
                calendar(writer, "year", to).append(" - ");
                yield calendar(writer, "year", from).append(")");
            }
            case MILLISECOND -> {
                writer.append("epochMillis(");
                asInstant(writer, to, what).append(") - epochMillis(");
                yield asInstant(writer, from, what).append(")");
            }
            case DAY_OF_YEAR, ISO_DAY_OF_WEEK, QUARTER, WEEK, HOUR, MINUTE, SECOND, MICROSECOND, NANOSECOND ->
                    throw new DialectCapability("a date difference in " + p + "S reached Deephaven: no shape yet");
        };
        return done.append(")");
    }

    /** The library's calendar part {@code fn} of a date, or of a date-time in UTC. */
    private SqlWriter calendar(SqlWriter writer, String fn, SqlExpr t) {
        return writer.append(fn + "(").expr(t, 0).append(isDate(t, "a calendar part") ? ")" : ", " + UTC + ")");
    }

    /** EPOCH_MS(t): the milliseconds since the epoch, cut toward zero as DuckDB's {@code epoch_ms} cuts them
     *  (measured 1.4.4: half a millisecond before the epoch is 0, where Java's {@code toEpochMilli} floors to -1) --
     *  the instant's nanoseconds less their remainder, by the library's floored division; a null is null (the
     *  library's and the operators' own sentinels), and an instant past the nanoseconds' range fails. */
    private SqlWriter epochMillis(SqlWriter writer, SqlExpr t) {
        String what = "an epoch's milliseconds";
        writer.append("floorDiv(epochNanos(");
        asInstant(writer, t, what).append(") - (epochNanos(");
        return asInstant(writer, t, what).append(") % 1000000L), 1000000L)");
    }

    /** TIME_BUCKET(unit, n, t): the start of t's bucket of n units, counted from the epoch -- a week's from its Monday
     *  ({@link #WEEK_OFFSET}) -- as the server's {@code lowerBin}; an instant whatever t is (the lowering casts a
     *  date's back). A calendar unit is none of the server's bins, refused by name (DH29). */
    private SqlWriter timeBucket(SqlWriter writer, List<SqlExpr> a) {
        com.legend.sql.DatePart unit = part(a.get(0));
        Long length = nanos(unit);
        if (length == null) {
            throw new DialectCapability("a time bucket of " + unit + "S reached Deephaven: a calendar unit is none of"
                    + " the server's bins (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md DH29)");
        }
        writer.append("lowerBin(");
        asInstant(writer, a.get(2), "a time bucket").append(", (").expr(a.get(1), 0).append(" * " + length + "L)");
        return writer.append(unit == com.legend.sql.DatePart.WEEK ? ", " + WEEK_OFFSET + "L)" : ")");
    }

    /** EXTRACT(part, t): a date's or a date-time's calendar part in UTC, a whole number (the IR's BIGINT), null when
     *  t is. A part a date does not have, or a fraction of a second, refused by name. */
    private SqlWriter extract(SqlWriter writer, List<SqlExpr> a) {
        com.legend.sql.DatePart p = part(a.get(0));
        SqlExpr t = a.get(1);
        boolean date = isDate(t, "a date part");
        String zone = date ? ")" : ", " + UTC + ")";
        if (nullable(t)) {
            writer.append("(isNull(").expr(t, 0).append(") ? NULL_LONG : (long) ");
        } else {
            writer.append("((long) ");
        }
        // an expression switch, so javac holds it to every part
        SqlWriter done = switch (p) {
            case YEAR -> writer.append("year(").expr(t, 0).append(zone);
            case MONTH -> writer.append("monthOfYear(").expr(t, 0).append(zone);
            case DAY -> writer.append("dayOfMonth(").expr(t, 0).append(zone);
            case DAY_OF_YEAR -> writer.append("dayOfYear(").expr(t, 0).append(zone);
            // ISO's: Monday 1 to Sunday 7 (the library's number: a DayOfWeek's own method is none the server's
            // validator permits, DH33)
            case ISO_DAY_OF_WEEK -> writer.append("dayOfWeekValue(").expr(t, 0).append(zone);
            case QUARTER -> writer.append("((int) ((monthOfYear(").expr(t, 0).append(zone).append(" - 1) / 3) + 1)");
            // ISO's week of its week-based year
            case WEEK -> asDate(writer, t, "a week of the year")
                    .append(".get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR)");
            case HOUR -> timeOfDay(writer, t, date, p).append("hourOfDay(").expr(t, 0).append(", " + UTC + ", false)");
            case MINUTE -> timeOfDay(writer, t, date, p).append("minuteOfHour(").expr(t, 0).append(zone);
            case SECOND -> timeOfDay(writer, t, date, p).append("secondOfMinute(").expr(t, 0).append(zone);
            case MILLISECOND, MICROSECOND, NANOSECOND -> throw new DialectCapability("a date-time's " + p
                    + " reached Deephaven: no shape yet");
        };
        return done.append(")");
    }

    /** A time-of-day part's writer: a date has none, refused by name. */
    private static SqlWriter timeOfDay(SqlWriter writer, SqlExpr t, boolean date, com.legend.sql.DatePart p) {
        if (date) {
            throw new DialectCapability("a date's " + p + " reached Deephaven: a date has none");
        }
        return writer;
    }

    /** DATE_TRUNC(part, t): where t's part starts, in UTC, as the platform's dialects answer it
     *  ({@link AnsiSqlRenderer#dateTruncDateGrained}): to a day or finer an instant, the server's {@code lowerBin}
     *  (the epoch is a UTC midnight); to a week, a month, a quarter or a year its first day, a date -- a week's from
     *  its Monday. Null when t is. */
    private SqlWriter dateTrunc(SqlWriter writer, List<SqlExpr> a) {
        com.legend.sql.DatePart p = part(a.get(0));
        SqlExpr t = a.get(1);
        String first = switch (p) {
            case NANOSECOND, MICROSECOND, MILLISECOND, SECOND, MINUTE, HOUR, DAY -> {
                writer.append("lowerBin(");
                asInstant(writer, t, "a date truncation").append(", " + nanos(p) + "L)");
                yield null;
            }
            case WEEK -> {
                writer.append("toLocalDate(lowerBin(");
                asInstant(writer, t, "a date truncation").append(", " + nanos(p) + "L, " + WEEK_OFFSET + "L), " + UTC + ")");
                yield null;
            }
            case MONTH -> ".withDayOfMonth(1)";
            case YEAR -> ".withDayOfYear(1)";
            case QUARTER -> "";
            case DAY_OF_YEAR, ISO_DAY_OF_WEEK -> throw new DialectCapability("a date truncation to its " + p
                    + " reached Deephaven: no shape yet");
        };
        if (first == null) {
            return writer;
        }
        // a method of a null date fails the operation: a null t is a null date
        writer.append("(");
        if (nullable(t)) {
            writer.append("isNull(").expr(t, 0).append(") ? (java.time.LocalDate) null : ");
        }
        asDate(writer, t, "a date truncation");
        if (p == com.legend.sql.DatePart.QUARTER) {
            // the quarter's first month: the month less its place in the quarter
            writer.append(".withDayOfMonth(1).minusMonths((");
            asDate(writer, t, "a date truncation").append(".getMonthValue() - 1) % 3)");
        } else {
            writer.append(first);
        }
        return writer.append(")");
    }

    /** ADD_INTERVAL(unit, n, t): t moved by n units, an instant (the IR's TIMESTAMP, a date included) -- a fixed unit
     *  by its nanoseconds (a day is 24 hours in UTC), a month or a year on the UTC calendar (a day past the month's
     *  end its last day, as java.time and the platform's lanes keep it); null when t or n is. */
    private SqlWriter addInterval(SqlWriter writer, List<SqlExpr> a) {
        com.legend.sql.DatePart unit = part(a.get(0));
        SqlExpr n = a.get(1);
        SqlExpr t = a.get(2);
        Long length = nanos(unit);
        if (length != null) {
            writer.append("plus(");
            asInstant(writer, t, "a date's adjustment").append(", (").expr(n, 0).append(" * " + length + "L)");
            return writer.append(")");
        }
        String method = switch (unit) {
            case MONTH -> ".plusMonths(";
            case QUARTER -> ".plusMonths(3 * ";
            case YEAR -> ".plusYears(";
            case DAY_OF_YEAR, ISO_DAY_OF_WEEK, NANOSECOND, MICROSECOND, MILLISECOND, SECOND, MINUTE, HOUR, DAY, WEEK ->
                    throw new DialectCapability("a date's adjustment by " + unit + " reached Deephaven: no shape yet");
        };
        writer.append("(");
        if (nullable(t) || nullable(n)) {
            writer.append("(");
            if (nullable(t)) {
                writer.append("isNull(").expr(t, 0).append(")").append(nullable(n) ? " || " : "");
            }
            if (nullable(n)) {
                writer.append("isNull(").expr(n, 0).append(")");
            }
            writer.append(") ? (java.time.Instant) null : ");
        }
        writer.append("toInstant(toZonedDateTime(");
        asInstant(writer, t, "a date's adjustment").append(", " + UTC + ")" + method).expr(n, 0);
        return writer.append(")))");
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
        if (e instanceof SqlExpr.Call k && k.fn() == SqlFn.ERROR) {
            // a guard's error (the lowering's CASE WHEN bad THEN ERROR(...) ELSE value): the CASE's value, never made
            return raise(writer, k, c.type(), "a CASE's runtime error");
        }
        return writer.expr(e, 0);
    }

    // ==================================================================
    // K5: whole-number division, the runtime error, a Float's text
    // ==================================================================

    /** This dialect writes a Float's two texts itself ({@link #floatPrint}): the portable spelling's casts of a double
     *  to text are none of the formula language's. */
    @Override
    protected boolean spellsFloatPrint() {
        return true;
    }

    /** A Float's text. Pure's ({@code FLOAT_PRINT}, {@code FloatPrintText}'s contract): its shortest round-trip digits
     *  written plainly -- Java's own text where it is plain (shortest digits since JDK 19; the server runs 25), and
     *  where Java writes an exponent, re-written as the platform's portable spelling re-writes the database's: a whole
     *  value below 1e38 as its exact digits (the double's own BigDecimal) and {@code .0}, a fraction in [1e-17, 2^53)
     *  as its digits' BigDecimal, trailing zeros stripped; outside those bands the exponent kept (Java's
     *  {@code 1.5E-18}, where DuckDB writes {@code 1.5e-18} and H2 Java's). legend-engine's ({@code
     *  FLOAT_PRINT_ENGINE}) is Java's {@code Double.toString} layout over the shortest digits: Java's own text. NaN and
     *  the infinities are Java's words, which are Pure's. A null stays null. BigDecimal's constructors and Double's
     *  text are the validator's (DH33, measured on 42.7). */
    private SqlWriter floatPrint(SqlWriter writer, SqlExpr.Call c) {
        SqlExpr x = c.args().get(0);
        if (scalarOf(x) != SqlType.Scalar.DOUBLE) {
            throw new DialectCapability("a Float's text of a " + x.type() + " reached Deephaven: no shape yet");
        }
        String empty = stringLiteral("");
        writer.append("(");
        if (nullable(x)) {
            writer.append("isNull(").expr(x, 0).append(") ? (String) null : ");
        }
        if (c.fn() == SqlFn.FLOAT_PRINT_ENGINE) {
            return writer.append("(" + empty + " + ").expr(x, 0).append("))");
        }
        writer.append("(" + empty + " + ").expr(x, 0).append(").indexOf(`E`) < 0 ? (" + empty + " + ").expr(x, 0)
                .append(") : (").expr(x, 0).append(" == floor(").expr(x, 0).append(") && abs(").expr(x, 0)
                .append(") < 1.0E38) ? (new java.math.BigDecimal(").expr(x, 0).append(").toPlainString() + `.0`) : (abs(")
                .expr(x, 0).append(") >= 1.0E-17 && abs(").expr(x, 0).append(") < 9.007199254740992E15) ?")
                .append(" new java.math.BigDecimal(" + empty + " + ").expr(x, 0)
                .append(").stripTrailingZeros().toPlainString() : (" + empty + " + ").expr(x, 0).append(")");
        return writer.append(")");
    }

    /** A whole-number quotient cut toward zero ({@code INT_DIVIDE}; the formula language's {@code /} answers a
     *  double): the library's floored division ({@code floorDiv}) of the dividend less its remainder -- an exact
     *  multiple of the divisor, whose floor is the cut quotient, with no double between. A null either side is the
     *  quotient's null: the operators and the library carry a sentinel through, but for an int divisor Java widens
     *  to a long dividend's call, its null with it, so such a divisor that may be null is guarded. A zero divisor
     *  fails, as Java's division does (the lowering guards one by name). */
    private SqlWriter intDivide(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        writer.append("(");
        SqlType.Scalar divisor = scalarOf(a.get(1));
        boolean widened = divisor != SqlType.Scalar.BIGINT && divisor != scalarOf(a.get(0));
        if (widened && !(a.get(1) instanceof SqlExpr.IntLit) && nullable(a.get(1))) {
            writer.append("isNull(").expr(a.get(1), 0).append(") ? ")
                    .append(typedNull(c.type(), "a whole-number quotient")).append(" : ");
        }
        writer.append("floorDiv(").expr(a.get(0), 0).append(" - (").expr(a.get(0), 0).append(" % ")
                .expr(a.get(1), 0).append("), ").expr(a.get(1), 0).append(")");
        return writer.append(")");
    }

    /** {@code e}'s scalar type, or null when it has none known. */
    private static SqlType.@com.legend.base.Nullable Scalar scalarOf(SqlExpr e) {
        return e.type() instanceof TypeFact.Typed t && t.type() instanceof SqlType.Scalar s ? s : null;
    }

    /** The platform's runtime error ({@code ERROR}: its message, and the raising call's place when it has one) as a
     *  value of {@code fact}'s type that is never made. */
    private SqlWriter raise(SqlWriter writer, SqlExpr.Call c, TypeFact fact, String what) {
        if (!(fact instanceof TypeFact.Typed t) || !(t.type() instanceof SqlType.Scalar s)) {
            throw new DialectCapability(what + " of no known type reached Deephaven, whose formulas are typed");
        }
        return raise(writer, c, s);
    }

    /** A formula has no throw, so the error is the library's parse of a whole number from its message inside the
     *  platform's raise envelope ({@code RaisedErrors}: U+001F either side, U+001E after the place), which fails
     *  naming the text it could not read ({@code NumberFormatException: For input string: "..."}): the server fails
     *  the operation, or a live table, at the first row that takes it. Never a double's parse, which trims the
     *  envelope's marks (a message of digits would parse); a value of another type is the parsed number converted,
     *  which is never reached. */
    private SqlWriter raise(SqlWriter writer, SqlExpr.Call c, SqlType.Scalar s) {
        String[] around = switch (s) {
            case INTEGER -> new String[] {"parseInt(", ")"};
            case BIGINT -> new String[] {"parseLong(", ")"};
            case DOUBLE -> new String[] {"((double) parseLong(", "))"};
            case BOOLEAN -> new String[] {"(parseLong(", ") == 0L)"};
            case VARCHAR -> new String[] {"(" + stringLiteral("") + " + parseLong(", "))"};
            case TIMESTAMP -> new String[] {"epochNanosToInstant(parseLong(", "))"};
            case DATE -> new String[] {"toLocalDate(epochNanosToInstant(parseLong(", ")), " + UTC + ")"};
            case TIME -> new String[] {"toLocalTime(epochNanosToInstant(parseLong(", ")), " + UTC + ")"};
            case HUGEINT, TIMESTAMPTZ, JSON, LITERAL, TEMPORAL_TEXT, DECIMAL_TEXT -> throw new DialectCapability(
                    "a runtime error of type " + s + " reached Deephaven: no column holds it yet");
        };
        List<SqlExpr> a = c.args();
        writer.append(around[0]).append("(" + stringLiteral("") + " + (char) 31 + ");
        if (a.size() > 1) {
            writer.expr(a.get(1), 0).append(" + (char) 30 + ");
        }
        return writer.append("(").expr(a.get(0), 0).append(") + (char) 31)").append(around[1]);
    }

    /** A numeric widening cast, a null kept null (a primitive cast of a null sentinel would answer a number); a
     *  decimal to a double by its own {@code doubleValue}; a date-time to its UTC date and a date to its UTC midnight
     *  (DH29). */
    private SqlWriter cast(SqlWriter writer, SqlExpr.Cast c) {
        if (!(c.target() instanceof SqlType.Scalar target)) {
            throw new DialectCapability("a cast to " + c.target() + " reached Deephaven: no shape yet");
        }
        if (target == SqlType.Scalar.DATE) {
            return asDate(writer, c.value(), "a cast to a date");
        }
        if (target == SqlType.Scalar.TIMESTAMP) {
            return asInstant(writer, c.value(), "a cast to a date-time");
        }
        if (target == SqlType.Scalar.VARCHAR) {
            return text(writer, c);
        }
        String primitive = switch (target) {
            case DOUBLE -> "double";
            case BIGINT -> "long";
            default -> throw new DialectCapability("a cast to " + target + " (from " + c.value().type()
                    + ") reached Deephaven: no shape yet");
        };
        if (target == SqlType.Scalar.DOUBLE && c.value().type() instanceof TypeFact.Typed from
                && from.type() instanceof SqlType.Decimal) {
            if (!bigDecimal(c.value())) {
                // a decimal the formula computes from no decimal column is a double already: its numbers are written
                // as doubles (decimal literals are refused) -- an if of two literals, typed DECIMAL by their digits (the
                // trader screens' FX pips, 2026-10-25: "(... ? 0.01 : 1.0E-4).doubleValue()" refused by the server) --
                // and so is one written in doubles (bigDecimal)
                return writer.append("(").expr(c.value(), 0).append(")");
            }
            // a DECIMAL column is a BigDecimal (DeephavenBatch's DDL): its own doubleValue, a permitted method (DH33)
            if (nullable(c.value())) {
                return writer.append("(isNull(").expr(c.value(), 0).append(") ? NULL_DOUBLE : ").expr(c.value(), 0)
                        .append(".doubleValue())");
            }
            return writer.append("(").expr(c.value(), 0).append(".doubleValue())");
        }
        if (!(c.value().type() instanceof TypeFact.Typed from) || !(from.type() instanceof SqlType.Scalar f)
                || !(f == SqlType.Scalar.INTEGER || f == SqlType.Scalar.BIGINT || f == SqlType.Scalar.DOUBLE)) {
            throw new DialectCapability("a cast of a non-numeric value (" + c.value().type() + ") to " + target
                    + " reached Deephaven: no shape yet");
        }
        if (f == target) {
            // a value of the type it is cast to is itself, a null kept null: the guard and its error's text around a
            // double cast to a double wrote it ten times, a square root's formula 1,700 characters, and the server's
            // parse of a formula grows faster than its length (2026-10-26, the trading desk's options chain)
            return writer.append("(").expr(c.value(), 0).append(")");
        }
        if (nullable(c.value())) {
            writer.append("(isNull(").expr(c.value(), 0).append(") ? ").append(typedNull(c.type(), "a cast"))
                    .append(" : ");
            return writer.append("(" + primitive + ") ").expr(c.value(), 0).append(")");
        }
        return writer.append("((" + primitive + ") ").expr(c.value(), 0).append(")");
    }

    /** Whether {@code e} reads a DECIMAL column (a BigDecimal), anywhere in it. */
    private static boolean readsDecimalColumn(SqlExpr e) {
        if (e instanceof SqlExpr.Column c) {
            return c.type() instanceof TypeFact.Typed t && t.type() instanceof SqlType.Decimal;
        }
        for (SqlExpr child : e.children()) {
            if (readsDecimalColumn(child)) {
                return true;
            }
        }
        return false;
    }

    /** A value as text: a string is itself; a whole number its digits, by Java's string concatenation (SQL's CAST of
     *  an INTEGER or a BIGINT to VARCHAR), a null kept null. A float's text and a date's differ between Java and SQL,
     *  and are refused by name until shaped. */
    private SqlWriter text(SqlWriter writer, SqlExpr.Cast c) {
        SqlType.Scalar from = c.value().type() instanceof TypeFact.Typed t && t.type() instanceof SqlType.Scalar f
                ? f : null;
        if (from == SqlType.Scalar.VARCHAR) {
            return writer.expr(c.value(), 0);
        }
        if (from != SqlType.Scalar.INTEGER && from != SqlType.Scalar.BIGINT) {
            throw new DialectCapability("a cast to VARCHAR (from " + c.value().type() + ") reached Deephaven: no shape"
                    + " yet");
        }
        String empty = stringLiteral("");
        if (nullable(c.value())) {
            return writer.append("(isNull(").expr(c.value(), 0).append(") ? (String) null : (" + empty + " + ")
                    .expr(c.value(), 0).append("))");
        }
        return writer.append("(" + empty + " + ").expr(c.value(), 0).append(")");
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
            // the value at the first or the last of an order (a candle's open and close: K3a)
            case ARG_MIN -> sorted(r, "sortedFirst", out, names);
            case ARG_MAX -> sorted(r, "sortedLast", out, names);
            // computed after their group by DeephavenBatch (AFTER, and a mode by its own aggregation joined back),
            // never an aggregation of their own
            case STDDEV_POP, VAR_POP, QUANTILE_CONT, CORR, COVAR_SAMP, COVAR_POP, MODE -> throw new IllegalStateException(
                    "aggregate " + r.fn() + " reached Deephaven's aggregation, which DeephavenBatch computes after it");
            case STRING_AGG, LIST, QUANTILE_DISC, BOOL_AND, BOOL_OR -> throw new DialectCapability("aggregate "
                    + r.fn() + " reached Deephaven: no shape yet");
            case WAVG, HASH_LIST, IS_DISTINCT_MARK, UNIQUE_VALUE_ONLY -> throw loweringMarker(r);
            // an ordered window's (the lowering refuses one in a group), which DeephavenBatch's windows write
            case EMA, ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE,
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
                + DeephavenJson.strings(List.of(notNullFilter(arg))) + "}}";
    }

    /**
     * ARG_MIN(v, k) and ARG_MAX(v, k): the value v of the row whose k is the least, or the greatest -- the server's
     * {@code sortedFirst} and {@code sortedLast} by k (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K3a). SQL's skip a row
     * whose k is null, where the server sorts a null first, so a v or a k that may be null is refused by name until
     * measured; a tie is any of the tied rows on both.
     */
    private static String sorted(SqlAgg.Reducer r, String spec, String out, String[] names) {
        if (r.distinct() || names.length < 3) {
            throw new IllegalStateException("aggregate " + r.fn() + " reached Deephaven without its order's column");
        }
        // a value or an order that may be null is made harmless first (DeephavenBatch.skipped: its fourth name)
        if (names.length < 4 && (nullable(r.args().get(0)) || nullable(r.args().get(1)))) {
            throw new IllegalStateException("aggregate " + r.fn() + " over a value or an order that may be null"
                    + " reached Deephaven unskipped");
        }
        return "{\"columns\": {\"spec\": {\"" + spec + "\": {\"columns\": [{\"columnName\": "
                + DeephavenJson.string(names[2]) + "}]}}, \"matchPairs\": "
                + DeephavenJson.strings(List.of(out + "=" + names[1])) + "}}";
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
