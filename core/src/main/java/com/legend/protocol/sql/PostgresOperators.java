// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql;

import static com.legend.protocol.sql.SqlToPure.*;
import static com.legend.protocol.sql.PostgresFunctions.*;
import static com.legend.protocol.sql.PostgresCasts.*;
import static com.legend.protocol.sql.PostgresDates.*;
import static com.legend.protocol.sql.PostgresJsonb.*;

import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.List;

/**
 * Postgres's operators and predicates as the {@code #SQL{...}#} island reads them: arithmetic, comparison,
 * concatenation, IS DISTINCT FROM, IN, BETWEEN, LIKE, and the one type of CASE's results ({@link PostgresDialect}'s,
 * moved out of {@link SqlToPure} with the dialect program's D11, 2026-10-18).
 */
final class PostgresOperators {

    private PostgresOperators() {
    }

    /** An operator's operands, a NULL constant typed as the other operand (Postgres resolves an unknown by the other
     *  side); two NULL constants are Postgres's error. */
    static List<Tx> typedOperands(Tx l, Tx r, String operator, SqlAst.Pos pos) {
        if (l.kind() == Kind.NULL && r.kind() == Kind.NULL) {
            throw SqlTranslationException.invalid("operator is not unique: unknown " + operator + " unknown", pos);
        }
        if (l.kind() == Kind.NULL) {
            return List.of(nullOf(r.kind()), r);
        }
        if (r.kind() == Kind.NULL) {
            return List.of(l, nullOf(l.kind()));
        }
        return List.of(l, r);
    }

    static Tx unary(SqlToPure translator, SqlAst.Unary u, Scope scope, String row) {
        if (u.op() == SqlAst.UnaryOp.MINUS) {
            holdsOperands(translator, u, u.operand()); // a plus is its operand, which holds itself
        }
        Tx operand = translator.tx(u.operand(), scope, row);
        return switch (u.op()) {
            case NOT -> not(truth(operand, "NOT", u.pos()));
            case MINUS -> {
                numericOperand(operand, u.operand());
                if (operand.kind() == Kind.INTEGER && constantWidth(u) == null
                        && narrowValue(translator, u.operand(), operand, scope)) {
                    throw SqlTranslationException.unsupported("negating a smallint value that is not a constant is not"
                            + " supported yet: the database negates it as a smallint, raising for -32768, where the"
                            + " island's integer is wider", u.pos());
                }
                yield heldToWidth(translator, strict(operand.kind(), List.of(operand), v -> call("minus", v.get(0))),
                        u);
            }
            case PLUS -> {
                numericOperand(operand, u.operand());
                yield operand;
            }
        };
    }

    static Tx binary(SqlToPure translator, SqlAst.Binary b, Scope scope, String row) {
        switch (b.op()) {
            case AND, OR -> {
                return translator.logical(b, scope, row);
            }
            case EQ, NE, LT, LE, GT, GE -> {
                List<Tx> sides = translator.comparable(b.left(), b.right(), scope, row, b.pos());
                return compare(b.op(), sides.get(0), sides.get(1));
            }
            case PLUS, MINUS, TIMES -> {
                holdsOperands(translator, b, b.left(), b.right());
                boolean additive = b.op() != SqlAst.BinaryOp.TIMES;
                if (additive && (intervalConstant(b.left()) != null || intervalConstant(b.right()) != null)) {
                    return intervalArithmetic(translator, b, scope, row);
                }
                Tx lt = translator.tx(b.left(), scope, row);
                Tx rt = translator.tx(b.right(), scope, row);
                if (additive && (isDate(lt.kind()) || isDate(rt.kind()))) {
                    return dateArithmetic(b, lt, rt);
                }
                List<Tx> lr = typedOperands(lt, rt,
                        b.op() == SqlAst.BinaryOp.PLUS ? "+" : b.op() == SqlAst.BinaryOp.MINUS ? "-" : "*", b.pos());
                Tx l = lr.get(0);
                Tx r = lr.get(1);
                numericOperand(l, b.left());
                numericOperand(r, b.right());
                noSmallintConstantBeside(b, l, r);
                noNarrowArithmetic(translator, b, l, r, scope);
                String fn = b.op() == SqlAst.BinaryOp.PLUS ? "plus" : b.op() == SqlAst.BinaryOp.MINUS ? "minus"
                        : "times";
                return heldToWidth(translator, strict(arithmetic(l.kind(), r.kind()), List.of(l, r),
                        v -> AppliedFunction.infixRun(fn, List.of(v.get(0), v.get(1)))), b);
            }
            case DIVIDE -> {
                holdsOperands(translator, b, b.left(), b.right());
                List<Tx> lr = typedOperands(translator.tx(b.left(), scope, row), translator.tx(b.right(), scope, row),
                        "/", b.pos());
                Tx l = lr.get(0);
                Tx r = lr.get(1);
                numericOperand(l, b.left());
                numericOperand(r, b.right());
                noSmallintConstantBeside(b, l, r);
                noNarrowArithmetic(translator, b, l, r, scope);
                if (l.kind() == Kind.WHOLE && (r.kind() == Kind.INTEGER || r.kind() == Kind.WHOLE)
                        || r.kind() == Kind.WHOLE && l.kind() == Kind.INTEGER) {
                    throw SqlTranslationException.unsupported("dividing a sum of integers by an integer is not supported"
                            + " yet: Postgres's sum of an integer column is a bigint or a numeric by the column's width,"
                            + " which divide differently (cast it: sum(x)::bigint / n or sum(x)::numeric / n)",
                            b.pos());
                }
                if (l.kind() == Kind.INTEGER && r.kind() == Kind.INTEGER) {
                    // 2026-10-18 (IQ landing 5's review): the exact estimate, not lite's quotient, until every
                    // dialect's integer division is exact (Snowflake's rounds a / b to six places before it cuts)
                    return heldToWidth(translator, strict(Kind.INTEGER, List.of(l, r), v -> integerQuotient(v.get(0),
                            v.get(1))),
                            b);
                }
                // a double's quotient is a double, in Postgres too; a numeric's is exact there, a Float here
                boolean doubles = (l.kind() == Kind.FLOAT || r.kind() == Kind.FLOAT) && l.kind() != Kind.INEXACT
                        && r.kind() != Kind.INEXACT;
                return strict(doubles ? Kind.FLOAT : Kind.INEXACT, List.of(l, r), v -> call("divide", v.get(0),
                        v.get(1)));
            }
            case POWER -> {
                List<Tx> lr = typedOperands(translator.tx(b.left(), scope, row), translator.tx(b.right(), scope, row),
                        "^", b.pos());
                Tx l = lr.get(0);
                Tx r = lr.get(1);
                numericOperand(l, b.left());
                numericOperand(r, b.right());
                // Postgres computes ^ in double precision but for a numeric operand, where it is exact; pow is a Float
                if (exactNumeric(l.kind()) || exactNumeric(r.kind())) {
                    return numericPower(translator, b.left(), b.right(), List.of(l, r), "^", b.pos(), scope, row);
                }
                return strict(powerKind(l.kind(), r.kind()), List.of(l, r), v -> call("pow", v.get(0), v.get(1)));
            }
            case MODULO -> {
                List<Tx> lr = typedOperands(translator.tx(b.left(), scope, row), translator.tx(b.right(), scope, row),
                        "%", b.pos());
                numericOperand(lr.get(0), b.left());
                numericOperand(lr.get(1), b.right());
                return remainder(lr.get(0), lr.get(1), "operator does not exist: " + postgresName(lr.get(0).kind())
                        + " % " + postgresName(lr.get(1).kind()), "%", b.pos());
            }
            case CONCAT -> {
                // a || b || c is one strict concatenation of all its operands: NULL when any is. Postgres's || reads a
                // value of another type as its text where the other side is text (text || anynonarray)
                List<SqlAst.Expr> exprs = concatenated(b);
                List<Tx> operands = new ArrayList<>();
                for (SqlAst.Expr operand : exprs) {
                    Tx t = translator.tx(operand, scope, row);
                    if (t.kind() == Kind.JSON) {
                        throw SqlTranslationException.unsupported("|| of jsonb values (which merges them) is not"
                                + " supported yet", operand.pos());
                    }
                    operands.add(t);
                }
                Kind first = operands.get(0).kind();
                Kind second = operands.get(1).kind();
                if (first != Kind.STRING && first != Kind.NULL && second != Kind.STRING && second != Kind.NULL) {
                    throw SqlTranslationException.invalid("operator does not exist: " + postgresName(first) + " || "
                            + postgresName(second), b.pos());
                }
                List<Kind> kinds = new ArrayList<>();
                for (int i = 0; i < operands.size(); i++) {
                    Tx t = operands.get(i);
                    if (t.kind() != Kind.STRING && t.kind() != Kind.NULL) {
                        asText(t, exprs.get(i).pos(), "||");
                    }
                    kinds.add(t.kind());
                }
                return strict(Kind.STRING, operands, v -> {
                    List<ValueSpecification> texts = new ArrayList<>();
                    for (int i = 0; i < v.size(); i++) {
                        texts.add(kinds.get(i) == Kind.STRING ? v.get(i) : call("toString", v.get(i)));
                    }
                    return AppliedFunction.infixRun("plus", texts);
                });
            }
        }
        throw new IllegalStateException("unreachable: " + b.op());
    }

    /** A power's kind: of an inexact numeric, numeric in Postgres (so inexact here) unless a double is in it. */
    static Kind powerKind(Kind a, Kind b) {
        // a sum of integers may be numeric, whose power Postgres computes exactly
        return a == Kind.INEXACT || b == Kind.INEXACT || a == Kind.WHOLE || b == Kind.WHOLE ? Kind.INEXACT
                : Kind.FLOAT;
    }

    static boolean exactNumeric(Kind kind) {
        return kind == Kind.DECIMAL || kind == Kind.NUMBER;
    }

    /** The operands of a run of {@code ||}, left to right. */
    private static List<SqlAst.Expr> concatenated(SqlAst.Expr e) {
        if (e instanceof SqlAst.Binary b && b.op() == SqlAst.BinaryOp.CONCAT) {
            List<SqlAst.Expr> all = new ArrayList<>(concatenated(b.left()));
            all.addAll(concatenated(b.right()));
            return all;
        }
        return List.of(e);
    }

    static void numericOperand(Tx t, SqlAst.Expr e) {
        if (t.kind() == Kind.NULL || isNullConstant(t) && t.kind().numeric()) {
            // an untyped or a numeric NULL constant: strict makes the result NULL of the operator's type (a NULL of
            // another type, a text one included, is no number)
            return;
        }
        // Postgres has these operators (an untyped constant takes the other operand's type; date - date, jsonb - key)
        if (e instanceof SqlAst.StringLit) {
            throw SqlTranslationException.unsupported("an untyped string constant in arithmetic is not supported yet",
                    e.pos());
        }
        if (isDate(t.kind())) {
            throw SqlTranslationException.unsupported("date and timestamp arithmetic is not supported yet", e.pos());
        }
        if (t.kind() == Kind.JSON) {
            throw SqlTranslationException.unsupported("jsonb's - (deleting a key or an element) is not supported yet",
                    e.pos());
        }
        if (t.kind() == Kind.OTHER) {
            throw SqlTranslationException.unsupported("arithmetic on values of this type is not supported yet",
                    e.pos());
        }
        if (!t.kind().numeric()) {
            throw SqlTranslationException.invalid("arithmetic needs numbers; this operand is not numeric", e.pos());
        }
    }

    /** The results of a CASE, or the arguments of COALESCE, GREATEST or LEAST, as one type, by Postgres's resolution
     *  ("UNION, CASE, and Related Constructs"): an untyped string constant takes the others' type, read by that
     *  type's input rules (all untyped: text); the others must share a category, numbers widening to the widest;
     *  else Postgres's error. */
    static Common common(String what, List<SqlAst.Expr> exprs, List<Tx> txs, SqlAst.Pos pos) {
        Kind target = null;
        boolean untyped = false;
        for (int i = 0; i < txs.size(); i++) {
            Kind k = txs.get(i).kind();
            if (exprs.get(i) instanceof SqlAst.StringLit) {
                untyped = true;
                continue;
            }
            if (k == Kind.NULL) {
                continue;
            }
            if (target == null || target == k) {
                target = k;
            } else if (target.numeric() && k.numeric()) {
                target = arithmetic(target, k);
            } else if (isDate(target) && isDate(k)) {
                // Postgres reads a date as a timestamp at midnight here
                throw SqlTranslationException.unsupported(what + " of date and timestamp values is not supported yet",
                        pos);
            } else {
                throw SqlTranslationException.invalid(what + " types " + postgresName(target) + " and "
                        + postgresName(k) + " cannot be matched", pos);
            }
        }
        if (target == null) {
            return new Common(txs, untyped ? Kind.STRING : Kind.NULL);
        }
        List<Tx> values = new ArrayList<>();
        int merged = 0;
        for (int i = 0; i < txs.size(); i++) {
            if (txs.get(i).kind() != Kind.NULL && !isNullConstant(txs.get(i))) {
                merged++;
            }
            if (exprs.get(i) instanceof SqlAst.StringLit s && target != Kind.STRING) {
                if (target == Kind.JSON) {
                    values.add(jsonConstant(s));
                } else if (readsStrings(target)) {
                    values.add(fromString(s, target));
                } else {
                    throw SqlTranslationException.unsupported(what + " of a string constant and a "
                            + postgresName(target) + " value is not supported yet", s.pos());
                }
            } else {
                values.add(txs.get(i));
            }
        }
        // decimals merged into one value take one scale in the database (DuckDB's CASE pads 1.5 to 1.50), each its
        // own in Postgres: the same number, but its text is refused (a numeric that is no column's)
        return new Common(values, target == Kind.DECIMAL && merged > 1 ? Kind.NUMBER : target);
    }

    /** {@code x IS [NOT] DISTINCT FROM y}: equality in which two NULLs are equal (Pure's {@code ==}); never NULL. */
    static Tx isDistinct(SqlToPure translator, SqlAst.IsDistinct d, Scope scope, String row) {
        ValueSpecification same;
        if (d.left() instanceof SqlAst.NullLit || d.right() instanceof SqlAst.NullLit) {
            SqlAst.Expr other = d.left() instanceof SqlAst.NullLit ? d.right() : d.left();
            same = call("isEmpty", valueOf(translator.tx(other, scope, row)));
        } else {
            List<Tx> sides = translator.comparableValues(d.left(), d.right(), scope, row, d.pos());
            same = call("equal", valueOf(sides.get(0)), valueOf(sides.get(1)));
        }
        Tx notDistinct = Tx.condition(same, call("not", same), false);
        return d.negated() ? notDistinct : not(notDistinct);
    }

    static Tx inList(SqlToPure translator, SqlAst.InList in, Scope scope, String row) {
        List<Tx> operands = new ArrayList<>();
        operands.add(translator.tx(in.operand(), scope, row));
        boolean nullInList = false;
        for (SqlAst.Expr v : in.values()) {
            if (v instanceof SqlAst.NullLit) {
                nullInList = true;
                continue;
            }
            operands.add(translator.comparable(in.operand(), v, scope, row, v.pos()).get(1));
        }
        if (operands.size() == 1) {
            // x IN (NULL): never TRUE, never FALSE
            return nullOf(Kind.BOOLEAN);
        }
        boolean nullableElement = false;
        for (Tx element : operands.subList(1, operands.size())) {
            nullableElement |= element.nullable();
        }
        Tx test;
        if (nullableElement) {
            // an element that may be NULL: x = e for each, in three-valued OR -- TRUE where one holds, FALSE where every
            // one fails, else NULL (x IN (col, 5) is TRUE for x = 5 whatever col is)
            ValueSpecification holds = null;
            ValueSpecification fails = null;
            for (Tx element : operands.subList(1, operands.size())) {
                Tx eq = compare(SqlAst.BinaryOp.EQ, operands.get(0), element);
                holds = holds == null ? eq.vs() : call("or", holds, eq.vs());
                fails = fails == null ? eq.falseWhen() : call("and", fails, eq.falseWhen());
            }
            test = Tx.condition(java.util.Objects.requireNonNull(holds), java.util.Objects.requireNonNull(fails), true);
        } else {
            test = strictTest(operands, v -> call("in", v.get(0), new PureCollection(v.subList(1, v.size()))));
        }
        if (nullInList) {
            // a NULL in the list: TRUE where the value is found, else NULL (it might equal the NULL), never FALSE
            test = Tx.condition(test.vs(), new CBoolean(false), true);
        }
        return in.negated() ? not(test) : test;
    }

    static Tx between(SqlToPure translator, SqlAst.Between b, Scope scope, String row) {
        List<Tx> low = translator.comparable(b.operand(), b.low(), scope, row, b.pos());
        List<Tx> high = translator.comparable(b.operand(), b.high(), scope, row, b.pos());
        // x >= low AND x <= high, in three-valued logic: a NULL bound leaves the other to decide FALSE
        Tx l = compare(SqlAst.BinaryOp.GE, low.get(0), low.get(1));
        Tx h = compare(SqlAst.BinaryOp.LE, high.get(0), high.get(1));
        Tx test = Tx.condition(call("and", l.vs(), h.vs()), call("or", l.falseWhen(), h.falseWhen()),
                l.nullable() || h.nullable());
        return b.negated() ? not(test) : test;
    }

    /** {@code LIKE} by the shape of its constant pattern: no wildcard is equality; a trailing, leading or enclosing
     *  {@code %} alone is startsWith, endsWith or contains; anything else is the equivalent regular expression. */
    static Tx like(SqlToPure translator, SqlAst.Like l, Scope scope, String row) {
        Tx operand = translator.tx(l.operand(), scope, row);
        if (operand.kind() != Kind.STRING) {
            throw SqlTranslationException.invalid("LIKE needs a text value", l.operand().pos());
        }
        if (!(l.pattern() instanceof SqlAst.StringLit pattern)) {
            throw SqlTranslationException.unsupported("LIKE with a pattern that is not a constant is not supported"
                    + " yet", l.pattern().pos());
        }
        char escape = '\\';
        SqlAst.Expr esc = l.escape();
        if (esc != null) {
            if (!(esc instanceof SqlAst.StringLit e) || e.value().length() > 1) {
                throw SqlTranslationException.invalid("invalid escape string: it must be empty or one character",
                        esc.pos());
            }
            escape = e.value().isEmpty() ? 0 : e.value().charAt(0);
        }
        // the pattern as literal runs between wildcards: run, wildcard, run, ..., run
        List<String> parts = new ArrayList<>();
        StringBuilder run = new StringBuilder();
        String p = pattern.value();
        for (int i = 0; i < p.length(); i++) {
            char c = p.charAt(i);
            if (escape != 0 && c == escape) {
                if (i + 1 >= p.length()) {
                    throw SqlTranslationException.invalid("LIKE pattern must not end with escape character",
                            pattern.pos());
                }
                run.append(p.charAt(++i));
            } else if (c == '%' || c == '_') {
                parts.add(run.toString());
                parts.add(String.valueOf(c));
                run.setLength(0);
            } else {
                run.append(c);
            }
        }
        parts.add(run.toString());
        List<String> wild = new ArrayList<>();
        for (int i = 1; i < parts.size(); i += 2) {
            wild.add(parts.get(i));
        }
        boolean ci = l.caseInsensitive();
        boolean onlyPercent = !wild.contains("_");
        java.util.function.Function<List<ValueSpecification>, ValueSpecification> test;
        if (wild.isEmpty()) {
            test = v -> textTest("equal", v.get(0), parts.get(0), ci);
        } else if (onlyPercent && wild.size() == 1 && parts.get(2).isEmpty() && !parts.get(0).isEmpty()) {
            test = v -> textTest("startsWith", v.get(0), parts.get(0), ci);
        } else if (onlyPercent && wild.size() == 1 && parts.get(0).isEmpty() && !parts.get(2).isEmpty()) {
            test = v -> textTest("endsWith", v.get(0), parts.get(2), ci);
        } else if (onlyPercent && wild.size() == 2 && parts.get(0).isEmpty() && parts.get(4).isEmpty()
                && !parts.get(2).isEmpty()) {
            test = v -> textTest("contains", v.get(0), parts.get(2), ci);
        } else {
            StringBuilder regex = new StringBuilder(ci ? "(?i)^" : "^");
            for (int i = 0; i < parts.size(); i++) {
                String part = parts.get(i);
                if (i % 2 == 1) {
                    regex.append(part.equals("%") ? ".*" : ".");
                } else {
                    for (int k = 0; k < part.length(); k++) {
                        char c = part.charAt(k);
                        if ("\\.[]{}()*+?^$|".indexOf(c) >= 0) {
                            regex.append('\\');
                        }
                        regex.append(c);
                    }
                }
            }
            regex.append('$');
            String re = regex.toString();
            test = v -> call("matches", v.get(0), new CString(re));
        }
        Tx result = strictTest(List.of(operand), test);
        return l.negated() ? not(result) : result;
    }

    private static ValueSpecification textTest(String fn, ValueSpecification value, String text,
            boolean caseInsensitive) {
        return caseInsensitive ? call(fn, call("toLower", value), call("toLower", new CString(text)))
                : call(fn, value, new CString(text));
    }

    /** One row of a subquery compared ({@link InputDialect#quantifiedTest}): {@code left op each}, a string constant
     *  read as {@code each}'s type. */
    static Tx quantifiedTest(SqlAst.BinaryOp op, SqlAst.Expr left, Tx each, SqlAst.Pos pos, SqlToPure.At at) {
        Tx value = left instanceof SqlAst.StringLit s && readsStrings(each.kind())
                ? fromString(at.translator().single(s, each.detail()), each.kind()) : at.tx(left);
        comparableKinds(value, each, pos);
        return compare(op, value, each);
    }

    /** A kind's Postgres type name, for Postgres's messages. */
    static String postgresName(Kind k) {
        return switch (k) {
            case INTEGER -> "integer";
            case WHOLE -> "bigint";
            case FLOAT -> "double precision";
            case DECIMAL, NUMBER, INEXACT -> "numeric";
            case STRING -> "text";
            case BOOLEAN -> "boolean";
            case DATE -> "date";
            case DATETIME -> "timestamp without time zone";
            case TIME -> "time without time zone";
            case ZONED -> "timestamp with time zone";
            case ZONED_TIME -> "time with time zone";
            case DURATION -> "interval";
            case JSON -> "jsonb";
            case NULL -> "unknown";
            case OTHER -> "an enumeration";
        };
    }
}
