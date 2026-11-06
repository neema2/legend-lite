// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.ts;

import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlTranslationException;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * A {@code #TS} QUERY AS TEXT (D12, docs/TS_ONE_TREE_PLAN_2026_10_31.md): the canonical text of what {@link TsParser}
 * reads -- {@code parse(print(q))} prints as {@code print(q)}, and means what {@code q} means. Its marks are written
 * back as the language writes them: a span ({@code 3mo}), {@code all} and {@code whole} as a window, a subscript
 * ({@code close['SPX']}), an option by name ({@code type => 'log'}), a variable of the enclosing Pure ({@code $px}).
 * Every expression is Postgres's, parenthesized where its precedence asks (a parenthesis is no node: the text reads
 * back as the same tree). What the parser never reads in an island (a subquery as a value, a window clause, a
 * parameter) is refused by name, never printed approximately.
 */
public final class TsPrinter {

    private final TsAst.Query query;

    private TsPrinter(TsAst.Query query) {
        this.query = query;
    }

    /** {@code q}'s text, on one line. */
    public static String print(TsAst.Query q) {
        return new TsPrinter(q).query(q);
    }

    /** One item's text, as a formula box shows a series ({@link TsParser#item}, A3 step 2): read back, the same item. */
    public static String print(TsAst.Line line) {
        TsAst.Query alone = new TsAst.Query(List.of(), List.of(line.item()), List.of(), null, "LAST", null, null,
                line.alls(), line.spans(), null);
        return new TsPrinter(alone).item(line.item());
    }

    private String query(TsAst.Query q) {
        if (q.plain() != null) {
            // plain SQL over a series query (A9, the spec's §8): the printer writes the language's own clauses alone, so
            // it is refused by name, never printed without its SELECT's items (the second review's finding 2, 2026-11-06)
            throw SqlTranslationException.unsupported("#TS prints no plain SQL over a series query yet: its text is"
                    + " the query as written", q.plain().pos());
        }
        StringBuilder b = new StringBuilder();
        if (!q.withs().isEmpty()) {
            b.append("WITH ").append(q.withs().stream().map(w -> ident(w.name()) + " AS "
                    + (w.body() instanceof TsAst.Nested n ? "TS (" + query(n.query()) + ")" : parenthesized(w.body())))
                    .collect(Collectors.joining(", "))).append(' ');
        }
        b.append("SELECT ").append(q.items().stream().map(this::item).collect(Collectors.joining(", ")));
        b.append(" FROM ").append(q.sources().stream().map(this::from).collect(Collectors.joining(", ")));
        if (q.sample() != null) {
            b.append(" SAMPLE BY ").append(span(q.sample()));
            if (!"LAST".equals(q.dated())) {
                b.append(" AT ").append(q.dated());
            }
        }
        if (q.fill() != null) {
            b.append(" FILL(").append("ZERO".equals(q.fill().how()) ? "0" : q.fill().how()).append(')');
        }
        if (q.filter() != null) {
            b.append(" FILTER ").append(expr(q.filter(), 0));
        }
        return b.toString();
    }

    private String item(TsAst.Item i) {
        if (i.expr() == null) {
            return "*";
        }
        // a column under its own name is written bare: day, never day AS day (the reader names it so)
        boolean own = i.alias() != null && i.expr() instanceof SqlAst.ColumnRef c && c.parts().size() == 1
                && c.parts().get(0).text().equals(i.alias().text()) && c.parts().get(0).quoted() == i.alias().quoted()
                && !query.spans().containsKey(c.pos()) && !query.alls().contains(c.pos());
        return expr(i.expr(), 0) + (i.alias() == null || own ? "" : " AS " + ident(i.alias()));
    }

    private String from(TsAst.From f) {
        StringBuilder b = new StringBuilder(source(f.source()));
        // a source under its own name is written bare: FROM px ON date, never FROM px px (the reader names it so)
        boolean own = f.alias() != null && !f.alias().quoted() && f.source() instanceof TsAst.Named n
                && n.name().equals(f.alias().text());
        if (f.alias() != null && !own) {
            b.append(' ').append(ident(f.alias()));
        }
        b.append(" ON ").append(path(f.on()));
        if (f.by() != null) {
            b.append(" BY ").append(path(f.by()));
        }
        return b.toString();
    }

    private String source(TsAst.Source s) {
        return switch (s) {
            case TsAst.Named n -> n.name();
            case TsAst.Variable v -> "$" + v.name();
            case TsAst.Nested n -> "TS (" + query(n.query()) + ")";
            case TsAst.Filtered f -> parenthesized(f);
            case TsAst.Flat f -> parenthesized(f);
        };
    }

    /** A source in parentheses: {@code (source WHERE condition)}, {@code (SELECT items FROM source [WHERE ...])}. */
    private String parenthesized(TsAst.Source s) {
        return switch (s) {
            case TsAst.Filtered f -> "(" + source(f.of()) + " WHERE " + expr(f.where(), 0) + ")";
            case TsAst.Flat f -> "(SELECT " + f.items().stream().map(this::item).collect(Collectors.joining(", "))
                    + " FROM " + source(f.from()) + (f.where() == null ? "" : " WHERE " + expr(f.where(), 0)) + ")";
            case TsAst.Named n -> throw refused("a named source in parentheses", n.pos());
            case TsAst.Variable v -> throw refused("a variable in parentheses", v.pos());
            case TsAst.Nested n -> "TS (" + query(n.query()) + ")";
        };
    }

    private static String span(TsAst.Span s) {
        return s.n() + s.unit();
    }

    private static String path(SqlAst.ColumnRef c) {
        return c.parts().stream().map(TsPrinter::ident).collect(Collectors.joining("."));
    }

    /** An identifier as written: quoted where it was (its quotes doubled). */
    static String ident(SqlAst.Ident i) {
        return i.quoted() ? "\"" + i.text().replace("\"", "\"\"") + "\"" : i.text();
    }

    // ------------------------------------------------------------------ expressions

    /** Postgres's precedence, lowest first: OR, AND, NOT, IS, a comparison, BETWEEN/IN/LIKE, another operator,
     *  + -, * / %, ^, a unary sign, an atom. */
    private static final int OR = 1;
    private static final int AND = 2;
    private static final int NOT = 3;
    private static final int IS = 4;
    private static final int COMPARE = 5;
    private static final int RANGE = 6;
    private static final int OTHER = 7;
    private static final int ADD = 8;
    private static final int MULTIPLY = 9;
    private static final int POWER = 10;
    private static final int SIGN = 11;
    private static final int ATOM = 12;

    /** {@code e} as text, in parentheses where its precedence is below {@code min}. */
    private String expr(SqlAst.Expr e, int min) {
        int p = precedence(e);
        String text = bare(e);
        return p < min ? "(" + text + ")" : text;
    }

    private static int precedence(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.Binary b -> switch (b.op()) {
                case OR -> OR;
                case AND -> AND;
                case EQ, NE, LT, LE, GT, GE -> COMPARE;
                case CONCAT -> OTHER;
                case PLUS, MINUS -> ADD;
                case TIMES, DIVIDE, MODULO -> MULTIPLY;
                case POWER -> POWER;
            };
            case SqlAst.Unary u -> u.op() == SqlAst.UnaryOp.NOT ? NOT : SIGN;
            case SqlAst.IsNull i -> IS;
            case SqlAst.IsTruth i -> IS;
            case SqlAst.IsDistinct i -> IS;
            case SqlAst.InList i -> RANGE;
            case SqlAst.Between b -> RANGE;
            case SqlAst.Like l -> RANGE;
            case SqlAst.Operator o -> OTHER;
            default -> ATOM;
        };
    }

    private String bare(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.ColumnRef c -> column(c);
            case SqlAst.IntegerLit i -> i.value().toString();
            case SqlAst.NumericLit n -> n.value().toPlainString();
            case SqlAst.StringLit s -> string(s.value());
            case SqlAst.BooleanLit b -> b.value() ? "TRUE" : "FALSE";
            case SqlAst.NullLit n -> "NULL";
            case SqlAst.TypedLit t -> type(t.type()) + " " + string(t.value());
            case SqlAst.Unary u -> unary(u);
            case SqlAst.Binary b -> binary(b);
            case SqlAst.Operator o -> (o.left() == null ? "" : expr(o.left(), OTHER) + " ") + o.operator() + " "
                    + expr(o.right(), OTHER + 1);
            case SqlAst.ArrayConstructor a -> "ARRAY[" + list(a.elements()) + "]";
            case SqlAst.IsNull i -> expr(i.operand(), OTHER) + (i.negated() ? " IS NOT NULL" : " IS NULL");
            case SqlAst.IsTruth i -> expr(i.operand(), OTHER) + " IS " + (i.negated() ? "NOT " : "")
                    + i.truth().name();
            case SqlAst.IsDistinct i -> expr(i.left(), OTHER) + " IS " + (i.negated() ? "NOT " : "")
                    + "DISTINCT FROM " + expr(i.right(), OTHER);
            case SqlAst.InList i -> expr(i.operand(), OTHER) + (i.negated() ? " NOT IN (" : " IN (") + list(i.values())
                    + ")";
            case SqlAst.Between b -> expr(b.operand(), OTHER) + (b.negated() ? " NOT BETWEEN " : " BETWEEN ")
                    + expr(b.low(), OTHER) + " AND " + expr(b.high(), OTHER);
            case SqlAst.Like l -> expr(l.operand(), OTHER) + (l.negated() ? " NOT LIKE " : " LIKE ")
                    + expr(l.pattern(), OTHER) + (l.escape() == null ? "" : " ESCAPE " + expr(l.escape(), OTHER));
            case SqlAst.Case c -> kase(c);
            case SqlAst.FunctionCall f -> applied(f);
            case SqlAst.Cast c -> "CAST(" + expr(c.operand(), 0) + " AS " + type(c.type()) + ")";
            case SqlAst.Param p -> throw refused("a parameter", p.pos());
            case SqlAst.Subscript s -> throw refused("an array's subscript", s.pos());
            case SqlAst.ScalarSubquery s -> throw refused("a subquery as a value", s.pos());
            case SqlAst.Exists x -> throw refused("EXISTS", x.pos());
            case SqlAst.InSubquery i -> throw refused("IN a subquery", i.pos());
            case SqlAst.Quantified q -> throw refused("a comparison with ANY or ALL", q.pos());
            case SqlAst.Window w -> throw refused("a window clause (OVER)", w.pos());
        };
    }

    /** A column, a window's {@code all} or {@code whole}, or a span ({@code 3mo}), each where the parser marked it. */
    private String column(SqlAst.ColumnRef c) {
        TsAst.Span span = query.spans().get(c.pos());
        if (span != null && c.parts().size() == 1) {
            return span(span);
        }
        if (query.alls().contains(c.pos()) && c.parts().size() == 1) {
            String name = c.parts().get(0).text();
            if (TsParser.ALL.equals(name)) {
                return "all";
            }
            if (TsParser.WHOLE.equals(name)) {
                return "whole";
            }
        }
        return path(c);
    }

    private String unary(SqlAst.Unary u) {
        if (u.op() == SqlAst.UnaryOp.NOT) {
            return "NOT " + expr(u.operand(), NOT);
        }
        String operand = expr(u.operand(), ATOM);
        // never "--": a comment
        return (u.op() == SqlAst.UnaryOp.MINUS ? "-" : "+") + (operand.startsWith("-") || operand.startsWith("+")
                ? "(" + operand + ")" : operand);
    }

    private String binary(SqlAst.Binary b) {
        int p = precedence(b);
        String op = switch (b.op()) {
            case OR -> "OR";
            case AND -> "AND";
            case EQ -> "=";
            case NE -> "<>";
            case LT -> "<";
            case LE -> "<=";
            case GT -> ">";
            case GE -> ">=";
            case PLUS -> "+";
            case MINUS -> "-";
            case TIMES -> "*";
            case DIVIDE -> "/";
            case MODULO -> "%";
            case POWER -> "^";
            case CONCAT -> "||";
        };
        // left-associative, a comparison none: its left as its precedence allows, its right one above
        boolean chains = p != COMPARE;
        return expr(b.left(), chains ? p : p + 1) + " " + op + " " + expr(b.right(), p + 1);
    }

    private String kase(SqlAst.Case c) {
        StringBuilder b = new StringBuilder("CASE");
        if (c.operand() != null) {
            b.append(' ').append(expr(c.operand(), 0));
        }
        for (SqlAst.When w : c.whens()) {
            b.append(" WHEN ").append(expr(w.condition(), 0)).append(" THEN ").append(expr(w.result(), 0));
        }
        if (c.otherwise() != null) {
            b.append(" ELSE ").append(expr(c.otherwise(), 0));
        }
        return b.append(" END").toString();
    }

    /** A call; a subscript ({@link TsParser#PICK}) and an option by name ({@link TsParser#OPTION}) as written. */
    private String applied(SqlAst.FunctionCall f) {
        if (!f.orderBy().isEmpty() || !f.withinGroup().isEmpty() || f.filter() != null) {
            throw refused("an aggregate's ORDER BY, WITHIN GROUP or FILTER", f.pos());
        }
        String name = f.name().size() == 1 ? f.name().get(0).text() : null;
        if (TsParser.PICK.equals(name) && !f.args().isEmpty()) {
            return expr(f.args().get(0), ATOM) + "[" + list(f.args().subList(1, f.args().size())) + "]";
        }
        if (TsParser.OPTION.equals(name) && f.args().size() == 2 && f.args().get(0) instanceof SqlAst.StringLit o) {
            return o.value() + " => " + expr(f.args().get(1), 0);
        }
        // a name the language reads as a call (gs-quant's union) is written bare: the parser marks it again
        String called = f.name().size() == 1 && "union".equals(name) ? name
                : f.name().stream().map(TsPrinter::ident).collect(Collectors.joining("."));
        if (f.star()) {
            return called + "(*)";
        }
        return called + "(" + (f.distinct() ? "DISTINCT " : "") + list(f.args()) + ")";
    }

    private String list(List<SqlAst.Expr> es) {
        List<String> out = new ArrayList<>(es.size());
        for (SqlAst.Expr e : es) {
            out.add(expr(e, 0));
        }
        return String.join(", ", out);
    }

    private static String type(SqlAst.TypeName t) {
        String name = t.name().toUpperCase(Locale.ROOT);
        return t.modifiers().isEmpty() ? name : name + "(" + t.modifiers().stream().map(String::valueOf)
                .collect(Collectors.joining(", ")) + ")";
    }

    /** A string literal: its quotes doubled. */
    static String string(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private static SqlTranslationException refused(String what, SqlAst.Pos pos) {
        return SqlTranslationException.unsupported("#TS prints no " + what + ": the language's text has none", pos);
    }
}
