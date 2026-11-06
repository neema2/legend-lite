// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlTranslationException;
import com.legend.protocol.ts.TsAst;
import com.legend.protocol.ts.TsParser;
import com.legend.protocol.ts.TsPrinter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * ONE ITEM'S TREE, AS DATA (A3 step 2, docs/TS_A3_ONE_READER_PLAN_2026_11_03.md): a series as {@link TsParser#item}
 * reads it, written as plain maps and lists for a client to walk -- a formula box's drawing (its unit, its level, the
 * columns it reads) without a grammar of its own. Core's tree, never a client's: each node its kind ({@code k}), its
 * place ({@code at}: line and column), and its parts. A window of everything is {@code all} or {@code whole}, a span
 * {@code span} with its count and unit, as the parser marks them; a subscript ({@code close['AAA']}) {@code pick}; an
 * option by name ({@code type => logarithmic}) {@code option}. What an item never is -- a parameter, a subquery, a
 * window clause -- is refused by name, as {@link TsPrinter} refuses it. The server's answer, so written here, beside the
 * route that sends it ({@link PureV1Api#tsItem}), not in the translator's package: a JSON key is no word's parameter.
 */
public final class TsTree {

    private final TsAst.Line line;

    private TsTree(TsAst.Line line) {
        this.line = line;
    }

    /** {@code line} as data: {@code {alias, expr}}, the alias null where the item has none. */
    public static Map<String, Object> of(TsAst.Line line) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("alias", line.item().alias() == null ? null : line.item().alias().text());
        out.put("expr", line.item().expr() == null ? null : new TsTree(line).node(line.item().expr()));
        return out;
    }

    private Map<String, Object> node(SqlAst.Expr e) {
        Map<String, Object> n = new LinkedHashMap<>();
        switch (e) {
            case SqlAst.ColumnRef c -> {
                TsAst.Span span = line.spans().get(c.pos());
                if (span != null && c.parts().size() == 1) {
                    n.put("k", "span");
                    n.put("n", span.n());
                    n.put("unit", span.unit());
                } else if (line.alls().contains(c.pos()) && c.parts().size() == 1) {
                    n.put("k", TsParser.WHOLE.equals(c.parts().get(0).text()) ? "whole" : "all");
                } else {
                    n.put("k", "column");
                    n.put("parts", c.parts().stream().map(SqlAst.Ident::text).toList());
                }
            }
            case SqlAst.IntegerLit i -> {
                n.put("k", "number");
                n.put("text", i.value().toString());
            }
            case SqlAst.NumericLit d -> {
                n.put("k", "number");
                n.put("text", d.value().toPlainString());
            }
            case SqlAst.StringLit s -> {
                n.put("k", "string");
                n.put("value", s.value());
            }
            case SqlAst.BooleanLit b -> {
                n.put("k", "boolean");
                n.put("value", b.value());
            }
            case SqlAst.NullLit x -> n.put("k", "null");
            case SqlAst.TypedLit t -> {
                n.put("k", "typed");
                n.put("type", t.type().name());
                n.put("value", t.value());
            }
            case SqlAst.Unary u -> {
                n.put("k", "unary");
                n.put("op", u.op().name());
                n.put("operand", node(u.operand()));
            }
            case SqlAst.Binary b -> {
                n.put("k", "binary");
                n.put("op", b.op().name());
                n.put("left", node(b.left()));
                n.put("right", node(b.right()));
            }
            case SqlAst.Operator o -> {
                n.put("k", "operator");
                n.put("op", o.operator());
                n.put("left", o.left() == null ? null : node(o.left()));
                n.put("right", node(o.right()));
            }
            case SqlAst.IsNull i -> {
                n.put("k", "isNull");
                n.put("operand", node(i.operand()));
                n.put("negated", i.negated());
            }
            case SqlAst.IsTruth i -> {
                n.put("k", "isTruth");
                n.put("operand", node(i.operand()));
                n.put("truth", i.truth().name());
                n.put("negated", i.negated());
            }
            case SqlAst.IsDistinct i -> {
                n.put("k", "isDistinct");
                n.put("left", node(i.left()));
                n.put("right", node(i.right()));
                n.put("negated", i.negated());
            }
            case SqlAst.InList i -> {
                n.put("k", "in");
                n.put("operand", node(i.operand()));
                n.put("values", nodes(i.values()));
                n.put("negated", i.negated());
            }
            case SqlAst.Between b -> {
                n.put("k", "between");
                n.put("operand", node(b.operand()));
                n.put("low", node(b.low()));
                n.put("high", node(b.high()));
                n.put("negated", b.negated());
            }
            case SqlAst.Like l -> {
                n.put("k", "like");
                n.put("operand", node(l.operand()));
                n.put("pattern", node(l.pattern()));
                n.put("escape", l.escape() == null ? null : node(l.escape()));
                n.put("negated", l.negated());
                n.put("caseInsensitive", l.caseInsensitive());
            }
            case SqlAst.Case c -> {
                n.put("k", "case");
                n.put("operand", c.operand() == null ? null : node(c.operand()));
                List<Map<String, Object>> whens = new ArrayList<>();
                for (SqlAst.When w : c.whens()) {
                    Map<String, Object> when = new LinkedHashMap<>();
                    when.put("when", node(w.condition()));
                    when.put("then", node(w.result()));
                    whens.add(when);
                }
                n.put("whens", whens);
                n.put("otherwise", c.otherwise() == null ? null : node(c.otherwise()));
            }
            case SqlAst.FunctionCall f -> function(f, n);
            case SqlAst.Cast c -> {
                n.put("k", "cast");
                n.put("operand", node(c.operand()));
                n.put("type", c.type().name());
            }
            case SqlAst.ArrayConstructor a -> {
                n.put("k", "array");
                n.put("elements", nodes(a.elements()));
            }
            case SqlAst.Param p -> throw refused("a parameter", p.pos());
            case SqlAst.Subscript s -> throw refused("an array's subscript", s.pos());
            case SqlAst.ScalarSubquery s -> throw refused("a subquery as a value", s.pos());
            case SqlAst.Exists x -> throw refused("EXISTS", x.pos());
            case SqlAst.InSubquery i -> throw refused("IN a subquery", i.pos());
            case SqlAst.Quantified q -> throw refused("a comparison with ANY or ALL", q.pos());
            case SqlAst.Window w -> throw refused("a window clause (OVER)", w.pos());
        }
        n.put("at", List.of(e.pos().line(), e.pos().column()));
        return n;
    }

    /** A call: a subscript ({@link TsParser#PICK}) as {@code pick}, an option by name ({@link TsParser#OPTION}) as
     *  {@code option}, else {@code call} with its name as written. */
    private void function(SqlAst.FunctionCall f, Map<String, Object> n) {
        if (!f.orderBy().isEmpty() || !f.withinGroup().isEmpty() || f.filter() != null || f.distinct()) {
            throw refused("an aggregate's DISTINCT, ORDER BY, WITHIN GROUP or FILTER", f.pos());
        }
        String name = f.name().size() == 1 ? f.name().get(0).text() : null;
        if (TsParser.PICK.equals(name) && !f.args().isEmpty()) {
            n.put("k", "pick");
            n.put("of", node(f.args().get(0)));
            n.put("keys", nodes(f.args().subList(1, f.args().size())));
        } else if (TsParser.OPTION.equals(name) && f.args().size() == 2
                && f.args().get(0) instanceof SqlAst.StringLit o) {
            n.put("k", "option");
            n.put("name", o.value());
            n.put("value", node(f.args().get(1)));
        } else {
            n.put("k", "call");
            n.put("name", f.name().stream().map(SqlAst.Ident::text).toList());
            n.put("args", nodes(f.args()));
            n.put("star", f.star());
        }
    }

    private List<Map<String, Object>> nodes(List<SqlAst.Expr> es) {
        List<Map<String, Object>> out = new ArrayList<>(es.size());
        for (SqlAst.Expr e : es) {
            out.add(node(e));
        }
        return out;
    }

    private static SqlTranslationException refused(String what, SqlAst.Pos pos) {
        return SqlTranslationException.unsupported(what + " is no item of #TS's", pos);
    }
}
