// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.ts;

import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.CDate;
import com.legend.protocol.spec.CDecimal;
import com.legend.protocol.spec.CFloat;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.IslandLanguage;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.SqlIsland;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;
import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlTranslationException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * SHOW #TS (D12, the user, 2026-10-31: "the Show #TS of the grid to actually show that single statement view"): a
 * query of {@code #TS} over grids -- each grid's relation bound by {@code let}, then the island reading it
 * ({@code let px = ...; #TS{... FROM $px ...}#}) -- as the one statement a person would write: each grid a flat source
 * named by WITH ({@code WITH px AS (SELECT instrument.ticker AS ticker, ... FROM MarketQuote WHERE ...)}), the island
 * reading it by that name, printed by {@link TsPrinter}. For display only, and exact as defined: the statement read
 * back answers the same rows, and prints the same text. A grid the flat form cannot say yet (a calculated column, a
 * group-by, a join, a filter of a kind not written yet) is refused by name -- the window then shows the query as it
 * is, never an approximation.
 */
public final class TsShow {

    private TsShow() {
    }

    /** The one statement ({@code refused} null), or why there is none ({@code statement} null). */
    public record Shown(@com.legend.base.Nullable String statement, @com.legend.base.Nullable String refused) {
    }

    /** No position: what the statement writes that its island's text did not. */
    private static final SqlAst.Pos NONE = new SqlAst.Pos(0, 0);

    /** {@code query} as one {@code #TS} statement, or why not. */
    public static Shown of(LambdaFunction query) {
        List<ValueSpecification> body = query.body();
        if (body.isEmpty() || !(body.get(body.size() - 1) instanceof SqlIsland island)
                || island.language() != IslandLanguage.TS) {
            return new Shown(null, "it is no #TS query");
        }
        Map<String, TsAst.Source> grids = new LinkedHashMap<>();
        for (ValueSpecification v : body.subList(0, body.size() - 1)) {
            if (!(v instanceof AppliedFunction let) || !binds(let)
                    || !(let.parameters().get(0) instanceof CString name)) {
                return new Shown(null, "it does more than bind its grids");
            }
            try {
                TsAst.Source grid = flat(let.parameters().get(1));
                // a WITH names a source in parentheses: a table's rows as SELECT *
                grids.put(name.value(), grid instanceof TsAst.Named table
                        ? new TsAst.Flat(List.of(new TsAst.Item(null, null, NONE)), table, null, NONE) : grid);
            } catch (Refused r) {
                return new Shown(null, name.value() + "'s grid " + r.getMessage());
            }
        }
        TsAst.Query q;
        try {
            q = TsParser.parse(island.sql());
        } catch (SqlTranslationException e) {
            return new Shown(null, "its #TS does not read: " + e.getMessage());
        }
        List<TsAst.With> withs = new ArrayList<>();
        for (Map.Entry<String, TsAst.Source> g : grids.entrySet()) {
            withs.add(new TsAst.With(new SqlAst.Ident(g.getKey(), false, NONE), g.getValue()));
        }
        withs.addAll(q.withs());
        List<TsAst.From> sources = q.sources().stream().map(f -> new TsAst.From(named(f.source(), grids), f.alias(),
                f.on(), f.by(), f.pos())).toList();
        List<TsAst.With> own = withs.stream().map(w -> new TsAst.With(w.name(), named(w.body(), grids))).toList();
        TsAst.Query one = new TsAst.Query(own, q.items(), sources, q.sample(), q.dated(), q.fill(), q.filter(), q.alls(),
                q.spans());
        try {
            return new Shown(TsPrinter.print(one), null);
        } catch (SqlTranslationException e) {
            return new Shown(null, e.getMessage());
        }
    }

    /** Whether {@code f} binds a grid: {@code let name = relation}. */
    private static boolean binds(AppliedFunction f) {
        return switch (f.function()) {
            case "letFunction" -> f.parameters().size() == 2;
            default -> false;
        };
    }

    /** The class {@code v} reads every instance of ({@code C.all()}), or null. */
    private static @com.legend.base.Nullable PackageableElementPtr everyOf(ValueSpecification v) {
        if (v instanceof AppliedFunction f && f.parameters().size() == 1
                && f.parameters().get(0) instanceof PackageableElementPtr cls) {
            return switch (f.function()) {
                case "getAll" -> cls;
                default -> null;
            };
        }
        return null;
    }

    /** {@code s} with each variable of a grid read by its WITH name. */
    private static TsAst.Source named(TsAst.Source s, Map<String, TsAst.Source> grids) {
        return switch (s) {
            case TsAst.Variable v -> grids.containsKey(v.name()) ? new TsAst.Named(v.name(), v.pos()) : v;
            case TsAst.Filtered f -> new TsAst.Filtered(named(f.of(), grids), f.where(), f.pos());
            case TsAst.Flat f -> new TsAst.Flat(f.items(), named(f.from(), grids), f.where(), f.pos());
            case TsAst.Nested n -> new TsAst.Nested(new TsAst.Query(n.query().withs().stream()
                    .map(w -> new TsAst.With(w.name(), named(w.body(), grids))).toList(), n.query().items(),
                    n.query().sources().stream().map(f -> new TsAst.From(named(f.source(), grids), f.alias(), f.on(),
                            f.by(), f.pos())).toList(), n.query().sample(), n.query().dated(), n.query().fill(),
                    n.query().filter(), n.query().alls(), n.query().spans()), n.pos());
            case TsAst.Named n -> n;
        };
    }

    // ------------------------------------------------------------------ a grid's relation as a flat source

    /** Why a grid has no flat form yet, in words. */
    private static final class Refused extends RuntimeException {
        Refused(String why) {
            super(why, null, false, false);
        }
    }

    /**
     * {@code relation} as a flat source: a class's paths projected ({@code SELECT p.q AS name, ... FROM Class}), a
     * table ({@code db::Store.schema.T}), and a filter of either as its WHERE -- over the projected names, the flat
     * form projecting before it filters.
     */
    private static TsAst.Source flat(ValueSpecification relation) {
        if (relation instanceof AppliedFunction f) {
            switch (f.function()) {
                case "project" -> {
                    PackageableElementPtr cls = f.parameters().size() == 2 ? everyOf(f.parameters().get(0)) : null;
                    if (cls != null && f.parameters().get(1) instanceof ColSpecArray cols) {
                        List<TsAst.Item> items = new ArrayList<>();
                        for (ColSpec c : cols.colSpecs()) {
                            items.add(new TsAst.Item(path(c), ident(c.name()), NONE));
                        }
                        return new TsAst.Flat(items, new TsAst.Named(cls.fullPath(), NONE), null, NONE);
                    }
                    throw new Refused("projects what is no class's paths");
                }
                case "tableReference" -> {
                    if (f.parameters().size() == 2 && f.parameters().get(0) instanceof PackageableElementPtr db
                            && f.parameters().get(1) instanceof CString table) {
                        return new TsAst.Named(db.fullPath() + "." + table.value(), NONE);
                    }
                    throw new Refused("reads a table it does not name");
                }
                case "filter" -> {
                    if (f.parameters().size() == 2 && f.parameters().get(1) instanceof LambdaFunction l
                            && l.parameters().size() == 1 && l.body().size() == 1) {
                        TsAst.Source of = flat(f.parameters().get(0));
                        SqlAst.Expr where = condition(l.body().get(0), l.parameters().get(0).name());
                        return switch (of) {
                            case TsAst.Flat flat -> {
                                // a flat source's WHERE reads its class's paths: each projected name as its path
                                SqlAst.Expr paths = paths(where, flat.items());
                                yield new TsAst.Flat(flat.items(), flat.from(), flat.where() == null ? paths
                                        : new SqlAst.Binary(SqlAst.BinaryOp.AND, flat.where(), paths, NONE), NONE);
                            }
                            case TsAst.Filtered filtered -> new TsAst.Filtered(filtered.of(), new SqlAst.Binary(
                                    SqlAst.BinaryOp.AND, filtered.where(), where, NONE), NONE);
                            default -> new TsAst.Filtered(of, where, NONE);
                        };
                    }
                    throw new Refused("filters by what is no condition of a row");
                }
                case "extend" -> throw new Refused("has a calculated column (" + names(f) + "): the flat form says"
                        + " a grid's paths and filters so far");
                default -> throw new Refused("is a " + f.function() + " of its rows: the flat form says a grid's paths"
                        + " and filters so far");
            }
        }
        throw new Refused("is no relation the flat form says");
    }

    /** {@code e} with each column a projected name as that item's path ({@code sym} as {@code instrument.ticker}). */
    private static SqlAst.Expr paths(SqlAst.Expr e, List<TsAst.Item> items) {
        return switch (e) {
            case SqlAst.ColumnRef c -> {
                if (c.parts().size() != 1) {
                    yield c;
                }
                for (TsAst.Item i : items) {
                    if (i.alias() != null && i.alias().text().equals(c.parts().get(0).text())) {
                        yield java.util.Objects.requireNonNull(i.expr());
                    }
                }
                throw new Refused("filters by " + c.parts().get(0).text() + ", a column it does not project");
            }
            case SqlAst.Binary b -> new SqlAst.Binary(b.op(), paths(b.left(), items), paths(b.right(), items), NONE);
            case SqlAst.Unary u -> new SqlAst.Unary(u.op(), paths(u.operand(), items), NONE);
            case SqlAst.IsNull n -> new SqlAst.IsNull(paths(n.operand(), items), n.negated(), NONE);
            case SqlAst.InList i -> new SqlAst.InList(paths(i.operand(), items), i.values(), i.negated(), NONE);
            default -> e;
        };
    }

    private static String names(AppliedFunction extend) {
        return extend.parameters().stream().skip(1).flatMap(p -> p instanceof ColSpec c ? java.util.stream.Stream.of(
                c.name()) : p instanceof ColSpecArray a ? a.colSpecs().stream().map(ColSpec::name)
                : java.util.stream.Stream.<String>empty()).reduce((a, b) -> a + ", " + b).orElse("?");
    }

    /** A projected column's path from its object: {@code x|$x.instrument.ticker} as {@code instrument.ticker}. An
     *  enumeration read as its values' names ({@code $x.side->toString()}, DataCube's way to read one) is refused by
     *  name: the #SQL reader casts no enumeration to text yet. */
    private static SqlAst.Expr path(ColSpec c) {
        LambdaFunction l = c.function1();
        if (l == null || l.parameters().size() != 1 || l.body().size() != 1) {
            throw new Refused("projects " + c.name() + " by what is no path");
        }
        ValueSpecification body = l.body().get(0);
        if (body instanceof AppliedFunction f) {
            switch (f.function()) {
                case "toString" -> throw new Refused("reads " + c.name() + ", an enumeration, as its values' names: the"
                        + " #SQL reader casts no enumeration to text yet, so the flat form cannot say it");
                default -> {
                }
            }
        }
        return path(c.name(), body, l);
    }

    private static SqlAst.Expr path(String name, ValueSpecification body, LambdaFunction l) {
        List<SqlAst.Ident> parts = new ArrayList<>();
        ValueSpecification v = body;
        while (v instanceof AppliedProperty p) {
            parts.add(0, ident(p.property()));
            v = p.receiver();
        }
        if (!(v instanceof Variable x) || !x.name().equals(l.parameters().get(0).name()) || parts.isEmpty()) {
            throw new Refused("projects " + name + " by what is no path of its object");
        }
        return new SqlAst.ColumnRef(parts, NONE);
    }

    /** A row's condition ({@code x|$x.sector == 'TECH' && ...}) as the WHERE Postgres reads to the same meaning. */
    private static SqlAst.Expr condition(ValueSpecification v, String row) {
        if (v instanceof AppliedFunction f) {
            List<ValueSpecification> ps = f.parameters();
            switch (f.function()) {
                case "and", "or" -> {
                    if (ps.size() == 2) {
                        SqlAst.BinaryOp op = switch (f.function()) {
                            case "and" -> SqlAst.BinaryOp.AND;
                            default -> SqlAst.BinaryOp.OR;
                        };
                        return new SqlAst.Binary(op, condition(ps.get(0), row), condition(ps.get(1), row), NONE);
                    }
                }
                case "not" -> {
                    if (ps.size() == 1) {
                        return new SqlAst.Unary(SqlAst.UnaryOp.NOT, condition(ps.get(0), row), NONE);
                    }
                }
                case "equal", "lessThan", "lessThanEqual", "greaterThan", "greaterThanEqual" -> {
                    if (ps.size() == 2) {
                        SqlAst.BinaryOp op = switch (f.function()) {
                            case "equal" -> SqlAst.BinaryOp.EQ;
                            case "lessThan" -> SqlAst.BinaryOp.LT;
                            case "lessThanEqual" -> SqlAst.BinaryOp.LE;
                            case "greaterThan" -> SqlAst.BinaryOp.GT;
                            default -> SqlAst.BinaryOp.GE;
                        };
                        return new SqlAst.Binary(op, value(ps.get(0), row), value(ps.get(1), row), NONE);
                    }
                }
                case "isEmpty", "isNotEmpty" -> {
                    if (ps.size() == 1) {
                        boolean some = switch (f.function()) {
                            case "isNotEmpty" -> true;
                            default -> false;
                        };
                        return new SqlAst.IsNull(value(ps.get(0), row), some, NONE);
                    }
                }
                case "in" -> {
                    if (ps.size() == 2 && ps.get(1) instanceof PureCollection c) {
                        List<SqlAst.Expr> values = new ArrayList<>();
                        for (ValueSpecification e : c.values()) {
                            values.add(literal(e));
                        }
                        return new SqlAst.InList(value(ps.get(0), row), values, false, NONE);
                    }
                }
                default -> {
                }
            }
            throw new Refused("filters by " + f.function() + ": the flat form's WHERE says comparisons, IN, AND, OR,"
                    + " NOT and IS NULL so far");
        }
        throw new Refused("filters by what is no condition");
    }

    /** A condition's operand: a column of the row ({@code $x.sector}) or a literal. */
    private static SqlAst.Expr value(ValueSpecification v, String row) {
        if (v instanceof AppliedProperty p && p.receiver() instanceof Variable x && x.name().equals(row)) {
            return new SqlAst.ColumnRef(List.of(ident(p.property())), NONE);
        }
        if (v instanceof AppliedFunction f && f.parameters().size() == 1) {
            switch (f.function()) {
                case "toOne" -> {
                    return value(f.parameters().get(0), row);
                }
                default -> {
                }
            }
        }
        return literal(v);
    }

    private static SqlAst.Expr literal(ValueSpecification v) {
        return switch (v) {
            case CString s -> new SqlAst.StringLit(s.value(), NONE);
            case CInteger i -> new SqlAst.IntegerLit(new BigInteger(i.value().toString()), NONE);
            case CFloat f -> new SqlAst.NumericLit(f.exact() != null ? f.exact() : BigDecimal.valueOf(f.value()), NONE);
            case CDecimal d -> new SqlAst.NumericLit(d.value(), NONE);
            case CBoolean b -> new SqlAst.BooleanLit(b.value(), NONE);
            case CDate d -> {
                String text = d.value().toString();
                boolean day = text.length() == 10;
                yield new SqlAst.TypedLit(new SqlAst.TypeName(day ? "date" : "timestamp", List.of(), NONE),
                        day ? text : text.replace('T', ' ').replace("+0000", ""), NONE);
            }
            case EnumValue e -> throw new Refused("filters by an enumeration's value (" + e.value() + "): the flat"
                    + " form's enumerations are written later");
            default -> throw new Refused("filters by a value the flat form does not write yet");
        };
    }

    /** A name as the flat form writes it: bare where Postgres reads it so, quoted otherwise. */
    private static SqlAst.Ident ident(String name) {
        boolean bare = !name.isEmpty() && (Character.isLetter(name.charAt(0)) || name.charAt(0) == '_')
                && name.chars().allMatch(ch -> ch < 128 && (Character.isLetterOrDigit(ch) || ch == '_'))
                && !RESERVED.contains(name.toLowerCase(java.util.Locale.ROOT));
        return new SqlAst.Ident(name, !bare, NONE);
    }

    /** Postgres's reserved words a flat source's names may meet (a column named so is written quoted). */
    private static final java.util.Set<String> RESERVED = java.util.Set.of("all", "and", "any", "as", "asc", "both",
            "case", "cast", "check", "column", "constraint", "create", "current_date", "current_time", "default",
            "desc", "distinct", "do", "else", "end", "except", "false", "fetch", "for", "foreign", "from", "grant",
            "group", "having", "in", "initially", "intersect", "into", "leading", "limit", "not", "null", "offset",
            "on", "only", "or", "order", "primary", "references", "returning", "select", "some", "table", "then",
            "to", "trailing", "true", "union", "unique", "user", "using", "when", "where", "window", "with", "by",
            "filter", "sample", "fill", "ts", "whole");
}
