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
 * back answers the same rows, and prints the same text. A calculated column is the flat source's item, the Postgres
 * expression the #SQL reader reads back to the same meaning (A6, 2026-11-03). A grid the flat form cannot say yet (a
 * window's column, a group-by, a join, a function not written yet) is refused by name -- the window then shows the
 * query as it is, never an approximation.
 */
public final class TsShow {

    private TsShow() {
    }

    /** The one statement ({@code refused} null), or why there is none ({@code statement} null). */
    public record Shown(@com.legend.base.Nullable String statement, @com.legend.base.Nullable String refused) {
    }

    /**
     * The compiler's answer to a value's type, asked where Postgres spells by it what Pure spells one way: {@code +}
     * adds numbers and joins text in Pure, {@code +} and {@code ||} in Postgres (A6). The path ({@code String},
     * {@code Integer}, ...) of the column {@code column} of the relation {@code query} answers, as the compiler types
     * it; the printer never works a type out itself (AGENTS.md invariant 1). The query is the grid's own relation
     * extended by the value asked.
     */
    @FunctionalInterface
    public interface Types {
        String of(LambdaFunction query, String column);
    }

    /** No position: what the statement writes that its island's text did not. */
    private static final SqlAst.Pos NONE = new SqlAst.Pos(0, 0);

    /** {@code query} as one {@code #TS} statement, or why not; {@code types} the compiler's, over the query's model. */
    public static Shown of(LambdaFunction query, Types types) {
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
                TsAst.Source grid = flat(let.parameters().get(1), types);
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
                f.on(), f.by(), f.latest(), f.pos())).toList();
        List<TsAst.With> own = withs.stream().map(w -> new TsAst.With(w.name(), named(w.body(), grids))).toList();
        TsAst.Query one = new TsAst.Query(own, q.items(), sources, q.sample(), q.dated(), q.fill(), q.filter(), q.alls(),
                q.spans(), q.plain());
        try {
            return new Shown(TsPrinter.print(one), null);
        } catch (SqlTranslationException e) {
            return new Shown(null, e.getMessage());
        }
    }

    /** Whether {@code f} binds a grid: {@code let name = relation}. */
    private static boolean binds(AppliedFunction f) {
        return Fn.of(f.function()) == Fn.LET && f.parameters().size() == 2;
    }

    /** The class {@code v} reads every instance of ({@code C.all()}), or null. */
    private static @com.legend.base.Nullable PackageableElementPtr everyOf(ValueSpecification v) {
        if (v instanceof AppliedFunction f && f.parameters().size() == 1
                && f.parameters().get(0) instanceof PackageableElementPtr cls) {
            return Fn.of(f.function()) == Fn.GET_ALL ? cls : null;
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
                            f.by(), f.latest(), f.pos())).toList(), n.query().sample(), n.query().dated(), n.query().fill(),
                    n.query().filter(), n.query().alls(), n.query().spans(), n.query().plain()), n.pos());
            case TsAst.Named n -> n;
        };
    }

    // ------------------------------------------------------------------ a grid's relation as a flat source

    /** A relation call, as the #TS translator writes one ({@code TsTranslatorGuardrailTest}: by its helper, so the
     *  guard reads its name -- here only {@code extend}, the type question's, bookkeeping). */
    private static AppliedFunction call(String fn, ValueSpecification... parameters) {
        return new AppliedFunction(fn, List.of(parameters));
    }

    /** Why a grid has no flat form yet, in words. */
    private static final class Refused extends RuntimeException {
        Refused(String why) {
            super(why, null, false, false);
        }
    }

    /**
     * {@code relation} as a flat source: a class's paths projected ({@code SELECT p.q AS name, ... FROM Class}), a
     * table ({@code db::Store.schema.T}), a filter of either as its WHERE -- over the projected names, the flat
     * form projecting before it filters -- and a calculated column as its item, over the paths it reads
     * ({@code quantity * price AS notional}; a table's as {@code SELECT *, CLOSE * 2 AS twice}).
     */
    private static TsAst.Source flat(ValueSpecification relation, Types types) {
        if (relation instanceof AppliedFunction f) {
            switch (Fn.of(f.function())) {
                case PROJECT -> {
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
                case TABLE_REFERENCE -> {
                    if (f.parameters().size() == 2 && f.parameters().get(0) instanceof PackageableElementPtr db
                            && f.parameters().get(1) instanceof CString table) {
                        return new TsAst.Named(db.fullPath() + "." + table.value(), NONE);
                    }
                    throw new Refused("reads a table it does not name");
                }
                case FILTER -> {
                    if (f.parameters().size() == 2 && f.parameters().get(1) instanceof LambdaFunction l
                            && l.parameters().size() == 1 && l.body().size() == 1) {
                        TsAst.Source of = flat(f.parameters().get(0), types);
                        String row = l.parameters().get(0).name();
                        SqlAst.Expr where = condition(l.body().get(0), v -> value(v, row), "filters by");
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
                case EXTEND -> {
                    return calculated(f, types);
                }
                default -> throw new Refused("is a " + f.function() + " of its rows: the flat form says a grid's paths"
                        + " and filters so far");
            }
        }
        throw new Refused("is no relation the flat form says");
    }

    /** {@code e} with each column a projected name as that item's path ({@code sym} as {@code instrument.ticker}), a
     *  calculated column's name as its expression; under {@code SELECT *} (a table's) any other name the table's own
     *  column. */
    private static SqlAst.Expr paths(SqlAst.Expr e, List<TsAst.Item> items) {
        return switch (e) {
            case SqlAst.ColumnRef c -> {
                if (c.parts().size() != 1) {
                    yield c;
                }
                boolean star = false;
                for (TsAst.Item i : items) {
                    if (i.expr() == null) {
                        star = true;
                    } else if (i.alias() != null && i.alias().text().equals(c.parts().get(0).text())) {
                        yield i.expr();
                    }
                }
                if (star) {
                    yield c;
                }
                throw new Refused("reads " + c.parts().get(0).text() + ", a column it does not project");
            }
            case SqlAst.Binary b -> new SqlAst.Binary(b.op(), paths(b.left(), items), paths(b.right(), items), NONE);
            case SqlAst.Unary u -> new SqlAst.Unary(u.op(), paths(u.operand(), items), NONE);
            case SqlAst.IsNull n -> new SqlAst.IsNull(paths(n.operand(), items), n.negated(), NONE);
            case SqlAst.InList i -> new SqlAst.InList(paths(i.operand(), items), i.values(), i.negated(), NONE);
            case SqlAst.Cast c -> new SqlAst.Cast(paths(c.operand(), items), c.type(), NONE);
            case SqlAst.Case c -> new SqlAst.Case(c.operand() == null ? null : paths(c.operand(), items),
                    c.whens().stream().map(w -> new SqlAst.When(paths(w.condition(), items), paths(w.result(),
                            items))).toList(), c.otherwise() == null ? null : paths(c.otherwise(), items), NONE);
            default -> e;
        };
    }

    // ------------------------------------------------------------------ a calculated column as its item

    /** A star item, {@code SELECT *}: a table's every column. */
    private static final TsAst.Item STAR = new TsAst.Item(null, null, NONE);

    /** Postgres's double, a Pure Float: {@code CAST(x AS double precision)}. */
    private static final SqlAst.TypeName DOUBLE = new SqlAst.TypeName("double precision", List.of(), NONE);

    /** The row a calculated column reads: its {@code extend}, the lambda's variable, and the compiler's types. */
    private record Row(AppliedFunction extend, Variable variable, Types types) {

        /** {@code value}'s type, the compiler's: the grid's own extend, its column {@code value} ({@link Types}). */
        String typeOf(ValueSpecification value) {
            String asked = "__ts_show_type";
            return types.of(new LambdaFunction(List.of(), List.of(call("extend", extend.parameters().get(0),
                    new ColSpecArray(List.of(new ColSpec(asked, new LambdaFunction(List.of(variable),
                            List.of(value)))))))), asked);
        }
    }

    /**
     * THE PROTOCOL FUNCTIONS SHOW #TS READS, each by its protocol name: a grid's relation's ({@code let},
     * {@code getAll}, {@code project}, ...), a condition's (each comparison with its Postgres operator) and a calculated
     * column's. A table, not name arms (docs/TS_V1_PROGRAM_2026_11_01.md A8: the 19 arms of 2026-11-01 folded here,
     * 2026-11-03); a function none of them is {@link #OTHER}, refused by name. One native at a time (A6), each spelled
     * as the #SQL reader reads it back to Pure's meaning.
     */
    private enum Fn {
        LET("letFunction"),
        GET_ALL("getAll"),
        PROJECT("project"),
        TABLE_REFERENCE("tableReference"),
        FILTER("filter"),
        EXTEND("extend"),
        /** An enumeration read as its values' names: its cast to text, {@code CAST(side AS text)}. */
        TO_STRING("toString"),
        AND("and", SqlAst.BinaryOp.AND),
        OR("or", SqlAst.BinaryOp.OR),
        NOT("not"),
        EQUAL("equal", SqlAst.BinaryOp.EQ),
        LESS_THAN("lessThan", SqlAst.BinaryOp.LT),
        LESS_THAN_EQUAL("lessThanEqual", SqlAst.BinaryOp.LE),
        GREATER_THAN("greaterThan", SqlAst.BinaryOp.GT),
        GREATER_THAN_EQUAL("greaterThanEqual", SqlAst.BinaryOp.GE),
        IS_EMPTY("isEmpty"),
        IS_NOT_EMPTY("isNotEmpty"),
        IN("in"),
        /** {@code +}: numbers' sum, Postgres's {@code +}; text joined, its {@code ||} -- by the compiler's type. */
        PLUS("plus"),
        /** {@code -}: a difference, or one value negated. */
        MINUS("minus"),
        TIMES("times"),
        /** {@code /}: always a Float in Pure ({@code divide(Number[1], Number[1]):Float[1]}), where Postgres divides
         *  two integers as integers: its left a double, {@code CAST(a AS DOUBLE PRECISION) / b}. */
        DIVIDE("divide"),
        /** {@code if(c, |a, |b)}: {@code CASE WHEN c THEN a ELSE b END}, an else's if a further WHEN. */
        IF("if"),
        /** {@code toOne}: the value itself, as a WHERE's operand is read. */
        TO_ONE("toOne"),
        /** None of these. */
        OTHER(null);

        private final @com.legend.base.Nullable String protocol;
        private final @com.legend.base.Nullable SqlAst.BinaryOp op;

        Fn(@com.legend.base.Nullable String protocol) {
            this(protocol, null);
        }

        Fn(@com.legend.base.Nullable String protocol, @com.legend.base.Nullable SqlAst.BinaryOp op) {
            this.protocol = protocol;
            this.op = op;
        }

        private static final Map<String, Fn> BY_NAME = byName();

        /** Each function by its protocol name, in declaration order. */
        private static Map<String, Fn> byName() {
            Map<String, Fn> out = new LinkedHashMap<>();
            for (Fn f : values()) {
                if (f.protocol != null) {
                    out.put(f.protocol, f);
                }
            }
            return java.util.Collections.unmodifiableMap(out);
        }

        static Fn of(String protocol) {
            return BY_NAME.getOrDefault(protocol, OTHER);
        }

        /** A condition's Postgres operator (AND, OR, a comparison's). */
        SqlAst.BinaryOp op() {
            return java.util.Objects.requireNonNull(op, () -> name() + " is no operator");
        }
    }

    /**
     * {@code extend} as its flat source's items: the source's own (a table's {@code *}), then each calculated column,
     * its expression over the paths it reads -- a projected name as its path, an earlier calculated column as its
     * expression, so the source projects, filters and calculates as Pure did (a calculated column changes no row).
     */
    private static TsAst.Source calculated(AppliedFunction extend, Types types) {
        List<ValueSpecification> ps = extend.parameters();
        if (ps.size() != 2) {
            throw new Refused("has a window's column (" + names(extend) + "): the flat form says a row's own values so"
                    + " far");
        }
        List<ColSpec> cols = ps.get(1) instanceof ColSpecArray a ? a.colSpecs()
                : ps.get(1) instanceof ColSpec c ? List.of(c) : List.of();
        if (cols.isEmpty()) {
            throw new Refused("extends by what is no column");
        }
        TsAst.Source of = flat(ps.get(0), types);
        TsAst.Flat base = switch (of) {
            case TsAst.Flat flat -> flat;
            case TsAst.Filtered filtered -> new TsAst.Flat(List.of(STAR), filtered.of(), filtered.where(), NONE);
            default -> new TsAst.Flat(List.of(STAR), of, null, NONE);
        };
        List<TsAst.Item> items = new ArrayList<>(base.items());
        for (ColSpec c : cols) {
            LambdaFunction l = c.function1();
            if (l == null || c.function2() != null || l.parameters().size() != 1 || l.body().size() != 1) {
                throw new Refused("has a calculated column " + c.name() + " that is no value of a row");
            }
            SqlAst.Expr e;
            try {
                e = calc(l.body().get(0), new Row(extend, l.parameters().get(0), types));
            } catch (Refused r) {
                throw new Refused("has a calculated column " + c.name() + " that " + r.getMessage());
            }
            items.add(new TsAst.Item(paths(e, items), ident(c.name()), NONE));
        }
        return new TsAst.Flat(items, base.from(), base.where(), NONE);
    }

    /** A calculated column's value as the Postgres expression the #SQL reader reads back to the same meaning. */
    private static SqlAst.Expr calc(ValueSpecification v, Row row) {
        if (v instanceof AppliedProperty p && p.receiver() instanceof Variable x
                && x.name().equals(row.variable().name())) {
            return new SqlAst.ColumnRef(List.of(ident(p.property())), NONE);
        }
        if (v instanceof CFloat) {
            // a Float, where Postgres reads 1.5 as an exact numeric (the reader's Decimal): a double, as Pure's is
            return new SqlAst.Cast(literal(v, "calculates by"), DOUBLE, NONE);
        }
        if (v instanceof PureCollection c && c.values().isEmpty()) {
            // no value ([]: an if's empty branch): NULL, its type the other branch's, as Pure's empty is
            return new SqlAst.NullLit(NONE);
        }
        if (!(v instanceof AppliedFunction f)) {
            return literal(v, "calculates by");
        }
        List<ValueSpecification> ps = f.parameters();
        // an operator run, a + b + c, is one collection (AppliedFunction.infixRun); unary minus its operand alone
        List<ValueSpecification> run = ps.size() == 1 && ps.get(0) instanceof PureCollection c ? c.values() : ps;
        return switch (Fn.of(f.function())) {
            case TO_ONE -> {
                if (ps.size() != 1) {
                    throw new Refused("calculates by a toOne of " + ps.size() + " values");
                }
                yield calc(ps.get(0), row);
            }
            case PLUS -> {
                String type = row.typeOf(f);
                SqlAst.BinaryOp plus = switch (type) {
                    case "String" -> SqlAst.BinaryOp.CONCAT;
                    case "Integer", "Float", "Decimal", "Number" -> SqlAst.BinaryOp.PLUS;
                    default -> throw new Refused("adds values of type " + type + ": the flat form adds numbers and"
                            + " joins text so far");
                };
                yield arithmetic(run, plus, row);
            }
            case MINUS -> run.size() == 1 ? new SqlAst.Unary(SqlAst.UnaryOp.MINUS, calc(run.get(0), row), NONE)
                    : arithmetic(run, SqlAst.BinaryOp.MINUS, row);
            case TIMES -> arithmetic(run, SqlAst.BinaryOp.TIMES, row);
            case DIVIDE -> {
                if (ps.size() != 2) {
                    throw new Refused("calculates by a divide of " + ps.size() + " values");
                }
                SqlAst.Expr left = calc(ps.get(0), row);
                yield new SqlAst.Binary(SqlAst.BinaryOp.DIVIDE, left instanceof SqlAst.Cast c && c.type() == DOUBLE
                        ? left : new SqlAst.Cast(left, DOUBLE, NONE), calc(ps.get(1), row), NONE);
            }
            case IF -> {
                List<SqlAst.When> whens = new ArrayList<>();
                ValueSpecification at = f;
                while (at instanceof AppliedFunction g && Fn.of(g.function()) == Fn.IF
                        && g.parameters().size() == 3) {
                    whens.add(new SqlAst.When(condition(g.parameters().get(0), x -> compared(x, row),
                            "calculates by"), calc(branch(g.parameters().get(1)), row)));
                    at = branch(g.parameters().get(2));
                }
                if (whens.isEmpty()) {
                    throw new Refused("calculates by an if of " + ps.size() + " values");
                }
                yield new SqlAst.Case(null, whens, calc(at, row), NONE);
            }
            default -> condition(v, x -> compared(x, row), "calculates by");
        };
    }

    /** A comparison's operand in a calculated column: a Float literal as written, a double and Postgres's numeric
     *  comparing alike (a WHERE's {@code close > 100.5}); any other value as it calculates. */
    private static SqlAst.Expr compared(ValueSpecification v, Row row) {
        return v instanceof CFloat ? literal(v, "calculates by") : calc(v, row);
    }

    /** {@code operands} joined by {@code op}, left to right ({@code a - b - c} as {@code (a - b) - c}). */
    private static SqlAst.Expr arithmetic(List<ValueSpecification> operands, SqlAst.BinaryOp op, Row row) {
        if (operands.size() < 2) {
            throw new Refused("calculates by an operator of " + operands.size() + " values");
        }
        SqlAst.Expr e = calc(operands.get(0), row);
        for (ValueSpecification next : operands.subList(1, operands.size())) {
            e = new SqlAst.Binary(op, e, calc(next, row), NONE);
        }
        return e;
    }

    /** An if's branch, {@code |value}: its value. */
    private static ValueSpecification branch(ValueSpecification v) {
        if (v instanceof LambdaFunction l && l.parameters().isEmpty() && l.body().size() == 1) {
            return l.body().get(0);
        }
        throw new Refused("calculates by an if whose branch is no value");
    }

    private static String names(AppliedFunction extend) {
        return extend.parameters().stream().skip(1).flatMap(p -> p instanceof ColSpec c ? java.util.stream.Stream.of(
                c.name()) : p instanceof ColSpecArray a ? a.colSpecs().stream().map(ColSpec::name)
                : java.util.stream.Stream.<String>empty()).reduce((a, b) -> a + ", " + b).orElse("?");
    }

    /** A projected column's path from its object: {@code x|$x.instrument.ticker} as {@code instrument.ticker}. An
     *  enumeration read as its values' names ({@code $x.side->toString()}, DataCube's way to read one) as its cast to
     *  text, {@code CAST(side AS text)}, which the #SQL reader reads back as the same (A6, 2026-11-03). */
    private static SqlAst.Expr path(ColSpec c) {
        LambdaFunction l = c.function1();
        if (l == null || l.parameters().size() != 1 || l.body().size() != 1) {
            throw new Refused("projects " + c.name() + " by what is no path");
        }
        ValueSpecification body = l.body().get(0);
        if (body instanceof AppliedFunction f && Fn.of(f.function()) == Fn.TO_STRING && f.parameters().size() == 1) {
            return new SqlAst.Cast(path(c.name(), f.parameters().get(0), l), new SqlAst.TypeName("text", List.of(),
                    NONE), NONE);
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

    /** A row's condition ({@code x|$x.sector == 'TECH' && ...}) as the WHERE Postgres reads to the same meaning, each
     *  operand by {@code operand} (a WHERE's a column or a literal, a calculated column's any value it calculates);
     *  {@code verb} what a refusal says it does ("filters by"). */
    private static SqlAst.Expr condition(ValueSpecification v,
            java.util.function.Function<ValueSpecification, SqlAst.Expr> operand, String verb) {
        if (v instanceof AppliedFunction f) {
            List<ValueSpecification> ps = f.parameters();
            Fn fn = Fn.of(f.function());
            switch (fn) {
                case AND, OR -> {
                    if (ps.size() == 2) {
                        return new SqlAst.Binary(fn.op(), condition(ps.get(0), operand, verb), condition(ps.get(1),
                                operand, verb), NONE);
                    }
                }
                case NOT -> {
                    if (ps.size() == 1) {
                        return new SqlAst.Unary(SqlAst.UnaryOp.NOT, condition(ps.get(0), operand, verb), NONE);
                    }
                }
                case EQUAL, LESS_THAN, LESS_THAN_EQUAL, GREATER_THAN, GREATER_THAN_EQUAL -> {
                    if (ps.size() == 2) {
                        return new SqlAst.Binary(fn.op(), operand.apply(ps.get(0)), operand.apply(ps.get(1)), NONE);
                    }
                }
                case IS_EMPTY, IS_NOT_EMPTY -> {
                    if (ps.size() == 1) {
                        return new SqlAst.IsNull(operand.apply(ps.get(0)), fn == Fn.IS_NOT_EMPTY, NONE);
                    }
                }
                case IN -> {
                    if (ps.size() == 2 && ps.get(1) instanceof PureCollection c) {
                        List<SqlAst.Expr> values = new ArrayList<>();
                        for (ValueSpecification e : c.values()) {
                            values.add(literal(e, verb));
                        }
                        return new SqlAst.InList(operand.apply(ps.get(0)), values, false, NONE);
                    }
                }
                default -> {
                }
            }
            throw new Refused(verb + " " + f.function() + ": the flat form says comparisons, IN, AND, OR, NOT and IS"
                    + " NULL so far, and a calculated column's arithmetic and if");
        }
        throw new Refused(verb + " what is no condition");
    }

    /** A condition's operand: a column of the row ({@code $x.sector}) or a literal. */
    private static SqlAst.Expr value(ValueSpecification v, String row) {
        if (v instanceof AppliedProperty p && p.receiver() instanceof Variable x && x.name().equals(row)) {
            return new SqlAst.ColumnRef(List.of(ident(p.property())), NONE);
        }
        if (v instanceof AppliedFunction f && f.parameters().size() == 1 && Fn.of(f.function()) == Fn.TO_ONE) {
            return value(f.parameters().get(0), row);
        }
        return literal(v, "filters by");
    }

    /** A literal value; {@code verb} what a refusal says it does. */
    private static SqlAst.Expr literal(ValueSpecification v, String verb) {
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
            case EnumValue e -> throw new Refused(verb + " an enumeration's value (" + e.value() + "): the flat"
                    + " form's enumerations are written later");
            default -> throw new Refused(verb + " a value the flat form does not write yet");
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
            "filter", "sample", "fill", "ts", "whole", "across");
}
