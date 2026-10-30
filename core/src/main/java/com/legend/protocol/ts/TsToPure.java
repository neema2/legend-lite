// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.ts;

import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;
import com.legend.protocol.sql.InputDialect;
import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlToPure;
import com.legend.protocol.sql.SqlTranslationException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * THE {@code #TS{...}#} QUERY AS THE PURE IT MEANS (docs/TIMESERIES_LANGUAGE_2026_10_29.md; the user's rulings of
 * 2026-10-30, docs/TS_LANGUAGE_BUILD_2026_10_30.md): the composition a person would write, built directly -- never
 * through SQL text -- as the protocol layer's translators build theirs ({@link SqlToPure}, CypherToPure). Its sources
 * are read as R1 says (a class's objects, their dotted paths projected; a flat subquery or a WITH source; a table; a
 * relation of the enclosing Pure); each column a line of its own ({@link TsLines}, R2), lines meeting on the dates
 * every one has (R6); every figure along time a call of the {@code timeseries} library over its line's own rows
 * (ruling 2, R4), its window the line's periods in order; every per-point expression and condition what Postgres
 * means, through the {@code #SQL{...}#} translator ({@link SqlToPure#rowLambda}); the answer the date, the instrument
 * and the items (R7), filtered by {@code FILTER} (R8), in date order. What it asks of the model -- a relation's
 * columns, a class's properties, an enumeration's values -- the compiler's island seam answers ({@link Model}). A
 * refusal names its place in the island and its fix.
 */
public final class TsToPure {

    private TsToPure() {
    }

    /** The package of the timeseries library (the project {@code timeseries}, projects/timeseries/model.pure), which
     *  {@code #TS} imports: a function a query names is the library's (ruling 4). */
    public static final String LIBRARY = "timeseries::";

    /** v0's statistics of a window (R4): the language's name, the library's function. */
    private static final Map<String, String> WINDOWED = Map.of("avg", "mean", "mean", "mean", "sum", "sum",
            "min", "min", "max", "max", "std", "std", "stddev", "std", "var", "var", "variance", "var");

    /** v0's functions of a value some observations back ({@code obs = 1}): the language's name, the library's. */
    private static final Map<String, String> BACK = Map.of("lag", "lag", "diff", "diff", "returns", "returns");

    /** The functions of the language's catalog (§6) that v1 brings, refused by name until then (P4). */
    private static final Set<String> LATER = Set.of("alpha", "annualize", "append", "atr", "basket", "beta",
            "bollinger_bands", "change", "compare", "consecutive", "correlation", "cov", "dickey_fuller",
            "donchian_channels", "down_capture", "drawdown", "excess_returns", "exponential_moving_average",
            "exponential_spread_volatility", "exponential_std", "exponential_volatility", "filter_values", "first",
            "first_of", "geometrically_aggregate", "half_life", "hedge_ratio", "if", "index", "information_ratio",
            "interpolate", "keltner_channels", "last", "last_of", "lead_lag", "macd", "max_drawdown", "obv",
            "percent_change", "percentile", "percentiles", "prepend", "prices", "product", "r_squared", "range_of",
            "rank", "rebase", "residual", "rsi", "seasonally_adjusted", "sharpe_ratio", "smooth_spikes",
            "smoothed_moving_average", "stochastic", "total_return", "tracking_error", "trend", "union_of",
            "up_capture", "value_at", "vol_swap_volatility", "volatility", "vwap", "weighted_sum", "winsorize",
            "zscores");

    /** What a query asks of the model beyond a relation's columns ({@link SqlToPure.Sources}): its classes by a name
     *  as a query writes it (R13), a class's properties, an enumeration's values. */
    public interface Model extends SqlToPure.Sources {

        /** The paths of the classes {@code name} names: itself where it is one's path, else those of that simple
         *  name. */
        List<String> classesNamed(String name);

        /** Reports the property {@code name} of the class {@code cls} (its superclasses' too, an association's end)
         *  to {@code sink}; nothing where it has none. */
        void property(String cls, String name, PropertySink sink);

        /** The names of the properties of the class {@code cls} and its superclasses. */
        List<String> properties(String cls);

        /** The values of the enumeration of path {@code type}, or null where it names none. */
        @com.legend.base.Nullable List<String> enumeration(String type);
    }

    /** A class's property, as {@link Model#property} reports it: its name, its type's name, the class it holds
     *  (none: null), whether it holds one value at most, whether it takes parameters (a qualified property). */
    @FunctionalInterface
    public interface PropertySink {
        void property(String name, String type, @com.legend.base.Nullable String cls, boolean one, boolean parameters);
    }

    /** A property {@link Model#property} reported. */
    record Property(String name, String type, @com.legend.base.Nullable String cls, boolean one, boolean parameters) {
    }

    /** The property {@code name} of {@code cls} in {@code model}, or null. */
    static @com.legend.base.Nullable Property property(Model model, String cls, String name) {
        Property[] found = new Property[1];
        model.property(cls, name, (n, type, holds, one, parameters) -> found[0] = new Property(n, type, holds, one,
                parameters));
        return found[0];
    }

    /** The relation {@code text}, a {@code #TS{...}#} query, means over {@code model}, its per-point expressions in
     *  {@code dialect} (Postgres's); a {@link SqlTranslationException}, placed in the text, where it means none. */
    public static ValueSpecification translate(String text, Model model, InputDialect dialect) {
        return new Builder(model, dialect, TsParser.parse(text)).query();
    }

    /** A source as R1 reads it: a class's objects (its paths still to be projected), or a relation. */
    private sealed interface Read permits Instances, Rows {
    }

    /** A class read as objects (R1.1), the condition its rows meet (a {@code (Class WHERE ...)}; none: null). */
    private record Instances(String cls, SqlAst.@com.legend.base.Nullable Expr where) implements Read {
    }

    /** A relation, and what the query calls it (a refusal's words). */
    private record Rows(ValueSpecification relation, String what) implements Read {
    }

    /** A path a class's query reads: the column it is read as, and its properties' names in order. */
    private record Path(String column, List<String> steps) {
    }

    /** One query's Pure, built clause by clause. */
    private static final class Builder {

        private final Model model;
        private final SqlToPure.Sources sources;
        private final InputDialect dialect;
        private final TsAst.Query query;
        private final Map<String, ValueSpecification> withs = new LinkedHashMap<>();

        Builder(Model model, InputDialect dialect, TsAst.Query query) {
            this.model = model;
            this.sources = model;
            this.dialect = dialect;
            this.query = query;
        }

        ValueSpecification query() {
            for (TsAst.With w : query.withs()) {
                if (withs.containsKey(w.name().text())) {
                    throw invalid("the WITH names " + w.name().text() + " twice", w.name().pos());
                }
                withs.put(w.name().text(), relation(read(w.body()), w.name()));
            }
            List<TsAst.Item> items = query.items();
            List<String> names = new ArrayList<>();
            List<SqlAst.Ident> aliases = new ArrayList<>();
            List<SqlAst.Expr> exprs = new ArrayList<>();
            for (TsAst.Item it : items) {
                SqlAst.Ident named = it.alias();
                SqlAst.Expr e = it.expr();
                if (named == null || e == null) {
                    throw invalid("name the item (R7): an item is expression AS name, avg(close, 20) AS ma20",
                            it.pos());
                }
                if (names.contains(named.text())) {
                    throw invalid("two items are named " + named.text() + " (R7)", named.pos());
                }
                names.add(named.text());
                aliases.add(named);
                exprs.add(perPoint(e));
            }
            // THE SOURCES (R1), each by its alias: what it reads, its date and its instrument
            List<Opening> opening = new ArrayList<>();
            for (TsAst.From from : query.sources()) {
                SqlAst.Ident as = from.alias();
                String alias = as != null ? as.text() : from.source() instanceof TsAst.Named n ? n.name() : "";
                if (opening.stream().anyMatch(o -> o.alias().equals(alias))) {
                    throw invalid("two sources are called " + alias + ": give each its alias, FROM prices px ON"
                            + " date, rates rf ON date", from.pos());
                }
                Read read = read(from.source());
                opening.add(new Opening("__ts_s" + opening.size(), alias, read,
                        read instanceof Instances o ? new Paths(o.cls()) : null,
                        read instanceof Rows r ? typed(r.relation()) : Map.of(), from));
            }
            Resolver resolver = new Resolver(opening, names);
            List<Resolved> resolved = new ArrayList<>();
            for (Opening o : opening) {
                String on = resolver.single(o, o.from().on(), "ON");
                SqlAst.ColumnRef by = o.from().by();
                String instrument = by == null ? null : resolver.single(o, by, "BY");
                SqlAst.Expr where = o.read() instanceof Instances i && i.where() != null
                        ? resolver.within(o, perPoint(i.where())) : null;
                resolved.add(new Resolved(o, on, instrument, where));
            }
            for (int i = 0; i < exprs.size(); i++) {
                exprs.set(i, resolver.expression(exprs.get(i), i));
            }
            // each source's relation: a class's paths projected (R1.1), its condition on the projection
            Map<String, TsLines.Source> opened = new LinkedHashMap<>();
            List<String> own = new ArrayList<>();
            for (Resolved r : resolved) {
                Opening o = r.opening();
                ValueSpecification rel;
                Map<String, String> types;
                if (o.read() instanceof Instances i) {
                    Paths paths = java.util.Objects.requireNonNull(o.paths());
                    rel = paths.projection();
                    SqlAst.Expr where = r.where();
                    if (where != null) {
                        rel = filter(rel, where, "WHERE");
                    }
                    types = typed(rel);
                    own.addAll(properties(i.cls()));
                } else {
                    rel = ((Rows) o.read()).relation();
                    types = o.types();
                }
                own.addAll(types.keySet());
                opened.put(o.key(), new TsLines.Source(rel, r.date(), java.util.Objects.requireNonNull(
                        types.get(r.date())), r.instrument(), o.alias().isEmpty() ? "the source" : o.alias()));
            }
            String date = resolved.get(0).date();
            for (SqlAst.Ident a : aliases) {
                String name = a.text();
                if (own.stream().anyMatch(c -> c.equalsIgnoreCase(name)) || name.equalsIgnoreCase(date)) {
                    throw invalid(name + " is a column of the source (R7): name the item another name", a.pos());
                }
            }
            // THE ITEMS' LINES (R2, R4, R6), each in turn, a later one reading an earlier one's
            Lines lines = new Lines(opened);
            List<TsLines.Valued> answered = new ArrayList<>();
            for (int i = 0; i < exprs.size(); i++) {
                TsLines.Node n = lines.node(exprs.get(i));
                if (!(n instanceof TsLines.Valued v)) {
                    throw invalid(names.get(i) + " reads no line: an item is a line of a source's column, or a"
                            + " figure of one (R2)", aliases.get(i).pos());
                }
                TsLines.Valued item = new TsLines.Valued(v.line().with(call("rename", v.line().rel(),
                        new ColSpec(v.value()), new ColSpec(names.get(i)))), names.get(i));
                lines.items.put(names.get(i), item);
                answered.add(item);
            }
            TsLines.Line all = TsLines.answer(lines, answered, aliases.get(0).pos());
            String instrument = all.byInstrument() ? all.part() : null;
            ValueSpecification rel = call("rename", all.rel(), new ColSpec(all.at()), new ColSpec(date));
            List<String> answer = new ArrayList<>(List.of(date));
            if (instrument != null) {
                answer.add(instrument);
            }
            answer.addAll(names);
            rel = call("select", rel, new ColSpecArray(answer.stream().map(ColSpec::new).toList()));
            if (query.filter() != null) {
                SqlAst.Expr condition = perPoint(query.filter());
                visit(condition, x -> {
                    if (x instanceof SqlAst.ColumnRef c && (c.parts().size() > 1 || resolve(answer, c) == null)) {
                        throw invalid(text(c) + " isn't a column of the answer (" + String.join(", ", answer)
                                + "): FILTER reads the answer's columns (R8); to use only some rows of the data, put"
                                + " the WHERE with its source: FROM (source WHERE ...)", c.pos());
                    }
                    if (x instanceof SqlAst.FunctionCall fc && TsParser.PICK.equals(fn(fc))) {
                        throw invalid("FILTER reads the answer's columns (R8): a subscript picks a source's line,"
                                + " an item's to name", fc.pos());
                    }
                });
                rel = call("filter", rel, rowLambda(condition, rel, sources, true, "FILTER"));
            }
            List<ValueSpecification> order = new ArrayList<>(List.of(call("ascending", new ColSpec(date))));
            if (instrument != null) {
                order.add(call("ascending", new ColSpec(instrument)));
            }
            return call("sort", rel, new PureCollection(order));
        }

        /** The types of {@code relation}'s columns, by name, in order. */
        private Map<String, String> typed(ValueSpecification relation) {
            Map<String, String> out = new LinkedHashMap<>();
            sources.columns(relation, (name, type, nullable, storeType) -> out.put(name, type));
            return out;
        }

        /** A source as it is opened: its key (the name its columns are read by, internally), its alias, what it reads,
         *  its paths (a class's), its columns' types (a relation's), its FROM. */
        private record Opening(String key, String alias, Read read, @com.legend.base.Nullable Paths paths,
                Map<String, String> types, TsAst.From from) {
        }

        /** A source opened and its clauses resolved: its date column, its instrument column (none: null), its
         *  condition (a class's {@code WHERE}; none: null). */
        private record Resolved(Opening opening, String date, @com.legend.base.Nullable String instrument,
                SqlAst.@com.legend.base.Nullable Expr where) {
        }

        /**
         * EVERY COLUMN A QUERY NAMES, RESOLVED TO ITS SOURCE (R1, R2): {@code px.close}, or {@code close} where one
         * source has it; a class's dotted path ({@code instrument.ticker}) registered for its projection and read by
         * its last step's name. An item's expression reads each as {@code <source key>.<column>}; a subscript's
         * condition reads its source's columns by name; an earlier item by its name.
         */
        private final class Resolver {
            private final List<Opening> opening;
            private final List<String> names;

            Resolver(List<Opening> opening, List<String> names) {
                this.opening = opening;
                this.names = names;
            }

            /** {@code c}, a column of {@code o} ({@code ON}'s, {@code BY}'s): its name in {@code o}'s relation. */
            String single(Opening o, SqlAst.ColumnRef c, String clause) {
                SqlAst.ColumnRef own = c.parts().size() > 1 && c.parts().get(0).text().equals(o.alias())
                        ? new SqlAst.ColumnRef(c.parts().subList(1, c.parts().size()), c.pos()) : c;
                return column(o, own, clause);
            }

            /** {@code e}, a condition over {@code o}'s columns (a source's WHERE, a subscript's), each read by name. */
            SqlAst.Expr within(Opening o, SqlAst.Expr e) {
                return map(e, x -> x instanceof SqlAst.ColumnRef c && !query.alls().contains(c.pos())
                        ? TsToPure.column(single(o, c, "WHERE"), c.pos()) : x);
            }

            /** Item {@code i}'s expression, each column resolved to its source; an earlier item kept by its name. */
            SqlAst.Expr expression(SqlAst.Expr e, int i) {
                if (e instanceof SqlAst.FunctionCall fc && TsParser.PICK.equals(fn(fc))) {
                    return pick(fc, i);
                }
                if (e instanceof SqlAst.ColumnRef c) {
                    return ref(c, i);
                }
                return children(e, x -> expression(x, i));
            }

            /** {@code line[...]}: its line a source's column, its matches that source's conditions. */
            private SqlAst.Expr pick(SqlAst.FunctionCall fc, int i) {
                if (fc.args().isEmpty() || !(fc.args().get(0) instanceof SqlAst.ColumnRef line)) {
                    throw invalid("a subscript picks a source's column's line: close['SPX']", fc.pos());
                }
                SqlAst.Expr base = ref(line, i);
                if (!(base instanceof SqlAst.ColumnRef r && r.parts().size() == 2
                        && r.parts().get(0).text().startsWith("__ts_s"))) {
                    throw invalid("a subscript picks a source's column's line, not an item's: close['SPX']; FILTER"
                            + " the answer to show some", fc.pos());
                }
                Opening o = opening.stream().filter(x -> x.key().equals(r.parts().get(0).text())).findFirst()
                        .orElseThrow();
                List<SqlAst.Expr> args = new ArrayList<>(List.of(base));
                for (SqlAst.Expr m : fc.args().subList(1, fc.args().size())) {
                    args.add(m instanceof SqlAst.StringLit || m instanceof SqlAst.IntegerLit
                            || m instanceof SqlAst.NumericLit || m instanceof SqlAst.TypedLit ? m : within(o, m));
                }
                return new SqlAst.FunctionCall(fc.name(), args, false, false, fc.pos());
            }

            /** A column reference of item {@code i}: an earlier item's name, {@code all}, or a source's column. */
            private SqlAst.Expr ref(SqlAst.ColumnRef c, int i) {
                if (query.alls().contains(c.pos())) {
                    return c;
                }
                if (c.parts().size() == 1 && names.subList(0, i).contains(text(c))) {
                    return c;
                }
                if (c.parts().size() == 1 && names.contains(text(c))) {
                    throw invalid(text(c) + " is a later item: an item reads the items before it (R7)", c.pos());
                }
                Opening o;
                SqlAst.ColumnRef own = c;
                List<Opening> named = opening.stream().filter(x -> c.parts().size() > 1
                        && !x.alias().isEmpty() && c.parts().get(0).text().equals(x.alias())).toList();
                if (!named.isEmpty()) {
                    o = named.get(0);
                    own = new SqlAst.ColumnRef(c.parts().subList(1, c.parts().size()), c.pos());
                } else if (opening.size() == 1) {
                    o = opening.get(0);
                } else {
                    List<Opening> having = opening.stream().filter(x -> has(x, c)).toList();
                    if (having.size() != 1) {
                        throw invalid(text(c) + (having.isEmpty() ? " is a column of no source"
                                : " is a column of " + String.join(" and ", having.stream().map(Opening::alias)
                                        .toList())) + ": name its source, px." + text(c) + " (R2)", c.pos());
                    }
                    o = having.get(0);
                }
                return new SqlAst.ColumnRef(List.of(new SqlAst.Ident(o.key(), true, c.pos()),
                        new SqlAst.Ident(column(o, own, "SELECT"), true, c.pos())), c.pos());
            }

            /** Whether {@code c} names a column of {@code o}. */
            private boolean has(Opening o, SqlAst.ColumnRef c) {
                if (o.read() instanceof Instances i) {
                    return property(model, i.cls(), c.parts().get(0).text()) != null;
                }
                return c.parts().size() == 1 && resolve(new ArrayList<>(o.types().keySet()), c) != null;
            }

            /** The column {@code c} names in {@code o}: a relation's by Postgres's rule; a class's path, registered. */
            private String column(Opening o, SqlAst.ColumnRef c, String clause) {
                Paths paths = o.paths();
                if (paths != null) {
                    return paths.add(c);
                }
                if (c.parts().size() != 1) {
                    throw invalid(clause + " names a column of " + (o.alias().isEmpty() ? "the source" : o.alias())
                            + ", not a path: a path is a class's", c.pos());
                }
                List<String> columns = new ArrayList<>(o.types().keySet());
                String found = resolve(columns, c);
                if (found == null) {
                    throw invalid(text(c) + " is not a column of " + (o.alias().isEmpty() ? "the source"
                            : o.alias()) + ": its columns are " + String.join(", ", columns), c.pos());
                }
                return found;
            }
        }

        // -------------------------------------------------------------- sources

        /** What {@code s} reads. */
        private Read read(TsAst.Source s) {
            return switch (s) {
                case TsAst.Named n -> named(n);
                case TsAst.Variable v -> new Rows(new Variable(v.name()), "$" + v.name());
                case TsAst.Filtered f -> {
                    Read of = read(f.of());
                    if (of instanceof Instances o) {
                        yield new Instances(o.cls(), f.where());
                    }
                    Rows r = (Rows) of;
                    yield new Rows(filter(r.relation(), perPoint(f.where()), "WHERE"), r.what());
                }
                case TsAst.Flat flat -> new Rows(flat(flat), "the subquery");
            };
        }

        /** A source by its name (R13): a WITH source, a class or a table, by its simple name where the model has one
         *  of that name, else by its path ({@code pkg::Class}, {@code pkg::Db.schema.table}). */
        private Read named(TsAst.Named n) {
            String name = n.name();
            if (withs.containsKey(name)) {
                return new Rows(java.util.Objects.requireNonNull(withs.get(name)), name);
            }
            List<SqlToPure.CatalogTable> catalog = SqlToPure.tables(model);
            int dot = name.indexOf('.');
            if (dot > 0) {
                // a table by its database's path: pkg::Db.schema.table, pkg::Db.table
                String db = name.substring(0, dot);
                String rest = name.substring(dot + 1);
                if (catalog.stream().noneMatch(t -> t.database().equals(db) && (rest.equals(t.schema() + "."
                        + t.table()) || "default".equals(t.schema()) && rest.equals(t.table())))) {
                    throw invalid(name + " is not a table of the model", n.pos());
                }
                return new Rows(AppliedFunction.tableReference(db, rest, null), rest);
            }
            List<String> classes = model.classesNamed(name);
            List<SqlToPure.CatalogTable> tables = catalog.stream().filter(t -> t.table().equals(name)).toList();
            if (classes.size() + tables.size() > 1) {
                List<String> all = new ArrayList<>(classes);
                tables.forEach(t -> all.add(t.database() + "." + ("default".equals(t.schema()) ? "" : t.schema() + ".")
                        + t.table()));
                throw invalid(name + " is " + String.join(" and ", all) + ": write one", n.pos());
            }
            if (classes.size() == 1) {
                return new Instances(classes.get(0), null);
            }
            if (tables.size() == 1) {
                SqlToPure.CatalogTable t = tables.get(0);
                return new Rows(AppliedFunction.tableReference(t.database(), "default".equals(t.schema()) ? t.table()
                        : t.schema() + "." + t.table(), null), name);
            }
            throw invalid(name + " is not a class or a table of the model, nor a WITH source", n.pos());
        }

        /** A source read as a relation: a class's objects, every path its condition reads projected (a WITH source
         *  over a class is a flat subquery, R1.2: its paths named once). */
        private ValueSpecification relation(Read read, SqlAst.Ident name) {
            if (read instanceof Rows r) {
                return r.relation();
            }
            throw invalid("a WITH source over a class names its paths: WITH " + name.text() + " AS (SELECT"
                    + " instrument.ticker AS ticker, quoteTime, closePrice AS close FROM "
                    + ((Instances) read).cls() + ")", name.pos());
        }

        /** A flat source (R1.2, R1.3): {@code (SELECT items FROM source [WHERE condition])}. Over a class, its paths
         *  projected ({@code Class.all()->project(~[ticker: x|$x.instrument.ticker, ...])}, what a saved grid is),
         *  the condition on the projection; over a relation, its rows filtered, its items extended. The items then
         *  selected, in order. */
        private ValueSpecification flat(TsAst.Flat flat) {
            Read of = read(flat.from());
            List<String> outputs = new ArrayList<>();
            ValueSpecification out;
            List<String> held;
            if (of instanceof Instances o) {
                Paths paths = new Paths(o.cls());
                Map<String, SqlAst.Expr> computed = new LinkedHashMap<>();
                for (TsAst.Item it : flat.items()) {
                    SqlAst.Expr item = it.expr();
                    SqlAst.Ident as = it.alias();
                    if (item == null) {
                        throw invalid("a flat source over a class names its paths: (SELECT instrument.ticker AS"
                                + " ticker, closePrice AS close FROM " + o.cls() + ")", it.pos());
                    }
                    SqlAst.Expr e = perPoint(item);
                    if (e instanceof SqlAst.ColumnRef c && !paths.has(c)) {
                        String name = as != null ? as.text() : c.parts().get(c.parts().size() - 1).text();
                        paths.add(c, name);
                        outputs.add(name);
                    } else if (as == null) {
                        throw invalid("name the item: ... AS name", it.pos());
                    } else {
                        computed.put(as.text(), e);
                        outputs.add(as.text());
                    }
                }
                if (flat.where() != null) {
                    collect(perPoint(flat.where()), null, paths, Set.of(), Set.of());
                }
                for (SqlAst.Expr e : computed.values()) {
                    collect(e, null, paths, Set.of(), Set.of());
                }
                out = paths.projection();
                held = new ArrayList<>(paths.columns());
                UnaryOperator<SqlAst.Expr> toColumns = e -> map(e, x -> x instanceof SqlAst.ColumnRef c
                        ? column(paths.columnOf(c), c.pos()) : x);
                if (flat.where() != null) {
                    out = filter(out, toColumns.apply(perPoint(flat.where())), "WHERE");
                }
                for (Map.Entry<String, SqlAst.Expr> c : computed.entrySet()) {
                    out = call("extend", out, new ColSpec(c.getKey(), rowLambda(toColumns.apply(
                            c.getValue()), out, sources, false, "SELECT")));
                    held.add(c.getKey());
                }
            } else {
                Rows r = (Rows) of;
                out = r.relation();
                if (flat.where() != null) {
                    out = filter(out, perPoint(flat.where()), "WHERE");
                }
                held = new ArrayList<>(columnsOf(out));
                List<String> source = List.copyOf(held);
                for (TsAst.Item it : flat.items()) {
                    SqlAst.Expr item = it.expr();
                    SqlAst.Ident as = it.alias();
                    if (item == null) {
                        outputs.addAll(source);
                        continue;
                    }
                    SqlAst.Expr e = perPoint(item);
                    String bare = e instanceof SqlAst.ColumnRef c && c.parts().size() == 1
                            ? resolve(source, c) : null;
                    if (bare != null && (as == null || as.text().equals(bare))) {
                        outputs.add(bare);
                        continue;
                    }
                    if (as == null) {
                        throw invalid("name the item: ... AS name", it.pos());
                    }
                    out = call("extend", out, new ColSpec(as.text(), rowLambda(e, out, sources, false,
                            "SELECT")));
                    held.add(as.text());
                    outputs.add(as.text());
                }
            }
            if (outputs.size() != new java.util.HashSet<>(outputs).size()) {
                throw invalid("a flat source names a column twice", flat.pos());
            }
            return outputs.equals(held) ? out : call("select", out, new ColSpecArray(outputs.stream()
                    .map(ColSpec::new).toList()));
        }

        /**
         * {@code e}, a Postgres expression over the columns of {@code relation}, as the lambda over its row the
         * translator writes ({@link SqlToPure#rowLambda}) -- and a string it compares with an enumeration's column
         * ({@code side = 'BUY'}, {@code side IN ('BUY', 'SELL')}) written as that enumeration's value
         * ({@code trading::Side.BUY}), the mapping's to turn into its stored code (R13: the language says the value's
         * name; a string the enumeration has not refused, naming its values).
         */
        private LambdaFunction rowLambda(SqlAst.Expr e, ValueSpecification relation, SqlToPure.Sources sources,
                boolean condition, String clause) {
            LambdaFunction l = SqlToPure.rowLambda(e, relation, sources, dialect, condition, clause);
            Map<String, List<String>> enums = new LinkedHashMap<>();
            Map<String, String> paths = new LinkedHashMap<>();
            sources.columns(relation, (name, type, nullable, storeType) -> {
                List<String> values = model.enumeration(type);
                if (values != null) {
                    enums.put(name, values);
                    paths.put(name, type);
                }
            });
            return enums.isEmpty() ? l : (LambdaFunction) valued(l, enums, paths, e.pos());
        }

        /** {@code v} with each string beside a read of a column of {@code enums} in a call (a comparison's, an
         *  {@code in}'s list) as that enumeration's value. */
        private ValueSpecification valued(ValueSpecification v, Map<String, List<String>> enums,
                Map<String, String> paths, SqlAst.Pos pos) {
            ValueSpecification out = v.mapChildren(c -> valued(c, enums, paths, pos));
            if (out instanceof AppliedFunction af && af.parameters().size() == 2) {
                List<ValueSpecification> ps = new ArrayList<>(af.parameters());
                for (int side = 0; side < 2; side++) {
                    String column = enumerationRead(ps.get(side), enums);
                    if (column != null) {
                        ps.set(1 - side, values(ps.get(1 - side), java.util.Objects.requireNonNull(paths.get(column)),
                                java.util.Objects.requireNonNull(enums.get(column)), pos));
                        return af.withParameters(ps);
                    }
                }
            }
            return out;
        }

        /** The column of {@code enums} {@code v} reads ({@code $r.side}, or it taken as one value), or null. */
        private static @com.legend.base.Nullable String enumerationRead(ValueSpecification v,
                Map<String, List<String>> enums) {
            if (v instanceof AppliedProperty ap && ap.receiver() instanceof Variable) {
                return enums.containsKey(ap.property()) ? ap.property() : null;
            }
            if (v instanceof AppliedFunction af && af.parameters().size() == 1) {
                return enumerationRead(af.parameters().get(0), enums);
            }
            return null;
        }

        /** {@code v}, a string or a list of them, as values of the enumeration {@code fqn}. */
        private static ValueSpecification values(ValueSpecification v, String fqn, List<String> values,
                SqlAst.Pos pos) {
            if (v instanceof com.legend.protocol.spec.CString s) {
                if (!values.contains(s.value())) {
                    throw invalid("'" + s.value() + "' is not a value of " + fqn + ": " + String.join(", ", values)
                            + " (R13)", pos);
                }
                return new EnumValue(fqn, s.value());
            }
            if (v instanceof PureCollection pc) {
                return pc.mapChildren(c -> values(c, fqn, values, pos));
            }
            return v;
        }

        /** {@code relation} filtered by {@code condition}, a Postgres condition over its columns. */
        private ValueSpecification filter(ValueSpecification relation, SqlAst.Expr condition, String clause) {
            visit(condition, x -> {
                if (x instanceof SqlAst.FunctionCall fc && TsParser.PICK.equals(fn(fc))) {
                    throw invalid("a " + clause + " filters a source's rows: a subscript picks a line, an item's",
                            fc.pos());
                }
            });
            return call("filter", relation, rowLambda(condition, relation, sources, true, clause));
        }

        // -------------------------------------------------------------- items

        /**
         * THE ITEMS' LINES (R2, R4, R6; {@link TsLines}): each expression's node -- a source's column its line (a
         * subscript's, one instrument's), an earlier item its own, a figure along time the library's call over its
         * line's rows, any other expression of lines the lines met on their dates and the expression Postgres's over
         * them, an expression of no line a constant.
         */
        private final class Lines implements TsLines.Writer {
            private final Map<String, TsLines.Source> opened;
            final Map<String, TsLines.Valued> items = new LinkedHashMap<>();
            private int fresh;

            Lines(Map<String, TsLines.Source> opened) {
                this.opened = opened;
            }

            @Override
            public LambdaFunction rowLambda(SqlAst.Expr e, ValueSpecification relation, boolean condition,
                    String clause) {
                return Builder.this.rowLambda(e, relation, sources, condition, clause);
            }

            @Override
            public String fresh(String what) {
                return "__ts_" + what + "_" + fresh++;
            }

            /** The node {@code e}, an item's expression with its columns resolved ({@link Resolver}), is. */
            TsLines.Node node(SqlAst.Expr e) {
                if (e instanceof SqlAst.ColumnRef c) {
                    TsLines.Valued item = c.parts().size() == 1 ? items.get(text(c)) : null;
                    if (item != null) {
                        return item;
                    }
                    TsLines.Source s = c.parts().size() == 2 ? opened.get(c.parts().get(0).text()) : null;
                    if (s != null) {
                        return TsLines.column(this, s, c.parts().get(1).text(), List.of(), s.instrument() != null);
                    }
                    if (query.alls().contains(c.pos())) {
                        throw invalid("all is a window: sum(x, all)", c.pos());
                    }
                }
                if (e instanceof SqlAst.FunctionCall fc && TsParser.PICK.equals(fn(fc))) {
                    return picked(fc);
                }
                if (e instanceof SqlAst.FunctionCall fc && alongTime(fc)) {
                    return along(fc);
                }
                return computed(e);
            }

            /** A subscript's line (R2): the column's rows of one instrument ({@code close['SPX']}), or of those that
             *  meet its conditions ({@code close['SPX', venue = 'NYSE']}, {@code close[sector = 'Energy']}). */
            private TsLines.Valued picked(SqlAst.FunctionCall fc) {
                SqlAst.ColumnRef base = (SqlAst.ColumnRef) fc.args().get(0);
                TsLines.Source s = java.util.Objects.requireNonNull(opened.get(base.parts().get(0).text()));
                String instrument = s.instrument();
                List<SqlAst.Expr> matches = new ArrayList<>();
                boolean one = false;
                for (SqlAst.Expr m : fc.args().subList(1, fc.args().size())) {
                    boolean value = m instanceof SqlAst.StringLit || m instanceof SqlAst.IntegerLit
                            || m instanceof SqlAst.NumericLit || m instanceof SqlAst.TypedLit;
                    if (value) {
                        if (instrument == null) {
                            throw invalid(s.what() + " has no BY: it has no instrument to pick, FROM prices ON date"
                                    + " BY symbol (R2)", m.pos());
                        }
                        if (one) {
                            throw invalid("a subscript names one instrument: close['SPX'] (R2)", m.pos());
                        }
                        matches.add(new SqlAst.Binary(SqlAst.BinaryOp.EQ, TsToPure.column(instrument, m.pos()), m,
                                m.pos()));
                        one = true;
                    } else {
                        matches.add(m);
                        one |= instrument != null && m instanceof SqlAst.Binary b && b.op() == SqlAst.BinaryOp.EQ
                                && (names(b.left(), instrument) || names(b.right(), instrument));
                    }
                }
                return TsLines.column(this, s, base.parts().get(1).text(), matches, instrument != null && !one);
            }

            /** {@code e}, an expression of lines: its lines met on their dates (R6), {@code e} Postgres's over them;
             *  of no line, a constant. */
            private TsLines.Node computed(SqlAst.Expr e) {
                List<TsLines.Valued> parts = new ArrayList<>();
                SqlAst.Expr over = leaves(e, parts);
                if (parts.isEmpty()) {
                    return new TsLines.Constant(e);
                }
                TsLines.Line met = parts.size() == 1 ? parts.get(0).line() : TsLines.joined(this, parts, e.pos());
                String value = fresh("pt");
                return new TsLines.Valued(met.with(call("extend", met.rel(), new ColSpec(value, rowLambda(over,
                        met.rel(), false, "SELECT")))), value);
            }

            /** {@code e} with each line in it read as its column, its lines added to {@code parts} (each once). */
            private SqlAst.Expr leaves(SqlAst.Expr e, List<TsLines.Valued> parts) {
                boolean line = e instanceof SqlAst.ColumnRef c && !query.alls().contains(c.pos())
                        || e instanceof SqlAst.FunctionCall fc && (TsParser.PICK.equals(fn(fc)) || alongTime(fc));
                if (!line) {
                    return children(e, x -> leaves(x, parts));
                }
                TsLines.Node n = node(e);
                if (n instanceof TsLines.Constant k) {
                    return k.expr();
                }
                TsLines.Valued v = (TsLines.Valued) n;
                if (parts.stream().noneMatch(p -> p == v)) {
                    parts.add(v);
                }
                return TsToPure.column(v.value(), e.pos());
            }

            /** Whether {@code fc} is a function along time v0 writes, or one of v1's (refused by name). */
            private boolean alongTime(SqlAst.FunctionCall fc) {
                String name = fn(fc);
                if (name == null) {
                    return false;
                }
                if (LATER.contains(name)) {
                    throw unsupported(name + " is v1's (§6, P4): v0's functions along time are avg, sum, min, max,"
                            + " count, std, var, lag, diff and returns", fc.pos());
                }
                return WINDOWED.containsKey(name) || BACK.containsKey(name) || "count".equals(name);
            }

            /** A figure along time (R4): the library's call over its line's rows, its window the line's periods. */
            private TsLines.Valued along(SqlAst.FunctionCall fc) {
                String name = java.util.Objects.requireNonNull(fn(fc));
                if (fc.distinct() || fc.star() || !fc.orderBy().isEmpty() || !fc.withinGroup().isEmpty()
                        || fc.args().isEmpty()) {
                    throw invalid(name + " along time takes a line and a window: " + name + "(x, 20)", fc.pos());
                }
                if (!(node(fc.args().get(0)) instanceof TsLines.Valued v)) {
                    throw invalid(name + " reads a line: its first argument is a number", fc.args().get(0).pos());
                }
                TsLines.Line l = v.line();
                String out = fresh(name);
                List<SqlAst.Expr> args = fc.args();
                ValueSpecification rel;
                if (WINDOWED.containsKey(name) || "count".equals(name)) {
                    if (args.size() == 1) {
                        throw invalid(name + "(x) has no window: along time write " + name + "(x, 20) or " + name
                                + "(x, all); a period's " + name + " is SAMPLE BY's (R5, v1)", fc.pos());
                    }
                    if (args.size() != 2) {
                        throw invalid(name + " takes a line and a window: " + name + "(x, 20)", fc.pos());
                    }
                    SqlAst.Expr w = args.get(1);
                    boolean all = w instanceof SqlAst.ColumnRef c && query.alls().contains(c.pos());
                    if ("count".equals(name)) {
                        if (!all) {
                            throw unsupported("count over a window of observations is v1's (P4): count(x, all)"
                                    + " counts a line's values to date", w.pos());
                        }
                        rel = call(LIBRARY + "count", l.rel(), new ColSpec(l.part()), new ColSpec(TsLines.PERIOD),
                                new ColSpec(v.value()), new ColSpec(out));
                    } else {
                        int n = all ? 0 : observations(w, "a window", name + "(x, 20)");
                        ValueSpecification frame = all ? call("rows", call("unbounded"), new CInteger(0L))
                                : call("rows", new CInteger((long) -(n - 1)), new CInteger(0L));
                        // a value once its window is full (R4): n of them; everything to date from its first
                        rel = call(LIBRARY + WINDOWED.get(name), l.rel(), TsLines.over(l, frame),
                                new ColSpec(v.value()), new CInteger(all ? 1L : n), new ColSpec(out));
                    }
                } else {
                    if (args.size() > 2) {
                        throw invalid(name + " takes a line and how many observations back: " + name + "(x, 1)",
                                fc.pos());
                    }
                    int obs = args.size() == 2 ? observations(args.get(1), "obs", name + "(x, 1)") : 1;
                    List<ValueSpecification> params = new ArrayList<>(List.of(l.rel(), TsLines.over(l, null),
                            new ColSpec(v.value()), new CInteger((long) obs)));
                    if ("returns".equals(name)) {
                        params.add(new EnumValue(LIBRARY + "Returns", "simple"));
                    }
                    params.add(new ColSpec(out));
                    rel = call(LIBRARY + BACK.get(name), params);
                }
                return new TsLines.Valued(l.with(rel), out);
            }

            /** A whole number of observations, from 1. */
            private int observations(SqlAst.Expr e, String what, String example) {
                if (e instanceof SqlAst.IntegerLit n && n.value().signum() > 0
                        && n.value().compareTo(java.math.BigInteger.valueOf(100_000)) <= 0) {
                    return n.value().intValue();
                }
                if (e instanceof SqlAst.StringLit) {
                    throw unsupported("a date as " + what + " ('2023-01-01') is v1's (R4)", e.pos());
                }
                throw invalid(what + " counts observations, a whole number from 1: " + example, e.pos());
            }
        }

        /** Whether {@code e} is a reference to the column {@code name}. */
        private static boolean names(SqlAst.Expr e, String name) {
            return e instanceof SqlAst.ColumnRef c && c.parts().size() == 1
                    && c.parts().get(0).text().equalsIgnoreCase(name);
        }

        private SqlAst.Expr perPoint(SqlAst.Expr e) {
            visit(e, x -> {
                switch (x) {
                    case SqlAst.Window w -> throw invalid("#TS writes a window as the function's argument,"
                            + " avg(x, 20): OVER is SQL's", w.pos());
                    case SqlAst.ScalarSubquery s -> throw invalid("a subquery in an expression is not #TS's: a"
                            + " source is FROM's", s.pos());
                    case SqlAst.Exists s -> throw invalid("a subquery in an expression is not #TS's: a source is"
                            + " FROM's", s.pos());
                    case SqlAst.InSubquery s -> throw invalid("a subquery in an expression is not #TS's: write the"
                            + " values, x IN ('a', 'b')", s.pos());
                    case SqlAst.Quantified s -> throw invalid("a subquery in an expression is not #TS's: a source"
                            + " is FROM's", s.pos());
                    case SqlAst.Param p -> throw invalid("#TS has no parameters", p.pos());
                    case SqlAst.FunctionCall fc when fc.filter() != null -> throw invalid("#TS picks a line's rows"
                            + " by its subscript (R8): write sum(qty['AAPL', side = 'buy'])", fc.pos());
                    default -> {
                    }
                }
            });
            return e;
        }

        // -------------------------------------------------------------- object mode

        /** The paths of {@code e} (a condition or an item of a class read as objects) added to {@code paths}: every
         *  column it names but an earlier item ({@code earlier}) or {@code all}; a later item's name refused. */
        private void collect(SqlAst.Expr e, @com.legend.base.Nullable String alias, Paths paths, Set<String> earlier,
                Set<String> later) {
            visit(e, x -> {
                if (!(x instanceof SqlAst.ColumnRef c) || query.alls().contains(c.pos())) {
                    return;
                }
                if (c.parts().size() == 1 && earlier.contains(text(c))) {
                    return;
                }
                if (c.parts().size() == 1 && later.contains(text(c))) {
                    throw invalid(text(c) + " is a later item: an item reads the items before it (R7)", c.pos());
                }
                paths.add(stripped(c, alias));
            });
        }

        /** The property names of {@code cls} and its superclasses' (R7: an item takes none of them). */
        private List<String> properties(String cls) {
            return model.properties(cls);
        }

        /**
         * THE PATHS A CLASS'S QUERY READS (R1.1): each a property, or a dotted path through associations to one object
         * ({@code instrument.sector.name}), checked against the model; each the column of its last step's name
         * (two ending alike refused, asking for a flat source); projected, the date's first, in the order read.
         */
        private final class Paths {
            private final String cls;
            /** Each path read, by its dotted text: its column and its properties' names. */
            private final Map<String, Path> read = new LinkedHashMap<>();

            Paths(String cls) {
                this.cls = cls;
            }

            boolean has(SqlAst.ColumnRef c) {
                return read.containsKey(key(c));
            }

            /** The column of path {@code c}, read as its last step's name. */
            String add(SqlAst.ColumnRef c) {
                Path known = read.get(key(c));
                return known != null ? known.column() : add(c, c.parts().get(c.parts().size() - 1).text());
            }

            /** The column of path {@code c}, read as {@code name} (a flat source's). */
            String add(SqlAst.ColumnRef c, String name) {
                String key = key(c);
                Path known = read.get(key);
                if (known != null) {
                    return known.column();
                }
                List<String> names = check(c);
                for (Map.Entry<String, Path> other : read.entrySet()) {
                    if (other.getValue().column().equalsIgnoreCase(name)) {
                        throw invalid(other.getKey() + " and " + key + " are both read as " + name + ": name them"
                                + " once in a flat source, FROM (SELECT " + other.getKey() + " AS ..., " + key
                                + " AS ... FROM " + cls + ") (R1)", c.pos());
                    }
                }
                read.put(key, new Path(name, names));
                return name;
            }

            String columnOf(SqlAst.ColumnRef c) {
                Path found = read.get(key(c));
                if (found == null) {
                    throw new IllegalStateException("a path the query reads was not collected: " + key(c));
                }
                return found.column();
            }

            List<String> columns() {
                return read.values().stream().map(Path::column).toList();
            }

            /** {@code Class.all()->project(~[name: x|$x.a.b, ...])}. */
            ValueSpecification projection() {
                List<ColSpec> cols = new ArrayList<>();
                for (Path path : read.values()) {
                    ValueSpecification nav = new Variable("x");
                    for (String step : path.steps()) {
                        nav = new AppliedProperty(nav, step);
                    }
                    cols.add(new ColSpec(path.column(), lambda("x", nav)));
                }
                return call("project", call("getAll", new PackageableElementPtr(cls)), new ColSpecArray(cols));
            }

            /** The steps of path {@code c}, each a property of the class before it holding one value; its last not a
             *  class. */
            private List<String> check(SqlAst.ColumnRef c) {
                String at = cls;
                List<String> names = new ArrayList<>();
                for (int i = 0; i < c.parts().size(); i++) {
                    SqlAst.Ident step = c.parts().get(i);
                    Property p = property(model, at, step.text());
                    if (p == null) {
                        throw invalid(at + " has no property " + step.text() + (i == 0 ? "" : " (in " + key(c)
                                + ")"), step.pos());
                    }
                    if (p.parameters()) {
                        throw invalid(step.text() + " takes parameters: a path reads a property", step.pos());
                    }
                    if (!p.one()) {
                        throw invalid(key(c) + " goes through " + step.text() + ", which holds many: a path follows"
                                + " an association to one object (a row per element is a later FLATTEN, R1)",
                                step.pos());
                    }
                    names.add(p.name());
                    boolean last = i == c.parts().size() - 1;
                    String cls = p.cls();
                    if (cls != null) {
                        if (last) {
                            throw invalid(key(c) + " is a " + cls + ": name one of its properties, " + key(c)
                                    + ".name", step.pos());
                        }
                        at = cls;
                    } else if (!last) {
                        throw invalid(step.text() + " is a " + p.type() + ": it has no property "
                                + c.parts().get(i + 1).text(), c.parts().get(i + 1).pos());
                    }
                }
                return names;
            }
        }

        // -------------------------------------------------------------- columns

        /** The columns of {@code relation}, typed as a FROM source's. */
        private List<String> columnsOf(ValueSpecification relation) {
            List<String> out = new ArrayList<>();
            sources.columns(relation, (name, type, nullable, storeType) -> out.add(name));
            return out;
        }
    }

    // ------------------------------------------------------------------ expressions

    /** The column of {@code names} {@code c} names, by Postgres's rule (a quoted name exactly; an unquoted one
     *  exactly, else the one name equal ignoring case); null for none. */
    static @com.legend.base.Nullable String resolve(List<String> names, SqlAst.ColumnRef c) {
        SqlAst.Ident id = c.parts().get(c.parts().size() - 1);
        if (names.contains(id.text())) {
            return id.text();
        }
        if (id.quoted()) {
            return null;
        }
        List<String> alike = names.stream().filter(n -> n.equalsIgnoreCase(id.text())).toList();
        return alike.size() == 1 ? alike.get(0) : null;
    }

    /** {@code c} without the source's alias ({@code px.close} is {@code close}). */
    private static SqlAst.ColumnRef stripped(SqlAst.ColumnRef c, @com.legend.base.Nullable String alias) {
        return alias != null && c.parts().size() > 1 && c.parts().get(0).text().equals(alias)
                ? new SqlAst.ColumnRef(c.parts().subList(1, c.parts().size()), c.pos()) : c;
    }

    private static String key(SqlAst.ColumnRef c) {
        return String.join(".", c.parts().stream().map(SqlAst.Ident::text).toList());
    }

    private static String text(SqlAst.ColumnRef c) {
        return key(c);
    }

    private static SqlAst.ColumnRef column(String name, SqlAst.Pos pos) {
        return new SqlAst.ColumnRef(List.of(new SqlAst.Ident(name, true, pos)), pos);
    }

    /** A call's name, folded as Postgres folds an unquoted one; null for a qualified name. */
    private static @com.legend.base.Nullable String fn(SqlAst.FunctionCall fc) {
        if (fc.name().size() != 1) {
            return null;
        }
        SqlAst.Ident id = fc.name().get(0);
        return id.quoted() ? id.text() : id.text().toLowerCase(Locale.ROOT);
    }

    /** {@code e} rebuilt bottom-up, {@code f} applied to each node once its children are. */
    static SqlAst.Expr map(SqlAst.Expr e, UnaryOperator<SqlAst.Expr> f) {
        return f.apply(children(e, c -> map(c, f)));
    }

    /** {@code see} shown every node of {@code e}. */
    static void visit(SqlAst.Expr e, java.util.function.Consumer<SqlAst.Expr> see) {
        map(e, x -> {
            see.accept(x);
            return x;
        });
    }

    /** {@code e} with {@code f} applied to each of its direct children (a subquery's own query left as it is). */
    static SqlAst.Expr children(SqlAst.Expr e, UnaryOperator<SqlAst.Expr> f) {
        return switch (e) {
            case SqlAst.ColumnRef x -> x;
            case SqlAst.IntegerLit x -> x;
            case SqlAst.NumericLit x -> x;
            case SqlAst.StringLit x -> x;
            case SqlAst.BooleanLit x -> x;
            case SqlAst.NullLit x -> x;
            case SqlAst.TypedLit x -> x;
            case SqlAst.Param x -> x;
            case SqlAst.Unary x -> new SqlAst.Unary(x.op(), f.apply(x.operand()), x.pos());
            case SqlAst.Binary x -> new SqlAst.Binary(x.op(), f.apply(x.left()), f.apply(x.right()), x.pos());
            case SqlAst.Operator x -> new SqlAst.Operator(x.operator(), x.left() == null ? null : f.apply(x.left()),
                    f.apply(x.right()), x.pos());
            case SqlAst.IsNull x -> new SqlAst.IsNull(f.apply(x.operand()), x.negated(), x.pos());
            case SqlAst.IsTruth x -> new SqlAst.IsTruth(f.apply(x.operand()), x.truth(), x.negated(), x.pos());
            case SqlAst.IsDistinct x -> new SqlAst.IsDistinct(f.apply(x.left()), f.apply(x.right()), x.negated(),
                    x.pos());
            case SqlAst.InList x -> new SqlAst.InList(f.apply(x.operand()), each(x.values(), f), x.negated(),
                    x.pos());
            case SqlAst.Between x -> new SqlAst.Between(f.apply(x.operand()), f.apply(x.low()), f.apply(x.high()),
                    x.negated(), x.pos());
            case SqlAst.Like x -> new SqlAst.Like(f.apply(x.operand()), f.apply(x.pattern()),
                    x.escape() == null ? null : f.apply(x.escape()), x.negated(), x.caseInsensitive(), x.pos());
            case SqlAst.Case x -> new SqlAst.Case(x.operand() == null ? null : f.apply(x.operand()),
                    x.whens().stream().map(w -> new SqlAst.When(f.apply(w.condition()), f.apply(w.result())))
                            .toList(), x.otherwise() == null ? null : f.apply(x.otherwise()), x.pos());
            case SqlAst.FunctionCall x -> new SqlAst.FunctionCall(x.name(), each(x.args(), f), x.distinct(),
                    x.star(), x.orderBy(), x.withinGroup(), x.filter() == null ? null : f.apply(x.filter()),
                    x.pos());
            case SqlAst.Cast x -> new SqlAst.Cast(f.apply(x.operand()), x.type(), x.pos());
            case SqlAst.ArrayConstructor x -> new SqlAst.ArrayConstructor(each(x.elements(), f), x.pos());
            case SqlAst.Subscript x -> new SqlAst.Subscript(f.apply(x.base()), f.apply(x.index()), x.pos());
            case SqlAst.InSubquery x -> new SqlAst.InSubquery(f.apply(x.operand()), x.query(), x.negated(), x.pos());
            case SqlAst.Quantified x -> new SqlAst.Quantified(x.op(), x.all(), f.apply(x.left()), x.query(),
                    x.pos());
            case SqlAst.ScalarSubquery x -> x;
            case SqlAst.Exists x -> x;
            case SqlAst.Window x -> x;
        };
    }

    private static List<SqlAst.Expr> each(List<SqlAst.Expr> es, UnaryOperator<SqlAst.Expr> f) {
        List<SqlAst.Expr> out = new ArrayList<>(es.size());
        for (SqlAst.Expr e : es) {
            out.add(f.apply(e));
        }
        return out;
    }

    // ------------------------------------------------------------------ Pure

    private static AppliedFunction call(String fn, ValueSpecification... args) {
        return new AppliedFunction(fn, List.of(args));
    }

    private static AppliedFunction call(String fn, List<ValueSpecification> args) {
        return new AppliedFunction(fn, args);
    }

    private static LambdaFunction lambda(String parameter, ValueSpecification body) {
        return new LambdaFunction(List.of(new Variable(parameter)), List.of(body));
    }

    private static SqlTranslationException invalid(String message, SqlAst.Pos pos) {
        return SqlTranslationException.invalid(message, pos);
    }

    private static SqlTranslationException unsupported(String message, SqlAst.Pos pos) {
        return SqlTranslationException.unsupported(message, pos);
    }
}
