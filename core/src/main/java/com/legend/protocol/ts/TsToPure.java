// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.ts;

import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.CString;
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

        /** The overloads of the function of path {@code fqn} ({@link TsWords}); none where the model defines no such
         *  function. */
        List<Overload> overloads(String fqn);

        /** The names of the functions directly in the package {@code pkg} ({@code timeseries::line}: the words). */
        List<String> functionsIn(String pkg);

        /** The name of the function of the package {@code pkg} that {@code word} is an alias of
         *  ({@code timeseries::Words.alias}: avg is mean's), or null where none is. */
        @com.legend.base.Nullable String aliased(String pkg, String word);
    }

    /** A function's overload as {@link Model#overloads} reports it: its parameters in order (a word's lines first), the
     *  columns of the relation it answers after its key and its date (a word's value, or a struct's fields: a band's
     *  lower, upper and width), its documentation ({@code doc.doc}; none: null), and the other names #TS reads it by
     *  ({@code timeseries::Words.alias}: mean's avg); and its facts, the {@code timeseries::Words} profile's other
     *  tags by name in the order written ({@code unit}, {@code level}, {@code param}, ...), and its stereotypes
     *  ({@code band}, {@code candles}, {@code anchored}) -- what a drawing and a formula box know of a word (A3 step 1);
     *  and, where it is a default -- its body one call of its own function, of its parameters and literals
     *  ({@code correlation(x, y)}'s {@code timeseries::line::correlation($x, $y, 63, $sample)}) -- that call's
     *  arguments, each a parameter's {@link com.legend.protocol.spec.Variable} or a literal (W5, 2026-11-06: the
     *  words read a default as the call it is, so its value is written once, in its body); else null. */
    public record Overload(List<Parameter> params, List<String> gives, @com.legend.base.Nullable String doc,
            List<String> aliases, Map<String, List<String>> facts, List<String> marks,
            @com.legend.base.Nullable List<ValueSpecification> delegate) {
        public Overload {
            aliases = List.copyOf(aliases);
            facts = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(facts));
            marks = List.copyOf(marks);
            delegate = delegate == null ? null : List.copyOf(delegate);
        }
    }

    /** A function's parameter as {@link Model#overloads} reports it: its name, its type's path ({@code Integer},
     *  {@code timeseries::Returns}; a line {@link #LINE}, weighted members {@link #MEMBERS}), and whether it may be given
     *  none (multiplicity {@code [0..1]}). */
    public record Parameter(String name, String type, boolean optional) {
    }

    /** A parameter's type where it is a line, {@code Relation<(key:K, date:D, value:Number)>}. */
    public static final String LINE = "Line";

    /** A parameter's type where it is a line with each value's number in time, in seconds since 1970 -- its moment,
     *  under SAMPLE BY its period's last tick's: {@code Relation<(key:K, date:D, time:Integer, value:Number)>}, a word
     *  that accrues over time (a rate's). */
    public static final String TIMED_LINE = "TimedLine";

    /** A parameter's type where it is lines with their weights, a long relation {@code Relation<(key:K, date:D,
     *  member:Integer, weight:Float, members:Integer, value:Number)>}: a basket's members, each row its member's number
     *  from 0, its weight and how many members were written. */
    public static final String MEMBERS = "Members";

    /** A parameter's type where it is weighted members with each value's number in time ({@link #TIMED_LINE}'s):
     *  {@code Relation<(key:K, date:D, time:Integer, member:Integer, weight:Float, members:Integer, value:Number)>}. */
    public static final String TIMED_MEMBERS = "TimedMembers";

    /** A class's property, as {@link Model#property} reports it: its name, its type's name, the class it holds
     *  (none: null), whether it holds one value at most, whether it takes parameters (a qualified property). */
    @FunctionalInterface
    public interface PropertySink {
        void property(String name, String type, @com.legend.base.Nullable String cls, boolean one, boolean parameters);
    }

    /** A property {@link Model#property} reported. */
    record Property(String name, String type, @com.legend.base.Nullable String cls, boolean one, boolean parameters) {
    }

    /** The sources a statement's plain SQL reads (TsParser.plain): the query's own, and each WITH by its name, a
     *  relation the #TS dialect reads as a table (TsSql.table). */
    private record Plain(SqlToPure.Sources of, Map<String, ValueSpecification> named) implements TsSql.Named {

        @Override
        public @com.legend.base.Nullable ValueSpecification relation(String name) {
            return named.get(name);
        }

        @Override
        public void columns(ValueSpecification source, SqlToPure.Column column) {
            of.columns(source, column);
        }

        @Override
        public @com.legend.base.Nullable List<String> primaryKey(String database, String table) {
            return of.primaryKey(database, table);
        }

        @Override
        public void tables(SqlToPure.Table table) {
            of.tables(table);
        }

        @Override
        public boolean variable(String name, SqlToPure.Scoped type) {
            return of.variable(name, type);
        }

        @Override
        public void variables(java.util.function.Consumer<String> names) {
            of.variables(names);
        }

        @Override
        public boolean isEnumeration(String type) {
            return of.isEnumeration(type);
        }

        @Override
        public ValueSpecification inlineView(ValueSpecification relation) {
            return of.inlineView(relation);
        }
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

    /** The prefix of the names {@link #bindShared} binds: the translator's own, as its columns' {@code __ts_}. */
    public static final String SHARED = "__ts_l";

    /** A translation's lets, in order, and its answer, which reads them. */
    public record Bound(List<ValueSpecification> lets, ValueSpecification answer) {
    }

    /**
     * A TRANSLATION'S REUSED RELATIONS BOUND ONCE (2026-11-04): {@code raw}, a translation, its names not yet resolved,
     * with each call it reaches from more than one place -- the translator builds a relation once and reads it at each
     * use, an item a later item reads, a line two items read -- replaced by a fresh name (one {@code taken} does not
     * hold, then held), that name's {@code let} among the lets, after the lets of the calls it reads. Only a call whose
     * free names are all in {@code names} (the query's own: its parameters, the lets before it) is bound; one that
     * reads a lambda's parameter stays where it is.
     */
    public static Bound bindShared(ValueSpecification raw, java.util.Set<String> names, java.util.Set<String> taken) {
        java.util.IdentityHashMap<ValueSpecification, Integer> reads = new java.util.IdentityHashMap<>();
        countReads(raw, reads);
        java.util.IdentityHashMap<ValueSpecification, java.util.Set<String>> free = new java.util.IdentityHashMap<>();
        java.util.IdentityHashMap<ValueSpecification, ValueSpecification> rebuilt = new java.util.IdentityHashMap<>();
        List<ValueSpecification> lets = new ArrayList<>();
        ValueSpecification answer = rebuild(raw, reads, free, names, taken, lets, rebuilt);
        return new Bound(List.copyOf(lets), answer);
    }

    /** Each node's reads: the parents that name it, each once per place (a node a parent names twice is read twice). */
    private static void countReads(ValueSpecification root, java.util.IdentityHashMap<ValueSpecification, Integer> reads) {
        java.util.ArrayDeque<ValueSpecification> todo = new java.util.ArrayDeque<>();
        java.util.Set<ValueSpecification> seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        todo.push(root);
        seen.add(root);
        while (!todo.isEmpty()) {
            for (ValueSpecification c : todo.pop().children()) {
                reads.merge(c, 1, Integer::sum);
                if (seen.add(c)) {
                    todo.push(c);
                }
            }
        }
    }

    private static ValueSpecification rebuild(ValueSpecification v,
            java.util.IdentityHashMap<ValueSpecification, Integer> reads,
            java.util.IdentityHashMap<ValueSpecification, java.util.Set<String>> free, java.util.Set<String> names,
            java.util.Set<String> taken, List<ValueSpecification> lets,
            java.util.IdentityHashMap<ValueSpecification, ValueSpecification> rebuilt) {
        ValueSpecification done = rebuilt.get(v);
        if (done != null) {
            return done;
        }
        List<ValueSpecification> kids = v.children();
        ValueSpecification out = v;
        if (!kids.isEmpty()) {
            List<ValueSpecification> nk = new ArrayList<>(kids.size());
            boolean changed = false;
            for (ValueSpecification k : kids) {
                ValueSpecification r = rebuild(k, reads, free, names, taken, lets, rebuilt);
                changed |= r != k;
                nk.add(r);
            }
            out = changed ? v.withChildren(nk) : v;
        }
        if (v instanceof com.legend.protocol.spec.AppliedFunction && reads.getOrDefault(v, 0) > 1
                && names.containsAll(freeNames(v, free))) {
            String name = fresh(taken);
            lets.add(call("letFunction", new CString(name), out));
            out = new Variable(name);
        }
        rebuilt.put(v, out);
        return out;
    }

    /** The names {@code v} reads that no lambda inside it binds. */
    private static java.util.Set<String> freeNames(ValueSpecification v,
            java.util.IdentityHashMap<ValueSpecification, java.util.Set<String>> memo) {
        java.util.Set<String> known = memo.get(v);
        if (known != null) {
            return known;
        }
        java.util.Set<String> out = new java.util.HashSet<>();
        if (v instanceof com.legend.protocol.spec.Variable var) {
            out.add(var.name());
        } else {
            for (ValueSpecification c : v.children()) {
                out.addAll(freeNames(c, memo));
            }
            if (v instanceof com.legend.protocol.spec.LambdaFunction lf) {
                lf.parameters().forEach(p -> out.remove(p.name()));
            }
        }
        java.util.Set<String> frozen = java.util.Collections.unmodifiableSet(out);
        memo.put(v, frozen);
        return frozen;
    }

    /** A name of {@link #SHARED}'s no statement of the query has taken, taken. */
    private static String fresh(java.util.Set<String> taken) {
        for (int i = 0; ; i++) {
            String name = SHARED + i;
            if (taken.add(name)) {
                return name;
            }
        }
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
        /** Each relation's columns as the model typed them, once (a node never changes): a source's when it is opened
         *  (FROM), a per-point lambda's relation when its lambda is written -- a subscript's condition at each read, a
         *  WHERE, an expression's -- read again, never typed again (the compile-time homework, 2026-11-03: the source
         *  typed five times for a query of one item and thirty-three for eight, and each expression's table twice). */
        private final java.util.IdentityHashMap<ValueSpecification, List<Held>> opened = new java.util.IdentityHashMap<>();
        private final SqlToPure.Sources sources;
        private final InputDialect dialect;
        private final TsAst.Query query;
        private final Map<String, ValueSpecification> withs = new LinkedHashMap<>();

        Builder(Model model, InputDialect dialect, TsAst.Query query) {
            this.model = model;
            this.sources = new Opened(model, opened);
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
            if (query.plain() != null) {
                // plain SQL over the series queries the WITHs name (the spec's §8): each name a table of its relation
                return SqlToPure.translate(query.plain(), new Plain(sources, withs), dialect);
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
                java.util.Objects.requireNonNull(types.get(r.date()), "the source's date column");
                opened.put(o.key(), new TsLines.Source(rel, r.date(), r.instrument(),
                        o.alias().isEmpty() ? "the source" : o.alias()));
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
            List<String> columns = new ArrayList<>();
            for (int i = 0; i < exprs.size(); i++) {
                TsLines.Node n = lines.item(exprs.get(i));
                if (!(n instanceof TsLines.Valued v)) {
                    throw invalid(names.get(i) + " reads no line: an item is a line of a source's column, or a"
                            + " figure of one (R2)", aliases.get(i).pos());
                }
                String name = names.get(i);
                Map<String, String> struct = lines.structOf(v);
                if (struct == null) {
                    // the item's name a column of its base's table (R7: one table, its rows once), its value copied:
                    // a later item may read the value under its own name
                    TsLines.Valued item = new TsLines.Valued(lines.grown(v.line(), rel -> call("extend", rel,
                            new ColSpec(name, lambda("x", new AppliedProperty(new Variable("x"), v.value()))))),
                            name);
                    // its points its value's
                    if (!TsLines.ORD.equals(lines.presence(v))) {
                        lines.present(name, lines.presence(v));
                    }
                    lines.items.put(name, item);
                    answered.add(item);
                    columns.add(name);
                    continue;
                }
                // a struct (R10): each field the answer's column <item>_<field>, read in the query as item.field, a
                // column of its base's table, its value copied
                List<ColSpec> copies = new ArrayList<>();
                Map<String, String> fields = new LinkedHashMap<>();
                for (Map.Entry<String, String> f : struct.entrySet()) {
                    String column = name + "_" + f.getKey();
                    if (names.stream().anyMatch(c -> c.equalsIgnoreCase(column))
                            || own.stream().anyMatch(c -> c.equalsIgnoreCase(column))) {
                        throw invalid(name + "'s " + f.getKey() + " is the answer's column " + column + ", a name"
                                + " the query has already (R10): name the item another name", aliases.get(i).pos());
                    }
                    copies.add(new ColSpec(column, lambda("x", new AppliedProperty(new Variable("x"), f.getValue()))));
                    fields.put(f.getKey(), column);
                    columns.add(column);
                }
                TsLines.Line line = lines.grown(v.line(), rel -> call("extend", rel, new ColSpecArray(copies)));
                lines.fields.put(name, java.util.Collections.unmodifiableMap(fields));
                lines.items.put(name, new TsLines.Valued(line, java.util.Objects.requireNonNull(fields.get(
                        struct.keySet().iterator().next()))));
                fields.values().forEach(c -> answered.add(new TsLines.Valued(line, c)));
            }
            // each item read from its base's newest table: items of one base one table, its rows once (R7)
            for (int i = 0; i < answered.size(); i++) {
                answered.set(i, new TsLines.Valued(lines.newest(answered.get(i).line()), answered.get(i).value()));
            }
            TsLines.Line all = TsLines.answer(lines, answered, aliases.get(0).pos());
            String instrument = all.byInstrument() ? all.part() : null;
            // a period dated at its latest line's date (R5: one date for every line)
            ValueSpecification rel = call("rename", all.rel(), new ColSpec(all.at()), new ColSpec(date));
            List<String> answer = new ArrayList<>(List.of(date));
            if (instrument != null) {
                answer.add(instrument);
            }
            answer.addAll(columns);
            rel = call("select", rel, new ColSpecArray(answer.stream().map(ColSpec::new).toList()));
            // a row where at least one item has a value (R6): the library's someValued over the items' columns
            rel = call(TsLines.LINES + "someValued", rel, new ColSpecArray(columns.stream().map(ColSpec::new).toList()));
            if (query.filter() != null) {
                // a struct's field as the answer names it: bb.lower its column bb_lower (R10)
                SqlAst.Expr condition = map(intervals(perPoint(query.filter())), x -> x instanceof SqlAst.ColumnRef c
                        && c.parts().size() == 2 && lines.fields.containsKey(c.parts().get(0).text())
                        && java.util.Objects.requireNonNull(lines.fields.get(c.parts().get(0).text()))
                                .containsKey(c.parts().get(1).text())
                        ? TsToPure.column(java.util.Objects.requireNonNull(java.util.Objects.requireNonNull(
                                lines.fields.get(c.parts().get(0).text())).get(c.parts().get(1).text())), c.pos())
                        : x);
                // latest (R9): the data's last date -- every source's after its WHERE, the latest of them -- (an
                // item's own name where one is called so)
                String latest = null;
                if (answer.stream().noneMatch(c -> c.equalsIgnoreCase("latest"))) {
                    boolean[] reads = new boolean[1];
                    visit(condition, x -> reads[0] |= x instanceof SqlAst.ColumnRef c && c.parts().size() == 1
                            && !c.parts().get(0).quoted() && "latest".equalsIgnoreCase(c.parts().get(0).text()));
                    if (reads[0]) {
                        // the library's latestOf the first source, laterOf each next: one date for the query,
                        // whichever items it answers
                        for (TsLines.Source s : opened.values()) {
                            String next = lines.fresh("latest");
                            rel = latest == null
                                    ? call(TsLines.LINES + "latestOf", rel, s.relation(), new ColSpec(s.date()),
                                            new ColSpec(next))
                                    : call(TsLines.LINES + "laterOf", rel, new ColSpec(latest), s.relation(),
                                            new ColSpec(s.date()), new ColSpec(next));
                            latest = next;
                        }
                        String l = java.util.Objects.requireNonNull(latest, "a query of no source");
                        condition = map(condition, x -> x instanceof SqlAst.ColumnRef c && c.parts().size() == 1
                                && !c.parts().get(0).quoted() && "latest".equalsIgnoreCase(c.parts().get(0).text())
                                ? TsToPure.column(l, c.pos()) : x);
                    }
                }
                String reading = latest;
                visit(condition, x -> {
                    if (x instanceof SqlAst.ColumnRef c && (c.parts().size() > 1 || resolve(answer, c) == null)
                            && !(reading != null && c.parts().get(0).text().equals(reading))) {
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
                if (latest != null) {
                    rel = call("select", rel, new ColSpecArray(answer.stream().map(ColSpec::new).toList()));
                }
            }
            // the answer's order, the library's: by date, then by instrument
            return instrument == null ? call(TsLines.LINES + "ordered", rel, new ColSpec(date))
                    : call(TsLines.LINES + "ordered", rel, new ColSpec(date), new ColSpec(instrument));
        }

        /** The types of {@code relation}'s columns, by name, in order: a source opened, its columns held. */
        private Map<String, String> typed(ValueSpecification relation) {
            Map<String, String> out = new LinkedHashMap<>();
            List<Held> held = new ArrayList<>();
            sources.columns(relation, (name, type, nullable, storeType) -> {
                out.put(name, type);
                held.add(new Held(name, type, nullable, storeType));
            });
            opened.put(relation, List.copyOf(held));
            return out;
        }

        /** {@code of} as a key by identity: a line is the very line, its relation a graph never compared by value. */
        private record Same(Object of) {
            @Override
            public boolean equals(@com.legend.base.Nullable Object o) {
                return o instanceof Same s && s.of == of;
            }

            @Override
            public int hashCode() {
                return System.identityHashCode(of);
            }
        }

        /** A column of a source opened, as the model gave it. */
        private record Held(String name, String type, boolean nullable, @com.legend.base.Nullable String storeType) {
        }

        /** The model's facts, a source opened answering its columns as it was given them. */
        private record Opened(SqlToPure.Sources of, java.util.IdentityHashMap<ValueSpecification, List<Held>> held)
                implements SqlToPure.Sources {
            @Override
            public void columns(ValueSpecification source, SqlToPure.Column column) {
                List<Held> cs = held.get(source);
                if (cs == null) {
                    of.columns(source, column);
                    return;
                }
                cs.forEach(c -> column.column(c.name(), c.type(), c.nullable(), c.storeType()));
            }

            @Override
            public @com.legend.base.Nullable List<String> primaryKey(String database, String table) {
                return of.primaryKey(database, table);
            }

            @Override
            public void tables(SqlToPure.Table table) {
                of.tables(table);
            }

            @Override
            public boolean isEnumeration(String type) {
                return of.isEnumeration(type);
            }

            @Override
            public boolean variable(String name, SqlToPure.Scoped type) {
                return of.variable(name, type);
            }

            @Override
            public void variables(java.util.function.Consumer<String> names) {
                of.variables(names);
            }

            @Override
            public ValueSpecification inlineView(ValueSpecification relation) {
                return of.inlineView(relation);
            }
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
                        && !query.spans().containsKey(c.pos()) ? TsToPure.column(single(o, c, "WHERE"), c.pos()) : x);
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
                if (query.alls().contains(c.pos()) || query.spans().containsKey(c.pos())) {
                    return c;
                }
                if (c.parts().size() == 1 && names.subList(0, i).contains(text(c))) {
                    return c;
                }
                if (c.parts().size() == 1 && names.contains(text(c))) {
                    throw invalid(text(c) + " is a later item: an item reads the items before it (R7)", c.pos());
                }
                String first = c.parts().get(0).text();
                if (c.parts().size() == 2 && names.subList(0, i).contains(first)) {
                    // an earlier item's field (R10): bb.lower
                    if (opening.stream().anyMatch(x -> x.alias().equals(first))) {
                        throw invalid(first + " names an item and a source: name the item another name (R7)",
                                c.pos());
                    }
                    return c;
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
                case TsAst.Nested n -> {
                    // a query of the language's own: its answer, read on as a relation (its WITH sources this one's)
                    Builder inner = new Builder(model, dialect, n.query());
                    inner.withs.putAll(withs);
                    yield new Rows(inner.query(), "the query in parentheses");
                }
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
                paths.take(computed.keySet());
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
        /** A relation's columns as the per-point SQL reads them (R3): a figure the library types Number a float (double
         *  precision) -- every figure is one, on every database, as the tile computes it; Postgres's numeric is exact
         *  decimal, which no figure is. */
        private record Figures(SqlToPure.Sources of) implements SqlToPure.Sources {
            @Override
            public void columns(ValueSpecification source, SqlToPure.Column column) {
                of.columns(source, (name, type, nullable, storeType) -> column.column(name,
                        "Number".equals(type) ? "Float" : type, nullable, storeType));
            }

            @Override
            public @com.legend.base.Nullable List<String> primaryKey(String database, String table) {
                return of.primaryKey(database, table);
            }

            @Override
            public void tables(SqlToPure.Table table) {
                of.tables(table);
            }

            @Override
            public boolean isEnumeration(String type) {
                return of.isEnumeration(type);
            }

            @Override
            public boolean variable(String name, SqlToPure.Scoped type) {
                return of.variable(name, type);
            }

            @Override
            public void variables(java.util.function.Consumer<String> names) {
                of.variables(names);
            }

            @Override
            public ValueSpecification inlineView(ValueSpecification relation) {
                return of.inlineView(relation);
            }
        }

        private LambdaFunction rowLambda(SqlAst.Expr e, ValueSpecification relation, SqlToPure.Sources sources,
                boolean condition, String clause) {
            if (!opened.containsKey(relation)) {
                typed(relation);   // its columns typed once: the lambda's and its enumerations' read them
            }
            LambdaFunction l = SqlToPure.rowLambda(e, relation, new Figures(sources), dialect, condition, clause);
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
            return call("filter", relation, rowLambda(intervals(condition), relation, sources, true, clause));
        }

        // -------------------------------------------------------------- items

        /**
         * THE ITEMS' LINES (R2, R4, R6; {@link TsLines}): each expression's node -- a source's column its line (a
         * subscript's, one instrument's), an earlier item its own, a figure along time the library's call over its
         * line's rows, any other expression of lines the lines met on their dates and the expression Postgres's over
         * them, an expression of no line a constant.
         */
        private final class Lines implements TsFunctions.Ctx {
            private final Map<String, TsLines.Source> opened;
            final Map<String, TsLines.Valued> items = new LinkedHashMap<>();
            /** Each struct's fields (R10), by the column a function answered it as. */
            private final Map<String, Map<String, String>> structs = new LinkedHashMap<>();
            /** Each item that is a struct: its fields' answer columns ({@code bb_lower}), by field. */
            final Map<String, Map<String, String>> fields = new LinkedHashMap<>();
            /** Each line made of another by adding a column (its rows and its columns, and one more), by the very line:
             *  a base's columns are one table (R7: one line's rows, never joined to themselves -- a line of several
             *  rows on a date would multiply them). */
            private final Map<TsLines.Line, TsLines.Line> extended = new java.util.IdentityHashMap<>();
            /** Each base's newest line, the base itself with every column added to it so far, by the very base. */
            private final Map<TsLines.Line, TsLines.Line> newest = new java.util.IdentityHashMap<>();
            /** Each source's column read, by what it reads ({@link TsLines.Writer#read}). */
            private final Map<Object, TsLines.Valued> reads = new java.util.HashMap<>();
            /** Each value that is not a point of every row of its table: the column of its points, by its column. */
            private final Map<String, String> presence = new java.util.HashMap<>();
            /** Each set of lines met ({@link #met}), by each one's base, value and points, in order. */
            private final Map<List<Object>, TsLines.Line> mets = new java.util.HashMap<>();
            /** Each met line's base, the bases whose rows hold its rows' periods (the lines met to make it, theirs in
             *  turn): its parts' of its own key column (a line of no instrument has its own, ONE_LINE), by the very base --
             *  a meeting keeps the periods every part has. */
            private final Map<TsLines.Line, java.util.Set<TsLines.Line>> within = new java.util.IdentityHashMap<>();
            /** Each met line's base, the value columns it carries: its first part's (and what that one carries),
             *  each other part's (TsLines.joined) -- a value's column is named for it alone, fresh, so a name it
             *  carries is that very value. */
            private final Map<TsLines.Line, java.util.Set<String>> carried = new java.util.IdentityHashMap<>();
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

            @Override
            public TsAst.Query query() {
                return query;
            }

            @Override
            public Model model() {
                return model;
            }

            @Override
            public void struct(String value, Map<String, String> of) {
                structs.put(value, of);
            }

            /** The fields of {@code v}, a function's answer, where it is a struct (R10); else null. */
            @com.legend.base.Nullable Map<String, String> structOf(TsLines.Valued v) {
                return structs.get(v.value());
            }

            @Override
            public TsLines.Valued read(Object key, java.util.function.Supplier<TsLines.Valued> make) {
                TsLines.Valued v = reads.get(key);
                if (v == null) {
                    v = make.get();
                    reads.put(key, v);
                }
                return v;
            }

            /** The base {@code line} is made of by added columns ({@link #extended}): itself where it is one. */
            private TsLines.Line base(TsLines.Line line) {
                TsLines.Line l = line;
                for (TsLines.Line of = extended.get(l); of != null; of = extended.get(l)) {
                    l = of;
                }
                return l;
            }

            /** {@code line}'s base with every column added to it so far: every column of {@code line}, its rows. */
            @Override
            public TsLines.Line newest(TsLines.Line line) {
                TsLines.Line b = base(line);
                TsLines.Line n = newest.get(b);
                return n == null ? b : n;
            }

            @Override
            public String presence(TsLines.Valued v) {
                return presence.getOrDefault(v.value(), TsLines.ORD);
            }

            @Override
            public void present(String value, String points) {
                presence.put(value, points);
            }

            @Override
            public TsLines.Valued points(TsLines.Valued v) {
                String p = presence.get(v.value());
                return p == null ? v : new TsLines.Valued(v.line().with(call(TsWords.ROWS + "points", v.line().rel(),
                        new ColSpec(p))), v.value());
            }

            /** The lines of {@code parts} met on their dates (R6), each where it is a point: met once for the same
             *  values of the same bases (a later column of a base read from it, the base's rows the same), its newest
             *  table at each read -- two words of two lines, two figures of them, one table (R7 part 3). */
            @Override
            public TsLines.Line met(List<TsLines.Valued> parts, SqlAst.Pos pos) {
                List<Object> key = new ArrayList<>();
                List<TsLines.Valued> read = new ArrayList<>();
                for (TsLines.Valued p : parts) {
                    TsLines.Valued v = new TsLines.Valued(newest(p.line()), p.value());
                    key.add(new Same(base(v.line())));
                    key.add(v.value());
                    key.add(presence(v));
                    read.add(v);
                }
                TsLines.Line m = mets.get(key);
                if (m == null) {
                    // each table once, its rows where every value read of it is a point (each value's own points,
                    // taken in turn): two values of one table are its columns, never the table met with itself
                    Map<TsLines.Line, TsLines.Line> pointed = new java.util.IdentityHashMap<>();
                    for (TsLines.Valued v : read) {
                        TsLines.Line from = pointed.getOrDefault(v.line(), v.line());
                        pointed.put(v.line(), points(new TsLines.Valued(from, v.value())).line());
                    }
                    TsLines.Valued first = read.stream().filter(v -> v.line().byInstrument()).findFirst()
                            .orElse(read.get(0));
                    m = TsLines.joined(this, read.stream().map(v -> new TsLines.Valued(
                            java.util.Objects.requireNonNull(pointed.get(v.line())), v.value())).toList(), pos, null,
                            carried.getOrDefault(base(first.line()), java.util.Set.of()));
                    mets.put(List.copyOf(key), m);
                    // its rows' periods are every part's of its key (an INNER meeting), and so those parts' bases'
                    java.util.Set<TsLines.Line> in = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
                    for (TsLines.Valued v : read) {
                        TsLines.Line b = base(v.line());
                        if (b.part().equals(m.part())) {
                            in.add(b);
                            in.addAll(within.getOrDefault(b, java.util.Set.of()));
                        }
                    }
                    within.put(m, in);
                    java.util.Set<String> cs = new java.util.HashSet<>(carried.getOrDefault(base(first.line()),
                            java.util.Set.of()));
                    read.forEach(v -> cs.add(v.value()));
                    carried.put(m, cs);
                }
                return newest(m);
            }

            @Override
            public TsLines.@com.legend.base.Nullable Line containing(List<TsLines.Line> lines) {
                for (TsLines.Line s : lines) {
                    TsLines.Line sb = base(s);
                    boolean all = true;
                    for (TsLines.Line l : lines) {
                        TsLines.Line lb = base(l);
                        all &= lb == sb || l.part().equals(s.part())
                                && within.getOrDefault(lb, java.util.Set.of()).contains(sb);
                    }
                    if (all) {
                        return s;
                    }
                }
                return null;
            }

            /** {@code line}'s base's newest line ({@link #newest}) with a column {@code add} adds to it: the base's
             *  newest line from now. */
            @Override
            public TsLines.Line grown(TsLines.Line line,
                    java.util.function.Function<ValueSpecification, ValueSpecification> add) {
                TsLines.Line from = newest(line);
                TsLines.Line out = from.with(add.apply(from.rel()));
                extended.put(out, from);
                newest.put(base(from), out);
                return out;
            }

            /** An item's node: a struct allowed where the item is a function's whole answer (R10). */
            TsLines.Node item(SqlAst.Expr e) {
                // a period's aggregate (R5: last(x), sum(x) under SAMPLE BY) before a word of the same name
                if (e instanceof SqlAst.FunctionCall fc && !periodOf(fc) && alongTime(fc)
                        && function(java.util.Objects.requireNonNull(fn(fc)))) {
                    Arguments a = arguments(fc);
                    return TsFunctions.call(this, fc, a.positional(), a.named());
                }
                return node(e);
            }

            /** The node {@code e}, an item's expression with its columns resolved ({@link Resolver}), is. */
            @Override
            public TsLines.Node node(SqlAst.Expr e) {
                if (e instanceof SqlAst.ColumnRef c) {
                    Map<String, String> struct = fields.get(c.parts().get(0).text());
                    if (c.parts().size() == 1 && struct != null) {
                        throw invalid(text(c) + " has " + String.join(", ", struct.keySet()) + ": read one, " + text(c)
                                + "." + struct.keySet().iterator().next() + " (R10)", c.pos());
                    }
                    if (c.parts().size() == 2 && items.containsKey(c.parts().get(0).text())) {
                        String field = c.parts().get(1).text();
                        String column = struct == null ? null : struct.get(field);
                        if (column == null) {
                            throw invalid(struct == null ? c.parts().get(0).text() + " is one value: " + text(c)
                                    + " reads a field of a function's several (R10)" : c.parts().get(0).text()
                                    + " has " + String.join(", ", struct.keySet()) + ", not " + field + " (R10)",
                                    c.pos());
                        }
                        TsLines.Valued whole = java.util.Objects.requireNonNull(items.get(c.parts().get(0).text()));
                        return new TsLines.Valued(whole.line(), column);
                    }
                    TsLines.Valued item = c.parts().size() == 1 ? items.get(text(c)) : null;
                    if (item != null) {
                        return item;
                    }
                    TsLines.Source s = c.parts().size() == 2 ? opened.get(c.parts().get(0).text()) : null;
                    if (s != null) {
                        bare(c.parts().get(1).text(), c.pos());
                        return TsLines.column(this, s, c.parts().get(1).text(), List.of(), s.instrument() != null);
                    }
                    if (query.alls().contains(c.pos())) {
                        throw invalid("all is a window: sum(x, all)", c.pos());
                    }
                }
                if (e instanceof SqlAst.FunctionCall fc && TsParser.PICK.equals(fn(fc))) {
                    bare(((SqlAst.ColumnRef) fc.args().get(0)).parts().get(1).text(), fc.pos());
                    return picked(fc, null);
                }
                if (e instanceof SqlAst.FunctionCall fc && periodOf(fc)) {
                    return period(fc);
                }
                if (e instanceof SqlAst.FunctionCall fc && alongTime(fc)) {
                    String name = fn(fc);
                    if (function(java.util.Objects.requireNonNull(name))) {
                        Arguments a = arguments(fc);
                        TsLines.Node n = TsFunctions.call(this, fc, a.positional(), a.named());
                        Map<String, String> struct = n instanceof TsLines.Valued v ? structs.get(v.value()) : null;
                        if (struct != null) {
                            throw invalid(name + " has " + String.join(", ", struct.keySet()) + ": name it, " + name
                                    + "(...) AS b, and read b." + struct.keySet().iterator().next() + " (R10)",
                                    fc.pos());
                        }
                        return n;
                    }
                    throw new IllegalStateException("no word " + name + " in the library: alongTime said it was one");
                }
                return computed(e);
            }

            /** Refused (R5): a source's column read bare under SAMPLE BY, which says how a period takes its rows. */
            private void bare(String column, SqlAst.Pos pos) {
                if (query.sample() != null) {
                    throw invalid("how is " + column + " taken each period? write last(" + column + "['...']) AS"
                            + " name (or first, max, min, sum, avg), then use the name (R5)", pos);
                }
            }

            /** Whether {@code fc} is a period's aggregate (R5): {@code last(x)} and its kin, one argument, under
             *  SAMPLE BY -- refused without it. */
            private boolean periodOf(SqlAst.FunctionCall fc) {
                String name = fn(fc);
                if (name == null || fc.args().size() != 1 || taken(name) == null) {
                    return false;
                }
                if (query.sample() == null) {
                    throw invalid("first".equals(name)
                            ? "first(x) is a period's first value: add SAMPLE BY 1w for each week's, or write first(x,"
                                    + " all) for the line's first (gs-quant's first; R5, D3)"
                            : "last".equals(name)
                            ? "last(x) is a period's last value: add SAMPLE BY 1w for each week's, or write last(x,"
                                    + " whole) for the line's last on every date (gs-quant's last, the future read; R5,"
                                    + " D3)"
                            : "a " + name + " over what? add SAMPLE BY 1w for each week's, or write " + name
                                    + "(x, 20) for the last 20, or " + name + "(x, all) to date (R5)", fc.pos());
                }
                return true;
            }

            /** The value of the library's {@code timeseries::Taken} a period's aggregate called {@code name} is: the
             *  word {@code name} names (its own name, or one of its spellings: avg, mean's -- R15, the library's
             *  tags), where the enumeration has a value of that word's name; null where it names none. */
            private @com.legend.base.Nullable String taken(String name) {
                List<String> values = model.enumeration(LIBRARY + "Taken");
                if (values == null || values.contains(name)) {
                    return values == null ? null : name;
                }
                String word = TsWords.fqn(model, name).substring(TsWords.PACKAGE.length());
                return values.contains(word) ? word : null;
            }

            /** A period's aggregate of a source's column (R5): its line by period. */
            private TsLines.Valued period(SqlAst.FunctionCall fc) {
                SqlAst.Expr arg = fc.args().get(0);
                String taken = java.util.Objects.requireNonNull(taken(java.util.Objects.requireNonNull(fn(fc))));
                if (arg instanceof SqlAst.FunctionCall pick && TsParser.PICK.equals(fn(pick))) {
                    return picked(pick, taken);
                }
                if (arg instanceof SqlAst.ColumnRef c && c.parts().size() == 2
                        && opened.containsKey(c.parts().get(0).text())) {
                    TsLines.Source s = java.util.Objects.requireNonNull(opened.get(c.parts().get(0).text()));
                    return TsLines.sampled(this, s, c.parts().get(1).text(), List.of(), s.instrument() != null,
                            java.util.Objects.requireNonNull(query.sample()).unit(), taken, query.dated());
                }
                throw invalid("a period's " + taken + " is of a source's column, " + taken + "(close['SPX']); of an"
                        + " expression of a source's rows, a flat source's column: FROM (SELECT *, price * qty AS"
                        + " value FROM trades) (R5)", arg.pos());
            }

            /** A subscript's line (R2): the column's rows of one instrument ({@code close['SPX']}), or of those that
             *  meet its conditions ({@code close['SPX', venue = 'NYSE']}, {@code close[sector = 'Energy']}). */
            private TsLines.Valued picked(SqlAst.FunctionCall fc, @com.legend.base.Nullable String taken) {
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
                        // the instrument is named by its value, one way (the user, 2026-11-02): a condition on the
                        // BY column itself is refused with its fix, never read as close['SPX'] by its shape
                        if (instrument != null && m instanceof SqlAst.Binary b && b.op() == SqlAst.BinaryOp.EQ
                                && (names(b.left(), instrument) || names(b.right(), instrument))) {
                            SqlAst.Expr v = names(b.left(), instrument) ? b.right() : b.left();
                            String written = v instanceof SqlAst.StringLit sl ? "'" + sl.value() + "'"
                                    : v instanceof SqlAst.IntegerLit il ? il.value().toString() : "...";
                            throw invalid("a subscript names its instrument by its value, "
                                    + base.parts().get(1).text() + "[" + written + "]: " + instrument
                                    + " is the BY column (R2)", m.pos());
                        }
                        matches.add(m);
                    }
                }
                boolean split = instrument != null && !one;
                return taken == null ? TsLines.column(this, s, base.parts().get(1).text(), matches, split)
                        : TsLines.sampled(this, s, base.parts().get(1).text(), matches, split,
                                java.util.Objects.requireNonNull(query.sample()).unit(), taken, query.dated());
            }

            /** {@code e}, an expression of lines: its lines met on their dates (R6), {@code e} Postgres's over them;
             *  of no line, a constant. */
            private TsLines.Node computed(SqlAst.Expr e) {
                List<TsLines.Valued> parts = new ArrayList<>();
                SqlAst.Expr over = leaves(e, parts);
                if (parts.isEmpty()) {
                    return new TsLines.Constant(e);
                }
                visit(over, x -> {
                    if (x instanceof SqlAst.FunctionCall o && TsParser.OPTION.equals(fn(o))) {
                        throw invalid("an option by name is a timeseries function's: returns(x, type => 'log') (R10)",
                                o.pos());
                    }
                });
                // each part read from its base's newest line: parts of one base are one table, its rows once (R7)
                List<TsLines.Valued> read = parts.stream().map(p -> new TsLines.Valued(newest(p.line()), p.value()))
                        .toList();
                TsAst.Fill fill = query.fill();
                List<String> points = read.stream().map(this::presence).filter(p -> !TsLines.ORD.equals(p)).distinct()
                        .toList();
                String value = fresh("pt");
                if (read.stream().allMatch(p -> p.line() == read.get(0).line()) && (fill == null || points.isEmpty())) {
                    // a column of the parts' table, a point where every part is (the library's rows::both), none
                    // elsewhere (rows::masked)
                    TsLines.Line out = grown(read.get(0).line(), rel -> call("extend", rel, new ColSpec(value,
                            rowLambda(intervals(over), rel, false, "SELECT"))));
                    if (points.isEmpty()) {
                        return new TsLines.Valued(out, value);
                    }
                    String at = points.get(0);
                    for (String next : points.subList(1, points.size())) {
                        String a = at;
                        String both = fresh("pts");
                        out = grown(out, rel -> call(TsWords.ROWS + "both", rel, new ColSpec(a), new ColSpec(next),
                                new ColSpec(both)));
                        at = both;
                    }
                    String p = at;
                    String masked = fresh("pt");
                    out = grown(out, rel -> call(TsWords.ROWS + "masked", rel, new ColSpec(value), new ColSpec(p),
                            new ColSpec(masked)));
                    present(masked, p);
                    return new TsLines.Valued(out, masked);
                }
                // lines meeting (R6): each part as its own line, met on their dates -- once for the same lines (met)
                TsLines.Line met = fill == null ? met(read, e.pos())
                        : TsLines.joined(this, read.stream().map(this::points).toList(), e.pos(), fill.how());
                TsLines.Line out = grown(met, rel -> call("extend", rel, new ColSpec(value, rowLambda(intervals(over),
                        rel, false, "SELECT"))));
                return new TsLines.Valued(out, value);
            }

            /** {@code e} with each line in it read as its column, its lines added to {@code parts} (each once). */
            private SqlAst.Expr leaves(SqlAst.Expr e, List<TsLines.Valued> parts) {
                boolean line = e instanceof SqlAst.ColumnRef c && !query.alls().contains(c.pos())
                        && !query.spans().containsKey(c.pos())
                        || e instanceof SqlAst.FunctionCall fc && (TsParser.PICK.equals(fn(fc)) || periodOf(fc)
                                || alongTime(fc));
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

            /** Whether {@code fc} is a word of the library: a figure along time (R4), the library's call over its
             *  lines' rows. */
            private boolean alongTime(SqlAst.FunctionCall fc) {
                String name = fn(fc);
                return name != null && TsFunctions.has(this, name);
            }

            /** Whether {@code name} is a word of the library ({@link #alongTime}'s). */
            private boolean function(String name) {
                return TsFunctions.has(this, name);
            }
        }

        /** A call's arguments (R10): those by place, then those by name. */
        private record Arguments(List<SqlAst.Expr> positional, Map<String, SqlAst.Expr> named, SqlAst.Pos pos) {

        }

        /** {@code fc}'s arguments: by place, then by name ({@code type => 'log'}, R10). */
        private static Arguments arguments(SqlAst.FunctionCall fc) {
            List<SqlAst.Expr> positional = new ArrayList<>();
            Map<String, SqlAst.Expr> named = new LinkedHashMap<>();
            for (SqlAst.Expr arg : fc.args()) {
                if (arg instanceof SqlAst.FunctionCall o && TsParser.OPTION.equals(fn(o))) {
                    String n = ((SqlAst.StringLit) o.args().get(0)).value();
                    if (named.put(n, o.args().get(1)) != null) {
                        throw invalid(fn(fc) + "'s option " + n + " is given twice (R10)", o.pos());
                    }
                } else if (!named.isEmpty()) {
                    throw invalid("an option by place comes before those by name: " + fn(fc) + "(x, 1, type => 'log')"
                            + " (R10)", arg.pos());
                } else {
                    positional.add(arg);
                }
            }
            return new Arguments(positional, named, fc.pos());
        }

        /** {@code e} with each span ({@code 1y}) Postgres's interval ({@code INTERVAL '1 year'}): date arithmetic in a
         *  condition or a per-point expression (R9). */
        private SqlAst.Expr intervals(SqlAst.Expr e) {
            return map(e, x -> {
                TsAst.Span s = x instanceof SqlAst.ColumnRef c ? query.spans().get(c.pos()) : null;
                if (s == null) {
                    return x;
                }
                // the span's count of a DurationUnit (a quarter three months: TsFunctions.period, the one table)
                TsFunctions.Period p = TsFunctions.period(s);
                String text = p.n() + " " + switch (p.unit()) {
                    case "DAYS" -> "day";
                    case "WEEKS" -> "week";
                    case "MONTHS" -> "month";
                    case "YEARS" -> "year";
                    case "HOURS" -> "hour";
                    case "MINUTES" -> "minute";
                    case "SECONDS" -> "second";
                    default -> throw new IllegalStateException("a span of " + p.unit());
                };
                return new SqlAst.TypedLit(new SqlAst.TypeName("interval", List.of(), s.pos()), text, s.pos());
            });
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
            /** The names a flat source's computed items take: a path read only by an item or the condition, its last
             *  step one of them, is read under a name of the translator's ({@code CAST(side AS text) AS side} reads
             *  the property side and names its own column side, as SQL means). */
            private final Set<String> taken = new java.util.TreeSet<>(String.CASE_INSENSITIVE_ORDER);

            Paths(String cls) {
                this.cls = cls;
            }

            boolean has(SqlAst.ColumnRef c) {
                return read.containsKey(key(c));
            }

            /** The names a flat source's computed items take ({@link #taken}). */
            void take(Set<String> names) {
                taken.addAll(names);
            }

            /** The column of path {@code c}, read as its last step's name -- or, that name a computed item's, a name
             *  of the translator's. */
            String add(SqlAst.ColumnRef c) {
                Path known = read.get(key(c));
                if (known != null) {
                    return known.column();
                }
                String last = c.parts().get(c.parts().size() - 1).text();
                return add(c, taken.contains(last) ? "__ts_read_" + last : last);
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
                                + " an association to one object -- read the many side as a source of its own,"
                                + " navigating to one (register S153)", step.pos());
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
    static @com.legend.base.Nullable String name(SqlAst.FunctionCall fc) {
        return fn(fc);
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
}
