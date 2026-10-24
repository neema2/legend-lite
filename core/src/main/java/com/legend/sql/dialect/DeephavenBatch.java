// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlDdl;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlUnion;
import com.legend.sql.SqlWith;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A query as Deephaven's table operations (docs/DEEPHAVEN_PLAN_2026_10_22.md DH1, DH20): the batch {@link Deephaven}
 * writes for a select, a union and their sources, each operation reading earlier ones by their place in the batch; the
 * batch's last operation is the query's result. The conventions every operation keeps:
 * <ul>
 *   <li>a select's RESULT columns are its outputs, each under its own name where Deephaven holds it as written, else
 *       {@code __c<i>} (the plan reads its columns by position);</li>
 *   <li>inside a select, each source's columns are renamed {@code <alias>__<column>}, one namespace for every alias in
 *       scope, so a join adds the right side's columns beside the left's with no clash;</li>
 *   <li>a group is one {@code aggregate} over the keys and the aggregates' arguments computed first ({@code __g<i>},
 *       {@code __a<j>}), each aggregate its column {@code __r<j>}, read back by expression;</li>
 *   <li>a sort key that may be null is preceded by its null flag ({@code __n<k>}): the platform's order puts a null
 *       last ascending and first descending, Deephaven's first and last;</li>
 *   <li>an equi-join's right keys that may be null are filtered first: SQL matches no null key, Deephaven matches
 *       nulls to each other; a LEFT join is a natural join that refuses a duplicate right key (a to-one join, DH20),
 *       an INNER join Deephaven's many-to-many join;</li>
 *   <li>a query with parameters joins the one-row parameters table into each select that reads one
 *       ({@link DeephavenJson#PARAMETERS_TICKET}).</li>
 * </ul>
 * What has no shape yet is refused by name: a right, full or lateral join, an outer join's residual, a window, a CTE,
 * a literal relation, an offset with no limit.
 */
final class DeephavenBatch {

    /** The names this writer gives its helper columns: an output named like one is renamed by its place. */
    private static final java.util.regex.Pattern HELPER = java.util.regex.Pattern.compile("__[acgnprs][0-9]+");

    private final Deephaven dialect;
    private final SqlWriter w;
    private int ops;

    DeephavenBatch(Deephaven dialect, SqlWriter writer) {
        this.dialect = dialect;
        this.w = writer;
    }

    /** A relation written so far: the operation that answers it, and its columns, each name to its Deephaven name. */
    private record Rel(int op, Map<String, String> columns) {
    }

    /** One column a view writes: its spec's text ({@code name}, or {@code name = other} for a rename), and, when the
     *  column is computed, the expression written after {@code name = }. */
    private record Spec(String text, @com.legend.base.Nullable SqlExpr expr) {
    }

    SqlWriter batch(SqlQuery q) {
        w.append("{\"ops\": [");
        query(q);
        return w.append("]}");
    }

    // ==================================================================
    // Operations
    // ==================================================================

    /** Opens the next operation; the caller writes its fields and closes it with {@link #close}. */
    private void open(String name) {
        if (ops > 0) {
            w.append(", ");
        }
        w.append("{\"" + name + "\": {");
    }

    private int close() {
        w.append("}}");
        return ops++;
    }

    private int simple(String name, String fields) {
        open(name);
        w.append(fields);
        return close();
    }

    private static String source(int op) {
        return "\"sourceId\": " + DeephavenJson.offset(op);
    }

    /** {@code view} (only the columns it names) or {@code updateView} (every column, and these): each column a formula,
     *  {@code name = formula}, or a kept column's name. */
    private int view(String name, int src, List<Spec> specs) {
        open(name);
        w.append(source(src)).append(", \"columnSpecs\": [");
        for (int i = 0; i < specs.size(); i++) {
            Spec s = specs.get(i);
            w.append(i > 0 ? ", \"" : "\"").append(s.text());
            if (s.expr() != null) {
                w.append(" = ").expr(s.expr(), 0);
            }
            w.append("\"");
        }
        w.append("]");
        return close();
    }

    /** The rows where {@code condition} is TRUE: its filter list ({@link Deephaven#filters}, what the server can read
     *  without compiling written so). */
    private int filter(int src, SqlExpr condition) {
        open("unstructuredFilter");
        w.append(source(src)).append(", \"filters\": [");
        dialect.filters(w, condition);
        w.append("]");
        return close();
    }

    private int filterText(int src, List<String> filters) {
        return simple("unstructuredFilter", source(src) + ", \"filters\": " + DeephavenJson.strings(filters));
    }

    // ==================================================================
    // Queries
    // ==================================================================

    private Rel query(SqlQuery q) {
        return switch (q) {
            case SqlSelect s -> select(s);
            case SqlUnion u -> union(u);
            case SqlWith with -> throw new DialectCapability("a WITH (common table expressions) reached Deephaven:"
                    + " no shape yet");
        };
    }

    /** A union: each branch's columns renamed to the first's, then merged; a deduplicating union keeps each row once. */
    private Rel union(SqlUnion u) {
        Rel first = query(u.branches().get(0));
        List<String> names = new ArrayList<>(first.columns().values());
        List<Integer> sources = new ArrayList<>(List.of(first.op()));
        for (SqlQuery branch : u.branches().subList(1, u.branches().size())) {
            Rel r = query(branch);
            List<String> theirs = new ArrayList<>(r.columns().values());
            if (theirs.size() != names.size()) {
                throw new IllegalStateException("a union's branches answer " + names.size() + " and "
                        + theirs.size() + " columns");
            }
            List<Spec> renames = new ArrayList<>();
            for (int i = 0; i < names.size(); i++) {
                renames.add(new Spec(names.get(i) + " = " + theirs.get(i), null));
            }
            sources.add(view("view", r.op(), renames));
        }
        StringBuilder ids = new StringBuilder("[");
        for (int i = 0; i < sources.size(); i++) {
            ids.append(i > 0 ? ", " : "").append(DeephavenJson.offset(sources.get(i)));
        }
        int merged = simple("merge", "\"sourceIds\": " + ids.append(']'));
        if (!u.all()) {
            merged = simple("selectDistinct", source(merged) + ", \"columnNames\": " + DeephavenJson.strings(names));
        }
        return new Rel(merged, first.columns());
    }

    private Rel select(SqlSelect s) {
        Deephaven.Scope scope = new Deephaven.Scope();
        int cur = source(s.from(), scope);
        dialect.pushScope(scope);
        try {
            boolean parameters = readsParameters(s);
            if (parameters) {
                cur = joinParameters(cur);
            }
            if (s.where() != null) {
                cur = filter(cur, s.where());
            }
            boolean grouped = !s.groupBy().isEmpty()
                    || s.projections().stream().anyMatch(p -> SqlAgg.aggregatesSelect(p.expr()));
            if (grouped) {
                cur = group(s, cur, scope);
                // a group keeps its keys and aggregates only: what reads a parameter after it reads it again
                if (parameters) {
                    cur = joinParameters(cur);
                }
            }
            if (s.having() != null) {
                if (!grouped) {
                    throw new IllegalStateException("a HAVING over an ungrouped select");
                }
                cur = filter(cur, s.having());
            }
            if (s.qualify() != null) {
                throw new DialectCapability("a QUALIFY (a filter over a window) reached Deephaven: no shape yet");
            }
            return project(s, cur, scope);
        } finally {
            dialect.popScope();
        }
    }

    /** Every row with the parameters table's one row beside it: a natural join on no column. */
    private int joinParameters(int cur) {
        return simple("naturalJoin", "\"leftId\": " + DeephavenJson.offset(cur) + ", \"rightId\": "
                + DeephavenJson.parametersTable() + ", \"columnsToMatch\": [], \"columnsToAdd\": []");
    }

    /** The select's outputs, distinct, sorted and limited, its helper columns dropped. */
    private Rel project(SqlSelect s, int cur, Deephaven.Scope scope) {
        Map<String, String> outputs = new LinkedHashMap<>();
        List<Spec> specs = new ArrayList<>();
        int index = 0;
        // no projection is SELECT * (a star frame): every source's columns
        List<SqlSelect.Projection> projections = s.projections().isEmpty()
                ? List.of(new SqlSelect.Projection(new SqlExpr.Star(null, com.legend.sql.SqlTyping.UNKNOWN), null, null))
                : s.projections();
        for (SqlSelect.Projection p : projections) {
            if (p.expr() instanceof SqlExpr.Star star) {
                if (scope.grouped) {
                    throw new IllegalStateException("a star over a grouped select");
                }
                for (Map.Entry<String, Map<String, String>> src : scope.sources.entrySet()) {
                    if (star.table() != null && !star.table().equals(src.getKey())) {
                        continue;
                    }
                    for (Map.Entry<String, String> col : src.getValue().entrySet()) {
                        String out = outputName(col.getKey(), index++, outputs);
                        outputs.put(col.getKey(), out);
                        specs.add(new Spec(out + " = " + col.getValue(), null));
                    }
                }
                continue;
            }
            String label = p.alias() != null ? p.alias()
                    : p.expr() instanceof SqlExpr.Column c ? c.name()
                    : p.out() != null ? p.out().name() : null;
            if (label == null) {
                throw new IllegalStateException("a projection with no name reached Deephaven: " + p.expr());
            }
            String out = outputName(label, index++, outputs);
            outputs.put(label, out);
            specs.add(new Spec(out, p.expr()));
        }
        if (specs.isEmpty()) {
            throw new IllegalStateException("a select with no column reached Deephaven");
        }
        // a sort key: an output by its name or its expression, else a hidden column computed beside the outputs
        List<String> hidden = new ArrayList<>();
        List<String[]> sorts = new ArrayList<>();
        List<Spec> flags = new ArrayList<>();
        int k = 0;
        for (SqlSelect.SortKey key : s.orderBy()) {
            String column = sortColumn(s, key, outputs);
            if (column == null) {
                if (s.distinct()) {
                    throw new IllegalStateException("a DISTINCT select sorted by an expression it does not output");
                }
                column = "__s" + k;
                hidden.add(column);
                specs.add(new Spec(column, key.expr()));
            }
            if (Deephaven.nullable(key.expr())) {
                boolean nullsFirst = key.nullOrder() != null
                        ? key.nullOrder() == SqlSelect.SortKey.NullOrder.NULLS_FIRST : !key.ascending();
                // the flag reads the key by its place, so its formula's text is the same whatever the column: the
                // server compiles it once for a place and a type, not once a column (P1: a pivot's values and every
                // level of a tree sorted by a new column paid a compile for its flag); the alias is a column renamed,
                // which compiles nothing
                String alias = "__k" + k;
                String flag = "__n" + k;
                hidden.add(alias);
                hidden.add(flag);
                flags.add(new Spec(alias + " = " + column, null));
                flags.add(new Spec(flag + " = isNull(" + alias + ")", null));
                sorts.add(new String[] {flag, nullsFirst ? "DESCENDING" : "ASCENDING"});
            }
            sorts.add(new String[] {column, key.ascending() ? "ASCENDING" : "DESCENDING"});
            k++;
        }
        scope.outputs.putAll(outputs);
        int out = view("view", cur, specs);
        if (s.distinct()) {
            out = simple("selectDistinct", source(out) + ", \"columnNames\": "
                    + DeephavenJson.strings(new ArrayList<>(outputs.values())));
        }
        if (!flags.isEmpty()) {
            out = view("updateView", out, flags);
        }
        if (!sorts.isEmpty()) {
            StringBuilder b = new StringBuilder(source(out)).append(", \"sorts\": [");
            for (int i = 0; i < sorts.size(); i++) {
                b.append(i > 0 ? ", " : "").append("{\"columnName\": ").append(DeephavenJson.string(sorts.get(i)[0]))
                        .append(", \"direction\": \"").append(sorts.get(i)[1]).append("\"}");
            }
            out = simple("sort", b.append(']').toString());
        }
        if (s.offset() != null && s.offset() > 0) {
            if (s.limit() == null) {
                throw new DialectCapability("an offset with no limit reached Deephaven: no shape yet");
            }
            out = simple("slice", source(out) + ", \"firstPositionInclusive\": \"" + s.offset()
                    + "\", \"lastPositionExclusive\": \"" + (s.offset() + s.limit()) + "\"");
        } else if (s.limit() != null) {
            out = simple("head", source(out) + ", \"numRows\": \"" + s.limit() + "\"");
        }
        if (!hidden.isEmpty()) {
            out = simple("dropColumns", source(out) + ", \"columnNames\": " + DeephavenJson.strings(hidden));
        }
        return new Rel(out, outputs);
    }

    /** {@code label}'s Deephaven name: itself where Deephaven holds it, else its place ({@code __c<i>}). */
    private static String outputName(String label, int index, Map<String, String> taken) {
        String name = DeephavenJson.holdsName(label) && !HELPER.matcher(label).matches() ? label : "__c" + index;
        if (taken.containsValue(name)) {
            throw new IllegalStateException("output " + label + " named twice (" + name + ")");
        }
        return name;
    }

    /** The output a sort key reads, by its name or its expression; null when it reads none. */
    private static @com.legend.base.Nullable String sortColumn(SqlSelect s, SqlSelect.SortKey key,
            Map<String, String> outputs) {
        if (key.outputName() != null && outputs.containsKey(key.outputName())) {
            return outputs.get(key.outputName());
        }
        if (key.expr() instanceof SqlExpr.Column c && c.table() == null && outputs.containsKey(c.name())) {
            return outputs.get(c.name());
        }
        for (SqlSelect.Projection p : s.projections()) {
            if (p.expr().equals(key.expr())) {
                String label = p.alias() != null ? p.alias()
                        : p.expr() instanceof SqlExpr.Column c ? c.name() : p.out() != null ? p.out().name() : null;
                if (label != null && outputs.containsKey(label)) {
                    return outputs.get(label);
                }
            }
        }
        return null;
    }

    // ==================================================================
    // Groups
    // ==================================================================

    /**
     * The keys and the aggregates' arguments computed, then one aggregate; its columns then read the select's keys and
     * aggregates. With no key, the one group of SQL's answer is kept even over no rows ({@code preserveEmpty}).
     *
     * A PIVOT'S CELLS are written with no formula (docs/DEEPHAVEN_PLAN_2026_10_22.md P1): a cell's count is the server's
     * count of the rows its match filters take, in the group's aggregate; its sum, average, minimum or maximum one
     * aggregation over the keys and the pivot's columns, each cell's rows taken from it by their match filters and
     * joined back by the keys -- a cell no row reaches is null, as SQL's aggregate of no rows. Measured on Deephaven 42.7
     * (2026-10-23): a pivot's conditional formulas, one for each value and measure, cost a first pivot 1.7 to 1.9 s of
     * compiling on a fresh server, and every new value another.
     */
    private int group(SqlSelect s, int cur, Deephaven.Scope scope) {
        // each SQL aggregate, or a whole expression the server holds as one aggregation (a unique value)
        List<SqlExpr> reducers = new ArrayList<>();
        for (SqlSelect.Projection p : s.projections()) {
            collect(p.expr(), reducers);
        }
        if (s.having() != null) {
            collect(s.having(), reducers);
        }
        for (SqlSelect.SortKey key : s.orderBy()) {
            collect(key.expr(), reducers);
        }
        // a key or an argument that is a column as it is read by its own name (G1: every operation is one more table
        // for the server to make and keep current); anything computed, into a column of its own first
        List<Spec> pre = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < s.groupBy().size(); i++) {
            String named = dialect.plainColumn(s.groupBy().get(i));
            if (named != null && !keys.contains(named)) {
                keys.add(named);
            } else {
                keys.add("__g" + i);
                pre.add(new Spec("__g" + i, s.groupBy().get(i)));
            }
        }
        List<@com.legend.base.Nullable Cell> cells = cells(reducers, keys);
        List<String> inputs = new ArrayList<>();
        for (int j = 0; j < reducers.size(); j++) {
            if (cells.get(j) != null) {
                inputs.add("");
                continue;
            }
            SqlExpr arg;
            // the value's order for a value at the first or last of an order (ARG_MIN, ARG_MAX: K3a), else none
            SqlExpr by = null;
            if (reducers.get(j) instanceof SqlAgg.Reducer r) {
                boolean ordered = r.fn() == SqlAgg.Fn.ARG_MIN || r.fn() == SqlAgg.Fn.ARG_MAX;
                if (r.args().size() > (ordered ? 2 : 1)) {
                    throw new DialectCapability("aggregate " + r.fn() + " over " + r.args().size()
                            + " arguments reached Deephaven: no shape yet");
                }
                arg = r.args().isEmpty() ? null : averaged(r.fn(), r.args().get(0));
                by = ordered ? r.args().get(1) : null;
            } else {
                arg = uniqueValueOf(reducers.get(j));
            }
            String input = input(arg, "__a" + j, keys, pre);
            inputs.add(input);
            if (reducers.get(j) instanceof SqlAgg.Reducer r) {
                scope.aggregates.put(r, by == null ? new String[] {"__r" + j, input}
                        : new String[] {"__r" + j, input, input(by, "__b" + j, keys, pre)});
            }
        }
        if (!pre.isEmpty()) {
            cur = view("updateView", cur, pre);
        }
        open("aggregate");
        w.append(source(cur)).append(", \"groupByColumns\": ").append(DeephavenJson.strings(keys))
                .append(", \"aggregations\": [");
        boolean first = true;
        for (int j = 0; j < reducers.size(); j++) {
            Cell cell = cells.get(j);
            if (cell != null && cell.fn() != SqlAgg.Fn.COUNT) {
                continue;
            }
            w.append(first ? "" : ", ");
            first = false;
            if (cell != null) {
                // a cell's count: the rows its match filters take (and its column's nulls off, when it counts one)
                List<String> filters = new ArrayList<>(cell.filters());
                if (cell.column() != null) {
                    filters.add(Deephaven.notNullFilter(cell.column()));
                }
                w.append("{\"countWhere\": {\"columnName\": \"__r" + j + "\", \"filters\": "
                        + DeephavenJson.strings(filters) + "}}");
            } else if (reducers.get(j) instanceof SqlAgg.Reducer r) {
                dialect.reducer(w, r);
            } else {
                w.append("{\"columns\": {\"spec\": {\"unique\": {\"nonUniqueSentinel\": {\"nullValue\": \"NULL_VALUE\"}}},"
                        + " \"matchPairs\": "
                        + DeephavenJson.strings(List.of("__r" + j + "=" + inputs.get(j))) + "}}");
            }
        }
        if (first) {
            // every aggregate a pivot's cell joined in below: the group's rows, counted only to be made
            w.append("{\"count\": {\"columnName\": \"__n\"}}");
        }
        w.append("]");
        if (keys.isEmpty()) {
            w.append(", \"preserveEmpty\": true");
        }
        int grouped = spread(cur, close(), keys, cells);
        for (int i = 0; i < s.groupBy().size(); i++) {
            scope.computed.put(s.groupBy().get(i), keys.get(i));
        }
        for (int j = 0; j < reducers.size(); j++) {
            scope.computed.put(reducers.get(j), "__r" + j);
        }
        scope.grouped = true;
        return grouped;
    }

    /** The column an aggregate reads {@code arg} from: a column as it is by its own name, anything computed (or a
     *  group key, which is read back as a key, never aggregated) into {@code otherwise} first. */
    private String input(@com.legend.base.Nullable SqlExpr arg, String otherwise, List<String> keys, List<Spec> pre) {
        String named = arg == null ? null : dialect.plainColumn(arg);
        String input = named != null && !keys.contains(named) ? named : otherwise;
        if (arg != null && !input.equals(named)) {
            pre.add(new Spec(input, arg));
        }
        return input;
    }

    /**
     * A pivot's cell (P1): an aggregate over the rows its pivot columns' match filters take -- DataCube's
     * {@code agg(x | if(cond, | v, | []), y | fn(y))}, lowered {@code fn(CASE WHEN cond THEN v ELSE NULL END)} -- of
     * {@code column} (null: a count of the rows), the pivot columns by name and their filters in the same order.
     */
    private record Cell(SqlAgg.Fn fn, @com.legend.base.Nullable String column, List<String> on, List<String> filters) {
    }

    /** Each reducer's cell, or null when it is no cell's; the cells other than counts all over the same pivot
     *  columns, none of them a key, or none of them a cell (one aggregation over the keys and those columns). */
    private List<@com.legend.base.Nullable Cell> cells(List<SqlExpr> reducers, List<String> keys) {
        List<@com.legend.base.Nullable Cell> cells = new ArrayList<>();
        List<String> on = null;
        boolean spread = true;
        for (SqlExpr e : reducers) {
            Cell cell = cellOf(e);
            cells.add(cell);
            if (cell == null || cell.fn() == SqlAgg.Fn.COUNT) {
                continue;
            }
            if (on == null) {
                on = cell.on();
            }
            spread = spread && on.equals(cell.on()) && java.util.Collections.disjoint(keys, cell.on());
        }
        if (!spread) {
            for (int j = 0; j < cells.size(); j++) {
                Cell cell = cells.get(j);
                if (cell != null && cell.fn() != SqlAgg.Fn.COUNT) {
                    cells.set(j, null);
                }
            }
        }
        return cells;
    }

    /** {@code e}'s cell, or null when it is any other aggregate. */
    private @com.legend.base.Nullable Cell cellOf(SqlExpr e) {
        // an average's promotion (`avg(1.0 * x)`) is the server's own: the cell is its argument's
        if (!(e instanceof SqlAgg.Reducer r) || r.args().size() != 1 || r.distinct() || !r.orderBy().isEmpty()
                || !(averaged(r.fn(), r.args().get(0)) instanceof SqlExpr.Case c) || c.whens().size() != 1
                || !(c.otherwise() instanceof SqlExpr.NullLit)) {
            return null;
        }
        java.util.SortedMap<String, String> cond = dialect.pivotCell(c.whens().get(0).condition());
        if (cond == null) {
            return null;
        }
        List<String> on = List.copyOf(cond.keySet());
        List<String> filters = cond.entrySet().stream().map(x -> x.getKey() + " == " + x.getValue()).toList();
        SqlExpr then = averaged(r.fn(), c.whens().get(0).then());
        String column = dialect.plainColumn(then);
        return switch (r.fn()) {
            // a count of a value never null counts the rows
            case COUNT -> then instanceof SqlExpr.IntLit || column != null && !Deephaven.nullable(then)
                    ? new Cell(SqlAgg.Fn.COUNT, null, on, filters)
                    : column != null ? new Cell(SqlAgg.Fn.COUNT, column, on, filters) : null;
            case SUM, AVG, MIN, MAX -> column == null ? null : new Cell(r.fn(), column, on, filters);
            default -> null;
        };
    }

    /** The pivot's cells other than counts (`cells`), joined onto the group {@code grouped} by its keys: one aggregation
     *  of {@code source} over the keys and the pivot's columns, each cell's rows taken from it by their match filters. */
    private int spread(int source, int grouped, List<String> keys, List<@com.legend.base.Nullable Cell> cells) {
        Map<String, String> aggregated = new LinkedHashMap<>();
        List<String> specs = new ArrayList<>();
        Map<List<String>, List<String>> joined = new LinkedHashMap<>();
        List<String> on = null;
        for (int j = 0; j < cells.size(); j++) {
            Cell cell = cells.get(j);
            if (cell == null || cell.fn() == SqlAgg.Fn.COUNT) {
                continue;
            }
            on = cell.on();
            String key = cell.fn() + " " + cell.column();
            String name = aggregated.get(key);
            if (name == null) {
                name = "__l" + aggregated.size();
                aggregated.put(key, name);
                String spec = switch (cell.fn()) {
                    case SUM -> "{\"sum\": {}}";
                    case AVG -> "{\"avg\": {}}";
                    case MIN -> "{\"min\": {}}";
                    case MAX -> "{\"max\": {}}";
                    default -> throw new IllegalStateException("a pivot's cell of " + cell.fn());
                };
                specs.add("{\"columns\": {\"spec\": " + spec + ", \"matchPairs\": "
                        + DeephavenJson.strings(List.of(name + "=" + cell.column())) + "}}");
            }
            joined.computeIfAbsent(cell.filters(), f -> new ArrayList<>()).add("__r" + j + "=" + name);
        }
        if (on == null) {
            return grouped;
        }
        List<String> by = new ArrayList<>(keys);
        by.addAll(on);
        int values = simple("aggregate", source(source) + ", \"groupByColumns\": " + DeephavenJson.strings(by)
                + ", \"aggregations\": [" + String.join(", ", specs) + "]");
        for (Map.Entry<List<String>, List<String>> cell : joined.entrySet()) {
            int taken = filterText(values, cell.getKey());
            grouped = simple("naturalJoin", "\"leftId\": " + DeephavenJson.offset(grouped) + ", \"rightId\": "
                    + DeephavenJson.offset(taken) + ", \"columnsToMatch\": " + DeephavenJson.strings(keys)
                    + ", \"columnsToAdd\": " + DeephavenJson.strings(cell.getValue()));
        }
        return grouped;
    }

    /** Every aggregate of {@code e}, once each, in order; a window's or a lambda's are its own. A unique value is one
     *  aggregate, not its parts. */
    private static void collect(SqlExpr e, List<SqlExpr> into) {
        if (e instanceof SqlAgg.Reducer || uniqueValueOf(e) != null) {
            if (!into.contains(e)) {
                into.add(e);
            }
            return;
        }
        if (e instanceof SqlExpr.WindowCall || e instanceof SqlExpr.Lambda) {
            return;
        }
        for (SqlExpr c : e.children()) {
            collect(c, into);
        }
    }

    /**
     * An average's argument as Deephaven averages it: its own average is a fraction of any primitive number, so the
     * lowering's promotion -- {@code avg(1.0 * x)}, the engine's on every database, whose own avg of an integer column
     * is an integer on some (H2) -- is the server's already, and the column is read as it is (G1: one formula fewer for
     * the server to parse). Any other argument as the lowering wrote it.
     */
    private static SqlExpr averaged(SqlAgg.Fn fn, SqlExpr arg) {
        if (fn == SqlAgg.Fn.AVG && arg instanceof SqlExpr.Call times && times.fn() == com.legend.sql.SqlFn.TIMES
                && times.args().size() == 2 && times.args().get(0) instanceof SqlExpr.FloatLit one && one.value() == 1.0
                && times.args().get(1).type() instanceof com.legend.sql.TypeFact.Typed t
                && (t.type() == com.legend.sql.SqlType.Scalar.INTEGER || t.type() == com.legend.sql.SqlType.Scalar.BIGINT
                        || t.type() == com.legend.sql.SqlType.Scalar.DOUBLE)) {
            return times.args().get(1);
        }
        return arg;
    }

    /**
     * The value {@code e} reads when it is the lowering's UNIQUE VALUE of a group with no default --
     * {@code CASE WHEN COUNT(DISTINCT v) = 1 THEN MAX(v) ELSE NULL END}, DataCube's aggregate of a column a level does not
     * group by -- which is Deephaven's own {@code unique} aggregation over {@code v}: the one value its rows hold, nulls
     * aside, else null (docs/DEEPHAVEN_PLAN_2026_10_22.md G1: measured, the two aggregations and the formula reading
     * them were most of a level's making). Null when {@code e} is any other expression; a default other than null is
     * not the server's (a group of no values answers it), so it is written as it was lowered.
     */
    static @com.legend.base.Nullable SqlExpr uniqueValueOf(SqlExpr e) {
        if (!(e instanceof SqlExpr.Case c) || c.whens().size() != 1 || !(c.otherwise() instanceof SqlExpr.NullLit)) {
            return null;
        }
        SqlExpr.Case.When when = c.whens().get(0);
        if (!(when.condition() instanceof SqlExpr.Call eq) || eq.fn() != com.legend.sql.SqlFn.EQUAL
                || !(eq.args().get(0) instanceof SqlAgg.Reducer count) || count.fn() != SqlAgg.Fn.COUNT
                || !count.distinct() || !count.orderBy().isEmpty() || count.args().size() != 1
                || !(eq.args().get(1) instanceof SqlExpr.IntLit one) || one.value() != 1
                || !(when.then() instanceof SqlAgg.Reducer max) || max.fn() != SqlAgg.Fn.MAX || max.distinct()
                || !max.orderBy().isEmpty() || !max.args().equals(count.args())) {
            return null;
        }
        return max.args().get(0);
    }

    // ==================================================================
    // Sources
    // ==================================================================

    /** {@code src}'s rows, its columns renamed into the select's namespace ({@link Deephaven.Scope#sources}). */
    private int source(SqlSource src, Deephaven.Scope scope) {
        return switch (src) {
            case SqlSource.Table t -> table(t, scope);
            case SqlSource.Subselect sub -> subselect(sub, scope);
            case SqlSource.Join j -> join(j, scope);
            case SqlSource.Dual d -> simple("emptyTable", "\"size\": \"1\"");
            case SqlSource.Values v -> throw new DialectCapability("a literal relation reached Deephaven: no shape"
                    + " yet");
            case SqlSource.Cte c -> throw new DialectCapability("a CTE reference reached Deephaven: no shape yet");
            case SqlSource.VarSetPlaceholder v -> throw new IllegalStateException("a plan variable (" + v.varName()
                    + ") reached an executable dialect: plan-text vocabulary only");
            case SqlSource.RawSql r -> throw new DialectCapability("a raw statement as a relation reached Deephaven,"
                    + " which runs no SQL text");
            case SqlSource.Catalog c -> throw new DialectCapability("a catalog grid reached Deephaven: its catalog is"
                    + " not read as a grid yet");
            case SqlSource.SourceUrl u -> throw new DialectCapability("sourceUrl reached Deephaven: no external"
                    + " source");
            case SqlSource.Pivot p -> throw new DialectCapability("a native pivot reached Deephaven: a pivot is a"
                    + " group of conditional aggregates there, not shaped from this node yet");
        };
    }

    /** A server's table, by its name (the default schema's: DH18), each column renamed under the alias. */
    private int table(SqlSource.Table t, Deephaven.Scope scope) {
        if (t.call()) {
            throw new DialectCapability("a tabular function (" + t.name() + ") reached Deephaven, which has none");
        }
        if (t.name().indexOf('.') >= 0) {
            throw new DialectCapability("a schema-qualified table (" + t.name() + ") reached Deephaven: a server has"
                    + " one namespace of tables, and a named schema's naming there is not decided (DH18)");
        }
        String table = DeephavenJson.name(t.name(), "a table");
        Map<String, String> columns = new LinkedHashMap<>();
        List<Spec> renames = new ArrayList<>();
        for (OutputCol c : t.outputs()) {
            String name = DeephavenJson.name(c.name(), "column " + t.name() + "." + c.name());
            String renamed = alias(t.alias()) + "__" + name;
            columns.put(c.name(), renamed);
            renames.add(new Spec(renamed + " = " + name, null));
        }
        scope.sources.put(t.alias(), columns);
        open("view");
        w.append("\"sourceId\": " + DeephavenJson.scopeTable(table) + ", \"columnSpecs\": [");
        for (int i = 0; i < renames.size(); i++) {
            w.append(i > 0 ? ", " : "").append(DeephavenJson.string(renames.get(i).text()));
        }
        w.append("]");
        return close();
    }

    private static String alias(String alias) {
        return DeephavenJson.name(alias, "an alias");
    }

    /** A subselect's rows, its outputs renamed under its alias. */
    private int subselect(SqlSource.Subselect sub, Deephaven.Scope scope) {
        Rel inner = query(sub.inner());
        Map<String, String> columns = new LinkedHashMap<>();
        List<Spec> renames = new ArrayList<>();
        for (Map.Entry<String, String> c : inner.columns().entrySet()) {
            String renamed = alias(sub.alias()) + "__" + c.getValue();
            columns.put(c.getKey(), renamed);
            renames.add(new Spec(renamed + " = " + c.getValue(), null));
        }
        scope.sources.put(sub.alias(), columns);
        return view("view", inner.op(), renames);
    }

    /** An equi-join: INNER as Deephaven's join, LEFT as a natural join that refuses a duplicate right key (DH20), CROSS
     *  as a join on no column; a right key that may be null filtered out first, as SQL matches no null. */
    private int join(SqlSource.Join j, Deephaven.Scope scope) {
        int left = source(j.left(), scope);
        Map<String, Map<String, String>> before = new LinkedHashMap<>(scope.sources);
        int right = source(j.right(), scope);
        List<String> rightColumns = new ArrayList<>();
        for (Map.Entry<String, Map<String, String>> e : scope.sources.entrySet()) {
            if (!before.containsKey(e.getKey())) {
                rightColumns.addAll(e.getValue().values());
            }
        }
        if (j.kind() == SqlSource.Join.Kind.CROSS) {
            return joinOp("crossJoin", left, right, List.of(), rightColumns, false);
        }
        if (j.kind() != SqlSource.Join.Kind.INNER && j.kind() != SqlSource.Join.Kind.LEFT) {
            throw new DialectCapability("a " + j.kind() + " join reached Deephaven: no shape yet (DH20)");
        }
        List<String> matches = new ArrayList<>();
        List<String> rightNullFilters = new ArrayList<>();
        List<SqlExpr> residual = new ArrayList<>();
        dialect.pushScope(scope);
        try {
            for (SqlExpr conjunct : conjuncts(j.on())) {
                String[] pair = keyPair(conjunct, before, scope);
                if (pair == null) {
                    residual.add(conjunct);
                    continue;
                }
                matches.add(pair[0] + "=" + pair[1]);
                if (pair[2] != null) {
                    rightNullFilters.add(Deephaven.notNullFilter(pair[1]));
                }
            }
        } finally {
            dialect.popScope();
        }
        if (matches.isEmpty()) {
            throw new DialectCapability("a join with no equality between its sides reached Deephaven: no shape yet"
                    + " (DH20)");
        }
        if (!rightNullFilters.isEmpty()) {
            right = filterText(right, rightNullFilters);
        }
        boolean left_ = j.kind() == SqlSource.Join.Kind.LEFT;
        if (left_ && !residual.isEmpty()) {
            throw new DialectCapability("a LEFT join with a condition beyond its key equalities reached Deephaven: no"
                    + " shape yet (DH20)");
        }
        int joined = joinOp(left_ ? "naturalJoin" : "crossJoin", left, right, matches, rightColumns, left_);
        if (!residual.isEmpty()) {
            dialect.pushScope(scope);
            try {
                SqlExpr on = residual.size() == 1 ? residual.get(0)
                        : new SqlExpr.Call(SqlFn.AND, residual, residual.get(0).type());
                joined = filter(joined, on);
            } finally {
                dialect.popScope();
            }
        }
        return joined;
    }

    private int joinOp(String name, int left, int right, List<String> matches, List<String> add, boolean toOne) {
        return simple(name, "\"leftId\": " + DeephavenJson.offset(left) + ", \"rightId\": " + DeephavenJson.offset(right)
                + ", \"columnsToMatch\": " + DeephavenJson.strings(matches) + ", \"columnsToAdd\": "
                + DeephavenJson.strings(add) + (toOne ? ", \"joinType\": \"ERROR_ON_DUPLICATE\"" : ""));
    }

    private static List<SqlExpr> conjuncts(@com.legend.base.Nullable SqlExpr on) {
        List<SqlExpr> out = new ArrayList<>();
        if (on == null) {
            return out;
        }
        SqlExpr e = on instanceof SqlExpr.Group g ? g.inner() : on;
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.AND) {
            for (SqlExpr a : c.args()) {
                out.addAll(conjuncts(a));
            }
        } else {
            out.add(e);
        }
        return out;
    }

    /** {@code left = right} between a column of each side: their Deephaven names, left's first, and whether the right
     *  key may be null; null when the conjunct is no such equality. */
    private static String @com.legend.base.Nullable [] keyPair(SqlExpr conjunct,
            Map<String, Map<String, String>> leftSources, Deephaven.Scope scope) {
        if (!(conjunct instanceof SqlExpr.Call c) || c.fn() != SqlFn.EQUAL
                || !(c.args().get(0) instanceof SqlExpr.Column a) || !(c.args().get(1) instanceof SqlExpr.Column b)
                || a.table() == null || b.table() == null) {
            return null;
        }
        boolean aLeft = leftSources.containsKey(a.table());
        boolean bLeft = leftSources.containsKey(b.table());
        if (aLeft == bLeft) {
            return null;
        }
        SqlExpr.Column l = aLeft ? a : b;
        SqlExpr.Column r = aLeft ? b : a;
        Map<String, String> ls = scope.sources.get(l.table());
        Map<String, String> rs = scope.sources.get(r.table());
        String ln = ls == null ? null : ls.get(l.name());
        String rn = rs == null ? null : rs.get(r.name());
        if (ln == null || rn == null) {
            return null;
        }
        return new String[] {ln, rn, Deephaven.nullable(r) ? "nullable" : null};
    }

    // ==================================================================
    // Parameters
    // ==================================================================

    /** Whether the select's own expressions (not its sources') read a plan parameter. */
    private static boolean readsParameters(SqlSelect s) {
        List<SqlExpr> own = new ArrayList<>();
        for (SqlSelect.Projection p : s.projections()) {
            own.add(p.expr());
        }
        if (s.where() != null) {
            own.add(s.where());
        }
        own.addAll(s.groupBy());
        if (s.having() != null) {
            own.add(s.having());
        }
        for (SqlSelect.SortKey k : s.orderBy()) {
            own.add(k.expr());
        }
        for (SqlExpr e : own) {
            if (readsParameter(e)) {
                return true;
            }
        }
        return false;
    }

    private static boolean readsParameter(SqlExpr e) {
        if (e instanceof SqlExpr.PlanParam) {
            return true;
        }
        for (SqlExpr c : e.children()) {
            if (readsParameter(c)) {
                return true;
            }
        }
        return false;
    }

    // ==================================================================
    // DDL: a table is an input table (DH15)
    // ==================================================================

    /**
     * A table's DDL: {@code {"createTable": name, "batch": ...}} whose batch's last operation is the input table the
     * connector publishes under the name -- keyed by the declared primary key, or append-only with none -- over an
     * empty table of the declared columns' types; {@code {"dropTable": name}}. A column type no Deephaven column holds,
     * a named schema and a temporary table are refused by name.
     */
    static String ddl(SqlDdl ddl) {
        return switch (ddl) {
            case SqlDdl.CreateTable t -> createTable(t);
            case SqlDdl.DropTable t -> "{\"dropTable\": " + DeephavenJson.string(tableName(t.schema(), t.table()))
                    + "}";
            case SqlDdl.CreateSchema s -> throw new DialectCapability("a schema (" + s.schema() + ") reached"
                    + " Deephaven: a server has one namespace of tables (DH18)");
            case SqlDdl.DropSchema s -> throw new DialectCapability("a schema (" + s.schema() + ") reached"
                    + " Deephaven: a server has one namespace of tables (DH18)");
        };
    }

    private static String tableName(@com.legend.base.Nullable String schema, String table) {
        if (schema != null && !"default".equals(schema)) {
            throw new DialectCapability("a table in schema " + schema + " reached Deephaven: a server has one"
                    + " namespace of tables, and a named schema's naming there is not decided (DH18)");
        }
        return DeephavenJson.name(table, "a table");
    }

    private static String createTable(SqlDdl.CreateTable t) {
        if (t.temporary()) {
            throw new DialectCapability("a temporary table (" + t.table() + ") reached Deephaven, which has none");
        }
        String name = tableName(t.schema(), t.table());
        List<String> columns = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        for (SqlDdl.Column c : t.columns()) {
            String column = DeephavenJson.name(c.name(), "column " + t.table() + "." + c.name());
            columns.add(column + " = " + nullOf(c.type(), t.table() + "." + c.name()));
            if (c.primaryKey()) {
                keys.add(column);
            }
        }
        String kind = keys.isEmpty() ? "{\"inMemoryAppendOnly\": {}}"
                : "{\"inMemoryKeyBacked\": {\"keyColumns\": " + DeephavenJson.strings(keys) + "}}";
        return "{\"createTable\": " + DeephavenJson.string(name) + ", \"batch\": {\"ops\": ["
                + "{\"emptyTable\": {\"size\": \"0\"}}, "
                + "{\"update\": {\"sourceId\": " + DeephavenJson.offset(0) + ", \"columnSpecs\": "
                + DeephavenJson.strings(columns) + "}}, "
                + "{\"createInputTable\": {\"sourceTableId\": " + DeephavenJson.offset(1) + ", \"kind\": " + kind + "}}"
                + "]}}";
    }

    /** The typed null a column of {@code type} starts from, which gives the input table its column's type
     *  (docs/DEEPHAVEN_PLAN_2026_10_22.md §3's table); a type no Deephaven column holds refused by name. */
    private static String nullOf(SqlDdl.ColumnType type, String column) {
        return switch (type) {
            case SqlDdl.ColumnType.Plain p -> switch (p.kind()) {
                case BIT -> "(Boolean) null";
                case TINYINT -> "NULL_BYTE";
                case SMALLINT -> "NULL_SHORT";
                case INTEGER -> "NULL_INT";
                case BIGINT -> "NULL_LONG";
                case REAL -> "NULL_FLOAT";
                case FLOAT, DOUBLE -> "NULL_DOUBLE";
                case DATE -> "(java.time.LocalDate) null";
                case TIME -> "(java.time.LocalTime) null";
                case TIMESTAMP -> "(java.time.Instant) null";
                case VARCHAR -> "(String) null";
                case TIMESTAMP_NS, JSON, OTHER, DISTINCT, ARRAY, OBJECT -> throw new DialectCapability("column " + column
                        + " is " + p.kind() + ", which no Deephaven column holds yet");
            };
            case SqlDdl.ColumnType.Sized s -> switch (s.kind()) {
                case "VARCHAR", "CHAR" -> "(String) null";
                default -> throw new DialectCapability("column " + column + " is " + s.kind() + "(" + s.size()
                        + "), which no Deephaven column holds yet");
            };
            case SqlDdl.ColumnType.Scaled s -> switch (s.kind()) {
                case "DECIMAL", "NUMERIC" -> "(java.math.BigDecimal) null";
                default -> throw new DialectCapability("column " + column + " is " + s.kind() + ", which no"
                        + " Deephaven column holds yet");
            };
        };
    }
}
