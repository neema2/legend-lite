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
    private static final java.util.regex.Pattern HELPER = java.util.regex.Pattern.compile("__[a-z][0-9]+");

    private final Deephaven dialect;
    private final SqlWriter w;
    private int ops;
    /** The helper columns a select's windows read and make ({@code __x<n>}, {@code __w<n>}): numbered across the batch. */
    private int windowColumns;
    /** Each store table the batch reads, as its store declares it (DH16): written after the operations, for the sender
     *  to check the server's table against ({@code DeephavenWire.made}). */
    private final List<String> declared = new ArrayList<>();

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
        w.append("]");
        if (!declared.isEmpty()) {
            w.append(", \"declared\": [" + String.join(", ", declared) + "]");
        }
        return w.append("}");
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

    /**
     * {@code view} (only the columns it names) or {@code updateView} (every column, and these): each column a formula,
     * {@code name = formula}, or a kept column's name.
     *
     * <p>A COLUMN THE VIEW HAS COMPUTED IS READ BY ITS NAME in its later columns (the server evaluates a view's columns in
     * order, a later one reading an earlier one): a select's computed columns are each its whole expression, an extend's
     * column written into every later one that reads it, so a chain of them grew in each formula as it went -- the
     * trader screens' Black and Scholes (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K11): a time to expiry written into
     * d1, d1 into d2, both into a price, measured at 30,000 characters and 109 s for the server to compile one price.
     * Named only within this view, so nothing outside it reads a column it may not have.
     */
    private int view(String name, int src, List<Spec> specs) {
        open(name);
        w.append(source(src)).append(", \"columnSpecs\": [");
        java.util.Map<SqlExpr, String> computed = dialect.computedOrNull();
        java.util.Map<String, String> copies = dialect.copiesOrNull();
        List<SqlExpr> named = new ArrayList<>();
        List<String> copied = new ArrayList<>();
        for (int i = 0; i < specs.size(); i++) {
            Spec s = specs.get(i);
            w.append(i > 0 ? ", \"" : "\"").append(s.text());
            if (s.expr() == null && copies != null) {
                // a source column copied by its name (a star's `d1 = t8__d1`): a later formula of this view reads the
                // copy (Deephaven.Scope#copies), its text the query's own, whatever the source's alias
                int eq = s.text().indexOf(" = ");
                String from = eq < 0 ? null : s.text().substring(eq + 3);
                if (from != null && DeephavenJson.holdsName(from) && !copies.containsKey(from)) {
                    copies.put(from, s.text().substring(0, eq));
                    copied.add(from);
                }
            }
            if (s.expr() != null) {
                w.append(" = ").expr(s.expr(), 0);
                if (computed != null && worthNaming(s.expr())) {
                    for (SqlExpr form : forms(s.expr())) {
                        if (!computed.containsKey(form)) {
                            computed.put(form, s.text());
                            named.add(form);
                        }
                    }
                }
            }
            w.append("\"");
        }
        if (computed != null) {
            named.forEach(computed::remove);
        }
        if (copies != null) {
            copied.forEach(copies::remove);
        }
        w.append("]");
        return close();
    }

    /** An expression worth reading by its column's name: one that computes, not a column or a value as it is. */
    private static boolean worthNaming(SqlExpr e) {
        return e instanceof SqlExpr.Call || e instanceof SqlExpr.Case || e instanceof SqlExpr.Cast
                || e instanceof SqlExpr.Group g && worthNaming(g.inner());
    }

    /** The forms an expression is read in elsewhere: as it is, and without its parentheses. */
    private static List<SqlExpr> forms(SqlExpr e) {
        return e instanceof SqlExpr.Group g ? List.of(e, g.inner()) : List.of(e);
    }

    // ==================================================================
    // Shared subtrees: computed once
    // ==================================================================

    /** A formula's shared subtrees worth a helper: of 5 nodes or more -- the server compiles every formula, its time
     *  growing faster than its length, so even a small subtree written twice costs more than a column. */
    private static final SharedSubtrees FORMULA_SUBTREES = new SharedSubtrees(5);

    /**
     * THE SUBTREES A VIEW'S COLUMNS SHARE, EACH COMPUTED ONCE ({@link SharedSubtrees}: which ones, and why). A view
     * already reads a column it has computed by its name (#view); this computes, as a helper column of its own
     * ({@code __e<i>}), each subtree written more than once among the view's columns, before the first column that
     * reads it, so that column and every later one read it by its name -- the options desk's formulas a thousand
     * characters and more each without it, which the server parses in time that grows faster than their length
     * (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K11: seconds of the options desk's first show).
     *
     * @param specs   a view's columns, in order
     * @param helpers the helpers' names, which the caller drops after the view
     * @return the columns with the helpers among them
     */
    private static List<Spec> withShared(List<Spec> specs, List<String> helpers) {
        List<SqlExpr> exprs = new ArrayList<>();
        for (Spec spec : specs) {
            if (spec.expr() != null) {
                exprs.add(spec.expr());
            }
        }
        // smallest first, so a helper reads the helpers inside it by their names
        List<SqlExpr> shared = FORMULA_SUBTREES.shared(exprs);
        if (shared.isEmpty()) {
            return specs;
        }
        List<Spec> out = new ArrayList<>();
        java.util.Set<SqlExpr> named = new java.util.HashSet<>();
        for (Spec spec : specs) {
            if (spec.expr() != null) {
                SqlExpr whole = SharedSubtrees.bare(spec.expr());
                java.util.Set<SqlExpr> within = FORMULA_SUBTREES.within(spec.expr());
                for (SqlExpr h : shared) {
                    // a column before this one computes it, or this column is it: read by that column's name
                    if (within.contains(h) && !named.contains(h) && !h.equals(whole)) {
                        String name = "__e" + helpers.size();
                        helpers.add(name);
                        out.add(new Spec(name, h));
                        named.add(h);
                    }
                }
                named.add(whole);
            }
            out.add(spec);
        }
        return out;
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
            cur = windows(s, cur, scope);
            // a filter over a window (a top-N of each group): its rows, the windows computed
            if (s.qualify() != null) {
                cur = filter(cur, s.qualify());
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
        // the subtrees its columns share, each computed once (#withShared); a DISTINCT keeps its outputs only
        List<String> helpers = new ArrayList<>();
        specs = withShared(specs, helpers);
        if (!s.distinct()) {
            hidden.addAll(helpers);
        }
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
    // Windows (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K6)
    // ==================================================================

    /**
     * EACH WINDOW OF THE SELECT'S OUTPUTS AND ITS QUALIFY, AS COLUMNS BESIDE ITS ROWS: computed after the rows are
     * filtered and grouped and before the QUALIFY and the outputs read them (SQL's order), each into a column of its own that the outputs then read by name
     * ({@link Deephaven.Scope#computed}). Windows over a whole partition are the partition's aggregates joined back;
     * windows over an order are the rows sorted by it (its nulls where the platform's order puts them) and the server's
     * {@code updateBy} grouped by the partition: running, over a window of rows, or over a window of time. Windows over
     * the same partition and order share their aggregate, or their sort and their {@code updateBy}: a screen's running
     * figures, moving averages and previous prints along one order are one pass of the server's.
     */
    private int windows(SqlSelect s, int cur, Deephaven.Scope scope) {
        List<SqlExpr.WindowCall> calls = new ArrayList<>();
        for (SqlSelect.Projection p : s.projections()) {
            collectWindows(p.expr(), calls);
        }
        for (SqlSelect.SortKey key : s.orderBy()) {
            collectWindows(key.expr(), calls);
        }
        if (s.qualify() != null) {
            collectWindows(s.qualify(), calls);
        }
        Map<List<Object>, List<SqlExpr.WindowCall>> sets = new LinkedHashMap<>();
        for (SqlExpr.WindowCall call : calls) {
            if (scope.computed.containsKey(call) || sets.values().stream().anyMatch(set -> set.contains(call))) {
                continue;
            }
            WindowFn kind = windowFn(call.fn().fn());
            if (call.fn() instanceof SqlAgg.Reducer r && (r.distinct() || !r.orderBy().isEmpty())) {
                throw new DialectCapability("a distinct or ordered aggregate (" + r.fn() + ") over a window reached"
                        + " Deephaven: no shape yet");
            }
            // a figure of the partition's values is the same in any order; a row's place, its neighbour, and the first
            // and the last value are the order's
            boolean ordering = switch (kind) {
                case SUM, MIN, MAX, AVG, COUNT -> false;
                case ROW_NUMBER, SHIFT, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, FIRST, LAST, EMA -> true;
            };
            if (ordering && call.orderBy().isEmpty()) {
                throw new DialectCapability("a window function (" + call.fn().fn() + ") with no order reached Deephaven:"
                        + " no shape yet");
            }
            SqlExpr.WindowCall.Frame frame = call.frame();
            boolean whole = !ordering && (call.orderBy().isEmpty() || frame != null && unbounded(frame));
            if (call.orderBy().isEmpty() && frame != null && !unbounded(frame)) {
                throw new DialectCapability("a window frame with no order reached Deephaven: no shape yet");
            }
            sets.computeIfAbsent(List.of(call.partitionBy(), whole ? List.of() : call.orderBy(), whole),
                    k -> new ArrayList<>()).add(call);
        }
        for (Map.Entry<List<Object>, List<SqlExpr.WindowCall>> set : sets.entrySet()) {
            cur = (Boolean) set.getKey().get(2) ? wholePartition(set.getValue(), cur, scope)
                    : ordered(set.getValue(), cur, scope);
        }
        return cur;
    }

    private static void collectWindows(SqlExpr e, List<SqlExpr.WindowCall> out) {
        if (e instanceof SqlExpr.WindowCall call) {
            out.add(call);
            return;
        }
        for (SqlExpr c : e.children()) {
            collectWindows(c, out);
        }
    }

    /** What a window function is on the server: an aggregation's figure, a row's place, a row's neighbour's value, or
     *  the first or the last value of its frame. */
    private enum WindowFn { SUM, MIN, MAX, AVG, COUNT, ROW_NUMBER, SHIFT, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE,
        FIRST, LAST, EMA }

    private static WindowFn windowFn(SqlAgg.Fn fn) {
        return switch (fn) {
            case SUM -> WindowFn.SUM;
            case MIN -> WindowFn.MIN;
            case MAX -> WindowFn.MAX;
            case AVG -> WindowFn.AVG;
            case COUNT -> WindowFn.COUNT;
            case ROW_NUMBER -> WindowFn.ROW_NUMBER;
            case LAG, LEAD -> WindowFn.SHIFT;
            case RANK -> WindowFn.RANK;
            case DENSE_RANK -> WindowFn.DENSE_RANK;
            case PERCENT_RANK -> WindowFn.PERCENT_RANK;
            case CUME_DIST -> WindowFn.CUME_DIST;
            case NTILE -> WindowFn.NTILE;
            case FIRST_VALUE -> WindowFn.FIRST;
            case LAST_VALUE -> WindowFn.LAST;
            case EMA -> WindowFn.EMA;
            case ANY_VALUE, STDDEV_SAMP, STDDEV_POP, VAR_SAMP, VAR_POP, MEDIAN, MODE, STRING_AGG, LIST, QUANTILE_CONT,
                 QUANTILE_DISC, CORR, COVAR_SAMP, COVAR_POP, ARG_MAX, ARG_MIN, BOOL_AND, BOOL_OR, VARIANCE, STDDEV,
                 NTH_VALUE ->
                    throw new DialectCapability("a window function (" + fn + ") reached Deephaven: no shape yet");
            case WAVG, HASH_LIST, IS_DISTINCT_MARK, UNIQUE_VALUE_ONLY ->
                    throw new IllegalStateException("the lowering's marker " + fn + " reached Deephaven in a window");
        };
    }

    /** A fresh helper column's name. */
    private String helper(String kind) {
        return "__" + kind + windowColumns++;
    }

    /** {@code e} as a column of the rows: a column as it is by its name, else computed into a helper column. */
    private String columnOf(SqlExpr e, List<Spec> pre) {
        String named = dialect.plainColumn(e);
        if (named != null) {
            return named;
        }
        String column = helper("x");
        pre.add(new Spec(column, e));
        return column;
    }

    /** One window's function and argument: its kind, its value's column (null for none) and whether it may be null. */
    private record Input(WindowFn kind, SqlAgg.Fn fn, @com.legend.base.Nullable String value, boolean nullable) {
        String arg() {
            if (value == null) {
                throw new IllegalStateException("a window " + fn + " with no argument reached Deephaven");
            }
            return value;
        }
    }

    private Input input(SqlExpr.WindowCall call, List<Spec> pre) {
        SqlAgg.Fn fn = call.fn().fn();
        // an NTILE's one argument is its number of buckets, not a value of the rows
        List<SqlExpr> args = fn == SqlAgg.Fn.NTILE ? List.of() : call.fn().args();
        return new Input(windowFn(fn), fn, args.isEmpty() ? null : columnOf(averaged(fn, args.get(0)), pre),
                !args.isEmpty() && Deephaven.nullable(args.get(0)));
    }

    /** Windows over their whole partition: the partition's aggregates, one operation, joined back to each of its rows
     *  by the partition's keys. */
    private int wholePartition(List<SqlExpr.WindowCall> calls, int cur, Deephaven.Scope scope) {
        List<Spec> pre = new ArrayList<>();
        List<String> partition = new ArrayList<>();
        for (SqlExpr p : calls.get(0).partitionBy()) {
            partition.add(columnOf(p, pre));
        }
        List<String> aggregations = new ArrayList<>();
        List<String> outs = new ArrayList<>();
        Map<SqlExpr.WindowCall, String> made = new LinkedHashMap<>();
        for (SqlExpr.WindowCall call : calls) {
            Input in = input(call, pre);
            String out = helper("w");
            aggregations.add(switch (in.kind()) {
                case SUM -> columnsOf("sum", out, in.arg());
                case MIN -> columnsOf("min", out, in.arg());
                case MAX -> columnsOf("max", out, in.arg());
                case AVG -> columnsOf("avg", out, in.arg());
                case COUNT -> !in.nullable() ? "{\"count\": {\"columnName\": \"" + out + "\"}}"
                        : "{\"countWhere\": {\"columnName\": \"" + out + "\", \"filters\": "
                                + DeephavenJson.strings(List.of(Deephaven.notNullFilter(in.arg()))) + "}}";
                case ROW_NUMBER, SHIFT, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, FIRST, LAST, EMA ->
                        throw new IllegalStateException("a window function (" + in.fn() + ") of an order reached the"
                                + " whole partition's");
            });
            outs.add(out);
            made.put(call, out);
        }
        if (!pre.isEmpty()) {
            cur = view("updateView", cur, pre);
        }
        int totals = simple("aggregate", source(cur) + ", \"groupByColumns\": " + DeephavenJson.strings(partition)
                + ", \"aggregations\": [" + String.join(", ", aggregations) + "]");
        cur = joinOp("naturalJoin", cur, totals, partition, outs, false);
        scope.computed.putAll(made);
        return cur;
    }

    private static String columnsOf(String spec, String out, String value) {
        return "{\"columns\": {\"spec\": {\"" + spec + "\": {}}, \"matchPairs\": "
                + DeephavenJson.strings(List.of(out + "=" + value)) + "}}";
    }

    /**
     * Windows over one partition and order, the rows sorted by it: the server's {@code updateBy}, grouped by the
     * partition, one operation each, and what is read across each tie or each partition. SQL's default frame (and RANGE
     * from the partition's start to the row) counts a row's ties with it -- every tie takes the figure at the last of
     * them -- so those figures are taken at each tie's last row and joined back to every tie by the partition and the
     * order, one aggregate for them all; a window of ROWS is the rows as they are; a RANGE of time a window of time along
     * the order's one date-time column; LAG and LEAD the one row a window that far behind or ahead holds. A row's rank
     * is the number of the first of its ties; its dense rank the running count of the ties begun; its percent rank and
     * cumulative distribution, and its bucket (NTILE), figures of those numbers and the partition's count.
     */
    private int ordered(List<SqlExpr.WindowCall> calls, int cur, Deephaven.Scope scope) {
        SqlExpr.WindowCall first = calls.get(0);
        List<Spec> pre = new ArrayList<>();
        List<String> partition = new ArrayList<>();
        for (SqlExpr p : first.partitionBy()) {
            partition.add(columnOf(p, pre));
        }
        // the order: each key a column, a key that may be null after its null flag (the platform's order: nulls last
        // ascending, first descending, unless the key says)
        List<String> keys = new ArrayList<>();
        List<String[]> sorts = new ArrayList<>();
        for (SqlSelect.SortKey key : first.orderBy()) {
            String column = columnOf(key.expr(), pre);
            if (Deephaven.nullable(key.expr())) {
                boolean nullsFirst = key.nullOrder() != null
                        ? key.nullOrder() == SqlSelect.SortKey.NullOrder.NULLS_FIRST : !key.ascending();
                String flag = helper("x");
                pre.add(new Spec(flag + " = isNull(" + column + ")", null));
                sorts.add(new String[] {flag, nullsFirst ? "DESCENDING" : "ASCENDING"});
                keys.add(flag);
            }
            sorts.add(new String[] {column, key.ascending() ? "ASCENDING" : "DESCENDING"});
            keys.add(column);
        }
        List<Input> inputs = new ArrayList<>();
        for (SqlExpr.WindowCall call : calls) {
            inputs.add(input(call, pre));
        }
        java.util.Set<WindowFn> kinds = java.util.EnumSet.noneOf(WindowFn.class);
        inputs.forEach(in -> kinds.add(in.kind()));
        boolean ranked = kinds.contains(WindowFn.RANK) || kinds.contains(WindowFn.DENSE_RANK)
                || kinds.contains(WindowFn.PERCENT_RANK) || kinds.contains(WindowFn.CUME_DIST)
                || kinds.contains(WindowFn.NTILE);
        // a count of what is never null, and a row's number, count a column of ones
        String ones = null;
        if (ranked || kinds.contains(WindowFn.ROW_NUMBER) || inputs.stream().anyMatch(in -> !in.nullable()
                && (in.kind() == WindowFn.COUNT || in.kind() == WindowFn.AVG))) {
            ones = helper("x");
            pre.add(new Spec(ones + " = 1L", null));
        }
        if (!pre.isEmpty()) {
            cur = view("updateView", cur, pre);
        }
        StringBuilder sort = new StringBuilder(source(cur)).append(", \"sorts\": [");
        for (int i = 0; i < sorts.size(); i++) {
            sort.append(i > 0 ? ", " : "").append("{\"columnName\": ").append(DeephavenJson.string(sorts.get(i)[0]))
                    .append(", \"direction\": \"").append(sorts.get(i)[1]).append("\"}");
        }
        cur = simple("sort", sort.append(']').toString());
        // the updateBy's operations, the formulas over them, what is read at each tie and each partition, and what is
        // computed from those last
        List<String> ops = new ArrayList<>();
        List<String> after = new ArrayList<>();
        List<String> tieFirsts = new ArrayList<>();
        List<String> tieLasts = new ArrayList<>();
        List<String> partFirsts = new ArrayList<>();
        List<String> partLasts = new ArrayList<>();
        List<String> flags = new ArrayList<>();
        List<String> dense = new ArrayList<>();
        List<String> finals = new ArrayList<>();
        // a row's number, its rank (the number of the first of its ties), the number of the last of them, and the
        // partition's count, where a ranking reads them
        String rn = null;
        String rank = null;
        String lastRn = null;
        String count = null;
        if (ranked) {
            rn = helper("x");
            ops.add(operation("{\"sum\": {}}", rn + "=" + ones));
            if (kinds.contains(WindowFn.RANK) || kinds.contains(WindowFn.DENSE_RANK)
                    || kinds.contains(WindowFn.PERCENT_RANK)) {
                rank = helper("x");
                tieFirsts.add(rank + "=" + rn);
            }
            if (kinds.contains(WindowFn.CUME_DIST)) {
                lastRn = helper("x");
                tieLasts.add(lastRn + "=" + rn);
            }
            if (kinds.contains(WindowFn.PERCENT_RANK) || kinds.contains(WindowFn.CUME_DIST)
                    || kinds.contains(WindowFn.NTILE)) {
                count = helper("x");
            }
        }
        Map<SqlExpr.WindowCall, String> made = new LinkedHashMap<>();
        for (int i = 0; i < calls.size(); i++) {
            SqlExpr.WindowCall call = calls.get(i);
            Input in = inputs.get(i);
            SqlExpr.WindowCall.Frame frame = call.frame();
            SqlExpr.WindowCall.Frame.Bound from = frame == null
                    ? new SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding() : frame.from();
            SqlExpr.WindowCall.Frame.Bound to = frame == null ? new SqlExpr.WindowCall.Frame.Bound.CurrentRow()
                    : frame.to();
            boolean fromStart = from instanceof SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding;
            boolean toRow = to instanceof SqlExpr.WindowCall.Frame.Bound.CurrentRow;
            boolean range = frame == null || frame.kind() == SqlExpr.WindowCall.Frame.Kind.RANGE;
            String out = helper("w");
            switch (in.kind()) {
                case RANK -> out = java.util.Objects.requireNonNull(rank);
                case DENSE_RANK -> {
                    // a tie begun at this row counts one: its row number is its rank
                    String begun = helper("x");
                    flags.add(begun + " = (" + rn + " == " + rank + ") ? 1L : 0L");
                    dense.add(operation("{\"sum\": {}}", out + "=" + begun));
                }
                case PERCENT_RANK -> finals.add(out + " = (" + count + " == 1 ? 0.0 : (double) (" + rank + " - 1) / ("
                        + count + " - 1))");
                case CUME_DIST -> finals.add(out + " = (double) " + lastRn + " / " + count);
                case NTILE -> finals.add(out + " = " + ntile(call, java.util.Objects.requireNonNull(rn),
                        java.util.Objects.requireNonNull(count)));
                case SHIFT -> {
                    String group = helper("x");
                    ops.add(shift(call, in, group));
                    after.add(out + " = " + dialect.ofGroup(call.type(), "first", "a " + in.fn()) + "(" + group + ")");
                }
                case FIRST -> {
                    if (fromStart && (toRow || ahead(to))) {
                        // a frame from the partition's start: its first row's
                        partFirsts.add(out + "=" + in.arg());
                    } else {
                        String group = helper("x");
                        ops.add(operation("{\"rollingGroup\": " + windowScale(java.util.Objects.requireNonNull(frame),
                                from, to, call) + "}", group + "=" + in.arg()));
                        after.add(out + " = " + dialect.ofGroup(call.type(), "first", "a " + in.fn()) + "(" + group
                                + ")");
                    }
                }
                case LAST -> {
                    if (to instanceof SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing && (fromStart || !ahead(from))) {
                        // a frame to the partition's end: its last row's
                        partLasts.add(out + "=" + in.arg());
                    } else if (fromStart && toRow && range) {
                        // to the row and its ties: the last tie's
                        tieLasts.add(out + "=" + in.arg());
                    } else if (fromStart && toRow) {
                        out = in.arg();
                    } else {
                        String group = helper("x");
                        ops.add(operation("{\"rollingGroup\": " + windowScale(java.util.Objects.requireNonNull(frame),
                                from, to, call) + "}", group + "=" + in.arg()));
                        after.add(out + " = " + dialect.ofGroup(call.type(), "last", "a " + in.fn()) + "(" + group
                                + ")");
                    }
                }
                case EMA -> {
                    // the server's own, running only: to the row and its ties, the last tie's (a tie's own order
                    // is none the platform names)
                    if (!(fromStart && toRow)) {
                        throw new DialectCapability("an exponential moving average over a frame other than the running"
                                + " one reached Deephaven: no shape yet");
                    }
                    String column = range ? helper("x") : out;
                    if (range) {
                        tieLasts.add(out + "=" + column);
                    }
                    ops.add(ema(call, in, column));
                }
                case SUM, MIN, MAX, AVG, COUNT, ROW_NUMBER -> {
                    boolean running = fromStart && toRow;
                    // a row's number counts the rows to it, ties or none: the rows as they are
                    boolean ties = running && in.kind() != WindowFn.ROW_NUMBER && range;
                    String column = ties ? helper("x") : out;
                    if (ties) {
                        tieLasts.add(out + "=" + column);
                    }
                    if (running) {
                        running(in, column, ones, ops, after);
                    } else {
                        rolling(call, in, java.util.Objects.requireNonNull(frame), from, to, column, ones, ops);
                    }
                }
            }
            made.put(call, out);
        }
        if (!ops.isEmpty()) {
            cur = updateBy(cur, ops, partition);
        }
        if (!after.isEmpty()) {
            cur = simple("updateView", source(cur) + ", \"columnSpecs\": " + DeephavenJson.strings(after));
        }
        List<String> byTie = new ArrayList<>(partition);
        byTie.addAll(keys);
        cur = readAcross(cur, byTie, tieFirsts, tieLasts, null);
        cur = readAcross(cur, partition, partFirsts, partLasts, count);
        if (!dense.isEmpty()) {
            cur = simple("updateView", source(cur) + ", \"columnSpecs\": " + DeephavenJson.strings(flags));
            cur = updateBy(cur, dense, partition);
        }
        if (!finals.isEmpty()) {
            cur = simple("updateView", source(cur) + ", \"columnSpecs\": " + DeephavenJson.strings(finals));
        }
        scope.computed.putAll(made);
        return cur;
    }

    /** Whether a bound is the row or after it. */
    private static boolean ahead(SqlExpr.WindowCall.Frame.Bound b) {
        return switch (b) {
            case SqlExpr.WindowCall.Frame.Bound.CurrentRow c -> true;
            case SqlExpr.WindowCall.Frame.Bound.Following f -> true;
            case SqlExpr.WindowCall.Frame.Bound.IntervalFollowing f -> true;
            case SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing u -> true;
            case SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding u -> false;
            case SqlExpr.WindowCall.Frame.Bound.Preceding p -> false;
            case SqlExpr.WindowCall.Frame.Bound.IntervalPreceding p -> false;
        };
    }

    /** A running figure from the partition's start to the row: the server's cumulative operation. */
    private void running(Input in, String column, @com.legend.base.Nullable String ones, List<String> ops,
            List<String> after) {
        switch (in.kind()) {
            case SUM -> ops.add(operation("{\"sum\": {}}", column + "=" + in.arg()));
            case MIN -> ops.add(operation("{\"min\": {}}", column + "=" + in.arg()));
            case MAX -> ops.add(operation("{\"max\": {}}", column + "=" + in.arg()));
            case COUNT -> ops.add(in.nullable() ? countWhere(column, in.arg())
                    : operation("{\"sum\": {}}", column + "=" + ones));
            case ROW_NUMBER -> ops.add(operation("{\"sum\": {}}", column + "=" + ones));
            case AVG -> {
                // no running average on the server: the running sum over the running count of the values
                String sum = helper("x");
                String count = helper("x");
                ops.add(operation("{\"sum\": {}}", sum + "=" + in.arg()));
                ops.add(in.nullable() ? countWhere(count, in.arg()) : operation("{\"sum\": {}}", count + "=" + ones));
                after.add(column + " = (" + count + " == 0 ? NULL_DOUBLE : (double) " + sum + " / " + count + ")");
            }
            case SHIFT, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, FIRST, LAST, EMA ->
                    throw new IllegalStateException("a " + in.fn() + " reached the running figures");
        }
    }

    /** A figure over a frame of rows or of time: the server's rolling operation. */
    private void rolling(SqlExpr.WindowCall call, Input in, SqlExpr.WindowCall.Frame frame,
            SqlExpr.WindowCall.Frame.Bound from, SqlExpr.WindowCall.Frame.Bound to, String column,
            @com.legend.base.Nullable String ones, List<String> ops) {
        String spec = switch (in.kind()) {
            case SUM -> "rollingSum";
            case MIN -> "rollingMin";
            case MAX -> "rollingMax";
            case AVG -> "rollingAvg";
            case COUNT -> "rollingCount";
            case ROW_NUMBER -> throw new DialectCapability("a " + in.fn() + " over a frame reached Deephaven: no shape yet");
            case SHIFT, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, FIRST, LAST, EMA ->
                    throw new IllegalStateException("a " + in.fn() + " reached the rolling figures");
        };
        // a count of every row counts the column of ones
        String counted = in.kind() == WindowFn.COUNT && !in.nullable() ? java.util.Objects.requireNonNull(ones) : in.arg();
        ops.add(operation("{\"" + spec + "\": " + windowScale(frame, from, to, call) + "}", column + "=" + counted));
    }

    /** The values read at the first and the last row of each group of {@code by} (each tie, or each partition), and its
     *  count of rows: one aggregate, joined back to each of its rows. */
    private int readAcross(int cur, List<String> by, List<String> firsts, List<String> lasts,
            @com.legend.base.Nullable String count) {
        List<String> aggregations = new ArrayList<>();
        List<String> outs = new ArrayList<>();
        for (String[] spec : List.of(new String[] {"first"}, new String[] {"last"})) {
            List<String> pairs = spec[0].equals("first") ? firsts : lasts;
            if (!pairs.isEmpty()) {
                aggregations.add("{\"columns\": {\"spec\": {\"" + spec[0] + "\": {}}, \"matchPairs\": "
                        + DeephavenJson.strings(pairs) + "}}");
                pairs.forEach(pair -> outs.add(pair.substring(0, pair.indexOf('='))));
            }
        }
        if (count != null) {
            aggregations.add("{\"count\": {\"columnName\": \"" + count + "\"}}");
            outs.add(count);
        }
        if (aggregations.isEmpty()) {
            return cur;
        }
        int across = simple("aggregate", source(cur) + ", \"groupByColumns\": " + DeephavenJson.strings(by)
                + ", \"aggregations\": [" + String.join(", ", aggregations) + "]");
        return joinOp("naturalJoin", cur, across, by, outs, false);
    }

    /** NTILE(k): SQL's buckets, the first {@code n % k} of them one row larger -- whole-number division as an exact
     *  quotient (the server's {@code /} is a fraction's: K5), the remainder taken off first. */
    private static String ntile(SqlExpr.WindowCall call, String rn, String n) {
        List<SqlExpr> args = call.fn().args();
        if (args.size() != 1 || !(args.get(0) instanceof SqlExpr.IntLit k) || k.value() < 1) {
            throw new DialectCapability("an NTILE of other than a positive whole number of buckets reached Deephaven: no"
                    + " shape yet");
        }
        String buckets = Long.toString(k.value());
        String q = quotient(n, buckets);
        String r = "(" + n + " % " + buckets + ")";
        return "(" + rn + " <= " + r + " * (" + q + " + 1) ? " + quotient("(" + rn + " - 1)", "(" + q + " + 1)")
                + " + 1 : " + quotient("(" + rn + " - 1 - " + r + ")", q) + " + 1)";
    }

    /** {@code a / b} of two whole numbers that are not negative, as a whole number. */
    private static String quotient(String a, String b) {
        return "((long) ((" + a + " - " + a + " % " + b + ") / " + b + "))";
    }

    private static boolean unbounded(SqlExpr.WindowCall.Frame frame) {
        return frame.from() instanceof SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding
                && frame.to() instanceof SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing;
    }

    /** A running count of the rows where the value is not null. */
    private static String countWhere(String column, String value) {
        return "{\"column\": {\"spec\": {\"countWhere\": {\"resultColumn\": " + DeephavenJson.string(column)
                + ", \"filters\": " + DeephavenJson.strings(List.of(Deephaven.notNullFilter(value))) + "}}}}";
    }

    private static String operation(String spec, String pair) {
        return "{\"column\": {\"spec\": " + spec + ", \"matchPairs\": " + DeephavenJson.strings(List.of(pair)) + "}}";
    }

    private int updateBy(int cur, List<String> operations, List<String> partition) {
        return simple("updateBy", source(cur) + ", \"operations\": [" + String.join(", ", operations) + "]"
                + (partition.isEmpty() ? "" : ", \"groupByColumns\": " + DeephavenJson.strings(partition)));
    }

    /** An exponential moving average of {@code in}'s values to {@code column} (lite's {@code ema(values, alpha)}): the
     *  server's tick-decayed EMA, which steps {@code a * so far + (1 - a) * value} with {@code a = exp(-1 / ticks)},
     *  so the ticks are alpha's {@code -1 / ln(1 - alpha)} -- alpha a literal strictly between 0 and 1 (the server's
     *  ticks are a number of the message, never a formula). A null skipped and a NaN poisoning what follows, as the
     *  platform's fold over the frame (DuckDB's list_reduce) does; the server's default skips a NaN. */
    private static String ema(SqlExpr.WindowCall call, Input in, String column) {
        SqlExpr a = call.fn().args().get(1);
        double alpha = a instanceof SqlExpr.FloatLit f ? f.value() : Double.NaN;
        if (!(alpha > 0 && alpha < 1)) {
            throw new DialectCapability("an exponential moving average whose alpha is not a literal between 0 and 1 ("
                    + a + ") reached Deephaven: no shape yet");
        }
        double ticks = -1 / Math.log1p(-alpha);
        return operation("{\"ema\": {\"options\": {\"onNullValue\": \"SKIP\", \"onNanValue\": \"POISON\"},"
                + " \"windowScale\": {\"ticks\": {\"ticks\": " + ticks + "}}}}", column + "=" + in.arg());
    }

    /** LAG(v, k) and LEAD(v, k): the value k rows before or after, within the partition, null where there is none --
     *  the one row a rolling group of that row holds, read by the library's {@code first} (DH33: a vector's own methods
     *  are not the validator's). A default other than null is refused by name. */
    private static String shift(SqlExpr.WindowCall call, Input in, String group) {
        List<SqlExpr> args = call.fn().args();
        if (args.size() > 2) {
            throw new DialectCapability("a " + in.fn() + " with a default reached Deephaven: no shape yet");
        }
        long k = 1;
        if (args.size() == 2) {
            if (!(args.get(1) instanceof SqlExpr.IntLit n) || n.value() < 1) {
                throw new DialectCapability("a " + in.fn() + " by other than a positive whole number reached Deephaven:"
                        + " no shape yet");
            }
            k = n.value();
        }
        // the window holds the row k before (a lag) or k after (a lead): the row itself counts as one behind it
        long reverse = in.fn() == SqlAgg.Fn.LAG ? k + 1 : 1 - k;
        long forward = in.fn() == SqlAgg.Fn.LAG ? -k : k;
        return operation("{\"rollingGroup\": {\"reverseWindowScale\": " + ticks(reverse) + ", \"forwardWindowScale\": "
                + ticks(forward) + "}}", group + "=" + in.arg());
    }

    private static String ticks(long n) {
        return "{\"ticks\": {\"ticks\": " + n + "}}";
    }

    /** A frame of ROWS as the server's ticks (the row itself one of those behind it), or a RANGE of time along the
     *  order's one date-time column as its nanoseconds; any other refused by name. */
    private String windowScale(SqlExpr.WindowCall.Frame frame, SqlExpr.WindowCall.Frame.Bound from,
            SqlExpr.WindowCall.Frame.Bound to, SqlExpr.WindowCall call) {
        if (frame.kind() == SqlExpr.WindowCall.Frame.Kind.ROWS) {
            Long behind = rowsAt(from);
            Long ahead = rowsAt(to);
            if (behind == null || ahead == null || behind > ahead) {
                throw new DialectCapability("a frame of rows from " + from + " to " + to + " reached Deephaven: no shape"
                        + " yet");
            }
            return "{\"reverseWindowScale\": " + ticks(1 - behind) + ", \"forwardWindowScale\": " + ticks(ahead) + "}";
        }
        SqlSelect.SortKey key = call.orderBy().get(0);
        if (call.orderBy().size() != 1 || !(key.expr().type() instanceof com.legend.sql.TypeFact.Typed t)
                || t.type() != com.legend.sql.SqlType.Scalar.TIMESTAMP || !key.ascending()
                || Deephaven.nullable(key.expr())) {
            throw new DialectCapability("a RANGE frame other than one of time along one ascending date-time that is never"
                    + " null reached Deephaven: no shape yet");
        }
        String column = dialect.plainColumn(key.expr());
        if (column == null) {
            throw new DialectCapability("a RANGE frame of time along a computed date-time reached Deephaven: no shape yet");
        }
        Long behind = nanosAt(from);
        Long ahead = nanosAt(to);
        if (behind == null || ahead == null || behind > ahead) {
            throw new DialectCapability("a frame of time from " + from + " to " + to + " reached Deephaven: no shape yet");
        }
        return "{\"reverseWindowScale\": " + time(column, -behind) + ", \"forwardWindowScale\": " + time(column, ahead)
                + "}";
    }

    private static String time(String column, long nanos) {
        return "{\"time\": {\"column\": " + DeephavenJson.string(column) + ", \"nanos\": \"" + nanos + "\"}}";
    }

    /** A bound of rows as its place from the row (before negative), or null when it has none. */
    private static @com.legend.base.Nullable Long rowsAt(SqlExpr.WindowCall.Frame.Bound b) {
        return switch (b) {
            case SqlExpr.WindowCall.Frame.Bound.CurrentRow c -> 0L;
            case SqlExpr.WindowCall.Frame.Bound.Preceding p -> negated(wholeRows(p.n()));
            case SqlExpr.WindowCall.Frame.Bound.Following f -> wholeRows(f.n());
            case SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding u -> null;
            case SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing u -> null;
            case SqlExpr.WindowCall.Frame.Bound.IntervalPreceding i -> null;
            case SqlExpr.WindowCall.Frame.Bound.IntervalFollowing i -> null;
        };
    }

    private static @com.legend.base.Nullable Long negated(@com.legend.base.Nullable Long n) {
        return n == null ? null : -n;
    }

    private static @com.legend.base.Nullable Long times(long n, @com.legend.base.Nullable Long unit) {
        return unit == null ? null : Math.multiplyExact(n, unit);
    }

    private static @com.legend.base.Nullable Long wholeRows(Number n) {
        double d = Math.abs(n.doubleValue());
        return d == Math.rint(d) ? (long) d : null;
    }

    /** A bound of time as its nanoseconds from the row (before negative), or null when it is none of a fixed length. */
    private static @com.legend.base.Nullable Long nanosAt(SqlExpr.WindowCall.Frame.Bound b) {
        return switch (b) {
            case SqlExpr.WindowCall.Frame.Bound.CurrentRow c -> 0L;
            case SqlExpr.WindowCall.Frame.Bound.IntervalPreceding i -> negated(times(Math.abs(i.n()), unitNanos(i.unit())));
            case SqlExpr.WindowCall.Frame.Bound.IntervalFollowing i -> times(Math.abs(i.n()), unitNanos(i.unit()));
            case SqlExpr.WindowCall.Frame.Bound.Preceding p -> null;
            case SqlExpr.WindowCall.Frame.Bound.Following f -> null;
            case SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding u -> null;
            case SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing u -> null;
        };
    }

    /** A fixed unit's nanoseconds, Pure's {@code DurationUnit} by name (a day 24 hours: Pure's DateTime is UTC's, DH29);
     *  a month or a year has none. */
    private static @com.legend.base.Nullable Long unitNanos(String unit) {
        return switch (unit) {
            case "NANOSECONDS" -> 1L;
            case "MICROSECONDS" -> 1_000L;
            case "MILLISECONDS" -> 1_000_000L;
            case "SECONDS" -> 1_000_000_000L;
            case "MINUTES" -> 60_000_000_000L;
            case "HOURS" -> 3_600_000_000_000L;
            case "DAYS" -> 86_400_000_000_000L;
            case "WEEKS" -> 604_800_000_000_000L;
            default -> null;
        };
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
        // the aggregates computed after the group, from what it gathered (a vector, a count), by one update
        Map<Integer, After> after = new LinkedHashMap<>();
        // the modes, each from an aggregation of its own joined back by the keys: its input column by its place
        Map<Integer, String> modes = new LinkedHashMap<>();
        for (int j = 0; j < reducers.size(); j++) {
            if (cells.get(j) != null) {
                inputs.add("");
                continue;
            }
            SqlExpr arg;
            // the value's order for a value at the first or last of an order (ARG_MIN, ARG_MAX: K3a), or a statistic's
            // second variable (CORR, COVAR_*), else none
            SqlExpr by = null;
            if (reducers.get(j) instanceof SqlAgg.Reducer r) {
                boolean ordered = r.fn() == SqlAgg.Fn.ARG_MIN || r.fn() == SqlAgg.Fn.ARG_MAX;
                boolean pair = BIVARIATE.contains(r.fn());
                int arity = ordered || pair || r.fn() == SqlAgg.Fn.QUANTILE_CONT ? 2 : 1;
                if (r.args().size() > arity) {
                    throw new DialectCapability("aggregate " + r.fn() + " over " + r.args().size()
                            + " arguments reached Deephaven: no shape yet");
                }
                arg = r.args().isEmpty() ? null : inDoubles(r, averaged(r.fn(), r.args().get(0)));
                by = ordered || pair ? inDoubles(r, r.args().get(1)) : null;
            } else {
                arg = uniqueValueOf(reducers.get(j));
            }
            String input = input(arg, "__a" + j, keys, pre);
            inputs.add(input);
            if (reducers.get(j) instanceof SqlAgg.Reducer r) {
                String second = by == null ? null : input(by, "__b" + j, keys, pre);
                if (AFTER.contains(r.fn())) {
                    after.put(j, new After(r, input, second));
                } else if (r.fn() == SqlAgg.Fn.MODE) {
                    if (r.distinct() || !r.orderBy().isEmpty()) {
                        throw new DialectCapability("a distinct or ordered MODE reached Deephaven: no shape yet");
                    }
                    modes.put(j, input);
                } else if (second != null && (Deephaven.nullable(r.args().get(0)) || Deephaven.nullable(r.args().get(1)))) {
                    // SQL's ARG_MIN and ARG_MAX skip a row whose value or order is null; the server's sortedFirst
                    // sorts a null first: such a row's value is made null and its order the last there is
                    scope.aggregates.put(r, skipped(r, j, input, second, pre));
                } else {
                    scope.aggregates.put(r, second == null ? new String[] {"__r" + j, input}
                            : new String[] {"__r" + j, input, second});
                }
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
            if (cell != null && cell.fn() != SqlAgg.Fn.COUNT || modes.containsKey(j)) {
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
            } else if (after.containsKey(j)) {
                w.append(gathered(j, java.util.Objects.requireNonNull(after.get(j))));
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
        for (Map.Entry<Integer, String> m : modes.entrySet()) {
            grouped = mode(grouped, cur, keys, m.getKey(), m.getValue());
        }
        if (!after.isEmpty()) {
            List<Spec> finals = new ArrayList<>();
            after.forEach((j, a) -> finals.addAll(computed(j, a)));
            grouped = view("update", grouped, finals);
        }
        for (int i = 0; i < s.groupBy().size(); i++) {
            scope.computed.put(s.groupBy().get(i), keys.get(i));
        }
        for (int j = 0; j < reducers.size(); j++) {
            scope.computed.put(reducers.get(j), "__r" + j);
        }
        scope.grouped = true;
        return grouped;
    }

    /** The statistics of two variables: over the rows where neither is null, as SQL's are. */
    private static final java.util.Set<SqlAgg.Fn> BIVARIATE = java.util.Set.of(SqlAgg.Fn.CORR,
            SqlAgg.Fn.COVAR_SAMP, SqlAgg.Fn.COVAR_POP);

    /** The aggregates the server has no aggregation for, computed by a formula after the group from what it gathered:
     *  a correlation and the covariances (the library's cor and cov over the group's two vectors), the population
     *  variance and deviation (the sample variance's aggregation, rescaled by the count), and the interpolated
     *  percentile (the group's vector sorted, two of its places read, weighted). */
    private static final java.util.Set<SqlAgg.Fn> AFTER = java.util.Set.of(SqlAgg.Fn.CORR, SqlAgg.Fn.COVAR_SAMP,
            SqlAgg.Fn.COVAR_POP, SqlAgg.Fn.VAR_POP, SqlAgg.Fn.STDDEV_POP, SqlAgg.Fn.QUANTILE_CONT);

    /** An aggregate computed after its group: its reducer and the columns it reads (the second a pair's). */
    private record After(SqlAgg.Reducer r, String input, @com.legend.base.Nullable String second) {
    }

    /** What the group gathers for an aggregate computed after it: the vectors and the count of the rows that count. */
    private String gathered(int j, After a) {
        if (a.r().distinct() || !a.r().orderBy().isEmpty()) {
            throw new DialectCapability("a distinct or ordered " + a.r().fn() + " reached Deephaven: no shape yet");
        }
        String n = countWhere("__n" + j, a.second() == null ? List.of(a.input()) : List.of(a.input(), a.second()));
        return switch (a.r().fn()) {
            case CORR, COVAR_SAMP, COVAR_POP -> "{\"columns\": {\"spec\": {\"group\": {}}, \"matchPairs\": "
                    + DeephavenJson.strings(List.of("__v" + j + "=" + a.input(),
                            "__w" + j + "=" + java.util.Objects.requireNonNull(a.second()))) + "}}, " + n;
            case VAR_POP, STDDEV_POP -> "{\"columns\": {\"spec\": {\"var\": {}}, \"matchPairs\": "
                    + DeephavenJson.strings(List.of("__v" + j + "=" + a.input())) + "}}, " + n;
            case QUANTILE_CONT -> "{\"columns\": {\"spec\": {\"group\": {}}, \"matchPairs\": "
                    + DeephavenJson.strings(List.of("__v" + j + "=" + a.input())) + "}}";
            default -> throw new IllegalStateException(a.r().fn() + " is not computed after its group");
        };
    }

    /**
     * SQL's MODE, which the server has no aggregation for: the group's values that are not null counted, each, by the
     * keys and the value; each group's value of the greatest count taken by sortedLast of it; joined back to
     * {@code grouped} by the keys into {@code __r<j>} -- a group with no value has no row there, so its mode is null, as
     * SQL's. A tie of counts is any of the tied values, as SQL's MODE is.
     */
    private int mode(int grouped, int source, List<String> keys, int j, String input) {
        int present = filterText(source, List.of(Deephaven.notNullFilter(input)));
        List<String> byValue = new ArrayList<>(keys);
        byValue.add(input);
        int counted = simple("aggregate", source(present) + ", \"groupByColumns\": " + DeephavenJson.strings(byValue)
                + ", \"aggregations\": [{\"count\": {\"columnName\": \"__m" + j + "\"}}]");
        int best = simple("aggregate", source(counted) + ", \"groupByColumns\": " + DeephavenJson.strings(keys)
                + ", \"aggregations\": [{\"columns\": {\"spec\": {\"sortedLast\": {\"columns\": [{\"columnName\": \"__m"
                + j + "\"}]}}, \"matchPairs\": " + DeephavenJson.strings(List.of("__r" + j + "=" + input)) + "}}]");
        return joinOp("naturalJoin", grouped, best, keys, List.of("__r" + j), false);
    }

    /** The rows where every one of {@code columns} is not null, counted into {@code out}. */
    private static String countWhere(String out, List<String> columns) {
        return "{\"countWhere\": {\"columnName\": \"" + out + "\", \"filters\": "
                + DeephavenJson.strings(columns.stream().map(Deephaven::notNullFilter).toList()) + "}}";
    }

    /**
     * An aggregate computed after its group, into {@code __r<j>}, SQL's rules kept: a covariance of no pair, or a
     * sample one or a correlation of fewer than two, is null; the server's cov is the population covariance (the
     * sums of the pairs over n), so the sample's is rescaled by n / (n - 1); a population variance of one value is 0
     * (the server's sample variance of it is not a number); the interpolated percentile reads, among the group's
     * values that are not null (sorted, its nulls first: the library's sort), the places either side of p * (n - 1).
     */
    private List<Spec> computed(int j, After a) {
        String r = "__r" + j;
        String v = "__v" + j;
        String n = "__n" + j;
        return switch (a.r().fn()) {
            case COVAR_POP -> List.of(new Spec(r + " = (" + n + " == 0 ? NULL_DOUBLE : cov(" + v + ", __w" + j + "))",
                    null));
            case COVAR_SAMP -> List.of(new Spec(r + " = (" + n + " < 2 ? NULL_DOUBLE : cov(" + v + ", __w" + j + ") * "
                    + n + " / (" + n + " - 1))", null));
            case CORR -> List.of(new Spec(r + " = (" + n + " < 2 ? NULL_DOUBLE : cor(" + v + ", __w" + j + "))", null));
            case VAR_POP -> List.of(new Spec(r + " = " + populationVariance(v, n), null));
            case STDDEV_POP -> List.of(new Spec(r + " = (" + n + " == 0 ? NULL_DOUBLE : sqrt(" + populationVariance(v, n)
                    + "))", null));
            case QUANTILE_CONT -> {
                if (!(a.r().args().get(1) instanceof SqlExpr.FloatLit p) || !(p.value() >= 0 && p.value() <= 1)) {
                    throw new DialectCapability("a percentile at a computed place reached Deephaven: no shape yet");
                }
                String c = "__c" + j;
                String k = "__k" + j;
                String o = "__o" + j;
                String sorted = "__s" + j;
                String lo = "nth(" + o + " + (long) floor(" + k + "), " + sorted + ")";
                String hi = "nth(" + o + " + (long) ceil(" + k + "), " + sorted + ")";
                yield List.of(new Spec(c + " = count(" + v + ")", null),
                        new Spec(o + " = len(" + v + ") - " + c, null),
                        new Spec(k + " = " + Deephaven.doubleLiteral(p.value()) + " * (" + c + " - 1)", null),
                        new Spec(sorted + " = sort(" + v + ")", null),
                        new Spec(r + " = (" + c + " == 0 ? NULL_DOUBLE : " + lo + " + (" + hi + " - " + lo + ") * ("
                                + k + " - floor(" + k + ")))", null));
            }
            default -> throw new IllegalStateException(a.r().fn() + " is not computed after its group");
        };
    }

    /** The population variance from the group's sample variance {@code v} of {@code n} values: null of none, 0 of one. */
    private static String populationVariance(String v, String n) {
        return "(" + n + " == 0 ? NULL_DOUBLE : " + n + " == 1 ? 0.0 : " + v + " * (" + n + " - 1) / " + n + ")";
    }

    /**
     * ARG_MIN's and ARG_MAX's inputs with SQL's skipped rows made harmless: a row whose value or order is null gets a
     * null value and the order that sorts it out of the way -- the greatest there is for ARG_MIN's sortedFirst, the
     * server's null (which sorts first) for ARG_MAX's sortedLast -- so the row the server takes is SQL's, and a group
     * every row of which is skipped answers null. An order that is not a number or a date-time has no greatest value
     * to write and is refused by name.
     */
    private String[] skipped(SqlAgg.Reducer r, int j, String value, String order, List<Spec> pre) {
        SqlExpr k = r.args().get(1);
        String skip = "(isNull(" + value + ") || isNull(" + order + "))";
        pre.add(new Spec("__x" + j + " = " + skip + " ? " + dialect.typedNull(r.args().get(0).type(), "a skipped value")
                + " : " + value, null));
        String last = switch (k.type() instanceof com.legend.sql.TypeFact.Typed t
                && t.type() instanceof com.legend.sql.SqlType.Scalar sc ? sc : com.legend.sql.SqlType.Scalar.JSON) {
            case INTEGER -> r.fn() == SqlAgg.Fn.ARG_MIN ? "2147483647" : "NULL_INT";
            case BIGINT -> r.fn() == SqlAgg.Fn.ARG_MIN ? "9223372036854775807L" : "NULL_LONG";
            case DOUBLE -> r.fn() == SqlAgg.Fn.ARG_MIN ? "1.7976931348623157E308" : "NULL_DOUBLE";
            default -> null;
        };
        if (last == null) {
            throw new DialectCapability("aggregate " + r.fn() + " over an order of " + k.type() + " that may be null"
                    + " reached Deephaven: no shape yet");
        }
        pre.add(new Spec("__y" + j + " = " + skip + " ? " + last + " : " + order, null));
        return new String[] {"__r" + j, "__x" + j, "__y" + j, "skipped"};
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
    /**
     * A statistic in doubles over a DECIMAL column (a BigDecimal: the DDL's) takes its argument as a double first, as SQL
     * computes it -- an average of decimals is a DOUBLE (DuckDB's AVG), where the server's own average of BigDecimals
     * keeps their scale (the stress corpus on Deephaven, task 15: a curve's mean 2.905, SQL's 2.9045833333333335). A
     * value at the first or last of an order keeps its type.
     */
    private static SqlExpr inDoubles(SqlAgg.Reducer r, SqlExpr arg) {
        boolean doubles = r.type() instanceof com.legend.sql.TypeFact.Typed t && t.type() == com.legend.sql.SqlType.Scalar.DOUBLE;
        return doubles && r.fn() != SqlAgg.Fn.ARG_MIN && r.fn() != SqlAgg.Fn.ARG_MAX && Deephaven.bigDecimal(arg)
                ? new SqlExpr.Cast(arg, com.legend.sql.SqlType.Scalar.DOUBLE) : arg;
    }

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

    /** A server's table, by its name (the default schema's: DH18), each column renamed under the alias; the table as its
     *  store declares it written beside the batch (DH16), each column with the Java type the server must hold it as. */
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
        List<String> types = new ArrayList<>();
        for (OutputCol c : t.outputs()) {
            String name = DeephavenJson.name(c.name(), "column " + t.name() + "." + c.name());
            String renamed = alias(t.alias()) + "__" + name;
            columns.put(c.name(), renamed);
            renames.add(new Spec(renamed + " = " + name, null));
            SqlDdl.ColumnType stored = t.storedTypes().get(c.name());
            if (stored == null) {
                throw new IllegalStateException("column " + t.name() + "." + c.name() + " reached Deephaven with no"
                        + " declared type: a server's table is checked against its store's declaration (DH16)");
            }
            types.add("{\"name\": " + DeephavenJson.string(renamed) + ", \"column\": " + DeephavenJson.string(name)
                    + ", \"type\": " + DeephavenJson.string(javaTypeOf(stored, t.name() + "." + c.name()))
                    + ", \"declared\": " + DeephavenJson.string(spelled(stored)) + "}");
        }
        scope.sources.put(t.alias(), columns);
        open("view");
        w.append("\"sourceId\": " + DeephavenJson.scopeTable(table) + ", \"columnSpecs\": [");
        for (int i = 0; i < renames.size(); i++) {
            w.append(i > 0 ? ", " : "").append(DeephavenJson.string(renames.get(i).text()));
        }
        w.append("]");
        int op = close();
        declared.add("{\"op\": " + op + ", \"table\": " + DeephavenJson.string(table) + ", \"columns\": ["
                + String.join(", ", types) + "]}");
        return op;
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
        if (j.kind() == SqlSource.Join.Kind.ASOF_LEFT) {
            return asOf(j, left, right, before, rightColumns, scope);
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

    /** An as-of join (DH20, K7): the server's own -- {@code aj}, each left row taking the right row with the greatest
     *  as-of value not after its own ({@code l >= r}, or {@code l > r} strictly), or {@code raj}, the least not before
     *  it ({@code l <= r}, {@code l < r}) -- among those whose keys equal its own; a left row with none keeps nulls, as
     *  SQL's ASOF LEFT JOIN does. A right row whose key or as-of value may be null is filtered out first, as DuckDB's
     *  as-of join and the corpus's oracle match no null; a left as-of value that may be null is refused for {@code raj} (the server orders a null before
     *  every value, which the reverse join would take). Anything beyond the equalities and the one comparison is
     *  refused by name. */
    private int asOf(SqlSource.Join j, int left, int right, Map<String, Map<String, String>> before,
            List<String> rightColumns, Deephaven.Scope scope) {
        List<String> exact = new ArrayList<>();
        List<String> rightNullFilters = new ArrayList<>();
        String asOfColumn = null;
        boolean reverse = false;
        dialect.pushScope(scope);
        try {
            for (SqlExpr conjunct : conjuncts(j.on())) {
                // a key matches no null: the lowering writes the as-of join's keys null-safe (IS NOT DISTINCT FROM),
                // but DuckDB's ASOF JOIN matches no null key under it and the corpus's oracle states the same ("a NULL
                // key or instant matches nothing"); H2's emulation does match them (measured 2026-10-25, DuckDB 1.4.4,
                // docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K7: a finding for the platform). The server's exact match
                // takes a null as a key value, so a right key that may be null is filtered out first.
                boolean nullSafe = conjunct instanceof SqlExpr.Call c && c.fn() == SqlFn.NULL_SAFE_EQUAL;
                String[] pair = keyPair(nullSafe ? new SqlExpr.Call(SqlFn.EQUAL, ((SqlExpr.Call) conjunct).args(),
                        conjunct.type()) : conjunct, before, scope);
                if (pair != null) {
                    exact.add(pair[0] + "=" + pair[1]);
                    if (pair[2] != null) {
                        rightNullFilters.add(Deephaven.notNullFilter(pair[1]));
                    }
                    continue;
                }
                AsOfSides sides = asOfSides(conjunct, before, scope);
                if (sides == null || asOfColumn != null) {
                    throw new DialectCapability("an as-of join with a condition beyond its key equalities and one"
                            + " comparison of its sides reached Deephaven: no shape yet (DH20)");
                }
                reverse = sides.op().startsWith("<");
                String leftAt = sides.left();
                if (reverse && sides.leftNullable()) {
                    // the server orders a null before every value, which the reverse join would match with the first
                    // right row: a null time is matched as the last whole second the server converts instead (its
                    // nanoseconds' range ends 2262-04-11T23:47:16.854775807Z; the very last overflows, measured on
                    // 42.7), after which no row comes but one in that range's last 0.85 s (a helper column; the left's
                    // own is as it was)
                    if (sides.leftType() != com.legend.sql.SqlType.Scalar.TIMESTAMP) {
                        throw new DialectCapability("an as-of join to the next row of a left value other than a"
                                + " date-time that may be null reached Deephaven: the server orders a null first, which"
                                + " its reverse as-of join would take");
                    }
                    leftAt = helper("x");
                    left = view("updateView", left, List.of(new Spec(leftAt + " = isNull(" + sides.left()
                            + ") ? epochNanosToInstant(9223372035999999999L) : " + sides.left(), null)));
                }
                if (sides.rightNullable()) {
                    rightNullFilters.add(Deephaven.notNullFilter(sides.right()));
                }
                asOfColumn = leftAt + sides.op() + sides.right();
            }
        } finally {
            dialect.popScope();
        }
        if (asOfColumn == null) {
            throw new DialectCapability("an as-of join with no comparison of its sides reached Deephaven: no shape yet");
        }
        if (!rightNullFilters.isEmpty()) {
            right = filterText(right, rightNullFilters);
        }
        return simple(reverse ? "raj" : "aj", "\"leftId\": " + DeephavenJson.offset(left) + ", \"rightId\": "
                + DeephavenJson.offset(right) + ", \"exactMatchColumns\": " + DeephavenJson.strings(exact)
                + ", \"asOfColumn\": " + DeephavenJson.string(asOfColumn) + ", \"columnsToAdd\": "
                + DeephavenJson.strings(rightColumns));
    }

    /** An as-of join's comparison as the left side reads it: its two columns' Deephaven names, the operator from the
     *  left's view ({@code >=}, {@code >}, {@code <=}, {@code <}), and whether each may be null. */
    private record AsOfSides(String left, String op, String right, boolean leftNullable, boolean rightNullable,
            com.legend.sql.SqlType.@com.legend.base.Nullable Scalar leftType) {
    }

    /** {@code conjunct} as an as-of comparison between a column of each side, or null when it is none. */
    private static @com.legend.base.Nullable AsOfSides asOfSides(SqlExpr conjunct,
            Map<String, Map<String, String>> leftSources, Deephaven.Scope scope) {
        if (!(conjunct instanceof SqlExpr.Call c) || !(c.args().size() == 2)
                || !(c.args().get(0) instanceof SqlExpr.Column a) || !(c.args().get(1) instanceof SqlExpr.Column b)
                || a.table() == null || b.table() == null) {
            return null;
        }
        String op = switch (c.fn()) {
            case GREATER_EQUAL -> ">=";
            case GREATER -> ">";
            case LESS_EQUAL -> "<=";
            case LESS -> "<";
            default -> null;
        };
        boolean aLeft = leftSources.containsKey(a.table());
        if (op == null || aLeft == leftSources.containsKey(b.table())) {
            return null;
        }
        SqlExpr.Column l = aLeft ? a : b;
        SqlExpr.Column r = aLeft ? b : a;
        if (!aLeft) {
            // r op l, as the left reads it: l (op reversed) r
            op = switch (op) {
                case ">=" -> "<=";
                case ">" -> "<";
                case "<=" -> ">=";
                case "<" -> ">";
                default -> throw new IllegalStateException("an as-of comparison " + op);
            };
        }
        Map<String, String> ls = scope.sources.get(l.table());
        Map<String, String> rs = scope.sources.get(r.table());
        String ln = ls == null ? null : ls.get(l.name());
        String rn = rs == null ? null : rs.get(r.name());
        if (ln == null || rn == null) {
            return null;
        }
        return new AsOfSides(ln, op, rn, Deephaven.nullable(l), Deephaven.nullable(r),
                l.type() instanceof com.legend.sql.TypeFact.Typed t && t.type() instanceof com.legend.sql.SqlType.Scalar ls2
                        ? ls2 : null);
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

    /** The Java type a Deephaven server holds a column of {@code type} as -- its {@code deephaven:type} (measured on
     *  42.7, 2026-10-29: {@link #nullOf}'s columns read back) -- which a server's table is checked against (DH16); a
     *  type no Deephaven column holds refused by name. */
    private static String javaTypeOf(SqlDdl.ColumnType type, String column) {
        return switch (type) {
            case SqlDdl.ColumnType.Plain p -> switch (p.kind()) {
                case BIT -> "java.lang.Boolean";
                case TINYINT -> "byte";
                case SMALLINT -> "short";
                case INTEGER -> "int";
                case BIGINT -> "long";
                case REAL -> "float";
                case FLOAT, DOUBLE -> "double";
                case DATE -> "java.time.LocalDate";
                case TIME -> "java.time.LocalTime";
                case TIMESTAMP -> "java.time.Instant";
                case VARCHAR -> "java.lang.String";
                case TIMESTAMP_NS, JSON, OTHER, DISTINCT, ARRAY, OBJECT -> throw new DialectCapability("column " + column
                        + " is " + p.kind() + ", which no Deephaven column holds yet");
            };
            case SqlDdl.ColumnType.Sized s -> switch (s.kind()) {
                case "VARCHAR", "CHAR" -> "java.lang.String";
                default -> throw new DialectCapability("column " + column + " is " + s.kind() + "(" + s.size()
                        + "), which no Deephaven column holds yet");
            };
            case SqlDdl.ColumnType.Scaled s -> switch (s.kind()) {
                case "DECIMAL", "NUMERIC" -> "java.math.BigDecimal";
                default -> throw new DialectCapability("column " + column + " is " + s.kind() + ", which no"
                        + " Deephaven column holds yet");
            };
        };
    }

    /** {@code type} as its store spells it, for a refusal's words. */
    private static String spelled(SqlDdl.ColumnType type) {
        return switch (type) {
            case SqlDdl.ColumnType.Plain p -> p.kind().name();
            case SqlDdl.ColumnType.Sized s -> s.kind() + "(" + s.size() + ")";
            case SqlDdl.ColumnType.Scaled s -> s.kind() + "(" + s.precision() + ", " + s.scale() + ")";
        };
    }

    /** The typed null a column of {@code type} starts from, which gives the input table its column's type
     *  (docs/DEEPHAVEN_PLAN_2026_10_22.md §3's table): its {@link #javaTypeOf Java type}'s null. */
    private static String nullOf(SqlDdl.ColumnType type, String column) {
        String java = javaTypeOf(type, column);
        return switch (java) {
            case "byte" -> "NULL_BYTE";
            case "short" -> "NULL_SHORT";
            case "int" -> "NULL_INT";
            case "long" -> "NULL_LONG";
            case "float" -> "NULL_FLOAT";
            case "double" -> "NULL_DOUBLE";
            case "java.lang.Boolean" -> "(Boolean) null";
            case "java.lang.String" -> "(String) null";
            default -> "(" + java + ") null";
        };
    }
}
