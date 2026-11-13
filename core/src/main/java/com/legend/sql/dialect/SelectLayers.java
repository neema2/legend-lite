// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.base.Nullable;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.TypeFact;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A CHAIN OF LEVELS LAID OUT AGAIN, EACH VALUE ONCE, AT THE LOWEST LEVEL IT CAN BE (2026-11-04, #169: the timeseries
 * SQL's windows shared). Pure steps lower to a level each, and a word's steps sit on the word before's: two words of
 * the same rows -- a pairs trade's hedge ratio and residual -- each compute the same covariance over the same window,
 * one above the other, fourteen levels where a hand writes four ({@link SelectMerge} merges two levels at a time, and
 * never where a value would be computed twice). Read whole, a chain of selects each over the one below that keep every
 * row is a set of values over the bottom's rows:
 * <ul>
 *   <li>each value is defined once: a column read through the levels is the value it reads, and two values of one
 *       expression are one -- each window call its own value, so two words' identical windows are one;</li>
 *   <li>each value is computed at the lowest level its reads allow: a window one level above the windows (and the
 *       values computed once) its arguments read, any other expression at the level of what it reads, written into
 *       its reader there -- but an expression read more than once whose copies would add more than {@link #INLINE}
 *       nodes, computed once at its own level and read as a column;</li>
 *   <li>the chain is written again as those levels, each value a column of its level read by the levels above,
 *       its top the chain's top's columns in their order, under their names and types.</li>
 * </ul>
 * The answer is kept by construction: every level of the chain keeps its source's rows (no WHERE but the bottom's, no
 * GROUP BY, DISTINCT, ORDER BY, LIMIT, QUALIFY, aggregate or UNNEST), so a value is the same value at any level of it,
 * a window over them the same window; a value that may raise ({@link SqlExpr#raisesNothing}: a calendar ramp's
 * interval) is a column of its level, read or not, computed on every row as before -- written into a reader's branch
 * too, it is evaluated there besides; every read is a qualified column of the level below (no star, no subquery).
 * Written again only for a chain with windows, where it has fewer levels than it had and no more than twice its
 * nodes. Not on the engine-text printers.
 */
final class SelectLayers extends SqlRewriter {

    /** The table of a value's read inside this pass: no SQL name. */
    private static final String REF = "\u0001";

    /** The nodes an expression read more than once may add to the statement by being written into each reader:
     *  {@code (reads - 1) * size}, as the lowering's fold bounds its own (Fold.Budget). */
    private static final int INLINE = 24;

    /** How the database labels a column it reads by a bare name: the name as written (DuckDB, SQLite, ClickHouse),
     *  folded to lower case (Postgres) or to upper case (Snowflake, H2). A projection of a table's own column keeps its
     *  alias only where that label differs from the name (namedAs; PR #81, 2026-11-13). */
    private final java.util.function.UnaryOperator<String> bareLabel;

    SelectLayers(java.util.function.UnaryOperator<String> bareLabel) {
        this.bareLabel = bareLabel;
    }

    @Override
    protected SqlQuery select(SqlSelect s) {
        SqlSelect out = laidOut(s, bareLabel);
        return out == null ? s : out;
    }

    /** One value of the chain. */
    private static final class Def {
        final int id;
        /** Over the bottom's columns and other values' reads ({@link #REF}). */
        final SqlExpr expr;
        /** A window call itself: computed at a level of its own, never written into its reader. */
        final boolean window;
        /** A column of the bottom's source: read at the first level, carried up. */
        final boolean base;
        @Nullable String name;
        @Nullable OutputCol out;
        final List<Def> reads = new ArrayList<>();
        /** How many times its expression reads each value: {@code x + x} reads x twice. */
        final Map<Def, Integer> times = new HashMap<>();
        int uses;
        int size;
        boolean once;
        /** Its evaluation may raise ({@link SqlExpr#raisesNothing}): computed on every row, as before. */
        final boolean raises;
        int level = -1;
        int need;
        boolean computed;
        @Nullable String column;
        @Nullable OutputCol col;

        Def(int id, SqlExpr expr, boolean window, boolean base) {
            this.id = id;
            this.expr = expr;
            this.window = window;
            this.base = base;
            this.raises = !SqlExpr.raisesNothing(expr);
        }

        /** Whether it is computed at a level of its own and read as a column, never written into its reader: a window,
         *  an expression read more than once. (One that may raise is computed as a column of its level whoever reads
         *  it -- on every row, as before -- and may be written into a reader of its level too.) */
        boolean own() {
            return window || once;
        }
    }

    /** {@code top} and the chain below it laid out again, or null where it is not a chain or has no fewer levels. */
    private static @Nullable SqlSelect laidOut(SqlSelect top, java.util.function.UnaryOperator<String> bareLabel) {
        // the chain, top first: levels each over the next as its one subselect, keeping its rows
        List<SqlSelect> chain = new ArrayList<>();
        if (!keepsRows(top, false)) {
            return null;
        }
        chain.add(top);
        for (SqlSelect at = top; at.where() == null && at.from() instanceof SqlSource.Subselect sub
                && sub.inner() instanceof SqlSelect in && keepsRows(in, true); at = in) {
            chain.add(in);
        }
        // a chain of windows: a chain of row values alone is the lowering's to lay out (its fold, SelectMerge)
        if (chain.size() < 2 || chain.stream().noneMatch(l -> l.projections().stream().anyMatch(
                p -> windowed(p.expr())))) {
            return null;
        }
        // each value once, bottom up: each level's columns by name, each read of the level below its value
        List<Def> defs = new ArrayList<>();
        Map<SqlExpr, Def> byExpr = new HashMap<>();
        Map<String, Def> below = null;
        for (int i = chain.size() - 1; i >= 0; i--) {
            SqlSelect level = chain.get(i);
            Map<String, Def> mine = new LinkedHashMap<>();
            for (SqlSelect.Projection p : level.projections()) {
                SqlExpr e = p.expr();
                if (below != null) {
                    Resolve r = new Resolve(((SqlSource.Subselect) level.from()).alias(), below);
                    e = r.apply(e);
                    if (r.refused) {
                        return null;
                    }
                } else if (reads(e, REF)) {
                    return null;
                } else {
                    // the bottom's reads of its source: each column a value of its own, read at the first level
                    e = new Bases(defs, byExpr).apply(e);
                }
                Def d = intern(e, defs, byExpr);
                if (d.name == null) {
                    d.name = p.outputName();
                    d.out = p.out();
                }
                mine.put(java.util.Objects.requireNonNull(p.outputName()), d);
            }
            below = mine;
        }
        List<SqlSelect.Projection> outputs = top.projections();
        List<Def> tops = new ArrayList<>();
        for (SqlSelect.Projection p : outputs) {
            tops.add(java.util.Objects.requireNonNull(java.util.Objects.requireNonNull(below).get(p.outputName())));
        }
        // the values the top reads, each one's readers counted; each one's size where written into its reader
        Set<Def> reached = new HashSet<>();
        tops.forEach(d -> {
            d.uses++;
            reach(d, reached);
        });
        for (Def d : defs) {
            if (reached.contains(d)) {
                d.times.forEach((r, n) -> r.uses += n);
            }
        }
        for (Def d : defs) {
            d.size = size(d.expr, defs);
            d.once = !d.window && !d.base && d.uses > 1 && (d.uses - 1) * d.size > INLINE;
        }
        // the lowest level of each: a value computed at a level of its own is read one level above it
        for (Def d : defs) {
            int l = d.base ? 0 : 1;
            for (Def r : d.reads) {
                l = Math.max(l, r.own() ? r.level + 1 : r.level);
            }
            d.level = l;
        }
        int levels = 1;
        for (Def d : tops) {
            levels = Math.max(levels, Math.max(1, d.level));
        }
        // a value that may raise is computed whoever reads it, on every row, as its level computed it before: below
        // the top, whose columns are the chain's
        List<Def> raising = new ArrayList<>();
        for (Def d : defs) {
            if (d.raises && !tops.contains(d)) {
                raising.add(d);
                levels = Math.max(levels, d.level + 1);
            }
        }
        // written again only where it has fewer levels (W4, 2026-11-06: laying out a chain of as many levels for its
        // windows alone was tried and dropped -- one such shape ran H2's planner out of heap)
        if (levels >= chain.size()) {
            return null;
        }
        for (Def d : raising) {
            read(d, d.level + 1, defs);
        }
        // which values are columns, and up to which level each is carried
        for (Def d : tops) {
            if (Math.max(1, d.level) == levels) {
                computeAt(d, levels, defs);
            } else {
                read(d, levels, defs);
            }
        }
        // each column its name: the value's own where it is free, else one of its own
        Set<String> taken = new HashSet<>();
        for (SqlSelect level : chain) {
            level.projections().forEach(p -> taken.add(java.util.Objects.requireNonNull(p.outputName())));
        }
        Set<String> used = new HashSet<>();
        for (Def d : defs) {
            if (d.need > 0) {
                String n = d.name != null && used.add(d.name) ? d.name : null;
                for (int k = 0; n == null; k++) {
                    String c = "lv_" + d.id + (k == 0 ? "" : "_" + k);
                    if (!taken.contains(c) && used.add(c)) {
                        n = c;
                    }
                }
                d.column = n;
                d.col = d.out == null ? null
                        : new OutputCol(n, d.out.type(), d.out.nullable(), OutputCol.Origin.DERIVED);
            }
        }
        // the levels, bottom up, each over the one below under an alias of the chain's own
        List<String> aliases = new ArrayList<>();
        for (int i = chain.size() - 2; i >= 0; i--) {
            aliases.add(((SqlSource.Subselect) chain.get(i).from()).alias());
        }
        SqlSelect bottom = chain.get(chain.size() - 1);
        SqlSource from = bottom.from();
        SqlExpr where = bottom.where();
        SqlSelect out = null;
        for (int k = 1; k <= levels; k++) {
            String alias = k == 1 ? null : aliases.get(k - 2);
            List<SqlSelect.Projection> ps = new ArrayList<>();
            if (k < levels) {
                for (Def d : defs) {
                    if (d.need > k && Math.max(1, d.level) == k) {
                        SqlExpr e = write(d, k, alias, defs);
                        if (d.col == null && e.type() instanceof TypeFact.Typed t) {
                            // a value of no projection of its own (a window inside an expression): its written type
                            d.col = new OutputCol(java.util.Objects.requireNonNull(d.column), t.type(), true,
                                    OutputCol.Origin.DERIVED);
                        }
                        ps.add(new SqlSelect.Projection(e, d.column, d.col));
                    } else if (d.need > k && Math.max(1, d.level) < k) {
                        ps.add(new SqlSelect.Projection(columnOf(d, alias), d.column, d.col));
                    }
                }
            } else {
                for (int i = 0; i < outputs.size(); i++) {
                    Def d = tops.get(i);
                    SqlSelect.Projection p = outputs.get(i);
                    SqlExpr e = Math.max(1, d.level) == levels ? write(d, k, alias, defs) : columnOf(d, alias);
                    String name = java.util.Objects.requireNonNull(p.outputName());
                    boolean named = SelectMerge.namedAs(e, name, bareLabel);
                    ps.add(new SqlSelect.Projection(e, named ? null : name, p.out()));
                }
            }
            SqlSource source = k == 1 ? from : new SqlSource.Subselect(java.util.Objects.requireNonNull(out),
                    java.util.Objects.requireNonNull(alias), null);
            out = new SqlSelect(ps, false, source, k == 1 ? where : null, List.of(), null, null, List.of(), null,
                    null, List.of());
        }
        // never a statement much larger (its columns carried up, its expressions written into readers): each one is
        // bounded by INLINE, a chain of them only here
        int before = 0;
        for (SqlSelect level : chain) {
            for (SqlSelect.Projection p : level.projections()) {
                before += nodes(p.expr());
            }
        }
        int after = 0;
        for (SqlSelect at = java.util.Objects.requireNonNull(out); ;
                at = (SqlSelect) ((SqlSource.Subselect) at.from()).inner()) {
            for (SqlSelect.Projection p : at.projections()) {
                after += nodes(p.expr());
            }
            if (at.from() == from) {
                break;
            }
        }
        return after > 2 * before ? null : out;
    }

    /** {@code e}'s nodes. */
    private static int nodes(SqlExpr e) {
        int n = 1;
        for (SqlExpr c : parts(e)) {
            n += nodes(c);
        }
        return n;
    }

    /** Whether {@code s} is a level of a chain: it keeps its source's rows (a WHERE only where {@code bottom}), and
     *  every value of it is a named, qualified reading that raises nothing. */
    private static boolean keepsRows(SqlSelect s, boolean bottom) {
        if (s.projections().isEmpty() || s.distinct() || !s.groupBy().isEmpty() || s.having() != null
                || s.qualify() != null || !s.orderBy().isEmpty() || s.limit() != null || s.offset() != null
                || s.where() != null && !bottom) {
            return false;
        }
        Set<String> names = new HashSet<>();
        for (SqlSelect.Projection p : s.projections()) {
            String n = p.outputName();
            if (n == null || !names.add(n) || !plain(p.expr())) {
                return false;
            }
        }
        return true;
    }

    /** Whether {@code e} is a value of a row or a window of rows this pass reads through: no star, subquery, lambda,
     *  aggregate, UNNEST or random draw, every column qualified. */
    private static boolean plain(SqlExpr e) {
        switch (e) {
            case SqlExpr.Column c -> {
                return c.table() != null;
            }
            case SqlExpr.Star st -> {
                return false;
            }
            case SqlExpr.StarExcept st -> {
                return false;
            }
            case SqlExpr.ScalarSubquery q -> {
                return false;
            }
            case SqlExpr.Exists q -> {
                return false;
            }
            case SqlExpr.InSubquery q -> {
                return false;
            }
            case SqlExpr.Quantified q -> {
                return false;
            }
            case SqlExpr.DeferredTdsString d -> {
                return false;
            }
            case SqlExpr.Lambda l -> {
                return false;
            }
            case SqlExpr.WindowCall w -> {
                // the window's own function is an aggregate of its frame: its arguments are read as any value
                for (SqlExpr c : parts(w)) {
                    for (SqlExpr x : c instanceof SqlAgg ? c.children() : List.of(c)) {
                        if (!plain(x) || windowed(x)) {
                            return false;
                        }
                    }
                }
                return true;
            }
            default -> {
                // a random draw or a uuid is a value of its own select, never written into its readers: two readers of
                // one would read two draws (SqlFn.drawsAfresh; the A2' review's finding 1, 2026-11-06)
                if (SqlAgg.aggregatesSelect(e) || e instanceof SqlExpr.Call c && (c.fn() == SqlFn.UNNEST
                        || c.fn().drawsAfresh())) {
                    return false;
                }
                for (SqlExpr c : parts(e)) {
                    if (!plain(c)) {
                        return false;
                    }
                }
                return true;
            }
        }
    }

    /** {@code e}'s sub-expressions -- a window's function's arguments among them: a ranking function (LAG, RANK) is no
     *  expression of its own, so a window's children leave its arguments out. */
    private static List<SqlExpr> parts(SqlExpr e) {
        if (e instanceof SqlExpr.WindowCall w && !(w.fn() instanceof SqlExpr)) {
            List<SqlExpr> out = new ArrayList<>(w.fn().args());
            out.addAll(e.children());
            return out;
        }
        return e.children();
    }

    /** Whether {@code e} holds a window call. */
    private static boolean windowed(SqlExpr e) {
        if (e instanceof SqlExpr.WindowCall) {
            return true;
        }
        for (SqlExpr c : parts(e)) {
            if (windowed(c)) {
                return true;
            }
        }
        return false;
    }

    /** Whether {@code e} reads a column of {@code table}. */
    private static boolean reads(SqlExpr e, String table) {
        if (e instanceof SqlExpr.Column c) {
            return table.equals(c.table());
        }
        for (SqlExpr c : parts(e)) {
            if (reads(c, table)) {
                return true;
            }
        }
        return false;
    }

    /** The value of {@code e} (its reads already values'): each window call in it a value of its own, then the rest. */
    private static Def intern(SqlExpr e, List<Def> defs, Map<SqlExpr, Def> byExpr) {
        Def read = refOf(e, defs);
        if (read != null) {
            return read;
        }
        SqlExpr rest = new Windows(defs, byExpr).apply(e);
        Def window = refOf(rest, defs);
        if (window != null) {
            return window;
        }
        return def(rest, false, defs, byExpr);
    }

    /** The value {@code e} is a read of, or null. */
    private static @Nullable Def refOf(SqlExpr e, List<Def> defs) {
        return e instanceof SqlExpr.Column c && REF.equals(c.table()) ? defs.get(Integer.parseInt(c.name())) : null;
    }

    /** The one value of {@code e}: the one already of it, else a new one. */
    private static Def def(SqlExpr e, boolean window, List<Def> defs, Map<SqlExpr, Def> byExpr) {
        Def known = byExpr.get(e);
        if (known != null) {
            return known;
        }
        Def d = new Def(defs.size(), e, window, e instanceof SqlExpr.Column c && !REF.equals(c.table()));
        collect(e, defs, d.reads, d.times);
        defs.add(d);
        byExpr.put(e, d);
        return d;
    }

    /** A read of {@code d}. */
    private static SqlExpr ref(Def d) {
        return SqlExpr.Column.derived(REF, Integer.toString(d.id));
    }

    /** Each value {@code e} reads, into {@code into} once each, and how many times, into {@code times}. */
    private static void collect(SqlExpr e, List<Def> defs, List<Def> into, Map<Def, Integer> times) {
        Def d = refOf(e, defs);
        if (d != null) {
            if (times.merge(d, 1, Integer::sum) == 1) {
                into.add(d);
            }
            return;
        }
        parts(e).forEach(c -> collect(c, defs, into, times));
    }

    /** {@code d} and every value it reads, into {@code reached}. */
    private static void reach(Def d, Set<Def> reached) {
        if (reached.add(d)) {
            d.reads.forEach(r -> reach(r, reached));
        }
    }

    /** {@code e}'s nodes, a value it reads counted as its own nodes where it is written into it, else as one. */
    private static int size(SqlExpr e, List<Def> defs) {
        Def d = refOf(e, defs);
        if (d != null) {
            return d.own() || d.base ? 1 : d.size;
        }
        int n = 1;
        for (SqlExpr c : parts(e)) {
            n += size(c, defs);
        }
        return n;
    }

    /** {@code d} computed at {@code level}: each value it reads written into it there, or read as a column. */
    private static void computeAt(Def d, int level, List<Def> defs) {
        for (Def r : d.reads) {
            if (inline(r, level)) {
                computeAt(r, level, defs);
            } else {
                read(r, level, defs);
            }
        }
    }

    /** {@code d} read as a column at {@code level}: computed at its own, carried up to it. */
    private static void read(Def d, int level, List<Def> defs) {
        d.need = Math.max(d.need, level);
        if (!d.computed) {
            d.computed = true;
            computeAt(d, Math.max(1, d.level), defs);
        }
    }

    /** Whether {@code d} is written into a reader at {@code level}: an expression of that level, read there. */
    private static boolean inline(Def d, int level) {
        return !d.own() && (d.base ? level == 1 : d.level == level);
    }

    /** {@code d}'s expression at {@code level}: each value it reads written in, or its column of the level below. */
    private static SqlExpr write(Def d, int level, @Nullable String alias, List<Def> defs) {
        return new Write(level, alias, defs).apply(d.expr);
    }

    /** A read of {@code d}'s column in the level below, {@code alias}. */
    private static SqlExpr columnOf(Def d, @Nullable String alias) {
        OutputCol c = d.col;
        return c != null ? SqlExpr.Column.of(alias, c)
                : SqlExpr.Column.derived(alias, java.util.Objects.requireNonNull(d.column));
    }

    /** A level's reads of the level below, {@code alias}, each the value of the column it names. */
    private static final class Resolve extends SqlRewriter {
        private final String alias;
        private final Map<String, Def> below;
        boolean refused;

        Resolve(String alias, Map<String, Def> below) {
            this.alias = alias;
            this.below = below;
        }

        SqlExpr apply(SqlExpr e) {
            return rewriteExpr(e);
        }

        @Override
        protected SqlExpr expr(SqlExpr e) {
            if (e instanceof SqlExpr.Column c) {
                Def d = alias.equals(c.table()) ? below.get(c.name()) : null;
                if (d == null) {
                    refused = true;
                    return e;
                }
                return ref(d);
            }
            return e;
        }
    }

    /** Each column of the bottom's source a value of its own, read where it was. */
    private static final class Bases extends SqlRewriter {
        private final List<Def> defs;
        private final Map<SqlExpr, Def> byExpr;

        Bases(List<Def> defs, Map<SqlExpr, Def> byExpr) {
            this.defs = defs;
            this.byExpr = byExpr;
        }

        SqlExpr apply(SqlExpr e) {
            return rewriteExpr(e);
        }

        @Override
        protected SqlExpr expr(SqlExpr e) {
            if (e instanceof SqlExpr.Column c) {
                Def d = def(c, false, defs, byExpr);
                if (d.name == null) {
                    d.name = c.name();
                }
                return ref(d);
            }
            return e;
        }
    }

    /** Each window call a value of its own, read where it was. */
    private static final class Windows extends SqlRewriter {
        private final List<Def> defs;
        private final Map<SqlExpr, Def> byExpr;

        Windows(List<Def> defs, Map<SqlExpr, Def> byExpr) {
            this.defs = defs;
            this.byExpr = byExpr;
        }

        SqlExpr apply(SqlExpr e) {
            return rewriteExpr(e);
        }

        @Override
        protected SqlExpr expr(SqlExpr e) {
            if (!(e instanceof SqlExpr.WindowCall)) {
                return e;
            }
            // one window whichever spelling of its reads a level wrote: known by its expanded form, written as the
            // first spelling met (its reads' values kept, so their budget holds: INLINE)
            SqlExpr key = expanded(e, defs);
            Def known = byExpr.get(key);
            if (known == null) {
                known = def(e, true, defs, byExpr);
                byExpr.putIfAbsent(key, known);
            }
            return ref(known);
        }
    }

    /** {@code e} with each read of a value that is an expression -- no window, no column of the source, raising
     *  nothing -- that value's expression, again within it: a window's one form whichever level wrote it, its reads
     *  named by a column above or written out below (a meeting's date: guide-12's hedge_ratio and residual, W4
     *  2026-11-06), so the two are one value. Only a window's identity: it is written as it was. */
    private static SqlExpr expanded(SqlExpr e, List<Def> defs) {
        return new Expand(defs).apply(e);
    }

    /** {@link #expanded}'s walk. */
    private static final class Expand extends SqlRewriter {
        private final List<Def> defs;

        Expand(List<Def> defs) {
            this.defs = defs;
        }

        SqlExpr apply(SqlExpr e) {
            return rewriteExpr(e);
        }

        @Override
        protected SqlExpr expr(SqlExpr x) {
            Def d = refOf(x, defs);
            return d != null && !d.window && !d.base && !d.raises ? apply(d.expr) : x;
        }
    }

    /** A value's expression at a level: each value it reads written in where it is that level's, else its column. */
    private static final class Write extends SqlRewriter {
        private final int level;
        private final @Nullable String alias;
        private final List<Def> defs;

        Write(int level, @Nullable String alias, List<Def> defs) {
            this.level = level;
            this.alias = alias;
            this.defs = defs;
        }

        SqlExpr apply(SqlExpr e) {
            return rewriteExpr(e);
        }

        @Override
        protected SqlExpr expr(SqlExpr e) {
            Def d = refOf(e, defs);
            if (d == null) {
                return e;
            }
            return inline(d, level) ? apply(d.expr) : columnOf(d, alias);
        }
    }
}
