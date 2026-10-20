// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql.iq;

import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CFloat;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.sql.InputDialect;
import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlToPure;
import com.legend.protocol.sql.SqlToPure.Kind;
import com.legend.protocol.sql.SqlToPure.Tx;
import com.legend.protocol.sql.SqlTranslationException;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

/**
 * IQ's window functions as the {@code #IQ{...}#} island reads them (IQ island, 2026-10-15, landing 4; the research's
 * GRAMMAR F30-F41 and G9, SEMANTICS §3.8). Every rule below was measured on the user's SAP IQ 17.1 (the IQ engine,
 * over its tables) and on SQL Anywhere 17 (its front end); where the two differ, the IQ engine's is read under it.
 * <ul>
 *   <li><b>Placement.</b> A window function stands in the select list and in ORDER BY. In WHERE or a join's ON it is
 *       IQ's -964; in GROUP BY -156; in HAVING -156 for one of no argument (a ranking function, {@code COUNT(*)}) and
 *       -964 for any other; inside another window function's arguments or window, or inside an aggregate, -156. In an
 *       IN, ANY or ALL subquery (or a derived table inside one) the IQ engine raises -1001030; a scalar or EXISTS
 *       subquery, a derived table and a common table expression take one.</li>
 *   <li><b>The window.</b> {@code OVER (...)} or {@code OVER name}, {@code WINDOW name AS (...)}: a reference names a
 *       window of its SELECT (-947 where none has the name, as IQ matches names; the first of two of one name), adds
 *       no PARTITION BY (-948), no ORDER BY to one that has one (-949), no frame to one that has one (-950). A key of
 *       PARTITION BY or ORDER BY is an expression; a bare integer constant there is -152 (a negative or computed
 *       constant is one partition). NULLs sort first ascending, last descending. A string key sorts and partitions by
 *       IQ's collation ({@link IqDialect#key}: {@code 'a'} and {@code 'a '} are one value).</li>
 *   <li><b>Ranking</b> -- ROW_NUMBER, RANK, DENSE_RANK (UNSIGNED BIGINT), PERCENT_RANK, CUME_DIST (DOUBLE) and
 *       NTILE(n) (UNSIGNED INT): no arguments (NTILE one), else -154; a window ORDER BY (-1331) of expressions over a
 *       column (a constant is -1005028), no frame (-950, -1330 through a named window). CUME_DIST of more than one
 *       key is the IQ engine's -1005028. NTILE's n is an integer constant from 1 to 32767 (-1005028 else; a computed
 *       one is refused by name). Ties share a rank; NTILE's buckets differ by at most one, the larger first.</li>
 *   <li><b>LAG and LEAD</b>{@code (x [, n [, default]])}: the IQ engine's own (SQL Anywhere 17 knows neither: its
 *       -131). n is an integer literal from 0 to 2^31 - 1 (0 is the current row; anything else -1005028), the default a
 *       constant (-1005028) converted to x's type, NULL when not given; a window ORDER BY (-1331), no frame (-950).
 *       The type is x's.</li>
 *   <li><b>FIRST_VALUE and LAST_VALUE</b>{@code (x [RESPECT NULLS])}: the frame's first and last row's x, x's type. A
 *       window without ORDER BY is the whole partition (its first row is the plan's choice).</li>
 *   <li><b>Aggregates over a window</b> -- COUNT, SUM, AVG, MIN, MAX: no DISTINCT (-134), LIST -134, MEDIAN refused
 *       by name (its integer midpoint wraps, measured). COUNT is an UNSIGNED BIGINT; under the IQ engine SUM of a
 *       SMALLINT is a BIGINT, of an INTEGER or a BIGINT a NUMERIC(20,0) (no overflow: 2^63 - 1 + 25 is answered),
 *       of a BIT an UNSIGNED BIGINT, of a NUMERIC(p,s) a NUMERIC of scale s, of a DOUBLE a DOUBLE; AVG of an integer
 *       a NUMERIC of scale 16, of a NUMERIC(p,s) one of scale s + 21, each cut toward zero there (5 / 3 is
 *       1.6666666666666666), of a DOUBLE a DOUBLE; a DOUBLE's SUM or AVG past its range is -1001010. MIN and MAX keep
 *       x's type; of strings only where each value is its own collation key ({@link IqKeys}), and not over a frame that
 *       starts after the partition's first row (IQ 17.1 answers a VARCHAR's MIN or MAX over a moving frame with NUL
 *       bytes in it: its defect, refused by name).</li>
 *   <li><b>Frames.</b> ROWS or RANGE, {@code start} or {@code BETWEEN start AND end} ({@link IqParser}'s syntax
 *       errors). Without a frame a window with ORDER BY is RANGE UNBOUNDED PRECEDING to the current row's last peer,
 *       one without the whole partition. Under the IQ engine a RANGE frame, written or not, needs one ORDER BY key
 *       (-966 for a written RANGE over several, -1005028 for an unwritten one) of a number (a string, a date or a
 *       timestamp is -1005028); a written RANGE needs an ORDER BY (-1331). An offset is an unsigned integer constant: a
 *       column there is -1005028, as are 0, a negative number and NULL; a computed or fractional one is refused by
 *       name. A frame whose end comes before its start ({@code 1 PRECEDING AND 2 PRECEDING}) is the IQ engine's
 *       -1005028; an empty frame's SUM, AVG, MIN, MAX and FIRST_VALUE are NULL, its COUNT 0.</li>
 *   <li><b>SQL Anywhere's front end</b> (a statement over no IQ table, a {@code sqla17} profile) reads the window
 *       functions SQL Anywhere 17 has, with the rules it shares with the IQ engine: SUM and AVG of an integer and AVG
 *       of a NUMERIC (whose types and overflow checks are its own) are refused by name there, LAG, LEAD and NTILE are
 *       SQL Anywhere's -131 in a {@code sqla17} profile and refused by name in an IQ database's front end (IQ 17.1
 *       hangs on them over {@code sa_rowgenerator}, so they have no oracle).</li>
 * </ul>
 */
final class IqWindows {

    private IqWindows() {
    }

    /** The ranking functions: no argument, a window ORDER BY, no frame. */
    private static final Set<String> RANKING = Set.of("row_number", "rank", "dense_rank", "percent_rank", "cume_dist");

    /** The aggregates read over a window (MEDIAN and LIST are refused there). */
    private static final Set<String> AGGREGATES = Set.of("count", "sum", "avg", "min", "max");

    /** The largest LAG and LEAD offset IQ reads (measured: 3000000000 is its -1005028). */
    private static final BigInteger MAX_OFFSET = BigInteger.valueOf(Integer.MAX_VALUE);

    /** The largest NTILE IQ reads (measured). */
    private static final BigInteger MAX_NTILE = BigInteger.valueOf(32767);

    private static String code(int sqlcode, String sqlstate) {
        return IqDialect.code(sqlcode, sqlstate);
    }

    private static SqlTranslationException iq(String message, int sqlcode, String sqlstate, SqlAst.Pos pos) {
        return SqlTranslationException.invalid(message + code(sqlcode, sqlstate), pos);
    }

    // ------------------------------------------------------------------ the tree

    /** What a window call reads: its arguments, its window's keys and its frame's offsets (not its call: a windowed
     *  aggregate is no aggregate of its query's grouping, nor a function of the catalog). */
    static List<SqlAst.Expr> children(SqlAst.Window w) {
        List<SqlAst.Expr> all = new ArrayList<>(w.call().args());
        all.addAll(w.spec().partitionBy());
        w.spec().orderBy().forEach(o -> all.add(o.expr()));
        SqlAst.Frame f = w.spec().frame();
        if (f != null) {
            for (SqlAst.FrameBound b : List.of(f.start(), f.end())) {
                if (b.offset() != null) {
                    all.add(java.util.Objects.requireNonNull(b.offset()));
                }
            }
        }
        return all;
    }

    /** {@code w} with {@code f} applied to its arguments and its window's keys ({@code w} itself when none changes);
     *  {@code copied} keeps the reader's texts with a new call or window. */
    static SqlAst.Expr rebuilt(SqlAst.Window w, UnaryOperator<SqlAst.Expr> f, BinaryOperator<SqlAst.Expr> copied) {
        SqlAst.FunctionCall c = w.call();
        List<SqlAst.Expr> args = new ArrayList<>();
        boolean same = true;
        for (SqlAst.Expr a : c.args()) {
            SqlAst.Expr x = f.apply(a);
            same &= x == a;
            args.add(x);
        }
        List<SqlAst.Expr> partition = new ArrayList<>();
        for (SqlAst.Expr e : w.spec().partitionBy()) {
            SqlAst.Expr x = f.apply(e);
            same &= x == e;
            partition.add(x);
        }
        List<SqlAst.SortItem> order = new ArrayList<>();
        for (SqlAst.SortItem o : w.spec().orderBy()) {
            SqlAst.Expr x = f.apply(o.expr());
            same &= x == o.expr();
            order.add(x == o.expr() ? o : new SqlAst.SortItem(x, o.descending(), o.nullsFirst(), o.pos()));
        }
        if (same) {
            return w;
        }
        return copy(w, args, partition, order, copied);
    }

    /** A copy of {@code w}, node by node ({@code copy} copies each expression). */
    static SqlAst.Expr copied(SqlAst.Window w, UnaryOperator<SqlAst.Expr> copy, BinaryOperator<SqlAst.Expr> copied) {
        List<SqlAst.Expr> args = new ArrayList<>();
        w.call().args().forEach(a -> args.add(copy.apply(a)));
        List<SqlAst.Expr> partition = new ArrayList<>();
        w.spec().partitionBy().forEach(e -> partition.add(copy.apply(e)));
        List<SqlAst.SortItem> order = new ArrayList<>();
        w.spec().orderBy().forEach(o -> order.add(new SqlAst.SortItem(copy.apply(o.expr()), o.descending(),
                o.nullsFirst(), o.pos())));
        return copy(w, args, partition, order, copied);
    }

    private static SqlAst.Window copy(SqlAst.Window w, List<SqlAst.Expr> args, List<SqlAst.Expr> partition,
            List<SqlAst.SortItem> order, BinaryOperator<SqlAst.Expr> copied) {
        SqlAst.FunctionCall c = w.call();
        SqlAst.FunctionCall call = new SqlAst.FunctionCall(c.name(), args, c.distinct(), c.star(), c.orderBy(),
                c.withinGroup(), c.filter(), c.pos());
        copied.apply(c, call);
        SqlAst.WindowSpec s = w.spec();
        return new SqlAst.Window(call, new SqlAst.WindowSpec(s.base(), partition, order, s.frame(), s.pos()),
                w.pos());
    }

    /** The window calls under {@code e}, outermost first (not into subqueries, nor into a window call). */
    static List<SqlAst.Window> windows(SqlAst.Expr e) {
        List<SqlAst.Window> out = new ArrayList<>();
        windows(e, out);
        return out;
    }

    private static void windows(SqlAst.Expr e, List<SqlAst.Window> out) {
        if (e instanceof SqlAst.Window w) {
            out.add(w);
            return;
        }
        for (SqlAst.Expr child : IqDialect.children(e)) {
            windows(child, out);
        }
    }

    // ------------------------------------------------------------------ IQ's errors, before translating

    /**
     * The errors IQ raises for {@code s}'s window functions before running it, and what is refused there by name
     * ({@link IqWindows}), under {@code engine} and {@code settings}.
     */
    static void parsed(SqlAst.Select s, IqDialect.Engine engine, IqSettings settings, Set<SqlAst.Window> numbering) {
        List<SqlAst.Expr> predicates = new ArrayList<>();
        if (s.where() != null) {
            predicates.add(java.util.Objects.requireNonNull(s.where()));
        }
        for (SqlAst.FromItem f : s.from()) {
            ons(f, predicates);
        }
        for (SqlAst.Expr p : predicates) {
            List<SqlAst.Window> found = windows(p);
            if (!found.isEmpty()) {
                throw iq("a window function cannot stand in WHERE or ON", -964, "42WA8", found.get(0).pos());
            }
        }
        for (SqlAst.Expr g : s.groupBy()) {
            List<SqlAst.Window> found = windows(g);
            if (!found.isEmpty()) {
                throw iq("a window function cannot stand in GROUP BY", -156, "42W08", found.get(0).pos());
            }
        }
        SqlAst.Expr having = s.having();
        if (having != null) {
            List<SqlAst.Window> found = windows(having);
            if (!found.isEmpty()) {
                SqlAst.FunctionCall c = found.get(0).call();
                boolean bare = c.star() || c.args().isEmpty();
                throw bare ? iq("a window function cannot stand in HAVING", -156, "42W08", found.get(0).pos())
                        : iq("a window function cannot stand in HAVING", -964, "42WA8", found.get(0).pos());
            }
        }
        List<SqlAst.Expr> placed = new ArrayList<>();
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x) {
                placed.add(x.expr());
            }
        }
        s.orderBy().forEach(o -> placed.add(o.expr()));
        for (SqlAst.Expr e : placed) {
            inAggregates(e, false);
            for (SqlAst.Window w : windows(e)) {
                for (SqlAst.Expr inner : children(w)) {
                    if (!windows(inner).isEmpty()) {
                        throw iq("a window function cannot stand inside another", -156, "42W08", inner.pos());
                    }
                }
                // NUMBER(*)'s window is the island's own, not one IQ reads (IqDialect.numbered)
                if (!numbering.contains(w)) {
                    window(w, s, engine, settings);
                }
            }
        }
    }

    /** A window call inside an aggregate's arguments ({@code SUM(ROW_NUMBER() OVER (...))}): IQ's -156. */
    private static void inAggregates(SqlAst.Expr e, boolean inside) {
        if (e instanceof SqlAst.Window && inside) {
            throw iq("a window function cannot stand inside an aggregate", -156, "42W08", e.pos());
        }
        boolean aggregate = e instanceof SqlAst.FunctionCall f && IqAggregates.named(f);
        for (SqlAst.Expr child : IqDialect.children(e)) {
            inAggregates(child, inside || aggregate);
        }
    }

    private static void ons(SqlAst.FromItem item, List<SqlAst.Expr> out) {
        if (item instanceof SqlAst.Join j) {
            ons(j.left(), out);
            ons(j.right(), out);
            if (j.on() != null) {
                out.add(java.util.Objects.requireNonNull(j.on()));
            }
        }
    }

    /** IQ's errors for one window call {@code w} of {@code s}. */
    private static void window(SqlAst.Window w, SqlAst.Select s, IqDialect.Engine engine, IqSettings settings) {
        SqlAst.FunctionCall f = w.call();
        String fn = IqLexer.lower(f.name().get(0).text());
        String upper = fn.toUpperCase(Locale.ROOT);
        Set<String> own = new java.util.HashSet<>();
        for (SqlAst.FromItem item : s.from()) {
            for (SqlAst.FromItem leaf : IqDialect.leaves(item)) {
                SqlAst.Ident n = correlationName(leaf);
                if (n != null) {
                    own.add(IqLexer.lower(n.text()));
                }
            }
        }
        for (SqlAst.Expr e : children(w)) {
            for (SqlAst.ColumnRef r : columns(e)) {
                if (r.parts().size() == 2 && !own.contains(IqLexer.lower(r.parts().get(0).text()))) {
                    throw outer(r);
                }
            }
        }
        boolean iqEngine = engine == IqDialect.Engine.IQ;
        Resolved r = resolve(w.spec(), s.windows(), 0);
        SqlAst.WindowSpec spec = r.spec();
        boolean framed = spec.frame() != null;
        for (SqlAst.Expr k : spec.partitionBy()) {
            position(k);
            subqueryKey(f, k);
        }
        for (SqlAst.SortItem o : spec.orderBy()) {
            position(o.expr());
            subqueryKey(f, o.expr());
        }
        if (fn.equals("median")) {
            throw SqlTranslationException.unsupported("MEDIAN over a window is not supported: IQ computes its integer"
                    + " midpoint in a type it can leave (measured: the MEDIAN of 2147483647 and 1 is -1073741824)",
                    f.pos());
        }
        if (fn.equals("list")) {
            throw iq("LIST is no window function", -134, "0A000", f.pos());
        }
        if (AGGREGATES.contains(fn)) {
            if (f.distinct()) {
                throw iq("DISTINCT is not allowed in a window function", -134, "0A000", f.pos());
            }
            boolean arity = fn.equals("count") ? f.star() || f.args().size() == 1 : !f.star() && f.args().size() == 1;
            if (!arity) {
                throw wrongArguments(f);
            }
        } else if (RANKING.contains(fn)) {
            if (!f.args().isEmpty()) {
                throw wrongArguments(f);
            }
            ranked(f, upper, spec, r, iqEngine);
            if (fn.equals("cume_dist") && spec.orderBy().size() > 1 && iqEngine) {
                throw iq("CUME_DIST takes one ORDER BY key under IQ's engine", -1005028, "QTA28", f.pos());
            }
        } else if (fn.equals("ntile")) {
            iqOnly(f, upper, engine, settings);
            if (f.args().size() != 1) {
                throw wrongArguments(f);
            }
            ranked(f, upper, spec, r, true);
            ntile(f.args().get(0));
        } else if (fn.equals("lag") || fn.equals("lead")) {
            iqOnly(f, upper, engine, settings);
            if (f.args().isEmpty() || f.args().size() > 3) {
                throw wrongArguments(f);
            }
            ordered(f, upper, spec);
            if (framed) {
                throw frameNotAllowed(f, upper, r, iqEngine);
            }
            if (f.args().size() >= 2) {
                offset(f.args().get(1));
            }
            if (f.args().size() == 3 && !columns(f.args().get(2)).isEmpty()) {
                throw iq("the default of LAG and LEAD is a constant", -1005028, "QTA28", f.args().get(2).pos());
            }
            if (f.args().size() == 3 && hasSubquery(f.args().get(2))) {
                throw SqlTranslationException.unsupported("a subquery as the default of " + upper + " is not supported",
                        f.args().get(2).pos());
            }
        } else if (fn.equals("first_value") || fn.equals("last_value")) {
            if (f.args().size() != 1) {
                throw wrongArguments(f);
            }
        } else {
            throw SqlTranslationException.unsupported(upper + "(...) OVER (...) is not supported", f.pos());
        }
        SqlAst.Frame frame = spec.frame();
        if (frame != null) {
            frame(f, upper, frame, spec, iqEngine);
        }
    }

    /** A window function reading an enclosing query's column (measured on IQ 17.1: -156 or -862), refused by name. */
    static SqlTranslationException outer(SqlAst.ColumnRef r) {
        return SqlTranslationException.unsupported("a window function reading an enclosing query's column ("
                + String.join(".", r.parts().stream().map(SqlAst.Ident::text).toList()) + ") is not supported (IQ"
                + " 17.1 rejects it: -156, -862)", r.pos());
    }

    /** The name a FROM leaf is known by in its query: its alias, else its own (a table's last part); null for none. */
    private static SqlAst.@com.legend.base.Nullable Ident correlationName(SqlAst.FromItem leaf) {
        return switch (leaf) {
            case SqlAst.TableName t -> t.alias() != null ? t.alias() : t.name().get(t.name().size() - 1);
            case SqlAst.TableFunction t -> t.alias() != null ? t.alias() : t.name().get(t.name().size() - 1);
            case SqlAst.SubqueryFrom q -> q.alias();
            case SqlAst.Join j -> null;
        };
    }

    /** The window {@code spec} written with its named base's clauses in place (IQ's rules: the class's), and whether
     *  its frame came from the base. */
    private record Resolved(SqlAst.WindowSpec spec, boolean namedFrame) {
    }

    private static Resolved resolve(SqlAst.WindowSpec spec, List<SqlAst.NamedWindow> windows, int depth) {
        SqlAst.Ident base = spec.base();
        if (base == null) {
            return new Resolved(spec, false);
        }
        SqlAst.NamedWindow named = null;
        for (SqlAst.NamedWindow n : windows) {
            // IQ's names match without regard to ASCII case, quoted or not (S51; 2026-10-16, the landing's review:
            // WINDOW "W" read by OVER w was its -947)
            if (named == null && IqLexer.lower(n.name().text()).equals(IqLexer.lower(base.text()))) {
                named = n;
            }
        }
        if (named == null || depth > windows.size()) {
            throw iq("window '" + base.text() + "' not found", -947, "42WA3", base.pos());
        }
        if (named.spec().base() != null) {
            // measured on IQ 17.1: WINDOW w1 AS (PARTITION BY k), w2 AS (w1 ORDER BY id) orders by id over the whole
            // result -- w1's PARTITION BY dropped, where OVER (w1 ORDER BY id) keeps it
            throw SqlTranslationException.unsupported("a named window built on another named window ("
                    + named.name().text() + " on " + java.util.Objects.requireNonNull(named.spec().base()).text()
                    + ") is not supported: IQ 17.1 drops the other window's PARTITION BY there", named.name().pos());
        }
        Resolved under = resolve(named.spec(), windows, depth + 1);
        SqlAst.WindowSpec b = under.spec();
        if (!spec.partitionBy().isEmpty()) {
            throw iq("a reference to window '" + base.text() + "' adds no PARTITION BY", -948, "42WA4", spec.pos());
        }
        if (!spec.orderBy().isEmpty() && !b.orderBy().isEmpty()) {
            throw iq("window '" + base.text() + "' has an ORDER BY", -949, "42WA5", spec.pos());
        }
        if (spec.frame() != null && b.frame() != null) {
            throw iq("window '" + base.text() + "' has a frame", -950, "42WA6", spec.pos());
        }
        boolean namedFrame = spec.frame() == null && b.frame() != null;
        return new Resolved(new SqlAst.WindowSpec(null, b.partitionBy(), spec.orderBy().isEmpty() ? b.orderBy()
                : spec.orderBy(), spec.frame() != null ? spec.frame() : b.frame(), spec.pos()), namedFrame);
    }

    /** A bare integer constant as a window's key: IQ's -152 (no position there). */
    private static void position(SqlAst.Expr k) {
        if (k instanceof SqlAst.IntegerLit) {
            throw iq("a window's key is an expression, not a position", -152, "53005", k.pos());
        }
    }

    /** IQ island, 2026-10-17 (the claim verification's SEDE ROW_NUMBER() OVER (ORDER BY (SELECT NULL)); measured on IQ
     *  17.1, PARTITION BY too, an aggregate's window too, with or without a FROM): a subquery in a window's key is IQ's
     *  -156, an invalid expression near the function. */
    private static void subqueryKey(SqlAst.FunctionCall f, SqlAst.Expr k) {
        if (hasSubquery(k)) {
            throw iq("a subquery in a window's key is an invalid expression near " + f.name().get(f.name().size() - 1)
                    .text().toUpperCase(Locale.ROOT), -156, "42W08", k.pos());
        }
    }

    private static SqlTranslationException wrongArguments(SqlAst.FunctionCall f) {
        return iq("wrong number of arguments to " + f.name().get(0).text().toUpperCase(Locale.ROOT), -154, "37505",
                f.pos());
    }

    /** A ranking function's window (and NTILE's): an ORDER BY of expressions over a column, no frame. */
    private static void ranked(SqlAst.FunctionCall f, String upper, SqlAst.WindowSpec spec, Resolved r,
            boolean iqEngine) {
        ordered(f, upper, spec);
        if (spec.frame() != null) {
            throw frameNotAllowed(f, upper, r, iqEngine);
        }
        // IQ island, 2026-10-17 (the differential fuzzer; measured on IQ 17.1): IQ's -1005028 where no key reads a
        // column; a constant key beside one that does is answered (ORDER BY MOD(5, 2), id)
        boolean anyColumn = false;
        for (SqlAst.SortItem o : spec.orderBy()) {
            anyColumn |= !columns(o.expr()).isEmpty() || !IqAggregates.aggregateCalls(o.expr()).isEmpty();
        }
        if (!anyColumn) {
            throw iq(upper + "'s ORDER BY keys are expressions over a column", -1005028, "QTA28",
                    spec.orderBy().get(0).expr().pos());
        }
    }

    private static void ordered(SqlAst.FunctionCall f, String upper, SqlAst.WindowSpec spec) {
        if (spec.orderBy().isEmpty()) {
            throw iq("the function '" + upper + "' needs the window's ORDER BY", -1331, "42WE1", f.pos());
        }
    }

    /** A frame where the function takes none: through a named window IQ's -1330, written inline the IQ engine's -950
     *  (SQL Anywhere's -1330). */
    private static SqlTranslationException frameNotAllowed(SqlAst.FunctionCall f, String upper, Resolved r,
            boolean iqEngine) {
        return r.namedFrame() || !iqEngine
                ? iq("the function '" + upper + "' takes no RANGE or ROWS frame", -1330, "42WE0", f.pos())
                : iq("the function '" + upper + "' takes no frame", -950, "42WA6", f.pos());
    }

    /** LAG, LEAD and NTILE are the IQ engine's: SQL Anywhere 17 knows none of them (its -131 in a {@code sqla17}
     *  profile); in an IQ database's front end they are refused by name (IQ 17.1 hangs on them over sa_rowgenerator,
     *  so they have no oracle there). */
    private static void iqOnly(SqlAst.FunctionCall f, String upper, IqDialect.Engine engine, IqSettings settings) {
        if (engine == IqDialect.Engine.IQ) {
            return;
        }
        if (!settings.iqDatabase()) {
            throw new SqlTranslationException(SqlTranslationException.Kind.SYNTAX, "SQL Anywhere has no " + upper
                    + " window function" + IqDialect.syntaxCode(), f.pos().line(), f.pos().column());
        }
        throw SqlTranslationException.unsupported(upper + " over no IQ table (in SQL Anywhere's front end) is not"
                + " supported: SQL Anywhere 17 has no " + upper + ", and IQ 17.1 hangs on it over sa_rowgenerator",
                f.pos());
    }

    /** NTILE's argument: an integer constant from 1 to 32767 (IQ's -1005028 else; a computed constant refused). */
    private static void ntile(SqlAst.Expr n) {
        BigInteger v = literal(n);
        if (v != null) {
            if (v.signum() <= 0 || v.compareTo(MAX_NTILE) > 0) {
                throw iq("NTILE's argument is an integer from 1 to 32767", -1005028, "QTA28", n.pos());
            }
            return;
        }
        if (!columns(n).isEmpty()) {
            throw iq("NTILE's argument is a constant integer", -1005028, "QTA28", n.pos());
        }
        if (n instanceof SqlAst.NullLit || n instanceof SqlAst.NumericLit || n instanceof SqlAst.StringLit
                || n instanceof SqlAst.Unary u && u.operand() instanceof SqlAst.IntegerLit) {
            throw iq("NTILE's argument is an integer from 1 to 32767", -1005028, "QTA28", n.pos());
        }
        throw SqlTranslationException.unsupported("NTILE of a computed constant is not supported: write its value",
                n.pos());
    }

    /** LAG's and LEAD's offset: an integer literal from 0 to 2^31 - 1 (IQ's -1005028 else, measured: a computed
     *  constant too). */
    private static void offset(SqlAst.Expr n) {
        BigInteger v = literal(n);
        if (v != null && v.signum() >= 0 && v.compareTo(MAX_OFFSET) <= 0 && !(n instanceof SqlAst.Unary u
                && u.op() == SqlAst.UnaryOp.MINUS)) {
            return;
        }
        if (v != null && v.compareTo(MAX_OFFSET) > 0) {
            throw iq("LAG's and LEAD's offset is out of range", -1005028, "QTA28", n.pos());
        }
        throw iq("LAG's and LEAD's offset is a constant integer, 0 or more", -1005028, "QTA28", n.pos());
    }

    /** An integer literal's value, a sign {@code +} before it read (IQ reads {@code +1} as 1); null for anything
     *  else. A {@code -} before one is its negative. */
    private static @com.legend.base.Nullable BigInteger literal(SqlAst.Expr e) {
        if (e instanceof SqlAst.IntegerLit n) {
            return n.value();
        }
        if (e instanceof SqlAst.Unary u && u.operand() instanceof SqlAst.IntegerLit n) {
            return u.op() == SqlAst.UnaryOp.PLUS ? n.value() : u.op() == SqlAst.UnaryOp.MINUS ? n.value().negate()
                    : null;
        }
        return null;
    }

    /** A frame's errors: its offsets (IQ's -1005028 for a column, NULL, 0 or a negative number; a computed or
     *  fractional one refused by name), RANGE's ORDER BY (-1331 without one, -966 over several), and, under the IQ
     *  engine, one ending before it starts (-1005028). */
    private static void frame(SqlAst.FunctionCall f, String upper, SqlAst.Frame frame, SqlAst.WindowSpec spec,
            boolean iqEngine) {
        SqlAst.BoundKind first = frame.start().kind();
        SqlAst.BoundKind last = frame.end().kind();
        boolean backwards = first == SqlAst.BoundKind.UNBOUNDED_FOLLOWING || last == SqlAst.BoundKind.UNBOUNDED_PRECEDING
                || first == SqlAst.BoundKind.CURRENT_ROW && last == SqlAst.BoundKind.PRECEDING
                || first == SqlAst.BoundKind.FOLLOWING && (last == SqlAst.BoundKind.PRECEDING
                || last == SqlAst.BoundKind.CURRENT_ROW);
        if (backwards) {
            // measured on IQ 17.1: each of these is its syntax error -131 (SQL Anywhere reads some of them)
            if (iqEngine) {
                throw new SqlTranslationException(SqlTranslationException.Kind.SYNTAX, "a frame cannot end before it"
                        + " starts" + IqDialect.syntaxCode(), frame.pos().line(), frame.pos().column());
            }
            throw SqlTranslationException.unsupported("a frame that ends before it starts is not supported in SQL"
                    + " Anywhere's front end", frame.pos());
        }
        BigInteger[] offsets = new BigInteger[2];
        List<SqlAst.FrameBound> bounds = List.of(frame.start(), frame.end());
        for (int i = 0; i < 2; i++) {
            SqlAst.Expr o = bounds.get(i).offset();
            if (o == null) {
                continue;
            }
            String which = i == 0 ? "lower" : "upper";
            if (!columns(o).isEmpty()) {
                throw iq("a frame's bound is a constant", -1005028, "QTA28", o.pos());
            }
            if (o instanceof SqlAst.NullLit) {
                throw iq("the window's " + which + " bound is a number", -1005028, "QTA28", o.pos());
            }
            BigInteger v = literal(o);
            if (v != null && (v.signum() < 0 || v.signum() == 0 && iqEngine)) {
                // measured on IQ 17.1: 0 is its -1005028 too (SQL Anywhere reads it as the current row; a negative
                // offset is its -957 at run time)
                if (!iqEngine) {
                    throw SqlTranslationException.unsupported("a negative frame offset is not supported", o.pos());
                }
                throw iq("the window's " + which + " bound is an unsigned number", -1005028, "QTA28", o.pos());
            }
            if (!(o instanceof SqlAst.IntegerLit) || v == null || v.bitLength() > 31) {
                throw SqlTranslationException.unsupported("a frame offset other than an integer constant up to"
                        + " 2147483647 is not supported (IQ reads a computed one, and cuts a fraction)", o.pos());
            }
            offsets[i] = v;
        }
        if (frame.unit() == SqlAst.FrameUnit.RANGE) {
            if (spec.orderBy().isEmpty()) {
                throw iq("the function '" + upper + "' needs the window's ORDER BY for RANGE", -1331, "42WE1",
                        f.pos());
            }
            if (spec.orderBy().size() > 1) {
                throw iq("RANGE takes one ORDER BY key", -966, "42WAA", frame.pos());
            }
        }
        boolean inverted = first == SqlAst.BoundKind.PRECEDING && last == SqlAst.BoundKind.PRECEDING
                && offsets[0] != null && offsets[1] != null && offsets[0].compareTo(offsets[1]) < 0
                || first == SqlAst.BoundKind.FOLLOWING && last == SqlAst.BoundKind.FOLLOWING && offsets[0] != null
                && offsets[1] != null && offsets[0].compareTo(offsets[1]) > 0;
        if (inverted) {
            if (iqEngine) {
                throw iq("the window's upper bound comes before its lower bound", -1005028, "QTA28", frame.pos());
            }
            throw SqlTranslationException.unsupported("a frame that ends before it starts is not supported in SQL"
                    + " Anywhere's front end", frame.pos());
        }
    }

    /** The column references under {@code e} (not into subqueries). */
    private static List<SqlAst.ColumnRef> columns(SqlAst.Expr e) {
        List<SqlAst.ColumnRef> out = new ArrayList<>();
        columns(e, out);
        return out;
    }

    private static void columns(SqlAst.Expr e, List<SqlAst.ColumnRef> out) {
        if (e instanceof SqlAst.ColumnRef r) {
            out.add(r);
        }
        for (SqlAst.Expr child : IqDialect.children(e)) {
            columns(child, out);
        }
    }

    /** The column references under {@code e} outside aggregates (a grouped query's aggregate reads its group's rows)
     *  and subqueries. */
    private static List<SqlAst.ColumnRef> columnsOutsideAggregates(SqlAst.Expr e) {
        List<SqlAst.ColumnRef> out = new ArrayList<>();
        if (e instanceof SqlAst.ColumnRef r) {
            out.add(r);
        }
        if (e instanceof SqlAst.FunctionCall f && IqAggregates.named(f)) {
            return out;
        }
        for (SqlAst.Expr child : IqDialect.children(e)) {
            out.addAll(columnsOutsideAggregates(child));
        }
        return out;
    }

    private static boolean hasSubquery(SqlAst.Expr e) {
        if (e instanceof SqlAst.ScalarSubquery || e instanceof SqlAst.Exists || e instanceof SqlAst.InSubquery
                || e instanceof SqlAst.Quantified) {
            return true;
        }
        for (SqlAst.Expr child : IqDialect.children(e)) {
            if (hasSubquery(child)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The IQ engine's -1001030 for a window function in an IN, ANY or ALL subquery (or in a derived table or a subquery
     * inside one), anywhere in {@code statement}. IQ island, 2026-10-16 (the corpus lane: 8 of SQLStorm's queries; measured on IQ 17.1): under IQ's engine a window
     * function in a subquery of a condition -- a WHERE's, a HAVING's or an ON's IN, EXISTS, ANY, ALL or comparison
     * operand -- or in a derived table or a common table expression such a subquery reads, at any depth, is its error
     * -1001030 ("RANK/NTILE/Window Aggregate nested in a subquery"). A scalar subquery of a select list may hold one,
     * and so may a derived table of the FROM clause and an EXISTS's own select list (never computed).
     */
    static void statement(SqlAst.Statement statement, IqDialect.Engine engine) {
        if (engine != IqDialect.Engine.IQ) {
            return;
        }
        for (SqlAst.Statement q : IqDialect.queries(statement)) {
            if (q instanceof SqlAst.Select s) {
                for (SqlAst.Expr e : IqDialect.conditions(s)) {
                    conditionSubqueries(e);
                }
            }
        }
    }

    private static void conditionSubqueries(SqlAst.Expr e) {
        SqlAst.Statement q = IqDialect.subqueryOf(e);
        if (q != null) {
            for (SqlAst.Statement inner : IqDialect.queries(q)) {
                if (!(inner instanceof SqlAst.Select s)) {
                    continue;
                }
                List<SqlAst.Expr> read = new java.util.ArrayList<>(IqDialect.expressions(s));
                if (e instanceof SqlAst.Exists && inner == q) {
                    // an EXISTS's own select list is never computed (measured: a window there is answered)
                    for (SqlAst.SelectItem item : s.items()) {
                        if (item instanceof SqlAst.SelectExpr x) {
                            read.remove(x.expr());
                        }
                    }
                }
                for (SqlAst.Expr x : read) {
                    List<SqlAst.Window> found = windows(x);
                    if (!found.isEmpty()) {
                        throw iq("a window function in a subquery of a WHERE, a HAVING or an ON, or in a derived table"
                                + " or a common table expression it reads, is not supported by IQ's engine", -1001030,
                                "QFA2A", found.get(0).pos());
                    }
                }
            }
        }
        for (SqlAst.Expr child : IqDialect.children(e)) {
            conditionSubqueries(child);
        }
    }

    // ------------------------------------------------------------------ translation

    /** {@code f}, a window function's call, computed {@code at} its window under {@code dialect}'s engine (the
     *  class's rules; its errors before translating are {@link #parsed}'s). */
    static SqlToPure.WindowFunction window(SqlAst.FunctionCall f, SqlToPure.Windowing at, IqDialect dialect) {
        IqDialect.Engine engine = java.util.Objects.requireNonNull(dialect.engine(), "the statement's engine");
        boolean iqEngine = engine == IqDialect.Engine.IQ;
        String fn = IqLexer.lower(f.name().get(0).text());
        String upper = fn.toUpperCase(Locale.ROOT);
        SqlAst.WindowSpec spec = at.spec();
        List<SqlAst.Expr> read = new ArrayList<>(f.args());
        read.addAll(spec.partitionBy());
        spec.orderBy().forEach(o -> read.add(o.expr()));
        for (SqlAst.Expr e : read) {
            for (SqlAst.ColumnRef ref : columnsOutsideAggregates(e)) {
                if (at.enclosing(ref)) {
                    throw outer(ref);
                }
            }
        }
        if (iqEngine && (AGGREGATES.contains(fn) || fn.equals("first_value") || fn.equals("last_value"))) {
            rangeKey(f, spec, at.sortKeys());
        }
        ValueSpecification p = at.partition();
        ValueSpecification w = at.window();
        ValueSpecification r = at.row();
        Fn which = Fn.of(fn);
        if (which == null) {
            throw SqlTranslationException.unsupported(upper + "(...) OVER (...) is not supported", f.pos());
        }
        switch (which) {
            case ROW_NUMBER, RANK, DENSE_RANK -> {
                ValueSpecification body = fn.equals("row_number") ? SqlToPure.call("rowNumber", p, r)
                        : SqlToPure.call(fn.equals("rank") ? "rank" : "denseRank", p, w, r);
                return one(at, body, Kind.INTEGER, false, countType(engine));
            }
            case PERCENT_RANK, CUME_DIST -> {
                ValueSpecification body = SqlToPure.call(fn.equals("percent_rank") ? "percentRank"
                        : "cumulativeDistribution", p, w, r);
                return one(at, body, Kind.FLOAT, false, new IqType("DOUBLE"));
            }
            case NTILE -> {
                BigInteger n = java.util.Objects.requireNonNull(literal(f.args().get(0)));
                return one(at, SqlToPure.call("ntile", p, r, new CInteger(n.longValue())), Kind.INTEGER, false,
                        new IqType("UNSIGNED INT"));
            }
            case LAG, LEAD -> {
                return offsetValue(f, fn, upper, at, dialect);
            }
            case FIRST_VALUE, LAST_VALUE -> {
                Tx x = at.column(f.args().get(0));
                value(x, upper, f.pos());
                ValueSpecification row = SqlToPure.call(fn.equals("first_value") ? "first" : "last", p, w, r);
                return one(at, new AppliedProperty(row, name(x)), x.kind(), true, x.detail());
            }
            case COUNT -> {
                ValueSpecification counted = f.star() ? new CInteger(1L) : SqlToPure.valueOf(at.column(
                        f.args().get(0)));
                ColSpec spec1 = new ColSpec(at.name(), at.overWindow(counted), at.overGroup(SqlToPure.call("count",
                        at.values())), null);
                return new SqlToPure.WindowFunction(List.of(spec1), null, Kind.INTEGER, false, countType(engine));
            }
            case SUM, AVG -> {
                return sum(f, which == Fn.AVG, at, iqEngine);
            }
            case MIN, MAX -> {
                return minMax(f, fn, upper, at, dialect);
            }
        }
        throw new IllegalStateException("every window function is read above: " + which);
    }

    /** The window functions the island reads, by their names in lower case ({@link #of}). */
    private enum Fn {
        ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, COUNT, SUM,
        AVG, MIN, MAX;

        /** The function named {@code lower} (a name in lower case); null for any other. */
        static @com.legend.base.Nullable Fn of(String lower) {
            for (Fn x : values()) {
                if (x.name().toLowerCase(Locale.ROOT).equals(lower)) {
                    return x;
                }
            }
            return null;
        }
    }

    /** A function of one window column. */
    private static SqlToPure.WindowFunction one(SqlToPure.Windowing at, ValueSpecification body, Kind kind,
            boolean nullable, InputDialect.@com.legend.base.Nullable TypeDetail detail) {
        return new SqlToPure.WindowFunction(List.of(new ColSpec(at.name(), at.overWindow(body), null, null)), null,
                kind, nullable, detail);
    }

    /** COUNT's, ROW_NUMBER's, RANK's and DENSE_RANK's type: SQL Anywhere's INTEGER in its front end, IQ's UNSIGNED
     *  BIGINT under its engine (measured). */
    private static IqType countType(IqDialect.Engine engine) {
        return engine == IqDialect.Engine.IQ ? new IqType("UNSIGNED BIGINT")
                : IqType.integer("INTEGER", BigInteger.ZERO, IqType.INTEGER_MAX, true);
    }

    /** A NUMERIC(p,s) type's s. */
    private static int scaleOf(IqType t) {
        String sp = t.spelling();
        int comma = sp.indexOf(',');
        return comma < 0 ? 0 : Integer.parseInt(sp.substring(comma + 1, sp.indexOf(')')).strip());
    }

    /** The column a window's argument is ({@link SqlToPure.Windowing#column}). */
    private static String name(Tx column) {
        return ((AppliedProperty) column.vs()).property();
    }

    /** An argument whose value is read as it is (LAG's, FIRST_VALUE's): a value of a type the island reads. */
    private static void value(Tx x, String upper, SqlAst.Pos pos) {
        if (x.kind() == Kind.NULL || x.kind() == Kind.OTHER || x.kind() == Kind.JSON || x.kind() == Kind.BOOLEAN
                || !(x.detail() instanceof IqType)) {
            throw SqlTranslationException.unsupported(upper + " of a value of this type is not supported yet", pos);
        }
    }

    /** Under the IQ engine, an aggregate's (or FIRST_VALUE's, LAST_VALUE's) RANGE frame, written or the one a window
     *  ORDER BY implies: one ORDER BY key, a number (measured: -1005028 for several, or a string, a date, a
     *  timestamp; a BIT is a number). */
    private static void rangeKey(SqlAst.FunctionCall f, SqlAst.WindowSpec spec, List<Tx> keys) {
        SqlAst.Frame frame = spec.frame();
        boolean range = !spec.orderBy().isEmpty() && (frame == null || frame.unit() == SqlAst.FrameUnit.RANGE);
        if (!range) {
            return;
        }
        boolean wholeOrDefault = frame == null;
        if (wholeOrDefault && keys.size() > 1 || !keys.isEmpty() && !keys.get(0).kind().numeric()) {
            throw iq("the ORDER BY key of a RANGE frame (a window ORDER BY without ROWS) is one number under IQ's"
                    + " engine", -1005028, "QTA28", spec.orderBy().get(0).pos());
        }
    }

    /** LAG and LEAD: the other row's x; its default (converted to x's type) where that row is outside the partition --
     *  not where its x is NULL. A column that is never NULL (the constant 1) tells which. */
    private static SqlToPure.WindowFunction offsetValue(SqlAst.FunctionCall f, String fn, String upper,
            SqlToPure.Windowing at, IqDialect dialect) {
        Tx x = at.column(f.args().get(0));
        value(x, upper, f.pos());
        IqType type = (IqType) java.util.Objects.requireNonNull(x.detail());
        long n = f.args().size() >= 2 ? java.util.Objects.requireNonNull(literal(f.args().get(1))).longValue() : 1L;
        ValueSpecification p = at.partition();
        ValueSpecification r = at.row();
        ValueSpecification other = n == 1L ? SqlToPure.call(fn, p, r) : SqlToPure.call(fn, p, r, new CInteger(n));
        ValueSpecification read = new AppliedProperty(other, name(x));
        if (f.args().size() < 3 || f.args().get(2) instanceof SqlAst.NullLit || n == 0L) {
            return one(at, read, x.kind(), true, type);
        }
        SqlAst.Expr d = f.args().get(2);
        Tx dflt = at.computed(defaultValue(d, x, type, upper, at, dialect));
        Tx one = at.column(new SqlAst.IntegerLit(BigInteger.ONE, d.pos()));
        ValueSpecification body = IqDialect.ifThen(SqlToPure.call("isEmpty", new AppliedProperty(other, name(one))),
                SqlToPure.valueOf(dflt), read);
        return one(at, body, x.kind(), true, type);
    }

    /** LAG's and LEAD's default, {@code CAST(d AS x's type)} as the island reads that CAST (measured on IQ 17.1: 2.5
     *  for an INTEGER is 2, '7' is 7, 100000 for a SMALLINT is -1009145). A string longer than a VARCHAR x's width is
     *  refused by name (IQ 17.1 answers it with an empty string); a type the island cannot spell for CAST, too. */
    private static Tx defaultValue(SqlAst.Expr d, Tx x, IqType type, String upper, SqlToPure.Windowing at,
            IqDialect dialect) {
        String family = type.family();
        SqlAst.TypeName target = switch (family) {
            case "SMALLINT", "INTEGER", "BIGINT", "DOUBLE", "DATE", "TIMESTAMP", "BIT" ->
                    new SqlAst.TypeName(family, List.of(), d.pos());
            case "NUMERIC", "DECIMAL" -> {
                String sp = type.spelling();
                int comma = sp.indexOf(',');
                int scale = comma < 0 ? 0 : Integer.parseInt(sp.substring(comma + 1, sp.indexOf(')')).strip());
                SqlAst.Expr literal = d instanceof SqlAst.Unary u && u.op() != SqlAst.UnaryOp.NOT ? u.operand() : d;
                if (!(literal instanceof SqlAst.IntegerLit || literal instanceof SqlAst.NumericLit n
                        && n.value().stripTrailingZeros().scale() <= scale)) {
                    // measured on IQ 17.1: 2.555 for a NUMERIC(7,2) is 2.55, the IQ engine's CAST cutting the places
                    // (the research's D-N6), which the island's CAST does not yet
                    throw SqlTranslationException.unsupported(upper + "'s default for a NUMERIC(" + type.size() + ","
                            + scale + ") is supported as a number constant of at most " + scale + " places", d.pos());
                }
                yield new SqlAst.TypeName("NUMERIC", List.of(type.size(), scale), d.pos());
            }
            case "VARCHAR", "CHAR" -> {
                if (!(d instanceof SqlAst.StringLit s) || type.size() < 0 || s.value().length() > type.size()) {
                    throw SqlTranslationException.unsupported(upper + "'s default for a string is supported as a"
                            + " string constant no longer than the value's type (IQ 17.1 answers a longer one with an"
                            + " empty string)", d.pos());
                }
                yield new SqlAst.TypeName(family, List.of(type.size()), d.pos());
            }
            default -> {
                BigInteger n = d instanceof SqlAst.IntegerLit l ? l.value() : d instanceof SqlAst.Unary u
                        && u.op() == SqlAst.UnaryOp.MINUS && u.operand() instanceof SqlAst.IntegerLit l
                        ? l.value().negate() : null;
                if (!type.spelling().equals("UNSIGNED BIGINT") || n == null) {
                    throw SqlTranslationException.unsupported(upper + "'s default for a " + type.spelling()
                            + " is not supported yet", d.pos());
                }
                // IQ island, 2026-10-17 (the corpus lane's JOB: LAG(cast_count, 1, 0) of a COUNT; measured on IQ
                // 17.1): a COUNT's default is an integer constant read as its UNSIGNED BIGINT, a negative one IQ's
                // -1009145 whether or not a row takes it
                if (n.signum() < 0) {
                    throw SqlTranslationException.invalid(upper + "'s default " + n + " is no UNSIGNED BIGINT"
                            + IqDialect.code(-1009145, "QFA2E"), d.pos());
                }
                yield new SqlAst.TypeName("BIGINT", List.of(), d.pos());
            }
        };
        return at.tx(new SqlAst.Cast(d, target, d.pos()));
    }

    /** SUM and AVG over a window (the class's types). */
    private static SqlToPure.WindowFunction sum(SqlAst.FunctionCall f, boolean average, SqlToPure.Windowing at,
            boolean iqEngine) {
        String what = average ? "AVG" : "SUM";
        // the argument's translation over a row (the columns computed from it read it, not its column: one extend
        // computes them all), and its column
        Tx arg = at.tx(f.args().get(0));
        Tx x = at.column(f.args().get(0));
        IqType type = x.detail() instanceof IqType t ? t : null;
        String family = type == null ? "" : type.family();
        if (x.kind() == Kind.FLOAT && family.equals("DOUBLE")) {
            return doubleSum(at, arg, x, average);
        }
        boolean unsigned = family.startsWith("UNSIGNED");
        boolean integer = x.kind() == Kind.INTEGER && (family.equals("SMALLINT") || family.equals("INTEGER")
                || family.equals("BIGINT") || family.equals("BIT") || family.equals("TINYINT") || unsigned);
        if (integer) {
            if (!iqEngine) {
                throw SqlTranslationException.unsupported(what + " of an integer over a window is not supported in"
                        + " SQL Anywhere's front end (its INTEGER total is checked for overflow, row by row)", f.pos());
            }
            if (!average && !(family.equals("INTEGER") || family.equals("BIGINT") || unsigned)) {
                // a BIGINT's or an UNSIGNED BIGINT's total of SMALLINTs or BITs fits 64 bits: an Integer
                ColSpec s = new ColSpec(at.name(), at.overWindow(SqlToPure.valueOf(x)), at.overGroup(
                        SqlToPure.call("sum", at.values())), null);
                return new SqlToPure.WindowFunction(List.of(s), null, Kind.INTEGER, true, new IqType(
                        family.equals("SMALLINT") ? "BIGINT" : "UNSIGNED BIGINT"));
            }
            // summed exactly as a decimal (a BIGINT's total leaves 64 bits: 2^63 - 1 + 25 is answered, measured)
            Tx dec = at.computed(SqlToPure.strict(Kind.DECIMAL, List.of(arg), v -> IqDialect.wide(SqlToPure.call(
                    "toDecimal", v.get(0)))));
            if (!average) {
                ColSpec s = new ColSpec(at.name(), at.overWindow(SqlToPure.valueOf(dec)), at.overGroup(
                        SqlToPure.call("sum", at.values())), null);
                // measured on IQ 17.1: a SUM of COUNTs (UNSIGNED BIGINTs) is a NUMERIC(22,0)
                return new SqlToPure.WindowFunction(List.of(s), null, Kind.DECIMAL, true, new IqType(unsigned
                        ? "NUMERIC(22,0)" : "NUMERIC(20,0)"));
            }
            int precision = switch (family) {
                case "SMALLINT" -> 35;
                case "INTEGER" -> 27;
                case "BIGINT", "BIT", "TINYINT" -> 36;
                // IQ island, 2026-10-15 (the corpus lane; measured on IQ 17.1): of COUNTs a NUMERIC(51,16), of a COUNT
                // beside a signed integer (COALESCE(n, 0), a range known) a NUMERIC(45,16)
                case "UNSIGNED BIGINT" -> type == null || type.low() == null ? 51 : 45;
                default -> throw SqlTranslationException.unsupported(what + " of a " + family + " over a window is not"
                        + " supported yet", f.pos());
            };
            return average(f, at, dec, 0, 20, 16, precision);
        }
        if ((x.kind() == Kind.DECIMAL || x.kind() == Kind.INTEGER) && (family.equals("NUMERIC")
                || family.equals("DECIMAL"))) {
            String sp = java.util.Objects.requireNonNull(type).spelling();
            int comma = sp.indexOf(',');
            int s = comma < 0 ? 0 : Integer.parseInt(sp.substring(comma + 1, sp.indexOf(')')).strip());
            int p = type.size();
            if (p + 2 > 38) {
                throw SqlTranslationException.unsupported(what + " of a NUMERIC(" + p + "," + s + ") over a window is"
                        + " not supported: its total may need more than the 38 digits lite's decimals hold", f.pos());
            }
            Tx dec = x.kind() == Kind.DECIMAL ? x : at.computed(SqlToPure.strict(Kind.DECIMAL, List.of(arg),
                    v -> SqlToPure.call("toDecimal", v.get(0))));
            if (!average) {
                ColSpec spec = new ColSpec(at.name(), at.overWindow(SqlToPure.valueOf(dec)), at.overGroup(
                        SqlToPure.call("sum", at.values())), null);
                return new SqlToPure.WindowFunction(List.of(spec), null, Kind.DECIMAL, true,
                        new IqType("NUMERIC(" + (iqEngine ? p + 2 : Math.min(p + 8, 38)) + "," + s + ")"));
            }
            if (!iqEngine) {
                throw SqlTranslationException.unsupported("AVG of a NUMERIC over a window is not supported in SQL"
                        + " Anywhere's front end yet", f.pos());
            }
            return average(f, at, dec, s, p - s + 2, s + 21, Math.min(126, p + 22));
        }
        throw SqlTranslationException.unsupported(what + " of " + (x.kind() == Kind.STRING ? "a string"
                : x.kind() == Kind.DATE || x.kind() == Kind.DATETIME ? "a date" : "a value of this type")
                + " over a window is not supported (IQ converts it to a number by its own rules)", f.pos());
    }

    /** The IQ engine's AVG of an exact number over a window: its total and count over the window, then the quotient
     *  cut toward zero at {@code scale} places ({@link IqFunctions#cutQuotient}); NULL over no values. */
    private static SqlToPure.WindowFunction average(SqlAst.FunctionCall f, SqlToPure.Windowing at, Tx dec, int s1,
            int whole, int scale, int precision) {
        String total = at.fresh();
        String count = at.fresh();
        Tx t = Tx.value(at.column(total), Kind.DECIMAL, true);
        Tx n = Tx.value(at.column(count), Kind.INTEGER, false);
        // IQ island, 2026-10-17 (the TPC-H corpus lane's 16): where one step does not fit, the dividend made whole
        // first (IqFunctions.truncatedQuotient), or the quotient divided in chunks, a value past 38 digits refused at
        // run time (IqFunctions.longQuotient), as the IQ engine's AVG over a group is
        String past = IqDialect.REFUSED_AT_RUN_TIME + "an AVG over a window of more than " + (38 - scale) + " digits"
                + " before its point is not supported: its NUMERIC(" + precision + "," + scale + ") needs more than the"
                + " 38 digits lite's decimals hold";
        java.util.function.BiFunction<ValueSpecification, ValueSpecification, ValueSpecification> divided = (x, y) -> {
            ValueSpecification one = IqFunctions.cutQuotient(x, s1, whole, y, 19, scale);
            if (one != null) {
                return one;
            }
            return IqFunctions.truncatedFits(s1, whole, scale) ? IqFunctions.truncatedQuotient(x, s1, whole, y, scale)
                    : IqFunctions.longQuotient(x, s1, whole, y, 19, scale, past);
        };
        if (divided.apply(at.column(total), at.column(count)) == null) {
            throw SqlTranslationException.unsupported("AVG over a window needs a step of more than the 38 digits"
                    + " lite's decimals hold here", f.pos());
        }
        Tx quotient = SqlToPure.strict(Kind.DECIMAL, List.of(t, n), v -> IqDialect.ifThen(SqlToPure.call("equal",
                v.get(1), new CInteger(0L)), SqlToPure.nullOf(Kind.DECIMAL).vs(), java.util.Objects.requireNonNull(
                divided.apply(v.get(0), SqlToPure.call("toDecimal", v.get(1))))));
        List<ColSpec> windows = List.of(
                new ColSpec(total, at.overWindow(SqlToPure.valueOf(dec)), at.overGroup(SqlToPure.call("sum",
                        at.values())), null),
                new ColSpec(count, at.overWindow(SqlToPure.valueOf(dec)), at.overGroup(SqlToPure.call("count",
                        at.values())), null));
        return new SqlToPure.WindowFunction(windows, new ColSpec(at.name(), at.overRow(quotient.vs()), null, null),
                Kind.DECIMAL, true, new IqType("NUMERIC(" + Math.min(precision, 38) + "," + scale + ")"));
    }

    /** SUM and AVG of a DOUBLE over a window: Pure's sum or average, IQ's -1001010 where the frame's positive or
     *  negative values' total leaves the DOUBLE's range (measured: 1e308 + 1e308, in a SUM or an AVG). */
    private static SqlToPure.WindowFunction doubleSum(SqlToPure.Windowing at, Tx arg, Tx x, boolean average) {
        String raw = at.fresh();
        String positive = at.fresh();
        String negative = at.fresh();
        CFloat zero = new CFloat(0.0);
        Tx up = SqlToPure.strict(Kind.FLOAT, List.of(arg), v -> IqDialect.ifThen(SqlToPure.call("greaterThan",
                v.get(0), zero), v.get(0), zero));
        Tx down = SqlToPure.strict(Kind.FLOAT, List.of(arg), v -> IqDialect.ifThen(SqlToPure.call("lessThan",
                v.get(0), zero), v.get(0), zero));
        Tx upCol = at.computed(up);
        Tx downCol = at.computed(down);
        List<ColSpec> windows = List.of(
                new ColSpec(raw, at.overWindow(SqlToPure.valueOf(x)), at.overGroup(SqlToPure.call(average ? "average"
                        : "sum", at.values())), null),
                new ColSpec(positive, at.overWindow(SqlToPure.valueOf(upCol)), at.overGroup(SqlToPure.call("sum",
                        at.values())), null),
                new ColSpec(negative, at.overWindow(SqlToPure.valueOf(downCol)), at.overGroup(SqlToPure.call("sum",
                        at.values())), null));
        Tx pos = Tx.value(at.column(positive), Kind.FLOAT, true);
        Tx neg = Tx.value(at.column(negative), Kind.FLOAT, true);
        Tx value = Tx.value(at.column(raw), Kind.FLOAT, true);
        Tx checked = SqlToPure.strict(Kind.FLOAT, List.of(pos, neg, value), v -> IqDialect.ifThen(SqlToPure.call("or",
                SqlToPure.call("greaterThan", v.get(0), new CFloat(Double.MAX_VALUE)), SqlToPure.call("lessThan",
                v.get(1), new CFloat(-Double.MAX_VALUE))), IqDialect.floatRaise(), v.get(2)));
        return new SqlToPure.WindowFunction(windows, new ColSpec(at.name(), at.overRow(checked.vs()), null, null),
                Kind.FLOAT, true, new IqType("DOUBLE"));
    }

    /**
     * MIN and MAX of strings over a window by IQ's collation key ({@link IqDialect#collated}): a value of the least
     * (greatest) key -- which of the values sharing it is the database's choice, as it is IQ's (the user's principle:
     * any spelling IQ could return is right). Pure's min and max of strings over each value packed behind its escaped
     * key ({@link IqAggregates#packedByKey}, the aggregate's packing; 2026-10-16, the landing's review: the key
     * unescaped, a value holding U+0001 could cut at the wrong place), the value what follows the separator.
     */
    private static SqlToPure.WindowFunction collatedMinMax(String fn, SqlToPure.Windowing at, Tx arg,
            @com.legend.base.Nullable IqType type, IqDialect dialect) {
        Tx keyed = at.computed(SqlToPure.strict(Kind.STRING, List.of(arg), v -> IqAggregates.packedByKey(v.get(0),
                dialect)));
        String both = at.fresh();
        List<ColSpec> windows = List.of(new ColSpec(both, at.overWindow(SqlToPure.valueOf(keyed)),
                at.overGroup(SqlToPure.call(fn, at.values())), null));
        Tx value = SqlToPure.strict(Kind.STRING, List.of(Tx.value(at.column(both), Kind.STRING, true)),
                v -> IqAggregates.unpacked(v.get(0)));
        return new SqlToPure.WindowFunction(windows, new ColSpec(at.name(), at.overRow(value.vs()), null, null),
                Kind.STRING, true, type);
    }

    /** MIN and MAX over a window: x's type; strings only where each value is its own collation key, over a frame from
     *  the partition's first row (the class's). */
    private static SqlToPure.WindowFunction minMax(SqlAst.FunctionCall f, String fn, String upper,
            SqlToPure.Windowing at, IqDialect dialect) {
        SqlAst.Expr argument = f.args().get(0);
        Tx x = at.column(argument);
        IqType type = x.detail() instanceof IqType t ? t : null;
        if (x.kind() == Kind.STRING) {
            SqlAst.Frame frame = at.spec().frame();
            boolean fixedWidth = type != null && type.family().equals("CHAR");
            if (frame != null && frame.start().kind() != SqlAst.BoundKind.UNBOUNDED_PRECEDING && !fixedWidth) {
                throw SqlTranslationException.unsupported(upper + " of strings over a frame that starts after the"
                        + " partition's first row is not supported: IQ 17.1 answers a VARCHAR's with NUL bytes in it"
                        + " (its defect, measured)", f.pos());
            }
            if (!dialect.keys().own(argument)) {
                return collatedMinMax(fn, at, at.tx(argument), type, dialect);
            }
        } else if (type == null || !(x.kind().numeric() || x.kind() == Kind.DATE || x.kind() == Kind.DATETIME)) {
            throw SqlTranslationException.unsupported(upper + " of a value whose IQ type is not known is not supported",
                    f.pos());
        }
        ValueSpecification map = SqlToPure.valueOf(x);
        ValueSpecification reduce = SqlToPure.call(fn, at.values());
        if (x.kind() == Kind.DECIMAL) {
            // Pure's min and max of decimals are Numbers (and minBy, the grouped MIN's, is no window function on
            // Postgres): the Number read back as the Decimal it is
            int scale = scaleOf(java.util.Objects.requireNonNull(type));
            if (type.size() - scale > 20) {
                throw SqlTranslationException.unsupported(upper + " of a " + type.spelling() + " over a window is not"
                        + " supported: its value would be read back through a decimal of 20 whole digits", f.pos());
            }
            String number = at.fresh();
            ColSpec spec = new ColSpec(number, at.overWindow(map), at.overGroup(reduce), null);
            Tx back = SqlToPure.strict(Kind.DECIMAL, List.of(Tx.value(at.column(number), Kind.NUMBER, true)),
                    v -> SqlToPure.call("round", SqlToPure.call("toDecimal", v.get(0)), new CInteger(scale)));
            return new SqlToPure.WindowFunction(List.of(spec), new ColSpec(at.name(), at.overRow(back.vs()), null,
                    null), Kind.DECIMAL, true, type);
        }
        ColSpec spec = new ColSpec(at.name(), at.overWindow(map), at.overGroup(reduce), null);
        return new SqlToPure.WindowFunction(List.of(spec), null, x.kind(), true, type);
    }
}
