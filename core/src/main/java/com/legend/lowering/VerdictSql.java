// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.SqlUnion;
import com.legend.sql.SqlWith;

import java.util.ArrayList;
import java.util.List;

/**
 * DATABASE-MODE JUDGING, leg 3.1 (docs/DATABASE_MODE_HOMEWORK_2026_09_18.md
 * §4b): ONE statement decides an equality assert. Both sides are the
 * canon-wrapped side plans the verdict lane already lowers
 * ({@link CanonicalRenderSql#wrapWithCanon}); this class composes them
 * into
 *
 * <pre>
 * WITH __e AS (SELECT canon AS __c, row_number() OVER () AS __rn FROM (e) w),
 *      __a AS (...)
 * SELECT (frame(__e) IS NOT DISTINCT FROM frame(__a)) AS __verdict,
 *        frame(__e) AS __expected, frame(__a) AS __actual,
 *        CASE WHEN a null canon cell THEN 'null-canon-cell' END AS __unjudged
 * </pre>
 *
 * where {@code frame} is CANONICAL_FORM_SPEC's side framing in SQL — the
 * same rule {@code AssertVerdicts.frame} applied in Java: no element
 * {@code '[]'}, one element its bare text, many {@code '[a, b]'} in the
 * side's order (arrival order for an ordered side, canon-text order for a
 * multiset side). The verdict column can never be NULL (P-19: a NULL
 * verdict read as a pass was the highest-ranked risk); the evidence
 * columns ARE the failure message; {@code __unjudged} names a shape the
 * statement could not decide, and the caller FAILS the assert with it.
 * No Java compares a value.
 */
public final class VerdictSql {

    private VerdictSql() {
    }

    public static final String VERDICT = "__verdict";
    public static final String EXPECTED = "__expected";
    public static final String ACTUAL = "__actual";
    public static final String UNJUDGED = "__unjudged";
    /** True when the verdict held ONLY through the declared 2-ULP Float
     * leniency (the harness counts it, as host mode counts its own). */
    public static final String LENIENT = "__lenient";

    /** One canon-wrapped side: the wrapped plan, the name of the canon
     * column that decides (one of {@code __canon<i>}), whether the side
     * is a collection, and whether its elements order by canon text (the
     * multiset forms) or by arrival (an ordered assert). */
    public record Side(SqlQuery wrapped, String canonColumn, boolean many,
            boolean byCanonText, boolean isFloat) {
    }

    private static final String C = "__c";
    private static final String RN = "__rn";
    /** The NAMED wrapped grid(s) a grid verdict reads (see {@link #over}). */
    private static final String GRID = "__wg";
    private static final String GRID_E = "__we";
    private static final String GRID_A = "__wa";
    private static final String V = "__v";

    /** The equality verdict statement over two framed sides. */
    public static SqlQuery equality(Side e, Side a) {
        // THE SIDE'S SHAPE FOLLOWS ITS DECLARED MULTIPLICITY (lean ladder rung
        // 3): a side declared exactly one spells its facts straight over its
        // plan — no rows CTE, no aggregate (count is one, first is the value);
        // an optional or many side is a rows CTE folded once. The 2-ULP
        // leniency (a declared-Float side) walks cells positionally, so a
        // Float side keeps the rows form — the general shape, one rule.
        boolean lenientPossible = e.isFloat() || a.isFloat();
        Folded ef = fold("__e", e, lenientPossible);
        Folded af = fold("__a", a, lenientPossible);
        return statementOf(ef, af, List.of(), List.of(),
                lenientPossible ? ef.rows() : null, lenientPossible ? af.rows() : null);
    }

    /** A side folded to its facts row: {@code rows} is the rows CTE's query
     * (null when the facts are spelled inline over the plan). */
    private record Folded(String name, @com.legend.Nullable SqlQuery rows, SqlQuery facts) {
    }

    private static Folded fold(String name, Side s, boolean keepRows) {
        if (!s.many() && !keepRows && s.wrapped() instanceof SqlSelect ws && plainProjection(ws)) {
            return new Folded(name, null, inlineFacts(s));
        }
        return new Folded(name, canonRows(s, keepRows), sideFacts(name, s.many(), s.byCanonText()));
    }

    // ── the GRID forms (leg 3.1b): a TABULAR side rides the grid wrap
    // (CanonicalRenderSql.wrapTdsCanon: __rowcanon + __cell<i>); its value
    // PEER (a literal list) is framed into rows of the grid's width, or
    // compared as a loose cell pool (the sameElements form) — the same
    // rules TdsCompare.peerRowCanons / tdsCellCanons apply in Java.

    /** A grid side: its wrapped plan and its width (columns). */
    /** {@code floatColumns}: per column, whether it is DECLARED Float —
     * the 2-ULP leniency's operand columns. */
    /** {@code emptyIsNull}: per column, whether the assert's own grammar
     * conflates the empty string with NULL ({@code toCSV} prints both as
     * an empty cell) — the verdict then judges under that equivalence: a
     * String cell's empty canon reads as the TDSNull sentinel. */
    public record GridSide(SqlQuery wrapped, int width, List<Boolean> floatColumns,
            List<Boolean> emptyIsNull) {
        public GridSide(SqlQuery wrapped, int width, List<Boolean> floatColumns) {
            this(wrapped, width, floatColumns, List.of());
        }
        boolean emptyIsNullAt(int i) {
            return i < emptyIsNull.size() && emptyIsNull.get(i);
        }
    }

    /** A value peer of a grid: its wrapped plan, the literal-channel canon
     * column, and whether it is the EXPECTED side (the golden's
     * {@code 'TDSNull'} string cells spell the bare sentinel there only —
     * a real 'TDSNull' string on OUR wire stays quoted). */
    public record PeerSide(SqlQuery wrapped, String canonColumn, boolean expected,
            boolean isFloat) {
    }

    /** {@code assertEquals} over rows: the grid's row canons against the
     * peer's cells chunked by the grid's width; ordered by arrival, or as
     * a row multiset when {@code multiset}. {@code gridIsExpected} says
     * which side the grid is. */
    public static SqlQuery gridRows(GridSide grid, PeerSide peer,
            boolean gridIsExpected, boolean multiset) {
        SqlQuery g = gridRowCanons(grid, GRID);
        SqlQuery p = peerRowCanons(peer, grid.width());
        SqlExpr divisible = SqlExpr.Call.of(SqlFn.NOT_EQUAL,
                SqlExpr.Call.of(SqlFn.MOD,
                        scalarOver("__peer", new SqlAgg.Reducer(SqlAgg.Fn.COUNT,
                                List.of(col("__peer", RN)), false, List.of()),
                                "__n", SqlType.Scalar.BIGINT, null),
                        new SqlExpr.IntLit(grid.width())),
                new SqlExpr.IntLit(0));
        List<SqlWith.Cte> extra = List.of(new SqlWith.Cte(GRID, grid.wrapped()),
                new SqlWith.Cte("__peer", peerCells(peer)));
        List<SqlExpr.Case.When> more = List.of(new SqlExpr.Case.When(divisible,
                new SqlExpr.StringLit("tds-peer: cells not divisible by width "
                        + grid.width())));
        SqlQuery gc = gridCellsRowMajor(grid, GRID);
        SqlQuery pc = peerCells(peer);
        return gridIsExpected
                ? statement(g, p, true, true, multiset, more, extra, gc, pc)
                : statement(p, g, true, true, multiset, more, extra, pc, gc);
    }

    /** {@code assertSameElements} over a grid: the loose CELL pool (every
     * cell of every row) against the peer's cells, as a multiset. */
    public static SqlQuery gridCells(GridSide grid, PeerSide peer,
            boolean gridIsExpected) {
        SqlQuery g = gridCellCanons(grid, GRID);
        SqlQuery p = peerCells(peer);
        List<SqlWith.Cte> named = List.of(new SqlWith.Cte(GRID, grid.wrapped()));
        return gridIsExpected
                ? statement(g, p, true, true, true, List.of(), named)
                : statement(p, g, true, true, true, List.of(), named);
    }

    // ── the ONE-LINE families (leg 3.1c): size / empty / contains / a boolean
    // condition / the tolerance assert / the forAll-contains subset — each a
    // predicate over the same row sources, returned in the same verdict row.

    /** {@code assertSize}: the side's row count against {@code n} (the size
     * side's one canon text as a BIGINT); {@code envelope} = the read is a
     * relation-rooted execute's {@code .values}, which holds ONE TDS. */
    public static SqlQuery size(SqlQuery sideRows, SqlQuery nScalar, boolean envelope) {
        OneRow c = envelope ? constantOf("c", new SqlExpr.IntLit(1), "__n", SqlType.Scalar.BIGINT)
                : countOf("c", "__a");
        OneRow n = rowOf("n", nScalar);
        SqlExpr count = c.col("__n");
        SqlExpr want = new SqlExpr.Cast(n.col(C), SqlType.Scalar.BIGINT);
        return predicateOver(envelope ? List.of() : List.of(new SqlWith.Cte("__a", sideRows)),
                List.of(c, n), SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, count, want),
                new SqlExpr.Cast(want, SqlType.Scalar.VARCHAR),
                new SqlExpr.Cast(count, SqlType.Scalar.VARCHAR), new SqlExpr.NullLit());
    }

    /** A GRAPH-shaped side (a class collection serialized as ONE JSON
     * document): its size is the number of ROOT ROWS the fold aggregates —
     * the host rule ({@code p instanceof List ? size : 1}) read off the
     * PLAN, not the document. An array-wrapped root ({@code JsonArrayAgg}
     * under the fold's VARCHAR cast / empty-array COALESCE) counts the rows
     * under the aggregate; a bare-object root is one document when a row
     * exists and NULL (0) otherwise. No JSON function on any dialect, no
     * document built to be measured. A plan that is not the fold's
     * one-projection select is a construction fault, loud. */
    /** A graph plan's ROOT ROWS (rung 12): the fold's own from / where / caps
     * projecting the root table's PHYSICAL columns ({@code root.*} — the
     * store's declared list can exceed what a seeded table carries; a reader
     * that names an absent column fails exactly as it would over the table).
     * Null when the plan is not a fold over a single root TABLE. */
    public static @com.legend.Nullable SqlSelect classExtentRows(SqlQuery graphPlan) {
        SqlQuery fold = graphPlan;
        for (int depth = 0; depth < 4 && fold instanceof SqlSelect w
                && !w.projections().isEmpty()
                && w.projections().get(0).expr() instanceof SqlExpr.Column
                && w.from() instanceof SqlSource.Subselect inner; depth++) {
            fold = inner.inner();
        }
        if (!(fold instanceof SqlSelect ps) || ps.projections().size() != 1
                || !ps.groupBy().isEmpty()) {
            return null;
        }
        SqlSource leftmost = ps.from();
        while (leftmost instanceof SqlSource.Join j) {
            leftmost = j.left();
        }
        if (!(leftmost instanceof SqlSource.Table root) || root.outputs().isEmpty()) {
            return null;
        }
        List<SqlSelect.Projection> ps2 = List.of(
                new SqlSelect.Projection(new SqlExpr.Star(root.alias()), null, null));
        return new SqlSelect(ps2, ps.distinct(), ps.from(), ps.where(), ps.groupBy(), ps.having(),
                ps.qualify(), ps.orderBy(), ps.limit(), ps.offset(), root.outputs());
    }

    public static SqlExpr graphCount(SqlQuery graphPlan) {
        // the canon wrap over a graph side is a pass-through select (the
        // document column beside its canon) around the fold: descend to it
        SqlQuery fold = graphPlan;
        for (int depth = 0; depth < 4 && fold instanceof SqlSelect w
                && !w.projections().isEmpty()
                && w.projections().get(0).expr() instanceof SqlExpr.Column
                && w.from() instanceof SqlSource.Subselect inner; depth++) {
            fold = inner.inner();
        }
        if (!(fold instanceof SqlSelect ps) || ps.projections().size() != 1
                || !ps.groupBy().isEmpty()) {
            throw new IllegalStateException("graph side: not the fold's one-projection select: "
                    + fold.getClass().getSimpleName()
                    + (fold instanceof SqlSelect gs ? " projections=" + gs.projections().stream()
                            .map(pr -> pr.alias() + ":" + pr.expr().getClass().getSimpleName()).toList()
                            + " groupBy=" + gs.groupBy().size() + " from=" + gs.from().getClass().getSimpleName()
                            : ""));
        }
        SqlExpr top = ps.projections().get(0).expr();
        while (true) {
            if (top instanceof SqlExpr.Cast c) {
                top = c.value();
            } else if (top instanceof SqlExpr.Call k && k.fn() == SqlFn.COALESCE
                    && !k.args().isEmpty()) {
                top = k.args().get(0);
            } else {
                break;
            }
        }
        OutputCol n = new OutputCol("__n", SqlType.Scalar.BIGINT, false);
        SqlSelect.Projection countStar = new SqlSelect.Projection(
                new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(), false, List.of()), "__n", n);
        if (top instanceof SqlExpr.JsonArrayAgg) {
            return new SqlExpr.ScalarSubquery(ps.withProjections(List.of(countStar)));
        }
        OutputCol one = new OutputCol("__one", SqlType.Scalar.BIGINT, false);
        SqlSelect first = new SqlSelect(
                List.of(new SqlSelect.Projection(new SqlExpr.IntLit(1), "__one", one)),
                ps.distinct(), ps.from(), ps.where(), ps.groupBy(), ps.having(), ps.qualify(),
                ps.orderBy(), 1L, ps.offset(), List.of(one));
        return new SqlExpr.ScalarSubquery(new SqlSelect(List.of(countStar), false,
                new SqlSource.Subselect(first, "w", null), null, List.of(), null, null,
                List.of(), null, null, List.of(n)));
    }

    /** {@code assertSize} over a graph side. */
    public static SqlQuery sizeOfGraph(SqlQuery graphPlan, SqlQuery nScalar) {
        OneRow c = rowOf("c", ((SqlExpr.ScalarSubquery) graphCount(graphPlan)).subquery());
        OneRow n = rowOf("n", nScalar);
        SqlExpr count = c.col("__n");
        SqlExpr want = new SqlExpr.Cast(n.col(C), SqlType.Scalar.BIGINT);
        return predicateOver(List.of(), List.of(c, n),
                SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, count, want),
                new SqlExpr.Cast(want, SqlType.Scalar.VARCHAR),
                new SqlExpr.Cast(count, SqlType.Scalar.VARCHAR), new SqlExpr.NullLit());
    }

    /** {@code assertEmpty} / {@code assertNotEmpty} over a graph side. */
    public static SqlQuery emptyOfGraph(SqlQuery graphPlan, boolean wantEmpty) {
        OneRow c = rowOf("c", ((SqlExpr.ScalarSubquery) graphCount(graphPlan)).subquery());
        SqlExpr count = c.col("__n");
        SqlExpr isEmpty = SqlExpr.Call.of(SqlFn.EQUAL, count, new SqlExpr.IntLit(0));
        return predicateOver(List.of(), List.of(c),
                wantEmpty ? isEmpty : SqlExpr.Call.of(SqlFn.NOT, isEmpty),
                new SqlExpr.StringLit(wantEmpty ? "empty" : "not empty"),
                SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.Cast(count, SqlType.Scalar.VARCHAR),
                        new SqlExpr.StringLit(" element(s)")), new SqlExpr.NullLit());
    }

    /** {@code assertEmpty} / {@code assertNotEmpty}: the side's row count. */
    public static SqlQuery empty(SqlQuery sideRows, boolean wantEmpty) {
        OneRow c = countOf("c", "__a");
        SqlExpr count = c.col("__n");
        SqlExpr isEmpty = SqlExpr.Call.of(SqlFn.EQUAL, count, new SqlExpr.IntLit(0));
        return predicateOver(List.of(new SqlWith.Cte("__a", sideRows)), List.of(c),
                wantEmpty ? isEmpty : SqlExpr.Call.of(SqlFn.NOT, isEmpty),
                new SqlExpr.StringLit(wantEmpty ? "empty" : "not empty"),
                SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.Cast(count, SqlType.Scalar.VARCHAR),
                        new SqlExpr.StringLit(" element(s)")), new SqlExpr.NullLit());
    }

    /** {@code assertContains}: some element's canon equals the value's. */
    public static SqlQuery contains(SqlQuery collRows, SqlQuery valScalar) {
        OutputCol vc = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        SqlSource v0 = new SqlSource.Table("__v0", "__v0", valScalar.outputs(), false);
        OneRow v = new OneRow("v", new SqlSelect(List.of(new SqlSelect.Projection(
                SqlExpr.Column.of("__v0", valScalar.outputs(), C), C, vc)),
                false, v0, null, List.of(), null, null, List.of(), null, null, List.of(vc)), List.of(vc));
        OutputCol mOut = new OutputCol("__m", SqlType.Scalar.BIGINT, false);
        SqlSource matched = new SqlSource.Join(cte("__a"),
                new SqlSource.Table("__v0", "__v0", valScalar.outputs(), false), SqlSource.Join.Kind.INNER,
                SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, col("__a", C), SqlExpr.Column.of("__v0", valScalar.outputs(), C)));
        OneRow m = new OneRow("m", new SqlSelect(List.of(new SqlSelect.Projection(
                new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(), false, List.of()), "__m", mOut)),
                false, matched, null, List.of(), null, null, List.of(), null, null, List.of(mOut)), List.of(mOut));
        OneRow t = textOf("t", "__sa");
        return predicateOver(List.of(new SqlWith.Cte("__a", collRows), new SqlWith.Cte("__v0", valScalar),
                        new SqlWith.Cte("__sa", sideFacts("__a", true, true))),
                List.of(v, m, t), SqlExpr.Call.of(SqlFn.GREATER, m.col("__m"), new SqlExpr.IntLit(0)),
                v.col(C), t.col(F_TEXT), new SqlExpr.NullLit());
    }

    /** {@code assert(cond)} / {@code assertFalse(cond)}: the condition's one
     * canon text is {@code true} / {@code false}. */
    public static SqlQuery condition(SqlQuery condScalar, boolean wantTrue) {
        OneRow a = rowOf("a", condScalar);
        SqlExpr c = a.col(C);
        return predicateOver(List.of(), List.of(a),
                SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, c, new SqlExpr.StringLit(wantTrue ? "true" : "false")),
                new SqlExpr.StringLit(wantTrue ? "true" : "false"),
                SqlExpr.Call.of(SqlFn.COALESCE, c, new SqlExpr.StringLit("[]")), new SqlExpr.NullLit());
    }

    /** {@code coll->map(x | assert(pred))} — the quantified assert (task #14 leg 2,
     * 2026-09-21): every element's condition canon is {@code true} / {@code false};
     * the verdict is that NO row says otherwise (a NULL condition is otherwise). */
    public static SqlQuery allOf(SqlQuery condRows, boolean wantTrue) {
        OutputCol n = new OutputCol("__n", SqlType.Scalar.BIGINT, false);
        SqlExpr wanted = SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, col("__a", C),
                new SqlExpr.StringLit(wantTrue ? "true" : "false"));
        OneRow otherwise = new OneRow("b", new SqlSelect(List.of(new SqlSelect.Projection(
                new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(col("__a", RN)), false, List.of()), "__n", n)),
                false, cte("__a"), SqlExpr.Call.of(SqlFn.NOT, wanted), List.of(), null, null, List.of(),
                null, null, List.of(n)), List.of(n));
        return predicateOver(List.of(new SqlWith.Cte("__a", condRows)), List.of(otherwise),
                SqlExpr.Call.of(SqlFn.EQUAL, otherwise.col("__n"), new SqlExpr.IntLit(0)),
                new SqlExpr.StringLit("every element " + (wantTrue ? "true" : "false")),
                SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.Cast(otherwise.col("__n"), SqlType.Scalar.VARCHAR),
                        new SqlExpr.StringLit(" element(s) otherwise")),
                new SqlExpr.NullLit());
    }

    /** {@code assertEqWithinTolerance(e, a, tol)}: {@code |e − a| ≤ tol} over
     * the three sides' canon texts as DOUBLEs. */
    public static SqlQuery tolerance(SqlQuery eScalar, SqlQuery aScalar, SqlQuery tolScalar) {
        OneRow eo = rowOf("e", eScalar);
        OneRow ao = rowOf("a", aScalar);
        OneRow to = rowOf("t", tolScalar);
        SqlExpr e = new SqlExpr.Cast(eo.col(C), SqlType.Scalar.DOUBLE);
        SqlExpr a = new SqlExpr.Cast(ao.col(C), SqlType.Scalar.DOUBLE);
        SqlExpr t = new SqlExpr.Cast(to.col(C), SqlType.Scalar.DOUBLE);
        SqlExpr within = SqlExpr.Call.of(SqlFn.LESS_EQUAL,
                SqlExpr.Call.of(SqlFn.ABS, SqlExpr.Call.of(SqlFn.MINUS, e, a)), t);
        return predicateOver(List.of(), List.of(eo, ao, to),
                SqlExpr.Call.of(SqlFn.COALESCE, within, new SqlExpr.BoolLit(false)),
                new SqlExpr.Cast(e, SqlType.Scalar.VARCHAR), new SqlExpr.Cast(a, SqlType.Scalar.VARCHAR),
                new SqlExpr.NullLit());
    }

    /** The {@code $need->forAll(n | $have->contains($n))} subset idiom:
     * no needed canon is absent from the haves. */
    public static SqlQuery subset(SqlQuery needRows, SqlQuery haveRows, boolean wantTrue) {
        OutputCol one = new OutputCol("__one", SqlType.Scalar.BIGINT, false);
        SqlExpr present = new SqlExpr.Exists(new SqlSelect(
                List.of(new SqlSelect.Projection(new SqlExpr.IntLit(1), "__one", one)),
                false, cte("__h"),
                SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, col("__h", C), col("__n", C)),
                List.of(), null, null, List.of(), null, null, List.of(one)));
        SqlExpr missing = new SqlExpr.Exists(new SqlSelect(
                List.of(new SqlSelect.Projection(new SqlExpr.IntLit(1), "__one", one)),
                false, cte("__n"), SqlExpr.Call.of(SqlFn.NOT, present),
                List.of(), null, null, List.of(), null, null, List.of(one)));
        OutputCol miss = new OutputCol("__missing", SqlType.Scalar.BIGINT, false);
        OneRow m = new OneRow("m", new SqlSelect(List.of(new SqlSelect.Projection(
                new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(), false, List.of()), "__missing", miss)),
                false, cte("__n"), SqlExpr.Call.of(SqlFn.NOT, present),
                List.of(), null, null, List.of(), null, null, List.of(miss)), List.of(miss));
        OneRow t = textOf("t", "__sn");
        SqlExpr anyMissing = SqlExpr.Call.of(SqlFn.GREATER, m.col("__missing"), new SqlExpr.IntLit(0));
        return predicateOver(List.of(new SqlWith.Cte("__n", needRows), new SqlWith.Cte("__h", haveRows),
                        new SqlWith.Cte("__sn", sideFacts("__n", true, true))),
                List.of(m, t), wantTrue ? SqlExpr.Call.of(SqlFn.NOT, anyMissing) : anyMissing,
                new SqlExpr.StringLit(wantTrue ? "subset" : "not a subset"), t.col(F_TEXT), new SqlExpr.NullLit());
    }

    /** The RENDERED-TEXT arm (leg 3.1d): a database-rendered grid text
     * (toCSV / toString / a join) against a string — byte-equal is the
     * verdict; a differing pair is UNJUDGED here with the reason (host
     * mode's policy for that case — data lines as a multiset, a bounded
     * print-precision float tolerance per cell — is not a SQL rule yet;
     * the differential gate holds those rows up). */
    public static SqlQuery renderedText(SqlQuery eRows, SqlQuery aRows) {
        OneRow eo = firstOf("e", "__e");
        OneRow ao = firstOf("a", "__a");
        SqlExpr equal = SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, eo.col(C), ao.col(C));
        SqlExpr unjudged = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.NOT, equal),
                new SqlExpr.StringLit("rendered-text: not byte-equal (host policy: line multiset, cell tolerance)"))),
                null);
        return predicateOver(List.of(new SqlWith.Cte("__e", eRows), new SqlWith.Cte("__a", aRows)),
                List.of(eo, ao), equal, eo.col(C), ao.col(C), unjudged);
    }

    /** {@code assertTdsEquivalent(one, two, delta[, timeDelta])} (bucket 5): the
     * two grids' cells ROW-MAJOR, aligned by position; a numeric pair within
     * {@code delta}, a temporal pair within {@code timeDelta} seconds, any
     * other pair canon-equal (the host rule, TdsCompare.tdsEquivalent); the
     * cell counts must match. {@code kinds} = the columns' declared kinds
     * (both grids share the schema — the names were checked statically). */
    public static SqlQuery gridTolerance(GridSide one, GridSide two, List<Type> kinds,
            SqlQuery deltaRows, SqlQuery timeDeltaRows) {
        SqlQuery e = toleranceCells(one, kinds, GRID_E);
        SqlQuery a = toleranceCells(two, kinds, GRID_A);
        SqlExpr delta = new SqlExpr.Cast(scalarOver("__d", col("__d", C), "__one",
                SqlType.Scalar.VARCHAR, 1L), SqlType.Scalar.DOUBLE);
        SqlExpr timeDelta = new SqlExpr.Cast(scalarOver("__t", col("__t", C), "__one",
                SqlType.Scalar.VARCHAR, 1L), SqlType.Scalar.DOUBLE);
        SqlExpr en = SqlExpr.Column.of("__e", "__n", SqlType.Scalar.DOUBLE, true, OutputCol.Origin.DERIVED);
        SqlExpr an = SqlExpr.Column.of("__a", "__n", SqlType.Scalar.DOUBLE, true, OutputCol.Origin.DERIVED);
        SqlExpr es = SqlExpr.Column.of("__e", "__s", SqlType.Scalar.DOUBLE, true, OutputCol.Origin.DERIVED);
        SqlExpr as = SqlExpr.Column.of("__a", "__s", SqlType.Scalar.DOUBLE, true, OutputCol.Origin.DERIVED);
        SqlExpr within = SqlExpr.Call.of(SqlFn.OR,
                SqlExpr.Call.of(SqlFn.OR,
                        SqlExpr.Call.of(SqlFn.AND,
                                SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.IS_NOT_NULL, en),
                                        SqlExpr.Call.of(SqlFn.IS_NOT_NULL, an)),
                                SqlExpr.Call.of(SqlFn.LESS_EQUAL,
                                        SqlExpr.Call.of(SqlFn.ABS, SqlExpr.Call.of(SqlFn.MINUS, en, an)),
                                        SqlExpr.Call.of(SqlFn.ABS, delta))),
                        SqlExpr.Call.of(SqlFn.AND,
                                SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.IS_NOT_NULL, es),
                                        SqlExpr.Call.of(SqlFn.IS_NOT_NULL, as)),
                                SqlExpr.Call.of(SqlFn.LESS_EQUAL,
                                        SqlExpr.Call.of(SqlFn.ABS, SqlExpr.Call.of(SqlFn.MINUS, es, as)),
                                        SqlExpr.Call.of(SqlFn.ABS, timeDelta)))),
                SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, col("__e", C), col("__a", C)));
        // the BAD pairs: cells joined by position that are not within
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        SqlSelect bad = new SqlSelect(List.of(new SqlSelect.Projection(col("__e", RN), RN, rnOut)),
                false, new SqlSource.Join(cte("__e"), cte("__a"), SqlSource.Join.Kind.INNER,
                        SqlExpr.Call.of(SqlFn.EQUAL, col("__e", RN), col("__a", RN))),
                SqlExpr.Call.of(SqlFn.NOT, SqlExpr.Call.of(SqlFn.COALESCE, within, new SqlExpr.BoolLit(false))),
                List.of(), null, null, List.of(), null, null, List.of(rnOut));
        SqlExpr sameCount = SqlExpr.Call.of(SqlFn.EQUAL, count("__e"), count("__a"));
        SqlExpr noBad = SqlExpr.Call.of(SqlFn.EQUAL, count("__p"), new SqlExpr.IntLit(0));
        return predicate(List.of(new SqlWith.Cte(GRID_E, one.wrapped()), new SqlWith.Cte(GRID_A, two.wrapped()),
                        new SqlWith.Cte("__e", e), new SqlWith.Cte("__a", a),
                        new SqlWith.Cte("__d", deltaRows), new SqlWith.Cte("__t", timeDeltaRows),
                        new SqlWith.Cte("__p", bad)),
                SqlExpr.Call.of(SqlFn.AND, sameCount, noBad),
                new SqlExpr.Cast(count("__e"), SqlType.Scalar.VARCHAR),
                new SqlExpr.Cast(count("__a"), SqlType.Scalar.VARCHAR));
    }

    /** A grid's cells row-major with a NUMERIC value ({@code __n}, any
     * numeric kind) and a TEMPORAL value in epoch seconds ({@code __s}). */
    private static SqlQuery toleranceCells(GridSide grid, List<Type> kinds, String named) {
        OutputCol cOut = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        OutputCol nOut = new OutputCol("__n", SqlType.Scalar.DOUBLE, true);
        OutputCol sOut = new OutputCol("__s", SqlType.Scalar.DOUBLE, true);
        List<SqlSelect.Projection> values = grid.wrapped() instanceof SqlSelect ws
                ? ws.projections().subList(0, Math.min(grid.width(), ws.projections().size()))
                : List.of();
        List<SqlExpr> cells = new ArrayList<>();
        List<SqlExpr> ns = new ArrayList<>();
        List<SqlExpr> secs = new ArrayList<>();
        for (int i = 0; i < grid.width(); i++) {
            cells.add(gridCell(grid, i));
            Type k = i < kinds.size() ? kinds.get(i) : null;
            String alias = i < values.size() ? values.get(i).alias() : null;
            SqlExpr raw = alias != null
                    ? SqlExpr.Column.of("w", alias, SqlType.Scalar.VARCHAR, true,
                            OutputCol.Origin.DERIVED)
                    : null;
            boolean numeric = k == Type.Primitive.INTEGER || k == Type.Primitive.FLOAT
                    || k == Type.Primitive.DECIMAL || k == Type.Primitive.NUMBER
                    || k instanceof Type.PrecisionDecimal;
            boolean temporal = k == Type.Primitive.DATE_TIME || k == Type.Primitive.STRICT_DATE
                    || k == Type.Primitive.DATE;
            ns.add(raw != null && numeric ? new SqlExpr.Cast(raw, SqlType.Scalar.DOUBLE)
                    : new SqlExpr.NullLit());
            // the grid's cells arrive DECODED as text (the fetch conformance:
            // nine-digit temporals) — a temporal cell casts back to a
            // timestamp for its epoch
            secs.add(raw != null && temporal
                    ? new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.EPOCH_SECONDS,
                            new SqlExpr.Cast(raw, SqlType.Scalar.TIMESTAMP)), SqlType.Scalar.DOUBLE)
                    : new SqlExpr.NullLit());
        }
        return new SqlSelect(List.of(
                        new SqlSelect.Projection(pick(cells), C, cOut),
                        new SqlSelect.Projection(position(grid.width()), RN, rnOut),
                        new SqlSelect.Projection(pick(ns), "__n", nOut),
                        new SqlSelect.Projection(pick(secs), "__s", sOut)),
                false, unpivot(grid, named), null,
                List.of(), null, null, List.of(), null, null, List.of(cOut, rnOut, nOut, sOut));
    }

    /** The JSON verdict (bucket 3): the document the database built (its
     * objects' keys sorted by {@link com.legend.sql.JsonKeyOrder}) against
     * the golden's canonical text (compact, keys sorted, the engine's
     * root {@code [x] ≡ x} applied at compile time) — byte-equal is the
     * verdict; a differing pair is unjudged with its evidence. */
    public static SqlQuery jsonText(SqlQuery eRows, SqlQuery aRows) {
        OneRow eo = firstOf("e", "__e");
        OneRow ao = firstOf("a", "__a");
        SqlExpr equal = SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, eo.col(C), ao.col(C));
        SqlExpr unjudged = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.NOT, equal),
                new SqlExpr.StringLit("json: not byte-equal (keys sorted, compact)"))),
                null);
        return predicateOver(List.of(new SqlWith.Cte("__e", eRows), new SqlWith.Cte("__a", aRows)),
                List.of(eo, ao), equal, eo.col(C), ao.col(C), unjudged);
    }

    /** {@code assertJsonStringsEqual} over a document whose root array has
     * no defined order: the golden's element texts against the document's
     * root elements, as a multiset. */
    public static SqlQuery jsonRootMultiset(SqlQuery eElementRows, SqlQuery aDocRows) {
        OutputCol vOut = new OutputCol("value", SqlType.Scalar.JSON, true);
        SqlExpr doc = new SqlExpr.Cast(SqlExpr.Column.of("d", aDocRows.outputs(), C), SqlType.Scalar.JSON);
        SqlSelect elements = CollectionRelations.rows(SqlExpr.Call.of(SqlFn.VARIANT_ELEMENTS, doc),
                "value", vOut, List.of(vOut), new SqlSource.Subselect(aDocRows, "d", null));
        SqlQuery aRows = rowsOf(new SqlExpr.Cast(SqlExpr.Column.of("w", List.of(vOut), "value"),
                SqlType.Scalar.VARCHAR), elements);
        return statement(eElementRows, aRows, true, true, true, List.of());
    }

    /** One verdict row from a predicate: {@code __verdict} never NULL, the
     * two evidence texts, no unjudged, no leniency. */
    // ── the ONE-LINE families: operands as ONE-ROW relations, cross-joined
    // ONCE into the __p facts row; the verdict row reads its columns — every
    // operand computed once, no scalar subquery (lean ladder rung 4) ───────
    private record OneRow(String alias, SqlQuery query, List<OutputCol> outs) {
        SqlExpr col(String name) {
            return SqlExpr.Column.of("p", pOuts(), pName(alias, name));
        }
        private List<OutputCol> pOuts() {
            List<OutputCol> out = new ArrayList<>();
            for (OutputCol o : outs) {
                out.add(new OutputCol(pName(alias, o.name()), o.type(), o.nullable()));
            }
            return out;
        }
        /** {@code __<alias>_<col>}: the facts row's column for an operand's column. */
        static String pName(String alias, String col) {
            return "__" + alias + "_" + col.replaceFirst("^_+", "");
        }
    }

    private static final String P = "__p";

    /** A TEXT assert's own verdict row (block-compiler rung 2a): the golden
     * SQL text against our rendered text, string equality — both constants of
     * the compiled body, FALSE by construction on an emitter that does not
     * copy the engine's spelling. The batch's APPEAL (the SQL-text referee)
     * then judges a failed row by ROWS; a byte-equal text needs no appeal. */
    public static SqlQuery textEquals(String golden, String ours) {
        OneRow e = constantOf("e", new SqlExpr.StringLit(golden), "__t", SqlType.Scalar.VARCHAR);
        OneRow a = constantOf("a", new SqlExpr.StringLit(ours), "__t", SqlType.Scalar.VARCHAR);
        return predicateOver(List.of(), List.of(e, a),
                SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, e.col("__t"), a.col("__t")),
                e.col("__t"), a.col("__t"), new SqlExpr.NullLit());
    }

    private static SqlQuery predicateOver(List<SqlWith.Cte> ctes, List<OneRow> ops,
            SqlExpr verdict, SqlExpr expected, SqlExpr actual, SqlExpr unjudged) {
        List<SqlSelect.Projection> pps = new ArrayList<>();
        List<OutputCol> pOuts = new ArrayList<>();
        SqlSource from = null;
        for (OneRow op : ops) {
            for (OutputCol o : op.outs()) {
                OutputCol po = new OutputCol(OneRow.pName(op.alias(), o.name()), o.type(), o.nullable());
                pps.add(new SqlSelect.Projection(SqlExpr.Column.of(op.alias(), op.outs(), o.name()),
                        po.name(), po));
                pOuts.add(po);
            }
            SqlSource src = new SqlSource.Subselect(op.query(), op.alias(), null);
            from = from == null ? src : new SqlSource.Join(from, src, SqlSource.Join.Kind.CROSS, null);
        }
        List<SqlWith.Cte> all = new ArrayList<>(ctes);
        all.add(new SqlWith.Cte(P, new SqlSelect(pps, false, java.util.Objects.requireNonNull(from),
                null, List.of(), null, null, List.of(), null, null, pOuts)));
        List<SqlSelect.Projection> ps = List.of(
                new SqlSelect.Projection(
                        SqlExpr.Call.of(SqlFn.COALESCE, verdict, new SqlExpr.BoolLit(false)), VERDICT,
                        new OutputCol(VERDICT, SqlType.Scalar.BOOLEAN, false)),
                new SqlSelect.Projection(expected, EXPECTED,
                        new OutputCol(EXPECTED, SqlType.Scalar.VARCHAR, true)),
                new SqlSelect.Projection(actual, ACTUAL,
                        new OutputCol(ACTUAL, SqlType.Scalar.VARCHAR, true)),
                new SqlSelect.Projection(unjudged, UNJUDGED,
                        new OutputCol(UNJUDGED, SqlType.Scalar.VARCHAR, true)),
                new SqlSelect.Projection(new SqlExpr.BoolLit(false), LENIENT,
                        new OutputCol(LENIENT, SqlType.Scalar.BOOLEAN, false)));
        SqlSelect body = new SqlSelect(ps, false, new SqlSource.Table(P, "p", pOuts, false), null,
                List.of(), null, null, List.of(), null, null, List.of());
        return new SqlWith(all, body);
    }

    private static OneRow countOf(String alias, String rowsCte) {
        OutputCol n = new OutputCol("__n", SqlType.Scalar.BIGINT, false);
        return new OneRow(alias, new SqlSelect(List.of(new SqlSelect.Projection(
                new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(col(rowsCte, RN)), false, List.of()), "__n", n)),
                false, cte(rowsCte), null, List.of(), null, null, List.of(), null, null, List.of(n)), List.of(n));
    }

    private static OneRow constantOf(String alias, SqlExpr value, String name, SqlType type) {
        OutputCol o = new OutputCol(name, type, false);
        return new OneRow(alias, new SqlSelect(List.of(new SqlSelect.Projection(value, name, o)),
                false, new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null, List.of(o)),
                List.of(o));
    }

    private static OneRow textOf(String alias, String factsCte) {
        return new OneRow(alias, new SqlSelect(List.of(
                new SqlSelect.Projection(SqlExpr.Column.of(factsCte, factOutputs(), F_TEXT), F_TEXT, factOutputs().get(0))),
                false, new SqlSource.Table(factsCte, factsCte, factOutputs(), false), null, List.of(), null, null,
                List.of(), null, null, List.of(factOutputs().get(0))), List.of(factOutputs().get(0)));
    }

    /** The first row of a rows CTE as a one-row relation ({@code __c}). */
    private static OneRow firstOf(String alias, String rowsCte) {
        OutputCol c = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        return new OneRow(alias, new SqlSelect(List.of(new SqlSelect.Projection(col(rowsCte, C), C, c)),
                false, cte(rowsCte), null, List.of(), null, null,
                List.of(new SqlSelect.SortKey(col(rowsCte, RN), true, null, null)), 1L, null, List.of(c)),
                List.of(c));
    }

    private static OneRow rowOf(String alias, SqlQuery oneRow) {
        return new OneRow(alias, oneRow, oneRow.outputs());
    }

    /** A side DECLARED exactly one as a one-row relation {@code (__c, value)}:
     * its canon text and value straight over its plan (a one-row seed LEFT
     * JOINed to the plan, so an empty plan is one NULL row), or over the
     * trimmed wrap when the wrap is not a plain projection. */
    public static SqlQuery scalarRow(SqlQuery wrapped, String canonColumn) {
        SqlSelect ws = (SqlSelect) wrapped;
        SqlSelect.Projection value = ws.projections().get(0);
        OutputCol cOut = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        OutputCol vOut = new OutputCol("value", value.out() != null ? value.out().type() : SqlType.Scalar.VARCHAR, true);
        if (plainProjection(ws)) {
            SqlExpr canon = asText(java.util.Objects.requireNonNull(projectionOf(ws, canonColumn), "canon column").expr());
            OutputCol seedOut = new OutputCol("__one", SqlType.Scalar.BIGINT, false);
            SqlSource seed = new SqlSource.Subselect(new SqlSelect(
                    List.of(new SqlSelect.Projection(new SqlExpr.IntLit(1), "__one", seedOut)),
                    false, new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null,
                    List.of(seedOut)), "__one", null);
            SqlSource from = new SqlSource.Join(seed, ws.from(), SqlSource.Join.Kind.LEFT, new SqlExpr.BoolLit(true));
            return new SqlSelect(List.of(new SqlSelect.Projection(canon, C, cOut),
                    new SqlSelect.Projection(value.expr(), "value", vOut)),
                    false, from, null, List.of(), null, null, List.of(), null, null, List.of(cOut, vOut));
        }
        SqlSelect.Projection chosen = java.util.Objects.requireNonNull(projectionOf(ws, canonColumn), "canon column");
        List<SqlSelect.Projection> kept = chosen == value ? List.of(value) : List.of(value, chosen);
        List<OutputCol> keptOuts = kept.stream().map(SqlSelect.Projection::out).filter(java.util.Objects::nonNull).toList();
        SqlSelect trimmed = new SqlSelect(kept, ws.distinct(), ws.from(), ws.where(), ws.groupBy(),
                ws.having(), ws.qualify(), List.of(), ws.limit(), ws.offset(), keptOuts);
        return new SqlSelect(List.of(
                new SqlSelect.Projection(asText(SqlExpr.Column.of("w", keptOuts, canonColumn)), C, cOut),
                new SqlSelect.Projection(SqlExpr.Column.of("w", keptOuts, java.util.Objects.requireNonNull(value.alias())), "value", vOut)),
                false, new SqlSource.Subselect(trimmed, "w", null), null, List.of(), null, null, List.of(), null, null,
                List.of(cOut, vOut));
    }

    private static SqlQuery predicate(List<SqlWith.Cte> ctes, SqlExpr verdict,
            SqlExpr expected, SqlExpr actual) {
        return predicate(ctes, verdict, expected, actual, new SqlExpr.NullLit());
    }

    private static SqlQuery predicate(List<SqlWith.Cte> ctes, SqlExpr verdict,
            SqlExpr expected, SqlExpr actual, SqlExpr unjudged) {
        List<SqlSelect.Projection> ps = List.of(
                new SqlSelect.Projection(
                        SqlExpr.Call.of(SqlFn.COALESCE, verdict, new SqlExpr.BoolLit(false)), VERDICT,
                        new OutputCol(VERDICT, SqlType.Scalar.BOOLEAN, false)),
                new SqlSelect.Projection(expected, EXPECTED,
                        new OutputCol(EXPECTED, SqlType.Scalar.VARCHAR, true)),
                new SqlSelect.Projection(actual, ACTUAL,
                        new OutputCol(ACTUAL, SqlType.Scalar.VARCHAR, true)),
                new SqlSelect.Projection(unjudged, UNJUDGED,
                        new OutputCol(UNJUDGED, SqlType.Scalar.VARCHAR, true)),
                new SqlSelect.Projection(new SqlExpr.BoolLit(false), LENIENT,
                        new OutputCol(LENIENT, SqlType.Scalar.BOOLEAN, false)));
        SqlSelect body = new SqlSelect(ps, false, new SqlSource.Dual(), null,
                List.of(), null, null, List.of(), null, null, List.of());
        return ctes.isEmpty() ? body : new SqlWith(ctes, body);   // a WITH needs expressions
    }

    /** A side's rows for the predicate forms: a grid's row canons, or a
     * scalar / collection side's canons (NULL values dropped). */
    public static SqlQuery sideRows(SqlQuery wrapped, boolean grid, String canonColumn, boolean many) {
        return grid ? gridRowCanons(wrapped)
                : canonRows(new Side(wrapped, canonColumn, many, false, false));
    }

    /** A side's rows for COUNTING only (size / emptiness): no canon needed —
     * a collection of instances the canon declines still has a row count;
     * NULL values are dropped as everywhere (pure has no null value). */
    public static SqlQuery countRows(SqlQuery plan) {
        SqlExpr where = null;
        if (plan instanceof SqlSelect vs && !vs.projections().isEmpty()
                && vs.projections().get(0).alias() != null) {
            where = SqlExpr.Call.of(SqlFn.IS_NOT_NULL,
                    SqlExpr.Column.of("w", vs.projections().get(0).alias(),
                            SqlType.Scalar.VARCHAR, true, OutputCol.Origin.DERIVED));
        }
        return rowsOf(new SqlExpr.NullLit(), new SqlExpr.NullLit(), plan, where);
    }

    /** A GRID's rows for counting: every row counts (a grid row is never
     * dropped), over the plan BEFORE the canon wrap — no cell is spelled. */
    public static SqlQuery gridCountRows(SqlQuery wrapped) {
        return rowsOf(new SqlExpr.NullLit(), new SqlExpr.NullLit(),
                CanonicalRenderSql.unwrapTdsCanon(wrapped), null);
    }

    /** Two grids: row canons against row canons. */
    public static SqlQuery gridPair(SqlQuery e, SqlQuery a, boolean multiset) {
        return statement(gridRowCanons(e), gridRowCanons(a), true, true, multiset, List.of());
    }

    /** Two grids of ONE schema (a golden brought to rows against the
     * rendered relation): row canons against row canons, the cells walked
     * positionally for the declared-Float leniency, each side's empty-is-NULL
     * columns read as the sentinel. */
    public static SqlQuery gridPair(GridSide e, GridSide a, boolean multiset) {
        return statement(gridRowCanons(e, GRID_E), gridRowCanons(a, GRID_A), true, true, multiset,
                List.of(), List.of(new SqlWith.Cte(GRID_E, e.wrapped()), new SqlWith.Cte(GRID_A, a.wrapped())),
                gridCellsRowMajor(e, GRID_E), gridCellsRowMajor(a, GRID_A));
    }

    private static SqlQuery statement(SqlQuery eRows, SqlQuery aRows,
            boolean eMany, boolean aMany, boolean byCanonText,
            List<SqlExpr.Case.When> moreUnjudged) {
        return statement(eRows, aRows, eMany, aMany, byCanonText, moreUnjudged, List.of());
    }

    private static SqlQuery statement(SqlQuery eRows, SqlQuery aRows,
            boolean eMany, boolean aMany, boolean byCanonText,
            List<SqlExpr.Case.When> moreUnjudged, List<SqlWith.Cte> extraCtes) {
        return statement(eRows, aRows, eMany, aMany, byCanonText, moreUnjudged, extraCtes,
                null, null);
    }

    /** The statement over two ROW SOURCES (each {@code (__c, __rn[, __v])});
     * {@code eCells}/{@code aCells} (each {@code (__c, __rn, __v)}, null =
     * no leniency for this form) are the POSITIONAL cell sequences the
     * declared 2-ULP Float leniency compares. */
    private static SqlQuery statement(SqlQuery eRows, SqlQuery aRows,
            boolean eMany, boolean aMany, boolean byCanonText,
            List<SqlExpr.Case.When> moreUnjudged, List<SqlWith.Cte> extraCtes,
            @com.legend.Nullable SqlQuery eCells, @com.legend.Nullable SqlQuery aCells) {
        return statementOf(new Folded("__e", eRows, sideFacts("__e", eMany, byCanonText)),
                new Folded("__a", aRows, sideFacts("__a", aMany, byCanonText)),
                moreUnjudged, extraCtes, eCells, aCells);
    }

    private static SqlQuery statementOf(Folded e, Folded a,
            List<SqlExpr.Case.When> moreUnjudged, List<SqlWith.Cte> extraCtes,
            @com.legend.Nullable SqlQuery eCells, @com.legend.Nullable SqlQuery aCells) {
        SqlQuery eRows = e.rows();
        SqlQuery aRows = a.rows();
        // THE GENERAL SHAPE (lean ladder, 2026-09-20): each side is a rows
        // relation (__c, __rn[, __v]) folded to ONE aggregate row (count, first,
        // joined, null count, tree count); the verdict row reads the two one-row
        // CTEs. No scalar subqueries, every side spelled once. The 2-ULP
        // leniency block exists only when a cell side carries a Float value —
        // a compile-time fact of the sides, not a special case — and folds to
        // one aggregate over the positional join.
        List<SqlWith.Cte> ctes = new ArrayList<>(extraCtes);
        if (eRows != null) {
            ctes.add(new SqlWith.Cte("__e", eRows));
        }
        if (aRows != null) {
            ctes.add(new SqlWith.Cte("__a", aRows));
        }
        ctes.add(new SqlWith.Cte("__se", e.facts()));
        ctes.add(new SqlWith.Cte("__sa", a.facts()));
        boolean cellsAreRows = eCells != null && eCells == eRows && aCells == aRows;
        boolean lenientPossible = eCells != null && aCells != null
                && (carriesValues(eCells) || carriesValues(aCells));
        SqlExpr lenient = null;
        SqlSource from = new SqlSource.Join(facts("__se", "e"), facts("__sa", "a"),
                SqlSource.Join.Kind.CROSS, null);
        if (lenientPossible) {
            String ec = cellsAreRows ? "__e" : "__ec";
            String ac = cellsAreRows ? "__a" : "__ac";
            if (!cellsAreRows) {
                ctes.add(new SqlWith.Cte("__ec", java.util.Objects.requireNonNull(eCells)));
                ctes.add(new SqlWith.Cte("__ac", java.util.Objects.requireNonNull(aCells)));
            }
            ctes.add(new SqlWith.Cte("__sl", pairFacts(ec, ac, !cellsAreRows)));
            from = new SqlSource.Join(from, pairFactsSource("__sl", "l"), SqlSource.Join.Kind.CROSS, null);
            // the cells of the equality form ARE its rows: the two counts are the
            // facts rows'; a grid's cells are counted by the pair facts
            SqlExpr sameCount = cellsAreRows
                    ? SqlExpr.Call.of(SqlFn.EQUAL, factCol("e", F_N), factCol("a", F_N))
                    : SqlExpr.Call.of(SqlFn.EQUAL, pairCol("l", "__ne"), pairCol("l", "__na"));
            lenient = SqlExpr.Call.of(SqlFn.AND, sameCount,
                    SqlExpr.Call.of(SqlFn.EQUAL, pairCol("l", "__bad"), new SqlExpr.IntLit(0)));
        }
        SqlExpr ex = factCol("e", F_TEXT);
        SqlExpr ax = factCol("a", F_TEXT);
        SqlExpr exact = SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, ex, ax);
        SqlExpr verdict = lenient == null ? exact : SqlExpr.Call.of(SqlFn.OR, exact, lenient);
        SqlExpr lenientOnly = lenient == null ? new SqlExpr.BoolLit(false)
                : SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.NOT, exact), lenient);
        List<SqlExpr.Case.When> whens = new ArrayList<>(moreUnjudged);
        whens.add(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.OR,
                        SqlExpr.Call.of(SqlFn.GREATER, factCol("e", F_NULLS), new SqlExpr.IntLit(0)),
                        SqlExpr.Call.of(SqlFn.GREATER, factCol("a", F_NULLS), new SqlExpr.IntLit(0))),
                new SqlExpr.StringLit("null-canon-cell")));
        whens.add(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.OR,
                        SqlExpr.Call.of(SqlFn.GREATER, factCol("e", F_TREES), new SqlExpr.IntLit(0)),
                        SqlExpr.Call.of(SqlFn.GREATER, factCol("a", F_TREES), new SqlExpr.IntLit(0))),
                new SqlExpr.StringLit("unclaimable tree cell")));
        SqlExpr unjudged = new SqlExpr.Case(whens, null);
        List<SqlSelect.Projection> ps = List.of(
                new SqlSelect.Projection(verdict, VERDICT,
                        new OutputCol(VERDICT, SqlType.Scalar.BOOLEAN, false)),
                new SqlSelect.Projection(ex, EXPECTED,
                        new OutputCol(EXPECTED, SqlType.Scalar.VARCHAR, false)),
                new SqlSelect.Projection(ax, ACTUAL,
                        new OutputCol(ACTUAL, SqlType.Scalar.VARCHAR, false)),
                new SqlSelect.Projection(unjudged, UNJUDGED,
                        new OutputCol(UNJUDGED, SqlType.Scalar.VARCHAR, true)),
                new SqlSelect.Projection(lenientOnly, LENIENT,
                        new OutputCol(LENIENT, SqlType.Scalar.BOOLEAN, false)));
        SqlSelect body = new SqlSelect(ps, false, from, null,
                List.of(), null, null, List.of(), null, null, List.of());
        return new SqlWith(ctes, body);
    }

    // ── the side FACTS row: one aggregate over a rows CTE ──────────────────
    private static final String F_TEXT = "__text";
    private static final String F_NULLS = "__nulls";
    private static final String F_TREES = "__trees";
    private static final String F_N = "__n";

    private static List<OutputCol> factOutputs() {
        return List.of(new OutputCol(F_TEXT, SqlType.Scalar.VARCHAR, true),
                new OutputCol(F_NULLS, SqlType.Scalar.BIGINT, false),
                new OutputCol(F_TREES, SqlType.Scalar.BIGINT, false),
                new OutputCol(F_N, SqlType.Scalar.BIGINT, false));
    }

    private static SqlSource facts(String cteName, String alias) {
        return new SqlSource.Table(cteName, alias, factOutputs(), false);
    }

    private static SqlExpr factCol(String alias, String col) {
        return SqlExpr.Column.of(alias, factOutputs(), col);
    }

    /** {@code SELECT <framed text>, nulls, trees FROM (SELECT COUNT, first,
     * STRING_AGG, null count, tree count FROM <rows>) AS s} — the side's
     * verdict facts, one row, the rows scanned once. */
    private static SqlQuery sideFacts(String rowsCte, boolean many, boolean byCanonText) {
        OutputCol nOut = new OutputCol("__n", SqlType.Scalar.BIGINT, false);
        OutputCol oneOut = new OutputCol("__one", SqlType.Scalar.VARCHAR, true);
        OutputCol joinedOut = new OutputCol("__joined", SqlType.Scalar.VARCHAR, true);
        OutputCol nullsOut = new OutputCol(F_NULLS, SqlType.Scalar.BIGINT, false);
        OutputCol treesOut = new OutputCol(F_TREES, SqlType.Scalar.BIGINT, false);
        List<OutputCol> aggOuts = List.of(nOut, oneOut, joinedOut, nullsOut, treesOut);
        SqlExpr c = col(rowsCte, C);
        SqlExpr rn = col(rowsCte, RN);
        SqlExpr key = byCanonText ? c : rn;
        SqlExpr one = new SqlAgg.Reducer(SqlAgg.Fn.MIN, List.of(new SqlExpr.Case(
                List.of(new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.EQUAL, rn, new SqlExpr.IntLit(1)), c)), null)),
                false, List.of());
        SqlSelect agg = new SqlSelect(List.of(
                new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(rn), false, List.of()), "__n", nOut),
                new SqlSelect.Projection(one, "__one", oneOut),
                new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG,
                        List.of(c, new SqlExpr.StringLit(", ")), false,
                        List.of(new SqlSelect.SortKey(key, true, null, null))), "__joined", joinedOut),
                new SqlSelect.Projection(countWhere(SqlExpr.Call.of(SqlFn.IS_NULL, c)), F_NULLS, nullsOut),
                new SqlSelect.Projection(countWhere(SqlExpr.Call.of(SqlFn.GREATER,
                        SqlExpr.Call.of(SqlFn.STRPOS, c, new SqlExpr.StringLit(CanonicalRenderSql.TREE_MARKER)),
                        new SqlExpr.IntLit(0))), F_TREES, treesOut)),
                false, cte(rowsCte), null, List.of(), null, null, List.of(), null, null, aggOuts);
        SqlExpr n = SqlExpr.Column.of("s", aggOuts, "__n");
        SqlExpr first = SqlExpr.Column.of("s", aggOuts, "__one");
        SqlExpr joined = SqlExpr.Column.of("s", aggOuts, "__joined");
        SqlExpr empty = new SqlExpr.StringLit("[]");
        SqlExpr text = many
                ? new SqlExpr.Case(List.of(
                        new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL, n, new SqlExpr.IntLit(0)), empty),
                        new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL, n, new SqlExpr.IntLit(1)), first)),
                        SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("["), joined, new SqlExpr.StringLit("]")))
                : SqlExpr.Call.of(SqlFn.COALESCE, first, empty);
        List<OutputCol> outs = factOutputs();
        return new SqlSelect(List.of(
                new SqlSelect.Projection(text, F_TEXT, outs.get(0)),
                new SqlSelect.Projection(SqlExpr.Column.of("s", aggOuts, F_NULLS), F_NULLS, outs.get(1)),
                new SqlSelect.Projection(SqlExpr.Column.of("s", aggOuts, F_TREES), F_TREES, outs.get(2)),
                new SqlSelect.Projection(n, F_N, outs.get(3))),
                false, new SqlSource.Subselect(agg, "s", null), null, List.of(), null, null,
                List.of(), null, null, outs);
    }

    /** {@code COUNT(CASE WHEN <cond> THEN 1 END)} — portable filtered count. */
    private static SqlExpr countWhere(SqlExpr cond) {
        return new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(new SqlExpr.Case(
                List.of(new SqlExpr.Case.When(cond, new SqlExpr.IntLit(1))), null)), false, List.of());
    }

    /** Whether a cells relation can carry a non-null {@code __v} (a Float
     * value) — statically, from its projections: every branch projects V as
     * a bare NULL when no column is declared Float. */
    private static boolean carriesValues(SqlQuery cells) {
        if (cells instanceof com.legend.sql.SqlUnion u) {
            return u.branches().stream().anyMatch(VerdictSql::carriesValues);
        }
        if (cells instanceof SqlSelect s) {
            for (SqlSelect.Projection p : s.projections()) {
                if (V.equals(p.alias())) {
                    return !(p.expr() instanceof SqlExpr.NullLit);
                }
            }
            return false;   // no value column at all
        }
        return true;   // an unknown shape keeps the leniency (never silently drops it)
    }

    // ── the positional PAIR facts: the leniency's one aggregate ─────────────
    private static List<OutputCol> pairOutputs() {
        return List.of(new OutputCol("__ne", SqlType.Scalar.BIGINT, false),
                new OutputCol("__na", SqlType.Scalar.BIGINT, false),
                new OutputCol("__bad", SqlType.Scalar.BIGINT, false));
    }

    private static SqlSource pairFactsSource(String cteName, String alias) {
        return new SqlSource.Table(cteName, alias, pairOutputs(), false);
    }

    private static SqlExpr pairCol(String alias, String col) {
        return SqlExpr.Column.of(alias, pairOutputs(), col);
    }

    /** {@code SELECT (SELECT COUNT FROM ec), (SELECT COUNT FROM ac), COUNT(bad
     * pairs) FROM ec JOIN ac ON rn} folded to ONE scan of each side: the two
     * counts as aggregates over a FULL join would need the dialect's emulation
     * on H2, so the counts ride as two subqueries of ONE row each — the only
     * subqueries the shape keeps, and only under a Float. */
    private static SqlQuery pairFacts(String ec, String ac, boolean withCounts) {
        List<OutputCol> outs = pairOutputs();
        SqlSource joined = new SqlSource.Join(cte(ec), cte(ac), SqlSource.Join.Kind.INNER,
                SqlExpr.Call.of(SqlFn.EQUAL, col(ec, RN), col(ac, RN)));
        SqlExpr ne = withCounts ? count(ec) : new SqlExpr.IntLit(0);
        SqlExpr na = withCounts ? count(ac) : new SqlExpr.IntLit(0);
        return new SqlSelect(List.of(
                new SqlSelect.Projection(ne, "__ne", outs.get(0)),
                new SqlSelect.Projection(na, "__na", outs.get(1)),
                new SqlSelect.Projection(countWhere(SqlExpr.Call.of(SqlFn.NOT, pairOk(ec, ac))), "__bad", outs.get(2))),
                false, joined, null, List.of(), null, null, List.of(), null, null, outs);
    }

    /** A HOST-CONSTANT side (a compile-time fact the pipeline answered
     * without SQL — a generated seed-data string, a rendered DDL text, a
     * folded literal) bound as a VALUES relation, so the comparison still
     * happens in the database (TWO_DESIGN_LEGS §2.6 blocker 1:
     * database-ADJUDICATED, not database-computed — the ledger says
     * which). Null when a value has no literal spelling here (the caller
     * reports it unjudged by kind). */
    public static @com.legend.Nullable SqlQuery constantPlan(List<Object> values) {
        List<List<SqlExpr>> rows = new ArrayList<>();
        SqlType type = SqlType.Scalar.VARCHAR;   // the EMPTY constant's column kind
        for (Object v : values) {
            SqlExpr lit;
            SqlType t;
            switch (v) {
                case String str -> { lit = new SqlExpr.StringLit(str); t = SqlType.Scalar.VARCHAR; }
                case Long l -> { lit = new SqlExpr.IntLit(l); t = SqlType.Scalar.BIGINT; }
                case Integer i -> { lit = new SqlExpr.IntLit(i); t = SqlType.Scalar.BIGINT; }
                case Boolean b -> { lit = new SqlExpr.BoolLit(b); t = SqlType.Scalar.BOOLEAN; }
                case null, default -> { return null; }
            }
            if (!rows.isEmpty() && type != t) {
                return null;   // a mixed constant collection has no one column type
            }
            type = t;
            rows.add(List.of(lit));
        }
        OutputCol out = new OutputCol("value", type, rows.isEmpty());
        SqlSource src = rows.isEmpty()
                ? new SqlSource.Subselect(new SqlSelect(
                        List.of(new SqlSelect.Projection(new SqlExpr.NullLit(), "value", out)),
                        false, new SqlSource.Dual(),
                        new SqlExpr.BoolLit(false), List.of(), null, null, List.of(),
                        null, null, List.of(out)), "k", null)
                : new SqlSource.Values(rows, List.of("value"), "k", List.of(out));
        return new SqlSelect(
                List.of(new SqlSelect.Projection(SqlExpr.Column.of("k", out), "value", out)),
                false, src, null, List.of(), null, null, List.of(), null, null, List.of(out));
    }

    /** The assert's position column of a fused batch statement. */
    public static final String INDEX = "__ix";

    /** LEG 3.4: several verdict statements as ONE. Every statement's CTEs
     * are hoisted to ONE top-level {@code WITH} under per-statement names
     * ({@code __e} of statement 3 is {@code __e_3}; the CTE's own alias is
     * unchanged, so its column references stand), and each verdict row is
     * a branch of a {@code UNION ALL} carrying its index — the per-assert
     * statement's exact shape, flattened. (A {@code WITH} inside a derived
     * table is NOT that shape: H2 blew its heap on one such branch, a
     * metamodel read with JSON aggregation — the Linux-independent catch of
     * 2026-09-20.) The caller reads each assert's row by its index (a
     * union's row order is not a contract). */
    public static SqlQuery batch(List<SqlQuery> statements,
            java.util.Map<String, SqlQuery> frames) {
        List<OutputCol> outs = batchOutputs();
        // leg 3.4 step 2: the frames the sides reference, DEFINED once at the
        // head (MATERIALIZED: every side sees the same rows), ahead of all
        List<SqlWith.Cte> ctes = new ArrayList<>();
        java.util.Set<String> used = new java.util.LinkedHashSet<>();
        for (SqlQuery st : statements) {
            used.addAll(com.legend.sql.FrameCtes.referenced(st));
        }
        for (var f : frames.entrySet()) {
            if (used.contains(f.getKey())) {
                ctes.add(new SqlWith.Cte(f.getKey(), f.getValue(), true));
            }
        }
        // A CTE whose body is IDENTICAL to one already hoisted (after the
        // renames) is not hoisted again: its name maps to the first. The asserts
        // of one body often read the same relation — each spelled its own copy
        // of the formatted grid (a 30 KB canon each, 2026-09-23). Walking in
        // order makes it compound: once two grids are one, the cell stacks over
        // them are identical too. Frames stay as they are (named, materialized).
        java.util.Map<SqlQuery, String> hoisted = new java.util.HashMap<>();
        List<SqlQuery> branches = new ArrayList<>(statements.size());
        for (int i = 0; i < statements.size(); i++) {
            SqlQuery st = statements.get(i);
            SqlQuery body = st;
            if (st instanceof SqlWith w) {
                java.util.Map<String, String> names = new java.util.LinkedHashMap<>();
                RenameCtes rename = new RenameCtes(names);
                for (SqlWith.Cte c : w.ctes()) {
                    SqlQuery q = rename.rewriteRoot(c.query());
                    String same = hoisted.get(q);
                    if (same != null && !c.materialized()) {
                        names.put(c.name(), same);
                        continue;
                    }
                    String name = c.name() + "_" + i;
                    names.put(c.name(), name);
                    hoisted.putIfAbsent(q, name);
                    ctes.add(new SqlWith.Cte(name, q, c.materialized()));
                }
                body = rename.rewriteRoot(w.body());
            }
            branches.add(indexed(i, body, outs));
        }
        SqlQuery union = branches.size() == 1 ? branches.get(0) : new SqlUnion(branches, true, outs);
        return ctes.isEmpty() ? union : new SqlWith(ctes, union);
    }

    /** {@code SELECT i AS __ix, <the row's columns>}: the verdict row's own
     * select with the index prepended (its projections matched to the batch
     * columns by label); any other body shape rides a derived table. */
    private static SqlQuery indexed(int i, SqlQuery body, List<OutputCol> outs) {
        List<SqlSelect.Projection> ps = new ArrayList<>(outs.size());
        ps.add(new SqlSelect.Projection(new SqlExpr.IntLit(i), INDEX, outs.get(0)));
        if (body instanceof SqlSelect sel && !sel.distinct() && sel.orderBy().isEmpty()
                && sel.limit() == null && sel.projections().size() == outs.size() - 1) {
            java.util.Map<String, SqlSelect.Projection> byLabel = new java.util.HashMap<>();
            for (SqlSelect.Projection pr : sel.projections()) {
                byLabel.put(pr.alias(), pr);
            }
            if (byLabel.size() == outs.size() - 1) {
                for (int c = 1; c < outs.size(); c++) {
                    SqlSelect.Projection pr = byLabel.get(outs.get(c).name());
                    if (pr == null) {
                        return derived(i, body, outs);
                    }
                    ps.add(new SqlSelect.Projection(pr.expr(), pr.alias(), outs.get(c)));
                }
                return new SqlSelect(ps, false, sel.from(), sel.where(), sel.groupBy(),
                        sel.having(), sel.qualify(), List.of(), null, null, outs);
            }
        }
        return derived(i, body, outs);
    }

    private static SqlQuery derived(int i, SqlQuery body, List<OutputCol> outs) {
        List<SqlSelect.Projection> ps = new ArrayList<>(outs.size());
        ps.add(new SqlSelect.Projection(new SqlExpr.IntLit(i), INDEX, outs.get(0)));
        for (int c = 1; c < outs.size(); c++) {
            String name = outs.get(c).name();
            ps.add(new SqlSelect.Projection(SqlExpr.Column.of("v", body.outputs(), name),
                    name, outs.get(c)));
        }
        return new SqlSelect(ps, false, new SqlSource.Subselect(body, "v", null), null,
                List.of(), null, null, List.of(), null, null, outs);
    }

    /** CTE references renamed (a table source named after a CTE; the
     * alias — what column references spell — is kept). */
    private static final class RenameCtes extends com.legend.sql.SqlRewriter {
        private final java.util.Map<String, String> names;

        RenameCtes(java.util.Map<String, String> names) {
            this.names = names;
        }

        @Override
        protected SqlSource source(SqlSource s) {
            if (s instanceof SqlSource.Table t) {
                String renamed = names.get(t.name());
                if (renamed != null) {
                    return new SqlSource.Table(renamed, t.alias(), t.outputs(), t.call());
                }
            }
            return s;
        }
    }

    private static List<OutputCol> batchOutputs() {
        return List.of(new OutputCol(INDEX, SqlType.Scalar.BIGINT, false),
                new OutputCol(VERDICT, SqlType.Scalar.BOOLEAN, false),
                new OutputCol(EXPECTED, SqlType.Scalar.VARCHAR, false),
                new OutputCol(ACTUAL, SqlType.Scalar.VARCHAR, false),
                new OutputCol(UNJUDGED, SqlType.Scalar.VARCHAR, true),
                new OutputCol(LENIENT, SqlType.Scalar.BOOLEAN, false));
    }

    /** The relation a fused batch statement returns: the index, then
     * {@link #schema}'s columns. */
    public static com.legend.compiler.element.type.Type.RelationType batchSchema() {
        List<com.legend.compiler.element.type.Type.RelationType.Column> cols = new ArrayList<>();
        cols.add(new com.legend.compiler.element.type.Type.RelationType.Column(INDEX,
                com.legend.compiler.element.type.Type.Primitive.INTEGER,
                new com.legend.compiler.element.type.Multiplicity.Bounded(1, 1)));
        cols.addAll(schema().columns());
        return new com.legend.compiler.element.type.Type.RelationType(cols);
    }

    /** The relation the verdict statement returns (the executor's
     * TABULAR decode needs the declared schema). */
    public static com.legend.compiler.element.type.Type.RelationType schema() {
        var one = new com.legend.compiler.element.type.Multiplicity.Bounded(1, 1);
        var opt = new com.legend.compiler.element.type.Multiplicity.Bounded(0, 1);
        var t = com.legend.compiler.element.type.Type.Primitive.STRING;
        return new com.legend.compiler.element.type.Type.RelationType(List.of(
                new com.legend.compiler.element.type.Type.RelationType.Column(VERDICT,
                        com.legend.compiler.element.type.Type.Primitive.BOOLEAN, one),
                new com.legend.compiler.element.type.Type.RelationType.Column(EXPECTED, t, one),
                new com.legend.compiler.element.type.Type.RelationType.Column(ACTUAL, t, one),
                new com.legend.compiler.element.type.Type.RelationType.Column(UNJUDGED, t, opt),
                new com.legend.compiler.element.type.Type.RelationType.Column(LENIENT,
                        com.legend.compiler.element.type.Type.Primitive.BOOLEAN, one)));
    }

    /** {@code SELECT canon AS __c, row_number() OVER () AS __rn FROM (wrapped) w}
     * — the side reduced to its deciding canon texts in arrival order. */
    /** The wrap's projection named {@code alias}, or null. */
    private static SqlSelect.@com.legend.Nullable Projection projectionOf(SqlSelect ws, String alias) {
        for (SqlSelect.Projection p : ws.projections()) {
            if (alias.equals(p.alias())) {
                return p;
            }
        }
        return null;
    }

    /** TEXT by contract: a cast to VARCHAR unless the expression is one already. */
    private static SqlExpr asText(SqlExpr e) {
        return e instanceof SqlExpr.Cast c && c.target() == SqlType.Scalar.VARCHAR ? e
                : new SqlExpr.Cast(e, SqlType.Scalar.VARCHAR);
    }

    /** A side declared exactly one: its facts row spelled straight over the
     * plan (ONE row by construction — a one-row seed LEFT JOINed to the plan,
     * so an empty plan still frames {@code []} exactly as the rows form does). */
    private static SqlQuery inlineFacts(Side s) {
        SqlSelect ws = (SqlSelect) s.wrapped();
        SqlExpr valueRef = ws.projections().get(0).expr();
        SqlExpr canon = asText(java.util.Objects.requireNonNull(
                projectionOf(ws, s.canonColumn()), "canon column").expr());
        List<OutputCol> outs = factOutputs();
        OutputCol seedOut = new OutputCol("__one", SqlType.Scalar.BIGINT, false);
        SqlSource seed = new SqlSource.Subselect(new SqlSelect(
                List.of(new SqlSelect.Projection(new SqlExpr.IntLit(1), "__one", seedOut)),
                false, new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null,
                List.of(seedOut)), "__one", null);
        SqlSource from = new SqlSource.Join(seed, ws.from(), SqlSource.Join.Kind.LEFT,
                new SqlExpr.BoolLit(true));
        SqlExpr flag = SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.IS_NOT_NULL, valueRef), SqlExpr.Call.of(SqlFn.IS_NULL, canon));
        return new SqlSelect(List.of(
                new SqlSelect.Projection(SqlExpr.Call.of(SqlFn.COALESCE, canon,
                        new SqlExpr.StringLit("[]")), F_TEXT, outs.get(0)),
                new SqlSelect.Projection(flagCount(flag), F_NULLS, outs.get(1)),
                new SqlSelect.Projection(flagCount(SqlExpr.Call.of(SqlFn.GREATER,
                        SqlExpr.Call.of(SqlFn.STRPOS, canon, new SqlExpr.StringLit(CanonicalRenderSql.TREE_MARKER)),
                        new SqlExpr.IntLit(0))), F_TREES, outs.get(2)),
                new SqlSelect.Projection(flagCount(SqlExpr.Call.of(SqlFn.IS_NOT_NULL, valueRef)), F_N, outs.get(3))),
                false, from, null, List.of(), null, null, List.of(), null, null, outs);
    }

    /** {@code CASE WHEN <cond> THEN 1 ELSE 0 END}. */
    private static SqlExpr flagCount(SqlExpr cond) {
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(cond, new SqlExpr.IntLit(1))),
                new SqlExpr.IntLit(0));
    }

    /** A many or optional side as rows {@code (__c, __rn[, __v])} at ONE
     * level: the chosen canon expression spliced over the plan; the wrap's
     * canonical order rides the row number's own window. */
    /** A wrap that only projects row expressions over its source: no
     * aggregate or window in a projection, no where / group / having /
     * qualify / distinct / limit / offset — its projections can be spliced
     * into a reader without changing what they compute. A JSON-document
     * side ({@code to_json(list(…))}) is NOT one: its value is an aggregate
     * over the plan's rows and must stay a layer. */
    private static boolean plainProjection(SqlSelect ws) {
        if (ws.where() != null || !ws.groupBy().isEmpty() || ws.having() != null
                || ws.qualify() != null || ws.distinct() || ws.limit() != null || ws.offset() != null) {
            return false;
        }
        boolean[] aggregate = {false};
        SqlSelect projectionsOnly = new SqlSelect(ws.projections(), false, new SqlSource.Dual(),
                null, List.of(), null, null, List.of(), null, null, List.of());
        new com.legend.sql.SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr e) {
                if (e instanceof SqlAgg || e instanceof SqlExpr.WindowCall
                        || e instanceof SqlExpr.JsonArrayAgg || e instanceof SqlExpr.ScalarSubquery
                        || e instanceof SqlExpr.Exists) {
                    aggregate[0] = true;
                }
                return e;
            }
        }.rewriteRoot(projectionsOnly);
        return !aggregate[0];
    }

    private static SqlQuery canonRows(Side s) {
        return canonRows(s, false);
    }

    /** {@code withValues}: the PAIR may be judged with the 2-ULP leniency,
     * so this side carries the value column — its Float value, or a typed
     * NULL when it has none (the column belongs to the pair, not the side). */
    private static SqlQuery canonRows(Side s, boolean withValues) {
        SqlSelect ws = (SqlSelect) s.wrapped();
        if (!plainProjection(ws)) {
            return canonRowsLayered(s, ws, withValues);
        }
        SqlExpr valueRef = ws.projections().get(0).expr();
        SqlExpr canon = asText(java.util.Objects.requireNonNull(
                projectionOf(ws, s.canonColumn()), "canon column").expr());
        // pure has no null VALUE: a NULL row of a value collection is dropped
        // and a NULL scalar reads as the EMPTY collection — the side drops the
        // row, so an empty [] frames '[]' and never counts as a null canon
        // cell (a NULL canon over a non-null value stays unjudged)
        SqlExpr where = SqlExpr.Call.of(SqlFn.IS_NOT_NULL, valueRef);
        // a canon-ordered side re-orders by __c in the aggregate; the wrap's
        // own ORDER BY is the row number's window otherwise
        List<SqlSelect.SortKey> order = s.byCanonText() ? List.of() : ws.orderBy();
        SqlExpr rn = new SqlExpr.WindowCall(new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER, List.of()),
                List.of(), order, null);
        OutputCol cOut = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        List<SqlSelect.Projection> ps = new ArrayList<>();
        ps.add(new SqlSelect.Projection(canon, C, cOut));
        ps.add(new SqlSelect.Projection(rn, RN, rnOut));
        List<OutputCol> outs = new ArrayList<>(List.of(cOut, rnOut));
        if (withValues) {
            OutputCol vOut = new OutputCol(V, SqlType.Scalar.DOUBLE, true);
            ps.add(new SqlSelect.Projection(valueColumn(valueRef, s.isFloat()), V, vOut));
            outs.add(vOut);
        }
        return new SqlSelect(ps, false, ws.from(), where, List.of(), null, null, List.of(),
                null, null, List.copyOf(outs));
    }

    /** A wrap that must stay a layer (an aggregate value, a filtered or
     * capped wrap): trimmed to its value and the ONE canon the side reads,
     * the rows read over it under {@code w}. */
    /** The pair's value column for one side: the Float value as DOUBLE, or a
     * typed NULL for a side that has none. */
    private static SqlExpr valueColumn(SqlExpr valueRef, boolean isFloat) {
        return isFloat ? new SqlExpr.Cast(valueRef, SqlType.Scalar.DOUBLE)
                : new SqlExpr.Cast(new SqlExpr.NullLit(), SqlType.Scalar.DOUBLE);
    }

    private static SqlQuery canonRowsLayered(Side s, SqlSelect ws, boolean withValues) {
        SqlSelect.Projection value = ws.projections().get(0);
        SqlSelect.Projection chosen = java.util.Objects.requireNonNull(
                projectionOf(ws, s.canonColumn()), "canon column");
        List<SqlSelect.Projection> kept = chosen == value ? List.of(value) : List.of(value, chosen);
        List<OutputCol> keptOuts = kept.stream().map(SqlSelect.Projection::out)
                .filter(java.util.Objects::nonNull).toList();
        // a canon-ordered side re-orders by __c in the aggregate; the wrap's
        // own ORDER BY stays for an ordered compare (the row number follows it)
        List<SqlSelect.SortKey> order = s.byCanonText() ? List.of() : ws.orderBy();
        SqlSelect trimmed = new SqlSelect(kept, ws.distinct(), ws.from(), ws.where(), ws.groupBy(),
                ws.having(), ws.qualify(), order, ws.limit(), ws.offset(), keptOuts);
        SqlExpr canon = asText(SqlExpr.Column.of("w", keptOuts, s.canonColumn()));
        SqlExpr valueRef = SqlExpr.Column.of("w", keptOuts, java.util.Objects.requireNonNull(value.alias()));
        SqlExpr rn = new SqlExpr.WindowCall(new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER, List.of()),
                List.of(), List.of(), null);
        OutputCol cOut = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        List<SqlSelect.Projection> ps = new ArrayList<>();
        ps.add(new SqlSelect.Projection(canon, C, cOut));
        ps.add(new SqlSelect.Projection(rn, RN, rnOut));
        List<OutputCol> outs = new ArrayList<>(List.of(cOut, rnOut));
        if (withValues) {
            OutputCol vOut = new OutputCol(V, SqlType.Scalar.DOUBLE, true);
            ps.add(new SqlSelect.Projection(valueColumn(valueRef, s.isFloat()), V, vOut));
            outs.add(vOut);
        }
        return new SqlSelect(ps, false, new SqlSource.Subselect(trimmed, "w", null),
                SqlExpr.Call.of(SqlFn.IS_NOT_NULL, valueRef), List.of(), null, null, List.of(),
                null, null, List.copyOf(outs));
    }

    /** A grid side's row canons in arrival order: {@code SELECT
     * CAST(w.__rowcanon AS VARCHAR) AS __c, row_number() OVER () AS __rn}. */
    private static SqlQuery gridRowCanons(SqlQuery wrapped) {
        return rowsOf(new SqlExpr.Cast(
                SqlExpr.Column.of("w", wrapped.outputs(), CanonicalRenderSql.ROW_CANON),
                SqlType.Scalar.VARCHAR), wrapped);
    }

    /** A grid's row canons; under a column's empty-is-NULL equivalence the
     * row canon is rebuilt from the cell canons ({@code __cell<i>} joined by
     * the cell separator, as the wrap builds {@code __rowcanon}). */
    private static SqlQuery gridRowCanons(GridSide grid, String named) {
        if (grid.emptyIsNull().stream().noneMatch(b -> b)) {
            return rowsOf(new SqlExpr.Cast(
                    SqlExpr.Column.of("w", grid.wrapped().outputs(), CanonicalRenderSql.ROW_CANON),
                    SqlType.Scalar.VARCHAR), new SqlExpr.NullLit(), over(grid, named), null);
        }
        SqlExpr row = null;
        for (int i = 0; i < grid.width(); i++) {
            SqlExpr cell = gridCell(grid, i);
            row = row == null ? cell : SqlExpr.Call.of(SqlFn.CONCAT,
                    SqlExpr.Call.of(SqlFn.CONCAT, row,
                            new SqlExpr.StringLit(CanonicalRenderSql.TDS_CELL_SEP)), cell);
        }
        return rowsOf(java.util.Objects.requireNonNull(row), new SqlExpr.NullLit(), over(grid, named), null);
    }

    /** One cell canon of a grid, the empty String canon ({@code ''}) read as
     * the sentinel where the column's grammar conflates it with NULL. */
    private static SqlExpr gridCell(GridSide grid, int i) {
        SqlExpr cell = new SqlExpr.Cast(SqlExpr.Column.of("w", grid.wrapped().outputs(),
                CanonicalRenderSql.CELL_CANON + i), SqlType.Scalar.VARCHAR);
        if (!grid.emptyIsNullAt(i)) {
            return cell;
        }
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.EQUAL, cell, new SqlExpr.StringLit("''")),
                new SqlExpr.StringLit(PlatformTypes.TDS_NULL_CELL))), cell);
    }

    /** A grid side's loose CELL pool: one row per cell of every row, the
     * per-cell canons the wrap projected ({@code __cell<i>}), stacked. */
    private static SqlQuery gridCellCanons(GridSide grid, String named) {
        OutputCol cOut = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        List<SqlExpr> cells = new ArrayList<>();
        for (int i = 0; i < grid.width(); i++) {
            cells.add(new SqlExpr.Cast(SqlExpr.Column.of("w", grid.wrapped().outputs(),
                    CanonicalRenderSql.CELL_CANON + i), SqlType.Scalar.VARCHAR));
        }
        SqlQuery stacked = new SqlSelect(List.of(new SqlSelect.Projection(pick(cells), C, cOut)),
                false, unpivot(grid, named), null,
                List.of(), null, null, List.of(), null, null, List.of(cOut));
        // the pool's arrival order is meaningless (a multiset by definition);
        // __rn exists for the frame's count and the LIMIT 1 read only
        return rowsOf(SqlExpr.Column.of("w", List.of(cOut), C), stacked);
    }

    /** The peer's element canons as cells: the literal-channel canon,
     * the golden's quoted {@code 'TDSNull'} cell spelled as the bare
     * sentinel on the expected side. */
    private static SqlQuery peerCells(PeerSide peer) {
        SqlExpr c = new SqlExpr.Cast(
                SqlExpr.Column.of("w", peer.wrapped().outputs(), peer.canonColumn()),
                SqlType.Scalar.VARCHAR);
        if (peer.expected()) {
            c = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                    SqlExpr.Call.of(SqlFn.EQUAL, c, new SqlExpr.StringLit("'TDSNull'")),
                    new SqlExpr.StringLit("TDSNull"))), c);
        }
        SqlExpr value = peer.wrapped() instanceof SqlSelect ps && !ps.projections().isEmpty()
                ? doubleValue(ps.projections().get(0), "w", peer.isFloat()) : new SqlExpr.NullLit();
        // pure has no null VALUE: the peer drops a NULL-value row like every
        // other side (an empty [] peer is one NULL row → zero cells)
        SqlExpr where = peer.wrapped() instanceof SqlSelect vs && !vs.projections().isEmpty()
                && vs.projections().get(0).alias() != null
                ? SqlExpr.Call.of(SqlFn.IS_NOT_NULL,
                        SqlExpr.Column.of("w", vs.projections().get(0).alias(),
                                SqlType.Scalar.VARCHAR, true, OutputCol.Origin.DERIVED))
                : null;
        return rowsOf(c, value, peer.wrapped(), where);
    }

    /** The peer's cells chunked into rows of {@code width}: cells in
     * arrival order, grouped by {@code (rn - 1) - ((rn - 1) MOD width)}
     * (integer arithmetic on every dialect), each group joined by the
     * cell separator in cell order — the same framing
     * {@code TdsCompare.peerRowCanons} writes in Java. Reads the
     * {@code __peer} CTE ({@link #peerCells}). */
    private static SqlQuery peerRowCanons(PeerSide peer, int width) {
        SqlExpr rn = col("__peer", RN);
        SqlExpr rn0 = SqlExpr.Call.of(SqlFn.MINUS, rn, new SqlExpr.IntLit(1));
        SqlExpr group = SqlExpr.Call.of(SqlFn.MINUS, rn0,
                SqlExpr.Call.of(SqlFn.MOD, rn0, new SqlExpr.IntLit(width)));
        OutputCol cOut = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        SqlExpr joined = new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG,
                List.of(col("__peer", C), new SqlExpr.StringLit(CanonicalRenderSql.TDS_CELL_SEP)),
                false, List.of(new SqlSelect.SortKey(rn, true, null, null)));
        SqlExpr first = new SqlAgg.Reducer(SqlAgg.Fn.MIN, List.of(rn), false, List.of());
        return new SqlSelect(List.of(
                        new SqlSelect.Projection(joined, C, cOut),
                        new SqlSelect.Projection(first, RN, rnOut)),
                false, cte("__peer"), null, List.of(group), null, null,
                List.of(), null, null, List.of(cOut, rnOut));
    }

    /** {@code SELECT <canon> AS __c, row_number() OVER () AS __rn, <value> AS __v
     * FROM (source) w} — {@code value} the cell's DOUBLE value when the cell
     * IS a Float (the leniency's operand), NULL otherwise. */
    private static SqlQuery rowsOf(SqlExpr canon, SqlQuery source) {
        return rowsOf(canon, new SqlExpr.NullLit(), source, null);
    }

    private static SqlQuery rowsOf(SqlExpr canon, SqlExpr value, SqlQuery source,
            @com.legend.Nullable SqlExpr where) {
        return rowsOf(canon, value, new SqlSource.Subselect(source, "w", null), where);
    }

    /** A grid side read through its NAMED CTE, aliased {@code w} as every
     * cell and row reference spells it. A grid's rows and each of its
     * columns' cell branches read the one wrapped plan; inlined, the plan —
     * every formatted cell and the row canon — was spelled once per reader:
     * width + 1 copies per side (columnValueDifferenceTest, 8 columns: a
     * 1.2 MB statement; H2's parser, which copies the token list per
     * subquery, needed over 4 GB to read it — 2026-09-23). */
    private static SqlSource over(GridSide grid, String named) {
        return new SqlSource.Table(named, "w", grid.wrapped().outputs(), false);
    }

    private static SqlQuery rowsOf(SqlExpr canon, SqlExpr value, SqlSource source,
            @com.legend.Nullable SqlExpr where) {
        SqlExpr rn = new SqlExpr.WindowCall(
                new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER, List.of()),
                List.of(), List.of(), null);
        OutputCol cOut = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        OutputCol vOut = new OutputCol(V, SqlType.Scalar.DOUBLE, true);
        return new SqlSelect(List.of(
                        new SqlSelect.Projection(canon, C, cOut),
                        new SqlSelect.Projection(rn, RN, rnOut),
                        new SqlSelect.Projection(value, V, vOut)),
                false, source, where,
                List.of(), null, null, List.of(), null, null,
                List.of(cOut, rnOut, vOut));
    }

    /** The DOUBLE value of a projection when its type fact says DOUBLE,
     * else NULL (only Float pairs take the leniency). */
    private static SqlExpr doubleValue(SqlSelect.Projection p, String table, boolean isFloat) {
        return isFloat && p.alias() != null
                ? new SqlExpr.Cast(SqlExpr.Column.of(table, p.alias(), SqlType.Scalar.DOUBLE,
                        true, OutputCol.Origin.DERIVED), SqlType.Scalar.DOUBLE)
                : new SqlExpr.NullLit();
    }

    /** A grid's cells ROW-MAJOR with their Float values: one row per cell,
     * {@code __rn = (row - 1) * width + i + 1} — the positional sequence
     * the leniency walks against the peer's cells. */
    private static SqlQuery gridCellsRowMajor(GridSide grid, String named) {
        OutputCol cOut = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        OutputCol vOut = new OutputCol(V, SqlType.Scalar.DOUBLE, true);
        List<SqlSelect.Projection> values = grid.wrapped() instanceof SqlSelect ws
                ? ws.projections().subList(0, Math.min(grid.width(), ws.projections().size()))
                : List.of();
        List<SqlExpr> cells = new ArrayList<>();
        List<SqlExpr> vs = new ArrayList<>();
        for (int i = 0; i < grid.width(); i++) {
            cells.add(gridCell(grid, i));
            boolean isFloat = i < grid.floatColumns().size() && grid.floatColumns().get(i);
            vs.add(i < values.size() ? doubleValue(values.get(i), "w", isFloat)
                    : new SqlExpr.NullLit());
        }
        return new SqlSelect(List.of(
                        new SqlSelect.Projection(pick(cells), C, cOut),
                        new SqlSelect.Projection(position(grid.width()), RN, rnOut),
                        new SqlSelect.Projection(pick(vs), V, vOut)),
                false, unpivot(grid, named), null,
                List.of(), null, null, List.of(), null, null, List.of(cOut, rnOut, vOut));
    }

    // ── THE UNPIVOT (2026-09-23): a grid as one row per cell, reading the grid
    // ONCE. It was one UNION ALL branch per column, each re-reading the grid
    // and numbering its rows on its own: width references to the grid, which
    // H2 re-expands per reference while planning (an 8-column grid needed
    // gigabytes to plan), and width row numberings that had to agree on
    // arrival order. Now: the grid's rows numbered once (__r), cross-joined
    // with the column positions 1..width (k.__i); each output row picks its
    // cell by position.

    private static final String ROW = "__r";
    private static final String POS = "__i";

    /** {@code (SELECT w.*, ROW_NUMBER() OVER () AS __r FROM <grid> w) w
     * CROSS JOIN (VALUES 1, …, width) k(__i)}. */
    private static SqlSource unpivot(GridSide grid, String named) {
        List<OutputCol> outs = new ArrayList<>(grid.wrapped().outputs());
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (OutputCol o : grid.wrapped().outputs()) {
            ps.add(new SqlSelect.Projection(
                    SqlExpr.Column.of("w", grid.wrapped().outputs(), o.name()), o.name(), o));
        }
        OutputCol r = new OutputCol(ROW, SqlType.Scalar.BIGINT, false);
        ps.add(new SqlSelect.Projection(new SqlExpr.WindowCall(
                new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER, List.of()),
                List.of(), List.of(), null), ROW, r));
        outs.add(r);
        SqlSource rows = new SqlSource.Subselect(new SqlSelect(ps, false, over(grid, named), null,
                List.of(), null, null, List.of(), null, null, List.copyOf(outs)), "w", null);
        List<List<SqlExpr>> positions = new ArrayList<>();
        for (int p = 1; p <= grid.width(); p++) {
            positions.add(List.of(new SqlExpr.IntLit(p)));
        }
        SqlSource k = new SqlSource.Values(positions, List.of(POS), "k",
                List.of(new OutputCol(POS, SqlType.Scalar.INTEGER, false)));
        return new SqlSource.Join(rows, k, SqlSource.Join.Kind.CROSS, null);
    }

    private static SqlExpr pos() {
        return SqlExpr.Column.of("k", List.of(new OutputCol(POS, SqlType.Scalar.INTEGER, false)), POS);
    }

    /** {@code CASE WHEN k.__i = 1 THEN e1 … END}: the cell at this row's position
     * (NULL when every candidate is NULL). */
    private static SqlExpr pick(List<SqlExpr> perColumn) {
        if (perColumn.stream().allMatch(e -> e instanceof SqlExpr.NullLit)) {
            return new SqlExpr.NullLit();
        }
        List<SqlExpr.Case.When> whens = new ArrayList<>();
        for (int i = 0; i < perColumn.size(); i++) {
            whens.add(new SqlExpr.Case.When(
                    SqlExpr.Call.of(SqlFn.EQUAL, pos(), new SqlExpr.IntLit(i + 1)), perColumn.get(i)));
        }
        return new SqlExpr.Case(whens, null);
    }

    /** {@code (w.__r - 1) * width + k.__i}: the cell's row-major position. */
    private static SqlExpr position(int width) {
        SqlExpr r = SqlExpr.Column.of("w", ROW, SqlType.Scalar.BIGINT, false, OutputCol.Origin.DERIVED);
        return SqlExpr.Call.of(SqlFn.PLUS,
                SqlExpr.Call.of(SqlFn.TIMES,
                        SqlExpr.Call.of(SqlFn.MINUS, r, new SqlExpr.IntLit(1)),
                        new SqlExpr.IntLit(width)),
                pos());
    }

    /** The declared 2-ULP Float leniency as ONE predicate over two cell
     * sequences: same length, and every position either canon-equal or a
     * finite Double pair within {@code 2 * ulp(max(|x|, |y|))}, ulp spelled
     * {@code 2^(floor(log2(max)) - 52)} through {@code ln} (a boundary at
     * an exact power of two may differ from Math.ulp by one binade — the
     * differential gate measures it). */
    private static SqlExpr pairOk(String ec, String ac) {
        SqlExpr ve = col(ec, V);
        SqlExpr va = col(ac, V);
        SqlExpr big = SqlExpr.Call.of(SqlFn.GREATEST,
                SqlExpr.Call.of(SqlFn.ABS, ve), SqlExpr.Call.of(SqlFn.ABS, va));
        SqlExpr finite = SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.LESS_EQUAL, SqlExpr.Call.of(SqlFn.ABS, ve),
                        new SqlExpr.FloatLit(Double.MAX_VALUE)),
                SqlExpr.Call.of(SqlFn.LESS_EQUAL, SqlExpr.Call.of(SqlFn.ABS, va),
                        new SqlExpr.FloatLit(Double.MAX_VALUE)));
        SqlExpr twoUlp = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.EQUAL, big, new SqlExpr.FloatLit(0.0)),
                new SqlExpr.FloatLit(0.0))),
                SqlExpr.Call.of(SqlFn.TIMES, new SqlExpr.FloatLit(2.0),
                        SqlExpr.Call.of(SqlFn.POW, new SqlExpr.FloatLit(2.0),
                                SqlExpr.Call.of(SqlFn.MINUS,
                                        SqlExpr.Call.of(SqlFn.FLOOR,
                                                SqlExpr.Call.of(SqlFn.DIVIDE,
                                                        SqlExpr.Call.of(SqlFn.LN, big),
                                                        SqlExpr.Call.of(SqlFn.LN, new SqlExpr.FloatLit(2.0)))),
                                        new SqlExpr.IntLit(52)))));
        SqlExpr pairOk = SqlExpr.Call.of(SqlFn.OR,
                SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, col(ec, C), col(ac, C)),
                SqlExpr.Call.of(SqlFn.AND,
                        SqlExpr.Call.of(SqlFn.AND,
                                SqlExpr.Call.of(SqlFn.IS_NOT_NULL, ve),
                                SqlExpr.Call.of(SqlFn.IS_NOT_NULL, va)),
                        SqlExpr.Call.of(SqlFn.AND, finite,
                                SqlExpr.Call.of(SqlFn.LESS_EQUAL,
                                        SqlExpr.Call.of(SqlFn.ABS,
                                                SqlExpr.Call.of(SqlFn.MINUS, ve, va)),
                                        twoUlp))));
        // a bad position exists?
        return pairOk;
    }

    private static List<OutputCol> cteOutputs() {
        return List.of(new OutputCol(C, SqlType.Scalar.VARCHAR, true),
                new OutputCol(RN, SqlType.Scalar.BIGINT, false),
                new OutputCol(V, SqlType.Scalar.DOUBLE, true));
    }

    private static SqlSource cte(String name) {
        return new SqlSource.Table(name, name, cteOutputs(), false);
    }

    private static SqlExpr col(String cteName, String col) {
        return SqlExpr.Column.of(cteName, cteOutputs(), col);
    }

    /** {@code (SELECT <agg> FROM cte)} as a scalar. */
    private static SqlExpr scalarOver(String cteName, SqlExpr projected,
            String alias, SqlType type, @com.legend.Nullable Long limit) {
        OutputCol out = new OutputCol(alias, type, true);
        return new SqlExpr.ScalarSubquery(new SqlSelect(
                List.of(new SqlSelect.Projection(projected, alias, out)),
                false, cte(cteName), null, List.of(), null, null,
                limit == null ? List.of()
                        : List.of(new SqlSelect.SortKey(col(cteName, RN), true, null, null)),
                limit, null, List.of(out)));
    }

    private static SqlExpr count(String cteName) {
        return scalarOver(cteName,
                new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(col(cteName, RN)),
                        false, List.of()),
                "__n", SqlType.Scalar.BIGINT, null);
    }

    /** {@code (SELECT count(*) FROM cte WHERE strpos(__c, marker) > 0) > 0}. */
    /** {@code (SELECT count(*) FROM cte WHERE __c IS NULL) > 0}. */
    /** The spec's side framing in SQL. */
}
