package com.legend.lowering;

import com.legend.builtin.Pure;
import com.legend.compiler.element.TypedFunction;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlSelect;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
/**
 * Window-native dispatch: the RESOLVED overload of a window-column body
 * ({@code $p->lag($r)}, {@code rank($p, $w, $r)}) &rarr; its SQL window
 * function and position kind. Identity-keyed like {@link Scalars};
 * unregistered = loud error. The kind decides the {@code SqlAgg} branch:
 * RANKING functions take no column argument; VALUE functions take the column
 * the wrapping property access names.
 */
final class Windows {

    enum Kind { RANKING, VALUE }

    record WindowFn(com.legend.sql.SqlAgg.Fn sqlName, Kind kind) {
    }

    private static final Map<String, WindowFn> FNS = new HashMap<>();

    private Windows() {
    }

    private static void family(com.legend.sql.SqlAgg.Fn sqlName, Kind kind, String pureName) {
        for (String f : Pure.nativeKeysAt(pureName)) {
            FNS.put(f, new WindowFn(sqlName, kind));
        }
    }

    /** SQL reducer names for the 4-arg colToAgg window aggregates. */
    private static final Map<String, com.legend.sql.SqlAgg.Fn> AGGREGATES = new HashMap<>();

    /** The signature keys of the window functions — CLAIMS, kind WINDOW_FN
     *  ({@link com.legend.builtin.Claims}). */
    static java.util.Set<String> fnKeys() {
        return java.util.Collections.unmodifiableSet(FNS.keySet());
    }

    /** The signature keys of the window-only aggregates — CLAIMS, kind WINDOW_AGG. */
    static java.util.Set<String> aggregateKeys() {
        return java.util.Collections.unmodifiableSet(AGGREGATES.keySet());
    }

    static {
        family(com.legend.sql.SqlAgg.Fn.ROW_NUMBER, Kind.RANKING, "rowNumber");
        family(com.legend.sql.SqlAgg.Fn.RANK, Kind.RANKING, "rank");
        family(com.legend.sql.SqlAgg.Fn.DENSE_RANK, Kind.RANKING, "denseRank");
        family(com.legend.sql.SqlAgg.Fn.PERCENT_RANK, Kind.RANKING, "percentRank");
        family(com.legend.sql.SqlAgg.Fn.CUME_DIST, Kind.RANKING, "cumulativeDistribution");
        family(com.legend.sql.SqlAgg.Fn.NTILE, Kind.RANKING, "ntile");
        family(com.legend.sql.SqlAgg.Fn.LAG, Kind.VALUE, "lag");
        family(com.legend.sql.SqlAgg.Fn.LEAD, Kind.VALUE, "lead");
        family(com.legend.sql.SqlAgg.Fn.FIRST_VALUE, Kind.VALUE, "first");
        family(com.legend.sql.SqlAgg.Fn.LAST_VALUE, Kind.VALUE, "last");
        family(com.legend.sql.SqlAgg.Fn.NTH_VALUE, Kind.VALUE, "nth");
        // variance/stdDev window forms exist ONLY in the _Window-bearing
        // overload — the reducer overloads of the same names must stay out.
        windowOnly(com.legend.sql.SqlAgg.Fn.VARIANCE, Kind.VALUE, "variance");
        windowOnly(com.legend.sql.SqlAgg.Fn.STDDEV, Kind.VALUE, "stdDev");
        // The 4-arg colToAgg window AGGREGATES — real pure has exactly these
        // (average, stdDevPopulation; everything else windows via the
        // agg-col spelling). Keyed by the _Window-bearing overload only —
        // the REDUCER overloads of the same names are Aggregates' domain.
        aggregate(com.legend.sql.SqlAgg.Fn.AVG, "average");
        aggregate(com.legend.sql.SqlAgg.Fn.STDDEV_POP, "stdDevPopulation");
    }

    /** Real pure's window-frame class — overload selection is by this
     * EXACT parameter FQN (audit 15: was a contains("_Window") key probe). */
    private static final String WINDOW_CLASS =
            "meta::pure::functions::relation::_Window";

    private static void windowOnly(com.legend.sql.SqlAgg.Fn sqlName, Kind kind, String pureName) {
        for (String key : Pure.nativeKeysAt(pureName, WINDOW_CLASS)) {
            FNS.put(key, new WindowFn(sqlName, kind));
        }
    }

    private static void aggregate(com.legend.sql.SqlAgg.Fn sqlName, String pureName) {
        for (String key : Pure.nativeKeysAt(pureName, WINDOW_CLASS)) {
            AGGREGATES.put(key, sqlName);
        }
    }

    /** The SQL reducer for a 4-arg window-aggregate callee, or null. */
    static com.legend.sql.SqlAgg.@com.legend.Nullable Fn aggregate(TypedFunction callee) {
        return AGGREGATES.get(callee.signatureKey());
    }

    /** The window fn for a resolved overload, or null when it is not a window native. */
    static @com.legend.Nullable WindowFn lookup(TypedFunction callee) {
        return FNS.get(callee.signatureKey());
    }
    /**
     * Window position accepts COMPOSED aggregates (wavg = SUM(v*w)/SUM(w),
     * hashCode = HASH(LIST(x))): every bare reducer inside the value
     * expression gets the SAME window spec.
     */
    static SqlExpr windowize(SqlExpr e, List<SqlExpr> partitionBy,
            List<SqlSelect.SortKey> orderBy,
            SqlExpr.WindowCall.@com.legend.Nullable Frame frame) {
        return switch (e) {
            case SqlExpr.TempTableInSplice t -> t;
            case SqlAgg.Reducer r ->
                    new SqlExpr.WindowCall(r, partitionBy, orderBy, frame);
            case SqlExpr.Call c -> new SqlExpr.Call(c.fn(), c.args().stream()
                    .map(x -> windowize(x, partitionBy, orderBy, frame)).toList());
            case SqlExpr.Cast c ->
                    new SqlExpr.Cast(windowize(c.value(), partitionBy, orderBy, frame),
                            c.target());
            // A reducer under a CASE arm must window too (audit: the first
            // agg recipe that guards with CASE would render bare).
            case SqlExpr.Case cs -> new SqlExpr.Case(
                    cs.whens().stream().map(w -> new SqlExpr.Case.When(
                            windowize(w.condition(), partitionBy, orderBy, frame),
                            windowize(w.then(), partitionBy, orderBy, frame))).toList(),
                    cs.otherwise() == null ? null
                            : windowize(cs.otherwise(), partitionBy, orderBy, frame));
            // Composite carriers: a reducer anywhere inside must window
            // (audit 15: the open default let these render bare aggregates).
            case SqlExpr.ArrayLit a -> new SqlExpr.ArrayLit(a.elements().stream()
                    .map(x -> windowize(x, partitionBy, orderBy, frame)).toList());
            case SqlExpr.StructLit s -> new SqlExpr.StructLit(s.fields().stream()
                    .map(f -> new SqlExpr.StructLit.Field(f.name(),
                            windowize(f.value(), partitionBy, orderBy, frame),
                            f.declared()))
                    .toList());
            case SqlExpr.StructGet g -> new SqlExpr.StructGet(
                    windowize(g.source(), partitionBy, orderBy, frame), g.field());
            case SqlExpr.JsonObject j -> new SqlExpr.JsonObject(j.kv().stream()
                    .map(x -> windowize(x, partitionBy, orderBy, frame)).toList());
            case SqlExpr.JsonArray j -> new SqlExpr.JsonArray(j.elements().stream()
                    .map(x -> windowize(x, partitionBy, orderBy, frame)).toList());
            case SqlExpr.FoldCall f -> new SqlExpr.FoldCall(
                    windowize(f.source(), partitionBy, orderBy, frame), f.lambda(),
                    windowize(f.init(), partitionBy, orderBy, frame),
                    f.accIsList(), f.homogeneous());
            // Ordered-set / array aggregates are not expressible as OVER()
            // window functions in this IR — bare emission would silently
            // change grouping semantics; the shape stays loud until built.
            case SqlExpr.OrderedListAgg ignored ->
                    throw new com.legend.error.NotImplementedException(
                            "ordered list aggregate in window position");
            case SqlExpr.JsonArrayAgg ignored ->
                    throw new com.legend.error.NotImplementedException(
                            "json array aggregate in window position");
            // Subqueries own their scope: a reducer inside aggregates THERE,
            // never over this window. Already-windowed calls keep their spec.
            case SqlExpr.Exists x -> x;
            case SqlExpr.ScalarSubquery s -> s;
            case SqlExpr.InSubquery i -> i;
            case SqlExpr.Quantified q -> q;
            case SqlExpr.CheckedOne co -> co;
            case SqlExpr.CompactList cl -> cl;
            case SqlExpr.DeferredTdsString dtds -> dtds;
            case SqlExpr.WindowCall w -> w;
            case SqlExpr.Lambda l -> l;
            case SqlExpr.Group g -> new SqlExpr.Group(
                    windowize(g.inner(), partitionBy, orderBy, frame));
            // Leaves: no reducer can hide below.
            case SqlExpr.PlanParam ignored -> e;
            case SqlExpr.Column ignored -> e;
            case SqlExpr.RowOrder ignored -> e;
            case SqlExpr.ReduceCollection ignored -> e;
            case SqlExpr.Membership ignored -> e;
            case SqlExpr.Star ignored -> e;
            case SqlExpr.StarExcept ignored -> e;
            case SqlExpr.StringLit ignored -> e;
            case SqlExpr.IntLit ignored -> e;
            case SqlExpr.FloatLit ignored -> e;
            case SqlExpr.DecimalLit ignored -> e;
            case SqlExpr.BoolLit ignored -> e;
            case SqlExpr.NullLit ignored -> e;
            case SqlExpr.DateLit ignored -> e;
            case SqlExpr.TimestampLit ignored -> e;
            case SqlExpr.FormatLit ignored -> e;
        };
    }


    /** The checker-classified frame, mapped 1:1 to the SQL IR shape.
     * LITERAL bound validation lives HERE (moved from the checker —
     * the spec observes the error lazily at eval via assertError; a
     * bad frame still never renders). */
    static SqlExpr.WindowCall.Frame sqlFrame(
            com.legend.compiler.spec.typed.WindowFrame f) {
        Double from = numericOf(f.from());
        Double to = numericOf(f.to());
        if (from != null && to != null && from > to) {
            // real rows()/_range() assert text verbatim (PCT parity)
            throw new com.legend.error.ModelException(
                    com.legend.error.LegendCompileException.Phase.TYPE,
                    "Invalid window frame boundary - lower bound of window"
                            + " frame cannot be greater than the upper"
                            + " bound!");
        }
        return new SqlExpr.WindowCall.Frame(
                f.kind() == com.legend.compiler.spec.typed.WindowFrame.Kind.ROWS
                        ? SqlExpr.WindowCall.Frame.Kind.ROWS
                        : SqlExpr.WindowCall.Frame.Kind.RANGE,
                sqlBound(f.from()), sqlBound(f.to()));
    }

    /** The bound's SIGNED numeric offset (Preceding negative,
     * Following positive, CurrentRow zero); null = unbounded. */
    static @com.legend.Nullable Double numericOf(
            com.legend.compiler.spec.typed.WindowFrame.Bound b) {
        return switch (b) {
            case com.legend.compiler.spec.typed.WindowFrame.Bound
                    .Preceding p -> -Math.abs(p.n().doubleValue());
            case com.legend.compiler.spec.typed.WindowFrame.Bound
                    .Following fo -> Math.abs(fo.n().doubleValue());
            case com.legend.compiler.spec.typed.WindowFrame.Bound
                    .CurrentRow ignored -> 0.0;
            default -> null;
        };
    }

    static SqlExpr.WindowCall.Frame.Bound sqlBound(
            com.legend.compiler.spec.typed.WindowFrame.Bound b) {
        return switch (b) {
            case com.legend.compiler.spec.typed.WindowFrame.Bound.UnboundedPreceding ignored ->
                    new SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding();
            case com.legend.compiler.spec.typed.WindowFrame.Bound.Preceding p ->
                    new SqlExpr.WindowCall.Frame.Bound.Preceding(p.n());
            case com.legend.compiler.spec.typed.WindowFrame.Bound.CurrentRow ignored ->
                    new SqlExpr.WindowCall.Frame.Bound.CurrentRow();
            case com.legend.compiler.spec.typed.WindowFrame.Bound.Following fo ->
                    new SqlExpr.WindowCall.Frame.Bound.Following(fo.n());
            case com.legend.compiler.spec.typed.WindowFrame.Bound.UnboundedFollowing ignored ->
                    new SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing();
            case com.legend.compiler.spec.typed.WindowFrame.Bound.IntervalPreceding ip ->
                    new SqlExpr.WindowCall.Frame.Bound.IntervalPreceding(ip.n(), ip.unit());
            case com.legend.compiler.spec.typed.WindowFrame.Bound.IntervalFollowing ifo ->
                    new SqlExpr.WindowCall.Frame.Bound.IntervalFollowing(ifo.n(), ifo.unit());
        };
    }
}
