package com.gs.legend.plan.lowering.natives;

import com.gs.legend.compiler.NativeFunctionDef;
import com.gs.legend.compiler.Pure;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.sql.SqlAggregate;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Window-context dispatch table keyed by typed {@link NativeFunctionDef}
 * constants from {@link Pure}. Parallel-shaped to {@link AggregateBindings}
 * (same {@code Binding} interface contract, same {@code bind/bindAll/lookup}
 * API) but returns the broader {@link SqlAggregate} — window context
 * accepts reducers, ranking functions, and value functions.
 *
 * <p>Two registration sources:
 * <ol>
 *   <li><b>Window-only natives</b> (ranking + value functions) are bound
 *       explicitly here. {@code rowNumber}, {@code rank}, {@code lag},
 *       {@code first} (window overload), etc. The window overloads of
 *       {@code first}/{@code last}/{@code nth} are referenced by their
 *       specific {@link Pure} constants (those that take
 *       {@code Relation<T>, _Window<T>, T}); the scalar list overloads of
 *       the same Pure names are bound separately in {@link NativeBindings}.</li>
 *   <li><b>Reducers in window context</b> are bound directly to their
 *       {@link SqlAggregate.Reducer} variant — same record types used in
 *       agg context. There is no lift wrapper; the renderer's single
 *       {@code render(SqlAggregate)} switch handles both positions.
 *       Additionally, every {@link AggregateBindings} entry is auto-promoted
 *       so scalar agg overloads ({@code sum} on {@code T[*]}, etc.) work
 *       when used inside an {@code over()} clause.</li>
 * </ol>
 *
 * <p>See AGENTS.md invariant 2: no string switch in lowering. The Pure name
 * appears at most once per overload, in {@link Pure}; everywhere else the
 * dispatch key is the {@link NativeFunctionDef} the frontend resolved to.
 */
public final class WindowBindings {
    private WindowBindings() {}

    /**
     * One binding per resolved overload. Receives the typed argument list and
     * a {@link LoweringContext} already bound to the window's row scope; the
     * binding lowers each typed arg via {@link Lowerer#lowerScalar} and emits
     * a typed {@link SqlAggregate} variant. Same shape as
     * {@link AggregateBindings.Binding} so scalar agg overloads can promote
     * directly into window context without a wrapping adapter.
     */
    @FunctionalInterface
    public interface Binding {
        SqlAggregate build(List<TypedSpec> args, LoweringContext ctx);
    }

    private static final Map<NativeFunctionDef, Binding> TABLE = new HashMap<>();

    static {
        // --- Ranking (zero-arg windows) -----------------------------------
        bind(Pure.ROW_NUMBER__RELATION_1__T_1,                         (args, ctx) -> new SqlAggregate.RowNumber());
        bind(Pure.RANK__RELATION_1__WINDOW_1__T_1,                     (args, ctx) -> new SqlAggregate.Rank());
        bind(Pure.DENSE_RANK__RELATION_1__WINDOW_1__T_1,               (args, ctx) -> new SqlAggregate.DenseRank());
        bind(Pure.PERCENT_RANK__RELATION_1__WINDOW_1__T_1,             (args, ctx) -> new SqlAggregate.PercentRank());
        bind(Pure.CUMULATIVE_DISTRIBUTION__RELATION_1__WINDOW_1__T_1,  (args, ctx) -> new SqlAggregate.CumulativeDistribution());

        // --- Value functions ----------------------------------------------
        // first/last/nth also have scalar-list overloads bound by NativeBindings
        // (LIST_EXTRACT). The Pure constants here are specifically the
        // window-context overloads (Relation<T>, _Window<T>, T) — distinct
        // typed identifiers. No predicate filtering needed.
        bind(Pure.FIRST__RELATION_1__WINDOW_1__T_1,            (args, ctx) -> new SqlAggregate.FirstValue(lower(args, ctx, 0)));
        bind(Pure.LAST__RELATION_1__WINDOW_1__T_1,             (args, ctx) -> new SqlAggregate.LastValue(lower(args, ctx, 0)));
        bind(Pure.NTH__RELATION_1__WINDOW_1__T_1__INTEGER_1,   (args, ctx) -> new SqlAggregate.NthValue(lower(args, ctx, 0), lower(args, ctx, 1)));
        bind(Pure.NTILE__RELATION_1__T_1__INTEGER_1,           (args, ctx) -> new SqlAggregate.Ntile(lower(args, ctx, 0)));

        // Lag/Lead — both arity-2 and arity-3 (with offset) overloads.
        bindAll((args, ctx) -> new SqlAggregate.Lag(lowerAll(args, ctx)),
                Pure.LAG__RELATION_1__T_1, Pure.LAG__RELATION_1__T_1__INTEGER_1);
        bindAll((args, ctx) -> new SqlAggregate.Lead(lowerAll(args, ctx)),
                Pure.LEAD__RELATION_1__T_1, Pure.LEAD__RELATION_1__T_1__INTEGER_1);

        // --- Reducers (window-context overloads) --------------------------
        // The frontend resolves a window-context call like {p,w,r|$p->sum($r)}
        // to the WINDOW overload (e.g. Pure.SUM__RELATION_1__WINDOW_1__T_1),
        // distinct from the scalar agg overload (Pure.SUM__NUMBER_MANY).
        // Bind each window overload directly to the Reducer variant — no
        // lift wrapper needed; the renderer treats reducers identically in
        // agg and window position.
        bind(Pure.SUM__RELATION_1__WINDOW_1__T_1,                  reducer(SqlAggregate.Sum::new));
        // count() with no value operand (funcArgs filtered to empty by the
        // checker for {p,w,r|$p->count($w,$r)}) lowers to CountStar; with one
        // operand it lowers to Count(arg). Two distinct typed variants — no
        // null sneaks into the renderer.
        bind(Pure.COUNT__RELATION_1__WINDOW_1__T_1, (args, ctx) -> args.isEmpty()
                ? new SqlAggregate.CountStar()
                : new SqlAggregate.Count(lower(args, ctx, 0)));
        bindAll(reducer(SqlAggregate.Avg::new),
                Pure.AVG__RELATION_1__WINDOW_1__T_1,
                Pure.AVERAGE__RELATION_1__WINDOW_1__T_1,
                Pure.MEAN__RELATION_1__WINDOW_1__T_1);
        bind(Pure.MIN__RELATION_1__WINDOW_1__T_1,                  reducer(SqlAggregate.Min::new));
        bind(Pure.MAX__RELATION_1__WINDOW_1__T_1,                  reducer(SqlAggregate.Max::new));
        bind(Pure.MEDIAN__RELATION_1__WINDOW_1__T_1,               reducer(SqlAggregate.Median::new));
        bind(Pure.STD_DEV__RELATION_1__WINDOW_1__T_1,              reducer(SqlAggregate.StdDev::new));
        bind(Pure.STD_DEV_SAMPLE__RELATION_1__WINDOW_1__T_1,       reducer(SqlAggregate.StdDevSample::new));
        bind(Pure.STD_DEV_POPULATION__RELATION_1__WINDOW_1__T_1,   reducer(SqlAggregate.StdDevPopulation::new));
        bind(Pure.VARIANCE__RELATION_1__WINDOW_1__T_1,             reducer(SqlAggregate.Variance::new));
        bind(Pure.VARIANCE_SAMPLE__RELATION_1__WINDOW_1__T_1,      reducer(SqlAggregate.VarianceSample::new));
        bind(Pure.VARIANCE_POPULATION__RELATION_1__WINDOW_1__T_1,  reducer(SqlAggregate.VariancePopulation::new));
        // Window-context multi-operand aggregates (percentile p, corr y,
        // covar y) — the Pure single-T window signatures don't carry the
        // second operand, so we cannot correctly synthesize the typed
        // variant here. These overloads throw at lowering; the proper fix
        // is to flatten extra reducer args in the frontend (see
        // TypedWindowExtendCol). Failing loudly is better than silently
        // emitting wrong SQL.
        bind(Pure.PERCENTILE__RELATION_1__WINDOW_1__T_1,
                (args, ctx) -> { throw new UnsupportedOperationException(
                        "window percentile requires p operand; not yet wired through frontend"); });
        bind(Pure.CORR__RELATION_1__WINDOW_1__T_1,
                (args, ctx) -> { throw new UnsupportedOperationException(
                        "window corr requires second column; not yet wired through frontend"); });
        bind(Pure.COVAR_POPULATION__RELATION_1__WINDOW_1__T_1,
                (args, ctx) -> { throw new UnsupportedOperationException(
                        "window covarPopulation requires second column; not yet wired through frontend"); });
        bind(Pure.COVAR_SAMPLE__RELATION_1__WINDOW_1__T_1,
                (args, ctx) -> { throw new UnsupportedOperationException(
                        "window covarSample requires second column; not yet wired through frontend"); });

        // --- Scalar agg overloads usable in window position ---------------
        // Some Pure expressions resolve to the scalar agg overload even when
        // used in window position (e.g., {p,w,r|$r->plus()} where plus has
        // only a scalar T[*] overload, not a window-specific one). Promote
        // each AggregateBindings entry to a window binding directly — the
        // signatures are identical (typed args + ctx) so Reducer widens to
        // SqlAggregate without a wrapping adapter. Window-specific overloads
        // bound above win via the containsKey guard.
        for (Map.Entry<NativeFunctionDef, AggregateBindings.Binding> e : AggregateBindings.snapshot().entrySet()) {
            NativeFunctionDef def = e.getKey();
            AggregateBindings.Binding aggBinding = e.getValue();
            if (TABLE.containsKey(def)) continue;
            TABLE.put(def, aggBinding::build);
        }
    }

    /** Lower one positional typed arg in the binding's row-bound context. */
    private static SqlExpr lower(List<TypedSpec> args, LoweringContext ctx, int i) {
        return Lowerer.lowerScalar(args.get(i), ctx);
    }

    /** Lower every typed arg in order; used by Lag/Lead which take a list. */
    private static List<SqlExpr> lowerAll(List<TypedSpec> args, LoweringContext ctx) {
        List<SqlExpr> out = new ArrayList<>(args.size());
        for (TypedSpec a : args) out.add(Lowerer.lowerScalar(a, ctx));
        return out;
    }

    /**
     * Build a window-binding from a one-arg {@link SqlAggregate.Reducer}
     * constructor. Used for window-context overloads of sum/avg/min/max/
     * etc. — the typed argument is the row expression lowered in the
     * OVER frame's row context.
     */
    private static Binding reducer(java.util.function.Function<SqlExpr, SqlAggregate.Reducer> ctor) {
        // Tolerate empty args list — count() with no operand lowers to COUNT(*),
        // which the checker should have routed to the CountStar arm above.
        return (args, ctx) -> ctor.apply(args.isEmpty() ? null : lower(args, ctx, 0));
    }

    /** Bind one Pure-constant overload to a single window lowering. */
    private static void bind(NativeFunctionDef def, Binding binding) {
        if (TABLE.put(def, binding) != null) {
            throw new IllegalStateException(
                    "[window-binding] duplicate registration for " + def.rawSignature());
        }
    }

    /** Bind a list of Pure-constant overloads to the same window lowering. */
    private static void bindAll(Binding binding, NativeFunctionDef... defs) {
        for (NativeFunctionDef def : defs) {
            bind(def, binding);
        }
    }

    /**
     * Resolve a binding for a typed window call. Throws when the frontend
     * resolved to an overload no lowering covers — never falls back to a
     * stringly-typed catch-all.
     */
    public static Binding lookup(NativeFunctionDef def) {
        Binding b = TABLE.get(def);
        if (b == null) {
            throw new IllegalStateException(
                    "[window-binding] no window binding for " + def.rawSignature()
                            + "; extend WindowBindings or fix the checker");
        }
        return b;
    }
}
