package com.gs.legend.plan.lowering.natives;

import com.gs.legend.compiler.NativeFunctionDef;
import com.gs.legend.compiler.Pure;
import com.gs.legend.plan.sql.SqlAggregate;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Aggregate dispatch table keyed by typed {@link NativeFunctionDef} constants
 * from {@link Pure}.
 *
 * <p>Each binding receives the FULL lowered argument list — multi-operand
 * aggregates (joinStrings/percentile/corr/covar) consume their second/third
 * args here. Single-arg aggregates use {@code args.get(0)} via the
 * {@link #unary(java.util.function.Function)} helper.
 *
 * <p>The frontend has resolved each {@code TypedAggCall.func()} to a specific
 * overload; lowering looks up the entry by its {@link Pure} constant and emits
 * a typed {@link SqlAggregate} variant. There is no string switch in the
 * lowering and no {@code BuiltinRegistry.resolve(name)} lookup. See AGENTS.md
 * invariant 2.
 *
 * <p><strong>Pure-name aliasing</strong>: several Pure native names share a
 * single {@link SqlAggregate} variant (e.g. {@code sum}/{@code plus} →
 * {@link SqlAggregate.Sum}; {@code avg}/{@code average}/{@code mean} →
 * {@link SqlAggregate.Avg}). Each overload of every alias is bound explicitly
 * — duplication is intentional and javac-checked.
 */
public final class AggregateBindings {
    private AggregateBindings() {}

    /**
     * One binding per resolved overload. Receives the already-lowered
     * argument list — for multi-operand aggregates (joinStrings, percentile,
     * corr, …) the second/third args are inside this list and the binding
     * picks them out by index. Returns a typed {@link SqlAggregate} variant;
     * everything the renderer needs is carried inside the variant.
     *
     * <p>The same binding is invoked from agg context
     * ({@code GroupByAggregateLowering}) and lifted into window context
     * ({@code WindowAggregateBindings.OfAggregate}); both paths hand the full
     * args list through.
     */
    @FunctionalInterface
    public interface Binding {
        SqlAggregate.Reducer build(List<SqlExpr> args);
    }

    private static final Map<NativeFunctionDef, Binding> TABLE = new HashMap<>();

    static {
        // ----- count / size -----
        bindAll(unary(SqlAggregate.Count::new), Pure.COUNT__T_MANY, Pure.SIZE__T_MANY);
        // NOTE: distinct(rel) and distinct(rel, ~cols) are RELATION-returning
        // functions in Pure (they produce a deduplicated relation), not
        // aggregates. They have no business in this table. The legacy
        // string-keyed switch had a `case "distinct"` arm that lowered them
        // to COUNT(DISTINCT arg) — that was a bug preserved by the previous
        // refactor. They should be lowered as SqlRelation.Distinct from the
        // relation-lowering path, never reaching this table. If a Pure
        // {@code distinct(rel)} ever resolves through TypedAggCall, lookup()
        // will throw — surfacing the misrouting instead of emitting wrong SQL.

        // ----- sums / products -----
        bind(Pure.SUM__NUMBER_MANY, unary(SqlAggregate.Sum::new));
        bindAll(unary(SqlAggregate.Sum::new),
                Pure.PLUS__T_MANY,
                Pure.PLUS__INTEGER_1__INTEGER_1, Pure.PLUS__FLOAT_1__FLOAT_1,
                Pure.PLUS__DECIMAL_1__DECIMAL_1, Pure.PLUS__NUMBER_1__NUMBER_1,
                Pure.PLUS__STRING_1__STRING_1);
        bindAll(unary(SqlAggregate.Product::new),
                Pure.TIMES__T_MANY,
                Pure.TIMES__INTEGER_1__INTEGER_1, Pure.TIMES__FLOAT_1__FLOAT_1,
                Pure.TIMES__DECIMAL_1__DECIMAL_1, Pure.TIMES__NUMBER_1__NUMBER_1);

        // ----- min / max / extrema-by -----
        bindAll(unary(SqlAggregate.Max::new),
                Pure.MAX__NUMBER_MANY,
                Pure.MAX__NUMBER_1__NUMBER_1, Pure.MAX__INTEGER_1__INTEGER_1, Pure.MAX__FLOAT_1__FLOAT_1,
                Pure.MAX__INTEGER_MANY, Pure.MAX__FLOAT_MANY,
                Pure.MAX__DATE_TIME_1__DATE_TIME_1, Pure.MAX__STRICT_DATE_1__STRICT_DATE_1, Pure.MAX__DATE_1__DATE_1,
                Pure.MAX__DATE_TIME_MANY, Pure.MAX__STRICT_DATE_MANY, Pure.MAX__DATE_MANY);
        bindAll(unary(SqlAggregate.Min::new),
                Pure.MIN__NUMBER_MANY,
                Pure.MIN__NUMBER_1__NUMBER_1, Pure.MIN__INTEGER_1__INTEGER_1, Pure.MIN__FLOAT_1__FLOAT_1,
                Pure.MIN__INTEGER_MANY, Pure.MIN__FLOAT_MANY,
                Pure.MIN__DATE_TIME_1__DATE_TIME_1, Pure.MIN__STRICT_DATE_1__STRICT_DATE_1, Pure.MIN__DATE_1__DATE_1,
                Pure.MIN__DATE_TIME_MANY, Pure.MIN__STRICT_DATE_MANY, Pure.MIN__DATE_MANY);
        bindAll(unary(SqlAggregate.MaxBy::new),
                Pure.MAX_BY__T_MANY__FUNCTION_1, Pure.MAX_BY__T_MANY__FUNCTION_1__INTEGER_1,
                Pure.MAX_BY__ROW_MAPPER_MANY,
                Pure.MAX_BY__T_MANY__T_MANY, Pure.MAX_BY__T_MANY__T_MANY__INTEGER_1);
        bindAll(unary(SqlAggregate.MinBy::new),
                Pure.MIN_BY__T_MANY__FUNCTION_1, Pure.MIN_BY__T_MANY__FUNCTION_1__INTEGER_1,
                Pure.MIN_BY__ROW_MAPPER_MANY,
                Pure.MIN_BY__T_MANY__T_MANY, Pure.MIN_BY__T_MANY__T_MANY__INTEGER_1);

        // ----- means -----
        bindAll(unary(SqlAggregate.Avg::new),
                Pure.AVG__NUMBER_MANY, Pure.AVERAGE__NUMBER_MANY, Pure.MEAN__NUMBER_MANY);
        bind(Pure.WAVG__ROW_MAPPER_MANY,        unary(SqlAggregate.WeightedAvg::new));
        bind(Pure.MEDIAN__NUMBER_MANY,          unary(SqlAggregate.Median::new));
        bind(Pure.MODE__ANY_MANY,               unary(SqlAggregate.Mode::new));

        // ----- spread (stdDev/variance) -----
        bind(Pure.STD_DEV__NUMBER_MANY,             unary(SqlAggregate.StdDev::new));
        bind(Pure.STD_DEV_POPULATION__NUMBER_MANY,  unary(SqlAggregate.StdDevPopulation::new));
        bind(Pure.STD_DEV_SAMPLE__NUMBER_MANY,      unary(SqlAggregate.StdDevSample::new));
        bindAll(unary(SqlAggregate.Variance::new),
                Pure.VARIANCE__NUMBER_MANY, Pure.VARIANCE__NUMBER_MANY__BOOLEAN_1);
        bind(Pure.VARIANCE_POPULATION__NUMBER_MANY, unary(SqlAggregate.VariancePopulation::new));
        bind(Pure.VARIANCE_SAMPLE__NUMBER_MANY,     unary(SqlAggregate.VarianceSample::new));

        // ----- quantiles (percentile/percentileCont/percentileDisc) -----
        // 2-arg forms default to ascending=true and continuous=true.
        // 4-arg percentile(values, p, ascending, continuous) honours both
        // flags. Pure's ascending=false semantics is "compute the (1-p)
        // quantile from the ascending sort" — implemented at binding time
        // by emitting (1 - p) as the SQL operand. continuous chooses
        // PercentileCont vs PercentileDisc. Both flags must be boolean
        // literals at compile time — read statically via literalBool().
        bindAll(args -> new SqlAggregate.PercentileCont(args.get(0), args.get(1)),
                Pure.PERCENTILE__NUMBER_MANY__NUMBER_1,
                Pure.PERCENTILE_CONT__NUMBER_MANY__NUMBER_1);
        bind(Pure.PERCENTILE__NUMBER_MANY__NUMBER_1__BOOLEAN_1__BOOLEAN_1, args -> {
            boolean ascending  = literalBool(args.get(2));
            boolean continuous = literalBool(args.get(3));
            SqlExpr p = ascending
                    ? args.get(1)
                    : new SqlExpr.BinaryArith(SqlExpr.ArithOp.MINUS,
                            new SqlExpr.NumericLiteral(1), args.get(1));
            return continuous
                    ? new SqlAggregate.PercentileCont(args.get(0), p)
                    : new SqlAggregate.PercentileDisc(args.get(0), p);
        });
        bind(Pure.PERCENTILE_DISC__NUMBER_MANY__NUMBER_1,
                args -> new SqlAggregate.PercentileDisc(args.get(0), args.get(1)));

        // ----- correlation / covariance (binary) -----
        // 2-arg numeric: corr(x, y). 1-arg rowMapper variants pack both columns
        // into a single struct expression — for now we fall back to (x, x) (the
        // legacy behavior; rowMapper unpacking is a separate concern and
        // currently passes a single struct expr through args.get(0)).
        bindAll(args -> new SqlAggregate.Corr(args.get(0), args.size() > 1 ? args.get(1) : args.get(0)),
                Pure.CORR__NUMBER_MANY__NUMBER_MANY, Pure.CORR__ROW_MAPPER_MANY);
        bindAll(args -> new SqlAggregate.CovarPopulation(args.get(0), args.size() > 1 ? args.get(1) : args.get(0)),
                Pure.COVAR_POPULATION__NUMBER_MANY__NUMBER_MANY, Pure.COVAR_POPULATION__ROW_MAPPER_MANY);
        bindAll(args -> new SqlAggregate.CovarSample(args.get(0), args.size() > 1 ? args.get(1) : args.get(0)),
                Pure.COVAR_SAMPLE__NUMBER_MANY__NUMBER_MANY, Pure.COVAR_SAMPLE__ROW_MAPPER_MANY);

        // ----- joinStrings (separator REQUIRED in the typed variant) -----
        // joinStrings(values, sep) → STRING_AGG(values, sep). The 4-arg form
        // (values, sep, prefix, suffix) is lowered as joinStrings(values, sep)
        // for now — prefix/suffix aren't currently honoured.
        bindAll(args -> new SqlAggregate.JoinStrings(args.get(0), args.get(1)),
                Pure.JOIN_STRINGS__STRING_MANY__STRING_1,
                Pure.JOIN_STRINGS__STRING_MANY__STRING_1__STRING_1__STRING_1);

        // ----- misc -----
        bind(Pure.HASH_CODE__ANY_MANY,          unary(SqlAggregate.HashCode::new));
    }

    /** Helper: a binding that builds a unary {@link SqlAggregate} from {@code args.get(0)}. */
    private static Binding unary(java.util.function.Function<SqlExpr, SqlAggregate.Reducer> ctor) {
        return args -> ctor.apply(args.get(0));
    }

    /**
     * Read a compile-time boolean literal from a lowered {@link SqlExpr}.
     * Used by aggregates like {@code percentile(values, p, ascending,
     * continuous)} where the trailing flags must be statically known to
     * decide which {@link SqlAggregate} variant (or which ORDER BY
     * direction) to emit. Throws when the operand is not a literal —
     * dynamic flags are not currently supported.
     */
    private static boolean literalBool(SqlExpr e) {
        if (e instanceof SqlExpr.BoolLiteral bl) return bl.value();
        throw new IllegalStateException(
                "[agg-binding] expected boolean literal flag, got " + e);
    }

    /** Bind one Pure-constant overload to a single agg lowering. */
    private static void bind(NativeFunctionDef def, Binding binding) {
        if (TABLE.put(def, binding) != null) {
            throw new IllegalStateException(
                    "[agg-binding] duplicate registration for " + def.rawSignature());
        }
    }

    /** Bind a list of Pure-constant overloads to the same agg lowering. */
    private static void bindAll(Binding binding, NativeFunctionDef... defs) {
        for (NativeFunctionDef def : defs) {
            bind(def, binding);
        }
    }

    /**
     * Resolve a binding for a typed aggregate call. Throws when the
     * frontend resolved to an overload no lowering covers — never falls
     * back to a stringly-typed catch-all.
     */
    public static Binding lookup(NativeFunctionDef def) {
        Binding b = TABLE.get(def);
        if (b == null) {
            throw new IllegalStateException(
                    "[agg-binding] no agg binding for " + def.rawSignature()
                            + "; extend AggregateBindings or fix the checker");
        }
        return b;
    }

    /** Test/diagnostic accessor; not used by lowering. */
    public static boolean has(NativeFunctionDef def) {
        return TABLE.containsKey(def);
    }

    /**
     * Read-only view of every registered (overload, binding) entry. Used by
     * {@link WindowAggregateBindings} to lift each agg-context binding into
     * its window-context counterpart ({@link SqlAggregate} wrapped in
     * {@code WindowAggregate.OfAggregate}). Package-private to keep the
     * agg-table the single source of truth.
     */
    static Map<NativeFunctionDef, Binding> snapshot() {
        return Map.copyOf(TABLE);
    }
}
