package com.gs.legend.plan.lowering.natives;

import com.gs.legend.compiler.NativeFunctionDef;
import com.gs.legend.compiler.Pure;
import com.gs.legend.compiler.typed.TypedCBoolean;
import com.gs.legend.compiler.typed.TypedNativeCall;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.sql.SqlAggregate;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Aggregate dispatch table keyed by typed {@link NativeFunctionDef} constants
 * from {@link Pure}.
 *
 * <p>Each binding receives the typed argument list and a {@link LoweringContext}
 * already bound to the aggregate's row scope. The binding lowers what it needs
 * via {@link Lowerer#lowerScalar(TypedSpec, LoweringContext)}. This shape lets
 * bindings whose overload signature describes a structured argument (e.g. the
 * {@code RowMapper<T,U>} overloads of corr/covar/maxBy/minBy/wavg) destructure
 * the typed tree directly — no checker-side unpacking required, and no
 * synthetic transport SqlExprs.
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
     * One binding per resolved overload. Receives the typed argument list and
     * a {@link LoweringContext} already bound to the aggregate's row scope;
     * the binding lowers each typed arg it needs via
     * {@link Lowerer#lowerScalar(TypedSpec, LoweringContext)}. Returns a typed
     * {@link SqlAggregate} variant — everything the renderer needs is carried
     * inside the variant.
     *
     * <p>The same binding is invoked from agg context
     * ({@code GroupByAggregateLowering}) and from window context
     * ({@code ExtendLowering.lowerWindowCol} via the
     * {@code AggregateBindings.snapshot()} bridge); both paths hand typed
     * args + a properly-bound context through.
     */
    @FunctionalInterface
    public interface Binding {
        SqlAggregate.Reducer build(List<TypedSpec> args, LoweringContext ctx);
    }

    private static final Map<NativeFunctionDef, Binding> TABLE = new HashMap<>();

    static {
        // ----- count / size -----
        bindAll(unary(SqlAggregate.Count::new), Pure.COUNT__T_MANY, Pure.SIZE__T_MANY);
        // NOTE on multi-operand bindings: they call {@code lower(args, ctx, i)}
        // for each typed arg they consume — keeping lowering inside the binding
        // so each variant decides what to lower (and what to inspect as typed
        // metadata, e.g. percentile's literalBool flags) instead of receiving
        // pre-lowered SqlExprs.
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
        // maxBy/minBy(values, keys) — separate bindings per overload shape:
        // the 2-arg numeric form receives [value, key] as positional typed
        // args; the rowMapper form receives a single typed rowMapper call
        // and destructures it locally. The function-keyed overloads (with a
        // closure as the second arg) aren't yet supported — they'd require
        // lowering a Pure lambda into SQL ARG_MAX(value, lambda(value)),
        // which DuckDB doesn't accept.
        bindAll(binary(SqlAggregate.MaxBy::new),
                Pure.MAX_BY__T_MANY__T_MANY, Pure.MAX_BY__T_MANY__T_MANY__INTEGER_1);
        bindAll(binary(SqlAggregate.MinBy::new),
                Pure.MIN_BY__T_MANY__T_MANY, Pure.MIN_BY__T_MANY__T_MANY__INTEGER_1);
        bind(Pure.MAX_BY__ROW_MAPPER_MANY, rowMapperBinary(SqlAggregate.MaxBy::new));
        bind(Pure.MIN_BY__ROW_MAPPER_MANY, rowMapperBinary(SqlAggregate.MinBy::new));

        // ----- means -----
        bindAll(unary(SqlAggregate.Avg::new),
                Pure.AVG__NUMBER_MANY, Pure.AVERAGE__NUMBER_MANY, Pure.MEAN__NUMBER_MANY);
        // wavg(rowMapper(value, weight)) → SqlAggregate.WeightedAvg(value, weight).
        // The binding's dispatch key (WAVG__ROW_MAPPER_MANY) guarantees its
        // single typed arg is a RowMapper-shaped call; destructure locally.
        bind(Pure.WAVG__ROW_MAPPER_MANY, rowMapperBinary(SqlAggregate.WeightedAvg::new));
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
        bindAll(binary(SqlAggregate.PercentileCont::new),
                Pure.PERCENTILE__NUMBER_MANY__NUMBER_1,
                Pure.PERCENTILE_CONT__NUMBER_MANY__NUMBER_1);
        bind(Pure.PERCENTILE__NUMBER_MANY__NUMBER_1__BOOLEAN_1__BOOLEAN_1, (args, ctx) -> {
            boolean ascending  = literalBool(args.get(2));
            boolean continuous = literalBool(args.get(3));
            SqlExpr values = lower(args, ctx, 0);
            SqlExpr pRaw = lower(args, ctx, 1);
            SqlExpr p = ascending
                    ? pRaw
                    : new SqlExpr.BinaryArith(SqlExpr.ArithOp.MINUS,
                            new SqlExpr.NumericLiteral(1), pRaw);
            return continuous
                    ? new SqlAggregate.PercentileCont(values, p)
                    : new SqlAggregate.PercentileDisc(values, p);
        });
        bind(Pure.PERCENTILE_DISC__NUMBER_MANY__NUMBER_1, binary(SqlAggregate.PercentileDisc::new));

        // ----- correlation / covariance (binary) -----
        // Two overload shapes per aggregate, two distinct bindings:
        //   • Numeric  : args = [x, y] positional typed specs.
        //   • RowMapper: args = [rowMapper(x, y)] — single typed call whose
        //     own args carry x and y. The binding destructures locally;
        //     the dispatch key encodes the shape, so no per-arg identity
        //     check is needed inside the binding body.
        bind(Pure.CORR__NUMBER_MANY__NUMBER_MANY,                binary(SqlAggregate.Corr::new));
        bind(Pure.CORR__ROW_MAPPER_MANY,                         rowMapperBinary(SqlAggregate.Corr::new));
        bind(Pure.COVAR_POPULATION__NUMBER_MANY__NUMBER_MANY,    binary(SqlAggregate.CovarPopulation::new));
        bind(Pure.COVAR_POPULATION__ROW_MAPPER_MANY,             rowMapperBinary(SqlAggregate.CovarPopulation::new));
        bind(Pure.COVAR_SAMPLE__NUMBER_MANY__NUMBER_MANY,        binary(SqlAggregate.CovarSample::new));
        bind(Pure.COVAR_SAMPLE__ROW_MAPPER_MANY,                 rowMapperBinary(SqlAggregate.CovarSample::new));

        // ----- joinStrings (separator REQUIRED in the typed variant) -----
        // joinStrings(values, sep) → STRING_AGG(values, sep). The 4-arg form
        // (values, sep, prefix, suffix) is lowered as joinStrings(values, sep)
        // for now — prefix/suffix aren't currently honoured.
        bindAll(binary(SqlAggregate.JoinStrings::new),
                Pure.JOIN_STRINGS__STRING_MANY__STRING_1,
                Pure.JOIN_STRINGS__STRING_MANY__STRING_1__STRING_1__STRING_1);

        // ----- misc -----
        bind(Pure.HASH_CODE__ANY_MANY,          unary(SqlAggregate.HashCode::new));
    }

    /** Helper: a binding that builds a unary {@link SqlAggregate} from {@code args.get(0)}. */
    private static Binding unary(java.util.function.Function<SqlExpr, SqlAggregate.Reducer> ctor) {
        return (args, ctx) -> ctor.apply(lower(args, ctx, 0));
    }

    /** Helper: a binding that builds a binary {@link SqlAggregate} from {@code args[0], args[1]}. */
    private static Binding binary(java.util.function.BiFunction<SqlExpr, SqlExpr, SqlAggregate.Reducer> ctor) {
        return (args, ctx) -> ctor.apply(lower(args, ctx, 0), lower(args, ctx, 1));
    }

    /**
     * Helper for the rowMapper-overload of a bivariate aggregate. The single
     * typed arg is structurally a {@link TypedNativeCall} whose own args are
     * the value and key — guaranteed by the overload's signature
     * ({@code RowMapper<T,U>}). Destructure locally, lower each side in the
     * caller-provided context, and feed the binary {@link SqlAggregate}
     * constructor. No reference to {@link Pure#ROW_MAPPER__T_0_1__U_0_1}
     * needed — the dispatch key alone encodes the structure.
     */
    private static Binding rowMapperBinary(
            java.util.function.BiFunction<SqlExpr, SqlExpr, SqlAggregate.Reducer> ctor) {
        return (args, ctx) -> {
            TypedSpec sole = args.get(0);
            if (!(sole instanceof TypedNativeCall pair) || pair.args().size() != 2) {
                throw new IllegalStateException(
                        "[agg-binding] rowMapper-overload expected a 2-arg rowMapper call, got "
                                + sole);
            }
            return ctor.apply(
                    Lowerer.lowerScalar(pair.args().get(0), ctx),
                    Lowerer.lowerScalar(pair.args().get(1), ctx));
        };
    }

    /** Lower one positional typed arg in the binding's row-bound context. */
    private static SqlExpr lower(List<TypedSpec> args, LoweringContext ctx, int i) {
        return Lowerer.lowerScalar(args.get(i), ctx);
    }

    /**
     * Read a compile-time boolean literal directly off the typed AST. Used by
     * aggregates like {@code percentile(values, p, ascending, continuous)}
     * where the trailing flags must be statically known to decide which
     * {@link SqlAggregate} variant (or ORDER BY direction) to emit. Throws
     * when the operand is not a {@link TypedCBoolean} — dynamic flags are
     * not currently supported.
     */
    private static boolean literalBool(TypedSpec e) {
        if (e instanceof TypedCBoolean bl) return bl.value();
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
