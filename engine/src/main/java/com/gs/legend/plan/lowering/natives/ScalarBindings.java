package com.gs.legend.plan.lowering.natives;

import com.gs.legend.compiler.NativeFunctionDef;
import com.gs.legend.compiler.Pure;
import com.gs.legend.compiler.typed.TypedCDateTime;
import com.gs.legend.compiler.typed.TypedCStrictDate;
import com.gs.legend.compiler.typed.TypedCString;
import com.gs.legend.compiler.typed.TypedEnumValue;
import com.gs.legend.compiler.typed.TypedNativeCall;
import com.gs.legend.compiler.typed.TypedPackageableRef;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Scalar (non-aggregate, non-window) native dispatch table keyed by typed
 * {@link NativeFunctionDef} constants from {@link Pure}. Parallel-shaped to
 * {@link AggregateBindings} and {@link WindowBindings}: same {@code Binding}
 * functional interface, same {@code bind/bindAll} helpers, same static
 * initialisation block, same {@link Pure}-constant-keyed registration.
 *
 * <p>Returns {@link SqlExpr} instead of {@code SqlAggregate} because scalar
 * natives produce arbitrary expressions (list access, arithmetic, string
 * functions, date functions, …). The binding signature also takes the
 * {@link TypedNativeCall} and {@link LoweringContext} — bindings need typed
 * arg shape for quirks like {@code get} variant key dispatch and partial-
 * date comparisons; the agg/window bindings don't need this because their
 * caller pre-extracts the relevant operands.
 *
 * <p>Lookup returns an {@link Optional} during the migration period — many
 * scalar natives still flow through the legacy switch in
 * {@link com.gs.legend.plan.lowering.scalar.NativeCallLowering}. When the
 * legacy switch is fully drained (Phase D in {@code docs/pipeline-architecture.md}),
 * lookup will be tightened to throw on miss like the other tables.
 *
 * <p>See AGENTS.md invariant 2: no string switch in lowering. The Pure name
 * appears at most once per overload, in {@link Pure}; everywhere else the
 * dispatch key is the {@link NativeFunctionDef} the frontend resolved to.
 */
public final class ScalarBindings {
    private ScalarBindings() {}

    /**
     * One binding per resolved scalar overload. Receives the typed call
     * (for arg-shape inspection), the already-lowered argument list, and
     * the lowering context. Emits typed IR — {@link SqlExpr}.
     */
    @FunctionalInterface
    public interface Binding {
        SqlExpr build(TypedNativeCall call, List<SqlExpr> args, LoweringContext ctx);
    }

    private static final Map<NativeFunctionDef, Binding> TABLE = new HashMap<>();

    static {
        // ----- list element / slice access --------------------------------
        // Pure indexes are 0-based; SQL (DuckDB LIST_EXTRACT/LIST_SLICE) is
        // 1-based with inclusive [from, to] bounds. Bindings translate at
        // lowering time; the typed IR carries SQL-native semantics.

        // head(set) / first(set) -> LIST_EXTRACT(set, 1)
        Binding firstOrHead = (call, args, ctx) ->
                new SqlExpr.ListExtract(args.get(0), intLit(1));
        bindAll(firstOrHead, Pure.HEAD__T_MANY, Pure.FIRST__T_MANY);

        // first(set, count) -> LIST_SLICE(set, 1, count)
        bind(Pure.FIRST__T_MANY__INTEGER_1, (call, args, ctx) ->
                new SqlExpr.ListSlice(args.get(0), intLit(1), args.get(1)));

        // last(set) -> LIST_EXTRACT(set, LEN(set))
        bind(Pure.LAST__T_MANY, (call, args, ctx) ->
                new SqlExpr.ListExtract(args.get(0), new SqlExpr.ListLength(args.get(0))));

        // tail(set) -> LIST_SLICE(set, 2, LEN(set))
        // init(set) -> LIST_SLICE(set, 1, LEN(set) - 1)
        // Both legacy paths called liftToList() to wrap a scalar value into
        // a singleton list when static multiplicity wasn't [*]. Preserved
        // here via wrapIfScalar(); migrated to a typed WrapList in Phase 3.6.
        bind(Pure.TAIL__T_MANY, (call, args, ctx) -> {
            SqlExpr lst = wrapIfScalar(call.args().get(0), args.get(0));
            return new SqlExpr.ListSlice(lst, intLit(2), new SqlExpr.ListLength(lst));
        });
        bind(Pure.INIT__T_MANY, (call, args, ctx) -> {
            SqlExpr lst = wrapIfScalar(call.args().get(0), args.get(0));
            return new SqlExpr.ListSlice(lst, intLit(1),
                    new SqlExpr.BinaryArith(SqlExpr.ArithOp.MINUS,
                            new SqlExpr.ListLength(lst), intLit(1)));
        });

        // at(set, idx) -> LIST_EXTRACT(set, idx + 1)  (Pure 0-based -> SQL 1-based)
        bind(Pure.AT__T_MANY__INTEGER_1, (call, args, ctx) ->
                new SqlExpr.ListExtract(args.get(0),
                        new SqlExpr.BinaryArith(SqlExpr.ArithOp.PLUS, args.get(1), intLit(1))));

        // slice(set, start, end) -> LIST_SLICE(set, start + 1, end)
        // Pure: [start, end) exclusive end, 0-based.
        // DuckDB: [from, to] inclusive, 1-based.
        // start: 0->1 shift via +1. end: Pure exclusive == DuckDB inclusive
        // after the start shift cancels out (verify on first PCT pass).
        bind(Pure.SLICE__T_MANY__INTEGER_1__INTEGER_1, (call, args, ctx) ->
                new SqlExpr.ListSlice(args.get(0),
                        new SqlExpr.BinaryArith(SqlExpr.ArithOp.PLUS, args.get(1), intLit(1)),
                        args.get(2)));

        // ----- min/max over a list (greatest/least) ----------------------
        // Pure: greatest(values:Any[*]):Any[0..1] / least(values:Any[*]):Any[0..1].
        // The single overload accepts both a list ([*]) and a scalar promoted
        // to a singleton list. Inspect the typed input's multiplicity:
        //   • [*]  → list_max / list_min over the lowered list value.
        //   • else → identity passthrough (greatest/least of a single value
        //            is the value itself; matches legacy plangen at
        //            "case greatest/least → if (params.size() == 1) yield x").
        bind(Pure.GREATEST__ANY_MANY, (call, args, ctx) ->
                isManyArg(call, 0) ? new SqlExpr.Greatest(args.get(0)) : args.get(0));
        bind(Pure.LEAST__ANY_MANY, (call, args, ctx) ->
                isManyArg(call, 0) ? new SqlExpr.Least(args.get(0)) : args.get(0));

        // ----- indexOf overload dispatch ---------------------------------
        // Pure: indexOf(s:String[1], sub:String[1]):Integer[1] (and 3-arg
        //   form with fromIndex) — string lookup. DuckDB INSTR is 1-based;
        //   Pure is 0-based with -1 for "not found". INSTR returns 0 when
        //   not found, so subtracting 1 gives -1 ✓.
        // Pure: indexOf<T>(set:T[*], val:T[1]):Integer[1] — list lookup.
        //   DuckDB LIST_POSITION is 1-based; same -1 convention applies.
        bindAll((call, args, ctx) -> stringIndexOf(args),
                Pure.INDEX_OF__STRING_1__STRING_1);
        bind(Pure.INDEX_OF__STRING_1__STRING_1__INTEGER_1, (call, args, ctx) ->
                // ((fromIndex + INSTR(SUBSTRING(s, fromIndex+1), sub)) - 1)
                // delegate to the existing dialect "indexOfFrom" composite.
                new SqlExpr.FunctionCall("indexOfFrom",
                        List.of(args.get(0), args.get(1), args.get(2))));
        bind(Pure.INDEX_OF__T_MANY__T_1, (call, args, ctx) ->
                new SqlExpr.BinaryArith(SqlExpr.ArithOp.MINUS,
                        new SqlExpr.FunctionCall("LIST_POSITION",
                                List.of(args.get(0), args.get(1))),
                        intLit(1)));

        // ----- hash dispatch ---------------------------------------------
        // Pure: hash(s:String[1]):String[1] — single-arg returns DuckDB's
        //   default HASH() which is BIGINT (matches HashCode test
        //   expectations).
        // Pure: hash(s:String[1], algo:HashType[1]):String[1] — algorithm
        //   may arrive as TypedEnumValue (HashType.MD5), TypedCString
        //   ('MD5'), or TypedPackageableRef (.MD5 path). All three resolve
        //   to a per-algorithm function: MD5(), SHA1(), SHA256().
        bind(Pure.HASH__STRING_1, (call, args, ctx) ->
                new SqlExpr.FunctionCall("HASH", List.of(args.get(0))));
        bind(Pure.HASH__STRING_1__HASH_TYPE_1, (call, args, ctx) -> {
            String algo = readHashAlgo(call.args().get(1));
            return new SqlExpr.FunctionCall(algo, List.of(args.get(0)));
        });

        // ----- has<DatePart>(date) ---------------------------------------
        // Pure semantics: returns Boolean — "does the date instance carry
        // this part?". The answer depends on the **literal value's
        // precision**, not just the typed kind: e.g. {@code %2015-04-15T17}
        // is a CDateTime but has no minute component, so {@code hasMinute}
        // returns false. The dispatch mirrors legacy plangen exactly:
        //   • CDateTime literal → inspect the value string for the part.
        //   • CStrictDate literal → only year/month/day are present.
        //   • Column ref / non-literal → emit the part-extraction call
        //     (MONTH/DAY/HOUR/…) which DuckDB returns as Integer; tests
        //     asserting Number-castable truthiness rely on this.
        bind(Pure.HAS_MONTH__DATE_1, (call, args, ctx) -> {
            TypedSpec a = call.args().get(0);
            if (a instanceof TypedCStrictDate || a instanceof TypedCDateTime) {
                return new SqlExpr.BoolLiteral(true);
            }
            return new SqlExpr.FunctionCall("MONTH", List.of(args.get(0)));
        });
        bind(Pure.HAS_DAY__DATE_1, (call, args, ctx) -> {
            TypedSpec a = call.args().get(0);
            if (a instanceof TypedCDateTime) {
                return new SqlExpr.BoolLiteral(true);
            }
            if (a instanceof TypedCStrictDate sd) {
                return new SqlExpr.BoolLiteral(sd.value().matches("\\d{4}-\\d{2}-\\d{2}.*"));
            }
            return new SqlExpr.FunctionCall("DAY", List.of(args.get(0)));
        });
        bind(Pure.HAS_HOUR__DATE_1, (call, args, ctx) -> {
            TypedSpec a = call.args().get(0);
            if (a instanceof TypedCDateTime) return new SqlExpr.BoolLiteral(true);
            if (a instanceof TypedCStrictDate) return new SqlExpr.BoolLiteral(false);
            return new SqlExpr.FunctionCall("HOUR", List.of(args.get(0)));
        });
        bind(Pure.HAS_MINUTE__DATE_1, (call, args, ctx) -> {
            TypedSpec a = call.args().get(0);
            if (a instanceof TypedCDateTime dt) {
                // Dates like %2015-04-15T17 have hour but no minute.
                return new SqlExpr.BoolLiteral(dt.value().matches(".*T\\d{2}:\\d{2}.*"));
            }
            if (a instanceof TypedCStrictDate) return new SqlExpr.BoolLiteral(false);
            return new SqlExpr.FunctionCall("MINUTE", List.of(args.get(0)));
        });
        bind(Pure.HAS_SECOND__DATE_1, (call, args, ctx) -> {
            TypedSpec a = call.args().get(0);
            if (a instanceof TypedCDateTime dt) {
                return new SqlExpr.BoolLiteral(dt.value().matches(".*T\\d{2}:\\d{2}:\\d{2}.*"));
            }
            if (a instanceof TypedCStrictDate) return new SqlExpr.BoolLiteral(false);
            return new SqlExpr.FunctionCall("SECOND", List.of(args.get(0)));
        });
        bind(Pure.HAS_SUBSECOND__DATE_1, (call, args, ctx) -> {
            TypedSpec a = call.args().get(0);
            if (a instanceof TypedCDateTime dt) {
                return new SqlExpr.BoolLiteral(dt.value().matches(".*\\.\\d+.*"));
            }
            // CStrictDate / column refs: no subsecond component possible
            // for the literal cases; column refs always lack subsecond
            // precision in our DuckDB types.
            return new SqlExpr.BoolLiteral(false);
        });
    }

    /**
     * Lower the 2-arg string indexOf:
     *   {@code (INSTR(s, sub) - 1)} — DuckDB INSTR is 1-based (0 = not found),
     *   Pure is 0-based (-1 = not found).
     */
    private static SqlExpr stringIndexOf(List<SqlExpr> args) {
        return new SqlExpr.BinaryArith(SqlExpr.ArithOp.MINUS,
                new SqlExpr.FunctionCall("INSTR", List.of(args.get(0), args.get(1))),
                intLit(1));
    }

    /**
     * Read the hash algorithm name off a typed AST node. Accepts the three
     * shapes Pure produces for {@code HashType} arguments: enum value,
     * string literal, and packageable reference path.
     */
    private static String readHashAlgo(TypedSpec algoSpec) {
        if (algoSpec instanceof TypedEnumValue ev) {
            return ev.member().toUpperCase();
        }
        if (algoSpec instanceof TypedCString cs) {
            return cs.value().toUpperCase();
        }
        if (algoSpec instanceof TypedPackageableRef pr) {
            String fullPath = pr.fullPath();
            if (fullPath.contains("SHA256")) return "SHA256";
            if (fullPath.contains("SHA1"))   return "SHA1";
            if (fullPath.contains("MD5"))    return "MD5";
        }
        throw new IllegalStateException(
                "[scalar-binding] hash() algorithm must be a literal HashType; got " + algoSpec);
    }

    /** True when arg {@code i} of the typed call has [*] multiplicity. */
    private static boolean isManyArg(TypedNativeCall call, int i) {
        TypedSpec a = call.args().get(i);
        return a.info() != null && a.info().isMany();
    }

    /** Bind one Pure-constant overload to a single scalar lowering. */
    private static void bind(NativeFunctionDef def, Binding binding) {
        if (TABLE.put(def, binding) != null) {
            throw new IllegalStateException(
                    "[scalar-binding] duplicate registration for " + def.rawSignature());
        }
    }

    /** Bind a list of Pure-constant overloads to the same scalar lowering. */
    private static void bindAll(Binding binding, NativeFunctionDef... defs) {
        for (NativeFunctionDef def : defs) {
            bind(def, binding);
        }
    }

    /**
     * Resolve a binding for a typed scalar call. Returns {@link Optional#empty}
     * when no binding exists — callers (specifically
     * {@code NativeCallLowering}) fall through to the legacy emit path.
     * After Phase D this returns a throwing lookup like the other tables.
     */
    public static Optional<Binding> lookup(NativeFunctionDef def) {
        return Optional.ofNullable(TABLE.get(def));
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private static SqlExpr intLit(int v) {
        return new SqlExpr.NumericLiteral(v);
    }

    /**
     * Mirrors the legacy {@code liftToList} hack: when static multiplicity
     * is not {@code [*]}, wrap the lowered expression in a singleton list so
     * list-only operators ({@code LIST_SLICE}) work uniformly. Migrated to
     * a typed {@code WrapList} variant in Phase 3.6.
     */
    private static SqlExpr wrapIfScalar(com.gs.legend.compiler.typed.TypedSpec typed,
                                        SqlExpr lowered) {
        var info = typed.info();
        if (info != null && info.isMany()) {
            return lowered;
        }
        return new SqlExpr.FunctionCall("wrapList", List.of(lowered));
    }
}
