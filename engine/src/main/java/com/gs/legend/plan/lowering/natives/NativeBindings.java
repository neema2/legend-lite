package com.gs.legend.plan.lowering.natives;

import com.gs.legend.compiler.BuiltinRegistry;
import com.gs.legend.compiler.NativeFunctionDef;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.List;
import java.util.function.Predicate;

/**
 * Static population of the scalar {@link NativeBindingTable}. Phases populate
 * this incrementally:
 * <ul>
 *   <li>Phase 2 — list overloads (head/first/last/tail/init/at/slice).</li>
 *   <li>Phase 3.3 — arithmetic / comparison / logical.</li>
 *   <li>Phase 3.4 — date / time.</li>
 *   <li>Phase 3.5 — string specials.</li>
 *   <li>Phase 3.6 — list / array specials.</li>
 *   <li>Phase 3.7 — variant audit.</li>
 *   <li>Phase 3.8 — final cleanup; totality enabled at boot.</li>
 * </ul>
 *
 * <p>Phase 2 onward, {@link com.gs.legend.plan.lowering.scalar.NativeCallLowering}
 * delegates to the binding when one exists and falls through to legacy logic
 * otherwise. Phase 3.8 deletes the fallthrough.
 */
public final class NativeBindings {
    private NativeBindings() {}

    /**
     * Process-wide scalar binding table. Bindings are registered once at
     * class init via {@link #populate(NativeBindingTable)}.
     */
    public static final NativeBindingTable TABLE = build();

    private static NativeBindingTable build() {
        NativeBindingTable t = new NativeBindingTable();
        populate(t);
        return t;
    }

    /**
     * Populates the given table with every scalar native binding registered
     * to date.
     */
    public static void populate(NativeBindingTable table) {
        BuiltinRegistry reg = BuiltinRegistry.instance();
        registerListAccess(table, reg);
    }

    // ---------------------------------------------------------------------
    // Phase 2 — list element / slice access
    // ---------------------------------------------------------------------

    /**
     * Register scalar list-access overloads. Pure indexes are 0-based; SQL
     * (DuckDB {@code LIST_EXTRACT}/{@code LIST_SLICE}) is 1-based with
     * inclusive {@code [from, to]} bounds. Bindings translate at lowering
     * time; the typed IR carries SQL-native semantics.
     *
     * <p>Excludes relation overloads of {@code first}/{@code last}
     * (window-aggregate path, owned by Phase 3.2) and the relation
     * overload of {@code slice} (owned by relation lowering).
     */
    private static void registerListAccess(NativeBindingTable table, BuiltinRegistry reg) {
        NativeFunctionDef head     = pick(reg, "head",  hasParam("set:T[*]"));
        NativeFunctionDef first1   = pick(reg, "first", arity(1));
        NativeFunctionDef first2   = pick(reg, "first", arity(2).and(hasParam("set:T[*]")));
        NativeFunctionDef last1    = pick(reg, "last",  arity(1));
        NativeFunctionDef tail     = pick(reg, "tail",  arity(1));
        NativeFunctionDef init     = pick(reg, "init",  arity(1));
        NativeFunctionDef at       = pick(reg, "at",    arity(2));
        NativeFunctionDef sliceArr = pick(reg, "slice", arity(3).and(hasParam("set:T[*]")));

        // head(set) / first(set) -> LIST_EXTRACT(set, 1)
        NativeBinding firstOrHead = (call, args, ctx) ->
                new SqlExpr.ListExtract(args.get(0), intLit(1));
        table.register(head, firstOrHead);
        table.register(first1, firstOrHead);

        // first(set, count) -> LIST_SLICE(set, 1, count)
        table.register(first2, (call, args, ctx) ->
                new SqlExpr.ListSlice(args.get(0), intLit(1), args.get(1)));

        // last(set) -> LIST_EXTRACT(set, LEN(set))
        table.register(last1, (call, args, ctx) ->
                new SqlExpr.ListExtract(args.get(0), new SqlExpr.ListLength(args.get(0))));

        // tail(set) -> LIST_SLICE(set, 2, LEN(set))
        // init(set) -> LIST_SLICE(set, 1, LEN(set) - 1)
        // Both legacy paths called liftToList() to wrap a scalar value into
        // a singleton list when static multiplicity wasn't [*]. Preserved
        // here via wrapIfScalar(); migrated to a typed WrapList in Phase 3.6.
        table.register(tail, (call, args, ctx) -> {
            SqlExpr lst = wrapIfScalar(call.args().get(0), args.get(0));
            return new SqlExpr.ListSlice(lst, intLit(2), new SqlExpr.ListLength(lst));
        });
        table.register(init, (call, args, ctx) -> {
            SqlExpr lst = wrapIfScalar(call.args().get(0), args.get(0));
            return new SqlExpr.ListSlice(lst, intLit(1),
                    new SqlExpr.BinaryArith(SqlExpr.ArithOp.MINUS,
                            new SqlExpr.ListLength(lst), intLit(1)));
        });

        // at(set, idx) -> LIST_EXTRACT(set, idx + 1)  (Pure 0-based -> SQL 1-based)
        table.register(at, (call, args, ctx) ->
                new SqlExpr.ListExtract(args.get(0),
                        new SqlExpr.BinaryArith(SqlExpr.ArithOp.PLUS, args.get(1), intLit(1))));

        // slice(set, start, end) -> LIST_SLICE(set, start + 1, end)
        // Pure: [start, end) exclusive end, 0-based.
        // DuckDB: [from, to] inclusive, 1-based.
        // start: 0->1 shift via +1. end: Pure exclusive == DuckDB inclusive
        // after the start shift cancels out (verify on first PCT pass).
        table.register(sliceArr, (call, args, ctx) ->
                new SqlExpr.ListSlice(args.get(0),
                        new SqlExpr.BinaryArith(SqlExpr.ArithOp.PLUS, args.get(1), intLit(1)),
                        args.get(2)));
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    /**
     * Pick a single overload of {@code name} matching the predicate. Throws
     * when zero or multiple match — registration must be unambiguous.
     */
    private static NativeFunctionDef pick(BuiltinRegistry reg, String name,
                                          Predicate<NativeFunctionDef> match) {
        List<NativeFunctionDef> defs = reg.resolve(name);
        List<NativeFunctionDef> hits = defs.stream().filter(match).toList();
        if (hits.isEmpty()) {
            throw new IllegalStateException(
                    "[native-binding] no overload of '" + name + "' matched predicate; "
                    + "candidates: " + defs.stream().map(NativeFunctionDef::rawSignature).toList());
        }
        if (hits.size() > 1) {
            throw new IllegalStateException(
                    "[native-binding] ambiguous '" + name + "' match: "
                    + hits.stream().map(NativeFunctionDef::rawSignature).toList());
        }
        return hits.get(0);
    }

    private static Predicate<NativeFunctionDef> arity(int n) {
        return d -> d.arity() == n;
    }

    private static Predicate<NativeFunctionDef> hasParam(String paramFragment) {
        return d -> d.rawSignature().contains(paramFragment);
    }

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
