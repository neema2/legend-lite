package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.NativeFunctionDef;

/**
 * Marker for typed HIR records produced by an applied <em>Pure native function</em>.
 *
 * <p>Each implementor carries its resolved {@link NativeFunctionDef} — the exact
 * overload picked by {@code AbstractChecker.resolveOverload}. Identity-keyed
 * binding tables ({@code FilterBindings}, {@code SortBindings}, {@code
 * AggregateBindings}, {@code WindowBindings}, {@code ScalarBindings}) dispatch on
 * this constant instead of the function's surface name. Result: the lowerer
 * never re-derives shape from {@code value.type()}, never name-keys, never
 * splits records by materialization.
 *
 * <p>The sealed permits clause is the audit: every applied-native record must
 * implement {@code TypedNative} (so {@code def()} is exhaustively available),
 * and every non-native typed record must NOT (so we cannot accidentally claim
 * Mechanism-A dispatch for a structural node). Adding a new applied native
 * means adding it to the permits clause and stamping {@code def()} from the
 * checker — javac enforces both.
 *
 * <p>This interface addresses Mechanism A (native overload dispatch) only.
 * Mechanism B (type-parametric structural dispatch — {@code TypedLet} value
 * binding lane, {@code TypedBlock} tail/intermediate, {@code TypedUserCall}
 * actuals, {@code TypedVariable}) is independent and does not flow through
 * {@code def()}.
 */
public sealed interface TypedNative extends TypedSpec permits
        // Already correctly carries def (renamed from `func` for interface uniformity).
        // (TypedAggCall and TypedWindowExtendCol also carry NativeFunctionDef, but
        // they sit outside the TypedSpec hierarchy — embedded in TypedGroupBy.aggs
        // etc. — so they are not part of this sealed family.)
        TypedNativeCall,
        // Plumbed by Phase F.2
        TypedFilter, TypedSort, TypedSlice, TypedConcatenate, TypedDistinct,
        TypedFlatten, TypedRename, TypedSelect, TypedFold, TypedMap, TypedZip,
        TypedExtend, TypedProject, TypedJoin, TypedAsOfJoin, TypedGroupBy,
        TypedAggregate, TypedPivot, TypedFrom, TypedSerialize, TypedWrite,
        TypedGraphFetch, TypedEval {

    /** Resolved Pure native overload that produced this node. Never null. */
    NativeFunctionDef def();
}
