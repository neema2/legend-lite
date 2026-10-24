package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A type-checked expression &mdash; the Phase-G HIR (engine {@code TypedSpec}).
 * Every node carries its {@link ExprType} via {@link #info()} (the
 * schema-in-{@code info()} invariant, PHASE_G_SPEC_COMPILER.md §5), so downstream
 * lowering reads the resolved type off the node rather than re-deriving it.
 *
 * <p>This {@code permits} clause grows as Phase G implements more forms (literals
 * and variables first; calls, control flow, relation operators, and the rest
 * land in later stages). Adding a variant = a new record here.
 */
public sealed interface TypedSpec permits
        TypedRawSqlRelation,
        TypedCatalogGrid,
        TypedFrameRef,
        TypedDeactivate,
        TypedCsvCensus,
        TypedTestDataGen,
        TypedCInteger,
        TypedCString,
        TypedCBoolean,
        TypedCFloat,
        TypedCDecimal,
        TypedVariable,
        TypedPropertyAccess,
        TypedNativeCall,
        TypedUserCall,
        TypedLet,
        TypedCollection,
        TypedIf,
        TypedLambda,
        TypedMap,
        TypedColSpec,
        TypedColSpecArray,
        TypedSortInfo,
        TypedFuncColSpec,
        TypedFuncColSpecArray,
        TypedAggColSpec,
        TypedAggColSpecArray,
        TypedEnumValue,
        TypedCDate,
        TypedCTime,
        TypedCLatestDate,
        TypedTypeRef,
        TypedCast,
        TypedMatch,
        TypedMatchRuntime,
        TypedEval,
        TypedTds,
        TypedSourceUrl,
        TypedFlatten, TypedCollectionRelation,
        TypedGetAll, TypedMilestonedAccess,
        TypedFrom,
        TypedWrite, TypedStoreWrite,
        TypedFold,
        TypedNavigate,
        TypedGraphFetch,
        TypedNewInstanceCast,
        TypedSerialize,
        TypedSerializeGraph,
        TypedOver,
        TypedJoin,
        TypedJoinSlot,
        TypedViewRelation,
        TypedAsOfJoin, TypedJsonAccess, TypedJsonResult,
        TypedConcatenate,
        TypedPackageableRef,
        TypedTableReference,
        TypedNewInstance,
        TypedCopyInstance,
        TypedRelationOp
        {

    /** This node's resolved type and multiplicity. */
    ExprType info();

    /**
     * This node's structural children, in source order &mdash; the one traversal
     * spine every pass reuses (callee collection, lowering walks, printing).
     * Deliberately <em>not</em> defaulted: every node must declare its children,
     * so adding a node forces the traversal decision at definition time (the same
     * exhaustiveness guarantee a switch would give, kept with the structure).
     */
    List<TypedSpec> children();

    /**
     * Rebuild this node with {@code children} replacing {@link #children()},
     * consumed POSITIONALLY in children()'s exact order — the inverse
     * traversal. Mandatory like {@code children()}: adding a variant forces
     * both traversal decisions at definition time, and every generic
     * rewriter collapses to one delegating arm (remediation T2.1). A child
     * must keep its node kind (a lambda child stays a lambda) — a mismatch
     * throws {@link ClassCastException} LOUDLY, never rebuilds wrong.
     */
    TypedSpec withChildren(List<TypedSpec> children);

    /**
     * Rebuild this node with {@code info} replacing {@link #info()}, every
     * other field kept — the type-stamp inverse of {@code info()}. Mandatory
     * like {@link #withChildren}. Its one consumer is GENERIC INSTANTIATION
     * at the inlining seam ({@code UserCallInliner.instantiate}): a callee
     * typed over its own type parameters leaves its body stamped with them,
     * and the application resolves every such stamp under its bindings.
     */
    TypedSpec withInfo(ExprType info);

    /**
     * The one-step structural rewrite every walker shares: apply {@code f} to
     * each child and reassemble through {@link #withChildren} — the variant's
     * own inverse, so a hand rebuild can never drop a field. Identity is
     * preserved when no child changed.
     */
    default TypedSpec mapChildren(java.util.function.UnaryOperator<TypedSpec> f) {
        List<TypedSpec> kids = children();
        List<TypedSpec> rw = new java.util.ArrayList<>(kids.size());
        boolean same = true;
        for (TypedSpec k : kids) {
            TypedSpec r = f.apply(k);
            same &= r == k;
            rw.add(r);
        }
        return same ? this : withChildren(rw);
    }

    /** {@link #withChildren} arity guard — count drift throws, never skews. */
    static void expectChildren(List<TypedSpec> children, int n, String who) {
        if (children.size() != n) {
            throw new IllegalArgumentException(who + ".withChildren expects "
                    + n + " children, got " + children.size());
        }
    }
}
