package com.legend.compiler.spec.typed;

/**
 * The lowering strategy of a {@code fold} (engine {@code FoldStrategy}) &mdash;
 * classified at type-check time, in order:
 *
 * <ol>
 *   <li>{@link Concatenation} &mdash; the identity-add pattern
 *       {@code {e, a | $a->add($e)}}: the fold IS the source collection.</li>
 *   <li>{@link SameType} &mdash; accumulator and element types agree (scalar
 *       init): a plain running reduction.</li>
 *   <li>{@link MapReduce} &mdash; the body decomposes into an element-only
 *       transform + an associative reducer over the accumulator type
 *       ({@code plus(acc, f(e))} &rarr; map {@code f}, reduce {@code plus}).</li>
 *   <li>{@link CollectionBuild} &mdash; none of the above; the accumulator is
 *       built element-by-element.</li>
 * </ol>
 */
public sealed interface FoldStrategy {

    /** {@code {e, a | $a->add($e)}} &mdash; the fold concatenates the source. */
    record Concatenation() implements FoldStrategy {
    }

    /** Element and accumulator types agree; a plain running reduction. */
    record SameType() implements FoldStrategy {
    }

    /**
     * The body decomposed into an element-only transform plus a reducer.
     *
     * <p>BOTH trees are CLOSED {@link TypedLambda}s — each binds its own
     * parameters. The first draft stored bare expressions whose free
     * variables were bound by the SIBLING reducer lambda's binders;
     * cross-tree binding is invisible to every compositional rewriter,
     * so the inliner's α-renaming left the strategy referencing dead
     * names (testPlusInIterate — the let-inlined fold's transform still
     * asked for {@code $p} after the reducer renamed it — {@code _i0} then, {@code p_1} under TypedSubst).
     * As lambdas they re-enter the walker's own TypedLambda arm and
     * α-hygiene stays uniform (UserCallInliner's documented contract);
     * the old {@code accParam}/{@code freshParam} name strings — which
     * no tree walk could ever rename — are derived from the lambdas'
     * OWN parameters at the one consumer instead.
     *
     * @param transform one-parameter lambda: element {@code T[1]} &rarr; accumulator type
     * @param reducer   two-parameter lambda in the fold convention
     *                  (transformed element first, accumulator second)
     */
    record MapReduce(TypedLambda transform, TypedLambda reducer) implements FoldStrategy {
    }

    /** Not decomposable; the accumulator is built element-by-element. */
    record CollectionBuild() implements FoldStrategy {
    }
}
