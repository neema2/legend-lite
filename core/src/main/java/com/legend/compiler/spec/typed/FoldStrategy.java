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
     * @param transform  the checked element transform (element parameter bound to {@code T[1]})
     * @param reducer    the checked reduction body over two accumulator-typed values
     * @param accParam   the accumulator parameter name the reducer binds
     * @param freshParam the synthetic second reducer parameter name
     */
    record MapReduce(TypedSpec transform, TypedSpec reducer,
                     String accParam, String freshParam) implements FoldStrategy {
    }

    /** Not decomposable; the accumulator is built element-by-element. */
    record CollectionBuild() implements FoldStrategy {
    }
}
