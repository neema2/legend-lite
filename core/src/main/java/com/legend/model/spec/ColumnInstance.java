package com.legend.model.spec;

/**
 * Marker for the tilde-column DSL ({@code ~col}) family of expression
 * values. Two structural variants:
 *
 * <ul>
 *   <li>{@link ColSpec} &mdash; a single column reference, optionally
 *       paired with a map lambda and/or an aggregate lambda.</li>
 *   <li>{@link ColSpecArray} &mdash; a bracketed list of {@link ColSpec}
 *       values, written {@code ~[a, b, c]}.</li>
 * </ul>
 *
 * <h2>Why this is not desugared to a function call</h2>
 *
 * <p>{@code let} and {@code ^} both desugar at parse time to
 * {@link AppliedFunction} calls because the corresponding
 * {@code letFunction} / {@code new} functions exist in Pure's stdlib
 * and the surface syntax is sugar for them. {@code ~col} has no
 * function counterpart: there is no stdlib {@code colSpec(name,
 * fn1, fn2)} function. The tilde syntax is a structural DSL
 * element, not sugar &mdash; matching engine-pure's M3 metamodel
 * (which carries {@code ColSpec} / {@code FuncColSpec} / {@code AggColSpec}
 * as first-class types) and engine-lite (which models {@code ColSpec}
 * directly as a {@code ValueSpecification} variant).
 *
 * <p>Legend-engine's wire protocol wraps these inside a
 * {@code ClassInstance(type='colSpec', value=...)} envelope for JSON
 * serialisation. That envelope is a serialisation concern, not an
 * AST shape concern; the parser builds the structured form
 * directly.
 *
 * <h2>Grouping rather than two top-level variants</h2>
 *
 * <p>Both {@link ColSpec} and {@link ColSpecArray} could implement
 * {@link ValueSpecification} directly without an intervening sealed
 * interface. The grouping under {@code ColumnInstance} mirrors
 * engine-lite's sealed hierarchy and keeps the relation-DSL nodes
 * together &mdash; helpful when later phases add {@code FuncColSpec},
 * {@code AggColSpec}, or other column-family variants without
 * touching every {@code ValueSpecification} consumer.
 */
public sealed interface ColumnInstance
        extends ValueSpecification
        permits ColSpec, ColSpecArray {
}
