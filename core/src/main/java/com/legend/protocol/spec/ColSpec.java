package com.legend.protocol.spec;

import java.util.List;
import java.util.Objects;

/**
 * A single column specification &mdash; the building block of Pure's
 * {@code ~col} DSL. Three source forms:
 *
 * <ul>
 *   <li><strong>Bare reference</strong>: {@code ~name} &mdash;
 *       just the column name, both lambdas {@code null}. Used in
 *       {@code project} / {@code select} positions where the column
 *       value is passed through unchanged.</li>
 *   <li><strong>With map function</strong>: {@code ~name:x|$x.amount}
 *       &mdash; {@code function1} is the map lambda; {@code function2}
 *       is {@code null}. Used in {@code extend} / {@code rename}
 *       positions.</li>
 *   <li><strong>With map and aggregate</strong>:
 *       {@code ~total:x|$x.amount:y|$y->sum()} &mdash; both lambdas
 *       populated. {@code function1} is the per-row map; {@code function2}
 *       is the reduction over the grouped values. Used in {@code groupBy}
 *       positions.</li>
 * </ul>
 *
 * <p>The two lambdas carry different semantic roles depending on the
 * enclosing relation-API call (the type-checker dispatches on the
 * function name); the parser just preserves both slots. {@code null}
 * is the canonical absence marker.
 *
 * <h2>Why not a sealed split per slot count</h2>
 *
 * <p>Three records ({@code BareColSpec}, {@code MappedColSpec},
 * {@code AggColSpec}) would let the type system enforce
 * &ldquo;function2 is never set when function1 is null&rdquo;. We
 * deliberately do not split because:
 *
 * <ol>
 *   <li>Engine-lite and engine-pure both use one shape with two
 *       optional fields. Matching their AST keeps corpus-level
 *       comparisons direct.</li>
 *   <li>The slot-count invariant is enforced by the parser: the
 *       grammar only admits two ordered lambdas, never a {@code function2}
 *       without a {@code function1}.</li>
 *   <li>Downstream type-checkers dispatch on the <em>enclosing</em>
 *       function name (e.g. {@code project} vs {@code groupBy}) to
 *       interpret the slots; a structural split here doesn't change
 *       that dispatch.</li>
 * </ol>
 *
 * @param name      the column name as written in source
 * @param function1 map lambda, or {@code null} for a bare reference
 * @param function2 aggregate lambda, or {@code null} for non-aggregate
 *                  forms; {@code null} when {@code function1} is null
 */
public record ColSpec(
        String name,
        @com.legend.base.Nullable LambdaFunction function1,
        @com.legend.base.Nullable LambdaFunction function2,
        @com.legend.base.Nullable String alias,
        List<ValueSpecification> args,
        boolean qualified,
        @com.legend.base.Nullable com.legend.protocol.SourceInfo pos,
        @com.legend.base.Nullable com.legend.protocol.TypeExpression colType,
        @com.legend.base.Nullable com.legend.protocol.Multiplicity colTypeMult,
        List<com.legend.protocol.Protocol.PStereotype> stereotypes,
        List<com.legend.protocol.Protocol.PTaggedValue> taggedValues)
        implements ColumnInstance {

    /** Annotation-free compatibility constructor. */
    public ColSpec(String name, @com.legend.base.Nullable LambdaFunction function1,
            @com.legend.base.Nullable LambdaFunction function2, @com.legend.base.Nullable String alias,
            List<ValueSpecification> args, boolean qualified,
            @com.legend.base.Nullable com.legend.protocol.SourceInfo pos,
            @com.legend.base.Nullable com.legend.protocol.TypeExpression colType,
            @com.legend.base.Nullable com.legend.protocol.Multiplicity colTypeMult) {
        this(name, function1, function2, alias, args, qualified, pos, colType, colTypeMult,
                List.of(), List.of());
    }

    /** The lambda/bare form (no declared column type). */
    public ColSpec(String name, @com.legend.base.Nullable LambdaFunction function1,
            @com.legend.base.Nullable LambdaFunction function2, @com.legend.base.Nullable String alias,
            List<ValueSpecification> args, boolean qualified,
            @com.legend.base.Nullable com.legend.protocol.SourceInfo pos) {
        this(name, function1, function2, alias, args, qualified, pos, null, null);
    }

    /** Position-free form for synthesis and tests. */
    public ColSpec(String name, @com.legend.base.Nullable LambdaFunction function1,
            @com.legend.base.Nullable LambdaFunction function2, @com.legend.base.Nullable String alias,
            List<ValueSpecification> args, boolean qualified) {
        this(name, function1, function2, alias, args, qualified, null, null, null);
    }

    /** Position is excluded from equality. */
    @Override
    public boolean equals(Object o) {
        return o instanceof ColSpec other
                && name.equals(other.name())
                && java.util.Objects.equals(function1, other.function1())
                && java.util.Objects.equals(function2, other.function2())
                && java.util.Objects.equals(alias, other.alias())
                && args.equals(other.args())
                && qualified == other.qualified()
                && java.util.Objects.equals(colType, other.colType())
                && java.util.Objects.equals(colTypeMult, other.colTypeMult());
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, function1, function2, alias, args, qualified,
                colType, colTypeMult);
    }

    public ColSpec {
        Objects.requireNonNull(name, "name");
        args = args == null ? List.of() : List.copyOf(args);
        stereotypes = stereotypes == null ? List.of() : List.copyOf(stereotypes);
        taggedValues = taggedValues == null ? List.of() : List.copyOf(taggedValues);
    }

    /** Historical arity: parenthesized-ness follows the args (the graph
     * parser passes the flag explicitly — {@code synonyms()} is
     * qualified with zero args). */
    public ColSpec(String name, @com.legend.base.Nullable LambdaFunction function1,
            @com.legend.base.Nullable LambdaFunction function2, @com.legend.base.Nullable String alias,
            @com.legend.base.Nullable List<ValueSpecification> args) {
        this(name, function1, function2, alias,
                args == null ? List.of() : args,
                args != null && !args.isEmpty());
    }

    /** Graph-path arity without call args. */
    public ColSpec(String name, @com.legend.base.Nullable LambdaFunction function1,
            @com.legend.base.Nullable LambdaFunction function2, @com.legend.base.Nullable String alias) {
        this(name, function1, function2, alias, List.of());
    }

    /** Un-aliased canonical arity (every non-graph colspec). */
    public ColSpec(String name, @com.legend.base.Nullable LambdaFunction function1,
            @com.legend.base.Nullable LambdaFunction function2) {
        this(name, function1, function2, null, List.of());
    }

    /** Bare-reference convenience: {@code ~name}. */
    public ColSpec(String name) {
        this(name, null, null, null);
    }

    /** Mapped-only convenience: {@code ~name:fn}. */
    public ColSpec(String name, @com.legend.base.Nullable LambdaFunction function1) {
        this(name, function1, null, null);
    }
}
