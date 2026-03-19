package com.gs.legend.ast;

/**
 * Column specification for the Relation API.
 *
 * <p>
 * Represents the tilde-prefixed column syntax in Pure:
 * <ul>
 * <li>{@code ~name} — simple column reference</li>
 * <li>{@code ~name:x|$x.firstName} — column with map function</li>
 * <li>{@code ~total:x|$x.amount:y|$y->plus()} — column with map + aggregate
 * functions</li>
 * </ul>
 *
 * <p>
 * Carried inside a {@link ClassInstance} with type "colSpec".
 *
 * @param name      The column name
 * @param function1 Optional first lambda (map function)
 * @param function2 Optional second lambda (aggregate function)
 */
public record ColSpec(
        String name,
        LambdaFunction function1,
        LambdaFunction function2) {

    /**
     * Simple column reference (no lambdas).
     */
    public ColSpec(String name) {
        this(name, null, null);
    }

    /**
     * Column with map function only.
     */
    public ColSpec(String name, LambdaFunction function1) {
        this(name, function1, null);
    }
}
