package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

import java.util.Map;

/**
 * Struct / object literal: {@code ^Class(prop1=val1, prop2=val2, ...)} in Pure.
 *
 * <p>Replaces the legacy {@code instanceLiteral} boolean on {@code TypeInfo} with
 * a first-class variant carrying the class FQN and typed property values.
 *
 * @param className Fully qualified class name of the instance being constructed.
 * @param values    Property-name \u2192 typed value expressions. Preserves insertion
 *                  order (LinkedHashMap is the typical backing).
 */
public record TypedNewInstance(
        String className,
        Map<String, TypedSpec> values,
        ExpressionType info
) implements TypedSpec {
    public TypedNewInstance {
        values = Map.copyOf(values);
    }
}
