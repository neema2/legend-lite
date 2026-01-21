package org.finos.legend.pure.dsl;

import java.util.Objects;

/**
 * AST for internalize() function to deserialize JSON to class.
 * 
 * Pure syntax: Person->internalize(jsonData) or Person->internalize(binding,
 * jsonData)
 * 
 * @param className   The target class
 * @param jsonData    The JSON data expression
 * @param bindingName Optional binding name (null if inferred)
 */
public record InternalizeExpression(
        String className,
        PureExpression jsonData,
        String bindingName) implements PureExpression {

    public InternalizeExpression {
        Objects.requireNonNull(className, "Class name cannot be null");
        Objects.requireNonNull(jsonData, "JSON data cannot be null");
    }

    /**
     * Creates an internalize expression without explicit binding.
     */
    public static InternalizeExpression of(String className, PureExpression jsonData) {
        return new InternalizeExpression(className, jsonData, null);
    }
}
