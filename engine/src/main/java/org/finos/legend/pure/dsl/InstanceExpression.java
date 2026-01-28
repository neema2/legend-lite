package org.finos.legend.pure.dsl;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * AST for instance construction with ^ operator.
 * 
 * Pure syntax: ^Person(firstName='John', lastName='Doe', age=30)
 * 
 * @param className  The class being instantiated
 * @param properties Map of property name to value
 */
public record InstanceExpression(
        String className,
        Map<String, Object> properties) implements PureExpression {

    public InstanceExpression {
        Objects.requireNonNull(className, "Class name cannot be null");
        Objects.requireNonNull(properties, "Properties cannot be null");
    }

    /**
     * Creates an instance expression with ordered properties.
     */
    public static InstanceExpression of(String className, String... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException("Properties must be key-value pairs");
        }
        Map<String, Object> props = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            props.put(pairs[i], pairs[i + 1]);
        }
        return new InstanceExpression(className, props);
    }
}
