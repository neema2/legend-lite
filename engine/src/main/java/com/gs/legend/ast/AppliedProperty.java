package com.gs.legend.ast;

import java.util.List;

/**
 * Property access expression.
 *
 * <p>
 * Represents {@code $x.name} in Pure, where {@code x} is the receiver
 * and {@code name} is the property being accessed.
 *
 * <p>
 * In the legend-engine protocol, parameter[0] is always the receiver.
 *
 * @param property   The property name (e.g., "name", "age", "firstName")
 * @param parameters The receiver as a single-element list: [Variable("x")]
 */
public record AppliedProperty(
        String property,
        List<ValueSpecification> parameters) implements ValueSpecification {

    public AppliedProperty {
        parameters = List.copyOf(parameters);
    }
}
