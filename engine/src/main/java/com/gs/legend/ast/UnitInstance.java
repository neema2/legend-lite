package com.gs.legend.ast;

import java.util.List;

/**
 * Unit instance value. Example: {@code 5 kilogram}.
 *
 * @param unitType  The unit type path (e.g., "mass~kilogram")
 * @param unitValue The numeric value as a list of ValueSpecification
 */
public record UnitInstance(
        String unitType,
        List<ValueSpecification> unitValue) implements ValueSpecification {

    public UnitInstance {
        unitValue = List.copyOf(unitValue);
    }
}
