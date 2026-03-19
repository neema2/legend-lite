package com.gs.legend.ast;

/**
 * Enumeration value reference.
 *
 * <p>
 * Represents {@code JoinKind.INNER} or {@code DurationUnit.DAYS} in Pure.
 *
 * @param fullPath The fully qualified enum type path (e.g.,
 *                 "meta::relational::metamodel::join::JoinKind")
 * @param value    The enum value name (e.g., "INNER")
 */
public record EnumValue(
        String fullPath,
        String value) implements ValueSpecification {
}
