package org.finos.legend.pure.dsl;

/**
 * Represents an enum value reference in Pure, such as:
 * - meta::pure::functions::date::DurationUnit.DAYS
 * - JoinKind.LEFT
 * 
 * This is distinct from ClassReference which represents class type references.
 * Enum values compile to string literals in SQL (e.g., 'DAYS').
 */
public record EnumValueReference(
        String enumType, // Full qualified enum type, e.g., "meta::pure::functions::date::DurationUnit"
        String valueName // Enum value name, e.g., "DAYS"
) implements PureExpression {

    /**
     * @return The simple enum type name (without package)
     */
    public String simpleEnumType() {
        int lastSeparator = enumType.lastIndexOf("::");
        return lastSeparator >= 0 ? enumType.substring(lastSeparator + 2) : enumType;
    }
}
