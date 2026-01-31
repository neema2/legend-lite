package org.finos.legend.pure.m3;

import org.finos.legend.pure.dsl.definition.EnumDefinition;

/**
 * Represents an Enum type in the Pure type system.
 * This wraps an EnumDefinition and implements the Type interface so enums
 * can be used as property types in classes.
 */
public record PureEnumType(EnumDefinition enumDefinition) implements Type {

    @Override
    public String typeName() {
        return enumDefinition.simpleName();
    }

    /**
     * @return The fully qualified name of the enum
     */
    public String qualifiedName() {
        return enumDefinition.qualifiedName();
    }

    /**
     * Checks if a value is valid for this enum.
     */
    public boolean hasValue(String valueName) {
        return enumDefinition.hasValue(valueName);
    }

    @Override
    public String toString() {
        return "Enum " + qualifiedName();
    }
}
