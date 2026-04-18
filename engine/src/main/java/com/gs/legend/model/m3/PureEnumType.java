package com.gs.legend.model.m3;

import com.gs.legend.model.def.EnumDefinition;

/**
 * Represents an Enum type in the Pure type system.
 * This wraps an EnumDefinition and implements the {@link TypeDecl} interface so enums
 * can be used as property types in classes.
 */
public record PureEnumType(EnumDefinition enumDefinition) implements TypeDecl {

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
