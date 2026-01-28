package org.finos.legend.pure.m3;

/**
 * Represents built-in primitive types in the Pure type system.
 * These are singleton instances representing scalar data types.
 */
public enum PrimitiveType implements Type {
    STRING("String"),
    INTEGER("Integer"),
    BOOLEAN("Boolean"),
    DATE("Date"), // Abstract supertype of StrictDate and DateTime
    STRICT_DATE("StrictDate"), // Date only: %YYYY-MM-DD
    DATE_TIME("DateTime"), // Date + time: %YYYY-MM-DD'T'HH:MM:SS
    FLOAT("Float"),
    DECIMAL("Decimal");

    private final String typeName;

    PrimitiveType(String typeName) {
        this.typeName = typeName;
    }

    @Override
    public String typeName() {
        return typeName;
    }

    /**
     * Resolves a primitive type by name.
     * 
     * @param name The type name to resolve
     * @return The corresponding PrimitiveType
     * @throws IllegalArgumentException if no matching type exists
     */
    public static PrimitiveType fromName(String name) {
        for (PrimitiveType type : values()) {
            if (type.typeName.equalsIgnoreCase(name)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown primitive type: " + name);
    }
}
