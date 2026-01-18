package org.finos.legend.pure.dsl.definition;

import java.util.List;
import java.util.Objects;

/**
 * Represents a Pure Enum definition.
 * 
 * Pure syntax:
 * 
 * <pre>
 * Enum package::EnumName
 * {
 *     VALUE1,
 *     VALUE2,
 *     VALUE3
 * }
 * </pre>
 * 
 * Example:
 * 
 * <pre>
 * Enum model::Status
 * {
 *     ACTIVE,
 *     INACTIVE,
 *     PENDING
 * }
 * </pre>
 * 
 * Enums are mapped to SQL VARCHAR columns, with values stored as strings.
 * 
 * @param qualifiedName The fully qualified enum name (e.g., "model::Status")
 * @param values        The list of enum value names
 */
public record EnumDefinition(
        String qualifiedName,
        List<String> values) implements PureDefinition {

    public EnumDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        Objects.requireNonNull(values, "Values cannot be null");
        if (values.isEmpty()) {
            throw new IllegalArgumentException("Enum must have at least one value");
        }
        values = List.copyOf(values);
    }

    /**
     * @return The simple enum name (without package)
     */
    public String simpleName() {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(idx + 2) : qualifiedName;
    }

    /**
     * @return The package path (without enum name)
     */
    public String packagePath() {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(0, idx) : "";
    }

    /**
     * Checks if a value is valid for this enum.
     * 
     * @param value The value to check
     * @return true if the value is a valid enum value
     */
    public boolean hasValue(String value) {
        return values.contains(value);
    }

    /**
     * Creates an EnumDefinition from qualified name and values.
     */
    public static EnumDefinition of(String qualifiedName, String... values) {
        return new EnumDefinition(qualifiedName, List.of(values));
    }

    /**
     * Creates an EnumDefinition from qualified name and values list.
     */
    public static EnumDefinition of(String qualifiedName, List<String> values) {
        return new EnumDefinition(qualifiedName, values);
    }
}
