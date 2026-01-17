package org.finos.legend.pure.dsl.definition;

import java.util.List;
import java.util.Objects;

/**
 * Represents a Pure Class definition.
 * 
 * Pure syntax:
 * <pre>
 * Class package::ClassName
 * {
 *     propertyName: Type[multiplicity];
 * }
 * </pre>
 * 
 * Example:
 * <pre>
 * Class model::Person
 * {
 *     firstName: String[1];
 *     lastName: String[1];
 *     age: Integer[1];
 * }
 * </pre>
 * 
 * @param qualifiedName The fully qualified class name (e.g., "model::Person")
 * @param properties The list of property definitions
 */
public record ClassDefinition(
        String qualifiedName,
        List<PropertyDefinition> properties
) implements PureDefinition {
    
    public ClassDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        Objects.requireNonNull(properties, "Properties cannot be null");
        properties = List.copyOf(properties);
    }
    
    /**
     * @return The simple class name (without package)
     */
    public String simpleName() {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(idx + 2) : qualifiedName;
    }
    
    /**
     * @return The package path (without class name)
     */
    public String packagePath() {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(0, idx) : "";
    }
    
    /**
     * Represents a property definition within a class.
     * 
     * @param name The property name
     * @param type The property type (e.g., "String", "Integer")
     * @param lowerBound Lower multiplicity bound
     * @param upperBound Upper multiplicity bound (null for *)
     */
    public record PropertyDefinition(
            String name,
            String type,
            int lowerBound,
            Integer upperBound
    ) {
        public PropertyDefinition {
            Objects.requireNonNull(name, "Property name cannot be null");
            Objects.requireNonNull(type, "Property type cannot be null");
        }
        
        /**
         * Creates a required [1] property.
         */
        public static PropertyDefinition required(String name, String type) {
            return new PropertyDefinition(name, type, 1, 1);
        }
        
        /**
         * Creates an optional [0..1] property.
         */
        public static PropertyDefinition optional(String name, String type) {
            return new PropertyDefinition(name, type, 0, 1);
        }
        
        /**
         * Creates a many [*] property.
         */
        public static PropertyDefinition many(String name, String type) {
            return new PropertyDefinition(name, type, 0, null);
        }
        
        public String multiplicityString() {
            if (upperBound == null) {
                return lowerBound == 0 ? "*" : lowerBound + "..*";
            }
            if (lowerBound == upperBound) {
                return String.valueOf(lowerBound);
            }
            return lowerBound + ".." + upperBound;
        }
    }
}
