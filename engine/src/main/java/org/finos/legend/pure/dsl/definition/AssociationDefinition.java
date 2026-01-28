package org.finos.legend.pure.dsl.definition;

import java.util.Objects;

/**
 * Represents a Pure Association definition.
 * 
 * An Association defines a bidirectional relationship between two classes.
 * 
 * Pure syntax:
 * <pre>
 * Association package::AssociationName
 * {
 *     propertyOnClassA: ClassB[multiplicity];
 *     propertyOnClassB: ClassA[multiplicity];
 * }
 * </pre>
 * 
 * Example:
 * <pre>
 * Association model::Person_Address
 * {
 *     person: Person[1];
 *     addresses: Address[*];
 * }
 * </pre>
 * 
 * @param qualifiedName The fully qualified association name
 * @param property1 The first association end
 * @param property2 The second association end
 */
public record AssociationDefinition(
        String qualifiedName,
        AssociationEndDefinition property1,
        AssociationEndDefinition property2
) implements PureDefinition {
    
    public AssociationDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        Objects.requireNonNull(property1, "Property1 cannot be null");
        Objects.requireNonNull(property2, "Property2 cannot be null");
    }
    
    /**
     * @return The simple association name (without package)
     */
    public String simpleName() {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(idx + 2) : qualifiedName;
    }
    
    /**
     * @return The package path (without name)
     */
    public String packagePath() {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(0, idx) : "";
    }
    
    /**
     * Represents one end of an association.
     * 
     * @param propertyName The property name
     * @param targetClass The class this property points to
     * @param lowerBound The lower multiplicity bound
     * @param upperBound The upper multiplicity bound (null for *)
     */
    public record AssociationEndDefinition(
            String propertyName,
            String targetClass,
            int lowerBound,
            Integer upperBound
    ) {
        public AssociationEndDefinition {
            Objects.requireNonNull(propertyName, "Property name cannot be null");
            Objects.requireNonNull(targetClass, "Target class cannot be null");
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
