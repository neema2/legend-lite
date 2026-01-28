package org.finos.legend.pure.m3;

import java.util.Objects;

/**
 * Represents an Association between two Pure classes.
 * 
 * An Association defines a bidirectional relationship between two classes,
 * with properties on each end specifying the navigation and multiplicity.
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
 * @param packagePath The package path
 * @param name The association name
 * @param property1 The first end property (typically the "one" side)
 * @param property2 The second end property (typically the "many" side)
 */
public record Association(
        String packagePath,
        String name,
        AssociationEnd property1,
        AssociationEnd property2
) {
    
    public Association {
        Objects.requireNonNull(packagePath, "Package path cannot be null");
        Objects.requireNonNull(name, "Association name cannot be null");
        Objects.requireNonNull(property1, "Property1 cannot be null");
        Objects.requireNonNull(property2, "Property2 cannot be null");
    }
    
    /**
     * Convenience constructor for root package.
     */
    public Association(String name, AssociationEnd property1, AssociationEnd property2) {
        this("", name, property1, property2);
    }
    
    /**
     * @return The fully qualified association name
     */
    public String qualifiedName() {
        return packagePath.isEmpty() ? name : packagePath + "::" + name;
    }
    
    /**
     * Gets the property that navigates TO a given class.
     * 
     * @param className The target class name
     * @return The property that points to that class, or null if not found
     */
    public AssociationEnd getPropertyForClass(String className) {
        if (property1.targetClass().equals(className)) {
            return property1;
        }
        if (property2.targetClass().equals(className)) {
            return property2;
        }
        return null;
    }
    
    /**
     * Gets the property FROM a given class (the reverse navigation).
     * 
     * @param className The source class name
     * @return The property from that class, or null if not found
     */
    public AssociationEnd getPropertyFromClass(String className) {
        if (property1.targetClass().equals(className)) {
            return property2;
        }
        if (property2.targetClass().equals(className)) {
            return property1;
        }
        return null;
    }
    
    /**
     * Represents one end of an association.
     * 
     * @param propertyName The property name as seen from the other class
     * @param targetClass The class this property points to
     * @param multiplicity The cardinality
     */
    public record AssociationEnd(
            String propertyName,
            String targetClass,
            Multiplicity multiplicity
    ) {
        public AssociationEnd {
            Objects.requireNonNull(propertyName, "Property name cannot be null");
            Objects.requireNonNull(targetClass, "Target class cannot be null");
            Objects.requireNonNull(multiplicity, "Multiplicity cannot be null");
        }
        
        /**
         * Creates a required [1] association end.
         */
        public static AssociationEnd one(String propertyName, String targetClass) {
            return new AssociationEnd(propertyName, targetClass, Multiplicity.ONE);
        }
        
        /**
         * Creates an optional [0..1] association end.
         */
        public static AssociationEnd zeroOrOne(String propertyName, String targetClass) {
            return new AssociationEnd(propertyName, targetClass, Multiplicity.ZERO_OR_ONE);
        }
        
        /**
         * Creates a many [*] association end.
         */
        public static AssociationEnd many(String propertyName, String targetClass) {
            return new AssociationEnd(propertyName, targetClass, Multiplicity.MANY);
        }
    }
    
    @Override
    public String toString() {
        return "Association " + qualifiedName() + " { " + 
                property1.propertyName() + ": " + property1.targetClass() + "[" + property1.multiplicity() + "]; " +
                property2.propertyName() + ": " + property2.targetClass() + "[" + property2.multiplicity() + "] }";
    }
}
