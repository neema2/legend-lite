package org.finos.legend.pure.m3;

import java.util.Objects;

/**
 * Represents a property within a Pure Class.
 * A property has a name, a generic type reference, and multiplicity constraints.
 * 
 * @param name The property name (e.g., "firstName")
 * @param genericType The type of this property (can be primitive or class reference)
 * @param multiplicity The cardinality constraints for this property
 */
public record Property(
        String name,
        Type genericType,
        Multiplicity multiplicity
) {
    public Property {
        Objects.requireNonNull(name, "Property name cannot be null");
        Objects.requireNonNull(genericType, "Property genericType cannot be null");
        Objects.requireNonNull(multiplicity, "Property multiplicity cannot be null");
        
        if (name.isBlank()) {
            throw new IllegalArgumentException("Property name cannot be blank");
        }
    }
    
    /**
     * Factory method for creating a required single-valued property.
     */
    public static Property required(String name, Type type) {
        return new Property(name, type, Multiplicity.ONE);
    }
    
    /**
     * Factory method for creating an optional single-valued property.
     */
    public static Property optional(String name, Type type) {
        return new Property(name, type, Multiplicity.ZERO_ONE);
    }
    
    /**
     * Factory method for creating a collection property.
     */
    public static Property many(String name, Type type) {
        return new Property(name, type, Multiplicity.MANY);
    }
    
    public boolean isRequired() {
        return multiplicity.isRequired();
    }
    
    public boolean isCollection() {
        return !multiplicity.isSingular();
    }
    
    @Override
    public String toString() {
        return name + ": " + genericType.typeName() + multiplicity;
    }
}
