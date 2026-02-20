package org.finos.legend.pure.m3;

import org.finos.legend.pure.dsl.definition.TaggedValue;

import java.util.List;
import java.util.Objects;

/**
 * Represents a property within a Pure Class.
 * A property has a name, a generic type reference, and multiplicity constraints.
 * 
 * @param name The property name (e.g., "firstName")
 * @param genericType The type of this property (can be primitive or class reference)
 * @param multiplicity The cardinality constraints for this property
 * @param taggedValues Tagged value annotations on this property (e.g., nlq.description)
 */
public record Property(
        String name,
        Type genericType,
        Multiplicity multiplicity,
        List<TaggedValue> taggedValues
) {
    public Property {
        Objects.requireNonNull(name, "Property name cannot be null");
        Objects.requireNonNull(genericType, "Property genericType cannot be null");
        Objects.requireNonNull(multiplicity, "Property multiplicity cannot be null");
        taggedValues = taggedValues == null ? List.of() : List.copyOf(taggedValues);
        
        if (name.isBlank()) {
            throw new IllegalArgumentException("Property name cannot be blank");
        }
    }

    /**
     * Constructor for backwards compatibility (no annotations).
     */
    public Property(String name, Type genericType, Multiplicity multiplicity) {
        this(name, genericType, multiplicity, List.of());
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

    /**
     * Gets the value of a tagged value by profile and tag name.
     * 
     * @param profileName The profile name (e.g., "nlq")
     * @param tagName The tag name (e.g., "description")
     * @return The tag value, or null if not found
     */
    public String getTagValue(String profileName, String tagName) {
        for (TaggedValue tv : taggedValues) {
            if (tv.profileName().equals(profileName) && tv.tagName().equals(tagName)) {
                return tv.value();
            }
        }
        return null;
    }
    
    @Override
    public String toString() {
        return name + ": " + genericType.typeName() + multiplicity;
    }
}
