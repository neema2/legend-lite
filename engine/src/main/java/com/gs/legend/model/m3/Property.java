package com.gs.legend.model.m3;

import com.gs.legend.model.def.TaggedValue;

import java.util.List;
import java.util.Objects;

/**
 * Represents a property within a Pure Class.
 * A property has a name, a type reference, and multiplicity constraints.
 *
 * @param name The property name (e.g., "firstName")
 * @param genericType The resolved type of this property (kept during Phase A adapter window)
 * @param multiplicity The cardinality constraints for this property
 * @param taggedValues Tagged value annotations on this property (e.g., nlq.description)
 * @param typeRef Lightweight type reference (FQN + kind), derived from {@code genericType} if null.
 *                Phase A of the Bazel cross-project dependency work introduces this as the
 *                canonical type reference; see {@code docs/BAZEL_IMPLEMENTATION_PLAN.md} §2.
 *                Consumers should read {@code typeRef()} instead of {@code genericType()} and
 *                prefer {@link com.gs.legend.plan.GenericType#fromTypeRef(TypeRef)} for conversion
 *                into the plan layer. A convenience {@link #typeFqn()} accessor returns the FQN
 *                string for sites that only need it for display or string comparison.
 */
public record Property(
        String name,
        Type genericType,
        Multiplicity multiplicity,
        List<TaggedValue> taggedValues,
        TypeRef typeRef
) {
    public Property {
        Objects.requireNonNull(name, "Property name cannot be null");
        Objects.requireNonNull(genericType, "Property genericType cannot be null");
        Objects.requireNonNull(multiplicity, "Property multiplicity cannot be null");
        taggedValues = taggedValues == null ? List.of() : List.copyOf(taggedValues);

        if (name.isBlank()) {
            throw new IllegalArgumentException("Property name cannot be blank");
        }

        // Derive typeRef from genericType when not supplied explicitly.
        if (typeRef == null) {
            typeRef = TypeRef.of(genericType);
        }
    }

    /**
     * Constructor matching the pre-TypeRef record shape; derives typeRef from genericType.
     */
    public Property(String name, Type genericType, Multiplicity multiplicity, List<TaggedValue> taggedValues) {
        this(name, genericType, multiplicity, taggedValues, null);
    }

    /**
     * Constructor for backwards compatibility (no annotations).
     */
    public Property(String name, Type genericType, Multiplicity multiplicity) {
        this(name, genericType, multiplicity, List.of(), null);
    }

    /**
     * Convenience accessor — returns the fully qualified name of this property's type.
     * Equivalent to {@code typeRef().fqn()}. Useful for display, logging, and string
     * comparison where the type's kind (primitive/class/enum) is not needed.
     */
    public String typeFqn() {
        return typeRef.fqn();
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
