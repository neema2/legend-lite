package com.gs.legend.model.m3;

import com.gs.legend.model.def.TaggedValue;

import java.util.List;
import java.util.Objects;

/**
 * Represents a property within a Pure Class.
 * A property has a name, a {@link TypeRef lightweight type reference} (FQN + kind),
 * and multiplicity constraints.
 *
 * <p>Phase A of the Bazel cross-project dependency work replaced the resolved
 * {@code Type genericType} field with {@link TypeRef} so cross-project property
 * types don't force their target classes or enums to load eagerly. See
 * {@code docs/BAZEL_IMPLEMENTATION_PLAN.md} §2. Consumers that need the plan-layer
 * type should go through {@link com.gs.legend.plan.GenericType#fromTypeRef(TypeRef)}.
 *
 * @param name The property name (e.g., "firstName")
 * @param multiplicity The cardinality constraints for this property
 * @param taggedValues Tagged value annotations on this property (e.g., nlq.description)
 * @param typeRef Lightweight type reference (FQN + kind) — the canonical type handle
 */
public record Property(
        String name,
        Multiplicity multiplicity,
        List<TaggedValue> taggedValues,
        TypeRef typeRef
) {
    public Property {
        Objects.requireNonNull(name, "Property name cannot be null");
        Objects.requireNonNull(multiplicity, "Property multiplicity cannot be null");
        Objects.requireNonNull(typeRef, "Property typeRef cannot be null");
        taggedValues = taggedValues == null ? List.of() : List.copyOf(taggedValues);

        if (name.isBlank()) {
            throw new IllegalArgumentException("Property name cannot be blank");
        }
    }

    /**
     * Ergonomic constructor with natural (name, typeRef, multiplicity, taggedValues) ordering
     * — delegates to the canonical constructor.
     */
    public Property(String name, TypeRef typeRef, Multiplicity multiplicity, List<TaggedValue> taggedValues) {
        this(name, multiplicity, taggedValues, typeRef);
    }

    /**
     * Ergonomic constructor — no annotations.
     */
    public Property(String name, TypeRef typeRef, Multiplicity multiplicity) {
        this(name, multiplicity, List.of(), typeRef);
    }

    /**
     * Convenience accessor — returns the fully qualified name of this property's type.
     * Equivalent to {@code typeRef().fqn()}. Useful for display, logging, and string
     * comparison where the type's kind (primitive/class/enum) is not needed.
     */
    public String typeFqn() {
        return typeRef.fqn();
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
        return name + ": " + typeRef.fqn() + multiplicity;
    }
}
