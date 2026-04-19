package com.gs.legend.model.m3;

import com.gs.legend.model.def.StereotypeApplication;
import com.gs.legend.model.def.TaggedValue;

import java.util.List;
import java.util.Objects;

/**
 * Represents a property within a Pure Class.
 * A property has a name, a nominal {@link Type type reference} (primitive, class, or enum),
 * and multiplicity constraints.
 *
 * <p>The {@code type} field is one of the nominal {@link Type} variants
 * ({@link Primitive}, {@link Type.ClassType}, {@link Type.EnumType}) and carries only
 * the FQN string plus kind discriminator — no resolved {@link PureClass} /
 * {@link PureEnum} object payload. Cross-project property types therefore do not force
 * their target classes or enums to load eagerly (the Bazel Phase A invariant; see
 * {@code docs/BAZEL_IMPLEMENTATION_PLAN.md} §2). This replaces the legacy
 * {@code m3.TypeRef} with the unified {@link Type} hierarchy introduced in Phase B 2.5a.
 *
 * <p>Phase 2.5e added {@code stereotypes} — user-authored stereotype applications like
 * {@code <<equality.Key>>} that previously lived only on the AST-layer
 * {@code ClassDefinition.PropertyDefinition} and were dropped during symbol conversion.
 *
 * @param name The property name (e.g., "firstName")
 * @param multiplicity The cardinality constraints for this property
 * @param taggedValues Tagged value annotations on this property (e.g., nlq.description)
 * @param stereotypes Stereotype annotations on this property (e.g., equality.Key)
 * @param type Nominal type (primitive / class / enum) — the canonical type handle
 */
public record Property(
        String name,
        Multiplicity multiplicity,
        List<TaggedValue> taggedValues,
        List<StereotypeApplication> stereotypes,
        Type type
) {
    public Property {
        Objects.requireNonNull(name, "Property name cannot be null");
        Objects.requireNonNull(multiplicity, "Property multiplicity cannot be null");
        Objects.requireNonNull(type, "Property type cannot be null");
        taggedValues = taggedValues == null ? List.of() : List.copyOf(taggedValues);
        stereotypes = stereotypes == null ? List.of() : List.copyOf(stereotypes);

        if (name.isBlank()) {
            throw new IllegalArgumentException("Property name cannot be blank");
        }
    }

    /**
     * Full-fidelity constructor preserving pre-phase-2.5e field ordering (no stereotypes).
     * Existing call sites continue to compile unchanged.
     */
    public Property(String name, Multiplicity multiplicity, List<TaggedValue> taggedValues, Type type) {
        this(name, multiplicity, taggedValues, List.of(), type);
    }

    /**
     * Ergonomic constructor with natural (name, type, multiplicity, taggedValues) ordering
     * — delegates to the canonical constructor.
     */
    public Property(String name, Type type, Multiplicity multiplicity, List<TaggedValue> taggedValues) {
        this(name, multiplicity, taggedValues, List.of(), type);
    }

    /**
     * Ergonomic constructor carrying stereotypes + tagged values in natural ordering.
     */
    public Property(String name, Type type, Multiplicity multiplicity,
                    List<TaggedValue> taggedValues, List<StereotypeApplication> stereotypes) {
        this(name, multiplicity, taggedValues, stereotypes, type);
    }

    /**
     * Ergonomic constructor — no annotations.
     */
    public Property(String name, Type type, Multiplicity multiplicity) {
        this(name, multiplicity, List.of(), List.of(), type);
    }

    /**
     * Checks whether this property carries the given stereotype.
     */
    public boolean hasStereotype(String profileName, String stereotypeName) {
        for (StereotypeApplication sa : stereotypes) {
            if (sa.profileName().equals(profileName) && sa.stereotypeName().equals(stereotypeName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Convenience accessor — returns the fully qualified name of this property's type.
     * For primitives returns the Pure name ({@code "Integer"}); for classes and enums
     * returns the fully qualified name ({@code "model::Person"}). Useful for display,
     * logging, and string comparison where the type's kind is not needed.
     */
    public String typeFqn() {
        return switch (type) {
            case Primitive p -> p.pureName();
            case Type.ClassType c -> c.qualifiedName();
            case Type.EnumType e -> e.qualifiedName();
            default -> throw new IllegalStateException(
                    "Property type must be nominal (primitive/class/enum), got: " + type);
        };
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
        return name + ": " + typeFqn() + multiplicity;
    }
}
