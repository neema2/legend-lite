package com.legend.model;

import com.legend.protocol.DerivedPropertyDefinition;

import com.legend.protocol.TypeExpression;

import com.legend.protocol.Multiplicity;

import java.util.List;
import java.util.Objects;

/**
 * A parsed Pure {@code Association} declaration.
 *
 * <p>Pure syntax:
 * <pre>
 *   Association package::AssociationName
 *   {
 *     propertyOnA: ClassB[multiplicity];
 *     propertyOnB: ClassA[multiplicity];
 *   }
 * </pre>
 *
 * <p>Mirrors engine's {@code com.gs.legend.model.def.AssociationDefinition}.
 * Engine's {@code simpleName()} / {@code packagePath()} default methods are
 * deliberately omitted &mdash; see
 * {@link PackageableElement} for the rationale.
 *
 * @param qualifiedName fully qualified association name
 * @param property1     the first association end
 * @param property2     the second association end
 */
public record AssociationDefinition(
        String qualifiedName,
        AssociationEndDefinition property1,
        AssociationEndDefinition property2,
        List<DerivedPropertyDefinition> derivedProperties,
        /** the association's own stereotypes and tagged values (their profiles are references the compiler checks) */
        List<StereotypeApplication> stereotypes,
        List<TaggedValue> taggedValues)
        implements PackageableElement {

    public AssociationDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        Objects.requireNonNull(property1, "Property1 cannot be null");
        Objects.requireNonNull(property2, "Property2 cannot be null");
        derivedProperties = derivedProperties == null ? List.of() : List.copyOf(derivedProperties);
        stereotypes = List.copyOf(stereotypes);
        taggedValues = List.copyOf(taggedValues);
    }

    /** An association with qualified properties and no annotations of its own. */
    public AssociationDefinition(String qualifiedName, AssociationEndDefinition property1,
            AssociationEndDefinition property2, List<DerivedPropertyDefinition> derivedProperties) {
        this(qualifiedName, property1, property2, derivedProperties, List.of(), List.of());
    }

    /** The common two-end form (no qualified properties). */
    public AssociationDefinition(String qualifiedName,
                                 AssociationEndDefinition property1,
                                 AssociationEndDefinition property2) {
        this(qualifiedName, property1, property2, List.of());
    }

    /**
     * One end of an association &mdash; a property name plus the target class
     * and multiplicity. Multiplicity is stored flat as
     * {@code (lowerBound, upperBound)} where {@code upperBound == null} means
     * unbounded ({@code *}).
     *
     * @param propertyName property name on the source side
     * @param targetClass  the class this property points to, as a structured AST
     * @param multiplicity the end's multiplicity
     * @param stereotypes  the stereotypes written on the end (the parser reads an end as an ordinary wire property,
     *                     annotations and all; the property-graph profile marks an edge class's ends here,
     *                     docs/CYPHER_STUDY_2026_10_15.md §5.6)
     */
    public record AssociationEndDefinition(
            String propertyName,
            TypeExpression targetClass,
            Multiplicity multiplicity,
            List<StereotypeApplication> stereotypes,
            List<TaggedValue> taggedValues) {
        public AssociationEndDefinition {
            Objects.requireNonNull(propertyName, "Property name cannot be null");
            Objects.requireNonNull(targetClass, "Target class cannot be null");
            Objects.requireNonNull(multiplicity, "End multiplicity cannot be null");
            stereotypes = stereotypes == null ? List.of() : List.copyOf(stereotypes);
            taggedValues = List.copyOf(taggedValues);
        }

        /** An end with stereotypes and no tagged value. */
        public AssociationEndDefinition(String propertyName, TypeExpression targetClass, Multiplicity multiplicity,
                List<StereotypeApplication> stereotypes) {
            this(propertyName, targetClass, multiplicity, stereotypes, List.of());
        }

        /** An end with no stereotype. */
        public AssociationEndDefinition(String propertyName, TypeExpression targetClass, Multiplicity multiplicity) {
            this(propertyName, targetClass, multiplicity, List.of());
        }

        /**
         * The end's target class FQN. Name resolution guarantees a resolved
         * {@link TypeExpression.NameRef} on every end that leaves the
         * normalizer — anything else here is a normalizer bug. This is the
         * ONE extraction site (audit 15: three independent copies, one an
         * unguarded cast, had divergent failure modes).
         */
        public String targetClassFqn() {
            if (targetClass instanceof TypeExpression.NameRef nr) {
                return nr.name();
            }
            throw new IllegalStateException("association end '" + propertyName
                    + "' has unresolved target class type "
                    + targetClass.getClass().getSimpleName()
                    + " — normalizer bug");
        }

        /** Known-bounds multiplicity? Parameter multiplicities are not. */
        public boolean isConcrete() {
            return multiplicity instanceof Multiplicity.Concrete;
        }

        /** Upper bound exactly 1. False for to-many AND non-concrete ends. */
        public boolean isToOne() {
            return multiplicity instanceof Multiplicity.Concrete c
                    && Integer.valueOf(1).equals(c.upperBound());
        }
    }
}
