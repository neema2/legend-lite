package com.legend.parser.element;

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
        AssociationEndDefinition property2) implements PackageableElement {

    public AssociationDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        Objects.requireNonNull(property1, "Property1 cannot be null");
        Objects.requireNonNull(property2, "Property2 cannot be null");
    }

    /**
     * One end of an association &mdash; a property name plus the target class
     * and multiplicity. Multiplicity is stored flat as
     * {@code (lowerBound, upperBound)} where {@code upperBound == null} means
     * unbounded ({@code *}).
     *
     * @param propertyName property name on the source side
     * @param targetClass  the class this property points to (simple or qualified, unresolved)
     * @param lowerBound   lower multiplicity bound
     * @param upperBound   upper multiplicity bound ({@code null} = unbounded)
     */
    public record AssociationEndDefinition(
            String propertyName,
            String targetClass,
            Multiplicity multiplicity) {
        public AssociationEndDefinition {
            Objects.requireNonNull(propertyName, "Property name cannot be null");
            Objects.requireNonNull(targetClass, "Target class cannot be null");
            Objects.requireNonNull(multiplicity, "End multiplicity cannot be null");
        }
    }
}
