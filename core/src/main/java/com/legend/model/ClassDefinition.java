package com.legend.model;

import com.legend.protocol.ConstraintDefinition;
import com.legend.protocol.DerivedPropertyDefinition;
import com.legend.protocol.Multiplicity;
import com.legend.protocol.TypeExpression;

import java.util.List;
import java.util.Objects;

/**
 * A parsed Pure {@code Class} declaration.
 *
 * <p>Pure syntax (simplified):
 * <pre>
 *   [&lt;&lt;stereotypes&gt;&gt;] [{ taggedValues }] Class qualifiedName
 *     [&lt;typeParams&gt;] [extends Super, Super]
 *     [[constraints]]
 *   {
 *     property: Type[multiplicity];
 *     derived() { expr }: Type[multiplicity];
 *   }
 * </pre>
 *
 * <p>Mirrors engine's {@code com.gs.legend.model.def.ClassDefinition}
 * record shape verbatim. The current parser (sub-slice B.1) populates
 * only {@code qualifiedName}, {@code typeParams}, {@code superClasses},
 * {@code properties}, {@code stereotypes}, {@code taggedValues}, and
 * {@code isNative}. Sub-slices B.2+ will populate
 * {@code derivedProperties} and {@code constraints}.
 *
 * <p>{@link PropertyDefinition} stays nested for engine parity.
 * {@link DerivedPropertyDefinition}, {@link ConstraintDefinition}, and
 * {@link com.legend.protocol.ParameterDefinition} are parse products and live
 * in {@code com.legend.protocol} — the parser's output types depend on
 * nothing above the protocol layer.
 *
 * @param qualifiedName     fully qualified class name (e.g. {@code "model::Person"})
 * @param typeParams        generic type parameter names ({@code <T, U>}); empty list if absent
 * @param superClasses      direct superclass references as structured ASTs; empty if none
 * @param properties        regular property declarations
 * @param derivedProperties derived (computed) property declarations
 * @param constraints       class-level constraints
 * @param stereotypes       stereotype annotations on the class
 * @param taggedValues      tagged-value annotations on the class
 * @param isNative          {@code true} if declared with the {@code native} prefix
 */
public record ClassDefinition(
        String qualifiedName,
        List<String> typeParams,
        /** TYPE VARIABLES ({@code Class X(x:Integer[1])}): carried as declared (name, type,
         * multiplicity); a body naming {@code $x} types against them — batch 174 */
        List<com.legend.protocol.ParameterDefinition> typeVariables,
        List<TypeExpression> superClasses,
        List<PropertyDefinition> properties,
        List<DerivedPropertyDefinition> derivedProperties,
        List<ConstraintDefinition> constraints,
        List<StereotypeApplication> stereotypes,
        List<TaggedValue> taggedValues,
        boolean isNative)
        implements PackageableElement {

    /** The pre-type-variable arity: no type variables. */
    public ClassDefinition(String qualifiedName, List<String> typeParams,
            List<TypeExpression> superClasses, List<PropertyDefinition> properties,
            List<DerivedPropertyDefinition> derivedProperties, List<ConstraintDefinition> constraints,
            List<StereotypeApplication> stereotypes, List<TaggedValue> taggedValues, boolean isNative) {
        this(qualifiedName, typeParams, List.of(), superClasses, properties, derivedProperties,
                constraints, stereotypes, taggedValues, isNative);
    }

    public ClassDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        typeParams = typeParams == null ? List.of() : List.copyOf(typeParams);
        superClasses = superClasses == null ? List.of() : List.copyOf(superClasses);
        properties = properties == null ? List.of() : List.copyOf(properties);
        derivedProperties = derivedProperties == null ? List.of() : List.copyOf(derivedProperties);
        constraints = constraints == null ? List.of() : List.copyOf(constraints);
        stereotypes = stereotypes == null ? List.of() : List.copyOf(stereotypes);
        taggedValues = taggedValues == null ? List.of() : List.copyOf(taggedValues);
    }

    // ============================================================
    // Nested data records (engine parity)
    // ============================================================

    /**
     * A regular property declaration.
     *
     * <p>Pure syntax: {@code propName: Type[multiplicity];}
     *
     * <p>Multiplicity is stored flat as {@code (lowerBound, upperBound)} where
     * {@code upperBound == null} means unbounded ({@code *}). This matches
     * engine's PropertyDefinition shape exactly.
     *
     * @param name         property name
     * @param type         property type (simple or qualified, unresolved until {@code NameResolver} runs)
     * @param lowerBound   lower multiplicity bound (0 = optional, 1+ = required)
     * @param upperBound   upper multiplicity bound ({@code null} = unbounded {@code *})
     * @param stereotypes  stereotype annotations on this property
     * @param taggedValues tagged-value annotations on this property
     */
    public record PropertyDefinition(
            String name,
            TypeExpression type,
            Multiplicity multiplicity,
            List<StereotypeApplication> stereotypes,
            List<TaggedValue> taggedValues,
            boolean hasDefault,
            /** The declared default VALUE ({@code distinct: Boolean[1] = false}) —
             * applied by the new-instance checker to an unspelled property
             * (real pure's constructor semantics); null when none. */
            com.legend.protocol.spec.@com.legend.base.Nullable ValueSpecification defaultValue) {
        public PropertyDefinition(String name, TypeExpression type,
                Multiplicity multiplicity,
                List<StereotypeApplication> stereotypes,
                List<TaggedValue> taggedValues, boolean hasDefault) {
            this(name, type, multiplicity, stereotypes, taggedValues, hasDefault, null);
        }
        public PropertyDefinition {
            Objects.requireNonNull(name, "Property name cannot be null");
            Objects.requireNonNull(type, "Property type cannot be null");
            Objects.requireNonNull(multiplicity, "Property multiplicity cannot be null");
            stereotypes = stereotypes == null ? List.of() : List.copyOf(stereotypes);
            taggedValues = taggedValues == null ? List.of() : List.copyOf(taggedValues);
        }

        /** Pre-default-flag arity (a defaulted declaration like the
         * engine's {@code distinct: Boolean[1] = false} exempts the
         * property from ^new's missing-required validation — pure
         * NewValidator parity). */
        public PropertyDefinition(String name, TypeExpression type,
                Multiplicity multiplicity,
                List<StereotypeApplication> stereotypes,
                List<TaggedValue> taggedValues) {
            this(name, type, multiplicity, stereotypes, taggedValues, false);
        }
    }

}
