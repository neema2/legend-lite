package com.legend.parser.element;

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
 * <p>{@link PropertyDefinition}, {@link DerivedPropertyDefinition},
 * {@link ConstraintDefinition}, and {@link ParameterDefinition} are
 * nested for engine parity &mdash; engine has them all inside
 * {@code ClassDefinition.java} as nested records.
 *
 * @param qualifiedName     fully qualified class name (e.g. {@code "model::Person"})
 * @param typeParams        generic type parameter names ({@code <T, U>}); empty list if absent
 * @param superClasses      direct superclass FQNs (or simple names, before resolution); empty if none
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
        List<String> superClasses,
        List<PropertyDefinition> properties,
        List<DerivedPropertyDefinition> derivedProperties,
        List<ConstraintDefinition> constraints,
        List<StereotypeApplication> stereotypes,
        List<TaggedValue> taggedValues,
        boolean isNative) implements PackageableElement {

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
            String type,
            int lowerBound,
            Integer upperBound,
            List<StereotypeApplication> stereotypes,
            List<TaggedValue> taggedValues) {
        public PropertyDefinition {
            Objects.requireNonNull(name, "Property name cannot be null");
            Objects.requireNonNull(type, "Property type cannot be null");
            stereotypes = stereotypes == null ? List.of() : List.copyOf(stereotypes);
            taggedValues = taggedValues == null ? List.of() : List.copyOf(taggedValues);
        }
    }

    /**
     * A derived (computed) property declaration. Parser captures the body
     * as a raw text span between braces; {@code SpecCompiler} parses and
     * type-checks it on demand (lazy bodies invariant).
     *
     * @param name        property name
     * @param parameters  parameter list (zero or more)
     * @param expression  raw text of the body between {@code {...}}
     * @param type        return type
     * @param lowerBound  lower multiplicity bound
     * @param upperBound  upper multiplicity bound ({@code null} = unbounded)
     */
    public record DerivedPropertyDefinition(
            String name,
            List<ParameterDefinition> parameters,
            String expression,
            String type,
            int lowerBound,
            Integer upperBound) {
        public DerivedPropertyDefinition {
            Objects.requireNonNull(name, "Derived property name cannot be null");
            Objects.requireNonNull(type, "Derived property type cannot be null");
            parameters = parameters == null ? List.of() : List.copyOf(parameters);
            expression = expression == null ? "" : expression;
        }
    }

    /**
     * A parameter declaration on a derived property or function.
     *
     * @param name       parameter name
     * @param type       parameter type
     * @param lowerBound lower multiplicity bound
     * @param upperBound upper multiplicity bound ({@code null} = unbounded)
     */
    public record ParameterDefinition(
            String name,
            String type,
            int lowerBound,
            Integer upperBound) {
        public ParameterDefinition {
            Objects.requireNonNull(name, "Parameter name cannot be null");
            Objects.requireNonNull(type, "Parameter type cannot be null");
        }
    }

    /**
     * A class-level constraint (validation rule).
     *
     * @param name       constraint name
     * @param expression the Pure expression body that must evaluate to true
     */
    public record ConstraintDefinition(String name, String expression) {
        public ConstraintDefinition {
            Objects.requireNonNull(name, "Constraint name cannot be null");
            Objects.requireNonNull(expression, "Constraint expression cannot be null");
        }
    }
}
