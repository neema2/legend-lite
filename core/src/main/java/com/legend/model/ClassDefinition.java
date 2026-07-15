package com.legend.model;

import com.legend.model.TypeExpression;

import com.legend.model.Multiplicity;

import com.legend.model.spec.ValueSpecification;

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
        List<TypeExpression> superClasses,
        List<PropertyDefinition> properties,
        List<DerivedPropertyDefinition> derivedProperties,
        List<ConstraintDefinition> constraints,
        List<StereotypeApplication> stereotypes,
        List<TaggedValue> taggedValues,
        boolean isNative)
        implements PackageableElement {

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
            List<TaggedValue> taggedValues) {
        public PropertyDefinition {
            Objects.requireNonNull(name, "Property name cannot be null");
            Objects.requireNonNull(type, "Property type cannot be null");
            Objects.requireNonNull(multiplicity, "Property multiplicity cannot be null");
            stereotypes = stereotypes == null ? List.of() : List.copyOf(stereotypes);
            taggedValues = taggedValues == null ? List.of() : List.copyOf(taggedValues);
        }
    }

    /**
     * A derived (computed) property declaration. Body is parsed eagerly
     * by {@code ElementParser} into a sequence of {@link ValueSpecification}
     * statements (the body grammar matches a function body's braced block).
     *
     * @param name        property name
     * @param parameters  parameter list (zero or more)
     * @param expression  parsed body statements between {@code {...}}; non-null,
     *                    may be empty for a {@code {}} body
     * @param type        return type
     * @param lowerBound  lower multiplicity bound
     * @param upperBound  upper multiplicity bound ({@code null} = unbounded)
     */
    public record DerivedPropertyDefinition(
            String name,
            List<ParameterDefinition> parameters,
            Realization realization,
            TypeExpression type,
            Multiplicity multiplicity) {
        public DerivedPropertyDefinition {
            Objects.requireNonNull(name, "Derived property name cannot be null");
            Objects.requireNonNull(type, "Derived property type cannot be null");
            Objects.requireNonNull(multiplicity, "Derived property multiplicity cannot be null");
            Objects.requireNonNull(realization, "Derived property realization cannot be null");
            parameters = parameters == null ? List.of() : List.copyOf(parameters);
        }

        /** Convenience: the sugar (inline-expression) form. */
        public DerivedPropertyDefinition(String name, List<ParameterDefinition> parameters,
                                         List<ValueSpecification> expression,
                                         TypeExpression type, Multiplicity multiplicity) {
            this(name, parameters, new Realization.Inline(expression), type, multiplicity);
        }

        /**
         * The inline body (sugar form). Valid only when the realization is an
         * {@link Realization.Inline}; a Door-4 function-ref binding has no inline
         * body (its realizing function is the bound FQN).
         */
        public List<ValueSpecification> expression() {
            if (realization instanceof Realization.Inline inl) return inl.body();
            throw new IllegalStateException(
                    "derived property '" + name + "' is a function-ref binding, not an inline body");
        }
    }

    /**
     * A parameter declaration on a derived property or function.
     *
     * @param name         parameter name
     * @param type         parameter type
     * @param multiplicity declared multiplicity (concrete or parameter ref)
     */
    public record ParameterDefinition(
            String name,
            TypeExpression type,
            Multiplicity multiplicity) {
        public ParameterDefinition {
            Objects.requireNonNull(name, "Parameter name cannot be null");
            Objects.requireNonNull(type, "Parameter type cannot be null");
            Objects.requireNonNull(multiplicity, "Parameter multiplicity cannot be null");
        }
    }

    /**
     * A class-level constraint (validation rule). The constraint body is
     * a single Pure expression that must evaluate to a {@code Boolean};
     * {@code ElementParser} parses it eagerly into a {@link ValueSpecification}.
     *
     * @param name       constraint name
     * @param expression parsed expression AST that must evaluate to true
     */
    public record ConstraintDefinition(String name, Realization realization) {
        public ConstraintDefinition {
            Objects.requireNonNull(name, "Constraint name cannot be null");
            Objects.requireNonNull(realization, "Constraint realization cannot be null");
        }

        /** Convenience: the sugar (inline-predicate) form. */
        public ConstraintDefinition(String name, ValueSpecification expression) {
            this(name, new Realization.Inline(List.of(expression)));
        }

        /**
         * The inline predicate (sugar form). Valid only when the realization is
         * an {@link Realization.Inline}; a Door-4 function-ref binding has none.
         */
        public ValueSpecification expression() {
            if (realization instanceof Realization.Inline inl && inl.body().size() == 1) {
                return inl.body().get(0);
            }
            throw new IllegalStateException(
                    "constraint '" + name + "' is a function-ref binding, not an inline predicate");
        }
    }
}
