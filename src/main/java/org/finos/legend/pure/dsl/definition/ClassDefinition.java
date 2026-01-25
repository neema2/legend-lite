package org.finos.legend.pure.dsl.definition;

import java.util.List;
import java.util.Objects;

/**
 * Represents a Pure Class definition.
 * 
 * Pure syntax:
 * 
 * <pre>
 * Class package::ClassName
 * {
 *     propertyName: Type[multiplicity];
 *     derivedName() {$this.prop1 + $this.prop2}: Type[multiplicity];
 * }
 * </pre>
 * 
 * Example:
 * 
 * <pre>
 * Class model::Person
 * {
 *     firstName: String[1];
 *     lastName: String[1];
 *     age: Integer[1];
 *     fullName() {$this.firstName + ' ' + $this.lastName}: String[1];
 * }
 * </pre>
 * 
 * @param qualifiedName     The fully qualified class name (e.g.,
 *                          "model::Person")
 * @param properties        The list of regular property definitions
 * @param derivedProperties The list of derived (computed) property definitions
 */
public record ClassDefinition(
        String qualifiedName,
        List<String> superClasses,
        List<PropertyDefinition> properties,
        List<DerivedPropertyDefinition> derivedProperties,
        List<ConstraintDefinition> constraints,
        List<StereotypeApplication> stereotypes,
        List<TaggedValue> taggedValues) implements PureDefinition {

    public ClassDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        Objects.requireNonNull(properties, "Properties cannot be null");
        superClasses = superClasses == null ? List.of() : List.copyOf(superClasses);
        properties = List.copyOf(properties);
        derivedProperties = derivedProperties == null ? List.of() : List.copyOf(derivedProperties);
        constraints = constraints == null ? List.of() : List.copyOf(constraints);
        stereotypes = stereotypes == null ? List.of() : List.copyOf(stereotypes);
        taggedValues = taggedValues == null ? List.of() : List.copyOf(taggedValues);
    }

    /**
     * Constructor for backwards compatibility (no superclasses, derived properties,
     * constraints, or annotations).
     */
    public ClassDefinition(String qualifiedName, List<PropertyDefinition> properties) {
        this(qualifiedName, List.of(), properties, List.of(), List.of(), List.of(), List.of());
    }

    /**
     * Constructor with derived properties but no superclasses, constraints or
     * annotations.
     */
    public ClassDefinition(String qualifiedName, List<PropertyDefinition> properties,
            List<DerivedPropertyDefinition> derivedProperties) {
        this(qualifiedName, List.of(), properties, derivedProperties, List.of(), List.of(), List.of());
    }

    /**
     * Constructor with constraints but no superclasses or annotations (backwards
     * compat).
     */
    public ClassDefinition(String qualifiedName, List<PropertyDefinition> properties,
            List<DerivedPropertyDefinition> derivedProperties, List<ConstraintDefinition> constraints) {
        this(qualifiedName, List.of(), properties, derivedProperties, constraints, List.of(), List.of());
    }

    /**
     * @return The simple class name (without package)
     */
    public String simpleName() {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(idx + 2) : qualifiedName;
    }

    /**
     * @return The package path (without class name)
     */
    public String packagePath() {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(0, idx) : "";
    }

    /**
     * Represents a regular property definition within a class.
     * 
     * @param name       The property name
     * @param type       The property type (e.g., "String", "Integer")
     * @param lowerBound Lower multiplicity bound
     * @param upperBound Upper multiplicity bound (null for *)
     */
    public record PropertyDefinition(
            String name,
            String type,
            int lowerBound,
            Integer upperBound) {
        public PropertyDefinition {
            Objects.requireNonNull(name, "Property name cannot be null");
            Objects.requireNonNull(type, "Property type cannot be null");
        }

        /**
         * Creates a required [1] property.
         */
        public static PropertyDefinition required(String name, String type) {
            return new PropertyDefinition(name, type, 1, 1);
        }

        /**
         * Creates an optional [0..1] property.
         */
        public static PropertyDefinition optional(String name, String type) {
            return new PropertyDefinition(name, type, 0, 1);
        }

        /**
         * Creates a many [*] property.
         */
        public static PropertyDefinition many(String name, String type) {
            return new PropertyDefinition(name, type, 0, null);
        }

        public String multiplicityString() {
            if (upperBound == null) {
                return lowerBound == 0 ? "*" : lowerBound + "..*";
            }
            if (lowerBound == upperBound) {
                return String.valueOf(lowerBound);
            }
            return lowerBound + ".." + upperBound;
        }
    }

    /**
     * Represents a derived (computed) property within a class.
     * 
     * Pure syntax: name() {expression}: Type[multiplicity]
     * 
     * @param name       The property name (without parentheses)
     * @param expression The Pure expression that computes the value (e.g.,
     *                   "$this.firstName + ' ' + $this.lastName")
     * @param type       The return type
     * @param lowerBound Lower multiplicity bound
     * @param upperBound Upper multiplicity bound (null for *)
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
            Objects.requireNonNull(expression, "Derived property expression cannot be null");
            Objects.requireNonNull(type, "Derived property type cannot be null");
            if (parameters == null) {
                parameters = List.of();
            }
        }

        /**
         * Creates a derived property with [1] multiplicity and no parameters.
         */
        public static DerivedPropertyDefinition of(String name, String expression, String type) {
            return new DerivedPropertyDefinition(name, List.of(), expression, type, 1, 1);
        }

        /**
         * Creates a derived property with parameters.
         */
        public static DerivedPropertyDefinition withParams(String name, List<ParameterDefinition> params,
                String expression, String type, int lowerBound, Integer upperBound) {
            return new DerivedPropertyDefinition(name, params, expression, type, lowerBound, upperBound);
        }

        public String multiplicityString() {
            if (upperBound == null) {
                return lowerBound == 0 ? "*" : lowerBound + "..*";
            }
            if (lowerBound == upperBound) {
                return String.valueOf(lowerBound);
            }
            return lowerBound + ".." + upperBound;
        }

        /**
         * @return true if this derived property has parameters
         */
        public boolean hasParameters() {
            return parameters != null && !parameters.isEmpty();
        }
    }

    /**
     * Represents a parameter in a derived property or function.
     * 
     * Pure syntax: paramName: Type[multiplicity]
     * Example: firmByName(name: String[1]) {...}
     * 
     * @param name       The parameter name
     * @param type       The parameter type
     * @param lowerBound Lower multiplicity bound
     * @param upperBound Upper multiplicity bound (null for *)
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

        /**
         * Creates a parameter with [1] multiplicity.
         */
        public static ParameterDefinition of(String name, String type) {
            return new ParameterDefinition(name, type, 1, 1);
        }

        public String multiplicityString() {
            if (upperBound == null) {
                return lowerBound == 0 ? "*" : lowerBound + "..*";
            }
            if (lowerBound == upperBound) {
                return String.valueOf(lowerBound);
            }
            return lowerBound + ".." + upperBound;
        }
    }

    /**
     * Represents a constraint (validation rule) on a class.
     * 
     * Pure syntax: constraintName: $this.property > 0
     * 
     * @param name       The constraint name (e.g., "validAge")
     * @param expression The Pure expression that must evaluate to true
     */
    public record ConstraintDefinition(
            String name,
            String expression) {
        public ConstraintDefinition {
            Objects.requireNonNull(name, "Constraint name cannot be null");
            Objects.requireNonNull(expression, "Constraint expression cannot be null");
        }

        /**
         * Factory method for creating a constraint.
         */
        public static ConstraintDefinition of(String name, String expression) {
            return new ConstraintDefinition(name, expression);
        }
    }
}
