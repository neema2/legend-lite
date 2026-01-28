package org.finos.legend.pure.dsl.definition;

import java.util.List;
import java.util.Objects;

/**
 * Represents a Pure function definition.
 * 
 * Pure syntax (legend-engine compatible):
 * 
 * <pre>
 * function my::package::functionName(param1: String[1], param2: Integer[0..1]): String[1]
 * {
 *     $param1 + ' is ' + $param2->toString()
 * }
 * </pre>
 * 
 * @param qualifiedName    Fully qualified function name (e.g.,
 *                         "my::utils::greet")
 * @param parameters       List of parameter definitions
 * @param returnType       Return type name (e.g., "String", "Integer",
 *                         "Boolean")
 * @param returnLowerBound Lower multiplicity bound for return (e.g., 0 for
 *                         optional)
 * @param returnUpperBound Upper multiplicity bound for return (null for *)
 * @param body             The function body expression
 * @param stereotypes      Applied stereotypes
 * @param taggedValues     Applied tagged values
 */
public record FunctionDefinition(
        String qualifiedName,
        List<ParameterDefinition> parameters,
        String returnType,
        int returnLowerBound,
        Integer returnUpperBound,
        String body,
        List<StereotypeApplication> stereotypes,
        List<TaggedValue> taggedValues) implements PureDefinition {

    public FunctionDefinition {
        Objects.requireNonNull(qualifiedName, "Function name cannot be null");
        Objects.requireNonNull(returnType, "Return type cannot be null");
        Objects.requireNonNull(body, "Function body cannot be null");
        parameters = parameters == null ? List.of() : List.copyOf(parameters);
        stereotypes = stereotypes == null ? List.of() : List.copyOf(stereotypes);
        taggedValues = taggedValues == null ? List.of() : List.copyOf(taggedValues);
    }

    /**
     * Constructor without stereotypes/tagged values.
     */
    public FunctionDefinition(String qualifiedName, List<ParameterDefinition> parameters,
            String returnType, int returnLowerBound, Integer returnUpperBound, String body) {
        this(qualifiedName, parameters, returnType, returnLowerBound, returnUpperBound, body, List.of(), List.of());
    }

    /**
     * @return The simple function name (without package)
     */
    public String simpleName() {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(idx + 2) : qualifiedName;
    }

    /**
     * @return The package path (without function name)
     */
    public String packagePath() {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(0, idx) : "";
    }

    /**
     * @return The return type with multiplicity as string (e.g., "String[1]",
     *         "Integer[*]")
     */
    public String returnTypeWithMultiplicity() {
        return returnType + "[" + multiplicityString(returnLowerBound, returnUpperBound) + "]";
    }

    private static String multiplicityString(int lower, Integer upper) {
        if (upper == null) {
            return lower == 0 ? "*" : lower + "..*";
        }
        if (lower == upper) {
            return String.valueOf(lower);
        }
        return lower + ".." + upper;
    }

    /**
     * Represents a function parameter.
     * 
     * @param name       Parameter name
     * @param type       Parameter type
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
         * Creates a required [1] parameter.
         */
        public static ParameterDefinition required(String name, String type) {
            return new ParameterDefinition(name, type, 1, 1);
        }

        /**
         * Creates an optional [0..1] parameter.
         */
        public static ParameterDefinition optional(String name, String type) {
            return new ParameterDefinition(name, type, 0, 1);
        }

        /**
         * @return Type with multiplicity as string (e.g., "String[1]")
         */
        public String typeWithMultiplicity() {
            return type + "[" + multiplicityString(lowerBound, upperBound) + "]";
        }

        private static String multiplicityString(int lower, Integer upper) {
            if (upper == null) {
                return lower == 0 ? "*" : lower + "..*";
            }
            if (lower == upper) {
                return String.valueOf(lower);
            }
            return lower + ".." + upper;
        }
    }
}
