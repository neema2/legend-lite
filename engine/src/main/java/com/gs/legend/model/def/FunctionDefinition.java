package com.gs.legend.model.def;

import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.PType;
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
 * @param stereotypes        Applied stereotypes
 * @param taggedValues       Applied tagged values
 * @param parsedReturnType   Full parsed PType for the return type; null when not available.
 *                           Used by the compiler for schema-aware return type validation.
 */
public record FunctionDefinition(
        String qualifiedName,
        List<ParameterDefinition> parameters,
        String returnType,
        int returnLowerBound,
        Integer returnUpperBound,
        String body,
        List<StereotypeApplication> stereotypes,
        List<TaggedValue> taggedValues,
        List<ValueSpecification> resolvedBody,
        PType parsedReturnType) implements PackageableElement {

    public FunctionDefinition {
        Objects.requireNonNull(qualifiedName, "Function name cannot be null");
        Objects.requireNonNull(returnType, "Return type cannot be null");
        Objects.requireNonNull(body, "Function body cannot be null");
        if (body.isBlank()) throw new IllegalArgumentException("Function body cannot be empty");
        parameters = parameters == null ? List.of() : List.copyOf(parameters);
        stereotypes = stereotypes == null ? List.of() : List.copyOf(stereotypes);
        taggedValues = taggedValues == null ? List.of() : List.copyOf(taggedValues);
        // resolvedBody is null until PureModelBuilder Phase 6 pre-resolves it
    }

    /**
     * Constructor without stereotypes/tagged values.
     */
    public FunctionDefinition(String qualifiedName, List<ParameterDefinition> parameters,
            String returnType, int returnLowerBound, Integer returnUpperBound, String body) {
        this(qualifiedName, parameters, returnType, returnLowerBound, returnUpperBound, body, List.of(), List.of(), null, null);
    }

    public FunctionDefinition(String qualifiedName, List<ParameterDefinition> parameters,
            String returnType, int returnLowerBound, Integer returnUpperBound, String body,
            List<StereotypeApplication> stereotypes, List<TaggedValue> taggedValues) {
        this(qualifiedName, parameters, returnType, returnLowerBound, returnUpperBound, body,
                stereotypes, taggedValues, null, null);
    }

    /**
     * Returns a copy with the pre-resolved body AST.
     */
    public FunctionDefinition withResolvedBody(List<ValueSpecification> resolved) {
        return new FunctionDefinition(qualifiedName, parameters, returnType,
                returnLowerBound, returnUpperBound, body, stereotypes, taggedValues, resolved, parsedReturnType);
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
     * @param name         Parameter name
     * @param type         Parameter type (simple name or FQN)
     * @param lowerBound   Lower multiplicity bound
     * @param upperBound   Upper multiplicity bound (null for *)
     * @param functionType Structured function type for Function<{...}> params;
     *                     null for non-function params (String, Integer, etc.)
     * @param parsedType   Full parsed PType from the grammar; null when not available.
     *                     Used by the compiler to resolve parameterized types like
     *                     Relation<(col:Type)> to Type for schema validation.
     */
    public record ParameterDefinition(
            String name,
            String type,
            int lowerBound,
            Integer upperBound,
            PType.FunctionType functionType,
            PType parsedType) {
        public ParameterDefinition {
            Objects.requireNonNull(name, "Parameter name cannot be null");
            Objects.requireNonNull(type, "Parameter type cannot be null");
        }

        public ParameterDefinition(String name, String type, int lowerBound, Integer upperBound,
                                    PType.FunctionType functionType) {
            this(name, type, lowerBound, upperBound, functionType, null);
        }

        public ParameterDefinition(String name, String type, int lowerBound, Integer upperBound) {
            this(name, type, lowerBound, upperBound, null, null);
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
            return type + "[" + FunctionDefinition.multiplicityString(lowerBound, upperBound) + "]";
        }
    }
}
