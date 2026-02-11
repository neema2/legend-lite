package org.finos.legend.pure.dsl;

import java.util.List;
import java.util.Objects;

/**
 * Represents a lambda expression with one or more parameters.
 * 
 * Examples:
 * p | $p.lastName == 'Smith' (single param)
 * {l, r | $l.id == $r.personId} (multi-param for joins)
 * a: Integer[1] | $a + 1 (typed param, used in match())
 * 
 * @param parameters     The lambda parameter names (e.g., ["p"] or ["l", "r"])
 * @param parameterTypes Optional type annotations parallel to parameters (null entries = untyped)
 * @param body           The lambda body expression
 */
public record LambdaExpression(
        List<String> parameters,
        List<TypeAnnotation> parameterTypes,
        PureExpression body) implements PureExpression {

    /**
     * Type annotation on a lambda parameter: name: Type[multiplicity]
     */
    public record TypeAnnotation(String typeName, String multiplicity) {
        public String simpleTypeName() {
            int lastSep = typeName.lastIndexOf("::");
            return lastSep >= 0 ? typeName.substring(lastSep + 2) : typeName;
        }
    }

    public LambdaExpression {
        Objects.requireNonNull(parameters, "Parameters cannot be null");
        if (parameters.isEmpty()) {
            throw new IllegalArgumentException("Lambda must have at least one parameter");
        }
        Objects.requireNonNull(body, "Body cannot be null");
        // Make immutable copy
        parameters = List.copyOf(parameters);
        parameterTypes = parameterTypes != null ? java.util.Collections.unmodifiableList(new java.util.ArrayList<>(parameterTypes)) : null;
    }

    /**
     * Convenience constructor for untyped parameters.
     */
    public LambdaExpression(List<String> parameters, PureExpression body) {
        this(parameters, null, body);
    }

    /**
     * Convenience constructor for single-parameter lambdas.
     */
    public LambdaExpression(String parameter, PureExpression body) {
        this(List.of(parameter), null, body);
    }

    /**
     * Get the first (or only) parameter name.
     * For backward compatibility with code expecting single-parameter lambdas.
     */
    public String parameter() {
        return parameters.getFirst();
    }

    /**
     * Check if this is a multi-parameter lambda.
     */
    public boolean isMultiParam() {
        return parameters.size() > 1;
    }
}
