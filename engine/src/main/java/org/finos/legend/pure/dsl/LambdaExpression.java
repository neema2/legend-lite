package org.finos.legend.pure.dsl;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.finos.legend.engine.plan.GenericType;

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

        /**
         * Parses a tuple/relation type like "(city:String, country:String, 'col:name':Integer)"
         * into a map of column name → GenericType. Returns empty map if not a tuple type.
         * This is the Pure relation type syntax — equivalent to legend-engine's RelationType._columns().
         */
        public Map<String, GenericType> columnTypes() {
            if (typeName == null || !typeName.startsWith("(") || !typeName.endsWith(")")) {
                return Map.of();
            }
            String inner = typeName.substring(1, typeName.length() - 1);
            Map<String, GenericType> result = new LinkedHashMap<>();
            // Split on commas, respecting quoted column names
            int start = 0;
            boolean inQuote = false;
            for (int i = 0; i <= inner.length(); i++) {
                if (i < inner.length() && inner.charAt(i) == '\'') {
                    inQuote = !inQuote;
                } else if ((i == inner.length() || (inner.charAt(i) == ',' && !inQuote))) {
                    String colDef = inner.substring(start, i).trim();
                    parseColumnDef(colDef, result);
                    start = i + 1;
                }
            }
            return result;
        }

        private static void parseColumnDef(String colDef, Map<String, GenericType> result) {
            if (colDef.isEmpty()) return;
            // Find the FIRST ':' outside quotes — separates name from type
            // Using first (not last) because type names can contain '::' (e.g., meta::pure::...::Variant)
            int firstColon = -1;
            boolean q = false;
            for (int i = 0; i < colDef.length(); i++) {
                if (colDef.charAt(i) == '\'') q = !q;
                else if (colDef.charAt(i) == ':' && !q) { firstColon = i; break; }
            }
            if (firstColon <= 0) return;
            String colName = colDef.substring(0, firstColon).trim();
            // Strip surrounding quotes from column name
            if (colName.startsWith("'") && colName.endsWith("'")) {
                colName = colName.substring(1, colName.length() - 1);
            }
            String typePart = colDef.substring(firstColon + 1).trim();
            result.put(colName, GenericType.fromTypeName(typePart));
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
