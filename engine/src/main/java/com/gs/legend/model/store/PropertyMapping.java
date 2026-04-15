package com.gs.legend.model.store;

import com.gs.legend.ast.ValueSpecification;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Maps a Pure property to a relational column or expression.
 * 
 * Supports four modes:
 * 1. Simple column mapping: propertyName -> columnName
 * 2. Expression mapping: propertyName -> expression (e.g.,
 * PAYLOAD->get('price', @Integer))
 * 3. Enum column mapping: propertyName -> CASE WHEN column=val1 THEN enum1 ...
 * 4. Join chain mapping: propertyName -> @J1 > @J2 | T.COL (column on remote table
 *    reachable through joins). Future: >> enforces 1-to-1 via scalar subquery.
 * 
 * @param propertyName     The Pure property name (e.g., "price")
 * @param columnName       The relational column name (e.g., "PRICE" or
 *                         "PAYLOAD")
 * @param expressionString The full expression (null for simple column mappings)
 * @param enumMapping      Map from enum value name to list of db values (null
 *                         if not enum)
 * @param enumType         The enum type name (null if not enum)
 * @param joinChain        Ordered list of resolved Join objects to traverse (null for local column mappings).
 *                         When present, columnName is the terminal column on the final joined table.
 * @param dynaExpression   Pre-compiled DynaFunction expression (null for simple column/expression mappings).
 *                         When present, the expression is a ValueSpecification AST tree that PlanGenerator
 *                         evaluates via generateScalar instead of simple column lookup.
 */
public record PropertyMapping(
        String propertyName,
        String columnName,
        String expressionString,
        Map<String, List<Object>> enumMapping,
        String enumType,
        List<Join> joinChain,
        ValueSpecification dynaExpression,
        List<List<Join>> multiJoinChains) {
    public PropertyMapping {
        Objects.requireNonNull(propertyName, "Property name cannot be null");
        Objects.requireNonNull(columnName, "Column name cannot be null");

        if (propertyName.isBlank()) {
            throw new IllegalArgumentException("Property name cannot be blank");
        }
        if (columnName.isBlank()) {
            throw new IllegalArgumentException("Column name cannot be blank");
        }
        if (joinChain != null) {
            joinChain = List.copyOf(joinChain);
        }
        if (multiJoinChains != null) {
            multiJoinChains = multiJoinChains.stream().map(List::copyOf).toList();
        }
    }

    /**
     * Creates a simple column mapping.
     */
    public PropertyMapping(String propertyName, String columnName) {
        this(propertyName, columnName, null, null, null, null, null, null);
    }

    /**
     * Creates a simple column mapping.
     */
    public static PropertyMapping column(String propertyName, String columnName) {
        return new PropertyMapping(propertyName, columnName, null, null, null, null, null, null);
    }

    /**
     * Creates an expression-based mapping.
     */
    public static PropertyMapping expression(String propertyName, String columnName, String expression) {
        return new PropertyMapping(propertyName, columnName, expression, null, null, null, null, null);
    }

    /**
     * Creates an enum column mapping with db-to-enum value translations.
     * 
     * @param propertyName The property name
     * @param columnName   The column containing database values
     * @param enumType     The target enum type name
     * @param enumMapping  Map from enum value name to list of source db values
     */
    public static PropertyMapping enumColumn(String propertyName, String columnName,
            String enumType, Map<String, List<Object>> enumMapping) {
        return new PropertyMapping(propertyName, columnName, null, Map.copyOf(enumMapping), enumType, null, null, null);
    }

    /**
     * Creates a join chain mapping: property value comes from a column on a remote table
     * reachable through the given join chain.
     * Pure syntax: {@code prop: [DB]@J1 > @J2 | T.COL}
     *
     * @param propertyName The property name
     * @param columnName   The terminal column name on the final joined table
     * @param joins        Ordered list of resolved Join objects to traverse
     */
    public static PropertyMapping joinChain(String propertyName, String columnName,
            List<Join> joins) {
        return new PropertyMapping(propertyName, columnName, null, null, null, joins, null, null);
    }

    /**
     * Creates a DynaFunction expression mapping: property value computed from a pre-compiled expression.
     * Pure syntax: {@code prop: concat([DB] T.FIRST, ' ', [DB] T.LAST)}
     *
     * @param propertyName The property name
     * @param expr         Pre-compiled ValueSpecification expression tree
     */
    public static PropertyMapping dynaFunction(String propertyName, ValueSpecification expr) {
        return new PropertyMapping(propertyName, propertyName, null, null, null, null, expr, null);
    }

    /**
     * Creates a combined DynaFunction + join chain mapping: property value computed from
     * an expression that references columns on a remote table reachable through joins.
     * Pure syntax: {@code prop: concat([DB] EMP.NAME, ' - ', @EmpDept | [DB] DEPT.DEPT_NAME)}
     *
     * @param propertyName The property name
     * @param expr         Pre-compiled ValueSpecification expression tree (uses $src/$tgt references)
     * @param joins        Ordered list of resolved Join objects to traverse
     */
    public static PropertyMapping dynaFunctionWithJoin(String propertyName, ValueSpecification expr,
            List<Join> joins) {
        return new PropertyMapping(propertyName, propertyName, null, null, null, joins, expr, null);
    }

    /**
     * Creates a multi-join DynaFunction mapping: expression references columns across multiple
     * joined tables. Each element of joinChains is a separate join path from the main table.
     * Pure syntax: {@code prop: concat(@J1|T1.COL, ' ', @J2|T2.COL)}
     */
    public static PropertyMapping dynaFunctionWithMultiJoin(String propertyName,
            ValueSpecification expr, List<List<Join>> joinChains) {
        return new PropertyMapping(propertyName, propertyName, null, null, null,
                null, expr, List.copyOf(joinChains));
    }

    /**
     * @return true if this property uses multiple join chains (multi-traverse)
     */
    public boolean hasMultiJoinChains() {
        return multiJoinChains != null && !multiJoinChains.isEmpty();
    }

    /**
     * @return true if this property uses an expression mapping
     */
    public boolean hasExpression() {
        return expressionString != null;
    }

    /**
     * @return true if this property uses an enumeration mapping
     */
    public boolean hasEnumMapping() {
        return enumMapping != null && !enumMapping.isEmpty();
    }

    /**
     * @return true if this property uses a join chain mapping
     */
    public boolean hasJoinChain() {
        return joinChain != null && !joinChain.isEmpty();
    }

    /**
     * @return true if this property uses a DynaFunction expression mapping
     */
    public boolean hasDynaExpression() {
        return dynaExpression != null;
    }

    /**
     * @return Optional containing the expression string, if present
     */
    public Optional<String> getExpressionString() {
        return Optional.ofNullable(expressionString);
    }

    /**
     * Pre-parsed expression access (e.g., ->get('price', @Integer)).
     * Parsed at construction time so consumers don't need regex.
     *
     * @param jsonKey  The JSON key to access (e.g., "price")
     * @param castType The Pure type to cast to (nullable, e.g., "Integer")
     */
    public record ExpressionAccess(String jsonKey, String castType) {
    }

    private static final Pattern GET_PATTERN = Pattern.compile("->get\\('(\\w+)'(?:,\\s*@(\\w+))?\\)");

    /**
     * Parses the expression string into a structured ExpressionAccess.
     * Returns empty if no expression or if the pattern doesn't match.
     */
    public Optional<ExpressionAccess> expressionAccess() {
        if (expressionString == null)
            return Optional.empty();
        Matcher m = GET_PATTERN.matcher(expressionString);
        if (m.find()) {
            return Optional.of(new ExpressionAccess(m.group(1), m.group(2)));
        }
        return Optional.empty();
    }

    @Override
    public String toString() {
        if (enumMapping != null) {
            return propertyName + " -> CASE " + columnName + " [enum]";
        }
        if (expressionString != null) {
            return propertyName + " -> " + expressionString;
        }
        return propertyName + " -> " + columnName;
    }
}
