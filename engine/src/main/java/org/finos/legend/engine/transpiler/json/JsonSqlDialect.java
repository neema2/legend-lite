package org.finos.legend.engine.transpiler.json;

/**
 * Dialect interface for JSON SQL generation.
 * 
 * Different databases have different JSON construction functions:
 * - DuckDB: json_object(), json_group_array()
 * - Snowflake: OBJECT_CONSTRUCT(), ARRAY_AGG()
 * - PostgreSQL: json_build_object(), json_agg()
 * 
 * Implement this interface to support new database dialects.
 */
public interface JsonSqlDialect {

    /**
     * Function name for constructing a JSON object from key-value pairs.
     * Example: json_object('name', expr, 'age', expr2)
     */
    String jsonObjectFunction();

    /**
     * Function name for aggregating values into a JSON array.
     * Example: json_group_array(expr)
     */
    String jsonArrayAggFunction();

    /**
     * Generates a JSON object construction expression.
     * 
     * @param keyValuePairs Alternating key, value expressions (already formatted)
     * @return The SQL expression for JSON object construction
     */
    default String jsonObject(String... keyValuePairs) {
        StringBuilder sb = new StringBuilder(jsonObjectFunction());
        sb.append("(");
        for (int i = 0; i < keyValuePairs.length; i++) {
            if (i > 0)
                sb.append(", ");
            sb.append(keyValuePairs[i]);
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * Generates a JSON array aggregation expression.
     * 
     * @param innerExpression The expression to aggregate
     * @return The SQL expression for array aggregation
     */
    default String jsonArrayAgg(String innerExpression) {
        return jsonArrayAggFunction() + "(" + innerExpression + ")";
    }

    /**
     * Wraps a string value in quotes for use as a JSON key.
     */
    default String quotedKey(String key) {
        return "'" + key + "'";
    }

    // ==================== Variant Navigation Functions ====================

    /**
     * Converts a string expression to JSON/Variant type.
     * Example: CAST(expr AS JSON)
     */
    String variantFromJson(String expr);

    /**
     * Converts a JSON/Variant expression back to string.
     * Example: CAST(expr AS VARCHAR)
     */
    String variantToJson(String expr);

    /**
     * Extracts a value from a JSON object by key.
     * Example: expr->>'key' or json_extract(expr, '$.key')
     */
    String variantGet(String expr, String key);

    /**
     * Extracts a JSON value (preserving structure) from a JSON object by key.
     * Used for arrays/objects that need to be processed by list functions.
     * Example: expr->'key' or json_extract(expr, '$.key')
     */
    String variantGetJson(String expr, String key);

    /**
     * Extracts a value from a JSON array by index.
     * Example: expr[0] or json_extract(expr, '$[0]')
     */
    String variantGetIndex(String expr, int index);
}
