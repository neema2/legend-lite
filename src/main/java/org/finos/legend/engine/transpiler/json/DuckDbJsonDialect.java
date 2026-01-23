package org.finos.legend.engine.transpiler.json;

/**
 * DuckDB implementation of JSON SQL dialect.
 * 
 * DuckDB JSON functions:
 * - json_object('key1', val1, 'key2', val2, ...)
 * - json_group_array(expr) - Aggregates into JSON array
 */
public final class DuckDbJsonDialect implements JsonSqlDialect {

    public static final DuckDbJsonDialect INSTANCE = new DuckDbJsonDialect();

    private DuckDbJsonDialect() {
    }

    @Override
    public String jsonObjectFunction() {
        return "json_object";
    }

    @Override
    public String jsonArrayAggFunction() {
        return "json_group_array";
    }

    @Override
    public String variantFromJson(String expr) {
        return "CAST(" + expr + " AS JSON)";
    }

    @Override
    public String variantToJson(String expr) {
        return "CAST(" + expr + " AS VARCHAR)";
    }

    @Override
    public String variantGet(String expr, String key) {
        // DuckDB uses ->> operator for text extraction
        return "(" + expr + ")->>" + "'" + key + "'";
    }

    @Override
    public String variantGetIndex(String expr, int index) {
        // DuckDB uses [n] for array indexing
        return "(" + expr + ")[" + index + "]";
    }
}
