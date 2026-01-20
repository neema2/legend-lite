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
}
