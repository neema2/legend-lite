package org.finos.legend.engine.transpiler;

import org.finos.legend.engine.transpiler.json.DuckDbJsonDialect;
import org.finos.legend.engine.transpiler.json.JsonSqlDialect;

/**
 * SQL dialect implementation for DuckDB.
 * DuckDB uses double quotes for identifiers and single quotes for strings.
 */
public final class DuckDBDialect implements SQLDialect {

    public static final DuckDBDialect INSTANCE = new DuckDBDialect();

    private DuckDBDialect() {
        // Singleton
    }

    @Override
    public String name() {
        return "DuckDB";
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (identifier == null)
            return "\"_unknown_\"";
        // DuckDB uses double quotes for identifiers
        // Escape any existing double quotes by doubling them
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public String quoteStringLiteral(String value) {
        // DuckDB uses single quotes for string literals
        // Escape any existing single quotes by doubling them
        return "'" + value.replace("'", "''") + "'";
    }

    @Override
    public String formatBoolean(boolean value) {
        // DuckDB supports TRUE/FALSE literals
        return value ? "TRUE" : "FALSE";
    }

    @Override
    public JsonSqlDialect getJsonDialect() {
        return DuckDbJsonDialect.INSTANCE;
    }

    @Override
    public String renderStructLiteral(java.util.LinkedHashMap<String, String> fields) {
        // DuckDB struct syntax: {'name': 'ok', 'age': 30}
        return "{" + fields.entrySet().stream()
                .map(e -> "'" + e.getKey() + "': " + e.getValue())
                .collect(java.util.stream.Collectors.joining(", ")) + "}";
    }

    @Override
    public String renderArrayLiteral(java.util.List<String> elements) {
        // DuckDB array syntax: [1, 2, 3]
        return "[" + String.join(", ", elements) + "]";
    }

    @Override
    public String renderUnnestExpression(String arrayPath) {
        // DuckDB: UNNEST(path)
        return "UNNEST(" + arrayPath + ")";
    }
}
