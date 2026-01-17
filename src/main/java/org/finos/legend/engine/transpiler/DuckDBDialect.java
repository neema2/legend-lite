package org.finos.legend.engine.transpiler;

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
}
