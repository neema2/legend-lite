package org.finos.legend.engine.transpiler;

/**
 * SQL dialect implementation for SQLite.
 * SQLite uses double quotes for identifiers and single quotes for strings.
 */
public final class SQLiteDialect implements SQLDialect {
    
    public static final SQLiteDialect INSTANCE = new SQLiteDialect();
    
    private SQLiteDialect() {
        // Singleton
    }
    
    @Override
    public String name() {
        return "SQLite";
    }
    
    @Override
    public String quoteIdentifier(String identifier) {
        // SQLite uses double quotes for identifiers (or backticks, but double quotes are more standard)
        // Escape any existing double quotes by doubling them
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
    
    @Override
    public String quoteStringLiteral(String value) {
        // SQLite uses single quotes for string literals
        // Escape any existing single quotes by doubling them
        return "'" + value.replace("'", "''") + "'";
    }
    
    @Override
    public String formatBoolean(boolean value) {
        // SQLite doesn't have native boolean, uses 1/0
        return value ? "1" : "0";
    }
}
