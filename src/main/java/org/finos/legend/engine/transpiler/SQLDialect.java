package org.finos.legend.engine.transpiler;

/**
 * Interface defining SQL dialect-specific behavior.
 * Implementations handle differences between database engines.
 */
public interface SQLDialect {
    
    /**
     * @return The dialect name (e.g., "DuckDB", "SQLite")
     */
    String name();
    
    /**
     * Quote an identifier (table name, column name, alias).
     * 
     * @param identifier The identifier to quote
     * @return The quoted identifier
     */
    String quoteIdentifier(String identifier);
    
    /**
     * Quote a string literal value.
     * 
     * @param value The string value to quote
     * @return The quoted string literal
     */
    String quoteStringLiteral(String value);
    
    /**
     * Format a boolean literal.
     * 
     * @param value The boolean value
     * @return The SQL boolean representation
     */
    String formatBoolean(boolean value);
    
    /**
     * Format a NULL literal.
     * 
     * @return The SQL NULL representation
     */
    default String formatNull() {
        return "NULL";
    }
}
