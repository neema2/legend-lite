package com.gs.legend.sqlgen;
import com.gs.legend.plan.*;
import com.gs.legend.model.m3.*;
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
        // SQLite uses double quotes for identifiers (or backticks, but double quotes
        // are more standard)
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

    @Override
    public String renderStructLiteral(java.util.LinkedHashMap<String, String> fields) {
        throw new UnsupportedOperationException("SQLite does not support struct types");
    }

    @Override
    public String renderArrayLiteral(java.util.List<String> elements) {
        throw new UnsupportedOperationException("SQLite does not support array types");
    }

    @Override
    public String renderUnnestExpression(String arrayPath) {
        throw new UnsupportedOperationException("SQLite does not support UNNEST");
    }

    @Override
    public String renderListContains(String listExpr, String elemExpr) {
        // SQLite: use IN or json_each for list membership
        throw new UnsupportedOperationException("SQLite does not support list contains");
    }

    @Override
    public String sqlTypeName(String pureTypeName) {
        if (pureTypeName.contains("(")) return pureTypeName.toUpperCase();
        return switch (pureTypeName) {
            case "String" -> "TEXT";
            case "Integer" -> "INTEGER";
            case "Float", "Decimal" -> "REAL";
            case "Boolean" -> "INTEGER"; // SQLite has no boolean type
            case "Date", "StrictDate", "DateTime" -> "TEXT"; // SQLite stores dates as text
            default -> throw new IllegalArgumentException(
                    "SQLite: unmapped Pure type name '" + pureTypeName + "' in sqlTypeName");
        };
    }

    @Override
    public String renderDateAdd(String dateExpr, String amount, String unit) {
        // SQLite uses datetime(date, '+N unit')
        throw new UnsupportedOperationException("SQLite date arithmetic not yet implemented");
    }

    @Override
    public String renderStartsWith(String str, String prefix) {
        return str + " LIKE " + prefix + " || '%'";
    }

    @Override
    public String renderEndsWith(String str, String suffix) {
        return str + " LIKE '%' || " + suffix;
    }

    @Override
    public String renderStarExcept(java.util.List<String> columns) {
        throw new UnsupportedOperationException("SQLite does not support SELECT * EXCLUDE/EXCEPT");
    }

    // ==================== Variant (not supported) ====================

    @Override
    public String renderVariantLiteral(String expr) {
        throw new UnsupportedOperationException("SQLite does not support Variant types");
    }

    @Override
    public String renderVariantAccess(String expr, String key) {
        throw new UnsupportedOperationException("SQLite does not support Variant types");
    }

    @Override
    public String renderVariantIndex(String expr, int index) {
        throw new UnsupportedOperationException("SQLite does not support Variant types");
    }

    @Override
    public String renderVariantTextAccess(String expr, String key) {
        throw new UnsupportedOperationException("SQLite does not support Variant types");
    }

    @Override
    public String renderToVariant(String expr) {
        throw new UnsupportedOperationException("SQLite does not support Variant types");
    }

    @Override
    public String renderVariantArrayCast(String expr, String sqlType) {
        throw new UnsupportedOperationException("SQLite does not support Variant types");
    }

    @Override
    public String renderVariantScalarCast(String expr, String sqlType) {
        throw new UnsupportedOperationException("SQLite does not support Variant types");
    }

    @Override
    public String renderVariantCast(String expr) {
        throw new UnsupportedOperationException("SQLite does not support Variant types");
    }
}
