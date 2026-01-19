package org.finos.legend.engine.execution;

/**
 * Column metadata for query results.
 * 
 * GraalVM native-image compatible.
 */
public record Column(String name, String sqlType, String javaType) {

    /**
     * Maps JDBC type code to Java type name.
     */
    public static String mapJdbcTypeToJava(int jdbcType) {
        return switch (jdbcType) {
            case java.sql.Types.VARCHAR, java.sql.Types.CHAR, java.sql.Types.LONGVARCHAR -> "String";
            case java.sql.Types.INTEGER, java.sql.Types.SMALLINT, java.sql.Types.TINYINT -> "Integer";
            case java.sql.Types.BIGINT -> "Long";
            case java.sql.Types.DOUBLE, java.sql.Types.FLOAT, java.sql.Types.REAL -> "Double";
            case java.sql.Types.DECIMAL, java.sql.Types.NUMERIC -> "BigDecimal";
            case java.sql.Types.BOOLEAN, java.sql.Types.BIT -> "Boolean";
            case java.sql.Types.DATE -> "LocalDate";
            case java.sql.Types.TIMESTAMP, java.sql.Types.TIMESTAMP_WITH_TIMEZONE -> "LocalDateTime";
            default -> "Object";
        };
    }
}
