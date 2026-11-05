package com.legend.result;

/**
 * THE DATABASE'S REPORT OF ONE RESULT COLUMN, as JDBC gives it ({@code ResultSetMetaData}): its label, the driver's
 * own type name, its JDBC type code (JDBC's {@code Types}), its precision and its scale. legend-engine types its
 * {@code ARROW_IPC} answer's columns from these alone ({@code RelationalResultToArrowIPCSerializer.buildSchema}:
 * {@link EngineArrowWriter}; docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md R4 step 3, D17, D19). legend-lite's server
 * alone answers that format (D22, 2026-11-05), so the report is its JDBC session's; the embedded hosts' reading of
 * DuckDB's own type names as its JDBC driver reports them went with their Arrow answer.
 */
public record ColumnReport(String label, String driverType, int jdbcType, int precision, int scale) {

    // JDBC's Types codes (this package names no JDBC type: the result layer is the runtime's, not JDBC's)
    public static final int BIT = -7;
    public static final int TINYINT = -6;
    public static final int SMALLINT = 5;
    public static final int INTEGER = 4;
    public static final int BIGINT = -5;
    public static final int FLOAT = 6;
    public static final int REAL = 7;
    public static final int DOUBLE = 8;
    public static final int NUMERIC = 2;
    public static final int DECIMAL = 3;
    public static final int VARCHAR = 12;
    public static final int DATE = 91;
    public static final int TIME = 92;
    public static final int TIMESTAMP = 93;
    public static final int OTHER = 1111;
    public static final int STRUCT = 2002;
    public static final int ARRAY = 2003;
    public static final int BLOB = 2004;
    public static final int BOOLEAN = 16;
    public static final int TIME_WITH_TIMEZONE = 2013;
    public static final int TIMESTAMP_WITH_TIMEZONE = 2014;
}
