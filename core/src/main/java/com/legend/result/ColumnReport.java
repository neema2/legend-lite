package com.legend.result;

import java.util.List;

/**
 * THE DATABASE'S REPORT OF ONE RESULT COLUMN, as JDBC gives it ({@code ResultSetMetaData}): its label, the driver's
 * own type name, its JDBC type code (JDBC's {@code Types}), its precision and its scale. legend-engine types its
 * {@code ARROW_IPC} answer's columns from these alone ({@code RelationalResultToArrowIPCSerializer.buildSchema}:
 * {@link EngineArrowWriter}; docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md R4 step 3, D17, D19).
 *
 * <p>A host has no JDBC driver: its database's own type names (duckdb-python's {@code description}, DuckDB-WASM's
 * {@code DESCRIBE}) are read as DuckDB's JDBC driver reports them ({@link #duckDb}; each spelling measured against
 * {@code duckdb_jdbc} 1.5.6.0, 2026-11-03, the last six by R4's audit), so a host's answer is the JVM's.
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

    /**
     * A column DuckDB names {@code type} (its own spelling: {@code INTEGER}, {@code DECIMAL(18,2)},
     * {@code TIMESTAMP WITH TIME ZONE}, {@code INTEGER[]}, {@code STRUCT(a INTEGER)}), labelled {@code label}, as
     * DuckDB's JDBC driver reports it: a {@code HUGEINT} (a sum of integers) {@code OTHER}; {@code UTINYINT}
     * {@code SMALLINT}, {@code USMALLINT} {@code INTEGER}, {@code UINTEGER} {@code BIGINT}; {@code UBIGINT},
     * {@code UHUGEINT}, {@code INTERVAL}, {@code UUID}, {@code JSON}, a map and an enumeration {@code OTHER}; every
     * {@code TIMESTAMP_*} {@code TIMESTAMP}; a list {@code ARRAY}, a struct {@code STRUCT}; {@code BIGNUM},
     * {@code VARIANT} and {@code TYPE} {@code OTHER}, {@code BIT} {@code BIT}, {@code GEOMETRY} {@code BLOB},
     * {@code TIME_NS} {@code TIME}. A name not measured is refused by name, never guessed.
     */
    public static ColumnReport duckDb(String label, String type) {
        String t = type.strip();
        if (t.endsWith("]")) {
            return new ColumnReport(label, t, ARRAY, 0, 0);
        }
        if (t.startsWith("STRUCT(")) {
            return new ColumnReport(label, t, STRUCT, 0, 0);
        }
        if (t.startsWith("MAP(") || t.startsWith("ENUM(") || t.startsWith("UNION(")) {
            return new ColumnReport(label, t, OTHER, 0, 0);
        }
        if (t.startsWith("DECIMAL(")) {
            int comma = t.indexOf(',');
            int close = t.indexOf(')');
            if (comma < 0 || close < comma) {
                throw new IllegalArgumentException("DuckDB's type '" + t + "' names no precision and scale");
            }
            return new ColumnReport(label, "DECIMAL", DECIMAL, Integer.parseInt(t.substring(8, comma).strip()),
                    Integer.parseInt(t.substring(comma + 1, close).strip()));
        }
        int code = switch (t) {
            case "BOOLEAN" -> BOOLEAN;
            case "TINYINT" -> TINYINT;
            case "SMALLINT", "UTINYINT" -> SMALLINT;
            case "INTEGER", "USMALLINT" -> INTEGER;
            case "BIGINT", "UINTEGER" -> BIGINT;
            case "HUGEINT", "UBIGINT", "UHUGEINT", "INTERVAL", "UUID", "JSON", "BIGNUM", "VARIANT", "TYPE" -> OTHER;
            case "BIT" -> BIT;
            case "FLOAT" -> FLOAT;
            case "DOUBLE" -> DOUBLE;
            case "VARCHAR" -> VARCHAR;
            case "BLOB", "GEOMETRY" -> BLOB;
            case "DATE" -> DATE;
            case "TIME", "TIME_NS" -> TIME;
            case "TIME WITH TIME ZONE" -> TIME_WITH_TIMEZONE;
            case "TIMESTAMP", "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS" -> TIMESTAMP;
            case "TIMESTAMP WITH TIME ZONE" -> TIMESTAMP_WITH_TIMEZONE;
            default -> throw new IllegalArgumentException("DuckDB's type '" + t + "' has no measured JDBC report:"
                    + " legend-engine's Arrow answer is typed from it");
        };
        return new ColumnReport(label, t, code, 0, 0);
    }

    /** Each of {@code labels} with DuckDB's type of the same place ({@link #duckDb}). */
    public static List<ColumnReport> duckDb(List<String> labels, List<String> types) {
        if (labels.size() != types.size()) {
            throw new IllegalArgumentException(labels.size() + " columns reported with " + types.size() + " types");
        }
        List<ColumnReport> out = new java.util.ArrayList<>(labels.size());
        for (int i = 0; i < labels.size(); i++) {
            out.add(duckDb(labels.get(i), types.get(i)));
        }
        return out;
    }
}
