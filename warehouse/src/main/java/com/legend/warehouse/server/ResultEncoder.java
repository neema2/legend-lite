package com.legend.warehouse.server;

import com.legend.Nullable;
import com.legend.server.Json;
import com.legend.warehouse.sqlapi.SqlApi.Column;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;

/**
 * A JDBC result as the API's columns and JSON values
 * (docs/WAREHOUSE_W1_DESIGN_2026_09_26.md §3).
 *
 * <p>THE TYPE LIST IS CLOSED. Each DuckDB type maps to exactly one API
 * type and one JSON spelling; a type outside the list is refused with its
 * name, never guessed at. 64-bit and wider integers and decimals travel as
 * STRINGS, so a JavaScript client loses no precision; dates and times are
 * ISO-8601.
 */
final class ResultEncoder {

    /** How one column's values are read and written. */
    enum Kind { BOOLEAN, SMALL_INT, WIDE_INT, BIG_INT, DECIMAL, FLOATING, TEXT, BLOB, DATE, TIME, TIMESTAMP, TIMESTAMP_TZ }

    record Col(String name, String apiType, Kind kind) {
    }

    /** A type the API does not carry. */
    static final class Unsupported extends SQLException {
        Unsupported(String column, String type) {
            super("column '" + column + "' has type " + type + ", which the API does not carry yet");
        }
    }

    private ResultEncoder() {
    }

    static List<Col> columns(ResultSetMetaData md) throws SQLException {
        List<Col> out = new ArrayList<>();
        for (int i = 1; i <= md.getColumnCount(); i++) {
            String name = md.getColumnLabel(i);
            String type = md.getColumnTypeName(i).toUpperCase(Locale.ROOT);
            Kind kind = kindOf(type);
            if (kind == null) throw new Unsupported(name, type);
            out.add(new Col(name, apiType(type, kind), kind));
        }
        return out;
    }

    static List<Column> api(List<Col> cols) {
        List<Column> out = new ArrayList<>(cols.size());
        for (Col c : cols) out.add(new Column(c.name(), c.apiType(), true));
        return out;
    }

    private static @Nullable Kind kindOf(String t) {
        if (t.startsWith("DECIMAL")) return Kind.DECIMAL;
        return switch (t) {
            case "BOOLEAN" -> Kind.BOOLEAN;
            case "TINYINT", "SMALLINT", "INTEGER", "UTINYINT", "USMALLINT", "UINTEGER" -> Kind.SMALL_INT;
            case "BIGINT" -> Kind.WIDE_INT;
            case "UBIGINT", "HUGEINT", "UHUGEINT" -> Kind.BIG_INT;
            case "FLOAT", "DOUBLE" -> Kind.FLOATING;
            case "VARCHAR", "UUID", "INTERVAL" -> Kind.TEXT;
            case "BLOB" -> Kind.BLOB;
            case "DATE" -> Kind.DATE;
            case "TIME" -> Kind.TIME;
            case "TIMESTAMP", "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS" -> Kind.TIMESTAMP;
            case "TIMESTAMP WITH TIME ZONE" -> Kind.TIMESTAMP_TZ;
            default -> null;
        };
    }

    private static String apiType(String t, Kind k) {
        return switch (k) {
            case TIMESTAMP -> "TIMESTAMP";
            default -> t;
        };
    }

    /** The current row as JSON values, in column order. */
    static List<Json.Node> row(ResultSet rs, List<Col> cols) throws SQLException {
        List<Json.Node> out = new ArrayList<>(cols.size());
        for (int i = 0; i < cols.size(); i++) out.add(value(rs, i + 1, cols.get(i).kind()));
        return out;
    }

    private static Json.Node value(ResultSet rs, int i, Kind k) throws SQLException {
        switch (k) {
            case BOOLEAN -> {
                boolean b = rs.getBoolean(i);
                return rs.wasNull() ? Json.nil() : Json.bool(b);
            }
            case SMALL_INT -> {
                long v = rs.getLong(i);
                return rs.wasNull() ? Json.nil() : Json.num(v);
            }
            case WIDE_INT -> {
                long v = rs.getLong(i);
                return rs.wasNull() ? Json.nil() : Json.str(Long.toString(v));
            }
            case BIG_INT -> {
                Object v = rs.getObject(i);
                return v == null ? Json.nil() : Json.str(v.toString());
            }
            case DECIMAL -> {
                BigDecimal v = rs.getBigDecimal(i);
                return v == null ? Json.nil() : Json.str(v.toPlainString());
            }
            case FLOATING -> {
                double v = rs.getDouble(i);
                if (rs.wasNull()) return Json.nil();
                // JSON has no NaN or Infinity: those travel as their names.
                return Double.isNaN(v) || Double.isInfinite(v) ? Json.str(Double.toString(v)) : Json.num(v);
            }
            case TEXT -> {
                String v = rs.getString(i);
                return v == null ? Json.nil() : Json.str(v);
            }
            case BLOB -> {
                byte[] v = rs.getBytes(i);
                return v == null ? Json.nil() : Json.str(Base64.getEncoder().encodeToString(v));
            }
            case DATE -> {
                LocalDate v = rs.getObject(i, LocalDate.class);
                return v == null ? Json.nil() : Json.str(v.toString());
            }
            case TIME -> {
                LocalTime v = rs.getObject(i, LocalTime.class);
                return v == null ? Json.nil() : Json.str(v.toString());
            }
            case TIMESTAMP -> {
                LocalDateTime v = rs.getObject(i, LocalDateTime.class);
                return v == null ? Json.nil() : Json.str(v.toString());
            }
            case TIMESTAMP_TZ -> {
                OffsetDateTime v = rs.getObject(i, OffsetDateTime.class);
                return v == null ? Json.nil() : Json.str(v.toString());
            }
        }
        throw new IllegalStateException("unreachable");
    }
}
