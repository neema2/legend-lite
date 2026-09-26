package com.legend.warehouse.client.jdbc;

import com.legend.Nullable;
import com.legend.server.Json;
import com.legend.warehouse.sqlapi.DuckType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The API's JSON values as the SAME Java objects DuckDB's own JDBC driver
 * returns, type by type (measured on 1.4.4 and 1.5.5.1, identical: see
 * WarehouseJdbcTest). legend-lite's executor is written against those
 * objects -- a Timestamp it re-reads as LocalDateTime, a java.sql.Array, a
 * java.sql.Struct, a LinkedHashMap -- so a remote cube must hand back the
 * same ones for the corpus to pass through the warehouse unchanged.
 */
final class Carriers {

    private Carriers() {
    }

    /** DuckDB's java.sql.Types code for a column of this type. */
    static int jdbcType(DuckType t) {
        return switch (t) {
            case DuckType.ListOf l -> Types.ARRAY;
            case DuckType.StructOf s -> Types.STRUCT;
            case DuckType.MapOf m -> Types.OTHER;
            case DuckType.Scalar s -> switch (s.base()) {
                case "BOOLEAN" -> Types.BOOLEAN;
                case "TINYINT" -> Types.TINYINT;
                case "SMALLINT", "UTINYINT" -> Types.SMALLINT;
                case "INTEGER", "USMALLINT" -> Types.INTEGER;
                case "BIGINT", "UINTEGER" -> Types.BIGINT;
                case "FLOAT" -> Types.FLOAT;
                case "DOUBLE" -> Types.DOUBLE;
                case "DECIMAL" -> Types.DECIMAL;
                case "VARCHAR" -> Types.VARCHAR;
                case "DATE" -> Types.DATE;
                case "TIME" -> Types.TIME;
                case "TIMESTAMP", "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS" -> Types.TIMESTAMP;
                case "TIMESTAMP WITH TIME ZONE" -> Types.TIMESTAMP_WITH_TIMEZONE;
                case "BLOB" -> Types.BLOB;
                default -> Types.OTHER;   // HUGEINT, UBIGINT, UHUGEINT, UUID, INTERVAL, ENUM, JSON
            };
        };
    }

    /** The Java object DuckDB's driver would return for this JSON value of this type. */
    static @Nullable Object decode(Json.Node v, DuckType t) {
        if (v instanceof Json.Null) return null;
        return switch (t) {
            case DuckType.ListOf l -> {
                List<Json.Node> items = ((Json.Arr) v).items();
                List<@Nullable Object> out = new ArrayList<>(items.size());
                for (Json.Node item : items) out.add(decode(item, l.element()));
                yield new WhArray(l.element(), out);
            }
            case DuckType.StructOf s -> {
                Json.Obj o = (Json.Obj) v;
                List<@Nullable Object> attrs = new ArrayList<>(s.fields().size());
                for (DuckType.Field f : s.fields()) {
                    attrs.add(decode(o.has(f.name()) ? o.get(f.name()) : Json.nil(), f.type()));
                }
                yield new WhStruct(s, attrs);
            }
            case DuckType.MapOf m -> {
                Map<Object, Object> out = new LinkedHashMap<>();
                for (Json.Node pair : ((Json.Arr) v).items()) {
                    List<Json.Node> kv = ((Json.Arr) pair).items();
                    out.put(decode(kv.get(0), m.key()), decode(kv.get(1), m.value()));
                }
                yield out;
            }
            case DuckType.Scalar s -> scalar(v, s.base());
        };
    }

    private static Object scalar(Json.Node v, String base) {
        return switch (base) {
            case "BOOLEAN" -> ((Json.Bool) v).value();
            case "TINYINT" -> (byte) num(v).longValue();
            case "SMALLINT", "UTINYINT" -> (short) num(v).longValue();
            case "INTEGER", "USMALLINT" -> (int) num(v).longValue();
            case "UINTEGER" -> num(v).longValue();
            case "BIGINT" -> Long.parseLong(str(v));
            case "HUGEINT", "UBIGINT", "UHUGEINT" -> new BigInteger(str(v));
            case "FLOAT" -> (float) floating(v);
            case "DOUBLE" -> floating(v);
            case "DECIMAL" -> new BigDecimal(str(v));
            case "UUID" -> UUID.fromString(str(v));
            case "DATE" -> LocalDate.parse(str(v));
            case "TIME" -> LocalTime.parse(str(v));
            case "TIMESTAMP", "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS" -> Timestamp.valueOf(LocalDateTime.parse(str(v)));
            case "TIMESTAMP WITH TIME ZONE" -> OffsetDateTime.parse(str(v));
            case "BLOB" -> new WhBlob(Base64.getDecoder().decode(str(v)));
            // VARCHAR, ENUM, INTERVAL, and JSON as its text: DuckDB's driver
            // returns its own node type for JSON; its text is the same.
            default -> str(v);
        };
    }

    private static Json.Num num(Json.Node v) {
        return (Json.Num) v;
    }

    private static String str(Json.Node v) {
        return ((Json.Str) v).value();
    }

    private static double floating(Json.Node v) {
        if (v instanceof Json.Str s) return Double.parseDouble(s.value());   // NaN, Infinity
        return ((Json.Num) v).doubleValue();
    }

    /** A top-level cell's value: a nested column's cell is {@code {"value", "text"}}. */
    static Json.Node valueOf(Json.Node cell, DuckType t) {
        return !(t instanceof DuckType.Scalar) && cell instanceof Json.Obj o && o.has("value") && o.has("text")
                ? o.get("value") : cell;
    }

    /** DuckDB's own text for a nested cell, or null for a scalar or a null. */
    static @Nullable String textOf(Json.Node cell, DuckType t) {
        return !(t instanceof DuckType.Scalar) && cell instanceof Json.Obj o && o.has("text")
                ? ((Json.Str) o.get("text")).value() : null;
    }

    /** A LocalDateTime for a TIMESTAMP carrier, as {@code getObject(i, LocalDateTime.class)} gives it. */
    static @Nullable LocalDateTime localDateTime(@Nullable Object v) {
        return v instanceof Timestamp ts ? ts.toLocalDateTime() : (LocalDateTime) v;
    }

}
