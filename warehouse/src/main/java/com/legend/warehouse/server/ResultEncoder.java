package com.legend.warehouse.server;

import com.legend.Nullable;
import com.legend.server.Json;
import com.legend.warehouse.sqlapi.DuckType;
import com.legend.warehouse.sqlapi.SqlApi.Column;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A JDBC result as the API's columns and JSON values
 * (docs/WAREHOUSE_W1_DESIGN_2026_09_26.md §3).
 *
 * <p>A column's API type is DuckDB's own type name, unchanged; its values
 * are encoded by walking that type ({@link DuckType}):
 * <ul>
 *   <li>64-bit and wider integers, and decimals, as STRINGS, so a
 *   JavaScript client loses no precision;</li>
 *   <li>dates and times ISO-8601; NaN and infinities as their names;
 *   blobs as base64; JSON as its text;</li>
 *   <li>a list as an array, a struct as an object, a map as an array of
 *   {@code [key, value]} pairs (keys need not be strings).</li>
 * </ul>
 * A type outside what this knows is refused with its name, never guessed.
 */
final class ResultEncoder {

    record Col(String name, String typeName, DuckType type) {
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
            String typeName = md.getColumnTypeName(i);
            DuckType t;
            try {
                t = DuckType.parse(typeName);
            } catch (IllegalArgumentException bad) {
                throw new Unsupported(name, typeName);
            }
            if (!supported(t)) throw new Unsupported(name, typeName);
            out.add(new Col(name, typeName, t));
        }
        return out;
    }

    static List<Column> api(List<Col> cols) {
        List<Column> out = new ArrayList<>(cols.size());
        for (Col c : cols) out.add(new Column(c.name(), c.typeName(), true));
        return out;
    }

    private static boolean supported(DuckType t) {
        return switch (t) {
            case DuckType.Scalar s -> SCALARS.contains(s.base());
            case DuckType.ListOf l -> supported(l.element());
            case DuckType.StructOf st -> st.fields().stream().allMatch(f -> supported(f.type()));
            case DuckType.MapOf m -> supported(m.key()) && supported(m.value());
        };
    }

    private static final java.util.Set<String> SCALARS = java.util.Set.of(
            "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
            "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT",
            "FLOAT", "DOUBLE", "DECIMAL", "VARCHAR", "UUID", "INTERVAL", "ENUM", "JSON", "BLOB",
            "DATE", "TIME", "TIMESTAMP", "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS",
            "TIMESTAMP WITH TIME ZONE");

    /** The current row as JSON values, in column order. */
    static List<Json.Node> row(ResultSet rs, List<Col> cols) throws SQLException {
        List<Json.Node> out = new ArrayList<>(cols.size());
        for (int i = 0; i < cols.size(); i++) {
            DuckType t = cols.get(i).type();
            Object v;
            // A top-level timestamp is read as java.time: java.sql.Timestamp's
            // epoch is wrong for BC years (legend-lite's executor does the same).
            if (t instanceof DuckType.Scalar s && s.base().startsWith("TIMESTAMP") && !s.base().contains("ZONE")) {
                v = rs.getObject(i + 1, LocalDateTime.class);
            } else {
                v = rs.getObject(i + 1);
            }
            Json.Node value = encode(v, t);
            // A NESTED column also carries DuckDB's own text for it, as its
            // driver's getString gives it ([{'x': 1, 'y': NULL}]): a client
            // must not re-derive DuckDB's quoting rules and drift from them.
            if (!(t instanceof DuckType.Scalar) && v != null) {
                LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
                f.put("value", value);
                f.put("text", Json.str(String.valueOf(rs.getString(i + 1))));
                value = new Json.Obj(f);
            }
            out.add(value);
        }
        return out;
    }

    static Json.Node encode(@Nullable Object v, DuckType t) throws SQLException {
        if (v == null) return Json.nil();
        return switch (t) {
            case DuckType.Scalar s -> scalar(v, s.base());
            case DuckType.ListOf l -> {
                Object[] items = (Object[]) ((Array) v).getArray();
                List<Json.Node> out = new ArrayList<>(items.length);
                for (Object item : items) out.add(encode(item, l.element()));
                yield new Json.Arr(out);
            }
            case DuckType.StructOf st -> {
                Object[] attrs = ((Struct) v).getAttributes();
                LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
                for (int i = 0; i < st.fields().size(); i++) {
                    f.put(st.fields().get(i).name(), encode(i < attrs.length ? attrs[i] : null, st.fields().get(i).type()));
                }
                yield new Json.Obj(f);
            }
            case DuckType.MapOf m -> {
                List<Json.Node> pairs = new ArrayList<>();
                for (Map.Entry<?, ?> e : ((Map<?, ?>) v).entrySet()) {
                    pairs.add(new Json.Arr(List.of(encode(e.getKey(), m.key()), encode(e.getValue(), m.value()))));
                }
                yield new Json.Arr(pairs);
            }
        };
    }

    private static Json.Node scalar(Object v, String base) throws SQLException {
        switch (base) {
            case "BOOLEAN":
                return Json.bool((Boolean) v);
            case "TINYINT": case "SMALLINT": case "INTEGER": case "UTINYINT": case "USMALLINT": case "UINTEGER":
                return Json.num(((Number) v).longValue());
            case "BIGINT": case "HUGEINT": case "UBIGINT": case "UHUGEINT":
                return Json.str(v.toString());
            case "DECIMAL":
                return Json.str(((BigDecimal) v).toPlainString());
            case "FLOAT": case "DOUBLE": {
                double d = ((Number) v).doubleValue();
                // JSON has no NaN or infinities, and loses the sign of -0.0:
                // those travel as their names.
                boolean negativeZero = d == 0.0 && Double.doubleToRawLongBits(d) != 0;
                return Double.isNaN(d) || Double.isInfinite(d) || negativeZero
                        ? Json.str(Double.toString(d)) : Json.num(d);
            }
            case "BLOB": {
                Blob b = (Blob) v;
                return Json.str(Base64.getEncoder().encodeToString(b.getBytes(1, (int) b.length())));
            }
            case "TIMESTAMP": case "TIMESTAMP_S": case "TIMESTAMP_MS": case "TIMESTAMP_NS":
                // Nested timestamps come as java.sql.Timestamp; top-level ones as LocalDateTime.
                return Json.str((v instanceof Timestamp ts ? ts.toLocalDateTime() : (LocalDateTime) v).toString());
            default:
                // VARCHAR, ENUM, INTERVAL, UUID, JSON, DATE, TIME, TIMESTAMP WITH TIME ZONE:
                // their Java objects print as the API's text (UUID, ISO dates and times, JSON text).
                return Json.str(v.toString());
        }
    }
}
