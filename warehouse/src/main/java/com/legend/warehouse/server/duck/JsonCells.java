package com.legend.warehouse.server.duck;

import com.legend.server.Json;
import com.legend.warehouse.sqlapi.DuckType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * A column's values as the API's JSON (docs/WAREHOUSE_W1_DESIGN_2026_09_26.md §3):
 * 64-bit and wider integers and decimals as strings, NaN, the infinities and
 * -0.0 by name, dates and times ISO-8601, blobs base64, TIMESTAMP WITH TIME ZONE
 * as the UTC instant (a client shows it in its own zone, as DuckDB's JDBC driver
 * does); a list as an array, a struct as an object, a map as [key, value] pairs.
 * A top-level nested cell also carries DuckDB's own text for it.
 */
final class JsonCells {

    private JsonCells() {
    }

    /** The top-level cell of {@code row}. */
    static Json.Node cell(Duck d, ColumnData c, int row) {
        Json.Node v = value(d, c, row);
        if (c.tree.type instanceof DuckType.Scalar || !c.present(row)) return v;
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("value", v);
        f.put("text", Json.str(DuckValues.text(d, c, row)));
        return new Json.Obj(f);
    }

    static Json.Node value(Duck d, ColumnData c, int row) {
        if (!c.present(row)) return Json.nil();
        return switch (c.tree.type) {
            case DuckType.ListOf l -> {
                long size = c.tree.arraySize(d);
                int from, to;
                if (size > 0) {
                    from = (int) (row * size);
                    to = (int) (from + size);
                } else {
                    int[] o = java.util.Objects.requireNonNull(c.offsets);
                    from = o[row];
                    to = o[row + 1];
                }
                List<Json.Node> out = new ArrayList<>(to - from);
                for (int i = from; i < to; i++) out.add(value(d, c.children.get(0), i));
                yield new Json.Arr(out);
            }
            case DuckType.StructOf s -> {
                LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
                for (int i = 0; i < s.fields().size(); i++) f.put(s.fields().get(i).name(), value(d, c.children.get(i), row));
                yield new Json.Obj(f);
            }
            case DuckType.MapOf m -> {
                int[] o = java.util.Objects.requireNonNull(c.offsets);
                List<Json.Node> pairs = new ArrayList<>();
                for (int i = o[row]; i < o[row + 1]; i++) {
                    pairs.add(new Json.Arr(List.of(value(d, c.children.get(0), i), value(d, c.children.get(1), i))));
                }
                yield new Json.Arr(pairs);
            }
            case DuckType.Scalar s -> scalar(d, c, row, s.base());
        };
    }

    private static Json.Node scalar(Duck d, ColumnData c, int row, String base) {
        ByteBuffer v = c.values;
        return switch (base) {
            case "BOOLEAN" -> Json.bool((v.get(row >>> 3) >> (row & 7) & 1) == 1);
            case "TINYINT" -> Json.num(v.get(row));
            case "UTINYINT" -> Json.num(Byte.toUnsignedInt(v.get(row)));
            case "SMALLINT" -> Json.num(v.getShort(2 * row));
            case "USMALLINT" -> Json.num(Short.toUnsignedInt(v.getShort(2 * row)));
            case "INTEGER" -> Json.num(v.getInt(4 * row));
            case "UINTEGER" -> Json.num(Integer.toUnsignedLong(v.getInt(4 * row)));
            case "BIGINT" -> Json.str(Long.toString(v.getLong(8 * row)));
            case "UBIGINT" -> Json.str(Long.toUnsignedString(v.getLong(8 * row)));
            case "HUGEINT" -> Json.str(int128(v, row, true).toString());
            case "UHUGEINT" -> Json.str(int128(v, row, false).toString());
            case "DECIMAL" -> Json.str(new BigDecimal(int128(v, row, true), c.tree.scale).toPlainString());
            case "FLOAT" -> floating(v.getFloat(4 * row));
            case "DOUBLE" -> floating(v.getDouble(8 * row));
            case "DATE" -> Json.str(LocalDate.ofEpochDay(v.getInt(4 * row)).toString());
            case "TIME" -> Json.str(LocalTime.ofNanoOfDay(v.getLong(8 * row) * 1_000).toString());
            case "TIMESTAMP" -> Json.str(since(v.getLong(8 * row), 1_000_000).toString());
            case "TIMESTAMP_S" -> Json.str(since(v.getLong(8 * row), 1).toString());
            case "TIMESTAMP_MS" -> Json.str(since(v.getLong(8 * row), 1_000).toString());
            case "TIMESTAMP_NS" -> Json.str(since(v.getLong(8 * row), 1_000_000_000).toString());
            case "TIMESTAMP WITH TIME ZONE" -> {
                long micros = v.getLong(8 * row);
                Instant i = Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000), Math.floorMod(micros, 1_000_000) * 1_000L);
                yield Json.str(OffsetDateTime.ofInstant(i, ZoneOffset.UTC).toString());
            }
            case "INTERVAL" -> Json.str(DuckValues.interval(d, v.getInt(16 * row), v.getInt(16 * row + 4),
                    v.getLong(16 * row + 8) / 1_000));
            case "BLOB" -> Json.str(Base64.getEncoder().encodeToString(c.bytesAt(row)));
            default -> Json.str(new String(c.bytesAt(row), StandardCharsets.UTF_8));   // VARCHAR, JSON, UUID, ENUM
        };
    }

    /** JSON has no NaN or infinities, and loses the sign of -0.0: those travel as their names. */
    private static Json.Node floating(double x) {
        boolean negativeZero = x == 0.0 && Double.doubleToRawLongBits(x) != 0;
        return Double.isNaN(x) || Double.isInfinite(x) || negativeZero ? Json.str(Double.toString(x)) : Json.num(x);
    }

    /** A 128-bit little-endian integer, signed or not. */
    static BigInteger int128(ByteBuffer v, int row, boolean signed) {
        long lower = v.getLong(16 * row);
        long upper = v.getLong(16 * row + 8);
        BigInteger hi = signed ? BigInteger.valueOf(upper) : new BigInteger(Long.toUnsignedString(upper));
        return hi.shiftLeft(64).add(new BigInteger(Long.toUnsignedString(lower)));
    }

    /** {@code units} ({@code perSecond} of them a second) since the epoch, as a local date-time. */
    private static LocalDateTime since(long units, long perSecond) {
        long seconds = Math.floorDiv(units, perSecond);
        long fraction = Math.floorMod(units, perSecond);
        return LocalDateTime.ofEpochSecond(seconds, (int) (fraction * (1_000_000_000L / perSecond)), ZoneOffset.UTC);
    }
}
