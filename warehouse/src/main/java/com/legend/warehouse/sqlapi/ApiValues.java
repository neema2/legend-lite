package com.legend.warehouse.sqlapi;

import com.legend.server.Json;
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
 * The API's JSON values (docs/WAREHOUSE_W1_DESIGN_2026_09_26.md §3), from a {@link Columnar}: the ONE
 * place the rules live. The server writes them from DuckDB's buffers; a client that fetched Arrow
 * reads them back from the chunk, so an Arrow result and a JSON result give a client the same values.
 *
 * <ul>
 *   <li>64-bit and wider integers and decimals as strings; NaN, the infinities and -0.0 by name;</li>
 *   <li>dates and times ISO-8601; TIMESTAMP WITH TIME ZONE as the UTC instant; INTERVAL as DuckDB
 *       spells it ({@link Intervals}); blobs base64;</li>
 *   <li>a list as an array, a struct as an object, a map as [key, value] pairs; a top-level nested
 *       cell as {@code {"value", "text"}}, the text DuckDB's own ({@link NestedText}).</li>
 * </ul>
 */
public final class ApiValues {

    private ApiValues() {
    }

    /** DuckDB's own text for a top-level nested cell: the server asks DuckDB; a client reads it off the chunk. */
    public interface NestedText {
        String text(int column, int row);
    }

    // -- as a tree ---------------------------------------------------------------------------

    public static Json.Node cell(Columnar c, int column, int row, NestedText texts) {
        Json.Node v = value(c, row);
        if (c.type instanceof DuckType.Scalar || !c.present(row)) return v;
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("value", v);
        f.put("text", Json.str(texts.text(column, row)));
        return new Json.Obj(f);
    }

    public static Json.Node value(Columnar c, int row) {
        if (!c.present(row)) return Json.nil();
        return switch (c.type) {
            case DuckType.ListOf l -> {
                int[] span = c.span(row);
                List<Json.Node> out = new ArrayList<>(span[1] - span[0]);
                for (int i = span[0]; i < span[1]; i++) out.add(value(c.children.get(0), i));
                yield new Json.Arr(out);
            }
            case DuckType.StructOf s -> {
                LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
                for (int i = 0; i < s.fields().size(); i++) f.put(s.fields().get(i).name(), value(c.children.get(i), row));
                yield new Json.Obj(f);
            }
            case DuckType.MapOf m -> {
                int[] o = java.util.Objects.requireNonNull(c.offsets);
                List<Json.Node> pairs = new ArrayList<>();
                for (int i = o[row]; i < o[row + 1]; i++) {
                    pairs.add(new Json.Arr(List.of(value(c.children.get(0), i), value(c.children.get(1), i))));
                }
                yield new Json.Arr(pairs);
            }
            case DuckType.Scalar s -> scalar(c, row, s.base());
        };
    }

    private static Json.Node scalar(Columnar c, int row, String base) {
        ByteBuffer v = c.values;
        return switch (base) {
            case "BOOLEAN" -> Json.bool(bit(v, row));
            case "TINYINT", "UTINYINT", "SMALLINT", "USMALLINT", "INTEGER", "UINTEGER" -> Json.num(small(v, row, base));
            case "FLOAT" -> floating(v.getFloat(4 * row));
            case "DOUBLE" -> floating(v.getDouble(8 * row));
            default -> Json.str(text(c, row, base));
        };
    }

    // -- written, with no tree (a JSON chunk's path) ------------------------------------------

    /** The top-level cell, written: the same JSON as {@link #cell}. */
    public static void writeCell(Json.Writer w, Columnar c, int column, int row, NestedText texts) {
        if (c.type instanceof DuckType.Scalar || !c.present(row)) {
            write(w, c, row);
            return;
        }
        w.beginObject();
        w.name("value");
        write(w, c, row);
        w.field("text", texts.text(column, row));
        w.endObject();
    }

    public static void write(Json.Writer w, Columnar c, int row) {
        if (!c.present(row)) {
            w.writeNull();
            return;
        }
        switch (c.type) {
            case DuckType.ListOf l -> {
                int[] span = c.span(row);
                w.beginArray();
                for (int i = span[0]; i < span[1]; i++) write(w, c.children.get(0), i);
                w.endArray();
            }
            case DuckType.StructOf s -> {
                w.beginObject();
                for (int i = 0; i < s.fields().size(); i++) {
                    w.name(s.fields().get(i).name());
                    write(w, c.children.get(i), row);
                }
                w.endObject();
            }
            case DuckType.MapOf m -> {
                int[] o = java.util.Objects.requireNonNull(c.offsets);
                w.beginArray();
                for (int i = o[row]; i < o[row + 1]; i++) {
                    w.beginArray();
                    write(w, c.children.get(0), i);
                    write(w, c.children.get(1), i);
                    w.endArray();
                }
                w.endArray();
            }
            case DuckType.Scalar s -> {
                ByteBuffer v = c.values;
                String base = s.base();
                switch (base) {
                    case "BOOLEAN" -> w.writeBool(bit(v, row));
                    case "TINYINT", "UTINYINT", "SMALLINT", "USMALLINT", "INTEGER", "UINTEGER" -> w.writeLong(small(v, row, base));
                    case "FLOAT" -> writeFloating(w, v.getFloat(4 * row));
                    case "DOUBLE" -> writeFloating(w, v.getDouble(8 * row));
                    default -> w.writeString(text(c, row, base));
                }
            }
        }
    }

    // -- the rules ---------------------------------------------------------------------------

    /** Every value the API carries as a string, as that string. */
    private static String text(Columnar c, int row, String base) {
        ByteBuffer v = c.values;
        return switch (base) {
            case "BIGINT" -> Long.toString(v.getLong(8 * row));
            case "UBIGINT" -> Long.toUnsignedString(v.getLong(8 * row));
            case "HUGEINT" -> int128(v, row, true).toString();
            case "UHUGEINT" -> c.textual ? new String(c.bytesAt(row), StandardCharsets.UTF_8) : int128(v, row, false).toString();
            case "DECIMAL" -> new BigDecimal(int128(v, row, true), c.scale).toPlainString();
            case "DATE" -> LocalDate.ofEpochDay(v.getInt(4 * row)).toString();
            case "TIME" -> LocalTime.ofNanoOfDay(v.getLong(8 * row) * 1_000).toString();
            case "TIMESTAMP" -> since(v.getLong(8 * row), 1_000_000).toString();
            case "TIMESTAMP_S" -> since(v.getLong(8 * row), 1).toString();
            case "TIMESTAMP_MS" -> since(v.getLong(8 * row), 1_000).toString();
            case "TIMESTAMP_NS" -> since(v.getLong(8 * row), 1_000_000_000).toString();
            case "TIMESTAMP WITH TIME ZONE" -> {
                long micros = v.getLong(8 * row);
                Instant i = Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000), Math.floorMod(micros, 1_000_000) * 1_000L);
                yield OffsetDateTime.ofInstant(i, ZoneOffset.UTC).toString();
            }
            // Arrow's month-day-nano: int32 months, int32 days, int64 nanoseconds
            case "INTERVAL" -> Intervals.text(v.getInt(16 * row), v.getInt(16 * row + 4), v.getLong(16 * row + 8) / 1_000);
            case "BLOB" -> Base64.getEncoder().encodeToString(c.bytesAt(row));
            default -> new String(c.bytesAt(row), StandardCharsets.UTF_8);   // VARCHAR, JSON, UUID, ENUM
        };
    }

    private static boolean bit(ByteBuffer v, int row) {
        return (v.get(row >>> 3) >> (row & 7) & 1) == 1;
    }

    private static long small(ByteBuffer v, int row, String base) {
        return switch (base) {
            case "TINYINT" -> v.get(row);
            case "UTINYINT" -> Byte.toUnsignedInt(v.get(row));
            case "SMALLINT" -> v.getShort(2 * row);
            case "USMALLINT" -> Short.toUnsignedInt(v.getShort(2 * row));
            case "INTEGER" -> v.getInt(4 * row);
            default -> Integer.toUnsignedLong(v.getInt(4 * row));   // UINTEGER
        };
    }

    /** JSON has no NaN or infinities, and loses the sign of -0.0: those travel as their names. */
    private static Json.Node floating(double x) {
        return special(x) ? Json.str(Double.toString(x)) : Json.num(x);
    }

    private static void writeFloating(Json.Writer w, double x) {
        if (special(x)) w.writeString(Double.toString(x));
        else w.writeDouble(x);
    }

    private static boolean special(double x) {
        return Double.isNaN(x) || Double.isInfinite(x) || (x == 0.0 && Double.doubleToRawLongBits(x) != 0);
    }

    /** A 128-bit little-endian integer, signed or not. */
    public static BigInteger int128(ByteBuffer v, int row, boolean signed) {
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
