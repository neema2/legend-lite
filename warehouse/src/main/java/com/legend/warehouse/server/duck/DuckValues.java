package com.legend.warehouse.server.duck;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

import com.legend.warehouse.sqlapi.Columnar;
import com.legend.warehouse.sqlapi.DuckType;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

/**
 * A cell's text as DuckDB itself renders it: the cell is built back into a
 * {@code duckdb_value} (against the column's logical type) and DuckDB casts it
 * to VARCHAR ({@code duckdb_get_varchar}). That cast is what DuckDB's JDBC
 * driver's {@code getString} gives for a nested value (measured: identical,
 * NULLs, quoting, timestamps, NaN included), so no client re-derives DuckDB's
 * quoting rules. Used for a top-level nested cell's {@code "text"}.
 *
 * <p>A few C calls per value: only nested columns pay them.
 */
final class DuckValues {

    private DuckValues() {
    }

    /** DuckDB's text for one row of the column. */
    static String text(Duck d, Columnar c, TypeTree t, int row) {
        try (Arena a = Arena.ofConfined()) {
            MemorySegment v = value(d, c, t, row, a);
            try {
                MemorySegment s = (MemorySegment) d.getVarchar.invokeExact(v);
                return d.owned(s);
            } finally {
                destroy(d, v, a);
            }
        } catch (Throwable failed) {
            throw Duck.fail(failed);
        }
    }

    /** The value (the caller destroys it); DuckDB copies children into a nested value, so they are destroyed here. */
    private static MemorySegment value(Duck d, Columnar c, TypeTree t, int row, Arena a) throws Throwable {
        if (!c.present(row)) return (MemorySegment) d.createNull.invokeExact();
        return switch (t.type) {
            case DuckType.ListOf l -> {
                int from, to;
                long size = t.arraySize(d);
                if (size > 0) {
                    from = (int) (row * size);
                    to = (int) (from + size);
                } else {
                    int[] o = java.util.Objects.requireNonNull(c.offsets);
                    from = o[row];
                    to = o[row + 1];
                }
                Columnar items = c.children.get(0);
                TypeTree it = t.children.get(0);
                MemorySegment vs = a.allocate(ADDRESS, Math.max(1, to - from));
                for (int i = from; i < to; i++) vs.setAtIndex(ADDRESS, i - from, value(d, items, it, i, a));
                MemorySegment out = size > 0
                        ? (MemorySegment) d.createArray.invokeExact(it.logical, vs, (long) (to - from))
                        : (MemorySegment) d.createList.invokeExact(it.logical, vs, (long) (to - from));
                for (int i = 0; i < to - from; i++) destroy(d, vs.getAtIndex(ADDRESS, i), a);
                yield refuseNull(out, t);
            }
            case DuckType.StructOf s -> {
                int n = c.children.size();
                MemorySegment vs = a.allocate(ADDRESS, Math.max(1, n));
                for (int i = 0; i < n; i++) vs.setAtIndex(ADDRESS, i, value(d, c.children.get(i), t.children.get(i), row, a));
                MemorySegment out = (MemorySegment) d.createStruct.invokeExact(t.logical, vs);
                for (int i = 0; i < n; i++) destroy(d, vs.getAtIndex(ADDRESS, i), a);
                yield refuseNull(out, t);
            }
            case DuckType.MapOf m -> {
                int[] o = java.util.Objects.requireNonNull(c.offsets);
                int from = o[row], to = o[row + 1];
                MemorySegment ks = a.allocate(ADDRESS, Math.max(1, to - from)), vs = a.allocate(ADDRESS, Math.max(1, to - from));
                for (int i = from; i < to; i++) {
                    ks.setAtIndex(ADDRESS, i - from, value(d, c.children.get(0), t.children.get(0), i, a));
                    vs.setAtIndex(ADDRESS, i - from, value(d, c.children.get(1), t.children.get(1), i, a));
                }
                MemorySegment out = (MemorySegment) d.createMap.invokeExact(t.logical, ks, vs, (long) (to - from));
                for (int i = 0; i < to - from; i++) {
                    destroy(d, ks.getAtIndex(ADDRESS, i), a);
                    destroy(d, vs.getAtIndex(ADDRESS, i), a);
                }
                yield refuseNull(out, t);
            }
            case DuckType.Scalar s -> scalar(d, c, t, row, s.base(), a);
        };
    }

    private static MemorySegment scalar(Duck d, Columnar c, TypeTree t, int row, String base, Arena a) throws Throwable {
        ByteBuffer v = c.values;
        return switch (base) {
            case "BOOLEAN" -> (MemorySegment) d.createBool.invokeExact((v.get(row >>> 3) >> (row & 7) & 1) == 1);
            case "TINYINT" -> (MemorySegment) d.createInt8.invokeExact(v.get(row));
            case "UTINYINT" -> (MemorySegment) d.createUint8.invokeExact(v.get(row));
            case "SMALLINT" -> (MemorySegment) d.createInt16.invokeExact(v.getShort(2 * row));
            case "USMALLINT" -> (MemorySegment) d.createUint16.invokeExact(v.getShort(2 * row));
            case "INTEGER" -> (MemorySegment) d.createInt32.invokeExact(v.getInt(4 * row));
            case "UINTEGER" -> (MemorySegment) d.createUint32.invokeExact(v.getInt(4 * row));
            case "BIGINT" -> (MemorySegment) d.createInt64.invokeExact(v.getLong(8 * row));
            case "UBIGINT" -> (MemorySegment) d.createUint64.invokeExact(v.getLong(8 * row));
            case "FLOAT" -> (MemorySegment) d.createFloat.invokeExact(v.getFloat(4 * row));
            case "DOUBLE" -> (MemorySegment) d.createDouble.invokeExact(v.getDouble(8 * row));
            case "HUGEINT" -> (MemorySegment) d.createHugeint.invokeExact(hugeint(a, v, row));
            case "UHUGEINT" -> (MemorySegment) d.createUhugeint.invokeExact(hugeint(a, v, row));
            case "DECIMAL" -> {
                MemorySegment dec = a.allocate(Duck.DECIMAL);
                dec.set(JAVA_BYTE, 0, (byte) t.precision);
                dec.set(JAVA_BYTE, 1, (byte) t.scale);
                dec.set(JAVA_LONG, 8, v.getLong(16 * row));
                dec.set(JAVA_LONG, 16, v.getLong(16 * row + 8));
                yield (MemorySegment) d.createDecimal.invokeExact(dec);
            }
            case "DATE" -> (MemorySegment) d.createDate.invokeExact(one(a, Duck.INT32, v.getInt(4 * row)));
            case "TIME" -> (MemorySegment) d.createTime.invokeExact(one(a, v.getLong(8 * row)));
            case "TIMESTAMP" -> (MemorySegment) d.createTimestamp.invokeExact(one(a, v.getLong(8 * row)));
            case "TIMESTAMP_S" -> (MemorySegment) d.createTimestampS.invokeExact(one(a, v.getLong(8 * row)));
            case "TIMESTAMP_MS" -> (MemorySegment) d.createTimestampMs.invokeExact(one(a, v.getLong(8 * row)));
            case "TIMESTAMP_NS" -> (MemorySegment) d.createTimestampNs.invokeExact(one(a, v.getLong(8 * row)));
            case "TIMESTAMP WITH TIME ZONE" -> (MemorySegment) d.createTimestampTz.invokeExact(one(a, v.getLong(8 * row)));
            case "INTERVAL" -> {
                MemorySegment in = a.allocate(Duck.INTERVAL);
                in.set(JAVA_INT, 0, v.getInt(16 * row));
                in.set(JAVA_INT, 4, v.getInt(16 * row + 4));
                in.set(JAVA_LONG, 8, v.getLong(16 * row + 8) / 1_000);   // Arrow carries nanoseconds
                yield (MemorySegment) d.createInterval.invokeExact(in);
            }
            case "ENUM" -> (MemorySegment) d.createEnum.invokeExact(t.logical,
                    (long) java.util.Objects.requireNonNull(c.enumIndex)[row]);
            case "UUID" -> {
                java.util.UUID u = java.util.UUID.fromString(new String(c.bytesAt(row), java.nio.charset.StandardCharsets.UTF_8));
                MemorySegment h = a.allocate(Duck.HUGEINT);
                h.set(JAVA_LONG, 0, u.getLeastSignificantBits());
                h.set(JAVA_LONG, 8, u.getMostSignificantBits());
                yield (MemorySegment) d.createUuid.invokeExact(h);
            }
            case "BLOB" -> {
                byte[] b = c.bytesAt(row);
                yield (MemorySegment) d.createBlob.invokeExact(a.allocateFrom(JAVA_BYTE, b), (long) b.length);
            }
            default -> {   // VARCHAR, JSON
                byte[] b = c.bytesAt(row);
                yield (MemorySegment) d.createVarchar.invokeExact(a.allocateFrom(JAVA_BYTE, b), (long) b.length);
            }
        };
    }

    private static MemorySegment hugeint(Arena a, ByteBuffer v, int row) {
        MemorySegment h = a.allocate(Duck.HUGEINT);
        h.set(JAVA_LONG, 0, v.getLong(16 * row));
        h.set(JAVA_LONG, 8, v.getLong(16 * row + 8));
        return h;
    }

    private static MemorySegment one(Arena a, java.lang.foreign.StructLayout l, int x) {
        MemorySegment s = a.allocate(l);
        s.set(JAVA_INT, 0, x);
        return s;
    }

    private static MemorySegment one(Arena a, long x) {
        MemorySegment s = a.allocate(Duck.INT64);
        s.set(JAVA_LONG, 0, x);
        return s;
    }

    private static MemorySegment refuseNull(MemorySegment v, TypeTree t) {
        if (v.equals(MemorySegment.NULL)) {
            throw new IllegalStateException("DuckDB could not build a " + t.type.name() + " value to render");
        }
        return v;
    }

    private static void destroy(Duck d, MemorySegment v, Arena a) throws Throwable {
        MemorySegment p = a.allocate(ADDRESS);
        p.set(ADDRESS, 0, v);
        d.destroyValue.invokeExact(p);
    }
}
