package com.legend.warehouse.sqlapi;

import com.legend.base.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * One column of one batch, in Arrow's layout, in the heap: what the server copies out of DuckDB
 * (duckdb_data_chunk_to_arrow) and what a client reads out of an Arrow chunk ({@link ArrowIpcReader}).
 * {@link ApiValues} reads it on both sides, so the API's value rules exist once.
 *
 * <ul>
 *   <li>BOOLEAN: validity + bits; fixed-width values (DECIMAL, HUGEINT, UHUGEINT and INTERVAL 16 bytes):
 *       validity + values, little-endian;</li>
 *   <li>VARCHAR, JSON, UUID, ENUM, BLOB (and UHUGEINT in an Arrow chunk): validity + offsets + bytes;</li>
 *   <li>LIST and MAP: validity + offsets + children (a MAP's two: keys and values); a fixed array
 *       {@code T[n]}: validity + the child; STRUCT: validity + a child per field.</li>
 * </ul>
 */
public final class Columnar {

    public final DuckType type;
    public final int length;
    public final int nulls;
    /** Null when every value is present. */
    public final byte @Nullable [] validity;
    public final ByteBuffer values;
    public final int @Nullable [] offsets;
    public final byte[] data;
    public final List<Columnar> children;
    /** UHUGEINT carried as its decimal text (an Arrow chunk) rather than 16 bytes (DuckDB's buffers). */
    public final boolean textual;
    /** A fixed array's size ({@code T[n]}); 0 otherwise. */
    public final long arraySize;
    /** DECIMAL(p,s)'s s; 0 otherwise. */
    public final int scale;
    /** An ENUM's dictionary index per row, where known (the server's: DuckDB rebuilds a nested ENUM from it). */
    public final int @Nullable [] enumIndex;

    public Columnar(DuckType type, int length, int nulls, byte @Nullable [] validity, ByteBuffer values,
            int @Nullable [] offsets, byte[] data, List<Columnar> children, boolean textual) {
        this(type, length, nulls, validity, values, offsets, data, children, textual, null);
    }

    public Columnar(DuckType type, int length, int nulls, byte @Nullable [] validity, ByteBuffer values,
            int @Nullable [] offsets, byte[] data, List<Columnar> children, boolean textual,
            int @Nullable [] enumIndex) {
        this.enumIndex = enumIndex;
        this.type = type;
        this.length = length;
        this.nulls = nulls;
        this.validity = validity;
        this.values = values;
        this.offsets = offsets;
        this.data = data;
        this.children = List.copyOf(children);
        this.textual = textual;
        this.arraySize = fixedSize(type);
        this.scale = type instanceof DuckType.Scalar s && s.base().equals("DECIMAL") ? decimal(s.name())[1] : 0;
    }

    public boolean present(int row) {
        byte[] v = validity;
        return v == null || (v[row >>> 3] >> (row & 7) & 1) == 1;
    }

    /** A TEXT/BLOB row's bytes. */
    public byte[] bytesAt(int row) {
        int[] o = java.util.Objects.requireNonNull(offsets);
        return java.util.Arrays.copyOfRange(data, o[row], o[row + 1]);
    }

    /** The rows a list (or fixed array) row spans in its child: {from, to}. */
    public int[] span(int row) {
        if (arraySize > 0) return new int[] {(int) (row * arraySize), (int) ((row + 1) * arraySize)};
        int[] o = java.util.Objects.requireNonNull(offsets);
        return new int[] {o[row], o[row + 1]};
    }

    /** {@code T[n]}: n; 0 for anything else (a list is {@code T[]}). */
    public static long fixedSize(DuckType t) {
        if (!(t instanceof DuckType.ListOf l)) return 0;
        String n = l.name();
        int open = n.lastIndexOf('[');
        if (open < 0 || n.endsWith("[]")) return 0;
        return Long.parseLong(n.substring(open + 1, n.length() - 1));
    }

    /** Bytes per value of a fixed-width scalar. */
    public static int width(String base) {
        return switch (base) {
            case "TINYINT", "UTINYINT" -> 1;
            case "SMALLINT", "USMALLINT" -> 2;
            case "INTEGER", "UINTEGER", "FLOAT", "DATE" -> 4;
            case "HUGEINT", "UHUGEINT", "DECIMAL", "INTERVAL" -> 16;
            default -> 8;   // BIGINT, UBIGINT, DOUBLE, TIME, the timestamps
        };
    }

    /** DECIMAL(p,s)'s p and s: DuckDB always spells both. */
    public static int[] decimal(String name) {
        int open = name.indexOf('('), comma = name.indexOf(',', open), close = name.indexOf(')', comma);
        return new int[] {Integer.parseInt(name.substring(open + 1, comma).strip()),
                Integer.parseInt(name.substring(comma + 1, close).strip())};
    }
}
