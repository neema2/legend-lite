package com.legend.warehouse.server.duck;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

import com.legend.Nullable;
import com.legend.warehouse.sqlapi.DuckType;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * One column of one chunk, copied out of DuckDB's Arrow buffers (struct
 * ArrowArray, duckdb_data_chunk_to_arrow) into the heap with one bulk copy per
 * buffer. The JSON encoder, the nested-text builder and the Arrow framer all
 * read it. Layouts, as DuckDB's conversion writes them (measured,
 * docs/WAREHOUSE_FFM_HOMEWORK_2026_09_26.md §3):
 *
 * <ul>
 *   <li>BOOLEAN: validity + bits; fixed-width values (DECIMAL, HUGEINT, UHUGEINT
 *       and INTERVAL 16 bytes each): validity + values;</li>
 *   <li>VARCHAR, JSON, UUID, BLOB: validity + int32 offsets + bytes;</li>
 *   <li>ENUM: indices into a dictionary of strings: decoded to text here (its
 *       index kept too: a nested ENUM's value is built from it);</li>
 *   <li>LIST and MAP: validity + int32 offsets + the child (a MAP's child is a
 *       struct of key and value); a fixed array: validity + the child;
 *       STRUCT: validity + a child per field.</li>
 * </ul>
 */
final class ColumnData {

    // struct ArrowArray
    private static final long LENGTH = 0, NULL_COUNT = 8, OFFSET = 16, N_BUFFERS = 24, N_CHILDREN = 32,
            BUFFERS = 40, CHILDREN = 48, DICTIONARY = 56;

    final TypeTree tree;
    final int length;
    final int nulls;
    /** Null when every value is present. */
    final byte @Nullable [] validity;
    /** Fixed-width values, or a BOOLEAN's bits, little-endian. */
    final ByteBuffer values;
    /** TEXT/BLOB/LIST/MAP: length + 1 offsets. */
    final int @Nullable [] offsets;
    /** TEXT/BLOB: the bytes the offsets span. */
    final byte[] data;
    /** ENUM: each row's dictionary index. */
    final int @Nullable [] enumIndex;
    final List<ColumnData> children;

    private ColumnData(TypeTree tree, int length, int nulls, byte @Nullable [] validity, ByteBuffer values,
            int @Nullable [] offsets, byte[] data, int @Nullable [] enumIndex, List<ColumnData> children) {
        this.tree = tree;
        this.length = length;
        this.nulls = nulls;
        this.validity = validity;
        this.values = values;
        this.offsets = offsets;
        this.data = data;
        this.enumIndex = enumIndex;
        this.children = children;
    }

    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0).order(ByteOrder.LITTLE_ENDIAN);

    /** Copies one column (an ArrowArray) typed by {@code tree}. */
    static ColumnData copy(MemorySegment array, TypeTree tree) {
        MemorySegment a = array.reinterpret(Duck.ARRAY_SIZE);
        int n = Math.toIntExact(a.get(JAVA_LONG, LENGTH));
        int nulls = Math.toIntExact(a.get(JAVA_LONG, NULL_COUNT));
        if (a.get(JAVA_LONG, OFFSET) != 0) throw new IllegalStateException("an offset Arrow array (DuckDB never writes one)");
        long nb = a.get(JAVA_LONG, N_BUFFERS);
        MemorySegment bufs = a.get(ADDRESS, BUFFERS).reinterpret(8 * nb);
        byte[] validity = nulls == 0 ? null : bytes(bufs.get(ADDRESS, 0), (n + 7) / 8);
        MemorySegment dict = a.get(ADDRESS, DICTIONARY);
        if (!dict.equals(MemorySegment.NULL)) return enumColumn(tree, n, nulls, validity, bufs, dict.reinterpret(Duck.ARRAY_SIZE));
        List<ColumnData> kids = new ArrayList<>();
        long nc = a.get(JAVA_LONG, N_CHILDREN);
        if (nc > 0) {
            MemorySegment cs = a.get(ADDRESS, CHILDREN).reinterpret(8 * nc);
            if (tree.type instanceof DuckType.MapOf) {
                // the map's single child: a struct of key and value, typed by the map's two children
                MemorySegment entries = cs.get(ADDRESS, 0).reinterpret(Duck.ARRAY_SIZE);
                MemorySegment ec = entries.get(ADDRESS, CHILDREN).reinterpret(16);
                kids.add(copy(ec.get(ADDRESS, 0), tree.children.get(0)));
                kids.add(copy(ec.get(ADDRESS, 8), tree.children.get(1)));
            } else {
                for (int i = 0; i < nc; i++) kids.add(copy(cs.get(ADDRESS, 8L * i), tree.children.get(i)));
            }
        }
        return switch (tree.type) {
            case DuckType.ListOf l -> tree.id == Duck.ARRAY
                    ? new ColumnData(tree, n, nulls, validity, EMPTY, null, new byte[0], null, List.copyOf(kids))
                    : new ColumnData(tree, n, nulls, validity, EMPTY, ints(bufs.get(ADDRESS, 8), n + 1), new byte[0], null, List.copyOf(kids));
            case DuckType.MapOf m -> new ColumnData(tree, n, nulls, validity, EMPTY, ints(bufs.get(ADDRESS, 8), n + 1), new byte[0], null, List.copyOf(kids));
            case DuckType.StructOf s -> new ColumnData(tree, n, nulls, validity, EMPTY, null, new byte[0], null, List.copyOf(kids));
            case DuckType.Scalar s -> scalar(tree, s.base(), n, nulls, validity, bufs);
        };
    }

    private static ColumnData scalar(TypeTree tree, String base, int n, int nulls, byte @Nullable [] validity,
            MemorySegment bufs) {
        return switch (base) {
            case "VARCHAR", "JSON", "UUID", "BLOB" -> {
                int[] offs = ints(bufs.get(ADDRESS, 8), n + 1);
                yield new ColumnData(tree, n, nulls, validity, EMPTY, offs, bytes(bufs.get(ADDRESS, 16), offs[n]), null, List.of());
            }
            case "BOOLEAN" -> new ColumnData(tree, n, nulls, validity, le(bytes(bufs.get(ADDRESS, 8), (n + 7) / 8)), null, new byte[0], null, List.of());
            default -> new ColumnData(tree, n, nulls, validity, le(bytes(bufs.get(ADDRESS, 8), (long) n * width(base))), null, new byte[0], null, List.of());
        };
    }

    private static ColumnData enumColumn(TypeTree tree, int n, int nulls, byte @Nullable [] validity, MemorySegment bufs,
            MemorySegment dict) {
        int dn = Math.toIntExact(dict.get(JAVA_LONG, LENGTH));
        MemorySegment dbufs = dict.get(ADDRESS, BUFFERS).reinterpret(24);
        int[] doffs = ints(dbufs.get(ADDRESS, 8), dn + 1);
        byte[] ddata = bytes(dbufs.get(ADDRESS, 16), doffs[dn]);
        ByteBuffer idx = le(bytes(bufs.get(ADDRESS, 8), (long) n * tree.enumWidth));
        int[] index = new int[n];
        int[] offs = new int[n + 1];
        java.io.ByteArrayOutputStream text = new java.io.ByteArrayOutputStream();
        for (int i = 0; i < n; i++) {
            boolean present = validity == null || (validity[i >>> 3] >> (i & 7) & 1) == 1;
            if (present) {
                int k = switch (tree.enumWidth) {
                    case 1 -> Byte.toUnsignedInt(idx.get(i));
                    case 2 -> Short.toUnsignedInt(idx.getShort(2 * i));
                    default -> idx.getInt(4 * i);
                };
                index[i] = k;
                text.write(ddata, doffs[k], doffs[k + 1] - doffs[k]);
            }
            offs[i + 1] = text.size();
        }
        return new ColumnData(tree, n, nulls, validity, EMPTY, offs, text.toByteArray(), index, List.of());
    }

    /** Bytes per value of a fixed-width scalar. */
    static int width(String base) {
        return switch (base) {
            case "TINYINT", "UTINYINT" -> 1;
            case "SMALLINT", "USMALLINT" -> 2;
            case "INTEGER", "UINTEGER", "FLOAT", "DATE" -> 4;
            case "HUGEINT", "UHUGEINT", "DECIMAL", "INTERVAL" -> 16;
            default -> 8;   // BIGINT, UBIGINT, DOUBLE, TIME, the timestamps
        };
    }

    boolean present(int row) {
        byte[] v = validity;
        return v == null || (v[row >>> 3] >> (row & 7) & 1) == 1;
    }

    /** A TEXT/BLOB row's bytes. */
    byte[] bytesAt(int row) {
        int[] o = java.util.Objects.requireNonNull(offsets);
        return java.util.Arrays.copyOfRange(data, o[row], o[row + 1]);
    }

    private static byte[] bytes(MemorySegment p, long n) {
        if (n == 0 || p.equals(MemorySegment.NULL)) return new byte[0];
        return p.reinterpret(n).toArray(JAVA_BYTE);
    }

    private static int[] ints(MemorySegment p, int n) {
        return p.reinterpret(4L * n).toArray(JAVA_INT);
    }

    private static ByteBuffer le(byte[] b) {
        return ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN);
    }
}
