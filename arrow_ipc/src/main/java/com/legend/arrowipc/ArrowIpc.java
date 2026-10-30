// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.arrowipc;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;

/**
 * THE ARROW IPC FRAMER (the stream format, Arrow's {@code Message.fbs} and {@code Schema.fbs}): a schema message
 * from fields, a record batch message from a batch's nodes and buffers, the end marker -- each an encapsulated
 * message (the continuation marker, the metadata length, the FlatBuffers metadata padded to 8 bytes, the body).
 * Written with {@link FlatBuilder}, no Arrow library (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md R9). The warehouse
 * server frames DuckDB's buffers with it ({@code duck/ArrowStreams}); core's {@code result/ArrowWriter} frames the
 * buffers it builds from a typed batch (track A, 2026-10-29). A reader of what this writes: pyarrow in the tests, the
 * browser's apache-arrow, the warehouse's own {@code sqlapi/ArrowIpcReader}.
 */
public final class ArrowIpc {

    private ArrowIpc() {
    }

    public static final String MEDIA_TYPE = "application/vnd.apache.arrow.stream";

    /** The end of a stream: a continuation marker and a zero metadata length. */
    public static final byte[] END = {-1, -1, -1, -1, 0, 0, 0, 0};

    private static final short V5 = 4;
    private static final byte SCHEMA = 1;
    private static final byte RECORD_BATCH = 3;

    /** One batch's columns laid out as Arrow lists them: a node (length, null count) per array, depth first, and the
     *  buffers in the same order (validity, then the type's own: offsets, values, type ids ...). */
    public static final class Layout {
        private final java.util.ArrayList<long[]> nodes = new java.util.ArrayList<>();
        private final java.util.ArrayList<byte[]> buffers = new java.util.ArrayList<>();

        /** The next array's node: its values and how many are null. */
        public void node(long length, long nulls) {
            nodes.add(new long[] {length, nulls});
        }

        /** The next buffer; an empty array where the layout needs none (no nulls: an empty validity buffer). */
        public void buffer(byte[] bytes) {
            buffers.add(bytes);
        }
    }

    /** The schema message: {@code fields} in order, with the stream's custom metadata (key-value texts). */
    public static byte[] schema(List<ArrowField> fields, Map<String, String> metadata) {
        FlatBuilder b = new FlatBuilder();
        int[] fs = new int[fields.size()];
        for (int i = 0; i < fs.length; i++) fs[i] = field(b, fields.get(i));
        int fv = b.offsets(fs);
        int md = metadata.isEmpty() ? 0 : keyValues(b, metadata);
        b.startTable(4);
        b.fieldShort(0, 0);   // little-endian
        b.fieldOffset(1, fv);
        if (md != 0) b.fieldOffset(2, md);
        return frame(message(b, SCHEMA, b.endTable(), 0, 0), new byte[0]);
    }

    /** A record batch message of {@code rows} rows: the layout's nodes and buffers (each buffer padded to 8 bytes in
     *  the body), with the message's custom metadata (key-value texts). */
    public static byte[] recordBatch(long rows, Layout layout, Map<String, String> metadata) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        int n = layout.buffers.size();
        long[] offsets = new long[n], lengths = new long[n];
        for (int i = 0; i < n; i++) {
            byte[] buffer = layout.buffers.get(i);
            offsets[i] = body.size();
            lengths[i] = buffer.length;
            put(body, buffer);
            put(body, new byte[(int) (pad8(lengths[i]) - lengths[i])]);
        }
        int m = layout.nodes.size();
        long[] nl = new long[m], nn = new long[m];
        for (int i = 0; i < m; i++) {
            nl[i] = layout.nodes.get(i)[0];
            nn[i] = layout.nodes.get(i)[1];
        }
        FlatBuilder b = new FlatBuilder();
        int nv = b.longPairs(nl, nn), bv = b.longPairs(offsets, lengths);
        b.startTable(5);
        b.fieldLong(0, rows);
        b.fieldOffset(1, nv);
        b.fieldOffset(2, bv);
        int batch = b.endTable();
        int md = metadata.isEmpty() ? 0 : keyValues(b, metadata);
        return frame(message(b, RECORD_BATCH, batch, body.size(), md), body.toByteArray());
    }

    // -- the schema ----------------------------------------------------------------------

    private static int field(FlatBuilder b, ArrowField f) {
        int[] kids = new int[f.children().size()];
        for (int i = 0; i < kids.length; i++) kids[i] = field(b, f.children().get(i));
        int kv = b.offsets(kids);
        int md = f.metadata().isEmpty() ? 0 : keyValues(b, f.metadata());
        int nameAt = b.string(f.name());
        int[] type = typeTable(b, f.type());
        b.startTable(7);
        b.fieldOffset(0, nameAt);
        b.fieldBool(1, f.nullable());
        b.fieldByte(2, type[0]);
        b.fieldOffset(3, type[1]);
        b.fieldOffset(5, kv);
        if (md != 0) b.fieldOffset(6, md);
        return b.endTable();
    }

    /** A vector of {@code KeyValue} tables, in the map's order. */
    private static int keyValues(FlatBuilder b, Map<String, String> metadata) {
        int[] entries = new int[metadata.size()];
        int i = 0;
        for (Map.Entry<String, String> e : metadata.entrySet()) {
            int key = b.string(e.getKey()), value = b.string(e.getValue());
            b.startTable(2);
            b.fieldOffset(0, key);
            b.fieldOffset(1, value);
            entries[i++] = b.endTable();
        }
        return b.offsets(entries);
    }

    /** {Arrow's Type union id, its table} for {@code t}. */
    private static int[] typeTable(FlatBuilder b, ArrowType t) {
        return switch (t) {
            case ArrowType.Null n -> empty(b, 1);
            case ArrowType.Int i -> {
                b.startTable(2);
                b.fieldInt(0, i.bits());
                b.fieldBool(1, i.signed());
                yield new int[] {2, b.endTable()};
            }
            case ArrowType.FloatingPoint f -> {
                b.startTable(1);
                b.fieldShort(0, f.precision().ordinal());
                yield new int[] {3, b.endTable()};
            }
            case ArrowType.Binary x -> empty(b, 4);
            case ArrowType.Utf8 x -> empty(b, 5);
            case ArrowType.Bool x -> empty(b, 6);
            case ArrowType.Decimal d -> {
                b.startTable(3);
                b.fieldInt(0, d.precision());
                b.fieldInt(1, d.scale());
                b.fieldInt(2, d.bits());
                yield new int[] {7, b.endTable()};
            }
            case ArrowType.Date d -> {
                b.startTable(1);
                b.fieldShort(0, d.unit().ordinal());
                yield new int[] {8, b.endTable()};
            }
            case ArrowType.Time x -> {
                b.startTable(2);
                b.fieldShort(0, x.unit().ordinal());
                b.fieldInt(1, x.bits());
                yield new int[] {9, b.endTable()};
            }
            case ArrowType.Timestamp ts -> {
                int tz = ts.timezone() == null ? 0 : b.string(ts.timezone());
                b.startTable(2);
                b.fieldShort(0, ts.unit().ordinal());
                if (tz != 0) b.fieldOffset(1, tz);
                yield new int[] {10, b.endTable()};
            }
            case ArrowType.Interval i -> {
                b.startTable(1);
                b.fieldShort(0, i.unit().ordinal());
                yield new int[] {11, b.endTable()};
            }
            case ArrowType.List x -> empty(b, 12);
            case ArrowType.Struct x -> empty(b, 13);
            case ArrowType.Union u -> {
                int[] ids = u.typeIds();
                int idv = ids == null ? 0 : b.ints(ids);
                b.startTable(2);
                b.fieldShort(0, u.dense() ? 1 : 0);
                if (idv != 0) b.fieldOffset(1, idv);
                yield new int[] {14, b.endTable()};
            }
            case ArrowType.FixedSizeList l -> {
                b.startTable(1);
                b.fieldInt(0, l.size());
                yield new int[] {16, b.endTable()};
            }
            case ArrowType.Map m -> {
                b.startTable(1);
                b.fieldBool(0, m.keysSorted());
                yield new int[] {17, b.endTable()};
            }
        };
    }

    private static int[] empty(FlatBuilder b, int id) {
        b.startTable(0);
        return new int[] {id, b.endTable()};
    }

    // -- framing -------------------------------------------------------------------------

    /** A Message table; {@code metadata}: a KeyValue vector's offset, or 0 for none. */
    private static byte[] message(FlatBuilder b, byte type, int header, long bodyLength, int metadata) {
        b.startTable(5);
        b.fieldLong(3, bodyLength);
        b.fieldOffset(2, header);
        if (metadata != 0) b.fieldOffset(4, metadata);
        b.fieldShort(0, V5);
        b.fieldByte(1, type);
        return b.finish(b.endTable());
    }

    /** An encapsulated message: continuation, metadata length, metadata padded to 8 bytes, then the body. */
    private static byte[] frame(byte[] metadata, byte[] body) {
        int padded = (int) pad8(8 + metadata.length) - 8;
        ByteBuffer head = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putInt(-1).putInt(padded);
        ByteArrayOutputStream out = new ByteArrayOutputStream(8 + padded + body.length);
        put(out, head.array());
        put(out, metadata);
        put(out, new byte[padded - metadata.length]);
        put(out, body);
        return out.toByteArray();
    }

    static long pad8(long n) {
        return (n + 7) & ~7L;
    }

    // -- buffers ---------------------------------------------------------------------------

    /** A validity bitmap of {@code n} values (bit set: present, least significant bit first), or no bytes at all
     *  when every value is present (a null count of zero needs no bitmap). */
    public static byte[] validity(int n, java.util.function.IntPredicate isNull) {
        byte[] bits = new byte[(n + 7) >>> 3];
        boolean any = false;
        for (int i = 0; i < n; i++) {
            if (isNull.test(i)) {
                any = true;
            } else {
                bits[i >>> 3] |= (byte) (1 << (i & 7));
            }
        }
        return any ? bits : new byte[0];
    }

    /** A bit per value, least significant bit first. */
    public static byte[] bits(int n, java.util.function.IntPredicate isSet) {
        byte[] bits = new byte[(n + 7) >>> 3];
        for (int i = 0; i < n; i++) {
            if (isSet.test(i)) bits[i >>> 3] |= (byte) (1 << (i & 7));
        }
        return bits;
    }

    public static byte[] int32s(int[] values, int n) {
        ByteBuffer b = ByteBuffer.allocate(4 * n).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < n; i++) b.putInt(values[i]);
        return b.array();
    }

    public static byte[] int64s(long[] values, int n) {
        ByteBuffer b = ByteBuffer.allocate(8 * n).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < n; i++) b.putLong(values[i]);
        return b.array();
    }

    public static byte[] float64s(double[] values, int n) {
        ByteBuffer b = ByteBuffer.allocate(8 * n).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < n; i++) b.putDouble(values[i]);
        return b.array();
    }

    /** 128-bit two's complement values, little-endian, 16 bytes each; a null slot's bytes are zero. */
    public static byte[] int128s(java.math.BigInteger[] values, int n) {
        byte[] out = new byte[16 * n];
        for (int i = 0; i < n; i++) {
            java.math.BigInteger v = values[i];
            if (v == null) continue;
            byte[] big = v.toByteArray();   // big-endian two's complement, as short as the value needs
            if (big.length > 16) {
                throw new IllegalArgumentException("a 128-bit value past 16 bytes: " + v);
            }
            for (int k = 0; k < big.length; k++) out[16 * i + k] = big[big.length - 1 - k];
            if (v.signum() < 0) {
                for (int k = big.length; k < 16; k++) out[16 * i + k] = -1;   // the sign extended
            }
        }
        return out;
    }

    /** Variable-length values: the offsets buffer ({@code n + 1} int32s) and the bytes; a null slot spans nothing. */
    public record Varying(byte[] offsets, byte[] data) {
    }

    public static Varying varying(byte[][] values, int n) {
        int total = 0;
        for (int i = 0; i < n; i++) if (values[i] != null) total += values[i].length;
        ByteBuffer offsets = ByteBuffer.allocate(4 * (n + 1)).order(ByteOrder.LITTLE_ENDIAN);
        byte[] data = new byte[total];
        int at = 0;
        offsets.putInt(0);
        for (int i = 0; i < n; i++) {
            byte[] v = values[i];
            if (v != null) {
                System.arraycopy(v, 0, data, at, v.length);
                at += v.length;
            }
            offsets.putInt(at);
        }
        return new Varying(offsets.array(), data);
    }

    /** {@code out.write(b)} whole: the WebAssembly tab's class library (TeaVM) has no
     *  {@code ByteArrayOutputStream.writeBytes} (track A, 2026-10-29, the l5 chain's PG group). */
    private static void put(java.io.ByteArrayOutputStream out, byte[] b) {
        out.write(b, 0, b.length);
    }
}
