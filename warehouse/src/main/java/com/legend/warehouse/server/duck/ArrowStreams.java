package com.legend.warehouse.server.duck;

import com.legend.warehouse.sqlapi.DuckType;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * A result's chunks as Arrow IPC streams: each chunk a whole stream (the schema,
 * one or more record batches, the end marker), so any Arrow reader reads any
 * chunk alone. DuckDB builds the buffers (duckdb_data_chunk_to_arrow); this
 * frames them with {@link FlatBuilder}. The type decisions are ours
 * (docs/WAREHOUSE_FFM_HOMEWORK_2026_09_26.md §3): UHUGEINT as its decimal text
 * (no signed 128-bit integer holds it), ENUM as text (not a dictionary),
 * TIMESTAMP WITH TIME ZONE tagged UTC; everything else as DuckDB writes it.
 * A chunk holds whole 2,048-row batches: at least the rows asked for, but the last.
 */
public final class ArrowStreams {

    public static final String MEDIA_TYPE = "application/vnd.apache.arrow.stream";

    private static final short V5 = 4;
    private static final byte SCHEMA = 1;
    private static final byte RECORD_BATCH = 3;

    private final byte[] schema;
    private ByteArrayOutputStream current = new ByteArrayOutputStream();
    private long rows;

    ArrowStreams(List<String> names, List<TypeTree> trees) {
        FlatBuilder b = new FlatBuilder();
        int[] fields = new int[trees.size()];
        for (int i = 0; i < fields.length; i++) fields[i] = field(b, names.get(i), trees.get(i).type, true);
        int fv = b.offsets(fields);
        b.startTable(4);
        b.fieldShort(0, 0);   // little-endian
        b.fieldOffset(1, fv);
        this.schema = frame(message(b, SCHEMA, b.endTable(), 0), new byte[0]);
        current.writeBytes(schema);
    }

    /** One chunk's columns as a record batch of the current stream. */
    void add(List<ColumnData> columns, int count) {
        List<long[]> nodes = new ArrayList<>();
        List<byte[]> buffers = new ArrayList<>();
        for (ColumnData c : columns) layout(c, nodes, buffers);
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        long[] offsets = new long[buffers.size()], lengths = new long[buffers.size()];
        for (int i = 0; i < buffers.size(); i++) {
            offsets[i] = body.size();
            lengths[i] = buffers.get(i).length;
            body.writeBytes(buffers.get(i));
            body.writeBytes(new byte[(int) (pad8(lengths[i]) - lengths[i])]);
        }
        long[] nl = new long[nodes.size()], nn = new long[nodes.size()];
        for (int i = 0; i < nodes.size(); i++) {
            nl[i] = nodes.get(i)[0];
            nn[i] = nodes.get(i)[1];
        }
        FlatBuilder b = new FlatBuilder();
        int nv = b.longPairs(nl, nn), bv = b.longPairs(offsets, lengths);
        b.startTable(5);
        b.fieldLong(0, count);
        b.fieldOffset(1, nv);
        b.fieldOffset(2, bv);
        int batch = b.endTable();
        current.writeBytes(frame(message(b, RECORD_BATCH, batch, body.size()), body.toByteArray()));
        rows += count;
    }

    /** Rows in the stream being built. */
    long rows() {
        return rows;
    }

    /** The stream so far, ended; the next begins with the schema again. */
    byte[] flush() {
        current.writeBytes(new byte[] {-1, -1, -1, -1, 0, 0, 0, 0});
        byte[] out = current.toByteArray();
        current = new ByteArrayOutputStream();
        current.writeBytes(schema);
        rows = 0;
        return out;
    }

    // -- the schema ----------------------------------------------------------------------

    private static int field(FlatBuilder b, String name, DuckType t, boolean nullable) {
        int[] kids = switch (t) {
            case DuckType.ListOf l -> new int[] {field(b, "item", l.element(), true)};
            case DuckType.StructOf s -> {
                int[] k = new int[s.fields().size()];
                for (int i = 0; i < k.length; i++) k[i] = field(b, s.fields().get(i).name(), s.fields().get(i).type(), true);
                yield k;
            }
            case DuckType.MapOf m -> {
                int key = field(b, "key", m.key(), false), value = field(b, "value", m.value(), true);
                int ev = b.offsets(new int[] {key, value});
                int en = b.string("entries");
                b.startTable(0);
                int st = b.endTable();
                b.startTable(7);
                b.fieldOffset(0, en);
                b.fieldBool(1, false);
                b.fieldByte(2, 13);
                b.fieldOffset(3, st);
                b.fieldOffset(5, ev);
                yield new int[] {b.endTable()};
            }
            case DuckType.Scalar s -> new int[0];
        };
        int kv = b.offsets(kids);
        int nameAt = b.string(name);
        int[] type = typeTable(b, t);
        b.startTable(7);
        b.fieldOffset(0, nameAt);
        b.fieldBool(1, nullable);
        b.fieldByte(2, type[0]);
        b.fieldOffset(3, type[1]);
        b.fieldOffset(5, kv);
        return b.endTable();
    }

    /** {Arrow's Type union id, its table} for a DuckDB type. */
    private static int[] typeTable(FlatBuilder b, DuckType t) {
        if (t instanceof DuckType.ListOf l) {
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("\\[(\\d+)]$").matcher(l.name());
            if (m.find()) {
                b.startTable(1);
                b.fieldInt(0, Integer.parseInt(m.group(1)));
                return new int[] {16, b.endTable()};   // FixedSizeList
            }
            b.startTable(0);
            return new int[] {12, b.endTable()};       // List
        }
        if (t instanceof DuckType.StructOf) {
            b.startTable(0);
            return new int[] {13, b.endTable()};
        }
        if (t instanceof DuckType.MapOf) {
            b.startTable(1);
            b.fieldBool(0, false);   // keysSorted
            return new int[] {17, b.endTable()};
        }
        String base = ((DuckType.Scalar) t).base();
        int tz = base.equals("TIMESTAMP WITH TIME ZONE") ? b.string("UTC") : 0;
        int id;
        switch (base) {
            case "BOOLEAN" -> { id = 6; b.startTable(0); }
            case "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT" -> {
                id = 2;
                b.startTable(2);
                b.fieldInt(0, 8 * ColumnData.width(base));
                b.fieldBool(1, !base.startsWith("U"));
            }
            case "FLOAT", "DOUBLE" -> { id = 3; b.startTable(1); b.fieldShort(0, base.equals("FLOAT") ? 1 : 2); }
            case "DECIMAL", "HUGEINT" -> {
                id = 7;
                int[] ps = base.equals("HUGEINT") ? new int[] {38, 0} : decimal(((DuckType.Scalar) t).name());
                b.startTable(3);
                b.fieldInt(0, ps[0]);
                b.fieldInt(1, ps[1]);
                b.fieldInt(2, 128);
            }
            case "BLOB" -> { id = 4; b.startTable(0); }
            case "DATE" -> { id = 8; b.startTable(1); b.fieldShort(0, 0); }
            case "TIME" -> { id = 9; b.startTable(2); b.fieldShort(0, 2); b.fieldInt(1, 64); }
            case "INTERVAL" -> { id = 11; b.startTable(1); b.fieldShort(0, 2); }   // MONTH_DAY_NANO
            case "TIMESTAMP", "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS", "TIMESTAMP WITH TIME ZONE" -> {
                id = 10;
                b.startTable(2);
                b.fieldShort(0, switch (base) {
                    case "TIMESTAMP_S" -> 0;
                    case "TIMESTAMP_MS" -> 1;
                    case "TIMESTAMP_NS" -> 3;
                    default -> 2;
                });
                if (tz != 0) b.fieldOffset(1, tz);
            }
            default -> { id = 5; b.startTable(0); }   // VARCHAR, JSON, UUID, ENUM, UHUGEINT: text
        }
        return new int[] {id, b.endTable()};
    }

    private static int[] decimal(String name) {
        int open = name.indexOf('('), comma = name.indexOf(',', open), close = name.indexOf(')', comma);
        return new int[] {Integer.parseInt(name.substring(open + 1, comma).strip()),
                Integer.parseInt(name.substring(comma + 1, close).strip())};
    }

    // -- a batch's body ------------------------------------------------------------------

    /** The column's nodes and buffers, depth first, as a record batch lists them. */
    private static void layout(ColumnData c, List<long[]> nodes, List<byte[]> buffers) {
        nodes.add(new long[] {c.length, c.nulls});
        byte[] v = c.validity;
        buffers.add(v == null ? new byte[0] : v);
        switch (c.tree.type) {
            case DuckType.ListOf l -> {
                if (c.offsets != null) buffers.add(ints(c.offsets));
                layout(c.children.get(0), nodes, buffers);
            }
            case DuckType.StructOf s -> {
                for (ColumnData k : c.children) layout(k, nodes, buffers);
            }
            case DuckType.MapOf m -> {
                buffers.add(ints(java.util.Objects.requireNonNull(c.offsets)));
                ColumnData key = c.children.get(0);
                nodes.add(new long[] {key.length, 0});   // the entries struct: never null
                buffers.add(new byte[0]);
                layout(key, nodes, buffers);
                layout(c.children.get(1), nodes, buffers);
            }
            case DuckType.Scalar s -> {
                switch (s.base()) {
                    case "VARCHAR", "JSON", "UUID", "ENUM", "BLOB" -> {
                        buffers.add(ints(java.util.Objects.requireNonNull(c.offsets)));
                        buffers.add(c.data);
                    }
                    case "UHUGEINT" -> uhugeintText(c, buffers);
                    default -> buffers.add(bytes(c.values));
                }
            }
        }
    }

    private static void uhugeintText(ColumnData c, List<byte[]> buffers) {
        ByteBuffer offs = ByteBuffer.allocate(4 * (c.length + 1)).order(ByteOrder.LITTLE_ENDIAN);
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        offs.putInt(0);
        for (int i = 0; i < c.length; i++) {
            if (c.present(i)) data.writeBytes(JsonCells.int128(c.values, i, false).toString().getBytes(StandardCharsets.UTF_8));
            offs.putInt(data.size());
        }
        buffers.add(offs.array());
        buffers.add(data.toByteArray());
    }

    private static byte[] ints(int[] a) {
        ByteBuffer b = ByteBuffer.allocate(4 * a.length).order(ByteOrder.LITTLE_ENDIAN);
        for (int x : a) b.putInt(x);
        return b.array();
    }

    private static byte[] bytes(ByteBuffer b) {
        return b.array();
    }

    // -- framing -------------------------------------------------------------------------

    private static byte[] message(FlatBuilder b, byte type, int header, long bodyLength) {
        b.startTable(5);
        b.fieldLong(3, bodyLength);
        b.fieldOffset(2, header);
        b.fieldShort(0, V5);
        b.fieldByte(1, type);
        return b.finish(b.endTable());
    }

    /** An encapsulated message: continuation, metadata length, metadata padded to 8 bytes, then the body. */
    private static byte[] frame(byte[] metadata, byte[] body) {
        int padded = (int) pad8(8 + metadata.length) - 8;
        ByteBuffer head = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putInt(-1).putInt(padded);
        ByteArrayOutputStream out = new ByteArrayOutputStream(8 + padded + body.length);
        out.writeBytes(head.array());
        out.writeBytes(metadata);
        out.writeBytes(new byte[padded - metadata.length]);
        out.writeBytes(body);
        return out.toByteArray();
    }

    private static long pad8(long n) {
        return (n + 7) & ~7L;
    }
}
