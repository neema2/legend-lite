package com.legend.warehouse.sqlapi;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An Arrow chunk from the warehouse, read back into {@link Columnar}s (plain Java: it compiles into
 * the WebAssembly module too). An Arrow IPC stream is messages, each a FlatBuffer header and a body;
 * a record batch's nodes and buffers are laid out depth first, as the warehouse writes them for the
 * API's column types, so the result's column types (the API's metadata) are all the reader needs to
 * read them. The schema message is skipped.
 */
public final class ArrowIpcReader {

    private ArrowIpcReader() {
    }

    /** One record batch: its rows, its columns, and its metadata (key to value). */
    public record Batch(int rows, List<Columnar> columns, Map<String, String> metadata) {
    }

    private static final byte SCHEMA = 1;
    private static final byte RECORD_BATCH = 3;

    public static List<Batch> read(byte[] stream, List<DuckType> types) {
        ByteBuffer in = ByteBuffer.wrap(stream).order(ByteOrder.LITTLE_ENDIAN);
        List<Batch> out = new ArrayList<>();
        int pos = 0;
        while (pos + 8 <= stream.length) {
            int first = in.getInt(pos);
            int metaLength;
            if (first == -1) {   // continuation marker, then the length
                metaLength = in.getInt(pos + 4);
                pos += 8;
            } else {             // the pre-1.0 framing: the length alone
                metaLength = first;
                pos += 4;
            }
            if (metaLength == 0) break;   // end of stream
            int meta = pos;
            Table message = Table.root(in, meta);
            byte type = message.byteField(1);
            long bodyLength = message.longField(3);
            int body = meta + metaLength;
            if (type == RECORD_BATCH) {
                out.add(batch(in, message, body, types));
            } else if (type != SCHEMA) {
                throw new IllegalArgumentException("an Arrow message the warehouse does not send: type " + type);
            }
            pos = body + Math.toIntExact(bodyLength);
        }
        return out;
    }

    private static Batch batch(ByteBuffer in, Table message, int body, List<DuckType> types) {
        Table rb = message.table(2);
        int rows = Math.toIntExact(rb.longField(0));
        Cursor cur = new Cursor(in, rb.vector(1), rb.vector(2), body);
        List<Columnar> cols = new ArrayList<>(types.size());
        for (DuckType t : types) cols.add(column(cur, t));
        Map<String, String> metadata = new LinkedHashMap<>();
        int kv = message.vector(4);
        if (kv >= 0) {
            int n = in.getInt(kv);
            for (int i = 0; i < n; i++) {
                Table pair = Table.at(in, kv + 4 + 4 * i + in.getInt(kv + 4 + 4 * i));
                metadata.put(pair.string(0), pair.string(1));
            }
        }
        return new Batch(rows, cols, metadata);
    }

    /** One column and its children, consuming nodes and buffers in the writer's order. */
    private static Columnar column(Cursor cur, DuckType t) {
        long[] node = cur.node();
        int length = Math.toIntExact(node[0]);
        int nulls = Math.toIntExact(node[1]);
        byte[] validity = cur.buffer();
        byte[] v = nulls == 0 ? null : validity;
        ByteBuffer none = ByteBuffer.allocate(0).order(ByteOrder.LITTLE_ENDIAN);
        return switch (t) {
            case DuckType.ListOf l -> Columnar.fixedSize(t) > 0
                    ? new Columnar(t, length, nulls, v, none, null, new byte[0], List.of(column(cur, l.element())), false)
                    : new Columnar(t, length, nulls, v, none, ints(cur.buffer()), new byte[0], List.of(column(cur, l.element())), false);
            case DuckType.StructOf s -> {
                List<Columnar> kids = new ArrayList<>();
                for (DuckType.Field f : s.fields()) kids.add(column(cur, f.type()));
                yield new Columnar(t, length, nulls, v, none, null, new byte[0], kids, false);
            }
            case DuckType.MapOf m -> {
                int[] offsets = ints(cur.buffer());
                cur.node();      // the entries struct: never null
                cur.buffer();    // its (empty) validity
                Columnar key = column(cur, m.key());
                Columnar value = column(cur, m.value());
                yield new Columnar(t, length, nulls, v, none, offsets, new byte[0], List.of(key, value), false);
            }
            case DuckType.Scalar s -> switch (s.base()) {
                case "VARCHAR", "JSON", "UUID", "ENUM", "BLOB", "UHUGEINT" -> {
                    int[] offsets = ints(cur.buffer());
                    yield new Columnar(t, length, nulls, v, none, offsets, cur.buffer(), List.of(), s.base().equals("UHUGEINT"));
                }
                default -> new Columnar(t, length, nulls, v, le(cur.buffer()), null, new byte[0], List.of(), false);
            };
        };
    }

    private static int[] ints(byte[] b) {
        int[] out = new int[b.length / 4];
        le(b).asIntBuffer().get(out);
        return out;
    }

    private static ByteBuffer le(byte[] b) {
        return ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN);
    }

    /** The batch's field nodes and buffers, taken in order. */
    private static final class Cursor {
        private final ByteBuffer in;
        private final int nodes;
        private final int buffers;
        private final int body;
        private int nextNode;
        private int nextBuffer;

        Cursor(ByteBuffer in, int nodes, int buffers, int body) {
            this.in = in;
            this.nodes = nodes;
            this.buffers = buffers;
            this.body = body;
        }

        long[] node() {
            int at = nodes + 4 + 16 * nextNode++;   // struct FieldNode { long length; long null_count; }
            return new long[] {in.getLong(at), in.getLong(at + 8)};
        }

        byte[] buffer() {
            int at = buffers + 4 + 16 * nextBuffer++;   // struct Buffer { long offset; long length; }
            int from = body + Math.toIntExact(in.getLong(at));
            return Arrays.copyOfRange(in.array(), from, from + Math.toIntExact(in.getLong(at + 8)));
        }
    }

    /** A FlatBuffers table: fields found through its vtable. */
    private static final class Table {
        private final ByteBuffer in;
        private final int pos;
        private final int vtable;

        private Table(ByteBuffer in, int pos) {
            this.in = in;
            this.pos = pos;
            this.vtable = pos - in.getInt(pos);
        }

        static Table root(ByteBuffer in, int start) {
            return new Table(in, start + in.getInt(start));
        }

        static Table at(ByteBuffer in, int pos) {
            return new Table(in, pos);
        }

        /** The field's position, or -1 when absent. */
        private int field(int index) {
            int vtableSize = Short.toUnsignedInt(in.getShort(vtable));
            int slot = 4 + 2 * index;
            if (slot >= vtableSize) return -1;
            int offset = Short.toUnsignedInt(in.getShort(vtable + slot));
            return offset == 0 ? -1 : pos + offset;
        }

        byte byteField(int index) {
            int f = field(index);
            return f < 0 ? 0 : in.get(f);
        }

        long longField(int index) {
            int f = field(index);
            return f < 0 ? 0 : in.getLong(f);
        }

        Table table(int index) {
            int f = field(index);
            if (f < 0) throw new IllegalArgumentException("an Arrow message without its header");
            return new Table(in, f + in.getInt(f));
        }

        /** A vector's position (its length first), or -1 when absent. */
        int vector(int index) {
            int f = field(index);
            return f < 0 ? -1 : f + in.getInt(f);
        }

        String string(int index) {
            int at = vector(index);
            if (at < 0) return "";
            return new String(in.array(), at + 4, in.getInt(at), StandardCharsets.UTF_8);
        }
    }
}
