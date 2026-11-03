// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.arrowipc;

import com.legend.base.Nullable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE ARROW IPC READER (the stream format, Arrow's {@code Message.fbs} and {@code Schema.fbs}): the twin of
 * {@link ArrowIpc}'s writer, no Arrow library (docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md R3, D8). It reads a stream a
 * host's database answered -- duckdb-python's, DuckDB-WASM's, a warehouse's -- into its schema and its record batches,
 * each column an {@link Array}: its field, its node (length, nulls) and its buffers as views over the stream's own
 * bytes, never copied (a native host hands the library its buffer where it lies). It knows the format and nothing of a
 * plan: what a column's values MEAN is read by the plan's columns, in {@code com.legend.result.ArrowRows}.
 *
 * <p>What it does not read is refused by name, never skipped (AGENTS.md invariant 4): a dictionary-encoded field, a
 * compressed batch, a big-endian stream, and the types whose layouts it does not lay out (a union,
 * a view, a run-end encoding, a half float, a large list).
 */
public final class ArrowReader {

    private ArrowReader() {
    }

    /** A stream's schema: its fields, in order, and its custom metadata. */
    public record Schema(List<ArrowField> fields, Map<String, String> metadata) {
        public Schema {
            fields = List.copyOf(fields);
            metadata = Collections.unmodifiableMap(new LinkedHashMap<>(metadata));
        }
    }

    /**
     * One column of a record batch, or one of its children: its field, how many values and how many of them are null,
     * its validity bitmap (null when no value is null), the buffers its type lays out after the validity (an
     * integer's values; a text's offsets, then its bytes; a list's offsets), each a little-endian view over the
     * stream's bytes, and its children (a list's element, a struct's fields, a map's entries).
     */
    public static final class Array {
        private final ArrowField field;
        private final int length;
        private final int nulls;
        private final @Nullable ByteBuffer validity;
        private final List<ByteBuffer> buffers;
        private final List<Array> children;

        Array(ArrowField field, int length, int nulls, @Nullable ByteBuffer validity, List<ByteBuffer> buffers,
                List<Array> children) {
            this.field = field;
            this.length = length;
            this.nulls = nulls;
            this.validity = validity;
            this.buffers = List.copyOf(buffers);
            this.children = List.copyOf(children);
        }

        public ArrowField field() {
            return field;
        }

        public ArrowType type() {
            return field.type();
        }

        public int length() {
            return length;
        }

        public int nulls() {
            return nulls;
        }

        /** Whether value {@code i} is null: its validity bit unset (a Null array's every value). */
        public boolean isNull(int i) {
            if (field.type() instanceof ArrowType.Null) {
                return true;
            }
            ByteBuffer v = validity;
            return v != null && (v.get(i >>> 3) & (1 << (i & 7))) == 0;
        }

        /** The type's {@code n}-th buffer after the validity. */
        public ByteBuffer buffer(int n) {
            return buffers.get(n);
        }

        public List<Array> children() {
            return children;
        }
    }

    /** One record batch: its rows, and one {@link Array} per schema field, in order. */
    public record Batch(int rows, List<Array> columns) {
        public Batch {
            columns = List.copyOf(columns);
        }
    }

    /** A stream being read: its schema read first, then each batch on {@link #next}. */
    public static final class Stream {
        private final ByteBuffer in;
        private final Schema schema;
        private int pos;
        private boolean ended;

        private Stream(ByteBuffer in) {
            this.in = in;
            this.pos = 0;
            Message first = message();
            if (first == null) {
                throw new IllegalArgumentException("an Arrow stream with no schema: it ends before its first message");
            }
            if (first.type != SCHEMA) {
                throw new IllegalArgumentException("an Arrow stream whose first message is not its schema (message type "
                        + first.type + ")");
            }
            this.schema = readSchema(first.header);
        }

        public Schema schema() {
            return schema;
        }

        /** The next record batch, or null at the stream's end. */
        public @Nullable Batch next() {
            if (ended) {
                return null;
            }
            Message m = message();
            if (m == null) {
                ended = true;
                return null;
            }
            return switch (m.type) {
                case RECORD_BATCH -> batch(m);
                case DICTIONARY_BATCH -> throw new IllegalArgumentException("an Arrow stream with a dictionary batch: "
                        + "a dictionary-encoded column is not read (its field names a dictionary)");
                case SCHEMA -> throw new IllegalArgumentException("an Arrow stream with a second schema message");
                default -> throw new IllegalArgumentException("an Arrow stream with a message of type " + m.type
                        + " (a tensor), which a result does not carry");
            };
        }

        /** The next encapsulated message, or null at the end marker (or the bytes' end). */
        private @Nullable Message message() {
            if (pos + 4 > in.limit()) {
                return null;
            }
            int first = in.getInt(pos);
            int metaLength;
            if (first == -1) {   // the continuation marker, then the metadata's length
                if (pos + 8 > in.limit()) {
                    throw new IllegalArgumentException("an Arrow stream cut short after a continuation marker at byte "
                            + pos);
                }
                metaLength = in.getInt(pos + 4);
                pos += 8;
            } else {             // the framing before format 1.0: the length alone
                metaLength = first;
                pos += 4;
            }
            if (metaLength == 0) {
                return null;
            }
            if (metaLength < 0 || pos + metaLength > in.limit()) {
                throw new IllegalArgumentException("an Arrow message's metadata of " + metaLength
                        + " bytes at byte " + pos + " runs past the stream's " + in.limit());
            }
            int meta = pos;
            Table message = Table.root(in, meta);
            byte type = message.byteField(1, (byte) 0);
            long bodyLength = message.longField(3, 0);
            int body = meta + metaLength;
            if (bodyLength < 0 || body + bodyLength > in.limit()) {
                throw new IllegalArgumentException("an Arrow message's body of " + bodyLength + " bytes at byte "
                        + body + " runs past the stream's " + in.limit());
            }
            pos = body + Math.toIntExact(bodyLength);
            return new Message(type, message.table(2), body);
        }

        private Batch batch(Message m) {
            Table rb = m.header;
            if (rb.has(3)) {
                throw new IllegalArgumentException("a compressed Arrow record batch: its buffers are not read "
                        + "(the stream is written without compression)");
            }
            int rows = Math.toIntExact(rb.longField(0, 0));
            Cursor cursor = new Cursor(in, rb.vector(1), rb.vector(2), m.body);
            List<Array> columns = new ArrayList<>(schema.fields().size());
            for (ArrowField f : schema.fields()) {
                columns.add(array(cursor, f));
            }
            return new Batch(rows, columns);
        }
    }

    /**
     * {@code stream}'s schema read; its batches on {@link Stream#next}. {@code stream}'s bytes are read in place (its
     * position and limit bound them), never copied. The stream format, or the FILE format (DuckDB-WASM's
     * {@code runQuery} answers one): the magic {@code ARROW1} and its padding, the stream's own messages, then a footer
     * that indexes them, its length and the magic again -- read as the stream it holds, between the leading magic and
     * the footer (the footer's index is not needed to read the messages in order).
     */
    public static Stream open(ByteBuffer stream) {
        ByteBuffer in = stream.slice().order(ByteOrder.LITTLE_ENDIAN);
        if (magicAt(in, 0)) {
            int end = in.limit();
            if (end < 8 + 10 || !magicAt(in, end - 6)) {
                throw new IllegalArgumentException("an Arrow file (its magic ARROW1) of " + end + " bytes that does not"
                        + " end with its footer and the magic");
            }
            int footer = in.getInt(end - 10);
            int messagesEnd = end - 10 - footer;
            if (footer < 0 || messagesEnd < 8) {
                throw new IllegalArgumentException("an Arrow file whose footer of " + footer + " bytes does not fit its "
                        + end);
            }
            ByteBuffer messages = in.duplicate();
            messages.limit(messagesEnd);
            messages.position(8);
            return new Stream(messages.slice().order(ByteOrder.LITTLE_ENDIAN));
        }
        return new Stream(in);
    }

    /** Whether {@code in} holds the file format's magic {@code ARROW1} at {@code at}. */
    private static boolean magicAt(ByteBuffer in, int at) {
        byte[] magic = {'A', 'R', 'R', 'O', 'W', '1'};
        if (at < 0 || at + magic.length > in.limit()) {
            return false;
        }
        for (int i = 0; i < magic.length; i++) {
            if (in.get(at + i) != magic[i]) {
                return false;
            }
        }
        return true;
    }

    // -- the format's constants ----------------------------------------------------------

    private static final byte SCHEMA = 1;
    private static final byte DICTIONARY_BATCH = 2;
    private static final byte RECORD_BATCH = 3;

    private record Message(byte type, Table header, int body) {
    }

    // -- the schema ----------------------------------------------------------------------

    private static Schema readSchema(Table s) {
        if (s.shortField(0, (short) 0) != 0) {
            throw new IllegalArgumentException("a big-endian Arrow stream: its values are not read");
        }
        List<ArrowField> fields = new ArrayList<>();
        int fv = s.vector(1);
        if (fv >= 0) {
            int n = s.in.getInt(fv);
            for (int i = 0; i < n; i++) {
                fields.add(field(Table.at(s.in, fv + 4 + 4 * i + s.in.getInt(fv + 4 + 4 * i))));
            }
        }
        return new Schema(fields, keyValues(s, 2));
    }

    private static ArrowField field(Table f) {
        String name = f.string(0);
        boolean nullable = f.boolField(1, false);
        if (f.has(4)) {
            throw new IllegalArgumentException("the Arrow field '" + name + "' is dictionary-encoded: its values are "
                    + "not read");
        }
        int typeId = Byte.toUnsignedInt(f.byteField(2, (byte) 0));
        Table type = f.has(3) ? f.table(3) : null;
        List<ArrowField> children = new ArrayList<>();
        int cv = f.vector(5);
        if (cv >= 0) {
            int n = f.in.getInt(cv);
            for (int i = 0; i < n; i++) {
                children.add(field(Table.at(f.in, cv + 4 + 4 * i + f.in.getInt(cv + 4 + 4 * i))));
            }
        }
        return new ArrowField(name, type(name, typeId, type), nullable, children, keyValues(f, 6));
    }

    /** Arrow's {@code Type} union member {@code id} with its table, as an {@link ArrowType}. */
    private static ArrowType type(String field, int id, @Nullable Table t) {
        return switch (id) {
            case 1 -> new ArrowType.Null();
            case 2 -> new ArrowType.Int(need(field, t).intField(0, 0), need(field, t).boolField(1, false));
            case 3 -> {
                int precision = need(field, t).shortField(0, (short) 0);
                if (precision == 0) {
                    throw new IllegalArgumentException("the Arrow field '" + field + "' is a half-precision float: "
                            + "its values are not read");
                }
                yield new ArrowType.FloatingPoint(ArrowType.FloatPrecision.values()[precision]);
            }
            case 4 -> new ArrowType.Binary();
            case 5 -> new ArrowType.Utf8();
            case 6 -> new ArrowType.Bool();
            case 7 -> {
                Table d = need(field, t);
                int bits = d.intField(2, 128);
                if (bits != 128 && bits != 256) {
                    throw new IllegalArgumentException("the Arrow field '" + field + "' is a decimal of " + bits
                            + " bits: its values are not read");
                }
                yield new ArrowType.Decimal(d.intField(0, 0), d.intField(1, 0), bits);
            }
            case 8 -> new ArrowType.Date(ArrowType.DateUnit.values()[t == null ? 1 : t.shortField(0, (short) 1)]);
            case 9 -> new ArrowType.Time(ArrowType.TimeUnit.values()[t == null ? 1 : t.shortField(0, (short) 1)],
                    t == null ? 32 : t.intField(1, 32));
            case 10 -> {
                Table ts = need(field, t);
                yield new ArrowType.Timestamp(ArrowType.TimeUnit.values()[ts.shortField(0, (short) 0)],
                        ts.has(1) ? ts.string(1) : null);
            }
            case 12 -> new ArrowType.List();
            case 13 -> new ArrowType.Struct();
            case 16 -> new ArrowType.FixedSizeList(need(field, t).intField(0, 0));
            case 17 -> new ArrowType.Map(t != null && t.boolField(0, false));
            case 20 -> new ArrowType.LargeUtf8();
            case 19 -> new ArrowType.LargeBinary();
            default -> throw new IllegalArgumentException("the Arrow field '" + field + "' is of a type whose layout "
                    + "is not read: " + typeName(id));
        };
    }

    private static String typeName(int id) {
        return switch (id) {
            case 0 -> "none";
            case 11 -> "Interval";
            case 14 -> "Union";
            case 15 -> "FixedSizeBinary";
            case 18 -> "Duration";
            case 21 -> "LargeList";
            case 22 -> "RunEndEncoded";
            case 23 -> "BinaryView";
            case 24 -> "Utf8View";
            case 25 -> "ListView";
            case 26 -> "LargeListView";
            default -> "type " + id;
        };
    }

    private static Table need(String field, @Nullable Table t) {
        if (t == null) {
            throw new IllegalArgumentException("the Arrow field '" + field + "' has no type table");
        }
        return t;
    }

    private static Map<String, String> keyValues(Table t, int index) {
        Map<String, String> out = new LinkedHashMap<>();
        int kv = t.vector(index);
        if (kv >= 0) {
            int n = t.in.getInt(kv);
            for (int i = 0; i < n; i++) {
                Table pair = Table.at(t.in, kv + 4 + 4 * i + t.in.getInt(kv + 4 + 4 * i));
                out.put(pair.string(0), pair.string(1));
            }
        }
        return out;
    }

    // -- a batch's arrays ----------------------------------------------------------------

    /** One array and its children, its nodes and buffers taken in the format's order (depth first). */
    private static Array array(Cursor cursor, ArrowField f) {
        long[] node = cursor.node(f);
        int length = Math.toIntExact(node[0]);
        int nulls = Math.toIntExact(node[1]);
        ArrowType t = f.type();
        if (t instanceof ArrowType.Null) {
            // a Null array lays out no buffer at all, not even a validity bitmap
            return new Array(f, length, length, null, List.of(), List.of());
        }
        ByteBuffer validity = cursor.buffer(f);
        ByteBuffer kept = nulls == 0 || validity.limit() == 0 ? null : validity;
        if (nulls > 0 && validity.limit() * 8L < length) {
            throw new IllegalArgumentException("the Arrow column '" + f.name() + "' has " + nulls + " nulls and a "
                    + "validity bitmap of " + validity.limit() + " bytes for " + length + " values");
        }
        List<ByteBuffer> buffers = new ArrayList<>(2);
        List<Array> children = new ArrayList<>();
        switch (t) {
            case ArrowType.Null n -> {
            }
            case ArrowType.Int i -> buffers.add(cursor.buffer(f));
            case ArrowType.FloatingPoint p -> buffers.add(cursor.buffer(f));
            case ArrowType.Bool b -> buffers.add(cursor.buffer(f));
            case ArrowType.Decimal d -> buffers.add(cursor.buffer(f));
            case ArrowType.Date d -> buffers.add(cursor.buffer(f));
            case ArrowType.Time x -> buffers.add(cursor.buffer(f));
            case ArrowType.Timestamp x -> buffers.add(cursor.buffer(f));
            case ArrowType.Utf8 u -> {
                buffers.add(cursor.buffer(f));
                buffers.add(cursor.buffer(f));
            }
            case ArrowType.Binary u -> {
                buffers.add(cursor.buffer(f));
                buffers.add(cursor.buffer(f));
            }
            case ArrowType.LargeUtf8 u -> {
                buffers.add(cursor.buffer(f));
                buffers.add(cursor.buffer(f));
            }
            case ArrowType.LargeBinary u -> {
                buffers.add(cursor.buffer(f));
                buffers.add(cursor.buffer(f));
            }
            case ArrowType.List l -> {
                buffers.add(cursor.buffer(f));
                children.add(array(cursor, only(f)));
            }
            case ArrowType.Map m -> {
                buffers.add(cursor.buffer(f));
                children.add(array(cursor, only(f)));
            }
            case ArrowType.FixedSizeList l -> children.add(array(cursor, only(f)));
            case ArrowType.Struct s -> {
                for (ArrowField c : f.children()) {
                    children.add(array(cursor, c));
                }
            }
            case ArrowType.Interval x -> throw unread(f);
            case ArrowType.Union x -> throw unread(f);
        }
        return new Array(f, length, nulls, kept, buffers, children);
    }

    private static ArrowField only(ArrowField f) {
        if (f.children().size() != 1) {
            throw new IllegalArgumentException("the Arrow column '" + f.name() + "' (" + f.type()
                    + ") has " + f.children().size() + " children, not one");
        }
        return f.children().get(0);
    }

    private static IllegalArgumentException unread(ArrowField f) {
        return new IllegalArgumentException("the Arrow column '" + f.name() + "' is of a type whose layout is not "
                + "read: " + f.type());
    }

    /** A batch's field nodes and buffers, taken in order. */
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

        /** The next node: {length, nulls} (struct FieldNode { long length; long null_count; }). */
        long[] node(ArrowField f) {
            if (nodes < 0 || nextNode >= in.getInt(nodes)) {
                throw new IllegalArgumentException("an Arrow record batch with fewer nodes than its schema's arrays "
                        + "(at the column '" + f.name() + "')");
            }
            int at = nodes + 4 + 16 * nextNode++;
            return new long[] {in.getLong(at), in.getLong(at + 8)};
        }

        /** The next buffer (struct Buffer { long offset; long length; }), a little-endian view of the body. */
        ByteBuffer buffer(ArrowField f) {
            if (buffers < 0 || nextBuffer >= in.getInt(buffers)) {
                throw new IllegalArgumentException("an Arrow record batch with fewer buffers than its schema's arrays "
                        + "lay out (at the column '" + f.name() + "')");
            }
            int at = buffers + 4 + 16 * nextBuffer++;
            long offset = in.getLong(at);
            long length = in.getLong(at + 8);
            long from = body + offset;
            if (offset < 0 || length < 0 || from + length > in.limit()) {
                throw new IllegalArgumentException("an Arrow buffer of the column '" + f.name() + "' runs past the "
                        + "stream (" + length + " bytes at " + from + " of " + in.limit() + ")");
            }
            // duplicate and bound, then slice: TeaVM's class library has no slice(index, length)
            ByteBuffer view = in.duplicate();
            view.limit(Math.toIntExact(from + length));
            view.position(Math.toIntExact(from));
            return view.slice().order(ByteOrder.LITTLE_ENDIAN);
        }
    }

    /** A FlatBuffers table: its fields found through its vtable. */
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
            if (slot >= vtableSize) {
                return -1;
            }
            int offset = Short.toUnsignedInt(in.getShort(vtable + slot));
            return offset == 0 ? -1 : pos + offset;
        }

        boolean has(int index) {
            return field(index) >= 0;
        }

        byte byteField(int index, byte absent) {
            int f = field(index);
            return f < 0 ? absent : in.get(f);
        }

        boolean boolField(int index, boolean absent) {
            int f = field(index);
            return f < 0 ? absent : in.get(f) != 0;
        }

        short shortField(int index, short absent) {
            int f = field(index);
            return f < 0 ? absent : in.getShort(f);
        }

        int intField(int index, int absent) {
            int f = field(index);
            return f < 0 ? absent : in.getInt(f);
        }

        long longField(int index, long absent) {
            int f = field(index);
            return f < 0 ? absent : in.getLong(f);
        }

        Table table(int index) {
            int f = field(index);
            if (f < 0) {
                throw new IllegalArgumentException("an Arrow message without its header");
            }
            return new Table(in, f + in.getInt(f));
        }

        /** A vector's position (its length first), or -1 when absent. */
        int vector(int index) {
            int f = field(index);
            return f < 0 ? -1 : f + in.getInt(f);
        }

        String string(int index) {
            int at = vector(index);
            if (at < 0) {
                return "";
            }
            byte[] bytes = new byte[in.getInt(at)];
            in.get(at + 4, bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }
}
