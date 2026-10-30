package com.legend.warehouse.server.duck;

import com.legend.arrowipc.ArrowField;
import com.legend.arrowipc.ArrowIpc;
import com.legend.arrowipc.ArrowType;
import com.legend.json.Json;
import com.legend.warehouse.sqlapi.ApiValues;
import com.legend.warehouse.sqlapi.Columnar;
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
 * lays them out and the shared framer writes the messages ({@link ArrowIpc}, {@code //arrow_ipc}: the warehouse's
 * own FlatBuffers framer, moved out 2026-10-29 so core's writers frame with it too -- the result boundary program's
 * track A). The type decisions are ours
 * (docs/WAREHOUSE_FFM_HOMEWORK_2026_09_26.md §3): UHUGEINT as its decimal text
 * (no signed 128-bit integer holds it), ENUM as text (not a dictionary),
 * TIMESTAMP WITH TIME ZONE tagged UTC; everything else as DuckDB writes it.
 * A chunk holds whole 2,048-row batches: at least the rows asked for, but the last.
 */
public final class ArrowStreams {

    public static final String MEDIA_TYPE = ArrowIpc.MEDIA_TYPE;

    private final byte[] schema;
    private ByteArrayOutputStream current = new ByteArrayOutputStream();
    private long rows;

    ArrowStreams(List<String> names, List<TypeTree> trees) {
        List<ArrowField> fields = new ArrayList<>(trees.size());
        for (int i = 0; i < trees.size(); i++) fields.add(field(names.get(i), trees.get(i).type, true));
        this.schema = ArrowIpc.schema(fields, java.util.Map.of());
        current.writeBytes(schema);
    }

    /** The batch metadata key under which a batch carries DuckDB's text for its nested cells. */
    public static final String CELL_TEXT = "legend.cell_text";

    /**
     * One chunk's columns as a record batch of the current stream; {@code texts} (column index to each
     * row's text, empty for none) goes in the batch's metadata as JSON, under {@link #CELL_TEXT}.
     */
    void add(List<Columnar> columns, int count, java.util.Map<Integer, List<String>> texts) {
        ArrowIpc.Layout layout = new ArrowIpc.Layout();
        for (Columnar c : columns) layout(c, layout);
        java.util.Map<String, String> metadata = java.util.Map.of();
        if (!texts.isEmpty()) {
            Json.Writer w = Json.compactWriter();
            w.beginObject();
            for (var e : texts.entrySet()) {
                w.name(Integer.toString(e.getKey()));
                w.beginArray();
                for (String t : e.getValue()) w.writeString(t);
                w.endArray();
            }
            w.endObject();
            metadata = java.util.Map.of(CELL_TEXT, w.toString());
        }
        current.writeBytes(ArrowIpc.recordBatch(count, layout, metadata));
        rows += count;
    }

    /** Rows in the stream being built. */
    long rows() {
        return rows;
    }

    /** The stream so far, ended; the next begins with the schema again. */
    byte[] flush() {
        current.writeBytes(ArrowIpc.END);
        byte[] out = current.toByteArray();
        current = new ByteArrayOutputStream();
        current.writeBytes(schema);
        rows = 0;
        return out;
    }

    // -- the schema ----------------------------------------------------------------------

    /** A column's field: its DuckDB type as the Arrow type and children {@link ArrowIpc} writes. */
    private static ArrowField field(String name, DuckType t, boolean nullable) {
        List<ArrowField> kids = switch (t) {
            case DuckType.ListOf l -> List.of(field("item", l.element(), true));
            case DuckType.StructOf s -> {
                List<ArrowField> k = new ArrayList<>(s.fields().size());
                for (DuckType.Field f : s.fields()) k.add(field(f.name(), f.type(), true));
                yield k;
            }
            case DuckType.MapOf m -> List.of(new ArrowField("entries", new ArrowType.Struct(), false,
                    List.of(field("key", m.key(), false), field("value", m.value(), true)), java.util.Map.of()));
            case DuckType.Scalar s -> List.of();
        };
        return new ArrowField(name, arrowType(t), nullable, kids, java.util.Map.of());
    }

    /** The Arrow type of a DuckDB type (docs/WAREHOUSE_FFM_HOMEWORK_2026_09_26.md §3). */
    private static ArrowType arrowType(DuckType t) {
        if (t instanceof DuckType.ListOf l) {
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("\\[(\\d+)]$").matcher(l.name());
            return m.find() ? new ArrowType.FixedSizeList(Integer.parseInt(m.group(1))) : new ArrowType.List();
        }
        if (t instanceof DuckType.StructOf) return new ArrowType.Struct();
        if (t instanceof DuckType.MapOf) return new ArrowType.Map(false);
        String base = ((DuckType.Scalar) t).base();
        return switch (base) {
            case "BOOLEAN" -> new ArrowType.Bool();
            case "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT" ->
                    new ArrowType.Int(8 * Columnar.width(base), !base.startsWith("U"));
            case "FLOAT" -> new ArrowType.FloatingPoint(ArrowType.FloatPrecision.SINGLE);
            case "DOUBLE" -> new ArrowType.FloatingPoint(ArrowType.FloatPrecision.DOUBLE);
            case "DECIMAL" -> {
                int[] ps = decimal(((DuckType.Scalar) t).name());
                yield new ArrowType.Decimal(ps[0], ps[1], 128);
            }
            case "HUGEINT" -> new ArrowType.Decimal(38, 0, 128);
            case "BLOB" -> new ArrowType.Binary();
            case "DATE" -> new ArrowType.Date(ArrowType.DateUnit.DAY);
            case "TIME" -> new ArrowType.Time(ArrowType.TimeUnit.MICROSECOND, 64);
            case "INTERVAL" -> new ArrowType.Interval(ArrowType.IntervalUnit.MONTH_DAY_NANO);
            case "TIMESTAMP_S" -> new ArrowType.Timestamp(ArrowType.TimeUnit.SECOND, null);
            case "TIMESTAMP_MS" -> new ArrowType.Timestamp(ArrowType.TimeUnit.MILLISECOND, null);
            case "TIMESTAMP_NS" -> new ArrowType.Timestamp(ArrowType.TimeUnit.NANOSECOND, null);
            case "TIMESTAMP" -> new ArrowType.Timestamp(ArrowType.TimeUnit.MICROSECOND, null);
            case "TIMESTAMP WITH TIME ZONE" -> new ArrowType.Timestamp(ArrowType.TimeUnit.MICROSECOND, "UTC");
            default -> new ArrowType.Utf8();   // VARCHAR, JSON, UUID, ENUM, UHUGEINT: text
        };
    }

    private static int[] decimal(String name) {
        int open = name.indexOf('('), comma = name.indexOf(',', open), close = name.indexOf(')', comma);
        return new int[] {Integer.parseInt(name.substring(open + 1, comma).strip()),
                Integer.parseInt(name.substring(comma + 1, close).strip())};
    }

    // -- a batch's body ------------------------------------------------------------------

    /** The column's nodes and buffers, depth first, as a record batch lists them. */
    private static void layout(Columnar c, ArrowIpc.Layout out) {
        out.node(c.length, c.nulls);
        byte[] v = c.validity;
        out.buffer(v == null ? new byte[0] : v);
        switch (c.type) {
            case DuckType.ListOf l -> {
                if (c.offsets != null) out.buffer(ints(c.offsets));
                layout(c.children.get(0), out);
            }
            case DuckType.StructOf s -> {
                for (Columnar k : c.children) layout(k, out);
            }
            case DuckType.MapOf m -> {
                out.buffer(ints(java.util.Objects.requireNonNull(c.offsets)));
                Columnar key = c.children.get(0);
                out.node(key.length, 0);   // the entries struct: never null
                out.buffer(new byte[0]);
                layout(key, out);
                layout(c.children.get(1), out);
            }
            case DuckType.Scalar s -> {
                switch (s.base()) {
                    case "VARCHAR", "JSON", "UUID", "ENUM", "BLOB" -> {
                        out.buffer(ints(java.util.Objects.requireNonNull(c.offsets)));
                        out.buffer(c.data);
                    }
                    case "UHUGEINT" -> uhugeintText(c, out);
                    default -> out.buffer(bytes(c.values));
                }
            }
        }
    }

    private static void uhugeintText(Columnar c, ArrowIpc.Layout out) {
        ByteBuffer offs = ByteBuffer.allocate(4 * (c.length + 1)).order(ByteOrder.LITTLE_ENDIAN);
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        offs.putInt(0);
        for (int i = 0; i < c.length; i++) {
            if (c.present(i)) data.writeBytes(ApiValues.int128(c.values, i, false).toString().getBytes(StandardCharsets.UTF_8));
            offs.putInt(data.size());
        }
        out.buffer(offs.array());
        out.buffer(data.toByteArray());
    }

    private static byte[] ints(int[] a) {
        ByteBuffer b = ByteBuffer.allocate(4 * a.length).order(ByteOrder.LITTLE_ENDIAN);
        for (int x : a) b.putInt(x);
        return b.array();
    }

    private static byte[] bytes(ByteBuffer b) {
        return b.array();
    }
}
