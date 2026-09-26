package com.legend.warehouse.server.duck;

import com.legend.server.Json;
import com.legend.warehouse.sqlapi.ApiValues;
import com.legend.warehouse.sqlapi.Columnar;
import com.legend.warehouse.sqlapi.DuckType;
import java.util.ArrayList;
import java.util.List;

/**
 * A result's rows in the API's two formats, under the row limit: JSON rows, or
 * Arrow chunks of at least {@code rowsPerChunk} rows each (the last may be
 * smaller). The column types must already be ones the API carries.
 */
public final class Collect {

    private Collect() {
    }

    /** More rows than the limit. */
    public static final class TooLarge extends Exception {
        TooLarge(long max) {
            super("the result has more than " + max + " rows");
        }
    }

    /** Every row as JSON cells. */
    public static List<List<Json.Node>> json(Result r, long maxRows) throws Exception {
        Duck d = Duck.api();
        List<TypeTree> trees = trees(d, r);
        List<List<Json.Node>> rows = new ArrayList<>();
        try {
            r.chunks((array, n) -> {
                if (rows.size() + n > maxRows) throw new TooLarge(maxRows);
                List<Columnar> cols = columns(array, trees);
                ApiValues.NestedText texts = (column, row) -> DuckValues.text(d, cols.get(column), trees.get(column), row);
                for (int row = 0; row < n; row++) {
                    List<Json.Node> out = new ArrayList<>(cols.size());
                    for (int c = 0; c < cols.size(); c++) out.add(ApiValues.cell(cols.get(c), c, row, texts));
                    rows.add(out);
                }
            });
        } finally {
            for (TypeTree t : trees) t.destroy(d);
        }
        return rows;
    }

    /**
     * Every row as the API's JSON, written straight to each chunk's bytes ({@code rowsPerChunk} rows a
     * chunk, the last smaller), each handed to {@code sink} as it is finished: no tree of objects per
     * value, and never the whole result in memory at once. The rows written.
     */
    public static long jsonChunks(Result r, int rowsPerChunk, long maxRows, java.util.function.Consumer<byte[]> sink)
            throws Exception {
        Duck d = Duck.api();
        List<TypeTree> trees = trees(d, r);
        int[] chunks = {0};
        Json.Writer[] w = {start(0)};
        int[] inChunk = {0};
        long[] total = {0};
        try {
            r.chunks((array, n) -> {
                total[0] += n;
                if (total[0] > maxRows) throw new TooLarge(maxRows);
                List<Columnar> cols = columns(array, trees);
                ApiValues.NestedText texts = (column, row) -> DuckValues.text(d, cols.get(column), trees.get(column), row);
                for (int row = 0; row < n; row++) {
                    if (inChunk[0] == rowsPerChunk) {
                        sink.accept(finish(w[0]));
                        w[0] = start(++chunks[0]);
                        inChunk[0] = 0;
                    }
                    w[0].beginArray();
                    for (int c = 0; c < cols.size(); c++) ApiValues.writeCell(w[0], cols.get(c), c, row, texts);
                    w[0].endArray();
                    inChunk[0]++;
                }
            });
        } finally {
            for (TypeTree t : trees) t.destroy(d);
        }
        sink.accept(finish(w[0]));   // the last chunk, or the one empty chunk of an empty result
        return total[0];
    }

    private static Json.Writer start(int index) {
        Json.Writer w = Json.compactWriter();
        w.beginObject();
        w.field("index", index);
        w.name("rows");
        w.beginArray();
        return w;
    }

    private static byte[] finish(Json.Writer w) {
        w.endArray();
        w.endObject();
        return w.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Arrow IPC streams, each holding whole batches until it has {@code rowsPerChunk} rows, each handed
     * to {@code sink} as it is finished. The rows written.
     */
    public static long arrow(Result r, int rowsPerChunk, long maxRows, boolean cellText,
            java.util.function.Consumer<byte[]> sink) throws Exception {
        Duck d = Duck.api();
        List<TypeTree> trees = trees(d, r);
        List<String> names = new ArrayList<>();
        for (Result.Column c : r.columns()) names.add(c.name());
        ArrowStreams streams = new ArrowStreams(names, trees);
        int[] chunks = {0};
        long[] total = {0};
        try {
            r.chunks((array, n) -> {
                total[0] += n;
                if (total[0] > maxRows) throw new TooLarge(maxRows);
                List<Columnar> cols = columns(array, trees);
                streams.add(cols, (int) n, cellText ? nestedTexts(d, cols, trees, (int) n) : java.util.Map.of());
                if (streams.rows() >= rowsPerChunk) {
                    sink.accept(streams.flush());
                    chunks[0]++;
                }
            });
        } finally {
            for (TypeTree t : trees) t.destroy(d);
        }
        if (streams.rows() > 0 || chunks[0] == 0) sink.accept(streams.flush());
        return total[0];
    }

    private static List<TypeTree> trees(Duck d, Result r) {
        List<TypeTree> out = new ArrayList<>();
        for (int i = 0; i < r.columns().size(); i++) {
            out.add(TypeTree.of(d, DuckType.parse(r.columns().get(i).typeName()), r.logicalType(i), false));
        }
        return out;
    }

    /** DuckDB's text for every top-level nested cell of a batch, by column: an Arrow batch's metadata. */
    private static java.util.Map<Integer, List<String>> nestedTexts(Duck d, List<Columnar> cols, List<TypeTree> trees, int n) {
        java.util.Map<Integer, List<String>> out = new java.util.LinkedHashMap<>();
        for (int c = 0; c < cols.size(); c++) {
            Columnar col = cols.get(c);
            if (col.type instanceof DuckType.Scalar) continue;
            List<String> texts = new ArrayList<>(n);
            for (int row = 0; row < n; row++) texts.add(col.present(row) ? DuckValues.text(d, col, trees.get(c), row) : "");
            out.put(c, texts);
        }
        return out;
    }

    private static List<Columnar> columns(java.lang.foreign.MemorySegment array, List<TypeTree> trees) {
        java.lang.foreign.MemorySegment a = array.reinterpret(Duck.ARRAY_SIZE);
        java.lang.foreign.MemorySegment kids = a.get(java.lang.foreign.ValueLayout.ADDRESS, 48).reinterpret(8L * trees.size());
        List<Columnar> out = new ArrayList<>(trees.size());
        for (int i = 0; i < trees.size(); i++) out.add(ColumnData.copy(kids.getAtIndex(java.lang.foreign.ValueLayout.ADDRESS, i), trees.get(i)));
        return out;
    }
}
