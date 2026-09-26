package com.legend.warehouse.server.duck;

import com.legend.server.Json;
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
                List<ColumnData> cols = columns(array, trees);
                for (int row = 0; row < n; row++) {
                    List<Json.Node> out = new ArrayList<>(cols.size());
                    for (ColumnData c : cols) out.add(JsonCells.cell(d, c, row));
                    rows.add(out);
                }
            });
        } finally {
            for (TypeTree t : trees) t.destroy(d);
        }
        return rows;
    }

    /** A result as Arrow chunks, and how many rows they hold in all. */
    public record Arrow(List<byte[]> chunks, long rows) {
    }

    /** Arrow IPC streams, each holding whole batches until it has {@code rowsPerChunk} rows. */
    public static Arrow arrow(Result r, int rowsPerChunk, long maxRows) throws Exception {
        Duck d = Duck.api();
        List<TypeTree> trees = trees(d, r);
        List<String> names = new ArrayList<>();
        for (Result.Column c : r.columns()) names.add(c.name());
        ArrowStreams streams = new ArrowStreams(names, trees);
        List<byte[]> chunks = new ArrayList<>();
        long[] total = {0};
        try {
            r.chunks((array, n) -> {
                total[0] += n;
                if (total[0] > maxRows) throw new TooLarge(maxRows);
                streams.add(columns(array, trees), (int) n);
                if (streams.rows() >= rowsPerChunk) chunks.add(streams.flush());
            });
        } finally {
            for (TypeTree t : trees) t.destroy(d);
        }
        if (streams.rows() > 0 || chunks.isEmpty()) chunks.add(streams.flush());
        return new Arrow(List.copyOf(chunks), total[0]);
    }

    private static List<TypeTree> trees(Duck d, Result r) {
        List<TypeTree> out = new ArrayList<>();
        for (int i = 0; i < r.columns().size(); i++) {
            out.add(TypeTree.of(d, DuckType.parse(r.columns().get(i).typeName()), r.logicalType(i), false));
        }
        return out;
    }

    private static List<ColumnData> columns(java.lang.foreign.MemorySegment array, List<TypeTree> trees) {
        java.lang.foreign.MemorySegment a = array.reinterpret(Duck.ARRAY_SIZE);
        java.lang.foreign.MemorySegment kids = a.get(java.lang.foreign.ValueLayout.ADDRESS, 48).reinterpret(8L * trees.size());
        List<ColumnData> out = new ArrayList<>(trees.size());
        for (int i = 0; i < trees.size(); i++) out.add(ColumnData.copy(kids.getAtIndex(java.lang.foreign.ValueLayout.ADDRESS, i), trees.get(i)));
        return out;
    }
}
