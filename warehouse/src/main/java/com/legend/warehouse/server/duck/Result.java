package com.legend.warehouse.server.duck;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

import com.legend.base.Nullable;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A finished statement's result: rows, a count of rows changed, or nothing. Rows
 * are read chunk by chunk as Arrow C arrays (duckdb_data_chunk_to_arrow: one
 * call per 2,048 rows). Used on one thread; closed once.
 */
public final class Result implements AutoCloseable {

    /** A result column: its name and its DuckDB type name. */
    public record Column(String name, String typeName) {
    }

    /** Receives one chunk: a struct ArrowArray whose children are the columns (released after the call). */
    public interface ChunkSink {
        void accept(MemorySegment array, long rows) throws Exception;
    }

    private final Duck d;
    private final @Nullable Arena arena;
    private final MemorySegment res;
    private final MemorySegment prepared;
    private final MemorySegment arrowOptions;
    private final int returnType;
    private final long changed;
    private final List<Column> columns;
    private final List<MemorySegment> types;

    Result(Duck d, Arena arena, MemorySegment res, MemorySegment prepared, MemorySegment arrowOptions,
            Set<String> keywords) throws Throwable {
        this.d = d;
        this.arena = arena;
        this.res = res;
        this.prepared = prepared;
        this.arrowOptions = arrowOptions;
        this.returnType = (int) d.resultReturnType.invokeExact(res);
        this.changed = (long) d.rowsChanged.invokeExact(res);
        List<Column> cols = new ArrayList<>();
        List<MemorySegment> ts = new ArrayList<>();
        if (returnType == Duck.RESULT_QUERY) {
            long n = (long) d.columnCount.invokeExact(res);
            for (long i = 0; i < n; i++) {
                String name = Duck.text((MemorySegment) d.columnName.invokeExact(res, i));
                MemorySegment t = (MemorySegment) d.columnLogicalType.invokeExact(res, i);
                ts.add(t);
                cols.add(new Column(name == null ? "" : name, TypeNames.of(d, t, keywords)));
            }
        }
        this.columns = List.copyOf(cols);
        this.types = List.copyOf(ts);
    }

    private Result(Duck d) {
        this.d = d;
        this.arena = null;
        this.res = MemorySegment.NULL;
        this.prepared = MemorySegment.NULL;
        this.arrowOptions = MemorySegment.NULL;
        this.returnType = Duck.RESULT_NOTHING;
        this.changed = 0;
        this.columns = List.of();
        this.types = List.of();
    }

    /** An empty script's result. */
    static Result nothing(Duck d) {
        return new Result(d);
    }

    /** Whether the statement returned rows (a query), as against a count or nothing. */
    public boolean hasRows() {
        return returnType == Duck.RESULT_QUERY;
    }

    /** Rows changed by a write; -1 for a statement that changes none and returns none (DDL, SET). */
    public long changed() {
        return returnType == Duck.RESULT_CHANGED_ROWS ? changed : -1;
    }

    public List<Column> columns() {
        return columns;
    }

    /** The column's DuckDB logical type (owned by this result). */
    MemorySegment logicalType(int column) {
        return types.get(column);
    }

    /** Every chunk, in order, as Arrow C arrays; each is released after the sink returns. */
    public void chunks(ChunkSink sink) throws Exception {
        if (!hasRows() || arena == null) return;
        try (Arena a = Arena.ofConfined()) {
            MemorySegment array = a.allocate(Duck.ARRAY_SIZE, 8);
            MemorySegment chunkPtr = a.allocate(ADDRESS);
            MemorySegment errPtr = a.allocate(ADDRESS);
            while (true) {
                MemorySegment chunk;
                try {
                    chunk = (MemorySegment) d.fetchChunk.invokeExact(res);
                } catch (Throwable t) {
                    throw Duck.fail(t);
                }
                if (chunk.equals(MemorySegment.NULL)) return;
                try {
                    MemorySegment err = (MemorySegment) d.chunkToArrow.invokeExact(arrowOptions, chunk, array);
                    if (!err.equals(MemorySegment.NULL)) {
                        boolean failed = (boolean) d.errorHas.invokeExact(err);
                        String m = failed ? Duck.text((MemorySegment) d.errorMessage.invokeExact(err)) : null;
                        errPtr.set(ADDRESS, 0, err);
                        d.destroyError.invokeExact(errPtr);
                        if (failed) throw new DuckException(-1, m == null ? "could not convert a chunk to Arrow" : m);
                    }
                    try {
                        sink.accept(array, array.get(JAVA_LONG, 0));
                    } finally {
                        MemorySegment release = array.get(ADDRESS, 64);
                        if (!release.equals(MemorySegment.NULL)) d.release.invokeExact(release, array);
                    }
                } catch (Exception e) {
                    throw e;
                } catch (Throwable t) {
                    throw Duck.fail(t);
                } finally {
                    chunkPtr.set(ADDRESS, 0, chunk);
                    try {
                        d.destroyChunk.invokeExact(chunkPtr);
                    } catch (Throwable t) {
                        throw Duck.fail(t);
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        if (arena == null) return;
        try (Arena a = Arena.ofConfined()) {
            MemorySegment p = a.allocate(ADDRESS);
            for (MemorySegment t : types) {
                p.set(ADDRESS, 0, t);
                d.destroyLogicalType.invokeExact(p);
            }
            d.destroyResult.invokeExact(res);
            p.set(ADDRESS, 0, prepared);
            d.destroyPrepare.invokeExact(p);
        } catch (Throwable t) {
            throw Duck.fail(t);
        } finally {
            arena.close();
        }
    }
}
