import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BOOLEAN;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Probe: one C call per chunk turns DuckDB's vectors into Arrow buffers (duckdb_data_chunk_to_arrow,
 * DuckDB's public C API); Java then only bulk-copies buffers. Few calls matter: an FFM call costs
 * ~8 ns on the JVM but ~2.3 us in a GraalVM CE 25 native image (measured, CallCost).
 */
public class FfmArrowProbe {
    static final Linker LINKER = Linker.nativeLinker();
    static SymbolLookup lib;
    static final StructLayout RESULT = MemoryLayout.structLayout(JAVA_LONG, JAVA_LONG, JAVA_LONG, ADDRESS, ADDRESS, ADDRESS);
    /** struct ArrowArray (the Arrow C data interface): 10 eight-byte fields. */
    static final long LENGTH = 0, NULL_COUNT = 8, OFFSET = 16, N_BUFFERS = 24, N_CHILDREN = 32, BUFFERS = 40, CHILDREN = 48, RELEASE = 64, ARRAY_SIZE = 80;

    static MethodHandle fn(String name, FunctionDescriptor d) {
        return LINKER.downcallHandle(lib.find(name).orElseThrow(() -> new IllegalStateException("no symbol " + name)), d);
    }

    static MethodHandle open, connect, query, destroyResult, fetchChunk, chunkSize, destroyChunk, arrowOptions, toArrow,
            errorHas, errorMessage, errorDestroy, release;

    public static void main(String[] a) throws Throwable {
        lib = SymbolLookup.libraryLookup(Path.of(a[0]), Arena.global());
        open = fn("duckdb_open", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
        connect = fn("duckdb_connect", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
        query = fn("duckdb_query", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS));
        destroyResult = fn("duckdb_destroy_result", FunctionDescriptor.ofVoid(ADDRESS));
        fetchChunk = fn("duckdb_fetch_chunk", FunctionDescriptor.of(ADDRESS, RESULT));
        chunkSize = fn("duckdb_data_chunk_get_size", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
        destroyChunk = fn("duckdb_destroy_data_chunk", FunctionDescriptor.ofVoid(ADDRESS));
        arrowOptions = fn("duckdb_connection_get_arrow_options", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS));
        toArrow = fn("duckdb_data_chunk_to_arrow", FunctionDescriptor.of(ADDRESS, ADDRESS, ADDRESS, ADDRESS));
        errorHas = fn("duckdb_error_data_has_error", FunctionDescriptor.of(JAVA_BOOLEAN, ADDRESS));
        errorMessage = fn("duckdb_error_data_message", FunctionDescriptor.of(ADDRESS, ADDRESS));
        errorDestroy = fn("duckdb_destroy_error_data", FunctionDescriptor.ofVoid(ADDRESS));
        release = LINKER.downcallHandle(FunctionDescriptor.ofVoid(ADDRESS));   // called through the array's own release pointer

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment db = arena.allocate(ADDRESS), conn = arena.allocate(ADDRESS);
            if ((int) open.invokeExact(MemorySegment.NULL, db) != 0) throw new IllegalStateException("open");
            if ((int) connect.invokeExact(db.get(ADDRESS, 0), conn) != 0) throw new IllegalStateException("connect");
            MemorySegment c = conn.get(ADDRESS, 0);
            exec(arena, c, "SET threads=4");
            exec(arena, c, "CREATE TABLE big AS SELECT i::INTEGER a, i::BIGINT b, i * 1.5::DOUBLE c, (i / 7)::DECIMAL(18,3) d,"
                    + " 'row ' || i e, DATE '2020-01-01' + (i % 1000)::INTEGER f, TIMESTAMP '2020-01-01' + to_seconds(i) g,"
                    + " i % 2 = 0 h FROM range(1000000) t(i)");
            MemorySegment opts = arena.allocate(ADDRESS);
            arrowOptions.invokeExact(c, opts);
            for (int rep = 0; rep < 5; rep++) read(arena, c, opts.get(ADDRESS, 0));
        }
    }

    static void exec(Arena arena, MemorySegment conn, String sql) throws Throwable {
        MemorySegment res = arena.allocate(RESULT);
        if ((int) query.invokeExact(conn, arena.allocateFrom(sql), res) != 0) throw new IllegalStateException(sql);
        destroyResult.invokeExact(res);
    }

    static final class Buf {
        byte[] b = new byte[1 << 16];
        int n;
        void add(MemorySegment src, long bytes) {
            int m = (int) bytes;
            if (n + m > b.length) b = Arrays.copyOf(b, Math.max(b.length * 2, n + m));
            MemorySegment.copy(src.reinterpret(m), JAVA_BYTE, 0, b, n, m);
            n += m;
        }
        void ones(long bytes) {
            int m = (int) bytes;
            if (n + m > b.length) b = Arrays.copyOf(b, Math.max(b.length * 2, n + m));
            Arrays.fill(b, n, n + m, (byte) -1);
            n += m;
        }
    }

    /** Value width per column in Arrow (0 = variable: offsets + bytes; -1 = bit-packed boolean). */
    static final int[] WIDTH = {4, 8, 8, 16, 0, 4, 8, -1};

    static void read(Arena arena, MemorySegment conn, MemorySegment opts) throws Throwable {
        long t0 = System.nanoTime();
        long calls = 0;
        MemorySegment res = arena.allocate(RESULT);
        if ((int) query.invokeExact(conn, arena.allocateFrom("SELECT * FROM big"), res) != 0) throw new IllegalStateException("query");
        Buf[] validity = new Buf[8], values = new Buf[8], data = new Buf[8];
        for (int i = 0; i < 8; i++) { validity[i] = new Buf(); values[i] = new Buf(); data[i] = new Buf(); }
        MemorySegment array = arena.allocate(ARRAY_SIZE, 8);
        MemorySegment chunkPtr = arena.allocate(ADDRESS);
        MemorySegment errPtr = arena.allocate(ADDRESS);
        long rows = 0;
        while (true) {
            MemorySegment chunk = (MemorySegment) fetchChunk.invokeExact(res);
            calls++;
            if (chunk.equals(MemorySegment.NULL)) break;
            MemorySegment err = (MemorySegment) toArrow.invokeExact(opts, chunk, array);
            calls++;
            if (!err.equals(MemorySegment.NULL)) {
                calls++;
                if ((boolean) errorHas.invokeExact(err)) {
                    throw new IllegalStateException(((MemorySegment) errorMessage.invokeExact(err)).reinterpret(Long.MAX_VALUE).getString(0));
                }
                errPtr.set(ADDRESS, 0, err);
                errorDestroy.invokeExact(errPtr);
                calls++;
            }
            long n = array.get(JAVA_LONG, LENGTH);
            MemorySegment children = array.get(ADDRESS, CHILDREN).reinterpret(8 * 8);
            for (int col = 0; col < 8; col++) {
                MemorySegment child = children.get(ADDRESS, col * 8L).reinterpret(ARRAY_SIZE);
                if (child.get(JAVA_LONG, OFFSET) != 0) throw new IllegalStateException("offset arrays not handled in the probe");
                MemorySegment bufs = child.get(ADDRESS, BUFFERS).reinterpret(8 * child.get(JAVA_LONG, N_BUFFERS));
                MemorySegment valid = bufs.get(ADDRESS, 0);
                if (valid.equals(MemorySegment.NULL)) validity[col].ones((n + 7) / 8);
                else validity[col].add(valid, (n + 7) / 8);
                int w = WIDTH[col];
                if (w > 0) values[col].add(bufs.get(ADDRESS, 8), n * w);
                else if (w < 0) values[col].add(bufs.get(ADDRESS, 8), (n + 7) / 8);
                else {   // offsets (n + 1 int32s) + the bytes they span
                    // each chunk's offsets start at 0: shift them by the bytes already collected, and drop the
                    // chunk's leading 0 after the first chunk (one column of all chunks, as one Arrow array)
                    MemorySegment offs = bufs.get(ADDRESS, 8).reinterpret(4 * (n + 1));
                    int base = data[col].n, from = values[col].n == 0 ? 0 : 1;
                    int[] o = new int[(int) n + 1];
                    MemorySegment.copy(offs, JAVA_INT, 0, o, 0, (int) n + 1);
                    Buf v = values[col];
                    int need = 4 * ((int) n + 1 - from);
                    if (v.n + need > v.b.length) v.b = Arrays.copyOf(v.b, Math.max(v.b.length * 2, v.n + need));
                    for (int i = from; i <= n; i++) { putInt(v.b, v.n, o[i] + base); v.n += 4; }
                    data[col].add(bufs.get(ADDRESS, 16), o[(int) n]);
                }
            }
            MemorySegment releaseFn = array.get(ADDRESS, RELEASE);
            release.invokeExact(releaseFn, array);
            calls++;
            rows += n;
            chunkPtr.set(ADDRESS, 0, chunk);
            destroyChunk.invokeExact(chunkPtr);
            calls++;
        }
        destroyResult.invokeExact(res);
        long total = 0;
        for (int i = 0; i < 8; i++) total += validity[i].n + values[i].n + data[i].n;
        // the last row, read back from the collected Arrow buffers
        int last = (int) rows - 1;
        String s = new String(data[4].b, getInt(values[4].b, 4 * last), getInt(values[4].b, 4 * last + 4) - getInt(values[4].b, 4 * last));
        System.out.printf("FFM chunk->Arrow: %d ms, %d rows, %d C calls, %.1f MB | last row: a=%d d(unscaled)=%d e=%s f=%d days h=%s%n",
                (System.nanoTime() - t0) / 1_000_000, rows, calls, total / 1e6, getInt(values[0].b, 4 * last),
                getLong(values[3].b, 16 * last), s, getInt(values[5].b, 4 * last), ((values[7].b[last >>> 3] >> (last & 7)) & 1) == 1);
    }

    static void putInt(byte[] b, int at, int v) { for (int i = 0; i < 4; i++) b[at + i] = (byte) (v >>> (8 * i)); }
    static int getInt(byte[] b, int at) { return (b[at] & 0xff) | (b[at + 1] & 0xff) << 8 | (b[at + 2] & 0xff) << 16 | (b[at + 3] & 0xff) << 24; }
    static long getLong(byte[] b, int at) { return (getInt(b, at) & 0xffffffffL) | ((long) getInt(b, at + 4)) << 32; }
}
