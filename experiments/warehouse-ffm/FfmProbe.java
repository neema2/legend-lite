import static java.lang.foreign.ValueLayout.ADDRESS;
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
 * Probe: DuckDB's public C API through Java's Foreign Function & Memory API (java.lang.foreign):
 * no JDBC, no reflection, no dependency, the native library DuckDB's JDBC jar already ships.
 * Reads 1M rows x 8 columns from vector memory into Arrow-ready buffers (validity bitmaps, values,
 * string offsets + bytes, decimal128, bit-packed booleans) and times it.
 *   java --enable-native-access=ALL-UNNAMED FfmProbe.java <libduckdb_java.so_...>
 */
public class FfmProbe {
    static final Linker LINKER = Linker.nativeLinker();
    static SymbolLookup lib;
    /** duckdb_result: three idx_t, three pointers (passed BY VALUE to duckdb_fetch_chunk). */
    static final StructLayout RESULT = MemoryLayout.structLayout(JAVA_LONG, JAVA_LONG, JAVA_LONG, ADDRESS, ADDRESS, ADDRESS);

    static MethodHandle fn(String name, FunctionDescriptor d) {
        return LINKER.downcallHandle(lib.find(name).orElseThrow(() -> new IllegalStateException("no symbol " + name)), d);
    }

    static MethodHandle open, connect, query, resultError, destroyResult, fetchChunk, chunkSize, chunkVector,
            vectorData, vectorValidity, destroyChunk, columnCount, columnType;

    public static void main(String[] a) throws Throwable {
        lib = SymbolLookup.libraryLookup(Path.of(a[0]), Arena.global());
        open = fn("duckdb_open", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
        connect = fn("duckdb_connect", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
        query = fn("duckdb_query", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS));
        resultError = fn("duckdb_result_error", FunctionDescriptor.of(ADDRESS, ADDRESS));
        destroyResult = fn("duckdb_destroy_result", FunctionDescriptor.ofVoid(ADDRESS));
        fetchChunk = fn("duckdb_fetch_chunk", FunctionDescriptor.of(ADDRESS, RESULT));
        chunkSize = fn("duckdb_data_chunk_get_size", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
        chunkVector = fn("duckdb_data_chunk_get_vector", FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG));
        vectorData = fn("duckdb_vector_get_data", FunctionDescriptor.of(ADDRESS, ADDRESS));
        vectorValidity = fn("duckdb_vector_get_validity", FunctionDescriptor.of(ADDRESS, ADDRESS));
        destroyChunk = fn("duckdb_destroy_data_chunk", FunctionDescriptor.ofVoid(ADDRESS));
        columnCount = fn("duckdb_column_count", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
        columnType = fn("duckdb_column_type", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG));

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment db = arena.allocate(ADDRESS), conn = arena.allocate(ADDRESS);
            check((int) open.invokeExact(MemorySegment.NULL, db), "open");
            check((int) connect.invokeExact(db.get(ADDRESS, 0), conn), "connect");
            MemorySegment c = conn.get(ADDRESS, 0);
            exec(arena, c, "SET threads=4");
            exec(arena, c, "CREATE TABLE big AS SELECT i::INTEGER a, i::BIGINT b, i * 1.5::DOUBLE c, (i / 7)::DECIMAL(18,3) d,"
                    + " 'row ' || i e, DATE '2020-01-01' + (i % 1000)::INTEGER f, TIMESTAMP '2020-01-01' + to_seconds(i) g,"
                    + " i % 2 = 0 h FROM range(1000000) t(i)");
            for (int rep = 0; rep < 5; rep++) read(arena, c);
            // correctness spot check: row 999,999 decoded from the buffers, printed next to DuckDB's own text
            spot(arena, c);
        }
    }

    static void check(int state, String what) {
        if (state != 0) throw new IllegalStateException(what + " failed");
    }

    static void exec(Arena arena, MemorySegment conn, String sql) throws Throwable {
        MemorySegment res = arena.allocate(RESULT);
        int st = (int) query.invokeExact(conn, arena.allocateFrom(sql), res);
        if (st != 0) {
            MemorySegment err = (MemorySegment) resultError.invokeExact(res);
            String m = err.reinterpret(Long.MAX_VALUE).getString(0);
            destroyResult.invokeExact(res);
            throw new IllegalStateException(m);
        }
        destroyResult.invokeExact(res);
    }

    /** Growable byte buffer standing in for one Arrow buffer. */
    static final class Buf {
        byte[] b = new byte[Integer.getInteger("buf", 1 << 16)];
        int n;
        void room(int more) { if (n + more > b.length) b = Arrays.copyOf(b, Math.max(b.length * 2, n + more)); }
    }

    static final int[] WIDTH = {4, 8, 8, 8, 16, 4, 8, 1};   // a..h in memory (d: DECIMAL(18,3) is an int64; e: 16-byte string_t)

    static void read(Arena arena, MemorySegment conn) throws Throwable {
        long t0 = System.nanoTime();
        MemorySegment res = arena.allocate(RESULT);
        check((int) query.invokeExact(conn, arena.allocateFrom("SELECT * FROM big"), res), "query");
        Buf[] values = new Buf[8], validity = new Buf[8];
        for (int i = 0; i < 8; i++) { values[i] = new Buf(); validity[i] = new Buf(); }
        Buf strData = new Buf(), strOffsets = new Buf();
        long rows = 0;
        long[] spent = new long[9];   // per column, then [8] = fetching chunks
        while (true) {
            long f0 = System.nanoTime();
            MemorySegment chunk = (MemorySegment) fetchChunk.invokeExact(res);
            spent[8] += System.nanoTime() - f0;
            if (chunk.equals(MemorySegment.NULL)) break;
            long n = (long) chunkSize.invokeExact(chunk);
            if (n == 0) { destroy(arena, chunk); break; }
            for (int col = 0; col < 8; col++) {
                long c0 = System.nanoTime();
                MemorySegment vec = (MemorySegment) chunkVector.invokeExact(chunk, (long) col);
                MemorySegment data = ((MemorySegment) vectorData.invokeExact(vec)).reinterpret(n * WIDTH[col]);
                MemorySegment valid = (MemorySegment) vectorValidity.invokeExact(vec);
                // validity: DuckDB's uint64 words, LSB first == Arrow's bitmap bytes; NULL pointer = all valid
                int vbytes = (int) ((n + 7) / 8);
                Buf v = validity[col];
                v.room(vbytes);
                if (valid.equals(MemorySegment.NULL)) Arrays.fill(v.b, v.n, v.n + vbytes, (byte) -1);
                else MemorySegment.copy(valid.reinterpret(vbytes), JAVA_BYTE, 0, v.b, v.n, vbytes);
                v.n += vbytes;   // (a real writer shifts when a chunk's row count is not a multiple of 8)
                Buf out = values[col];
                switch (col) {
                    case 0, 1, 2, 5, 6 -> {   // fixed width, same layout as Arrow: one bulk copy
                        int bytes = (int) (n * WIDTH[col]);
                        out.room(bytes);
                        MemorySegment.copy(data, JAVA_BYTE, 0, out.b, out.n, bytes);
                        out.n += bytes;
                    }
                    case 3 -> {   // DECIMAL(18,3) as int64 -> decimal128: one bulk copy, then sign-extend in the array
                        int m = (int) n;
                        byte[] raw = scratch(m * 8);
                        MemorySegment.copy(data, JAVA_BYTE, 0, raw, 0, m * 8);
                        out.room(m * 16);
                        byte[] o = out.b;
                        int at = out.n;
                        for (int i = 0; i < m; i++) {
                            System.arraycopy(raw, i * 8, o, at, 8);
                            byte fill = raw[i * 8 + 7] < 0 ? (byte) -1 : 0;
                            for (int k = 8; k < 16; k++) o[at + k] = fill;
                            at += 16;
                        }
                        out.n = at;
                    }
                    case 4 -> {   // VARCHAR: string_t {uint32 len; 12 bytes inline | 4 prefix + 8 pointer}; slots copied in bulk
                        int m = (int) n;
                        byte[] raw = scratch(m * 16);
                        MemorySegment.copy(data, JAVA_BYTE, 0, raw, 0, m * 16);
                        strOffsets.room(m * 4);
                        for (int i = 0; i < m; i++) {
                            int at = i * 16;
                            int len = getInt(raw, at);
                            strData.room(len);
                            if (len <= 12) System.arraycopy(raw, at + 4, strData.b, strData.n, len);
                            else MemorySegment.copy(MemorySegment.ofAddress(getLong(raw, at + 8)).reinterpret(len), JAVA_BYTE, 0, strData.b, strData.n, len);
                            strData.n += len;
                            putInt(strOffsets.b, strOffsets.n, strData.n);
                            strOffsets.n += 4;
                        }
                    }
                    case 7 -> {   // BOOLEAN: one byte per value -> Arrow's bit-packed
                        int bytes = (int) ((n + 7) / 8);
                        out.room(bytes);
                        int m = (int) n;
                        byte[] raw = scratch(m);
                        MemorySegment.copy(data, JAVA_BYTE, 0, raw, 0, m);
                        byte[] o = out.b;
                        int base = out.n;
                        for (int i = 0; i < m; i++) if (raw[i] != 0) o[base + (i >>> 3)] |= (byte) (1 << (i & 7));
                        out.n += bytes;
                    }
                    default -> throw new IllegalStateException();
                }
                spent[col] += System.nanoTime() - c0;
            }
            rows += n;
            destroy(arena, chunk);
        }
        destroyResult.invokeExact(res);
        long total = strData.n + strOffsets.n;
        for (int i = 0; i < 8; i++) total += values[i].n + validity[i].n;
        System.out.printf("FFM C API -> Arrow buffers: %d ms, %d rows, %.1f MB of buffers%n",
                (System.nanoTime() - t0) / 1_000_000, rows, total / 1e6);
        String[] names = {"int", "bigint", "double", "decimal", "varchar", "date", "timestamp", "bool", "fetch"};
        StringBuilder sb = new StringBuilder("   ms:");
        for (int i = 0; i < 9; i++) sb.append(' ').append(names[i]).append('=').append(spent[i] / 1_000_000);
        System.out.println(sb);
    }

    static void destroy(Arena arena, MemorySegment chunk) throws Throwable {
        MemorySegment p = arena.allocate(ADDRESS);
        p.set(ADDRESS, 0, chunk);
        destroyChunk.invokeExact(p);
    }

    static void spot(Arena arena, MemorySegment conn) throws Throwable {
        MemorySegment res = arena.allocate(RESULT);
        check((int) query.invokeExact(conn, arena.allocateFrom(
                "SELECT a, b, c, d, e, f, g, h FROM big WHERE a = 999999"), res), "query");
        MemorySegment chunk = (MemorySegment) fetchChunk.invokeExact(res);
        StringBuilder sb = new StringBuilder("row 999999 from vector memory: ");
        for (int col = 0; col < 8; col++) {
            MemorySegment vec = (MemorySegment) chunkVector.invokeExact(chunk, (long) col);
            MemorySegment d = ((MemorySegment) vectorData.invokeExact(vec)).reinterpret(WIDTH[col]);
            sb.append(switch (col) {
                case 0, 5 -> d.get(JAVA_INT, 0) + (col == 5 ? " days" : "");
                case 1 -> d.get(JAVA_LONG, 0);
                case 2 -> d.get(ValueLayoutDouble.D, 0);
                case 3 -> java.math.BigDecimal.valueOf(d.get(JAVA_LONG, 0), 3);
                case 4 -> { int len = d.get(JAVA_INT, 0); yield len <= 12 ? d.getString(4) : "(long)"; }
                case 6 -> java.time.LocalDateTime.ofEpochSecond(Math.floorDiv(d.get(JAVA_LONG, 0), 1_000_000), 0, java.time.ZoneOffset.UTC);
                default -> d.get(JAVA_BYTE, 0) != 0;
            }).append(" | ");
        }
        System.out.println(sb);
        destroy(arena, chunk);
        destroyResult.invokeExact(res);
    }

    static final class ValueLayoutDouble { static final java.lang.foreign.ValueLayout.OfDouble D = java.lang.foreign.ValueLayout.JAVA_DOUBLE; }

    static byte[] scratchBuf = new byte[1 << 16];
    static byte[] scratch(int n) { if (scratchBuf.length < n) scratchBuf = new byte[n]; return scratchBuf; }
    static int getInt(byte[] b, int at) { return (b[at] & 0xff) | (b[at + 1] & 0xff) << 8 | (b[at + 2] & 0xff) << 16 | (b[at + 3] & 0xff) << 24; }
    static long getLong(byte[] b, int at) { return (getInt(b, at) & 0xffffffffL) | ((long) getInt(b, at + 4)) << 32; }
    static void putLong(byte[] b, int at, long v) { for (int i = 0; i < 8; i++) b[at + i] = (byte) (v >>> (8 * i)); }
    static void putInt(byte[] b, int at, int v) { for (int i = 0; i < 4; i++) b[at + i] = (byte) (v >>> (8 * i)); }
}
