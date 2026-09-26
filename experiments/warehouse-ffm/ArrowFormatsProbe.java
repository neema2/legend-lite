import static java.lang.foreign.ValueLayout.*;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

/**
 * Probe: what DuckDB's own Arrow conversion (duckdb_to_arrow_schema / duckdb_data_chunk_to_arrow, the
 * public C API) produces for every API type: the Arrow format string, extension metadata, dictionary,
 * buffer count -- under default settings and under arrow_lossless_conversion.
 */
public class ArrowFormatsProbe {
    static final Linker L = Linker.nativeLinker();
    static SymbolLookup lib;
    static final StructLayout RESULT = MemoryLayout.structLayout(JAVA_LONG, JAVA_LONG, JAVA_LONG, ADDRESS, ADDRESS, ADDRESS);
    static MethodHandle fn(String n, FunctionDescriptor d) { return L.downcallHandle(lib.find(n).orElseThrow(() -> new IllegalStateException(n)), d); }

    static final String[][] TYPES = {
        {"boolean", "true"}, {"tinyint", "1::TINYINT"}, {"smallint", "1::SMALLINT"}, {"integer", "1::INTEGER"},
        {"bigint", "9007199254740993::BIGINT"}, {"hugeint", "170141183460469231731687303715884105727::HUGEINT"},
        {"utinyint", "255::UTINYINT"}, {"usmallint", "65535::USMALLINT"}, {"uinteger", "4294967295::UINTEGER"},
        {"ubigint", "18446744073709551615::UBIGINT"}, {"uhugeint", "340282366920938463463374607431768211455::UHUGEINT"},
        {"float", "1.5::FLOAT"}, {"double", "'-0.0'::DOUBLE"}, {"decimal", "123.45::DECIMAL(9,2)"},
        {"decimal38", "1.5::DECIMAL(38,10)"}, {"varchar", "'héllo'"}, {"uuid", "'11111111-2222-3333-4444-555555555555'::UUID"},
        {"interval", "INTERVAL 1 DAY + INTERVAL 2 HOUR"}, {"enum", "'a'::ENUM('a','b')"}, {"json", "'{\"a\":1}'::JSON"},
        {"blob", "'\\xAA\\xBB'::BLOB"}, {"date", "DATE '0001-01-01'"}, {"time", "TIME '01:02:03.456789'"},
        {"timestamp", "TIMESTAMP '1500-01-01 01:02:03.123456'"}, {"timestamp_s", "TIMESTAMP_S '2020-01-01 01:02:03'"},
        {"timestamp_ms", "TIMESTAMP_MS '2020-01-01 01:02:03.123'"}, {"timestamp_ns", "TIMESTAMP_NS '2020-01-01 01:02:03.123456789'"},
        {"timestamptz", "TIMESTAMPTZ '2020-01-01 01:02:03+05'"}, {"list", "[1, NULL, 3]"}, {"array", "[1, 2]::INTEGER[2]"},
        {"struct", "{'x': 1, 'y': 'b'}"}, {"map", "MAP {'k': 1}"}, {"nested", "[{'x': [1.5::DECIMAL(4,1)]}]"},
    };

    public static void main(String[] a) throws Throwable {
        lib = SymbolLookup.libraryLookup(Path.of(a[0]), Arena.global());
        MethodHandle open = fn("duckdb_open", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
        MethodHandle connect = fn("duckdb_connect", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
        MethodHandle query = fn("duckdb_query", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS));
        MethodHandle destroyResult = fn("duckdb_destroy_result", FunctionDescriptor.ofVoid(ADDRESS));
        MethodHandle columnType = fn("duckdb_column_logical_type", FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG));
        MethodHandle columnName = fn("duckdb_column_name", FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG));
        MethodHandle options = fn("duckdb_connection_get_arrow_options", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS));
        MethodHandle toSchema = fn("duckdb_to_arrow_schema", FunctionDescriptor.of(ADDRESS, ADDRESS, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS));
        for (String mode : new String[] {"default", "lossless"}) {
            System.out.println("== " + mode);
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment db = arena.allocate(ADDRESS), conn = arena.allocate(ADDRESS);
                int s1 = (int) open.invokeExact(MemorySegment.NULL, db);
                int s2 = (int) connect.invokeExact(db.get(ADDRESS, 0), conn);
                MemorySegment c = conn.get(ADDRESS, 0);
                if (mode.equals("lossless")) {
                    MemorySegment r = arena.allocate(RESULT);
                    int s3 = (int) query.invokeExact(c, arena.allocateFrom("SET arrow_lossless_conversion = true"), r);
                    destroyResult.invokeExact(r);
                }
                MemorySegment opts = arena.allocate(ADDRESS);
                options.invokeExact(c, opts);
                for (String[] t : TYPES) {
                    MemorySegment r = arena.allocate(RESULT);
                    if ((int) query.invokeExact(c, arena.allocateFrom("SELECT " + t[1] + " AS v"), r) != 0) { System.out.println(t[0] + ": query failed"); continue; }
                    MemorySegment types = arena.allocate(ADDRESS), names = arena.allocate(ADDRESS);
                    types.set(ADDRESS, 0, (MemorySegment) columnType.invokeExact(r, 0L));
                    names.set(ADDRESS, 0, (MemorySegment) columnName.invokeExact(r, 0L));
                    MemorySegment schema = arena.allocate(72, 8);
                    MemorySegment err = (MemorySegment) toSchema.invokeExact(opts.get(ADDRESS, 0), types, names, 1L, schema);
                    MemorySegment col = schema.get(ADDRESS, 40).reinterpret(8).get(ADDRESS, 0).reinterpret(72);
                    System.out.printf("%-13s %s%n", t[0], describe(col, 0));
                    destroyResult.invokeExact(r);
                }
            }
        }
    }

    /** format, extension name from the metadata, dictionary, children -- recursively. */
    static String describe(MemorySegment s, int depth) {
        String fmt = s.get(ADDRESS, 0).reinterpret(Long.MAX_VALUE).getString(0);
        StringBuilder sb = new StringBuilder(fmt);
        MemorySegment md = s.get(ADDRESS, 16);
        if (!md.equals(MemorySegment.NULL)) {
            // metadata: int32 n, then n x (int32 klen, key, int32 vlen, value)
            MemorySegment m = md.reinterpret(1 << 16);
            int n = m.get(JAVA_INT_UNALIGNED, 0);
            long at = 4;
            for (int i = 0; i < n; i++) {
                int kl = m.get(JAVA_INT_UNALIGNED, at); String k = new String(m.asSlice(at + 4, kl).toArray(JAVA_BYTE)); at += 4 + kl;
                int vl = m.get(JAVA_INT_UNALIGNED, at); String v = new String(m.asSlice(at + 4, vl).toArray(JAVA_BYTE)); at += 4 + vl;
                if (k.startsWith("ARROW:extension")) sb.append(" [").append(k.substring(16)).append('=').append(v.replace("\n", " ")).append(']');
            }
        }
        MemorySegment dict = s.get(ADDRESS, 48);
        if (!dict.equals(MemorySegment.NULL)) sb.append(" dictionary<").append(describe(dict.reinterpret(72), depth + 1)).append('>');
        long nc = s.get(JAVA_LONG, 32);
        if (nc > 0) {
            MemorySegment kids = s.get(ADDRESS, 40).reinterpret(8 * nc);
            sb.append(" (");
            for (long i = 0; i < nc; i++) sb.append(i > 0 ? ", " : "").append(describe(kids.get(ADDRESS, 8 * i).reinterpret(72), depth + 1));
            sb.append(')');
        }
        return sb.toString();
    }
}
