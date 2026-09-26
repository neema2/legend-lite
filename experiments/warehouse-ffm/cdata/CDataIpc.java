package com.legend.warehouse.server.arrow;   // a probe: beside FlatBuilder, to use it

import static java.lang.foreign.ValueLayout.*;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Probe: DuckDB builds the Arrow buffers (duckdb_data_chunk_to_arrow, its public C API); Java frames them
 * as Arrow IPC with our FlatBuilder. Three types are ours to decide: UHUGEINT as text, ENUM (a dictionary)
 * as text, TIMESTAMPTZ tagged UTC. One stream per type for pyarrow; the values are compared in Python.
 */
public class CDataIpc {
    static final Linker L = Linker.nativeLinker();
    static SymbolLookup lib;
    static final StructLayout RESULT = MemoryLayout.structLayout(JAVA_LONG, JAVA_LONG, JAVA_LONG, ADDRESS, ADDRESS, ADDRESS);
    static MethodHandle fn(String n, FunctionDescriptor d) { return L.downcallHandle(lib.find(n).orElseThrow(() -> new IllegalStateException(n)), d); }
    static final MethodHandle RELEASE = L.downcallHandle(FunctionDescriptor.ofVoid(ADDRESS));

    // struct ArrowSchema (72 bytes) and struct ArrowArray (80 bytes)
    static final long S_FORMAT = 0, S_NAME = 8, S_FLAGS = 24, S_NCHILD = 32, S_CHILDREN = 40, S_DICT = 48, S_RELEASE = 56;
    static final long A_LENGTH = 0, A_NULLS = 8, A_OFFSET = 16, A_NBUF = 24, A_NCHILD = 32, A_BUFFERS = 40, A_CHILDREN = 48, A_DICT = 56, A_RELEASE = 64;

    /** One column's Arrow form after our three decisions: format, name, children, and a body writer. */
    record Node(String format, String name, boolean nullable, List<Node> children, @com.legend.base.Nullable String tz,
                long length, long nulls, List<byte[]> buffers) {}

    public static void main(String[] a) throws Throwable {
        lib = SymbolLookup.libraryLookup(Path.of(a[0]), Arena.global());
        Path out = Path.of(a[1]);
        Files.createDirectories(out);
        MethodHandle open = fn("duckdb_open", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
        MethodHandle connect = fn("duckdb_connect", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
        MethodHandle query = fn("duckdb_query", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS));
        MethodHandle destroyResult = fn("duckdb_destroy_result", FunctionDescriptor.ofVoid(ADDRESS));
        MethodHandle columnCount = fn("duckdb_column_count", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
        MethodHandle columnType = fn("duckdb_column_logical_type", FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG));
        MethodHandle columnName = fn("duckdb_column_name", FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG));
        MethodHandle typeId = fn("duckdb_get_type_id", FunctionDescriptor.of(JAVA_INT, ADDRESS));
        MethodHandle options = fn("duckdb_connection_get_arrow_options", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS));
        MethodHandle toSchema = fn("duckdb_to_arrow_schema", FunctionDescriptor.of(ADDRESS, ADDRESS, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS));
        MethodHandle fetch = fn("duckdb_fetch_chunk", FunctionDescriptor.of(ADDRESS, RESULT));
        MethodHandle toArrow = fn("duckdb_data_chunk_to_arrow", FunctionDescriptor.of(ADDRESS, ADDRESS, ADDRESS, ADDRESS));
        MethodHandle destroyChunk = fn("duckdb_destroy_data_chunk", FunctionDescriptor.ofVoid(ADDRESS));

        Arena arena = Arena.global();
        MemorySegment db = arena.allocate(ADDRESS), conn = arena.allocate(ADDRESS);
        int s1 = (int) open.invokeExact(MemorySegment.NULL, db);
        int s2 = (int) connect.invokeExact(db.get(ADDRESS, 0), conn);
        MemorySegment c = conn.get(ADDRESS, 0);
        MemorySegment opts = arena.allocate(ADDRESS);
        options.invokeExact(c, opts);
        List<String> lines = Files.readAllLines(Path.of(a[2]));   // name<TAB>sql
        for (String line : lines) {
            String[] t = line.split("\t", 2);
            MemorySegment r = arena.allocate(RESULT);
            if ((int) query.invokeExact(c, arena.allocateFrom(t[1]), r) != 0) throw new IllegalStateException(t[1]);
            long nc = (long) columnCount.invokeExact(r);
            MemorySegment types = arena.allocate(ADDRESS, nc), names = arena.allocate(ADDRESS, nc);
            int[] duckTypes = new int[(int) nc];
            for (long i = 0; i < nc; i++) {
                MemorySegment lt = (MemorySegment) columnType.invokeExact(r, i);
                types.setAtIndex(ADDRESS, i, lt);
                duckTypes[(int) i] = (int) typeId.invokeExact(lt);
                names.setAtIndex(ADDRESS, i, (MemorySegment) columnName.invokeExact(r, i));
            }
            MemorySegment schema = arena.allocate(72, 8);
            MemorySegment e1 = (MemorySegment) toSchema.invokeExact(opts.get(ADDRESS, 0), types, names, nc, schema);
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            boolean first = true;
            MemorySegment array = arena.allocate(80, 8), cp = arena.allocate(ADDRESS);
            MemorySegment chunk;
            List<Node> cols = null;
            while (!(chunk = (MemorySegment) fetch.invokeExact(r)).equals(MemorySegment.NULL)) {
                MemorySegment e2 = (MemorySegment) toArrow.invokeExact(opts.get(ADDRESS, 0), chunk, array);
                MemorySegment sKids = schema.get(ADDRESS, S_CHILDREN).reinterpret(8 * nc);
                MemorySegment aKids = array.get(ADDRESS, A_CHILDREN).reinterpret(8 * nc);
                cols = new ArrayList<>();
                for (int i = 0; i < nc; i++) {
                    cols.add(node(sKids.get(ADDRESS, 8L * i).reinterpret(72), aKids.get(ADDRESS, 8L * i).reinterpret(80), duckTypes[i]));
                }
                if (first) { stream.writeBytes(schemaMessage(cols)); first = false; }
                stream.writeBytes(batchMessage(cols, array.get(JAVA_LONG, A_LENGTH)));
                RELEASE.invokeExact(array.get(ADDRESS, A_RELEASE), array);
                cp.set(ADDRESS, 0, chunk);
                destroyChunk.invokeExact(cp);
            }
            stream.writeBytes(new byte[] {-1, -1, -1, -1, 0, 0, 0, 0});
            Files.write(out.resolve(t[0] + ".arrows"), stream.toByteArray());
            RELEASE.invokeExact(schema.get(ADDRESS, S_RELEASE), schema);
            destroyResult.invokeExact(r);
        }
        System.out.println("wrote " + lines.size() + " streams");
    }

    static final int DUCKDB_TYPE_UHUGEINT = 32;

    /** One column (recursively), after our decisions. Buffers are copied out of DuckDB's memory. */
    static Node node(MemorySegment s, MemorySegment a, int duckType) {
        String fmt = s.get(ADDRESS, S_FORMAT).reinterpret(Long.MAX_VALUE).getString(0);
        String name = s.get(ADDRESS, S_NAME).reinterpret(Long.MAX_VALUE).getString(0);
        boolean nullable = (s.get(JAVA_LONG, S_FLAGS) & 2) != 0;
        long n = a.get(JAVA_LONG, A_LENGTH), nulls = a.get(JAVA_LONG, A_NULLS);
        if (a.get(JAVA_LONG, A_OFFSET) != 0) throw new IllegalStateException("sliced arrays: not in the probe");
        long nb = a.get(JAVA_LONG, A_NBUF);
        MemorySegment bufs = a.get(ADDRESS, A_BUFFERS).reinterpret(8 * nb);
        byte[] validity = nulls == 0 ? new byte[0] : copy(bufs.get(ADDRESS, 0), (n + 7) / 8);
        MemorySegment dict = a.get(ADDRESS, A_DICT);
        if (!dict.equals(MemorySegment.NULL)) {   // ENUM: indices into a dictionary of strings -> plain text
            MemorySegment d = dict.reinterpret(80);
            MemorySegment dbufs = d.get(ADDRESS, A_BUFFERS).reinterpret(24);
            long dn = d.get(JAVA_LONG, A_LENGTH);
            MemorySegment doffs = dbufs.get(ADDRESS, 8).reinterpret(4 * (dn + 1));
            MemorySegment ddata = dbufs.get(ADDRESS, 16).reinterpret(doffs.get(JAVA_INT, 4 * dn));
            int width = switch (fmt) { case "c", "C" -> 1; case "s", "S" -> 2; default -> 4; };
            MemorySegment idx = bufs.get(ADDRESS, 8).reinterpret(n * width);
            ByteArrayOutputStream offs = new ByteArrayOutputStream(), data = new ByteArrayOutputStream();
            int pos = 0;
            putInt(offs, 0);
            for (long i = 0; i < n; i++) {
                boolean valid = validity.length == 0 || (validity[(int) (i >>> 3)] >> (i & 7) & 1) == 1;
                if (valid) {
                    long k = width == 1 ? Byte.toUnsignedLong(idx.get(JAVA_BYTE, i)) : width == 2 ? Short.toUnsignedLong(idx.get(JAVA_SHORT_UNALIGNED, 2 * i)) : Integer.toUnsignedLong(idx.get(JAVA_INT_UNALIGNED, 4 * i));
                    int from = doffs.get(JAVA_INT, 4 * k), to = doffs.get(JAVA_INT, 4 * k + 4);
                    data.writeBytes(ddata.asSlice(from, to - from).toArray(JAVA_BYTE));
                    pos += to - from;
                }
                putInt(offs, pos);
            }
            return new Node("u", name, nullable, List.of(), null, n, nulls, List.of(validity, offs.toByteArray(), data.toByteArray()));
        }
        if (duckType == DUCKDB_TYPE_UHUGEINT) {   // 128-bit unsigned: no Arrow integer holds it -> its decimal text
            MemorySegment v = bufs.get(ADDRESS, 1 * 8).reinterpret(16 * n);
            ByteArrayOutputStream offs = new ByteArrayOutputStream(), data = new ByteArrayOutputStream();
            putInt(offs, 0);
            for (long i = 0; i < n; i++) {
                boolean valid = validity.length == 0 || (validity[(int) (i >>> 3)] >> (i & 7) & 1) == 1;
                if (valid) {
                    byte[] le = v.asSlice(16 * i, 16).toArray(JAVA_BYTE);
                    byte[] be = new byte[17];
                    for (int k = 0; k < 16; k++) be[16 - k] = le[k];
                    data.writeBytes(new BigInteger(be).toString().getBytes(StandardCharsets.UTF_8));
                }
                putInt(offs, data.size());
            }
            return new Node("u", name, nullable, List.of(), null, n, nulls, List.of(validity, offs.toByteArray(), data.toByteArray()));
        }
        List<byte[]> out = new ArrayList<>();
        out.add(validity);
        List<Node> kids = new ArrayList<>();
        long nk = s.get(JAVA_LONG, S_NCHILD);
        MemorySegment sk = nk == 0 ? MemorySegment.NULL : s.get(ADDRESS, S_CHILDREN).reinterpret(8 * nk);
        MemorySegment ak = nk == 0 ? MemorySegment.NULL : a.get(ADDRESS, A_CHILDREN).reinterpret(8 * nk);
        for (long i = 0; i < nk; i++) kids.add(node(sk.get(ADDRESS, 8 * i).reinterpret(72), ak.get(ADDRESS, 8 * i).reinterpret(80), -1));
        String tz = null;
        if (fmt.equals("u") || fmt.equals("z")) {
            MemorySegment offs = bufs.get(ADDRESS, 8).reinterpret(4 * (n + 1));
            out.add(offs.toArray(JAVA_BYTE));
            out.add(copy(bufs.get(ADDRESS, 16), offs.get(JAVA_INT, 4 * n)));
        } else if (fmt.equals("+l") || fmt.equals("+m")) {
            out.add(copy(bufs.get(ADDRESS, 8), 4 * (n + 1)));
        } else if (fmt.equals("+s") || fmt.startsWith("+w:")) {
            // validity only
        } else if (fmt.equals("b")) {
            out.add(copy(bufs.get(ADDRESS, 8), (n + 7) / 8));
        } else {
            out.add(copy(bufs.get(ADDRESS, 8), n * width(fmt)));
            if (fmt.startsWith("ts")) { tz = fmt.length() > 4 ? "UTC" : null; fmt = fmt.substring(0, 4); }
        }
        return new Node(fmt, name, nullable, kids, tz, n, nulls, out);
    }

    static int width(String f) {
        if (f.startsWith("d:")) return 16;
        return switch (f.substring(0, Math.min(f.length(), 3))) {
            case "c", "C" -> 1; case "s", "S" -> 2; case "i", "I", "f", "tdD" -> 4; case "tin" -> 16;
            default -> switch (f.charAt(0)) { case 'l', 'L', 'g' -> 8; case 't' -> 8; default -> throw new IllegalStateException("width of " + f); };
        };
    }

    static byte[] copy(MemorySegment p, long bytes) { return p.reinterpret(bytes).toArray(JAVA_BYTE); }
    static void putInt(ByteArrayOutputStream o, int v) { for (int i = 0; i < 4; i++) o.write(v >>> (8 * i)); }

    // -- IPC framing with our FlatBuilder ---------------------------------------------------------

    static int field(FlatBuilder b, Node n) {
        int[] kids = new int[n.children().size()];
        for (int i = 0; i < kids.length; i++) kids[i] = field(b, n.children().get(i));
        int kv = b.offsets(kids);
        int nameAt = b.string(n.name());
        int[] tt = typeTable(b, n);
        b.startTable(7);
        b.fieldOffset(0, nameAt);
        b.fieldBool(1, n.nullable());
        b.fieldByte(2, tt[0]);
        b.fieldOffset(3, tt[1]);
        b.fieldOffset(5, kv);
        return b.endTable();
    }

    /** {Type union id, type table} for a C format string. */
    static int[] typeTable(FlatBuilder b, Node n) {
        String f = n.format();
        int tz = n.tz() != null ? b.string(n.tz()) : 0;
        int id;
        switch (f.charAt(0)) {
            case 'c', 'C', 's', 'S', 'i', 'I', 'l', 'L' -> {
                id = 2; b.startTable(2); b.fieldInt(0, 8 * width(f)); b.fieldBool(1, Character.isLowerCase(f.charAt(0)));
            }
            case 'f', 'g' -> { id = 3; b.startTable(1); b.fieldShort(0, f.equals("f") ? 1 : 2); }
            case 'u' -> { id = 5; b.startTable(0); }
            case 'z' -> { id = 4; b.startTable(0); }
            case 'b' -> { id = 6; b.startTable(0); }
            case 'd' -> {
                String[] ps = f.substring(2).split(",");
                id = 7; b.startTable(3); b.fieldInt(0, Integer.parseInt(ps[0])); b.fieldInt(1, Integer.parseInt(ps[1])); b.fieldInt(2, 128);
            }
            case 't' -> {
                switch (f.substring(0, 2)) {
                    case "td" -> { id = 8; b.startTable(1); b.fieldShort(0, 0); }
                    case "tt" -> { id = 9; b.startTable(2); b.fieldShort(0, 2); b.fieldInt(1, 64); }
                    case "ti" -> { id = 11; b.startTable(1); b.fieldShort(0, 2); }
                    default -> {
                        id = 10; b.startTable(2);
                        b.fieldShort(0, switch (f.charAt(2)) { case 's' -> 0; case 'm' -> 1; case 'n' -> 3; default -> 2; });
                        if (n.tz() != null) b.fieldOffset(1, tz);
                    }
                }
            }
            case '+' -> {
                switch (f.charAt(1)) {
                    case 'l' -> { id = 12; b.startTable(0); }
                    case 's' -> { id = 13; b.startTable(0); }
                    case 'm' -> { id = 17; b.startTable(1); b.fieldBool(0, false); }
                    case 'w' -> { id = 16; b.startTable(1); b.fieldInt(0, Integer.parseInt(f.substring(3))); }
                    default -> throw new IllegalStateException("format " + f);
                }
            }
            default -> throw new IllegalStateException("format " + f);
        }
        return new int[] {id, b.endTable()};
    }

    static byte[] schemaMessage(List<Node> cols) {
        FlatBuilder b = new FlatBuilder();
        int[] fs = new int[cols.size()];
        for (int i = 0; i < fs.length; i++) fs[i] = field(b, cols.get(i));
        int fv = b.offsets(fs);
        b.startTable(4); b.fieldShort(0, 0); b.fieldOffset(1, fv);
        int sch = b.endTable();
        return frame(message(b, (byte) 1, sch, 0), new byte[0]);
    }

    static void layout(Node n, List<long[]> nodes, List<byte[]> bufs) {
        nodes.add(new long[] {n.length(), n.nulls()});
        bufs.addAll(n.buffers());
        for (Node k : n.children()) layout(k, nodes, bufs);
    }

    static byte[] batchMessage(List<Node> cols, long rows) {
        List<long[]> nodes = new ArrayList<>();
        List<byte[]> bufs = new ArrayList<>();
        for (Node c : cols) layout(c, nodes, bufs);
        long[] off = new long[bufs.size()], len = new long[bufs.size()];
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        for (int i = 0; i < bufs.size(); i++) {
            off[i] = body.size(); len[i] = bufs.get(i).length;
            body.writeBytes(bufs.get(i));
            body.writeBytes(new byte[(int) (pad8(len[i]) - len[i])]);
        }
        long[] nl = new long[nodes.size()], nn = new long[nodes.size()];
        for (int i = 0; i < nodes.size(); i++) { nl[i] = nodes.get(i)[0]; nn[i] = nodes.get(i)[1]; }
        FlatBuilder b = new FlatBuilder();
        int nv = b.longPairs(nl, nn), bv = b.longPairs(off, len);
        b.startTable(5); b.fieldLong(0, rows); b.fieldOffset(1, nv); b.fieldOffset(2, bv);
        int rb = b.endTable();
        return frame(message(b, (byte) 3, rb, body.size()), body.toByteArray());
    }

    static byte[] message(FlatBuilder b, byte type, int header, long bodyLength) {
        b.startTable(5); b.fieldLong(3, bodyLength); b.fieldOffset(2, header); b.fieldShort(0, 4); b.fieldByte(1, type);
        return b.finish(b.endTable());
    }

    static byte[] frame(byte[] meta, byte[] body) {
        int padded = (int) pad8(8 + meta.length) - 8;
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        putInt(o, -1); putInt(o, padded); o.writeBytes(meta); o.writeBytes(new byte[padded - meta.length]); o.writeBytes(body);
        return o.toByteArray();
    }

    static long pad8(long n) { return (n + 7) & ~7L; }
}
