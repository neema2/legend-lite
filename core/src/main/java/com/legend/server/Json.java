package com.legend.server;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Canonical zero-dependency JSON utility for legend-lite.
 *
 * <p><b>Two-parser adjudication (Pile-1 audit, 2026-08-18):</b> this is
 * the STRICT RFC 8259 boundary parser (Node AST, depth limits, the
 * rejection set its tests pin) — {@code com.legend.sql.Json} is the
 * deliberately LENIENT data-bridge decoder (plain Java values, the
 * leading-zero/trailing-dot leniencies TdsChecker's validator depends
 * on, audit-18 BigDecimal exactness). Two contracts, each behavior-
 * pinned; registered as distinct owners on purpose — a merge would need
 * a mode flag and would move corpus classifications. Keep the two docs
 * cross-referenced; grammar fixes must consider both.
 *
 * <h2>Design</h2>
 * <ul>
 *   <li><b>Zero external deps</b> — GraalVM native-image friendly.</li>
 *   <li><b>Streaming Writer</b> — backed by any {@link Appendable} so output can flow
 *       directly to an {@code OutputStream}, {@code Writer}, or an {@code HttpExchange}
 *       response body without materializing the result in memory. A {@link StringBuilder}-backed
 *       variant is provided for convenience.</li>
 *   <li><b>Sealed Node tree</b> for parsed JSON — type-safe, JIT-friendly pattern matching,
 *       zero reflection, zero casting at call sites.</li>
 *   <li><b>Deterministic output</b> — {@link Obj} uses {@link LinkedHashMap} so field
 *       emission order is preserved. Required for golden byte-equality tests.</li>
 *   <li><b>Pretty or compact output</b> — pretty mode uses 2-space indentation and
 *       newlines; compact mode emits no whitespace (wire-efficient).</li>
 *   <li><b>Strict parser</b> — RFC 8259 compliant, fails fast on malformed input
 *       with line/column position, rejects trailing content, rejects unescaped
 *       control characters in strings.</li>
 *   <li><b>Configurable depth limit</b> (default 64) — prevents stack overflow from
 *       malicious deeply-nested input.</li>
 *   <li><b>Public escape/unescape</b> — the escape TABLE itself lives once in
 *       {@code protocol/Escapes.jsonEscape} (F3.1c); this class exposes the
 *       server-facing wrappers. The strict READER here is the documented
 *       HTTP-boundary exemption to the platform's one reader
 *       ({@code sql/Json}) — different policy on purpose (fail-fast with
 *       positions vs lenient wire reading).</li>
 * </ul>
 *
 * <h2>Typical usage</h2>
 *
 * <h3>Streaming output (query results, HTTP responses)</h3>
 * <pre>
 * try (var resp = new OutputStreamWriter(exchange.getResponseBody(), UTF_8)) {
 *     Json.Writer w = Json.writer(resp);   // streaming, pretty
 *     w.beginArray();
 *     while (resultSet.next()) {
 *         w.beginObject();
 *         w.field("id", resultSet.getLong("id"));
 *         w.field("name", resultSet.getString("name"));
 *         w.endObject();
 *     }
 *     w.endArray();
 * }  // no materialization — bytes flow to client as written
 * </pre>
 *
 * <h3>Ad-hoc serialization</h3>
 * <pre>
 * Map&lt;String, Object&gt; payload = Map.of("id", 42, "name", "test");
 * String json = Json.toCompact(payload);
 * </pre>
 *
 * <h3>Parsing</h3>
 * <pre>
 * Json.Obj root = Json.parseObject(text);
 * String name = root.getString("name");
 * long id = root.getLong("id");
 * </pre>
 */
public final class Json {

    private Json() {}

    /** Default maximum nesting depth for parsing. */
    public static final int DEFAULT_MAX_DEPTH = 64;

    // ==================== Config ====================

    /**
     * Parser configuration.
     *
     * @param maxDepth Maximum nesting depth. Inputs exceeding this throw
     *                 {@link IllegalArgumentException}. Default: {@link #DEFAULT_MAX_DEPTH}.
     */
    public record Config(int maxDepth) {
        public static final Config DEFAULT = new Config(DEFAULT_MAX_DEPTH);

        public Config {
            if (maxDepth < 1) throw new IllegalArgumentException("maxDepth must be >= 1");
        }
    }

    // ==================== Tree model ====================

    /** Sealed JSON tree node. */
    public sealed interface Node permits Obj, Arr, Str, Num, Bool, Null {}

    /** Object node — insertion-ordered for deterministic output. */
    public record Obj(LinkedHashMap<String, Node> fields) implements Node {
        public Obj { fields = new LinkedHashMap<>(fields); }

        public boolean has(String key) { return fields.containsKey(key); }

        public Node get(String key) {
            Node n = fields.get(key);
            if (n == null) throw new IllegalArgumentException("Missing required field: " + key);
            return n;
        }

        /**
         * Return the value for {@code key}, or {@code def} if the field is missing
         * <em>or</em> present with a JSON null value. Consistent with the typed
         * {@code *Or} variants (getStringOr, getIntOr, ...) which all treat
         * both cases as "use the default".
         */
        public @com.legend.Nullable Node getOr(String key, @com.legend.Nullable Node def) {
            Node n = fields.get(key);
            return (n == null || n instanceof Null) ? def : n;
        }

        public String getString(String key) { return ((Str) get(key)).value(); }

        public @com.legend.Nullable String getStringOr(String key, @com.legend.Nullable String def) {
            Node n = fields.get(key);
            return n == null || n instanceof Null ? def : ((Str) n).value();
        }

        public int getInt(String key) { return (int) ((Num) get(key)).longValue(); }

        public int getIntOr(String key, int def) {
            Node n = fields.get(key);
            return n == null || n instanceof Null ? def : (int) ((Num) n).longValue();
        }

        public long getLong(String key) { return ((Num) get(key)).longValue(); }

        public long getLongOr(String key, long def) {
            Node n = fields.get(key);
            return n == null || n instanceof Null ? def : ((Num) n).longValue();
        }

        public double getDouble(String key) { return ((Num) get(key)).doubleValue(); }

        public boolean getBool(String key) { return ((Bool) get(key)).value(); }

        public boolean getBoolOr(String key, boolean def) {
            Node n = fields.get(key);
            return n == null || n instanceof Null ? def : ((Bool) n).value();
        }

        public Obj getObj(String key) { return (Obj) get(key); }

        public @com.legend.Nullable Obj getObjOr(String key, @com.legend.Nullable Obj def) {
            Node n = fields.get(key);
            return n == null || n instanceof Null ? def : (Obj) n;
        }

        public Arr getArr(String key) { return (Arr) get(key); }

        public @com.legend.Nullable Arr getArrOr(String key, @com.legend.Nullable Arr def) {
            Node n = fields.get(key);
            return n == null || n instanceof Null ? def : (Arr) n;
        }

        public List<String> getStringArray(String key) {
            Arr a = getArr(key);
            List<String> out = new ArrayList<>(a.items().size());
            for (Node n : a.items()) out.add(((Str) n).value());
            return out;
        }

        public List<String> getStringArrayOr(String key, List<String> def) {
            Node n = fields.get(key);
            if (n == null || n instanceof Null) return def;
            return getStringArray(key);
        }
    }

    /** Array node. */
    public record Arr(List<Node> items) implements Node {
        public Arr { items = List.copyOf(items); }
    }

    /** String node. */
    public record Str(String value) implements Node {
        public Str { Objects.requireNonNull(value, "String node value cannot be null"); }
    }

    /** Number node — stored as long if integral, else double. */
    public record Num(long longValue, double doubleValue, boolean isInteger,
            java.math.@com.legend.Nullable BigDecimal decimalValue) implements Node {
        /** Pre-F3.1a arity — no exact decimal available. */
        public Num(long longValue, double doubleValue, boolean isInteger) {
            this(longValue, doubleValue, isInteger, null);
        }
        public static Num ofLong(long v) { return new Num(v, (double) v, true); }
        public static Num ofDouble(double v) {
            if (Double.isNaN(v) || Double.isInfinite(v)) {
                throw new IllegalArgumentException("JSON cannot represent NaN or Infinity");
            }
            return new Num((long) v, v, false);
        }
        /** F3.1a (audit A15/audit 18): the EXACT decimal token — two
         * distinct Decimals beyond 17 significant digits round to the
         * SAME double, so the double channel alone corrupts round trips.
         * doubleValue stays populated for the informational getters. */
        public static Num ofDecimal(java.math.BigDecimal v) {
            return new Num(v.longValue(), v.doubleValue(), false, v);
        }
    }

    /** Boolean node. */
    public record Bool(boolean value) implements Node {}

    /** Null node (singleton). */
    public record Null() implements Node {
        public static final Null INSTANCE = new Null();
    }

    // ==================== Node construction ====================

    public static Obj obj() { return new Obj(new LinkedHashMap<>()); }

    public static Arr arr() { return new Arr(List.of()); }

    public static Str str(String v) { return new Str(v); }

    public static Num num(long v) { return Num.ofLong(v); }

    public static Num num(double v) { return Num.ofDouble(v); }

    public static Bool bool(boolean v) { return new Bool(v); }

    public static Null nil() { return Null.INSTANCE; }

    /**
     * Coerce an arbitrary value into a {@link Node} tree.
     *
     * <p>Dispatch rules:
     * <ul>
     *   <li>{@code null} → {@link Null#INSTANCE}</li>
     *   <li>{@link Node} → returned as-is</li>
     *   <li>{@link String} → {@link Str}</li>
     *   <li>{@link Number} → {@link Num} (long for integral types, double otherwise)</li>
     *   <li>{@link Boolean} → {@link Bool}</li>
     *   <li>{@link Map} (String keys) → {@link Obj}</li>
     *   <li>{@link Collection} or array → {@link Arr}</li>
     * </ul>
     *
     * @throws IllegalArgumentException if the value or any nested element is unsupported.
     */
    public static Node of(Object v) {
        if (v == null) return Null.INSTANCE;
        if (v instanceof Node n) return n;
        if (v instanceof String s) return new Str(s);
        if (v instanceof Boolean b) return new Bool(b);
        if (v instanceof Number num) {
            if (num instanceof Long || num instanceof Integer || num instanceof Short || num instanceof Byte) {
                return Num.ofLong(num.longValue());
            }
            if (num instanceof java.math.BigDecimal bd) {
                return Num.ofDecimal(bd);   // F3.1a: never through double
            }
            if (num instanceof java.math.BigInteger bi) {
                return Num.ofDecimal(new java.math.BigDecimal(bi));
            }
            return Num.ofDouble(num.doubleValue());
        }
        if (v instanceof Map<?, ?> m) {
            LinkedHashMap<String, Node> fields = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : m.entrySet()) {
                if (!(e.getKey() instanceof String key)) {
                    throw new IllegalArgumentException("JSON object keys must be strings, got "
                            + (e.getKey() == null ? "null" : e.getKey().getClass().getSimpleName()));
                }
                fields.put(key, of(e.getValue()));
            }
            return new Obj(fields);
        }
        if (v instanceof Collection<?> c) {
            List<Node> items = new ArrayList<>(c.size());
            for (Object item : c) items.add(of(item));
            return new Arr(items);
        }
        if (v instanceof Object[] arr) {
            List<Node> items = new ArrayList<>(arr.length);
            for (Object item : arr) items.add(of(item));
            return new Arr(items);
        }
        if (v instanceof int[] || v instanceof long[] || v instanceof double[] || v instanceof boolean[]) {
            List<Node> items = new ArrayList<>();
            for (Object item : boxed(v)) items.add(of(item));
            return new Arr(items);
        }
        throw new IllegalArgumentException("Cannot coerce to JSON node: " + v.getClass().getName());
    }

    // ==================== Serialization ====================

    /** Serialize any supported value to compact JSON (no whitespace). */
    public static String toCompact(Object value) {
        Writer w = compactWriter();
        writeValue(w, value);
        return w.toString();
    }

    /** Serialize any supported value to pretty-printed JSON (2-space indent). */
    public static String toPretty(Object value) {
        Writer w = writer();
        writeValue(w, value);
        return w.toString();
    }

    /** Stream any supported value into an existing Writer. */
    public static void write(Writer out, Object value) {
        writeValue(out, value);
    }

    /** Emits a value. Accepts Node | Map | Collection | array | String | Number | Boolean | null. */
    private static void writeValue(Writer w, Object v) {
        if (v == null) { w.writeNull(); return; }
        if (v instanceof Node n) { writeNode(w, n); return; }
        if (v instanceof String s) { w.writeString(s); return; }
        if (v instanceof Boolean b) { w.writeBool(b); return; }
        if (v instanceof Number num) {
            if (num instanceof Long || num instanceof Integer || num instanceof Short || num instanceof Byte) {
                w.writeLong(num.longValue());
            } else if (num instanceof java.math.BigDecimal bd) {
                w.writeDecimal(bd);   // F3.1a: never through double
            } else if (num instanceof java.math.BigInteger bi) {
                w.writeDecimal(new java.math.BigDecimal(bi));
            } else {
                w.writeDouble(num.doubleValue());
            }
            return;
        }
        if (v instanceof Map<?, ?> m) {
            w.beginObject();
            for (Map.Entry<?, ?> e : m.entrySet()) {
                if (!(e.getKey() instanceof String key)) {
                    throw new IllegalArgumentException("JSON object keys must be strings, got "
                            + (e.getKey() == null ? "null" : e.getKey().getClass().getSimpleName()));
                }
                w.name(key);
                writeValue(w, e.getValue());
            }
            w.endObject();
            return;
        }
        if (v instanceof Collection<?> c) {
            w.beginArray();
            for (Object item : c) writeValue(w, item);
            w.endArray();
            return;
        }
        if (v instanceof Object[] arr) {
            w.beginArray();
            for (Object item : arr) writeValue(w, item);
            w.endArray();
            return;
        }
        if (v instanceof int[] || v instanceof long[] || v instanceof double[] || v instanceof boolean[]) {
            w.beginArray();
            for (Object item : boxed(v)) writeValue(w, item);
            w.endArray();
            return;
        }
        throw new IllegalArgumentException("Cannot serialize to JSON: " + v.getClass().getName());
    }

    /** The primitive array kinds this serializer accepts, boxed — named
     * one by one; no reflective array access (the architecture bans
     * java.lang.reflect in the product). */
    private static List<Object> boxed(Object primitiveArray) {
        List<Object> out = new ArrayList<>();
        switch (primitiveArray) {
            case int[] a -> { for (int x : a) out.add(x); }
            case long[] a -> { for (long x : a) out.add(x); }
            case double[] a -> { for (double x : a) out.add(x); }
            case boolean[] a -> { for (boolean x : a) out.add(x); }
            default -> throw new IllegalArgumentException(
                    "Cannot serialize to JSON: " + primitiveArray.getClass().getName());
        }
        return out;
    }

    private static void writeNode(Writer w, Node n) {
        switch (n) {
            case Obj o -> {
                w.beginObject();
                for (Map.Entry<String, Node> e : o.fields().entrySet()) {
                    w.name(e.getKey());
                    writeNode(w, e.getValue());
                }
                w.endObject();
            }
            case Arr a -> {
                w.beginArray();
                for (Node item : a.items()) writeNode(w, item);
                w.endArray();
            }
            case Str s -> w.writeString(s.value());
            case Num num -> {
                if (num.isInteger()) w.writeLong(num.longValue());
                else if (num.decimalValue() != null) {
                    w.writeDecimal(num.decimalValue());
                } else {
                    w.writeDouble(num.doubleValue());
                }
            }
            case Bool b -> w.writeBool(b.value());
            case Null ignored -> w.writeNull();
        }
    }

    // ==================== Writer factories ====================

    /** New pretty-print writer backed by a StringBuilder; call {@link Writer#toString()} for the result. */
    public static Writer writer() { return new Writer(new StringBuilder(256), true); }

    /** New pretty-print writer streaming into the given Appendable. */
    public static Writer writer(Appendable out) {
        Objects.requireNonNull(out);
        return new Writer(out, true);
    }

    /** New compact (no whitespace) writer backed by a StringBuilder. */
    public static Writer compactWriter() { return new Writer(new StringBuilder(256), false); }

    /** New compact writer streaming into the given Appendable. */
    public static Writer compactWriter(Appendable out) {
        Objects.requireNonNull(out);
        return new Writer(out, false);
    }

    // ==================== Writer ====================

    /**
     * Streaming JSON writer. Stateful — callers must balance begin/end calls.
     *
     * <p>Backed by any {@link Appendable}; IOExceptions are wrapped in
     * {@link UncheckedIOException}. A {@link StringBuilder}-backed variant is
     * exposed via {@link Json#writer()} / {@link Json#compactWriter()} and
     * supports {@link #toString()} to retrieve the final text.
     */
    public static final class Writer {
        private final Appendable out;
        private final boolean pretty;
        private final ArrayDeque<Frame> stack = new ArrayDeque<>();
        private boolean awaitingFieldValue = false;

        private enum Kind { OBJECT, ARRAY }

        private static final class Frame {
            final Kind kind;
            int count = 0;
            Frame(Kind k) { this.kind = k; }
        }

        private Writer(Appendable out, boolean pretty) {
            this.out = out;
            this.pretty = pretty;
        }

        public Writer beginObject() {
            preWrite();
            append('{');
            stack.push(new Frame(Kind.OBJECT));
            return this;
        }

        public Writer endObject() {
            Frame f = stack.pop();
            if (f.kind != Kind.OBJECT) throw new IllegalStateException("endObject without matching beginObject");
            if (f.count > 0 && pretty) {
                append('\n');
                indent();
            }
            append('}');
            return this;
        }

        public Writer beginArray() {
            preWrite();
            append('[');
            stack.push(new Frame(Kind.ARRAY));
            return this;
        }

        public Writer endArray() {
            Frame f = stack.pop();
            if (f.kind != Kind.ARRAY) throw new IllegalStateException("endArray without matching beginArray");
            if (f.count > 0 && pretty) {
                append('\n');
                indent();
            }
            append(']');
            return this;
        }

        /** Emits a field name followed by {@code : }; caller must emit the value next. */
        public Writer name(String n) {
            Frame f = stack.peek();
            if (f == null || f.kind != Kind.OBJECT) throw new IllegalStateException("name() only valid inside an object");
            if (f.count > 0) append(',');
            if (pretty) {
                append('\n');
                f.count++;
                indent();
            } else {
                f.count++;
            }
            append('"');
            escapeTo(out, n);
            append('"');
            append(':');
            if (pretty) append(' ');
            awaitingFieldValue = true;
            return this;
        }

        // --- convenience field emitters (object context only) ---

        public Writer field(String n, String value) {
            name(n);
            if (value == null) writeNull(); else writeString(value);
            return this;
        }

        public Writer field(String n, long value) { name(n); writeLong(value); return this; }
        public Writer field(String n, double value) { name(n); writeDouble(value); return this; }
        public Writer field(String n, boolean value) { name(n); writeBool(value); return this; }
        public Writer fieldNull(String n) { name(n); writeNull(); return this; }

        public Writer fieldStringArray(String n, Iterable<String> values) {
            name(n);
            beginArray();
            for (String v : values) writeString(v);
            endArray();
            return this;
        }

        // --- scalar value emitters ---

        public Writer writeString(String v) {
            preWrite();
            if (v == null) {
                append("null");
            } else {
                append('"');
                escapeTo(out, v);
                append('"');
            }
            return this;
        }

        public Writer writeLong(long v) { preWrite(); append(Long.toString(v)); return this; }

        /** F3.1a: the exact decimal token — BigDecimal.toString is a
         * valid JSON number (may carry an exponent), and NEVER rounds. */
        public Writer writeDecimal(java.math.BigDecimal v) {
            preWrite();
            append(v.toString());
            return this;
        }

        public Writer writeDouble(double v) {
            preWrite();
            if (Double.isNaN(v) || Double.isInfinite(v)) {
                throw new IllegalArgumentException("JSON cannot represent NaN or Infinity");
            }
            if (v == Math.floor(v) && !Double.isInfinite(v) && Math.abs(v) < 1e19) {
                append(Long.toString((long) v));
            } else {
                append(Double.toString(v));
            }
            return this;
        }

        public Writer writeBool(boolean v) { preWrite(); append(v ? "true" : "false"); return this; }

        public Writer writeNull() { preWrite(); append("null"); return this; }

        /**
         * Returns the accumulated JSON text. Only supported when the Writer was constructed
         * with a {@link StringBuilder} backing (via {@link Json#writer()} or
         * {@link Json#compactWriter()}). For Appendable-backed writers, output goes
         * directly to the target; {@link #toString()} throws.
         */
        @Override
        public String toString() {
            if (!stack.isEmpty()) throw new IllegalStateException("JSON writer has unclosed containers: " + stack.size());
            if (out instanceof StringBuilder sb) return sb.toString();
            throw new IllegalStateException("toString() only supported for StringBuilder-backed Writers");
        }

        // --- internal ---

        private void preWrite() {
            if (awaitingFieldValue) { awaitingFieldValue = false; return; }
            Frame f = stack.peek();
            if (f == null) return; // root scalar
            if (f.kind == Kind.ARRAY) {
                if (f.count > 0) append(',');
                if (pretty) {
                    append('\n');
                    f.count++;
                    indent();
                } else {
                    f.count++;
                }
            }
            // OBJECT values without a name() call are caught by name() itself.
        }

        private void indent() {
            for (int i = 0; i < stack.size(); i++) append("  ");
        }

        private void append(char c) {
            try {
                out.append(c);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private void append(CharSequence s) {
            try {
                out.append(s);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    // ==================== Escape primitives (public) ====================

    /**
     * Escape a string for use as a JSON string value (between the surrounding quotes).
     * Escapes control characters (0x00-0x1F), quotes, and backslashes per RFC 8259.
     * Does NOT include the surrounding double-quotes.
     *
     * <p>This is the <b>single source of truth</b> for JSON string escaping across the
     * legend-lite codebase. Other implementations are being migrated away.
     */
    public static String escape(@com.legend.Nullable String s) {
        if (s == null) return "";
        StringBuilder out = new StringBuilder(s.length() + 8);
        escapeTo(out, s);
        return out.toString();
    }

    /** Streaming escape — emits escaped characters directly to an
     *  Appendable. F3.1c: the table lives ONCE in
     *  {@link com.legend.protocol.Escapes#jsonEscape} (lowercase hex —
     *  the protocol emitter's Jackson-parity uppercase is the one knob). */
    public static void escapeTo(Appendable out, String s) {
        if (s == null) return;
        try {
            com.legend.protocol.Escapes.jsonEscape(out, s, false);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Reverse of {@link #escape} — decodes escape sequences in a JSON-string payload. */
    public static @com.legend.Nullable String unescape(@com.legend.Nullable String s) {
        if (s == null) return null;
        StringBuilder out = new StringBuilder(s.length());
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i++);
            if (c != '\\') { out.append(c); continue; }
            if (i >= s.length()) throw new IllegalArgumentException("Bad escape at end of string");
            char e = s.charAt(i++);
            switch (e) {
                case '"'  -> out.append('"');
                case '\\' -> out.append('\\');
                case '/'  -> out.append('/');
                case 'n'  -> out.append('\n');
                case 'r'  -> out.append('\r');
                case 't'  -> out.append('\t');
                case 'b'  -> out.append('\b');
                case 'f'  -> out.append('\f');
                case 'u'  -> {
                    if (i + 4 > s.length()) throw new IllegalArgumentException("Bad unicode escape");
                    out.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
                    i += 4;
                }
                default -> throw new IllegalArgumentException("Invalid escape: \\" + e);
            }
        }
        return out.toString();
    }

    // ==================== Parser ====================

    /** Parse a JSON string. Equivalent to {@code parse(text, Config.DEFAULT)}. */
    public static Node parse(String text) { return parse(text, Config.DEFAULT); }

    /** Parse a JSON string with custom config. */
    public static Node parse(String text, Config config) {
        Parser p = new Parser(text, config);
        p.skipWs();
        Node n = p.parseValue();
        p.skipWs();
        if (p.pos < p.src.length()) {
            throw p.error("Unexpected trailing content");
        }
        return n;
    }

    /** Parse from a Reader (slurps to String; streaming parse is not yet supported). */
    public static Node parse(Reader reader) { return parse(slurp(reader), Config.DEFAULT); }

    /** Parse from a Reader with custom config. */
    public static Node parse(Reader reader, Config config) { return parse(slurp(reader), config); }

    /** Parse and require top-level object. */
    public static Obj parseObject(String text) {
        Node n = parse(text);
        if (!(n instanceof Obj o)) {
            throw new IllegalArgumentException("Expected top-level JSON object, got " + n.getClass().getSimpleName());
        }
        return o;
    }

    /** Parse from a Reader and require top-level object. */
    public static Obj parseObject(Reader reader) {
        Node n = parse(reader);
        if (!(n instanceof Obj o)) {
            throw new IllegalArgumentException("Expected top-level JSON object, got " + n.getClass().getSimpleName());
        }
        return o;
    }

    private static String slurp(Reader reader) {
        StringBuilder sb = new StringBuilder();
        char[] buf = new char[4096];
        try {
            int n;
            while ((n = reader.read(buf)) >= 0) sb.append(buf, 0, n);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return sb.toString();
    }

    private static final class Parser {
        final String src;
        final Config config;
        int pos;
        int depth = 0;

        Parser(String src, Config config) {
            this.src = src;
            this.config = config;
            this.pos = 0;
        }

        Node parseValue() {
            skipWs();
            if (pos >= src.length()) throw error("Unexpected end of input");
            char c = src.charAt(pos);
            return switch (c) {
                case '{' -> parseObject();
                case '[' -> parseArray();
                case '"' -> new Str(parseString());
                case 't', 'f' -> parseBool();
                case 'n' -> parseNull();
                default -> parseNumber();
            };
        }

        Obj parseObject() {
            expect('{');
            enterContainer();
            LinkedHashMap<String, Node> fields = new LinkedHashMap<>();
            skipWs();
            if (peek() == '}') { pos++; leaveContainer(); return new Obj(fields); }
            while (true) {
                skipWs();
                String key = parseString();
                skipWs();
                expect(':');
                Node value = parseValue();
                fields.put(key, value);
                skipWs();
                char c = next();
                if (c == ',') continue;
                if (c == '}') { leaveContainer(); return new Obj(fields); }
                throw error("Expected ',' or '}' in object");
            }
        }

        Arr parseArray() {
            expect('[');
            enterContainer();
            List<Node> items = new ArrayList<>();
            skipWs();
            if (peek() == ']') { pos++; leaveContainer(); return new Arr(items); }
            while (true) {
                items.add(parseValue());
                skipWs();
                char c = next();
                if (c == ',') continue;
                if (c == ']') { leaveContainer(); return new Arr(items); }
                throw error("Expected ',' or ']' in array");
            }
        }

        void enterContainer() {
            depth++;
            if (depth > config.maxDepth()) {
                throw error("Nesting depth exceeds limit of " + config.maxDepth());
            }
        }

        void leaveContainer() { depth--; }

        String parseString() {
            expect('"');
            StringBuilder out = new StringBuilder();
            while (pos < src.length()) {
                char c = src.charAt(pos++);
                if (c == '"') return out.toString();
                if (c == '\\') {
                    if (pos >= src.length()) throw error("Bad escape at end of string");
                    char e = src.charAt(pos++);
                    switch (e) {
                        case '"'  -> out.append('"');
                        case '\\' -> out.append('\\');
                        case '/'  -> out.append('/');
                        case 'n'  -> out.append('\n');
                        case 'r'  -> out.append('\r');
                        case 't'  -> out.append('\t');
                        case 'b'  -> out.append('\b');
                        case 'f'  -> out.append('\f');
                        case 'u'  -> {
                            if (pos + 4 > src.length()) throw error("Bad unicode escape");
                            out.append((char) Integer.parseInt(src.substring(pos, pos + 4), 16));
                            pos += 4;
                        }
                        default -> throw error("Invalid escape: \\" + e);
                    }
                } else if (c < 0x20) {
                    throw error("Unescaped control character in string: 0x" + Integer.toHexString(c));
                } else {
                    out.append(c);
                }
            }
            throw error("Unterminated string");
        }

        Bool parseBool() {
            if (src.startsWith("true", pos)) { pos += 4; return new Bool(true); }
            if (src.startsWith("false", pos)) { pos += 5; return new Bool(false); }
            throw error("Expected boolean literal");
        }

        Null parseNull() {
            if (src.startsWith("null", pos)) { pos += 4; return Null.INSTANCE; }
            throw error("Expected null literal");
        }

        Num parseNumber() {
            // RFC 8259 §6 grammar:
            //   number = [ "-" ] int [ frac ] [ exp ]
            //   int    = "0" / ( digit1-9 *DIGIT )             -- no leading zeros
            //   frac   = "." 1*DIGIT                            -- requires digits after "."
            //   exp    = ("e"|"E") [ "+"|"-" ] 1*DIGIT          -- requires digits after "e"
            int start = pos;
            if (peek() == '-') pos++;

            // Integer part: "0" or digit1-9 followed by any digits.
            if (pos >= src.length()) throw error("Invalid number: expected digit");
            char first = src.charAt(pos);
            if (first == '0') {
                pos++;
                // After leading zero, next character cannot be another digit.
                if (pos < src.length() && Character.isDigit(src.charAt(pos))) {
                    throw error("Leading zeros are not permitted in JSON numbers");
                }
            } else if (first >= '1' && first <= '9') {
                pos++;
                while (pos < src.length() && Character.isDigit(src.charAt(pos))) pos++;
            } else {
                throw error("Invalid number: expected digit, got '" + first + "'");
            }

            boolean isFloat = false;

            // Optional fraction.
            if (pos < src.length() && src.charAt(pos) == '.') {
                isFloat = true;
                pos++;
                if (pos >= src.length() || !Character.isDigit(src.charAt(pos))) {
                    throw error("Number fraction requires at least one digit after '.'");
                }
                while (pos < src.length() && Character.isDigit(src.charAt(pos))) pos++;
            }

            // Optional exponent.
            if (pos < src.length() && (src.charAt(pos) == 'e' || src.charAt(pos) == 'E')) {
                isFloat = true;
                pos++;
                if (pos < src.length() && (src.charAt(pos) == '+' || src.charAt(pos) == '-')) pos++;
                if (pos >= src.length() || !Character.isDigit(src.charAt(pos))) {
                    throw error("Number exponent requires at least one digit");
                }
                while (pos < src.length() && Character.isDigit(src.charAt(pos))) pos++;
            }

            String num = src.substring(start, pos);
            if (isFloat) return Num.ofDecimal(new java.math.BigDecimal(num));
            // an integer past a long (DuckDB writes UINT64's max) is still a JSON number: kept exact
            if (num.length() >= 19) {
                java.math.BigInteger big = new java.math.BigInteger(num);
                if (big.bitLength() > 63) return Num.ofDecimal(new java.math.BigDecimal(big));
            }
            return Num.ofLong(Long.parseLong(num));
        }

        void expect(char c) {
            if (pos >= src.length() || src.charAt(pos) != c) throw error("Expected '" + c + "'");
            pos++;
        }

        char next() {
            if (pos >= src.length()) throw error("Unexpected end of input");
            return src.charAt(pos++);
        }

        char peek() {
            return pos < src.length() ? src.charAt(pos) : '\0';
        }

        void skipWs() {
            while (pos < src.length()) {
                char c = src.charAt(pos);
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r') pos++;
                else break;
            }
        }

        IllegalArgumentException error(String msg) {
            int line = 1, col = 1;
            for (int i = 0; i < pos && i < src.length(); i++) {
                if (src.charAt(i) == '\n') { line++; col = 1; } else col++;
            }
            return new IllegalArgumentException(msg + " at line " + line + " col " + col);
        }
    }
}
