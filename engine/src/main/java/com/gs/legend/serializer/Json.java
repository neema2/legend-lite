package com.gs.legend.serializer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal self-contained JSON utility for Phase B element serialization.
 *
 * <p>Design goals:
 * <ul>
 *   <li>Zero external dependencies (GraalVM native-image friendly)</li>
 *   <li>Deterministic output ordering (required for golden byte-equality tests)</li>
 *   <li>Pretty-printed output with 2-space indentation (fixtures are human-reviewable diffs)</li>
 *   <li>Strict parser — unknown structure fails fast, no silent skipping</li>
 * </ul>
 *
 * <p>Two halves:
 * <ul>
 *   <li>{@link Writer} — stateful, stream-like object/array builder</li>
 *   <li>{@link Node} + {@link #parse(String)} — tree model for deserialization</li>
 * </ul>
 */
public final class Json {

    private Json() { }

    // ==================== Tree model ====================

    /** Sealed JSON tree node. */
    public sealed interface Node permits Obj, Arr, Str, Num, Bool, Null { }

    /** Object node — preserves insertion order for deterministic round-trip. */
    public record Obj(LinkedHashMap<String, Node> fields) implements Node {
        public Obj { fields = new LinkedHashMap<>(fields); }

        public boolean has(String key) { return fields.containsKey(key); }

        public Node get(String key) {
            Node n = fields.get(key);
            if (n == null) throw new IllegalArgumentException("Missing required field: " + key);
            return n;
        }

        public Node getOr(String key, Node def) {
            Node n = fields.get(key);
            return n == null ? def : n;
        }

        public String getString(String key) {
            return ((Str) get(key)).value();
        }

        public String getStringOr(String key, String def) {
            Node n = fields.get(key);
            return n == null || n instanceof Null ? def : ((Str) n).value();
        }

        public int getInt(String key) {
            return (int) ((Num) get(key)).longValue();
        }

        public long getLong(String key) {
            return ((Num) get(key)).longValue();
        }

        public boolean getBool(String key) {
            return ((Bool) get(key)).value();
        }

        public Obj getObj(String key) { return (Obj) get(key); }

        public Arr getArr(String key) { return (Arr) get(key); }

        public List<String> getStringArray(String key) {
            Arr a = getArr(key);
            List<String> out = new ArrayList<>(a.items().size());
            for (Node n : a.items()) out.add(((Str) n).value());
            return out;
        }
    }

    public record Arr(List<Node> items) implements Node {
        public Arr { items = List.copyOf(items); }
    }

    public record Str(String value) implements Node { }

    /** Number node — stores as long if integral, else double. */
    public record Num(long longValue, double doubleValue, boolean isInteger) implements Node {
        public static Num ofLong(long v) { return new Num(v, (double) v, true); }
        public static Num ofDouble(double v) { return new Num((long) v, v, false); }
    }

    public record Bool(boolean value) implements Node { }

    public record Null() implements Node {
        public static final Null INSTANCE = new Null();
    }

    // ==================== Writer ====================

    /**
     * Streaming JSON writer. Stateful; callers must balance begin/end calls.
     *
     * <p>Produces pretty-printed output with 2-space indentation and deterministic ordering
     * (fields appear in the order emitted).
     */
    public static final class Writer {
        private final StringBuilder sb = new StringBuilder(256);
        // Stack of frames; top frame tracks current container state.
        private final java.util.ArrayDeque<Frame> stack = new java.util.ArrayDeque<>();
        private boolean awaitingFieldValue = false;

        private enum Kind { OBJECT, ARRAY }

        private static final class Frame {
            final Kind kind;
            int count = 0; // number of entries emitted so far
            Frame(Kind k) { this.kind = k; }
        }

        public Writer beginObject() {
            preWrite();
            sb.append('{');
            stack.push(new Frame(Kind.OBJECT));
            return this;
        }

        public Writer endObject() {
            Frame f = stack.pop();
            if (f.kind != Kind.OBJECT) throw new IllegalStateException("endObject without matching beginObject");
            if (f.count > 0) {
                sb.append('\n');
                indent();
            }
            sb.append('}');
            return this;
        }

        public Writer beginArray() {
            preWrite();
            sb.append('[');
            stack.push(new Frame(Kind.ARRAY));
            return this;
        }

        public Writer endArray() {
            Frame f = stack.pop();
            if (f.kind != Kind.ARRAY) throw new IllegalStateException("endArray without matching beginArray");
            if (f.count > 0) {
                sb.append('\n');
                indent();
            }
            sb.append(']');
            return this;
        }

        /** Emits a field name followed by `: `; caller must then emit the value. */
        public Writer name(String n) {
            Frame f = stack.peek();
            if (f == null || f.kind != Kind.OBJECT) throw new IllegalStateException("name() only valid inside an object");
            if (f.count > 0) sb.append(',');
            sb.append('\n');
            f.count++;
            indent();
            sb.append('"').append(escape(n)).append('"').append(": ");
            awaitingFieldValue = true;
            return this;
        }

        // --- convenience field emitters (object context only) ---

        public Writer field(String n, String value) {
            name(n);
            if (value == null) writeNull(); else writeString(value);
            return this;
        }

        public Writer field(String n, long value) {
            name(n); writeLong(value); return this;
        }

        public Writer field(String n, double value) {
            name(n); writeDouble(value); return this;
        }

        public Writer field(String n, boolean value) {
            name(n); writeBool(value); return this;
        }

        public Writer fieldNull(String n) { name(n); writeNull(); return this; }

        public Writer fieldStringArray(String n, List<String> values) {
            name(n); beginArray();
            for (String v : values) writeString(v);
            endArray();
            return this;
        }

        // --- scalar value emitters ---

        public Writer writeString(String v) {
            preWrite();
            if (v == null) { sb.append("null"); }
            else { sb.append('"').append(escape(v)).append('"'); }
            return this;
        }

        public Writer writeLong(long v) { preWrite(); sb.append(v); return this; }

        public Writer writeDouble(double v) {
            preWrite();
            if (v == Math.floor(v) && !Double.isInfinite(v)) {
                sb.append((long) v);
            } else {
                sb.append(v);
            }
            return this;
        }

        public Writer writeBool(boolean v) { preWrite(); sb.append(v); return this; }

        public Writer writeNull() { preWrite(); sb.append("null"); return this; }

        @Override
        public String toString() {
            if (!stack.isEmpty()) throw new IllegalStateException("JSON writer has unclosed containers: " + stack.size());
            return sb.toString();
        }

        // --- internal ---

        private void preWrite() {
            // Entering a value slot: either the root, an object field value (just wrote ": "),
            // or an array element (need comma + newline if not first).
            if (awaitingFieldValue) {
                awaitingFieldValue = false;
                return;
            }
            Frame f = stack.peek();
            if (f == null) return; // root scalar (rare)
            if (f.kind == Kind.ARRAY) {
                if (f.count > 0) sb.append(',');
                sb.append('\n');
                f.count++;
                indent();
            }
            // OBJECT values without a name() call are an error caught by name() itself.
        }

        private void indent() {
            for (int i = 0; i < stack.size(); i++) sb.append("  ");
        }
    }

    public static Writer writer() { return new Writer(); }

    private static String escape(String s) {
        StringBuilder out = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"'  -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                default   -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.toString();
    }

    // ==================== Parser ====================

    public static Node parse(String text) {
        Parser p = new Parser(text);
        p.skipWs();
        Node n = p.parseValue();
        p.skipWs();
        if (p.pos < p.src.length()) {
            throw p.error("Unexpected trailing content");
        }
        return n;
    }

    public static Obj parseObject(String text) {
        Node n = parse(text);
        if (!(n instanceof Obj o)) throw new IllegalArgumentException("Expected top-level JSON object, got " + n.getClass().getSimpleName());
        return o;
    }

    private static final class Parser {
        final String src;
        int pos;

        Parser(String src) { this.src = src; this.pos = 0; }

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
            LinkedHashMap<String, Node> fields = new LinkedHashMap<>();
            skipWs();
            if (peek() == '}') { pos++; return new Obj(fields); }
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
                if (c == '}') return new Obj(fields);
                throw error("Expected ',' or '}' in object");
            }
        }

        Arr parseArray() {
            expect('[');
            List<Node> items = new ArrayList<>();
            skipWs();
            if (peek() == ']') { pos++; return new Arr(items); }
            while (true) {
                items.add(parseValue());
                skipWs();
                char c = next();
                if (c == ',') continue;
                if (c == ']') return new Arr(items);
                throw error("Expected ',' or ']' in array");
            }
        }

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
            int start = pos;
            if (peek() == '-') pos++;
            while (pos < src.length() && Character.isDigit(src.charAt(pos))) pos++;
            boolean isFloat = false;
            if (pos < src.length() && src.charAt(pos) == '.') {
                isFloat = true;
                pos++;
                while (pos < src.length() && Character.isDigit(src.charAt(pos))) pos++;
            }
            if (pos < src.length() && (src.charAt(pos) == 'e' || src.charAt(pos) == 'E')) {
                isFloat = true;
                pos++;
                if (pos < src.length() && (src.charAt(pos) == '+' || src.charAt(pos) == '-')) pos++;
                while (pos < src.length() && Character.isDigit(src.charAt(pos))) pos++;
            }
            String num = src.substring(start, pos);
            if (num.isEmpty() || num.equals("-")) throw error("Invalid number");
            if (isFloat) return Num.ofDouble(Double.parseDouble(num));
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

    // ==================== Helpers ====================

    /** Convenience accessor for Map-shaped Obj field. */
    public static Map<String, Node> asMap(Node n) {
        if (!(n instanceof Obj o)) throw new IllegalArgumentException("Expected object, got " + n.getClass().getSimpleName());
        return o.fields();
    }
}
