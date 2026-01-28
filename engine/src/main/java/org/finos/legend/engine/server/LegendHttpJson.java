package org.finos.legend.engine.server;

import java.util.*;

/**
 * Zero-dependency JSON parser for LSP messages.
 * 
 * Parses JSON-RPC 2.0 messages used by Language Server Protocol.
 * GraalVM native-image compatible - no reflection.
 * 
 * Supports: objects, arrays, strings, numbers, booleans, null
 */
public final class LegendHttpJson {

    private LegendHttpJson() {
    }

    // ========== PARSING ==========

    /**
     * Parse a JSON string into a Map (for objects) or List (for arrays).
     */
    public static Object parse(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        return new Parser(json.trim()).parseValue();
    }

    /**
     * Parse JSON and cast to Map.
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> parseObject(String json) {
        Object result = parse(json);
        return result instanceof Map ? (Map<String, Object>) result : null;
    }

    // ========== SERIALIZATION ==========

    /**
     * Serialize an object to JSON string.
     */
    public static String toJson(Object value) {
        StringBuilder sb = new StringBuilder();
        writeValue(sb, value);
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private static void writeValue(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof String s) {
            writeString(sb, s);
        } else if (value instanceof Number n) {
            sb.append(n);
        } else if (value instanceof Boolean b) {
            sb.append(b ? "true" : "false");
        } else if (value instanceof Map<?, ?> m) {
            writeObject(sb, (Map<String, Object>) m);
        } else if (value instanceof List<?> l) {
            writeArray(sb, l);
        } else {
            writeString(sb, value.toString());
        }
    }

    private static void writeString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    private static void writeObject(StringBuilder sb, Map<String, Object> map) {
        sb.append('{');
        boolean first = true;
        for (var entry : map.entrySet()) {
            if (!first)
                sb.append(',');
            first = false;
            writeString(sb, entry.getKey());
            sb.append(':');
            writeValue(sb, entry.getValue());
        }
        sb.append('}');
    }

    private static void writeArray(StringBuilder sb, List<?> list) {
        sb.append('[');
        boolean first = true;
        for (Object item : list) {
            if (!first)
                sb.append(',');
            first = false;
            writeValue(sb, item);
        }
        sb.append(']');
    }

    // ========== PARSER IMPLEMENTATION ==========

    private static class Parser {
        private final String json;
        private int pos = 0;

        Parser(String json) {
            this.json = json;
        }

        Object parseValue() {
            skipWhitespace();
            if (pos >= json.length())
                return null;

            char c = json.charAt(pos);
            return switch (c) {
                case '{' -> parseObject();
                case '[' -> parseArray();
                case '"' -> parseString();
                case 't', 'f' -> parseBoolean();
                case 'n' -> parseNull();
                default -> parseNumber();
            };
        }

        private Map<String, Object> parseObject() {
            Map<String, Object> map = new LinkedHashMap<>();
            pos++; // skip '{'
            skipWhitespace();

            if (pos < json.length() && json.charAt(pos) == '}') {
                pos++;
                return map;
            }

            while (pos < json.length()) {
                skipWhitespace();
                String key = parseString();
                skipWhitespace();
                expect(':');
                skipWhitespace();
                Object value = parseValue();
                map.put(key, value);
                skipWhitespace();

                if (pos >= json.length())
                    break;
                char c = json.charAt(pos);
                if (c == '}') {
                    pos++;
                    break;
                } else if (c == ',') {
                    pos++;
                }
            }
            return map;
        }

        private List<Object> parseArray() {
            List<Object> list = new ArrayList<>();
            pos++; // skip '['
            skipWhitespace();

            if (pos < json.length() && json.charAt(pos) == ']') {
                pos++;
                return list;
            }

            while (pos < json.length()) {
                skipWhitespace();
                list.add(parseValue());
                skipWhitespace();

                if (pos >= json.length())
                    break;
                char c = json.charAt(pos);
                if (c == ']') {
                    pos++;
                    break;
                } else if (c == ',') {
                    pos++;
                }
            }
            return list;
        }

        private String parseString() {
            pos++; // skip opening quote
            StringBuilder sb = new StringBuilder();
            while (pos < json.length()) {
                char c = json.charAt(pos++);
                if (c == '"') {
                    break;
                } else if (c == '\\' && pos < json.length()) {
                    char escaped = json.charAt(pos++);
                    switch (escaped) {
                        case '"' -> sb.append('"');
                        case '\\' -> sb.append('\\');
                        case '/' -> sb.append('/');
                        case 'b' -> sb.append('\b');
                        case 'f' -> sb.append('\f');
                        case 'n' -> sb.append('\n');
                        case 'r' -> sb.append('\r');
                        case 't' -> sb.append('\t');
                        case 'u' -> {
                            if (pos + 4 <= json.length()) {
                                String hex = json.substring(pos, pos + 4);
                                sb.append((char) Integer.parseInt(hex, 16));
                                pos += 4;
                            }
                        }
                        default -> sb.append(escaped);
                    }
                } else {
                    sb.append(c);
                }
            }
            return sb.toString();
        }

        private Number parseNumber() {
            int start = pos;
            if (pos < json.length() && json.charAt(pos) == '-')
                pos++;
            while (pos < json.length() && Character.isDigit(json.charAt(pos)))
                pos++;

            boolean isFloat = false;
            if (pos < json.length() && json.charAt(pos) == '.') {
                isFloat = true;
                pos++;
                while (pos < json.length() && Character.isDigit(json.charAt(pos)))
                    pos++;
            }
            if (pos < json.length() && (json.charAt(pos) == 'e' || json.charAt(pos) == 'E')) {
                isFloat = true;
                pos++;
                if (pos < json.length() && (json.charAt(pos) == '+' || json.charAt(pos) == '-'))
                    pos++;
                while (pos < json.length() && Character.isDigit(json.charAt(pos)))
                    pos++;
            }

            String num = json.substring(start, pos);
            return isFloat ? Double.parseDouble(num) : Long.parseLong(num);
        }

        private Boolean parseBoolean() {
            if (json.startsWith("true", pos)) {
                pos += 4;
                return true;
            } else if (json.startsWith("false", pos)) {
                pos += 5;
                return false;
            }
            throw new IllegalArgumentException("Invalid boolean at position " + pos);
        }

        private Object parseNull() {
            if (json.startsWith("null", pos)) {
                pos += 4;
                return null;
            }
            throw new IllegalArgumentException("Invalid null at position " + pos);
        }

        private void skipWhitespace() {
            while (pos < json.length() && Character.isWhitespace(json.charAt(pos))) {
                pos++;
            }
        }

        private void expect(char expected) {
            if (pos < json.length() && json.charAt(pos) == expected) {
                pos++;
            } else {
                throw new IllegalArgumentException("Expected '" + expected + "' at position " + pos);
            }
        }
    }

    // ========== HELPER METHODS FOR LSP ==========

    /**
     * Get a string value from a map.
     */
    public static String getString(Map<String, Object> map, String key) {
        Object value = map.get(key);
        return value instanceof String s ? s : null;
    }

    /**
     * Get an integer value from a map.
     */
    public static Integer getInt(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value instanceof Number n) {
            return n.intValue();
        }
        return null;
    }

    /**
     * Get a nested object from a map.
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> getObject(Map<String, Object> map, String key) {
        Object value = map.get(key);
        return value instanceof Map ? (Map<String, Object>) value : null;
    }

    /**
     * Get a list from a map.
     */
    @SuppressWarnings("unchecked")
    public static List<Object> getList(Map<String, Object> map, String key) {
        Object value = map.get(key);
        return value instanceof List ? (List<Object>) value : null;
    }
}
