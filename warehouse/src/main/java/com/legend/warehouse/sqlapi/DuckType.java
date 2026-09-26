package com.legend.warehouse.sqlapi;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * A DuckDB type name, as DuckDB's JDBC metadata spells it, read into a
 * tree: {@code STRUCT(x INTEGER, y DECIMAL(2,1)[])[]} is a list of a struct
 * whose {@code y} is a list of decimals.
 *
 * <p>The API's column types ARE DuckDB's type names (the warehouse speaks
 * DuckDB SQL), passed through unchanged, so a client's metadata reads the
 * same as DuckDB's own driver. Both ends parse them here: the server to
 * encode nested values, the client to decode them into the same Java
 * objects DuckDB's driver returns.
 */
public sealed interface DuckType permits DuckType.Scalar, DuckType.ListOf, DuckType.StructOf, DuckType.MapOf {

    /** Its name exactly as DuckDB spells it (the column's type name). */
    String name();

    /** A type with no parts: INTEGER, DECIMAL(10,2), TIMESTAMP WITH TIME ZONE, JSON, ENUM. */
    record Scalar(String name) implements DuckType {
        /** The base, without parameters: DECIMAL(10,2) → DECIMAL. */
        public String base() {
            int p = name.indexOf('(');
            return (p < 0 ? name : name.substring(0, p)).strip().toUpperCase(Locale.ROOT);
        }
    }

    /** A list or a fixed-size array ({@code T[]}, {@code T[3]}). */
    record ListOf(String name, DuckType element) implements DuckType {
    }

    record Field(String name, DuckType type) {
    }

    record StructOf(String name, List<Field> fields) implements DuckType {
        public StructOf {
            fields = List.copyOf(fields);
        }
    }

    record MapOf(String name, DuckType key, DuckType value) implements DuckType {
    }

    static DuckType parse(String name) {
        Parser p = new Parser(name.strip());
        DuckType t = p.type();
        p.ws();
        if (p.i != p.s.length()) throw new IllegalArgumentException("unexpected text in type '" + name + "' at " + p.i);
        return t;
    }

    /** A small recursive-descent reader of DuckDB's type spellings. */
    final class Parser {
        private final String s;
        private int i;

        private Parser(String s) {
            this.s = s;
        }

        private void ws() {
            while (i < s.length() && s.charAt(i) == ' ') i++;
        }

        private boolean ahead(String word) {
            return s.regionMatches(true, i, word, 0, word.length());
        }

        DuckType type() {
            ws();
            int start = i;
            DuckType t;
            if (ahead("STRUCT(")) {
                i += "STRUCT(".length();
                List<Field> fields = new ArrayList<>();
                ws();
                while (i < s.length() && s.charAt(i) != ')') {
                    String fname = fieldName();
                    DuckType ft = type();
                    fields.add(new Field(fname, ft));
                    ws();
                    if (i < s.length() && s.charAt(i) == ',') i++;
                    ws();
                }
                expect(')');
                t = new StructOf(s.substring(start, i), fields);
            } else if (ahead("MAP(")) {
                i += "MAP(".length();
                DuckType k = type();
                ws();
                expect(',');
                DuckType v = type();
                ws();
                expect(')');
                t = new MapOf(s.substring(start, i), k, v);
            } else {
                t = scalar(start);
            }
            // Any number of list suffixes: [] or [N].
            ws();
            while (i < s.length() && s.charAt(i) == '[') {
                int close = s.indexOf(']', i);
                if (close < 0) throw new IllegalArgumentException("unclosed [ in type '" + s + "'");
                i = close + 1;
                t = new ListOf(s.substring(start, i), t);
                ws();
            }
            return t;
        }

        /** A scalar: words and a parenthesised argument list, up to , ) or [. */
        private DuckType scalar(int start) {
            int depth = 0;
            while (i < s.length()) {
                char c = s.charAt(i);
                if (c == '(') depth++;
                else if (c == ')') {
                    if (depth == 0) break;
                    depth--;
                } else if (c == '\'') {
                    int close = s.indexOf('\'', i + 1);
                    i = close < 0 ? s.length() : close;
                } else if (depth == 0 && (c == ',' || c == '[')) break;
                i++;
            }
            String name = s.substring(start, i).strip();
            if (name.isEmpty()) throw new IllegalArgumentException("empty type in '" + s + "'");
            return new Scalar(name);
        }

        private String fieldName() {
            ws();
            if (i < s.length() && s.charAt(i) == '"') {
                StringBuilder b = new StringBuilder();
                i++;
                while (i < s.length()) {
                    char c = s.charAt(i++);
                    if (c == '"') {
                        if (i < s.length() && s.charAt(i) == '"') {
                            b.append('"');
                            i++;
                        } else {
                            break;
                        }
                    } else {
                        b.append(c);
                    }
                }
                return b.toString();
            }
            int start = i;
            while (i < s.length() && s.charAt(i) != ' ') i++;
            return s.substring(start, i);
        }

        private void expect(char c) {
            if (i >= s.length() || s.charAt(i) != c) {
                throw new IllegalArgumentException("expected '" + c + "' in type '" + s + "' at " + i);
            }
            i++;
        }
    }
}
