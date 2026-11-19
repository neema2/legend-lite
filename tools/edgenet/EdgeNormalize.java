// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.edgenet;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * THE EDGE GOLDEN NET's NORMALIZER (2026-11-16): what the PCT module records in the order it receives it, put in one
 * order before the net compares it with its golden -- the PCT module itself holds no comparison or ordering machinery
 * (pct's PctDisciplineTest: comparison stays in interpreted Pure). A relation's rows, which the recorder separates by
 * U+001E, are joined by commas in one order; then the file's lines are sorted. Arguments: {@code <in> <out>}.
 *
 * <p>THE ORDER (S0 of the presentation program, 2026-11-18, docs/records/presentation-program-design-2026-11-18.md
 * §4 (d)): a line whose plan sorts at its root carries {@code order=[...]}, the output columns its ORDER BY names. Its
 * rows keep their order across distinct values of those columns and are sorted only within a tie, whose order is the
 * database's; a row-order change across distinct keys is a difference the net sees. A key that names no column
 * ({@code "?"}: a sort on an expression the query does not project) cannot be followed, so the net follows the longest
 * prefix of the keys that names columns (the review of PR #123, 2026-11-19); a line with no order, or whose first key
 * names no column, has its rows sorted, as before S0.
 *
 * <p>It reads the recorder's compact JSON itself ({@link #elements}, {@link #text}), with no JSON library: a column's
 * name and a row's cells are the only things it reads, and the product's JSON library stays the product's (the review
 * of PR #123: //json's visibility is not widened for a test tool).
 */
public final class EdgeNormalize {

    private static final String ROW_SEPARATOR = "\u001e";
    private static final String ROWS = " rows=";

    private EdgeNormalize() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            throw new IllegalArgumentException("EdgeNormalize <in> <out>");
        }
        List<String> out = new ArrayList<>();
        for (String line : Files.readAllLines(Path.of(args[0]), StandardCharsets.UTF_8)) {
            out.add(normalized(line));
        }
        Collections.sort(out);
        StringBuilder text = new StringBuilder();
        for (String line : out) {
            text.append(line).append('\n');
        }
        Files.writeString(Path.of(args[1]), text, StandardCharsets.UTF_8);
    }

    private static final String ORDER = " order=";

    private static final String COLUMNS = "columns=";

    /** A relation's line with its rows in the net's one order (the class's rule); any other line as it is. */
    public static String normalized(String line) {
        String[] f = line.split("\t", -1);
        if (f.length < 4 || !f[2].equals("RELATION")) {
            return line;
        }
        int at = f[3].indexOf(ROWS);
        if (at < 0) {
            return line;
        }
        String head = f[3].substring(0, at + ROWS.length());
        List<String> rows = new ArrayList<>(Arrays.asList(f[3].substring(at + ROWS.length()).split(ROW_SEPARATOR, -1)));
        Optional<int[]> keys = keys(head);
        if (keys.isPresent()) {
            rows = sortedWithinTies(rows, keys.get());
        } else {
            Collections.sort(rows);
        }
        f[3] = head + String.join(",", rows);
        return String.join("\t", f);
    }

    /** The column indexes of the longest prefix of the line's {@code order=[...]} keys that names columns; none when it
     *  names none, or its first key no column is. */
    static Optional<int[]> keys(String head) {
        int order = head.indexOf(ORDER);
        if (!head.startsWith(COLUMNS) || order < 0) {
            return Optional.empty();
        }
        Optional<List<String>> columns = elements(head.substring(COLUMNS.length(), order));
        Optional<List<String>> names = elements(head.substring(order + ORDER.length(), head.length() - ROWS.length()));
        if (columns.isEmpty() || names.isEmpty()) {
            return Optional.empty();
        }
        List<Integer> out = new ArrayList<>();
        for (String name : names.get()) {
            int index = name.startsWith("\"") ? column(columns.get(), text(name)) : -1;
            if (index < 0) {
                break;   // the keys after one the net cannot follow are not followed either
            }
            out.add(index);
        }
        return out.isEmpty() ? Optional.empty() : Optional.of(out.stream().mapToInt(Integer::intValue).toArray());
    }

    /** The index of the column whose object's {@code "name"} is {@code name}, or -1 when no column's is. */
    private static int column(List<String> columns, String name) {
        for (int i = 0; i < columns.size(); i++) {
            if (field(columns.get(i), "name").filter(name::equals).isPresent()) {
                return i;
            }
        }
        return -1;
    }

    /** The text of {@code object}'s string field {@code key} (a compact JSON object's top-level member), if it has one. */
    private static Optional<String> field(String object, String key) {
        if (!object.startsWith("{") || !object.endsWith("}")) {
            return Optional.empty();
        }
        for (String member : split(object.substring(1, object.length() - 1))) {
            List<String> kv = splitAt(member, ':');
            if (kv.size() == 2 && kv.get(0).startsWith("\"") && text(kv.get(0)).equals(key)
                    && kv.get(1).startsWith("\"")) {
                return Optional.of(text(kv.get(1)));
            }
        }
        return Optional.empty();
    }

    /** A compact JSON array's top-level elements, each as its own text; none when {@code array} is no array. */
    public static Optional<List<String>> elements(String array) {
        if (!array.startsWith("[") || !array.endsWith("]")) {
            return Optional.empty();
        }
        String inner = array.substring(1, array.length() - 1);
        return Optional.of(inner.isEmpty() ? List.of() : split(inner));
    }

    /** {@code s} split at its top-level commas: outside strings, brackets and braces. */
    private static List<String> split(String s) {
        return splitAt(s, ',');
    }

    private static List<String> splitAt(String s, char at) {
        List<String> out = new ArrayList<>();
        int depth = 0;
        boolean inString = false;
        int start = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (inString) {
                if (c == '\\') {
                    i++;   // an escape: the next character is the string's
                } else if (c == '"') {
                    inString = false;
                }
            } else if (c == '"') {
                inString = true;
            } else if (c == '[' || c == '{') {
                depth++;
            } else if (c == ']' || c == '}') {
                depth--;
            } else if (c == at && depth == 0) {
                out.add(s.substring(start, i));
                start = i + 1;
                if (at == ':') {
                    out.add(s.substring(start));
                    return out;   // a member splits at its first top-level colon only
                }
            }
        }
        out.add(s.substring(start));
        return out;
    }

    /** A JSON string literal's text, its escapes read ({@code \"}, {@code \\}, {@code \/}, {@code \b}, {@code \f},
     *  {@code \n}, {@code \r}, {@code \t}, {@code \\u} and four hexadecimal digits). */
    public static String text(String literal) {
        StringBuilder out = new StringBuilder();
        for (int i = 1; i < literal.length() - 1; i++) {
            char c = literal.charAt(i);
            if (c != '\\') {
                out.append(c);
                continue;
            }
            char e = literal.charAt(++i);
            switch (e) {
                case 'b' -> out.append('\b');
                case 'f' -> out.append('\f');
                case 'n' -> out.append('\n');
                case 'r' -> out.append('\r');
                case 't' -> out.append('\t');
                case 'u' -> {
                    out.append((char) Integer.parseInt(literal.substring(i + 1, i + 5), 16));
                    i += 4;
                }
                default -> out.append(e);
            }
        }
        return out.toString();
    }

    /** The rows in the order received across distinct key values, each run of equal keys (a tie) sorted. */
    static List<String> sortedWithinTies(List<String> rows, int[] keys) {
        List<String> out = new ArrayList<>(rows.size());
        List<String> tie = new ArrayList<>();
        String tieKey = "";
        for (String row : rows) {
            String key = key(row, keys);
            if (!tie.isEmpty() && !tieKey.equals(key)) {
                Collections.sort(tie);
                out.addAll(tie);
                tie.clear();
            }
            tie.add(row);
            tieKey = key;
        }
        Collections.sort(tie);
        out.addAll(tie);
        return out;
    }

    /** A row's values at the key columns, as their compact JSON text joined by U+0000 (the row is
     *  {@code {"values":[...]}}, compact as the recorder wrote it, so two equal cells are equal texts). The recorder wrote
     *  the row with each backslash doubled (its one escape a compact JSON row can need: the row holds no raw line end,
     *  tab or control character), undone here to read it. */
    private static String key(String row, int[] keys) {
        if (row.isEmpty()) {
            return "";
        }
        String json = row.replace("\\\\", "\\");
        String values = field0(json);
        List<String> cells = elements(values).orElseThrow(() -> new IllegalStateException("a row's values are no array: "
                + row));
        StringBuilder key = new StringBuilder();
        for (int k : keys) {
            key.append(cells.get(k)).append('\u0000');
        }
        return key.toString();
    }

    /** The text of a row object's {@code "values"} member (its one member). */
    private static String field0(String row) {
        for (String member : split(row.substring(1, row.length() - 1))) {
            List<String> kv = splitAt(member, ':');
            if (kv.size() == 2 && text(kv.get(0)).equals("values")) {
                return kv.get(1);
            }
        }
        throw new IllegalStateException("a row with no values: " + row);
    }
}
