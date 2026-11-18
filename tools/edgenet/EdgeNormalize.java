// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.edgenet;

import com.legend.json.Json;

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
 * database's; a row-order change across distinct keys is a difference the net sees. A line with no order, or one whose
 * key names no column ({@code "?"}: a sort on an expression the query does not project), has its rows sorted, as
 * before S0.
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

    /** The column indexes the line's {@code order=[...]} names; none when it names none, or a key no column is. */
    static Optional<int[]> keys(String head) {
        int order = head.indexOf(ORDER);
        if (!head.startsWith(COLUMNS) || order < 0) {
            return Optional.empty();
        }
        Json.Node columns = Json.parse(head.substring(COLUMNS.length(), order));
        Json.Node names = Json.parse(head.substring(order + ORDER.length(), head.length() - ROWS.length()));
        if (!(columns instanceof Json.Arr cols) || !(names instanceof Json.Arr keys)) {
            return Optional.empty();
        }
        int[] out = new int[keys.items().size()];
        for (int i = 0; i < out.length; i++) {
            int index = keys.items().get(i) instanceof Json.Str s ? column(cols, s.value()) : -1;
            if (index < 0) {
                return Optional.empty();
            }
            out[i] = index;
        }
        return Optional.of(out);
    }

    /** The index of the column named {@code name}, or -1 when no column is. */
    private static int column(Json.Arr columns, String name) {
        for (int i = 0; i < columns.items().size(); i++) {
            if (columns.items().get(i) instanceof Json.Obj o && o.get("name") instanceof Json.Str s
                    && s.value().equals(name)) {
                return i;
            }
        }
        return -1;
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

    /** A row's values at the key columns, as their compact JSON joined by U+0000 ({@code {"values":[...]}}). The
     *  recorder wrote the row with each backslash doubled (its one escape a compact JSON row can need: the row holds
     *  no raw line end, tab or control character), undone here to read it. */
    private static String key(String row, int[] keys) {
        if (row.isEmpty()) {
            return "";
        }
        Json.Node values = ((Json.Obj) Json.parse(row.replace("\\\\", "\\"))).get("values");
        List<Json.Node> cells = ((Json.Arr) values).items();
        StringBuilder key = new StringBuilder();
        for (int k : keys) {
            key.append(Json.toCompact(cells.get(k))).append('\u0000');
        }
        return key.toString();
    }
}
