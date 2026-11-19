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

/**
 * THE EDGE GOLDEN NET's NORMALIZER (2026-11-16): what the PCT module records in the order it receives it, put in one
 * order before the net compares it with its golden -- the PCT module itself holds no comparison or ordering machinery
 * (pct's PctDisciplineTest: comparison stays in interpreted Pure). A relation's rows, which the recorder separates by
 * U+001E, are sorted (an unordered result's order is the database's) and joined by commas; then the file's lines are
 * sorted. Arguments: {@code <in> <out>}.
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

    /** A line with a relation's rows sorted; any other line as it is. */
    static String normalized(String line) {
        String[] f = line.split("\t", -1);
        if (f.length < 4 || !f[2].equals("RELATION")) {
            return line;
        }
        int at = f[3].indexOf(ROWS);
        if (at < 0) {
            return line;
        }
        List<String> rows = new ArrayList<>(Arrays.asList(f[3].substring(at + ROWS.length()).split(ROW_SEPARATOR, -1)));
        Collections.sort(rows);
        f[3] = f[3].substring(0, at + ROWS.length()) + String.join(",", rows);
        return String.join("\t", f);
    }
}
