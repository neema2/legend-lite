// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.base;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A CSV text SPLIT INTO CELLS, every cell text or null (docs/DATA_IN_THE_PLAN_2026_11_09.md 2.5: a streamed file's
 * rows, which the database casts): carriage, never a value computed. The rule is H2's {@code CSVREAD}'s, which a CSV
 * file's load on H2 ran until 2026-11-09 (legend-engine loads one so), kept so a load's rows are what they were:
 * comma-separated, {@code "} quoting with {@code ""} inside a quoted cell; an unquoted cell's surrounding whitespace
 * dropped and an empty one null; a quoted cell exactly its text; spaces and tabs between a closing quote and the next
 * separator dropped; a line ends at {@code \n}, {@code \r} or the end; an empty line is no row.
 */
public final class CsvCells {

    private CsvCells() {
    }

    /** {@code text}'s rows, its header first where it has one, each row its cells in order. */
    public static List<List<String>> rows(String text) {
        Reader in = new Reader(text);
        List<List<String>> rows = new ArrayList<>();
        while (!in.atEnd()) {
            List<String> row = new ArrayList<>();
            boolean first = true;
            while (true) {
                Cell cell = in.cell();
                if (cell.endsLine() && first && cell.text() == null && !cell.quoted()) {
                    // a line of nothing: no row
                    break;
                }
                row.add(cell.text());
                first = false;
                if (cell.endsLine()) {
                    break;
                }
            }
            if (!row.isEmpty()) {
                rows.add(Collections.unmodifiableList(row));
            }
        }
        return Collections.unmodifiableList(rows);
    }

    /** {@code rows}' data rows, the header dropped, each as wide as {@code width}: a short row's missing cells null, a
     *  long row's extra cells dropped (CSVREAD's rule). */
    public static List<List<String>> body(List<List<String>> rows, int width) {
        List<List<String>> out = new ArrayList<>(Math.max(0, rows.size() - 1));
        for (List<String> row : rows.subList(Math.min(1, rows.size()), rows.size())) {
            List<String> cells = new ArrayList<>(width);
            for (int c = 0; c < width; c++) {
                cells.add(c < row.size() ? row.get(c) : null);
            }
            out.add(Collections.unmodifiableList(cells));
        }
        return Collections.unmodifiableList(out);
    }

    /** One cell read: its text (null for an empty unquoted cell), whether it was quoted, whether its line ends. */
    private record Cell(@Nullable String text, boolean quoted, boolean endsLine) {
    }

    private static final class Reader {
        private final String text;
        private int at;

        Reader(String text) {
            this.text = text;
        }

        boolean atEnd() {
            return at >= text.length();
        }

        /** The next char, or -1 at the end. */
        private int next() {
            return at < text.length() ? text.charAt(at++) : -1;
        }

        private static boolean endOfLine(int ch) {
            return ch == '\n' || ch == '\r' || ch < 0;
        }

        Cell cell() {
            while (true) {
                int ch = next();
                if (ch == '"') {
                    return quoted();
                }
                if (endOfLine(ch)) {
                    return new Cell(null, false, true);
                }
                if (ch == ',') {
                    return new Cell(null, false, false);
                }
                if (ch <= ' ') {
                    // whitespace before a cell
                    continue;
                }
                int start = at - 1;
                boolean ends;
                while (true) {
                    ch = next();
                    if (ch == ',') {
                        ends = false;
                        break;
                    }
                    if (endOfLine(ch)) {
                        ends = true;
                        break;
                    }
                }
                int end = ch < 0 ? at : at - 1;
                String cell = text.substring(start, end).trim();
                return new Cell(cell.isEmpty() ? null : cell, false, ends);
            }
        }

        /** A quoted cell, its opening quote read. */
        private Cell quoted() {
            StringBuilder cell = new StringBuilder();
            int ch;
            while (true) {
                ch = next();
                if (ch < 0) {
                    break;
                }
                if (ch == '"') {
                    ch = next();
                    if (ch != '"') {
                        break;
                    }
                }
                cell.append((char) ch);
            }
            // after the closing quote: spaces and tabs, then the separator or the line's end; any other char starts the
            // next cell (CSVREAD's rule)
            while (true) {
                if (ch == ',') {
                    return new Cell(cell.toString(), true, false);
                }
                if (endOfLine(ch)) {
                    return new Cell(cell.toString(), true, true);
                }
                if (ch != ' ' && ch != '\t') {
                    at--;
                    return new Cell(cell.toString(), true, false);
                }
                ch = next();
            }
        }
    }
}
