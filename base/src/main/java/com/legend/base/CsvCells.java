// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.base;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A CSV text SPLIT INTO CELLS, every cell text or null (docs/DATA_IN_THE_PLAN_2026_11_09.md 2.5 and decision 6: a
 * streamed file's rows, which the database casts): carriage, never a value computed. The rule is the declared CSV
 * format's DEFAULT, which is upstream's (2026-11-09, L2; read from the pinned sources): legend-pure's
 * {@code loadCsvToDbTable} reads a file with Commons CSV 1.5's {@code CSVFormat.EXCEL} ({@code CsvReader.readCsv}) and
 * reads an empty cell as NULL ({@code LoadToDbTableHelper}). So, with {@code delimiter} and {@code quote} the format's:
 * <ul>
 *   <li>a record ends at {@code \n}, {@code \r\n} or {@code \r}; the text's last line break ends the last record and
 *       opens none;</li>
 *   <li>a cell that STARTS with the quote is quoted: its text up to the closing quote, a doubled quote one quote,
 *       line breaks kept; after the closing quote only whitespace may come before the delimiter or the record's end,
 *       and anything else is a malformed text, refused; a text that ends inside a quoted cell is refused;</li>
 *   <li>any other cell is its text up to the delimiter or the record's end, exactly: no whitespace dropped, a quote
 *       inside it a character;</li>
 *   <li>an empty line is a record of one empty cell (EXCEL keeps empty lines);</li>
 *   <li>an empty cell, quoted or not, is NULL; every other cell its text.</li>
 * </ul>
 * A row's width is the caller's to check ({@link #body}): a row whose width is not the table's is refused by name,
 * never padded or cut (AGENTS.md invariant 4; decision 6: upstream binds too few values or fails on the extra one).
 */
public final class CsvCells {

    private CsvCells() {
    }

    /** A text that is not a CSV text by the format's rule (a malformed quoted cell): refused, never repaired. */
    public static final class Malformed extends IllegalArgumentException {
        private static final long serialVersionUID = 1L;

        Malformed(String message) {
            super(message);
        }
    }

    /** A file larger than a load admits: refused, never cut (upstream's own refusal, its words). */
    public static final class TooLarge extends IllegalArgumentException {
        private static final long serialVersionUID = 1L;

        TooLarge(String message) {
            super(message);
        }
    }

    /** {@code bytes} refused ({@link TooLarge}) where they are more than {@code limitMegabytes} megabytes, in
     *  upstream's words for {@code path} (legend-pure's {@code CsvReader.readCsv}, read 2026-11-09 at 5.99.0: the UTF-8
     *  length against the limit, the text's length in the message, wrapped by its "Unable to read the CSV file"). */
    public static void requireSize(String path, byte[] bytes, int limitMegabytes) {
        long megabyte = 1024 * 1024L;
        if (bytes.length > limitMegabytes * megabyte) {
            int length = new String(bytes, java.nio.charset.StandardCharsets.UTF_8).length();
            // upstream's outer exception's info: its "Unable to read the CSV file" wrapping the inner exception's
            // message, which carries no source information ("Execution error at ??, " and the info quoted:
            // legend-pure's PureException.printMessage; the M1 correctness audit's C4, 2026-11-09)
            throw new TooLarge("Unable to read the CSV file '" + path + "' Execution error at ??, \"File is too large,"
                    + " file was " + String.format(java.util.Locale.ROOT, "%.2f", length * 1.0 / megabyte)
                    + " Mb, limit is " + limitMegabytes + " Mb\"");
        }
    }

    /** {@code text}'s records by the default format's rule (comma, double quote): its header first, each record its
     *  cells in order. */
    public static List<List<String>> rows(String text) {
        return rows(text, ',', '"');
    }

    /** {@code text}'s records by the format's rule ({@code delimiter}, {@code quote}): its header first, each record
     *  its cells in order. */
    public static List<List<String>> rows(String text, char delimiter, char quote) {
        Reader in = new Reader(text, delimiter, quote);
        List<List<String>> rows = new ArrayList<>();
        while (!in.atEnd()) {
            List<String> row = new ArrayList<>();
            while (true) {
                Cell cell = in.cell();
                row.add(cell.text().isEmpty() ? null : cell.text());
                if (cell.endsRecord()) {
                    break;
                }
            }
            rows.add(Collections.unmodifiableList(row));
        }
        return Collections.unmodifiableList(rows);
    }

    /** {@code rows}' data rows, the first record (the header) dropped unread, as upstream drops it
     *  ({@code LazyIterate.drop(..., 1)}): refused by name unless every data row is exactly {@code width} cells wide,
     *  the table's (a short row is never padded, a long one never cut, an empty line in a wider table never skipped). */
    public static List<List<String>> body(List<List<String>> rows, int width) {
        List<List<String>> out = new ArrayList<>(Math.max(0, rows.size() - 1));
        for (int r = 1; r < rows.size(); r++) {
            List<String> row = rows.get(r);
            if (row.size() != width) {
                throw new Malformed("the CSV text's record " + (r + 1) + " has " + row.size() + " cell(s), and its"
                        + " table takes " + width + (row.size() == 1 && row.get(0) == null ? " (an empty line)" : ""));
            }
            out.add(row);
        }
        return Collections.unmodifiableList(out);
    }

    /** {@code text}'s data rows by the format's rule, each exactly {@code width} cells, the header dropped unread as
     *  upstream drops it (none for a text of no record or a header alone): refused by name ({@link Malformed}) where the
     *  text is malformed or a data record is not {@code width} wide, the table's. */
    public static List<List<String>> table(String text, char delimiter, char quote, int width) {
        return body(rows(text, delimiter, quote), width);
    }

    /** One cell read: its text (empty for an empty cell), and whether its record ends after it. */
    private record Cell(String text, boolean endsRecord) {
    }

    private static final class Reader {
        private final String text;
        private final char delimiter;
        private final char quote;
        private int at;
        private int line = 1;

        Reader(String text, char delimiter, char quote) {
            this.text = text;
            this.delimiter = delimiter;
            this.quote = quote;
        }

        boolean atEnd() {
            return at >= text.length();
        }

        /** The next char, or -1 at the end. */
        private int next() {
            return at < text.length() ? text.charAt(at++) : -1;
        }

        /** Whether {@code ch} ends a record, a {@code \r\n} read as one. */
        private boolean endOfLine(int ch) {
            if (ch == '\r' && at < text.length() && text.charAt(at) == '\n') {
                at++;
            }
            if (ch == '\n' || ch == '\r') {
                line++;
                return true;
            }
            return false;
        }

        Cell cell() {
            int ch = next();
            if (ch == quote) {
                return quoted();
            }
            StringBuilder cell = new StringBuilder();
            while (true) {
                if (ch < 0 || endOfLine(ch)) {
                    return new Cell(cell.toString(), true);
                }
                if (ch == delimiter) {
                    return new Cell(cell.toString(), false);
                }
                cell.append((char) ch);
                ch = next();
            }
        }

        /** A quoted cell, its opening quote read. */
        private Cell quoted() {
            int started = line;
            StringBuilder cell = new StringBuilder();
            while (true) {
                int ch = next();
                if (ch < 0) {
                    throw new Malformed("the CSV text ends inside the quoted cell that starts on line " + started);
                }
                if (ch == quote) {
                    if (at < text.length() && text.charAt(at) == quote) {
                        at++;
                        cell.append(quote);
                        continue;
                    }
                    // the closing quote: whitespace, then the delimiter or the record's end
                    while (true) {
                        ch = next();
                        if (ch == delimiter) {
                            return new Cell(cell.toString(), false);
                        }
                        if (ch < 0 || endOfLine(ch)) {
                            return new Cell(cell.toString(), true);
                        }
                        if (!Character.isWhitespace((char) ch)) {
                            throw new Malformed("the CSV text's line " + line + " has '" + (char) ch + "' between a"
                                    + " quoted cell's closing quote and the next delimiter");
                        }
                    }
                }
                if (ch == '\n' || (ch == '\r' && !(at < text.length() && text.charAt(at) == '\n'))) {
                    line++;
                }
                cell.append((char) ch);
            }
        }
    }
}
