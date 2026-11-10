// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.base;

/**
 * The size a load admits: a file of more megabytes than its load's limit is refused, never cut, in upstream's words
 * (legend-pure's {@code CsvReader.readCsv}). Its own class (the A checkpoint's tenets audit, 4b, 2026-11-10): the run
 * checks a file's size before its database reads it, and reaches no parser of its cells ({@link CsvCells}, the edge's
 * and the planner's), which {@code ArchitectureTest.execParsesNoData} holds.
 */
public final class DataSize {

    private DataSize() {
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
}
