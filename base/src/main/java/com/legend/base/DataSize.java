// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.base;

/**
 * The size a load admits: a file of more megabytes than its load's limit is refused, never cut (legend-pure's
 * {@code CsvReader.readCsv} admits the same). The refusal is a kind with its facts -- the file, its size and the limit
 * ({@link TooLarge}); its words are its reader's: the product's error kind, legend-pure's sentence the compatibility
 * edge's (J25 of the edge audit, 2026-11-17). Its own class (the A checkpoint's tenets audit, 4b, 2026-11-10): the run
 * checks a file's size before its database reads it, and reaches no parser of its cells (there is none since B2b: a
 * database reads a file itself), which {@code ArchitectureTest.execParsesNoData} holds.
 */
public final class DataSize {

    private DataSize() {
    }

    /** A file larger than a load admits: refused, never cut. Its facts, no words of upstream's: {@code path} the file as
     *  the load names it, {@code bytes} its size, {@code characters} its UTF-8 text's length (legend-pure measures the
     *  text it prints by), {@code limitMegabytes} the load's limit. */
    public static final class TooLarge extends IllegalArgumentException {
        private static final long serialVersionUID = 1L;
        private final String path;
        private final long bytes;
        private final long characters;
        private final int limitMegabytes;

        TooLarge(String path, long bytes, long characters, int limitMegabytes) {
            super("the file '" + path + "' is larger than the load admits: " + bytes + " bytes, the limit "
                    + limitMegabytes + " MB");
            this.path = path;
            this.bytes = bytes;
            this.characters = characters;
            this.limitMegabytes = limitMegabytes;
        }

        public String path() {
            return path;
        }

        public long bytes() {
            return bytes;
        }

        public long characters() {
            return characters;
        }

        public int limitMegabytes() {
            return limitMegabytes;
        }
    }

    /** The refusal its facts state: a test plan's planned failure rebuilt at its verdict's turn (stage B's B1,
     *  2026-11-20), so its words are worded from the same facts as a live one's. */
    public static TooLarge refusal(String path, long bytes, long characters, int limitMegabytes) {
        return new TooLarge(path, bytes, characters, limitMegabytes);
    }

    /** {@code bytes} refused ({@link TooLarge}) where they are more than {@code limitMegabytes} megabytes (legend-pure's
     *  {@code CsvReader.readCsv}, read 2026-11-09 at 5.99.0: the UTF-8 length against the limit). */
    public static void requireSize(String path, byte[] bytes, int limitMegabytes) {
        long megabyte = 1024 * 1024L;
        if (bytes.length > limitMegabytes * megabyte) {
            throw new TooLarge(path, bytes.length, new String(bytes, java.nio.charset.StandardCharsets.UTF_8).length(),
                    limitMegabytes);
        }
    }
}
