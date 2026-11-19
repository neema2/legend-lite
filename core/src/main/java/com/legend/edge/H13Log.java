// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

/**
 * H13 SPIKE INSTRUMENTATION, scratch (never proposed for merge): where {@code -Dlegend.h13.raised=<file>} names a file,
 * each raise the legend-pure formatter words after a run is appended to it -- its kind, its operands, the sentence, and
 * how many printed operands were withheld (computed, A3) or printed by Java where the SQL sentence had the database
 * print them. Off, and free, without the property.
 */
final class H13Log {

    private static final String FILE = System.getProperty("legend.h13.raised");

    private H13Log() {
    }

    static void raised(com.legend.executionplan.ExecutionPlan.Site site, PureMessages.Worded worded) {
        if (FILE == null) {
            return;
        }
        String line = site.kind() + "\t" + worded.withheld() + "\t" + worded.javaPrinted() + "\t" + site.operands()
                + "\t" + worded.sentence().replace("\n", "\\n") + "\n";
        synchronized (H13Log.class) {
            try {
                java.nio.file.Files.writeString(java.nio.file.Path.of(FILE), line,
                        java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }
    }
}
