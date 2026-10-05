// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.extension;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;

/**
 * DEBUG ONLY: the render census's input (tools/census/README.md). With {@code -Dlegend.diagnostics=pct-cases},
 * every (model, expression) pair the PCT lane executes is appended to {@code pct-cases.tsv} in the
 * test's undeclared outputs -- each field Base64, one case a line -- so the census can lower and
 * render the same cases under every dialect, at any two commits, without the interpreter. Unset
 * (every normal run), nothing is written.
 */
final class PctCaseRecorder {

    private static final String DIR = !com.legend.diagnostics.Diagnostics.on("pct-cases") ? null
            : System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");

    private PctCaseRecorder() {
    }

    static void record(String model, String expression) {
        if (DIR == null) {
            return;
        }
        Base64.Encoder b64 = Base64.getEncoder();
        String line = b64.encodeToString(model.getBytes(StandardCharsets.UTF_8)) + "\t"
                + b64.encodeToString(expression.getBytes(StandardCharsets.UTF_8)) + "\n";
        try {
            Files.writeString(Path.of(DIR, "pct-cases.tsv"), line,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
