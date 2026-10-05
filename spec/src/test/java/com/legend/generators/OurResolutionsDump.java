// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import com.legend.testing.Programs;

import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Our side of the reference differential as a file ({@link OurResolutions}), for one module. The gated form of the
 * differential is {@code //spec:reference_lane} ({@link ReferenceLaneTest}); this dump stays for ad-hoc joins
 * ({@code tools/reference/join.py}). A program, not a test (Bazel workplan P3-17):
 * {@code bazel run //spec:our_resolutions -- <module> [--out FILE]}, by default {@code our-resolutions-<module>.txt}
 * where it is run.
 */
public final class OurResolutionsDump {

    private OurResolutionsDump() {}

    public static void main(String[] args) throws Exception {
        String[] rest = Programs.withoutOut(args);
        if (rest.length != 1) {
            throw new IllegalArgumentException("our_resolutions <module> [--out FILE]");
        }
        String target = rest[0];
        String out = Programs.option(args, "--out");
        Path file = Programs.argument(out != null ? out : "our-resolutions-" + target + ".txt");
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(file, StandardCharsets.UTF_8))) {
            OurResolutions.Result r = OurResolutions.dump(target, w);
            System.out.println("[our-resolutions] " + target + ": functions=" + r.functions()
                    + " failed=" + r.failedFunctions().size() + " dropped=" + r.droppedSources().size()
                    + " rows=" + r.rows() + " -> " + file);
        }
    }
}
