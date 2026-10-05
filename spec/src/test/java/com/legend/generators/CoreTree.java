// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import java.nio.file.Path;

/**
 * Core's files, from this module: the generators WRITE core's generated resources (the prelude, the signature text,
 * the dynafunction registry, the implicit-import sequence) and the parity tests READ them — the generator lives
 * outside, the generated resource inside, byte-parity asserted (the upstream boundary's contract, workstream C). Each
 * file is one spec_tests declares (SourceFiles: its :core_main_sources list; Bazel workplan P3-33), never found from a
 * repository root.
 */
public final class CoreTree {

    private CoreTree() {
    }

    public static Path main(String relative) {
        return com.legend.testing.SourceFiles.file("core/src/main/java/" + relative);
    }

    public static Path resource(String relative) {
        return com.legend.testing.SourceFiles.file("core/src/main/resources/" + relative);
    }
}
