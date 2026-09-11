// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools;

import java.nio.file.Path;

/**
 * Where core's tree is, from this module: the generators WRITE core's generated
 * resources (the prelude, the signature text, the dynafunction registry, the
 * implicit-import sequence) and the parity tests READ them — the generator
 * lives outside, the generated resource inside, byte-parity asserted (the
 * upstream boundary's contract, workstream C). One root, every path through it.
 */
public final class CoreTree {

    private CoreTree() {
    }

    /** The core module's directory ({@code ../core} from this module's). */
    public static final Path CORE = Path.of("..", "core");

    public static Path main(String relative) {
        return CORE.resolve("src/main/java").resolve(relative);
    }

    public static Path resource(String relative) {
        return CORE.resolve("src/main/resources").resolve(relative);
    }
}
