// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.error;

/**
 * AN ERROR'S CODE, IN THE ONE SPACE EVERY ERROR LITE RAISES IS NAMED IN (stage B's BE, the one error model,
 * 2026-11-21; docs/records/compat-error-model-2026-11-21.md): what went wrong, apart from the values it is about and
 * from any audience's words. A code carries lite's template, its own catalog entry; another audience's catalog words
 * the same code its way (the compatibility edge's {@code edge.PureMessages}). Its name in JSON is its family and its
 * name ({@link #qualified}): two families may share a name.
 */
public sealed interface ErrorCode permits CompileKind, ErrorKind, FrameKind, AssertKind {

    /** The code's name within its family. */
    String name();

    /** The family the code belongs to: {@code compile}, {@code run}, {@code frame}, {@code assert}. */
    String family();

    /** Lite's sentence of this code over {@code values}. */
    String sentence(java.util.List<String> values);

    /** The code's name in JSON: its family, a dot, its name. */
    default String qualified() {
        return family() + "." + name();
    }

    /** The code {@code qualified} names ({@link #qualified}); refused by name where none does. */
    static ErrorCode named(String qualified) {
        for (ErrorCode[] family : new ErrorCode[][] {CompileKind.values(), ErrorKind.values(), FrameKind.values(),
                AssertKind.values()}) {
            for (ErrorCode c : family) {
                if (c.qualified().equals(qualified)) {
                    return c;
                }
            }
        }
        throw new IllegalArgumentException("no error code '" + qualified + "' (a code is its family and its name)");
    }
}
