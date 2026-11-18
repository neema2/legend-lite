// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.error;

/**
 * A RUN'S PARAMETER VALUES THAT THEIR PARAMETERS DO NOT TAKE (J8 of the edge audit, 2026-11-17; beside
 * {@link MissingParameters}): each problem named with its parameter, in lite's sentence; legend-engine's
 * ({@code Invalid provided parameter(s): [...]}) is the compatibility edge's ({@code edge.PureMessages.compile}), which
 * words it where an upstream client reads the error.
 */
public final class InvalidParameters extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;
    private final java.util.List<String> problems;

    public InvalidParameters(java.util.List<String> problems) {
        super(sentence(problems));
        this.problems = java.util.List.copyOf(problems);
    }

    /** Lite's sentence: each value its parameter does not take. */
    public static String sentence(java.util.List<String> problems) {
        return "invalid value(s) for the parameter(s): " + String.join("; ", problems);
    }

    /** Each problem, as {@code parameter 'p' (Type[m]): why}. */
    public java.util.List<String> problems() {
        return problems;
    }
}
