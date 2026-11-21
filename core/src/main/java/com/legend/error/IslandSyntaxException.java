// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.error;

/**
 * A syntax error inside an embedded-language island ({@code #SQL{...}#}). The island's text is read when the island
 * is typed, so the error surfaces then; its phase is {@link Phase#PARSE} all the same, because the text does not
 * parse. The message names the position in the enclosing source.
 */
public final class IslandSyntaxException extends LegendCompileException {

    /** The error {@code record} states (stage B's BE, 2026-11-21). */
    public IslandSyntaxException(ErrorRecord record) {
        super(Phase.PARSE, record, null);
    }

    public IslandSyntaxException(String message) {
        super(Phase.PARSE, message);
    }
}
