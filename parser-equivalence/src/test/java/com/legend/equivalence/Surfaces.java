// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import com.legend.model.ParsedModel;

/**
 * THE THREE SURFACES, each named ONCE for the whole parity harness —
 * this module's job is comparing the levels against the referee, and
 * these are the levels it compares.
 */
public final class Surfaces {

    private Surfaces() {
    }

    public static ParsedModel platform(String source) {
        return com.legend.door.ModelFromText.parse(source,
                com.legend.parser.GrammarLevel.LEGEND_PLATFORM);
    }

    public static ParsedModel platform(com.legend.lexer.TokenStream tokens) {
        return com.legend.door.ModelFromText.parse(tokens.source(),
                com.legend.parser.GrammarLevel.LEGEND_PLATFORM);
    }

    public static ParsedModel lite(String source) {
        return com.legend.door.ModelFromText.parse(source,
                com.legend.parser.GrammarLevel.LEGEND_LITE);
    }

    public static com.legend.protocol.Protocol.PMapping platformMapping(
            com.legend.lexer.TokenStream tokens, int tokenIndex,
            int sectionStartLine) {
        return com.legend.parser.MappingProtocolParser.parse(tokens,
                tokenIndex, sectionStartLine,
                com.legend.parser.GrammarLevel.LEGEND_PLATFORM);
    }

    /** A parser positioned at {@code tokenIndex}, engine level. */
    public static com.legend.parser.ElementParser engineAt(
            com.legend.lexer.TokenStream tokens, int tokenIndex) {
        return com.legend.parser.ElementParser.at(tokens, tokenIndex,
                com.legend.parser.GrammarLevel.LEGEND_ENGINE);
    }

    /** One {@code Database} element at {@code tokenIndex}, engine level
     *  (byte parity's subject is the drop-in surface). */
    public static com.legend.protocol.Protocol.PDatabase databaseAt(
            com.legend.lexer.TokenStream tokens, int tokenIndex) {
        return com.legend.parser.DatabaseProtocolParser.parse(tokens,
                tokenIndex, com.legend.parser.GrammarLevel.LEGEND_ENGINE);
    }

    public static ParsedModel engine(String source) {
        return com.legend.door.ModelFromText.parse(source,
                com.legend.parser.GrammarLevel.LEGEND_ENGINE);
    }
}
