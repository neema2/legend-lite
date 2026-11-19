// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testing;

import com.legend.lexer.TokenStream;
import com.legend.parser.GrammarLevel;
import com.legend.door.ModelFromText;
import com.legend.parser.ElementParser;

/**
 * THE ENGINE-WIRE test regime, named ONCE — the protocol emission pin
 * tests byte-compare our wire output against {@code PureGrammarParser}'s,
 * so their parses run at the drop-in level.
 */
public final class Engine {

    /** The engine-wire regime's level. */
    public static final GrammarLevel SUITE = GrammarLevel.LEGEND_ENGINE;

    private Engine() {
    }

    /** A whole-model parse at the engine level (strict-surface pins). */
    public static com.legend.model.ParsedModel model(String source) {
        return ModelFromText.parse(source, SUITE);
    }

    /** A parser positioned at {@code tokenIndex}, engine level. */
    public static ElementParser at(TokenStream tokens, int tokenIndex) {
        return ElementParser.at(tokens, tokenIndex, SUITE);
    }
}
