// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testing;

import com.legend.model.ParsedModel;
import com.legend.parser.GrammarLevel;
import com.legend.door.ModelFromText;
import com.legend.parser.SpecParser;
import com.legend.protocol.spec.ValueSpecification;

import java.util.List;

/**
 * THE PLATFORM test regime, named ONCE — the sibling of {@link Own}
 * (the product regime). A test that parses through this fixture is
 * declaring, visibly at the import, that it exercises the m2/bootstrap
 * SUPERSET: the parser's own grammar suites, the indexer's
 * full-superset parity sweep, the corpus harness, and the quarantined
 * {@code com.legend.platform} tests.
 */
public final class Platform {

    /** The platform regime's level. */
    public static final GrammarLevel SUITE = GrammarLevel.LEGEND_PLATFORM;

    private Platform() {
    }

    public static ParsedModel model(String source) {
        return ModelFromText.parse(source, SUITE);
    }

    public static ParsedModel model(com.legend.lexer.TokenStream tokens) {
        return ModelFromText.parse(tokens.source(), SUITE);
    }

    public static ValueSpecification spec(String source) {
        return SpecParser.parse(source, SUITE);
    }

    public static ValueSpecification spec(com.legend.lexer.TokenStream tokens) {
        return SpecParser.parse(tokens, SUITE);
    }

    public static List<ValueSpecification> block(String source) {
        return SpecParser.parseCodeBlock(source, SUITE);
    }

    public static List<ValueSpecification> block(
            com.legend.lexer.TokenStream tokens) {
        return SpecParser.parseCodeBlock(tokens, SUITE);
    }

    /** One {@code Mapping} element at {@code tokenIndex} — the protocol
     *  pin tests' entry. */
    public static com.legend.protocol.Protocol.PMapping mapping(
            com.legend.lexer.TokenStream tokens, int tokenIndex,
            int sectionStartLine) {
        return com.legend.parser.MappingProtocolParser.parse(tokens,
                tokenIndex, sectionStartLine, SUITE);
    }
}
