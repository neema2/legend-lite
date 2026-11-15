// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.lexer.Lexer;
import com.legend.lexer.TokenStream;
import com.legend.parser.SpecParser;

/**
 * A class PLANTED in the server's package to parse past the parse doors, for one test alone (C5-J item 1, 2026-11-15):
 * {@code ArchitectureTest.PARSE_BY_THE_DOORS} fails on it, by a call and by a reference. A test class: the rule's real
 * check reads the product's classes, never this one.
 */
public final class PlantedParse {

    private PlantedParse() {
    }

    /** A lambda parsed outside the doors: the defect the rule refuses. */
    public static String parse(String lambda) {
        return SpecParser.lambdaJson(lambda, false);
    }

    /** A range of a lexed text lexed again outside the doors (the C5-J audit's SHOULD 2, 2026-11-15:
     *  {@code TokenStream.lexRange} lexes, so it is no reader). */
    public static final class LexesARange {
        private LexesARange() {
        }

        public static TokenStream island(TokenStream tokens) {
            return tokens.lexRange(0, 1);
        }
    }

    /** A text lexed outside the doors, by the method's reference, never called. */
    public static final class ByReference {
        private ByReference() {
        }

        public static java.util.function.Function<String, TokenStream> lexing() {
            return Lexer::tokenize;
        }
    }
}
