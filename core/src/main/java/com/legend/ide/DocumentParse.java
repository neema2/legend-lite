// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.ide;

import com.legend.lexer.Lexer;
import com.legend.lexer.TokenStream;
import com.legend.parser.GrammarLevel;
import com.legend.parser.ElementParser;
import com.legend.model.PackageableElement;

import java.util.Objects;

/**
 * THE IDE'S PARSE DOORS (C5-J item 1, 2026-11-15: a wider Java witness): a document's tokens, and an element or an
 * import parsed from a slice of them. A token stream is the parser's and the IDE's alone (invariant 6f), so the IDE's
 * doors are its own; each asks the run's wall FIRST, then makes the one call, as {@code door.Parse}'s do (whose javadoc
 * says why). A door holds no lambda, no reference and no state ({@code ArchitectureTest
 * .everyParseIsReachedThroughADoorThatAsks}); {@code PhaseEntriesRefusedInsideARunTest} pins each refusal.
 */
public final class DocumentParse {

    private DocumentParse() {
    }

    /** A model's text as its tokens (phase A): the IDE's document. */
    public static TokenStream tokens(String source) {
        com.legend.base.RunScope.planning("lex a model's text (phase A)");
        return Lexer.tokenize(Objects.requireNonNull(source, "source"));
    }

    /** One element from a slice of a document's tokens (phase B): its record, made the compiler's model by the text's
     *  door ({@code door.ModelFromText.element}, which asks as well). */
    public static PackageableElement element(TokenStream slice, GrammarLevel dialect) {
        com.legend.base.RunScope.planning("parse a model's element (phase B)");
        return com.legend.door.ModelFromText.element(ElementParser.parseSingle(slice, dialect));
    }

    /** One {@code import} statement from a slice of a document's tokens (phase B). */
    public static String importOf(TokenStream slice, GrammarLevel dialect) {
        com.legend.base.RunScope.planning("parse a model's import (phase B)");
        return ElementParser.parseSingleImport(slice, dialect);
    }
}
