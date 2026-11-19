// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser.section;

import com.legend.lexer.TokenType;
import com.legend.parser.TokenStreamCursor;
import com.legend.protocol.Protocol;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link LexableSectionGrammar} whose section is a flat run of
 * elements (plus {@code import} statements): the grammar supplies ONE
 * method — {@link #parseOne} — and both feeds (the shared-stream
 * {@link #parseSection} and the SPI re-lex {@link #parse}) drive it
 * through the defaults here, so the loop discipline is written once.
 */
public interface ElementwiseSectionGrammar extends LexableSectionGrammar {

    /** Parse ONE element at the cursor (which sits at the declaration
     *  keyword). */
    Protocol.Element parseOne(TokenStreamCursor c);

    /** The element's qualified name, for the SPI sink. */
    String qualifiedNameOf(Protocol.Element e);

    @Override
    default void parse(com.legend.spi.SectionSource src,
            com.legend.spi.ElementSink out) {
        var c = new SliceCursor(com.legend.lexer.Lexer.tokenize(src.text()),
                // SPI overlay hosting = the drop-in seam: engine grammar
                com.legend.parser.GrammarLevel.LEGEND_ENGINE);
        while (!c.atEnd()) {
            if (c.peek() == TokenType.IMPORT) {
                SectionImports.parseImport(c);
                continue;
            }
            Protocol.Element e = parseOne(c);
            out.accept(qualifiedNameOf(e),
                    com.legend.protocol.ProtocolEmitter.emitElement(e));
        }
    }

    @Override
    default ParsedSection parseSection(TokenStreamCursor host,
            int sectionEndOffset) {
        List<ParsedElement> elements = new ArrayList<>();
        List<String> imports = new ArrayList<>();
        while (!host.atEnd()
                && host.tokens().start(host.pos()) < sectionEndOffset) {
            if (host.peek() == TokenType.IMPORT) {
                imports.add(SectionImports.parseImport(host));
                continue;
            }
            int at = host.tokens().start(host.pos());
            elements.add(new ParsedElement(parseOne(host), at));
        }
        return new ParsedSection(elements, imports);
    }
}
