// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser.section;

import com.legend.lexer.TokenType;
import com.legend.parser.TokenStreamCursor;
import com.legend.protocol.Protocol;

/** {@code ###Text} — {@code Text qn { type: STRING; content: '...'; }};
 *  type optional (ZTailProbe "text"). */
public final class TextSectionGrammar implements ElementwiseSectionGrammar {

    public static final TextSectionGrammar INSTANCE = new TextSectionGrammar();

    private TextSectionGrammar() {
    }

    @Override
    public String name() {
        return "Text";
    }

    @Override
    public String qualifiedNameOf(Protocol.Element e) {
        return ((Protocol.PText) e).qualifiedName();
    }

    @Override
    public com.legend.model.PackageableElement toModel(Protocol.Element element) {
        return com.legend.model.ModelFromProtocol.element(element);
    }

    @Override
    public Protocol.Element parseOne(TokenStreamCursor c) {
        SectionParse.Head h = SectionParse.head(c, "Text");
        c.expect(TokenType.BRACE_OPEN);
        String type = null;
        String content = null;
        java.util.Set<String> seenKeys = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seenKeys, key, c, h.declStart());
            c.expect(TokenType.COLON);
            switch (key) {
                case "type" -> type = c.parseIdentifier();
                case "content" -> content = SectionParse.stringValue(c);
                default -> throw c.error("unknown Text key '" + key + "'");
            }
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        if (content == null) {
            throw com.legend.parser.TokenStreamCursor.throwAt(
                    c.tokens(), h.declStart(), "Text needs content");
        }
        return new Protocol.PText(h.pkg(), h.name(), type, content,
                c.spanOf(h.declStart(), c.pos() - 1));
    }
}
