// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser.section;

import com.legend.lexer.TokenType;
import com.legend.parser.TokenStreamCursor;
import com.legend.protocol.Protocol;

import java.util.ArrayList;
import java.util.List;

/** {@code ###GenerationSpecification} (ZTailProbe "genspec"): generation
 *  nodes (id defaults to the element path) + FILE_GENERATION pointers. */
public final class GenerationSpecificationSectionGrammar
        implements ElementwiseSectionGrammar {

    public static final GenerationSpecificationSectionGrammar INSTANCE =
            new GenerationSpecificationSectionGrammar();

    private GenerationSpecificationSectionGrammar() {
    }

    @Override
    public String name() {
        return "GenerationSpecification";
    }

    @Override
    public String qualifiedNameOf(Protocol.Element e) {
        return ((Protocol.PGenerationSpecification) e).qualifiedName();
    }

    @Override
    public com.legend.model.PackageableElement toModel(Protocol.Element element) {
        return com.legend.model.ModelFromProtocol.element(element);
    }

    @Override
    public Protocol.Element parseOne(TokenStreamCursor c) {
        SectionParse.Head h = SectionParse.head(c, "GenerationSpecification");
        c.expect(TokenType.BRACE_OPEN);
        List<Protocol.PGenerationNode> nodes = new ArrayList<>();
        List<Protocol.PPointer> fileGens = new ArrayList<>();
        java.util.Set<String> seenKeys = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seenKeys, key, c, h.declStart());
            c.expect(TokenType.COLON);
            switch (key) {
                case "generationNodes" -> parseNodes(c, nodes);
                case "fileGenerations" -> parsePointers(c, fileGens);
                default -> throw c.error(
                        "unknown GenerationSpecification key '" + key + "'");
            }
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        return new Protocol.PGenerationSpecification(h.pkg(), h.name(), nodes,
                fileGens, c.spanOf(h.declStart(), c.pos() - 1));
    }

    private static void parseNodes(TokenStreamCursor c,
            List<Protocol.PGenerationNode> nodes) {
        c.expect(TokenType.BRACKET_OPEN);
        while (c.peek() != TokenType.BRACKET_CLOSE) {
            int nodeStart = c.pos();
            c.expect(TokenType.BRACE_OPEN);
            String id = null;
            String element = null;
            while (c.peek() != TokenType.BRACE_CLOSE) {
                String nk = c.parseIdentifier();
                c.expect(TokenType.COLON);
                switch (nk) {
                    case "id" -> id = SectionParse.stringValue(c);
                    case "generationElement" -> element = Protocol.unquotePath(
                            c.parseQualifiedName());
                    default -> throw c.error(
                            "unknown generation-node key '" + nk + "'");
                }
                c.expect(TokenType.SEMI_COLON);
            }
            c.expect(TokenType.BRACE_CLOSE);
            if (element == null) {
                throw c.error("generation node needs a generationElement");
            }
            // the wire id defaults to the element path (ZTailProbe genspec)
            nodes.add(new Protocol.PGenerationNode(element,
                    id == null ? element : id,
                    c.spanOf(nodeStart, c.pos() - 1)));
            if (!c.match(TokenType.COMMA)) {
                break;
            }
        }
        c.expect(TokenType.BRACKET_CLOSE);
    }

    private static void parsePointers(TokenStreamCursor c,
            List<Protocol.PPointer> out) {
        c.expect(TokenType.BRACKET_OPEN);
        while (c.peek() != TokenType.BRACKET_CLOSE) {
            int s = c.pos();
            String path = Protocol.unquotePath(c.parseQualifiedName());
            out.add(new Protocol.PPointer("FILE_GENERATION", path,
                    c.spanOf(s, c.pos() - 1)));
            if (!c.match(TokenType.COMMA)) {
                break;
            }
        }
        c.expect(TokenType.BRACKET_CLOSE);
    }
}
