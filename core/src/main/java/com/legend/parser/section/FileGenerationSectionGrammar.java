// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser.section;

import com.legend.lexer.TokenType;
import com.legend.parser.TokenStreamCursor;
import com.legend.protocol.Protocol;
import com.legend.protocol.SourceInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code ###FileGeneration} (ZTailProbe "filegen"/"filegen-quoted"): the
 * declaration keyword is OPEN (Avro/Java/Protobuf/... — engine's own
 * grammar is generic over generator kinds); {@code scopeElements} and
 * {@code generationOutputPath} hoist out of the typed
 * configurationProperties, and a QUOTED property name keeps its quotes.
 */
public final class FileGenerationSectionGrammar
        implements ElementwiseSectionGrammar {

    public static final FileGenerationSectionGrammar INSTANCE =
            new FileGenerationSectionGrammar();

    private FileGenerationSectionGrammar() {
    }

    @Override
    public String name() {
        return "FileGeneration";
    }

    @Override
    public String qualifiedNameOf(Protocol.Element e) {
        return ((Protocol.PFileGeneration) e).qualifiedName();
    }

    @Override
    public com.legend.model.PackageableElement toModel(Protocol.Element element) {
        return com.legend.model.ModelFromProtocol.element(element);
    }

    @Override
    public Protocol.Element parseOne(TokenStreamCursor c) {
        int declStart = c.pos();
        if (!c.isIdentifierToken(c.peek())) {
            throw c.error("unsupported ###FileGeneration element: "
                    + c.safeText());
        }
        String kind = c.safeText();
        SourceInfo typeSpan = c.spanOf(declStart, declStart);
        c.advance();
        // the engine's FileGeneration grammar admits NO decorations
        // (probed live 2026-08-12: Unexpected token '<'); the old
        // parse-and-discard silently dropped them — refuse like the engine
        if (c.peek() == com.legend.lexer.TokenType.LESS_THAN
                || c.peek() == com.legend.lexer.TokenType.BRACE_OPEN) {
            throw c.error("Unexpected token '" + c.safeText() + "'");
        }
        String qn = Protocol.unquotePath(c.parseQualifiedName());
        int cut = qn.lastIndexOf("::");
        String pkg = cut < 0 ? "" : qn.substring(0, cut);
        String name = cut < 0 ? qn : qn.substring(cut + 2);
        c.expect(TokenType.BRACE_OPEN);
        String outputPath = null;
        java.util.Set<String> namedSeen = new java.util.HashSet<>();
        List<String> scopeElements = new ArrayList<>();
        List<Protocol.PConfigProperty> props = new ArrayList<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            int keyStart = c.pos();
            // a quoted property name keeps its QUOTES on the wire
            String key = c.peek() == TokenType.STRING
                    ? SectionParse.rawStringToken(c) : c.parseIdentifier();
            // NAMED fields are once-only (engine validateAndExtract...;
            // sectioned negative pins #4/#6); arbitrary CONFIG properties
            // accumulate and may repeat
            if (("scopeElements".equals(key) || "generationOutputPath".equals(key))
                    && !namedSeen.add(key)) {
                throw com.legend.parser.TokenStreamCursor.throwAt(
                        c.tokens(), declStart,
                        "Field '" + key + "' should be specified only once");
            }
            c.expect(TokenType.COLON);
            if ("scopeElements".equals(key)) {
                if (c.peek() != TokenType.BRACKET_OPEN) {
                    // a non-canonical value shape parses as a CONFIG
                    // PROPERTY in the engine grammar; the walker then
                    // refuses the reserved name at the ELEMENT (probed
                    // reprobe pins #8/#10)
                    throw com.legend.parser.TokenStreamCursor.throwAt(
                            c.tokens(), declStart,
                            "Can't have config property with reserved name '"
                            + key + "'");
                }
                c.expect(TokenType.BRACKET_OPEN);
                while (c.peek() != TokenType.BRACKET_CLOSE) {
                    scopeElements.add(Protocol.unquotePath(
                            c.parseQualifiedName()));
                    if (!c.match(TokenType.COMMA)) {
                        break;
                    }
                }
                c.expect(TokenType.BRACKET_CLOSE);
                c.expect(TokenType.SEMI_COLON);
                continue;
            }
            if ("generationOutputPath".equals(key)) {
                if (c.peek() != TokenType.STRING) {
                    throw com.legend.parser.TokenStreamCursor.throwAt(
                            c.tokens(), declStart,
                            "Can't have config property with reserved name '"
                            + key + "'");
                }
                outputPath = SectionParse.stringValue(c);
                c.expect(TokenType.SEMI_COLON);
                continue;
            }
            Protocol.PConfigValue v = parseConfigValue(c);
            c.expect(TokenType.SEMI_COLON);
            // span covers `name: value;` INCLUSIVE of the semicolon
            props.add(new Protocol.PConfigProperty(key, v,
                    c.spanOf(keyStart, c.pos() - 1)));
        }
        c.expect(TokenType.BRACE_CLOSE);
        return new Protocol.PFileGeneration(pkg, name,
                SectionParse.lowerFirst(kind), typeSpan, outputPath,
                scopeElements, props, c.spanOf(declStart, c.pos() - 1));
    }

    private static Protocol.PConfigValue parseConfigValue(TokenStreamCursor c) {
        switch (c.peek()) {
            case TRUE -> {
                c.advance();
                return new Protocol.PConfigValue.PCBoolean(true);
            }
            case FALSE -> {
                c.advance();
                return new Protocol.PConfigValue.PCBoolean(false);
            }
            case INTEGER -> {
                long v = Long.parseLong(c.text());
                c.advance();
                return new Protocol.PConfigValue.PCInteger(v);
            }
            case STRING -> {
                return new Protocol.PConfigValue.PCString(
                        SectionParse.stringValue(c));
            }
            case BRACKET_OPEN -> {
                c.advance();
                List<String> vs = new ArrayList<>();
                while (c.peek() != TokenType.BRACKET_CLOSE) {
                    vs.add(SectionParse.stringValue(c));
                    if (!c.match(TokenType.COMMA)) {
                        break;
                    }
                }
                c.expect(TokenType.BRACKET_CLOSE);
                return new Protocol.PConfigValue.PCStrings(vs);
            }
            case BRACE_OPEN -> {
                c.advance();
                var entries = new java.util.LinkedHashMap<String, String>();
                while (c.peek() != TokenType.BRACE_CLOSE) {
                    String k = c.parseIdentifier();
                    c.expect(TokenType.COLON);
                    entries.put(k, SectionParse.stringValue(c));
                    c.expect(TokenType.SEMI_COLON);
                }
                c.expect(TokenType.BRACE_CLOSE);
                return new Protocol.PConfigValue.PCMap(entries);
            }
            default -> throw c.error("unsupported config value: "
                    + c.safeText());
        }
    }
}
