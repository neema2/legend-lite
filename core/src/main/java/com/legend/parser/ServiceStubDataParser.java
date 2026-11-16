// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser;

import com.legend.lexer.TokenType;
import com.legend.protocol.Protocol;
import com.legend.protocol.SourceInfo;

import java.util.ArrayList;
import java.util.List;

/** The ServiceStore embedded-data STUB grammar ({@code request:}/
 *  {@code response:} pairs with parameter contentPatterns) — extracted
 *  from {@link MappingProtocolParser} (file-size guardrail); the engine
 *  reference is ServiceStoreEmbeddedDataParseTreeWalker. */
final class ServiceStubDataParser {

    private ServiceStubDataParser() {
    }

    static Protocol.PServiceStub parseServiceStub(MappingProtocolParser p) {
        int sTok = p.pos();
        p.expect(TokenType.BRACE_OPEN);
        String method = null;
        boolean methodSpelled = false;
        String url = null;
        String urlPath = null;
        List<Protocol.PStubParam> queryParams = null;
        List<Protocol.PStubParam> headerParams = null;
        List<Protocol.PStringValuePattern> bodyPatterns = null;
        SourceInfo requestSpan = null;
        SourceInfo responseSpan = null;
        int requestKeyTok = -1;
        int responseKeyTok = -1;
        Protocol.PEmbeddedDataValue bodyValue = null;
        int bodyKindTok = -1;
        while (!p.atEnd() && p.peek() != TokenType.BRACE_CLOSE) {
            int keyTok = p.pos();
            String key = p.parseIdentifier();
            p.expect(TokenType.COLON);
            if ("request".equals(key)) {
                if (requestSpan != null) {
                    throw TokenStreamCursor.throwAt(p.tokens(), sTok,
                            "Field 'request' should be specified only once");
                }
                requestKeyTok = keyTok;
                p.expect(TokenType.BRACE_OPEN);
                java.util.Set<String> seenReq = new java.util.HashSet<>();
                while (!p.atEnd() && p.peek() != TokenType.BRACE_CLOSE) {
                    String rk = p.parseIdentifier();
                    // the engine extracts each request field once-only,
                    // anchored at the request ctx
                    if (!seenReq.add(rk)) {
                        throw TokenStreamCursor.throwAt(p.tokens(), requestKeyTok,
                                "Field '" + rk
                                + "' should be specified only once");
                    }
                    p.expect(TokenType.COLON);
                    switch (rk) {
                        case "method" -> {
                            methodSpelled = true;
                            method = p.parseIdentifier();
                            if (!"GET".equals(method) && !"POST".equals(method)) {
                                // engine-verbatim (embedded-data pin #12)
                                throw p.error("Unsupported HTTP Method type - "
                                        + method
                                        + ". Supported types are - GET,POST");
                            }
                        }
                        case "url" -> url = p.consumeStringLiteral("'url'");
                        case "urlPath" ->
                                urlPath = p.consumeStringLiteral("'urlPath'");
                        case "queryParameters" ->
                                queryParams = parseStubParams(p, "Query Param");
                        case "headerParameters" ->
                                headerParams = parseStubParams(p, "Header Param");
                        case "bodyPatterns" -> {
                            // [ pattern* ] — NO separators (engine grammar)
                            bodyPatterns = new ArrayList<>();
                            p.expect(TokenType.BRACKET_OPEN);
                            while (!p.atEnd()
                                    && p.peek() != TokenType.BRACKET_CLOSE) {
                                bodyPatterns.add(parseContentPattern(p));
                            }
                            p.expect(TokenType.BRACKET_CLOSE);
                        }
                        default -> throw p.error(
                                "unknown service-stub request key: " + rk);
                    }
                    p.expect(TokenType.SEMI_COLON);
                }
                p.expect(TokenType.BRACE_CLOSE);
                p.expect(TokenType.SEMI_COLON);
                requestSpan = p.spanOf(keyTok, p.pos() - 1);
            } else if ("response".equals(key)) {
                if (responseSpan != null) {
                    throw TokenStreamCursor.throwAt(p.tokens(), sTok,
                            "Field 'response' should be specified only once");
                }
                responseKeyTok = keyTok;
                p.expect(TokenType.BRACE_OPEN);
                while (!p.atEnd() && p.peek() != TokenType.BRACE_CLOSE) {
                    String rk = p.parseIdentifier();
                    p.expect(TokenType.COLON);
                    if (!"body".equals(rk)) {
                        throw p.error("unknown service-stub response key: " + rk);
                    }
                    if (bodyValue != null) {
                        throw TokenStreamCursor.throwAt(p.tokens(), responseKeyTok,
                                "Field 'body' should be specified only once");
                    }
                    // the engine parses ANY embedded data here and refuses
                    // non-ExternalFormat AFTER the parse, anchored at the
                    // data KIND (probed pin #14)
                    bodyKindTok = p.pos();
                    bodyValue = p.parseEmbeddedValue();
                    p.expect(TokenType.SEMI_COLON);
                }
                p.expect(TokenType.BRACE_CLOSE);
                p.expect(TokenType.SEMI_COLON);
                responseSpan = p.spanOf(keyTok, p.pos() - 1);
            } else {
                throw p.error("unknown service-stub key: " + key);
            }
        }
        int closeTok = p.pos();
        p.expect(TokenType.BRACE_CLOSE);
        // engine validation order (probed pins #6/#10/#14): stub-level
        // request/response extraction first, then the request visit
        // (method), then the response visit (body, ExternalFormat check)
        if (requestSpan == null) {
            throw TokenStreamCursor.throwAt(p.tokens(), sTok,
                    "Field 'request' is required");
        }
        if (responseSpan == null) {
            throw TokenStreamCursor.throwAt(p.tokens(), sTok,
                    "Field 'response' is required");
        }
        if (!methodSpelled) {
            throw TokenStreamCursor.throwAt(p.tokens(), requestKeyTok,
                    "Field 'method' is required");
        }
        if (bodyValue == null) {
            throw TokenStreamCursor.throwAt(p.tokens(), responseKeyTok,
                    "Field 'body' is required");
        }
        if (!(bodyValue instanceof Protocol.PExternalFormatData body)) {
            throw TokenStreamCursor.throwAt(p.tokens(), bodyKindTok,
                    "Service response body should be ExternalFormat"
                    + " Embedded Data");
        }
        return new Protocol.PServiceStub(
                java.util.Objects.requireNonNull(method), url, urlPath,
                queryParams, headerParams, bodyPatterns, requestSpan, body,
                responseSpan, p.spanOf(sTok, closeTok));
    }

    /** {@code { name: <pattern>, 'quoted': <pattern> }} — the engine
     *  refuses a repeated name at the ENTRY, engine-verbatim message. */
    private static List<Protocol.PStubParam> parseStubParams(
            MappingProtocolParser p, String flavor) {
        p.expect(TokenType.BRACE_OPEN);
        List<Protocol.PStubParam> out = new ArrayList<>();
        java.util.Set<String> names = new java.util.HashSet<>();
        while (!p.atEnd() && p.peek() != TokenType.BRACE_CLOSE) {
            int pTok = p.pos();
            String pname;
            if (p.peek() == TokenType.QUOTED_STRING
                    || p.peek() == TokenType.STRING) {
                pname = p.consumeStringLiteral("parameter name");
            } else {
                pname = p.parseIdentifier();
            }
            p.expect(TokenType.COLON);
            Protocol.PStringValuePattern pattern = parseContentPattern(p);
            if (!names.add(pname)) {
                throw TokenStreamCursor.throwAt(p.tokens(), pTok,
                        flavor + " : '" + pname
                        + "' value should be defined only once");
            }
            out.add(new Protocol.PStubParam(pname, pattern));
            if (!p.match(TokenType.COMMA)) {
                break;
            }
        }
        p.expect(TokenType.BRACE_CLOSE);
        return out;
    }

    /** {@code EqualTo #{ expected: '...'; }#} — the engine's TWO
     *  registered contentPattern extensions; an unknown kind refuses at
     *  the KIND token, engine-verbatim (probed pin #16). */
    private static Protocol.PStringValuePattern parseContentPattern(
            MappingProtocolParser p) {
        int kindTok = p.pos();
        String kind = p.parseIdentifier();
        String type = switch (kind) {
            case "EqualTo" -> "equalTo";
            case "EqualToJson" -> "equalToJson";
            default -> throw TokenStreamCursor.throwAt(p.tokens(), kindTok,
                    "Unknown contentPattern pattern type: " + kind);
        };
        MappingProtocolParser.IslandBlock island = p.readIsland();
        MappingProtocolParser inner = new MappingProtocolParser(
                island.tokens(), 0, p.dialect());
        if (!"expected".equals(inner.text())) {
            throw inner.error("unknown contentPattern key: "
                    + inner.safeText());
        }
        inner.advance();
        inner.expect(TokenType.COLON);
        String expected = expectedValue(inner, "equalToJson".equals(type));
        // both pattern grammars are `definition: expectedDefinition EOF;
        // expectedDefinition: EXPECTED COLON STRING SEMI_COLON` (4.145.0):
        // the semicolon is required and nothing follows it (the sibling
        // deck's mutants, 2026-11-14)
        inner.expect(TokenType.SEMI_COLON);
        if (!inner.atEnd()) {
            throw inner.error("Unexpected token '" + inner.safeText() + "'");
        }
        return new Protocol.PStringValuePattern(type, expected);
    }

    /** A content pattern's {@code expected} STRING, read as the engine's
     *  two pattern parsers read it (4.145.0): {@code EqualTo} by
     *  {@code fromGrammarString(text, true)}, {@code EqualToJson} by
     *  {@code fromGrammarString(text, false)} — the JSON keeps its escapes
     *  as written ({@code '{\"a\": 1}'} is {@code {\"a\": 1}} on the wire).
     *  The engine's STRING is {@code TextBlock | String}: a
     *  {@code '''...'''} text block takes the block's layout, unescaped
     *  for {@code EqualTo} only; a {@code '''} literal that is no text
     *  block the engine's lexer splits into adjacent strings, and refuses
     *  (Studio's roundtrip case DSL_Data-basic-dataElement, 2026-11-14). */
    private static String expectedValue(MappingProtocolParser inner,
            boolean keepEscapes) {
        if (!inner.atEnd() && inner.peek() == TokenType.DOC_STRING) {
            String raw = inner.text();
            if (!TokenStreamCursor.isTextBlock(raw)) {
                throw inner.error("Unexpected token '" + raw + "'");
            }
            inner.advance();
            return keepEscapes ? TokenStreamCursor.textBlockLayout(raw)
                    : TokenStreamCursor.docStringValue(raw);
        }
        if (!keepEscapes) {
            return inner.consumeStringLiteral("'expected'");
        }
        if (inner.atEnd() || inner.peek() != TokenType.STRING) {
            inner.rejectInvalid();
            throw inner.error("expected a string literal for 'expected', got "
                    + inner.peek());
        }
        String raw = inner.text();
        inner.advance();
        return raw.substring(1, raw.length() - 1);
    }

}
