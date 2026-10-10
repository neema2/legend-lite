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
 * {@code ###ServiceStore} — the service-store DSL (ZTailProbe
 * "svcstore-empty"/"svcstore-rich"): an optional description (parsed,
 * never wired), then services and recursively nested service groups.
 * Parameters carry typed references ({@code String} / {@code [Integer]} /
 * {@code Class <- binding}) and OpenAPI-ish facets whose {@code x = y}
 * texts keep their own spans.
 */
public final class ServiceStoreSectionGrammar
        implements ElementwiseSectionGrammar {

    public static final ServiceStoreSectionGrammar INSTANCE =
            new ServiceStoreSectionGrammar();

    private ServiceStoreSectionGrammar() {
    }

    @Override
    public String name() {
        return "ServiceStore";
    }

    @Override
    public String qualifiedNameOf(Protocol.Element e) {
        return ((Protocol.PServiceStoreDefinition) e).qualifiedName();
    }

    @Override
    public com.legend.model.PackageableElement toModel(Protocol.Element element) {
        return com.legend.model.ModelFromProtocol.element(element);
    }

    @Override
    public Protocol.Element parseOne(TokenStreamCursor c) {
        SectionParse.Head h = SectionParse.head(c, "ServiceStore");
        c.expect(TokenType.PAREN_OPEN);
        String description = null;
        List<Protocol.PServiceStoreElement> elements = new ArrayList<>();
        while (!c.atEnd() && c.peek() != TokenType.PAREN_CLOSE) {
            String kw = c.safeText();
            switch (kw) {
                case "description" -> {
                    if (description != null) {
                        // once-only (mutant duplicate-field probe)
                        throw c.error("Field 'description' should be"
                                + " specified only once");
                    }
                    c.advance();
                    c.expect(TokenType.COLON);
                    description = SectionParse.stringValue(c);
                    c.expect(TokenType.SEMI_COLON);
                }
                case "Service" -> elements.add(parseService(c));
                case "ServiceGroup" -> elements.add(parseGroup(c));
                default -> throw c.error(
                        "unknown ServiceStore element: " + kw);
            }
        }
        c.expect(TokenType.PAREN_CLOSE);
        // engine-verbatim unique-id rule (sectioned negative pin #14)
        java.util.Set<String> ids = new java.util.HashSet<>();
        java.util.List<String> dup = new java.util.ArrayList<>();
        for (Protocol.PServiceStoreElement e : elements) {
            if (!ids.add(idOf(e)) && !dup.contains(idOf(e))) {
                dup.add(idOf(e));
            }
        }
        if (!dup.isEmpty()) {
            throw com.legend.parser.TokenStreamCursor.throwAt(
                    c.tokens(), h.declStart(),
                    "Service Store Elements should have unique ids."
                    + " Multiple elements found with ids - " + dup);
        }
        return new Protocol.PServiceStoreDefinition(h.pkg(), h.name(),
                description, elements, c.spanOf(h.declStart(), c.pos() - 1));
    }

    private static Protocol.PSsServiceGroup parseGroup(TokenStreamCursor c) {
        int start = c.pos();
        c.advance();
        String id = c.parseIdentifier();
        c.expect(TokenType.PAREN_OPEN);
        String path = null;
        List<Protocol.PServiceStoreElement> elements = new ArrayList<>();
        while (!c.atEnd() && c.peek() != TokenType.PAREN_CLOSE) {
            String kw = c.safeText();
            switch (kw) {
                case "path" -> {
                    c.advance();
                    c.expect(TokenType.COLON);
                    path = SectionParse.stringValue(c);
                    c.expect(TokenType.SEMI_COLON);
                }
                case "Service" -> elements.add(parseService(c));
                case "ServiceGroup" -> elements.add(parseGroup(c));
                default -> throw c.error(
                        "unknown ServiceGroup element: " + kw);
            }
        }
        c.expect(TokenType.PAREN_CLOSE);
        if (path == null) {
            throw c.error("ServiceGroup '" + id + "' needs a path");
        }
        return new Protocol.PSsServiceGroup(id, path, elements,
                c.spanOf(start, c.pos() - 1));
    }

    private static String idOf(Protocol.PServiceStoreElement e) {
        return e instanceof Protocol.PSsService sv ? sv.id()
                : ((Protocol.PSsServiceGroup) e).id();
    }

    private static Protocol.PSsService parseService(TokenStreamCursor c) {
        int start = c.pos();
        c.advance();
        String id = c.parseIdentifier();
        c.expect(TokenType.PAREN_OPEN);
        String path = null;
        Protocol.PSsTypeRef requestBody = null;
        String method = null;
        List<Protocol.PSsParam> parameters = null;
        Protocol.PSsTypeRef response = null;
        List<String> security = new ArrayList<>();
        while (!c.atEnd() && c.peek() != TokenType.PAREN_CLOSE) {
            String key = c.parseIdentifier();
            c.expect(TokenType.COLON);
            switch (key) {
                case "path" -> {
                    path = SectionParse.stringValue(c);
                    // engine-verbatim (sectioned negative pins #4/#6)
                    if (!path.startsWith("/") || path.endsWith("/")) {
                        throw c.error("Path should start with '/' & should"
                                + " not end with '/'");
                    }
                }
                case "requestBody" -> requestBody = parseTypeRef(c);
                case "method" -> {
                    if (method != null) {
                        throw c.error("Field 'method' should be"
                                + " specified only once");
                    }
                    method = c.parseIdentifier();
                    if (!"GET".equals(method) && !"POST".equals(method)) {
                        // engine-verbatim (embedded-data pin #12)
                        throw c.error("Unsupported HTTP Method type - "
                                + method + ". Supported types are - GET,POST");
                    }
                }
                case "parameters" -> parameters = parseParameters(c);
                case "response" -> response = parseTypeRef(c);
                case "security" -> {
                    c.expect(TokenType.BRACKET_OPEN);
                    while (c.peek() != TokenType.BRACKET_CLOSE) {
                        String scheme = c.parseIdentifier();
                        // engine-verbatim (pin #22): schemes resolve via
                        // IServiceStoreGrammarParserExtension and the
                        // reference jar set registers NONE — every id
                        // refuses (probed: Http/ApiKey/oauth all refuse)
                        throw c.error("Unsupported SecurityScheme - " + scheme);
                    }
                    c.expect(TokenType.BRACKET_CLOSE);
                }
                default -> throw c.error(
                        "unknown Service key '" + key + "'");
            }
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.PAREN_CLOSE);
        if (path == null || method == null || response == null) {
            throw c.error("Service '" + id + "' needs path, method and"
                    + " response");
        }
        // engine walker cross-validations (pins #18/#20/#28), verbatim
        if ("GET".equals(method) && requestBody != null) {
            throw com.legend.parser.TokenStreamCursor.throwAt(
                    c.tokens(), start,
                    "Request Body should not be specified for GET"
                    + " end point");
        }
        if (parameters != null) {
            java.util.List<String> missing = new java.util.ArrayList<>();
            for (Protocol.PSsParam pm : parameters) {
                if ("PATH".equals(pm.location())) {   // stored UPPERCASED on the wire
                    if (Boolean.FALSE.equals(pm.required())) {
                        throw new com.legend.parser.ParseException(
                                "Path parameters cannot be optional",
                                com.legend.protocol.SourceInfo.parsed(pm.sourceInformation()).startLine(),
                                com.legend.protocol.SourceInfo.parsed(pm.sourceInformation()).startColumn());
                    }
                    if (!path.contains("{" + pm.name() + "}")) {
                        missing.add(pm.name());
                    }
                }
            }
            if (!missing.isEmpty()) {
                throw com.legend.parser.TokenStreamCursor.throwAt(
                        c.tokens(), start,
                        "Path parameters should be specified in path"
                        + " as '{param_name}'. [" + String.join(",", missing)
                        + "] parameters were not found in path " + path);
            }
        }
        return new Protocol.PSsService(id, path, requestBody, method,
                parameters, response, security,
                c.spanOf(start, c.pos() - 1));
    }

    private static List<Protocol.PSsParam> parseParameters(
            TokenStreamCursor c) {
        List<Protocol.PSsParam> out = new ArrayList<>();
        c.expect(TokenType.PAREN_OPEN);
        while (c.peek() != TokenType.PAREN_CLOSE) {
            out.add(parseParameter(c));
            if (!c.match(TokenType.COMMA)) {
                break;
            }
        }
        c.expect(TokenType.PAREN_CLOSE);
        return out;
    }

    private static Protocol.PSsParam parseParameter(TokenStreamCursor c) {
        int start = c.pos();
        String name;
        if (c.peek() == TokenType.QUOTED_STRING) {
            String raw = c.text();
            c.advance();
            name = raw.substring(1, raw.length() - 1);
        } else {
            name = c.parseIdentifier();
        }
        c.expect(TokenType.COLON);
        Protocol.PSsTypeRef type = parseTypeRef(c);
        Boolean allowReserved = null;
        Boolean required = null;
        String location = null;
        String style = null;
        SourceInfo styleSpan = null;
        Boolean explode = null;
        SourceInfo explodeSpan = null;
        String enumeration = null;
        if (c.match(TokenType.PAREN_OPEN)) {
            while (c.peek() != TokenType.PAREN_CLOSE) {
                int optStart = c.pos();
                String opt = c.parseIdentifier();
                c.expect(TokenType.EQUAL);
                switch (opt) {
                    case "allowReserved" -> allowReserved = bool(c);
                    case "required" -> required = bool(c);
                    case "location" -> {
                        location = c.parseIdentifier();
                        if (!"header".equals(location) && !"path".equals(location)
                                && !"query".equals(location)) {
                            // engine-verbatim (pin #26)
                            throw c.error("Unsupported Parameter Location - "
                                    + location + ". Supported Locations are -"
                                    + " header,path,query");
                        }
                    }
                    case "style" -> {
                        style = c.parseIdentifier();
                        styleSpan = c.spanOf(optStart, c.pos() - 1);
                    }
                    case "explode" -> {
                        explode = bool(c);
                        explodeSpan = c.spanOf(optStart, c.pos() - 1);
                    }
                    case "enum" -> enumeration = Protocol.unquotePath(
                            c.parseQualifiedName());
                    default -> throw c.error(
                            "unknown parameter option '" + opt + "'");
                }
                if (!c.match(TokenType.COMMA)) {
                    break;
                }
            }
            c.expect(TokenType.PAREN_CLOSE);
        }
        if (location == null) {
            throw c.error("parameter '" + name + "' needs a location");
        }
        // engine-verbatim list-parameter rules (pins #8/#10/#12): list
        // params REQUIRE style+explode; non-list params must not carry them
        if (type.list()) {
            if (style == null) {
                throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), start,
                        "Field 'style' is required");
            }
            if (explode == null) {
                throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), start,
                        "Field 'explode' is required");
            }
        } else {
            if (style != null) {
                throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), start,
                        "style should not be provided with non-list"
                        + " service parameter");
            }
            if (explode != null) {
                throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), start,
                        "explode should not be provided with non-list"
                        + " service parameter");
            }
        }
        return new Protocol.PSsParam(name, type, allowReserved, required,
                location.toUpperCase(java.util.Locale.ROOT), style,
                styleSpan, explode, explodeSpan, enumeration,
                c.spanOf(start, c.pos() - 1));
    }

    /** {@code String | [Integer] | Class <- binding | [Class <- binding]}. */
    private static Protocol.PSsTypeRef parseTypeRef(TokenStreamCursor c) {
        int start = c.pos();
        boolean list = c.match(TokenType.BRACKET_OPEN);
        String head = Protocol.unquotePath(c.parseQualifiedName());
        String complexType = null;
        String binding = null;
        if (c.peek() == TokenType.LESS_THAN) {
            c.advance();
            c.expect(TokenType.MINUS);
            complexType = head;
            binding = Protocol.unquotePath(c.parseQualifiedName());
        }
        if (list) {
            c.expect(TokenType.BRACKET_CLOSE);
        }
        SourceInfo span = c.spanOf(start, c.pos() - 1);
        return complexType != null
                ? new Protocol.PSsTypeRef(null, complexType, binding, list,
                        span)
                : new Protocol.PSsTypeRef(head, null, null, list, span);
    }

    private static Boolean bool(TokenStreamCursor c) {
        if (c.match(TokenType.TRUE)) {
            return Boolean.TRUE;
        }
        if (c.match(TokenType.FALSE)) {
            return Boolean.FALSE;
        }
        throw c.error("expected true or false, got " + c.safeText());
    }
}
