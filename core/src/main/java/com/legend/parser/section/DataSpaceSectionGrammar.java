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
 * THE {@code ###DataSpace} grammar — fifth built-in behind the
 * {@link com.legend.spi.SectionGrammar} seam. Owns {@code DataSpace}
 * elements to the corpus-censused scope: decorated envelope (documentation
 * included, 4.145.0), executionContexts (mapping OR mappingProvider,
 * optional defaultRuntime, {@code testData} islands carried raw),
 * defaultExecutionContext, title/description, executables (path form AND
 * inline-query form, with sampleValues), diagrams, supportInfo (Email /
 * Combined / the keyword-less full form), operationalMetadata and the
 * include/exclude {@code elements} scope list. Since 4.145.0 the engine
 * requires nothing but the name: executionContexts and
 * defaultExecutionContext are optional.
 *
 * <p>Wire shape claimed (ZTailProbe "dataspace-rich"/"dataspace-email"):
 * {@code _type:"dataSpace"}, byte-exact via {@code TailEmitter}.
 */
public final class DataSpaceSectionGrammar
        implements ElementwiseSectionGrammar {

    /** The one stateless instance the registry hands out. */
    public static final DataSpaceSectionGrammar INSTANCE =
            new DataSpaceSectionGrammar();

    private DataSpaceSectionGrammar() {
    }

    @Override
    public String name() {
        return "DataSpace";
    }

    @Override
    public String qualifiedNameOf(Protocol.Element e) {
        return ((Protocol.PDataSpace) e).qualifiedName();
    }

    @Override
    public Protocol.Element parseOne(TokenStreamCursor c) {
        return parseElement(c);
    }

    @Override
    public com.legend.model.PackageableElement toModel(Protocol.Element element) {
        return com.legend.model.FromProtocol.toDataSpaceDefinition(
                (Protocol.PDataSpace) element);
    }

    /** The engine's own enum names (DataSpaceRegion, DataSpaceDeliveryFrequency):
     *  a value is validated as written, with the engine's message. */
    private static final List<String> REGIONS = List.of("APAC", "EMEA", "LAMR", "NAMR");
    private static final List<String> FREQUENCIES = List.of("INTRADAY", "DAILY", "WEEKLY",
            "MONTHLY", "QUARTERLY", "YEARLY", "ON_DEMAND");

    /** One {@code '''doc''' DataSpace <<...>> {...tags} qn { body }} element. */
    public static Protocol.PDataSpace parseElement(TokenStreamCursor c) {
        int declStart = c.pos();
        // dataSpaceElement: documentation? DATA_SPACE stereotypes? taggedValues? ...
        TokenStreamCursor.Documentation doc = c.parseDocumentation();
        if (!c.isIdentifierToken(c.peek())
                || !"DataSpace".equals(c.safeText())) {
            throw c.error("unsupported ###DataSpace element: " + c.safeText());
        }
        c.advance();                                // 'DataSpace'
        TokenStreamCursor.Decorations dec = c.withDocumentation(doc, c.parseDecorations());
        String qn = Protocol.unquotePath(c.parseQualifiedName());
        int cut = qn.lastIndexOf("::");
        String pkg = cut < 0 ? "" : qn.substring(0, cut);
        String name = cut < 0 ? qn : qn.substring(cut + 2);
        c.expect(TokenType.BRACE_OPEN);

        // optional since 4.145.0, but the wire always carries the list (an
        // unspelled key is an EMPTY list, never an omitted slot)
        List<Protocol.PDataSpaceContext> contexts = new ArrayList<>();
        String defaultContext = null;
        String title = null;
        String description = null;
        List<Protocol.PDataSpaceExecutable> executables = null;
        List<Protocol.PDataSpaceDiagram> diagrams = null;
        List<Protocol.PDataSpaceDiagram> featuredDiagrams = new ArrayList<>();
        Protocol.PDataSpaceSupport supportInfo = null;
        Protocol.PDataSpaceOperationalMetadata operationalMetadata = null;
        List<Protocol.PDataSpaceElementRef> elements = null;

        java.util.Set<String> seenKeys = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            int keyStart = c.pos();
            String key = c.parseIdentifier();
            if (!"groupId".equals(key) && !"artifactId".equals(key)
                    && !"versionId".equals(key)) {
                // the deprecated coordinates DUPLICATE freely — the walker
                // silently drops the whole field on >1 (probed dup wire)
                TokenStreamCursor.once(seenKeys, key, c, declStart);
            }
            c.expect(TokenType.COLON);
            switch (key) {
                case "groupId", "artifactId", "versionId" -> {
                    // parsed and DROPPED: the engine grammar admits these
                    // deprecated coordinates but its walker never reads
                    // them (C12 TestDataSpaceGrammarRoundtrip#7)
                    SectionParse.stringValue(c);
                    c.expect(TokenType.SEMI_COLON);
                }
                case "featuredDiagrams" -> {
                    // deprecated alias: each path becomes a diagram entry
                    // with an EMPTY title, spans = the path; the walker
                    // APPENDS featured after declared diagrams
                    c.expect(TokenType.BRACKET_OPEN);
                    while (c.peek() != TokenType.BRACKET_CLOSE) {
                        int ds = c.pos();
                        String path = Protocol.unquotePath(
                                c.parseQualifiedName());
                        var span = c.spanOf(ds, c.pos() - 1);
                        featuredDiagrams.add(new Protocol.PDataSpaceDiagram(
                                "", null, path, span, span));
                        if (!c.match(TokenType.COMMA)) {
                            break;
                        }
                    }
                    c.expect(TokenType.BRACKET_CLOSE);
                    c.expect(TokenType.SEMI_COLON);
                }
                case "executionContexts" -> parseContexts(c, contexts);
                case "defaultExecutionContext" -> {
                    defaultContext = SectionParse.stringValue(c);
                    c.expect(TokenType.SEMI_COLON);
                }
                case "title" -> {
                    title = SectionParse.stringValue(c);
                    c.expect(TokenType.SEMI_COLON);
                }
                case "description" -> {
                    description = SectionParse.stringValue(c);
                    c.expect(TokenType.SEMI_COLON);
                }
                case "executables" -> {
                    executables = new ArrayList<>();
                    parseExecutables(c, executables);
                }
                case "diagrams" -> {
                    diagrams = new ArrayList<>();
                    parseDiagrams(c, diagrams);
                }
                case "supportInfo" -> {
                    supportInfo = parseSupportInfo(c);
                    c.expect(TokenType.SEMI_COLON);
                }
                case "operationalMetadata" -> {
                    operationalMetadata = parseOperationalMetadata(c, keyStart);
                }
                case "elements" -> {
                    // [ model, -model::experiment ] — the ref span
                    // includes the '-' of an exclusion
                    elements = new ArrayList<>();
                    c.expect(TokenType.BRACKET_OPEN);
                    while (c.peek() != TokenType.BRACKET_CLOSE && !c.atEnd()) {
                        int s = c.pos();
                        boolean excluded = c.match(TokenType.MINUS);
                        String path = Protocol.unquotePath(
                                c.parseQualifiedName());
                        elements.add(new Protocol.PDataSpaceElementRef(path,
                                excluded, c.spanOf(s, c.pos() - 1)));
                        c.match(TokenType.COMMA);
                    }
                    c.expect(TokenType.BRACKET_CLOSE);
                    c.expect(TokenType.SEMI_COLON);
                }
                default -> throw c.error("unknown key '" + key
                        + "' inside DataSpace '" + qn + "'");
            }
        }
        c.expect(TokenType.BRACE_CLOSE);
        List<Protocol.PDataSpaceDiagram> mergedDiagrams = diagrams;
        if (!featuredDiagrams.isEmpty()) {
            mergedDiagrams = mergedDiagrams == null
                    ? featuredDiagrams
                    : new ArrayList<>(mergedDiagrams);
            if (mergedDiagrams != featuredDiagrams) {
                mergedDiagrams.addAll(featuredDiagrams);
            }
        }
        return new Protocol.PDataSpace(pkg, name, dec.stereotypes(),
                dec.taggedValues(), contexts, defaultContext, title,
                description, executables, mergedDiagrams, supportInfo,
                operationalMetadata, elements, c.spanOf(declStart, c.pos() - 1));
    }

    /** {@code operationalMetadata: { coverageRegions: [A, B]; updateFrequency: F; };}
     *  — the span covers key through the closing brace (the engine's ctx
     *  includes the terminating semicolon: {@code ... BRACE_CLOSE SEMI_COLON}). */
    private static Protocol.PDataSpaceOperationalMetadata parseOperationalMetadata(
            TokenStreamCursor c, int keyStart) {
        c.expect(TokenType.BRACE_OPEN);
        List<String> regions = List.of();
        String frequency = null;
        java.util.Set<String> seen = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seen, key, c, keyStart);
            c.expect(TokenType.COLON);
            switch (key) {
                case "coverageRegions" -> {
                    regions = new ArrayList<>();
                    c.expect(TokenType.BRACKET_OPEN);
                    while (c.peek() != TokenType.BRACKET_CLOSE && !c.atEnd()) {
                        int vTok = c.pos();
                        String v = c.parseIdentifier();
                        if (!REGIONS.contains(v)) {
                            throw TokenStreamCursor.throwAt(c.tokens(), vTok,
                                    "Unknown coverage region '" + v + "'. Valid values: " + REGIONS);
                        }
                        regions.add(v);
                        if (!c.match(TokenType.COMMA)) {
                            break;
                        }
                    }
                    c.expect(TokenType.BRACKET_CLOSE);
                }
                case "updateFrequency" -> {
                    int vTok = c.pos();
                    frequency = c.parseIdentifier();
                    if (!FREQUENCIES.contains(frequency)) {
                        throw TokenStreamCursor.throwAt(c.tokens(), vTok,
                                "Unknown update frequency '" + frequency + "'. Valid values: "
                                        + FREQUENCIES);
                    }
                }
                default -> throw c.error("unknown operationalMetadata key '" + key + "'");
            }
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        c.expect(TokenType.SEMI_COLON);
        return new Protocol.PDataSpaceOperationalMetadata(regions, frequency,
                c.spanOf(keyStart, c.pos() - 1));
    }

    /** {@code Email { address: '...'; } | Combined { ...; emails: [..]; } |
     *  { documentation: {..}; ... }} (the keyword-less FULL form, 4.145.0)
     *  — span covers the value only ({@code Kind { ... }} / {@code { ... }}). */
    private static Protocol.PDataSpaceSupport parseSupportInfo(
            TokenStreamCursor c) {
        int start = c.pos();
        if (c.peek() == TokenType.BRACE_OPEN) {
            return parseSupportFull(c);
        }
        String kind = c.parseIdentifier();
        c.expect(TokenType.BRACE_OPEN);
        String address = null;
        String documentationUrl = null;
        String website = null;
        String faqUrl = null;
        String supportUrl = null;
        List<String> emails = null;
        java.util.Set<String> seenKeys2 = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seenKeys2, key, c, start);
            c.expect(TokenType.COLON);
            switch (key) {
                case "address" -> address = SectionParse.stringValue(c);
                case "documentationUrl" -> documentationUrl = SectionParse.stringValue(c);
                case "website" -> website = SectionParse.stringValue(c);
                case "faqUrl" -> faqUrl = SectionParse.stringValue(c);
                case "supportUrl" -> supportUrl = SectionParse.stringValue(c);
                case "emails" -> {
                    emails = new ArrayList<>();
                    c.expect(TokenType.BRACKET_OPEN);
                    while (c.peek() != TokenType.BRACKET_CLOSE) {
                        emails.add(SectionParse.stringValue(c));
                        if (!c.match(TokenType.COMMA)) {
                            break;
                        }
                    }
                    c.expect(TokenType.BRACKET_CLOSE);
                }
                default -> throw c.error("unknown supportInfo key '" + key
                        + "'");
            }
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        SourceInfo span = c.spanOf(start, c.pos() - 1);
        return switch (kind) {
            case "Email" -> {
                if (address == null) {
                    // engine-verbatim (probed live: Email{} without address)
                    throw TokenStreamCursor.throwAt(c.tokens(), start,
                            "Field 'address' is required");
                }
                yield new Protocol.PDataSpaceSupport.PSupportEmail(address,
                        documentationUrl, span);
            }
            case "Combined" -> new Protocol.PDataSpaceSupport
                    .PSupportCombined(documentationUrl, website, faqUrl,
                            supportUrl, emails, span);
            default -> throw c.error("unknown supportInfo kind '" + kind
                    + "'");
        };
    }

    /** The full form: {@code { documentation: {label; url}; website: {..};
     *  faqUrl: {..}; supportUrl: {..}; emails: [{title; address}];
     *  expertise: [{description; expertIds: [..]}]; }}. */
    private static Protocol.PDataSpaceSupport parseSupportFull(TokenStreamCursor c) {
        int start = c.pos();
        c.expect(TokenType.BRACE_OPEN);
        Protocol.PDataSpaceLink documentation = null;
        Protocol.PDataSpaceLink website = null;
        Protocol.PDataSpaceLink faqUrl = null;
        Protocol.PDataSpaceLink supportUrl = null;
        List<Protocol.PDataSpaceEmail> emails = null;
        List<Protocol.PDataSpaceExpertise> expertise = null;
        java.util.Set<String> seen = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seen, key, c, start);
            c.expect(TokenType.COLON);
            switch (key) {
                case "documentation" -> documentation = parseLink(c);
                case "website" -> website = parseLink(c);
                case "faqUrl" -> faqUrl = parseLink(c);
                case "supportUrl" -> supportUrl = parseLink(c);
                case "emails" -> {
                    emails = new ArrayList<>();
                    c.expect(TokenType.BRACKET_OPEN);
                    while (c.peek() != TokenType.BRACKET_CLOSE && !c.atEnd()) {
                        emails.add(parseEmail(c));
                        if (!c.match(TokenType.COMMA)) {
                            break;
                        }
                    }
                    c.expect(TokenType.BRACKET_CLOSE);
                }
                case "expertise" -> {
                    expertise = new ArrayList<>();
                    c.expect(TokenType.BRACKET_OPEN);
                    while (c.peek() != TokenType.BRACKET_CLOSE && !c.atEnd()) {
                        expertise.add(parseExpertise(c));
                        if (!c.match(TokenType.COMMA)) {
                            break;
                        }
                    }
                    c.expect(TokenType.BRACKET_CLOSE);
                }
                default -> throw c.error("unknown supportInfo key '" + key + "'");
            }
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        return new Protocol.PDataSpaceSupport.PSupportFull(documentation, website, faqUrl,
                supportUrl, emails, expertise, c.spanOf(start, c.pos() - 1));
    }

    /** {@code { label: '..'; url: '..'; }} — url required. */
    private static Protocol.PDataSpaceLink parseLink(TokenStreamCursor c) {
        int start = c.pos();
        c.expect(TokenType.BRACE_OPEN);
        String label = null;
        String url = null;
        java.util.Set<String> seen = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seen, key, c, start);
            c.expect(TokenType.COLON);
            switch (key) {
                case "label" -> label = SectionParse.stringValue(c);
                case "url" -> url = SectionParse.stringValue(c);
                default -> throw c.error("unknown link key '" + key + "'");
            }
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        if (url == null) {
            throw TokenStreamCursor.throwAt(c.tokens(), start, "Field 'url' is required");
        }
        return new Protocol.PDataSpaceLink(label, url, c.spanOf(start, c.pos() - 1));
    }

    /** {@code { title: '..'; address: '..'; }} — both required. */
    private static Protocol.PDataSpaceEmail parseEmail(TokenStreamCursor c) {
        int start = c.pos();
        c.expect(TokenType.BRACE_OPEN);
        String title = null;
        String address = null;
        java.util.Set<String> seen = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seen, key, c, start);
            c.expect(TokenType.COLON);
            switch (key) {
                case "title" -> title = SectionParse.stringValue(c);
                case "address" -> address = SectionParse.stringValue(c);
                default -> throw c.error("unknown email key '" + key + "'");
            }
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        if (title == null) {
            throw TokenStreamCursor.throwAt(c.tokens(), start, "Field 'title' is required");
        }
        if (address == null) {
            throw TokenStreamCursor.throwAt(c.tokens(), start, "Field 'address' is required");
        }
        return new Protocol.PDataSpaceEmail(title, address, c.spanOf(start, c.pos() - 1));
    }

    /** {@code { description: '..'; expertIds: ['..', '..']; }} — both optional. */
    private static Protocol.PDataSpaceExpertise parseExpertise(TokenStreamCursor c) {
        int start = c.pos();
        c.expect(TokenType.BRACE_OPEN);
        String description = null;
        List<String> expertIds = null;
        java.util.Set<String> seen = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seen, key, c, start);
            c.expect(TokenType.COLON);
            switch (key) {
                case "description" -> description = SectionParse.stringValue(c);
                case "expertIds" -> {
                    expertIds = new ArrayList<>();
                    c.expect(TokenType.BRACKET_OPEN);
                    while (c.peek() != TokenType.BRACKET_CLOSE && !c.atEnd()) {
                        expertIds.add(SectionParse.stringValue(c));
                        if (!c.match(TokenType.COMMA)) {
                            break;
                        }
                    }
                    c.expect(TokenType.BRACKET_CLOSE);
                }
                default -> throw c.error("unknown expertise key '" + key + "'");
            }
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        return new Protocol.PDataSpaceExpertise(description, expertIds,
                c.spanOf(start, c.pos() - 1));
    }

    private static void parseContexts(TokenStreamCursor c,
            List<Protocol.PDataSpaceContext> out) {
        c.expect(TokenType.BRACKET_OPEN);
        while (c.peek() != TokenType.BRACKET_CLOSE && !c.atEnd()) {
            int ctxStart = c.pos();
            c.expect(TokenType.BRACE_OPEN);
            String name = null;
            String title = null;
            String description = null;
            String mapping = null;
            SourceInfo mappingSpan = null;
            Protocol.PDataSpaceMappingProvider mappingProvider = null;
            String defaultRuntime = null;
            SourceInfo runtimeSpan = null;
            Protocol.PDataSpaceTestData testData = null;
            java.util.Set<String> seenKeys3 = new java.util.HashSet<>();
            while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                int keyStart = c.pos();
                String key = c.parseIdentifier();
                TokenStreamCursor.once(seenKeys3, key, c, ctxStart);
                c.expect(TokenType.COLON);
                String providerElement = null;
                SourceInfo providerElementSpan = null;
                List<String> providerKeys = null;
                switch (key) {
                    case "name" -> name = SectionParse.stringValue(c);
                    case "title" -> title = SectionParse.stringValue(c);
                    case "description" -> description = SectionParse.stringValue(c);
                    case "mapping" -> mapping =
                            Protocol.unquotePath(c.parseQualifiedName());
                    case "mappingProvider" -> {
                        // mappingProvider: fn.k1, k2; (4.145.0) — a typeless
                        // element pointer plus keys (an empty list when none)
                        int eS = c.pos();
                        providerElement = Protocol.unquotePath(c.parseQualifiedName());
                        providerElementSpan = c.spanOf(eS, c.pos() - 1);
                        providerKeys = new ArrayList<>();
                        if (c.match(TokenType.DOT)) {
                            providerKeys.add(c.parseIdentifier());
                            while (c.match(TokenType.COMMA)) {
                                providerKeys.add(c.parseIdentifier());
                            }
                        }
                    }
                    case "defaultRuntime" -> defaultRuntime =
                            Protocol.unquotePath(c.parseQualifiedName());
                    case "testData" -> {
                        // Reference #{ path }# — the span covers the VALUE
                        // (kind through }#), no key, no semicolon
                        int vs = c.pos();
                        String kind = c.parseIdentifier();
                        if (!"Reference".equals(kind)
                                && !"DataspaceTestData".equals(kind)) {
                            // the engine's DataspaceDataElementReference
                            // parser accepts ONLY the pointer kinds —
                            // inline embedded data refuses (sibling
                            // negative neg-dataspace-testdata-embedded)
                            throw TokenStreamCursor.throwAt(c.tokens(), vs,
                                    "Unexpected token");
                        }
                        String path = rawIsland(c).trim();
                        testData = new Protocol.PDataSpaceTestData(kind,
                                path, c.spanOf(vs, c.pos() - 1));
                    }
                    default -> throw c.error(
                            "unknown executionContexts key: " + key);
                }
                c.expect(TokenType.SEMI_COLON);
                // POINTER spans cover the whole `key: value;` statement
                if ("mapping".equals(key)) {
                    mappingSpan = c.spanOf(keyStart, c.pos() - 1);
                } else if ("defaultRuntime".equals(key)) {
                    runtimeSpan = c.spanOf(keyStart, c.pos() - 1);
                } else if ("mappingProvider".equals(key)) {
                    mappingProvider = new Protocol.PDataSpaceMappingProvider(
                            java.util.Objects.requireNonNull(providerElement),
                            java.util.Objects.requireNonNull(providerElementSpan),
                            java.util.Objects.requireNonNull(providerKeys),
                            c.spanOf(keyStart, c.pos() - 1));
                }
            }
            c.expect(TokenType.BRACE_CLOSE);
            SourceInfo ctxSpan = c.spanOf(ctxStart, c.pos() - 1);
            if (name == null) {
                throw TokenStreamCursor.throwAt(c.tokens(), ctxStart,
                        "Field 'name' is required");
            }
            // engine-verbatim (DataSpaceParseTreeWalker, 4.145.0)
            if (mapping == null && mappingProvider == null) {
                throw TokenStreamCursor.throwAt(c.tokens(), ctxStart,
                        "Data space execution context must define either 'mapping' or 'mappingProvider'");
            }
            if (mapping != null && mappingProvider != null) {
                throw TokenStreamCursor.throwAt(c.tokens(), ctxStart,
                        "Data space execution context cannot define both 'mapping' and 'mappingProvider'");
            }
            out.add(new Protocol.PDataSpaceContext(name, title, description,
                    mapping, mappingSpan, mappingProvider, defaultRuntime, runtimeSpan,
                    testData, ctxSpan));
            c.match(TokenType.COMMA);
        }
        c.expect(TokenType.BRACKET_CLOSE);
        c.expect(TokenType.SEMI_COLON);
    }

    private static void parseExecutables(TokenStreamCursor c,
            List<Protocol.PDataSpaceExecutable> out) {
        c.expect(TokenType.BRACKET_OPEN);
        while (c.peek() != TokenType.BRACKET_CLOSE && !c.atEnd()) {
            int entryStart = c.pos();
            c.expect(TokenType.BRACE_OPEN);
            String id = null;
            String title = null;
            String description = null;
            String executable = null;
            SourceInfo executableSpan = null;
            com.legend.protocol.spec.ValueSpecification query = null;
            String contextKey = null;
            Protocol.PRelationElement sampleValues = null;
            java.util.Set<String> seenKeys4 = new java.util.HashSet<>();
            while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                int keyStart = c.pos();
                String key = c.parseIdentifier();
                TokenStreamCursor.once(seenKeys4, key, c, entryStart);
                c.expect(TokenType.COLON);
                switch (key) {
                    // ids appear as bare identifiers AND integers — the
                    // wire stringifies both
                    case "id" -> {
                        // ids can be digit-PREFIXED (2Id) — the engine
                        // lexes that as INTEGER + identifier and the walker
                        // concatenates (C12 TestDataSpaceGrammarParser#68)
                        StringBuilder idb = new StringBuilder();
                        while (!c.atEnd()
                                && c.peek() != TokenType.SEMI_COLON) {
                            idb.append(c.safeText());
                            c.advance();
                        }
                        id = idb.toString();
                    }
                    case "title" -> title = SectionParse.stringValue(c);
                    case "description" -> description = SectionParse.stringValue(c);
                    case "executable" -> {
                        executable = Protocol.unquotePath(c.parseQualifiedName());
                        if (c.peek() == TokenType.PAREN_OPEN) {
                            // a FUNCTION POINTER with its full signature —
                            // executable: fn():TabularDataSet[1]; — kept as
                            // written
                            executable += rawToSemicolon(c);
                        }
                    }
                    case "query" -> query = SectionParse.lambdaToSemicolon(c);
                    case "executionContextKey" -> contextKey = SectionParse.stringValue(c);
                    // sampleValues: Relation #{ ... }# (4.145.0) — ONE
                    // standalone relation element, the test-assertion reader
                    case "sampleValues" -> sampleValues =
                            com.legend.parser.RelationIslands.parseStandaloneRelationAt(c);
                    default -> throw c.error("unknown executables key: " + key);
                }
                c.expect(TokenType.SEMI_COLON);
                if ("executable".equals(key)) {
                    // the pointer span covers the whole `key: value;`
                    executableSpan = c.spanOf(keyStart, c.pos() - 1);
                }
            }
            c.expect(TokenType.BRACE_CLOSE);
            if (id == null && query != null) {
                // engine-verbatim (sectioned negative pin #66) — required
                // for the TEMPLATE form only; the pointer form
                // (executable: path) is id-less in oracle-accepted corpus
                // (dataSpaceWithExecutables.pure; the first ceiling here
                // over-tightened and refused three accepted files)
                throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), entryStart,
                        "Field 'id' is required");
            }
            if (title == null || (executable == null && query == null)) {
                throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(), entryStart,
                        "an executable needs a title and an executable"
                        + " path or query");
            }
            out.add(new Protocol.PDataSpaceExecutable(id, title, description,
                    executable, executableSpan, query, contextKey, sampleValues,
                    c.spanOf(entryStart, c.pos() - 1)));
            c.match(TokenType.COMMA);
        }
        c.expect(TokenType.BRACKET_CLOSE);
        c.expect(TokenType.SEMI_COLON);
    }

    private static void parseDiagrams(TokenStreamCursor c,
            List<Protocol.PDataSpaceDiagram> out) {
        c.expect(TokenType.BRACKET_OPEN);
        while (c.peek() != TokenType.BRACKET_CLOSE && !c.atEnd()) {
            int entryStart = c.pos();
            c.expect(TokenType.BRACE_OPEN);
            String title = null;
            String description = null;
            String diagram = null;
            SourceInfo diagramSpan = null;
            java.util.Set<String> seenKeys5 = new java.util.HashSet<>();
            while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                int keyStart = c.pos();
                String key = c.parseIdentifier();
                TokenStreamCursor.once(seenKeys5, key, c);
                c.expect(TokenType.COLON);
                switch (key) {
                    case "title" -> title = SectionParse.stringValue(c);
                    case "description" -> description = SectionParse.stringValue(c);
                    case "diagram" -> diagram =
                            Protocol.unquotePath(c.parseQualifiedName());
                    default -> throw c.error("unknown diagrams key: " + key);
                }
                c.expect(TokenType.SEMI_COLON);
                if ("diagram".equals(key)) {
                    diagramSpan = c.spanOf(keyStart, c.pos() - 1);
                }
            }
            c.expect(TokenType.BRACE_CLOSE);
            if (title == null || diagram == null || diagramSpan == null) {
                throw com.legend.parser.TokenStreamCursor.throwAt(
                        c.tokens(), entryStart,
                        "a diagram entry needs title and diagram");
            }
            out.add(new Protocol.PDataSpaceDiagram(title, description,
                    diagram, diagramSpan, c.spanOf(entryStart, c.pos() - 1)));
            c.match(TokenType.COMMA);
        }
        c.expect(TokenType.BRACKET_CLOSE);
        c.expect(TokenType.SEMI_COLON);
    }

    /** Raw token text up to (not consuming) the next top-level {@code ;} —
     *  depth-aware across ALL bracket kinds (brace-lambda queries carry
     *  inner semicolons). */
    private static String rawToSemicolon(TokenStreamCursor c) {
        int bs = c.pos();
        int d = 0;
        while (!c.atEnd()) {
            TokenType tk = c.peek();
            switch (tk) {
                case PAREN_OPEN, BRACE_OPEN, BRACKET_OPEN -> d++;
                case PAREN_CLOSE, BRACE_CLOSE, BRACKET_CLOSE -> d--;
                default -> { }
            }
            if (tk == TokenType.SEMI_COLON && d <= 0) {
                break;
            }
            c.advance();
        }
        return c.reconstructText(bs, c.pos());
    }

    /** One {@code #{ ... }#} island's raw content text. */
    private static String rawIsland(TokenStreamCursor c) {
        c.advance();                                // ISLAND_OPEN
        int bs = c.pos();
        int depth = 0;
        while (!c.atEnd()) {
            TokenType t = c.peek();
            if (t == TokenType.ISLAND_START) {
                depth++;                // a NESTED #...{ island opened
            } else if (t == TokenType.ISLAND_END) {
                if (depth == 0) {
                    break;
                }
                depth--;
            }
            c.advance();
        }
        String raw = c.reconstructText(bs, c.pos());
        c.expect(TokenType.ISLAND_END);
        return raw;
    }


}
