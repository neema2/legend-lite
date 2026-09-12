// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser;

import com.legend.lexer.TokenStream;
import com.legend.lexer.TokenType;
import com.legend.protocol.Protocol;
import com.legend.protocol.SourceInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * PROTOCOL-path {@code ###Mapping} parse (section-parity leg 4), built
 * family-by-family against ZMappingProbe's oracle shapes. Families without
 * probed wire shapes REFUSE loudly — the harness turns each refusal into a
 * named WALL row on the worklist.
 */
public final class MappingProtocolParser implements TokenStreamCursor {

    private final TokenStream tokens;
    private int pos;

    /** The three-level {@link Dialect} — carried from the
     *  constructing parser so m2-only mapping forms and body-level dialect
     *  constructs gate on the drop-in surface. */
    private final Dialect dialect;

    @Override
    public Dialect dialect() {
        return dialect;
    }

    MappingProtocolParser(TokenStream tokens, int pos,
            Dialect dialect) {
        this.tokens = tokens;
        this.pos = pos;
        this.dialect = dialect;
    }

    @Override
    public TokenStream tokens() {
        return tokens;
    }

    @Override
    public int pos() {
        return pos;
    }

    @Override
    public void setPos(int pos) {
        this.pos = pos;
    }

    /** Parse one {@code Mapping qn ( ... )} at {@code tokenIndex}. */
    public static Protocol.PMapping parse(TokenStream ts, int tokenIndex,
            Dialect dialect) {
        return parse(ts, tokenIndex, -1, null, dialect);
    }

    /** As {@link #parse(TokenStream, int, Dialect)} with the SECTION's
     *  first content line — AggregationAware nested-CM spans shift by
     *  {@code memberLine - sectionStartLine + 2} (probe agg-off-A/B). */
    public static Protocol.PMapping parse(TokenStream ts, int tokenIndex,
            int sectionStartLine, Dialect dialect) {
        return parse(ts, tokenIndex, sectionStartLine, null, dialect);
    }

    /** As above, but reports where the element ENDED in {@code endOut[0]} so
     *  a caller iterating elements can resume — the twin of
     *  {@code DatabaseProtocolParser.parse(ts, i, endOut)}, needed now that
     *  the ###Mapping model is built from protocol rather than by a second
     *  parser (PARSER_COMPLETENESS_PLAN.md §1). */
    public static Protocol.PMapping parse(TokenStream ts, int tokenIndex,
            int sectionStartLine, int @com.legend.Nullable [] endOut,
            Dialect dialect) {
        MappingProtocolParser p = new MappingProtocolParser(ts, tokenIndex,
                dialect);
        p.sectionStartLine = sectionStartLine;
        Protocol.PMapping m = p.parseMapping();
        if (endOut != null) {
            endOut[0] = p.pos;
        }
        return m;
    }

    /** -1 = unknown (AggregationAware members then refuse loudly). */
    private int sectionStartLine = -1;

    /** Lines the CURRENT ~modelOperation block's content shifts UP —
     *  braceLine minus tildeLine (engine anchor quirk). */
    private int aggLambdaShift = 0;

    /** Parse one {@code Data [decorations] qn { <body> }} at
     *  {@code tokenIndex} (probe data-section). */
    public static Protocol.PDataElement parseData(TokenStream ts,
            int tokenIndex, Dialect dialect) {
        return parseData(ts, tokenIndex, dialect, null);
    }

    /** As above, reporting where the element ENDED (the strict document
     *  walk resumes there). */
    public static Protocol.PDataElement parseData(TokenStream ts,
            int tokenIndex, Dialect dialect,
            int @com.legend.Nullable [] endOut) {
        MappingProtocolParser p =
                new MappingProtocolParser(ts, tokenIndex, dialect);
        Protocol.PDataElement d = p.parseDataElement();
        if (endOut != null) {
            endOut[0] = p.pos;
        }
        return d;
    }

    private Protocol.PDataElement parseDataElement() {
        int declStart = pos;
        // dataElement: documentation? DATA stereotypes? taggedValues? ...
        Documentation doc = parseDocumentation();
        expectText("Data");
        Decorations dec = parseDecorations();
        List<Protocol.PTaggedValue> taggedValues =
                taggedValuesWithDocumentation(doc, dec.taggedValues());
        String qn = Protocol.unquotePath(parseQualifiedName());
        int cut = qn.lastIndexOf("::");
        String pkg = cut < 0 ? "" : qn.substring(0, cut);
        String name = cut < 0 ? qn : qn.substring(cut + 2);
        expect(TokenType.BRACE_OPEN);
        // engine wire: an optional VALUE plus optional RESOLVER entries —
        // `<Kind> #{...}#`, `store::S: <value>;` (base) and bare `fqn;`
        // (reference) mix freely (harvest testDataElementWith*Resolvers)
        Protocol.PEmbeddedDataValue value = null;
        List<Protocol.PDataResolver> resolvers = new ArrayList<>();
        while (!atEnd() && peek() != TokenType.BRACE_CLOSE) {
            int term = fqnTerminator();
            if (term == 1) {                        // fqn ':' — base resolver
                resolvers.add(parseDataResolver());
            } else if (term == 2) {                 // fqn ';' — reference
                int rS = pos;
                String path = Protocol.unquotePath(parseQualifiedName());
                SourceInfo rSpan = spanOf(rS, pos - 1);
                expect(TokenType.SEMI_COLON);
                resolvers.add(new Protocol.PDataResolver(
                        new Protocol.PElementRef(path, rSpan), null, rSpan));
            } else {
                if (value != null) {
                    throw error("a Data element carries ONE embedded value");
                }
                value = parseEmbeddedValue();
            }
        }
        Protocol.PDataBody body = new Protocol.PDataBody(value, resolvers);
        int close = pos;
        expect(TokenType.BRACE_CLOSE);
        return new Protocol.PDataElement(pkg, name, body, dec.stereotypes(),
                taggedValues, spanOf(declStart, close));
    }

    /** Body-entry lookahead: 1 = qualified name then {@code ':'} (base
     *  resolver), 2 = qualified name then {@code ';'} (reference resolver),
     *  0 = anything else (an embedded value). Structural, not a keyword
     *  list — store paths routinely open on lexer KEYWORDS. */
    private int fqnTerminator() {
        int p = pos;
        while (p < tokens.count()
                && (isFqnSegmentToken(tokens.type(p))
                        || tokens.type(p) == TokenType.PATH_SEPARATOR)) {
            p++;
        }
        if (p == pos || p >= tokens.count()) {
            return 0;
        }
        if (tokens.type(p) == TokenType.COLON) {
            return 1;
        }
        return tokens.type(p) == TokenType.SEMI_COLON ? 2 : 0;
    }

    /** {@code store::S: <value>;} — the resolver's span runs the store
     *  path through the value's close, EXCLUDING the trailing {@code ';'}
     *  (probe store-keyed-multi). */
    private Protocol.PDataResolver parseDataResolver() {
        int pS = pos;
        String store = Protocol.unquotePath(parseQualifiedName());
        SourceInfo storeSpan = spanOf(pS, pos - 1);
        expect(TokenType.COLON);
        Protocol.PEmbeddedDataValue value = parseEmbeddedValue();
        SourceInfo vSpan = embeddedSpan(value);
        expect(TokenType.SEMI_COLON);
        return new Protocol.PDataResolver(
                new Protocol.PElementRef(store, storeSpan), value,
                new SourceInfo("", storeSpan.startLine(),
                        storeSpan.startColumn(), vSpan.endLine(),
                        vSpan.endColumn()));
    }

    /** ONE embedded-data value grammar — {@code ExternalFormat},
     *  {@code Reference}, {@code Relation}, {@code Relational} and
     *  {@code ModelStore} islands, shared by every site that admits an
     *  EmbeddedData (###Data bodies, resolvers, ModelStore entries). */
    /** One embedded-data VALUE ({@code Kind #{...}#} / reference) parsed
     *  off a HOST cursor — the service test-suite data seam. */
    public static Protocol.PEmbeddedDataValue parseEmbeddedValueAt(
            com.legend.parser.TokenStreamCursor host) {
        MappingProtocolParser p = new MappingProtocolParser(host.tokens(),
                host.pos(), host.dialect());
        Protocol.PEmbeddedDataValue v = p.parseEmbeddedValue();
        host.setPos(p.pos());
        return v;
    }


    Protocol.PEmbeddedDataValue parseEmbeddedValue() {
        // 'Relational' is a LEXER KEYWORD, the rest lex as identifiers
        if (peek() == TokenType.RELATIONAL) {
            return parseRelationalCsv();
        }
        String kind = text();
        if ("ExternalFormat".equals(kind)) {
            return parseExternalFormat();
        }
        if ("Reference".equals(kind) || "DataspaceTestData".equals(kind)) {
            // DataspaceTestData references a DATASPACE, Reference a DATA
            // element — same wire shape, different pointer type
            int rTok = pos;
            advance();
            IslandBlock ri = readIsland();
            MappingProtocolParser rp = new MappingProtocolParser(
                    ri.tokens(), 0, dialect);
            String dataPath = Protocol.unquotePath(rp.parseQualifiedName());
            SourceInfo si = new SourceInfo("", tokens.startLine(rTok),
                    tokens.startColumn(rTok), ri.endLine(), ri.endColumn());
            return new Protocol.PDataReference(
                    new Protocol.PPointer("Reference".equals(kind) ? "DATA"
                            : "DATASPACE", dataPath, si), si);
        }
        if ("Relation".equals(kind)) {
            advance();
            IslandBlock ri = readIsland();
            List<Protocol.PRelationElement> rels =
                    parseRelationElements(ri, true);
            // the accessor span starts at the FIRST element, not the
            // keyword, and its end column overshoots the island close by 3
            // (the engine's relation-data walker offset — probe relation-csv)
            SourceInfo first = rels.isEmpty()
                    ? spanOf(pos - 1, pos - 1)
                    : rels.get(0).sourceInformation();
            return new Protocol.PRelationData(rels,
                    new SourceInfo("", first.startLine(), first.startColumn(),
                            ri.endLine(), ri.endColumn() + 3));
        }
        if ("ModelStore".equals(kind)) {
            int msTok = pos;
            advance();
            IslandBlock island = readIsland();
            List<Protocol.PModelData> modelData = new ArrayList<>();
            MappingProtocolParser inner = new MappingProtocolParser(
                    island.tokens(), 0, dialect);
            while (!inner.atEnd()) {
                Protocol.PModelData entry = inner.parseModelEntry(island);
                requireUniqueModelType(modelData, entry);
                modelData.add(entry);
                inner.match(TokenType.COMMA);
            }
            return new Protocol.PModelStoreData(modelData,
                    new SourceInfo("", tokens.startLine(msTok),
                            tokens.startColumn(msTok), island.endLine(),
                            island.endColumn()));
        }
        if ("ServiceStore".equals(kind)) {
            // ServiceStore #{ [ { request: {...}; response: {...}; } ] }# —
            // _type:"serviceStore" (ZTailProbe "servicestore-data")
            int kTok = pos;
            advance();
            IslandBlock si = readIsland();
            MappingProtocolParser inner = new MappingProtocolParser(
                    si.tokens(), 0, dialect);
            List<Protocol.PServiceStub> stubs = new ArrayList<>();
            inner.expect(TokenType.BRACKET_OPEN);
            while (inner.peek() != TokenType.BRACKET_CLOSE && !inner.atEnd()) {
                stubs.add(ServiceStubDataParser.parseServiceStub(inner));
                inner.match(TokenType.COMMA);
            }
            inner.expect(TokenType.BRACKET_CLOSE);
            return new Protocol.PServiceStoreData(stubs,
                    new SourceInfo("", tokens.startLine(kTok),
                            tokens.startColumn(kTok), si.endLine(),
                            si.endColumn()));
        }
        throw error("embedded data kind '" + safeText() + "' is unbuilt");
    }

    /** {@code Relational #{ schema.table: 'csv' + 'csv'; ... }#} — the path
     *  is exactly TWO segments and the value is the escape-decoded
     *  concatenation of its literals; each table's span runs the schema
     *  token through the terminating {@code ';'} (probe relational-csv). */
    private Protocol.PRelationalCsvData parseRelationalCsv() {
        int kwTok = pos;
        expect(TokenType.RELATIONAL);
        IslandBlock island = readIsland();
        List<Protocol.PRelationalCsvTable> tables = new ArrayList<>();
        MappingProtocolParser in = new MappingProtocolParser(
                island.tokens(), 0, dialect);
        while (!in.atEnd()) {
            int tS = in.pos;
            String schema = Protocol.unquotePath(in.parseIdentifier());
            in.expect(TokenType.DOT);
            String table = Protocol.unquotePath(in.parseIdentifier());
            in.expect(TokenType.COLON);
            StringBuilder values = new StringBuilder();
            while (true) {
                String quoted = in.text();
                in.expect(TokenType.STRING);
                values.append(TokenStreamCursor.unquoteAndUnescape(quoted, in));
                if (!in.match(TokenType.PLUS)) {
                    break;
                }
            }
            int semi = in.pos;
            in.expect(TokenType.SEMI_COLON);
            tables.add(new Protocol.PRelationalCsvTable(schema, table,
                    values.toString(), in.spanOf(tS, semi)));
        }
        return new Protocol.PRelationalCsvData(tables,
                new SourceInfo("", tokens.startLine(kwTok),
                        tokens.startColumn(kwTok), island.endLine(),
                        island.endColumn()));
    }

    /** The span an embedded value contributes to its enclosing entry. */
    private static SourceInfo embeddedSpan(Protocol.PEmbeddedDataValue v) {
        return switch (v) {
            case Protocol.PExternalFormatData e -> e.sourceInformation();
            case Protocol.PDataReference r -> r.sourceInformation();
            case Protocol.PModelStoreData m -> m.sourceInformation();
            case Protocol.PRelationalCsvData c -> c.sourceInformation();
            case Protocol.PServiceStoreData ss -> ss.sourceInformation();
            // the accessor's own end overshoots by the walker offset; a
            // resolver anchors on the RAW island close (probe store-keyed)
            case Protocol.PRelationData d -> new SourceInfo("",
                    d.sourceInformation().startLine(),
                    d.sourceInformation().startColumn(),
                    d.sourceInformation().endLine(),
                    d.sourceInformation().endColumn() - 3);
        };
    }

    private Protocol.PMapping parseMapping() {
        int declStart = pos;
        expect(TokenType.MAPPING);
        String qn = Protocol.unquotePath(parseQualifiedName());
        int cut = qn.lastIndexOf("::");
        String pkg = cut < 0 ? "" : qn.substring(0, cut);
        String name = cut < 0 ? qn : qn.substring(cut + 2);
        expect(TokenType.PAREN_OPEN);
        List<Protocol.PAssociationMapping> assocMappings = new ArrayList<>();
        List<Protocol.PClassMapping> classMappings = new ArrayList<>();
        List<Protocol.PEnumerationMapping> enums = new ArrayList<>();
        List<Protocol.PMappingInclude> includes = new ArrayList<>();
        List<Protocol.PMappingTestSuite> suites = new ArrayList<>();
        String testSuitesSource = null;
        List<Protocol.PLegacyMappingTest> legacyTests = new ArrayList<>();
        // The mapping BODY is an ordered prefix too:
        //   mapping: MAPPING qualifiedName PAREN_OPEN (includeMapping)*
        //            (mappingElement)* (tests)? (mappingTestableDefinition)?
        //            PAREN_CLOSE
        // (MappingParserGrammar.g4:33-40). Includes and elements repeat;
        // MappingTests and testSuites appear at most once and only at the
        // end. Adjudicated: an element before an include, an element after
        // testSuites, MappingTests after testSuites and a repeat of either
        // all REJECT (ZM4FixtureOracle4).
        int bodyRank = 0;
        while (!atEnd() && peek() != TokenType.PAREN_CLOSE) {
            if (peek() == TokenType.VALID_STRING
                    && "MappingTests".equals(text())) {
                bodyRank = bodySection(bodyRank, BODY_LEGACY_TESTS,
                        "MappingTests");
                advance();
                expect(TokenType.BRACKET_OPEN);
                while (!atEnd() && peek() != TokenType.BRACKET_CLOSE) {
                    legacyTests.add(parseLegacyTest(qn));
                    match(TokenType.COMMA);
                }
                expect(TokenType.BRACKET_CLOSE);
                continue;
            }
            if (peek() == TokenType.MAPPING_TESTABLE_SUITES) {
                bodyRank = bodySection(bodyRank, BODY_TEST_SUITES,
                        "testSuites");
                advance();
                expect(TokenType.COLON);
                int suitesOpen = pos;
                expect(TokenType.BRACKET_OPEN);
                while (!atEnd() && peek() != TokenType.BRACKET_CLOSE) {
                    suites.add(parseTestSuite(qn));
                    match(TokenType.COMMA);
                }
                expect(TokenType.BRACKET_CLOSE);
                // The MODEL stores testSuites as RAW TEXT (Phase C parses
                // them lazily), and parsed protocol cannot be turned back
                // into source. Capture the verbatim slice here, over the
                // same span the legacy parser reconstructs
                // (MappingGrammarParser:83-88). Carried on the record, never
                // emitted — the wire has the PARSED suites.
                testSuitesSource = tokens.source().substring(
                        tokens.start(suitesOpen), tokens.end(pos - 1));
                continue;
            }
            bodyRank = bodySection(bodyRank,
                    peek() == TokenType.INCLUDE ? BODY_INCLUDE : BODY_ELEMENT,
                    peek() == TokenType.INCLUDE ? "include" : "a mapping element");
            parseMember(assocMappings, classMappings, enums, includes);
        }
        expect(TokenType.PAREN_CLOSE);
        return new Protocol.PMapping(pkg, name, assocMappings, classMappings,
                enums, includes, suites, legacyTests, testSuitesSource,
                spanOf(declStart, pos - 1));
    }

    private void parseMember(List<Protocol.PAssociationMapping> assocMappings,
            List<Protocol.PClassMapping> classMappings,
            List<Protocol.PEnumerationMapping> enums,
            List<Protocol.PMappingInclude> includes) {
        if (peek() == TokenType.INCLUDE) {
            int s = pos;
            advance();
            if (peek() == TokenType.SERVICE_MAPPING
                    && peek(1)
                            != TokenType.PATH_SEPARATOR) {
                advance();                          // 'include mapping qn'
            } else if (peek() == TokenType.VALID_STRING
                    && "dataspace".equals(text())) {
                // include dataspace my::DS — _type mappingIncludeDataSpace
                // (ZTailProbe "include-dataspace")
                advance();
                String ds = Protocol.unquotePath(parseQualifiedName());
                includes.add(new Protocol.PMappingInclude(null, ds, null,
                        null, List.of(), spanOf(s, pos - 1)));
                return;
            }
            String inc = Protocol.unquotePath(parseQualifiedName());
            String srcDb = null;
            String tgtDb = null;
            List<Protocol.PStoreSubstitution> subs = new ArrayList<>();
            if (peek() == TokenType.BRACKET_OPEN) {
                // include m[srcDb->tgtDb, ...] — MULTIPLE substitutions
                // emit NEITHER key (probe include-multi-subst)
                advance();
                while (peek() != TokenType.BRACKET_CLOSE && !atEnd()) {
                    srcDb = Protocol.unquotePath(parseQualifiedName());
                    expect(TokenType.ARROW);
                    tgtDb = Protocol.unquotePath(parseQualifiedName());
                    match(TokenType.COMMA);
                    subs.add(new Protocol.PStoreSubstitution(srcDb, tgtDb));
                }
                expect(TokenType.BRACKET_CLOSE);
                if (subs.size() > 1) {
                    // engine records NEITHER path when there is more than one
                    // pair (CorePureGrammarParser:504-516). The emitted keys
                    // follow it exactly; `subs` keeps what engine throws away
                    // so the MODEL does not lose the substitution.
                    srcDb = null;
                    tgtDb = null;
                }
            }
            includes.add(new Protocol.PMappingInclude(inc, srcDb, tgtDb, subs,
                    spanOf(s, pos - 1)));
            return;
        }
        int memberStart = pos;
        boolean root = match(TokenType.STAR);       // root marker
        int targetStart = pos;
        String target = Protocol.unquotePath(parseQualifiedName());
        SourceInfo targetSpan = spanOf(targetStart, pos - 1);
        String id = null;
        if (peek() == TokenType.BRACKET_OPEN) {
            advance();                              // [setId]
            id = parseSetId();
            expect(TokenType.BRACKET_CLOSE);
        }
        String extendsId = null;
        if (peek() == TokenType.EXTENDS) {
            advance();                              // extends [otherId]
            expect(TokenType.BRACKET_OPEN);
            extendsId = parseIdentifier();
            expect(TokenType.BRACKET_CLOSE);
        }
        expect(TokenType.COLON);
        String kind = safeText();
        if (peek() == TokenType.ENUMERATION_MAPPING) {
            advance();
            // the enumeration mapping's span starts at the root marker when one
            // is written (`*model::S: EnumerationMapping …` — the engine's span
            // begins at the `*`; batch 6 own-corpus differential)
            enums.add(parseEnumerationMapping(target, memberStart, targetSpan));
            return;
        }
        if (peek() == TokenType.ASSOCIATION_MAPPING && cleanSheetAheadAt(1)) {
            advance();
            assocMappings.add(parseFunctionAssociationMapping(target,
                    memberStart, targetSpan));
            return;
        }
        if (peek() == TokenType.RELATIONAL) {
            advance();
            if (peekIsAssociationBody()) {
                assocMappings.add(parseRelAssociationMapping(target,
                        memberStart, targetSpan, id));
                return;
            }
            if (peek(1) != TokenType.BRACE_OPEN && cleanSheetAhead()) {
                classMappings.add(parseFunctionClassMapping(target, memberStart,
                        targetSpan, id, extendsId, root, "RELATIONAL"));
                return;
            }
            classMappings.add(parseRelationalClassMapping(target, memberStart,
                    targetSpan, id, root, extendsId));
            return;
        }
        if (peek() == TokenType.VALID_STRING && "XStore".equals(text())) {
            advance();
            assocMappings.add(parseXStoreAssociationMapping(target,
                    memberStart, targetSpan, id));
            return;
        }
        if (peek() == TokenType.PURE_MAPPING) {
            advance();
            if (cleanSheetAhead()) {
                classMappings.add(parseFunctionClassMapping(target, memberStart,
                        targetSpan, id, extendsId, root, "PURE"));
                return;
            }
            classMappings.add(parsePureClassMapping(target, memberStart,
                    targetSpan, id, root, extendsId));
            return;
        }
        if (peek() == TokenType.VALID_STRING
                && "AggregationAware".equals(text())) {
            advance();
            classMappings.add(parseAggregationAware(target, memberStart,
                    targetSpan, id, root));
            return;
        }
        if (peek() == TokenType.VALID_STRING && "ModelJoin".equals(text())) {
            advance();
            assocMappings.add(parseModelJoin(target, memberStart, targetSpan,
                    id));
            return;
        }
        if (peek() == TokenType.VALID_STRING && "Relation".equals(text())) {
            advance();
            classMappings.add(parseRelationClassMapping(target, memberStart,
                    targetSpan, id, extendsId, root));
            return;
        }
        if (peek() == TokenType.VALID_STRING && "Operation".equals(text())) {
            advance();
            // The engine PARSES but DROPS extends on Operation mappings
            // (OperationClassMappingParseTreeWalker TODO), so it is NOT
            // emitted — see MappingEmitter's operation arm. legend-lite's
            // model has always kept it, so it rides the protocol record
            // WITHOUT going on the wire, exactly like a multi-pair include
            // substitution. Byte parity untouched, capability kept.
            classMappings.add(parseOperationClassMapping(target, memberStart,
                    targetSpan, id, extendsId, root));
            return;
        }
        if (peek() == TokenType.VALID_STRING && "ServiceStore".equals(kind)) {
            if (extendsId != null) {
                // engine-verbatim (sectioned negative pin,
                // TestServiceStoreMappingGrammarParser#4)
                throw error("Service Store Mapping does not support extends");
            }
            advance();
            classMappings.add(parseServiceStoreClassMapping(target,
                    memberStart, targetSpan, id, root));
            return;
        }
        if (peek() == TokenType.VALID_STRING && "MongoDB".equals(kind)) {
            advance();
            classMappings.add(parseMongoDbClassMapping(target, id, root));
            return;
        }
        if (extendsId != null) {
            throw error("'extends' on this class-mapping kind is unbuilt");
        }
        throw error("unsupported class mapping type: '" + kind + "'");
    }

    /**
     * {@code *Class[id]: ServiceStore { ~service [store] G.S (~request
     * (...))* }} — structured to the wire (ZTailProbe probes): dotted
     * segments keep per-segment spans, parameter/body transforms are
     * SPEC-parsed expressions whose lambda wrapper spans the expression.
     */
    private Protocol.PServiceStoreClassMapping parseServiceStoreClassMapping(
            String target, int memberStart, SourceInfo targetSpan,
            @com.legend.Nullable String id, boolean root) {
        expect(TokenType.BRACE_OPEN);
        List<Protocol.PServiceStoreLocalProp> localProps = new ArrayList<>();
        List<Protocol.PServiceMapping> services = new ArrayList<>();
        while (!atEnd() && peek() != TokenType.BRACE_CLOSE) {
            if (peek() == TokenType.PLUS) {
                // +name: Type[mult]; — a LOCAL mapping property
                int lpStart = pos;
                advance();
                String lpName = parseIdentifier();
                expect(TokenType.COLON);
                String lpType = parseIdentifier();
                // parseMultiplicity consumes its own brackets
                var mult = (com.legend.protocol.Multiplicity.Concrete)
                        parseMultiplicity();
                expect(TokenType.SEMI_COLON);
                // the span INCLUDES the ';' (corpus DIFF pinned it)
                localProps.add(new Protocol.PServiceStoreLocalProp(lpName,
                        lpType, mult.lowerBound(), mult.upperBound(),
                        spanOf(lpStart, pos - 1)));
                continue;
            }
            int svcStart = pos;
            expect(TokenType.TILDE);
            String kw = parseIdentifier();
            if (!"service".equals(kw)) {
                throw error("unknown ServiceStore mapping directive: ~" + kw);
            }
            expect(TokenType.BRACKET_OPEN);
            String store = Protocol.unquotePath(parseQualifiedName());
            expect(TokenType.BRACKET_CLOSE);
            List<Protocol.PServiceSegment> segments = new ArrayList<>();
            while (true) {
                int segStart = pos;
                String seg = parseIdentifier();
                segments.add(new Protocol.PServiceSegment(seg,
                        spanOf(segStart, pos - 1)));
                if (!match(TokenType.DOT)) {
                    break;
                }
            }
            Protocol.PPathOffset pathOffset = null;
            Protocol.PRequestBuildInfo request = null;
            if (peek() == TokenType.PAREN_OPEN) {
                advance();
                while (peek() == TokenType.TILDE
                        && "path".equals(peekText(1))) {
                    // ~path $service.response.a.b — NO spans on the wire
                    advance();
                    parseIdentifier();              // 'path'
                    expect(TokenType.DOLLAR);
                    String v = parseIdentifier();
                    if (!"service".equals(v)) {
                        throw error("~path must start at $service");
                    }
                    expect(TokenType.DOT);
                    String r = parseIdentifier();
                    if (!"response".equals(r)) {
                        throw error("~path must start at $service.response");
                    }
                    List<String> segs = new ArrayList<>();
                    while (match(TokenType.DOT)) {
                        segs.add(parseIdentifier());
                    }
                    pathOffset = new Protocol.PPathOffset("$service.response",
                            segs);
                }
                if (peek() == TokenType.PAREN_CLOSE) {
                    // a (~path ...) block with no ~request
                    int outerEnd0 = pos;
                    advance();
                    services.add(new Protocol.PServiceMapping(
                            new Protocol.PServicePtr(store, segments),
                            pathOffset, null, spanOf(svcStart, outerEnd0)));
                    continue;
                }
                if (peek() == TokenType.TILDE
                        && !"request".equals(peekText(1))) {
                    // the LEGACY blocks (grammar: "TO BE REMOVED"):
                    // ~paramMapping(...) and/or a bare elementMapping list
                    services.add(ServiceLegacyMappingParser.parseTail(this,
                            store, segments, pathOffset, null, svcStart));
                    continue;
                }
                if (peek() != TokenType.TILDE) {
                    // bare mappingBlock with no ~request/~paramMapping
                    services.add(ServiceLegacyMappingParser.parseTail(this,
                            store, segments, pathOffset, null, svcStart));
                    continue;
                }
                int reqStart = pos;
                expect(TokenType.TILDE);
                String rkw = parseIdentifier();
                if (!"request".equals(rkw)) {
                    throw error("unknown service-mapping block: ~" + rkw);
                }
                expect(TokenType.PAREN_OPEN);
                Protocol.PParametersBuildInfo params = null;
                Protocol.PBodyBuildInfo body = null;
                while (!atEnd() && peek() != TokenType.PAREN_CLOSE) {
                    int keyStart = pos;
                    String key = parseIdentifier();
                    if ("parameters".equals(key)) {
                        expect(TokenType.PAREN_OPEN);
                        List<Protocol.PParameterBuildInfo> entries =
                                new ArrayList<>();
                        while (!atEnd() && peek() != TokenType.PAREN_CLOSE) {
                            int eStart = pos;
                            String pname;
                            if (peek() == TokenType.QUOTED_STRING) {
                                // "q param" = ... — a quoted parameter name
                                pname = TokenStreamCursor
                                        .stripDoubleQuotes(text());
                                advance();
                            } else {
                                pname = parseIdentifier();
                            }
                            expect(TokenType.EQUAL);
                            int exprStart = pos;
                            var expr = parseTransformExpr();
                            entries.add(new Protocol.PParameterBuildInfo(pname,
                                    expr, spanOf(exprStart, pos - 1),
                                    spanOf(eStart, pos - 1)));
                            match(TokenType.COMMA);
                        }
                        int pEnd = pos;
                        expect(TokenType.PAREN_CLOSE);
                        params = new Protocol.PParametersBuildInfo(entries,
                                spanOf(keyStart, pEnd));
                    } else if ("body".equals(key)) {
                        expect(TokenType.EQUAL);
                        int exprStart = pos;
                        var expr = parseTransformExpr();
                        body = new Protocol.PBodyBuildInfo(expr,
                                spanOf(exprStart, pos - 1),
                                spanOf(keyStart, pos - 1));
                    } else {
                        throw TokenStreamCursor.throwAt(tokens, keyStart,
                                "unknown ~request key: " + key);
                    }
                }
                int reqEnd = pos;
                expect(TokenType.PAREN_CLOSE);
                request = new Protocol.PRequestBuildInfo(params, body,
                        spanOf(reqStart, reqEnd));
                if (peek() != TokenType.PAREN_CLOSE) {
                    // legacy blocks after a ~request — the walker REPLACES
                    // the request's parameters with the legacy list
                    services.add(ServiceLegacyMappingParser.parseTail(this,
                            store, segments, pathOffset, request, svcStart));
                    continue;
                }
                int outerEnd = pos;
                expect(TokenType.PAREN_CLOSE);
                services.add(new Protocol.PServiceMapping(
                        new Protocol.PServicePtr(store, segments), pathOffset,
                        request, spanOf(svcStart, outerEnd)));
            } else {
                services.add(new Protocol.PServiceMapping(
                        new Protocol.PServicePtr(store, segments), null, null,
                        spanOf(svcStart, pos - 1)));
            }
        }
        expect(TokenType.BRACE_CLOSE);
        return new Protocol.PServiceStoreClassMapping(target, targetSpan, id,
                root, localProps, services, spanOf(memberStart, pos - 1));
    }

    /** One transform expression up to the next top-level {@code ,} or
     *  {@code )} — spec-parsed on a slice of the SHARED stream so spans
     *  stay file-absolute. */
    com.legend.protocol.spec.ValueSpecification parseTransformExpr() {
        int bs = pos;
        int d = 0;
        while (!atEnd()) {
            TokenType t = peek();
            if (t == TokenType.PAREN_OPEN || t == TokenType.BRACKET_OPEN
                    || t == TokenType.BRACE_OPEN) {
                d++;
            } else if (t == TokenType.PAREN_CLOSE
                    || t == TokenType.BRACKET_CLOSE
                    || t == TokenType.BRACE_CLOSE) {
                if (d == 0) {
                    break;
                }
                d--;
            } else if (t == TokenType.COMMA && d == 0) {
                break;
            }
            advance();
        }
        return com.legend.parser.SpecParser.parse(tokens.slice(bs, pos), dialect);
    }

    /** {@code *Class[id]: MongoDB { ~mainCollection [db] Coll }} — the wire
     *  carries NO spans (ZTailProbe "mongodb-mapping"). */
    private Protocol.PClassMappingMongoDb parseMongoDbClassMapping(
            String target, @com.legend.Nullable String id, boolean root) {
        expect(TokenType.BRACE_OPEN);
        String store = null;
        String coll = null;
        String binding = null;
        while (!atEnd() && (peek() == TokenType.TILDE
                || peek() == TokenType.FILTER_CMD)) {
            String kw;
            if (peek() == TokenType.FILTER_CMD) {
                // '~filter' fuses in the lexer
                advance();
                kw = "filter";
            } else {
                advance();
                kw = parseIdentifier();
            }
            switch (kw) {
                case "mainCollection" -> {
                    expect(TokenType.BRACKET_OPEN);
                    store = Protocol.unquotePath(parseQualifiedName());
                    expect(TokenType.BRACKET_CLOSE);
                    coll = mongoName();
                }
                case "filter" -> {
                    // ~filter [store] name — parsed and DROPPED: not on
                    // the wire at all (probe t2-mongodb 2026-08-14)
                    expect(TokenType.BRACKET_OPEN);
                    Protocol.unquotePath(parseQualifiedName());
                    expect(TokenType.BRACKET_CLOSE);
                    mongoName();
                }
                case "binding" ->
                        binding = Protocol.unquotePath(parseQualifiedName());
                default -> throw error(
                        "unknown MongoDB mapping directive: ~" + kw);
            }
        }
        expect(TokenType.BRACE_CLOSE);
        if (store == null || coll == null) {
            throw error("MongoDB mapping needs ~mainCollection");
        }
        return new Protocol.PClassMappingMongoDb(target, id, root, store,
                coll, binding);
    }

    /** A Mongo collection/filter NAME — plain identifier or a
     *  {@code ~}-spelled keyword ({@code ~distinct}/{@code ~primaryKey}
     *  are identifier-only-reachable; the wire keeps the tilde
     *  verbatim, probe t2-mongodb mainCollectionName "~distinct"). */
    private String mongoName() {
        if (peek() == TokenType.TILDE) {
            advance();
            return "~" + parseIdentifier();
        }
        if (!atEnd() && text().startsWith("~")) {
            String t = text();
            advance();
            return t;
        }
        return parseIdentifier();
    }

    /** As {@link #cleanSheetAhead()} but the kind keyword is still
     *  {@code ahead} tokens back from the body's brace. */
    private boolean cleanSheetAheadAt(int ahead) {
        int save = pos;
        for (int i = 0; i < ahead; i++) {
            advance();
        }
        boolean clean = cleanSheetAhead();
        pos = save;
        return clean;
    }

    /** {@code assoc: AssociationMapping { acme::funcs::personFirmMatch }} —
     *  the clean-sheet association binding. */
    private Protocol.PFunctionAssociationMapping parseFunctionAssociationMapping(
            String target, int memberStart, SourceInfo targetSpan) {
        expect(TokenType.BRACE_OPEN);
        int bodyStart = pos;
        int depth = 1;
        while (!atEnd() && depth > 0) {
            TokenType t = peek();
            if (t == TokenType.BRACE_OPEN) {
                depth++;
            } else if (t == TokenType.BRACE_CLOSE) {
                depth--;
            }
            if (depth > 0) {
                advance();
            }
        }
        List<com.legend.protocol.spec.ValueSpecification> body =
                SpecParser.parseCodeBlock(tokens.slice(bodyStart, pos), dialect);
        int close = pos;
        expect(TokenType.BRACE_CLOSE);
        if (body.isEmpty()) {
            throw error("clean-sheet association binding for '" + target
                    + "' has an empty body");
        }
        com.legend.protocol.spec.PackageableElementPtr fnRef = null;
        com.legend.protocol.spec.LambdaFunction lambda = null;
        if (body.size() == 1
                && body.get(0) instanceof com.legend.protocol.spec.PackageableElementPtr ptr) {
            fnRef = ptr;
        } else {
            lambda = new com.legend.protocol.spec.LambdaFunction(
                    List.of(), body, null);
        }
        return new Protocol.PFunctionAssociationMapping(
                new Protocol.PPointer("ASSOCIATION", target, targetSpan),
                fnRef, lambda, spanOf(memberStart, close));
    }

    /** The kind keyword has been consumed and the next token is the body's
     *  opening brace; peek PAST it to ask the shared cursor whether this is
     *  a clean-sheet body. */
    private boolean cleanSheetAhead() {
        if (dialect.refusesLiteExtensions()) {
            // the clean-sheet forms (mapping-as-function, inline
            // association — OWN_CORPUS_DECISIONS §6/§8) are DECLARED lite
            // extensions: the exact-engine surface never detects them, so
            // the engine-shaped parse (and its engine-shaped refusals)
            // takes over
            return false;
        }
        if (peek() != TokenType.BRACE_OPEN) {
            return false;
        }
        int save = pos;
        advance();
        boolean clean = isCleanSheetBody();
        pos = save;
        return clean;
    }

    /**
     * {@code *cls[id] extends [p]: Relational { acme::funcs::personMapping }}
     * — the CLEAN-SHEET function form. The body is parsed by the ordinary
     * {@code SpecParser}, the same entry point every ###Pure function body
     * goes through, and a body that is a single packageable-element pointer
     * collapses to a function REFERENCE exactly as
     * {@code ElementParser.realizationOf} decides it.
     */
    private Protocol.PClassMappingFunction parseFunctionClassMapping(
            String target, int memberStart, SourceInfo targetSpan,
            @com.legend.Nullable String id,
            @com.legend.Nullable String extendsId, boolean root, String kind) {
        expect(TokenType.BRACE_OPEN);
        int bodyStart = pos;
        int depth = 1;
        while (!atEnd() && depth > 0) {
            TokenType t = peek();
            if (t == TokenType.BRACE_OPEN) {
                depth++;
            } else if (t == TokenType.BRACE_CLOSE) {
                depth--;
            }
            if (depth > 0) {
                advance();
            }
        }
        List<com.legend.protocol.spec.ValueSpecification> body =
                SpecParser.parseCodeBlock(tokens.slice(bodyStart, pos), dialect);
        int close = pos;
        expect(TokenType.BRACE_CLOSE);
        if (body.isEmpty()) {
            throw error("clean-sheet mapping binding for '" + target
                    + "' has an empty body");
        }
        com.legend.protocol.spec.PackageableElementPtr fnRef = null;
        com.legend.protocol.spec.LambdaFunction lambda = null;
        if (body.size() == 1
                && body.get(0) instanceof com.legend.protocol.spec.PackageableElementPtr ptr) {
            fnRef = ptr;
        } else {
            lambda = new com.legend.protocol.spec.LambdaFunction(
                    List.of(), body, null);
        }
        return new Protocol.PClassMappingFunction(target, targetSpan, id,
                extendsId, root, kind, fnRef, lambda,
                spanOf(memberStart, close));
    }

    // A relational class mapping's ~clauses are a FIXED, STRICTLY ASCENDING
    // prefix, each at most once, all before the property list —
    // `RelationalParserGrammar.g4:249` spells it out as
    //     classMapping: mappingFilter? DISTINCT_CMD? mappingGroupBy?
    //                   mappingPrimaryKey? mappingMainTable?
    //                   (propertyMapping (COMMA propertyMapping)*)? EOF
    // so ANTLR refuses a repeat and a swap alike, and refuses any ~clause
    // after a property line. Adjudicated against the real engine parser, not
    // read off the grammar alone (ZM4FixtureOracle: five REJECT verdicts).
    private static final int RANK_FILTER = 1;
    private static final int RANK_DISTINCT = 2;
    private static final int RANK_GROUP_BY = 3;
    private static final int RANK_PRIMARY_KEY = 4;
    private static final int RANK_MAIN_TABLE = 5;
    private static final int RANK_PROPERTIES = 6;

    /**
     * The optional {@code EnumerationMapping <id>:} head of a property
     * mapping, or {@code null} when the property line has none.
     *
     * <p>The id is NOT optional once the keyword is written. Both grammars
     * spell the head the same way —
     * {@code (ENUMERATION_MAPPING identifier COLON)?}
     * (`PureInstanceClassMappingParserGrammar.g4:34`) and
     * {@code ENUMERATION_MAPPING identifier COLON} inside
     * `relationalPropertyMapping` — so the ANONYMOUS form
     * {@code prop: EnumerationMapping: <expr>} is not Legend grammar. Both of
     * our parsers used to accept it on the theory that the property's enum
     * type resolves the mapping at normalize time; the real engine parser
     * refuses it (`Unexpected token ':'. Valid alternatives: [',']`,
     * ZM4FixtureOracle). EnumerationMapping is a construct Legend owns, so
     * there is no defensible superset here.
     */
    /** The engine's {@code bindingTransformer: BINDING qualifiedName COLON}
     *  ({@code transformer} rule, shared with enumTransformer) — returns
     *  the Binding FQN or null. */
    private @com.legend.Nullable String bindingTransformerId() {
        if (peek() == TokenType.VALID_STRING && "Binding".equals(text())
                && isIdentifierToken(peek(1))) {
            advance();                              // 'Binding'
            String fqn = Protocol.unquotePath(parseQualifiedName());
            expect(TokenType.COLON);
            return fqn;
        }
        return null;
    }

    private @com.legend.Nullable String enumerationMappingId() {
        if (peek() != TokenType.ENUMERATION_MAPPING) {
            return null;
        }
        advance();                                  // 'EnumerationMapping'
        if (peek() == TokenType.COLON) {
            throw error("EnumerationMapping on a property mapping requires an"
                    + " enumeration mapping id (prop: EnumerationMapping"
                    + " <id>: <expr>)");
        }
        String enumId = parseMappingWord();
        expect(TokenType.COLON);
        return enumId;
    }

    /** Mapping-body sections, in the one order the grammar allows. The first
     *  two repeat; the last two are singletons. */
    private static final int BODY_INCLUDE = 1;
    private static final int BODY_ELEMENT = 2;
    private static final int BODY_LEGACY_TESTS = 3;
    private static final int BODY_TEST_SUITES = 4;

    /** A Relation binding's right-hand side: a column NAME, or a row
     *  expression when it is richer than one; {@code span} covers the RHS
     *  tokens (the valueFn wrapper's columns — see PRelationFnPropertyMapping). */
    private record RelationBinding(@com.legend.Nullable String column,
            @com.legend.Nullable com.legend.protocol.spec.ValueSpecification expr,
            SourceInfo span) {
    }

    /**
     * The RHS of a Relation class-mapping binding, up to (not consuming)
     * {@code stop} at depth 0.
     *
     * <p>It is a full ROW EXPRESSION, not just a column name: a bare
     * {@code COL} and a quoted {@code 'COL'} are plain column bindings,
     * {@code $src.COL} is the same binding in row-lambda spelling, and
     * anything richer ({@code $src.city_REGION->toOne()}) is a real spec
     * that rides {@code expr}. Mirrors MappingGrammarParser:626-661, which
     * is the behaviour the corpus was read with.
     */
    private RelationBinding relationBinding(TokenType stop) {
        int exprStart = pos;
        int depth = 0;
        while (!atEnd()) {
            TokenType t = peek();
            if (depth == 0 && (t == TokenType.COMMA || t == stop)) {
                break;
            }
            if (t == TokenType.PAREN_OPEN || t == TokenType.BRACKET_OPEN
                    || t == TokenType.BRACE_OPEN) {
                depth++;
            } else if (t == TokenType.PAREN_CLOSE
                    || t == TokenType.BRACKET_CLOSE
                    || t == TokenType.BRACE_CLOSE) {
                depth--;
                if (depth < 0) {
                    break;                  // the enclosing block's close
                }
            }
            advance();
        }
        com.legend.protocol.spec.ValueSpecification rhs =
                SpecParser.parse(tokens.slice(exprStart, pos), dialect);
        SourceInfo span = spanOf(exprStart, pos - 1);
        // NO $src.COL→column fold: the 4.138 wire keeps `$src.ID` as a
        // valueFn expression — only a BARE identifier (or quoted name) is a
        // column (relationUnionAdvancedSetup cm[3] pm[1], G8-adjudicated).
        if (rhs instanceof com.legend.protocol.spec.CString cs) {
            return new RelationBinding(cs.value(), null, span);
        }
        if (rhs instanceof com.legend.protocol.spec.PackageableElementPtr ptr
                && !ptr.fullPath().contains("::")) {
            return new RelationBinding(ptr.fullPath(), null, span);
        }
        return new RelationBinding(null, rhs, span);
    }

    /** Admit one mapping-body section, or refuse the swap / the duplicate;
     *  returns the new high-water rank. */
    private int bodySection(int seen, int rank, String what) {
        if (rank < seen) {
            throw error(what + " is out of order: a mapping body holds its"
                    + " includes, then its elements, then MappingTests, then"
                    + " testSuites");
        }
        if (rank == seen && rank >= BODY_LEGACY_TESTS) {
            throw error("duplicate " + what + ": a mapping declares it at"
                    + " most once");
        }
        return rank;
    }

    /** Admit one ~clause, or refuse it as a repeat / a swap. */
    private void clause(int[] seen, int rank, String name) {
        if (rank <= seen[0]) {
            throw error(name + " is out of order: a relational class"
                    + " mapping's directives appear at most once each, in"
                    + " the order ~filter ~distinct ~groupBy ~primaryKey"
                    + " ~mainTable, and all of them before the first"
                    + " property mapping");
        }
        seen[0] = rank;
    }

    /** {@code [*]cls: Relational { ~mainTable [db]T ~primaryKey(...)
     *  prop: <op>, ... }} — spans and shapes per probe
     *  relational-class-mapping; unsupported directives wall by name. */
    private Protocol.PClassMappingRel parseRelationalClassMapping(
            String target, int memberStart, SourceInfo targetSpan,
            @com.legend.Nullable String id, boolean root,
            @com.legend.Nullable String extendsId) {
        expect(TokenType.BRACE_OPEN);
        Protocol.PFilterMapping filter = null;
        Protocol.PTablePtr mainTable = null;
        List<Protocol.PRelOp> primaryKey = new ArrayList<>();
        List<Protocol.PRelOp> groupBy = new ArrayList<>();
        boolean distinct = false;
        List<Protocol.PPropertyMapping> props = new ArrayList<>();
        int[] clauseRank = {0};
        while (!atEnd() && peek() != TokenType.BRACE_CLOSE) {
            if (peek() == TokenType.MAIN_TABLE_CMD
                    || (peek() == TokenType.TILDE && "mainTable".equals(
                            peekText(1)))) {
                clause(clauseRank, RANK_MAIN_TABLE, "~mainTable");
                if (peek() == TokenType.TILDE) {
                    advance();
                }
                advance();                          // mainTable
                expect(TokenType.BRACKET_OPEN);
                String db = Protocol.unquotePath(parseQualifiedName());
                expect(TokenType.BRACKET_CLOSE);
                int tS = pos;                       // FIRST ident anchors
                String schema = "default";
                String tbl = parseMaybeQuotedIdentifier();
                if (peek() == TokenType.DOT) {
                    advance();
                    schema = tbl;
                    tbl = parseMaybeQuotedIdentifier();
                }
                mainTable = new Protocol.PTablePtr(db, db, schema, tbl,
                        spanOf(tS, pos - 1));
                continue;
            }
            if (peek() == TokenType.PRIMARY_KEY_CMD) {
                clause(clauseRank, RANK_PRIMARY_KEY, "~primaryKey");
                advance();
                expect(TokenType.PAREN_OPEN);
                while (peek() != TokenType.PAREN_CLOSE && !atEnd()) {
                    primaryKey.add(parseEmbeddedOperation());
                    match(TokenType.COMMA);
                }
                expect(TokenType.PAREN_CLOSE);
                continue;
            }
            if (peek() == TokenType.DISTINCT_CMD) {
                clause(clauseRank, RANK_DISTINCT, "~distinct");
                advance();
                distinct = true;
                continue;
            }
            if (peek() == TokenType.GROUP_BY_CMD) {
                clause(clauseRank, RANK_GROUP_BY, "~groupBy");
                advance();
                expect(TokenType.PAREN_OPEN);
                while (peek() != TokenType.PAREN_CLOSE && !atEnd()) {
                    groupBy.add(parseEmbeddedOperation());
                    match(TokenType.COMMA);
                }
                expect(TokenType.PAREN_CLOSE);
                continue;
            }
            if (peek() == TokenType.FILTER_CMD) {
                clause(clauseRank, RANK_FILTER, "~filter");
                filter = parseFilterMapping();
                continue;
            }
            if (peek() == TokenType.TILDE) {
                throw error("class-mapping directive '" + safeText()
                        + "' is unbuilt");
            }
            // Every ~clause precedes every property mapping; entering the
            // property list closes the clause prefix for good.
            clauseRank[0] = RANK_PROPERTIES;
            if (peek() == TokenType.VALID_STRING && "scope".equals(text())
                    && peek(1)
                            == TokenType.PAREN_OPEN) {
                parseScopeBlock(props, target, id);
                match(TokenType.COMMA);
                continue;
            }
            // property line: prop: <operation>
            parsePropertyLine(props, target, id, null, null);
        }
        int close = pos;
        expect(TokenType.BRACE_CLOSE);
        return new Protocol.PClassMappingRel(target, targetSpan, id, root,
                distinct, extendsId, filter, groupBy, mainTable, primaryKey,
                props,
                spanOf(memberStart, close));
    }

    /** {@code cls: Pure { ~src my::S  prop: <pure expr>, ... }} — the
     *  transforms parse through SpecParser (the ###Pure machinery) with
     *  absolute spans via token slices (probe pure-m2m). */
    private Protocol.PClassMappingPure parsePureClassMapping(String target,
            int memberStart, SourceInfo targetSpan,
            @com.legend.Nullable String id, boolean root,
            @com.legend.Nullable String extendsId) {
        expect(TokenType.BRACE_OPEN);
        String srcClass = null;
        SourceInfo srcSpan = null;
        List<com.legend.protocol.spec.ValueSpecification> filterBody = null;
        List<Protocol.PPurePropertyMapping> props = new ArrayList<>();
        while (!atEnd() && peek() != TokenType.BRACE_CLOSE) {
            if (peek() == TokenType.SRC_CMD) {
                if (srcClass != null) {
                    // ENGINE-VERBATIM (harvest TestMappingGrammarParser
                    // testPureInstanceClassMapping)
                    throw TokenStreamCursor.throwAt(tokens, memberStart,
                            "Field '~src' should be specified only once");
                }
                advance();                          // '~src' is ONE token
                int sS = pos;
                srcClass = Protocol.unquotePath(parseQualifiedName());
                srcSpan = spanOf(sS, pos - 1);
                continue;
            }
            if (peek() == TokenType.FILTER_CMD) {
                if (filterBody != null) {
                    throw TokenStreamCursor.throwAt(tokens, memberStart,
                            "Field '~filter' should be specified only once");
                }
                advance();
                int fStart = pos;
                int depth = 0;
                while (!atEnd()) {
                    TokenType t = peek();
                    if (depth == 0) {
                        if (t == TokenType.BRACE_CLOSE
                                || t == TokenType.SRC_CMD
                                || t == TokenType.FILTER_CMD
                                || t == TokenType.PLUS) {
                            break;
                        }
                        if ((t == TokenType.VALID_STRING
                                || t == TokenType.STRING)
                                && pos + 1 < tokens.count()
                                && tokens.type(pos + 1) == TokenType.COLON) {
                            break;                  // next property line
                        }
                    }
                    if (t == TokenType.PAREN_OPEN
                            || t == TokenType.BRACKET_OPEN
                            || t == TokenType.BRACE_OPEN) {
                        depth++;
                    } else if (t == TokenType.PAREN_CLOSE
                            || t == TokenType.BRACKET_CLOSE
                            || t == TokenType.BRACE_CLOSE) {
                        depth--;
                    }
                    advance();
                }
                filterBody = SpecParser.parseCodeBlock(
                        tokens.slice(fStart, pos), dialect);
                continue;
            }
            if (peek() == TokenType.TILDE) {
                throw error("pure class-mapping directive '" + safeText()
                        + "' is unbuilt");
            }
            int pS = pos;
            boolean local = match(TokenType.PLUS);  // span INCLUDES the '+'
            int identTok = pos;
            String prop = parseIdentifier();
            SourceInfo propSpan = spanOf(identTok, pos - 1);
            String srcId = id != null ? id : "";
            String tgtId = null;
            if (peek() == TokenType.BRACKET_OPEN) {
                advance();
                // the engine's walker (PureInstanceClassMappingParseTreeWalker:
                // purePropertyMapping.target = sourceAndTargetMappingId().sourceId(),
                // .source = the enclosing class mapping's id) reads the FIRST
                // bracketed id as the TARGET and never reads a second one; the
                // grammar admits `[a, b]`, the wire carries only `target: a`.
                // Byte parity (batch 6 own-corpus differential) — the second id
                // is parsed and dropped, as the engine drops it.
                tgtId = parseSetId();
                if (peek() == TokenType.COMMA) {
                    advance();
                    parseSetId();
                }
                expect(TokenType.BRACKET_CLOSE);
            }
            // The explosion marker sits AFTER the [set] group, not before it:
            //   propertyMapping: ... qualifiedName (sourceAndTargetMappingId)?
            //                    STAR? COLON ...
            // (PureInstanceClassMappingParserGrammar.g4:34). `d[setB]*:`
            // parses; `d*[setB]:` does not (ZM4FixtureOracle2).
            boolean explode = match(TokenType.STAR);
            Protocol.PLocalProp localProp = null;
            expect(TokenType.COLON);
            String enumId = enumerationMappingId();
            if (local) {
                // +prop: Type[m]: <expr> — the local prop's span is the
                // IDENT (probe pure-decorations, unlike relational)
                String type = Protocol.unquotePath(parseQualifiedName());
                MultBounds mb = parseLongMultBounds();
                expect(TokenType.BRACKET_CLOSE);
                localProp = new Protocol.PLocalProp(type, mb.lower(), mb.upper(),
                        propSpan);
                expect(TokenType.COLON);
            }
            int exprStart = pos;
            int depth = 0;
            while (!atEnd()) {
                TokenType t = peek();
                if (depth == 0 && (t == TokenType.COMMA
                        || t == TokenType.BRACE_CLOSE)) {
                    break;
                }
                if (depth == 0 && t == TokenType.SEMI_COLON) {
                    throw error("a property mapping binds ONE expression and"
                            + " is separated by ',' — ';' is not a mapping"
                            + " separator");
                }
                if (t == TokenType.PAREN_OPEN || t == TokenType.BRACKET_OPEN
                        || t == TokenType.BRACE_OPEN) {
                    depth++;
                } else if (t == TokenType.PAREN_CLOSE
                        || t == TokenType.BRACKET_CLOSE
                        || t == TokenType.BRACE_CLOSE) {
                    depth--;
                }
                advance();
            }
            // A property mapping's RHS is ONE expression, not a code block:
            // engine's rule is `... COLON combinedExpression` with COMMA
            // between mappings and no statement terminator anywhere
            // (PureInstanceClassMappingParserGrammar.g4:34). Reading it with
            // parseCodeBlock means a stray `;` is silently accepted as a
            // statement separator — which the legacy parser refused
            // ("trailing tokens after expression") and the reference parser
            // refuses too. Caught by SectionParseSentinelTest's LENIENT
            // ratchet when the switch exposed it (57 -> 59).
            List<com.legend.protocol.spec.ValueSpecification> body =
                    SpecParser.parseCodeBlock(tokens.slice(exprStart, pos), dialect);
            if (body.isEmpty()) {
                throw error("a property mapping has an empty body");
            }
            if (body.size() != 1) {
                throw error("a property mapping binds ONE expression; got "
                        + body.size());
            }
            props.add(new Protocol.PPurePropertyMapping(
                    local ? null : target, prop, propSpan, enumId, explode,
                    localProp, body, srcId, tgtId, spanOf(pS, pos - 1)));
            entryComma(TokenType.BRACE_CLOSE);
        }
        int close = pos;
        expect(TokenType.BRACE_CLOSE);
        if (srcClass == null && filterBody != null) {
            // A ~filter reads $src, so the mapping must declare its type.
            // Engine agrees in intent and enforces it by CRASHING: the
            // implicit $src is built as `new PackageableType(srcClass)` with
            // no null check (ClassMappingFirstPassBuilder:127). Refuse here,
            // where the position is known.
            throw error("a ~filter needs a ~src: the filter reads $src, so"
                    + " the mapping must declare its type");
        }
        return new Protocol.PClassMappingPure(target, targetSpan, extendsId,
                id, root, srcClass, srcSpan, filterBody, props,
                spanOf(memberStart, close));
    }

    /** Whether the {@code Relational} body opens with the
     *  {@code AssociationMapping} keyword (association form). */
    private boolean peekIsAssociationBody() {
        return peek() == TokenType.BRACE_OPEN
                && tokens.count() > pos + 1
                && tokens.type(pos + 1) == TokenType.ASSOCIATION_MAPPING;
    }

    /** {@code assoc: Relational { AssociationMapping ( side: [db]@J, ... ) }}
     *  — sides are join-only navs; stores = the [db] pointers in order of
     *  first appearance (probe include-and-assoc). */
    private Protocol.PRelAssociationMapping parseRelAssociationMapping(
            String target, int memberStart, SourceInfo targetSpan,
            @com.legend.Nullable String id) {
        expect(TokenType.BRACE_OPEN);
        expect(TokenType.ASSOCIATION_MAPPING);
        expect(TokenType.PAREN_OPEN);
        List<Protocol.PRelAssocPropertyMapping> props = new ArrayList<>();
        List<String> stores = new ArrayList<>();
        boolean storesClosed = false;
        while (!atEnd() && peek() != TokenType.PAREN_CLOSE) {
            int pS = pos;
            String prop = parseIdentifier();
            SourceInfo propSpan = spanOf(pS, pos - 1);
            String srcId = null;
            String tgtId = null;
            if (peek() == TokenType.BRACKET_OPEN) {
                // side[srcSet, tgtSet] (probe assoc-set-ids)
                advance();
                String first = parseSetId();
                if (peek() == TokenType.COMMA) {
                    advance();
                    srcId = first;
                    tgtId = parseSetId();
                } else {
                    tgtId = first;              // TARGET; assoc source null
                }
                expect(TokenType.BRACKET_CLOSE);
            }
            expect(TokenType.COLON);
            int oS = pos;
            Protocol.PRelOp op = parseEmbeddedOperation();
            if (op instanceof Protocol.PElemtWithJoins ej
                    && !storesClosed) {
                for (Protocol.PJoinPtr jp : ej.joins()) {
                    String jdb = jp.db();
                    if (jdb != null && !stores.contains(jdb)) {
                        stores.add(jdb);
                    }
                }
            } else if (!(op instanceof Protocol.PElemtWithJoins)) {
                // ENGINE-TRUE (harvest variant probes av/av2): the walker
                // collects join stores IN ORDER and STOPS at the first
                // non-join property mapping — join-then-column keeps the
                // store, column-then-join collects nothing
                storesClosed = true;
            }
            props.add(new Protocol.PRelAssocPropertyMapping(prop, propSpan,
                    op, srcId, tgtId, spanOf(oS - 1, pos - 1)));
            match(TokenType.COMMA);
        }
        expect(TokenType.PAREN_CLOSE);
        int close = pos;
        expect(TokenType.BRACE_CLOSE);
        return new Protocol.PRelAssociationMapping(
                new Protocol.PPointer("ASSOCIATION", target, targetSpan), id,
                props, stores, spanOf(memberStart, close));
    }

    /** {@code assoc: XStore { side: <cross expr>, ... }} — cross
     *  expressions are Pure lambdas via SpecParser (probe xstore). */
    private Protocol.PXStoreAssociationMapping parseXStoreAssociationMapping(
            String target, int memberStart, SourceInfo targetSpan,
            @com.legend.Nullable String id) {
        expect(TokenType.BRACE_OPEN);
        List<Protocol.PXStorePropertyMapping> props = new ArrayList<>();
        while (!atEnd() && peek() != TokenType.BRACE_CLOSE) {
            int pS = pos;
            String prop = parseIdentifier();
            SourceInfo propSpan = spanOf(pS, pos - 1);
            String srcId = "";
            String tgtId = "";
            if (peek() == TokenType.BRACKET_OPEN) {
                // side[srcSet, tgtSet] (probe xstore-ids)
                advance();
                String first = parseSetId();
                if (peek() != TokenType.COMMA) {
                    // engine crossExpr requires BOTH ids: side[src, tgt]
                    // (negative fixture engine-fixture#119). No m2 corpus
                    // source, no engine test, and no own test writes a
                    // single id (census 2026-08-12), so the old
                    // platform-level tolerance had ZERO users: every
                    // dialect refuses.
                    throw error("Unexpected token '" + safeText() + "'");
                }
                advance();
                srcId = first;
                tgtId = parseSetId();
                expect(TokenType.BRACKET_CLOSE);
            }
            expect(TokenType.COLON);
            int exprStart = pos;
            int depth = 0;
            while (!atEnd()) {
                TokenType t = peek();
                if (depth == 0 && (t == TokenType.COMMA
                        || t == TokenType.BRACE_CLOSE)) {
                    break;
                }
                // ...or the head of the NEXT entry, when the comma that
                // should separate them is missing. The engine REFUSES the
                // missing comma (negative fixture engine-fixture#118) —
                // the drop-in surface does too; lite's own dialect keeps
                // the tolerant break.
                if (depth == 0 && pos > exprStart && isIdentifierToken(t)
                        && (peek(1)
                                    == TokenType.BRACKET_OPEN
                                || peek(1)
                                    == TokenType.COLON)) {
                    if (dialect.refusesPlatformDialect()) {
                        throw error("Unexpected token '" + safeText() + "'");
                    }
                    break;
                }
                if (t == TokenType.PAREN_OPEN || t == TokenType.BRACKET_OPEN
                        || t == TokenType.BRACE_OPEN) {
                    depth++;
                } else if (t == TokenType.PAREN_CLOSE
                        || t == TokenType.BRACKET_CLOSE
                        || t == TokenType.BRACE_CLOSE) {
                    depth--;
                }
                advance();
            }
            List<com.legend.protocol.spec.ValueSpecification> body =
                    SpecParser.parseCodeBlock(tokens.slice(exprStart, pos), dialect);
            props.add(new Protocol.PXStorePropertyMapping(target, prop,
                    propSpan, body, srcId, tgtId, spanOf(pS, pos - 1)));
            if (!match(TokenType.COMMA) && peek() != TokenType.BRACE_CLOSE) {
                // A MISSING COMMA ends the entry list and DISCARDS the rest.
                //
                // legend-pure and legend-engine disagree here, and the
                // corpus is legend-pure's. Engine's ANTLR rule anchors on
                // EOF —  `(xStorePropertyMapping (COMMA xStorePropertyMapping)*)? EOF`
                // (XStoreAssociationMappingParserGrammar.g4:12-14) — so the
                // engine parser REJECTS this text. legend-pure's compiler
                // completes the rule and ignores what follows, and
                // `tests/mft/xStore/testMappingCrossStore.pure:238` is
                // written that way: two of its four entries never reach the
                // model, and the family's golden encodes their absence.
                // Reading the corpus at all requires legend-pure's reading.
                while (!atEnd() && peek() != TokenType.BRACE_CLOSE) {
                    advance();
                }
                break;
            }
        }
        int close = pos;
        expect(TokenType.BRACE_CLOSE);
        return new Protocol.PXStoreAssociationMapping(
                new Protocol.PPointer("ASSOCIATION", target, targetSpan),
                id, props, spanOf(memberStart, close));
    }

    /** {@code cls[id]: Operation { fqn(p1, p2) }} — the called FQN maps to
     *  the operation discriminator by EXACT FQN; identification is never
     *  by suffix (probe operation; STORE_UNION vs ROUTER_UNION semantics
     *  differ, a contains-match would run the wrong one). */
    private Protocol.PClassMapping parseOperationClassMapping(
            String target, int memberStart, SourceInfo targetSpan,
            @com.legend.Nullable String id,
            @com.legend.Nullable String extendsId, boolean root) {
        expect(TokenType.BRACE_OPEN);
        int fqnTok = pos;
        String fqn = Protocol.unquotePath(parseQualifiedName());
        String operation = switch (fqn) {
            case "meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_" ->
                    "STORE_UNION";
            case "meta::pure::router::operations::special_union_OperationSetImplementation_1__SetImplementation_MANY_" ->
                    "ROUTER_UNION";
            case "meta::pure::router::operations::inheritance_OperationSetImplementation_1__SetImplementation_MANY_" ->
                    "INHERITANCE";
            case "merge_OperationSetImplementation_1__SetImplementation_MANY_",
                 "meta::pure::router::operations::merge_OperationSetImplementation_1__SetImplementation_MANY_" ->
                    null;   // MERGE emits NO discriminator (probe merge-op)
            // UNKNOWN functions also emit NO discriminator (probe
            // custom-op-fn: a__SetImplementation_MANY_())
            default -> null;
        };
        expect(TokenType.PAREN_OPEN);
        List<String> params = new ArrayList<>();
        if (peek() == TokenType.BRACKET_OPEN) {
            // merge_...([p1,p2], {typed lambda}) — _type mergeOperation
            // WITH MERGE + validationFunction (probe merge-params-lambda)
            advance();
            while (peek() != TokenType.BRACKET_CLOSE && !atEnd()) {
                params.add(parseSetId());
                match(TokenType.COMMA);
            }
            expect(TokenType.BRACKET_CLOSE);
            expect(TokenType.COMMA);
            int lS = pos;
            expect(TokenType.BRACE_OPEN);
            int depth = 0;
            while (!atEnd()) {
                TokenType t = peek();
                if (t == TokenType.BRACE_OPEN) {
                    depth++;
                } else if (t == TokenType.BRACE_CLOSE) {
                    if (depth == 0) {
                        break;
                    }
                    depth--;
                }
                advance();
            }
            int lEnd = pos;
            expect(TokenType.BRACE_CLOSE);
            // the engine reparses ANTLR getText() — the lambda's TOKEN
            // TEXTS concatenated with NO whitespace — anchored at the
            // merge FQN token's line:column
            // (OperationClassMappingParseTreeWalker:86-93); emulate with
            // a padded re-lex of the same concatenation
            StringBuilder padded = new StringBuilder();
            for (int i = 1; i < tokens.startLine(fqnTok); i++) {
                padded.append('\n');
            }
            for (int i = 1; i < tokens.startColumn(fqnTok); i++) {
                padded.append(' ');
            }
            for (int i = lS; i <= lEnd; i++) {
                padded.append(tokens.text(i));
            }
            List<com.legend.protocol.spec.ValueSpecification> body =
                    SpecParser.parseCodeBlock(
                            com.legend.lexer.Lexer.tokenize(
                                    padded.toString()), dialect);
            if (body.size() != 1) {
                throw error("merge validation must be ONE lambda");
            }
            expect(TokenType.PAREN_CLOSE);
            match(TokenType.SEMI_COLON);
            int close = pos;
            expect(TokenType.BRACE_CLOSE);
            return new Protocol.PClassMappingMergeOperation(target,
                    targetSpan, id, root, params, body.get(0),
                    spanOf(memberStart, close));
        }
        while (peek() != TokenType.PAREN_CLOSE && !atEnd()) {
            params.add(parseSetId());
            match(TokenType.COMMA);
        }
        expect(TokenType.PAREN_CLOSE);
        match(TokenType.SEMI_COLON);                // trailing ';' legal
        int close = pos;
        expect(TokenType.BRACE_CLOSE);
        return new Protocol.PClassMappingOperation(target, targetSpan, id,
                extendsId, root, operation, params, spanOf(memberStart, close));
    }

    /** A set-implementation id — bare NUMBERS are legal ids (probe
     *  numeric-id: {@code *test::A[1]} emits id "1"). */
    private String parseSetId() {
        return parseMappingWord();
    }

    /** The engine's {@code word: identifier | BOOLEAN | INTEGER}
     *  (CoreParserGrammar) — set ids and enumeration-mapping ids admit
     *  numbers and booleans (engine fixtures testSpecialMappingElementId /
     *  testDuplicatedEnumerationMappingIds). */
    private String parseMappingWord() {
        TokenType t = peek();
        if (t == TokenType.INTEGER || t == TokenType.BOOLEAN
                || t == TokenType.TRUE || t == TokenType.FALSE) {
            String w = text();
            advance();
            return w;
        }
        return parseIdentifier();
    }

    /** {@code ~filter [db] NAME} or {@code ~filter [db]@J ... | [db2]NAME}
     *  — span '~' through the name (probe rel-filter/-joined). */
    private Protocol.PFilterMapping parseFilterMapping() {
        int fS = pos;
        advance();                                  // '~filter'
        if (peek() != TokenType.BRACKET_OPEN) {
            // mappingFilter: FILTER_CMD databasePointer
            //                (joinSequence PIPE databasePointer)? identifier
            // — unlike viewFilterMapping, the pointer is NOT optional here
            throw error("a class mapping's ~filter requires a [DB] pointer"
                    + " (~filter [DB] FilterName); only a View's ~filter may"
                    + " omit it");
        }
        expect(TokenType.BRACKET_OPEN);
        String db = Protocol.unquotePath(parseQualifiedName());
        expect(TokenType.BRACKET_CLOSE);
        List<Protocol.PJoinPtr> joins = new ArrayList<>();
        String fdb = db;
        String name;
        String firstType = null;
        if (peek() == TokenType.PAREN_OPEN) {
            // ~filter [db] (INNER)@J | ... (probe filter-jointype)
            advance();
            firstType = parseIdentifier();
            validateJoinType(firstType);
            expect(TokenType.PAREN_CLOSE);
        }
        if (peek() == TokenType.AT) {
            String curDb = db;
            String pendingType = firstType;
            while (peek() == TokenType.AT) {
                int jS = pos;                       // span INCLUDES the '@'
                advance();
                joins.add(new Protocol.PJoinPtr(curDb, pendingType,
                        parseIdentifier(), spanOf(jS, pos - 1)));
                pendingType = null;
                if (peek() != TokenType.GREATER_THAN) {
                    break;
                }
                advance();
                if (peek() == TokenType.PAREN_OPEN) {
                    advance();
                    pendingType = parseIdentifier();
                    validateJoinType(pendingType);
                    expect(TokenType.PAREN_CLOSE);
                }
                if (peek() == TokenType.BRACKET_OPEN) {
                    advance();
                    curDb = Protocol.unquotePath(parseQualifiedName());
                    expect(TokenType.BRACKET_CLOSE);
                }
            }
            expect(TokenType.PIPE);
            expect(TokenType.BRACKET_OPEN);
            fdb = Protocol.unquotePath(parseQualifiedName());
            expect(TokenType.BRACKET_CLOSE);
            name = parseIdentifier();
        } else {
            name = parseIdentifier();
        }
        return new Protocol.PFilterMapping(fdb, name, joins,
                spanOf(fS, pos - 1));
    }

    /** One {@code prop[srcId,tgtId]: [EnumerationMapping em:] <op>} line.
     *  ONE bracket id is the TARGET set (source stays the enclosing id);
     *  TWO are source,target (probe prop-set-ids). An inline
     *  {@code EnumerationMapping em:} rides enumMappingId (probe
     *  inline-enum-transform). */
    private void parsePropertyLine(List<Protocol.PPropertyMapping> props,
            @com.legend.Nullable String target, @com.legend.Nullable String id,
            @com.legend.Nullable String scopeDb,
            DatabaseProtocolParser.@com.legend.Nullable ScopeCtx scope) {
        int pS = pos;
        Protocol.PLocalProp localProp = null;
        boolean local = match(TokenType.PLUS);
        if (local) {
            pS = pos;                               // ident anchors the span
        }
        String prop = parseIdentifier();
        SourceInfo propSpan = spanOf(pS, pos - 1);
        String srcId = id;
        String tgtId = null;
        String embId = null;
        if (peek() == TokenType.BRACKET_OPEN) {
            advance();
            String first = parseSetId();
            if (peek() == TokenType.COMMA) {
                advance();
                srcId = first;
                tgtId = parseSetId();
            } else {
                // ONE id is the TARGET; source stays the ENCLOSING id
                // (engine RelationalParseTreeWalker:1211-1216)
                tgtId = first;
                embId = first;
            }
            expect(TokenType.BRACKET_CLOSE);
        }
        if (peek() == TokenType.PAREN_OPEN) {
            // prop[k] ( lines... ) — embedded block; the span is the
            // paren region; inner lines drop the class key and keep the
            // ENCLOSING set id as source (probe embedded-plain)
            int parenTok = pos;
            advance();
            if (peek() == TokenType.PAREN_CLOSE
                    && "Inline".equals(peekText(1))) {
                // prop() Inline[setId] — span '('..']' (probe
                // inline-embedded)
                advance();
                advance();                          // Inline
                expect(TokenType.BRACKET_OPEN);
                String setId = parseIdentifier();
                int close = pos;
                expect(TokenType.BRACKET_CLOSE);
                props.add(new Protocol.PInlineEmbeddedPropertyMapping(target,
                        prop, propSpan, embId, setId,
                        spanOf(parenTok, close)));
                entryComma(TokenType.BRACE_CLOSE, TokenType.PAREN_CLOSE);
                return;
            }
            List<Protocol.PPropertyMapping> inner = new ArrayList<>();
            List<Protocol.PRelOp> pk = new ArrayList<>();
            while (!atEnd() && peek() != TokenType.PAREN_CLOSE) {
                if (peek() == TokenType.PRIMARY_KEY_CMD) {
                    advance();
                    expect(TokenType.PAREN_OPEN);
                    while (peek() != TokenType.PAREN_CLOSE && !atEnd()) {
                        pk.add(parseOpInCtx(scopeDb, scope));
                        match(TokenType.COMMA);
                    }
                    expect(TokenType.PAREN_CLOSE);
                    continue;
                }
                parsePropertyLine(inner, null, id, scopeDb, scope);
            }
            int close = pos;
            expect(TokenType.PAREN_CLOSE);
            if (peek() == TokenType.OTHERWISE) {
                // ) Otherwise ( [tgt]:<op> ) — the embedded class
                // mapping's span runs paren..OTHERWISE-close but the
                // OUTER pm's span runs OTHERWISE..close; the op span
                // stretches back to the '[' (probe otherwise-embedded)
                int otherwiseTok = pos;
                advance();
                expect(TokenType.PAREN_OPEN);
                int oS = pos;
                expect(TokenType.BRACKET_OPEN);
                String tgt = parseSetId();
                expect(TokenType.BRACKET_CLOSE);
                expect(TokenType.COLON);
                if (dialect.refusesPlatformDialect()) {
                    // engine otherwisePropertyMapping is `databasePointer?
                    // joinSequence` — a JOIN target only. m2 additionally
                    // allows a plain column pointer ([id]: [db]T.col);
                    // that form is pure-dialect. Messages are the
                    // oracle's own (probe ZOtherwiseSpellingProbe).
                    int look = pos;
                    boolean hasDb = tokens.type(look) == TokenType.BRACKET_OPEN;
                    if (hasDb) {
                        while (look < tokens.count()
                                && tokens.type(look) != TokenType.BRACKET_CLOSE) {
                            look++;
                        }
                        look++;
                    }
                    if (look < tokens.count()
                            && tokens.type(look) != TokenType.AT
                            && tokens.type(look) != TokenType.PAREN_OPEN) {
                        pos = look;
                        throw error("Unexpected token '" + safeText()
                                + "'. Valid alternatives: "
                                + (hasDb ? "['(', '@']" : "['[', '(', '@']"));
                    }
                }
                Protocol.PRelOp op = parseOpInCtx(scopeDb, scope);
                op = withSpanStart(op, oS);
                int oClose = pos;
                expect(TokenType.PAREN_CLOSE);
                props.add(new Protocol.POtherwiseEmbeddedPropertyMapping(
                        target, prop, propSpan, embId, pk, inner, op, tgt,
                        spanOf(parenTok, oClose),
                        spanOf(otherwiseTok, oClose)));
                entryComma(TokenType.BRACE_CLOSE, TokenType.PAREN_CLOSE);
                return;
            }
            props.add(new Protocol.PEmbeddedPropertyMapping(target, prop,
                    propSpan, embId, pk, inner, spanOf(parenTok, close)));
            entryComma(TokenType.BRACE_CLOSE, TokenType.PAREN_CLOSE);
            return;
        }
        int colonTok = pos;
        expect(TokenType.COLON);
        if (local) {
            // +prop: Type[m]: <op> — span FIRST colon..bracket close
            String type = Protocol.unquotePath(parseQualifiedName());
            MultBounds mb = parseLongMultBounds();
            int mClose = pos;
            expect(TokenType.BRACKET_CLOSE);
            localProp = new Protocol.PLocalProp(type, mb.lower(), mb.upper(),
                    spanOf(colonTok, mClose));
            colonTok = pos;                         // PM span = SECOND colon
            expect(TokenType.COLON);
        }
        String enumId = enumerationMappingId();
        String bindingId = bindingTransformerId();
        Protocol.PRelOp op = parseOpInCtx(scopeDb, scope);
        props.add(new Protocol.PRelPropertyMapping(
                localProp != null ? null : target, bindingId,
                prop, propSpan, enumId,
                localProp, op, srcId, localProp != null ? null : tgtId,
                spanOf(colonTok, pos - 1)));
        entryComma(TokenType.BRACE_CLOSE, TokenType.PAREN_CLOSE);
    }

    /** {@code scope([db]seg(.seg)?) ( prop-lines )} — the engine FLATTENS
     *  scope at parse: inner lines emit as plain property mappings whose
     *  table (when the header has one) spans the HEADER tokens (probe
     *  scope-forms). */
    private void parseScopeBlock(List<Protocol.PPropertyMapping> props,
            @com.legend.Nullable String target,
            @com.legend.Nullable String id) {
        advance();                                  // 'scope'
        expect(TokenType.PAREN_OPEN);
        expect(TokenType.BRACKET_OPEN);
        String db = Protocol.unquotePath(parseQualifiedName());
        expect(TokenType.BRACKET_CLOSE);
        DatabaseProtocolParser.ScopeCtx scope = null;
        if (peek() != TokenType.PAREN_CLOSE) {
            int g1 = pos;
            String seg1 = parseIdentifier();
            if (peek() == TokenType.DOT) {
                advance();
                parseIdentifier();
                scope = new DatabaseProtocolParser.ScopeCtx(db, seg1,
                        tokens.text(pos - 1), spanOf(g1, pos - 1), false);
            } else {
                scope = new DatabaseProtocolParser.ScopeCtx(db, "default",
                        seg1, spanOf(g1, g1), true);
            }
        }
        expect(TokenType.PAREN_CLOSE);
        expect(TokenType.PAREN_OPEN);
        while (!atEnd() && peek() != TokenType.PAREN_CLOSE) {
            parsePropertyLine(props, target, id, scope == null ? db : null,
                    scope);
        }
        expect(TokenType.PAREN_CLOSE);
    }

    /** The op with its span's START moved to token {@code startTok}. */
    private Protocol.PRelOp withSpanStart(Protocol.PRelOp op, int startTok) {
        SourceInfo st = spanOf(startTok, startTok);
        SourceInfo w = new SourceInfo("", st.startLine(), st.startColumn(),
                op.sourceInformation().endLine(),
                op.sourceInformation().endColumn());
        return switch (op) {
            case Protocol.PElemtWithJoins ej ->
                    new Protocol.PElemtWithJoins(ej.joins(),
                            ej.relationalElement(), w);
            case Protocol.PColumnRef c -> new Protocol.PColumnRef(c.column(),
                    c.table(), c.tableAlias(), w);
            case Protocol.PDynaFunc f -> new Protocol.PDynaFunc(f.funcName(),
                    f.parameters(), w);
            case Protocol.PRelLiteral l ->
                    new Protocol.PRelLiteral(l.value(), w);
            case Protocol.PRelLambda lam ->
                    new Protocol.PRelLambda(lam.parameterNames(), lam.body(), w);
            case Protocol.PLambdaParam lp -> new Protocol.PLambdaParam(lp.name(), w);
            case Protocol.PRelLiteralList ll ->
                    new Protocol.PRelLiteralList(ll.values(), w);
        };
    }

    /** {@code assoc: ModelJoin { {x:T[1], y:U[1]|expr} }} — the inner
     *  braces are ONE typed ###Pure lambda through SpecParser (probe
     *  modeljoin); member span target..outer close. */
    private Protocol.PModelJoinAssociationMapping parseModelJoin(
            String target, int memberStart, SourceInfo targetSpan,
            @com.legend.Nullable String id) {
        expect(TokenType.BRACE_OPEN);
        int lS = pos;
        int depth = 0;
        while (!atEnd()) {
            TokenType t = peek();
            if (t == TokenType.BRACE_OPEN) {
                depth++;
            } else if (t == TokenType.BRACE_CLOSE) {
                if (depth == 0) {
                    break;
                }
                depth--;
            }
            advance();
        }
        List<com.legend.protocol.spec.ValueSpecification> body =
                SpecParser.parseCodeBlock(tokens.slice(lS, pos), dialect);
        if (body.size() != 1) {
            throw error("ModelJoin body must be ONE lambda, got "
                    + body.size());
        }
        if (!(body.get(0)
                instanceof com.legend.protocol.spec.LambdaFunction)) {
            // ENGINE-VERBATIM (ModelJoinAssociationMappingParseTreeWalker):
            // a bare boolean expression is NOT a legal join condition —
            // the walker re-parses the body and refuses non-lambdas
            // (refusal-asymmetry engine-fixture#505)
            throw error("ModelJoin association mapping requires a lambda"
                    + " join condition of the form '{src: SrcClass[1],"
                    + " tgt: TgtClass[1] | <boolean expression>}'");
        }
        int close = pos;
        expect(TokenType.BRACE_CLOSE);
        return new Protocol.PModelJoinAssociationMapping(
                new Protocol.PPointer("ASSOCIATION", target, targetSpan),
                id, body.get(0), spanOf(memberStart, close));
    }

    /** {@code cls[id]: Relation { ~func <descriptor> prop: col, ... }} —
     *  the descriptor pointer path is the CANONICAL text (tokens joined
     *  with NO spaces); property lines map columns (probe relation-fn). */
    private Protocol.PClassMappingRelation parseRelationClassMapping(
            String target, int memberStart, SourceInfo targetSpan,
            @com.legend.Nullable String id,
            @com.legend.Nullable String extendsId, boolean root) {
        int braceTok = pos;
        expect(TokenType.BRACE_OPEN);
        int braceLine = tokens.startLine(braceTok);
        // `~func <ref>` is the FUNCTION-pointer form; `~src <ref>()` is the
        // row-source form. Both name an extent function, but their WIRE
        // differs (4.138, probe ZRelationMappingProbe): ~func emits a
        // relationFunction pointer, ~src emits a sourceLambda.
        boolean srcForm = false;
        if (peek() == TokenType.SRC_CMD) {
            advance();                              // '~src' is ONE token
            srcForm = true;
            if (!isIdentifierToken(peek())) {
                // ~src <expression> — the engine parses ANY expression
                // here and wraps it in sourceLambda (compile adjudicates;
                // harvest fixture '~src 1'). Slice runs to the next
                // clause/property-line/close at depth 0.
                int eS = pos;
                int depth = 0;
                while (!atEnd()) {
                    TokenType t = peek();
                    if (t == TokenType.BRACE_OPEN || t == TokenType.PAREN_OPEN
                            || t == TokenType.BRACKET_OPEN) {
                        depth++;
                    } else if (t == TokenType.PAREN_CLOSE
                            || t == TokenType.BRACKET_CLOSE) {
                        depth--;
                    } else if (depth == 0
                            && (t == TokenType.BRACE_CLOSE
                                    || t == TokenType.PRIMARY_KEY_CMD
                                    || (isIdentifierToken(t) && peek(1)
                                            == TokenType.COLON))) {
                        break;
                    } else if (t == TokenType.BRACE_CLOSE) {
                        depth--;
                    }
                    advance();
                }
                // the engine WALKER's re-parse anchor quirk (see
                // PRelationSrcLambda), same split as the ~src fn form:
                // INNER expression spans stay RAW; only the lambda
                // WRAPPER span shifts DOWN by (expr line - brace line)
                int delta = tokens.startLine(eS) - braceLine;
                List<com.legend.protocol.spec.ValueSpecification> eBody =
                        SpecParser.parseCodeBlock(tokens.slice(eS, pos),
                                dialect);
                if (eBody.size() != 1) {
                    throw error("~src expression must be ONE expression");
                }
                return finishRelationClassMapping(target, id, extendsId,
                        root, memberStart, braceLine,
                        new Protocol.PRelationSrcLambda(null, null,
                                eBody.get(0),
                                shiftLines(spanOf(eS, pos - 1), delta)),
                        null, null);
            }
        } else if (peek() == TokenType.TILDE
                && "func".equals(peekText(1))) {
            advance();
            advance();                              // func
        } else {
            throw error("Relation class mapping must open with ~func"
                    + " (or its ~src alias)");
        }
        // The descriptor is `<fqn>` with an OPTIONAL signature spelling
        // `f():Relation<Any>[1]` — the SHARED pointer-site scan
        // (TokenStreamCursor#parseFunctionDescriptor; this arm's canonical
        // text is the tokens joined with NO spaces).
        FunctionDescriptor fd = parseFunctionDescriptor();
        String desc = compactText(fd.start(), fd.end() - 1);
        SourceInfo fnSpan = spanOf(fd.start(), fd.end() - 1);
        Protocol.PRelationSrcLambda srcLambda = null;
        if (srcForm) {
            int delta = tokens.startLine(fd.start()) - braceLine;
            srcLambda = new Protocol.PRelationSrcLambda(
                    compactText(fd.start(), fd.nameEnd() - 1),
                    spanOf(fd.start(), fd.nameEnd() - 1), null,
                    shiftLines(fnSpan, delta));
        }
        return finishRelationClassMapping(target, id, extendsId, root,
                memberStart, braceLine, srcLambda,
                srcForm ? null : desc, srcForm ? null : fnSpan);
    }

    /** The primaryKey + property-line + close tail of a Relation class
     *  mapping — shared by the descriptor (~func / ~src fn) and the
     *  expression-source (~src <expr>) heads. */
    private Protocol.PClassMappingRelation finishRelationClassMapping(
            String target, @com.legend.Nullable String id,
            @com.legend.Nullable String extendsId, boolean root,
            int memberStart, int braceLine,
            Protocol.@com.legend.Nullable PRelationSrcLambda srcLambda,
            @com.legend.Nullable String desc,
            @com.legend.Nullable SourceInfo fnSpan) {
        List<String> pk = new ArrayList<>();
        if (peek() == TokenType.PRIMARY_KEY_CMD) {
            // engine (RelationFunctionMappingParserGrammar): ~primaryKey:
            // COL | [COL, ...] — a single bare column needs no brackets
            advance();
            expect(TokenType.COLON);
            if (match(TokenType.BRACKET_OPEN)) {
                while (peek() != TokenType.BRACKET_CLOSE && !atEnd()) {
                    pk.add(parseIdentifier());
                    match(TokenType.COMMA);
                }
                expect(TokenType.BRACKET_CLOSE);
            } else {
                pk.add(parseIdentifier());
            }
        }
        List<Protocol.PRelationFnPropertyMapping> props = new ArrayList<>();
        while (!atEnd() && peek() != TokenType.BRACE_CLOSE) {
            int pS = pos;
            boolean local = match(TokenType.PLUS);
            int identTok = pos;
            String prop = parseIdentifier();
            SourceInfo propSpan = spanOf(identTok, pos - 1);
            if (peek() == TokenType.PAREN_OPEN
                    && peek(1)
                            != TokenType.PAREN_CLOSE) {
                // prop ( street: COL, ... ) — embedded with nested
                // classless pms; span prop..close paren (probe
                // relation-embedded)
                advance();
                List<Protocol.PRelationFnPropertyMapping> nested =
                        new ArrayList<>();
                while (!atEnd() && peek() != TokenType.PAREN_CLOSE) {
                    int nS = pos;
                    String nProp = parseIdentifier();
                    SourceInfo nSpan = spanOf(nS, pos - 1);
                    expect(TokenType.COLON);
                    // a NESTED binding's RHS is a full row expression too —
                    // `legalName: $src.firmName->toOne()` in
                    // tests/mapping/union/relation. Reading only a bare
                    // identifier here is the same gap the outer arm had.
                    String nEnum = enumerationMappingId();
                    String nBinding = bindingTransformerId();
                    RelationBinding nb = relationBinding(
                            TokenType.PAREN_CLOSE);
                    nested.add(new Protocol.PRelationFnPropertyMapping(null,
                            nBinding,
                            nProp, nSpan, nb.column(), null, null, null, null,
                            nEnum, nb.expr(), exprWrapper(nb, braceLine),
                            spanOf(nS, pos - 1)));
                    match(TokenType.COMMA);
                }
                int pClose = pos;
                expect(TokenType.PAREN_CLOSE);
                props.add(new Protocol.PRelationFnPropertyMapping(target,
                        null,
                        prop, propSpan, null, null, nested, null, id,
                        null, null, null, spanOf(pS, pClose)));
                match(TokenType.COMMA);
                continue;
            }
            if (peek() == TokenType.PAREN_OPEN) {
                // prop () Inline [set] (probe relation-inline)
                advance();
                expect(TokenType.PAREN_CLOSE);
                if (!(peek() == TokenType.INLINE)) {
                    throw error("expected Inline after ()");
                }
                advance();
                expect(TokenType.BRACKET_OPEN);
                String setId = parseSetId();
                int bClose = pos;
                expect(TokenType.BRACKET_CLOSE);
                props.add(new Protocol.PRelationFnPropertyMapping(target,
                        null,
                        prop, propSpan, null, setId, null, null, id,
                        null, null, null, spanOf(pS, bClose)));
                match(TokenType.COMMA);
                continue;
            }
            expect(TokenType.COLON);
            Protocol.PLocalProp lp = null;
            if (local) {
                // +prop: Type[m]: COL — local SI = the IDENT, pm span
                // includes the '+', class KEPT (probe relation-extras)
                String type = Protocol.unquotePath(parseQualifiedName());
                MultBounds mb = parseLongMultBounds();
                expect(TokenType.BRACKET_CLOSE);
                lp = new Protocol.PLocalProp(type, mb.lower(), mb.upper(), propSpan);
                expect(TokenType.COLON);
            }
            // `prop: EnumerationMapping <id>: <rhs>` — the id is required,
            // exactly as everywhere else (see #enumerationMappingId).
            String enumId = enumerationMappingId();
            String bindingId = bindingTransformerId();
            RelationBinding rb = relationBinding(TokenType.BRACE_CLOSE);
            props.add(new Protocol.PRelationFnPropertyMapping(target,
                    bindingId, prop,
                    propSpan, rb.column(), null, null, lp, id, enumId,
                    rb.expr(), exprWrapper(rb, braceLine),
                    spanOf(pS, pos - 1)));
            match(TokenType.COMMA);
        }
        int close = pos;
        expect(TokenType.BRACE_CLOSE);
        return new Protocol.PClassMappingRelation(target, id, extendsId,
                pk, props,
                desc, fnSpan, srcLambda,
                root, spanOf(memberStart, close));
    }

    /** The valueFn WRAPPER span for an expression binding: the RHS columns
     *  with both lines shifted DOWN by (RHS line - cm brace line) — the
     *  engine walker's re-parse anchor quirk (see PRelationSrcLambda). */
    private static @com.legend.Nullable SourceInfo exprWrapper(
            RelationBinding rb, int braceLine) {
        if (rb.expr() == null) {
            return null;
        }
        return shiftLines(rb.span(), rb.span().startLine() - braceLine);
    }

    /** {@code id: { function: |...; tests: [...] }} — suite span
     *  id..close; the query lambda reparses with the MAPPING path as its
     *  span sourceId (probe test-suites). */
    private Protocol.PMappingTestSuite parseTestSuite(String mappingFqn) {
        int sS = pos;
        String suiteId = parseIdentifier();
        expect(TokenType.COLON);
        expect(TokenType.BRACE_OPEN);
        com.legend.protocol.spec.ValueSpecification func = null;
        List<Protocol.PMappingTest> tests = new ArrayList<>();
        String doc = null;
        while (!atEnd() && peek() != TokenType.BRACE_CLOSE) {
            String key = text();
            if ("doc".equals(key)) {
                // doc: '<string>'; — suite documentation (harvest
                // testSimpleTestSuite; wire "doc" right after _type)
                if (doc != null) {
                    // once-only (mutant duplicate-field probe)
                    throw error("Field 'doc' should be specified only once");
                }
                advance();
                expect(TokenType.COLON);
                doc = consumeStringLiteral("'doc'");
                expect(TokenType.SEMI_COLON);
                continue;
            }
            if ("function".equals(key)) {
                if (func != null) {
                    throw error("Field 'function' should be specified"
                            + " only once");
                }
                advance();
                expect(TokenType.COLON);
                int fS = pos;
                int depth = 0;
                while (!atEnd()) {
                    TokenType t = peek();
                    if (depth == 0 && t == TokenType.SEMI_COLON) {
                        break;
                    }
                    if (t == TokenType.PAREN_OPEN
                            || t == TokenType.BRACKET_OPEN
                            || t == TokenType.BRACE_OPEN) {
                        depth++;
                    } else if (t == TokenType.PAREN_CLOSE
                            || t == TokenType.BRACKET_CLOSE
                            || t == TokenType.BRACE_CLOSE) {
                        depth--;
                    }
                    advance();
                }
                List<com.legend.protocol.spec.ValueSpecification> body =
                        SpecParser.parseCodeBlock(tokens.slice(fS, pos),
                                mappingFqn, dialect);
                if (body.size() != 1) {
                    throw error("suite function must be ONE lambda");
                }
                func = body.get(0);
                expect(TokenType.SEMI_COLON);
                continue;
            }
            if ("tests".equals(key)) {
                advance();
                expect(TokenType.COLON);
                expect(TokenType.BRACKET_OPEN);
                while (!atEnd() && peek() != TokenType.BRACKET_CLOSE) {
                    tests.add(parseMappingTest());
                    match(TokenType.COMMA);
                }
                expect(TokenType.BRACKET_CLOSE);
                match(TokenType.SEMI_COLON);
                continue;
            }
            throw error("mapping test-suite key '" + safeText()
                    + "' is unbuilt");
        }
        int close = pos;
        expect(TokenType.BRACE_CLOSE);
        if (func == null) {
            throw TokenStreamCursor.throwAt(tokens, sS,
                    "mapping test suite without a function");
        }
        return new Protocol.PMappingTestSuite(suiteId, doc, func, tests,
                spanOf(sS, close));
    }

    /** {@code name ( query: |...; data: [<Object, JSON, cls, 'json'>];
     *  assert: 'str'; )} — the legacy MappingTests form (probe
     *  legacy-mapping-tests): test span name..close paren; assert span
     *  keyword..string; inputData span = the angle region. */
    private Protocol.PLegacyMappingTest parseLegacyTest(String mappingFqn) {
        int tS = pos;
        String name = parseIdentifier();
        expect(TokenType.PAREN_OPEN);
        com.legend.protocol.spec.ValueSpecification query = null;
        List<Protocol.PLegacyInputData> inputData = new ArrayList<>();
        String expected = null;
        SourceInfo assertSpan = null;
        boolean seenData = false;
        while (!atEnd() && peek() != TokenType.PAREN_CLOSE) {
            String key = text();
            if ("query".equals(key)
                    || peek() == TokenType.MAPPING_TESTS_QUERY) {
                if (query != null) {
                    // ENGINE-VERBATIM cardinality (harvest
                    // TestMappingGrammarParser#testMappingTest)
                    throw TokenStreamCursor.throwAt(tokens, tS,
                            "Field 'query' should be specified only once");
                }
                advance();
                expect(TokenType.COLON);
                int fS = pos;
                int depth = 0;
                while (!atEnd()) {
                    TokenType t = peek();
                    if (depth == 0 && t == TokenType.SEMI_COLON) {
                        break;
                    }
                    if (t == TokenType.PAREN_OPEN
                            || t == TokenType.BRACKET_OPEN
                            || t == TokenType.BRACE_OPEN) {
                        depth++;
                    } else if (t == TokenType.PAREN_CLOSE
                            || t == TokenType.BRACKET_CLOSE
                            || t == TokenType.BRACE_CLOSE) {
                        depth--;
                    }
                    advance();
                }
                List<com.legend.protocol.spec.ValueSpecification> body =
                        SpecParser.parseCodeBlock(tokens.slice(fS, pos),
                                mappingFqn, dialect);
                if (body.size() != 1) {
                    throw error("legacy test query must be ONE lambda");
                }
                query = body.get(0);
                expect(TokenType.SEMI_COLON);
                continue;
            }
            if ("data".equals(key)) {
                if (seenData) {
                    throw TokenStreamCursor.throwAt(tokens, tS,
                            "Field 'data' should be specified only once");
                }
                seenData = true;
                advance();
                expect(TokenType.COLON);
                expect(TokenType.BRACKET_OPEN);
                while (!atEnd() && peek() != TokenType.BRACKET_CLOSE) {
                    int dS = pos;
                    expect(TokenType.LESS_THAN);
                    String kind = parseIdentifier();
                    boolean relational = "Relational".equals(kind);
                    if (!relational && !"Object".equals(kind)) {
                        throw error("legacy input kind '" + kind
                                + "' is unbuilt");
                    }
                    expect(TokenType.COMMA);
                    // testInputElement: LESS_THAN testInputType
                    //   (COMMA testInputFormat)? COMMA testInputSrc
                    //   COMMA testInputDataContent GREATER_THAN
                    // The FORMAT is optional, so the components before the
                    // data are either [src] or [format, src]. The data is
                    // always last and always starts with a STRING, which is
                    // what tells them apart without lookahead games.
                    List<String> heads = new ArrayList<>();
                    while (!atEnd() && peek() != TokenType.STRING) {
                        heads.add(Protocol.unquotePath(parseQualifiedName()));
                        expect(TokenType.COMMA);
                    }
                    // The grammar makes the FORMAT optional, but BOTH
                    // walkers then refuse a missing one
                    // (RelationalGrammarParserExtension:325-330,
                    // CorePureGrammarParser:338-341), so it is mandatory in
                    // practice and we refuse it in the same words rather
                    // than inventing a wire shape we cannot observe.
                    if (heads.size() != 2) {
                        throw error("Mapping test "
                                + (relational ? "relational" : "object")
                                + " 'input type' is missing");
                    }
                    String inputType = heads.get(0);
                    // engine validates the format at PARSE
                    // (RelationalGrammarParserExtension:334 /
                    // CorePureGrammarParser:346); 'FUNK_UNKNOWN_FORMAT' is
                    // engine's own negative fixture
                    boolean knownFormat = relational
                            ? "SQL".equals(inputType) || "CSV".equals(inputType)
                            : "JSON".equals(inputType) || "XML".equals(inputType);
                    if (!knownFormat) {
                        throw error("Mapping test "
                                + (relational ? "relational" : "object")
                                + " input data does not support format '"
                                + inputType + "'. Possible values: "
                                + (relational ? "SQL, CSV" : "JSON, XML"));
                    }
                    String targetPath = heads.get(1);
                    // testInputDataContent: STRING ('+' STRING)* — the parts
                    // CONCATENATE. Engine decodes each part and joins with
                    // "" (RelationalGrammarParserExtension:339,
                    // CorePureGrammarParser:349), and the two kinds decode
                    // DIFFERENTLY: Relational unescapes, Object does not.
                    StringBuilder dataBuf = new StringBuilder();
                    do {
                        String raw = text();
                        if (peek() != TokenType.STRING) {
                            throw error("expected a quoted string in the test"
                                    + " input data, got " + peek());
                        }
                        dataBuf.append(relational
                                // the `\;` guard is engine's, and it is
                                // load-bearing: our unescape drops the
                                // backslash of an unrecognised escape, so
                                // without it `\;` would decode to `;`
                                ? TokenStreamCursor.unquoteAndUnescape(
                                        raw.replace("\\;", "\\\\;"), this)
                                : raw.substring(1, raw.length() - 1));
                        advance();
                    } while (match(TokenType.PLUS));
                    String data = dataBuf.toString();
                    int dE = pos;
                    expect(TokenType.GREATER_THAN);
                    inputData.add(new Protocol.PLegacyInputData(relational,
                            targetPath, inputType, data, spanOf(dS, dE)));
                    match(TokenType.COMMA);
                }
                expect(TokenType.BRACKET_CLOSE);
                expect(TokenType.SEMI_COLON);
                continue;
            }
            if ("assert".equals(key)) {
                if (expected != null) {
                    throw TokenStreamCursor.throwAt(tokens, tS,
                            "Field 'assert' should be specified only once");
                }
                int aS = pos;
                advance();
                expect(TokenType.COLON);
                // engine: fromGrammarString(x, FALSE) — RAW body, quotes
                // stripped, NO unescape (MappingParseTreeWalker:295).
                // MUST be a STRING token: the old unguarded substring
                // silently corrupted `assert: foo;` to "o" and crashed raw
                // on `assert: 5;` (adversarial audit F1)
                if (atEnd() || peek() != TokenType.STRING) {
                    throw error("expected a string literal for 'assert'");
                }
                String rawStr = text();
                expected = rawStr.substring(1, rawStr.length() - 1);
                advance();
                assertSpan = spanOf(aS, pos);   // span INCLUDES the ';'
                expect(TokenType.SEMI_COLON);
                continue;
            }
            throw error("legacy mapping-test key '" + safeText()
                    + "' is unbuilt");
        }
        int close = pos;
        expect(TokenType.PAREN_CLOSE);
        if (!seenData) {
            // ENGINE-VERBATIM (harvest TestMappingGrammarParser)
            throw TokenStreamCursor.throwAt(tokens, tS,
                    "Field 'data' is required");
        }
        if (query == null || expected == null || assertSpan == null) {
            throw TokenStreamCursor.throwAt(tokens, tS,
                    "legacy mapping test needs query AND assert");
        }
        return new Protocol.PLegacyMappingTest(name, query, inputData,
                expected, assertSpan, spanOf(tS, close));
    }

    private Protocol.PMappingTest parseMappingTest() {
        int tS = pos;
        String testId = parseIdentifier();
        expect(TokenType.COLON);
        expect(TokenType.BRACE_OPEN);
        List<Protocol.PStoreTestData> data = new ArrayList<>();
        List<Protocol.PTestAssertion> asserts = new ArrayList<>();
        String doc = null;
        while (!atEnd() && peek() != TokenType.BRACE_CLOSE) {
            String key = text();
            if ("doc".equals(key)) {
                advance();
                expect(TokenType.COLON);
                doc = consumeStringLiteral("'doc'");
                expect(TokenType.SEMI_COLON);
                continue;
            }
            if ("data".equals(key)) {
                advance();
                expect(TokenType.COLON);
                expect(TokenType.BRACKET_OPEN);
                while (!atEnd() && peek() != TokenType.BRACKET_CLOSE) {
                    data.add(parseStoreTestData());
                    match(TokenType.COMMA);
                }
                expect(TokenType.BRACKET_CLOSE);
                expect(TokenType.SEMI_COLON);
                continue;
            }
            if ("asserts".equals(key)) {
                advance();
                expect(TokenType.COLON);
                expect(TokenType.BRACKET_OPEN);
                while (!atEnd() && peek() != TokenType.BRACKET_CLOSE) {
                    asserts.add(parseTestAssertion());
                    match(TokenType.COMMA);
                }
                expect(TokenType.BRACKET_CLOSE);
                expect(TokenType.SEMI_COLON);
                continue;
            }
            throw error("mapping test key '" + safeText() + "' is unbuilt");
        }
        if (asserts.isEmpty()) {
            // ENGINE-VERBATIM (harvest TestMappingGrammarParser
            // testMappingTestSuites)
            throw TokenStreamCursor.throwAt(tokens, tS,
                    "Field 'asserts' is required");
        }
        int close = pos;
        expect(TokenType.BRACE_CLOSE);
        return new Protocol.PMappingTest(testId, doc, data, asserts,
                spanOf(tS, close));
    }

    /** {@code Store: ModelStore #{ path: ExternalFormat #{...}# }#}. */
    private Protocol.PStoreTestData parseStoreTestData() {
        int sS = pos;
        String storePath = Protocol.unquotePath(parseQualifiedName());
        SourceInfo storeSpan = spanOf(sS, pos - 1);
        expect(TokenType.COLON);
        if (peek() == TokenType.VALID_STRING && "Reference".equals(text())) {
            // Reference #{ my::DataElement }# — the pointer span runs the
            // Reference KEYWORD through the island close, like
            // ExternalFormat (probe reference-data)
            int refTok = pos;
            advance();
            IslandBlock refIsland = readIsland();
            MappingProtocolParser ri = new MappingProtocolParser(
                    refIsland.tokens(), 0, dialect);
            String dataPath = Protocol.unquotePath(ri.parseQualifiedName());
            Protocol.PPointer de = new Protocol.PPointer("DATA", dataPath,
                    new SourceInfo("",
                            tokens.startLine(refTok),
                            tokens.startColumn(refTok),
                            refIsland.endLine(),
                            refIsland.endColumn()));
            return new Protocol.PStoreTestData(
                    new Protocol.PPointer("STORE", storePath, storeSpan),
                    null, null, de, null, null, null,
                    new SourceInfo("", storeSpan.startLine(),
                            storeSpan.startColumn(), refIsland.endLine(),
                            refIsland.endColumn()));
        }
        if (peek() == TokenType.VALID_STRING && "Relation".equals(text())) {
            // Relation #{ schema.table: CSV rows...; }# (probe
            // relation-data-exact); cells TRIM both sides
            advance();
            IslandBlock ri = readIsland();
            List<Protocol.PRelationElement> rels =
                    parseRelationElements(ri, true);
            SourceInfo first = rels.isEmpty()
                    ? spanOf(pos - 1, pos - 1)
                    : rels.get(0).sourceInformation();
            SourceInfo accessor = new SourceInfo("", first.startLine(),
                    first.startColumn(), ri.endLine(), ri.endColumn() + 3);
            return new Protocol.PStoreTestData(
                    new Protocol.PPointer("STORE", storePath, storeSpan),
                    null, null, null, rels, accessor, null,
                    new SourceInfo("", storeSpan.startLine(),
                            storeSpan.startColumn(), ri.endLine(),
                            ri.endColumn()));
        }
        if (!(peek() == TokenType.VALID_STRING
                && "ModelStore".equals(text()))) {
            // Any OTHER kind is a plain embedded-data value. Engine's rule
            // is `mappingTestDataContent: qualifiedName COLON embeddedData`
            // with `embeddedData: identifier ISLAND_OPEN ...`
            // (MappingParserGrammar.g4:94-97), so the kind is open-ended and
            // `ModelStore: ExternalFormat #{...}#` is as legal as the three
            // shapes above — which are themselves just embedded-data kinds
            // that carry extra span detail. Refusing here was a gap, not a
            // rule.
            Protocol.PEmbeddedDataValue v = parseEmbeddedValue();
            SourceInfo vSi = embeddedSpan(v);
            return new Protocol.PStoreTestData(
                    new Protocol.PPointer("STORE", storePath, storeSpan),
                    null, null, null, null, null, v,
                    new SourceInfo("", storeSpan.startLine(),
                            storeSpan.startColumn(), vSi.endLine(),
                            vSi.endColumn()));
        }
        int msTok = pos;
        advance();                                  // ModelStore
        IslandBlock island = readIsland();
        List<Protocol.PModelData> modelData = new ArrayList<>();
        MappingProtocolParser inner = new MappingProtocolParser(
                island.tokens(), 0, dialect);
        while (!inner.atEnd()) {
            Protocol.PModelData entry = inner.parseModelEntry(island);
            requireUniqueModelType(modelData, entry);
            modelData.add(entry);
            inner.match(TokenType.COMMA);
        }
        // modelStore span = the ModelStore token .. the island close
        SourceInfo msSpan = new SourceInfo("",
                tokens.startLine(msTok), tokens.startColumn(msTok),
                island.endLine(), island.endColumn());
        return new Protocol.PStoreTestData(
                new Protocol.PPointer("STORE", storePath, storeSpan),
                modelData, msSpan, null, null, null, null,
                new SourceInfo("", storeSpan.startLine(),
                        storeSpan.startColumn(), island.endLine(),
                        island.endColumn()));
    }

    /** One {@code path: <value>} entry inside a ModelStore island — an
     *  embedded data value, or a {@code [ ^X(...) ]} INSTANCE collection
     *  parsed by the ###Pure expression machinery (probes test-suites,
     *  embedded-reference, data-section, model-instances). */
    /** ENGINE-VERBATIM (harvest TestDataGrammarParser
     *  testIncorrectModelStoreData): one ModelStore entry per type. */
    private void requireUniqueModelType(List<Protocol.PModelData> seen,
            Protocol.PModelData entry) {
        for (Protocol.PModelData d : seen) {
            if (d.model().equals(entry.model())) {
                // anchored at the DUPLICATE entry (reprobe pin #10)
                throw new com.legend.parser.ParseException(
                        "Multiple entries found for type: '"
                        + entry.model() + "'",
                        entry.sourceInformation().startLine(),
                        entry.sourceInformation().startColumn());
            }
        }
    }

    private Protocol.PModelData parseModelEntry(IslandBlock island) {
        int mS = pos;
        String model = Protocol.unquotePath(parseQualifiedName());
        expect(TokenType.COLON);
        if (peek() == TokenType.BRACKET_OPEN) {
            int lS = pos;
            skipBracketBlock();
            int close = pos - 1;
            List<com.legend.protocol.spec.ValueSpecification> vs =
                    SpecParser.parseCodeBlock(
                            island.tokens().slice(lS, pos), spanSourceId(),
                            dialect);
            if (vs.size() != 1) {
                throw error("model instance data must be ONE collection");
            }
            // the engine BUILDS this collection in its data walker rather
            // than taking it from the parse tree, so the outer node carries
            // no span — only the leaf literals do (probe model-instances)
            com.legend.protocol.spec.ValueSpecification instances =
                    vs.get(0) instanceof com.legend.protocol.spec.PureCollection c
                            ? new com.legend.protocol.spec.PureCollection(c.values())
                            : vs.get(0);
            return new Protocol.PModelInstanceData(model, instances,
                    new SourceInfo("", island.tokens().startLine(mS),
                            island.tokens().startColumn(mS),
                            island.tokens().endLine(close),
                            island.tokens().endColumn(close)));
        }
        Protocol.PEmbeddedDataValue val = parseEmbeddedValue();
        SourceInfo vSi = embeddedSpan(val);
        return new Protocol.PModelEmbeddedData(model, val,
                new SourceInfo("", island.tokens().startLine(mS),
                        island.tokens().startColumn(mS),
                        vSi.endLine(), vSi.endColumn()));
    }

    /** Skip a bracket-balanced block starting AT the open bracket. */
    private void skipBracketBlock() {
        expect(TokenType.BRACKET_OPEN);
        int depth = 0;
        while (!atEnd()) {
            TokenType t = peek();
            if (t == TokenType.BRACKET_OPEN || t == TokenType.PAREN_OPEN
                    || t == TokenType.BRACE_OPEN) {
                depth++;
            } else if (t == TokenType.PAREN_CLOSE
                    || t == TokenType.BRACE_CLOSE) {
                depth--;
            } else if (t == TokenType.BRACKET_CLOSE) {
                if (depth == 0) {
                    advance();
                    return;
                }
                depth--;
            }
            advance();
        }
        throw error("unterminated instance collection");
    }

    /** {@code id: EqualToJson #{ expected: ExternalFormat #{...}#; }#}. */
    /** One {@code id: EqualToJson #{...}#} assertion parsed off a HOST
     *  cursor — the service test-suite asserts seam. */
    public static Protocol.PTestAssertion parseTestAssertionAt(
            com.legend.parser.TokenStreamCursor host) {
        MappingProtocolParser p = new MappingProtocolParser(host.tokens(),
                host.pos(), host.dialect());
        Protocol.PTestAssertion v = p.parseTestAssertion();
        host.setPos(p.pos());
        return v;
    }

    /** One COMPACT-form assertion value ({@code => Relation #{...}#})
     *  parsed off a HOST cursor — the wire id is "default"
     *  (ZServiceV2Probe). */
    public static Protocol.PTestAssertion parseDefaultAssertionAt(
            com.legend.parser.TokenStreamCursor host) {
        MappingProtocolParser p = new MappingProtocolParser(host.tokens(),
                host.pos(), host.dialect());
        Protocol.PTestAssertion v = p.assertionValue("default");
        host.setPos(p.pos());
        return v;
    }

    private Protocol.PTestAssertion parseTestAssertion() {
        String assertId = parseIdentifier();
        expect(TokenType.COLON);
        return assertionValue(assertId);
    }

    /** The kind-dispatched assertion VALUE ({@code Relation #{...}#} /
     *  {@code EqualToJson #{...}#} / compact {@code ExternalFormat
     *  #{...}#}) — shared by the keyed form ({@code id: <value>}) and the
     *  compact form ({@code => <value>}). */
    private Protocol.PTestAssertion assertionValue(String assertId) {
        if (peek() == TokenType.VALID_STRING
                && "ExternalFormat".equals(text())) {
            // compact spelling of equalToJson: assertion AND expected share
            // the kind..}# span (ZServiceV2Probe "inline-compact")
            Protocol.PExternalFormatData ef = parseExternalFormat();
            match(TokenType.SEMI_COLON);
            return new Protocol.PTestAssertion(assertId, ef,
                    ef.sourceInformation());
        }
        if (peek() == TokenType.VALID_STRING && "Relation".equals(text())) {
            int rTok = pos;
            advance();
            int braceTok = pos;                     // the ISLAND_OPEN '#{'
            IslandBlock ri = readIsland();
            List<Protocol.PRelationElement> rels =
                    parseRelationElements(ri, false);
            if (rels.size() != 1) {
                throw error("Relation assertion must be ONE element");
            }
            match(TokenType.SEMI_COLON);
            // assertion expected spans run (braceLine, braceCol+2) ..
            // (semiLine-1, semiCol) — probe relation-rows; DATA elements
            // keep path..semi
            Protocol.PRelationElement re = rels.get(0);
            SourceInfo es = re.sourceInformation();
            Protocol.PRelationElement shifted =
                    new Protocol.PRelationElement(re.columns(), re.paths(),
                            re.rows(), new SourceInfo("",
                                    tokens.startLine(braceTok),
                                    tokens.startColumn(braceTok) + 2,
                                    es.endLine() - 1, es.endColumn()));
            return new Protocol.PTestAssertion(assertId, shifted,
                    new SourceInfo("", tokens.startLine(rTok),
                            tokens.startColumn(rTok), ri.endLine(),
                            ri.endColumn()));
        }
        if (peek() == TokenType.VALID_STRING && "EqualTo".equals(text())) {
            // EqualTo #{ expected: <value>; }# — wire _type:"equalTo",
            // expected is a SPEC value; span kind..island close (harvest
            // testServiceTestSuite)
            int eqTok = pos;
            advance();
            IslandBlock island = readIsland();
            MappingProtocolParser inner = new MappingProtocolParser(
                    island.tokens(), 0, dialect);
            if (!"expected".equals(inner.text())) {
                throw inner.error("assertion key '" + inner.safeText()
                        + "' is unbuilt");
            }
            inner.advance();
            inner.expect(TokenType.COLON);
            int vS = inner.pos();
            int d = 0;
            while (!inner.atEnd()) {
                TokenType tk = inner.peek();
                if (d == 0 && tk == TokenType.SEMI_COLON) {
                    break;
                }
                if (tk == TokenType.PAREN_OPEN || tk == TokenType.BRACE_OPEN
                        || tk == TokenType.BRACKET_OPEN) {
                    d++;
                } else if (tk == TokenType.PAREN_CLOSE
                        || tk == TokenType.BRACE_CLOSE
                        || tk == TokenType.BRACKET_CLOSE) {
                    d--;
                }
                inner.advance();
            }
            com.legend.protocol.spec.ValueSpecification value =
                    SpecParser.parse(inner.tokens().slice(vS, inner.pos()),
                            dialect);
            return new Protocol.PTestAssertion(assertId,
                    new Protocol.PEqualToValue(value),
                    new SourceInfo("", tokens.startLine(eqTok),
                            tokens.startColumn(eqTok), island.endLine(),
                            island.endColumn()));
        }
        if (!(peek() == TokenType.VALID_STRING
                && "EqualToJson".equals(text()))) {
            throw error("test assertion kind '" + safeText()
                    + "' is unbuilt");
        }
        int eqTok = pos;
        advance();                                  // EqualToJson
        IslandBlock island = readIsland();
        MappingProtocolParser inner = new MappingProtocolParser(
                island.tokens(), 0, dialect);
        if (!"expected".equals(inner.text())) {
            throw inner.error("assertion key '" + inner.safeText()
                    + "' is unbuilt");
        }
        inner.advance();
        inner.expect(TokenType.COLON);
        if (!(inner.peek() == TokenType.VALID_STRING
                && "ExternalFormat".equals(inner.text()))) {
            throw inner.error("assertion payload '" + inner.safeText()
                    + "' is unbuilt");
        }
        Protocol.PExternalFormatData ef = inner.parseExternalFormat();
        inner.match(TokenType.SEMI_COLON);
        return new Protocol.PTestAssertion(assertId, ef,
                new SourceInfo("", tokens.startLine(eqTok),
                        tokens.startColumn(eqTok), island.endLine(),
                        island.endColumn()));
    }

    /** One {@code { request: { method; url; }; response: { body:
     *  ExternalFormat #{...}#; }; }} service stub. Spans probed (ZTailProbe
     *  "servicestore-data" + the corpus DIFF that corrected it):
     *  request/response run their KEYWORD through the closing {@code ;};
     *  the stub spans its braces exactly. */
    /** {@code ExternalFormat #{ contentType: '...'; data: '...'; }#} —
     *  span ExternalFormat..}#. */
    private Protocol.PExternalFormatData parseExternalFormat() {
        int efTok = pos;
        advance();                                  // ExternalFormat
        IslandBlock island = readIsland();
        MappingProtocolParser inner = new MappingProtocolParser(
                island.tokens(), 0, dialect);
        String contentType = null;
        String dataStr = null;
        while (!inner.atEnd()) {
            String key = inner.text();
            inner.advance();
            inner.expect(TokenType.COLON);
            String v = TokenStreamCursor.unquoteAndUnescape(inner.text(),
                    inner);
            inner.advance();
            inner.expect(TokenType.SEMI_COLON);
            if ("contentType".equals(key)) {
                contentType = v;
            } else if ("data".equals(key)) {
                dataStr = v;
            } else {
                throw inner.error("external-format key '" + key
                        + "' is unbuilt");
            }
        }
        if (contentType == null || dataStr == null) {
            throw TokenStreamCursor.throwAt(tokens, efTok,
                    "external format needs contentType AND data");
        }
        return new Protocol.PExternalFormatData(contentType, dataStr,
                new SourceInfo("", tokens.startLine(efTok),
                        tokens.startColumn(efTok), island.endLine(),
                        island.endColumn()));
    }

    /** One {@code #{ ... }#} island: RE-LEXES the raw content with
     *  line/column padding so inner spans stay absolute (island content
     *  arrives as raw chunks; same emulation as the merge getText
     *  reparse). Nested islands re-lex recursively through the SAME
     *  machinery. */
    record IslandBlock(TokenStream tokens, int endLine,
            int endColumn, int contentStart, int contentEnd) { }

    IslandBlock readIsland() {
        expect(TokenType.ISLAND_OPEN);
        int contentStart = tokens.start(pos);
        int depth = 0;
        while (!atEnd()) {
            TokenType t = peek();
            if (t == TokenType.ISLAND_START) {
                depth++;
            } else if (t == TokenType.ISLAND_END) {
                if (depth == 0) {
                    break;
                }
                depth--;
            }
            advance();
        }
        int endTok = pos;
        int contentEnd = tokens.start(endTok);
        expect(TokenType.ISLAND_END);
        String source = tokens.source();
        // the stream's CACHED line index, not a char scan from offset 0
        // (deep-audit H2: the island path was O(K*N) — twelve call sites
        // each rescanned the whole prefix per island)
        int line = tokens.lineOf(contentStart);
        int col = tokens.columnOf(contentStart);
        StringBuilder padded = new StringBuilder(
                (line - 1) + (col - 1) + (contentEnd - contentStart));
        for (int i = 1; i < line; i++) {
            padded.append('\n');
        }
        for (int i = 1; i < col; i++) {
            padded.append(' ');
        }
        padded.append(source, contentStart, contentEnd);
        return new IslandBlock(
                com.legend.lexer.Lexer.tokenize(padded.toString()),
                tokens.endLine(endTok), tokens.endColumn(endTok),
                contentStart, contentEnd);
    }

    /** {@code cls[id]: AggregationAware { Views: [...] , ~mainMapping:
     *  Relational {...} }} — see probes agg-off-A/B for the two span
     *  shift rules (nested CMs +DELTA lines; lambdas -1 line). */
    private Protocol.PClassMappingAggregationAware parseAggregationAware(
            String target, int memberStart, SourceInfo targetSpan,
            @com.legend.Nullable String id, boolean root) {
        if (sectionStartLine < 0) {
            throw error("AggregationAware needs the section start line"
                    + " (span-shift emulation)");
        }
        String outerId = id != null ? id : target.replace("::", "_");
        int braceOpenTok = pos;
        expect(TokenType.BRACE_OPEN);
        List<boolean[]> canAggs = new ArrayList<>();
        List<List<com.legend.protocol.spec.ValueSpecification>> groupBys =
                new ArrayList<>();
        List<List<Protocol.PAggregateValue>> aggVals = new ArrayList<>();
        List<Integer> aggBodyToks = new ArrayList<>();
        int mainBodyTok = 0;                        // 0 = absent
        while (!atEnd() && peek() != TokenType.BRACE_CLOSE) {
            if (peek() == TokenType.VALID_STRING && "Views".equals(text())) {
                advance();
                expect(TokenType.COLON);
                expect(TokenType.BRACKET_OPEN);
                while (!atEnd() && peek() != TokenType.BRACKET_CLOSE) {
                    expect(TokenType.PAREN_OPEN);
                    int tildeTok = pos;
                    expect(TokenType.TILDE);
                    expectText("modelOperation");
                    expect(TokenType.COLON);
                    int braceTok = pos;
                    expect(TokenType.BRACE_OPEN);
                    // engine anchors the spec sub-parse at the TILDE line
                    // but slices text after the BRACE — everything inside
                    // shifts UP by the gap (AggregationAwareMapping
                    // ParseTreeWalker.visitModelOperationContext)
                    aggLambdaShift = tokens.startLine(braceTok)
                            - tokens.startLine(tildeTok);
                    boolean canAgg = false;
                    List<com.legend.protocol.spec.ValueSpecification> gbs =
                            new ArrayList<>();
                    List<Protocol.PAggregateValue> avs = new ArrayList<>();
                    while (!atEnd() && peek() != TokenType.BRACE_CLOSE) {
                        expect(TokenType.TILDE);
                        String cmd = parseIdentifier();
                        switch (cmd) {
                            case "canAggregate" -> {
                                canAgg = Boolean.parseBoolean(text());
                                advance();
                            }
                            case "groupByFunctions" -> {
                                expect(TokenType.PAREN_OPEN);
                                while (peek() != TokenType.PAREN_CLOSE
                                        && !atEnd()) {
                                    gbs.add(parseAggLambda());
                                    match(TokenType.COMMA);
                                }
                                expect(TokenType.PAREN_CLOSE);
                            }
                            case "aggregateValues" -> {
                                expect(TokenType.PAREN_OPEN);
                                while (peek() != TokenType.PAREN_CLOSE
                                        && !atEnd()) {
                                    expect(TokenType.PAREN_OPEN);
                                    expect(TokenType.TILDE);
                                    expectText("mapFn");
                                    expect(TokenType.COLON);
                                    com.legend.protocol.spec
                                            .ValueSpecification mapFn =
                                            parseAggLambda();
                                    expect(TokenType.COMMA);
                                    expect(TokenType.TILDE);
                                    expectText("aggregateFn");
                                    expect(TokenType.COLON);
                                    com.legend.protocol.spec
                                            .ValueSpecification aggFn =
                                            parseAggLambda();
                                    expect(TokenType.PAREN_CLOSE);
                                    avs.add(new Protocol.PAggregateValue(
                                            mapFn, aggFn));
                                    match(TokenType.COMMA);
                                }
                                expect(TokenType.PAREN_CLOSE);
                            }
                            default -> throw error("modelOperation command"
                                    + " '~" + cmd + "' is unbuilt");
                        }
                        match(TokenType.COMMA);
                    }
                    expect(TokenType.BRACE_CLOSE);
                    match(TokenType.COMMA);
                    expect(TokenType.TILDE);
                    expectText("aggregateMapping");
                    expect(TokenType.COLON);
                    boolean aggIsPure = peek() == TokenType.PURE_MAPPING;
                    if (aggIsPure) {
                        advance();
                    } else {
                        expect(TokenType.RELATIONAL);
                    }
                    canAggs.add(new boolean[]{canAgg});
                    groupBys.add(gbs);
                    aggVals.add(avs);
                    aggBodyToks.add(aggIsPure ? -pos : pos);
                    skipBraceBlock();
                    expect(TokenType.PAREN_CLOSE);
                    match(TokenType.COMMA);
                }
                expect(TokenType.BRACKET_CLOSE);
                match(TokenType.COMMA);
                continue;
            }
            if (peek() == TokenType.TILDE
                    && "mainMapping".equals(peekText(1))) {
                advance();
                advance();
                expect(TokenType.COLON);
                boolean mainIsPure = peek() == TokenType.PURE_MAPPING;
                if (mainIsPure) {
                    advance();
                } else {
                    expect(TokenType.RELATIONAL);
                }
                mainBodyTok = mainIsPure ? -pos : pos;
                skipBraceBlock();
                match(TokenType.COMMA);
                continue;
            }
            throw error("AggregationAware member '" + safeText()
                    + "' is unbuilt");
        }
        int close = pos;
        expect(TokenType.BRACE_CLOSE);
        if (mainBodyTok == 0) {
            throw error("AggregationAware without ~mainMapping");
        }
        // engine sub-parse shift (probes agg-off-A/B + corpus
        // mapping.pure/saleMapping): nested CM CONTENTS keep TRUE spans;
        // ONLY classSourceInformation (= the outer target span) and the
        // CM's sourceInformation (= the member span) shift DOWN by
        // DELTA = memberIslandBraceLine - sectionStartLine + 1
        int delta = tokens.startLine(braceOpenTok) - sectionStartLine + 1;
        SourceInfo shiftedTarget = shiftLines(targetSpan, delta);
        SourceInfo shiftedMember =
                shiftLines(spanOf(memberStart, close), delta);
        List<Protocol.PAggregateSetImplementation> asis = new ArrayList<>();
        for (int i = 0; i < aggBodyToks.size(); i++) {
            Protocol.PClassMapping cm = parseNested(aggBodyToks.get(i),
                    target, shiftedTarget,
                    outerId + "_Aggregate_" + i, id, root, shiftedMember);
            asis.add(new Protocol.PAggregateSetImplementation(
                    canAggs.get(i)[0], groupBys.get(i), aggVals.get(i), i,
                    cm));
        }
        Protocol.PClassMapping mainCm = parseNested(mainBodyTok,
                target, shiftedTarget, outerId + "_Main", id, root,
                shiftedMember);
        List<Protocol.PPurePropertyMapping> aggPms = new ArrayList<>();
        if (mainCm instanceof Protocol.PClassMappingPure pu) {
            // Pure main mappings COPY their pms onto the agg node
            // (walker:92-95, probe agg-pure-nested)
            aggPms.addAll(pu.propertyMappings());
        }
        return new Protocol.PClassMappingAggregationAware(target, outerId,
                asis, mainCm, aggPms, root, spanOf(memberStart, close));
    }

    private static SourceInfo shiftLines(SourceInfo si, int delta) {
        return new SourceInfo(si.sourceId(), si.startLine() + delta,
                si.startColumn(), si.endLine() + delta, si.endColumn());
    }

    /** A nested Relational CM parsed from the ORIGINAL stream (contents
     *  keep TRUE spans); its class span and OWN span are the SHIFTED
     *  outer spans (probe agg-off). */
    private Protocol.PClassMapping parseNested(int signedBodyTok,
            String target, SourceInfo shiftedTarget, String cmId,
            @com.legend.Nullable String explicitOuterId, boolean root,
            SourceInfo shiftedMember) {
        if (signedBodyTok < 0) {
            MappingProtocolParser p = new MappingProtocolParser(tokens,
                    -signedBodyTok, dialect);
            Protocol.PClassMappingPure cm = p.parsePureClassMapping(target,
                    -signedBodyTok, shiftedTarget, explicitOuterId, root,
                    null);
            return new Protocol.PClassMappingPure(cm.className(),
                    shiftedTarget, cm.extendsClassMappingId(), cmId,
                    cm.root(), cm.srcClass(),
                    cm.sourceClassSourceInformation(), cm.filter(),
                    cm.propertyMappings(), shiftedMember);
        }
        return parseNestedRel(signedBodyTok, target, shiftedTarget, cmId,
                explicitOuterId, root, shiftedMember);
    }

    private Protocol.PClassMappingRel parseNestedRel(int bodyTok,
            String target, SourceInfo shiftedTarget, String cmId,
            @com.legend.Nullable String explicitOuterId, boolean root,
            SourceInfo shiftedMember) {
        MappingProtocolParser p = new MappingProtocolParser(tokens, bodyTok, dialect);
        // pm sources keep the EXPLICIT outer id (or null) — the engine
        // suffixes the CM id only AFTER the sub-parse
        // (AggregationAwareMappingParseTreeWalker:218)
        Protocol.PClassMappingRel cm = p.parseRelationalClassMapping(target,
                bodyTok, shiftedTarget, explicitOuterId, root, null);
        return new Protocol.PClassMappingRel(cm.className(), shiftedTarget,
                cmId, cm.root(), cm.distinct(),
                cm.extendsClassMappingId(), cm.filter(), cm.groupBy(),
                cm.mainTable(), cm.primaryKey(), cm.propertyMappings(),
                shiftedMember);
    }

    /** One aggregate lambda expr — spans ride a CONSTANT -1 line shift
     *  (probe agg-off): pad-relex the slice one line short. */
    private com.legend.protocol.spec.ValueSpecification parseAggLambda() {
        int eS = pos;
        int depth = 0;
        while (!atEnd()) {
            TokenType t = peek();
            if (depth == 0 && (t == TokenType.COMMA
                    || t == TokenType.PAREN_CLOSE)) {
                break;
            }
            if (t == TokenType.PAREN_OPEN || t == TokenType.BRACKET_OPEN
                    || t == TokenType.BRACE_OPEN) {
                depth++;
            } else if (t == TokenType.PAREN_CLOSE
                    || t == TokenType.BRACKET_CLOSE
                    || t == TokenType.BRACE_CLOSE) {
                depth--;
            }
            advance();
        }
        StringBuilder padded = new StringBuilder();
        for (int i = 1; i < tokens.startLine(eS) - aggLambdaShift; i++) {
            padded.append('\n');
        }
        for (int i = 1; i < tokens.startColumn(eS); i++) {
            padded.append(' ');
        }
        padded.append(tokens.source(), tokens.start(eS),
                tokens.end(pos - 1));
        List<com.legend.protocol.spec.ValueSpecification> body =
                SpecParser.parseCodeBlock(
                        com.legend.lexer.Lexer.tokenize(padded.toString()),
                        dialect);
        if (body.size() != 1) {
            throw error("aggregate lambda must be ONE expression");
        }
        return body.get(0);
    }

    /** Quoted table spellings KEEP their quotes as the wire name (same
     *  rule as DatabaseProtocolParser.parseIdentifier). */
    private String parseMaybeQuotedIdentifier() {
        if (peek() == TokenType.QUOTED_STRING) {
            String raw = text();
            advance();
            return raw;
        }
        return parseIdentifier();
    }

    /** {@code [schema.table:]\n HEADER \n ROWS... ;} groups inside a
     *  Relation island — a CSV sub-format the engine parses from raw
     *  chars; cells TRIM both sides (probe relation-data-exact). */
    List<Protocol.PRelationElement> parseRelationElements(
            IslandBlock island, boolean withPaths) {
        String src = tokens.source();
        int p = island.contentStart();
        int end = island.contentEnd();
        List<Protocol.PRelationElement> out = new ArrayList<>();
        while (true) {
            while (p < end && Character.isWhitespace(src.charAt(p))) {
                p++;
            }
            if (p >= end) {
                break;
            }
            int elemStart = p;
            List<String> paths = new ArrayList<>();
            if (withPaths) {
                int eol = lineEnd(src, p, end);
                String line = src.substring(p, eol).strip();
                if (!line.endsWith(":")) {
                    throw error("relation data path line must end ':'");
                }
                splitOn(line.substring(0, line.length() - 1), '.', paths);
                p = eol + 1;
                while (p < end && Character.isWhitespace(src.charAt(p))) {
                    p++;
                }
            }
            int eol = lineEnd(src, p, end);
            List<String> columns = new ArrayList<>();
            String colLine = src.substring(p, eol).strip();
            List<List<String>> rows = new ArrayList<>();
            int semiPos = -1;
            if (colLine.endsWith(";")) {
                // header-only entry — zero rows (engine fixture
                // testRelationElementsDataDuplicateAccessors)
                semiPos = p + src.substring(p, eol).lastIndexOf(';');
                colLine = colLine.substring(0, colLine.length() - 1);
            }
            splitCells(colLine, columns);
            p = eol + 1;
            while (semiPos < 0 && p < end) {
                int rEol = lineEnd(src, p, end);
                String line = src.substring(p, rEol).strip();
                if (line.isEmpty()) {
                    p = rEol + 1;
                    continue;
                }
                boolean last = line.endsWith(";");
                if (last) {
                    semiPos = p + src.substring(p, rEol).lastIndexOf(';');
                    line = line.substring(0, line.length() - 1);
                }
                List<String> row = new ArrayList<>();
                splitCells(line, row);
                rows.add(row);
                p = rEol + 1;
                if (last) {
                    break;
                }
            }
            if (semiPos < 0) {
                throw error("relation data rows must end with ';'");
            }
            out.add(new Protocol.PRelationElement(columns, paths, rows,
                    new SourceInfo("", tokens.lineOf(elemStart),
                            tokens.columnOf(elemStart),
                            tokens.lineOf(semiPos),
                            tokens.columnOf(semiPos))));
        }
        return out;
    }

    private static int lineEnd(String src, int from, int end) {
        int i = from;
        while (i < end && src.charAt(i) != '\n') {
            i++;
        }
        return i;
    }

    /** One CSV line into cells. A {@code "} groups commas — but the engine's
     *  reader KEEPS the quotes in the value, so {@code "Doe, Jr"} is ONE cell
     *  whose text is still {@code "Doe, Jr"} (probe relation-quoted-cells).
     *  Trimming applies to the raw cell, hence never inside the quotes. */
    private static void splitCells(String s, List<String> out) {
        int start = 0;
        boolean quoted = false;
        for (int i = 0; i <= s.length(); i++) {
            if (i < s.length() && s.charAt(i) == '"') {
                quoted = !quoted;
            } else if (i == s.length() || (s.charAt(i) == ',' && !quoted)) {
                out.add(s.substring(start, i).strip());
                start = i + 1;
            }
        }
    }

    private static void splitOn(String s, char sep, List<String> out) {
        int start = 0;
        for (int i = 0; i <= s.length(); i++) {
            if (i == s.length() || s.charAt(i) == sep) {
                out.add(s.substring(start, i).strip());
                start = i + 1;
            }
        }
    }

    private void expectText(String want) {
        if (!want.equals(text())) {
            throw error("expected '" + want + "' but found '" + safeText()
                    + "'");
        }
        advance();
    }

    /** Skip a brace-balanced block starting AT the open brace. */
    private void skipBraceBlock() {
        expect(TokenType.BRACE_OPEN);
        int depth = 0;
        while (!atEnd()) {
            TokenType t = peek();
            if (t == TokenType.BRACE_OPEN) {
                depth++;
            } else if (t == TokenType.BRACE_CLOSE) {
                if (depth == 0) {
                    advance();
                    return;
                }
                depth--;
            }
            advance();
        }
        throw error("unclosed brace block");
    }

    /** One embedded op under the ACTIVE scope context (scope table >
     *  scope db > bare). */
    private Protocol.PRelOp parseOpInCtx(
            @com.legend.Nullable String scopeDb,
            DatabaseProtocolParser.@com.legend.Nullable ScopeCtx scope) {
        int[] posOut = new int[1];
        Protocol.PRelOp op;
        if (scope != null) {
            op = DatabaseProtocolParser.scopedOperationAt(tokens, pos, scope,
                    posOut, dialect);
        } else if (scopeDb != null) {
            op = DatabaseProtocolParser.operationAt(tokens, pos, scopeDb,
                    "default", posOut, dialect);
        } else {
            return parseEmbeddedOperation();
        }
        pos = posOut[0];
        return op;
    }

    /** One embedded relational operation via THE relational op grammar. */
    private Protocol.PRelOp parseEmbeddedOperation() {
        int[] posOut = new int[1];
        Protocol.PRelOp op = DatabaseProtocolParser.operationAt(tokens, pos,
                "", "default", posOut, dialect);
        pos = posOut[0];
        return op;
    }

    /** {@code path: EnumerationMapping id { V: [src, ...], ... }} — the
     *  mapping's span runs the TARGET path through the closing brace;
     *  pointer type ENUMERATION (probe enum-mapping). */
    /** Both references spell lists {@code x (COMMA x)*} — a comma PROMISES
     *  another entry, so a list closer right after one is a refusal
     *  (invention census batch 3; engine and pure M2 grammars have no
     *  trailing-comma arm anywhere in the mapping surface). */
    private void entryComma(TokenType... closers) {
        if (!match(TokenType.COMMA)) {
            return;
        }
        for (TokenType closer : closers) {
            if (peek() == closer) {
                throw error("trailing comma: a comma must be followed by"
                        + " another entry");
            }
        }
    }

    private Protocol.PEnumerationMapping parseEnumerationMapping(
            String target, int targetStart, SourceInfo targetSpan) {
        String id = peek() == TokenType.BRACE_OPEN ? null
                : parseMappingWord();
        expect(TokenType.BRACE_OPEN);
        List<Protocol.PEnumValueMapping> rows = new ArrayList<>();
        while (!atEnd() && peek() != TokenType.BRACE_CLOSE) {
            String enumValue = parseIdentifier();
            expect(TokenType.COLON);
            List<Protocol.PEnumSourceValue> sources = new ArrayList<>();
            if (peek() == TokenType.BRACKET_OPEN) {
                advance();
                while (peek() != TokenType.BRACKET_CLOSE && !atEnd()) {
                    sources.add(parseSourceValue());
                    entryComma(TokenType.BRACKET_CLOSE);
                }
                if (sources.isEmpty()) {
                    // `X: []`. Engine's enumerationMapping rule requires a
                    // sourceValue inside the brackets and fails on the ']'.
                    // Refuse HERE, where the position is known, rather than
                    // letting EnumValueMapping's constructor invariant fire
                    // an unpositioned IllegalArgumentException at build time.
                    throw error("enum value '" + enumValue + "' must list at"
                            + " least one source value");
                }
                expect(TokenType.BRACKET_CLOSE);
            } else {
                sources.add(parseSourceValue());
            }
            rows.add(new Protocol.PEnumValueMapping(enumValue, sources));
            entryComma(TokenType.BRACE_CLOSE);
        }
        int close = pos;
        expect(TokenType.BRACE_CLOSE);
        return new Protocol.PEnumerationMapping(id,
                new Protocol.PPointer("ENUMERATION", target, targetSpan),
                rows, spanOf(targetStart, close));
    }

    /** `[m]` bounds — the long-typed engine wire form shared by the three
     *  local-property sites (was three drifting copies, adversarial audit
     *  F8); stops BEFORE the closing ']' so callers can span it. Overflow
     *  refuses positioned via consumeLong. */
    private record MultBounds(long lower, @com.legend.Nullable Long upper) {
    }

    private MultBounds parseLongMultBounds() {
        expect(TokenType.BRACKET_OPEN);
        long lower;
        Long upper = null;
        if (peek() == TokenType.STAR) {
            advance();
            lower = 0L;
        } else {
            lower = consumeLong();
            upper = lower;
        }
        if (peek() == TokenType.DOT_DOT) {
            advance();
            if (peek() == TokenType.STAR) {
                advance();
                upper = null;
            } else {
                upper = consumeLong();
            }
        }
        return new MultBounds(lower, upper);
    }

    private Protocol.PEnumSourceValue parseSourceValue() {
        if (peek() == TokenType.STRING) {
            String v = TokenStreamCursor.unquoteAndUnescape(text(), this);
            advance();
            return new Protocol.PEnumSourceValue(null, v);
        }
        if (peek() == TokenType.INTEGER) {
            return new Protocol.PEnumSourceValue(null, consumeLong());
        }
        if (isIdentifierToken(peek())) {
            // my::Other.bla — an enum VALUE reference (probe
            // enum-source-enumref). The path admits KEYWORD identifiers
            // (a class named 'Enum' — engine fixture testEnumerationMapping)
            // and the value name may be QUOTED ('30_ACT').
            String enumPath = Protocol.unquotePath(parseQualifiedName());
            expect(TokenType.DOT);
            String v;
            if (peek() == TokenType.STRING) {
                v = TokenStreamCursor.unquoteAndUnescape(text(), this);
                advance();
            } else {
                v = parseIdentifier();
            }
            return new Protocol.PEnumSourceValue(enumPath, v);
        }
        throw error("unsupported enum source value: " + safeText());
    }
}
