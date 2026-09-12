// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser.section;

import com.legend.lexer.Lexer;
import com.legend.lexer.TokenStream;
import com.legend.lexer.TokenType;
import com.legend.parser.TokenStreamCursor;
import com.legend.protocol.Protocol;
import com.legend.protocol.ProtocolEmitter;
import com.legend.protocol.SourceInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * THE {@code ###Connection} grammar — the first built-in section behind the
 * {@link com.legend.spi.SectionGrammar} seam (SECTION_PROGRAM_HANDOFF.md
 * §2.6). Owns every connection spelling: the four standalone element flavors
 * ({@code RelationalDatabaseConnection}, {@code JsonModelConnection},
 * {@code XmlModelConnection}, {@code ModelChainConnection}) and the embedded
 * runtime-island form, which shares the exact same VALUE grammar — one
 * grammar, several feeds, so the divergence that let the old straight-to-model
 * twin mis-parse {@code testDataSetupSqls} arrays for months cannot recur.
 *
 * <p>Engine-parity shapes are probed byte-for-byte (ZConnectionProbe).
 * The former lite-only flavors (InMemory/LocalFile/NoAuth/url/database:/
 * literal UsernamePassword) are DELETED — conform-to-engine migration
 * (2026-08-10): in-process DuckDB spells the ENGINE's {@code DuckDB {
 * (path)* }} and test auth spells {@code Test} (ZMigrationTargetProbe,
 * oracle-verified).
 */
public final class ConnectionSectionGrammar implements LexableSectionGrammar {

    /** The one stateless instance the registry hands out. */
    public static final ConnectionSectionGrammar INSTANCE =
            new ConnectionSectionGrammar();

    /** DatabaseType at the 4.138.2 oracle pin — the walker refuses
     *  anything else via valueOf (javap'd from the oracle jar; re-check
     *  on an oracle bump). */
    private static final java.util.Set<String> DATABASE_TYPES =
            java.util.Set.of("DB2", "H2", "MemSQL", "Sybase", "SybaseIQ",
                    "Composite", "Postgres", "SqlServer", "Hive", "Snowflake",
                    "Presto", "Trino", "BigQuery", "Redshift", "Databricks",
                    "Spanner", "Athena", "DuckDB", "Oracle", "ClickHouse",
                    "Aurora");

    private ConnectionSectionGrammar() {
    }

    @Override
    public String name() {
        return "Connection";
    }

    /** The SPI feed: re-lex the raw section text and drive the shared parse,
     *  emitting each element's protocol JSON. Spans are slice-relative (line
     *  1 = the section's first content line); an external host maps them
     *  through its own walker offsets. */
    @Override
    public void parse(com.legend.spi.SectionSource src,
            com.legend.spi.ElementSink out) {
        Cursor c = new Cursor(Lexer.tokenize(src.text()), 0, 0, // SPI overlay hosting = the drop-in seam: engine grammar
                com.legend.parser.Dialect.LEGEND_ENGINE);
        while (!c.atEnd()) {
            if (c.peek() == TokenType.IMPORT) {
                SectionImports.parseImport(c);
                continue;
            }
            Protocol.PConnection pc = parseElement(c);
            out.accept(pc.qualifiedName(), ProtocolEmitter.emitElement(pc));
        }
    }

    @Override
    public ParsedSection parseSection(TokenStreamCursor host,
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
            elements.add(new ParsedElement(parseElement(host), at));
        }
        return new ParsedSection(elements, imports);
    }

    @Override
    public com.legend.model.PackageableElement toModel(Protocol.Element element) {
        try {
            return com.legend.model.FromProtocol.toConnectionElement(
                    (Protocol.PConnection) element);
        } catch (com.legend.model.FromProtocol.UnsupportedConnectionShape u) {
            throw new UnsupportedElementShape(u.reason());
        }
    }

    // ============================================================
    // Element envelope
    // ============================================================

    /**
     * One {@code <Flavor> qn { body }} connection element at the cursor.
     * The BODY grammar is shared with embedded runtime islands
     * ({@link #parseIslandValue}), so there is one connection grammar.
     * Envelope and value share one span (ZConnectionProbe).
     */
    public static Protocol.PConnection parseElement(TokenStreamCursor c) {
        int declStart = c.pos();
        String flavor = c.safeText();
        c.advance();
        String qn = Protocol.unquotePath(c.parseQualifiedName());
        int cut = qn.lastIndexOf("::");
        String pkg = cut < 0 ? "" : qn.substring(0, cut);
        String name = cut < 0 ? qn : qn.substring(cut + 2);
        Protocol.PConnectionValue value = parseValue(c, flavor, declStart, true);
        return new Protocol.PConnection(pkg, name, value,
                c.spanOf(declStart, c.pos() - 1));
    }

    /** An embedded runtime-island body ({@code #{ Flavor {...} }#} content,
     *  already extracted): re-lex with the engine's walker-offset rule (line
     *  offset every line, column offset first line only) so spans stay
     *  file-absolute. */
    public static Protocol.PConnectionValue parseIslandValue(String islandText,
            int baseLine, int baseColumn, com.legend.parser.Dialect dialect) {
        Cursor c = new Cursor(Lexer.tokenize(islandText),
                baseLine - 1, baseColumn - 1, dialect);
        String flavor = c.safeText();
        int fStart = c.pos();
        c.advance();
        return parseValue(c, flavor, fStart, false);
    }

    /**
     * One connection VALUE body {@code { key: ...; }} for {@code flavor}.
     * {@code standalone} controls the model-connection {@code element}
     * field ({@code "ModelStore"} for section elements, absent embedded).
     * Corpus-censused shapes plus the named lite extensions; anything else
     * refuses loudly.
     */
    public static Protocol.PConnectionValue parseValue(TokenStreamCursor c,
            String flavor, int declStart, boolean standalone) {
        return switch (flavor) {
            case "JsonModelConnection", "XmlModelConnection" -> {
                ModelConnBody body = parseModelConnectionBody(c, declStart);
                com.legend.protocol.SourceInfo span =
                        c.spanOf(declStart, c.pos() - 1);
                yield "JsonModelConnection".equals(flavor)
                        ? new Protocol.PJsonModelConnection(
                                body.cls(), body.clsSpan(),
                                standalone ? "ModelStore" : null, body.url(), span)
                        : new Protocol.PXmlModelConnection(
                                body.cls(), body.clsSpan(),
                                standalone ? "ModelStore" : null, body.url(), span);
            }
            case "ModelChainConnection" -> parseModelChainBody(c, declStart,
                    standalone);
            case "RelationalDatabaseConnection" ->
                    parseRelationalConnectionBody(c, declStart);
            case "ServiceStoreConnection" ->
                    parseServiceStoreConnectionBody(c, declStart);
            case "DeephavenConnection" -> parseDeephavenConnectionBody(c);
            case "MongoDBConnection" -> parseMongoConnectionBody(c, declStart);
            case "Elasticsearch7ClusterConnection" ->
                    parseElasticsearchBody(c);
            // ANCHORED at the element (engine walker: sourceInformation of
            // the CONNECTION ctx — position-exactness lane 2026-08-13)
            default -> throw com.legend.parser.TokenStreamCursor.throwAt(
                    c.tokens(),
                    c.peek() == TokenType.BRACE_OPEN ? c.pos() + 1 : c.pos(),
                    "unsupported connection flavor: " + flavor);
        };
    }

    private record ModelConnBody(String cls,
            com.legend.protocol.SourceInfo clsSpan, String url) {
    }

    private static ModelConnBody parseModelConnectionBody(TokenStreamCursor c,
            int declStart) {
        c.expect(TokenType.BRACE_OPEN);
        String cls = null;
        com.legend.protocol.SourceInfo clsSpan = null;
        String url = null;
        java.util.Set<String> seenKeys = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seenKeys, key, c, declStart);
            c.expect(TokenType.COLON);
            if ("class".equals(key)) {
                int cS = c.pos();
                cls = Protocol.unquotePath(c.parseQualifiedName());
                clsSpan = c.spanOf(cS, c.pos() - 1);
            } else if ("url".equals(key)) {
                if (url != null) {
                    // ENGINE-VERBATIM (harvest TestConnectionGrammarParser)
                    throw TokenStreamCursor.throwAt(c.tokens(), declStart,
                            "Field 'url' should be specified only once");
                }
                String quoted = c.text();
                c.expect(TokenType.STRING);
                url = TokenStreamCursor.unquoteAndUnescape(quoted, c);
            } else {
                throw c.error("unknown model-connection key: " + key);
            }
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        if (cls == null || clsSpan == null || url == null) {
            throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(),
                    declStart, "model connection needs class and url");
        }
        return new ModelConnBody(cls, clsSpan, url);
    }

    private static Protocol.PModelChainConnection parseModelChainBody(
            TokenStreamCursor c, int declStart, boolean standalone) {
        c.expect(TokenType.BRACE_OPEN);
        List<String> mappings = new ArrayList<>();
        com.legend.protocol.SourceInfo mapSpan = null;
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            int kS = c.pos();
            String key = c.safeText();
            if (c.peek() != TokenType.MAPPINGS) {
                throw c.error("unknown ModelChainConnection key: " + key);
            }
            if (mapSpan != null) {
                // ENGINE-VERBATIM (harvest TestConnectionGrammarParser)
                throw com.legend.parser.TokenStreamCursor.throwAt(
                        c.tokens(), declStart,
                        "Field 'mappings' should be specified only once");
            }
            c.advance();
            c.expect(TokenType.COLON);
            c.expect(TokenType.BRACKET_OPEN);
            while (c.peek() != TokenType.BRACKET_CLOSE && !c.atEnd()) {
                mappings.add(Protocol.unquotePath(c.parseQualifiedName()));
                c.match(TokenType.COMMA);
            }
            c.expect(TokenType.BRACKET_CLOSE);
            c.match(TokenType.SEMI_COLON);      // span INCLUDES the ';' (probe)
            mapSpan = c.spanOf(kS, c.pos() - 1);
        }
        c.expect(TokenType.BRACE_CLOSE);
        if (mapSpan == null) {
            throw com.legend.parser.TokenStreamCursor.throwAt(
                    c.tokens(), declStart, "ModelChainConnection needs mappings");
        }
        return new Protocol.PModelChainConnection(
                standalone ? "ModelStore" : null, mappings, mapSpan,
                c.spanOf(declStart, c.pos() - 1));
    }

    /** {@code { store: qn; baseUrl: 'url'; }} (ZTailProbe
     *  "servicestore-conn"). */
    private static Protocol.PServiceStoreConnection
            parseServiceStoreConnectionBody(TokenStreamCursor c, int declStart) {
        c.expect(TokenType.BRACE_OPEN);
        String element = null;
        com.legend.protocol.SourceInfo elementSpan = null;
        String baseUrl = null;
        java.util.Set<String> seenKeys2 = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seenKeys2, key, c);
            c.expect(TokenType.COLON);
            switch (key) {
                case "store" -> {
                    int eS = c.pos();
                    element = Protocol.unquotePath(c.parseQualifiedName());
                    elementSpan = c.spanOf(eS, c.pos() - 1);
                }
                case "baseUrl" -> {
                    baseUrl = SectionParse.stringValue(c);
                    if (baseUrl.endsWith("/")) {
                        // engine-verbatim (sectioned negative pin #6)
                        throw c.error("baseUrl should not end with '/'");
                    }
                }
                default -> throw c.error(
                        "unknown ServiceStoreConnection key: " + key);
            }
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        if (baseUrl == null) {
            throw com.legend.parser.TokenStreamCursor.throwAt(
                    c.tokens(), declStart, "ServiceStoreConnection needs baseUrl");
        }
        return new Protocol.PServiceStoreConnection(baseUrl, element,
                elementSpan, c.spanOf(declStart, c.pos() - 1));
    }

    /** {@code { store: qn; serverUrl: 'url' authentication: # PSK { psk:
     *  'v'; }#; }} — serverUrl takes NO semicolon in the corpus spelling;
     *  the value span runs the FIRST body key through the island close
     *  (ZTailProbe "deephaven-conn"). */
    private static Protocol.PDeephavenConnection
            parseDeephavenConnectionBody(TokenStreamCursor c) {
        c.expect(TokenType.BRACE_OPEN);
        int bodyStart = c.pos();
        String element = null;
        com.legend.protocol.SourceInfo elementSpan = null;
        String serverUrl = null;
        String psk = null;
        int islandEndTok = -1;
        java.util.Set<String> seenKeys3 = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seenKeys3, key, c);
            c.expect(TokenType.COLON);
            switch (key) {
                case "store" -> {
                    int eS = c.pos();
                    element = Protocol.unquotePath(c.parseQualifiedName());
                    elementSpan = c.spanOf(eS, c.pos() - 1);
                    c.expect(TokenType.SEMI_COLON);
                }
                case "serverUrl" -> {
                    // serverUrlDefinition has NO SEMI_COLON in the .g4 —
                    // a trailing ';' is an engine refusal (sibling
                    // negative neg-deephavenconnection-serverurl-semicolon)
                    serverUrl = SectionParse.stringValue(c);
                    if (c.peek() == TokenType.SEMI_COLON) {
                        throw c.error("Unexpected token");
                    }
                }
                case "authentication" -> {
                    if (c.peek() != TokenType.ISLAND_OPEN) {
                        throw c.error("DeephavenConnection authentication"
                                + " must be a # PSK {...}# island");
                    }
                    String kind = islandKind(c);
                    if (!"PSK".equals(kind)) {
                        throw c.error("unsupported Deephaven auth kind: "
                                + kind);
                    }
                    IslandParse ip = reLexIsland(c);
                    islandEndTok = ip.endTok();
                    Cursor ic = ip.cursor();
                    String ik = ic.parseIdentifier();
                    ic.expect(TokenType.COLON);
                    if (!"psk".equals(ik)) {
                        throw ic.error("unknown PSK key: " + ik);
                    }
                    psk = SectionParse.stringValue(ic);
                    ic.expect(TokenType.SEMI_COLON);
                    // pskAuthentication has no EOF — trailing island
                    // tokens are ignored, first psk wins (same rule as
                    // the ES PSK island)
                    while (!ic.atEnd()) {
                        ic.advance();
                    }
                    c.expect(TokenType.SEMI_COLON);
                }
                default -> throw c.error(
                        "unknown DeephavenConnection key: " + key);
            }
        }
        int closeTok = c.pos();
        c.expect(TokenType.BRACE_CLOSE);
        if (serverUrl == null || psk == null) {
            throw c.error("DeephavenConnection needs serverUrl and"
                    + " authentication");
        }
        // the engine reparses the connection VALUE with a +4 column
        // offset that leaks into the ctx END: span = first body token
        // through the outer '}' at closeCol+4 (probe matrix 2026-08-14 —
        // the old island-'}' cross-product was a coincidental fit; same
        // overshoot family as the ES connection)
        var sp = c.spanOf(bodyStart, closeTok);
        com.legend.protocol.SourceInfo vSpan =
                com.legend.protocol.SpanOrigin.anchoredOvershoot(sp,
                        new com.legend.protocol.SourceInfo("",
                                sp.startLine(), sp.startColumn(),
                                sp.endLine(),
                                c.tokens().startColumn(closeTok)), 4);
        return new Protocol.PDeephavenConnection(serverUrl, psk, element,
                elementSpan, vSpan);
    }

    /** {@code { database: id; store: qn; serverURLs: [host:port,...];
     *  authentication: # UserPassword {...}#; }} (ZTailProbe
     *  "mongodb-conn"). */
    private static Protocol.PMongoDbConnection parseMongoConnectionBody(
            TokenStreamCursor c, int declStart) {
        c.expect(TokenType.BRACE_OPEN);
        String database = null;
        String element = null;
        com.legend.protocol.SourceInfo elementSpan = null;
        List<Protocol.PMongoServerUrl> urls = new ArrayList<>();
        Protocol.PAuthSpecValue auth = null;
        java.util.Set<String> seenKeys4 = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            int keyTok = c.pos();
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seenKeys4, key, c);
            c.expect(TokenType.COLON);
            switch (key) {
                case "database" -> database = c.parseIdentifier();
                case "store" -> {
                    int eS = c.pos();
                    element = Protocol.unquotePath(c.parseQualifiedName());
                    // MIXED ./:: separators are engine-legal here (mutant
                    // package-dot probes: fixture::mongo.Db AND
                    // fixture.mongo::Db both accept)
                    while (c.peek() == TokenType.DOT) {
                        c.advance();
                        element += "." + Protocol.unquotePath(
                                c.parseQualifiedName());
                    }
                    elementSpan = c.spanOf(eS, c.pos() - 1);
                }
                case "serverURLs" -> {
                    c.expect(TokenType.BRACKET_OPEN);
                    while (!c.atEnd() && c.peek() != TokenType.BRACKET_CLOSE) {
                        // HOST_STRING is [a-zA-Z0-9-.]+ in the engine
                        // lexer — dashes and dots ride unquoted (probe
                        // t2-mongodb f12345-001.ab.com)
                        int hS = c.pos();
                        while (!c.atEnd() && c.peek() != TokenType.COLON) {
                            c.advance();
                        }
                        String host = c.reconstructText(hS, c.pos());
                        c.expect(TokenType.COLON);
                        long port = c.consumeLong();
                        urls.add(new Protocol.PMongoServerUrl(host, port));
                        c.match(TokenType.COMMA);
                    }
                    c.expect(TokenType.BRACKET_CLOSE);
                }
                case "authentication" ->
                        auth = parseUserPasswordAuthIsland(c, keyTok);
                default -> throw c.error(
                        "unknown MongoDBConnection key: " + key);
            }
            // every field rule ends SEMI_COLON in the .g4 (mutant
            // drop-semicolon probe: engine refuses without it)
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        if (database == null || auth == null) {
            throw c.error("MongoDBConnection needs database and"
                    + " authentication");
        }
        if (urls.isEmpty()) {
            // walker-required (mutant delete-field probe)
            throw TokenStreamCursor.throwAt(c.tokens(), declStart,
                    "Field 'serverURLs' is required");
        }
        return new Protocol.PMongoDbConnection(database, urls, auth, element,
                elementSpan, c.spanOf(declStart, c.pos() - 1));
    }

    /** The DSL type between {@code #} and {@code {} of the island opener at
     *  the cursor (e.g. {@code # PSK {} → "PSK"}), NOT consumed. */
    private static String islandKind(TokenStreamCursor c) {
        String t = c.text();
        int brace = t.indexOf('{');
        return t.substring(1, brace < 0 ? t.length() : brace).trim();
    }

    /** Consume the island at the cursor and re-lex its content with walker
     *  offsets so inner spans stay file-absolute. */
    private record IslandParse(Cursor cursor, int endTok) {
    }

    private static IslandParse reLexIsland(TokenStreamCursor c) {
        c.advance();                                // ISLAND_OPEN
        int embStart = c.pos();
        int depth = 0;
        while (!c.atEnd()) {
            TokenType t = c.peek();
            if (t == TokenType.ISLAND_START) {
                depth++;
            } else if (t == TokenType.ISLAND_END) {
                if (depth == 0) {
                    break;
                }
                depth--;
            }
            c.advance();
        }
        String emb = c.reconstructText(embStart, c.pos());
        int endTok = c.pos();
        c.expect(TokenType.ISLAND_END);
        Cursor ic = new Cursor(Lexer.tokenize(emb),
                c.tokens().startLine(embStart) - 1,
                c.tokens().startColumn(embStart) - 1, c.dialect());
        return new IslandParse(ic, endTok);
    }

    private static Protocol.PRelationalDatabaseConnection
            parseRelationalConnectionBody(TokenStreamCursor c, int declStart) {
        c.expect(TokenType.BRACE_OPEN);
        String element = null;
        com.legend.protocol.SourceInfo elementSpan = null;
        String dbType = null;
        Protocol.PDatasourceSpec spec = null;
        Protocol.PAuthStrategy auth = null;
        List<Protocol.PPostProcessor> posts = new ArrayList<>();
        Boolean quoteIdentifiers = null;
        Long queryTimeOut = null;
        List<Protocol.PGenerationFeaturesConfig> queryGenConfigs = null;
        String timeZone = null;
        boolean localMode = false;
        java.util.Set<String> seenKeys = new java.util.HashSet<>();
        java.util.Set<String> seenKeys5 = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            int kS = c.pos();
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seenKeys5, key, c, declStart);
            c.expect(TokenType.COLON);
            switch (key) {
                case "store" -> {
                    int eS = c.pos();
                    element = Protocol.unquotePath(c.parseQualifiedName());
                    elementSpan = c.spanOf(eS, c.pos() - 1);
                    c.expect(TokenType.SEMI_COLON);
                }
                case "type" -> {
                    int tTok = c.pos();
                    dbType = c.parseIdentifier();
                    // DatabaseType.valueOf in the walker — enum javap'd
                    // from the 4.138.2 oracle jar (21 values); SQLite is
                    // a DECLARED lite extension (same gate as BOOLEAN/BOOL
                    // columns), so it refuses only on the drop-in surface
                    if (!DATABASE_TYPES.contains(dbType)
                            && !(!c.dialect().refusesLiteExtensions()
                                    && "SQLite".equals(dbType))) {
                        throw TokenStreamCursor.throwAt(c.tokens(), tTok,
                                "Unknown database type '" + dbType + "'");
                    }
                    c.expect(TokenType.SEMI_COLON);
                }
                case "specification" -> spec = parseDatasourceSpec(c, kS);
                case "auth" -> auth = parseAuthStrategy(c, kS);
                case "postProcessors" -> parseMapperPostProcessors(c, posts);
                case "quoteIdentifiers" -> {
                    quoteIdentifiers = parseBoolean(c);
                    c.expect(TokenType.SEMI_COLON);
                }
                case "queryTimeOutInSeconds" -> {
                    // wire: bare integer (harvest f_qto probe)
                    queryTimeOut = c.consumeLong();
                    c.expect(TokenType.SEMI_COLON);
                }
                case "queryGenerationConfigs" -> {
                    // [ GenerationFeaturesConfig { enabled: ['..'];
                    //   disabled: ['..']; } ] (harvest f_qgc probe)
                    queryGenConfigs = new ArrayList<>();
                    c.expect(TokenType.BRACKET_OPEN);
                    while (c.peek() != TokenType.BRACKET_CLOSE
                            && !c.atEnd()) {
                        int gS = c.pos();
                        String gKind = c.parseIdentifier();
                        if (!"GenerationFeaturesConfig".equals(gKind)) {
                            throw c.error("unknown query generation config: "
                                    + gKind);
                        }
                        c.expect(TokenType.BRACE_OPEN);
                        List<String> enabled = new ArrayList<>();
                        List<String> disabled = new ArrayList<>();
                        java.util.Set<String> seenGk =
                                new java.util.HashSet<>();
                        while (c.peek() != TokenType.BRACE_CLOSE
                                && !c.atEnd()) {
                            String gk = c.parseIdentifier();
                            c.expect(TokenType.COLON);
                            if (!seenGk.add(gk)) {
                                throw com.legend.parser.TokenStreamCursor
                                        .throwAt(c.tokens(), gS, "Field '" + gk
                                        + "' should be specified only once");
                            }
                            List<String> into = switch (gk) {
                                case "enabled" -> enabled;
                                case "disabled" -> disabled;
                                default -> throw c.error(
                                        "unknown GenerationFeaturesConfig"
                                                + " key: " + gk);
                            };
                            c.expect(TokenType.BRACKET_OPEN);
                            while (c.peek() != TokenType.BRACKET_CLOSE
                                    && !c.atEnd()) {
                                into.add(SectionParse.stringValue(c));
                                c.match(TokenType.COMMA);
                            }
                            c.expect(TokenType.BRACKET_CLOSE);
                            c.expect(TokenType.SEMI_COLON);
                        }
                        int gE = c.pos();
                        c.expect(TokenType.BRACE_CLOSE);
                        queryGenConfigs.add(
                                new Protocol.PGenerationFeaturesConfig(
                                        enabled, disabled,
                                        c.spanOf(gS, gE)));
                        c.match(TokenType.COMMA);
                    }
                    c.expect(TokenType.BRACKET_CLOSE);
                    c.expect(TokenType.SEMI_COLON);
                }
                case "timezone" -> {
                    if (c.peek() == TokenType.STRING) {
                        // a QUOTED zone id: the quotes are grammar syntax,
                        // not part of the id — the engine strips them
                        // since 4.145.0 (RelationalParseTreeWalker:
                        // "'US/Arizona' names no zone"); until 4.138.2 the
                        // wire carried them verbatim
                        timeZone = TokenStreamCursor.unquoteAndUnescape(c.text(), c);
                        c.advance();
                    } else {
                        // UNQUOTED offset: +0700 / -0500 (harvest
                        // testTimezoneConfiguration) — sign + digits
                        StringBuilder z = new StringBuilder();
                        if (c.peek() == TokenType.PLUS) {
                            z.append('+');
                            c.advance();
                        } else if (c.peek() == TokenType.MINUS) {
                            z.append('-');
                            c.advance();
                        }
                        z.append(c.text());
                        c.expect(TokenType.INTEGER);
                        timeZone = z.toString();
                    }
                    c.expect(TokenType.SEMI_COLON);
                }
                case "mode" -> {
                    String m = c.parseIdentifier();
                    if (!"local".equals(m)) {
                        throw c.error("unknown connection mode: " + m);
                    }
                    localMode = true;
                    c.expect(TokenType.SEMI_COLON);
                }
                default -> throw c.error(
                        "unknown RelationalDatabaseConnection key: " + key);
            }
        }
        c.expect(TokenType.BRACE_CLOSE);
        if (localMode && "Snowflake".equals(dbType) && element != null) {
            // mode:local — the snowflake extension SYNTHESIZES spec+auth
            // from the store path (probed live; note the 'publicuserName'
            // spelling inside the VALUE)
            String tail = element.replace("::", "-");
            spec = new Protocol.PSnowflakeSpec(
                    "legend-local-snowflake-accountName-" + tail,
                    null, "legend-local-snowflake-cloudType-" + tail,
                    "legend-local-snowflake-databaseName-" + tail,
                    null, null, null, null, null, null,
                    "legend-local-snowflake-region-" + tail,
                    "legend-local-snowflake-role-" + tail, null, null,
                    "legend-local-snowflake-warehouseName-" + tail, null);
            auth = new Protocol.PSnowflakePublic(
                    "legend-local-snowflake-passphraseVaultReference-" + tail,
                    "legend-local-snowflake-privateKeyVaultReference-" + tail,
                    "legend-local-snowflake-publicuserName-" + tail, null);
        }
        if (dbType == null || spec == null) {
            // store: is OPTIONAL (probe test-auth-empty-body-no-store —
            // element+span omitted from the wire entirely)
            throw com.legend.parser.TokenStreamCursor.throwAt(
                    c.tokens(), declStart, "RelationalDatabaseConnection needs type and"
                    + " specification");
        }
        if (auth == null) {
            // ENGINE-TRUE: auth is required (the omit-defaults-to-NoAuth
            // tolerance died with the lite flavors — conform-to-engine)
            throw com.legend.parser.TokenStreamCursor.throwAt(
                    c.tokens(), declStart, "RelationalDatabaseConnection needs auth");
        }
        return new Protocol.PRelationalDatabaseConnection(
                auth, dbType, spec, element, elementSpan,
                localMode ? Boolean.TRUE : null, posts,
                queryGenConfigs, queryTimeOut,
                quoteIdentifiers, timeZone, c.spanOf(declStart, c.pos() - 1));
    }

    /** {@code proxyPort} admits an UNQUOTED integer (engine grammar
     *  INTEGER | STRING); the wire carries a string either way. */
    private static String stringOrInt(TokenStreamCursor c) {
        if (c.peek() == TokenType.INTEGER) {
            String v = c.text();
            c.advance();
            return v;
        }
        return SectionParse.stringValue(c);
    }

    private static Boolean parseBoolean(TokenStreamCursor c) {
        if (c.peek() == TokenType.TRUE) {
            c.advance();
            return Boolean.TRUE;
        }
        if (c.peek() == TokenType.FALSE) {
            c.advance();
            return Boolean.FALSE;
        }
        throw c.error("expected true or false, got " + c.safeText());
    }

    /** One {@code key: 'string';} value. */

    /** {@code postProcessors: [ mapper { mappers: [ table {...}, schema
     *  {...} ]; } ]} — only the mapper flavor exists in the corpus; table
     *  mappers carry from/to/schemaFrom/schemaTo (probe post-processors). */
    private static void parseMapperPostProcessors(TokenStreamCursor c,
            List<Protocol.PPostProcessor> out) {
        c.expect(TokenType.BRACKET_OPEN);
        while (c.peek() != TokenType.BRACKET_CLOSE && !c.atEnd()) {
            int kindTok = c.pos();
            String kind = c.parseIdentifier();
            if ("relationalMapper".equals(kind)) {
                // relationalMapper { qn, qn } — a pointer per path, typed
                // QUERYPOSTPROCESSOR (harvest testRelationalMapperRoundTrip).
                // The engine re-parses spec.getText() — keyword + '{' +
                // raw island content with the HIDDEN whitespace between
                // keyword and '{' dropped — offset by the KEYWORD token
                // (PostProcessorParseTreeWalker + ParseTreeWalkerSource
                // Information.offset), so content lines compose one line
                // per keyword..'{' line break HIGH.
                int braceTok = c.pos();
                c.expect(TokenType.BRACE_OPEN);
                int shift = c.spanOf(braceTok, braceTok).startLine()
                        - c.spanOf(kindTok, kindTok).startLine();
                List<Protocol.PPointer> ptrs = new ArrayList<>();
                while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                    int at = c.pos();
                    String qn = Protocol.unquotePath(c.parseQualifiedName());
                    SourceInfo real = c.spanOf(at, c.pos() - 1);
                    ptrs.add(new Protocol.PPointer("QUERYPOSTPROCESSOR", qn,
                            new SourceInfo(real.sourceId(),
                                    real.startLine() - shift,
                                    real.startColumn(),
                                    real.endLine() - shift,
                                    real.endColumn())));
                    c.match(TokenType.COMMA);
                }
                c.expect(TokenType.BRACE_CLOSE);
                out.add(new Protocol.PRelationalMapperPostProcessor(ptrs));
                c.match(TokenType.COMMA);
                continue;
            }
            if (!"mapper".equals(kind)) {
                throw c.error("unsupported postProcessor flavor: " + kind);
            }
            c.expect(TokenType.BRACE_OPEN);
            List<Protocol.PMapper> mappers = new ArrayList<>();
            java.util.Set<String> seenKeys6 = new java.util.HashSet<>();
            while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                String key = c.parseIdentifier();
                TokenStreamCursor.once(seenKeys6, key, c);
                c.expect(TokenType.COLON);
                if (!"mappers".equals(key)) {
                    throw c.error("unknown mapper key: " + key);
                }
                c.expect(TokenType.BRACKET_OPEN);
                while (c.peek() != TokenType.BRACKET_CLOSE && !c.atEnd()) {
                    mappers.add(parseOneMapper(c));
                    c.match(TokenType.COMMA);
                }
                c.expect(TokenType.BRACKET_CLOSE);
                c.expect(TokenType.SEMI_COLON);
            }
            c.expect(TokenType.BRACE_CLOSE);
            out.add(new Protocol.PMapperPostProcessor(mappers));
            c.match(TokenType.COMMA);
        }
        c.expect(TokenType.BRACKET_CLOSE);
        c.expect(TokenType.SEMI_COLON);
    }

    private static Protocol.PMapper parseOneMapper(TokenStreamCursor c) {
        String flavor = c.parseIdentifier();
        c.expect(TokenType.BRACE_OPEN);
        java.util.Map<String, String> kv = new java.util.HashMap<>();
        java.util.Set<String> seenKeys7 = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seenKeys7, key, c);
            c.expect(TokenType.COLON);
            String quoted = c.text();
            c.expect(TokenType.STRING);
            kv.put(key, TokenStreamCursor.unquoteAndUnescape(quoted, c));
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        // per-flavor key sets are CLOSED in the .g4: schema takes only
        // from/to (sibling negative neg-connection-schemafrom-in-schema-
        // mapper), table adds schemaFrom/schemaTo
        java.util.Set<String> allowed = "table".equals(flavor)
                ? java.util.Set.of("from", "to", "schemaFrom", "schemaTo")
                : java.util.Set.of("from", "to");
        for (String k : kv.keySet()) {
            if (!allowed.contains(k)) {
                throw c.error("unknown " + flavor + " mapper key: " + k);
            }
        }
        String from = kv.get("from");
        String to = kv.get("to");
        if (from == null || to == null) {
            throw c.error("mapper needs from and to");
        }
        return switch (flavor) {
            case "table" -> {
                String sf = kv.get("schemaFrom");
                String st = kv.get("schemaTo");
                if (sf == null || st == null) {
                    throw c.error("table mapper needs schemaFrom and schemaTo");
                }
                yield new Protocol.PTableMapper(from, to, sf, st);
            }
            case "schema" -> new Protocol.PSchemaMapper(from, to);
            default -> throw c.error("unsupported mapper flavor: " + flavor);
        };
    }

    /** Spec span runs the {@code specification} KEYWORD through the closing
     *  token of the body (probes: LocalH2 one-line and Static multi-line). */
    private static Protocol.PDatasourceSpec parseDatasourceSpec(
            TokenStreamCursor c, int keywordTok) {
        String kind = c.parseIdentifier();
        return switch (kind) {
            case "LocalH2" -> {
                List<String> sqls = null;
                String csv = null;
                c.expect(TokenType.BRACE_OPEN);
                java.util.Set<String> seenKeys8 = new java.util.HashSet<>();
                while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                    String key = c.parseIdentifier();
                    TokenStreamCursor.once(seenKeys8, key, c, keywordTok);
                    c.expect(TokenType.COLON);
                    if ("testDataSetupSqls".equals(key)) {
                        if (sqls != null) {
                            throw c.error("Field 'testDataSetupSqls' should"
                                    + " be specified only once");
                        }
                        sqls = new ArrayList<>();
                        c.expect(TokenType.BRACKET_OPEN);
                        while (c.peek() != TokenType.BRACKET_CLOSE && !c.atEnd()) {
                            String quoted = c.text();
                            c.expect(TokenType.STRING);
                            sqls.add(TokenStreamCursor.unquoteAndUnescape(quoted, c));
                            c.match(TokenType.COMMA);
                        }
                        c.expect(TokenType.BRACKET_CLOSE);
                    } else if ("testDataSetupCSV".equals(key)) {
                        // wire spelling flips case: testDataSetupCsv (probe)
                        if (csv != null) {
                            throw c.error("Field 'testDataSetupCsv' should"
                                    + " be specified only once");
                        }
                        String quoted = c.text();
                        c.expect(TokenType.STRING);
                        csv = TokenStreamCursor.unquoteAndUnescape(quoted, c);
                    } else {
                        throw c.error("unknown LocalH2 key: " + key);
                    }
                    c.match(TokenType.SEMI_COLON);
                }
                c.expect(TokenType.BRACE_CLOSE);
                c.expect(TokenType.SEMI_COLON); // span INCLUDES the ';' (probe)
                yield new Protocol.PH2Local(csv, sqls,
                        c.spanOf(keywordTok, c.pos() - 1));
            }
            case "Static" -> {
                c.expect(TokenType.BRACE_OPEN);
                String name = null;
                String host = null;
                Long port = null;
                java.util.Set<String> seenKeys9 = new java.util.HashSet<>();
                while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                    String key = c.parseIdentifier();
                    TokenStreamCursor.once(seenKeys9, key, c);
                    c.expect(TokenType.COLON);
                    switch (key) {
                        // engine spelling: name: IS the databaseName
                        case "name" -> {
                            String quoted = c.text();
                            c.expect(TokenType.STRING);
                            name = TokenStreamCursor.unquoteAndUnescape(quoted, c);
                        }
                        case "host" -> {
                            String quoted = c.text();
                            c.expect(TokenType.STRING);
                            host = TokenStreamCursor.unquoteAndUnescape(quoted, c);
                        }
                        case "port" -> {
                            port = c.consumeLong();
                        }
                        default -> throw c.error("unknown Static key: " + key);
                    }
                    c.expect(TokenType.SEMI_COLON);
                }
                c.expect(TokenType.BRACE_CLOSE);
                c.expect(TokenType.SEMI_COLON); // span INCLUDES the ';' (probe)
                if (name == null || host == null) {
                    throw c.error("Static needs name (or database) and host");
                }
                if (port == null) {
                    // walker: validateAndExtractRequiredField (sibling
                    // negative neg-connection-static-missing-port)
                    throw TokenStreamCursor.throwAt(c.tokens(), keywordTok,
                            "Field 'port' is required");
                }
                yield new Protocol.PStaticSpec(name, host, port,
                        c.spanOf(keywordTok, c.pos() - 1));
            }
            case "DuckDB" -> {
                // ENGINE-REAL (DuckDBParserGrammar, oracle-verified):
                // DuckDB { (path: '...';)* } — no path = in-memory
                c.expect(TokenType.BRACE_OPEN);
                String path = null;
                java.util.Set<String> seenKeys10 = new java.util.HashSet<>();
                while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                    String key = c.parseIdentifier();
                    TokenStreamCursor.once(seenKeys10, key, c);
                    c.expect(TokenType.COLON);
                    if (!"path".equals(key)) {
                        throw c.error("unknown DuckDB key: " + key);
                    }
                    String quoted = c.text();
                    c.expect(TokenType.STRING);
                    path = TokenStreamCursor.unquoteAndUnescape(quoted, c);
                    c.expect(TokenType.SEMI_COLON);
                }
                c.expect(TokenType.BRACE_CLOSE);
                c.expect(TokenType.SEMI_COLON);
                yield new Protocol.PDuckDBSpec(path,
                        c.spanOf(keywordTok, c.pos() - 1));
            }
            case "SQLite" -> {
                yield parseSqliteSpec(c, keywordTok);
            }
            case "Snowflake" -> parseSnowflakeSpec(c, keywordTok);
            case "Redshift" -> parseRedshiftSpec(c, keywordTok);
            case "Trino" -> parseTrinoSpec(c, keywordTok);
            case "Spanner" -> {
                c.expect(TokenType.BRACE_OPEN);
                String projectId = null;
                String instanceId = null;
                String databaseId = null;
                String sProxyHost = null;
                Long sProxyPort = null;
                java.util.Set<String> seenKeys12 = new java.util.HashSet<>();
                while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                    String key = c.parseIdentifier();
                    TokenStreamCursor.once(seenKeys12, key, c);
                    c.expect(TokenType.COLON);
                    switch (key) {
                        case "projectId" -> projectId = SectionParse.stringValue(c);
                        case "instanceId" -> instanceId = SectionParse.stringValue(c);
                        case "databaseId" -> databaseId = SectionParse.stringValue(c);
                        case "proxyHost" -> sProxyHost = SectionParse.stringValue(c);
                        case "proxyPort" -> sProxyPort = c.consumeLong();
                        default -> throw c.error("unknown Spanner key: " + key);
                    }
                    c.expect(TokenType.SEMI_COLON);
                }
                c.expect(TokenType.BRACE_CLOSE);
                c.expect(TokenType.SEMI_COLON);
                if (projectId == null || instanceId == null
                        || databaseId == null) {
                    throw c.error("Spanner needs projectId, instanceId and"
                            + " databaseId");
                }
                yield new Protocol.PSpannerSpec(databaseId, instanceId,
                        projectId, sProxyHost, sProxyPort,
                        c.spanOf(keywordTok, c.pos() - 1));
            }
            case "Databricks" -> {
                c.expect(TokenType.BRACE_OPEN);
                String hostname = null;
                String port = null;
                String protocol = null;
                String httpPath = null;
                java.util.Set<String> seenKeys13 = new java.util.HashSet<>();
                while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                    String key = c.parseIdentifier();
                    TokenStreamCursor.once(seenKeys13, key, c);
                    c.expect(TokenType.COLON);
                    switch (key) {
                        case "hostname" -> hostname = SectionParse.stringValue(c);
                        // port is a quoted STRING in source and on the wire
                        case "port" -> port = SectionParse.stringValue(c);
                        case "protocol" -> protocol = SectionParse.stringValue(c);
                        case "httpPath" -> httpPath = SectionParse.stringValue(c);
                        default -> throw c.error("unknown Databricks key: " + key);
                    }
                    c.expect(TokenType.SEMI_COLON);
                }
                c.expect(TokenType.BRACE_CLOSE);
                c.expect(TokenType.SEMI_COLON);
                if (hostname == null || port == null || protocol == null
                        || httpPath == null) {
                    throw c.error("Databricks needs hostname, port, protocol"
                            + " and httpPath");
                }
                yield new Protocol.PDatabricksSpec(hostname, httpPath, port,
                        protocol, c.spanOf(keywordTok, c.pos() - 1));
            }
            case "BigQuery" -> {
                c.expect(TokenType.BRACE_OPEN);
                String projectId = null;
                String defaultDataset = null;
                String bqProxyHost = null;
                String bqProxyPort = null;
                java.util.Set<String> seenKeys14 = new java.util.HashSet<>();
                while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                    String key = c.parseIdentifier();
                    TokenStreamCursor.once(seenKeys14, key, c);
                    c.expect(TokenType.COLON);
                    switch (key) {
                        case "projectId" -> projectId = SectionParse.stringValue(c);
                        case "defaultDataset" -> defaultDataset = SectionParse.stringValue(c);
                        case "proxyHost" -> bqProxyHost = SectionParse.stringValue(c);
                        case "proxyPort" -> bqProxyPort = stringOrInt(c);
                        default -> throw c.error("unknown BigQuery key: " + key);
                    }
                    c.expect(TokenType.SEMI_COLON);
                }
                c.expect(TokenType.BRACE_CLOSE);
                c.expect(TokenType.SEMI_COLON);
                if (projectId == null || defaultDataset == null) {
                    throw c.error("BigQuery needs projectId and defaultDataset");
                }
                yield new Protocol.PBigQuerySpec(defaultDataset, projectId,
                        bqProxyHost, bqProxyPort,
                        c.spanOf(keywordTok, c.pos() - 1));
            }
            case "EmbeddedH2" -> {
                // EmbeddedH2 { name; directory; autoServerMode; } —
                // _type:"h2Embedded" (probe connection-auth 2026-08-14)
                c.expect(TokenType.BRACE_OPEN);
                String ename = null;
                String directory = null;
                Boolean autoServer = null;
                java.util.Set<String> seenEh = new java.util.HashSet<>();
                while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                    String key = c.parseIdentifier();
                    TokenStreamCursor.once(seenEh, key, c);
                    c.expect(TokenType.COLON);
                    switch (key) {
                        case "name" -> ename = SectionParse.stringValue(c);
                        case "directory" ->
                                directory = SectionParse.stringValue(c);
                        case "autoServerMode" ->
                                autoServer = parseBoolean(c);
                        default -> throw c.error(
                                "unknown EmbeddedH2 key: " + key);
                    }
                    c.expect(TokenType.SEMI_COLON);
                }
                c.expect(TokenType.BRACE_CLOSE);
                c.expect(TokenType.SEMI_COLON);
                if (ename == null || directory == null
                        || autoServer == null) {
                    throw c.error("EmbeddedH2 needs name, directory and"
                            + " autoServerMode");
                }
                yield new Protocol.PH2EmbeddedSpec(ename, directory,
                        autoServer, c.spanOf(keywordTok, c.pos() - 1));
            }
            default -> throw c.error("unsupported datasource specification: "
                    + kind + " (corpus-censused shapes only)");
        };
    }

    /** Auth span runs the {@code auth} KEYWORD through the last body token
     *  (bodyless: through the kind identifier). */
    /** The SQLite datasource spec — a DECLARED lite extension
     *  (OWN_CORPUS_DECISIONS §9); split from parseDatasourceSpec
     *  (method-size guardrail). */
    private static Protocol.PDatasourceSpec parseSqliteSpec(
            TokenStreamCursor c, int keywordTok) {
        if (c.dialect().refusesLiteExtensions()) {
            // the sqlite-backend is a DECLARED lite extension
            // (OWN_CORPUS_DECISIONS §9) — the exact-engine
            // surface refuses it like any unknown spec
            throw c.error("unsupported datasource specification:"
                    + " SQLite");
        }
        // LITE BACKEND, engine-extension-shaped on purpose:
        // SQLite { (path: '...';)* } mirrors DuckDBParserGrammar
        // exactly, so a future engine SQLite extension finds this
        // text conformant (own-corpus decision review)
        c.expect(TokenType.BRACE_OPEN);
        String path = null;
        java.util.Set<String> seenKeys15 = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seenKeys15, key, c);
            c.expect(TokenType.COLON);
            if (!"path".equals(key)) {
                throw c.error("unknown SQLite key: " + key);
            }
            String quoted = c.text();
            c.expect(TokenType.STRING);
            path = TokenStreamCursor.unquoteAndUnescape(quoted, c);
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        c.expect(TokenType.SEMI_COLON);
        return new Protocol.PSQLiteSpec(path,
                c.spanOf(keywordTok, c.pos() - 1));
    }

    private static Protocol.PAuthStrategy parseAuthStrategy(
            TokenStreamCursor c, int keywordTok) {
        String kind = c.parseIdentifier();
        return switch (kind) {
            case "DefaultH2" -> {
                if (c.peek() == TokenType.BRACE_OPEN) {
                    c.advance();                // optional EMPTY body (lite)
                    c.expect(TokenType.BRACE_CLOSE);
                }
                c.expect(TokenType.SEMI_COLON); // span INCLUDES the ';' (probe)
                yield new Protocol.PH2Default(c.spanOf(keywordTok, c.pos() - 1));
            }
            case "Test" -> {
                if (c.peek() == TokenType.BRACE_OPEN) {
                    c.advance();                // optional EMPTY body (probe)
                    c.expect(TokenType.BRACE_CLOSE);
                }
                c.expect(TokenType.SEMI_COLON);
                yield new Protocol.PTestAuth(c.spanOf(keywordTok, c.pos() - 1));
            }
            case "OAuth" -> {
                c.expect(TokenType.BRACE_OPEN);
                String oauthKey = null;
                String scopeName = null;
                java.util.Set<String> seenKeys16 = new java.util.HashSet<>();
                while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                    String key = c.parseIdentifier();
                    TokenStreamCursor.once(seenKeys16, key, c);
                    c.expect(TokenType.COLON);
                    String quoted = c.text();
                    c.expect(TokenType.STRING);
                    String v = TokenStreamCursor.unquoteAndUnescape(quoted, c);
                    switch (key) {
                        case "oauthKey" -> oauthKey = v;
                        case "scopeName" -> scopeName = v;
                        default -> throw c.error("unknown OAuth key: " + key);
                    }
                    c.expect(TokenType.SEMI_COLON);
                }
                c.expect(TokenType.BRACE_CLOSE);
                c.expect(TokenType.SEMI_COLON);
                if (oauthKey == null || scopeName == null) {
                    throw c.error("OAuth needs oauthKey and scopeName");
                }
                yield new Protocol.POAuth(oauthKey, scopeName,
                        c.spanOf(keywordTok, c.pos() - 1));
            }
            case "DelegatedKerberos" -> {
                String principal = null;
                if (c.peek() == TokenType.BRACE_OPEN) {
                    c.advance();
                    java.util.Set<String> seenKeys17 = new java.util.HashSet<>();
                    while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                        String key = c.parseIdentifier();
                        TokenStreamCursor.once(seenKeys17, key, c);
                        c.expect(TokenType.COLON);
                        if (!"serverPrincipal".equals(key)) {
                            throw c.error("unknown DelegatedKerberos key: " + key);
                        }
                        String quoted = c.text();
                        c.expect(TokenType.STRING);
                        principal = TokenStreamCursor.unquoteAndUnescape(quoted, c);
                        c.expect(TokenType.SEMI_COLON);
                    }
                    c.expect(TokenType.BRACE_CLOSE);
                }
                c.expect(TokenType.SEMI_COLON);
                yield new Protocol.PDelegatedKerberos(principal,
                        c.spanOf(keywordTok, c.pos() - 1));
            }
            case "UserNamePassword" -> {
                c.expect(TokenType.BRACE_OPEN);
                String base = null;
                String user = null;
                String pass = null;
                java.util.Set<String> seenKeys18 = new java.util.HashSet<>();
                while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                    String key = c.parseIdentifier();
                    TokenStreamCursor.once(seenKeys18, key, c);
                    c.expect(TokenType.COLON);
                    String quoted = c.text();
                    c.expect(TokenType.STRING);
                    String v = TokenStreamCursor.unquoteAndUnescape(quoted, c);
                    switch (key) {
                        case "baseVaultReference" -> base = v;
                        case "userNameVaultReference" -> user = v;
                        case "passwordVaultReference" -> pass = v;
                        default -> throw c.error(
                                "unknown UserNamePassword key: " + key);
                    }
                    c.expect(TokenType.SEMI_COLON);
                }
                c.expect(TokenType.BRACE_CLOSE);
                c.expect(TokenType.SEMI_COLON);
                if (user == null || pass == null) {
                    throw c.error("UserNamePassword needs userNameVaultReference"
                            + " and passwordVaultReference");
                }
                yield new Protocol.PUserNamePassword(base, user, pass,
                        c.spanOf(keywordTok, c.pos() - 1));
            }
            case "SnowflakePublic" -> {
                c.expect(TokenType.BRACE_OPEN);
                String publicUserName = null;
                String privateKey = null;
                String passPhrase = null;
                java.util.Set<String> seenKeys19 = new java.util.HashSet<>();
                while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                    String key = c.parseIdentifier();
                    TokenStreamCursor.once(seenKeys19, key, c, keywordTok);
                    c.expect(TokenType.COLON);
                    switch (key) {
                        case "publicUserName" -> publicUserName = SectionParse.stringValue(c);
                        case "privateKeyVaultReference" ->
                                privateKey = SectionParse.stringValue(c);
                        case "passPhraseVaultReference" ->
                                passPhrase = SectionParse.stringValue(c);
                        default -> throw c.error(
                                "unknown SnowflakePublic key: " + key);
                    }
                    c.expect(TokenType.SEMI_COLON);
                }
                c.expect(TokenType.BRACE_CLOSE);
                c.expect(TokenType.SEMI_COLON);
                if (publicUserName == null || privateKey == null
                        || passPhrase == null) {
                    throw TokenStreamCursor.throwAt(c.tokens(), keywordTok,
                            "SnowflakePublic needs publicUserName,"
                            + " privateKeyVaultReference and"
                            + " passPhraseVaultReference");
                }
                yield new Protocol.PSnowflakePublic(passPhrase, privateKey,
                        publicUserName, c.spanOf(keywordTok, c.pos() - 1));
            }
            case "GCPApplicationDefaultCredentials" -> {
                if (c.peek() == TokenType.BRACE_OPEN) {
                    c.advance();                // optional EMPTY body
                    c.expect(TokenType.BRACE_CLOSE);
                }
                c.expect(TokenType.SEMI_COLON);
                yield new Protocol.PGCPApplicationDefaultCredentials(
                        c.spanOf(keywordTok, c.pos() - 1));
            }
            case "ApiToken" -> {
                c.expect(TokenType.BRACE_OPEN);
                String apiToken = null;
                java.util.Set<String> seenKeys20 = new java.util.HashSet<>();
                while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                    String key = c.parseIdentifier();
                    TokenStreamCursor.once(seenKeys20, key, c);
                    c.expect(TokenType.COLON);
                    if (!"apiToken".equals(key)) {
                        throw c.error("unknown ApiToken key: " + key);
                    }
                    apiToken = SectionParse.stringValue(c);
                    c.expect(TokenType.SEMI_COLON);
                }
                c.expect(TokenType.BRACE_CLOSE);
                c.expect(TokenType.SEMI_COLON);
                if (apiToken == null) {
                    throw c.error("ApiToken needs apiToken");
                }
                yield new Protocol.PApiToken(apiToken,
                        c.spanOf(keywordTok, c.pos() - 1));
            }
            case "MiddleTierUserNamePassword" -> {
                c.expect(TokenType.BRACE_OPEN);
                String vaultReference = null;
                java.util.Set<String> seenKeys21 = new java.util.HashSet<>();
                while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                    String key = c.parseIdentifier();
                    TokenStreamCursor.once(seenKeys21, key, c);
                    c.expect(TokenType.COLON);
                    if (!"vaultReference".equals(key)) {
                        throw c.error(
                                "unknown MiddleTierUserNamePassword key: " + key);
                    }
                    vaultReference = SectionParse.stringValue(c);
                    c.expect(TokenType.SEMI_COLON);
                }
                c.expect(TokenType.BRACE_CLOSE);
                c.expect(TokenType.SEMI_COLON);
                if (vaultReference == null) {
                    throw c.error("MiddleTierUserNamePassword needs"
                            + " vaultReference");
                }
                yield new Protocol.PMiddleTierUserNamePassword(vaultReference,
                        c.spanOf(keywordTok, c.pos() - 1));
            }
            case "TrinoDelegatedKerberos" ->
                    parseTrinoKerberosAuth(c, keywordTok);
            case "GCPWorkloadIdentityFederation" ->
                    parseGcpWifAuth(c, keywordTok);
            default -> throw c.error("unsupported auth strategy: " + kind
                    + " (corpus-censused shapes only)");
        };
    }

    // ============================================================
    // The re-lex cursor (island + SPI feeds)
    // ============================================================

    /** A minimal cursor over a re-lexed slice, with the walker-offset span
     *  rule for embedded islands. The shared-stream feed uses the host
     *  parser's own cursor instead. */
    private static final class Cursor implements TokenStreamCursor {

        private final TokenStream tokens;
        private final com.legend.parser.Dialect dialect;
        private int pos;
        private final int lineOffset;
        private final int colOffset;

        Cursor(TokenStream tokens, int lineOffset, int colOffset,
                com.legend.parser.Dialect dialect) {
            this.tokens = tokens;
            this.lineOffset = lineOffset;
            this.colOffset = colOffset;
            this.dialect = dialect;
        }

        @Override
        public com.legend.parser.Dialect dialect() {
            return dialect;
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

        @Override
        public com.legend.protocol.SourceInfo spanOf(int fromTok, int toTok) {
            return com.legend.protocol.SpanOrigin.islandShift(
                    TokenStreamCursor.super.spanOf(fromTok, toTok),
                    lineOffset, colOffset);
        }
    }

    /** {@code Snowflake { name; account; warehouse; region; ... }} —
     *  split from parseDatasourceSpec (file-shape guardrail). */
    private static Protocol.PSnowflakeSpec parseSnowflakeSpec(
            TokenStreamCursor c, int keywordTok) {

                c.expect(TokenType.BRACE_OPEN);
                String name = null;
                String account = null;
                String warehouse = null;
                String region = null;
                String accountType = null;
                String cloudType = null;
                Boolean enableQueryTags = null;
                String organization = null;
                String role = null;
                String proxyHost = null;
                String proxyPort = null;
                String nonProxyHosts = null;
                String tempTableDb = null;
                String tempTableSchema = null;
                Boolean quotedIdentifiersIgnoreCase = null;
                java.util.Set<String> seenKeys11 = new java.util.HashSet<>();
                while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                    String key = c.parseIdentifier();
                    TokenStreamCursor.once(seenKeys11, key, c);
                    c.expect(TokenType.COLON);
                    switch (key) {
                        case "name" -> name = SectionParse.stringValue(c);
                        case "account" -> account = SectionParse.stringValue(c);
                        case "warehouse" -> warehouse = SectionParse.stringValue(c);
                        case "region" -> region = SectionParse.stringValue(c);
                        // a BARE enum identifier (VPS / MultiTenant)
                        case "accountType" -> accountType = c.parseIdentifier();
                        case "cloudType" -> cloudType = SectionParse.stringValue(c);
                        case "enableQueryTags" ->
                                enableQueryTags = parseBoolean(c);
                        case "organization" -> organization = SectionParse.stringValue(c);
                        case "role" -> role = SectionParse.stringValue(c);
                        case "proxyHost" -> proxyHost = SectionParse.stringValue(c);
                        case "proxyPort" -> proxyPort = stringOrInt(c);
                        case "nonProxyHosts" ->
                                nonProxyHosts = SectionParse.stringValue(c);
                        case "tempTableDb" -> tempTableDb = SectionParse.stringValue(c);
                        case "tempTableSchema" ->
                                tempTableSchema = SectionParse.stringValue(c);
                        case "quotedIdentifiersIgnoreCase" ->
                                quotedIdentifiersIgnoreCase = parseBoolean(c);
                        default -> throw c.error("unknown Snowflake key: " + key);
                    }
                    c.expect(TokenType.SEMI_COLON);
                }
                c.expect(TokenType.BRACE_CLOSE);
                c.expect(TokenType.SEMI_COLON);
                if (name == null || account == null || warehouse == null
                        || region == null) {
                    throw c.error("Snowflake needs name, account, warehouse"
                            + " and region");
                }
                return new Protocol.PSnowflakeSpec(account, accountType,
                        cloudType, name, enableQueryTags, nonProxyHosts,
                        organization, proxyHost, proxyPort,
                        quotedIdentifiersIgnoreCase, region, role,
                        tempTableDb, tempTableSchema, warehouse,
                        c.spanOf(keywordTok, c.pos() - 1));
                }

    /** {@code Redshift { name; host; port; clusterID; region;
     *  endpointURL; }} — engine grammar (census C12 flavor leg). */
    private static Protocol.PRedshiftSpec parseRedshiftSpec(
            TokenStreamCursor c, int keywordTok) {
        c.expect(TokenType.BRACE_OPEN);
        String name = null;
        String host = null;
        Long port = null;
        String clusterID = null;
        String region = null;
        String endpointURL = null;
        java.util.Set<String> seen = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seen, key, c, keywordTok);
            c.expect(TokenType.COLON);
            switch (key) {
                case "name" -> name = SectionParse.stringValue(c);
                case "host" -> host = SectionParse.stringValue(c);
                case "port" -> port = c.consumeLong();
                case "clusterID" -> clusterID = SectionParse.stringValue(c);
                case "region" -> region = SectionParse.stringValue(c);
                case "endpointURL" -> endpointURL = SectionParse.stringValue(c);
                default -> throw c.error("unknown Redshift key: " + key);
            }
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        c.expect(TokenType.SEMI_COLON);
        if (name == null || host == null || port == null || clusterID == null
                || region == null) {
            throw c.error("Redshift needs name, host, port, clusterID"
                    + " and region");
        }
        return new Protocol.PRedshiftSpec(clusterID, name, endpointURL, host,
                port, region, c.spanOf(keywordTok, c.pos() - 1));
    }

    /** {@code Trino { host; port; catalog?; schema?; clientTags?;
     *  sslSpecification: {...}?; }} (census C12 flavor leg). */
    private static Protocol.PTrinoSpec parseTrinoSpec(
            TokenStreamCursor c, int keywordTok) {
        c.expect(TokenType.BRACE_OPEN);
        String host = null;
        Long port = null;
        String catalog = null;
        String schema = null;
        String clientTags = null;
        Protocol.PTrinoSsl ssl = null;
        java.util.Set<String> seen = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seen, key, c, keywordTok);
            c.expect(TokenType.COLON);
            switch (key) {
                case "host" -> host = SectionParse.stringValue(c);
                case "port" -> port = c.consumeLong();
                case "catalog" -> catalog = SectionParse.stringValue(c);
                case "schema" -> schema = SectionParse.stringValue(c);
                case "clientTags" -> clientTags = SectionParse.stringValue(c);
                case "sslSpecification" -> {
                    c.expect(TokenType.BRACE_OPEN);
                    Boolean sslFlag = null;
                    String tsPath = null;
                    String tsPass = null;
                    java.util.Set<String> seenSsl = new java.util.HashSet<>();
                    while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                        String sk = c.parseIdentifier();
                        TokenStreamCursor.once(seenSsl, sk, c, keywordTok);
                        c.expect(TokenType.COLON);
                        switch (sk) {
                            case "ssl" -> sslFlag = parseBoolean(c);
                            case "trustStorePathVaultReference" ->
                                    tsPath = SectionParse.stringValue(c);
                            case "trustStorePasswordVaultReference" ->
                                    tsPass = SectionParse.stringValue(c);
                            default -> throw c.error(
                                    "unknown sslSpecification key: " + sk);
                        }
                        c.expect(TokenType.SEMI_COLON);
                    }
                    c.expect(TokenType.BRACE_CLOSE);
                    if (sslFlag == null) {
                        throw c.error("sslSpecification needs ssl");
                    }
                    ssl = new Protocol.PTrinoSsl(sslFlag, tsPath, tsPass);
                }
                default -> throw c.error("unknown Trino key: " + key);
            }
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        c.expect(TokenType.SEMI_COLON);
        if (host == null || port == null) {
            throw c.error("Trino needs host and port");
        }
        return new Protocol.PTrinoSpec(catalog, clientTags, host, port,
                schema, ssl, c.spanOf(keywordTok, c.pos() - 1));
    }

    /** Split from parseAuthStrategy (method-shape guardrail). */
    private static Protocol.PTrinoKerberosAuth parseTrinoKerberosAuth(
            TokenStreamCursor c, int keywordTok) {

                c.expect(TokenType.BRACE_OPEN);
                String remoteService = null;
                Boolean canonical = null;
                String serverPrincipal = null;
                java.util.Set<String> seenTk = new java.util.HashSet<>();
                while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                    String k = c.parseIdentifier();
                    TokenStreamCursor.once(seenTk, k, c, keywordTok);
                    c.expect(TokenType.COLON);
                    switch (k) {
                        case "kerberosRemoteServiceName" ->
                                remoteService = SectionParse.stringValue(c);
                        case "kerberosUseCanonicalHostname" ->
                                canonical = parseBoolean(c);
                        case "serverPrincipal" ->
                                serverPrincipal = SectionParse.stringValue(c);
                        default -> throw c.error(
                                "unknown TrinoDelegatedKerberos key: " + k);
                    }
                    c.expect(TokenType.SEMI_COLON);
                }
                c.expect(TokenType.BRACE_CLOSE);
                c.expect(TokenType.SEMI_COLON);
                return new Protocol.PTrinoKerberosAuth(remoteService,
                        canonical, serverPrincipal,
                        c.spanOf(keywordTok, c.pos() - 1));
                }

    /** Split from parseAuthStrategy (method-shape guardrail). */
    private static Protocol.PGcpWifAuth parseGcpWifAuth(
            TokenStreamCursor c, int keywordTok) {

                c.expect(TokenType.BRACE_OPEN);
                String serviceAccountEmail = null;
                List<String> scopes = null;
                java.util.Set<String> seenWif = new java.util.HashSet<>();
                while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                    String k = c.parseIdentifier();
                    TokenStreamCursor.once(seenWif, k, c, keywordTok);
                    c.expect(TokenType.COLON);
                    switch (k) {
                        case "serviceAccountEmail" ->
                                serviceAccountEmail = SectionParse.stringValue(c);
                        case "additionalGcpScopes" -> {
                            scopes = new ArrayList<>();
                            c.expect(TokenType.BRACKET_OPEN);
                            while (c.peek() != TokenType.BRACKET_CLOSE) {
                                scopes.add(SectionParse.stringValue(c));
                                if (!c.match(TokenType.COMMA)) {
                                    break;
                                }
                            }
                            c.expect(TokenType.BRACKET_CLOSE);
                        }
                        default -> throw c.error(
                                "unknown GCPWorkloadIdentityFederation key: "
                                + k);
                    }
                    c.expect(TokenType.SEMI_COLON);
                }
                c.expect(TokenType.BRACE_CLOSE);
                c.expect(TokenType.SEMI_COLON);
                if (serviceAccountEmail == null) {
                    throw c.error("GCPWorkloadIdentityFederation needs"
                            + " serviceAccountEmail");
                }
                return new Protocol.PGcpWifAuth(scopes, serviceAccountEmail,
                        c.spanOf(keywordTok, c.pos() - 1));
                }

    /** {@code # UserPassword { username; password: <Kind>Secret {...}; }#}
     *  — ONE owner for the auth island (Mongo + Elasticsearch share the
     *  engine's authentication module wire). */
    private static Protocol.PAuthSpecValue parseUserPasswordAuthIsland(
            TokenStreamCursor c, int keyTok) {
        if (c.peek() != TokenType.ISLAND_OPEN) {
            throw c.error("MongoDBConnection authentication must"
                    + " be a # <Kind> {...}# island");
        }
        // the walker dispatches through the SAME registered auth-module
        // extensions as ES — ANY kind is legal (probe t2-authentication:
        // ApiKey on a MongoDBConnection)
        return parseEsAuthIsland(c, keyTok);
    }

    /** The {@code UserPassword} island BODY (kind already consumed) —
     *  shared by the Mongo-restricted and the general (ES) auth paths. */
    private static Protocol.PMongoAuth parseUserPasswordBody(
            TokenStreamCursor c, int keyTok) {
        IslandParse ip = reLexIsland(c);
        Cursor ic = ip.cursor();
        String username = null;
        Protocol.PVaultSecret secret = null;
        java.util.Set<String> seen = new java.util.HashSet<>();
        while (!ic.atEnd()) {
            String ik = ic.parseIdentifier();
            TokenStreamCursor.once(seen, ik, ic);
            ic.expect(TokenType.COLON);
            if ("username".equals(ik)) {
                username = SectionParse.stringValue(ic);
                ic.expect(TokenType.SEMI_COLON);
            } else if ("password".equals(ik)) {
                secret = parseVaultSecret(ic);
            } else {
                throw ic.error("unknown UserPassword key: " + ik);
            }
        }
        if (username == null || secret == null) {
            throw c.error("UserPassword needs username and password");
        }
        return new Protocol.PMongoAuth(username, secret,
                authSpan(c, keyTok, ip));
    }

    /** {@code { store: qn; clusterDetails: # URL { <uri> }#;
     *  authentication: # UserPassword {...}#; }} — the value span runs the
     *  FIRST BODY KEY through the close (walker ctx = the body content;
     *  wire probed live, C12 ES leg). */
    private static Protocol.PElasticsearchConnection parseElasticsearchBody(
            TokenStreamCursor c) {
        c.expect(TokenType.BRACE_OPEN);
        int firstKeyTok = c.pos();
        String element = null;
        com.legend.protocol.SourceInfo elementSpan = null;
        String url = null;
        Protocol.PAuthSpecValue auth = null;
        java.util.Set<String> seenEs = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            int keyTok = c.pos();
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seenEs, key, c, firstKeyTok);
            c.expect(TokenType.COLON);
            switch (key) {
                case "store" -> {
                    int eS = c.pos();
                    element = Protocol.unquotePath(c.parseQualifiedName());
                    elementSpan = c.spanOf(eS, c.pos() - 1);
                }
                case "clusterDetails" -> {
                    if (c.peek() != TokenType.ISLAND_OPEN) {
                        throw c.error("clusterDetails must be a"
                                + " # URL {...}# island");
                    }
                    int kindTok = c.pos();
                    String kind = islandKind(c);
                    if (!"URL".equals(kind)) {
                        // engine-verbatim (TWO spaces before Supported)
                        throw TokenStreamCursor.throwAt(c.tokens(), kindTok,
                                "Unsupported cluster details type: " + kind
                                + ".  Supported: URL");
                    }
                    // RAW source slice: '//' inside a URL is NOT a
                    // comment (the re-lex dropped it — C12 ES leg)
                    c.advance();                        // ISLAND_OPEN
                    int bs = c.pos();
                    int d = 0;
                    while (!c.atEnd()) {
                        TokenType t = c.peek();
                        if (t == TokenType.ISLAND_START) {
                            d++;
                        } else if (t == TokenType.ISLAND_END) {
                            if (d == 0) {
                                break;
                            }
                            d--;
                        }
                        c.advance();
                    }
                    String raw = c.reconstructText(bs, c.pos()).trim();
                    c.expect(TokenType.ISLAND_END);
                    try {
                        url = new java.net.URI(raw).toString();
                    } catch (java.net.URISyntaxException e) {
                        throw new com.legend.parser.ParseException(
                                "URL is not valid",
                                c.tokens().startLine(bs),
                                c.tokens().startColumn(bs));
                    }
                }
                case "authentication" ->
                        auth = parseEsAuthIsland(c, keyTok);
                default -> throw c.error(
                        "unknown Elasticsearch7ClusterConnection key: "
                        + key);
            }
            // every connection field rule ends SEMI_COLON in the .g4 —
            // a missing semicolon is an engine refusal, not leniency
            c.expect(TokenType.SEMI_COLON);
        }
        int closeTok = c.pos();
        c.expect(TokenType.BRACE_CLOSE);
        if (element == null || elementSpan == null) {
            throw TokenStreamCursor.throwAt(c.tokens(), firstKeyTok,
                    "Field 'store' is required");
        }
        if (url == null) {
            throw TokenStreamCursor.throwAt(c.tokens(), firstKeyTok,
                    "Field 'clusterDetails' is required");
        }
        if (auth == null) {
            throw TokenStreamCursor.throwAt(c.tokens(), firstKeyTok,
                    "Field 'authentication' is required");
        }
        // the engine reparses the value snippet with a +4 column offset
        // that leaks into the ctx END (C12 byte pin: '}' col 1 -> wire 5)
        var vs = c.spanOf(firstKeyTok, closeTok);
        return new Protocol.PElasticsearchConnection(element, elementSpan,
                url, auth,
                com.legend.protocol.SpanOrigin.overshootEnd(vs, 4));
    }

    /** One vault secret: the single-field kinds or the AWS secrets
     *  manager shape (probed 2026-08-14; STSAssumeRole credentials are
     *  oracle-unreachable at the 4.138.2 pin). */
    private static Protocol.PVaultSecret parseVaultSecret(Cursor ic) {
        int vS = ic.pos();
        String sk = ic.parseIdentifier();
        if ("AWSSecretsManagerSecret".equals(sk)) {
            ic.expect(TokenType.BRACE_OPEN);
            String secretId = null;
            String versionId = null;
            String versionStage = null;
            String credsKind = null;
            Protocol.PMongoSecret accessKeyId = null;
            Protocol.PMongoSecret secretAccessKey = null;
            com.legend.protocol.SourceInfo credsSpan = null;
            while (!ic.atEnd() && ic.peek() != TokenType.BRACE_CLOSE) {
                String k = ic.parseIdentifier();
                ic.expect(TokenType.COLON);
                switch (k) {
                    case "secretId" -> {
                        secretId = SectionParse.stringValue(ic);
                        ic.expect(TokenType.SEMI_COLON);
                    }
                    case "versionId" -> {
                        versionId = SectionParse.stringValue(ic);
                        ic.expect(TokenType.SEMI_COLON);
                    }
                    case "versionStage" -> {
                        versionStage = SectionParse.stringValue(ic);
                        ic.expect(TokenType.SEMI_COLON);
                    }
                    case "awsCredentials" -> {
                        int cS = ic.pos();
                        String ck = ic.parseIdentifier();
                        ic.expect(TokenType.BRACE_OPEN);
                        if ("Default".equals(ck)) {
                            credsKind = "awsDefault";
                            ic.expect(TokenType.BRACE_CLOSE);
                            // spanless on the wire (probed asymmetry)
                        } else if ("Static".equals(ck)) {
                            credsKind = "awsStatic";
                            while (!ic.atEnd()
                                    && ic.peek() != TokenType.BRACE_CLOSE) {
                                String ak = ic.parseIdentifier();
                                ic.expect(TokenType.COLON);
                                var sec = parseVaultSecret(ic);
                                if (!(sec instanceof
                                        Protocol.PMongoSecret ms)) {
                                    throw ic.error("nested AWS credentials"
                                            + " need a single-field secret");
                                }
                                switch (ak) {
                                    case "accessKeyId" -> accessKeyId = ms;
                                    case "secretAccessKey" ->
                                            secretAccessKey = ms;
                                    default -> throw ic.error(
                                            "unknown Static key: " + ak);
                                }
                            }
                            ic.expect(TokenType.BRACE_CLOSE);
                            credsSpan = ic.spanOf(cS, ic.pos() - 1);
                        } else {
                            throw ic.error("unsupported awsCredentials"
                                    + " kind: " + ck);
                        }
                    }
                    default -> throw ic.error(
                            "unknown AWSSecretsManagerSecret key: " + k);
                }
            }
            ic.expect(TokenType.BRACE_CLOSE);
            if (secretId == null || credsKind == null) {
                throw ic.error("AWSSecretsManagerSecret needs secretId and"
                        + " awsCredentials");
            }
            ic.expect(TokenType.SEMI_COLON);
            return new Protocol.PAwsSecret(secretId, versionId, versionStage,
                    credsKind, accessKeyId, secretAccessKey, credsSpan);
        }
        String wireKind;
        String wireField;
        switch (sk) {
            case "PropertiesFileSecret" -> {
                wireKind = "properties";
                wireField = "propertyName";
            }
            case "SystemPropertiesSecret" -> {
                wireKind = "systemproperties";
                wireField = "systemPropertyName";
            }
            case "EnvironmentSecret" -> {
                // wire "environment" (AuthenticationProtocolExtension;
                // oracle-accepted, audit probe 2026-08-14)
                wireKind = "environment";
                wireField = "envVariableName";
            }
            default -> throw ic.error("unsupported secret kind: " + sk);
        }
        ic.expect(TokenType.BRACE_OPEN);
        String fieldKey = ic.parseIdentifier();
        ic.expect(TokenType.COLON);
        String v = SectionParse.stringValue(ic);
        ic.expect(TokenType.SEMI_COLON);
        ic.expect(TokenType.BRACE_CLOSE);
        if (!wireField.equals(fieldKey)) {
            throw ic.error("unknown " + sk + " key: " + fieldKey);
        }
        Protocol.PMongoSecret secret = new Protocol.PMongoSecret(wireKind,
                wireField, v, ic.spanOf(vS, ic.pos() - 1));
        ic.expect(TokenType.SEMI_COLON);
        return secret;
    }

    /** One AWS credentials value ({@code Default {} | Static {...} |
     *  STSAssumeRole {...}}) — recursive; span = kind keyword through the
     *  closing brace; Default is spanless (probe t2-authentication). */
    private static Protocol.PAwsCredentials parseAwsCredentials(Cursor ic) {
        int kS = ic.pos();
        String ck = ic.parseIdentifier();
        switch (ck) {
            case "Default" -> {
                ic.expect(TokenType.BRACE_OPEN);
                ic.expect(TokenType.BRACE_CLOSE);
                return new Protocol.PAwsDefault();
            }
            case "Static" -> {
                ic.expect(TokenType.BRACE_OPEN);
                Protocol.PMongoSecret accessKeyId = null;
                Protocol.PMongoSecret secretAccessKey = null;
                while (!ic.atEnd() && ic.peek() != TokenType.BRACE_CLOSE) {
                    String k = ic.parseIdentifier();
                    ic.expect(TokenType.COLON);
                    var sec = parseVaultSecret(ic);
                    if (!(sec instanceof Protocol.PMongoSecret ms)) {
                        throw ic.error("Static credentials need"
                                + " single-field secrets");
                    }
                    switch (k) {
                        case "accessKeyId" -> accessKeyId = ms;
                        case "secretAccessKey" -> secretAccessKey = ms;
                        default -> throw ic.error("unknown Static key: " + k);
                    }
                }
                ic.expect(TokenType.BRACE_CLOSE);
                if (accessKeyId == null || secretAccessKey == null) {
                    throw ic.error("Static needs accessKeyId and"
                            + " secretAccessKey");
                }
                return new Protocol.PAwsStatic(accessKeyId, secretAccessKey,
                        ic.spanOf(kS, ic.pos() - 1));
            }
            case "STSAssumeRole" -> {
                ic.expect(TokenType.BRACE_OPEN);
                String roleArn = null;
                String roleSessionName = null;
                Protocol.PAwsCredentials nested = null;
                while (!ic.atEnd() && ic.peek() != TokenType.BRACE_CLOSE) {
                    String k = ic.parseIdentifier();
                    ic.expect(TokenType.COLON);
                    switch (k) {
                        case "roleArn" -> {
                            roleArn = SectionParse.stringValue(ic);
                            ic.expect(TokenType.SEMI_COLON);
                        }
                        case "roleSessionName" -> {
                            roleSessionName = SectionParse.stringValue(ic);
                            ic.expect(TokenType.SEMI_COLON);
                        }
                        case "awsCredentials" ->
                                nested = parseAwsCredentials(ic);
                        default -> throw ic.error(
                                "unknown STSAssumeRole key: " + k);
                    }
                }
                ic.expect(TokenType.BRACE_CLOSE);
                if (roleArn == null || roleSessionName == null
                        || nested == null) {
                    throw ic.error("STSAssumeRole needs roleArn,"
                            + " roleSessionName and awsCredentials");
                }
                return new Protocol.PAwsStsRole(roleArn, roleSessionName,
                        nested, ic.spanOf(kS, ic.pos() - 1));
            }
            default -> throw ic.error("unsupported awsCredentials kind: "
                    + ck);
        }
    }

    /** The auth-island CONTENT span: first content token through the
     *  island close +3 (the reparse overshoot family); an EMPTY island
     *  anchors at its own '}#' (probed # Kerberos {}#: 7:3..7:7). */
    private static com.legend.protocol.SourceInfo authSpan(
            TokenStreamCursor c, int keyTok, IslandParse ip) {
        var aSp = c.spanOf(keyTok, ip.endTok());
        if (ip.cursor().tokens().count() == 0) {
            return com.legend.protocol.SpanOrigin.anchoredOvershoot(
                    new com.legend.protocol.SourceInfo("",
                            c.tokens().startLine(ip.endTok()),
                            c.tokens().startColumn(ip.endTok()),
                            aSp.endLine(), aSp.endColumn()),
                    aSp, 3);
        }
        var firstTok = ip.cursor().spanOf(0, 0);
        return com.legend.protocol.SpanOrigin.anchoredOvershoot(firstTok,
                aSp, 3);
    }

    /** The GENERAL authentication island for stores whose walker accepts
     *  any registered auth spec (Elasticsearch; probed 2026-08-14 —
     *  ApiKey/Kerberos/EncryptedPrivateKey beyond UserPassword). */
    private static Protocol.PAuthSpecValue parseEsAuthIsland(
            TokenStreamCursor c, int keyTok) {
        if (c.peek() != TokenType.ISLAND_OPEN) {
            throw c.error("authentication must be a # <Kind> {...}# island");
        }
        String kind = islandKind(c);
        return switch (kind) {
            case "UserPassword" -> parseUserPasswordBody(c, keyTok);
            case "Kerberos" -> {
                IslandParse ip = reLexIsland(c);
                if (!ip.cursor().atEnd()) {
                    throw c.error("Kerberos auth takes no fields");
                }
                yield new Protocol.PKerberosAuth(authSpan(c, keyTok, ip));
            }
            case "ApiKey" -> {
                IslandParse ip = reLexIsland(c);
                Cursor ic = ip.cursor();
                String location = null;
                String keyName = null;
                Protocol.PVaultSecret value = null;
                java.util.Set<String> seen = new java.util.HashSet<>();
                while (!ic.atEnd()) {
                    String ik = ic.parseIdentifier();
                    TokenStreamCursor.once(seen, ik, ic);
                    ic.expect(TokenType.COLON);
                    switch (ik) {
                        case "location" -> {
                            // the walker does Location.valueOf(upper) —
                            // anything outside {HEADER, COOKIE} throws
                            location = SectionParse.stringValue(ic)
                                    .toUpperCase(java.util.Locale.ROOT);
                            if (!"HEADER".equals(location)
                                    && !"COOKIE".equals(location)) {
                                throw ic.error("unknown ApiKey location: "
                                        + location);
                            }
                            ic.expect(TokenType.SEMI_COLON);
                        }
                        case "keyName" -> {
                            keyName = SectionParse.stringValue(ic);
                            ic.expect(TokenType.SEMI_COLON);
                        }
                        case "value" -> value = parseVaultSecret(ic);
                        default -> throw ic.error("unknown ApiKey key: " + ik);
                    }
                }
                if (location == null || keyName == null || value == null) {
                    throw c.error("ApiKey needs location, keyName and value");
                }
                yield new Protocol.PApiKeyAuth(keyName, location, value,
                        authSpan(c, keyTok, ip));
            }
            case "GCPWIFWithAWSIdP" -> {
                IslandParse ip = reLexIsland(c);
                Cursor ic = ip.cursor();
                String email = null;
                String accountId = null;
                String region = null;
                String role = null;
                Protocol.PAwsCredentials creds = null;
                String projectNumber = null;
                String providerId = null;
                String poolId = null;
                java.util.Set<String> seen = new java.util.HashSet<>();
                while (!ic.atEnd()) {
                    String ik = ic.parseIdentifier();
                    TokenStreamCursor.once(seen, ik, ic);
                    ic.expect(TokenType.COLON);
                    switch (ik) {
                        case "serviceAccountEmail" -> {
                            email = SectionParse.stringValue(ic);
                            ic.expect(TokenType.SEMI_COLON);
                        }
                        case "idP" -> {
                            String ipk = ic.parseIdentifier();
                            if (!"AWSIdP".equals(ipk)) {
                                throw ic.error("unknown idP kind: " + ipk);
                            }
                            ic.expect(TokenType.BRACE_OPEN);
                            java.util.Set<String> seenIdp =
                                    new java.util.HashSet<>();
                            while (!ic.atEnd()
                                    && ic.peek() != TokenType.BRACE_CLOSE) {
                                String k2 = ic.parseIdentifier();
                                TokenStreamCursor.once(seenIdp, k2, ic);
                                ic.expect(TokenType.COLON);
                                switch (k2) {
                                    case "accountId" -> {
                                        accountId = SectionParse.stringValue(ic);
                                        ic.expect(TokenType.SEMI_COLON);
                                    }
                                    case "region" -> {
                                        region = SectionParse.stringValue(ic);
                                        ic.expect(TokenType.SEMI_COLON);
                                    }
                                    case "role" -> {
                                        role = SectionParse.stringValue(ic);
                                        ic.expect(TokenType.SEMI_COLON);
                                    }
                                    case "awsCredentials" ->
                                            creds = parseAwsCredentials(ic);
                                    default -> throw ic.error(
                                            "unknown AWSIdP key: " + k2);
                                }
                            }
                            ic.expect(TokenType.BRACE_CLOSE);
                        }
                        case "workload" -> {
                            String wk = ic.parseIdentifier();
                            if (!"GCPWorkload".equals(wk)) {
                                throw ic.error("unknown workload kind: "
                                        + wk);
                            }
                            ic.expect(TokenType.BRACE_OPEN);
                            java.util.Set<String> seenW =
                                    new java.util.HashSet<>();
                            while (!ic.atEnd()
                                    && ic.peek() != TokenType.BRACE_CLOSE) {
                                String k3 = ic.parseIdentifier();
                                TokenStreamCursor.once(seenW, k3, ic);
                                ic.expect(TokenType.COLON);
                                String v = SectionParse.stringValue(ic);
                                ic.expect(TokenType.SEMI_COLON);
                                switch (k3) {
                                    case "projectNumber" -> projectNumber = v;
                                    case "providerId" -> providerId = v;
                                    case "poolId" -> poolId = v;
                                    default -> throw ic.error(
                                            "unknown GCPWorkload key: " + k3);
                                }
                            }
                            ic.expect(TokenType.BRACE_CLOSE);
                        }
                        default -> throw ic.error(
                                "unknown GCPWIFWithAWSIdP key: " + ik);
                    }
                }
                if (email == null || accountId == null || region == null
                        || role == null || creds == null
                        || projectNumber == null || providerId == null
                        || poolId == null) {
                    throw c.error("GCPWIFWithAWSIdP needs "
                            + "serviceAccountEmail, idP and workload");
                }
                yield new Protocol.PGcpWifIslandAuth(email, accountId,
                        region, role, creds, projectNumber, providerId,
                        poolId, authSpan(c, keyTok, ip));
            }
            case "PSK" -> {
                // pskAuthentication: PSK COLON STRING SEMI_COLON — the
                // Deephaven pre-shared key registered on the general
                // island registry (engine accepts it on ANY auth island)
                IslandParse ip = reLexIsland(c);
                Cursor ic = ip.cursor();
                String ik = ic.parseIdentifier();
                if (!"psk".equals(ik)) {
                    throw ic.error("unknown PSK key: " + ik);
                }
                ic.expect(TokenType.COLON);
                String psk = SectionParse.stringValue(ic);
                ic.expect(TokenType.SEMI_COLON);
                // pskAuthentication has NO trailing EOF in the .g4 —
                // ANTLR stops after the first field and silently ignores
                // the rest of the island (probed "psk extra field")
                while (!ic.atEnd()) {
                    ic.advance();
                }
                yield new Protocol.PPskAuth(psk);
            }
            case "EncryptedPrivateKey" -> {
                IslandParse ip = reLexIsland(c);
                Cursor ic = ip.cursor();
                String userName = null;
                Protocol.PVaultSecret privateKey = null;
                Protocol.PVaultSecret passphrase = null;
                java.util.Set<String> seen = new java.util.HashSet<>();
                while (!ic.atEnd()) {
                    String ik = ic.parseIdentifier();
                    TokenStreamCursor.once(seen, ik, ic);
                    ic.expect(TokenType.COLON);
                    switch (ik) {
                        case "userName" -> {
                            userName = SectionParse.stringValue(ic);
                            ic.expect(TokenType.SEMI_COLON);
                        }
                        case "privateKey" -> privateKey = parseVaultSecret(ic);
                        case "passphrase" -> passphrase = parseVaultSecret(ic);
                        default -> throw ic.error(
                                "unknown EncryptedPrivateKey key: " + ik);
                    }
                }
                if (userName == null || privateKey == null
                        || passphrase == null) {
                    throw c.error("EncryptedPrivateKey needs userName,"
                            + " privateKey and passphrase");
                }
                yield new Protocol.PEpkAuth(userName, privateKey, passphrase,
                        authSpan(c, keyTok, ip));
            }
            default -> throw c.error("unsupported auth island kind: " + kind);
        };
    }
}
