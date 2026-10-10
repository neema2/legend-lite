// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser.section;

import com.legend.lexer.TokenType;
import com.legend.parser.TokenStreamCursor;
import com.legend.protocol.Protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The SHARED grammar for the FUNCTION-ACTIVATOR sections — Snowflake
 * (SnowflakeApp / SnowflakeM2MUdf), MemSql (MemSqlFunction), BigQuery
 * (BigQueryFunction), HostedService, FunctionJar. One uniform body shape:
 * typed scalar/boolean keys, a {@code function:} pointer keeping its full
 * signature text and span, {@code ownership: Deployment { identifier }},
 * and an optional {@code activationConfiguration:} connection pointer.
 * Wire shapes probed byte-for-byte (ZTailProbe "activatorShapes" /
 * "activatorShapes2").
 */
public final class FunctionActivatorSectionGrammar
        implements LexableSectionGrammar {

    private static final Set<String> STRING_KEYS = Set.of("applicationName",
            "udfName", "functionName", "description", "documentation",
            "pattern", "deploymentSchema", "deploymentStage", "usageRole");
    private static final Set<String> BOOLEAN_KEYS = Set.of(
            "autoActivateUpdates", "generateLineage", "storeModel");

    /** The censused config-element kinds (EXACT names — no suffix
     *  matching on element kinds). */
    private static final Set<String> DEPLOYMENT_CONFIG_KINDS = Set.of(
            "SnowflakeAppDeploymentConfiguration",
            "BigQueryFunctionDeploymentConfiguration",
            "MemSqlFunctionDeploymentConfiguration",
            "HostedServiceDeploymentConfiguration");

    private final String section;
    private final Set<String> kinds;

    public FunctionActivatorSectionGrammar(String section, Set<String> kinds) {
        this.section = section;
        this.kinds = kinds;
    }

    /** The censused activator kinds this section admits — read by
     *  PmcdParser's registry-routed dispatch (SectionCatalog step: the
     *  document surface previously merged ALL kind sets, accepting
     *  MemSqlFunction inside ###Snowflake — adversarial audit #18). */
    public Set<String> kinds() {
        return kinds;
    }

    @Override
    public String name() {
        return section;
    }

    @Override
    public void parse(com.legend.spi.SectionSource src,
            com.legend.spi.ElementSink out) {
        var c = new SliceCursor(com.legend.lexer.Lexer.tokenize(src.text()),
                // SPI overlay hosting = the drop-in seam: engine grammar
                com.legend.parser.Dialect.LEGEND_ENGINE);
        while (!c.atEnd()) {
            if (c.peek() == TokenType.IMPORT) {
                SectionImports.parseImport(c);
                continue;
            }
            Protocol.Element e = parseElement(c);
            if (e instanceof Protocol.PFunctionActivator a
                    && "SnowflakeAppDeploymentConfiguration"
                            .equals(a.kind())) {
                continue;       // walker-dropped (probe 2026-08-14)
            }
            String qname = e instanceof Protocol.PFunctionActivator a2
                    ? a2.qualifiedName()
                    : ((Protocol.PExecutionEnvironment) e).qualifiedName();
            out.accept(qname,
                    com.legend.protocol.ProtocolEmitter.emitElement(e));
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
        return com.legend.model.ModelFromProtocol.element(element);
    }

    /** {@code XDeploymentConfiguration qn { activationConnection: qn; }}
     *  — the walkers treat these strangely (probed 2026-08-14): the
     *  Snowflake config is parsed then DROPPED (no element, no section
     *  entry); the BigQuery config emits a NAMELESS element
     *  ({@code bigQueryFunctionConfig} with no name/package/span) and a
     *  literal {@code null} in the section's element list. The kind rides
     *  a PFunctionActivator with an empty functionPath; the emitter and
     *  PmcdParser dispatch on the kind before anything reads it. */
    private Protocol.PFunctionActivator parseDeploymentConfig(
            TokenStreamCursor c, String kind) {
        int declStart = c.pos();
        c.advance();
        String qn = Protocol.unquotePath(c.parseQualifiedName());
        int cut = qn.lastIndexOf("::");
        String pkg = cut < 0 ? "" : qn.substring(0, cut);
        String name = cut < 0 ? qn : qn.substring(cut + 2);
        c.expect(TokenType.BRACE_OPEN);
        String conn = null;
        com.legend.protocol.SourceInfo connSpan = null;
        boolean hosted = "HostedServiceDeploymentConfiguration".equals(kind);
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            c.expect(TokenType.COLON);
            if (!"activationConnection".equals(key)) {
                throw c.error("unknown " + kind + " key: " + key);
            }
            int aS = c.pos();
            conn = Protocol.unquotePath(c.parseQualifiedName());
            connSpan = c.spanOf(aS, c.pos() - 1);
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        if (conn == null && !hosted) {
            throw TokenStreamCursor.throwAt(c.tokens(), declStart,
                    "Field 'activationConnection' is required");
        }
        var span = c.spanOf(declStart, c.pos() - 1);
        return new Protocol.PFunctionActivator(pkg, name, kind,
                java.util.List.of(), java.util.List.of(),
                java.util.Map.of(), java.util.Map.of(), "", span,
                null, null, conn, connSpan, span);
    }

    /** One {@code Kind <<dec>> qn { key: value; ... }} activator (or an
     *  {@code ExecutionEnvironment} — the HostedService section hosts
     *  them with the SERVICE grammar's shape, probe t2-hostedservice). */
    public Protocol.Element parseElement(TokenStreamCursor c) {
        int declStart = c.pos();
        if (!c.isIdentifierToken(c.peek())) {
            throw c.error("unsupported ###" + section + " element: "
                    + c.safeText());
        }
        String kind = c.safeText();
        if (!kinds.contains(kind)) {
            throw c.error("unsupported ###" + section + " element: " + kind);
        }
        if ("ExecutionEnvironment".equals(kind)) {
            return ServiceSectionGrammar.parseExecutionEnvironment(c);
        }
        if (DEPLOYMENT_CONFIG_KINDS.contains(kind)) {
            return parseDeploymentConfig(c, kind);
        }
        c.advance();
        TokenStreamCursor.Decorations dec = c.parseDecorations();
        String qn = Protocol.unquotePath(c.parseQualifiedName());
        int cut = qn.lastIndexOf("::");
        String pkg = cut < 0 ? "" : qn.substring(0, cut);
        String name = cut < 0 ? qn : qn.substring(cut + 2);
        c.expect(TokenType.BRACE_OPEN);

        Map<String, String> scalars = new java.util.LinkedHashMap<>();
        Map<String, Boolean> booleans = new java.util.LinkedHashMap<>();
        String functionPath = null;
        com.legend.protocol.SourceInfo functionSpan = null;
        String ownerId = null;
        List<String> userListUsers = null;
        String actConn = null;
        com.legend.protocol.SourceInfo actConnSpan = null;

        java.util.Set<String> seenKeys = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            if (!("FunctionJar".equals(kind)
                    && "activationConfiguration".equals(key))) {
                // FunctionJar's activationConfiguration DUPLICATES freely —
                // the walker drops the block entirely (mutant probe)
                TokenStreamCursor.once(seenKeys, key, c);
            }
            c.expect(TokenType.COLON);
            if (STRING_KEYS.contains(key)) {
                scalars.put(key, SectionParse.stringValue(c));
            } else if (BOOLEAN_KEYS.contains(key)) {
                booleans.put(key, SectionParse.booleanValue(c));
            } else if ("function".equals(key)) {
                // the SHARED pointer-site scan (TokenStreamCursor
                // #parseFunctionDescriptor); the signature joins token
                // texts with NO separators — the walkers all render
                // ctx.getText(), so `f(String[1], Integer[*])` loses the
                // space (sibling fixture t2-deephaven); the fqn part
                // keeps raw spelling, unquoted
                var fd = c.parseFunctionDescriptor();
                if ("Deephaven".equals(section)) {
                    // appMultiplicity wants DOT DOT but CoreLexer emits
                    // one DOT_DOT token — ranges are unreachable in the
                    // DEEPHAVEN grammar only (M3-imported activators
                    // accept them; sibling negative
                    // neg-deephaven-app-multiplicity-range)
                    for (int i = fd.nameEnd(); i < fd.end(); i++) {
                        if ("..".equals(c.tokens().text(i))) {
                            throw TokenStreamCursor.throwAt(c.tokens(), i,
                                    "Unexpected token '..'");
                        }
                    }
                }
                functionPath = Protocol.unquotePath(
                        c.reconstructText(fd.start(), fd.nameEnd()))
                        + c.compactText(fd.nameEnd(), fd.end() - 1);
                functionSpan = c.spanOf(fd.start(), fd.end() - 1);
            } else if ("ownership".equals(key)) {
                String oKind = c.parseIdentifier();
                c.expect(TokenType.BRACE_OPEN);
                String ik = c.parseIdentifier();
                c.expect(TokenType.COLON);
                if ("Deployment".equals(oKind)) {
                    if (!"identifier".equals(ik)) {
                        throw c.error("unknown Deployment key: " + ik);
                    }
                    ownerId = SectionParse.stringValue(c);
                } else if ("UserList".equals(oKind)
                        && "HostedService".equals(section)) {
                    // UserList exists ONLY in the HostedService .g4; the
                    // other activator grammars are Deployment-only
                    // (sibling negative neg-functionjar-userlist-ownership)
                    if (!"users".equals(ik)) {
                        throw c.error("unknown UserList key: " + ik);
                    }
                    userListUsers = new ArrayList<>();
                    c.expect(TokenType.BRACKET_OPEN);
                    while (c.peek() != TokenType.BRACKET_CLOSE) {
                        userListUsers.add(SectionParse.stringValue(c));
                        if (!c.match(TokenType.COMMA)) {
                            break;
                        }
                    }
                    c.expect(TokenType.BRACKET_CLOSE);
                } else {
                    throw c.error("unsupported ownership kind: " + oKind);
                }
                c.match(TokenType.SEMI_COLON);
                c.expect(TokenType.BRACE_CLOSE);
            } else if ("permissionScheme".equals(key)) {
                // an ENUM-shaped value — engine-parity validation against
                // SnowflakePermissionScheme (SnowflakeTreeWalker:113)
                String scheme = c.parseIdentifier();
                if (!"DEFAULT".equals(scheme)
                        && !"SEQUESTERED".equals(scheme)) {
                    throw c.error("Unknown permission scheme '" + scheme
                            + "'");
                }
                scalars.put(key, scheme);
            } else if ("activationConfiguration".equals(key)) {
                int aS = c.pos();
                actConn = Protocol.unquotePath(c.parseQualifiedName());
                actConnSpan = c.spanOf(aS, c.pos() - 1);
            } else if ("HostedService".equals(section)
                    && "contentType".equals(key)) {
                // parsed and DROPPED by the walker — no wire field
                // (probe t2-hostedservice 2026-08-14)
                SectionParse.stringValue(c);
            } else if ("HostedService".equals(section)
                    && "binding".equals(key)) {
                // serviceBindingOrContent: the rule AND serviceBinding
                // both end SEMI_COLON — `binding: p::B;;` (double
                // semicolon); dropped from the wire
                c.parseQualifiedName();
                c.expect(TokenType.SEMI_COLON);
            } else if ("HostedService".equals(section)
                    && ("postValidations".equals(key)
                            || "testSuites".equals(key))) {
                // both blocks are walker-DROPPED on the wire; captured
                // balanced (islands ride as coarse tokens)
                c.skipBalancedBlock();
                c.match(TokenType.SEMI_COLON);
                continue;
            } else {
                throw c.error("unknown key '" + key + "' inside " + kind
                        + " '" + qn + "'");
            }
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        if (functionPath == null || functionSpan == null) {
            throw com.legend.parser.TokenStreamCursor.throwAt(
                    c.tokens(), declStart,
                    kind + " '" + qn + "' needs a function");
        }
        // engine-required fields (leniency audit: the engine deserializer
        // refuses these when absent — structured parity refusals).
        // BigQueryFunction is EXEMPT from ownership: accepted corpus
        // fixtures omit it (the corpus adjudicates).
        if (ownerId == null && userListUsers == null
                && !"BigQueryFunction".equals(kind)) {
            throw com.legend.parser.TokenStreamCursor.throwAt(
                    c.tokens(), declStart, "Field 'ownership' is required");
        }
        if ("DeephavenApp".equals(kind)
                && !scalars.containsKey("applicationName")) {
            // walker-required (mutant delete-field probe)
            throw com.legend.parser.TokenStreamCursor.throwAt(
                    c.tokens(), declStart,
                    "Field 'applicationName' is required");
        }
        if ("MemSqlFunction".equals(kind)
                && !scalars.containsKey("functionName")) {
            throw com.legend.parser.TokenStreamCursor.throwAt(
                    c.tokens(), declStart,
                    "Field 'functionName' is required");
        }
        if ("SnowflakeM2MUdf".equals(kind)) {
            if (!scalars.containsKey("deploymentSchema")) {
                throw com.legend.parser.TokenStreamCursor.throwAt(
                        c.tokens(), declStart,
                        "Field 'deploymentSchema' is required");
            }
            if (!scalars.containsKey("deploymentStage")) {
                throw com.legend.parser.TokenStreamCursor.throwAt(
                        c.tokens(), declStart,
                        "Field 'deploymentStage' is required");
            }
        }
        return new Protocol.PFunctionActivator(pkg, name, kind,
                dec.stereotypes(), dec.taggedValues(), scalars, booleans,
                functionPath, functionSpan, ownerId, userListUsers, actConn,
                actConnSpan, c.spanOf(declStart, c.pos() - 1));
    }



    /** A minimal cursor over a re-lexed slice for the SPI feed. */
    private static final class SliceCursor implements TokenStreamCursor {

        private final com.legend.lexer.TokenStream tokens;
        private final com.legend.parser.Dialect dialect;
        private int pos;

        SliceCursor(com.legend.lexer.TokenStream tokens, com.legend.parser.Dialect dialect) {
            this.tokens = tokens;
            this.dialect = dialect;
        }

        @Override
        public com.legend.parser.Dialect dialect() {
            return dialect;
        }

        @Override
        public com.legend.lexer.TokenStream tokens() {
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
    }
}
