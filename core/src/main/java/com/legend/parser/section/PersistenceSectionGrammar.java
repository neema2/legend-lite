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
 * THE {@code ###Persistence} grammar — sixth built-in behind the
 * {@link com.legend.spi.SectionGrammar} seam. Owns {@code Persistence} and
 * {@code PersistenceContext} elements to the corpus-censused scope: the
 * TOP-LEVEL keys are structured (doc, service, persistence pointer, trigger
 * kind) and the deep sub-DSLs (persister, serviceOutputTargets, tests,
 * notifier, platform, serviceParameters, sinkConnection) ride as RAW
 * balanced blocks — the sentinel's LENIENT ratchet arbitrates whether that
 * capture is too blind, and structures deeper if it ever grows.
 *
 * <p>No WIRE shape claimed — emission walls; parity oos unchanged.
 */
public final class PersistenceSectionGrammar
        implements ElementwiseSectionGrammar {

    /** The one stateless instance the registry hands out. */
    public static final PersistenceSectionGrammar INSTANCE =
            new PersistenceSectionGrammar();

    private PersistenceSectionGrammar() {
    }

    @Override
    public String name() {
        return "Persistence";
    }

    @Override
    public String qualifiedNameOf(Protocol.Element e) {
        return switch (e) {
            case Protocol.PPersistence p -> p.qualifiedName();
            case Protocol.PPersistenceContext p -> p.qualifiedName();
            default -> throw new IllegalStateException(
                    "not a persistence-section element: " + e.getClass());
        };
    }

    @Override
    public Protocol.Element parseOne(TokenStreamCursor c) {
        return parseElement(c);
    }

    @Override
    public com.legend.model.PackageableElement toModel(Protocol.Element element) {
        return com.legend.model.FromProtocol.toPersistenceElement(element);
    }

    /** One element: {@code Persistence} or {@code PersistenceContext}. */
    public static Protocol.Element parseElement(TokenStreamCursor c) {
        if (!c.isIdentifierToken(c.peek())) {
            throw c.error("unsupported ###Persistence element: " + c.safeText());
        }
        return switch (c.safeText()) {
            case "Persistence" -> parsePersistence(c);
            case "PersistenceContext" -> parseContext(c);
            default -> throw c.error(
                    "unsupported ###Persistence element: " + c.safeText());
        };
    }

    private static Protocol.PPersistence parsePersistence(TokenStreamCursor c) {
        int declStart = c.pos();
        c.advance();                                // 'Persistence'
        TokenStreamCursor.Decorations dec = c.parseDecorations();
        String qn = Protocol.unquotePath(c.parseQualifiedNameAdmittingBooleans());
        int cut = qn.lastIndexOf("::");
        String pkg = cut < 0 ? "" : qn.substring(0, cut);
        String name = cut < 0 ? qn : qn.substring(cut + 2);
        c.expect(TokenType.BRACE_OPEN);

        String doc = null;
        String triggerKind = null;
        String service = null;
        SourceInfo serviceSpan = null;
        Protocol.PPersistenceNode persister = null;
        Protocol.PPersistenceNotifier notifier = null;
        List<Protocol.PServiceOutputTarget> outputTargets = null;
        List<Protocol.PPersistenceTest> tests = null;

        java.util.Set<String> seen = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            int keyStart = c.pos();
            String key = c.parseIdentifier();
            c.expect(TokenType.COLON);
            if (!seen.add(key)) {
                throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(),
                        declStart, "Field '" + key
                                + "' should be specified only once");
            }
            switch (key) {
                case "doc" -> {
                    doc = SectionParse.stringValue(c);
                    c.expect(TokenType.SEMI_COLON);
                }
                case "service" -> {
                    service = Protocol.unquotePath(c.parseQualifiedNameAdmittingBooleans());
                    c.expect(TokenType.SEMI_COLON);
                    serviceSpan = c.spanOf(keyStart, c.pos() - 1);
                }
                case "trigger" -> {
                    int tTok = c.pos();
                    triggerKind = c.parseIdentifier();
                    if (!"Manual".equals(triggerKind)
                            && !"Cron".equals(triggerKind)) {
                        // grammar alternatives (sibling negative
                        // neg-persistence-invented-trigger-type)
                        throw TokenStreamCursor.throwAt(c.tokens(), tTok,
                                "Unexpected token '" + triggerKind + "'");
                    }
                    c.expect(TokenType.SEMI_COLON);
                }
                case "persister" -> {
                    // validation DEFERRED: the engine walker checks the
                    // element trio (doc/trigger/service) BEFORE visiting
                    // the persister (probed pins #4/#8/#12/#16)
                    persister = parseNode(c);
                    c.match(TokenType.SEMI_COLON);
                }
                case "serviceOutputTargets" -> {
                    outputTargets = parseOutputTargets(c);
                    c.match(TokenType.SEMI_COLON);
                }
                case "notifier" -> {
                    notifier = parseNotifier(c, keyStart);
                    c.match(TokenType.SEMI_COLON);
                }
                case "tests" -> {
                    tests = parseTests(c);
                    c.match(TokenType.SEMI_COLON);
                }
                default -> throw c.error("unknown key '" + key
                        + "' inside Persistence '" + qn + "'");
            }
        }
        c.expect(TokenType.BRACE_CLOSE);
        // walker-required trio (visitPersistence)
        for (String r : new String[] {"doc", "trigger", "service"}) {
            if (!seen.contains(r)) {
                throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(),
                        declStart, "Field '" + r + "' is required");
            }
        }
        if (persister != null) {
            validateNode(c, "persister", persister);
        }
        if (outputTargets != null) {
            // the v2 tree validates the same way as the persister tree —
            // it was UNVALIDATED until the sibling fixtures caught the
            // wrong-kind keys leaking through (sweep 2026-08-14)
            for (Protocol.PServiceOutputTarget t : outputTargets) {
                validateNode(c, "serviceOutput", t.serviceOutput());
                validateNode(c, "target", t.persistenceTarget());
            }
        }
        return new Protocol.PPersistence(pkg, name, dec.stereotypes(),
                dec.taggedValues(), doc,
                java.util.Objects.requireNonNull(triggerKind), service,
                serviceSpan, persister, notifier, outputTargets, tests,
                c.spanOf(declStart, c.pos() - 1));
    }

    /** The engine walker's cardinality contract, keyed {@code slot/kind}:
     *  which fields each node REQUIRES (PersistenceParseTreeWalker's
     *  validateAndExtractRequiredField calls, transcribed 1:1). Every
     *  field — required or optional — additionally validates at most
     *  once (validateAndExtract*Field both enforce size <= 1). */
    private static final java.util.Map<String, List<String>> REQUIRED_FIELDS =
            java.util.Map.ofEntries(
                    java.util.Map.entry("persister/Batch",
                            List.of("sink", "targetShape", "ingestMode")),
                    java.util.Map.entry("persister/Streaming",
                            List.of("sink")),
                    java.util.Map.entry("sink/Relational",
                            List.of("database")),
                    java.util.Map.entry("sink/ObjectStorage",
                            List.of("binding")),
                    java.util.Map.entry("targetShape/Flat",
                            List.of("modelClass", "targetName")),
                    java.util.Map.entry("targetShape/MultiFlat",
                            List.of("modelClass", "transactionScope",
                                    "parts")),
                    java.util.Map.entry("parts/__part__",
                            List.of("modelProperty", "targetName")),
                    java.util.Map.entry("deduplicationStrategy/MaxVersion",
                            List.of("versionField")),
                    java.util.Map.entry(
                            "deduplicationStrategy/DuplicateCount",
                            List.of("duplicateCountName")),
                    java.util.Map.entry("ingestMode/NontemporalSnapshot",
                            List.of("auditing")),
                    java.util.Map.entry("ingestMode/UnitemporalSnapshot",
                            List.of("transactionMilestoning")),
                    java.util.Map.entry("ingestMode/BitemporalSnapshot",
                            List.of("transactionMilestoning",
                                    "validityMilestoning")),
                    java.util.Map.entry("ingestMode/NontemporalDelta",
                            List.of("mergeStrategy", "auditing")),
                    java.util.Map.entry("ingestMode/UnitemporalDelta",
                            List.of("mergeStrategy",
                                    "transactionMilestoning")),
                    java.util.Map.entry("ingestMode/BitemporalDelta",
                            List.of("mergeStrategy", "transactionMilestoning",
                                    "validityMilestoning")),
                    java.util.Map.entry("ingestMode/AppendOnly",
                            List.of("auditing", "filterDuplicates")),
                    java.util.Map.entry("mergeStrategy/DeleteIndicator",
                            List.of("deleteField", "deleteValues")),
                    java.util.Map.entry("auditing/DateTime",
                            List.of("dateTimeName")),
                    java.util.Map.entry("transactionMilestoning/BatchId",
                            List.of("batchIdInName", "batchIdOutName")),
                    java.util.Map.entry("transactionMilestoning/DateTime",
                            List.of("dateTimeInName", "dateTimeOutName")),
                    java.util.Map.entry(
                            "transactionMilestoning/BatchIdAndDateTime",
                            List.of("batchIdInName", "batchIdOutName",
                                    "dateTimeInName", "dateTimeOutName")),
                    java.util.Map.entry(
                            "derivation/SourceSpecifiesInDateTime",
                            List.of("sourceDateTimeInField")),
                    java.util.Map.entry(
                            "derivation/SourceSpecifiesInAndOutDateTime",
                            List.of("sourceDateTimeInField",
                                    "sourceDateTimeOutField")),
                    java.util.Map.entry("validityMilestoning/DateTime",
                            List.of("dateTimeFromName", "dateTimeThruName",
                                    "derivation")),
                    java.util.Map.entry(
                            "derivation/SourceSpecifiesFromDateTime",
                            List.of("sourceDateTimeFromField")),
                    java.util.Map.entry(
                            "derivation/SourceSpecifiesFromAndThruDateTime",
                            List.of("sourceDateTimeFromField",
                                    "sourceDateTimeThruField")),
                    java.util.Map.entry("notifyees/Email",
                            List.of("address")),
                    java.util.Map.entry("notifyees/PagerDuty",
                            List.of("url")),
                    // v2 tree (sibling negatives, sweep 2026-08-14)
                    java.util.Map.entry("serviceOutput/TDS",
                            List.of("keys")),
                    java.util.Map.entry("target/Relational",
                            List.of("table")),
                    java.util.Map.entry("temporality/None",
                            List.of("updatesHandling")));

    /** Recursive engine-walker cardinality over the generic node tree:
     *  every key at most once; the {@link #REQUIRED_FIELDS} set for the
     *  node's {@code slot/kind} present. */
    /** Keys the engine grammar PERMITS per node kind — entries only for
     *  the kinds the sibling handoff's protocol-check proved corrupting
     *  (a wrong-kind key emits JSON the engine's Jackson cannot
     *  deserialize); sets grounded in the g4 rules. Grows down-only. */
    private static final java.util.Map<String, List<String>> PERMITTED_FIELDS =
            java.util.Map.ofEntries(
                    java.util.Map.entry("persister/Streaming",
                            List.of("sink")),
                    java.util.Map.entry("targetShape/Flat",
                            List.of("modelClass", "targetName",
                                    "partitionFields",
                                    "deduplicationStrategy")),
                    java.util.Map.entry("transactionMilestoning/BatchId",
                            List.of("batchIdInName", "batchIdOutName")),
                    java.util.Map.entry("datasetType/Snapshot",
                            List.of("partitioning")),
                    // tdsServiceOutput / graphFetchServiceOutput: only
                    // datasetKeys | deduplication | datasetType
                    java.util.Map.entry("serviceOutput/TDS",
                            List.of("keys", "deduplication", "datasetType")),
                    // PersistenceRelationalParserGrammar temporality arms
                    java.util.Map.entry("temporality/None",
                            List.of("auditing", "updatesHandling")),
                    java.util.Map.entry("temporality/Unitemporal",
                            List.of("processingDimension")),
                    java.util.Map.entry("temporality/Bitemporal",
                            List.of("processingDimension",
                                    "sourceDerivedDimension")),
                    java.util.Map.entry("sourceFields/Start",
                            List.of("startField")),
                    java.util.Map.entry("sourceFields/StartAndEnd",
                            List.of("startField", "endField")));

    /** Derivation KINDS are context-split in the .g4: the transaction
     *  arm admits only In/InAndOut, the validity arm only From/
     *  FromAndThru (sibling negative neg-persistence-validity-
     *  derivation-under-transaction). Keyed {@code parentSlot/childKey}. */
    private static final java.util.Map<String, List<String>> CHILD_KINDS =
            java.util.Map.of(
                    "transactionMilestoning/derivation",
                    List.of("SourceSpecifiesInDateTime",
                            "SourceSpecifiesInAndOutDateTime"),
                    "validityMilestoning/derivation",
                    List.of("SourceSpecifiesFromDateTime",
                            "SourceSpecifiesFromAndThruDateTime"));

    /** Slots whose KIND set is closed in the grammar — an invented kind
     *  refuses at parse like the engine, instead of walling at emission
     *  (sibling negatives neg-persistence-invented-target-type /
     *  -validity-milestoning-batchid). */
    private static final java.util.Map<String, List<String>> SLOT_KINDS =
            java.util.Map.of(
                    "validityMilestoning", List.of("DateTime"),
                    "target", List.of("Relational", "__empty__"));

    private static void validateNode(TokenStreamCursor c, String slot,
            Protocol.PPersistenceNode node) {
        // ANCHORED at the NODE's own span start — the engine walker passes
        // each definition ctx to validateAndExtract, so a persister-level
        // error reports at `Batch`, an auditing error at its ingest-mode
        // block, never at the cursor (position-exactness lane: the
        // Persistence family carried 67 of the 288 line diverges)
        int line = node.sourceInformation().startLine();
        int col = node.sourceInformation().startColumn();
        List<String> slotKinds = SLOT_KINDS.get(slot);
        if (slotKinds != null && !slotKinds.contains(node.kind())) {
            throw new com.legend.parser.ParseException(
                    "Unexpected token", line, col);
        }
        // the engine walker extracts each field IN ORDER (once + required
        // check, then an immediate visit that recurses) — so a nested
        // error in an EARLIER field outranks a missing LATER field
        // (probed pin #72: MultiFlat 'parts' beats Batch 'ingestMode')
        List<String> permitted = PERMITTED_FIELDS.get(slot + "/" + node.kind());
        if (permitted != null) {
            for (Protocol.PPersistenceEntry e : node.entries()) {
                if (!permitted.contains(e.key())) {
                    throw new com.legend.parser.ParseException(
                            "Unexpected token", line, col);
                }
            }
        }
        for (Protocol.PPersistenceEntry e : node.entries()) {
            if (e instanceof Protocol.PPersistenceEntry.Node nd) {
                List<String> kinds = CHILD_KINDS.get(slot + "/" + nd.key());
                if (kinds != null && !kinds.contains(nd.node().kind())) {
                    throw new com.legend.parser.ParseException(
                            "Unexpected token", line, col);
                }
            }
        }
        List<String> ordered = REQUIRED_FIELDS.get(slot + "/" + node.kind());
        java.util.Set<String> walked = new java.util.HashSet<>();
        if (ordered != null) {
            for (String r : ordered) {
                walked.add(r);
                long n = node.entries().stream()
                        .filter(e -> r.equals(e.key())).count();
                if (n > 1) {
                    throw new com.legend.parser.ParseException("Field '" + r
                            + "' should be specified only once", line, col);
                }
                if (n == 0) {
                    throw new com.legend.parser.ParseException(
                            "Field '" + r + "' is required", line, col);
                }
                for (Protocol.PPersistenceEntry e : node.entries()) {
                    if (r.equals(e.key())) {
                        recurseEntry(c, e);
                    }
                }
            }
        }
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (Protocol.PPersistenceEntry e : node.entries()) {
            if (!walked.contains(e.key()) && !seen.add(e.key())) {
                throw new com.legend.parser.ParseException("Field '" + e.key()
                        + "' should be specified only once", line, col);
            }
        }
        for (Protocol.PPersistenceEntry e : node.entries()) {
            if (!walked.contains(e.key())) {
                recurseEntry(c, e);
            }
        }
    }

    private static void recurseEntry(TokenStreamCursor c,
            Protocol.PPersistenceEntry e) {
        switch (e) {
            case Protocol.PPersistenceEntry.Node nd ->
                    validateNode(c, nd.key(), nd.node());
            case Protocol.PPersistenceEntry.NodeList nl -> {
                for (Protocol.PPersistenceNode n : nl.nodes()) {
                    validateNode(c, nl.key(), n);
                }
            }
            default -> {
            }
        }
    }

    /** An island node whose span anchors at the CONTENT (line after '#{',
     *  first content column) and ends ONE PAST '}#' — the
     *  persistenceTarget/platform walker quirk (DIFF-pinned). */
    private static Protocol.PPersistenceNode contentAnchoredIslandNode(
            TokenStreamCursor c) {
        String kind = c.parseIdentifier();
        if (c.peek() != TokenType.ISLAND_OPEN
                && c.peek() != TokenType.ISLAND_START) {
            throw c.error("'" + kind + "' needs an island body");
        }
        c.advance();
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
        List<Protocol.PPersistenceEntry> entries = new ArrayList<>();
        parseEntries(new OffsetCursor(com.legend.lexer.Lexer.tokenize(emb),
                c.tokens().startLine(embStart) - 1,
                c.tokens().startColumn(embStart) - 1, c.dialect()),
                entries, null);
        SourceInfo cs = c.spanOf(embStart, embStart);
        int openLine = c.tokens().startLine(embStart - 1);
        int endTok = c.pos();
        c.expect(TokenType.ISLAND_END);
        SourceInfo es = c.spanOf(endTok, endTok);
        return new Protocol.PPersistenceNode(kind, entries,
                com.legend.protocol.SpanOrigin.contentAnchored(
                        Math.max(openLine + 1, cs.startLine()),
                        cs.startColumn(), es.endLine(), es.endColumn()));
    }

    private static Protocol.PPersistenceNode parseNode(TokenStreamCursor c) {
        int s = c.pos();
        if (c.peek() == TokenType.PATH_LITERAL) {
            // a PATH-HEADED node: `#/Class/prop# { ... }` — the graphFetch
            // service output; the path rides the spec wire
            com.legend.protocol.spec.ValueSpecification headPath =
                    com.legend.parser.SpecParser.parse(c.tokens().slice(c.pos(), c.pos() + 1), c.dialect());
            c.advance();
            List<Protocol.PPersistenceEntry> pathEntries = new ArrayList<>();
            c.expect(TokenType.BRACE_OPEN);
            parseEntries(c, pathEntries, TokenType.BRACE_CLOSE);
            c.expect(TokenType.BRACE_CLOSE);
            return new Protocol.PPersistenceNode("#path", headPath,
                    pathEntries, c.spanOf(s, c.pos() - 1));
        }
        String kind = c.parseIdentifier();
        List<Protocol.PPersistenceEntry> entries = new ArrayList<>();
        if (c.match(TokenType.BRACE_OPEN)) {
            parseEntries(c, entries, TokenType.BRACE_CLOSE);
            c.expect(TokenType.BRACE_CLOSE);
        } else if (c.peek() == TokenType.ISLAND_OPEN
                || c.peek() == TokenType.ISLAND_START) {
            // island interiors lex as RAW content chunks — RE-LEX the
            // text and parse on an offset cursor so spans stay
            // file-absolute (the ConnectionSectionGrammar precedent);
            // offsets COMPOSE for islands nested in re-lexed islands
            c.advance();
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
            int baseLine = c.tokens().startLine(embStart) - 1;
            int baseCol = c.tokens().startColumn(embStart) - 1;
            if (c instanceof OffsetCursor oc) {
                baseLine += oc.lineOffset;
                baseCol += oc.colOffset;
            }
            OffsetCursor ic = new OffsetCursor(
                    com.legend.lexer.Lexer.tokenize(emb), baseLine, baseCol,
                    c.dialect());
            c.expect(TokenType.ISLAND_END);
            parseEntries(ic, entries, null);
        } else {
            return new Protocol.PPersistenceNode(kind, entries,
                    c.spanOf(s, s));
        }
        return new Protocol.PPersistenceNode(kind, entries,
                c.spanOf(s, c.pos() - 1));
    }

    private static void parseEntries(TokenStreamCursor c,
            List<Protocol.PPersistenceEntry> out,
            @com.legend.base.Nullable TokenType close) {
        while (!c.atEnd() && (close == null || c.peek() != close)) {
            int keyStart = c.pos();
            String key = c.parseIdentifier();
            c.expect(TokenType.COLON);
            switch (c.peek()) {
                case STRING -> {
                    out.add(new Protocol.PPersistenceEntry.Scalar(key,
                            SectionParse.stringValue(c), true));
                    c.expect(TokenType.SEMI_COLON);
                }
                case TRUE, FALSE -> {
                    boolean v = c.peek() == TokenType.TRUE;
                    c.advance();
                    out.add(new Protocol.PPersistenceEntry.Scalar(key,
                            v ? "true" : "false", false));
                    c.expect(TokenType.SEMI_COLON);
                }
                case BRACKET_OPEN -> {
                    // keys: [ID, NAME] — NO trailing semicolon in grammar;
                    // MultiFlat parts carry KEYLESS braced nodes; string
                    // lists ('Yes', 'true') keep their unquoted values
                    c.advance();
                    if (c.peek() == TokenType.BRACE_OPEN) {
                        List<Protocol.PPersistenceNode> nodes =
                                new ArrayList<>();
                        while (c.peek() != TokenType.BRACKET_CLOSE) {
                            int ps = c.pos();
                            c.expect(TokenType.BRACE_OPEN);
                            List<Protocol.PPersistenceEntry> pe =
                                    new ArrayList<>();
                            parseEntries(c, pe, TokenType.BRACE_CLOSE);
                            c.expect(TokenType.BRACE_CLOSE);
                            nodes.add(new Protocol.PPersistenceNode("__part__",
                                    pe, c.spanOf(ps, c.pos() - 1)));
                            if (!c.match(TokenType.COMMA)) {
                                break;
                            }
                        }
                        c.expect(TokenType.BRACKET_CLOSE);
                        c.match(TokenType.SEMI_COLON);
                        out.add(new Protocol.PPersistenceEntry.NodeList(key,
                                nodes));
                        continue;
                    }
                    if (c.peek() == TokenType.PATH_LITERAL) {
                        List<com.legend.protocol.spec.ValueSpecification>
                                specs = new ArrayList<>();
                        while (c.peek() != TokenType.BRACKET_CLOSE) {
                            specs.add(com.legend.parser.SpecParser.parse(c.tokens().slice(c.pos(), c.pos() + 1), c.dialect()));
                            c.expect(TokenType.PATH_LITERAL);
                            if (!c.match(TokenType.COMMA)) {
                                break;
                            }
                        }
                        c.expect(TokenType.BRACKET_CLOSE);
                        c.match(TokenType.SEMI_COLON);
                        out.add(new Protocol.PPersistenceEntry.PathList(key,
                                specs));
                        continue;
                    }
                    List<String> vals = new ArrayList<>();
                    while (c.peek() != TokenType.BRACKET_CLOSE) {
                        if (c.peek() == TokenType.STRING) {
                            vals.add(SectionParse.stringValue(c));
                        } else if ("deleteValues".equals(key)) {
                            // mergeStrategyDeleteValues: STRING (COMMA
                            // STRING)* — bare identifiers refuse (sibling
                            // negative neg-persistence-mergestrategy-
                            // deletevalues-identifier)
                            throw c.error("Unexpected token '"
                                    + c.safeText() + "'");
                        } else {
                            vals.add(c.parseIdentifier());
                        }
                        if (!c.match(TokenType.COMMA)) {
                            break;
                        }
                    }
                    c.expect(TokenType.BRACKET_CLOSE);
                    c.match(TokenType.SEMI_COLON);
                    out.add(new Protocol.PPersistenceEntry.Strings(key, vals));
                }
                case PATH_LITERAL -> {
                    out.add(new Protocol.PPersistenceEntry.PathValue(key,
                            com.legend.parser.SpecParser.parse(c.tokens().slice(c.pos(), c.pos() + 1), c.dialect())));
                    c.advance();
                    c.expect(TokenType.SEMI_COLON);
                }
                case INTEGER -> {
                    // dataProcessingUnits: 10 — bare numbers stay unquoted
                    out.add(new Protocol.PPersistenceEntry.Scalar(key,
                            c.text(), false));
                    c.advance();
                    c.expect(TokenType.SEMI_COLON);
                }
                default -> {
                    String head = Protocol.unquotePath(c.parseQualifiedNameAdmittingBooleans());
                    // dotted table names: `table: schemaA.personTable;`
                    while (c.peek() == TokenType.DOT) {
                        c.advance();
                        head += "." + c.parseIdentifier();
                    }
                    if (c.peek() == TokenType.BRACE_OPEN
                            || c.peek() == TokenType.ISLAND_OPEN
                            || c.peek() == TokenType.ISLAND_START) {
                        c.setPos(keyStart);
                        c.parseIdentifier();
                        c.expect(TokenType.COLON);
                        out.add(new Protocol.PPersistenceEntry.Node(key,
                                parseNode(c)));
                        c.match(TokenType.SEMI_COLON);
                    } else if ("database".equals(key)
                            || "binding".equals(key)) {
                        // pointer keys — span covers `key: path;`;
                        // database wires type STORE, binding no type
                        c.expect(TokenType.SEMI_COLON);
                        out.add(new Protocol.PPersistenceEntry.Pointer(key,
                                head, c.spanOf(keyStart, c.pos() - 1)));
                    } else if (!head.isEmpty()
                            && Character.isUpperCase(head.charAt(0))
                            && head.chars().anyMatch(Character::isLowerCase)
                            && !head.contains("::")
                            && !"table".equals(key)
                            && !"targetName".equals(key)) {
                        // a CamelCase bare keyword = a leaf node
                        // (deduplication: None) — lowercase identifiers,
                        // ALL_CAPS enums and name-valued keys (table:
                        // TableA) are scalars. Leaf spans are
                        // END-EXCLUSIVE (DIFF-pinned)
                        c.expect(TokenType.SEMI_COLON);
                        SourceInfo ks = c.spanOf(keyStart + 2, keyStart + 2);
                        out.add(new Protocol.PPersistenceEntry.Node(key,
                                new Protocol.PPersistenceNode(head,
                                        List.of(),
                                        new SourceInfo(ks.sourceId(),
                                                ks.startLine(),
                                                ks.startColumn(),
                                                ks.endLine(),
                                                ks.endColumn() + 1))));
                    } else {
                        c.expect(TokenType.SEMI_COLON);
                        out.add(new Protocol.PPersistenceEntry.Scalar(key,
                                head, false));
                    }
                }
            }
        }
    }

    private static List<Protocol.PServiceOutputTarget> parseOutputTargets(
            TokenStreamCursor c) {
        List<Protocol.PServiceOutputTarget> out = new ArrayList<>();
        c.expect(TokenType.BRACKET_OPEN);
        if (c.peek() == TokenType.BRACKET_CLOSE) {
            // serviceOutputTarget (COMMA ...)* — non-empty in the .g4
            // (sibling negative neg-persistence-empty-serviceoutputtargets)
            throw c.error("Unexpected token ']'");
        }
        while (c.peek() != TokenType.BRACKET_CLOSE && !c.atEnd()) {
            int s = c.pos();
            Protocol.PPersistenceNode serviceOutput = parseNode(c);
            c.expect(TokenType.ARROW);
            Protocol.PPersistenceNode target;
            if (c.peek() == TokenType.BRACE_OPEN) {
                // a KEYLESS target: strictly `{ }` in the .g4
                // (BRACE_OPEN BRACE_CLOSE) — the wire omits the
                // persistenceTarget slot entirely
                int ts = c.pos();
                c.expect(TokenType.BRACE_OPEN);
                c.expect(TokenType.BRACE_CLOSE);
                target = new Protocol.PPersistenceNode("__empty__",
                        List.of(), c.spanOf(ts, c.pos() - 1));
            } else {
                target = contentAnchoredIslandNode(c);
            }
            out.add(new Protocol.PServiceOutputTarget(serviceOutput, target,
                    c.spanOf(s, c.pos() - 1)));
            c.match(TokenType.COMMA);
        }
        c.expect(TokenType.BRACKET_CLOSE);
        return out;
    }

    private static Protocol.PPersistenceNotifier parseNotifier(
            TokenStreamCursor c, int keyStart) {
        c.expect(TokenType.BRACE_OPEN);
        List<Protocol.PPersistenceNode> notifyees = new ArrayList<>();
        boolean notifyeesSpelled = false;
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            c.expect(TokenType.COLON);
            if (!"notifyees".equals(key)) {
                throw c.error("unknown notifier key '" + key + "'");
            }
            if (notifyeesSpelled) {
                throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(),
                        keyStart, "Field 'notifyees' should be specified"
                                + " only once");
            }
            notifyeesSpelled = true;
            c.expect(TokenType.BRACKET_OPEN);
            while (c.peek() != TokenType.BRACKET_CLOSE) {
                Protocol.PPersistenceNode n = parseNode(c);
                validateNode(c, "notifyees", n);
                notifyees.add(n);
                if (!c.match(TokenType.COMMA)) {
                    break;
                }
            }
            c.expect(TokenType.BRACKET_CLOSE);
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        if (!notifyeesSpelled) {
            throw com.legend.parser.TokenStreamCursor.throwAt(c.tokens(),
                    keyStart, "Field 'notifyees' is required");
        }
        return new Protocol.PPersistenceNotifier(notifyees,
                c.spanOf(keyStart, c.pos() - 1));
    }

    private static List<Protocol.PPersistenceTest> parseTests(
            TokenStreamCursor c) {
        List<Protocol.PPersistenceTest> out = new ArrayList<>();
        c.expect(TokenType.BRACKET_OPEN);
        while (c.peek() != TokenType.BRACKET_CLOSE && !c.atEnd()) {
            int s = c.pos();
            String id = c.parseIdentifier();
            c.expect(TokenType.COLON);
            c.expect(TokenType.BRACE_OPEN);
            List<Protocol.PPersistenceTestBatch> batches = new ArrayList<>();
            boolean fromServiceOutput = false;
            com.legend.protocol.spec.ValueSpecification graphFetchPath = null;
            java.util.Set<String> seenKeys = new java.util.HashSet<>();
            boolean batchesSpelled = false;
            while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                String key = c.parseIdentifier();
                c.expect(TokenType.COLON);
                if (!seenKeys.add(key)) {
                    throw c.error("Field '" + key
                            + "' should be specified only once");
                }
                switch (key) {
                    case "testBatches" -> {
                        batchesSpelled = true;
                        parseTestBatches(c, batches);
                    }
                    case "isTestDataFromServiceOutput" -> {
                        fromServiceOutput = c.match(TokenType.TRUE);
                        if (!fromServiceOutput) {
                            c.expect(TokenType.FALSE);
                        }
                        c.expect(TokenType.SEMI_COLON);
                    }
                    case "graphFetchPath" -> {
                        graphFetchPath = com.legend.parser.SpecParser.parse(c.tokens().slice(c.pos(), c.pos() + 1), c.dialect());
                        c.expect(TokenType.PATH_LITERAL);
                        c.expect(TokenType.SEMI_COLON);
                    }
                    default -> throw c.error("unknown test key '" + key
                            + "'");
                }
            }
            c.expect(TokenType.BRACE_CLOSE);
            if (!batchesSpelled) {
                throw com.legend.parser.TokenStreamCursor.throwAt(
                        c.tokens(), s, "Field 'testBatches' is required");
            }
            out.add(new Protocol.PPersistenceTest(id, batches,
                    fromServiceOutput, graphFetchPath,
                    c.spanOf(s, c.pos() - 1)));
            c.match(TokenType.COMMA);
        }
        c.expect(TokenType.BRACKET_CLOSE);
        return out;
    }

    private static void parseTestBatches(TokenStreamCursor c,
            List<Protocol.PPersistenceTestBatch> out) {
        c.expect(TokenType.BRACKET_OPEN);
        while (c.peek() != TokenType.BRACKET_CLOSE && !c.atEnd()) {
            int s = c.pos();
            String id = c.parseIdentifier();
            c.expect(TokenType.COLON);
            c.expect(TokenType.BRACE_OPEN);
            Protocol.PPersistenceNode connData = null;
            SourceInfo connSpan = null;
            SourceInfo dataSpan = null;
            List<Protocol.PPersistenceAssert> asserts = new ArrayList<>();
            boolean assertsSpelled = false;
            java.util.Set<String> seenBatchKeys = new java.util.HashSet<>();
            while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                int keyStart = c.pos();
                String key = c.parseIdentifier();
                c.expect(TokenType.COLON);
                if (!seenBatchKeys.add(key)) {
                    throw c.error("Field '" + key
                            + "' should be specified only once");
                }
                switch (key) {
                    case "data" -> {
                        c.expect(TokenType.BRACE_OPEN);
                        int connKey = c.pos();
                        String ck = c.parseIdentifier();
                        if (!"connection".equals(ck)) {
                            throw c.error("unknown data key '" + ck + "'");
                        }
                        c.expect(TokenType.COLON);
                        c.expect(TokenType.BRACE_OPEN);
                        connData = parseNode(c);
                        c.expect(TokenType.BRACE_CLOSE);
                        connSpan = c.spanOf(connKey, c.pos() - 1);
                        c.expect(TokenType.BRACE_CLOSE);
                        dataSpan = c.spanOf(keyStart, c.pos() - 1);
                    }
                    case "asserts" -> {
                        assertsSpelled = true;
                        c.expect(TokenType.BRACKET_OPEN);
                        while (c.peek() != TokenType.BRACKET_CLOSE) {
                            String aid = c.parseIdentifier();
                            c.expect(TokenType.COLON);
                            // the wire span starts at the assertion VALUE
                            // (kind keyword), not the id (DIFF-pinned)
                            int as = c.pos();
                            Protocol.PPersistenceNode an = parseNode(c);
                            asserts.add(new Protocol.PPersistenceAssert(aid,
                                    an, c.spanOf(as, c.pos() - 1)));
                            if (!c.match(TokenType.COMMA)) {
                                break;
                            }
                        }
                        c.expect(TokenType.BRACKET_CLOSE);
                    }
                    default -> throw c.error("unknown testBatch key '" + key
                            + "'");
                }
            }
            c.expect(TokenType.BRACE_CLOSE);
            if (connData == null || connSpan == null || dataSpan == null) {
                throw com.legend.parser.TokenStreamCursor.throwAt(
                        c.tokens(), s, "Field 'data' is required");
            }
            if (!assertsSpelled) {
                // engine deserializer parity (leniency audit row #30)
                throw com.legend.parser.TokenStreamCursor.throwAt(
                        c.tokens(), s, "Field 'asserts' is required");
            }
            out.add(new Protocol.PPersistenceTestBatch(id, connData,
                    connSpan, dataSpan, asserts, c.spanOf(s, c.pos() - 1)));
            c.match(TokenType.COMMA);
        }
        c.expect(TokenType.BRACKET_CLOSE);
    }

    private static Protocol.PPersistenceContext parseContext(
            TokenStreamCursor c) {
        int declStart = c.pos();
        c.advance();                                // 'PersistenceContext'
        TokenStreamCursor.Decorations dec = c.parseDecorations();
        String qn = Protocol.unquotePath(c.parseQualifiedNameAdmittingBooleans());
        int cut = qn.lastIndexOf("::");
        String pkg = cut < 0 ? "" : qn.substring(0, cut);
        String name = cut < 0 ? qn : qn.substring(cut + 2);
        c.expect(TokenType.BRACE_OPEN);

        String persistence = null;
        SourceInfo persistenceSpan = null;
        Protocol.PPersistenceNode platform = null;
        List<Protocol.PCtxParam> params = new ArrayList<>();
        Protocol.PConnectionValue sinkConnection = null;

        java.util.Set<String> seenCtx = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            int keyStart = c.pos();
            String key = c.parseIdentifier();
            c.expect(TokenType.COLON);
            if (!seenCtx.add(key)) {
                throw com.legend.parser.TokenStreamCursor.throwAt(
                        c.tokens(), declStart,
                        "Field '" + key + "' should be specified only once");
            }
            switch (key) {
                case "persistence" -> {
                    persistence = Protocol.unquotePath(c.parseQualifiedNameAdmittingBooleans());
                    c.expect(TokenType.SEMI_COLON);
                    persistenceSpan = c.spanOf(keyStart, c.pos() - 1);
                }
                case "platform" -> {
                    // span quirk: CONTENT start .. '}#' end + 1 (probed)
                    String kind = c.parseIdentifier();
                    if (!"Default".equals(kind) && !"AwsGlue".equals(kind)) {
                        // extension dispatch (IPersistenceParserExtension
                        // .process over the ServiceLoader set): the oracle
                        // registers Default + the cloud extension's
                        // AwsGlue (gate-8 corpus carries its tests)
                        throw c.error("Unsupported persistence platform"
                                + " type '" + kind + "'");
                    }
                    if (c.peek() == TokenType.SEMI_COLON) {
                        if ("AwsGlue".equals(kind)) {
                            // engine-verbatim (cloud pin #2): the cloud
                            // platform REQUIRES a body
                            throw c.error("Persistence platform 'AwsGlue'"
                                    + " must have a non-empty body");
                        }
                        // BARE kind: `platform: Default;` — wire
                        // {"_type":"<kind lowercased>"} whose span is the
                        // kind WORD alone (harvest
                        // persistenceContextPersistencePlatform)
                        int kindTok = c.pos() - 1;
                        c.advance();
                        platform = new Protocol.PPersistenceNode(kind,
                                java.util.List.of(),
                                c.spanOf(kindTok, kindTok));
                        continue;
                    }
                    if (c.peek() != TokenType.ISLAND_OPEN
                            && c.peek() != TokenType.ISLAND_START) {
                        throw c.error("platform '" + kind
                                + "' needs an island body");
                    }
                    c.advance();
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
                    List<Protocol.PPersistenceEntry> entries =
                            new ArrayList<>();
                    parseEntries(new OffsetCursor(
                            com.legend.lexer.Lexer.tokenize(emb),
                            c.tokens().startLine(embStart) - 1,
                            c.tokens().startColumn(embStart) - 1,
                            c.dialect()), entries, null);
                    if ("AwsGlue".equals(kind)) {
                        // engine-verbatim (cloud pins #4/#7); the cloud
                        // extension parses the ISLAND CONTENT as its own
                        // source, so errors anchor at the first content
                        // token — or the ANTLR EOF position (island-end
                        // line, col 1) when the island is empty (probed)
                        var embTs = com.legend.lexer.Lexer.tokenize(emb);
                        int aLine;
                        int aCol;
                        if (embTs.count() > 0) {
                            int l = embTs.startLine(0);
                            aLine = l + c.tokens().startLine(embStart) - 1;
                            aCol = l == 1
                                    ? embTs.startColumn(0)
                                            + c.tokens().startColumn(embStart) - 1
                                    : embTs.startColumn(0);
                        } else {
                            aLine = c.tokens().startLine(c.pos());
                            aCol = 1;
                        }
                        long dpu = entries.stream().filter(en ->
                                "dataProcessingUnits".equals(en.key())).count();
                        if (dpu == 0) {
                            throw new com.legend.parser.ParseException(
                                    "Field 'dataProcessingUnits' is required",
                                    aLine, aCol);
                        }
                        if (dpu > 1) {
                            throw new com.legend.parser.ParseException(
                                    "Field 'dataProcessingUnits' should"
                                    + " be specified only once", aLine, aCol);
                        }
                    }
                    SourceInfo cs = c.spanOf(embStart, embStart);
                    // engine walker start = the line AFTER '#{' even when
                    // content shares the opener's line (DIFF-pinned)
                    int openLine = c.tokens().startLine(embStart - 1);
                    int endTok = c.pos();
                    c.expect(TokenType.ISLAND_END);
                    SourceInfo es = c.spanOf(endTok, endTok);
                    platform = new Protocol.PPersistenceNode(kind, entries,
                            new SourceInfo("",
                                    Math.max(openLine + 1, cs.startLine()),
                                    cs.startColumn(), es.endLine(),
                                    es.endColumn() + 1));
                    c.match(TokenType.SEMI_COLON);
                }
                case "serviceParameters" -> {
                    parseCtxParams(c, params);
                    c.match(TokenType.SEMI_COLON);
                }
                case "sinkConnection" -> {
                    if (c.peek() == TokenType.ISLAND_OPEN
                            || c.peek() == TokenType.ISLAND_START) {
                        // a KINDLESS island: `#{ Flavor { ... } }#`
                        sinkConnection = parseConnectionIsland(c);
                    } else {
                        // pointer form: `sinkConnection: test::conn;`
                        int vs = c.pos();
                        String path = Protocol.unquotePath(
                                c.parseQualifiedNameAdmittingBooleans());
                        sinkConnection = new Protocol.PConnectionPointer(
                                path, c.spanOf(vs, c.pos() - 1));
                    }
                    c.match(TokenType.SEMI_COLON);
                }
                default -> throw c.error("unknown key '" + key
                        + "' inside PersistenceContext '" + qn + "'");
            }
        }
        c.expect(TokenType.BRACE_CLOSE);
        if (persistence == null) {
            throw com.legend.parser.TokenStreamCursor.throwAt(
                    c.tokens(), declStart,
                    "Field 'persistence' is required");
        }
        return new Protocol.PPersistenceContext(pkg, name, dec.stereotypes(),
                dec.taggedValues(), persistence,
                java.util.Objects.requireNonNull(persistenceSpan), platform,
                params, sinkConnection, c.spanOf(declStart, c.pos() - 1));
    }

    /** {@code [ name=value, ... ]} — values are primitives (spec wire),
     *  connection pointers, or embedded connection islands. */
    private static void parseCtxParams(TokenStreamCursor c,
            List<Protocol.PCtxParam> out) {
        c.expect(TokenType.BRACKET_OPEN);
        while (!c.atEnd() && c.peek() != TokenType.BRACKET_CLOSE) {
            int s = c.pos();
            String name = c.parseIdentifier();
            c.expect(TokenType.EQUAL);
            Protocol.PCtxParamValue value;
            if (c.peek() == TokenType.ISLAND_OPEN
                    || c.peek() == TokenType.ISLAND_START) {
                value = new Protocol.PCtxParamValue.ConnectionVal(
                        parseConnectionIsland(c));
            } else if (c.isIdentifierToken(c.peek())
                    && c.tokens().text(c.pos()).length() > 0
                    && !c.tokens().text(c.pos()).startsWith("'")) {
                int vs = c.pos();
                String head = Protocol.unquotePath(c.parseQualifiedNameAdmittingBooleans());
                value = new Protocol.PCtxParamValue.ConnectionPtr(head,
                        c.spanOf(vs, c.pos() - 1));
            } else {
                // a primitive — parsed by THE SpecParser (value spans ride
                // the spec wire)
                int vs = c.pos();
                int d = 0;
                while (!c.atEnd()) {
                    TokenType tk = c.peek();
                    switch (tk) {
                        case PAREN_OPEN, BRACE_OPEN, BRACKET_OPEN -> d++;
                        case PAREN_CLOSE, BRACE_CLOSE, BRACKET_CLOSE -> d--;
                        default -> { }
                    }
                    if ((tk == TokenType.COMMA && d <= 0)
                            || (tk == TokenType.BRACKET_CLOSE && d < 0)) {
                        break;
                    }
                    c.advance();
                }
                value = new Protocol.PCtxParamValue.Primitive(
                        com.legend.parser.SpecParser.parse(c.tokens().slice(vs, c.pos()), c.dialect()));
            }
            out.add(new Protocol.PCtxParam(name, value,
                    c.spanOf(s, c.pos() - 1)));
            c.match(TokenType.COMMA);
        }
        c.expect(TokenType.BRACKET_CLOSE);
    }

    /** A KINDLESS connection island {@code #{ Flavor { ... } }#} — the
     *  interior re-lexes through THE connection grammar with the walker
     *  offset rule, so the embedded value's spans stay file-absolute. */
    private static Protocol.PConnectionValue parseConnectionIsland(
            TokenStreamCursor c) {
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
        Protocol.PConnectionValue value = ConnectionSectionGrammar
                .parseIslandValue(emb, c.tokens().startLine(embStart),
                        c.tokens().startColumn(embStart), c.dialect());
        c.expect(TokenType.ISLAND_END);
        return value;
    }

    /** The island re-lex cursor — spans shift by the island's position in
     *  the enclosing source, and offsets COMPOSE across nesting. */
    private static final class OffsetCursor implements TokenStreamCursor {

        private final com.legend.lexer.TokenStream tokens;
        private final com.legend.parser.Dialect dialect;
        private int pos;
        private final int lineOffset;
        private final int colOffset;

        OffsetCursor(com.legend.lexer.TokenStream tokens, int lineOffset,
                int colOffset, com.legend.parser.Dialect dialect) {
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

        @Override
        public com.legend.protocol.SourceInfo spanOf(int fromTok, int toTok) {
            return com.legend.protocol.SpanOrigin.islandShift(
                    TokenStreamCursor.super.spanOf(fromTok, toTok),
                    lineOffset, colOffset);
        }
    }
}
