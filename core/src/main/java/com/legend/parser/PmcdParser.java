// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser;

import com.legend.lexer.Lexer;
import com.legend.lexer.TokenStream;
import com.legend.lexer.TokenType;
import com.legend.protocol.Protocol;
import com.legend.protocol.ProtocolEmitter;
import com.legend.protocol.SourceInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The WHOLE-DOCUMENT drop-in surface: source text in, the engine's full
 * PMCD JSON out — envelope {@code {"_type":"data","elements":[...]}},
 * elements in SOURCE order, and the {@code __internal__::SectionIndex}
 * appended last, exactly as {@code PureGrammarParser.parseModel} builds it
 * (ZPmcdProbe / ZPmcdProbe2).
 *
 * <p>ENGINE-STRICT throughout: every element parses through the strict
 * surface ({@code ElementParser.at}), and an unregistered section refuses
 * in the engine's own words. Site discovery walks the ONE lexer's token
 * stream per section range — the section list comes from the lexer's own
 * header records, never a second text scan.
 *
 * <p>Section spans reproduce the engine's coordinate system: the engine
 * parses {@code "\n###Pure\n" + code} (PureGrammarParser.DEFAULT_SECTION_
 * BEGIN) and reports section spans in that BUFFER's coordinates, where a
 * SECTION_START token INCLUDES its leading newline. See
 * {@link #sectionSpan}.
 */
public final class PmcdParser {



    private PmcdParser() {
    }

    /** THE level this whole class exists for — PmcdParser is the
     *  drop-in surface — named ONCE, used by {@link #at} and the
     *  sub-parser entries. */
    private static final Dialect LEVEL = Dialect.LEGEND_ENGINE;

    /** A parser at {@code site}, at {@link #LEVEL}. */
    private static ElementParser at(TokenStream ts, int site) {
        return ElementParser.at(ts, site, LEVEL);
    }

    /** Parsers whose sections are {@code importAware} on the wire — from
     *  the engine's own extension sources (Default vs ImportAware
     *  CodeSection) plus ZPmcdProbe2; everything else emits
     *  {@code default}. */
    private static final Set<String> IMPORT_AWARE = Set.of("Pure", "Mapping",
            "Runtime", "Connection", "Service", "ExternalFormat", "Diagram",
            "GenerationSpecification", "FileGeneration", "HostedService",
            "QueryPostProcessor", "FunctionJar", "Persistence",
            "Deephaven", "Elasticsearch");

    /** Tail sections (site kind 12) — mirror of the registry's
     *  elementwise grammars. */
    /** The reference refuses residue after a section name (lexer records
     *  it; C12 sentinel TestGrammarToJsonApi#42). ONE owner for both the
     *  document and the strict element surface. */
    public static void requireCleanHeader(
            com.legend.lexer.TokenStream.SectionHeader h) {
        if (h.residueToken() != null) {
            throw new com.legend.parser.ParseException("Unexpected token '"
                    + h.residueToken() + "'", h.residueLine(), h.residueCol());
        }
    }

    private static final List<String> TAIL_SECTIONS = List.of("Text",
            "GenerationSpecification", "FileGeneration", "Deephaven",
            "MongoDB", "DataQualityValidation", "Elasticsearch",
            "ExternalFormat", "ServiceStore", "DataSpace", "Persistence",
            "Service", "QueryPostProcessor");

    /** THE activator grammar for a section comes from the REGISTRY — the
     *  old hand-merged kind set here accepted MemSqlFunction inside
     *  ###Snowflake while the registry path refused it (adversarial audit
     *  #18: two routing authorities, already disagreeing). */
    private static com.legend.parser.section.FunctionActivatorSectionGrammar
            activatorGrammar(String section) {
        return (com.legend.parser.section.FunctionActivatorSectionGrammar)
                SectionGrammarRegistry.lookup(section).orElseThrow(
                        () -> new IllegalStateException(
                                "no activator grammar registered for '"
                                        + section + "'"));
    }

    /** One parsed element: its wire JSON, the path the sectionIndex
     *  lists (functions use their MANGLED path, like the engine), and the
     *  PROTOCOL type (for rule-grouping — the old JSON-prefix sniff broke
     *  silently on any emitter field-order change, adversarial audit). */
    public record DocElement(@com.legend.Nullable String path, String json,
            Class<? extends Protocol.Element> kind) {
    }

    /** One document section with its parsed elements and imports. */
    public record DocSection(String parserName, List<DocElement> elements,
                             List<String> imports, SourceInfo span) {
    }

    /** Parse {@code source} into the engine's full PMCD JSON. */
    public static String parseDocument(String source) {
        List<DocSection> sections = parseSections(source);
        StringBuilder b = new StringBuilder("{\"_type\":\"data\",\"elements\":[");
        boolean first = true;
        for (DocSection s : sections) {
            for (DocElement e : s.elements()) {
                if (!first) {
                    b.append(',');
                }
                first = false;
                b.append(e.json());
            }
        }
        List<Protocol.PSection> pSections = new ArrayList<>();
        for (DocSection s : sections) {
            List<String> paths = new ArrayList<>();
            for (DocElement e : s.elements()) {
                // the sectionIndex lists each path ONCE even when a source
                // declares it twice (TestSectionRoundtrip duplicates); the
                // envelope keeps every element
                if (!paths.contains(e.path())) {
                    paths.add(e.path());
                }
            }
            pSections.add(new Protocol.PSection(
                    IMPORT_AWARE.contains(s.parserName()), s.parserName(),
                    paths, s.imports(), s.span()));
        }
        if (!first) {
            b.append(',');
        }
        b.append(ProtocolEmitter.emitElement(new Protocol.PSectionIndex(
                "__internal__", "SectionIndex", pSections)));
        return b.append("]}").toString();
    }

    /** The ordered section list with parsed elements — the assembly
     *  {@link #parseDocument} serializes. */
    public static List<DocSection> parseSections(String source) {
        TokenStream ts = Lexer.tokenize(source);

        // section boundaries from the LEXER's records: every ###X header in
        // source order; the implicit leading section is Pure
        record Bounds(String name, int headerStart, int contentStart) {
        }
        List<Bounds> bounds = new ArrayList<>();
        List<TokenStream.SectionHeader> headers = ts.sectionHeaders();
        bounds.add(new Bounds("Pure", -1, 0));
        for (TokenStream.SectionHeader h : headers) {
            requireCleanHeader(h);
            bounds.add(new Bounds(h.name(), h.nameOffset() - 3,
                    h.contentStartOffset()));
        }

        List<DocSection> out = new ArrayList<>();
        for (int i = 0; i < bounds.size(); i++) {
            Bounds bd = bounds.get(i);
            // the newline directly before a '###' header belongs to that
            // header's SECTION_START token — content stops BEFORE it
            int contentEnd = i + 1 < bounds.size()
                    ? bounds.get(i + 1).headerStart() - 1 : source.length();
            SourceInfo span = sectionSpan(ts, source, bd.name(),
                    bd.headerStart(), contentEnd);
            long[] range = {bd.contentStart(), contentEnd};
            List<DocElement> elements = sectionElements(ts, source,
                    bd.name(), range, bd.headerStart());
            List<String> imports;
            if ("Diagram".equals(bd.name())) {
                // a RAW section has no tokens — its own parse carries the
                // imports (TestDiagramGrammarRoundtrip)
                imports = diagramImports(source, range);
            } else {
                imports = IMPORT_AWARE.contains(bd.name())
                        ? sectionImports(ts, range) : List.of();
            }
            out.add(new DocSection(bd.name(), elements, imports, span));
        }
        return out;
    }

    // ------------------------------------------------------------------
    // Section spans — the engine's buffer coordinates (see class doc)
    // ------------------------------------------------------------------

    /** The engine reports each section over the buffer
     *  {@code "\n###Pure\n" + source}: bufferLine = userLine + 2; a
     *  section starts at its SECTION_START's leading newline (the
     *  synthetic prefix for the implicit section) and stops at the LAST
     *  content character (the newline before the next header belongs to
     *  the NEXT section's token); an EMPTY section stops at its own
     *  header token's end. */
    private static SourceInfo sectionSpan(TokenStream ts, String source,
            String name, int headerStart, int contentEnd) {
        int startLine;
        int startCol;
        if (headerStart < 0) {
            startLine = 1;                          // synthetic '\n###Pure'
            startCol = 1;
        } else if (headerStart == 0) {
            startLine = 2;                          // '\n' of the synthetic
            startCol = 8;                           // after '###Pure'
        } else {
            int nl = headerStart - 1;               // the header's own '\n'
            startLine = ts.lineOf(nl) + 2;
            startCol = ts.columnOf(nl);
        }
        int lastContent = contentEnd - 1;
        // the newline directly before a following header belongs to that
        // header's token, and CR before it stays content; content begins
        // right AFTER '###'+name — the trailing newline is content
        int firstContent = headerStart < 0 ? 0
                : headerStart + 3 + name.length();
        if (headerStart < 0 && lastContent < 0) {
            // the SYNTHETIC prefix's trailing newline is the implicit
            // section's content — UNLESS the source's first char is a
            // header, whose SECTION_START token consumes it
            // (garmanKlassVolatility vs s3, ZPmcdProbe2)
            return lastContent == -1 ? new SourceInfo("", 1, 1, 2, 8)
                    : new SourceInfo("", 1, 1, 1, 8);
        }
        if (lastContent < firstContent) {
            // EMPTY section: span = its own SECTION_START token
            int len = 4 + name.length();            // '\n###' + name
            if (headerStart == 0) {
                // the leading '\n' is the synthetic prefix's second one
                return new SourceInfo("", 2, 8, 2, 7 + len);
            }
            return new SourceInfo("", startLine, startCol, startLine,
                    startCol + len - 1);
        }
        int endLine = ts.lineOf(lastContent) + 2;
        int endCol = ts.columnOf(lastContent);
        return new SourceInfo("", startLine, startCol, endLine, endCol);
    }

    // ------------------------------------------------------------------
    // Imports
    // ------------------------------------------------------------------

    /** The section's {@code import a::b::*;} PREFIX — the sectionIndex
     *  lists the path text minus the trailing {@code ::*}. Engine section
     *  grammars are {@code imports (element)*}: imports can ONLY precede
     *  the first element, so the scan STOPS at the first non-import
     *  token. (The old depth-0 whole-range scan fabricated an import out
     *  of a keyword-as-identifier declaration like
     *  {@code Class import::Class::…} — caught by the harvested
     *  identifier-inclusion fixtures, DIFF batch 2026-08-11.) */
    private static List<String> sectionImports(TokenStream ts, long[] r) {
        List<String> out = new ArrayList<>();
        int cursor = skipTo(ts, r[0]);
        while (cursor < ts.count() && ts.start(cursor) < r[1]
                && ts.type(cursor) == TokenType.IMPORT) {
            StringBuilder p = new StringBuilder();
            cursor++;
            while (cursor < ts.count()
                    && ts.type(cursor) != TokenType.SEMI_COLON) {
                p.append(ts.text(cursor));
                cursor++;
            }
            cursor++;                               // past the ';'
            String imp = Protocol.unquotePath(
                    stripWildcard(p.toString()));
            // the engine DEDUPES section imports keeping the
            // FIRST occurrence (PmcdEquivalenceTest:
            // storeContract/flatDataToPure)
            if (!out.contains(imp)) {
                out.add(imp);
            }
        }
        return out;
    }

    // ------------------------------------------------------------------
    // Per-section element parsing (the drop-in dispatch)
    // ------------------------------------------------------------------

    private static List<DocElement> sectionElements(TokenStream ts,
            String source, String parserName, long[] r, int headerStart) {
        boolean importsOk = IMPORT_AWARE.contains(parserName);
        return switch (parserName) {
            case "Pure" -> strictWalk(ts, r, PmcdParser::pureHead, importsOk,
                    -1, null, null);
            case "Runtime" -> {
                // the engine's runtime walker collects BY GRAMMAR RULE:
                // every `Runtime` first, then every
                // `SingleConnectionRuntime` — NOT source order
                // (PmcdEquivalenceTest join-relational)
                List<TokenType> heads = new ArrayList<>();
                List<DocElement> parsed = strictWalk(ts, r,
                        (t, c) -> ts.type(c) == TokenType.RUNTIME
                                || ts.type(c)
                                        == TokenType.SINGLE_CONNECTION_RUNTIME
                                ? 6 : -1,
                        importsOk, -1, null, heads);
                List<DocElement> out2 = new ArrayList<>();
                for (int i = 0; i < parsed.size(); i++) {
                    if (heads.get(i) == TokenType.RUNTIME) {
                        out2.add(parsed.get(i));
                    }
                }
                for (int i = 0; i < parsed.size(); i++) {
                    if (heads.get(i) != TokenType.RUNTIME) {
                        out2.add(parsed.get(i));
                    }
                }
                yield out2;
            }
            case "Connection" -> strictWalk(ts, r,
                    (t, c) -> CONNECTION_FLAVORS.contains(ts.text(c)) ? 7 : -1,
                    importsOk, -1, null, null);
            case "Relational" -> strictWalk(ts, r,
                    (t, c) -> ts.type(c) == TokenType.DATABASE
                            // database: documentation? DATABASE ... (4.145.0)
                            || (ts.type(c) == TokenType.DOC_STRING && c + 1 < ts.count()
                                    && ts.type(c + 1) == TokenType.DATABASE) ? 8 : -1,
                    importsOk, -1, null, null);
            case "Mapping" -> {
                // the aggLambdaShift anchor is the HEADER line + 1 (the
                // engine sub-parses content whose text starts at the
                // header's own newline)
                int line = ts.lineOf(Math.max(headerStart, 0)) + 1;
                yield strictWalk(ts, r,
                        (t, c) -> ts.type(c) == TokenType.MAPPING ? 9 : -1,
                        importsOk, line, null, null);
            }
            case "Data" -> strictWalk(ts, r,
                    (t, c) -> "Data".equals(ts.text(c))
                            // dataElement: documentation? DATA ... (4.145.0)
                            || (ts.type(c) == TokenType.DOC_STRING && c + 1 < ts.count()
                                    && "Data".equals(ts.text(c + 1))) ? 10 : -1,
                    importsOk, -1, null, null);
            case "Snowflake", "MemSql", "BigQuery", "HostedService",
                    "FunctionJar" -> {
                var ag = activatorGrammar(parserName);
                yield strictWalk(ts, r,
                        (t, c) -> ag.kinds().contains(ts.text(c)) ? 11 : -1,
                        importsOk, -1, null, null, ag);
            }
            case "Diagram" -> diagramElements(source, r);
            default -> {
                int idx = TAIL_SECTIONS.indexOf(parserName);
                if (idx < 0) {
                    throw new ParseException("'" + parserName
                            + "' is not a known section parser",
                            ts.lineOf((int) r[0]), 1);
                }
                var g = (com.legend.parser.section.ElementwiseSectionGrammar)
                        SectionGrammarRegistry.lookup(parserName).orElseThrow();
                yield ruleGroup(parserName, strictWalk(ts, r,
                        (t, c) -> TokenStreamCursor.IDENTIFIER_TOKENS
                                .contains(ts.type(c))
                                && (c + 1 >= ts.count() || ts.type(c + 1)
                                        != TokenType.PATH_SEPARATOR)
                                ? 12 : -1,
                        importsOk, -1, g, null));
            }
        };
    }

    /** Head rule for {@code ###Pure}: the domain markers, {@code Measure},
     *  nothing else. A marker followed by {@code ::} is a REFERENCE, not a
     *  declaration. */
    private static int pureHead(TokenStream ts, int cursor) {
        TokenType t = ts.type(cursor);
        // documentation stands BEFORE the marker (`documentation? CLASS ...`,
        // 4.145.0): the head is the token after it; the site stays at the
        // literal, which the element parser reads as the declaration's own
        if (t == TokenType.DOC_STRING && cursor + 1 < ts.count()) {
            return pureHead(ts, cursor + 1);
        }
        Integer kind = MARKERS.get(t);
        if (kind != null && (cursor + 1 >= ts.count()
                || ts.type(cursor + 1) != TokenType.PATH_SEPARATOR)) {
            return kind;
        }
        if (t == TokenType.VALID_STRING && "Measure".equals(ts.text(cursor))) {
            return 5;
        }
        return -1;
    }

    /** {@code (stream, cursor) -> site kind, or -1}. */
    private interface HeadRule {
        int kindOf(TokenStream ts, int cursor);
    }

    /** THE STRICT WALK (the endgame's lenient->strict flip): parse the
     *  section range SEQUENTIALLY — leading imports (import-aware sections
     *  only), then one element after another, each parse consuming its own
     *  extent. Anything else REFUSES with the engine's refusal shape; the
     *  old scan-and-skip silently dropped unrecognised content (m3
     *  dialect, native declarations, top-level ^instances), which is how
     *  the document surface stayed lenient. */
    private static List<DocElement> strictWalk(TokenStream ts, long[] r,
            HeadRule rule, boolean importsAllowed, int mappingSectionLine,
            com.legend.parser.section.@com.legend.Nullable ElementwiseSectionGrammar tailGrammar,
            @com.legend.Nullable List<TokenType> headsOut) {
        return strictWalk(ts, r, rule, importsAllowed, mappingSectionLine,
                tailGrammar, headsOut, null);
    }

    private static List<DocElement> strictWalk(TokenStream ts, long[] r,
            HeadRule rule, boolean importsAllowed, int mappingSectionLine,
            com.legend.parser.section.@com.legend.Nullable ElementwiseSectionGrammar tailGrammar,
            @com.legend.Nullable List<TokenType> headsOut,
            com.legend.parser.section.@com.legend.Nullable FunctionActivatorSectionGrammar activatorGrammar) {
        List<DocElement> out = new ArrayList<>();
        int cursor = skipTo(ts, r[0]);
        boolean sawElement = false;
        while (cursor < ts.count() && ts.start(cursor) < r[1]) {
            TokenType t = ts.type(cursor);
            if (t == TokenType.IMPORT) {
                if (!importsAllowed || sawElement) {
                    // imports are PREFIX-only (engine: imports (element)*)
                    throw orphan(ts, cursor);
                }
                // the engine's import statement is EXACTLY
                // `import pkg(::pkg)*::*;` — a dotted Java import the
                // snippet extractor scraped, or a wildcard-less path,
                // REFUSES (the flip closed the skip-to-semicolon hole)
                cursor++;
                boolean wildcard = false;
                while (cursor < ts.count()
                        && ts.type(cursor) != TokenType.SEMI_COLON) {
                    TokenType it = ts.type(cursor);
                    if (it == TokenType.STAR) {
                        wildcard = true;
                    } else if (it != TokenType.PATH_SEPARATOR
                            && !TokenStreamCursor.IDENTIFIER_TOKENS
                                    .contains(it)) {
                        throw orphan(ts, cursor);
                    }
                    cursor++;
                }
                if (!wildcard || cursor >= ts.count()) {
                    throw orphan(ts, Math.min(cursor, ts.count() - 1));
                }
                cursor++;
                continue;
            }
            // DOCUMENTATION (4.145.0): a '''...''' literal at element position
            // belongs to the declaration after it — the head rule reads the
            // keyword past it; the element parser (at `cursor`) reads the
            // literal as the element's own and refuses where its grammar
            // does not admit one
            int kind = rule.kindOf(ts, t == TokenType.DOC_STRING && cursor + 1 < ts.count()
                    ? cursor + 1 : cursor);
            if (kind < 0) {
                throw orphan(ts, cursor);
            }
            sawElement = true;
            if (headsOut != null) {
                // the head is the keyword, not the documentation before it
                headsOut.add(t == TokenType.DOC_STRING && cursor + 1 < ts.count()
                        ? ts.type(cursor + 1) : t);
            }
            int[] end = new int[1];
            DocElement de = parseOneAt(ts, cursor, kind, mappingSectionLine,
                    tailGrammar, activatorGrammar, end);
            if (de != null) {
                out.add(de);
            }
            cursor = end[0];
        }
        return out;
    }

    /** The engine's walkers collect BY GRAMMAR RULE, not source order,
     *  where a section owns several element kinds: schemaSets before
     *  bindings, services before execution environments
     *  (PmcdEquivalenceTest testGrammar / multiExec-embeddedParam). */
    private static List<DocElement> ruleGroup(String parserName,
            List<DocElement> parsed) {
        Class<? extends Protocol.Element> firstKind = switch (parserName) {
            case "ExternalFormat" -> Protocol.PSchemaSet.class;
            case "Service" -> Protocol.PService.class;
            default -> null;
        };
        if (firstKind == null) {
            return parsed;
        }
        List<DocElement> first = new ArrayList<>();
        List<DocElement> rest = new ArrayList<>();
        for (DocElement e : parsed) {
            (firstKind.isAssignableFrom(e.kind()) ? first : rest).add(e);
        }
        first.addAll(rest);
        return first;
    }

    private static List<String> diagramImports(String source, long[] r) {
        int startLine = 1;
        for (int k = 0; k < r[0]; k++) {
            if (source.charAt(k) == '\n') {
                startLine++;
            }
        }
        var parsed = com.legend.parser.section.DiagramSectionGrammar.INSTANCE
                .parseRaw(new com.legend.spi.SectionSource("Diagram",
                        source.substring((int) r[0], (int) r[1]),
                        (int) r[0], (int) r[1], startLine),
                        com.legend.parser.Dialect.LEGEND_ENGINE);
        List<String> out = new ArrayList<>();
        for (String imp : parsed.imports()) {
            String path = stripWildcard(imp);
            if (!out.contains(path)) {
                out.add(path);
            }
        }
        return out;
    }

    /** {@code a::b::*} → {@code a::b} — the sectionIndex lists import
     *  paths without the wildcard segment. */
    private static String stripWildcard(String imp) {
        int cut = imp.lastIndexOf("::");
        return cut >= 0 && "*".equals(imp.substring(cut + 2))
                ? imp.substring(0, cut) : imp;
    }

    private static List<DocElement> diagramElements(String source, long[] r) {
        int startLine = 1;
        for (int k = 0; k < r[0]; k++) {
            if (source.charAt(k) == '\n') {
                startLine++;
            }
        }
        var parsed = com.legend.parser.section.DiagramSectionGrammar.INSTANCE
                .parseRaw(new com.legend.spi.SectionSource("Diagram",
                        source.substring((int) r[0], (int) r[1]),
                        (int) r[0], (int) r[1], startLine),
                        com.legend.parser.Dialect.LEGEND_ENGINE);
        List<DocElement> out = new ArrayList<>();
        for (var pe : parsed.elements()) {
            Protocol.PDiagram d = (Protocol.PDiagram) pe.protocol();
            out.add(new DocElement(d.qualifiedName(),
                    ProtocolEmitter.emitElement(d), d.getClass()));
        }
        return out;
    }

    private static @com.legend.Nullable DocElement parseOneAt(TokenStream ts, int site, int kind,
            int mappingSectionLine,
            com.legend.parser.section.@com.legend.Nullable ElementwiseSectionGrammar tailGrammar,
            com.legend.parser.section.@com.legend.Nullable FunctionActivatorSectionGrammar activatorGrammar,
            int[] endOut) {
        Protocol.Element el;
        String path;
        switch (kind) {
            case 0 -> {
                ElementParser p = at(ts, site);
                Protocol.PClass c = p.parseClassDefinition(false);
                el = c;
                path = c.qualifiedName();
                endOut[0] = p.pos();
            }
            case 1 -> {
                ElementParser p = at(ts, site);
                Protocol.PEnumeration e = p.parseEnumDefinition();
                el = e;
                path = e.qualifiedName();
                endOut[0] = p.pos();
            }
            case 2 -> {
                ElementParser p = at(ts, site);
                Protocol.PProfile pr = p.parseProfileDefinition();
                el = pr;
                path = pr.qualifiedName();
                endOut[0] = p.pos();
            }
            case 3 -> {
                ElementParser p = at(ts, site);
                Protocol.PAssociation a = p.parseAssociationDefinition();
                el = a;
                path = a.qualifiedName();
                endOut[0] = p.pos();
            }
            case 4 -> {
                ElementParser p = at(ts, site);
                Protocol.PFunction f = p.parseFunctionProtocol();
                el = f;
                path = f.pkg().isEmpty() ? f.mangledName()
                        : f.pkg() + "::" + f.mangledName();
                endOut[0] = p.pos();
            }
            case 5 -> {
                ElementParser p = at(ts, site);
                Protocol.PMeasure m = p.parseMeasureDefinition();
                el = m;
                path = m.qualifiedName();
                endOut[0] = p.pos();
            }
            case 6 -> {
                ElementParser p = at(ts, site);
                Protocol.PRuntime rt = p.parseRuntimeProtocol();
                el = rt;
                path = rt.qualifiedName();
                endOut[0] = p.pos();
            }
            case 7 -> {
                ElementParser p = at(ts, site);
                Protocol.PConnection cn = p.parseConnectionProtocol();
                el = cn;
                path = cn.qualifiedName();
                endOut[0] = p.pos();
            }
            case 8 -> {
                Protocol.PDatabase db = DatabaseProtocolParser
                        .parse(ts, site, endOut, LEVEL);
                el = db;
                path = db.qualifiedName();
            }
            case 9 -> {
                Protocol.PMapping mp = MappingProtocolParser.parse(ts,
                        site, mappingSectionLine, endOut,
                        // the drop-in surface parses at the engine level
                        LEVEL);
                el = mp;
                path = mp.qualifiedName();
            }
            case 10 -> {
                Protocol.PDataElement de = MappingProtocolParser
                        .parseData(ts, site, LEVEL, endOut);
                el = de;
                path = de.qualifiedName();
            }
            case 11 -> {
                ElementParser p = at(ts, site);
                Protocol.Element ae = java.util.Objects
                        .requireNonNull(activatorGrammar).parseElement(p);
                endOut[0] = p.pos();
                if (ae instanceof Protocol.PFunctionActivator fa) {
                    if ("SnowflakeAppDeploymentConfiguration"
                            .equals(fa.kind())) {
                        // parsed then DROPPED by the walker: no element,
                        // no section entry (probe 2026-08-14)
                        return null;
                    }
                    el = fa;
                    // the config walkers never register a path — the
                    // section's element list carries a literal null
                    // (EXACT kind names; no suffix matching)
                    path = "BigQueryFunctionDeploymentConfiguration"
                            .equals(fa.kind())
                            || "HostedServiceDeploymentConfiguration"
                                    .equals(fa.kind())
                            ? null : fa.qualifiedName();
                } else {
                    // ExecutionEnvironment hosted by ###HostedService
                    el = ae;
                    path = ((Protocol.PExecutionEnvironment) ae)
                            .qualifiedName();
                }
            }
            case 12 -> {
                ElementParser p = at(ts, site);
                var g = java.util.Objects.requireNonNull(tailGrammar);
                Protocol.Element e = g.parseOne(p);
                el = e;
                path = g.qualifiedNameOf(e);
                endOut[0] = p.pos();
            }
            default -> throw new IllegalStateException(
                    "unknown site kind " + kind);
        }
        return new DocElement(path, ProtocolEmitter.emitElement(el), el.getClass());
    }

    // ------------------------------------------------------------------
    // Site scanners — ONE depth/decl-position discipline per family
    // ------------------------------------------------------------------

    private static final Map<TokenType, Integer> MARKERS = Map.of(
            TokenType.CLASS, 0,
            TokenType.ENUM, 1,
            TokenType.PROFILE, 2,
            TokenType.ASSOCIATION, 3,
            TokenType.FUNCTION, 4);

    /** THE STRICT FLIP (endgame charter): a token at a depth-0
     *  declaration position that no scanner recognises REFUSES — the
     *  old scan-and-skip silently dropped unrecognised content (m3
     *  dialect, native declarations, top-level ^instances), which is
     *  how the document surface stayed lenient. Engine-shaped sources
     *  are untouched: every recognised head parses exactly as before,
     *  in the same order. */
    private static ParseException orphan(TokenStream ts, int cursor) {
        return new ParseException("Unexpected token '" + ts.text(cursor)
                + "'", ts.lineOf(ts.start(cursor)),
                ts.startColumn(cursor));
    }

    private static final Set<String> CONNECTION_FLAVORS = Set.of(
            "JsonModelConnection", "XmlModelConnection",
            "ModelChainConnection", "RelationalDatabaseConnection",
            "ServiceStoreConnection", "DeephavenConnection",
            "MongoDBConnection", "Elasticsearch7ClusterConnection");

    private static int skipTo(TokenStream ts, long offset) {
        // token starts are sorted — binary search, not a linear rescan
        // per section (deep-audit perf)
        int lo = 0;
        int hi = ts.count();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (ts.start(mid) < offset) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

}
