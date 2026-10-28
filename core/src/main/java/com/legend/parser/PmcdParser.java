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
    private static ElementParser at(TokenStream ts, int site, Dialect dialect) {
        return ElementParser.at(ts, site, dialect);
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
     *  silently on any emitter field-order change, adversarial audit) --
     *  and the parsed RECORD itself, the protocol program's hub. */
    public record DocElement(@com.legend.base.Nullable String path,
            Class<? extends Protocol.Element> kind, Protocol.Element element) {

        /** One parsed element. */
        static DocElement of(@com.legend.base.Nullable String path, Protocol.Element element) {
            return new DocElement(path, element.getClass(), element);
        }

        /** Its wire JSON, emitted from the record when asked -- never on the compiler's path, which takes the record
         *  (leg 8: one parser, and the compile does not pay for JSON it does not read). */
        public String json() {
            return ProtocolEmitter.emitElement(element);
        }
    }

    /**
     * A text parsed at a dialect: its records (every element in section order, then the section index), and the
     * sections no grammar claims, which the platform dialect records instead of refusing (the engine's and lite's
     * refuse them by name).
     */
    public record Parsed(Protocol.PureModelContextData model, List<UnclaimedSection> unclaimed) {
    }

    /** A {@code ###} section no registered grammar claims, recorded and counted (the platform dialect), never
     *  silently skipped. */
    public record UnclaimedSection(String name, int startOffset, int endOffset) {
    }

    /** One document section with its parsed elements and imports. */
    public record DocSection(String parserName, List<DocElement> elements,
                             List<String> imports, SourceInfo span) {
    }

    /** Parse {@code source} into the engine's full PMCD JSON: the records of {@link #parseModel}, emitted. */
    public static String parseDocument(String source) {
        return ProtocolEmitter.emit(parseModel(source));
    }

    /**
     * Text to JSON for a model (the conversion legend-engine's {@code grammarToJson/model} makes, its
     * {@code returnSourceInformation} the second argument): {@link #parseDocument}, without any source
     * information when it is off -- every span, the named ones ({@code classSourceInformation}, ...) included, as the
     * engine's parser then records none (its span builder answers null; CorpusSweepTest's claim 1c).
     */
    public static String parseDocument(String source, boolean sourceInformation) {
        return spans(parseDocument(source), sourceInformation);
    }

    /**
     * {@link #parseDocument(String, boolean)} at lite's dialect: the engine's text and lite's declared extensions (a
     * SQLite, MariaDB, MySQL or Deephaven connection, a PreSharedKey sign-in, ...), the same JSON for any text the
     * engine's dialect reads; lite's own words in JSON of lite's own (docs/ONE_CONNECTION_DESK_2026_10_28.md D1: lite's
     * {@code grammarToJson/model}, PureV1Api).
     */
    public static String parseLiteDocument(String source, boolean sourceInformation) {
        return spans(ProtocolEmitter.emit(parse(source, Dialect.LEGEND_LITE).model()), sourceInformation);
    }

    private static String spans(String json, boolean sourceInformation) {
        return sourceInformation ? json : com.legend.protocol.SourceInformation.withoutSpans(json);
    }

    /**
     * Parse {@code source} into the typed protocol records (text to records, the protocol program's parse
     * leg): every element in section order, then the section index the engine appends.
     */
    public static Protocol.PureModelContextData parseModel(String source) {
        return parse(source, LEVEL).model();
    }

    /**
     * THE ONE PARSER (docs/PROTOCOL_PROGRAM_2026_10_05.md, leg 8): {@code source} at {@code dialect} into its records --
     * the engine's section walk and import scoping for every dialect (imports bound to their section), the dialect
     * deciding only what is accepted: the engine's exactly; lite's adds its declared extensions ({@code Primitive});
     * the platform's (lite's own Pure: the prelude, the system metamodel, the test harness) accepts any declaration in
     * its built-in sections, native declarations, and records an unclaimed section where the others refuse it. The
     * compiler builds from these records through its one door ({@code ModelFromProtocol}).
     */
    public static Parsed parse(String source, Dialect dialect) {
        List<UnclaimedSection> unclaimed = new ArrayList<>();
        List<DocSection> sections = parseSections(source, dialect, unclaimed);
        List<Protocol.Element> elements = new ArrayList<>();
        for (DocSection s : sections) {
            for (DocElement e : s.elements()) {
                elements.add(e.element());
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
        elements.add(new Protocol.PSectionIndex("__internal__", "SectionIndex", pSections));
        return new Parsed(new Protocol.PureModelContextData(elements), List.copyOf(unclaimed));
    }

    /** The ordered section list with parsed elements — the assembly
     *  {@link #parseDocument} serializes (the engine's dialect). */
    public static List<DocSection> parseSections(String source) {
        return parseSections(source, LEVEL, new ArrayList<>());
    }

    private static List<DocSection> parseSections(String source, Dialect dialect, List<UnclaimedSection> unclaimed) {
        TokenStream ts = Lexer.tokenize(source);

        // section boundaries from the LEXER's records: every ###X header in
        // source order; the implicit leading section is Pure
        record Bounds(String name, int headerStart, int contentStart, int nameOffset) {
        }
        List<Bounds> bounds = new ArrayList<>();
        List<TokenStream.SectionHeader> headers = ts.sectionHeaders();
        bounds.add(new Bounds("Pure", -1, 0, -1));
        for (TokenStream.SectionHeader h : headers) {
            requireCleanHeader(h);
            bounds.add(new Bounds(h.name(), h.nameOffset() - 3,
                    h.contentStartOffset(), h.nameOffset()));
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
            List<DocElement> elements;
            List<String> imports;
            if (dialect != LEVEL && !BUILT_IN_SECTIONS.contains(bd.name()) && !TAIL_SECTIONS.contains(bd.name())) {
                // a section only a registered grammar can read (lite's and the platform's dialects): the registry's
                Registered reg = registered(ts, source, bd.name(), range, bd.nameOffset(), dialect, unclaimed);
                if (reg == null) {
                    continue;   // unclaimed: recorded, not a section of the model
                }
                elements = reg.elements();
                imports = reg.imports();
            } else {
                elements = sectionElements(ts, source, bd.name(), range, bd.headerStart(), dialect);
                if ("Diagram".equals(bd.name())) {
                    // a RAW section has no tokens — its own parse carries the
                    // imports (TestDiagramGrammarRoundtrip)
                    imports = diagramImports(source, range, dialect);
                } else {
                    imports = IMPORT_AWARE.contains(bd.name())
                            ? sectionImports(ts, range) : List.of();
                }
            }
            out.add(new DocSection(bd.name(), elements, imports, span));
        }
        return out;
    }

    /** The sections this class reads itself (the engine's built-in parsers), beside {@link #TAIL_SECTIONS}. */
    private static final Set<String> BUILT_IN_SECTIONS = Set.of("Pure", "Runtime", "Connection", "Relational",
            "Mapping", "Data", "Snowflake", "MemSql", "BigQuery", "HostedService", "FunctionJar", "Diagram");

    /** A registered grammar's section: its elements, and the imports it carried (as the section index lists them). */
    private record Registered(List<DocElement> elements, List<String> imports) {
    }

    /**
     * A section no built-in parser reads, at lite's or the platform's dialect: through the grammar the registry has for
     * it -- a raw grammar's own parse, an overlay's opaque elements (a foreign grammar, carried whole), a lexable
     * grammar's records. With no grammar, lite's dialect refuses it by name, as the engine's does; the platform's
     * records it unclaimed and goes on (null: not a section of the model).
     */
    private static @com.legend.base.Nullable Registered registered(TokenStream ts, String source, String name,
            long[] r, int nameOffset, Dialect dialect, List<UnclaimedSection> unclaimed) {
        TokenStream.SkippedSection skipped = null;
        for (TokenStream.SkippedSection sk : ts.skippedSections()) {
            if (sk.nameOffset() == nameOffset) {
                skipped = sk;
            }
        }
        var grammar = SectionGrammarRegistry.lookup(name);
        if (grammar.isEmpty()) {
            if (dialect.refusesPlatformDialect()) {
                throw new ParseException("'" + name + "' is not a known section parser",
                        ts.lineOf(nameOffset), ts.columnOf(nameOffset));
            }
            int start = skipped != null ? skipped.startOffset() : (int) r[0];
            int end = skipped != null ? skipped.endOffset() : (int) r[1];
            unclaimed.add(new UnclaimedSection(name, start, end));
            return null;
        }
        var g = grammar.get();
        if (skipped != null) {
            com.legend.spi.SectionSource text = new com.legend.spi.SectionSource(name,
                    source.substring(skipped.startOffset(), skipped.endOffset()), skipped.startOffset(),
                    skipped.endOffset(), ts.lineOf(skipped.startOffset()));
            if (g instanceof com.legend.parser.section.RawSectionGrammar raw) {
                var parsed = raw.parseRaw(text, dialect);
                List<DocElement> out = new ArrayList<>();
                for (var pe : parsed.elements()) {
                    out.add(DocElement.of(Protocol.pathOf(pe.protocol()), pe.protocol()));
                }
                return new Registered(out, importPaths(parsed.imports()));
            }
            if (!g.lexable()) {
                // an OVERLAY grammar owns this opaque section: its elements are carried whole, never opened
                List<Protocol.Element> opaque = new ArrayList<>();
                g.parse(text, new OverlayElementSink(name, opaque));
                List<DocElement> out = new ArrayList<>();
                for (Protocol.Element e : opaque) {
                    out.add(DocElement.of(Protocol.pathOf(e), e));
                }
                return new Registered(out, List.of());
            }
            // a LEXABLE registered grammar whose section the lexer nonetheless raw-skipped: lexer/registry drift (the
            // content would vanish silently; the Elasticsearch 2026-08-10 incident)
            throw new ParseException("section '" + name + "' has a lexable grammar registered but was raw-skipped by"
                    + " the lexer — lexer/registry drift; add it to Lexer.lexableSections()",
                    ts.lineOf(nameOffset), ts.columnOf(nameOffset));
        }
        if (!(g instanceof com.legend.parser.section.LexableSectionGrammar lexable)) {
            throw new IllegalStateException("section '" + name + "' was lexed but its grammar is not lexable");
        }
        // a lexable grammar the registry routes (not a built-in section): its whole token range through it
        var parsed = lexable.parseSection(at(ts, skipTo(ts, r[0]), dialect), (int) r[1]);
        List<DocElement> out = new ArrayList<>();
        for (var pe : parsed.elements()) {
            out.add(DocElement.of(Protocol.pathOf(pe.protocol()), pe.protocol()));
        }
        return new Registered(out, importPaths(parsed.imports()));
    }

    /** {@code a::b::*} imports as a section index lists them ({@code a::b}), each once, in order. */
    private static List<String> importPaths(List<String> imports) {
        List<String> out = new ArrayList<>();
        for (String imp : imports) {
            String path = stripWildcard(imp);
            if (!out.contains(path)) {
                out.add(path);
            }
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
            String source, String parserName, long[] r, int headerStart, Dialect dialect) {
        boolean importsOk = IMPORT_AWARE.contains(parserName);
        if (!dialect.refusesPlatformDialect() && PLATFORM_OPEN_SECTIONS.contains(parserName)) {
            // the platform dialect (lite's own Pure, its test models) reads any declaration in its built-in sections,
            // native ones and headerless stores, mappings and services included, as lite's compiler always has
            int line = "Mapping".equals(parserName) ? ts.lineOf(Math.max(headerStart, 0)) + 1 : -1;
            return strictWalk(ts, r, PmcdParser::platformHead, importsOk, line, null, null, null, dialect);
        }
        return switch (parserName) {
            case "Pure" -> strictWalk(ts, r, PmcdParser::domainHead, importsOk, -1, null, null, null, dialect);
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
                        importsOk, -1, null, heads, null, dialect);
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
                    importsOk, -1, null, null, null, dialect);
            case "Relational" -> strictWalk(ts, r,
                    (t, c) -> ts.type(c) == TokenType.DATABASE
                            // database: documentation? DATABASE ... (4.145.0)
                            || (ts.type(c) == TokenType.DOC_STRING && c + 1 < ts.count()
                                    && ts.type(c + 1) == TokenType.DATABASE) ? 8 : -1,
                    importsOk, -1, null, null, null, dialect);
            case "Mapping" -> {
                // the aggLambdaShift anchor is the HEADER line + 1 (the
                // engine sub-parses content whose text starts at the
                // header's own newline)
                int line = ts.lineOf(Math.max(headerStart, 0)) + 1;
                yield strictWalk(ts, r,
                        (t, c) -> ts.type(c) == TokenType.MAPPING ? 9 : -1,
                        importsOk, line, null, null, null, dialect);
            }
            case "Data" -> strictWalk(ts, r,
                    (t, c) -> "Data".equals(ts.text(c))
                            // dataElement: documentation? DATA ... (4.145.0)
                            || (ts.type(c) == TokenType.DOC_STRING && c + 1 < ts.count()
                                    && "Data".equals(ts.text(c + 1))) ? 10 : -1,
                    importsOk, -1, null, null, null, dialect);
            case "Snowflake", "MemSql", "BigQuery", "HostedService",
                    "FunctionJar" -> {
                var ag = activatorGrammar(parserName);
                yield strictWalk(ts, r,
                        (t, c) -> ag.kinds().contains(ts.text(c)) ? 11 : -1,
                        importsOk, -1, null, null, ag, dialect);
            }
            case "Diagram" -> diagramElements(source, r, dialect);
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
                        importsOk, -1, g, null, null, dialect));
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

    /** Site kinds the engine's sections do not have: a {@code Primitive} declaration (lite's and the platform's), a
     *  {@code native} declaration and a headerless {@code Service} (the platform's), and a construct the platform
     *  dialect reads past (a headerless {@code Diagram}, a top-level {@code ^Instance}, a stray {@code )}). */
    private static final int PRIMITIVE_SITE = 13;
    private static final int NATIVE_SITE = 14;
    private static final int HEADERLESS_SERVICE_SITE = 15;
    private static final int SKIP = -2;

    /** The sections the platform dialect reads with {@link #platformHead}: the built-in ones lite's compiler always
     *  read any declaration in. */
    private static final Set<String> PLATFORM_OPEN_SECTIONS = Set.of("Pure", "Mapping", "Relational", "Runtime",
            "Connection", "Data");

    /**
     * {@code ###Pure} at the engine's and lite's dialects: the engine's declarations, and the two the other dialects
     * add -- {@code Primitive} (lite's) and {@code native} (the platform's) -- recognized so that their element parser
     * refuses them BY NAME where the dialect does not admit them ("Primitive type declarations are not supported",
     * "Unsupported syntax": the dialect quarantine's own words, which the parity sweep classifies), never as a bare
     * unexpected token.
     */
    private static int domainHead(TokenStream ts, int cursor) {
        int kind = pureHead(ts, cursor);
        if (kind >= 0) {
            return kind;
        }
        if (ts.type(cursor) == TokenType.NATIVE) {
            return NATIVE_SITE;
        }
        return TokenStreamCursor.IDENTIFIER_TOKENS.contains(ts.type(cursor)) && "Primitive".equals(ts.text(cursor))
                ? PRIMITIVE_SITE : -1;
    }

    /** The platform dialect's head rule: any declaration lite's compiler reads, in any of its built-in sections. */
    private static int platformHead(TokenStream ts, int cursor) {
        TokenType t = ts.type(cursor);
        if (t == TokenType.DOC_STRING && cursor + 1 < ts.count()) {
            return platformHead(ts, cursor + 1);
        }
        int kind = pureHead(ts, cursor);
        if (kind >= 0) {
            return kind;
        }
        switch (t) {
            case NATIVE -> {
                return NATIVE_SITE;
            }
            case SERVICE -> {
                return HEADERLESS_SERVICE_SITE;
            }
            case RUNTIME, SINGLE_CONNECTION_RUNTIME -> {
                return 6;
            }
            case RELATIONAL_DATABASE_CONNECTION -> {
                return 7;
            }
            case DATABASE -> {
                return 8;
            }
            case MAPPING -> {
                return 9;
            }
            case NEW_SYMBOL, PAREN_CLOSE -> {
                return SKIP;
            }
            default -> {
            }
        }
        if (TokenStreamCursor.IDENTIFIER_TOKENS.contains(t)) {
            String word = ts.text(cursor);
            if (CONNECTION_FLAVORS.contains(word)) {
                return 7;   // every connection flavor the engine's ###Connection reads
            }
            if ("Primitive".equals(word)) {
                return PRIMITIVE_SITE;
            }
            if ("Data".equals(word)) {
                return 10;
            }
            if ("Diagram".equals(word)) {
                return SKIP;
            }
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
            com.legend.parser.section.@com.legend.base.Nullable ElementwiseSectionGrammar tailGrammar,
            @com.legend.base.Nullable List<TokenType> headsOut,
            com.legend.parser.section.@com.legend.base.Nullable FunctionActivatorSectionGrammar activatorGrammar,
            Dialect dialect) {
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
            if (t == TokenType.STRING && cursor + 1 < ts.count() && rule.kindOf(ts, cursor + 1) >= 0) {
                // a single-quoted string before a declaration: documentation is a multi-line literal (the engine's own
                // refusal, parser-equivalence's engine-grammar-fixtures)
                throw new ParseException("Documentation must be written as a multi-line ('''...''') literal",
                        ts.lineOf(ts.start(cursor)), ts.startColumn(cursor));
            }
            int kind = rule.kindOf(ts, t == TokenType.DOC_STRING && cursor + 1 < ts.count()
                    ? cursor + 1 : cursor);
            if (kind == SKIP) {
                // the platform dialect reads past a construct that defines no element (a section scope boundary, as
                // an element is: imports after it are refused)
                ElementParser p = at(ts, cursor, dialect);
                if (!p.skipTopLevelNonElement()) {
                    throw orphan(ts, cursor);
                }
                sawElement = true;
                cursor = p.pos();
                continue;
            }
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
                    tailGrammar, activatorGrammar, end, dialect);
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

    private static List<String> diagramImports(String source, long[] r, Dialect dialect) {
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
                        dialect);
        return importPaths(parsed.imports());
    }

    /** {@code a::b::*} → {@code a::b} — the sectionIndex lists import
     *  paths without the wildcard segment. */
    private static String stripWildcard(String imp) {
        int cut = imp.lastIndexOf("::");
        return cut >= 0 && "*".equals(imp.substring(cut + 2))
                ? imp.substring(0, cut) : imp;
    }

    private static List<DocElement> diagramElements(String source, long[] r, Dialect dialect) {
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
                        dialect);
        List<DocElement> out = new ArrayList<>();
        for (var pe : parsed.elements()) {
            Protocol.PDiagram d = (Protocol.PDiagram) pe.protocol();
            out.add(DocElement.of(d.qualifiedName(), d));
        }
        return out;
    }

    private static @com.legend.base.Nullable DocElement parseOneAt(TokenStream ts, int site, int kind,
            int mappingSectionLine,
            com.legend.parser.section.@com.legend.base.Nullable ElementwiseSectionGrammar tailGrammar,
            com.legend.parser.section.@com.legend.base.Nullable FunctionActivatorSectionGrammar activatorGrammar,
            int[] endOut, Dialect dialect) {
        Protocol.Element el;
        String path;
        switch (kind) {
            case PRIMITIVE_SITE -> {
                ElementParser p = at(ts, site, dialect);
                el = p.parsePrimitiveProtocol();
                path = Protocol.pathOf(el);
                endOut[0] = p.pos();
            }
            case NATIVE_SITE -> {
                ElementParser p = at(ts, site, dialect);
                el = p.parseNativeProtocol();
                path = Protocol.pathOf(el);
                endOut[0] = p.pos();
            }
            case HEADERLESS_SERVICE_SITE -> {
                ElementParser p = at(ts, site, dialect);
                el = com.legend.parser.section.ServiceSectionGrammar.parseElement(p);
                path = Protocol.pathOf(el);
                endOut[0] = p.pos();
            }
            case 0 -> {
                ElementParser p = at(ts, site, dialect);
                Protocol.PClass c = p.parseClassDefinition(false);
                el = c;
                path = c.qualifiedName();
                endOut[0] = p.pos();
            }
            case 1 -> {
                ElementParser p = at(ts, site, dialect);
                Protocol.PEnumeration e = p.parseEnumDefinition();
                el = e;
                path = e.qualifiedName();
                endOut[0] = p.pos();
            }
            case 2 -> {
                ElementParser p = at(ts, site, dialect);
                Protocol.PProfile pr = p.parseProfileDefinition();
                el = pr;
                path = pr.qualifiedName();
                endOut[0] = p.pos();
            }
            case 3 -> {
                ElementParser p = at(ts, site, dialect);
                el = p.parseAssociationDefinition();
                path = Protocol.pathOf(el);
                endOut[0] = p.pos();
            }
            case 4 -> {
                ElementParser p = at(ts, site, dialect);
                Protocol.PFunction f = p.parseFunctionProtocol();
                el = f;
                path = Protocol.pathOf(f);
                endOut[0] = p.pos();
            }
            case 5 -> {
                ElementParser p = at(ts, site, dialect);
                Protocol.PMeasure m = p.parseMeasureDefinition();
                el = m;
                path = m.qualifiedName();
                endOut[0] = p.pos();
            }
            case 6 -> {
                ElementParser p = at(ts, site, dialect);
                Protocol.PRuntime rt = p.parseRuntimeProtocol();
                el = rt;
                path = rt.qualifiedName();
                endOut[0] = p.pos();
            }
            case 7 -> {
                ElementParser p = at(ts, site, dialect);
                Protocol.PConnection cn = p.parseConnectionProtocol();
                el = cn;
                path = cn.qualifiedName();
                endOut[0] = p.pos();
            }
            case 8 -> {
                Protocol.PDatabase db = DatabaseProtocolParser
                        .parse(ts, site, endOut, dialect);
                el = db;
                path = db.qualifiedName();
            }
            case 9 -> {
                // a headerless mapping (the platform dialect) anchors to its enclosing ###Mapping header, if any, as
                // lite's compiler always has
                int line = mappingSectionLine >= 0 ? mappingSectionLine
                        : ts.sectionContentLine("Mapping", ts.start(site));
                Protocol.PMapping mp = MappingProtocolParser.parse(ts,
                        site, line, endOut, dialect);
                el = mp;
                path = mp.qualifiedName();
            }
            case 10 -> {
                Protocol.PDataElement de = MappingProtocolParser
                        .parseData(ts, site, dialect, endOut);
                el = de;
                path = de.qualifiedName();
            }
            case 11 -> {
                ElementParser p = at(ts, site, dialect);
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
                            || "MemSqlFunctionDeploymentConfiguration"
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
                ElementParser p = at(ts, site, dialect);
                var g = java.util.Objects.requireNonNull(tailGrammar);
                Protocol.Element e = g.parseOne(p);
                el = e;
                path = g.qualifiedNameOf(e);
                endOut[0] = p.pos();
            }
            default -> throw new IllegalStateException(
                    "unknown site kind " + kind);
        }
        return DocElement.of(path, el);
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
