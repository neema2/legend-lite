package com.legend.parser;

import com.legend.protocol.Multiplicity;
import com.legend.protocol.TypeExpression;
import com.legend.model.ParsedModel;
import com.legend.model.ImportScope;

import com.legend.lexer.Lexer;
import com.legend.lexer.TokenStream;
import com.legend.lexer.TokenType;
import com.legend.model.AuthenticationSpec;
import com.legend.model.ClassDefinition;
import com.legend.protocol.ConstraintDefinition;
import com.legend.protocol.DerivedPropertyDefinition;
import com.legend.protocol.ParameterDefinition;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionSpecification;
import com.legend.model.FunctionDefinition;
import com.legend.model.NativeFunctionDefinition;
import com.legend.model.LegacyMappingDefinition;
import com.legend.protocol.Realization;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.model.JsonModelConnection;
import com.legend.model.PackageableElement;
import com.legend.model.RuntimeDefinition;
import com.legend.model.ServiceDefinition;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Hand-rolled recursive-descent parser for Pure top-level element
 * declarations &mdash; the driver for step B of the pipeline.
 *
 * <h2>Usage</h2>
 * <pre>
 *   ParsedModel model = ElementParser.parse(pureSource, Dialect.LEGEND_LITE);
 *   // ONE entry, the caller names the level — see {@link Dialect}
 * </pre>
 *
 * <h2>Status (Phase B.4a)</h2>
 * Handles:
 * <ul>
 *   <li>{@code import} statements (wildcard and specific).</li>
 *   <li>{@code Class} (regular and {@code native}) with stereotypes, tagged
 *       values, generic type parameters, superclasses, properties,
 *       derived properties (body parsed eagerly into {@link ValueSpecification}
 *       statements), and class-level constraints (expression parsed eagerly).</li>
 *   <li>{@code Association} &mdash; exactly two ends.</li>
 *   <li>{@code Enum} &mdash; one or more value names.</li>
 *   <li>{@code Profile} &mdash; stereotypes and tags.</li>
 *   <li>{@code function} &mdash; signature plus body parsed eagerly into a
 *       {@code List<ValueSpecification>} via {@link SpecParser#parseCodeBlock}.</li>
 *   <li>{@code Service} &mdash; pattern, doc, execution (query parsed eagerly as a
 *       single {@link ValueSpecification}; mapping/runtime as FQN refs),
 *       and {@code testSuites} block captured as raw text (decision D-3 &mdash;
 *       still deferred until the test-suite grammar lands).</li>
 *   <li>{@code Runtime} (and {@code SingleConnectionRuntime}) &mdash; mappings,
 *       connection bindings, embedded {@code JsonModelConnection} islands.</li>
 *   <li>{@code RelationalDatabaseConnection} &mdash; store, type, specification,
 *       authentication.</li>
 * </ul>
 *
 * <p>B.4a (this slice) adds {@code Database} with the full relational
 * expression sub-AST &mdash; column refs, target columns, literals,
 * function calls, comparisons, boolean combinations, null tests, groups,
 * array literals, and join-navigation chains. Used by {@code Join},
 * {@code Filter}, {@code MultiGrainFilter}, and view column expressions.
 *
 * <p>NOT yet supported (sub-slices B.4b-f):
 * <ul>
 *   <li>{@code Mapping} (next: B.4b Relational class mappings, then
 *       Association / Enum / M2M class mappings / test-suites).</li>
 * </ul>
 * Encountering an unsupported keyword raises a {@link ParseException}.
 *
 * <h2>Deliberate divergences from engine</h2>
 * <ul>
 *   <li>{@link FunctionDefinition} drops engine's compiler-cache fields
 *       ({@code resolvedBody}, {@code parsedReturnType}, parameter
 *       {@code functionType}/{@code parsedType}) &mdash; those bridge parser
 *       output and compiler output via {@code withResolvedBody}, which is
 *       the F-must-not-trigger-G violation AGENTS.md invariant 1 forbids.
 *       Compile-side output lives on {@code compiler.element.TypedFunction}
 *       (Phase F, future).</li>
 *   <li>{@link ServiceDefinition} drops engine's {@code toRegexPattern()},
 *       {@code extractPathParams()}, and convenience factories &mdash;
 *       those are REST-runtime concerns, not parser data.</li>
 *   <li><strong>Unknown keys inside {@code Runtime} / {@code Connection} /
 *       {@code Service} bodies throw</strong> (engine silently
 *       {@code skipToSemicolon}'s them). Matches AGENTS.md invariant 4
 *       (no fallbacks). May fire on forward-compat Pure syntax &mdash;
 *       tracked as decision D-2 in core's README.</li>
 * </ul>
 *
 * <h2>Stateful only during parse</h2>
 * A parser instance is allocated per call to {@link #parse(TokenStream)},
 * driven to completion, then discarded. The resulting {@link ParsedModel}
 * is the only object that survives.
 *
 * <p>Ported from engine's {@code com.gs.legend.parser.PureModelParser},
 * keeping the same recursive-descent shape and identifier-token set
 * for parity testing.
 */
public final class ElementParser implements TokenStreamCursor {

    // (the embedded-island offset constructor and its spanOf override are
    // DELETED, 2026-08-12: the offset form had zero callers, and its latent
    // inconsistency — node spans shifted, error positions not — was flagged
    // by the adversarial audit, EP finding 7)

    // ============================================================
    // Instance state (transient during parse)
    // ============================================================

    final TokenStream tokens;
    int pos;

    // (the legacy mapping parser's mutable scope fields — currentMappingScope,
    // currentTargetSets, currentScopeBlock — died with it, 2026-08-12; they
    // had zero readers and were whitelisted PAST the mutable-field guardrail,
    // adversarial audit EP finding 8)

    ElementParser(TokenStream tokens, Dialect dialect) {
        this.tokens = tokens;
        this.pos = 0;
        this.dialect = dialect;
    }

    // ============================================================
    // Public API
    // ============================================================

    /**
     * A parser positioned at an arbitrary token — the protocol-output entry point used by the
     * equivalence harness (formerly via reflection) and by per-element callers. The token stream
     * must be the WHOLE file's, so source positions stay file-absolute; parsing isolated chunks
     * restarts line numbers at 1, which presents as a parser bug in any positional comparison.
     */
    /** LEGEND-STRICT mode: reject what engine's PureGrammarParser rejects (type and
     *  multiplicity parameters on classes/functions) even though legend-lite's own
     *  dialect supports them. ON for the drop-in surface ({@link #at}), OFF for the
     *  internal pipeline. */
    private final Dialect dialect;

    @Override
    public Dialect dialect() {
        return dialect;
    }

    /** A parser POSITIONED at {@code tokenIndex}, at the CALLER'S level —
     *  the per-element protocol entry (PmcdParser and the wire-pin tests
     *  pass LEGEND_ENGINE; SpecParser's decoration sub-parse passes its
     *  own host dialect). */
    public static ElementParser at(TokenStream tokens, int tokenIndex,
            Dialect dialect) {
        ElementParser p = new ElementParser(tokens, dialect);
        p.pos = tokenIndex;
        return p;
    }

    /**
     * Indexes of {@code marker} tokens at bracket/brace/paren depth 0 — i.e. real top-level
     * declarations, not occurrences inside bodies. Companion to {@link #at}.
     */
    public static java.util.List<Integer> topLevelIndexes(TokenStream ts, TokenType marker) {
        java.util.List<Integer> out = new ArrayList<>();
        int depth = 0;
        for (int i = 0; i < ts.count(); i++) {
            TokenType t = ts.type(i);
            switch (t) {
                case BRACE_OPEN, BRACKET_OPEN, PAREN_OPEN -> depth++;
                case BRACE_CLOSE, BRACKET_CLOSE, PAREN_CLOSE -> depth--;
                default -> {
                    // keywords are legal identifiers in Pure ('function' packages,
                    // 'Class<Any>' types, 'PCT.function' stereotype values) — a marker is
                    // a DECLARATION only where one can start: at stream start (sections
                    // are lexer-skipped), after a block close, or after a semicolon
                    if (depth == 0 && t == marker
                            && (i == 0 || ts.type(i - 1) == TokenType.BRACE_CLOSE
                                    || ts.type(i - 1) == TokenType.SEMI_COLON)
                            && (i + 1 >= ts.count()
                                    || ts.type(i + 1) != TokenType.PATH_SEPARATOR)) {
                        out.add(i);
                    }
                }
            }
        }
        return out;
    }

    /**
     * Tokenize and parse a Pure source string into a {@link ParsedModel}.
     *
     * <p>Linear eager parse: every declared element is parsed in source
     * order. The result is the batch compiler's parser-stage output.
     *
     * <p>Demand-driven element loading (parse only the elements an
     * incremental client touches) is provided by the dormant IDE layer in
     * {@code com.legend.ide}; it is not used by the batch compiler.
     *
     * <p>THE PLATFORM SURFACE ({@link Dialect#PLATFORM}) — legend-lite's
     * own legend-pure dialect superset, for bootstrap/platform-source
     * loading and the test harness ONLY. Caller-whitelisted
     * (PlatformSurfaceGuardrailTest); user entries route through
     * {@link #parseLegendLite} or {@link #parseStrict}.
     */
    /** THE parse entry — ONE way in: the caller NAMES the level
     *  (provenance routing); there is no default and no named-entry
     *  synonym. {@link Dialect#LEGEND_LITE} is the product surface,
     *  {@link Dialect#LEGEND_ENGINE} the drop-in/parity surface,
     *  {@link Dialect#LEGEND_PLATFORM} the bootstrap/m2 superset (naming
     *  it in production trips the guardrail; Pure.java is the one
     *  consumer). */
    public static ParsedModel parse(String source, Dialect dialect) {
        return parse(Lexer.tokenize(
                Objects.requireNonNull(source, "source")), dialect);
    }

    /** Pre-lexed variant of {@link #parse(String, Dialect)}. */
    public static ParsedModel parse(TokenStream tokens, Dialect dialect) {
        return new ElementParser(Objects.requireNonNull(tokens, "tokens"),
                Objects.requireNonNull(dialect, "dialect")).parseModel();
    }

    /**
     * Parse exactly one packageable element from a token-stream slice.
     *
     * <p>The slice is expected to contain the element's full token range
     * (header + body); the first token must be the element's leading
     * keyword (or {@code native} for native classes). After parsing the
     * element the slice should be fully consumed &mdash; trailing tokens
     * cause a {@link ParseException}.
     *
     * <p>Exposed for the IDE layer ({@code com.legend.ide}), which uses it
     * to deep-parse a single element sliced out of a larger token stream
     * via the shallow indexer. Not used by the batch compiler.
     */
    public static PackageableElement parseSingle(TokenStream slice,
            Dialect dialect) {
        ElementParser parser = new ElementParser(slice, dialect);
        PackageableElement element = parser.parseSingleElement();
        if (!parser.atEnd()) {
            parser.error("trailing tokens after element body: expected slice to be fully consumed");
        }
        return element;
    }

    /** Parse one {@code import path::* ;} statement from a token-stream slice. */
    public static String parseSingleImport(TokenStream slice,
            Dialect dialect) {
        ElementParser parser = new ElementParser(slice, dialect);
        String imp = parser.parseImportStatement();
        if (!parser.atEnd()) {
            parser.error("trailing tokens after import statement");
        }
        return imp;
    }

    // ============================================================
    // Top-level
    // ============================================================

    private ParsedModel parseModel() {
        List<PackageableElement> elements = new ArrayList<>();
        ImportScope.Builder imports = new ImportScope.Builder();
        Map<String, Integer> offsets = new HashMap<>();
        // SECTION-scoped imports (real pure): an import following elements
        // opens a NEW section scope; each element records the scope active
        // where it was declared.
        Map<String, ImportScope> elementImports = new HashMap<>();
        ImportScope.Builder sectionImports = new ImportScope.Builder();
        boolean sawElementSinceImport = false;

        // Registry-routed section dispatch (§2.6): a LEXABLE section whose
        // registered grammar is REAL (not the BuiltIn stub) parses as a
        // WHOLE through that grammar — the same routing an overlay gets —
        // instead of element-by-element through this switch.
        java.util.List<ClaimedSection> claimed = claimedSections();
        int nextClaimed = 0;
        int lastElementStart = -1;

        while (!atEnd()) {
            while (nextClaimed < claimed.size() && tokens.start(pos)
                    >= claimed.get(nextClaimed).contentStart()) {
                lastElementStart = claimed.get(nextClaimed).contentStart();
                parseClaimedSection(claimed.get(nextClaimed++), elements,
                        offsets, elementImports, imports);
                sawElementSinceImport = true;   // a section is a scope boundary
            }
            if (atEnd()) {
                break;
            }
            if (peek() == TokenType.IMPORT) {
                if (sawElementSinceImport) {
                    // BOTH references bind imports to a SECTION START:
                    // engine sections are `imports (element)*`, pure M3 is
                    // `imports (element)* EOF`. An import after an element
                    // is legal ONLY across a ### boundary; the old
                    // anywhere-import scope reset was a lite-only
                    // invention (invention census batch 2).
                    if (!sectionBoundaryBetween(lastElementStart,
                            tokens.start(pos))) {
                        error("imports are only allowed at the start of a"
                                + " section (before its first element)");
                    }
                    sectionImports = new ImportScope.Builder();
                    sawElementSinceImport = false;
                }
                String imp = parseImportStatement();
                imports.add(imp);
                sectionImports.add(imp);
            } else if (skipTopLevelNonElement()) {
                sawElementSinceImport = true;
                lastElementStart = tokens.start(Math.max(0, pos - 1));
            } else {
                int at = tokens.start(pos);
                PackageableElement e = parseSingleElement();
                sawElementSinceImport = true;
                lastElementStart = at;
                elementImports.putIfAbsent(e.qualifiedName(), sectionImports.build());
                offsets.putIfAbsent(e.qualifiedName(), at);
                elements.add(e);
            }
        }
        // claimed sections the token walk never reached (empty, or at EOF)
        while (nextClaimed < claimed.size()) {
            parseClaimedSection(claimed.get(nextClaimed++), elements, offsets,
                    elementImports, imports);
        }

        // Sections the lexer raw-skipped, adjudicated by THE registry — and
        // the two surfaces answer DIFFERENTLY, on purpose.
        //
        // The DROP-IN surface refuses an unregistered section in the engine's
        // own words (PureGrammarParser:160). It has to: accepting a file whose
        // sections we cannot read is not tolerance, it is silence — the
        // elements inside simply vanish, and arbitrary nonsense in that
        // section is swallowed as happily as valid grammar.
        //
        // The INTERNAL pipeline keeps reading. Real Legend models routinely
        // mix ###Service / ###DataSpace / ###Persistence with the sections we
        // implement, and legend-lite has to load those models to compile the
        // parts it DOES own — refusing them cost the relational corpus its
        // whole library layer (296 verified -> 0). The skip is recorded on the
        // model rather than silent, so a caller can surface it.
        //
        // Either way, the honest way to make a section acceptable is to
        // REGISTER a grammar for it — the same escape hatch the engine offers.
        java.util.List<com.legend.model.ParsedModel.UnclaimedSection> unclaimed =
                new java.util.ArrayList<>();
        for (var sk : tokens.skippedSections()) {
            var g = SectionGrammarRegistry.lookup(sk.name());
            if (g.isEmpty()) {
                if (dialect.refusesPlatformDialect()) {
                    throw new ParseException("'" + sk.name() + "' is not a known"
                            + " section parser",
                            tokens.lineOf(sk.nameOffset()),
                            tokens.columnOf(sk.nameOffset()));
                }
                unclaimed.add(new com.legend.model.ParsedModel.UnclaimedSection(
                        sk.name(), sk.startOffset(), sk.endOffset()));
            } else if (g.get() instanceof com.legend.parser.section
                    .RawSectionGrammar raw) {
                // a BUILT-IN raw grammar (Diagram): typed elements from raw
                // text — the same claimed-seam routing lexable sections get
                var parsed = raw.parseRaw(new com.legend.spi.SectionSource(
                        sk.name(), tokens.source().substring(sk.startOffset(),
                                sk.endOffset()),
                        sk.startOffset(), sk.endOffset(),
                        tokens.lineOf(sk.startOffset())), dialect);
                ImportScope.Builder scope = new ImportScope.Builder();
                for (String imp : parsed.imports()) {
                    scope.add(imp);
                    imports.add(imp);
                }
                ImportScope sectionScope = scope.build();
                for (var pe : parsed.elements()) {
                    PackageableElement el;
                    try {
                        el = raw.toModel(pe.protocol());
                    } catch (com.legend.parser.section.LexableSectionGrammar
                            .UnsupportedElementShape u) {
                        throw error(u.reason());
                    }
                    elements.add(el);
                    offsets.putIfAbsent(el.qualifiedName(),
                            sk.startOffset() + pe.startOffset());
                    elementImports.putIfAbsent(el.qualifiedName(), sectionScope);
                }
            } else if (!g.get().lexable()) {
                // an OVERLAY grammar owns this opaque section: hand it the
                // raw text (foreign grammars never adopt our lexer); its
                // elements enter the MODEL as the sealed opaque carrier —
                // indexed and routed like any element, never opened
                g.get().parse(new com.legend.spi.SectionSource(sk.name(),
                                tokens.source().substring(sk.startOffset(),
                                        sk.endOffset()),
                                sk.startOffset(), sk.endOffset(),
                                tokens.lineOf(sk.startOffset())),
                        new OverlayElementSink(sk.name(), elements));
            } else {
                // a LEXABLE registered grammar whose section the lexer
                // nonetheless raw-skipped: lexer/registry drift. The old
                // chain had no arm here and the content VANISHED silently
                // (adversarial audit, ElementParser finding 1) — the exact
                // failure mode the Elasticsearch 2026-08-10 incident hit.
                throw new ParseException("section '" + sk.name()
                        + "' has a lexable grammar registered but was"
                        + " raw-skipped by the lexer — lexer/registry drift;"
                        + " add it to Lexer.lexableSections()",
                        tokens.lineOf(sk.nameOffset()),
                        tokens.columnOf(sk.nameOffset()));
            }
        }
        return new ParsedModel(elements, imports.build(), tokens.source(),
                offsets, elementImports, java.util.Map.of(), unclaimed);
    }

    /** A lexable section owned by a REAL registered grammar: its whole token
     *  range parses through the registry, never this file's switch. {@code
     *  end} is the char offset of the next {@code ###} header (or EOF). */
    private record ClaimedSection(
            com.legend.parser.section.LexableSectionGrammar grammar,
            int contentStart, int end) {
    }

    private java.util.List<ClaimedSection> claimedSections() {
        var headers = tokens.sectionHeaders();
        if (headers.isEmpty()) {
            return java.util.List.of();
        }
        var out = new java.util.ArrayList<ClaimedSection>();
        for (int i = 0; i < headers.size(); i++) {
            var h = headers.get(i);
            PmcdParser.requireCleanHeader(h);
            var g = SectionGrammarRegistry.lookup(h.name()).orElse(null);
            if (g instanceof com.legend.parser.section.LexableSectionGrammar lg) {
                // a FILE-final section runs to EOF — every remaining token
                // is inside it, so an unbounded end needs no source re-read
                int end = i + 1 < headers.size()
                        ? headers.get(i + 1).nameOffset() - 3   // its '###'
                        : Integer.MAX_VALUE;
                out.add(new ClaimedSection(lg, h.contentStartOffset(), end));
            }
        }
        return out;
    }

    /** One claimed section through its grammar: protocol elements out, the
     *  transform on the {@code FromProtocol} side, section-scoped imports
     *  (engine's built-in sections are import-aware). */
    private void parseClaimedSection(ClaimedSection c,
            List<PackageableElement> elements, Map<String, Integer> offsets,
            Map<String, ImportScope> elementImports,
            ImportScope.Builder fileImports) {
        var parsed = c.grammar().parseSection(this, c.end());
        ImportScope.Builder scope = new ImportScope.Builder();
        for (String imp : parsed.imports()) {
            scope.add(imp);
            fileImports.add(imp);
        }
        ImportScope sectionScope = scope.build();
        for (var pe : parsed.elements()) {
            PackageableElement el;
            try {
                el = c.grammar().toModel(pe.protocol());
            } catch (com.legend.parser.section.LexableSectionGrammar
                    .UnsupportedElementShape u) {
                throw error(u.reason());
            }
            elements.add(el);
            offsets.putIfAbsent(el.qualifiedName(), pe.startOffset());
            elementImports.putIfAbsent(el.qualifiedName(), sectionScope);
        }
    }

    /**
     * Non-model top-level artifacts real pure files carry — {@code Diagram
     * fqn(w,h) { ... }} blocks and top-level {@code ^Instance(...)}
     * declarations. They define no queryable element; consumed and DROPPED
     * so the elements around them load (previously each sank its whole
     * file's parse). Returns false (nothing consumed) for anything else.
     */
    /**
     * A {@code ###Data} element on the RUNNER path. It is fully parsed —
     * the same grammar the byte-parity harness proves — and carried as the
     * sealed opaque element with its protocol JSON: legend-lite's compile
     * model has no data-element concept, so nothing here can be opened, but
     * the element is still indexed and named rather than silently dropped.
     */
    /** PROTOCOL-FIRST. */
    private PackageableElement dataElement() {
        int start = pos;
        com.legend.protocol.Protocol.PDataElement de =
                MappingProtocolParser.parseData(tokens, start, dialect);
        advance();                                  // 'Data'
        parseDecorations();
        parseQualifiedName();
        skipBalancedBlock();         // { <body> }
        return new com.legend.model.OpaqueElementDefinition(de.qualifiedName(),
                "Data", com.legend.protocol.ProtocolEmitter.emitElement(de));
    }

    private boolean skipTopLevelNonElement() {
        if (isIdentifierToken(peek()) && "Diagram".equals(text())) {
            advance();
            parseQualifiedName();       // the diagram's name — anything else
                                        // after it is a parse error, never an
                                        // unbounded token skip (audit 8 S9)
            if (peek() == TokenType.PAREN_OPEN) {
                skipBalancedBlock();    // (width=..., height=...)
            }
            if (peek() == TokenType.BRACE_OPEN) {
                skipBalancedBlock();    // { TypeView ... }
            }
            return true;
        }
        if (peek() == TokenType.NEW_SYMBOL) {
            advance();
            parseQualifiedName();       // the instance's type reference
            if (peek() != TokenType.PAREN_OPEN) {
                throw error("top-level ^Instance must be followed by (...)");
            }
            skipBalancedBlock();
            return true;
        }
        // a STRAY top-level closer: the corpus\u0027s own
        // m2m2rExecutionPlanTests.pure carries an unbalanced extra )
        // (opens=4, closes=5) that the engine tolerates; skip exactly
        // this token, never other junk
        if (peek() == TokenType.PAREN_CLOSE) {
            advance();
            return true;
        }
        return false;
    }

    /**
     * Dispatch on the current token (an element-starting keyword) and
     * parse exactly one packageable element. Used both by
     * {@link #parseModel} when iterating a full file and by
     * {@link #parseSingle} when the IDE layer's shallow scanner
     * ({@code com.legend.ide.ModelIndexer}) has sliced one element out of a
     * larger stream.
     */
    /**
     * THE ONE SHAPE. Every arm is {@code case X -> xElement();} and nothing
     * else — no transform spelled inline, no {@code yield} block, no
     * exception plumbing. Each {@code xElement()} does exactly one thing:
     * parse the PROTOCOL, then transform it into the model.
     *
     * <p>This switch previously carried five different shapes for the same
     * operation — a bare transform call, a transform wrapped in a helper, a
     * {@code yield} block threading {@code endOut}, the same plus a
     * section-line lookup and two catch clauses, and a straight-to-model
     * call — which made it impossible to tell by reading which elements had
     * been migrated. Two of them were one-line wrappers over the protocol
     * path that read exactly like the un-migrated ones.
     *
     * <p>So the STATE is now written down where the work is: every
     * {@code xElement()} is tagged PROTOCOL-FIRST or STRAIGHT-TO-MODEL. The
     * ranked worklist is {@code docs/PROTOCOL_MIGRATION_CENSUS.md}; this
     * switch is its index.
     */
    private PackageableElement parseSingleElement() {
        // PLATFORM: a top-level '''...''' documentation literal is m3's
        // element-doc sugar attached to the NEXT declaration (legend-pure
        // documentation.pure). Doc text does not affect execution — the
        // platform charter — so it reads as leading trivia here. The
        // drop-in/lite surfaces keep refusing it like the engine does.
        // (A5 platform-gap burn 2026-08-15: 43 top-level rows.)
        // DOCUMENTATION (4.145.0): a '''...''' literal before a declaration
        // is the declaration's own (TokenStreamCursor.parseDocumentation) —
        // dispatch reads through it to the keyword; the site consumes it.
        // Until 4.145.0 the platform dialect dropped these as trivia (A5
        // platform-gap burn 2026-08-15); now every dialect keeps them, as
        // the engine does.
        TokenType t = peek();
        if (t == TokenType.DOC_STRING && pos + 1 < tokens.count()) {
            t = peek(1);
        } else if (t == TokenType.STRING && pos + 1 < tokens.count()
                && DECLARATION_HEADS.contains(peek(1))) {
            throw error("Documentation must be written as a multi-line ('''...''') literal");
        }
        // STRICT SECTION BINDING (quarantine doctrine, OwnCorpusConformance):
        // the engine's default/###Pure section admits ONLY domain elements —
        // a Mapping/Runtime/Connection/Database/Service declared without its
        // section header is a LITE convenience (our own tests are written
        // headerless) that the drop-in surface refuses like the engine does.
        if (dialect.refusesPlatformDialect() && !inNonPureSection(tokens.start(pos))) {
            switch (t) {
                case SERVICE, RUNTIME, SINGLE_CONNECTION_RUNTIME,
                        RELATIONAL_DATABASE_CONNECTION, DATABASE, MAPPING ->
                        throw error("Unexpected token '" + safeText() + "'");
                default -> {
                }
            }
        }
        return switch (t) {
            case CLASS -> classElement(false);
            case NATIVE -> nativeElement();
            case ASSOCIATION -> associationElement();
            case ENUM -> enumElement();
            case PROFILE -> profileElement();
            case FUNCTION -> functionElement();
            case SERVICE -> serviceElement();
            case RUNTIME, SINGLE_CONNECTION_RUNTIME -> runtimeElement();
            case RELATIONAL_DATABASE_CONNECTION -> connectionElement();
            case DATABASE -> databaseElement();
            case MAPPING -> mappingElement();
            case VALID_STRING -> keywordElement(t);
            default -> throw error("unsupported top-level keyword: " + t
                    + " ('" + safeText() + "')");
        };
    }

    /** Whether a {@code ###} section header opens between two char offsets
     *  — the import-legality lookup ({@code (from, to)} exclusive/inclusive;
     *  {@code from < 0} means file start, where imports are always legal). */
    private boolean sectionBoundaryBetween(int from, int to) {
        if (from < 0) {
            return true;
        }
        for (var h : tokens.sectionHeaders()) {
            if (h.nameOffset() > from && h.nameOffset() <= to) {
                return true;
            }
        }
        return false;
    }

    /** Char ranges of NON-Pure lexed sections — the strict section-binding
     *  gate's lookup (lazy; only strict parses consult it). */
    private long @com.legend.Nullable [][] nonPureRanges;

    private boolean inNonPureSection(int offset) {
        if (nonPureRanges == null) {
            var headers = tokens.sectionHeaders();
            java.util.List<long[]> rs = new ArrayList<>();
            for (int i = 0; i < headers.size(); i++) {
                var h = headers.get(i);
                if (!"Pure".equals(h.name())) {
                    // the LAST section's range is open-ended — membership
                    // only needs the start bound (no source() re-read)
                    long end = i + 1 < headers.size()
                            ? headers.get(i + 1).nameOffset() - 3
                            : Long.MAX_VALUE;
                    rs.add(new long[]{h.contentStartOffset(), end});
                }
            }
            nonPureRanges = rs.toArray(new long[0][]);
        }
        for (long[] r : nonPureRanges) {
            if (offset >= r[0] && offset < r[1]) {
                return true;
            }
        }
        return false;
    }

    /** PROTOCOL-FIRST. */
    private PackageableElement classElement(boolean isNative) {
        return com.legend.model.FromProtocol.toClassDefinition(
                parseClassDefinition(isNative));
    }

    /** PROTOCOL-FIRST for {@code native Class}; the function arm is not.
     *  The documentation stands BEFORE {@code native} (pure's grammar:
     *  {@code nativeFunction: documentation? NATIVE FUNCTION ...}), so it
     *  is read here and handed to the arm. */
    private PackageableElement nativeElement() {
        int declStart = pos;
        Documentation doc = parseDocumentation();
        if (dialect.refusesPlatformDialect()) {
            // engine-verbatim (dialect quarantine): `native` declarations
            // are pure-dialect only — the engine grammar has no arm for them
            throw error("Unsupported syntax");
        }
        advance();                                  // consume 'native'
        return switch (peek()) {
            case CLASS -> com.legend.model.FromProtocol.toClassDefinition(
                    parseClassDefinition(true, declStart, doc));
            case FUNCTION -> nativeFunctionElement(declStart, doc);
            default -> throw error("expected 'Class' or 'function' after"
                    + " 'native', got " + peek() + " ('" + safeText() + "')");
        };
    }

    /** PROTOCOL-FIRST. */
    private PackageableElement enumElement() {
        return com.legend.model.FromProtocol.toEnumDefinition(
                parseEnumDefinition());
    }

    /** PROTOCOL-FIRST. */
    private PackageableElement profileElement() {
        return com.legend.model.FromProtocol.toProfileDefinition(
                parseProfileDefinition());
    }

    /** A HEADERLESS RelationalDatabaseConnection element (no
     *  {@code ###Connection} header: mixed fixtures, IDE slices). Sectioned
     *  files never reach it — {@link #parseModel} dispatches the whole
     *  section through the registry. */
    private PackageableElement connectionElement() {
        com.legend.protocol.Protocol.PConnection pc =
                com.legend.parser.section.ConnectionSectionGrammar
                        .parseElement(this);
        try {
            return com.legend.model.FromProtocol.toConnectionElement(pc);
        } catch (com.legend.model.FromProtocol.UnsupportedConnectionShape u) {
            throw error(u.reason());
        }
    }

    /** PROTOCOL-FIRST (R3) — the ###Relational model is a TRANSFORM on
     *  protocol, not a second parse. */
    private PackageableElement databaseElement() {
        int[] endOut = new int[1];
        com.legend.protocol.Protocol.PDatabase db =
                DatabaseProtocolParser.parse(tokens, pos, endOut, dialect);
        pos = endOut[0];
        return com.legend.model.FromProtocol.toDatabaseDefinition(db);
    }

    /** PROTOCOL-FIRST (M4) — one parse, one grammar. */
    private PackageableElement mappingElement() {
        int[] endOut = new int[1];
        int sectionLine = tokens.sectionContentLine("Mapping", tokens.start(pos));
        try {
            com.legend.protocol.Protocol.PMapping m =
                    MappingProtocolParser.parse(tokens, pos, sectionLine,
                            endOut, dialect);
            pos = endOut[0];
            return com.legend.model.MappingFromProtocol.toMappingElement(m);
        } catch (com.legend.model.MappingFromProtocol.UnsupportedMappingShape u) {
            // the transform's refusals become ParseExceptions HERE, where the
            // position is known and where com.legend.model need not depend on
            // the parser's exception type
            throw error(u.reason());
        }
    }

    /** Elements whose keyword is not a reserved token. */
    private PackageableElement keywordElement(TokenType t) {
        if ("Primitive".equals(safeText())) {
            // pure-m2 dialect: the ENGINE wire grammar has no Primitive
            // element (its parser dies with a raw InputMismatchException,
            // null message — adjudication probe 2026-08-14, upstream
            // defect row); PLATFORM/LITE keep it
            if (dialect.refusesLiteExtensions()) {
                throw error("Primitive type declarations are not supported");
            }
            return primitiveElement();
        }
        if ("Data".equals(safeText())) {
            // strict section binding: Data lives in ###Data only
            if (dialect.refusesPlatformDialect() && !inNonPureSection(tokens.start(pos))) {
                throw error("Unexpected token '" + safeText() + "'");
            }
            return dataElement();
        }
        if ("Measure".equals(safeText())) {
            return measureElement();
        }
        throw error("unsupported top-level keyword: " + t
                + " ('" + safeText() + "')");
    }

    /** PROTOCOL-FIRST — the byte-parity-proven {@link #parseMeasureDefinition}
     *  finally has a caller on the model path (worklist item: finishes
     *  ###Pure). */
    private PackageableElement measureElement() {
        return com.legend.model.FromProtocol.toMeasureDefinition(
                parseMeasureDefinition());
    }

    // ============================================================
    // Import statement
    // ============================================================

    /** {@code import packagePath::* ;} or {@code import packagePath::Type ;} */
    private String parseImportStatement() {
        expect(TokenType.IMPORT);
        StringBuilder sb = new StringBuilder();
        sb.append(parseIdentifier());
        boolean starred = false;
        while (match(TokenType.PATH_SEPARATOR)) {
            sb.append("::");
            if (match(TokenType.STAR)) {
                sb.append("*");
                starred = true;
                break;
            }
            sb.append(parseIdentifier());
        }
        if (!starred) {
            // BOTH references import a package WILDCARD only — engine
            // grammar `IMPORT packagePath STAR`, pure M3 identical. The
            // old specific-import arm (import a::b::C;) was a lite-only
            // invention (invention census batch 2).
            error("an import names a package wildcard — expected '::*'"
                    + " (import " + sb + "::*;)");
        }
        expect(TokenType.SEMI_COLON);
        return sb.toString();
    }

    // ============================================================
    // Class declaration
    // ============================================================

    /** Parses one {@code Class} declaration at the cursor into its protocol record. Public as
     *  the per-element protocol entry point (see {@link #at}); most callers want
     *  {@link #parse(String)} instead. */
    public com.legend.protocol.Protocol.PClass parseClassDefinition(boolean isNative) {
        int classStartTok = pos;
        return parseClassDefinition(isNative, classStartTok, parseDocumentation());
    }

    /** {@code documentation? CLASS stereotypes? taggedValues? ...}; the
     *  element's own range starts at the documentation, as in the engine. */
    private com.legend.protocol.Protocol.PClass parseClassDefinition(boolean isNative,
            int classStartTok, @com.legend.Nullable Documentation doc) {
        expect(TokenType.CLASS);
        List<com.legend.protocol.Protocol.PStereotype> stereotypes = parseStereotypes();
        List<com.legend.protocol.Protocol.PTaggedValue> taggedValues =
                taggedValuesWithDocumentation(doc, parseTaggedValues());
        String qualifiedName = parseQualifiedName();

        List<String> typeParams = parseClassTypeParams();
        List<com.legend.protocol.ParameterDefinition> typeVariables = parseTypeVariableDeclarations();
        if (dialect.refusesLiteExtensions() && !typeParams.isEmpty()) {
            // DECLARED extension LITE-DESIGN-function-types-generics
            // (OWN_CORPUS_DECISIONS §11): LEGEND_LITE parses generics,
            // the exact-engine surface refuses engine-verbatim
            throw error("Type and/or multiplicity parameters are not"
                    + " authorized in Legend Engine");
        }

        // PROJECTION class: `Class X projects Y { > name [expr] | +[props]
        // | * }` (engine class-projection grammar). Parsed as a NOMINAL
        // element so referencing mappings/queries resolve the name; the
        // projection semantics (flattened derived surface) stay loud
        // downstream — parse-level unlock only.
        if (peek() == TokenType.VALID_STRING && "projects".equals(safeText())) {
            if (dialect.refusesPlatformDialect()) {
                // the engine refuses class projections (its walker NPEs,
                // normalized to a developer message); silently emitting an
                // EMPTY class here was accept-divergence + data loss
                // (adversarial audit #11, oracle-verified)
                throw error("class projections are not supported by the engine");
            }
            advance();
            parseQualifiedName();      // the projected source class
            if (peek() == TokenType.BRACE_OPEN) {
                skipBalancedBlock();
            }
            String[] pn = com.legend.protocol.Protocol.splitFqn(qualifiedName);
            return new com.legend.protocol.Protocol.PClass(pn[0], pn[1], typeParams, typeVariables, List.of(),
                    List.of(), List.of(), List.of(), stereotypes, taggedValues, isNative,
                    spanOf(classStartTok, pos - 1));
        }

        List<com.legend.protocol.Protocol.PSuperType> superClasses = new ArrayList<>();
        if (match(TokenType.EXTENDS)) {
            int stTok = pos;
            superClasses.add(new com.legend.protocol.Protocol.PSuperType(
                    parseType(), spanOf(stTok, pos - 1)));
            while (match(TokenType.COMMA)) {
                int nTok = pos;
                superClasses.add(new com.legend.protocol.Protocol.PSuperType(
                        parseType(), spanOf(nTok, pos - 1)));
            }
        }

        List<ConstraintDefinition> constraints = peek() == TokenType.BRACKET_OPEN
                ? parseConstraints()
                : List.of();

        expect(TokenType.BRACE_OPEN);

        List<com.legend.protocol.Protocol.PProperty> properties = new ArrayList<>();
        List<DerivedPropertyDefinition> derivedProperties = new ArrayList<>();
        while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
            // a member's documentation is the member's own (property /
            // qualifiedProperty: documentation? stereotypes? ...)
            if (isDerivedPropertyStart()) {
                derivedProperties.add(parseDerivedProperty());
            } else {
                properties.add(parseProperty());
            }
        }
        expect(TokenType.BRACE_CLOSE);

        String[] pn = com.legend.protocol.Protocol.splitFqn(qualifiedName);
        return new com.legend.protocol.Protocol.PClass(
                pn[0],
                pn[1],
                typeParams,
                typeVariables,
                superClasses,
                properties,
                derivedProperties,
                constraints,
                stereotypes,
                taggedValues,
                isNative,
                spanOf(classStartTok, pos - 1));
    }

    /** Optional generic type parameters: {@code <T>}, {@code <U, V>}, ... */
    /** {@code (x:Integer[1], y:String[*])} after a Class / Primitive name —
     * TYPE VARIABLES (legend-pure m3 Class.typeVariables; new.pure, cast.pure,
     * precisePrimitives.pure). Empty when the header has no parenthesis. */
    private List<com.legend.protocol.ParameterDefinition> parseTypeVariableDeclarations() {
        // PLATFORM grammar (legend-pure m3): the engine's grammar has no type
        // variables — the drop-in surface keeps refusing them where it did
        if (peek() != TokenType.PAREN_OPEN || dialect.refusesPlatformDialect()) {
            return List.of();
        }
        advance();   // '('
        List<com.legend.protocol.ParameterDefinition> out = new ArrayList<>();
        while (peek() != TokenType.PAREN_CLOSE) {
            int start = pos;
            String name = text();
            advance();
            expect(TokenType.COLON);
            com.legend.protocol.TypeExpression type = parseType();
            com.legend.protocol.Multiplicity mult = parseMultiplicity();
            out.add(new com.legend.protocol.ParameterDefinition(name, type, mult, spanOf(start, pos - 1)));
            if (!match(TokenType.COMMA)) {
                break;
            }
        }
        expect(TokenType.PAREN_CLOSE);
        return out;
    }

    private List<String> parseClassTypeParams() {
        if (peek() != TokenType.LESS_THAN) return List.of();
        advance(); // consume <
        List<String> params = new ArrayList<>();
        // M3 DIALECT: `<T|m>` / `<|m>` — multiplicity parameters after
        // the pipe (PCT map.pure's M_MultiplicityHolder<|m>). The names
        // join the parameter list (the checker's late-binding treats
        // them nominally); the exact-engine surface refuses at the
        // caller's gate as before.
        if (peek() != TokenType.PIPE) {
            params.add(parseTypeParamName());
            while (match(TokenType.COMMA)) {
                params.add(parseTypeParamName());
            }
        }
        if (!dialect.refusesPlatformDialect() && match(TokenType.PIPE)) {
            params.add(parseIdentifier());
            while (match(TokenType.COMMA)) {
                params.add(parseIdentifier());
            }
        }
        expect(TokenType.GREATER_THAN);
        return params;
    }

    /** M3 DIALECT: a VARIANCE marker on a type parameter — {@code
     * Path<-U,V|m>} (contravariant {@code -U}, covariant {@code +V}, real
     * m3 path.pure:17). The marker is grammar only: our checker binds type
     * parameters nominally, so the name is what is kept. */
    private String parseTypeParamName() {
        if (!dialect.refusesPlatformDialect()
                && (peek() == TokenType.MINUS || peek() == TokenType.PLUS)) {
            advance();
        }
        return parseIdentifier();
    }

    /**
     * Property starts as {@code identifier (':' ...)}; derived property starts
     * as {@code identifier '(' ...}. Distinguish by lookahead at offset +1
     * across allowed stereotype/tag annotations.
     */
    private boolean isDerivedPropertyStart() {
        int saved = pos;
        // skip a member's documentation literal
        if (peek() == TokenType.DOC_STRING) {
            pos++;
        }
        // skip optional stereotypes <<...>>
        if (peek() == TokenType.LESS_THAN && peek(1) == TokenType.LESS_THAN) {
            int depth = 2;
            pos += 2;
            while (pos < tokens.count() && depth > 0) {
                if (peek() == TokenType.GREATER_THAN) depth--;
                else if (peek() == TokenType.LESS_THAN) depth++;
                pos++;
            }
        }
        // skip optional tagged values { ... }
        if (peek() == TokenType.BRACE_OPEN) {
            int depth = 1;
            pos++;
            while (pos < tokens.count() && depth > 0) {
                if (peek() == TokenType.BRACE_OPEN) depth++;
                else if (peek() == TokenType.BRACE_CLOSE) depth--;
                pos++;
            }
        }
        boolean derived = isIdentifierToken(peek()) && peek(1) == TokenType.PAREN_OPEN;
        pos = saved;
        return derived;
    }

    /**
     * Parse a derived (computed) property:
     * {@code <<stereos>> {tags} name(params) { body }: Type[mult];}.
     * The body between {@code &lcub;...&rcub;} is parsed eagerly via
     * {@link SpecParser#parseCodeBlock} and stored as a
     * {@code List<ValueSpecification>} on the resulting
     * {@link DerivedPropertyDefinition}.
     *
     * <p>NOTE: stereotypes and tagged values on derived properties are
     * parsed-and-discarded for engine parity (engine drops them too).
     * If we want to preserve them later, this is the place.
     */
    private DerivedPropertyDefinition parseDerivedProperty() {
        int declStart = pos;
        Documentation doc = parseDocumentation();
        // CAPTURED, not dropped: the wire carries qualified-property annotations — the old
        // "engine consumes and drops" comment was engine-lite lore, refuted by the harness
        // (DIFF on ClassWithQualifiedProperties: stereotypes size expected=2 actual=0).
        List<com.legend.protocol.Protocol.PStereotype> stereotypes = parseStereotypes();
        List<com.legend.protocol.Protocol.PTaggedValue> taggedValues =
                taggedValuesWithDocumentation(doc, parseTaggedValues());
        String name = parseIdentifier();

        expect(TokenType.PAREN_OPEN);
        List<ParameterDefinition> params = new ArrayList<>();
        if (peek() != TokenType.PAREN_CLOSE) {
            params.add(parseDerivedPropertyParameter());
            while (match(TokenType.COMMA)) {
                params.add(parseDerivedPropertyParameter());
            }
        }
        expect(TokenType.PAREN_CLOSE);

        List<ValueSpecification> body = List.of();
        if (peek() == TokenType.BRACE_OPEN) {
            advance(); // consume {
            int bodyStart = pos;
            int depth = 1;
            while (!atEnd() && depth > 0) {
                TokenType t = peek();
                if (t == TokenType.BRACE_OPEN) depth++;
                else if (t == TokenType.BRACE_CLOSE) depth--;
                if (depth > 0) advance();
            }
            body = SpecParser.parseCodeBlock(tokens.slice(bodyStart, pos), dialect);
            expect(TokenType.BRACE_CLOSE);
        }

        expect(TokenType.COLON);
        TypeExpression type = parseType();
        Multiplicity mult = parseMultiplicity();
        expect(TokenType.SEMI_COLON);

        // Door 4: a bare function-reference body binds the derived property to a
        // user function; any other expression is the sugar (inline) form.
        // Engine convention: the span covers the whole declaration incl. the semicolon-
        // terminated tail (name..';' end column is the ';' - 1? — the probe shows the span
        // running to the declaration's last token; the harness arbitrates).
        return new DerivedPropertyDefinition(
                name, params, realizationOf(body), type, mult,
                stereotypes, taggedValues, spanOf(declStart, pos - 1));
    }

    /**
     * Classify a parsed body as a {@link Realization}: a single bare element
     * reference ({@link PackageableElementPtr}) is a function ref (Door 1/4);
     * anything else is an inline expression body (sugar / Door 3). Shared by
     * mapping bindings and the class/service hats.
     */
    static Realization realizationOf(List<ValueSpecification> body) {
        if (body.size() == 1 && body.get(0) instanceof PackageableElementPtr ptr) {
            return new Realization.Ref(ptr.fullPath(), ptr);
        }
        return new Realization.Inline(body);
    }

    private ParameterDefinition parseDerivedPropertyParameter() {
        int pStart = pos;
        String name = parseIdentifier();
        expect(TokenType.COLON);
        TypeExpression type = parseType();
        Multiplicity mult = parseMultiplicity();
        // Engine convention: a parameter's span covers its whole `name: Type[mult]` decl.
        return new ParameterDefinition(name, type, mult, spanOf(pStart, pos - 1));
    }

    // ============================================================
    // Constraint declarations (class-level)
    // ============================================================

    /**
     * {@code [ constraint (, constraint)* ]} after the class header. Each
     * constraint may be {@code name: expression} or just {@code expression}
     * (named by POSITION INDEX for parity with real m3). The expression is
     * parsed eagerly via {@link SpecParser#parse(com.legend.lexer.TokenStream)}
     * into a single {@link ValueSpecification}.
     */
    private List<ConstraintDefinition> parseConstraints() {
        expect(TokenType.BRACKET_OPEN);
        List<ConstraintDefinition> result = new ArrayList<>();
        if (peek() == TokenType.BRACKET_CLOSE) {
            // engine's constraints block requires >= 1 constraint —
            // `Class C [ ] {...}` refuses there (Tier-3 residue R4,
            // oracle-verified)
            throw error("Unexpected token ']'");
        }
        result.add(parseConstraint(0));
        while (match(TokenType.COMMA)) {
            result.add(parseConstraint(result.size()));
        }
        expect(TokenType.BRACKET_CLOSE);
        return result;
    }

    private ConstraintDefinition parseConstraint(int index) {
        int constraintStart = pos;
        // real m3: an unnamed constraint is named by its POSITION ("0",
        // "1", ...) — the id the checked goldens serialize, and distinct
        // lifted-FQN identity for multiple unnamed constraints
        String name = String.valueOf(index);
        // EXTENDED form (real m3): name( ~function: expr ~enforcementLevel: X
        // ~message: expr ) — the predicate is the ~function expression;
        // enforcement level and message are instantiation-time concerns,
        // parsed and dropped (engine parity for query compilation).
        // Dispatch on `name( ~` — a bare function-call constraint like
        // [ eq($this.a, 1) ] is the SIMPLE form (the real grammar allows any
        // expression there); only `name(~...)` opens the extended clause block.
        if (isIdentifierToken(peek()) && peek(1) == TokenType.PAREN_OPEN
                && peek(2) == TokenType.TILDE) {
            name = parseIdentifier();
            expect(TokenType.PAREN_OPEN);
            expect(TokenType.TILDE);
            String kw = parseIdentifier();
            // real clause order: ~owner? ~externalId? ~function
            // ~enforcementLevel? ~message? — externalId is RECORDED (the wire carries it;
            // dropping it was DIFF #1 the harness ever caught on constraints); owner is
            // recorded as present-only until its wire spelling is probed.
            // ORDERED, ONCE-ONLY prelude — the engine's complexConstraint
            // fixes ~owner? ~externalId? ~function ~enforcementLevel?
            // ~message? (Tier-3 residue R2/R3: the old any-order loop
            // accepted repeats and SWALLOWED malformed values)
            String externalId = null;
            String owner = null;
            if (kw.equals("owner")) {
                expect(TokenType.COLON);
                owner = parseIdentifier();
                expect(TokenType.TILDE);
                kw = parseIdentifier();
            }
            if (kw.equals("externalId")) {
                expect(TokenType.COLON);
                externalId = consumeStringLiteral("'externalId'");
                expect(TokenType.TILDE);
                kw = parseIdentifier();
            }
            if (!kw.equals("function")) {
                throw error("extended constraint must lead with ~function:, got ~" + kw);
            }
            expect(TokenType.COLON);
            int fnStart = pos;
            int d = 0;
            while (!atEnd()) {
                TokenType t = peek();
                if (t == TokenType.TILDE && d == 0) {
                    break;
                }
                if (t == TokenType.BRACKET_OPEN || t == TokenType.PAREN_OPEN
                        || t == TokenType.BRACE_OPEN) {
                    d++;
                } else if (t == TokenType.BRACKET_CLOSE || t == TokenType.PAREN_CLOSE
                        || t == TokenType.BRACE_CLOSE) {
                    if (d == 0) {
                        break;
                    }
                    d--;
                }
                advance();
            }
            ValueSpecification fn = SpecParser.parse(tokens.slice(fnStart, pos), dialect);
            // trailing ~key: value sections — ~message (an EXPRESSION over
            // $this) and ~enforcementLevel feed the validation projection
            // (#45); others parse and drop (engine: instantiation concerns)
            ValueSpecification message = null;
            String level = null;
            boolean sawLevel = false;
            boolean sawMessage = false;
            while (!atEnd() && peek() == TokenType.TILDE) {
                expect(TokenType.TILDE);
                String kw2 = parseIdentifier();
                // CLOSED, ORDERED tail — ~enforcementLevel? ~message? only
                // (Tier-3 residue R2: the old loop accepted ANY ~key here
                // and silently dropped it, so ~externalId AFTER ~function
                // parsed where the engine refuses)
                if (kw2.equals("enforcementLevel")
                        ? (sawLevel || sawMessage) : !kw2.equals("message")) {
                    throw error("unexpected constraint clause '~" + kw2
                            + "' — the order is ~owner? ~externalId?"
                            + " ~function ~enforcementLevel? ~message?");
                }
                if (kw2.equals("message") && sawMessage) {
                    throw error("constraint clause '~message' may appear"
                            + " only once");
                }
                expect(TokenType.COLON);
                int vStart = pos;
                int vd = 0;
                while (!atEnd()) {
                    TokenType t = peek();
                    if (t == TokenType.TILDE && vd == 0) {
                        break;
                    }
                    if (t == TokenType.BRACKET_OPEN || t == TokenType.PAREN_OPEN
                            || t == TokenType.BRACE_OPEN) {
                        vd++;
                    } else if (t == TokenType.BRACKET_CLOSE
                            || t == TokenType.PAREN_CLOSE
                            || t == TokenType.BRACE_CLOSE) {
                        if (vd == 0) {
                            break;
                        }
                        vd--;
                    }
                    advance();
                }
                if (kw2.equals("message")) {
                    sawMessage = true;
                    message = SpecParser.parse(tokens.slice(vStart, pos), dialect);
                } else if (kw2.equals("enforcementLevel")) {
                    sawLevel = true;
                    ValueSpecification lv =
                            SpecParser.parse(tokens.slice(vStart, pos), dialect);
                    level = enforcementLevelName(lv);
                    if (!"Error".equals(level) && !"Warn".equals(level)) {
                        // BOTH reference grammars admit only Error|Warn
                        // (DomainParserGrammar constraintEnforcementLevelType;
                        // legend-pure M3 agrees — refusal-asymmetry
                        // INVESTIGATE row AbstractTestConstraints#110)
                        throw error("constraint enforcementLevel must be"
                                + " Error or Warn, got '" + level + "'");
                    }
                }
            }
            expect(TokenType.PAREN_CLOSE);
            // Engine convention: the span covers the whole `name ( ... )` block.
            return new ConstraintDefinition(name, realizationOf(List.of(fn)),
                    message, level, externalId, owner, spanOf(constraintStart, pos - 1));
        }
        if (isIdentifierToken(peek()) && peek(1) == TokenType.COLON) {
            name = parseIdentifier();
            advance(); // consume :
        }

        // Scan until top-level ',' or matching ']' &mdash; balance brackets,
        // parens, and braces so embedded expressions don't fool us.
        int bodyStart = pos;
        int depth = 0;
        while (!atEnd()) {
            TokenType t = peek();
            if (t == TokenType.BRACKET_OPEN || t == TokenType.PAREN_OPEN || t == TokenType.BRACE_OPEN) {
                depth++;
            } else if (t == TokenType.BRACKET_CLOSE || t == TokenType.PAREN_CLOSE || t == TokenType.BRACE_CLOSE) {
                if (depth == 0) break;
                depth--;
            } else if (t == TokenType.COMMA && depth == 0) {
                break;
            }
            advance();
        }
        if (pos == bodyStart) {
            throw error("empty constraint expression for '" + name + "'");
        }
        ValueSpecification expression = SpecParser.parse(tokens.slice(bodyStart, pos), dialect);
        // Door 4: `[name: some::fn]` binds the constraint to a predicate
        // function; any other expression is the sugar (inline) predicate.
        // Engine convention: the span covers `name: expr`, name inclusive.
        return new ConstraintDefinition(name, realizationOf(List.of(expression)),
                null, null, null, null, spanOf(constraintStart, pos - 1));
    }

    /** The bare level name of a parsed ~enforcementLevel value —
     * {@code Error} / {@code Warn} spellings arrive as refs or enum-style
     * accesses; the projection wants the simple name. */
    private static @com.legend.Nullable String enforcementLevelName(ValueSpecification lv) {
        if (lv instanceof com.legend.protocol.spec.PackageableElementPtr p) {
            String f = p.fullPath();
            return f.contains("::") ? f.substring(f.lastIndexOf("::") + 2) : f;
        }
        if (lv instanceof com.legend.protocol.spec.CString cs) {
            return cs.value();
        }
        if (lv instanceof com.legend.protocol.spec.AppliedProperty ap) {
            return ap.property();
        }
        if (lv instanceof com.legend.protocol.spec.Variable v) {
            return v.name();
        }
        return null;
    }

    /** {@code Primitive fqn extends Base} with an optional dropped constraint block. */
    /** STRAIGHT-TO-MODEL — not yet migrated; see docs/PROTOCOL_MIGRATION_CENSUS.md. */
    private PackageableElement primitiveElement() {
        advance();   // 'Primitive'
        String fqn = parseQualifiedName();
        List<com.legend.protocol.ParameterDefinition> typeVariables = parseTypeVariableDeclarations();
        expect(TokenType.EXTENDS);
        String base = parseQualifiedName();
        // optional (args) on the base (e.g. Decimal(10,2)) — dropped
        if (peek() == TokenType.PAREN_OPEN) {
            skipBalancedBlock();
        }
        // optional [constraints] — instantiation-time; dropped
        if (peek() == TokenType.BRACKET_OPEN) {
            skipBalancedBlock();
        }
        return new com.legend.model.PrimitiveExtensionDefinition(fqn, base, typeVariables);
    }

    // ============================================================
    // Association
    // ============================================================

    /** {@code Association <<stereos>> {tags} qualifiedName { end1; end2; }} */
    /** PROTOCOL-FIRST. */
    private PackageableElement associationElement() {
        int declStart = pos;
        Documentation doc = parseDocumentation();
        expect(TokenType.ASSOCIATION);
        // CAPTURED, not dropped: the wire carries association annotations
        // (ProbeWireShapes "association").
        List<com.legend.protocol.Protocol.PStereotype> stereotypes = parseStereotypes();
        List<com.legend.protocol.Protocol.PTaggedValue> taggedValues =
                taggedValuesWithDocumentation(doc, parseTaggedValues());
        String qualifiedName = parseQualifiedName();
        // PROJECTION association: `Association X projects Y<A, B>` — a
        // nominal registration only (like projection classes); the
        // projected navigation semantics stay loud downstream.
        if (peek() == TokenType.VALID_STRING && "projects".equals(safeText())) {
            if (dialect.refusesPlatformDialect()) {
                // align with parseAssociationDefinition (the protocol path
                // already refused; this model path accepted and returned
                // the WRONG element kind — adversarial audit EP finding 3)
                throw error("association projections are not supported by the engine");
            }
            advance();
            parseQualifiedName();
            if (peek() == TokenType.LESS_THAN) {
                // <A, B> type arguments — consume to the matching '>'
                advance();
                int depth = 1;
                while (!atEnd() && depth > 0) {
                    if (peek() == TokenType.LESS_THAN) depth++;
                    if (peek() == TokenType.GREATER_THAN) depth--;
                    advance();
                }
            }
            return new ClassDefinition(qualifiedName, List.of(), List.of(),
                    List.of(), List.of(), List.of(), List.of(), List.of(),
                    false);
        }
        return com.legend.model.FromProtocol.toAssociationDefinition(
                parseAssociationBody(declStart, stereotypes, taggedValues, qualifiedName));
    }

    /** Parses one non-projection {@code Association} at the cursor into its protocol
     *  record — the per-element protocol entry point (see {@link #at}). */
    public com.legend.protocol.Protocol.PAssociation parseAssociationDefinition() {
        int declStart = pos;
        Documentation doc = parseDocumentation();
        expect(TokenType.ASSOCIATION);
        List<com.legend.protocol.Protocol.PStereotype> stereotypes = parseStereotypes();
        List<com.legend.protocol.Protocol.PTaggedValue> taggedValues =
                taggedValuesWithDocumentation(doc, parseTaggedValues());
        String qualifiedName = parseQualifiedName();
        if (peek() == TokenType.VALID_STRING && "projects".equals(safeText())) {
            throw error("projection associations are a legend-lite-local form with no"
                    + " protocol shape (engine rejects 'projects')");
        }
        return parseAssociationBody(declStart, stereotypes, taggedValues, qualifiedName);
    }

    private com.legend.protocol.Protocol.PAssociation parseAssociationBody(
            int declStart,
            List<com.legend.protocol.Protocol.PStereotype> stereotypes,
            List<com.legend.protocol.Protocol.PTaggedValue> taggedValues,
            String qualifiedName) {
        expect(TokenType.BRACE_OPEN);

        List<com.legend.protocol.Protocol.PProperty> ends = new ArrayList<>();
        List<DerivedPropertyDefinition> derived = new ArrayList<>();
        while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
            // real pure allows QUALIFIED properties in associations — they
            // are alternate accessors of one end, owned by the OPPOSITE
            // end's class (adopted there during normalization)
            if (isDerivedPropertyStart()) {
                derived.add(parseDerivedProperty());
                continue;
            }
            // an end is an ordinary wire property (annotations, span and all)
            ends.add(parseProperty());
        }
        expect(TokenType.BRACE_CLOSE);

        // arity is a COMPILE error, not a parse error — the engine parses 1- or 3-end
        // associations and serializes whatever it read (inline-snippet corpus)
        String[] pn = com.legend.protocol.Protocol.splitFqn(qualifiedName);
        return new com.legend.protocol.Protocol.PAssociation(pn[0], pn[1], ends, derived,
                stereotypes, taggedValues, spanOf(declStart, pos - 1));
    }

    // ============================================================
    // Enum
    // ============================================================

    /** {@code Enum <<stereos>> {tags} qualifiedName { VAL (, VAL)* }} */
    /** Parses one {@code Enum} declaration at the cursor into its protocol record —
     *  the per-element protocol entry point (see {@link #at}). Annotations are CAPTURED:
     *  the wire carries declaration- and value-level stereotypes/taggedValues. */
    public com.legend.protocol.Protocol.PEnumeration parseEnumDefinition() {
        int declStart = pos;
        Documentation doc = parseDocumentation();
        expect(TokenType.ENUM);
        List<com.legend.protocol.Protocol.PStereotype> stereotypes = parseStereotypes();
        List<com.legend.protocol.Protocol.PTaggedValue> taggedValues =
                taggedValuesWithDocumentation(doc, parseTaggedValues());
        String qualifiedName = parseQualifiedName();
        expect(TokenType.BRACE_OPEN);

        List<com.legend.protocol.Protocol.PEnumValue> values = new ArrayList<>();
        if (peek() != TokenType.BRACE_CLOSE) {
            values.add(parseEnumValue());
            while (match(TokenType.COMMA)) {
                values.add(parseEnumValue());
            }
        }
        expect(TokenType.BRACE_CLOSE);

        // an EMPTY enum parses (values: [] on the wire) — rejection, if any, is the
        // compiler's job (inline-snippet corpus)
        String[] pn = com.legend.protocol.Protocol.splitFqn(qualifiedName);
        return new com.legend.protocol.Protocol.PEnumeration(pn[0], pn[1], values,
                stereotypes, taggedValues, spanOf(declStart, pos - 1));
    }

    private com.legend.protocol.Protocol.PEnumValue parseEnumValue() {
        int entryStart = pos;
        Documentation doc = parseDocumentation();
        List<com.legend.protocol.Protocol.PStereotype> ss = parseStereotypes();
        List<com.legend.protocol.Protocol.PTaggedValue> ts =
                taggedValuesWithDocumentation(doc, parseTaggedValues());
        String value = parseIdentifier();
        // Engine convention: the entry span runs annotations..value name, comma excluded.
        return new com.legend.protocol.Protocol.PEnumValue(value, ss, ts,
                spanOf(entryStart, pos - 1));
    }

    // ============================================================
    // Profile
    // ============================================================

    /**
     * {@code Profile qualifiedName { (stereotypes: [...]; | tags: [...];)* }}
     */
    /** Parses one {@code Profile} declaration at the cursor into its protocol record —
     *  the per-element protocol entry point (see {@link #at}). Entry spans cover the
     *  declared name token only (engine convention, ProbeWireShapes "profile"). */
    public com.legend.protocol.Protocol.PProfile parseProfileDefinition() {
        int declStart = pos;
        // pure's grammar admits documentation on a Profile (`profile:
        // documentation? PROFILE ...`); the engine's does not — and the
        // protocol Profile carries no applied-annotation field, so the
        // platform lanes read it and DROP it like the profile's own
        // stereotypes below; the exact-engine surface refuses at the keyword
        Documentation doc = parseDocumentation();
        if (doc != null && dialect.refusesPlatformDialect()) {
            throw error("Unexpected token '" + safeText() + "'");
        }
        expect(TokenType.PROFILE);
        // Stereotypes/tags ON the profile itself (m4-pure syntax —
        // upstream pctQualifiers.pure: Profile <<PCT.testQualifierProfile>>
        // ...). The ENGINE GRAMMAR has no such slot (DomainParserGrammar
        // profile: PROFILE qualifiedName BRACE_OPEN — verified 2026-08-23),
        // so the exact-engine surface refuses verbatim; platform-source
        // lanes parse them and DROP them faithfully — the engine protocol
        // Profile carries no applied-annotation field, so the engine's own
        // PMCD transform drops them too.
        List<com.legend.protocol.Protocol.PStereotype> selfStereotypes =
                parseStereotypes();
        List<com.legend.protocol.Protocol.PTaggedValue> selfTags =
                parseTaggedValues();
        if (dialect.refusesLiteExtensions()
                && (!selfStereotypes.isEmpty() || !selfTags.isEmpty())) {
            throw error("Profile stereotypes/tagged values are not"
                    + " authorized in Legend Engine");
        }
        String qualifiedName = parseQualifiedName();
        expect(TokenType.BRACE_OPEN);

        List<com.legend.protocol.Protocol.PProfileEntry> stereotypes = new ArrayList<>();
        List<com.legend.protocol.Protocol.PProfileEntry> tags = new ArrayList<>();

        java.util.Set<String> seenKeys = new java.util.HashSet<>();
        while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
            List<com.legend.protocol.Protocol.PProfileEntry> target;
            if (peek() == TokenType.STEREOTYPES) {
                TokenStreamCursor.once(seenKeys, "stereotypes", this);
                target = stereotypes;
            } else if (peek() == TokenType.TAGS) {
                TokenStreamCursor.once(seenKeys, "tags", this);
                target = tags;
            } else {
                throw error("expected 'stereotypes' or 'tags' inside Profile, found " + peek()
                        + " ('" + safeText() + "')");
            }
            advance();
            expect(TokenType.COLON);
            expect(TokenType.BRACKET_OPEN);
            if (peek() != TokenType.BRACKET_CLOSE) {
                target.add(parseProfileEntry());
                while (match(TokenType.COMMA)) {
                    target.add(parseProfileEntry());
                }
            }
            expect(TokenType.BRACKET_CLOSE);
            expect(TokenType.SEMI_COLON);
        }

        expect(TokenType.BRACE_CLOSE);
        String[] pn = com.legend.protocol.Protocol.splitFqn(qualifiedName);
        return new com.legend.protocol.Protocol.PProfile(pn[0], pn[1], stereotypes, tags,
                spanOf(declStart, pos - 1));
    }

    private com.legend.protocol.Protocol.PProfileEntry parseProfileEntry() {
        int nameTok = pos;
        String value = parseIdentifier();
        return new com.legend.protocol.Protocol.PProfileEntry(value, spanOf(nameTok, pos - 1));
    }

    // ============================================================
    // Function declaration
    // ============================================================

    /**
     * {@code function <<stereos>> {tags} qualifiedName(params) : returnType[mult]
     * [optional-constraints] { body }}.
     * Body parsed eagerly via {@link SpecParser#parseCodeBlock} into a
     * {@code List<ValueSpecification>} (sequence of statements).
     */
    /**
     * The signature grammar shared by concrete and native functions (audit
     * M2 found it duplicated verbatim): stereotypes, tags, FQN,
     * {@code <T,V|m,n>}, parameters, {@code :R[m]}.
     */
    private record FunctionSignature(
            int declStart,
            String qualifiedName,
            List<String> typeParams,
            List<String> multParams,
            List<com.legend.protocol.ParameterDefinition> params,
            TypeExpression returnType,
            Multiplicity returnMult,
            List<com.legend.protocol.Protocol.PStereotype> stereotypes,
            List<com.legend.protocol.Protocol.PTaggedValue> taggedValues) {
    }

    private FunctionSignature parseFunctionSignature() {
        int declStart = pos;
        return parseFunctionSignature(declStart, parseDocumentation());
    }

    /** {@code documentation? FUNCTION stereotypes? taggedValues? ...} — the
     *  documentation, when the caller read it (before {@code native}), is
     *  folded in first. */
    private FunctionSignature parseFunctionSignature(int declStart, @com.legend.Nullable Documentation doc) {
        expect(TokenType.FUNCTION);
        List<com.legend.protocol.Protocol.PStereotype> stereotypes = parseStereotypes();
        List<com.legend.protocol.Protocol.PTaggedValue> taggedValues =
                taggedValuesWithDocumentation(doc, parseTaggedValues());
        String qualifiedName = parseQualifiedName();
        List<String> typeParams = new ArrayList<>();
        List<String> multParams = new ArrayList<>();
        parseTypeAndMultiplicityParameters(typeParams, multParams);
        if (dialect.refusesLiteExtensions() && (!typeParams.isEmpty() || !multParams.isEmpty())) {
            // DECLARED extension LITE-DESIGN-function-types-generics
            // (OWN_CORPUS_DECISIONS §11)
            throw error("Type and/or multiplicity parameters are not"
                    + " authorized in Legend Engine");
        }
        expect(TokenType.PAREN_OPEN);
        List<com.legend.protocol.ParameterDefinition> params = new ArrayList<>();
        if (peek() != TokenType.PAREN_CLOSE) {
            params.add(parseFunctionParameter());
            while (match(TokenType.COMMA)) {
                params.add(parseFunctionParameter());
            }
        }
        expect(TokenType.PAREN_CLOSE);
        expect(TokenType.COLON);
        TypeExpression returnType = parseType();
        Multiplicity returnMult = parseMultiplicity();
        return new FunctionSignature(declStart, qualifiedName, List.copyOf(typeParams),
                List.copyOf(multParams), params, returnType, returnMult,
                stereotypes, taggedValues);
    }

    /** PROTOCOL-FIRST. */
    private FunctionDefinition functionElement() {
        return com.legend.model.FromProtocol.toFunctionDefinition(parseFunctionProtocol());
    }
    /** Parses one {@code Measure} declaration at the cursor into its protocol record
     *  (probe: vanilla engine Measure JSON). Engine grammar (DomainParserGrammar.g4):
     *  {@code (measureExpr* canonicalExpr measureExpr*) | nonConvertibleMeasureExpr+} —
     *  either every unit has a conversion and exactly one carries {@code *}, or every
     *  unit is a bare name and the FIRST is promoted to canonical
     *  (DomainParseTreeWalker: {@code canonicalUnit = nonConvertibleUnits.get(0)}). */
    public com.legend.protocol.Protocol.PMeasure parseMeasureDefinition() {
        int declStart = pos;
        // pure's grammar admits documentation on a Measure; the engine's does
        // not, and the protocol Measure carries no tagged values — read and
        // dropped on the platform lanes (as a Profile's), refused by the
        // exact-engine surface at the keyword
        Documentation doc = parseDocumentation();
        if (doc != null && dialect.refusesPlatformDialect()) {
            throw error("Unexpected token '" + safeText() + "'");
        }
        advance();                                  // 'Measure'
        String qualifiedName = parseQualifiedName();
        String measureFqn = com.legend.protocol.Protocol.unquotePath(qualifiedName);
        expect(TokenType.BRACE_OPEN);
        List<com.legend.protocol.Protocol.PUnit> units = new ArrayList<>();
        int starredIdx = -1;
        Boolean convertible = null;                 // form locked by the first unit
        while (!atEnd() && peek() != TokenType.BRACE_CLOSE) {
            boolean starred = false;
            if (peek() == TokenType.STAR) {
                starred = true;
                advance();
            }
            int unitStart = pos;
            String unitName = parseIdentifier();
            boolean hasConversion = starred || peek() == TokenType.COLON;
            if (convertible == null) {
                convertible = hasConversion;
            } else if (convertible != hasConversion) {
                throw error("measure units cannot mix conversion functions with"
                        + " bare (non-convertible) units");
            }
            String param = null;
            com.legend.protocol.spec.ValueSpecification body = null;
            if (hasConversion) {
                expect(TokenType.COLON);
                param = parseIdentifier();
                expect(TokenType.ARROW);
                int bodyStart = pos;
                int depth = 0;
                while (!atEnd()) {
                    TokenType t = peek();
                    if (depth == 0 && t == TokenType.SEMI_COLON) {
                        break;
                    }
                    if (t == TokenType.PAREN_OPEN || t == TokenType.BRACKET_OPEN
                            || t == TokenType.BRACE_OPEN) {
                        depth++;
                    } else if (t == TokenType.PAREN_CLOSE || t == TokenType.BRACKET_CLOSE
                            || t == TokenType.BRACE_CLOSE) {
                        depth--;
                    }
                    advance();
                }
                body = SpecParser.parse(tokens.slice(bodyStart, pos), dialect);
            }
            expect(TokenType.SEMI_COLON);
            if (starred) {
                if (starredIdx >= 0) {
                    throw error("measure declares more than one canonical ('*') unit");
                }
                starredIdx = units.size();
            }
            units.add(new com.legend.protocol.Protocol.PUnit(unitName, measureFqn, param,
                    body, spanOf(unitStart, pos - 1)));
        }
        if (units.isEmpty()) {
            throw error("measure requires at least one unit");
        }
        if (Boolean.TRUE.equals(convertible) && starredIdx < 0) {
            throw error("measure with conversion functions requires a canonical"
                    + " ('*') unit");
        }
        expect(TokenType.BRACE_CLOSE);
        int canonicalIdx = Math.max(starredIdx, 0);
        com.legend.protocol.Protocol.PUnit canonical = units.get(canonicalIdx);
        List<com.legend.protocol.Protocol.PUnit> others = new ArrayList<>(units);
        others.remove(canonicalIdx);
        String[] pn = com.legend.protocol.Protocol.splitFqn(qualifiedName);
        return new com.legend.protocol.Protocol.PMeasure(pn[0], pn[1], canonical, others,
                spanOf(declStart, pos - 1));
    }

    /** Parses one {@code function} declaration at the cursor into its protocol record —
     *  the per-element protocol entry point (see {@link #at}). Constraint blocks are
     *  CAPTURED (the wire carries pre/postConstraints; the emitter walls until probed). */
    public com.legend.protocol.Protocol.PFunction parseFunctionProtocol() {
        FunctionSignature sig = parseFunctionSignature();

        List<ConstraintDefinition> constraints = peek() == TokenType.BRACKET_OPEN
                ? parseConstraints()
                : List.of();

        expect(TokenType.BRACE_OPEN);
        int bodyStart = pos;
        int depth = 1;
        while (!atEnd() && depth > 0) {
            TokenType t = peek();
            if (t == TokenType.BRACE_OPEN) depth++;
            else if (t == TokenType.BRACE_CLOSE) depth--;
            if (depth > 0) advance();
        }
        List<ValueSpecification> body = SpecParser.parseCodeBlock(tokens.slice(bodyStart, pos), dialect);
        if (body.isEmpty() && dialect.refusesPlatformDialect()) {
            // engine codeBlock requires >= 1 programLine — `{ }` refuses
            // there (adversarial audit R5, oracle-verified)
            throw error("Unexpected token '}'");
        }
        expect(TokenType.BRACE_CLOSE);

        // Optional TEST-SUITE block: `function f(...) { body } { suite... }` (legend-testable).
        // Parsed into PTestSuite records; the wire's function span covers the block.
        List<com.legend.protocol.Protocol.PTestSuite> suites = List.of();
        if (peek() == TokenType.BRACE_OPEN) {
            suites = parseFunctionTestBlock(sig);
        }

        String[] pn = com.legend.protocol.Protocol.splitFqn(sig.qualifiedName());
        return new com.legend.protocol.Protocol.PFunction(pn[0], pn[1],
                sig.typeParams(), sig.multParams(), sig.params(),
                sig.returnType(), sig.returnMult(), body, constraints, suites,
                sig.stereotypes(), sig.taggedValues(),
                spanOf(sig.declStart(), pos - 1));
    }

    /**
     * The legend-testable trailing block. Two forms (ProbeWireShapes "fn tests wire",
     * "fn tests named suite"): named suites {@code { name ( test; ... ) ... }} and the
     * unnamed brace form {@code { test; ... }} (wire id {@code "default"}, span = the
     * whole block). Mixing both in one block has no probed wire order — refuse loudly.
     */
    private List<com.legend.protocol.Protocol.PTestSuite> parseFunctionTestBlock(
            FunctionSignature sig) {
        int blockOpen = pos;
        expect(TokenType.BRACE_OPEN);
        List<com.legend.protocol.Protocol.PTestSuite> suites = new ArrayList<>();
        List<com.legend.protocol.Protocol.PFunctionTest> unnamed = new ArrayList<>();
        List<com.legend.protocol.Protocol.PTestData> unnamedData = new ArrayList<>();
        while (!atEnd() && peek() != TokenType.BRACE_CLOSE) {
            if (peek(1) == TokenType.PAREN_OPEN) {
                int suiteStart = pos;
                String suiteId = parseIdentifier();
                expect(TokenType.PAREN_OPEN);
                List<com.legend.protocol.Protocol.PTestData> data = new ArrayList<>();
                List<com.legend.protocol.Protocol.PFunctionTest> tests = new ArrayList<>();
                while (!atEnd() && peek() != TokenType.PAREN_CLOSE) {
                    parseSuiteEntry(sig, data, tests);
                }
                expect(TokenType.PAREN_CLOSE);
                suites.add(new com.legend.protocol.Protocol.PTestSuite(
                        suiteId, spanOf(suiteStart, pos - 1), data, tests));
            } else {
                parseSuiteEntry(sig, unnamedData, unnamed);
            }
        }
        expect(TokenType.BRACE_CLOSE);
        if (!unnamed.isEmpty() || !unnamedData.isEmpty()) {
            // the DEFAULT (bare) suite serializes FIRST, then named suites in source
            // order (probe "pf mixed suites xml")
            suites.add(0, new com.legend.protocol.Protocol.PTestSuite(
                    null, spanOf(blockOpen, pos - 1), unnamedData, unnamed));
        }
        return suites;
    }

    /** One suite entry: {@code store: <payload>;} (test DATA, spotted by the ':' after
     *  the leading name) or {@code id | call(args) => expected;} (a test). */
    private void parseSuiteEntry(FunctionSignature sig,
            List<com.legend.protocol.Protocol.PTestData> data,
            List<com.legend.protocol.Protocol.PFunctionTest> tests) {
        int entryStart = pos;
        String pointerType = null;
        if (peek() == TokenType.PAREN_OPEN) {
            // (dataspace) my::DS: DataspaceTestData #{...}# — the marker
            // types the pointer (ZTailProbe "dataspace-testref"); the
            // pointer span still starts at the '('
            advance();
            pointerType = parseIdentifier()
                    .toUpperCase(java.util.Locale.ROOT);
            expect(TokenType.PAREN_CLOSE);
        }
        String head = parseQualifiedName();
        int headEnd = pos - 1;
        if (peek() == TokenType.COLON) {
            advance();
            com.legend.protocol.Protocol.PTestPayload payload = parseTestPayload();
            expect(TokenType.SEMI_COLON);
            // a relationAccessor's WRAPPER span runs through the entry semicolon
            // (probe "pf relation island data")
            if (payload instanceof com.legend.protocol.Protocol.PTestPayload
                    .RelationElements re) {
                // the accessor wrapper's end lands TWO past the entry ';' — the same
                // walker length quirk as single-line assertion blocks (corpus #58 +
                // probe "pf relation island data": ';' col 9, wire col 11)
                payload = new com.legend.protocol.Protocol.PTestPayload.RelationElements(
                        re.elements(), new com.legend.protocol.SourceInfo("",
                                re.sourceInformation().startLine(),
                                re.sourceInformation().startColumn(),
                                tokens.endLine(pos - 1), tokens.endColumn(pos - 1) + 2));
            }
            data.add(new com.legend.protocol.Protocol.PTestData(head,
                    spanOf(entryStart, headEnd), payload, pointerType,
                    spanOf(entryStart, pos - 1)));
            return;
        }
        if (pointerType != null) {
            throw error("a (" + pointerType.toLowerCase(java.util.Locale.ROOT)
                    + ") marker introduces test DATA, not a test");
        }
        tests.add(parseFunctionTest(sig, entryStart, head));
    }

    /** {@code (JSON) '...'} | {@code some::Reference} | {@code Relation #{...}#}
     *  (probes "pf test store data", "pf reference test data", "pf relation island
     *  data"). */
    private com.legend.protocol.Protocol.PTestPayload parseTestPayload() {
        if (peek() == TokenType.PAREN_OPEN) {
            int fmtStart = pos;
            advance();
            String fmt = parseIdentifier();
            expect(TokenType.PAREN_CLOSE);
            if (peek() != TokenType.STRING) {
                throw error("expected a quoted payload after (" + fmt + ")");
            }
            String raw = text();
            advance();
            return new com.legend.protocol.Protocol.PTestPayload.ExternalFormat(
                    formatContentType(fmt),
                    TokenStreamCursor.unquoteAndUnescape(raw, this),
                    spanOf(fmtStart, pos - 1));
        }
        if ("DataspaceTestData".equals(peekText(0))
                && peek(1) == TokenType.ISLAND_OPEN) {
            // DataspaceTestData #{ my::Ref }# — a DATASPACE-typed reference;
            // the dataElement span runs the KIND keyword through the island
            // close (ZTailProbe "dataspace-testref")
            int kindTok = pos;
            advance();                              // 'DataspaceTestData'
            advance();                              // ISLAND_OPEN
            int embStart = pos;
            while (peek() != TokenType.ISLAND_END && !atEnd()) {
                advance();
            }
            String refPath = reconstructText(embStart, pos).trim();
            expect(TokenType.ISLAND_END);
            return new com.legend.protocol.Protocol.PTestPayload.Reference(
                    com.legend.protocol.Protocol.unquotePath(refPath),
                    "DATASPACE", spanOf(kindTok, pos - 1));
        }
        if ("Relation".equals(peekText(0)) && peek(1) == TokenType.ISLAND_OPEN) {
            advance();                              // 'Relation'
            List<com.legend.protocol.Protocol.PTestPayload.RelationElement> els =
                    parseRelationIslandElements();
            com.legend.protocol.SourceInfo first = els.isEmpty()
                    ? spanOf(pos - 1, pos - 1) : els.get(0).sourceInformation();
            return new com.legend.protocol.Protocol.PTestPayload.RelationElements(
                    els, first);
        }
        if ("ModelStore".equals(peekText(0)) && peek(1) == TokenType.ISLAND_OPEN) {
            return parseModelStoreIsland();
        }
        if ("Relational".equals(peekText(0)) && peek(1) == TokenType.ISLAND_OPEN) {
            return parseRelationalCsvIsland();
        }
        int refStart = pos;
        String ref = parseQualifiedName();
        return new com.legend.protocol.Protocol.PTestPayload.Reference(
                ref, spanOf(refStart, pos - 1));
    }

    /** Format keyword &rarr; contentType (probe "pf mixed suites xml": XML rides the
     *  SAME equalToJson assertion, only the contentType changes). */
    private String formatContentType(String fmt) {
        return switch (fmt) {
            case "JSON" -> "application/json";
            case "XML" -> "application/xml";
            default -> throw error(
                    "test payload format (" + fmt + ") has no probed contentType");
        };
    }

    /**
     * {@code ModelStore #{ FQN: ExternalFormat #{ contentType: '...'; data: '...'; }# }#}
     * (probe "pf modelstore island"): modelStore spans keyword..outer {@code }#}; each
     * modelEmbeddedData spans its FQN..inner {@code }#}; the externalFormat spans
     * {@code ExternalFormat}..inner {@code }#}.
     */
    private com.legend.protocol.Protocol.PTestPayload.ModelStoreData parseModelStoreIsland() {
        int kw = pos;
        advance();                                  // 'ModelStore'
        int outerOpen = pos;
        expect(TokenType.ISLAND_OPEN);
        // island content arrives as coarse chunks — walk the token structure for the
        // NESTED islands, the chars for everything else
        List<com.legend.protocol.Protocol.PTestPayload.ModelEmbedded> models =
                new ArrayList<>();
        String source = tokens.source();
        while (!atEnd() && peek() != TokenType.ISLAND_END) {
            if (peek() == TokenType.ISLAND_OPEN) {
                throw error("expected 'FQN: ExternalFormat' before a nested data island");
            }
            // chunk text holds "FQN:\n ExternalFormat" (possibly with commas between
            // entries); the nested island follows
            String chunk = tokens.text(pos);
            int chunkStart = tokens.start(pos);
            int colon = singleColon(chunk);
            if (colon < 0) {
                throw error("expected 'FQN:' inside a ModelStore data island");
            }
            int a = 0;
            while (a < colon && (Character.isWhitespace(chunk.charAt(a))
                    || chunk.charAt(a) == ',')) {
                a++;
            }
            String model = chunk.substring(a, colon).trim();
            String rest = chunk.substring(colon + 1).trim();
            if (!rest.equals("ExternalFormat")) {
                throw error("expected ExternalFormat inside a ModelStore data island,"
                        + " got '" + rest + "'");
            }
            int efStart = chunkStart + chunk.indexOf("ExternalFormat", colon);
            advance();                              // past the chunk
            if (peek() != TokenType.ISLAND_OPEN && peek() != TokenType.ISLAND_START) {
                throw error("expected a nested data island after ExternalFormat");
            }
            advance();
            StringBuilder inner = new StringBuilder();
            int depth = 0;
            while (!atEnd()) {
                TokenType t = peek();
                if (t == TokenType.ISLAND_START) {
                    depth++;
                } else if (t == TokenType.ISLAND_END) {
                    if (depth == 0) {
                        break;          // THIS island's end, not a nested one
                    }
                    depth--;
                }
                inner.append(tokens.text(pos));
                advance();
            }
            int innerEndTok = pos;
            expect(TokenType.ISLAND_END);
            String[] kv = parseExternalFormatKv(inner.toString());
            models.add(new com.legend.protocol.Protocol.PTestPayload.ModelEmbedded(model,
                    new com.legend.protocol.Protocol.PTestPayload.ExternalFormat(
                            kv[0], kv[1], new com.legend.protocol.SourceInfo("",
                                    tokens.lineOf(efStart), tokens.columnOf(efStart),
                                    tokens.endLine(innerEndTok), tokens.endColumn(innerEndTok))),
                    new com.legend.protocol.SourceInfo("",
                            tokens.lineOf(chunkStart + a), tokens.columnOf(chunkStart + a),
                            tokens.endLine(innerEndTok), tokens.endColumn(innerEndTok))));
            // trailing whitespace chunk before the outer close is fine
            if (!atEnd() && peek() != TokenType.ISLAND_END
                    && tokens.text(pos).isBlank()) {
                advance();
            }
        }
        expect(TokenType.ISLAND_END);
        return new com.legend.protocol.Protocol.PTestPayload.ModelStoreData(models,
                spanOf(kw, pos - 1));
    }

    /** The first ':' that is NOT part of a '::' package separator, or -1. */
    private static int singleColon(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == ':'
                    && (i + 1 >= s.length() || s.charAt(i + 1) != ':')
                    && (i == 0 || s.charAt(i - 1) != ':')) {
                return i;
            }
        }
        return -1;
    }

    /** {@code contentType: '...'; data: '...';} — quote-aware. */
    private String[] parseExternalFormatKv(String body) {
        String contentType = null;
        String data = null;
        int i = 0;
        int n = body.length();
        while (i < n) {
            char c = body.charAt(i);
            if (Character.isWhitespace(c) || c == ';') {
                i++;
                continue;
            }
            int colon = body.indexOf(':', i);
            if (colon < 0) {
                break;
            }
            String key = body.substring(i, colon).trim();
            int q1 = body.indexOf('\'', colon);
            if (q1 < 0) {
                throw error("expected a quoted value for ExternalFormat key '" + key + "'");
            }
            // THE shared decoder (was the second private n/t/r-only escape
            // table — adversarial audit EP finding 13)
            int q2 = q1 + 1;
            while (q2 < n && body.charAt(q2) != '\'') {
                q2 += body.charAt(q2) == '\\' && q2 + 1 < n ? 2 : 1;
            }
            StringBuilder v = new StringBuilder(com.legend.protocol.Escapes
                    .unescapeJavaLike(body.substring(q1 + 1, q2)));
            if (key.equals("contentType")) {
                contentType = v.toString();
            } else if (key.equals("data")) {
                data = v.toString();
            } else {
                throw error("unknown ExternalFormat key '" + key + "'");
            }
            i = q2 + 1;
        }
        if (contentType == null || data == null) {
            throw error("ExternalFormat needs contentType and data");
        }
        return new String[]{contentType, data};
    }

    /**
     * {@code Relational #{ schema.table: 'csv' + 'csv'; }#} (probe "pf relational
     * island"): relationalCSVData spans keyword..{@code }#}; each table spans
     * {@code schema.table:}..the last value literal (the ';' excluded); values are the
     * '+'-concatenated unescaped strings.
     */
    private com.legend.protocol.Protocol.PTestPayload.RelationalCsv
            parseRelationalCsvIsland() {
        int kw = pos;
        advance();                                  // 'Relational'
        int openTok = pos;
        expect(TokenType.ISLAND_OPEN);
        skipIslandContent();
        int endTok = pos;
        expect(TokenType.ISLAND_END);
        String source = tokens.source();
        int from = tokens.end(openTok);
        int to = tokens.start(endTok);
        List<com.legend.protocol.Protocol.PTestPayload.CsvTable> tables = new ArrayList<>();
        int i = from;
        while (i < to) {
            char c = source.charAt(i);
            if (Character.isWhitespace(c) || c == ';') {
                i++;
                continue;
            }
            int tStart = i;
            int colon = source.indexOf(':', i);
            if (colon < 0 || colon >= to) {
                throw error("expected 'schema.table:' inside a Relational data island");
            }
            String head = source.substring(i, colon).trim();
            int dot = head.indexOf('.');
            if (dot < 0) {
                throw error("expected 'schema.table:' inside a Relational data island,"
                        + " got '" + head + "'");
            }
            String schema = head.substring(0, dot).trim();
            String table = head.substring(dot + 1).trim();
            i = colon + 1;
            StringBuilder values = new StringBuilder();
            int valEnd = colon;
            while (i < to) {
                char v = source.charAt(i);
                if (Character.isWhitespace(v) || v == '+') {
                    i++;
                    continue;
                }
                if (v == ';') {
                    valEnd = i;                     // the table span INCLUDES the ';'
                    i++;
                    break;
                }
                if (v != '\'') {
                    throw error("expected a quoted CSV chunk inside a Relational"
                            + " data island");
                }
                // THE shared decoder — this was a third private escape
                // table (n/t/r only; \b \f octal corrupted silently —
                // adversarial audit EP finding 13)
                int q = i + 1;
                int close = q;
                while (close < to && source.charAt(close) != '\'') {
                    close += source.charAt(close) == '\\' && close + 1 < to ? 2 : 1;
                }
                values.append(com.legend.protocol.Escapes.unescapeJavaLike(
                        source.substring(q, close)));
                valEnd = close;
                i = close + 1;
            }
            tables.add(new com.legend.protocol.Protocol.PTestPayload.CsvTable(
                    schema, table, values.toString(), new com.legend.protocol.SourceInfo("",
                            tokens.lineOf(tStart), tokens.columnOf(tStart),
                            tokens.lineOf(valEnd), tokens.columnOf(valEnd))));
        }
        return new com.legend.protocol.Protocol.PTestPayload.RelationalCsv(tables,
                spanOf(kw, pos - 1));
    }

    /** {@code id | call(args) => expected;} — the call NAME is not on the wire; each
     *  argument binds to the signature parameter at its position ({@code null} name when
     *  the signature runs out). */
    private com.legend.protocol.Protocol.PFunctionTest parseFunctionTest(
            FunctionSignature sig, int testStart, String testId) {
        expect(TokenType.PIPE);
        // the call NAME is not serialized but the engine VALIDATES it against the
        // enclosing function (rejection corpus: 'Function name in test ... does not
        // match')
        String callName = parseQualifiedName();
        String simpleCall = callName.contains("::")
                ? callName.substring(callName.lastIndexOf("::") + 2) : callName;
        if (!simpleCall.equals(sig.qualifiedName().contains("::")
                ? sig.qualifiedName().substring(sig.qualifiedName().lastIndexOf("::") + 2)
                : sig.qualifiedName())) {
            throw error("Function name in test '" + simpleCall
                    + "' does not match the enclosing function");
        }
        expect(TokenType.PAREN_OPEN);
        List<com.legend.protocol.Protocol.PTestParam> params = new ArrayList<>();
        while (!atEnd() && peek() != TokenType.PAREN_CLOSE) {
            if (peek() == TokenType.COMMA) {
                advance();
                continue;
            }
            int vStart = pos;
            int depth = 0;
            while (!atEnd()) {
                TokenType t = peek();
                if (depth == 0 && (t == TokenType.COMMA || t == TokenType.PAREN_CLOSE)) {
                    break;
                }
                // ALL THREE bracket kinds balance — a braced-lambda arg
                // {a,b|...} carries a top-level comma and mis-sliced here
                // when BRACE was omitted (adversarial audit EP finding 6)
                if (t == TokenType.PAREN_OPEN || t == TokenType.BRACKET_OPEN
                        || t == TokenType.BRACE_OPEN) depth++;
                else if (t == TokenType.PAREN_CLOSE || t == TokenType.BRACKET_CLOSE
                        || t == TokenType.BRACE_CLOSE) depth--;
                advance();
            }
            int idx = params.size();
            params.add(new com.legend.protocol.Protocol.PTestParam(
                    idx < sig.params().size() ? sig.params().get(idx).name() : null,
                    SpecParser.parse(tokens.slice(vStart, pos), dialect),
                    spanOf(vStart, pos - 1)));
        }
        expect(TokenType.PAREN_CLOSE);
        expect(TokenType.EQUAL);
        expect(TokenType.GREATER_THAN);
        com.legend.protocol.Protocol.PAssertion assertion;
        if (peek() == TokenType.PAREN_OPEN) {
            // => (JSON) '...' — equalToJson (probe "pf fmt expected and data")
            com.legend.protocol.Protocol.PTestPayload.ExternalFormat fmt =
                    (com.legend.protocol.Protocol.PTestPayload.ExternalFormat)
                            parseTestPayload();
            assertion = new com.legend.protocol.Protocol.PAssertion.EqualToJson(
                    fmt, fmt.sourceInformation());
        } else if (peek(1) == TokenType.ISLAND_OPEN
                && !"Relation".equals(peekText(0))
                && peek() == TokenType.VALID_STRING) {
            // an embedded-data assertion of any OTHER kind: the engine
            // refuses at the kind token (reprobe TestDomainGrammarParser#53)
            throw error("Unsupported embedded data type for function test"
                    + " assertion: " + peekText(0)
                    + ". Only 'Relation' is supported.");
        } else if ("Relation".equals(peekText(0)) && peek(1) == TokenType.ISLAND_OPEN) {
            // => Relation #{...}# — equalToRelation spanning Relation..}#
            int relStart = pos;
            advance();
            List<com.legend.protocol.Protocol.PTestPayload.RelationElement> els =
                    parseRelationIslandElements(true);
            if (els.size() != 1) {
                throw error("an equalTo Relation assertion needs exactly one block,"
                        + " got " + els.size());
            }
            assertion = new com.legend.protocol.Protocol.PAssertion.EqualToRelation(
                    els.get(0), spanOf(relStart, pos - 1));
        } else {
            int eStart = pos;
            int depth = 0;
            while (!atEnd()) {
                TokenType t = peek();
                if (depth == 0 && t == TokenType.SEMI_COLON) {
                    break;
                }
                if (t == TokenType.PAREN_OPEN || t == TokenType.BRACKET_OPEN
                        || t == TokenType.BRACE_OPEN) depth++;   // all three kinds, see above
                else if (t == TokenType.PAREN_CLOSE || t == TokenType.BRACKET_CLOSE
                        || t == TokenType.BRACE_CLOSE) depth--;
                advance();
            }
            assertion = new com.legend.protocol.Protocol.PAssertion.EqualTo(
                    SpecParser.parse(tokens.slice(eStart, pos), dialect), spanOf(eStart, pos - 1));
        }
        expect(TokenType.SEMI_COLON);
        return new com.legend.protocol.Protocol.PFunctionTest(testId,
                spanOf(testStart, pos - 1), params, assertion);
    }

    /**
     * A relation-data island {@code #{ path: col, col  v, v; ... }#}: dotted PATH line(s)
     * ending {@code :}, one COLUMNS line, semicolon-terminated string-valued ROWS
     * (probes "pf relation expected", "pf relation island data" — every cell serializes
     * as a STRING; the element span covers the island CONTENT, first path token to last
     * row token).
     */
    /**
     * A relation island {@code #{ [path.path:] cols  row  row; [next-block] }#}
     * (probes "pf relation expected", "pf relation island data"; corpus roundtrips):
     * {@code ;} terminates each BLOCK; within a block the first line is the columns,
     * every further NON-BLANK line is one row (no per-row semicolons); a block may be
     * header-only. Every cell serializes as a STRING; a block's span runs first
     * char..its terminating {@code ;}.
     */
    private List<com.legend.protocol.Protocol.PTestPayload.RelationElement>
            parseRelationIslandElements() {
        return parseRelationIslandElements(false);
    }


    /** Advance from just past an {@code ISLAND_OPEN} to ITS matching
     *  {@code ISLAND_END}: the lexer emits nested {@code ISLAND_START}/
     *  {@code ISLAND_END} pairs inside island content (Lexer islandDepth),
     *  so a flat {@code peek() != ISLAND_END} scan stops at an INNER
     *  {@code }#} and truncates the island (audit §5.5). */
    private void skipIslandContent() {
        int depth = 0;
        while (!atEnd()) {
            TokenType t = peek();
            if (t == TokenType.ISLAND_START) {
                depth++;
            } else if (t == TokenType.ISLAND_END) {
                if (depth == 0) {
                    return;
                }
                depth--;
            }
            advance();
        }
    }

    /**
     * In ASSERTION position ({@code => Relation #{...}#}) the engine REPARSES
     * {@code ": " + content.trim()} through a walker anchored at the {@code #{}
     * token (HelperTestAssertionGrammarParser.parseRelationElement +
     * buildIslandSourceInformation), so the element's span is a reparse
     * ARTIFACT, not the content's real coordinates: it starts at the synthetic
     * {@code :} — {@code (lineOf(#{), colOf(#{)+2)} whatever the content's
     * indentation — and ends where the trimmed text ends in reparse space
     * (probe ZAssertSpanProbe, all 8 geometries). DATA-position islands keep
     * real coordinates.
     */
    private List<com.legend.protocol.Protocol.PTestPayload.RelationElement>
            parseRelationIslandElements(boolean assertionSpans) {
        int openTok = pos;
        expect(TokenType.ISLAND_OPEN);
        skipIslandContent();
        int endTok = pos;
        expect(TokenType.ISLAND_END);
        String source = tokens.source();
        int from = tokens.end(openTok);
        int to = tokens.start(endTok);
        com.legend.protocol.SourceInfo assertSpan = assertionSpans
                ? islandReparseSpan(source, from, to, openTok) : null;
        List<com.legend.protocol.Protocol.PTestPayload.RelationElement> out =
                new ArrayList<>();
        int blockStart = from;
        boolean inQuotes = false;
        for (int i = from; i < to; i++) {
            char c = source.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;               // "" escapes toggle twice — net safe
                continue;
            }
            if (c != ';' || inQuotes) {
                continue;
            }
            int a = blockStart;
            while (a < i && Character.isWhitespace(source.charAt(a))) {
                a++;
            }
            if (a < i) {
                out.add(relationBlock(source, a, i, assertSpan));
            }
            blockStart = i + 1;
        }
        return out;
    }

    /** The engine's assertion-island span: where {@code ": " + trim(content)}
     *  lands when reparsed with lineOffset {@code lineOf(#{)-1} and
     *  columnOffset {@code colOf(#{)+1}. Start is the synthetic {@code :}
     *  (reparse 1:0); end is the trimmed text's last char — on reparse line 1
     *  the column offset applies, on later lines the reparse column is raw. */
    private com.legend.protocol.SourceInfo islandReparseSpan(
            String source, int from, int to, int openTok) {
        int t0 = from;
        while (t0 < to && Character.isWhitespace(source.charAt(t0))) {
            t0++;
        }
        int t1 = to - 1;
        while (t1 >= t0 && Character.isWhitespace(source.charAt(t1))) {
            t1--;
        }
        int line = tokens.startLine(openTok);
        int col = tokens.startColumn(openTok);
        int newlines = 0;
        int lastNl = -1;
        for (int i = t0; i <= t1; i++) {
            if (source.charAt(i) == '\n') {
                newlines++;
                lastNl = i;
            }
        }
        int eLine = newlines == 0 ? line : line + newlines;
        int eCol = newlines == 0 ? (t1 - t0 + 1) + col + 3 : t1 - lastNl;
        return new com.legend.protocol.SourceInfo("", line, col + 2, eLine, eCol);
    }


    /** The block's PATH separator colon, or -1 when the block has no path
     *  part. A bare {@code indexOf(':')} found a colon ANYWHERE — a data
     *  cell containing {@code 10:30} silently garbled path, columns and
     *  rows (text-surgery audit §1.1 #2). The engine's grammar admits a
     *  path only as leading {@code ident(.ident)*:}, so the colon counts
     *  only when UNQUOTED and its prefix is one dotted name — no newline,
     *  comma or quote before it. */
    static int pathColonOf(String body) {
        boolean inQuotes = false;
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ':' && !inQuotes) {
                String prefix = body.substring(0, i);
                for (int j = 0; j < prefix.length(); j++) {
                    char pc = prefix.charAt(j);
                    if (pc == '\n' || pc == ',' || pc == '"' || pc == '\'') {
                        return -1;      // rows/columns before the colon: no path
                    }
                }
                return prefix.strip().isEmpty() ? -1 : i;
            }
        }
        return -1;
    }

    /** Quote-aware cell split: commas inside double-quotes do not split; every cell
     *  keeps its RAW spelling, quotes and all (probe "pf csv cells"). */
    private static List<String> csvCells(String line) {
        List<String> cells = new ArrayList<>();
        boolean q = false;
        int start = 0;
        for (int i = 0; i <= line.length(); i++) {
            char c = i < line.length() ? line.charAt(i) : ',';
            if (c == '"') {
                q = !q;
            } else if (c == ',' && !q) {
                cells.add(line.substring(start, i).trim());
                start = i + 1;
            }
        }
        return cells;
    }

    /** One relation block over {@code source[a, semi]}; the span includes the ';'.
     *  A non-null {@code assertSpan} (assertion position) REPLACES the real
     *  coordinates — see {@link #islandReparseSpan}. */
    private com.legend.protocol.Protocol.PTestPayload.RelationElement relationBlock(
            String source, int a, int semi,
            @com.legend.Nullable com.legend.protocol.SourceInfo assertSpan) {
        String body = source.substring(a, semi);
        List<String> paths = new ArrayList<>();
        int colon = pathColonOf(body);
        if (colon >= 0) {
            String pathPart = body.substring(0, colon).trim();
            int segStart = 0;
            while (segStart <= pathPart.length()) {
                int dot = pathPart.indexOf('.', segStart);
                paths.add(pathPart.substring(segStart,
                        dot < 0 ? pathPart.length() : dot).trim());
                if (dot < 0) {
                    break;
                }
                segStart = dot + 1;
            }
            body = body.substring(colon + 1);
        }
        List<String> columns = new ArrayList<>();
        List<List<String>> rows = new ArrayList<>();
        boolean first = true;
        for (String line : body.lines().toList()) {
            if (line.isBlank()) {
                continue;
            }
            List<String> cells = csvCells(line);
            if (first) {
                columns.addAll(cells);
                first = false;
            } else {
                rows.add(cells);
            }
        }
        com.legend.protocol.SourceInfo span = assertSpan != null ? assertSpan
                : new com.legend.protocol.SourceInfo("", tokens.lineOf(a),
                        tokens.columnOf(a), tokens.lineOf(semi),
                        tokens.columnOf(semi));
        return new com.legend.protocol.Protocol.PTestPayload.RelationElement(
                columns, paths, rows, span);
    }


    /**
     * Parse a {@code native function ...;} declaration. {@code native} has
     * already been consumed by the caller. Mirrors Pure's
     * {@code nativeFunction} grammar rule: same signature shape as
     * {@link #functionElement()}, but no body block &mdash; the
     * declaration is terminated by a semicolon.
     *
     * <p>Pure syntax:
     * <pre>
     *   native function &lt;&lt;stereo&gt;&gt; {tag=v}
     *       my::pkg::fn&lt;T,V|m,n&gt;(p1:T1[m1], p2:T2[m2]):R[m];
     * </pre>
     */
    /** STRAIGHT-TO-MODEL — not yet migrated; see docs/PROTOCOL_MIGRATION_CENSUS.md. */
    private NativeFunctionDefinition nativeFunctionElement(int declStart,
            @com.legend.Nullable Documentation doc) {
        FunctionSignature sig = parseFunctionSignature(declStart, doc);
        expect(TokenType.SEMI_COLON);
        return new NativeFunctionDefinition(
                sig.qualifiedName(), sig.typeParams(), sig.multParams(),
                com.legend.model.FromProtocol.toFunctionParams(sig.params()),
                sig.returnType(), sig.returnMult(),
                com.legend.model.FromProtocol.stereotypes(sig.stereotypes()),
                com.legend.model.FromProtocol.taggedValues(sig.taggedValues()));
    }

    private com.legend.protocol.ParameterDefinition parseFunctionParameter() {
        int pStart = pos;
        String name = parseIdentifier();
        expect(TokenType.COLON);
        TypeExpression type = parseType();
        Multiplicity mult = parseMultiplicity();
        // Engine convention: a parameter's span covers its whole `name: Type[mult]` decl.
        return new com.legend.protocol.ParameterDefinition(name, type, mult,
                spanOf(pStart, pos - 1));
    }

    /**
     * Optional {@code <T,V|m,n>} block declaring generic type and/or
     * multiplicity parameters on a function (concrete or native).
     *
     * <p>Mirrors Pure's M3 grammar:
     * <pre>
     *   typeAndMultiplicityParameters: '<' ( typeParameters ('|' multParameters)? | multParameters ) '>'
     * </pre>
     * Either side may be omitted &mdash; e.g. {@code <T>}, {@code <T,V>},
     * {@code <T|m>}, {@code <T,V|m,n>}, or just {@code <|m>}.
     *
     * <p>Caller passes empty mutable lists; this method appends discovered
     * names. Does nothing if no leading {@code <}.
     */
    private void parseTypeAndMultiplicityParameters(List<String> typeParams,
                                                     List<String> multParams) {
        if (peek() != TokenType.LESS_THAN) return;
        advance(); // consume '<'

        // Type-parameter side (may be empty if next token is PIPE).
        if (peek() != TokenType.PIPE) {
            typeParams.add(parseIdentifier());
            while (match(TokenType.COMMA)) {
                typeParams.add(parseIdentifier());
            }
        }

        // Optional multiplicity side: '|' multParam (',' multParam)*
        if (match(TokenType.PIPE)) {
            multParams.add(parseIdentifier());
            while (match(TokenType.COMMA)) {
                multParams.add(parseIdentifier());
            }
        }

        expect(TokenType.GREATER_THAN);
    }

    // ============================================================
    // Service declaration
    // ============================================================

    /** PROTOCOL-FIRST — THE Service grammar is the registered
     *  {@link com.legend.parser.section.ServiceSectionGrammar}; this arm is
     *  its internal-dialect feed for BARE service elements. */
    private PackageableElement serviceElement() {
        return com.legend.model.FromProtocol.toServiceSectionElement(
                com.legend.parser.section.ServiceSectionGrammar
                        .parseElement(this));
    }

    // ============================================================
    // Runtime declaration
    // ============================================================

    /**
     * {@code Runtime qualifiedName { mappings: [...]; connections: [...]; }}.
     * Embedded {@code JsonModelConnection} islands ({@code #{ ... }#}) are
     * captured and parsed via regex (engine parity).
     */
    /**
     * PROTOCOL-path Runtime parse — a delegator kept for the per-element
     * callers (the equivalence harness). THE grammar lives in
     * {@link com.legend.parser.section.RuntimeSectionGrammar}.
     */
    public com.legend.protocol.Protocol.PRuntime parseRuntimeProtocol() {
        return com.legend.parser.section.RuntimeSectionGrammar
                .parseElement(this);
    }

    /**
     * PROTOCOL-path ###Connection element parse — a delegator kept for the
     * per-element callers (the equivalence harness, probes). THE grammar
     * lives in {@link com.legend.parser.section.ConnectionSectionGrammar};
     * this cursor is just its shared-stream feed.
     */
    public com.legend.protocol.Protocol.PConnection parseConnectionProtocol() {
        return com.legend.parser.section.ConnectionSectionGrammar
                .parseElement(this);
    }


    /** PROTOCOL-FIRST — THE Runtime grammar is the registered
     *  {@link com.legend.parser.section.RuntimeSectionGrammar}; this arm is
     *  its internal-dialect feed for BARE runtime elements (handles both
     *  {@code Runtime} and {@code SingleConnectionRuntime}). */
    private PackageableElement runtimeElement() {
        com.legend.protocol.Protocol.PRuntime pr =
                com.legend.parser.section.RuntimeSectionGrammar
                        .parseElement(this);
        try {
            return com.legend.model.FromProtocol.toRuntimeElement(pr);
        } catch (com.legend.model.FromProtocol.UnsupportedConnectionShape u) {
            throw error(u.reason());
        }
    }

    // ============================================================
    // RelationalDatabaseConnection
    // ============================================================
    // ============================================================
    // Source-text reconstruction (for lazy expression bodies)
    // ============================================================

    /**
     * Reconstruct the verbatim source text spanning tokens
     * {@code [startToken, endToken)}. Returns empty string if the range is
     * empty. Still used to capture {@code testSuites} blocks (D-3) and
     * embedded JSON-island raw text; expression bodies are now sliced and
     * handed to {@link SpecParser} instead of being kept as text.
     */
    // ============================================================
    // Property declaration (regular)
    // ============================================================

    private com.legend.protocol.Protocol.PProperty parseProperty() {
        int startTok = pos;
        Documentation doc = parseDocumentation();
        List<com.legend.protocol.Protocol.PStereotype> stereotypes = parseStereotypes();
        List<com.legend.protocol.Protocol.PTaggedValue> taggedValues =
                taggedValuesWithDocumentation(doc, parseTaggedValues());
        // AGGREGATION KIND — (composite) / (shared) / (none); UPPERCASE on the wire
        // (ProbeWireShapes "agg kind and varchar")
        String aggregation = null;
        if (peek() == TokenType.PAREN_OPEN) {
            advance();
            String kind = parseIdentifier();
            if (!kind.equals("composite") && !kind.equals("shared") && !kind.equals("none")) {
                throw error("unknown aggregation kind '(" + kind + ")'");
            }
            aggregation = kind.toUpperCase(java.util.Locale.ROOT);
            expect(TokenType.PAREN_CLOSE);
        }
        String name = parseIdentifier();
        expect(TokenType.COLON);
        TypeExpression type = parseType();   // parseType threads the type's own span onto the node
        Multiplicity mult = parseMultiplicity();
        // property DEFAULT VALUE (real pure: prop: Boolean[1] = false;). The expression is
        // captured as a value-spec tree via SpecParser over a slice of THIS token stream, so
        // positions stay file-absolute. A default SpecParser cannot read REFUSES, positioned
        // — engine-style; carrying null "totally" was a silent accept on the compile path.
        com.legend.protocol.Protocol.PDefaultValue defaultValue = null;
        if (match(TokenType.EQUAL)) {
            int defStart = pos;
            int depth = 0;
            while (!atEnd()) {
                TokenType t = peek();
                if (depth == 0 && t == TokenType.SEMI_COLON) {
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
            // a malformed default value is a PARSE REFUSAL, engine-style —
            // the old catch carried null and silently accepted what the
            // engine rejects on the compile path (adversarial audit,
            // ElementParser finding 2); if a legal-but-unmodelled form
            // resurfaces in the corpus it must land as a named leniency,
            // not a swallow
            ValueSpecification value =
                    SpecParser.parse(tokens.slice(defStart, pos), dialect);
            if (value instanceof com.legend.protocol.spec.LambdaFunction) {
                // BOTH reference grammars refuse a lambda default
                // (defaultValueExpression admits references, ^instances,
                // literals and arrays only; legend-pure M3 agrees —
                // refusal-asymmetry INVESTIGATE row TestDefaultValue#41)
                throw error("a lambda is not a legal property default value");
            }
            // the engine's shared span helper ends a MULTI-LINE token on its
            // start line (start column + text length - 1): a '''...''' default
            // value keeps that arithmetic (pure TestDocumentation, 4.145.0)
            com.legend.protocol.SourceInfo defSpan = tokens.type(pos - 1) == TokenType.DOC_STRING
                    ? new com.legend.protocol.SourceInfo(spanSourceId(),
                            tokens.startLine(defStart), tokens.startColumn(defStart),
                            docStringSpan(pos - 1).endLine(), docStringSpan(pos - 1).endColumn())
                    : spanOf(defStart, pos - 1);
            defaultValue = new com.legend.protocol.Protocol.PDefaultValue(value, defSpan);
        }
        expect(TokenType.SEMI_COLON);
        // Positions are captured HERE, at construction, because this is the only point where the
        // token span of this property is in hand. No side table, no second pass.
        return new com.legend.protocol.Protocol.PProperty(
                name, type, mult, stereotypes, taggedValues,
                spanOf(startTok, pos - 1), defaultValue, aggregation);
    }



    // ============================================================
    // Shared helpers (engine-parity)
    // ============================================================

    // -----------------------------------------------------------------
    // TokenStreamCursor accessors.
    //
    // Implementing the interface gives us the entire lexical layer
    // (peek/match/expect/consume/advance/text/safeText/textEquals/error,
    // parseIdentifier, parseQualifiedName) plus the type-expression
    // grammar (parseType / parseTypeArgument / parseMultiplicity and
    // their sub-grammars) as inherited defaults.
    //
    // Previously ElementParser carried its own private duplicates of
    // every primitive AND a bridge that allocated a standalone helper
    // class for each type expression. Both layers are now gone; the
    // interface is the single source of truth.
    // -----------------------------------------------------------------

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

    /** {@code <<profile.stereotype, ...>>}; returns empty list if absent. */
    List<com.legend.protocol.Protocol.PStereotype> parseStereotypes() {
        if (peek() != TokenType.LESS_THAN || peek(1) != TokenType.LESS_THAN) {
            return List.of();
        }
        advance();
        advance();
        List<com.legend.protocol.Protocol.PStereotype> result = new ArrayList<>();
        result.add(parseStereotype());
        while (match(TokenType.COMMA)) {
            result.add(parseStereotype());
        }
        expect(TokenType.GREATER_THAN);
        expect(TokenType.GREATER_THAN);
        return result;
    }

    private com.legend.protocol.Protocol.PStereotype parseStereotype() {
        int profStart = pos;
        String profile = com.legend.protocol.Protocol.unquotePath(parseQualifiedName());
        int profEnd = pos - 1;
        expect(TokenType.DOT);
        String name = parseIdentifier();
        // profileSourceInformation covers the profile FQN; sourceInformation the whole ptr.
        return new com.legend.protocol.Protocol.PStereotype(profile, name,
                spanOf(profStart, profEnd), spanOf(profStart, pos - 1));
    }

    /** {@code { profile.tag = 'value', ... }}; returns empty list if not a tag block. */
    List<com.legend.protocol.Protocol.PTaggedValue> parseTaggedValues() {
        if (peek() != TokenType.BRACE_OPEN) return List.of();
        if (!looksLikeTaggedValueBlock(tokens, pos)) return List.of();

        advance(); // skip {
        List<com.legend.protocol.Protocol.PTaggedValue> result = new ArrayList<>();
        result.add(parseTaggedValue());
        while (match(TokenType.COMMA)) {
            result.add(parseTaggedValue());
        }
        expect(TokenType.BRACE_CLOSE);
        return result;
    }

    /**
     * Heuristic: does the {@code {} block starting at {@code bracePos} look
     * like a tagged-value block (e.g.
     * {@code {meta::pure::profiles::doc.doc = 'desc'}}) rather than the
     * element body?
     *
     * <p>Pattern matched: {@code '{' IDENT ('::' IDENT)* '.' IDENT '='}.
     * Shared by the parser (which dispatches on this when consuming tagged
     * values inline) and the shallow scanner in the IDE layer
     * ({@code com.legend.ide.ModelIndexer}), which
     * needs to skip a tagged-value block to reach the FQN. <strong>Both
     * must agree on the heuristic</strong>: if the scanner says yes but
     * the parser says no (or vice versa), token offsets drift and
     * downstream parses fail confusingly. Keeping the predicate in one
     * place prevents that drift.
     */
    public static boolean looksLikeTaggedValueBlock(TokenStream tokens, int bracePos) {
        int n = tokens.count();
        if (bracePos >= n || tokens.type(bracePos) != TokenType.BRACE_OPEN) return false;
        int p = bracePos + 1;
        if (p >= n || !IDENTIFIER_TOKENS.contains(tokens.type(p))) return false;
        while (p < n && IDENTIFIER_TOKENS.contains(tokens.type(p))) {
            p++;
            if (p >= n) return false;
            if (tokens.type(p) == TokenType.PATH_SEPARATOR) p++;
            else break;
        }
        if (p >= n || tokens.type(p) != TokenType.DOT) return false;
        p++;
        if (p >= n || !IDENTIFIER_TOKENS.contains(tokens.type(p))) return false;
        p++;
        return p < n && tokens.type(p) == TokenType.EQUAL;
    }

    private com.legend.protocol.Protocol.PTaggedValue parseTaggedValue() {
        int start = pos;
        int profStart = pos;
        String profile = com.legend.protocol.Protocol.unquotePath(parseQualifiedName());
        int profEnd = pos - 1;
        expect(TokenType.DOT);
        int tagStart = pos;
        String tag = parseIdentifier();
        int tagEnd = pos - 1;
        expect(TokenType.EQUAL);
        if (peek() == TokenType.DOC_STRING) {
            if (dialect.refusesPlatformDialect() && text().indexOf('\n') < 0) {
                // single-line ''' — the oracle's lexer splits it into
                // adjacent strings and refuses (probed live 2026-08-12)
                throw error("Unexpected token '" + text() + "'");
            }
            // '''...''' tagged-value VALUE (4.138, ZMissedRowsProbe): shared
            // strip rule; the tv span ends by the token's single-line
            // column arithmetic
            int dTok = pos;
            String raw = text();
            String value = TokenStreamCursor.docStringValue(raw);
            advance();
            com.legend.protocol.SourceInfo d = docStringSpan(dTok);
            com.legend.protocol.SourceInfo s = spanOf(start, start);
            // the engine's toCString: a block-shaped literal is flagged
            // multiLine (4.145.0) and rides the wire as an object
            return new com.legend.protocol.Protocol.PTaggedValue(
                    new com.legend.protocol.Protocol.PTag(profile, tag,
                            spanOf(profStart, profEnd),
                            spanOf(tagStart, tagEnd)),
                    value, TokenStreamCursor.isTextBlock(raw),
                    new com.legend.protocol.SourceInfo(s.sourceId(),
                            s.startLine(), s.startColumn(), d.endLine(),
                            d.endColumn()));
        }
        String rawValue = consume(TokenType.STRING);
        // Unquote AND unescape: the wire carries the LOGICAL string ("it's", not "it\\'s") —
        // quote-stripping alone left escapes behind (harness DIFF on dateExtension.pure).
        String value = TokenStreamCursor.unquoteAndUnescape(rawValue, this);
        // M3 DIALECT (Phase 4 entry gate — the PCT sources spell long doc
        // strings as 'a' + 'b' + ...): fold the concatenation at parse;
        // the exact-engine surface refuses engine-verbatim
        if (!dialect.refusesPlatformDialect()) {
            StringBuilder folded = null;
            while (peek() == TokenType.PLUS
                    && peek(1) == TokenType.STRING) {
                advance();   // +
                if (folded == null) {
                    folded = new StringBuilder(value);
                }
                folded.append(TokenStreamCursor.unquoteAndUnescape(
                        consume(TokenType.STRING), this));
            }
            if (folded != null) {
                value = folded.toString();
            }
        }
        // NOTE the asymmetry, verified against legend-engine: a TAG's sourceInformation covers only
        // the tag name, while a STEREOTYPE's covers the whole profile.name.
        return new com.legend.protocol.Protocol.PTaggedValue(
                new com.legend.protocol.Protocol.PTag(profile, tag,
                        spanOf(profStart, profEnd), spanOf(tagStart, tagEnd)),
                value, false, spanOf(start, pos - 1));
    }

    // ============================================================
    // Token cursor: peek/peek(int)/text/safeText/textEquals/advance/
    // atEnd/match/expect/consume/error/parseIdentifier/parseQualifiedName
    // all live on TokenStreamCursor as default methods. The local
    // duplicates that used to sit here were removed when this class
    // started implementing the interface.
    // ============================================================
}
