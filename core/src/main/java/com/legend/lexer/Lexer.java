package com.legend.lexer;

import java.util.HashMap;
import java.util.Map;

/**
 * Hand-rolled batch lexer for Pure source code.
 *
 * <p>Tokenizes the entire source upfront into three parallel
 * {@code int[]} arrays (token-type ordinals, source start offsets,
 * source end offsets), then wraps them in an immutable
 * {@link TokenStream} by reference &mdash; no trim, no copy. The
 * arrays may carry slack capacity from the last doubling {@link #grow()}.
 *
 * <h2>Usage</h2>
 * <pre>
 *   TokenStream stream = Lexer.tokenize(pureSource);
 * </pre>
 *
 * <p><strong>Stateful only during tokenization.</strong> A {@code Lexer}
 * instance is allocated per call to {@link #tokenize(String)}, run to
 * completion, then thrown away. The resulting {@link TokenStream} owns
 * the final arrays and is the only object that survives.
 *
 * <p>Ported from {@code engine/com.gs.legend.parser.PureLexer2} with no
 * functional changes &mdash; only the public surface (returns
 * {@code TokenStream}) and package were updated. See engine source for
 * detailed line-by-line scan rules; this class is a faithful translation.
 */
public final class Lexer {

    private static final int INITIAL_CAPACITY = 4096;

    // ==================== Keyword Map ====================

    private static final Map<String, TokenType> KEYWORDS = new HashMap<>(200);

    static {
        // M3
        KEYWORDS.put("all", TokenType.ALL);
        KEYWORDS.put("let", TokenType.LET);
        KEYWORDS.put("allVersions", TokenType.ALL_VERSIONS);
        KEYWORDS.put("allVersionsInRange", TokenType.ALL_VERSIONS_IN_RANGE);
        // Domain
        KEYWORDS.put("import", TokenType.IMPORT);
        KEYWORDS.put("Class", TokenType.CLASS);
        KEYWORDS.put("Association", TokenType.ASSOCIATION);
        KEYWORDS.put("Profile", TokenType.PROFILE);
        KEYWORDS.put("Enum", TokenType.ENUM);
        KEYWORDS.put("function", TokenType.FUNCTION);
        KEYWORDS.put("extends", TokenType.EXTENDS);
        KEYWORDS.put("stereotypes", TokenType.STEREOTYPES);
        KEYWORDS.put("tags", TokenType.TAGS);
        KEYWORDS.put("native", TokenType.NATIVE);
        KEYWORDS.put("as", TokenType.AS);
        // Boolean literals
        KEYWORDS.put("true", TokenType.TRUE);
        KEYWORDS.put("false", TokenType.FALSE);
        // Mapping
        KEYWORDS.put("Mapping", TokenType.MAPPING);
        KEYWORDS.put("include", TokenType.INCLUDE);
        KEYWORDS.put("testSuites", TokenType.MAPPING_TESTABLE_SUITES);
        KEYWORDS.put("query", TokenType.MAPPING_TESTS_QUERY);
        // Service
        KEYWORDS.put("Service", TokenType.SERVICE);
        KEYWORDS.put("pattern", TokenType.SERVICE_PATTERN);
        KEYWORDS.put("owners", TokenType.SERVICE_OWNERS);
        KEYWORDS.put("documentation", TokenType.SERVICE_DOCUMENTATION);
        KEYWORDS.put("autoActivateUpdates", TokenType.SERVICE_AUTO_ACTIVATE_UPDATES);
        KEYWORDS.put("execution", TokenType.SERVICE_EXEC);
        KEYWORDS.put("Single", TokenType.SERVICE_SINGLE);
        KEYWORDS.put("mapping", TokenType.SERVICE_MAPPING);
        KEYWORDS.put("runtime", TokenType.SERVICE_RUNTIME);
        // Runtime
        KEYWORDS.put("Runtime", TokenType.RUNTIME);
        KEYWORDS.put("SingleConnectionRuntime", TokenType.SINGLE_CONNECTION_RUNTIME);
        KEYWORDS.put("mappings", TokenType.MAPPINGS);
        KEYWORDS.put("connections", TokenType.CONNECTIONS);
        KEYWORDS.put("connection", TokenType.CONNECTION);
        // Database / Relational
        KEYWORDS.put("Database", TokenType.DATABASE);
        KEYWORDS.put("Table", TokenType.TABLE);
        KEYWORDS.put("Schema", TokenType.SCHEMA);
        KEYWORDS.put("View", TokenType.VIEW);
        KEYWORDS.put("Filter", TokenType.FILTER);
        KEYWORDS.put("MultiGrainFilter", TokenType.MULTIGRAIN_FILTER);
        KEYWORDS.put("Join", TokenType.JOIN);
        KEYWORDS.put("and", TokenType.RELATIONAL_AND);
        KEYWORDS.put("or", TokenType.RELATIONAL_OR);
        KEYWORDS.put("Relational", TokenType.RELATIONAL);
        KEYWORDS.put("Pure", TokenType.PURE_MAPPING);
        // Milestoning
        // Mapping modifiers
        KEYWORDS.put("AssociationMapping", TokenType.ASSOCIATION_MAPPING);
        KEYWORDS.put("EnumerationMapping", TokenType.ENUMERATION_MAPPING);
        KEYWORDS.put("Otherwise", TokenType.OTHERWISE);
        KEYWORDS.put("Inline", TokenType.INLINE);
        // Connection
        KEYWORDS.put("RelationalDatabaseConnection", TokenType.RELATIONAL_DATABASE_CONNECTION);
        KEYWORDS.put("store", TokenType.STORE);
        KEYWORDS.put("type", TokenType.TYPE);
        KEYWORDS.put("specification", TokenType.RELATIONAL_DATASOURCE_SPEC);
        KEYWORDS.put("auth", TokenType.RELATIONAL_AUTH_STRATEGY);
        KEYWORDS.put("H2", TokenType.H2);
    }

    // ==================== Tilde Command Map ====================

    private static final Map<String, TokenType> TILDE_COMMANDS = Map.ofEntries(
            Map.entry("~filter", TokenType.FILTER_CMD),
            Map.entry("~distinct", TokenType.DISTINCT_CMD),
            Map.entry("~groupBy", TokenType.GROUP_BY_CMD),
            Map.entry("~mainTable", TokenType.MAIN_TABLE_CMD),
            Map.entry("~primaryKey", TokenType.PRIMARY_KEY_CMD),
            Map.entry("~src", TokenType.SRC_CMD));

    // ==================== Instance state ====================

    private final String source;
    private final int length;
    private int pos;
    private int islandDepth;

    private int[] types;
    private int[] starts;
    private int[] ends;
    private int count;

    private Lexer(String source) {
        this(source, 0, source.length());
    }

    /** A scan of {@code source} from {@code from} to {@code to} only: the offsets it emits are the source's
     *  own. An island's content is lexed this way, in place (W0.8): no padded copy, no second line index. */
    private Lexer(String source, int from, int to) {
        this.source = source;
        this.pos = from;
        this.length = to;
        // Heuristic: ~3.5 chars per token on average for Pure source.
        int estimated = Math.max(INITIAL_CAPACITY, (to - from) / 3);
        this.types = new int[estimated];
        this.starts = new int[estimated];
        this.ends = new int[estimated];
    }

    // ==================== Public API ====================

    /**
     * Tokenize a Pure source string into an immutable {@link TokenStream}.
     *
     * <p>The lexer's internal arrays are handed off to the returned stream
     * by reference &mdash; no trim, no copy. Arrays may carry slack capacity
     * beyond {@link TokenStream#count()} (the last doubling {@link #grow()}
     * may have over-allocated); this matches engine's
     * {@code PureLexer2} memory profile exactly.
     */
    public static TokenStream tokenize(String source) {
        Lexer lexer = new Lexer(source);
        lexer.run();
        return new TokenStream(source, lexer.count, lexer.types, lexer.starts,
                lexer.ends, lexer.skippedSections, lexer.sectionHeaders);
    }

    /** {@link #tokenize} over {@code source}'s characters {@code [from, to)} only, in normal mode, every offset
     *  the source's own; {@link TokenStream#lexRange} is the caller (it shares the line index). */
    static TokenStream tokenizeRange(String source, int from, int to) {
        Lexer lexer = new Lexer(source, from, to);
        lexer.run();
        return new TokenStream(source, lexer.count, lexer.types, lexer.starts,
                lexer.ends, lexer.skippedSections, lexer.sectionHeaders);
    }

    private void run() {
        while (pos < length) {
            if (islandDepth > 0) {
                scanIslandToken();
            } else {
                scanNormalToken();
            }
        }
    }

    // ==================== Token Emission ====================

    private void emit(TokenType type, int start, int end) {
        if (count == types.length) grow();
        types[count] = type.ordinal();
        starts[count] = start;
        ends[count] = end;
        count++;
    }

    private void grow() {
        int newLen = types.length * 2;
        int[] nt = new int[newLen], ns = new int[newLen], ne = new int[newLen];
        System.arraycopy(types, 0, nt, 0, count);
        System.arraycopy(starts, 0, ns, 0, count);
        System.arraycopy(ends, 0, ne, 0, count);
        types = nt; starts = ns; ends = ne;
    }

    // ==================== Char Classification ====================

    private static boolean isIdentStart(char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_' || (c >= '0' && c <= '9');
    }

    private static boolean isIdentPart(char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '$';
    }

    // ==================== Normal Mode Scanning ====================

    private void scanNormalToken() {
        char c = source.charAt(pos);

        // Whitespace
        if (c == ' ' || c == '\t' || c == '\r' || c == '\n') { skipWhitespace(); return; }
        // Comments and divide
        if (c == '/') {
            if (pos + 1 < length) {
                char c2 = source.charAt(pos + 1);
                if (c2 == '/') { skipLineComment(); return; }
                if (c2 == '*') { skipBlockComment(); return; }
            }
            emit(TokenType.DIVIDE, pos, pos + 1); pos++; return;
        }
        // Section header — LINE-ANCHORED, the engine's rule
        // (CodeLexerGrammar SECTION_START is '\n###' ValidString; a
        // mid-line '###' stays section CONTENT there and refuses in the
        // section grammar — adversarial audit: lite accepted it as a
        // header). A mid-line '###' here falls through to scanHash and
        // dies as INVALID.
        if (atSectionBoundary(pos)) {
            skipSectionHeader(); return;
        }
        // String literal
        if (c == '\'') { scanStringLiteral(); return; }
        // Quoted string
        if (c == '"') { scanQuotedString(); return; }
        // Percent
        if (c == '%') { scanPercent(); return; }
        // Hash
        if (c == '#') { scanHash(); return; }
        // Tilde
        if (c == '~') { scanTilde(); return; }
        // Arrow or minus
        if (c == '-') { scanMinusOrArrow(); return; }
        // Numeric literal (must check BEFORE identifier - INTEGER has priority over VALID_STRING)
        if (c >= '0' && c <= '9') { scanNumericLiteral(); return; }
        // Identifier / keyword
        if (isIdentStart(c)) { scanIdentifierOrKeyword(); return; }
        // File name ?[...
        if (c == '?' && pos + 1 < length && source.charAt(pos + 1) == '[') { scanFileName(); return; }
        // Multi-char punctuation
        if (scanMultiCharPunct(c)) return;
        // Single-char punctuation
        scanSingleCharPunct(c);
    }

    // ==================== Skip Rules ====================

    /** A LINE-ANCHORED {@code ###} at {@code p} — the engine's raw
     *  sectionizer splits on {@code '\n###'} BEFORE any lexing
     *  (PureGrammarParser prepends {@code "\n###Pure\n"}), so a section
     *  boundary is a HARD STOP that no comment, string, or island scan may
     *  cross (adversarial audit: a {@code ###Mapping} inside a block
     *  comment or multi-line string split sections there and stayed hidden
     *  here). */
    private boolean atSectionBoundary(int p) {
        return p + 2 < length && source.charAt(p) == '#'
                && source.charAt(p + 1) == '#' && source.charAt(p + 2) == '#'
                && (p == 0 || source.charAt(p - 1) == '\n');
    }

    private void skipWhitespace() {
        while (pos < length) {
            char c = source.charAt(pos);
            if (c == ' ' || c == '\t' || c == '\r' || c == '\n') pos++;
            else break;
        }
    }

    private void skipLineComment() {
        pos += 2;
        while (pos < length && source.charAt(pos) != '\n') pos++;
        if (pos < length) pos++;
    }

    private void skipBlockComment() {
        int start = pos;
        pos += 2;
        while (pos < length) {
            if (atSectionBoundary(pos)) {
                // the raw sectionizer splits the comment in half — the
                // half-open comment is unlexable content, like the engine
                emit(TokenType.INVALID, start, pos);
                return;
            }
            if (source.charAt(pos) == '*' && pos + 1 < length && source.charAt(pos + 1) == '/') {
                pos += 2; return;
            }
            pos++;
        }
        // UNTERMINATED block comment: the engine refuses (its lexer never
        // silently comments-to-EOF — adversarial audit divergence 8)
        emit(TokenType.INVALID, start, pos);
    }

    /**
     * Section kinds whose CONTENT this lexer understands. Any other kind
     * (###Diagram — presentation metadata) is an OPAQUE DSL: its content
     * must not reach the Pure token rules (a diagram color literal
     * {@code #FFFFCC} is unlexable Pure), so the whole section skips to
     * the next header. Its elements are simply invisible — exactly what
     * the engine's semantics need from a diagram.
     */
    /** The lexable-section names, for the registry-agreement guardrail —
     *  this set and the registry's lexable grammars must not drift
     *  (an unlisted claimed section raw-skips SILENTLY: Elasticsearch
     *  2026-08-10). */
    public static java.util.Set<String> lexableSections() {
        return LEXABLE_SECTIONS;
    }

    private static final java.util.Set<String> LEXABLE_SECTIONS =
            java.util.Set.of("Pure", "Mapping", "Relational", "Connection",
                    "Runtime", "Data", "Service", "DataSpace", "Persistence",
                    "Snowflake", "ExternalFormat", "FileGeneration", "MemSql",
                    "BigQuery", "HostedService", "FunctionJar", "Text",
                    "GenerationSpecification", "DataQualityValidation",
                    "ServiceStore", "MongoDB", "Deephaven", "Elasticsearch",
                    "QueryPostProcessor");

    /** Opaque sections raw-skipped by {@link #skipSectionHeader} — RECORDED,
     *  never silent (GRAMMAR_EXTENSIBILITY.md step 1): the parser surfaces
     *  each through the SectionGrammarRegistry as "no grammar registered". */
    private final java.util.List<TokenStream.SectionHeader> sectionHeaders =
            new java.util.ArrayList<>();
    private final java.util.List<TokenStream.SkippedSection> skippedSections =
            new java.util.ArrayList<>();

    private void skipSectionHeader() {
        int nameStart = pos + 3;
        int nameEnd = nameStart;
        while (nameEnd < length && Character.isLetterOrDigit(source.charAt(nameEnd))) {
            nameEnd++;
        }
        String kind = source.substring(nameStart, nameEnd);
        while (pos < length && source.charAt(pos) != '\n') pos++;
        if (pos < length) pos++;
        // HEADER-LINE RESIDUE: only whitespace or a // comment may follow
        // the name — recorded here (the lexer never throws); the section
        // consumers refuse it, reference-shaped
        String residue = null;
        int rLine = -1;
        int rCol = -1;
        int lineEnd = source.indexOf('\n', nameEnd);
        if (lineEnd > length) {
            lineEnd = length;   // a range scan (W0.8) stops at its bound
        }
        if (lineEnd < 0) {
            lineEnd = length;
        }
        int r = nameEnd;
        while (r < lineEnd && (source.charAt(r) == ' '
                || source.charAt(r) == '\t' || source.charAt(r) == '\r')) {
            r++;
        }
        boolean comment = r + 1 < lineEnd && source.charAt(r) == '/'
                && source.charAt(r + 1) == '/';
        if (r < lineEnd && !comment) {
            int we = r;
            while (we < lineEnd
                    && !Character.isWhitespace(source.charAt(we))) {
                we++;
            }
            residue = source.substring(r, we);
            rLine = 1;
            rCol = 1;
            for (int k = 0; k < r; k++) {
                if (source.charAt(k) == '\n') {
                    rLine++;
                    rCol = 1;
                } else {
                    rCol++;
                }
            }
        }
        // record EVERY header, lexable or not — a parser may need to know
        // where its own section started (TokenStream.SectionHeader)
        sectionHeaders.add(new TokenStream.SectionHeader(kind, nameStart, pos,
                residue, rLine, rCol));
        if (!kind.isEmpty() && !LEXABLE_SECTIONS.contains(kind)) {
            // opaque section: raw-skip to the next line-anchored ### — a
            // lexing necessity (Diagram color literals are unlexable Pure),
            // but the SKIP is data, not silence
            int skipStart = pos;
            while (pos < length) {
                if (source.charAt(pos) == '#' && pos + 3 <= length && source.startsWith("###", pos)
                        && (pos == 0 || source.charAt(pos - 1) == '\n')) {
                    break;
                }
                pos++;
            }
            skippedSections.add(new TokenStream.SkippedSection(
                    kind, nameStart, skipStart, pos));
        }
    }

    private void skipWhitespaceOnly() {
        while (pos < length) {
            char c = source.charAt(pos);
            if (c == ' ' || c == '\t' || c == '\r' || c == '\n') pos++;
            else break;
        }
    }

    // ==================== String Literals ====================

    private void scanStringLiteral() {
        int start = pos;
        // ''' opens a multi-line documentation literal (engine #5008
        // doc.doc sugar) — one token to the closing ''' so parsers can
        // reject (or later support) it as a UNIT, never as a silent pair
        // of adjacent strings
        if (pos + 2 < length && source.charAt(pos + 1) == '\''
                && source.charAt(pos + 2) == '\'') {
            pos += 3;
            while (pos < length && !atSectionBoundary(pos)) {
                if (source.charAt(pos) == '\'' && pos + 2 < length
                        && source.charAt(pos + 1) == '\''
                        && source.charAt(pos + 2) == '\'') {
                    pos += 3;
                    emit(TokenType.DOC_STRING, start, pos);
                    return;
                }
                pos++;
            }
            emit(TokenType.DOC_STRING, start, pos);
            return;
        }
        pos++;
        while (pos < length && !atSectionBoundary(pos)) {
            char c = source.charAt(pos);
            // clamp: a backslash as the LAST source char must not push pos
            // past length — the emitted end offset would make every text()
            // call throw a raw SIOOBE (deep-audit 1f)
            if (c == '\\') { pos = Math.min(pos + 2, length); }
            else if (c == '\'') { pos++; emit(TokenType.STRING, start, pos); return; }
            else { pos++; }
        }
        // unterminated (EOF or a raw section boundary cut the literal)
        emit(TokenType.STRING, start, pos);
    }

    private void scanQuotedString() {
        int start = pos;
        pos++;
        while (pos < length && !atSectionBoundary(pos)) {
            char c = source.charAt(pos);
            if (c == '\\') { pos = Math.min(pos + 2, length); }   // clamp, see scanStringLiteral
            else if (c == '"') { pos++; emit(TokenType.QUOTED_STRING, start, pos); return; }
            else { pos++; }
        }
        emit(TokenType.QUOTED_STRING, start, pos);
    }

    // ==================== Numeric Literals ====================

    private void scanNumericLiteral() {
        int start = pos;
        while (pos < length && source.charAt(pos) >= '0' && source.charAt(pos) <= '9') pos++;

        if (pos < length && source.charAt(pos) == '.' && pos + 1 < length && source.charAt(pos + 1) != '.') {
            pos++;
            while (pos < length && source.charAt(pos) >= '0' && source.charAt(pos) <= '9') pos++;
            scanExponent();
            if (pos < length && (source.charAt(pos) == 'd' || source.charAt(pos) == 'D')) {
                pos++; emit(TokenType.DECIMAL, start, pos); return;
            }
            if (pos < length && (source.charAt(pos) == 'f' || source.charAt(pos) == 'F')) pos++;
            emit(TokenType.FLOAT, start, pos);
            return;
        }

        // Integer with EXPONENT (1e3, 1e3d — engine's Float/Decimal
        // grammar admits exponent without a fraction; 1e3d was unlexable
        // here, deep-audit 1f)
        if (pos < length && (source.charAt(pos) == 'e' || source.charAt(pos) == 'E')
                && pos + 1 < length
                && (Character.isDigit(source.charAt(pos + 1))
                        || ((source.charAt(pos + 1) == '+' || source.charAt(pos + 1) == '-')
                                && pos + 2 < length
                                && Character.isDigit(source.charAt(pos + 2))))) {
            scanExponent();
            if (pos < length && (source.charAt(pos) == 'd' || source.charAt(pos) == 'D')) {
                pos++; emit(TokenType.DECIMAL, start, pos); return;
            }
            if (pos < length && (source.charAt(pos) == 'f' || source.charAt(pos) == 'F')) pos++;
            emit(TokenType.FLOAT, start, pos);
            return;
        }
        // Integer with decimal suffix: 42d
        if (pos < length && (source.charAt(pos) == 'd' || source.charAt(pos) == 'D')) {
            pos++; emit(TokenType.DECIMAL, start, pos); return;
        }

        emit(TokenType.INTEGER, start, pos);
    }

    private void scanExponent() {
        if (pos < length && (source.charAt(pos) == 'e' || source.charAt(pos) == 'E')) {
            pos++;
            if (pos < length && (source.charAt(pos) == '+' || source.charAt(pos) == '-')) pos++;
            while (pos < length && source.charAt(pos) >= '0' && source.charAt(pos) <= '9') pos++;
        }
    }

    // ==================== Percent ====================

    private void scanPercent() {
        int start = pos;
        pos++;
        if (pos >= length) { emit(TokenType.INVALID, start, pos); return; }

        // %latest
        if (pos + 6 <= length && source.startsWith("latest", pos) && (pos + 6 >= length || !isIdentPart(source.charAt(pos + 6)))) {
            pos += 6; emit(TokenType.LATEST_DATE, start, pos); return;
        }

        char next = source.charAt(pos);
        // NEGATIVE-year date literal (%-799997984-02-29 — m3 pure's BC
        // dates, PCT adjust.pure): '%-' before a digit joins the date
        // token. Previously INVALID, so no existing lex is changed.
        if (next == '-' && pos + 1 < length
                && source.charAt(pos + 1) >= '0'
                && source.charAt(pos + 1) <= '9') {
            pos++;
            next = source.charAt(pos);
        }
        if (next >= '0' && next <= '9') {
            boolean hasDash = false;
            boolean hasColon = false;
            int scanPos = pos;
            while (scanPos < length) {
                char sc = source.charAt(scanPos);
                if (sc >= '0' && sc <= '9') { scanPos++; continue; }
                if (sc == '-') {
                    // Don't consume '->' (arrow operator)
                    if (scanPos + 1 < length && source.charAt(scanPos + 1) == '>') break;
                    hasDash = true; scanPos++; continue;
                }
                if (sc == ':') { hasColon = true; scanPos++; continue; }
                if (sc == 'T' || sc == '.') { scanPos++; continue; }
                if ((sc == '+') && scanPos > pos) { scanPos++; continue; }
                // 'Z' (UTC suffix) TERMINATES the literal — grammar
                // TimeZone: 'Z' | (+|-)DDDD. It was previously unlexable
                // (audit M5) though the date parser fully supports it.
                if (sc == 'Z' && (scanPos + 1 >= length
                        || !Character.isLetterOrDigit(source.charAt(scanPos + 1)))) {
                    scanPos++;
                    break;
                }
                break;
            }
            pos = scanPos;
            // (+/-)DDDD timezones are consumed by the scan loop above; the
            // old post-loop rescan was dead code (audit M5) and is gone.
            // StrictTime only if colons present but no dashes (e.g. %10:30:00)
            // Everything else (dates, datetimes, year-only, year-month) is DATE
            emit(hasColon && !hasDash ? TokenType.STRICTTIME : TokenType.DATE, start, pos);
            return;
        }

        // Standalone %
        pos = start + 1;
        emit(TokenType.INVALID, start, pos);
    }

    // ==================== Hash ====================

    private void scanHash() {
        int start = pos;
        // #TDS...#
        if (pos + 3 < length && source.startsWith("TDS", pos + 1)) {
            pos++;
            while (pos < length && source.charAt(pos) != '#'
                    && !atSectionBoundary(pos)) pos++;
            if (pos < length && source.charAt(pos) == '#') pos++;
            emit(TokenType.TDS_LITERAL, start, pos); return;
        }
        // #/Type/prop...# navigation path — one token; the parser desugars
        // simple property paths to their lambda meaning and is LOUD on the
        // richer path features (parameters, subtype casts).
        if (pos + 1 < length && source.charAt(pos + 1) == '/') {
            pos++;
            while (pos < length && source.charAt(pos) != '#'
                    && !atSectionBoundary(pos)) pos++;
            if (pos < length && source.charAt(pos) == '#') pos++;
            emit(TokenType.PATH_LITERAL, start, pos);
            return;
        }
        // #...{ → ISLAND_OPEN
        pos++;
        while (pos < length && source.charAt(pos) != '{' && source.charAt(pos) != '#') pos++;
        if (pos < length && source.charAt(pos) == '{') {
            pos++;
            emit(TokenType.ISLAND_OPEN, start, pos);
            islandDepth++;
            return;
        }
        emit(TokenType.INVALID, start, pos);
    }

    // ==================== Tilde ====================

    private void scanTilde() {
        int start = pos;
        if (pos + 1 < length && isIdentStart(source.charAt(pos + 1))) {
            int idEnd = pos + 1;
            while (idEnd < length && isIdentPart(source.charAt(idEnd))) idEnd++;
            String candidate = source.substring(start, idEnd);
            TokenType cmdType = TILDE_COMMANDS.get(candidate);
            if (cmdType != null) { pos = idEnd; emit(cmdType, start, pos); return; }
        }
        emit(TokenType.TILDE, pos, pos + 1); pos++;
    }

    // ==================== Minus / Arrow ====================

    private void scanMinusOrArrow() {
        if (pos + 1 < length && source.charAt(pos + 1) == '>') {
            emit(TokenType.ARROW, pos, pos + 2);
            pos += 2;
            return;
        }
        emit(TokenType.MINUS, pos, pos + 1); pos++;
    }

    // ==================== Brace Open ====================

    private void scanBraceOpen() {
        if (pos + 7 < length && source.startsWith("{target}", pos)) {
            emit(TokenType.TARGET, pos, pos + 8); pos += 8; return;
        }
        emit(TokenType.BRACE_OPEN, pos, pos + 1); pos++;
    }

    // ==================== File Name ====================

    private void scanFileName() {
        // ?[file.ext form is UNSUPPORTED: the span is INVALID, never a
        // silent disappearance (the first purge cut consumed and emitted
        // nothing).
        int start = pos;
        pos += 2;
        while (pos < length) {
            char c = source.charAt(pos);
            if (Character.isLetterOrDigit(c) || c == '_' || c == '.' || c == '/') pos++;
            else break;
        }
        emit(TokenType.INVALID, start, pos);
    }

    // ==================== Identifier / Keyword ====================

    private void scanIdentifierOrKeyword() {
        int start = pos;
        pos++;
        while (pos < length && isIdentPart(source.charAt(pos))) pos++;
        String text = source.substring(start, pos);

        // Multi-word composite tokens
        if (checkMultiWordToken(text, start)) return;

        // Keyword lookup
        TokenType kw = KEYWORDS.get(text);
        emit(kw != null ? kw : TokenType.VALID_STRING, start, pos);
    }

    private boolean checkMultiWordToken(String text, int start) {
        if ("PRIMARY".equals(text)) {
            int saved = pos; skipWhitespaceOnly();
            if (pos + 3 <= length && source.startsWith("KEY", pos)
                    && (pos + 3 >= length || !isIdentPart(source.charAt(pos + 3)))) {
                pos += 3; emit(TokenType.PRIMARY_KEY, start, pos); return true;
            }
            pos = saved;
        } else if ("NOT".equals(text)) {
            int saved = pos; skipWhitespaceOnly();
            if (pos + 4 <= length && source.startsWith("NULL", pos)
                    && (pos + 4 >= length || !isIdentPart(source.charAt(pos + 4)))) {
                pos += 4; emit(TokenType.NOT_NULL, start, pos); return true;
            }
            pos = saved;
        } else if ("is".equals(text)) {
            int saved = pos; skipWhitespaceOnly();
            if (pos + 3 <= length && source.startsWith("not", pos)
                    && (pos + 3 >= length || !isIdentPart(source.charAt(pos + 3)))) {
                pos += 3; skipWhitespaceOnly();
                if (pos + 4 <= length && source.startsWith("null", pos)
                        && (pos + 4 >= length || !isIdentPart(source.charAt(pos + 4)))) {
                    pos += 4; emit(TokenType.IS_NOT_NULL, start, pos); return true;
                }
                pos = saved;
            } else if (pos + 4 <= length && source.startsWith("null", pos)
                    && (pos + 4 >= length || !isIdentPart(source.charAt(pos + 4)))) {
                pos += 4; emit(TokenType.IS_NULL, start, pos); return true;
            } else {
                pos = saved;
            }
        }
        return false;
    }

    // ==================== Multi-Char Punctuation ====================

    private boolean scanMultiCharPunct(char c) {
        if (c == ':') {
            if (pos + 1 < length && source.charAt(pos + 1) == ':') {
                emit(TokenType.PATH_SEPARATOR, pos, pos + 2); pos += 2; return true;
            }
            emit(TokenType.COLON, pos, pos + 1); pos++; return true;
        }
        if (c == '.' && pos + 1 < length && source.charAt(pos + 1) == '.') {
            emit(TokenType.DOT_DOT, pos, pos + 2); pos += 2; return true;
        }
        if (c == '.' && pos + 1 < length
                && source.charAt(pos + 1) >= '0' && source.charAt(pos + 1) <= '9') {
            // bare-fraction literal (.5) — the numeric scanner's fractional branch
            // handles a zero-digit integer part
            scanNumericLiteral();
            return true;
        }
        if (c == '&' && pos + 1 < length && source.charAt(pos + 1) == '&') {
            emit(TokenType.AND, pos, pos + 2); pos += 2; return true;
        }
        if (c == '|' && pos + 1 < length && source.charAt(pos + 1) == '|') {
            emit(TokenType.OR, pos, pos + 2); pos += 2; return true;
        }
        if (c == '=' && pos + 1 < length && source.charAt(pos + 1) == '=') {
            emit(TokenType.TEST_EQUAL, pos, pos + 2); pos += 2; return true;
        }
        if (c == '!' && pos + 1 < length && source.charAt(pos + 1) == '=') {
            emit(TokenType.TEST_NOT_EQUAL, pos, pos + 2); pos += 2; return true;
        }
        if (c == '<') {
            if (pos + 1 < length) {
                char c2 = source.charAt(pos + 1);
                if (c2 == '=') { emit(TokenType.LESS_OR_EQUAL, pos, pos + 2); pos += 2; return true; }
                if (c2 == '>') { emit(TokenType.NOT_EQUAL, pos, pos + 2); pos += 2; return true; }
            }
            emit(TokenType.LESS_THAN, pos, pos + 1); pos++; return true;
        }
        if (c == '>') {
            if (pos + 1 < length && source.charAt(pos + 1) == '=') {
                emit(TokenType.GREATER_OR_EQUAL, pos, pos + 2); pos += 2; return true;
            }
            emit(TokenType.GREATER_THAN, pos, pos + 1); pos++; return true;
        }
        return false;
    }

    // ==================== Single-Char Punctuation ====================

    private void scanSingleCharPunct(char c) {
        TokenType type = switch (c) {
            case '{' -> { scanBraceOpen(); yield null; }
            case '}' -> TokenType.BRACE_CLOSE;
            case '(' -> TokenType.PAREN_OPEN;
            case ')' -> TokenType.PAREN_CLOSE;
            case '[' -> TokenType.BRACKET_OPEN;
            case ']' -> TokenType.BRACKET_CLOSE;
            case ',' -> TokenType.COMMA;
            case '=' -> TokenType.EQUAL;
            case ';' -> TokenType.SEMI_COLON;
            case '.' -> TokenType.DOT;
            case '$' -> TokenType.DOLLAR;
            case '^' -> TokenType.NEW_SYMBOL;
            case '|' -> TokenType.PIPE;
            case '@' -> TokenType.AT;
            case '+' -> TokenType.PLUS;
            case '*' -> TokenType.STAR;
            case '!' -> TokenType.NOT;
            case '?' -> TokenType.QUESTION;
            case '\u2286' -> TokenType.SUBSET; // ⊆
            default -> TokenType.INVALID;
        };
        if (type != null) {
            emit(type, pos, pos + 1);
            pos++;
        }
    }

    // ==================== Island Mode Scanning ====================

    private void scanIslandToken() {
        char c = source.charAt(pos);

        // }# — ISLAND_END
        if (c == '}' && pos + 1 < length && source.charAt(pos + 1) == '#') {
            emit(TokenType.ISLAND_END, pos, pos + 2); pos += 2; islandDepth--; return;
        }
        // } — ISLAND_BRACE_CLOSE
        if (c == '}') { emit(TokenType.ISLAND_BRACE_CLOSE, pos, pos + 1); pos++; return; }
        // #{ — ISLAND_START (nested island)
        if (c == '#' && pos + 1 < length && source.charAt(pos + 1) == '{') {
            emit(TokenType.ISLAND_START, pos, pos + 2); pos += 2; islandDepth++; return;
        }
        // # — ISLAND_HASH
        if (c == '#') { emit(TokenType.ISLAND_HASH, pos, pos + 1); pos++; return; }
        // { — ISLAND_BRACE_OPEN
        if (c == '{') { emit(TokenType.ISLAND_BRACE_OPEN, pos, pos + 1); pos++; return; }

        // Anything else — ISLAND_CONTENT (greedy: consume until {, }, or #)
        int start = pos;
        while (pos < length && !atSectionBoundary(pos)) {
            char ic = source.charAt(pos);
            if (ic == '{' || ic == '}' || ic == '#') break;
            pos++;
        }
        if (pos > start) {
            emit(TokenType.ISLAND_CONTENT, start, pos);
        }
    }
}
