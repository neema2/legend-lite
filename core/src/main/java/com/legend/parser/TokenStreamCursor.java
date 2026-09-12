package com.legend.parser;

import com.legend.protocol.Multiplicity;
import com.legend.protocol.TypeExpression;

import com.legend.lexer.TokenStream;
import com.legend.lexer.TokenType;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * A capability shared by every parser that walks a {@link TokenStream}.
 *
 * <p>Implementers provide a three-method window onto their cursor state
 * &mdash; {@link #tokens()}, {@link #pos()}, {@link #setPos(int)} &mdash;
 * and in exchange inherit:
 *
 * <ul>
 *   <li>The full Pure type-expression grammar: {@link #parseType()},
 *       {@link #parseTypeArgument()}, {@link #parseMultiplicity()}, plus
 *       function-type / relation-type / typed-parameter sub-grammars.</li>
 *   <li>A lexical layer of cursor primitives: {@link #peek()},
 *       {@link #peek(int)}, {@link #advance()}, {@link #atEnd()},
 *       {@link #match(TokenType)}, {@link #expect(TokenType)},
 *       {@link #consume(TokenType)}, {@link #text()},
 *       {@link #safeText()},
 *       {@link #error(String)}, plus the qualified-name and identifier
 *       readers that the type grammar (and the rest of the parser
 *       fleet) lean on.</li>
 * </ul>
 *
 * <h2>Design rationale</h2>
 *
 * <p>The previous design had three problems that this interface
 * collectively solves:
 *
 * <ul>
 *   <li><strong>Allocation churn.</strong> A standalone
 *       {@code TypeExpressionParser} helper was instantiated per type
 *       expression. A real parse builds hundreds; an interface default
 *       owns the grammar instead, allocating nothing.</li>
 *   <li><strong>Cursor bridging.</strong> Call sites read
 *       {@code helper = new ...(tokens, pos); ...; pos = helper.pos();}
 *       around every type parse. Default methods on a shared cursor
 *       eliminate the bridge.</li>
 *   <li><strong>Primitive duplication.</strong> {@link ElementParser}
 *       had its own private {@code peek}/{@code match}/{@code expect},
 *       the helper class had a parallel set, and {@link SpecParser}
 *       inlined the same predicates inline at every site. Promoting
 *       the primitives to public defaults gives one canonical
 *       definition, which {@link ElementParser} inherits and
 *       {@link SpecParser} can adopt incrementally.</li>
 * </ul>
 *
 * <p>An interface (rather than an abstract base class) is the right
 * shape: walking a token stream is a <em>capability</em>, not an
 * identity. Implementers retain their single-inheritance slot for
 * actual {@code is-a} relationships, and future grammar fragments
 * (signatures, generic bounds, schema algebra) can land as additional
 * interfaces that <em>extend</em> this one without forcing a class
 * hierarchy on the parsers.
 *
 * <h2>Past-end behaviour</h2>
 *
 * <p>{@link #peek()} and {@link #peek(int)} return {@link TokenType#EOF}
 * past the end of the stream, matching the convention used inside
 * {@link ElementParser}. This lets call sites compare to specific
 * tokens with {@code ==} unconditionally; no null guards required.
 *
 * <h2>Grammar covered</h2>
 *
 * <ul>
 *   <li>Function type: {@code {Type[mult], ... -> Type[mult]}}</li>
 *   <li>Relation type: {@code (col:Type[mult], ...)}</li>
 *   <li>Generic application: {@code my::pkg::List<arg, ...>} with
 *       schema-algebra operators ({@code +}, {@code -}, {@code \u2286},
 *       {@code =}) inside the argument list</li>
 *   <li>Plain name reference: {@code my::pkg::Type} or {@code T}</li>
 * </ul>
 *
 * <h2>Multiplicity grammar</h2>
 *
 * <ul>
 *   <li>Concrete: {@code [1]}, {@code [0..1]}, {@code [*]},
 *       {@code [1..*]}, {@code [3..7]}</li>
 *   <li>Parameter: {@code [m]} &mdash; refers to a multiplicity
 *       parameter declared in the enclosing function signature</li>
 * </ul>
 *
 * <p>Mirrors engine's split between {@code parsePType} (the
 * {@link #parseType()} entry, which does not consume schema-algebra
 * operators at the top level) and {@code parseTypeWithOperation}
 * (the {@link #parseTypeArgument()} entry used inside generic
 * argument lists).
 */
public interface TokenStreamCursor {

    // -----------------------------------------------------------------
    // Shared token-set: identifier-shaped tokens
    // -----------------------------------------------------------------

    /**
     * Every token type that can stand in for an identifier in Pure
     * source. Includes {@code VALID_STRING} (the bare identifier
     * production) and {@code STRING} (quoted-string-as-identifier in
     * keyword positions like profile names) plus every keyword that
     * Pure's grammar also admits as an identifier in some context.
     *
     * <p>Shared by every parser that walks Pure source: {@link ElementParser},
     * {@link SpecParser}, the IDE shallow scanner, and the inherited
     * type-expression / identifier defaults on this interface.
     */
    Set<TokenType> IDENTIFIER_TOKENS = java.util.Collections.unmodifiableSet(EnumSet.of(
            TokenType.VALID_STRING, TokenType.STRING,
            // M3
            TokenType.ALL, TokenType.LET, TokenType.ALL_VERSIONS, TokenType.ALL_VERSIONS_IN_RANGE,
            TokenType.IMPORT, TokenType.CLASS, TokenType.FUNCTION, TokenType.PROFILE,
            TokenType.ASSOCIATION, TokenType.ENUM, TokenType.EXTENDS,
            TokenType.STEREOTYPES, TokenType.TAGS, TokenType.NATIVE, TokenType.AS,
            // Mapping
            TokenType.MAPPING, TokenType.INCLUDE, TokenType.MAPPING_TESTABLE_SUITES, TokenType.MAPPING_TESTS_QUERY,
            // Runtime
            TokenType.RUNTIME, TokenType.SINGLE_CONNECTION_RUNTIME, TokenType.MAPPINGS,
            TokenType.CONNECTIONS, TokenType.CONNECTION, // Relational / Database
            TokenType.DATABASE, TokenType.TABLE, TokenType.SCHEMA, TokenType.VIEW,
            TokenType.FILTER, TokenType.MULTIGRAIN_FILTER, TokenType.JOIN,
            TokenType.RELATIONAL_AND, TokenType.RELATIONAL_OR,
            TokenType.ASSOCIATION_MAPPING, TokenType.ENUMERATION_MAPPING,
            TokenType.OTHERWISE, TokenType.INLINE, TokenType.PURE_MAPPING, TokenType.RELATIONAL,
            // Connection
            TokenType.STORE, TokenType.TYPE, TokenType.RELATIONAL_DATASOURCE_SPEC, TokenType.RELATIONAL_AUTH_STRATEGY,
            TokenType.H2, // Service
            TokenType.SERVICE, TokenType.SERVICE_PATTERN, TokenType.SERVICE_OWNERS,
            TokenType.SERVICE_DOCUMENTATION, TokenType.SERVICE_AUTO_ACTIVATE_UPDATES,
            TokenType.SERVICE_EXEC, TokenType.SERVICE_SINGLE, TokenType.SERVICE_MAPPING, TokenType.SERVICE_RUNTIME,
            // Additional. NOTE: TRUE/FALSE are NOT identifier tokens —
            // the engine's identifier rule excludes the boolean literals
            // in every section except Persistence (which has its own
            // parseQualifiedNameAdmittingBooleans); admitting them made
            // `Class a::A { true: String[1]; }` parse where the engine
            // refuses (adversarial fuzz, oracle-verified x6)
            TokenType.RELATIONAL_DATABASE_CONNECTION
    ));

    // -----------------------------------------------------------------
    // Required accessors. Implementers expose their cursor state.
    // -----------------------------------------------------------------

    /** Backing token stream. */
    TokenStream tokens();

    /** Current cursor position (index of the next token to consume). */
    int pos();

    /** Update the cursor position. Used by grammar defaults to advance
     *  past consumed tokens. */
    void setPos(int pos);

    // -----------------------------------------------------------------
    // Lexical primitives
    // -----------------------------------------------------------------

    /** Type of the token under the cursor; {@link TokenType#EOF} past end. */
    default TokenType peek() {
        return pos() < tokens().count() ? tokens().type(pos()) : TokenType.EOF;
    }

    /** Type of the token {@code offset} positions past the cursor;
     *  {@link TokenType#EOF} past end. */
    default TokenType peek(int offset) {
        int idx = pos() + offset;
        return idx < tokens().count() ? tokens().type(idx) : TokenType.EOF;
    }

    /** Source text of the token under the cursor. Caller must ensure
     *  {@code !atEnd()}; use {@link #safeText()} for diagnostic
     *  contexts where past-end is possible. */
    default String text() {
        return tokens().text(pos());
    }

    /** Which of the THREE dialect levels this parse serves
     *  ({@link Dialect}). ABSTRACT on purpose: there is no default level.
     *  Every cursor names its dialect at construction — island re-lex
     *  cursors inherit the HOST's — so "something quietly parses at
     *  platform level" is unrepresentable (HONEST_DEBT #9, the
     *  collapse). */
    Dialect dialect();

    /** Token texts {@code [fromTok, toTok]} joined with NO separators —
     *  the same rendering as ANTLR's {@code ctx.getText()}, which the
     *  engine uses to compose its refusal messages. */
    default String compactText(int fromTok, int toTok) {
        StringBuilder sb = new StringBuilder();
        for (int i = fromTok; i <= toTok && i < tokens().count(); i++) {
            sb.append(tokens().text(i));
        }
        return sb.toString();
    }

    /** Source text of the token under the cursor, or {@code "<EOF>"}
     *  if the cursor is past end. Used in error messages. */
    default String safeText() {
        return pos() < tokens().count() ? text() : "<EOF>";
    }

    /** Source text of the token {@code offset} past the cursor, or {@code ""}
     *  past end — an empty string matches NO keyword, so lookahead equality
     *  tests are EOF-safe (adversarial audit F10: the old
     *  {@code text(Math.min(pos+1, count-1))} clamp re-read the LAST token
     *  and could false-match at end of stream). */
    default String peekText(int offset) {
        int idx = pos() + offset;
        return idx >= 0 && idx < tokens().count() ? tokens().text(idx) : "";
    }

    /** Advance the cursor by one token. */
    default void advance() {
        rejectInvalid();   // unlexable input dies HERE, not three phases later
        setPos(pos() + 1);
    }

    /** Whether the cursor is past the last token. */
    /** True when the parenthesis group at {@code pos()} is immediately followed by
     * another {@code (} — the {@code ^X(values)(bindings)} instantiation form. */
    default boolean parenGroupFollowedByParen() {
        int depth = 0;
        for (int i = pos(); i < tokens().count(); i++) {
            TokenType t = tokens().type(i);
            if (t == TokenType.PAREN_OPEN) {
                depth++;
            } else if (t == TokenType.PAREN_CLOSE) {
                depth--;
                if (depth == 0) {
                    return i + 1 < tokens().count() && tokens().type(i + 1) == TokenType.PAREN_OPEN;
                }
            }
        }
        return false;
    }

    default boolean atEnd() {
        return pos() >= tokens().count();
    }

    /** Consume the next token if it matches {@code type}; return whether
     *  it did. */
    default boolean match(TokenType type) {
        if (peek() == type) {
            advance();
            return true;
        }
        return false;
    }

    /** Require the next token to be {@code type} and advance past it;
     *  fail with a source-located {@link ParseException} otherwise. */
    default void expect(TokenType type) {
        // INVALID rejection lives in the FAILURE branch only — the happy
        // path pays nothing extra (advance() below carries the real trap).
        if (peek() != type) {
            rejectInvalid();
            throw error("expected " + type + " but found " + peek()
                    + " ('" + safeText() + "')");
        }
        advance();
    }

    /**
     * Like {@link #expect(TokenType)} but uses {@code customMessage}
     * verbatim on failure instead of the generic
     * {@code "expected X but found ..."} template. Compresses the
     * inlined {@code if (peek()!=X) throw error("..."); advance();}
     * pattern that hand-rolled bespoke error messages at ~30 sites in
     * {@link SpecParser}.
     */
    default void expect(TokenType type, String customMessage) {
        if (peek() != type) {
            throw error(customMessage);
        }
        advance();
    }

    /** Require an INTEGER at the cursor and return its {@code long} value.
     *  A non-integer token or an overflowing literal becomes a POSITIONED
     *  refusal — never a raw {@code NumberFormatException} (adversarial
     *  audit: six sites parsed BEFORE type-checking and crashed unlocated
     *  on {@code port: xyz;}). */
    /** THE once-only policy for {@code key: value;} body loops — the
     *  engine's walkers route every named field through
     *  {@code validateAndExtractRequiredField}/{@code optionalField},
     *  which refuse a duplicate with exactly this message. Enforced in
     *  ~4 of 23 grammars before the 2026-08-12 adversarial audit
     *  (oracle-verified holes: DataSpace title/description, Connection
     *  class/type, Runtime mappings, Profile tags); every key-dispatch
     *  loop calls this FIRST so the policy has one owner. */
    static void once(java.util.Set<String> seen, String key,
            TokenStreamCursor at) {
        if (!seen.add(key)) {
            throw at.error("Field '" + key + "' should be specified only once");
        }
    }

    /** {@link #once} ANCHORED at the containing block's start token — the
     *  engine's walkers pass each definition ctx to validateAndExtract, so
     *  a duplicate-field error reports at the BLOCK, not the cursor
     *  (position-exactness lane; anchored at the containing block). */
    static void once(java.util.Set<String> seen, String key,
            TokenStreamCursor at, int anchorTok) {
        if (!seen.add(key)) {
            throw throwAt(at.tokens(), anchorTok,
                    "Field '" + key + "' should be specified only once");
        }
    }

    /** Require a STRING literal at the cursor, return its decoded body,
     *  advance. Positioned refusal on any other token — the old
     *  decode-before-check pattern died with "malformed quoted name" (or a
     *  raw IOOBE at EOF) on {@code doc: 5;} (adversarial audit F15). */
    default String consumeStringLiteral(String what) {
        if (atEnd() || peek() != TokenType.STRING) {
            rejectInvalid();
            throw error("expected a string literal for " + what
                    + ", got " + peek());
        }
        String v = unquoteAndUnescape(text(), this);
        advance();
        return v;
    }

    /** {@link #consumeLong()} narrowed to {@code int} range, with a
     *  positioned out-of-range refusal naming {@code what}. */
    default int consumeBoundedInt(String what) {
        int mark = pos();
        long v = consumeLong();
        if (v > Integer.MAX_VALUE) {
            throw throwAt(tokens(), mark,
                    what + " out of range: " + v);
        }
        return (int) v;
    }

    default long consumeLong() {
        if (peek() != TokenType.INTEGER) {
            rejectInvalid();
            throw error("expected " + TokenType.INTEGER + " but found " + peek()
                    + " ('" + safeText() + "')");
        }
        String t = text();
        try {
            long v = Long.parseLong(t);
            advance();
            return v;
        } catch (NumberFormatException overflow) {
            throw error("integer literal out of range: '" + t + "'");
        }
    }

    /** Require the next token to be {@code type}, advance past it, and
     *  return its source text. */
    default String consume(TokenType type) {
        if (peek() != type) {
            rejectInvalid();   // failure branch only — see expect()
            throw error("expected " + type + " but found " + peek()
                    + " ('" + safeText() + "')");
        }
        String t = text();
        advance();
        return t;
    }

    // -----------------------------------------------------------------
    // Source-located error construction
    // -----------------------------------------------------------------

    /**
     * Build and throw a {@link ParseException} reporting the offending
     * token's 1-indexed line and column derived from the source string.
     * Declared to return {@link RuntimeException} so callers can write
     * {@code throw throwAt(tokens, pos, msg)} &mdash; that pattern lets
     * the Java compiler see that control does not return, eliminating
     * the {@code return null; // unreachable} boilerplate that an
     * earlier {@code void}-returning version forced everywhere.
     *
     * <p>The method <em>always throws</em>; the return type exists
     * solely for the {@code throw}-expression idiom. Callers that need
     * a fluent failure in a switch arm or ternary still see the
     * exception propagate.
     *
     * <p>Static (rather than a {@code default}) because code outside an
     * instance &mdash; the IDE layer's {@code ModelIndexer},
     * top-of-pipeline error reporters, the date / time literal recovery
     * path in {@link SpecParser} &mdash; also needs to attach a
     * source-located message and has a {@link TokenStream} but no
     * parser instance. {@link #error(String)} below covers the instance
     * case.
     *
     * @param tokens   the token stream the position refers to
     * @param tokenPos token index whose start offset becomes the error
     *                 point; if at or past the end of the stream the
     *                 error is reported at end-of-input
     * @param message  human-readable message; no location suffix
     *                 needed (the exception carries line/column
     *                 separately)
     */
    static RuntimeException throwAt(TokenStream tokens, int tokenPos, String message) {
        int n = tokens.count();
        int charPos;
        if (tokenPos < n) {
            charPos = tokens.start(tokenPos);
        } else if (n > 0) {
            charPos = tokens.end(n - 1);
        } else {
            throw new ParseException(message);
        }
        // the stream's cached line index: 1-based line AND column, same as the
        // engine's ANTLR positions (audit §3.5: this was the only renderer
        // still 0-based and the only one still rescanning the source)
        throw new ParseException(message, tokens.lineOf(charPos), tokens.columnOf(charPos));
    }

    /**
     * Build a {@link ParseException} located at the cursor.
     *
     * <p>Always throws; the return type exists so callers can write
     * {@code throw error("msg")}, which lets the compiler see that
     * control does not fall through. The earlier {@code void}
     * signature forced {@code return null; // unreachable} all over
     * the parser fleet.
     */
    default RuntimeException error(String message) {
        throw throwAt(tokens(), pos(), message);
    }

    // -----------------------------------------------------------------
    // Identifier / qualified-name readers
    // -----------------------------------------------------------------

    /** Whether {@code t} can stand in for an identifier in element /
     *  type contexts (per {@link #IDENTIFIER_TOKENS}). Permits
     *  {@code STRING} (quoted strings) where Pure's grammar treats
     *  quoted strings as keyword-position identifiers (profile names,
     *  tagged-value names).
     *
     *  <p>Default method so implementing classes can call
     *  {@code isIdentifierToken(t)} unqualified. Code outside the
     *  cursor hierarchy (e.g. the IDE shallow scanner) checks
     *  {@code IDENTIFIER_TOKENS.contains(t)} directly. */
    /**
     * Whether the class-mapping body at the cursor is the CLEAN-SHEET
     * function form rather than engine's legacy declarative DSL
     * (CLEAN_SHEET_INVERSION §5.1). A legacy body opens with a
     * {@code ~directive} or a {@code prop:} property mapping; anything else
     * is a Pure expression — a function reference or an inline body.
     *
     * <p>Lives HERE, on the cursor, because BOTH mapping parsers need the
     * same answer: {@code MappingProtocolParser} to pick the protocol shape
     * and {@code MappingGrammarParser} to pick the model shape. Duplicating
     * the disambiguator while deleting a duplicate parser would just move
     * the drift down a level — the failure mode PARSER_COMPLETENESS_PLAN.md
     * §1 exists to end. It dies with the legacy mapping parser.
     */
    default boolean isCleanSheetBody() {
        if (peek() == TokenType.BRACE_CLOSE) {
            return false;   // {} — empty LEGACY body (extends inherits all)
        }
        if (isLegacyMappingCommand(peek())) {
            return false;   // ~mainTable / ~filter / ~src / ...
        }
        if (isIdentifierToken(peek()) && peek(1) == TokenType.COLON) {
            return false;   // prop: legacy property mapping
        }
        if (isIdentifierToken(peek()) && "scope".equals(safeText())
                && peek(1) == TokenType.PAREN_OPEN) {
            return false;   // scope([db]...)( legacy PMs )
        }
        if (isIdentifierToken(peek()) && peek(1) == TokenType.PAREN_OPEN) {
            return false;   // prop( embedded )
        }
        if (isIdentifierToken(peek()) && peek(1) == TokenType.BRACKET_OPEN) {
            return false;   // prop[setId]: / prop[setId](
        }
        return peek() != TokenType.PLUS;   // +localProp:
    }

    /** The {@code ~directive} keywords that open a legacy mapping body. */
    static boolean isLegacyMappingCommand(TokenType t) {
        return t == TokenType.MAIN_TABLE_CMD
            || t == TokenType.FILTER_CMD
            || t == TokenType.DISTINCT_CMD
            || t == TokenType.GROUP_BY_CMD
            || t == TokenType.PRIMARY_KEY_CMD
            || t == TokenType.SRC_CMD;
    }

    /**
     * Consume one balanced {@code {...}}, {@code (...)} or {@code [...]}
     * block, balancing every bracket kind (the lexer already skipped
     * strings). Used wherever a section body is carried verbatim rather
     * than modelled.
     *
     * <p>Lived on {@code MappingGrammarParser} until that parser was
     * deleted; it never had anything to do with mappings.
     */
    default void skipBalancedBlock() {
        int depth = 0;
        boolean started = false;
        while (!atEnd()) {
            TokenType t = peek();
            if (t == TokenType.BRACE_OPEN || t == TokenType.PAREN_OPEN
                    || t == TokenType.BRACKET_OPEN) {
                depth++;
                started = true;
            } else if (t == TokenType.BRACE_CLOSE || t == TokenType.PAREN_CLOSE
                    || t == TokenType.BRACKET_CLOSE) {
                depth--;
            }
            advance();
            if (started && depth == 0) {
                return;
            }
        }
    }

    default boolean isIdentifierToken(TokenType t) {
        return t != null && IDENTIFIER_TOKENS.contains(t);
    }

    /** Whether {@code t} can stand as a segment of a qualified name
     *  ({@code a::b::c}). Stricter than {@link #isIdentifierToken}:
     *  quoted strings are <em>not</em> admissible as FQN segments &mdash;
     *  {@code foo::'bar'::baz} is not legal Pure in any position. */
    /** The index of the {@code ~} that ends a unit path ({@code RomanLength~Pes})
     * starting at token {@code i}, or -1 when no unit path starts there. */
    default int unitPathEnd(int i) {
        boolean sawName = false;
        while (i < tokens().count() && isFqnSegmentToken(tokens().type(i))) {
            sawName = true;
            i++;
            if (i < tokens().count() && tokens().type(i) == TokenType.PATH_SEPARATOR) {
                i++;
                continue;
            }
            break;
        }
        return sawName && i < tokens().count() && tokens().type(i) == TokenType.TILDE ? i : -1;
    }

    default boolean isFqnSegmentToken(TokenType t) {
        // the engine's identifier rule admits many keywords but NOT the boolean
        // literals (they left IDENTIFIER_TOKENS 2026-08-12) or STRING
        return t != null && t != TokenType.STRING
                && IDENTIFIER_TOKENS.contains(t);
    }

    /**
     * Parse a possibly-qualified name: {@code Foo} or
     * {@code my::pkg::Foo}. Consumes the dotted-path tokens; single
     * identifiers (e.g. type parameters {@code T}) are admissible.
     *
     * <p>Segments use {@link #isFqnSegmentToken(TokenType)}, which
     * excludes {@code STRING}. A quoted string can be an identifier in
     * a few keyword positions, but never a segment of an FQN.
     */
    default String parseQualifiedName() {
        if (!isFqnSegmentToken(peek()) && peek() != TokenType.STRING
                && !(peek() == TokenType.INTEGER && intLeadsIdentifier())) {
            throw error("expected type name, got " + peek());
        }
        StringBuilder sb = new StringBuilder(fqnSegmentText());
        while (peek() == TokenType.PATH_SEPARATOR) {
            advance();
            sb.append("::").append(fqnSegmentText());
        }
        return sb.toString();
    }

    /** Persistence-dialect qualified name: that grammar's identifier rule
     *  ALSO admits {@code TRUE|FALSE} as segments (its g4 lists every
     *  keyword including the boolean literals), unlike ###Pure where
     *  {@code Class false::me} is a parse error. */
    default String parseQualifiedNameAdmittingBooleans() {
        StringBuilder sb = new StringBuilder(boolOrFqnSegmentText());
        while (peek() == TokenType.PATH_SEPARATOR) {
            advance();
            sb.append("::").append(boolOrFqnSegmentText());
        }
        return sb.toString();
    }

    default String boolOrFqnSegmentText() {
        if (peek() == TokenType.TRUE || peek() == TokenType.FALSE) {
            String seg = text();
            advance();
            return seg;
        }
        return fqnSegmentText();
    }

    /**
     * One FQN segment: a plain identifier, a QUOTED name ({@code test::'p a c k'::A} —
     * unescaped, engine emits it raw), or a DIGIT-LEADING run ({@code pkg::2_0_0::A} —
     * lexes as INTEGER + adjacent identifier pieces, glued back; engine's VALID_STRING
     * admits digits first, ours cannot without colliding with number literals).
     */
    /** A lone INTEGER is a literal; only an INTEGER glued to an ADJACENT identifier
     *  piece (4prop, 2_0_0) reads as a digit-leading name. */
    default boolean intLeadsIdentifier() {
        return pos() + 1 < tokens().count()
                && tokens().type(pos() + 1) == TokenType.VALID_STRING
                && tokens().end(pos()) == tokens().start(pos() + 1);
    }

    default String fqnSegmentText() {
        if (peek() == TokenType.STRING) {
            // a REFERENCE keeps the raw quoted spelling ('2000 Integer' as a column
            // type); DECLARATION names unquote in Protocol.splitFqn
            String seg = text();
            advance();
            return seg;
        }
        if (peek() == TokenType.INTEGER && intLeadsIdentifier()) {
            StringBuilder seg = new StringBuilder(text());
            advance();
            TokenStream ts = tokens();
            while (!atEnd()
                    && (peek() == TokenType.VALID_STRING || peek() == TokenType.INTEGER)
                    && ts.end(pos() - 1) == ts.start(pos())) {
                seg.append(text());
                advance();
            }
            return seg.toString();
        }
        if (!isFqnSegmentToken(peek())) {
            throw error("expected identifier after '::' in qualified name");
        }
        String seg = text();
        advance();
        return seg;
    }

    /** Single identifier (no path). Accepts any token in
     *  {@link #IDENTIFIER_TOKENS}. */
    /**
     * THE lexer-error trap: {@link TokenType#INVALID} marks unlexable input
     * and must never flow silently into a parse (it used to — audit). Called
     * directly by {@code advance()}, {@code expect}, and {@code consume}.
     */
    default void rejectInvalid() {
        if (peek() == TokenType.INVALID) {
            throw error("unlexable input: '" + safeText() + "'");
        }
    }

    default String parseIdentifier() {
        // digit-leading names (4prop) lex as INTEGER + adjacent identifier pieces —
        // glue like FQN segments (inline-snippet corpus)
        if (peek() == TokenType.INTEGER && intLeadsIdentifier()) {
            return fqnSegmentText();
        }
        if (!isIdentifierToken(peek())) {
            throw error("expected identifier, got " + peek());
        }
        // A QUOTED identifier ('my prop') is admitted by IDENTIFIER_TOKENS;
        // its NAME is the unquoted, unescaped text — the declared name and
        // every use site must agree (audit M10: they previously disagreed,
        // e.g. let 'my var' vs $'my var').
        String name = peek() == TokenType.STRING
                ? unquoteAndUnescape(text(), this)
                : text();
        advance();
        return name;
    }

    /**
     * THE quoted-name decoder: strip surrounding single quotes and resolve
     * escapes. One implementation for every identifier-ish position across
     * both parsers (audit M11 found EIGHT copies, half of which forgot the
     * escapes).
     */
    static String unquoteAndUnescape(String raw, TokenStreamCursor at) {
        if (raw.length() < 2 || raw.charAt(0) != '\'' || raw.charAt(raw.length() - 1) != '\'') {
            throw at.error("malformed quoted name: missing surrounding quotes");
        }
        return unescapeBody(raw.substring(1, raw.length() - 1), "quoted name", at);
    }

    /**
     * THE escape table — quoted names AND string literals decode the same
     * token the same way (audit §2.5: the earlier de-duplication kept the
     * WEAKER 5-escape table for names, so {@code 'x\by'} was a value in
     * expression position and a parse error in name position). Real pure's
     * set (M4Fragment.g4 EscSeq): {@code [btnfr"'\\]}; an UNRECOGNIZED escape
     * drops the backslash and keeps the char (legend-pure
     * StringEscape.UNESCAPE_PURE's terminal rule — the corpus's {@code '\ '}
     * seed literal depends on it). Octal and {@code \\uXXXX} DECODE — the
     * oracle's decoder is commons-text {@code unescapeJava}
     * (PureGrammarParserUtility.fromGrammarString), which handles both;
     * refusing them was an invented divergence (adversarial-audit fuzz).
     */
    static String unescapeBody(String body, String what, TokenStreamCursor at) {
        if (body.indexOf('\\') < 0) {
            return body;
        }
        // a backslash escaping END-OF-BODY stays a loud, located error (it
        // cannot come from a well-lexed literal; reaching it means a scanner
        // upstream mangled the extent)
        int i = 0;
        while (i < body.length()) {
            i += body.charAt(i) == '\\' ? 2 : 1;
        }
        if (i > body.length()) {
            throw at.error("malformed " + what + ": trailing backslash");
        }
        // the oracle decodes via unescapeJava — octal and \\uXXXX INCLUDED
        // (the old table refused them "until a corpus file demands them";
        // the engine accepts them, so refusing was an invented divergence —
        // adversarial-audit fuzz row 'a\\u0041b')
        try {
            return com.legend.protocol.Escapes.unescapeJavaLike(body);
        } catch (IllegalArgumentException e) {
            throw at.error("malformed " + what + ": " + e.getMessage());
        }
    }

    /** THE double-quoted-identifier stripper (SQL-ish spellings in Relational
     *  and Mapping positions) — no escapes, quotes-off if present. One copy;
     *  the two per-parser twins it replaces were byte-identical (audit §2.5). */
    static String stripDoubleQuotes(String n) {
        return n.length() > 1 && n.startsWith("\"") && n.endsWith("\"")
                ? n.substring(1, n.length() - 1) : n;
    }

    // -----------------------------------------------------------------
    // Type-expression grammar entry points
    // -----------------------------------------------------------------

    /**
     * Parse a single type expression at the current cursor position.
     * Dispatches by leading token:
     * <ul>
     *   <li>{@code {} &rarr; function type</li>
     *   <li>{@code (} &rarr; relation type</li>
     *   <li>identifier (qualified or simple) &rarr; name ref, possibly
     *       followed by {@code <arg, ...>} for generic application</li>
     * </ul>
     */
    default TypeExpression parseType() {
        if (peek() == TokenType.BRACE_OPEN) {
            return parseFunctionType();
        }
        if (peek() == TokenType.PAREN_OPEN) {
            return parseRelationType();
        }
        int startTok = pos();
        String name = parseQualifiedName();
        // Measure~Unit (Mass~Kilogram[1], cast(@Mass~Kilogram)): the
        // UNIT type spelling — folded into ONE NameRef carrying the
        // tilde; the classifier walls it as an unported platform type
        // until the units feature lands (parse-level coverage only)
        if (peek() == TokenType.TILDE && isFqnSegmentToken(peek(1))) {
            advance();
            name = name + "~" + text();
            advance();
        }
        // precise primitives with TYPE-VARIABLE VALUES — Varchar(200), V('ok'),
        // Numeric(10, 2): the wire's typeVariableValues; the rawType span covers the
        // whole application (ProbeWireShapes "agg kind and varchar", "pf string tvv")
        if (peek() == TokenType.PAREN_OPEN) {
            List<com.legend.protocol.spec.ValueSpecification> tvv = parseTypeVariableValues();
            return new TypeExpression.Generic(name, List.of(), List.of(), tvv,
                    spanOf(startTok, pos() - 1));
        }
        if (!match(TokenType.LESS_THAN)) {
            return new TypeExpression.NameRef(name, spanOf(startTok, pos() - 1));
        }
        List<TypeExpression> args = new ArrayList<>();
        // TestClass<|1>: multiplicity arguments with NO type arguments
        if (peek() == TokenType.PIPE) {
            advance();
            List<String> multArgs = new ArrayList<>();
            multArgs.add(parseMultiplicityArgumentText());
            while (match(TokenType.COMMA)) {
                multArgs.add(parseMultiplicityArgumentText());
            }
            expect(TokenType.GREATER_THAN);
            return new TypeExpression.Generic(name, args, multArgs,
                    spanOf(startTok, pos() - 1));
        }
        args.add(parseTypeArgument());
        while (match(TokenType.COMMA)) {
            args.add(parseTypeArgument());
        }
        // MULTIPLICITY type parameters (real M3: Result<T|m>,
        // Result<TabularDataSet|1>): '|' separates the type arguments
        // from the multiplicity arguments
        List<String> multArgs = new ArrayList<>();
        if (match(TokenType.PIPE)) {
            multArgs.add(parseMultiplicityArgumentText());
            while (match(TokenType.COMMA)) {
                multArgs.add(parseMultiplicityArgumentText());
            }
        }
        expect(TokenType.GREATER_THAN);
        // Res<String>(1, 'a'): type-variable VALUES may follow the angle brackets too
        List<com.legend.protocol.spec.ValueSpecification> tvv =
                !atEnd() && peek() == TokenType.PAREN_OPEN
                        ? parseTypeVariableValues() : List.of();
        // Engine convention (verified via ProbeWireShapes): a generic's rawType span
        // covers the WHOLE application incl. the closing '>'; each argument carries
        // its own span on its own node.
        return new TypeExpression.Generic(name, args, multArgs, tvv,
                spanOf(startTok, pos() - 1));
    }

    /** {@code (200)}, {@code (10, 2)}, {@code ('ok')} — integer or string literals. */
    default List<com.legend.protocol.spec.ValueSpecification> parseTypeVariableValues() {
        advance();                                  // past '('
        List<com.legend.protocol.spec.ValueSpecification> tvv = new ArrayList<>();
        while (!atEnd() && peek() != TokenType.PAREN_CLOSE) {
            if (peek() == TokenType.COMMA) {
                advance();
                continue;
            }
            if (peek() == TokenType.INTEGER) {
                tvv.add(new com.legend.protocol.spec.CInteger(
                        Long.parseLong(text()), spanOf(pos(), pos())));
            } else if (peek() == TokenType.STRING) {
                tvv.add(new com.legend.protocol.spec.CString(
                        unquoteAndUnescape(text(), this), spanOf(pos(), pos())));
            } else {
                throw error("type variable values support integer and string literals,"
                        + " got " + peek());
            }
            advance();
        }
        expect(TokenType.PAREN_CLOSE);
        return tvv;
    }

    /** A {@link com.legend.protocol.SourceInfo} covering an inclusive token range,
     *  in the engine's 1-based / inclusive-end convention. */
    record Decorations(java.util.List<com.legend.protocol.Protocol.PStereotype> stereotypes,
            java.util.List<com.legend.protocol.Protocol.PTaggedValue> taggedValues) {
    }

    /** {@code <<p::P.v, ...>>} and/or {@code {p::P.t = 'v', ...}} before a
     *  schema/table/view name — VALUE-only stereotype spans (probe
     *  decorated-schema-table). */
    default Decorations parseDecorations() {
        java.util.List<com.legend.protocol.Protocol.PStereotype> stereos = new java.util.ArrayList<>();
        java.util.List<com.legend.protocol.Protocol.PTaggedValue> tags = new java.util.ArrayList<>();
        if (peek() == TokenType.LESS_THAN) {
            advance();
            expect(TokenType.LESS_THAN);
            while (!atEnd() && peek() != TokenType.GREATER_THAN) {
                int pS = pos();
                String profile = com.legend.protocol.Protocol.unquotePath(parseQualifiedName());
                com.legend.protocol.SourceInfo pSpan = spanOf(pS, pos() - 1);
                expect(TokenType.DOT);
                int vS = pos();
                String value = parseIdentifier();
                stereos.add(new com.legend.protocol.Protocol.PStereotype(profile, value, pSpan,
                        spanOf(vS, vS)));
                // engine requires the comma BETWEEN entries (its message:
                // Valid alternatives: [',', '>'] — the optional match here
                // accepted <<p.a p.b>>, adversarial audit finding 12)
                if (peek() != TokenType.GREATER_THAN) {
                    expect(TokenType.COMMA);
                }
            }
            expect(TokenType.GREATER_THAN);
            expect(TokenType.GREATER_THAN);
        }
        if (peek() == TokenType.BRACE_OPEN) {
            advance();
            while (!atEnd() && peek() != TokenType.BRACE_CLOSE) {
                int tS = pos();
                String profile = com.legend.protocol.Protocol.unquotePath(parseQualifiedName());
                com.legend.protocol.SourceInfo pSpan = spanOf(tS, pos() - 1);
                expect(TokenType.DOT);
                int vS = pos();
                String tagName = parseIdentifier();
                expect(TokenType.EQUAL);
                String value;
                boolean multiLine;
                com.legend.protocol.SourceInfo tvSpan;
                if (peek() == TokenType.DOC_STRING) {
                    if (dialect().refusesPlatformDialect()
                            && text().indexOf('\n') < 0) {
                        // single-line ''' — the oracle's lexer splits it into
                        // adjacent strings and refuses (probed live 2026-08-12)
                        throw error("Unexpected token '" + text() + "'");
                    }
                    // '''...''' tagged-value VALUE (4.138, ZMissedRowsProbe):
                    // shared strip rule; the tv span ends by the token's
                    // single-line column arithmetic
                    int dTok = pos();
                    String raw = text();
                    value = docStringValue(raw);
                    multiLine = isTextBlock(raw);
                    advance();
                    com.legend.protocol.SourceInfo d = docStringSpan(dTok);
                    com.legend.protocol.SourceInfo s = spanOf(tS, tS);
                    tvSpan = new com.legend.protocol.SourceInfo(
                            s.sourceId(), s.startLine(), s.startColumn(),
                            d.endLine(), d.endColumn());
                } else {
                    String quoted = text();
                    expect(TokenType.STRING);
                    value = unquoteAndUnescape(quoted, this);
                    multiLine = false;
                    tvSpan = spanOf(tS, pos() - 1);
                }
                tags.add(new com.legend.protocol.Protocol.PTaggedValue(
                        new com.legend.protocol.Protocol.PTag(profile, tagName, pSpan,
                                spanOf(vS, vS)),
                        value, multiLine, tvSpan));
                if (peek() != TokenType.BRACE_CLOSE) {
                    expect(TokenType.COMMA);          // same engine rule as stereotypes
                }
            }
            expect(TokenType.BRACE_CLOSE);
        }
        return new Decorations(stereos, tags);
    }

    /** A function DESCRIPTOR's extent — {@code fqn}, optional balanced
     *  {@code (args)}, optional {@code :Type<...>[m]} signature suffix.
     *  {@code nameEnd}/{@code end} are EXCLUSIVE token indexes. */
    record FunctionDescriptor(int start, int nameEnd, int end) {
    }

    /**
     * ONE scan for every function-pointer site (Relation {@code ~func}/
     * {@code ~src}, activator {@code function:}), so the descriptor grammar
     * cannot drift per site. Each site renders the text by its own wire
     * rule ({@link #compactText} canonical join vs {@link #reconstructText}
     * raw spelling) — the shared part is the EXTENT, not the rendering.
     */
    default FunctionDescriptor parseFunctionDescriptor() {
        int start = pos();
        parseQualifiedName();
        int nameEnd = pos();
        if (peek() == TokenType.PAREN_OPEN) {
            int parens = 0;
            do {
                if (peek() == TokenType.PAREN_OPEN) {
                    parens++;
                } else if (peek() == TokenType.PAREN_CLOSE) {
                    parens--;
                }
                advance();
            } while (!atEnd() && parens > 0);
            if (match(TokenType.COLON)) {
                parseQualifiedName();
                skipTypeArgsAndMultiplicity();
            }
        }
        return new FunctionDescriptor(start, nameEnd, pos());
    }

    /** Consume {@code <...>} type arguments and a {@code [..]} multiplicity,
     *  if present — the tail of a signature spelling. */
    default void skipTypeArgsAndMultiplicity() {
        if (peek() == TokenType.LESS_THAN) {
            int depth = 0;
            while (!atEnd()) {
                if (peek() == TokenType.LESS_THAN) {
                    depth++;
                } else if (peek() == TokenType.GREATER_THAN) {
                    depth--;
                    if (depth == 0) {
                        advance();
                        break;
                    }
                }
                advance();
            }
        }
        if (peek() == TokenType.BRACKET_OPEN) {
            while (!atEnd() && peek() != TokenType.BRACKET_CLOSE) {
                advance();
            }
            match(TokenType.BRACKET_CLOSE);
        }
    }

    /**
     * The logical VALUE of a {@code '''...'''} literal — a VERBATIM port of
     * the pinned oracle's {@code PureGrammarParserUtility.processTextBlock}
     * (4.138.2 sources jar; the earlier reverse-engineered version missed
     * four of its rules — CRLF normalization, min-indent over ALL non-blank
     * lines, per-line trailing-whitespace strip, and the unescape —
     * deep-audit 1f, re-verified differentially): normalize line
     * terminators, drop the opening delimiter LINE and the closing
     * {@code '''}, strip the minimum indent measured over every non-blank
     * line plus the last line even when blank, strip each line's trailing
     * whitespace, then Java-unescape.
     */
    static String docStringValue(String raw) {
        return com.legend.protocol.Escapes.unescapeJavaLike(textBlockLayout(raw));
    }

    /** The engine's {@code isTextBlock}: a {@code '''} literal whose opening
     *  delimiter is followed — after optional spaces or tabs — by a line
     *  terminator, so the first content line is the line after it.
     *  {@code '''abc'''} and {@code ''''''} are deliberately NOT blocks. */
    static boolean isTextBlock(String raw) {
        if (raw.length() < 7 || !raw.startsWith("'''") || !raw.endsWith("'''")) {
            return false;
        }
        int i = 3;
        while (i < raw.length() && (raw.charAt(i) == ' ' || raw.charAt(i) == '\t')) {
            i++;
        }
        return i < raw.length() && (raw.charAt(i) == '\n' || raw.charAt(i) == '\r');
    }

    /** The engine's {@code canonicalizeDocumentation} (4.145.0,
     *  {@code PureGrammarParserUtility}; identical to legend-pure's
     *  {@code DocumentationCanonicalizer}): the text-block LAYOUT with NO
     *  escape processing — documentation is prose, and unescaping it would
     *  rewrite a regex, a Markdown escape or a Windows path — minus the
     *  leading and trailing blank lines, which is what makes a block and the
     *  equivalent explicit {@code doc.doc} tagged value hold the same string. */
    static String canonicalizeDocumentation(String raw) {
        String layout = textBlockLayout(raw);
        // the layout already stripped trailing whitespace: blank == empty —
        // so the edge blank lines are exactly the leading and trailing '\n'
        // runs (hand-rolled: the regex family is banned on the drop-in
        // surface, and split() is one of it)
        int start = 0;
        while (start < layout.length() && layout.charAt(start) == '\n') {
            start++;
        }
        int end = layout.length();
        while (end > start && layout.charAt(end - 1) == '\n') {
            end--;
        }
        return layout.substring(start, end);
    }

    /** The text-block layout of a {@code '''...'''} literal WITHOUT the
     *  unescape: the shared half of {@link #docStringValue} (which unescapes)
     *  and {@link #canonicalizeDocumentation} (which must not). */
    static String textBlockLayout(String raw) {
        String normalized = raw.replace("\r\n", "\n").replace('\r', '\n');
        int firstNl = normalized.indexOf('\n');
        if (firstNl < 0) {
            // single-line ''' — the oracle's grammar refuses this form
            // outright; keep the naive strip for the lenient dialects
            return normalized.substring(3, normalized.length() - 3);
        }
        String body = normalized.substring(firstNl + 1, normalized.length() - 3);
        // hand-rolled split (regex family is banned on the drop-in surface);
        // KEEPS the trailing empty line, like the engine's split("\n", -1)
        java.util.List<String> lineList = new java.util.ArrayList<>();
        int lineStart = 0;
        for (int i = 0; i <= body.length(); i++) {
            if (i == body.length() || body.charAt(i) == '\n') {
                lineList.add(body.substring(lineStart, i));
                lineStart = i + 1;
            }
        }
        String[] lines = lineList.toArray(new String[0]);
        int minIndent = Integer.MAX_VALUE;
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            int leading = 0;
            while (leading < line.length()
                    && Character.isWhitespace(line.charAt(leading))) {
                leading++;
            }
            if (leading < line.length() || i == lines.length - 1) {
                minIndent = Math.min(minIndent, leading);
            }
        }
        if (minIndent == Integer.MAX_VALUE) {
            minIndent = 0;
        }
        StringBuilder builder = new StringBuilder(body.length());
        for (int i = 0; i < lines.length; i++) {
            if (i > 0) {
                builder.append('\n');
            }
            String line = lines[i].substring(Math.min(minIndent, lines[i].length()));
            int end = line.length();
            while (end > 0 && Character.isWhitespace(line.charAt(end - 1))) {
                end--;
            }
            builder.append(line, 0, end);
        }
        return builder.toString();
    }

    // -----------------------------------------------------------------
    // Documentation — `'''...'''` before a declaration (legend-pure 5.99.0
    // / legend-engine 4.145.0): sugar for the meta::pure::profiles::doc
    // `doc` tagged value, prepended to the declaration's own, its value
    // canonicalized (never unescaped) and flagged multiLine on the wire.
    // Listed explicitly at each declaration that accepts it, so a block in
    // expression position is untouched. The engine reads it in ONE helper
    // (PureGrammarParserUtility.taggedValuesWithDocumentation); so does
    // the fleet, here.
    // -----------------------------------------------------------------

    static final String DOC_PROFILE_PATH = "meta::pure::profiles::doc";
    static final String DOC_TAG = "doc";

    /** A documentation literal read at declaration position: the token, its
     *  raw text, and its span over the WHOLE literal (closing delimiter
     *  included — a token reports only the line it starts on). */
    record Documentation(int token, String raw, com.legend.protocol.SourceInfo span) {
    }

    /** The span of a documentation literal — computed from the text, the
     *  engine's {@code documentationSourceInformation}: it always spans
     *  lines and always ends on {@code '''}. (A tagged value's own
     *  {@code '''} VALUE keeps the shared single-line arithmetic of
     *  {@link #docStringSpan} — the engine's helper does, and its suites
     *  pin those coordinates.) */
    default com.legend.protocol.SourceInfo documentationSpan(int tok, String raw) {
        TokenStream ts = tokens();
        int newLines = 0;
        for (int i = raw.indexOf('\n'); i >= 0; i = raw.indexOf('\n', i + 1)) {
            newLines++;
        }
        int endColumn = raw.length() - raw.lastIndexOf('\n') - 1;
        return new com.legend.protocol.SourceInfo(spanSourceId(),
                ts.startLine(tok), ts.startColumn(tok),
                ts.startLine(tok) + newLines, endColumn);
    }

    /** Read a documentation literal at the cursor, if one stands there.
     *  A {@code '''} token is consumed and returned whatever its shape
     *  (the block-shape check is {@link #taggedValuesWithDocumentation}'s,
     *  as in the engine); a plain quoted string at declaration position is
     *  the engine's refusal, verbatim. */
    default @com.legend.Nullable Documentation parseDocumentation() {
        if (peek() == TokenType.STRING && pos() + 1 < tokens().count()
                && DECLARATION_HEADS.contains(peek(1))) {
            throw error("Documentation must be written as a multi-line ('''...''') literal");
        }
        if (peek() != TokenType.DOC_STRING) {
            return null;
        }
        int tok = pos();
        String raw = text();
        advance();
        return new Documentation(tok, raw, documentationSpan(tok, raw));
    }

    /** The declaration keywords documentation may precede (the parser's
     *  own dispatch reads through a leading literal to reach one). */
    static final java.util.Set<TokenType> DECLARATION_HEADS = java.util.Set.of(
            TokenType.CLASS, TokenType.ENUM, TokenType.ASSOCIATION, TokenType.FUNCTION,
            TokenType.NATIVE, TokenType.PROFILE);

    /** {@link #parseDecorations()}'s result with the documentation folded
     *  into its tagged values — the relational store's five sites. */
    default Decorations withDocumentation(@com.legend.Nullable Documentation doc, Decorations dec) {
        return doc == null ? dec
                : new Decorations(dec.stereotypes(), taggedValuesWithDocumentation(doc, dec.taggedValues()));
    }

    /** An element's tagged values with its documentation folded in as the
     *  FIRST — the engine's helper, rule for rule: a non-block literal and a
     *  conflicting explicit {@code doc.doc} (bare {@code doc} through an
     *  import, or the qualified profile; {@code my::pkg::doc} is another
     *  profile) both refuse with the engine's message. */
    default java.util.List<com.legend.protocol.Protocol.PTaggedValue> taggedValuesWithDocumentation(
            @com.legend.Nullable Documentation doc,
            java.util.List<com.legend.protocol.Protocol.PTaggedValue> taggedValues) {
        if (doc == null) {
            return taggedValues;
        }
        if (!isTextBlock(doc.raw())) {
            throw throwAt(tokens(), doc.token(),
                    "Documentation must be written as a multi-line ('''...''') literal");
        }
        for (com.legend.protocol.Protocol.PTaggedValue tv : taggedValues) {
            if (DOC_TAG.equals(tv.tag().value())
                    && (DOC_TAG.equals(tv.tag().profile()) || DOC_PROFILE_PATH.equals(tv.tag().profile()))) {
                throw new ParseException("Element has both documentation and an explicit doc.doc"
                        + " tagged value. Use one.", tv.sourceInformation().startLine(),
                        tv.sourceInformation().startColumn());
            }
        }
        java.util.List<com.legend.protocol.Protocol.PTaggedValue> out =
                new java.util.ArrayList<>(taggedValues.size() + 1);
        out.add(new com.legend.protocol.Protocol.PTaggedValue(
                new com.legend.protocol.Protocol.PTag(DOC_PROFILE_PATH, DOC_TAG, doc.span(), doc.span()),
                canonicalizeDocumentation(doc.raw()), true, doc.span()));
        out.addAll(taggedValues);
        return out;
    }


    /** The engine's span for a {@code '''...'''} token: SINGLE-LINE column
     *  arithmetic over the raw length — endLine stays the start line and
     *  endColumn = startColumn + rawLength - 1 (ZMissedRowsProbe: 3:15-3:41
     *  over a three-line literal). */
    default com.legend.protocol.SourceInfo docStringSpan(int tok) {
        TokenStream ts = tokens();
        int rawLen = ts.end(tok) - ts.start(tok);
        return new com.legend.protocol.SourceInfo(spanSourceId(),
                ts.startLine(tok), ts.startColumn(tok),
                ts.startLine(tok), ts.startColumn(tok) + rawLen - 1);
    }

    default com.legend.protocol.SourceInfo spanOf(int fromTok, int toTok) {
        TokenStream ts = tokens();
        return new com.legend.protocol.SourceInfo(spanSourceId(),
                ts.startLine(fromTok), ts.startColumn(fromTok),
                ts.endLine(toTok), ts.endColumn(toTok));
    }

    /** Engine rule (RelationalParseTreeWalker:1036): a join-pointer TYPE
     *  annotation {@code (X)@join} admits exactly INNER | OUTER,
     *  case-sensitive — {@code (inner)} is one of engine's own negative
     *  fixtures. Shared by every parser that reads join pointers, so the
     *  leniency cannot survive in one grammar after dying in another. */
    default void validateJoinType(String joinType) {
        if (!"INNER".equals(joinType) && !"OUTER".equals(joinType)) {
            // ENGINE-VERBATIM (message-parity gate 2026-08-13): the thrown
            // message is the bare form — the "supported join types are"
            // suffix lite carried does not appear on the 4.138.2 wire
            throw error("Unsupported join type '" + joinType + "'");
        }
    }

    /** The raw text spanned by tokens {@code [startToken, endToken)} —
     *  island content arrives as COARSE chunks, so reconstructing the
     *  original characters for a re-lex is the engine's own island
     *  mechanism (see DropInSurfaceTextRuleTest's source whitelist). */
    default String reconstructText(int startToken, int endToken) {
        if (startToken >= endToken) {
            return "";
        }
        // spelled tokens().source() so DropInSurfaceTextRuleTest's census
        // SEES this site — hiding it behind a local would evade the audit
        return tokens().source().substring(tokens().start(startToken),
                tokens().end(endToken - 1));
    }

    /** The {@code sourceId} stamped on spans — "" everywhere EXCEPT
     *  mapping test-suite query lambdas, which the engine reparses with
     *  the MAPPING's own path as the source id (probe test-suites). */
    default String spanSourceId() {
        return "";
    }

    /**
     * Parse one entry inside a generic argument list, recognising the
     * schema-algebra operators that engine's
     * {@code parseTypeWithOperation} accepts. Precedence (binding
     * tightest first):
     * <ol>
     *   <li>{@code =} (equal) applied to the immediately-parsed base;</li>
     *   <li>{@code +} / {@code -} (union / difference) chained
     *       left-leaning on whatever the EQUAL stage produced;</li>
     *   <li>{@code \u2286} (subset) applied last.</li>
     * </ol>
     * If no operator follows, returns the plain {@link #parseType()}
     * result unchanged.
     */
    default TypeExpression parseTypeArgument() {
        TypeExpression result = parseType();
        if (match(TokenType.EQUAL)) {
            TypeExpression right = parseType();
            result = new TypeExpression.SchemaAlgebra(result, TypeExpression.Op.EQUAL, right);
        }
        while (peek() == TokenType.PLUS || peek() == TokenType.MINUS) {
            TypeExpression.Op op = match(TokenType.PLUS)
                    ? TypeExpression.Op.UNION
                    : (match(TokenType.MINUS) ? TypeExpression.Op.DIFFERENCE : null);
            TypeExpression right = parseType();
            result = new TypeExpression.SchemaAlgebra(result,
                    java.util.Objects.requireNonNull(op, "schema algebra without +/-"), right);
        }
        if (match(TokenType.SUBSET)) {
            TypeExpression superSet = parseType();
            result = new TypeExpression.SchemaAlgebra(result, TypeExpression.Op.SUBSET, superSet);
        }
        return result;
    }

    /**
     * Parse a multiplicity annotation: {@code [N]}, {@code [N..M]},
     * {@code [N..*]}, {@code [*]}, or {@code [identifier]} (parameter
     * reference). Opening {@code '['} has not yet been consumed.
     */
    /**
     * One multiplicity argument inside a generic application's
     * {@code |}-section ({@code Result<T|m>} / {@code Result<TDS|1>} /
     * {@code Result<X|0..1>}) — the UNBRACKETED multiplicity spelling,
     * kept as text (the argument names or fixes a multiplicity
     * parameter; nothing downstream computes with it yet).
     */
    default String parseMultiplicityArgumentText() {
        TokenType t = peek();
        if (t == TokenType.STAR) {
            advance();
            return "*";
        }
        if (t == TokenType.INTEGER) {
            String lower = text();
            advance();
            if (match(TokenType.DOT_DOT)) {
                if (match(TokenType.STAR)) {
                    return lower + "..*";
                }
                if (peek() == TokenType.INTEGER) {
                    String upper = text();
                    advance();
                    return lower + ".." + upper;
                }
                throw error("expected integer or '*' after '..' in a"
                        + " multiplicity argument");
            }
            return lower;
        }
        if (isIdentifierToken(t)) {
            String name = text();
            advance();
            return name;
        }
        throw error("expected a multiplicity argument (n, n..m, *, or a"
                + " multiplicity parameter), got " + t);
    }

    default Multiplicity parseMultiplicity() {
        expect(TokenType.BRACKET_OPEN);
        Multiplicity result;
        TokenType t = peek();
        if (t == TokenType.STAR) {
            advance();
            result = Multiplicity.Concrete.ZERO_MANY;
        } else if (t == TokenType.INTEGER) {
            int lower = consumeBoundedInt("multiplicity bound");
            if (match(TokenType.DOT_DOT)) {
                if (match(TokenType.STAR)) {
                    result = new Multiplicity.Concrete(lower, null);
                } else if (peek() == TokenType.INTEGER) {
                    int upper = consumeBoundedInt("multiplicity bound");
                    // NO upper>=lower check: the engine's grammar has none
                    // ([2..1] parses there; deep-audit 1f) — bound sanity
                    // is the compiler's
                    result = new Multiplicity.Concrete(lower, upper);
                } else {
                    throw error("expected integer or '*' after '..' in multiplicity");
                }
            } else {
                result = new Multiplicity.Concrete(lower, lower);
            }
        } else if (isIdentifierToken(t)) {
            String name = text();
            advance();
            result = new Multiplicity.Parameter(name);
        } else {
            throw error("expected multiplicity bound or parameter, got " + t);
        }
        expect(TokenType.BRACKET_CLOSE);
        return result;
    }

    // -----------------------------------------------------------------
    // Sub-grammars (private to the interface)
    // -----------------------------------------------------------------

    /** {@code {Type[mult], ... -> Type[mult]}}. */
    private TypeExpression parseFunctionType() {
        int startTok = pos();
        expect(TokenType.BRACE_OPEN);
        List<TypeExpression.TypedParameter> params = new ArrayList<>();
        if (peek() != TokenType.ARROW) {
            params.add(parseTypedParameter());
            while (match(TokenType.COMMA)) {
                params.add(parseTypedParameter());
            }
        }
        expect(TokenType.ARROW);
        TypeExpression resultType = parseType();
        Multiplicity resultMult = parseMultiplicity();
        expect(TokenType.BRACE_CLOSE);
        if (dialect().refusesLiteExtensions()) {
            // DECLARED extension LITE-DESIGN-function-types-generics
            // (OWN_CORPUS_DECISIONS §11): the engine's own message says
            // "yet" — LEGEND_LITE parses what lite's type checker fully
            // supports; the exact-engine surface refuses engine-verbatim
            throw error("The type " + compactText(startTok, pos() - 1)
                    + " is not supported yet");
        }
        return new TypeExpression.FunctionType(
                params,
                new TypeExpression.TypedParameter(resultType, resultMult));
    }

    /** {@code (col:Type[mult], ...)}. Column multiplicity defaults to
     *  {@code [1]} when not declared (engine parity). */
    private TypeExpression parseRelationType() {
        expect(TokenType.PAREN_OPEN);
        List<TypeExpression.Column> columns = new ArrayList<>();
        if (peek() != TokenType.PAREN_CLOSE) {
            columns.add(parseRelationColumn());
            while (match(TokenType.COMMA)) {
                columns.add(parseRelationColumn());
            }
        }
        expect(TokenType.PAREN_CLOSE);
        return new TypeExpression.RelationType(columns);
    }

    /** One column in a {@link TypeExpression.RelationType}:
     *  {@code name : Type [mult]?}. The column name may be a literal
     *  {@code "?"} wildcard (used in the rename DSL). */
    private TypeExpression.Column parseRelationColumn() {
        int nameTok = pos();
        String colName = match(TokenType.QUESTION) ? "?" : parseIdentifier();
        expect(TokenType.COLON);
        // the column TYPE may be the '?' wildcard too — m3's
        // schema-algebra column patterns ((?:?)⊆T, PCT over.pure);
        // position-bounded to the relation-column production
        int qTok = pos();
        TypeExpression colType = match(TokenType.QUESTION)
                ? new TypeExpression.NameRef("?", spanOf(qTok, qTok))
                : parseType();
        // wire span = name token (quotes included) .. TYPE end, extended through a
        // DECLARED multiplicity (ProbeWireShapes "relation type sigs", "declared col
        // mult and relation shape")
        boolean declared = peek() == TokenType.BRACKET_OPEN;
        Multiplicity mult = declared ? parseMultiplicity() : Multiplicity.exactly(1);
        com.legend.protocol.SourceInfo span = spanOf(nameTok, pos() - 1);
        return new TypeExpression.Column(colName, colType, mult, declared, span);
    }

    /** {@code Type[mult]} &mdash; a typed parameter in a function-type
     *  signature. */
    private TypeExpression.TypedParameter parseTypedParameter() {
        TypeExpression t = parseType();
        Multiplicity mult = parseMultiplicity();
        return new TypeExpression.TypedParameter(t, mult);
    }
}
