package com.legend.parser;

import com.legend.model.Multiplicity;
import com.legend.model.TypeExpression;

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
            TokenType.COMPARATOR, // Domain
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
            // Boolean literals
            TokenType.TRUE, TokenType.FALSE,
            // Additional
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

    /** Source text of the token under the cursor, or {@code "<EOF>"}
     *  if the cursor is past end. Used in error messages. */
    default String safeText() {
        return pos() < tokens().count() ? text() : "<EOF>";
    }

    /** Advance the cursor by one token. */
    default void advance() {
        rejectInvalid();   // unlexable input dies HERE, not three phases later
        setPos(pos() + 1);
    }

    /** Whether the cursor is past the last token. */
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
        int line = 1, col = 0;
        String src = tokens.source();
        for (int i = 0; i < charPos && i < src.length(); i++) {
            if (src.charAt(i) == '\n') { line++; col = 0; }
            else col++;
        }
        throw new ParseException(message, line, col);
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
    default boolean isIdentifierToken(TokenType t) {
        return t != null && IDENTIFIER_TOKENS.contains(t);
    }

    /** Whether {@code t} can stand as a segment of a qualified name
     *  ({@code a::b::c}). Stricter than {@link #isIdentifierToken}:
     *  quoted strings are <em>not</em> admissible as FQN segments &mdash;
     *  {@code foo::'bar'::baz} is not legal Pure in any position. */
    default boolean isFqnSegmentToken(TokenType t) {
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
        if (!isFqnSegmentToken(peek())) {
            throw error("expected type name, got " + peek());
        }
        StringBuilder sb = new StringBuilder(text());
        advance();
        while (peek() == TokenType.PATH_SEPARATOR) {
            advance();
            if (!isFqnSegmentToken(peek())) {
                throw error("expected identifier after '::' in qualified name");
            }
            sb.append("::").append(text());
            advance();
        }
        return sb.toString();
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
        String body = raw.substring(1, raw.length() - 1);
        if (body.indexOf('\\') < 0) {
            return body;
        }
        StringBuilder sb = new StringBuilder(body.length());
        int i = 0;
        while (i < body.length()) {
            char c = body.charAt(i);
            if (c != '\\') {
                sb.append(c);
                i++;
                continue;
            }
            if (i + 1 >= body.length()) {
                throw at.error("malformed quoted name: trailing backslash");
            }
            char esc = body.charAt(i + 1);
            switch (esc) {
                case '\\' -> sb.append('\\');
                case '\'' -> sb.append('\'');
                case 'n' -> sb.append('\n');
                case 't' -> sb.append('\t');
                case 'r' -> sb.append('\r');
                default -> throw at.error(
                        "malformed quoted name: unsupported escape '\\" + esc + "'");
            }
            i += 2;
        }
        return sb.toString();
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
        String name = parseQualifiedName();
        if (!match(TokenType.LESS_THAN)) {
            return new TypeExpression.NameRef(name);
        }
        List<TypeExpression> args = new ArrayList<>();
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
        return new TypeExpression.Generic(name, args, multArgs);
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
            result = new TypeExpression.SchemaAlgebra(result, op, right);
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
            int lower = Integer.parseInt(text());
            advance();
            if (match(TokenType.DOT_DOT)) {
                if (match(TokenType.STAR)) {
                    result = new Multiplicity.Concrete(lower, null);
                } else if (peek() == TokenType.INTEGER) {
                    int upper = Integer.parseInt(text());
                    advance();
                    if (upper < lower) {
                        throw throwAt(tokens(), pos() - 1,
                                "multiplicity upper bound (" + upper
                                        + ") must be >= lower bound (" + lower + ")");
                    }
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
        String colName = match(TokenType.QUESTION) ? "?" : parseIdentifier();
        expect(TokenType.COLON);
        // real m3 mayColumnType: (QUESTION | type) — the wildcard type
        // slot ((?:?) in over/SortInfo signatures) mirrors the name
        // slot's literal-"?" convention.
        TypeExpression colType = match(TokenType.QUESTION)
                ? new TypeExpression.NameRef("?")
                : parseType();
        Multiplicity mult = (peek() == TokenType.BRACKET_OPEN)
                ? parseMultiplicity()
                : Multiplicity.exactly(1);
        return new TypeExpression.Column(colName, colType, mult);
    }

    /** {@code Type[mult]} &mdash; a typed parameter in a function-type
     *  signature. */
    private TypeExpression.TypedParameter parseTypedParameter() {
        TypeExpression t = parseType();
        Multiplicity mult = parseMultiplicity();
        return new TypeExpression.TypedParameter(t, mult);
    }
}
