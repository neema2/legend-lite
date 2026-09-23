package com.legend.parser;

import com.legend.protocol.Multiplicity;
import com.legend.protocol.TypeExpression;

import com.legend.lexer.Lexer;
import com.legend.lexer.TokenStream;
import com.legend.lexer.TokenType;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.CDate;
import com.legend.protocol.spec.CDecimal;
import com.legend.protocol.spec.CFloat;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CLatestDate;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.CTime;
import com.legend.values.PureDateLiteral;
import com.legend.values.PureTimeLiteral;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.ColumnInstance;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.KeyExpression;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.NewInstance;
import com.legend.protocol.spec.NewInstanceCast;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.TypeAnnotation;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Parser for Pure value specifications (expressions). Produces a sealed
 * {@link ValueSpecification} AST.
 *
 * <h2>Phase status &mdash; C.5 (typed lambda params + multiplicity)</h2>
 * The grammar covers:
 * <ul>
 *   <li><strong>Literals</strong> &mdash; {@link CInteger} (narrowing
 *       to {@link Long} when the value fits else {@link BigInteger}),
 *       {@link CFloat}, {@link CDecimal} ({@code d}/{@code D} suffix),
 *       {@link CString} (quotes stripped, escapes resolved),
 *       {@link CBoolean}, {@link CDate} (year, year-month, strict-date,
 *       date-with-hour, date-with-minute, date-with-second, date-with-subsecond),
 *       {@link CTime} (strict-time), {@link CLatestDate} ({@code %latest}).</li>
 *   <li><strong>Variables</strong> ({@link Variable}) &mdash; {@code $name}.</li>
 *   <li><strong>Collection literals</strong> ({@link PureCollection})
 *       &mdash; {@code [v1, v2, ...]}, empty form {@code []} legal.</li>
 *   <li><strong>Parenthesised grouping</strong> &mdash; {@code (expr)}
 *       returns {@code expr} verbatim; the AST carries no grouping
 *       node because parens encode no semantic information beyond
 *       precedence (which has no consumer until C.3's operators).</li>
 *   <li><strong>Bare element references</strong>
 *       ({@link PackageableElementPtr}) &mdash; {@code MyClass},
 *       {@code my::pkg::MyClass}; identifier or {@code ::}-qualified
 *       name at expression position that is <em>not</em> followed by
 *       {@code (} (otherwise it is a prefix function call).</li>
 *   <li><strong>Prefix function application</strong>
 *       ({@link AppliedFunction}) &mdash; {@code fn(x, y)} or
 *       {@code my::pkg::fn(x)}; an identifier (or FQN) immediately
 *       followed by {@code (args)}.</li>
 *   <li><strong>Property access</strong> ({@link AppliedProperty})
 *       &mdash; {@code receiver.prop}, including chains
 *       {@code $x.foo.bar} and quoted property names
 *       {@code $x.'My Name'}. Surrounding quotes on quoted names are
 *       stripped and standard escapes resolved.</li>
 *   <li><strong>Method-on-receiver</strong> ({@link AppliedFunction})
 *       &mdash; {@code receiver.method(args)} desugars to
 *       {@code AppliedFunction("method", [receiver, args...])}; the
 *       receiver lives at parameter index 0.</li>
 *   <li><strong>Arrow function application</strong>
 *       ({@link AppliedFunction}) &mdash; {@code receiver->fn(args)}
 *       (and chains: {@code a->f()->g()->h()}); identical desugaring
 *       to method-on-receiver. After parsing, arrow and method forms
 *       are indistinguishable from a prefix call with the receiver
 *       passed as the first argument &mdash; <em>by design</em>, see
 *       {@link AppliedFunction}'s divergence note.</li>
 * </ul>
 *
 * <p>C.3 additionally recognises:
 * <ul>
 *   <li><strong>Binary operators</strong> desugared to
 *       {@link AppliedFunction}: arithmetic {@code +} {@code -}
 *       {@code *} {@code /} ({@code plus}, {@code minus},
 *       {@code times}, {@code divide}); comparison {@code <}
 *       {@code <=} {@code >} {@code >=} ({@code lessThan},
 *       {@code lessThanEqual}, {@code greaterThan},
 *       {@code greaterThanEqual}); equality {@code ==} {@code !=}
 *       ({@code equal}, {@code notEqual}); boolean {@code &&}
 *       {@code ||} ({@code and}, {@code or}).</li>
 *   <li><strong>Unary operators</strong> &mdash; {@code !} ({@code not}),
 *       unary {@code -} ({@code minus}), unary {@code +}
 *       ({@code plus}).</li>
 *   <li><strong>{@code ^NewInstance(...)}</strong>
 *       ({@link NewInstance}) &mdash; struct literal with property
 *       bindings; optional {@code <T1, T2>} type arguments before the
 *       open paren.</li>
 * </ul>
 *
 * <p>C.4 additionally recognises:
 * <ul>
 *   <li><strong>Lambdas</strong> ({@link LambdaFunction}) in three
 *       source forms:
 *       <ul>
 *         <li>Braced: {@code {p | body}}, {@code {p, q | body}},
 *             {@code {| body}} (zero-param). Body is a code block
 *             (semicolon-separated statements).</li>
 *         <li>Shorthand single-param: {@code x | body} (one body
 *             expression, no braces).</li>
 *         <li>Pipe zero-param: {@code | body} (one body
 *             expression).</li>
 *       </ul>
 *       All three produce {@link LambdaFunction}; the source-form
 *       distinction is irrelevant after parsing. Typed parameters
 *       ({@code {p: Integer[1] | ...}}) land in C.5.</li>
 *   <li><strong>{@code let varName = value}</strong> &mdash;
 *       statement-level name binding, desugared to
 *       {@code AppliedFunction("letFunction", [CString(varName),
 *       value])} matching engine's parse-time representation
 *       (engine's stdlib defines a real {@code letFunction} with
 *       signature {@code String[1], T[m] -> T[m]}; the surface
 *       {@code let} syntax is sugar for calling it). Allowed only
 *       at the top of a program line; inside an expression
 *       {@code let} is silently absorbed as an identifier and the
 *       remaining tokens trip the trailing-tokens check.</li>
 *   <li><strong>Code blocks</strong> &mdash; semicolon-separated
 *       sequences of statements, exposed via
 *       {@link #parseCodeBlock(String, Dialect)}. The braced lambda body
 *       uses the same machinery internally.</li>
 * </ul>
 *
 * <h3>Precedence (the engine's {@code DomainParseTreeWalker}, verbatim)</h3>
 * <p>The top level reproduces the engine's
 * {@code combinedExpression = expression (arithmeticPart | booleanPart)*}
 * walk exactly &mdash; including its accumulator-threading quirks (see
 * {@link EngineQuirks}) &mdash; so the tree the compiler receives IS the
 * tree the wire describes; the emitter has no shape patches. Grammar
 * facts this encodes (M3ParserGrammar.g4:199-211):
 * <ul>
 *   <li>an {@code arithmeticPart} is a SAME-OP RUN
 *       ({@code (PLUS expression)+} | minus/star/divide runs) or a
 *       single comparison ({@code < <= > >= expression});</li>
 *   <li>{@code plus}/{@code minus}/{@code times} wrap all operands in
 *       ONE collection parameter ({@code plus[Collection[a,b,c]]});
 *       {@code divide} and the comparisons take pairwise params;</li>
 *   <li>{@code booleanPart} is {@code (AND|OR) expression}; boolean and
 *       arithmetic parts interleave through the shared accumulators of
 *       {@link #parseCombinedExpression()}.</li>
 * </ul>
 * The structure has three nested levels:
 * <ol>
 *   <li>{@link #parseCombinedExpression()} &mdash; outermost. The
 *       engine's combined loop over boolean/arithmetic parts.</li>
 *   <li>{@link #parseExpression()} &mdash; middle. Handles postfix
 *       ({@code .}, {@code ->}) and a single trailing
 *       {@code ==}/{@code !=} after them.</li>
 *   <li>{@link #parseUnaryAndPrimary()} &mdash; inner. Eats unary
 *       prefix operators ({@code !}, {@code -}, {@code +}) before
 *       dispatching to {@link #parsePrimary()}.</li>
 * </ol>
 * <p>Parens, collections, and call-argument lists all parse their
 * contents with {@link #parseCombinedExpression()} so the full
 * operator grammar works inside.
 *
 * <p>Anything else &mdash; milestoning, type annotations
 * ({@code @Type}), column instances ({@code ~col}) &mdash; raises a
 * {@link ParseException} that names the unsupported construct by
 * token type. Per the AGENTS.md no-fallbacks rule, the parser never
 * silently accepts an unknown construct as something else; the error
 * lists the source line/column so callers can repair the input
 * directly. The remaining phase (C.5) covers the last grammar
 * additions to reach engine parity.
 *
 * <h2>Entry points</h2>
 * <ul>
 *   <li>{@link #parse(String, Dialect)} / {@link #parse(TokenStream, Dialect)} &mdash;
 *       parse a single program line (one expression or one
 *       let-binding). Convenience entry for tests, embedded
 *       expressions (e.g. property defaults), and any context that
 *       admits exactly one statement.</li>
 *   <li>{@link #parseCodeBlock(String, Dialect)} /
 *       {@link #parseCodeBlock(TokenStream)} &mdash; parse a
 *       semicolon-separated sequence of statements, returning a
 *       {@code List<ValueSpecification>}. Used for function bodies
 *       and any multi-statement context.</li>
 * </ul>
 *
 * <p>{@link TokenStream} entry points accept a
 * {@link TokenStream#slice(int, int) slice} of a larger file; source
 * offsets are preserved so error reporting points back to the
 * original source line/column.
 *
 * <p>Both entry points consume the <em>entire</em> input: any tokens
 * remaining after the single expression cause a fail-fast error. This
 * matches {@link ElementParser}'s contract and prevents silent
 * truncation when the source contains more than one expression.
 */
public final class SpecParser implements TokenStreamCursor {

    private final TokenStream tokens;
    private int pos;

    /** Body-level dialect constructs ({@code .allVersionsInRange},
     *  function-type literals, m2 forms) gate on the THREE-LEVEL
     *  {@link Dialect} — always via the PRECISE predicate
     *  ({@code refusesPlatformDialect} vs {@code refusesLiteExtensions});
     *  the old two-valued {@code legendStrict} conflated them. */
    private final Dialect dialect;

    @Override
    public Dialect dialect() {
        return dialect;
    }

    /** The character-level island scanner (extracted seam — see
     *  {@link IslandScan}); shares this parser's tokens and error anchor. */
    private final IslandScan islandScan;

    private SpecParser(TokenStream tokens, String spanSourceId,
            Dialect dialect) {
        this.tokens = Objects.requireNonNull(tokens, "tokens");
        this.spanSourceId = spanSourceId;
        this.dialect = dialect;
        this.islandScan = new IslandScan(tokens, spanSourceId, this);
        constStrings.push(new java.util.HashMap<>());
    }

    /** LET-BOUND STRING CONSTANTS in scope, innermost body first: a
     *  {@code let} whose value is a literal string (or a {@code +} chain
     *  of them, through earlier constants) records here so the quote/eval
     *  fold ({@link QuotedSpecParser#fold}) folds
     *  {@code compileLegendValueSpecification($treeString)} exactly as the
     *  inline-literal spelling — strings die at the parser either way.
     *  Every lambda body pushes a scope; a lambda parameter SHADOWS an
     *  outer constant of its name ({@link #SHADOW}). Pure lets are
     *  single-assignment, so a recorded value never changes. */
    private final java.util.ArrayDeque<java.util.Map<String, String>> constStrings =
            new java.util.ArrayDeque<>();

    /** Identity sentinel: the name is bound in this scope but NOT to a
     *  constant (a lambda parameter). Compared by reference. */
    @SuppressWarnings("StringOperationCanBeSimplified")
    private static final String SHADOW = new String("<shadowed>");

    private @com.legend.Nullable String constString(String name) {
        for (java.util.Map<String, String> scope : constStrings) {
            String v = scope.get(name);
            if (v != null) {
                return v == SHADOW ? null : v;
            }
        }
        return null;
    }

    private void pushConstScope(List<Variable> params) {
        java.util.Map<String, String> scope = new java.util.HashMap<>();
        for (Variable p : params) {
            scope.put(p.name(), SHADOW);
        }
        constStrings.push(scope);
    }

    /** Mapping test-suite query lambdas carry the MAPPING's path as the
     *  span source id (probe test-suites); everything else "". */
    private final String spanSourceId;

    @Override
    public String spanSourceId() {
        return spanSourceId;
    }

    // -------------------------------------------------------------------
    // Public entry points
    // -------------------------------------------------------------------

    /** Lex {@code source} and parse the result as a single expression. */
    public static ValueSpecification parse(String source, Dialect dialect) {
        return parse(Lexer.tokenize(Objects.requireNonNull(source, "source")),
                dialect);
    }

    /**
     * Parse the given token stream as a single value specification at the
     * NAMED dialect level — there is no default (HONEST_DEBT #9). The
     * stream may be a {@link TokenStream#slice(int, int)} of a larger
     * file; offsets are preserved so errors point at the original
     * source location.
     */
    public static ValueSpecification parse(TokenStream tokens,
            Dialect dialect) {
        SpecParser parser = new SpecParser(tokens, "", dialect);
        ValueSpecification result = parser.parseProgramLine();
        if (!parser.atEnd()) {
            throw parser.error("trailing tokens after expression: "
                    + parser.peek() + " ('" + parser.safeText() + "')");
        }
        return result;
    }

    /**
     * Parse a multi-statement code block from source text. A code
     * block is a {@code ;}-separated sequence of statements; each
     * statement is one program line (an expression or a let-binding).
     * The block's value is the value of its last statement; the parser
     * does not enforce or annotate this &mdash; it just returns the
     * sequence.
     *
     * <p>This is the entry point {@link ElementParser} uses to eagerly
     * parse element bodies (function bodies, derived-property bodies)
     * as part of a single batch parse. It is also used internally by the
     * braced lambda form to parse the body between {@code {...|...}}.
     */
    public static List<ValueSpecification> parseCodeBlock(String source,
            Dialect dialect) {
        return parseCodeBlock(Lexer.tokenize(source), "", dialect);
    }

    /** Token-stream variant of {@link #parseCodeBlock(String, Dialect)},
     *  at the NAMED dialect level — there is no default. Consumes the
     *  entire stream; trailing tokens after the last statement raise a
     *  fail-fast error. */
    public static List<ValueSpecification> parseCodeBlock(TokenStream tokens,
            Dialect dialect) {
        return parseCodeBlock(tokens, "", dialect);
    }

    /** As above with a span source id (mapping test-suite lambdas). */
    public static List<ValueSpecification> parseCodeBlock(TokenStream tokens,
            String spanSourceId, Dialect dialect) {
        SpecParser parser = new SpecParser(tokens, spanSourceId, dialect);
        List<ValueSpecification> stmts = parser.parseCodeBlockUntil(null);
        if (!parser.atEnd()) {
            throw parser.error("trailing tokens after code block: "
                    + parser.peek() + " ('" + parser.safeText() + "')");
        }
        return stmts;
    }

    // -------------------------------------------------------------------
    // Top-level program-line dispatch (statement level)
    // -------------------------------------------------------------------

    /**
     * One statement &mdash; either a {@code let}-binding (desugared
     * to a {@code letFunction} call) or a full expression.
     * {@code let} is admitted <em>only</em> at this level (the top of
     * a program line, including each statement in a code block); it
     * is not a sub-expression. Engine has the same structural
     * restriction in its {@code parseProgramLine}.
     */
    private ValueSpecification parseProgramLine() {
        if (!atEnd() && peek() == TokenType.LET) {
            return parseLetExpression();
        }
        return parseCombinedExpression();
    }

    /**
     * Parse {@code let varName = value} and desugar to an
     * {@link AppliedFunction} call to {@code letFunction} matching
     * engine's parse-time representation.
     *
     * <h3>Why desugar instead of producing a dedicated record</h3>
     *
     * <p>Pure's design pillar is &ldquo;everything is a
     * function&rdquo;. {@code letFunction} is a real, type-signatured
     * function in the Pure stdlib
     * ({@code letFunction_String_1__T_m__T_m_}); the surface
     * {@code let} keyword is <em>syntactic sugar</em> for calling it,
     * in the same way {@code +} is sugar for {@code plus} and
     * {@code ==} is sugar for {@code equal}. Desugaring at parse
     * time has three concrete payoffs:
     *
     * <ol>
     *   <li><strong>Single type-inference path.</strong> The
     *       type-checker can look up {@code letFunction} in the
     *       stdlib and apply the standard function-application
     *       inference rule; no dedicated {@code Let}-shape case arm
     *       in the inference pipeline. Same as engine
     *       ({@code FunctionExpressionProcessor}).</li>
     *   <li><strong>Type rule lives in data.</strong> The fact that
     *       {@code let x = v} has the type of {@code v} is encoded
     *       in the function's signature, not in the
     *       type-checker. Consistent with how {@code plus},
     *       {@code if}, {@code match}, etc. are handled.</li>
     *   <li><strong>AST byte-comparability with engine.</strong>
     *       Corpus tests that compare against engine's parser output
     *       can compare directly without an adapter.</li>
     * </ol>
     *
     * <p>Downstream concerns (scope registration so {@code $varName}
     * resolves later, closure capture, codegen) recognise let by
     * checking the function name &mdash; same as engine's
     * {@code "letFunction".equals(functionName)} guards in
     * {@code FunctionExpressionProcessor}, {@code LambdaFunctionProcessor},
     * and {@code FunctionProcessor}.
     *
     * <p>A {@code LetExpression} record was considered and rejected;
     * see the C.4 commit message for the analysis.
     *
     * <p>Pre-condition: cursor is on {@link TokenType#LET}. The
     * value is parsed as a full combined expression so binary
     * operators and arrow chains in the right-hand side work as
     * expected.
     */
    private AppliedFunction parseLetExpression() {
        int letTok = pos;
        pos++; // consume LET
        // Variable name may be a bare identifier OR a quoted
        // string (e.g. {@code let 'my var' = 42}). The quoted
        // form lets users bind variables whose surface spelling
        // isn't a legal Pure identifier (whitespace, punctuation,
        // keyword collisions). Engine-lite's parseIdentifierText
        // unquotes transparently; we match.
        String varName;
        if (!atEnd() && peek() == TokenType.STRING) {
            // CString's own parseString handles escape resolution; we
            // reuse it so escape handling is one place.
            varName = ((CString) parseString()).value();
        } else if (!atEnd() && isFqnSegmentToken(peek())) {
            varName = text();
            pos++;
        } else {
            throw error("expected variable name after 'let'");
        }
        expect(TokenType.EQUAL, "expected '=' after 'let " + varName + "'");
        ValueSpecification value = parseCombinedExpression();
        String constant = QuotedSpecParser.foldStringConcat(value,
                this::constString);
        constStrings.peek().put(varName, constant == null ? SHADOW : constant);
        // Engine convention (ProbeWireShapes function): letFunction spans the whole
        // `let name = value` statement; its name-string parameter carries NO span.
        // A statement ENDING on a '''...''' token takes that token's
        // single-line arithmetic end (ZMissedRowsProbe letStatement 3:35).
        com.legend.protocol.SourceInfo letSpan = spanOf(letTok, pos - 1);
        if (tokens.type(pos - 1) == TokenType.DOC_STRING) {
            com.legend.protocol.SourceInfo d = docStringSpan(pos - 1);
            letSpan = new com.legend.protocol.SourceInfo(letSpan.sourceId(),
                    letSpan.startLine(), letSpan.startColumn(),
                    d.endLine(), d.endColumn());
        }
        return new AppliedFunction(
                "letFunction",
                List.of(new CString(varName), value),
                List.of(), letSpan);
    }

    /**
     * Code-block helper used by both the public
     * {@link #parseCodeBlock(TokenStream)} entry and by
     * {@link #parseLambdaFunction()} for braced lambda bodies.
     *
     * <p>{@code terminator} is the token type that ends the block
     * <em>without consuming it</em>: {@link TokenType#BRACE_CLOSE} for
     * lambda bodies, {@code null} for the top-level entry (which
     * reads to EOF). A trailing {@code ;} before the terminator is
     * silently permitted, matching engine.
     */
    private List<ValueSpecification> parseCodeBlockUntil(
            @com.legend.Nullable TokenType terminator) {
        List<ValueSpecification> stmts = new ArrayList<>();
        if (atTerminator(terminator)) {
            return stmts;
        }
        stmts.add(parseProgramLine());
        boolean lastTerminated = false;
        while (!atEnd() && peek() == TokenType.SEMI_COLON) {
            pos++; // consume ';'
            lastTerminated = true;
            if (atTerminator(terminator)) {
                break;
            }
            stmts.add(parseProgramLine());
            lastTerminated = false;
        }
        if (stmts.size() > 1 && !lastTerminated && atTerminator(terminator)) {
            // EVERY dialect: in a MULTI-statement body every statement
            // carries ';' — only a lone statement may omit it. The engine
            // refuses (adversarial audit divergence 16, oracle-verified)
            // and so does REAL legend-pure (M3 codeBlock: programLine
            // (END_LINE (programLine END_LINE)*)? — the old LITE/PLATFORM
            // tolerance was an engine-lite INVENTION, census D4)
            throw error("Unexpected token");
        }
        return stmts;
    }

    private boolean atTerminator(@com.legend.Nullable TokenType terminator) {
        if (atEnd()) {
            return true; // EOF terminates
        }
        return terminator != null && peek() == terminator;
    }

    // -------------------------------------------------------------------
    // Combined-expression level (operators)
    // -------------------------------------------------------------------

    /**
     * Outermost expression entry &mdash; the engine's
     * {@code DomainParseTreeWalker.combinedExpression} loop, verbatim.
     * Consecutive parts of one kind accumulate and flush as a group when
     * the other kind (or the end) arrives; the two results thread
     * crosswise &mdash; a boolean group folds over the latest
     * <em>arithmetic</em> result and vice versa. This single rule
     * produces the engine's boolean-then-arithmetic shape
     * ({@code 1 && 2 + 3} &rarr; {@code plus[Collection[and(1,2), 3]]})
     * and its depth-2 form ({@code 1 || 2 < 3 && 4} &rarr;
     * {@code and(lessThan(or(1,2), 3), 4)}).
     */
    private ValueSpecification parseCombinedExpression() {
        ValueSpecification expr = parseExpression();
        // engine invariant: arth and bool are never both non-empty — one is
        // flushed the moment a part of the other kind arrives
        List<OperatorParts.ArithPart> arth = new ArrayList<>();
        List<OperatorParts.BoolPart> bool = new ArrayList<>();
        ValueSpecification boolResult = expr;
        ValueSpecification arithResult = expr;
        while (!atEnd()) {
            TokenType t = peek();
            if (isArithmeticOp(t)) {
                if (!bool.isEmpty()) {
                    boolResult = OperatorParts.booleanPart(bool, arithResult);
                    bool.clear();
                }
                arth.add(parseArithmeticPartContext());
            } else if (t == TokenType.AND || t == TokenType.OR) {
                if (!arth.isEmpty()) {
                    arithResult = OperatorParts.arithmeticPart(arth, boolResult);
                    arth.clear();
                }
                bool.add(parseBooleanPartContext());
            } else {
                break;
            }
        }
        if (!arth.isEmpty()) {
            return OperatorParts.arithmeticPart(arth, boolResult);
        }
        if (!bool.isEmpty()) {
            return OperatorParts.booleanPart(bool, arithResult);
        }
        return expr;
    }

    /**
     * Middle level: unary-then-primary, followed by a postfix loop
     * over {@code .} (property / method on receiver) and {@code ->}
     * (arrow function call), with a single trailing {@code ==} /
     * {@code !=} consumed at the end. The asymmetry &mdash; postfix
     * binds tighter than {@code ==} which binds tighter than arithmetic
     * &mdash; is Pure's grammar verbatim; see engine's
     * {@code PureQueryParser.parseExpression} for the same shape.
     */
    private ValueSpecification parseExpression() {
        ValueSpecification expr = parseUnaryAndPrimary();
        while (!atEnd()) {
            TokenType t = peek();
            if (t == TokenType.DOT) {
                expr = parseDotPostfix(expr);
            } else if (t == TokenType.ARROW) {
                expr = parseArrowPostfix(expr);
            } else if (t == TokenType.BRACKET_OPEN) {
                // Bracket-indexing postfix: '$x[0]' (integer index) or
                // '$x[\'key\']' (string key). Must be guarded against
                // the top-level BRACKET_OPEN collection literal —
                // reached here only if we already have a receiver
                // expression, which is structurally distinct from a
                // bare '[a,b,c]' collection.
                expr = parseBracketPostfix(expr);
            } else {
                break;
            }
        }
        if (!atEnd()) {
            TokenType t = peek();
            // Pure spells inequality two ways: '!=' (lexed as
            // TEST_NOT_EQUAL) and '<>' (lexed as NOT_EQUAL). Both
            // desugar to the same 'notEqual' AppliedFunction so the
            // model layer doesn't have to know which form was written.
            if (t == TokenType.NOT_EQUAL && dialect.refusesPlatformDialect()) {
                // '<>' is a PURE-DIALECT spelling: the engine has no such
                // token and refuses (adversarial audit fuzz, oracle-verified)
                throw error("Unexpected token '<>'");
            }
            if (t == TokenType.TEST_EQUAL
                    || t == TokenType.TEST_NOT_EQUAL
                    || t == TokenType.NOT_EQUAL) {
                int opTok = pos;
                pos++;
                ValueSpecification right = parseCombinedArithmeticOnly();
                // != desugars to not(equal(...)) — REAL pure's spelling
                // (there is no notEqual native; FQN_MIGRATION finding).
                // Engine convention: equal (and the not wrapping a !=) span the
                // OPERATOR TOKEN only — unlike comparisons, which span op..RHS.
                AppliedFunction eq = new AppliedFunction("equal", List.of(expr, right),
                        List.of(), spanOf(opTok, opTok), false, false, true);
                expr = (t == TokenType.TEST_EQUAL) ? eq
                        : new AppliedFunction("not", List.of(eq), List.of(),
                                spanOf(opTok, opTok), false, false, true);
            }
        }
        return expr;
    }

    /**
     * Inner level: zero-or-one unary prefix operator wrapping a
     * primary. The unary call's <em>operand</em> is a full
     * {@link #parseExpression()} (postfix-aware), so {@code -$x.foo}
     * desugars to {@code minus($x.foo)} &mdash; the minus binds the
     * entire property chain, not just the variable.
     */
    private ValueSpecification parseUnaryAndPrimary() {
        if (!atEnd()) {
            TokenType t = peek();
            if (t == TokenType.NOT) {
                int notTok = pos;
                pos++;
                ValueSpecification operand = parseExpression();
                // Engine convention: `!expr` spans from the bang to the operand's end,
                // closing paren inclusive when the operand is parenthesised.
                return new AppliedFunction("not", List.of(operand), List.of(),
                        spanOf(notTok, pos - 1));
            }
            if (t == TokenType.MINUS) {
                int opTok = pos;
                pos++;
                // Engine convention (ProbeWireShapes cNeg): unary minus is a ONE-parameter
                // func (no collection) spanning just the operator token.
                // -5 RomanLength~Pes: the sign belongs to the NUMBER of a unit
                // literal (engine grammar: unitInstanceLiteral takes a signed number)
                if ((peek() == TokenType.INTEGER || peek() == TokenType.FLOAT
                        || peek() == TokenType.DECIMAL) && unitPathEnd(pos + 1) >= 0) {
                    ValueSpecification n = peek() == TokenType.INTEGER ? parseInteger()
                            : peek() == TokenType.FLOAT ? parseFloat() : parseDecimal();
                    return unitLiteral(new AppliedFunction("minus", List.of(n),
                            List.of(), spanOf(opTok, opTok), false, false, true));
                }
                return new AppliedFunction("minus", List.of(parseExpression()),
                        List.of(), spanOf(opTok, opTok), false, false, true);
            }
            if (t == TokenType.PLUS) {
                int opTok = pos;
                pos++;
                return new AppliedFunction("plus", List.of(parseExpression()),
                        List.of(), spanOf(opTok, opTok), false, false, true);
            }
        }
        return parsePrimary();
    }

    /**
     * The grammar's {@code combinedArithmeticOnly: expression
     * arithmeticPart*} &mdash; the right-hand side of {@code ==}/
     * {@code !=} (no {@code &&}/{@code ||}; those would re-enter the
     * combined-expression loop and never terminate the equality
     * production). Same accumulator machinery as the combined loop.
     */
    private ValueSpecification parseCombinedArithmeticOnly() {
        ValueSpecification expr = parseExpression();
        List<OperatorParts.ArithPart> parts = new ArrayList<>();
        while (!atEnd() && isArithmeticOp(peek())) {
            parts.add(parseArithmeticPartContext());
        }
        return parts.isEmpty() ? expr : OperatorParts.arithmeticPart(parts, expr);
    }

    private static boolean isArithmeticOp(TokenType t) {
        return t == TokenType.PLUS
                || t == TokenType.MINUS
                || t == TokenType.STAR
                || t == TokenType.DIVIDE
                || t == TokenType.LESS_THAN
                || t == TokenType.LESS_OR_EQUAL
                || t == TokenType.GREATER_THAN
                || t == TokenType.GREATER_OR_EQUAL;
    }

    // -------------------------------------------------------------------
    // Engine-shaped operator contexts (the builders live in OperatorParts)
    // -------------------------------------------------------------------

    private OperatorParts.ArithPart parseArithmeticPartContext() {
        TokenType op = peek();
        int firstOp = pos;
        String fn = switch (op) {
            case PLUS -> "plus";
            case MINUS -> "minus";
            case STAR -> "times";
            case DIVIDE -> "divide";
            case LESS_THAN -> "lessThan";
            case LESS_OR_EQUAL -> "lessThanEqual";
            case GREATER_THAN -> "greaterThan";
            case GREATER_OR_EQUAL -> "greaterThanEqual";
            default -> throw new IllegalStateException(
                    "parseArithmeticPartContext on non-arithmetic token: " + op);
        };
        if (op == TokenType.LESS_THAN || op == TokenType.LESS_OR_EQUAL
                || op == TokenType.GREATER_THAN || op == TokenType.GREATER_OR_EQUAL) {
            pos++;
            ValueSpecification rhs = parseExpression();
            return new OperatorParts.ArithPart(fn, true, List.of(rhs),
                    spanOf(firstOp, pos - 1));
        }
        // same-op run: (PLUS expression)+ etc. — greedy, one context
        List<ValueSpecification> operands = new ArrayList<>();
        while (!atEnd() && peek() == op) {
            pos++;
            operands.add(parseExpression());
        }
        return new OperatorParts.ArithPart(fn, false, operands,
                spanOf(firstOp, pos - 1));
    }

    private OperatorParts.BoolPart parseBooleanPartContext() {
        String fn = (peek() == TokenType.AND) ? "and" : "or";
        int opTok = pos;
        pos++;
        return new OperatorParts.BoolPart(fn, parseExpression(), spanOf(opTok, opTok));
    }

    /**
     * Parse one primary expression &mdash; everything that can start an
     * expression. The {@code default} arm folds identifier-shaped
     * tokens (any member of {@link ElementParser#IDENTIFIER_TOKENS},
     * which includes keyword-as-identifier forms) into
     * {@link #parseQualifiedNameStart()} so a bare {@code MyClass} can
     * become either a {@link PackageableElementPtr} or the function
     * name of an {@link AppliedFunction}, decided by lookahead.
     */
    private ValueSpecification parsePrimary() {
        if (atEnd()) {
            throw error( "expected expression, got end of input");
        }
        TokenType t = peek();
        return switch (t) {
            case INTEGER -> unitLiteral(parseInteger());
            case FLOAT -> unitLiteral(parseFloat());
            case DECIMAL -> unitLiteral(parseDecimal());
            // a QUOTED NAME can start an expression when a call or path
            // follows — 'abs'($x) and 'pkg'::f are engine-legal (the
            // identifier rule admits quoted strings; Tier-3 residue,
            // oracle-verified); a bare string stays a literal
            case STRING -> peek(1) == TokenType.PAREN_OPEN
                    || peek(1) == TokenType.PATH_SEPARATOR
                    ? parseQualifiedNameStart() : parseString();
            case DOC_STRING -> parseDocString();
            case TRUE -> consumeBoolean(true);
            case FALSE -> consumeBoolean(false);
            case DATE -> parseDateOrDateTime();
            case STRICTTIME -> parseStrictTime();
            case LATEST_DATE -> parseLatestDate();
            case DOLLAR -> parseVariable();
            case BRACKET_OPEN -> parseCollection();
            case PAREN_OPEN -> parseParenthesised();
            case NEW_SYMBOL -> parseNewInstance();
            case BRACE_OPEN -> parseLambdaFunction();
            case PIPE -> parseLambdaPipe();
            case TILDE -> parseColumnBuilders();
            // The lexer fuses '~distinct' & friends into MAPPING-grammar
            // command tokens; in EXPRESSION position they are ordinary
            // colspecs whose column happens to bear the command's name.
            case DISTINCT_CMD, FILTER_CMD, GROUP_BY_CMD, MAIN_TABLE_CMD,
                    PRIMARY_KEY_CMD, SRC_CMD -> parseTildeCommandColSpec();
            case AT -> parseTypeAnnotation();
            case TDS_LITERAL -> parseTdsLiteral();
            case PATH_LITERAL -> parsePathLiteral();
            case ISLAND_OPEN -> parseDsl();
            default -> {
                if (isIdentifierToken(t)) {
                    // Single-param lambda shorthand: 'x | body'. The
                    // lookahead must NOT cross a PATH_SEPARATOR, since
                    // 'my::pkg' is never a lambda parameter (lambda
                    // params are simple identifiers, not FQNs).
                    if (pos + 1 < tokens.count()
                            && tokens.type(pos + 1) == TokenType.PIPE) {
                        yield parseSingleParamLambda();
                    }
                    // Typed single-param shorthand:
                    // 'x: Type[mult] | body'. Speculative lookahead
                    // commits only if the full pattern is present;
                    // otherwise an identifier-followed-by-colon
                    // could be a different construct (none today,
                    // but the disciplined check keeps the dispatch
                    // honest).
                    if (pos + 1 < tokens.count()
                            && tokens.type(pos + 1) == TokenType.COLON
                            && looksLikeTypedLambdaParam()) {
                        yield parseSingleParamLambda();
                    }
                    yield parseQualifiedNameStart();
                }
                // a BARE `::` in value position is the ROOT-PACKAGE
                // reference (corpus: ^Database(package = ::)) — a nominal
                // element pointer; instance-metamodel consumers stay loud.
                // But `::` as a CALLER (`::.all()`) is refused with the
                // engine's own text (harvest TestMappingGrammarParser
                // testLambdaWithFunctionAllWithoutFunctionCaller)
                if (t == TokenType.PATH_SEPARATOR) {
                    advance();
                    if (!atEnd() && peek() == TokenType.DOT) {
                        throw error("Expected a non-empty function caller");
                    }
                    yield new PackageableElementPtr("::", spanOf(pos - 1, pos - 1));
                }
                throw error("unsupported expression token: " + t
                        + " ('" + safeText() + "')");
            }
        };
    }

    // -------------------------------------------------------------------
    // Numeric literals
    // -------------------------------------------------------------------

    private CInteger parseInteger() {
        int tok = pos++;
        return NumberLiterals.integer(tokens.text(tok), spanOf(tok, tok), dialect(), tokens, tok);
    }

    private ValueSpecification parseFloat() {
        int tok = pos++;
        return NumberLiterals.floating(tokens.text(tok), spanOf(tok, tok), dialect());
    }

    private CDecimal parseDecimal() {
        int tok = pos++;
        return NumberLiterals.decimal(tokens.text(tok), spanOf(tok, tok));
    }

    // -------------------------------------------------------------------
    // String / boolean
    // -------------------------------------------------------------------

    /**
     * STRING token &rarr; {@link CString}. The raw lexer text includes
     * the surrounding single quotes and any backslash escapes; this
     * method strips the quotes and resolves the standard escape set
     * ({@code \\}, {@code \'}, {@code \n}, {@code \t}, {@code \r}).
     * Other escape sequences are surfaced as errors so we never silently
     * accept malformed input.
     */
    private CString parseString() {
        String raw = text();
        if (raw.length() < 2 || raw.charAt(0) != '\'' || raw.charAt(raw.length() - 1) != '\'') {
            throw error("malformed string literal: missing surrounding quotes");
        }
        String body = raw.substring(1, raw.length() - 1);
        String unescaped = unescapeString(body);
        int tok = pos;
        pos++;
        // Engine convention: a string literal's span INCLUDES the surrounding quotes.
        return new CString(unescaped, spanOf(tok, tok));
    }

    private String unescapeString(String body) {
        // delegates to THE escape table (TokenStreamCursor.unescapeBody) —
        // names and string literals must decode identically (audit §2.5)
        return TokenStreamCursor.unescapeBody(body, "string literal", this);
    }

    /** A {@code '''...'''} literal in expression position (4.138,
     *  ZMissedRowsProbe): value via the shared strip rule; span via the
     *  engine's single-line column arithmetic; NO escape resolution —
     *  documentation blocks have no escape mechanism at all. */
    private CString parseDocString() {
        int tok = pos;
        String raw = text();
        pos++;
        return new CString(TokenStreamCursor.docStringValue(raw),
                docStringSpan(tok), true);
    }

    private CBoolean consumeBoolean(boolean value) {
        int tok = pos;
        pos++;
        return new CBoolean(value, spanOf(tok, tok));
    }

    // -------------------------------------------------------------------
    // Temporal literals
    // -------------------------------------------------------------------

    /**
     * DATE token &rarr; {@link CDate} carrying a structured
     * {@link PureDateLiteral}. The lexer routes year-only, year-month,
     * strict-date, date-with-hour, date-with-minute, date-with-second,
     * and date-with-subsecond shapes to a single DATE token (with
     * optional TZ suffix on time-bearing forms); the structural variant
     * is decided by {@link PureDateLiteral#parse}, which also validates
     * component values and normalises any TZ to GMT (the {@code %}
     * prefix is stripped here, mirroring engine's
     * record contract).
     */
    private ValueSpecification parseDateOrDateTime() {
        String raw = text();
        if (raw.isEmpty() || raw.charAt(0) != '%') {
            throw error("malformed date literal: expected leading '%'");
        }
        String value = raw.substring(1);
        int datePos = pos;
        pos++;
        if (dialect.refusesPlatformDialect() && value.indexOf('Z') >= 0) {
            // the engine's DATETIME lexer has no 'Z' timezone suffix and
            // refuses it (adversarial audit fuzz, oracle-verified); the
            // pure dialect keeps the audit-M5 Z support
            throw error("Unexpected token '%" + value + "'");
        }
        try {
            // Component-range validation by dialect (D6b): LEGEND_ENGINE
            // defers like the engine's parser (%2024-02-30 parses there —
            // oracle byte-parity; THEIR compiler validates downstream);
            // LEGEND_PLATFORM and LEGEND_LITE validate at parse — the
            // product surface must not ship a date the database later
            // refuses with an unattributed conversion error.
            return new CDate(PureDateLiteral.parse(value, dialect != Dialect.LEGEND_ENGINE),
                    value, spanOf(datePos, datePos));
        } catch (IllegalArgumentException e) {
            throw TokenStreamCursor.throwAt(tokens, datePos,
                    "invalid date literal '%" + value + "': " + e.getMessage());
        }
    }

    private CTime parseStrictTime() {
        String raw = text();
        if (raw.isEmpty() || raw.charAt(0) != '%') {
            throw error("malformed time literal: expected leading '%'");
        }
        int timePos = pos;
        pos++;
        String value = raw.substring(1);
        try {
            return new CTime(PureTimeLiteral.parse(value), value,
                    spanOf(timePos, timePos));
        } catch (IllegalArgumentException e) {
            // the ENGINE parses out-of-range times (%200:12:22) — carry the raw text,
            // validation is the compiler's (inline-snippet corpus)
            return new CTime(null, value, spanOf(timePos, timePos));
        }
    }

    private CLatestDate parseLatestDate() {
        int tok = pos;
        pos++;
        return new CLatestDate(spanOf(tok, tok));
    }

    // -------------------------------------------------------------------
    // Variable
    // -------------------------------------------------------------------

    /**
     * {@code $} IDENTIFIER &rarr; {@link Variable}. Strict: the
     * {@code $} must be immediately followed by an identifier (or any
     * keyword that participates in {@link ElementParser#IDENTIFIER_TOKENS},
     * matching how the element parser admits keyword-as-identifier).
     */
    private Variable parseVariable() {
        int dollarTok = pos;
        pos++; // consume '$'
        if (!isIdentifierToken(peek())) {
            throw error("expected identifier after '$' to form a variable reference");
        }
        // $'my var' must reference the SAME name `let 'my var' = ...` bound
        // (audit M10: the quoted forms previously disagreed).
        String name = peek() == TokenType.STRING
                ? TokenStreamCursor.unquoteAndUnescape(text(), this)
                : text();
        pos++;
        // Engine convention: a variable's span covers `$name`, dollar inclusive.
        return new Variable(name, null, null, spanOf(dollarTok, pos - 1));
    }

    // -------------------------------------------------------------------
    // Collection
    // -------------------------------------------------------------------

    /**
     * {@code '[' (expr (',' expr)*)? ']'} &rarr; {@link PureCollection}.
     * The empty form {@code []} is legal and yields an empty collection.
     * Trailing commas are <em>not</em> permitted &mdash; engine rejects
     * them and C.1 follows suit so corpora remain byte-comparable.
     */
    private ValueSpecification parseCollection() {
        int openTok = pos;
        pos++; // consume '['
        boundedDepth++;
        try {
            return parseCollectionBody(openTok);
        } finally {
            boundedDepth--;
        }
    }

    private ValueSpecification parseCollectionBody(int openTok) {
        List<ValueSpecification> values = new ArrayList<>();
        if (!atEnd() && peek() == TokenType.BRACKET_CLOSE) {
            pos++;
            return new PureCollection(values, spanOf(openTok, pos - 1));
        }
        // M3 RANGE LITERAL (platform dialect; PCT range.pure):
        // [start:stop(:step)] and [:stop] desugar at parse to the
        // range(...) natives they mean — grammar sugar, not a node.
        if (!dialect.refusesPlatformDialect()
                && peek() == TokenType.COLON) {
            pos++;   // ':'  — the [:stop] form, start defaults 0
            ValueSpecification stop = parseCombinedExpression();
            expect(TokenType.BRACKET_CLOSE,
                    "expected ']' to close range literal");
            return new AppliedFunction("range", List.of(
                    new CInteger(0L), stop), List.of(),
                    spanOf(openTok, pos - 1), false, false, true);
        }
        // STRICT surfaces use the engine's expressionsArray production —
        // `expression`, which has no top-level arithmetic/boolean parts:
        // [1 + 2] refuses there (adversarial audit 1c, oracle-verified).
        // The pure dialect keeps the wider combinedExpression.
        values.add(dialect.refusesPlatformDialect() ? parseExpression() : parseCombinedExpression());
        if (!dialect.refusesPlatformDialect()
                && peek() == TokenType.COLON) {
            pos++;   // ':' — [start:stop(:step)]
            List<ValueSpecification> args = new ArrayList<>();
            args.add(values.get(0));
            args.add(parseCombinedExpression());
            if (peek() == TokenType.COLON) {
                pos++;
                args.add(parseCombinedExpression());
            }
            expect(TokenType.BRACKET_CLOSE,
                    "expected ']' to close range literal");
            return new AppliedFunction("range", args, List.of(),
                    spanOf(openTok, pos - 1), false, false, true);
        }
        while (!atEnd() && peek() == TokenType.COMMA) {
            pos++; // consume ','
            if (!atEnd() && peek() == TokenType.BRACKET_CLOSE) {
                throw error("trailing comma in collection literal");
            }
            values.add(dialect.refusesPlatformDialect() ? parseExpression() : parseCombinedExpression());
        }
        expect(TokenType.BRACKET_CLOSE, "expected ']' to close collection literal");
        // Engine convention: a literal collection's span covers `[...]`, brackets inclusive.
        return new PureCollection(values, spanOf(openTok, pos - 1));
    }

    // -------------------------------------------------------------------
    // Parenthesised grouping
    // -------------------------------------------------------------------

    /**
     * {@code '(' expr ')'} &rarr; {@code expr}. Grouping is structural
     * sugar that disappears at parse time; the AST carries no
     * {@code Grouping} node because the only semantic role of parens
     * is precedence override, which has no operator-level consumer
     * until C.3.
     */
    private ValueSpecification parseParenthesised() {
        pos++; // consume '('
        // parseCombinedExpression (not parseExpression) so the full
        // operator grammar works inside parens — this is what gives
        // users a way to override Pure's flat-arithmetic precedence,
        // e.g. '1 + (2 * 3)' to force right-side grouping.
        ValueSpecification inner = parseCombinedExpression();
        expect(TokenType.PAREN_CLOSE, "expected ')' to close parenthesised expression");
        // Parens are a FLATTEN BOUNDARY on the wire: engine folds `a - b - 7` into one
        // 3-operand collection but keeps `(a - b) - 7` nested. Mark, don't wrap.
        if (inner instanceof AppliedFunction af) {
            return af.asGrouped();
        }
        return inner;
    }

    // -------------------------------------------------------------------
    // Qualified-name dispatch (PackageableElementPtr / prefix AppliedFunction)
    // -------------------------------------------------------------------

    /**
     * Consume an identifier or {@code ::}-qualified name at expression
     * position. Lookahead on the trailing token decides the shape:
     * {@code PAREN_OPEN} &rarr; prefix {@link AppliedFunction}; anything
     * else &rarr; bare {@link PackageableElementPtr}. The
     * method-on-receiver case ({@code MyClass.all()}) is handled via
     * the dot postfix on top of a {@link PackageableElementPtr}
     * receiver &mdash; the postfix machinery is the same one used for
     * variable receivers.
     */
    /**
     * Parse an identifier-starting primary. Grammar branches (in
     * lookahead order &mdash; engine-lite calls this
     * {@code parseInstanceReference}):
     *
     * <ul>
     *   <li><strong>Unit name</strong>: {@code Mass~kilogram} &mdash;
     *       qualified name followed by {@code ~} and an identifier.
     *       Emits a {@link PackageableElementPtr} whose
     *       {@code fullPath} embeds the unit marker verbatim
     *       ({@code "Mass~kilogram"}), matching engine-lite.
     *       Unit-syntax must be handled HERE rather than in the
     *       {@code TILDE} case of {@link #parsePrimary()} because
     *       {@code ~} at the start of an expression is a column
     *       builder, not a unit.</li>
     *   <li><strong>Milestoning class access</strong>:
     *       {@code Person.all()} &rarr;
     *       {@code AppliedFunction("getAll", [PackageableElementPtr])};
     *       {@code Person.all(%2024-01-01)} adds the date as a
     *       second argument. Engine-lite uses the {@code "getAll"}
     *       function name (not {@code "all"}) to match the Pure
     *       stdlib resolved-overload signature; matching that name
     *       keeps downstream binding tables working.
     *       {@code allVersions()} and {@code allVersionsInRange}
     *       follow the same pattern with their own {@code "get"}-
     *       prefixed names.</li>
     *   <li><strong>Top-level function call</strong>:
     *       {@code my::pkg::add(x, y)} &rarr;
     *       {@code AppliedFunction("my::pkg::add", [x, y])}
     *       (qualifiedName followed by PAREN_OPEN).</li>
     *   <li><strong>Bare class reference</strong>: {@code Person}
     *       &rarr; {@code PackageableElementPtr("Person")} (no
     *       suffix).</li>
     * </ul>
     */
    /** {@code 5 RomanLength~Pes}: a number followed by a unit path is the
     * engine grammar's unitInstanceLiteral — spelled as its constructor,
     * {@code newUnit(RomanLength~Pes, 5)} (m3 essential/lang/unit). */
    private ValueSpecification unitLiteral(ValueSpecification number) {
        if (unitPathEnd(pos) < 0) {
            return number;
        }
        // legend-pure grammar only: the ENGINE's parser refuses unit
        // instances (DomainParseTreeWalker — its exact message, for the
        // rejection-parity pin)
        if (dialect().refusesLiteExtensions()) {
            throw TokenStreamCursor.throwAt(tokens, pos, "Unit instance not supported");
        }
        return new AppliedFunction("newUnit", List.of(parseQualifiedNameStart(), number));
    }

    private ValueSpecification parseQualifiedNameStart() {
        int fqnStart = pos;
        String fqn = parseQualifiedName();

        // Unit name: 'Mass~kilogram'. The TILDE here cannot be a
        // column builder (that requires starting position) nor a
        // column binding within a ColSpec (which has its own parse
        // path). Engine-lite attaches the unit part with '~' as a
        // literal separator in the name string.
        if (!atEnd() && peek() == TokenType.TILDE
                && pos + 1 < tokens.count()
                && isFqnSegmentToken(tokens.type(pos + 1))) {
            pos++; // consume '~'
            String unitPart = text();
            pos++;
            fqn = fqn + "~" + unitPart;
        }

        int fqnEnd = pos - 1;
        // Class.all()/.allVersions()/.allVersionsInRange(...) path.
        // Engine-lite emits 'getAll' / 'getAllVersions' /
        // 'getAllVersionsInRange' (note the 'get' prefix), matching
        // the resolved stdlib overload names. Using the same names
        // keeps downstream binding tables uniform across the two
        // parsers.
        if (!atEnd() && peek() == TokenType.DOT) {
            int savedPos = pos;
            pos++; // consume '.'
            if (!atEnd()) {
                TokenType t = peek();
                // commit to the all-call families ONLY when '(' follows —
                // an enum value literally named 'all' (a::E.all) is a
                // plain member access in the engine (adversarial audit
                // divergence 5, oracle-verified); without the lookahead
                // this arm demanded '(' and refused it
                boolean called = peek(1) == TokenType.PAREN_OPEN;
                if (t == TokenType.ALL && called) {
                    pos++;
                    return parseAllCall(fqn, savedPos, spanOf(fqnStart, fqnEnd));
                }
                if (t == TokenType.ALL_VERSIONS_IN_RANGE && called) {
                    pos++;
                    return parseAllVersionsInRangeCall(fqn, savedPos, spanOf(fqnStart, fqnEnd));
                }
                if (t == TokenType.ALL_VERSIONS) {
                    // allVersions is legal WITHOUT parens (Person.allVersions)
                    pos++;
                    return parseAllVersionsCall(fqn, savedPos, spanOf(fqnStart, fqnEnd));
                }
            }
            // Not an all-like method -- backtrack; the outer postfix
            // loop (parseExpression) will re-read the '.' as a
            // regular property/method access on the
            // PackageableElementPtr / EnumValue shape.
            pos = savedPos;
        }

        if (!atEnd() && peek() == TokenType.PAREN_OPEN) {
            List<ValueSpecification> args = parseArgList();
            // Top-level call: span = the function-name tokens (engine call convention).
            AppliedFunction call = new AppliedFunction(
                    com.legend.protocol.Protocol.unquotePath(fqn), args,
                    java.util.List.of(), spanOf(fqnStart, fqnEnd));
            // the quoted-code fold: compileLegendValueSpecification('literal')
            // parses HERE — strings die at the parser. The payload parses
            // at the HOST's level: the engine's native uses "the user
            // grammar" (LegendCompile), and this parse's dialect IS the
            // host's user grammar — a lite model may quote lite syntax.
            ValueSpecification folded = QuotedSpecParser.fold(call, dialect(),
                    this::constString);
            if (folded == null) {
                // the GRAMMAR quote/eval fold (compileLegendGrammar over a
                // functions-only payload) — same place, same rule
                folded = QuotedSpecParser.foldGrammar(call, this::constString);
            }
            return folded != null ? folded : call;
        }
        // Engine convention: a packageableElementPtr spans its FQN tokens.
        return new PackageableElementPtr(fqn, spanOf(fqnStart, fqnEnd));
    }

    /**
     * Parse the argument list of {@code Class.all(...)}. Pre-condition:
     * cursor is on the opening {@code (}. Milestoning arguments ({@code %date},
     * {@code %latest}, {@code $variable}) are the only values accepted in this
     * position; any other value would fail in the type-checker so we reject
     * early via {@link #parseMilestoningExpression}. Up to two milestoning
     * args are legal (business-from, business-thru).
     */
    private AppliedFunction parseAllCall(String fqn, int dotTok,
            com.legend.protocol.SourceInfo fqnSpan) {
        int nameTok = pos - 1;                       // the 'all' token
        expect(TokenType.PAREN_OPEN, "expected '(' after '.all'");
        List<ValueSpecification> args = new ArrayList<>();
        args.add(new PackageableElementPtr(fqn, fqnSpan));
        if (!atEnd() && peek() != TokenType.PAREN_CLOSE
                && !opensMilestoningLiteral(peek())) {
            // NON-literal first argument: the engine's allFunction grammar
            // does not match — the expression is a PROPERTY CALL named
            // 'all' with general arguments, spanning the name token only
            // (adversarial byte gate caught the getAll mis-wire)
            return propertyFormCall("all", nameTok, args);
        }
        if (!atEnd() && peek() != TokenType.PAREN_CLOSE) {
            args.add(parseMilestoningExpression());
            if (!atEnd() && peek() == TokenType.COMMA) {
                pos++;
                args.add(parseMilestoningExpression());
            }
        }
        expect(TokenType.PAREN_CLOSE, "expected ')' to close '.all(...)'");
        // Engine convention (ProbeWireShapes cPtr): getAll spans `.all()` — DOT to close-paren.
        return new AppliedFunction("getAll", args, java.util.List.of(),
                spanOf(dotTok, pos - 1));
    }

    /** The engine's property-call fallback for all/allVersionsInRange with
     *  non-literal arguments: general args after the receiver, wire
     *  {@code {"_type":"property"}} via the propertyCall marker. */
    private AppliedFunction propertyFormCall(String name, int nameTok,
            List<ValueSpecification> args) {
        while (!atEnd() && peek() != TokenType.PAREN_CLOSE) {
            args.add(parseCombinedExpression());
            if (!match(TokenType.COMMA)) {
                break;
            }
        }
        expect(TokenType.PAREN_CLOSE, "expected ')' to close '." + name + "(...)'");
        return new AppliedFunction(name, args, java.util.List.of(),
                spanOf(nameTok, nameTok), true, false, false);
    }

    private AppliedFunction parseAllVersionsCall(String fqn, int dotTok,
            com.legend.protocol.SourceInfo fqnSpan) {
        expect(TokenType.PAREN_OPEN, "expected '(' after '.allVersions'");
        expect(TokenType.PAREN_CLOSE, "'.allVersions()' takes no arguments");
        return new AppliedFunction("getAllVersions",
                List.of(new PackageableElementPtr(fqn, fqnSpan)),
                java.util.List.of(), spanOf(dotTok, pos - 1));
    }

    private AppliedFunction parseAllVersionsInRangeCall(String fqn, int dotTok,
            com.legend.protocol.SourceInfo fqnSpan) {
        int nameTok = pos - 1;
        int argsStart = pos;
        expect(TokenType.PAREN_OPEN, "expected '(' after '.allVersionsInRange'");
        if (!atEnd() && !opensMilestoningLiteral(peek())) {
            List<ValueSpecification> args = new ArrayList<>();
            args.add(new PackageableElementPtr(fqn, fqnSpan));
            return propertyFormCall("allVersionsInRange", nameTok, args);
        }
        ValueSpecification start = parseMilestoningExpression();
        expect(TokenType.COMMA, "expected ',' between range endpoints in '.allVersionsInRange'");
        ValueSpecification end = parseMilestoningExpression();
        expect(TokenType.PAREN_CLOSE, "expected ')' to close '.allVersionsInRange(...)'");
        if (dialect.refusesPlatformDialect()) {
            // engine-verbatim refusal (dialect quarantine): the range sweep
            // is pure-dialect only
            throw error(".allVersionsInRange" + compactText(argsStart, pos - 1)
                    + " is not supported");
        }
        return new AppliedFunction("getAllVersionsInRange",
                List.of(new PackageableElementPtr(fqn, fqnSpan), start, end),
                java.util.List.of(), spanOf(dotTok, pos - 1));
    }

    /**
     * Parse a single milestoning-position argument. The literal fast
     * paths keep their bespoke wire shapes; ANY OTHER expression parses
     * generally — the engine's {@code allFunction} arguments are plain
     * expressions and validation is the compiler's
     * ({@code Person.all(now())} is engine-legal; the old closed set
     * refused it — adversarial audit divergence 4, oracle-verified).
     */
    private ValueSpecification parseMilestoningExpression() {
        if (atEnd()) {
            throw error("expected milestoning expression (%date, %latest, or $variable)");
        }
        TokenType t = peek();
        if (t == TokenType.LATEST_DATE) return parseLatestDate();
        if (t == TokenType.DATE) return parseDateOrDateTime();
        if (t == TokenType.DOLLAR) return parseVariable();
        // ONLY reached when the FIRST argument was a milestoning literal —
        // the engine then closes the set (all(%latest, now()) refuses with
        // "Valid alternatives: ['%latest']"); a NON-literal first argument
        // never gets here (property-form dispatch in parseAllCall)
        throw error(
                "expected milestoning expression (%date, %latest, or $variable), got "
                + t + " ('" + safeText() + "')");
    }

    /** Whether the token can OPEN a milestoning literal — the dispatch
     *  between the getAll wire and the engine's property-call fallback
     *  (a::A.all(now()) is {"_type":"property"} on the wire, probed). */
    private static boolean opensMilestoningLiteral(TokenType t) {
        return t == TokenType.LATEST_DATE || t == TokenType.DATE
                || t == TokenType.DOLLAR;
    }

    // parseQualifiedName(), isFqnSegmentToken(TokenType), and
    // isIdentifierToken(TokenType) are inherited from
    // TokenStreamCursor; call sites reference them directly.

    // -------------------------------------------------------------------
    // Postfix operators: '.' and '->'
    // -------------------------------------------------------------------

    /**
     * Parse a {@code .} postfix &mdash; either a property access
     * ({@code receiver.prop}) or a method-on-receiver
     * ({@code receiver.method(args)}). The trailing {@code (} is the
     * lookahead that distinguishes the two. The receiver may be any
     * expression (variable, packageable ref, another postfix, etc.);
     * the postfix loop in {@link #parseExpression()} composes these
     * left-associatively so chains like {@code $x.foo.bar.baz()} all
     * work without per-level grammar duplication.
     *
     * <p>Property names may be quoted ({@code .'My Name'}): the lexer
     * emits {@code STRING} for the quoted form, the parser strips
     * quotes and resolves standard escapes via the shared
     * {@link #unescapeString(String) string-unescape} helper.
     */
    private ValueSpecification parseDotPostfix(ValueSpecification receiver) {
        int dotTok = pos;
        pos++; // consume '.'
        if (atEnd()) {
            throw error("expected property name after '.'");
        }
        int nameStart = pos;
        String name = readPropertyName();
        int nameEnd = pos - 1;
        if (!atEnd() && peek() == TokenType.PAREN_OPEN) {
            List<ValueSpecification> args = parseArgList();
            List<ValueSpecification> params = new ArrayList<>(1 + args.size());
            params.add(receiver);
            params.addAll(args);
            // Engine convention (ProbeWireShapes cPcall): a dot-spelled property CALL is a
            // property node on the wire, spanning the NAME token only — the dot..close-paren
            // span belongs solely to the .all()-family getAll desugar. The propertyCall
            // marker records the dot spelling for the emitter.
            return new AppliedFunction(name, params, List.of(), spanOf(nameStart, nameEnd), true);
        }
        // Enum-value form: 'MyEnum.VALUE' on a PackageableElementPtr
        // receiver. Engine-lite emits EnumValue rather than
        // AppliedProperty here; the disambiguation is purely
        // structural (property access on a class-name doesn't make
        // sense), but committing to the distinct AST shape at parse
        // time avoids a downstream re-walk to re-classify every
        // property access against the model.
        if (receiver instanceof PackageableElementPtr ptr) {
            // On the wire this is a property access on the ptr: keep both spans.
            return new EnumValue(ptr.fullPath(), name, ptr.pos(), spanOf(nameStart, nameEnd));
        }
        // Engine convention: a property access's span covers the property-NAME token only,
        // not the receiver or the dot (verified via ProbeWireShapes).
        return new AppliedProperty(receiver, name, spanOf(nameStart, nameEnd));
    }

    /**
     * Read a property name &mdash; either a bare identifier (the usual
     * case) or a quoted string literal ({@code 'My Name'}) so the
     * property can contain whitespace / punctuation. The quoted form
     * goes through the same unescape pipeline as a top-level
     * {@link CString} literal.
     */
    private String readPropertyName() {
        TokenType t = peek();
        // STRING must come FIRST: it is also a member of IDENTIFIER_TOKENS
        // (the element parser admits quoted strings as identifiers in
        // some keyword positions), so the bare-identifier branch below
        // would otherwise capture the quoted form including its outer
        // quotes \u2014 yielding {@code "'My Name'"} as the property name
        // instead of {@code "My Name"}.
        if (t == TokenType.STRING) {
            String raw = text();
            if (raw.length() < 2 || raw.charAt(0) != '\'' || raw.charAt(raw.length() - 1) != '\'') {
                throw error("malformed quoted property: missing surrounding quotes");
            }
            String name = unescapeString(raw.substring(1, raw.length() - 1));
            pos++;
            return name;
        }
        if (isIdentifierToken(t)) {
            String name = text();
            pos++;
            return name;
        }
        throw error("expected property name (identifier or 'quoted name') after '.'");
    }

    /**
     * Parse an {@code ->} postfix: {@code receiver->fn(args)} or
     * {@code receiver->my::pkg::fn(args)}. Desugars to
     * {@code AppliedFunction(fn, [receiver, args...])}, identical to
     * the method-on-receiver desugaring; this is the intended
     * uniformity (see {@link AppliedFunction}'s divergence note).
     */
    private AppliedFunction parseArrowPostfix(ValueSpecification receiver) {
        pos++; // consume '->'
        int fnStart = pos;
        String fn = parseQualifiedName();
        int fnEnd = pos - 1;
        if (peek() != TokenType.PAREN_OPEN) {
            throw error("expected '(' after arrow-call function name '" + fn + "'");
        }
        List<ValueSpecification> args = parseArgList();
        List<ValueSpecification> params = new ArrayList<>(1 + args.size());
        params.add(receiver);
        params.addAll(args);
        // Engine convention: a call's span covers the function-NAME token only,
        // not the receiver, arrow, or argument parens (verified via ProbeWireShapes).
        // CALLABLE names unquote (PureGrammarParserUtility.fromIdentifier —
        // 08-05 audit 2.6: $x->'pkg::abs'() kept its quotes on the wire here
        // while the dot-path unquoted; Protocol.unquotePath is quote-aware)
        return new AppliedFunction(com.legend.protocol.Protocol.unquotePath(fn),
                params, List.of(), spanOf(fnStart, fnEnd));
    }

    // -------------------------------------------------------------------
    // Argument list (shared by prefix call, method, and arrow)
    // -------------------------------------------------------------------

    /**
     * {@code '(' (expr (',' expr)*)? ')'}. The empty form {@code ()}
     * is legal and yields an empty list. Trailing commas are not
     * permitted, matching engine and {@link #parseCollection()}.
     * Pre-condition: cursor is on {@code PAREN_OPEN}.
     */
    private List<ValueSpecification> parseArgList() {
        pos++; // consume '('
        boundedDepth++;
        try {
            return parseArgListBody();
        } finally {
            boundedDepth--;
        }
    }

    private List<ValueSpecification> parseArgListBody() {
        List<ValueSpecification> args = new ArrayList<>();
        if (!atEnd() && peek() == TokenType.PAREN_CLOSE) {
            pos++;
            return args;
        }
        args.add(parseCombinedExpression());
        while (!atEnd() && peek() == TokenType.COMMA) {
            pos++; // consume ','
            if (!atEnd() && peek() == TokenType.PAREN_CLOSE) {
                throw error("trailing comma in argument list");
            }
            args.add(parseCombinedExpression());
        }
        expect(TokenType.PAREN_CLOSE, "expected ')' to close argument list");
        return args;
    }

    // -------------------------------------------------------------------
    // New-instance: ^my::pkg::Class<T1, T2>(prop1=val1, prop2=val2)
    // -------------------------------------------------------------------

    /**
     * Parse a {@code ^}-prefixed struct literal and desugar to a
     * function call. Pre-condition: cursor is on {@code NEW_SYMBOL}.
     * Grammar:
     * <pre>
     *   newInstance = '^' qualifiedName typeArguments? '(' keyBindings? ')'
     *   typeArguments = '&lt;' qualifiedName (',' qualifiedName)* '&gt;'
     *   keyBindings   = keyExpression (',' keyExpression)*
     *   keyExpression = identifier ('=' | '+=') combinedExpression
     * </pre>
     *
     * <h3>Desugar shape</h3>
     *
     * <p>The surface {@code ^Foo(x=1)} desugars at parse time to:
     * <pre>
     *   AppliedFunction("new", [
     *     PackageableElementPtr("Foo"),
     *     NewInstance("Foo", [], {x -&gt; KeyExpression(CInteger(1), false)})
     *   ])
     * </pre>
     * Matches engine-lite's parse-time representation
     * ({@code PureQueryParser.parseExpressionInstance}). The outer
     * {@link AppliedFunction} wrapper makes new-expressions slot into
     * the uniform function-dispatch pipeline (typechecker, scope
     * registration, codegen all dispatch on function name); the inner
     * {@link NewInstance} record carries the named-binding data that a
     * positional-arg function call cannot express.
     *
     * <p>This mirrors how {@code let x = v} desugars to
     * {@code AppliedFunction("letFunction", ...)} (see
     * {@link #parseLetExpression()}): both are stdlib functions whose
     * surface syntax is sugar. Pure's stdlib has
     * {@code new_Class_1__String_1__KeyExpression_MANY__T_1_} as a
     * real callable function; {@code ^Foo(...)} is sugar for invoking
     * it.
     *
     * <p>The empty form {@code ^Foo()} is legal (zero bindings) and
     * compiles to a default-constructed instance. Trailing commas in
     * the binding list are rejected, matching the
     * {@link #parseArgList()} and {@link #parseCollection()}
     * conventions.
     *
     * <p>Type arguments are stored as source-level FQN strings. Nested
     * generics ({@code ^List&lt;Pair&lt;A, B&gt;&gt;(...)}) require
     * lexing {@code >>} as two tokens (which the lexer does), but the
     * current parser only consumes one level of {@code <...>}. Real
     * corpora can extend this when needed; the
     * {@link NewInstance#typeArguments()} field is already in place.
     */
    private AppliedFunction parseNewInstance() {
        pos++; // consume '^'
        // Two grammars share the '^' prefix:
        //  (1) '^ClassName<T>(f=v, ...)' — fresh instance by class
        //      name. Receiver slot of the emitted 'new' call is a
        //      {@link PackageableElementPtr} pointing at the class.
        //  (2) '^$existing(f=v, ...)' — copy-with-update: produce a
        //      new instance structurally equal to $existing except
        //      for the listed field overrides. Receiver slot is a
        //      {@link Variable} pointing at the source binding;
        //      NewInstance's className is the empty string because
        //      the class is recovered from the variable's static
        //      type at type-check time.
        //
        // Engine-lite encodes case (2) by stuffing "$name" into a
        // PackageableElementPtr. That's a hack — a $-prefixed FQN is
        // structurally a lie about what the receiver is. We improve
        // by emitting a real Variable node in the receiver slot,
        // which keeps the rest of the pipeline free of the
        // $-prefix-sniffing dispatch engine-lite has to do downstream.
        // Function name stays "new" for binding-table parity.
        ValueSpecification receiver;
        String className;
        List<TypeExpression> typeArgs = List.of();
        List<String> typeMultArgs = new ArrayList<>();
        if (!atEnd() && peek() == TokenType.DOLLAR) {
            if (dialect().refusesLiteExtensions()) {
                // ^$x(...) copy-with-update is PURE-DIALECT spelling
                // (legend-pure uses it everywhere) that the ENGINE wire
                // grammar cannot parse (its walker NPEs — oracle-probed);
                // adopted on the LITE product surface, refused drop-in
                throw error("copy-with-update (^$var) is not supported");
            }
            pos++; // consume '$'
            if (!isFqnSegmentToken(peek())) {
                throw error("expected variable name after '^$' in copy-with-update");
            }
            String varName = text();
            pos++;
            receiver = new Variable(varName);
            className = ""; // class recovered from $var's static type at typecheck
        } else {
            int cnStart = pos;
            className = parseQualifiedName();
            int cnEnd = pos - 1;
            if (!atEnd() && peek() == TokenType.LESS_THAN) {
                typeArgs = parseTypeArguments(typeMultArgs);
            }
            receiver = new PackageableElementPtr(className, spanOf(cnStart, cnEnd));
            // ^X(10)(text = …) — type-variable VALUES (m3 GenericType.typeVariableValues;
            // legend-pure new.pure): parsed, not carried — an instance's type-variable
            // values are not modeled (the bodies that read $x are walled by decision)
            if (!dialect().refusesPlatformDialect()
                    && !atEnd() && peek() == TokenType.PAREN_OPEN && parenGroupFollowedByParen()) {
                pos++; // '('
                while (!atEnd() && peek() != TokenType.PAREN_CLOSE) {
                    parseCombinedExpression();
                    if (!atEnd() && peek() == TokenType.COMMA) {
                        pos++;
                    }
                }
                expect(TokenType.PAREN_CLOSE, "expected ')' to close type-variable values");
            }
        }
        // OLD-pure named form '^Person klp (...)': the instance NAME parses and DROPS —
        // the wire's name slot stays "" (probe "pf named new and store tref")
        if (!atEnd() && isFqnSegmentToken(peek()) && peek(1) == TokenType.PAREN_OPEN) {
            pos++;
        }
        expect(TokenType.PAREN_OPEN, "expected '(' after class name or $variable in ^NewInstance");
        // a LIST: the engine keeps EVERY binding including duplicate
        // keys (deep audit #2 1b — a map silently collapsed them)
        List<com.legend.protocol.spec.NewInstance.KeyBinding> properties =
                new ArrayList<>();
        if (!atEnd() && peek() == TokenType.PAREN_CLOSE) {
            pos++;
            return wrapNewInstance(receiver, className, typeArgs, typeMultArgs, properties);
        }
        // Disambiguate explicit-bindings vs positional-cast form.
        // Positional cast: ^Class($srcExpr) — the body is a single
        // ValueSpecification, no property bindings.
        //
        // Rule: bindings ALWAYS start with an identifier (the property
        // name); cast bodies start with anything else (a $variable, a
        // literal, a parenthesised expression, etc.). Any identifier-
        // led body goes through the binding path so a missing '='
        // surfaces the canonical "expected '='" error instead of a
        // confusing "expected ')' to close cast" message. This excludes
        // bare-identifier cast sources like ^Class(somePackageableThing) —
        // not a documented form; users wanting a cast write $x.
        //
        // Cast is only legal in the class-literal form (className non-
        // empty), never in the copy-with-update ^$var(...) form.
        if (!className.isEmpty() && !isFqnSegmentToken(peek())
                && !(peek() == TokenType.STRING && pos + 1 < tokens.count()
                        && tokens.type(pos + 1) == TokenType.EQUAL)) {
            ValueSpecification src = parseCombinedExpression();
            expect(TokenType.PAREN_CLOSE,
                    "expected ')' to close ^" + className + "($src) positional cast");
            return new AppliedFunction(
                    AppliedFunction.NEW,
                    List.of(
                            receiver,
                            new NewInstanceCast(className, typeArgs, src)));
        }
        parseAndPutKeyExpression(properties);
        while (!atEnd() && peek() == TokenType.COMMA) {
            pos++; // consume ','
            if (!atEnd() && peek() == TokenType.PAREN_CLOSE) {
                throw error("trailing comma in ^NewInstance binding list");
            }
            parseAndPutKeyExpression(properties);
        }
        expect(TokenType.PAREN_CLOSE, "expected ')' to close ^NewInstance");
        return wrapNewInstance(receiver, className, typeArgs, typeMultArgs, properties);
    }

    /**
     * Build the {@code AppliedFunction("new", [receiver, NewInstance])}
     * wrapper. The {@code receiver} is a {@link PackageableElementPtr}
     * for the class-literal form and a {@link Variable} for the
     * {@code ^$var(...)} copy-with-update form.
     */
    private AppliedFunction wrapNewInstance(
            ValueSpecification receiver,
            String className,
            List<TypeExpression> typeArgs,
            List<String> typeMultArgs,
            List<com.legend.protocol.spec.NewInstance.KeyBinding> properties) {
        return new AppliedFunction(
                AppliedFunction.NEW,
                List.of(
                        receiver,
                        new NewInstance(className, typeArgs, typeMultArgs, properties)));
    }

    /**
     * Parse one {@code key (= | +=) value} binding and insert into
     * the accumulating map. The {@code +=} variant sets
     * {@link KeyExpression#isAdd()} to {@code true} so the typechecker
     * can distinguish the append form from the assign form
     * downstream.
     *
     * <p>Duplicate-key behaviour: {@link Map#put} silently overwrites,
     * matching engine-lite. Engine-pure also accepts duplicates
     * (verified in {@code NewValidator} which iterates bindings but
     * never tracks seen keys), so the silent last-wins is
     * cross-engine-consistent.
     */
    private void parseAndPutKeyExpression(
            List<com.legend.protocol.spec.NewInstance.KeyBinding> properties) {
        // quoted keys are legal: ^A('first name' = ...) — the wire keeps the RAW
        // quoted spelling (corpus DIFF: key.value = "'firstname'")
        if (peek() == TokenType.STRING && pos + 1 < tokens.count()
                && tokens.type(pos + 1) == TokenType.EQUAL) {
            String key = text();
            pos += 2;                               // name + '='
            ValueSpecification value = parseCombinedExpression();
            properties.add(new com.legend.protocol.spec.NewInstance
                    .KeyBinding(key, new KeyExpression(value, false, false)));
            return;
        }
        if (!isFqnSegmentToken(peek())) {
            throw error("expected property name in ^NewInstance binding");
        }
        // Dotted property paths: '^Foo(addr.city = "NYC")' sets a
        // nested field atomically. Engine-lite admits arbitrary
        // {@code IDENT (DOT IDENT)*} paths here; we match by
        // concatenating segments with '.' into the key. Downstream
        // (TypeChecker) walks the property chain against the class's
        // declared properties. Keeping the segments joined into a
        // single string matches engine-lite's Map<String,
        // ValueSpecification> representation; the dot-separation in
        // the key is the only signal of nesting.
        StringBuilder key = new StringBuilder(text());
        pos++;
        while (pos + 1 < tokens.count()
                && peek() == TokenType.DOT
                && isFqnSegmentToken(tokens.type(pos + 1))) {
            pos++; // consume '.'
            key.append('.').append(text());
            pos++;
        }
        boolean isAdd = false;
        if (!atEnd() && peek() == TokenType.PLUS) {
            isAdd = true;
            pos++; // consume '+'
        }
        expect(TokenType.EQUAL,
                "expected '" + (isAdd ? "+=" : "=")
                + "' after property name '" + key
                + "' in ^NewInstance binding");
        ValueSpecification value = parseCombinedExpression();
        properties.add(new com.legend.protocol.spec.NewInstance
                .KeyBinding(key.toString(),
                        new KeyExpression(value, isAdd, false)));
    }

    /**
     * Parse a {@code <T1, T2, ...>} type-argument list on a
     * {@code ^Class<...>(...)} new-instance expression. Returns the
     * arguments as structured {@link TypeExpression}s &mdash; the
     * same shape produced everywhere else type text occurs in the
     * AST. Cursor is on the opening {@code <}; advances past the
     * closing {@code >}.
     */
    private List<TypeExpression> parseTypeArguments() {
        return parseTypeArguments(new ArrayList<>());
    }

    private List<TypeExpression> parseTypeArguments(List<String> multArgsOut) {
        pos++; // consume '<'
        List<TypeExpression> args = new ArrayList<>();
        if (!atEnd() && peek() == TokenType.GREATER_THAN) {
            pos++;
            return args;
        }
        // TestClass<|1>: multiplicity arguments with NO type arguments
        if (!atEnd() && peek() == TokenType.PIPE) {
            pos++;
            multArgsOut.add(parseMultiplicityArgumentText());
            while (!atEnd() && peek() == TokenType.COMMA) {
                pos++;
                multArgsOut.add(parseMultiplicityArgumentText());
            }
            expect(TokenType.GREATER_THAN, "expected '>' to close type arguments");
            return args;
        }
        args.add(parseType());
        while (!atEnd() && peek() == TokenType.COMMA) {
            pos++; // consume ','
            args.add(parseType());
        }
        // MULTIPLICITY type parameters (real M3: cast(@Property<Nil,Any|*>),
        // ^Result<TabularDataSet|1>(...)): CAPTURED — the wire carries them as
        // genericType.multiplicityArguments (harness DIFF on transformMappingClass;
        // the old parse-and-discard here was audit-21a's failure mode again).
        if (!atEnd() && peek() == TokenType.PIPE) {
            pos++;
            multArgsOut.add(parseMultiplicityArgumentText());
            while (!atEnd() && peek() == TokenType.COMMA) {
                pos++;
                multArgsOut.add(parseMultiplicityArgumentText());
            }
        }
        expect(TokenType.GREATER_THAN, "expected '>' to close type arguments");
        return args;
    }

    // -------------------------------------------------------------------
    // Lambdas: { p, q | body } / x | body / | body
    // -------------------------------------------------------------------

    /**
     * Braced lambda. Grammar:
     * <pre>
     *   lambdaFunction = '{' params? '|' codeBlock '}'
     *   params         = identifier (',' identifier)*
     * </pre>
     *
     * <p>Body is a code block &mdash; multiple {@code ;}-separated
     * statements are admitted, the lambda's value is the last
     * statement. Typed parameters
     * ({@code {p: Integer[1] | ...}}) are deferred to C.5; the parser
     * fails with an informative error if a {@code :} follows a lambda
     * parameter name in C.4.
     *
     * <p>Pre-condition: cursor is on {@link TokenType#BRACE_OPEN}.
     */
    private LambdaFunction parseLambdaFunction() {
        pos++; // consume '{'
        List<Variable> params = new ArrayList<>();
        if (!atEnd() && peek() != TokenType.PIPE) {
            params.add(parseLambdaParam());
            while (!atEnd() && peek() == TokenType.COMMA) {
                pos++; // consume ','
                params.add(parseLambdaParam());
            }
        }
        int pipeTok = pos;
        expect(TokenType.PIPE, "expected '|' to separate lambda parameters from body");
        pushConstScope(params);
        List<ValueSpecification> body;
        try {
            body = parseCodeBlockUntil(TokenType.BRACE_CLOSE);
        } finally {
            constStrings.pop();
        }
        if (body.isEmpty()) {
            throw error("lambda body must contain at least one statement");
        }
        int bodyEnd = pos - 1;
        expect(TokenType.BRACE_CLOSE, "expected '}' to close lambda");
        // Engine convention (ProbeWireShapes cBrace, verified against token columns): a
        // braced lambda spans PIPE..body-end — the same rule as the shorthand form; braces
        // are excluded on both sides.
        return new LambdaFunction(params, body, spanOf(pipeTok, bodyEnd));
    }

    /**
     * Parse one lambda parameter. Two forms:
     *
     * <ul>
     *   <li>Untyped: just the identifier. Produces
     *       {@code Variable(name)} with {@code typeName} and
     *       {@code multiplicity} both {@code null}.</li>
     *   <li>Typed: {@code identifier ':' type multiplicity}. Produces
     *       a fully-populated {@link Variable}. The multiplicity is
     *       <em>required</em> after the type &mdash; engine grammar
     *       does not admit {@code {p: Integer | body}} (no
     *       multiplicity), only {@code {p: Integer[1] | body}}.</li>
     * </ul>
     */
    private Variable parseLambdaParam() {
        if (!isFqnSegmentToken(peek())) {
            throw error("expected lambda parameter name");
        }
        int nameTok = pos;
        String name = text();
        pos++;
        if (peek() != TokenType.COLON) return new Variable(name);
        advance();
        TypeExpression type = parseType();
        Multiplicity multiplicity = parseMultiplicity();
        // Engine convention (ProbeWireShapes cLambda2): a TYPED lambda parameter spans the
        // whole `name: Type[mult]` declaration; an untyped one carries no span at all.
        return new Variable(name, type, multiplicity, spanOf(nameTok, pos - 1));
    }

    /**
     * Shorthand single-param lambda. Two source forms:
     *
     * <ul>
     *   <li>Untyped: {@code x | body} &mdash; common inline
     *       predicate, e.g. {@code $xs->filter(p | $p.age > 21)}.</li>
     *   <li>Typed: {@code x: Type[mult] | body} &mdash; less common
     *       at this position but admitted for grammar parity with
     *       the braced form.</li>
     * </ul>
     *
     * <p>Body is one expression, not a code block. Pre-condition:
     * cursor is on the identifier; the caller in {@link #parsePrimary()}
     * has already verified via {@link #looksLikeTypedLambdaParam()}
     * (typed) or a 1-token IDENT+PIPE lookahead (untyped) that this
     * dispatch is correct.
     */
    private LambdaFunction parseSingleParamLambda() {
        Variable param = parseLambdaParam();
        int pipeTok = pos;
        expect(TokenType.PIPE, "expected '|' after shorthand lambda parameter");
        // The body is a CODE BLOCK per real Pure (lambdaPipe: PIPE
        // codeBlock), parsed as a DETERMINISTIC subset of ANTLR's
        // backtracking resolution of the grammar's ambiguity:
        //   - a body STARTING with 'let' commits to multi-statement —
        //     every statement requires its trailing ';' and the block
        //     runs to a closing token (corpus:
        //     filter(p | let n = ...; $n->at(0);))
        //   - an expression body stays single-statement; a ';'
        //     IMMEDIATELY followed by a closer is the codeBlock's own
        //     optional END_LINE (corpus: [d: Database[*]| f($d);]);
        //     any other ';' belongs to the ENCLOSING statement sequence
        //     ($x->map(e|$e+1); let b = ... must not swallow the let)
        List<ValueSpecification> body = new java.util.ArrayList<>();
        boolean letStart = !atEnd() && peek() == TokenType.LET;
        body.add(parseProgramLine());
        if (!atEnd() && peek() == TokenType.SEMI_COLON) {
            if (boundedDepth > 0 || letStart) {
                // BOUNDED context (call args / collection): no outer
                // statement can own a ';' here, so the greedy codeBlock
                // read is unambiguous. STATEMENT context: only a body
                // that STARTS with 'let' commits (the outer sequence owns
                // an expression body's ';').
                pos++; // the first statement's ';'
                while (!atEnd() && !isLambdaBodyTerminator(peek())) {
                    body.add(parseProgramLine());
                    if (atEnd() || peek() != TokenType.SEMI_COLON) {
                        throw error("expected ';' after statement in a"
                                + " multi-statement lambda body (real Pure"
                                + " requires it)");
                    }
                    pos++; // the REQUIRED trailing ';'
                }
            } else if (pos + 1 < tokens.count()
                    && isLambdaBodyTerminator(tokens.type(pos + 1))) {
                pos++; // the codeBlock's own trailing END_LINE
            }
        }
        // Engine convention (ProbeWireShapes cLambda/cLambda2, verified against token
        // columns): an inline lambda spans from the PIPE to the body's end.
        return new LambdaFunction(List.of(param), body, spanOf(pipeTok, pos - 1));
    }

    // -------------------------------------------------------------------
    // Type and multiplicity annotations (C.5)
    // -------------------------------------------------------------------

    // parseType() and parseMultiplicity() are inherited from
    // TokenStreamCursor as default methods. Earlier revisions of this
    // file carried bridge methods that constructed a per-call
    // TypeExpressionParser helper and copied its cursor back into our
    // 'pos' field; the interface design eliminates both the helper
    // class and the bridge.
    //
    // Call sites use parseType() directly. (The legacy alias
    // 'parseTypeExpression()' is gone — callers were updated.)

    /**
     * Speculative lookahead: does the cursor sit on a typed
     * single-param lambda shorthand ({@code x: Type[mult] | body})?
     * Used by {@link #parsePrimary()} to disambiguate IDENT+COLON
     * from any other identifier-then-colon source &mdash; e.g. a
     * future grammar extension where a colon could follow an
     * identifier in some other position.
     *
     * <p>Engine-lite (which also has no committed parser state to
     * roll back) does the same: save the position, try to skip
     * IDENT, COLON, type, optional multiplicity, and check for the
     * trailing PIPE. Restore the position regardless. Returning
     * {@code true} means the caller should commit to the typed
     * parse via {@link #parseSingleParamLambda()}.
     */
    private boolean looksLikeTypedLambdaParam() {
        int saved = pos;
        try {
            if (!isFqnSegmentToken(peek())) {
                return false;
            }
            pos++; // identifier
            if (peek() != TokenType.COLON) return false;
            advance();
            if (!skipTypeForLookahead()) {
                return false;
            }
            if (!atEnd() && peek() == TokenType.BRACKET_OPEN) {
                if (!skipMultiplicityForLookahead()) {
                    return false;
                }
            }
            return !atEnd() && peek() == TokenType.PIPE;
        } finally {
            pos = saved;
        }
    }

    private boolean skipTypeForLookahead() {
        // Bare ROW-STRUCT type ((id:Integer, v:Variant) — the PCT
        // fully-annotated lambda form) or FUNCTION type ({T[1]->R[1]}):
        // skip the balanced region; parseType() owns the real grammar.
        if (peek() == TokenType.PAREN_OPEN) {
            return skipBalancedForLookahead(TokenType.PAREN_OPEN, TokenType.PAREN_CLOSE);
        }
        if (peek() == TokenType.BRACE_OPEN) {
            return skipBalancedForLookahead(TokenType.BRACE_OPEN, TokenType.BRACE_CLOSE);
        }
        if (!isFqnSegmentToken(peek())) {
            return false;
        }
        pos++;
        while (!atEnd() && peek() == TokenType.PATH_SEPARATOR) {
            pos++;
            if (!isFqnSegmentToken(peek())) {
                return false;
            }
            pos++;
        }
        if (!atEnd() && peek() == TokenType.LESS_THAN) {
            int depth = 1;
            pos++;
            while (!atEnd() && depth > 0) {
                TokenType t = peek();
                if (t == TokenType.LESS_THAN) depth++;
                if (t == TokenType.GREATER_THAN) depth--;
                pos++;
            }
            if (depth != 0) return false;
        }
        return true;
    }

    private boolean skipBalancedForLookahead(TokenType open, TokenType close) {
        int depth = 1;
        pos++;   // the opener
        while (!atEnd() && depth > 0) {
            TokenType t = peek();
            if (t == open) depth++;
            if (t == close) depth--;
            pos++;
        }
        return depth == 0;
    }

    private boolean skipMultiplicityForLookahead() {
        if (peek() != TokenType.BRACKET_OPEN) return false;
        advance();
        while (!atEnd() && peek() != TokenType.BRACKET_CLOSE) {
            pos++;
        }
        if (atEnd()) return false;
        pos++; // ']'
        return true;
    }

    /**
     * Zero-param pipe-shorthand lambda {@code | body}. Body is one
     * expression. Common as a zero-arg thunk argument:
     * {@code $opt->orElse(| 'default')}.
     *
     * <p>Pre-condition: cursor is on {@link TokenType#PIPE}.
     */
    private LambdaFunction parseLambdaPipe() {
        int pipeTok = pos;
        pos++; // consume '|'
        // The body is a STATEMENT SEQUENCE per REAL Pure's grammar
        // (M3ParserGrammar.g4): codeBlock: programLine (';' (programLine ';')*)?
        // — the FIRST statement's ';' is optional; every SUBSEQUENT statement
        // REQUIRES its trailing ';'. "|let a = 1; $a" is invalid Pure.
        List<ValueSpecification> body = new java.util.ArrayList<>();
        pushConstScope(List.of());
        try {
            body.add(parseProgramLine());
            if (!atEnd() && peek() == TokenType.SEMI_COLON) {
                pos++; // the first statement's optional ';'
                while (!atEnd() && !isLambdaBodyTerminator(peek())) {
                    body.add(parseProgramLine());
                    if (atEnd() || peek() != TokenType.SEMI_COLON) {
                        throw error("expected ';' after statement in a multi-statement"
                                + " lambda body (real Pure requires it)");
                    }
                    pos++; // the REQUIRED trailing ';'
                }
            }
        } finally {
            constStrings.pop();
        }
        // Engine convention: pipe..body-end, the same rule as every inline lambda form.
        return new LambdaFunction(List.of(), body, spanOf(pipeTok, pos - 1));
    }

    /** Depth of contexts where a ';' cannot belong to an enclosing
     * statement (call argument lists, collection literals) — inside them
     * the unbraced-lambda codeBlock reads greedily, matching real Pure. */
    private int boundedDepth = 0;

    private static boolean isLambdaBodyTerminator(TokenType t) {
        return t == TokenType.PAREN_CLOSE || t == TokenType.COMMA
                || t == TokenType.BRACKET_CLOSE || t == TokenType.BRACE_CLOSE;
    }

    // -------------------------------------------------------------------
    // Column builders (C.6): ~name, ~[a, b], ~name:lambda, ~name:fn:fn
    // -------------------------------------------------------------------

    /**
     * Parse the tilde-column DSL. Grammar:
     * <pre>
     *   columnBuilders = '~' (colSpec | colSpecArray)
     *   colSpecArray   = '[' colSpec (',' colSpec)* ']'
     *   colSpec        = identifier (':' lambda (':' lambda)?)?
     * </pre>
     *
     * <p>Pre-condition: cursor is on {@link TokenType#TILDE}. Emits
     * a {@link ColumnInstance} subtype &mdash; either {@link ColSpec}
     * (single) or {@link ColSpecArray} (bracketed). Both flow up
     * through the normal {@link ValueSpecification} interface.
     *
     * <h3>Deferred forms</h3>
     *
     * <p>Engine grammar also admits a typed column spec
     * {@code ~name:Type[mult]} (used in relation-type declarations).
     * Disambiguating between {@code ~name:lambda} and
     * {@code ~name:type[mult]} requires the same speculative
     * lookahead as typed lambda parameters but with an additional
     * branch. Deferred to a follow-up phase &mdash; for now the
     * post-colon position parses as a lambda.
     *
     * <p>Engine's column-builder grammar also supports {@code FuncColSpec}
     * and {@code AggColSpec} as distinct stdlib-class variants. We
     * collapse those into a single {@link ColSpec} record with two
     * optional lambda slots; the type-checker (which knows the
     * enclosing function) dispatches downstream.
     */
    /** A fused mapping-command token ({@code ~filter}, {@code ~src}, …) read
     *  as an ORDINARY ColSpec whose column bears the command's name — same
     *  typed/lambda/span machinery as {@link #parseOneColSpec} (adversarial
     *  audit: the old special-case dropped the span, crashing the emitter on
     *  engine-legal input, and mis-parsed typed specs). The fused token's
     *  start column is the tilde, so the name anchor shifts one column right. */
    private ColSpec parseTildeCommandColSpec() {
        String name = text().substring(1);
        int cmdTok = pos;
        pos++;
        return parseColSpecTail(name, cmdTok, 1, List.of(), List.of());
    }

    private ColumnInstance parseColumnBuilders() {
        int tildeTok = pos;
        pos++; // consume '~'
        if (!atEnd() && peek() == TokenType.BRACKET_OPEN) {
            return parseColSpecArray();
        }
        return parseOneColSpec(tildeTok);
    }

    private ColSpec parseOneColSpec() {
        return parseOneColSpec(-1);
    }

    private ColSpec parseOneColSpec(int tildeTok) {
        // Column names can be either bare identifiers or single-quoted
        // strings (for names with whitespace / punctuation, e.g.
        // {@code ~'My Column'}). Engine grammar admits both forms
        // pervasively; quoted form is the canonical way to reference
        // a CSV/JDBC column whose name doesn't match identifier rules.
        // Mirrors readPropertyName's STRING-first dispatch (STRING is
        // also a member of IDENTIFIER_TOKENS, so the bare-identifier
        // branch would otherwise capture the quoted form with its
        // outer quotes intact).
        if (atEnd()) {
            throw error("expected column name after '~'");
        }
        // ~<<stereo>> {tag='v'} name:Type — annotations ride the colSpec VALUE as
        // ordinary stereotype/taggedValue arrays; the spec's span STARTS at the first
        // annotation token (probe "pf colspec annotations", corpus DIFFs)
        int annotStart = -1;
        List<com.legend.protocol.Protocol.PStereotype> csStereotypes = List.of();
        List<com.legend.protocol.Protocol.PTaggedValue> csTaggedValues = List.of();
        if ((peek() == TokenType.LESS_THAN && peek(1) == TokenType.LESS_THAN)
                || (peek() == TokenType.BRACE_OPEN && looksLikeTaggedValues())) {
            annotStart = pos;
            // HOST level: a decoration sub-parse inside a platform/lite spec
            // must not silently run at another level
            ElementParser sub = ElementParser.at(tokens, pos, dialect());
            csStereotypes = sub.parseStereotypes();
            csTaggedValues = sub.parseTaggedValues();
            pos = sub.pos();
        }
        int nameTokIdx = annotStart >= 0 ? annotStart : pos;
        TokenType nameTok = peek();
        String name;
        if (nameTok == TokenType.STRING) {
            String raw = text();
            if (raw.length() < 2 || raw.charAt(0) != '\''
                    || raw.charAt(raw.length() - 1) != '\'') {
                throw error("malformed quoted column name: missing surrounding quotes");
            }
            name = unescapeString(raw.substring(1, raw.length() - 1));
            pos++;
        } else if (isFqnSegmentToken(nameTok)) {
            name = text();
            pos++;
        } else {
            throw error("expected column name after '~'");
        }
        return parseColSpecTail(name, nameTokIdx, 0, csStereotypes, csTaggedValues);
    }

    /** Everything after a colspec's NAME — the typed attempt, the lambda
     *  forms, and the span rule — shared by the ordinary {@code ~name} path
     *  and the fused-command-token path. {@code startColShift} shifts the
     *  span's start column right (1 for a fused {@code ~filter} token whose
     *  start char is the tilde; engine anchors at the NAME char). */
    private ColSpec parseColSpecTail(String name, int nameTokIdx, int startColShift,
            List<com.legend.protocol.Protocol.PStereotype> csStereotypes,
            List<com.legend.protocol.Protocol.PTaggedValue> csTaggedValues) {
        LambdaFunction function1 = null;
        LambdaFunction function2 = null;
        if (!atEnd() && peek() == TokenType.COLON) {
            pos++; // consume ':'
            // ~name:Type[m] (TYPED spec, probe "typed colspec stmt") vs ~name:x|expr /
            // ~name:thunk() (lambda forms): ATTEMPT the type parse and accept it only
            // when a spec terminator follows — anything else backtracks to the lambda
            // path ('Target.all()' parses as the type 'Target' but the '.' refutes it)
            int colonMark = pos;
            // CHEAP token-shape pre-check first: the exception-backtracking attempt was
            // 2.2x parse-time on braced-lambda colspecs (corpus gate profile) — only
            // attempt the type parse when the tokens actually spell a type
            if (looksLikeColumnType()) {
                ColSpec typed = tryTypedColSpec(name, nameTokIdx, startColShift,
                        csStereotypes, csTaggedValues);
                if (typed != null) {
                    return typed;
                }
                pos = colonMark;
            }
            function1 = parseColumnLambda();
            if (!atEnd() && peek() == TokenType.COLON) {
                pos++; // consume ':'
                function2 = parseColumnLambda();
            }
        }
        // Engine convention (exact columns, twice mis-eyeballed): a colSpec ALWAYS
        // anchors at its NAME token — bare specs span the name alone, function-bearing
        // ones span name..body-end. The tilde is never included.
        com.legend.protocol.SourceInfo span = function1 == null
                ? colSpecSpan(nameTokIdx, nameTokIdx, startColShift)
                : colSpecSpan(nameTokIdx, pos - 1, startColShift);
        return new ColSpec(name, function1, function2, null, List.of(), false, span,
                null, null, csStereotypes, csTaggedValues);
    }

    private com.legend.protocol.SourceInfo colSpecSpan(int fromTok, int toTok,
            int startColShift) {
        com.legend.protocol.SourceInfo s = spanOf(fromTok, toTok);
        return startColShift == 0 ? s
                : new com.legend.protocol.SourceInfo(s.sourceId(), s.startLine(),
                        s.startColumn() + startColShift, s.endLine(), s.endColumn());
    }

    /** A '{' opening TAGGED VALUES ({profile.tag='v'}) rather than a lambda: the next
     *  tokens spell an identifier path followed by '.'. */
    private boolean looksLikeTaggedValues() {
        int k = pos + 1;
        if (k >= tokens.count() || !isFqnSegmentToken(tokens.type(k))) {
            return false;
        }
        k++;
        while (k + 1 < tokens.count() && tokens.type(k) == TokenType.PATH_SEPARATOR
                && isFqnSegmentToken(tokens.type(k + 1))) {
            k += 2;
        }
        return k < tokens.count() && tokens.type(k) == TokenType.DOT;
    }

    /** {@link #parseOneColSpec}'s TYPED attempt: a type (with optional multiplicity)
     *  followed immediately by a spec terminator, or {@code null} (cursor position is the
     *  caller's to restore). */
    /**
     * Zero-allocation lookahead: do the tokens after {@code :} spell a TYPE
     * ({@code FQN (<...>)? ((...))? ([mult])?}) followed immediately by a spec
     * terminator? Anything else — lambdas, thunk calls, literals — skips the typed
     * attempt entirely (hot path: no exceptions, no backtracking).
     */
    private boolean looksLikeColumnType() {
        int k = pos;
        int n = tokens.count();
        if (k >= n || !isFqnSegmentToken(tokens.type(k))) {
            return false;
        }
        k++;
        while (k + 1 < n && tokens.type(k) == TokenType.PATH_SEPARATOR
                && isFqnSegmentToken(tokens.type(k + 1))) {
            k += 2;
        }
        if (k < n && tokens.type(k) == TokenType.LESS_THAN) {
            int depth = 1;
            k++;
            while (k < n && depth > 0) {
                TokenType t = tokens.type(k);
                if (t == TokenType.LESS_THAN) depth++;
                else if (t == TokenType.GREATER_THAN) depth--;
                k++;
            }
        }
        if (k < n && tokens.type(k) == TokenType.PAREN_OPEN) {
            int depth = 1;
            k++;
            while (k < n && depth > 0) {
                TokenType t = tokens.type(k);
                if (t == TokenType.PAREN_OPEN) depth++;
                else if (t == TokenType.PAREN_CLOSE) depth--;
                k++;
            }
        }
        if (k < n && tokens.type(k) == TokenType.BRACKET_OPEN) {
            int depth = 1;
            k++;
            while (k < n && depth > 0) {
                TokenType t = tokens.type(k);
                if (t == TokenType.BRACKET_OPEN) depth++;
                else if (t == TokenType.BRACKET_CLOSE) depth--;
                k++;
            }
        }
        if (k >= n) {
            return true;
        }
        TokenType t = tokens.type(k);
        return t == TokenType.COMMA || t == TokenType.BRACKET_CLOSE
                || t == TokenType.SEMI_COLON || t == TokenType.PAREN_CLOSE
                || t == TokenType.BRACE_CLOSE;
    }

    private @com.legend.Nullable ColSpec tryTypedColSpec(String name, int nameTokIdx,
            int startColShift,
            List<com.legend.protocol.Protocol.PStereotype> stereotypes,
            List<com.legend.protocol.Protocol.PTaggedValue> taggedValues) {
        try {
            TypeExpression colType = parseType();
            Multiplicity colMult = !atEnd() && peek() == TokenType.BRACKET_OPEN
                    ? parseMultiplicity() : null;
            if (atEnd() || peek() == TokenType.COMMA || peek() == TokenType.BRACKET_CLOSE
                    || peek() == TokenType.SEMI_COLON || peek() == TokenType.PAREN_CLOSE
                    || peek() == TokenType.BRACE_CLOSE) {
                return new ColSpec(name, null, null, null, List.of(), false,
                        colSpecSpan(nameTokIdx, pos - 1, startColShift), colType, colMult,
                        stereotypes, taggedValues);
            }
            return null;
        } catch (ParseException e) {
            return null;
        }
    }

    private ColSpecArray parseColSpecArray() {
        int openTok = pos;
        pos++; // consume '['
        List<ColSpec> specs = new ArrayList<>();
        if (!atEnd() && peek() == TokenType.BRACKET_CLOSE) {
            pos++;
            return new ColSpecArray(specs, spanOf(openTok, pos - 1));
        }
        specs.add(parseOneColSpec());
        while (!atEnd() && peek() == TokenType.COMMA) {
            pos++; // consume ','
            if (!atEnd() && peek() == TokenType.BRACKET_CLOSE) {
                throw error("trailing comma in ColSpec array");
            }
            specs.add(parseOneColSpec());
        }
        expect(TokenType.BRACKET_CLOSE, "expected ']' to close ColSpec array");
        // Engine convention: the array spans brackets inclusive, tilde excluded.
        return new ColSpecArray(specs, spanOf(openTok, pos - 1));
    }

    /**
     * Parse a lambda in column-spec position. Same four dispatch
     * paths as {@link #parsePrimary()}'s lambda branches:
     * {@code | body}, {@code {... | body}}, {@code x | body},
     * {@code x: T[m] | body}. Factored out because column specs
     * only admit lambdas in the post-colon position, not arbitrary
     * expressions &mdash; calling {@link #parseCombinedExpression()}
     * would silently accept (and then mis-emit) non-lambda values.
     */
    private LambdaFunction parseColumnLambda() {
        // ColSpec lambdas use the ORDINARY inline-lambda span rule (pipe..body-end) —
        // verified against exact columns after an eyeballed "off-by-one" turned out to be
        // arithmetic error, not an engine quirk. The raw parse already sets it.
        return parseColumnLambdaRaw();
    }

    private LambdaFunction parseColumnLambdaRaw() {
        if (atEnd()) {
            throw error("expected lambda after ':' in column spec");
        }
        TokenType t = peek();
        if (t == TokenType.PIPE) {
            return parseLambdaPipe();
        }
        if (t == TokenType.BRACE_OPEN) {
            return parseLambdaFunction();
        }
        if (isIdentifierToken(t)) {
            if (pos + 1 < tokens.count()
                    && tokens.type(pos + 1) == TokenType.PIPE) {
                return parseSingleParamLambda();
            }
            if (pos + 1 < tokens.count()
                    && tokens.type(pos + 1) == TokenType.COLON
                    && looksLikeTypedLambdaParam()) {
                return parseSingleParamLambda();
            }
        }
        // Not a lambda: a general expression body (the clean-sheet navigate form
        // `~firm: acme::Firm.all()`, MAPPING_CLEAN_SHEET.md §3.1) — wrapped as a
        // zero-parameter thunk, so the AST stays uniformly lambda-shaped and the
        // checker decides which enclosing calls admit expression bodies.
        return new LambdaFunction(List.of(), List.of(parseCombinedExpression()));
    }

    // -------------------------------------------------------------------
    // Type annotations (C.7): @Type, @Relation<(col:Type, ...)>
    // -------------------------------------------------------------------

    /**
     * Parse a type annotation. Grammar:
     * <pre>
     *   typeAnnotation = '@' (relationShape | namedType)
     *   relationShape  = 'Relation' '&lt;' '(' relationColumns? ')' '&gt;'
     *   namedType      = qualifiedName ('&lt;' typeArgs '&gt;')?
     * </pre>
     *
     * <p>Pre-condition: cursor is on {@link TokenType#AT}. The
     * dispatch is governed by a 2-token lookahead after the
     * qualified name: simple-name {@code "Relation"} + LESS_THAN +
     * PAREN_OPEN commits to the structural-shape parse; anything
     * else falls through to {@link TypeAnnotation.Named}.
     *
     * <p>For non-relation generic types ({@code @List<Integer>}) the
     * type-argument list is parsed structurally into a nested
     * {@link TypeExpression.Generic}. Earlier code path collected
     * the {@code <...>} body as verbatim text; the structured form
     * is symmetric to lambda parameter types and unblocks a single
     * NameResolver tree-walk to rewrite simple names to FQNs.
     */
    private TypeAnnotation parseTypeAnnotation() {
        int atTok = pos;
        pos++; // consume '@'
        // @[m] — a multiplicity annotation (toMultiplicity.pure); PLATFORM grammar
        boolean platform = !dialect().refusesPlatformDialect();
        if (platform && !atEnd() && peek() == TokenType.BRACKET_OPEN) {
            Multiplicity m = parseMultiplicity();
            return new TypeAnnotation.MultiplicityRef(m, spanOf(atTok, pos - 1));
        }
        // @(x:String, …) — a bare relation-shape annotation (addColumns.pure);
        // the same shape @Relation<(…)> spells with its name. EVERY dialect:
        // the engine's own grammar accepts it (probed 2026-09-10 against the
        // 4.138.2 oracle — bare, `->cast(@(a:Integer))`, and
        // `@(name:Varchar(200))->genericType()` all ACCEPT; the engine's
        // core_external_query_sql/server/tests/testSchema.pure carries the
        // last form). The earlier "engine refuses bare @(…)" note on
        // CorpusSweepTest.MAX_PLATFORM_CATALOG was wrong about this form.
        if (!atEnd() && peek() == TokenType.PAREN_OPEN) {
            TypeAnnotation.RelationShape shape = parseRelationShape();
            return new TypeAnnotation.RelationShape(shape.columns(), null,
                    spanOf(atTok, pos - 1), spanOf(atTok, pos - 1));
        }
        if (!isFqnSegmentToken(peek())) {
            throw error("expected type name after '@'");
        }
        int nameStart = pos;
        String name = parseQualifiedName();
        // @Mass~Kilogram — the UNIT type spelling; folded into one name
        // (same convention as the type grammar and expression primary)
        if (!atEnd() && peek() == TokenType.TILDE
                && pos + 1 < tokens.count()
                && isFqnSegmentToken(tokens.type(pos + 1))) {
            pos++;
            name = name + "~" + text();
            pos++;
        }
        String simple = name.contains("::")
                ? name.substring(name.lastIndexOf("::") + 2)
                : name;

        // @Relation<(col:Type, ...)> — structural relation shape.
        // The lookahead must check BOTH '<' and the following '(' to
        // distinguish from @Relation<T> (a hypothetical generic
        // application of a class named 'Relation'). Engine grammar
        // reserves the structural form for the parenthesised inner.
        if ("Relation".equals(simple)
                && !atEnd()
                && peek() == TokenType.LESS_THAN
                && pos + 1 < tokens.count()
                && tokens.type(pos + 1) == TokenType.PAREN_OPEN) {
            pos++; // consume '<'
            TypeAnnotation.RelationShape shape = parseRelationShape();
            expect(TokenType.GREATER_THAN, "expected '>' to close @Relation type annotation");
            // wire spans (ProbeWireShapes "simple relation cast"): rawType covers
            // Relation<(...)>, the annotation node covers @..>
            return new TypeAnnotation.RelationShape(shape.columns(), name,
                    spanOf(nameStart, pos - 1), spanOf(atTok, pos - 1));
        }

        // Precise primitives with TYPE-VARIABLE VALUES — @Varchar(200) — mirror
        // parseType's handling (probe "agg kind and varchar"; rawType span covers the
        // whole application)
        if (!atEnd() && peek() == TokenType.PAREN_OPEN) {
            // ONE tvv grammar — the shared cursor helper (integers AND
            // strings, V('ok') is a probe-verified engine shape); this arm
            // was a second copy that refused strings (adversarial audit H1)
            List<ValueSpecification> tvv = parseTypeVariableValues();
            return new TypeAnnotation.Named(
                    new TypeExpression.Generic(name, List.of(), List.of(), tvv,
                            spanOf(nameStart, pos - 1)),
                    spanOf(atTok, pos - 1));
        }
        // Generic type arguments — parse structurally via the shared
        // helper. Nested '<...>' (e.g. Map<K, List<V>>) is handled by
        // recursive descent inside the helper.
        TypeExpression type;
        if (!atEnd() && peek() == TokenType.LESS_THAN) {
            List<String> multArgs = new ArrayList<>();
            List<TypeExpression> args = parseTypeArguments(multArgs);
            type = new TypeExpression.Generic(name, args, multArgs,
                    spanOf(nameStart, pos - 1));
        } else {
            type = new TypeExpression.NameRef(name, spanOf(nameStart, pos - 1));
        }
        // Engine convention (ProbeWireShapes "burn zoo" casts): the annotation node spans
        // @..type-end; its rawType spans the type name alone.
        return new TypeAnnotation.Named(type, spanOf(atTok, pos - 1));
    }

    /**
     * Parse the body of a {@code @Relation<(...)>} annotation.
     * Pre-condition: cursor is on the opening {@code (}.
     * Post-condition: cursor is on the closing {@code >} of the
     * outer angle pair (the caller consumes the {@code >}).
     */
    private TypeAnnotation.RelationShape parseRelationShape() {
        pos++; // consume '('
        List<TypeAnnotation.RelationShape.Column> columns = new ArrayList<>();
        if (!atEnd() && peek() != TokenType.PAREN_CLOSE) {
            columns.add(parseRelationColumn());
            while (!atEnd() && peek() == TokenType.COMMA) {
                pos++; // consume ','
                if (!atEnd() && peek() == TokenType.PAREN_CLOSE) {
                    throw error("trailing comma in @Relation columns");
                }
                columns.add(parseRelationColumn());
            }
        }
        expect(TokenType.PAREN_CLOSE, "expected ')' to close @Relation columns");
        return new TypeAnnotation.RelationShape(columns);
    }

    /**
     * Parse one column inside a {@code @Relation<(...)>}. Grammar:
     * <pre>
     *   relationColumn = (identifier | quotedString | '?') ':' columnType multiplicity?
     *   columnType     = '?' | type
     * </pre>
     *
     * <p>Both the name and the type can independently be a
     * wildcard ({@code ?}), matching engine-pure's
     * {@code mayColumnName} / {@code mayColumnType} grammar
     * productions. Quoted names are unescaped through the shared
     * {@link #unescapeString} pipeline.
     */
    private TypeAnnotation.RelationShape.Column parseRelationColumn() {
        if (atEnd()) {
            throw error("expected column name or '?' inside @Relation");
        }
        TokenType nameTok = peek();
        int nameTokIdx = pos;
        String name;
        if (nameTok == TokenType.QUESTION) {
            name = null;
            pos++;
        } else if (nameTok == TokenType.STRING) {
            String raw = text();
            if (raw.length() < 2 || raw.charAt(0) != '\''
                    || raw.charAt(raw.length() - 1) != '\'') {
                throw error("malformed quoted column name inside @Relation");
            }
            name = unescapeString(raw.substring(1, raw.length() - 1));
            pos++;
        } else if (isFqnSegmentToken(nameTok)) {
            name = text();
            pos++;
        } else {
            throw error(
                    "expected column name or '?' inside @Relation, got "
                    + nameTok);
        }
        expect(TokenType.COLON, "expected ':' after column name inside @Relation");

        TypeAnnotation type;
        if (!atEnd() && peek() == TokenType.QUESTION) {
            type = new TypeAnnotation.Wildcard();
            pos++;
        } else {
            // Inline type: structured TypeExpression. Wrapped in a
            // Named TypeAnnotation for downstream uniformity.
            type = new TypeAnnotation.Named(parseType());
        }

        Multiplicity multiplicity = null;
        if (!atEnd() && peek() == TokenType.BRACKET_OPEN) {
            multiplicity = parseMultiplicity();
        }
        // wire span = name .. type end, extended through a DECLARED multiplicity
        return new TypeAnnotation.RelationShape.Column(name, type, multiplicity,
                spanOf(nameTokIdx, pos - 1));
    }

    // -------------------------------------------------------------------
    // Bracket postfix (C.7a): $x[0] and $x['key']
    // -------------------------------------------------------------------

    /**
     * Parse a bracket-indexing postfix on an existing receiver.
     * Two shapes:
     *
     * <ul>
     *   <li><strong>Integer index</strong> {@code $x[0]} &rarr;
     *       {@code AppliedFunction("at", [receiver, CInteger(0)])}.
     *       Desugars to the Pure stdlib
     *       {@code at(collection: T[*], index: Integer[1]): T[1]}
     *       function; by emitting the call at parse time we let the
     *       existing {@code AppliedFunction} machinery handle binding
     *       rather than introducing a dedicated "indexed-access" AST
     *       node.</li>
     *   <li><strong>String key</strong> {@code $x['key']} &rarr;
     *       {@code AppliedProperty(receiver, "key")}. Engine-lite
     *       treats the string form as shorthand for
     *       {@code $x.key} &mdash; the brackets are sugar allowing
     *       keys whose spelling isn't a legal identifier.</li>
     * </ul>
     *
     * <p>Pre-condition: cursor is on {@code [}. Post-condition:
     * cursor is past the matching {@code ]}. Errors for any other
     * content (e.g. expressions, ranges) since Pure doesn't admit
     * them in bracket-indexing position.
     */
    private ValueSpecification parseBracketPostfix(ValueSpecification receiver) {
        if (dialect.refusesPlatformDialect()) {
            // engine-verbatim (probed live): bracket indexing is an m3
            // platform construct the engine refuses (deep-audit 1c/D1 —
            // it was previously parsed ungated and crashed the emitter)
            throw error("Bracket operation is not supported");
        }
        pos++; // consume '['
        if (atEnd()) {
            throw error("expected integer or string inside '[...]' index");
        }
        TokenType t = peek();
        ValueSpecification result;
        if (t == TokenType.INTEGER) {
            CInteger index = parseInteger();
            result = new AppliedFunction("at", List.of(receiver, index));
        } else if (t == TokenType.STRING) {
            CString key = parseString();
            result = new AppliedProperty(receiver, key.value());
        } else {
            throw error(
                    "expected integer or string inside '[...]' index, got "
                    + t + " ('" + safeText() + "')");
        }
        expect(TokenType.BRACKET_CLOSE, "expected ']' to close bracket-index expression");
        return result;
    }

    // -------------------------------------------------------------------
    /**
     * {@code #/Type/prop1/prop2#} — a navigation PATH. Its value semantics
     * for plain property segments IS the navigation lambda
     * {@code x | $x.prop1.prop2} (real pure evaluates a Path by walking its
     * property chain); richer path features (parameterized segments,
     * subtype casts {@code @X}, indices) are LOUD until built.
     */
    /** All chars in the date-literal class {@code [0-9T:.Z+-]}, non-empty. */
    private static boolean isDateBody(String s) {
        if (s.isEmpty()) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (!Character.isDigit(c) && "T:.Z+-".indexOf(c) < 0) {
                return false;
            }
        }
        return true;
    }

    /** All chars word-class ({@code \w}: letter/digit/_), non-empty. */
    private static boolean isWord(String s) {
        if (s.isEmpty()) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (!Character.isLetterOrDigit(c) && c != '_') {
                return false;
            }
        }
        return true;
    }

    /** Split on EVERY occurrence of {@code sep} (trailing empties kept —
     *  the split() this replaces used the default limit over single chars
     *  where no trailing empties arise in practice; the text-rule gate
     *  bans the regex form). */
    private static List<String> splitChar(String s, char sep) {
        // QUOTE-AWARE: a separator inside a quoted string ('a/b', 'a,b')
        // must not split (Tier-3 residue F2 — the blind indexOf mis-split
        // #/T/p('a/b')# into garbage segments; oracle-verified)
        List<String> out = new ArrayList<>();
        int start = 0;
        boolean inQuote = false;
        for (int i = 0; i <= s.length(); i++) {
            if (i == s.length()) {
                out.add(s.substring(start));
                break;
            }
            char c = s.charAt(i);
            if (inQuote && c == '\\') {
                i++;
            } else if (c == '\'') {
                inQuote = !inQuote;
            } else if (c == sep && !inQuote) {
                out.add(s.substring(start, i));
                start = i + 1;
            }
        }
        return out;
    }

    private ValueSpecification parsePathLiteral() {
        String text = text();
        int litTok = pos;
        pos++;
        String inner = text.substring(2, text.length() - 1);   // strip '#/' and '#'
        List<String> segList = splitChar(inner, '/');
        String[] segs = segList.toArray(new String[0]);
        // the TYPE segment must be a well-formed qualified name — the
        // engine refuses single/multiple colons (#/my:P/..#, #/my::::P/..#
        // — invention audit family 2, oracle-probed)
        if (segs.length > 0) {
            String tseg = segs[0].strip();
            boolean prevColon = false;
            int colons = 0;
            for (int ci = 0; ci < tseg.length(); ci++) {
                if (tseg.charAt(ci) == ':') {
                    colons++;
                    if (colons > 2 || ci == 0 || ci == tseg.length() - 1) {
                        throw error("Unexpected token");
                    }
                } else {
                    if (colons == 1) {
                        throw error("Unexpected token");
                    }
                    colons = 0;
                }
            }
            if (colons == 1) {
                throw error("Unexpected token");
            }
        }
        // a SEGMENT-LESS path (#/Person#) is legal: path [] + startType on the wire
        // (probe "pf path exotic")
        // Track each PLAIN segment's 0-based char range inside the literal text — the
        // emitter's shifted-span rule needs them (PathLiteral javadoc).
        List<com.legend.protocol.spec.PathLiteral.Segment> pieces = new ArrayList<>();
        boolean hasDated = false;
        int cursor = 2;   // index of the current segment's first char within `text`
        ValueSpecification body = new Variable("_path");
        String alias = null;
        for (int i = 1; i < segs.length; i++) {
            cursor += segs[i - 1].length() + 1;   // previous segment + '/'
            String rawSeg = segs[i];
            String seg = rawSeg.strip();
            int lead = rawSeg.indexOf(seg.isEmpty() ? "" : seg.substring(0, 1));
            // real pure: the LAST segment may carry '!alias' — the Path's
            // NAME (buildColumnNameOutOfPath uses it for the column)
            int bang = seg.indexOf('!');
            if (i == segs.length - 1 && bang > 0
                    && isWord(seg.substring(0, bang)) && isWord(seg.substring(bang + 1))) {
                alias = seg.substring(bang + 1);
                seg = seg.substring(0, bang);
            }
            // DATED segment prop(%d) / prop(%d1, %d2) / prop(%latest):
            // desugars to the milestoned property-FUNCTION call the Typer
            // already handles ($x.prop(%d)) — args re-parse as expressions
            // dated segment prop(args): whole seg = word '(' no-')' ')'
            int open = seg.indexOf('(');
            boolean isDatedSeg = open > 0 && seg.endsWith(")")
                    && isWord(seg.substring(0, open))
                    && seg.indexOf(')') == seg.length() - 1;
            if (isDatedSeg) {
                String datedName = seg.substring(0, open);
                String datedArgs = seg.substring(open + 1, seg.length() - 1);
                hasDated = true;
                List<ValueSpecification> args = new ArrayList<>();
                args.add(body);
                for (String arg : splitTopLevel(datedArgs)) {
                    args.add(SpecParser.parse("|" + arg.strip(), dialect())
                            instanceof LambdaFunction lf && lf.body().size() == 1
                            ? lf.body().get(0)
                            : new Variable(arg.strip()));
                }
                // wire pieces for the DATED segment — a charwise scan so collections
                // and empty argument lists work; anything unclassifiable walls
                List<com.legend.protocol.spec.PathLiteral.PathArg> wireArgs =
                        new ArrayList<>();
                boolean[] unsupportedFlag = {false};
                scanPathArgs(rawSeg, rawSeg.indexOf('(') + 1, rawSeg.lastIndexOf(')'),
                        cursor + lead, wireArgs, unsupportedFlag, false);
                boolean unsupported = unsupportedFlag[0];
                pieces.add(new com.legend.protocol.spec.PathLiteral.Segment(
                        datedName, cursor + lead, cursor + lead + seg.length() - 1,
                        wireArgs, unsupported));
                body = new AppliedFunction(datedName, args);
                continue;
            }
            if (!isWord(seg)) {
                throw error("navigation path segment '" + seg
                        + "' uses an unsupported path feature (only plain"
                        + " property segments desugar): " + text);
            }
            pieces.add(new com.legend.protocol.spec.PathLiteral.Segment(
                    seg, cursor + lead, cursor + lead + seg.length() - 1));
            body = new AppliedProperty(body, seg);
        }
        // the path's ROOT segment IS the param's type (real pure's Path
        // carries Path<Root,Leaf>) — the annotated lambda self-types in
        // let/argument positions, not just call positions
        LambdaFunction fn = new LambdaFunction(List.of(new Variable("_path",
                new com.legend.protocol.TypeExpression.NameRef(segs[0].strip()),
                null)), List.of(body));
        com.legend.protocol.spec.PathLiteral lit = new com.legend.protocol.spec.PathLiteral(
                segs[0].strip(), pieces, fn, alias, hasDated,
                spanOf(litTok, litTok), text.length());
        // the alias rides the node (real pure Path.name); the project and
        // sort checkers read it there — no carrier (2026-09-11 audit)
        return lit;
    }

    /**
     * Charwise scan of a dated path segment's argument list {@code rawSeg[from,to)}.
     * Scalars via {@link #scanScalar}; {@code [a, b]} recurses one level as a
     * span-less collection (probe "pf path exotic"). Offsets are relative to
     * the literal text via {@code base}. Kinds with no probed PATH wire shape
     * (booleans, variables, floats) mark the literal unsupported — a loud
     * wall, not a wrong read.
     */
    private void scanPathArgs(String rawSeg, int from, int to, int base,
            List<com.legend.protocol.spec.PathLiteral.PathArg> out,
            boolean[] unsupported, boolean inCollection) {
        int i = from;
        while (i < to && i >= 0) {
            char c = rawSeg.charAt(i);
            if (Character.isWhitespace(c) || c == ',') {
                i++;
                continue;
            }
            if (c == '[' && !inCollection) {
                int close = matchingBracket(rawSeg, i, to);
                if (close < 0) {
                    unsupported[0] = true;
                    return;
                }
                List<com.legend.protocol.spec.PathLiteral.PathArg> elems = new ArrayList<>();
                scanPathArgs(rawSeg, i + 1, close, base, elems, unsupported, true);
                out.add(new com.legend.protocol.spec.PathLiteral.PathArg.CollectionArg(elems));
                i = close + 1;
                continue;
            }
            IslandScan.Scalar sc = islandScan.scanScalar(rawSeg, i, to);
            switch (sc.kind()) {
                case SKIP -> { }
                case LATEST -> out.add(new com.legend.protocol.spec.PathLiteral
                        .PathArg.Latest(base + sc.s(), base + sc.e()));
                case DATE -> {
                    if (isDateBody(sc.a())) {
                        out.add(new com.legend.protocol.spec.PathLiteral
                                .PathArg.DateArg(sc.a(), base + sc.s(), base + sc.e()));
                    } else {
                        unsupported[0] = true;
                    }
                }
                case INT -> out.add(new com.legend.protocol.spec.PathLiteral
                        .PathArg.IntArg(sc.intValue(), base + sc.s(), base + sc.e()));
                case STRING -> out.add(new com.legend.protocol.spec.PathLiteral
                        .PathArg.StrArg(sc.a(), base + sc.s(), base + sc.e()));
                case ENUM -> out.add(new com.legend.protocol.spec.PathLiteral
                        .PathArg.EnumArg(sc.a(), sc.b()));
                default -> unsupported[0] = true;
            }
            i = sc.next();
        }
    }

    /** Split on top-level commas only (bracket/paren-aware); empty pieces drop —
     *  {@code ()} yields no arguments, {@code ['a','b']} stays one. */
    private static List<String> splitTopLevel(String s) {
        List<String> out = new ArrayList<>();
        int depth = 0;
        int start = 0;
        boolean inQuote = false;
        for (int i = 0; i <= s.length(); i++) {
            char c = i < s.length() ? s.charAt(i) : ',';
            if (inQuote && i < s.length()) {
                // quoted region: honour escapes, close on the bare quote —
                // a ',' inside 'a,b' must not split (Tier-3 residue F2)
                if (c == '\\') {
                    i++;
                } else if (c == '\'') {
                    inQuote = false;
                }
                continue;
            }
            if (c == '\'') {
                inQuote = true;
            } else if (c == '[' || c == '(') {
                depth++;
            } else if (c == ']' || c == ')') {
                depth--;
            } else if (c == ',' && depth == 0) {
                String piece = s.substring(start, i).strip();
                if (!piece.isEmpty()) {
                    out.add(piece);
                }
                start = i + 1;
            }
        }
        return out;
    }

    private static int matchingBracket(String s, int open, int limit) {
        int depth = 0;
        for (int i = open; i < limit; i++) {
            if (s.charAt(i) == '[') {
                depth++;
            } else if (s.charAt(i) == ']' && --depth == 0) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Parse a TDS literal. {@code TDS_LITERAL} is a lexer-side
     * aggregation of a {@code #TDS ... #} test-fixture block into a
     * single token carrying the verbatim CSV-ish body. The literal
     * desugars into an application of the lite-internal {@code tds}
     * native, spelled by its EXACT FQN ({@code meta::legend::lite::tds}
     * — the literal string here keeps the parser free of the builtin
     * catalog; NativeCatalogGovernanceTest binds the spelling): the
     * bare name {@code tds} is NOT user-resolvable, so the only route
     * to the native is this literal. The {@code "TDS"} leading argument
     * is a type-discriminator that the {@code tds} native uses to
     * distinguish bag-of-values overloads.
     */
    private ValueSpecification parseTdsLiteral() {
        // NO dialect gate: #TDS is REAL — legend-pure ships the TDS DSL
        // (platform_dsl_tds) and the ENGINE's xts-tds extension parses
        // the accessor forms (the 2026-08-14 gate attempt refused six
        // oracle-ACCEPTED corpus sources; the invention audit's "engine
        // refuses #TDS" held only for a bare parser without extensions)
        // NOT platform-gated: the ENGINE's xt-tds extension parses #TDS
        // accessors (6 oracle-accepted TestTDSAccessor/TestTDSCompiler
        // files byte-match through here — a 2026-08-12 gate attempt went
        // red on exactly those). The allowlisted TDS rows are the FINER
        // leniency: content/contexts the engine's TDS parser refuses and
        // lite's accepts.
        String raw = text();
        int tok = pos;
        pos++;
        // the wire's tdsString is the INNER text — after the '{', before
        // the '}#', UNTRIMMED (ZTailProbe "tds-accessor"); the desugared
        // tds(...) application keeps the verbatim token text the compiler
        // has always consumed
        int open = raw.indexOf('{');
        int close = raw.lastIndexOf('}');
        // The BRACE form (#TDS{...}#) is the ENGINE's own xt-tds grammar;
        // the BRACE-LESS form (#TDS ... #) is legend-pure's
        // platform_dsl_tds spelling, which the engine REFUSES
        // (adjudication probe 2026-08-14: brace form ACCEPT, brace-less
        // refuse — the earlier "#TDS is real everywhere" note conflated
        // the two). Drop-in refuses brace-less; PLATFORM/LITE keep it.
        if (!(open >= 0 && close > open) && dialect().refusesLiteExtensions()) {
            throw error("brace-less #TDS literal is not supported");
        }
        String inner = open >= 0 && close > open
                ? raw.substring(open + 1, close) : raw;
        return new com.legend.protocol.spec.TdsLiteral(inner,
                new AppliedFunction("meta::legend::lite::tds",
                        List.of(new CString("TDS"), new CString(raw))),
                spanOf(tok, tok));
    }

    // -------------------------------------------------------------------
    // DSL islands (C.7b): #{...}# and #>{...}#
    // -------------------------------------------------------------------

    /**
     * Parse a DSL-island expression. Grammar:
     * <pre>
     *   dsl = ISLAND_OPEN islandContent ISLAND_END
     * </pre>
     *
     * <p>An island is a "hole" in the main Pure grammar where an
     * embedded sub-language is spelled verbatim. The lexer flips
     * into island mode between {@code #{} and {@code }#} and emits
     * {@code ISLAND_BRACE_OPEN} /
     * {@code ISLAND_BRACE_CLOSE} / text tokens for everything in
     * between so the main parser can track depth without committing
     * to a specific sub-grammar at lex time.
     *
     * <p>The {@code ISLAND_OPEN} token text embeds the DSL
     * discriminator between {@code #} and {@code }: {@code #{}
     * &rarr; empty discriminator (graph-fetch tree), {@code #&gt;{}
     * &rarr; {@code "&gt;"} (table reference). Engine-lite supports
     * exactly these two, and we match.
     *
     * <p>Implementation: we collect the island content into a
     * string, then re-tokenize it with a fresh
     * {@link Lexer#tokenize} call and run a nested
     * {@link SpecParser} over the result. The re-lex costs one
     * pass over the content but keeps the sub-grammar parser
     * unaware of island-mode tokens. Whitespace between content
     * tokens is lost (we concatenate {@code text(pos)} values
     * directly), which matches engine-lite and is fine because
     * graph-fetch / table-reference syntax is whitespace-tolerant.
     *
     * <p>Islands close with {@code }#} ONLY. Engine-lite's fused
     * {@code }->} exit was an INVENTED closer with no engine
     * counterpart — deleted 2026-08-14 (MutationFuzzTest caught a
     * damaged {@code }#} re-lexing as the invented exit and
     * silently parsing).
     */
    /** A legacy m3 island KIND is a (possibly qualified) type name —
     *  checked charwise (the drop-in surface's regex family is frozen;
     *  DropInSurfaceTextRuleTest). */
    private static boolean isTypeNameShaped(String s) {
        if (s.isEmpty() || !(Character.isLetter(s.charAt(0))
                || s.charAt(0) == '_')) {
            return false;
        }
        for (int i = 1; i < s.length(); i++) {
            char c = s.charAt(i);
            if (!Character.isLetterOrDigit(c) && c != '_' && c != ':') {
                return false;
            }
        }
        return true;
    }

    private ValueSpecification parseDsl() {
        int islandStart = pos;
        String islandOpen = text();
        pos++; // consume ISLAND_OPEN
        // Extract DSL discriminator: strip leading '#' and trailing '{'.
        // ISLAND_OPEN text always has that shape by lexer contract.
        String dslType = islandOpen.substring(1, islandOpen.length() - 1);

        StringBuilder content = new StringBuilder();
        int depth = 0;   // NESTED #{...}# islands stay inside the outer one (audit M8a)
        while (!atEnd()) {
            TokenType t = peek();
            if (t == TokenType.ISLAND_OPEN || t == TokenType.ISLAND_START) {
                // ISLAND_START is the LEXER's spelling of a nested '#{'
                // (islands re-lex their own openers differently).
                depth++;
                content.append(text());
            } else if (t == TokenType.ISLAND_END) {
                if (depth == 0) {
                    pos++;
                    break;
                }
                depth--;
                content.append(text());
            } else if (t == TokenType.ISLAND_BRACE_OPEN) {
                content.append('{');
            } else if (t == TokenType.ISLAND_BRACE_CLOSE) {
                content.append('}');
            } else {
                content.append(text());
            }
            pos++;
        }
        String contentText = content.toString().trim();

        ValueSpecification result = switch (dslType) {
            case "" -> wrapGraphFetch(
                    parseGraphFetchTree(contentText, islandStart), islandStart, pos - 1);
            // Engine convention (ProbeWireShapes "burn zoo 2" tref): the classInstance
            // spans the whole #>{...}# literal.
            case ">" -> parseTableReference(contentText, spanOf(islandStart, pos - 1));
            // #SQL{ select ... }# — the sql-expression extension; carried
            // as its wire classInstance (ZTailProbe "sql-island"), refused
            // by the compiler. The sql value is the UNTRIMMED inner text —
            // engine keeps trailing whitespace (corpus byte diff caught the
            // trim on 2026-08-09)
            case "SQL" -> new com.legend.protocol.spec.SqlIsland(
                    content.toString(), spanOf(islandStart, pos - 1));
            // #GQL{ query {...} }# — the graphQL extension: the island
            // content parses from the RAW SOURCE SLICE (the token
            // reconstruction drops the spacing GraphQL names need) into
            // the engine's canonical AST wire (probe gql-wire 2026-08-14)
            case "GQL" -> new com.legend.protocol.spec.GqlIsland(
                    com.legend.parser.GqlParser.parseDocument(
                            tokens.source().substring(
                                    tokens.end(islandStart),
                                    tokens.start(pos - 1)),
                            tokens.endLine(islandStart),
                            tokens.endColumn(islandStart) + 1),
                    spanOf(islandStart, pos - 1));
            default -> {
                // PLATFORM: legend-pure's LEGACY graph-fetch spelling
                // puts the root class in the island KIND — #Person{name}#
                // is m3 for #{Person{name}}#. Canonicalize by splicing
                // the kind back as the tree root and riding the modern
                // path (A5 platform-gap burn 2026-08-15, ~66 rows). The
                // drop-in/lite surfaces keep the engine's refusal.
                if (!dialect().refusesPlatformDialect()
                        && isTypeNameShaped(dslType)) {
                    yield wrapGraphFetch(parseGraphFetchTree(
                            dslType + "{" + contentText + "}", islandStart),
                            islandStart, pos - 1);
                }
                throw error(
                        "unknown DSL island type: '#" + dslType + "{'");
            }
        };

        return result;
    }

    /**
     * Parse a table reference DSL: {@code #>{db::path.TABLE}#}
     * &rarr; {@code AppliedFunction("tableReference",
     * [PackageableElementPtr(db), CString(tableName)])}. The content
     * is a dotted path where everything before the LAST {@code .} is
     * the database FQN and everything after is the table name.
     *
     * <p>The {@code db} argument is emitted as a
     * {@link PackageableElementPtr} (a typed FQN reference) rather
     * than a {@link CString}. This means the resolver and downstream
     * type checker treat it uniformly with every other element
     * reference \u2014 no special-case "this string is actually a
     * name" carve-out is needed. The table name remains a
     * {@link CString} because it is opaque physical-DB-level
     * identifier, not a Pure FQN.
     *
     * <p>Diverges intentionally from engine-lite, which emitted
     * {@code CString} for both args and added per-layer special cases
     * (in {@code NameResolver}, {@code TableReferenceChecker},
     * {@code MappingNormalizer}) to interpret the first string as an
     * FQN. Encoding the distinction once, at parse time, lets every
     * downstream layer dispatch structurally.
     */
    private AppliedFunction parseTableReference(String content,
            com.legend.protocol.SourceInfo span) {
        // the accessor grammar admits only a dotted qualified path — a
        // DAMAGED island close (}# -> }) swallows source through the NEXT
        // }# and the junk arrived here as a "table name" (MutationFuzzTest
        // island-close-plain; the engine refuses)
        for (char ch : content.toCharArray()) {
            if (ch == '(' || ch == ')' || ch == '|' || ch == ';'
                    || ch == '=' || ch == '{' || ch == '}' || ch == '\n') {
                throw new ParseException("Unexpected token",
                        span.startLine(), span.startColumn());
            }
        }
        // Split at the FIRST dot after the ::-path: "db::DB.schema.TABLE"
        // is (db::DB, "schema.TABLE") — lastIndexOf('.') mis-split it into
        // an FQN with an embedded dot (audit M7). The db part is the
        // ::-qualified prefix; everything after its first dot is the
        // (possibly schema-qualified) table name, matching the element
        // parser's SCHEMA.TABLE folding.
        int pathEnd = content.lastIndexOf("::");
        int dot = content.indexOf('.', pathEnd < 0 ? 0 : pathEnd + 2);
        if (dot < 0) {
            // a STORE-ONLY reference (#>{my::Store}#) is legal — one path element on
            // the wire (probe "pf named new and store tref" b)
            return new AppliedFunction("tableReference",
                    List.of(new PackageableElementPtr(content)),
                    List.of(), span);
        }
        String db = content.substring(0, dot);
        String tableName = content.substring(dot + 1);
        return new AppliedFunction("tableReference",
                List.of(new PackageableElementPtr(db), new CString(tableName)),
                List.of(), span);
    }

    /**
     * Parse a graph-fetch tree content string like
     * {@code "ClassName {prop1, prop2 { subprop } }"}. Returns a
     * {@link ColSpecArray} of nested {@link ColSpec}s describing
     * the tree.
     *
     * <p>Engine-lite's desugaring: each property becomes a
     * {@link ColSpec} whose {@code function1} is a single-param
     * lambda {@code x | $x.prop} (producing the property value),
     * and whose {@code function2} (when present) is a nested
     * {@link ColSpecArray} wrapped in a lambda for the sub-tree.
     * This lets graph-fetch reuse the same ColSpec machinery the
     * tilde-column DSL uses (C.6).
     *
     * <p>The root class name is NOT retained in the AST &mdash;
     * engine-lite gets it from arg[0] of the enclosing
     * {@code graphFetch($classCollection, #{ ... }#)} call and
     * discards the inline root name. We follow suit.
     */
    /**
     * Wrap a parsed graph-fetch desugar with the wire-facing span tree. The island lexer
     * emits coarse {@code ISLAND_CONTENT} chunks (names live INSIDE chunk strings), so the
     * scan runs CHARWISE over the raw source between {@code #"{"} and the island closer,
     * with positions derived from character offsets — absolute, because graph-fetch spans
     * are NOT island-shifted (unlike navigation paths). Any form beyond plain property
     * names and nesting — aliases, parameters, subtype trees — marks the literal
     * unsupported and the emitter walls.
     */
    private ValueSpecification wrapGraphFetch(ValueSpecification desugared,
            int islandStart, int islandEnd) {
        String src = tokens.source();
        int a = tokens.end(islandStart);           // char offset just past '#{'
        int z = tokens.start(islandEnd);           // char offset of the island closer
        int brace = src.indexOf('{', a);
        int ns = a;
        while (ns < z && Character.isWhitespace(src.charAt(ns))) {
            ns++;
        }
        if (brace < 0 || brace >= z || ns >= brace) {
            return new com.legend.protocol.spec.GraphFetchLiteral(
                    "", List.of(), desugared, true, spanOf(islandStart, islandStart));
        }
        int ne = brace - 1;
        while (ne > ns && Character.isWhitespace(src.charAt(ne))) {
            ne--;
        }
        com.legend.protocol.SourceInfo namePos = charSpan(ns, ne);
        boolean[] unsupported = {false};
        int[] cursor = {brace};
        List<com.legend.protocol.spec.GraphFetchLiteral.SubTypeNode> subTypes =
                new ArrayList<>();
        List<com.legend.protocol.spec.GraphFetchLiteral.Node> nodes =
                islandScan.scanGraphNodes(src, cursor, z, unsupported, subTypes);
        return new com.legend.protocol.spec.GraphFetchLiteral(
                src.substring(ns, ne + 1), nodes, subTypes, desugared,
                unsupported[0], namePos);
    }

    /** Inclusive character range → engine 1-based/inclusive-end {@link com.legend.protocol.SourceInfo}. */
    private com.legend.protocol.SourceInfo charSpan(int from, int to) {
        return new com.legend.protocol.SourceInfo(spanSourceId(),
                tokens.lineOf(from), tokens.columnOf(from),
                tokens.lineOf(to), tokens.columnOf(to));
    }


    private ValueSpecification parseGraphFetchTree(String content,
            int islandStart) {
        TokenStream innerTokens = Lexer.tokenize(content);
        if (innerTokens.count() == 0) {
            // ENGINE-VERBATIM (reprobe TestMappingGrammarParser#12):
            // #{}# refuses at the island opener
            throw new com.legend.parser.ParseException(
                    "Graph fetch tree must not be empty",
                    tokens.startLine(islandStart),
                    tokens.startColumn(islandStart));
        }
        SpecParser inner = new SpecParser(innerTokens, "", dialect);
        try {
            inner.parseQualifiedName();          // skip root class name
            ValueSpecification tree = inner.parseGraphDefinition(0);
            if (!inner.atEnd()) {
                // LOUD: #{Person {name} GARBAGE}# previously dropped
                // GARBAGE silently (audit M8c).
                throw inner.error("trailing content after graph-fetch tree: '"
                        + inner.safeText() + "'");
            }
            return tree;
        } catch (com.legend.parser.ParseException e) {
            // the content is RE-LEXED from a reconstructed string, so
            // inner positions start at 1:1 — compose the island's base
            // position back in (reprobe: a tree error surfaced at 1:17
            // inside a line-14 document)
            int baseTok = Math.min(islandStart + 1, tokens.count() - 1);
            int baseLine = tokens.startLine(baseTok);
            int baseCol = tokens.startColumn(baseTok);
            String raw = String.valueOf(e.getMessage());
            String prefix = "[" + e.line() + ":" + e.column() + "] ";
            if (raw.startsWith(prefix)) {
                raw = raw.substring(prefix.length());
            }
            if (e.line() <= 0) {
                // a positionless inner error (EOF): anchor at the opener
                throw new com.legend.parser.ParseException(raw,
                        tokens.startLine(islandStart),
                        tokens.startColumn(islandStart));
            }
            throw new com.legend.parser.ParseException(raw,
                    baseLine + e.line() - 1,
                    e.line() == 1 ? baseCol + e.column() - 1 : e.column());
        }
    }

    /**
     * Parse {@code { path (, path)* }} into a {@link ColSpecArray}.
     * Trailing comma is tolerated (engine-lite: "if
     * check(BRACE_CLOSE) after comma, stop"). We match so
     * {@code { a, b, }} works the same as {@code { a, b }}.
     */
    private ColSpecArray parseGraphDefinition(int depth) {
        expect(TokenType.BRACE_OPEN, "expected '{' to open graph-fetch body");
        List<ColSpec> specs = new ArrayList<>();
        if (!atEnd() && peek() != TokenType.BRACE_CLOSE) {
            specs.add(parseGraphPath(depth));
            while (!atEnd() && peek() == TokenType.COMMA) {
                pos++;
                if (!atEnd() && peek() == TokenType.BRACE_CLOSE) {
                    if (dialect.refusesPlatformDialect()) {
                        // engine graphPaths has no trailing comma
                        throw error("Unexpected token '}'");
                    }
                    break; // trailing comma tolerated (PLATFORM dialect —
                           // gated refusesPlatformDialect on drop-in/lite)
                }
                specs.add(parseGraphPath(depth));
            }
        }
        if (specs.isEmpty() && dialect.refusesPlatformDialect()) {
            // the engine's graphDefinition requires at least ONE path —
            // an EMPTY #{Class{}}# body is a parse error (negative
            // fixture engine-fixture#0a4ae2ea42a3, refusal at the '}')
            throw error("Unexpected token '}'");
        }
        expect(TokenType.BRACE_CLOSE, "expected '}' to close graph-fetch body");
        return new ColSpecArray(specs);
    }

    /**
     * Parse one path inside a graph-fetch definition. Grammar:
     * <pre>
     *   graphPath    = (STRING ':')? identifier propertyParams? graphDefinition?
     *   propertyParams = '(' ... ')'   (milestoning args -- skipped)
     * </pre>
     *
     * <p>The optional {@code STRING ':'} prefix is a graph alias
     * used for deduplicating renamed paths in downstream tooling.
     * Engine-lite skips it (consumes without storing); we match.
     *
     * <p>Property parameters inside {@code (...)} are also skipped
     * \u2014 they're milestoning args that engine-lite parses but
     * discards pending a schema update. Keeping the same behaviour
     * preserves AST compatibility.
     */
    private ColSpec parseGraphPath(int depth) {
        // ->subType(@FQN) { ... } — the SUBTYPE VIEW of the enclosing node
        // (engine graph grammar): children read the subtype's own
        // properties; rows not of that type omit them. Encoded as a
        // ColSpec named '->subType' carrying the @Type annotation arg.
        if (!atEnd() && peek() == TokenType.ARROW
                && pos + 1 < tokens.count()
                && isFqnSegmentToken(tokens.type(pos + 1))
                && "subType".equals(tokens.text(pos + 1))) {
            pos += 2;
            expect(TokenType.PAREN_OPEN, "expected '(' after ->subType");
            expect(TokenType.AT, "expected '@' in ->subType(@Type)");
            String subFqn = parseQualifiedName();
            expect(TokenType.PAREN_CLOSE, "expected ')' after ->subType type");
            // a LEAF subType view (no braces) fetches the subtype with no extra props
            ColSpecArray nested = !atEnd() && peek() == TokenType.BRACE_OPEN
                    ? parseGraphDefinition(depth + 1)
                    : new ColSpecArray(List.of());
            LambdaFunction fn2 = new LambdaFunction(List.of(), List.of(nested));
            return new ColSpec("->subType", null, fn2, null,
                    List.of(new TypeAnnotation.Named(
                            new TypeExpression.NameRef(subFqn))));
        }
        // Optional alias: 'aliasName': property — the engine serializes
        // the node under the ALIAS (task #78; the discard was engine-lite
        // behaviour our envelope emission has outgrown)
        String alias = null;
        if (pos + 1 < tokens.count()
                && peek() == TokenType.STRING
                && tokens.type(pos + 1) == TokenType.COLON) {
            String raw = text();
            alias = raw.length() >= 2 ? raw.substring(1, raw.length() - 1)
                    : raw;
            pos += 2;
        }
        if (!isFqnSegmentToken(peek())) {
            throw error("expected property name in graph-fetch path");
        }
        String propName = text();
        pos++;

        // function1: x | $x.prop  (produces the property value)
        String paramName = "_gf" + depth;
        Variable param = new Variable(paramName);
        AppliedProperty propAccess = new AppliedProperty(param, propName);
        LambdaFunction fn1 = new LambdaFunction(
                List.of(param), List.of(propAccess));

        // Optional propertyParameters: parsed as REAL expressions —
        // qualifier call args ('Mr') inline the derived body (task #78);
        // milestoning args stay checker-dropped for non-derived props.
        List<ValueSpecification> args = List.of();
        boolean qualified = false;
        if (!atEnd() && peek() == TokenType.PAREN_OPEN) {
            qualified = true;
            args = parseArgList();
        }

        // prop->subType(@Sub) { ... } — the PROPERTY-LEVEL subtype view
        // (simpleObject corpus family): sugar for prop { ->subType(@Sub)
        // { ... } } — desugared to the head-form child the checker
        // already validates (children against the subtype class).
        if (!atEnd() && peek() == TokenType.ARROW
                && pos + 1 < tokens.count()
                && isFqnSegmentToken(tokens.type(pos + 1))
                && "subType".equals(tokens.text(pos + 1))) {
            pos += 2;
            expect(TokenType.PAREN_OPEN, "expected '(' after ->subType");
            expect(TokenType.AT, "expected '@' in ->subType(@Type)");
            String subFqn = parseQualifiedName();
            expect(TokenType.PAREN_CLOSE, "expected ')' after ->subType type");
            ColSpecArray subBody = !atEnd() && peek() == TokenType.BRACE_OPEN
                    ? parseGraphDefinition(depth + 1)
                    : new ColSpecArray(List.of());
            ColSpec subChild = new ColSpec("->subType", null,
                    new LambdaFunction(List.of(), List.of(subBody)), null,
                    List.of(new TypeAnnotation.Named(
                            new TypeExpression.NameRef(subFqn))));
            LambdaFunction fn2 = new LambdaFunction(List.of(),
                    List.of(new ColSpecArray(List.of(subChild))));
            return new ColSpec(propName, fn1, fn2, alias, args, qualified);
        }

        // Optional nested graph definition: { subpaths }
        if (!atEnd() && peek() == TokenType.BRACE_OPEN) {
            ColSpecArray nested = parseGraphDefinition(depth + 1);
            // function2 wraps the nested array in a zero-param lambda
            // so the two ColSpec slots are uniformly typed (LambdaFunction).
            LambdaFunction fn2 = new LambdaFunction(
                    List.of(), List.of(nested));
            return new ColSpec(propName, fn1, fn2, alias, args, qualified);
        }

        return new ColSpec(propName, fn1, null, alias, args, qualified);
    }

    // -----------------------------------------------------------------
    // TokenStreamCursor accessors. Implementing the interface gives us
    // the lexical layer (peek/match/expect/error/...) plus the
    // type-expression grammar (parseType / parseMultiplicity / ...) as
    // inherited defaults shared with ElementParser.
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
}
