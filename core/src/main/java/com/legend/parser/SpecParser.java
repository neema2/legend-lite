package com.legend.parser;

import com.legend.lexer.Lexer;
import com.legend.lexer.TokenStream;
import com.legend.lexer.TokenType;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.AppliedProperty;
import com.legend.parser.spec.CBoolean;
import com.legend.parser.spec.CDate;
import com.legend.parser.spec.CDecimal;
import com.legend.parser.spec.CFloat;
import com.legend.parser.spec.CInteger;
import com.legend.parser.spec.CLatestDate;
import com.legend.parser.spec.CString;
import com.legend.parser.spec.CTime;
import com.legend.values.PureDateLiteral;
import com.legend.values.PureTimeLiteral;
import com.legend.parser.spec.ColSpec;
import com.legend.parser.spec.ColSpecArray;
import com.legend.parser.spec.ColumnInstance;
import com.legend.parser.spec.EnumValue;
import com.legend.parser.spec.KeyExpression;
import com.legend.parser.spec.LambdaFunction;
import com.legend.parser.spec.NewInstance;
import com.legend.parser.spec.NewInstanceCast;
import com.legend.parser.spec.PackageableElementPtr;
import com.legend.parser.spec.PureCollection;
import com.legend.parser.spec.TypeAnnotation;
import com.legend.parser.spec.ValueSpecification;
import com.legend.parser.spec.Variable;

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
 *       {@link #parseCodeBlock(String)}. The braced lambda body
 *       uses the same machinery internally.</li>
 * </ul>
 *
 * <h3>Precedence (matching engine's {@code PureQueryParser})</h3>
 * <p>Pure's grammar is intentionally <em>flat</em> within arithmetic:
 * {@code 1 + 2 * 3} parses as {@code (1 + 2) * 3}, not
 * {@code 1 + (2 * 3)}. There is no precedence distinction between
 * {@code +} and {@code *}; same-operator chains stay together
 * ({@code 1 + 2 + 3} chains in one node-builder call), different
 * operators bubble up to the combined-expression loop and chain
 * left-to-right. The structure has three nested levels:
 * <ol>
 *   <li>{@link #parseCombinedExpression()} &mdash; outermost.
 *       Loops over {@code &&}/{@code ||}/arithmetic ops, building
 *       left-associative call chains.</li>
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
 *   <li>{@link #parse(String)} / {@link #parse(TokenStream)} &mdash;
 *       parse a single program line (one expression or one
 *       let-binding). Convenience entry for tests, embedded
 *       expressions (e.g. property defaults), and any context that
 *       admits exactly one statement.</li>
 *   <li>{@link #parseCodeBlock(String)} /
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

    private SpecParser(TokenStream tokens) {
        this.tokens = Objects.requireNonNull(tokens, "tokens");
    }

    // -------------------------------------------------------------------
    // Public entry points
    // -------------------------------------------------------------------

    /** Lex {@code source} and parse the result as a single expression. */
    public static ValueSpecification parse(String source) {
        return parse(Lexer.tokenize(Objects.requireNonNull(source, "source")));
    }

    /**
     * Parse the given token stream as a single value specification. The
     * stream may be a {@link TokenStream#slice(int, int)} of a larger
     * file; offsets are preserved so errors point at the original
     * source location.
     */
    public static ValueSpecification parse(TokenStream tokens) {
        SpecParser parser = new SpecParser(tokens);
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
    public static List<ValueSpecification> parseCodeBlock(String source) {
        return parseCodeBlock(Lexer.tokenize(source));
    }

    /**
     * Token-stream variant of {@link #parseCodeBlock(String)}.
     * Consumes the entire stream; trailing tokens after the last
     * statement raise a fail-fast error.
     */
    public static List<ValueSpecification> parseCodeBlock(TokenStream tokens) {
        SpecParser parser = new SpecParser(tokens);
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
        return new AppliedFunction(
                "letFunction",
                List.of(new CString(varName), value));
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
    private List<ValueSpecification> parseCodeBlockUntil(TokenType terminator) {
        List<ValueSpecification> stmts = new ArrayList<>();
        if (atTerminator(terminator)) {
            return stmts;
        }
        stmts.add(parseProgramLine());
        while (!atEnd() && peek() == TokenType.SEMI_COLON) {
            pos++; // consume ';'
            if (atTerminator(terminator)) {
                break;
            }
            stmts.add(parseProgramLine());
        }
        return stmts;
    }

    private boolean atTerminator(TokenType terminator) {
        if (atEnd()) {
            return true; // EOF terminates
        }
        return terminator != null && peek() == terminator;
    }

    // -------------------------------------------------------------------
    // Combined-expression level (operators)
    // -------------------------------------------------------------------

    /**
     * Outermost expression entry &mdash; handles {@code &&}/{@code ||}
     * and arithmetic-style binary operators in a flat, left-associative
     * sequence (matching Pure's grammar; see precedence note in the
     * class Javadoc). Calls {@link #parseExpression()} for each operand.
     */
    private ValueSpecification parseCombinedExpression() {
        ValueSpecification expr = parseExpression();
        while (!atEnd()) {
            TokenType t = peek();
            if (t == TokenType.AND || t == TokenType.OR) {
                expr = parseBooleanPart(expr);
            } else if (isArithmeticOp(t)) {
                expr = parseArithmeticPart(expr);
            } else {
                break;
            }
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
            if (t == TokenType.TEST_EQUAL
                    || t == TokenType.TEST_NOT_EQUAL
                    || t == TokenType.NOT_EQUAL) {
                pos++;
                ValueSpecification right = parseCombinedArithmeticOnly();
                // != desugars to not(equal(...)) — REAL pure's spelling
                // (there is no notEqual native; FQN_MIGRATION finding).
                AppliedFunction eq = new AppliedFunction("equal", List.of(expr, right));
                expr = (t == TokenType.TEST_EQUAL) ? eq
                        : new AppliedFunction("not", List.of(eq));
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
                pos++;
                return new AppliedFunction("not", List.of(parseExpression()));
            }
            if (t == TokenType.MINUS) {
                pos++;
                return new AppliedFunction("minus", List.of(parseExpression()));
            }
            if (t == TokenType.PLUS) {
                pos++;
                return new AppliedFunction("plus", List.of(parseExpression()));
            }
        }
        return parsePrimary();
    }

    /**
     * Helper used by the right-hand side of {@code ==}/{@code !=}: a
     * single expression plus any trailing arithmetic chain (but no
     * {@code &&}/{@code ||} &mdash; those would re-enter the
     * combined-expression loop and never terminate the equality
     * production).
     */
    private ValueSpecification parseCombinedArithmeticOnly() {
        ValueSpecification expr = parseExpression();
        while (!atEnd() && isArithmeticOp(peek())) {
            expr = parseArithmeticPart(expr);
        }
        return expr;
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

    /**
     * {@code &&} or {@code ||}, desugared to
     * {@code and(left, right)} / {@code or(left, right)}. The right
     * operand is parsed via {@link #parseCombinedArithmeticOnly()} so
     * the boolean operator binds <em>looser</em> than arithmetic but
     * <em>tighter</em> than no operator at all &mdash; matching
     * engine's grammar.
     */
    private AppliedFunction parseBooleanPart(ValueSpecification left) {
        TokenType t = peek();
        String fn = (t == TokenType.AND) ? "and" : "or";
        pos++;
        ValueSpecification right = parseCombinedArithmeticOnly();
        // REAL Pure: && binds tighter than || (engine DomainParseTreeWalker's
        // isLowerPrecedenceBoolean) — an AND-chain claims the operand before
        // an OR folds it: a || b && c == or(a, and(b, c)).
        while (fn.equals("or") && !atEnd() && peek() == TokenType.AND) {
            right = parseBooleanPart(right);
        }
        return new AppliedFunction(fn, List.of(left, right));
    }

    /**
     * Arithmetic and comparison operators desugared to
     * {@link AppliedFunction}s via PRECEDENCE CLIMBING &mdash; REAL Pure's
     * grammar (legend-pure {@code AbstractTestPrecedence} is the spec):
     * {@code *}/{@code /} bind tighter than {@code +}/{@code -}, which bind
     * tighter than comparisons, so {@code 1 + 2 * 3} is
     * {@code plus(1, times(2, 3))} = 7. (Engine-lite's flat grammar —
     * {@code times(plus(1,2),3)} = 9 — was a DIVERGENCE from real Pure that
     * we deliberately do not carry; caught by an executed lowering test.)
     * Left-associative within a tier.
     */
    private ValueSpecification parseArithmeticPart(ValueSpecification left) {
        return parseArithmeticClimb(left, 1);
    }

    private ValueSpecification parseArithmeticClimb(ValueSpecification left, int minPrec) {
        while (!atEnd() && isArithmeticOp(peek())
                && precedenceOf(peek()) >= minPrec) {
            TokenType op = peek();
            int prec = precedenceOf(op);
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
                        "parseArithmeticClimb on non-arithmetic token: " + op);
            };
            pos++;
            ValueSpecification right = parseExpression();
            // Tighter-binding operators on the right claim the operand first.
            while (!atEnd() && isArithmeticOp(peek())
                    && precedenceOf(peek()) > prec) {
                right = parseArithmeticClimb(right, precedenceOf(peek()));
            }
            left = new AppliedFunction(fn, List.of(left, right));
        }
        return left;
    }

    /** {@code *}/{@code /} &gt; {@code +}/{@code -} &gt; comparisons (real Pure). */
    private static int precedenceOf(TokenType t) {
        return switch (t) {
            case STAR, DIVIDE -> 3;
            case PLUS, MINUS -> 2;
            case LESS_THAN, LESS_OR_EQUAL, GREATER_THAN, GREATER_OR_EQUAL -> 1;
            default -> 0;
        };
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
            case INTEGER -> parseInteger();
            case FLOAT -> parseFloat();
            case DECIMAL -> parseDecimal();
            case STRING -> parseString();
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
            case AT -> parseTypeAnnotation();
            case COMPARATOR -> parseComparatorExpression();
            case TDS_LITERAL -> parseTdsLiteral();
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
                throw error("unsupported expression token: " + t
                        + " ('" + safeText() + "')");
            }
        };
    }

    // -------------------------------------------------------------------
    // Numeric literals
    // -------------------------------------------------------------------

    /**
     * INTEGER token &rarr; {@link CInteger}. Narrows to {@link Long} when
     * the value fits in 64 signed bits, else falls back to
     * {@link BigInteger} so overflow is preserved exactly (matches the
     * engine record contract).
     */
    private CInteger parseInteger() {
        String text = text();
        pos++;
        try {
            return new CInteger(Long.parseLong(text));
        } catch (NumberFormatException overflow) {
            return new CInteger(new BigInteger(text));
        }
    }

    /**
     * FLOAT token &rarr; {@link CFloat} with an <em>automatic
     * precision-loss promotion</em> to {@link CDecimal}. Rationale:
     * the lexer classifies any non-suffixed number containing a
     * decimal point or exponent as {@code FLOAT}. If the textual
     * value doesn't round-trip through Java {@code double} exactly,
     * emitting {@code CFloat(parsed-double)} would silently
     * <em>lose information that was present in source</em> &mdash; a
     * user writing {@code 1.0000000000000001} would get
     * {@code CFloat(1.0)} back. Engine-lite detects this by
     * comparing {@link BigDecimal} parses, and falls back to
     * {@link CDecimal} so the exact value is preserved. We match
     * that behaviour verbatim.
     *
     * <p>Note that this makes {@link #parseFloat()} sometimes return
     * a {@code CFloat} and sometimes a {@code CDecimal}. The return
     * type is the common supertype {@link ValueSpecification}; the
     * callsite (the {@code FLOAT} case in {@link #parsePrimary()})
     * never cares about the distinction because both are literal
     * value specifications.
     */
    private ValueSpecification parseFloat() {
        String text = text();
        pos++;
        // Strip optional 'f'/'F' suffix; Pure permits it on float literals
        // but Java's Double.parseDouble does not.
        if (!text.isEmpty()) {
            char last = text.charAt(text.length() - 1);
            if (last == 'f' || last == 'F') text = text.substring(0, text.length() - 1);
        }
        BigDecimal exact = new BigDecimal(text);
        double d = Double.parseDouble(text);
        if (exact.compareTo(BigDecimal.valueOf(d)) != 0) {
            return new CDecimal(exact);
        }
        return new CFloat(d);
    }

    /**
     * DECIMAL token &rarr; {@link CDecimal}. The lexer admits both
     * {@code 42d} (integer-shaped) and {@code 3.14d} (float-shaped)
     * forms; both end with a {@code d}/{@code D} that {@link BigDecimal}
     * does not accept, so it is stripped before parsing.
     */
    private CDecimal parseDecimal() {
        String text = text();
        pos++;
        char last = text.charAt(text.length() - 1);
        if (last == 'd' || last == 'D') text = text.substring(0, text.length() - 1);
        return new CDecimal(new BigDecimal(text));
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
        pos++;
        return new CString(unescaped);
    }

    private String unescapeString(String body) {
        if (body.indexOf('\\') < 0) return body;
        StringBuilder sb = new StringBuilder(body.length());
        int i = 0;
        while (i < body.length()) {
            char c = body.charAt(i);
            if (c != '\\') { sb.append(c); i++; continue; }
            if (i + 1 >= body.length()) {
                throw error("malformed string literal: trailing backslash");
            }
            char esc = body.charAt(i + 1);
            switch (esc) {
                case '\\' -> sb.append('\\');
                case '\'' -> sb.append('\'');
                case 'n' -> sb.append('\n');
                case 't' -> sb.append('\t');
                case 'r' -> sb.append('\r');
                default -> throw error(
                        "malformed string literal: unsupported escape '\\" + esc + "'");
            }
            i += 2;
        }
        return sb.toString();
    }

    private CBoolean consumeBoolean(boolean value) {
        pos++;
        return new CBoolean(value);
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
        try {
            return new CDate(PureDateLiteral.parse(value));
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
            return new CTime(PureTimeLiteral.parse(value));
        } catch (IllegalArgumentException e) {
            throw TokenStreamCursor.throwAt(tokens, timePos,
                    "invalid time literal '%" + value + "': " + e.getMessage());
        }
    }

    private CLatestDate parseLatestDate() {
        pos++;
        return new CLatestDate();
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
        return new Variable(name);
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
    private PureCollection parseCollection() {
        pos++; // consume '['
        List<ValueSpecification> values = new ArrayList<>();
        if (!atEnd() && peek() == TokenType.BRACKET_CLOSE) {
            pos++;
            return new PureCollection(values);
        }
        values.add(parseCombinedExpression());
        while (!atEnd() && peek() == TokenType.COMMA) {
            pos++; // consume ','
            if (!atEnd() && peek() == TokenType.BRACKET_CLOSE) {
                throw error("trailing comma in collection literal");
            }
            values.add(parseCombinedExpression());
        }
        expect(TokenType.BRACKET_CLOSE, "expected ']' to close collection literal");
        return new PureCollection(values);
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
    private ValueSpecification parseQualifiedNameStart() {
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
                if (t == TokenType.ALL) {
                    pos++;
                    return parseAllCall(fqn);
                }
                if (t == TokenType.ALL_VERSIONS) {
                    pos++;
                    return parseAllVersionsCall(fqn);
                }
                if (t == TokenType.ALL_VERSIONS_IN_RANGE) {
                    pos++;
                    return parseAllVersionsInRangeCall(fqn);
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
            return new AppliedFunction(fqn, args);
        }
        return new PackageableElementPtr(fqn);
    }

    /**
     * Parse the argument list of {@code Class.all(...)}. Pre-condition:
     * cursor is on the opening {@code (}. Milestoning arguments ({@code %date},
     * {@code %latest}, {@code $variable}) are the only values accepted in this
     * position; any other value would fail in the type-checker so we reject
     * early via {@link #parseMilestoningExpression}. Up to two milestoning
     * args are legal (business-from, business-thru).
     */
    private AppliedFunction parseAllCall(String fqn) {
        expect(TokenType.PAREN_OPEN, "expected '(' after '.all'");
        List<ValueSpecification> args = new ArrayList<>();
        args.add(new PackageableElementPtr(fqn));
        if (!atEnd() && peek() != TokenType.PAREN_CLOSE) {
            args.add(parseMilestoningExpression());
            if (!atEnd() && peek() == TokenType.COMMA) {
                pos++;
                args.add(parseMilestoningExpression());
            }
        }
        expect(TokenType.PAREN_CLOSE, "expected ')' to close '.all(...)'");
        return new AppliedFunction("getAll", args);
    }

    private AppliedFunction parseAllVersionsCall(String fqn) {
        expect(TokenType.PAREN_OPEN, "expected '(' after '.allVersions'");
        expect(TokenType.PAREN_CLOSE, "'.allVersions()' takes no arguments");
        return new AppliedFunction("getAllVersions",
                List.of(new PackageableElementPtr(fqn)));
    }

    private AppliedFunction parseAllVersionsInRangeCall(String fqn) {
        expect(TokenType.PAREN_OPEN, "expected '(' after '.allVersionsInRange'");
        ValueSpecification start = parseMilestoningExpression();
        expect(TokenType.COMMA, "expected ',' between range endpoints in '.allVersionsInRange'");
        ValueSpecification end = parseMilestoningExpression();
        expect(TokenType.PAREN_CLOSE, "expected ')' to close '.allVersionsInRange(...)'");
        return new AppliedFunction("getAllVersionsInRange",
                List.of(new PackageableElementPtr(fqn), start, end));
    }

    /**
     * Parse a single milestoning-position argument. Grammar admits
     * only {@code %2024-01-15} / {@code %2024-01-15T10:30:00}
     * ({@code DATE}), {@code %latest} ({@code LATEST_DATE}), or
     * {@code $var} ({@code DOLLAR}). Anything else is a semantic
     * error better caught at the parser than at the type-checker
     * (one extra token's worth of work to pin the allowed shapes).
     */
    private ValueSpecification parseMilestoningExpression() {
        if (atEnd()) {
            throw error("expected milestoning expression (%date, %latest, or $variable)");
        }
        TokenType t = peek();
        if (t == TokenType.LATEST_DATE) return parseLatestDate();
        if (t == TokenType.DATE) return parseDateOrDateTime();
        if (t == TokenType.DOLLAR) return parseVariable();
        throw error(
                "expected milestoning expression (%date, %latest, or $variable), got "
                + t + " ('" + safeText() + "')");
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
        pos++; // consume '.'
        if (atEnd()) {
            throw error("expected property name after '.'");
        }
        String name = readPropertyName();
        if (!atEnd() && peek() == TokenType.PAREN_OPEN) {
            List<ValueSpecification> args = parseArgList();
            List<ValueSpecification> params = new ArrayList<>(1 + args.size());
            params.add(receiver);
            params.addAll(args);
            return new AppliedFunction(name, params);
        }
        // Enum-value form: 'MyEnum.VALUE' on a PackageableElementPtr
        // receiver. Engine-lite emits EnumValue rather than
        // AppliedProperty here; the disambiguation is purely
        // structural (property access on a class-name doesn't make
        // sense), but committing to the distinct AST shape at parse
        // time avoids a downstream re-walk to re-classify every
        // property access against the model.
        if (receiver instanceof PackageableElementPtr ptr) {
            return new EnumValue(ptr.fullPath(), name);
        }
        return new AppliedProperty(receiver, name);
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
        String fn = parseQualifiedName();
        if (peek() != TokenType.PAREN_OPEN) {
            throw error("expected '(' after arrow-call function name '" + fn + "'");
        }
        List<ValueSpecification> args = parseArgList();
        List<ValueSpecification> params = new ArrayList<>(1 + args.size());
        params.add(receiver);
        params.addAll(args);
        return new AppliedFunction(fn, params);
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
        if (!atEnd() && peek() == TokenType.DOLLAR) {
            pos++; // consume '$'
            if (!isFqnSegmentToken(peek())) {
                throw error("expected variable name after '^$' in copy-with-update");
            }
            String varName = text();
            pos++;
            receiver = new Variable(varName);
            className = ""; // class recovered from $var's static type at typecheck
        } else {
            className = parseQualifiedName();
            if (!atEnd() && peek() == TokenType.LESS_THAN) {
                typeArgs = parseTypeArguments();
            }
            receiver = new PackageableElementPtr(className);
        }
        expect(TokenType.PAREN_OPEN, "expected '(' after class name or $variable in ^NewInstance");
        // LinkedHashMap to preserve source order for the small
        // observable cases (debug pretty-printers, AST dumps);
        // duplicate keys silently last-win matching engine-lite.
        Map<String, KeyExpression> properties = new LinkedHashMap<>();
        if (!atEnd() && peek() == TokenType.PAREN_CLOSE) {
            pos++;
            return wrapNewInstance(receiver, className, typeArgs, properties);
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
        if (!className.isEmpty() && !isFqnSegmentToken(peek())) {
            ValueSpecification src = parseCombinedExpression();
            expect(TokenType.PAREN_CLOSE,
                    "expected ')' to close ^" + className + "($src) positional cast");
            return new AppliedFunction(
                    "new",
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
        return wrapNewInstance(receiver, className, typeArgs, properties);
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
            Map<String, KeyExpression> properties) {
        return new AppliedFunction(
                "new",
                List.of(
                        receiver,
                        new NewInstance(className, typeArgs, properties)));
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
    private void parseAndPutKeyExpression(Map<String, KeyExpression> properties) {
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
        properties.put(key.toString(), new KeyExpression(value, isAdd));
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
        pos++; // consume '<'
        List<TypeExpression> args = new ArrayList<>();
        if (!atEnd() && peek() == TokenType.GREATER_THAN) {
            pos++;
            return args;
        }
        args.add(parseType());
        while (!atEnd() && peek() == TokenType.COMMA) {
            pos++; // consume ','
            args.add(parseType());
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
        expect(TokenType.PIPE, "expected '|' to separate lambda parameters from body");
        List<ValueSpecification> body = parseCodeBlockUntil(TokenType.BRACE_CLOSE);
        if (body.isEmpty()) {
            throw error("lambda body must contain at least one statement");
        }
        expect(TokenType.BRACE_CLOSE, "expected '}' to close lambda");
        return new LambdaFunction(params, body);
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
        String name = text();
        pos++;
        if (peek() != TokenType.COLON) return new Variable(name);
        advance();
        TypeExpression type = parseType();
        Multiplicity multiplicity = parseMultiplicity();
        return new Variable(name, type, multiplicity);
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
        expect(TokenType.PIPE, "expected '|' after shorthand lambda parameter");
        ValueSpecification body = parseCombinedExpression();
        return new LambdaFunction(
                List.of(param),
                List.of(body));
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
        pos++; // consume '|'
        // The body is a STATEMENT SEQUENCE per REAL Pure's grammar
        // (M3ParserGrammar.g4): codeBlock: programLine (';' (programLine ';')*)?
        // — the FIRST statement's ';' is optional; every SUBSEQUENT statement
        // REQUIRES its trailing ';'. "|let a = 1; $a" is invalid Pure.
        List<ValueSpecification> body = new java.util.ArrayList<>();
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
        return new LambdaFunction(List.of(), body);
    }

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
    private ColumnInstance parseColumnBuilders() {
        pos++; // consume '~'
        if (!atEnd() && peek() == TokenType.BRACKET_OPEN) {
            return parseColSpecArray();
        }
        return parseOneColSpec();
    }

    private ColSpec parseOneColSpec() {
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
        LambdaFunction function1 = null;
        LambdaFunction function2 = null;
        if (!atEnd() && peek() == TokenType.COLON) {
            pos++; // consume ':'
            function1 = parseColumnLambda();
            if (!atEnd() && peek() == TokenType.COLON) {
                pos++; // consume ':'
                function2 = parseColumnLambda();
            }
        }
        return new ColSpec(name, function1, function2);
    }

    private ColSpecArray parseColSpecArray() {
        pos++; // consume '['
        List<ColSpec> specs = new ArrayList<>();
        if (!atEnd() && peek() == TokenType.BRACKET_CLOSE) {
            pos++;
            return new ColSpecArray(specs);
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
        return new ColSpecArray(specs);
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
        pos++; // consume '@'
        if (!isFqnSegmentToken(peek())) {
            throw error("expected type name after '@'");
        }
        String name = parseQualifiedName();
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
            return shape;
        }

        // Generic type arguments — parse structurally via the shared
        // helper. Nested '<...>' (e.g. Map<K, List<V>>) is handled by
        // recursive descent inside the helper.
        TypeExpression type;
        if (!atEnd() && peek() == TokenType.LESS_THAN) {
            List<TypeExpression> args = parseTypeArguments();
            type = new TypeExpression.Generic(name, args);
        } else {
            type = new TypeExpression.NameRef(name);
        }
        return new TypeAnnotation.Named(type);
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
        return new TypeAnnotation.RelationShape.Column(name, type, multiplicity);
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
    // Comparator expressions (C.7a): comparator(a:T[1], b:T[1]): Bool[1] { body }
    // -------------------------------------------------------------------

    /**
     * Parse a {@code comparator} expression. Grammar:
     * <pre>
     *   comparatorExpr = 'comparator' '(' typedParam (',' typedParam)* ')'
     *                    ':' type multiplicity '{' statements '}'
     *   typedParam     = identifier ':' type multiplicity
     * </pre>
     *
     * <p>Desugars to a {@link LambdaFunction} whose parameters carry
     * the declared types and multiplicities. Matches engine-lite's
     * behaviour of collapsing the named {@code comparator} syntax
     * into a plain typed-lambda AST &mdash; downstream
     * overload-resolution dispatches on the {@code Comparator<T>}
     * function type which is recovered from the lambda's parameter
     * types, not from a distinct AST variant.
     *
     * <p>The trailing return-type and return-multiplicity are
     * consumed but dropped: the lambda's return type is inferred by
     * the type-checker and the declared annotation is redundant
     * (engine-lite does the same).
     */
    private LambdaFunction parseComparatorExpression() {
        pos++; // consume 'comparator'
        expect(TokenType.PAREN_OPEN, "expected '(' after 'comparator'");
        List<Variable> params = new ArrayList<>();
        params.add(parseComparatorParam());
        while (!atEnd() && peek() == TokenType.COMMA) {
            pos++;
            params.add(parseComparatorParam());
        }
        expect(TokenType.PAREN_CLOSE, "expected ')' to close comparator parameter list");
        expect(TokenType.COLON, "expected ':' and return type after comparator parameters");
        parseType();          // return type, discarded
        parseMultiplicity();  // return multiplicity, discarded
        expect(TokenType.BRACE_OPEN, "expected '{' to open comparator body");
        List<ValueSpecification> body = parseCodeBlockUntil(TokenType.BRACE_CLOSE);
        expect(TokenType.BRACE_CLOSE, "expected '}' to close comparator body");
        return new LambdaFunction(params, body);
    }

    private Variable parseComparatorParam() {
        if (!isFqnSegmentToken(peek())) {
            throw error("expected parameter name in comparator(...)");
        }
        String name = text();
        pos++;
        expect(TokenType.COLON, "comparator parameter requires ': Type[mult]' annotation");
        TypeExpression type = parseType();
        Multiplicity mult = parseMultiplicity();
        return new Variable(name, type, mult);
    }

    // -------------------------------------------------------------------
    // TDS literal (C.7a)
    // -------------------------------------------------------------------

    /**
     * Parse a TDS literal. {@code TDS_LITERAL} is a lexer-side
     * aggregation of a {@code #TDS ... #} test-fixture block into a
     * single token carrying the verbatim CSV-ish body. Engine-lite
     * desugars the literal into
     * {@code AppliedFunction("tds", [CString("TDS"), CString(body)])}
     * so downstream resolution can dispatch on the function name.
     * We emit the same shape; the {@code "TDS"} leading argument is
     * a type-discriminator that the stdlib's {@code tds} function
     * uses to distinguish bag-of-values overloads.
     */
    private AppliedFunction parseTdsLiteral() {
        String raw = text();
        pos++;
        return new AppliedFunction("tds",
                List.of(new CString("TDS"), new CString(raw)));
    }

    // -------------------------------------------------------------------
    // DSL islands (C.7b): #{...}# and #>{...}#
    // -------------------------------------------------------------------

    /**
     * Parse a DSL-island expression. Grammar:
     * <pre>
     *   dsl = ISLAND_OPEN islandContent (ISLAND_END | ISLAND_ARROW_EXIT)
     * </pre>
     *
     * <p>An island is a "hole" in the main Pure grammar where an
     * embedded sub-language is spelled verbatim. The lexer flips
     * into island mode between {@code #{} and {@code }#} (or
     * {@code }-&gt;}) and emits {@code ISLAND_BRACE_OPEN} /
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
     * <p>After the island is consumed, if the closer was
     * {@code ISLAND_ARROW_EXIT} ({@code }-&gt;}), the next tokens
     * are an arrow-chain continuation ({@code .func(args)->...}).
     * We handle this by synthesising an arrow-postfix loop over
     * the produced DSL value. Engine-lite uses a dedicated
     * {@code parseFunctionChainAfterArrow} method; we reuse
     * {@link #parseArrowPostfix} because the only structural
     * difference (the {@code ->} is implicit from
     * ISLAND_ARROW_EXIT) can be handled by treating the first
     * chain call specially.
     */
    private ValueSpecification parseDsl() {
        String islandOpen = text();
        pos++; // consume ISLAND_OPEN
        // Extract DSL discriminator: strip leading '#' and trailing '{'.
        // ISLAND_OPEN text always has that shape by lexer contract.
        String dslType = islandOpen.substring(1, islandOpen.length() - 1);

        StringBuilder content = new StringBuilder();
        boolean arrowExit = false;
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
            } else if (t == TokenType.ISLAND_ARROW_EXIT) {
                if (depth == 0) {
                    pos++;
                    arrowExit = true;
                    break;
                }
                depth--;   // a NESTED island closed by '}->' (re-audit M7)
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
            case "" -> parseGraphFetchTree(contentText);
            case ">" -> parseTableReference(contentText);
            default -> throw error(
                    "unknown DSL island type: '#" + dslType + "{'");
        };

        // Post-island arrow chain: if the closer was '}->' then the
        // next tokens form a function-call chain whose first arrow
        // has already been consumed as part of ISLAND_ARROW_EXIT.
        // We inject a synthetic call by running the standard
        // arrow-chain machinery once, then let the main postfix
        // loop handle any subsequent arrows.
        if (arrowExit) {
            result = parseArrowChainAfterIslandExit(result);
        }
        return result;
    }

    /**
     * After {@code ISLAND_ARROW_EXIT} the next tokens look like
     * {@code funcName(args)} (no leading {@code ->} because the
     * exit token consumed it). Engine-lite calls this
     * {@code parseFunctionChainAfterArrow}; we mirror the shape.
     * After the first synthesised call, the main postfix loop
     * picks up any subsequent explicit {@code ->} arrows.
     */
    private ValueSpecification parseArrowChainAfterIslandExit(ValueSpecification source) {
        String funcName = parseQualifiedName();
        List<ValueSpecification> args = parseArgList();
        List<ValueSpecification> params = new ArrayList<>(1 + args.size());
        params.add(source);
        params.addAll(args);
        return new AppliedFunction(funcName, params);
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
    private AppliedFunction parseTableReference(String content) {
        // Split at the FIRST dot after the ::-path: "db::DB.schema.TABLE"
        // is (db::DB, "schema.TABLE") — lastIndexOf('.') mis-split it into
        // an FQN with an embedded dot (audit M7). The db part is the
        // ::-qualified prefix; everything after its first dot is the
        // (possibly schema-qualified) table name, matching the element
        // parser's SCHEMA.TABLE folding.
        int pathEnd = content.lastIndexOf("::");
        int dot = content.indexOf('.', pathEnd < 0 ? 0 : pathEnd + 2);
        if (dot < 0) {
            throw error(
                    "table reference must be db.TABLE, got: '" + content + "'");
        }
        String db = content.substring(0, dot);
        String tableName = content.substring(dot + 1);
        return new AppliedFunction("tableReference",
                List.of(new PackageableElementPtr(db), new CString(tableName)));
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
    private ValueSpecification parseGraphFetchTree(String content) {
        TokenStream innerTokens = Lexer.tokenize(content);
        SpecParser inner = new SpecParser(innerTokens);
        inner.parseQualifiedName();          // skip root class name
        ValueSpecification tree = inner.parseGraphDefinition(0);
        if (!inner.atEnd()) {
            // LOUD: #{Person {name} GARBAGE}# previously dropped GARBAGE
            // silently (audit M8c).
            throw inner.error("trailing content after graph-fetch tree: '"
                    + inner.safeText() + "'");
        }
        return tree;
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
                    break; // trailing comma tolerated
                }
                specs.add(parseGraphPath(depth));
            }
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
        // Optional alias: 'aliasName': property
        if (pos + 1 < tokens.count()
                && peek() == TokenType.STRING
                && tokens.type(pos + 1) == TokenType.COLON) {
            pos += 2; // skip alias and colon
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

        // Optional propertyParameters (milestoning args): consume to close.
        if (!atEnd() && peek() == TokenType.PAREN_OPEN) {
            int depthParens = 1;
            pos++;
            while (!atEnd() && depthParens > 0) {
                TokenType t = peek();
                if (t == TokenType.PAREN_OPEN) depthParens++;
                else if (t == TokenType.PAREN_CLOSE) depthParens--;
                if (depthParens > 0) pos++;
            }
            if (atEnd()) {
                throw error("unterminated graph-fetch property parameters");
            }
            pos++; // consume matching ')'
        }

        // Optional nested graph definition: { subpaths }
        if (!atEnd() && peek() == TokenType.BRACE_OPEN) {
            ColSpecArray nested = parseGraphDefinition(depth + 1);
            // function2 wraps the nested array in a zero-param lambda
            // so the two ColSpec slots are uniformly typed (LambdaFunction).
            LambdaFunction fn2 = new LambdaFunction(
                    List.of(), List.of(nested));
            return new ColSpec(propName, fn1, fn2);
        }

        return new ColSpec(propName, fn1, null);
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
