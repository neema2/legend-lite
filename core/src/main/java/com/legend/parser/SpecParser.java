package com.legend.parser;

import com.legend.lexer.Lexer;
import com.legend.lexer.TokenStream;
import com.legend.lexer.TokenType;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.AppliedProperty;
import com.legend.parser.spec.CBoolean;
import com.legend.parser.spec.CDateTime;
import com.legend.parser.spec.CDecimal;
import com.legend.parser.spec.CFloat;
import com.legend.parser.spec.CInteger;
import com.legend.parser.spec.CLatestDate;
import com.legend.parser.spec.CStrictDate;
import com.legend.parser.spec.CStrictTime;
import com.legend.parser.spec.CString;
import com.legend.parser.spec.ColSpec;
import com.legend.parser.spec.ColSpecArray;
import com.legend.parser.spec.ColumnInstance;
import com.legend.parser.spec.KeyExpression;
import com.legend.parser.spec.LambdaFunction;
import com.legend.parser.spec.Multiplicity;
import com.legend.parser.spec.NewInstance;
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
 *       {@link CBoolean}, {@link CDateTime}, {@link CStrictDate},
 *       {@link CStrictTime}, {@link CLatestDate} ({@code %latest}).</li>
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
public final class SpecParser {

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
        if (parser.pos < tokens.count()) {
            ElementParser.throwAt(tokens, parser.pos,
                    "trailing tokens after expression: " + tokens.type(parser.pos)
                    + " ('" + safeText(tokens, parser.pos) + "')");
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
     * <p>This is the entry point ModelOrchestrator will use to parse
     * function bodies in C.6. It is also used internally by the
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
        if (parser.pos < tokens.count()) {
            ElementParser.throwAt(tokens, parser.pos,
                    "trailing tokens after code block: " + tokens.type(parser.pos)
                    + " ('" + safeText(tokens, parser.pos) + "')");
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
        if (pos < tokens.count() && tokens.type(pos) == TokenType.LET) {
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
        if (pos >= tokens.count() || !isFqnSegmentToken(tokens.type(pos))) {
            ElementParser.throwAt(tokens, pos,
                    "expected variable name after 'let'");
        }
        String varName = tokens.text(pos);
        pos++;
        if (pos >= tokens.count() || tokens.type(pos) != TokenType.EQUAL) {
            ElementParser.throwAt(tokens, pos,
                    "expected '=' after 'let " + varName + "'");
        }
        pos++; // consume '='
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
        while (pos < tokens.count() && tokens.type(pos) == TokenType.SEMI_COLON) {
            pos++; // consume ';'
            if (atTerminator(terminator)) {
                break;
            }
            stmts.add(parseProgramLine());
        }
        return stmts;
    }

    private boolean atTerminator(TokenType terminator) {
        if (pos >= tokens.count()) {
            return true; // EOF terminates
        }
        return terminator != null && tokens.type(pos) == terminator;
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
        while (pos < tokens.count()) {
            TokenType t = tokens.type(pos);
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
        while (pos < tokens.count()) {
            TokenType t = tokens.type(pos);
            if (t == TokenType.DOT) {
                expr = parseDotPostfix(expr);
            } else if (t == TokenType.ARROW) {
                expr = parseArrowPostfix(expr);
            } else {
                break;
            }
        }
        if (pos < tokens.count()) {
            TokenType t = tokens.type(pos);
            // Pure spells inequality two ways: '!=' (lexed as
            // TEST_NOT_EQUAL) and '<>' (lexed as NOT_EQUAL). Both
            // desugar to the same 'notEqual' AppliedFunction so the
            // model layer doesn't have to know which form was written.
            if (t == TokenType.TEST_EQUAL
                    || t == TokenType.TEST_NOT_EQUAL
                    || t == TokenType.NOT_EQUAL) {
                String fn = (t == TokenType.TEST_EQUAL) ? "equal" : "notEqual";
                pos++;
                ValueSpecification right = parseCombinedArithmeticOnly();
                expr = new AppliedFunction(fn, List.of(expr, right));
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
        if (pos < tokens.count()) {
            TokenType t = tokens.type(pos);
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
        while (pos < tokens.count() && isArithmeticOp(tokens.type(pos))) {
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
        TokenType t = tokens.type(pos);
        String fn = (t == TokenType.AND) ? "and" : "or";
        pos++;
        ValueSpecification right = parseCombinedArithmeticOnly();
        return new AppliedFunction(fn, List.of(left, right));
    }

    /**
     * Arithmetic and comparison operators desugared to
     * {@link AppliedFunction}s. Pure's grammar treats all of
     * {@code +}, {@code -}, {@code *}, {@code /}, {@code <},
     * {@code <=}, {@code >}, {@code >=} as a single flat precedence
     * level &mdash; there is no {@code *} binding tighter than
     * {@code +}. Same-operator runs ({@code 1 + 2 + 3}) are flattened
     * into a chain of same-named {@link AppliedFunction}s; different
     * operators ({@code 1 + 2 * 3}) leave this method and re-enter via
     * the {@link #parseCombinedExpression()} loop, yielding
     * {@code times(plus(1, 2), 3)} &mdash; left-associative across
     * operator kinds.
     */
    private AppliedFunction parseArithmeticPart(ValueSpecification left) {
        TokenType op = tokens.type(pos);
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
                    "parseArithmeticPart invoked on non-arithmetic token: " + op);
        };
        pos++;
        ValueSpecification right = parseExpression();
        AppliedFunction result = new AppliedFunction(fn, List.of(left, right));
        // Same-operator chaining is only meaningful for the binary
        // arithmetic ops; comparison ops do not chain ('a < b < c' is
        // not legal Pure).
        if (op == TokenType.PLUS || op == TokenType.MINUS
                || op == TokenType.STAR || op == TokenType.DIVIDE) {
            while (pos < tokens.count() && tokens.type(pos) == op) {
                pos++;
                right = parseExpression();
                result = new AppliedFunction(fn, List.of(result, right));
            }
        }
        return result;
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
        if (pos >= tokens.count()) {
            ElementParser.throwAt(tokens, pos, "expected expression, got end of input");
        }
        TokenType t = tokens.type(pos);
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
            default -> {
                if (ElementParser.IDENTIFIER_TOKENS.contains(t)) {
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
                ElementParser.throwAt(tokens, pos,
                        "unsupported expression token: " + t
                        + " ('" + safeText(tokens, pos) + "')");
                yield null; // unreachable; throwAt does not return
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
        String text = tokens.text(pos);
        pos++;
        try {
            return new CInteger(Long.parseLong(text));
        } catch (NumberFormatException overflow) {
            return new CInteger(new BigInteger(text));
        }
    }

    private CFloat parseFloat() {
        String text = tokens.text(pos);
        pos++;
        // Strip optional 'f'/'F' suffix; Pure permits it on float literals
        // but Java's Double.parseDouble does not.
        if (!text.isEmpty()) {
            char last = text.charAt(text.length() - 1);
            if (last == 'f' || last == 'F') text = text.substring(0, text.length() - 1);
        }
        return new CFloat(Double.parseDouble(text));
    }

    /**
     * DECIMAL token &rarr; {@link CDecimal}. The lexer admits both
     * {@code 42d} (integer-shaped) and {@code 3.14d} (float-shaped)
     * forms; both end with a {@code d}/{@code D} that {@link BigDecimal}
     * does not accept, so it is stripped before parsing.
     */
    private CDecimal parseDecimal() {
        String text = tokens.text(pos);
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
        String raw = tokens.text(pos);
        if (raw.length() < 2 || raw.charAt(0) != '\'' || raw.charAt(raw.length() - 1) != '\'') {
            ElementParser.throwAt(tokens, pos,
                    "malformed string literal: missing surrounding quotes");
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
                ElementParser.throwAt(tokens, pos,
                        "malformed string literal: trailing backslash");
            }
            char esc = body.charAt(i + 1);
            switch (esc) {
                case '\\' -> sb.append('\\');
                case '\'' -> sb.append('\'');
                case 'n' -> sb.append('\n');
                case 't' -> sb.append('\t');
                case 'r' -> sb.append('\r');
                default -> ElementParser.throwAt(tokens, pos,
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
     * DATE token &rarr; {@link CDateTime} or {@link CStrictDate}. The
     * lexer collapses both into a single token type; the discriminator
     * is the presence of the time-separator {@code T} in the source
     * text (the {@code %} prefix is stripped here, mirroring engine's
     * record contract).
     */
    private ValueSpecification parseDateOrDateTime() {
        String raw = tokens.text(pos);
        if (raw.isEmpty() || raw.charAt(0) != '%') {
            ElementParser.throwAt(tokens, pos,
                    "malformed date literal: expected leading '%'");
        }
        String value = raw.substring(1);
        pos++;
        return value.indexOf('T') >= 0 ? new CDateTime(value) : new CStrictDate(value);
    }

    private CStrictTime parseStrictTime() {
        String raw = tokens.text(pos);
        if (raw.isEmpty() || raw.charAt(0) != '%') {
            ElementParser.throwAt(tokens, pos,
                    "malformed time literal: expected leading '%'");
        }
        pos++;
        return new CStrictTime(raw.substring(1));
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
        if (pos >= tokens.count() || !ElementParser.IDENTIFIER_TOKENS.contains(tokens.type(pos))) {
            ElementParser.throwAt(tokens, pos,
                    "expected identifier after '$' to form a variable reference");
        }
        String name = tokens.text(pos);
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
        if (pos < tokens.count() && tokens.type(pos) == TokenType.BRACKET_CLOSE) {
            pos++;
            return new PureCollection(values);
        }
        values.add(parseCombinedExpression());
        while (pos < tokens.count() && tokens.type(pos) == TokenType.COMMA) {
            pos++; // consume ','
            if (pos < tokens.count() && tokens.type(pos) == TokenType.BRACKET_CLOSE) {
                ElementParser.throwAt(tokens, pos,
                        "trailing comma in collection literal");
            }
            values.add(parseCombinedExpression());
        }
        if (pos >= tokens.count() || tokens.type(pos) != TokenType.BRACKET_CLOSE) {
            ElementParser.throwAt(tokens, pos,
                    "expected ']' to close collection literal");
        }
        pos++; // consume ']'
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
        if (pos >= tokens.count() || tokens.type(pos) != TokenType.PAREN_CLOSE) {
            ElementParser.throwAt(tokens, pos,
                    "expected ')' to close parenthesised expression");
        }
        pos++; // consume ')'
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
    private ValueSpecification parseQualifiedNameStart() {
        String fqn = parseQualifiedName();
        if (pos < tokens.count() && tokens.type(pos) == TokenType.PAREN_OPEN) {
            List<ValueSpecification> args = parseArgList();
            return new AppliedFunction(fqn, args);
        }
        return new PackageableElementPtr(fqn);
    }

    /**
     * IDENT ({@code ::} IDENT)* &rarr; reconstructed FQN string. Uses
     * {@link ElementParser#IDENTIFIER_TOKENS} so that keyword-as-
     * identifier names ({@code let}, {@code class}, {@code all}, ...)
     * are admitted everywhere an identifier is required, matching the
     * element parser's contract.
     */
    private String parseQualifiedName() {
        if (pos >= tokens.count() || !isFqnSegmentToken(tokens.type(pos))) {
            ElementParser.throwAt(tokens, pos,
                    "expected identifier (qualified-name start)");
        }
        StringBuilder sb = new StringBuilder();
        sb.append(tokens.text(pos));
        pos++;
        while (pos + 1 < tokens.count()
                && tokens.type(pos) == TokenType.PATH_SEPARATOR
                && isFqnSegmentToken(tokens.type(pos + 1))) {
            sb.append("::").append(tokens.text(pos + 1));
            pos += 2;
        }
        return sb.toString();
    }

    /**
     * FQN segments may be any identifier-shaped token EXCEPT
     * {@link TokenType#STRING}. The element parser admits {@code STRING}
     * as an "identifier" in some keyword positions (which is why
     * {@link ElementParser#IDENTIFIER_TOKENS} includes it), but a Pure
     * qualified name like {@code foo::'bar'} is not legal: FQN
     * components are bare identifiers. Filtering {@code STRING} out
     * here keeps a stray quoted string from sneaking into an FQN
     * field with its surrounding quotes intact.
     */
    private static boolean isFqnSegmentToken(TokenType t) {
        return t != TokenType.STRING && ElementParser.IDENTIFIER_TOKENS.contains(t);
    }

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
        if (pos >= tokens.count()) {
            ElementParser.throwAt(tokens, pos,
                    "expected property name after '.'");
        }
        String name = readPropertyName();
        if (pos < tokens.count() && tokens.type(pos) == TokenType.PAREN_OPEN) {
            List<ValueSpecification> args = parseArgList();
            List<ValueSpecification> params = new ArrayList<>(1 + args.size());
            params.add(receiver);
            params.addAll(args);
            return new AppliedFunction(name, params);
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
        TokenType t = tokens.type(pos);
        // STRING must come FIRST: it is also a member of IDENTIFIER_TOKENS
        // (the element parser admits quoted strings as identifiers in
        // some keyword positions), so the bare-identifier branch below
        // would otherwise capture the quoted form including its outer
        // quotes \u2014 yielding {@code "'My Name'"} as the property name
        // instead of {@code "My Name"}.
        if (t == TokenType.STRING) {
            String raw = tokens.text(pos);
            if (raw.length() < 2 || raw.charAt(0) != '\'' || raw.charAt(raw.length() - 1) != '\'') {
                ElementParser.throwAt(tokens, pos,
                        "malformed quoted property: missing surrounding quotes");
            }
            String name = unescapeString(raw.substring(1, raw.length() - 1));
            pos++;
            return name;
        }
        if (ElementParser.IDENTIFIER_TOKENS.contains(t)) {
            String name = tokens.text(pos);
            pos++;
            return name;
        }
        ElementParser.throwAt(tokens, pos,
                "expected property name (identifier or 'quoted name') after '.'");
        return null; // unreachable
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
        if (pos >= tokens.count() || tokens.type(pos) != TokenType.PAREN_OPEN) {
            ElementParser.throwAt(tokens, pos,
                    "expected '(' after arrow-call function name '" + fn + "'");
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
        if (pos < tokens.count() && tokens.type(pos) == TokenType.PAREN_CLOSE) {
            pos++;
            return args;
        }
        args.add(parseCombinedExpression());
        while (pos < tokens.count() && tokens.type(pos) == TokenType.COMMA) {
            pos++; // consume ','
            if (pos < tokens.count() && tokens.type(pos) == TokenType.PAREN_CLOSE) {
                ElementParser.throwAt(tokens, pos,
                        "trailing comma in argument list");
            }
            args.add(parseCombinedExpression());
        }
        if (pos >= tokens.count() || tokens.type(pos) != TokenType.PAREN_CLOSE) {
            ElementParser.throwAt(tokens, pos,
                    "expected ')' to close argument list");
        }
        pos++; // consume ')'
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
        String className = parseQualifiedName();
        List<String> typeArgs = List.of();
        if (pos < tokens.count() && tokens.type(pos) == TokenType.LESS_THAN) {
            typeArgs = parseTypeArguments();
        }
        if (pos >= tokens.count() || tokens.type(pos) != TokenType.PAREN_OPEN) {
            ElementParser.throwAt(tokens, pos,
                    "expected '(' after class name in ^NewInstance");
        }
        pos++; // consume '('
        // LinkedHashMap to preserve source order for the small
        // observable cases (debug pretty-printers, AST dumps);
        // duplicate keys silently last-win matching engine-lite.
        Map<String, KeyExpression> properties = new LinkedHashMap<>();
        if (pos < tokens.count() && tokens.type(pos) == TokenType.PAREN_CLOSE) {
            pos++;
            return wrapNewInstance(className, typeArgs, properties);
        }
        parseAndPutKeyExpression(properties);
        while (pos < tokens.count() && tokens.type(pos) == TokenType.COMMA) {
            pos++; // consume ','
            if (pos < tokens.count() && tokens.type(pos) == TokenType.PAREN_CLOSE) {
                ElementParser.throwAt(tokens, pos,
                        "trailing comma in ^NewInstance binding list");
            }
            parseAndPutKeyExpression(properties);
        }
        if (pos >= tokens.count() || tokens.type(pos) != TokenType.PAREN_CLOSE) {
            ElementParser.throwAt(tokens, pos,
                    "expected ')' to close ^NewInstance");
        }
        pos++; // consume ')'
        return wrapNewInstance(className, typeArgs, properties);
    }

    /**
     * Build the {@code AppliedFunction("new", [PE, NewInstance])}
     * wrapper around a parsed {@link NewInstance} payload. Factored
     * out because both the empty-bindings and the populated-bindings
     * paths in {@link #parseNewInstance()} produce the same shape.
     */
    private AppliedFunction wrapNewInstance(
            String className,
            List<String> typeArgs,
            Map<String, KeyExpression> properties) {
        return new AppliedFunction(
                "new",
                List.of(
                        new PackageableElementPtr(className),
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
        if (pos >= tokens.count() || !isFqnSegmentToken(tokens.type(pos))) {
            ElementParser.throwAt(tokens, pos,
                    "expected property name in ^NewInstance binding");
        }
        String key = tokens.text(pos);
        pos++;
        boolean isAdd = false;
        if (pos < tokens.count() && tokens.type(pos) == TokenType.PLUS) {
            isAdd = true;
            pos++; // consume '+'
        }
        if (pos >= tokens.count() || tokens.type(pos) != TokenType.EQUAL) {
            ElementParser.throwAt(tokens, pos,
                    "expected '" + (isAdd ? "+=" : "=")
                    + "' after property name '" + key
                    + "' in ^NewInstance binding");
        }
        pos++; // consume '='
        ValueSpecification value = parseCombinedExpression();
        properties.put(key, new KeyExpression(value, isAdd));
    }

    private List<String> parseTypeArguments() {
        pos++; // consume '<'
        List<String> args = new ArrayList<>();
        if (pos < tokens.count() && tokens.type(pos) == TokenType.GREATER_THAN) {
            pos++;
            return args;
        }
        args.add(parseQualifiedName());
        while (pos < tokens.count() && tokens.type(pos) == TokenType.COMMA) {
            pos++; // consume ','
            args.add(parseQualifiedName());
        }
        if (pos >= tokens.count() || tokens.type(pos) != TokenType.GREATER_THAN) {
            ElementParser.throwAt(tokens, pos,
                    "expected '>' to close type arguments");
        }
        pos++; // consume '>'
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
        if (pos < tokens.count() && tokens.type(pos) != TokenType.PIPE) {
            params.add(parseLambdaParam());
            while (pos < tokens.count() && tokens.type(pos) == TokenType.COMMA) {
                pos++; // consume ','
                params.add(parseLambdaParam());
            }
        }
        if (pos >= tokens.count() || tokens.type(pos) != TokenType.PIPE) {
            ElementParser.throwAt(tokens, pos,
                    "expected '|' to separate lambda parameters from body");
        }
        pos++; // consume '|'
        List<ValueSpecification> body = parseCodeBlockUntil(TokenType.BRACE_CLOSE);
        if (body.isEmpty()) {
            ElementParser.throwAt(tokens, pos,
                    "lambda body must contain at least one statement");
        }
        if (pos >= tokens.count() || tokens.type(pos) != TokenType.BRACE_CLOSE) {
            ElementParser.throwAt(tokens, pos,
                    "expected '}' to close lambda");
        }
        pos++; // consume '}'
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
        if (pos >= tokens.count() || !isFqnSegmentToken(tokens.type(pos))) {
            ElementParser.throwAt(tokens, pos,
                    "expected lambda parameter name");
        }
        String name = tokens.text(pos);
        pos++;
        if (pos >= tokens.count() || tokens.type(pos) != TokenType.COLON) {
            return new Variable(name);
        }
        pos++; // consume ':'
        String typeName = parseTypeText();
        Multiplicity multiplicity = parseMultiplicity();
        return new Variable(name, typeName, multiplicity);
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
        if (pos >= tokens.count() || tokens.type(pos) != TokenType.PIPE) {
            ElementParser.throwAt(tokens, pos,
                    "expected '|' after shorthand lambda parameter");
        }
        pos++; // consume '|'
        ValueSpecification body = parseCombinedExpression();
        return new LambdaFunction(
                List.of(param),
                List.of(body));
    }

    // -------------------------------------------------------------------
    // Type and multiplicity annotations (C.5)
    // -------------------------------------------------------------------

    /**
     * Parse the type portion of a typed lambda parameter (or other
     * annotated context). Returns a raw source string &mdash; the
     * type-checker resolves the name to a concrete type by
     * consulting the model.
     *
     * <p>Grammar (C.5 minimal subset):
     * <pre>
     *   type = qualifiedName ('&lt;' typeArgs '&gt;')?
     * </pre>
     *
     * <p>Function types ({@code {T[m] -> U[m]}}) and relation types
     * ({@code (col: T, ...)}) appear in stdlib signatures but not in
     * the lambda-parameter position we currently support. They land
     * with the rest of the type-grammar in C.6/C.7.
     */
    private String parseTypeText() {
        if (pos >= tokens.count() || !isFqnSegmentToken(tokens.type(pos))) {
            ElementParser.throwAt(tokens, pos,
                    "expected type name after ':'");
        }
        StringBuilder sb = new StringBuilder(parseQualifiedName());
        if (pos < tokens.count() && tokens.type(pos) == TokenType.LESS_THAN) {
            // Type arguments — nested generics, so depth-track and
            // splice the source text verbatim. The type-checker
            // re-parses this if it needs to inspect the args.
            sb.append('<');
            pos++;
            int depth = 1;
            while (pos < tokens.count() && depth > 0) {
                TokenType t = tokens.type(pos);
                if (t == TokenType.LESS_THAN) {
                    depth++;
                } else if (t == TokenType.GREATER_THAN) {
                    depth--;
                    if (depth == 0) {
                        pos++;
                        break;
                    }
                }
                sb.append(tokens.text(pos));
                pos++;
            }
            sb.append('>');
            if (depth != 0) {
                ElementParser.throwAt(tokens, pos,
                        "unterminated type-argument list in type annotation");
            }
        }
        return sb.toString();
    }

    /**
     * Parse a multiplicity annotation into a structured
     * {@link Multiplicity}. Grammar:
     * <pre>
     *   multiplicity = '[' (concrete | parameter) ']'
     *   concrete     = '*'
     *                | INTEGER ('..' ('*' | INTEGER))?
     *   parameter    = identifier         // e.g. 'm' in T[m]
     * </pre>
     *
     * <p>Engine-lite produces a similar structured value (their
     * {@code Multiplicity.Bounded} / {@code Var}); matching the
     * shape (rather than carrying raw text) lets the type-checker
     * consume the result directly with no re-parse step.
     */
    private Multiplicity parseMultiplicity() {
        if (pos >= tokens.count() || tokens.type(pos) != TokenType.BRACKET_OPEN) {
            ElementParser.throwAt(tokens, pos,
                    "expected '[' to open multiplicity annotation");
        }
        pos++; // consume '['
        Multiplicity result = parseMultiplicityBody();
        if (pos >= tokens.count() || tokens.type(pos) != TokenType.BRACKET_CLOSE) {
            ElementParser.throwAt(tokens, pos,
                    "expected ']' to close multiplicity annotation");
        }
        pos++; // consume ']'
        return result;
    }

    private Multiplicity parseMultiplicityBody() {
        if (pos >= tokens.count()) {
            ElementParser.throwAt(tokens, pos,
                    "unexpected end-of-input inside multiplicity annotation");
        }
        TokenType t = tokens.type(pos);
        if (t == TokenType.STAR) {
            // '*' alone is shorthand for 0..* (ZERO_MANY).
            pos++;
            return Multiplicity.Concrete.ZERO_MANY;
        }
        if (t == TokenType.INTEGER) {
            int lower = Integer.parseInt(tokens.text(pos));
            pos++;
            if (pos < tokens.count() && tokens.type(pos) == TokenType.DOT_DOT) {
                pos++;
                if (pos >= tokens.count()) {
                    ElementParser.throwAt(tokens, pos,
                            "expected upper bound after '..' in multiplicity");
                }
                if (tokens.type(pos) == TokenType.STAR) {
                    pos++;
                    return new Multiplicity.Concrete(lower, null);
                }
                if (tokens.type(pos) != TokenType.INTEGER) {
                    ElementParser.throwAt(tokens, pos,
                            "expected integer or '*' as upper bound, got "
                            + tokens.type(pos));
                }
                int upperPos = pos;
                int upper = Integer.parseInt(tokens.text(pos));
                pos++;
                // Pre-check the bounds at the parser layer so the
                // error carries source line/col. Multiplicity.Concrete's
                // constructor enforces the same invariant defensively
                // (for programmatic constructors), but we want a
                // ParseException here, not a raw IllegalArgumentException.
                if (upper < lower) {
                    ElementParser.throwAt(tokens, upperPos,
                            "multiplicity upper bound (" + upper
                            + ") must be >= lower bound (" + lower + ")");
                }
                return new Multiplicity.Concrete(lower, upper);
            }
            // Single integer: '[N]' means '[N..N]'.
            return new Multiplicity.Concrete(lower, lower);
        }
        if (isFqnSegmentToken(t)) {
            // Multiplicity variable, e.g. the 'm' in T[m]. Pervasive
            // in stdlib native function signatures (letFunction, if,
            // cast, match, reverse, sort, map). User code typically
            // uses concrete multiplicities; legend-engine's user
            // grammar walker (DomainParseTreeWalker) rejects them,
            // but stdlib parsing must admit them.
            String name = tokens.text(pos);
            pos++;
            return new Multiplicity.Parameter(name);
        }
        ElementParser.throwAt(tokens, pos,
                "unexpected token in multiplicity annotation: " + t
                + " ('" + safeText(tokens, pos) + "')");
        return null; // unreachable
    }

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
            if (pos >= tokens.count() || !isFqnSegmentToken(tokens.type(pos))) {
                return false;
            }
            pos++; // identifier
            if (pos >= tokens.count() || tokens.type(pos) != TokenType.COLON) {
                return false;
            }
            pos++; // ':'
            if (!skipTypeForLookahead()) {
                return false;
            }
            if (pos < tokens.count() && tokens.type(pos) == TokenType.BRACKET_OPEN) {
                if (!skipMultiplicityForLookahead()) {
                    return false;
                }
            }
            return pos < tokens.count() && tokens.type(pos) == TokenType.PIPE;
        } finally {
            pos = saved;
        }
    }

    private boolean skipTypeForLookahead() {
        if (pos >= tokens.count() || !isFqnSegmentToken(tokens.type(pos))) {
            return false;
        }
        pos++;
        while (pos < tokens.count() && tokens.type(pos) == TokenType.PATH_SEPARATOR) {
            pos++;
            if (pos >= tokens.count() || !isFqnSegmentToken(tokens.type(pos))) {
                return false;
            }
            pos++;
        }
        if (pos < tokens.count() && tokens.type(pos) == TokenType.LESS_THAN) {
            int depth = 1;
            pos++;
            while (pos < tokens.count() && depth > 0) {
                TokenType t = tokens.type(pos);
                if (t == TokenType.LESS_THAN) depth++;
                if (t == TokenType.GREATER_THAN) depth--;
                pos++;
            }
            if (depth != 0) return false;
        }
        return true;
    }

    private boolean skipMultiplicityForLookahead() {
        if (pos >= tokens.count() || tokens.type(pos) != TokenType.BRACKET_OPEN) {
            return false;
        }
        pos++;
        while (pos < tokens.count() && tokens.type(pos) != TokenType.BRACKET_CLOSE) {
            pos++;
        }
        if (pos >= tokens.count()) return false;
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
        ValueSpecification body = parseCombinedExpression();
        return new LambdaFunction(List.of(), List.of(body));
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
        if (pos < tokens.count() && tokens.type(pos) == TokenType.BRACKET_OPEN) {
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
        if (pos >= tokens.count()) {
            ElementParser.throwAt(tokens, pos,
                    "expected column name after '~'");
        }
        TokenType nameTok = tokens.type(pos);
        String name;
        if (nameTok == TokenType.STRING) {
            String raw = tokens.text(pos);
            if (raw.length() < 2 || raw.charAt(0) != '\''
                    || raw.charAt(raw.length() - 1) != '\'') {
                ElementParser.throwAt(tokens, pos,
                        "malformed quoted column name: missing surrounding quotes");
            }
            name = unescapeString(raw.substring(1, raw.length() - 1));
            pos++;
        } else if (isFqnSegmentToken(nameTok)) {
            name = tokens.text(pos);
            pos++;
        } else {
            ElementParser.throwAt(tokens, pos,
                    "expected column name after '~'");
            return null; // unreachable
        }
        LambdaFunction function1 = null;
        LambdaFunction function2 = null;
        if (pos < tokens.count() && tokens.type(pos) == TokenType.COLON) {
            pos++; // consume ':'
            function1 = parseColumnLambda();
            if (pos < tokens.count() && tokens.type(pos) == TokenType.COLON) {
                pos++; // consume ':'
                function2 = parseColumnLambda();
            }
        }
        return new ColSpec(name, function1, function2);
    }

    private ColSpecArray parseColSpecArray() {
        pos++; // consume '['
        List<ColSpec> specs = new ArrayList<>();
        if (pos < tokens.count() && tokens.type(pos) == TokenType.BRACKET_CLOSE) {
            pos++;
            return new ColSpecArray(specs);
        }
        specs.add(parseOneColSpec());
        while (pos < tokens.count() && tokens.type(pos) == TokenType.COMMA) {
            pos++; // consume ','
            if (pos < tokens.count() && tokens.type(pos) == TokenType.BRACKET_CLOSE) {
                ElementParser.throwAt(tokens, pos,
                        "trailing comma in ColSpec array");
            }
            specs.add(parseOneColSpec());
        }
        if (pos >= tokens.count() || tokens.type(pos) != TokenType.BRACKET_CLOSE) {
            ElementParser.throwAt(tokens, pos,
                    "expected ']' to close ColSpec array");
        }
        pos++; // consume ']'
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
        if (pos >= tokens.count()) {
            ElementParser.throwAt(tokens, pos,
                    "expected lambda after ':' in column spec");
        }
        TokenType t = tokens.type(pos);
        if (t == TokenType.PIPE) {
            return parseLambdaPipe();
        }
        if (t == TokenType.BRACE_OPEN) {
            return parseLambdaFunction();
        }
        if (ElementParser.IDENTIFIER_TOKENS.contains(t)) {
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
        ElementParser.throwAt(tokens, pos,
                "expected lambda after ':' in column spec, got " + t
                + " ('" + safeText(tokens, pos) + "')");
        return null; // unreachable
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
     * <p>For non-relation generic types ({@code @List<Integer>})
     * the type-argument list is collected verbatim into the
     * {@link TypeAnnotation.Named#typeName() typeName} via the same
     * token-concatenation mechanism {@link #parseTypeText()} uses.
     * Whitespace between tokens is lost (documented C.5 quirk); a
     * downstream re-parse can recover structure if needed.
     */
    private TypeAnnotation parseTypeAnnotation() {
        pos++; // consume '@'
        if (pos >= tokens.count() || !isFqnSegmentToken(tokens.type(pos))) {
            ElementParser.throwAt(tokens, pos,
                    "expected type name after '@'");
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
                && pos < tokens.count()
                && tokens.type(pos) == TokenType.LESS_THAN
                && pos + 1 < tokens.count()
                && tokens.type(pos + 1) == TokenType.PAREN_OPEN) {
            pos++; // consume '<'
            TypeAnnotation.RelationShape shape = parseRelationShape();
            if (pos >= tokens.count() || tokens.type(pos) != TokenType.GREATER_THAN) {
                ElementParser.throwAt(tokens, pos,
                        "expected '>' to close @Relation type annotation");
            }
            pos++; // consume '>'
            return shape;
        }

        // Generic type arguments — collect verbatim into typeName.
        // Same depth-tracking as parseTypeText so nested
        // '<...>' (e.g. Map<K, List<V>>) round-trips correctly.
        if (pos < tokens.count() && tokens.type(pos) == TokenType.LESS_THAN) {
            StringBuilder sb = new StringBuilder(name);
            sb.append('<');
            pos++;
            int depth = 1;
            while (pos < tokens.count() && depth > 0) {
                TokenType tt = tokens.type(pos);
                if (tt == TokenType.LESS_THAN) {
                    depth++;
                } else if (tt == TokenType.GREATER_THAN) {
                    depth--;
                    if (depth == 0) {
                        pos++;
                        break;
                    }
                }
                sb.append(tokens.text(pos));
                pos++;
            }
            sb.append('>');
            if (depth != 0) {
                ElementParser.throwAt(tokens, pos,
                        "unterminated type-argument list in @"
                        + name + " annotation");
            }
            name = sb.toString();
        }

        return new TypeAnnotation.Named(name);
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
        if (pos < tokens.count() && tokens.type(pos) != TokenType.PAREN_CLOSE) {
            columns.add(parseRelationColumn());
            while (pos < tokens.count() && tokens.type(pos) == TokenType.COMMA) {
                pos++; // consume ','
                if (pos < tokens.count() && tokens.type(pos) == TokenType.PAREN_CLOSE) {
                    ElementParser.throwAt(tokens, pos,
                            "trailing comma in @Relation columns");
                }
                columns.add(parseRelationColumn());
            }
        }
        if (pos >= tokens.count() || tokens.type(pos) != TokenType.PAREN_CLOSE) {
            ElementParser.throwAt(tokens, pos,
                    "expected ')' to close @Relation columns");
        }
        pos++; // consume ')'
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
        if (pos >= tokens.count()) {
            ElementParser.throwAt(tokens, pos,
                    "expected column name or '?' inside @Relation");
        }
        TokenType nameTok = tokens.type(pos);
        String name;
        if (nameTok == TokenType.QUESTION) {
            name = null;
            pos++;
        } else if (nameTok == TokenType.STRING) {
            String raw = tokens.text(pos);
            if (raw.length() < 2 || raw.charAt(0) != '\''
                    || raw.charAt(raw.length() - 1) != '\'') {
                ElementParser.throwAt(tokens, pos,
                        "malformed quoted column name inside @Relation");
            }
            name = unescapeString(raw.substring(1, raw.length() - 1));
            pos++;
        } else if (isFqnSegmentToken(nameTok)) {
            name = tokens.text(pos);
            pos++;
        } else {
            ElementParser.throwAt(tokens, pos,
                    "expected column name or '?' inside @Relation, got "
                    + nameTok);
            return null; // unreachable
        }
        if (pos >= tokens.count() || tokens.type(pos) != TokenType.COLON) {
            ElementParser.throwAt(tokens, pos,
                    "expected ':' after column name inside @Relation");
        }
        pos++; // consume ':'

        TypeAnnotation type;
        if (pos < tokens.count() && tokens.type(pos) == TokenType.QUESTION) {
            type = new TypeAnnotation.Wildcard();
            pos++;
        } else {
            // Inline type: qualifiedName + optional <typeArgs>.
            // Reuses parseTypeText (C.5) so the same generics
            // handling applies. The result is wrapped in a Named
            // TypeAnnotation for downstream uniformity.
            type = new TypeAnnotation.Named(parseTypeText());
        }

        Multiplicity multiplicity = null;
        if (pos < tokens.count() && tokens.type(pos) == TokenType.BRACKET_OPEN) {
            multiplicity = parseMultiplicity();
        }
        return new TypeAnnotation.RelationShape.Column(name, type, multiplicity);
    }

    // -------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------

    private static String safeText(TokenStream tokens, int pos) {
        return pos < tokens.count() ? tokens.text(pos) : "<EOF>";
    }
}
