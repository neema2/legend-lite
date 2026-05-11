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
import com.legend.parser.spec.PackageableElementPtr;
import com.legend.parser.spec.PureCollection;
import com.legend.parser.spec.ValueSpecification;
import com.legend.parser.spec.Variable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Parser for Pure value specifications (expressions). Produces a sealed
 * {@link ValueSpecification} AST.
 *
 * <h2>Phase status &mdash; C.2 (postfix expressions)</h2>
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
 * <p>Anything else &mdash; operators, lambdas, {@code let}, code
 * blocks, milestoning, {@code new} instances, type annotations,
 * column instances ({@code ~col}) &mdash; raises a
 * {@link ParseException} that names the unsupported construct by
 * token type. Per the AGENTS.md no-fallbacks rule, the parser never
 * silently accepts an unknown construct as something else; the error
 * lists the source line/column so callers can repair the input
 * directly. Subsequent phases (C.3 &ndash; C.5) progressively extend
 * the grammar to cover the full engine value-spec.
 *
 * <h2>Entry points</h2>
 * <ul>
 *   <li>{@link #parse(String)} &mdash; lex the source and parse a single
 *       expression. Convenience entry for tests and one-off callers.</li>
 *   <li>{@link #parse(TokenStream)} &mdash; parse over a pre-lexed
 *       stream, which may be a {@link TokenStream#slice(int, int) slice}
 *       of a larger file. Source offsets in the slice are preserved, so
 *       error reporting points back to the original source.</li>
 * </ul>
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
        ValueSpecification result = parser.parseExpression();
        if (parser.pos < tokens.count()) {
            ElementParser.throwAt(tokens, parser.pos,
                    "trailing tokens after expression: " + tokens.type(parser.pos)
                    + " ('" + safeText(tokens, parser.pos) + "')");
        }
        return result;
    }

    // -------------------------------------------------------------------
    // Top-level expression dispatch
    // -------------------------------------------------------------------

    /**
     * Parse a single expression starting at the current cursor. Splits
     * into {@link #parsePrimary()} (literals, variables, parens,
     * collections, qualified-name-or-prefix-call) and a postfix loop
     * over {@code .} (property / method on receiver) and {@code ->}
     * (arrow function call). Later phases will introduce Pratt-style
     * precedence levels for binary operators on top of this skeleton.
     */
    private ValueSpecification parseExpression() {
        ValueSpecification expr = parsePrimary();
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
        return expr;
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
            default -> {
                if (ElementParser.IDENTIFIER_TOKENS.contains(t)) {
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
        values.add(parseExpression());
        while (pos < tokens.count() && tokens.type(pos) == TokenType.COMMA) {
            pos++; // consume ','
            if (pos < tokens.count() && tokens.type(pos) == TokenType.BRACKET_CLOSE) {
                ElementParser.throwAt(tokens, pos,
                        "trailing comma in collection literal");
            }
            values.add(parseExpression());
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
        ValueSpecification inner = parseExpression();
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
        args.add(parseExpression());
        while (pos < tokens.count() && tokens.type(pos) == TokenType.COMMA) {
            pos++; // consume ','
            if (pos < tokens.count() && tokens.type(pos) == TokenType.PAREN_CLOSE) {
                ElementParser.throwAt(tokens, pos,
                        "trailing comma in argument list");
            }
            args.add(parseExpression());
        }
        if (pos >= tokens.count() || tokens.type(pos) != TokenType.PAREN_CLOSE) {
            ElementParser.throwAt(tokens, pos,
                    "expected ')' to close argument list");
        }
        pos++; // consume ')'
        return args;
    }

    // -------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------

    private static String safeText(TokenStream tokens, int pos) {
        return pos < tokens.count() ? tokens.text(pos) : "<EOF>";
    }
}
