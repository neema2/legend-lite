package org.finos.legend.pure.dsl.m2m;

import org.finos.legend.pure.dsl.PureParseException;

import java.util.ArrayList;
import java.util.List;

/**
 * Parser for M2M mapping expressions.
 * 
 * Supports:
 * - $src.property references
 * - $src.prop1.prop2 chained property access
 * - $src.prop->function() chained function calls
 * - String concatenation with +
 * - Literals: 'string', 42, 3.14, true/false
 * - if(condition, |then, |else) conditionals
 * - Comparison operators: ==, !=, <, <=, >, >=
 * 
 * Example expressions:
 * 
 * <pre>
 * $src.firstName
 * $src.firstName + ' ' + $src.lastName
 * $src.firstName->toUpper()
 * if($src.age < 18, |'Minor', |'Adult')
 * </pre>
 */
public final class M2MExpressionParser {

    private final String input;
    private int pos = 0;

    private M2MExpressionParser(String input) {
        this.input = input;
    }

    /**
     * Parses an M2M expression string.
     * 
     * @param expression The expression to parse
     * @return The parsed M2M expression
     */
    public static M2MExpression parse(String expression) {
        return new M2MExpressionParser(expression.trim()).parseExpression();
    }

    private M2MExpression parseExpression() {
        return parseOr();
    }

    // Parse || (lowest precedence for boolean)
    private M2MExpression parseOr() {
        M2MExpression left = parseAnd();

        while (match("||")) {
            M2MExpression right = parseAnd();
            // Logical expressions would be handled here if needed
            // For now, we focus on comparisons in if() conditions
            left = new M2MComparisonExpr(left, M2MComparisonExpr.Operator.EQUALS, right); // placeholder
        }

        return left;
    }

    // Parse &&
    private M2MExpression parseAnd() {
        M2MExpression left = parseComparison();

        while (match("&&")) {
            parseComparison();
        }

        return left;
    }

    // Parse comparisons: ==, !=, <, <=, >, >=
    private M2MExpression parseComparison() {
        M2MExpression left = parseAdditive();

        M2MComparisonExpr.Operator op = null;
        if (match("==")) {
            op = M2MComparisonExpr.Operator.EQUALS;
        } else if (match("!=")) {
            op = M2MComparisonExpr.Operator.NOT_EQUALS;
        } else if (match("<=")) {
            op = M2MComparisonExpr.Operator.LESS_THAN_OR_EQUALS;
        } else if (match(">=")) {
            op = M2MComparisonExpr.Operator.GREATER_THAN_OR_EQUALS;
        } else if (match("<")) {
            op = M2MComparisonExpr.Operator.LESS_THAN;
        } else if (match(">")) {
            op = M2MComparisonExpr.Operator.GREATER_THAN;
        }

        if (op != null) {
            M2MExpression right = parseAdditive();
            return new M2MComparisonExpr(left, op, right);
        }

        return left;
    }

    // Parse + and - (string concatenation and arithmetic)
    private M2MExpression parseAdditive() {
        M2MExpression left = parseMultiplicative();

        List<M2MExpression> concatParts = null;

        while (peek() == '+' || (peek() == '-' && !isArrow())) {
            char op = advance();
            M2MExpression right = parseMultiplicative();

            if (op == '+') {
                // Could be string concat or arithmetic
                // We'll treat + as string concat by default, accumulating parts
                if (concatParts == null) {
                    concatParts = new ArrayList<>();
                    concatParts.add(left);
                }
                concatParts.add(right);
            } else {
                // Arithmetic subtraction
                left = BinaryArithmeticExpr.subtract(left, right);
            }
        }

        if (concatParts != null) {
            return new StringConcatExpr(concatParts);
        }

        return left;
    }

    // Parse * and /
    private M2MExpression parseMultiplicative() {
        M2MExpression left = parseUnary();

        while (peek() == '*' || peek() == '/') {
            char op = advance();
            M2MExpression right = parseUnary();

            if (op == '*') {
                left = BinaryArithmeticExpr.multiply(left, right);
            } else {
                left = BinaryArithmeticExpr.divide(left, right);
            }
        }

        return left;
    }

    // Parse unary operators (like !)
    private M2MExpression parseUnary() {
        // Could handle ! (not) here
        return parsePostfix();
    }

    // Parse postfix operators: ->function()
    private M2MExpression parsePostfix() {
        M2MExpression expr = parsePrimary();

        // Handle chained function calls: ->func1()->func2()
        while (matchArrow()) {
            String funcName = parseIdentifier();
            expect('(');
            List<M2MExpression> args = new ArrayList<>();

            if (peek() != ')') {
                args.add(parseExpression());
                while (match(",")) {
                    args.add(parseExpression());
                }
            }
            expect(')');

            expr = new FunctionCallExpr(expr, funcName, args);
        }

        return expr;
    }

    // Parse primary expressions
    private M2MExpression parsePrimary() {
        skipWhitespace();

        // if(condition, |then, |else)
        if (matchKeyword("if")) {
            return parseIf();
        }

        // @JoinName - association reference for deep fetch
        if (peek() == '@') {
            advance(); // consume @
            String joinName = parseIdentifier();
            return new AssociationRef(joinName);
        }

        // $src.property
        if (peek() == '$') {
            return parseSourceRef();
        }

        // String literal 'xxx'
        if (peek() == '\'') {
            return parseStringLiteral();
        }

        // Number literal
        if (Character.isDigit(peek()) || (peek() == '-' && Character.isDigit(peekNext()))) {
            return parseNumberLiteral();
        }

        // Boolean literals
        if (matchKeyword("true")) {
            return M2MLiteral.bool(true);
        }
        if (matchKeyword("false")) {
            return M2MLiteral.bool(false);
        }

        // Parenthesized expression
        if (match("(")) {
            M2MExpression expr = parseExpression();
            expect(')');
            return expr;
        }

        // Pipe for lambda in if() - just parse the value after it
        if (peek() == '|') {
            advance(); // consume |
            return parseExpression();
        }

        throw new PureParseException("Unexpected character at position " + pos + ": '" + peek() + "' in: " + input);
    }

    // Parse if(condition, |then, |else)
    private M2MExpression parseIf() {
        expect('(');
        M2MExpression condition = parseExpression();
        expect(',');
        skipWhitespace();

        // Expect | before then branch
        if (peek() == '|') {
            advance();
        }
        M2MExpression thenBranch = parseExpression();
        expect(',');
        skipWhitespace();

        // Expect | before else branch
        if (peek() == '|') {
            advance();
        }
        M2MExpression elseBranch = parseExpression();
        expect(')');

        return new IfExpr(condition, thenBranch, elseBranch);
    }

    // Parse $src.prop or $src.prop1.prop2
    private M2MExpression parseSourceRef() {
        expect('$');
        String varName = parseIdentifier();

        if (!varName.equals("src")) {
            throw new PureParseException("Expected $src, got $" + varName);
        }

        List<String> properties = new ArrayList<>();

        while (peek() == '.' && !isArrow()) {
            advance(); // consume .
            String propName = parseIdentifier();
            properties.add(propName);
        }

        if (properties.isEmpty()) {
            throw new PureParseException("Expected property access after $src");
        }

        return new SourcePropertyRef(properties);
    }

    // Parse 'string literal'
    private M2MExpression parseStringLiteral() {
        expect('\'');
        StringBuilder sb = new StringBuilder();

        while (pos < input.length() && input.charAt(pos) != '\'') {
            if (input.charAt(pos) == '\\' && pos + 1 < input.length()) {
                pos++;
                char escaped = input.charAt(pos);
                switch (escaped) {
                    case 'n' -> sb.append('\n');
                    case 't' -> sb.append('\t');
                    case '\'' -> sb.append('\'');
                    case '\\' -> sb.append('\\');
                    default -> sb.append(escaped);
                }
            } else {
                sb.append(input.charAt(pos));
            }
            pos++;
        }

        expect('\'');
        return M2MLiteral.string(sb.toString());
    }

    // Parse number literal (integer or float)
    private M2MExpression parseNumberLiteral() {
        StringBuilder sb = new StringBuilder();

        if (peek() == '-') {
            sb.append(advance());
        }

        while (Character.isDigit(peek())) {
            sb.append(advance());
        }

        if (peek() == '.') {
            sb.append(advance());
            while (Character.isDigit(peek())) {
                sb.append(advance());
            }
            return M2MLiteral.floatVal(Double.parseDouble(sb.toString()));
        }

        return M2MLiteral.integer(Long.parseLong(sb.toString()));
    }

    // Parse an identifier (alphanumeric + underscore)
    private String parseIdentifier() {
        skipWhitespace();
        StringBuilder sb = new StringBuilder();

        if (!Character.isLetter(peek()) && peek() != '_') {
            throw new PureParseException("Expected identifier at position " + pos);
        }

        while (Character.isLetterOrDigit(peek()) || peek() == '_') {
            sb.append(advance());
        }

        return sb.toString();
    }

    // Helper methods

    private char peek() {
        skipWhitespace();
        return pos < input.length() ? input.charAt(pos) : '\0';
    }

    private char peekNext() {
        int nextPos = pos + 1;
        while (nextPos < input.length() && Character.isWhitespace(input.charAt(nextPos))) {
            nextPos++;
        }
        return nextPos < input.length() ? input.charAt(nextPos) : '\0';
    }

    private char advance() {
        skipWhitespace();
        return pos < input.length() ? input.charAt(pos++) : '\0';
    }

    private void expect(char c) {
        skipWhitespace();
        if (pos >= input.length() || input.charAt(pos) != c) {
            throw new PureParseException("Expected '" + c + "' at position " + pos + " in: " + input);
        }
        pos++;
    }

    private boolean match(String s) {
        skipWhitespace();
        if (input.substring(pos).startsWith(s)) {
            pos += s.length();
            return true;
        }
        return false;
    }

    private boolean matchKeyword(String keyword) {
        skipWhitespace();
        if (input.substring(pos).startsWith(keyword)) {
            int endPos = pos + keyword.length();
            // Make sure it's not part of a longer identifier
            if (endPos >= input.length() || !Character.isLetterOrDigit(input.charAt(endPos))) {
                pos = endPos;
                return true;
            }
        }
        return false;
    }

    private boolean matchArrow() {
        skipWhitespace();
        if (pos + 1 < input.length() && input.charAt(pos) == '-' && input.charAt(pos + 1) == '>') {
            pos += 2;
            return true;
        }
        return false;
    }

    private boolean isArrow() {
        skipWhitespace();
        return pos + 1 < input.length() && input.charAt(pos) == '-' && input.charAt(pos + 1) == '>';
    }

    private void skipWhitespace() {
        while (pos < input.length() && Character.isWhitespace(input.charAt(pos))) {
            pos++;
        }
    }
}
