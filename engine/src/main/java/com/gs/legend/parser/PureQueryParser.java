package com.gs.legend.parser;

import com.gs.legend.ast.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Hand-rolled recursive descent parser for Pure query expressions.
 *
 * Reads tokens from {@link PureLexer2} and builds {@link ValueSpecification}
 * AST nodes directly — no intermediate parse tree.
 *
 * Entry points:
 * - {@link #parseQuery(String)} — single query expression
 * - {@link #parseCodeBlock(String)} — multi-statement function body
 */
public final class PureQueryParser {

    private final PureLexer2 lexer;
    private final int tokenCount;
    private int pos;
    private final String source; // original source for extracting source text

    private PureQueryParser(String source) {
        this.source = source;
        this.lexer = new PureLexer2(source);
        this.lexer.tokenize();
        this.tokenCount = lexer.tokenCount();
        this.pos = 0;
    }

    // ==================== Public API ====================

    public static ValueSpecification parseQuery(String query) {
        PureQueryParser parser = new PureQueryParser(query);
        ValueSpecification result = parser.parseProgramLine();
        if (parser.pos < parser.tokenCount) {
            parser.error("Unexpected token after end of expression: " + parser.text());
        }
        return result;
    }

    public static List<ValueSpecification> parseCodeBlock(String body) {
        PureQueryParser parser = new PureQueryParser(body);
        List<ValueSpecification> stmts = new ArrayList<>();
        stmts.add(parser.parseProgramLine());
        while (parser.pos < parser.tokenCount && parser.peek() == TokenType.SEMI_COLON) {
            parser.advance(); // consume ;
            if (parser.pos < parser.tokenCount) {
                stmts.add(parser.parseProgramLine());
            }
        }
        if (stmts.isEmpty()) {
            throw new PureParseException("Empty function body");
        }
        return stmts;
    }

    // ==================== Token helpers ====================

    private TokenType peek() {
        return pos < tokenCount ? lexer.tokenType(pos) : null;
    }

    private TokenType peek(int offset) {
        int idx = pos + offset;
        return idx < tokenCount ? lexer.tokenType(idx) : null;
    }

    private String text() {
        return pos < tokenCount ? lexer.tokenText(pos) : "<EOF>";
    }

    private void advance() {
        pos++;
    }

    private String consume(TokenType expected) {
        if (pos >= tokenCount || lexer.tokenType(pos) != expected) {
            error("Expected " + expected + " but got " + (pos < tokenCount ? lexer.tokenType(pos) + " '" + text() + "'" : "EOF"));
        }
        String t = lexer.tokenText(pos);
        pos++;
        return t;
    }

    private boolean match(TokenType type) {
        if (pos < tokenCount && lexer.tokenType(pos) == type) {
            pos++;
            return true;
        }
        return false;
    }

    private boolean check(TokenType type) {
        return pos < tokenCount && lexer.tokenType(pos) == type;
    }

    private int tokenStart() {
        return pos < tokenCount ? lexer.tokenStart(pos) : source.length();
    }

    private void error(String msg) {
        throw new PureParseException(msg);
    }

    // ==================== Grammar: programLine ====================

    /**
     * programLine: combinedExpression | letExpression
     */
    private ValueSpecification parseProgramLine() {
        if (check(TokenType.LET)) {
            return parseLetExpression();
        }
        return parseCombinedExpression();
    }

    /**
     * letExpression: LET identifier EQUAL combinedExpression
     */
    private ValueSpecification parseLetExpression() {
        consume(TokenType.LET);
        String varName = parseIdentifierText();
        consume(TokenType.EQUAL);
        ValueSpecification value = parseCombinedExpression();
        return new AppliedFunction("letFunction", List.of(new CString(varName), value));
    }

    // ==================== Grammar: combinedExpression ====================

    /**
     * combinedExpression: expression expressionPart*
     */
    private ValueSpecification parseCombinedExpression() {
        ValueSpecification expr = parseExpression();
        // expressionPart: booleanPart | arithmeticPart
        while (pos < tokenCount) {
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
     * combinedArithmeticOnly: expression arithmeticPart*
     */
    private ValueSpecification parseCombinedArithmeticOnly() {
        ValueSpecification expr = parseExpression();
        while (pos < tokenCount && isArithmeticOp(peek())) {
            expr = parseArithmeticPart(expr);
        }
        return expr;
    }

    private boolean isArithmeticOp(TokenType t) {
        return t == TokenType.PLUS || t == TokenType.MINUS
                || t == TokenType.STAR || t == TokenType.DIVIDE
                || t == TokenType.LESS_THAN || t == TokenType.LESS_OR_EQUAL
                || t == TokenType.GREATER_THAN || t == TokenType.GREATER_OR_EQUAL;
    }

    private ValueSpecification parseBooleanPart(ValueSpecification left) {
        String op = peek() == TokenType.AND ? "and" : "or";
        advance(); // consume AND/OR
        ValueSpecification right = parseCombinedArithmeticOnly();
        return new AppliedFunction(op, List.of(left, right));
    }

    private ValueSpecification parseArithmeticPart(ValueSpecification left) {
        TokenType op = peek();
        String funcName = switch (op) {
            case PLUS -> "plus";
            case MINUS -> "minus";
            case STAR -> "times";
            case DIVIDE -> "divide";
            case LESS_THAN -> "lessThan";
            case LESS_OR_EQUAL -> "lessThanEqual";
            case GREATER_THAN -> "greaterThan";
            case GREATER_OR_EQUAL -> "greaterThanEqual";
            default -> "plus";
        };
        advance(); // consume operator
        ValueSpecification result = left;

        // For +, -, *, / the grammar allows chaining: PLUS expr (PLUS expr)*
        if (op == TokenType.PLUS || op == TokenType.MINUS
                || op == TokenType.STAR || op == TokenType.DIVIDE) {
            ValueSpecification right = parseExpression();
            result = new AppliedFunction(funcName, List.of(result, right));
            while (check(op)) {
                advance();
                right = parseExpression();
                result = new AppliedFunction(funcName, List.of(result, right));
            }
        } else {
            // Comparison operators: single right operand
            ValueSpecification right = parseExpression();
            result = new AppliedFunction(funcName, List.of(result, right));
        }
        return result;
    }

    // ==================== Grammar: expression ====================

    /**
     * expression: nonArrowOrEqualExpression (propertyOrFunctionExpression)* (equalNotEqual)?
     */
    private ValueSpecification parseExpression() {
        ValueSpecification expr = parseNonArrowOrEqualExpression();

        // propertyOrFunctionExpression: propertyExpression | functionExpression | propertyBracketExpression
        while (pos < tokenCount) {
            TokenType t = peek();
            if (t == TokenType.DOT) {
                expr = parsePropertyExpression(expr);
            } else if (t == TokenType.ARROW) {
                expr = parseFunctionExpression(expr);
            } else if (t == TokenType.BRACKET_OPEN) {
                expr = parsePropertyBracketExpression(expr);
            } else {
                break;
            }
        }

        // equalNotEqual: (TEST_EQUAL | TEST_NOT_EQUAL) combinedArithmeticOnly
        if (check(TokenType.TEST_EQUAL) || check(TokenType.TEST_NOT_EQUAL)) {
            String funcName = peek() == TokenType.TEST_EQUAL ? "equal" : "notEqual";
            advance();
            ValueSpecification right = parseCombinedArithmeticOnly();
            return new AppliedFunction(funcName, List.of(expr, right));
        }

        return expr;
    }

    // ==================== Grammar: nonArrowOrEqualExpression ====================

    private ValueSpecification parseNonArrowOrEqualExpression() {
        TokenType t = peek();

        if (t == TokenType.NOT) {
            advance();
            ValueSpecification inner = parseExpression();
            return new AppliedFunction("not", List.of(inner));
        }

        if (t == TokenType.MINUS || t == TokenType.PLUS) {
            // signedExpression: (MINUS | PLUS) expression
            boolean isMinus = t == TokenType.MINUS;
            advance();
            // If next token is a literal number, fold the sign into the literal
            if (check(TokenType.INTEGER) || check(TokenType.FLOAT) || check(TokenType.DECIMAL)) {
                // Don't fold — the grammar produces AppliedFunction("minus"/"plus", [literal])
                // Let parseExpression handle it
            }
            ValueSpecification inner = parseExpression();
            return new AppliedFunction(isMinus ? "minus" : "plus", List.of(inner));
        }

        if (t == TokenType.BRACKET_OPEN) {
            return parseExpressionsArray();
        }

        if (t == TokenType.PAREN_OPEN) {
            advance(); // consume (
            ValueSpecification inner = parseCombinedExpression();
            consume(TokenType.PAREN_CLOSE);
            return inner;
        }

        return parseAtomicExpression();
    }

    // ==================== Grammar: expressionsArray ====================

    private ValueSpecification parseExpressionsArray() {
        consume(TokenType.BRACKET_OPEN);
        List<ValueSpecification> elements = new ArrayList<>();
        if (!check(TokenType.BRACKET_CLOSE)) {
            elements.add(parseExpression());
            while (match(TokenType.COMMA)) {
                elements.add(parseExpression());
            }
        }
        consume(TokenType.BRACKET_CLOSE);
        return new PureCollection(elements);
    }

    // ==================== Grammar: atomicExpression ====================

    private ValueSpecification parseAtomicExpression() {
        TokenType t = peek();

        // DSL: #{...}# or #>{...}#
        if (t == TokenType.ISLAND_OPEN) {
            return parseDsl();
        }

        // TDS literal
        if (t == TokenType.TDS_LITERAL) {
            String raw = text();
            advance();
            return new AppliedFunction("tds", List.of(new CString("TDS"), new CString(raw)));
        }

        // Literals
        if (t == TokenType.STRING) {
            String val = unquote(text());
            advance();
            return new CString(val);
        }
        if (t == TokenType.INTEGER) {
            String val = text();
            advance();
            try {
                return new CInteger(Long.parseLong(val));
            } catch (NumberFormatException e) {
                return new CInteger(new BigInteger(val));
            }
        }
        if (t == TokenType.FLOAT) {
            String val = text();
            advance();
            double d = Double.parseDouble(val);
            BigDecimal exact = new BigDecimal(val);
            if (exact.compareTo(BigDecimal.valueOf(d)) != 0) {
                return new CDecimal(exact);
            }
            return new CFloat(d);
        }
        if (t == TokenType.DECIMAL) {
            String val = text();
            advance();
            val = val.substring(0, val.length() - 1); // remove 'd' suffix
            return new CDecimal(new BigDecimal(val));
        }
        if (t == TokenType.BOOLEAN || t == TokenType.TRUE || t == TokenType.FALSE) {
            boolean val = (t == TokenType.TRUE) || (t == TokenType.BOOLEAN && Boolean.parseBoolean(text()));
            advance();
            return new CBoolean(val);
        }
        if (t == TokenType.DATE) {
            String val = text();
            advance();
            val = val.startsWith("%") ? val.substring(1) : val;
            if (val.contains("T")) {
                return new CDateTime(val);
            }
            return new CStrictDate(val);
        }
        if (t == TokenType.STRICTTIME) {
            String val = text();
            advance();
            val = val.startsWith("%") ? val.substring(1) : val;
            return new CStrictTime(val);
        }
        if (t == TokenType.LATEST_DATE) {
            advance();
            return new CLatestDate();
        }

        // Variable: $identifier
        if (t == TokenType.DOLLAR) {
            advance();
            String varName = parseIdentifierText();
            return new Variable(varName);
        }

        // Lambda: {x, y | body} or {| body}
        if (t == TokenType.BRACE_OPEN) {
            return parseLambdaFunction();
        }

        // Lambda pipe: |expr (zero-param lambda)
        if (t == TokenType.PIPE) {
            return parseLambdaPipe();
        }

        // Column builders: ~name or ~[name, age]
        if (t == TokenType.TILDE) {
            return parseColumnBuilders();
        }

        // Expression instance: ^ClassName(...)
        if (t == TokenType.NEW_SYMBOL) {
            return parseExpressionInstance();
        }

        // Type annotation: @Type
        if (t == TokenType.AT) {
            return parseTypeAnnotation();
        }

        // Comparator: comparator(a: Type[1], b: Type[1]): Boolean[1] { body }
        if (t == TokenType.COMPARATOR) {
            return parseComparatorExpression();
        }

        // Lambda param with pipe: identifier|body (single-param shorthand)
        // e.g., x|$x.name
        if (isIdentifierToken(t) && peek(1) == TokenType.PIPE) {
            return parseSingleParamLambda();
        }

        // Lambda param with colon-type then pipe: x:Type[1]|body
        if (isIdentifierToken(t) && peek(1) == TokenType.COLON) {
            // Could be a typed lambda param: x:Type[1]|body
            // Or it could be instanceReference.  Disambiguate by checking if we have
            // the full pattern: identifier COLON type multiplicity PIPE
            if (looksLikeTypedLambdaParam()) {
                return parseTypedSingleParamLambda();
            }
        }

        // Instance reference: qualifiedName (.all() | .allVersions() | (args) | ε)
        if (isIdentifierToken(t) || t == TokenType.PATH_SEPARATOR) {
            return parseInstanceReference();
        }

        error("Unexpected token in expression: " + t + " '" + text() + "'");
        return null; // unreachable
    }

    // ==================== Property/Function expressions ====================

    /**
     * propertyExpression: DOT identifier (functionExpressionParameters)?
     */
    private ValueSpecification parsePropertyExpression(ValueSpecification source) {
        consume(TokenType.DOT);
        String propName = parseIdentifierText();

        // Method call with args: source.method(args)
        if (check(TokenType.PAREN_OPEN)) {
            List<ValueSpecification> args = parseFunctionExpressionParameters();
            var params = new ArrayList<ValueSpecification>();
            params.add(source);
            params.addAll(args);
            return new AppliedFunction(propName, params, true);
        }

        // Enum value: EnumType.VALUE
        if (source instanceof PackageableElementPtr ptr) {
            return new EnumValue(ptr.fullPath(), propName);
        }

        // Property access: $x.name
        return new AppliedProperty(propName, List.of(source));
    }

    /**
     * functionExpression: ARROW qualifiedName functionExpressionParameters
     *                     (ARROW qualifiedName functionExpressionParameters)*
     */
    private ValueSpecification parseFunctionExpression(ValueSpecification source) {
        ValueSpecification result = source;

        while (check(TokenType.ARROW)) {
            int sourceTextEnd = tokenStart(); // position just before the arrow
            advance(); // consume ->

            String funcName = parseQualifiedName();
            int argsStartPos = pos;
            List<ValueSpecification> args = parseFunctionExpressionParameters();

            // Capture source text (everything before this arrow)
            String sourceText = sourceTextEnd > 0 ? extractSourceText(0, sourceTextEnd) : null;
            List<String> argTexts = extractArgTexts(argsStartPos);

            var allParams = new ArrayList<ValueSpecification>();
            allParams.add(result);
            allParams.addAll(args);
            result = new AppliedFunction(funcName, allParams, true, sourceText, argTexts);
        }

        return result;
    }

    /**
     * propertyBracketExpression: BRACKET_OPEN (STRING | INTEGER) BRACKET_CLOSE
     */
    private ValueSpecification parsePropertyBracketExpression(ValueSpecification source) {
        consume(TokenType.BRACKET_OPEN);
        TokenType t = peek();
        ValueSpecification result;
        if (t == TokenType.INTEGER) {
            int index = Integer.parseInt(text());
            advance();
            result = new AppliedFunction("at", List.of(source, new CInteger(index)), true);
        } else if (t == TokenType.STRING) {
            String key = unquote(text());
            advance();
            result = new AppliedProperty(key, List.of(source));
        } else {
            error("Expected STRING or INTEGER in bracket expression, got " + t);
            return null;
        }
        consume(TokenType.BRACKET_CLOSE);
        return result;
    }

    // ==================== Function parameters ====================

    /**
     * functionExpressionParameters: PAREN_OPEN (combinedExpression (COMMA combinedExpression)*)? PAREN_CLOSE
     */
    private List<ValueSpecification> parseFunctionExpressionParameters() {
        consume(TokenType.PAREN_OPEN);
        List<ValueSpecification> args = new ArrayList<>();
        if (!check(TokenType.PAREN_CLOSE)) {
            args.add(parseCombinedExpression());
            while (match(TokenType.COMMA)) {
                args.add(parseCombinedExpression());
            }
        }
        consume(TokenType.PAREN_CLOSE);
        return args;
    }

    // ==================== Instance reference ====================

    /**
     * instanceReference: (PATH_SEPARATOR | qualifiedName | unitName) allOrFunction?
     */
    private ValueSpecification parseInstanceReference() {
        String name;
        if (check(TokenType.PATH_SEPARATOR)) {
            name = "::";
            advance();
        } else {
            name = parseQualifiedName();
            // Check for unitName: qualifiedName TILDE identifier
            if (check(TokenType.TILDE)) {
                advance();
                String unitPart = parseIdentifierText();
                name = name + "~" + unitPart;
            }
        }

        // allOrFunction
        if (check(TokenType.DOT)) {
            // .all() or .allVersions() or .allVersionsInRange(...)
            int savedPos = pos;
            advance(); // consume DOT
            TokenType t = peek();

            if (t == TokenType.ALL) {
                advance();
                consume(TokenType.PAREN_OPEN);
                // Check for milestoning args
                if (!check(TokenType.PAREN_CLOSE)) {
                    // allFunctionWithMilestoning: .all(milestoningExpr, ...)
                    List<ValueSpecification> args = new ArrayList<>();
                    args.add(new PackageableElementPtr(name));
                    args.add(parseMilestoningExpression());
                    if (match(TokenType.COMMA)) {
                        args.add(parseMilestoningExpression());
                    }
                    consume(TokenType.PAREN_CLOSE);
                    return new AppliedFunction("getAll", args);
                }
                consume(TokenType.PAREN_CLOSE);
                return new AppliedFunction("getAll", List.of(new PackageableElementPtr(name)));
            }
            if (t == TokenType.ALL_VERSIONS) {
                advance();
                consume(TokenType.PAREN_OPEN);
                consume(TokenType.PAREN_CLOSE);
                return new AppliedFunction("getAllVersions", List.of(new PackageableElementPtr(name)));
            }
            if (t == TokenType.ALL_VERSIONS_IN_RANGE) {
                advance();
                consume(TokenType.PAREN_OPEN);
                ValueSpecification start = parseMilestoningExpression();
                consume(TokenType.COMMA);
                ValueSpecification end = parseMilestoningExpression();
                consume(TokenType.PAREN_CLOSE);
                return new AppliedFunction("getAllVersionsInRange",
                        List.of(new PackageableElementPtr(name), start, end));
            }

            // Not .all()/.allVersions() — backtrack, it's a property expression
            pos = savedPos;
        }

        // functionExpressionParameters (standalone function call)
        if (check(TokenType.PAREN_OPEN)) {
            int argsStartPos = pos;
            List<ValueSpecification> args = parseFunctionExpressionParameters();
            List<String> argTexts = extractArgTexts(argsStartPos);
            return new AppliedFunction(name, args, false, null, argTexts);
        }

        // Just a class/package reference
        return new PackageableElementPtr(name);
    }

    private ValueSpecification parseMilestoningExpression() {
        if (check(TokenType.LATEST_DATE)) {
            advance();
            return new CLatestDate();
        }
        if (check(TokenType.DATE)) {
            String val = text();
            advance();
            val = val.startsWith("%") ? val.substring(1) : val;
            if (val.contains("T")) return new CDateTime(val);
            return new CStrictDate(val);
        }
        if (check(TokenType.DOLLAR)) {
            advance();
            String varName = parseIdentifierText();
            return new Variable(varName);
        }
        error("Expected milestoning expression (date, %latest, or $variable)");
        return null;
    }

    // ==================== Lambda ====================

    /**
     * lambdaFunction: BRACE_OPEN (lambdaParam (COMMA lambdaParam)*)? lambdaPipe BRACE_CLOSE
     */
    private ValueSpecification parseLambdaFunction() {
        consume(TokenType.BRACE_OPEN);
        List<Variable> params = new ArrayList<>();

        // Check if there are parameters before the pipe
        if (!check(TokenType.PIPE)) {
            // Could be parameters or could be empty lambda {}
            if (check(TokenType.BRACE_CLOSE)) {
                // Empty braces — treat as empty lambda
                consume(TokenType.BRACE_CLOSE);
                return new LambdaFunction(List.of(new Variable("_")), List.of(new CString("")));
            }

            // Parse params: identifier (COLON type multiplicity)? (COMMA ...)*
            // But we need to distinguish {expr} from {x | expr} and {x, y | expr}
            // Lookahead: if we see identifier followed by PIPE or COMMA, it's params
            if (looksLikeLambdaParams()) {
                params.add(parseLambdaParam());
                while (match(TokenType.COMMA)) {
                    params.add(parseLambdaParam());
                }
            }

            if (params.isEmpty()) {
                params.add(new Variable("_"));
            }
        }

        // lambdaPipe: PIPE codeBlock
        consume(TokenType.PIPE);
        List<ValueSpecification> body = parseCodeBlockStatements();
        consume(TokenType.BRACE_CLOSE);
        return new LambdaFunction(params, body);
    }

    /**
     * |codeBlock (zero-param lambda)
     */
    private ValueSpecification parseLambdaPipe() {
        consume(TokenType.PIPE);
        List<ValueSpecification> body = parseCodeBlockStatements();
        return new LambdaFunction(List.of(), body);
    }

    /**
     * Single-param shorthand: identifier|body
     */
    private ValueSpecification parseSingleParamLambda() {
        String paramName = parseIdentifierText();
        consume(TokenType.PIPE);
        List<ValueSpecification> body = parseCodeBlockStatements();
        return new LambdaFunction(List.of(new Variable(paramName)), body);
    }

    /**
     * Typed single-param: identifier:Type[mult]|body
     */
    private ValueSpecification parseTypedSingleParamLambda() {
        Variable param = parseLambdaParam();
        consume(TokenType.PIPE);
        List<ValueSpecification> body = parseCodeBlockStatements();
        return new LambdaFunction(List.of(param), body);
    }

    private Variable parseLambdaParam() {
        String name = parseIdentifierText();
        if (check(TokenType.COLON)) {
            advance();
            String typeName = parseTypeText();
            return new Variable(name, typeName, parseMultiplicity());
        }
        return new Variable(name);
    }

    private List<ValueSpecification> parseCodeBlockStatements() {
        List<ValueSpecification> stmts = new ArrayList<>();
        stmts.add(parseProgramLine());
        while (match(TokenType.SEMI_COLON)) {
            if (pos < tokenCount && !check(TokenType.BRACE_CLOSE)) {
                stmts.add(parseProgramLine());
            }
        }
        return stmts;
    }

    // ==================== Column builders ====================

    /**
     * columnBuilders: TILDE (oneColSpec | colSpecArray)
     */
    private ValueSpecification parseColumnBuilders() {
        consume(TokenType.TILDE);

        if (check(TokenType.BRACKET_OPEN)) {
            return parseColSpecArray();
        }
        return parseOneColSpec();
    }

    /**
     * oneColSpec: identifier ((COLON (type multiplicity? | anyLambda) extraFunction? ))?
     */
    private ValueSpecification parseOneColSpec() {
        String colName = parseIdentifierText();

        LambdaFunction fn1 = null;
        LambdaFunction fn2 = null;

        if (check(TokenType.COLON)) {
            advance(); // consume :
            TokenType t = peek();

            // Check if it's a lambda or a type annotation
            if (t == TokenType.PIPE || t == TokenType.BRACE_OPEN) {
                // anyLambda (|body or {x|body})
                fn1 = (LambdaFunction) parseAnyLambda();
            } else if (isIdentifierToken(t)) {
                // Could be: x|body (simple lambda), x:Type[1]|body (typed lambda),
                // or Type multiplicity? (type annotation)
                if (peek(1) == TokenType.PIPE) {
                    // simple: x|body
                    fn1 = (LambdaFunction) parseAnyLambda();
                } else if (peek(1) == TokenType.COLON && looksLikeTypedLambdaParam()) {
                    // typed: x:Type[1]|body
                    fn1 = (LambdaFunction) parseAnyLambda();
                } else {
                    // type annotation: Type multiplicity?
                    parseTypeText();
                    if (check(TokenType.BRACKET_OPEN)) {
                        parseMultiplicityText();
                    }
                }
            } else {
                // type annotation starting with ( for relationType or { for functionType
                parseTypeText();
                if (check(TokenType.BRACKET_OPEN)) {
                    parseMultiplicityText();
                }
            }

            // extraFunction: COLON anyLambda
            if (check(TokenType.COLON)) {
                advance();
                fn2 = (LambdaFunction) parseAnyLambda();
            }
        }

        ColSpec spec = new ColSpec(colName, fn1, fn2);
        return new ClassInstance("colSpec", spec);
    }

    private ValueSpecification parseColSpecArray() {
        consume(TokenType.BRACKET_OPEN);
        List<ColSpec> specs = new ArrayList<>();
        if (!check(TokenType.BRACKET_CLOSE)) {
            ClassInstance ci = (ClassInstance) parseOneColSpec();
            specs.add((ColSpec) ci.value());
            while (match(TokenType.COMMA)) {
                ci = (ClassInstance) parseOneColSpec();
                specs.add((ColSpec) ci.value());
            }
        }
        consume(TokenType.BRACKET_CLOSE);
        return new ClassInstance("colSpecArray", new ColSpecArray(specs));
    }

    private ValueSpecification parseAnyLambda() {
        TokenType t = peek();
        if (t == TokenType.PIPE) {
            return parseLambdaPipe();
        }
        if (t == TokenType.BRACE_OPEN) {
            return parseLambdaFunction();
        }
        // identifier|body or identifier:Type[mult]|body
        if (isIdentifierToken(t)) {
            if (peek(1) == TokenType.PIPE) {
                return parseSingleParamLambda();
            }
            if (peek(1) == TokenType.COLON) {
                // typed param: x:Type[mult]|body — verify with lookahead
                if (looksLikeTypedLambdaParam()) {
                    return parseTypedSingleParamLambda();
                }
            }
        }
        error("Expected lambda expression, got " + t + " '" + text() + "'");
        return null;
    }

    // ==================== DSL (Island grammars) ====================

    private ValueSpecification parseDsl() {
        String islandOpen = text();
        consume(TokenType.ISLAND_OPEN);

        String dslType = islandOpen.substring(1, islandOpen.length() - 1);

        // Collect island content
        StringBuilder content = new StringBuilder();
        while (pos < tokenCount) {
            TokenType t = peek();
            if (t == TokenType.ISLAND_END) {
                advance();
                break;
            }
            if (t == TokenType.ISLAND_ARROW_EXIT) {
                advance();
                break;
            }
            if (t == TokenType.ISLAND_BRACE_OPEN) {
                content.append("{");
                advance();
            } else if (t == TokenType.ISLAND_BRACE_CLOSE) {
                content.append("}");
                advance();
            } else {
                content.append(text());
                advance();
            }
        }

        String contentText = content.toString().trim();

        ValueSpecification result = switch (dslType) {
            case "" -> parseGraphFetchTree(contentText);
            case ">" -> {
                int lastDot = contentText.lastIndexOf('.');
                if (lastDot < 0) throw new PureParseException("Table reference must be db.TABLE: " + contentText);
                String db = contentText.substring(0, lastDot);
                String tableName = contentText.substring(lastDot + 1);
                yield new AppliedFunction("tableReference", List.of(new CString(db), new CString(tableName)));
            }
            default -> throw new PureParseException("Unknown DSL type: #" + dslType + "{");
        };

        // Handle arrow chains after DSL (functionChainAfterArrow)
        // The ISLAND_ARROW_EXIT already consumed the -> so we go straight to funcName(args)
        if (pos < tokenCount && isIdentifierToken(peek())) {
            result = parseFunctionChainAfterArrow(result);
        }

        return result;
    }

    private ValueSpecification parseFunctionChainAfterArrow(ValueSpecification source) {
        ValueSpecification current = source;

        // First call (arrow already consumed by ISLAND_ARROW_EXIT)
        String funcName = parseQualifiedName();
        List<ValueSpecification> args = parseFunctionExpressionParameters();
        var allParams = new ArrayList<ValueSpecification>();
        allParams.add(current);
        allParams.addAll(args);
        current = new AppliedFunction(funcName, allParams, true, null, List.of());

        // Subsequent arrow calls
        while (check(TokenType.ARROW)) {
            advance();
            funcName = parseQualifiedName();
            args = parseFunctionExpressionParameters();
            allParams = new ArrayList<ValueSpecification>();
            allParams.add(current);
            allParams.addAll(args);
            current = new AppliedFunction(funcName, allParams, true, null, List.of());
        }

        return current;
    }

    // ==================== GraphFetch tree ====================

    /**
     * Parse graphFetch content: ClassName { prop1, prop2 { sub } }
     * Desugar into ColSpec-based AST.
     */
    private ValueSpecification parseGraphFetchTree(String content) {
        // Re-lex the graphFetch content with a new parser instance
        PureQueryParser inner = new PureQueryParser(content);

        // Skip the root class name (graphFetch gets it from arg[0])
        inner.parseQualifiedName();

        // Parse graphDefinition: { graphPaths }
        ColSpecArray colSpecArray = inner.parseGraphDefinition(0);
        return new ClassInstance("colSpecArray", colSpecArray);
    }

    private ColSpecArray parseGraphDefinition(int depth) {
        consume(TokenType.BRACE_OPEN);
        List<ColSpec> specs = new ArrayList<>();
        if (!check(TokenType.BRACE_CLOSE)) {
            specs.add(parseGraphPath(depth));
            while (match(TokenType.COMMA)) {
                if (!check(TokenType.BRACE_CLOSE)) {
                    specs.add(parseGraphPath(depth));
                }
            }
        }
        consume(TokenType.BRACE_CLOSE);
        return new ColSpecArray(specs);
    }

    private ColSpec parseGraphPath(int depth) {
        // subTypeGraphPath: ->subtype() graphDefinition
        // graphPath: graphAlias? identifier propertyParameters? subtype? graphDefinition?

        // Skip graphAlias: STRING COLON
        if (check(TokenType.STRING) && peek(1) == TokenType.COLON) {
            advance(); // skip alias string
            advance(); // skip colon
        }

        String propName = parseIdentifierText();
        String paramName = "_gf" + depth;
        Variable param = new Variable(paramName);
        AppliedProperty propAccess = new AppliedProperty(propName, List.of(param));
        LambdaFunction fn1 = new LambdaFunction(List.of(param), List.of(propAccess));

        // propertyParameters: PAREN_OPEN (graphFetchParameter (COMMA graphFetchParameter)*)? PAREN_CLOSE
        if (check(TokenType.PAREN_OPEN)) {
            advance();
            // Skip parameters for now — milestoning params
            while (!check(TokenType.PAREN_CLOSE) && pos < tokenCount) {
                advance();
            }
            consume(TokenType.PAREN_CLOSE);
        }

        // subtype: ->subtype(ClassName)
        // (rare — skip for now)

        // Nested graphDefinition?
        if (check(TokenType.BRACE_OPEN)) {
            ColSpecArray nestedSpecs = parseGraphDefinition(depth + 1);
            ClassInstance nestedCI = new ClassInstance("colSpecArray", nestedSpecs);
            LambdaFunction fn2 = new LambdaFunction(List.of(), List.of(nestedCI));
            return new ColSpec(propName, fn1, fn2);
        }

        return new ColSpec(propName, fn1);
    }

    // ==================== Expression instance ====================

    /**
     * expressionInstance: NEW_SYMBOL qualifiedName typeArguments? PAREN_OPEN assignments PAREN_CLOSE
     */
    private ValueSpecification parseExpressionInstance() {
        consume(TokenType.NEW_SYMBOL);

        // Can be ^$variable or ^ClassName
        String className;
        if (check(TokenType.DOLLAR)) {
            // ^$variable — rare, just capture name
            advance();
            className = "$" + parseIdentifierText();
        } else {
            className = parseQualifiedName();
        }

        // Optional type arguments: <Type, Type>
        List<String> typeArgs = List.of();
        if (check(TokenType.LESS_THAN)) {
            typeArgs = parseTypeArguments();
        }

        // Optional identifier (named instance)
        // Skip if present

        // Property assignments
        consume(TokenType.PAREN_OPEN);
        Map<String, ValueSpecification> properties = new LinkedHashMap<>();
        if (!check(TokenType.PAREN_CLOSE)) {
            parsePropertyAssignment(properties);
            while (match(TokenType.COMMA)) {
                parsePropertyAssignment(properties);
            }
        }
        consume(TokenType.PAREN_CLOSE);

        return new AppliedFunction("new",
                List.of(new PackageableElementPtr(className),
                        new ClassInstance("instance",
                                new com.gs.legend.ast.InstanceData(
                                        className, properties, typeArgs))));
    }

    private void parsePropertyAssignment(Map<String, ValueSpecification> properties) {
        // identifier (DOT identifier)* PLUS? EQUAL expressionInstanceRightSide
        StringBuilder propName = new StringBuilder(parseIdentifierText());
        while (check(TokenType.DOT)) {
            advance();
            propName.append(".").append(parseIdentifierText());
        }
        if (check(TokenType.PLUS)) advance(); // optional PLUS
        consume(TokenType.EQUAL);
        ValueSpecification value = parseCombinedExpression();
        properties.put(propName.toString(), value);
    }

    private List<String> parseTypeArguments() {
        consume(TokenType.LESS_THAN);
        List<String> args = new ArrayList<>();
        // Simple: collect text until >
        StringBuilder current = new StringBuilder();
        int depth = 1;
        while (pos < tokenCount && depth > 0) {
            TokenType t = peek();
            if (t == TokenType.LESS_THAN) depth++;
            if (t == TokenType.GREATER_THAN) {
                depth--;
                if (depth == 0) break;
            }
            if (t == TokenType.COMMA && depth == 1) {
                args.add(current.toString().trim());
                current = new StringBuilder();
                advance();
                continue;
            }
            current.append(text());
            advance();
        }
        if (current.length() > 0) args.add(current.toString().trim());
        consume(TokenType.GREATER_THAN);
        return args;
    }

    // ==================== Type annotation ====================

    private ValueSpecification parseTypeAnnotation() {
        consume(TokenType.AT);
        String typeName = parseQualifiedName();

        // Check for Relation<(col:Type, ...)>
        String simpleName = typeName.contains("::") ? typeName.substring(typeName.lastIndexOf("::") + 2) : typeName;
        if ("Relation".equals(simpleName) && check(TokenType.LESS_THAN)) {
            consume(TokenType.LESS_THAN);
            // Expect relationType: (col:Type, ...)
            if (check(TokenType.PAREN_OPEN)) {
                com.gs.legend.plan.GenericType resolved = parseRelationType();
                consume(TokenType.GREATER_THAN);
                return new GenericTypeInstance("Relation", resolved);
            }
            // Fall through for other type argument patterns
            // Collect text until >
            StringBuilder content = new StringBuilder();
            int depth = 1;
            while (pos < tokenCount && depth > 0) {
                if (peek() == TokenType.LESS_THAN) depth++;
                if (peek() == TokenType.GREATER_THAN) {
                    depth--;
                    if (depth == 0) break;
                }
                content.append(text());
                advance();
            }
            consume(TokenType.GREATER_THAN);
        }

        return new GenericTypeInstance(typeName,
                com.gs.legend.plan.GenericType.fromTypeName(typeName));
    }

    private com.gs.legend.plan.GenericType parseRelationType() {
        consume(TokenType.PAREN_OPEN);
        var columns = new LinkedHashMap<String, com.gs.legend.plan.GenericType>();
        if (!check(TokenType.PAREN_CLOSE)) {
            parseColumnInfo(columns);
            while (match(TokenType.COMMA)) {
                parseColumnInfo(columns);
            }
        }
        consume(TokenType.PAREN_CLOSE);
        return new com.gs.legend.plan.GenericType.Relation(
                com.gs.legend.plan.GenericType.Relation.Schema.withoutPivot(columns));
    }

    private void parseColumnInfo(Map<String, com.gs.legend.plan.GenericType> columns) {
        // mayColumnName: QUESTION | identifier
        String colName;
        if (check(TokenType.QUESTION)) {
            colName = "?";
            advance();
        } else {
            colName = parseIdentifierText();
        }
        consume(TokenType.COLON);
        // mayColumnType: QUESTION | type
        com.gs.legend.plan.GenericType colType;
        if (check(TokenType.QUESTION)) {
            colType = com.gs.legend.plan.GenericType.Primitive.ANY;
            advance();
        } else {
            String typeName = parseTypeText();
            colType = com.gs.legend.plan.GenericType.fromTypeName(typeName);
        }
        // optional multiplicity
        if (check(TokenType.BRACKET_OPEN)) {
            parseMultiplicityText(); // consume and discard
        }
        columns.put(colName, colType);
    }

    // ==================== Comparator ====================

    private ValueSpecification parseComparatorExpression() {
        consume(TokenType.COMPARATOR);
        consume(TokenType.PAREN_OPEN);
        List<Variable> params = new ArrayList<>();
        params.add(parseFunctionVariableExpression());
        while (match(TokenType.COMMA)) {
            params.add(parseFunctionVariableExpression());
        }
        consume(TokenType.PAREN_CLOSE);
        consume(TokenType.COLON);
        parseTypeText(); // return type (ignored — grammar requires it)
        parseMultiplicityText(); // return multiplicity
        consume(TokenType.BRACE_OPEN);
        List<ValueSpecification> body = parseCodeBlockStatements();
        consume(TokenType.BRACE_CLOSE);
        return new LambdaFunction(params, body);
    }

    private Variable parseFunctionVariableExpression() {
        String name = parseIdentifierText();
        consume(TokenType.COLON);
        String typeName = parseTypeText();
        return new Variable(name, typeName, parseMultiplicity());
    }

    // ==================== Helpers ====================

    private String parseIdentifierText() {
        if (pos >= tokenCount) error("Expected identifier, got EOF");
        TokenType t = peek();
        if (!isIdentifierToken(t)) {
            error("Expected identifier, got " + t + " '" + text() + "'");
        }
        String result = text();
        advance();
        // Strip surrounding single quotes from quoted identifiers
        if (result.length() >= 2 && result.startsWith("'") && result.endsWith("'")) {
            result = result.substring(1, result.length() - 1);
        }
        return result;
    }

    private String parseQualifiedName() {
        StringBuilder sb = new StringBuilder(parseIdentifierText());
        while (check(TokenType.PATH_SEPARATOR)) {
            sb.append("::");
            advance();
            sb.append(parseIdentifierText());
        }
        return sb.toString();
    }

    private String parseTypeText() {
        // type can be: qualifiedName optionally with <typeArgs>
        // or relationType: (col:Type, ...)
        // or functionType: {Type[m] -> Type[m]}
        // or unitName: qualifiedName~identifier
        TokenType t = peek();
        // relationType: (col:Type, ...)
        if (t == TokenType.PAREN_OPEN) {
            int start = pos;
            advance();
            int depth = 1;
            while (pos < tokenCount && depth > 0) {
                if (peek() == TokenType.PAREN_OPEN) depth++;
                if (peek() == TokenType.PAREN_CLOSE) depth--;
                advance();
            }
            return source.substring(lexer.tokenStart(start), lexer.tokenEnd(pos - 1));
        }
        // functionType: {Type[m] -> Type[m]}
        if (t == TokenType.BRACE_OPEN) {
            int start = pos;
            advance();
            int depth = 1;
            while (pos < tokenCount && depth > 0) {
                if (peek() == TokenType.BRACE_OPEN) depth++;
                if (peek() == TokenType.BRACE_CLOSE) depth--;
                advance();
            }
            return source.substring(lexer.tokenStart(start), lexer.tokenEnd(pos - 1));
        }
        String name = parseQualifiedName();
        // Optional type args: <...>
        if (check(TokenType.LESS_THAN)) {
            StringBuilder sb = new StringBuilder(name);
            sb.append('<');
            advance();
            int depth = 1;
            while (pos < tokenCount && depth > 0) {
                if (peek() == TokenType.LESS_THAN) depth++;
                if (peek() == TokenType.GREATER_THAN) {
                    depth--;
                    if (depth == 0) { advance(); break; }
                }
                sb.append(text());
                advance();
            }
            sb.append('>');
            name = sb.toString();
        }
        // unitName: qualifiedName~identifier
        if (check(TokenType.TILDE)) {
            advance();
            name = name + "~" + parseIdentifierText();
        }
        return name;
    }

    private String parseMultiplicityText() {
        consume(TokenType.BRACKET_OPEN);
        StringBuilder sb = new StringBuilder();
        while (!check(TokenType.BRACKET_CLOSE) && pos < tokenCount) {
            sb.append(text());
            advance();
        }
        consume(TokenType.BRACKET_CLOSE);
        return sb.toString();
    }

    /**
     * Parses a multiplicity at the token level: {@code [1]}, {@code [*]}, {@code [0..1]},
     * {@code [1..*]}, {@code [m]} (variable).
     *
     * <p>Returns a structured {@link com.gs.legend.model.m3.Multiplicity} directly — no
     * text round-trip through the model layer. The m3 layer is a pure data model and
     * does not know how to parse.
     */
    private com.gs.legend.model.m3.Multiplicity parseMultiplicity() {
        consume(TokenType.BRACKET_OPEN);
        com.gs.legend.model.m3.Multiplicity result;
        if (match(TokenType.STAR)) {
            result = com.gs.legend.model.m3.Multiplicity.MANY;
        } else if (check(TokenType.INTEGER)) {
            int lower = Integer.parseInt(consume(TokenType.INTEGER));
            if (match(TokenType.DOT_DOT)) {
                Integer upper = match(TokenType.STAR)
                        ? null
                        : Integer.parseInt(consume(TokenType.INTEGER));
                result = new com.gs.legend.model.m3.Multiplicity.Bounded(lower, upper);
            } else {
                result = new com.gs.legend.model.m3.Multiplicity.Bounded(lower, lower);
            }
        } else {
            // Multiplicity variable: identifier like m, n
            String name = parseIdentifierText();
            result = new com.gs.legend.model.m3.Multiplicity.Var(name);
        }
        consume(TokenType.BRACKET_CLOSE);
        return result;
    }

    private boolean isIdentifierToken(TokenType t) {
        if (t == null) return false;
        return t == TokenType.VALID_STRING || t == TokenType.STRING
                || t == TokenType.ALL || t == TokenType.LET
                || t == TokenType.ALL_VERSIONS || t == TokenType.ALL_VERSIONS_IN_RANGE
                || t == TokenType.COMPARATOR || t == TokenType.TO_BYTES_FUNCTION
                || t == TokenType.IMPORT || t == TokenType.CLASS
                || t == TokenType.FUNCTION || t == TokenType.PROFILE
                || t == TokenType.ASSOCIATION || t == TokenType.ENUM
                || t == TokenType.MEASURE || t == TokenType.EXTENDS
                || t == TokenType.STEREOTYPES || t == TokenType.TAGS
                || t == TokenType.NATIVE || t == TokenType.PROJECTS
                || t == TokenType.AS
                || t == TokenType.CONSTRAINT_ENFORCEMENT_LEVEL_ERROR
                || t == TokenType.CONSTRAINT_ENFORCEMENT_LEVEL_WARN
                || t == TokenType.AGGREGATION_TYPE_COMPOSITE
                || t == TokenType.AGGREGATION_TYPE_SHARED
                || t == TokenType.AGGREGATION_TYPE_NONE
                || t == TokenType.MAPPING || t == TokenType.INCLUDE
                || t == TokenType.TESTS
                || t == TokenType.MAPPING_TESTABLE_DOC
                || t == TokenType.MAPPING_TESTABLE_DATA
                || t == TokenType.MAPPING_TESTABLE_ASSERT
                || t == TokenType.MAPPING_TESTABLE_SUITES
                || t == TokenType.MAPPING_TEST_ASSERTS
                || t == TokenType.MAPPING_TESTS
                || t == TokenType.MAPPING_TESTS_QUERY
                || t == TokenType.RUNTIME || t == TokenType.SINGLE_CONNECTION_RUNTIME
                || t == TokenType.MAPPINGS || t == TokenType.CONNECTIONS
                || t == TokenType.CONNECTION || t == TokenType.CONNECTIONSTORES
                || t == TokenType.DATABASE || t == TokenType.TABLE
                || t == TokenType.SCHEMA || t == TokenType.VIEW
                || t == TokenType.TABULAR_FUNC || t == TokenType.FILTER
                || t == TokenType.MULTIGRAIN_FILTER || t == TokenType.JOIN
                || t == TokenType.RELATIONAL_AND || t == TokenType.RELATIONAL_OR
                || t == TokenType.MILESTONING || t == TokenType.BUSINESS_MILESTONING
                || t == TokenType.BUSINESS_MILESTONING_FROM
                || t == TokenType.BUSINESS_MILESTONING_THRU
                || t == TokenType.THRU_IS_INCLUSIVE || t == TokenType.BUS_SNAPSHOT_DATE
                || t == TokenType.PROCESSING_MILESTONING
                || t == TokenType.PROCESSING_MILESTONING_IN
                || t == TokenType.PROCESSING_MILESTONING_OUT
                || t == TokenType.OUT_IS_INCLUSIVE || t == TokenType.INFINITY_DATE
                || t == TokenType.PROCESSING_SNAPSHOT_DATE
                || t == TokenType.ASSOCIATION_MAPPING || t == TokenType.ENUMERATION_MAPPING
                || t == TokenType.OTHERWISE || t == TokenType.INLINE
                || t == TokenType.BINDING || t == TokenType.SCOPE
                || t == TokenType.PURE_MAPPING
                || t == TokenType.STORE || t == TokenType.TYPE
                || t == TokenType.MODE
                || t == TokenType.RELATIONAL_DATASOURCE_SPEC
                || t == TokenType.RELATIONAL_AUTH_STRATEGY
                || t == TokenType.DB_TIMEZONE || t == TokenType.QUOTE_IDENTIFIERS
                || t == TokenType.QUERY_GENERATION_CONFIGS
                || t == TokenType.DUCKDB || t == TokenType.SQLITE
                || t == TokenType.POSTGRES || t == TokenType.H2
                || t == TokenType.SNOWFLAKE || t == TokenType.LOCALDUCKDB
                || t == TokenType.INMEMORY || t == TokenType.NOAUTH
                || t == TokenType.SERVICE || t == TokenType.SERVICE_PATTERN
                || t == TokenType.SERVICE_OWNERS
                || t == TokenType.SERVICE_DOCUMENTATION
                || t == TokenType.SERVICE_AUTO_ACTIVATE_UPDATES
                || t == TokenType.SERVICE_EXEC || t == TokenType.SERVICE_SINGLE
                || t == TokenType.SERVICE_MULTI || t == TokenType.SERVICE_MAPPING
                || t == TokenType.SERVICE_RUNTIME
                || t == TokenType.NOT
                || t == TokenType.BOOLEAN
                || t == TokenType.TRUE
                || t == TokenType.FALSE;
    }

    /**
     * Disambiguate lambda params from expressions in { ... | ... } context.
     * Returns true if the tokens look like lambda parameters (identifier sequences
     * followed by PIPE).
     */
    private boolean looksLikeLambdaParams() {
        int saved = pos;
        try {
            // Try to skip lambda params
            while (pos < tokenCount) {
                TokenType t = peek();
                if (t == TokenType.PIPE) return true;
                if (!isIdentifierToken(t)) return false;
                advance(); // identifier
                if (check(TokenType.COLON)) {
                    advance(); // :
                    // Skip type and multiplicity
                    if (!skipType()) return false;
                    if (check(TokenType.BRACKET_OPEN)) skipMultiplicity();
                }
                if (check(TokenType.COMMA)) {
                    advance();
                    continue;
                }
                if (check(TokenType.PIPE)) return true;
                return false;
            }
            return false;
        } finally {
            pos = saved;
        }
    }

    private boolean looksLikeTypedLambdaParam() {
        int saved = pos;
        try {
            if (!isIdentifierToken(peek())) return false;
            advance(); // identifier
            if (!check(TokenType.COLON)) return false;
            advance(); // :
            if (!skipType()) return false;
            if (check(TokenType.BRACKET_OPEN)) skipMultiplicity();
            return check(TokenType.PIPE);
        } finally {
            pos = saved;
        }
    }

    private boolean skipType() {
        TokenType t = peek();
        // relationType: (col:Type, ...)
        if (t == TokenType.PAREN_OPEN) {
            advance();
            int depth = 1;
            while (pos < tokenCount && depth > 0) {
                if (peek() == TokenType.PAREN_OPEN) depth++;
                if (peek() == TokenType.PAREN_CLOSE) depth--;
                advance();
            }
            return true;
        }
        // functionType: {Type[m] -> Type[m]}
        if (t == TokenType.BRACE_OPEN) {
            advance();
            int depth = 1;
            while (pos < tokenCount && depth > 0) {
                if (peek() == TokenType.BRACE_OPEN) depth++;
                if (peek() == TokenType.BRACE_CLOSE) depth--;
                advance();
            }
            return true;
        }
        if (!isIdentifierToken(t)) return false;
        advance();
        while (check(TokenType.PATH_SEPARATOR)) {
            advance();
            if (!isIdentifierToken(peek())) return false;
            advance();
        }
        // Optional type args
        if (check(TokenType.LESS_THAN)) {
            advance();
            int depth = 1;
            while (pos < tokenCount && depth > 0) {
                if (peek() == TokenType.LESS_THAN) depth++;
                if (peek() == TokenType.GREATER_THAN) depth--;
                advance();
            }
        }
        // unitName: qualifiedName~identifier
        if (check(TokenType.TILDE)) {
            advance();
            if (isIdentifierToken(peek())) advance();
        }
        return true;
    }

    private void skipMultiplicity() {
        if (check(TokenType.BRACKET_OPEN)) {
            advance();
            while (pos < tokenCount && !check(TokenType.BRACKET_CLOSE)) {
                advance();
            }
            if (check(TokenType.BRACKET_CLOSE)) advance();
        }
    }

    // ==================== Source text extraction ====================

    private String extractSourceText(int startCharIdx, int endCharIdx) {
        if (source == null || startCharIdx < 0 || endCharIdx <= startCharIdx
                || endCharIdx > source.length()) {
            return null;
        }
        return source.substring(startCharIdx, endCharIdx).trim();
    }

    private List<String> extractArgTexts(int argsStartPos) {
        // For now, return empty — source text capture can be refined later
        return List.of();
    }

    // ==================== String utilities ====================

    private String unquote(String s) {
        if (s == null || s.length() < 2) return s;
        char first = s.charAt(0);
        char last = s.charAt(s.length() - 1);
        if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
            return unescapeString(s.substring(1, s.length() - 1));
        }
        return s;
    }

    private String unescapeString(String s) {
        if (s == null || !s.contains("\\")) return s;
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char next = s.charAt(i + 1);
                switch (next) {
                    case 'n' -> { sb.append('\n'); i++; }
                    case 'r' -> { sb.append('\r'); i++; }
                    case 't' -> { sb.append('\t'); i++; }
                    case '\\' -> { sb.append('\\'); i++; }
                    case '\'' -> { sb.append('\''); i++; }
                    case '"' -> { sb.append('"'); i++; }
                    default -> sb.append(c);
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
