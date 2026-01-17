package org.finos.legend.pure.dsl;

import org.finos.legend.pure.dsl.Token.TokenType;

import java.util.ArrayList;
import java.util.List;

/**
 * Recursive descent parser for the Pure language.
 * 
 * Parses Pure query expressions like:
 *   Person.all()->filter({p | $p.lastName == 'Smith'})->project([{p | $p.firstName}, {p | $p.lastName}])
 * 
 * Lambda syntax uses curly braces: {parameter | body}
 */
public final class PureParser {
    
    private final List<Token> tokens;
    private int position;
    
    public PureParser(List<Token> tokens) {
        this.tokens = tokens;
        this.position = 0;
    }
    
    /**
     * Parses a Pure query string.
     * 
     * @param query The Pure query string
     * @return The parsed expression AST
     */
    public static PureExpression parse(String query) {
        PureLexer lexer = new PureLexer(query);
        List<Token> tokens = lexer.tokenize();
        PureParser parser = new PureParser(tokens);
        return parser.parseExpression();
    }
    
    /**
     * Parses the top-level expression.
     */
    public PureExpression parseExpression() {
        return parseChainExpression();
    }
    
    /**
     * Parses a chain of arrow expressions: Class.all()->filter(...)->project(...)
     */
    private PureExpression parseChainExpression() {
        PureExpression expr = parsePrimaryExpression();
        
        while (check(TokenType.ARROW)) {
            consume(TokenType.ARROW, "Expected '->'");
            String functionName = consume(TokenType.IDENTIFIER, "Expected function name after '->'").value();
            consume(TokenType.LPAREN, "Expected '(' after function name");
            
            expr = switch (functionName) {
                case "filter" -> parseFilterCall(expr);
                case "project" -> parseProjectCall(expr);
                default -> throw new PureParseException("Unknown function: " + functionName);
            };
        }
        
        return expr;
    }
    
    /**
     * Parses primary expressions: Class.all(), variables, literals
     */
    private PureExpression parsePrimaryExpression() {
        if (check(TokenType.DOLLAR)) {
            return parseVariableAccess();
        }
        
        if (check(TokenType.STRING_LITERAL)) {
            Token token = advance();
            return LiteralExpr.string(token.value());
        }
        
        if (check(TokenType.INTEGER_LITERAL)) {
            Token token = advance();
            return LiteralExpr.integer(Long.parseLong(token.value()));
        }
        
        if (check(TokenType.FLOAT_LITERAL)) {
            Token token = advance();
            return LiteralExpr.floatValue(Double.parseDouble(token.value()));
        }
        
        if (check(TokenType.TRUE)) {
            advance();
            return LiteralExpr.bool(true);
        }
        
        if (check(TokenType.FALSE)) {
            advance();
            return LiteralExpr.bool(false);
        }
        
        if (check(TokenType.IDENTIFIER)) {
            return parseClassAllOrIdentifier();
        }
        
        if (check(TokenType.LPAREN)) {
            consume(TokenType.LPAREN, "Expected '('");
            PureExpression expr = parseOrExpression();
            consume(TokenType.RPAREN, "Expected ')'");
            return expr;
        }
        
        throw new PureParseException("Unexpected token: " + peek());
    }
    
    /**
     * Parses Class.all() expression or simple identifier
     */
    private PureExpression parseClassAllOrIdentifier() {
        Token className = consume(TokenType.IDENTIFIER, "Expected class name");
        
        if (check(TokenType.DOT)) {
            consume(TokenType.DOT, "Expected '.'");
            Token methodName = consume(TokenType.IDENTIFIER, "Expected 'all'");
            
            if (!"all".equals(methodName.value())) {
                throw new PureParseException("Expected 'all' but got: " + methodName.value());
            }
            
            consume(TokenType.LPAREN, "Expected '(' after 'all'");
            consume(TokenType.RPAREN, "Expected ')' after 'all('");
            
            return new ClassAllExpression(className.value());
        }
        
        // Just an identifier (might be in a lambda context)
        return new VariableExpr(className.value());
    }
    
    /**
     * Parses variable access: $p.propertyName
     */
    private PureExpression parseVariableAccess() {
        consume(TokenType.DOLLAR, "Expected '$'");
        Token varName = consume(TokenType.IDENTIFIER, "Expected variable name after '$'");
        
        PureExpression expr = new VariableExpr(varName.value());
        
        // Handle property access chain: $p.firstName or $p.address.city
        while (check(TokenType.DOT)) {
            consume(TokenType.DOT, "Expected '.'");
            Token propName = consume(TokenType.IDENTIFIER, "Expected property name after '.'");
            expr = new PropertyAccessExpression(expr, propName.value());
        }
        
        return expr;
    }
    
    /**
     * Parses filter function call: filter({p | $p.lastName == 'Smith'})
     */
    private PureExpression parseFilterCall(PureExpression source) {
        LambdaExpression lambda = parseLambda();
        consume(TokenType.RPAREN, "Expected ')' after filter lambda");
        return new FilterExpression(source, lambda);
    }
    
    /**
     * Parses project function call: project([{p | $p.firstName}, {p | $p.lastName}])
     * Or simpler: project({p | $p.firstName}, {p | $p.lastName})
     */
    private PureExpression parseProjectCall(PureExpression source) {
        List<LambdaExpression> projections = new ArrayList<>();
        List<String> aliases = new ArrayList<>();
        
        // Check for array syntax
        boolean arrayMode = check(TokenType.LBRACKET);
        if (arrayMode) {
            consume(TokenType.LBRACKET, "Expected '['");
        }
        
        // Parse first projection
        projections.add(parseLambda());
        
        // Parse additional projections
        while (check(TokenType.COMMA)) {
            consume(TokenType.COMMA, "Expected ','");
            
            // Check if this is an alias list
            if (check(TokenType.STRING_LITERAL)) {
                // Parse alias list
                aliases.add(consume(TokenType.STRING_LITERAL, "Expected alias").value());
                while (check(TokenType.COMMA)) {
                    consume(TokenType.COMMA, "Expected ','");
                    aliases.add(consume(TokenType.STRING_LITERAL, "Expected alias").value());
                }
                break;
            }
            
            projections.add(parseLambda());
        }
        
        if (arrayMode) {
            consume(TokenType.RBRACKET, "Expected ']'");
            
            // Check for optional aliases array
            if (check(TokenType.COMMA)) {
                consume(TokenType.COMMA, "Expected ','");
                consume(TokenType.LBRACKET, "Expected '[' for aliases");
                aliases.add(consume(TokenType.STRING_LITERAL, "Expected alias").value());
                while (check(TokenType.COMMA)) {
                    consume(TokenType.COMMA, "Expected ','");
                    aliases.add(consume(TokenType.STRING_LITERAL, "Expected alias").value());
                }
                consume(TokenType.RBRACKET, "Expected ']'");
            }
        }
        
        consume(TokenType.RPAREN, "Expected ')' after project");
        return new ProjectExpression(source, projections, aliases);
    }
    
    /**
     * Parses a lambda expression: {p | $p.lastName == 'Smith'}
     * Pure lambda syntax uses curly braces.
     */
    private LambdaExpression parseLambda() {
        consume(TokenType.LBRACE, "Expected '{' to start lambda");
        Token param = consume(TokenType.IDENTIFIER, "Expected lambda parameter");
        consume(TokenType.PIPE, "Expected '|' in lambda");
        PureExpression body = parseOrExpression();
        consume(TokenType.RBRACE, "Expected '}' to end lambda");
        return new LambdaExpression(param.value(), body);
    }
    
    /**
     * Parses OR expressions: expr || expr
     */
    private PureExpression parseOrExpression() {
        PureExpression left = parseAndExpression();
        
        while (check(TokenType.OR)) {
            consume(TokenType.OR, "Expected '||'");
            PureExpression right = parseAndExpression();
            left = LogicalExpr.or(left, right);
        }
        
        return left;
    }
    
    /**
     * Parses AND expressions: expr && expr
     */
    private PureExpression parseAndExpression() {
        PureExpression left = parseComparisonExpression();
        
        while (check(TokenType.AND)) {
            consume(TokenType.AND, "Expected '&&'");
            PureExpression right = parseComparisonExpression();
            left = LogicalExpr.and(left, right);
        }
        
        return left;
    }
    
    /**
     * Parses comparison expressions: $p.age > 25
     */
    private PureExpression parseComparisonExpression() {
        PureExpression left = parsePrimaryExpression();
        
        if (check(TokenType.EQUALS) || check(TokenType.NOT_EQUALS) ||
            check(TokenType.LESS_THAN) || check(TokenType.LESS_THAN_EQ) ||
            check(TokenType.GREATER_THAN) || check(TokenType.GREATER_THAN_EQ)) {
            
            Token opToken = advance();
            ComparisonExpr.Operator op = switch (opToken.type()) {
                case EQUALS -> ComparisonExpr.Operator.EQUALS;
                case NOT_EQUALS -> ComparisonExpr.Operator.NOT_EQUALS;
                case LESS_THAN -> ComparisonExpr.Operator.LESS_THAN;
                case LESS_THAN_EQ -> ComparisonExpr.Operator.LESS_THAN_OR_EQUALS;
                case GREATER_THAN -> ComparisonExpr.Operator.GREATER_THAN;
                case GREATER_THAN_EQ -> ComparisonExpr.Operator.GREATER_THAN_OR_EQUALS;
                default -> throw new PureParseException("Unknown operator: " + opToken);
            };
            
            PureExpression right = parsePrimaryExpression();
            return new ComparisonExpr(left, op, right);
        }
        
        return left;
    }
    
    // ==================== Helper Methods ====================
    
    private Token peek() {
        return tokens.get(position);
    }
    
    private boolean check(TokenType type) {
        return position < tokens.size() && peek().type() == type;
    }
    
    private Token advance() {
        if (position < tokens.size()) {
            return tokens.get(position++);
        }
        return tokens.getLast();
    }
    
    private Token consume(TokenType type, String message) {
        if (check(type)) {
            return advance();
        }
        throw new PureParseException(message + " at position " + peek().position() + ", got: " + peek());
    }
}
