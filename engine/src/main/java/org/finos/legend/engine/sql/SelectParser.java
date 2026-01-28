package org.finos.legend.engine.sql;

import org.finos.legend.engine.sql.ast.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.finos.legend.engine.sql.Token.*;

/**
 * Recursive descent parser for SELECT statements.
 * 
 * Combines patterns from:
 * - JOOQ: check()/consumeIf() for clean lookahead
 * - Alibaba Druid: SavePoint for backtracking
 * 
 * Features:
 * - PostgreSQL dialect (quoted identifiers, :: cast)
 * - Legend-Engine table functions (service, table, class)
 */
public final class SelectParser {

    private final Lexer lexer;

    public SelectParser(String sql) {
        this.lexer = new Lexer(sql);
    }

    public SelectParser(Lexer lexer) {
        this.lexer = lexer;
    }

    // ==================== Token Helpers (JOOQ-style) ====================

    private Token current() {
        return lexer.token();
    }

    private String stringVal() {
        return lexer.stringVal();
    }

    private boolean check(Token t) {
        return current() == t;
    }

    private void advance() {
        lexer.nextToken();
    }

    private boolean consumeIf(Token t) {
        if (check(t)) {
            advance();
            return true;
        }
        return false;
    }

    private void expect(Token t) {
        if (!check(t)) {
            throw error("Expected " + t + ", got " + current());
        }
        advance();
    }

    private String expectIdentifier() {
        if (check(IDENTIFIER) || check(QUOTED_IDENTIFIER)) {
            String val = stringVal();
            advance();
            return val;
        }
        // Allow some keywords as identifiers
        if (current().isKeyword()) {
            String val = stringVal();
            advance();
            return val;
        }
        throw error("Expected identifier");
    }

    private int expectInteger() {
        if (!check(INTEGER)) {
            throw error("Expected integer");
        }
        int val = Integer.parseInt(stringVal());
        advance();
        return val;
    }

    private SQLParseException error(String message) {
        return new SQLParseException(message + " at " + lexer.info(), lexer.tokenPos());
    }

    // ==================== List Parsing ====================

    private <T> List<T> parseList(Supplier<T> parser) {
        List<T> items = new ArrayList<>();
        do {
            items.add(parser.get());
        } while (consumeIf(COMMA));
        return items;
    }

    // ==================== SELECT Statement ====================

    public SelectStatement parseSelect() {
        expect(SELECT);

        boolean distinct = consumeIf(DISTINCT);
        if (!distinct)
            consumeIf(ALL);

        List<SelectItem> items = parseSelectList();

        expect(FROM);
        List<FromItem> fromItems = parseList(this::parseFromItem);

        Expression where = null;
        if (consumeIf(WHERE)) {
            where = parseExpression();
        }

        List<Expression> groupBy = List.of();
        if (consumeIf(GROUP)) {
            expect(BY);
            groupBy = parseList(this::parseExpression);
        }

        Expression having = null;
        if (consumeIf(HAVING)) {
            having = parseExpression();
        }

        List<OrderSpec> orderBy = List.of();
        if (consumeIf(ORDER)) {
            expect(BY);
            orderBy = parseList(this::parseOrderSpec);
        }

        Integer limit = null;
        if (consumeIf(LIMIT)) {
            limit = expectInteger();
        }

        Integer offset = null;
        if (consumeIf(OFFSET)) {
            offset = expectInteger();
        }

        return new SelectStatement(distinct, items, fromItems, where, groupBy, having, orderBy, limit, offset);
    }

    // ==================== SELECT List ====================

    private List<SelectItem> parseSelectList() {
        if (consumeIf(STAR)) {
            List<SelectItem> items = new ArrayList<>();
            items.add(SelectItem.AllColumns.unqualified());
            while (consumeIf(COMMA)) {
                items.add(parseSelectItem());
            }
            return items;
        }
        return parseList(this::parseSelectItem);
    }

    private SelectItem parseSelectItem() {
        // Check for table.*
        Lexer.SavePoint mark = lexer.mark();
        if (check(IDENTIFIER)) {
            String name = stringVal();
            advance();
            if (consumeIf(DOT) && consumeIf(STAR)) {
                return SelectItem.AllColumns.qualified(name);
            }
            lexer.reset(mark);
        }

        Expression expr = parseExpression();
        String alias = parseOptionalAlias();
        return new SelectItem.ExpressionItem(expr, alias);
    }

    private String parseOptionalAlias() {
        if (consumeIf(AS)) {
            return expectIdentifier();
        }
        // Allow identifiers and non-reserved keywords as aliases
        if (check(IDENTIFIER) || check(QUOTED_IDENTIFIER)) {
            return expectIdentifier();
        }
        // Also allow non-reserved keywords as aliases (e.g., "first", "last", "name")
        if (current().isKeyword() && !isReserved(current())) {
            String val = stringVal();
            advance();
            return val;
        }
        return null;
    }

    private boolean isReserved(Token t) {
        return t == FROM || t == WHERE || t == GROUP || t == HAVING ||
                t == ORDER || t == LIMIT || t == OFFSET ||
                t == JOIN || t == LEFT || t == RIGHT || t == INNER ||
                t == FULL || t == CROSS || t == ON ||
                t == UNION || t == INTERSECT || t == EXCEPT ||
                t == AND || t == OR || t == AS || t == SELECT;
    }

    // ==================== FROM Clause ====================

    private FromItem parseFromItem() {
        FromItem item = parseFromPrimary();

        // Handle JOINs
        while (isJoinKeyword()) {
            FromItem.JoinedTable.JoinType joinType = parseJoinType();
            FromItem right = parseFromPrimary();
            Expression condition = null;
            if (consumeIf(ON)) {
                condition = parseExpression();
            }
            item = new FromItem.JoinedTable(item, joinType, right, condition);
        }

        return item;
    }

    private FromItem parseFromPrimary() {
        // Subquery: (SELECT ...)
        if (check(LPAREN)) {
            Lexer.SavePoint mark = lexer.mark();
            advance();
            if (check(SELECT)) {
                SelectStatement subquery = parseSelect();
                expect(RPAREN);
                String alias = parseOptionalFromAlias();
                if (alias == null)
                    throw error("Subquery must have an alias");
                return new FromItem.SubQuery(subquery, alias);
            }
            lexer.reset(mark);
        }

        // Legend-Engine table functions: service(...), table(...), class(...)
        if (isTableFunction()) {
            return parseTableFunction();
        }

        // Regular table reference
        return parseTableRef();
    }

    private boolean isTableFunction() {
        Lexer.SavePoint mark = lexer.mark();
        if (check(SERVICE) || check(TABLE) || check(CLASS)) {
            advance();
            boolean hasParen = check(LPAREN);
            lexer.reset(mark);
            return hasParen;
        }
        return false;
    }

    private FromItem.TableFunction parseTableFunction() {
        String funcName = stringVal();
        advance();
        expect(LPAREN);

        List<Expression> args = new ArrayList<>();
        if (!check(RPAREN)) {
            args = parseList(this::parseExpression);
        }
        expect(RPAREN);

        String alias = parseOptionalFromAlias();
        return new FromItem.TableFunction(funcName, args, alias);
    }

    private FromItem.TableRef parseTableRef() {
        String first = expectIdentifier();
        String schema = null;
        String table = first;

        if (consumeIf(DOT)) {
            schema = first;
            table = expectIdentifier();
        }

        String alias = parseOptionalFromAlias();
        return new FromItem.TableRef(schema, table, alias);
    }

    private String parseOptionalFromAlias() {
        if (consumeIf(AS)) {
            return expectIdentifier();
        }
        if ((check(IDENTIFIER) || check(QUOTED_IDENTIFIER)) && !isReserved(current())) {
            return expectIdentifier();
        }
        return null;
    }

    private boolean isJoinKeyword() {
        return check(JOIN) || check(INNER) || check(LEFT) ||
                check(RIGHT) || check(FULL) || check(CROSS);
    }

    private FromItem.JoinedTable.JoinType parseJoinType() {
        if (consumeIf(INNER)) {
            expect(JOIN);
            return FromItem.JoinedTable.JoinType.INNER;
        }
        if (consumeIf(LEFT)) {
            consumeIf(OUTER);
            expect(JOIN);
            return FromItem.JoinedTable.JoinType.LEFT_OUTER;
        }
        if (consumeIf(RIGHT)) {
            consumeIf(OUTER);
            expect(JOIN);
            return FromItem.JoinedTable.JoinType.RIGHT_OUTER;
        }
        if (consumeIf(FULL)) {
            consumeIf(OUTER);
            expect(JOIN);
            return FromItem.JoinedTable.JoinType.FULL_OUTER;
        }
        if (consumeIf(CROSS)) {
            expect(JOIN);
            return FromItem.JoinedTable.JoinType.CROSS;
        }
        if (consumeIf(JOIN)) {
            return FromItem.JoinedTable.JoinType.INNER;
        }
        throw error("Expected JOIN keyword");
    }

    // ==================== ORDER BY ====================

    private OrderSpec parseOrderSpec() {
        Expression expr = parseExpression();

        OrderSpec.Direction dir = OrderSpec.Direction.ASC;
        if (consumeIf(ASC))
            dir = OrderSpec.Direction.ASC;
        else if (consumeIf(DESC))
            dir = OrderSpec.Direction.DESC;

        OrderSpec.NullsOrder nulls = OrderSpec.NullsOrder.DEFAULT;
        if (consumeIf(NULLS)) {
            if (consumeIf(FIRST))
                nulls = OrderSpec.NullsOrder.NULLS_FIRST;
            else if (consumeIf(LAST))
                nulls = OrderSpec.NullsOrder.NULLS_LAST;
            else
                throw error("Expected FIRST or LAST after NULLS");
        }

        return new OrderSpec(expr, dir, nulls);
    }

    // ==================== Expressions ====================

    private Expression parseExpression() {
        return parseOrExpr();
    }

    private Expression parseOrExpr() {
        Expression left = parseAndExpr();
        while (consumeIf(OR)) {
            Expression right = parseAndExpr();
            left = new Expression.BinaryOp(left, Expression.BinaryOperator.OR, right);
        }
        return left;
    }

    private Expression parseAndExpr() {
        Expression left = parseNotExpr();
        while (consumeIf(AND)) {
            Expression right = parseNotExpr();
            left = new Expression.BinaryOp(left, Expression.BinaryOperator.AND, right);
        }
        return left;
    }

    private Expression parseNotExpr() {
        if (consumeIf(NOT)) {
            return new Expression.UnaryOp(Expression.UnaryOperator.NOT, parseNotExpr());
        }
        return parseComparisonExpr();
    }

    private Expression parseComparisonExpr() {
        Expression left = parseAddExpr();

        // IS NULL / IS NOT NULL
        if (consumeIf(IS)) {
            boolean negated = consumeIf(NOT);
            expect(NULL);
            return new Expression.IsNullExpr(left, negated);
        }

        // BETWEEN
        if (check(BETWEEN) || (check(NOT) && peekIs(BETWEEN))) {
            boolean negated = consumeIf(NOT);
            expect(BETWEEN);
            Expression low = parseAddExpr();
            expect(AND);
            Expression high = parseAddExpr();
            return new Expression.BetweenExpr(left, low, high, negated);
        }

        // IN
        if (check(IN) || (check(NOT) && peekIs(IN))) {
            boolean negated = consumeIf(NOT);
            expect(IN);
            expect(LPAREN);
            if (check(SELECT)) {
                SelectStatement subquery = parseSelect();
                expect(RPAREN);
                return new Expression.InExpr(left, null, subquery, negated);
            }
            List<Expression> values = parseList(this::parseExpression);
            expect(RPAREN);
            return new Expression.InExpr(left, values, null, negated);
        }

        // LIKE / ILIKE
        if (check(LIKE) || check(ILIKE)) {
            Expression.BinaryOperator op = check(LIKE) ? Expression.BinaryOperator.LIKE
                    : Expression.BinaryOperator.ILIKE;
            advance();
            return new Expression.BinaryOp(left, op, parseAddExpr());
        }

        // Comparison operators
        if (current().isComparisonOp()) {
            Expression.BinaryOperator op = switch (current()) {
                case EQ -> Expression.BinaryOperator.EQ;
                case NE -> Expression.BinaryOperator.NE;
                case LT -> Expression.BinaryOperator.LT;
                case LE -> Expression.BinaryOperator.LE;
                case GT -> Expression.BinaryOperator.GT;
                case GE -> Expression.BinaryOperator.GE;
                default -> throw error("Unexpected comparison operator");
            };
            advance();
            return new Expression.BinaryOp(left, op, parseAddExpr());
        }

        return left;
    }

    private boolean peekIs(Token t) {
        Lexer.SavePoint mark = lexer.mark();
        advance();
        boolean result = check(t);
        lexer.reset(mark);
        return result;
    }

    private Expression parseAddExpr() {
        Expression left = parseMulExpr();
        while (check(PLUS) || check(MINUS) || check(CONCAT)) {
            Expression.BinaryOperator op = switch (current()) {
                case PLUS -> Expression.BinaryOperator.PLUS;
                case MINUS -> Expression.BinaryOperator.MINUS;
                case CONCAT -> Expression.BinaryOperator.CONCAT;
                default -> throw error("Unexpected");
            };
            advance();
            left = new Expression.BinaryOp(left, op, parseMulExpr());
        }
        return left;
    }

    private Expression parseMulExpr() {
        Expression left = parseUnaryExpr();
        while (check(STAR) || check(SLASH) || check(PERCENT)) {
            Expression.BinaryOperator op = switch (current()) {
                case STAR -> Expression.BinaryOperator.MULTIPLY;
                case SLASH -> Expression.BinaryOperator.DIVIDE;
                case PERCENT -> Expression.BinaryOperator.MODULO;
                default -> throw error("Unexpected");
            };
            advance();
            left = new Expression.BinaryOp(left, op, parseUnaryExpr());
        }
        return left;
    }

    private Expression parseUnaryExpr() {
        if (consumeIf(MINUS)) {
            return new Expression.UnaryOp(Expression.UnaryOperator.MINUS, parseUnaryExpr());
        }
        if (consumeIf(PLUS)) {
            return new Expression.UnaryOp(Expression.UnaryOperator.PLUS, parseUnaryExpr());
        }
        return parsePrimaryExpr();
    }

    private Expression parsePrimaryExpr() {
        // EXISTS
        if (consumeIf(EXISTS)) {
            expect(LPAREN);
            SelectStatement subquery = parseSelect();
            expect(RPAREN);
            return new Expression.ExistsExpr(subquery, false);
        }

        // CASE
        if (check(CASE)) {
            return parseCaseExpr();
        }

        // CAST
        if (consumeIf(CAST)) {
            expect(LPAREN);
            Expression expr = parseExpression();
            expect(AS);
            String type = expectIdentifier();
            expect(RPAREN);
            return new Expression.CastExpr(expr, type);
        }

        // Parenthesized expression or subquery
        if (consumeIf(LPAREN)) {
            if (check(SELECT)) {
                SelectStatement subquery = parseSelect();
                expect(RPAREN);
                return new Expression.SubqueryExpr(subquery);
            }
            Expression expr = parseExpression();
            expect(RPAREN);
            return expr;
        }

        // Literals
        if (check(STRING)) {
            String val = stringVal();
            advance();
            return Expression.stringLiteral(val);
        }
        if (check(INTEGER)) {
            long val = Long.parseLong(stringVal());
            advance();
            return Expression.intLiteral(val);
        }
        if (check(DECIMAL)) {
            double val = Double.parseDouble(stringVal());
            advance();
            return Expression.decimalLiteral(val);
        }
        if (consumeIf(TRUE))
            return Expression.boolLiteral(true);
        if (consumeIf(FALSE))
            return Expression.boolLiteral(false);
        if (consumeIf(NULL))
            return Expression.nullLiteral();

        // Identifier, function call, or column reference
        return parseIdentifierExpr();
    }

    private Expression parseIdentifierExpr() {
        String name = expectIdentifier();

        // Function call
        if (consumeIf(LPAREN)) {
            // COUNT(*)
            if (name.equalsIgnoreCase("COUNT") && consumeIf(STAR)) {
                expect(RPAREN);
                Expression.FunctionCall func = new Expression.FunctionCall(name, List.of(Expression.column("*")),
                        false);
                return maybeParseOver(func);
            }

            boolean distinct = consumeIf(DISTINCT);
            List<Expression> args = check(RPAREN) ? List.of() : parseList(this::parseExpression);
            expect(RPAREN);

            Expression.FunctionCall func = new Expression.FunctionCall(name, args, distinct);
            return maybeParseOver(func);
        }

        // Qualified column: table.column
        if (consumeIf(DOT)) {
            String column = expectIdentifier();
            return Expression.column(name, column);
        }

        // Postgres cast: name::type
        if (consumeIf(DOUBLE_COLON)) {
            String type = expectIdentifier();
            return new Expression.CastExpr(Expression.column(name), type);
        }

        return Expression.column(name);
    }

    private Expression maybeParseOver(Expression.FunctionCall func) {
        if (!consumeIf(OVER))
            return func;

        expect(LPAREN);

        List<Expression> partitionBy = List.of();
        if (consumeIf(PARTITION)) {
            expect(BY);
            partitionBy = parseList(this::parseExpression);
        }

        List<OrderSpec> orderBy = List.of();
        if (consumeIf(ORDER)) {
            expect(BY);
            orderBy = parseList(this::parseOrderSpec);
        }

        Expression.FrameSpec frame = null;
        if (check(ROWS) || check(RANGE)) {
            frame = parseFrameSpec();
        }

        expect(RPAREN);
        return new Expression.WindowExpr(func, partitionBy, orderBy, frame);
    }

    private Expression.FrameSpec parseFrameSpec() {
        Expression.FrameType type = consumeIf(ROWS) ? Expression.FrameType.ROWS : Expression.FrameType.RANGE;
        if (type == null) {
            consume(RANGE);
            type = Expression.FrameType.RANGE;
        }

        expect(BETWEEN);
        Expression.FrameBound start = parseFrameBound();
        expect(AND);
        Expression.FrameBound end = parseFrameBound();

        return new Expression.FrameSpec(type, start, end);
    }

    private void consume(Token t) {
        expect(t);
    }

    private Expression.FrameBound parseFrameBound() {
        if (consumeIf(UNBOUNDED)) {
            if (consumeIf(PRECEDING))
                return Expression.FrameBound.unboundedPreceding();
            if (consumeIf(FOLLOWING))
                return Expression.FrameBound.unboundedFollowing();
            throw error("Expected PRECEDING or FOLLOWING");
        }
        if (consumeIf(CURRENT)) {
            expect(ROW);
            return Expression.FrameBound.currentRow();
        }
        int offset = expectInteger();
        if (consumeIf(PRECEDING))
            return Expression.FrameBound.preceding(offset);
        if (consumeIf(FOLLOWING))
            return Expression.FrameBound.following(offset);
        throw error("Expected PRECEDING or FOLLOWING");
    }

    private Expression.CaseExpr parseCaseExpr() {
        expect(CASE);
        List<Expression.WhenClause> whenClauses = new ArrayList<>();

        while (consumeIf(WHEN)) {
            Expression condition = parseExpression();
            expect(THEN);
            Expression result = parseExpression();
            whenClauses.add(new Expression.WhenClause(condition, result));
        }

        Expression elseExpr = null;
        if (consumeIf(ELSE)) {
            elseExpr = parseExpression();
        }

        expect(END);
        return new Expression.CaseExpr(whenClauses, elseExpr);
    }
}
