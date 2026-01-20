package org.finos.legend.pure.dsl;

import org.finos.legend.pure.dsl.Token.TokenType;
import org.finos.legend.pure.dsl.graphfetch.GraphFetchTree;

import java.util.ArrayList;
import java.util.List;

/**
 * Recursive descent parser for the Pure language.
 * 
 * Parses Pure query expressions like:
 * Person.all()->filter({p | $p.lastName == 'Smith'})->project([{p |
 * $p.firstName}, {p | $p.lastName}])
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
     * Parses a chain of arrow expressions with type tracking:
     * Class.all()->filter(...)->project(...)->groupBy(...)
     * 
     * Type transitions:
     * - Class.all() -> ClassExpression
     * - filter() on ClassExpression -> ClassExpression
     * - project() on ClassExpression -> RelationExpression
     * - filter() on RelationExpression -> RelationExpression
     * - groupBy() on RelationExpression -> RelationExpression
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
                case "groupBy" -> parseGroupByCall(expr);
                case "sortBy" -> parseSortByCall(expr);
                case "sort" -> parseSortCall(expr);
                case "limit", "take" -> parseLimitCall(expr);
                case "drop" -> parseDropCall(expr);
                case "slice" -> parseSliceCall(expr);
                // Relation API operations
                case "select" -> parseSelectCall(expr);
                case "extend" -> parseExtendCall(expr);
                case "from" -> parseFromCall(expr);
                // M2M graphFetch operations
                case "graphFetch" -> parseGraphFetchCall(expr);
                case "serialize" -> parseSerializeCall(expr);
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

        // Relation literal: #>{store::DB.TABLE_NAME}
        if (check(TokenType.HASH_GREATER)) {
            return parseRelationLiteral();
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
     * Parses a Relation literal: #>{store::DB.TABLE_NAME}
     * 
     * Syntax: #>{databaseRef.tableName} where databaseRef can be qualified (e.g.,
     * store::PersonDb)
     */
    private RelationLiteral parseRelationLiteral() {
        consume(TokenType.HASH_GREATER, "Expected '#>'");
        consume(TokenType.LBRACE, "Expected '{' after '#>'");

        // Parse qualified database reference (e.g., store::PersonDatabase)
        String dbRef = parseQualifiedName();

        consume(TokenType.DOT, "Expected '.' between database and table");
        Token tableName = consume(TokenType.IDENTIFIER, "Expected table name");

        consume(TokenType.RBRACE, "Expected '}' to close Relation literal");

        return new RelationLiteral(dbRef, tableName.value());
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
     * Parses filter function call with polymorphic dispatch:
     * - On ClassExpression -> ClassFilterExpression
     * - On RelationExpression -> RelationFilterExpression
     */
    private PureExpression parseFilterCall(PureExpression source) {
        LambdaExpression lambda = parseLambda();
        consume(TokenType.RPAREN, "Expected ')' after filter lambda");

        // Type-safe polymorphic dispatch
        if (source instanceof ClassExpression classSource) {
            return new ClassFilterExpression(classSource, lambda);
        } else if (source instanceof RelationExpression relationSource) {
            return new RelationFilterExpression(relationSource, lambda);
        } else {
            throw new PureParseException(
                    "filter() requires a Class or Relation expression, got: " + source.getClass().getSimpleName());
        }
    }

    /**
     * Parses project function call: project([{p | $p.firstName}, {p |
     * $p.lastName}])
     * 
     * project() converts ClassExpression to RelationExpression.
     * At parse time, we validate the source is a ClassExpression.
     */
    private RelationExpression parseProjectCall(PureExpression source) {
        // Type validation: project() only works on ClassExpression
        if (!(source instanceof ClassExpression classSource)) {
            throw new PureParseException(
                    "project() requires a Class expression (from .all() or filter()), got: " +
                            source.getClass().getSimpleName());
        }

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
        return new ProjectExpression(classSource, projections, aliases);
    }

    /**
     * Parses groupBy function call:
     * groupBy([{p | $p.department}], [{p | $p.salary}], ['totalSalary'])
     * 
     * groupBy() only works on RelationExpression (from project()).
     * At parse time, we validate the source is a RelationExpression.
     */
    private RelationExpression parseGroupByCall(PureExpression source) {
        // Type validation: groupBy() only works on RelationExpression
        if (!(source instanceof RelationExpression relationSource)) {
            throw new PureParseException(
                    "groupBy() requires a Relation expression (from project()). " +
                            "Use Class.all()->project(...)->groupBy(...). Got: " +
                            source.getClass().getSimpleName());
        }

        List<LambdaExpression> groupByColumns = new ArrayList<>();
        List<LambdaExpression> aggregations = new ArrayList<>();
        List<String> aliases = new ArrayList<>();

        // Parse groupBy columns (first argument)
        boolean arrayMode = check(TokenType.LBRACKET);
        if (arrayMode) {
            consume(TokenType.LBRACKET, "Expected '['");
        }

        groupByColumns.add(parseLambda());

        while (check(TokenType.COMMA) && !check(TokenType.RBRACKET)) {
            // Peek ahead to see if next is a lambda or something else
            int savedPos = position;
            consume(TokenType.COMMA, "Expected ','");

            if (check(TokenType.LBRACE)) {
                groupByColumns.add(parseLambda());
            } else {
                // Rewind and exit the loop - next arg is aggregations
                position = savedPos;
                break;
            }
        }

        if (arrayMode) {
            consume(TokenType.RBRACKET, "Expected ']'");
        }

        // Parse aggregation lambdas (second argument)
        consume(TokenType.COMMA, "Expected ',' after groupBy columns");

        arrayMode = check(TokenType.LBRACKET);
        if (arrayMode) {
            consume(TokenType.LBRACKET, "Expected '['");
        }

        aggregations.add(parseLambda());

        while (check(TokenType.COMMA)) {
            int savedPos = position;
            consume(TokenType.COMMA, "Expected ','");

            if (check(TokenType.LBRACE)) {
                aggregations.add(parseLambda());
            } else if (check(TokenType.STRING_LITERAL)) {
                // This is the aliases section
                aliases.add(consume(TokenType.STRING_LITERAL, "Expected alias").value());
                while (check(TokenType.COMMA)) {
                    consume(TokenType.COMMA, "Expected ','");
                    if (check(TokenType.STRING_LITERAL)) {
                        aliases.add(consume(TokenType.STRING_LITERAL, "Expected alias").value());
                    } else {
                        break;
                    }
                }
                break;
            } else {
                position = savedPos;
                break;
            }
        }

        if (arrayMode) {
            consume(TokenType.RBRACKET, "Expected ']'");
        }

        // Parse optional aliases (third argument)
        if (check(TokenType.COMMA) && aliases.isEmpty()) {
            consume(TokenType.COMMA, "Expected ','");

            if (check(TokenType.LBRACKET)) {
                consume(TokenType.LBRACKET, "Expected '['");
                aliases.add(consume(TokenType.STRING_LITERAL, "Expected alias").value());
                while (check(TokenType.COMMA)) {
                    consume(TokenType.COMMA, "Expected ','");
                    aliases.add(consume(TokenType.STRING_LITERAL, "Expected alias").value());
                }
                consume(TokenType.RBRACKET, "Expected ']'");
            } else if (check(TokenType.STRING_LITERAL)) {
                aliases.add(consume(TokenType.STRING_LITERAL, "Expected alias").value());
            }
        }

        consume(TokenType.RPAREN, "Expected ')' after groupBy");
        return new GroupByExpression(relationSource, groupByColumns, aggregations, aliases);
    }

    /**
     * Parses sortBy function call (for ClassExpression):
     * sortBy({p | $p.lastName}) or sortBy({p | $p.lastName}, 'desc')
     */
    private PureExpression parseSortByCall(PureExpression source) {
        if (!(source instanceof ClassExpression classSource)) {
            throw new PureParseException(
                    "sortBy() requires a Class expression. Got: " + source.getClass().getSimpleName());
        }

        LambdaExpression lambda = parseLambda();

        // Parse optional direction ('asc' or 'desc')
        boolean ascending = true;
        if (check(TokenType.COMMA)) {
            consume(TokenType.COMMA, "Expected ','");
            String direction = consume(TokenType.STRING_LITERAL, "Expected 'asc' or 'desc'").value();
            ascending = !direction.equalsIgnoreCase("desc");
        }

        consume(TokenType.RPAREN, "Expected ')' after sortBy");
        return new ClassSortByExpression(classSource, lambda, ascending);
    }

    /**
     * Parses sort function call (for RelationExpression):
     * sort('columnName') or sort('columnName', 'desc')
     */
    private PureExpression parseSortCall(PureExpression source) {
        if (!(source instanceof RelationExpression relationSource)) {
            throw new PureParseException(
                    "sort() requires a Relation expression. Got: " + source.getClass().getSimpleName());
        }

        String column = consume(TokenType.STRING_LITERAL, "Expected column name").value();

        // Parse optional direction ('asc' or 'desc')
        boolean ascending = true;
        if (check(TokenType.COMMA)) {
            consume(TokenType.COMMA, "Expected ','");
            String direction = consume(TokenType.STRING_LITERAL, "Expected 'asc' or 'desc'").value();
            ascending = !direction.equalsIgnoreCase("desc");
        }

        consume(TokenType.RPAREN, "Expected ')' after sort");
        return new RelationSortExpression(relationSource, column, ascending);
    }

    /**
     * Parses limit/take function call:
     * limit(10) or take(10)
     */
    private PureExpression parseLimitCall(PureExpression source) {
        String limitStr = consume(TokenType.INTEGER_LITERAL, "Expected limit number").value();
        int limit = Integer.parseInt(limitStr);
        consume(TokenType.RPAREN, "Expected ')' after limit");

        if (source instanceof ClassExpression classSource) {
            return ClassLimitExpression.limit(classSource, limit);
        } else if (source instanceof RelationExpression relationSource) {
            return RelationLimitExpression.limit(relationSource, limit);
        } else {
            throw new PureParseException("limit() requires Class or Relation expression");
        }
    }

    /**
     * Parses drop function call:
     * drop(10) - skip first 10 rows
     */
    private PureExpression parseDropCall(PureExpression source) {
        String offsetStr = consume(TokenType.INTEGER_LITERAL, "Expected offset number").value();
        int offset = Integer.parseInt(offsetStr);
        consume(TokenType.RPAREN, "Expected ')' after drop");

        if (source instanceof ClassExpression classSource) {
            return ClassLimitExpression.offset(classSource, offset);
        } else if (source instanceof RelationExpression relationSource) {
            return RelationLimitExpression.offset(relationSource, offset);
        } else {
            throw new PureParseException("drop() requires Class or Relation expression");
        }
    }

    /**
     * Parses slice function call:
     * slice(5, 15) - rows 5-14 (start inclusive, stop exclusive)
     */
    private PureExpression parseSliceCall(PureExpression source) {
        String startStr = consume(TokenType.INTEGER_LITERAL, "Expected start number").value();
        int start = Integer.parseInt(startStr);
        consume(TokenType.COMMA, "Expected ','");
        String stopStr = consume(TokenType.INTEGER_LITERAL, "Expected stop number").value();
        int stop = Integer.parseInt(stopStr);
        consume(TokenType.RPAREN, "Expected ')' after slice");

        if (source instanceof ClassExpression classSource) {
            return ClassLimitExpression.slice(classSource, start, stop);
        } else if (source instanceof RelationExpression relationSource) {
            return RelationLimitExpression.slice(relationSource, start, stop);
        } else {
            throw new PureParseException("slice() requires Class or Relation expression");
        }
    }

    // ==================== Relation API Operations ====================

    /**
     * Parses select function call on Relation:
     * select(~col1, ~col2, ~col3)
     * 
     * Uses the ~ prefix for column specifications.
     */
    private RelationExpression parseSelectCall(PureExpression source) {
        if (!(source instanceof RelationExpression relationSource)) {
            throw new PureParseException(
                    "select() requires a Relation expression. Got: " + source.getClass().getSimpleName());
        }

        List<String> columns = new ArrayList<>();

        // Parse column specifications: ~col1, ~col2, ...
        do {
            if (check(TokenType.COMMA)) {
                consume(TokenType.COMMA, "Expected ','");
            }
            consume(TokenType.TILDE, "Expected '~' before column name");
            String colName = consume(TokenType.IDENTIFIER, "Expected column name after '~'").value();
            columns.add(colName);
        } while (check(TokenType.COMMA));

        consume(TokenType.RPAREN, "Expected ')' after select");
        return new RelationSelectExpression(relationSource, columns);
    }

    /**
     * Parses extend function call on Relation:
     * 1. Simple: extend(~newCol : x | $x.col1 + $x.col2)
     * 2. Window: extend(~rowNum : row_number()->over(~department))
     * 3. Aggregate window: extend(~runningSum : sum(~salary)->over(~department,
     * ~salary))
     * 
     * Adds a calculated column (optionally with window function) to the Relation.
     */
    private RelationExpression parseExtendCall(PureExpression source) {
        if (!(source instanceof RelationExpression relationSource)) {
            throw new PureParseException(
                    "extend() requires a Relation expression. Got: " + source.getClass().getSimpleName());
        }

        // Parse column specification: ~newCol : ...
        consume(TokenType.TILDE, "Expected '~' before new column name");
        String newColName = consume(TokenType.IDENTIFIER, "Expected new column name").value();
        consume(TokenType.COLON, "Expected ':' after column name in extend");

        // Check if this is a window function or simple expression
        if (check(TokenType.IDENTIFIER) && isWindowFunctionName(peekAhead())) {
            // Window function syntax: row_number(), rank(), sum(~col), etc.
            RelationExtendExpression.WindowFunctionSpec windowSpec = parseWindowFunctionSpec();
            consume(TokenType.RPAREN, "Expected ')' after extend");
            return RelationExtendExpression.window(relationSource, newColName, windowSpec);
        } else {
            // Simple expression with lambda
            LambdaExpression lambda = parseLambda();
            consume(TokenType.RPAREN, "Expected ')' after extend");
            return new RelationExtendExpression(relationSource, newColName, lambda);
        }
    }

    /**
     * Checks if the current token is a known window function name.
     */
    private boolean isWindowFunctionName(String name) {
        return switch (name.toLowerCase()) {
            case "row_number", "rank", "dense_rank", "ntile",
                    "lag", "lead", "first_value", "last_value",
                    "sum", "avg", "min", "max", "count" ->
                true;
            default -> false;
        };
    }

    /**
     * Peeks at the current token value without advancing.
     */
    private String peekAhead() {
        return peek().value();
    }

    /**
     * Parses a window function specification:
     * row_number()->over(~department, ~salary->desc())
     * sum(~salary)->over(~department, ~date->asc(), rows(unbounded(), 0))
     */
    private RelationExtendExpression.WindowFunctionSpec parseWindowFunctionSpec() {
        String functionName = consume(TokenType.IDENTIFIER, "Expected window function name").value();
        consume(TokenType.LPAREN, "Expected '(' after function name");

        // Parse optional aggregate column for aggregate window functions
        String aggregateColumn = null;
        if (!check(TokenType.RPAREN)) {
            // Aggregate function with column: sum(~salary)
            consume(TokenType.TILDE, "Expected '~' before column name");
            aggregateColumn = consume(TokenType.IDENTIFIER, "Expected column name").value();
        }
        consume(TokenType.RPAREN, "Expected ')' after function arguments");

        // Parse ->over(...)
        consume(TokenType.ARROW, "Expected '->' before over");
        String overKeyword = consume(TokenType.IDENTIFIER, "Expected 'over'").value();
        if (!overKeyword.equalsIgnoreCase("over")) {
            throw new PureParseException("Expected 'over' but got: " + overKeyword);
        }
        consume(TokenType.LPAREN, "Expected '(' after over");

        // Parse partition and order columns
        List<String> partitionColumns = new ArrayList<>();
        List<RelationExtendExpression.SortSpec> orderColumns = new ArrayList<>();
        RelationExtendExpression.FrameSpec frame = null;

        // Parse columns inside over(): ~col1, ~col2->desc(), rows(...)
        while (!check(TokenType.RPAREN)) {
            if (!partitionColumns.isEmpty() || !orderColumns.isEmpty() || frame != null) {
                consume(TokenType.COMMA, "Expected ',' between columns");
            }

            // Check for frame spec: rows(...) or range(...)
            if (check(TokenType.IDENTIFIER)) {
                String tokenValue = peek().value();
                if (tokenValue.equalsIgnoreCase("rows") || tokenValue.equalsIgnoreCase("range")) {
                    frame = parseFrameSpec();
                    continue;
                }
            }

            consume(TokenType.TILDE, "Expected '~' before column name");
            String colName = consume(TokenType.IDENTIFIER, "Expected column name").value();

            // Check for sort direction: ~col->desc() or ~col->asc()
            if (check(TokenType.ARROW)) {
                consume(TokenType.ARROW, "Expected '->'");
                String direction = consume(TokenType.IDENTIFIER, "Expected 'asc' or 'desc'").value();
                consume(TokenType.LPAREN, "Expected '('");
                consume(TokenType.RPAREN, "Expected ')'");

                RelationExtendExpression.SortDirection sortDir = direction.equalsIgnoreCase("desc")
                        ? RelationExtendExpression.SortDirection.DESC
                        : RelationExtendExpression.SortDirection.ASC;
                orderColumns.add(new RelationExtendExpression.SortSpec(colName, sortDir));
            } else {
                // No direction specified - treat as partition column
                partitionColumns.add(colName);
            }
        }

        consume(TokenType.RPAREN, "Expected ')' to close over");

        if (aggregateColumn != null) {
            return RelationExtendExpression.WindowFunctionSpec.aggregate(
                    functionName, aggregateColumn, partitionColumns, orderColumns, frame);
        } else {
            return RelationExtendExpression.WindowFunctionSpec.ranking(
                    functionName, partitionColumns, orderColumns, frame);
        }
    }

    /**
     * Parses a frame specification: rows(start, end) or range(start, end)
     * 
     * Frame bounds:
     * - unbounded() = UNBOUNDED
     * - 0 = CURRENT ROW
     * - negative integer = PRECEDING
     * - positive integer = FOLLOWING
     */
    private RelationExtendExpression.FrameSpec parseFrameSpec() {
        String frameType = consume(TokenType.IDENTIFIER, "Expected 'rows' or 'range'").value();
        consume(TokenType.LPAREN, "Expected '(' after frame type");

        RelationExtendExpression.FrameBound start = parseFrameBound();
        consume(TokenType.COMMA, "Expected ',' between frame bounds");
        RelationExtendExpression.FrameBound end = parseFrameBound();

        consume(TokenType.RPAREN, "Expected ')' to close frame");

        if (frameType.equalsIgnoreCase("rows")) {
            return RelationExtendExpression.FrameSpec.rows(start, end);
        } else {
            return RelationExtendExpression.FrameSpec.range(start, end);
        }
    }

    /**
     * Parses a frame bound: unbounded(), integer (0, -3, 1), etc.
     * Note: The lexer handles negative numbers, so -3 is a single INTEGER_LITERAL
     * token.
     */
    private RelationExtendExpression.FrameBound parseFrameBound() {
        // Check for unbounded()
        if (check(TokenType.IDENTIFIER) && peek().value().equalsIgnoreCase("unbounded")) {
            consume(TokenType.IDENTIFIER, "Expected 'unbounded'");
            consume(TokenType.LPAREN, "Expected '('");
            consume(TokenType.RPAREN, "Expected ')'");
            return RelationExtendExpression.FrameBound.unbounded();
        }

        // Parse the number (lexer includes negative sign in INTEGER_LITERAL)
        String numStr = consume(TokenType.INTEGER_LITERAL, "Expected number for frame bound").value();
        int value = Integer.parseInt(numStr);

        return RelationExtendExpression.FrameBound.fromInteger(value);
    }

    /**
     * Parses from function call:
     * from(My::Runtime::DuckDb)
     * 
     * Binds a Relation query to its runtime (database connection).
     */
    private FromExpression parseFromCall(PureExpression source) {
        // Parse qualified runtime reference: My::Runtime::DuckDb
        String runtimeRef = parseQualifiedName();

        consume(TokenType.RPAREN, "Expected ')' after from");
        return new FromExpression(source, runtimeRef);
    }

    // ==================== M2M graphFetch Operations ====================

    /**
     * Parses graphFetch function call:
     * graphFetch(#{ Person { firstName, lastName } }#)
     * 
     * graphFetch() specifies which properties to fetch from the object graph.
     * Must be called on a ClassExpression (e.g., Person.all()).
     */
    private GraphFetchExpression parseGraphFetchCall(PureExpression source) {
        if (!(source instanceof ClassExpression classSource)) {
            throw new PureParseException(
                    "graphFetch() requires a Class expression. Got: " + source.getClass().getSimpleName());
        }

        // Parse #{...}# tree
        GraphFetchTree tree = parseGraphFetchTree();

        consume(TokenType.RPAREN, "Expected ')' after graphFetch");
        return new GraphFetchExpression(classSource, tree);
    }

    /**
     * Parses serialize function call:
     * serialize(#{ Person { firstName, lastName } }#)
     * 
     * serialize() terminates a graphFetch chain and produces JSON output.
     * Must be called on a GraphFetchExpression.
     */
    private SerializeExpression parseSerializeCall(PureExpression source) {
        if (!(source instanceof GraphFetchExpression graphFetchSource)) {
            throw new PureParseException(
                    "serialize() requires a graphFetch() expression. " +
                            "Use: Class.all()->graphFetch(...)->serialize(...). Got: " +
                            source.getClass().getSimpleName());
        }

        // Parse #{...}# tree
        GraphFetchTree tree = parseGraphFetchTree();

        consume(TokenType.RPAREN, "Expected ')' after serialize");
        return new SerializeExpression(graphFetchSource, tree);
    }

    /**
     * Parses a graphFetch tree: #{ ClassName { prop1, prop2 } }#
     */
    private GraphFetchTree parseGraphFetchTree() {
        consume(TokenType.HASH_LBRACE, "Expected '#{' to start graphFetch tree");

        // Parse class name
        String className = parseQualifiedName();
        consume(TokenType.LBRACE, "Expected '{' after class name in graphFetch tree");

        // Parse property list
        List<GraphFetchTree.PropertyFetch> properties = new ArrayList<>();

        while (!check(TokenType.RBRACE)) {
            if (!properties.isEmpty()) {
                consume(TokenType.COMMA, "Expected ',' between properties");
            }

            String propName = consume(TokenType.IDENTIFIER, "Expected property name").value();

            // Check for nested properties: propName { subProp1, subProp2 }
            if (check(TokenType.LBRACE)) {
                consume(TokenType.LBRACE, "Expected '{'");
                List<GraphFetchTree.PropertyFetch> nestedProps = new ArrayList<>();

                while (!check(TokenType.RBRACE)) {
                    if (!nestedProps.isEmpty()) {
                        consume(TokenType.COMMA, "Expected ','");
                    }
                    String nestedPropName = consume(TokenType.IDENTIFIER, "Expected property name").value();
                    nestedProps.add(GraphFetchTree.PropertyFetch.simple(nestedPropName));
                }

                consume(TokenType.RBRACE, "Expected '}' after nested properties");
                GraphFetchTree subTree = new GraphFetchTree(propName, nestedProps);
                properties.add(GraphFetchTree.PropertyFetch.nested(propName, subTree));
            } else {
                properties.add(GraphFetchTree.PropertyFetch.simple(propName));
            }
        }

        consume(TokenType.RBRACE, "Expected '}' after properties");
        consume(TokenType.RBRACE_HASH, "Expected '}#' to end graphFetch tree");

        return new GraphFetchTree(className, properties);
    }

    /**
     * Parses a qualified name: name::name::name
     */
    private String parseQualifiedName() {
        StringBuilder name = new StringBuilder();
        name.append(consume(TokenType.IDENTIFIER, "Expected identifier").value());

        while (check(TokenType.DOUBLE_COLON)) {
            consume(TokenType.DOUBLE_COLON, "Expected '::'");
            name.append("::");
            name.append(consume(TokenType.IDENTIFIER, "Expected identifier after '::'").value());
        }

        return name.toString();
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
