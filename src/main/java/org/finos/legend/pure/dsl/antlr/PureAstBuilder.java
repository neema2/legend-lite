package org.finos.legend.pure.dsl.antlr;

import org.finos.legend.pure.dsl.*;
import org.finos.legend.pure.dsl.graphfetch.GraphFetchTree;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * ANTLR visitor that converts parse tree to PureExpression IR.
 * This bridges the ANTLR-generated parser (based on legend-engine grammar) to
 * the existing IR types.
 * 
 * The legend-engine grammar structure:
 * - combinedExpression: expression expressionPart*
 * - expression: nonArrowOrEqualExpression (propertyOrFunctionExpression)*
 * (equalNotEqual)?
 * - nonArrowOrEqualExpression: atomicExpression | notExpression |
 * signedExpression | ...
 * - atomicExpression: instanceReference | instanceLiteralToken | variable |
 * lambdaFunction | ...
 * - instanceReference: qualifiedName allOrFunction? (e.g., Person.all())
 * - functionExpression: ARROW qualifiedName functionExpressionParameters
 * (->filter() ->project())
 */
public class PureAstBuilder extends PureParserBaseVisitor<PureExpression> {

    // ========================================
    // ENTRY POINT - Combined Expression
    // ========================================

    @Override
    public PureExpression visitCombinedExpression(PureParser.CombinedExpressionContext ctx) {
        // Start with the first comparison expression
        PureExpression expr = visitComparisonExpression(ctx.comparisonExpression());

        // Process boolean parts (&& / ||) which chain comparison expressions
        for (PureParser.BooleanPartContext bp : ctx.booleanPart()) {
            PureExpression right = visitComparisonExpression(bp.comparisonExpression());
            if (bp.AND() != null) {
                expr = LogicalExpr.and(expr, right);
            } else {
                expr = LogicalExpr.or(expr, right);
            }
        }

        return expr;
    }

    public PureExpression visitComparisonExpression(PureParser.ComparisonExpressionContext ctx) {
        // Start with the base expression
        PureExpression expr = visit(ctx.expression());

        // Process arithmetic parts (including comparison operators like >, <, etc.)
        for (PureParser.ArithmeticPartContext ap : ctx.arithmeticPart()) {
            expr = processArithmeticPart(expr, ap);
        }

        return expr;
    }

    private PureExpression processArithmeticPart(PureExpression left, PureParser.ArithmeticPartContext ctx) {
        List<PureParser.ExpressionContext> exprs = ctx.expression();
        if (!exprs.isEmpty()) {
            String op = determineArithmeticOp(ctx);
            PureExpression result = left;
            for (PureParser.ExpressionContext exprCtx : exprs) {
                PureExpression right = visit(exprCtx);
                result = new BinaryExpression(result, op, right);
            }
            return result;
        }
        return left;
    }

    private String determineArithmeticOp(PureParser.ArithmeticPartContext ctx) {
        if (!ctx.PLUS().isEmpty())
            return "+";
        if (!ctx.MINUS().isEmpty())
            return "-";
        if (!ctx.STAR().isEmpty())
            return "*";
        if (!ctx.DIVIDE().isEmpty())
            return "/";
        if (ctx.LESS_THAN() != null)
            return "<";
        if (ctx.LESS_OR_EQUAL() != null)
            return "<=";
        if (ctx.GREATER_THAN() != null)
            return ">";
        if (ctx.GREATER_OR_EQUAL() != null)
            return ">=";
        return "+";
    }

    // ========================================
    // EXPRESSION - Main chain processing
    // ========================================

    @Override
    public PureExpression visitExpression(PureParser.ExpressionContext ctx) {
        // First get the primary expression
        PureExpression expr = visit(ctx.nonArrowOrEqualExpression());

        // Process chain of property/function expressions: .property or ->function()
        for (PureParser.PropertyOrFunctionExpressionContext pofCtx : ctx.propertyOrFunctionExpression()) {
            expr = processPropertyOrFunction(expr, pofCtx);
        }

        // Process == or != comparison
        if (ctx.equalNotEqual() != null) {
            PureParser.EqualNotEqualContext eq = ctx.equalNotEqual();
            String op = eq.TEST_EQUAL() != null ? "==" : "!=";
            PureExpression right = visit(eq.combinedArithmeticOnly());
            return new ComparisonExpr(expr, ComparisonExpr.Operator.fromSymbol(op), right);
        }

        return expr;
    }

    private PureExpression processPropertyOrFunction(PureExpression source,
            PureParser.PropertyOrFunctionExpressionContext ctx) {
        if (ctx.propertyExpression() != null) {
            return processPropertyExpression(source, ctx.propertyExpression());
        }
        if (ctx.functionExpression() != null) {
            return processFunctionExpression(source, ctx.functionExpression());
        }
        if (ctx.propertyBracketExpression() != null) {
            // Handle array access like [0] or ['key']
            PureParser.PropertyBracketExpressionContext bracket = ctx.propertyBracketExpression();
            if (bracket.INTEGER() != null) {
                int index = Integer.parseInt(bracket.INTEGER().getText());
                return new IndexAccess(source, index);
            }
            if (bracket.STRING() != null) {
                String key = unquote(bracket.STRING().getText());
                return new PropertyAccessExpression(source, key);
            }
        }
        throw new PureParseException("Unknown property/function: " + ctx.getText());
    }

    private PureExpression processPropertyExpression(PureExpression source, PureParser.PropertyExpressionContext ctx) {
        // .propertyName or .method()
        String propName = getIdentifierText(ctx.identifier());

        if (ctx.functionExpressionParameters() != null) {
            // It's a method call like .toOne()
            List<PureExpression> args = parseFunctionArgs(ctx.functionExpressionParameters());
            return new MethodCall(source, propName, args);
        }

        // It's a property access
        return new PropertyAccessExpression(source, propName);
    }

    private PureExpression processFunctionExpression(PureExpression source, PureParser.FunctionExpressionContext ctx) {
        // Process chain of arrow functions: ->filter()->project()->limit()
        PureExpression result = source;

        List<PureParser.QualifiedNameContext> names = ctx.qualifiedName();
        List<PureParser.FunctionExpressionParametersContext> params = ctx.functionExpressionParameters();

        for (int i = 0; i < names.size(); i++) {
            String funcName = getQualifiedNameText(names.get(i));
            List<PureExpression> args = parseFunctionArgs(params.get(i));
            result = createFunctionCall(result, funcName, args);
        }

        return result;
    }

    private PureExpression createFunctionCall(PureExpression source, String funcName, List<PureExpression> args) {
        return switch (funcName) {
            case "filter" -> parseFilterCall(source, args);
            case "project" -> parseProjectCall(source, args);
            case "groupBy" -> parseGroupByCall(source, args);
            case "join" -> parseJoinCall(source, args);
            case "sortBy" -> parseSortByCall(source, args);
            case "sort" -> parseSortCall(source, args);
            case "limit", "take" -> parseLimitCall(source, args);
            case "drop" -> parseDropCall(source, args);
            case "slice" -> parseSliceCall(source, args);
            case "select" -> parseSelectCall(source, args);
            case "extend" -> parseExtendCall(source, args);
            case "from" -> parseFromCall(source, args);
            case "graphFetch" -> parseGraphFetchCall(source, args);
            case "serialize" -> parseSerializeCall(source, args);
            case "save" -> new SaveExpression(source);
            case "update" -> parseUpdateCall(source, args);
            case "delete" -> new DeleteExpression(source);
            default -> new MethodCall(source, funcName, args);
        };
    }

    // ========================================
    // NON-ARROW EXPRESSION (Primary)
    // ========================================

    @Override
    public PureExpression visitNonArrowOrEqualExpression(PureParser.NonArrowOrEqualExpressionContext ctx) {
        if (ctx.atomicExpression() != null) {
            return visit(ctx.atomicExpression());
        }
        if (ctx.notExpression() != null) {
            PureExpression inner = visit(ctx.notExpression().expression());
            return new UnaryExpression("!", inner);
        }
        if (ctx.signedExpression() != null) {
            PureParser.SignedExpressionContext signed = ctx.signedExpression();
            String op = signed.MINUS() != null ? "-" : "+";
            PureExpression inner = visit(signed.expression());
            return new UnaryExpression(op, inner);
        }
        if (ctx.expressionsArray() != null) {
            return visit(ctx.expressionsArray());
        }
        if (ctx.combinedExpression() != null) {
            return visit(ctx.combinedExpression());
        }
        throw new PureParseException("Unknown non-arrow expression: " + ctx.getText());
    }

    // ========================================
    // ATOMIC EXPRESSION
    // ========================================

    @Override
    public PureExpression visitAtomicExpression(PureParser.AtomicExpressionContext ctx) {
        if (ctx.dsl() != null) {
            return visitDsl(ctx.dsl());
        }
        if (ctx.instanceReference() != null) {
            return visit(ctx.instanceReference());
        }
        if (ctx.instanceLiteralToken() != null) {
            return visit(ctx.instanceLiteralToken());
        }
        if (ctx.variable() != null) {
            return visit(ctx.variable());
        }
        if (ctx.anyLambda() != null) {
            return visit(ctx.anyLambda());
        }
        if (ctx.expressionInstance() != null) {
            return visit(ctx.expressionInstance());
        }
        if (ctx.columnBuilders() != null) {
            return visit(ctx.columnBuilders());
        }
        if (ctx.type() != null) {
            // @Type reference
            String typeName = getTypeText(ctx.type());
            return new TypeReference(typeName);
        }
        throw new PureParseException("Unknown atomic expression: " + ctx.getText());
    }

    /**
     * Handles DSL/island mode expressions.
     * 
     * ISLAND_OPEN captures '#...{' where ... is the DSL type identifier:
     * - '#{' (empty) -> GraphFetch tree
     * - '#>{' ('>') -> Relation literal
     */
    @Override
    public PureExpression visitDsl(PureParser.DslContext ctx) {
        if (ctx.dslExtension() != null) {
            return visitDslExtension(ctx.dslExtension());
        }
        if (ctx.dslNavigationPath() != null) {
            // Navigation paths not yet supported
            throw new PureParseException("Navigation paths not supported: " + ctx.getText());
        }
        throw new PureParseException("Unknown DSL: " + ctx.getText());
    }

    @Override
    public PureExpression visitDslExtension(PureParser.DslExtensionContext ctx) {
        // ISLAND_OPEN is the '#...{' token
        String islandOpen = ctx.ISLAND_OPEN().getText();

        // Extract DSL type: everything between '#' and '{'
        String dslType = islandOpen.substring(1, islandOpen.length() - 1);

        // Collect island content tokens
        // For relation literals (>), stop at first ISLAND_BRACE_CLOSE since they use
        // }-> not }#
        // For GraphFetch (empty), collect everything except ISLAND_END
        boolean isRelation = ">".equals(dslType);
        StringBuilder content = new StringBuilder();
        StringBuilder trailingContent = new StringBuilder();
        boolean seenBraceClose = false;

        for (PureParser.DslExtensionContentContext contentCtx : ctx.dslExtensionContent()) {
            // Skip ISLAND_END token
            if (contentCtx.ISLAND_END() != null) {
                continue;
            }

            if (isRelation) {
                if (!seenBraceClose && contentCtx.ISLAND_BRACE_CLOSE() != null) {
                    // First } marks end of relation literal
                    seenBraceClose = true;
                    continue;
                }
                if (seenBraceClose) {
                    // Collect trailing content including braces
                    if (contentCtx.ISLAND_BRACE_OPEN() != null) {
                        trailingContent.append("{");
                    } else if (contentCtx.ISLAND_BRACE_CLOSE() != null) {
                        trailingContent.append("}");
                    } else {
                        trailingContent.append(contentCtx.getText());
                    }
                } else {
                    content.append(contentCtx.getText());
                }
            } else {
                content.append(contentCtx.getText());
            }
        }
        String contentText = content.toString().trim();
        String trailingText = trailingContent.toString().trim();

        // For GraphFetch only, remove trailing } from nested braces
        if (!isRelation) {
            while (contentText.endsWith("}")) {
                contentText = contentText.substring(0, contentText.length() - 1).trim();
            }
        }

        // Parse the main content
        PureExpression result = switch (dslType) {
            case "" -> parseGraphFetchTree(contentText);
            case ">" -> parseRelationLiteral(contentText);
            default -> throw new PureParseException("Unknown DSL type: #" + dslType + "{");
        };

        // For relation literals, parse trailing arrow chains and wrap the result
        if (isRelation && !trailingText.isEmpty()) {
            result = parseTrailingArrowChain(result, trailingText);
        }

        return result;
    }

    /**
     * Parses trailing arrow chain content and applies it to the source expression.
     * Example: "->from(myRuntime)" or "->select(~name, ~age)->from(duckdb)"
     */
    private PureExpression parseTrailingArrowChain(PureExpression source, String trailingText) {
        // Build a synthetic expression: "source" + trailing
        // We'll re-parse the trailing as a combinedExpression and extract function
        // calls

        // Parse the trailing content - format is like "->from(runtime)" or
        // "->select(...)->from(...)"
        // For each arrow function, wrap the source
        PureExpression result = source;
        String remaining = trailingText.trim();

        while (remaining.startsWith("->")) {
            remaining = remaining.substring(2); // Skip "->"

            // Find function name (up to '(')
            int parenIdx = remaining.indexOf('(');
            if (parenIdx == -1) {
                throw new PureParseException("Invalid arrow chain: " + trailingText);
            }
            String funcName = remaining.substring(0, parenIdx).trim();

            // Find matching closing paren
            int depth = 1;
            int closeIdx = parenIdx + 1;
            while (closeIdx < remaining.length() && depth > 0) {
                char c = remaining.charAt(closeIdx);
                if (c == '(')
                    depth++;
                else if (c == ')')
                    depth--;
                closeIdx++;
            }

            String argsContent = remaining.substring(parenIdx + 1, closeIdx - 1).trim();
            remaining = (closeIdx < remaining.length()) ? remaining.substring(closeIdx).trim() : "";

            // Parse arguments and create function expression
            result = createFunctionCall(result, funcName, argsContent);
        }

        return result;
    }

    /**
     * Creates a function call expression wrapping the source.
     */
    private PureExpression createFunctionCall(PureExpression source, String funcName, String argsContent) {
        return switch (funcName) {
            case "from" -> new FromExpression(source, argsContent);
            case "select" -> parseSelectCall(source, argsContent);
            case "filter" -> parseFilterCallFromString(source, argsContent);
            case "extend" -> parseExtendCallFromString(source, argsContent);
            case "sort" -> parseSortCallFromString(source, argsContent);
            case "limit", "take" -> parseLimitCallFromString(source, argsContent);
            case "drop" -> parseDropCallFromString(source, argsContent);
            default -> throw new PureParseException("Unknown function in arrow chain: " + funcName);
        };
    }

    /**
     * Parses select arguments and creates a RelationSelectExpression.
     */
    private PureExpression parseSelectCall(PureExpression source, String argsContent) {
        // Parse column specs like "~name, ~age"
        List<String> columns = new ArrayList<>();
        for (String part : argsContent.split(",")) {
            String col = part.trim();
            if (col.startsWith("~")) {
                col = col.substring(1);
            }
            if (!col.isEmpty()) {
                columns.add(col);
            }
        }
        return new RelationSelectExpression((RelationExpression) source, columns);
    }

    private PureExpression parseFilterCallFromString(PureExpression source, String argsContent) {
        PureExpression predicate = org.finos.legend.pure.dsl.PureParser.parse(argsContent);
        LambdaExpression lambda = (predicate instanceof LambdaExpression)
                ? (LambdaExpression) predicate
                : new LambdaExpression("x", predicate);
        // Relation API chains always start from RelationExpression
        return new RelationFilterExpression((RelationExpression) source, lambda);
    }

    private PureExpression parseExtendCallFromString(PureExpression source, String argsContent) {
        // Format: ~columnName : {param | body}
        // or: ~columnName : expression
        String content = argsContent.trim();

        // Extract column name
        if (content.startsWith("~")) {
            content = content.substring(1);
        }

        int colonIdx = content.indexOf(':');
        if (colonIdx > 0) {
            String columnName = content.substring(0, colonIdx).trim();
            String exprContent = content.substring(colonIdx + 1).trim();

            PureExpression extension = org.finos.legend.pure.dsl.PureParser.parse(exprContent);
            LambdaExpression lambda = (extension instanceof LambdaExpression)
                    ? (LambdaExpression) extension
                    : new LambdaExpression("x", extension);

            return new RelationExtendExpression((RelationExpression) source, columnName, lambda);
        }

        // Fallback for simple extend
        PureExpression extension = org.finos.legend.pure.dsl.PureParser.parse(content);
        return new ExtendExpression(source, List.of(extension));
    }

    private PureExpression parseSortCallFromString(PureExpression source, String argsContent) {
        PureExpression sortSpec = org.finos.legend.pure.dsl.PureParser.parse(argsContent);
        return new SortExpression(source, List.of(sortSpec));
    }

    private PureExpression parseLimitCallFromString(PureExpression source, String argsContent) {
        int limit = Integer.parseInt(argsContent.trim());
        return new LimitExpression(source, limit);
    }

    private PureExpression parseDropCallFromString(PureExpression source, String argsContent) {
        int dropCount = Integer.parseInt(argsContent.trim());
        return new DropExpression(source, dropCount);
    }

    /**
     * Parses GraphFetch tree content using ANTLR graphFetchTree rule.
     * Content format: ClassName { prop1, prop2, nested { nestedProp } }
     */
    private PureExpression parseGraphFetchTree(String content) {
        // Use ANTLR to parse the content as a graphFetchTree
        PureLexer lexer = new PureLexer(org.antlr.v4.runtime.CharStreams.fromString(content));
        org.antlr.v4.runtime.CommonTokenStream tokens = new org.antlr.v4.runtime.CommonTokenStream(lexer);
        PureParser parser = new PureParser(tokens);

        // Parse as graphFetchTree rule
        PureParser.GraphFetchTreeContext tree = parser.graphFetchTree();
        return buildGraphFetchTree(tree);
    }

    /**
     * Visits a graphFetchTree context and builds a GraphFetchTree.
     */
    private GraphFetchTree buildGraphFetchTree(PureParser.GraphFetchTreeContext ctx) {
        String rootClass = getQualifiedNameText(ctx.qualifiedName());
        List<GraphFetchTree.PropertyFetch> properties = buildGraphDefinition(ctx.graphDefinition());
        return new GraphFetchTree(rootClass, properties);
    }

    /**
     * Builds properties from a graphDefinition context (the { } block with
     * properties).
     */
    private List<GraphFetchTree.PropertyFetch> buildGraphDefinition(PureParser.GraphDefinitionContext ctx) {
        return buildGraphPaths(ctx.graphPaths());
    }

    /**
     * Visits graphPaths (list of graphPath or subTypeGraphPath).
     */
    private List<GraphFetchTree.PropertyFetch> buildGraphPaths(PureParser.GraphPathsContext ctx) {
        List<GraphFetchTree.PropertyFetch> properties = new ArrayList<>();

        for (PureParser.GraphPathContext pathCtx : ctx.graphPath()) {
            properties.add(buildGraphPath(pathCtx));
        }

        // Handle subtypes if present (we ignore them for now, can extend later)

        return properties;
    }

    /**
     * Visits a single graphPath (property name with optional nested structure).
     */
    private GraphFetchTree.PropertyFetch buildGraphPath(PureParser.GraphPathContext ctx) {
        String propertyName = ctx.identifier().getText();

        // Check for nested graphDefinition
        if (ctx.graphDefinition() != null) {
            List<GraphFetchTree.PropertyFetch> nestedProps = buildGraphDefinition(ctx.graphDefinition());
            GraphFetchTree nestedTree = new GraphFetchTree(null, nestedProps);
            return GraphFetchTree.PropertyFetch.nested(propertyName, nestedTree);
        }

        return GraphFetchTree.PropertyFetch.simple(propertyName);
    }

    /**
     * Parses Relation literal content using ANTLR relationLiteral rule.
     * Content format: store::DatabaseRef.TABLE_NAME
     */
    private PureExpression parseRelationLiteral(String content) {
        // Use ANTLR to parse the content as a relationLiteral
        PureLexer lexer = new PureLexer(org.antlr.v4.runtime.CharStreams.fromString(content));
        org.antlr.v4.runtime.CommonTokenStream tokens = new org.antlr.v4.runtime.CommonTokenStream(lexer);
        PureParser parser = new PureParser(tokens);

        // Parse as relationLiteral rule
        PureParser.RelationLiteralContext tree = parser.relationLiteral();
        return buildRelationLiteral(tree);
    }

    /**
     * Builds a RelationLiteral from the parsed context.
     */
    private RelationLiteral buildRelationLiteral(PureParser.RelationLiteralContext ctx) {
        String dbRef = getQualifiedNameText(ctx.qualifiedName());
        String tableName = ctx.identifier().getText();
        return new RelationLiteral(dbRef, tableName);
    }

    // ========================================
    // INSTANCE REFERENCE (e.g., Person.all())
    // ========================================

    @Override
    public PureExpression visitInstanceReference(PureParser.InstanceReferenceContext ctx) {
        String name = null;

        if (ctx.qualifiedName() != null) {
            name = getQualifiedNameText(ctx.qualifiedName());
        } else if (ctx.unitName() != null) {
            name = getUnitNameText(ctx.unitName());
        } else if (ctx.PATH_SEPARATOR() != null) {
            name = "::";
        }

        if (name == null) {
            throw new PureParseException("Invalid instance reference: " + ctx.getText());
        }

        // Check for .all() or function call
        if (ctx.allOrFunction() != null) {
            PureParser.AllOrFunctionContext aof = ctx.allOrFunction();

            if (aof.allFunction() != null || aof.allFunctionWithMilestoning() != null) {
                return new ClassAllExpression(name);
            }
            if (aof.allVersionsFunction() != null) {
                return new ClassAllVersionsExpression(name);
            }
            if (aof.functionExpressionParameters() != null) {
                List<PureExpression> args = parseFunctionArgs(aof.functionExpressionParameters());
                return new FunctionCall(name, args);
            }
        }

        // Just a class/package reference
        return new ClassReference(name);
    }

    // ========================================
    // VARIABLE
    // ========================================

    @Override
    public PureExpression visitVariable(PureParser.VariableContext ctx) {
        String varName = getIdentifierText(ctx.identifier());
        return new VariableExpr(varName);
    }

    // ========================================
    // LITERALS
    // ========================================

    @Override
    public PureExpression visitInstanceLiteralToken(PureParser.InstanceLiteralTokenContext ctx) {
        if (ctx.STRING() != null) {
            return LiteralExpr.string(unquote(ctx.STRING().getText()));
        }
        if (ctx.INTEGER() != null) {
            return LiteralExpr.integer(Long.parseLong(ctx.INTEGER().getText()));
        }
        if (ctx.FLOAT() != null) {
            return LiteralExpr.floatValue(Double.parseDouble(ctx.FLOAT().getText()));
        }
        if (ctx.DECIMAL() != null) {
            String text = ctx.DECIMAL().getText();
            text = text.substring(0, text.length() - 1);
            return LiteralExpr.floatValue(Double.parseDouble(text));
        }
        if (ctx.BOOLEAN() != null) {
            return LiteralExpr.bool(Boolean.parseBoolean(ctx.BOOLEAN().getText()));
        }
        if (ctx.DATE() != null) {
            return LiteralExpr.string(ctx.DATE().getText());
        }
        if (ctx.STRICTTIME() != null) {
            return LiteralExpr.string(ctx.STRICTTIME().getText());
        }
        throw new PureParseException("Unknown literal: " + ctx.getText());
    }

    @Override
    public PureExpression visitExpressionsArray(PureParser.ExpressionsArrayContext ctx) {
        List<PureExpression> elements = new ArrayList<>();
        for (PureParser.ExpressionContext expr : ctx.expression()) {
            elements.add(visit(expr));
        }
        return new ArrayLiteral(elements);
    }

    // ========================================
    // LAMBDA EXPRESSIONS
    // ========================================

    @Override
    public PureExpression visitAnyLambda(PureParser.AnyLambdaContext ctx) {
        if (ctx.lambdaFunction() != null) {
            return visit(ctx.lambdaFunction());
        }
        // IMPORTANT: Check lambdaParam BEFORE lambdaPipe!
        // The grammar is: anyLambda : lambdaPipe | lambdaFunction | lambdaParam
        // lambdaPipe
        // When matching "x | expr", BOTH lambdaParam() and lambdaPipe() are non-null
        // We must check lambdaParam first to correctly capture the parameter name
        if (ctx.lambdaParam() != null) {
            // x|expr - lambda with single param
            String param = getIdentifierText(ctx.lambdaParam().identifier());
            PureExpression body = visitLambdaBody(ctx.lambdaPipe());
            return new LambdaExpression(param, body);
        }
        if (ctx.lambdaPipe() != null) {
            // |expr - lambda with no params (use default "_")
            PureExpression body = visitLambdaBody(ctx.lambdaPipe());
            return new LambdaExpression("_", body);
        }
        throw new PureParseException("Unknown lambda: " + ctx.getText());
    }

    @Override
    public PureExpression visitLambdaFunction(PureParser.LambdaFunctionContext ctx) {
        // {x, y | body} - capture ALL params as comma-separated string for fold()
        // support
        List<PureParser.LambdaParamContext> params = ctx.lambdaParam();
        String param;
        if (params.isEmpty()) {
            param = "_";
        } else if (params.size() == 1) {
            param = getIdentifierText(params.get(0).identifier());
        } else {
            // Multiple parameters - join as comma-separated for fold() lambda
            param = params.stream()
                    .map(p -> getIdentifierText(p.identifier()))
                    .collect(java.util.stream.Collectors.joining(", "));
        }
        PureExpression body = visitLambdaBody(ctx.lambdaPipe());
        return new LambdaExpression(param, body);
    }

    private PureExpression visitLambdaBody(PureParser.LambdaPipeContext ctx) {
        return visit(ctx.codeBlock());
    }

    @Override
    public PureExpression visitCodeBlock(PureParser.CodeBlockContext ctx) {
        List<PureParser.ProgramLineContext> lines = ctx.programLine();
        if (lines.isEmpty()) {
            return LiteralExpr.string("");
        }
        if (lines.size() == 1) {
            return visit(lines.get(0));
        }
        PureExpression result = null;
        for (PureParser.ProgramLineContext line : lines) {
            result = visit(line);
        }
        return result;
    }

    @Override
    public PureExpression visitProgramLine(PureParser.ProgramLineContext ctx) {
        if (ctx.combinedExpression() != null) {
            return visit(ctx.combinedExpression());
        }
        if (ctx.letExpression() != null) {
            return visit(ctx.letExpression());
        }
        throw new PureParseException("Unknown program line: " + ctx.getText());
    }

    @Override
    public PureExpression visitLetExpression(PureParser.LetExpressionContext ctx) {
        String varName = getIdentifierText(ctx.identifier());
        PureExpression value = visit(ctx.combinedExpression());
        return new LetExpression(varName, value);
    }

    // ========================================
    // INSTANCE EXPRESSIONS (^Person(name='John'))
    // ========================================

    @Override
    public PureExpression visitExpressionInstance(PureParser.ExpressionInstanceContext ctx) {
        String className;
        if (ctx.qualifiedName() != null) {
            className = getQualifiedNameText(ctx.qualifiedName());
        } else if (ctx.variable() != null) {
            className = "$" + getIdentifierText(ctx.variable().identifier());
        } else {
            throw new PureParseException("Instance without class: " + ctx.getText());
        }

        Map<String, Object> properties = new LinkedHashMap<>();
        for (PureParser.ExpressionInstanceParserPropertyAssignmentContext prop : ctx
                .expressionInstanceParserPropertyAssignment()) {
            String propName = prop.identifier().stream()
                    .map(this::getIdentifierText)
                    .collect(Collectors.joining("."));
            PureExpression value = visit(prop.expressionInstanceRightSide());
            properties.put(propName, extractValue(value));
        }

        return new InstanceExpression(className, properties);
    }

    private Object extractValue(PureExpression expr) {
        if (expr instanceof LiteralExpr lit) {
            return lit.value();
        }
        return expr;
    }

    @Override
    public PureExpression visitExpressionInstanceRightSide(PureParser.ExpressionInstanceRightSideContext ctx) {
        return visit(ctx.expressionInstanceAtomicRightSide());
    }

    @Override
    public PureExpression visitExpressionInstanceAtomicRightSide(
            PureParser.ExpressionInstanceAtomicRightSideContext ctx) {
        if (ctx.combinedExpression() != null) {
            return visit(ctx.combinedExpression());
        }
        if (ctx.expressionInstance() != null) {
            return visit(ctx.expressionInstance());
        }
        if (ctx.qualifiedName() != null) {
            return new ClassReference(getQualifiedNameText(ctx.qualifiedName()));
        }
        throw new PureParseException("Unknown instance right side: " + ctx.getText());
    }

    // ========================================
    // COLUMN BUILDERS (~col or ~[col1, col2])
    // ========================================

    @Override
    public PureExpression visitColumnBuilders(PureParser.ColumnBuildersContext ctx) {
        if (ctx.oneColSpec() != null) {
            return visitOneColSpec(ctx.oneColSpec());
        }
        if (ctx.colSpecArray() != null) {
            return visitColSpecArray(ctx.colSpecArray());
        }
        throw new PureParseException("Unknown column builder: " + ctx.getText());
    }

    @Override
    public PureExpression visitOneColSpec(PureParser.OneColSpecContext ctx) {
        String colName = getIdentifierText(ctx.identifier());

        if (ctx.anyLambda() != null) {
            PureExpression lambda = visit(ctx.anyLambda());
            PureExpression extra = ctx.extraFunction() != null ? visit(ctx.extraFunction().anyLambda()) : null;
            return new ColumnSpec(colName, lambda, extra);
        }

        return new ColumnSpec(colName, null, null);
    }

    @Override
    public PureExpression visitColSpecArray(PureParser.ColSpecArrayContext ctx) {
        List<PureExpression> specs = new ArrayList<>();
        for (PureParser.OneColSpecContext col : ctx.oneColSpec()) {
            specs.add(visitOneColSpec(col));
        }
        return new ColumnSpecArray(specs);
    }

    // ========================================
    // FUNCTION CALL PARSING HELPERS
    // ========================================

    private List<PureExpression> parseFunctionArgs(PureParser.FunctionExpressionParametersContext ctx) {
        List<PureExpression> args = new ArrayList<>();
        if (ctx != null && ctx.combinedExpression() != null) {
            for (PureParser.CombinedExpressionContext expr : ctx.combinedExpression()) {
                args.add(visit(expr));
            }
        }
        return args;
    }

    // ========================================
    // SPECIFIC FUNCTION HANDLERS
    // ========================================

    private PureExpression parseFilterCall(PureExpression source, List<PureExpression> args) {
        if (args.isEmpty()) {
            throw new PureParseException("filter requires a lambda argument");
        }
        // Return based on source type
        if (source instanceof ClassExpression classExpr) {
            return new ClassFilterExpression(classExpr, (LambdaExpression) args.get(0));
        }
        if (source instanceof RelationExpression relExpr) {
            return new RelationFilterExpression(relExpr, (LambdaExpression) args.get(0));
        }
        // Generic filter
        return new FilterExpression(source, (LambdaExpression) args.get(0));
    }

    private PureExpression parseProjectCall(PureExpression source, List<PureExpression> args) {
        if (args.isEmpty()) {
            throw new PureParseException("project requires arguments");
        }

        List<LambdaExpression> columns = new ArrayList<>();
        List<String> aliases = new ArrayList<>();

        for (PureExpression arg : args) {
            if (arg instanceof ColumnSpecArray csa) {
                for (PureExpression spec : csa.specs()) {
                    if (spec instanceof ColumnSpec cs) {
                        if (cs.lambda() != null) {
                            columns.add((LambdaExpression) cs.lambda());
                        } else {
                            columns.add(new LambdaExpression("x",
                                    new PropertyAccessExpression(new VariableExpr("x"), cs.name())));
                        }
                        aliases.add(cs.name());
                    }
                }
            } else if (arg instanceof ColumnSpec cs) {
                if (cs.lambda() != null) {
                    columns.add((LambdaExpression) cs.lambda());
                } else {
                    columns.add(
                            new LambdaExpression("x", new PropertyAccessExpression(new VariableExpr("x"), cs.name())));
                }
                aliases.add(cs.name());
            } else if (arg instanceof LambdaExpression lambda) {
                columns.add(lambda);
            } else if (arg instanceof ArrayLiteral array) {
                // Handle array of lambdas: [{x | $x.prop1}, {x | $x.prop2}]
                // Or array of aliases: ['alias1', 'alias2']
                for (PureExpression element : array.elements()) {
                    if (element instanceof LambdaExpression lambda) {
                        columns.add(lambda);
                    } else if (element instanceof LiteralExpr literal) {
                        // String literal for alias
                        aliases.add(literal.value().toString());
                    }
                }
            }
        }

        if (source instanceof ClassExpression classExpr) {
            return new ProjectExpression(classExpr, columns, aliases);
        }
        // For RelationExpression, create appropriate type
        return new ProjectExpression((ClassExpression) source, columns, aliases);
    }

    private PureExpression parseGroupByCall(PureExpression source, List<PureExpression> args) {
        // Validate: groupBy cannot be called directly on Class.all() - must use
        // project() first
        if (source instanceof ClassExpression || source instanceof ClassAllExpression) {
            throw new PureParseException(
                    "groupBy() cannot be called directly on a Class. " +
                            "Use project() first to create a relation: Class.all()->project(...)->groupBy(...)");
        }

        // groupBy requires exactly 3 arguments: [groupByCols], [aggCols], [aliases]
        if (args.size() < 3) {
            throw new PureParseException(
                    "groupBy() requires 3 arguments: groupBy columns, aggregation columns, and aliases");
        }

        // Extract groupBy columns from first ArrayLiteral
        List<LambdaExpression> groupByColumns = extractLambdasFromArg(args.get(0), "groupBy columns");

        // Extract aggregation columns from second ArrayLiteral
        List<LambdaExpression> aggregations = extractLambdasFromArg(args.get(1), "aggregation columns");

        // Extract aliases from third ArrayLiteral
        List<String> aliases = extractStringsFromArg(args.get(2), "aliases");

        // Source must be a RelationExpression
        if (source instanceof RelationExpression relationSource) {
            return new GroupByExpression(relationSource, groupByColumns, aggregations, aliases);
        }

        // Fallback to MethodCall for other source types
        return new MethodCall(source, "groupBy", args);
    }

    private PureExpression parseJoinCall(PureExpression source, List<PureExpression> args) {
        // Validate: join cannot be called directly on Class.all() - must use project()
        // first
        if (source instanceof ClassExpression || source instanceof ClassAllExpression) {
            throw new PureParseException(
                    "join() cannot be called directly on a Class. " +
                            "Use project() first to create a relation: Class.all()->project(...)->join(...)");
        }

        // join requires exactly 3 arguments: rightRelation, joinType, condition
        if (args.size() < 3) {
            throw new PureParseException(
                    "join() requires 3 arguments: right relation, join type, and condition lambda");
        }

        // First arg: right relation (must be RelationExpression)
        PureExpression rightArg = args.get(0);
        if (!(rightArg instanceof RelationExpression)) {
            throw new PureParseException(
                    "join() first argument must be a Relation (use project() first), got: " +
                            rightArg.getClass().getSimpleName());
        }
        RelationExpression right = (RelationExpression) rightArg;

        // Second arg: join type (enum reference like JoinType.LEFT_OUTER)
        JoinExpression.JoinType joinType = extractJoinType(args.get(1));

        // Third arg: condition lambda
        PureExpression condArg = args.get(2);
        if (!(condArg instanceof LambdaExpression)) {
            throw new PureParseException(
                    "join() third argument must be a lambda condition, got: " +
                            condArg.getClass().getSimpleName());
        }
        LambdaExpression condition = (LambdaExpression) condArg;

        // Source must be a RelationExpression
        if (source instanceof RelationExpression leftRelation) {
            return new JoinExpression(leftRelation, right, joinType, condition);
        }

        // Fallback to MethodCall for other source types
        return new MethodCall(source, "join", args);
    }

    private JoinExpression.JoinType extractJoinType(PureExpression arg) {
        // Handle enum reference like JoinType.LEFT_OUTER or PropertyAccess
        if (arg instanceof PropertyAccessExpression propAccess) {
            return JoinExpression.JoinType.fromString(propAccess.propertyName());
        }
        if (arg instanceof ClassReference ref) {
            // Could be JoinType.INNER parsed as a class reference
            String name = ref.className();
            if (name.contains(".")) {
                return JoinExpression.JoinType.fromString(name.substring(name.lastIndexOf('.') + 1));
            }
            return JoinExpression.JoinType.fromString(name);
        }
        if (arg instanceof LiteralExpr lit && lit.value() instanceof String s) {
            return JoinExpression.JoinType.fromString(s);
        }
        throw new PureParseException(
                "join() second argument must be a JoinType (INNER, LEFT_OUTER, etc), got: " +
                        arg.getClass().getSimpleName());
    }

    private List<LambdaExpression> extractLambdasFromArg(PureExpression arg, String argName) {
        List<LambdaExpression> lambdas = new ArrayList<>();
        if (arg instanceof ArrayLiteral array) {
            for (PureExpression element : array.elements()) {
                if (element instanceof LambdaExpression lambda) {
                    lambdas.add(lambda);
                } else {
                    throw new PureParseException(
                            argName + " must contain lambda expressions, got: " + element.getClass().getSimpleName());
                }
            }
        } else if (arg instanceof LambdaExpression lambda) {
            lambdas.add(lambda);
        } else {
            throw new PureParseException(
                    argName + " must be an array of lambdas, got: " + arg.getClass().getSimpleName());
        }
        return lambdas;
    }

    private List<String> extractStringsFromArg(PureExpression arg, String argName) {
        List<String> strings = new ArrayList<>();
        if (arg instanceof ArrayLiteral array) {
            for (PureExpression element : array.elements()) {
                if (element instanceof LiteralExpr literal && literal.value() instanceof String s) {
                    strings.add(s);
                } else {
                    throw new PureParseException(argName + " must contain string literals");
                }
            }
        } else if (arg instanceof LiteralExpr literal && literal.value() instanceof String s) {
            strings.add(s);
        } else {
            throw new PureParseException(
                    argName + " must be an array of strings, got: " + arg.getClass().getSimpleName());
        }
        return strings;
    }

    private PureExpression parseSortByCall(PureExpression source, List<PureExpression> args) {
        if (args.isEmpty()) {
            throw new PureParseException("sortBy requires at least a lambda argument");
        }

        // First arg should be the lambda
        PureExpression firstArg = args.get(0);
        LambdaExpression lambda = null;
        if (firstArg instanceof LambdaExpression l) {
            lambda = l;
        } else {
            throw new PureParseException(
                    "sortBy first argument must be a lambda, got: " + firstArg.getClass().getSimpleName());
        }

        // Second arg (optional) is the direction: 'asc' or 'desc'
        boolean ascending = true;
        if (args.size() > 1) {
            PureExpression dirArg = args.get(1);
            if (dirArg instanceof LiteralExpr lit && lit.value() instanceof String dir) {
                ascending = !"desc".equalsIgnoreCase(dir);
            }
        }

        // Return ClassSortByExpression for class sources
        if (source instanceof ClassExpression classExpr) {
            return new ClassSortByExpression(classExpr, lambda, ascending);
        }

        // For other sources, use generic SortByExpression with just the lambda (not
        // direction string)
        return new SortByExpression(source, List.of(lambda));
    }

    private PureExpression parseSortCall(PureExpression source, List<PureExpression> args) {
        return new SortExpression(source, args);
    }

    private PureExpression parseLimitCall(PureExpression source, List<PureExpression> args) {
        if (args.isEmpty()) {
            throw new PureParseException("limit/take requires a count argument");
        }
        int count = extractInt(args.get(0));
        if (source instanceof ClassExpression classExpr) {
            return ClassLimitExpression.limit(classExpr, count);
        }
        if (source instanceof RelationExpression relExpr) {
            return RelationLimitExpression.limit(relExpr, count);
        }
        return new LimitExpression(source, count);
    }

    private PureExpression parseDropCall(PureExpression source, List<PureExpression> args) {
        if (args.isEmpty()) {
            throw new PureParseException("drop requires a count argument");
        }
        int count = extractInt(args.get(0));
        return new DropExpression(source, count);
    }

    private PureExpression parseSliceCall(PureExpression source, List<PureExpression> args) {
        if (args.size() < 2) {
            throw new PureParseException("slice requires start and end arguments");
        }
        int start = extractInt(args.get(0));
        int end = extractInt(args.get(1));
        return new SliceExpression(source, start, end);
    }

    private PureExpression parseSelectCall(PureExpression source, List<PureExpression> args) {
        if (!(source instanceof RelationExpression relExpr)) {
            throw new PureParseException(
                    "select() requires a Relation source, got: " + source.getClass().getSimpleName());
        }
        // Extract column names from args (ColumnSpec or ColumnSpecArray)
        List<String> columns = extractColumnsFromArgs(args);
        return new RelationSelectExpression(relExpr, columns);
    }

    private PureExpression parseExtendCall(PureExpression source, List<PureExpression> args) {
        // extend() is Relation-only
        if (!(source instanceof RelationExpression relExpr)) {
            throw new PureParseException(
                    "extend() requires a Relation source, got: " + source.getClass().getSimpleName());
        }

        // Check for simple ColumnSpec with lambda: ~col: x | expr
        if (args.size() == 1 && args.get(0) instanceof ColumnSpec cs
                && cs.lambda() instanceof LambdaExpression lambda) {
            return new RelationExtendExpression(relExpr, cs.name(), lambda);
        }

        // For window functions and other complex cases, keep using ExtendExpression
        // The compiler will handle it
        return new ExtendExpression(source, args);
    }

    /**
     * Extracts column names from select() arguments (ColumnSpec or
     * ColumnSpecArray).
     */
    private List<String> extractColumnsFromArgs(List<PureExpression> args) {
        List<String> columns = new ArrayList<>();
        for (PureExpression arg : args) {
            if (arg instanceof ColumnSpec cs) {
                columns.add(cs.name());
            } else if (arg instanceof ColumnSpecArray csa) {
                for (PureExpression spec : csa.specs()) {
                    if (spec instanceof ColumnSpec cs) {
                        columns.add(cs.name());
                    }
                }
            }
        }
        return columns;
    }

    private PureExpression parseFromCall(PureExpression source, List<PureExpression> args) {
        if (args.isEmpty()) {
            throw new PureParseException("from requires a mapping reference");
        }
        String mappingRef = extractString(args.get(0));
        return new FromExpression(source, mappingRef);
    }

    private PureExpression parseGraphFetchCall(PureExpression source, List<PureExpression> args) {
        // The argument should be a GraphFetchTree from island mode parsing (#{...}#)
        if (args.size() >= 1 && args.get(0) instanceof GraphFetchTree tree) {
            // Source should be a ClassExpression (e.g., Person.all())
            if (source instanceof ClassExpression classExpr) {
                return new GraphFetchExpression(classExpr, tree);
            }
        }
        // Fallback to MethodCall for unsupported patterns
        return new MethodCall(source, "graphFetch", args);
    }

    private PureExpression parseSerializeCall(PureExpression source, List<PureExpression> args) {
        // Validate: serialize() requires graphFetch() to precede it
        if (!(source instanceof GraphFetchExpression)) {
            throw new PureParseException(
                    "serialize() must be called on a graphFetch() expression. " +
                            "Use: Class.all()->graphFetch(#{...}#)->serialize(#{...}#)");
        }

        GraphFetchExpression graphFetch = (GraphFetchExpression) source;
        GraphFetchTree serializeTree = null;
        if (args.size() >= 1 && args.get(0) instanceof GraphFetchTree tree) {
            serializeTree = tree;
        }
        return new SerializeExpression(graphFetch, serializeTree);
    }

    private PureExpression parseUpdateCall(PureExpression source, List<PureExpression> args) {
        if (args.isEmpty()) {
            // Simple update without lambda
            return new MethodCall(source, "update", args);
        }
        return new UpdateExpression(source, (LambdaExpression) args.get(0));
    }

    // ========================================
    // COMBINED ARITHMETIC ONLY
    // ========================================

    @Override
    public PureExpression visitCombinedArithmeticOnly(PureParser.CombinedArithmeticOnlyContext ctx) {
        PureExpression result = visit(ctx.expression());
        for (PureParser.ArithmeticPartContext part : ctx.arithmeticPart()) {
            result = processArithmeticPart(result, part);
        }
        return result;
    }

    // ========================================
    // HELPER METHODS
    // ========================================

    private int extractInt(PureExpression expr) {
        if (expr instanceof LiteralExpr lit && lit.value() instanceof Number n) {
            return n.intValue();
        }
        if (expr instanceof IntegerLiteral intLit) {
            return intLit.value().intValue();
        }
        throw new PureParseException("Expected integer, got: " + expr);
    }

    private String extractString(PureExpression expr) {
        if (expr instanceof ClassReference ref) {
            return ref.className();
        }
        if (expr instanceof LiteralExpr lit) {
            return String.valueOf(lit.value());
        }
        if (expr instanceof StringLiteral strLit) {
            return strLit.value();
        }
        return expr.toString();
    }

    private String getQualifiedNameText(PureParser.QualifiedNameContext ctx) {
        if (ctx.packagePath() != null) {
            String pkg = ctx.packagePath().identifier().stream()
                    .map(this::getIdentifierText)
                    .collect(Collectors.joining("::"));
            return pkg + "::" + getIdentifierText(ctx.identifier());
        }
        return getIdentifierText(ctx.identifier());
    }

    private String getIdentifierText(PureParser.IdentifierContext ctx) {
        if (ctx.VALID_STRING() != null) {
            return ctx.VALID_STRING().getText();
        }
        if (ctx.STRING() != null) {
            return unquote(ctx.STRING().getText());
        }
        // Handle keywords used as identifiers
        return ctx.getText();
    }

    private String getUnitNameText(PureParser.UnitNameContext ctx) {
        return getQualifiedNameText(ctx.qualifiedName()) + "~" + getIdentifierText(ctx.identifier());
    }

    private String getTypeText(PureParser.TypeContext ctx) {
        if (ctx.qualifiedName() != null) {
            return getQualifiedNameText(ctx.qualifiedName());
        }
        return ctx.getText();
    }

    private String unquote(String s) {
        if (s == null || s.length() < 2)
            return s;
        char first = s.charAt(0);
        char last = s.charAt(s.length() - 1);
        if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }
}
