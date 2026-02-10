package org.finos.legend.pure.dsl.antlr;

import org.antlr.v4.runtime.ParserRuleContext;
import org.finos.legend.pure.dsl.*;
import org.finos.legend.pure.dsl.GraphFetchTree;

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

    // Registry of user-defined Pure functions for inlining
    private static final PureFunctionRegistry functionRegistry = PureFunctionRegistry.withBuiltins();

    // Current input source for extracting source text
    private String inputSource;

    /**
     * Set the input source for source text extraction.
     * Called before parsing to enable source text preservation.
     */
    public void setInputSource(String source) {
        this.inputSource = source;
    }

    /**
     * Extract the original source text from a parser rule context.
     */
    private String extractSourceText(ParserRuleContext ctx) {
        if (inputSource == null || ctx == null || ctx.getStart() == null || ctx.getStop() == null) {
            return null;
        }
        int start = ctx.getStart().getStartIndex();
        int stop = ctx.getStop().getStopIndex();
        if (start >= 0 && stop >= start && stop < inputSource.length()) {
            return inputSource.substring(start, stop + 1);
        }
        return null;
    }

    // ========================================
    // ENTRY POINT - Combined Expression
    // ========================================

    @Override
    public PureExpression visitCombinedExpression(PureParser.CombinedExpressionContext ctx) {
        // Start with the first expression
        PureExpression expr = visit(ctx.expression());

        // Process expression parts (booleanPart | arithmeticPart)
        for (PureParser.ExpressionPartContext part : ctx.expressionPart()) {
            expr = processExpressionPart(expr, part);
        }

        return expr;
    }

    private PureExpression processExpressionPart(PureExpression left, PureParser.ExpressionPartContext ctx) {
        if (ctx.booleanPart() != null) {
            return processBooleanPart(left, ctx.booleanPart());
        } else if (ctx.arithmeticPart() != null) {
            return processArithmeticPart(left, ctx.arithmeticPart());
        }
        return left;
    }

    private PureExpression processBooleanPart(PureExpression left, PureParser.BooleanPartContext bp) {
        // Use combinedArithmeticOnly to ensure comparisons are evaluated before boolean
        // ops
        PureExpression right = visitCombinedArithmeticOnly(bp.combinedArithmeticOnly());
        if (bp.AND() != null) {
            return LogicalExpr.and(left, right);
        } else {
            return LogicalExpr.or(left, right);
        }
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

        // Check if this is an enum value access: ClassReference.VALUE ->
        // EnumValueReference
        if (source instanceof ClassReference classRef) {
            // EnumType.VALUE pattern - return EnumValueReference
            return new EnumValueReference(classRef.className(), propName);
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

            // For registered user functions, we need the source text
            // Extract it from the full context up to this point
            String sourceText = null;
            List<String> argTexts = new ArrayList<>();

            String simpleName = funcName.contains("::")
                    ? funcName.substring(funcName.lastIndexOf("::") + 2)
                    : funcName;

            if (functionRegistry.hasFunction(funcName) || functionRegistry.hasFunction(simpleName)) {
                // Extract source text for the receiver (everything up to this arrow)
                // The source is defined as: ctx.start up to just before the arrow
                sourceText = extractSourceTextUpTo(ctx, i);

                // Extract source text for each argument
                if (params.get(i).combinedExpression() != null) {
                    for (var argCtx : params.get(i).combinedExpression()) {
                        String argText = extractSourceText(argCtx);
                        argTexts.add(argText != null ? argText : "");
                    }
                }
            }

            result = createFunctionCall(result, funcName, args, sourceText, argTexts);
        }

        return result;
    }

    /**
     * Extract source text for the receiver expression up to (but not including) the
     * i-th arrow function.
     * This is needed for user function inlining.
     */
    private String extractSourceTextUpTo(PureParser.FunctionExpressionContext ctx, int arrowIndex) {
        if (inputSource == null)
            return null;

        // Trace up to find the ExpressionContext (not just the immediate parent)
        // This ensures we get the full expression chain including "$x.payload->..."
        ParserRuleContext current = ctx.getParent();
        while (current != null && !(current instanceof PureParser.ExpressionContext)) {
            current = current.getParent();
        }

        if (current == null || current.getStart() == null)
            return null;

        int start = current.getStart().getStartIndex();

        // Get the position just before the arrow for this function call
        // The qualifiedName starts after the arrow
        var nameCtx = ctx.qualifiedName().get(arrowIndex);
        if (nameCtx == null || nameCtx.getStart() == null)
            return null;

        // Go back 2 for the "->" token
        int stop = nameCtx.getStart().getStartIndex() - 2;

        if (start >= 0 && stop > start && stop < inputSource.length()) {
            return inputSource.substring(start, stop).trim();
        }
        return null;
    }

    private PureExpression createFunctionCall(PureExpression source, String funcName, List<PureExpression> args,
            String sourceText, List<String> argTexts) {
        // Extract simple function name from fully qualified names
        // e.g., meta::pure::functions::relation::distinct -> distinct
        String simpleName = funcName.contains("::")
                ? funcName.substring(funcName.lastIndexOf("::") + 2)
                : funcName;

        // Check if this is a registered user function - create
        // UserFunctionCallExpression
        if (sourceText != null
                && (functionRegistry.hasFunction(funcName) || functionRegistry.hasFunction(simpleName))) {
            return new UserFunctionCallExpression(source, sourceText, simpleName, args, argTexts);
        }

        return switch (simpleName) {
            case "filter" -> parseFilterCall(source, args);
            case "project" -> parseProjectCall(source, args);
            case "groupBy" -> parseGroupByCall(source, args);
            case "join" -> parseJoinCall(source, args);
            case "sortBy" -> parseSortByCall(source, args);
            case "sort" -> parseSortCall(source, args);
            case "limit", "take" -> parseLimitCall(source, args);
            case "drop" -> parseDropCall(source, args);
            case "slice" -> parseSliceCall(source, args);
            case "first" -> {
                // Distinguish between collection first() and window first($w,$r)
                if (args.isEmpty()) {
                    yield new FirstExpression(source);
                } else {
                    // Window function: first($w, $r) - use standard MethodCall
                    yield new MethodCall(source, "first", args);
                }
            }
            case "select" -> parseSelectCall(source, args);
            case "write" -> parseWriteCall(source, args);
            case "extend" -> parseExtendCall(source, args);
            case "from" -> parseFromCall(source, args);
            case "graphFetch" -> parseGraphFetchCall(source, args);
            case "serialize" -> parseSerializeCall(source, args);
            case "save" -> new SaveExpression(source);
            case "update" -> parseUpdateCall(source, args);
            case "delete" -> new DeleteExpression(source);
            case "cast" -> parseCastCall(source, args);
            case "flatten" -> parseFlattenCall(source, args);
            case "distinct" -> parseDistinctCall(source, args);
            case "rename" -> parseRenameCall(source, args);
            case "concatenate" -> parseConcatenateCall(source, args);
            case "asOfJoin" -> parseAsOfJoinCall(source, args);
            case "aggregate" -> parseAggregateCall(source, args);
            case "pivot" -> parsePivotCall(source, args);
            default -> new MethodCall(source, simpleName, args);
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
        if (ctx.tdsLiteral() != null) {
            return visitTdsLiteral(ctx.tdsLiteral());
        }
        if (ctx.comparatorExpression() != null) {
            return visitComparatorExpression(ctx.comparatorExpression());
        }
        throw new PureParseException("Unknown atomic expression: " + ctx.getText());
    }

    /**
     * Parses TDS (Tabular Data Set) literals.
     * 
     * Syntax:
     * {@code
     * #TDS
     *   col1, col2
     *   val1, val2
     * #
     * }
     */
    public TdsLiteral visitTdsLiteral(PureParser.TdsLiteralContext ctx) {
        String raw = ctx.TDS_LITERAL().getText();

        // Strip #TDS prefix and # suffix
        String content = raw.substring(4, raw.length() - 1).trim();

        // Split into lines
        String[] lines = content.split("\\n");

        // First line is column definitions - columns can have type annotations like
        // payload:meta::pure::metamodel::variant::Variant
        // We parse both name and type
        List<TdsLiteral.TdsColumn> columns = new ArrayList<>();
        if (lines.length > 0) {
            for (String col : parseTdsCsvLine(lines[0])) {
                col = col.trim();
                // Parse name:Type annotation if present
                int colonIdx = col.indexOf(':');
                if (colonIdx > 0) {
                    String colName = col.substring(0, colonIdx).trim();
                    String colType = col.substring(colonIdx + 1).trim();
                    columns.add(TdsLiteral.TdsColumn.of(colName, colType));
                } else {
                    columns.add(TdsLiteral.TdsColumn.of(col));
                }
            }
        }

        // Remaining lines are data rows
        List<List<Object>> rows = new ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            String line = lines[i].trim();
            if (line.isEmpty())
                continue;

            List<String> values = parseTdsCsvLine(line);
            List<Object> row = new ArrayList<>();
            for (String val : values) {
                row.add(parseTdsValue(val.trim()));
            }
            rows.add(row);
        }

        return new TdsLiteral(columns, rows);
    }

    /**
     * Parse a CSV-style line respecting quoted strings.
     * Commas inside quotes are not treated as delimiters.
     * Handles: 1, "[1,2,3]", "hello, world" -> [1, "[1,2,3]", "hello, world"]
     */
    private List<String> parseTdsCsvLine(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                inQuotes = !inQuotes;
                current.append(c);
            } else if (c == ',' && !inQuotes) {
                result.add(current.toString().trim());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }

        // Add the last field
        result.add(current.toString().trim());

        return result;
    }

    /**
     * Parse a TDS cell value to appropriate type.
     */
    private Object parseTdsValue(String value) {
        if (value.isEmpty() || "null".equalsIgnoreCase(value)) {
            return null;
        }
        // Try Decimal/Number suffix (e.g., 21d, 41.0d, 101.0D)
        if ((value.endsWith("d") || value.endsWith("D")) && value.length() > 1) {
            try {
                return Double.parseDouble(value.substring(0, value.length() - 1));
            } catch (NumberFormatException e) {
                // Not a decimal number, fall through
            }
        }
        // Try integer
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            // Not an integer
        }
        // Try float
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            // Not a number
        }
        // Strip surrounding quotes from string values (e.g., "[1,2,3]" -> [1,2,3])
        // This is important for JSON/Variant values that need to be parsed correctly
        if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1);
        }
        // String value
        return value;
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

        // Collect island content tokens as text
        StringBuilder content = new StringBuilder();
        for (PureParser.DslExtensionContentContext contentCtx : ctx.dslExtensionContent()) {
            if (contentCtx.ISLAND_BRACE_OPEN() != null) {
                content.append("{");
            } else if (contentCtx.ISLAND_BRACE_CLOSE() != null) {
                content.append("}");
            } else {
                content.append(contentCtx.getText());
            }
        }
        String contentText = content.toString().trim();

        // Parse the main DSL content
        PureExpression result = switch (dslType) {
            case "" -> parseGraphFetchTree(contentText);
            case ">" -> parseRelationLiteral(contentText);
            default -> throw new PureParseException("Unknown DSL type: #" + dslType + "{");
        };

        // Handle arrow chains via ANTLR-parsed functionChainAfterArrow (from
        // ISLAND_ARROW_EXIT path)
        if (ctx.functionChainAfterArrow() != null) {
            result = processFunctionChainAfterArrow(result, ctx.functionChainAfterArrow());
        }

        return result;
    }

    /**
     * Processes a function chain that starts after the arrow is already consumed.
     * The grammar rule is: qualifiedName functionExpressionParameters (ARROW
     * qualifiedName functionExpressionParameters)*
     */
    private PureExpression processFunctionChainAfterArrow(PureExpression source,
            PureParser.FunctionChainAfterArrowContext ctx) {
        PureExpression current = source;

        // Process all function calls in the chain
        var qualifiedNames = ctx.qualifiedName();
        var params = ctx.functionExpressionParameters();

        for (int i = 0; i < qualifiedNames.size(); i++) {
            String funcName = qualifiedNames.get(i).getText();
            List<PureExpression> args = new ArrayList<>();
            if (i < params.size()) {
                for (var argCtx : params.get(i).combinedExpression()) {
                    args.add(visit(argCtx));
                }
            }
            current = createFunctionCall(current, funcName, args, null, List.of());
        }

        return current;
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
                // Extract just the simple function name (last segment) from qualified names
                // e.g., meta::pure::functions::math::exp -> exp
                String simpleName = name.contains("::") ? name.substring(name.lastIndexOf("::") + 2) : name;
                // Check if this is a registered user function (standalone call, no receiver)
                if (functionRegistry.hasFunction(name) || functionRegistry.hasFunction(simpleName)) {
                    // Extract argument source texts
                    List<String> argTexts = new ArrayList<>();
                    if (aof.functionExpressionParameters().combinedExpression() != null) {
                        for (var argCtx : aof.functionExpressionParameters().combinedExpression()) {
                            String argText = extractSourceText(argCtx);
                            argTexts.add(argText != null ? argText : "");
                        }
                    }
                    return new UserFunctionCallExpression(null, null, simpleName, args, argTexts);
                }
                return new FunctionCall(simpleName, args);
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
            String text = ctx.INTEGER().getText();
            try {
                return LiteralExpr.integer(Long.parseLong(text));
            } catch (NumberFormatException e) {
                // Exceeds Long range (e.g., 9223372036854775898) — use BigInteger
                return LiteralExpr.integer(new java.math.BigInteger(text));
            }
        }
        if (ctx.FLOAT() != null) {
            String text = ctx.FLOAT().getText();
            java.math.BigDecimal bd = new java.math.BigDecimal(text);
            double d = bd.doubleValue();
            // Use BigDecimal only when double representation loses precision
            if (bd.compareTo(java.math.BigDecimal.valueOf(d)) != 0) {
                return LiteralExpr.floatValue(bd);
            }
            return LiteralExpr.floatValue(d);
        }
        if (ctx.DECIMAL() != null) {
            String text = ctx.DECIMAL().getText();
            text = text.substring(0, text.length() - 1);
            return LiteralExpr.decimalValue(new java.math.BigDecimal(text));
        }
        if (ctx.BOOLEAN() != null) {
            return LiteralExpr.bool(Boolean.parseBoolean(ctx.BOOLEAN().getText()));
        }
        if (ctx.DATE() != null) {
            return LiteralExpr.date(ctx.DATE().getText());
        }
        if (ctx.STRICTTIME() != null) {
            return LiteralExpr.strictTime(ctx.STRICTTIME().getText());
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
    public PureExpression visitComparatorExpression(PureParser.ComparatorExpressionContext ctx) {
        // comparator(a: Type[1], b: Type[1]): Boolean[1] { body } -> LambdaExpression({a, b | body})
        List<String> paramList = ctx.functionVariableExpression().stream()
                .map(fve -> getIdentifierText(fve.identifier()))
                .toList();
        PureExpression body = visit(ctx.codeBlock());
        return new LambdaExpression(paramList, body);
    }

    @Override
    public PureExpression visitLambdaFunction(PureParser.LambdaFunctionContext ctx) {
        // {x, y | body} - capture ALL params as List<String> for multi-param lambdas
        List<PureParser.LambdaParamContext> params = ctx.lambdaParam();
        List<String> paramList;
        if (params.isEmpty()) {
            paramList = List.of("_");
        } else {
            paramList = params.stream()
                    .map(p -> getIdentifierText(p.identifier()))
                    .toList();
        }
        PureExpression body = visitLambdaBody(ctx.lambdaPipe());
        return new LambdaExpression(paramList, body);
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

        // Multiple statements - collect let statements and final result
        List<LetExpression> letStatements = new java.util.ArrayList<>();
        PureExpression result = null;

        for (int i = 0; i < lines.size(); i++) {
            PureExpression expr = visit(lines.get(i));
            if (expr instanceof LetExpression let) {
                letStatements.add(let);
            } else if (i == lines.size() - 1) {
                // Last expression is the result
                result = expr;
            }
            // Intermediate non-let expressions are executed for side effects (rare)
        }

        if (result == null) {
            // If the last statement was a let, return the last let value
            if (!letStatements.isEmpty()) {
                LetExpression lastLet = letStatements.get(letStatements.size() - 1);
                result = new VariableExpr(lastLet.variableName());
            } else {
                result = LiteralExpr.string("");
            }
        }

        if (letStatements.isEmpty()) {
            return result;
        }

        return new BlockExpression(letStatements, result);
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
        // Support CastExpression - unwrap and treat inner pivot/relation as source
        if (source instanceof CastExpression cast && cast.source() instanceof RelationExpression castSource) {
            return new RelationFilterExpression(castSource, (LambdaExpression) args.get(0));
        }
        // VariableExpr as source (from let bindings like $a->filter(...))
        // Treat as relation filter - the variable will be resolved at compile time
        if (source instanceof VariableExpr) {
            return new RelationFilterExpression(source, (LambdaExpression) args.get(0));
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
        // For RelationExpression (TDS, etc.), create RelationProjectExpression
        return new RelationProjectExpression(source, columns, aliases);
    }

    private PureExpression parseGroupByCall(PureExpression source, List<PureExpression> args) {
        // Validate: groupBy cannot be called directly on Class.all() - must use
        // project() first
        if (source instanceof ClassExpression || source instanceof ClassAllExpression) {
            throw new PureParseException(
                    "groupBy() cannot be called directly on a Class. " +
                            "Use project() first to create a relation: Class.all()->project(...)->groupBy(...)");
        }

        // Support two syntaxes:
        // 1. Relation API syntax: groupBy(~groupCol, ~aggSpec) - 2 args with ColumnSpec
        // 2. Legacy syntax: groupBy([lambdas], [aggLambdas], [aliases]) - 3 args with
        // arrays

        // Check for Relation API syntax (ColumnSpec or ColumnSpecArray args)
        if (args.size() >= 2 && isColumnSpecArg(args.get(0))) {
            return parseRelationApiGroupBy(source, args);
        }

        // Legacy 3-arg syntax: [groupByCols], [aggCols], [aliases]
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

        // Source must be a RelationExpression or CastExpression (for cast to Relation)
        if (source instanceof RelationExpression relationSource) {
            return new GroupByExpression(relationSource, groupByColumns, aggregations, aliases);
        }
        // Support CastExpression - unwrap and treat inner pivot/relation as source
        if (source instanceof CastExpression cast && cast.source() instanceof RelationExpression castSource) {
            return new GroupByExpression(castSource, groupByColumns, aggregations, aliases);
        }

        // Fallback to MethodCall for other source types
        return new MethodCall(source, "groupBy", args);
    }

    /**
     * Builds an aggregation lambda from a ColumnSpec with potentially two lambdas.
     * 
     * Pattern: ~col:x|$x.name:y|$y->joinStrings('')
     * - First lambda (cs.lambda): extracts column ($x.name)
     * - Second lambda (cs.extraFunction): applies aggregate ($y->joinStrings(''))
     * 
     * We merge these into a single lambda: r | $r.name->joinStrings('')
     * which the compiler expects.
     */
    private LambdaExpression buildAggregationLambda(ColumnSpec cs) {
        LambdaExpression mapLambda = cs.lambda() instanceof LambdaExpression le ? le : null;
        LambdaExpression aggLambda = cs.extraFunction() instanceof LambdaExpression le ? le : null;

        if (mapLambda != null && aggLambda != null) {
            // Two-lambda pattern: merge them
            // Extract the property access from mapLambda body (e.g., $x.name)
            PureExpression columnAccess = mapLambda.body();

            // Get the aggregate function from aggLambda body (e.g., $y->joinStrings(''))
            if (aggLambda.body() instanceof MethodCall aggCall) {
                // Replace the variable reference in aggCall.source() with the column access
                // The aggCall.source() is $y, we replace it with the actual property access
                // Create: $x.name->joinStrings('') style expression wrapped in lambda
                PureExpression mergedBody = new MethodCall(columnAccess, aggCall.methodName(), aggCall.arguments());
                return new LambdaExpression(mapLambda.parameter(), mergedBody);
            }
            // Fallback: just use the map lambda if agg pattern doesn't match
            return mapLambda;
        } else if (mapLambda != null) {
            return mapLambda;
        } else if (aggLambda != null) {
            return aggLambda;
        } else {
            throw new PureParseException("ColumnSpec has no lambda for aggregation: " + cs.name());
        }
    }

    /**
     * Parse Relation API groupBy syntax: groupBy(~groupCol, ~aggSpec)
     * The aggSpec is a ColumnSpec with aggregation lambda
     */
    private PureExpression parseRelationApiGroupBy(PureExpression source, List<PureExpression> args) {
        List<String> groupByColumnNames = new java.util.ArrayList<>();
        List<LambdaExpression> aggregations = new java.util.ArrayList<>();
        List<String> aliases = new java.util.ArrayList<>();

        // First arg: group-by column(s) - can be ColumnSpec or ColumnSpecArray
        PureExpression groupByArg = args.get(0);
        if (groupByArg instanceof ColumnSpec cs) {
            groupByColumnNames.add(cs.name());
        } else if (groupByArg instanceof ColumnSpecArray csa) {
            for (PureExpression spec : csa.specs()) {
                if (spec instanceof ColumnSpec cs) {
                    groupByColumnNames.add(cs.name());
                }
            }
        }

        // Second and subsequent args: aggregation column specs
        for (int i = 1; i < args.size(); i++) {
            PureExpression aggArg = args.get(i);
            if (aggArg instanceof ColumnSpec cs) {
                aliases.add(cs.name());
                aggregations.add(buildAggregationLambda(cs));
            } else if (aggArg instanceof ColumnSpecArray csa) {
                for (PureExpression spec : csa.specs()) {
                    if (spec instanceof ColumnSpec cs) {
                        aliases.add(cs.name());
                        aggregations.add(buildAggregationLambda(cs));
                    }
                }
            }
        }

        // Convert group-by column names to lambdas
        List<LambdaExpression> groupByLambdas = new java.util.ArrayList<>();
        for (String colName : groupByColumnNames) {
            // Create property access lambda: r | $r.colName
            String paramName = "r";
            PropertyAccessExpression propAccess = new PropertyAccessExpression(
                    new VariableExpr(paramName), colName);
            groupByLambdas.add(new LambdaExpression(paramName, propAccess));
        }

        // Also add the group-by column names to aliases (they appear in output)
        List<String> fullAliases = new java.util.ArrayList<>(groupByColumnNames);
        fullAliases.addAll(aliases);

        // Source can be RelationExpression, VariableExpr, or CastExpression (for cast
        // to Relation)
        if (source instanceof RelationExpression || source instanceof VariableExpr) {
            return new GroupByExpression(source, groupByLambdas, aggregations, fullAliases);
        }
        // Support CastExpression - unwrap and treat inner pivot/relation as source
        if (source instanceof CastExpression cast && cast.source() instanceof RelationExpression castSource) {
            return new GroupByExpression(castSource, groupByLambdas, aggregations, fullAliases);
        }

        // Fallback to MethodCall for other source types
        return new MethodCall(source, "groupBy", args);
    }

    /**
     * Parse aggregate() function - groupBy with no grouping columns.
     * 
     * Syntax: aggregate(~aggSpec) or aggregate(~[aggSpec1, aggSpec2])
     * 
     * AggColSpec: ~name : mapFn : aggFn
     * - mapFn extracts value from row (x | $x.id)
     * - aggFn aggregates values (y | $y->plus())
     * 
     * This aggregates the entire relation into a single row.
     */
    private PureExpression parseAggregateCall(PureExpression source, List<PureExpression> args) {
        if (args.isEmpty()) {
            throw new PureParseException("aggregate() requires at least one aggregation specification");
        }

        List<LambdaExpression> mapFunctions = new java.util.ArrayList<>();
        List<LambdaExpression> aggFunctions = new java.util.ArrayList<>();
        List<String> aliases = new java.util.ArrayList<>();

        // All args are aggregation specs - can be ColumnSpec or ColumnSpecArray
        for (PureExpression arg : args) {
            if (arg instanceof ColumnSpec cs) {
                aliases.add(cs.name());
                // lambda is the map function, extraFunction is the agg function
                if (cs.lambda() instanceof LambdaExpression mapLe) {
                    mapFunctions.add(mapLe);
                }
                if (cs.extraFunction() instanceof LambdaExpression aggLe) {
                    aggFunctions.add(aggLe);
                } else {
                    // No agg function, default to SUM - use map function
                    if (cs.lambda() instanceof LambdaExpression le) {
                        aggFunctions.add(le);
                    }
                }
            } else if (arg instanceof ColumnSpecArray csa) {
                for (PureExpression spec : csa.specs()) {
                    if (spec instanceof ColumnSpec cs) {
                        aliases.add(cs.name());
                        if (cs.lambda() instanceof LambdaExpression mapLe) {
                            mapFunctions.add(mapLe);
                        }
                        if (cs.extraFunction() instanceof LambdaExpression aggLe) {
                            aggFunctions.add(aggLe);
                        } else {
                            if (cs.lambda() instanceof LambdaExpression le) {
                                aggFunctions.add(le);
                            }
                        }
                    }
                }
            }
        }

        // Use AggregateExpression (no grouping columns)
        if (source instanceof RelationExpression relExpr) {
            return new AggregateExpression(relExpr, mapFunctions, aggFunctions, aliases);
        }

        return new MethodCall(source, "aggregate", args);
    }

    /**
     * Parse pivot() call.
     * 
     * Syntax: pivot(~[pivotCols], ~[aggName : x | $x.col : y | $y->plus()])
     * 
     * First arg: pivot column(s) as ColumnSpec/ColumnSpecArray
     * Second arg: aggregate spec(s) as ColumnSpec/ColumnSpecArray
     */
    private PureExpression parsePivotCall(PureExpression source, List<PureExpression> args) {
        if (args.size() < 2) {
            throw new PureParseException("pivot() requires at least 2 arguments: pivot columns and aggregates");
        }

        // Extract pivot column names from first arg
        List<String> pivotColumns = new ArrayList<>();
        PureExpression pivotColArg = args.get(0);
        if (pivotColArg instanceof ColumnSpec cs) {
            pivotColumns.add(cs.name());
        } else if (pivotColArg instanceof ColumnSpecArray csa) {
            for (PureExpression spec : csa.specs()) {
                if (spec instanceof ColumnSpec cs) {
                    pivotColumns.add(cs.name());
                } else if (spec instanceof StringLiteral sl) {
                    pivotColumns.add(sl.value());
                }
            }
        } else {
            throw new PureParseException(
                    "pivot() first arg must be column spec(s), got: " + pivotColArg.getClass().getSimpleName());
        }

        // Extract aggregate specs from second arg
        List<PivotExpression.AggregateSpec> aggregates = new ArrayList<>();
        PureExpression aggArg = args.get(1);
        if (aggArg instanceof ColumnSpec cs) {
            aggregates.add(extractPivotAggSpec(cs));
        } else if (aggArg instanceof ColumnSpecArray csa) {
            for (PureExpression spec : csa.specs()) {
                if (spec instanceof ColumnSpec cs) {
                    aggregates.add(extractPivotAggSpec(cs));
                }
            }
        } else {
            throw new PureParseException(
                    "pivot() second arg must be aggregate spec(s), got: " + aggArg.getClass().getSimpleName());
        }

        // Check for static values (optional third arg)
        List<Object> staticValues = new ArrayList<>();
        if (args.size() >= 3) {
            PureExpression valuesArg = args.get(2);
            if (valuesArg instanceof ArrayLiteral al) {
                for (PureExpression elem : al.elements()) {
                    if (elem instanceof StringLiteral sl) {
                        staticValues.add(sl.value());
                    } else if (elem instanceof IntegerLiteral il) {
                        staticValues.add(il.value());
                    } else if (elem instanceof LiteralExpr le) {
                        staticValues.add(le.value());
                    }
                }
            }
        }

        if (staticValues.isEmpty()) {
            return PivotExpression.dynamic(source, pivotColumns, aggregates);
        } else {
            return PivotExpression.withValues(source, pivotColumns, staticValues, aggregates);
        }
    }

    /**
     * Extract aggregate spec from ColumnSpec for pivot.
     * 
     * Format: ~aggName : x | $x.col : y | $y->aggFunc()
     * 
     * Handles both:
     * - Simple column references: $x.treePlanted
     * - Computed expressions: 1, $x.a * $x.b
     */
    private PivotExpression.AggregateSpec extractPivotAggSpec(ColumnSpec cs) {
        String name = cs.name();
        String valueColumn = null;
        String valueExpression = null;
        String aggFunction = "sum"; // default

        // The lambda body should give us the value column or expression
        if (cs.lambda() instanceof LambdaExpression mapLambda) {
            PureExpression body = mapLambda.body();
            // Try to extract a column name first
            valueColumn = tryExtractColumnFromExpression(body);
            if (valueColumn == null) {
                // For computed expressions (literals, binary ops), convert to expression string
                valueExpression = expressionToSqlString(body);
            }
        }

        // The extra function should give us the aggregate function
        if (cs.extraFunction() instanceof LambdaExpression aggLambda) {
            // Second lambda: y | $y->plus() - extracts the aggregate
            PureExpression body = aggLambda.body();
            if (body instanceof MethodCall mc) {
                aggFunction = mapAggFunctionName(mc.methodName());
            } else if (body instanceof FunctionCall fc) {
                aggFunction = mapAggFunctionName(fc.functionName());
            }
        }

        if (valueColumn != null) {
            return PivotExpression.AggregateSpec.column(name, valueColumn, aggFunction);
        } else if (valueExpression != null) {
            return PivotExpression.AggregateSpec.expression(name, valueExpression, aggFunction);
        } else {
            // Fallback to name
            return PivotExpression.AggregateSpec.column(name, name, aggFunction);
        }
    }

    /**
     * Try to extract a column name from expression. Returns null if not a simple
     * column reference.
     */
    private String tryExtractColumnFromExpression(PureExpression expr) {
        try {
            return extractColumnFromExpression(expr);
        } catch (PureParseException e) {
            return null;
        }
    }

    /**
     * Convert a Pure expression to SQL expression string for use in aggregations.
     * Handles literals and binary expressions.
     */
    private String expressionToSqlString(PureExpression expr) {
        if (expr instanceof LiteralExpr lit) {
            Object value = lit.value();
            if (value instanceof String s) {
                return "'" + s.replace("'", "''") + "'";
            }
            return String.valueOf(value);
        } else if (expr instanceof IntegerLiteral lit) {
            return lit.value().toString();
        } else if (expr instanceof PropertyAccessExpression pae) {
            return "\"" + pae.propertyName() + "\"";
        } else if (expr instanceof PropertyAccess pa) {
            return "\"" + pa.propertyName() + "\"";
        } else if (expr instanceof BinaryExpression bin) {
            String left = expressionToSqlString(bin.left());
            String right = expressionToSqlString(bin.right());
            return "(" + left + " " + bin.operator() + " " + right + ")";
        } else if (expr instanceof MethodCall mc) {
            if ("toOne".equals(mc.methodName())) {
                // toOne() just unwraps - return the source
                return expressionToSqlString(mc.source());
            }
            return expressionToSqlString(mc.source());
        } else if (expr instanceof FunctionCall fc) {
            // Handle meta::pure::functions::multiplicity::toOne and similar
            if (fc.functionName().endsWith("toOne") && !fc.arguments().isEmpty()) {
                return expressionToSqlString(fc.arguments().get(0));
            }
        }
        return "1"; // Default fallback for count-like expressions
    }

    /**
     * Map Pure aggregate function name to SQL aggregate function name.
     */
    private String mapAggFunctionName(String pureName) {
        return switch (pureName) {
            case "plus" -> "sum";
            case "count" -> "count";
            case "average", "avg", "mean" -> "avg";
            case "min" -> "min";
            case "max" -> "max";
            case "stdDev", "stdDevSample" -> "stddev_samp";
            case "stdDevPopulation" -> "stddev_pop";
            default -> pureName.toLowerCase();
        };
    }

    /**
     * Check if an argument is a ColumnSpec-style argument (for Relation API syntax)
     */
    private boolean isColumnSpecArg(PureExpression arg) {
        return arg instanceof ColumnSpec || arg instanceof ColumnSpecArray;
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

        // First arg: right relation (must be RelationExpression or VariableExpr for
        // lambda contexts)
        PureExpression rightArg = args.get(0);
        if (!(rightArg instanceof RelationExpression) && !(rightArg instanceof VariableExpr)) {
            throw new PureParseException(
                    "join() first argument must be a Relation (use project() first), got: " +
                            rightArg.getClass().getSimpleName());
        }

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

        // Source can be RelationExpression or VariableExpr (for lambda contexts)
        if (source instanceof RelationExpression || source instanceof VariableExpr) {
            return new JoinExpression(source, rightArg, joinType, condition);
        }

        // Fallback to MethodCall for other source types
        return new MethodCall(source, "join", args);
    }

    /**
     * Parses asOfJoin(rightRelation, matchCondition) or
     * asOfJoin(rightRelation, matchCondition, keyCondition)
     * 
     * asOfJoin is a temporal join that finds the closest matching row from the
     * right
     * based on the match condition (typically time comparison).
     */
    private PureExpression parseAsOfJoinCall(PureExpression source, List<PureExpression> args) {
        // asOfJoin requires 2 or 3 arguments
        if (args.size() < 2 || args.size() > 3) {
            throw new PureParseException(
                    "asOfJoin() requires 2 or 3 arguments: right relation, match condition, [optional key condition]");
        }

        // First arg: right relation (TdsLiteral, RelationExpression, or VariableExpr)
        PureExpression rightArg = args.get(0);
        if (!(rightArg instanceof RelationExpression) && !(rightArg instanceof VariableExpr)) {
            throw new PureParseException(
                    "asOfJoin() first argument must be a Relation, got: " +
                            rightArg.getClass().getSimpleName());
        }

        // Second arg: match condition lambda {x,y | $x.time > $y.time}
        PureExpression matchArg = args.get(1);
        if (!(matchArg instanceof LambdaExpression)) {
            throw new PureParseException(
                    "asOfJoin() second argument must be a lambda condition, got: " +
                            matchArg.getClass().getSimpleName());
        }
        LambdaExpression matchCondition = (LambdaExpression) matchArg;

        // Optional third arg: key condition lambda {x,y | $x.key == $y.key}
        LambdaExpression keyCondition = null;
        if (args.size() == 3) {
            PureExpression keyArg = args.get(2);
            if (!(keyArg instanceof LambdaExpression)) {
                throw new PureParseException(
                        "asOfJoin() third argument must be a lambda condition, got: " +
                                keyArg.getClass().getSimpleName());
            }
            keyCondition = (LambdaExpression) keyArg;
        }

        return new AsOfJoinExpression(source, rightArg, matchCondition, keyCondition);
    }

    private JoinExpression.JoinType extractJoinType(PureExpression arg) {
        // EnumValueReference from proper parsing: JoinType.LEFT_OUTER ->
        // EnumValueReference(JoinType, LEFT_OUTER)
        if (arg instanceof EnumValueReference enumRef) {
            return JoinExpression.JoinType.fromString(enumRef.valueName());
        }
        // Handle enum reference like JoinType.LEFT_OUTER or PropertyAccess - legacy
        // fallback
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
        // Flatten ArrayLiteral if passed: sort([~a->asc(), ~b->desc()]) ->
        // sort(~a->asc(), ~b->desc())
        List<PureExpression> flatArgs = new ArrayList<>();
        for (PureExpression arg : args) {
            if (arg instanceof ArrayLiteral arr) {
                flatArgs.addAll(arr.elements());
            } else {
                flatArgs.add(arg);
            }
        }
        return new SortExpression(source, flatArgs);
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
        // Accept RelationExpression or VariableExpr (for lambda parameters like
        // $t->select())
        // Type validation happens at compile time
        if (!(source instanceof RelationExpression) && !(source instanceof VariableExpr)
                && !(source instanceof CastExpression)) {
            throw new PureParseException(
                    "select() requires a Relation source, got: " + source.getClass().getSimpleName());
        }
        // Extract column names from args (ColumnSpec or ColumnSpecArray)
        List<String> columns = extractColumnsFromArgs(args);
        return new RelationSelectExpression(source, columns);
    }

    private PureExpression parseWriteCall(PureExpression source, List<PureExpression> args) {
        // write(accessor) - writes source relation to accessor, returns row count
        if (args.size() != 1) {
            throw new PureParseException("write() requires exactly 1 argument (accessor)");
        }
        return new RelationWriteExpression(source, args.get(0));
    }

    private PureExpression parseExtendCall(PureExpression source, List<PureExpression> args) {
        // Accept RelationExpression or VariableExpr
        if (!(source instanceof RelationExpression) && !(source instanceof VariableExpr)
                && !(source instanceof CastExpression)) {
            throw new PureParseException(
                    "extend() requires a Relation source, got: " + source.getClass().getSimpleName());
        }

        // Simple calculated column: extend(~col: x | expr) with no window
        if (args.size() == 1 && args.get(0) instanceof ColumnSpec cs
                && cs.lambda() instanceof LambdaExpression lambda
                && cs.extraFunction() == null) {
            return new RelationExtendExpression(source, cs.name(), lambda);
        }

        // Multiple calculated columns: extend(~[col1:{x|...}, col2:{x|...}])
        // Each column may be simple (no extraFunction) or OLAP aggregate (with
        // extraFunction)
        if (args.size() == 1 && args.get(0) instanceof ColumnSpecArray csa) {
            // Create chain of extend expressions for each column
            PureExpression result = source;
            for (PureExpression spec : csa.specs()) {
                if (spec instanceof ColumnSpec cs) {
                    // Check if this is an OLAP aggregate pattern (has extraFunction)
                    if (cs.lambda() != null && cs.extraFunction() != null) {
                        // OLAP aggregate: ~col:c|$c.id:y|$y->plus()
                        result = parseAggregateExtendNoOver(result, cs);
                    } else if (cs.lambda() instanceof LambdaExpression lambda) {
                        // Simple calculated column: ~col:x|$x.col1 + $x.col2
                        result = new RelationExtendExpression(result, cs.name(), lambda);
                    }
                }
            }
            return result;
        }

        // Window function pattern 1: extend(over(...), ~col:{p,w,r|...})
        if (!args.isEmpty() && isOverCall(args.get(0))) {
            return parseTypedWindowExtend(source, args);
        }

        // Window function pattern 2: extend(~col : func()->over(...)) where over is in
        // extraFunction
        if (args.size() == 1 && args.get(0) instanceof ColumnSpec cs && isOverCall(cs.extraFunction())) {
            return parseTypedWindowExtendFromChain(source, cs);
        }

        // Window function pattern 3: extend(~col : row_number()->over()) where lambda
        // is MethodCall
        if (args.size() == 1 && args.get(0) instanceof ColumnSpec cs && isOverCall(cs.lambda())) {
            return parseTypedWindowExtendFromLambda(source, cs);
        }

        // Aggregate extend pattern (no explicit over):
        // extend(~col:c|$c.id:y|$y->plus())
        // This is an aggregate function applied to the entire relation (implicit
        // over())
        if (args.size() == 1 && args.get(0) instanceof ColumnSpec cs
                && cs.lambda() != null && cs.extraFunction() != null) {
            return parseAggregateExtendNoOver(source, cs);
        }

        throw new PureParseException("extend() requires a simple column spec or window function pattern. Got: " + args);
    }

    // ===================== TYPED WINDOW PARSING =====================

    private boolean isOverCall(PureExpression expr) {
        return (expr instanceof FunctionCall fc && "over".equals(fc.functionName())) ||
                (expr instanceof MethodCall mc && "over".equals(mc.methodName()));
    }

    /**
     * Parses: extend(over(...), ~col:{p,w,r|...})
     */
    private PureExpression parseTypedWindowExtend(PureExpression source, List<PureExpression> args) {
        PureExpression overExpr = args.get(0);
        WindowContext ctx = parseWindowContext(overExpr);

        if (args.size() < 2) {
            throw new PureParseException("extend(over(...)) requires column specification");
        }

        PureExpression colArg = args.get(1);

        // Single column spec
        if (colArg instanceof ColumnSpec cs) {
            WindowFunctionSpec funcSpec = parseWindowFunctionFromColumnSpec(cs);
            RelationExtendExpression.TypedWindowSpec typedSpec = RelationExtendExpression.TypedWindowSpec.of(
                    funcSpec, ctx.partitionCols, ctx.orderSpecs, ctx.frame);
            return RelationExtendExpression.window(source, cs.name(), typedSpec);
        }

        // Multiple column specs in array: ~[col1:..., col2:...]
        if (colArg instanceof ColumnSpecArray csa) {
            PureExpression result = source;
            for (PureExpression spec : csa.specs()) {
                if (spec instanceof ColumnSpec cs) {
                    WindowFunctionSpec funcSpec = parseWindowFunctionFromColumnSpec(cs);
                    RelationExtendExpression.TypedWindowSpec typedSpec = RelationExtendExpression.TypedWindowSpec.of(
                            funcSpec, ctx.partitionCols, ctx.orderSpecs, ctx.frame);
                    result = RelationExtendExpression.window(result, cs.name(), typedSpec);
                }
            }
            return result;
        }

        throw new PureParseException(
                "extend(over(...)) requires a ColumnSpec or ColumnSpecArray, got: " + colArg.getClass());
    }

    /**
     * Parses: extend(~col : row_number()->over(...))
     */
    private PureExpression parseTypedWindowExtendFromChain(PureExpression source, ColumnSpec cs) {
        MethodCall overCall = (MethodCall) cs.extraFunction();
        WindowContext ctx = parseWindowContext(overCall);
        WindowFunctionSpec funcSpec = parseWindowFunctionFromExpr(overCall.source());

        RelationExtendExpression.TypedWindowSpec typedSpec = RelationExtendExpression.TypedWindowSpec.of(
                funcSpec, ctx.partitionCols, ctx.orderSpecs, ctx.frame);

        return RelationExtendExpression.window(source, cs.name(), typedSpec);
    }

    /**
     * Parses: extend(~col : row_number()->over()) where cs.lambda() is a MethodCall
     * to over()
     */
    private PureExpression parseTypedWindowExtendFromLambda(PureExpression source, ColumnSpec cs) {
        MethodCall overCall = (MethodCall) cs.lambda();
        WindowContext ctx = parseWindowContext(overCall);
        WindowFunctionSpec funcSpec = parseWindowFunctionFromExpr(overCall.source());

        RelationExtendExpression.TypedWindowSpec typedSpec = RelationExtendExpression.TypedWindowSpec.of(
                funcSpec, ctx.partitionCols, ctx.orderSpecs, ctx.frame);

        return RelationExtendExpression.window(source, cs.name(), typedSpec);
    }

    /**
     * Parses: extend(~col:c|$c.id:y|$y->plus())
     * This is an aggregate extend without explicit over() - uses entire relation as
     * window.
     * - cs.lambda() is the map function (extract column value)
     * - cs.extraFunction() is the aggregate function (plus/sum, avg, etc.)
     */
    private PureExpression parseAggregateExtendNoOver(PureExpression source, ColumnSpec cs) {
        // Extract the column name from the map lambda
        String columnName = extractColumnFromMapLambda(cs.lambda());

        // Get the aggregate function from extraFunction
        AggregateFunctionSpec.AggregateFunction aggFunc = AggregateFunctionSpec.AggregateFunction.SUM; // default
        if (cs.extraFunction() instanceof LambdaExpression aggLambda) {
            PureExpression aggBody = aggLambda.body();
            if (aggBody instanceof MethodCall mc) {
                aggFunc = switch (mc.methodName().toLowerCase()) {
                    case "plus", "sum" -> AggregateFunctionSpec.AggregateFunction.SUM;
                    case "avg", "average" -> AggregateFunctionSpec.AggregateFunction.AVG;
                    case "count", "size" -> AggregateFunctionSpec.AggregateFunction.COUNT;
                    case "min" -> AggregateFunctionSpec.AggregateFunction.MIN;
                    case "max" -> AggregateFunctionSpec.AggregateFunction.MAX;
                    case "stddev" -> AggregateFunctionSpec.AggregateFunction.STDDEV;
                    case "stddevsample" -> AggregateFunctionSpec.AggregateFunction.STDDEV_SAMP;
                    case "stddevpopulation" -> AggregateFunctionSpec.AggregateFunction.STDDEV_POP;
                    case "variance" -> AggregateFunctionSpec.AggregateFunction.VARIANCE;
                    case "variancesample" -> AggregateFunctionSpec.AggregateFunction.VAR_SAMP;
                    case "variancepopulation" -> AggregateFunctionSpec.AggregateFunction.VAR_POP;
                    case "median" -> AggregateFunctionSpec.AggregateFunction.MEDIAN;
                    case "mode" -> AggregateFunctionSpec.AggregateFunction.MODE;
                    case "corr" -> AggregateFunctionSpec.AggregateFunction.CORR;
                    case "covarsample" -> AggregateFunctionSpec.AggregateFunction.COVAR_SAMP;
                    case "covarpopulation" -> AggregateFunctionSpec.AggregateFunction.COVAR_POP;
                    case "percentile", "percentilecont" -> AggregateFunctionSpec.AggregateFunction.PERCENTILE_CONT;
                    case "percentiledisc" -> AggregateFunctionSpec.AggregateFunction.PERCENTILE_DISC;
                    default -> throw new PureParseException("Unknown aggregate function: " + mc.methodName());
                };
                // Extract percentile value for percentile functions
                if ((aggFunc == AggregateFunctionSpec.AggregateFunction.PERCENTILE_CONT
                        || aggFunc == AggregateFunctionSpec.AggregateFunction.PERCENTILE_DISC)
                        && !mc.arguments().isEmpty()) {
                    double pVal = ((Number) extractLiteralValue(mc.arguments().get(0))).doubleValue();
                    // percentile(p, ascending, continuous): 3rd arg false -> DISC
                    if (mc.arguments().size() >= 3) {
                        Object contVal = extractLiteralValue(mc.arguments().get(2));
                        if (Boolean.FALSE.equals(contVal)) {
                            aggFunc = AggregateFunctionSpec.AggregateFunction.PERCENTILE_DISC;
                        }
                    }
                    // 2nd arg false -> ascending=false -> use (1-p)
                    if (mc.arguments().size() >= 2) {
                        Object ascVal = extractLiteralValue(mc.arguments().get(1));
                        if (Boolean.FALSE.equals(ascVal)) {
                            pVal = 1.0 - pVal;
                        }
                    }
                    AggregateFunctionSpec pSpec = AggregateFunctionSpec.percentile(aggFunc, columnName, pVal, List.of(), List.of());
                    RelationExtendExpression.TypedWindowSpec pTypedSpec = RelationExtendExpression.TypedWindowSpec.of(
                            pSpec, List.of(), List.of(), null);
                    return RelationExtendExpression.window(source, cs.name(), pTypedSpec);
                }
            }
        }

        // Create aggregate function spec with empty partition (entire relation)
        AggregateFunctionSpec funcSpec = AggregateFunctionSpec.of(aggFunc, columnName, List.of(), List.of());

        // Create window spec with no partition and no order (entire relation as window)
        RelationExtendExpression.TypedWindowSpec typedSpec = RelationExtendExpression.TypedWindowSpec.of(
                funcSpec, List.of(), List.of(), null);

        return RelationExtendExpression.window(source, cs.name(), typedSpec);
    }

    private record WindowContext(
            List<String> partitionCols,
            List<RelationExtendExpression.SortSpec> orderSpecs,
            RelationExtendExpression.FrameSpec frame) {
    }

    private WindowContext parseWindowContext(PureExpression overExpr) {
        List<PureExpression> overArgs = (overExpr instanceof FunctionCall fc)
                ? fc.arguments()
                : (overExpr instanceof MethodCall mc)
                        ? concatWithSource(mc.source(), mc.arguments())
                        : List.of();

        List<String> partitionCols = new ArrayList<>();
        List<RelationExtendExpression.SortSpec> orderSpecs = new ArrayList<>();
        RelationExtendExpression.FrameSpec frame = null;

        for (PureExpression arg : overArgs) {
            if (arg instanceof ColumnSpec cs) {
                partitionCols.add(cs.name());
            } else if (arg instanceof ColumnSpecArray csa) {
                for (PureExpression spec : csa.specs()) {
                    if (spec instanceof ColumnSpec c)
                        partitionCols.add(c.name());
                }
            } else if (arg instanceof MethodCall mc) {
                String method = mc.methodName();
                if ("descending".equals(method) || "desc".equals(method)) {
                    orderSpecs.add(new RelationExtendExpression.SortSpec(
                            extractColName(mc.source()), RelationExtendExpression.SortDirection.DESC));
                } else if ("ascending".equals(method) || "asc".equals(method)) {
                    orderSpecs.add(new RelationExtendExpression.SortSpec(
                            extractColName(mc.source()), RelationExtendExpression.SortDirection.ASC));
                } else if ("rows".equals(method)) {
                    // Handle: unbounded()->rows(unbounded()) as MethodCall
                    frame = parseFrameFromMethodCall(mc, RelationExtendExpression.FrameType.ROWS);
                } else if ("range".equals(method) || "_range".equals(method)) {
                    // Handle: unbounded()->range(unbounded()) as MethodCall
                    frame = parseFrameFromMethodCall(mc, RelationExtendExpression.FrameType.RANGE);
                }
            } else if (arg instanceof FunctionCall fc) {
                if ("rows".equals(fc.functionName())) {
                    frame = parseFrame(fc, RelationExtendExpression.FrameType.ROWS);
                } else if ("range".equals(fc.functionName()) || "_range".equals(fc.functionName())) {
                    frame = parseFrame(fc, RelationExtendExpression.FrameType.RANGE);
                }
            } else if (arg instanceof ArrayLiteral arr) {
                // Handle: [~col1->ascending(), ~col2->descending()] as array of order specs
                for (PureExpression element : arr.elements()) {
                    if (element instanceof MethodCall mc) {
                        String method = mc.methodName();
                        if ("descending".equals(method) || "desc".equals(method)) {
                            orderSpecs.add(new RelationExtendExpression.SortSpec(
                                    extractColName(mc.source()), RelationExtendExpression.SortDirection.DESC));
                        } else if ("ascending".equals(method) || "asc".equals(method)) {
                            orderSpecs.add(new RelationExtendExpression.SortSpec(
                                    extractColName(mc.source()), RelationExtendExpression.SortDirection.ASC));
                        }
                    }
                }
            }
        }

        return new WindowContext(partitionCols, orderSpecs, frame);
    }

    private List<PureExpression> concatWithSource(PureExpression source, List<PureExpression> args) {
        List<PureExpression> result = new ArrayList<>();
        result.add(source);
        result.addAll(args);
        return result;
    }

    private RelationExtendExpression.FrameSpec parseFrame(FunctionCall fc, RelationExtendExpression.FrameType type) {
        if (fc.arguments().size() < 2)
            return null;
        return new RelationExtendExpression.FrameSpec(type,
                parseFrameBound(fc.arguments().get(0)),
                parseFrameBound(fc.arguments().get(1)));
    }

    /**
     * Parses frame from MethodCall pattern: unbounded()->rows(unbounded())
     * Source is the start bound, first argument is the end bound.
     */
    private RelationExtendExpression.FrameSpec parseFrameFromMethodCall(MethodCall mc,
            RelationExtendExpression.FrameType type) {
        RelationExtendExpression.FrameBound startBound = parseFrameBound(mc.source());
        RelationExtendExpression.FrameBound endBound = mc.arguments().isEmpty()
                ? RelationExtendExpression.FrameBound.currentRow()
                : parseFrameBound(mc.arguments().get(0));
        return new RelationExtendExpression.FrameSpec(type, startBound, endBound);
    }

    private RelationExtendExpression.FrameBound parseFrameBound(PureExpression expr) {
        if (expr instanceof FunctionCall fc && "unbounded".equals(fc.functionName())) {
            return RelationExtendExpression.FrameBound.unbounded();
        } else if (expr instanceof IntegerLiteral lit) {
            return RelationExtendExpression.FrameBound.fromDouble(lit.value().doubleValue());
        } else if (expr instanceof LiteralExpr lit && lit.value() instanceof Number num) {
            return RelationExtendExpression.FrameBound.fromDouble(num.doubleValue());
        } else if (expr instanceof UnaryExpression unary && "-".equals(unary.operator())) {
            // Handle negative numbers like -1 for PRECEDING
            if (unary.operand() instanceof LiteralExpr lit && lit.value() instanceof Number num) {
                return RelationExtendExpression.FrameBound.fromDouble(-num.doubleValue());
            } else if (unary.operand() instanceof IntegerLiteral lit) {
                return RelationExtendExpression.FrameBound.fromDouble(-lit.value().doubleValue());
            }
        }
        return RelationExtendExpression.FrameBound.currentRow();
    }

    private WindowFunctionSpec parseWindowFunctionFromColumnSpec(ColumnSpec cs) {
        // Pattern with extraFunction:
        // ~newCol:{p,w,r|$r.id->cast(@Integer)}:y|$y->plus()
        // - cs.lambda() is the map function that extracts the column
        // - cs.extraFunction() is the agg function (e.g., plus for SUM)
        if (cs.extraFunction() instanceof LambdaExpression aggLambda) {
            String columnName = extractColumnFromMapLambda(cs.lambda());

            // Get the aggregate function from the agg lambda body
            PureExpression aggBody = aggLambda.body();
            if (aggBody instanceof MethodCall mc) {
                // Pattern: $y->plus() or $y->sum() or $y->joinStrings('')
                AggregateFunctionSpec.AggregateFunction aggFunc = switch (mc.methodName().toLowerCase()) {
                    case "plus", "sum" -> AggregateFunctionSpec.AggregateFunction.SUM;
                    case "avg", "average" -> AggregateFunctionSpec.AggregateFunction.AVG;
                    case "count", "size" -> AggregateFunctionSpec.AggregateFunction.COUNT;
                    case "min" -> AggregateFunctionSpec.AggregateFunction.MIN;
                    case "max" -> AggregateFunctionSpec.AggregateFunction.MAX;
                    case "stddev" -> AggregateFunctionSpec.AggregateFunction.STDDEV;
                    case "stddevsample" -> AggregateFunctionSpec.AggregateFunction.STDDEV_SAMP;
                    case "stddevpopulation" -> AggregateFunctionSpec.AggregateFunction.STDDEV_POP;
                    case "variance" -> AggregateFunctionSpec.AggregateFunction.VARIANCE;
                    case "variancesample" -> AggregateFunctionSpec.AggregateFunction.VAR_SAMP;
                    case "variancepopulation" -> AggregateFunctionSpec.AggregateFunction.VAR_POP;
                    case "median" -> AggregateFunctionSpec.AggregateFunction.MEDIAN;
                    case "mode" -> AggregateFunctionSpec.AggregateFunction.MODE;
                    case "corr" -> AggregateFunctionSpec.AggregateFunction.CORR;
                    case "covarsample" -> AggregateFunctionSpec.AggregateFunction.COVAR_SAMP;
                    case "covarpopulation" -> AggregateFunctionSpec.AggregateFunction.COVAR_POP;
                    case "percentile", "percentilecont" -> AggregateFunctionSpec.AggregateFunction.PERCENTILE_CONT;
                    case "percentiledisc" -> AggregateFunctionSpec.AggregateFunction.PERCENTILE_DISC;
                    case "joinstrings" -> AggregateFunctionSpec.AggregateFunction.STRING_AGG;
                    default -> throw new PureParseException("Unknown aggregate function: " + mc.methodName());
                };
                // Extract percentile value for percentile functions
                if ((aggFunc == AggregateFunctionSpec.AggregateFunction.PERCENTILE_CONT
                        || aggFunc == AggregateFunctionSpec.AggregateFunction.PERCENTILE_DISC)
                        && !mc.arguments().isEmpty()) {
                    double pVal = ((Number) extractLiteralValue(mc.arguments().get(0))).doubleValue();
                    // percentile(p, ascending, continuous): 3rd arg false -> DISC
                    if (mc.arguments().size() >= 3) {
                        Object contVal = extractLiteralValue(mc.arguments().get(2));
                        if (Boolean.FALSE.equals(contVal)) {
                            aggFunc = AggregateFunctionSpec.AggregateFunction.PERCENTILE_DISC;
                        }
                    }
                    // 2nd arg false -> ascending=false -> use (1-p)
                    if (mc.arguments().size() >= 2) {
                        Object ascVal = extractLiteralValue(mc.arguments().get(1));
                        if (Boolean.FALSE.equals(ascVal)) {
                            pVal = 1.0 - pVal;
                        }
                    }
                    return AggregateFunctionSpec.percentile(aggFunc, columnName, pVal, List.of(), List.of());
                }
                return AggregateFunctionSpec.of(aggFunc, columnName, List.of(), List.of());
            }
            // If no method call, default to SUM on extracted column
            return AggregateFunctionSpec.of(AggregateFunctionSpec.AggregateFunction.SUM,
                    columnName, List.of(), List.of());
        }

        if (!(cs.lambda() instanceof LambdaExpression lambda)) {
            throw new PureParseException("Expected lambda expression for window function column spec: " + cs.name());
        }

        PureExpression body = lambda.body();

        // Pattern: {p,w,r|$p->rowNumber($r)} - ranking functions
        // Pattern: {p,w,r|$p->cumulativeDistribution($w,$r)->round(2)} - with
        // post-processor
        if (body instanceof MethodCall mc) {
            // Check if this is a chain: window_func()->scalar_func()
            if (isScalarPostProcessor(mc.methodName()) && mc.source() instanceof MethodCall innerMc) {
                // outer mc is post-processor, inner is the window function
                WindowFunctionSpec spec = parseWindowFunctionFromMethodCall(innerMc);
                // Extract post-processor args as Objects
                List<Object> postProcessorArgs = mc.arguments().stream()
                        .map(this::extractLiteralValue)
                        .toList();
                return new PostProcessedWindowFunctionSpec(spec, mc.methodName(), postProcessorArgs);
            }
            return parseWindowFunctionFromMethodCall(mc);
        }

        // Pattern: {p,w,r|$p->lead($r).salary} or {p,w,r|$p->sum($w,$r).salary}
        // - value/aggregate functions with PropertyAccessExpression
        if (body instanceof PropertyAccessExpression pae && pae.source() instanceof MethodCall mc) {
            // The property name (salary) becomes the column for the function
            WindowFunctionSpec spec = parseWindowFunctionFromMethodCall(mc);
            // Update the column name based on function type
            if (spec instanceof ValueFunctionSpec vfs) {
                return new ValueFunctionSpec(vfs.function(), pae.propertyName(),
                        vfs.offset(), vfs.partitionBy(), vfs.orderBy(), vfs.frame());
            }
            if (spec instanceof AggregateFunctionSpec afs) {
                return AggregateFunctionSpec.of(afs.function(), pae.propertyName(),
                        afs.partitionBy(), afs.orderBy());
            }
            return spec;
        }

        // Pattern: {p,w,r|$r.salary} - bare property access is ambiguous
        if (body instanceof PropertyAccessExpression pae) {
            throw new PureParseException("Bare property access '" + pae.propertyName()
                    + "' in window function - specify explicit function (sum, avg, count, etc.)");
        }

        // Also check PropertyAccess for compatibility
        if (body instanceof PropertyAccess pa && pa.source() instanceof MethodCall mc) {
            WindowFunctionSpec spec = parseWindowFunctionFromMethodCall(mc);
            if (spec instanceof ValueFunctionSpec vfs) {
                return new ValueFunctionSpec(vfs.function(), pa.propertyName(),
                        vfs.offset(), vfs.partitionBy(), vfs.orderBy(), vfs.frame());
            }
            return spec;
        }

        if (body instanceof PropertyAccess pa) {
            throw new PureParseException("Bare property access '" + pa.propertyName()
                    + "' in window function - specify explicit function (sum, avg, count, etc.)");
        }

        throw new PureParseException(
                "Unrecognized window function pattern in lambda body: " + body.getClass().getSimpleName());
    }

    /**
     * Extracts the column name from a map lambda like
     * {p,w,r|$r.id->cast(@Integer)}.
     */
    private String extractColumnFromMapLambda(PureExpression expr) {
        if (expr instanceof LambdaExpression lambda) {
            return extractColumnFromExpression(lambda.body());
        }
        return extractColumnFromExpression(expr);
    }

    /**
     * Extracts the column name from an expression like $r.id->cast(@Integer).
     */
    private String extractColumnFromExpression(PureExpression expr) {
        // Pattern: $r.id->cast(@Integer) - property access with method call
        if (expr instanceof MethodCall mc) {
            return extractColumnFromExpression(mc.source());
        }

        // Pattern: $r.id->cast(@Integer) - parsed as CastExpression
        if (expr instanceof CastExpression cast) {
            return extractColumnFromExpression(cast.source());
        }

        // Pattern: meta::pure::functions::lang::cast($r.id, @Integer) - qualified
        // function call
        if (expr instanceof FunctionCall fc) {
            if (!fc.arguments().isEmpty()) {
                return extractColumnFromExpression(fc.arguments().get(0));
            }
        }

        // Pattern: $r.id - direct property access
        if (expr instanceof PropertyAccessExpression pae) {
            return pae.propertyName();
        }
        if (expr instanceof PropertyAccess pa) {
            return pa.propertyName();
        }

        throw new PureParseException(
                "Unable to extract column name from expression: " + expr.getClass().getSimpleName());
    }

    private WindowFunctionSpec parseWindowFunctionFromExpr(PureExpression expr) {
        if (expr instanceof FunctionCall fc) {
            return switch (fc.functionName()) {
                case "row_number", "rowNumber" -> RankingFunctionSpec.of(
                        RankingFunctionSpec.RankingFunction.ROW_NUMBER, List.of(), List.of());
                case "rank" -> RankingFunctionSpec.of(
                        RankingFunctionSpec.RankingFunction.RANK, List.of(), List.of());
                case "dense_rank", "denseRank" -> RankingFunctionSpec.of(
                        RankingFunctionSpec.RankingFunction.DENSE_RANK, List.of(), List.of());
                case "ntile" -> {
                    int n = fc.arguments().isEmpty() ? 1 : ((IntegerLiteral) fc.arguments().get(0)).value().intValue();
                    yield RankingFunctionSpec.ntile(n, List.of(), List.of());
                }
                default -> throw new PureParseException("Unknown window function: " + fc.functionName());
            };
        }
        throw new PureParseException(
                "Expected FunctionCall for window function, got: " + expr.getClass().getSimpleName());
    }

    private WindowFunctionSpec parseWindowFunctionFromMethodCall(MethodCall mc) {
        return switch (mc.methodName()) {
            // Ranking functions
            case "rowNumber" -> RankingFunctionSpec.of(
                    RankingFunctionSpec.RankingFunction.ROW_NUMBER, List.of(), List.of());
            case "rank" -> RankingFunctionSpec.of(
                    RankingFunctionSpec.RankingFunction.RANK, List.of(), List.of());
            case "denseRank" -> RankingFunctionSpec.of(
                    RankingFunctionSpec.RankingFunction.DENSE_RANK, List.of(), List.of());
            case "percentRank" -> RankingFunctionSpec.of(
                    RankingFunctionSpec.RankingFunction.PERCENT_RANK, List.of(), List.of());
            case "cumulativeDistribution" -> RankingFunctionSpec.of(
                    RankingFunctionSpec.RankingFunction.CUME_DIST, List.of(), List.of());
            case "ntile" -> {
                // ntile($r, 2) - relation is first arg, bucket count is second
                int n = 1;
                if (mc.arguments().size() >= 2) {
                    var bucketArg = mc.arguments().get(1);
                    if (bucketArg instanceof LiteralExpr lit && lit.value() instanceof Number num) {
                        n = num.intValue();
                    } else if (bucketArg instanceof IntegerLiteral lit) {
                        n = lit.value().intValue();
                    }
                }
                yield RankingFunctionSpec.ntile(n, List.of(), List.of());
            }

            // Value functions (LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE)
            case "lag" -> ValueFunctionSpec.lagLead(
                    ValueFunctionSpec.ValueFunction.LAG, extractValueColumn(mc), 1, List.of(), List.of());
            case "lead" -> ValueFunctionSpec.lagLead(
                    ValueFunctionSpec.ValueFunction.LEAD, extractValueColumn(mc), 1, List.of(), List.of());
            case "first", "firstValue" -> ValueFunctionSpec.firstLast(
                    ValueFunctionSpec.ValueFunction.FIRST_VALUE, extractValueColumn(mc), List.of(), List.of(), null);
            case "last", "lastValue" -> ValueFunctionSpec.firstLast(
                    ValueFunctionSpec.ValueFunction.LAST_VALUE, extractValueColumn(mc), List.of(), List.of(), null);
            case "nth", "nthValue" -> ValueFunctionSpec.lagLead(
                    ValueFunctionSpec.ValueFunction.NTH_VALUE, extractValueColumn(mc), extractNthOffset(mc), List.of(),
                    List.of());

            // Aggregate functions
            case "sum", "plus" -> AggregateFunctionSpec.of(AggregateFunctionSpec.AggregateFunction.SUM,
                    extractColFromMethodCall(mc), List.of(), List.of());
            case "avg", "average" -> AggregateFunctionSpec.of(AggregateFunctionSpec.AggregateFunction.AVG,
                    extractColFromMethodCall(mc), List.of(), List.of());
            case "count" -> AggregateFunctionSpec.of(AggregateFunctionSpec.AggregateFunction.COUNT,
                    "*", List.of(), List.of()); // COUNT(*) when no column specified
            case "min" -> AggregateFunctionSpec.of(AggregateFunctionSpec.AggregateFunction.MIN,
                    extractColFromMethodCall(mc), List.of(), List.of());
            case "max" -> AggregateFunctionSpec.of(AggregateFunctionSpec.AggregateFunction.MAX,
                    extractColFromMethodCall(mc), List.of(), List.of());
            case "stdDev" -> AggregateFunctionSpec.of(AggregateFunctionSpec.AggregateFunction.STDDEV,
                    extractColFromMethodCall(mc), List.of(), List.of());
            case "variance" -> AggregateFunctionSpec.of(AggregateFunctionSpec.AggregateFunction.VARIANCE,
                    extractColFromMethodCall(mc), List.of(), List.of());

            default -> throw new PureParseException("Unknown window function method: " + mc.methodName());
        };
    }

    /**
     * Checks if the method name is a scalar post-processor function.
     */
    private boolean isScalarPostProcessor(String methodName) {
        return switch (methodName.toLowerCase()) {
            case "round", "abs", "floor", "ceiling", "ceil", "sqrt", "exp", "log", "log10",
                    "sin", "cos", "tan", "asin", "acos", "atan", "toone" ->
                true;
            default -> false;
        };
    }

    /**
     * Extracts a literal value from a PureExpression for post-processor arguments.
     */
    private Object extractLiteralValue(PureExpression expr) {
        if (expr instanceof LiteralExpr lit) {
            return lit.value();
        }
        if (expr instanceof IntegerLiteral intLit) {
            return intLit.value();
        }
        if (expr instanceof StringLiteral strLit) {
            return strLit.value();
        }
        // For unsupported types, return string representation
        return expr.toString();
    }

    private String extractValueColumn(MethodCall mc) {
        // For lag($r).salary, the column name comes from the property access AFTER the
        // method call
        // (the outer PropertyAccessExpression), not from inside the MethodCall.
        // This method tries to extract from the source if it happens to be a
        // PropertyAccess,
        // otherwise returns null (the caller will override with the correct property
        // name).
        if (mc.source() instanceof PropertyAccess pa) {
            return pa.propertyName();
        }
        // Return null - the caller will provide the actual column name from the outer
        // PropertyAccessExpression
        return null;
    }

    private int extractNthOffset(MethodCall mc) {
        // For nth($w, $r, N), extract the integer offset from arguments
        for (PureExpression arg : mc.arguments()) {
            if (arg instanceof LiteralExpr lit && lit.type() == LiteralExpr.LiteralType.INTEGER) {
                return ((Number) lit.value()).intValue();
            } else if (arg instanceof IntegerLiteral lit) {
                return lit.value().intValue();
            }
        }
        throw new PureParseException("nth() function requires an integer offset argument");
    }

    private String extractColName(PureExpression expr) {
        if (expr instanceof ColumnSpec cs)
            return cs.name();
        if (expr instanceof PropertyAccess pa)
            return pa.propertyName();
        return "unknown";
    }

    private String extractColFromMethodCall(MethodCall mc) {
        return "value"; // placeholder for aggregate column extraction
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
        // graphFetch() requires a ClassExpression as source (e.g., Person.all())
        // It cannot be used on RelationExpressions (e.g., #>{DB.TABLE}#)
        if (source instanceof RelationExpression) {
            throw new PureParseException(
                    "graphFetch() can only be called on class expressions like Person.all(), " +
                            "not on relation expressions like #>{db.table}#");
        }

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

    private PureExpression parseCastCall(PureExpression source, List<PureExpression> args) {
        if (args.isEmpty()) {
            throw new PureParseException("cast() requires a type argument like @Integer");
        }
        PureExpression arg = args.get(0);
        if (!(arg instanceof TypeReference typeRef)) {
            throw new PureParseException("cast() requires a type argument like @Integer, got: " + arg);
        }
        return new CastExpression(source, typeRef.typeName());
    }

    private PureExpression parseFlattenCall(PureExpression source, List<PureExpression> args) {
        // Accept RelationExpression or CastExpression (for
        // ->cast(@Relation<...>)->flatten())
        if (!(source instanceof RelationExpression) && !(source instanceof CastExpression)) {
            throw new PureParseException(
                    "flatten() requires a Relation source, got: " + source.getClass().getSimpleName());
        }
        if (args.isEmpty()) {
            throw new PureParseException("flatten() requires a column reference like ~items");
        }
        PureExpression arg = args.get(0);
        if (!(arg instanceof ColumnSpec colSpec)) {
            throw new PureParseException("flatten() requires a column reference like ~items, got: " + arg);
        }
        return new FlattenExpression(source, colSpec.name());
    }

    private PureExpression parseDistinctCall(PureExpression source, List<PureExpression> args) {
        // distinct() - all columns
        // distinct(~[col1, col2]) - specific columns
        if (args.isEmpty()) {
            return DistinctExpression.all(source);
        }

        // Parse column list
        List<String> columns = new ArrayList<>();
        for (PureExpression arg : args) {
            if (arg instanceof ColumnSpec colSpec) {
                columns.add(colSpec.name());
            } else if (arg instanceof ColumnSpecArray csArray) {
                // Handle ColSpecArray from PCT: distinct(~[col1, col2])
                for (PureExpression elem : csArray.specs()) {
                    if (elem instanceof ColumnSpec cs) {
                        columns.add(cs.name());
                    }
                }
            } else if (arg instanceof ArrayLiteral arr) {
                for (PureExpression elem : arr.elements()) {
                    if (elem instanceof ColumnSpec cs) {
                        columns.add(cs.name());
                    }
                }
            }
        }
        return DistinctExpression.columns(source, columns);
    }

    private PureExpression parseRenameCall(PureExpression source, List<PureExpression> args) {
        // rename(~oldCol, ~newCol)
        if (args.size() < 2) {
            throw new PureParseException("rename() requires 2 column references: rename(~oldCol, ~newCol)");
        }
        String oldCol = extractColumnName(args.get(0));
        String newCol = extractColumnName(args.get(1));
        return new RenameExpression(source, oldCol, newCol);
    }

    private String extractColumnName(PureExpression expr) {
        if (expr instanceof ColumnSpec colSpec) {
            return colSpec.name();
        }
        if (expr instanceof LiteralExpr lit && lit.value() instanceof String s) {
            return s;
        }
        throw new PureParseException("Expected column reference, got: " + expr);
    }

    private PureExpression parseConcatenateCall(PureExpression source, List<PureExpression> args) {
        // concatenate(otherRelation)
        if (args.isEmpty()) {
            throw new PureParseException("concatenate() requires a relation argument");
        }
        PureExpression other = args.get(0);
        return new ConcatenateExpression(source, other);
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
        // Handle negative literals: UnaryExpression[operator=-, operand=LiteralExpr[value=1]]
        if (expr instanceof UnaryExpression unary && unary.operator().equals("-")) {
            return -extractInt(unary.operand());
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
