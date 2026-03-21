package com.gs.legend.antlr;

import com.gs.legend.ast.*;
import com.gs.legend.parser.PureParseException;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Clean ANTLR visitor that produces {@link ValueSpecification} AST nodes.
 *
 * <p>
 * This is the "thin parser" — it does ZERO semantic interpretation:
 * <ul>
 * <li>ALL function calls become {@link AppliedFunction} — no specialized
 * nodes</li>
 * <li>NO SQL generation — no expressionToSqlString, no mapAggFunctionName</li>
 * <li>NO type checking — no ClassExpression vs RelationExpression
 * distinction</li>
 * <li>NO argument destructuring — arguments stay as flat parameters</li>
 * </ul>
 *
 * <p>
 * The compiler is responsible for all semantic interpretation.
 */
public class ValueSpecificationBuilder extends PureParserBaseVisitor<ValueSpecification> {

    // Current input source for extracting source text
    private String inputSource;

    public void setInputSource(String source) {
        this.inputSource = source;
    }

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

    /**
     * Extract source text for the receiver expression up to the i-th arrow in a
     * FunctionExpressionContext. Mirrors PureAstBuilder.extractSourceTextUpTo.
     */
    private String extractSourceTextUpTo(PureParser.FunctionExpressionContext ctx, int arrowIndex) {
        if (inputSource == null)
            return null;

        // Walk up to the ExpressionContext to get full expression start
        ParserRuleContext current = ctx.getParent();
        while (current != null && !(current instanceof PureParser.ExpressionContext)) {
            current = current.getParent();
        }
        if (current == null || current.getStart() == null)
            return null;

        int start = current.getStart().getStartIndex();

        // Get position just before the arrow for this function call
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

    /**
     * Extract source text for the receiver expression up to the i-th arrow in a
     * FunctionChainAfterArrowContext.
     */
    private String extractSourceTextUpToChain(PureParser.FunctionChainAfterArrowContext ctx, int arrowIndex) {
        if (inputSource == null)
            return null;

        // Walk up to the ExpressionContext
        ParserRuleContext current = ctx.getParent();
        while (current != null && !(current instanceof PureParser.ExpressionContext)) {
            current = current.getParent();
        }
        if (current == null || current.getStart() == null)
            return null;

        int start = current.getStart().getStartIndex();

        var nameCtx = ctx.qualifiedName().get(arrowIndex);
        if (nameCtx == null || nameCtx.getStart() == null)
            return null;

        int stop = nameCtx.getStart().getStartIndex() - 2;

        if (start >= 0 && stop > start && stop < inputSource.length()) {
            return inputSource.substring(start, stop).trim();
        }
        return null;
    }

    /**
     * Extract argument source texts from a function call's parameters context.
     */
    private List<String> extractArgTexts(PureParser.FunctionExpressionParametersContext ctx) {
        List<String> argTexts = new ArrayList<>();
        if (ctx != null && ctx.combinedExpression() != null) {
            for (var argCtx : ctx.combinedExpression()) {
                String argText = extractSourceText(argCtx);
                argTexts.add(argText != null ? argText : "");
            }
        }
        return argTexts;
    }

    // ========================================
    // ENTRY POINT - Combined Expression
    // ========================================

    @Override
    public ValueSpecification visitCombinedExpression(PureParser.CombinedExpressionContext ctx) {
        ValueSpecification expr = visit(ctx.expression());
        for (PureParser.ExpressionPartContext part : ctx.expressionPart()) {
            expr = processExpressionPart(expr, part);
        }
        return expr;
    }

    private ValueSpecification processExpressionPart(ValueSpecification left, PureParser.ExpressionPartContext ctx) {
        if (ctx.booleanPart() != null) {
            return processBooleanPart(left, ctx.booleanPart());
        } else if (ctx.arithmeticPart() != null) {
            return processArithmeticPart(left, ctx.arithmeticPart());
        }
        return left;
    }

    private ValueSpecification processBooleanPart(ValueSpecification left, PureParser.BooleanPartContext bp) {
        ValueSpecification right = visitCombinedArithmeticOnly(bp.combinedArithmeticOnly());
        String op = bp.AND() != null ? "and" : "or";
        return new AppliedFunction(op, List.of(left, right));
    }

    private ValueSpecification processArithmeticPart(ValueSpecification left, PureParser.ArithmeticPartContext ctx) {
        List<PureParser.ExpressionContext> exprs = ctx.expression();
        if (!exprs.isEmpty()) {
            String funcName = determineArithmeticFuncName(ctx);
            ValueSpecification result = left;
            for (PureParser.ExpressionContext exprCtx : exprs) {
                ValueSpecification right = visit(exprCtx);
                result = new AppliedFunction(funcName, List.of(result, right));
            }
            return result;
        }
        return left;
    }

    private String determineArithmeticFuncName(PureParser.ArithmeticPartContext ctx) {
        if (!ctx.PLUS().isEmpty())
            return "plus";
        if (!ctx.MINUS().isEmpty())
            return "minus";
        if (!ctx.STAR().isEmpty())
            return "times";
        if (!ctx.DIVIDE().isEmpty())
            return "divide";
        if (ctx.LESS_THAN() != null)
            return "lessThan";
        if (ctx.LESS_OR_EQUAL() != null)
            return "lessThanEqual";
        if (ctx.GREATER_THAN() != null)
            return "greaterThan";
        if (ctx.GREATER_OR_EQUAL() != null)
            return "greaterThanEqual";
        return "plus";
    }

    @Override
    public ValueSpecification visitCombinedArithmeticOnly(PureParser.CombinedArithmeticOnlyContext ctx) {
        ValueSpecification expr = visit(ctx.expression());
        for (PureParser.ArithmeticPartContext part : ctx.arithmeticPart()) {
            expr = processArithmeticPart(expr, part);
        }
        return expr;
    }

    // ========================================
    // EXPRESSION - Main chain processing
    // ========================================

    @Override
    public ValueSpecification visitExpression(PureParser.ExpressionContext ctx) {
        ValueSpecification expr = visit(ctx.nonArrowOrEqualExpression());

        for (PureParser.PropertyOrFunctionExpressionContext pofCtx : ctx.propertyOrFunctionExpression()) {
            expr = processPropertyOrFunction(expr, pofCtx);
        }

        if (ctx.equalNotEqual() != null) {
            PureParser.EqualNotEqualContext eq = ctx.equalNotEqual();
            String funcName = eq.TEST_EQUAL() != null ? "equal" : "notEqual";
            ValueSpecification right = visit(eq.combinedArithmeticOnly());
            return new AppliedFunction(funcName, List.of(expr, right));
        }

        return expr;
    }

    private ValueSpecification processPropertyOrFunction(ValueSpecification source,
            PureParser.PropertyOrFunctionExpressionContext ctx) {
        if (ctx.propertyExpression() != null) {
            return processPropertyExpression(source, ctx.propertyExpression());
        }
        if (ctx.functionExpression() != null) {
            return processFunctionExpression(source, ctx.functionExpression());
        }
        if (ctx.propertyBracketExpression() != null) {
            PureParser.PropertyBracketExpressionContext bracket = ctx.propertyBracketExpression();
            if (bracket.INTEGER() != null) {
                int index = Integer.parseInt(bracket.INTEGER().getText());
                return new AppliedFunction("at", List.of(source, new CInteger(index)), true);
            }
            if (bracket.STRING() != null) {
                String key = unquote(bracket.STRING().getText());
                return new AppliedProperty(key, List.of(source));
            }
        }
        throw new PureParseException("Unknown property/function: " + ctx.getText());
    }

    private ValueSpecification processPropertyExpression(ValueSpecification source,
            PureParser.PropertyExpressionContext ctx) {
        String propName = getIdentifierText(ctx.identifier());

        if (ctx.functionExpressionParameters() != null) {
            // Method call: source.method(args) → AppliedFunction("method", [source,
            // args...])
            List<ValueSpecification> args = parseFunctionArgs(ctx.functionExpressionParameters());
            var params = new ArrayList<ValueSpecification>();
            params.add(source);
            params.addAll(args);
            return new AppliedFunction(propName, params, true);
        }

        // Enum value access: EnumType.VALUE → EnumValue
        if (source instanceof PackageableElementPtr(String fullPath)) {
            return new EnumValue(fullPath, propName);
        }

        // Property access: $x.name → AppliedProperty("name", [$x])
        return new AppliedProperty(propName, List.of(source));
    }

    private ValueSpecification processFunctionExpression(ValueSpecification source,
            PureParser.FunctionExpressionContext ctx) {
        ValueSpecification result = source;

        List<PureParser.QualifiedNameContext> names = ctx.qualifiedName();
        List<PureParser.FunctionExpressionParametersContext> params = ctx.functionExpressionParameters();

        for (int i = 0; i < names.size(); i++) {
            String funcName = getQualifiedNameText(names.get(i));
            List<ValueSpecification> args = parseFunctionArgs(params.get(i));

            // Capture source text for UDF inlining (mirrors PureAstBuilder pattern)
            String sourceText = extractSourceTextUpTo(ctx, i);
            List<String> argTexts = extractArgTexts(params.get(i));

            // Preserve full qualified name — adapter strips when needed
            var allParams = new ArrayList<ValueSpecification>();
            allParams.add(result); // receiver is always first parameter
            allParams.addAll(args);
            result = new AppliedFunction(funcName, allParams, true, sourceText, argTexts);
        }

        return result;
    }

    // ========================================
    // NON-ARROW EXPRESSION (Primary)
    // ========================================

    @Override
    public ValueSpecification visitNonArrowOrEqualExpression(PureParser.NonArrowOrEqualExpressionContext ctx) {
        if (ctx.atomicExpression() != null) {
            return visit(ctx.atomicExpression());
        }
        if (ctx.notExpression() != null) {
            ValueSpecification inner = visit(ctx.notExpression().expression());
            return new AppliedFunction("not", List.of(inner));
        }
        if (ctx.signedExpression() != null) {
            PureParser.SignedExpressionContext signed = ctx.signedExpression();
            ValueSpecification inner = visit(signed.expression());
            if (signed.MINUS() != null) {
                return new AppliedFunction("minus", List.of(inner));
            }
            return inner; // unary + is identity
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
    public ValueSpecification visitAtomicExpression(PureParser.AtomicExpressionContext ctx) {
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
            String typeName = getTypeText(ctx.type());
            return new GenericTypeInstance(typeName);
        }
        if (ctx.tdsLiteral() != null) {
            return visitTdsLiteral(ctx.tdsLiteral());
        }
        if (ctx.comparatorExpression() != null) {
            return visitComparatorExpression(ctx.comparatorExpression());
        }
        throw new PureParseException("Unknown atomic expression: " + ctx.getText());
    }

    // ========================================
    // TDS LITERAL
    // ========================================

    public ValueSpecification visitTdsLiteral(PureParser.TdsLiteralContext ctx) {
        // TDS literals are DSL constructs → ClassInstance("tdsLiteral", raw content)
        String raw = ctx.TDS_LITERAL().getText();
        return new ClassInstance("tdsLiteral", raw);
    }

    // ========================================
    // DSL (Island Grammar)
    // ========================================

    @Override
    public ValueSpecification visitDsl(PureParser.DslContext ctx) {
        if (ctx.dslExtension() != null) {
            return visitDslExtension(ctx.dslExtension());
        }
        if (ctx.dslNavigationPath() != null) {
            throw new PureParseException("Navigation paths not supported: " + ctx.getText());
        }
        throw new PureParseException("Unknown DSL: " + ctx.getText());
    }

    @Override
    public ValueSpecification visitDslExtension(PureParser.DslExtensionContext ctx) {
        String islandOpen = ctx.ISLAND_OPEN().getText();
        String dslType = islandOpen.substring(1, islandOpen.length() - 1);

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

        ValueSpecification result = switch (dslType) {
            case "" -> parseGraphFetchTree(contentText);
            case ">" -> parseRelationLiteral(contentText);
            default -> throw new PureParseException("Unknown DSL type: #" + dslType + "{");
        };

        // Handle arrow chains after DSL
        if (ctx.functionChainAfterArrow() != null) {
            result = processFunctionChainAfterArrow(result, ctx.functionChainAfterArrow());
        }

        return result;
    }

    private ValueSpecification processFunctionChainAfterArrow(ValueSpecification source,
            PureParser.FunctionChainAfterArrowContext ctx) {
        ValueSpecification current = source;
        var qualifiedNames = ctx.qualifiedName();
        var params = ctx.functionExpressionParameters();

        for (int i = 0; i < qualifiedNames.size(); i++) {
            String funcName = qualifiedNames.get(i).getText();
            List<ValueSpecification> args = new ArrayList<>();
            List<String> argTexts = new ArrayList<>();
            if (i < params.size()) {
                for (var argCtx : params.get(i).combinedExpression()) {
                    args.add(visit(argCtx));
                    String argText = extractSourceText(argCtx);
                    argTexts.add(argText != null ? argText : "");
                }
            }
            // Capture source text for receiver
            String sourceText = extractSourceTextUpToChain(ctx, i);

            // ALL arrows become AppliedFunction — preserve qualified name
            var allParams = new ArrayList<ValueSpecification>();
            allParams.add(current);
            allParams.addAll(args);
            current = new AppliedFunction(funcName, allParams, true, sourceText, argTexts);
        }
        return current;
    }

    private ValueSpecification parseGraphFetchTree(String content) {
        // Sub-parse the island content through PureParser.graphFetchTree() rule.
        // The island grammar captures #{...}# as raw text; we re-lex/parse to get
        // structured GraphFetchTree nodes with full ANTLR validation.
        PureLexer lexer = new PureLexer(CharStreams.fromString(content));
        PureParser parser = new PureParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(new org.antlr.v4.runtime.BaseErrorListener() {
            @Override
            public void syntaxError(org.antlr.v4.runtime.Recognizer<?, ?> recognizer,
                    Object offendingSymbol, int line, int charPositionInLine,
                    String msg, org.antlr.v4.runtime.RecognitionException e) {
                throw new PureParseException(
                        "GraphFetch tree parse error at " + line + ":" + charPositionInLine + " - " + msg);
            }
        });

        PureParser.GraphFetchTreeContext tree = parser.graphFetchTree();
        GraphFetchTree gft = buildGraphFetchTree(tree);
        return new ClassInstance("rootGraphFetchTree", gft);
    }

    /**
     * Builds a GraphFetchTree from the parser context.
     */
    private GraphFetchTree buildGraphFetchTree(PureParser.GraphFetchTreeContext ctx) {
        String rootClass = ctx.qualifiedName().getText();
        List<GraphFetchTree.PropertyFetch> properties = buildGraphPaths(ctx.graphDefinition());
        return new GraphFetchTree(rootClass, properties);
    }

    /**
     * Builds property fetches from a graphDefinition: { graphPaths }
     */
    private List<GraphFetchTree.PropertyFetch> buildGraphPaths(PureParser.GraphDefinitionContext ctx) {
        List<GraphFetchTree.PropertyFetch> props = new ArrayList<>();
        if (ctx.graphPaths() != null) {
            for (PureParser.GraphPathContext gp : ctx.graphPaths().graphPath()) {
                props.add(buildPropertyFetch(gp));
            }
            // Handle subTypeGraphPath if present (future extension)
            for (PureParser.SubTypeGraphPathContext stgp : ctx.graphPaths().subTypeGraphPath()) {
                // For now, we could support subtypes but skip for simplicity
                String subTypeName = stgp.subtype().qualifiedName().getText();
                List<GraphFetchTree.PropertyFetch> subProps = buildGraphPaths(stgp.graphDefinition());
                // Represent as a special property fetch with subtype marker
                GraphFetchTree subTree = new GraphFetchTree(subTypeName, subProps);
                props.add(new GraphFetchTree.PropertyFetch("@" + subTypeName, subTree));
            }
        }
        return props;
    }

    /**
     * Builds a single PropertyFetch from a graphPath: alias? identifier params? subtype? graphDefinition?
     */
    private GraphFetchTree.PropertyFetch buildPropertyFetch(PureParser.GraphPathContext ctx) {
        String propName = getIdentifierText(ctx.identifier());

        // Check for nested graph definition
        if (ctx.graphDefinition() != null) {
            List<GraphFetchTree.PropertyFetch> nested = buildGraphPaths(ctx.graphDefinition());
            // For nested, the rootClass is the property name (or resolved class)
            GraphFetchTree subTree = new GraphFetchTree(propName, nested);
            return GraphFetchTree.PropertyFetch.nested(propName, subTree);
        }

        return GraphFetchTree.PropertyFetch.simple(propName);
    }

    private ValueSpecification parseRelationLiteral(String content) {
        // Relation literals: store::DB.TABLE → ClassInstance("relation", content)
        return new ClassInstance("relation", content);
    }

    // ========================================
    // INSTANCE REFERENCE (e.g., Person.all())
    // ========================================

    @Override
    public ValueSpecification visitInstanceReference(PureParser.InstanceReferenceContext ctx) {
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

        if (ctx.allOrFunction() != null) {
            PureParser.AllOrFunctionContext aof = ctx.allOrFunction();

            if (aof.allFunction() != null || aof.allFunctionWithMilestoning() != null) {
                // Person.all() → AppliedFunction("getAll", [PackageableElementPtr("Person")])
                return new AppliedFunction("getAll", List.of(new PackageableElementPtr(name)));
            }
            if (aof.allVersionsFunction() != null) {
                return new AppliedFunction("getAllVersions", List.of(new PackageableElementPtr(name)));
            }
            if (aof.functionExpressionParameters() != null) {
                // Standalone function call: funcName(args) — preserve qualified name
                List<ValueSpecification> args = parseFunctionArgs(aof.functionExpressionParameters());
                List<String> argTexts = extractArgTexts(aof.functionExpressionParameters());
                return new AppliedFunction(name, args, false, null, argTexts);
            }
        }

        // Just a class/package reference → PackageableElementPtr
        return new PackageableElementPtr(name);
    }

    // ========================================
    // VARIABLE
    // ========================================

    @Override
    public ValueSpecification visitVariable(PureParser.VariableContext ctx) {
        String varName = getIdentifierText(ctx.identifier());
        return new Variable(varName);
    }

    // ========================================
    // LITERALS
    // ========================================

    @Override
    public ValueSpecification visitInstanceLiteralToken(PureParser.InstanceLiteralTokenContext ctx) {
        if (ctx.STRING() != null) {
            return new CString(unquote(ctx.STRING().getText()));
        }
        if (ctx.INTEGER() != null) {
            try {
                return new CInteger(Long.parseLong(ctx.INTEGER().getText()));
            } catch (NumberFormatException e) {
                // Integer > Long.MAX_VALUE — use BigInteger to preserve value
                return new CInteger(new BigInteger(ctx.INTEGER().getText()));
            }
        }
        if (ctx.FLOAT() != null) {
            String text = ctx.FLOAT().getText();
            double d = Double.parseDouble(text);
            // Check if double preserves the original value — if not, use BigDecimal
            BigDecimal exact = new BigDecimal(text);
            if (exact.compareTo(BigDecimal.valueOf(d)) != 0) {
                return new CDecimal(exact);
            }
            return new CFloat(d);
        }
        if (ctx.DECIMAL() != null) {
            String text = ctx.DECIMAL().getText();
            text = text.substring(0, text.length() - 1); // Remove 'd' suffix
            return new CDecimal(new BigDecimal(text));
        }
        if (ctx.BOOLEAN() != null) {
            return new CBoolean(Boolean.parseBoolean(ctx.BOOLEAN().getText()));
        }
        if (ctx.DATE() != null) {
            String dateText = ctx.DATE().getText();
            // Dates in Pure start with % — strip it
            String value = dateText.startsWith("%") ? dateText.substring(1) : dateText;
            // Determine if it's a DateTime or StrictDate based on presence of 'T'
            if (value.contains("T")) {
                return new CDateTime(value);
            }
            return new CStrictDate(value);
        }
        if (ctx.STRICTTIME() != null) {
            String timeText = ctx.STRICTTIME().getText();
            String value = timeText.startsWith("%") ? timeText.substring(1) : timeText;
            return new CStrictTime(value);
        }
        throw new PureParseException("Unknown literal: " + ctx.getText());
    }

    // ========================================
    // LAMBDA
    // ========================================

    @Override
    public ValueSpecification visitAnyLambda(PureParser.AnyLambdaContext ctx) {
        if (ctx.lambdaFunction() != null) {
            return visitLambdaFunction(ctx.lambdaFunction());
        }
        // x|expr or x:Type[1]|expr — single param lambda
        if (ctx.lambdaParam() != null) {
            Variable param = extractVariable(ctx.lambdaParam());
            List<ValueSpecification> body = lambdaBodyStatements(ctx.lambdaPipe());
            return new LambdaFunction(List.of(param), body);
        }
        // |expr — lambda with no params
        if (ctx.lambdaPipe() != null) {
            List<ValueSpecification> body = lambdaBodyStatements(ctx.lambdaPipe());
            return new LambdaFunction(List.of(), body);
        }
        throw new PureParseException("Unknown lambda: " + ctx.getText());
    }

    @Override
    public ValueSpecification visitLambdaFunction(PureParser.LambdaFunctionContext ctx) {
        // {x, y | body} or {x:Type[1], y:Type[*] | body}
        List<Variable> params = new ArrayList<>();
        List<PureParser.LambdaParamContext> paramContexts = ctx.lambdaParam();
        if (paramContexts.isEmpty()) {
            params.add(new Variable("_"));
        } else {
            for (PureParser.LambdaParamContext paramCtx : paramContexts) {
                params.add(extractVariable(paramCtx));
            }
        }

        List<ValueSpecification> body = lambdaBodyStatements(ctx.lambdaPipe());
        return new LambdaFunction(params, body);
    }

    /**
     * Extract a Variable from a lambdaParam, including optional type annotation.
     */
    private Variable extractVariable(PureParser.LambdaParamContext paramCtx) {
        String name = getIdentifierText(paramCtx.identifier());
        if (paramCtx.lambdaParamType() != null) {
            String typeName = paramCtx.lambdaParamType().type().getText();
            String multiplicity = paramCtx.lambdaParamType().multiplicity().getText();
            // Strip brackets: [1] → 1, [0..1] → 0..1, [*] → *
            if (multiplicity.startsWith("[") && multiplicity.endsWith("]")) {
                multiplicity = multiplicity.substring(1, multiplicity.length() - 1);
            }
            return new Variable(name, typeName, multiplicity);
        }
        return new Variable(name);
    }

    /**
     * Returns lambda body as a list of statements.
     * Matches legend-pure's M3 model: lambda body IS the statement list.
     * Single-statement bodies return a one-element list; multi-statement
     * bodies (let x = ...; let y = ...; result) return all statements.
     */
    private List<ValueSpecification> lambdaBodyStatements(PureParser.LambdaPipeContext ctx) {
        return lambdaBodyStatements(ctx.codeBlock());
    }

    private List<ValueSpecification> lambdaBodyStatements(PureParser.CodeBlockContext ctx) {
        List<PureParser.ProgramLineContext> lines = ctx.programLine();
        if (lines.isEmpty()) {
            return List.of(new CString(""));
        }
        List<ValueSpecification> stmts = new ArrayList<>();
        for (PureParser.ProgramLineContext line : lines) {
            stmts.add(visit(line));
        }
        return stmts;
    }

    @Override
    public ValueSpecification visitCodeBlock(PureParser.CodeBlockContext ctx) {
        List<PureParser.ProgramLineContext> lines = ctx.programLine();
        if (lines.isEmpty()) {
            return new CString("");
        }
        if (lines.size() == 1) {
            return visit(lines.get(0));
        }
        // Multi-statement: this path should only be hit by visitComparatorExpression.
        // Lambda construction uses codeBlockStatements() instead.
        List<ValueSpecification> stmts = new ArrayList<>();
        for (PureParser.ProgramLineContext line : lines) {
            stmts.add(visit(line));
        }
        return new AppliedFunction("block", stmts);
    }

    // ========================================
    // COMPARATOR EXPRESSION
    // ========================================

    public ValueSpecification visitComparatorExpression(PureParser.ComparatorExpressionContext ctx) {
        // comparator(a: Type[1], b: Type[1]): Boolean[1] { body }
        List<Variable> params = ctx.functionVariableExpression().stream()
                .map(fve -> {
                    String name = getIdentifierText(fve.identifier());
                    if (fve.type() != null && fve.multiplicity() != null) {
                        String typeName = fve.type().getText();
                        String multiplicity = fve.multiplicity().getText();
                        if (multiplicity.startsWith("[") && multiplicity.endsWith("]")) {
                            multiplicity = multiplicity.substring(1, multiplicity.length() - 1);
                        }
                        return new Variable(name, typeName, multiplicity);
                    }
                    return new Variable(name);
                })
                .toList();
        List<ValueSpecification> body = lambdaBodyStatements(ctx.codeBlock());
        return new LambdaFunction(params, body);
    }

    // ========================================
    // EXPRESSION ARRAY → Collection
    // ========================================

    @Override
    public ValueSpecification visitExpressionsArray(PureParser.ExpressionsArrayContext ctx) {
        List<ValueSpecification> elements = new ArrayList<>();
        for (PureParser.ExpressionContext exprCtx : ctx.expression()) {
            elements.add(visit(exprCtx));
        }
        return new PureCollection(elements);
    }

    // ========================================
    // COLUMN BUILDERS (~ syntax)
    // ========================================

    @Override
    public ValueSpecification visitColumnBuilders(PureParser.ColumnBuildersContext ctx) {
        if (ctx.oneColSpec() != null) {
            return visitOneColSpec(ctx.oneColSpec());
        }
        if (ctx.colSpecArray() != null) {
            return visitColSpecArray(ctx.colSpecArray());
        }
        throw new PureParseException("Unknown column builder: " + ctx.getText());
    }

    public ValueSpecification visitOneColSpec(PureParser.OneColSpecContext ctx) {
        String colName = getIdentifierText(ctx.identifier());

        LambdaFunction fn1 = null;
        LambdaFunction fn2 = null;

        if (ctx.anyLambda() != null) {
            fn1 = (LambdaFunction) visit(ctx.anyLambda());
            if (ctx.extraFunction() != null) {
                fn2 = (LambdaFunction) visit(ctx.extraFunction().anyLambda());
            }
        }

        ColSpec spec = new ColSpec(colName, fn1, fn2);
        return new ClassInstance("colSpec", spec);
    }

    public ValueSpecification visitColSpecArray(PureParser.ColSpecArrayContext ctx) {
        List<ColSpec> specs = new ArrayList<>();
        for (PureParser.OneColSpecContext specCtx : ctx.oneColSpec()) {
            ClassInstance ci = (ClassInstance) visitOneColSpec(specCtx);
            specs.add((ColSpec) ci.value());
        }
        return new ClassInstance("colSpecArray", new ColSpecArray(specs));
    }

    // ========================================
    // EXPRESSION INSTANCE (^ClassName(...))
    // ========================================

    @Override
    public ValueSpecification visitExpressionInstance(PureParser.ExpressionInstanceContext ctx) {
        String className = getQualifiedNameText(ctx.qualifiedName());

        // Capture generic type arguments: ^Pair<String, String>(...)
        List<String> typeArgs = List.of();
        if (ctx.typeArguments() != null && ctx.typeArguments().typeWithOperation() != null) {
            typeArgs = ctx.typeArguments().typeWithOperation().stream()
                    .map(t -> t.getText())
                    .collect(java.util.stream.Collectors.toList());
        }

        // Collect property assignments
        Map<String, ValueSpecification> properties = new LinkedHashMap<>();
        if (ctx.expressionInstanceParserPropertyAssignment() != null) {
            for (var assign : ctx.expressionInstanceParserPropertyAssignment()) {
                String propName = assign.identifier().stream()
                        .map(this::getIdentifierText)
                        .collect(java.util.stream.Collectors.joining("."));
                // Navigate the grammar: assignment -> rightSide
                ValueSpecification value = visit(assign.expressionInstanceRightSide());
                properties.put(propName, value);
            }
        }

        return new ClassInstance("instance",
                new InstanceData(className, properties, typeArgs));
    }

    /**
     * Data holder for instance expressions (^ClassName(prop=value)).
     */
    public record InstanceData(
            String className,
            Map<String, ValueSpecification> properties,
            List<String> typeArguments) {
        public InstanceData(String className, Map<String, ValueSpecification> properties) {
            this(className, properties, List.of());
        }
    }

    // ========================================
    // PROGRAM LINE (let statements + result)
    // ========================================

    @Override
    public ValueSpecification visitProgramLine(PureParser.ProgramLineContext ctx) {
        if (ctx.combinedExpression() != null) {
            return visit(ctx.combinedExpression());
        }
        if (ctx.letExpression() != null) {
            return visit(ctx.letExpression());
        }
        // If there are multiple program lines, visit the last expression
        throw new PureParseException("Unknown program line: " + ctx.getText());
    }

    @Override
    public ValueSpecification visitLetExpression(PureParser.LetExpressionContext ctx) {
        // let x = expr → AppliedFunction("letFunction", [CString("x"), value])
        String varName = ctx.identifier() != null
                ? getIdentifierText(ctx.identifier())
                : ctx.getText();
        ValueSpecification value = visit(ctx.combinedExpression());
        return new AppliedFunction("letFunction", List.of(new CString(varName), value));
    }

    // ========================================
    // HELPERS
    // ========================================

    private List<ValueSpecification> parseFunctionArgs(PureParser.FunctionExpressionParametersContext ctx) {
        List<ValueSpecification> args = new ArrayList<>();
        if (ctx.combinedExpression() != null) {
            for (PureParser.CombinedExpressionContext exprCtx : ctx.combinedExpression()) {
                args.add(visit(exprCtx));
            }
        }
        return args;
    }

    private String getIdentifierText(PureParser.IdentifierContext ctx) {
        if (ctx == null)
            return "";
        String text = ctx.getText();
        // Strip surrounding single quotes from quoted identifiers like
        // '2011__|__newCol'
        if (text.length() >= 2 && text.startsWith("'") && text.endsWith("'")) {
            text = text.substring(1, text.length() - 1);
        }
        return text;
    }

    private String getQualifiedNameText(PureParser.QualifiedNameContext ctx) {
        if (ctx == null)
            return "";
        return ctx.getText();
    }

    private String getUnitNameText(PureParser.UnitNameContext ctx) {
        if (ctx == null)
            return "";
        return ctx.getText();
    }

    private String getTypeText(PureParser.TypeContext ctx) {
        if (ctx == null)
            return "";
        return ctx.getText();
    }

    private String unquote(String s) {
        if (s == null || s.length() < 2)
            return s;
        char first = s.charAt(0);
        char last = s.charAt(s.length() - 1);
        if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
            return unescapeString(s.substring(1, s.length() - 1));
        }
        return s;
    }

    private String unescapeString(String s) {
        if (s == null || !s.contains("\\"))
            return s;
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char next = s.charAt(i + 1);
                switch (next) {
                    case 'n' -> {
                        sb.append('\n');
                        i++;
                    }
                    case 'r' -> {
                        sb.append('\r');
                        i++;
                    }
                    case 't' -> {
                        sb.append('\t');
                        i++;
                    }
                    case '\\' -> {
                        sb.append('\\');
                        i++;
                    }
                    case '\'' -> {
                        sb.append('\'');
                        i++;
                    }
                    case '"' -> {
                        sb.append('"');
                        i++;
                    }
                    default -> sb.append(c);
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
