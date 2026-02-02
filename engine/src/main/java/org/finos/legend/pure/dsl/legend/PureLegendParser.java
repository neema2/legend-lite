package org.finos.legend.pure.dsl.legend;

import org.finos.legend.pure.dsl.antlr.PureLexer;
import org.finos.legend.pure.dsl.antlr.PureParser;
import org.finos.legend.pure.dsl.antlr.PureParserBaseVisitor;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.util.ArrayList;
import java.util.List;

/**
 * "Thin" parser that converts Pure code to minimal Expression AST.
 * 
 * Key design:
 * - All method calls ($x->foo(a)) become Function("foo", [$x, a])
 * - No semantic interpretation of function names (groupBy, filter, etc.)
 * - Column specs (~col) become Function("column", [...])
 * - The compiler handles ALL semantic interpretation
 */
public class PureLegendParser extends PureParserBaseVisitor<Expression> {

    /**
     * Parse a Pure expression string to Expression AST.
     */
    public static Expression parse(String code) {
        PureLexer lexer = new PureLexer(CharStreams.fromString(code));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        PureParser parser = new PureParser(tokens);
        return new PureLegendParser().visit(parser.combinedExpression());
    }

    // ========================================
    // ENTRY POINT
    // ========================================

    @Override
    public Expression visitCombinedExpression(PureParser.CombinedExpressionContext ctx) {
        Expression expr = visit(ctx.expression());

        for (PureParser.ExpressionPartContext part : ctx.expressionPart()) {
            expr = processExpressionPart(expr, part);
        }

        return expr;
    }

    private Expression processExpressionPart(Expression left, PureParser.ExpressionPartContext ctx) {
        if (ctx.booleanPart() != null) {
            PureParser.BooleanPartContext bp = ctx.booleanPart();
            Expression right = visitCombinedArithmeticOnly(bp.combinedArithmeticOnly());
            String op = bp.AND() != null ? "and" : "or";
            return Function.of(op, left, right);
        }
        if (ctx.arithmeticPart() != null) {
            return processArithmeticPart(left, ctx.arithmeticPart());
        }
        return left;
    }

    private Expression processArithmeticPart(Expression left, PureParser.ArithmeticPartContext ctx) {
        List<PureParser.ExpressionContext> exprs = ctx.expression();
        if (!exprs.isEmpty()) {
            String op = determineArithmeticOp(ctx);
            Expression result = left;
            for (PureParser.ExpressionContext exprCtx : exprs) {
                Expression right = visit(exprCtx);
                result = Function.of(op, result, right);
            }
            return result;
        }
        return left;
    }

    private String determineArithmeticOp(PureParser.ArithmeticPartContext ctx) {
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

    // ========================================
    // EXPRESSION
    // ========================================

    @Override
    public Expression visitExpression(PureParser.ExpressionContext ctx) {
        // Start with nonArrowOrEqualExpression (not expressionOrExpressionGroup)
        Expression expr = visit(ctx.nonArrowOrEqualExpression());

        for (PureParser.PropertyOrFunctionExpressionContext pof : ctx.propertyOrFunctionExpression()) {
            expr = processPropertyOrFunction(expr, pof);
        }

        if (ctx.equalNotEqual() != null) {
            PureParser.EqualNotEqualContext eq = ctx.equalNotEqual();
            Expression right = visitCombinedArithmeticOnly(eq.combinedArithmeticOnly());
            String op = eq.TEST_EQUAL() != null ? "equal" : "notEqual";
            expr = Function.of(op, expr, right);
        }

        return expr;
    }

    private Expression processPropertyOrFunction(Expression source,
            PureParser.PropertyOrFunctionExpressionContext ctx) {
        if (ctx.propertyExpression() != null) {
            PureParser.PropertyExpressionContext propCtx = ctx.propertyExpression();
            String propName = getIdentifierText(propCtx.identifier());

            if (propCtx.functionExpressionParameters() != null) {
                List<Expression> args = new ArrayList<>();
                args.add(source);
                args.addAll(parseFunctionArgs(propCtx.functionExpressionParameters()));
                return new Function(propName, args);
            }

            return new Property(source, propName);
        }
        if (ctx.functionExpression() != null) {
            return processFunctionExpression(source, ctx.functionExpression());
        }
        return source;
    }

    private Expression processFunctionExpression(Expression source, PureParser.FunctionExpressionContext ctx) {
        Expression result = source;

        List<PureParser.QualifiedNameContext> names = ctx.qualifiedName();
        List<PureParser.FunctionExpressionParametersContext> params = ctx.functionExpressionParameters();

        for (int i = 0; i < names.size(); i++) {
            String funcName = getSimpleName(names.get(i).getText());
            List<Expression> args = new ArrayList<>();
            args.add(result);
            if (i < params.size()) {
                args.addAll(parseFunctionArgs(params.get(i)));
            }
            result = new Function(funcName, args);
        }

        return result;
    }

    @Override
    public Expression visitCombinedArithmeticOnly(PureParser.CombinedArithmeticOnlyContext ctx) {
        // Uses expression(), not expressionOrExpressionGroup
        Expression result = visit(ctx.expression());
        for (PureParser.ArithmeticPartContext part : ctx.arithmeticPart()) {
            result = processArithmeticPart(result, part);
        }
        return result;
    }

    @Override
    public Expression visitNonArrowOrEqualExpression(PureParser.NonArrowOrEqualExpressionContext ctx) {
        if (ctx.atomicExpression() != null) {
            return visit(ctx.atomicExpression());
        }
        if (ctx.notExpression() != null) {
            Expression operand = visit(ctx.notExpression().expression());
            return Function.of("not", operand);
        }
        if (ctx.signedExpression() != null) {
            Expression operand = visit(ctx.signedExpression().expression());
            if (ctx.signedExpression().MINUS() != null) {
                return Function.of("negate", operand);
            }
            return operand;
        }
        if (ctx.expressionsArray() != null) {
            return visitExpressionsArray(ctx.expressionsArray());
        }
        return Literal.nil();
    }

    @Override
    public Expression visitExpressionsArray(PureParser.ExpressionsArrayContext ctx) {
        List<Expression> elements = new ArrayList<>();
        for (PureParser.ExpressionContext exprCtx : ctx.expression()) {
            elements.add(visit(exprCtx));
        }
        return new Collection(elements);
    }

    // ========================================
    // ATOMIC EXPRESSIONS
    // ========================================

    @Override
    public Expression visitAtomicExpression(PureParser.AtomicExpressionContext ctx) {
        if (ctx.variable() != null) {
            String name = getIdentifierText(ctx.variable().identifier());
            return new Variable(name);
        }

        if (ctx.instanceLiteralToken() != null) {
            return visitInstanceLiteralToken(ctx.instanceLiteralToken());
        }

        if (ctx.anyLambda() != null) {
            return visitAnyLambda(ctx.anyLambda());
        }

        if (ctx.instanceReference() != null) {
            return visitInstanceReference(ctx.instanceReference());
        }

        if (ctx.columnBuilders() != null) {
            return visitColumnBuilders(ctx.columnBuilders());
        }

        if (ctx.type() != null) {
            String typeName = ctx.type().getText();
            return Function.of("type", Literal.string(typeName));
        }

        if (ctx.tdsLiteral() != null) {
            return visitTdsLiteral(ctx.tdsLiteral());
        }

        return Literal.nil();
    }

    /**
     * Parses TDS (Tabular Data Set) literals.
     * 
     * Syntax:
     * #TDS
     * col1, col2
     * val1, val2
     * #
     */
    public TdsLiteral visitTdsLiteral(PureParser.TdsLiteralContext ctx) {
        String raw = ctx.TDS_LITERAL().getText();

        // Strip #TDS prefix and # suffix
        String content = raw.substring(4, raw.length() - 1).trim();

        // Split into lines
        String[] lines = content.split("\\n");

        // First line is column names
        java.util.List<String> columnNames = new ArrayList<>();
        if (lines.length > 0) {
            for (String col : lines[0].split(",")) {
                columnNames.add(col.trim());
            }
        }

        // Remaining lines are data rows
        java.util.List<java.util.List<Object>> rows = new ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            String line = lines[i].trim();
            if (line.isEmpty())
                continue;

            java.util.List<Object> row = new ArrayList<>();
            String[] values = line.split(",", -1); // -1 preserves trailing empty strings
            for (String val : values) {
                row.add(parseTdsValue(val.trim()));
            }
            rows.add(row);
        }

        return new TdsLiteral(columnNames, rows);
    }

    /**
     * Parse a TDS cell value to appropriate type.
     */
    private Object parseTdsValue(String value) {
        if (value.isEmpty() || "null".equalsIgnoreCase(value)) {
            return null;
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
        // String value
        return value;
    }

    @Override
    public Expression visitInstanceLiteralToken(PureParser.InstanceLiteralTokenContext ctx) {
        if (ctx.STRING() != null) {
            String text = ctx.STRING().getText();
            String value = text.substring(1, text.length() - 1);
            return Literal.string(value);
        }
        if (ctx.INTEGER() != null) {
            return Literal.integer(Long.parseLong(ctx.INTEGER().getText()));
        }
        if (ctx.FLOAT() != null) {
            return Literal.decimal(Double.parseDouble(ctx.FLOAT().getText()));
        }
        if (ctx.DECIMAL() != null) {
            String text = ctx.DECIMAL().getText();
            return Literal.decimal(Double.parseDouble(text));
        }
        if (ctx.BOOLEAN() != null) {
            return Literal.bool(Boolean.parseBoolean(ctx.BOOLEAN().getText()));
        }
        return Literal.nil();
    }

    @Override
    public Expression visitAnyLambda(PureParser.AnyLambdaContext ctx) {
        if (ctx.lambdaFunction() != null) {
            return visitLambdaFunction(ctx.lambdaFunction());
        }
        if (ctx.lambdaParam() != null) {
            String param = getIdentifierText(ctx.lambdaParam().identifier());
            Expression body = visitLambdaBody(ctx.lambdaPipe());
            return Lambda.of(param, body);
        }
        if (ctx.lambdaPipe() != null) {
            Expression body = visitLambdaBody(ctx.lambdaPipe());
            return Lambda.of("_", body);
        }
        return Literal.nil();
    }

    @Override
    public Expression visitLambdaFunction(PureParser.LambdaFunctionContext ctx) {
        List<String> params = new ArrayList<>();
        if (ctx.lambdaParam() != null) {
            for (PureParser.LambdaParamContext paramCtx : ctx.lambdaParam()) {
                params.add(getIdentifierText(paramCtx.identifier()));
            }
        }
        if (params.isEmpty()) {
            params.add("_");
        }
        Expression body = visitLambdaBody(ctx.lambdaPipe());
        return new Lambda(params, body);
    }

    private Expression visitLambdaBody(PureParser.LambdaPipeContext ctx) {
        return visitCodeBlock(ctx.codeBlock());
    }

    @Override
    public Expression visitCodeBlock(PureParser.CodeBlockContext ctx) {
        List<PureParser.ProgramLineContext> lines = ctx.programLine();
        if (lines.isEmpty()) {
            return Literal.nil();
        }
        if (lines.size() == 1) {
            return visitProgramLine(lines.get(0));
        }
        return visitProgramLine(lines.get(lines.size() - 1));
    }

    @Override
    public Expression visitProgramLine(PureParser.ProgramLineContext ctx) {
        if (ctx.combinedExpression() != null) {
            return visit(ctx.combinedExpression());
        }
        return Literal.nil();
    }

    @Override
    public Expression visitInstanceReference(PureParser.InstanceReferenceContext ctx) {
        String name = ctx.qualifiedName().getText();

        if (ctx.allOrFunction() != null) {
            var aof = ctx.allOrFunction();
            if (aof.allFunction() != null) {
                Expression classRef = Function.of("class", Literal.string(name));
                return Function.of("all", classRef);
            }
            if (aof.allVersionsFunction() != null) {
                Expression classRef = Function.of("class", Literal.string(name));
                return Function.of("allVersions", classRef);
            }
            if (aof.allVersionsInRangeFunction() != null) {
                Expression classRef = Function.of("class", Literal.string(name));
                return Function.of("allVersionsInRange", classRef);
            }
            if (aof.functionExpressionParameters() != null) {
                List<Expression> args = parseFunctionArgs(aof.functionExpressionParameters());
                return new Function(name, args);
            }
        }

        return Function.of("class", Literal.string(name));
    }

    // ========================================
    // COLUMN BUILDERS (~col syntax)
    // ========================================

    @Override
    public Expression visitColumnBuilders(PureParser.ColumnBuildersContext ctx) {
        if (ctx.oneColSpec() != null) {
            return visitOneColSpec(ctx.oneColSpec());
        }
        if (ctx.colSpecArray() != null) {
            return visitColSpecArray(ctx.colSpecArray());
        }
        return Literal.nil();
    }

    @Override
    public Expression visitOneColSpec(PureParser.OneColSpecContext ctx) {
        String colName = getIdentifierText(ctx.identifier());
        List<Expression> args = new ArrayList<>();
        args.add(Literal.string(colName));

        if (ctx.anyLambda() != null) {
            args.add(visitAnyLambda(ctx.anyLambda()));
        }

        if (ctx.extraFunction() != null && ctx.extraFunction().anyLambda() != null) {
            args.add(visitAnyLambda(ctx.extraFunction().anyLambda()));
        }

        return new Function("column", args);
    }

    @Override
    public Expression visitColSpecArray(PureParser.ColSpecArrayContext ctx) {
        List<Expression> columns = new ArrayList<>();
        for (PureParser.OneColSpecContext colCtx : ctx.oneColSpec()) {
            columns.add(visitOneColSpec(colCtx));
        }
        return new Collection(columns);
    }

    // ========================================
    // HELPERS
    // ========================================

    private List<Expression> parseFunctionArgs(PureParser.FunctionExpressionParametersContext ctx) {
        List<Expression> args = new ArrayList<>();
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
        return ctx.getText();
    }

    private String getSimpleName(String qualifiedName) {
        if (qualifiedName.contains("::")) {
            return qualifiedName.substring(qualifiedName.lastIndexOf("::") + 2);
        }
        return qualifiedName;
    }
}
