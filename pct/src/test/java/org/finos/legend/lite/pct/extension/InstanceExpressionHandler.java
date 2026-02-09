package org.finos.legend.lite.pct.extension;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.regex.Pattern;

import org.finos.legend.engine.execution.BufferedResult;
import org.finos.legend.engine.execution.Result;
import org.finos.legend.engine.execution.ScalarResult;
import org.finos.legend.engine.plan.ConstantNode;
import org.finos.legend.engine.plan.Expression;
import org.finos.legend.engine.plan.ListFilterExpression;
import org.finos.legend.engine.plan.ListLiteral;
import org.finos.legend.engine.plan.RelationNode;
import org.finos.legend.engine.plan.SqlCollectionCall;
import org.finos.legend.engine.plan.SqlFunctionCall;
import org.finos.legend.engine.plan.StructLiteralExpression;
import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLGenerator;
import org.finos.legend.pure.dsl.ArrayLiteral;
import org.finos.legend.pure.dsl.InstanceExpression;
import org.finos.legend.pure.dsl.PureCompiler;
import org.finos.legend.pure.dsl.PureExpression;
import org.finos.legend.pure.dsl.PureParser;

/**
 * Handles PCT queries that use InstanceExpression arrays as relation sources.
 * 
 * Uses the new STRUCT-based approach:
 * 1. Parse Pure expression
 * 2. Compile to IR (including StructLiteralNode for instance arrays)
 * 3. Generate SQL with DuckDB STRUCT literals
 * 4. Execute directly - NO DDL, NO INSERT needed!
 * 
 * Pattern: [^FirmType(prop1='val1', ...),
 * ^FirmType(...)...]-&gt;project(~[...])
 */
public class InstanceExpressionHandler {

    // Regex to find instance array: [^ClassName(...), ...] at start of expression
    private static final Pattern INSTANCE_ARRAY_PATTERN = Pattern.compile("^\\s*\\[\\s*\\^([\\w:]+)\\s*\\(");

    /**
     * Checks if the expression contains an InstanceExpression array.
     */
    public boolean requiresInstanceHandling(String pureExpression) {
        return INSTANCE_ARRAY_PATTERN.matcher(pureExpression).find();
    }

    /**
     * Executes an InstanceExpression-based query using STRUCT literals.
     * 
     * The new approach:
     * 1. Parse Pure expression to AST
     * 2. Compile AST to RelationNode IR (StructLiteralNode for instance arrays)
     * 3. Generate SQL with DuckDB STRUCT literals
     * 4. Execute SQL directly
     */
    public Result execute(String pureExpression, Connection connection) throws SQLException {
        // 1. Parse to AST
        PureExpression ast = PureParser.parse(pureExpression);
        System.out.println("[InstanceHandler] Parsed AST: " + ast.getClass().getSimpleName());

        // 2. Compile to IR (null mapping registry is OK for STRUCT literal compilation)
        PureCompiler compiler = new PureCompiler(null, null);
        RelationNode ir = compiler.compileExpression(ast, null);
        System.out.println("[InstanceHandler] Compiled IR: " + ir.getClass().getSimpleName());

        // 3. Generate SQL with STRUCT literals
        SQLGenerator generator = new SQLGenerator(DuckDBDialect.INSTANCE);
        String sql = generator.generate(ir);
        System.out.println("[InstanceHandler] Generated SQL: " + sql);

        // 4. Execute SQL using BufferedResult helper
        try (var stmt = connection.createStatement();
                var rs = stmt.executeQuery(sql)) {
            BufferedResult buffered = BufferedResult.fromResultSet(rs);

            // For scalar results (e.g., find() returning a single struct),
            // wrap as ScalarResult with pureType for class instance reconstruction
            if (buffered.rowCount() == 1 && buffered.columnCount() == 1) {
                String sqlType = buffered.columns().getFirst().sqlType();
                String pureType = (ir instanceof ConstantNode cn) ? extractPureType(cn.expression()) : null;
                return new ScalarResult(buffered.getValue(0, 0), sqlType, pureType);
            }
            return buffered;
        }
    }

    /**
     * Extracts the Pure class type name from an IR expression tree.
     * Walks into function calls and list operations to find StructLiteralExpression nodes.
     */
    private String extractPureType(Expression expr) {
        if (expr instanceof StructLiteralExpression struct) return struct.className();
        if (expr instanceof SqlFunctionCall func) {
            for (Expression arg : func.arguments()) {
                String type = extractPureType(arg);
                if (type != null) return type;
            }
        }
        if (expr instanceof ListLiteral list && !list.isEmpty()) return extractPureType(list.elements().getFirst());
        if (expr instanceof ListFilterExpression filter) return extractPureType(filter.source());
        if (expr instanceof SqlCollectionCall coll) return extractPureType(coll.source());
        return null;
    }

    /**
     * Finds the root InstanceExpression array in an AST.
     */
    @SuppressWarnings("unused")
    private ArrayLiteral findInstanceArray(PureExpression expr) {
        return switch (expr) {
            case ArrayLiteral arr when !arr.elements().isEmpty()
                    && arr.elements().get(0) instanceof InstanceExpression ->
                arr;
            case org.finos.legend.pure.dsl.MethodCall mc -> findInstanceArray(mc.source());
            case org.finos.legend.pure.dsl.RelationProjectExpression rpe -> findInstanceArray(rpe.source());
            case org.finos.legend.pure.dsl.RelationFilterExpression rfe -> findInstanceArray(rfe.source());
            case org.finos.legend.pure.dsl.LimitExpression le -> findInstanceArray(le.source());
            case org.finos.legend.pure.dsl.SortExpression se -> findInstanceArray(se.source());
            default -> null;
        };
    }
}
