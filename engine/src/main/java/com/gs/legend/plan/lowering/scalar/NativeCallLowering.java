package com.gs.legend.plan.lowering.scalar;

import com.gs.legend.compiler.typed.TypedNativeCall;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.plan.PlanGenNotPortedException;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.ArrayList;
import java.util.List;

/**
 * Lowers a {@link TypedNativeCall} (built-in function application) to a
 * {@link SqlExpr} tree.
 *
 * <p><strong>Stage 3 scope</strong>: core comparison, arithmetic and logical
 * operators plus the most common scalar built-ins (string / null / collection
 * tests). Each group maps to a structural {@link SqlExpr} variant
 * (e.g. {@link SqlExpr.Binary}, {@link SqlExpr.And}, {@link SqlExpr.Not}).
 * Rendering is still dialect-deferred: no dialect calls happen here.
 *
 * <p>Arithmetic / functions not yet covered throw
 * {@link PlanGenNotPortedException} tagged {@code stage-3-native:&lt;funcName&gt;}
 * so progress can be measured per-function in test runs.
 */
public final class NativeCallLowering {
    private NativeCallLowering() {}

    public static SqlExpr lower(TypedNativeCall n, LoweringContext ctx) {
        String name = n.func().name();
        List<SqlExpr> args = lowerArgs(n.args(), ctx);
        return switch (name) {
            // Comparison
            case "equal", "eq"                  -> binary(args, "=");
            case "notEqual"                     -> binary(args, "<>");
            case "greaterThan"                  -> binary(args, ">");
            case "lessThan"                     -> binary(args, "<");
            case "greaterThanEqual"             -> binary(args, ">=");
            case "lessThanEqual"                -> binary(args, "<=");

            // Logical
            case "and"                          -> new SqlExpr.And(args);
            case "or"                           -> new SqlExpr.Or(args);
            case "not"                          -> new SqlExpr.Not(requireArity(args, 1, name).get(0));

            // Arithmetic
            case "plus"                         -> binary(args, "+");
            case "minus"                        -> binary(args, "-");
            case "times"                        -> binary(args, "*");
            case "divide"                       -> binary(args, "/");

            // Null / presence
            case "isEmpty", "isNull"            -> new SqlExpr.IsNull(requireArity(args, 1, name).get(0));
            case "isNotEmpty", "isNotNull"      -> new SqlExpr.IsNotNull(requireArity(args, 1, name).get(0));

            // Collection membership
            case "in"                           -> new SqlExpr.In(args.get(0), args.subList(1, args.size()));

            // Everything else: emit a structural {@code FunctionCall} carrying the
            // Pure native name. {@link SqlExpr.FunctionCall#toSql} delegates to
            // {@link com.gs.legend.sqlgen.SQLDialect#renderFunction}, which is
            // per-AGENTS.md the single source of truth for function-name mapping
            // and dialect-specific decompositions (e.g., {@code dateDiff},
            // {@code timeBucket}, {@code lpadSafe}). No name shimming here.
            default -> new SqlExpr.FunctionCall(name, args);
        };
    }

    private static List<SqlExpr> lowerArgs(List<TypedSpec> args, LoweringContext ctx) {
        List<SqlExpr> out = new ArrayList<>(args.size());
        for (TypedSpec a : args) out.add(Lowerer.lowerScalar(a, ctx));
        return out;
    }

    private static SqlExpr binary(List<SqlExpr> args, String op) {
        if (args.size() != 2) {
            throw new IllegalStateException(
                    "[plangen-c0954a] binary '" + op + "' expects 2 args, got " + args.size());
        }
        return new SqlExpr.Binary(args.get(0), op, args.get(1));
    }

    private static List<SqlExpr> requireArity(List<SqlExpr> args, int n, String name) {
        if (args.size() != n) {
            throw new IllegalStateException(
                    "[plangen-c0954a] native '" + name + "' expects " + n + " args, got " + args.size());
        }
        return args;
    }
}
