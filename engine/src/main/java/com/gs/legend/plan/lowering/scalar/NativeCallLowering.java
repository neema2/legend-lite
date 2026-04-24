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

            // Arithmetic — {@code plus} is overloaded on String (concat) and
            // numeric types. Legacy {@code generateScalarFunctionCall} checked
            // TypeInfo on the operands; we mirror that via the typed args'
            // {@link com.gs.legend.compiler.ExpressionType}. Unary {@code plus}
            // / {@code minus} are pass-through and negation respectively
            // (legacy line 2702-2750).
            case "plus"                         -> lowerPlus(n.args(), args);
            case "minus", "sub"                 -> lowerMinus(args);
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

    /**
     * Pure {@code plus}: unary pass-through, binary string-concat ({@code ||}),
     * or binary numeric add. Dispatch uses the typed args' static type —
     * AGENTS.md invariant #2 forbids type inference here, but reading
     * compiler-stamped {@link com.gs.legend.compiler.ExpressionType} from
     * {@link TypedSpec#info()} is structural look-up, not inference.
     */
    private static SqlExpr lowerPlus(List<TypedSpec> typedArgs, List<SqlExpr> args) {
        if (args.size() == 1) return args.get(0);
        if (args.size() != 2) {
            throw new IllegalStateException(
                    "[plangen-c0954a] plus expects 1 or 2 args, got " + args.size());
        }
        boolean isStringConcat = false;
        for (TypedSpec t : typedArgs) {
            var info = t.info();
            if (info != null && info.type() == com.gs.legend.model.m3.Primitive.STRING) {
                isStringConcat = true;
                break;
            }
        }
        return new SqlExpr.Binary(args.get(0), isStringConcat ? "||" : "+", args.get(1));
    }

    /** Unary {@code minus} → {@code (-1 * x)}; binary → {@code a - b}. */
    private static SqlExpr lowerMinus(List<SqlExpr> args) {
        if (args.size() == 1) {
            return new SqlExpr.Binary(new SqlExpr.NumericLiteral(-1), "*", args.get(0));
        }
        if (args.size() != 2) {
            throw new IllegalStateException(
                    "[plangen-c0954a] minus expects 1 or 2 args, got " + args.size());
        }
        return new SqlExpr.Binary(args.get(0), "-", args.get(1));
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
