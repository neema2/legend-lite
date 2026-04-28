package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedConcatenate;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.model.m3.Primitive;
import com.gs.legend.model.m3.Type;


import java.util.stream.Collectors;

/**
 * Signature-driven type checker for {@code concatenate()}.
 *
 * <p>Signature: {@code concatenate<T>(rel1:Relation<T>[1], rel2:Relation<T>[1]):Relation<T>[1]}
 *
 * <p>Three code paths:
 * <ol>
 *   <li><b>Scalar list</b>: non-relational sources → list concatenation</li>
 *   <li><b>Class LCA</b>: different class-based sources → find lowest common ancestor</li>
 *   <li><b>Relational</b>: same-schema sources → strict column alignment</li>
 * </ol>
 *
 * <p>Uses {@link #resolveClassLCA} from AbstractChecker for class hierarchy resolution.
 * Both "concatenate" and "union" route here (both produce UNION ALL).
 */
public class ConcatenateChecker extends AbstractChecker {

    public ConcatenateChecker(TypeCheckEnv env) {
        super(env);
    }

    @Override
    public TypedConcatenate check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        var params = af.parameters();
        NativeFunctionDef def = resolveOverload("concatenate", params, source);
        TypedSpec left = source;
        TypedSpec right = env.compileExpr(params.get(1), ctx);

        // Scalar list concatenation: [1,2]->concatenate([3,4]).
        if (!left.isRelation()) {
            if (left.type() == null) {
                throw new PureCompileException(
                        "concatenate(): cannot determine type of left source");
            }
            return new TypedConcatenate(left, right, def, ExpressionType.many(left.type()));
        }

        // Bind type variables from signature (T from left source).
        var bindings = unify(def, left.expressionType());

        Type.Schema leftSchema = left.schema();
        Type.Schema rightSchema = right.schema();

        // Class LCA: different class-based sources with different columns.
        if (left.type() instanceof Type.ClassType
                && right.type() instanceof Type.ClassType
                && leftSchema != null && rightSchema != null
                && !leftSchema.columns().keySet().equals(rightSchema.columns().keySet())) {
            ExpressionType lca = resolveClassLCA(left, right);
            // No common supertype — fall back to an untyped variant list.
            ExpressionType out = lca != null ? lca : ExpressionType.many(Primitive.ANY);
            return new TypedConcatenate(left, right, def, out);
        }

        // Relational: strict column alignment; output from signature (Relation<T>[1]).
        if (leftSchema != null && rightSchema != null) {
            validateColumnAlignment(leftSchema, rightSchema);
        }
        return new TypedConcatenate(left, right, def,
                resolveOutput(def, bindings, "concatenate()"));
    }

    /**
     * Validates column alignment between left and right schemas.
     * Reports symmetric diff — both left-only and right-only columns.
     */
    private void validateColumnAlignment(Type.Schema left,
                                          Type.Schema right) {
        var leftCols = left.columns().keySet();
        var rightCols = right.columns().keySet();

        // Check both directions for clear error messages
        var leftOnly = leftCols.stream().filter(c -> !rightCols.contains(c)).collect(Collectors.toSet());
        var rightOnly = rightCols.stream().filter(c -> !leftCols.contains(c)).collect(Collectors.toSet());

        if (!leftOnly.isEmpty() || !rightOnly.isEmpty()) {
            var sb = new StringBuilder("concatenate(): column mismatch —");
            if (!leftOnly.isEmpty()) {
                sb.append(" left-only: ").append(leftOnly);
            }
            if (!rightOnly.isEmpty()) {
                if (!leftOnly.isEmpty()) sb.append(",");
                sb.append(" right-only: ").append(rightOnly);
            }
            throw new PureCompileException(sb.toString());
        }
    }
}
