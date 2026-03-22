package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

import java.util.Map;
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

    public TypeInfo check(AppliedFunction af, TypeInfo left, TypeInfo right,
                          TypeChecker.CompilationContext ctx, NativeFunctionDef def) {

        // 1. Scalar list concatenation: [1,2]->concatenate([3,4])
        if (!left.isRelational()) {
            if (left.type() == null) {
                throw new PureCompileException("concatenate(): cannot determine type of left source");
            }
            GenericType elemType = left.type().isList() && left.type().elementType() != null
                    ? left.type().elementType()
                    : left.type();
            return TypeInfo.builder()
                    .expressionType(ExpressionType.many(GenericType.listOf(elemType)))
                    .build();
        }

        // 2. Bind type variables from signature (T from left source)
        Map<String, GenericType> bindings = unify(def, left.expressionType());

        GenericType.Relation.Schema leftSchema = left.schema();
        GenericType.Relation.Schema rightSchema = right.schema();

        // 3. Class LCA: different class-based sources with different columns
        if (left.mapping() != null && right.mapping() != null
                && leftSchema != null && rightSchema != null
                && !leftSchema.columns().keySet().equals(rightSchema.columns().keySet())) {
            TypeInfo lca = resolveClassLCA(left, right);
            if (lca != null) return lca;
            // No common supertype — variant list fallback
            return TypeInfo.builder()
                    .expressionType(ExpressionType.one(GenericType.listOf(GenericType.Primitive.ANY)))
                    .build();
        }

        // 4. Relational: strict column alignment
        if (leftSchema != null && rightSchema != null) {
            validateColumnAlignment(leftSchema, rightSchema);
        }

        // Output from signature: Relation<T>[1]
        return TypeInfo.builder()
                .mapping(left.mapping())
                .expressionType(resolveOutput(def, bindings, "concatenate()"))
                .build();
    }

    /**
     * Validates column alignment between left and right schemas.
     * Reports symmetric diff — both left-only and right-only columns.
     */
    private void validateColumnAlignment(GenericType.Relation.Schema left,
                                          GenericType.Relation.Schema right) {
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
