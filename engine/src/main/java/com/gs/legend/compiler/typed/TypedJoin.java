package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;
import com.gs.legend.compiler.NativeFunctionDef;

import java.util.Map;

/**
 * Relational join: {@code left->join(right, JoinKind.LEFT, {l, r | cond})}.
 *
 * <p>{@code joinType} is an enum, not a string. {@code renames} carries the
 * column-rename map applied when the right side's columns collide with the left.
 */
public record TypedJoin(
        TypedSpec left,
        TypedSpec right,
        TypedLambda condition,
        JoinType joinType,
        Map<String, String> renames,
        NativeFunctionDef def,
        ExpressionType info
) implements TypedNative {
    public TypedJoin {
        renames = Map.copyOf(renames);
    }
}
