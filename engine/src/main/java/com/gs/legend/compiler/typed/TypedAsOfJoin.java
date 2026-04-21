package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

import java.util.Map;
import java.util.Optional;

/**
 * Relational as-of join: {@code left->asOfJoin(right, match[, key[, prefix]])}.
 *
 * <p>{@code matchCondition} is the temporal match predicate; {@code keyCondition}
 * is an optional equality predicate (for partitioned as-of). {@code renames}
 * carries any right-side prefix-renamings applied to disambiguate duplicate
 * column names.
 */
public record TypedAsOfJoin(
        TypedSpec left,
        TypedSpec right,
        TypedLambda matchCondition,
        Optional<TypedLambda> keyCondition,
        Map<String, String> renames,
        ExpressionType info
) implements TypedSpec {
    public TypedAsOfJoin {
        renames = Map.copyOf(renames);
    }
}
