package com.legend.compiler.spec.typed;

import com.legend.compiler.spec.ExprType;

import java.util.List;

import java.util.Optional;

/**
 * A type-checked {@code asOfJoin} (engine {@code AsOfJoinChecker}) &mdash;
 * {@code asOfJoin<T,V>(rel1, rel2, match:{T[1],V[1]->Boolean[1]}
 * [, join:{T[1],V[1]->Boolean[1]}]):Relation<T+V>[1]}: a temporal join whose
 * {@code match} picks the as-of row and whose optional {@code join} adds an
 * equi-join condition. Output schema is the union {@code T+V}, signature-computed.
 *
 * @param left      the left relation
 * @param right     the right (temporal) relation
 * @param match     the as-of match condition
 * @param condition the optional additional join condition
 * @param prefix    the optional right-side column prefix (the 5-argument overload);
 *                  when present, EVERY right column is renamed — same rule as join
 *                  (deliberate divergence from engine-lite's overlap-only prefixing)
 * @param info      the result &mdash; {@code T+V} resolved
 */
public record TypedAsOfJoin(TypedSpec left, TypedSpec right, TypedLambda match,
                            Optional<TypedLambda> condition, Optional<String> prefix,
                            ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return condition
                .map(c -> List.of(left, right, match, c))
                .orElseGet(() -> List.of(left, right, match));
    }
}
