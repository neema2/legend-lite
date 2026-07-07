package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A type-checked relation {@code join} (engine {@code JoinChecker}) &mdash;
 * {@code join<T,V>(rel1, rel2, joinKind:JoinKind[1], f:{T[1],V[1]->Boolean[1]})
 * :Relation<T+V>[1]}: the condition lambda sees one row of each side; the output
 * schema is the union {@code T+V}, signature-computed.
 *
 * @param left      the left relation
 * @param right     the right relation
 * @param kind      the join kind ({@code INNER}, {@code LEFT}, …)
 * @param condition the checked two-parameter join condition
 * @param prefix    the right-side column prefix (the 5-argument overload); when present,
 *                  EVERY right column is renamed {@code prefix + name} in the output schema
 * @param info      the result &mdash; {@code T+V} resolved (prefixed on the right when applicable)
 */
public record TypedJoin(TypedSpec left, TypedSpec right, TypedEnumValue kind,
                        TypedLambda condition, java.util.Optional<String> prefix,
                        ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of(left, right, kind, condition);
    }
}
