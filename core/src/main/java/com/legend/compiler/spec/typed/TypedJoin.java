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
                        @com.legend.base.Nullable String frameName, ExprType info,
                        boolean userCondition) implements TypedSpec {

    // frameName: the RIGHT side's derived-table identity (a view-backed
    // target's view name) — null for anonymous targets. NO short overload:
    // a defaulted frameName silently anonymized view-backed targets at
    // rebuild sites (remediation T2.2); every construction names every
    // field.
    //
    // userCondition: TRUE only for the relation-surface join() lambda
    // (JoinChecker) — the engine's nullSafeEqualsOperation grants
    // [0..1]==[0..1] null-safe equality to USER pure expressions
    // (witness testJoinOnNullKey: pure joins null keys); a
    // RESOLVER-SYNTHESIZED navigation join is the MAPPING's own
    // definition and stays verbatim plain '=' (the engine emits mapping
    // Join operations as defined — slotDemandJoins' golden pins it).
    // NO defaulting overload (this record's own T2.2 rule): a defaulted
    // userCondition silently DROPPED the null-safe grant at rebuild
    // sites the day it was introduced — every construction names every
    // field: rebuilds PRESERVE the source join's flag, synthesis says
    // false.

    @Override
    public List<TypedSpec> children() {
        return List.of(left, right, kind, condition);
    }

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 4, "TypedJoin");
        return new TypedJoin(kids.get(0), kids.get(1), (TypedEnumValue) kids.get(2),
                (TypedLambda) kids.get(3), prefix, frameName, info,
                userCondition);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedJoin(left, right, kind, condition, prefix, frameName, info, userCondition);
    }
}
