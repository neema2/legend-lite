package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * The pipeline slot join {@code rel->join(~alias: #>{db.T}#, {s,t|cond})}
 * (lite; no real pure counterpart &mdash; real {@code relation::join} takes
 * two relations + a JoinKind). This is the mapping normalizer's join-chain
 * step (MAPPING_LEGACY_TO_FUNCTION.md): the slot's thunk names the TARGET
 * TABLE, and the output row gains an {@code alias} column carrying the
 * joined sub-row &mdash; typed as the target's row schema so
 * {@code $row.alias.COL} reads compose through ordinary member access.
 * Result: {@code Relation<S + (alias:TargetRow[1])>}.
 *
 * <p>Store-only: the Phase H resolver rewrites this into a physical join;
 * it never reaches the lowerer directly (which fails loud if it does).
 *
 * @param source    the pipeline relation being widened
 * @param alias     the sub-row slot name (the normalizer's chain alias)
 * @param target    the slot thunk's body &mdash; the target table reference
 * @param condition the checked join condition {@code {s,t|...}}
 * @param info      {@code Relation<S + (alias:TargetRow[1])>[1]}
 */
public record TypedJoinSlot(TypedSpec source, String alias, TypedSpec target,
                            TypedLambda condition,
                            @com.legend.base.Nullable String frameName,
                            ExprType info) implements TypedSpec {

    /** frameName: the target's derived-table identity (a VIEW hop's view
     * name) — null for physical-table hops. */
    public TypedJoinSlot(TypedSpec source, String alias, TypedSpec target,
            TypedLambda condition, ExprType info) {
        this(source, alias, target, condition, null, info);
    }

    @Override
    public List<TypedSpec> children() {
        return List.of(source, target, condition);
    }

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 3, "TypedJoinSlot");
        return new TypedJoinSlot(kids.get(0), alias, kids.get(1),
                (TypedLambda) kids.get(2), frameName, info);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedJoinSlot(source, alias, target, condition, frameName, info);
    }
}
