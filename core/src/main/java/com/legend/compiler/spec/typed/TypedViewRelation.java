// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec.typed;

import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;
import com.legend.model.FunctionDefinition;
import com.legend.model.SynthHat;

import java.util.List;

/**
 * A store VIEW as a NAMED relation — the inlined body of the view's lifted
 * zero-arg function ({@code <db>$view$<name>}, the normalizer's E.5 lift)
 * with the view's name kept on it. The engine's own node is
 * {@code ViewSelectSQLQuery(view, name, select)}: a view is planned as an
 * inline derived table aliased by its name ({@code personview_0}), its
 * root table {@code "root"} inside. The mapping route carries the same
 * fact on a join hop ({@link TypedJoinSlot#frameName()} /
 * {@link TypedJoin#frameName()}); this node carries it at ROOT position —
 * the relation accessor {@code #>{db.view}#}.
 *
 * <p>Minted by the user-call inliner when the callee is a lifted view
 * (every lowering path inlines first). Lowering: the ENGINE-TEXT render
 * emits the body as a subselect named by the view (the alias plan groups
 * it by that name); the product render is transparent — the body lowers
 * flat (lean SQL: a name is not a nesting level).
 *
 * @param view the view's bare name (a schema-qualified view's own name —
 *             the engine's alias groups take the bare name)
 * @param body the view's relation (the lifted function's inlined body)
 * @param info the body's relation type
 */
public record TypedViewRelation(String view, TypedSpec body, ExprType info)
        implements TypedSpec {

    @Override
    public List<TypedSpec> children() {
        return List.of(body);
    }

    @Override
    public TypedSpec withChildren(List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 1, "TypedViewRelation");
        return new TypedViewRelation(view, kids.get(0), info);
    }

    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedViewRelation(view, body, info);
    }

    /** The view's bare name when {@code fn} is a lifted view function
     *  (E.5 provenance, {@link SynthHat#VIEW}: the member IS the view's
     *  name); null for every other callee. ONE reader of the provenance. */
    public static @com.legend.base.Nullable String liftedViewName(TypedFunction fn) {
        if (!(fn.definition() instanceof FunctionDefinition fd)
                || fd.synthesizedFrom() == null
                || fd.synthesizedFrom().hat() != SynthHat.VIEW) {
            return null;
        }
        return fd.synthesizedFrom().memberName();
    }

    /** THE one mint: the lifted view {@code fn}'s relation, its compiled
     *  (or inlined) {@code body} named by the view. */
    public static TypedViewRelation of(TypedFunction fn, TypedSpec body) {
        String view = liftedViewName(fn);
        if (view == null) {
            throw new IllegalArgumentException("not a lifted view function: " + fn.qualifiedName());
        }
        return new TypedViewRelation(view, body, body.info());
    }
}
