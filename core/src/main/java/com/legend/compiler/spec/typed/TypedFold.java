package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.ArrayList;
import java.util.List;

/**
 * A checked {@code fold<T,V>(source:T[*], {t,acc|…}, init:V[1]):V[1]} (engine
 * {@code FoldChecker}) &mdash; typed generically ({@code V} binds from the init,
 * the reducer checks against {@code {T[1],V[1]->V[1]}}), then classified into a
 * {@link FoldStrategy} for lowering.
 *
 * @param source   the collection being folded
 * @param reducer  the checked two-parameter reduction lambda
 * @param init     the initial accumulator value
 * @param strategy the classified lowering strategy
 * @param info     {@code V[1]}
 */
public record TypedFold(TypedSpec source, TypedLambda reducer, TypedSpec init,
                        FoldStrategy strategy, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        List<TypedSpec> out = new ArrayList<>(List.of(source, reducer, init));
        if (strategy instanceof FoldStrategy.MapReduce mr) {
            out.add(mr.transform());
            out.add(mr.reducer());
        }
        return out;
    }

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        boolean mr = strategy instanceof FoldStrategy.MapReduce;
        TypedSpec.expectChildren(kids, mr ? 5 : 3, "TypedFold");
        FoldStrategy st = mr
                ? new FoldStrategy.MapReduce((TypedLambda) kids.get(3),
                        (TypedLambda) kids.get(4))
                : strategy;
        return new TypedFold(kids.get(0), (TypedLambda) kids.get(1), kids.get(2), st, info);
    }

    /** The per-row element expression of a COLUMN-COLLECT fold over a
     * relation — {@code fold({e,a| concatenate(elemExpr, $a)}, [])}
     * with a relation-typed source (Phase 1c: the corpus's grid idiom;
     * semantically the per-row map, which is how it lowers). Null
     * otherwise. */
    public @com.legend.base.Nullable TypedSpec columnCollectBody() {
        if (!com.legend.compiler.element.type.Type
                        .relationValued(source.info())
                || reducer.parameters().size() != 2
                || reducer.body().isEmpty()
                || !(init instanceof TypedCollection ic)
                || !ic.elements().isEmpty()) {
            return null;
        }
        TypedSpec body = reducer.body().get(reducer.body().size() - 1);
        if (body instanceof TypedNativeCall cc
                && "meta::pure::functions::collection::concatenate"
                        .equals(cc.callee().qualifiedName())
                && cc.args().size() == 2
                && cc.args().get(1) instanceof TypedVariable acc
                && acc.name().equals(reducer.parameters().get(1))) {
            return cc.args().get(0);
        }
        return null;
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedFold(source, reducer, init, strategy, info);
    }
}
