package com.legend.compiler.spec.typed;

import com.legend.compiler.spec.ExprType;

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
}
