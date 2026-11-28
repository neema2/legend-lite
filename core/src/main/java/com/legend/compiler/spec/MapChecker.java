package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;

/**
 * {@code map} (engine {@code MapChecker}) &mdash; checked generically (the
 * signature's {@code V} binds from the mapper body); this class only emits the
 * construct node.
 */
final class MapChecker {

    private MapChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        com.legend.compiler.element.type.ExprType out = a.out();
        // map over a RELATION value is a ROW map: the result cardinality is
        // the (unknown) row count, never the relation VALUE's multiplicity —
        // a Relation[1] source (the Result.values envelope, audit 19d B2)
        // must not type its row map [0..1].
        if (com.legend.compiler.element.type.Type
                .isRelation(a.args().get(0).info().type())) {
            out = new com.legend.compiler.element.type.ExprType(out.type(),
                    com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_MANY);
        }
        // HIGHER-ORDER map: a function-VALUED argument ($f->map($func) in a
        // user fn body typed generically) has no literal lambda yet — emit
        // the plain call node; UserCallInliner β-reduces or rebuilds the
        // TypedMap once substitution reveals the literal (same family as
        // its eval rule). An un-substituted survivor stays loud at the
        // lowerer's TypedNativeCall wall.
        if (!(a.args().get(1)
                instanceof com.legend.compiler.spec.typed.TypedLambda)) {
            return CallNodes.mint(t.ctx().implementations(), a.chosen(), a.args(), out);
        }
        com.legend.compiler.spec.typed.TypedLambda function = Args.lambda(a, 1);
        return new TypedMap(a.args().get(0), function, countedByItsFunction(a.args().get(0), function, out));
    }

    /**
     * A MAP'S COUNT FOLLOWS WHAT ITS FUNCTION MAY ANSWER (the user's ruling of 2026-11-28, "Yes fix all map
     * family"). legend-pure declares three maps: over {@code T[m]} with a function that answers exactly one
     * ({@code V[m]}), over {@code T[0..1]} with one that answers at most one ({@code V[0..1]}), and over
     * {@code T[*]} with one that answers any number ({@code V[*]}). A function that may answer NOTHING for an
     * element ({@code x | if($x == 1, |1, |[])}) was typed by the first -- the source's own count -- so
     * {@code [1, 2]->map(x | if($x == 1, |1, |[]))} was declared to hold exactly two values where it holds one
     * (measured 2026-11-28: the host judge refused such a list by its declared size). Here the count is the
     * signature's that fits the function: at most one over at most one, else any number. A function that answers
     * exactly one keeps the count the signature chosen gives, as before.
     */
    private static com.legend.compiler.element.type.ExprType countedByItsFunction(TypedSpec source,
            com.legend.compiler.spec.typed.TypedLambda function, com.legend.compiler.element.type.ExprType out) {
        if (function.body().isEmpty() || com.legend.compiler.element.type.Type.isRelation(source.info().type())
                || !(function.body().get(function.body().size() - 1).info().multiplicity()
                        instanceof com.legend.compiler.element.type.Multiplicity.Bounded answers)
                || !(out.multiplicity() instanceof com.legend.compiler.element.type.Multiplicity.Bounded declared)) {
            return out;
        }
        boolean exactlyOne = answers.lower() == 1 && answers.upper() != null && answers.upper() == 1;
        boolean mayAnswerNothing = answers.lower() == 0;
        // (only where the declared count promises more than the function gives: a count that already allows none
        // and has no upper bound says nothing false)
        if (exactlyOne || !mayAnswerNothing || declared.lower() == 0) {
            return out;
        }
        boolean atMostOne = declared.upper() != null && declared.upper() <= 1 && answers.upper() != null
                && answers.upper() <= 1;
        return new com.legend.compiler.element.type.ExprType(out.type(), atMostOne
                ? com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_ONE
                : answers.upper() != null && answers.upper() <= 1 && declared.upper() != null
                        ? new com.legend.compiler.element.type.Multiplicity.Bounded(0, declared.upper())
                        : com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_MANY);
    }
}
