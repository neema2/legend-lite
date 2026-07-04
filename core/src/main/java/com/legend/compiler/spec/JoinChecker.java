package com.legend.compiler.spec;

import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedEnumValue;
import com.legend.compiler.spec.typed.TypedJoin;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.LambdaFunction;

import java.util.Optional;

/**
 * Relation {@code join} (engine {@code JoinChecker}) &mdash; checked generically
 * against {@code join<T,V>(rel1, rel2, joinKind:JoinKind[1],
 * f:{T[1],V[1]->Boolean[1]}):Relation<T+V>[1]}: the condition lambda sees one row
 * of each side; the output schema is the union {@code T+V} (a name collision is
 * a loud error &mdash; real legend-pure's rule).
 *
 * <p>The 5-argument {@code prefix} overload exists exactly to resolve such
 * collisions: EVERY right-side column is renamed {@code prefix + name} in the
 * output (engine's behavior). The prefix renaming is beyond the signature's
 * {@code T+V} algebra, so that path validates each argument against the
 * registered 5-arity signature and computes the prefixed union bespoke.
 */
final class JoinChecker {

    private JoinChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        if (af.parameters().size() == 5) {
            return withPrefix(t, af, env);
        }
        Application a = t.checkGeneric(af, env);
        if (a.args().size() != 4 || !(a.args().get(2) instanceof TypedEnumValue kind)
                || !(a.args().get(3) instanceof TypedLambda cond)) {
            throw new TypeInferenceException(
                    "join expects (rel1, rel2, JoinKind, {t,v|cond} [, 'prefix'])");
        }
        return new TypedJoin(a.args().get(0), a.args().get(1), kind, cond, Optional.empty(), a.out());
    }

    private static TypedSpec withPrefix(Typer t, AppliedFunction af, Env env) {
        TypedFunction sig = t.model().findFunction(CoreFn.JOIN.parseName()).stream()
                .filter(c -> c.parameters().size() == 5)
                .findFirst()
                .orElseThrow(() -> new TypeInferenceException(
                        "no 5-argument join overload is registered"));

        // Validate every argument against the registered signature (never bypassed);
        // the condition lambda types against the signature's function parameter with
        // T and V already bound from the two sides.
        Bindings b = new Bindings();
        TypedSpec left = Checkers.unifiedArg(t, sig, 0, af, b, env);
        TypedSpec right = Checkers.unifiedArg(t, sig, 1, af, b, env);
        TypedSpec kindArg = Checkers.unifiedArg(t, sig, 2, af, b, env);
        if (!(af.parameters().get(3) instanceof LambdaFunction condLam)
                || !(kindArg instanceof TypedEnumValue kind)) {
            throw new TypeInferenceException(
                    "join expects (rel1, rel2, JoinKind, {t,v|cond}, 'prefix')");
        }
        TypedLambda cond = (TypedLambda) t.typeLambda(condLam, sig.parameters().get(3).type(), b, env);
        String prefix = Checkers.stringLiteralArg(t, af, 4, env, "join prefix");

        // Bespoke output: left columns + EVERY right column renamed prefix+name.
        Type.RelationType schema = Checkers.prefixedUnion(left, right, prefix, c -> true);
        return new TypedJoin(left, right, kind, cond, Optional.of(prefix),
                new ExprType(schema, sig.returnMultiplicity()));
    }
}
