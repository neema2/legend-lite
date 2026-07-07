package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedAsOfJoin;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.LambdaFunction;

import java.util.Optional;

/**
 * {@code asOfJoin} (engine {@code AsOfJoinChecker}) &mdash; a temporal join whose
 * {@code match} picks the as-of row plus an optional additional condition;
 * checked generically, output {@code T+V} (a name collision fails loudly).
 *
 * <p>The 5-argument {@code prefix} overload resolves collisions: EVERY
 * right-side column is renamed {@code prefix + name} &mdash; the SAME rule as
 * {@code join}. <strong>Deliberate divergence from engine-lite</strong>, which
 * prefixes only the overlapping columns here while join prefixes all of them;
 * two prefix semantics for one concept is an engine inconsistency we do not
 * carry. The renaming is beyond the signature's {@code T+V} algebra, so the
 * prefix path validates each argument against the registered 5-arity signature
 * and computes the prefixed union bespoke.
 */
final class AsOfJoinChecker {

    private AsOfJoinChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        if (af.parameters().size() == 5) {
            return withPrefix(t, af, env);
        }
        Application a = t.checkGeneric(af, env);
        if (a.args().size() < 3 || a.args().size() > 4
                || !(a.args().get(2) instanceof TypedLambda match)) {
            throw new TypeInferenceException(
                    "asOfJoin expects (rel1, rel2, {t,v|match} [, {t,v|cond} [, 'prefix']])");
        }
        Optional<TypedLambda> cond = a.args().size() >= 4
                ? Optional.of((TypedLambda) a.args().get(3)) : Optional.empty();
        return new TypedAsOfJoin(a.args().get(0), a.args().get(1), match, cond,
                Optional.empty(), a.out());
    }

    private static TypedSpec withPrefix(Typer t, AppliedFunction af, Env env) {
        TypedFunction sig = t.model().findFunction(CoreFn.AS_OF_JOIN.parseName()).stream()
                .filter(c -> c.parameters().size() == 5)
                .findFirst()
                .orElseThrow(() -> new TypeInferenceException(
                        "no 5-argument asOfJoin overload is registered"));

        Bindings b = new Bindings();
        TypedSpec left = Checkers.unifiedArg(t, sig, 0, af, b, env);
        TypedSpec right = Checkers.unifiedArg(t, sig, 1, af, b, env);
        if (!(af.parameters().get(2) instanceof LambdaFunction matchLam)
                || !(af.parameters().get(3) instanceof LambdaFunction condLam)) {
            throw new TypeInferenceException(
                    "asOfJoin expects (rel1, rel2, {t,v|match}, {t,v|cond}, 'prefix')");
        }
        TypedLambda match = (TypedLambda) t.typeLambda(matchLam, sig.parameters().get(2).type(), b, env);
        TypedLambda cond = (TypedLambda) t.typeLambda(condLam, sig.parameters().get(3).type(), b, env);
        String prefix = Checkers.stringLiteralArg(t, af, 4, env, "asOfJoin prefix");

        // Bespoke output: EVERY right column prefixed — consistent with join (see class doc).
        Type.RelationType schema = Checkers.prefixedUnion(left, right, prefix, c -> true);
        return new TypedAsOfJoin(left, right, match, Optional.of(cond), Optional.of(prefix),
                new ExprType(schema, sig.returnMultiplicity()));
    }
}
