package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedMatch;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.LambdaFunction;
import com.legend.parser.spec.PureCollection;
import com.legend.parser.spec.ValueSpecification;
import com.legend.parser.spec.Variable;

import java.util.List;
import java.util.Optional;

/**
 * {@code match(value, [t:Type[m]|…, …])} (engine {@code MatchChecker}) &mdash;
 * <strong>static dispatch</strong>: the first branch whose declared parameter
 * type accepts the input (subtype-aware: {@code Integer} matches a
 * {@code Number} branch, {@code Employee} a {@code Person} branch) is selected
 * at compile time, its body checked with the parameter bound to the
 * <em>branch's declared</em> (narrowed) type, and the result is the body's type
 * &mdash; not the registered signature's {@code Any[*]} collapse.
 *
 * <p>Like {@code if}, {@code match} cannot run full {@code resolveOverload}: its
 * branches are function values the plain path can't unify, and its result is the
 * selected branch's, which the signature can't express. The registered signature
 * still governs the shape (value + at-least-one branch).
 */
final class MatchChecker {

    private MatchChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        List<ValueSpecification> params = af.parameters();
        // Arity is governed by the REGISTERED overloads (the class doc's
        // claim, previously hardcoded — audit finding): match(Any[*],
        // Function[*]) and match(Any[*], Function[*], Any[*]).
        boolean arityRegistered = t.model().findFunction(CoreFn.MATCH.parseName()).stream()
                .anyMatch(f -> f.parameters().size() == params.size());
        if (!arityRegistered) {
            throw new TypeInferenceException("no registered 'match' overload accepts "
                    + params.size() + " argument(s)");
        }
        TypedSpec input = t.synth(params.get(0), env);
        Optional<TypedSpec> extra = params.size() == 3
                ? Optional.of(t.synth(params.get(2), env)) : Optional.empty();

        for (LambdaFunction branch : branches(params.get(1))) {
            if (branch.parameters().isEmpty() || branch.parameters().size() > 2
                    || branch.body().size() != 1) {
                throw new TypeInferenceException(
                        "a match branch must be a one- or two-parameter, single-expression lambda");
            }
            if (branch.parameters().size() == 2 && extra.isEmpty()) {
                throw new TypeInferenceException("a two-parameter match branch needs an extra argument");
            }
            Variable param = branch.parameters().get(0);
            if (param.type() == null) {
                throw new TypeInferenceException(
                        "match branch parameter '" + param.name() + "' needs a declared type (t:Type[m]|…)");
            }
            Type branchType = t.namedType(param.type());
            if (!t.kernel().accepts(branchType, input.info().type())
                    || !multAccepts(param, input.info().multiplicity())) {
                continue;
            }
            // The body sees the parameter at the branch's DECLARED (narrowed) type — that
            // is the point of match — at the branch's declared multiplicity (or the input's).
            Multiplicity bound = param.multiplicity() != null
                    ? Multiplicity.from(param.multiplicity()) : input.info().multiplicity();
            Env scope = env.with(param.name(), new ExprType(branchType, bound));
            Optional<String> extraParam = Optional.empty();
            if (branch.parameters().size() == 2) {
                Variable second = branch.parameters().get(1);
                ExprType extraBound = second.type() != null
                        ? new ExprType(t.namedType(second.type()),
                                second.multiplicity() != null
                                        ? Multiplicity.from(second.multiplicity())
                                        : extra.get().info().multiplicity())
                        : extra.get().info();
                scope = scope.with(second.name(), extraBound);
                extraParam = Optional.of(second.name());
            }
            TypedSpec body = t.synth(branch.body().get(0), scope);
            return new TypedMatch(input, param.name(), body, extraParam,
                    extraParam.isPresent() ? extra : Optional.empty(), body.info());
        }
        throw new TypeInferenceException("match: no branch matches input type '"
                + input.info().type().typeName() + input.info().multiplicity().text() + "'");
    }

    /** A many-valued input needs a branch whose declared multiplicity can hold it (engine rule). */
    private static boolean multAccepts(Variable param, Multiplicity inputMult) {
        if (!(inputMult instanceof Multiplicity.Bounded in) || in.upper() != null && in.upper() <= 1) {
            return true;   // to-one input: any branch multiplicity accepts
        }
        if (param.multiplicity() == null) {
            return true;
        }
        return Multiplicity.from(param.multiplicity()).isMany();
    }

    /** Branches: a collection of lambdas, or one bare lambda. */
    private static List<LambdaFunction> branches(ValueSpecification vs) {
        if (vs instanceof LambdaFunction lf) {
            return List.of(lf);
        }
        if (vs instanceof PureCollection c && !c.values().isEmpty()
                && c.values().stream().allMatch(v -> v instanceof LambdaFunction)) {
            return c.values().stream().map(v -> (LambdaFunction) v).toList();
        }
        throw new TypeInferenceException("match expects a collection of branch lambdas");
    }
}
