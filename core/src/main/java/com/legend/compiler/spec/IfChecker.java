package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.model.spec.AppliedFunction;
import com.legend.model.spec.LambdaFunction;
import com.legend.model.spec.ValueSpecification;

import java.util.List;
import java.util.Optional;

/**
 * {@code if(cond, |then, |else)} (engine {@code IfChecker}): condition is Boolean;
 * result is the branches' common supertype at their common multiplicity.
 *
 * <p>{@code if} cannot run full {@code resolveOverload} &mdash; its branch params
 * are thunks ({@code Function<{->T[m]}>}) that don't match compiled-lambda types,
 * and its result is a <em>join</em> (least upper bound) over the branch types,
 * which unification cannot compute. But it still does <strong>not ignore</strong>
 * the signature: the condition's expected type is <em>sourced</em> from {@code if}'s
 * registered first param (Boolean), not hardcoded, so a contract change tracks here.
 */
final class IfChecker {

    private IfChecker() {
    }

    static TypedIf check(Typer t, AppliedFunction af, Env env) {
        List<ValueSpecification> args = af.parameters();
        // The CONDLIST overload (real if.pure: if(condList:Pair<Function<
        // {->Boolean}>, Function<{->T}>>[*], last)) — its real-pure body
        // FOLDS the pairs into nested if()s; the emission here is that
        // fold, thunks β-reduced (the TypedMatch precedent).
        if (args.size() == 2
                && args.get(0) instanceof com.legend.model.spec.PureCollection pairs) {
            return multiIf(t, pairs, args.get(1), env);
        }
        // A BARE pair is the one-element condList (real if.pure overload
        // if(cond:Pair<...>[1], last) — pair(|c,|v)->if(|else)).
        if (args.size() == 2
                && args.get(0) instanceof AppliedFunction pf
                && (pf.function().equals("pair")
                        || pf.function().equals("meta::pure::functions::collection::pair"))
                && pf.parameters().size() == 2) {
            return multiIf(t, new com.legend.model.spec.PureCollection(
                    List.of(args.get(0))), args.get(1), env);
        }
        TypedSpec cond = t.synth(args.get(0), env);
        var ifSigs = t.model().findFunction(CoreFn.IF.parseName());
        if (ifSigs.isEmpty()) {
            throw new TypeInferenceException("no registered signature for 'if'");
        }
        Type expectedCond = ifSigs.get(0).parameters().get(0).type();   // Boolean, from the signature
        t.kernel().unify(expectedCond, cond.info().type(), new Bindings());   // condition must conform
        // Real Pure: if(test:Boolean[1], ...) — the condition's MULTIPLICITY
        // is part of the signature too; Boolean[0..1]/[*] must not slip by.
        if (!(cond.info().multiplicity() instanceof Multiplicity.Bounded b)
                || b.lower() != 1 || b.upper() == null || b.upper() != 1) {
            throw new TypeInferenceException("if condition must be Boolean[1], got multiplicity "
                    + cond.info().multiplicity());
        }
        TypedSpec thenBranch = thunkBody(t, args.get(1), env);
        Optional<TypedSpec> elseBranch = args.size() > 2
                ? Optional.of(thunkBody(t, args.get(2), env)) : Optional.empty();
        Type result = elseBranch
                .map(e -> t.kernel().commonSupertype(thenBranch.info().type(), e.info().type()))
                .orElse(thenBranch.info().type());
        // Result multiplicity is the branches' shared `m` (real-pure if<T|m>:T[m]) — the common
        // multiplicity of the two branches, NOT a hardcoded [1]. An else-less if is optional (the
        // false path yields nothing). Fixes engine-lite's unconditional [1] (§4.2 flagged bug).
        Multiplicity resultMult = elseBranch
                .map(e -> commonMultiplicity(thenBranch.info().multiplicity(), e.info().multiplicity()))
                .orElse(optional(thenBranch.info().multiplicity()));
        return new TypedIf(cond, thenBranch, elseBranch, new ExprType(result, resultMult));
    }

    /**
     * The condList fold: each element must be a LITERAL pair(|cond, |value);
     * the chain nests right — if(c1, v1, if(c2, v2, last)). Validated against
     * the registered condList signature's shape (two args, pair collection).
     */
    private static TypedIf multiIf(Typer t, com.legend.model.spec.PureCollection pairs,
            ValueSpecification last, Env env) {
        TypedSpec chain = thunkBody(t, last, env);
        Type result = chain.info().type();
        Multiplicity resultMult = chain.info().multiplicity();
        List<ValueSpecification> elements = pairs.values();
        TypedIf out = null;
        for (int i = elements.size() - 1; i >= 0; i--) {
            // EXACT names only — a user function merely ENDING in "pair"
            // must not be hijacked and silently never called (audit; the
            // exact-FQN identification rule).
            if (!(elements.get(i) instanceof AppliedFunction pf)
                    || !(pf.function().equals("pair")
                            || pf.function().equals("meta::pure::functions::collection::pair"))
                    || pf.parameters().size() != 2) {
                throw new TypeInferenceException("if(condList, last) expects literal"
                        + " pair(|cond, |value) elements");
            }
            TypedSpec cond = thunkBody(t, pf.parameters().get(0), env);
            t.kernel().unify(Type.Primitive.BOOLEAN, cond.info().type(), new Bindings());
            if (!(cond.info().multiplicity() instanceof Multiplicity.Bounded cb)
                    || cb.lower() != 1 || cb.upper() == null || cb.upper() != 1) {
                throw new TypeInferenceException("if condition must be Boolean[1],"
                        + " got multiplicity " + cond.info().multiplicity());
            }
            TypedSpec value = thunkBody(t, pf.parameters().get(1), env);
            result = t.kernel().commonSupertype(result, value.info().type());
            resultMult = commonMultiplicity(resultMult, value.info().multiplicity());
            TypedSpec elseB = out == null ? chain : out;
            out = new TypedIf(cond, value, Optional.of(elseB),
                    new ExprType(result, resultMult));
        }
        if (out == null) {
            throw new TypeInferenceException("if(condList, last) needs at least one pair");
        }
        return out;
    }

    /** The widest multiplicity covering both branches &mdash; the shared {@code m} of {@code if<T|m>}. */
    private static Multiplicity commonMultiplicity(Multiplicity a, Multiplicity b) {
        if (a instanceof Multiplicity.Bounded x && b instanceof Multiplicity.Bounded y) {
            int lower = Math.min(x.lower(), y.lower());
            Integer upper = (x.upper() == null || y.upper() == null) ? null : Math.max(x.upper(), y.upper());
            return new Multiplicity.Bounded(lower, upper);
        }
        return a;   // multiplicity variables do not occur on compiled if branches
    }

    /** Make a multiplicity optional ({@code lower = 0}) &mdash; an else-less branch may yield nothing. */
    private static Multiplicity optional(Multiplicity m) {
        return m instanceof Multiplicity.Bounded b ? new Multiplicity.Bounded(0, b.upper()) : m;
    }

    /** Type the body of a zero-parameter lambda thunk ({@code |expr}) in the current scope. */
    private static TypedSpec thunkBody(Typer t, ValueSpecification vs, Env env) {
        if (!(vs instanceof LambdaFunction lam) || !lam.parameters().isEmpty() || lam.body().size() != 1) {
            throw new TypeInferenceException("expected a zero-parameter single-expression thunk");
        }
        return t.synth(lam.body().get(0), env);
    }
}
