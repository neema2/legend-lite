package com.legend.compiler.spec;


import com.legend.platform.CoreFn;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedEval;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code eval} (engine {@code EvalChecker}) &mdash; the four source forms, typed
 * precisely instead of the signature's {@code Any[*]} collapse:
 *
 * <ul>
 *   <li>{@code ~col->eval($row)} &mdash; DESUGARS to {@code $row.col} (property
 *       access) and re-checks; never reaches a node.</li>
 *   <li>{@code funcRef->eval(args…)} &mdash; DESUGARS to a direct call
 *       {@code funcRef(args…)} on the generic path.</li>
 *   <li>{@code lambda->eval(args…)} &mdash; &beta;-reduction: each parameter binds
 *       to its argument's type (declared types take precedence), the body is
 *       checked, and the result is the <em>body's</em> type.</li>
 *   <li>{@code $f->eval(args…)} where {@code $f} is function-typed &mdash; typed
 *       from the declared function type's result; the concrete lambda arrives at
 *       each call site.</li>
 * </ul>
 */
final class EvalChecker {

    private EvalChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        List<ValueSpecification> params = af.parameters();
        if (params.isEmpty()) {
            throw new TypeInferenceException("eval expects a function value as its first argument");
        }
        // a helper CALL producing the function value (evalWrapper():
        // Function<{->TabularDataSet[1]}>) expands raw so its lambda
        // literal rides the β-reduction arm below
        if (params.get(0) instanceof AppliedFunction call) {
            ValueSpecification ex = t.rawSchemaErasedExpansion(call);
            if (ex != null) {
                List<ValueSpecification> np = new ArrayList<>(params);
                np.set(0, ex);
                params = np;
            }
        }
        return switch (params.get(0)) {
            // ~col->eval($row)  ==>  $row.col
            case ColSpec cs -> {
                if (cs.function1() != null || params.size() != 2) {
                    throw new TypeInferenceException("eval on ~" + cs.name() + " expects exactly one row argument");
                }
                yield t.synth(new AppliedProperty(params.get(1), cs.name()), env);
            }
            // funcRef->eval(args…)  ==>  funcRef(args…)
            case PackageableElementPtr ref -> referenceEval(t, ref, params.subList(1, params.size()), env);
            case LambdaFunction lam -> lambdaEval(t, lam, params.subList(1, params.size()), env);
            case ValueSpecification fn -> variableEval(t, fn, params.subList(1, params.size()), env);
        };
    }

    /** {@code funcRef->eval(args…)} is the call {@code funcRef(args…)}; an
     * argument whose MULTIPLICITY misses the parameter's still types (real
     * pure checks the size when the value arrives — eval.pure's
     * testEvalWithCollectionWithOneElement passes Integer[*] into
     * Integer[1]): the referenced function's declared multiplicities govern. */
    private static TypedSpec referenceEval(Typer t, PackageableElementPtr ref,
            List<ValueSpecification> rawArgs, Env env) {
        try {
            return t.synth(new AppliedFunction(ref.fullPath(), rawArgs), env);
        } catch (TypeInferenceException e) {
            List<TypedFunction> fns = t.functionCandidates(ref.fullPath()).stream()
                    .filter(f -> f.parameters().size() == rawArgs.size()).toList();
            if (fns.size() != 1) {
                throw e;
            }
            TypedFunction fn = fns.get(0);
            List<TypedSpec> args = new ArrayList<>(rawArgs.size());
            Bindings b = new Bindings();
            for (int i = 0; i < rawArgs.size(); i++) {
                TypedSpec arg = t.synth(rawArgs.get(i), env);
                t.kernel().unify(fn.parameters().get(i).type(), arg.info().type(), b);   // types must fit
                args.add(arg);
            }
            return CallNodes.mint(t.ctx().implementations(), fn, args, new ExprType(
                    t.kernel().resolve(fn.returnType(), b), fn.returnMultiplicity()));
        }
    }

    /** β-reduction: bind each parameter to its argument's type (declared type wins), check the body. */
    private static TypedSpec lambdaEval(Typer t, LambdaFunction lam, List<ValueSpecification> rawArgs, Env env) {
        if (lam.parameters().size() != rawArgs.size()) {
            throw new TypeInferenceException("eval: lambda has " + lam.parameters().size()
                    + " parameter(s) but " + rawArgs.size() + " argument(s) were supplied");
        }
        boolean multiStatement = false;
        if (lam.body().size() != 1) {
            // [let*, final] folds by source-level let-inlining (SourceSubst);
            // other shapes take the shared body rule below
            LambdaFunction folded = SourceSubst.inlineLets(lam);
            if (folded == null) {
                multiStatement = true;
            } else {
                lam = folded;
            }
        }
        List<TypedSpec> args = new ArrayList<>(rawArgs.size());
        Env scope = env;
        List<String> names = new ArrayList<>(rawArgs.size());
        List<Type.Param> paramTypes = new ArrayList<>(rawArgs.size());
        for (int i = 0; i < rawArgs.size(); i++) {
            TypedSpec arg = t.synth(rawArgs.get(i), env);   // caller-scope evaluation (Pure semantics)
            args.add(arg);
            var p = lam.parameters().get(i);
            ExprType bound = p.type() != null
                    ? new ExprType(t.namedType(p.type()),
                            p.multiplicity() != null ? Multiplicity.from(p.multiplicity())
                                    : arg.info().multiplicity())
                    : arg.info();
            // A DECLARED param type is a contract the argument must meet —
            // binding the declared type while ignoring the arg's let
            // {x:Integer[1]|...}->eval('s') through (the eval-wrong-arg
            // engine spec; audit).
            if (p.type() != null) {
                try {
                    t.kernel().unify(bound.type(), arg.info().type(), new Bindings());
                    t.kernel().unifyMult(bound.multiplicity(), arg.info().multiplicity(),
                            arg.info().type(), new Bindings());
                } catch (TypeInferenceException e) {
                    throw new TypeInferenceException("eval argument " + (i + 1)
                            + ": " + e.getMessage(), e);
                }
            }
            names.add(p.name());
            paramTypes.add(new Type.Param(bound.type(), bound.multiplicity()));
            scope = scope.with(p.name(), bound);
        }
        TypedSpec body = multiStatement ? t.synthBody(lam, scope) : t.synth(lam.body().get(0), scope);
        TypedLambda typed = new TypedLambda(names, List.of(body),
                ExprType.one(new Type.FunctionType(paramTypes,
                        new Type.Param(body.info().type(), body.info().multiplicity()))));
        return new TypedEval(typed, args, body.info());
    }

    /** A function-typed value ($f): the result comes from its declared function type. */
    private static TypedSpec variableEval(Typer t, ValueSpecification fn, List<ValueSpecification> rawArgs, Env env) {
        TypedSpec fnTyped = t.synth(fn, env);
        Type declared = fnTyped.info().type();
        // The element compiler declares function params in the WRAPPED Function<{…}> form;
        // a lambda-typed binding may carry the bare FunctionType — accept both.
        boolean functionTyped = declared instanceof Type.FunctionType
                || (declared instanceof Type.GenericType g && g.arguments().size() == 1
                        && g.arguments().get(0) instanceof Type.FunctionType)
                // an m3 Function subclass value (a Property) — its
                // instantiated supertype spells the function type
                || t.kernel().functionTypeOf(declared).isPresent();
        // any function-typed EXPRESSION serves — a variable, or a CALL
        // RESULT (transformNonCached(), createTempTableStatement(...):
        // the index's eval-of-call-result rows); the kernel's
        // FunctionType unification below is expression-agnostic
        if (!functionTyped) {
            throw new TypeInferenceException(
                    "eval expects a lambda, a function reference, ~col, or a function-typed value; got "
                            + declared.typeName());
        }
        // THE registered verbatim signatures govern (eval<T,V|m,n>(
        // Function<{T[n]->V[m]}>[1], param:T[n]):V[m], arities 1-4): overload
        // selection by arity, param types AND multiplicities via the kernel's
        // FunctionType unification arm, result V[m] from the bindings. The
        // hand-rolled per-arg loop this replaces was the LITE-WEAKENED era.
        List<TypedSpec> args = new ArrayList<>(rawArgs.size());
        List<ExprType> argTypes = new ArrayList<>(rawArgs.size() + 1);
        argTypes.add(fnTyped.info());
        for (ValueSpecification raw : rawArgs) {
            TypedSpec arg = t.synth(raw, env);
            args.add(arg);
            argTypes.add(arg.info());
        }
        InferenceKernel.Resolution r;
        try {
            r = t.kernel().resolveOverload(
                    t.model().findFunction(CoreFn.EVAL.parseName()), argTypes);
        } catch (TypeInferenceException e) {
            // real pure checks an eval argument's MULTIPLICITY when the value
            // arrives (eval.pure testEvalWithCollectionWithOneElement passes
            // Integer[*] into Integer[1]): the function's declared parameter
            // multiplicities govern the typing; the run-time size check is
            // the lowering's
            var ft = t.kernel().functionTypeOf(declared).orElse(null);
            if (ft == null || ft.params().size() != rawArgs.size()) {
                throw e;
            }
            List<ExprType> declaredArgs = new ArrayList<>(argTypes.size());
            declaredArgs.add(fnTyped.info());
            for (int i = 0; i < args.size(); i++) {
                declaredArgs.add(new ExprType(args.get(i).info().type(),
                        ft.params().get(i).multiplicity()));
            }
            r = t.kernel().resolveOverload(
                    t.model().findFunction(CoreFn.EVAL.parseName()), declaredArgs);
        }
        return new TypedEval(fnTyped, args, r.output());
    }
}
