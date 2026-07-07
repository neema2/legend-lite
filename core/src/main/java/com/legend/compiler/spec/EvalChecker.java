package com.legend.compiler.spec;

import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedEval;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.AppliedProperty;
import com.legend.parser.spec.ColSpec;
import com.legend.parser.spec.LambdaFunction;
import com.legend.parser.spec.PackageableElementPtr;
import com.legend.parser.spec.ValueSpecification;

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
        return switch (params.get(0)) {
            // ~col->eval($row)  ==>  $row.col
            case ColSpec cs -> {
                if (cs.function1() != null || params.size() != 2) {
                    throw new TypeInferenceException("eval on ~" + cs.name() + " expects exactly one row argument");
                }
                yield t.synth(new AppliedProperty(params.get(1), cs.name()), env);
            }
            // funcRef->eval(args…)  ==>  funcRef(args…)
            case PackageableElementPtr ref -> t.synth(
                    new AppliedFunction(ref.fullPath(), params.subList(1, params.size())), env);
            case LambdaFunction lam -> lambdaEval(t, lam, params.subList(1, params.size()), env);
            case ValueSpecification fn -> variableEval(t, fn, params.subList(1, params.size()), env);
        };
    }

    /** β-reduction: bind each parameter to its argument's type (declared type wins), check the body. */
    private static TypedSpec lambdaEval(Typer t, LambdaFunction lam, List<ValueSpecification> rawArgs, Env env) {
        if (lam.parameters().size() != rawArgs.size()) {
            throw new TypeInferenceException("eval: lambda has " + lam.parameters().size()
                    + " parameter(s) but " + rawArgs.size() + " argument(s) were supplied");
        }
        if (lam.body().size() != 1) {
            throw new TypeInferenceException("only single-expression lambdas are supported yet");
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
            names.add(p.name());
            paramTypes.add(new Type.Param(bound.type(), bound.multiplicity()));
            scope = scope.with(p.name(), bound);
        }
        TypedSpec body = t.synth(lam.body().get(0), scope);
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
                        && g.arguments().get(0) instanceof Type.FunctionType);
        if (!(fnTyped instanceof TypedVariable) || !functionTyped) {
            throw new TypeInferenceException(
                    "eval expects a lambda, a function reference, ~col, or a function-typed variable; got "
                            + declared.typeName());
        }
        Type.FunctionType ft = Typer.extractFunctionType(declared);
        if (ft.params().size() != rawArgs.size()) {
            throw new TypeInferenceException("eval: function type expects " + ft.params().size()
                    + " argument(s) but " + rawArgs.size() + " were supplied");
        }
        List<TypedSpec> args = new ArrayList<>(rawArgs.size());
        for (int i = 0; i < rawArgs.size(); i++) {
            TypedSpec arg = t.synth(rawArgs.get(i), env);
            t.kernel().unify(ft.params().get(i).type(), arg.info().type(), new Bindings());
            // Multiplicity conformance (audit finding): [*] into [1] must not
            // slip by. Vars (unresolved signature mults) conform trivially.
            if (ft.params().get(i).multiplicity() instanceof Multiplicity.Bounded want
                    && arg.info().multiplicity() instanceof Multiplicity.Bounded got
                    && (got.lower() < want.lower()
                        || (want.upper() != null
                            && (got.upper() == null || got.upper() > want.upper())))) {
                throw new TypeInferenceException("eval: argument " + (i + 1)
                        + " has multiplicity " + got + " but the function type declares "
                        + want);
            }
            args.add(arg);
        }
        return new TypedEval(fnTyped, args, new ExprType(ft.result().type(), ft.result().multiplicity()));
    }
}
