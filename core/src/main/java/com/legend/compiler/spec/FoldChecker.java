package com.legend.compiler.spec;

import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.FoldStrategy;
import com.legend.compiler.spec.typed.TypedFold;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.LambdaFunction;
import com.legend.parser.spec.ValueSpecification;
import com.legend.parser.spec.Variable;

import java.util.List;

/**
 * {@code fold<T,V>(source:T[*], {e, acc|…}, init:V[1]):V[1]} (engine
 * {@code FoldChecker}) &mdash; typed generically ({@code V} binds from the init,
 * the reducer checks against {@code {T[1],V[1]->V[1]}}), then CLASSIFIED into a
 * {@link FoldStrategy} &mdash; the lowering signal the plain signature cannot
 * carry. Classification order (engine's): Concatenation &rarr; SameType &rarr;
 * MapReduce &rarr; CollectionBuild.
 */
final class FoldChecker {

    private FoldChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        if (a.args().size() != 3 || !(a.args().get(1) instanceof TypedLambda reducer)
                || !(af.parameters().get(1) instanceof LambdaFunction reducerAst)) {
            throw new TypeInferenceException("fold expects (source, {e, acc|…}, init)");
        }
        Type elementType = a.args().get(0).info().type();
        TypedSpec init = a.args().get(2);
        FoldStrategy strategy = classify(t, env, reducerAst, elementType, init.info());
        return new TypedFold(a.args().get(0), reducer, init, strategy, a.out());
    }

    /** Engine's 4-way classification (see {@link FoldStrategy} for the order and meanings). */
    private static FoldStrategy classify(Typer t, Env env, LambdaFunction lambda,
                                         Type elementType, ExprType init) {
        // 1. Concatenation: {e, a | $a->add($e)} — the fold IS the source.
        if (isAddPattern(lambda)) {
            return new FoldStrategy.Concatenation();
        }
        // 2. SameType: element and accumulator agree (precision-agnostic), scalar init.
        if (normalize(elementType).equals(normalize(init.type())) && !isMany(init.multiplicity())) {
            return new FoldStrategy.SameType();
        }
        // 3. MapReduce: the body's left op-chain spine strips the accumulator, leaving
        // an element-only transform; the reducer is the same op over two accumulators.
        String elemParam = lambda.parameters().get(0).name();
        String accParam = lambda.parameters().size() >= 2 ? lambda.parameters().get(1).name() : "y";
        ValueSpecification transform = elementTransform(lambda.body().get(0), accParam);
        if (transform == null) {
            transform = commutativeElementTransform(lambda.body().get(0), accParam, init);
        }
        if (transform != null) {
            TypedSpec typedTransform = t.synth(transform,
                    env.with(elemParam, new ExprType(elementType, Multiplicity.Bounded.ONE)));
            String op = ((AppliedFunction) lambda.body().get(0)).function();
            String freshParam = "__mr_x";
            TypedSpec typedReducer = t.synth(
                    new AppliedFunction(op, List.of(new Variable(accParam), new Variable(freshParam))),
                    env.with(accParam, init).with(freshParam, init));
            return new FoldStrategy.MapReduce(typedTransform, typedReducer, accParam, freshParam);
        }
        // 4. Not decomposable — the accumulator is built element-by-element.
        return new FoldStrategy.CollectionBuild();
    }

    /** {@code {e, a | $a->add($e)}} — the identity-add pattern. */
    private static boolean isAddPattern(LambdaFunction lf) {
        if (lf.parameters().size() < 2 || lf.body().isEmpty()) {
            return false;
        }
        String elemParam = lf.parameters().get(0).name();
        String accParam = lf.parameters().get(1).name();
        return lf.body().get(0) instanceof AppliedFunction body
                && body.function().equals("add")
                && body.parameters().size() == 2
                && body.parameters().get(0) instanceof Variable acc && acc.name().equals(accParam)
                && body.parameters().get(1) instanceof Variable elem && elem.name().equals(elemParam);
    }

    /**
     * Strip the accumulator off the LEFT spine of a binary op chain, leaving the
     * element-only transform: {@code plus(plus(acc, '; '), name)} &rarr;
     * {@code plus('; ', name)}; {@code times(acc, length(x))} &rarr; {@code length(x)}.
     * Returns {@code null} when the body is not decomposable this way.
     */
    private static ValueSpecification elementTransform(ValueSpecification body, String accParam) {
        if (!(body instanceof AppliedFunction af) || af.parameters().size() != 2) {
            return null;
        }
        ValueSpecification left = af.parameters().get(0);
        ValueSpecification right = af.parameters().get(1);
        if (left instanceof Variable v && v.name().equals(accParam)) {
            return right;
        }
        if (left instanceof AppliedFunction leftAf && leftAf.function().equals(af.function())) {
            ValueSpecification stripped = elementTransform(left, accParam);
            if (stripped != null) {
                return new AppliedFunction(af.function(), List.of(stripped, right));
            }
        }
        return null;
    }

    /**
     * The commutative retry: {@code op(elemExpr, acc)} decomposes exactly like
     * {@code op(acc, elemExpr)} when {@code op} is commutative FOR THE
     * ACCUMULATOR'S TYPE — {@code plus}/{@code times} on numbers,
     * {@code and}/{@code or} on booleans. {@code plus} on Strings is
     * concatenation (order-sensitive) and is excluded. Engine's checker only
     * strips the left spine, leaving {@code $e->length() + $a} an
     * UN-LOWERABLE CollectionBuild (scalar accumulator) — a gap, not a
     * behavior; the executed fold tests pin the improvement.
     */
    private static ValueSpecification commutativeElementTransform(
            ValueSpecification body, String accParam, ExprType init) {
        if (!(body instanceof AppliedFunction af) || af.parameters().size() != 2) {
            return null;
        }
        boolean commutative = switch (af.function()) {
            case "plus", "times" -> !Type.Primitive.STRING.equals(init.type());
            case "and", "or" -> true;
            default -> false;
        };
        if (commutative
                && af.parameters().get(1) instanceof Variable v && v.name().equals(accParam)) {
            return af.parameters().get(0);
        }
        return null;
    }

    /** Precision-agnostic normalization: any {@code Decimal(p,s)} counts as plain Decimal. */
    private static Type normalize(Type t) {
        return t instanceof Type.PrecisionDecimal ? Type.Primitive.DECIMAL : t;
    }

    private static boolean isMany(Multiplicity m) {
        return m.isMany();
    }
}
