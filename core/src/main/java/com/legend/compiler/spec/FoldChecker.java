package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.FoldStrategy;
import com.legend.compiler.spec.typed.TypedFold;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

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
        // FQN spellings canonicalize to the parse name up front (the
        // ProjectChecker lesson): rebuilds and generic resolution key on
        // the name, and an FQN finds only its own narrow catalog entry.
        if (af.function().contains("::")) {
            af = new AppliedFunction("fold", af.parameters());
        }
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
        if (normalize(elementType).equals(normalize(init.type())) && !init.multiplicity().isMany()) {
            return new FoldStrategy.SameType();
        }
        // 3. MapReduce: the body's left op-chain spine strips the accumulator, leaving
        // an element-only transform; the reducer is the same op over two accumulators.
        // The parser spells the body's operator runs the ENGINE's n-ary way
        // (plus[Collection[$a, e1, e2]] — upstream's plus(Number[*]), batch 5
        // leg 5): the run IS the left spine, the accumulator its head.
        String elemParam = lambda.parameters().get(0).name();
        String accParam = lambda.parameters().size() >= 2 ? lambda.parameters().get(1).name() : "y";
        ValueSpecification body0 = lambda.body().get(0);
        ValueSpecification transform = elementTransform(body0, accParam);
        if (transform == null) {
            transform = commutativeElementTransform(body0, accParam, init);
        }
        if (transform != null) {
            ExprType elemInfo = new ExprType(elementType, Multiplicity.Bounded.ONE);
            TypedSpec typedTransform = t.synth(transform, env.with(elemParam, elemInfo));
            String freshParam = "__mr_x";
            TypedSpec typedReducer = t.synth(
                    sameShape((AppliedFunction) body0,
                            List.of(new Variable(accParam), new Variable(freshParam))),
                    env.with(accParam, init).with(freshParam, init));
            // CLOSED lambdas (see MapReduce's javadoc): each tree binds
            // its own parameters, so the inliner's α-hygiene reaches them
            // through its ordinary TypedLambda arm. The reducer's params
            // ride in the fold convention (transformed element first,
            // accumulator second — the same order SameType's ps carries).
            TypedLambda transformFn = new TypedLambda(
                    List.of(elemParam), List.of(typedTransform),
                    ExprType.one(new Type.FunctionType(
                            List.of(new Type.Param(elementType, Multiplicity.Bounded.ONE)),
                            new Type.Param(typedTransform.info().type(),
                                    typedTransform.info().multiplicity()))));
            TypedLambda reducerFn = new TypedLambda(
                    List.of(freshParam, accParam), List.of(typedReducer),
                    ExprType.one(new Type.FunctionType(
                            List.of(new Type.Param(init.type(), init.multiplicity()),
                                    new Type.Param(init.type(), init.multiplicity())),
                            new Type.Param(typedReducer.info().type(),
                                    typedReducer.info().multiplicity()))));
            return new FoldStrategy.MapReduce(transformFn, reducerFn);
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
                && com.legend.compiler.ResolvedNames.names(body, com.legend.compiler.element.type.PlatformTypes.ADD)
                && body.parameters().size() == 2
                && body.parameters().get(0) instanceof Variable acc && acc.name().equals(accParam)
                && body.parameters().get(1) instanceof Variable elem && elem.name().equals(elemParam);
    }

    /** Corpus queries spell natives FULLY QUALIFIED — patterns match the simple name. */

    /**
     * Strip the accumulator off the LEFT spine of a binary op chain, leaving the
     * element-only transform: {@code plus(plus(acc, '; '), name)} &rarr;
     * {@code plus('; ', name)}; {@code times(acc, length(x))} &rarr; {@code length(x)}.
     * Returns {@code null} when the body is not decomposable this way.
     */
    private static @com.legend.base.Nullable ValueSpecification elementTransform(ValueSpecification body, String accParam) {
        if (!(body instanceof AppliedFunction af)) {
            return null;
        }
        List<ValueSpecification> run = operands(af);
        if (run.size() < 2) {
            return null;
        }
        if (run.get(0) instanceof Variable v && v.name().equals(accParam)) {
            List<ValueSpecification> rest = run.subList(1, run.size());
            return rest.size() == 1 ? rest.get(0) : sameShape(af, rest);
        }
        if (run.size() == 2 && run.get(0) instanceof AppliedFunction leftAf
                && leftAf.function().equals(af.function())) {
            ValueSpecification stripped = elementTransform(leftAf, accParam);
            if (stripped != null) {
                return sameShape(af, List.of(stripped, run.get(1)));
            }
        }
        return null;
    }

    /** The operands of an operator application: the n-ary carrier's run
     *  ({@code plus[Collection[a,b,c]]}) or the pairwise parameters
     *  ({@code and(a,b)}). */
    private static List<ValueSpecification> operands(AppliedFunction af) {
        return af.parameters().size() == 1
                && af.parameters().get(0) instanceof com.legend.protocol.spec.PureCollection run
                && run.values().size() >= 2
                ? run.values() : af.parameters();
    }

    /** {@code af} re-applied to {@code operands} in ITS OWN spelling — the
     *  n-ary carrier stays a carrier, a pairwise call stays pairwise. */
    private static AppliedFunction sameShape(AppliedFunction af, List<ValueSpecification> operands) {
        return operands(af) == af.parameters()
                ? af.withParameters(operands)
                : af.withParameters(List.of(new com.legend.protocol.spec.PureCollection(operands)));
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
    private static @com.legend.base.Nullable ValueSpecification commutativeElementTransform(
            ValueSpecification body, String accParam, ExprType init) {
        if (!(body instanceof AppliedFunction af) || operands(af).size() != 2) {
            return null;
        }
        List<ValueSpecification> run = operands(af);
        // Commutativity must be PROVEN from the init's type: plus on
        // Strings is order-sensitive concatenation, and a []-born init
        // types as Nil — which proves nothing (audit: a Nil-typed
        // string fold would have been reordered).
        boolean arithmetic = com.legend.compiler.element.type.PlatformTypes.isPlus(af.function())
                || com.legend.compiler.ResolvedNames.names(af, com.legend.compiler.element.type.PlatformTypes.TIMES);
        boolean commutative = arithmetic
                ? init.type() instanceof Type.Primitive p && p != Type.Primitive.STRING
                : com.legend.compiler.ResolvedNames.names(af, com.legend.compiler.element.type.PlatformTypes.AND) || com.legend.compiler.ResolvedNames.names(af, com.legend.compiler.element.type.PlatformTypes.OR);
        if (commutative
                && run.get(1) instanceof Variable v && v.name().equals(accParam)) {
            return run.get(0);
        }
        return null;
    }

    /** Precision-agnostic normalization: any {@code Decimal(p,s)} counts as plain Decimal. */
    private static Type normalize(Type t) {
        return t instanceof Type.PrecisionDecimal ? Type.Primitive.DECIMAL : t;
    }

}
