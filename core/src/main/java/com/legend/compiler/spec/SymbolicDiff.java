// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.builtin.Pure;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCDecimal;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedSubst;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.NotImplementedException;
import com.legend.model.FunctionId;
import com.legend.model.NativeFunctionDefinition;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Compile-time symbolic differentiation for {@code meta::legend::lite::diff}
 * in Phase G&frac12; ({@link UserCallInliner}): transforms a reduced
 * univariate {@link TypedLambda} body {@code f(x)} into its closed-form
 * symbolic derivative {@code df/dx} evaluated at {@code point}.
 *
 * <p>All differentiation rules are purely structural {@code TypedSpec -> TypedSpec}
 * AST rewrites (Tenet C1.6 &amp; C6.2): the compiler constructs the derivative
 * expression tree using only structural ring identities ({@code 0 + u -> u},
 * {@code 0 * u -> 0}, {@code 1 * u -> u}), and the database evaluates all
 * numeric values in SQL.
 */
final class SymbolicDiff {

    private static final ExprType FLOAT_ONE = ExprType.one(Type.Primitive.FLOAT);
    private static final ExprType BOOL_ONE = ExprType.one(Type.Primitive.BOOLEAN);
    private static final BigInteger BI_TWO = BigInteger.valueOf(2);
    private static final BigDecimal BD_TWO = BigDecimal.valueOf(2);

    private final ModelContext ctx;
    private final String var;

    private SymbolicDiff(ModelContext ctx, String var) {
        this.ctx = ctx;
        this.var = var;
    }

    static TypedSpec differentiate(ModelContext ctx, List<TypedSpec> args) {
        if (args.size() != 2 || !(args.get(0) instanceof TypedLambda lam) || lam.parameters().size() != 1) {
            throw new NotImplementedException("diff expects a single-parameter lambda and an evaluation point, got "
                    + (args.isEmpty() ? "no arguments" : args.get(0).getClass().getSimpleName()));
        }
        String param = lam.parameters().get(0);
        TypedSpec body = inlineLambdaLets(lam.body());
        TypedSpec floatBody = promoteToFloat(ctx, body);
        TypedSpec dBody = new SymbolicDiff(ctx, param).diff(floatBody);
        TypedSpec point = promoteToFloat(ctx, args.get(1));
        TypedSpec evaluated = TypedSubst.apply(dBody, Map.of(param, point));
        return promoteToFloat(ctx, evaluated);
    }

    private static TypedSpec inlineLambdaLets(List<TypedSpec> stmts) {
        if (stmts.size() == 1) {
            return stmts.get(0) instanceof TypedLet let ? let.value() : stmts.get(0);
        }
        Map<String, TypedSpec> env = new LinkedHashMap<>();
        for (int i = 0; i < stmts.size() - 1; i++) {
            if (!(stmts.get(i) instanceof TypedLet let)) {
                throw new NotImplementedException("diff lambda body contains non-let intermediate statement: "
                        + stmts.get(i).getClass().getSimpleName());
            }
            env.put(let.name(), TypedSubst.apply(let.value(), env));
        }
        TypedSpec last = stmts.get(stmts.size() - 1);
        TypedSpec target = last instanceof TypedLet let ? let.value() : last;
        return TypedSubst.apply(target, env);
    }

    private TypedSpec diff(TypedSpec e) {
        if (!dependsOn(e, var)) {
            return lit(0.0);
        }
        return switch (e) {
            case TypedVariable tv -> tv.name().equals(var) ? lit(1.0) : lit(0.0);
            case TypedCast c -> diff(c.source());
            case TypedIf ti -> diffIf(ti);
            case TypedNativeCall nc -> diffNative(nc);
            default -> throw new NotImplementedException(
                    "diff: unsupported expression construct '" + e.getClass().getSimpleName() + "'");
        };
    }

    private TypedSpec diffIf(TypedIf ti) {
        TypedSpec dThen = diff(ti.thenBranch());
        TypedSpec dElse = ti.elseBranch().map(this::diff).orElseGet(() -> lit(0.0));
        if (isZero(dThen) && isZero(dElse)) {
            return lit(0.0);
        }
        return new TypedIf(ti.condition(), dThen, Optional.of(dElse), FLOAT_ONE);
    }

    private TypedSpec diffNative(TypedNativeCall nc) {
        FunctionId id = nc.callee().id();
        List<TypedSpec> args = nc.args();
        if (Pure.isToOneCall(nc.callee().qualifiedName())
                || Pure.AT_MATH_TO_FLOAT.contains(id)
                || Pure.AT_MATH_TO_DECIMAL.contains(id)) {
            return diff(args.get(0));
        }
        if (Pure.AT_MATH_PLUS.contains(id)) {
            return diffPlus(args);
        }
        if (Pure.AT_MATH_MINUS.contains(id)) {
            return diffMinus(args);
        }
        if (Pure.AT_MATH_TIMES.contains(id)) {
            return diffTimes(args);
        }
        if (Pure.AT_MATH_DIVIDE.contains(id)) {
            return diffDivide(args);
        }
        if (Pure.AT_MATH_EXP.contains(id)) {
            TypedSpec u = args.get(0);
            return mul(call1(Pure.EXP__NUMBER_1, u), diff(u));
        }
        if (Pure.AT_MATH_LOG.contains(id)) {
            TypedSpec u = args.get(0);
            return div(diff(u), u);
        }
        if (Pure.AT_MATH_LOG10.contains(id)) {
            TypedSpec u = args.get(0);
            return div(diff(u), mul(u, call1(Pure.LOG__NUMBER_1, lit(10.0))));
        }
        if (Pure.AT_MATH_SQRT.contains(id)) {
            TypedSpec u = args.get(0);
            return div(diff(u), mul(lit(2.0), call1(Pure.SQRT__NUMBER_1, u)));
        }
        if (Pure.AT_MATH_CBRT.contains(id)) {
            TypedSpec u = args.get(0);
            TypedSpec cbrtU = call1(Pure.CBRT__NUMBER_1, u);
            return div(diff(u), mul(lit(3.0), mul(cbrtU, cbrtU)));
        }
        if (Pure.AT_MATH_POW.contains(id)) {
            return diffPow(args.get(0), args.get(1));
        }
        if (Pure.AT_LEGEND_LITE_ERF.contains(id)) {
            TypedSpec u = args.get(0);
            TypedSpec twoOverSqrtPi = div(lit(2.0), call1(Pure.SQRT__NUMBER_1, call0(Pure.PI)));
            TypedSpec expNegUSq = call1(Pure.EXP__NUMBER_1, neg(mul(u, u)));
            return mul(mul(twoOverSqrtPi, expNegUSq), diff(u));
        }
        return diffTranscendentalOrStep(nc, id, args);
    }

    private TypedSpec diffTranscendentalOrStep(TypedNativeCall nc, FunctionId id, List<TypedSpec> args) {
        if (Pure.AT_MATH_SIN.contains(id)) {
            TypedSpec u = args.get(0);
            return mul(call1(Pure.COS__NUMBER_1, u), diff(u));
        }
        if (Pure.AT_MATH_COS.contains(id)) {
            TypedSpec u = args.get(0);
            return neg(mul(call1(Pure.SIN__NUMBER_1, u), diff(u)));
        }
        if (Pure.AT_MATH_TAN.contains(id)) {
            TypedSpec u = args.get(0);
            TypedSpec cosU = call1(Pure.COS__NUMBER_1, u);
            return div(diff(u), mul(cosU, cosU));
        }
        if (Pure.AT_MATH_ASIN.contains(id)) {
            TypedSpec u = args.get(0);
            return div(diff(u), call1(Pure.SQRT__NUMBER_1, sub(lit(1.0), mul(u, u))));
        }
        if (Pure.AT_MATH_ACOS.contains(id)) {
            TypedSpec u = args.get(0);
            return div(neg(diff(u)), call1(Pure.SQRT__NUMBER_1, sub(lit(1.0), mul(u, u))));
        }
        if (Pure.AT_MATH_ATAN.contains(id)) {
            TypedSpec u = args.get(0);
            return div(diff(u), add(lit(1.0), mul(u, u)));
        }
        if (Pure.AT_MATH_ATAN2.contains(id)) {
            TypedSpec y = args.get(0);
            TypedSpec x = args.get(1);
            return div(sub(mul(x, diff(y)), mul(y, diff(x))), add(mul(x, x), mul(y, y)));
        }
        if (Pure.AT_MATH_SINH.contains(id)) {
            TypedSpec u = args.get(0);
            return mul(call1(Pure.COSH__NUMBER_1, u), diff(u));
        }
        if (Pure.AT_MATH_COSH.contains(id)) {
            TypedSpec u = args.get(0);
            return mul(call1(Pure.SINH__NUMBER_1, u), diff(u));
        }
        if (Pure.AT_MATH_TANH.contains(id)) {
            TypedSpec u = args.get(0);
            TypedSpec tanhU = call1(Pure.TANH__NUMBER_1, u);
            return mul(sub(lit(1.0), mul(tanhU, tanhU)), diff(u));
        }
        if (Pure.AT_MATH_ABS.contains(id)) {
            TypedSpec u = args.get(0);
            TypedSpec du = diff(u);
            TypedSpec cond = callBool2(Pure.GREATER_THAN_EQUAL__NUMBER_1__NUMBER_1, u, lit(0.0));
            return new TypedIf(cond, du, Optional.of(neg(du)), FLOAT_ONE);
        }
        if (Pure.AT_MATH_MAX.contains(id) && args.size() == 2) {
            TypedSpec u = args.get(0);
            TypedSpec v = args.get(1);
            TypedSpec cond = callBool2(Pure.GREATER_THAN_EQUAL__NUMBER_1__NUMBER_1, u, v);
            return new TypedIf(cond, diff(u), Optional.of(diff(v)), FLOAT_ONE);
        }
        if (Pure.AT_MATH_MIN.contains(id) && args.size() == 2) {
            TypedSpec u = args.get(0);
            TypedSpec v = args.get(1);
            TypedSpec cond = callBool2(Pure.LESS_THAN_EQUAL__NUMBER_1__NUMBER_1, u, v);
            return new TypedIf(cond, diff(u), Optional.of(diff(v)), FLOAT_ONE);
        }
        if (Pure.AT_MATH_SIGN.contains(id)
                || Pure.AT_MATH_FLOOR.contains(id)
                || Pure.AT_MATH_CEILING.contains(id)
                || Pure.AT_MATH_ROUND.contains(id)) {
            return lit(0.0);
        }
        throw new NotImplementedException(
                "diff: cannot differentiate native function '" + nc.callee().qualifiedName() + "'");
    }

    private TypedSpec diffPlus(List<TypedSpec> args) {
        if (args.size() == 1 && args.get(0) instanceof TypedCollection tc) {
            TypedSpec acc = lit(0.0);
            for (TypedSpec el : tc.elements()) {
                acc = add(acc, diff(el));
            }
            return acc;
        }
        return diff(args.get(0));
    }

    private TypedSpec diffMinus(List<TypedSpec> args) {
        if (args.size() == 1 && args.get(0) instanceof TypedCollection tc) {
            if (tc.elements().isEmpty()) {
                return lit(0.0);
            }
            if (tc.elements().size() == 1) {
                return neg(diff(tc.elements().get(0)));
            }
            TypedSpec acc = diff(tc.elements().get(0));
            for (int i = 1; i < tc.elements().size(); i++) {
                acc = sub(acc, diff(tc.elements().get(i)));
            }
            return acc;
        }
        return neg(diff(args.get(0)));
    }

    private TypedSpec diffTimes(List<TypedSpec> args) {
        if (args.size() == 1 && args.get(0) instanceof TypedCollection tc) {
            List<TypedSpec> els = tc.elements();
            if (els.isEmpty()) {
                return lit(0.0);
            }
            if (els.size() == 1) {
                return diff(els.get(0));
            }
            TypedSpec u = els.get(0);
            TypedSpec v = els.size() == 2
                    ? els.get(1)
                    : new TypedNativeCall(nativeFn(ctx, Pure.TIMES__FLOAT_MANY),
                            List.of(runOf(els.subList(1, els.size()))), FLOAT_ONE);
            return add(mul(diff(u), v), mul(u, diff(v)));
        }
        return diff(args.get(0));
    }

    private TypedSpec diffDivide(List<TypedSpec> args) {
        TypedSpec u = args.get(0);
        TypedSpec v = args.get(1);
        if (!dependsOn(v, var)) {
            return div(diff(u), v);
        }
        return div(sub(mul(diff(u), v), mul(u, diff(v))), mul(v, v));
    }

    private TypedSpec diffPow(TypedSpec u, TypedSpec v) {
        boolean uDep = dependsOn(u, var);
        boolean vDep = dependsOn(v, var);
        if (uDep && !vDep) {
            if (isZero(v)) {
                return lit(0.0);
            }
            if (isOne(v)) {
                return diff(u);
            }
            if (isTwo(v)) {
                return mul(mul(lit(2.0), u), diff(u));
            }
            TypedSpec vMinusOne = sub(v, lit(1.0));
            return mul(mul(v, call2(Pure.POW__NUMBER_1__NUMBER_1, u, vMinusOne)), diff(u));
        }
        if (!uDep && vDep) {
            return mul(mul(call2(Pure.POW__NUMBER_1__NUMBER_1, u, v), call1(Pure.LOG__NUMBER_1, u)), diff(v));
        }
        TypedSpec powUV = call2(Pure.POW__NUMBER_1__NUMBER_1, u, v);
        TypedSpec term1 = mul(diff(v), call1(Pure.LOG__NUMBER_1, u));
        TypedSpec term2 = div(mul(v, diff(u)), u);
        return mul(powUV, add(term1, term2));
    }

    // ---- Algebraic constructors with structural 0 / 1 ring identities ----

    private TypedSpec add(TypedSpec a, TypedSpec b) {
        if (isZero(a)) {
            return b;
        }
        if (isZero(b)) {
            return a;
        }
        return new TypedNativeCall(nativeFn(ctx, Pure.PLUS__FLOAT_MANY),
                List.of(runOf(List.of(a, b))), FLOAT_ONE);
    }

    private TypedSpec sub(TypedSpec a, TypedSpec b) {
        if (isZero(b)) {
            return a;
        }
        if (isZero(a)) {
            return neg(b);
        }
        return new TypedNativeCall(nativeFn(ctx, Pure.MINUS__FLOAT_MANY),
                List.of(runOf(List.of(a, b))), FLOAT_ONE);
    }

    private TypedSpec neg(TypedSpec a) {
        if (isZero(a)) {
            return lit(0.0);
        }
        if (a instanceof TypedCFloat cf && cf.value() == -0.0) {
            return lit(0.0);
        }
        if (a instanceof TypedNativeCall nc && Pure.AT_MATH_MINUS.contains(nc.callee().id())
                && nc.args().size() == 1 && !(nc.args().get(0) instanceof TypedCollection)) {
            return nc.args().get(0);
        }
        return new TypedNativeCall(nativeFn(ctx, Pure.MINUS__FLOAT_MANY), List.of(a), FLOAT_ONE);
    }

    private TypedSpec mul(TypedSpec a, TypedSpec b) {
        if (isZero(a) || isZero(b)) {
            return lit(0.0);
        }
        if (isOne(a)) {
            return b;
        }
        if (isOne(b)) {
            return a;
        }
        if (isMinusOne(a)) {
            return neg(b);
        }
        if (isMinusOne(b)) {
            return neg(a);
        }
        return new TypedNativeCall(nativeFn(ctx, Pure.TIMES__FLOAT_MANY),
                List.of(runOf(List.of(a, b))), FLOAT_ONE);
    }

    private TypedSpec div(TypedSpec a, TypedSpec b) {
        if (isZero(a)) {
            return lit(0.0);
        }
        if (isOne(b)) {
            return a;
        }
        return call2(Pure.DIVIDE__NUMBER_1__NUMBER_1, a, b);
    }

    private TypedSpec call0(NativeFunctionDefinition def) {
        return new TypedNativeCall(nativeFn(ctx, def), List.of(), FLOAT_ONE);
    }

    private TypedSpec call1(NativeFunctionDefinition def, TypedSpec a) {
        return new TypedNativeCall(nativeFn(ctx, def), List.of(a), FLOAT_ONE);
    }

    private TypedSpec call2(NativeFunctionDefinition def, TypedSpec a, TypedSpec b) {
        return new TypedNativeCall(nativeFn(ctx, def), List.of(a, b), FLOAT_ONE);
    }

    private TypedSpec callBool2(NativeFunctionDefinition def, TypedSpec a, TypedSpec b) {
        return new TypedNativeCall(nativeFn(ctx, def), List.of(a, b), BOOL_ONE);
    }

    private static TypedCollection runOf(List<TypedSpec> els) {
        return new TypedCollection(els,
                new ExprType(Type.Primitive.FLOAT, new Multiplicity.Bounded(els.size(), els.size())),
                false, true);
    }

    private static TypedCFloat lit(double v) {
        return new TypedCFloat(v, null, FLOAT_ONE);
    }

    private static boolean isZero(TypedSpec e) {
        return (e instanceof TypedCFloat cf && cf.value() == 0.0)
                || (e instanceof TypedCInteger ci && ci.big().equals(BigInteger.ZERO))
                || (e instanceof TypedCDecimal cd && cd.value().compareTo(BigDecimal.ZERO) == 0);
    }

    private static boolean isOne(TypedSpec e) {
        return (e instanceof TypedCFloat cf && cf.value() == 1.0)
                || (e instanceof TypedCInteger ci && ci.big().equals(BigInteger.ONE))
                || (e instanceof TypedCDecimal cd && cd.value().compareTo(BigDecimal.ONE) == 0);
    }

    private static boolean isMinusOne(TypedSpec e) {
        return (e instanceof TypedCFloat cf && cf.value() == -1.0)
                || (e instanceof TypedCInteger ci && ci.big().equals(BigInteger.ONE.negate()))
                || (e instanceof TypedCDecimal cd && cd.value().compareTo(BigDecimal.ONE.negate()) == 0);
    }

    private static boolean isTwo(TypedSpec e) {
        return (e instanceof TypedCFloat cf && cf.value() == 2.0)
                || (e instanceof TypedCInteger ci && ci.big().equals(BI_TWO))
                || (e instanceof TypedCDecimal cd && cd.value().compareTo(BD_TWO) == 0);
    }

    private static boolean dependsOn(TypedSpec e, String var) {
        if (e instanceof TypedVariable tv) {
            return tv.name().equals(var);
        }
        if (e instanceof TypedLambda lam && lam.parameters().contains(var)) {
            return false;
        }
        for (TypedSpec child : e.children()) {
            if (dependsOn(child, var)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Normalizes numeric subexpressions to {@code Float[1]} so operator runs
     * ({@code plus}, {@code minus}, {@code times}) remain strictly homogeneous
     * {@code Float} chains in SQL lowering rather than mixed-type JSON lists.
     */
    private static TypedSpec promoteToFloat(ModelContext ctx, TypedSpec e) {
        if (e instanceof TypedCInteger ci) {
            if (ci.big().bitLength() <= 52) {
                return lit(ci.big().doubleValue());
            }
            return new TypedNativeCall(nativeFn(ctx, Pure.TO_FLOAT__NUMBER_1), List.of(ci), FLOAT_ONE);
        }
        if (e instanceof TypedCDecimal cd) {
            if (isZero(cd)) {
                return lit(0.0);
            }
            if (isOne(cd)) {
                return lit(1.0);
            }
            if (isMinusOne(cd)) {
                return lit(-1.0);
            }
            if (isTwo(cd)) {
                return lit(2.0);
            }
            return new TypedNativeCall(nativeFn(ctx, Pure.TO_FLOAT__NUMBER_1), List.of(cd), FLOAT_ONE);
        }
        if (e instanceof TypedIf ti) {
            TypedSpec cond = promoteChildrenInBool(ctx, ti.condition());
            TypedSpec thenB = promoteToFloat(ctx, ti.thenBranch());
            Optional<TypedSpec> elseB = ti.elseBranch().map(b -> promoteToFloat(ctx, b));
            return new TypedIf(cond, thenB, elseB, FLOAT_ONE);
        }
        if (e instanceof TypedNativeCall nc) {
            FunctionId id = nc.callee().id();
            if (Pure.AT_MATH_PLUS.contains(id)) {
                return promoteRunCall(ctx, nc, Pure.PLUS__FLOAT_MANY);
            }
            if (Pure.AT_MATH_MINUS.contains(id)) {
                return promoteRunCall(ctx, nc, Pure.MINUS__FLOAT_MANY);
            }
            if (Pure.AT_MATH_TIMES.contains(id)) {
                return promoteRunCall(ctx, nc, Pure.TIMES__FLOAT_MANY);
            }
            if (Pure.AT_MATH_ABS.contains(id) && nc.args().size() == 1) {
                return new TypedNativeCall(nativeFn(ctx, Pure.ABS__FLOAT_1),
                        List.of(promoteToFloat(ctx, nc.args().get(0))), FLOAT_ONE);
            }
            if (Pure.AT_MATH_MAX.contains(id) && nc.args().size() == 2) {
                return new TypedNativeCall(nativeFn(ctx, Pure.MAX__FLOAT_1__FLOAT_1),
                        List.of(promoteToFloat(ctx, nc.args().get(0)), promoteToFloat(ctx, nc.args().get(1))),
                        FLOAT_ONE);
            }
            if (Pure.AT_MATH_MIN.contains(id) && nc.args().size() == 2) {
                return new TypedNativeCall(nativeFn(ctx, Pure.MIN__FLOAT_1__FLOAT_1),
                        List.of(promoteToFloat(ctx, nc.args().get(0)), promoteToFloat(ctx, nc.args().get(1))),
                        FLOAT_ONE);
            }
            if (Pure.AT_MATH_TO_FLOAT.contains(id) && nc.args().size() == 1) {
                return promoteToFloat(ctx, nc.args().get(0));
            }
            if (Pure.isToOneCall(nc.callee().qualifiedName())) {
                if (nc.info().type() != Type.Primitive.FLOAT && isNumericType(nc.info().type())) {
                    return new TypedNativeCall(nativeFn(ctx, Pure.TO_FLOAT__NUMBER_1), List.of(nc), FLOAT_ONE);
                }
                return nc;
            }
            List<TypedSpec> promotedArgs = new ArrayList<>(nc.args().size());
            for (TypedSpec arg : nc.args()) {
                promotedArgs.add(isNumericType(arg.info().type())
                        ? promoteToFloat(ctx, arg)
                        : promoteChildrenInBool(ctx, arg));
            }
            TypedSpec rebuilt = nc.withChildren(promotedArgs);
            if (rebuilt.info().type() != Type.Primitive.FLOAT && isNumericType(rebuilt.info().type())) {
                return new TypedNativeCall(nativeFn(ctx, Pure.TO_FLOAT__NUMBER_1), List.of(rebuilt), FLOAT_ONE);
            }
            return rebuilt;
        }
        if (e.info().type() != Type.Primitive.FLOAT && isNumericType(e.info().type())) {
            return new TypedNativeCall(nativeFn(ctx, Pure.TO_FLOAT__NUMBER_1), List.of(e), FLOAT_ONE);
        }
        return e;
    }

    private static TypedSpec promoteRunCall(ModelContext ctx, TypedNativeCall nc, NativeFunctionDefinition floatDef) {
        if (nc.args().size() == 1 && nc.args().get(0) instanceof TypedCollection tc) {
            List<TypedSpec> els = new ArrayList<>(tc.elements().size());
            for (TypedSpec el : tc.elements()) {
                els.add(promoteToFloat(ctx, el));
            }
            return new TypedNativeCall(nativeFn(ctx, floatDef), List.of(runOf(els)), FLOAT_ONE);
        }
        List<TypedSpec> args = new ArrayList<>(nc.args().size());
        for (TypedSpec arg : nc.args()) {
            args.add(promoteToFloat(ctx, arg));
        }
        return new TypedNativeCall(nativeFn(ctx, floatDef), args, FLOAT_ONE);
    }

    private static TypedSpec promoteChildrenInBool(ModelContext ctx, TypedSpec e) {
        if (isNumericType(e.info().type())) {
            return promoteToFloat(ctx, e);
        }
        return e.mapChildren(c -> promoteChildrenInBool(ctx, c));
    }

    private static boolean isNumericType(Type t) {
        return t == Type.Primitive.FLOAT
                || t == Type.Primitive.INTEGER
                || t == Type.Primitive.DECIMAL
                || t == Type.Primitive.NUMBER
                || t instanceof Type.PrecisionDecimal;
    }

    private static TypedFunction nativeFn(ModelContext ctx, NativeFunctionDefinition def) {
        FunctionId id = FunctionId.of(def);
        return ctx.findFunction(def.qualifiedName()).stream()
                .filter(f -> f.id().equals(id))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("missing native function in catalog: " + id));
    }
}
