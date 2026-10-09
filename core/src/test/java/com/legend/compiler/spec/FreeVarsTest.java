// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.FreeVars;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedMatch;
import com.legend.compiler.spec.typed.TypedMatchRuntime;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.element.PureModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSubst;
import com.legend.compiler.spec.typed.TypedVariable;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Rebuild W0.6 push 1: the free-variable function of the typed tree and the
 * capture-avoiding substitution built on it. Every binder kind shadows; a
 * let binds only the statements after it; a binder is renamed exactly when
 * a substituted term it would capture is read beneath it.
 */
class FreeVarsTest {

    private static final ExprType INT = ExprType.one(Type.Primitive.INTEGER);

    private static TypedVariable v(String name) {
        return new TypedVariable(name, INT);
    }

    private static TypedSpec both(TypedSpec... terms) {
        return new TypedCollection(List.of(terms), INT);
    }

    private static TypedLambda lam(String param, TypedSpec... body) {
        return new TypedLambda(List.of(param), List.of(body), INT);
    }

    @Test
    void aLambdaParameterShadows() {
        assertEquals(Set.of("y"), FreeVars.of(lam("x", both(v("x"), v("y")))));
    }

    @Test
    void aLetBindsOnlyTheStatementsAfterIt() {
        // {x | let a = $a; $a + $b}: the let's own value reads the OUTER a
        TypedLambda l = lam("x", new TypedLet("a", v("a"), INT), both(v("a"), v("b")));
        assertEquals(Set.of("a", "b"), FreeVars.of(l));
        assertEquals(Set.of("b"), FreeVars.ofBody(List.of(
                new TypedLet("a", new TypedCInteger(1, INT), INT), both(v("a"), v("b")))));
        // independent terms: no term binds for another
        assertEquals(Set.of("a", "b"), FreeVars.of(List.of(
                new TypedLet("a", new TypedCInteger(1, INT), INT), both(v("a"), v("b")))));
    }

    @Test
    void aMatchBindsItsParametersOverTheBodyOnly() {
        TypedMatch m = new TypedMatch(v("i"), "i", both(v("i"), v("k"), v("z")),
                Optional.of("k"), Optional.of(v("k")), INT);
        assertEquals(Set.of("i", "k", "z"), FreeVars.of(m),
                "the input and the extra argument are outside the binders");
        TypedMatch closed = new TypedMatch(v("a"), "i", both(v("i"), v("k")),
                Optional.of("k"), Optional.of(v("b")), INT);
        assertEquals(Set.of("a", "b"), FreeVars.of(closed));
    }

    @Test
    void aRuntimeMatchBindsTheExtraParameterInEveryArm() {
        TypedMatchRuntime mr = new TypedMatchRuntime(v("in"),
                List.of(new TypedMatchRuntime.Arm("Integer", "i", both(v("i"), v("k"), v("n"))),
                        new TypedMatchRuntime.Arm("Number", "n", both(v("n"), v("k"), v("i")))),
                Optional.of("k"), Optional.of(v("ex")), Optional.of(v("dyn")), INT);
        assertEquals(Set.of("in", "ex", "dyn", "n", "i"), FreeVars.of(mr),
                "each arm binds its own parameter only; the extra parameter binds in both");
        assertEquals(Set.of("i", "n", "k"), FreeVars.binders(mr));
    }

    @Test
    void aKnownSubtermIsNotEntered() {
        TypedSpec known = both(v("a"), v("x"));
        // x is free in the known subterm and bound where the subterm stands
        assertEquals(Set.of("a", "b"),
                FreeVars.of(lam("x", both(known, v("b"))), known, Set.of("a", "x")));
        // the walk takes the given set and does not read the subterm
        assertEquals(Set.of("q", "b"), FreeVars.of(both(known, v("b")), known, Set.of("q")));
        // an equal term that is not the same node is read as usual
        assertEquals(Set.of("a", "x"), FreeVars.of(both(v("a"), v("x")), known, Set.of("q")));
    }

    @Test
    void anExecuteCallsRuntimeArgumentStaysAsSpelled() {
        // the ORCHESTRATION position (NativeFn.Handle.orchestrationArgument): the
        // statement executor reads execute()'s runtime argument in its source
        // form, a let's name resolved through the query's lets -- the
        // substitution leaves it, and substitutes the other arguments
        PureModelContext ctx = (PureModelContext) com.legend.Compiler.buildModel(
                com.legend.testing.Own.model("Class model::Person {}\n"));
        TypedFunction execute = ctx.findFunction("meta::pure::router::execute").stream()
                .filter(f -> f.parameters().size() == 4).findFirst().orElseThrow();
        TypedNativeCall call = new TypedNativeCall(execute, List.of(v("q"), v("m"), v("rt"), v("x")), INT);
        TypedSpec r = TypedSubst.apply(call, Map.of("q", both(v("a")), "rt", both(v("b"))));
        assertEquals(new TypedNativeCall(execute, List.of(both(v("a")), v("m"), v("rt"), v("x")), INT), r);
        assertSame(call, TypedSubst.apply(call, Map.of("rt", both(v("b")))));
    }

    @Test
    void aBinderRenamedAboveAnExecuteCallIsFollowedInsideItsRuntimeArgument() {
        // the lambda's x would capture the term of i (x free in it) and is
        // renamed; the runtime argument's read of x follows the binder,
        // though nothing is substituted into that argument
        PureModelContext ctx = (PureModelContext) com.legend.Compiler.buildModel(
                com.legend.testing.Own.model("Class model::Person {}\n"));
        TypedFunction execute = ctx.findFunction("meta::pure::router::execute").stream()
                .filter(f -> f.parameters().size() == 4).findFirst().orElseThrow();
        TypedLambda l = lam("x", new TypedNativeCall(execute,
                List.of(v("i"), v("m"), v("x"), v("e")), INT));
        TypedSpec r = TypedSubst.apply(l, Map.of("i", v("x")));
        assertEquals(lam("x_1", new TypedNativeCall(execute,
                List.of(v("x"), v("m"), v("x_1"), v("e")), INT)), r);
    }

    @Test
    void substitutionStopsAtAShadowingBinder() {
        TypedLambda l = lam("i", v("i"));
        assertSame(l, TypedSubst.apply(l, Map.of("i", v("x"))));
    }

    @Test
    void aBinderThatWouldCaptureIsRenamed() {
        // {x | $i + $x}[i := $x]  =>  {x_1 | $x + $x_1}
        TypedSpec r = TypedSubst.apply(lam("x", both(v("i"), v("x"))), Map.of("i", v("x")));
        assertEquals(lam("x_1", both(v("x"), v("x_1"))), r);
    }

    @Test
    void theNewNameAvoidsEveryNameInSight() {
        // x_1 is read by the body, x_2 is a binder inside, x_3 is free in the term
        TypedLambda l = lam("x", both(v("i"), v("x"), v("x_1"), lam("x_2", v("x_2"))));
        TypedSpec term = both(v("x"), v("x_3"));
        TypedSpec r = TypedSubst.apply(l, Map.of("i", term));
        assertEquals(lam("x_4", both(term, v("x_4"), v("x_1"), lam("x_2", v("x_2")))), r);
    }

    @Test
    void aBinderIsKeptWhenItsScopeNeverReadsTheEntry() {
        // x is free in the term, but the lambda never reads $i: no hazard, no rename
        TypedLambda l = lam("x", v("x"));
        assertSame(l, TypedSubst.apply(l, Map.of("i", v("x"))));
    }

    @Test
    void aLambdaBodyLetIsABinderToo() {
        // {| let x = 1; $i + $x}[i := $x]  =>  {| let x_1 = 1; $x + $x_1}
        TypedLambda l = new TypedLambda(List.of(), List.of(
                new TypedLet("x", new TypedCInteger(1, INT), INT), both(v("i"), v("x"))), INT);
        TypedSpec r = TypedSubst.apply(l, Map.of("i", v("x")));
        assertEquals(new TypedLambda(List.of(), List.of(
                new TypedLet("x_1", new TypedCInteger(1, INT), INT), both(v("x"), v("x_1"))), INT), r);
        // a let named like the entry stops the substitution below it, not in its own value
        TypedLambda shadow = new TypedLambda(List.of(), List.of(
                new TypedLet("i", v("i"), INT), v("i")), INT);
        assertEquals(new TypedLambda(List.of(), List.of(
                new TypedLet("i", v("z"), INT), v("i")), INT),
                TypedSubst.apply(shadow, Map.of("i", v("z"))));
    }

    @Test
    void matchBindersAreRenamedOnCapture() {
        TypedMatch m = new TypedMatch(v("in"), "x", both(v("i"), v("x"), v("k")),
                Optional.of("k"), Optional.of(v("ex")), INT);
        TypedSpec r = TypedSubst.apply(m, Map.of("i", both(v("x"), v("k"))));
        assertEquals(new TypedMatch(v("in"), "x_1",
                both(both(v("x"), v("k")), v("x_1"), v("k_1")),
                Optional.of("k_1"), Optional.of(v("ex")), INT), r);
        TypedMatchRuntime mr = new TypedMatchRuntime(v("in"),
                List.of(new TypedMatchRuntime.Arm("Integer", "x", both(v("i"), v("x"))),
                        new TypedMatchRuntime.Arm("Number", "n", both(v("n"), v("k")))),
                Optional.of("k"), Optional.of(v("ex")), INT);
        assertEquals(new TypedMatchRuntime(v("in"),
                List.of(new TypedMatchRuntime.Arm("Integer", "x_1", both(v("x"), v("x_1"))),
                        new TypedMatchRuntime.Arm("Number", "n", both(v("n"), v("k")))),
                Optional.of("k"), Optional.of(v("ex")), INT),
                TypedSubst.apply(mr, Map.of("i", v("x"))));
    }
}
