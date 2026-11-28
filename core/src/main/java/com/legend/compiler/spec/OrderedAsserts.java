// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.builtin.NativeFn;
import com.legend.compiler.spec.typed.Calls;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.ArrayList;
import java.util.List;

/**
 * AN ORDER-DEPENDENT COMPARISON OF A RESULT WITH NO DEFINED ORDER (B3 step 4; the user, 2026-11-24: "my ideal state for
 * product is that we don't let platform devs or users write tests that do not have a sort ... But we still need
 * multi-set compare for backwards compatibility"). {@code assertEquals} and {@code assertNotEquals} compare in order; a
 * side whose order the program does not define ({@link OrderView#INCIDENTAL}: a store read or a frame read with no sort,
 * or past a grouping, a join, a union) arrives in whatever order the database chose, so the verdict would be the
 * database's luck. Lite's planner refuses it, naming the two fixes: a sort, or {@code assertSameElements} when the order
 * does not matter. Upstream's such asserts (its judge compared them as multisets) are rewritten to the explicit
 * order-free assert by the compatibility edge's test-body door before planning ({@code edge.UpstreamTestBodies}), by the
 * same walk ({@link #rewrite}), so the door rewrites exactly what the planner would refuse.
 *
 * <p>The walk's scope (step 4's review, M3, 2026-11-24): inside a lambda its parameters are values the walk cannot see
 * (never a frame read) and its own lets are read in order; an {@code assertError}'s lambda is not walked (its asserts are
 * the thing that must fail). An assert inside a helper the body calls is refused where the judges inline it
 * ({@code testplan.BodyDriver.helperAssertRoot}); the door does not reach it (none in the corpus or PCT, measured
 * 2026-11-24). Not reached (recorded, step 4's review): a single value picked from unordered rows ({@code at(0)},
 * {@code first()}).
 */
public final class OrderedAsserts {

    private OrderedAsserts() {
    }

    /** What the walk does at an order-dependent assert: the product refuses it, a door rewrites it. */
    @FunctionalInterface
    public interface AtAssert {
        TypedSpec apply(TypedNativeCall call, NativeFn.Verdict fn, int unorderedSide, List<TypedSpec> lets);
    }

    /** Refuses an order-dependent {@code assertEquals} or {@code assertNotEquals} anywhere in {@code body}'s statements,
     *  each read with the statements before it as its lets. */
    public static void refuse(List<? extends TypedSpec> body) {
        List<TypedSpec> lets = new ArrayList<>();
        for (TypedSpec statement : body) {
            rewrite(statement, lets, OrderedAsserts::refused);
            lets.add(statement);
        }
    }

    /** {@code n} with {@code at} applied to each order-dependent {@code assertEquals} or {@code assertNotEquals} in it,
     *  {@code lets} the statements before it: the one walk the planner's refusal and the door's rewrite share. */
    public static TypedSpec rewrite(TypedSpec n, List<TypedSpec> lets, AtAssert at) {
        if (n instanceof TypedNativeCall call) {
            if (NativeFn.ContextOwner.of(Calls.calleeIdOf(n)).orElse(null) == NativeFn.ContextOwner.ASSERT_ERROR) {
                return n;
            }
            NativeFn.Verdict fn = NativeFn.Verdict.of(Calls.calleeIdOf(n)).orElse(null);
            if ((fn == NativeFn.Verdict.ASSERT_EQUALS || fn == NativeFn.Verdict.ASSERT_NOT_EQUALS)
                    && call.args().size() >= 2) {
                int side = unorderedSide(call.args().get(0), call.args().get(1), lets);
                if (side >= 0) {
                    return at.apply(call, fn, side, lets);
                }
            }
            // assert($a == $b): the comparison assertEquals makes, written with the operator (the user, 2026-11-27:
            // "Assert( == ) only if it's simple/easy"). A door's to rewrite as it rewrites assertEquals; the product
            // leaves it to =='s own refusal where it is planned (refused, below)
            if (fn == NativeFn.Verdict.ASSERT && !call.args().isEmpty()
                    && call.args().get(0) instanceof TypedNativeCall equal && equal.args().size() == 2
                    && com.legend.builtin.Pure.AT_BOOLEAN_EQUAL.contains(equal.callee().id())
                    // (two TABLES under == are compared by identity in legend-pure, register S430: never read as
                    // an any-order assert of their rows -- the code audit of the closing batch, S5)
                    && !com.legend.compiler.element.type.Type.isRelation(equal.args().get(0).info().type())
                    && !com.legend.compiler.element.type.Type.isRelation(equal.args().get(1).info().type())) {
                int side = unorderedSide(equal.args().get(0), equal.args().get(1), lets);
                if (side >= 0) {
                    return at.apply(call, fn, side, lets);
                }
            }
        }
        if (n instanceof TypedLambda lambda) {
            List<TypedSpec> scope = new ArrayList<>(lets);
            for (String p : lambda.parameters()) {
                // a parameter is a value the walk cannot see: read as a literal (DEFINED), never as a frame
                scope.add(new TypedLet(p, new TypedCollection(List.of(), lambda.info(), false, false), lambda.info()));
            }
            List<TypedSpec> body = new ArrayList<>(lambda.body().size());
            boolean changed = false;
            for (TypedSpec statement : lambda.body()) {
                TypedSpec r = rewrite(statement, scope, at);
                changed |= r != statement;
                body.add(r);
                scope.add(r);
            }
            return changed ? lambda.withChildren(body) : n;
        }
        List<TypedSpec> kids = n.children();
        List<TypedSpec> rebuilt = new ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec r = rewrite(k, lets, at);
            changed |= r != k;
            rebuilt.add(r);
        }
        return changed ? n.withChildren(rebuilt) : n;
    }

    /** The side (0 expected, 1 actual) whose rows have no defined order while the other side holds more than one value,
     *  or -1: with at most one value on either side, an ordered and an order-free comparison agree (step 4's review,
     *  M2). */
    public static int unorderedSide(TypedSpec expected, TypedSpec actual, List<TypedSpec> lets) {
        if (atMostOne(expected) || atMostOne(actual)) {
            return -1;
        }
        if (unorderedRows(expected, lets)) {
            return 0;
        }
        return unorderedRows(actual, lets) ? 1 : -1;
    }

    /** The lets the order rule reads for a comparison planned outside any body walk: {@code parameters}, each a
     *  value its caller gives in the caller's order ({@link OrderView#unseen}); {@code bound}, the lets an
     *  environment seeds, each its value; then the statement's own {@code lets}. */
    public static List<TypedSpec> scope(java.util.Collection<String> parameters,
            java.util.Map<String, TypedSpec> bound, List<TypedSpec> lets, TypedSpec at) {
        List<TypedSpec> out = new ArrayList<>();
        for (String parameter : parameters) {
            out.add(OrderView.unseen(parameter, at));
        }
        bound.forEach((name, value) -> out.add(new TypedLet(name, value, value.info())));
        out.addAll(lets);
        return out;
    }

    /** Whether {@code side} is rows whose order the program does not define: a relation or a collection (not one
     *  value) whose order view is {@link OrderView#INCIDENTAL}. A printed or joined text is the lift's and the join
     *  rewrite's; the same rule for a text that prints or joins such rows is deferred (B3 step 4's measurement,
     *  2026-11-24: 30 corpus tests a lane -- for the user's word). */
    public static boolean unorderedRows(TypedSpec side, List<TypedSpec> lets) {
        return rows(side) && OrderView.of(side, lets) == OrderView.INCIDENTAL;
    }

    private static boolean rows(TypedSpec side) {
        com.legend.compiler.element.type.ExprType info = side.info();
        boolean toOne = info.multiplicity() instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                && Integer.valueOf(1).equals(b.upper());
        return !toOne || com.legend.compiler.element.type.Type.isRelation(info.type());
    }

    /** Whether {@code side} holds at most one value: a to-one or zero-to-one value that is not a relation, or a literal
     *  list of at most one element. */
    private static boolean atMostOne(TypedSpec side) {
        if (side instanceof TypedCollection c) {
            return c.elements().size() <= 1;
        }
        com.legend.compiler.element.type.ExprType info = side.info();
        return info.multiplicity() instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                && b.upper() != null && b.upper() <= 1
                && !com.legend.compiler.element.type.Type.isRelation(info.type());
    }

    /** The planner's answer at an order-dependent assert: a TYPE-phase refusal naming the two fixes. */
    public static TypedSpec refused(TypedNativeCall call, NativeFn.Verdict fn, int side, List<TypedSpec> lets) {
        if (fn == NativeFn.Verdict.ASSERT) {
            // assert($a == $b): the product's == refuses an unordered side itself, by its own words, where the
            // comparison is planned (planner.RootComparison)
            return call;
        }
        throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.TYPE,
                fn.bareName() + " compares in order, and its " + (side == 0 ? "expected" : "actual")
                        + " side's order is not defined (a store or frame read with no sort, or past a grouping, a"
                        + " join or a union): the database's arrival order would decide the verdict -- sort it, or"
                        + " say assertSameElements when the order does not matter");
    }
}
