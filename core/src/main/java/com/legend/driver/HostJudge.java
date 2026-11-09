// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.driver;

import com.legend.testexecutionplan.HostJudgment;
import com.legend.builtin.NativeFn;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import java.util.ArrayList;
import java.util.List;

/**
 * THE HOST JUDGE (task #27, 2026-09-21 — split out of AssertVerdicts): both sides of an
 * assert EXECUTE in the database and Java compares the rows ({@code testexec.Equality},
 * {@code testexec.PureAsserts}, {@code testexec.TdsCompare}) — the verdict of
 * record in host mode; in agreement mode the database judge's peer. The router
 * ({@link AssertVerdicts}) classifies the assert and hands the sides over; the database
 * judge ({@link DatabaseJudge}) is the other arm of the same router.
 */

final class HostJudge {
    private HostJudge() {
    }

    // ---- the arm's sides as one verdict step's base (the seam program's S2f, 2026-10-30) ----------------------

    /** A side as {@code AssertVerdicts.side} fetched it: folded where literal, no identity lane. */
    /** A statement read as a value side: what the walk's generic path plans for an assert no arm plans. */
    static HostSides.Ask statementAsk(TypedSpec statement, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return valueAsk(statement, hook);
    }

    private static HostSides.Ask valueAsk(TypedSpec arg, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return new HostSides.Ask(arg, false, true, hook);
    }

    /** A side on the IDENTITY LANE without a canon rider (F13c: the assert-condition/predicate evaluator; in-SQL
     *  eq/equal needs instance identity, and the boolean egress keeps every other lane blind to the field): folded
     *  where literal. */
    private static HostSides.Ask identityAsk(TypedSpec arg,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return new HostSides.Ask(arg, true, true, hook);
    }

    /** A side the canon fetch reads ({@link #sideCanonOf}): never folded, on the identity lane. */
    private static HostSides.Ask canonAsk(TypedSpec arg, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return new HostSides.Ask(arg, true, false, hook);
    }

    /** The static facts of one side, read once at planning. */
    private static HostJudgment.SideFacts facts(TypedSpec s) {
        return JudgmentFacts.sideFacts(s);
    }

    /** {@link #tdsEquivalent} planned: the two grids and the tolerances, one verdict's base (M6: the walk read the
     *  tolerances after the grids, a group of their own; literal tolerances send nothing either way). */
    static HostSides.Check planTdsEquivalent(List<TypedSpec> targs,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        List<HostSides.Ask> asks = new java.util.ArrayList<>(List.of(valueAsk(targs.get(0), hook),
                valueAsk(targs.get(1), hook), valueAsk(targs.get(2), hook)));
        if (targs.size() == 4) {
            asks.add(valueAsk(targs.get(3), hook));
        }
        return new HostSides.Check(asks, new HostJudgment.TdsEquivalent(targs.size() == 4));
    }

    /** The GRID PAIR ({@code assertEquals} / {@code assertNotEquals} over two statically relation-stamped sides,
     *  D3): both grids executed in definition order, the grid owner ({@code testexec.TdsCompare#grids}:
     *  columns ordered, rows under the order view of the actual side) judges -- never a byte decline. */
    static HostSides.Check planGridPair(String name, boolean wantEqual, List<TypedSpec> args,
            List<TypedSpec> letPrefix, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        boolean sorted = com.legend.compiler.spec.OrderView.of(args.get(1), letPrefix)
                == com.legend.compiler.spec.OrderView.SORTED;
        return new HostSides.Check(List.of(valueAsk(args.get(0), hook), valueAsk(args.get(1), hook)),
                new HostJudgment.GridPair(name, wantEqual, sorted));
    }

    static HostSides.Check planSize(List<TypedSpec> args, List<TypedSpec> letPrefix,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return new HostSides.Check(List.of(valueAsk(args.get(1), hook), valueAsk(args.get(0), hook)),
                new HostJudgment.Size(AssertVerdicts.envelopeValuesRead(args.get(0), letPrefix)));
    }

    static HostSides.Check planContains(List<TypedSpec> args,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return new HostSides.Check(List.of(valueAsk(args.get(0), hook), valueAsk(args.get(1), hook)),
                new HostJudgment.Contains(facts(args.get(0)), facts(args.get(1))));
    }

    static HostSides.Check planTolerance(List<TypedSpec> args,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return new HostSides.Check(List.of(valueAsk(args.get(0), hook), valueAsk(args.get(1), hook),
                valueAsk(args.get(2), hook)), new HostJudgment.Tolerance());
    }

    static HostSides.Check planCondition(String name, TypedSpec cond, boolean wantTrue,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        // forAll-contains SUBSET (the functionvariables idiom
        // — the harness's audited fc arm, moved to the owner):
        // both sides evaluate IN THE DATABASE; the membership
        // fold is assert-level logic judged host-side (a
        // subquery inside a SQL lambda cannot lower, and
        // pure's own evaluation of this shape is in-memory).
        TypedSpec[] fc = AssertVerdicts.forAllContains(cond);
        if (fc != null) {
            return new HostSides.Check(List.of(valueAsk(fc[0], hook), valueAsk(fc[1], hook)),
                    new HostJudgment.Subset(name, wantTrue));
        }
        // F13c: the CONDITION rides the identity lane — eq/
        // equal over instances compile the engine relation
        // (identity/key canon); the egress is one boolean, so
        // no other lane ever sees the identity field
        return new HostSides.Check(List.of(identityAsk(cond, hook)), new HostJudgment.Condition(name, wantTrue));
    }

    /** {@code check} with the caller's failure message (the asserts-as-plan-nodes program, step 2): the message one
     *  more side, folded where it is a constant (a literal costs no statement). */
    static HostSides.Check messaged(HostSides.Check check, TypedSpec message,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        List<HostSides.Ask> asks = new java.util.ArrayList<>(check.asks());
        asks.add(new HostSides.Ask(message, false, true, hook));
        return new HostSides.Check(asks, new HostJudgment.Messaged(check.judgment(), check.asks().size()));
    }

    static HostSides.Check planEmpty(TypedSpec arg, boolean wantEmpty,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        // §8 leg 1: a TABULAR side's emptiness is its ROW count
        // (engine relation semantics) — no canon involved
        return new HostSides.Check(List.of(valueAsk(arg, hook)), new HostJudgment.Empty(wantEmpty));
    }

    static HostSides.Check planEq(List<TypedSpec> args, List<TypedSpec> letPrefix, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        HostJudgment.SideFacts e = facts(args.get(0));
        HostJudgment.SideFacts a = facts(args.get(1));
        HostJudgment.Keys keys = JudgmentFacts.keys(args.get(0), args.get(1), env.ctx());
        return new HostSides.Check(List.of(canonAsk(args.get(0), hook), canonAsk(args.get(1), hook)),
                new HostJudgment.Eq(e, a, keys));
    }

    static HostSides.Check planInstanceOf(List<TypedSpec> args, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        // the classes the type accepts: itself and its subtree, the model's subtype relation read at planning (the
        // judgment reads the wire's classifier against the set); a type argument that is no reference names none, and
        // the judgment declines the side it read (the walk's generic path, as before)
        String type = AssertVerdicts.typeRefName(args.get(1));
        java.util.Set<String> accepted = new java.util.LinkedHashSet<>();
        if (type != null) {
            accepted.add(type);
            accepted.addAll(env.ctx().subtree(type));
        }
        return new HostSides.Check(List.of(valueAsk(args.get(0), hook)), new HostJudgment.InstanceOf(type, accepted));
    }

    /** {@link #is} planned; null (no side asked) where the pair is not two elements. */
    static HostSides.@com.legend.base.Nullable Check planIs(String name, List<TypedSpec> args, SpecCompiler specs,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        // ELEMENT IDENTITY (metamodel-as-relations D2/D3): a tracked
        // element's identity is its row's primary key — `is` over an
        // element reference and a metamodel row is the same equality
        // the chain normalizer rewrites to a key compare
        // (ChainNormalizer.identityEquality); adjudicated as the
        // condition assert on the identity lane
        if (AssertVerdicts.elementTyped(args.get(0), specs) && AssertVerdicts.elementTyped(args.get(1), specs)) {
            TypedSpec cond = com.legend.resolver.ChainNormalizer.identityCondition(
                    specs.ctx(), args.get(0), args.get(1));
            return new HostSides.Check(List.of(identityAsk(cond, hook)), new HostJudgment.Is(name));
        }
        return null;
    }

    static HostSides.Check planSameElements(List<TypedSpec> args, boolean gridPair, List<TypedSpec> letPrefix,
            BodyDriver.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        HostJudgment.SideFacts e = facts(args.get(0));
        HostJudgment.SideFacts a = facts(args.get(1));
        HostJudgment.Keys keys = JudgmentFacts.keys(args.get(0), args.get(1), env.ctx());
        return new HostSides.Check(List.of(canonAsk(args.get(0), hook), canonAsk(args.get(1), hook)),
                new HostJudgment.SameElements(e, a, gridPair, keys));
    }

    /** The VECTOR judgment planned (M10c item 5): one identity-lane side, the predicate vector; judged by
     *  {@link #allRowsOf} as {@link #quantified} judges its read. */
    static HostSides.Check planAllRows(String fqn, TypedSpec predMap, boolean wantTrue, String message,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return new HostSides.Check(List.of(identityAsk(predMap, hook)),
                new HostJudgment.AllRows(fqn, wantTrue, message));
    }

    /** A quantified assert over rows planned for the program road (HostJudgment.Rows): its asks the zip's arms or the
     *  one relation source, its arguments read at planning -- a row's field, the row, or a literal; any other argument
     *  refused by name, never evaluated here (the walk's rule, {@link #rowValue}). Null: another family or source. */
    static @com.legend.base.Nullable HostSides.Check planRows(String fqn, TypedSpec source,
            com.legend.compiler.spec.typed.TypedLambda lam, TypedSpec root, List<TypedSpec> letPrefix,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        com.legend.builtin.NativeFn.Verdict family = com.legend.builtin.NativeFn.Verdict
                .of(com.legend.compiler.spec.typed.Calls.calleeIdOf(root)).orElse(null);
        boolean isEquals = family == com.legend.builtin.NativeFn.Verdict.ASSERT_EQUALS;
        boolean isTolerance = family == com.legend.builtin.NativeFn.Verdict.ASSERT_EQ_WITHIN_TOLERANCE;
        List<TypedSpec> args = com.legend.compiler.spec.typed.Calls.argsOf(root);
        if (!isEquals && !isTolerance || args.size() < (isTolerance ? 3 : 2)) {
            return null;
        }
        String binder = lam.parameters().get(0);
        List<HostJudgment.RowArg> read = new java.util.ArrayList<>();
        for (TypedSpec arg : args.subList(0, isTolerance ? 3 : 2)) {
            HostJudgment.RowArg a = rowArg(arg, binder, fqn);
            if (a == null) {
                return null;   // an argument computed per element: not this read's
            }
            read.add(a);
        }
        List<TypedSpec> arms = new java.util.ArrayList<>();
        TypedSpec bound = com.legend.compiler.spec.typed.Lets.bound(source, letPrefix);
        zipArms(bound, letPrefix, arms);
        List<HostSides.Ask> asks = new java.util.ArrayList<>(arms.size());
        for (TypedSpec arm : arms) {
            asks.add(valueAsk(arms.size() == 1 ? source : arm, hook));
        }
        HostJudgment.Pairing pairing = arms.size() == 1 ? null : pairing(bound, letPrefix, new int[] {0});
        return new HostSides.Check(asks, new HostJudgment.Rows(fqn, isTolerance, read,
                com.legend.compiler.element.EqualityKeys.fqnOf(args.get(0).info().type()),
                com.legend.compiler.element.EqualityKeys.fqnOf(args.get(1).info().type()), pairing));
    }

    /** A row assert's argument as data: {@link #rowValue}'s shapes, read at planning. */
    private static HostJudgment.@com.legend.base.Nullable RowArg rowArg(TypedSpec arg, String binder, String fqn) {
        return switch (arg) {
            case com.legend.compiler.spec.typed.TypedCast c -> rowArg(c.source(), binder, fqn);
            case com.legend.compiler.spec.typed.TypedVariable v when v.name().equals(binder) ->
                    new HostJudgment.RowArg.Whole();
            case com.legend.compiler.spec.typed.TypedPropertyAccess pa
                    when pa.source() instanceof com.legend.compiler.spec.typed.TypedVariable v
                            && v.name().equals(binder) -> new HostJudgment.RowArg.Field(pa.property());
            // a literal as its carrier and its text: read back to the same value at the step (the plan-as-data
            // design's step 4)
            case com.legend.compiler.spec.typed.TypedCString l -> new HostJudgment.RowArg.Literal("String", l.value());
            case com.legend.compiler.spec.typed.TypedCInteger l -> new HostJudgment.RowArg.Literal(
                    l.value().getClass().getSimpleName(), l.value().toString());
            case com.legend.compiler.spec.typed.TypedCFloat l -> new HostJudgment.RowArg.Literal("Double",
                    Double.toString(l.value()));
            case com.legend.compiler.spec.typed.TypedCBoolean l -> new HostJudgment.RowArg.Literal("Boolean",
                    Boolean.toString(l.value()));
            // another argument is computed per element: the database computes it (the per-element form's operand
            // pairs, or the unroll's per-element sides), never this read of the row (2026-11-04)
            default -> null;
        };
    }

    /** A quantified equality element by element on the host (the asserts-as-plan-nodes program, step 5): its one
     *  side the operand pairs the database computes, each element judged as the element arm judges a pair
     *  ({@code testexec.PureAsserts}: the same decision, the same message) -- nothing computed here. */
    static HostSides.Check planPerElement(String fqn, TypedSpec pairs, com.legend.compiler.element.type.ExprType expected,
            com.legend.compiler.element.type.ExprType actual, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return new HostSides.Check(List.of(valueAsk(pairs, hook)), new HostJudgment.Rows(fqn, false,
                List.of(new HostJudgment.RowArg.Field("first"), new HostJudgment.RowArg.Field("second")),
                com.legend.compiler.element.EqualityKeys.fqnOf(expected.type()),
                com.legend.compiler.element.EqualityKeys.fqnOf(actual.type()), null));
    }

    /** The zip's pairing over its arms, in {@link #zipArms}' order. */
    private static HostJudgment.Pairing pairing(TypedSpec arm, List<TypedSpec> letPrefix, int[] next) {
        TypedSpec bound = com.legend.compiler.spec.typed.Lets.bound(arm, letPrefix);
        if (bound instanceof com.legend.compiler.spec.typed.TypedNativeCall z
                && com.legend.compiler.element.type.PlatformTypes.COLLECTION_ZIP.equals(z.callee().qualifiedName())
                && z.args().size() == 2) {
            HostJudgment.Pairing left = pairing(z.args().get(0), letPrefix, next);
            HostJudgment.Pairing right = pairing(z.args().get(1), letPrefix, next);
            return new HostJudgment.Pairing.Zip(left, right);
        }
        return new HostJudgment.Pairing.Arm(next[0]++);
    }

    /** The arms of a (nested) zip, left to right; a source that is no zip is its own one arm. */
    private static void zipArms(TypedSpec source, List<TypedSpec> letPrefix, List<TypedSpec> out) {
        if (source instanceof com.legend.compiler.spec.typed.TypedNativeCall z
                && com.legend.compiler.element.type.PlatformTypes.COLLECTION_ZIP.equals(z.callee().qualifiedName())
                && z.args().size() == 2) {
            for (TypedSpec arm : z.args()) {
                TypedSpec bound = com.legend.compiler.spec.typed.Lets.bound(arm, letPrefix);
                if (bound instanceof com.legend.compiler.spec.typed.TypedNativeCall inner
                        && com.legend.compiler.element.type.PlatformTypes.COLLECTION_ZIP.equals(
                                inner.callee().qualifiedName())) {
                    zipArms(bound, letPrefix, out);
                } else {
                    out.add(arm);
                }
            }
            return;
        }
        out.add(source);
    }

    static HostSides.Check planEquals(String name, boolean wantEqual, List<TypedSpec> args, boolean incidental,
            boolean gridPair, List<TypedSpec> letPrefix, BodyDriver.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        HostJudgment.SideFacts e = facts(args.get(0));
        HostJudgment.SideFacts a = facts(args.get(1));
        HostJudgment.Keys keys = JudgmentFacts.keys(args.get(0), args.get(1), env.ctx());
        HostJudgment.FlatFacts flat = new HostJudgment.FlatFacts(
                AssertVerdicts.bareRowStamp(args.get(0), letPrefix), AssertVerdicts.bareRowStamp(args.get(1), letPrefix),
                AssertVerdicts.wrappedRelationStamp(args.get(0), letPrefix),
                AssertVerdicts.wrappedRelationStamp(args.get(1), letPrefix));
        return new HostSides.Check(List.of(canonAsk(args.get(0), hook), canonAsk(args.get(1), hook)),
                new HostJudgment.Equals(name, wantEqual, incidental, gridPair, e, a, keys, flat));
    }

    /** Upstream's {@code assertEquals} failure text for two values ({@code '\nexpected: %r\nactual:   %r'}, the host
     *  judge's spelling): an engine-text arm declining the texts its verdict step read, a plain String equality that
     *  failed (2026-11-04, the core program). */
    static String equalsMessage(Object expected, Object actual) {
        return "\nexpected: " + com.legend.testexecutionplan.Verdicts.repr(expected) + "\nactual:   "
                + com.legend.testexecutionplan.Verdicts.repr(actual);
    }

    static HostSides.Check planJsonStringsEqual(List<TypedSpec> args, List<TypedSpec> letPrefix,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return new HostSides.Check(List.of(valueAsk(args.get(0), hook), valueAsk(args.get(1), hook)),
                new HostJudgment.Json(com.legend.compiler.spec.OrderView.of(args.get(1), letPrefix)
                        == com.legend.compiler.spec.OrderView.INCIDENTAL));
    }

    /** {@link #rendered} planned; {@code counted} whether the route is counted where the judgment holds (the host
     *  judge's own plan path counts it, as {@code AssertVerdicts.adjudicate} counts the walk's once the arm answered). */
    static HostSides.Check planRendered(String name, boolean wantEqual, List<TypedSpec> args, String form,
            TypedSpec rendered, @com.legend.base.Nullable String eForm, @com.legend.base.Nullable String aForm,
            boolean orderedForm, List<TypedSpec> letPrefix, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook,
            boolean counted) {
        // a BOTH-rendered pair always judges as a multiset (even
        // sorted queries legally tie-flip between two executions)
        boolean sorted = orderedForm
                && (eForm == null ^ aForm == null)
                && com.legend.compiler.spec.OrderView.of(rendered, letPrefix)
                        == com.legend.compiler.spec.OrderView.SORTED;
        return new HostSides.Check(List.of(valueAsk(args.get(0), hook), valueAsk(args.get(1), hook)),
                new HostJudgment.Rendered(name, wantEqual, form, eForm, aForm, sorted, counted));
    }
}
