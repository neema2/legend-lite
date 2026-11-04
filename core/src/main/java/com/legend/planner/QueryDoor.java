// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.builtin.NativeFn;
import com.legend.builtin.Pure;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.StatementEffects;
import com.legend.compiler.spec.typed.ExecutionContext;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.NotImplementedException;
import com.legend.model.FunctionId;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * THE QUERY DOOR (the test-is-the-product plan's B6, R13's N1; M3's brief B12 item 1, narrowed by M3a's measurements):
 * what a test's program may hold and a product query may not, found by one scan of the typed body and refused by
 * name at the first statement holding it, before anything is planned. The kinds ({@link TestOnly}) are what only the
 * test door ({@code Execution.runProgram}, its program planned by the test-planning library) plans: an effect; a
 * {@code from()}'s inline test data, or the test data of a named runtime OTHER than the plan's own (the plan's own
 * runtime's test data is its target's setup already, {@code PlanMaker.target}: M3a found 47 working server queries
 * naming it); a let-bound {@code execute(...)} (Pure runs it at the let, a test's eager run; an inline lazy read stays
 * the product's, D15, R17 C12); an assert; an {@code assertError}. {@code fail} is a raise, not a verdict here: a
 * Cypher write's guard calls it, and the product plans it as a check step (M3a: the 16 queries that reach it). A
 * refusal is an error, not an output spelling (R17 C8).
 */
public final class QueryDoor {

    /** A kind of statement only a test's program holds, in the order the scan asks (B12 item 1's, but for
     *  {@code assertError} before the assert it is one of: the specific name wins). */
    public enum TestOnly {
        EFFECT("an effect", "effects run in a test's program"),
        INLINE_SEEDS("a from()'s inline test data", "seeds are a test's"),
        RUNTIME_SEEDS("the test data of a runtime that is not the plan's",
                "seeds are a test's; the plan's own runtime's are its target's setup"),
        LET_EXECUTE("a let-bound execute(...)",
                "Pure runs it at the let, a test's eager run; a query reads an execute(...)'s result inline"),
        ASSERT_ERROR("an assertError", "asserts are a test's"),
        ASSERT("an assert", "asserts are a test's");

        private final String what;
        private final String why;

        TestOnly(String what, String why) {
            this.what = what;
            this.why = why;
        }

        /** The kind as a refusal names it. */
        public String what() {
            return what;
        }
    }

    /** The scan's memos, one per reach (a memo belongs to one predicate): one per program. */
    public static final class Scan {
        private final TypedQuery query;
        private final @Nullable String runtime;
        private final Map<FunctionId, Boolean> effects = new HashMap<>();
        private final Map<FunctionId, Boolean> catches = new HashMap<>();
        private final Map<FunctionId, Boolean> verdicts = new HashMap<>();

        /** A scan of {@code query}'s statements, {@code runtime} the plan's own. */
        public Scan(TypedQuery query, @Nullable String runtime) {
            this.query = query;
            this.runtime = runtime;
        }

        /** The first kind {@code statement} holds, or null: a statement the query door plans. */
        public @Nullable TestOnly of(TypedSpec statement) {
            EnumSet<TestOnly> kinds = kinds(statement);
            return kinds.isEmpty() ? null : kinds.iterator().next();
        }

        /** Every kind {@code statement} holds, in the scan's order. */
        public EnumSet<TestOnly> kinds(TypedSpec statement) {
            EnumSet<TestOnly> kinds = EnumSet.noneOf(TestOnly.class);
            SpecCompiler specs = query.specs();
            if (StatementEffects.containsEffect(statement, specs, effects)) {
                kinds.add(TestOnly.EFFECT);
            }
            for (TypedFrom fr
                    : ExecutionContext.froms(statement)) {
                if (!fr.sqlSetups().isEmpty() || !fr.csvSetups().isEmpty()) {
                    kinds.add(TestOnly.INLINE_SEEDS);
                }
                var named = fr.context().runtime();
                if (named.isPresent() && !named.get().fullPath().equals(runtime)
                        && declaresTestData(named.get().fullPath(), query.context())) {
                    kinds.add(TestOnly.RUNTIME_SEEDS);
                }
            }
            if (statement instanceof TypedLet let
                    && LetPlanner.rhs(let) instanceof TypedNativeCall ec
                    && NativeFn.Handle.runsAFrame(ec.callee().id())) {
                kinds.add(TestOnly.LET_EXECUTE);
            }
            if (StatementEffects.reaches(statement, n -> n instanceof TypedNativeCall nc
                    && NativeFn.ContextOwner.of(nc.callee().id()).isPresent(), specs, catches)) {
                kinds.add(TestOnly.ASSERT_ERROR);
            }
            if (StatementEffects.reaches(statement, QueryDoor::isAssert, specs, verdicts)) {
                kinds.add(TestOnly.ASSERT);
            }
            return kinds;
        }
    }

    private QueryDoor() {
    }

    /** Refuses {@code query}, planned for {@code runtime}, by name at its first statement holding a kind only a
     *  test's program holds. */
    public static void refuseTestOnly(TypedQuery query, @Nullable String runtime) {
        Scan scan = new Scan(query, runtime);
        for (TypedSpec statement : query.body()) {
            TestOnly kind = scan.of(statement);
            if (kind != null) {
                throw new NotImplementedException(kind.what + " in a query: " + kind.why
                        + ", and the product's query door plans none");
            }
        }
    }

    /** Whether {@code statement} is an {@code assertError(f, ...)} at its root -- not a let -- whose function's body
     *  holds no assert of its own, or IS one assert (the scan names such a statement both an assertError and an
     *  assert, the call being a verdict call): the program road plans it as one verdict step, f's body its side (the
     *  walker-deletion program, 2026-11-03), or f's assert planned as any assert and its failure the error assertError
     *  matches (the asserts-as-plan-nodes program, 2026-11-04: upstream's assertError catches an assert's failure as
     *  any error). */
    public static boolean assertErrorRoot(TypedSpec statement, SpecCompiler specs) {
        if (!(statement instanceof TypedNativeCall call)
                || NativeFn.ContextOwner.of(call.callee().id()).isEmpty() || call.args().isEmpty()) {
            return false;
        }
        return !StatementEffects.reaches(call.args().get(0), QueryDoor::isAssert, specs, new HashMap<>())
                || innerAssert(call, specs) != null;
    }

    /** The one assert {@code assertError}'s function body is -- a single statement, a verdict call (no assertError
     *  itself) whose arguments hold no other assert -- or null. */
    public static @Nullable TypedSpec innerAssert(TypedNativeCall assertError, SpecCompiler specs) {
        if (!(assertError.args().get(0) instanceof com.legend.compiler.spec.typed.TypedLambda f)
                || !f.parameters().isEmpty() || f.body().size() != 1
                || !(f.body().get(0) instanceof TypedNativeCall inner) || !isAssert(inner)
                || NativeFn.ContextOwner.of(inner.callee().id()).isPresent()) {
            return null;
        }
        for (TypedSpec arg : inner.args()) {
            if (StatementEffects.reaches(arg, QueryDoor::isAssert, specs, new HashMap<>())) {
                return null;
            }
        }
        return inner;
    }

    /** An assert's call: a verdict function, {@code fail} aside (a raise; by its declarations). */
    private static boolean isAssert(TypedSpec n) {
        return StatementEffects.isVerdictCall(n) && !(n instanceof TypedNativeCall nc
                && Pure.AT_ASSERTS_FAIL.contains(nc.callee().id()));
    }

    /** Whether the runtime {@code fqn} binds a connection that declares test data (its connection's seeds). */
    public static boolean declaresTestData(String fqn, ModelContext ctx) {
        var rt = ctx.findRuntime(fqn).orElseThrow(() -> new IllegalStateException("a from() names the runtime '"
                + fqn + "', which the model does not hold"));
        for (var binding : rt.connectionBindings().entrySet()) {
            for (String connFqn : binding.getValue()) {
                var cd = ctx.findConnection(connFqn).orElseThrow(() -> new IllegalStateException("the runtime '"
                        + fqn + "' binds the connection '" + connFqn + "', which the model does not hold"));
                if (cd.specification() != null) {
                    var data = cd.specification().testData();
                    if (data != null && ((data.sqls() != null && !data.sqls().isEmpty()) || data.csv() != null)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
