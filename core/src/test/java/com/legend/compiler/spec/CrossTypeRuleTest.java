// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.spec.typed.TypedCrossTypeComparison;
import com.legend.compiler.spec.typed.TypedSpec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE COMPARISON SETTING REACHES THE CODE A QUERY REACHES (the edition program's step 2b, 2026-11-26;
 * {@link CrossTypeRule}): the type-checker asks the door's setting where it types a comparison across types -- in the
 * query, in a function's body typed on demand, in a derived property, in a body typed into its call -- and an
 * argument written in the query keeps the query's answer when a body is typed around it. A recording setting lists
 * what it is asked; it answers the call, as legend-engine's does.
 */
class CrossTypeRuleTest {

    private static final String MODEL = """
            Class x::A { n: Integer[1]; big() { $this.n > 1.5 }: Boolean[1]; }
            function x::loose(a: Integer[1]): Boolean[1] { $a == 1.0 }
            function x::keep<T>(r: Relation<T>[1], b: Boolean[1]): Relation<T>[1] { $r->filter(x | $b) }
            function x::ones<T>(r: Relation<T>[1]): Relation<T>[1] { $r->filter(x | $x.n == 1.0) }
            function x::same<T,P,Q>(r: Relation<T>[1], a: ColSpec<P⊆T>[1], b: ColSpec<Q⊆T>[1]): Relation<T>[1]
            { $r->filter(x | $x->read($a)->toOne() == $x->read($b)->toOne()) }
            """;

    /** A setting that lists what it is asked and answers the call. */
    private static final class Recording implements CrossTypeRule {

        private final List<String> asked = new ArrayList<>();

        @Override
        public TypedSpec inQuery(TypedCrossTypeComparison c) {
            asked.add("query: " + c.left().typeName() + " " + c.operator() + " " + c.right().typeName());
            return c.comparison();
        }

        @Override
        public boolean readsBodies() {
            return true;
        }

        @Override
        public TypedSpec inBody(TypedCrossTypeComparison c) {
            asked.add("body: " + c.left().typeName() + " " + c.operator() + " " + c.right().typeName());
            return c.comparison();
        }
    }

    /** A setting that reads no body, as every door's today: asked of a body's comparison, it fails the test. */
    private static final class QueryOnly implements CrossTypeRule {

        private final List<String> asked = new ArrayList<>();

        @Override
        public TypedSpec inQuery(TypedCrossTypeComparison c) {
            asked.add("query: " + c.left().typeName() + " " + c.operator() + " " + c.right().typeName());
            return c.comparison();
        }

        @Override
        public boolean readsBodies() {
            return false;
        }

        @Override
        public TypedSpec inBody(TypedCrossTypeComparison c) {
            throw new AssertionError("a setting that reads no body was asked of one: " + c.operator());
        }
    }

    private static TypedFunction one(ModelContext ctx, String fqn) {
        List<TypedFunction> found = ctx.findFunction(fqn);
        assertEquals(1, found.size(), fqn);
        return found.get(0);
    }

    @Test
    @DisplayName("the query's own comparison is the setting's for the query, and no marker is left")
    void theQuerysComparison() {
        Recording rule = new Recording();
        TypedQuery q = Compiler.query(Compiler.compileModel(MODEL), "|1 == 1.0").comparing(rule);
        assertEquals(List.of(), CrossTypeComparisons.in(q.body()));
        assertEquals(List.of("query: Integer == Float"), rule.asked);
    }

    @Test
    @DisplayName("a function's body, typed when the function is used, and a derived property's: the setting's for a body")
    void aBodyTypedOnDemand() {
        Recording rule = new Recording();
        ModelContext ctx = Compiler.compileModel(MODEL);
        TypedQuery q = Compiler.query(ctx, "|x::loose(1)").comparing(rule);
        q.body();
        assertEquals(List.of(), rule.asked, "a body is typed when it is used, never with the query");
        q.specs().compile(one(ctx, "x::loose"));
        assertEquals(List.of("body: Integer == Float"), rule.asked);
        q.specs().compile(one(ctx, "x::A$prop$big"));
        assertEquals(List.of("body: Integer == Float", "body: Integer > Float"), rule.asked);
    }

    @Test
    @DisplayName("a body typed into its call, over the caller's own columns, is a body; an argument written in the"
            + " query keeps the query's answer")
    void aBodyTypedIntoItsCall() {
        Recording inBody = new Recording();
        Compiler.query(Compiler.compileModel(MODEL), "|#TDS\nn\n1\n#->x::ones()").comparing(inBody).body();
        assertEquals(List.of("body: Integer == Float"), inBody.asked);
        Recording argument = new Recording();
        Compiler.query(Compiler.compileModel(MODEL), "|#TDS\nn\n1\n#->x::keep(1 == 1.0)").comparing(argument).body();
        assertEquals(List.of("query: Integer == Float"), argument.asked,
                "typed once, as the query's: never again as the body's");
    }

    @Test
    @DisplayName("a setting that reads no body is never asked of one: a function's body, a body typed into its call")
    void aSettingThatReadsNoBodyIsNotAskedOfOne() {
        QueryOnly rule = new QueryOnly();
        ModelContext ctx = Compiler.compileModel(MODEL);
        TypedQuery q = Compiler.query(ctx, "|#TDS\nn\n1\n#->x::ones()->x::keep(1 == 1.0)").comparing(rule);
        q.body();
        q.specs().compile(one(ctx, "x::loose"));
        q.specs().compile(one(ctx, "x::A$prop$big"));
        assertEquals(List.of("query: Integer == Float"), rule.asked);
    }

    @Test
    @DisplayName("lite's own setting: a comparison across types is a mistake in the query (the planner refuses its"
            + " marker) and in a body (refused when the body is typed whole); a number literal against a value reads"
            + " as the value's type")
    void litesSetting() {
        ModelContext ctx = Compiler.compileModel(MODEL);
        TypedQuery q = Compiler.query(ctx, "|1 == 1.0");
        assertEquals(1, CrossTypeComparisons.in(q.body()).size());
        // $a == 1.0 over an Integer: the literal reads as 1 (the literal rule), so the body holds no marker
        assertEquals(List.of(), CrossTypeComparisons.in(q.specs().compile(one(ctx, "x::loose")).body()));
        // $this.n > 1.5 over an Integer: 1.5 is no Integer, so the body is refused by name (step 4, 2026-11-26)
        com.legend.error.ModelException refused = assertThrows(com.legend.error.ModelException.class,
                () -> q.specs().compile(one(ctx, "x::A$prop$big")));
        assertTrue(refused.getMessage().startsWith("> compares Integer with Float"), refused.getMessage());
        // the model's compile check says the same of the same body
        assertTrue(Compiler.compileAllBodies(ctx).values().stream()
                .anyMatch(m -> m.contains("> compares Integer with Float")), Compiler.compileAllBodies(ctx).toString());
    }

    @Test
    @DisplayName("lite's setting in a body typed into its call: two generic positions compare when the query runs, no"
            + " mistake of the function as written; a literal there still reads as the call's column type")
    void litesSettingInABodyTypedIntoItsCall() {
        ModelContext ctx = Compiler.compileModel(MODEL);
        // an Integer column against a Float column, through a function generic over both: no marker
        assertEquals(List.of(), CrossTypeComparisons.in(
                Compiler.query(ctx, "|#TDS\nn, f\n1, 1.5\n#->x::same(~n, ~f)").body()));
        // the same two columns compared in the query itself: a mistake
        assertEquals(1, CrossTypeComparisons.in(
                Compiler.query(ctx, "|#TDS\nn, f\n1, 1.5\n#->filter(x | $x.n == $x.f)").body()).size());
        // a literal in a generic body reads as this call's column type: $x.n == 1.0 over an Integer column
        assertEquals(List.of(), CrossTypeComparisons.in(Compiler.query(ctx, "|#TDS\nn\n1\n#->x::ones()").body()));
    }

    @Test
    @DisplayName("a query whose body was rewritten is not typed again under another setting")
    void aRewrittenBodyIsNotTypedAgain() {
        TypedQuery q = Compiler.query(Compiler.compileModel(MODEL), "|1 == 2");
        TypedQuery rewritten = q.withBody(q.body());
        assertThrows(IllegalStateException.class, () -> rewritten.comparing(new Recording()));
    }
}
