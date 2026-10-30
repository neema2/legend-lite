// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testcases.PlanCases;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Object identity (docs/OBJECT_IDENTITY_2026_10_20.md): every evaluation of a construction makes a new object and a let
 * names one; {@code eq} is identity, and {@code ==} and everything built on it compare a keyed class by its keys and a
 * keyless class by identity. Each case is Pure's own answer, run through the plan on each embedded database that holds
 * an object as a value (H2 has no struct: its PCT lane runs the relation suite alone).
 */
class ObjectIdentityTest {

    private static final String MODEL = """
            Class s::P { n: Integer[1]; }
            Class s::Side { <<equality.Key>> id: String[1]; n: Integer[1]; }
            function s::mk():s::P[1] { ^s::P(n=1) }
            function s::same(p: s::P[1]):s::P[1] { $p }
            function s::mkAll(xs: Integer[*]):s::P[*] { $xs->map(x | ^s::P(n=1)) }
            """;

    /** Each case: Pure's answer, then the query. */
    private static final String[][] CASES = {
        // a let names one object; two constructions are two
        {"false", "|let a = ^s::P(n=1); let b = ^s::P(n=1); eq($a, $b);"},
        {"false", "|let a = ^s::P(n=1); let b = ^s::P(n=1); $a == $b;"},
        {"1", "|let a = ^s::P(n=1); [$a, $a]->removeDuplicates()->size();"},
        {"2", "|let a = ^s::P(n=1); [$a, $a, ^s::P(n=1)]->removeDuplicates()->size();"},
        {"false", "|let a = ^s::P(n=1); [^s::P(n=1)]->contains($a);"},
        // a keyed class compares its keys
        {"1", "|[^s::Side(id='a', n=1), ^s::Side(id='a', n=2)]->removeDuplicates()->size()"},
        {"true", "|[^s::Side(id='a', n=1)]->contains(^s::Side(id='a', n=2))"},
        {"true", "|^s::Side(id='a', n=1) == ^s::Side(id='a', n=2)"},
        {"1", "|[1, 2]->map(i | ^s::Side(id='a', n=$i))->removeDuplicates()->size()"},
        // a construction inside a map is evaluated once per element
        {"2", "|[1, 2]->map(i | ^s::P(n=1))->removeDuplicates()->size()"},
        {"4", "|[1, 2]->map(i | [^s::P(n=1), ^s::P(n=1)])->removeDuplicates()->size()"},
        {"4", "|[1, 2]->map(i | [3, 4]->map(j | ^s::P(n=1)))->removeDuplicates()->size()"},
        // a let in a lambda's body names one object per evaluation of the body
        {"2", "|[1, 2]->map(i | let a = ^s::P(n=$i); [$a, $a];)->removeDuplicates()->size()"},
        {"2", "|[1, 2]->map(i | let a = ^s::P(n=1); [$a, $a];)->removeDuplicates()->size()"},
        {"2", "|[1, 2]->map(i | let a = ^s::P(n=$i); $a == $a;)->filter(b | $b)->size()"},
        // a lambda body's let is spliced into its uses before typing: the copies of a call are its one evaluation
        {"2", "|[1, 2]->map(i | let a = s::mk(); [$a, $a];)->removeDuplicates()->size()"},
        {"2", "|[1, 2]->map(i | let a = s::mk(); $a == $a;)->filter(b | $b)->size()"},
        // a let read inside a nested lambda keeps its values (its objects' identity is refused below)
        {"2", "|[1, 2]->map(i | let a = ^s::P(n=$i); [3, 4]->map(j | $a.n);)->removeDuplicates()->size()"},
        // an object built outside a map is one object inside it
        {"1", "|let a = ^s::P(n=1); [1, 2]->map(i | $a)->removeDuplicates()->size();"},
        // each call is an evaluation of its body; an argument is the caller's object
        {"false", "|s::mk() == s::mk()"},
        {"true", "|let a = ^s::P(n=1); s::same($a) == $a;"},
        {"2", "|[1, 2]->map(i | s::mk())->removeDuplicates()->size()"},
        {"4", "|s::mkAll([1, 2])->concatenate(s::mkAll([1, 2]))->removeDuplicates()->size()"},
        // a lambda's body is evaluated where the lambda is used
        {"2", "|let f = {x: Integer[1] | ^s::P(n=1)}; [1, 2]->map($f)->removeDuplicates()->size();"},
        {"false", "|let f = {| ^s::P(n=1)}; $f->eval() == $f->eval();"},
        {"true", "|let a = ^s::P(n=1); let f = {| $a}; $f->eval() == $f->eval();"},
        // every operation built on equal(): union, distinct, isDistinct, indexOf, a collection's ==
        {"2", "|[^s::P(n=1)]->union([^s::P(n=1)])->size()"},
        {"1", "|let a = ^s::P(n=1); [$a]->union([$a])->size();"},
        {"1", "|[^s::Side(id='a', n=1)]->union([^s::Side(id='a', n=2)])->size()"},
        {"1", "|let a = ^s::P(n=1); [^s::P(n=1), $a]->indexOf($a);"},
        {"-1", "|let a = ^s::P(n=1); [^s::P(n=1)]->indexOf($a);"},
        {"1", "|[^s::Side(id='a', n=1), ^s::Side(id='b', n=2)]->indexOf(^s::Side(id='b', n=9))"},
        {"2", "|[^s::P(n=1), ^s::P(n=1)]->distinct()->size()"},
        {"1", "|[^s::Side(id='a', n=1), ^s::Side(id='a', n=2)]->distinct()->size()"},
        {"true", "|[^s::P(n=1), ^s::P(n=1)]->isDistinct()"},
        {"false", "|let a = ^s::P(n=1); [$a, $a]->isDistinct();"},
        {"false", "|[^s::Side(id='a', n=1), ^s::Side(id='a', n=2)]->isDistinct()"},
        {"true", "|let a = ^s::P(n=1); [$a] == [$a];"},
        {"false", "|[^s::P(n=1)] == [^s::P(n=1)]"},
        {"true", "|[^s::Side(id='a', n=1), ^s::Side(id='b', n=1)] == [^s::Side(id='a', n=2), ^s::Side(id='b', n=3)]"},
        {"false", "|[^s::Side(id='a', n=1), ^s::Side(id='b', n=1)] == [^s::Side(id='b', n=2), ^s::Side(id='a', n=3)]"},
    };

    @Test
    void everyEvaluationMakesAnObject_aLetNamesOne_onThePlatform() throws Exception {
        assertEveryEvaluationMakesAnObject(com.legend.testbackend.TestBackends.platform());    // SQLite: its own test
    }

    /** {@link #everyEvaluationMakesAnObject_aLetNamesOne_onThePlatform} on {@code backend} (main's merge, 2026-10-22:
     *  one test named DuckDB and SQLite; each database's run is its own test's). */
    static void assertEveryEvaluationMakesAnObject(com.legend.testbackend.TestBackend backend) throws Exception {
        List<String> wrong = new ArrayList<>();
        {
            DatabaseType type = backend.type();
            String model = com.legend.test.StorelessRuntime.with(MODEL, type);
            for (String[] c : CASES) {
                try (Connection connection = backend.open()) {
                    String got = PlanCases.pureText(Compiler.query(Compiler.compileModel(model), c[1])
                            .executionPlan(com.legend.test.StorelessRuntime.RUNTIME), connection);
                    if (!got.equals(c[0])) {
                        wrong.add(type + ": " + c[1] + " answered " + got + ", Pure " + c[0]);
                    }
                } catch (RuntimeException e) {
                    wrong.add(type + ": " + c[1] + " raised " + e.getMessage());
                }
            }
        }
        assertEquals(List.of(), wrong);
    }

    /** Where the product does not track an object's evaluations (docs/OBJECT_IDENTITY_2026_10_20.md §4), reading its
     *  identity raises by name: never an answer from an id shared by every row, iteration or copy. */
    @Test
    void anUntrackedObjectsIdentityIsRefusedByName() throws Exception {
        String model = com.legend.test.StorelessRuntime.with(MODEL, com.legend.database.Databases.PLATFORM);
        String[] queries = {
            // a lambda body's let read inside a nested lambda: Pure's 2, a use per inner element here
            "|[1, 2]->map(i | let a = ^s::P(n=$i); [3, 4]->map(j | $a);)->removeDuplicates()->size()",
        };
        for (String q : queries) {
            try (Connection connection = com.legend.testbackend.TestBackends.platform().open()) {
                var plan = Compiler.query(Compiler.compileModel(model), q)
                        .executionPlan(com.legend.test.StorelessRuntime.RUNTIME);
                RuntimeException e = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                        () -> PlanCases.pureText(plan, connection), q);
                assertTrue(e.getMessage().contains("an object's identity is read where the product does not track"),
                        q + ": " + e.getMessage());
            }
        }
        // built in a fold's reducer the database runs (one evaluation for every iteration): no id is minted for it,
        // the initial value's objects keep theirs (the fold's reduce takes an accumulator of the elements' own type on
        // DuckDB, so a fold that builds objects from integers does not run there yet: the SQL is the witness)
        String fold = Compiler.query(Compiler.compileModel(model),
                "|range(0, 3, 1)->fold({i, acc | [^s::P(n=$i)]->concatenate($acc)}, [^s::P(n=8), ^s::P(n=9)])"
                        + "->removeDuplicates()->size()").plan(com.legend.test.StorelessRuntime.RUNTIME).sql();
        assertTrue(fold.contains("'__id': CAST(NULL AS VARCHAR)") && fold.contains("'__id': 'o1'"), fold);
    }

    /** A map's result is the concatenation of its mapper's values: a mapper whose value is a list flattens. */
    @Test
    void aMapConcatenatesItsMappersValues() throws Exception {
        String model = com.legend.test.StorelessRuntime.with("Class s::Q { xs: Integer[*]; }",
                com.legend.database.Databases.PLATFORM);
        String[][] cases = {
            {"3", "|[^s::Q(xs=[1, 2]), ^s::Q(xs=[3])]->map(q | $q.xs)->size()"},
            {"4", "|[1, 2]->map(i | [3, 4]->map(j | $i + $j))->size()"},
            {"4", "|[1, 2]->map(i | [3, 4]->filter(j | $j > 0))->size()"},
            {"2", "|[^s::Q(xs=[1, 2]), ^s::Q(xs=[3])]->map(q | $q.xs->size())->size()"},
        };
        for (String[] c : cases) {
            try (Connection connection = com.legend.testbackend.TestBackends.platform().open()) {
                assertEquals(c[0], PlanCases.pureText(Compiler.query(Compiler.compileModel(model), c[1])
                        .executionPlan(com.legend.test.StorelessRuntime.RUNTIME), connection), c[1]);
            }
        }
    }
}
