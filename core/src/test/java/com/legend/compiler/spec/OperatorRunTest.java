// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.element.PureModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.model.ParsedModel;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * An operator run ({@code a + b + c}, {@code plus([a, b, c])}) is decided by the call's RESOLVED declaration -- one
 * of the platform's {@code [*]} arithmetic carriers ({@code NativeFn.OperatorRun}) -- never by the name it was written
 * with (2026-10-21: a set of spellings decided it, before overload resolution on one of its two paths).
 */
class OperatorRunTest {

    private static final String MODEL = """
            Class a::C
            {
              v: Integer[1];
            }
            function a::plus(cs: a::C[*]): Integer[1]
            {
              $cs.v->sum()
            }
            function a::platform(): Integer[1]
            {
              1 + 2 + 3
            }
            function a::platformWritten(): Integer[1]
            {
              plus([1, 2, 3])
            }
            function a::own(): Integer[1]
            {
              a::plus([^a::C(v = 1), ^a::C(v = 2)])
            }
            """;

    private static List<TypedSpec> body(String fqn) {
        ParsedModel p = com.legend.testing.Own.model(MODEL);
        PureModelContext c = (PureModelContext) com.legend.Compiler.buildModel(p);
        List<TypedFunction> fns = c.findFunction(fqn);
        assertEquals(1, fns.size(), fqn);
        return new SpecCompiler(c).compile(fns.get(0)).body();
    }

    /** Each collection in the tree, and whether it carries the operator-run mark. */
    private static List<Boolean> runs(List<TypedSpec> body) {
        List<Boolean> out = new ArrayList<>();
        java.util.ArrayDeque<TypedSpec> todo = new java.util.ArrayDeque<>(body);
        while (!todo.isEmpty()) {
            TypedSpec t = todo.pop();
            if (t instanceof TypedCollection c && c.elements().size() >= 2) {
                out.add(c.operatorRun());
            }
            todo.addAll(t.children());
        }
        return out;
    }

    @Test
    void thePlatformsArithmeticOverAListIsARun() {
        assertEquals(List.of(true), runs(body("a::platform")));
        assertEquals(List.of(true), runs(body("a::platformWritten")));
    }

    @Test
    void aUsersOwnPlusOverAListIsACallNotARun() {
        assertEquals(List.of(false), runs(body("a::own")));
    }
}
