// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The catalog-dispatch staging contract (charter §4AG, census §10l):
 * JAVA_ROUTINE calls evaluate by exact-FQN lookup wherever they stand;
 * CONTEXT_OWNER calls own their arguments' evaluation — staging must
 * NOT enter them (user catch 2026-08-31: pre-staging a walling call
 * inside assertError's lambda would escape the catch the engine
 * applies; the corpus carries ZERO witnesses of the shape — in-lambda
 * census 2026-08-31 — so THIS test is the committed witness).
 */
class NativeDispatchTest {

    /** A callee WITH its catalog declaration — a callee is dispatched by its
     *  identity (execution plan step 2), so a test callee carries one too; a
     *  name the catalog does not declare is a test bug, loudly. */
    private static TypedNativeCall call(String fqn, TypedSpec... args) {
        var catalog = com.legend.builtin.Pure.nativeFunctionsAt(fqn);
        if (catalog.isEmpty()) {
            throw new IllegalArgumentException("not a catalog native: " + fqn);
        }
        com.legend.model.NativeFunctionDefinition def = catalog.get(0);
        TypedFunction callee = new TypedFunction(fqn, List.of(), List.of(),
                List.of(), Type.Primitive.STRING,
                com.legend.compiler.element.type.Multiplicity.Bounded.ONE, Optional.empty(),
                true, def, com.legend.model.FunctionId.of(def));
        return new TypedNativeCall(callee, List.of(args),
                ExprType.one(Type.Primitive.STRING));
    }

    @Test
    @DisplayName("JAVA_ROUTINE calls stage to literals wherever they stand")
    void routineStagesNested() {
        TypedSpec nested = call("meta::pure::functions::string::toUpper",
                call(PlatformTypes.PLAN_TO_STRING));
        TypedSpec staged = NativeDispatch.stage(nested, List.of(),
                Map.of(PlatformTypes.PLAN_TO_STRING, (c, lets) -> "TEXT"));
        TypedNativeCall outer = (TypedNativeCall) staged;
        assertEquals("TEXT",
                ((TypedCString) outer.args().get(0)).value());
    }

    @Test
    @DisplayName("CONTEXT_OWNER arguments are never entered (assertError witness)")
    void contextOwnerArgumentsAreNeverEntered() {
        // the BOMB routine: invoking it at staging time IS the defect
        NativeDispatch.Routine bomb = (c, lets) -> {
            throw new AssertionError("staging entered a CONTEXT_OWNER's"
                    + " arguments — the wall would escape assertError's"
                    + " catch");
        };
        TypedSpec lambda = new TypedLambda(List.of(),
                List.of(call(PlatformTypes.PLAN_TO_STRING)),
                ExprType.one(Type.Primitive.STRING));
        TypedSpec assertError = call(PlatformTypes.ASSERT_ERROR, lambda,
                new TypedCString("expected message",
                        ExprType.one(Type.Primitive.STRING)));
        TypedSpec staged = NativeDispatch.stage(assertError, List.of(),
                Map.of(PlatformTypes.PLAN_TO_STRING, bomb));
        assertSame(assertError, staged);
    }

    @Test
    @DisplayName("a JAVA_ROUTINE row without a registered routine is loud")
    void missingRoutineIsLoud() {
        assertThrows(com.legend.error.NotImplementedException.class,
                () -> NativeDispatch.stage(
                        call(PlatformTypes.PLAN_TO_STRING),
                        List.of(), Map.of()));
    }
}
