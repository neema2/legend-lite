// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.legend.model.NativeFunctionDefinition;
import com.legend.builtin.Pure;
import com.legend.compiler.element.PureModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.model.FunctionId;
import java.math.BigInteger;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The folds over two spelled integers compare them exactly. The parser keeps a literal beyond long a
 * {@link BigInteger}, and {@code longValue()} wraps it: until 2026-10-14 {@link LiteralUnroll}'s compares and
 * {@link NormalizeFolds}' equality read the wrapped value, so {@code 99999999999999999999 > 9223372036854775807}
 * folded to false (the tenth line's find, reached from the islands). Both now read {@link TypedCInteger#big()}.
 */
class ExactIntegerFoldTest {

    private static final ExprType INT = ExprType.one(Type.Primitive.INTEGER);
    private static final ExprType BOOL = ExprType.one(Type.Primitive.BOOLEAN);
    private static final NativeFunctionDefinition GT = Pure.GREATER_THAN__NUMBER_1__NUMBER_1;
    private static final NativeFunctionDefinition LT = Pure.LESS_THAN__NUMBER_1__NUMBER_1;
    private static final NativeFunctionDefinition EQ = Pure.EQ__ANY_1__ANY_1;
    private static final NativeFunctionDefinition EQUAL = Pure.EQUAL__ANY_MANY__ANY_MANY;
    private static final String BEYOND = "99999999999999999999";
    /** {@code 99999999999999999999 + 2^64}: the same long once wrapped, a different integer. */
    private static final String BEYOND_PLUS_2_64 = "118446744073709551615";

    private static PureModelContext ctx() {
        return (PureModelContext) com.legend.Compiler.buildModel(
                com.legend.testing.Own.model("Class model::Person {}\n"));
    }

    private static TypedCInteger spelled(String digits) {
        return new TypedCInteger(new BigInteger(digits), INT);
    }

    private static TypedCInteger spelled(long v) {
        return new TypedCInteger(v, INT);
    }

    /** The native call the typer builds for two integers: {@code overload}'s own function, over them. */
    private static TypedNativeCall call(PureModelContext c, NativeFunctionDefinition overload,
            TypedSpec l, TypedSpec r) {
        FunctionId id = FunctionId.of(overload);
        TypedFunction f = c.findFunction(overload.qualifiedName()).stream().filter(fn -> fn.id().equals(id))
                .findFirst().orElseThrow(() -> new AssertionError("the model has no " + id));
        return new TypedNativeCall(f, List.of(l, r), BOOL, null);
    }

    private static boolean folded(TypedSpec s) {
        return assertInstanceOf(TypedCBoolean.class, s, "the compare of two spelled integers folds").value();
    }

    @Test
    void aLiteralBeyondLongComparesExactly() {
        PureModelContext c = ctx();
        assertEquals(true, folded(LiteralUnroll.fold(
                call(c, GT, spelled(BEYOND), spelled(Long.MAX_VALUE)), c)),
                BEYOND + " > " + Long.MAX_VALUE + " (longValue() wrapped the literal below long's maximum)");
        assertEquals(false, folded(LiteralUnroll.fold(
                call(c, LT, spelled(BEYOND), spelled(Long.MAX_VALUE)), c)));
        assertEquals(true, folded(LiteralUnroll.fold(
                call(c, LT, spelled(BEYOND), spelled(BEYOND_PLUS_2_64)), c)),
                "two literals beyond long with the same low 64 bits compare by their value");
        assertEquals(true, folded(LiteralUnroll.fold(
                call(c, GT, spelled(7L), spelled(3L)), c)), "within long, as before");
    }

    @Test
    void twoLiteralsBeyondLongWithTheSameLowBitsAreNotEqual() {
        PureModelContext c = ctx();
        assertEquals(false, folded(NormalizeFolds.fold(
                call(c, EQ, spelled(BEYOND), spelled(BEYOND_PLUS_2_64)))),
                BEYOND + " == " + BEYOND_PLUS_2_64 + " (their longValue()s agree)");
        assertEquals(true, folded(NormalizeFolds.fold(
                call(c, EQ, spelled(BEYOND), spelled(BEYOND)))));
        assertEquals(true, folded(NormalizeFolds.fold(
                call(c, EQ, spelled(5L), spelled(5L)))), "within long, as before");
    }

    @Test
    void theInlinerFoldOfEqualityIsExactToo() {
        // LiteralUnroll.fold is what every inliner rewrite runs (UserCallInliner.rewrite); its equality fold compares
        // the spelled values, which carried longValue() until the audit of 2026-10-14
        PureModelContext c = ctx();
        for (NativeFunctionDefinition f : List.of(EQ, EQUAL)) {
            assertEquals(false, folded(LiteralUnroll.fold(
                    call(c, f, spelled(BEYOND), spelled(BEYOND_PLUS_2_64)), c)),
                    f.qualifiedName() + ": " + BEYOND + " == " + BEYOND_PLUS_2_64);
            assertEquals(true, folded(LiteralUnroll.fold(call(c, f, spelled(BEYOND), spelled(BEYOND)), c)),
                    f.qualifiedName() + ": a literal beyond long equals itself");
            assertEquals(true, folded(LiteralUnroll.fold(call(c, f, spelled(5L), spelled(5L)), c)),
                    f.qualifiedName() + ": within long, as before");
        }
    }
}
