// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.Compiler;
import com.legend.builtin.NativeFn;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.model.DerivedPropertyNames;
import com.legend.model.FunctionDefinition;
import com.legend.platform.FunctionId;
import com.legend.platform.Implementation;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE PICK BY TABLE (platform architecture untangle, step 4a): a call node's
 * kind is decided by the implementation table's row for the resolved
 * declaration, never by the declaration's kind. A bodied declaration the
 * platform runs by a rule is minted a native call — the {@code max[1..*]}
 * class that regressed channel B once such a body was admitted.
 */
class PickByTableTest {

    private static final String TWIN_ID = "meta::pure::functions::string::toUpper_String_1__String_1_";

    /** A user body under a catalog native's id: the table says Intrinsic (the
     *  rule is registered by key), so the body's call is minted native. */
    @Test
    void aBodiedDeclarationWithARuleIsMintedANativeCall() {
        ModelContext ctx = Compiler.compileModel(
                "function meta::pure::functions::string::toUpper(s:String[1]):String[1] { 'never' }");
        TypedFunction body = ctx.findFunctionById(TWIN_ID).stream()
                .filter(f -> f.definition() instanceof FunctionDefinition).findFirst().orElseThrow();
        Implementation row = ctx.implementations().of(FunctionId.of(body.definition()));
        assertInstanceOf(Implementation.Intrinsic.class, row);
        assertTrue(((Implementation.Intrinsic) row).positions().contains(Implementation.Position.SCALAR));
        TypedSpec call = CallNodes.mint(ctx.implementations(), body, List.of(),
                new ExprType(body.returnType(), body.returnMultiplicity()));
        assertInstanceOf(TypedNativeCall.class, call);
        assertEquals(body, ((TypedNativeCall) call).callee());
    }

    /** A user body nothing registers is a Body row and is minted a user call — inlined. */
    @Test
    void aPlainBodyIsMintedAUserCall() {
        ModelContext ctx = Compiler.compileModel("function my::pkg::twice(x:Integer[1]):Integer[1] { $x + $x }");
        TypedFunction body = ctx.findFunction("my::pkg::twice").get(0);
        assertInstanceOf(Implementation.Body.class, ctx.implementations().of(FunctionId.of(body.definition())));
        TypedSpec call = CallNodes.mint(ctx.implementations(), body, List.of(),
                new ExprType(body.returnType(), body.returnMultiplicity()));
        assertInstanceOf(TypedUserCall.class, call);
    }

    /** The row accessors are class members a family implements: the lifted
     *  declaration's row is Intrinsic[RowGetter] by its provenance, so its
     *  call is minted native and RowGetters lowers it — no name list. */
    @Test
    void aFamilyImplementedMemberIsIntrinsicByProvenance() {
        ModelContext ctx = Compiler.compileModel("");
        NativeFn.RowGetter getter = NativeFn.RowGetter.GET_STRING;
        String lifted = DerivedPropertyNames.lifted(getter.owner(), getter.property());
        List<TypedFunction> at = ctx.findFunction(lifted);
        assertFalse(at.isEmpty(), lifted);
        for (TypedFunction f : at) {
            Implementation row = ctx.implementations().of(FunctionId.of(f.definition()));
            assertInstanceOf(Implementation.Intrinsic.class, row, f.signatureKey());
            assertEquals(Set.of(NativeFn.RowGetter.class), ((Implementation.Intrinsic) row).families());
            assertInstanceOf(TypedNativeCall.class, CallNodes.mint(ctx.implementations(), f, List.of(),
                    new ExprType(f.returnType(), f.returnMultiplicity())));
        }
    }

    /** The context's tables cover the boot layer too: an upstream native the
     *  prelude carries respelled is declared, and Unimplemented. */
    @Test
    void theContextTablesCoverTheBootLayer() {
        ModelContext ctx = Compiler.compileModel("Class my::pkg::A { n: Integer[1]; }");
        assertEquals(List.of(), ctx.implementations().conflicts());
        List<com.legend.model.Function> at = ctx.declarations()
                .at("meta::pure::functions::collection::removeAllOptimized");
        assertFalse(at.isEmpty(), "the prelude's respelled native is a declaration of the context");
        assertInstanceOf(Implementation.Unimplemented.class, ctx.implementations().of(FunctionId.of(at.get(0))));
    }
}
