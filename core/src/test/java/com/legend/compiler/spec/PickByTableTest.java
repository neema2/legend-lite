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
import com.legend.model.FunctionId;
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

    /** A user body under a catalog native's id IS that function (build rebuild Phase 3): resolution offers the
     *  catalog's declaration alone, the table's row for the id is the rule (Intrinsic), and the call is minted
     *  native — the max[1..*] class that regressed channel B once such a body was admitted. */
    @Test
    void aBodyUnderACatalogIdIsThatFunctionAndRunsByItsRule() {
        ModelContext ctx = Compiler.compileModel(
                "function meta::pure::functions::string::toUpper(s:String[1]):String[1] { 'never' }");
        List<TypedFunction> found = List.copyOf(ctx.findFunctionById(TWIN_ID));
        assertEquals(1, found.size(), "the body under the catalog's id is not a second candidate: " + found);
        TypedFunction fn = found.get(0);
        assertFalse(fn.definition() instanceof FunctionDefinition, "the catalog's declaration stands");
        Implementation row = ctx.implementations().of(FunctionId.of(fn.definition()));
        assertInstanceOf(Implementation.Intrinsic.class, row);
        assertTrue(((Implementation.Intrinsic) row).positions().contains(Implementation.Position.SCALAR));
        TypedSpec call = CallNodes.mint(ctx.implementations(), fn, List.of(),
                new ExprType(fn.returnType(), fn.returnMultiplicity()));
        assertInstanceOf(TypedNativeCall.class, call);
        assertEquals(fn, ((TypedNativeCall) call).callee());
    }

    /** Another version under a catalog FQN, with no row of its own, is a candidate and is refused (NO_ROW):
     *  the platform never runs upstream's body for a function it implements (build rebuild Phase 3). */
    @Test
    void aVersionOfACatalogFunctionWithNoRowIsACandidateAndRefused() {
        ModelContext ctx = Compiler.compileModel(
                "function meta::pure::functions::string::toUpper(s:String[*]):String[*] { $s }");
        TypedFunction version = ctx.findFunction("meta::pure::functions::string::toUpper").stream()
                .filter(f -> f.definition() instanceof FunctionDefinition).findFirst().orElseThrow();
        Implementation row = ctx.implementations().of(FunctionId.of(version.definition()));
        assertInstanceOf(Implementation.Refused.class, row);
        assertEquals(Implementation.Reason.NO_ROW, ((Implementation.Refused) row).reason());
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
            assertInstanceOf(Implementation.Intrinsic.class, row, f.id().toString());
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
