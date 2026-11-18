// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.builtin.Pure;
import com.legend.error.LegendCompileException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * THE COMPILER'S OWN NATIVES ARE NOT A USER'S TO CALL (PR #118 fix round, 2026-11-18, N1; the edge follow-ups' F32):
 * {@code meta::legend::lite::typeRaise} and {@code meta::legend::lite::cellText} are written by the compiler alone (the
 * resolver's chain dispatch and casts, the match checker; the TDS cell's text), born with their record
 * ({@code compiler.spec.Calls}) after the name resolver. A user's call to either -- by its full name, in a query or a
 * function's body -- is refused by name where the user's names are resolved (a bare name is none of the user's, an import
 * of the package included); the platform's own uses keep compiling (their tests: the TDS cell's text, the type-check
 * raises).
 */
class InternalNativeCallTest {

    private static final List<String> CALLS = List.of(
            Pure.Lite.TYPE_RAISE + "('CAST_IMPOSSIBLE', 'a', 'b')",
            Pure.Lite.CELL_TEXT + "('x')",
            // the in-body assert's default message (N2, the same day): the compiler's too
            Pure.Lite.ASSERT_FAILED_TEXT + "()");

    @Test
    void aQueryCallingAnInternalNativeByItsFullNameIsRefusedByName() {
        var ctx = Compiler.compileModel("Class demo::A { x: String[1]; }");
        for (String call : CALLS) {
            String fqn = call.substring(0, call.indexOf('('));
            LegendCompileException e = assertThrows(LegendCompileException.class,
                    () -> Compiler.query(ctx, "|" + call), call);
            assertTrue(e.getMessage().contains("'" + fqn + "' is the compiler's own"), e.getMessage());
        }
    }

    @Test
    void aReferenceUnderEvalAndACallBySignatureIdAreRefusedToo() {
        // the two routes around the call's own name (the re-review of the batch, 2026-11-18): a function reference
        // under eval, and a call by the overload's signature id -- each names the base function
        var ctx = Compiler.compileModel("Class demo::A { x: String[1]; }");
        for (String query : List.of("|" + Pure.Lite.CELL_TEXT + "->eval('x')",
                "|" + Pure.Lite.CELL_TEXT + "_String_$0_1$__String_1_('x')")) {
            LegendCompileException e = assertThrows(LegendCompileException.class,
                    () -> Compiler.query(ctx, query), query);
            assertTrue(e.getMessage().contains("'" + Pure.Lite.CELL_TEXT + "' is the compiler's own"), e.getMessage());
        }
    }

    @Test
    void aFunctionBodyCallingOneIsRefusedByName() {
        for (String call : CALLS) {
            String fqn = call.substring(0, call.indexOf('('));
            LegendCompileException e = assertThrows(LegendCompileException.class,
                    () -> Compiler.compileModel("function demo::f(): Any[1] { " + call + " }"), call);
            assertTrue(e.getMessage().contains("'" + fqn + "' is the compiler's own"), e.getMessage());
        }
    }

    @Test
    void aBareNameIsNoneOfTheUsersEvenThroughAnImport() {
        // the bare-name index holds no internal name (NativeCatalogGovernanceTest), and an import of the package does
        // not reach one: the body's call is an unknown function, as it was before the refusal by name
        for (String call : CALLS) {
            String bare = call.substring(Pure.Lite.PKG.length());
            String name = bare.substring(0, bare.indexOf('('));
            List<String> errors = Compiler.compileErrors("import meta::legend::lite::*;\nfunction demo::f(): Any[1] { "
                    + bare + " }");
            assertTrue(errors.size() == 1 && errors.get(0).contains("unknown function '" + name + "'"),
                    String.valueOf(errors));
        }
    }
}
