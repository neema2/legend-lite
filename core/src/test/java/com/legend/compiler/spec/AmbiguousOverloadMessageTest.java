// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Build rebuild Phase 3b, item 5a: the "ambiguous overload" error names every candidate with all of its parameters.
 * It printed each candidate's first parameter, so two no-argument candidates crashed the message with an index
 * error instead of an error (nine upstream bodies in the manifest census, each calling a no-argument function
 * declared both in the caller's package and in an imported one).
 */
class AmbiguousOverloadMessageTest {

    @Test
    @DisplayName("two no-argument candidates tie: an error naming both, not an index crash")
    void zeroParameterCandidatesTieWithAMessage() {
        ModelContext ctx = Compiler.compileModel("""
                ###Pure
                function a::f(): Integer[1] { 1 }
                function b::f(): String[1] { 's' }
                ###Pure
                import b::*;
                function a::g(): Any[1] { f() }
                """);
        Map<String, String> walls = Compiler.compileAllBodies(ctx);
        String wall = walls.get("a::g__Any_1_");
        assertTrue(wall != null, "a::g's body is walled: " + walls);
        // the name is as the resolver left it (the import's b::f; the caller's own package adds a::f as a
        // candidate, the parked W2.3b difference); both candidates, each with its empty parameter list
        assertTrue(wall.contains("ambiguous overload of '"), wall);
        assertTrue(wall.contains("a::f:module():Integer[1]") && wall.contains("b::f:module():String[1]"), wall);
        assertEquals(1, walls.size(), walls.toString());
    }
}
