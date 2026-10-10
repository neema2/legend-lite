// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler;

import com.legend.Compiler;
import com.legend.model.FunctionDefinition;
import com.legend.model.ParsedModel;
import com.legend.protocol.spec.AppliedFunction;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Review #13: import scopes were keyed by element FQN, so overloads of one FQN declared in different sections all
 *  resolved with the first section's imports (ElementParser's putIfAbsent; NameResolver's lookup). Fixed 2026-10-09
 *  (build rebuild Phase 3b, item 5b): the side maps are keyed per element ({@code ParsedModel.keyOf}, a function's id),
 *  so each overload carries its own section's scope; the pin came off and this is the regression. */
class SectionImportScopeKnownDefectTest {

    private static final String SOURCE = "###Pure\n"
            + "function a::whichOne(): String[1] { 'a' }\n"
            + "function b::whichOne(): String[1] { 'b' }\n"
            + "\n###Pure\n"
            + "import a::*;\n"
            + "function test::f(x: Integer[1]): String[1] { whichOne() }\n"
            + "\n###Pure\n"
            + "import b::*;\n"
            + "function test::f(x: String[1]): String[1] { whichOne() }\n";

    @Test
    void eachOverloadResolvesInItsOwnSectionsImports() {
        ParsedModel resolved = NameResolver.resolve(Compiler.parseModel(SOURCE));
        List<FunctionDefinition> overloads = resolved.elements().stream()
                .filter(FunctionDefinition.class::isInstance).map(FunctionDefinition.class::cast)
                .filter(f -> f.qualifiedName().equals("test::f")).toList();
        if (overloads.size() != 2) {
            throw new IllegalStateException("expected two test::f overloads, got " + overloads.size());
        }
        String first = ((AppliedFunction) overloads.get(0).body().get(0)).function();
        if (!first.equals("a::whichOne")) {
            throw new IllegalStateException("precondition: section 2 (import a::*) resolves to a::whichOne, got " + first);
        }
        String second = ((AppliedFunction) overloads.get(1).body().get(0)).function();
        assertEquals("b::whichOne", second, "section 3 imports b::*, so its overload's bare call must bind there");
    }
}
