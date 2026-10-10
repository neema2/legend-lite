// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.element.ModelContext;
import com.legend.error.ModelException;
import com.legend.model.ParsedModel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Build rebuild Phase 3b, item 5b: an element's import scope, source and position are its own
 * ({@link ParsedModel#keyOf}), not its name's. Keyed by name, a function's overloads shared one record and the
 * last file read set the import scope for all of them — in the manifest census, core's {@code from} overloads
 * (which import {@code meta::core::runtime::*} for their {@code Runtime} parameter) resolved under the import-less
 * scope of a later file that also declares {@code from}, and {@code routeFunction} likewise lost {@code Mapping}.
 */
class ImportScopePerElementTest {

    private static final String TYPES = """
            ###Pure
            Class a::A { x: Integer[1]; }
            Class b::B { y: Integer[1]; }
            """;
    /** Declares {@code my::f(A)} and can see {@code A} only through its own import. */
    private static final String WITH_A = """
            ###Pure
            import a::*;
            function my::f(v: A[1]): Integer[1] { $v.x }
            """;
    /** Declares {@code my::f(B)} with its own import; sees no {@code A}. */
    private static final String WITH_B = """
            ###Pure
            import b::*;
            function my::f(v: B[1]): Integer[1] { $v.y }
            """;

    @Test
    @DisplayName("two files, one overload each, different imports: both overloads resolve, in either file order")
    void overloadsAcrossFilesKeepTheirOwnImports() {
        for (List<Compiler.ModelSource> order : List.of(
                List.of(src("types.pure", TYPES), src("fa.pure", WITH_A), src("fb.pure", WITH_B)),
                List.of(src("types.pure", TYPES), src("fb.pure", WITH_B), src("fa.pure", WITH_A)))) {
            ModelContext ctx = Compiler.compileModel(order);
            assertEquals(2, ctx.findFunction("my::f").size(), order.get(1).name() + " first");
        }
    }

    @Test
    @DisplayName("one file, two sections, one overload each: the second section's imports apply to its overload")
    void overloadsAcrossSectionsKeepTheirOwnImports() {
        String one = TYPES + WITH_A + WITH_B;
        assertEquals(2, Compiler.compileModel(one).findFunction("my::f").size());
        assertEquals(2, Compiler.compileModel(List.of(src("one.pure", one))).findFunction("my::f").size());
    }

    @Test
    @DisplayName("the side maps hold one record per overload, under the function id")
    void sideMapsAreKeyedPerOverload() {
        Compiler.ParsedModule module = Compiler.parseSources(
                List.of(src("types.pure", TYPES), src("fa.pure", WITH_A), src("fb.pure", WITH_B)));
        ParsedModel model = module.model();
        assertEquals("fa.pure", model.elementSources().get("my::f_A_1__Integer_1_"));
        assertEquals("fb.pure", model.elementSources().get("my::f_B_1__Integer_1_"));
        assertEquals(List.of("a"), model.elementImports().get("my::f_A_1__Integer_1_").wildcards());
        assertEquals(List.of("b"), model.elementImports().get("my::f_B_1__Integer_1_").wildcards());
        assertEquals("a::A", ParsedModel.keyOf(model.elements().get(0)), "a class's key is its name");
    }

    @Test
    @DisplayName("a strict error about one overload names that overload and its own file and line")
    void errorNamesTheOverloadAndItsFile() {
        // fb.pure's overload names a type that does not exist: the error names that overload, in its file
        String broken = "###Pure\nimport b::*;\nfunction my::f(v: Nope[1]): Integer[1] { 1 }\n";
        ModelException e = assertThrows(ModelException.class, () -> Compiler.compileModel(
                List.of(src("types.pure", TYPES), src("fa.pure", WITH_A), src("fb.pure", broken))));
        assertEquals("my::f_Nope_1__Integer_1_", e.element());
        assertTrue(e.getMessage().startsWith("fb.pure [3:"), e.getMessage());
    }

    private static Compiler.ModelSource src(String name, String text) {
        return new Compiler.ModelSource(name, text);
    }
}
