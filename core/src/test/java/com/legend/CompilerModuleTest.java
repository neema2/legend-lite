// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.element.ModelContext;
import com.legend.error.ModelException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MULTI-SOURCE module compilation ({@code Compiler.parseSources} /
 * {@code compileModel(List)}): a real legend project is many files
 * compiled together — cross-file references are normal, imports are
 * per-file (per SECTION within a file), errors name the offending FILE,
 * and duplicate definitions are reported, never silently shadowed.
 */
class CompilerModuleTest {

    @Test
    @DisplayName("module: cross-file references resolve (class in A, association in B)")
    void crossFileReferences() {
        String a = "Class my::pkg::Person { name: String[1]; }\n"
                + "Class my::pkg::Firm { legal: String[1]; }\n";
        String b = "Association my::pkg::Employment {\n"
                + "  employer: my::pkg::Firm[1];\n"
                + "  employees: my::pkg::Person[*];\n"
                + "}\n";
        ModelContext ctx = Compiler.compileModel(List.of(
                new Compiler.ModelSource("a.pure", a),
                new Compiler.ModelSource("b.pure", b)));
        assertTrue(ctx.findClass("my::pkg::Person").isPresent());
        assertTrue(ctx.findAssociationOf("my::pkg::Person", "employer").isPresent());
    }

    @Test
    @DisplayName("module: imports do NOT leak across source units")
    void importsScopedPerUnit() {
        // TWO classes share the simple name (so the unique-suffix
        // leniency — audit 16 follow-up — cannot decide): a.pure's import
        // picks my::pkg::Target; b.pure has no import and the same simple
        // name must FAIL there, not silently resolve through a.pure's
        // import.
        String shared = "Class my::pkg::Target { id: Integer[1]; }\n"
                + "Class other::pkg::Target { id: Integer[1]; }\n";
        String a = "import my::pkg::*;\n"
                + "Class my::a::UsesImport { t: Target[1]; }\n";
        String b = "Class my::b::NoImport { t: Target[1]; }\n";
        ModelContext ok = Compiler.compileModel(List.of(
                new Compiler.ModelSource("shared.pure", shared),
                new Compiler.ModelSource("a.pure", a)));
        assertTrue(ok.findClass("my::a::UsesImport").isPresent());
        ModelException ex = assertThrows(ModelException.class,
                () -> Compiler.compileModel(List.of(
                        new Compiler.ModelSource("shared.pure", shared),
                        new Compiler.ModelSource("a.pure", a),
                        new Compiler.ModelSource("b.pure", b))));
        assertTrue(ex.getMessage().contains("Target"), ex.getMessage());
        assertTrue(ex.getMessage().contains("b.pure"),
                () -> "error must name the offending FILE, got: " + ex.getMessage());
    }

    @Test
    @DisplayName("module: duplicate definitions are reported with both sources, first wins")
    void duplicatesReported() {
        String a = "Class my::pkg::Dup { x: Integer[1]; }\n";
        String b = "Class my::pkg::Dup { y: String[1]; }\n";
        Compiler.ParsedModule module = Compiler.parseSources(List.of(
                new Compiler.ModelSource("first.pure", a),
                new Compiler.ModelSource("second.pure", b)));
        assertEquals(1, module.duplicateElements().size());
        String report = module.duplicateElements().get(0);
        assertTrue(report.contains("my::pkg::Dup"), report);
        assertTrue(report.contains("second.pure"), report);
        assertTrue(report.contains("kept first.pure"), report);
        // the surviving definition is the FIRST one
        ModelContext ctx = Compiler.buildModel(module.model());
        assertTrue(ctx.findClass("my::pkg::Dup").orElseThrow()
                .properties().stream().anyMatch(p -> p.name().equals("x")));
    }

    @Test
    @DisplayName("module: one signature-broken overload does not break its healthy siblings")
    void brokenOverloadSkipped() {
        // f(Integer) is healthy; f(Missing) has a signature referencing an
        // unknown class. In a tolerant module the broken one is poisoned-
        // and-kept; candidate collection must return the healthy overload
        // (natives registered at the same FQN depend on this too).
        String src = "function my::pkg::f(x: Integer[1]): Integer[1] { $x }\n"
                + "function my::pkg::f(x: my::pkg::Missing[1]): Integer[1] { 1 }\n";
        Compiler.BuiltModule built = Compiler.buildModule(
                Compiler.parseSources(List.of(
                        new Compiler.ModelSource("m.pure", src))).model());
        assertEquals(1, built.context().findFunction("my::pkg::f").size());
        // NOTE (follow-up in #53): integrity's checkFunction does not yet
        // reject unknown PARAMETER types, so the broken overload is skipped
        // at collection rather than enumerated in the build walls — the
        // eager-diagnosis gap is tracked; the healthy-sibling guarantee is
        // what this pin protects.
    }

    @Test
    @DisplayName("module: compile errors carry the source unit's name and position")
    void errorAttribution() {
        String good = "Class my::pkg::Fine { id: Integer[1]; }\n";
        String bad = "Class my::pkg::Broken { ref: my::pkg::Missing[1]; }\n";
        ModelException ex = assertThrows(ModelException.class,
                () -> Compiler.compileModel(List.of(
                        new Compiler.ModelSource("good.pure", good),
                        new Compiler.ModelSource("bad.pure", bad))));
        assertTrue(ex.getMessage().contains("bad.pure"),
                () -> "error must name the offending file, got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("[1:"),
                () -> "error must carry the position, got: " + ex.getMessage());
    }
}
