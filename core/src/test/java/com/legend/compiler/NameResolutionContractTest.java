// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler;

import com.legend.Compiler;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The NAME_RESOLUTION_BUG.md contract, pinned failing-before /
 * passing-after: a reference resolves through the referring element's
 * OWN import scope — explicit imports, wildcard imports, and the
 * implicit own-package import — with the platform prelude as a
 * FALLBACK tier. There is NO global-scan leniency: an element the
 * file never made visible is unresolvable, exactly like real Pure.
 */
class NameResolutionContractTest {

    /** §2 repro: the only myDB lives in pkg::B; the referring file
     * imports pkg::A::* (which defines nothing). Real Pure rejects
     * this model; the deleted global-scan fallback used to bind it
     * silently and emit SQL against the unimported store. */
    @Test
    @DisplayName("unimported cross-package store is UNRESOLVABLE (the FqnBug repro)")
    void unimportedStoreDoesNotBind() {
        String f1 = "\n###Relational\nDatabase pkg::B::myDB ( Table T (ID INTEGER, NAME VARCHAR(50)) )";
        String f2 = "import pkg::A::*;\n"
                + "Class model::Person { name: String[1]; }\n"
                + "\n###Mapping\nimport pkg::A::*;\nMapping my::M ( *model::Person: Relational "
                + "{ ~mainTable [myDB] T name: T.NAME } )";
        var ctx = Compiler.compileModel(List.of(
                new Compiler.ModelSource("f1.pure", f1),
                new Compiler.ModelSource("f2.pure", f2)));
        assertTrue(ctx.findDatabase("myDB").isEmpty(),
                "bare unimported name must not bind via any global scan");
        assertTrue(ctx.findDatabase("pkg::B::myDB").isPresent(),
                "exact FQN still binds");
    }

    @Test
    @DisplayName("imported store resolves; the same-named unimported one never shadows it")
    void importedStoreWins() {
        String f1 = "\n###Relational\nDatabase pkgA::myDB ( Table T_A (ID INTEGER, N VARCHAR(50)) )";
        String f2 = "\n###Relational\nDatabase pkgB::myDB ( Table T_B (ID INTEGER, N VARCHAR(50)) )";
        String f3 = "import pkgA::*;\n"
                + "Class model::P { n: String[1]; }\n"
                + "\n###Mapping\nimport pkgA::*;\nMapping my::M ( *model::P: Relational "
                + "{ ~mainTable [myDB] T_A n: T_A.N } )";
        var ctx = Compiler.compileModel(List.of(
                new Compiler.ModelSource("f1.pure", f1),
                new Compiler.ModelSource("f2.pure", f2),
                new Compiler.ModelSource("f3.pure", f3)));
        var md = ctx.findLegacyMapping("my::M").orElseThrow();
        var cm = (com.legend.model.ClassMapping.Relational)
                md.classMappings().get(0);
        assertEquals("pkgA::myDB", java.util.Objects.requireNonNull(
                cm.mainTable()).database(),
                "the file's own wildcard import chooses among same-named stores");
    }

    /** Real pure's implicit same-package import (§2.4b): a sibling in
     * the element's OWN package is visible bare, no import needed. */
    @Test
    @DisplayName("own-package sibling resolves bare without any import")
    void ownPackageSiblingResolves() {
        String src = "\n###Relational\nDatabase app::DB ( Table T (ID INTEGER, N VARCHAR(50)) )\n"
                + "\n###Pure\nClass app::P { n: String[1]; }\n"
                + "\n###Mapping\nMapping app::M ( *app::P: Relational "
                + "{ ~mainTable [DB] T n: T.N } )";
        var ctx = Compiler.compileModel(src);
        var md = ctx.findLegacyMapping("app::M").orElseThrow();
        var cm = (com.legend.model.ClassMapping.Relational)
                md.classMappings().get(0);
        assertEquals("app::DB", java.util.Objects.requireNonNull(
                cm.mainTable()).database());
    }

    /** The retired resolveMappedClassName case, now owned by the
     * general precedence: a USER class visible via wildcards beats the
     * prelude metaclass of the same simple name (relation::View). */
    @Test
    @DisplayName("user class shadows a prelude metaclass (View) via wildcard import")
    void userClassShadowsPrelude() {
        String src = "import model::*;\n"
                + "Class model::View { label: String[1]; }\n"
                + "Class model::Raw { label: String[1]; }\n"
                + "\n###Relational\nDatabase store::DB ( Table R (LABEL VARCHAR(50)) )\n"
                + "\n###Mapping\nimport model::*;\nMapping model::M (\n"
                + "    Raw: Relational { ~mainTable [store::DB] R label: [store::DB] R.LABEL }\n"
                + "    View: Pure { ~src Raw label: $src.label }\n"
                + ")";
        var ctx = Compiler.compileModel(src);
        var md = ctx.findLegacyMapping("model::M").orElseThrow();
        boolean bindsUserView = md.classMappings().stream().anyMatch(cm ->
                cm instanceof com.legend.model.ClassMapping.Pure p
                        && p.className().equals("model::View"));
        assertTrue(bindsUserView,
                "mapping set target resolves to the user's model::View, not"
                        + " meta::relational::metamodel::relation::View");
    }

    /** OWN PACKAGE is a tier BELOW imports, never a peer: the corpus's
     * testUnionPartial.pure resolves bare 'Address' to the IMPORTED
     * simple::Address in an import-bearing section even though a
     * same-package partial::Address exists (the engine compiles both
     * spellings; a peer-level own package read as a fake ambiguity). */
    @Test
    @DisplayName("an import match wins over a same-package sibling (no ambiguity)")
    void importBeatsOwnPackage() {
        String src = "Class ext::Address { name: String[1]; }\n"
                + "Class app::Address { name: String[1]; }\n"
                + "\n###Relational\nDatabase app::DB ( Table T (NAME VARCHAR(50)) )\n";
        String mapping = "import ext::*;\n"
                + "\n###Mapping\nimport ext::*;\nMapping app::M ( *Address: Relational "
                + "{ ~mainTable [app::DB] T name: [app::DB] T.NAME } )";
        var ctx = Compiler.compileModel(List.of(
                new Compiler.ModelSource("m1.pure", src),
                new Compiler.ModelSource("m2.pure", mapping)));
        var md = ctx.findLegacyMapping("app::M").orElseThrow();
        var cm = (com.legend.model.ClassMapping.Relational)
                md.classMappings().get(0);
        assertEquals("ext::Address", cm.className(),
                "the import claims the name; own package stays the fallback");
    }

    /** CALL position has NO user/prelude tiering (real pure): a user
     * function capturing a bare call name must not HIDE same-named
     * prelude natives — both travel as candidates and the signature
     * picks. Regression: corpus relation::schema(rel) newly visible in
     * the global compile starved toDDL's schema($db, $name) 2-arg call
     * of the platform schema(Database,String). (Batch 142 made schema()
     * a system-metamodel Pure accessor, so the example native here is
     * joinStrings — the contract is the same.) */
    /** The candidate universe a model context memoizes (platform ∪ model), built
     *  here once per test — the per-call overload that built it is gone
     *  (step 3 homework, 2026-09-26). */
    private static java.util.Set<String> universe(java.util.Set<String> modelFqns) {
        java.util.Set<String> known = new java.util.HashSet<>(
                com.legend.compiler.NameResolver.platformFqns());
        known.addAll(modelFqns);
        return java.util.Set.copyOf(known);
    }

    @Test
    @DisplayName("prelude natives join user candidates at call position")
    void preludeNativesJoinCallCandidates() {
        var imports = new com.legend.model.ImportScope.Builder()
                .add("app::fns::*").build();
        var call = new com.legend.protocol.spec.AppliedFunction("joinStrings",
                List.of(new com.legend.protocol.spec.Variable("strs"),
                        new com.legend.protocol.spec.CString(",")));
        var resolved = (com.legend.protocol.spec.AppliedFunction)
                com.legend.compiler.NameResolver.resolveQueryIn(call, imports,
                        universe(java.util.Set.of("app::fns::joinStrings")));
        assertTrue(resolved.candidateFqns().contains("app::fns::joinStrings"),
                "the user wildcard candidate is carried");
        assertTrue(resolved.candidateFqns().contains(
                        "meta::pure::functions::string::joinStrings"),
                "the prelude native joins the candidate set instead of"
                        + " being shadowed");
    }

    /** 4b.1: an engine native in a package the FILE imports qualifies through
     *  that import — the universe now holds the platform's function FQNs. */
    @Test
    @DisplayName("a bare engine native qualifies through the file's own import")
    void importedEngineNativeQualifies() {
        var imports = new com.legend.model.ImportScope.Builder()
                .add("meta::relational::metamodel::execute::*").build();
        var call = new com.legend.protocol.spec.AppliedFunction("executeInDb",
                List.of(new com.legend.protocol.spec.CString("select 1"),
                        new com.legend.protocol.spec.Variable("conn")));
        var resolved = (com.legend.protocol.spec.AppliedFunction)
                com.legend.compiler.NameResolver.resolveQueryIn(call, imports, universe(java.util.Set.of()));
        assertEquals("meta::relational::metamodel::execute::executeInDb", resolved.function());
    }

    /** 4b.1: CALL position collects EVERY core-group package a name lives in —
     *  the candidates ride the node and the signature picks. */
    @Test
    @DisplayName("a core-group name in several packages carries every one as a candidate")
    void coreGroupNameCarriesEveryPackage() {
        var call = new com.legend.protocol.spec.AppliedFunction("map",
                List.of(new com.legend.protocol.spec.Variable("xs"), new com.legend.protocol.spec.Variable("f")));
        var resolved = (com.legend.protocol.spec.AppliedFunction)
                com.legend.compiler.NameResolver.resolveQuery(call);
        assertEquals("map", resolved.function(), "several packages: the spelling stays, the candidates ride");
        assertTrue(resolved.candidateFqns().contains("meta::pure::functions::collection::map"), String.valueOf(resolved.candidateFqns()));
        assertTrue(resolved.candidateFqns().contains("meta::pure::functions::relation::map"), String.valueOf(resolved.candidateFqns()));
    }

    /** 4b.1: a name in ONE core package qualifies, and the form that owns that
     *  FQN still dispatches (CoreFn.of by the owned map). */
    @Test
    @DisplayName("a unique core-group name qualifies and keeps its form")
    void uniqueCoreGroupNameQualifiesAndKeepsItsForm() {
        var call = new com.legend.protocol.spec.AppliedFunction("extend",
                List.of(new com.legend.protocol.spec.Variable("r"), new com.legend.protocol.spec.Variable("c")));
        var resolved = (com.legend.protocol.spec.AppliedFunction)
                com.legend.compiler.NameResolver.resolveQuery(call);
        assertEquals("meta::pure::functions::relation::extend", resolved.function());
        assertEquals(java.util.Optional.of(com.legend.platform.CoreFn.EXTEND),
                com.legend.platform.CoreFn.of(resolved.function()));
    }

    /** The prelude tier still serves names the user made no claim on. */
    @Test
    @DisplayName("prelude resolves when no user candidate exists")
    void preludeStillResolves() {
        // Any model compiles against prelude types (e.g. a class using a
        // platform enum/class type in a property) — pin the simplest
        // observable: compile succeeds and the class is present.
        String src = "Class app::Holder { v: Variant[0..1]; }";
        var ctx = Compiler.compileModel(src);
        assertTrue(ctx.findClass("app::Holder").isPresent());
    }
}
