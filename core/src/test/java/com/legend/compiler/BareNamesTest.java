// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler;

import com.legend.builtin.Pure;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The one bare-name rule (untangle 4b.2): three tiers, each a declaration. */
class BareNamesTest {

    /** Tier 1: an engine handler name resolves to the FQN the platform declares
     *  for it, with no import in sight. */
    @Test
    void anEngineHandlerNameIsServedByTheEngineSurface() {
        List<String> fqns = BareNames.fqns("from");
        assertEquals("meta::pure::mapping::from", fqns.get(0));
        assertTrue(BareNames.catalog("from").stream()
                .anyMatch(n -> n.qualifiedName().equals("meta::pure::mapping::from")));
    }

    /** Tier 2: a name in several core-group packages carries every one; the
     *  catalog's overloads at each join the candidate set. */
    @Test
    void aCoreGroupNameCarriesEveryPackage() {
        List<String> fqns = BareNames.fqns("map");
        assertTrue(fqns.contains("meta::pure::functions::collection::map"), String.valueOf(fqns));
        assertTrue(fqns.contains("meta::pure::functions::relation::map"), String.valueOf(fqns));
        assertTrue(BareNames.catalog("map").size() >= 2);
    }

    /** The lite partition: a form's lite-internal declaration is never reachable
     *  from a bare spelling, while the product surface is. */
    @Test
    void theLitePartitionHolds() {
        assertTrue(BareNames.fqns("joinSlot").stream().noneMatch(f -> f.startsWith(Pure.Lite.PKG)),
                String.valueOf(BareNames.fqns("joinSlot")));
        assertTrue(!BareNames.catalog("joinWithPrefix").isEmpty());
    }

    /** A name the platform declares nowhere is served by no tier: the typer
     *  says "unknown function", never a near miss. */
    @Test
    void anUndeclaredNameHasNoCatalogCandidates() {
        assertEquals(List.of(), BareNames.catalog("noSuchFunctionAnywhere"));
    }

    /** The catalog refuses a bare name as a declaration lookup. */
    @Test
    void theCatalogIsFqnKeyed() {
        assertThrows(IllegalArgumentException.class, () -> Pure.nativeFunctionsAt("map"));
    }

    /** Tier 3 names only the form's declarations spelled like the call: the
     *  select form owns newTDSRelationAccessor too, and a call spelled select
     *  must not tie with it. */
    @Test
    void aFormsOwnedDeclarationsJoinOnlyUnderTheirOwnName() {
        List<String> fqns = BareNames.fqns("select");
        assertTrue(fqns.contains("meta::pure::functions::relation::select"), String.valueOf(fqns));
        assertTrue(fqns.stream().noneMatch(f -> f.endsWith("newTDSRelationAccessor")), String.valueOf(fqns));
    }
}
