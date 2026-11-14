// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The runtime a query runs on, one rule for every door ({@link TypedQuery#runtime}; the checkpoint tenets audit's 1,
 * 2026-11-10): the caller's, else the one its {@code ->from}s name -- a from() inside a function it calls included,
 * read after G½ -- and a query naming several, given none, refused by name. Until then the server's door took the
 * first from()'s runtime (its own walk of the typed tree, {@code Compiler.Target}) and ran every statement on it, where
 * every other door refused.
 */
class QueryRuntimeTest {

    private static final String MODEL = """
            ###Relational
            Database s::DB ( Table T ( ID INTEGER PRIMARY KEY ) )
            ###Connection
            RelationalDatabaseConnection s::C1 { store: s::DB; type: H2; specification: LocalH2 { }; auth: DefaultH2; }
            RelationalDatabaseConnection s::C2 { store: s::DB; type: H2; specification: LocalH2 { }; auth: DefaultH2; }
            ###Runtime
            Runtime s::RT1 { mappings: [ ]; connections: [ s::DB: [ c: s::C1 ] ]; }
            Runtime s::RT2 { mappings: [ ]; connections: [ s::DB: [ c: s::C2 ] ]; }
            ###Pure
            function s::onTwo(): meta::pure::metamodel::relation::Relation<(ID:Integer)>[1]
            {
                #>{s::DB.T}#->select(~[ID])->from(s::RT2)
            }
            """;

    private static TypedQuery query(String text) {
        return Compiler.query(Compiler.compileModel(MODEL), text);
    }

    @Test
    void aQueryNamingSeveralRuntimesAndGivenNoneIsRefusedByName_givenOneItRunsThere() {
        TypedQuery two = query("|#>{s::DB.T}#->select(~[ID])->from(s::RT1)->concatenate("
                + "#>{s::DB.T}#->select(~[ID])->from(s::RT2))");
        var refused = assertThrows(com.legend.error.MappingResolutionException.class, () -> two.runtime(null));
        assertTrue(refused.getMessage().contains("[s::RT1, s::RT2]"), refused.getMessage());
        assertEquals("s::RT2", two.runtime("s::RT2"));
    }

    @Test
    void aFromInsideACalledFunctionNamesItsRuntime_readAfterInlining() {
        assertEquals("s::RT2", query("|s::onTwo()").runtime(null));
        var refused = assertThrows(com.legend.error.MappingResolutionException.class,
                () -> query("|#>{s::DB.T}#->select(~[ID])->from(s::RT1)->concatenate(s::onTwo())").runtime(null));
        assertTrue(refused.getMessage().contains("[s::RT1, s::RT2]"), refused.getMessage());
        assertNull(query("|#>{s::DB.T}#->select(~[ID])").runtime(null));
    }
}
