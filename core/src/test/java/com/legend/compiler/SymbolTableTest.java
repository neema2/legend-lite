package com.legend.compiler;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("SymbolTable — interning primitive")
class SymbolTableTest {

    @Test
    void freshTableIsEmpty() {
        SymbolTable t = new SymbolTable();
        assertEquals(0, t.size());
        assertTrue(t.allFqns().isEmpty());
        assertEquals(SymbolTable.UNRESOLVED, t.resolveId("model::Anything"));
    }

    @Test
    void internAllocatesMonotonicIds() {
        SymbolTable t = new SymbolTable();
        assertEquals(0, t.intern("model::A"));
        assertEquals(1, t.intern("model::B"));
        assertEquals(2, t.intern("model::C"));
        assertEquals(3, t.size());
    }

    @Test
    void internIsIdempotent() {
        SymbolTable t = new SymbolTable();
        int a1 = t.intern("model::A");
        int a2 = t.intern("model::A");
        assertEquals(a1, a2, "Re-interning the same FQN must return the same id");
        assertEquals(1, t.size(), "Re-interning must not grow the table");
    }

    @Test
    void resolveIdFindsInternedFqn() {
        SymbolTable t = new SymbolTable();
        int id = t.intern("model::A");
        assertEquals(id, t.resolveId("model::A"));
    }

    @Test
    void resolveIdReturnsSentinelForUnknownFqn() {
        SymbolTable t = new SymbolTable();
        t.intern("model::A");
        assertEquals(SymbolTable.UNRESOLVED, t.resolveId("model::B"));
    }

    @Test
    void resolveIdDoesNotIntern() {
        SymbolTable t = new SymbolTable();
        t.resolveId("model::Untouched");
        assertEquals(0, t.size(), "resolveId must not allocate");
    }

    @Test
    void nameOfRoundTripsToInternedFqn() {
        SymbolTable t = new SymbolTable();
        int id = t.intern("model::Person");
        assertEquals("model::Person", t.nameOf(id));
    }

    @Test
    void nameOfThrowsOnOutOfBoundsId() {
        SymbolTable t = new SymbolTable();
        t.intern("model::A");
        assertThrows(IndexOutOfBoundsException.class, () -> t.nameOf(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> t.nameOf(5));
    }

    @Test
    void distinctFqnsGetDistinctIds() {
        SymbolTable t = new SymbolTable();
        int a = t.intern("model::A");
        int b = t.intern("model::B");
        assertNotEquals(a, b);
    }

    @Test
    void allFqnsReflectsInsertions() {
        SymbolTable t = new SymbolTable();
        t.intern("model::A");
        t.intern("model::B");
        assertEquals(2, t.allFqns().size());
        assertTrue(t.allFqns().contains("model::A"));
        assertTrue(t.allFqns().contains("model::B"));
    }

    @Test
    void unresolvedSentinelIsNegative() {
        // Document the invariant that callers can distinguish "found"
        // (id >= 0) from "not found" (id < 0) with a single branch.
        assertTrue(SymbolTable.UNRESOLVED < 0);
    }
}
