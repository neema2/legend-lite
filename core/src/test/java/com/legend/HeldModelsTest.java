// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.compiler.element.ModelContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The models a host holds (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §9): compiled once per text, counted by their
 * holders, planned against under their own lock, refused by name once let go.
 */
@DisplayName("HeldModels -- a model compiled once, planned against many times")
class HeldModelsTest {

    private final HeldModels held = new HeldModels();

    private static final String MODEL = """
            ###Relational
            Database held::DB ( Table T ( a INTEGER, b VARCHAR(10) ) )
            ###Connection
            RelationalDatabaseConnection held::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime held::RT { mappings: []; connections: [ held::DB: [ c1: held::Conn ] ]; }
            """;

    @Test
    @DisplayName("the same text is the same model, compiled once; an edited one is another")
    void theTextIsTheKey() {
        int a = held.hold(MODEL);
        int b = held.hold(MODEL);
        int c = held.hold(MODEL + "\n// edited\n");
        try {
            assertNotEquals(a, b, "each hold its own handle");
            ModelContext first = held.with(a, ctx -> ctx);
            assertSame(first, held.with(b, ctx -> ctx), "the same text: compiled once, the one context");
            assertNotSame(first, held.with(c, ctx -> ctx), "an edited model: another");
            assertEquals(2, held.size());
        } finally {
            held.release(a);
            held.release(b);
            held.release(c);
        }
    }

    @Test
    @DisplayName("a model named by its text: the one held, compiled no more; one not held, compiled for its use")
    void usedByItsText() {
        int h = held.hold(MODEL);
        try {
            ModelContext first = held.with(h, ctx -> ctx);
            assertSame(first, held.withText(MODEL, ctx -> ctx), "held: the one context");
            String other = MODEL + "\n// another\n";
            int before = held.size();
            assertNotSame(first, held.withText(other, ctx -> ctx), "not held: compiled for this use");
            assertEquals(before, held.size(), "and not held after");
        } finally {
            held.release(h);
        }
    }

    @Test
    @DisplayName("held while anyone holds it; gone with its last holder, and then refused by name")
    void heldWhileHeld() {
        int before = held.size();
        int a = held.hold(MODEL);
        int b = held.hold(MODEL);
        held.release(a);
        assertEquals(before + 1, held.size(), "one holder left");
        held.release(a);
        assertEquals(before + 1, held.size(), "a's second release takes nothing from b");
        held.release(b);
        assertEquals(before, held.size(), "gone with the last");
        IllegalArgumentException gone = assertThrows(IllegalArgumentException.class,
                () -> held.with(a, ctx -> ctx));
        assertTrue(gone.getMessage().contains("nothing is held under handle " + a), gone.getMessage());
        held.release(a);
    }

    @Test
    @DisplayName("plans against the model held, as against a model compiled for the plan")
    void plansAsCompiled() {
        int h = held.hold(MODEL);
        try {
            String query = "|#>{held::DB.T}#->filter(x|$x.a > 1)->select(~[b])";
            String planned = held.with(h, ctx -> Compiler.query(ctx, query).plan("held::RT").sql());
            assertEquals(Compiler.query(Compiler.compileModel(MODEL), query).plan("held::RT").sql(), planned);
            for (int i = 2; i < 5; i++) {
                String q = "|#>{held::DB.T}#->filter(x|$x.a > " + i + ")->select(~[b])";
                assertTrue(held.with(h, ctx -> Compiler.query(ctx, q).plan("held::RT").sql()).contains("> " + i));
            }
        } finally {
            held.release(h);
        }
    }

    @Test
    @DisplayName("a model that does not compile is refused, and nothing is held")
    void aRefusedModelIsNotHeld() {
        int before = held.size();
        assertThrows(RuntimeException.class, () -> held.hold("###Pure\nClass bad::C { a: Nope[1]; }"));
        assertEquals(before, held.size());
    }
}
