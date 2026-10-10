// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler;

import com.legend.Compiler;
import com.legend.compiler.element.PureModelContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * F-L1 (projects/FINDINGS.md, 2026-10-05; the build rebuild's Phase 3b, item 1a): a view declared inside a
 * {@code Schema} was lifted twice — the model builder indexed it from the flat {@code views()} mirror and again
 * from its schema, and the name resolver rebuilt the two lists independently, so a view it rewrote was two
 * objects. Each view is now indexed once and resolved once.
 */
class SchemaViewLiftTest {

    private static PureModelContext build(String model) {
        return (PureModelContext) Compiler.buildModel(com.legend.testing.Own.model(model));
    }

    @Test
    @DisplayName("the recorded minimal model: a view inside a schema is lifted exactly once")
    void aSchemaViewIsLiftedOnce() {
        PureModelContext ctx = build("""
                ###Pure
                Class m::X { k: String[1]; }
                ###Relational
                Database r::Store
                (
                  Schema s
                  (
                    Table L (K VARCHAR(10) PRIMARY KEY, V INTEGER)
                    View V ( K: s.L.K PRIMARY KEY, N: s.L.V )
                  )
                )
                """);
        assertEquals(1, ctx.findFunction("r::Store$view$s.V").size());
    }

    @Test
    @DisplayName("two schemas with a same-named view: two lifted functions, one each")
    void twoSchemasOneViewNameEach() {
        PureModelContext ctx = build("""
                ###Pure
                Class m::X { k: String[1]; }
                ###Relational
                Database r::Store
                (
                  Schema a ( Table L (K VARCHAR(10) PRIMARY KEY, V INTEGER) View V ( K: a.L.K PRIMARY KEY, N: a.L.V ) )
                  Schema b ( Table L (K VARCHAR(10) PRIMARY KEY, V INTEGER) View V ( K: b.L.K PRIMARY KEY, N: b.L.V ) )
                )
                """);
        assertEquals(1, ctx.findFunction("r::Store$view$a.V").size());
        assertEquals(1, ctx.findFunction("r::Store$view$b.V").size());
    }

    @Test
    @DisplayName("a schema view the resolver rewrites (its column reaches a [db]-qualified join) stays one object")
    void aRewrittenSchemaViewIsOneObject() {
        PureModelContext ctx = build("""
                ###Pure
                Class m::X { k: String[1]; }
                ###Relational
                Database r::Store
                (
                  Schema s
                  (
                    Table L (K VARCHAR(10) PRIMARY KEY, V INTEGER)
                    Table R (K VARCHAR(10) PRIMARY KEY, W INTEGER)
                    View V ( K: s.L.K PRIMARY KEY, W: [r::Store] @LR | s.R.W )
                  )
                  Join LR (s.L.K = s.R.K)
                )
                """);
        assertEquals(1, ctx.findFunction("r::Store$view$s.V").size());
        var db = ctx.findDatabase("r::Store").orElseThrow();
        var inSchema = db.schemas().get(0).views().get(0);
        var inFlat = db.views().stream().filter(v -> v.name().equals("V")).findFirst().orElseThrow();
        assertSame(inSchema, inFlat, "the flat mirror and the schema hold the same resolved view");
        assertEquals(0, db.defaultSchemaViews().size(), "no default-schema view: the schema owns it");
    }
}
