// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedTds;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCollection;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Bucket 8 (homework §4s): a rendered golden is brought to typed rows by
 * the render function's own grammar — the compiler layer's law. */
class VerdictQueriesRenderedTest {

    private static Type.RelationType schema() {
        return new Type.RelationType(List.of(
                new Type.Column("hireType", Type.Primitive.STRING, Multiplicity.Bounded.ONE),
                new Type.Column("wtd", Type.Primitive.FLOAT, Multiplicity.Bounded.ZERO_ONE)));
    }

    @Test
    void csvJoinedGoldenBecomesTypedCells() {
        var p = VerdictQueries.parseRendered("hireType,wtd,Campus,1.08,Lateral,1.62,",
                new VerdictQueries.RenderGrammar.Csv(","), schema(), null, GoldenNullWord.Product.NONE);
        assertNull(p.reason());
        var t = (TypedTds) java.util.Objects.requireNonNull(p.literal());
        assertEquals(List.of(List.of("Campus", "1.08"), List.of("Lateral", "1.62")), t.rows());
        assertTrue(Type.isRelation(t.info().type()));
    }

    @Test
    void csvTextWithQuotedCellAndNullCell() {
        var p = VerdictQueries.parseRendered("hireType,wtd\n\"Smith, John\",\nCampus,2.0\n",
                new VerdictQueries.RenderGrammar.Csv("\n"), schema(), null, GoldenNullWord.Product.NONE);
        assertNull(p.reason());
        var t = (TypedTds) java.util.Objects.requireNonNull(p.literal());
        assertEquals(List.of(List.of("Smith, John", ""), List.of("Campus", "2.0")), t.rows());
    }

    @Test
    void headerMismatchIsStatic() {
        var p = VerdictQueries.parseRendered("type,wtd\nCampus,1.0\n",
                new VerdictQueries.RenderGrammar.Csv("\n"), schema(), null, GoldenNullWord.Product.NONE);
        assertTrue(p.headerMismatch());
    }

    @Test
    void tdsFrameAndRowsForm() {
        var p = VerdictQueries.parseRendered("#TDS\n   hireType,wtd\n   Campus,1.08\n   Lateral,null\n#",
                new VerdictQueries.RenderGrammar.Tds(), schema(), null, GoldenNullWord.Product.NONE);
        assertNull(p.reason());
        assertEquals(2, ((TypedTds) java.util.Objects.requireNonNull(p.literal())).rows().size());
        var r = VerdictQueries.parseRendered("1,100.0, 2,200.0",
                new VerdictQueries.RenderGrammar.Rows(", ", ","),
                new Type.RelationType(List.of(
                        new Type.Column("orderId", Type.Primitive.INTEGER, Multiplicity.Bounded.ONE),
                        new Type.Column("pnl", Type.Primitive.FLOAT, Multiplicity.Bounded.ONE))), null, GoldenNullWord.Product.NONE);
        assertNull(r.reason());
        var rc = (TypedTds) java.util.Objects.requireNonNull(r.literal());
        assertEquals(List.of(List.of("1", "100.0"), List.of("2", "200.0")), rc.rows());
        var flat = VerdictQueries.parseRendered("Anthony,New York,David,New York",
                new VerdictQueries.RenderGrammar.Rows(",", ","),
                new Type.RelationType(List.of(
                        new Type.Column("name", Type.Primitive.STRING, Multiplicity.Bounded.ONE),
                        new Type.Column("address", Type.Primitive.STRING, Multiplicity.Bounded.ONE))), null, GoldenNullWord.Product.NONE);
        assertEquals(2, ((TypedTds) java.util.Objects.requireNonNull(flat.literal())).rows().size());
    }

    @Test
    void widthAndKindMismatchesDecline() {
        assertNotNull(VerdictQueries.parseRendered("hireType,wtd\nCampus\n",
                new VerdictQueries.RenderGrammar.Csv("\n"), schema(), null, GoldenNullWord.Product.NONE).reason());
        assertNotNull(VerdictQueries.parseRendered("hireType,wtd\nCampus,abc\n",
                new VerdictQueries.RenderGrammar.Csv("\n"), schema(), null, GoldenNullWord.Product.NONE).reason());
    }

    @Test
    void flatJoinElementsByKind() {
        var p = VerdictQueries.parseRendered("Firm A,Firm C,Firm X",
                new VerdictQueries.RenderGrammar.Flat(","), null, Type.Primitive.STRING, GoldenNullWord.Product.NONE);
        assertNull(p.reason());
        assertEquals(3, ((TypedCollection) java.util.Objects.requireNonNull(p.literal())).elements().size());
        assertEquals(0, ((TypedCollection) java.util.Objects.requireNonNull(VerdictQueries.parseRendered("",
                new VerdictQueries.RenderGrammar.Flat(","), null, Type.Primitive.STRING, GoldenNullWord.Product.NONE).literal()))
                .elements().size());
    }
}
