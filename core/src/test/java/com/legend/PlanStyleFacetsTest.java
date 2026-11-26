// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.database.PlanStyle;
import com.legend.error.NotImplementedException;
import com.legend.testplan.PlanStyleFacets;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE FACET KINDS ARE CLOSED (the orchestrator's condition of 2026-11-16 on {@code PlanStyle.facet}): every style
 * answers exactly {@link PlanStyleFacets#KINDS} -- the edge each kind, the product none (each refused by name) -- and a
 * kind outside the list fails by name at the call, never null and never a default.
 */
class PlanStyleFacetsTest {

    /** Every style ({@code ArchitectureTest.theFacetTestsHoldEveryStyle} holds it to the imported implementers). */
    static final List<PlanStyle> STYLES = List.of(PlanStyle.PRODUCT, com.legend.edge.PureText.UPSTREAM,
            com.legend.edge.PureText.PURE_V1,
            // legend-engine's own tests' style, the relational corpus's door (the edition program's step 3, 2026-11-26)
            com.legend.edge.PureText.ENGINE_TESTS);
            // (the edge's recorder of one planning call's raise sites' words, K10b of 2026-11-17: deleted 2026-11-20
            // with stage B's B1)

    @Test
    void theEdgeAnswersEveryDeclaredKind() {
        for (Class<?> kind : PlanStyleFacets.KINDS) {
            Object f = com.legend.edge.PureText.UPSTREAM.facet(kind);
            assertTrue(kind.isInstance(f), kind.getName());
        }
    }

    @Test
    void theProductRefusesEveryDeclaredKindByName() {
        for (Class<?> kind : PlanStyleFacets.KINDS) {
            NotImplementedException e = assertThrows(NotImplementedException.class, () -> PlanStyle.PRODUCT.facet(kind));
            assertTrue(e.getMessage().contains(kind.getName()) && e.getMessage().contains("compatibility edge"),
                    e.getMessage());
        }
    }

    /** One catalog (stage B's BE, the user's ruling of 2026-11-21): a compile kind's sentence is upstream's wherever
     *  upstream has one, so the product and the edge say the same words (until then the product said lite's own,
     *  "window frame starts after it ends", and the edge upstream's; errorWords, 2026-11-16). */
    @Test
    void aCompileKindSaysUpstreamsSentenceOnTheProductAndAtTheEdge() {
        RuntimeException frame = new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.TYPE,
                com.legend.error.CompileKind.WINDOW_FRAME_INVERTED, List.of());
        String upstream = "Invalid window frame boundary - lower bound of window frame cannot be greater than the upper"
                + " bound!";
        assertTrue(frame.getMessage().contains(upstream), frame.getMessage());
        String edge = com.legend.edge.PureMessages.compile(frame);
        assertTrue(edge.contains(upstream), edge);
    }

    /** A load's file larger than the load admits (J25 of the edge audit, 2026-11-17): the product's kind and where, and
     *  legend-pure's sentence, the text's megabytes against the limit, on the product and at the edge alike (the one
     *  catalog, stage B's BE, 2026-11-21: until then lite's own on the product). */
    @Test
    void aFileTooLargeIsTheProductsKindInLegendPuresSentence() {
        byte[] big = new byte[1024 * 1024 + 1];
        java.util.Arrays.fill(big, (byte) 'a');
        com.legend.base.DataSize.TooLarge tooLarge = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.base.DataSize.TooLarge.class, () -> com.legend.base.DataSize.requireSize("/big.csv", big, 1));
        RuntimeException refused = com.legend.error.DataError.fileTooLarge(tooLarge.path(), tooLarge.bytes(),
                tooLarge.characters(), tooLarge.limitMegabytes(), tooLarge);
        String upstream = "Unable to read the CSV file '/big.csv' Execution error at ??, \"File is too large, file was"
                + " 1.00 Mb, limit is 1 Mb\"";
        org.junit.jupiter.api.Assertions.assertEquals(upstream, refused.getMessage());
        org.junit.jupiter.api.Assertions.assertEquals(upstream, com.legend.edge.PureMessages.compile(refused));
    }

    /** A parameter value its parameter does not take (J8 of the edge audit, 2026-11-17): the product's words are lite's
     *  sentence; legend-engine's "Invalid provided parameter(s): [...]" is the edge's. */
    @Test
    void anInvalidParameterIsTheProductsKindAndTheEdgesSentence() {
        RuntimeException invalid = new com.legend.error.InvalidParameters(java.util.List.of(
                "parameter 'p' (Integer[1]): not an Integer", "parameter 'q' (Date[1]): not a date"));
        org.junit.jupiter.api.Assertions.assertEquals("invalid value(s) for the parameter(s): parameter 'p' (Integer[1]):"
                + " not an Integer; parameter 'q' (Date[1]): not a date", invalid.getMessage());
        org.junit.jupiter.api.Assertions.assertEquals("Invalid provided parameter(s): [parameter 'p' (Integer[1]): not an"
                + " Integer; parameter 'q' (Date[1]): not a date]", com.legend.edge.PureMessages.compile(invalid));
    }

    /** The model errors J10 of the edge audit (2026-11-17) made kinds: each the product's kind, and, since the one
     *  catalog (stage B's BE, 2026-11-21), legend-engine's (legend-pure's) sentence on the product and at the edge alike
     *  (lite's own sentences, the product's from J10 until then, are gone). */
    @Test
    void aModelsDuplicatesAndMissingStoresAreTheProductsKindsInUpstreamsSentences() {
        String db = "###Relational\nDatabase s::DB ( Table T (ID INTEGER) Table U (ID INTEGER) )\n";
        String person = "Class m::P { id: Integer[1]; }\n";
        Object[][] cases = {
            {"Class m::A { x: String[1]; }\nClass m::A { y: Integer[1]; }",
                com.legend.error.CompileKind.DUPLICATE_ELEMENT, "Duplicated element 'm::A'"},
            {"Class m::A { x: String[1]; x: Integer[1]; }", com.legend.error.CompileKind.DUPLICATE_PROPERTY,
                "Property conflict on class A: property 'x' defined more than once"},
            {"Class m::A { x: Integer[0..1] = 5; }", com.legend.error.CompileKind.DEFAULT_ON_OPTIONAL,
                "Default values are supported only for mandatory fields, and property 'x' is optional."},
            {"Enum m::E { A, A }", com.legend.error.CompileKind.DUPLICATE_ENUM_VALUE,
                "Enumeration m::E has duplicate values: A"},
            {"###Relational\nDatabase s::DB ( Table T (ID INTEGER) Join bad(T.ID = T.GHOST) )\n",
                com.legend.error.CompileKind.MISSING_STORE_COLUMN,
                "The column 'GHOST' can't be found in the table 'T'"},
            {"###Relational\nDatabase s::DB ( Table T (ID INTEGER) Join bad(T.ID = NOPE.ID) )\n",
                com.legend.error.CompileKind.MISSING_STORE_TABLE,
                "The table 'NOPE' can't be found in the schema 'default' in the database 'DB'"},
            {person + db + "###Mapping\nMapping m::Base ( )\nMapping m::M ( include m::Base[s::NoDb -> s::DB] )\n",
                com.legend.error.CompileKind.MISSING_STORE,
                "The store 's::NoDb' can't be found."},
            {person + db + "###Mapping\nMapping m::A ( *m::P[p] : Relational { ~mainTable [s::DB] T id: T.ID } )\n"
                    + "Mapping m::B ( *m::P[p] : Relational { ~mainTable [s::DB] U id: U.ID } )\n"
                    + "Mapping m::AB ( include m::A include m::B )\n",
                com.legend.error.CompileKind.DUPLICATE_CLASS_MAPPING_IDS,
                "Duplicated class mappings found with ID 'p' in mapping 'm::AB'"},
            {person + db + "###Mapping\nMapping m::A ( *m::P[p] : Relational { ~mainTable [s::DB] T id: T.ID } )\n"
                    + "Mapping m::AA ( include m::A include m::A )\n",
                com.legend.error.CompileKind.DUPLICATE_MAPPING_INCLUDE,
                "Duplicated mapping include 'm::A' in mapping 'm::AA'"},
        };
        for (Object[] c : cases) {
            com.legend.error.ModelException e = assertThrows(com.legend.error.ModelException.class,
                    () -> Compiler.compileModel((String) c[0]));
            org.junit.jupiter.api.Assertions.assertEquals(c[1], e.kind(), e.getMessage());
            assertTrue(e.getMessage().contains((String) c[2]), e.getMessage());
            String edge = com.legend.edge.PureMessages.compile(e);
            assertTrue(edge.contains((String) c[2]), edge);
        }
    }

    @Test
    void aKindOutsideTheListFailsByNameOnEveryStyle() {
        for (PlanStyle style : STYLES) {
            NotImplementedException e = assertThrows(NotImplementedException.class, () -> style.facet(Runnable.class));
            assertTrue(e.getMessage().contains(Runnable.class.getName()), e.getMessage());
        }
    }
}
