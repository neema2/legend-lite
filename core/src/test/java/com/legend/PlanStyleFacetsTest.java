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
            com.legend.edge.PureText.PURE_V1);

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

    /** errorWords (2026-11-16, the PCT window-frame fix, the orchestrator's conditions): the product's style answers
     *  an error's OWN message, never the edge's words; the edge's answers upstream's sentence of a compile kind. */
    @Test
    void theProductsErrorWordsAreItsOwnTheEdgesUpstreams() {
        RuntimeException frame = new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.TYPE,
                com.legend.error.CompileKind.WINDOW_FRAME_INVERTED, List.of());
        String product = PlanStyle.PRODUCT.errorWords(frame);
        assertTrue(product.equals(frame.getMessage()) && product.contains("window frame starts after it ends")
                && !product.contains("Invalid window frame boundary"), product);
        String edge = com.legend.edge.PureText.UPSTREAM.errorWords(frame);
        assertTrue(edge.contains("Invalid window frame boundary - lower bound of window frame cannot be greater"), edge);
    }

    /** A load's file larger than the load admits (J25 of the edge audit, 2026-11-17): the product's words are its kind and
     *  where; legend-pure's sentence, the text's megabytes against the limit, is the edge's. */
    @Test
    void aFileTooLargeIsTheProductsKindAndTheEdgesSentence() {
        byte[] big = new byte[1024 * 1024 + 1];
        java.util.Arrays.fill(big, (byte) 'a');
        com.legend.base.DataSize.TooLarge tooLarge = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.base.DataSize.TooLarge.class, () -> com.legend.base.DataSize.requireSize("/big.csv", big, 1));
        RuntimeException refused = new com.legend.error.DataError(com.legend.error.ErrorKind.FILE_TOO_LARGE,
                tooLarge.path(), tooLarge);
        org.junit.jupiter.api.Assertions.assertEquals("the file is larger than the load admits: /big.csv",
                PlanStyle.PRODUCT.errorWords(refused));
        org.junit.jupiter.api.Assertions.assertEquals("Unable to read the CSV file '/big.csv' Execution error at ??,"
                + " \"File is too large, file was 1.00 Mb, limit is 1 Mb\"",
                com.legend.edge.PureText.UPSTREAM.errorWords(refused));
    }

    /** A parameter value its parameter does not take (J8 of the edge audit, 2026-11-17): the product's words are lite's
     *  sentence; legend-engine's "Invalid provided parameter(s): [...]" is the edge's. */
    @Test
    void anInvalidParameterIsTheProductsKindAndTheEdgesSentence() {
        RuntimeException invalid = new com.legend.error.InvalidParameters(java.util.List.of(
                "parameter 'p' (Integer[1]): not an Integer", "parameter 'q' (Date[1]): not a date"));
        org.junit.jupiter.api.Assertions.assertEquals("invalid value(s) for the parameter(s): parameter 'p' (Integer[1]):"
                + " not an Integer; parameter 'q' (Date[1]): not a date", PlanStyle.PRODUCT.errorWords(invalid));
        org.junit.jupiter.api.Assertions.assertEquals("Invalid provided parameter(s): [parameter 'p' (Integer[1]): not an"
                + " Integer; parameter 'q' (Date[1]): not a date]", com.legend.edge.PureText.UPSTREAM.errorWords(invalid));
    }

    /** The model errors J10 of the edge audit (2026-11-17) made kinds: each the product's kind in lite's sentence, and
     *  legend-engine's (legend-pure's) sentence, as the product wrote it until then, the edge's. */
    @Test
    void aModelsDuplicatesAndMissingStoresAreTheProductsKindsAndTheEdgesSentences() {
        String db = "###Relational\nDatabase s::DB ( Table T (ID INTEGER) Table U (ID INTEGER) )\n";
        String person = "Class m::P { id: Integer[1]; }\n";
        Object[][] cases = {
            {"Class m::A { x: String[1]; }\nClass m::A { y: Integer[1]; }",
                com.legend.error.CompileKind.DUPLICATE_ELEMENT, "'m::A' is defined more than once",
                "Duplicated element 'm::A'"},
            {"Class m::A { x: String[1]; x: Integer[1]; }", com.legend.error.CompileKind.DUPLICATE_PROPERTY,
                "class 'm::A' declares property 'x' more than once", "Found duplicated property 'x' in class 'm::A'"},
            {"Class m::A { x: Integer[0..1] = 5; }", com.legend.error.CompileKind.DEFAULT_ON_OPTIONAL,
                "property 'x' is optional and cannot have a default value",
                "Default values are supported only for mandatory fields, and property 'x' is optional."},
            {"Enum m::E { A, A }", com.legend.error.CompileKind.DUPLICATE_ENUM_VALUE,
                "enumeration 'm::E' declares value 'A' more than once",
                "Found duplicated value 'A' in enumeration 'm::E'"},
            {"###Relational\nDatabase s::DB ( Table T (ID INTEGER) Join bad(T.ID = T.GHOST) )\n",
                com.legend.error.CompileKind.MISSING_STORE_COLUMN,
                "table 'T' of database 's::DB' has no column 'GHOST' (join 'bad')",
                "The column 'GHOST' can't be found in the table 'T' (join 'bad' of database 's::DB')"},
            {"###Relational\nDatabase s::DB ( Table T (ID INTEGER) Join bad(T.ID = NOPE.ID) )\n",
                com.legend.error.CompileKind.MISSING_STORE_TABLE, "database 's::DB' has no table 'NOPE' (join 'bad')",
                "The table 'NOPE' can't be found in the database 's::DB' (join 'bad')"},
            {person + db + "###Mapping\nMapping m::Base ( )\nMapping m::M ( include m::Base[s::NoDb -> s::DB] )\n",
                com.legend.error.CompileKind.MISSING_STORE, "store 's::NoDb' is not defined",
                "The store 's::NoDb' can't be found."},
            {person + db + "###Mapping\nMapping m::A ( *m::P[p] : Relational { ~mainTable [s::DB] T id: T.ID } )\n"
                    + "Mapping m::B ( *m::P[p] : Relational { ~mainTable [s::DB] U id: U.ID } )\n"
                    + "Mapping m::AB ( include m::A include m::B )\n",
                com.legend.error.CompileKind.DUPLICATE_CLASS_MAPPING_IDS,
                "mapping 'm::AB' has more than one class mapping with the ids [p (m::A, m::B)]",
                "Duplicated class mappings found with ID [p (m::A, m::B)] in mapping 'm::AB'"},
            {person + db + "###Mapping\nMapping m::A ( *m::P[p] : Relational { ~mainTable [s::DB] T id: T.ID } )\n"
                    + "Mapping m::AA ( include m::A include m::A )\n",
                com.legend.error.CompileKind.DUPLICATE_MAPPING_INCLUDE, "mapping 'm::AA' includes 'm::A' more than once",
                "Duplicated mapping include 'm::A' in mapping 'm::AA'"},
        };
        for (Object[] c : cases) {
            com.legend.error.ModelException e = assertThrows(com.legend.error.ModelException.class,
                    () -> Compiler.compileModel((String) c[0]));
            org.junit.jupiter.api.Assertions.assertEquals(c[1], e.kind(), e.getMessage());
            String product = PlanStyle.PRODUCT.errorWords(e);
            assertTrue(product.contains((String) c[2]) && !product.contains((String) c[3]), product);
            String upstream = com.legend.edge.PureText.UPSTREAM.errorWords(e);
            assertTrue(upstream.contains((String) c[3]) && !upstream.contains((String) c[2]), upstream);
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
