package org.finos.legend.pure.dsl;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for graphFetch/serialize parsing.
 * 
 * All parsing now uses ANTLR via PureParser. The standalone GraphFetchParser
 * was
 * removed as redundant - island mode (#{ }#) is handled directly by the ANTLR
 * grammar.
 */
class GraphFetchParserTest {

    @Test
    void testParseSimpleGraphFetchTree() {
        // Parse a simple graphFetch expression - island mode is parsed via ANTLR
        String query = "Person.all()->graphFetch(#{ Person { firstName, lastName } }#)";
        PureExpression expr = PureParser.parse(query);

        assertTrue(expr instanceof GraphFetchExpression,
                "Expected GraphFetchExpression, got: " + expr.getClass().getSimpleName());

        GraphFetchExpression graphFetch = (GraphFetchExpression) expr;
        GraphFetchTree tree = graphFetch.fetchTree();

        assertEquals("Person", tree.rootClass());
        assertEquals(2, tree.properties().size());
        assertEquals("firstName", tree.properties().get(0).name());
        assertEquals("lastName", tree.properties().get(1).name());
        assertFalse(tree.properties().get(0).isNested());
        assertFalse(tree.properties().get(1).isNested());
    }

    @Test
    void testParseGraphFetchExpression() {
        String query = "Person.all()->graphFetch(#{ Person { fullName, upperLastName } }#)->serialize(#{ Person { fullName, upperLastName } }#)";
        PureExpression expr = PureParser.parse(query);

        assertTrue(expr instanceof SerializeExpression,
                "Expected SerializeExpression, got: " + expr.getClass().getSimpleName());

        SerializeExpression serialize = (SerializeExpression) expr;
        assertEquals("Person", serialize.getRootClassName());
        assertEquals(2, serialize.serializeTree().properties().size());

        GraphFetchExpression graphFetch = serialize.source();
        assertEquals("Person", graphFetch.fetchTree().rootClass());
    }

    @Test
    void testParseWithFilter() {
        String query = "Person.all()->filter({p | $p.age > 18})->graphFetch(#{ Person { name } }#)->serialize(#{ Person { name } }#)";
        PureExpression expr = PureParser.parse(query);

        assertTrue(expr instanceof SerializeExpression);
        SerializeExpression serialize = (SerializeExpression) expr;
        assertEquals("Person", serialize.getRootClassName());
    }

    @Test
    void testGraphFetchRequiresClassExpression() {
        // graphFetch on a non-class expression should fail
        String invalidQuery = "#>{store::DB.TABLE}->graphFetch(#{ Person { name } }#)";

        assertThrows(PureParseException.class, () -> {
            PureParser.parse(invalidQuery);
        });
    }

    @Test
    void testSerializeRequiresGraphFetch() {
        // serialize without graphFetch should fail
        String invalidQuery = "Person.all()->serialize(#{ Person { name } }#)";

        assertThrows(PureParseException.class, () -> {
            PureParser.parse(invalidQuery);
        });
    }
}
