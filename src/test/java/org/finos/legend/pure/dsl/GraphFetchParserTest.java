package org.finos.legend.pure.dsl;

import org.finos.legend.pure.dsl.graphfetch.GraphFetchTree;
import org.finos.legend.pure.dsl.graphfetch.GraphFetchParser;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for graphFetch/serialize parsing.
 */
class GraphFetchParserTest {

    @Test
    void testParseSimpleGraphFetchTree() {
        String tree = "#{ Person { firstName, lastName } }#";
        GraphFetchTree result = GraphFetchParser.parse(tree);

        assertEquals("Person", result.rootClass());
        assertEquals(2, result.properties().size());
        assertEquals("firstName", result.properties().get(0).name());
        assertEquals("lastName", result.properties().get(1).name());
        assertFalse(result.properties().get(0).isNested());
        assertFalse(result.properties().get(1).isNested());
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
