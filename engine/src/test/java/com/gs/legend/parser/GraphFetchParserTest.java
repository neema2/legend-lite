package com.gs.legend.parser;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.ClassInstance;
import com.gs.legend.ast.GraphFetchTree;
import com.gs.legend.ast.ValueSpecification;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for graphFetch/serialize parsing via g4 sub-parse in ValueSpecificationBuilder.
 * 
 * Verifies that #{...}# island content is sub-parsed through
 * PureParser.graphFetchTree() into structured GraphFetchTree records.
 */
class GraphFetchParserTest {

    @Test
    void testParseSimpleGraphFetchTree() {
        String query = "Person.all()->graphFetch(#{ Person { firstName, lastName } }#)";
        ValueSpecification vs = PureParser.parseQuery(query);

        // graphFetch(...) is an AppliedFunction
        assertInstanceOf(AppliedFunction.class, vs);
        AppliedFunction af = (AppliedFunction) vs;
        assertEquals("graphFetch", af.function());

        // Second parameter is ClassInstance wrapping GraphFetchTree
        ClassInstance ci = (ClassInstance) af.parameters().get(1);
        assertEquals("rootGraphFetchTree", ci.type());
        assertInstanceOf(GraphFetchTree.class, ci.value());

        GraphFetchTree tree = (GraphFetchTree) ci.value();
        assertEquals("Person", tree.rootClass());
        assertEquals(2, tree.properties().size());
        assertEquals("firstName", tree.properties().get(0).name());
        assertEquals("lastName", tree.properties().get(1).name());
        assertFalse(tree.properties().get(0).isNested());
        assertFalse(tree.properties().get(1).isNested());
    }

    @Test
    void testParseGraphFetchWithSerialize() {
        String query = "Person.all()->graphFetch(#{ Person { fullName, upperLastName } }#)->serialize(#{ Person { fullName, upperLastName } }#)";
        ValueSpecification vs = PureParser.parseQuery(query);

        // serialize is the outermost function
        assertInstanceOf(AppliedFunction.class, vs);
        AppliedFunction serialize = (AppliedFunction) vs;
        assertEquals("serialize", serialize.function());

        // First param of serialize is graphFetch(...)
        AppliedFunction graphFetch = (AppliedFunction) serialize.parameters().get(0);
        assertEquals("graphFetch", graphFetch.function());

        // serialize tree
        ClassInstance serializeCi = (ClassInstance) serialize.parameters().get(1);
        GraphFetchTree serializeTree = (GraphFetchTree) serializeCi.value();
        assertEquals("Person", serializeTree.rootClass());
        assertEquals(2, serializeTree.properties().size());

        // graphFetch tree
        ClassInstance fetchCi = (ClassInstance) graphFetch.parameters().get(1);
        GraphFetchTree fetchTree = (GraphFetchTree) fetchCi.value();
        assertEquals("Person", fetchTree.rootClass());
    }

    @Test
    void testParseWithFilter() {
        String query = "Person.all()->filter({p | $p.age > 18})->graphFetch(#{ Person { name } }#)->serialize(#{ Person { name } }#)";
        ValueSpecification vs = PureParser.parseQuery(query);

        assertInstanceOf(AppliedFunction.class, vs);
        AppliedFunction serialize = (AppliedFunction) vs;
        assertEquals("serialize", serialize.function());

        // Verify the tree inside serialize
        ClassInstance ci = (ClassInstance) serialize.parameters().get(1);
        GraphFetchTree tree = (GraphFetchTree) ci.value();
        assertEquals("Person", tree.rootClass());
        assertEquals(1, tree.properties().size());
        assertEquals("name", tree.properties().get(0).name());
    }

    @Test
    void testNestedGraphFetchTree() {
        String query = "Person.all()->graphFetch(#{ Person { firstName, addresses { street, city } } }#)";
        ValueSpecification vs = PureParser.parseQuery(query);

        AppliedFunction af = (AppliedFunction) vs;
        ClassInstance ci = (ClassInstance) af.parameters().get(1);
        GraphFetchTree tree = (GraphFetchTree) ci.value();

        assertEquals("Person", tree.rootClass());
        assertEquals(2, tree.properties().size());

        // Simple property
        assertEquals("firstName", tree.properties().get(0).name());
        assertFalse(tree.properties().get(0).isNested());

        // Nested property
        GraphFetchTree.PropertyFetch addressFetch = tree.properties().get(1);
        assertEquals("addresses", addressFetch.name());
        assertTrue(addressFetch.isNested());
        assertEquals(2, addressFetch.subTree().properties().size());
        assertEquals("street", addressFetch.subTree().properties().get(0).name());
        assertEquals("city", addressFetch.subTree().properties().get(1).name());
    }

    @Test
    void testGraphFetchRequiresClassExpression() {
        // graphFetch on a relation expression should parse (validation is compiler's job)
        String invalidQuery = "#>{store::DB.TABLE}#->graphFetch(#{ Person { name } }#)";
        // This should parse without error — the clean pipeline doesn't do semantic validation
        assertDoesNotThrow(() -> PureParser.parseQuery(invalidQuery));
    }
}
