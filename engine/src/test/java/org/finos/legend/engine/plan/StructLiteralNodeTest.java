package org.finos.legend.engine.plan;

import org.finos.legend.engine.transpiler.SQLDialect;
import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLGenerator;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for StructLiteralNode SQL generation.
 */
public class StructLiteralNodeTest {

    private final SQLDialect dialect = DuckDBDialect.INSTANCE;

    @Test
    void testFlatStructLiteral() {
        // ^Person(firstName='John', age=30)
        StructInstance instance = new StructInstance(orderedMap(
                "firstName", "John",
                "age", 30));

        StructLiteralNode node = new StructLiteralNode("Person", List.of(instance));

        SQLGenerator generator = new SQLGenerator(dialect);
        String sql = node.accept(generator);

        assertEquals("SELECT * FROM (VALUES ({'firstName': 'John', 'age': 30})) AS t(person)", sql);
    }

    @Test
    void testMultipleInstances() {
        // [^Person(firstName='John'), ^Person(firstName='Jane')]
        StructInstance john = new StructInstance(orderedMap("firstName", "John"));
        StructInstance jane = new StructInstance(orderedMap("firstName", "Jane"));

        StructLiteralNode node = new StructLiteralNode("Person", List.of(john, jane));

        SQLGenerator generator = new SQLGenerator(dialect);
        String sql = node.accept(generator);

        assertEquals("SELECT * FROM (VALUES ({'firstName': 'John'}), ({'firstName': 'Jane'})) AS t(person)", sql);
    }

    @Test
    void testNestedStruct() {
        // ^Person(address=^Address(city='NYC'))
        StructInstance address = new StructInstance(orderedMap("city", "NYC"));
        StructInstance person = new StructInstance(orderedMap(
                "firstName", "John",
                "address", address));

        StructLiteralNode node = new StructLiteralNode("Person", List.of(person));

        SQLGenerator generator = new SQLGenerator(dialect);
        String sql = node.accept(generator);

        assertEquals("SELECT * FROM (VALUES ({'firstName': 'John', 'address': {'city': 'NYC'}})) AS t(person)", sql);
    }

    @Test
    void testNestedArrayOfStructs() {
        // ^Firm(employees=[^Person(firstName='John'), ^Person(firstName='Jane')])
        StructInstance john = new StructInstance(orderedMap("firstName", "John"));
        StructInstance jane = new StructInstance(orderedMap("firstName", "Jane"));

        StructInstance firm = new StructInstance(orderedMap(
                "legalName", "Goldman",
                "employees", List.of(john, jane)));

        StructLiteralNode node = new StructLiteralNode("Firm", List.of(firm));

        SQLGenerator generator = new SQLGenerator(dialect);
        String sql = node.accept(generator);

        assertEquals(
                "SELECT * FROM (VALUES ({'legalName': 'Goldman', 'employees': [{'firstName': 'John'}, {'firstName': 'Jane'}]})) AS t(firm)",
                sql);
    }

    @Test
    void testAliasDerivedFromClassName() {
        StructLiteralNode node = new StructLiteralNode("Person", List.of(
                new StructInstance(Map.of("a", 1))));
        assertEquals("person", node.alias());

        // Qualified name
        StructLiteralNode qualified = new StructLiteralNode("test::Person", List.of(
                new StructInstance(Map.of("a", 1))));
        assertEquals("person", qualified.alias());
    }

    @Test
    void testPrimitiveList() {
        // ^Product(tags=['a', 'b', 'c'])
        StructInstance product = new StructInstance(orderedMap(
                "name", "Widget",
                "tags", List.of("electronics", "gadget")));

        StructLiteralNode node = new StructLiteralNode("Product", List.of(product));

        SQLGenerator generator = new SQLGenerator(dialect);
        String sql = node.accept(generator);

        assertEquals(
                "SELECT * FROM (VALUES ({'name': 'Widget', 'tags': ['electronics', 'gadget']})) AS t(product)",
                sql);
    }

    /**
     * Helper to create ordered maps for predictable key ordering in tests.
     */
    private static Map<String, Object> orderedMap(Object... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException("Pairs must be even");
        }
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put((String) pairs[i], pairs[i + 1]);
        }
        return map;
    }
}
