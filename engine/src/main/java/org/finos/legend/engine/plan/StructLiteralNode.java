package org.finos.legend.engine.plan;

import java.util.List;

/**
 * IR node representing inline structured data (class instances) as a relation
 * source.
 * 
 * Compiles to DuckDB STRUCT literals in a VALUES clause:
 * 
 * <pre>
 * Pure:
 *   [^Person(firstName='John', age=30), ^Person(firstName='Jane', age=25)]
 * 
 * SQL:
 *   SELECT * FROM (VALUES 
 *     ({firstName: 'John', age: 30}),
 *     ({firstName: 'Jane', age: 25})
 *   ) AS t(person)
 * </pre>
 * 
 * Nested objects are represented as nested STRUCTs:
 * 
 * <pre>
 * Pure:
 *   [^Firm(legalName='Goldman', employees=[^Person(firstName='John')])]
 * 
 * SQL:
 *   SELECT * FROM (VALUES 
 *     ({legalName: 'Goldman', employees: [{firstName: 'John'}]})
 *   ) AS t(firm)
 * </pre>
 * 
 * @param className The class name (used for alias, e.g., "Person" -> "person")
 * @param instances The list of structured instances
 */
public record StructLiteralNode(
        String className,
        List<StructInstance> instances) implements RelationNode {

    /**
     * Gets the alias for this struct (lowercase class name).
     */
    public String alias() {
        if (className == null || className.isEmpty()) {
            return "struct";
        }
        // Handle qualified names: "test::Person" -> "person"
        String simpleName = className.contains("::")
                ? className.substring(className.lastIndexOf("::") + 2)
                : className;
        return simpleName.substring(0, 1).toLowerCase() + simpleName.substring(1);
    }

    @Override
    public <T> T accept(RelationNodeVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
