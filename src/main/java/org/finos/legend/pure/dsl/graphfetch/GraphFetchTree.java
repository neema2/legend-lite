package org.finos.legend.pure.dsl.graphfetch;

import java.util.List;
import org.finos.legend.pure.dsl.PureExpression;

/**
 * Represents a graphFetch tree specification.
 * 
 * Syntax: #{ ClassName { property1, property2, nested { subProp } } }#
 * 
 * Example:
 * 
 * <pre>
 * Person.all()
 *     ->graphFetch(#{
 *         Person {
 *             firstName,
 *             lastName,
 *             addresses {
 *                 street,
 *                 city
 *             }
 *         }
 *     }#)
 * </pre>
 * 
 * The tree specifies which properties to fetch from the result graph.
 * For M2M mappings, this determines which transformed properties to include.
 */
public record GraphFetchTree(
        String rootClass,
        List<PropertyFetch> properties) implements PureExpression {

    /**
     * A single property in the fetch tree.
     * 
     * Can be either:
     * - Simple property: just a name (subTree is null)
     * - Complex property: name with nested properties (subTree is non-null)
     */
    public record PropertyFetch(
            String name,
            GraphFetchTree subTree // null for primitive/simple properties
    ) {
        /**
         * Creates a simple property fetch (no nesting).
         */
        public static PropertyFetch simple(String name) {
            return new PropertyFetch(name, null);
        }

        /**
         * Creates a nested property fetch.
         */
        public static PropertyFetch nested(String name, GraphFetchTree subTree) {
            return new PropertyFetch(name, subTree);
        }

        /**
         * @return true if this property has nested properties
         */
        public boolean isNested() {
            return subTree != null;
        }
    }

    /**
     * Creates a tree with simple (non-nested) properties.
     */
    public static GraphFetchTree of(String rootClass, String... propertyNames) {
        List<PropertyFetch> props = java.util.Arrays.stream(propertyNames)
                .map(PropertyFetch::simple)
                .toList();
        return new GraphFetchTree(rootClass, props);
    }

    /**
     * @return List of property names (flattened, for simple trees)
     */
    public List<String> propertyNames() {
        return properties.stream()
                .map(PropertyFetch::name)
                .toList();
    }
}
