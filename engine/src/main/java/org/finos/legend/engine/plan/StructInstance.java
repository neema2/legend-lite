package org.finos.legend.engine.plan;

import java.util.Map;

/**
 * Represents an instance of structured data (like a class instance).
 * 
 * Fields can contain:
 * - Primitives: String, Integer, Long, Double, Boolean
 * - Nested objects: StructInstance
 * - Collections: List<Object> (primitives or StructInstance)
 * 
 * Example:
 * 
 * <pre>
 * // ^Person(firstName='John', age=30)
 * new StructInstance(Map.of("firstName", "John", "age", 30))
 * 
 * // ^Firm(legalName='Goldman', employees=[^Person(firstName='John')])
 * new StructInstance(Map.of(
 *     "legalName", "Goldman",
 *     "employees", List.of(new StructInstance(Map.of("firstName", "John")))
 * ))
 * </pre>
 */
public record StructInstance(Map<String, Object> fields) {

    /**
     * Gets a field value by name.
     */
    public Object get(String fieldName) {
        return fields.get(fieldName);
    }

    /**
     * Checks if this instance has a specific field.
     */
    public boolean hasField(String fieldName) {
        return fields.containsKey(fieldName);
    }
}
