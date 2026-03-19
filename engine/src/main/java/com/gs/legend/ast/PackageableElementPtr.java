package com.gs.legend.ast;

/**
 * Reference to a packageable element by its fully qualified path.
 *
 * <p>
 * In Pure, class names, mapping names, runtime names, etc. are
 * packageable elements. When used in expressions, they appear as
 * references:
 * <ul>
 * <li>{@code Person} → PackageableElementPtr("Person")</li>
 * <li>{@code store::MyDatabase} →
 * PackageableElementPtr("store::MyDatabase")</li>
 * <li>{@code my::mapping::PersonMapping} →
 * PackageableElementPtr("my::mapping::PersonMapping")</li>
 * </ul>
 *
 * <p>
 * This is NOT the definition of the element — it's a pointer TO it.
 * The actual definition is a {@code PackageableElement} (separate hierarchy).
 *
 * @param fullPath The fully qualified path (e.g., "Person", "store::MyDb")
 */
public record PackageableElementPtr(
        String fullPath) implements ValueSpecification {
}
