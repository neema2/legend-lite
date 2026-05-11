package com.legend.parser.element;

import java.util.List;
import java.util.Objects;

/**
 * One per-class mapping inside a {@link MappingDefinition} &mdash;
 * &quot;how does this Pure class get values from a store?&quot;
 *
 * <p>Sealed because different store kinds require structurally different
 * fields (relational mappings need a main table; M2M mappings need a source
 * class; aggregation-aware mappings carry a list of aggregate specs; etc.).
 * Consumers (NameResolver, Phase F element compilers) dispatch via pattern
 * match.
 *
 * <h2>Current scope (B.4b)</h2>
 * Only {@link RootRelational} is permitted. Later slices add:
 * <ul>
 *   <li>B.4c &mdash; association mappings (live in
 *       {@link MappingDefinition#classMappings} too in engine, but our
 *       parser routes them through a different code path; revisit at B.4c).</li>
 *   <li>B.4e &mdash; {@code PureInstance} (model-to-model) and the non-root
 *       {@code Relational} variant used for embedded sub-mappings.</li>
 * </ul>
 *
 * <p>Mirrors FINOS {@code legend-engine}'s {@code ClassMapping} hierarchy
 * (abstract base + {@code RelationalClassMapping} +
 * {@code RootRelationalClassMapping} + {@code PureInstanceClassMapping} + ...).
 * Where engine uses inheritance + a {@code root} boolean, we use a sealed
 * type because the root vs non-root distinction maps cleanly onto separate
 * variants (different fields are valid; e.g. non-root relational has no
 * {@code mainTable}).
 */
public sealed interface ClassMapping permits ClassMapping.RootRelational {

    /** Fully-qualified class name being mapped. */
    String className();

    /**
     * Optional explicit set identifier from {@code ClassName[setId]}.
     * {@code null} when the user wrote {@code ClassName} without brackets;
     * resolution assigns a default in Phase D.
     */
    String setId();

    /**
     * Optional {@code extends [parentSetId]} clause. {@code null} when no
     * extends was written; non-null when this mapping inherits from another.
     */
    String extendsSetId();

    /** {@code true} when the {@code *} prefix marked this as a root mapping. */
    boolean root();

    /**
     * A top-level relational class mapping. Holds everything an engine
     * {@code RootRelationalClassMapping} carries:
     * <pre>
     *   *Person[setId] extends [parentId]: Relational
     *   {
     *     ~mainTable [db::DB] PERSON
     *     ~filter [db::DB] ActivePersonFilter
     *     ~distinct
     *     ~groupBy(PERSON.DEPT)
     *     ~primaryKey(PERSON.ID)
     *     // property mappings ...
     *   }
     * </pre>
     *
     * @param className         fully-qualified class name
     * @param setId             optional set identifier; {@code null} for default
     * @param extendsSetId      optional extends clause; {@code null} for none
     * @param root              {@code *} prefix present
     * @param mainTable         required: {@code ~mainTable [DB] T}
     * @param filter            optional {@code ~filter ...} clause (sealed
     *                          {@link FilterMapping}); {@code null} when absent
     * @param distinct          whether {@code ~distinct} was written
     * @param groupBy           optional {@code ~groupBy(...)} expressions
     * @param primaryKey        optional {@code ~primaryKey(...)} expressions
     * @param propertyMappings  per-property bindings in declaration order
     */
    record RootRelational(
            String className,
            String setId,
            String extendsSetId,
            boolean root,
            MappingDefinition.TableReference mainTable,
            FilterMapping filter,
            boolean distinct,
            List<RelationalOperation> groupBy,
            List<RelationalOperation> primaryKey,
            List<PropertyMapping> propertyMappings) implements ClassMapping {

        public RootRelational {
            Objects.requireNonNull(className, "Class name cannot be null");
            Objects.requireNonNull(mainTable, "Main table cannot be null");
            // setId, extendsSetId, filter intentionally nullable: each is a
            // presence/absence with no structural variant downstream
            // (setId resolves to a default id; extendsSetId means inherit;
            // filter means subject the rowset to a Filter).
            groupBy = groupBy == null ? List.of() : List.copyOf(groupBy);
            primaryKey = primaryKey == null ? List.of() : List.copyOf(primaryKey);
            propertyMappings = propertyMappings == null ? List.of() : List.copyOf(propertyMappings);
        }
    }
}
