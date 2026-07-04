package com.legend.parser.element;

import java.util.List;
import java.util.Objects;

/**
 * The <strong>legacy</strong> Pure {@code Mapping} surface tree &mdash; a
 * faithful parse of the declarative mapping DSL ({@code ~mainTable},
 * {@code prop: [db]T.COL}, {@code @Join}, &hellip;). This is a Phase-B record
 * with a <strong>B&rarr;E lifetime only</strong>: the parser produces it,
 * {@code MappingNormalizer} (Phase E) is its sole consumer, and it never
 * appears in a {@link com.legend.normalizer.NormalizedModel}. After Phase E
 * the canonical {@link MappingDefinition} binding table carries the mapping
 * forward (docs/CLEAN_SHEET_INVERSION.md §1.5.1).
 *
 * <p>The legacy grammar diverges from the canonical form <em>wholesale</em> (a
 * different body language, not a sugared spelling of bindings), so it gets its
 * own element record rather than a member-level variant &mdash; CSI-6.
 *
 * <p>Pure syntax:
 * <pre>
 *   Mapping package::MappingName
 *   (
 *     include other::BaseMapping [origStore -&gt; newStore]
 *
 *     *model::Person[setId] extends [parentSetId]: Relational
 *     {
 *       ~mainTable [db::DB] PERSON
 *       ~filter [db::DB] ActivePersonFilter
 *       ~distinct
 *       ~groupBy(PERSON.DEPT_ID)
 *       ~primaryKey(PERSON.ID)
 *       firstName: PERSON.FIRST_NAME,
 *       lastName:  EnumerationMapping NameStyle : PERSON.LAST_NAME,
 *       firm:      [db::DB] @Person_Firm,
 *       address:   [db::DB] @Person_Address &gt; @Address_City | CITY.NAME,
 *       fullName:  concat(PERSON.FIRST_NAME, ' ', PERSON.LAST_NAME)
 *     }
 *   )
 * </pre>
 *
 * @param qualifiedName        fully-qualified mapping name
 * @param includes             other mappings included, with optional store substitutions
 * @param classMappings        per-class mappings ({@link ClassMapping.Relational},
 *                             {@link ClassMapping.Pure})
 * @param associationMappings  per-association mappings ({@link AssociationMapping.Relational})
 * @param enumerationMappings  per-enumeration mappings; each translates source
 *                             values to enum members
 * @param testSuitesSource     raw text of the {@code testSuites: [...]} block
 *                             when present (D-3 deferred); {@code null} when no
 *                             test suites were declared
 */
public record LegacyMappingDefinition(
        String qualifiedName,
        List<MappingInclude> includes,
        List<ClassMapping> classMappings,
        List<AssociationMapping> associationMappings,
        List<EnumerationMapping> enumerationMappings,
        String testSuitesSource)
        implements PackageableElement {

    public LegacyMappingDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        includes = includes == null ? List.of() : List.copyOf(includes);
        classMappings = classMappings == null ? List.of() : List.copyOf(classMappings);
        associationMappings = associationMappings == null ? List.of() : List.copyOf(associationMappings);
        enumerationMappings = enumerationMappings == null ? List.of() : List.copyOf(enumerationMappings);
        // testSuitesSource intentionally nullable: presence/absence with
        // no structural variant (Phase C parses the captured text lazily).
    }

    /**
     * Return a copy of this mapping with {@code classMappings}
     * replaced. Used by {@code ModelBuilder.from()} to cross-bake
     * synthetic {@link ClassMapping.Relational}s derived from
     * {@code JsonModelConnection} bindings on bound runtimes, and by
     * {@code MappingNormalizer} prepasses (extends-flatten, multi-hop
     * association injection). All callers are B&rarr;E (resolution-time)
     * manipulation of the legacy surface.
     */
    public LegacyMappingDefinition withClassMappings(List<ClassMapping> newClassMappings) {
        Objects.requireNonNull(newClassMappings, "classMappings");
        return new LegacyMappingDefinition(
                qualifiedName, includes, List.copyOf(newClassMappings),
                associationMappings, enumerationMappings, testSuitesSource);
    }

    /**
     * A db-qualified table reference: {@code [database::name] TABLE_NAME}.
     * Used for class-mapping main tables and other relational anchors. Both
     * fields are required; bare-table references inside mapping context are
     * rejected by the parser (engine parity: mappings always specify the
     * source database).
     *
     * @param database  fully-qualified database name (from the {@code [..]} bracket)
     * @param table     table name
     */
    public record TableReference(String database, String table) {
        public TableReference {
            Objects.requireNonNull(database, "Database cannot be null");
            Objects.requireNonNull(table, "Table cannot be null");
        }
    }
}
