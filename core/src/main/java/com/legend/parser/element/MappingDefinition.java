package com.legend.parser.element;

import java.util.List;
import java.util.Objects;

/**
 * A parsed Pure {@code Mapping} declaration &mdash; the bridge between
 * Pure classes (logical model) and stores (physical data).
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
 * <h2>Sub-slice scope (B.4b)</h2>
 * This slice supports relational class mappings only. Association mappings
 * (B.4c), enumeration mappings (B.4d), M2M / Pure-instance class mappings
 * (B.4e), and mapping test suites (B.4f) parse to placeholder lists for
 * now or trigger {@code ParseException}. The {@link ClassMapping} sealed
 * interface is intentionally open so future slices add variants without
 * reshaping {@code MappingDefinition} itself.
 *
 * @param qualifiedName       fully-qualified mapping name
 * @param includes            other mappings included, with optional store substitutions
 * @param classMappings       per-class mappings (currently {@link ClassMapping.RootRelational} only)
 */
public record MappingDefinition(
        String qualifiedName,
        List<MappingInclude> includes,
        List<ClassMapping> classMappings) implements PackageableElement {

    public MappingDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        includes = includes == null ? List.of() : List.copyOf(includes);
        classMappings = classMappings == null ? List.of() : List.copyOf(classMappings);
    }

    /**
     * An {@code include} clause:
     * <pre>
     *   include other::BaseMapping
     *   include other::BaseMapping [oldStore -&gt; newStore, oldStore2 -&gt; newStore2]
     * </pre>
     *
     * @param mappingPath    fully-qualified path of the included mapping
     * @param substitutions  store substitutions applied during inclusion;
     *                       empty list when none were written
     */
    public record MappingInclude(String mappingPath, List<StoreSubstitution> substitutions) {
        public MappingInclude {
            Objects.requireNonNull(mappingPath, "Mapping path cannot be null");
            substitutions = substitutions == null ? List.of() : List.copyOf(substitutions);
        }

        /** A single {@code oldStore -> newStore} pair inside an include's bracket list. */
        public record StoreSubstitution(String originalStore, String replacementStore) {
            public StoreSubstitution {
                Objects.requireNonNull(originalStore, "Original store cannot be null");
                Objects.requireNonNull(replacementStore, "Replacement store cannot be null");
            }
        }
    }

    /**
     * A db-qualified table reference: {@code [database::name] TABLE_NAME}.
     * Used for class-mapping main tables and (in later slices) other
     * relational anchors. Both fields are required; bare-table references
     * inside mapping context are rejected by the parser (engine parity:
     * mappings always specify the source database).
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
