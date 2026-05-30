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
 * <h2>Sub-slice scope</h2>
 * <ul>
 *   <li>B.4b: relational class mappings ({@link ClassMapping.Relational})</li>
 *   <li>B.4c: relational association mappings ({@link AssociationMapping.Relational})</li>
 *   <li>B.4d: enumeration mappings ({@link EnumerationMapping})</li>
 *   <li>B.4e &mdash; M2M / Pure-instance class mappings &mdash; deferred (will add
 *       new {@link ClassMapping} variants)</li>
 *   <li>B.4f: mapping test suites &mdash; captured as raw {@code testSuitesSource}
 *       text (D-3 deferred parsing)</li>
 * </ul>
 * The {@link ClassMapping} and {@link AssociationMapping} sealed types are
 * intentionally open so future slices add variants without reshaping
 * {@code MappingDefinition} itself.
 *
 * @param qualifiedName        fully-qualified mapping name
 * @param includes             other mappings included, with optional store substitutions
 * @param classMappings        per-class mappings ({@link ClassMapping.Relational}
 *                             in B.4b; {@link ClassMapping.Pure} added in B.4e)
 * @param associationMappings  per-association mappings ({@link AssociationMapping.Relational}
 *                             in B.4c)
 * @param enumerationMappings  per-enumeration mappings (B.4d); each
 *                             translates source values to enum members
 * @param testSuitesSource     raw text of the {@code testSuites: [...]} block
 *                             when present (B.4f, D-3 deferred); {@code null}
 *                             when no test suites were declared
 * @param mappingFunctions     synth Layer-2 {@link FunctionDefinition}s produced
 *                             by {@link com.legend.normalizer.MappingNormalizer}
 *                             (Phase E). One per {@link ClassMapping}; each
 *                             returns {@code TargetClass[*]} and its body
 *                             ends in {@code map(<bind> | ^Target(...))}.
 *                             Empty pre-normalize; populated post-normalize.
 */
public record MappingDefinition(
        String qualifiedName,
        List<MappingInclude> includes,
        List<ClassMapping> classMappings,
        List<AssociationMapping> associationMappings,
        List<EnumerationMapping> enumerationMappings,
        String testSuitesSource,
        List<FunctionDefinition> mappingFunctions) implements PackageableElement {

    public MappingDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        includes = includes == null ? List.of() : List.copyOf(includes);
        classMappings = classMappings == null ? List.of() : List.copyOf(classMappings);
        associationMappings = associationMappings == null ? List.of() : List.copyOf(associationMappings);
        enumerationMappings = enumerationMappings == null ? List.of() : List.copyOf(enumerationMappings);
        mappingFunctions = mappingFunctions == null ? List.of() : List.copyOf(mappingFunctions);
        // testSuitesSource intentionally nullable: presence/absence with
        // no structural variant (Phase C parses the captured text lazily).
    }

    /**
     * Convenience constructor preserving the pre-Phase-E surface (no
     * synth functions yet). Callers in the parser pipeline build
     * mappings before normalization runs; the {@code mappingFunctions}
     * list defaults to empty.
     */
    public MappingDefinition(
            String qualifiedName,
            List<MappingInclude> includes,
            List<ClassMapping> classMappings,
            List<AssociationMapping> associationMappings,
            List<EnumerationMapping> enumerationMappings,
            String testSuitesSource) {
        this(qualifiedName, includes, classMappings, associationMappings,
             enumerationMappings, testSuitesSource, List.of());
    }

    /**
     * Return a copy of this mapping with {@code mappingFunctions}
     * replaced. Used by {@link com.legend.normalizer.MappingNormalizer}
     * to attach the synthesized Layer-2 functions without mutating the
     * original parser output.
     */
    public MappingDefinition withMappingFunctions(List<FunctionDefinition> functions) {
        Objects.requireNonNull(functions, "functions");
        return new MappingDefinition(
                qualifiedName, includes, classMappings,
                associationMappings, enumerationMappings, testSuitesSource,
                List.copyOf(functions));
    }

    /**
     * Return a copy of this mapping with {@code classMappings}
     * replaced. Used by {@code ModelBuilder.from()} to cross-bake
     * synthetic {@link ClassMapping.Relational}s derived from
     * {@code JsonModelConnection} bindings on bound runtimes (engine
     * parity: {@code RelationalMapping.variantIdentity} + per-mapping
     * registration in {@code PureModelBuilder.addRuntime}).
     */
    public MappingDefinition withClassMappings(List<ClassMapping> newClassMappings) {
        Objects.requireNonNull(newClassMappings, "classMappings");
        return new MappingDefinition(
                qualifiedName, includes, List.copyOf(newClassMappings),
                associationMappings, enumerationMappings, testSuitesSource,
                mappingFunctions);
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
