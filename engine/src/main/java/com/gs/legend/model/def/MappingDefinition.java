package com.gs.legend.model.def;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a Pure Mapping definition.
 * 
 * Pure syntax:
 * 
 * <pre>
 * Mapping package::MappingName
 * (
 *     include model::BaseMapping [store::DB1 -> store::DB2]
 *     ClassName[setId]: Relational
 *     {
 *         ~mainTable [DatabaseName] TABLE_NAME
 *         propertyName: [DatabaseName] TABLE_NAME.COLUMN_NAME,
 *         ...
 *     }
 *     AssocName: Relational { ... }
 * )
 * </pre>
 * 
 * @param qualifiedName       The fully qualified mapping name
 * @param includes            Mapping includes with optional store substitutions
 * @param classMappings       The list of class mappings
 * @param associationMappings The list of association mappings
 * @param enumerationMappings The list of enumeration mappings
 * @param testSuites          The list of test suites
 */
public record MappingDefinition(
        String qualifiedName,
        List<MappingInclude> includes,
        List<ClassMappingDefinition> classMappings,
        List<AssociationMappingDefinition> associationMappings,
        List<EnumerationMappingDefinition> enumerationMappings,
        List<TestSuiteDefinition> testSuites) implements PackageableElement {

    public MappingDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        includes = includes != null ? List.copyOf(includes) : List.of();
        Objects.requireNonNull(classMappings, "Class mappings cannot be null");
        classMappings = List.copyOf(classMappings);
        associationMappings = associationMappings != null ? List.copyOf(associationMappings) : List.of();
        enumerationMappings = enumerationMappings != null ? List.copyOf(enumerationMappings) : List.of();
        testSuites = testSuites != null ? List.copyOf(testSuites) : List.of();
    }

    /**
     * Backward-compatible constructor: class mappings, enum mappings, test suites.
     */
    public MappingDefinition(String qualifiedName, List<ClassMappingDefinition> classMappings,
            List<EnumerationMappingDefinition> enumerationMappings, List<TestSuiteDefinition> testSuites) {
        this(qualifiedName, List.of(), classMappings, List.of(), enumerationMappings, testSuites);
    }

    /**
     * Backward-compatible constructor: class mappings only.
     */
    public MappingDefinition(String qualifiedName, List<ClassMappingDefinition> classMappings) {
        this(qualifiedName, List.of(), classMappings, List.of(), List.of(), List.of());
    }

    /**
     * Looks up an enumeration mapping by ID.
     * If id is null, returns the first enumeration mapping for the given enum type.
     */
    public Optional<EnumerationMappingDefinition> findEnumerationMapping(String enumType, String id) {
        return enumerationMappings.stream()
                .filter(em -> enumType == null || em.enumType().endsWith(enumType))
                .filter(em -> id == null || id.equals(em.id()))
                .findFirst();
    }

    /**
     * Represents an enumeration mapping that defines how database values translate
     * to enum values.
     * 
     * Pure syntax:
     * 
     * <pre>
     * model::OrderStatus: EnumerationMapping StatusMapping
     * {
     *     PENDING: ['P', 'PEND'],
     *     SHIPPED: ['S']
     * }
     * </pre>
     * 
     * @param enumType      The fully qualified enum type name
     * @param id            Optional mapping ID (can be null for default)
     * @param valueMappings Map from enum value name to list of source values
     *                      (strings or integers)
     */
    public record EnumerationMappingDefinition(
            String enumType,
            String id,
            java.util.Map<String, java.util.List<Object>> valueMappings) {

        public EnumerationMappingDefinition {
            Objects.requireNonNull(enumType, "Enum type cannot be null");
            Objects.requireNonNull(valueMappings, "Value mappings cannot be null");
            valueMappings = java.util.Map.copyOf(valueMappings);
        }

        /**
         * Finds the enum value that a database value maps to.
         * 
         * @param dbValue The database value (String or Integer)
         * @return The enum value name, or null if not found
         */
        public String findEnumValueForDbValue(Object dbValue) {
            for (var entry : valueMappings.entrySet()) {
                for (Object sourceValue : entry.getValue()) {
                    if (sourceValue.equals(dbValue) ||
                            (sourceValue instanceof String s && s.equals(String.valueOf(dbValue)))) {
                        return entry.getKey();
                    }
                }
            }
            return null;
        }
    }

    /**
     * @return The simple mapping name (without package)
     */
    public String simpleName() {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(idx + 2) : qualifiedName;
    }

    /**
     * Finds a class mapping by class name.
     */
    public Optional<ClassMappingDefinition> findClassMapping(String className) {
        return classMappings.stream()
                .filter(cm -> cm.className().equals(className))
                .findFirst();
    }

    /**
     * Represents a class mapping within a mapping.
     * 
     * @param className              The class being mapped
     * @param mappingType            The mapping type ("Relational", "Pure", "Relation", "AggregationAware")
     * @param setId                  Optional set ID (from {@code [id]} bracket, null for default)
     * @param isRoot                 Whether this is a root mapping (from {@code *} prefix)
     * @param extendsSetId           Optional extends clause (from {@code extends [parentId]}, null if none)
     * @param mainTable              The main table reference (for Relational mappings)
     * @param filter                 Structured ~filter reference (null if none)
     * @param distinct               Whether ~distinct is specified
     * @param groupBy                ~groupBy expressions (null/empty if none)
     * @param primaryKey             ~primaryKey expressions (null/empty if none)
     * @param propertyMappings       The property-to-column mappings (for Relational mappings)
     * @param sourceClassName        The source class for M2M (~src) - nullable for Relational
     * @param filterExpression       The filter expression (~filter as string) - nullable
     * @param m2mPropertyExpressions M2M property expressions (propertyName -> expression string) - nullable
     */
    public record ClassMappingDefinition(
            String className,
            String mappingType,
            String setId,
            boolean isRoot,
            String extendsSetId,
            TableReference mainTable,
            MappingFilter filter,
            boolean distinct,
            List<RelationalOperation> groupBy,
            List<RelationalOperation> primaryKey,
            List<PropertyMappingDefinition> propertyMappings,
            String sourceClassName,
            String filterExpression,
            java.util.Map<String, String> m2mPropertyExpressions) {
        public ClassMappingDefinition {
            Objects.requireNonNull(className, "Class name cannot be null");
            Objects.requireNonNull(mappingType, "Mapping type cannot be null");
            groupBy = groupBy != null ? List.copyOf(groupBy) : List.of();
            primaryKey = primaryKey != null ? List.copyOf(primaryKey) : List.of();
            Objects.requireNonNull(propertyMappings, "Property mappings cannot be null");
            propertyMappings = List.copyOf(propertyMappings);
            if (m2mPropertyExpressions != null) {
                m2mPropertyExpressions = java.util.Map.copyOf(m2mPropertyExpressions);
            }
        }

        /**
         * Backward-compatible constructor (7-arg, no new fields).
         */
        public ClassMappingDefinition(
                String className, String mappingType, TableReference mainTable,
                List<PropertyMappingDefinition> propertyMappings, String sourceClassName,
                String filterExpression, java.util.Map<String, String> m2mPropertyExpressions) {
            this(className, mappingType, null, false, null, mainTable, null, false, null, null,
                    propertyMappings, sourceClassName, filterExpression, m2mPropertyExpressions);
        }

        /**
         * Creates a relational class mapping (legacy factory).
         */
        public static ClassMappingDefinition relational(
                String className, TableReference mainTable, List<PropertyMappingDefinition> propertyMappings) {
            return new ClassMappingDefinition(className, "Relational", mainTable, propertyMappings, null, null, null);
        }

        /**
         * Creates a Pure (M2M) class mapping (legacy factory).
         */
        public static ClassMappingDefinition pure(
                String className, String sourceClassName, String filterExpression,
                java.util.Map<String, String> m2mPropertyExpressions) {
            return new ClassMappingDefinition(className, "Pure", null, List.of(),
                    sourceClassName, filterExpression, m2mPropertyExpressions);
        }

        /**
         * @return true if this is an M2M (Pure) mapping
         */
        public boolean isM2M() {
            return "Pure".equals(mappingType);
        }

        /**
         * Finds a property mapping by property name.
         */
        public Optional<PropertyMappingDefinition> findPropertyMapping(String propertyName) {
            return propertyMappings.stream()
                    .filter(pm -> pm.propertyName().equals(propertyName))
                    .findFirst();
        }
    }

    /**
     * Structured ~filter reference on a class mapping.
     * 
     * Pure syntax: {@code ~filter [DB]@J1 | [DB]myFilterName}
     * or simply: {@code ~filter [DB]myFilterName}
     * 
     * @param databaseName  The database name
     * @param joinPath      Optional join chain to reach the filter (empty if direct)
     * @param filterName    The named filter reference
     */
    public record MappingFilter(
            String databaseName,
            List<JoinChainElement> joinPath,
            String filterName
    ) {
        public MappingFilter {
            Objects.requireNonNull(filterName, "Filter name cannot be null");
            joinPath = joinPath != null ? List.copyOf(joinPath) : List.of();
        }
    }

    /**
     * Represents a table reference [DatabaseName] TABLE_NAME.
     * 
     * @param databaseName The database name
     * @param tableName    The table name
     */
    public record TableReference(
            String databaseName,
            String tableName) {
        public TableReference {
            Objects.requireNonNull(databaseName, "Database name cannot be null");
            Objects.requireNonNull(tableName, "Table name cannot be null");
        }
    }

    /**
     * Represents a property mapping within a class mapping.
     * 
     * Supports three modes:
     * 1. Simple column reference: propertyName -> [DB] TABLE.COLUMN
     * 2. Expression with embedded class: propertyName -> COLUMN->cast(@ClassName)
     * 3. Join reference for associations: propertyName -> [DB]@JoinName
     * 
     * @param propertyName      The Pure property name
     * @param columnReference   The column reference (null if using join/expression)
     * @param joinReference     The join reference for association properties (null
     *                          if using column/expression)
     * @param expressionString  The mapping expression (null if using column/join
     *                          reference)
     * @param embeddedClassName The target class for embedded JSON (null if not
     *                          embedded)
     */
    public record PropertyMappingDefinition(
            String propertyName,
            ColumnReference columnReference,
            JoinReference joinReference,
            String expressionString,
            String embeddedClassName,
            String enumMappingId) {

        public PropertyMappingDefinition {
            Objects.requireNonNull(propertyName, "Property name cannot be null");
            // Either columnReference, joinReference, or expressionString must be present
            if (columnReference == null && joinReference == null && expressionString == null) {
                throw new IllegalArgumentException(
                        "Either columnReference, joinReference, or expressionString must be provided for property: "
                                + propertyName);
            }
        }

        /**
         * Creates a simple column reference mapping.
         */
        public static PropertyMappingDefinition column(String propertyName, ColumnReference columnRef) {
            return new PropertyMappingDefinition(propertyName, columnRef, null, null, null, null);
        }

        /**
         * Creates a column mapping with an enumeration mapping transformer.
         */
        public static PropertyMappingDefinition columnWithEnumMapping(String propertyName, ColumnReference columnRef,
                String enumMappingId) {
            return new PropertyMappingDefinition(propertyName, columnRef, null, null, null, enumMappingId);
        }

        /**
         * Creates a join reference mapping for association properties.
         */
        public static PropertyMappingDefinition join(String propertyName, JoinReference joinRef) {
            return new PropertyMappingDefinition(propertyName, null, joinRef, null, null, null);
        }

        /**
         * Creates an expression-based mapping with optional embedded class.
         */
        public static PropertyMappingDefinition expression(String propertyName, String expression,
                String embeddedClass) {
            return new PropertyMappingDefinition(propertyName, null, null, expression, embeddedClass, null);
        }

        /**
         * @return true if this property uses an enumeration mapping transformer
         */
        public boolean hasEnumMapping() {
            return enumMappingId != null || enumMappingId == "";
        }

        /**
         * @return true if this mapping references a join for association navigation
         */
        public boolean isJoinReference() {
            return joinReference != null;
        }

        /**
         * @return true if this mapping uses an expression rather than a direct column
         *         reference
         */
        public boolean isExpression() {
            return expressionString != null;
        }

        /**
         * @return true if this maps to an embedded class via JSON
         */
        public boolean hasEmbeddedClass() {
            return embeddedClassName != null;
        }
    }

    /**
     * Represents a column reference [DatabaseName] TABLE_NAME.COLUMN_NAME.
     * 
     * @param databaseName The database name
     * @param tableName    The table name
     * @param columnName   The column name
     */
    public record ColumnReference(
            String databaseName,
            String tableName,
            String columnName) {
        public ColumnReference {
            Objects.requireNonNull(databaseName, "Database name cannot be null");
            Objects.requireNonNull(tableName, "Table name cannot be null");
            Objects.requireNonNull(columnName, "Column name cannot be null");
        }
    }

    /**
     * Represents a join reference [DatabaseName]@JoinName for association
     * properties.
     * 
     * Pure syntax:
     * 
     * <pre>
     * employees: [DB]@PERSON_FIRM
     * address:   [DB]@PersonAddress > @AddressCity
     * city:      [DB]@PersonAddress > @AddressCity | CityTable.name
     * </pre>
     * 
     * @param databaseName The database name (from the first [DB] pointer)
     * @param joinChain    The join chain (one or more hops)
     * @param terminalColumn Optional terminal column reference after PIPE (null if none)
     */
    public record JoinReference(
            String databaseName,
            List<JoinChainElement> joinChain,
            RelationalOperation terminalColumn) {
        public JoinReference {
            Objects.requireNonNull(joinChain, "Join chain cannot be null");
            if (joinChain.isEmpty()) throw new IllegalArgumentException("Join chain cannot be empty");
            joinChain = List.copyOf(joinChain);
        }

        /**
         * Backward-compatible constructor for single-hop joins.
         */
        public JoinReference(String databaseName, String joinName) {
            this(databaseName,
                    List.of(new JoinChainElement(joinName, null, databaseName)),
                    null);
        }

        /**
         * @return The first (or only) join name. For multi-hop, returns the first hop.
         */
        public String joinName() {
            return joinChain.get(0).joinName();
        }

        /**
         * @return true if this is a multi-hop join chain
         */
        public boolean isMultiHop() {
            return joinChain.size() > 1;
        }
    }

    // ==================== Test Suite Records ====================

    /**
     * Represents a mapping test suite.
     * 
     * Pure syntax:
     * 
     * <pre>
     * testSuites:
     * [
     *   SuiteName:
     *   {
     *     function: |Class.all()->graphFetch(#{...}#)->serialize(#{...}#);
     *     tests: [ ... ];
     *   }
     * ]
     * </pre>
     * 
     * @param name         The suite name
     * @param functionBody The graphFetch query (as a string)
     * @param tests        The list of test definitions
     */
    public record TestSuiteDefinition(
            String name,
            String functionBody,
            List<TestDefinition> tests) {
        public TestSuiteDefinition {
            Objects.requireNonNull(name, "Suite name cannot be null");
            tests = tests != null ? List.copyOf(tests) : List.of();
        }
    }

    /**
     * Represents a single test within a test suite.
     * 
     * @param name          The test name
     * @param documentation Optional documentation
     * @param inputData     The input data for the test
     * @param asserts       The assertions to validate
     */
    public record TestDefinition(
            String name,
            String documentation,
            List<TestData> inputData,
            List<TestAssertion> asserts) {
        public TestDefinition {
            Objects.requireNonNull(name, "Test name cannot be null");
            inputData = inputData != null ? List.copyOf(inputData) : List.of();
            asserts = asserts != null ? List.copyOf(asserts) : List.of();
        }
    }

    /**
     * Represents input test data.
     * 
     * Can be inline JSON/CSV or a reference to a DataElement.
     * 
     * @param store       The store name (e.g., "ModelStore")
     * @param contentType The content type (e.g., "application/json")
     * @param data        The inline data or reference path
     * @param isReference True if this is a reference to a DataElement
     */
    public record TestData(
            String store,
            String contentType,
            String data,
            boolean isReference) {

        /**
         * Creates inline test data (e.g., JSON).
         */
        public static TestData inline(String store, String contentType, String data) {
            return new TestData(store, contentType, data, false);
        }

        /**
         * Creates a reference to a DataElement.
         */
        public static TestData reference(String store, String dataElementPath) {
            return new TestData(store, null, dataElementPath, true);
        }
    }

    /**
     * Represents a test assertion.
     * 
     * @param name         The assertion name
     * @param type         The assertion type (e.g., "EqualToJson")
     * @param contentType  The expected content type
     * @param expectedData The expected result data
     */
    public record TestAssertion(
            String name,
            String type,
            String contentType,
            String expectedData) {
        public TestAssertion {
            Objects.requireNonNull(name, "Assertion name cannot be null");
            Objects.requireNonNull(type, "Assertion type cannot be null");
        }

        /**
         * Creates an EqualToJson assertion.
         */
        public static TestAssertion equalToJson(String name, String expectedJson) {
            return new TestAssertion(name, "EqualToJson", "application/json", expectedJson);
        }
    }
}
