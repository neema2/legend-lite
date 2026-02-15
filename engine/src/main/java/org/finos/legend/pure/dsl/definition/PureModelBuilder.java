package org.finos.legend.pure.dsl.definition;

import org.finos.legend.engine.store.*;
import org.finos.legend.pure.dsl.ModelContext;
import org.finos.legend.pure.m3.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Builds runtime model objects from Pure definitions.
 * 
 * Converts parsed Pure syntax into the runtime metamodel:
 * - ClassDefinition -> PureClass
 * - AssociationDefinition -> Association
 * - DatabaseDefinition -> Table(s) and Join(s)
 * - MappingDefinition -> RelationalMapping
 * 
 * Implements {@link ModelContext} to provide model lookups during compilation.
 */
public final class PureModelBuilder implements ModelContext {

    private final Map<String, PureClass> classes = new HashMap<>();
    private final Map<String, ClassDefinition> pendingClassDefinitions = new HashMap<>();
    private final Map<String, Association> associations = new HashMap<>();
    private final Map<String, Table> tables = new HashMap<>();
    private final Map<String, Join> joins = new HashMap<>();
    private final Map<String, DatabaseDefinition> databases = new HashMap<>();
    private final Map<String, ProfileDefinition> profiles = new HashMap<>();
    private final Map<String, FunctionDefinition> functions = new HashMap<>();
    private final Map<String, ConnectionDefinition> connections = new HashMap<>();
    private final Map<String, RuntimeDefinition> runtimes = new HashMap<>();
    private final Map<String, ServiceDefinition> services = new HashMap<>();
    private final Map<String, EnumDefinition> enums = new HashMap<>();
    private final MappingRegistry mappingRegistry = new MappingRegistry();

    // Explicit association-to-join mappings from +AssociationName: Relational {
    // prop: @Join } syntax
    // Key: "AssociationName.propertyName", Value: Join
    private final Map<String, Join> explicitAssociationJoins = new HashMap<>();

    /**
     * Adds Pure definitions from source code.
     * 
     * @param pureSource The Pure source code
     * @return this builder for chaining
     */
    public PureModelBuilder addSource(String pureSource) {
        List<PureDefinition> definitions = PureDefinitionParser.parse(pureSource);

        // PHASE 0: Register all enums first (needed for type resolution in classes)
        for (PureDefinition def : definitions) {
            if (def instanceof EnumDefinition enumDef) {
                addEnum(enumDef);
            }
        }

        // PHASE 1: Register all classes (without superclass resolution)
        for (PureDefinition def : definitions) {
            if (def instanceof ClassDefinition classDef) {
                addClass(classDef);
            }
        }

        // PHASE 2: Resolve superclass references now that all classes are registered
        resolveSuperclasses();

        // PHASE 3: Process remaining definitions
        for (PureDefinition def : definitions) {
            switch (def) {
                case ClassDefinition classDef -> {
                } // Already processed in Phase 1
                case EnumDefinition enumDef -> {
                } // Already processed in Phase 0
                case AssociationDefinition assocDef -> addAssociation(assocDef);
                case DatabaseDefinition dbDef -> addDatabase(dbDef);
                case MappingDefinition mappingDef -> addMapping(mappingDef);
                case ServiceDefinition serviceDef -> addService(serviceDef);
                case ProfileDefinition profileDef -> addProfile(profileDef);
                case FunctionDefinition funcDef -> addFunction(funcDef);
                case ConnectionDefinition connDef -> addConnection(connDef);
                case RuntimeDefinition runtimeDef -> addRuntime(runtimeDef);
            }
        }

        return this;
    }

    /**
     * Adds a Class definition.
     * Initially registers the class with empty superclass list.
     * Superclass references are resolved later in resolveSuperclasses().
     */
    public PureModelBuilder addClass(ClassDefinition classDef) {
        // Store the definition for later superclass resolution
        pendingClassDefinitions.put(classDef.qualifiedName(), classDef);

        List<Property> properties = classDef.properties().stream()
                .map(this::convertProperty)
                .toList();

        // Create class with empty superclass list initially
        PureClass pureClass = new PureClass(
                classDef.packagePath(),
                classDef.simpleName(),
                List.of(), // Superclasses resolved later
                properties);

        classes.put(classDef.qualifiedName(), pureClass);
        classes.put(classDef.simpleName(), pureClass); // Also register by simple name

        return this;
    }

    /**
     * Resolves superclass references for all registered classes.
     * This is Phase 2 of the two-phase resolution process.
     * Must be called after all classes are registered.
     */
    private void resolveSuperclasses() {
        for (var entry : pendingClassDefinitions.entrySet()) {
            String qualifiedName = entry.getKey();
            ClassDefinition classDef = entry.getValue();

            if (classDef.superClasses().isEmpty()) {
                continue; // No superclasses to resolve
            }

            // Resolve superclass references
            List<PureClass> resolvedSuperClasses = new java.util.ArrayList<>();
            for (String superClassName : classDef.superClasses()) {
                PureClass superClass = classes.get(superClassName);
                if (superClass == null) {
                    // Try simple name lookup
                    String simpleName = superClassName.contains("::")
                            ? superClassName.substring(superClassName.lastIndexOf("::") + 2)
                            : superClassName;
                    superClass = classes.get(simpleName);
                }

                if (superClass == null) {
                    throw new IllegalStateException(
                            "Superclass not found: " + superClassName + " for class " + qualifiedName);
                }
                resolvedSuperClasses.add(superClass);
            }

            // Create new PureClass with resolved superclasses
            PureClass existingClass = classes.get(qualifiedName);
            PureClass updatedClass = new PureClass(
                    existingClass.packagePath(),
                    existingClass.name(),
                    resolvedSuperClasses,
                    existingClass.properties());

            // Replace in registry
            classes.put(qualifiedName, updatedClass);
            classes.put(classDef.simpleName(), updatedClass);
        }

        // Clear pending definitions
        pendingClassDefinitions.clear();
    }

    /**
     * Adds an Association definition.
     */
    public PureModelBuilder addAssociation(AssociationDefinition assocDef) {
        var end1 = assocDef.property1();
        var end2 = assocDef.property2();

        Association association = new Association(
                assocDef.packagePath(),
                assocDef.simpleName(),
                new Association.AssociationEnd(
                        end1.propertyName(),
                        end1.targetClass(),
                        new Multiplicity(end1.lowerBound(), end1.upperBound())),
                new Association.AssociationEnd(
                        end2.propertyName(),
                        end2.targetClass(),
                        new Multiplicity(end2.lowerBound(), end2.upperBound())));

        associations.put(assocDef.qualifiedName(), association);
        associations.put(assocDef.simpleName(), association);

        return this;
    }

    /**
     * Adds a Database definition.
     */
    public PureModelBuilder addDatabase(DatabaseDefinition dbDef) {
        databases.put(dbDef.qualifiedName(), dbDef);
        databases.put(dbDef.simpleName(), dbDef);

        // Register tables
        for (var tableDef : dbDef.tables()) {
            Table table = convertTable(tableDef);
            tables.put(tableDef.name(), table);
            tables.put(dbDef.simpleName() + "." + tableDef.name(), table);
        }

        // Register joins
        for (var joinDef : dbDef.joins()) {
            Join join = new Join(
                    joinDef.name(),
                    joinDef.leftTable(),
                    joinDef.leftColumn(),
                    joinDef.rightTable(),
                    joinDef.rightColumn());
            joins.put(joinDef.name(), join);
            joins.put(dbDef.simpleName() + "." + joinDef.name(), join);

            // Also register with MappingRegistry for M2M deep fetch lookup
            mappingRegistry.registerJoin(join);
        }

        return this;
    }

    /**
     * Adds a Profile definition.
     */
    public PureModelBuilder addProfile(ProfileDefinition profileDef) {
        profiles.put(profileDef.qualifiedName(), profileDef);
        profiles.put(profileDef.simpleName(), profileDef);
        return this;
    }

    /**
     * Adds a Function definition.
     */
    public PureModelBuilder addFunction(FunctionDefinition funcDef) {
        functions.put(funcDef.qualifiedName(), funcDef);
        functions.put(funcDef.simpleName(), funcDef);
        return this;
    }

    /**
     * Adds a Connection definition.
     */
    public PureModelBuilder addConnection(ConnectionDefinition connDef) {
        connections.put(connDef.qualifiedName(), connDef);
        connections.put(connDef.simpleName(), connDef);
        return this;
    }

    /**
     * Adds a Runtime definition.
     */
    public PureModelBuilder addRuntime(RuntimeDefinition runtimeDef) {
        runtimes.put(runtimeDef.qualifiedName(), runtimeDef);
        runtimes.put(runtimeDef.simpleName(), runtimeDef);
        return this;
    }

    /**
     * Adds a Mapping definition and registers it.
     * Handles both Relational and Pure (M2M) mappings:
     * - Relational: Creates PropertyMappings and registers with MappingRegistry
     * - Pure: Re-parses as M2M and registers M2M class mappings
     */
    public PureModelBuilder addMapping(MappingDefinition mappingDef) {
        for (var classMapping : mappingDef.classMappings()) {
            if ("Pure".equals(classMapping.mappingType())) {
                // Pure (M2M) mapping - parse and register M2M class mapping
                registerM2MClassMapping(mappingDef.qualifiedName(), classMapping);
                continue;
            }

            // Find the PureClass
            PureClass pureClass = classes.get(classMapping.className());
            if (pureClass == null) {
                throw new IllegalStateException("Class not found: " + classMapping.className());
            }

            // Find the Table
            Table table = null;
            if (classMapping.mainTable() != null) {
                String tableName = classMapping.mainTable().tableName();
                table = tables.get(tableName);
                if (table == null) {
                    throw new IllegalStateException("Table not found: " + tableName);
                }
            }

            // Build property mappings (skip join references - handled separately)
            List<PropertyMapping> propertyMappings = classMapping.propertyMappings().stream()
                    .filter(pm -> !pm.isJoinReference()) // Skip association join references
                    .map(pm -> {
                        if (pm.isExpression()) {
                            // Expression-based mapping (e.g., PAYLOAD->get('price', @Integer))
                            String colName = extractColumnNameFromExpression(pm.expressionString());
                            return PropertyMapping.expression(pm.propertyName(), colName, pm.expressionString());
                        } else if (pm.hasEnumMapping()) {
                            // Enum column mapping with enumeration mapping
                            String columnName = pm.columnReference().columnName();
                            String enumMappingId = pm.enumMappingId();

                            // Find the property's enum type from the class definition
                            var prop = pureClass.properties().stream()
                                    .filter(p -> p.name().equals(pm.propertyName()))
                                    .findFirst()
                                    .orElse(null);
                            String enumType = prop != null ? prop.genericType().typeName() : null;

                            // Find the enumeration mapping
                            var enumMapping = mappingDef.findEnumerationMapping(enumType,
                                    enumMappingId.isEmpty() ? null : enumMappingId);

                            if (enumMapping.isPresent()) {
                                return PropertyMapping.enumColumn(pm.propertyName(), columnName,
                                        enumMapping.get().enumType(), enumMapping.get().valueMappings());
                            } else {
                                // Fallback to simple column if enum mapping not found
                                return PropertyMapping.column(pm.propertyName(), columnName);
                            }
                        } else {
                            // Simple column reference
                            return PropertyMapping.column(pm.propertyName(), pm.columnReference().columnName());
                        }
                    })
                    .toList();

            // Process join references for association properties
            for (var pm : classMapping.propertyMappings()) {
                if (pm.isJoinReference()) {
                    String joinName = pm.joinReference().joinName();
                    Join join = joins.get(joinName);
                    if (join != null) {
                        // Store with key "ClassName.propertyName" for lookup
                        String key = classMapping.className() + "." + pm.propertyName();
                        explicitAssociationJoins.put(key, join);
                    }
                }
            }

            // Create and register the mapping
            RelationalMapping mapping = new RelationalMapping(pureClass, table, propertyMappings);
            mappingRegistry.register(mapping);
        }

        return this;
    }

    /**
     * Creates an M2MClassMapping from the parsed ClassMappingDefinition and
     * registers it.
     * This is called when addMapping encounters a "Pure" mapping type.
     */
    private void registerM2MClassMapping(String mappingName, MappingDefinition.ClassMappingDefinition classMapping) {
        // Convert m2mPropertyExpressions to M2MPropertyMappings
        List<org.finos.legend.pure.dsl.m2m.M2MPropertyMapping> propertyMappings = new java.util.ArrayList<>();

        if (classMapping.m2mPropertyExpressions() != null) {
            for (var entry : classMapping.m2mPropertyExpressions().entrySet()) {
                String propertyName = entry.getKey();
                String expressionString = entry.getValue();

                // Parse the expression string into an M2MExpression
                org.finos.legend.pure.dsl.m2m.M2MExpression expression = org.finos.legend.pure.dsl.m2m.M2MExpressionParser
                        .parse(expressionString);

                propertyMappings.add(new org.finos.legend.pure.dsl.m2m.M2MPropertyMapping(
                        propertyName, expression));
            }
        }

        // Parse filter expression if present
        org.finos.legend.pure.dsl.m2m.M2MExpression filter = null;
        if (classMapping.filterExpression() != null && !classMapping.filterExpression().isEmpty()) {
            filter = org.finos.legend.pure.dsl.m2m.M2MExpressionParser.parse(classMapping.filterExpression());
        }

        // Create M2MClassMapping
        org.finos.legend.pure.dsl.m2m.M2MClassMapping m2mMapping = new org.finos.legend.pure.dsl.m2m.M2MClassMapping(
                classMapping.className(),
                null, // tag
                classMapping.sourceClassName(),
                filter,
                propertyMappings);

        // Register with MappingRegistry
        mappingRegistry.registerM2M(m2mMapping);
    }

    /**
     * Adds a Service definition.
     * Services are used by the hosted service runtime for HTTP endpoints.
     */
    public PureModelBuilder addService(ServiceDefinition serviceDef) {
        services.put(serviceDef.qualifiedName(), serviceDef);
        services.put(serviceDef.simpleName(), serviceDef);
        return this;
    }

    /**
     * Adds an Enum definition.
     * Enums are type-safe value sets used for properties.
     */
    public PureModelBuilder addEnum(EnumDefinition enumDef) {
        enums.put(enumDef.qualifiedName(), enumDef);
        enums.put(enumDef.simpleName(), enumDef);
        return this;
    }

    /**
     * Looks up an enum definition by name (simple or qualified).
     * 
     * @param enumName The enum name to look up
     * @return The EnumDefinition, or null if not found
     */
    public EnumDefinition getEnum(String enumName) {
        return enums.get(enumName);
    }

    /**
     * @return The built mapping registry
     */
    public MappingRegistry getMappingRegistry() {
        return mappingRegistry;
    }

    /**
     * @param className The class name (simple or qualified)
     * @return The PureClass
     */
    public PureClass getClass(String className) {
        return classes.get(className);
    }

    /**
     * @param tableName The table name
     * @return The Table
     */
    public Table getTable(String tableName) {
        return tables.get(tableName);
    }

    /**
     * @param joinName The join name
     * @return The Join, if found
     */
    public Optional<Join> getJoin(String joinName) {
        return Optional.ofNullable(joins.get(joinName));
    }

    /**
     * @param associationName The association name
     * @return The Association, if found
     */
    public Optional<Association> getAssociation(String associationName) {
        return Optional.ofNullable(associations.get(associationName));
    }

    /**
     * @param connectionName The connection name (simple or qualified)
     * @return The ConnectionDefinition, or null if not found
     */
    public ConnectionDefinition getConnection(String connectionName) {
        return connections.get(connectionName);
    }

    /**
     * @param runtimeName The runtime name (simple or qualified)
     * @return The RuntimeDefinition, or null if not found
     */
    public RuntimeDefinition getRuntime(String runtimeName) {
        return runtimes.get(runtimeName);
    }

    /**
     * @param serviceName The service name (simple or qualified)
     * @return The ServiceDefinition, or null if not found
     */
    public ServiceDefinition getService(String serviceName) {
        return services.get(serviceName);
    }

    // ==================== ModelContext Implementation ====================

    @Override
    public Optional<RelationalMapping> findMapping(String className) {
        return mappingRegistry.findByClassName(className);
    }

    @Override
    public Optional<PureClass> findClass(String className) {
        return Optional.ofNullable(classes.get(className));
    }

    @Override
    public Optional<AssociationNavigation> findAssociationByProperty(String fromClassName, String propertyName) {
        // Search all associations for one that has this property navigating from the
        // given class
        for (Association assoc : associations.values()) {
            var prop1 = assoc.property1();
            var prop2 = assoc.property2();

            // Check if property1's name matches and property2's target is fromClassName
            // (property1 navigates FROM prop2.targetClass TO prop1.targetClass)
            if (prop1.propertyName().equals(propertyName) && prop2.targetClass().equals(fromClassName)) {
                boolean isToMany = prop1.multiplicity().isMany();
                Join join = findJoinForAssociationProperty(assoc.name(), propertyName);
                return Optional.of(new AssociationNavigation(assoc, prop2, prop1, isToMany, join));
            }

            // Check if property2's name matches and property1's target is fromClassName
            if (prop2.propertyName().equals(propertyName) && prop1.targetClass().equals(fromClassName)) {
                boolean isToMany = prop2.multiplicity().isMany();
                Join join = findJoinForAssociationProperty(assoc.name(), propertyName);
                return Optional.of(new AssociationNavigation(assoc, prop1, prop2, isToMany, join));
            }
        }
        return Optional.empty();
    }

    /**
     * Finds a join for an association property.
     * Checks explicit mappings first (from class mapping [DB]@JoinName syntax),
     * then falls back to convention-based matching.
     * 
     * @param associationName The association name
     * @param propertyName    The property name being navigated
     * @return The Join to use, or null if not found
     */
    private Join findJoinForAssociationProperty(String associationName, String propertyName) {
        // 1. Check explicit mapping from class mapping (ClassName.propertyName)
        // This is stored when parsing [DB]@JoinName in property mappings
        for (String key : explicitAssociationJoins.keySet()) {
            if (key.endsWith("." + propertyName)) {
                return explicitAssociationJoins.get(key);
            }
        }

        // 2. Check explicit mapping with association name (AssocName.propertyName)
        String simpleAssocName = associationName.contains("::")
                ? associationName.substring(associationName.lastIndexOf("::") + 2)
                : associationName;
        String key = simpleAssocName + "." + propertyName;
        Join explicitJoin = explicitAssociationJoins.get(key);
        if (explicitJoin != null) {
            return explicitJoin;
        }

        // 3. Fall back to convention-based matching (Join name = Association name)
        return findJoinForAssociation(associationName).orElse(null);
    }

    /**
     * Finds a join that matches an association name (convention-based).
     */
    private Optional<Join> findJoinForAssociation(String associationName) {
        // Try exact match first
        Join join = joins.get(associationName);
        if (join != null) {
            return Optional.of(join);
        }
        // Try without package prefix
        String simpleName = associationName.contains("::")
                ? associationName.substring(associationName.lastIndexOf("::") + 2)
                : associationName;
        return Optional.ofNullable(joins.get(simpleName));
    }

    @Override
    public Optional<Join> findJoin(String joinName) {
        return Optional.ofNullable(joins.get(joinName));
    }

    @Override
    public Optional<Table> findTable(String tableName) {
        return Optional.ofNullable(tables.get(tableName));
    }

    @Override
    public Optional<EnumDefinition> findEnum(String enumName) {
        return Optional.ofNullable(enums.get(enumName));
    }

    @Override
    public boolean hasEnumValue(String enumName, String valueName) {
        EnumDefinition enumDef = enums.get(enumName);
        return enumDef != null && enumDef.hasValue(valueName);
    }

    // ==================== Conversion Helpers ====================

    private Property convertProperty(ClassDefinition.PropertyDefinition propDef) {
        Type type = resolveType(propDef.type());
        Multiplicity multiplicity = new Multiplicity(propDef.lowerBound(), propDef.upperBound());
        return new Property(propDef.name(), type, multiplicity);
    }

    private Type resolveType(String typeName) {
        return switch (typeName) {
            case "String" -> PrimitiveType.STRING;
            case "Integer" -> PrimitiveType.INTEGER;
            case "Boolean" -> PrimitiveType.BOOLEAN;
            case "Date" -> PrimitiveType.DATE;
            case "StrictDate" -> PrimitiveType.STRICT_DATE;
            case "DateTime" -> PrimitiveType.DATE_TIME;
            case "Float" -> PrimitiveType.FLOAT;
            case "Decimal" -> PrimitiveType.DECIMAL;
            default -> {
                // Look up as class reference
                PureClass classType = classes.get(typeName);
                if (classType != null) {
                    yield classType;
                }
                // Look up as enum type
                EnumDefinition enumDef = enums.get(typeName);
                if (enumDef != null) {
                    yield new PureEnumType(enumDef);
                }
                throw new IllegalStateException("Unknown type: " + typeName);
            }
        };
    }

    private Table convertTable(DatabaseDefinition.TableDefinition tableDef) {
        List<Column> columns = tableDef.columns().stream()
                .map(this::convertColumn)
                .toList();
        return new Table(tableDef.name(), columns);
    }

    private Column convertColumn(DatabaseDefinition.ColumnDefinition colDef) {
        SqlDataType dataType = resolveSqlType(colDef.dataType());
        return new Column(colDef.name(), dataType, !colDef.notNull());
    }

    private SqlDataType resolveSqlType(String typeName) {
        String upper = typeName.toUpperCase();
        if (upper.startsWith("VARCHAR"))
            return SqlDataType.VARCHAR;
        if (upper.equals("INTEGER") || upper.equals("INT"))
            return SqlDataType.INTEGER;
        if (upper.equals("BIGINT"))
            return SqlDataType.BIGINT;
        if (upper.equals("BOOLEAN") || upper.equals("BOOL"))
            return SqlDataType.BOOLEAN;
        if (upper.equals("DATE"))
            return SqlDataType.DATE;
        if (upper.equals("TIMESTAMP"))
            return SqlDataType.TIMESTAMP;
        if (upper.equals("DOUBLE") || upper.equals("FLOAT"))
            return SqlDataType.DOUBLE;
        if (upper.equals("DECIMAL") || upper.startsWith("DECIMAL"))
            return SqlDataType.DECIMAL;
        if (upper.equals("SEMISTRUCTURED") || upper.equals("JSON") || upper.equals("VARIANT"))
            return SqlDataType.SEMISTRUCTURED;
        throw new IllegalArgumentException("Unknown SQL data type: '" + typeName + "'. Add it to SqlDataType enum.");
    }

    /**
     * Extracts column name from an expression like "[DB]
     * TABLE.COLUMN->cast(@Class)".
     */
    private String extractColumnNameFromExpression(String expression) {
        // Pattern: [DB] TABLE.COLUMN...
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
                "\\[\\w+\\]\\s+\\w+\\.(\\w+)");
        java.util.regex.Matcher matcher = pattern.matcher(expression);
        if (matcher.find()) {
            return matcher.group(1);
        }
        throw new IllegalArgumentException("Cannot extract column name from expression: " + expression);
    }
}
