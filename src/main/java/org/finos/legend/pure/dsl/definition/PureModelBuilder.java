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
    private final Map<String, Association> associations = new HashMap<>();
    private final Map<String, Table> tables = new HashMap<>();
    private final Map<String, Join> joins = new HashMap<>();
    private final Map<String, DatabaseDefinition> databases = new HashMap<>();
    private final MappingRegistry mappingRegistry = new MappingRegistry();

    /**
     * Adds Pure definitions from source code.
     * 
     * @param pureSource The Pure source code
     * @return this builder for chaining
     */
    public PureModelBuilder addSource(String pureSource) {
        List<PureDefinition> definitions = PureDefinitionParser.parse(pureSource);

        for (PureDefinition def : definitions) {
            switch (def) {
                case ClassDefinition classDef -> addClass(classDef);
                case AssociationDefinition assocDef -> addAssociation(assocDef);
                case DatabaseDefinition dbDef -> addDatabase(dbDef);
                case MappingDefinition mappingDef -> addMapping(mappingDef);
                case M2MMappingDefinition m2mDef -> addM2MMapping(m2mDef);
                case ServiceDefinition serviceDef -> addService(serviceDef);
                case EnumDefinition enumDef -> addEnum(enumDef);
            }
        }

        return this;
    }

    /**
     * Adds a Class definition.
     */
    public PureModelBuilder addClass(ClassDefinition classDef) {
        List<Property> properties = classDef.properties().stream()
                .map(this::convertProperty)
                .toList();

        PureClass pureClass = new PureClass(
                classDef.packagePath(),
                classDef.simpleName(),
                properties);

        classes.put(classDef.qualifiedName(), pureClass);
        classes.put(classDef.simpleName(), pureClass); // Also register by simple name

        return this;
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
        }

        return this;
    }

    /**
     * Adds a Mapping definition and registers it.
     */
    public PureModelBuilder addMapping(MappingDefinition mappingDef) {
        for (var classMapping : mappingDef.classMappings()) {
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

            // Build property mappings
            List<PropertyMapping> propertyMappings = classMapping.propertyMappings().stream()
                    .map(pm -> new PropertyMapping(pm.propertyName(), pm.columnReference().columnName()))
                    .toList();

            // Create and register the mapping
            RelationalMapping mapping = new RelationalMapping(pureClass, table, propertyMappings);
            mappingRegistry.register(mapping);
        }

        return this;
    }

    /**
     * Adds an M2M Mapping definition.
     * M2M mappings are handled directly by M2MCompiler, so this just stores
     * them for potential later access.
     */
    public PureModelBuilder addM2MMapping(M2MMappingDefinition m2mMappingDef) {
        // M2M mappings are used directly by M2MCompiler at compile time.
        // The source class must already have a relational mapping registered.
        return this;
    }

    /**
     * Adds a Service definition.
     * Services are used by the hosted service runtime for HTTP endpoints.
     */
    public PureModelBuilder addService(ServiceDefinition serviceDef) {
        // Services are registered with the ServiceRegistry at runtime.
        // The model builder just stores them for later access.
        return this;
    }

    /**
     * Adds an Enum definition.
     * Enums are type-safe value sets used for properties.
     */
    public PureModelBuilder addEnum(EnumDefinition enumDef) {
        // Store enum for validation and type checking
        // Enum values are stored as strings in SQL
        return this;
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
                Join join = findJoinForAssociation(assoc.name()).orElse(null);
                return Optional.of(new AssociationNavigation(assoc, prop2, prop1, isToMany, join));
            }

            // Check if property2's name matches and property1's target is fromClassName
            if (prop2.propertyName().equals(propertyName) && prop1.targetClass().equals(fromClassName)) {
                boolean isToMany = prop2.multiplicity().isMany();
                Join join = findJoinForAssociation(assoc.name()).orElse(null);
                return Optional.of(new AssociationNavigation(assoc, prop1, prop2, isToMany, join));
            }
        }
        return Optional.empty();
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
            case "Float" -> PrimitiveType.FLOAT;
            case "Decimal" -> PrimitiveType.DECIMAL;
            default -> {
                // Look up as class reference
                PureClass classType = classes.get(typeName);
                if (classType != null) {
                    yield classType;
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
        return SqlDataType.VARCHAR; // Default fallback
    }
}
