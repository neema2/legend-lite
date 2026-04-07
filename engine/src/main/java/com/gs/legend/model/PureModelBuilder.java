package com.gs.legend.model;

import com.gs.legend.model.def.*;
import com.gs.legend.model.m3.*;
import com.gs.legend.model.mapping.ClassMapping;
import com.gs.legend.model.mapping.MappingRegistry;
import com.gs.legend.model.mapping.RelationalMapping;
import com.gs.legend.model.store.*;
import com.gs.legend.parser.PureParser;

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
    private final Map<String, View> views = new HashMap<>();
    private final Map<String, Filter> filters = new HashMap<>();
    private final Map<String, DatabaseDefinition> databases = new HashMap<>();
    private final Map<String, ProfileDefinition> profiles = new HashMap<>();
    private final Map<String, FunctionDefinition> functions = new HashMap<>();
    private final Map<String, ConnectionDefinition> connections = new HashMap<>();
    private final Map<String, RuntimeDefinition> runtimes = new HashMap<>();
    private final Map<String, ServiceDefinition> services = new HashMap<>();
    private final Map<String, EnumDefinition> enums = new HashMap<>();
    private final Map<String, MappingDefinition> mappingDefinitions = new HashMap<>();
    private final MappingRegistry mappingRegistry = new MappingRegistry();

    {
        // Register Pure platform enums — these are defined in legend-pure/legend-engine
        // and referenced by built-in function signatures (e.g., month()->Month[1]).
        // Without this, findEnum("Month") returns empty and the compiler can't resolve
        // enum return types or validate enum input params against the model.
        registerPlatformEnums();
    }

    private void registerPlatformEnums() {
        // Date enums (legend-pure: essential/date/_structures.pure)
        addEnum(EnumDefinition.of("meta::pure::functions::date::Month",
                "January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"));
        addEnum(EnumDefinition.of("meta::pure::functions::date::Quarter",
                "Q1", "Q2", "Q3", "Q4"));
        addEnum(EnumDefinition.of("meta::pure::functions::date::DayOfWeek",
                "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"));
        addEnum(EnumDefinition.of("meta::pure::functions::date::DurationUnit",
                "YEARS", "MONTHS", "WEEKS", "DAYS", "HOURS", "MINUTES",
                "SECONDS", "MILLISECONDS", "MICROSECONDS", "NANOSECONDS"));
        // Relation enums (legend-engine: relation/functions/transformation/)
        addEnum(EnumDefinition.of("meta::pure::functions::relation::JoinKind",
                "INNER", "LEFT", "RIGHT", "FULL"));
        addEnum(EnumDefinition.of("meta::pure::functions::relation::SortType",
                "ASC", "DESC"));
        // Hash enum (legend-engine: hash/hash.pure)
        addEnum(EnumDefinition.of("meta::pure::functions::hash::HashType",
                "MD5", "SHA1", "SHA256"));
    }

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
        List<PackageableElement> definitions = PureParser.parseModel(pureSource);

        // PHASE 0: Register all enums first (needed for type resolution in classes)
        for (PackageableElement def : definitions) {
            if (def instanceof EnumDefinition enumDef) {
                addEnum(enumDef);
            }
        }

        // PHASE 1a: Register class stubs (names only, no properties yet)
        // This allows forward references between classes (e.g., TypeForProjectTest → PrimitiveContainer)
        for (PackageableElement def : definitions) {
            if (def instanceof ClassDefinition classDef) {
                registerClassStub(classDef);
            }
        }

        // PHASE 1b: Resolve properties now that all class names are registered
        for (PackageableElement def : definitions) {
            if (def instanceof ClassDefinition classDef) {
                addClass(classDef);
            }
        }

        // PHASE 2: Resolve superclass references now that all classes are registered
        resolveSuperclasses();

        // PHASE 3: Process remaining definitions
        for (PackageableElement def : definitions) {
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

        // PHASE 4: Resolve database includes (merge tables/joins/views/filters from included DBs)
        resolveDatabaseIncludes();

        // PHASE 5: Resolve mapping includes (copy class mappings from included mappings)
        resolveMappingIncludes();

        return this;
    }

    /**
     * Resolves database include directives. For each database that includes another,
     * registers the included database's tables, joins, views, and filters under the
     * including database's namespace. Handles transitive includes.
     */
    private void resolveDatabaseIncludes() {
        for (var dbDef : databases.values().stream().distinct().toList()) {
            for (String includedPath : dbDef.includes()) {
                DatabaseDefinition included = databases.get(includedPath);
                if (included == null) continue;
                String dbName = dbDef.simpleName();

                // Merge tables
                for (var tableDef : included.tables()) {
                    Table table = convertTable(tableDef);
                    tables.putIfAbsent(dbName + "." + tableDef.name(), table);
                    tables.putIfAbsent(tableDef.name(), table);
                }
                for (var schema : included.schemas()) {
                    for (var tableDef : schema.tables()) {
                        Table table = convertTable(schema.name(), tableDef);
                        tables.putIfAbsent(dbName + "." + schema.name() + "." + tableDef.name(), table);
                        tables.putIfAbsent(schema.name() + "." + tableDef.name(), table);
                        tables.putIfAbsent(tableDef.name(), table);
                    }
                }

                // Merge joins
                for (var joinDef : included.joins()) {
                    Join join = new Join(joinDef.name(), joinDef.operation());
                    joins.putIfAbsent(dbName + "." + joinDef.name(), join);
                    joins.putIfAbsent(joinDef.name(), join);
                    mappingRegistry.registerJoin(join);
                }

                // Merge views
                for (var viewDef : included.views()) {
                    View view = convertView(viewDef);
                    views.putIfAbsent(dbName + "." + viewDef.name(), view);
                    views.putIfAbsent(viewDef.name(), view);
                }

                // Merge filters
                for (var filterDef : included.filters()) {
                    Filter filter = new Filter(filterDef.name(), filterDef.condition());
                    filters.putIfAbsent(dbName + "." + filterDef.name(), filter);
                    filters.putIfAbsent(filterDef.name(), filter);
                }
            }
        }
    }

    /**
     * Resolves mapping include directives. For each mapping that includes another,
     * copies the included mapping's class mappings into the including mapping's scope
     * in MappingRegistry.
     */
    private void resolveMappingIncludes() {
        for (var mappingDef : mappingDefinitions.values().stream().distinct().toList()) {
            for (var include : mappingDef.includes()) {
                String includedPath = include.includedMappingPath();
                // Look up included mapping's class mappings in the registry
                var includedMappings = mappingRegistry.getAllClassMappings(includedPath);
                if (includedMappings.isEmpty()) continue;

                // Register each included class mapping under the current mapping's scope
                for (var entry : includedMappings.entrySet()) {
                    var cm = entry.getValue();
                    if (cm instanceof RelationalMapping rm) {
                        mappingRegistry.register(mappingDef.qualifiedName(), rm);
                    } else if (cm instanceof com.gs.legend.model.mapping.PureClassMapping pcm) {
                        mappingRegistry.registerPureClassMapping(mappingDef.qualifiedName(), pcm);
                    }
                }
            }
        }
    }

    /**
     * Pre-registers a class by name (stub) so forward references can resolve.
     * Called before addClass() to handle mutual/forward dependencies.
     */
    private void registerClassStub(ClassDefinition classDef) {
        PureClass stub = new PureClass(
                classDef.packagePath(),
                classDef.simpleName(),
                List.of(), // no properties yet
                List.of(),
                classDef.stereotypes(),
                classDef.taggedValues());
        classes.put(classDef.qualifiedName(), stub);
        classes.put(classDef.simpleName(), stub);
    }

    /**
     * Adds a Class definition.
     * Resolves properties (all class names already registered via registerClassStub).
     * Superclass references are resolved later in resolveSuperclasses().
     */
    public PureModelBuilder addClass(ClassDefinition classDef) {
        // Store the definition for later superclass resolution
        pendingClassDefinitions.put(classDef.qualifiedName(), classDef);

        List<Property> properties = classDef.properties().stream()
                .map(this::convertProperty)
                .toList();

        // Create class with empty superclass list initially, but carry metadata
        PureClass pureClass = new PureClass(
                classDef.packagePath(),
                classDef.simpleName(),
                List.of(), // Superclasses resolved later
                properties,
                classDef.stereotypes(),
                classDef.taggedValues());

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

            // Create new PureClass with resolved superclasses (preserve metadata)
            PureClass existingClass = classes.get(qualifiedName);
            PureClass updatedClass = new PureClass(
                    existingClass.packagePath(),
                    existingClass.name(),
                    resolvedSuperClasses,
                    existingClass.properties(),
                    existingClass.stereotypes(),
                    existingClass.taggedValues());

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

        // Register schema tables (schema-qualified and simple name)
        for (var schema : dbDef.schemas()) {
            for (var tableDef : schema.tables()) {
                Table table = convertTable(schema.name(), tableDef);
                tables.put(tableDef.name(), table);
                tables.put(schema.name() + "." + tableDef.name(), table);
                tables.put(dbDef.simpleName() + "." + schema.name() + "." + tableDef.name(), table);
            }
        }

        // Register joins
        for (var joinDef : dbDef.joins()) {
            Join join = new Join(joinDef.name(), joinDef.operation());
            joins.put(joinDef.name(), join);
            joins.put(dbDef.simpleName() + "." + joinDef.name(), join);

            // Also register with MappingRegistry for M2M deep fetch lookup
            mappingRegistry.registerJoin(join);
        }

        // Register views
        for (var viewDef : dbDef.views()) {
            View view = convertView(viewDef);
            views.put(viewDef.name(), view);
            views.put(dbDef.simpleName() + "." + viewDef.name(), view);
        }

        // Register filters
        for (var filterDef : dbDef.filters()) {
            Filter filter = new Filter(filterDef.name(), filterDef.condition());
            filters.put(filterDef.name(), filter);
            filters.put(dbDef.simpleName() + "." + filterDef.name(), filter);
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
        // Store for include resolution
        mappingDefinitions.put(mappingDef.qualifiedName(), mappingDef);
        mappingDefinitions.put(mappingDef.simpleName(), mappingDef);

        // Process association mappings: register join associations
        for (var assocMapping : mappingDef.associationMappings()) {
            for (var prop : assocMapping.properties()) {
                if (!prop.joinChain().isEmpty()) {
                    String joinName = prop.joinChain().get(0).joinName();
                    Join join = joins.get(joinName);
                    if (join != null) {
                        String key = assocMapping.associationName() + "." + prop.propertyName();
                        explicitAssociationJoins.put(key, join);
                        // Also register with simple association name
                        String simpleName = assocMapping.associationName().contains("::")
                                ? assocMapping.associationName().substring(
                                        assocMapping.associationName().lastIndexOf("::") + 2)
                                : assocMapping.associationName();
                        explicitAssociationJoins.put(simpleName + "." + prop.propertyName(), join);
                    }
                }
            }
        }

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

            // Find the Table (or View → infer base table)
            Table table = null;
            View view = null;
            if (classMapping.mainTable() != null) {
                String tableName = classMapping.mainTable().tableName();
                table = tables.get(tableName);
                if (table == null) {
                    // Views are usable as tables in mappings (same as legend-engine)
                    view = views.get(tableName);
                    if (view != null) {
                        String baseTableName = inferViewMainTable(view);
                        table = tables.get(baseTableName);
                        if (table == null) {
                            throw new IllegalStateException(
                                    "View '" + tableName + "' references table '" + baseTableName + "' which was not found");
                        }
                    }
                }
                if (table == null) {
                    throw new IllegalStateException("Table or View not found: " + tableName);
                }
            }

            // Build property mappings:
            // - Bare join references (no terminal column) are skipped — handled as associations
            // - Join references WITH terminal columns become join-chain PropertyMappings
            // - Embedded/inline PMDs are skipped — handled separately below
            List<PropertyMapping> propertyMappings = classMapping.propertyMappings().stream()
                    .filter(pm -> pm.structuredValue() == null)
                    .filter(pm -> !pm.isJoinReference()
                            || pm.joinReference().terminalColumn() != null)
                    .map(pm -> {
                        // Join chain property mapping: @J1 > @J2 | T.COL
                        if (pm.isJoinReference() && pm.joinReference().terminalColumn() != null) {
                            var joinRef = pm.joinReference();
                            var terminal = joinRef.terminalColumn();
                            String terminalCol;
                            if (terminal instanceof com.gs.legend.model.def.RelationalOperation.ColumnRef cr) {
                                terminalCol = cr.column();
                            } else {
                                terminalCol = pm.propertyName();
                            }
                            List<String> joinNames = joinRef.joinChain().stream()
                                    .map(com.gs.legend.model.def.JoinChainElement::joinName)
                                    .toList();
                            return PropertyMapping.joinChain(pm.propertyName(), terminalCol, joinNames);
                        }
                        if (pm.hasMappingExpression()) {
                            // DynaFunction expression: convert RelationalOperation → ValueSpecification
                            var joinNavs = findAllJoinNavigations(pm.mappingExpression());
                            if (joinNavs.size() >= 2 && classMapping.mainTable() != null) {
                                // Multi-join DynaFunction: build tableToParam for all referenced tables
                                String mainTableName = classMapping.mainTable().tableName();
                                var tableToParam = new java.util.HashMap<String, String>();
                                tableToParam.put(mainTableName, "src");
                                var allJoinChains = new java.util.ArrayList<java.util.List<String>>();
                                for (int ji = 0; ji < joinNavs.size(); ji++) {
                                    var nav = joinNavs.get(ji);
                                    String paramName = "t" + (ji + 1);
                                    var termTables = RelationalMappingConverter.collectTableNames(nav.terminal());
                                    String termTable = termTables.stream()
                                            .filter(t -> !t.equals(mainTableName))
                                            .findFirst().orElse(termTables.iterator().next());
                                    tableToParam.put(termTable, paramName);
                                    allJoinChains.add(nav.joinChain().stream()
                                            .map(com.gs.legend.model.def.JoinChainElement::joinName).toList());
                                }
                                var vsExpr = RelationalMappingConverter.convert(pm.mappingExpression(), tableToParam);
                                return PropertyMapping.dynaFunctionWithMultiJoin(pm.propertyName(), vsExpr, allJoinChains);
                            }
                            var joinNav = joinNavs.isEmpty() ? null : joinNavs.get(0);
                            if (joinNav != null && classMapping.mainTable() != null) {
                                // Combined join + DynaFunction: expression references columns across tables
                                String mainTableName = classMapping.mainTable().tableName();
                                // Terminal table from JoinNavigation's terminal ColumnRef
                                var terminalTables = RelationalMappingConverter.collectTableNames(joinNav.terminal());
                                String terminalTable = terminalTables.stream()
                                        .filter(t -> !t.equals(mainTableName))
                                        .findFirst().orElse(terminalTables.iterator().next());
                                var tableToParam = new java.util.HashMap<String, String>();
                                tableToParam.put(mainTableName, "src");
                                tableToParam.put(terminalTable, "tgt");
                                var vsExpr = RelationalMappingConverter.convert(pm.mappingExpression(), tableToParam);
                                List<String> joinNames = joinNav.joinChain().stream()
                                        .map(com.gs.legend.model.def.JoinChainElement::joinName).toList();
                                return PropertyMapping.dynaFunctionWithJoin(pm.propertyName(), vsExpr, joinNames);
                            }
                            var vsExpr = RelationalMappingConverter.convert(pm.mappingExpression());
                            return PropertyMapping.dynaFunction(pm.propertyName(), vsExpr);
                        }
                        if (pm.isExpression()) {
                            // Expression-based mapping (e.g., PAYLOAD->get('price', @Integer))
                            String colName = extractColumnNameFromExpression(pm.expressionString());
                            String expr = pm.expressionString();

                            // Auto-infer cast type from class property when get() lacks @Type
                            var prop = pureClass.findProperty(pm.propertyName());
                            if (prop.isPresent() && expr.matches(".*->get\\('[^']+?'\\)\\s*$")) {
                                String pureType = prop.get().genericType().typeName();
                                if (!"Any".equals(pureType) && !"Variant".equals(pureType)) {
                                    expr = expr.replaceFirst(
                                            "->get\\('([^']+?)'\\)\\s*$",
                                            "->get('$1', @" + pureType + ")");
                                }
                            }
                            return PropertyMapping.expression(pm.propertyName(), colName, expr);
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

            // Extract mapping-level directives
            String setId = classMapping.setId();
            boolean isRoot = classMapping.isRoot();
            boolean distinct = classMapping.distinct();
            String filterName = classMapping.filter() != null ? classMapping.filter().filterName() : null;
            String filterDbName = classMapping.filter() != null ? classMapping.filter().databaseName() : null;

            // Build embedded property mappings from structured PMDs
            var embeddedMappings = new java.util.LinkedHashMap<String, List<PropertyMapping>>();
            for (var pm : classMapping.propertyMappings()) {
                if (pm.structuredValue() instanceof com.gs.legend.model.def.PropertyMappingValue.EmbeddedMapping emb) {
                    var subMappings = new java.util.ArrayList<PropertyMapping>();
                    for (var sub : emb.properties()) {
                        if (sub.columnReference() != null) {
                            subMappings.add(PropertyMapping.column(sub.propertyName(),
                                    sub.columnReference().columnName()));
                        }
                    }
                    embeddedMappings.put(pm.propertyName(), subMappings);
                } else if (pm.structuredValue() instanceof com.gs.legend.model.def.PropertyMappingValue.InlineMapping inl) {
                    // Inline: resolve target set ID → use its property mappings as embedded
                    var targetDef = mappingDef.findClassMappingBySetId(inl.targetSetId());
                    if (targetDef.isEmpty()) {
                        throw new IllegalStateException(
                                "Inline mapping target set ID '" + inl.targetSetId() + "' not found");
                    }
                    var subMappings = new java.util.ArrayList<PropertyMapping>();
                    for (var sub : targetDef.get().propertyMappings()) {
                        if (sub.columnReference() != null) {
                            subMappings.add(PropertyMapping.column(sub.propertyName(),
                                    sub.columnReference().columnName()));
                        }
                    }
                    embeddedMappings.put(pm.propertyName(), subMappings);
                } else if (pm.structuredValue() instanceof com.gs.legend.model.def.PropertyMappingValue.OtherwiseMapping ow) {
                    // Otherwise: embedded sub-mappings + fallback join
                    var subMappings = new java.util.ArrayList<PropertyMapping>();
                    for (var sub : ow.embedded().properties()) {
                        if (sub.columnReference() != null) {
                            subMappings.add(PropertyMapping.column(sub.propertyName(),
                                    sub.columnReference().columnName()));
                        }
                    }
                    embeddedMappings.put(pm.propertyName(), subMappings);
                    // Register fallback join as explicit association join
                    String joinName = ow.fallbackJoin().joinChain().get(0).joinName();
                    Join join = joins.get(joinName);
                    if (join != null) {
                        String key = classMapping.className() + "." + pm.propertyName();
                        explicitAssociationJoins.put(key, join);
                    }
                }
            }

            // Extract ~groupBy column names
            List<String> groupByColumns = classMapping.groupBy().stream()
                    .filter(op -> op instanceof com.gs.legend.model.def.RelationalOperation.ColumnRef)
                    .map(op -> ((com.gs.legend.model.def.RelationalOperation.ColumnRef) op).column())
                    .toList();

            // Create and register the mapping
            RelationalMapping mapping = new RelationalMapping(pureClass, table, propertyMappings,
                    false, setId, isRoot, distinct, filterName, filterDbName, embeddedMappings,
                    groupByColumns, view);
            mappingRegistry.register(mappingDef.qualifiedName(), mapping);
        }

        return this;
    }

    /**
     * Creates M2M class mappings from the parsed ClassMappingDefinition and
     * registers them.
     * This is called when addMapping encounters a "Pure" mapping type.
     * 
     * Registers BOTH old M2MClassMapping (for backward compat) and new
     * PureClassMapping (for clean pipeline). Phase 4 will remove old one.
     */
    private void registerM2MClassMapping(String mappingName, MappingDefinition.ClassMappingDefinition classMapping) {
        // === NEW: Parse M2M expressions via ValueSpecificationBuilder → PureClassMapping ===
        java.util.Map<String, com.gs.legend.ast.ValueSpecification> parsedExpressions = new java.util.LinkedHashMap<>();

        if (classMapping.m2mPropertyExpressions() != null) {
            for (var entry : classMapping.m2mPropertyExpressions().entrySet()) {
                String propertyName = entry.getKey();
                String expressionString = entry.getValue();
                parsedExpressions.put(propertyName,
                        com.gs.legend.parser.PureParser.parseQuery(expressionString));
            }
        }

        // Parse filter via ValueSpecificationBuilder
        com.gs.legend.ast.ValueSpecification parsedFilter = null;
        if (classMapping.filterExpression() != null && !classMapping.filterExpression().isEmpty()) {
            parsedFilter = com.gs.legend.parser.PureParser.parseQuery(classMapping.filterExpression());
        }

        // Register new PureClassMapping (clean pipeline)
        com.gs.legend.model.mapping.PureClassMapping pureMapping =
                new com.gs.legend.model.mapping.PureClassMapping(
                        classMapping.className(),
                        classMapping.sourceClassName(),
                        parsedExpressions,
                        parsedFilter,
                        null,  // targetClass — resolved later
                        null); // sourceMapping — resolved later
        mappingRegistry.registerPureClassMapping(mappingName, pureMapping);
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
     * @param filterName The filter name (simple or qualified with db prefix)
     * @return The Filter, if found
     */
    public Optional<Filter> getFilter(String filterName) {
        return Optional.ofNullable(filters.get(filterName));
    }

    /**
     * Resolves all mapping names from a runtime definition.
     *
     * @param runtimeName The runtime name
     * @return The list of mapping names, or empty if no mappings defined
     */
    public java.util.List<String> resolveMappingNames(String runtimeName) {
        RuntimeDefinition runtime = runtimes.get(runtimeName);
        if (runtime == null || runtime.mappings() == null || runtime.mappings().isEmpty()) {
            return java.util.List.of();
        }
        return runtime.mappings();
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
     * Resolves a live JDBC Connection from a Runtime name.
     * Looks up Runtime → Connection binding → ConnectionDefinition → JDBC Connection.
     *
     * @param runtimeName The runtime name (simple or qualified)
     * @return A live JDBC Connection
     * @throws java.sql.SQLException If connection cannot be established
     */
    public java.sql.Connection resolveConnection(String runtimeName) throws java.sql.SQLException {
        RuntimeDefinition runtime = runtimes.get(runtimeName);
        if (runtime == null) {
            throw new IllegalArgumentException("Runtime not found: " + runtimeName);
        }
        String storeRef = runtime.connectionBindings().keySet().iterator().next();
        String connectionRef = runtime.connectionBindings().get(storeRef);
        ConnectionDefinition def = connections.get(connectionRef);
        if (def == null) {
            throw new IllegalArgumentException("Connection not found: " + connectionRef);
        }
        return com.gs.legend.exec.ConnectionResolver.resolve(def);
    }

    /**
     * Resolves the SQL dialect from a Runtime name.
     * Looks up Runtime → Connection binding → ConnectionDefinition → dialect().
     * Does NOT require a live JDBC connection — works from model metadata alone.
     *
     * @param runtimeName The runtime name (simple or qualified)
     * @return The SQLDialect for this runtime's connection
     */
    public com.gs.legend.sqlgen.SQLDialect resolveDialect(String runtimeName) {
        RuntimeDefinition runtime = runtimes.get(runtimeName);
        if (runtime == null) {
            throw new IllegalArgumentException("Runtime not found: " + runtimeName);
        }
        String storeRef = runtime.connectionBindings().keySet().iterator().next();
        String connectionRef = runtime.connectionBindings().get(storeRef);
        ConnectionDefinition def = connections.get(connectionRef);
        if (def == null) {
            throw new IllegalArgumentException("Connection not found: " + connectionRef);
        }
        return com.gs.legend.exec.ConnectionResolver.dialectFor(def);
    }

    /**
     * @param serviceName The service name (simple or qualified)
     * @return The ServiceDefinition, or null if not found
     */
    public ServiceDefinition getService(String serviceName) {
        return services.get(serviceName);
    }

    // ==================== Bulk Accessors (for NLQ indexing) ====================

    /**
     * Returns all registered classes, deduplicated by qualified name.
     * Classes registered by both simple and qualified name are included once.
     */
    public Map<String, PureClass> getAllClasses() {
        Map<String, PureClass> result = new HashMap<>();
        for (var entry : classes.entrySet()) {
            PureClass pc = entry.getValue();
            result.putIfAbsent(pc.qualifiedName(), pc);
        }
        return result;
    }

    /**
     * Returns all registered associations, deduplicated by qualified name.
     */
    public Map<String, Association> getAllAssociations() {
        Map<String, Association> result = new HashMap<>();
        for (var entry : associations.entrySet()) {
            Association a = entry.getValue();
            result.putIfAbsent(a.qualifiedName(), a);
        }
        return result;
    }

    /**
     * Returns all registered enums, deduplicated by qualified name.
     */
    public Map<String, EnumDefinition> getAllEnums() {
        Map<String, EnumDefinition> result = new HashMap<>();
        for (var entry : enums.entrySet()) {
            EnumDefinition e = entry.getValue();
            result.putIfAbsent(e.qualifiedName(), e);
        }
        return result;
    }

    // ==================== ModelContext Implementation ====================

    public Optional<ClassMapping> findMapping(String className) {
        // Try relational mapping first, then M2M
        var relOpt = mappingRegistry.findByClassName(className);
        if (relOpt.isPresent()) return Optional.of(relOpt.get());
        var pureOpt = mappingRegistry.findPureClassMapping(className);
        if (pureOpt.isPresent()) return Optional.of(pureOpt.get());
        return Optional.empty();
    }

    @Override
    public Optional<MappingExpression> findMappingExpression(String className) {
        return mappingRegistry.findPureClassMapping(className)
                .map(pcm -> new MappingExpression.M2M(pcm.sourceClassName(),
                        pcm.propertyExpressions(), pcm.filter()));
    }

    @Override
    public Optional<PureClass> findClass(String className) {
        return Optional.ofNullable(classes.get(className));
    }

    /**
     * Merges external class definitions into this model.
     * Used to add classes extracted from external sources (e.g., PCT interpreter metadata).
     * Does not overwrite existing classes from parsed Pure source.
     */
    public void addClasses(java.util.Map<String, PureClass> externalClasses) {
        if (externalClasses != null) {
            for (var entry : externalClasses.entrySet()) {
                classes.putIfAbsent(entry.getKey(), entry.getValue());
                // Also register by simple name for unqualified lookups
                String simpleName = entry.getValue().name();
                if (simpleName != null && !simpleName.equals(entry.getKey())) {
                    classes.putIfAbsent(simpleName, entry.getValue());
                }
            }
        }
    }

    /**
     * Returns all property→Join mappings for a class from explicit mapping references.
     * Includes both relational class mapping {@code @JoinName} and association mapping references.
     * Key: property name, Value: Join.
     *
     * <p>This is the mapping-level source of truth for which properties use which joins,
     * independent of whether the property comes from an Association or is declared directly on the class.
     */
    public java.util.Map<String, Join> findAllPropertyJoins(String className) {
        var result = new java.util.LinkedHashMap<String, Join>();
        String prefix = className + ".";
        for (var entry : explicitAssociationJoins.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                String propName = entry.getKey().substring(prefix.length());
                result.putIfAbsent(propName, entry.getValue());
            }
        }
        // Also check simple class name
        String simpleName = className.contains("::")
                ? className.substring(className.lastIndexOf("::") + 2) : className;
        if (!simpleName.equals(className)) {
            String simplePrefix = simpleName + ".";
            for (var entry : explicitAssociationJoins.entrySet()) {
                if (entry.getKey().startsWith(simplePrefix)) {
                    String propName = entry.getKey().substring(simplePrefix.length());
                    result.putIfAbsent(propName, entry.getValue());
                }
            }
        }
        return result;
    }

    /**
     * Full association navigation info for physical layers (MappingNormalizer).
     * NOT on ModelContext — only accessible via PureModelBuilder directly.
     */
    public record FullAssociationNavigation(
            String targetClassName,
            boolean isToMany,
            Join join) {}

    /**
     * Returns all association navigations for a given class, with full physical info.
     * Called by MappingNormalizer directly (not through ModelContext).
     */
    public java.util.Map<String, FullAssociationNavigation> findAllAssociationNavigationsFull_impl(String className) {
        var result = new java.util.LinkedHashMap<String, FullAssociationNavigation>();
        for (Association assoc : associations.values()) {
            var prop1 = assoc.property1();
            var prop2 = assoc.property2();

            if (classNameMatches(prop2.targetClass(), className)) {
                boolean isToMany = prop1.multiplicity().isMany();
                Join join = findJoinForAssociationProperty(assoc.name(), prop1.propertyName());
                result.putIfAbsent(prop1.propertyName(),
                        new FullAssociationNavigation(prop1.targetClass(), isToMany, join));
            }
            if (classNameMatches(prop1.targetClass(), className)) {
                boolean isToMany = prop2.multiplicity().isMany();
                Join join = findJoinForAssociationProperty(assoc.name(), prop2.propertyName());
                result.putIfAbsent(prop2.propertyName(),
                        new FullAssociationNavigation(prop2.targetClass(), isToMany, join));
            }
        }
        return result;
    }

    /**
     * Matches class names flexibly: "test::H5" matches both "test::H5" and "H5".
     * Handles the case where associations store qualified names but lookup uses simple names or vice versa.
     */
    private static boolean classNameMatches(String assocClassName, String lookupName) {
        if (assocClassName.equals(lookupName)) return true;
        // "test::H5".endsWith("::H5") matches lookupName "H5"
        if (assocClassName.endsWith("::" + lookupName)) return true;
        // lookupName "test::H5".endsWith("::H5") matches assocClassName "H5"
        if (lookupName.endsWith("::" + assocClassName)) return true;
        return false;
    }

    @Override
    public Map<String, AssociationNavigation> findAllAssociationNavigations(String className) {
        var result = new java.util.LinkedHashMap<String, AssociationNavigation>();
        for (var entry : findAllAssociationNavigationsFull(className).entrySet()) {
            var nav = entry.getValue();
            result.put(entry.getKey(), new AssociationNavigation(nav.targetClassName(), nav.isToMany()));
        }
        return result;
    }

    /**
     * Returns all association navigations with full physical info (including Join).
     * Called by MappingNormalizer directly (not through ModelContext).
     */
    public Map<String, FullAssociationNavigation> findAllAssociationNavigationsFull(String className) {
        return findAllAssociationNavigationsFull_impl(className);
    }

    @Override
    public Optional<AssociationNavigation> findAssociationByProperty(String fromClassName, String propertyName) {
        // Search all associations for one that has this property navigating from the
        // given class. Returns lightweight AssociationNavigation for TypeChecker.
        for (Association assoc : associations.values()) {
            var prop1 = assoc.property1();
            var prop2 = assoc.property2();

            // Check if property1's name matches and property2's target is fromClassName
            // (property1 navigates FROM prop2.targetClass TO prop1.targetClass)
            if (prop1.propertyName().equals(propertyName) && classNameMatches(prop2.targetClass(), fromClassName)) {
                boolean isToMany = prop1.multiplicity().isMany();
                return Optional.of(new AssociationNavigation(prop1.targetClass(), isToMany));
            }

            // Check if property2's name matches and property1's target is fromClassName
            if (prop2.propertyName().equals(propertyName) && classNameMatches(prop1.targetClass(), fromClassName)) {
                boolean isToMany = prop2.multiplicity().isMany();
                return Optional.of(new AssociationNavigation(prop2.targetClass(), isToMany));
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
        return new Property(propDef.name(), type, multiplicity, propDef.taggedValues());
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
        return convertTable("", tableDef);
    }

    private Table convertTable(String schema, DatabaseDefinition.TableDefinition tableDef) {
        List<Column> columns = tableDef.columns().stream()
                .map(this::convertColumn)
                .toList();
        return new Table(schema, tableDef.name(), columns);
    }

    /**
     * Infers the view's main table by scanning ALL non-join column expressions for ColumnRefs.
     * Collects distinct table names and errors if 0 or >1 (matches legend-engine's identifyMainTable).
     */
    private String inferViewMainTable(View view) {
        var tableNames = new java.util.LinkedHashSet<String>();
        for (var col : view.columnMappings()) {
            var expr = col.expression();
            // Skip columns that reference joined tables (direct JoinNavigation or DynaFunc containing joins)
            if (expr instanceof com.gs.legend.model.def.RelationalOperation.JoinNavigation) {
                continue;
            }
            if (!findAllJoinNavigations(expr).isEmpty()) {
                continue;
            }
            tableNames.addAll(RelationalMappingConverter.collectTableNames(expr));
        }
        if (tableNames.isEmpty()) {
            throw new IllegalStateException(
                    "View '" + view.name() + "': cannot infer main table — no column references found");
        }
        if (tableNames.size() > 1) {
            throw new IllegalStateException(
                    "View '" + view.name() + "' references multiple tables " + tableNames
                    + " — there should be only one root table for views");
        }
        return tableNames.iterator().next();
    }

    private View convertView(DatabaseDefinition.ViewDefinition viewDef) {
        List<View.ViewColumn> viewColumns = viewDef.columnMappings().stream()
                .map(vc -> new View.ViewColumn(vc.name(), vc.expression(), vc.primaryKey()))
                .toList();
        return new View(viewDef.name(), viewDef.filterMapping(),
                viewDef.groupByColumns(), viewDef.distinct(), viewColumns);
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
     * Finds ALL {@link com.gs.legend.model.def.RelationalOperation.JoinNavigation} nodes in a RelationalOperation tree.
     * Returns them in encounter order (left-to-right, depth-first).
     */
    public static java.util.List<com.gs.legend.model.def.RelationalOperation.JoinNavigation> findAllJoinNavigations(
            com.gs.legend.model.def.RelationalOperation op) {
        var result = new java.util.ArrayList<com.gs.legend.model.def.RelationalOperation.JoinNavigation>();
        collectJoinNavigations(op, result);
        return result;
    }

    private static void collectJoinNavigations(
            com.gs.legend.model.def.RelationalOperation op,
            java.util.List<com.gs.legend.model.def.RelationalOperation.JoinNavigation> out) {
        switch (op) {
            case com.gs.legend.model.def.RelationalOperation.JoinNavigation nav -> out.add(nav);
            case com.gs.legend.model.def.RelationalOperation.FunctionCall func ->
                    func.args().forEach(a -> collectJoinNavigations(a, out));
            case com.gs.legend.model.def.RelationalOperation.Comparison cmp -> {
                collectJoinNavigations(cmp.left(), out);
                collectJoinNavigations(cmp.right(), out);
            }
            case com.gs.legend.model.def.RelationalOperation.BooleanOp bool -> {
                collectJoinNavigations(bool.left(), out);
                collectJoinNavigations(bool.right(), out);
            }
            case com.gs.legend.model.def.RelationalOperation.Group g -> collectJoinNavigations(g.inner(), out);
            case com.gs.legend.model.def.RelationalOperation.IsNull n -> collectJoinNavigations(n.operand(), out);
            case com.gs.legend.model.def.RelationalOperation.IsNotNull n -> collectJoinNavigations(n.operand(), out);
            default -> { }
        }
    }

    /**
     * Extracts column name from an expression like "[DB] TABLE.COLUMN->cast(@Class)".
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
