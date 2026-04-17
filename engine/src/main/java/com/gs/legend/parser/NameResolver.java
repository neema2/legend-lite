package com.gs.legend.parser;

import com.gs.legend.ast.*;
import com.gs.legend.model.def.*;
import com.gs.legend.model.def.ClassDefinition.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Name resolution pass — resolves simple names to FQN using imports.
 *
 * <p>Sits between parsing and model building in the compiler pipeline:
 * <pre>
 * Parser → NameResolver → PureModelBuilder / TypeChecker
 * </pre>
 *
 * <p>Resolution rule (matches legend-engine CompileContext.resolve):
 * <ul>
 *   <li>Contains {@code ::} → already FQN, return as-is</li>
 *   <li>Simple name → try each wildcard import ({@code pkg::name}), check against {@code knownFqns}</li>
 *   <li>0 matches → return as-is (may be a primitive like String, Integer)</li>
 *   <li>1 match → return it</li>
 *   <li>&gt;1 matches → ambiguity error</li>
 * </ul>
 */
public final class NameResolver {

    private NameResolver() {}

    /**
     * Resolves all simple name references in model definitions to FQN.
     *
     * <p>Walks each definition record and resolves name fields:
     * <ul>
     *   <li>{@link ClassDefinition}: superclass names, property types, derived property types</li>
     *   <li>{@link AssociationDefinition}: target class names on each end</li>
     *   <li>{@link MappingDefinition}: className and sourceClassName in class mappings</li>
     * </ul>
     *
     * <p>Definition types with only FQN qualifiedName (Database, Runtime, Service, etc.)
     * pass through unchanged — their internal references use element names that are
     * already scoped (e.g., table names, join names).
     *
     * @param definitions The raw definitions from the parser
     * @param imports     The import scope extracted from the same source
     * @param knownFqns   All registered FQN element names
     * @return New list with resolved definitions (originals unchanged if already FQN)
     */
    public static List<PackageableElement> resolveDefinitions(
            List<PackageableElement> definitions, ImportScope imports, Set<String> knownFqns) {

        List<PackageableElement> resolved = new ArrayList<>(definitions.size());
        for (PackageableElement def : definitions) {
            resolved.add(resolveDefinition(def, imports, knownFqns));
        }
        return resolved;
    }

    private static PackageableElement resolveDefinition(
            PackageableElement def, ImportScope imports, Set<String> knownFqns) {
        return switch (def) {
            case ClassDefinition classDef -> resolveClass(classDef, imports, knownFqns);
            case AssociationDefinition assocDef -> resolveAssociation(assocDef, imports, knownFqns);
            case MappingDefinition mappingDef -> resolveMapping(mappingDef, imports, knownFqns);
            case FunctionDefinition funcDef -> resolveFunction(funcDef, imports, knownFqns);
            case DatabaseDefinition dbDef -> resolveDatabase(dbDef, imports, knownFqns);
            case RuntimeDefinition rtDef -> resolveRuntime(rtDef, imports, knownFqns);
            case ServiceDefinition svcDef -> resolveService(svcDef, imports, knownFqns);
            case ConnectionDefinition connDef -> resolveConnection(connDef, imports, knownFqns);
            default -> def; // EnumDefinition, ProfileDefinition — no cross-project refs
        };
    }

    // ==================== ClassDefinition ====================

    private static ClassDefinition resolveClass(
            ClassDefinition classDef, ImportScope imports, Set<String> knownFqns) {

        // Resolve superclass names
        List<String> resolvedSuperClasses = classDef.superClasses().stream()
                .map(s -> imports.resolve(s, knownFqns))
                .toList();

        // Resolve property types
        List<PropertyDefinition> resolvedProps = classDef.properties().stream()
                .map(p -> resolveProperty(p, imports, knownFqns))
                .toList();

        // Resolve derived property types
        List<DerivedPropertyDefinition> resolvedDerived = classDef.derivedProperties().stream()
                .map(d -> resolveDerivedProperty(d, imports, knownFqns))
                .toList();

        // Canonicalize class-level stereotype and tagged-value profile references to FQN
        // so cross-project profiles work — e.g. `<<rootEntity>>` with `import nlq::*`
        // becomes `<<nlq::NlqProfile.rootEntity>>` in the stored record.
        var resolvedStereotypes = resolveStereotypes(classDef.stereotypes(), imports, knownFqns);
        var resolvedTaggedValues = resolveTaggedValues(classDef.taggedValues(), imports, knownFqns);

        // Only create new record if something changed
        if (resolvedSuperClasses.equals(classDef.superClasses())
                && resolvedProps.equals(classDef.properties())
                && resolvedDerived.equals(classDef.derivedProperties())
                && resolvedStereotypes == classDef.stereotypes()
                && resolvedTaggedValues == classDef.taggedValues()) {
            return classDef;
        }

        return new ClassDefinition(
                classDef.qualifiedName(),
                resolvedSuperClasses,
                resolvedProps,
                resolvedDerived,
                classDef.constraints(),
                resolvedStereotypes,
                resolvedTaggedValues);
    }

    private static PropertyDefinition resolveProperty(
            PropertyDefinition prop, ImportScope imports, Set<String> knownFqns) {
        String resolvedType = imports.resolve(prop.type(), knownFqns);
        var resolvedStereotypes = resolveStereotypes(prop.stereotypes(), imports, knownFqns);
        var resolvedTaggedValues = resolveTaggedValues(prop.taggedValues(), imports, knownFqns);
        if (resolvedType.equals(prop.type())
                && resolvedStereotypes == prop.stereotypes()
                && resolvedTaggedValues == prop.taggedValues()) {
            return prop;
        }
        return new PropertyDefinition(
                prop.name(), resolvedType, prop.lowerBound(), prop.upperBound(),
                resolvedStereotypes, resolvedTaggedValues);
    }

    private static DerivedPropertyDefinition resolveDerivedProperty(
            DerivedPropertyDefinition dp, ImportScope imports, Set<String> knownFqns) {
        String resolvedType = imports.resolve(dp.type(), knownFqns);
        List<ParameterDefinition> resolvedParams = dp.parameters().stream()
                .map(p -> resolveParameter(p, imports, knownFqns))
                .toList();
        if (resolvedType.equals(dp.type()) && resolvedParams.equals(dp.parameters())) return dp;
        return new DerivedPropertyDefinition(
                dp.name(), resolvedParams, dp.expression(), resolvedType,
                dp.lowerBound(), dp.upperBound());
    }

    private static ParameterDefinition resolveParameter(
            ParameterDefinition param, ImportScope imports, Set<String> knownFqns) {
        String resolvedType = imports.resolve(param.type(), knownFqns);
        if (resolvedType.equals(param.type())) return param;
        return new ParameterDefinition(param.name(), resolvedType, param.lowerBound(), param.upperBound());
    }

    // ==================== FunctionDefinition ====================

    private static FunctionDefinition resolveFunction(
            FunctionDefinition funcDef, ImportScope imports, Set<String> knownFqns) {

        String resolvedReturnType = imports.resolve(funcDef.returnType(), knownFqns);
        List<FunctionDefinition.ParameterDefinition> resolvedParams = funcDef.parameters().stream()
                .map(p -> resolveFuncParam(p, imports, knownFqns))
                .toList();

        // Canonicalize function-level stereotype and tagged-value profile references to FQN
        var resolvedStereotypes = resolveStereotypes(funcDef.stereotypes(), imports, knownFqns);
        var resolvedTaggedValues = resolveTaggedValues(funcDef.taggedValues(), imports, knownFqns);

        if (resolvedReturnType.equals(funcDef.returnType())
                && resolvedParams.equals(funcDef.parameters())
                && resolvedStereotypes == funcDef.stereotypes()
                && resolvedTaggedValues == funcDef.taggedValues()) {
            return funcDef;
        }

        return new FunctionDefinition(funcDef.qualifiedName(), resolvedParams,
                resolvedReturnType, funcDef.returnLowerBound(), funcDef.returnUpperBound(),
                funcDef.body(), resolvedStereotypes, resolvedTaggedValues,
                null, funcDef.parsedReturnType());
    }

    // ==================== Stereotype / TaggedValue FQN Canonicalization ====================

    /**
     * Resolves every {@link StereotypeApplication}'s profile name to its FQN via the import
     * scope. This ensures cross-project stereotype references compare equal regardless of
     * how the user wrote them (short name vs. fully qualified).
     *
     * <p>Returns the input list unchanged (identity equal) if no profile names needed
     * canonicalization — callers can use {@code ==} to detect a no-op.
     */
    private static List<StereotypeApplication> resolveStereotypes(
            List<StereotypeApplication> apps, ImportScope imports, Set<String> knownFqns) {
        if (apps.isEmpty()) return apps;
        boolean changed = false;
        List<StereotypeApplication> result = new ArrayList<>(apps.size());
        for (var app : apps) {
            String resolved = imports.resolve(app.profileName(), knownFqns);
            if (resolved.equals(app.profileName())) {
                result.add(app);
            } else {
                changed = true;
                result.add(new StereotypeApplication(resolved, app.stereotypeName()));
            }
        }
        return changed ? List.copyOf(result) : apps;
    }

    /**
     * Resolves every {@link TaggedValue}'s profile name to its FQN via the import scope.
     * See {@link #resolveStereotypes} for the rationale.
     */
    private static List<TaggedValue> resolveTaggedValues(
            List<TaggedValue> tvs, ImportScope imports, Set<String> knownFqns) {
        if (tvs.isEmpty()) return tvs;
        boolean changed = false;
        List<TaggedValue> result = new ArrayList<>(tvs.size());
        for (var tv : tvs) {
            String resolved = imports.resolve(tv.profileName(), knownFqns);
            if (resolved.equals(tv.profileName())) {
                result.add(tv);
            } else {
                changed = true;
                result.add(new TaggedValue(resolved, tv.tagName(), tv.value()));
            }
        }
        return changed ? List.copyOf(result) : tvs;
    }

    private static FunctionDefinition.ParameterDefinition resolveFuncParam(
            FunctionDefinition.ParameterDefinition param, ImportScope imports, Set<String> knownFqns) {
        String resolvedType = imports.resolve(param.type(), knownFqns);
        if (resolvedType.equals(param.type())) return param;
        return new FunctionDefinition.ParameterDefinition(
                param.name(), resolvedType, param.lowerBound(), param.upperBound(),
                param.functionType(), param.parsedType());
    }

    // ==================== DatabaseDefinition ====================

    private static DatabaseDefinition resolveDatabase(
            DatabaseDefinition dbDef, ImportScope imports, Set<String> knownFqns) {
        // Canonicalize included database FQNs (cross-project DB includes)
        List<String> resolvedIncludes = resolveFqnList(dbDef.includes(), imports, knownFqns);
        if (resolvedIncludes == dbDef.includes()) return dbDef;
        return new DatabaseDefinition(
                dbDef.qualifiedName(),
                resolvedIncludes,
                dbDef.schemas(),
                dbDef.tables(),
                dbDef.views(),
                dbDef.joins(),
                dbDef.filters(),
                dbDef.multiGrainFilters());
    }

    // ==================== RuntimeDefinition ====================

    private static RuntimeDefinition resolveRuntime(
            RuntimeDefinition rtDef, ImportScope imports, Set<String> knownFqns) {
        // Canonicalize mapping FQNs referenced by this runtime
        List<String> resolvedMappings = resolveFqnList(rtDef.mappings(), imports, knownFqns);
        // Canonicalize both sides of every connection binding (store FQN → connection FQN)
        Map<String, String> resolvedBindings = resolveFqnMap(rtDef.connectionBindings(), imports, knownFqns);
        if (resolvedMappings == rtDef.mappings()
                && resolvedBindings == rtDef.connectionBindings()) {
            return rtDef;
        }
        return new RuntimeDefinition(
                rtDef.qualifiedName(),
                resolvedMappings,
                resolvedBindings,
                rtDef.jsonConnections());
    }

    // ==================== ServiceDefinition ====================

    private static ServiceDefinition resolveService(
            ServiceDefinition svcDef, ImportScope imports, Set<String> knownFqns) {
        // Canonicalize mapping and runtime refs (nullable in the record)
        String resolvedMappingRef = svcDef.mappingRef() != null
                ? imports.resolve(svcDef.mappingRef(), knownFqns) : null;
        String resolvedRuntimeRef = svcDef.runtimeRef() != null
                ? imports.resolve(svcDef.runtimeRef(), knownFqns) : null;
        if (java.util.Objects.equals(resolvedMappingRef, svcDef.mappingRef())
                && java.util.Objects.equals(resolvedRuntimeRef, svcDef.runtimeRef())) {
            return svcDef;
        }
        return new ServiceDefinition(
                svcDef.qualifiedName(),
                svcDef.pattern(),
                svcDef.functionBody(),
                svcDef.pathParams(),
                svcDef.documentation(),
                resolvedMappingRef,
                resolvedRuntimeRef,
                svcDef.testSuites());
    }

    // ==================== ConnectionDefinition ====================

    private static ConnectionDefinition resolveConnection(
            ConnectionDefinition connDef, ImportScope imports, Set<String> knownFqns) {
        // Canonicalize the store FQN this connection binds to. Some connection types
        // (e.g., JSON/model connections) don't bind to a relational store, so storeName
        // can be null — leave it null in that case.
        String resolvedStore = connDef.storeName() != null
                ? imports.resolve(connDef.storeName(), knownFqns) : null;
        if (java.util.Objects.equals(resolvedStore, connDef.storeName())) return connDef;
        return new ConnectionDefinition(
                connDef.qualifiedName(),
                resolvedStore,
                connDef.databaseType(),
                connDef.specification(),
                connDef.authentication());
    }

    // ==================== FQN List/Map Helpers ====================

    /**
     * Resolves every string in a list through the import scope. Returns the input list
     * unchanged (identity equal) if no strings needed canonicalization.
     */
    private static List<String> resolveFqnList(
            List<String> fqns, ImportScope imports, Set<String> knownFqns) {
        if (fqns.isEmpty()) return fqns;
        boolean changed = false;
        List<String> result = new ArrayList<>(fqns.size());
        for (String fqn : fqns) {
            String resolved = imports.resolve(fqn, knownFqns);
            if (!resolved.equals(fqn)) changed = true;
            result.add(resolved);
        }
        return changed ? List.copyOf(result) : fqns;
    }

    /**
     * Resolves every key and value in a map through the import scope (used for
     * {@code RuntimeDefinition.connectionBindings}, where both keys and values are FQNs).
     */
    private static java.util.Map<String, String> resolveFqnMap(
            java.util.Map<String, String> map, ImportScope imports, Set<String> knownFqns) {
        if (map.isEmpty()) return map;
        boolean changed = false;
        java.util.Map<String, String> result = new java.util.LinkedHashMap<>(map.size());
        for (var entry : map.entrySet()) {
            String resolvedKey = imports.resolve(entry.getKey(), knownFqns);
            String resolvedValue = imports.resolve(entry.getValue(), knownFqns);
            if (!resolvedKey.equals(entry.getKey()) || !resolvedValue.equals(entry.getValue())) {
                changed = true;
            }
            result.put(resolvedKey, resolvedValue);
        }
        return changed ? java.util.Map.copyOf(result) : map;
    }

    // ==================== AssociationDefinition ====================

    private static AssociationDefinition resolveAssociation(
            AssociationDefinition assocDef, ImportScope imports, Set<String> knownFqns) {

        var end1 = assocDef.property1();
        var end2 = assocDef.property2();

        String resolved1 = imports.resolve(end1.targetClass(), knownFqns);
        String resolved2 = imports.resolve(end2.targetClass(), knownFqns);

        if (resolved1.equals(end1.targetClass()) && resolved2.equals(end2.targetClass())) {
            return assocDef;
        }

        return new AssociationDefinition(
                assocDef.qualifiedName(),
                new AssociationDefinition.AssociationEndDefinition(
                        end1.propertyName(), resolved1, end1.lowerBound(), end1.upperBound()),
                new AssociationDefinition.AssociationEndDefinition(
                        end2.propertyName(), resolved2, end2.lowerBound(), end2.upperBound()));
    }

    // ==================== MappingDefinition ====================

    private static MappingDefinition resolveMapping(
            MappingDefinition mappingDef, ImportScope imports, Set<String> knownFqns) {

        List<MappingDefinition.ClassMappingDefinition> resolvedMappings = mappingDef.classMappings().stream()
                .map(cm -> resolveClassMapping(cm, imports, knownFqns))
                .toList();

        List<AssociationMappingDefinition> resolvedAssocMappings =
                mappingDef.associationMappings().stream()
                        .map(am -> resolveAssociationMapping(am, imports, knownFqns))
                        .toList();

        // Canonicalize each enumeration mapping's enumType to FQN so downstream lookups can
        // compare qualified names directly instead of relying on fuzzy endsWith matching.
        // See docs/BAZEL_DEPENDENCY_PROPOSAL.md §6 (Category A dependency matrix).
        List<MappingDefinition.EnumerationMappingDefinition> resolvedEnumMappings =
                mappingDef.enumerationMappings().stream()
                        .map(em -> resolveEnumerationMapping(em, imports, knownFqns))
                        .toList();

        // Canonicalize mapping includes: the included-mapping FQN plus both sides of every
        // store substitution. Needed for cross-project mapping compositions.
        List<MappingInclude> resolvedIncludes = resolveMappingIncludes(
                mappingDef.includes(), imports, knownFqns);

        if (resolvedMappings.equals(mappingDef.classMappings())
                && resolvedAssocMappings.equals(mappingDef.associationMappings())
                && resolvedEnumMappings.equals(mappingDef.enumerationMappings())
                && resolvedIncludes == mappingDef.includes()) {
            return mappingDef;
        }

        return new MappingDefinition(
                mappingDef.qualifiedName(),
                resolvedIncludes,
                resolvedMappings,
                resolvedAssocMappings,
                resolvedEnumMappings,
                mappingDef.testSuites());
    }

    private static List<MappingInclude> resolveMappingIncludes(
            List<MappingInclude> includes, ImportScope imports, Set<String> knownFqns) {
        if (includes.isEmpty()) return includes;
        boolean changed = false;
        List<MappingInclude> result = new ArrayList<>(includes.size());
        for (MappingInclude inc : includes) {
            String resolvedPath = imports.resolve(inc.includedMappingPath(), knownFqns);
            List<MappingInclude.StoreSubstitution> resolvedSubs = resolveStoreSubstitutions(
                    inc.storeSubstitutions(), imports, knownFqns);
            if (resolvedPath.equals(inc.includedMappingPath())
                    && resolvedSubs == inc.storeSubstitutions()) {
                result.add(inc);
            } else {
                changed = true;
                result.add(new MappingInclude(resolvedPath, resolvedSubs));
            }
        }
        return changed ? List.copyOf(result) : includes;
    }

    private static List<MappingInclude.StoreSubstitution> resolveStoreSubstitutions(
            List<MappingInclude.StoreSubstitution> subs, ImportScope imports, Set<String> knownFqns) {
        if (subs.isEmpty()) return subs;
        boolean changed = false;
        List<MappingInclude.StoreSubstitution> result = new ArrayList<>(subs.size());
        for (var sub : subs) {
            String resolvedOrig = imports.resolve(sub.originalStore(), knownFqns);
            String resolvedSub = imports.resolve(sub.substituteStore(), knownFqns);
            if (resolvedOrig.equals(sub.originalStore()) && resolvedSub.equals(sub.substituteStore())) {
                result.add(sub);
            } else {
                changed = true;
                result.add(new MappingInclude.StoreSubstitution(resolvedOrig, resolvedSub));
            }
        }
        return changed ? List.copyOf(result) : subs;
    }

    private static MappingDefinition.EnumerationMappingDefinition resolveEnumerationMapping(
            MappingDefinition.EnumerationMappingDefinition em, ImportScope imports, Set<String> knownFqns) {
        String resolvedEnumType = imports.resolve(em.enumType(), knownFqns);
        if (resolvedEnumType.equals(em.enumType())) {
            return em;
        }
        return new MappingDefinition.EnumerationMappingDefinition(
                resolvedEnumType, em.id(), em.valueMappings());
    }

    private static MappingDefinition.ClassMappingDefinition resolveClassMapping(
            MappingDefinition.ClassMappingDefinition cm, ImportScope imports, Set<String> knownFqns) {

        String resolvedClassName = imports.resolve(cm.className(), knownFqns);
        String resolvedSourceClassName = cm.sourceClassName() != null
                ? imports.resolve(cm.sourceClassName(), knownFqns) : null;

        // Resolve database names in mainTable, filter, and property mappings
        var resolvedMainTable = resolveTableReference(cm.mainTable(), imports, knownFqns);
        var resolvedFilter = resolveMappingFilter(cm.filter(), imports, knownFqns);
        var resolvedPropertyMappings = cm.propertyMappings().stream()
                .map(pm -> resolvePropertyMapping(pm, imports, knownFqns))
                .toList();

        return new MappingDefinition.ClassMappingDefinition(
                resolvedClassName, cm.mappingType(), cm.setId(), cm.isRoot(), cm.extendsSetId(),
                resolvedMainTable, resolvedFilter, cm.distinct(), cm.groupBy(), cm.primaryKey(),
                resolvedPropertyMappings, resolvedSourceClassName, cm.filterExpression(),
                cm.m2mPropertyExpressions());
    }

    // ==================== Database-Scoped Name Resolution ====================

    private static MappingDefinition.TableReference resolveTableReference(
            MappingDefinition.TableReference ref, ImportScope imports, Set<String> knownFqns) {
        if (ref == null) return null;
        String resolvedDb = imports.resolve(ref.databaseName(), knownFqns);
        if (resolvedDb.equals(ref.databaseName())) return ref;
        return new MappingDefinition.TableReference(resolvedDb, ref.tableName());
    }

    private static MappingDefinition.MappingFilter resolveMappingFilter(
            MappingDefinition.MappingFilter filter, ImportScope imports, Set<String> knownFqns) {
        if (filter == null) return null;
        String resolvedDb = filter.databaseName() != null
                ? imports.resolve(filter.databaseName(), knownFqns) : null;
        var resolvedJoinPath = filter.joinPath().stream()
                .map(jce -> resolveJoinChainElement(jce, imports, knownFqns))
                .toList();
        return new MappingDefinition.MappingFilter(resolvedDb, resolvedJoinPath, filter.filterName());
    }

    private static MappingDefinition.ColumnReference resolveColumnReference(
            MappingDefinition.ColumnReference ref, ImportScope imports, Set<String> knownFqns) {
        if (ref == null) return null;
        String resolvedDb = imports.resolve(ref.databaseName(), knownFqns);
        if (resolvedDb.equals(ref.databaseName())) return ref;
        return new MappingDefinition.ColumnReference(resolvedDb, ref.tableName(), ref.columnName());
    }

    private static MappingDefinition.JoinReference resolveJoinReference(
            MappingDefinition.JoinReference ref, ImportScope imports, Set<String> knownFqns) {
        if (ref == null) return null;
        String resolvedDb = ref.databaseName() != null
                ? imports.resolve(ref.databaseName(), knownFqns) : null;
        var resolvedChain = ref.joinChain().stream()
                .map(jce -> resolveJoinChainElement(jce, imports, knownFqns))
                .toList();
        return new MappingDefinition.JoinReference(resolvedDb, resolvedChain, ref.terminalColumn());
    }

    private static JoinChainElement resolveJoinChainElement(
            JoinChainElement jce, ImportScope imports, Set<String> knownFqns) {
        // databaseName is never null — parser stamps parent DB on every hop
        String resolvedDb = imports.resolve(jce.databaseName(), knownFqns);
        if (resolvedDb.equals(jce.databaseName())) return jce;
        return new JoinChainElement(jce.joinName(), jce.joinType(), resolvedDb, jce.strict());
    }

    private static MappingDefinition.PropertyMappingDefinition resolvePropertyMapping(
            MappingDefinition.PropertyMappingDefinition pm, ImportScope imports, Set<String> knownFqns) {
        var resolvedCol = resolveColumnReference(pm.columnReference(), imports, knownFqns);
        var resolvedJoin = resolveJoinReference(pm.joinReference(), imports, knownFqns);
        if (resolvedCol == pm.columnReference() && resolvedJoin == pm.joinReference()) return pm;
        return new MappingDefinition.PropertyMappingDefinition(
                pm.propertyName(), resolvedCol, resolvedJoin, pm.expressionString(),
                pm.embeddedClassName(), pm.enumMappingId(), pm.mappingExpression(), pm.structuredValue());
    }

    private static AssociationMappingDefinition resolveAssociationMapping(
            AssociationMappingDefinition am, ImportScope imports, Set<String> knownFqns) {

        String resolvedAssocName = imports.resolve(am.associationName(), knownFqns);

        // Resolve database names in association property mapping join chains
        var resolvedProps = am.properties().stream()
                .map(prop -> {
                    var resolvedChain = prop.joinChain().stream()
                            .map(jce -> resolveJoinChainElement(jce, imports, knownFqns))
                            .toList();
                    if (resolvedChain.equals(prop.joinChain())) return prop;
                    return new AssociationMappingDefinition.AssociationPropertyMapping(
                            prop.propertyName(), prop.sourceSetId(), prop.targetSetId(),
                            resolvedChain, prop.crossExpression());
                })
                .toList();

        if (resolvedAssocName.equals(am.associationName()) && resolvedProps.equals(am.properties())) return am;

        return new AssociationMappingDefinition(
                resolvedAssocName, am.mappingType(), resolvedProps);
    }

    // ==================== Query AST Resolution ====================

    /**
     * Resolves all class name references in a query AST to FQN.
     *
     * <p>Walks the AST tree and resolves:
     * <ul>
     *   <li>{@link PackageableElementPtr#fullPath()} — e.g., {@code Employee} in {@code Employee.all()}</li>
     *   <li>Desugared graphFetch root class — e.g., {@code Employee} in {@code #{Employee{...}}#}</li>
     *   <li>{@link GenericTypeInstance#fullPath()} — e.g., {@code Employee} in {@code @Employee}</li>
     * </ul>
     *
     * @param ast       The raw query AST from the parser
     * @param imports   The import scope from the model source
     * @param knownFqns All registered FQN element names
     * @return New AST with resolved names (original returned if nothing changed)
     */
    public static ValueSpecification resolveQuery(
            ValueSpecification ast, ImportScope imports, Set<String> knownFqns) {
        return resolveVs(ast, imports, knownFqns);
    }

    private static ValueSpecification resolveVs(
            ValueSpecification vs, ImportScope imports, Set<String> knownFqns) {
        return switch (vs) {
            case PackageableElementPtr ptr -> {
                String resolved = imports.resolve(ptr.fullPath(), knownFqns);
                yield resolved.equals(ptr.fullPath()) ? ptr : new PackageableElementPtr(resolved);
            }
            case GenericTypeInstance gti -> {
                String resolved = imports.resolve(gti.fullPath(), knownFqns);
                yield resolved.equals(gti.fullPath()) ? gti : new GenericTypeInstance(resolved, gti.resolvedType());
            }
            case AppliedFunction af -> {
                String resolvedFunc = imports.resolve(af.function(), knownFqns);
                List<ValueSpecification> resolvedParams = resolveList(af.parameters(), imports, knownFqns);
                // tableReference(CString(db), CString(name)): resolve the db arg
                if ("tableReference".equals(af.function()) && resolvedParams.size() == 2
                        && resolvedParams.get(0) instanceof CString dbCs) {
                    String resolvedDb = imports.resolve(dbCs.value(), knownFqns);
                    if (!resolvedDb.equals(dbCs.value())) {
                        resolvedParams = List.of(new CString(resolvedDb), resolvedParams.get(1));
                    }
                }
                yield (resolvedFunc.equals(af.function()) && resolvedParams == af.parameters()) ? af
                        : new AppliedFunction(resolvedFunc, resolvedParams, af.hasReceiver(),
                                af.sourceText(), af.argTexts());
            }
            case LambdaFunction lf -> {
                List<ValueSpecification> resolvedBody = resolveList(lf.body(), imports, knownFqns);
                yield resolvedBody == lf.body() ? lf : new LambdaFunction(lf.parameters(), resolvedBody);
            }
            case PureCollection coll -> {
                List<ValueSpecification> resolvedValues = resolveList(coll.values(), imports, knownFqns);
                yield resolvedValues == coll.values() ? coll : new PureCollection(resolvedValues);
            }
            case ClassInstance ci -> {
                Object resolvedValue = resolveClassInstanceValue(ci.value(), imports, knownFqns);
                yield resolvedValue == ci.value() ? ci : new ClassInstance(ci.type(), resolvedValue);
            }
            case AppliedProperty ap -> {
                List<ValueSpecification> resolvedParams = resolveList(ap.parameters(), imports, knownFqns);
                yield resolvedParams == ap.parameters() ? ap
                        : new AppliedProperty(ap.property(), resolvedParams);
            }
            case EnumValue ev -> {
                String resolved = imports.resolve(ev.fullPath(), knownFqns);
                yield resolved.equals(ev.fullPath()) ? ev : new EnumValue(resolved, ev.value());
            }
            // Leaf nodes — no names to resolve
            default -> vs;
        };
    }

    private static List<ValueSpecification> resolveList(
            List<ValueSpecification> list, ImportScope imports, Set<String> knownFqns) {
        boolean changed = false;
        List<ValueSpecification> result = new ArrayList<>(list.size());
        for (ValueSpecification vs : list) {
            ValueSpecification resolved = resolveVs(vs, imports, knownFqns);
            if (resolved != vs) changed = true;
            result.add(resolved);
        }
        return changed ? List.copyOf(result) : list;
    }

    private static Object resolveClassInstanceValue(
            Object value, ImportScope imports, Set<String> knownFqns) {
        // ClassInstance values (ColSpecArray, ColSpec, etc.) contain no class names to resolve.
        return value;
    }

}
