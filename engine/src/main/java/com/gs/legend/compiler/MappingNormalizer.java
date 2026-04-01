package com.gs.legend.compiler;

import com.gs.legend.model.ModelContext;
import com.gs.legend.model.PureModelBuilder;
import com.gs.legend.model.RelationalMappingConverter;
import com.gs.legend.model.mapping.ClassMapping;
import com.gs.legend.model.mapping.PureClassMapping;
import com.gs.legend.model.mapping.RelationalMapping;
import com.gs.legend.model.m3.PureClass;
import com.gs.legend.model.store.Filter;
import com.gs.legend.model.store.Join;
import com.gs.legend.plan.GenericType;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Pipeline step that reads from {@link com.gs.legend.model.mapping.MappingRegistry}
 * and produces an immutable {@link NormalizedMapping} snapshot.
 *
 * <p>This is the <b>only consumer</b> of MappingRegistry. It performs:
 * <ol>
 *   <li>M2M chain resolution (fills in {@code targetClass} + {@code sourceMapping})</li>
 *   <li>Relational filter conversion ({@code ~filter} → ValueSpecification)</li>
 *   <li>MappingExpression construction (for TypeChecker via ModelContext)</li>
 *   <li>Join collection from the mapping's database definitions</li>
 * </ol>
 *
 * <p>Runs after parsing, before TypeChecker. The resulting NormalizedMapping is
 * immutable — MappingResolver becomes purely read-only.
 */
public final class MappingNormalizer {

    private final PureModelBuilder model;
    private NormalizedMapping normalized;

    /**
     * Normalizes eagerly. If mappingNames is empty, produces an empty snapshot
     * and modelContext() returns the base model.
     */
    public MappingNormalizer(PureModelBuilder model, java.util.List<String> mappingNames) {
        this.model = model;
        this.normalized = normalize(mappingNames);
    }

    /**
     * Restricted view for TypeChecker — only sees findMappingExpression.
     */
    public ModelContext modelContext() {
        if (normalized.allClassMappings().isEmpty()) return model;
        return new ModelContext() {
            @Override public java.util.Optional<com.gs.legend.model.m3.PureClass> findClass(String n) { return model.findClass(n); }
            @Override public java.util.Optional<AssociationNavigation> findAssociationByProperty(String c, String p) { return model.findAssociationByProperty(c, p); }
            @Override public java.util.Map<String, AssociationNavigation> findAllAssociationNavigations(String c) { return model.findAllAssociationNavigations(c); }
            @Override public java.util.Optional<com.gs.legend.model.store.Join> findJoin(String n) { return model.findJoin(n); }
            @Override public java.util.Optional<com.gs.legend.model.store.Table> findTable(String n) { return model.findTable(n); }
            @Override public java.util.Optional<com.gs.legend.model.def.EnumDefinition> findEnum(String n) { return model.findEnum(n); }
            @Override public java.util.Optional<MappingExpression> findMappingExpression(String className) {
                return normalized.findMappingExpression(className);
            }
        };
    }

    /**
     * Full mapping data for MappingResolver — findClassMapping + findJoin.
     * TypeChecker never sees this.
     */
    public NormalizedMapping normalizedMapping() {
        return normalized;
    }

    // ==================== Normalization ====================

    private NormalizedMapping normalize(java.util.List<String> mappingNames) {
        if (mappingNames.isEmpty()) return NormalizedMapping.empty();

        var registry = model.getMappingRegistry();
        Map<String, ClassMapping> allMappings = new LinkedHashMap<>();
        for (String name : mappingNames) {
            allMappings.putAll(registry.getAllClassMappings(name));
        }

        Map<String, ClassMapping> resolvedMappings = new LinkedHashMap<>();
        Map<String, ModelContext.MappingExpression> expressions = new LinkedHashMap<>();

        // Phase 1: Resolve M2M chains
        Set<String> resolving = new HashSet<>();
        for (var entry : allMappings.entrySet()) {
            String className = entry.getKey();
            ClassMapping cm = entry.getValue();

            if (cm instanceof PureClassMapping pcm) {
                ClassMapping resolved = resolveM2MChain(pcm, allMappings, resolving);
                resolvedMappings.put(className, resolved);
            } else {
                resolvedMappings.put(className, cm);
            }
        }

        // Phase 2: Build MappingExpressions + resolve filters
        for (var entry : resolvedMappings.entrySet()) {
            String className = entry.getKey();
            ClassMapping cm = entry.getValue();

            if (cm instanceof PureClassMapping pcm) {
                expressions.put(className, new ModelContext.MappingExpression.M2M(
                        pcm.sourceClassName(), pcm.propertyExpressions(), pcm.filter()));
            } else if (cm instanceof RelationalMapping rm) {
                var filterExpr = resolveRelationalFilter(rm);
                var schema = buildSchemaFromTable(rm);
                expressions.put(className, new ModelContext.MappingExpression.Relational(
                        className, schema, filterExpr, rm.distinct()));
            }
        }

        // Phase 3: Collect all joins from registry (database-level, not mapping-scoped)
        Map<String, Join> joins = new LinkedHashMap<>(registry.getAllJoins());

        return new NormalizedMapping(resolvedMappings, expressions, joins);
    }

    // ==================== M2M Chain Resolution ====================

    /**
     * Resolves M2M chains recursively. Same algorithm as the former
     * MappingResolver.resolveM2MSource(), but runs at normalization time.
     */
    private ClassMapping resolveM2MChain(
            PureClassMapping pcm,
            Map<String, ClassMapping> allMappings,
            Set<String> resolving) {

        String targetClassName = pcm.targetClassName();
        if (resolving.contains(targetClassName)) {
            throw new IllegalStateException(
                    "Circular M2M chain detected for class: " + targetClassName);
        }

        String sourceClassName = pcm.sourceClassName();
        ClassMapping sourceMapping = allMappings.get(sourceClassName);

        if (sourceMapping == null) {
            // Source class has no mapping in this scope — leave unresolved
            return pcm;
        }

        // If source is also M2M, resolve it recursively first
        if (sourceMapping instanceof PureClassMapping srcPcm) {
            resolving.add(targetClassName);
            sourceMapping = resolveM2MChain(srcPcm, allMappings, resolving);
            resolving.remove(targetClassName);

            // Update the source in allMappings so other chains can use the resolved version
            allMappings.put(sourceClassName, sourceMapping);
        }

        // Resolve target class
        PureClass targetClass = model.findClass(targetClassName).orElse(null);

        // Fill in the two nulls
        return pcm.withResolved(targetClass, sourceMapping);
    }

    // ==================== Relational Filter Resolution ====================

    /**
     * Resolves a relational mapping's ~filter reference to a ValueSpecification.
     * Returns null if no filter is defined.
     */
    private com.gs.legend.ast.ValueSpecification resolveRelationalFilter(RelationalMapping rm) {
        if (rm.filterName() == null) return null;

        // Look up the filter by name, trying qualified (db.filterName) first
        Filter filter = null;
        if (rm.filterDbName() != null) {
            filter = model.getFilter(rm.filterDbName() + "." + rm.filterName()).orElse(null);
        }
        if (filter == null) {
            filter = model.getFilter(rm.filterName()).orElse(null);
        }

        if (filter == null) {
            throw new IllegalStateException(
                    "Filter '" + rm.filterName() + "' not found for mapping of "
                            + rm.pureClass().qualifiedName());
        }

        // Convert RelationalOperation → ValueSpecification
        return RelationalMappingConverter.convert(filter.condition());
    }

    /**
     * Builds a Relation Schema from the table's columns for typing filter expressions.
     */
    private GenericType.Relation.Schema buildSchemaFromTable(RelationalMapping rm) {
        Map<String, GenericType> columns = new LinkedHashMap<>();
        for (var col : rm.table().columns()) {
            columns.put(col.name(), col.dataType().toGenericType());
        }
        return new GenericType.Relation.Schema(columns, java.util.List.of());
    }
}
