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
                var sourceRelation = synthesizeSourceRelation(rm);
                expressions.put(className, new ModelContext.MappingExpression.Relational(
                        className, sourceRelation));
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

    // ==================== Source Relation Synthesis ====================

    /**
     * Synthesizes a sourceRelation ValueSpecification for a RelationalMapping.
     * This is the single source of truth for the mapping's data source.
     *
     * <p>Canonical ordering:
     * <ol>
     *   <li>tableReference("db.TABLE") — base table</li>
     *   <li>→filter(lambda) — ~filter condition (if any)</li>
     *   <li>→join(...) — property mapping join chains (LEFT OUTER)</li>
     *   <li>→distinct() — ~distinct (if set)</li>
     * </ol>
     */
    private com.gs.legend.ast.ValueSpecification synthesizeSourceRelation(RelationalMapping rm) {
        // 1. Base: tableReference("db.TABLE")
        String tableName = rm.table().name();
        com.gs.legend.ast.ValueSpecification source = new com.gs.legend.ast.AppliedFunction(
                "tableReference",
                java.util.List.of(new com.gs.legend.ast.CString(tableName)),
                false);

        // 2. ~filter → ->filter({row | <condition>})
        if (rm.filterName() != null) {
            var filterBody = resolveFilterCondition(rm);
            if (filterBody != null) {
                var lambda = new com.gs.legend.ast.LambdaFunction(
                        java.util.List.of(new com.gs.legend.ast.Variable("row")),
                        filterBody);
                source = new com.gs.legend.ast.AppliedFunction(
                        "filter",
                        java.util.List.of(source, lambda),
                        true);
            }
        }

        // 3. Property mapping join chains → ->extend(traverse(...), ~[colSpecs])
        source = addTraverseExtends(rm, source);

        // 4. ~distinct → ->distinct()
        if (rm.distinct()) {
            source = new com.gs.legend.ast.AppliedFunction(
                    "distinct",
                    java.util.List.of(source),
                    true);
        }

        return source;
    }

    /**
     * Groups join-chain property mappings by their chain path and synthesizes
     * {@code ->extend(traverse(...), ~[prop:t|$t.COL])} for each unique chain.
     *
     * <p>Properties sharing the same join chain path are grouped into a single
     * extend call with multiple colSpecs. Different chains get separate extends.
     */
    private com.gs.legend.ast.ValueSpecification addTraverseExtends(
            RelationalMapping rm,
            com.gs.legend.ast.ValueSpecification source) {

        // Group join-chain properties by their full chain path
        var chainGroups = new LinkedHashMap<java.util.List<String>,
                java.util.List<com.gs.legend.model.store.PropertyMapping>>();
        for (var pm : rm.propertyMappings()) {
            if (!pm.hasJoinChain()) continue;
            chainGroups.computeIfAbsent(pm.joinChain(), k -> new java.util.ArrayList<>()).add(pm);
        }

        if (chainGroups.isEmpty()) return source;

        String mainTable = rm.table().name();

        for (var entry : chainGroups.entrySet()) {
            java.util.List<String> chainNames = entry.getKey();
            var props = entry.getValue();

            // Build traverse chain
            var traverseExpr = buildTraverseChain(mainTable, chainNames);

            // Build colSpecs: ~[prop1:t|$t.COL1, prop2:t|$t.COL2]
            var colSpecs = new java.util.ArrayList<com.gs.legend.ast.ColSpec>();
            for (var pm : props) {
                var lambda = new com.gs.legend.ast.LambdaFunction(
                        java.util.List.of(new com.gs.legend.ast.Variable("t")),
                        new com.gs.legend.ast.AppliedProperty(
                                pm.columnName(),
                                java.util.List.of(new com.gs.legend.ast.Variable("t"))));
                colSpecs.add(new com.gs.legend.ast.ColSpec(pm.propertyName(), lambda));
            }

            // Wrap in ClassInstance
            com.gs.legend.ast.ClassInstance colSpecCI;
            if (colSpecs.size() == 1) {
                colSpecCI = new com.gs.legend.ast.ClassInstance("colSpec", colSpecs.get(0));
            } else {
                colSpecCI = new com.gs.legend.ast.ClassInstance("colSpecArray",
                        new com.gs.legend.ast.ColSpecArray(colSpecs));
            }

            source = new com.gs.legend.ast.AppliedFunction(
                    "extend",
                    java.util.List.of(source, traverseExpr, colSpecCI),
                    true);
        }

        return source;
    }

    /**
     * Builds a traverse chain from join definitions:
     * {@code traverse(T_DEPT, {prev,hop|...})->traverse(T_ORG, {prev,hop|...})}
     */
    private com.gs.legend.ast.ValueSpecification buildTraverseChain(
            String startTable, java.util.List<String> joinNames) {

        String currentTable = startTable;
        com.gs.legend.ast.ValueSpecification traverseExpr = null;

        for (String joinName : joinNames) {
            Join join = model.findJoin(joinName)
                    .orElseThrow(() -> new IllegalStateException(
                            "Join not found: " + joinName));

            String rightTable = join.getOtherTable(currentTable);
            String leftCol = join.getColumnForTable(currentTable);
            String rightCol = join.getColumnForTable(rightTable);

            var targetRef = new com.gs.legend.ast.AppliedFunction(
                    "tableReference",
                    java.util.List.of(new com.gs.legend.ast.CString(rightTable)),
                    false);

            var condBody = new com.gs.legend.ast.AppliedFunction(
                    "equal",
                    java.util.List.of(
                            new com.gs.legend.ast.AppliedProperty(
                                    leftCol,
                                    java.util.List.of(new com.gs.legend.ast.Variable("prev"))),
                            new com.gs.legend.ast.AppliedProperty(
                                    rightCol,
                                    java.util.List.of(new com.gs.legend.ast.Variable("hop")))));
            var condLambda = new com.gs.legend.ast.LambdaFunction(
                    java.util.List.of(
                            new com.gs.legend.ast.Variable("prev"),
                            new com.gs.legend.ast.Variable("hop")),
                    condBody);

            if (traverseExpr == null) {
                // Standalone: traverse(target, cond)
                traverseExpr = new com.gs.legend.ast.AppliedFunction(
                        "traverse",
                        java.util.List.of(targetRef, condLambda),
                        false);
            } else {
                // Chained: traverse(prev, target, cond)
                traverseExpr = new com.gs.legend.ast.AppliedFunction(
                        "traverse",
                        java.util.List.of(traverseExpr, targetRef, condLambda),
                        false);
            }

            currentTable = rightTable;
        }

        return traverseExpr;
    }

    /**
     * Resolves a ~filter name to a ValueSpecification condition body.
     * Returns null if the filter is not found.
     */
    private com.gs.legend.ast.ValueSpecification resolveFilterCondition(RelationalMapping rm) {
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
        return RelationalMappingConverter.convert(filter.condition());
    }
}
