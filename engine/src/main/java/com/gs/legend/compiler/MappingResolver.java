package com.gs.legend.compiler;

import com.gs.legend.ast.*;
import com.gs.legend.model.ModelContext;
import com.gs.legend.model.mapping.ClassMapping;
import com.gs.legend.model.mapping.PureClassMapping;
import com.gs.legend.model.mapping.RelationalMapping;
import com.gs.legend.model.m3.PureClass;
import com.gs.legend.model.store.Join;
import com.gs.legend.model.store.PropertyMapping;
import com.gs.legend.plan.GenericType;

import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Pass 3: Resolves mappings to physical store concepts.
 *
 * <p>Runs AFTER {@link TypeChecker} (Pass 2), BEFORE PlanGenerator (Pass 4).
 * Receives {@link NormalizedMapping} — all M2M chains are pre-resolved,
 * so this pass is purely read-only.
 *
 * <p>Produces a per-node {@link StoreResolution} sidecar with the same
 * IdentityHashMap pattern as TypeInfo.
 *
 * <h3>Pipeline</h3>
 * <pre>
 * PureParser → MappingNormalizer → TypeChecker → MappingResolver → PlanGenerator
 *  (Pass 1)      (Pass 1.5)         (Pass 2)       (Pass 3)          (Pass 4)
 * </pre>
 */
public final class MappingResolver {

    private final TypeCheckResult typeResult;
    private final ModelContext modelContext;
    private final NormalizedMapping normalized;
    private final IdentityHashMap<ValueSpecification, StoreResolution> resolutions = new IdentityHashMap<>();
    private final java.util.Set<String> resolving = new java.util.HashSet<>();

    /**
     * @param typeResult   Typed AST from TypeChecker
     * @param normalized   Immutable mapping snapshot (from MappingNormalizer)
     * @param modelContext Model for class/association lookups
     */
    public MappingResolver(TypeCheckResult typeResult, NormalizedMapping normalized,
                           ModelContext modelContext) {
        this.typeResult = typeResult;
        this.modelContext = modelContext;
        this.normalized = normalized;
    }

    /**
     * Resolves all mapping-dependent nodes in the typed AST.
     *
     * @return per-node StoreResolution sidecar
     */
    public IdentityHashMap<ValueSpecification, StoreResolution> resolve() {
        walkNode(typeResult.root(), null);
        return resolutions;
    }

    // ==================== AST Walk ====================

    /**
     * Walks the AST, propagating the active StoreResolution from source to downstream.
     * getAll()/new() create a new resolution; filter/project/sort/etc. inherit from source.
     */
    private void walkNode(ValueSpecification vs, StoreResolution active) {
        TypeInfo info = typeResult.types().get(vs);
        if (info != null && info.inlinedBody() != null) {
            walkNode(info.inlinedBody(), active);
        }

        switch (vs) {
            case AppliedFunction af -> walkFunction(af, active);
            case LambdaFunction lf -> {
                for (var expr : lf.body()) walkNode(expr, active);
            }
            case PureCollection pc -> {
                for (var v : pc.values()) walkNode(v, active);
            }
            default -> { }
        }
    }

    private void walkFunction(AppliedFunction af, StoreResolution active) {
        String funcName = TypeInfo.simpleName(af.function());

        // getAll and new create NEW resolutions from explicit mappings
        if ("getAll".equals(funcName)) {
            StoreResolution resolution = resolveGetAll(af);
            if (resolution != null) {
                resolutions.put(af, resolution);
            }
            for (var param : af.parameters()) walkNode(param, null);
            return;
        }
        if ("new".equals(funcName)) {
            StoreResolution resolution = resolveNew(af);
            if (resolution != null) {
                resolutions.put(af, resolution);
                active = resolution;
            }
        }

        // Walk parameters — propagate active resolution
        var params = af.parameters();
        for (var param : params) {
            walkNode(param, active);
        }

        // Downstream functions inherit source resolution.
        // If we don't have an active resolution yet, bubble up from source (first param).
        // This handles chains like getAll()->filter()->project() where getAll creates
        // the resolution and downstream functions inherit it.
        if (active == null && !params.isEmpty()) {
            active = resolutions.get(params.get(0));
        }
        if (active != null) {
            resolutions.put(af, active);
        }
    }

    // ==================== Resolve getAll / new ====================

    private StoreResolution resolveGetAll(AppliedFunction af) {
        if (!(af.parameters().get(0) instanceof PackageableElementPtr(String fullPath))) {
            return null;
        }
        String className = TypeInfo.simpleName(fullPath);
        ClassMapping mapping = normalized.findClassMapping(className).orElse(null);
        if (mapping == null) return null;
        return resolveClassMapping(mapping, className);
    }

    private StoreResolution resolveNew(AppliedFunction af) {
        TypeInfo info = typeResult.types().get(af);
        if (info == null || !info.instanceLiteral()) return null;
        if (!(info.type() instanceof GenericType.ClassType ct)) return null;
        String className = TypeInfo.simpleName(ct.qualifiedName());
        // ^Class — create identity mapping from model context
        PureClass pc = modelContext.findClass(className).orElse(null);
        if (pc == null) return null;
        return resolveClassMapping(RelationalMapping.identity(pc), className);
    }

    // ==================== ClassMapping → StoreResolution ====================

    private StoreResolution resolveClassMapping(ClassMapping mapping, String className) {
        return switch (mapping) {
            case RelationalMapping rm -> resolveRelational(rm, className);
            case PureClassMapping pcm -> resolveM2M(pcm);
        };
    }

    private StoreResolution resolveRelational(RelationalMapping rm, String className) {
        resolving.add(className);
        String tableName = rm.table().name();
        Map<String, String> propToCol = new LinkedHashMap<>();
        Map<String, StoreResolution.PropertyResolution> properties = new LinkedHashMap<>();

        for (PropertyMapping pm : rm.propertyMappings()) {
            String prop = pm.propertyName();
            String col = pm.columnName();
            propToCol.put(prop, col);

            var exprAccess = pm.expressionAccess();
            if (exprAccess.isPresent()) {
                var ea = exprAccess.get();
                properties.put(prop, new StoreResolution.PropertyResolution.Expression(
                        col, ea.jsonKey(), ea.castType()));
            } else if (pm.hasEnumMapping()) {
                properties.put(prop, new StoreResolution.PropertyResolution.Enum(
                        col, pm.enumMapping()));
            } else {
                properties.put(prop, new StoreResolution.PropertyResolution.Column(col));
            }
        }

        // Resolve association joins from model (not TypeInfo)
        Map<String, StoreResolution.JoinResolution> joins =
                resolveAssociationJoins(className, rm);

        // Resolved filter + distinct from NormalizedMapping (normalizer already converted ~filter)
        var filterExpr = normalized.findMappingExpression(className)
                .filter(e -> e instanceof ModelContext.MappingExpression.Relational)
                .map(e -> ((ModelContext.MappingExpression.Relational) e).filter())
                .orElse(null);

        resolving.remove(className);
        return new StoreResolution(
                tableName, propToCol, properties, joins,
                filterExpr, rm.nested(), rm.distinct());
    }

    private StoreResolution resolveM2M(PureClassMapping pcm) {
        // M2M chains are pre-resolved by MappingNormalizer —
        // pcm.sourceMapping() is already set.
        StoreResolution sourceResolution = pcm.sourceMapping() != null
                ? resolveClassMapping(pcm.sourceMapping(), pcm.sourceClassName())
                : null;

        String tableName = pcm.sourceTable().name();
        Map<String, String> propToCol = new LinkedHashMap<>();
        Map<String, StoreResolution.PropertyResolution> properties = new LinkedHashMap<>();

        for (var entry : pcm.propertyExpressions().entrySet()) {
            String prop = entry.getKey();
            properties.put(prop, new StoreResolution.PropertyResolution.M2MExpression(
                    entry.getValue(), sourceResolution));
            propToCol.put(prop, prop);
        }

        // Resolve M2M join references
        Map<String, StoreResolution.JoinResolution> joins =
                resolveM2MJoinReferences(pcm);

        return new StoreResolution(
                tableName, propToCol, properties, joins,
                pcm.filter(), false);
    }

    // ==================== Association / Join Resolution ====================

    /**
     * Resolves association joins for a relational mapping using model definitions.
     * Uses ModelContext.findAllAssociationNavigations to find association-contributed
     * properties (which are NOT in PureClass.properties()).
     */
    private Map<String, StoreResolution.JoinResolution> resolveAssociationJoins(
            String className, RelationalMapping sourceMapping) {
        Map<String, StoreResolution.JoinResolution> joins = new LinkedHashMap<>();
        String sourceTable = sourceMapping.table().name();

        // 1. Model associations (Association objects — properties not on the class itself)
        for (var entry : modelContext.findAllAssociationNavigations(className).entrySet()) {
            String propName = entry.getKey();
            var nav = entry.getValue();
            String targetClassName = nav.targetClassName();
            // Skip back-references to avoid infinite recursion (Person→Address→Person)
            if (resolving.contains(targetClassName)) continue;
            ClassMapping targetMapping = normalized.findClassMapping(targetClassName).orElse(null);
            if (targetMapping == null) continue;

            Join join = nav.join();
            StoreResolution targetResolution = resolveClassMapping(targetMapping, targetClassName);
            String targetTable = targetMapping.sourceTable().name();
            String sourceCol = join != null ? join.getColumnForTable(sourceTable) : null;
            String targetCol = join != null ? join.getColumnForTable(targetTable) : null;

            joins.put(propName, new StoreResolution.JoinResolution(
                    targetTable, sourceCol, targetCol,
                    nav.isToMany(), join, targetResolution));
        }

        // 2. Inline struct-array properties (to-many class-typed on the class itself).
        //    These are UNNEST candidates (join=null) for ^Class patterns.
        var pureClass = modelContext.findClass(className).orElse(null);
        if (pureClass != null) {
            for (var prop : pureClass.properties()) {
                if (joins.containsKey(prop.name())) continue; // already resolved via Association
                if (prop.multiplicity().isSingular()) continue;
                String typeName = prop.genericType().typeName();
                // Only class-typed properties — skip primitives
                if (modelContext.findClass(typeName).isEmpty()) continue;

                // Build identity resolution for the element class
                var elementClass = modelContext.findClass(typeName).get();
                var identityMapping = RelationalMapping.identity(elementClass);
                StoreResolution targetResolution = resolveClassMapping(identityMapping, typeName);
                joins.put(prop.name(), new StoreResolution.JoinResolution(
                        null, null, null, true, null, targetResolution));
            }
        }

        return joins;
    }

    /**
     * Resolves M2M @JoinName references to JoinResolutions.
     */
    private Map<String, StoreResolution.JoinResolution> resolveM2MJoinReferences(
            PureClassMapping pcm) {
        Map<String, StoreResolution.JoinResolution> joins = new LinkedHashMap<>();

        for (var entry : pcm.joinReferences().entrySet()) {
            String propName = entry.getKey();
            String joinName = entry.getValue();

            Join join = normalized.findJoin(joinName).orElse(null);
            if (join == null) continue;

            // Find the target class from the PureClass property
            PureClass targetClass = modelContext.findClass(pcm.targetClassName()).orElse(null);
            if (targetClass == null) continue;

            var prop = targetClass.findProperty(propName).orElse(null);
            if (prop == null) continue;

            String targetClassName = prop.genericType().typeName();
            boolean isToMany = !prop.multiplicity().isSingular();

            // NormalizedMapping already resolved M2M chains
            ClassMapping targetMapping = normalized.findClassMapping(targetClassName).orElse(null);
            if (targetMapping == null) continue;

            StoreResolution targetResolution = resolveClassMapping(targetMapping, targetClassName);
            String targetTable = targetMapping.sourceTable().name();
            String sourceCol = join.getColumnForTable(join.leftTable());
            String targetCol = join.getColumnForTable(join.rightTable());

            joins.put(propName, new StoreResolution.JoinResolution(
                    targetTable, sourceCol, targetCol,
                    isToMany, join, targetResolution));
        }
        return joins;
    }
}
