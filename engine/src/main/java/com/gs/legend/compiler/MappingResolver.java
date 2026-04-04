package com.gs.legend.compiler;

import com.gs.legend.ast.*;
import com.gs.legend.model.ModelContext;
import com.gs.legend.model.mapping.ClassMapping;
import com.gs.legend.model.mapping.PureClassMapping;
import com.gs.legend.model.mapping.RelationalMapping;
import com.gs.legend.model.m3.PureClass;
import com.gs.legend.model.store.PropertyMapping;
import com.gs.legend.plan.GenericType;

import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

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
    private final Set<String> resolving = new HashSet<>();

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
            // Join-chain properties: extend(traverse(), ~propName:t|$t.COL) names them directly
            String col = pm.hasJoinChain()
                    ? pm.propertyName()
                    : pm.columnName();
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

        // Resolve association joins from NormalizedMapping (pre-resolved by MappingNormalizer)
        Map<String, StoreResolution.JoinResolution> joins =
                resolveAssociationJoins(className);

        // sourceRelation from NormalizedMapping (dedicated accessor — no MappingExpression needed)
        var sourceRelation = normalized.findSourceRelation(className);

        resolving.remove(className);
        return new StoreResolution(
                tableName, propToCol, properties, joins,
                null, rm.nested(), sourceRelation);
    }

    private StoreResolution resolveM2M(PureClassMapping pcm) {
        // M2M chains are pre-resolved by MappingNormalizer —
        // pcm.sourceMapping() MUST be set by this point.
        if (pcm.sourceMapping() == null) {
            throw new PureCompileException(
                    "M2M mapping for '" + pcm.targetClassName()
                            + "' has unresolved source '" + pcm.sourceClassName()
                            + "' — MappingNormalizer should have resolved this");
        }
        StoreResolution sourceResolution = resolveClassMapping(pcm.sourceMapping(), pcm.sourceClassName());

        String tableName = pcm.sourceTable().name();
        Map<String, String> propToCol = new LinkedHashMap<>();
        Map<String, StoreResolution.PropertyResolution> properties = new LinkedHashMap<>();

        for (var entry : pcm.propertyExpressions().entrySet()) {
            String prop = entry.getKey();
            properties.put(prop, new StoreResolution.PropertyResolution.M2MExpression(
                    entry.getValue(), sourceResolution));
            propToCol.put(prop, prop);
        }

        // Resolve Association navigations from pre-resolved NormalizedMapping data
        Map<String, StoreResolution.JoinResolution> joins = new LinkedHashMap<>();
        for (var navEntry : normalized.findM2MAssociationNavigations(pcm.targetClassName()).entrySet()) {
            String m2mPropName = navEntry.getKey();
            var info = navEntry.getValue();
            ClassMapping targetMapping = normalized.findClassMapping(info.targetClassName())
                    .orElseThrow(() -> new PureCompileException(
                            "M2M Association navigation targets class '" + info.targetClassName()
                                    + "' which has no mapping in scope"));
            StoreResolution targetResolution = resolveClassMapping(targetMapping, info.targetClassName());
            String targetTable = targetMapping.sourceTable().name();
            joins.put(m2mPropName, buildJoinResolution(
                    targetTable, info.isToMany(), info.traversal(), targetResolution));
        }

        return new StoreResolution(
                tableName, propToCol, properties, joins,
                pcm.filter(), false);
    }

    // ==================== Association / Join Resolution ====================

    /**
     * Resolves association joins for a relational mapping.
     * Reads pre-resolved AssociationJoinInfo from NormalizedMapping — no model queries needed.
     * Also discovers inline struct-array properties (UNNEST candidates) from the model.
     */
    private Map<String, StoreResolution.JoinResolution> resolveAssociationJoins(
            String className) {
        Map<String, StoreResolution.JoinResolution> joins = new LinkedHashMap<>();

        // 1. Pre-resolved association joins (from MappingNormalizer via NormalizedMapping)
        for (var entry : normalized.findAssociationJoins(className).entrySet()) {
            String propName = entry.getKey();
            var info = entry.getValue();
            ClassMapping targetMapping = normalized.findClassMapping(info.targetClassName())
                    .orElseThrow(() -> new PureCompileException(
                            "Association join for property '" + propName + "' targets class '"
                                    + info.targetClassName() + "' which has no mapping in scope"
                                    + " — MappingNormalizer should have excluded this"));

            StoreResolution targetResolution;
            if (resolving.contains(info.targetClassName())) {
                // Self-join or back-reference: build shallow resolution (same table/columns,
                // no recursive association joins) to avoid infinite recursion.
                targetResolution = buildShallowResolution(targetMapping);
            } else {
                targetResolution = resolveClassMapping(targetMapping, info.targetClassName());
            }
            String targetTable = targetMapping.sourceTable().name();

            joins.put(propName, buildJoinResolution(
                    targetTable, info.isToMany(), info.traversal(), targetResolution));
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
                        null, null, null, true, null, Set.of(), targetResolution));
            }
        }

        return joins;
    }

    /**
     * Builds a shallow StoreResolution from a ClassMapping — table name and property-to-column
     * mappings only, no recursive association join resolution. Used for self-joins and
     * back-references where the target class is already being resolved (in {@code resolving}).
     */
    private StoreResolution buildShallowResolution(ClassMapping mapping) {
        if (!(mapping instanceof RelationalMapping rm)) {
            throw new PureCompileException(
                    "Self-join/back-reference target must be relational, got: " + mapping.getClass().getSimpleName());
        }
        String tableName = rm.table().name();
        Map<String, String> propToCol = new LinkedHashMap<>();
        Map<String, StoreResolution.PropertyResolution> properties = new LinkedHashMap<>();
        for (PropertyMapping pm : rm.propertyMappings()) {
            String prop = pm.propertyName();
            String col = pm.hasJoinChain() ? pm.propertyName() : pm.columnName();
            propToCol.put(prop, col);
            properties.put(prop, new StoreResolution.PropertyResolution.Column(col));
        }
        return new StoreResolution(tableName, propToCol, properties, Map.of(),
                null, rm.nested(), null);
    }

    // ==================== Join Condition Helpers ====================

    /**
     * Builds a JoinResolution from a traverse expression.
     * The joinTraversal is: {@code traverse(tableRef(src), tableRef(tgt), {src, tgt | body})}.
     * Unwraps the lambda to extract param names and condition body.
     * TypeInfo already stamped by TypeChecker (via GetAllChecker → TraverseChecker).
     */
    private StoreResolution.JoinResolution buildJoinResolution(
            String targetTable, boolean isToMany,
            ValueSpecification joinTraversal, StoreResolution targetResolution) {
        // Unwrap traverse(source, target, lambda) → extract lambda
        if (!(joinTraversal instanceof AppliedFunction traverseAf)) {
            throw new IllegalArgumentException(
                    "Expected traverse() AppliedFunction, got: " + joinTraversal.getClass().getSimpleName());
        }
        if (!(traverseAf.parameters().get(2) instanceof LambdaFunction lambda)) {
            throw new IllegalArgumentException(
                    "Expected LambdaFunction as traverse param[2], got: "
                            + traverseAf.parameters().get(2).getClass().getSimpleName());
        }

        String sourceParam = lambda.parameters().get(0).name();
        String targetParam = lambda.parameters().get(1).name();
        ValueSpecification condBody = lambda.body().get(0);

        Set<String> sourceColumns = extractSourceColumns(condBody, sourceParam);

        return new StoreResolution.JoinResolution(
                targetTable, sourceParam, targetParam, isToMany,
                condBody, sourceColumns, targetResolution);
    }

    /**
     * Extracts column names referenced on the source side of a join condition.
     * Finds all AppliedProperty nodes whose Variable matches sourceParam.
     */
    private static Set<String> extractSourceColumns(ValueSpecification vs, String sourceParam) {
        Set<String> columns = new HashSet<>();
        collectSourceColumns(vs, sourceParam, columns);
        return columns;
    }

    private static void collectSourceColumns(ValueSpecification vs, String sourceParam, Set<String> cols) {
        switch (vs) {
            case AppliedProperty ap -> {
                if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof Variable v
                        && v.name().equals(sourceParam)) {
                    cols.add(ap.property());
                }
            }
            case AppliedFunction af -> {
                for (var p : af.parameters()) collectSourceColumns(p, sourceParam, cols);
            }
            case CString ignored -> {} // literal — no columns
            case CInteger ignored -> {}
            case CFloat ignored -> {}
            case CDecimal ignored -> {}
            case CBoolean ignored -> {}
            default -> throw new IllegalArgumentException(
                    "Unexpected node in join condition ValueSpec: " + vs.getClass().getSimpleName());
        }
    }
}
