package com.gs.legend.compiler;

import com.gs.legend.ast.*;
import com.gs.legend.model.ModelContext;
import com.gs.legend.model.mapping.ClassMapping;
import com.gs.legend.model.mapping.PureClassMapping;
import com.gs.legend.model.mapping.RelationalMapping;
import com.gs.legend.model.m3.PureClass;
import com.gs.legend.model.store.PropertyMapping;
import com.gs.legend.compiler.checkers.ExtendChecker;
import com.gs.legend.plan.GenericType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
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
    private final IdentityHashMap<StoreResolution, String> storeClassNames = new IdentityHashMap<>();
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
        stampExtendOverrides();
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
        StoreResolution result = switch (mapping) {
            case RelationalMapping rm -> resolveRelational(rm, className);
            case PureClassMapping pcm -> resolveM2M(pcm);
        };
        if (result != null) storeClassNames.put(result, className);
        return result;
    }

    private StoreResolution resolveRelational(RelationalMapping rm, String className) {
        resolving.add(className);
        String tableName = rm.table().name();
        Map<String, String> propToCol = new LinkedHashMap<>();
        Map<String, StoreResolution.PropertyResolution> properties = new LinkedHashMap<>();

        // PMs are already view-resolved by MappingNormalizer Phase 1 — use directly.
        for (PropertyMapping pm : rm.propertyMappings()) {
            String prop = pm.propertyName();
            // Join-chain properties: extend(traverse(), ~propName:t|$t.COL) names them directly
            String col = pm.hasJoinChain()
                    ? pm.propertyName()
                    : pm.columnName();
            propToCol.put(prop, col);

            var exprAccess = pm.expressionAccess();
            if (pm.hasMultiJoinChains()) {
                // Multi-traverse: extend already computes the column
                properties.put(prop, new StoreResolution.PropertyResolution.Column(prop));
            } else if (pm.hasDynaExpression() && !pm.hasJoinChain()) {
                if (!rm.groupByColumns().isEmpty()) {
                    // ~groupBy active: DynaFunction is incorporated into the groupBy call
                    // in sourceRelation — output column is aliased as the property name.
                    properties.put(prop, new StoreResolution.PropertyResolution.Column(prop));
                } else {
                    properties.put(prop, new StoreResolution.PropertyResolution.DynaFunction(
                            pm.dynaExpression()));
                }
            } else if (exprAccess.isPresent()) {
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
                resolveAssociationJoins(className, tableName);

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

        // Resolve M2M association navigations by walking the source class's sourceRelation
        // extend nodes. For each M2M property matching $src.assocProp, find the traverse
        // embedded in the source class's extend node for that association property.
        Map<String, StoreResolution.JoinResolution> joins = new LinkedHashMap<>();
        var srcSourceRelation = normalized.findSourceRelation(pcm.sourceClassName());
        if (srcSourceRelation != null) {
            for (var propEntry : pcm.propertyExpressions().entrySet()) {
                String m2mPropName = propEntry.getKey();
                var expr = propEntry.getValue();

                // Match pattern: AppliedProperty(assocPropName, [Variable("src")])
                if (!(expr instanceof AppliedProperty ap)) continue;
                if (ap.parameters().size() != 1) continue;
                if (!(ap.parameters().get(0) instanceof Variable v)) continue;
                if (!"src".equals(v.name())) continue;

                String assocPropName = ap.property();

                // Find the traverse for this property in the source's sourceRelation
                AppliedFunction traverseAf = findTraverseForProp(srcSourceRelation, assocPropName);
                if (traverseAf == null) continue;

                // Look up association for target class + multiplicity
                var nav = modelContext.findAssociationByProperty(pcm.sourceClassName(), assocPropName)
                        .orElse(null);
                if (nav == null) continue;

                // Determine isToMany from M2M target class property multiplicity
                boolean isToMany = nav.isToMany();
                var targetClassOpt = modelContext.findClass(pcm.targetClassName());
                if (targetClassOpt.isPresent()) {
                    var propOpt = targetClassOpt.get().findProperty(m2mPropName);
                    if (propOpt.isPresent()) {
                        isToMany = !propOpt.get().multiplicity().isSingular();
                    }
                }

                ClassMapping targetMapping = normalized.findClassMapping(nav.targetClassName())
                        .orElseThrow(() -> new PureCompileException(
                                "M2M Association navigation targets class '" + nav.targetClassName()
                                        + "' which has no mapping in scope"));
                StoreResolution targetResolution = resolveClassMapping(targetMapping, nav.targetClassName());
                String targetTable = targetMapping.sourceTable().name();
                joins.put(m2mPropName, buildJoinResolution(
                        targetTable, isToMany, traverseAf, targetResolution));
            }
        }

        return new StoreResolution(
                tableName, propToCol, properties, joins,
                pcm.filter(), false);
    }

    // ==================== Association / Join Resolution ====================

    /**
     * Resolves association joins for a relational mapping by walking the sourceRelation's
     * extend nodes. Each association extend has a ColSpec with fn1=traverse (0-param lambda).
     * Also discovers inline struct-array properties (UNNEST candidates) from the model.
     */
    private Map<String, StoreResolution.JoinResolution> resolveAssociationJoins(
            String className, String tableName) {
        Map<String, StoreResolution.JoinResolution> joins = new LinkedHashMap<>();

        // 1. Walk extend nodes in sourceRelation to find association extends
        var sourceRelation = normalized.findSourceRelation(className);
        if (sourceRelation != null) {
            for (AppliedFunction extendAf : findExtendNodes(sourceRelation)) {
                for (int i = 1; i < extendAf.parameters().size(); i++) {
                    if (!(extendAf.parameters().get(i) instanceof ClassInstance ci)) continue;
                    if (!(ci.value() instanceof ColSpec cs)) continue;
                    if (!ExtendChecker.isAssociationExtend(cs)) continue;

                    String propName = cs.name();
                    // Extract traverse call from fn1's body
                    AppliedFunction traverseAf = (AppliedFunction) cs.function1().body().get(0);

                    // Look up association for target class and multiplicity
                    var nav = modelContext.findAssociationByProperty(className, propName)
                            .orElse(null);
                    if (nav == null) continue;

                    ClassMapping targetMapping = normalized.findClassMapping(nav.targetClassName())
                            .orElseThrow(() -> new PureCompileException(
                                    "Association join for property '" + propName + "' targets class '"
                                            + nav.targetClassName() + "' which has no mapping in scope"));

                    StoreResolution targetResolution;
                    if (resolving.contains(nav.targetClassName())) {
                        targetResolution = buildShallowResolution(targetMapping);
                    } else {
                        targetResolution = resolveClassMapping(targetMapping, nav.targetClassName());
                    }
                    String targetTable = targetMapping.sourceTable().name();

                    joins.put(propName, buildJoinResolution(
                            targetTable, nav.isToMany(), traverseAf, targetResolution));
                }
            }
        }

        // 2. Embedded extends: fn1 is 0-param lambda wrapping ColSpecArray.
        //    No JOIN — sub-properties resolve to parent table columns.
        if (sourceRelation != null) {
            for (AppliedFunction extendAf : findExtendNodes(sourceRelation)) {
                for (int i = 1; i < extendAf.parameters().size(); i++) {
                    if (!(extendAf.parameters().get(i) instanceof ClassInstance ci)) continue;
                    if (!(ci.value() instanceof ColSpec cs)) continue;
                    if (!ExtendChecker.isEmbeddedExtend(cs)) continue;

                    String propName = cs.name();
                    // Extract sub-ColSpecs from the ColSpecArray
                    ClassInstance innerCI = (ClassInstance) cs.function1().body().get(0);
                    ColSpecArray subArray = (ColSpecArray) innerCI.value();

                    // Build embedded sub-property resolutions
                    Map<String, String> subPropToCol = new LinkedHashMap<>();
                    Map<String, StoreResolution.PropertyResolution> subProperties = new LinkedHashMap<>();
                    for (ColSpec sub : subArray.colSpecs()) {
                        // fn1 body is AppliedProperty(columnName, [Variable("row")])
                        var subBody = sub.function1().body().get(0);
                        if (subBody instanceof AppliedProperty ap) {
                            String colName = ap.property();
                            subPropToCol.put(sub.name(), colName);
                            subProperties.put(sub.name(),
                                    new StoreResolution.PropertyResolution.EmbeddedColumn(colName));
                        }
                    }

                    // Otherwise merge: if association JoinResolution already exists for this
                    // property, merge embedded columns INTO the association's target resolution.
                    // Embedded sub-properties use parent alias; others use join alias.
                    var existingJoin = joins.get(propName);
                    if (existingJoin != null && !existingJoin.embedded()) {
                        // Merge: start with association's target, override with EmbeddedColumn
                        var assocTarget = existingJoin.targetResolution();
                        var mergedPropToCol = new LinkedHashMap<>(assocTarget.propertyToColumn());
                        mergedPropToCol.putAll(subPropToCol);
                        var mergedProperties = new LinkedHashMap<>(assocTarget.properties());
                        mergedProperties.putAll(subProperties);
                        var mergedResolution = new StoreResolution(
                                assocTarget.tableName(), mergedPropToCol, mergedProperties,
                                assocTarget.joins(), assocTarget.filterExpr(), assocTarget.nested());
                        joins.put(propName, new StoreResolution.JoinResolution(
                                existingJoin.targetTable(), existingJoin.sourceParam(),
                                existingJoin.targetParam(), existingJoin.isToMany(),
                                existingJoin.joinCondition(), existingJoin.sourceColumns(),
                                mergedResolution, false));
                    } else {
                        // Pure embedded (no fallback join)
                        var embeddedResolution = new StoreResolution(
                                tableName, subPropToCol, subProperties, Map.of(),
                                null, false);
                        joins.put(propName, new StoreResolution.JoinResolution(
                                null, null, null, false, null, Set.of(), embeddedResolution, true));
                    }
                }
            }
        }

        // 4. Traverse extends: extend(source, traverse(...), ~[col:{src,tgt|$tgt.COL}])
        //    Join-chain properties that require an additional JOIN. The traverse expression
        //    encodes the join condition; ColSpecs encode the projected columns on the target table.
        if (sourceRelation != null) {
            for (AppliedFunction extendAf : findExtendNodes(sourceRelation)) {
                if (!isTraverseExtend(extendAf)) continue;

                AppliedFunction traverseAf = (AppliedFunction) extendAf.parameters().get(1);
                String targetTable = extractTraverseTargetTable(traverseAf);
                if (targetTable == null) continue;

                List<ColSpec> colSpecs = extractTraverseColSpecs(extendAf);
                if (colSpecs.isEmpty()) continue;

                // Build targetResolution mapping propName → physicalCol for all ColSpecs
                Map<String, String> subPropToCol = new LinkedHashMap<>();
                Map<String, StoreResolution.PropertyResolution> subProperties = new LinkedHashMap<>();
                for (ColSpec cs : colSpecs) {
                    String physCol = extractTraversePhysicalColumn(cs);
                    if (physCol != null) {
                        subPropToCol.put(cs.name(), physCol);
                        subProperties.put(cs.name(),
                                new StoreResolution.PropertyResolution.Column(physCol));
                    } else if (cs.function1() != null && !cs.function1().body().isEmpty()) {
                        // DynaFunction body (e.g., concat($src.A, $tgt.B))
                        subPropToCol.put(cs.name(), cs.name());
                        subProperties.put(cs.name(),
                                new StoreResolution.PropertyResolution.DynaFunction(
                                        cs.function1().body().get(0)));
                    }
                }

                var targetResolution = new StoreResolution(
                        targetTable, subPropToCol, subProperties, Map.of(), null, false);
                var joinRes = buildJoinResolution(
                        targetTable, false, traverseAf, targetResolution);

                for (ColSpec cs : colSpecs) {
                    joins.put(cs.name(), joinRes);
                }
            }
        }

        // 3. Inline struct-array properties (to-many class-typed on the class itself).
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

    /**
     * Finds the traverse AppliedFunction for a given property name by walking
     * the sourceRelation's extend nodes. Returns null if not found.
     */
    private static AppliedFunction findTraverseForProp(ValueSpecification sourceRel, String propName) {
        for (AppliedFunction extendAf : findExtendNodes(sourceRel)) {
            for (int i = 1; i < extendAf.parameters().size(); i++) {
                if (!(extendAf.parameters().get(i) instanceof ClassInstance ci)) continue;
                if (!(ci.value() instanceof ColSpec cs)) continue;
                if (!cs.name().equals(propName)) continue;
                if (!ExtendChecker.isAssociationExtend(cs)) continue;
                return (AppliedFunction) cs.function1().body().get(0);
            }
        }
        return null;
    }

    // ==================== Join Condition Helpers ====================

    /**
     * Builds a JoinResolution from a traverse expression.
     * Handles both inside-extend 2-param traverse {@code traverse(targetRef, {prev,hop|cond})}
     * and chained 3-param traverse {@code traverse(prev_traverse, targetRef, {prev,hop|cond})}.
     * Lambda is always the last parameter.
     * TypeInfo already stamped by TypeChecker (via GetAllChecker → ExtendChecker).
     */
    private StoreResolution.JoinResolution buildJoinResolution(
            String targetTable, boolean isToMany,
            ValueSpecification joinTraversal, StoreResolution targetResolution) {
        if (!(joinTraversal instanceof AppliedFunction traverseAf)) {
            throw new IllegalArgumentException(
                    "Expected traverse() AppliedFunction, got: " + joinTraversal.getClass().getSimpleName());
        }
        // Lambda is always last param: index 1 for 2-param, index 2 for 3-param
        int lambdaIdx = traverseAf.parameters().size() - 1;
        if (!(traverseAf.parameters().get(lambdaIdx) instanceof LambdaFunction lambda)) {
            throw new IllegalArgumentException(
                    "Expected LambdaFunction as traverse param[" + lambdaIdx + "], got: "
                            + traverseAf.parameters().get(lambdaIdx).getClass().getSimpleName());
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

    // ==================== Extend Override Stamping ====================

    /**
     * Stamps {@link StoreResolution.ExtendOverride} on sourceRelation extend nodes
     * whose columns are not all used by the query.
     *
     * <p>Reads {@code typeResult.classPropertyAccesses()} (populated by TypeChecker)
     * to determine which model properties are referenced. Intersects with each
     * extend node's colSpec names to produce column-level cancellation.
     */
    private void stampExtendOverrides() {
        // Collect overrides first, then apply — avoids ConcurrentModificationException
        var overrides = new IdentityHashMap<ValueSpecification, StoreResolution>();

        // Iterate storeClassNames (all resolved stores), not resolutions (user-query AST only).
        // M2M source stores are in storeClassNames but not in resolutions.
        for (var entry : storeClassNames.entrySet()) {
            StoreResolution store = entry.getKey();
            if (store.sourceRelation() == null) continue;

            String className = entry.getValue();

            Set<String> usedProps = typeResult.classPropertyAccesses()
                    .getOrDefault(className, Set.of());

            for (AppliedFunction extendAf : findExtendNodes(store.sourceRelation())) {
                Set<String> extendCols = extractColSpecNames(extendAf);
                if (extendCols.isEmpty()) continue;

                Set<String> active = new HashSet<>(extendCols);
                active.retainAll(usedProps);
                if (active.size() == extendCols.size()) continue; // all used — no override needed

                overrides.put(extendAf, StoreResolution.forExtend(
                        new StoreResolution.ExtendOverride(active)));
            }
        }

        resolutions.putAll(overrides);
    }

    // ==================== Traverse Extend Helpers ====================

    /** True if the extend node is a 3-param traverse extend: extend(source, traverse(...), colSpecCI). */
    private static boolean isTraverseExtend(AppliedFunction extendAf) {
        if (extendAf.parameters().size() < 3) return false;
        return extendAf.parameters().get(1) instanceof AppliedFunction af
                && "traverse".equals(TypeInfo.simpleName(af.function()));
    }

    /** Extracts the terminal target table name from a traverse expression. */
    private static String extractTraverseTargetTable(AppliedFunction traverseAf) {
        // Table reference is always at index size-2 (before the lambda at last index)
        int tableRefIdx = traverseAf.parameters().size() - 2;
        var tableRef = traverseAf.parameters().get(tableRefIdx);
        if (tableRef instanceof AppliedFunction af
                && "tableReference".equals(TypeInfo.simpleName(af.function()))) {
            if (!af.parameters().isEmpty() && af.parameters().get(0) instanceof CString cs) {
                return cs.value();
            }
        }
        return null;
    }

    /** Extracts ColSpecs from a traverse extend's last ClassInstance parameter. */
    private static List<ColSpec> extractTraverseColSpecs(AppliedFunction extendAf) {
        // ColSpec ClassInstance is the last param
        var lastParam = extendAf.parameters().get(extendAf.parameters().size() - 1);
        if (lastParam instanceof ClassInstance ci) {
            if (ci.value() instanceof ColSpec cs) return List.of(cs);
            if (ci.value() instanceof ColSpecArray arr) return arr.colSpecs();
        }
        return List.of();
    }

    /**
     * Extracts the physical column name from a traverse ColSpec's lambda body.
     * For simple column access ({src,tgt|$tgt.COL}), returns "COL".
     * Returns null for DynaFunction/complex expressions.
     */
    private static String extractTraversePhysicalColumn(ColSpec cs) {
        if (cs.function1() == null) return null;
        var body = cs.function1().body();
        if (body == null || body.isEmpty()) return null;
        if (body.get(0) instanceof AppliedProperty ap) {
            return ap.property();
        }
        return null;
    }

    /** Walks the sourceRelation chain to find extend nodes (outermost first). */
    private static List<AppliedFunction> findExtendNodes(ValueSpecification sourceRel) {
        var result = new ArrayList<AppliedFunction>();
        var cur = sourceRel;
        while (cur instanceof AppliedFunction af
                && "extend".equals(TypeInfo.simpleName(af.function()))) {
            result.add(af);
            cur = af.parameters().get(0);
        }
        return result;
    }

    /** Extracts colSpec names from an extend node's ClassInstance parameter. */
    private static Set<String> extractColSpecNames(AppliedFunction extendAf) {
        var names = new HashSet<String>();
        // extend(source, [traverse], colSpecCI) — colSpec is the last param
        for (int i = 1; i < extendAf.parameters().size(); i++) {
            if (extendAf.parameters().get(i) instanceof ClassInstance ci) {
                if (ci.value() instanceof ColSpec cs) {
                    names.add(cs.name());
                } else if (ci.value() instanceof ColSpecArray arr) {
                    for (ColSpec cs : arr.colSpecs()) names.add(cs.name());
                }
            }
        }
        return names;
    }
}
