package com.gs.legend.compiler;

import com.gs.legend.ast.*;
import com.gs.legend.model.ModelContext;
import com.gs.legend.model.SymbolTable;
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
            // Propagate resolution from inlined body to original node.
            // Enables query-returning user functions: test::adults() inlines to
            // getAll()->filter(), and the StoreResolution bubbles to the call site.
            StoreResolution inlinedRes = resolutions.get(info.inlinedBody());
            if (inlinedRes != null && !resolutions.containsKey(vs)) {
                resolutions.put(vs, inlinedRes);
            }
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
        String funcName = SymbolTable.extractSimpleName(af.function());

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
        // Skip identity stores (instanceLiteral) — they're synthetic for struct literals
        // and must not leak to parent functions like find(), eq(), etc.
        if (active == null && !params.isEmpty()) {
            TypeInfo firstParamInfo = typeResult.types().get(params.get(0));
            if (firstParamInfo == null || !firstParamInfo.instanceLiteral()) {
                active = resolutions.get(params.get(0));
            }
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
        String className = fullPath;
        ClassMapping mapping = normalized.findClassMapping(className).orElse(null);
        if (mapping == null) return null;
        return resolveClassMapping(mapping, className);
    }

    private StoreResolution resolveNew(AppliedFunction af) {
        TypeInfo info = typeResult.types().get(af);
        if (info == null || !info.instanceLiteral()) return null;
        if (!(info.type() instanceof GenericType.ClassType ct)) return null;
        String className = ct.qualifiedName();
        // ^Class — create identity mapping from model context
        PureClass pc = modelContext.findClass(className).orElse(null);
        if (pc == null) return null;
        return resolveClassMapping(RelationalMapping.identity(pc), className);
    }

    // ==================== ClassMapping → StoreResolution ====================

    private StoreResolution resolveClassMapping(ClassMapping mapping, String className) {
        // Canonicalize to FQN — matches TypeChecker's classPropertyAccesses keys
        String fqn = modelContext.findClass(className)
                .map(PureClass::qualifiedName)
                .orElse(className);
        StoreResolution result = switch (mapping) {
            case RelationalMapping rm -> resolveRelational(rm, fqn);
            case PureClassMapping pcm -> resolveM2M(pcm);
        };
        if (result != null) storeClassNames.put(result, fqn);
        return result;
    }

    private StoreResolution resolveRelational(RelationalMapping rm, String className) {
        resolving.add(className);
        String tableName = rm.table().name();
        Map<String, String> propToCol = new LinkedHashMap<>();
        Map<String, StoreResolution.PropertyResolution> properties = new LinkedHashMap<>();

        // 1. Default: simple column PMs → Column(physicalCol).
        //    Join-chain PMs → Column(propName) (traverse extend names them directly).
        for (PropertyMapping pm : rm.propertyMappings()) {
            String prop = pm.propertyName();
            String col = pm.hasJoinChain() ? pm.propertyName() : pm.columnName();
            propToCol.put(prop, col);
            properties.put(prop, new StoreResolution.PropertyResolution.Column(col));
        }

        // 2. Walk sourceSpec extends ONCE — override computed properties with
        //    DynaFunction(expression) derived from the extend's lambda body.
        //    Also resolves association joins, embedded extends, and traverse extends.
        var sourceSpec = normalized.findSourceSpec(className);
        Map<String, StoreResolution.JoinResolution> joins =
                resolveSourceSpecExtends(className, tableName, sourceSpec, propToCol, properties);

        resolving.remove(className);
        var store = new StoreResolution(
                tableName, propToCol, properties, joins,
                null, rm.nested(), sourceSpec);
        // Thread sourceUrl for external data sources (JSON data: / file: URIs)
        if (rm.sourceUrl() != null) {
            store = new StoreResolution(tableName, propToCol, properties, joins,
                    null, rm.nested(), sourceSpec, null, rm.sourceUrl());
        }
        return store;
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

        // 1. Resolve source → sourceStore
        StoreResolution sourceStore = resolveClassMapping(pcm.sourceMapping(), pcm.sourceClassName());

        // 2. Inherit source's properties + propToCol (DynaFunction bodies resolve through these)
        Map<String, String> propToCol = new LinkedHashMap<>(sourceStore.propertyToColumn());
        Map<String, StoreResolution.PropertyResolution> properties = new LinkedHashMap<>(sourceStore.properties());
        Map<String, StoreResolution.JoinResolution> joins = new LinkedHashMap<>();

        // 3. For each M2M property expression: classify as Column, DynaFunction, or association
        for (var entry : pcm.propertyExpressions().entrySet()) {
            String prop = entry.getKey();
            var expr = entry.getValue();

            if (isSimplePropertyAccess(expr)) {
                String srcProp = ((AppliedProperty) expr).property();
                // Check if it's an association navigation
                var srcJoin = sourceStore.hasJoins() ? sourceStore.joins().get(srcProp) : null;
                if (srcJoin != null) {
                    // Distinguish scalar traverse columns from association joins.
                    // Scalar traverse: target property is primitive (String, Integer, etc.)
                    //   → resolve as Column(srcProp); the relational sourceSpec already
                    //     handles the LEFT JOIN + column alias via traverse extend.
                    // Association: target property is class-typed (PureClass)
                    //   → forward as JoinResolution for graphFetch correlated subquery.
                    boolean isScalarTraverse = true;
                    var targetClassOpt = modelContext.findClass(pcm.targetClassName());
                    if (targetClassOpt.isPresent()) {
                        var propOpt = targetClassOpt.get().findProperty(prop);
                        if (propOpt.isPresent()
                                && propOpt.get().genericType() instanceof PureClass) {
                            isScalarTraverse = false;
                        }
                    }

                    if (isScalarTraverse) {
                        // Traverse column: relational sourceSpec chain already generates
                        // the LEFT JOIN. Just reference the column alias.
                        properties.put(prop, new StoreResolution.PropertyResolution.Column(srcProp));
                    } else {
                        // Association join: forward with M2M TARGET class multiplicity
                        // (e.g., PersonWithSingleAddress.address is [0..1] even though
                        // RawPerson.rawAddresses is [*])
                        boolean isToMany = srcJoin.isToMany();
                        if (targetClassOpt.isPresent()) {
                            var propOpt = targetClassOpt.get().findProperty(prop);
                            if (propOpt.isPresent()) {
                                isToMany = !propOpt.get().multiplicity().isSingular();
                            }
                        }
                        if (isToMany != srcJoin.isToMany()) {
                            srcJoin = new StoreResolution.JoinResolution(
                                    srcJoin.targetTable(), srcJoin.sourceParam(), srcJoin.targetParam(),
                                    isToMany, srcJoin.joinCondition(), srcJoin.sourceColumns(),
                                    srcJoin.targetResolution(), srcJoin.embedded());
                        }
                        joins.put(prop, srcJoin);
                    }
                } else {
                    // Simple property → inherit source's resolution (Column or DynaFunction)
                    var srcRes = sourceStore.resolveProperty(srcProp);
                    if (srcRes != null) {
                        properties.put(prop, srcRes);
                    } else {
                        String col = sourceStore.columnFor(srcProp);
                        properties.put(prop, new StoreResolution.PropertyResolution.Column(
                                col != null ? col : srcProp));
                    }
                }
            } else {
                // Complex expression → DynaFunction (body stays in class-space,
                // resolved at SQL time against the inherited properties)
                properties.put(prop, new StoreResolution.PropertyResolution.DynaFunction(expr));
            }
            propToCol.put(prop, prop);
        }

        // 4. Get M2M sourceSpec and stamp stores on chain nodes
        var sourceSpec = normalized.findSourceSpec(pcm.targetClassName());
        if (sourceSpec != null) {
            stampM2MSourceSpecStores(sourceSpec, sourceStore, properties, propToCol, joins);
        }

        return new StoreResolution(
                sourceStore.tableName(), propToCol, properties, joins,
                null, false, sourceSpec);
    }

    /**
     * Checks if a ValueSpecification is a simple property access: {@code $src.propName}.
     */
    private boolean isSimplePropertyAccess(ValueSpecification expr) {
        return expr instanceof AppliedProperty ap
                && ap.parameters().size() == 1
                && ap.parameters().get(0) instanceof Variable;
    }

    /**
     * Stamps StoreResolution on every AppliedFunction node in the M2M sourceSpec chain.
     * The chain is linear (each node's first param is the next node down).
     * Inner getAll gets sourceStore (so PlanGen follows sourceStore.sourceSpec());
     * all other nodes get the M2M target store.
     */
    private void stampM2MSourceSpecStores(
            ValueSpecification sourceSpec,
            StoreResolution sourceStore,
            Map<String, StoreResolution.PropertyResolution> properties,
            Map<String, String> propToCol,
            Map<String, StoreResolution.JoinResolution> joins) {

        var targetStore = new StoreResolution(
                sourceStore.tableName(), propToCol, properties, joins,
                null, false, sourceStore.sourceSpec());

        ValueSpecification current = sourceSpec;
        while (current instanceof AppliedFunction af) {
            // Leaf (no params or first param is not AF) → stamp sourceStore
            // so PlanGen can follow sourceStore.sourceSpec() for the physical chain.
            if (af.parameters().isEmpty()
                    || !(af.parameters().get(0) instanceof AppliedFunction)) {
                resolutions.put(af, sourceStore);
                return;
            }
            resolutions.put(af, targetStore);
            current = af.parameters().get(0);
        }
    }

    // ==================== Association / Join Resolution ====================

    /**
     * Single-pass walk of sourceSpec extend nodes. Handles all extend types:
     * <ul>
     *   <li><b>Scalar extends</b> — 1-param lambda (dynaFunction, enum, expression access).
     *       Overrides the default Column resolution with DynaFunction(lambdaBody),
     *       deriving the expression from the sourceSpec instead of from PropertyMapping.</li>
     *   <li><b>Association extends</b> — 0-param lambda wrapping traverse(). Builds JoinResolution.</li>
     *   <li><b>Embedded extends</b> — 0-param lambda wrapping ColSpecArray. Builds embedded JoinResolution.</li>
     *   <li><b>Traverse extends</b> — 3-param extend(source, traverse, colSpecs). Builds JoinResolution.</li>
     * </ul>
     *
     * @param properties  mutable — scalar extends override Column entries with DynaFunction
     * @param propToCol   mutable — scalar extends override col with propName
     */
    private Map<String, StoreResolution.JoinResolution> resolveSourceSpecExtends(
            String className, String tableName, ValueSpecification sourceSpec,
            Map<String, String> propToCol,
            Map<String, StoreResolution.PropertyResolution> properties) {

        Map<String, StoreResolution.JoinResolution> joins = new LinkedHashMap<>();
        if (sourceSpec == null) {
            addStructArrayJoins(className, joins);
            return joins;
        }

        // Demand-driven: only resolve associations the query actually navigates
        Set<String> neededAssocs = typeResult.associationNavigations()
                .getOrDefault(className, Set.of());

        for (AppliedFunction extendAf : findExtendNodes(sourceSpec)) {

            // --- Traverse extends: extend(source, traverse(...), colSpecCI) ---
            if (isTraverseExtend(extendAf)) {
                resolveTraverseExtend(extendAf, joins);
                continue;
            }

            // --- ColSpec-based extends (scalar, association, embedded) ---
            for (int i = 1; i < extendAf.parameters().size(); i++) {
                if (!(extendAf.parameters().get(i) instanceof ClassInstance ci)) continue;

                // Single ColSpec
                if (ci.value() instanceof ColSpec cs) {
                    resolveColSpec(cs, className, tableName, neededAssocs,
                            propToCol, properties, joins);
                }
                // ColSpecArray — batch of ColSpecs
                else if (ci.value() instanceof ColSpecArray csa) {
                    for (ColSpec cs : csa.colSpecs()) {
                        resolveColSpec(cs, className, tableName, neededAssocs,
                                propToCol, properties, joins);
                    }
                }
            }
        }

        // Inline struct-array properties (UNNEST candidates)
        addStructArrayJoins(className, joins);

        return joins;
    }

    /**
     * Resolves a single ColSpec from a sourceSpec extend node.
     * Classifies the ColSpec and updates the appropriate output map.
     */
    private void resolveColSpec(ColSpec cs, String className, String tableName,
                                Set<String> neededAssocs,
                                Map<String, String> propToCol,
                                Map<String, StoreResolution.PropertyResolution> properties,
                                Map<String, StoreResolution.JoinResolution> joins) {
        String propName = cs.name();

        // Association extend: 0-param lambda wrapping traverse()
        if (ExtendChecker.isAssociationExtend(cs)) {
            if (!neededAssocs.contains(propName)) return;
            resolveAssociationExtend(cs, className, joins);
            return;
        }

        // Embedded extend: 0-param lambda wrapping ColSpecArray
        if (ExtendChecker.isEmbeddedExtend(cs)) {
            resolveEmbeddedExtend(cs, tableName, joins);
            return;
        }

        // Scalar extend: exactly 1-param lambda ({row|expr}) with computed expression body.
        // Override the default Column resolution with DynaFunction(expression).
        // Multi-param lambdas ({t1,t2|...}) reference join aliases — keep as Column(propName)
        // since the multi-join extend already computes the result.
        if (cs.function1() != null && cs.function1().parameters().size() == 1
                && !cs.function1().body().isEmpty()) {
            propToCol.put(propName, propName);
            properties.put(propName, new StoreResolution.PropertyResolution.DynaFunction(
                    cs.function1().body().get(0)));
        }
    }

    /** Resolves an association extend ColSpec into a JoinResolution. */
    private void resolveAssociationExtend(ColSpec cs, String className,
                                          Map<String, StoreResolution.JoinResolution> joins) {
        String propName = cs.name();
        AppliedFunction traverseAf = (AppliedFunction) cs.function1().body().get(0);

        var nav = modelContext.findAssociationByProperty(className, propName).orElse(null);
        if (nav == null) return;

        String targetClassName = nav.targetClassName();
        ClassMapping targetMapping = normalized.findClassMapping(targetClassName)
                .orElseThrow(() -> new PureCompileException(
                        "Association join for property '" + propName + "' targets class '"
                                + targetClassName + "' which has no mapping in scope"));

        StoreResolution targetResolution;
        if (resolving.contains(targetClassName)) {
            targetResolution = buildShallowResolution(targetMapping);
        } else {
            targetResolution = resolveClassMapping(targetMapping, targetClassName);
        }
        String targetTable = targetMapping.sourceTable().name();

        joins.put(propName, buildJoinResolution(
                targetTable, nav.isToMany(), traverseAf, targetResolution));
    }

    /** Resolves an embedded extend ColSpec into a JoinResolution (no physical JOIN). */
    private void resolveEmbeddedExtend(ColSpec cs, String tableName,
                                       Map<String, StoreResolution.JoinResolution> joins) {
        String propName = cs.name();
        ClassInstance innerCI = (ClassInstance) cs.function1().body().get(0);
        ColSpecArray subArray = (ColSpecArray) innerCI.value();

        Map<String, String> subPropToCol = new LinkedHashMap<>();
        Map<String, StoreResolution.PropertyResolution> subProperties = new LinkedHashMap<>();
        for (ColSpec sub : subArray.colSpecs()) {
            var subBody = sub.function1().body().get(0);
            if (subBody instanceof AppliedProperty ap) {
                String colName = ap.property();
                subPropToCol.put(sub.name(), colName);
                subProperties.put(sub.name(),
                        new StoreResolution.PropertyResolution.EmbeddedColumn(colName));
            }
        }

        var existingJoin = joins.get(propName);
        if (existingJoin != null && !existingJoin.embedded()) {
            // Merge embedded columns into association's target resolution
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
            var embeddedResolution = new StoreResolution(
                    tableName, subPropToCol, subProperties, Map.of(), null, false);
            joins.put(propName, new StoreResolution.JoinResolution(
                    null, null, null, false, null, Set.of(), embeddedResolution, true));
        }
    }

    /** Resolves a traverse extend: extend(source, traverse(...), colSpecCI). */
    private void resolveTraverseExtend(AppliedFunction extendAf,
                                       Map<String, StoreResolution.JoinResolution> joins) {
        AppliedFunction traverseAf = (AppliedFunction) extendAf.parameters().get(1);
        String targetTable = extractTraverseTargetTable(traverseAf);
        if (targetTable == null) return;

        List<ColSpec> colSpecs = extractTraverseColSpecs(extendAf);
        if (colSpecs.isEmpty()) return;

        Map<String, String> subPropToCol = new LinkedHashMap<>();
        Map<String, StoreResolution.PropertyResolution> subProperties = new LinkedHashMap<>();
        for (ColSpec cs : colSpecs) {
            String physCol = extractTraversePhysicalColumn(cs);
            if (physCol != null) {
                subPropToCol.put(cs.name(), physCol);
                subProperties.put(cs.name(),
                        new StoreResolution.PropertyResolution.Column(physCol));
            } else if (cs.function1() != null && !cs.function1().body().isEmpty()) {
                subPropToCol.put(cs.name(), cs.name());
                subProperties.put(cs.name(),
                        new StoreResolution.PropertyResolution.DynaFunction(
                                cs.function1().body().get(0)));
            }
        }

        var targetResolution = new StoreResolution(
                targetTable, subPropToCol, subProperties, Map.of(), null, false);
        var joinRes = buildJoinResolution(targetTable, false, traverseAf, targetResolution);
        for (ColSpec cs : colSpecs) {
            joins.put(cs.name(), joinRes);
        }
    }

    /** Adds inline struct-array properties (UNNEST candidates) from the model. */
    private void addStructArrayJoins(String className,
                                     Map<String, StoreResolution.JoinResolution> joins) {
        var pureClass = modelContext.findClass(className).orElse(null);
        if (pureClass == null) return;
        for (var prop : pureClass.properties()) {
            if (joins.containsKey(prop.name())) continue;
            if (prop.multiplicity().isSingular()) continue;
            var propType = prop.genericType();
            if (!(propType instanceof PureClass targetClass)) continue;
            String targetFqn = targetClass.qualifiedName();
            if (modelContext.findClass(targetFqn).isEmpty()) continue;

            var elementClass = modelContext.findClass(targetFqn).get();
            var identityMapping = RelationalMapping.identity(elementClass);
            StoreResolution targetResolution = resolveClassMapping(identityMapping, targetFqn);
            joins.put(prop.name(), new StoreResolution.JoinResolution(
                    null, null, null, true, null, Set.of(), targetResolution));
        }
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
     * Stamps {@link StoreResolution.ExtendOverride} on sourceSpec extend nodes
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
            if (store.sourceSpec() == null) continue;

            String className = entry.getValue();

            Set<String> usedProps = typeResult.classPropertyAccesses()
                    .getOrDefault(className, Set.of());

            for (AppliedFunction extendAf : findExtendNodes(store.sourceSpec())) {
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
                && "traverse".equals(SymbolTable.extractSimpleName(af.function()));
    }

    /** Extracts the terminal target table name from a traverse expression. */
    private static String extractTraverseTargetTable(AppliedFunction traverseAf) {
        // Table reference is always at index size-2 (before the lambda at last index)
        int tableRefIdx = traverseAf.parameters().size() - 2;
        var tableRef = traverseAf.parameters().get(tableRefIdx);
        if (tableRef instanceof AppliedFunction af
                && "tableReference".equals(SymbolTable.extractSimpleName(af.function()))) {
            if (af.parameters().size() >= 2 && af.parameters().get(1) instanceof CString cs) {
                // 2nd arg is the bare table name (1st is db FQN)
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

    /** Walks the sourceSpec chain to find extend nodes (outermost first). */
    private static List<AppliedFunction> findExtendNodes(ValueSpecification sourceRel) {
        var result = new ArrayList<AppliedFunction>();
        var cur = sourceRel;
        while (cur instanceof AppliedFunction af
                && "extend".equals(SymbolTable.extractSimpleName(af.function()))) {
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
