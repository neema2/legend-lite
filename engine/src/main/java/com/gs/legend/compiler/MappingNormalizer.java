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
 *   <li>Association join resolution (pre-resolves traversals for downstream stages)</li>
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
            @Override public java.util.Optional<com.gs.legend.model.store.Table> findTable(String n) { return model.findTable(n); }
            @Override public java.util.Optional<com.gs.legend.model.def.EnumDefinition> findEnum(String n) { return model.findEnum(n); }
            @Override public java.util.Optional<MappingExpression> findMappingExpression(String className) {
                return normalized.findMappingExpression(className);
            }
        };
    }

    /**
     * Full mapping data for MappingResolver — findClassMapping + findAssociationJoins.
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

        // Phase 2a: Build Relational MappingExpressions (with per-property associationJoins).
        // Relational expressions must be built first so M2M expressions can reference
        // the source class's associationJoins to wire up association navigations.
        for (var entry : resolvedMappings.entrySet()) {
            String className = entry.getKey();
            ClassMapping cm = entry.getValue();
            if (!(cm instanceof RelationalMapping rm)) continue;

            var sourceRelation = synthesizeSourceRelation(rm);
            String sourceTable = rm.table().name();

            // Build per-property association joins from Association navigations.
            // Direction is deterministic: sourceTable = this class, targetTable = Association target.
            var assocJoins = new LinkedHashMap<String, ModelContext.AssociationJoinInfo>();
            for (var navEntry : model.findAllAssociationNavigations(className).entrySet()) {
                String propName = navEntry.getKey();
                var nav = navEntry.getValue();
                // No relational join for this association — pure-model-only, skip
                if (nav.join() == null) continue;

                // Target class must have a mapping in this scope to wire up
                ClassMapping targetCm = resolvedMappings.get(nav.targetClassName());
                if (targetCm == null) continue;
                if (targetCm.sourceTable() == null) {
                    throw new com.gs.legend.compiler.PureCompileException(
                            "Association property '" + propName + "' targets class '"
                                    + nav.targetClassName() + "' whose mapping has no source table");
                }
                String targetTable = targetCm.sourceTable().name();

                // nav.join() is already a resolved Join from PureModelBuilder — use directly
                var traversal = buildTraverse(nav.join(), sourceTable, targetTable);
                assocJoins.put(propName, new ModelContext.AssociationJoinInfo(
                        nav.targetClassName(), traversal, nav.isToMany()));
            }
            expressions.put(className, new ModelContext.MappingExpression.Relational(
                    className, sourceRelation, assocJoins));
        }

        // Phase 2b: Build M2M MappingExpressions, detecting association navigations.
        // For each M2M property expression matching AppliedProperty(prop, [Variable("src")]),
        // check if it's an Association navigation on the source class. If so, look up the
        // source class's already-built Relational associationJoins for the traversal.
        for (var entry : resolvedMappings.entrySet()) {
            String className = entry.getKey();
            ClassMapping cm = entry.getValue();
            if (!(cm instanceof PureClassMapping pcm)) continue;

            var assocNavs = detectM2MAssociationNavigations(pcm, expressions);
            expressions.put(className, new ModelContext.MappingExpression.M2M(
                    pcm.sourceClassName(), pcm.propertyExpressions(), pcm.filter(), assocNavs));
        }

        return new NormalizedMapping(resolvedMappings, expressions);
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

    // ==================== M2M Association Detection ====================

    /**
     * Detects Association navigations in M2M property expressions.
     * When a Pure mapping has {@code address: $src.rawAddresses}, the expression is
     * {@code AppliedProperty("rawAddresses", [Variable("src")])}. If "rawAddresses" is an
     * Association property on the source class, we look up the source class's
     * already-built Relational associationJoins to find the traversal.
     *
     * <p>Multiplicity comes from the M2M TARGET class property (e.g., address:[0..1]),
     * not the Association end (rawAddresses:[*]).
     */
    private Map<String, ModelContext.AssociationJoinInfo> detectM2MAssociationNavigations(
            PureClassMapping pcm,
            Map<String, ModelContext.MappingExpression> builtExpressions) {

        var result = new LinkedHashMap<String, ModelContext.AssociationJoinInfo>();
        String sourceClassName = pcm.sourceClassName();

        // Find the source class's Relational expression (built in Phase 2a)
        var sourceExpr = builtExpressions.get(sourceClassName);
        if (!(sourceExpr instanceof ModelContext.MappingExpression.Relational sourceRel)) {
            return result; // Source is not relational — no association joins to wire
        }

        // Build reverse lookup: assocPropName on source → AssociationJoinInfo
        // (associationJoins is already keyed by property name on the source class)
        var sourceAssocJoins = sourceRel.associationJoins();

        for (var entry : pcm.propertyExpressions().entrySet()) {
            String m2mPropName = entry.getKey();
            var expr = entry.getValue();

            // Match pattern: AppliedProperty(assocPropName, [Variable("src")])
            if (!(expr instanceof com.gs.legend.ast.AppliedProperty ap)) continue;
            if (ap.parameters().size() != 1) continue;
            if (!(ap.parameters().get(0) instanceof com.gs.legend.ast.Variable v)) continue;
            if (!"src".equals(v.name())) continue;

            String assocPropName = ap.property();

            // Check if this property has a pre-resolved association join on the source class
            var sourceJoinInfo = sourceAssocJoins.get(assocPropName);
            if (sourceJoinInfo == null) continue;

            // Determine isToMany from the M2M TARGET class property multiplicity,
            // not the Association end multiplicity.
            boolean isToMany = sourceJoinInfo.isToMany();
            var targetClassOpt = model.findClass(pcm.targetClassName());
            if (targetClassOpt.isPresent()) {
                var propOpt = targetClassOpt.get().findProperty(m2mPropName);
                if (propOpt.isPresent()) {
                    isToMany = !propOpt.get().multiplicity().isSingular();
                }
            }

            result.put(m2mPropName, new ModelContext.AssociationJoinInfo(
                    sourceJoinInfo.targetClassName(), sourceJoinInfo.traversal(), isToMany));
        }

        return result;
    }

    // ==================== Traverse Builder ====================

    /**
     * Builds {@code traverse(tableRef(src), tableRef(tgt), {src, tgt | cond})}.
     */
    private static com.gs.legend.ast.ValueSpecification buildTraverse(Join join, String sourceTable, String targetTable) {
        var tableToParam = new java.util.HashMap<String, String>();
        tableToParam.put(sourceTable, "src");
        if (!targetTable.equals(sourceTable)) tableToParam.put(targetTable, "tgt");

        var condBody = RelationalMappingConverter.convert(join.condition(), tableToParam, "tgt");
        var lambda = new com.gs.legend.ast.LambdaFunction(
                java.util.List.of(new com.gs.legend.ast.Variable("src"), new com.gs.legend.ast.Variable("tgt")),
                java.util.List.of(condBody));
        return new com.gs.legend.ast.AppliedFunction("meta::pure::functions::relation::traverse", java.util.List.of(
                new com.gs.legend.ast.AppliedFunction("meta::pure::functions::relation::tableReference",
                        java.util.List.of(new com.gs.legend.ast.CString(sourceTable))),
                new com.gs.legend.ast.AppliedFunction("meta::pure::functions::relation::tableReference",
                        java.util.List.of(new com.gs.legend.ast.CString(targetTable))),
                lambda));
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

        // 4. DynaFunction property mappings → ->extend(~[prop:row|<dynaExpr>])
        source = addDynaFunctionExtends(rm, source);

        // 5. Non-join property mappings → ->extend(~[name:row|$row.COL, ...])
        // TEMPORARILY DISABLED for debugging — uncomment after Phase 3
        // source = addPropertyExtends(rm, source);

        // 6. ~distinct → ->distinct()
        if (rm.distinct()) {
            source = new com.gs.legend.ast.AppliedFunction(
                    "distinct",
                    java.util.List.of(source),
                    true);
        }

        // TODO Phase 3: add ->select(~[prop1, prop2, ...]) here once resolveColumnExpr
        // is updated to identity mapping. For now, both physical columns and mapped
        // property names coexist in the TDS.

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
     * Adds {@code ->extend(~[prop:row|<dynaExpr>, ...])} for DynaFunction property mappings.
     * Each DynaFunction property carries a pre-compiled ValueSpecification expression tree
     * (e.g., {@code concat($row.FIRST, ' ', $row.LAST)}). These are wrapped in ColSpec lambdas
     * and added as extend columns so TypeChecker can stamp them with TypeInfo.
     */
    private com.gs.legend.ast.ValueSpecification addDynaFunctionExtends(
            RelationalMapping rm,
            com.gs.legend.ast.ValueSpecification source) {

        var colSpecs = new java.util.ArrayList<com.gs.legend.ast.ColSpec>();

        for (var pm : rm.propertyMappings()) {
            if (!pm.hasDynaExpression()) continue;

            var rowVar = new com.gs.legend.ast.Variable("row");
            // The dynaExpression already uses $row.COLUMN references (from RelationalMappingConverter)
            var lambda = new com.gs.legend.ast.LambdaFunction(
                    java.util.List.of(rowVar), pm.dynaExpression());
            colSpecs.add(new com.gs.legend.ast.ColSpec(pm.propertyName(), lambda));
        }

        if (colSpecs.isEmpty()) return source;

        com.gs.legend.ast.ClassInstance colSpecCI;
        if (colSpecs.size() == 1) {
            colSpecCI = new com.gs.legend.ast.ClassInstance("colSpec", colSpecs.get(0));
        } else {
            colSpecCI = new com.gs.legend.ast.ClassInstance("colSpecArray",
                    new com.gs.legend.ast.ColSpecArray(colSpecs));
        }

        return new com.gs.legend.ast.AppliedFunction(
                "extend",
                java.util.List.of(source, colSpecCI),
                true);
    }

    /**
     * Synthesizes a single {@code ->extend(~[prop:row|expr, ...])} for all non-join
     * property mappings: column renames, expression access, and enum mappings.
     * All are batched into one FuncColSpecArray to avoid nested subqueries.
     */
    private com.gs.legend.ast.ValueSpecification addPropertyExtends(
            RelationalMapping rm,
            com.gs.legend.ast.ValueSpecification source) {

        var colSpecs = new java.util.ArrayList<com.gs.legend.ast.ColSpec>();

        for (var pm : rm.propertyMappings()) {
            if (pm.hasJoinChain()) continue; // handled by addTraverseExtends

            var rowVar = new com.gs.legend.ast.Variable("row");
            com.gs.legend.ast.ValueSpecification body;

            if (pm.hasEnumMapping()) {
                // Enum mapping: if(equal($row.COL, dbVal1), 'ENUM1', if(..., null))
                body = synthesizeEnumIf(rowVar, pm);
            } else if (pm.hasExpression()) {
                // Expression access: get($row.COLUMN, 'key') with optional cast
                body = synthesizeExpressionAccess(rowVar, pm);
            } else {
                // Simple column rename: $row.COLUMN_NAME
                body = new com.gs.legend.ast.AppliedProperty(
                        pm.columnName(), java.util.List.of(rowVar));
            }

            var lambda = new com.gs.legend.ast.LambdaFunction(
                    java.util.List.of(rowVar), body);
            colSpecs.add(new com.gs.legend.ast.ColSpec(pm.propertyName(), lambda));
        }

        if (colSpecs.isEmpty()) return source;

        com.gs.legend.ast.ClassInstance colSpecCI;
        if (colSpecs.size() == 1) {
            colSpecCI = new com.gs.legend.ast.ClassInstance("colSpec", colSpecs.get(0));
        } else {
            colSpecCI = new com.gs.legend.ast.ClassInstance("colSpecArray",
                    new com.gs.legend.ast.ColSpecArray(colSpecs));
        }

        return new com.gs.legend.ast.AppliedFunction(
                "extend",
                java.util.List.of(source, colSpecCI),
                true);
    }

    /**
     * Adds {@code ->select(~[prop1, prop2, ...])} to project only the mapped property names.
     * This removes the original table columns, leaving only the Pure property names.
     */
    private com.gs.legend.ast.ValueSpecification addSelectProjection(
            RelationalMapping rm,
            com.gs.legend.ast.ValueSpecification source) {

        var colSpecs = new java.util.ArrayList<com.gs.legend.ast.ColSpec>();
        for (var pm : rm.propertyMappings()) {
            colSpecs.add(new com.gs.legend.ast.ColSpec(pm.propertyName()));
        }

        if (colSpecs.isEmpty()) return source;

        com.gs.legend.ast.ClassInstance colSpecCI;
        if (colSpecs.size() == 1) {
            colSpecCI = new com.gs.legend.ast.ClassInstance("colSpec", colSpecs.get(0));
        } else {
            colSpecCI = new com.gs.legend.ast.ClassInstance("colSpecArray",
                    new com.gs.legend.ast.ColSpecArray(colSpecs));
        }

        return new com.gs.legend.ast.AppliedFunction(
                "select",
                java.util.List.of(source, colSpecCI),
                true);
    }

    /**
     * Synthesizes an if/else chain for enum mapping:
     * {@code if(equal($row.COL, dbVal1), 'ENUM1', if(equal($row.COL, dbVal2), 'ENUM2', null))}
     */
    private com.gs.legend.ast.ValueSpecification synthesizeEnumIf(
            com.gs.legend.ast.Variable rowVar,
            com.gs.legend.model.store.PropertyMapping pm) {

        var colAccess = new com.gs.legend.ast.AppliedProperty(
                pm.columnName(), java.util.List.of(rowVar));

        // Build reverse list of (enumValue, dbValue) pairs
        var entries = new java.util.ArrayList<java.util.Map.Entry<String, Object>>();
        for (var e : pm.enumMapping().entrySet()) {
            for (var dbVal : e.getValue()) {
                entries.add(java.util.Map.entry(e.getKey(), dbVal));
            }
        }

        // Build if/else chain from end to start
        com.gs.legend.ast.ValueSpecification result = new com.gs.legend.ast.CString(""); // fallback
        for (int i = entries.size() - 1; i >= 0; i--) {
            var entry = entries.get(i);
            String enumVal = entry.getKey();
            Object dbVal = entry.getValue();

            com.gs.legend.ast.ValueSpecification dbLiteral;
            if (dbVal instanceof String s) {
                dbLiteral = new com.gs.legend.ast.CString(s);
            } else if (dbVal instanceof Number n) {
                dbLiteral = new com.gs.legend.ast.CInteger(n.longValue());
            } else {
                dbLiteral = new com.gs.legend.ast.CString(dbVal.toString());
            }

            var condition = new com.gs.legend.ast.AppliedFunction(
                    "equal", java.util.List.of(colAccess, dbLiteral));
            var thenVal = new com.gs.legend.ast.CString(enumVal);

            // if(condition, {|thenVal}, {|result})
            var thenLambda = new com.gs.legend.ast.LambdaFunction(java.util.List.of(), thenVal);
            var elseLambda = new com.gs.legend.ast.LambdaFunction(java.util.List.of(), result);
            result = new com.gs.legend.ast.AppliedFunction(
                    "if", java.util.List.of(condition, thenLambda, elseLambda));
        }

        return result;
    }

    /**
     * Synthesizes expression access: {@code get($row.COLUMN, 'key')} with optional
     * {@code cast(get(...), @Type)} if a cast type is specified.
     */
    private com.gs.legend.ast.ValueSpecification synthesizeExpressionAccess(
            com.gs.legend.ast.Variable rowVar,
            com.gs.legend.model.store.PropertyMapping pm) {

        var parsedAccess = pm.expressionAccess().orElse(null);
        if (parsedAccess == null) {
            // Unparseable expression — fall back to simple column access
            return new com.gs.legend.ast.AppliedProperty(
                    pm.columnName(), java.util.List.of(rowVar));
        }

        // get($row.COLUMN, 'key')
        var colAccess = new com.gs.legend.ast.AppliedProperty(
                pm.columnName(), java.util.List.of(rowVar));
        com.gs.legend.ast.ValueSpecification body = new com.gs.legend.ast.AppliedFunction(
                "get", java.util.List.of(colAccess, new com.gs.legend.ast.CString(parsedAccess.jsonKey())));

        // Optional cast: cast(get(...), @Type)
        if (parsedAccess.castType() != null) {
            var prim = com.gs.legend.plan.GenericType.Primitive.fromTypeName(parsedAccess.castType());
            var typeRef = new com.gs.legend.ast.GenericTypeInstance(parsedAccess.castType(), prim);
            body = new com.gs.legend.ast.AppliedFunction(
                    "cast", java.util.List.of(body, typeRef));
        }

        return body;
    }

    /**
     * Builds a traverse chain from join definitions:
     * {@code traverse(T_DEPT, {prev,hop|...})->traverse(T_ORG, {prev,hop|...})}
     *
     * Uses {@link RelationalMappingConverter#convert(RelationalOperation, java.util.Map)} to convert
     * the full join condition to Pure AST, supporting complex conditions (multi-column, function-based, etc.).
     */
    private com.gs.legend.ast.ValueSpecification buildTraverseChain(
            String startTable, java.util.List<String> joinNames) {

        String[] currentTableHolder = { startTable };
        com.gs.legend.ast.ValueSpecification traverseExpr = null;

        for (String joinName : joinNames) {
            String curTable = currentTableHolder[0];
            Join join = model.findJoin(joinName)
                    .orElseThrow(() -> new IllegalStateException(
                            "Join not found: " + joinName));

            // Find the "other" table in the join condition
            var tableNames = RelationalMappingConverter.collectTableNames(join.condition());
            String targetTable = tableNames.stream()
                    .filter(t -> !t.equals(curTable))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "Join '" + joinName + "' does not reference a table other than '" + curTable + "'"));

            // Convert the full join condition with table→param mapping
            var tableToParam = java.util.Map.of(curTable, "prev", targetTable, "hop");
            var condBody = RelationalMappingConverter.convert(join.condition(), tableToParam);

            var targetRef = new com.gs.legend.ast.AppliedFunction(
                    "tableReference",
                    java.util.List.of(new com.gs.legend.ast.CString(targetTable)),
                    false);

            var condLambda = new com.gs.legend.ast.LambdaFunction(
                    java.util.List.of(
                            new com.gs.legend.ast.Variable("prev"),
                            new com.gs.legend.ast.Variable("hop")),
                    condBody);

            if (traverseExpr == null) {
                traverseExpr = new com.gs.legend.ast.AppliedFunction(
                        "traverse",
                        java.util.List.of(targetRef, condLambda),
                        false);
            } else {
                traverseExpr = new com.gs.legend.ast.AppliedFunction(
                        "traverse",
                        java.util.List.of(traverseExpr, targetRef, condLambda),
                        false);
            }

            currentTableHolder[0] = targetTable;
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
