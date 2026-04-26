package com.gs.legend.compiler;

import com.gs.legend.model.ModelContext;
import com.gs.legend.model.PureModelBuilder;
import com.gs.legend.model.RelationalMappingConverter;
import com.gs.legend.model.mapping.ClassMapping;
import com.gs.legend.model.mapping.PureClassMapping;
import com.gs.legend.model.mapping.RelationalMapping;
import com.gs.legend.model.SymbolTable;
import com.gs.legend.model.store.Filter;
import com.gs.legend.model.store.Join;

import java.util.HashMap;
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
    private final SymbolTable symbols;
    private NormalizedMapping normalized;

    /**
     * Normalizes eagerly. If mappingNames is empty, produces an empty snapshot
     * and modelContext() returns the base model.
     */
    public MappingNormalizer(PureModelBuilder model, java.util.List<String> mappingNames) {
        this.model = model;
        this.symbols = model.symbolTable();
        this.normalized = normalize(mappingNames);
    }

    /**
     * Model context view for TypeChecker and MappingResolver — overlays the
     * synthetic mapping functions onto the base model's {@code findFunction}
     * channel, so synthetic functions are indistinguishable from user functions
     * to both downstream consumers.
     */
    public ModelContext modelContext() {
        if (!normalized.hasClassMappings()) return model;
        return new ModelContext() {
            @Override public java.util.Optional<com.gs.legend.model.m3.PureClass> findClass(String n) { return model.findClass(n); }
            @Override public java.util.Optional<AssociationNavigation> findAssociationByProperty(String c, String p) { return model.findAssociationByProperty(c, p); }
            @Override public java.util.Optional<com.gs.legend.model.store.Table> findTable(String db, String name) { return model.findTable(db, name); }
            @Override public java.util.Optional<com.gs.legend.model.def.EnumDefinition> findEnum(String n) { return model.findEnum(n); }
            @Override public java.util.Map<String, AssociationNavigation> findAllAssociationNavigations(String c) { return model.findAllAssociationNavigations(c); }
            @Override public java.util.List<com.gs.legend.model.m3.PureFunction> findFunction(String n) {
                // Synthetic mapping functions win when the FQN matches — they
                // live in the per-query mapping scope, overlayed on top of
                // the model's permanent user-function registry. One expression,
                // one Optional, one dispatch.
                return normalized.findMappingFunction(n)
                        .<java.util.List<com.gs.legend.model.m3.PureFunction>>map(java.util.List::of)
                        .orElseGet(() -> model.findFunction(n));
            }
            @Override public java.util.Optional<String> findMappingFunctionFqn(String className) {
                return normalized.findMappingFunctionFqn(className);
            }
        };
    }

    /**
     * Full mapping data for MappingResolver — findClassMapping + findSourceSpec.
     * TypeChecker never sees this.
     */
    public NormalizedMapping normalizedMapping() {
        return normalized;
    }

    // ==================== Normalization ====================

    private NormalizedMapping normalize(java.util.List<String> mappingNames) {
        if (mappingNames.isEmpty()) return NormalizedMapping.empty();

        var registry = model.getMappingRegistry();
        Map<Integer, ClassMapping> allMappings = new HashMap<>();
        for (String name : mappingNames) {
            allMappings.putAll(registry.getAllClassMappings(name));
        }

        Map<Integer, ClassMapping> resolvedMappings = new HashMap<>();
        Map<Integer, com.gs.legend.ast.ValueSpecification> rawSourceSpecs = new LinkedHashMap<>();

        // Phase 1: Detect circular M2M chains + apply view macros.
        // M2M chains no longer bake the upstream ClassMapping reference into
        // PureClassMapping (object-identity shortcut is gone) — the upstream
        // is resolved by FQN through this same NormalizedMapping at query time.
        Set<Integer> resolving = new HashSet<>();
        for (var entry : allMappings.entrySet()) {
            int classId = entry.getKey();
            ClassMapping cm = entry.getValue();

            if (cm instanceof PureClassMapping pcm) {
                detectM2MCycles(pcm, allMappings, resolving);
                resolvedMappings.put(classId, pcm);
            } else if (cm instanceof RelationalMapping rm && rm.view() != null) {
                // View macro: resolve PMs through the view once, store the resolved mapping.
                // MappingResolver reads the resolved PMs — no second resolution needed.
                var resolvedPMs = resolvePropertyMappingsThroughView(
                        rm.propertyMappings(), rm.view(), rm.table().dbName());
                var resolved = rm.withPropertyMappings(resolvedPMs);
                // View ~groupBy: resolve key columns to class property names
                if (!rm.view().groupBy().isEmpty()) {
                    resolved = resolved.withGroupByColumns(
                            resolveViewGroupByKeys(rm.view(), resolvedPMs));
                }
                resolvedMappings.put(classId, resolved);
            } else {
                resolvedMappings.put(classId, cm);
            }
        }

        // Phase 2a: Build relational sourceSpecs.
        // Association traversals are embedded in sourceSpec as extend() nodes
        // with fn1=traverse — no separate AssociationJoinInfo needed.
        for (var entry : resolvedMappings.entrySet()) {
            int classId = entry.getKey();
            String className = symbols.nameOf(classId);
            ClassMapping cm = entry.getValue();
            if (!(cm instanceof RelationalMapping rm)) continue;

            var sourceSpec = synthesizeSourceSpec(rm);
            sourceSpec = addAssociationExtends(rm, className, sourceSpec, resolvedMappings);
            rawSourceSpecs.put(classId, sourceSpec);
        }

        // Phase 2b: Build M2M sourceSpec chains.
        // Same shape as relational — one synthesized ValueSpecification:
        //   getAll("SrcClass") → filter(src|cond) → extend(~[prop:src|expr, ...])
        for (var entry : resolvedMappings.entrySet()) {
            int classId = entry.getKey();
            ClassMapping cm = entry.getValue();
            if (!(cm instanceof PureClassMapping pcm)) continue;

            rawSourceSpecs.put(classId, synthesizeM2MSourceSpec(pcm));
        }

        // Phase 3: Wrap each raw sourceSpec AST as a synthetic PureFunction,
        // deterministically named "<classFqn>::mappingFunction". These functions
        // are compiled by TypeChecker exactly like user functions and walked
        // by MappingResolver via the same primitive as TypedUserCall.
        //
        // "::mappingFunction" is a reserved canonical suffix: a future
        // user-authored function with this name for a given class is
        // treated as an intentional override of the synthesized default
        // (same mechanism Python uses for __eq__, Rust for Default::default,
        // Go for init/main, etc.). The overlay's findFunction path resolves
        // both uniformly, so no special-casing is needed downstream.
        //
        // NormalizedMapping is FQN-keyed (no integer handles leak across the
        // module boundary), so we convert from the internal classId-keyed
        // representation here in one place.
        Map<String, ClassMapping> classMappingsByFqn = new LinkedHashMap<>();
        for (var entry : resolvedMappings.entrySet()) {
            classMappingsByFqn.put(symbols.nameOf(entry.getKey()), entry.getValue());
        }
        Map<String, String> mappingFunctionFqns = new LinkedHashMap<>();
        Map<String, com.gs.legend.model.m3.PureFunction> mappingFunctions = new LinkedHashMap<>();
        for (var entry : rawSourceSpecs.entrySet()) {
            String className = symbols.nameOf(entry.getKey());
            String fnFqn = className + "::mappingFunction";
            var pureFn = new com.gs.legend.model.m3.PureFunction(
                    fnFqn,
                    /* typeParams */ java.util.List.of(),
                    /* parameters */ java.util.List.of(),
                    /* returnType */ com.gs.legend.model.m3.Primitive.ANY,
                    /* returnMult */ com.gs.legend.model.m3.Multiplicity.MANY,
                    /* body       */ java.util.List.of(entry.getValue()),
                    /* stereotypes */ java.util.List.of(),
                    /* taggedValues */ java.util.List.of());
            mappingFunctionFqns.put(className, fnFqn);
            mappingFunctions.put(fnFqn, pureFn);
        }

        return new NormalizedMapping(classMappingsByFqn, mappingFunctionFqns, mappingFunctions);
    }

    // ==================== Chain Walking ====================

    /**
     * Walks a class mapping's M2M source chain to its terminal relational
     * mapping's {@link com.gs.legend.model.store.Table}. Returns {@code null}
     * if the chain doesn't bottom out at a relational mapping in the active
     * scope (e.g., the upstream class has no mapping here).
     *
     * <p>Replaces the former {@code pcm.sourceMapping().sourceTable()} shortcut
     * — which baked the upstream reference into the mapping def — with an
     * explicit, principle-compliant walk through the {@code NormalizedMapping}
     * scope's class-FQN-keyed registry.
     */
    private com.gs.legend.model.store.Table terminalSourceTable(
            ClassMapping cm, Map<Integer, ClassMapping> resolvedMappings) {
        ClassMapping current = cm;
        java.util.Set<Integer> seen = new HashSet<>();
        while (current instanceof PureClassMapping pcm) {
            int srcId = symbols.resolveId(pcm.sourceClassName());
            if (srcId < 0 || !seen.add(srcId)) return null;
            current = resolvedMappings.get(srcId);
            if (current == null) return null;
        }
        if (current instanceof RelationalMapping rm) {
            return rm.table();
        }
        return null;
    }

    // ==================== M2M Cycle Detection ====================

    /**
     * Detects circular M2M chains via DFS over the class-mapping graph.
     *
     * <p>Does <em>not</em> bake any upstream ClassMapping reference into the
     * {@link PureClassMapping} — under the cross-project-joins principle, the
     * upstream mapping is resolved by class FQN through this
     * {@link NormalizedMapping} at query time. The old
     * {@code withResolvedSource} / {@code pcm.sourceMapping()} shortcut is
     * gone; this method purely guards against infinite chains.
     */
    private void detectM2MCycles(
            PureClassMapping pcm,
            Map<Integer, ClassMapping> allMappings,
            Set<Integer> resolving) {

        int targetId = symbols.resolveId(pcm.targetClassName());
        if (resolving.contains(targetId)) {
            throw new IllegalStateException(
                    "Circular M2M chain detected for class: " + pcm.targetClassName());
        }

        int sourceId = symbols.resolveId(pcm.sourceClassName());
        ClassMapping sourceMapping = sourceId >= 0 ? allMappings.get(sourceId) : null;

        if (sourceMapping instanceof PureClassMapping srcPcm) {
            resolving.add(targetId);
            detectM2MCycles(srcPcm, allMappings, resolving);
            resolving.remove(targetId);
        }
    }

    // ==================== Association Extends ====================

    /**
     * Adds association extend nodes to the sourceSpec chain.
     * Each association becomes: {@code extend(source, ~propName:traverse(target, {prev,hop|cond}))}
     * where the ColSpec's fn1 is a 0-param lambda wrapping the traverse() call.
     *
     * <p>Uses the 2-param extend form (source + ColSpec). The traverse chain is embedded
     * in the ColSpec's fn1 body. Self-joins are supported via TargetColumnRef.
     */
    private com.gs.legend.ast.ValueSpecification addAssociationExtends(
            RelationalMapping rm, String className,
            com.gs.legend.ast.ValueSpecification source,
            Map<Integer, ClassMapping> resolvedMappings) {

        String sourceTable = rm.table().dbName();

        for (var navEntry : model.findAllAssociationNavigationsFull(className).entrySet()) {
            String propName = navEntry.getKey();
            var nav = navEntry.getValue();
            if (nav.join() == null) continue;

            int targetId = symbols.resolveId(nav.targetClassName());
            ClassMapping targetCm = targetId >= 0 ? resolvedMappings.get(targetId) : null;
            // Walk M2M chains to their terminal relational mapping for the
            // physical source-table. Under the cross-project-joins principle
            // we no longer bake the source reference into PureClassMapping, so
            // this walk goes through `resolvedMappings` instead of pcm.sourceMapping().
            com.gs.legend.model.store.Table targetTable = terminalSourceTable(
                    targetCm, resolvedMappings);
            if (targetTable == null) continue;

            // Build traverse chain (supports self-joins via TargetColumnRef)
            var traverseCall = buildTraverseChain(sourceTable, java.util.List.of(nav.join()));

            // Wrap traverse in a 0-param lambda: fn1 = { -> traverse(...) }
            var fn1 = new com.gs.legend.ast.LambdaFunction(
                    java.util.List.of(),
                    java.util.List.of(traverseCall));

            // ColSpec with fn1=traverse lambda (fn2=null) — association extend marker
            var colSpecCI = new com.gs.legend.ast.ColSpec(propName, fn1);

            // 2-param extend: extend(source, ColSpec) — no separate _Traversal param
            source = new com.gs.legend.ast.AppliedFunction(
                    "extend",
                    java.util.List.of(source, colSpecCI),
                    true);
        }
        return source;
    }

    // ==================== Embedded Extends ====================

    /**
     * Adds embedded property extend nodes to the sourceSpec chain.
     * Each embedded property becomes:
     * {@code extend(source, ~propName:{-> ~[sub1:r|$r.COL1, sub2:r|$r.COL2]})}
     * where fn1 is a 0-param lambda whose body is a ColSpecArray of sub-property mappings.
     *
     * <p>This is structurally distinct from association extends (fn1 body = traverse)
     * and is detected by {@code ExtendChecker.isEmbeddedExtend()} and
     * {@code MappingResolver.resolveAssociationJoins()}.
     */
    private com.gs.legend.ast.ValueSpecification addEmbeddedExtends(
            RelationalMapping rm,
            com.gs.legend.ast.ValueSpecification source) {

        if (rm.embeddedMappings().isEmpty()) return source;

        for (var entry : rm.embeddedMappings().entrySet()) {
            String propName = entry.getKey();
            var subMappings = entry.getValue();

            // Build sub-ColSpecs: ~[sub1:row|$row.COL1, sub2:row|$row.COL2]
            var subColSpecs = new java.util.ArrayList<com.gs.legend.ast.ColSpec>();
            for (var sub : subMappings) {
                var rowVar = new com.gs.legend.ast.Variable("row");
                var body = new com.gs.legend.ast.AppliedProperty(
                        sub.columnName(), java.util.List.of(rowVar));
                var fn1 = new com.gs.legend.ast.LambdaFunction(
                        java.util.List.of(rowVar), body);
                subColSpecs.add(new com.gs.legend.ast.ColSpec(sub.propertyName(), fn1));
            }

            // Wrap in ColSpecArray
            var colSpecArrayCI = new com.gs.legend.ast.ColSpecArray(subColSpecs);

            // fn1 = 0-param lambda wrapping ColSpecArray: { -> ~[...] }
            var outerFn1 = new com.gs.legend.ast.LambdaFunction(
                    java.util.List.of(),
                    java.util.List.of(colSpecArrayCI));

            // ColSpec with fn1=embedded lambda — embedded extend marker
            var colSpecCI = new com.gs.legend.ast.ColSpec(propName, outerFn1);

            // 2-param extend: extend(source, ColSpec)
            source = new com.gs.legend.ast.AppliedFunction(
                    "extend",
                    java.util.List.of(source, colSpecCI),
                    true);
        }
        return source;
    }

    // ==================== Source Relation Synthesis ====================

    /**
     * Synthesizes a sourceSpec ValueSpecification for a RelationalMapping.
     * This is the single source of truth for the mapping's data source.
     *
     * <p>Canonical ordering:
     * <ol>
     *   <li>tableReference(db, name) — base table</li>
     *   <li>→filter(lambda) — ~filter condition (if any)</li>
     *   <li>→join(...) — property mapping join chains (LEFT OUTER)</li>
     *   <li>→distinct() — ~distinct (if set)</li>
     * </ol>
     */
    private com.gs.legend.ast.ValueSpecification synthesizeSourceSpec(RelationalMapping rm) {
        // 1. Base: either tableReference(db, name) for physical tables, or
        //    sourceUrl(url) for external data-source classes (e.g. those bound
        //    to a {@code JsonModelConnection}). The URL form lets the synth
        //    body honestly describe its source as a URL instead of a fake
        //    {@code _json_<Class>} table; downstream extends fan the single
        //    {@code data} column into property columns via {@code get($row.data, '<prop>')}.
        com.gs.legend.ast.ValueSpecification source;
        // Bare SQL name for join condition table matching (join conditions use bare names).
        // For URL sources there is no underlying table; traverse-extends keyed off
        // this name will simply not match any join chains (URL-source classes
        // cannot have property-mapping join chains by construction).
        String tableSqlName = rm.table() != null ? rm.table().dbName() : null;
        if (rm.sourceUrl() != null) {
            source = new com.gs.legend.ast.AppliedFunction(
                    "sourceUrl",
                    java.util.List.of(new com.gs.legend.ast.CString(rm.sourceUrl())),
                    false);
        } else {
            String tableDb = rm.table().db();
            source = new com.gs.legend.ast.AppliedFunction(
                    "tableReference",
                    java.util.List.of(new com.gs.legend.ast.CString(tableDb), new com.gs.legend.ast.CString(tableSqlName)),
                    false);
        }

        // PMs are already view-resolved (Phase 1 macro expansion) — use directly.
        java.util.List<com.gs.legend.model.store.PropertyMapping> effectivePMs = rm.propertyMappings();

        // 1c. View ~filter → ->filter({row | <viewFilterExpr>})
        if (rm.view() != null && rm.view().filterMapping() != null) {
            var filterBody = resolveViewFilter(rm.view());
            if (filterBody != null) {
                var lambda = new com.gs.legend.ast.LambdaFunction(
                        java.util.List.of(new com.gs.legend.ast.Variable("row")),
                        filterBody);
                source = new com.gs.legend.ast.AppliedFunction(
                        "filter", java.util.List.of(source, lambda), true);
            }
        }

        // 1d. View ~distinct → ->distinct()
        if (rm.view() != null && rm.view().distinct()) {
            source = new com.gs.legend.ast.AppliedFunction(
                    "distinct", java.util.List.of(source), true);
        }

        // 2. Mapping ~filter → ->filter({row | <condition>})
        if (rm.filterFqn() != null) {
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

        // 3. Property mapping join chains → ->extend(traverse(...), ~[prop:t|$t.COL])
        source = addTraverseExtends(effectivePMs, tableSqlName, source);

        // 3b. Multi-join DynaFunction mappings → ->extend(PureCollection[traverse1, traverse2], ~[prop:{src,t1,t2|expr}])
        source = addMultiTraverseExtends(rm, source);

        // 4. DynaFunction property mappings → ->extend(~[prop:row|<dynaExpr>])
        //    When ~groupBy is active, DynaFunction mappings become aggregate columns
        //    in the groupBy call (step 6), so skip extends for them here.
        if (rm.groupByColumns().isEmpty()) {
            source = addDynaFunctionExtends(effectivePMs, source);
        }

        // 5. Embedded property mappings → ->extend(~prop:{->~[sub1:r|$r.COL1, ...]})
        source = addEmbeddedExtends(rm, source);

        // 6. ~groupBy → ->groupBy(source, ~[keyCols], ~[aggAlias:fn1:fn2])
        //    DynaFunction property mappings become aggregate columns;
        //    ~groupBy columns become the GROUP BY keys.
        if (!rm.groupByColumns().isEmpty()) {
            source = addMappingGroupBy(rm, source);
        }

        // 7. ~distinct → ->distinct()
        if (rm.distinct()) {
            source = new com.gs.legend.ast.AppliedFunction(
                    "distinct",
                    java.util.List.of(source),
                    true);
        }

        return source;
    }

    /**
     * Builds the class-space sourceSpec chain for an M2M mapping:
     * {@code getAll("SrcClass") → filter(src|cond) → extend(~[prop:src|expr, ...])}.
     *
     * <p>Mirrors {@link #synthesizeSourceSpec(RelationalMapping)} for relational.
     * The chain is walked by MappingResolver to derive StoreResolution, and by
     * PlanGenerator to generate SQL (filter in chain, not bespoke filterExpr).
     */
    private com.gs.legend.ast.ValueSpecification synthesizeM2MSourceSpec(PureClassMapping pcm) {
        // 1. Base: getAll(sourceClass)
        com.gs.legend.ast.ValueSpecification source = new com.gs.legend.ast.AppliedFunction(
                "getAll",
                java.util.List.of(new com.gs.legend.ast.PackageableElementPtr(pcm.sourceClassName())),
                false);

        // 2. Filter: → filter(src | filterExpr)
        if (pcm.filter() != null) {
            var lambda = new com.gs.legend.ast.LambdaFunction(
                    java.util.List.of(new com.gs.legend.ast.Variable("src")),
                    pcm.filter());
            source = new com.gs.legend.ast.AppliedFunction(
                    "filter", java.util.List.of(source, lambda), true);
        }

        // 3. Extends: → extend(~[prop:src|expr, ...])
        var colSpecs = new java.util.ArrayList<com.gs.legend.ast.ColSpec>();
        for (var entry : pcm.propertyExpressions().entrySet()) {
            var srcVar = new com.gs.legend.ast.Variable("src");
            var lambda = new com.gs.legend.ast.LambdaFunction(
                    java.util.List.of(srcVar), entry.getValue());
            colSpecs.add(new com.gs.legend.ast.ColSpec(entry.getKey(), lambda));
        }
        if (!colSpecs.isEmpty()) {
            com.gs.legend.ast.ColumnInstance colSpecCI = (colSpecs.size() == 1)
                    ? colSpecs.get(0)
                    : new com.gs.legend.ast.ColSpecArray(colSpecs);
            source = new com.gs.legend.ast.AppliedFunction(
                    "extend", java.util.List.of(source, colSpecCI), true);
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
            java.util.List<com.gs.legend.model.store.PropertyMapping> propertyMappings,
            String mainTable,
            com.gs.legend.ast.ValueSpecification source) {

        // Group join-chain properties by their full chain path
        var chainGroups = new LinkedHashMap<java.util.List<Join>,
                java.util.List<com.gs.legend.model.store.PropertyMapping>>();
        for (var pm : propertyMappings) {
            if (!pm.hasJoinChain()) continue;
            chainGroups.computeIfAbsent(pm.joinChain(), k -> new java.util.ArrayList<>()).add(pm);
        }

        if (chainGroups.isEmpty()) return source;

        for (var entry : chainGroups.entrySet()) {
            java.util.List<Join> chainJoins = entry.getKey();
            var props = entry.getValue();

            // Build traverse chain
            var traverseExpr = buildTraverseChain(mainTable, chainJoins);

            // Build colSpecs: ~[prop1:{src,tgt|$tgt.COL1}, prop2:{src,tgt|$tgt.COL2}]
            var srcVar = new com.gs.legend.ast.Variable("src");
            var tgtVar = new com.gs.legend.ast.Variable("tgt");
            var params = java.util.List.of(srcVar, tgtVar);
            var colSpecs = new java.util.ArrayList<com.gs.legend.ast.ColSpec>();
            for (var pm : props) {
                com.gs.legend.ast.ValueSpecification body;
                if (pm.hasDynaExpression()) {
                    // Combined join + DynaFunction: expression already uses $src/$tgt references
                    body = pm.dynaExpression();
                } else {
                    // Simple column access on terminal table: $tgt.COL
                    body = new com.gs.legend.ast.AppliedProperty(
                            pm.columnName(),
                            java.util.List.of(new com.gs.legend.ast.Variable("tgt")));
                }
                var lambda = new com.gs.legend.ast.LambdaFunction(params, body);
                colSpecs.add(new com.gs.legend.ast.ColSpec(pm.propertyName(), lambda));
            }

            com.gs.legend.ast.ColumnInstance colSpecCI = (colSpecs.size() == 1)
                    ? colSpecs.get(0)
                    : new com.gs.legend.ast.ColSpecArray(colSpecs);

            source = new com.gs.legend.ast.AppliedFunction(
                    "extend",
                    java.util.List.of(source, traverseExpr, colSpecCI),
                    true);
        }

        return source;
    }

    /**
     * Adds {@code ->extend(PureCollection[traverse1, traverse2, ...], ~[prop:{src,t1,t2|expr}])}
     * for multi-join DynaFunction property mappings. Each property gets its own extend call
     * with a PureCollection of traverse expressions and a colSpec lambda whose parameters
     * are bound to the source table and each terminal table.
     */
    private com.gs.legend.ast.ValueSpecification addMultiTraverseExtends(
            RelationalMapping rm,
            com.gs.legend.ast.ValueSpecification source) {

        String mainTable = rm.table().dbName();

        for (var pm : rm.propertyMappings()) {
            if (!pm.hasMultiJoinChains()) continue;

            // Build a traverse expression for each join chain
            var traverseExprs = new java.util.ArrayList<com.gs.legend.ast.ValueSpecification>();
            for (var chain : pm.multiJoinChains()) {
                traverseExprs.add(buildTraverseChain(mainTable, chain));
            }

            // Wrap in PureCollection so ExtendChecker sees multiple traverses
            var traverseCollection = new com.gs.legend.ast.PureCollection(traverseExprs);

            // Build colSpec lambda: {src, t1, t2, ... | <dynaExpr>}
            // The dynaExpr already uses $src, $t1, $t2 references from RelationalMappingConverter
            var lambdaParams = new java.util.ArrayList<com.gs.legend.ast.Variable>();
            lambdaParams.add(new com.gs.legend.ast.Variable("src"));
            for (int i = 0; i < pm.multiJoinChains().size(); i++) {
                lambdaParams.add(new com.gs.legend.ast.Variable("t" + (i + 1)));
            }
            var lambda = new com.gs.legend.ast.LambdaFunction(lambdaParams, pm.dynaExpression());
            var colSpecCI = new com.gs.legend.ast.ColSpec(pm.propertyName(), lambda);

            source = new com.gs.legend.ast.AppliedFunction(
                    "extend",
                    java.util.List.of(source, traverseCollection, colSpecCI),
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
            java.util.List<com.gs.legend.model.store.PropertyMapping> propertyMappings,
            com.gs.legend.ast.ValueSpecification source) {

        var colSpecs = new java.util.ArrayList<com.gs.legend.ast.ColSpec>();

        for (var pm : propertyMappings) {
            if (pm.hasJoinChain()) continue; // handled by addTraverseExtends
            if (pm.hasMultiJoinChains()) continue; // handled by addMultiTraverseExtends

            var rowVar = new com.gs.legend.ast.Variable("row");
            com.gs.legend.ast.ValueSpecification body;

            if (pm.hasDynaExpression()) {
                // DynaFunction: already a ValueSpec tree (from RelationalMappingConverter)
                body = pm.dynaExpression();
            } else if (pm.hasEnumMapping()) {
                // Enum mapping: if(equal($row.COL, dbVal1), 'ENUM1', if(..., null))
                body = synthesizeEnumIf(rowVar, pm);
            } else if (pm.hasExpression()) {
                // Expression access: get($row.COLUMN, 'key') with optional cast
                body = synthesizeExpressionAccess(rowVar, pm);
            } else {
                continue; // simple column rename — handled by select at the end
            }

            var lambda = new com.gs.legend.ast.LambdaFunction(
                    java.util.List.of(rowVar), body);
            colSpecs.add(new com.gs.legend.ast.ColSpec(pm.propertyName(), lambda));
        }

        if (colSpecs.isEmpty()) return source;

        com.gs.legend.ast.ColumnInstance colSpecCI = (colSpecs.size() == 1)
                ? colSpecs.get(0)
                : new com.gs.legend.ast.ColSpecArray(colSpecs);

        return new com.gs.legend.ast.AppliedFunction(
                "extend",
                java.util.List.of(source, colSpecCI),
                true);
    }

    // ========== View Macro Resolution ==========

    /**
     * Resolves mapping property mappings through the View definition.
     * Each mapping PM's column name is looked up in the View's column mappings,
     * and the PM is rewritten to reference the physical column/join/dyna directly.
     * The view acts as a macro — its column names are resolved away.
     *
     * <ul>
     *   <li>{@code ColumnRef(T, col)} → rewrite PM to use physical column name</li>
     *   <li>{@code JoinNavigation(@J|T.col)} → rewrite PM to joinChain</li>
     *   <li>{@code FunctionCall/complex} → rewrite PM to dynaFunction</li>
     * </ul>
     */
    private java.util.List<com.gs.legend.model.store.PropertyMapping> resolvePropertyMappingsThroughView(
            java.util.List<com.gs.legend.model.store.PropertyMapping> mappingPMs,
            com.gs.legend.model.store.View view,
            String mainTable) {
        var result = new java.util.ArrayList<com.gs.legend.model.store.PropertyMapping>();
        for (var pm : mappingPMs) {
            // Look up the PM's column name in the view
            var viewCol = view.findColumn(pm.columnName());
            if (viewCol.isEmpty()) {
                // Not a view column — pass through unchanged (e.g., join/dyna PMs from the mapping itself)
                result.add(pm);
                continue;
            }
            var expr = viewCol.get().expression();
            String propName = pm.propertyName();
            switch (expr) {
                case com.gs.legend.model.def.RelationalOperation.ColumnRef ref ->
                        // Simple column: empId:EmpView.emp_id → emp_id maps to EMPLOYEES.ID → column("empId", "ID")
                        result.add(com.gs.legend.model.store.PropertyMapping.column(propName, ref.column()));

                case com.gs.legend.model.def.RelationalOperation.JoinNavigation nav -> {
                    java.util.List<Join> resolvedJoins = resolveJoinChain(nav.joinChain());
                    if (nav.terminal() instanceof com.gs.legend.model.def.RelationalOperation.ColumnRef cr) {
                        result.add(com.gs.legend.model.store.PropertyMapping.joinChain(
                                propName, cr.column(), resolvedJoins));
                    } else if (nav.terminal() != null) {
                        var terminalTables = RelationalMappingConverter.collectTableNames(nav.terminal());
                        String terminalTable = terminalTables.stream()
                                .filter(t -> !t.equals(mainTable))
                                .findFirst().orElse(terminalTables.iterator().next());
                        var tableToParam = new java.util.HashMap<String, String>();
                        tableToParam.put(mainTable, "src");
                        tableToParam.put(terminalTable, "tgt");
                        var vsExpr = RelationalMappingConverter.convert(nav.terminal(), tableToParam);
                        result.add(com.gs.legend.model.store.PropertyMapping.dynaFunctionWithJoin(
                                propName, vsExpr, resolvedJoins));
                    }
                }

                default -> {
                    var joinNavs = PureModelBuilder.findAllJoinNavigations(expr);
                    if (!joinNavs.isEmpty() && joinNavs.size() == 1) {
                        var nav = joinNavs.get(0);
                        var terminalTables = RelationalMappingConverter.collectTableNames(nav.terminal());
                        String terminalTable = terminalTables.stream()
                                .filter(t -> !t.equals(mainTable))
                                .findFirst().orElse(terminalTables.iterator().next());
                        var tableToParam = new java.util.HashMap<String, String>();
                        tableToParam.put(mainTable, "src");
                        tableToParam.put(terminalTable, "tgt");
                        java.util.List<Join> resolvedJoins = resolveJoinChain(nav.joinChain());
                        var vsExpr = RelationalMappingConverter.convert(expr, tableToParam);
                        result.add(com.gs.legend.model.store.PropertyMapping.dynaFunctionWithJoin(
                                propName, vsExpr, resolvedJoins));
                    } else if (joinNavs.size() >= 2) {
                        var tableToParam = new java.util.HashMap<String, String>();
                        tableToParam.put(mainTable, "src");
                        var allJoinChains = new java.util.ArrayList<java.util.List<Join>>();
                        for (int ji = 0; ji < joinNavs.size(); ji++) {
                            var nav = joinNavs.get(ji);
                            String paramName = "t" + (ji + 1);
                            var termTables = RelationalMappingConverter.collectTableNames(nav.terminal());
                            String termTable = termTables.stream()
                                    .filter(t -> !t.equals(mainTable))
                                    .findFirst().orElse(termTables.iterator().next());
                            tableToParam.put(termTable, paramName);
                            allJoinChains.add(resolveJoinChain(nav.joinChain()));
                        }
                        var vsExpr = RelationalMappingConverter.convert(expr, tableToParam);
                        result.add(com.gs.legend.model.store.PropertyMapping.dynaFunctionWithMultiJoin(
                                propName, vsExpr, allJoinChains));
                    } else {
                        var vsExpr = RelationalMappingConverter.convert(expr);
                        result.add(com.gs.legend.model.store.PropertyMapping.dynaFunction(propName, vsExpr));
                    }
                }
            }
        }
        return result;
    }

    /**
     * Synthesizes a standard {@code groupBy(source, ~[keyCols], ~[aggAlias:fn1:fn2])} call
     * from mapping-level {@code ~groupBy} directive and DynaFunction property mappings.
     *
     * <p>Key columns come from {@code ~groupBy}; aggregate columns come from DynaFunction
     * property mappings. Each DynaFunction is decomposed into fn1 (value extraction) and
     * fn2 (aggregate function). E.g., {@code sum($row.QTY)} →
     * {@code fn1={c|$c.QTY}, fn2={y|$y->sum()}}.
     */
    private com.gs.legend.ast.ValueSpecification addMappingGroupBy(
            RelationalMapping rm,
            com.gs.legend.ast.ValueSpecification source) {

        // 1. Key columns from ~groupBy directive
        var keyCols = rm.groupByColumns().stream()
                .map(com.gs.legend.ast.ColSpec::new)
                .toList();

        // 2. Aggregate columns from DynaFunction property mappings
        var aggCols = new java.util.ArrayList<com.gs.legend.ast.ColSpec>();
        for (var pm : rm.propertyMappings()) {
            if (!pm.hasDynaExpression()) continue;
            if (pm.hasJoinChain()) continue;

            // Decompose aggregate: outerFunc(innerExpr) → fn1={c|innerExpr}, fn2={y|$y->outerFunc()}
            var dynaExpr = pm.dynaExpression();
            if (dynaExpr instanceof com.gs.legend.ast.AppliedFunction aggFunc && !aggFunc.parameters().isEmpty()) {
                // fn1: extract the value — body is the argument to the aggregate
                // dynaExpression uses $row, so fn1 param must also be "row"
                var fn1Body = aggFunc.parameters().get(0);
                var fn1 = new com.gs.legend.ast.LambdaFunction(
                        java.util.List.of(new com.gs.legend.ast.Variable("row")), fn1Body);

                // fn2: apply the aggregate function to the collection parameter
                var fn2Param = new com.gs.legend.ast.Variable("y");
                var fn2Body = new com.gs.legend.ast.AppliedFunction(
                        aggFunc.function(),
                        java.util.List.of(fn2Param),
                        true);
                var fn2 = new com.gs.legend.ast.LambdaFunction(
                        java.util.List.of(fn2Param), fn2Body);

                aggCols.add(new com.gs.legend.ast.ColSpec(pm.propertyName(), fn1, fn2));
            }
        }

        // Build: groupBy(source, ~[keyCols], ~[aggCols:fn1:fn2])
        var keyArray = new com.gs.legend.ast.ColSpecArray(keyCols);

        com.gs.legend.ast.ColumnInstance aggCI = (aggCols.size() == 1)
                ? aggCols.get(0)
                : new com.gs.legend.ast.ColSpecArray(aggCols);

        return new com.gs.legend.ast.AppliedFunction(
                "groupBy",
                java.util.List.of(source, keyArray, aggCI),
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
     * Synthesizes expression access: {@code to(get($row.COLUMN, 'key'), @Type)}.
     *
     * <p>Uses {@code to()} (not {@code cast()}) because PlanGenerator's {@code to()} converts
     * {@code VariantAccess} ({@code ->}) to {@code VariantTextAccess} ({@code ->>}) before
     * casting, which strips JSON string quotes. The castType comes from the mapping grammar
     * ({@code ->get('key', @Type)}) or is auto-inferred from the class property type.
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

        // get($row.COLUMN, 'key') → variant access (-> in SQL)
        var colAccess = new com.gs.legend.ast.AppliedProperty(
                pm.columnName(), java.util.List.of(rowVar));
        com.gs.legend.ast.ValueSpecification body = new com.gs.legend.ast.AppliedFunction(
                "get", java.util.List.of(colAccess, new com.gs.legend.ast.CString(parsedAccess.jsonKey())));

        // to(get(...), @Type) → text extraction (->> in SQL) + CAST.
        // Parser-layer artifact emits a purely syntactic TypeAnnotation; the downstream
        // TypeConversionChecker resolves it to an m3.Type via the ModelContext.
        //
        // The raw cast type from parsedAccess is a simple name ("Integer") or FQN — we must
        // resolve it through the model's ImportScope so downstream ModelContext.findType (which
        // is FQN-only by contract) receives a canonical name. Without this, simple names leak
        // past NameResolver via this programmatic construction path.
        if (parsedAccess.castType() != null) {
            String castFqn = model.imports().resolve(parsedAccess.castType(), model.allFqns());
            var typeRef = new com.gs.legend.ast.TypeAnnotation.Named(castFqn);
            body = new com.gs.legend.ast.AppliedFunction(
                    "to", java.util.List.of(body, typeRef));
        }

        return body;
    }

    /**
     * Resolves a list of JoinChainElements to Join objects via model lookup.
     */
    private java.util.List<Join> resolveJoinChain(java.util.List<com.gs.legend.model.def.JoinChainElement> chain) {
        return chain.stream().map(jce -> model.findJoin(jce.databaseName(), jce.joinName())
                .orElseThrow(() -> new IllegalStateException(
                        "Join not found: " + jce.databaseName() + "." + jce.joinName())))
                .toList();
    }

    /**
     * Builds a traverse chain from join definitions:
     * {@code traverse(T_DEPT, {prev,hop|...})->traverse(T_ORG, {prev,hop|...})}
     *
     * Uses {@link RelationalMappingConverter#convert(RelationalOperation, java.util.Map)} to convert
     * the full join condition to Pure AST, supporting complex conditions (multi-column, function-based, etc.).
     */
    private com.gs.legend.ast.ValueSpecification buildTraverseChain(
            String startTable, java.util.List<Join> resolvedJoins) {

        String[] currentTableHolder = { startTable };
        com.gs.legend.ast.ValueSpecification traverseExpr = null;

        for (Join join : resolvedJoins) {
            String curTable = currentTableHolder[0];

            // Find the target table. For self-joins (all table names == curTable),
            // target stays as curTable — TargetColumnRef disambiguates in the condition.
            var tableNames = RelationalMappingConverter.collectTableNames(join.condition());
            String targetTable = tableNames.stream()
                    .filter(t -> !t.equals(curTable))
                    .findFirst()
                    .orElse(curTable); // self-join: target == source

            // Convert join condition. For self-joins, use 3-param convert with "hop"
            // as targetParamName so TargetColumnRef maps to Variable("hop").
            var tableToParam = new java.util.HashMap<String, String>();
            tableToParam.put(curTable, "prev");
            if (!targetTable.equals(curTable)) tableToParam.put(targetTable, "hop");
            var condBody = RelationalMappingConverter.convert(join.condition(), tableToParam, "hop");

            // tableReference(db, name) — structured for model-world lookup
            var targetRef = new com.gs.legend.ast.AppliedFunction(
                    "tableReference",
                    java.util.List.of(new com.gs.legend.ast.CString(join.db()), new com.gs.legend.ast.CString(targetTable)),
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
     * Resolves View ~groupBy key columns to class property names.
     * For each groupBy ColumnRef, finds the view column whose expression matches,
     * then finds the resolved PM whose physical column name matches.
     */
    private static java.util.List<String> resolveViewGroupByKeys(
            com.gs.legend.model.store.View view,
            java.util.List<com.gs.legend.model.store.PropertyMapping> resolvedPMs) {
        var keys = new java.util.ArrayList<String>();
        for (var groupByOp : view.groupBy()) {
            if (groupByOp instanceof com.gs.legend.model.def.RelationalOperation.ColumnRef gbRef) {
                // Find the view column whose expression matches this groupBy key
                for (var vc : view.columnMappings()) {
                    if (vc.expression() instanceof com.gs.legend.model.def.RelationalOperation.ColumnRef vcRef
                            && vcRef.column().equals(gbRef.column())) {
                        // Use the physical column name — the source TDS has physical columns
                        keys.add(vcRef.column());
                        break;
                    }
                }
            }
        }
        return keys;
    }

    /**
     * Resolves a View's ~filter (named filter reference) to a ValueSpecification condition body.
     * The filter reference is stored as a Literal "~filter:FilterName" by the parser.
     */
    private com.gs.legend.ast.ValueSpecification resolveViewFilter(com.gs.legend.model.store.View view) {
        var filterOp = view.filterMapping();
        if (filterOp instanceof com.gs.legend.model.def.RelationalOperation.Literal lit) {
            String text = lit.value().toString();
            if (text.startsWith("~filter:")) {
                String filterName = text.substring("~filter:".length());
                var filter = model.getFilter(filterName).orElse(null);
                if (filter != null) {
                    return com.gs.legend.model.RelationalMappingConverter.convert(filter.condition());
                }
                throw new IllegalStateException(
                        "View filter '" + filterName + "' not found in database for view " + view.name());
            }
        }
        // Inline expression (future): convert directly
        return com.gs.legend.model.RelationalMappingConverter.convert(filterOp);
    }

    /**
     * Resolves a ~filter name to a ValueSpecification condition body.
     * Returns null if the filter is not found.
     */
    private com.gs.legend.ast.ValueSpecification resolveFilterCondition(RelationalMapping rm) {
        Filter filter = model.getFilter(rm.filterFqn())
                .orElseThrow(() -> new IllegalStateException(
                        "Filter '" + rm.filterFqn() + "' not found for mapping of "
                                + rm.pureClassFqn()));
        return RelationalMappingConverter.convert(filter.condition());
    }
}
