// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.builtin.Pure;
import com.legend.compiler.ModelBuilder;
import com.legend.compiler.SynthFqn;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.error.NotImplementedException;
import com.legend.protocol.Multiplicity;
import com.legend.model.NormalizedModel;
import com.legend.model.ParsedModel;
import com.legend.protocol.TypeExpression;
import com.legend.model.AssociationDefinition;
import com.legend.model.AssociationMapping;
import com.legend.model.AssociationPropertyMapping;
import com.legend.model.ClassDefinition;
import com.legend.model.ClassMapping;
import com.legend.model.ComparisonOp;
import com.legend.model.DatabaseDefinition;
import com.legend.model.EnumerationMapping;
import com.legend.model.FilterMapping;
import com.legend.model.FilterPointer;
import com.legend.model.FunctionDefinition;
import com.legend.model.JoinChainElement;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.LogicalOp;
import com.legend.model.MappingDefinition;
import com.legend.model.PackageableElement;
import com.legend.model.PropertyMapping;
import com.legend.protocol.Realization;
import com.legend.model.RelationalDataType;
import com.legend.model.RelationalOperation;
import com.legend.model.SynthHat;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.CFloat;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.KeyExpression;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.NewInstance;
import com.legend.protocol.spec.NewInstanceCast;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.TypeAnnotation;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
/**
 * Join-chain hop emission (Pass 1 structural, Pass 2 nested JoinNav): slot minting, path dedup, chain walking. Split from MappingNormalizer (the Doors split).
 */
final class JoinChainEmission {

    private JoinChainEmission() {}

    static void emitHopsForStructuralPm(Pipeline p, PropertyMapping pm,
                                               String ownerClassFqn, String mainDb,
                                               String mainTable, Variable rowBind,
                                               ModelBuilder model, LegacyMappingDefinition md) {
        switch (pm) {
            case PropertyMapping.Join j when p.droppedRoutedProps
                    .contains(j.propertyName()) -> {
                // routed property dropped at classification (poisoned
                // reason on the ledger) — no hops, no slot, no binding
            }
            case PropertyMapping.Join j -> emitJoinChain(p, j.joins(), j.database(),
                    j.propertyName(), ownerClassFqn, mainDb, mainTable,
                    rowBind, model, md, /*classTypedTerminus*/ true,
                    j.targetSetId());
            case PropertyMapping.JoinTerminalColumn jtc -> emitJoinChain(p,
                    jtc.joins(), jtc.database(), /*propName*/ null, ownerClassFqn,
                    mainDb, mainTable, rowBind, model, md, /*classTypedTerminus*/ false);
            case PropertyMapping.LocalProperty lp -> emitHopsForStructuralPm(p, lp.body(),
                    ownerClassFqn, mainDb, mainTable, rowBind, model, md);
            case PropertyMapping.OtherwiseEmbedded oe -> {
                emitOtherwiseEmbeddedHop(p, oe,
                        ownerClassFqn, mainDb, mainTable, rowBind, model, md);
                // the PARTIAL's structural sub-PMs hoist like a plain
                // embedded block (bondClassification: @J inside
                // Otherwise(...)) — without them the partial's ctor
                // field reads a slot that was never minted
                ClassDefinition oeOwner = MappingNormalizer.classDef(model, ownerClassFqn)
                        .orElse(null);
                TypeExpression oeType = oeOwner == null ? null
                        : MappingNormalizer.findPropertyTypeDeep(oeOwner,
                                oe.propertyName(), model);
                if (oeType instanceof TypeExpression.NameRef nr) {
                    for (PropertyMapping sub : oe.embedded()) {
                        if (sub instanceof PropertyMapping.Join j
                                && p.aliasToTargetTable.containsKey(j.propertyName())
                                && classTypedTargetIfMapped(nr.name(),
                                        j.propertyName(), model) != null
                                && !nr.name().equals(
                                        p.navSlotOwner.get(j.propertyName()))) {
                            throw new NotImplementedException(
                                    "Otherwise-embedded sub-PM '"
                                  + j.propertyName()
                                  + "' collides with an existing pipeline slot"
                                  + " of the same name. Mapping="
                                  + md.qualifiedName());
                        }
                        recordNavSlotOwner(p, sub, nr.name(), model);
                        emitHopsForStructuralPm(p, sub, nr.name(), mainDb,
                                mainTable, rowBind, model, md);
                    }
                }
            }
            case PropertyMapping.Embedded emb -> {
                // sub-PM join chains hoist into the TOP pipeline (the
                // embedded instance shares the owner's row); the owner for
                // class-typed detection is the EMBEDDED class
                ClassDefinition owner = MissProbe.knownMiss(MappingNormalizer.classDef(model, ownerClassFqn));
                TypeExpression propType = owner == null ? null
                        : MappingNormalizer.findPropertyTypeDeep(owner, emb.propertyName(), model);
                if (propType instanceof TypeExpression.NameRef nr) {
                    for (PropertyMapping sub : emb.propertyMappings()) {
                        if (sub instanceof PropertyMapping.Join j
                                && p.aliasToTargetTable.containsKey(j.propertyName())
                                && classTypedTargetIfMapped(nr.name(),
                                        j.propertyName(), model) != null
                                && !nr.name().equals(
                                        p.navSlotOwner.get(j.propertyName()))) {
                            throw new NotImplementedException(
                                    "Embedded sub-PM '" + j.propertyName()
                                  + "' collides with an existing pipeline slot"
                                  + " of the same name; distinct same-named"
                                  + " class-typed joins across embedded levels"
                                  + " are a roadmap feature. Mapping="
                                  + md.qualifiedName());
                        }
                        recordNavSlotOwner(p, sub, nr.name(), model);
                        emitHopsForStructuralPm(p, sub, nr.name(), mainDb,
                                mainTable, rowBind, model, md);
                    }
                }
            }
            case PropertyMapping.InlineEmbedded ie -> {
                // the referenced set's PMs splice at materialization (the
                // inline instance shares the owner's row) — their
                // STRUCTURAL hops hoist into the TOP pipeline exactly like
                // a direct embedded block (firm() Inline[f1] where f1
                // carries employees: @firmEmployees)
                ClassMapping.Relational referenced = null;
                for (ClassMapping cm : md.classMappings()) {
                    if (cm instanceof ClassMapping.Relational r2
                            && java.util.Objects.equals(
                                    MappingNormalizer.setIdOf(r2), ie.setId())) {
                        referenced = r2;
                        break;
                    }
                }
                ClassDefinition owner = MappingNormalizer.classDef(model, ownerClassFqn).orElseThrow(() -> new IllegalStateException("F7.8: class unresolved at JoinChainEmission#2 (this default NEVER fired on the corpus census; a miss here is a real model gap): " + ownerClassFqn));
                TypeExpression propType = owner == null ? null
                        : MappingNormalizer.findPropertyTypeDeep(owner,
                                ie.propertyName(), model);
                if (referenced != null
                        && propType instanceof TypeExpression.NameRef) {
                    // the spliced PMs belong to the REFERENCED set's class
                    // (a subclass of the declared prop type — Inline[airline]
                    // splices Airline's own props like planes)
                    String inlCls = referenced.className();
                    for (PropertyMapping sub : referenced.propertyMappings()) {
                        if (sub instanceof PropertyMapping.Join j
                                && p.aliasToTargetTable.containsKey(j.propertyName())
                                && classTypedTargetIfMapped(inlCls,
                                        j.propertyName(), model) != null
                                && !inlCls.equals(
                                        p.navSlotOwner.get(j.propertyName()))) {
                            throw new NotImplementedException(
                                    "Inline-embedded sub-PM '" + j.propertyName()
                                  + "' collides with an existing pipeline slot"
                                  + " of the same name. Mapping="
                                  + md.qualifiedName());
                        }
                        recordNavSlotOwner(p, sub, inlCls, model);
                        emitHopsForStructuralPm(p, sub, inlCls, mainDb,
                                mainTable, rowBind, model, md);
                    }
                }
            }
            default -> { /* Column / Enum / Expression:
                            nested JoinNav handled in Pass 2 */ }
        }
    }

    /**
     * Emit the OtherwiseEmbedded fallback's pipeline step: a
     * {@code legacyNavigate(~<propName>: getAll(Target), {sr,tr|cond})}
     * binding the fallback class instance as a named slot. The map
     * terminal then composes {@code otherwise(^Inner(...), $row.slot)}.
     */
    static void emitOtherwiseEmbeddedHop(Pipeline p,
                                                PropertyMapping.OtherwiseEmbedded oe,
                                                String ownerClassFqn, String mainDb,
                                                String mainTable, Variable rowBind,
                                                ModelBuilder model, LegacyMappingDefinition md) {
        if (!(oe.fallback() instanceof PropertyMapping.Join joinFallback)) {
            throw new NotImplementedException(
                    "OtherwiseEmbedded PM '" + oe.propertyName() + "' fallback kind "
                  + oe.fallback().getClass().getSimpleName()
                  + " not supported (Join only). Mapping=" + md.qualifiedName());
        }
        ClassDefinition owner = MappingNormalizer.classDef(model, ownerClassFqn).orElseThrow(() ->
                new ModelException(LegendCompileException.Phase.NORMALIZE,
                        "OtherwiseEmbedded PM '" + oe.propertyName()
                        + "': unknown owner class '" + ownerClassFqn
                        + "'; mapping=" + md.qualifiedName()));
        TypeExpression propType = MappingNormalizer.findPropertyTypeDeep(owner, oe.propertyName(), model);
        if (!(propType instanceof TypeExpression.NameRef nr)) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                    "OtherwiseEmbedded PM '" + oe.propertyName()
                  + "' has non-class property type; mapping=" + md.qualifiedName());
        }
        String targetClassFqn = nr.name();
        if (!model.isMappedClass(targetClassFqn)) {
            throw new NotImplementedException(
                    "OtherwiseEmbedded PM '" + oe.propertyName() + "' target class '"
                  + targetClassFqn + "' is not mapped; mapping=" + md.qualifiedName());
        }
        // Emit the fallback Join chain as a legacyNavigate step under
        // a slot named after the property. The terminal `map` reads
        // $row.<propName> and composes with the partial ^Inner(...).
        emitJoinChain(p, joinFallback.joins(), joinFallback.database(),
                oe.propertyName(), ownerClassFqn, mainDb, mainTable,
                rowBind, model, md, /*classTypedTerminus*/ true);
    }

    /**
     * PER-ARM routed chains (push-into-arm): the member routes diverge, so
     * NO shared prefix exists to emit physically — each route's mid hops
     * live INSIDE the owning member's thread (union synthesis inbound
     * chains) and the ONE navigate reads each route's FIRST hop. The
     * representative hop for the slot is the primary route's first hop
     * (the one touching the main table).
     */
    private static List<JoinChainElement> perArmHops(Pipeline p,
            @com.legend.Nullable String propName, @com.legend.Nullable String targetClassFqn,
            List<JoinChainElement> hops) {
        if (targetClassFqn == null || hops.size() < 2) {
            return hops;
        }
        List<UnionSynthesis.UnionRoute> rr = p.unionRoutes.get(propName);
        return rr != null && !UnionSynthesis.uniformChainedRoutes(
                UnionSynthesis.memberJoins(rr))
                ? List.of(hops.get(0)) : hops;
    }

    /**
     * Emit one join chain. Intermediate hops are clean-sheet
     * {@code join} steps. The final hop is a {@code legacyNavigate}
     * iff {@code classTypedTerminus} is true AND the property's
     * declared target class is mapped; otherwise the final hop is
     * also a clean {@code join} binding a physical sub-row.
     *
     * <p>Shared chain prefixes across PMs dedup via
     * {@link Pipeline#aliasToTargetTable}.
     */
    static void emitJoinChain(Pipeline p, List<JoinChainElement> hops,
                                     @com.legend.Nullable String chainDb,
                                     @com.legend.Nullable String propName,
                                     @com.legend.Nullable String ownerClassFqn, String mainDb,
                                     String mainTable, Variable rowBind,
                                     ModelBuilder model, LegacyMappingDefinition md,
                                     boolean classTypedTerminus) {
        emitJoinChain(p, hops, chainDb, propName, ownerClassFqn, mainDb, mainTable,
                rowBind, model, md, classTypedTerminus, null);
    }

    /** {@code routedSetId}: the PM's {@code prop[setId]} route. When the
     * property has exactly ONE route entry, the declared target class has
     * NO Relational set of its own (an Operation-mapped hierarchy root
     * such as the metamodel store's RelationalOperationElement) and the
     * routed set's class is a strict subclass, the navigation lands on
     * THAT class — the engine's one routed set IS the rows. Several
     * entries ({@code vehicles[p, car]} + {@code vehicles[p, bike]}) keep
     * the per-arm union dispatch. */
    static void emitJoinChain(Pipeline p, List<JoinChainElement> hops,
                                     @com.legend.Nullable String chainDb,
                                     @com.legend.Nullable String propName,
                                     @com.legend.Nullable String ownerClassFqn, String mainDb,
                                     String mainTable, Variable rowBind,
                                     ModelBuilder model, LegacyMappingDefinition md,
                                     boolean classTypedTerminus,
                                     @com.legend.Nullable String routedSetId) {
        String targetClassFqn = null;
        if (classTypedTerminus && propName != null) {
            targetClassFqn = classTypedTargetIfMapped(ownerClassFqn, propName, model);
            List<UnionSynthesis.UnionRoute> routeEntries = propName == null
                    ? null : p.unionRoutes.get(propName);
            if (targetClassFqn != null && routedSetId != null
                    && (routeEntries == null || routeEntries.size() == 1)
                    && !MappingNormalizer.hasMainTable(md, targetClassFqn, model)
                    && MappingNormalizer.findSetById(md, model, routedSetId)
                            instanceof ClassMapping routed
                    && !routed.className().equals(targetClassFqn)
                    && UnionSynthesis.isSubclassOf(routed.className(),
                            targetClassFqn, model)) {
                targetClassFqn = routed.className();
                // the navigation is no longer a union route: its target
                // rows are the member's own extent, keys unsuffixed
                p.unionRoutes.remove(propName);
            }
        }
        hops = perArmHops(p, propName, targetClassFqn, hops);
        int lastIdx = hops.size() - 1;
        List<String> prefixPath = new ArrayList<>();
        String prevAlias = null;
        String prevTable = mainTable;
        for (int i = 0; i < hops.size(); i++) {
            JoinChainElement hop = hops.get(i);
            prefixPath.add(hop.joinName());
            boolean isLastHop = i == lastIdx;
            boolean emitNavigate = isLastHop && targetClassFqn != null;

            // Dedup. Physical sub-row hops are identified by their STRUCTURED
            // prefix path (the ordered join names from the main table), so a
            // chain [A, B] and a single join literally named "A__B" stay
            // distinct. Class-instance hops dedup on the property name, since
            // their slot is the property, not a flattened chain.
            List<String> pathKey = emitNavigate ? null : List.copyOf(prefixPath);
            String navAlias = emitNavigate
                    ? mintNavSlotAlias(p, model, mainDb, mainTable, Objects
                            .requireNonNull(propName, "nav hop needs propName")) : null;
            if (emitNavigate) {
                if (p.aliasToTargetTable.containsKey(navAlias)) {
                    prevTable = p.aliasToTargetTable.get(navAlias);
                    prevAlias = navAlias;
                    continue;
                }
            } else {
                String existing = p.pathToSlot.get(pathKey);
                if (existing != null) {
                    prevTable = p.aliasToTargetTable.get(existing);
                    prevAlias = existing;
                    continue;
                }
            }
            // The slot name read back in generated Pure ($row.<slot>). For a
            // class hop it is the property name; for a physical hop it is the
            // human-readable "__"-joined path, disambiguated only if that name
            // would clash with a different slot (the structured pathKey, not
            // the name, is the identity readers resolve through slotFor).
            String slotAlias = emitNavigate ? navAlias : uniqueSlotName(p, pathKey);

            String hopDb = hop.databaseName() != null ? hop.databaseName()
                    : (chainDb != null ? chainDb : mainDb);
            DatabaseDefinition.JoinDefinition jd = model.findJoin(hopDb, hop.joinName())
                    .orElseThrow(() -> new ModelException(LegendCompileException.Phase.NORMALIZE, 
                            "Join '" + hop.joinName() + "' not found in db '"
                          + hopDb + "'; PM='" + propName + "', mapping="
                          + md.qualifiedName()));
            // PASS 1: the class's BACKING view always substitutes to its
            // physical expressions (its row semantics live in the class
            // pipeline). Then a SOLE remaining non-source view candidate is
            // the join's TARGET — expanded as a relation, never substituted.
            RelationalOperation joinCond = p.backingView == null ? jd.operation()
                    : MappingNormalizer.resolveViewRefsInJoin(jd.operation(), hopDb, prevTable,
                            model, md, p.backingView, p.backingView);
            Set<String> condTables = new LinkedHashSet<>();
            RelOpTranslator.collectTablesIn(joinCond, condTables);
            condTables.remove(prevTable);
            String viewTarget = condTables.size() == 1
                    && model.findView(hopDb, condTables.iterator().next()).isPresent()
                    ? condTables.iterator().next() : null;
            if (viewTarget != null && emitNavigate) {
                RelationalOperation sub = plainClassViewCond(joinCond,
                        viewTarget, targetClassFqn, hopDb, model, md);
                if (sub != null) { joinCond = sub; viewTarget = null; }
            }
            HopTarget ht = hopTarget(joinCond, viewTarget, prevTable, hopDb,
                    hop.joinName(), propName, i, p, model, md);
            joinCond = ht.cond();
            String targetTable = ht.table();
            viewTarget = ht.view();

            Variable s = new Variable("s");
            Variable t = new Variable("t");
            Map<String, ValueSpecification> condScope = new LinkedHashMap<>();
            condScope.put(prevTable, prevAlias == null
                    ? s : new AppliedProperty(s, prevAlias));
            if (!targetTable.equals(prevTable)) condScope.put(targetTable, t);
            ValueSpecification cond = RelOpTranslator.translate(joinCond, condScope, t,
                    /*rowBind*/ null, RelOpTranslator.PipelineView.NONE);
            LambdaFunction condLambda = new LambdaFunction(List.of(s, t), List.of(cond));

            if (emitNavigate) {
                ColSpec slot = new ColSpec(Objects.requireNonNull(slotAlias, "slotAlias"),
                        new LambdaFunction(List.of(), List.of(new AppliedFunction(
                                "getAll", List.of(new PackageableElementPtr(Objects
                                        .requireNonNull(targetClassFqn)))))), null);
                // The condition speaks TABLE-row scope while the slot's
                // thunk is the CLASS extent — spell the target's table row
                // into the call so the cond lambda's T types (the same
                // conform-by-emission cure as legacyAssocPredicate).
                // a VIEW target navigates over the view's RELATION (the
                // same expansion the physical-hop arm uses — engine: views
                // are subselects, joins accept Table OR View)
                ValueSpecification targetRows = viewTarget != null
                        ? ViewRelation.viewRelationExpr(
                                model.findView(hopDb, viewTarget).orElseThrow(),
                                viewTarget, hopDb, model, md)
                        : new AppliedFunction(
                                "tableReference", List.of(
                                        new PackageableElementPtr(hopDb),
                                        new CString(targetTable)));
                // ROUTED union navigation: ONE navigate carries the OR over
                // ALL the property's route entries, each entry's condition
                // built from ITS OWN join with target-side reads suffixed by
                // ITS member ordinal (engine `<col>_<i>`) — exactly the
                // routed members' threads carry keys, the others read NULL.
                List<UnionSynthesis.UnionRoute> routes = propName == null ? null
                        : p.unionRoutes.get(propName);
                LambdaFunction navCond = condLambda;
                // full-coverage same-join routes MERGE: the plain condition
                // over the shared key serves every member (no suffixing —
                // engine snapshot-union propagation golden)
                if (routes != null
                        && routesMerge(routes, md, model, targetClassFqn)) {
                    routes = null;
                }
                if (routes != null) {
                    RoutedNav rn = routedNavigation(routes, propName,
                            prevTable, prevAlias, hopDb, targetTable,
                            targetRows, s, t, model, md);
                    navCond = rn.cond();
                    targetRows = rn.rows();
                }
                p.expr = new AppliedFunction(Pure.Lite.LEGACY_NAVIGATE,
                        List.of(p.expr, slot, targetRows, navCond));
                p.classSlots.add(slotAlias);
            } else {
                ValueSpecification targetRel = viewTarget != null
                        ? ViewRelation.viewRelationExpr(model.findView(hopDb, viewTarget).orElseThrow(),
                                viewTarget, hopDb, model, md)
                        : new AppliedFunction("tableReference",
                                List.of(new PackageableElementPtr(hopDb), new CString(targetTable)));
                // a VIEW hop carries the frame identity in the spare
                // alias channel — the checker lifts it onto the slot
                ColSpec slot = new ColSpec(java.util.Objects.requireNonNull(slotAlias, "slotAlias"),
                        new LambdaFunction(List.of(), List.of(targetRel)),
                        null, viewTarget);
                p.expr = new AppliedFunction(Pure.Lite.JOIN_SLOT,
                        List.of(p.expr, slot, condLambda));
            }
            p.aliasToTargetTable.put(slotAlias, targetTable);
            p.aliasToTargetColumns.put(slotAlias,
                    targetColumnNames(hopDb, targetTable, viewTarget, model));
            if (!emitNavigate) p.pathToSlot.put(pathKey, slotAlias);
            prevTable = targetTable;
            prevAlias = slotAlias;
        }
    }

    /** The routed-union navigate pieces: the OR-of-member-routes condition
     * and the target relation with the suffixed key columns projected. */
    private record RoutedNav(LambdaFunction cond, ValueSpecification rows) {
    }

    /** One routed entry before suffixing: its RAW translated condition. */
    private record RouteEntry(UnionSynthesis.UnionRoute route, ValueSpecification raw,
            String db, String tgt, boolean inArm) {
    }

    /** {@code [key column, canonical table]} when every entry of a group is
     * single-hop into a member whose main table declares a sole PRIMARY KEY,
     * all on ONE table, and the condition's only target read is that key;
     * else null (the per-route form stays). */
    private static String @com.legend.Nullable [] sharedTableKey(List<RouteEntry> es,
            Variable t, LegacyMappingDefinition md, ModelBuilder model) {
        String key = null;
        String table = null;
        String db = null;
        for (RouteEntry e : es) {
            if (e.inArm()) {
                return null;
            }
            ClassMapping set = MappingNormalizer.findSetById(md, model,
                    e.route().join().targetSetId());
            String k = UnionSynthesis.tableKey(set, model);
            if (k == null || !(set instanceof ClassMapping.Relational r) || r.mainTable() == null) {
                return null;
            }
            String tb = MappingNormalizer.canonicalTable(r.mainTable().table());
            if (key == null || table == null || db == null) {
                key = k;
                table = tb;
                db = r.mainTable().database();
            } else if (!key.equals(k) || !table.equals(tb)
                    || !db.equals(r.mainTable().database())) {
                return null;
            }
        }
        if (key == null) {
            return null;
        }
        Map<String, String> reads = new LinkedHashMap<>();
        UnionSynthesis.suffixTargetReads(es.get(0).raw(), t, "_x", reads);
        return reads.keySet().equals(Set.of(key)) ? new String[]{key, table} : null;
    }

    private static RoutedNav routedNavigation(
            List<UnionSynthesis.UnionRoute> routes, @com.legend.Nullable String propName,
            @com.legend.Nullable String prevTable, @com.legend.Nullable String prevAlias,
            String hopDb, String targetTable, ValueSpecification targetRows,
            Variable s, Variable t, ModelBuilder model,
            LegacyMappingDefinition md) {
        ValueSpecification orCond = null;
        // suffixed name -> [base column, its route's db, its
        // route's landing table] (the typing arg needs the kind)
        Map<String, String[]> keyCols = new LinkedHashMap<>();
        List<RouteEntry> entries = new ArrayList<>();
        boolean perArm = !UnionSynthesis.uniformChainedRoutes(
                UnionSynthesis.memberJoins(routes));
        for (UnionSynthesis.UnionRoute route : routes) {
            // SHARED-PREFIX chains contribute their FINAL hop
            // sourced at the shared landing (prefix emitted as
            // physical joins above). PER-ARM chains contribute
            // their FIRST hop sourced at the main table — the
            // mids live INSIDE the owning member's thread
            // (push-into-arm) and the target side reads the
            // property-scoped chain keys that thread projects.
            List<JoinChainElement> rChain = route.join().joins();
            boolean rInArm = perArm && rChain.size() > 1;
            JoinChainElement rHop = rInArm ? rChain.get(0)
                    : rChain.get(rChain.size() - 1);
            String rPrevTable = prevTable;
            String rPrevAlias = prevAlias;
            String rDb = rHop.databaseName() != null
                    ? rHop.databaseName() : route.join().database();
            DatabaseDefinition.JoinDefinition rJd =
                    model.findJoin(rDb, rHop.joinName()).orElseThrow(() ->
                            new ModelException(
                                    LegendCompileException
                                            .Phase.NORMALIZE,
                                    "Join '" + rHop.joinName()
                                    + "' not found in db '" + rDb
                                    + "'; PM='" + propName + "', mapping="
                                    + md.qualifiedName()));
            Set<String> rCondTables = new LinkedHashSet<>();
            RelOpTranslator.collectTablesIn(rJd.operation(), rCondTables);
            rCondTables.remove(rPrevTable);
            String rTgt = rCondTables.size() == 1
                    && model.findView(rDb, rCondTables.iterator().next()).isPresent()
                    ? rCondTables.iterator().next()
                    : MappingNormalizer.determineTargetTable(rJd.operation(),
                            rPrevTable, rHop.joinName(), propName,
                            rChain.size(), md.qualifiedName());
            Map<String, ValueSpecification> rScope = new LinkedHashMap<>();
            rScope.put(rPrevTable, rPrevAlias == null
                    ? s : new AppliedProperty(s, rPrevAlias));
            if (!rTgt.equals(prevTable)) {
                rScope.put(rTgt, t);
            }
            ValueSpecification rCond = RelOpTranslator.translate(
                    rJd.operation(), rScope, t, null,
                    RelOpTranslator.PipelineView.NONE);
            entries.add(new RouteEntry(route, rCond, rDb, rTgt, rInArm));
        }
        // routes grouped by their RAW condition: routes into members of ONE
        // table keyed on its PRIMARY KEY emit ONE equality on the shared
        // key (indexable) AND the members' gated keys (non-null exactly on
        // a member's own rows — the membership the per-member OR carried);
        // every other group keeps the per-route member-suffixed disjuncts
        Map<ValueSpecification, List<RouteEntry>> groups = new LinkedHashMap<>();
        for (RouteEntry e : entries) {
            groups.computeIfAbsent(e.raw(), k -> new ArrayList<>()).add(e);
        }
        for (var g : groups.entrySet()) {
            List<RouteEntry> es = g.getValue();
            String[] shared = sharedTableKey(es, t, md, model);
            if (shared != null) {
                String key = shared[0];
                String table = shared[1];
                Map<String, String> out = new LinkedHashMap<>();
                ValueSpecification eq = UnionSynthesis.suffixTargetReads(g.getKey(), t,
                        UnionSynthesis.TABLE_KEY_SUFFIX + "_"
                                + table.replaceAll("[^A-Za-z0-9_]", "_"), out);
                keyCols.put(UnionSynthesis.sharedKeyName(table, key),
                        new String[]{key, es.get(0).db(), es.get(0).tgt()});
                ValueSpecification flags = null;
                for (RouteEntry e : es) {
                    String gated = key + "_" + e.route().targetOrdinal();
                    keyCols.put(gated, new String[]{key, e.db(), e.tgt()});
                    flags = UnionSynthesis.orDistinct(flags, new AppliedFunction("not",
                            List.of(new AppliedFunction("isEmpty",
                                    List.of(new AppliedProperty(t, gated))))));
                }
                orCond = UnionSynthesis.orDistinct(orCond, new AppliedFunction("and",
                        List.of(eq, java.util.Objects.requireNonNull(flags))));
                continue;
            }
            for (RouteEntry e : es) {
                Map<String, String> out = new LinkedHashMap<>();
                // in-arm chained routes read the PROPERTY-SCOPED
                // chain keys the member thread projects
                ValueSpecification rCond = e.inArm()
                        ? UnionSynthesis.suffixTargetReads(e.raw(), t,
                                "__" + propName + "_" + e.route().targetOrdinal(), out)
                        : UnionSynthesis.suffixTargetReads(e.raw(), t,
                                e.route().targetOrdinal(), out);
                for (var en : out.entrySet()) {
                    keyCols.put(en.getValue(), new String[]{en.getKey(), e.db(), e.tgt()});
                }
                orCond = UnionSynthesis.orDistinct(orCond, rCond);
            }
        }
        LambdaFunction navCond = new LambdaFunction(List.of(s, t), List.of(orCond));
        // typing arg: the suffixed key schema off the FIRST
        // landing table; a key whose base column is absent
        // there types as a NULL cast of ITS OWN landing
        // table's column kind (audit 11: heterogeneous target
        // key names across routed members)
        List<ColSpec> keySpecs = new ArrayList<>();
        for (var en : keyCols.entrySet()) {
            Variable kr = new Variable("kr");
            String base = en.getValue()[0];
            ValueSpecification read;
            if (relationHasColumn(hopDb, targetTable, base, model)) {
                read = new AppliedProperty(kr, base);
            } else {
                String kind = columnPureKind(en.getValue()[1],
                        en.getValue()[2], base, model);
                if (kind == null) {
                    throw new NotImplementedException(
                            "routed union key column '" + base
                            + "' has no derivable pure kind on table '"
                            + en.getValue()[2] + "'; mapping="
                            + md.qualifiedName());
                }
                read = new AppliedFunction("cast", List.of(
                        new PureCollection(List.of()),
                        new TypeAnnotation.Named(
                                new TypeExpression.NameRef(kind))));
            }
            keySpecs.add(new ColSpec(en.getKey(),
                    new LambdaFunction(List.of(kr), List.of(read)), null));
        }
        return new RoutedNav(navCond, new AppliedFunction("project",
                List.of(targetRows, new ColSpecArray(keySpecs))));
    }

    /**
     * Pick the slot identifier for a freshly-emitted physical sub-row hop.
     * The default is the human-readable {@code "__"}-joined path. If a
     * <em>different</em> path already produced that exact name (a genuine
     * collision — e.g. chain {@code [A, B]} vs a single join named
     * {@code "A__B"}), a deterministic suffix is appended until the name is
     * free. The structured {@link Pipeline#pathToSlot} key, not this name,
     * is the dedup identity; readers recover the name via {@link #slotFor}.
     */
    /**
     * The navigate-slot alias for a class-typed join PM: the property name,
     * MINTED PAST any physical main-table column of the same name (the slot
     * pseudo-column and the physical column share one relation row — the
     * checker rightly rejects the duplicate). Deterministic and recorded so
     * the binding read uses the same name.
     */
    /** Record which class OWNS a class-typed sub-PM's nav slot (keyed by
     * property name — the slot alias space) so the collision guards can
     * distinguish a same-owner routed SIBLING (dedups into one routed
     * navigate) from a genuine cross-level clash (ledger cluster 66). */
    private static void recordNavSlotOwner(Pipeline p, PropertyMapping sub,
            String ownerCls, ModelBuilder model) {
        if (sub instanceof PropertyMapping.Join j
                && classTypedTargetIfMapped(ownerCls, j.propertyName(),
                        model) != null) {
            p.navSlotOwner.putIfAbsent(j.propertyName(), ownerCls);
        }
    }

    static String mintNavSlotAlias(Pipeline p, ModelBuilder model,
            String mainDb, String mainTable, String propName) {
        String known = p.navSlotByProp.get(propName);
        if (known != null) {
            return known;
        }
        String tableName = mainTable != null && mainTable.contains(".")
                ? mainTable.substring(mainTable.lastIndexOf('.') + 1) : mainTable;
        // include-aware: the main table may live in an INCLUDED database
        // (classMappingFilterWithInnerJoin's milestongingDB includes the
        // milestoning db that declares ProductTable.exchange)
        // a slot named after the platform's RELATION accessors (`columns`,
        // `rows`) would read as the accessor on the row var — mint clear
        boolean collides = tableHasColumn(model, mainDb, tableName, propName,
                new java.util.LinkedHashSet<>())
                || propName.equals("columns")
                || propName.equals(com.legend.compiler.element.type.PlatformTypes.ROWS_MARKER);
        String alias = propName;
        if (collides) {
            alias = propName + "_nav";
            while (p.aliasToTargetTable.containsKey(alias)) {
                alias = alias + "_";
            }
        }
        p.navSlotByProp.put(propName, alias);
        return alias;
    }

    /** Whether {@code table} (in {@code dbFqn} or any INCLUDED database)
     * declares a column named {@code col} — the slot-alias collision test. */
    private static boolean tableHasColumn(ModelBuilder model, String dbFqn,
            String table, String col, java.util.Set<String> seen) {
        if (dbFqn == null || !seen.add(dbFqn)) {
            return false;
        }
        var db = model.findDatabase(dbFqn).orElse(null);
        if (db == null) {
            return false;
        }
        if (db.tables().stream()
                .filter(t -> t.name().equalsIgnoreCase(table))
                .flatMap(t -> t.columns().stream())
                .anyMatch(c -> c.name().equalsIgnoreCase(col))) {
            return true;
        }
        for (String inc : db.includes()) {
            if (tableHasColumn(model, inc, table, col, seen)) {
                return true;
            }
        }
        return false;
    }

    static String uniqueSlotName(Pipeline p,
            @com.legend.Nullable List<String> path) {
        String base = String.join("__", path);
        if (!p.aliasToTargetTable.containsKey(base)) return base;
        int n = 2;
        String candidate;
        do {
            candidate = base + "__" + n++;
        } while (p.aliasToTargetTable.containsKey(candidate));
        return candidate;
    }

    /** Ordered join names of a chain — the structured {@link Pipeline#pathToSlot} key. */
    static List<String> joinPath(List<JoinChainElement> hops) {
        List<String> names = new ArrayList<>(hops.size());
        for (JoinChainElement h : hops) names.add(h.joinName());
        return names;
    }

    /**
     * Recover the pipeline slot name a physical chain was emitted under.
     * Resolves through the structured path registry (collision-proof).
     * A miss is LOUD: emitted chains are always registered, so a miss
     * means some translated expression navigates a join that was never
     * hoisted — the old flattened-name fallback let the terminal read
     * silently bind through ANOTHER chain's slot (audit 18 finding 2).
     */
    static String slotFor(Pipeline p, List<JoinChainElement> hops) {
        List<String> key = joinPath(hops);
        String slot = p.pathToSlot.get(key);
        if (slot == null) {
            throw new NotImplementedException("join chain " + key
                    + " was never emitted on this pipeline — the expression"
                    + " navigates a join that was not hoisted as a slot");
        }
        return slot;
    }

    static @com.legend.Nullable String classTypedTargetIfMapped(
            @com.legend.Nullable String ownerClassFqn,
                                                  String propName, ModelBuilder model) {
        ClassDefinition owner = MissProbe.knownMiss(MappingNormalizer.classDef(model, ownerClassFqn));
        if (owner == null) return null;
        TypeExpression propType = MappingNormalizer.findPropertyTypeDeep(owner, propName, model);
        // a parameterized declaration (SetImplementation.class: Class<Any>,
        // PropertyMapping.property: Property<Nil,Any|*> — the real m3 ends)
        // targets its RAW class: the mapped set is the raw class's
        String tgt = propType instanceof TypeExpression.NameRef nr ? nr.name()
                : propType instanceof TypeExpression.Generic g ? g.name() : null;
        if (tgt == null) return null;
        // the declared class itself, or an ABSTRACTION of mapped classes
        // (Table.columns : RelationalOperationElement[*] routed to the
        // Column set): either way the property is a navigation, never a
        // column read; the route names the concrete target downstream
        return model.isMappedClass(tgt) || hasMappedSubclass(tgt, model) ? tgt : null;
    }

    private static boolean hasMappedSubclass(String base, ModelBuilder model) {
        return model.classes().anyMatch(c -> model.isMappedClass(c.qualifiedName())
                && UnionSynthesis.isSubclassOf(c.qualifiedName(), base, model));
    }

    record JoinNavSpec(List<JoinChainElement> chain, @com.legend.Nullable String chainDb) {}

    static void collectJoinNavigationsInPms(List<PropertyMapping> pms,
                                                   List<JoinNavSpec> out) {
        collectJoinNavigationsInPms(pms, out, null);
    }

    /** {@code md} non-null resolves Inline splice references — the
     * referenced set's expression-level {@code @Join} navigations hoist
     * into the OWNER pipeline exactly like a direct embedded block. */
    static void collectJoinNavigationsInPms(List<PropertyMapping> pms,
            List<JoinNavSpec> out, @com.legend.Nullable LegacyMappingDefinition md) {
        for (PropertyMapping pm : pms) {
            switch (pm) {
                case PropertyMapping.EnumeratedExpression ee -> collectJoinNavigations(ee.expression(), out);
                case PropertyMapping.Expression expr -> collectJoinNavigations(expr.expression(), out);
                case PropertyMapping.LocalProperty lp -> collectJoinNavigationsInPms(List.of(lp.body()), out, md);
                case PropertyMapping.Embedded emb -> collectJoinNavigationsInPms(emb.propertyMappings(), out, md);
                case PropertyMapping.OtherwiseEmbedded oe ->
                        collectJoinNavigationsInPms(oe.embedded(), out, md);
                case PropertyMapping.JoinTerminalColumn jtc ->
                        collectJoinNavigations(jtc.terminalColumn(), out);
                // an Inline splice CARRIES the referenced set's JoinNavs
                // (bookCatalogMap: authors() Inline[author_impl] whose
                // author_impl reads @Book_Authorship | ...) — resolvable
                // only with the enclosing mapping in hand
                case PropertyMapping.InlineEmbedded ie -> {
                    if (md != null) {
                        for (var cm : md.classMappings()) {
                            if (cm instanceof ClassMapping.Relational r2
                                    && java.util.Objects.equals(
                                            MappingNormalizer.setIdOf(r2),
                                            ie.setId())) {
                                collectJoinNavigationsInPms(
                                        r2.propertyMappings(), out, md);
                                break;
                            }
                        }
                    }
                }
                // Join / Column / EnumeratedColumn: Join handled by
                // Pass 1; the others don't carry JoinNav.
                case PropertyMapping.Join ignored -> { }
                case PropertyMapping.Column ignored -> { }
                case PropertyMapping.EnumeratedColumn ignored -> { }
            }
        }
    }

    static void collectJoinNavigations(RelationalOperation op,
                                              List<JoinNavSpec> out) {
        switch (op) {
            case RelationalOperation.JoinNavigation jn -> {
                out.add(new JoinNavSpec(jn.chain(), jn.databaseName()));
                if (jn.terminal() != null) collectJoinNavigations(jn.terminal(), out);
            }
            case RelationalOperation.FunctionCall fc ->
                    fc.args().forEach(a -> collectJoinNavigations(a, out));
            case RelationalOperation.Comparison cmp -> {
                collectJoinNavigations(cmp.left(), out);
                collectJoinNavigations(cmp.right(), out);
            }
            case RelationalOperation.BooleanOp bo -> {
                collectJoinNavigations(bo.left(), out);
                collectJoinNavigations(bo.right(), out);
            }
            case RelationalOperation.IsNull n   -> collectJoinNavigations(n.operand(), out);
            case RelationalOperation.IsNotNull n -> collectJoinNavigations(n.operand(), out);
            case RelationalOperation.Group g    -> collectJoinNavigations(g.inner(), out);
            case RelationalOperation.ArrayLiteral a ->
                    a.elements().forEach(e -> collectJoinNavigations(e, out));
            case RelationalOperation.Lambda lam -> collectJoinNavigations(lam.body(), out);
            case RelationalOperation.LambdaParam ignored -> { }
            case RelationalOperation.ColumnRef ignored -> { }
            case RelationalOperation.TargetColumnRef ignored -> { }
            case RelationalOperation.Literal ignored -> { }
        }
    }
    /** PASS-2 source-side substitution + target determination. A VIEW
     * landing (detected up front OR determined after substitution) carries
     * its identity in {@code view} — the emission arms expand it as the
     * view's RELATION frame instead of a tableReference. */
    private record HopTarget(RelationalOperation cond, String table,
            @com.legend.Nullable String view) {
    }

    private static HopTarget hopTarget(RelationalOperation joinCond,
            @com.legend.Nullable String viewTarget, @com.legend.Nullable String prevTable, String hopDb,
            String joinName, @com.legend.Nullable String propName, int i, Pipeline p,
            ModelBuilder model, LegacyMappingDefinition md) {
        if (viewTarget != null) {
            return new HopTarget(joinCond, viewTarget, viewTarget);
        }
        // PASS 2: source-side view projections substitute (plain views
        // only); target-side view refs stay VERBATIM, so a view landing
        // is recognizable here too (the source-view + view-target pair —
        // the early detection above only fires when the source side is
        // the pipeline's own prevTable)
        joinCond = MappingNormalizer.resolveViewRefsInJoin(joinCond, hopDb,
                prevTable, model, md, p.backingView, null);
        String targetTable = MappingNormalizer.determineTargetTable(
                joinCond, prevTable, joinName,
                propName == null ? "<nested>" : propName,
                i + 1, md.qualifiedName());
        String view = model.findView(hopDb, targetTable).isPresent()
                ? targetTable : null;
        return new HopTarget(joinCond, targetTable, view);
    }

    /** A CLASS-typed navigate re-roots at the TARGET CLASS's pipeline,
     * whose row speaks PHYSICAL columns — when the sole view candidate is
     * a PLAIN (rename-only) re-spelling of that class's own ~mainTable,
     * the cond substitutes to physical (PASS-1 doctrine) and the returned
     * cond replaces the expansion. Null = keep the expansion: a non-plain
     * view (filter/groupBy/distinct — a REAL relation the join lands on),
     * a foreign table's view, or an unmapped target class. */
    private static @com.legend.Nullable RelationalOperation plainClassViewCond(
            RelationalOperation joinCond, String viewTarget,
            @com.legend.Nullable String targetClassFqn, String hopDb, ModelBuilder model,
            LegacyMappingDefinition md) {
        if (targetClassFqn == null) {
            return null;
        }
        DatabaseDefinition.ViewDefinition vd =
                model.findView(hopDb, viewTarget).orElseThrow();
        if (vd.filter() != null || !vd.groupByColumns().isEmpty()
                || vd.distinct()) {
            return null;
        }
        // JOIN-NAVIGATING columns disqualify too: substituting them would
        // splice a @Join expr into the cond (personFirmView.firm_name) —
        // rename-only means every column is a plain ColumnRef
        for (DatabaseDefinition.ViewDefinition.ViewColumnMapping vc
                : vd.columnMappings()) {
            List<JoinNavSpec> navs = new ArrayList<>();
            collectJoinNavigations(vc.expression(), navs);
            if (!navs.isEmpty()) {
                return null;
            }
        }
        String vPhys = ViewRelation.inferViewMainTable(
                vd, viewTarget, md, model, hopDb);
        String tgtMain;
        try {
            tgtMain = MappingNormalizer.mainTableOf(md, targetClassFqn, model);
        } catch (ModelException unmappedTarget) {
            return null;
        }
        // the declared ~mainTable may spell the VIEW itself or its
        // physical table (inference vs explicit) — both mean this class.
        // A ~groupBy/~distinct class mapping's pipeline speaks GROUPED
        // outputs, not physical — its row is a real relation; keep the
        // expansion there (testReprocessGroupByAlias's grouped Person).
        if (!vPhys.equals(tgtMain) && !viewTarget.equals(tgtMain)) {
            return null;
        }
        // SINGLE-SET, group-free, NON-TEMPORAL targets only: union routes
        // and milestoned targets carry their own emission disciplines
        // (suffixed keys / temporal columns) that speak the view's row —
        // those keep the expansion (sweep-proven: unionOfViews +
        // milestoned-view regressions under the looser gate).
        List<ClassMapping.Relational> sets = MappingNormalizer
                .relationalMappingsInClosure(md, model, targetClassFqn);
        if (sets.size() != 1 || !sets.get(0).groupBy().isEmpty()
                || sets.get(0).distinct()
                || MappingNormalizer.isTemporalClass(targetClassFqn, model)) {
            return null;
        }
        return MappingNormalizer.resolveViewRefsInJoin(
                joinCond, hopDb, vPhys, model, md, viewTarget, null);
    }

    /**
     * The (INNER)-typed mapping ~filter as a ROW-EXPLODING source relation:
     * {@code project(filter(main-table + filter join chain, cond),
     * [every base column under its original name])}. Duplicate parent rows
     * (one per matching chain terminal row) survive the projection — the
     * engine's getRelationalElementWithInnerJoin shape. The chain emits as
     * pipeline joins; the null-rejecting WHERE makes LEFT ≡ INNER row-for-row.
     */
    static ValueSpecification innerFilteredSource(
            ClassMapping.Relational rcm, FilterMapping.JoinMediated jm,
            ModelBuilder model, LegacyMappingDefinition md) {
        var jmMain = java.util.Objects.requireNonNull(rcm.mainTable(),
                "join-mediated filter on a set without ~mainTable");
        String mainDb = jmMain.database();
        String mainTable = jmMain.table();
        Variable r = new Variable("irow");
        // NOTE (leg 4, sweep-bisected): threading the class's backingView
        // here converts the view-to-view (INNER) ~filter pair BUT breaks
        // unionOfViews + a milestoned-view single under FULL-corpus
        // assembly (cross-family duplicate resolution) — the next rung
        // must gate that threading by the failing assemblies' shapes.
        Pipeline p = new Pipeline(new AppliedFunction("tableReference",
                List.of(new PackageableElementPtr(mainDb), new CString(mainTable))),
                null);
        JoinChainEmission.emitJoinChain(p, jm.joins(), jm.sourceDb(),
                /* propName */ null, rcm.className(), mainDb, mainTable,
                r, model, md, /* classTypedTerminus */ false);
        String dbFqn = switch (jm.filter()) {
            case FilterPointer.Cross c -> c.db();
            case FilterPointer.Local l -> jm.sourceDb();
        };
        DatabaseDefinition.FilterDefinition fd = model.findFilter(
                dbFqn, jm.filter().name()).orElseThrow(() -> new ModelException(
                LegendCompileException.Phase.NORMALIZE,
                "~filter '" + jm.filter().name() + "' not found in db '"
              + dbFqn + "'; class=" + rcm.className() + ", mapping="
              + md.qualifiedName()));
        // LEFT JOIN + WHERE ≡ INNER JOIN + WHERE only when the condition is
        // NULL-REJECTING on the joined side — a null-TOLERANT condition
        // (is-null arms) keeps null-extended parents the engine's INNER
        // join drops (audit 19c, probe-verified). Every corpus (INNER)
        // condition today is null-rejecting; the first tolerant one must
        // be LOUD, never silently wrong. Real INNER emission is follow-up.
        if (nullTolerant(fd.condition())) {
            throw new NotImplementedException("(INNER) mapping ~filter '"
                    + jm.filter().name() + "' has a NULL-TOLERANT condition —"
                    + " the LEFT+WHERE realization would keep parents the"
                    + " engine's INNER join drops; class=" + rcm.className()
                    + ", mapping=" + md.qualifiedName());
        }
        String terminalAlias = JoinChainEmission.slotFor(p, jm.joins());
        ValueSpecification terminalRow = new AppliedProperty(r, terminalAlias);
        Map<String, ValueSpecification> scope = new LinkedHashMap<>();
        // The engine anchors the condition at the chain TERMINUS
        // (firmtable_1 in the ChainedJoins golden) — the terminal binding
        // takes precedence over the root even when the chain lands back on
        // the MAIN table; binding root-first silently self-filtered and
        // left the chain undemanded (elided joins, one row per parent).
        String terminalTable = p.aliasToTargetTable.get(terminalAlias);
        if (terminalTable != null) {
            scope.put(terminalTable, terminalRow);
        }
        scope.putIfAbsent(mainTable, r);
        MappingNormalizer.seedAliasScope(scope, p, r, mainTable);
        ValueSpecification cond = RelOpTranslator.translate(fd.condition(),
                scope, terminalRow, r, p.view());
        ValueSpecification src = new AppliedFunction("filter", List.of(p.expr,
                new LambdaFunction(List.of(r), List.of(cond))));
        DatabaseDefinition.TableDefinition td = findPhysicalTable(
                mainDb, mainTable, model, new HashSet<>());
        if (td == null) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                    "main table '" + mainTable + "' not found in db '" + mainDb
                  + "' for the (INNER) mapping ~filter of class '"
                  + rcm.className() + "', mapping=" + md.qualifiedName());
        }
        Variable vd = new Variable("vd");
        List<ColSpec> baseCols = new ArrayList<>(td.columns().size());
        for (DatabaseDefinition.ColumnDefinition cd : td.columns()) {
            baseCols.add(new ColSpec(cd.name(),
                    new LambdaFunction(List.of(vd),
                            List.of(new AppliedProperty(vd, cd.name()))), null));
        }
        return new AppliedFunction("project", List.of(src,
                new ColSpecArray(baseCols)));
    }

    /** A condition with an is-null arm (or a null-literal comparison /
     * isNull dyna) anywhere in it — the LEFT+WHERE ≡ INNER equivalence
     * breaks exactly there. Conservative: OR/NOT around a null test also
     * counts. */
    private static boolean nullTolerant(RelationalOperation op) {
        return switch (op) {
            case RelationalOperation.IsNull ignored -> true;
            case RelationalOperation.FunctionCall fc -> {
                if (fc.name().equalsIgnoreCase("isNull")
                        || fc.name().equalsIgnoreCase("sqlNull")) {
                    yield true;
                }
                // audit 23: NULL-swallowing functions defeat the
                // classification — LOUD, never a silent null-rejecting
                // verdict (the (INNER) LEFT+WHERE realization would keep
                // NULL-extended parents the engine's INNER join drops)
                if (fc.name().equalsIgnoreCase("coalesce")
                        || fc.name().equalsIgnoreCase("ifnull")
                        || fc.name().equalsIgnoreCase("nvl")
                        || fc.name().equalsIgnoreCase("case")
                        || fc.name().equalsIgnoreCase("if")) {
                    throw new NotImplementedException("(INNER) mapping-"
                            + "filter condition uses '" + fc.name()
                            + "' — null-tolerance cannot be classified;"
                            + " not supported yet");
                }
                yield fc.args().stream()
                        .anyMatch(JoinChainEmission::nullTolerant);
            }
            case RelationalOperation.BooleanOp b ->
                    nullTolerant(b.left()) || nullTolerant(b.right());
            case RelationalOperation.Comparison c ->
                    nullTolerant(c.left()) || nullTolerant(c.right());
            case RelationalOperation.Group g -> nullTolerant(g.inner());
            default -> false;
        };
    }

    /** The declared column names of a hop's target relation — a view's
     * column mappings, else the physical table's columns; empty when
     * unknown (the terminal rebase then leaves the column where spelled). */
    private static Set<String> targetColumnNames(String dbFqn, String targetTable,
            @com.legend.Nullable String viewTarget, ModelBuilder model) {
        Set<String> out = new java.util.LinkedHashSet<>();
        if (viewTarget != null) {
            model.findView(dbFqn, viewTarget).ifPresent(v ->
                    v.columnMappings().forEach(c -> out.add(c.name())));
            return out;
        }
        DatabaseDefinition.TableDefinition t =
                PhysicalTables.find(dbFqn, targetTable, model);
        if (t != null) {
            t.columns().forEach(c -> out.add(c.name()));
        }
        return out;
    }

    private static DatabaseDefinition.@com.legend.Nullable TableDefinition findPhysicalTable(
            String dbFqn, String table, ModelBuilder model, Set<String> seen) {
        if (!seen.add(dbFqn)) {
            return null;
        }
        DatabaseDefinition db = model.findDatabase(dbFqn).orElse(null);
        if (db == null) {
            return null;
        }
        List<DatabaseDefinition.TableDefinition> tables = new ArrayList<>(db.tables());
        for (DatabaseDefinition.SchemaDefinition s : db.schemas()) {
            tables.addAll(s.tables());
        }
        for (DatabaseDefinition.TableDefinition td : tables) {
            if (td.name().equalsIgnoreCase(table)) {
                return td;
            }
        }
        for (String inc : db.includes()) {
            DatabaseDefinition.TableDefinition hit =
                    findPhysicalTable(inc, table, model, seen);
            if (hit != null) {
                return hit;
            }
        }
        return null;
    }

    /** Whether the relation named {@code table} (physical table OR view)
     * carries a column named {@code col} — routed union keys land on
     * VIEW-backed members too (unionOfViews). */
    private static boolean relationHasColumn(String db, String table,
            String col, ModelBuilder model) {
        if (MappingNormalizer.findPhysicalColumn(db, table, col, model) != null) {
            return true;
        }
        DatabaseDefinition.ViewDefinition view =
                model.findView(db, table).orElse(null);
        return view != null && view.columnMappings().stream()
                .anyMatch(vc -> vc.name().equals(col));
    }

    /** View-aware column kind — see {@link ViewRelation#columnPureKind}. */
    private static @com.legend.Nullable String columnPureKind(String db, String table, String col,
            ModelBuilder model) {
        return ViewRelation.columnPureKind(db, table, col, model);
    }

    /** Routed navigation collapses to the ONE plain condition when the
     * routes are full-coverage same-join over a union target, or the
     * target is a SAME-TABLE inheritance hierarchy (one physical
     * relation — member suffixes don't exist on its row). */
    private static boolean routesMerge(List<UnionSynthesis.UnionRoute> routes,
            LegacyMappingDefinition md, ModelBuilder model,
            @com.legend.Nullable String targetClassFqn) {
        return UnionSynthesis.mergedTargetRoutes(routes,
                UnionSynthesis.unionForClass(md, model, targetClassFqn))
                || UnionSynthesis.sameTableInheritanceMerge(md, model,
                        targetClassFqn, routes);
    }

}
