// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.compiler.ModelBuilder;
import com.legend.compiler.SynthFqn;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.error.NotImplementedException;
import com.legend.model.Multiplicity;
import com.legend.model.NormalizedModel;
import com.legend.model.ParsedModel;
import com.legend.model.TypeExpression;
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
import com.legend.model.Realization;
import com.legend.model.RelationalDataType;
import com.legend.model.RelationalOperation;
import com.legend.model.SynthHat;
import com.legend.model.spec.AppliedFunction;
import com.legend.model.spec.AppliedProperty;
import com.legend.model.spec.CBoolean;
import com.legend.model.spec.CFloat;
import com.legend.model.spec.CInteger;
import com.legend.model.spec.CString;
import com.legend.model.spec.ColSpec;
import com.legend.model.spec.ColSpecArray;
import com.legend.model.spec.EnumValue;
import com.legend.model.spec.KeyExpression;
import com.legend.model.spec.LambdaFunction;
import com.legend.model.spec.NewInstance;
import com.legend.model.spec.NewInstanceCast;
import com.legend.model.spec.PackageableElementPtr;
import com.legend.model.spec.PureCollection;
import com.legend.model.spec.TypeAnnotation;
import com.legend.model.spec.ValueSpecification;
import com.legend.model.spec.Variable;
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
                    rowBind, model, md, /*classTypedTerminus*/ true);
            case PropertyMapping.JoinTerminalColumn jtc -> emitJoinChain(p,
                    jtc.joins(), jtc.database(), /*propName*/ null, ownerClassFqn,
                    mainDb, mainTable, rowBind, model, md, /*classTypedTerminus*/ false);
            case PropertyMapping.LocalProperty lp -> emitHopsForStructuralPm(p, lp.body(),
                    ownerClassFqn, mainDb, mainTable, rowBind, model, md);
            case PropertyMapping.OtherwiseEmbedded oe -> emitOtherwiseEmbeddedHop(p, oe,
                    ownerClassFqn, mainDb, mainTable, rowBind, model, md);
            case PropertyMapping.Embedded emb -> {
                // sub-PM join chains hoist into the TOP pipeline (the
                // embedded instance shares the owner's row); the owner for
                // class-typed detection is the EMBEDDED class
                ClassDefinition owner = model.findClass(ownerClassFqn).orElse(null);
                TypeExpression propType = owner == null ? null
                        : MappingNormalizer.findPropertyTypeDeep(owner, emb.propertyName(), model);
                if (propType instanceof TypeExpression.NameRef nr) {
                    for (PropertyMapping sub : emb.propertyMappings()) {
                        if (sub instanceof PropertyMapping.Join j
                                && p.aliasToTargetTable.containsKey(j.propertyName())
                                && classTypedTargetIfMapped(nr.name(),
                                        j.propertyName(), model) != null) {
                            throw new NotImplementedException(
                                    "Embedded sub-PM '" + j.propertyName()
                                  + "' collides with an existing pipeline slot"
                                  + " of the same name; distinct same-named"
                                  + " class-typed joins across embedded levels"
                                  + " are a roadmap feature. Mapping="
                                  + md.qualifiedName());
                        }
                        emitHopsForStructuralPm(p, sub, nr.name(), mainDb,
                                mainTable, rowBind, model, md);
                    }
                }
            }
            default -> { /* Column / Enum / Expression / InlineEmbedded:
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
        ClassDefinition owner = model.findClass(ownerClassFqn).orElseThrow();
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
                                     String chainDb, String propName,
                                     String ownerClassFqn, String mainDb,
                                     String mainTable, Variable rowBind,
                                     ModelBuilder model, LegacyMappingDefinition md,
                                     boolean classTypedTerminus) {
        String targetClassFqn = null;
        if (classTypedTerminus && propName != null) {
            targetClassFqn = classTypedTargetIfMapped(ownerClassFqn, propName, model);
        }
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
                    ? mintNavSlotAlias(p, model, mainDb, mainTable, propName)
                    : null;
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
            if (viewTarget == null) {
                // PASS 2: source-side view projections substitute (plain
                // views only; non-plain non-backing stays a loud wall)
                joinCond = MappingNormalizer.resolveViewRefsInJoin(joinCond, hopDb, prevTable,
                        model, md, p.backingView, null);
            }
            String targetTable = viewTarget != null ? viewTarget
                    : MappingNormalizer.determineTargetTable(joinCond, prevTable,
                            hop.joinName(), propName == null ? "<nested>" : propName,
                            i + 1, md.qualifiedName());
            if (viewTarget == null) {
                MappingNormalizer.requireNonViewTarget(targetTable, hopDb, hop.joinName(), model, md);
            }

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
                ColSpec slot = new ColSpec(slotAlias,
                        new LambdaFunction(List.of(), List.of(new AppliedFunction(
                                "getAll", List.of(new PackageableElementPtr(targetClassFqn))))),
                        null);
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
                if (routes != null && UnionSynthesis.mergedTargetRoutes(routes,
                        UnionSynthesis.unionForClass(md, model, targetClassFqn))) {
                    routes = null;
                }
                if (routes != null) {
                    ValueSpecification orCond = null;
                    // suffixed name -> [base column, its route's db, its
                    // route's landing table] (the typing arg needs the kind)
                    Map<String, String[]> keyCols = new LinkedHashMap<>();
                    for (UnionSynthesis.UnionRoute route : routes) {
                        // CHAINED routes walk a SHARED prefix (validated at
                        // classification) — the prefix hops already emitted
                        // as physical joins above, so each route contributes
                        // only its FINAL hop, sourced at the last mid table.
                        List<JoinChainElement> rChain = route.join().joins();
                        JoinChainElement rHop = rChain.get(rChain.size() - 1);
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
                        rCondTables.remove(prevTable);
                        String rTgt = rCondTables.size() == 1
                                && model.findView(rDb, rCondTables.iterator().next()).isPresent()
                                ? rCondTables.iterator().next()
                                : MappingNormalizer.determineTargetTable(rJd.operation(),
                                        prevTable, rHop.joinName(), propName,
                                        rChain.size(), md.qualifiedName());
                        Map<String, ValueSpecification> rScope = new LinkedHashMap<>();
                        rScope.put(prevTable, prevAlias == null
                                ? s : new AppliedProperty(s, prevAlias));
                        if (!rTgt.equals(prevTable)) {
                            rScope.put(rTgt, t);
                        }
                        ValueSpecification rCond = RelOpTranslator.translate(
                                rJd.operation(), rScope, t, null,
                                RelOpTranslator.PipelineView.NONE);
                        Map<String, String> out = new LinkedHashMap<>();
                        rCond = MappingNormalizer.suffixTargetReads(rCond, t, route.targetOrdinal(), out);
                        for (var en : out.entrySet()) {
                            keyCols.put(en.getValue(),
                                    new String[]{en.getKey(), rDb, rTgt});
                        }
                        orCond = orCond == null ? rCond
                                : new AppliedFunction("or", List.of(orCond, rCond));
                    }
                    navCond = new LambdaFunction(List.of(s, t), List.of(orCond));
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
                    targetRows = new AppliedFunction("project",
                            List.of(targetRows, new ColSpecArray(keySpecs)));
                }
                p.expr = new AppliedFunction("legacyNavigate",
                        List.of(p.expr, slot, targetRows, navCond));
                p.classSlots.add(slotAlias);
            } else {
                ValueSpecification targetRel = viewTarget != null
                        ? ViewRelation.viewRelationExpr(model.findView(hopDb, viewTarget).orElseThrow(),
                                viewTarget, hopDb, model, md)
                        : new AppliedFunction("tableReference",
                                List.of(new PackageableElementPtr(hopDb), new CString(targetTable)));
                ColSpec slot = new ColSpec(slotAlias,
                        new LambdaFunction(List.of(), List.of(targetRel)), null);
                p.expr = new AppliedFunction("join",
                        List.of(p.expr, slot, condLambda));
            }
            p.aliasToTargetTable.put(slotAlias, targetTable);
            if (!emitNavigate) p.pathToSlot.put(pathKey, slotAlias);
            prevTable = targetTable;
            prevAlias = slotAlias;
        }
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
    static String mintNavSlotAlias(Pipeline p, ModelBuilder model,
            String mainDb, String mainTable, String propName) {
        String known = p.navSlotByProp.get(propName);
        if (known != null) {
            return known;
        }
        String tableName = mainTable != null && mainTable.contains(".")
                ? mainTable.substring(mainTable.lastIndexOf('.') + 1) : mainTable;
        boolean collides = model.findDatabase(mainDb)
                .map(db -> db.tables().stream()
                        .filter(t -> t.name().equalsIgnoreCase(tableName))
                        .flatMap(t -> t.columns().stream())
                        .anyMatch(c -> c.name().equalsIgnoreCase(propName)))
                .orElse(false);
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

    static String uniqueSlotName(Pipeline p, List<String> path) {
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

    static String classTypedTargetIfMapped(String ownerClassFqn,
                                                  String propName, ModelBuilder model) {
        ClassDefinition owner = model.findClass(ownerClassFqn).orElse(null);
        if (owner == null) return null;
        TypeExpression propType = MappingNormalizer.findPropertyTypeDeep(owner, propName, model);
        if (!(propType instanceof TypeExpression.NameRef nr)) return null;
        String tgt = nr.name();
        return model.isMappedClass(tgt) ? tgt : null;
    }

    record JoinNavSpec(List<JoinChainElement> chain, String chainDb) {}

    static void collectJoinNavigationsInPms(List<PropertyMapping> pms,
                                                   List<JoinNavSpec> out) {
        for (PropertyMapping pm : pms) {
            switch (pm) {
                case PropertyMapping.EnumeratedExpression ee -> collectJoinNavigations(ee.expression(), out);
                case PropertyMapping.Expression expr -> collectJoinNavigations(expr.expression(), out);
                case PropertyMapping.LocalProperty lp -> collectJoinNavigationsInPms(List.of(lp.body()), out);
                case PropertyMapping.Embedded emb -> collectJoinNavigationsInPms(emb.propertyMappings(), out);
                case PropertyMapping.OtherwiseEmbedded oe ->
                        collectJoinNavigationsInPms(oe.embedded(), out);
                case PropertyMapping.JoinTerminalColumn jtc ->
                        collectJoinNavigations(jtc.terminalColumn(), out);
                // Join / Column / EnumeratedColumn / InlineEmbedded:
                // Join handled by Pass 1; the others don't carry JoinNav.
                case PropertyMapping.Join ignored -> { }
                case PropertyMapping.Column ignored -> { }
                case PropertyMapping.EnumeratedColumn ignored -> { }
                case PropertyMapping.InlineEmbedded ignored -> { }
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
            case RelationalOperation.ColumnRef ignored -> { }
            case RelationalOperation.TargetColumnRef ignored -> { }
            case RelationalOperation.Literal ignored -> { }
            case RelationalOperation.TypeRef ignored -> { }
        }
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
        String mainDb = rcm.mainTable().database();
        String mainTable = rcm.mainTable().table();
        Variable r = new Variable("irow");
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

    private static DatabaseDefinition.TableDefinition findPhysicalTable(
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
    private static String columnPureKind(String db, String table, String col,
            ModelBuilder model) {
        return ViewRelation.columnPureKind(db, table, col, model);
    }

}
