// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.parser.NormalizedModel;
import com.legend.compiler.ModelBuilder;
import com.legend.compiler.SynthFqn;
import com.legend.parser.Multiplicity;
import com.legend.parser.ParsedModel;
import com.legend.parser.TypeExpression;
import com.legend.parser.element.AssociationDefinition;
import com.legend.parser.element.AssociationMapping;
import com.legend.parser.element.AssociationPropertyMapping;
import com.legend.parser.element.ClassDefinition;
import com.legend.parser.element.ClassMapping;
import com.legend.parser.element.ComparisonOp;
import com.legend.parser.element.DatabaseDefinition;
import com.legend.parser.element.RelationalDataType;
import com.legend.parser.element.EnumerationMapping;
import com.legend.parser.element.FilterMapping;
import com.legend.parser.element.FilterPointer;
import com.legend.parser.element.FunctionDefinition;
import com.legend.parser.element.JoinChainElement;
import com.legend.parser.element.LogicalOp;
import com.legend.parser.element.LegacyMappingDefinition;
import com.legend.parser.element.MappingDefinition;
import com.legend.parser.element.Realization;
import com.legend.parser.element.PackageableElement;
import com.legend.parser.element.PropertyMapping;
import com.legend.parser.element.SynthHat;
import com.legend.parser.element.RelationalOperation;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.AppliedProperty;
import com.legend.parser.spec.CBoolean;
import com.legend.parser.spec.CFloat;
import com.legend.parser.spec.CInteger;
import com.legend.parser.spec.CString;
import com.legend.parser.spec.ColSpec;
import com.legend.parser.spec.ColSpecArray;
import com.legend.parser.spec.EnumValue;
import com.legend.parser.spec.KeyExpression;
import com.legend.parser.spec.LambdaFunction;
import com.legend.parser.spec.NewInstance;
import com.legend.parser.spec.NewInstanceCast;
import com.legend.parser.spec.PackageableElementPtr;
import com.legend.parser.spec.PureCollection;
import com.legend.parser.spec.TypeAnnotation;
import com.legend.parser.spec.ValueSpecification;
import com.legend.parser.spec.Variable;
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
                            throw new com.legend.error.NotImplementedException(
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
            throw new com.legend.error.NotImplementedException(
                    "OtherwiseEmbedded PM '" + oe.propertyName() + "' fallback kind "
                  + oe.fallback().getClass().getSimpleName()
                  + " not supported (Join only). Mapping=" + md.qualifiedName());
        }
        ClassDefinition owner = model.findClass(ownerClassFqn).orElseThrow();
        TypeExpression propType = MappingNormalizer.findPropertyTypeDeep(owner, oe.propertyName(), model);
        if (!(propType instanceof TypeExpression.NameRef nr)) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                    "OtherwiseEmbedded PM '" + oe.propertyName()
                  + "' has non-class property type; mapping=" + md.qualifiedName());
        }
        String targetClassFqn = nr.name();
        if (!model.isMappedClass(targetClassFqn)) {
            throw new com.legend.error.NotImplementedException(
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
                    .orElseThrow(() -> new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
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
            } else if (emitNavigate) {
                throw new com.legend.error.NotImplementedException(
                        "Join '" + hop.joinName() + "' navigates to a CLASS"
                      + " mapped over view '" + viewTarget + "'; class"
                      + " navigation onto view relations is a roadmap"
                      + " feature. mapping=" + md.qualifiedName());
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
                ValueSpecification targetRows = new AppliedFunction(
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
                        JoinChainElement rHop = route.join().joins().get(0);
                        String rDb = rHop.databaseName() != null
                                ? rHop.databaseName() : route.join().database();
                        DatabaseDefinition.JoinDefinition rJd =
                                model.findJoin(rDb, rHop.joinName()).orElseThrow(() ->
                                        new com.legend.error.ModelException(
                                                com.legend.error.LegendCompileException
                                                        .Phase.NORMALIZE,
                                                "Join '" + rHop.joinName()
                                                + "' not found in db '" + rDb
                                                + "'; PM='" + propName + "', mapping="
                                                + md.qualifiedName()));
                        String rTgt = MappingNormalizer.determineTargetTable(rJd.operation(),
                                prevTable, rHop.joinName(), propName, 1,
                                md.qualifiedName());
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
                        if (MappingNormalizer.findPhysicalColumn(hopDb, targetTable, base, model) != null) {
                            read = new AppliedProperty(kr, base);
                        } else {
                            DatabaseDefinition.ColumnDefinition cd =
                                    MappingNormalizer.findPhysicalColumn(en.getValue()[1],
                                            en.getValue()[2], base, model);
                            String kind = cd == null ? null : MappingNormalizer.pureKindOf(cd.dataType());
                            if (kind == null) {
                                throw new com.legend.error.NotImplementedException(
                                        "routed union key column '" + base
                                        + "' has no derivable pure kind on table '"
                                        + en.getValue()[2] + "'; mapping="
                                        + md.qualifiedName());
                            }
                            read = new AppliedFunction("cast", List.of(
                                    new PureCollection(List.of()),
                                    new com.legend.parser.spec.TypeAnnotation.Named(
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
                        ? MappingNormalizer.viewRelationExpr(model.findView(hopDb, viewTarget).orElseThrow(),
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
     * Resolves through the structured path registry (collision-proof);
     * falls back to the flattened name only if the chain was never emitted
     * (defensive — emitted chains are always registered).
     */
    static String slotFor(Pipeline p, List<JoinChainElement> hops) {
        List<String> key = joinPath(hops);
        String slot = p.pathToSlot.get(key);
        return slot != null ? slot : String.join("__", key);
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
}
