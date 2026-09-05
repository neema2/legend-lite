package com.legend.resolver;
import com.legend.builtin.Pure;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedAggCol;
import com.legend.compiler.spec.typed.TypedAggregate;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedConcatenate;
import com.legend.compiler.spec.typed.TypedDistinct;
import com.legend.compiler.spec.typed.TypedDrop;
import com.legend.compiler.spec.typed.TypedEnumValue;
import com.legend.compiler.spec.typed.TypedExtend;
import com.legend.compiler.spec.typed.TypedExtendAgg;
import com.legend.compiler.spec.typed.TypedExtendWindow;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedGraphFetch;
import com.legend.compiler.spec.typed.TypedGraphTree;
import com.legend.compiler.spec.typed.TypedGroupBy;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedJoin;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedMilestonedAccess;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNavigate;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedRename;
import com.legend.compiler.spec.typed.TypedSelect;
import com.legend.compiler.spec.typed.TypedSerialize;
import com.legend.compiler.spec.typed.TypedSerializeGraph;
import com.legend.compiler.spec.typed.TypedSlice;
import com.legend.compiler.spec.typed.TypedSort;
import com.legend.compiler.spec.typed.TypedSortBy;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.MappingResolutionException;
import com.legend.error.NotImplementedException;
import com.legend.model.RuntimeDefinition;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * #69 — the CORRELATED navigation subselect machinery (engine parent-copy
 * architecture, testFunctionVariables goldens): a correlated pred's
 * parent-nav reads can never resolve on a flat join ON, so the head's join
 * target becomes a subselect that re-joins the PARENT extent (with the
 * navs the pred demands), filters by the pred over the joined row, and
 * joins back on parent-key equality — GROUPED for aggregated demands
 * (fold 2c), ROW-PRESERVING for exploding reads (fold 2b).
 */
final class CorrelatedSubselects {

    private final ClassSources sources;
    private final AssociationJoins assocMaterial;

    CorrelatedSubselects(ClassSources sources, AssociationJoins assocMaterial) {
        this.sources = sources;
        this.assocMaterial = assocMaterial;
    }

    Map<String, AssociationJoins.AssocJoin> buildAggMaterials(
            TemporalFrame temporal, ClassSource cs,
            StoreResolver.Context context,
            Map<String, List<StoreResolver.AggDemand>> aggDemands,
            Map<String, AssociationJoins.AssocJoin> chainMidsOut) {
        Map<String, AssociationJoins.AssocJoin> aggMaterials = new LinkedHashMap<>();
        for (var entry : aggDemands.entrySet()) {
            Set<String> leaves = new LinkedHashSet<>();
            Set<List<String>> mapperPaths = new LinkedHashSet<>();
            for (StoreResolver.AggDemand dm : entry.getValue()) {
                leaves.addAll(dm.demandLeaves());
                if (dm.mapper() != null) {
                    for (TypedSpec b : dm.mapper().body()) {
                        FlattenOps.consumedPaths(b, dm.mapper().parameters().get(0),
                                mapperPaths);
                    }
                }
            }
            String key = entry.getKey();
            int dot = key.indexOf('.');
            if (dot < 0) {
                aggMaterials.put(key,
                        assocMaterial.aggJoinMaterial(temporal, cs, key,
                                context, leaves, mapperPaths));
                continue;
            }
            // CHAIN-AGG key (mid.final — the aggScan chain arm): the MID
            // to-one hop materializes as an ordinary association/nav join
            // and the FINAL head's aggregation material anchors at the mid
            // hop's target class (its parked filter applies there exactly
            // like the depth-1 route).
            String mid = key.substring(0, dot);
            String fin = key.substring(dot + 1);
            AssociationJoins.AssocJoin midAj = assocMaterial.aggJoinMaterial(
                    temporal, cs, mid, context, java.util.Set.of(),
                    java.util.Set.of());
            chainMidsOut.put(key, midAj);
            aggMaterials.put(key,
                    assocMaterial.aggJoinMaterial(temporal, midAj.target(),
                            fin, context, leaves, mapperPaths));
        }
        return aggMaterials;
    }


    /** CHAIN-AGG fold step output: the pipe widened with the MID hop's
     * LEFT join, and the FINAL hop's material with its parent-side
     * condition reads re-pointed onto the mid-prefixed columns. */
    record ChainMidFold(TypedSpec withJoins, AssociationJoins.AssocJoin aj) {}

    /** CHAIN-AGG head (mid.final — the aggScan chain arm): emit the MID
     * hop's LEFT join with a chain-private prefix and re-point the FINAL
     * hop's parent-side condition reads onto it; the caller's grouped
     * subselect then keys/joins back against the mid hop's row exactly
     * like a depth-1 parent. Filter-position and outer-correlated chain
     * predicates stay LOUD (their isolation shapes are not built). */
    ChainMidFold foldChainMid(ClassSource cs, String head,
            AssociationJoins.AssocJoin aj, AssociationJoins.AssocJoin midAj,
            boolean filterPos, SyntheticHeads synthetics, TypedSpec withJoins,
            Set<String> usedChainPrefixes, @com.legend.Nullable String frameName) {
        String chainFinal = head.substring(head.indexOf('.') + 1);
        if (filterPos) {
            throw new NotImplementedException("aggregate over the chained"
                    + " navigation '" + SyntheticHeads.realHead(chainFinal)
                    + "' in filter position is not supported yet");
        }
        if (synthetics.correlatedPred(chainFinal) != null) {
            throw new NotImplementedException("aggregate over the chained"
                    + " navigation '" + SyntheticHeads.realHead(chainFinal)
                    + "' whose filter predicate reads the outer row is not"
                    + " supported yet");
        }
        String midBase = head.substring(0, head.indexOf('.')) + "_"
                + SyntheticHeads.realHead(chainFinal) + "_mid";
        String midPrefix = AssociationJoins.prefixFor(midBase, cs);
        int mOrd = 2;
        while (!usedChainPrefixes.add(midPrefix)) {
            midPrefix = AssociationJoins.prefixFor(midBase + "_" + mOrd++, cs);
        }
        Type.RelationType leftRowM =
                Type.requireRelationSchema(withJoins.info().type());
        List<Type.Column> colsM = new ArrayList<>(leftRowM.columns());
        for (Type.Column c : midAj.targetRow().columns()) {
            colsM.add(new Type.Column(midPrefix + c.name(),
                    c.type(), c.multiplicity()));
        }
        Type.RelationType midJoinedRow = new Type.RelationType(colsM);
        TypedSpec widened = new TypedJoin(withJoins, midAj.targetPipeline(),
                AssociationJoins.leftKind(),
                java.util.Objects.requireNonNull(midAj.condition(),
                        "mid-hop association condition"),
                Optional.of(midPrefix), frameName,
                new ExprType(Type.relation(midJoinedRow),
                        com.legend.compiler.element.type.Multiplicity
                                .Bounded.ONE),
                false /* resolver-synth */);
        TypedLambda finCond = java.util.Objects.requireNonNull(
                aj.condition(), "chain-final association condition");
        String lpChain = finCond.parameters().get(0);
        TypedSpec finBody = Pipelines.prefixColumns(
                finCond.body().get(finCond.body().size() - 1),
                lpChain, midPrefix,
                v -> new TypedVariable(lpChain,
                        new ExprType(midJoinedRow,
                                com.legend.compiler.element.type
                                        .Multiplicity.Bounded.ONE)));
        return new ChainMidFold(widened,
                aj.withCondition(new TypedLambda(finCond.parameters(),
                        List.of(finBody), finCond.info())));
    }

    record CorrAggSub(TypedSpec subSource,
            @com.legend.Nullable List<String> keyCols,
            Type.RelationType keyRow,
            @com.legend.Nullable String targetPrefix,
            @com.legend.Nullable String rowVar,
            Type.@com.legend.Nullable RelationType joinedRow,
            @com.legend.Nullable ParentCopy pc) {}


    CorrAggSub corrAggSubSource(ClassSource cs, String head,
            AssociationJoins.AssocJoin aj, @com.legend.Nullable TypedLambda corrAgg,
            boolean filterPosition) {
        if (corrAgg == null) {
            List<String> tKeys = targetEquiKeysOrNull(java.util.Objects.requireNonNull(aj.condition()));
            // FILTER position takes the PARENT-COPY shape below even for a
            // simple equi condition (engine isolation copies the root tree
            // — duplicate root rows double the group; constraint8 golden);
            // the chained shape already re-joins the parent inside.
            if (tKeys != null && !filterPosition) {
                return new CorrAggSub(aj.targetPipeline(), tKeys,
                        aj.targetRow(), null, null, null, null);
            }
            if (tKeys == null) {
                return chainedAggSubSource(cs, head, aj);
            }
        }
        List<String> keyCols = parentEquiKeys(aj.condition(), head);
        ParentCopy pc = java.util.Objects.requireNonNull(
                parentCopyFor(cs, corrAgg));
        Type.RelationType pcRow = Type.requireRelationSchema(pc.mat().pipeline().info().type());
        String corrTp = AssociationJoins.prefixFor(head + "_t", cs);
        // audit 23 B7: the joined row is the PARENT COPY (extra
        // slot-prefixed columns beyond cs.rowType()) — collision-check
        // against IT, exactly like the exploding sibling below
        while (hasColPrefixed(pcRow, corrTp)) {
            corrTp = "_" + corrTp;
        }
        List<Type.Column> jCols = new ArrayList<>(pcRow.columns());
        for (Type.Column c : aj.targetRow().columns()) {
            jCols.add(new Type.Column(
                    corrTp + c.name(), c.type(), c.multiplicity()));
        }
        Type.RelationType corrJoinedRow = new Type.RelationType(jCols);
        var jInfo = new ExprType(Type.relation(corrJoinedRow),
                com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
        TypedSpec joinedSub = new TypedJoin(pc.mat().pipeline(),
                aj.targetPipeline(), AssociationJoins.leftKind(), java.util.Objects.requireNonNull(aj.condition()),
                Optional.of(corrTp), null, jInfo,
                false /* resolver-synth */);
        String corrRowVar = "_cj";
        // audit 23: a user lambda variable named _cj would shadow-stop the
        // rewriters — bump until fresh against the pred's own names
        Set<String> cjTaken = new LinkedHashSet<>();
        if (corrAgg != null) {
            collectVarNamesInto(corrAgg, cjTaken);
        }
        int cjOrd = 2;
        while (cjTaken.contains(corrRowVar)) {
            corrRowVar = "_cj" + cjOrd++;
        }
        if (corrAgg == null) {
            // filter-position parent copy: no correlated pred, the sub is
            // the bare parentCopy JOIN target grouped by the parent keys
            return new CorrAggSub(joinedSub, keyCols, pcRow, corrTp,
                    corrRowVar, corrJoinedRow, pc);
        }
        TypedLambda where = assocMaterial.corrPredOnJoinedRow(
                corrAgg, cs, aj.target(), corrTp,
                aj.targetSlotPrefixes(), aj.targetSubNavs(),
                pc.mat().slotPrefixes(),
                pc.subNavs(), corrRowVar, corrJoinedRow);
        return new CorrAggSub(new TypedFilter(joinedSub, where, jInfo),
                keyCols, pcRow, corrTp, corrRowVar, corrJoinedRow, pc);
    }


    record ExplodingSub(TypedSpec target, Type.RelationType row,
            TypedLambda cond) {}


    ExplodingSub explodingSubselect(ClassSource cs,
            AssociationJoins.AssocJoin aj, Type.RelationType leftRowT) {
        // #69 EXPLODING parent-copy subselect (engine
        // testFunctionVariables goldens): a correlated pred whose
        // OUTER reads hop a parent NAV composes inside a subselect
        // that re-joins the PARENT extent — sub = parentCopy JOIN
        // target ON the association condition, WHERE the pred over
        // the joined row, projected to (parent equi keys under
        // collision-proof _pk aliases + the target columns under
        // their own names), LEFT-joined back on key equality. One
        // row per matching target instance — the row explosion of
        // the flat join, with the pred resolvable.
        // Re-key by the PARENT's PK (engine #69 goldens: 'root'.ID =
        // sub.ID) — the assoc-FK equi keys collapse same-FK parents into
        // each other's correlation scope (testVariableReferenceWith-
        // NestedFilterMultiple: 15 rows for 7 people). FK keys stay the
        // fallback for PK-less parents.
        List<String> pkKeys = RelationalRootForm.primaryKeyColumns(
                cs.classFqn(), cs.pipeline(), cs.mappingFqn(),
                sources.ctx());
        List<String> keyCols = !pkKeys.isEmpty() ? pkKeys
                : parentEquiKeys(aj.condition(), aj.prefix());
        List<TypedLambda> allCorrs = new ArrayList<>();
        if (aj.corrSubPred() != null) {
            allCorrs.add(aj.corrSubPred());
        }
        for (String snHead0 : aj.targetSubNavs().keySet()) {
            TypedLambda tp0 = assocMaterial.synthetics().correlatedPred(snHead0);
            if (tp0 != null) {
                allCorrs.add(tp0);
            }
        }
        ParentCopy pc = java.util.Objects.requireNonNull(
                parentCopyFor(cs, allCorrs));
        Type.RelationType pcRow = Type.requireRelationSchema(pc.mat().pipeline().info().type());
        String corrTp = aj.prefix() + "t_";
        while (hasColPrefixed(pcRow, corrTp)) {
            corrTp = "_" + corrTp;
        }
        List<Type.Column> jCols = new ArrayList<>(pcRow.columns());
        for (Type.Column c : aj.targetRow().columns()) {
            jCols.add(new Type.Column(
                    corrTp + c.name(), c.type(), c.multiplicity()));
        }
        Type.RelationType jRow = new Type.RelationType(jCols);
        var jInfo = new ExprType(Type.relation(jRow),
                com.legend.compiler.element.type.Multiplicity
                        .Bounded.ONE);
        TypedSpec joinedSub = new TypedJoin(pc.mat().pipeline(),
                aj.targetPipeline(), AssociationJoins.leftKind(), java.util.Objects.requireNonNull(aj.condition()),
                Optional.of(corrTp), null, jInfo,
                false /* resolver-synth */);
        // audit 23: same _cj freshness bump as corrAggSubSource
        String cjVar = "_cj";
        Set<String> cjTaken2 = new LinkedHashSet<>();
        collectVarNamesInto(aj.corrSubPred(), cjTaken2);
        int cjOrd2 = 2;
        while (cjTaken2.contains(cjVar)) {
            cjVar = "_cj" + cjOrd2++;
        }
        // a HEAD-pred-less reroute (only TAIL-hop preds correlate) skips
        // the head WHERE; the tail loop below is the sub's only filter
        TypedSpec filtered = joinedSub;
        if (aj.corrSubPred() != null) {
            TypedLambda where = assocMaterial.corrPredOnJoinedRow(
                    aj.corrSubPred(), cs, aj.target(), corrTp,
                    aj.targetSlotPrefixes(), aj.targetSubNavs(),
                    pc.mat().slotPrefixes(), pc.subNavs(),
                    cjVar, jRow);
            filtered = new TypedFilter(joinedSub, where, jInfo);
        }
        // TAIL-hop parked CORRELATED preds (#69 second filter — the
        // firm#f0.address#f1 chain): a target sub-nav head carrying a
        // parked pred ANDs into this sub's WHERE — both sides already
        // ride the joined row (the target's sub-nav slot columns and the
        // parentCopy's own materialized demand columns). The engine nests
        // a second subselect; the row set is identical for [0..1] hops.
        for (var snE : aj.targetSubNavs().entrySet()) {
            TypedLambda parked =
                    assocMaterial.synthetics().correlatedPred(snE.getKey());
            if (parked == null) {
                continue;
            }
            TypedLambda w2 = assocMaterial.corrPredOnJoinedRowForSubNav(
                    parked, cs, aj.target(), corrTp, snE.getValue(),
                    pc.mat().slotPrefixes(), pc.subNavs(), cjVar, jRow);
            filtered = new TypedFilter(filtered, w2, jInfo);
        }
        var cjInfo = new ExprType(jRow,
                com.legend.compiler.element.type.Multiplicity
                        .Bounded.ONE);
        List<com.legend.compiler.spec.typed.TypedFuncCol> pCols =
                new ArrayList<>();
        List<Type.Column> subColsX = new ArrayList<>();
        List<String> subKeys = new ArrayList<>();
        java.util.Objects.requireNonNull(keyCols,
                "chained agg sub requires equi keys");
        for (int ki = 0; ki < keyCols.size(); ki++) {
            String k = keyCols.get(ki);
            var col = pcRow.columns().stream()
                    .filter(c -> c.name().equals(k)).findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "resolver bug: equi-key column '" + k
                                    + "' missing from the parent"
                                    + " copy row"));
            String pk = "_pk" + ki;
            // audit 23 #75: a physical column literally named _pk<i> on
            // the parent-copy row would collide with the projected key
            while (hasCol(pcRow, pk)) {
                pk = "_" + pk;
            }
            subKeys.add(pk);
            pCols.add(projectedCol(pk, cjVar, cjInfo, k,
                    new ExprType(col.type(), col.multiplicity())));
            subColsX.add(new Type.Column(pk, col.type(),
                    col.multiplicity()));
        }
        for (Type.Column c : aj.targetRow().columns()) {
            pCols.add(projectedCol(c.name(), cjVar, cjInfo,
                    corrTp + c.name(),
                    new ExprType(c.type(), c.multiplicity())));
            subColsX.add(c);
        }
        Type.RelationType subRowX = new Type.RelationType(subColsX);
        TypedSpec subPipe = new TypedProject(filtered, pCols,
                new ExprType(Type.relation(subRowX),
                        com.legend.compiler.element.type.Multiplicity
                                .Bounded.ONE));
        return new ExplodingSub(subPipe, subRowX,
                assocMaterial.pkEqualityCond(keyCols, subKeys,
                        leftRowT, subRowX));
    }


private static com.legend.compiler.spec.typed.TypedFuncCol projectedCol(
            String name, String rowVar, ExprType rowInfo, String readCol,
            ExprType colInfo) {
        var read = new com.legend.compiler.spec.typed.TypedPropertyAccess(
                new TypedVariable(rowVar, rowInfo), readCol, colInfo);
        var fn = new TypedLambda(List.of(rowVar), List.<TypedSpec>of(read),
                new ExprType(
                        new Type.FunctionType(
                                List.of(new Type.Param(rowInfo.type(),
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ONE)),
                        new Type.Param(colInfo.type(), colInfo.multiplicity())),
                        com.legend.compiler.element.type.Multiplicity
                                .Bounded.ONE));
        return new com.legend.compiler.spec.typed.TypedFuncCol(name, fn);
    }


private static boolean hasColPrefixed(Type.RelationType row, String prefix) {
        for (Type.Column c : row.columns()) {
            if (c.name().startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }


private static @com.legend.Nullable List<String> parentEquiKeys(@com.legend.Nullable TypedLambda cond, String head) {
        List<String> keys = new ArrayList<>();
        if (!collectEquiKeys(java.util.Objects.requireNonNull(cond).body().get(cond.body().size() - 1),
                cond.parameters().get(1), cond.parameters().get(0), keys)
                || keys.isEmpty()) {
            throw new NotImplementedException("correlated aggregate over"
                    + " navigation '" + head + "' requires a conjunctive"
                    + " equi-join association condition (parent-copy"
                    + " grouped-subselect emission)");
        }
        return keys;
    }


    record ParentCopy(Pipelines.Materialized mat,
            Map<String, Substitution.SubNav> subNavs) {}


    /** Null {@code corr} = an UNCORRELATED parent copy (filter-position
     * aggregate): no outer reads, the plain parent pipeline materializes. */
    @com.legend.Nullable ParentCopy parentCopyFor(ClassSource cs,
            @com.legend.Nullable TypedLambda corr) {
        return parentCopyFor(cs,
                corr == null ? List.of() : List.of(corr));
    }

    /** Parent copy demanded by SEVERAL correlated preds (a head pred
     * plus tail-hop preds — the exploding sub's whole pred set): every
     * pred's OUTER reads join the copy's demand. */
    @com.legend.Nullable ParentCopy parentCopyFor(ClassSource cs,
            List<TypedLambda> corrs) {
        Set<List<String>> outerPaths = new LinkedHashSet<>();
        for (TypedLambda corr : corrs) {
            Set<String> names = new LinkedHashSet<>();
            for (TypedSpec b : corr.body()) {
                collectVarNamesInto(b, names);
            }
            names.removeAll(corr.parameters());
            for (String v : names) {
                for (TypedSpec b : corr.body()) {
                    FlattenOps.consumedPaths(b, v, outerPaths);
                }
            }
        }
        Set<String> slots = Pipelines.slotAliases(cs.pipeline());
        var navSteps = Pipelines.navSteps(cs.pipeline());
        Set<String> slotDemand = new LinkedHashSet<>();
        Set<String> navDemand = new LinkedHashSet<>();
        Map<String, String> navByHead = new LinkedHashMap<>();
        for (List<String> pp : outerPaths) {
            TypedSpec hb = cs.bindings().get(pp.get(0));
            if (hb == null) {
                continue;
            }
            if (pp.size() >= 2) {
                String al = InnerDemand.navSlotAlias(hb, cs.rowVar(), navSteps.keySet());
                if (al != null) {
                    navDemand.add(al);
                    navByHead.put(pp.get(0), al);
                    continue;
                }
            }
            collectAliasReads(hb, cs.rowVar(), slots, slotDemand);
        }
        slotDemand = Pipelines.closeOverConditions(cs.pipeline(), slotDemand);
        Pipelines.Materialized mat = navDemand.isEmpty()
                ? Pipelines.materialize(cs.pipeline(), slotDemand, cs.classFqn())
                : Pipelines.materialize(cs.pipeline(), slotDemand, navDemand,
                        cs.classFqn(),
                        (al2, tc2) -> Pipelines.materialize(
                                sources.get(cs.mappingFqn(), tc2, cs.scope()).pipeline(),
                                java.util.Set.of(), tc2).pipeline());
        Map<String, Substitution.SubNav> subNavs = new LinkedHashMap<>();
        for (var e : navByHead.entrySet()) {
            String pfx = mat.slotPrefixes().get(e.getValue());
            var stepT = java.util.Objects.requireNonNull(navSteps.get(e.getValue())).target();
            if (pfx == null || !(stepT instanceof TypedGetAll stg)) {
                continue;
            }
            ClassSource sub = sources.get(cs.mappingFqn(), stg.classFqn(), cs.scope());
            subNavs.put(e.getKey(), new Substitution.SubNav(
                    pfx, sub.rowVar(), sub.bindings()));
        }
        return new ParentCopy(mat, subNavs);
    }


static void collectVarNamesInto(@com.legend.Nullable TypedSpec n, Set<String> out) {
        if (n == null) {
            return;
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedVariable v) {
            out.add(v.name());
        }
        for (TypedSpec c : n.children()) {
            collectVarNamesInto(c, out);
        }
    }


private static @com.legend.Nullable List<String> targetEquiKeysOrNull(TypedLambda cond) {
        List<String> keys = new ArrayList<>();
        if (!collectEquiKeys(cond.body().get(cond.body().size() - 1),
                cond.parameters().get(0), cond.parameters().get(1), keys)
                || keys.isEmpty()) {
            return null;
        }
        return keys;
    }

    /** #77 CHAINED/ROUTED aggregated navigation (engine parent-copy shape,
     * unionOfViews golden): the association condition reads the parent
     * through a MID join slot (firm → midTable → target) and/or ORs
     * union-route keys, so the flat target-grouped subselect cannot key it
     * — grouping by OR'd TARGET keys would be one group per target row
     * (row explosion, wrong rows; never widen collectEquiKeys). Instead:
     * sub = the parent pipeline with the mid slots AND the head's navigate
     * join materialized (exploded INSIDE the sub), GROUP BY the FIRST
     * hop's parent-side equi keys (chased through the mid slot's own join
     * condition), aggregates over the prefixed target columns; the outer
     * row joins back on those keys only — no mid join outside (engine:
     * `... group by "firmextension_2".firmId) ... on (root.firmId =
     * sub.firmId)`, testUnion.pure golden). */
    /** zip's two inputs as (source, scalar mapper) — the auto-map
     * spelling {@code $vals.prop} (TypedMap) or a bare property access;
     * null when either input has no such shape or the sources differ
     * (value equality — the spliced result chain appears twice). */
    static @com.legend.Nullable TypedSpec zipPairMap(TypedMap zm, TypedNativeCall zc,
            java.util.function.UnaryOperator<TypedSpec> resolver) {
        TypedSpec zp = zipPairProject(zc, resolver);
        if (zp == null) {
            // not two per-row reads of ONE class chain: zip is the
            // POSITIONAL pairing of two ordered collections (collection.pure
            // zip — truncates to the shorter); that is the LIST carrier's
            // list_zip (Scalars), never a row pairing
            return null;
        }
        return new TypedMap(zp, zm.mapper(), zm.info());
    }

    private static @com.legend.Nullable TypedSpec zipPairProject(TypedNativeCall zc,
            java.util.function.UnaryOperator<TypedSpec> resolver) {
        Object[] a = zipSide(zc.args().get(0));
        Object[] b = zipSide(zc.args().get(1));
        if (a == null || b == null || !a[0].equals(b[0])) {
            return null;
        }
        TypedLambda fa = (TypedLambda) a[1];
        TypedLambda fb = (TypedLambda) b[1];
        Type.RelationType row = new Type.RelationType(List.of(
                new Type.Column("first",
                        fa.functionType().result().type(),
                        fa.functionType().result().multiplicity()),
                new Type.Column("second",
                        fb.functionType().result().type(),
                        fb.functionType().result().multiplicity())));
        TypedProject proj = new TypedProject((TypedSpec) a[0],
                List.of(new TypedFuncCol("first", fa),
                        new TypedFuncCol("second", fb)),
                com.legend.compiler.element.type.ExprType.one(
                        Type.relation(row)));
        return resolver.apply(proj);
    }

    /** The one-param lambda's body with its parameter read as {@code name}
     * (typed {@code elemOne}) — the zip sides share ONE row variable. */
    private static TypedSpec renameParam(TypedLambda lam, String name,
            ExprType elemOne) {
        String from = lam.parameters().get(0);
        TypedSpec body = lam.body().get(lam.body().size() - 1);
        return renameVar(body, from, new TypedVariable(name, elemOne));
    }

    private static TypedSpec renameVar(TypedSpec n, String from, TypedVariable to) {
        if (n instanceof TypedVariable v) {
            return v.name().equals(from) ? to : v;
        }
        if (n instanceof TypedLambda l && l.parameters().contains(from)) {
            return l;
        }
        return n.mapChildren(c -> renameVar(c, from, to));
    }

    private static Object @com.legend.Nullable [] zipSide(TypedSpec n) {
        // NESTED zip(b, c) as a side: the inner pair is ONE column whose
        // value is the ^Pair(first, second) STRUCT (the platform's Pair
        // carrier) over the same source — zip(a, zip(b, c))->map(p |
        // $p.second.first) reads the struct field (testSortByLambdaDeepOptional)
        if (n instanceof TypedNativeCall zc
                && "meta::pure::functions::collection::zip".equals(
                        zc.callee().qualifiedName())
                && zc.args().size() == 2) {
            Object[] a = zipSide(zc.args().get(0));
            Object[] b = zipSide(zc.args().get(1));
            if (a == null || b == null || !a[0].equals(b[0])) {
                return null;
            }
            TypedLambda fa = (TypedLambda) a[1];
            TypedLambda fb = (TypedLambda) b[1];
            Type pairT = zc.info().type();
            Type.Param elem = fa.functionType().params().get(0);
            var elemOne = new ExprType(elem.type(),
                    com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
            java.util.Map<String, TypedSpec> fields = new java.util.LinkedHashMap<>();
            fields.put("first", renameParam(fa, "_zp", elemOne));
            fields.put("second", renameParam(fb, "_zp", elemOne));
            var fnT = new Type.FunctionType(
                    List.of(new Type.Param(elem.type(),
                            com.legend.compiler.element.type.Multiplicity
                                    .Bounded.ONE)),
                    new Type.Param(pairT,
                            com.legend.compiler.element.type.Multiplicity
                                    .Bounded.ONE));
            return new Object[] {a[0], new TypedLambda(List.of("_zp"),
                    List.of(new com.legend.compiler.spec.typed.TypedNewInstance(
                            com.legend.compiler.element.type.PlatformTypes.PAIR,
                            fields, ExprType.one(pairT))),
                    ExprType.one(fnT))};
        }
        if (n instanceof TypedMap m && m.mapper().parameters().size() == 1
                && !(m.mapper().functionType().result()
                        .type() instanceof Type.ClassType)) {
            return new Object[] {m.source(), m.mapper()};
        }
        // bare $vals.prop / $vals.hop.prop spelling (scalar read over a
        // class collection, auto-mapped through to-one hops): the source
        // is the class-collection ROOT — the deepest class-typed source
        // that is not itself a class-typed read — and the mapper rebuilds
        // the hop chain over the row variable (zip($vals.address.name,
        // zip($vals.firstName, $vals.lastName)) shares ONE root).
        if (n instanceof TypedPropertyAccess pa
                && !(pa.info().type() instanceof Type.ClassType)
                && pa.source().info().type() instanceof Type.ClassType) {
            java.util.List<TypedPropertyAccess> hops = new java.util.ArrayList<>();
            TypedSpec root = pa;
            while (root instanceof TypedPropertyAccess h
                    && h.source().info().type() instanceof Type.ClassType) {
                hops.add(0, h);
                root = h.source();
            }
            if (hops.size() != 1) {
                // a read THROUGH a hop ($vals.address.name) auto-maps —
                // an empty hop DROPS the element, so the pairing is
                // positional over the survivors (engine zip semantics),
                // not per row: the list carrier's zip owns it
                return null;
            }
            Type.ClassType ec = (Type.ClassType) root.info().type();
            var elemOne = new com.legend.compiler.element.type.ExprType(
                    ec, com.legend.compiler.element.type.Multiplicity
                            .Bounded.ONE);
            TypedSpec body = new TypedVariable("_zp", elemOne);
            for (TypedPropertyAccess h : hops) {
                body = new TypedPropertyAccess(body, h.property(),
                        new com.legend.compiler.element.type.ExprType(
                                h.info().type(),
                                com.legend.compiler.element.type.Multiplicity
                                        .Bounded.ZERO_ONE));
            }
            var fnT = new Type.FunctionType(
                    List.of(new Type.Param(ec,
                            com.legend.compiler.element.type.Multiplicity
                                    .Bounded.ONE)),
                    new Type.Param(pa.info().type(),
                            com.legend.compiler.element.type.Multiplicity
                                    .Bounded.ZERO_ONE));
            return new Object[] {root, new TypedLambda(
                    List.of("_zp"), List.of(body),
                    new com.legend.compiler.element.type.ExprType(fnT,
                            com.legend.compiler.element.type.Multiplicity
                                    .Bounded.ONE))};
        }
        return null;
    }


    /** The grouped subselect's GROUP-BY keys + their columns; a key that
     * only exists as per-member splits (k_0…) is the U4 union rung —
     * loud; a key missing entirely is a resolver bug. */
    static void groupKeysInto(List<String> keyCols, Type.RelationType keyRow,
            List<com.legend.compiler.spec.typed.TypedGroupBy.GroupKey> keys,
            List<Type.Column> subCols) {
        for (String k : keyCols) {
            var col = keyRow.columns().stream()
                    .filter(c -> c.name().equals(k)).findFirst()
                    .orElseThrow(() -> keyRow.columns().stream()
                            .anyMatch(c -> c.name().equals(k + "_0"))
                    ? new NotImplementedException(
                            "aggregate over navigation into a UNION-"
                            + "mapped target: the equi-key '" + k
                            + "' splits into per-member columns ("
                            + k + "_0…) — the grouped subselect needs"
                            + " per-member key pairs + OR join-back"
                            + " (U4 rung, not built yet)")
                    : new IllegalStateException(
                            "resolver bug: equi-key column '" + k
                                    + "' missing from the "
                                    + "grouping row"));
            keys.add(new com.legend.compiler.spec.typed.TypedGroupBy.GroupKey(
                    k, Optional.empty()));
            subCols.add(col);
        }
    }

    /** Per-head demand groups in emission order: projection-position
     * demands first, then filter-position demands as their OWN group —
     * each group becomes one grouped subselect (positions differ in
     * shape: only filter isolation parent-copies the root tree). */
    static List<Map.Entry<String, List<StoreResolver.AggDemand>>> splitAggGroups(
            Map<String, List<StoreResolver.AggDemand>> aggDemands) {
        List<Map.Entry<String, List<StoreResolver.AggDemand>>> groups =
                new ArrayList<>();
        for (var byHead : aggDemands.entrySet()) {
            List<StoreResolver.AggDemand> proj = new ArrayList<>();
            List<StoreResolver.AggDemand> filt = new ArrayList<>();
            for (StoreResolver.AggDemand d : byHead.getValue()) {
                (d.filterPosition() ? filt : proj).add(d);
            }
            if (!proj.isEmpty()) {
                groups.add(Map.entry(byHead.getKey(), proj));
            }
            if (!filt.isEmpty()) {
                groups.add(Map.entry(byHead.getKey(), filt));
            }
        }
        return groups;
    }

    /** UNION-split key expansion: a key absent from the row but present
     * as per-member variants (k_0, k_1, …) expands to ALL of them — the
     * grouped subselect groups by every split column and joins back on
     * OR of pairs (task #27 U4). Keys found verbatim pass through. */
    static List<String> expandSplitKeys(List<String> keys,
            Type.RelationType row) {
        List<String> out = new ArrayList<>();
        for (String k : keys) {
            if (row.columns().stream().anyMatch(
                    c -> c.name().equals(k))) {
                out.add(k);
                continue;
            }
            // A REAL member split mints k_0..k_{n-1} for every union member
            // — contiguous from 0, at least two. Any-digit-suffix matching
            // absorbed a PHYSICAL column named ID_2 as a split key and
            // grouped by it (text-surgery audit §1.1 #6); the shape check
            // stands in for provenance until split names are threaded from
            // UnionSynthesis's own mint.
            List<String> split = new ArrayList<>();
            for (int i = 0; ; i++) {
                String cand = k + "_" + i;
                if (row.columns().stream()
                        .noneMatch(c -> c.name().equals(cand))) {
                    break;
                }
                split.add(cand);
            }
            if (split.size() >= 2) {
                out.addAll(split);
            } else {
                out.add(k);
            }
        }
        return out;
    }

    private CorrAggSub chainedAggSubSource(ClassSource cs, String head,
            AssociationJoins.AssocJoin aj) {
        TypedLambda cond = aj.condition();
        String srcVar = java.util.Objects.requireNonNull(cond, "cond").parameters().get(0);
        Set<String> slots = Pipelines.slotAliases(cs.pipeline());
        Set<String> condSlots = new LinkedHashSet<>();
        for (TypedSpec b : cond.body()) {
            collectAliasReads(b, srcVar, slots, condSlots);
        }
        List<String> keyCols = new ArrayList<>();
        if (condSlots.isEmpty()) {
            List<String> ks = parentKeysLenient(cond.body()
                    .get(cond.body().size() - 1), srcVar);
            if (ks == null || ks.isEmpty()) {
                throw new NotImplementedException("aggregate over navigation '"
                        + head + "' requires equi-join parent keys"
                        + " (grouped-subselect emission)");
            }
            keyCols.addAll(ks);
        } else {
            var slotSteps = Pipelines.slotSteps(cs.pipeline());
            for (String alias : condSlots) {
                var slot = slotSteps.get(alias);
                List<String> ks = slot == null ? null
                        : parentKeysLenient(slot.condition().body()
                                .get(slot.condition().body().size() - 1),
                                slot.condition().parameters().get(0));
                if (ks == null || ks.isEmpty()) {
                    throw new NotImplementedException(
                            "aggregate over chained navigation '" + head
                            + "': the first hop's join condition carries no"
                            + " clean parent-side equi keys (deeper mid"
                            + " chains are not supported yet)");
                }
                keyCols.addAll(ks);
            }
        }
        keyCols = new ArrayList<>(new LinkedHashSet<>(keyCols));
        // The head's navigate step: materialize the parent pipeline with
        // the condition's mid slots demanded AND the head's navigate join
        // flattened onto it (materialize rewrites the predicate's slot
        // reads to the prefixed columns; the target widens for the keys
        // the predicate binds on — the union-route ID_i columns).
        TypedSpec binding = cs.bindings().get(SyntheticHeads.realHead(head));
        var navSteps = Pipelines.navSteps(cs.pipeline());
        String headAlias = binding == null ? null
                : InnerDemand.navSlotAlias(binding, cs.rowVar(),
                        navSteps.keySet());
        if (headAlias != null) {
            Pipelines.Materialized mat2 = Pipelines.materialize(cs.pipeline(),
                    Pipelines.closeOverConditions(cs.pipeline(), condSlots),
                    java.util.Set.of(headAlias), cs.classFqn(),
                    (al, tc) -> aj.targetPipeline());
            String corrTp = mat2.slotPrefixes().get(headAlias);
            Type.RelationType joinedRow =
                    Type.requireRelationSchema(mat2.pipeline().info().type());
            return new CorrAggSub(mat2.pipeline(), keyCols, joinedRow,
                    corrTp, "_cj", joinedRow, null);
        }
        // ASSOCIATION-route head (no navigate step; the OR'd condition
        // reads the parent main row directly — to-side union dispatch):
        // hand-build parentCopy JOIN target ON cond, same shape as the
        // correlated arm minus the predicate filter.
        if (!condSlots.isEmpty()) {
            throw new NotImplementedException("aggregate over association"
                    + " navigation '" + head + "' whose condition reads a"
                    + " join slot is not supported yet");
        }
        Pipelines.Materialized pMat = Pipelines.materialize(
                cs.pipeline(), java.util.Set.of(), cs.classFqn());
        Type.RelationType pRow =
                Type.requireRelationSchema(pMat.pipeline().info().type());
        String corrTp = AssociationJoins.prefixFor(head + "_t", cs);
        while (hasColPrefixed(pRow, corrTp)) {
            corrTp = "_" + corrTp;
        }
        List<Type.Column> jCols = new ArrayList<>(pRow.columns());
        for (Type.Column c : aj.targetRow().columns()) {
            jCols.add(new Type.Column(
                    corrTp + c.name(), c.type(), c.multiplicity()));
        }
        Type.RelationType jRow = new Type.RelationType(jCols);
        TypedSpec joined = new TypedJoin(pMat.pipeline(), aj.targetPipeline(),
                AssociationJoins.leftKind(), cond, Optional.of(corrTp), null,
                new ExprType(Type.relation(jRow),
                        com.legend.compiler.element.type.Multiplicity
                                .Bounded.ONE),
                false /* resolver-synth */);
        return new CorrAggSub(joined, keyCols, jRow, corrTp, "_cj", jRow, null);
    }

    /** Parent-side equi keys, OR-tolerant: descends and/or; every `equal`
     * contributes its bare parent-side column when the other operand does
     * not reference the parent row. Returns null on any unrecognized
     * conjunct (slot-shaped reads are NOT bare — the caller's chase or
     * wall handles them). */
    private static @com.legend.Nullable List<String> parentKeysLenient(TypedSpec n,
            String parentVar) {
        if (!(n instanceof TypedNativeCall c)) {
            return null;
        }
        String q = c.callee().qualifiedName();
        if (q.equals("meta::pure::functions::boolean::and")
                || q.equals("meta::pure::functions::boolean::or")) {
            List<String> out = new ArrayList<>();
            for (TypedSpec a : c.args()) {
                List<String> ks = parentKeysLenient(a, parentVar);
                if (ks == null) {
                    return null;
                }
                out.addAll(ks);
            }
            return out;
        }
        if (q.equals("meta::pure::functions::boolean::equal")
                && c.args().size() == 2) {
            TypedSpec a = c.args().get(0);
            TypedSpec b = c.args().get(1);
            String aCol = bareColumnOn(a, parentVar);
            String bCol = bareColumnOn(b, parentVar);
            if (aCol != null && !referencesVar(b, parentVar)) {
                return List.of(aCol);
            }
            if (bCol != null && !referencesVar(a, parentVar)) {
                return List.of(bCol);
            }
            return null;
        }
        return null;
    }


private static boolean collectEquiKeys(TypedSpec n, String srcVar,
                                           String tgtVar,
                                           List<String> out) {
        if (!(n instanceof TypedNativeCall c)) {
            return false;
        }
        String q = c.callee().qualifiedName();
        if (q.equals("meta::pure::functions::boolean::and")) {
            return c.args().stream()
                    .allMatch(a -> collectEquiKeys(a, srcVar, tgtVar, out));
        }
        if (q.equals("meta::pure::functions::boolean::equal")
                && c.args().size() == 2) {
            TypedSpec a = c.args().get(0);
            TypedSpec b = c.args().get(1);
            String aCol = bareColumnOn(a, tgtVar);
            String bCol = bareColumnOn(b, tgtVar);
            if (bCol != null && !referencesVar(a, tgtVar)) {
                out.add(bCol);
                return true;
            }
            if (aCol != null && !referencesVar(b, tgtVar)) {
                out.add(aCol);
                return true;
            }
        }
        return false;
    }


private static boolean referencesVar(TypedSpec n, String var) {
        if (n instanceof TypedVariable v
                && v.name().equals(var)) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (referencesVar(c, var)) {
                return true;
            }
        }
        return false;
    }


    /** ONE aggregate column of the grouped subselect (fold 2c per-demand):
     * the map lambda (leaf binding or #69 computed mapper, prefixed onto
     * the correlated joined row when present) and the reduce lambda
     * rebuilt over the aggregate's own callee. */
    TypedAggCol aggColFor(ClassSource cs, String head,
            AssociationJoins.AssocJoin aj, StoreResolver.AggDemand d,
            String alias, @com.legend.Nullable TypedLambda corrAgg,
            @com.legend.Nullable String corrTp,
            @com.legend.Nullable String corrRowVar,
            Type.@com.legend.Nullable RelationType corrJoinedRow,
            @com.legend.Nullable ParentCopy pc) {
        TypedSpec mapBody;
        String mapVar = aj.target().rowVar();
        var mapRowType = aj.targetRow();
        Type leafType;
        com.legend.compiler.element.type.Multiplicity leafMult;
        if (d.mapper() != null) {
            // COMPUTED mapper (#69): the mapper body substitutes through
            // the target's bindings onto the sub row (the same rewriter
            // that serves the correlated pred; empty parent side when
            // uncorrelated). A mapper reading the OUTER instance is a
            // separate demand feed — loud.
            Set<String> mFree = new LinkedHashSet<>();
            for (TypedSpec b : d.mapper().body()) {
                collectVarNamesInto(b, mFree);
            }
            mFree.removeAll(d.mapper().parameters());
            if (!mFree.isEmpty()) {
                throw new NotImplementedException(
                        "computed aggregate mapper over navigation '"
                                + head + "' reads outer variable(s) "
                                + mFree + " — outer-correlated mapper"
                                + " bodies are not supported yet");
            }
            TypedLambda mm = corrTp == null
                    ? assocMaterial.corrPredOnJoinedRow(d.mapper(),
                            cs, aj.target(), "",
                            aj.targetSlotPrefixes(),
                            aj.targetSubNavs(), Map.of(),
                            Map.of(), aj.target().rowVar(),
                            aj.targetRow())
                    : assocMaterial.corrPredOnJoinedRow(d.mapper(),
                            cs, aj.target(), corrTp,
                            aj.targetSlotPrefixes(),
                            aj.targetSubNavs(),
                            pc == null ? Map.of() : pc.mat().slotPrefixes(),
                            pc == null ? Map.of() : pc.subNavs(),
                            java.util.Objects.requireNonNull(corrRowVar, "corrRowVar"),
                            java.util.Objects.requireNonNull(corrJoinedRow, "corrJoinedRow"));
            mapBody = mm.body().get(0);
            leafType = mapBody.info().type();
            leafMult = mapBody.info().multiplicity();
        } else {
            TypedSpec leafBinding =
                    aj.target().bindings().get(d.leaf());
            if (leafBinding == null) {
                throw new MappingResolutionException("property '"
                        + d.leaf()
                        + "' of class '" + aj.target().classFqn()
                        + "' has no binding in mapping '"
                        + cs.mappingFqn()
                        + "' (aggregated navigation leaf)",
                        aj.target().classFqn());
            }
            mapBody = leafBinding;
            leafType = leafBinding.info().type();
            leafMult = leafBinding.info().multiplicity();
            if (corrTp != null) {
                // correlated/chained sub: the leaf reads land PREFIXED on
                // the joined row
                final String rv = corrRowVar;
                final var jr = corrJoinedRow;
                mapBody = Pipelines.prefixColumns(leafBinding,
                        aj.target().rowVar(), corrTp,
                        v -> new com.legend.compiler.spec.typed
                                .TypedVariable(
                                        java.util.Objects.requireNonNull(rv, "rv"),
                                        new ExprType(java.util.Objects
                                                .requireNonNull(jr, "jr"),
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ONE)));
            }
        }
        if (corrTp != null) {
            mapVar = corrRowVar;
            mapRowType = corrJoinedRow;
        }
        TypedLambda map = new TypedLambda(
                List.of(mapVar),
                List.of(mapBody),
                new ExprType(
                        new Type.FunctionType(
                                List.of(new com.legend.compiler
                                        .element.type.Type.Param(
                                        java.util.Objects.requireNonNull(
                                                mapRowType,
                                                "assoc join without target row"),
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ONE)),
                                new Type.Param(leafType, leafMult)),
                        com.legend.compiler.element.type.Multiplicity
                                .Bounded.ONE));
        String yv = "_y";
        List<TypedSpec> reduceArgs = new ArrayList<>();
        reduceArgs.add(new TypedVariable(yv,
                new ExprType(leafType,
                        com.legend.compiler.element.type.Multiplicity
                                .Bounded.ZERO_MANY)));
        for (int i = 1; i < d.node().args().size(); i++) {
            TypedSpec extra = d.node().args().get(i);
            if (referencesVar(extra, aj.target().rowVar())) {
                throw new NotImplementedException("aggregate '"
                        + d.node().callee().qualifiedName()
                        + "' over navigation '" + head + "' with an"
                        + " instance-dependent extra argument is not"
                        + " supported");
            }
            reduceArgs.add(extra);
        }
        TypedSpec reduceCall = new com.legend.compiler.spec.typed
                .TypedNativeCall(d.node().callee(), reduceArgs,
                d.node().info());
        TypedLambda reduce = new TypedLambda(List.of(yv),
                List.of(reduceCall),
                new ExprType(
                        new Type.FunctionType(
                                List.of(new com.legend.compiler
                                        .element.type.Type.Param(
                                        leafType,
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ZERO_MANY)),
                                new Type.Param(
                                        d.node().info().type(),
                                        d.node().info().multiplicity())),
                        com.legend.compiler.element.type.Multiplicity
                                .Bounded.ONE));
        // ORDERED aggregation (sortBy before joinStrings): the key
        // substitutes through the target's bindings onto the same sub
        // row as the map body and rides on the agg col as the reducer's
        // ORDER BY (string_agg(x, sep ORDER BY k)).
        TypedLambda orderLambda = null;
        if (d.orderKey() != null) {
            Set<String> oFree = new LinkedHashSet<>();
            for (TypedSpec b : d.orderKey().body()) {
                collectVarNamesInto(b, oFree);
            }
            oFree.removeAll(d.orderKey().parameters());
            if (!oFree.isEmpty()) {
                throw new NotImplementedException("ordered aggregate key"
                        + " over navigation '" + head + "' reads outer"
                        + " variable(s) " + oFree + " — not supported yet");
            }
            orderLambda = corrTp == null
                    ? assocMaterial.corrPredOnJoinedRow(d.orderKey(),
                            cs, aj.target(), "",
                            aj.targetSlotPrefixes(),
                            aj.targetSubNavs(), Map.of(),
                            Map.of(), aj.target().rowVar(),
                            aj.targetRow())
                    : assocMaterial.corrPredOnJoinedRow(d.orderKey(),
                            cs, aj.target(), corrTp,
                            aj.targetSlotPrefixes(),
                            aj.targetSubNavs(),
                            pc == null ? Map.of() : pc.mat().slotPrefixes(),
                            pc == null ? Map.of() : pc.subNavs(),
                            java.util.Objects.requireNonNull(corrRowVar, "corrRowVar"),
                            java.util.Objects.requireNonNull(corrJoinedRow, "corrJoinedRow"));
        }
        return new TypedAggCol(alias, map, reduce, orderLambda,
                d.orderAsc());
    }
    /** The bare column a side of an equi conjunct reads on {@code var}. */
    private static @com.legend.Nullable String bareColumnOn(TypedSpec n, String var) {
        return n instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable v
                && v.name().equals(var) ? pa.property() : null;
    }


    /** A fresh row var colliding with NO lambda parameter in reach (user
     * lambdas may legally be named _rN — audit capture finding). */
    static String freshRowVar(ClassSource cs, List<TypedSpec> ops,
            TypedSpec top, List<AssociationJoins.AssocJoin> assocJoins,
            List<AssociationJoins.AssocJoin> aggAssocJoins,
            Map<String, Substitution.ExistsSub> existsSubs,
            java.util.function.IntSupplier counter) {
        Set<String> paramsInReach = new LinkedHashSet<>();
        for (TypedSpec op : ops) {
            PipelineWalks.collectLambdaParams(op, paramsInReach);
        }
        PipelineWalks.collectLambdaParams(top, paramsInReach);
        for (TypedSpec b : cs.bindings().values()) {
            PipelineWalks.collectLambdaParams(b, paramsInReach);
        }
        for (AssociationJoins.AssocJoin aj : assocJoins) {
            for (TypedSpec b : aj.target().bindings().values()) {
                PipelineWalks.collectLambdaParams(b, paramsInReach);
            }
        }
        for (AssociationJoins.AssocJoin aj : aggAssocJoins) {
            paramsInReach.add(aj.target().rowVar());
            paramsInReach.add("_y");
            for (TypedSpec b : aj.target().bindings().values()) {
                PipelineWalks.collectLambdaParams(b, paramsInReach);
            }
        }
        for (Substitution.ExistsSub ex : existsSubs.values()) {
            paramsInReach.addAll(ex.orientedCond().parameters());
            for (TypedSpec b : ex.targetBindings().values()) {
                PipelineWalks.collectLambdaParams(b, paramsInReach);
            }
        }
        String fresh;
        do {
            fresh = "_r" + counter.getAsInt();
        } while (paramsInReach.contains(fresh));
        return fresh;
    }

    /** Column names a join condition reads off its SOURCE param (param 0). */
    static void collectVarColumnReads(TypedSpec n, String var, Set<String> out) {
        if (n instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable v
                && v.name().equals(var)) {
            out.add(pa.property());
        }
        for (TypedSpec c : n.children()) {
            collectVarColumnReads(c, var, out);
        }
    }

    /** Nested-scope association materials: one AssocSub per demanded
     * head (assoc or navigate-slot backed), the exists relation widened
     * with each head's LEFT join, then deep chain keys folded on top.
     * Extracted from StoreResolver.nestedScope (guardrail). */
    record NestedMaterials(TypedSpec pipe,
            Map<String, Substitution.AssocSub> assocs) {}

    /** The flatten route's own-step splicer (NavProvenance.spliceOwnStep),
     * wired by the resolver; null until then. */
    private java.util.function.@com.legend.Nullable BiFunction<ClassSource, String, ClassSource>
            ownStepSplicer;

    void setOwnStepSplicer(
            java.util.function.BiFunction<ClassSource, String, ClassSource> splicer) {
        this.ownStepSplicer = splicer;
    }

    NestedMaterials nestedAssocMaterials(TemporalFrame temporal,
            StoreResolver.Context context, ClassSource t,
            TypedSpec targetPipe,
            java.util.Set<List<String>> innerPaths,
            java.util.Set<List<String>> innerFullPaths,
            java.util.function.BiPredicate<String, String> hasAssoc) {
        return nestedAssocMaterials(temporal, context, t, targetPipe,
                innerPaths, innerFullPaths, hasAssoc, Map.of());
    }

    /** {@code preJoins}: heads ALREADY joined onto {@code targetPipe} by
     * the caller (a to-one flatten hop whose below-ops read it — the
     * hop's own join IS the scope's material for that head; joining it
     * again doubled every column name). */
    NestedMaterials nestedAssocMaterials(TemporalFrame temporal,
            StoreResolver.Context context, ClassSource t,
            TypedSpec targetPipe,
            java.util.Set<List<String>> innerPaths,
            java.util.Set<List<String>> innerFullPaths,
            java.util.function.BiPredicate<String, String> hasAssoc,
            Map<String, AssociationJoins.AssocJoin> preJoins) {
        // nested ASSOC materials (leaf reads): widen the exists relation
        // with each demanded association's LEFT join, prefix-renamed —
        // the same descriptor->emission fold the root pipeline uses
        Map<String, Substitution.AssocSub> nestedAssocs = new LinkedHashMap<>();
        Map<String, AssociationJoins.AssocJoin> nestedByHead =
                new LinkedHashMap<>();
        for (var pe : preJoins.entrySet()) {
            AssociationJoins.AssocJoin pj = pe.getValue();
            nestedAssocs.put(pe.getKey(), new Substitution.AssocSub(pj.prefix(),
                    pj.target().rowVar(), pj.target().bindings(),
                    pj.target().classFqn(),
                    Pipelines.slotAliases(pj.target().pipeline()),
                    pj.targetSlotPrefixes(),
                    /*readVar*/ null, /*readRowType*/ null,
                    Map.of(), pj.targetSubNavs()));
            nestedByHead.put(pe.getKey(), pj);
        }
        TypedSpec pipe = targetPipe;
        var tNavSteps = Pipelines.navSteps(t.pipeline());
        for (List<String> path : innerPaths) {
            String h = path.get(0);
            // an exists material and an assoc material COEXIST for one
            // head: emptiness consumption reads existsSubs, leaf reads
            // read assocs — different arms of rewritePath/rewriteCallArms
            if (nestedAssocs.containsKey(h)) {
                continue;
            }
            // association heads AND nav-slot-backed heads both resolve
            // through associationJoin (its binding!=null arm is the
            // navigate-slot route — $e.address.name where address is a
            // Join-PM property)
            TypedSpec hb = t.bindings().get(SyntheticHeads.realHead(h));
            boolean slotBacked = hb != null
                    && InnerDemand.navSlotAlias(hb, t.rowVar(), tNavSteps.keySet()) != null;
            // a slot STRIPPED inside an earlier hop's target (the scope's
            // source is a composed row carrying no step for it): the
            // class's own step splices onto the composed row, exactly as
            // the flatten route does for a hop (group F burn 2026-09-02)
            ClassSource th = t;
            if (hb != null && !slotBacked && ownStepSplicer != null) {
                ClassSource ws = ownStepSplicer.apply(t, SyntheticHeads.realHead(h));
                if (ws != null) {
                    th = ws;
                    slotBacked = true;
                }
            }
            if (!slotBacked && (hb != null
                    || !hasAssoc.test(t.classFqn(),
                            SyntheticHeads.realHead(h)))) {
                continue;
            }
            Set<String> hLeaves = new LinkedHashSet<>();
            for (List<String> p2 : innerPaths) {
                if (p2.size() >= 2 && p2.get(0).equals(h)) {
                    hLeaves.add(p2.get(1));
                }
            }
            Set<List<String>> hNavTails = new LinkedHashSet<>();
            for (List<String> p2 : innerFullPaths) {
                if (p2.size() >= 2 && p2.get(0).equals(h)) {
                    hLeaves.add(p2.get(1));
                    if (p2.size() >= 3) {
                        // the tail past the head is the TARGET-side nav
                        // path (placeOfInterest.name on Location) —
                        // aggJoinMaterial's predPaths mechanism demands
                        // the nav slot and builds the SubNav
                        hNavTails.add(p2.subList(1, p2.size()));
                    }
                }
            }
            // aggJoinMaterial is the nav-slot-aware entry (binding-backed
            // heads route through the navigate slot; associations fall
            // through to the assoc route)
            AssociationJoins.AssocJoin aj2 = assocMaterial.aggJoinMaterial(
                    temporal, th, h, context, hLeaves, hNavTails);
            List<Type.Column> cols = new ArrayList<>(
                    (Type.requireRelationSchema(pipe.info().type())).columns());
            for (Type.Column c : aj2.targetRow().columns()) {
                cols.add(new Type.Column(aj2.prefix() + c.name(),
                        c.type(), c.multiplicity()));
            }
            Type.RelationType widened = new Type.RelationType(cols);
            pipe = new TypedJoin(pipe, aj2.targetPipeline(), AssociationJoins.leftKind(),
                    java.util.Objects.requireNonNull(aj2.condition()), Optional.of(aj2.prefix()), null,
                    new ExprType(Type.relation(widened),
                            com.legend.compiler.element.type.Multiplicity.Bounded.ONE),
                false /* resolver-synth */);
            nestedAssocs.put(h, new Substitution.AssocSub(aj2.prefix(),
                    aj2.target().rowVar(), aj2.target().bindings(),
                    aj2.target().classFqn(),
                    Pipelines.slotAliases(aj2.target().pipeline()),
                    aj2.targetSlotPrefixes(),
                    /*readVar*/ null, /*readRowType*/ null,
                    Map.of(), aj2.targetSubNavs()));
            nestedByHead.put(h, aj2);
        }
        // deep paths whose mid hop is ITSELF an association (locations.
        // placeOfInterest.name) register CHAIN keys, mirroring the root
        // chain-walk (task #70/#78)
        pipe = foldNestedChains(temporal, context, t, pipe,
                innerFullPaths, nestedByHead, nestedAssocs);
        return new NestedMaterials(pipe, nestedAssocs);
    }

    /** Nested-scope CHAIN registration (task #70/#78 multi-hop exists:
     * $e.locations.placeOfInterest.name): deep inner paths walk hop-by-hop
     * exactly like the root chain-walk — each mid hop joins its target
     * with a COMPOSED prefix, the condition's left reads re-pointed onto
     * the accumulated prefixed row; the chain key (locations.placeOfInterest)
     * lands in {@code nestedAssocs} so rewriteMultiHop's chainKey arm
     * resolves the leaf. Returns the widened pipe. */
    TypedSpec foldNestedChains(TemporalFrame temporal,
            StoreResolver.Context context, ClassSource t, TypedSpec pipe,
            java.util.Set<List<String>> fullPaths,
            Map<String, AssociationJoins.AssocJoin> byHead,
            Map<String, Substitution.AssocSub> nestedAssocs) {
        Map<String, AssociationJoins.AssocJoin> byChain =
                new LinkedHashMap<>();
        for (List<String> p3 : fullPaths) {
            if (p3.size() < 3) {
                continue;
            }
            AssociationJoins.AssocJoin baseAj = byHead.get(p3.get(0));
            if (baseAj == null) {
                continue;
            }
            ClassSource parent = baseAj.target();
            String parentPrefix = baseAj.prefix();
            Type.RelationType parentRow = baseAj.targetRow();
            String chainKey = p3.get(0);
            for (int hop = 1; hop + 1 < p3.size(); hop++) {
                String seg = p3.get(hop);
                chainKey = chainKey + "." + seg;
                AssociationJoins.AssocJoin known = byChain.get(chainKey);
                if (known != null) {
                    parent = known.target();
                    parentPrefix = known.prefix();
                    parentRow = known.targetRow();
                    continue;
                }
                // W4 ALREADY COMPOSED this hop inside the head's target
                // pipeline (its materialized row carries '<seg>_*') —
                // RE-POINT instead of a second join (the chain-pair
                // re-root rule; a second join collides: 'duplicate column
                // addresses_location_ID'). The dispatch AssocSub keys the
                // composed prefix over the SUB class's own bindings.
                final String segPfx = seg + "_";
                if (parentRow != null && parentRow.columns().stream()
                        .anyMatch(c -> c.name().startsWith(segPfx))) {
                    ClassSource sub = navSubSource(parent, seg);
                    if (sub != null) {
                        String composed = parentPrefix + segPfx;
                        nestedAssocs.put(chainKey, new Substitution.AssocSub(
                                composed, sub.rowVar(), sub.bindings(),
                                sub.classFqn(),
                                Pipelines.slotAliases(sub.pipeline())));
                        byChain.put(chainKey, new AssociationJoins.AssocJoin(
                                composed, sub, sub.pipeline(),
                                sub.rowType(), null, Map.of(), Map.of(),
                                null, null,
                                // mid-hop sub-join: row-preserving (deep
                                // VALUE chains = open cell, addendum §7)
                                false));
                        parent = sub;
                        parentPrefix = composed;
                        parentRow = null;   // deeper composition: fold route
                        continue;
                    }
                }
                AssociationJoins.AssocJoin aj3 = assocMaterial.aggJoinMaterial(
                        temporal, parent, seg, context,
                        java.util.Set.of(p3.get(hop + 1)), java.util.Set.of());
                String chainPrefix = AssociationJoins.chainedPrefix(
                        parentPrefix + seg, t, byChain);
                TypedLambda cond3 = aj3.condition();
                List<Type.Column> leftCols3 = new ArrayList<>();
                for (Type.Column c : parent.rowType().columns()) {
                    leftCols3.add(new Type.Column(parentPrefix + c.name(),
                            c.type(), c.multiplicity()));
                }
                Type.RelationType leftRow3 = new Type.RelationType(leftCols3);
                String lp3 = java.util.Objects.requireNonNull(cond3, "cond3").parameters().get(0);
                final String ppf = parentPrefix;
                TypedSpec body3 = Pipelines.prefixColumns(
                        cond3.body().get(cond3.body().size() - 1), lp3, ppf,
                        v -> new TypedVariable(lp3, new ExprType(leftRow3,
                                com.legend.compiler.element.type.Multiplicity
                                        .Bounded.ONE)));
                cond3 = new TypedLambda(cond3.parameters(), List.of(body3),
                        cond3.info());
                List<Type.Column> cols3 = new ArrayList<>(
                        (Type.requireRelationSchema(pipe.info().type())).columns());
                for (Type.Column c : aj3.targetRow().columns()) {
                    cols3.add(new Type.Column(chainPrefix + c.name(),
                            c.type(), c.multiplicity()));
                }
                pipe = new TypedJoin(pipe, aj3.targetPipeline(),
                        AssociationJoins.leftKind(), cond3,
                        Optional.of(chainPrefix), null,
                        new ExprType(Type.relation(new Type.RelationType(cols3)),
                                com.legend.compiler.element.type.Multiplicity
                                        .Bounded.ONE),
                false /* resolver-synth */);
                AssociationJoins.AssocJoin stored =
                        new AssociationJoins.AssocJoin(chainPrefix,
                                aj3.target(), aj3.targetPipeline(),
                                aj3.targetRow(), cond3,
                                aj3.targetSlotPrefixes(), Map.of(),
                                null, null,
                                // chained mid material: row-preserving
                                false);
                byChain.put(chainKey, stored);
                nestedAssocs.put(chainKey, new Substitution.AssocSub(
                        chainPrefix, aj3.target().rowVar(),
                        aj3.target().bindings(), aj3.target().classFqn(),
                        Pipelines.slotAliases(aj3.target().pipeline()),
                        aj3.targetSlotPrefixes(), null, null,
                        Map.of(), aj3.targetSubNavs()));
                parent = aj3.target();
                parentPrefix = chainPrefix;
                parentRow = aj3.targetRow();
            }
        }
        return pipe;
    }

    /** The SUB class source a navigate-slot property of {@code parent}
     * targets, or null when the property is not a nav-slot binding. */
    private @com.legend.Nullable ClassSource navSubSource(ClassSource parent, String seg) {
        TypedSpec b = parent.bindings().get(seg);
        var navSteps = Pipelines.navSteps(parent.pipeline());
        String alias = b == null ? null
                : InnerDemand.navSlotAlias(b, parent.rowVar(),
                        navSteps.keySet());
        if (alias == null) {
            return null;
        }
        return java.util.Objects.requireNonNull(navSteps.get(alias), "navSteps.get(alias)").target()
                instanceof com.legend.compiler.spec.typed.TypedGetAll g
                ? sources.get(parent.mappingFqn(), g.classFqn(), parent.scope()) : null;
    }

record CompositeChain(TypedSpec pipeline,
            TypedLambda orientedCond) {}


@com.legend.Nullable CompositeChain compositeChainTarget(ClassSource cs,
        TypedLambda navCond, TypedSpec targetPipe) {
        return compositeChainTarget(cs, navCond, targetPipe, false);
    }

    /** {@code allowUpstreamSlotReads} (batch 5, deep chains): hop-1's
     * condition may reference FURTHER sibling slots — those reads stay
     * parent-level slot reads in the ORIENTED condition (the engine's
     * testIsolationForFiltersWithoutAliasAndInnerJoins golden: each
     * occurrence's frame bundles ITS OWN mid, and joins onto the shared
     * upstream hop, which remains a parent join). The caller owns
     * keeping those upstream slots demanded. Existing (sub-level)
     * callers keep the loud guard. */
@com.legend.Nullable CompositeChain compositeChainTarget(ClassSource cs,
        TypedLambda navCond, TypedSpec targetPipe,
        boolean allowUpstreamSlotReads) {
        Set<String> parentSlots = Pipelines.slotAliases(cs.pipeline());
        if (parentSlots.isEmpty()) {
            return null;
        }
        String sParam = navCond.parameters().get(0);
        String tParam = navCond.parameters().get(1);
        boolean anySlot = false;
        for (String sl : parentSlots) {
            for (TypedSpec b : navCond.body()) {
                anySlot |= Pipelines.referencesAliasOn(b, sParam, Set.of(sl));
            }
        }
        if (!anySlot) {
            return null;
        }
        Type.RelationType tgtRow0 = Type.relationSchema(targetPipe.info().type());
        if (tgtRow0 == null) {
            return null;
        }
        // audit 23 #75: a multi-statement join condition would silently
        // drop its leading statements (let-bound sub-expressions) — loud
        if (navCond.body().size() != 1) {
            throw new NotImplementedException("composite join condition"
                    + " with " + navCond.body().size() + " statements"
                    + " (let-carrying bodies) is not supported yet");
        }
        // The routed-union emission builds the condition as an OR of
        // per-route terms; classify each disjunct — DIRECT parent reads
        // stay on the outer correlation, single-slot disjuncts pull their
        // slot table INTO the composite target (engine V4: subselect
        // contains both tables, correlated outward by hop-1's condition).
        List<TypedSpec> disjuncts = new ArrayList<>();
        TypedNativeCall[] orCallee = {null};
        flattenOrInto(navCond.body().get(0), disjuncts, orCallee);
        List<TypedSpec> direct = new ArrayList<>();
        Map<String, List<TypedSpec>> bySlot = new LinkedHashMap<>();
        for (TypedSpec d : disjuncts) {
            Set<String> slotsRead = new LinkedHashSet<>();
            for (String sl : parentSlots) {
                if (Pipelines.referencesAliasOn(d, sParam, Set.of(sl))) {
                    slotsRead.add(sl);
                }
            }
            if (slotsRead.isEmpty()) {
                direct.add(d);
                continue;
            }
            if (slotsRead.size() > 1) {
                throw new NotImplementedException(
                        "navigate-step condition disjunct reads MULTIPLE"
                                + " sibling joinslots (" + slotsRead
                                + ") — the multi-slot disjunct is not"
                                + " built yet");
            }
            String sl = slotsRead.iterator().next();
            if (readsVarOutsideSlot(d, sParam, sl)) {
                throw new NotImplementedException(
                        "navigate-step condition disjunct mixes"
                                + " sibling-slot reads with DIRECT parent"
                                + " reads — the mixed disjunct is not"
                                + " built yet");
            }
            bySlot.computeIfAbsent(sl, k -> new ArrayList<>()).add(d);
        }
        if (bySlot.isEmpty()) {
            return null;
        }
        var joinSlots = Pipelines.joinSlots(cs.pipeline());
        for (String sl : bySlot.keySet()) {
            var js = joinSlots.get(sl);
            if (js == null || !Type.isRelation(js.target().info().type())) {
                // NOT walled (audit 23 B6 probe): a sibling that is a
                // NAVIGATE step (not a joinSlot) degrades to the flat
                // form, which the chained-union V2 family pins as
                // row-correct — the explosion risk is multiplicity-
                // dependent, and the blanket wall over-fired on those
                // passing shapes.
                return null;
            }
        }
        var one = com.legend.compiler.element.type.Multiplicity.Bounded.ONE;
        Set<String> takenLr = new LinkedHashSet<>();
        collectVarNamesInto(navCond.body().get(0), takenLr);
        TypedSpec composite = targetPipe;
        Type.RelationType compRow = tgtRow0;
        Map<String, String> slotPfx = new LinkedHashMap<>();
        int lrOrd = 2;
        for (var en : bySlot.entrySet()) {
            String slotRef = en.getKey();
            var js = java.util.Objects.requireNonNull(
                    joinSlots.get(slotRef));
            // FRAMED VIEW slot target (Leg 4): the frame carries its own
            // internal slots — materialize it in its OWN scope before it
            // joins the composite (walkJoinSlot's frame rule; the frame's
            // project/distinct terminal keeps its declared row)
            TypedSpec slotTarget = js.target();
            if (Pipelines.containsSlot(slotTarget)) {
                slotTarget = Pipelines.materialize(slotTarget,
                        Set.of(), cs.classFqn()).pipeline();
            }
            Type.RelationType optRow =
                    Type.requireRelationSchema(slotTarget.info().type());
            TypedLambda c1 = js.condition();
            // GUARD (loud, never silent): hop-1's own condition must not
            // read further slots.
            if (!allowUpstreamSlotReads) {
                for (TypedSpec b : c1.body()) {
                    for (String sl : parentSlots) {
                        if (Pipelines.referencesAliasOn(b,
                                c1.parameters().get(0), Set.of(sl))) {
                            throw new NotImplementedException(
                                    "chained joinslot condition reads a"
                                            + " further sibling slot — deep"
                                            + " composite chains are not"
                                            + " built here");
                        }
                    }
                }
            }
            String pfx = slotRef + "_";
            boolean clash = true;
            while (clash) {
                clash = false;
                for (Type.Column c : compRow.columns()) {
                    if (c.name().startsWith(pfx)) {
                        pfx = "_" + pfx;
                        clash = true;
                    }
                }
            }
            slotPfx.put(slotRef, pfx);
            List<Type.Column> compCols = new ArrayList<>(compRow.columns());
            for (Type.Column c : optRow.columns()) {
                compCols.add(new Type.Column(pfx + c.name(), c.type(),
                        c.multiplicity()));
            }
            Type.RelationType newRow = new Type.RelationType(compCols);
            String lv = "_cl";
            String rv = "_cr";
            while (takenLr.contains(lv) || takenLr.contains(rv)) {
                lv = "_cl" + lrOrd;
                rv = "_cr" + lrOrd;
                lrOrd++;
            }
            takenLr.add(lv);
            takenLr.add(rv);
            // inner cond: THIS slot's disjuncts — target reads land on the
            // current composite row, slot reads on the slot table's row
            TypedSpec inner = orJoin(en.getValue(), orCallee[0]);
            final String lvF = lv;
            final String rvF = rv;
            final Type.RelationType lRow = compRow;
            TypedSpec b1 = Pipelines.rewriteRowReads(inner, tParam, Map.of(),
                    Set.of(), v -> new TypedVariable(lvF,
                            new ExprType(lRow, one)));
            TypedSpec b2 = Pipelines.rewriteRowReads(b1, sParam,
                    Map.of(slotRef, ""), Set.of(),
                    v -> new TypedVariable(rvF, new ExprType(optRow, one)));
            TypedLambda joinCond = new TypedLambda(List.of(lvF, rvF),
                    List.of(b2),
                    new ExprType(new Type.FunctionType(
                            List.of(new Type.Param(lRow, one),
                                    new Type.Param(optRow, one)),
                            new Type.Param(Type.Primitive.BOOLEAN, one)), one));
            composite = new TypedJoin(composite, slotTarget,
                    AssociationJoins.leftKind(), joinCond, Optional.of(pfx), null,
                    new ExprType(Type.relation(newRow), one),
                false /* resolver-synth */);
            compRow = newRow;
        }
        final Type.RelationType finalRow = compRow;
        // oriented outer condition: direct disjuncts keep their parent
        // reads; each slot contributes hop-1's condition with the slot-
        // table reads landing on ITS prefixed composite columns (NULL off
        // the unmatched side keeps the OR exact).
        List<TypedSpec> orientedTerms = new ArrayList<>();
        for (TypedSpec d : direct) {
            orientedTerms.add(Pipelines.rewriteRowReads(d, tParam, Map.of(),
                    Set.of(), v -> new TypedVariable(tParam,
                            new ExprType(finalRow, one))));
        }
        for (var en : bySlot.entrySet()) {
            var js = java.util.Objects.requireNonNull(
                    joinSlots.get(en.getKey()));
            TypedLambda c1 = js.condition();
            String pfx = slotPfx.get(en.getKey());
            String c1t = c1.parameters().get(1);
            TypedSpec oc = Pipelines.prefixColumns(
                    c1.body().get(c1.body().size() - 1), c1t, java.util.Objects.requireNonNull(pfx),
                    v -> new TypedVariable(tParam,
                            new ExprType(finalRow, one)));
            String c1s = c1.parameters().get(0);
            if (!c1s.equals(sParam)) {
                oc = Pipelines.rewriteRowReads(oc, c1s, Map.of(), Set.of(),
                        v -> new TypedVariable(sParam,
                                new ExprType(cs.rowType(), one)));
            }
            orientedTerms.add(oc);
        }
        TypedSpec orientedBody = orJoin(orientedTerms, orCallee[0]);
        TypedLambda oriented = new TypedLambda(List.of(sParam, tParam),
                List.of(orientedBody),
                new ExprType(new Type.FunctionType(
                        List.of(new Type.Param(cs.rowType(), one),
                                new Type.Param(finalRow, one)),
                        new Type.Param(Type.Primitive.BOOLEAN, one)), one));
        return new CompositeChain(composite, oriented);
    }

    /** Flatten a {@code boolean::or} tree into its disjuncts, remembering
     * one or-node to rebuild with (exact FQN — never a name suffix). */
    private static void flattenOrInto(TypedSpec n, List<TypedSpec> out,
            TypedNativeCall[] orCallee) {
        if (n instanceof TypedNativeCall c && c.args().size() == 2
                && "meta::pure::functions::boolean::or"
                        .equals(c.callee().qualifiedName())) {
            orCallee[0] = c;
            flattenOrInto(c.args().get(0), out, orCallee);
            flattenOrInto(c.args().get(1), out, orCallee);
            return;
        }
        out.add(n);
    }

    /** OR-join terms with the captured or-node's callee; a single term
     * passes through untouched. */
    private static TypedSpec orJoin(List<TypedSpec> terms,
            TypedNativeCall orCallee) {
        if (terms.size() == 1) {
            return terms.get(0);
        }
        if (orCallee == null) {
            throw new IllegalStateException("resolver bug: multi-term"
                    + " oriented condition without an or-node to rebuild"
                    + " from");
        }
        TypedSpec acc = terms.get(0);
        for (int i = 1; i < terms.size(); i++) {
            acc = orCallee.withChildren(List.of(acc, terms.get(i)));
        }
        return acc;
    }


private static boolean readsVarOutsideSlot(TypedSpec n, String var,
            String slot) {
        if (n instanceof TypedPropertyAccess outer
                && outer.source() instanceof TypedPropertyAccess inner
                && inner.source() instanceof TypedVariable v
                && v.name().equals(var)
                && inner.property().equals(slot)) {
            return false;   // the sanctioned two-level slot read
        }
        if (n instanceof TypedVariable v && v.name().equals(var)) {
            return true;
        }
        if (n instanceof TypedLambda l && l.parameters().contains(var)) {
            return false;
        }
        for (TypedSpec c : n.children()) {
            if (readsVarOutsideSlot(c, var, slot)) {
                return true;
            }
        }
        return false;
    }


    /** T1.7: the hand-kept AGG_FQNS name list (missing stdDev, variance,
     * mode, corr, ... — 15 gaps) is DELETED; membership is the reducer
     * catalog itself ({@link com.legend.lowering.Aggregates#isReducer}). */
    static boolean isAggregate(TypedFunction callee) {
        return com.legend.lowering.Aggregates.isDemandReducer(callee);
    }

    /** Node-level gate (§4AD decision 1): infix plus (n-ary args) is
     * row-wise over navigations — see Aggregates.isDemandReducer. */
    static boolean isAggregate(TypedNativeCall nc) {
        return com.legend.lowering.Aggregates.isDemandReducer(
                nc.callee(), nc.args().size());
    }


static boolean isCountFamily(TypedNativeCall nc) {
        String q = nc.callee().qualifiedName();
        return q.equals("meta::pure::functions::collection::count")
                || q.equals("meta::pure::functions::collection::size");
    }


static void collectParamColumnReads(TypedLambda cond, Set<String> out) {
        String src = cond.parameters().get(0);
        for (TypedSpec b : cond.body()) {
            collectVarColumnReads(b, src, out);
        }
    }


static TypedSpec predFilteredPipe(TypedSpec tPipe, ClassSource target,
            Map<String, String> slotPrefixes, TypedLambda pred,
            String mappingFqn) {
        return predFilteredPipe(tPipe, target, slotPrefixes, Map.of(),
                pred, mappingFqn);
    }


static TypedSpec predFilteredPipe(TypedSpec tPipe, ClassSource target,
            Map<String, String> slotPrefixes,
            Map<String, Substitution.SubNav> subNavs, TypedLambda pred,
            String mappingFqn) {
        Set<String> unconverted = new LinkedHashSet<>(
                Pipelines.slotAliases(target.pipeline()));
        unconverted.removeAll(slotPrefixes.keySet());
        Type.RelationType rowT = Type.requireRelationSchema(tPipe.info().type());
        Map<String, Substitution.AssocSub> navAssocs = new LinkedHashMap<>();
        for (var e : subNavs.entrySet()) {
            var sn = e.getValue();
            navAssocs.put(e.getKey(), new Substitution.AssocSub(
                    sn.prefix(), sn.rowVar(), sn.bindings(),
                    target.classFqn() + "." + e.getKey(),
                    Set.of(), Map.of(), target.rowVar(), rowT,
                    Map.of(), sn.children()));
        }
        // the predicate's run-time type tests ($n->instanceOf(Sub),
        // ->cast(@Sub)) dispatch through the TARGET ROW's subtype
        // columns (its member witnesses) — registered from the row alone
        // (the plan-node hierarchy: executionNodes->filter(n|$n->instanceOf(
        // RelationalInstantiationExecutionNode)))
        registerSubTypeSubs(target, pred, null, navAssocs);
        Substitution predSub = new Substitution(new Substitution.Target(
                new Substitution.RowScope(pred.parameters().get(0),
                        target.rowVar(), target.classFqn(), mappingFqn,
                        target.rowVar(), target.bindings(), rowT,
                        unconverted, slotPrefixes, Map.of()),
                navAssocs.isEmpty() ? Substitution.Registries.NONE
                        : new Substitution.Registries(navAssocs, Set.of(),
                                Map.of(), Map.of(), null, null),
                Substitution.TemporalView.NONE,
                true, true));
        return new TypedFilter(tPipe, predSub.rewriteLambda(pred),
                tPipe.info());
    }


static void scanLambda(TypedLambda lambda, Set<List<String>> out) {
        for (TypedSpec b : lambda.body()) {
            FlattenOps.consumedPaths(b, lambda.parameters().get(0), out);
        }
    }

    /**
     * subType(@Sub) reads over the instance variable: register each
     * demanded SUBTYPE's binding table under the subtype key so
     * property reads through the cast dispatch to the SUB class's
     * bindings, renamed onto the parent row (engine same-source
     * inheritance: the cast never joins — non-members read the sub's
     * columns as NULL naturally). Single-source only: a sub whose
     * bindings read columns outside the parent row (its own table,
     * or its own join slots via the AssocSub slot wall) stays loud.
     */
    /** The row's subtype-column reads of {@code fqn} (prop -> read),
     * under {@code hopPrefix} ({@code ""} = anywhere on the row). */
    private static Map<String, TypedSpec> subtypeBindings(ClassSource cs,
            String fqn, String stPrefix, String hopPrefix, boolean strict) {
        Map<String, TypedSpec> stBindings = new LinkedHashMap<>();
        for (Type.Column c : cs.rowType().columns()) {
            if (!hopPrefix.isEmpty()
                    && !c.name().startsWith(hopPrefix + stPrefix)) {
                continue;
            }
            // ANCHORED: bare marker or marker right after a slot-prefix
            // boundary ("alias_") — indexOf-anywhere let the marker match
            // mid-name (text-surgery audit §1.1 #5)
            int at = c.name().indexOf(stPrefix);
            if (at < 0 || (at > 0 && c.name().charAt(at - 1) != '_')) {
                continue;
            }
            String prop = c.name().substring(at + stPrefix.length());
            TypedSpec prior = stBindings.put(prop,
                    new TypedPropertyAccess(
                            new TypedVariable(cs.rowVar(),
                                    ExprType.one(cs.rowType())),
                            c.name(),
                            new ExprType(c.type(), c.multiplicity())));
            if (prior != null) {
                if (!strict) {
                    return Map.of();
                }
                // two hops carry the same subtype column: silently keeping
                // the LAST bound the cast leaf to the wrong hop — refuse
                // until per-hop disambiguation is designed
                throw new com.legend.error.NotImplementedException(
                        "subtype column '" + prop + "' of " + fqn
                                + " rides more than one hop prefix");
            }
        }
        return stBindings;
    }

    static void registerSubTypeSubs(ClassSource cs, TypedSpec top,
            @com.legend.Nullable ClassSources sources,
            Map<String, Substitution.AssocSub> assocs) {
        registerSubTypeSubs(cs, top, sources, assocs, "");
    }

    /** {@code hopPrefix}: a COMPOSED row (a flattened hop's scope) carries
     * the subtype columns of every hop it composed — only the columns
     * under this hop's prefix are this scope's ({@code ""} = any). */
    static void registerSubTypeSubs(ClassSource cs, TypedSpec top,
            @com.legend.Nullable ClassSources sources,
            Map<String, Substitution.AssocSub> assocs, String hopPrefix) {
        Set<String> fqns = new LinkedHashSet<>();
        collectSubTypeFqns(top, fqns);
        for (String fqn : fqns) {
            if (fqn.equals(cs.classFqn())
                    || assocs.containsKey(Substitution.SUBTYPE_KEY + fqn)) {
                continue;
            }
            // UNION/INHERITANCE parent: the synthesis carries each member
            // subclass's mapped properties as class-qualified thread-local
            // columns (NULL in other threads) — the cast's binding table
            // is those column reads off the union row
            // the contract marker may ride under a materialization slot
            // prefix (auto-map hop rows carry the union's columns
            // hop-prefixed) — match the marker anywhere, read the FULL
            // column name
            String stPrefix = com.legend.model.ClassMapping.subTypeColumnPrefix(fqn);
            // the hop's own prefix first (a composed row of a member-union
            // chain carries every hop's witnesses); a row carrying the
            // subtype only under some other slot prefix (an auto-map hop
            // row) keeps the anywhere-scan
            Map<String, TypedSpec> stBindings =
                    subtypeBindings(cs, fqn, stPrefix, hopPrefix, true);
            if (stBindings.isEmpty() && !hopPrefix.isEmpty()) {
                // lenient: a subtype riding SEVERAL other hops' prefixes is
                // another scope's dispatch (its own registration served it
                // below) — no table here, never a guess
                stBindings = subtypeBindings(cs, fqn, stPrefix, "", false);
            }
            if (!stBindings.isEmpty()) {
                assocs.put(Substitution.SUBTYPE_KEY + fqn,
                        new Substitution.AssocSub("", cs.rowVar(),
                                stBindings, fqn, Set.of()));
                continue;
            }
            // without the sources registry (a predicate target over a
            // navigated collection) only the row's own subtype columns
            // can register
            if (sources == null || !sources.binds(cs.mappingFqn(), fqn)) {
                continue;
            }
            ClassSource sub = sources.get(cs.mappingFqn(), fqn, cs.scope());
            Set<String> cols = new LinkedHashSet<>();
            for (TypedSpec b : sub.bindings().values()) {
                collectVarColumnReads(b, sub.rowVar(), cols);
            }
            Set<String> parentCols = new LinkedHashSet<>();
            for (Type.Column c : cs.rowType().columns()) {
                parentCols.add(c.name().toLowerCase());
            }
            // a sub reading columns outside the parent row is not servable
            // same-source — SKIP (not throw): the cast may sit in a NAV
            // position served by the path funnel's subtype-leaf
            // canonicalization; a truly unservable read goes loud at the
            // read site
            boolean sameSource = true;
            for (String col : cols) {
                if (!parentCols.contains(col.toLowerCase())) {
                    sameSource = false;
                    break;
                }
            }
            if (!sameSource) {
                continue;
            }
            assocs.put(Substitution.SUBTYPE_KEY + fqn,
                    new Substitution.AssocSub("", sub.rowVar(),
                            sub.bindings(), fqn,
                            Pipelines.slotAliases(sub.pipeline())));
        }
    }

    /** Class targets of subType calls over a variable, anywhere in the
     * chain — plus the RUN-TIME BRANCH CHOICE shapes over a variable
     * (match arms, instanceOf targets, cast targets), which the
     * substitution serves off the same subtype binding tables
     * (Substitution.discriminatedMatch / instanceOfHead / castLeafRead).
     * Over-collection is harmless: registration only lands where the row
     * carries the subtype's columns. */
    private static void collectSubTypeFqns(TypedSpec n, Set<String> out) {
        if (n instanceof TypedNativeCall nc
                && nc.callee().qualifiedName()
                        .equals("meta::pure::functions::lang::subType")
                && !nc.args().isEmpty()
                && nc.args().get(0) instanceof TypedVariable
                && nc.info().type() instanceof Type.ClassType ct) {
            out.add(ct.fqn());
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedMatchRuntime mr
                && mr.input() instanceof TypedVariable) {
            for (var arm : mr.arms()) {
                out.add(arm.typeFqn());
            }
        }
        if (n instanceof TypedNativeCall io
                && io.callee().qualifiedName()
                        .equals(Substitution.INSTANCE_OF_FQN)
                && io.args().size() == 2
                && io.args().get(0) instanceof TypedVariable) {
            String t = Substitution.typeTargetFqn(io.args().get(1));
            if (t != null) {
                out.add(t);
            }
        }
        // a cast over the instance variable, OR in CHAIN position over
        // the class chain itself (the chain's cast gate reads the same
        // subtype table — harness burn-down leg 1)
        if (n instanceof com.legend.compiler.spec.typed.TypedCast tc
                && (tc.source() instanceof TypedVariable
                        || tc.source().info().type() instanceof Type.ClassType)
                && tc.target() instanceof Type.ClassType cct) {
            out.add(cct.fqn());
        }
        for (TypedSpec c : n.children()) {
            collectSubTypeFqns(c, out);
        }
    }

    /**
     * subType(@C).prop over a TO-MANY navigation where C has PARTIAL
     * membership (the nav target carries C's witness pseudo-binding): the
     * engine ROUTES the navigation to conforming member sets only — the
     * cast canonicalizes to the filtered-nav shape (filter by witness
     * isNotEmpty, leaf = stc column) so the park machinery mints a
     * per-cast join identity. Total-membership casts return {@code n}
     * unchanged (row-neutral — the plain stc-leaf path serves them).
     * Node-local: runs as the liftFilteredHeads canonicalizer hook.
     */
    TypedSpec subTypeNavCastCanon(TypedSpec n,
            Function<String, String> mappingOf, TypedFunction isNotEmpty) {
        // FLATTENED-EMBEDDED leaf (ledger cluster 45): union synthesis
        // publishes an embedded subtype property ONLY as flat per-leaf
        // columns (stc_..._prop__leaf, addStcEmbeddedLeaf) — no plain
        // stc_..._prop exists by construction. The canonicalizer runs
        // top-down, so (subType($v,@Sub).prop).leaf is visited before its
        // child: fold the trailing hop into the flat column name. Both
        // guards are load-bearing — !plain keeps the existing route for
        // genuinely class-typed stc navigations; flat fires only where
        // union synthesis flattened.
        if (n instanceof TypedPropertyAccess outer
                && outer.source() instanceof TypedPropertyAccess mid
                && mid.source() instanceof TypedNativeCall msc
                && msc.callee().qualifiedName()
                        .equals("meta::pure::functions::lang::subType")
                && !msc.args().isEmpty()
                && msc.info().type() instanceof Type.ClassType msct
                && msc.args().get(0).info().type()
                        instanceof Type.ClassType mnavCt) {
            ClassSource mt = castTarget(mappingOf, mnavCt);
            String mwKey = com.legend.model.ClassMapping.subTypeColumn(
                    msct.fqn(), com.legend.model.ClassMapping.memberWitness());
            String plain = com.legend.model.ClassMapping.subTypeColumn(
                    msct.fqn(), mid.property());
            String flat = com.legend.model.ClassMapping.subTypeColumn(
                    msct.fqn(), mid.property() + "__" + outer.property());
            if (mt != null && mt.bindings().containsKey(mwKey)
                    && !mt.bindings().containsKey(plain)
                    && mt.bindings().containsKey(flat)) {
                TypedSpec mnav = msc.args().get(0);
                return new TypedPropertyAccess(
                        new TypedFilter(mnav,
                                witnessPred(mnavCt, mwKey, isNotEmpty),
                                mnav.info()),
                        flat, outer.info());
            }
        }
        // EMPTINESS over the bare cast (exists(nav->subType(@Car), pred)):
        // same routing rule, no leaf — the cast canonicalizes to the
        // filtered-nav head and the PREDICATE's depth-1 subtype reads
        // rename to their stc columns (the union frame's flat spellings)
        if (n instanceof TypedNativeCall em && !em.args().isEmpty()
                && (com.legend.builtin.Pure.nativeNamed("exists",
                                em.callee().signatureKey())
                        || com.legend.builtin.Pure.nativeNamed("isEmpty",
                                em.callee().signatureKey())
                        || com.legend.builtin.Pure.nativeNamed("isNotEmpty",
                                em.callee().signatureKey()))
                && em.args().get(0) instanceof TypedNativeCall sc0
                && sc0.callee().qualifiedName()
                        .equals("meta::pure::functions::lang::subType")
                && !sc0.args().isEmpty()
                && sc0.info().type() instanceof Type.ClassType sct0
                && sc0.args().get(0).info().type()
                        instanceof Type.ClassType navCt0) {
            ClassSource t0 = castTarget(mappingOf, navCt0);
            String wKey0 = com.legend.model.ClassMapping.subTypeColumn(
                    sct0.fqn(), com.legend.model.ClassMapping.memberWitness());
            if (t0 != null && t0.bindings().containsKey(wKey0)) {
                TypedSpec nav0 = sc0.args().get(0);
                List<TypedSpec> newArgs = new ArrayList<>(em.args());
                newArgs.set(0, new TypedFilter(nav0,
                        witnessPred(navCt0, wKey0, isNotEmpty), nav0.info()));
                if (em.args().size() == 2
                        && em.args().get(1) instanceof TypedLambda pl0
                        && pl0.parameters().size() == 1) {
                    newArgs.set(1, renameSubTypeReads(pl0, sct0.fqn(), t0));
                }
                return em.withChildren(newArgs);
            }
        }
        if (!(n instanceof TypedPropertyAccess pa)
                || !(pa.source() instanceof TypedNativeCall sc)
                || !sc.callee().qualifiedName()
                        .equals("meta::pure::functions::lang::subType")
                || sc.args().isEmpty()
                || !(sc.info().type() instanceof Type.ClassType sct)
                || !(sc.args().get(0).info().type()
                        instanceof Type.ClassType navCt)) {
            // NOTE multiplicity is NOT gated: a TO-ONE cast over a
            // partial-membership target needs the same per-cast routed
            // join (MilestonedInheritanceMapping golden: vehicle[1] casts
            // to Car and Bicycle read SEPARATE per-pair joins, one output
            // row per owner); total-membership targets have no witness
            // binding and fall through below regardless.
            return n;
        }
        // an EMBEDDED (ctor-valued, same-row) head has no join to route —
        // the cast reads through the ctor drill / SUBTYPE_KEY machinery
        // (inline-embedded golden: $x.vehicleOwner->subType(@Person).name)
        if (sc.args().get(0) instanceof TypedPropertyAccess ha
                && ha.source().info().type() instanceof Type.ClassType ownCt) {
            String om;
            try {
                om = mappingOf.apply(ownCt.fqn());
            } catch (MappingResolutionException e) {
                om = null;
            }
            if (om != null && sources.binds(om, ownCt.fqn())
                    && Pipelines.unwrapToOne(sources.get(om, ownCt.fqn(), null /* binding-shape probe */)
                            .bindings().getOrDefault(ha.property(), ha))
                            instanceof com.legend.compiler.spec.typed
                                    .TypedNewInstance) {
                return n;
            }
        }
        String wKey = com.legend.model.ClassMapping.subTypeColumn(sct.fqn(),
                com.legend.model.ClassMapping.memberWitness());
        ClassSource target = castTarget(mappingOf, navCt);
        if (target == null || !target.bindings().containsKey(wKey)) {
            return n;
        }
        TypedSpec nav = sc.args().get(0);
        return new TypedPropertyAccess(
                new TypedFilter(nav, witnessPred(navCt, wKey, isNotEmpty),
                        nav.info()),
                com.legend.model.ClassMapping.subTypeColumn(sct.fqn(),
                        pa.property()),
                pa.info());
    }

    /** The cast head's dispatch-resolved ClassSource — audit 23 A5: only
     * an UNDECIDABLE dispatch context (no runtime, unknown mapping) skips
     * canonicalization (null); a resolution failure AFTER binds()
     * confirmed the class is a real bug and propagates. */
    private @com.legend.Nullable ClassSource castTarget(
            Function<String, String> mappingOf, Type.ClassType navCt) {
        String m;
        try {
            m = mappingOf.apply(navCt.fqn());
        } catch (MappingResolutionException e) {
            m = null;
        }
        return m != null && sources.binds(m, navCt.fqn())
                ? sources.get(m, navCt.fqn(), null /* binding-shape probe */) : null;
    }

    /** {@code v | $v.<witness>->isNotEmpty()} — the member-routing filter. */
    private TypedLambda witnessPred(Type.ClassType navCt, String wKey,
            TypedFunction isNotEmpty) {
        var bool1 = new ExprType(Type.Primitive.BOOLEAN,
                com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
        TypedSpec wRead = new TypedPropertyAccess(
                // 'v_stw' is safe UNGUARDED: this lambda body is fully
                // SYNTHESIZED here (witness read + isNotEmpty only — no
                // user expression is embedded under the binder), and the
                // downstream park machinery alpha-canonicalizes binders
                new TypedVariable("v_stw", new ExprType(navCt,
                        com.legend.compiler.element.type
                                .Multiplicity.Bounded.ONE)),
                wKey, new ExprType(Type.Primitive.BOOLEAN,
                        com.legend.compiler.element.type
                                .Multiplicity.Bounded.ZERO_ONE));
        return new TypedLambda(List.of("v_stw"),
                List.of(new TypedNativeCall(isNotEmpty, List.of(wRead), bool1)),
                new ExprType(new Type.FunctionType(
                        List.of(new Type.Param(navCt,
                                com.legend.compiler.element.type
                                        .Multiplicity.Bounded.ONE)),
                        new Type.Param(Type.Primitive.BOOLEAN,
                                com.legend.compiler.element.type
                                        .Multiplicity.Bounded.ONE)),
                        com.legend.compiler.element.type
                                .Multiplicity.Bounded.ONE));
    }

    /** The predicate's DEPTH-1 subtype reads ({@code $c.engineType})
     * renamed to their stc-qualified union-frame spellings when the cast
     * target binds them; shared properties keep their plain reads. */
    private TypedLambda renameSubTypeReads(TypedLambda pl, String subFqn,
            ClassSource target) {
        String p0 = pl.parameters().get(0);
        List<TypedSpec> body = pl.body().stream()
                .map(b -> renameSubTypeReads(b, p0, subFqn, target)).toList();
        return new TypedLambda(pl.parameters(), body, pl.info());
    }

    private TypedSpec renameSubTypeReads(TypedSpec n, String p0,
            String subFqn, ClassSource target) {
        if (n instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable v
                && v.name().equals(p0)) {
            String stc = com.legend.model.ClassMapping.subTypeColumn(
                    subFqn, pa.property());
            if (target.bindings().containsKey(stc)) {
                return new TypedPropertyAccess(pa.source(), stc, pa.info());
            }
            return n;
        }
        if (n instanceof TypedLambda sh && sh.parameters().contains(p0)) {
            return n;   // shadowing binder stops the rename
        }
        return SyntheticHeads.rebuildChildren(n,
                c -> renameSubTypeReads(c, p0, subFqn, target));
    }

    /** ORDERING CONTRACT (audit 15 B3): aggReads is IDENTITY-keyed on the
     * scanned nodes — this scan must run AFTER every identity-changing
     * rewrite (splitDatedHeads etc.) and its keys are consumed in the SAME
     * resolveObject pass. A rewrite inserted between scan and substitution
     * dangles the keys silently. */
    /** FILTER-POSITION aggregates join the same demand registry the
     * terminal lambdas feed (memberScan skips them; unregistered shapes
     * die loud at the Substitution backstop). BARE paths are DISCARDED:
     * in filter position a bare to-many navigation is memberScan's
     * (implicit EXISTS), and registering it as a projection-path join
     * demand splats rows (testContainsOnToManyProperty 1->6). */
    static void aggScanFilters(java.util.List<TypedSpec> ops, ClassSource cs,
            Map<String, List<StoreResolver.AggDemand>> aggOut,
            java.util.function.BiPredicate<ClassSource, String> toManyHead,
            java.util.function.BiPredicate<ClassSource, String> bareHead) {
        Set<List<String>> discardedBare = new LinkedHashSet<>();
        Map<String, List<StoreResolver.AggDemand>> local =
                new LinkedHashMap<>();
        for (TypedSpec op : ops) {
            if (op instanceof com.legend.compiler.spec.typed.TypedFilter ff) {
                for (TypedSpec b : ff.predicate().body()) {
                    aggScan(b, ff.predicate().parameters().get(0), cs,
                            local, discardedBare, toManyHead, bareHead);
                }
            }
        }
        if (!local.isEmpty()) {
            com.legend.lowering.NavArmCensus.fire("agg-filter-position");
        }
        // demands re-stamped filterPosition=true: the emission takes the
        // PARENT-COPY grouped subselect (engine BuildCorrelatedSubQuery
        // copies the root tree into the isolation subquery, so duplicate
        // root rows double the aggregated collection — validation
        // constraint8 golden pins the difference)
        for (var e : local.entrySet()) {
            for (StoreResolver.AggDemand d : e.getValue()) {
                aggOut.computeIfAbsent(e.getKey(), k -> new ArrayList<>())
                        .add(new StoreResolver.AggDemand(d.node(), d.leaf(),
                                d.mapper(), d.orderKey(), d.orderAsc(),
                                true));
            }
        }
    }

    static void aggScan(TypedSpec n, String userVar, ClassSource cs,
                         Map<String, List<StoreResolver.AggDemand>> aggOut,
                         Set<List<String>> bareOut,
                         java.util.function.BiPredicate<ClassSource, String> toManyHead,
                         java.util.function.BiPredicate<ClassSource, String> bareHead) {
        if (n instanceof TypedNativeCall nc
                && !nc.args().isEmpty()
                && isAggregate(nc)) {
            List<String> path =
                    Substitution.pathOf(nc.args().get(0), userVar);
            // AGG(PA(leaf, sortBy(<nav>, key))) — ORDERED aggregation:
            // sortBy between the (lifted) navigation and the reducer is
            // ORDER metadata; the demand re-routes exactly as the
            // unordered spelling, the key rides into the grouped
            // subselect's reducer (string_agg(x, sep ORDER BY k)).
            if (nc.args().get(0) instanceof TypedPropertyAccess spa
                    && spa.source() instanceof TypedSortBy ssb) {
                List<String> sp = Substitution.pathOf(ssb.source(), userVar);
                if (sp != null && sp.size() == 1
                        && toManyHead.test(cs, sp.get(0))) {
                    com.legend.lowering.NavArmCensus.fire("agg-sortby-arm");
                    aggOut.computeIfAbsent(sp.get(0), k -> new ArrayList<>())
                            .add(new StoreResolver.AggDemand(nc, spa.property(), null,
                                    ssb.key(), ssb.ascending()));
                    for (int i = 1; i < nc.args().size(); i++) {
                        aggScan(nc.args().get(i), userVar, cs, aggOut, bareOut, toManyHead, bareHead);
                    }
                    return;
                }
            }
            // AGG(map(<nav>, λe.<scalar body>)) — the qualifier-inlined
            // COMPUTED-mapper spelling (#69): the mapper body aggregates
            // inside the grouped subselect, substituted through the
            // target's bindings at the fold. A sortBy on the map SOURCE
            // is the same ORDER metadata as above.
            if (nc.args().get(0) instanceof TypedMap tmap
                    && tmap.mapper().parameters().size() == 1
                    && !(tmap.mapper().functionType().result().type()
                            instanceof Type.ClassType)) {
                TypedSpec mapSrc = tmap.source();
                TypedLambda mOrder = null;
                boolean mAsc = true;
                if (mapSrc instanceof TypedSortBy msb) {
                    mOrder = msb.key();
                    mAsc = msb.ascending();
                    mapSrc = msb.source();
                }
                List<String> srcPath =
                        Substitution.pathOf(mapSrc, userVar);
                if (srcPath != null && srcPath.size() == 1
                        && toManyHead.test(cs, srcPath.get(0))) {
                    com.legend.lowering.NavArmCensus.fire(
                            "agg-computed-mapper-arm");
                    aggOut.computeIfAbsent(srcPath.get(0),
                                    k -> new ArrayList<>())
                            .add(new StoreResolver.AggDemand(nc, null, tmap.mapper(),
                                    mOrder, mAsc));
                    // the mapper BODY can still reference the OUTER row
                    // variable ($userVar) — its demands register too
                    // (study #13: the arm previously returned without
                    // rescanning, silently dropping outer-var aggregates)
                    for (TypedSpec b : tmap.mapper().body()) {
                        aggScan(b, userVar, cs, aggOut, bareOut,
                                toManyHead, bareHead);
                    }
                    for (int i = 1; i < nc.args().size(); i++) {
                        aggScan(nc.args().get(i), userVar, cs, aggOut, bareOut, toManyHead, bareHead);
                    }
                    return;
                }
            }
            if (path != null && path.size() == 2
                    && toManyHead.test(cs, path.get(0))) {
                com.legend.lowering.NavArmCensus.fire("agg-size2-leaf-arm");
                aggOut.computeIfAbsent(path.get(0), k -> new ArrayList<>())
                        .add(new StoreResolver.AggDemand(nc, path.get(1)));
                for (int i = 1; i < nc.args().size(); i++) {
                    aggScan(nc.args().get(i), userVar, cs, aggOut, bareOut, toManyHead, bareHead);
                }
                return;   // the path is agg-consumed, not bare
            }
            // BARE-HEAD COUNT ($x.employees->count()) — the matched
            // targets' ROW COUNT: encoded as the computed-mapper spelling
            // over a CONSTANT (one per target row), so the grouped-
            // subselect fold serves it unchanged (COUNT(agg col), zero-
            // when-empty join-back). The modelJoin/XStore sub-agg family.
            if (path != null && path.size() == 1 && isCountFamily(nc)
                    && bareHead.test(cs, path.get(0))) {
                var one1 = com.legend.compiler.element.type.Multiplicity
                        .Bounded.ONE;
                TypedLambda constMapper = new TypedLambda(List.of("_cnt"),
                        List.of(new TypedCInteger(1,
                                new ExprType(Type.Primitive.INTEGER, one1))),
                        new ExprType(new Type.FunctionType(
                                List.of(new Type.Param(
                                        nc.args().get(0).info().type(), one1)),
                                new Type.Param(Type.Primitive.INTEGER, one1)),
                                one1));
                com.legend.lowering.NavArmCensus.fire("agg-bare-count-arm");
                aggOut.computeIfAbsent(path.get(0), k -> new ArrayList<>())
                        .add(new StoreResolver.AggDemand(nc, null,
                                constMapper));
                for (int i = 1; i < nc.args().size(); i++) {
                    aggScan(nc.args().get(i), userVar, cs, aggOut, bareOut,
                            toManyHead, bareHead);
                }
                return;
            }
            // CHAIN bare count over a TO-ONE hop's to-many navigation —
            // count($p.firm.employees#f0), the qualifier-inlined
            // employeesByAge(30)->count() shape (engine: a grouped
            // subselect keyed on the chained hop's parent, joined back
            // through the mid hop). Registers under the DOTTED chain key;
            // buildAggMaterials anchors the material at the mid hop's
            // target class and the fold emits the mid LEFT join + the
            // prefix-re-pointed join-back condition.
            if (path != null && path.size() == 2 && isCountFamily(nc)
                    && !toManyHead.test(cs, path.get(0))
                    && bareHead.test(cs, path.get(0))
                    && !(nc.args().get(0).info().multiplicity()
                            instanceof com.legend.compiler.element.type
                                    .Multiplicity.Bounded bm
                            && Integer.valueOf(1).equals(bm.upper()))) {
                var one1 = com.legend.compiler.element.type.Multiplicity
                        .Bounded.ONE;
                TypedLambda constMapper = new TypedLambda(List.of("_cnt"),
                        List.of(new TypedCInteger(1,
                                new ExprType(Type.Primitive.INTEGER, one1))),
                        new ExprType(new Type.FunctionType(
                                List.of(new Type.Param(
                                        nc.args().get(0).info().type(), one1)),
                                new Type.Param(Type.Primitive.INTEGER, one1)),
                                one1));
                com.legend.lowering.NavArmCensus.fire("agg-chain-count-arm");
                aggOut.computeIfAbsent(path.get(0) + "." + path.get(1),
                                k -> new ArrayList<>())
                        .add(new StoreResolver.AggDemand(nc, null,
                                constMapper));
                for (int i = 1; i < nc.args().size(); i++) {
                    aggScan(nc.args().get(i), userVar, cs, aggOut, bareOut,
                            toManyHead, bareHead);
                }
                return;
            }
            // DEEP leaf ($f.employees.address.name->count()): encode as
            // the COMPUTED-MAPPER spelling (λe.$e.address.name) — the
            // fold's mapper machinery substitutes it through the target's
            // SubNav dispatch, and buildAggMaterials threads the mapper
            // paths as the target's sub-slot demand. Shapes the rebuild
            // can't peel (auto-map/milestoned spellings) stay loud below.
            if (path != null && path.size() > 2
                    && toManyHead.test(cs, path.get(0))) {
                TypedLambda synth = tailMapperOf(nc.args().get(0), userVar);
                if (synth != null) {
                    com.legend.lowering.NavArmCensus.fire("agg-deep-tail-arm");
                    aggOut.computeIfAbsent(path.get(0), k -> new ArrayList<>())
                            .add(new StoreResolver.AggDemand(nc, null, synth));
                    for (int i = 1; i < nc.args().size(); i++) {
                        aggScan(nc.args().get(i), userVar, cs, aggOut,
                                bareOut, toManyHead, bareHead);
                    }
                    return;
                }
            }
            // LOUD FALLTHROUGH (audit 9): any other aggregate whose argument
            // crosses a to-many would bare-demand the path — the join
            // explodes and the scalar reducer's to-one identity silently
            // EATS the aggregate. Never silent.
            if (path != null && path.size() > 2
                    && toManyHead.test(cs, path.get(0))) {
                throw new NotImplementedException("aggregate '"
                        + nc.callee().qualifiedName() + "' over the multi-hop"
                        + " to-many navigation " + String.join(".", path)
                        + " is not supported yet");
            }
            // STUDY #12: a to-many hop at index > 0 behind a to-one HEAD
            // escaped both audit-9 guards. For the IDENTITY-ELIDING
            // reducer family (sum/average/mean — Scalars' to-one elision)
            // that silently EATS the aggregate (the witnessed wrong
            // rows); list-space reducers (joinStrings, count) reduce the
            // exploded values correctly and stay allowed. Chain-demand
            // registration for this class is the real fix (own slice).
            if (path != null && path.size() >= 2
                    && !toManyHead.test(cs, path.get(0))
                    && isElidingReducer(nc)
                    && !(nc.args().get(0).info().multiplicity()
                            instanceof com.legend.compiler.element.type
                                    .Multiplicity.Bounded mb
                            && Integer.valueOf(1).equals(mb.upper()))) {
                throw new NotImplementedException("aggregate '"
                        + nc.callee().qualifiedName() + "' over the navigation "
                        + String.join(".", path) + " whose to-many hop sits"
                        + " BEHIND a to-one head is not supported yet"
                        + " (study #12 — the silent-eaten-aggregate class)");
            }
            if (path == null && containsToManyCrossing(nc.args().get(0), userVar, cs, toManyHead)) {
                throw new NotImplementedException("aggregate '"
                        + nc.callee().qualifiedName() + "' over an expression"
                        + " containing a to-many navigation is not supported yet");
            }
        }
        // VALUE-POSITION fan-out (task #78 step 2): a BARE ->map over a
        // to-many head (no reducer — the exploded values ARE the result;
        // engine golden testAdvancedDerivedPropertyThroughAssociation)
        // demands [head, leaf] for every leaf the mapper reads off its
        // param, so the flat LEFT JOIN materializes with those columns and
        // the substitution's inline arm resolves them.
        if (n instanceof TypedMap tm && tm.mapper().parameters().size() == 1) {
            List<String> sp = Substitution.pathOf(tm.source(), userVar);
            if (sp != null && sp.size() == 1 && toManyHead.test(cs, sp.get(0))) {
                String mv = tm.mapper().parameters().get(0);
                Set<List<String>> mp = new LinkedHashSet<>();
                for (TypedSpec b : tm.mapper().body()) {
                    FlattenOps.consumedPaths(b, mv, mp);
                }
                for (List<String> lp : mp) {
                    List<String> full = new ArrayList<>();
                    full.add(sp.get(0));
                    full.addAll(lp);
                    bareOut.add(full);
                }
                bareOut.add(sp);
            }
        }
        List<String> path = Substitution.pathOf(n, userVar);
        if (path != null) {
            bareOut.add(path);
        }
        if (n instanceof TypedLambda l && l.parameters().contains(userVar)) {
            return;   // shadowing: same stop as consumedPaths
        }
        for (TypedSpec c : n.children()) {
            aggScan(c, userVar, cs, aggOut, bareOut, toManyHead, bareHead);
        }
    }

    /** The per-element TAIL of a deep aggregated navigation, rebuilt as
     * a mapper lambda over the head's element ($f.employees.address.name
     * -> λ_agm.$_agm.address.name): peels pa/toOne wrappers down to the
     * 1-hop head access, then re-roots them on a fresh [1]-stamped param.
     * Null when a wrapper is not peelable (auto-map / milestoned
     * spellings) — the caller's loud wall stands. */
    private static @com.legend.Nullable TypedLambda tailMapperOf(TypedSpec arg, String userVar) {
        ArrayDeque<Function<TypedSpec, TypedSpec>> shell = new ArrayDeque<>();
        TypedSpec cur = arg;
        while (true) {
            List<String> p = Substitution.pathOf(cur, userVar);
            if (p != null && p.size() == 1) {
                break;
            }
            if (cur instanceof TypedNativeCall c && c.args().size() == 1
                    && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())) {
                final TypedNativeCall cc = c;
                shell.push(x -> cc.withChildren(List.of(x)));
                cur = c.args().get(0);
                continue;
            }
            if (cur instanceof TypedPropertyAccess pa) {
                final TypedPropertyAccess pp = pa;
                shell.push(x -> new TypedPropertyAccess(x, pp.property(),
                        pp.info()));
                cur = pa.source();
                continue;
            }
            return null;
        }
        if (!(cur.info().type() instanceof Type.ClassType)) {
            return null;
        }
        String v = "_agm";
        TypedSpec body = new TypedVariable(v, new ExprType(cur.info().type(),
                com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
        Type paramType = cur.info().type();
        while (!shell.isEmpty()) {
            body = shell.pop().apply(body);
        }
        return new TypedLambda(List.of(v), List.of(body),
                new ExprType(new Type.FunctionType(
                        List.of(new Type.Param(paramType,
                                com.legend.compiler.element.type.Multiplicity
                                        .Bounded.ONE)),
                        new Type.Param(body.info().type(),
                                body.info().multiplicity())),
                        com.legend.compiler.element.type.Multiplicity
                                .Bounded.ONE));
    }


    /** Any {@code $p.<toManyHead>.<...>} read anywhere under {@code n}. */
    static boolean containsToManyCrossing(TypedSpec n, String userVar,
            ClassSource cs,
            java.util.function.BiPredicate<ClassSource, String> toManyHead) {
        List<String> path = Substitution.pathOf(n, userVar);
        if (path != null && path.size() >= 2 && toManyHead.test(cs, path.get(0))) {
            return true;
        }
        if (n instanceof TypedLambda l && l.parameters().contains(userVar)) {
            return false;
        }
        for (TypedSpec c : n.children()) {
            if (containsToManyCrossing(c, userVar, cs, toManyHead)) {
                return true;
            }
        }
        return false;
    }

    /** The reducers whose to-one identity elision (Scalars) EATS the
     *  aggregate when the arg bare-demands to a per-row column — the
     *  guard fires only for these; list-carrier reducers survive. */
    private static boolean isElidingReducer(
            com.legend.compiler.spec.typed.TypedNativeCall nc) {
        String n = nc.callee().qualifiedName();
        return n.equals("meta::pure::functions::math::sum")
                || n.equals("meta::pure::functions::math::average")
                || n.equals("meta::pure::functions::math::mean");
    }

    private static boolean hasCol(Type.RelationType row, String name) {
        for (Type.Column c : row.columns()) {
            if (c.name().equals(name)) {
                return true;
            }
        }
        return false;
    }

    /** Slot aliases a binding expression reads ($row.alias...). */
    static void collectAliasReads(TypedSpec n, String rowVar,
            Set<String> slotAliases, Set<String> out) {
        if (n instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable v
                && v.name().equals(rowVar)
                && slotAliases.contains(pa.property())) {
            out.add(pa.property());
        }
        for (TypedSpec c : n.children()) {
            collectAliasReads(c, rowVar, slotAliases, out);
        }
    }

}
