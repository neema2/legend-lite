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
            Map<String, List<StoreResolver.AggDemand>> aggDemands) {
        Map<String, AssociationJoins.AssocJoin> aggMaterials = new LinkedHashMap<>();
        for (var entry : aggDemands.entrySet()) {
            Set<String> leaves = new LinkedHashSet<>();
            Set<List<String>> mapperPaths = new LinkedHashSet<>();
            for (StoreResolver.AggDemand dm : entry.getValue()) {
                leaves.addAll(dm.demandLeaves());
                if (dm.mapper() != null) {
                    for (TypedSpec b : dm.mapper().body()) {
                        StoreResolver.consumedPaths(b, dm.mapper().parameters().get(0),
                                mapperPaths);
                    }
                }
            }
            aggMaterials.put(entry.getKey(),
                    assocMaterial.aggJoinMaterial(temporal, cs, entry.getKey(),
                            context, leaves, mapperPaths));
        }
        return aggMaterials;
    }


    record CorrAggSub(TypedSpec subSource, List<String> keyCols,
            Type.RelationType keyRow, String targetPrefix, String rowVar,
            Type.RelationType joinedRow, ParentCopy pc) {}


    CorrAggSub corrAggSubSource(ClassSource cs, String head,
            AssociationJoins.AssocJoin aj, TypedLambda corrAgg) {
        if (corrAgg == null) {
            return new CorrAggSub(aj.targetPipeline(),
                    targetEquiKeys(aj.condition(), head), aj.targetRow(),
                    null, null, null, null);
        }
        List<String> keyCols = parentEquiKeys(aj.condition(), head);
        ParentCopy pc = parentCopyFor(cs, corrAgg);
        Type.RelationType pcRow = (Type.RelationType)
                pc.mat().pipeline().info().type();
        String corrTp = AssociationJoins.prefixFor(head + "_t", cs);
        List<Type.Column> jCols = new ArrayList<>(pcRow.columns());
        for (Type.Column c : aj.targetRow().columns()) {
            jCols.add(new Type.Column(
                    corrTp + c.name(), c.type(), c.multiplicity()));
        }
        Type.RelationType corrJoinedRow = new Type.RelationType(jCols);
        var jInfo = new ExprType(corrJoinedRow,
                com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
        TypedSpec joinedSub = new TypedJoin(pc.mat().pipeline(),
                aj.targetPipeline(), StoreResolver.leftKind(), aj.condition(),
                Optional.of(corrTp), jInfo);
        String corrRowVar = "_cj";
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
        List<String> keyCols = parentEquiKeys(aj.condition(),
                aj.prefix());
        ParentCopy pc = parentCopyFor(cs, aj.corrSubPred());
        Type.RelationType pcRow = (Type.RelationType)
                pc.mat().pipeline().info().type();
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
        var jInfo = new ExprType(jRow,
                com.legend.compiler.element.type.Multiplicity
                        .Bounded.ONE);
        TypedSpec joinedSub = new TypedJoin(pc.mat().pipeline(),
                aj.targetPipeline(), StoreResolver.leftKind(), aj.condition(),
                Optional.of(corrTp), jInfo);
        TypedLambda where = assocMaterial.corrPredOnJoinedRow(
                aj.corrSubPred(), cs, aj.target(), corrTp,
                aj.targetSlotPrefixes(), aj.targetSubNavs(),
                pc.mat().slotPrefixes(), pc.subNavs(),
                "_cj", jRow);
        TypedSpec filtered = new TypedFilter(joinedSub, where, jInfo);
        var cjInfo = new ExprType(jRow,
                com.legend.compiler.element.type.Multiplicity
                        .Bounded.ONE);
        List<com.legend.compiler.spec.typed.TypedFuncCol> pCols =
                new ArrayList<>();
        List<Type.Column> subColsX = new ArrayList<>();
        List<String> subKeys = new ArrayList<>();
        for (int ki = 0; ki < keyCols.size(); ki++) {
            String k = keyCols.get(ki);
            var col = pcRow.columns().stream()
                    .filter(c -> c.name().equals(k)).findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "resolver bug: equi-key column '" + k
                                    + "' missing from the parent"
                                    + " copy row"));
            String pk = "_pk" + ki;
            subKeys.add(pk);
            pCols.add(projectedCol(pk, "_cj", cjInfo, k,
                    new ExprType(col.type(), col.multiplicity())));
            subColsX.add(new Type.Column(pk, col.type(),
                    col.multiplicity()));
        }
        for (Type.Column c : aj.targetRow().columns()) {
            pCols.add(projectedCol(c.name(), "_cj", cjInfo,
                    corrTp + c.name(),
                    new ExprType(c.type(), c.multiplicity())));
            subColsX.add(c);
        }
        Type.RelationType subRowX = new Type.RelationType(subColsX);
        TypedSpec subPipe = new TypedProject(filtered, pCols,
                new ExprType(subRowX,
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


private static List<String> parentEquiKeys(TypedLambda cond, String head) {
        List<String> keys = new ArrayList<>();
        if (!collectEquiKeys(cond.body().get(cond.body().size() - 1),
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


    ParentCopy parentCopyFor(ClassSource cs, TypedLambda corr) {
        Set<String> names = new LinkedHashSet<>();
        for (TypedSpec b : corr.body()) {
            collectVarNamesInto(b, names);
        }
        names.removeAll(corr.parameters());
        Set<List<String>> outerPaths = new LinkedHashSet<>();
        for (String v : names) {
            for (TypedSpec b : corr.body()) {
                StoreResolver.consumedPaths(b, v, outerPaths);
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
                String al = StoreResolver.navSlotAlias(hb, cs.rowVar(), navSteps.keySet());
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
                                sources.get(cs.mappingFqn(), tc2).pipeline(),
                                java.util.Set.of(), tc2).pipeline());
        Map<String, Substitution.SubNav> subNavs = new LinkedHashMap<>();
        for (var e : navByHead.entrySet()) {
            String pfx = mat.slotPrefixes().get(e.getValue());
            var stepT = navSteps.get(e.getValue()).target();
            if (pfx == null || !(stepT instanceof TypedGetAll stg)) {
                continue;
            }
            ClassSource sub = sources.get(cs.mappingFqn(), stg.classFqn());
            subNavs.put(e.getKey(), new Substitution.SubNav(
                    pfx, sub.rowVar(), sub.bindings()));
        }
        return new ParentCopy(mat, subNavs);
    }


private static void collectVarNamesInto(TypedSpec n, Set<String> out) {
        if (n instanceof com.legend.compiler.spec.typed.TypedVariable v) {
            out.add(v.name());
        }
        for (TypedSpec c : n.children()) {
            collectVarNamesInto(c, out);
        }
    }


private static List<String> targetEquiKeys(TypedLambda cond,
                                                         String head) {
        List<String> keys = new ArrayList<>();
        if (!collectEquiKeys(cond.body().get(cond.body().size() - 1),
                cond.parameters().get(0), cond.parameters().get(1), keys)
                || keys.isEmpty()) {
            throw new NotImplementedException("aggregate over navigation '"
                    + head + "' requires a conjunctive equi-join association"
                    + " condition (grouped-subselect emission)");
        }
        return keys;
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
            String alias, TypedLambda corrAgg, String corrTp,
            String corrRowVar, Type.RelationType corrJoinedRow,
            ParentCopy pc) {
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
            TypedLambda mm = corrAgg == null
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
                            pc.mat().slotPrefixes(), pc.subNavs(),
                            corrRowVar, corrJoinedRow);
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
            if (corrAgg != null) {
                // correlated sub: the leaf reads land PREFIXED on the
                // joined row
                final String rv = corrRowVar;
                final var jr = corrJoinedRow;
                mapBody = Pipelines.prefixColumns(leafBinding,
                        aj.target().rowVar(), corrTp,
                        v -> new com.legend.compiler.spec.typed
                                .TypedVariable(rv, new ExprType(jr,
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ONE)));
            }
        }
        if (corrAgg != null) {
            mapVar = corrRowVar;
            mapRowType = corrJoinedRow;
        }
        TypedLambda map = new TypedLambda(
                List.of(mapVar),
                List.of(mapBody),
                new ExprType(
                        new Type.FunctionType(
                                List.of(new com.legend.compiler
                                        .element.type.Type.Param(mapRowType,
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
            orderLambda = corrAgg == null
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
                            pc.mat().slotPrefixes(), pc.subNavs(),
                            corrRowVar, corrJoinedRow);
        }
        return new TypedAggCol(alias, map, reduce, orderLambda,
                d.orderAsc());
    }
    /** The bare column a side of an equi conjunct reads on {@code var}. */
    private static String bareColumnOn(TypedSpec n, String var) {
        return n instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable v
                && v.name().equals(var) ? pa.property() : null;
    }


record CompositeChain(TypedSpec pipeline,
            TypedLambda orientedCond) {}


CompositeChain compositeChainTarget(ClassSource cs,
            TypedLambda navCond, TypedSpec targetPipe) {
        Set<String> parentSlots = Pipelines.slotAliases(cs.pipeline());
        if (parentSlots.isEmpty()) {
            return null;
        }
        String sParam = navCond.parameters().get(0);
        String tParam = navCond.parameters().get(1);
        String slotRef = null;
        for (String sl : parentSlots) {
            for (TypedSpec b : navCond.body()) {
                if (Pipelines.referencesAliasOn(b, sParam, Set.of(sl))) {
                    if (slotRef != null && !slotRef.equals(sl)) {
                        throw new NotImplementedException(
                                "navigate-step condition reads MULTIPLE"
                                        + " sibling joinslots (" + slotRef
                                        + ", " + sl + ") — the multi-slot"
                                        + " composite is not built yet");
                    }
                    slotRef = sl;
                }
            }
        }
        if (slotRef == null) {
            return null;
        }
        var js = Pipelines.joinSlots(cs.pipeline()).get(slotRef);
        if (js == null
                || !(js.target().info().type() instanceof Type.RelationType optRow)
                || !(targetPipe.info().type() instanceof Type.RelationType tgtRow)) {
            return null;
        }
        // GUARDS (loud, never silent): the step condition must read the
        // parent ONLY through the slot; hop-1's own condition must not
        // read further slots.
        for (TypedSpec b : navCond.body()) {
            if (readsVarOutsideSlot(b, sParam, slotRef)) {
                throw new NotImplementedException(
                        "navigate-step condition mixes sibling-slot reads"
                                + " with DIRECT parent reads — the mixed"
                                + " composite is not built yet");
            }
        }
        TypedLambda c1 = js.condition();
        for (TypedSpec b : c1.body()) {
            for (String sl : parentSlots) {
                if (Pipelines.referencesAliasOn(b, c1.parameters().get(0),
                        Set.of(sl))) {
                    throw new NotImplementedException(
                            "chained joinslot condition reads a further"
                                    + " sibling slot — deep composite"
                                    + " chains are not built yet");
                }
            }
        }
        String pfx = slotRef + "_";
        boolean clash = true;
        while (clash) {
            clash = false;
            for (Type.Column c : tgtRow.columns()) {
                if (c.name().startsWith(pfx)) {
                    pfx = "_" + pfx;
                    clash = true;
                }
            }
        }
        List<Type.Column> compCols = new ArrayList<>(tgtRow.columns());
        for (Type.Column c : optRow.columns()) {
            compCols.add(new Type.Column(pfx + c.name(), c.type(),
                    c.multiplicity()));
        }
        Type.RelationType compRow = new Type.RelationType(compCols);
        var one = com.legend.compiler.element.type.Multiplicity.Bounded.ONE;
        String lv = "_cl";
        String rv = "_cr";
        TypedSpec b0 = navCond.body().get(navCond.body().size() - 1);
        TypedSpec b1 = Pipelines.rewriteRowReads(b0, tParam, Map.of(),
                Set.of(), v -> new TypedVariable(lv,
                        new ExprType(tgtRow, one)));
        TypedSpec b2 = Pipelines.rewriteRowReads(b1, sParam,
                Map.of(slotRef, ""), Set.of(),
                v -> new TypedVariable(rv, new ExprType(optRow, one)));
        TypedLambda joinCond = new TypedLambda(List.of(lv, rv), List.of(b2),
                new ExprType(new Type.FunctionType(
                        List.of(new Type.Param(tgtRow, one),
                                new Type.Param(optRow, one)),
                        new Type.Param(Type.Primitive.BOOLEAN, one)), one));
        TypedSpec composite = new TypedJoin(targetPipe, js.target(),
                StoreResolver.leftKind(), joinCond, Optional.of(pfx),
                new ExprType(compRow, one));
        // hop-1's condition, its TARGET (slot-table) reads landing on the
        // composite's prefixed columns
        String c1t = c1.parameters().get(1);
        TypedSpec oc = Pipelines.prefixColumns(
                c1.body().get(c1.body().size() - 1), c1t, pfx,
                v -> new TypedVariable(c1t, new ExprType(compRow, one)));
        Type c1p0 = c1.info().type() instanceof Type.FunctionType cft
                ? cft.params().get(0).type() : cs.rowType();
        TypedLambda oriented = new TypedLambda(c1.parameters(), List.of(oc),
                new ExprType(new Type.FunctionType(
                        List.of(new Type.Param(c1p0, one),
                                new Type.Param(compRow, one)),
                        new Type.Param(Type.Primitive.BOOLEAN, one)), one));
        return new CompositeChain(composite, oriented);
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


static final Set<String> AGG_FQNS = aggFqns();


private static Set<String> aggFqns() {
        Set<String> out = new LinkedHashSet<>();
        for (String name : List.of("average", "mean", "sum", "max",
                "min", "joinStrings", "percentile", "median",
                "stdDevPopulation", "stdDevSample",
                "variancePopulation", "varianceSample", "count", "size")) {
            for (var f : Pure.nativeFunctionsAt(name)) {
                out.add(f.qualifiedName());
            }
        }
        return out;
    }


static boolean isCountFamily(TypedNativeCall nc) {
        String q = nc.callee().qualifiedName();
        return q.equals("meta::pure::functions::collection::count")
                || q.equals("meta::pure::functions::collection::size");
    }


static void collectParamColumnReads(TypedLambda cond, Set<String> out) {
        String src = cond.parameters().get(0);
        for (TypedSpec b : cond.body()) {
            StoreResolver.collectVarColumnReads(b, src, out);
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
        Type.RelationType rowT = (Type.RelationType) tPipe.info().type();
        Map<String, Substitution.AssocSub> navAssocs = new LinkedHashMap<>();
        for (var e : subNavs.entrySet()) {
            var sn = e.getValue();
            navAssocs.put(e.getKey(), new Substitution.AssocSub(
                    sn.prefix(), sn.rowVar(), sn.bindings(),
                    target.classFqn() + "." + e.getKey(),
                    Set.of(), Map.of(), target.rowVar(), rowT,
                    Map.of(), sn.children()));
        }
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
            StoreResolver.consumedPaths(b, lambda.parameters().get(0), out);
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
    static void registerSubTypeSubs(ClassSource cs, TypedSpec top,
            ClassSources sources,
            Map<String, Substitution.AssocSub> assocs) {
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
            Map<String, TypedSpec> stBindings = new LinkedHashMap<>();
            for (Type.Column c : cs.rowType().columns()) {
                int at = c.name().indexOf(stPrefix);
                if (at >= 0) {
                    stBindings.put(c.name().substring(at + stPrefix.length()),
                            new TypedPropertyAccess(
                                    new TypedVariable(cs.rowVar(),
                                            ExprType.one(cs.rowType())),
                                    c.name(),
                                    new ExprType(c.type(), c.multiplicity())));
                }
            }
            if (!stBindings.isEmpty()) {
                assocs.put(Substitution.SUBTYPE_KEY + fqn,
                        new Substitution.AssocSub("", cs.rowVar(),
                                stBindings, fqn, Set.of()));
                continue;
            }
            if (!sources.binds(cs.mappingFqn(), fqn)) {
                continue;
            }
            ClassSource sub = sources.get(cs.mappingFqn(), fqn);
            Set<String> cols = new LinkedHashSet<>();
            for (TypedSpec b : sub.bindings().values()) {
                StoreResolver.collectVarColumnReads(b, sub.rowVar(), cols);
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

    /** Class targets of subType calls over a variable, anywhere in the chain. */
    private static void collectSubTypeFqns(TypedSpec n, Set<String> out) {
        if (n instanceof TypedNativeCall nc
                && nc.callee().qualifiedName()
                        .equals("meta::pure::functions::lang::subType")
                && !nc.args().isEmpty()
                && nc.args().get(0) instanceof TypedVariable
                && nc.info().type() instanceof Type.ClassType ct) {
            out.add(ct.fqn());
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
        if (!(n instanceof TypedPropertyAccess pa)
                || !(pa.source() instanceof TypedNativeCall sc)
                || !sc.callee().qualifiedName()
                        .equals("meta::pure::functions::lang::subType")
                || sc.args().isEmpty()
                || !(sc.info().type() instanceof Type.ClassType sct)
                || !sc.info().multiplicity().isMany()
                || !(sc.args().get(0).info().type()
                        instanceof Type.ClassType navCt)) {
            return n;
        }
        String wKey = com.legend.model.ClassMapping.subTypeColumn(sct.fqn(),
                com.legend.model.ClassMapping.memberWitness());
        // audit 23 A5: only an UNDECIDABLE dispatch context (no runtime,
        // unknown mapping) skips canonicalization; a resolution failure
        // AFTER binds() confirmed the class is a real bug and propagates
        String m;
        try {
            m = mappingOf.apply(navCt.fqn());
        } catch (MappingResolutionException e) {
            m = null;
        }
        ClassSource target = m != null && sources.binds(m, navCt.fqn())
                ? sources.get(m, navCt.fqn()) : null;
        if (target == null || !target.bindings().containsKey(wKey)) {
            return n;
        }
        TypedSpec nav = sc.args().get(0);
        var bool1 = new ExprType(Type.Primitive.BOOLEAN,
                com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
        TypedSpec wRead = new TypedPropertyAccess(
                new TypedVariable("v_stw", new ExprType(navCt,
                        com.legend.compiler.element.type
                                .Multiplicity.Bounded.ONE)),
                wKey, new ExprType(Type.Primitive.BOOLEAN,
                        com.legend.compiler.element.type
                                .Multiplicity.Bounded.ZERO_ONE));
        TypedLambda pred = new TypedLambda(List.of("v_stw"),
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
        return new TypedPropertyAccess(
                new TypedFilter(nav, pred, nav.info()),
                com.legend.model.ClassMapping.subTypeColumn(sct.fqn(),
                        pa.property()),
                pa.info());
    }

    /** ORDERING CONTRACT (audit 15 B3): aggReads is IDENTITY-keyed on the
     * scanned nodes — this scan must run AFTER every identity-changing
     * rewrite (splitDatedHeads etc.) and its keys are consumed in the SAME
     * resolveObject pass. A rewrite inserted between scan and substitution
     * dangles the keys silently. */
    static void aggScan(TypedSpec n, String userVar, ClassSource cs,
                         Map<String, List<StoreResolver.AggDemand>> aggOut,
                         Set<List<String>> bareOut,
                         java.util.function.BiPredicate<ClassSource, String> toManyHead) {
        if (n instanceof TypedNativeCall nc
                && !nc.args().isEmpty()
                && AGG_FQNS.contains(nc.callee().qualifiedName())) {
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
                    aggOut.computeIfAbsent(sp.get(0), k -> new ArrayList<>())
                            .add(new StoreResolver.AggDemand(nc, spa.property(), null,
                                    ssb.key(), ssb.ascending()));
                    for (int i = 1; i < nc.args().size(); i++) {
                        aggScan(nc.args().get(i), userVar, cs, aggOut, bareOut, toManyHead);
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
                    && !(tmap.mapper().info().type()
                            instanceof Type.FunctionType mft
                            && mft.result().type() instanceof Type.ClassType)) {
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
                    aggOut.computeIfAbsent(srcPath.get(0),
                                    k -> new ArrayList<>())
                            .add(new StoreResolver.AggDemand(nc, null, tmap.mapper(),
                                    mOrder, mAsc));
                    for (int i = 1; i < nc.args().size(); i++) {
                        aggScan(nc.args().get(i), userVar, cs, aggOut, bareOut, toManyHead);
                    }
                    return;
                }
            }
            if (path != null && path.size() == 2
                    && toManyHead.test(cs, path.get(0))) {
                aggOut.computeIfAbsent(path.get(0), k -> new ArrayList<>())
                        .add(new StoreResolver.AggDemand(nc, path.get(1)));
                for (int i = 1; i < nc.args().size(); i++) {
                    aggScan(nc.args().get(i), userVar, cs, aggOut, bareOut, toManyHead);
                }
                return;   // the path is agg-consumed, not bare
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
            if (path == null && containsToManyCrossing(nc.args().get(0), userVar, cs, toManyHead)) {
                throw new NotImplementedException("aggregate '"
                        + nc.callee().qualifiedName() + "' over an expression"
                        + " containing a to-many navigation is not supported yet");
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
            aggScan(c, userVar, cs, aggOut, bareOut, toManyHead);
        }
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
