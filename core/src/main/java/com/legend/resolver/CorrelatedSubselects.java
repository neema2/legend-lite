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
            StoreResolver.collectAliasReads(hb, cs.rowVar(), slots, slotDemand);
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
        return new TypedAggCol(alias, map, reduce);
    }
    /** The bare column a side of an equi conjunct reads on {@code var}. */
    private static String bareColumnOn(TypedSpec n, String var) {
        return n instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable v
                && v.name().equals(var) ? pa.property() : null;
    }


}
