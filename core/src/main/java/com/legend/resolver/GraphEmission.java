// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.MilestoningStrategy;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedGraphTree;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNavigate;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedNewInstanceCast;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSerializeGraph;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.MappingResolutionException;
import com.legend.error.NotImplementedException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.IntSupplier;
import java.util.function.UnaryOperator;
/**
 * GRAPH-terminal emission (plan H4a SNAPSHOT): the serialize envelope —
 * leaves substituted over the row, class-typed children as correlated
 * per-hop nodes (the EXISTS material shape), to-many children
 * array-wrapped; the implicit envelope enumerates the class's scalar
 * bindings plus the GENERATED temporal-context dates.
 */
final class GraphEmission {

    private final ModelContext ctx;
    private final ClassSources sources;
    private final AssociationJoins assocMaterial;
    private final TemporalFrame temporal;
    /** Mapping dispatch (context, classFqn) -> mappingFqn — the
     * resolver's runtime-aware routing. */
    private final BiFunction<StoreResolver.Context,
            String, String> dispatch;
    /** Fresh child row-var mint (shares the resolver's counter). */
    private final IntSupplier freshVar;

    GraphEmission(ModelContext ctx, ClassSources sources,
            AssociationJoins assocMaterial, TemporalFrame temporal,
            BiFunction<StoreResolver.Context, String,
                    String> dispatch,
            IntSupplier freshVar) {
        this.ctx = ctx;
        this.sources = sources;
        this.assocMaterial = assocMaterial;
        this.temporal = temporal;
        this.dispatch = dispatch;
        this.freshVar = freshVar;
    }


    /**
     * The implicit-serialize tree for a BARE class root: one leaf per
     * SCALAR binding, declaration order — class-typed bindings (embedded
     * ctors, navigation slots) are graph CHILDREN territory and stay out
     * of the bare-root envelope (plan §E10).
     */
    List<TypedGraphTree> synthesizeScalarTree(ClassSource cs) {
        // the IMPLICIT envelope serializes every binding — a class with a
        // deferred M2M wall (ledger cluster 21) must stay LOUD here, never
        // silently omit the property from graph output
        for (var dw : cs.deferredWalls().entrySet()) {
            throw new com.legend.error.NotImplementedException(dw.getValue());
        }
        List<TypedGraphTree> tree = new ArrayList<>();
        // the engine's instance select projects the set's OWN property
        // mappings (batch 68): a property the implicit same-extent
        // inheritance pre-pass merged from an ancestor's set is served on
        // ACCESS through that binding, never fetched with the instance
        // (StockProduct over milestoningmap: id, name, type — not the
        // Product set's stockProductName / classificationType joins)
        java.util.Set<String> own = sources.ownPropertiesOf(
                cs.mappingFqn(), cs.classFqn(), cs.setId());
        for (Map.Entry<String, TypedSpec> e : cs.bindings().entrySet()) {
            // subtype-dispatch pseudo-bindings are CAST machinery, not
            // properties of the class — the implicit envelope never
            // serializes them (they broke testAllForB when the #71
            // same-source synthesis joined the binding table)
            if (com.legend.model.ClassMapping.isSubTypeColumn(e.getKey())
                    || com.legend.model.ClassMapping.isPrimaryKeyBinding(e.getKey())) {
                continue;   // ...and so are the D3 key pseudo-bindings
            }
            if (!own.isEmpty() && !own.contains(e.getKey())) {
                continue;   // implicitly inherited: on demand, not eager
            }
            TypedSpec inner = e.getValue();
            if (inner instanceof TypedNativeCall c
                    && c.args().size() == 1
                    && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())) {
                inner = c.args().get(0);
            }
            if (inner instanceof TypedNewInstance
                    || inner.info().type()
                            instanceof Type.ClassType) {
                continue;
            }
            tree.add(new TypedGraphTree(e.getKey(), List.of()));
        }
        // GENERATED temporal-context properties ride the implicit envelope
        // (engine: temporal instances serialize their dates; sweeps read
        // each version row's own validity-start)
        MilestoningStrategy strat = temporal.temporalStrategy(cs.classFqn());
        if (strat != null) {
            // engine getTemporalDateAlias: a %latest date on a relation
            // that CANNOT support the strategy contributes NO alias — the
            // generated column simply does not exist (ledger cluster 13;
            // decided per axis — a bitemporal class over a business-only
            // table keeps k_businessDate, drops k_processingDate). A
            // CONCRETE date still projects (audit 14's ungate stands).
            Map<String, String> mcols = temporal.milestoneColumnsOf(
                    cs.pipeline(), cs.classFqn());
            if (strat != MilestoningStrategy.PROCESSING
                    && !cs.bindings().containsKey("businessDate")
                    && !(temporal.root().business()
                                instanceof com.legend.compiler.spec.typed
                                        .TypedCLatestDate
                            && mcols.get(TemporalFrame.GEN_BUSINESS_DATE)
                                    == null)) {
                tree.add(new TypedGraphTree("businessDate", List.of()));
            }
            if (strat != MilestoningStrategy.BUSINESS
                    && !cs.bindings().containsKey("processingDate")
                    && !(temporal.root().processing()
                                instanceof com.legend.compiler.spec.typed
                                        .TypedCLatestDate
                            && mcols.get(TemporalFrame.GEN_PROCESSING_DATE)
                                    == null)) {
                tree.add(new TypedGraphTree("processingDate", List.of()));
            }
        }
        return tree;
    }

    /** The generated date's VALUE for an envelope leaf: the fetch context
     * date when one exists (point fetch), else the row's own
     * validity-start milestone column (version sweep); {@code null} when
     * the property is not a generated date here. */
    @com.legend.Nullable TypedSpec generatedDateLeaf(ClassSource cs, String prop,
            Type.RelationType rowType,
            String rowVar) {
        if ((!prop.equals("businessDate") && !prop.equals("processingDate"))
                || temporal.temporalStrategy(cs.classFqn()) == null) {
            return null;
        }
        // the point-fetch CONSTANT needs no milestone columns — a temporal
        // class on a capability-tolerance (non-milestoned) table still has
        // a well-defined context date (audit 14 B-F8: the column check
        // walled it needlessly)
        TypedSpec ctxDate = prop.equals("businessDate")
                ? temporal.root().business() : temporal.root().processing();
        if (ctxDate != null) {
            return ctxDate;
        }
        Map<String, String> mc = temporal.milestoneColumnsOf(cs.pipeline(), cs.classFqn());
        if (mc.isEmpty()) {
            return null;
        }
        String col = mc.get(prop.equals("processingDate")
                ? TemporalFrame.GEN_PROCESSING_DATE
                : TemporalFrame.GEN_BUSINESS_DATE);
        if (col == null) {
            return null;
        }
        var colDef = rowType.columns().stream()
                .filter(c -> c.name().equals(col)).findFirst().orElse(null);
        if (colDef == null) {
            return null;
        }
        return new TypedPropertyAccess(
                new TypedVariable(rowVar,
                        new ExprType(rowType,
                                com.legend.compiler.element.type.Multiplicity
                                        .Bounded.ONE)),
                col, new ExprType(
                        colDef.type(), colDef.multiplicity()));
    }

    /**
     * One envelope node (recursive): {@code leaves} substitute the class's
     * bindings over the row var; class-typed children become correlated
     * per-hop nodes — the child pipeline FILTERED by the association
     * condition with the PARENT row var free (the EXISTS material shape),
     * to-many children array-wrapped. Same-leaf pruning is by construction:
     * only tree entries are emitted.
     */
    TypedSerializeGraph buildGraphNode(ClassSource cs, TypedSpec pipeline,
            Map<String, String> slotPrefixes, Set<String> stripped, String rowVar,
            List<TypedGraphTree> tree, StoreResolver.Context context, boolean arrayWrap,
            ExprType info) {
        return buildGraphNode(cs, pipeline, slotPrefixes, stripped, rowVar,
                tree, context, arrayWrap, info, false);
    }

    TypedSerializeGraph buildGraphNode(ClassSource cs, TypedSpec pipeline,
            Map<String, String> slotPrefixes, Set<String> stripped, String rowVar,
            List<TypedGraphTree> tree, StoreResolver.Context context, boolean arrayWrap,
            ExprType info, boolean checked) {
        var rowType = Type.requireRelationSchema(pipeline.info().type());
        java.util.function.Function<@com.legend.Nullable TypedSpec, TypedSpec> toRow = v -> new TypedVariable(
                rowVar, new ExprType(rowType,
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
        List<TypedFuncCol> leaves = new ArrayList<>();
        List<TypedSerializeGraph.Child> children = new ArrayList<>();
        List<TypedSerializeGraph.SubTypePatch> subTypePatches = new ArrayList<>();
        for (TypedGraphTree node : tree) {
            // ->subType(@X){...}: the subtype VIEW — extracted builder
            if (node.subTypeFqn() != null) {
                subTypePatches.add(subTypePatch(cs, node, context, rowVar,
                        rowType, toRow));
                continue;
            }
            if (!node.children().isEmpty()
                    || (!cs.bindings().containsKey(node.property())
                            && ctx.findAssociationOf(cs.classFqn(), node.property())
                                    .isPresent())
                    || isDerivedClassProp(cs, node)
                    || isPlainClassProp(cs, node)) {
                children.add(graphChild(cs, node, context, rowVar, rowType, pipeline));
                continue;
            }
            TypedSpec binding = cs.bindings().get(node.property());
            if (binding == null) {
                // DERIVED (qualified) leaf: the class's parameterless
                // derived property inlines through its lifted body
                // function ($this reads substitute to the row bindings) —
                // the QUERY position gets this at the Typer front door;
                // tree leaves are bare names and inline here (task #78)
                TypedSpec derived = derivedLeaf(cs, node, context,
                        rowVar, rowType);
                if (derived != null) {
                    var dFn = new Type.FunctionType(
                            List.of(new Type.Param(rowType,
                                    com.legend.compiler.element.type
                                            .Multiplicity.Bounded.ONE)),
                            new Type.Param(derived.info().type(),
                                    derived.info().multiplicity()));
                    // the engine serializes qualifier leaves under the
                    // ALIAS when given, else the CALL spelling {"name()"}
                    leaves.add(new TypedFuncCol(node.alias() != null
                            ? node.alias() : callKey(node),
                            new TypedLambda(List.of(rowVar),
                                    List.of(derived),
                                    new ExprType(dFn,
                                            com.legend.compiler.element.type
                                                    .Multiplicity.Bounded
                                                    .ONE))));
                    continue;
                }
                // GENERATED temporal-context property on the envelope:
                // point fetch = the context date constant; VERSION SWEEP =
                // each row's own validity-start column (engine: the
                // property maps to BUS_FROM / PROCESSING_IN / snapshot)
                TypedSpec gen = generatedDateLeaf(cs, node.property(),
                        rowType, rowVar);
                if (gen == null) {
                    // an UNMAPPED OPTIONAL property serializes as null
                    // (engine graph semantics: milestoningModel's
                    // inlinedExchangeName rides unmapped in milestoningmap
                    // and the golden emits null); REQUIRED stays loud
                    var mp = ctx.findProperty(cs.classFqn(), node.property())
                            .orElse(null);
                    if (mp != null && mp.multiplicity()
                            instanceof com.legend.compiler.element.type
                                    .Multiplicity.Bounded ob
                            && ob.lower() == 0) {
                        gen = new com.legend.compiler.spec.typed
                                .TypedCollection(List.of(),
                                        new ExprType(mp.type(),
                                                com.legend.compiler.element
                                                        .type.Multiplicity
                                                        .Bounded.ZERO_ONE));
                    }
                }
                if (gen == null) {
                    throw new MappingResolutionException("property '"
                            + SyntheticHeads.displayName(node.property())
                            + "' of class '" + cs.classFqn()
                            + "' is not mapped in mapping '" + cs.mappingFqn()
                            + "'", cs.classFqn());
                }
                var genFn = new Type.FunctionType(
                        List.of(new Type.Param(
                                rowType,
                                com.legend.compiler.element.type.Multiplicity
                                        .Bounded.ONE)),
                        new Type.Param(
                                gen.info().type(), gen.info().multiplicity()));
                leaves.add(new TypedFuncCol(keyOf(node),
                        new TypedLambda(List.of(rowVar), List.of(gen),
                                new ExprType(genFn,
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ONE))));
                continue;
            }
            TypedSpec inner = binding;
            if (inner instanceof TypedNativeCall c
                    && c.args().size() == 1
                    && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())) {
                inner = c.args().get(0);
            }
            TypedSerializeGraph.Child arrChild = primitiveArrayLeaf(
                    cs, node, inner, rowVar, rowType);
            if (arrChild != null) {
                children.add(arrChild);
                continue;
            }
            // audit 23: any OTHER to-many read through a slot (wrapped in
            // a computed expression, deeper chain) would fall to the
            // joined-scalar path and EXPLODE parent rows — loud
            if (!(inner.info().multiplicity()
                    instanceof com.legend.compiler.element.type
                            .Multiplicity.Bounded bW
                    && Integer.valueOf(1).equals(bW.upper()))) {
                java.util.Set<String> slotish = new java.util.LinkedHashSet<>(
                        Pipelines.outerNavSteps(cs.pipeline()).keySet());
                slotish.addAll(Pipelines.joinSlots(cs.pipeline()).keySet());
                if (Pipelines.referencesAliasOn(inner, cs.rowVar(), slotish)) {
                    throw new NotImplementedException("graph leaf '"
                            + node.property() + "' reads a TO-MANY slot"
                            + " through a computed expression — the joined-"
                            + "scalar emission would explode parent rows");
                }
            }
            if (inner instanceof TypedNewInstance) {
                throw new NotImplementedException("graph leaf '" + node.property()
                        + "' is an EMBEDDED class property — embedded graph"
                        + " children are not supported yet (H4b)");
            }
            if (inner instanceof TypedNewInstanceCast) {
                throw new NotImplementedException("graph property '" + node.property()
                        + "' is a MODEL-TO-MODEL cast binding — M2M graph"
                        + " children are not supported yet (H5c)");
            }
            // AGG-CONSUMED reads of STRIPPED to-many nav steps (the H4b
            // sub-aggregation leaf: employeeCount = count($row.employees)
            // through the M2M chain composition): the class-typed slot
            // read becomes the CORRELATED TARGET RELATION — the nav
            // condition keyed on the parent row var — and count/size/
            // isEmpty lower through the relation-predicate scalar arms as
            // correlated subqueries (engine: per-leaf subselect). Only
            // STRIPPED aliases rewrite; a materialized slot keeps the
            // joined-scalar path.
            TypedSpec navCorr = correlateStrippedNavReads(binding, cs,
                    rowVar, rowType, stripped, context);
            if (navCorr != null) {
                // ALREADY in final row terms (the walker re-points parent
                // reads at the leaf row var) — the rewriteRowReads pass
                // below must not walk the embedded relation
                leaves.add(aggLeaf(keyOf(node), navCorr, rowVar, rowType));
                continue;
            }
            // NAV CHAINS inside a scalar leaf (the M2M chain composition
            // inlines source nav markers verbatim: managers = joinStrings(
            // $row.manager.firstName->concatenate(...))): each chain
            // through an UNMATERIALIZED alias rewrites to the correlated
            // scalar subquery the derived-leaf route emits (depth-N).
            boolean[] navHit = {false};
            TypedSpec navRew = rewriteNavChains(binding, cs, context,
                    rowVar, rowType, slotPrefixes, navHit);
            if (navHit[0]) {
                binding = navRew;
            }
            // A leaf mapped through STRIPPED join slots (a nested child's
            // own joins materialize with empty demand) is a feature gap,
            // not a resolver bug (audit F5).
            if (Pipelines.referencesAliasOn(binding, cs.rowVar(), stripped)) {
                throw new NotImplementedException("graph leaf '" + node.property()
                        + "' of class '" + cs.classFqn() + "' is mapped through"
                        + " the class's own join slots — nested join demand"
                        + " inside a graph child is not supported yet (H4b)");
            }
            TypedSpec body = Pipelines.rewriteRowReads(binding, cs.rowVar(),
                    slotPrefixes, stripped, toRow::apply);
            // the leaf's RESULT type is the MODEL property's for the
            // DATE family: DateTime -> full instant, StrictDate -> bare,
            // abstract Date -> by the PHYSICAL value's precision (the
            // wrap dispatches at runtime; setup DDL can diverge from
            // the store declaration the binding is typed by)
            Type resultType = body.info().type();
            var mprop = ctx.findProperty(cs.classFqn(), node.property())
                    .orElse(null);
            if (mprop != null && isDateFamily(mprop.type())
                    && isDateFamily(resultType)) {
                resultType = mprop.type();
            }
            var fnType = new Type.FunctionType(
                    List.of(new Type.Param(rowType,
                            com.legend.compiler.element.type.Multiplicity.Bounded.ONE)),
                    new Type.Param(
                            resultType, body.info().multiplicity()));
            leaves.add(new TypedFuncCol(keyOf(node),
                    new TypedLambda(List.of(rowVar), List.of(body),
                            new ExprType(fnType,
                                    com.legend.compiler.element.type.Multiplicity.Bounded.ONE))));
        }
        // UNION-root row order: the engine serializes members in BRANCH
        // DECLARATION order (serial union); DuckDB's aggregation is scan-
        // ordered. The member WITNESS columns (row-type column order =
        // declaration order) become ORDER keys — TRUE-first per member.
        List<TypedFuncCol> orderKeys = new ArrayList<>();
        if (arrayWrap) {
            for (Type.Column c : rowType.columns()) {
                if (com.legend.model.ClassMapping
                        .witnessPrefixOf(c.name()) == null) {
                    continue;
                }
                TypedSpec wread = new TypedPropertyAccess(toRow.apply(null),
                        c.name(), new ExprType(c.type(), c.multiplicity()));
                var wFn = new Type.FunctionType(
                        List.of(new Type.Param(rowType,
                                com.legend.compiler.element.type
                                        .Multiplicity.Bounded.ONE)),
                        new Type.Param(c.type(), c.multiplicity()));
                orderKeys.add(new TypedFuncCol(c.name(),
                        new TypedLambda(List.of(rowVar), List.of(wread),
                                new ExprType(wFn,
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ONE))));
            }
            // an EXPLICIT user sort in the pipeline governs row order —
            // appending PK keys emitted BOTH order specs and the PK one
            // won, silently discarding the user's sortBy (study #23,
            // the near-miss the audit named)
            if (!containsExplicitSort(pipeline)) {
                orderKeys.addAll(pkOrderKeys(cs, pipeline, rowType, rowVar, toRow));
            }
        }
        TypedSerializeGraph node = new TypedSerializeGraph(pipeline, rowVar,
                leaves, children, arrayWrap, false, cs.classFqn(), info,
                false, subTypePatches, orderKeys);
        return checked ? withChecked(node, cs, slotPrefixes, stripped,
                rowVar, rowType, context, toRow) : node;
    }

    /** An explicit {@code sortBy} anywhere in the pipeline (the user's
     *  order, which the graph contract must preserve). */
    private static boolean containsExplicitSort(TypedSpec n) {
        if (n instanceof com.legend.compiler.spec.typed.TypedSortBy) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (containsExplicitSort(c)) {
                return true;
            }
        }
        return false;
    }

    /** ROW-ORDER determinism keys (engine graph contract: rows serialize
     * in the driving table's scan order — H2 preserves it, DuckDB's hash
     * joins do not): the mapped class's PRIMARY-KEY columns, when present
     * on the row, become TRAILING order keys (witness keys stay first —
     * union branch order dominates), marked {@code PK_ORDER_PREFIX} so the
     * lowering treats them ASC and best-effort. */
    private List<TypedFuncCol> pkOrderKeys(ClassSource cs, TypedSpec pipeline,
            Type.RelationType rowType, String rowVar,
            java.util.function.Function<@com.legend.Nullable TypedSpec, TypedSpec> toRow) {
        List<TypedFuncCol> keys = new ArrayList<>();
        for (String pk : RelationalRootForm.primaryKeyColumns(
                cs.classFqn(), pipeline, cs.mappingFqn(), ctx)) {
            Type.Column col = rowType.columns().stream()
                    .filter(c -> c.name().equals(pk)
                            || RelationalRootForm.stripQ(c.name()).equals(pk))
                    .findFirst().orElse(null);
            if (col == null) {
                continue;
            }
            TypedSpec pkRead = new TypedPropertyAccess(toRow.apply(null),
                    col.name(), new ExprType(col.type(), col.multiplicity()));
            var pkFn = new Type.FunctionType(
                    List.of(new Type.Param(rowType,
                            com.legend.compiler.element.type
                                    .Multiplicity.Bounded.ONE)),
                    new Type.Param(col.type(), col.multiplicity()));
            keys.add(new TypedFuncCol(
                    TypedSerializeGraph.PK_ORDER_PREFIX + col.name(),
                    new TypedLambda(List.of(rowVar), List.of(pkRead),
                            new ExprType(pkFn,
                                    com.legend.compiler.element.type
                                            .Multiplicity.Bounded.ONE))));
        }
        return keys;
    }

    private TypedSerializeGraph withChecked(TypedSerializeGraph node,
            ClassSource cs, Map<String, String> slotPrefixes,
            Set<String> stripped, String rowVar, Type.RelationType rowType,
            StoreResolver.Context context, java.util.function.Function<@com.legend.Nullable TypedSpec, TypedSpec> toRow) {
        // CHECKED + ->subType patches: the engine's checked value carries
        // ONLY the subtype projection — base leaves/children drop
        // (RootSubType...Checked golden: {"value":{"coordinate":…,
        // "street":…}}, no Id/landmark; the unchecked run keeps them)
        List<TypedFuncCol> cLeaves = node.subTypePatches().isEmpty()
                ? node.leaves() : List.of();
        List<TypedSerializeGraph.Child> cNested =
                node.subTypePatches().isEmpty() ? node.nested() : List.of();
        return new TypedSerializeGraph(node.source(), node.rowVar(),
                cLeaves, cNested, node.arrayWrap(),
                node.bareValue(), node.classFqn(), node.info(),
                node.inlineChild(), node.subTypePatches(), node.orderKeys(),
                node.typeKeyName(), node.fqTypePath(),
                checkedConstraints(cs, slotPrefixes, stripped, rowVar,
                        rowType, context, toRow));
    }

    /** The CHECKED envelope's constraint set — the class hierarchy's
     * constraints (own first, then supertypes'; the engine's
     * allConstraintsInHierarchy walk), each predicate/message inlined
     * through the row bindings like any leaf. The DEFINER class rides as
     * {@code ruleDefinerPath}; a constraint without {@code ~message}
     * takes the engine's default spelling (flatdata checked goldens). */
    private java.util.List<TypedSerializeGraph.CheckedConstraint>
            checkedConstraints(ClassSource cs,
            Map<String, String> slotPrefixes, Set<String> stripped,
            String rowVar, Type.RelationType rowType,
            StoreResolver.Context context, java.util.function.Function<@com.legend.Nullable TypedSpec, TypedSpec> toRow) {
        java.util.List<TypedSerializeGraph.CheckedConstraint> out =
                new ArrayList<>();
        java.util.ArrayDeque<String> work = new java.util.ArrayDeque<>();
        java.util.Set<String> seen = new java.util.LinkedHashSet<>();
        work.add(cs.classFqn());
        while (!work.isEmpty()) {
            String fqn = work.poll();
            if (!seen.add(fqn)) {
                continue;
            }
            var tc = ctx.findClass(fqn).orElse(null);
            if (tc == null) {
                continue;
            }
            for (var con : tc.constraints()) {
                out.add(new TypedSerializeGraph.CheckedConstraint(con.name(),
                        constraintFnCol("constraint$" + con.name(),
                                con.predicateFqn(), cs, slotPrefixes,
                                stripped, rowVar, rowType, context, toRow),
                        con.messageFqn().isPresent()
                                ? constraintFnCol("constraintMsg$"
                                        + con.name(), con.messageFqn().get(),
                                        cs, slotPrefixes, stripped, rowVar,
                                        rowType, context, toRow)
                                : constMsgCol(con.name(), fqn, rowVar,
                                        rowType),
                        con.level() == com.legend.compiler.element
                                .TypedConstraint.EnforcementLevel.WARN
                                ? "Warn" : "Error",
                        fqn));
            }
            work.addAll(tc.superClassFqns());
        }
        return out;
    }

    /** One constraint body (predicate or message fn) inlined through the
     * bindings and re-pointed at the envelope row. */
    private TypedFuncCol constraintFnCol(String name, String bodyFqn,
            ClassSource cs, Map<String, String> slotPrefixes,
            Set<String> stripped, String rowVar, Type.RelationType rowType,
            StoreResolver.Context context, java.util.function.Function<@com.legend.Nullable TypedSpec, TypedSpec> toRow) {
        var cf = sources.compileSynthFn(bodyFqn);
        String thisVar = cf.signature().parameters().get(0).name();
        TypedSpec body = inlineThis(cf.body().get(cf.body().size() - 1),
                thisVar, java.util.Map.of(), cs.bindings(), cs.classFqn(),
                name, new SubqueryEnv(cs, context, rowVar, rowType));
        body = Pipelines.rewriteRowReads(body, cs.rowVar(), slotPrefixes,
                stripped, toRow::apply);
        return rowFnCol(name, body, rowVar, rowType);
    }

    /** The engine's DEFAULT constraint message (no {@code ~message}). */
    private static TypedFuncCol constMsgCol(String id, String definerFqn,
            String rowVar, Type.RelationType rowType) {
        String simple = definerFqn.substring(
                definerFqn.lastIndexOf(':') + 1);
        TypedSpec msg = new com.legend.compiler.spec.typed.TypedCString(
                "Constraint :[" + id + "] violated in the Class " + simple,
                ExprType.one(Type.Primitive.STRING));
        return rowFnCol("constraintMsg$" + id, msg, rowVar, rowType);
    }

    private static TypedFuncCol rowFnCol(String name, TypedSpec body,
            String rowVar, Type.RelationType rowType) {
        var fn = new Type.FunctionType(
                java.util.List.of(new Type.Param(rowType,
                        com.legend.compiler.element.type.Multiplicity
                                .Bounded.ONE)),
                new Type.Param(body.info().type(),
                        body.info().multiplicity()));
        return new TypedFuncCol(name, new TypedLambda(
                java.util.List.of(rowVar), java.util.List.of(body),
                new ExprType(fn, com.legend.compiler.element.type
                        .Multiplicity.Bounded.ONE)));
    }

    /** A leaf whose body is ALREADY in final row terms (the correlated
     * sub-aggregation emission) — plain lambda wrap, no row-read pass. */
    private TypedFuncCol aggLeaf(String key, TypedSpec body, String rowVar,
            Type.RelationType rowType) {
        var one = com.legend.compiler.element.type.Multiplicity.Bounded.ONE;
        var fnT = new Type.FunctionType(
                List.of(new Type.Param(rowType, one)),
                new Type.Param(body.info().type(), body.info().multiplicity()));
        return new TypedFuncCol(key, new TypedLambda(List.of(rowVar),
                List.of(body), new ExprType(fnT, one)));
    }

    /**
     * Rewrite reads of STRIPPED class-typed nav steps ({@code $row.<alias>})
     * inside a leaf binding to the CORRELATED TARGET RELATION: the step's
     * target ClassSource materialized (empty demand), filtered by the nav
     * condition with the parent-side reads re-pointed at {@code rowVar}.
     * Null when nothing rewrote (unchanged bindings keep their own walls).
     * Temporal targets stay un-rewritten (the per-hop date calculus does
     * not thread here) — their reads keep the loud H4b wall.
     */
    private @com.legend.Nullable TypedSpec correlateStrippedNavReads(TypedSpec n, ClassSource cs,
            String rowVar, Type.RelationType rowType, Set<String> stripped,
            StoreResolver.Context context) {
        var navSteps = Pipelines.outerNavSteps(cs.pipeline());
        boolean[] rewrote = {false};
        TypedSpec out = rewriteNavReads(n, cs, rowVar, rowType, stripped,
                navSteps, context, rewrote);
        if (!rewrote[0]) {
            return null;
        }
        // MIXED leaf (agg + plain column reads): the direct emission
        // bypasses the row-read rewrite, so residual parent-row reads
        // would dangle — keep the loud wall for that shape until demanded
        Set<String> residual = new LinkedHashSet<>();
        Pipelines.collectVarReads(out, cs.rowVar(), residual);
        return residual.isEmpty() ? out : null;
    }

    private TypedSpec rewriteNavReads(TypedSpec n, ClassSource cs,
            String rowVar, Type.RelationType rowType, Set<String> stripped,
            Map<String, TypedNavigate> navSteps,
            StoreResolver.Context context, boolean[] rewrote) {
        if (n instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable v
                && v.name().equals(cs.rowVar())
                && stripped.contains(pa.property())
                && navSteps.get(pa.property()) != null) {
            TypedNavigate st = navSteps.get(pa.property());
            if (!(st.target() instanceof TypedGetAll ng)
                    || st.predicate().parameters().size() != 2
                    || temporal.temporalStrategy(ng.classFqn()) != null) {
                return n;
            }
            ClassSource t = sources.get(
                    dispatch.apply(context, ng.classFqn()), ng.classFqn(),
                    context.constructedScope());
            TypedSpec tPipe = Pipelines.materialize(t.pipeline(),
                    Set.of(), t.classFqn()).pipeline();
            Type.RelationType tRow = Type.requireRelationSchema(tPipe.info().type());
            String pVar = st.predicate().parameters().get(0);
            String tVar = st.predicate().parameters().get(1);
            var one = com.legend.compiler.element.type.Multiplicity.Bounded.ONE;
            List<TypedSpec> corrBody = st.predicate().body().stream().map(x ->
                    Pipelines.rewriteRowReads(x, pVar, Map.of(), Set.of(),
                            v2 -> new TypedVariable(rowVar,
                                    new ExprType(rowType, one)))).toList();
            TypedLambda corr = new TypedLambda(List.of(tVar), corrBody,
                    new ExprType(new Type.FunctionType(
                            List.of(new Type.Param(tRow, one)),
                            new Type.Param(Type.Primitive.BOOLEAN, one)), one));
            rewrote[0] = true;
            // CORRELATION stamp = the HONEST provenance (the deleted
            // toOneJoinEquals fake-[1] wraps existed only to defeat the
            // null-safe equality arm; the lowering now enters the
            // verbatim-equality scope on this stamp — C2,
            // STAMP_DISCIPLINE_PROGRAM)
            return new TypedFilter(tPipe, corr,
                    new ExprType(Type.relation(tRow), one),
                    TypedFilter.Stamp.CORRELATION);
        }
        if (n instanceof TypedNativeCall c) {
            List<TypedSpec> args = c.args().stream().map(a ->
                    rewriteNavReads(a, cs, rowVar, rowType, stripped,
                            navSteps, context, rewrote)).toList();
            return c.withChildren(args);
        }
        return n;
    }

    /**
     * A TO-MANY PRIMITIVE leaf as a BARE-VALUE child: the navigate slot's
     * target relation, correlated on the parent row exactly like
     * {@link #correlatedGraphChild}, aggregating the single column's raw
     * values (TypedSerializeGraph.bareValue).
     */
    private TypedSerializeGraph.Child primitiveArrayChild(String property,
            TypedSpec targetPipeline, TypedLambda cond,
            TypedPropertyAccess colRead,
            String parentRowVar, Type.RelationType parentRowType) {
        return primitiveArrayChild(property, targetPipeline, cond, colRead,
                parentRowVar, parentRowType,
                java.util.function.UnaryOperator.identity());
    }

    /** A TO-MANY PRIMITIVE leaf ($row.<slot>.COL through a to-many
     * navigate/join slot): reading it as a joined scalar EXPLODES the
     * parent rows — the engine serializes ["abc","def"] per parent. The
     * DECLARED property multiplicity is the guard (the raw read types
     * per COLUMN [0..1], masking the property's [*] — same rule as the
     * whole-source path). Null when the shape does not match. */
    private TypedSerializeGraph.@com.legend.Nullable Child primitiveArrayLeaf(ClassSource cs,
            com.legend.compiler.spec.typed.TypedGraphTree node,
            TypedSpec inner, String rowVar, Type.RelationType rowType) {
        var declLeaf = ctx.findProperty(cs.classFqn(), node.property())
                .orElse(null);
        boolean declaredMany = declLeaf != null
                && !(declLeaf.multiplicity()
                        instanceof com.legend.compiler.element.type
                                .Multiplicity.Bounded db1
                        && Integer.valueOf(1).equals(db1.upper()));
        // M2M MILESTONED leaf chain (synonymNames:
        // \$src.<nav>AllVersions.synonym / <nav>(%date).synonym): the
        // hop's temporal spec windows the nav target (a SWEEP serves the
        // RAW extent, a DATED hop stamps) — same primitive-array
        // emission over the resolved target pipeline
        if (declaredMany && inner instanceof TypedPropertyAccess mcp
                && mcp.source() instanceof com.legend.compiler.spec.typed
                        .TypedMilestonedAccess mma
                && mma.source() instanceof TypedVariable mmv
                && mmv.name().equals(cs.rowVar())) {
            var mNavP = Pipelines.outerNavSteps(cs.pipeline())
                    .get(mma.property());
            if (mNavP != null
                    && mNavP.target() instanceof com.legend.compiler.spec
                            .typed.TypedGetAll mtg
                    && sources.binds(cs.mappingFqn(), mtg.classFqn())) {
                ClassSource tcs = sources.get(cs.mappingFqn(), mtg.classFqn(), cs.scope());
                TemporalFrame tf2 = temporal.withSpecs(java.util.Map.of(
                        mma.property(), new TemporalFrame.TemporalSpec(
                                temporal.normalizeContextDates(mma.dates()),
                                mma.sweep())));
                TypedSpec tPipe = tf2.temporalTargetPipe(cs, tcs,
                        mma.property(),
                        Pipelines.materialize(tcs.pipeline(),
                                java.util.Set.of(), mtg.classFqn())
                                .pipeline());
                TypedSpec hopVar = new TypedPropertyAccess(mma.source(),
                        mma.property(), mma.info());
                return primitiveArrayChild(keyOf(node), tPipe,
                        mNavP.predicate(),
                        new TypedPropertyAccess(hopVar, mcp.property(),
                                mcp.info()),
                        rowVar, rowType);
            }
        }
        if (!(inner instanceof TypedPropertyAccess colPa
                && colPa.source() instanceof TypedPropertyAccess slotPa
                && slotPa.source() instanceof TypedVariable slotV
                && slotV.name().equals(cs.rowVar())
                && (declaredMany
                        || !(colPa.info().multiplicity()
                                instanceof com.legend.compiler.element.type
                                        .Multiplicity.Bounded b1
                                && Integer.valueOf(1).equals(b1.upper()))))) {
            return null;
        }
        var navPrim = Pipelines.outerNavSteps(cs.pipeline())
                .get(slotPa.property());
        if (navPrim != null) {
            return primitiveArrayChild(keyOf(node), navPrim.target(),
                    navPrim.predicate(), colPa, rowVar, rowType);
        }
        var slotPrim = Pipelines.joinSlots(cs.pipeline())
                .get(slotPa.property());
        if (slotPrim != null) {
            return primitiveArrayChild(keyOf(node), slotPrim.target(),
                    slotPrim.condition(), colPa, rowVar, rowType);
        }
        return null;
    }

    /** {@code valueWrap} rebuilds any per-element wrapper calls
     * (toString/datePart chains) over the correlated child-row read. */
    private TypedSerializeGraph.Child primitiveArrayChild(String property,
            TypedSpec targetPipeline, TypedLambda cond,
            TypedPropertyAccess colRead,
            String parentRowVar, Type.RelationType parentRowType,
            java.util.function.UnaryOperator<TypedSpec> valueWrap) {
        Type.RelationType targetRow =
                Type.requireRelationSchema(targetPipeline.info().type());
        String pVar = cond.parameters().get(0);
        String tVar = cond.parameters().get(1);
        List<TypedSpec> corrBody = cond.body().stream().map(x ->
                Pipelines.rewriteRowReads(x, pVar, Map.of(), Set.of(),
                        v -> new TypedVariable(parentRowVar,
                                new ExprType(parentRowType,
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ONE))))
                .toList();
        TypedLambda corr = new TypedLambda(List.of(tVar), corrBody,
                new ExprType(
                        new Type.FunctionType(
                                List.of(new Type.Param(targetRow,
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ONE)),
                                new Type.Param(Type.Primitive.BOOLEAN,
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ONE)),
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
        TypedSpec childRel = new TypedFilter(targetPipeline, corr,
                targetPipeline.info(), TypedFilter.Stamp.CORRELATION);
        // same freshness guard as correlatedGraphChild (audit 23 #75 —
        // this mint lacked it): bump past any name the child value or
        // the correlation could capture
        Set<String> pcParams = new LinkedHashSet<>();
        PipelineWalks.collectLambdaParams(colRead, pcParams);
        pcParams.addAll(corr.parameters());
        String childVar;
        do {
            childVar = "_r" + freshVar.getAsInt();
        } while (pcParams.contains(childVar));
        TypedSpec value = valueWrap.apply(new TypedPropertyAccess(
                new TypedVariable(childVar,
                        new ExprType(targetRow,
                                com.legend.compiler.element.type
                                        .Multiplicity.Bounded.ONE)),
                colRead.property(), colRead.info()));
        var fnType = new Type.FunctionType(
                List.of(new Type.Param(targetRow,
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE)),
                new Type.Param(value.info().type(), value.info().multiplicity()));
        TypedFuncCol leaf = new TypedFuncCol(property,
                new TypedLambda(List.of(childVar), List.of(value),
                        new ExprType(fnType,
                                com.legend.compiler.element.type
                                        .Multiplicity.Bounded.ONE)));
        TypedSerializeGraph node = new TypedSerializeGraph(childRel, childVar,
                List.of(leaf), List.of(), /*arrayWrap*/ true, /*bareValue*/ true,
                colRead.info());
        return new TypedSerializeGraph.Child(property, node);
    }

    /** One nested hop: correlated child pipeline + the child's own envelope. */
    TypedSerializeGraph.Child graphChild(ClassSource cs, TypedGraphTree node,
            StoreResolver.Context context, String parentRowVar,
            Type.RelationType parentRowType, TypedSpec parentPipeline) {
        if (node.children().isEmpty()) {
            // a childless DERIVED class node still serializes (its filter
            // may make it empty — synonymsByTypes([]) yields [] per root)
            TypedSerializeGraph.Child dch0 = derivedChild(cs, node, context,
                    parentRowVar, parentRowType);
            if (dch0 != null) {
                return dch0;
            }
            // IMPLICIT CHILD EXPANSION: a childless CLASS-typed leaf
            // serializes the FULL nested object (engine) — synthesize the
            // child tree from the child class's scalar/enum model
            // properties and re-enter (routes handle demand; unmapped
            // optional scalars ride the [0..1]-gen arm)
            java.util.List<TypedGraphTree> implicit = implicitLeaves(cs,
                    node, context);
            if (implicit != null) {
                return graphChild(cs, new TypedGraphTree(node.property(),
                        implicit, node.alias(), node.args(), node.sweep(),
                        node.subTypeFqn(), node.qualified()),
                        context, parentRowVar, parentRowType, parentPipeline);
            }
            throw new NotImplementedException("graph child '" + node.property()
                    + "' of class '" + cs.classFqn() + "' has no sub-tree — a"
                    + " class-typed leaf serializes nothing; list its properties");
        }
        // A BINDING-backed head: the M2M SOURCE-NAV MARKER ($src.assocProp,
        // re-pointed at the composed row var by ClassSources) fans out as a
        // correlated child through the UPSTREAM association; every other
        // binding kind (embedded ctor, navigate slot, otherwise) stays loud.
        if (cs.bindings().containsKey(node.property())) {
            TypedSpec b0 = cs.bindings().get(node.property());
            TypedSpec inner = b0;
            TypedNewInstanceCast srcCast = null;
            // Unwrap the M2M cast (^Target($src.assocProp)) and toOne.
            if (inner instanceof TypedNewInstanceCast nic) {
                srcCast = nic;
                inner = nic.source();
            }
            if (inner instanceof TypedNativeCall c1
                    && c1.args().size() == 1
                    && com.legend.builtin.Pure.isToOneCall(c1.callee().qualifiedName())) {
                inner = c1.args().get(0);
            }
            if (inner instanceof TypedNewInstanceCast nic2) {
                srcCast = nic2;
                inner = nic2.source();
            }
            // DATED fetch over a NON-TEMPORAL channel: [] at any date
            var edc = datedNonTemporalChild(cs, node, inner, context,
                    parentRowVar, parentRowType);
            if (edc != null) {
                return edc;
            }
            if (inner instanceof TypedPropertyAccess pa
                    && pa.source() instanceof TypedVariable v
                    && v.name().equals(cs.rowVar())
                    && v.info().type() instanceof Type.ClassType srcCls
                    && ctx.findAssociationOf(srcCls.fqn(), pa.property()).isPresent()) {
                return m2mAssocChild(cs, node, srcCls.fqn(), pa.property(),
                        context, parentRowVar, parentRowType);
            }
            // M2M MILESTONED-NAV marker ($src.<nav>(d)/<nav>AllVersions
            // composed as the temporal step over the source-class
            // marker): the assoc-marker dispatch with the hop's temporal
            // spec riding the frame (dated head / version sweep)
            if (inner instanceof com.legend.compiler.spec.typed
                    .TypedMilestonedAccess ma0
                    && ma0.source() instanceof TypedVariable mv0
                    && mv0.name().equals(cs.rowVar())
                    && mv0.info().type() instanceof Type.ClassType mSrc) {
                // the TREE NODE's own temporal spec (synonymsMilestoned(
                // %date){…}) WINDOWS the child and beats the binding's
                // sweep — the mapping populates all versions, the fetch
                // date narrows (engine serialize-tree date semantics)
                boolean nodeTemporal = !node.args().isEmpty()
                        || node.sweep();
                GraphEmission em0 = new GraphEmission(ctx, sources,
                        assocMaterial,
                        temporal.withSpecs(java.util.Map.of(ma0.property(),
                                nodeTemporal
                                        ? new TemporalFrame.TemporalSpec(
                                                temporal.normalizeContextDates(
                                                        node.args()),
                                                node.sweep())
                                        : new TemporalFrame.TemporalSpec(
                                                temporal.normalizeContextDates(
                                                        ma0.dates()),
                                                ma0.sweep()))),
                        dispatch, freshVar);
                if (ctx.findAssociationOf(mSrc.fqn(), ma0.property())
                        .isPresent()) {
                    return em0.m2mAssocChild(cs, node, mSrc.fqn(),
                            ma0.property(), context, parentRowVar,
                            parentRowType);
                }
                // PLAIN class-typed property of the source: the composed
                // pipeline's nav step carries the target + join predicate
                var mNav = Pipelines.outerNavSteps(cs.pipeline())
                        .get(ma0.property());
                if (mNav != null) {
                    return em0.navSlotChild(cs, node, mNav,
                            b0 instanceof TypedNewInstanceCast nicM
                                    ? nicM.classFqn() : null,
                            context, parentRowVar, parentRowType);
                }
            }
            // WHOLE-SOURCE marker (trader[trader_set]: $src): the bare
            // composed-row var, source-class-typed — the child is the SAME
            // row seen through the cast's set; inline, no join (XStore
            // route A, whole-$src edit).
            if (inner instanceof TypedVariable wv
                    && wv.name().equals(cs.rowVar())
                    && wv.info().type() instanceof Type.ClassType wSrc
                    && srcCast != null) {
                return wholeSrcChild(cs, node, srcCast, wSrc.fqn(), context,
                        parentPipeline);
            }
            // A NAVIGATE-SLOT read ($row.<alias>, the relational
            // association injected into the source pipeline): the slot's
            // TypedNavigate carries the raw target and the join predicate.
            if (inner instanceof TypedPropertyAccess pa2
                    && pa2.source() instanceof TypedVariable v2
                    && v2.name().equals(cs.rowVar())) {
                var navSteps = Pipelines.outerNavSteps(cs.pipeline());
                var nav = navSteps.get(pa2.property());
                if (nav != null) {
                    return navSlotChild(cs, node, nav,
                            b0 instanceof TypedNewInstanceCast nic0
                                    ? nic0.classFqn() : null,
                            context, parentRowVar, parentRowType);
                }
            }
            // EMBEDDED child: the ^Inner ctor's bindings read the PARENT
            // row — an INLINE json object, no join, no subquery (V1 §D.4
            // semantics at the graph envelope; task #78 H4b slice 1).
            // otherwise() takes the embedded partial: per-leaf FK dispatch
            // at graph depth is its own rung, the partial serves the
            // mapped leaves and missing ones stay loud below.
            TypedSpec emb = inner;
            var ow2 = com.legend.resolver.Substitution.otherwiseOf(b0);
            if (ow2 != null) {
                // GRAPHFETCH ALWAYS TAKES THE FK FALLBACK (V1 §D.5; corpus
                // testOtherwiseEmbeddedMapping: description serializes the
                // fallback table's NOT_SO_GOOD_DETAIL 'P 1', NOT the
                // partial's parent column 'Bond 1') — the whole child
                // routes through the fallback's navigate; the partial
                // serves QUERY-position reads only.
                TypedSpec fb = ow2.args().get(1);
                while (fb instanceof TypedNativeCall fw && fw.args().size() == 1
                        && (com.legend.builtin.Pure.isToOneCall(fw.callee().qualifiedName())
                            || fw.callee().qualifiedName().equals(
                                "meta::pure::functions::collection::first"))) {
                    fb = fw.args().get(0);
                }
                if (fb instanceof TypedPropertyAccess fpa
                        && fpa.source() instanceof TypedVariable fv
                        && fv.name().equals(cs.rowVar())) {
                    var owNav = Pipelines.outerNavSteps(cs.pipeline())
                            .get(fpa.property());
                    if (owNav != null) {
                        return navSlotChild(cs, node, owNav, null,
                                context, parentRowVar, parentRowType);
                    }
                }
                emb = ow2.args().get(0);
            }
            if (emb instanceof TypedNewInstance ctor2) {
                return embeddedChild(cs, node, ctor2, context, parentPipeline,
                        ow2 == null ? null : ow2.args().get(1));
            }
            throw new NotImplementedException("graph child '" + node.property()
                    + "' of class '" + cs.classFqn() + "' is mapped as an"
                    + " embedded/join-slot/otherwise/M2M binding — only"
                    + " association children are supported yet (H4b/H5c)");
        }
        // DERIVED (qualified) child: the inline body's nav head + filter
        TypedSerializeGraph.Child dch = derivedChild(cs, node, context,
                parentRowVar, parentRowType);
        if (dch != null) {
            return dch;
        }
        // MIXED-UNION extent: class-typed children dispatch PER MEMBER ARM —
        // ONE correlated subquery over the keyed child union with the
        // OR-of-member-keys condition (route b, XSTORE_LEG design)
        if ("union".equals(cs.setId())) {
            TypedSerializeGraph.Child mcc = mixedUnionChild(cs, node, context,
                    parentRowVar, parentRowType);
            if (mcc != null) {
                return mcc;
            }
        }
        // the child tree's LEAF property names ride as demanded leaves —
        // a leaf mapped through the target's own join slots needs them
        // CONVERTED, not stripped (H4b; same rule as leafSlotDemand)
        java.util.Set<String> childLeaves = new java.util.LinkedHashSet<>();
        for (TypedGraphTree c0 : node.children()) {
            childLeaves.add(c0.property());
        }
        AssociationJoins.AssocJoin aj = assocMaterial.associationJoin(temporal,
                cs, node.property(), context, /*forExists*/ true, childLeaves);
        var assoc = ctx.findAssociationOf(cs.classFqn(), node.property()).orElseThrow();
        var end = assoc.property1().propertyName().equals(node.property())
                ? assoc.property1() : assoc.property2();
        // a version SWEEP (<base>AllVersions) is MANY regardless of the
        // base end's cardinality — every version row serializes
        boolean toMany = !end.isToOne() || node.sweep();
        return correlatedGraphChild(aj.target(), aj.targetPipeline(), aj.targetRow(),
                java.util.Objects.requireNonNull(aj.condition()), toMany, node, parentRowVar, parentRowType,
                context, aj.targetSlotPrefixes());
    }

    /**
     * A graph child through a NAVIGATE SLOT: the slot's raw target composes
     * into the child's declared (possibly M2M) class; the slot predicate
     * λ(sourceRow, targetRow) correlates them.
     */
    TypedSerializeGraph.Child navSlotChild(ClassSource cs, TypedGraphTree node,
            TypedNavigate nav, @com.legend.Nullable String castClassFqn,
            StoreResolver.Context context, String parentRowVar,
            Type.RelationType parentRowType) {
        return navSlotChild(cs, cs.classFqn(), node, nav, castClassFqn,
                context, parentRowVar, parentRowType);
    }

    /** {@code ownerClassFqn}: the class DECLARING the child property —
     * the source class itself, or the EMBEDDED class when the slot sits
     * inside an embedded ctor (the pipeline/row stay the source's). */
    TypedSerializeGraph.Child navSlotChild(ClassSource cs,
            String ownerClassFqn, TypedGraphTree node,
            TypedNavigate nav, @com.legend.Nullable String castClassFqn,
            StoreResolver.Context context, String parentRowVar,
            Type.RelationType parentRowType) {
        String key = (context.explicitMapping() == null ? "" : context.explicitMapping())
                + '\u0000'
                + (context.runtimeFqn() == null ? "" : context.runtimeFqn());
        String rawTarget = ((TypedGetAll) nav.target()).classFqn();
        var prop = ctx.findProperty(ownerClassFqn, node.property()).orElseThrow(
                () -> new IllegalStateException("resolver bug: graph child '"
                        + node.property() + "' is not a property of '"
                        + ownerClassFqn + "'"));
        String childClass = castClassFqn != null ? castClassFqn
                : prop.type() instanceof Type.ClassType cc
                        ? cc.fqn() : null;
        if (childClass == null) {
            throw new IllegalStateException("resolver bug: navigate-slot graph child '"
                    + node.property() + "' is not class-typed");
        }
        boolean toMany = node.sweep() || !(prop.multiplicity()
                instanceof com.legend.compiler.element.type.Multiplicity.Bounded bm
                && Integer.valueOf(1).equals(bm.upper()));
        String setHint = ctx.routedTargetSetOf(cs.mappingFqn(),
                node.property()).orElse(null);
        ClassSource child = childClass.equals(rawTarget)
                ? sources.get(dispatch.apply(context, rawTarget), rawTarget,
                        setHint, (target, excl) -> dispatch.apply(context, target), key,
                        cs.scope())
                : sources.get(cs.mappingFqn(), childClass, setHint,
                        (target, excl) -> dispatch.apply(context, target), key,
                        cs.scope());
        // The slot predicate's right side reads the RAW TARGET's physical
        // columns — the child's composed pipeline must bottom at that same
        // row or the correlation filters the wrong relation (audit: the
        // m2mAssocChild guard, applied to this sibling too).
        if (!childClass.equals(rawTarget)) {
            ClassSource rawSource = sources.get(dispatch.apply(context, rawTarget), rawTarget,
                    (target, excl) -> dispatch.apply(context, target), key, cs.scope());
            if (!child.rowVar().equals(rawSource.rowVar())) {
                throw new NotImplementedException("navigate-slot graph child '"
                        + node.property() + "': the child class '" + childClass
                        + "' does not compose the slot target '" + rawTarget
                        + "' — cross-source children are not supported yet");
            }
        }
        // demand CLOSES over join-condition prerequisites: a chained
        // composite slot (A__B) rides its base slot (A) — stripping the
        // base breaks the composite (same rule as the flatten route)
        Pipelines.Materialized cMat = Pipelines.materialize(
                child.pipeline(),
                Pipelines.closeOverConditions(child.pipeline(),
                        leafSlotDemand(child, node)),
                childClass);
        // MILESTONED SLOTS inside the child: the hop's date context
        // windows the child's own slot-join targets (mirror of the
        // relational navigate's slot-target stamping) — unstamped they
        // serialize every version row (the classification fan)
        TypedSpec cPipe = cMat.pipeline();
        TemporalContext hopCtx = temporal.contextAt(node.property(),
                childClass, TemporalContext.NONE);
        if (!hopCtx.isEmpty()
                && temporal.hasMilestonedSlotTarget(child.pipeline())) {
            cPipe = temporal.filterMilestonedJoinTargets(cPipe, hopCtx);
        }
        // MILESTONED child: the tree node's date arg registered as the
        // hop's temporal spec (collectTreeSweeps) — the child pipeline
        // filters by its window here, exactly like the relational
        // navigate's target (unfiltered children serialize every version
        // row: the multi-level union 2->4 duplication)
        TypedSpec childPipe = temporal.temporalTargetPipe(cs, child,
                node.property(), cPipe);
        // GRAPH children pair STRICTLY per union member (the engine's
        // graph executor runs per-member serial child queries) — a merged
        // union navigate carries the paired variant for exactly this
        // consumer (TypedNavigate.pairedPredicate)
        TypedLambda navCond = nav.pairedPredicate().orElse(nav.predicate());
        // COMPOSITE chain (bridge-table hop: employees: @A > @B): the
        // condition reads a SIBLING joinslot of the parent — pull the
        // bridge INTO the child pipeline, correlated outward by hop-1's
        // condition (same reorientation as the exists route, #70)
        if (navCond.parameters().size() == 2) {
            CorrelatedSubselects.CompositeChain cc =
                    new CorrelatedSubselects(sources, assocMaterial)
                            .compositeChainTarget(cs, navCond, childPipe);
            if (cc != null) {
                childPipe = cc.pipeline();
                navCond = cc.orientedCond();
            }
        }
        return correlatedGraphChild(child, childPipe,
                Type.requireRelationSchema(childPipe.info().type()),
                navCond,
                toMany, node, parentRowVar, parentRowType, context,
                cMat.slotPrefixes());
    }

    /**
     * An M2M child through the SOURCE class's association: the upstream
     * association supplies the condition and the RAW target; the child
     * serialized is the node property's DECLARED M2M class, whose composed
     * pipeline bottoms at that same raw target row (M2M composition
     * preserves the inner pipeline and row var — the correlation aligns by
     * construction, asserted loudly).
     */
    /** DATED fetch over a NON-TEMPORAL channel (nonMilestonedSource ->
     * milestoned target): the produced instances carry NO business
     * validity — the engine serializes [] at any date (the AllVersions
     * fetch still serves the rows). The child emits with a FALSE
     * correlation; null when not this shape. */
    private TypedSerializeGraph.@com.legend.Nullable Child
            datedNonTemporalChild(ClassSource cs, TypedGraphTree node,
            TypedSpec inner, StoreResolver.Context context,
            String parentRowVar, Type.RelationType parentRowType) {
        if (!(!node.args().isEmpty() && !node.sweep()
                    && inner instanceof TypedPropertyAccess pdt
                    && pdt.source() instanceof TypedVariable pvt
                    && pvt.name().equals(cs.rowVar())
                    && pdt.info().type() instanceof Type.ClassType pdc
                    && com.legend.compiler.element.Temporal.strategyOf(
                            ctx, pdc.fqn()) == null
                    && ctx.findProperty(cs.classFqn(), node.property())
                            .map(pp -> pp.type()
                                    instanceof Type.ClassType tcc
                                    && com.legend.compiler.element.Temporal
                                            .strategyOf(ctx, tcc.fqn())
                                            != null)
                            .orElse(false))) {
            return null;
        }
                var declP = ctx.findProperty(cs.classFqn(), node.property())
                        .orElseThrow();
                String ccFqn = ((Type.ClassType) declP.type()).fqn();
                String key0 = (context.explicitMapping() == null ? ""
                        : context.explicitMapping()) + '\u0000'
                        + (context.runtimeFqn() == null ? ""
                                : context.runtimeFqn());
                ClassSource ecs = sources.get(
                        dispatch.apply(context, ccFqn), ccFqn,
                        (t2, ex2) -> dispatch.apply(context, t2), key0,
                        context.constructedScope());
                Pipelines.Materialized eMat = Pipelines.materialize(
                        ecs.pipeline(), java.util.Set.of(), ccFqn);
                var one0 = com.legend.compiler.element.type
                        .Multiplicity.Bounded.ONE;
                Type.RelationType eRow = Type.requireRelationSchema(eMat.pipeline().info().type());
                TypedLambda never = new TypedLambda(
                        java.util.List.of("p_ec", "t_ec"),
                        java.util.List.of(new com.legend.compiler.spec.typed
                                .TypedCBoolean(false,
                                        new ExprType(Type.Primitive.BOOLEAN,
                                                one0))),
                        new ExprType(new Type.FunctionType(
                                java.util.List.of(
                                        new Type.Param(parentRowType, one0),
                                        new Type.Param(eRow, one0)),
                                new Type.Param(Type.Primitive.BOOLEAN, one0)),
                                one0));
                boolean eMany = !(declP.multiplicity()
                        instanceof com.legend.compiler.element.type
                                .Multiplicity.Bounded eb
                        && Integer.valueOf(1).equals(eb.upper()));
                return correlatedGraphChild(ecs, eMat.pipeline(), eRow,
                        never, eMany, node, parentRowVar, parentRowType,
                        context, eMat.slotPrefixes());
    }

    TypedSerializeGraph.Child m2mAssocChild(ClassSource cs, TypedGraphTree node,
            String srcClassFqn, String assocProp, StoreResolver.Context context,
            String parentRowVar,
            Type.RelationType parentRowType) {
        String key = (context.explicitMapping() == null ? "" : context.explicitMapping())
                + '\u0000'
                + (context.runtimeFqn() == null ? "" : context.runtimeFqn());
        ClassSource rawParent = sources.get(dispatch.apply(context, srcClassFqn), srcClassFqn,
                (target, excl) -> dispatch.apply(context, target), key, cs.scope());
        AssociationJoins.AssocJoin aj = assocMaterial.associationJoin(temporal, rawParent, assocProp, context, /*forExists*/ true);
        var prop = ctx.findProperty(cs.classFqn(), node.property()).orElseThrow(
                () -> new IllegalStateException("resolver bug: graph child '"
                        + node.property() + "' is not a property of '"
                        + cs.classFqn() + "'"));
        // Cardinality from the DECLARED property — the spec the consumer
        // typed against — not the source association's end (consistent with
        // navSlotChild; audit).
        boolean toMany = node.sweep() || !(prop.multiplicity()
                instanceof com.legend.compiler.element.type.Multiplicity.Bounded bm
                && Integer.valueOf(1).equals(bm.upper()));
        if (!(prop.type() instanceof Type.ClassType childCls)) {
            throw new IllegalStateException("resolver bug: M2M graph child '"
                    + node.property() + "' is not class-typed");
        }
        ClassSource child = sources.get(cs.mappingFqn(), childCls.fqn(),
                (target, excl) -> dispatch.apply(context, target), key, cs.scope());
        if (!child.rowVar().equals(aj.target().rowVar())) {
            throw new NotImplementedException("M2M graph child '" + node.property()
                    + "': the child class '" + childCls.fqn() + "' does not compose"
                    + " the association target '" + aj.target().classFqn()
                    + "' — cross-source M2M children are not supported yet");
        }
        Pipelines.Materialized cMat = Pipelines.materialize(
                child.pipeline(), leafSlotDemand(child, node), childCls.fqn());
        return correlatedGraphChild(child, cMat.pipeline(),
                Type.requireRelationSchema(cMat.pipeline().info().type()),
                java.util.Objects.requireNonNull(aj.condition()), toMany, node, parentRowVar, parentRowType,
                context, cMat.slotPrefixes());
    }

    /**
     * The correlated-child tail shared by ASSOCIATION children and M2M
     * source-association children: the condition λ(parent, target) frees its
     * parent reads onto the enclosing row var, filters the target pipeline,
     * and the child's own envelope builds beneath.
     */
    TypedSerializeGraph.Child correlatedGraphChild(ClassSource target,
            TypedSpec targetPipeline,
            Type.RelationType targetRow,
            TypedLambda condition, boolean toMany, TypedGraphTree node,
            String parentRowVar,
            Type.RelationType parentRowType,
            StoreResolver.Context context) {
        return correlatedGraphChild(target, targetPipeline, targetRow,
                condition, toMany, node, parentRowVar, parentRowType,
                context, Map.of());
    }

    /** {@code slotPrefixes}: the child pipeline's CONVERTED slot aliases
     * (from its materialization) — leaves reading through them rewrite to
     * the prefixed flat columns instead of walling as stripped (H4b:
     * ViewAtChild's pnl reads Order's own @OrderPnlView join slot, which
     * leafSlotDemand had the materialization convert). */
    TypedSerializeGraph.Child correlatedGraphChild(ClassSource target,
            TypedSpec targetPipeline,
            Type.RelationType targetRow,
            TypedLambda condition, boolean toMany, TypedGraphTree node,
            String parentRowVar,
            Type.RelationType parentRowType,
            StoreResolver.Context context,
            Map<String, String> slotPrefixes) {
        // The association condition λ(parent, target): parent reads become
        // the FREE parent row var (the lowerer's enclosing-scope channel);
        // the target param stays as the child filter's own row.
        TypedLambda cond = condition;
        String pVar = cond.parameters().get(0);
        String tVar = cond.parameters().get(1);
        List<TypedSpec> corrBody = cond.body().stream().map(b ->
                Pipelines.rewriteRowReads(b, pVar, Map.of(), Set.of(),
                        v -> new TypedVariable(parentRowVar,
                                new ExprType(parentRowType,
                                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE))))
                .toList();
        TypedLambda corr = new TypedLambda(List.of(tVar), corrBody,
                new ExprType(
                        new Type.FunctionType(
                                List.of(new Type.Param(
                                        targetRow,
                                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE)),
                                new Type.Param(
                                        Type.Primitive.BOOLEAN,
                                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE)),
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
        TypedSpec childRel = new TypedFilter(targetPipeline, corr,
                targetPipeline.info(), TypedFilter.Stamp.CORRELATION);
        return childFromRel(target, childRel,
                new LinkedHashSet<>(cond.parameters()), toMany, node,
                context, slotPrefixes);
    }

    /** The child-node tail shared by the correlated and DERIVED routes:
     * fresh child row var (capture-proof against {@code extraParams} and
     * the target's own lambda params), nested temporal frame (ONE CONTEXT
     * PER CURSOR — engine getMilestoningContextFor QualifiedProperty),
     * recursive node build over the corr-filtered relation. */
    private TypedSerializeGraph.Child childFromRel(ClassSource target,
            TypedSpec childRel, Set<String> extraParams, boolean toMany,
            TypedGraphTree node, StoreResolver.Context context,
            Map<String, String> slotPrefixes) {
        Set<String> childParams = new LinkedHashSet<>();
        for (TypedSpec b : target.bindings().values()) {
            PipelineWalks.collectLambdaParams(b, childParams);
        }
        childParams.addAll(extraParams);
        String childVar;
        do {
            childVar = "_r" + freshVar.getAsInt();
        } while (childParams.contains(childVar));
        var childInfo = new ExprType(
                new Type.ClassType(target.classFqn()),
                toMany ? com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_MANY
                        : com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_ONE);
        TemporalFrame childFrame = temporal.nestedFrame(target.classFqn(),
                node.property());
        GraphEmission em = childFrame == null ? this
                : new GraphEmission(ctx, sources, assocMaterial, childFrame,
                        dispatch, freshVar);
        Set<String> childStripped = new LinkedHashSet<>(
                Pipelines.slotAliases(target.pipeline()));
        childStripped.removeAll(slotPrefixes.keySet());
        TypedSerializeGraph child = em.buildGraphNode(target, childRel,
                slotPrefixes, childStripped, childVar,
                node.children(), context, toMany, childInfo);
        return new TypedSerializeGraph.Child(
                childKey(node, target.classFqn()), child);
    }

    /** A DERIVED (qualified) property as a graph CHILD
     * ({@code synonymByType(CUSIP) { name }}): the lifted body's
     * navigation head resolves like any association hop
     * ({@link #navHeadRelation}); the body's own filter predicate rides
     * the child relation, substituted through the TARGET's bindings; tree
     * args bind positionally after {@code $this}. Null when the property
     * is not derived or the body is not a [filtered] nav head — the
     * association walls stay downstream. */
    /** A DERIVED class-typed tree node with matching call arity — routes
     * through {@link #graphChild} even childless. */
    /** A childless CLASS-typed PLAIN property (slot/assoc-backed, not
     * derived) — routes to graphChild for implicit expansion. */
    private boolean isPlainClassProp(ClassSource cs, TypedGraphTree node) {
        return node.children().isEmpty()
                && ctx.findProperty(cs.classFqn(), node.property())
                        .map(p -> !(p instanceof com.legend.compiler
                                .element.Property.Derived)
                                && p.type() instanceof Type.ClassType)
                        .orElse(false);
    }

    /** The implicit full tree of a childless class-typed leaf: the child
     * class's primitive/enum-typed properties as leaves; null when the
     * child class is unresolvable. */
    private java.util.@com.legend.Nullable List<TypedGraphTree>
            implicitLeaves(ClassSource cs, TypedGraphTree node,
                    StoreResolver.Context context) {
        var p = ctx.findProperty(cs.classFqn(), node.property()).orElse(null);
        if (p == null || !(p.type() instanceof Type.ClassType ct)) {
            return null;
        }
        var cls = ctx.findClass(ct.fqn()).orElse(null);
        if (cls == null) {
            return null;
        }
        // MAPPED properties only (the engine's implicit tree): filter
        // through the child source's bindings
        java.util.Set<String> mapped;
        try {
            String key = (context.explicitMapping() == null ? ""
                    : context.explicitMapping()) + '\u0000'
                    + (context.runtimeFqn() == null ? ""
                            : context.runtimeFqn());
            mapped = sources.get(dispatch.apply(context, ct.fqn()),
                    ct.fqn(), (t, ex) -> dispatch.apply(context, t), key,
                    context.constructedScope())
                    .bindings().keySet();
        } catch (RuntimeException unresolvable) {
            return null;
        }
        java.util.List<TypedGraphTree> out = new ArrayList<>();
        for (var cp : cls.properties()) {
            if ((cp.type() instanceof Type.Primitive
                    || cp.type() instanceof Type.EnumType)
                    && mapped.contains(cp.name())) {
                out.add(new TypedGraphTree(cp.name(), List.of()));
            }
        }
        return out.isEmpty() ? null : out;
    }

    private boolean isDerivedClassProp(ClassSource cs, TypedGraphTree node) {
        return ctx.findProperty(cs.classFqn(), node.property()).orElse(null)
                instanceof com.legend.compiler.element.Property.Derived d
                && d.type() instanceof Type.ClassType
                && d.parameters().size() == node.args().size();
    }

    private TypedSerializeGraph.@com.legend.Nullable Child derivedChild(ClassSource cs,
            TypedGraphTree node, StoreResolver.Context context,
            String parentRowVar, Type.RelationType parentRowType) {
        var p = ctx.findProperty(cs.classFqn(), node.property()).orElse(null);
        if (!(p instanceof com.legend.compiler.element.Property.Derived d)
                || d.parameters().size() != node.args().size()) {
            return null;
        }
        var cf = sources.compileSynthFn(d.bodyFunctionFqn());
        if (cf.body().size() != 1) {
            return null;
        }
        String thisVar = cf.signature().parameters().get(0).name();
        Map<String, TypedSpec> argBinds = new java.util.LinkedHashMap<>();
        for (int i = 0; i < node.args().size(); i++) {
            argBinds.put(cf.signature().parameters().get(i + 1).name(),
                    node.args().get(i));
        }
        TypedSpec hop = unwrapToOneFirst(inlineDerivedCalls(
                substVars(cf.body().get(0), argBinds), 0));
        // CHAINED two-hop body ($this.h1[->filter(p)].h2->toOne() — the
        // qualifier-inside-qualifier shape after nested-call inlining)
        if (hop instanceof TypedPropertyAccess hp2
                && hp2.info().type() instanceof Type.ClassType) {
            TypedSpec chSrc = unwrapToOneFirst(hp2.source());
            TypedLambda midPred = null;
            if (chSrc instanceof TypedFilter chF
                    && chF.predicate().parameters().size() == 1
                    && chF.predicate().body().size() == 1) {
                midPred = chF.predicate();
                chSrc = unwrapToOneFirst(chF.source());
            }
            if (chSrc instanceof TypedPropertyAccess hp1
                    && hp1.source() instanceof TypedVariable chV
                    && chV.name().equals(thisVar)
                    && hp1.info().type() instanceof Type.ClassType) {
                var chained = chainedDerivedChild(cs, node, context,
                        parentRowVar, parentRowType, hp1.property(),
                        midPred, hp2.property(), d, thisVar);
                if (chained != null) {
                    return chained;
                }
            }
        }
        TypedLambda extraPred = null;
        if (hop instanceof TypedFilter hf
                && hf.predicate().parameters().size() == 1
                && hf.predicate().body().size() == 1) {
            extraPred = hf.predicate();
            hop = unwrapToOneFirst(hf.source());
        }
        SubqueryEnv env = new SubqueryEnv(cs, context, parentRowVar,
                parentRowType);
        HeadRel hr = navHeadRelation(env, hop, thisVar);
        if (hr == null) {
            return null;
        }
        ClassSource target = hr.target();
        TypedSpec childRel = hr.rel();
        Set<String> extraParams = new LinkedHashSet<>();
        extraParams.add(target.rowVar());
        if (extraPred != null) {
            TypedSpec pb = inlineThis(extraPred.body().get(0),
                    extraPred.parameters().get(0), Map.of(),
                    target.bindings(), target.classFqn(), node.property());
            var predFn = new Type.FunctionType(
                    List.of(new Type.Param(hr.targetRow(),
                            com.legend.compiler.element.type
                                    .Multiplicity.Bounded.ONE)),
                    new Type.Param(Type.Primitive.BOOLEAN,
                            com.legend.compiler.element.type
                                    .Multiplicity.Bounded.ONE));
            childRel = new TypedFilter(childRel,
                    new TypedLambda(List.of(target.rowVar()), List.of(pb),
                            new ExprType(predFn,
                                    com.legend.compiler.element.type
                                            .Multiplicity.Bounded.ONE)),
                    childRel.info());
            extraParams.add(extraPred.parameters().get(0));
        }
        boolean toMany = !(d.multiplicity()
                instanceof com.legend.compiler.element.type
                        .Multiplicity.Bounded bm
                && Integer.valueOf(1).equals(bm.upper()));
        return childFromRel(target, childRel, extraParams, toMany, node,
                context, Map.of());
    }

    /** β-inline NESTED derived-property calls inside a derived body
     * (qualifier-inside-qualifier): a user call with a single-statement
     * compilable body substitutes params for args, recursively
     * (depth-capped; a non-compilable callee keeps the call). */
    private TypedSpec inlineDerivedCalls(TypedSpec n, int depth) {
        if (depth > 8) {
            return n;
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedUserCall uc
                && uc.callee().body().isPresent()) {
            try {
                var cf2 = sources.compileSynthFn(uc.callee().qualifiedName());
                if (cf2.body().size() == 1 && cf2.signature().parameters()
                        .size() == uc.args().size()) {
                    Map<String, TypedSpec> binds =
                            new java.util.LinkedHashMap<>();
                    for (int i = 0; i < uc.args().size(); i++) {
                        binds.put(cf2.signature().parameters().get(i).name(),
                                inlineDerivedCalls(uc.args().get(i),
                                        depth + 1));
                    }
                    return inlineDerivedCalls(
                            substVars(cf2.body().get(0), binds), depth + 1);
                }
            } catch (RuntimeException notCompilable) {
                return n;
            }
            return n;
        }
        List<TypedSpec> kids = n.children();
        if (kids.isEmpty()) {
            return n;
        }
        List<TypedSpec> out = new ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec r = inlineDerivedCalls(k, depth);
            changed |= r != k;
            out.add(r);
        }
        return changed ? n.withChildren(out) : n;
    }

    /** The chained-hop derived child: h2's target correlated to the
     * parent through an EXISTS over h1's relation — parent↔h1 via the
     * first association join, h1↔child via the second, the mid filter
     * substituted through h1's bindings (engine: nested per-hop join
     * chain; the EXISTS form is the [0..1]-safe emission). */
    private TypedSerializeGraph.@com.legend.Nullable Child chainedDerivedChild(
            ClassSource cs, TypedGraphTree node, StoreResolver.Context context,
            String parentRowVar, Type.RelationType parentRowType,
            String h1, @com.legend.Nullable TypedLambda midPred, String h2,
            com.legend.compiler.element.Property.Derived d, String thisVar) {
        HopJoin j1 = hopJoin(cs, h1, context);
        HopJoin j2 = j1 == null ? null : hopJoin(j1.target(), h2, context);
        if (j1 == null || j2 == null) {
            return null;
        }
        ClassSource mid = j1.target();
        ClassSource target = j2.target();
        Type.RelationType midRow = j1.targetRow();
        Type.RelationType targetRow = j2.targetRow();
        TypedLambda c1 = j1.cond();
        TypedLambda c2 = j2.cond();
        var one = com.legend.compiler.element.type.Multiplicity.Bounded.ONE;
        // FRESH row vars — source row-var names collide across hops
        // (both 'row'); every substitution below is single-key sequential
        String midName = "_r" + freshVar.getAsInt();
        String tName = "_r" + freshVar.getAsInt();
        TypedSpec midRead = new TypedVariable(midName,
                new ExprType(midRow, one));
        TypedSpec parentRead = new TypedVariable(parentRowVar,
                new ExprType(parentRowType, one));
        // MID-relation conjuncts (parent correlation + the body's own
        // filter) ride the mid relation's OWN filter — the exists
        // lambda keeps ONLY the mid<->child link, so the exists
        // emission chooser sees exactly one cross-row predicate
        List<TypedSpec> midConj = new ArrayList<>();
        TypedSpec userConj = null;
        for (TypedSpec cb : c1.body()) {
            TypedSpec b = Pipelines.rewriteRowReads(cb,
                    c1.parameters().get(0), Map.of(), Set.of(),
                    v -> parentRead);
            b = substVars(b, Map.of(c1.parameters().get(1), midRead));
            midConj.add(b);
        }
        if (midPred != null) {
            // the predicate reads BOTH rows: the mid instance through
            // mid's bindings, then $this through the PARENT's bindings
            TypedSpec mb = inlineThis(midPred.body().get(0),
                    midPred.parameters().get(0), Map.of(), mid.bindings(),
                    mid.classFqn(), node.property());
            mb = substVars(mb, Map.of(mid.rowVar(), midRead));
            mb = inlineThis(mb, thisVar, Map.of(), cs.bindings(),
                    cs.classFqn(), node.property());
            mb = substVars(mb, Map.of(cs.rowVar(), parentRead));
            userConj = mb;
        }
        var midFn = new Type.FunctionType(
                List.of(new Type.Param(midRow, one)),
                new Type.Param(Type.Primitive.BOOLEAN, one));
        // LAYERED filters (the Substitution membership idiom): the USER
        // predicate (midPred) keeps ordinary pure equality in its own
        // NONE-stamped layer; the mapping-join correlation conjuncts ride
        // a CORRELATION-stamped layer (verbatim '=' — engine @join
        // semantics; the deleted per-conjunct toOne wraps were the old,
        // type-lying spelling of the same split)
        TypedSpec midBase = j1.targetPipeline();
        if (userConj != null) {
            midBase = new TypedFilter(midBase,
                    new TypedLambda(List.of(midName), List.of(userConj),
                            new ExprType(midFn, one)),
                    midBase.info());
        }
        TypedSpec midRel = new TypedFilter(midBase,
                new TypedLambda(List.of(midName), List.of(andFold(midConj)),
                        new ExprType(midFn, one)),
                midBase.info(), TypedFilter.Stamp.CORRELATION);
        List<TypedSpec> conj = new ArrayList<>();
        for (TypedSpec cb : c2.body()) {
            TypedSpec b = substVars(cb,
                    Map.of(c2.parameters().get(0), midRead));
            b = substVars(b, Map.of(c2.parameters().get(1),
                    new TypedVariable(tName, new ExprType(targetRow, one))));
            conj.add(b);
        }
        var existsFn = new Type.FunctionType(
                List.of(new Type.Param(midRow, one)),
                new Type.Param(Type.Primitive.BOOLEAN, one));
        TypedSpec existsCall = new TypedNativeCall(existsCallee(),
                List.of(midRel,
                        new TypedLambda(List.of(midName),
                                List.of(andFold(conj)),
                                new ExprType(existsFn, one))),
                new ExprType(Type.Primitive.BOOLEAN, one));
        var childFn = new Type.FunctionType(
                List.of(new Type.Param(targetRow, one)),
                new Type.Param(Type.Primitive.BOOLEAN, one));
        TypedSpec childRel = new TypedFilter(j2.targetPipeline(),
                new TypedLambda(List.of(tName), List.of(existsCall),
                        new ExprType(childFn, one)),
                j2.targetPipeline().info(), TypedFilter.Stamp.CORRELATION);
        Set<String> extraParams = new LinkedHashSet<>();
        extraParams.add(tName);
        extraParams.add(midName);
        boolean toMany = !(d.multiplicity()
                instanceof com.legend.compiler.element.type
                        .Multiplicity.Bounded bm
                && Integer.valueOf(1).equals(bm.upper()));
        return childFromRel(target, childRel, extraParams, toMany, node,
                context, Map.of());
    }

    private record HopJoin(ClassSource target, TypedSpec targetPipeline,
            Type.RelationType targetRow, TypedLambda cond) {
    }

    /** ONE navigation hop resolved to (target, pipeline, row, condition)
     * — association ends AND join-slot-backed class properties (the two
     * navHeadRelation arms, hop-composable). Null when neither resolves. */
    private @com.legend.Nullable HopJoin hopJoin(ClassSource src, String prop,
            StoreResolver.Context context) {
        try {
            var aj = assocMaterial.associationJoin(temporal, src, prop,
                    context, /*forExists*/ true);
            if (aj != null && aj.condition() != null) {
                return new HopJoin(aj.target(), aj.targetPipeline(),
                        aj.targetRow(), aj.condition());
            }
        } catch (RuntimeException notAnAssoc) {
            // fall through to the slot route
        }
        TypedSpec bindingRead = src.bindings().get(prop);
        String alias = prop;
        var ow = bindingRead == null ? null
                : com.legend.resolver.Substitution.otherwiseOf(bindingRead);
        if (ow != null) {
            bindingRead = ow.args().get(1);
        }
        bindingRead = bindingRead == null ? null
                : unwrapToOneFirst(bindingRead);
        if (bindingRead instanceof TypedPropertyAccess bpa
                && bpa.source() instanceof TypedVariable bv
                && bv.name().equals(src.rowVar())) {
            alias = bpa.property();
        }
        TypedNavigate nav = Pipelines.outerNavSteps(src.pipeline())
                .get(alias);
        if (nav == null
                || !(nav.target() instanceof TypedGetAll navGa)) {
            return null;
        }
        String key = (context.explicitMapping() == null ? ""
                : context.explicitMapping()) + '\u0000'
                + (context.runtimeFqn() == null ? ""
                        : context.runtimeFqn());
        String rawTarget = navGa.classFqn();
        ClassSource target = sources.get(
                dispatch.apply(context, rawTarget), rawTarget,
                (t, excl) -> dispatch.apply(context, t), key,
                context.constructedScope());
        Pipelines.Materialized cMat = Pipelines.materialize(
                target.pipeline(), Set.of(), rawTarget);
        TypedSpec targetPipeline = temporal.temporalTargetPipe(src, target,
                prop, cMat.pipeline());
        return new HopJoin(target, targetPipeline,
                Type.requireRelationSchema(targetPipeline.info().type()),
                nav.pairedPredicate().orElse(nav.predicate()));
    }

    /** Conjunction of predicate expressions — a multi-statement lambda
     * body keeps only its LAST statement (pure statement semantics), so
     * conjunct lists must AND-fold into one expression. */
    private TypedSpec andFold(List<TypedSpec> conj) {
        TypedSpec out = conj.get(0);
        var one = com.legend.compiler.element.type.Multiplicity.Bounded.ONE;
        for (int i = 1; i < conj.size(); i++) {
            out = new TypedNativeCall(andCallee(),
                    List.of(out, conj.get(i)),
                    new ExprType(Type.Primitive.BOOLEAN, one));
        }
        return out;
    }

    private com.legend.compiler.element.TypedFunction andCallee() {
        var fns = ctx.findFunction("meta::pure::functions::boolean::and");
        if (fns.isEmpty()) {
            throw new IllegalStateException(
                    "resolver bug: no and registration");
        }
        return fns.get(0);
    }

    /** The registered exists overload — the chained-hop EXISTS emission. */
    private com.legend.compiler.element.TypedFunction existsCallee() {
        var fns = ctx.findFunction(
                "meta::pure::functions::collection::exists");
        if (fns.isEmpty()) {
            throw new IllegalStateException(
                    "resolver bug: no exists registration");
        }
        return fns.get(0);
    }

    /** The serialized key for a CLASS child: a QUALIFIED spelling with
     * empty parens over a milestoned target spells the RESOLVED context
     * date — the engine serializes the source call spelling with the
     * propagated date filled in
     * ({@code synonyms(2023-10-15T00:00:00+0000)}). */
    private String childKey(TypedGraphTree node, String childFqn) {
        if (node.alias() == null && node.qualified()
                && node.args().isEmpty()) {
            MilestoningStrategy strat = temporal.temporalStrategy(childFqn);
            TypedSpec d = strat == null ? null
                    : temporal.rootContextDate(
                            strat != MilestoningStrategy.PROCESSING);
            if (d instanceof com.legend.compiler.spec.typed.TypedCDate cd) {
                String iso = cd.value().toEngineString();
                return node.property() + "(" + iso
                        + (iso.indexOf('T') >= 0 ? "+0000" : "") + ")";
            }
        }
        return keyOf(node);
    }

    /** An EMBEDDED graph child: leaves come straight from the ^Inner
     * ctor (parent-row expressions), nested embedded ctors recurse as
     * further inline children. Non-ctor leaves (slot/assoc reads inside
     * the embedded) stay loud — their join machinery is a later rung. */
    /**
     * A class-typed child over a MIXED-UNION extent: per-member dispatch as
     * ONE correlated subquery — target = the keyed child union (arms paired
     * by each parent member's declared route target set), condition =
     * OR over pairs of AND over key-column equalities (NULL keys in other
     * arms make cross-pair matches impossible). Null when the property is
     * not class-typed (scalar walls stay downstream).
     */
    private TypedSerializeGraph.@com.legend.Nullable Child mixedUnionChild(ClassSource cs,
            TypedGraphTree node, StoreResolver.Context context,
            String parentRowVar, Type.RelationType parentRowType) {
        String childClass = mixedChildClassOf(cs.classFqn(), node.property());
        if (childClass == null) {
            return null;
        }
        ClassSources.MixedChild mc = sources.mixedChildMaterial(
                cs.mappingFqn(), cs.classFqn(), node.property(), childClass);
        if (mc == null) {
            return null;
        }
        var one = com.legend.compiler.element.type.Multiplicity.Bounded.ONE;
        var eqFns = ctx.findFunction("meta::pure::functions::boolean::equal")
                .stream().filter(f -> f.parameters().size() == 2).toList();
        var andFns = ctx.findFunction("meta::pure::functions::boolean::and")
                .stream().filter(f -> f.parameters().size() == 2).toList();
        var orFns = ctx.findFunction("meta::pure::functions::boolean::or")
                .stream().filter(f -> f.parameters().size() == 2).toList();
        if (eqFns.size() != 1 || andFns.size() != 1 || orFns.size() != 1) {
            throw new IllegalStateException("resolver bug: expected one 2-arg"
                    + " equal/and/or in the catalog");
        }
        ExprType boolInfo = new ExprType(Type.Primitive.BOOLEAN, one);
        String pv = "u_pr";
        String tv = "u_tr";
        Type.RelationType tRow = mc.target().rowType();
        ExprType pInfo = new ExprType(parentRowType, one);
        ExprType tInfo = new ExprType(tRow, one);
        TypedSpec cond = null;
        for (java.util.List<String> pair : mc.keysPerPair()) {
            TypedSpec pairCond = null;
            for (String keyName : pair) {
                Type.Column kc = mixedKeyColumn(tRow, keyName);
                mixedKeyColumn(parentRowType, keyName);   // parent carries it
                ExprType ki = new ExprType(kc.type(), kc.multiplicity());
                TypedSpec eq = new TypedNativeCall(eqFns.get(0), List.of(
                        new TypedPropertyAccess(new TypedVariable(pv, pInfo),
                                keyName, ki),
                        new TypedPropertyAccess(new TypedVariable(tv, tInfo),
                                keyName, ki)), boolInfo);
                pairCond = pairCond == null ? eq : new TypedNativeCall(
                        andFns.get(0), List.of(pairCond, eq), boolInfo);
            }
            cond = cond == null ? pairCond : new TypedNativeCall(
                    orFns.get(0), List.of(cond, pairCond), boolInfo);
        }
        var fnT = new Type.FunctionType(
                List.of(new Type.Param(parentRowType, one),
                        new Type.Param(tRow, one)),
                new Type.Param(Type.Primitive.BOOLEAN, one));
        // the union arms' key columns are NULL by construction outside
        // their own arm and must NEVER pair — the CORRELATION filter
        // stamp keeps them SQL-semantics (verbatim '=' at lowering)
        TypedLambda condL = new TypedLambda(List.of(pv, tv),
                List.of(java.util.Objects.requireNonNull(cond,
                        "mixed-union child needs at least one key pair")),
                new ExprType(fnT, one));
        return correlatedGraphChild(mc.target(), mc.target().pipeline(), tRow,
                condL, mixedChildToMany(cs.classFqn(), node.property()), node,
                parentRowVar, parentRowType, context);
    }

    private static Type.Column mixedKeyColumn(Type.RelationType row,
            String name) {
        for (Type.Column c : row.columns()) {
            if (c.name().equals(name)) {
                return c;
            }
        }
        throw new IllegalStateException("resolver bug: mixed-union key"
                + " column '" + name + "' is not on the row — the extent"
                + " and the child material drifted");
    }

    private @com.legend.Nullable String mixedChildClassOf(String clsFqn, String prop) {
        var p = ctx.findProperty(clsFqn, prop).orElse(null);
        if (p != null && p.type() instanceof Type.ClassType ct) {
            return ct.fqn();
        }
        return ctx.findAssociationOf(clsFqn, prop)
                .map(a -> (a.property1().propertyName().equals(prop)
                        ? a.property1() : a.property2()).targetClassFqn())
                .orElse(null);
    }

    private boolean mixedChildToMany(String clsFqn, String prop) {
        var p = ctx.findProperty(clsFqn, prop).orElse(null);
        if (p != null && p.type() instanceof Type.ClassType) {
            return !(p.multiplicity() instanceof com.legend.compiler.element
                    .type.Multiplicity.Bounded mb && mb.isToOne());
        }
        return ctx.findAssociationOf(clsFqn, prop)
                .map(a -> !(a.property1().propertyName().equals(prop)
                        ? a.property1() : a.property2()).isToOne())
                .orElse(true);
    }

    /**
     * WHOLE-SOURCE child ({@code trader[trader_set]: $src}): the child
     * instance is the SAME source row seen through ANOTHER set of this
     * mapping — resolve that set (by the cast's declared set id), guard
     * that it composes over the parent's row columns, and rebase its
     * bindings onto the parent row var. Inline, no join — the engine's
     * same-instance ModelStore composition (XStore leg, route A).
     */
    private TypedSerializeGraph.Child wholeSrcChild(ClassSource cs,
            TypedGraphTree node, TypedNewInstanceCast cast,
            String srcClassFqn, StoreResolver.Context context,
            TypedSpec parentPipeline) {
        ClassSource child = cast.targetSetId() != null
                ? sources.get(cs.mappingFqn(), cast.classFqn(), cast.targetSetId(), null, "", cs.scope())
                : sources.get(cs.mappingFqn(), cast.classFqn(), cs.scope());
        Type.RelationType rowT = Type.requireRelationSchema(parentPipeline.info().type());
        // audit 24 F4: SOURCE-CLASS identity is the real same-frame check —
        // two frames can share every column NAME (S_Trade vs S_Trade2) and
        // a name-subset proxy alone would silently serve the parent's rows
        // as the other class's data. The structural subset check remains
        // as the fallback ONLY when the child's source class is unknown.
        if (child.sourceClass() != null
                && !child.sourceClass().equals(srcClassFqn)) {
            throw new NotImplementedException("whole-source graph child '"
                    + node.property() + "' of '" + cs.classFqn()
                    + "': set '" + cast.classFqn() + "' composes over source"
                    + " class '" + child.sourceClass() + "' but the marker's"
                    + " row is a '" + srcClassFqn + "' frame — different"
                    + " sources never share a row");
        }
        Set<String> parentCols = new LinkedHashSet<>();
        for (Type.Column pc : rowT.columns()) {
            parentCols.add(pc.name());
        }
        for (Type.Column cc : child.rowType().columns()) {
            if (!parentCols.contains(cc.name())) {
                throw new NotImplementedException("whole-source graph child '"
                        + node.property() + "' of '" + cs.classFqn()
                        + "': set '" + cast.classFqn() + "' composes over"
                        + " column '" + cc.name() + "' absent from the"
                        + " parent row — the sets do not share the source"
                        + " frame (source class '" + srcClassFqn + "')");
            }
        }
        var rowInfo = new ExprType(rowT,
                com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
        List<TypedFuncCol> leaves = new ArrayList<>();
        List<TypedSerializeGraph.Child> nested = new ArrayList<>();
        for (TypedGraphTree c : node.children()) {
            TypedSpec e = child.bindings().get(c.property());
            // Non-scalar entries — association ends (XStore included),
            // class-typed bindings, nested whole-source hops — recurse the
            // ORDINARY graph-child machinery rooted at the child set: the
            // row is frame-compatible (guarded above), so the child set IS
            // this row's class source and every dispatch arm applies.
            if (e == null && c.children().isEmpty()) {
                throw new MappingResolutionException("property '"
                        + c.property() + "' of whole-source child '"
                        + node.property() + "' on class '" + cast.classFqn()
                        + "' is not mapped in mapping '" + cs.mappingFqn()
                        + "'", cast.classFqn());
            }
            if (e == null || e.info().type() instanceof Type.ClassType) {
                nested.add(graphChild(child, c, context, cs.rowVar(), rowT,
                        parentPipeline));
                continue;
            }
            TypedSpec e2 = renameRowVar(e, child.rowVar(), cs.rowVar(),
                    rowInfo);
            var lFn = new Type.FunctionType(
                    List.of(new Type.Param(rowT,
                            com.legend.compiler.element.type.Multiplicity
                                    .Bounded.ONE)),
                    new Type.Param(e2.info().type(), e2.info().multiplicity()));
            leaves.add(new TypedFuncCol(keyOf(c),
                    new TypedLambda(List.of(cs.rowVar()), List.of(e2),
                            new ExprType(lFn,
                                    com.legend.compiler.element.type
                                            .Multiplicity.Bounded.ONE))));
        }
        TypedSerializeGraph nodeG = new TypedSerializeGraph(parentPipeline,
                cs.rowVar(), leaves, nested, false, false, cast.classFqn(),
                rowInfo, true);
        return new TypedSerializeGraph.Child(
                childKey(node, cast.classFqn()), nodeG);
    }

    /** {@code from}-var reads re-pointed at {@code to} with the parent's
     * row info — same-frame rebase for whole-source children. */
    private TypedSpec renameRowVar(TypedSpec n, String from, String to,
            ExprType rowInfo) {
        if (n instanceof TypedVariable v && v.name().equals(from)) {
            return new TypedVariable(to, rowInfo);
        }
        return SyntheticHeads.rebuildChildren(n,
                c -> renameRowVar(c, from, to, rowInfo));
    }

    private TypedSerializeGraph.Child embeddedChild(ClassSource cs,
            TypedGraphTree node, TypedNewInstance ctor,
            StoreResolver.Context context, TypedSpec parentPipeline) {
        return embeddedChild(cs, node, ctor, context, parentPipeline, null);
    }

    private TypedSerializeGraph.Child embeddedChild(ClassSource cs,
            TypedGraphTree node, TypedNewInstance ctor,
            StoreResolver.Context context, TypedSpec parentPipeline,
            @com.legend.Nullable TypedSpec otherwiseFallback) {
        return embeddedChild(cs, cs.classFqn(), node, ctor, context,
                parentPipeline, otherwiseFallback);
    }

    /** {@code ownerFqn}: the class whose property {@code node} names — the
     *  PARENT'S class at the top level, the computed child class on
     *  nested recursion (study #22: it was computed then discarded, so
     *  nested embedded children resolved against the wrong class). */
    private TypedSerializeGraph.Child embeddedChild(ClassSource cs,
            String ownerFqn, TypedGraphTree node, TypedNewInstance ctor,
            StoreResolver.Context context, TypedSpec parentPipeline,
            @com.legend.Nullable TypedSpec otherwiseFallback) {
        var prop = ctx.findProperty(ownerFqn, node.property())
                .orElseThrow(() -> new IllegalStateException(
                        "resolver bug: graph child '" + node.property()
                        + "' is not a property of '" + ownerFqn + "'"));
        String childClass = prop.type() instanceof Type.ClassType cc
                ? cc.fqn() : ownerFqn;
        List<TypedGraphTree> want = node.children().isEmpty()
                ? ctor.properties().keySet().stream()
                        .map(k -> new TypedGraphTree(k, List.of()))
                        .toList()
                : node.children();
        Type.RelationType rowT = Type.requireRelationSchema(parentPipeline.info().type());
        var rowInfo = new ExprType(rowT,
                com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
        List<TypedFuncCol> leaves = new ArrayList<>();
        List<TypedSerializeGraph.Child> nested = new ArrayList<>();
        for (TypedGraphTree c : want) {
            TypedSpec e = ctor.properties().get(c.property());
            if (e == null && otherwiseFallback != null) {
                // OTHERWISE per-leaf dispatch (map §6 / V1 §D.5): a leaf
                // MISSING from the embedded partial reads through the
                // FALLBACK join target — a correlated scalar subquery,
                // never a silent null
                HeadRel hr = navHeadRelation(
                        new SubqueryEnv(cs, context, cs.rowVar(), rowT),
                        otherwiseFallback, cs.rowVar());
                TypedSpec fb = hr == null ? null
                        : hr.target().bindings().get(c.property());
                if (fb != null
                        && !(fb.info().type() instanceof Type.ClassType)) {
                    var hr2 = java.util.Objects.requireNonNull(hr, "hr");
                    e = scalarLeafSubquery(hr2.rel(), hr2.target().rowVar(),
                            hr2.targetRow(), c.property(), fb);
                }
            }
            if (e == null) {
                // ASSOCIATION child of the EMBEDDED class (the inline-
                // embedded set's association mapping): the embedded node
                // shares the parent's row — the association correlates
                // against it (correlatedGraphChild rewrites the source
                // param onto the parent row var; same-table sets align)
                var embAssoc = ctx.findAssociationOf(childClass,
                        c.property());
                if (embAssoc.isPresent()) {
                    ClassSource embSrc = sources.get(cs.mappingFqn(), childClass, cs.scope());
                    java.util.Set<String> embLeaves =
                            new java.util.LinkedHashSet<>();
                    for (TypedGraphTree cc2 : c.children()) {
                        embLeaves.add(cc2.property());
                    }
                    AssociationJoins.AssocJoin aj = assocMaterial
                            .associationJoin(temporal, embSrc, c.property(),
                                    context, /*forExists*/ true, embLeaves);
                    var embEnd = embAssoc.get().property1().propertyName()
                            .equals(c.property())
                            ? embAssoc.get().property1()
                            : embAssoc.get().property2();
                    nested.add(correlatedGraphChild(aj.target(),
                            aj.targetPipeline(), aj.targetRow(),
                            java.util.Objects.requireNonNull(aj.condition()), !embEnd.isToOne(), c,
                            cs.rowVar(), rowT, context,
                            aj.targetSlotPrefixes()));
                    continue;
                }
                // DERIVED property of the EMBEDDED class: the lifted body
                // inlines against the ctor's bindings — parent-row exprs,
                // same machinery as top-level qualifier leaves (task #78)
                TypedSpec dv = derivedLeaf(ctor.properties(), childClass, c);
                if (dv != null) {
                    var dFn2 = new Type.FunctionType(
                            List.of(new Type.Param(rowT,
                                    com.legend.compiler.element.type
                                            .Multiplicity.Bounded.ONE)),
                            new Type.Param(dv.info().type(),
                                    dv.info().multiplicity()));
                    leaves.add(new TypedFuncCol(c.alias() != null
                            ? c.alias() : callKey(c),
                            new TypedLambda(List.of(cs.rowVar()),
                                    List.of(dv),
                                    new ExprType(dFn2,
                                            com.legend.compiler.element.type
                                                    .Multiplicity.Bounded
                                                    .ONE))));
                    continue;
                }
                throw new MappingResolutionException("property '"
                        + c.property() + "' of embedded '" + node.property()
                        + "' on class '" + cs.classFqn()
                        + "' is not mapped in mapping '" + cs.mappingFqn()
                        + "' [otherwise fallback="
                        + (otherwiseFallback == null ? "ABSENT"
                                : otherwiseFallback.getClass().getSimpleName())
                        + "]", cs.classFqn());
            }
            TypedSpec ei = e;
            if (ei instanceof TypedNativeCall tc1 && tc1.args().size() == 1
                    && com.legend.builtin.Pure.isToOneCall(tc1.callee().qualifiedName())) {
                ei = tc1.args().get(0);
            }
            if (ei instanceof TypedNewInstance subCtor) {
                nested.add(embeddedChild(cs, childClass, c, subCtor,
                        context, parentPipeline, null));
                continue;
            }
            if (ei.info().type() instanceof Type.ClassType) {
                // JOIN property INSIDE the embedded ctor (firm(employees:
                // @firmEmployees)): the embedded node shares the PARENT's
                // row, so the slot child correlates against the parent —
                // the same navigate-slot route as a top-level child
                if (ei instanceof TypedPropertyAccess pa3
                        && pa3.source() instanceof TypedVariable v3
                        && v3.name().equals(cs.rowVar())) {
                    var nav3 = Pipelines.outerNavSteps(cs.pipeline())
                            .get(pa3.property());
                    if (nav3 != null) {
                        nested.add(navSlotChild(cs, childClass, c, nav3,
                                null, context, cs.rowVar(), rowT));
                        continue;
                    }
                }
                throw new NotImplementedException("embedded graph child '"
                        + node.property() + "." + c.property()
                        + "' is class-typed through a non-ctor binding —"
                        + " not supported yet");
            }
            // a TO-MANY PRIMITIVE leaf reading a join slot (the Inline
            // splice's authorId: @Book_Authorship | ... over String[*]):
            // the inline read would take ONE joined row — the engine
            // serializes the AGGREGATED array; same correlated emission
            // as the top-level to-many slot-read arm
            // single-arg WRAPPER CALLS peel for detection and REBUILD
            // over the correlated child row (authorId:
            // toString(@Book_Authorship | ...) — per-element scalar fns)
            TypedSpec peeled2 = ei;
            java.util.List<TypedNativeCall> wraps2 = new ArrayList<>();
            while (peeled2 instanceof TypedNativeCall wc2
                    && wc2.args().size() == 1) {
                wraps2.add(wc2);
                peeled2 = wc2.args().get(0);
            }
            // the guard is the DECLARED property multiplicity (the
            // Inline-splice read types per COLUMN — [0..1] — masking the
            // property's [*])
            var declProp2 = ctx.findProperty(childClass, c.property())
                    .orElse(null);
            boolean many2 = declProp2 != null && !(declProp2.multiplicity()
                    instanceof com.legend.compiler.element.type
                            .Multiplicity.Bounded dm2
                    && Integer.valueOf(1).equals(dm2.upper()));
            if (many2 && peeled2 instanceof TypedPropertyAccess colPa2
                    && colPa2.source() instanceof TypedPropertyAccess slotPa2
                    && slotPa2.source() instanceof TypedVariable sv2
                    && sv2.name().equals(cs.rowVar())) {
                java.util.function.UnaryOperator<TypedSpec> rewrap =
                        v0 -> {
                            TypedSpec out2 = v0;
                            for (int wi = wraps2.size() - 1; wi >= 0; wi--) {
                                TypedNativeCall w3 = wraps2.get(wi);
                                out2 = w3.withChildren(List.of(out2));
                            }
                            return out2;
                        };
                // the hop's date context WINDOWS the correlated target
                // (authors($businessDate): unstamped it collects every
                // version row — the 5002 leak)
                TemporalContext hc3 = temporal.contextAt(node.property(),
                        childClass, TemporalContext.NONE);
                java.util.function.UnaryOperator<TypedSpec> window3 =
                        t3 -> !hc3.isEmpty()
                                && temporal.temporalStrategy(childClass)
                                        != null
                                ? temporal.stampForClassOrDefer(t3, hc3,
                                        childClass, node.property())
                                : t3;
                var navE = Pipelines.outerNavSteps(cs.pipeline())
                        .get(slotPa2.property());
                if (navE != null) {
                    nested.add(primitiveArrayChild(keyOf(c),
                            window3.apply(navE.target()), navE.predicate(),
                            colPa2, cs.rowVar(), rowT, rewrap));
                    continue;
                }
                var slotE = Pipelines.joinSlots(cs.pipeline())
                        .get(slotPa2.property());
                if (slotE != null) {
                    nested.add(primitiveArrayChild(keyOf(c),
                            window3.apply(slotE.target()), slotE.condition(),
                            colPa2, cs.rowVar(), rowT, rewrap));
                    continue;
                }
            }
            var lFn = new Type.FunctionType(
                    List.of(new Type.Param(rowT,
                            com.legend.compiler.element.type.Multiplicity
                                    .Bounded.ONE)),
                    new Type.Param(e.info().type(), e.info().multiplicity()));
            leaves.add(new TypedFuncCol(keyOf(c),
                    new TypedLambda(List.of(cs.rowVar()), List.of(e),
                            new ExprType(lFn,
                                    com.legend.compiler.element.type
                                            .Multiplicity.Bounded.ONE))));
        }
        // INLINE child: the pipeline is a row-typing CARRIER only (the
        // envelope reads leaves off the PARENT base) — carry the parent's
        // MATERIALIZED pipeline, never the raw cs.pipeline() whose
        // undemanded navigate thunks still hold getAll (the embedded
        // family's join-inside-embedded shape tripped the escapee wall)
        // a TO-MANY embedded property serializes as an ARRAY around the
        // inline object (engine: "authors":[{...}])
        boolean many = !(prop.multiplicity()
                instanceof com.legend.compiler.element.type.Multiplicity
                        .Bounded pb
                && Integer.valueOf(1).equals(pb.upper()));
        TypedSerializeGraph nodeG = new TypedSerializeGraph(parentPipeline,
                cs.rowVar(), leaves, nested, many, false, childClass,
                rowInfo, true);
        return new TypedSerializeGraph.Child(
                childKey(node, childClass), nodeG);
    }

    /** The INLINED typed body of a parameterless derived property, its
     * {@code $this} reads substituted to the class's row bindings —
     * null when the name is not a parameterless derived property. */
    private @com.legend.Nullable TypedSpec derivedLeaf(ClassSource cs, TypedGraphTree node,
            StoreResolver.Context context, String rowVar,
            Type.RelationType rowType) {
        return derivedLeaf(cs, cs.classFqn(), node, context, rowVar, rowType);
    }

    /** {@code ownerClassFqn}: the class DECLARING the derived property —
     * the source class, or the SUBTYPE inside a {@code ->subType(@X)}
     * patch (the body's {@code $this} reads still resolve through the
     * SOURCE's bindings: inherited navs live there; subtype-only stored
     * reads fall to the louder walls). */
    private @com.legend.Nullable TypedSpec derivedLeaf(ClassSource cs, String ownerClassFqn,
            TypedGraphTree node,
            StoreResolver.Context context, String rowVar,
            Type.RelationType rowType) {
        String prop = node.property();
        var p = ctx.findProperty(ownerClassFqn, prop).orElse(null);
        if (!(p instanceof com.legend.compiler.element.Property.Derived d)
                || d.parameters().size() != node.args().size()) {
            return null;
        }
        var cf = sources.compileSynthFn(d.bodyFunctionFqn());
        if (cf.body().size() == 1) {
            String thisVar = cf.signature().parameters().get(0).name();
            Map<String, TypedSpec> binds = new java.util.LinkedHashMap<>();
            for (int i = 0; i < node.args().size(); i++) {
                binds.put(cf.signature().parameters().get(i + 1).name(),
                        node.args().get(i));
            }
            TypedSpec inlined = substVars(cf.body().get(0), binds);
            TypedSpec nav = navLeafSubquery(cs, inlined, thisVar,
                    context, rowVar, rowType);
            if (nav != null) {
                return nav;
            }
            TypedSpec aggN = navAggSubquery(cs, inlined, thisVar,
                    context, rowVar, rowType);
            if (aggN != null) {
                return aggN;
            }
            // aggregates NESTED under arithmetic (average(...)*2.0):
            // rewrite each agg-over-nav subnode; when the rewrite consumed
            // every $this read, the remaining expression is plain scalar
            boolean[] hit = {false};
            TypedSpec rew = rewriteNavAggs(inlined, cs, thisVar, context,
                    rowVar, rowType, hit);
            if (hit[0]) {
                Set<String> residual = new LinkedHashSet<>();
                Pipelines.collectVarReads(rew, thisVar, residual);
                if (residual.isEmpty()) {
                    return rew;
                }
            }
        }
        return derivedLeaf(cs.bindings(), cs.classFqn(), node,
                new SubqueryEnv(cs, context, rowVar, rowType));
    }

    /** A derived body that AGGREGATES a depth-1 navigation chain —
     * {@code $this.employees.age->average()}: the corr-filtered target
     * projects the leaf and the aggregate native wraps the RELATION — the
     * lowering renders a correlated scalar aggregate subquery (the T1.7
     * reducer catalog is the one membership test). Null when the shape
     * does not match (the inline route's louder walls take over). */
    private @com.legend.Nullable TypedSpec navAggSubquery(ClassSource cs, TypedSpec body,
            String thisVar, StoreResolver.Context context,
            String parentRowVar, Type.RelationType parentRowType) {
        if (!(body instanceof TypedNativeCall agg && agg.args().size() == 1
                && com.legend.lowering.Aggregates.isReducer(agg.callee()))) {
            return null;
        }
        TypedSpec chain = unwrapToOneFirst(agg.args().get(0));
        if (!(chain instanceof TypedPropertyAccess leaf)) {
            return null;
        }
        SubqueryEnv env = new SubqueryEnv(cs, context, parentRowVar,
                parentRowType);
        HeadRel hr = navHeadRelation(env, leaf.source(), thisVar);
        if (hr == null) {
            return null;
        }
        TypedSpec leafBind = hr.target().bindings().get(leaf.property());
        if (leafBind == null
                || leafBind.info().type() instanceof Type.ClassType) {
            return null;
        }
        var one = com.legend.compiler.element.type.Multiplicity.Bounded.ONE;
        var leafFn = new Type.FunctionType(
                List.of(new Type.Param(hr.targetRow(), one)),
                new Type.Param(leafBind.info().type(),
                        leafBind.info().multiplicity()));
        Type.RelationType oneCol = new Type.RelationType(List.of(
                new Type.Column(leaf.property(), leafBind.info().type(),
                        leafBind.info().multiplicity())));
        TypedSpec proj = new com.legend.compiler.spec.typed.TypedProject(
                hr.rel(),
                List.of(new TypedFuncCol(leaf.property(),
                        new TypedLambda(List.of(hr.target().rowVar()),
                                List.of(leafBind),
                                new ExprType(leafFn, one)))),
                new ExprType(Type.relation(oneCol),
                        com.legend.compiler.element.type
                                .Multiplicity.Bounded.ZERO_MANY));
        return agg.withChildren(List.of(proj));
    }

    /** Each maximal row-var-rooted navigation CHAIN sub-expression
     * ({@code $row.manager.firstName} — head alias unmaterialized)
     * replaced by {@link #navLeafSubquery}'s correlated scalar subquery;
     * materialized slots keep the prefixed-column path. */
    private TypedSpec rewriteNavChains(TypedSpec n, ClassSource cs,
            StoreResolver.Context context, String rowVar,
            Type.RelationType rowType, Map<String, String> slotPrefixes,
            boolean[] hit) {
        String head = chainHeadAlias(n, cs.rowVar());
        if (head != null && !slotPrefixes.containsKey(head)) {
            TypedSpec r = navLeafSubquery(cs, n, cs.rowVar(), context,
                    rowVar, rowType);
            if (r != null) {
                hit[0] = true;
                return r;
            }
        }
        return n.mapChildren(c -> rewriteNavChains(c, cs, context, rowVar,
                rowType, slotPrefixes, hit));
    }

    /** The FIRST property read off the row var when {@code n} is a
     * SCALAR-resulting access chain crossing at least one CLASS-typed
     * hop; null otherwise. */
    private @com.legend.Nullable String chainHeadAlias(TypedSpec n,
            String rowVar) {
        TypedSpec b = unwrapToOneFirst(n);
        if (!(b instanceof TypedPropertyAccess leaf)
                || leaf.info().type() instanceof Type.ClassType) {
            return null;
        }
        TypedSpec cur = unwrapToOneFirst(leaf.source());
        boolean classHop = false;
        String head = null;
        while (true) {
            switch (cur) {
                case TypedPropertyAccess pa -> {
                    classHop |= pa.info().type() instanceof Type.ClassType;
                    head = pa.property();
                    cur = unwrapToOneFirst(pa.source());
                }
                case com.legend.compiler.spec.typed.TypedMilestonedAccess ma -> {
                    classHop |= ma.info().type() instanceof Type.ClassType;
                    head = ma.property();
                    cur = unwrapToOneFirst(ma.source());
                }
                case TypedVariable v -> {
                    return classHop && v.name().equals(rowVar) ? head : null;
                }
                default -> {
                    return null;
                }
            }
        }
    }

    private TypedSpec rewriteNavAggs(TypedSpec n, ClassSource cs,
            String thisVar, StoreResolver.Context context, String rowVar,
            Type.RelationType rowType, boolean[] hit) {
        TypedSpec r = navAggSubquery(cs, n, thisVar, context, rowVar, rowType);
        if (r != null) {
            hit[0] = true;
            return r;
        }
        return n.mapChildren(c -> rewriteNavAggs(c, cs, thisVar, context,
                rowVar, rowType, hit));
    }

    /** The enclosing-frame material the inline route needs to fall back
     * to a CORRELATED SCALAR SUBQUERY for a navigation SUB-EXPRESSION
     * (map §7 contract 4: qualified properties are child computations).
     * Null on paths without a frame (embedded ctor leaves). */
    record SubqueryEnv(ClassSource cs, StoreResolver.Context context,
            String rowVar, Type.RelationType rowType) {
    }

    /** The corr-filtered TARGET RELATION for a navigation HEAD read off
     * {@code $this} — the shared spine of the qualifier-as-child-
     * computation emissions (scalar leaf subquery, emptiness). Null when
     * the head shape does not match. */
    private record HeadRel(ClassSource target, Type.RelationType targetRow,
            TypedSpec rel) {
    }

    private @com.legend.Nullable HeadRel navHeadRelation(SubqueryEnv env, TypedSpec hop,
            String thisVar) {
        ClassSource cs = env.cs();
        StoreResolver.Context context = env.context();
        while (hop instanceof TypedNativeCall w && w.args().size() == 1
                && (com.legend.builtin.Pure.isToOneCall(w.callee().qualifiedName())
                    || w.callee().qualifiedName().equals(
                        "meta::pure::functions::collection::first"))) {
            hop = w.args().get(0);
        }
        String headProp;
        List<TypedSpec> hopDates = List.of();
        boolean hopSweep = false;
        if (hop instanceof TypedPropertyAccess hpa
                && hpa.source() instanceof TypedVariable hv
                && hv.name().equals(thisVar)
                && hpa.info().type() instanceof Type.ClassType) {
            headProp = hpa.property();
        } else if (hop instanceof com.legend.compiler.spec.typed
                        .TypedMilestonedAccess hma
                && hma.source() instanceof TypedVariable hv2
                && hv2.name().equals(thisVar)) {
            // a DATED head registers its dates as the head's temporal spec
            // (the same channel query-position property functions use).
            // Generated-date reads ($this.businessDate from the inlined
            // qualifier) NORMALIZE against the fetch's root context — the
            // literal date the local subquery can actually spell (the
            // cycle-71 nested-cursor rule; a dateless root keeps the raw
            // read and its honest lowering wall).
            headProp = hma.property();
            hopDates = temporal.normalizeContextDates(hma.dates());
            hopSweep = hma.sweep();
        } else {
            return null;
        }
        ClassSource target;
        TypedSpec targetPipeline;
        Type.RelationType targetRow;
        TypedLambda cond;
        TemporalFrame tf = hopDates.isEmpty() && !hopSweep ? temporal
                : temporal.withSpecs(Map.of(headProp,
                        new TemporalFrame.TemporalSpec(hopDates, hopSweep)));
        AssociationJoins.AssocJoin aj = null;
        try {
            aj = assocMaterial.associationJoin(tf, cs, headProp,
                    context, /*forExists*/ true);
        } catch (RuntimeException notAnAssoc) {
            aj = null;
        }
        if (aj != null) {
            target = aj.target();
            targetPipeline = aj.targetPipeline();
            targetRow = aj.targetRow();
            cond = aj.condition();
        } else {
            // NAVIGATE-SLOT-backed head (join PM): the slot's TypedNavigate
            // carries the raw target and the predicate — navSlotChild's
            // route, scalar-shaped
            TypedSpec bindingRead = cs.bindings().get(headProp);
            if (bindingRead == null) {
                // the OTHERWISE-fallback case: the hop IS the slot read
                // (an alias, not a class property — bindings has no entry)
                bindingRead = hop;
            }
            // an OTHERWISE-wrapped binding routes through its FALLBACK
            // read (the FK-join navigate; the embedded partial serves
            // only its own ctor leaves)
            var owN = com.legend.resolver.Substitution
                    .otherwiseOf(bindingRead);
            if (owN != null) {
                bindingRead = owN.args().get(1);
            }
            // conform-by-emission wrappers (toOne over the slot read) unwrap
            while (bindingRead instanceof TypedNativeCall bw
                    && bw.args().size() == 1
                    && (com.legend.builtin.Pure.isToOneCall(bw.callee().qualifiedName())
                        || bw.callee().qualifiedName().equals(
                            "meta::pure::functions::collection::first"))) {
                bindingRead = bw.args().get(0);
            }
            TypedNavigate nav = null;
            if (bindingRead instanceof TypedPropertyAccess bpa
                    && bpa.source() instanceof TypedVariable bvv
                    && bvv.name().equals(cs.rowVar())) {
                nav = Pipelines.outerNavSteps(cs.pipeline()).get(bpa.property());
            }
            if (nav == null) {
                return null;
            }
            String key = (context.explicitMapping() == null ? ""
                    : context.explicitMapping()) + '\u0000'
                    + (context.runtimeFqn() == null ? ""
                            : context.runtimeFqn());
            String rawTarget = ((TypedGetAll) nav.target()).classFqn();
            target = sources.get(dispatch.apply(context, rawTarget), rawTarget,
                    (t, excl) -> dispatch.apply(context, t), key,
                    context.constructedScope());
            Pipelines.Materialized cMat = Pipelines.materialize(
                    target.pipeline(), Set.of(), rawTarget);
            targetPipeline = tf.temporalTargetPipe(cs, target, headProp,
                    cMat.pipeline());
            targetRow = Type.requireRelationSchema(targetPipeline.info().type());
            cond = nav.pairedPredicate().orElse(nav.predicate());
        }
        String pVar = java.util.Objects.requireNonNull(cond, "cond").parameters().get(0);
        String tVar = cond.parameters().get(1);
        List<TypedSpec> corrBody = cond.body().stream().map(cb ->
                Pipelines.rewriteRowReads(cb, pVar, Map.of(), Set.of(),
                        v -> new TypedVariable(env.rowVar(),
                                new ExprType(env.rowType(),
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ONE))))
                .toList();
        var boolFn = new Type.FunctionType(
                List.of(new Type.Param(targetRow,
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE)),
                new Type.Param(Type.Primitive.BOOLEAN,
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
        // conjuncts AND-fold: a multi-statement lambda body keeps only
        // its LAST statement (the chained-hop lesson — a compound join
        // condition would silently drop all but one equality)
        TypedLambda corr = new TypedLambda(List.of(tVar),
                List.of(andFold(corrBody)),
                new ExprType(boolFn,
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
        return new HeadRel(target, targetRow,
                new TypedFilter(targetPipeline, corr, targetPipeline.info()));
    }

    /**
     * A derived body that is a DEPTH-1 NAVIGATION with a scalar leaf —
     * {@code $this.assoc.leaf} behind optional toOne/first wrappers —
     * emits as the fnlr encoding: the target pipeline filtered by the
     * oriented association condition (parent reads freed onto the
     * enclosing row var), projecting the leaf binding, LIMIT 1 — the
     * lowerer renders a relation in scalar position as a correlated
     * scalar subquery. Null when the shape does not match (the inline
     * route and its louder walls take over).
     */
    private @com.legend.Nullable TypedSpec navLeafSubquery(ClassSource cs, TypedSpec body,
            String thisVar, StoreResolver.Context context, String parentRowVar,
            Type.RelationType parentRowType) {
        TypedSpec b = body;
        while (b instanceof TypedNativeCall w && w.args().size() == 1
                && (com.legend.builtin.Pure.isToOneCall(w.callee().qualifiedName())
                    || w.callee().qualifiedName().equals(
                        "meta::pure::functions::collection::first"))) {
            b = w.args().get(0);
        }
        if (!(b instanceof TypedPropertyAccess leaf)) {
            return null;
        }
        TypedSpec hop = leaf.source();
        while (hop instanceof TypedNativeCall w2 && w2.args().size() == 1
                && (com.legend.builtin.Pure.isToOneCall(w2.callee().qualifiedName())
                    || w2.callee().qualifiedName().equals(
                        "meta::pure::functions::collection::first"))) {
            hop = w2.args().get(0);
        }
        // a NESTED $prop$ qualifier CALL β-inlines here (compileSynthFn
        // keeps user calls: $this.synonymByType(CUSIP) arrives as a
        // TypedUserCall whose callee body is the filter shape below)
        if (hop instanceof com.legend.compiler.spec.typed.TypedUserCall uc
                && uc.callee().qualifiedName().contains("$prop$")) {
            var ccf = sources.compileSynthFn(uc.callee().qualifiedName());
            if (ccf.body().size() == 1
                    && ccf.signature().parameters().size() == uc.args().size()) {
                Map<String, TypedSpec> sub = new java.util.LinkedHashMap<>();
                for (int i = 0; i < uc.args().size(); i++) {
                    sub.put(ccf.signature().parameters().get(i).name(),
                            uc.args().get(i));
                }
                hop = substVars(ccf.body().get(0), sub);
                while (hop instanceof TypedNativeCall w4
                        && w4.args().size() == 1
                        && (com.legend.builtin.Pure.isToOneCall(w4.callee().qualifiedName())
                            || w4.callee().qualifiedName().equals(
                                "meta::pure::functions::collection::first"))) {
                    hop = w4.args().get(0);
                }
            }
        }
        // a FILTERED head (the qualifier-calls-qualifier inline:
        // toOne(filter($this.synonyms, pred)).name) carries its predicate
        // into the subquery, substituted through the TARGET's bindings
        TypedLambda extraPred = null;
        if (hop instanceof TypedFilter hf
                && hf.predicate().parameters().size() == 1
                && hf.predicate().body().size() == 1) {
            TypedSpec inner = hf.source();
            while (inner instanceof TypedNativeCall w3 && w3.args().size() == 1
                    && (com.legend.builtin.Pure.isToOneCall(w3.callee().qualifiedName())
                        || w3.callee().qualifiedName().equals(
                            "meta::pure::functions::collection::first"))) {
                inner = w3.args().get(0);
            }
            extraPred = hf.predicate();
            hop = inner;
        }
        SubqueryEnv env = new SubqueryEnv(cs, context,
                parentRowVar, parentRowType);
        HeadRel hr = navHeadRelation(env, hop, thisVar);
        if (hr == null) {
            // DEPTH-2+ chain ($this.classification(bd).exchange(d).name,
            // $row.manager.manager.manager.firstName): peel the FIRST hop
            // off the var to its frame, rebuild the REMAINING chain over
            // the frame's row and recurse — each hop nests one correlated
            // subquery (the engine chains the joins; nesting keeps each
            // hop's window on its own alias)
            java.util.List<TypedSpec> steps = new ArrayList<>();
            TypedSpec walk = hop;
            while (true) {
                TypedSpec w = unwrapToOneFirst(walk);
                if (w instanceof TypedPropertyAccess wp) {
                    steps.add(w);
                    walk = wp.source();
                } else if (w instanceof com.legend.compiler.spec.typed
                        .TypedMilestonedAccess wm) {
                    steps.add(w);
                    walk = wm.source();
                } else {
                    break;
                }
            }
            if (steps.size() < 2 || !(unwrapToOneFirst(walk)
                    instanceof TypedVariable wv
                    && wv.name().equals(thisVar))) {
                return null;
            }
            HeadRel ih = navHeadRelation(env,
                    steps.get(steps.size() - 1), thisVar);
            if (ih == null) {
                return null;
            }
            String iVar = ih.target().rowVar();
            TypedSpec rebuilt = new TypedVariable(iVar,
                    new ExprType(ih.targetRow(),
                            com.legend.compiler.element.type
                                    .Multiplicity.Bounded.ONE));
            for (int si = steps.size() - 2; si >= 0; si--) {
                rebuilt = switch (steps.get(si)) {
                    case TypedPropertyAccess hp -> new TypedPropertyAccess(
                            rebuilt, hp.property(), hp.info());
                    case com.legend.compiler.spec.typed.TypedMilestonedAccess hm ->
                            new com.legend.compiler.spec.typed.TypedMilestonedAccess(
                                    rebuilt, hm.property(), hm.dates(),
                                    hm.sweep(), hm.info());
                    default -> throw new IllegalStateException(
                            "resolver bug: non-access chain step");
                };
            }
            TypedSpec nested = navLeafSubquery(ih.target(),
                    new TypedPropertyAccess(rebuilt,
                            leaf.property(), leaf.info()),
                    iVar, context, iVar, ih.targetRow());
            if (nested == null) {
                return null;
            }
            var nFn = new Type.FunctionType(
                    List.of(new Type.Param(ih.targetRow(),
                            com.legend.compiler.element.type
                                    .Multiplicity.Bounded.ONE)),
                    new Type.Param(leaf.info().type(),
                            com.legend.compiler.element.type
                                    .Multiplicity.Bounded.ZERO_ONE));
            Type.RelationType nCol = new Type.RelationType(List.of(
                    new Type.Column(leaf.property(), leaf.info().type(),
                            com.legend.compiler.element.type
                                    .Multiplicity.Bounded.ZERO_ONE)));
            TypedSpec nProj = new com.legend.compiler.spec.typed.TypedProject(
                    ih.rel(),
                    List.of(new TypedFuncCol(leaf.property(),
                            new TypedLambda(List.of(iVar), List.of(nested),
                                    new ExprType(nFn,
                                            com.legend.compiler.element.type
                                                    .Multiplicity.Bounded.ONE)))),
                    new ExprType(Type.relation(nCol),
                            com.legend.compiler.element.type
                                    .Multiplicity.Bounded.ZERO_MANY));
            return new com.legend.compiler.spec.typed.TypedLimit(nProj,
                    new com.legend.compiler.spec.typed.TypedCInteger(1L,
                            ExprType.one(Type.Primitive.INTEGER)),
                    new ExprType(Type.relation(nCol),
                            com.legend.compiler.element.type
                                    .Multiplicity.Bounded.ZERO_ONE));
        }
        ClassSource target = hr.target();
        Type.RelationType targetRow = hr.targetRow();
        TypedSpec leafBind = target.bindings().get(leaf.property());
        if (leafBind == null
                || leafBind.info().type() instanceof Type.ClassType) {
            return null;
        }
        TypedSpec rel = hr.rel();
        if (extraPred != null) {
            TypedSpec pb;
            try {
                pb = inlineThis(extraPred.body().get(0),
                        extraPred.parameters().get(0), Map.of(),
                        target.bindings(), target.classFqn(),
                        leaf.property());
            } catch (NotImplementedException unsupported) {
                return null;   // the inline route's louder wall takes over
            }
            var predFn = new Type.FunctionType(
                    List.of(new Type.Param(targetRow,
                            com.legend.compiler.element.type
                                    .Multiplicity.Bounded.ONE)),
                    new Type.Param(Type.Primitive.BOOLEAN,
                            com.legend.compiler.element.type
                                    .Multiplicity.Bounded.ONE));
            rel = new TypedFilter(rel,
                    new TypedLambda(List.of(target.rowVar()),
                            List.of(pb), new ExprType(predFn,
                                    com.legend.compiler.element.type
                                            .Multiplicity.Bounded.ONE)),
                    rel.info());
        }
        return scalarLeafSubquery(rel, target.rowVar(), targetRow,
                leaf.property(), leafBind);
    }

    /** The [0..1] scalar-subquery tail shared by the nav-leaf and
     * otherwise-fallback emissions: project ONE leaf column over the
     * corr-filtered relation, DEDUPED — the ENGINE's graph-fetch
     * discipline (D6, witness graphFetchCommon.pure:163: distinct=true
     * + hard-fail; no row-cap vocabulary exists in the reference).
     * Identical rows collapse; genuinely-multiple distinct values hit
     * the backend's own more-than-one-row subquery error. The old
     * {@code LIMIT 1} (commented "pure toOne semantics" — wrong on any
     * lane, pure raises on 2) silently picked a winner in the case the
     * engine treats as fatal AND suppressed that free backend check. */
    static TypedSpec scalarLeafSubquery(TypedSpec rel, String rowVar,
            Type.RelationType targetRow, String leafName, TypedSpec leafBind) {
        var leafFn = new Type.FunctionType(
                List.of(new Type.Param(targetRow,
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE)),
                new Type.Param(leafBind.info().type(),
                        leafBind.info().multiplicity()));
        Type.RelationType oneCol = new Type.RelationType(List.of(
                new Type.Column(leafName, leafBind.info().type(),
                        leafBind.info().multiplicity())));
        TypedSpec proj = new com.legend.compiler.spec.typed.TypedProject(rel,
                List.of(new TypedFuncCol(leafName,
                        new TypedLambda(List.of(rowVar),
                                List.of(leafBind),
                                new ExprType(leafFn,
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ONE)))),
                new ExprType(Type.relation(oneCol),
                        com.legend.compiler.element.type
                                .Multiplicity.Bounded.ZERO_MANY));
        return new com.legend.compiler.spec.typed.TypedDistinct(proj,
                List.of(),
                new ExprType(Type.relation(oneCol),
                        com.legend.compiler.element.type
                                .Multiplicity.Bounded.ZERO_ONE));
    }

    private @com.legend.Nullable TypedSpec derivedLeaf(Map<String, TypedSpec> bindings,
            String classFqn, TypedGraphTree node) {
        return derivedLeaf(bindings, classFqn, node, null);
    }

    private @com.legend.Nullable TypedSpec derivedLeaf(Map<String, TypedSpec> bindings,
            String classFqn, TypedGraphTree node, @com.legend.Nullable SubqueryEnv env) {
        String prop = node.property();
        var p = ctx.findProperty(classFqn, prop).orElse(null);
        if (!(p instanceof com.legend.compiler.element.Property.Derived d)
                || d.parameters().size() != node.args().size()) {
            return null;
        }
        var cf = sources.compileSynthFn(d.bodyFunctionFqn());
        if (cf.body().size() != 1) {
            throw new NotImplementedException("derived graph leaf '" + prop
                    + "' has a multi-statement body — not supported yet");
        }
        // lifted signature = (this, <declared params>...) — call args
        // bind positionally after $this (tree args are typed literals)
        String thisVar = cf.signature().parameters().get(0).name();
        Map<String, TypedSpec> binds = new java.util.LinkedHashMap<>();
        for (int i = 0; i < node.args().size(); i++) {
            binds.put(cf.signature().parameters().get(i + 1).name(),
                    node.args().get(i));
        }
        return inlineThis(cf.body().get(0), thisVar, binds, bindings,
                classFqn, prop, env);
    }

    /** Substitute {@code $this.<stored>} reads with the row bindings.
     * Rebuild arms cover the expression shapes derived bodies produce;
     * an UNHANDLED node still CONTAINING {@code $this} throws loud —
     * never silent (an unreplaced var would only error later at the
     * lowering, far from its cause). */
    /** toOne/first wrappers drop for SHAPE matching (the value they wrap
     * is the read). */
    private static TypedSpec unwrapToOneFirst(TypedSpec v) {
        while (v instanceof TypedNativeCall w && w.args().size() == 1
                && (com.legend.builtin.Pure.isToOneCall(w.callee().qualifiedName())
                    || w.callee().qualifiedName().equals(
                        "meta::pure::functions::collection::first"))) {
            v = w.args().get(0);
        }
        return v;
    }

    private TypedSpec inlineThis(TypedSpec n, String thisVar,
            Map<String, TypedSpec> binds, Map<String, TypedSpec> bindings,
            String classFqn, String prop) {
        return inlineThis(n, thisVar, binds, bindings, classFqn, prop, null);
    }

    private TypedSpec inlineThis(TypedSpec n, String thisVar,
            Map<String, TypedSpec> binds, Map<String, TypedSpec> bindings,
            String classFqn, String prop, @com.legend.Nullable SubqueryEnv env) {
        if (n instanceof TypedVariable bv && binds.containsKey(bv.name())) {
            return binds.get(bv.name());
        }
        // EMBEDDED mid-hop ($this.address.name where 'address' binds to
        // an embedded ctor): the leaf is the ctor's own sub-binding — a
        // parent-row read, no join (the engine's embedded per-leaf rule)
        TypedSpec eSrc = n instanceof TypedPropertyAccess epa0
                ? unwrapToOneFirst(epa0.source()) : null;
        if (n instanceof TypedPropertyAccess epa
                && eSrc instanceof TypedPropertyAccess mid
                && mid.source() instanceof TypedVariable emv
                && emv.name().equals(thisVar)) {
            TypedSpec mb = bindings.get(mid.property());
            while (mb instanceof TypedNativeCall mw && mw.args().size() == 1
                    && (com.legend.builtin.Pure.isToOneCall(mw.callee().qualifiedName())
                        || mw.callee().qualifiedName().equals(
                            "meta::pure::functions::collection::first"))) {
                mb = mw.args().get(0);
            }
            if (mb instanceof com.legend.compiler.spec.typed
                    .TypedNewInstance eni) {
                TypedSpec lb = eni.properties().get(epa.property());
                if (lb != null
                        && !(lb.info().type() instanceof Type.ClassType)) {
                    return lb;
                }
            }
        }
        if (n instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable v
                && v.name().equals(thisVar)) {
            TypedSpec b = bindings.get(pa.property());
            if (b == null || b.info().type() instanceof Type.ClassType) {
                throw new NotImplementedException("derived graph leaf '"
                        + prop + "' reads '" + pa.property() + "' which is "
                        + (b == null ? "not a stored binding"
                                : "class-typed (navigation)")
                        + " on '" + classFqn + "' — only same-class"
                        + " stored reads inline yet");
            }
            return b;
        }
        if (n instanceof TypedVariable v && v.name().equals(thisVar)) {
            throw new NotImplementedException("derived graph leaf '" + prop
                    + "' uses $" + thisVar + " as a whole value —"
                    + " not supported yet");
        }
        // EMPTINESS over a navigation head ($this.assoc->isEmpty()): the
        // corr-filtered target relation feeds the emptiness native — the
        // §133 lowering renders [NOT] EXISTS (engine qualifier goldens)
        if (env != null && n instanceof TypedNativeCall ec
                && ec.args().size() == 1
                && (ec.callee().qualifiedName().equals(
                        "meta::pure::functions::collection::isEmpty")
                    || ec.callee().qualifiedName().equals(
                        "meta::pure::functions::collection::isNotEmpty"))
                && containsVar(ec.args().get(0), thisVar)) {
            HeadRel hr = navHeadRelation(env, ec.args().get(0), thisVar);
            if (hr != null) {
                return new TypedNativeCall(ec.callee(),
                        List.of(hr.rel()), n.info());
            }
        }
        if (n instanceof TypedNativeCall c) {
            List<TypedSpec> args = new ArrayList<>(c.args().size());
            for (TypedSpec a : c.args()) {
                args.add(inlineThis(a, thisVar, binds, bindings, classFqn, prop, env));
            }
            return c.withChildren(args);
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedIf i) {
            return new com.legend.compiler.spec.typed.TypedIf(
                    inlineThis(i.condition(), thisVar, binds, bindings, classFqn, prop, env),
                    inlineThis(i.thenBranch(), thisVar, binds, bindings, classFqn, prop, env),
                    i.elseBranch().map(e ->
                            inlineThis(e, thisVar, binds, bindings, classFqn, prop, env)),
                    n.info());
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedCollection tc) {
            List<TypedSpec> els = new ArrayList<>(tc.elements().size());
            for (TypedSpec e : tc.elements()) {
                els.add(inlineThis(e, thisVar, binds, bindings, classFqn, prop, env));
            }
            return new com.legend.compiler.spec.typed.TypedCollection(
                    els, tc.info());
        }
        if (containsVar(n, thisVar)) {
            // NAVIGATION SUB-EXPRESSION fallback: a nav+leaf chain inside
            // a larger expression emits as its own correlated scalar
            // subquery (map §7 contract 4 — the engine computes qualifier
            // expressions with navigations as embedded scalar subqueries).
            // TO-ONE reads only: the subquery LIMITs 1, so a to-many chain
            // (feeding an aggregate) would silently drop rows — stays loud.
            if (env != null
                    && n.info().multiplicity()
                            instanceof com.legend.compiler.element.type
                                    .Multiplicity.Bounded mb
                    && mb.upper() != null && mb.upper() <= 1) {
                TypedSpec sub = navLeafSubquery(env.cs(), n, thisVar,
                        env.context(), env.rowVar(), env.rowType());
                if (sub != null) {
                    return sub;
                }
            }
            throw new NotImplementedException("derived graph leaf '" + prop
                    + "' body node " + n.getClass().getSimpleName()
                    + " referencing $" + thisVar
                    + " is not inlinable yet");
        }
        return n;
    }

    /** Slot aliases the tree's LEAF bindings read off the child row —
     * the child pipeline's materialization demand (H4b: a leaf mapped
     * through the class's own join slots needs those slots CONVERTED,
     * not stripped; empty demand left them stripped and the leaf walled). */
    private static boolean isDateFamily(Type t) {
        return t == Type.Primitive.DATE || t == Type.Primitive.DATE_TIME
                || t == Type.Primitive.STRICT_DATE;
    }

    private static java.util.Set<String> leafSlotDemand(ClassSource child,
            TypedGraphTree node) {
        java.util.Set<String> universe =
                Pipelines.slotAliases(child.pipeline());
        java.util.Set<String> out = new java.util.LinkedHashSet<>();
        if (universe.isEmpty()) {
            return out;
        }
        for (TypedGraphTree c : node.children()) {
            TypedSpec b = child.bindings().get(c.property());
            if (b == null) {
                continue;
            }
            collectAliasReads(b, child.rowVar(), universe, out);
            // reads WRAPPED in computed expressions (concat over
            // $row.slot.COL) demand the slot the same way — any reference
            // shape converts, the direct pattern above is just the fast path
            for (String a : universe) {
                if (!out.contains(a)
                        && Pipelines.referencesAliasOn(b, child.rowVar(),
                                java.util.Set.of(a))) {
                    out.add(a);
                }
            }
        }
        return out;
    }

    private static void collectAliasReads(TypedSpec n, String rowVar,
            java.util.Set<String> universe, java.util.Set<String> out) {
        if (n instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedPropertyAccess mid
                && mid.source() instanceof TypedVariable v
                && v.name().equals(rowVar)
                && universe.contains(mid.property())) {
            out.add(mid.property());
        }
        for (TypedSpec c : n.children()) {
            collectAliasReads(c, rowVar, universe, out);
        }
    }

    /** The engine's key for a non-aliased qualifier leaf: the SOURCE
     * call spelling — {@code fullName(false)}, {@code name()}. Literal
     * args render in pure form; non-literal args require an alias
     * (loud). */
    private static String callKey(TypedGraphTree node) {
        StringBuilder k = new StringBuilder(node.property()).append('(');
        for (int i = 0; i < node.args().size(); i++) {
            if (i > 0) {
                k.append(", ");
            }
            TypedSpec a = node.args().get(i);
            if (a instanceof com.legend.compiler.spec.typed.TypedCBoolean b) {
                k.append(b.value());
            } else if (a instanceof
                    com.legend.compiler.spec.typed.TypedCInteger ci) {
                k.append(ci.value());
            } else if (a instanceof
                    com.legend.compiler.spec.typed.TypedCString cstr) {
                k.append('\'').append(cstr.value()).append('\'');
            } else if (a instanceof
                    com.legend.compiler.spec.typed.TypedCDate cd) {
                // engine prints the date VALUE with an explicit +0000
                // zone when it carries a time component
                String d = cd.value().toEngineString();
                k.append(d).append(d.indexOf('T') >= 0 ? "+0000" : "");
            } else if (a instanceof
                    com.legend.compiler.spec.typed.TypedVariable v) {
                // a VARIABLE arg keeps its source spelling — the engine
                // key is "classification($bd)" verbatim
                k.append('$').append(v.name());
            } else {
                throw new NotImplementedException("parameterized qualifier"
                        + " tree leaf '" + node.property() + "' with a"
                        + " non-literal argument needs an alias — the"
                        + " rendered-key form only covers literals");
            }
        }
        return k.append(')').toString();
    }

    /**
     * A ->subType(@X){...} node as a member-gated patch: scalar leaves read
     * the row's stc carrier columns (NULL off-member); class-typed children
     * correlate against the SUBTYPE's own ClassSource; the $member witness
     * gates the branch at the envelope.
     */
    private TypedSerializeGraph.SubTypePatch subTypePatch(ClassSource cs,
            TypedGraphTree node, StoreResolver.Context context, String rowVar,
            Type.RelationType rowType,
            java.util.function.Function<@com.legend.Nullable TypedSpec, TypedSpec> toRow) {
        List<TypedFuncCol> patch = new ArrayList<>();
        List<TypedSerializeGraph.Child> patchChildren = new ArrayList<>();
        for (TypedGraphTree sub : node.children()) {
            if (!sub.children().isEmpty()) {
                // CLASS-typed subtype child (coordinate{...}): a correlated
                // child against the SUBTYPE's own ClassSource (its member
                // pipeline holds the binding); it rides INSIDE the
                // member-gated branch, so non-member rows never emit it
                String skey = (context.explicitMapping() == null ? ""
                        : context.explicitMapping()) + '\u0000'
                        + (context.runtimeFqn() == null ? ""
                                : context.runtimeFqn());
                ClassSource subCs = sources.get(cs.mappingFqn(),
                        java.util.Objects.requireNonNull(node.subTypeFqn(), "node.subTypeFqn()"),
                        (target, excl) -> dispatch.apply(context, target), skey,
                        cs.scope());
                patchChildren.add(graphChild(subCs, sub, context,
                        rowVar, rowType,
                        Pipelines.materialize(subCs.pipeline(), Set.of(),
                                subCs.classFqn()).pipeline()));
                continue;
            }
            String col = com.legend.model.ClassMapping.subTypeColumn(
                    java.util.Objects.requireNonNull(node.subTypeFqn(),
                            "subtype node without a class"),
                    sub.property());
            Type.Column rc = rowType.columns().stream()
                    .filter(c -> c.name().equals(col))
                    .findFirst().orElse(null);
            if (rc == null) {
                // DERIVED property of the SUBTYPE (landmarkName(){$this
                // .landmark.lmName}): no carrier exists — inline the
                // lifted body against the SOURCE's bindings (inherited
                // navs); the member witness gates the branch
                TypedSpec dv = derivedLeaf(cs,
                        java.util.Objects.requireNonNull(node.subTypeFqn()), sub,
                        context, rowVar, rowType);
                if (dv != null) {
                    var dFn = new Type.FunctionType(
                            List.of(new Type.Param(rowType,
                                    com.legend.compiler.element.type
                                            .Multiplicity.Bounded.ONE)),
                            new Type.Param(dv.info().type(),
                                    dv.info().multiplicity()));
                    patch.add(new TypedFuncCol(sub.alias() != null
                            ? sub.alias() : callKey(sub),
                            new TypedLambda(List.of(rowVar), List.of(dv),
                                    new ExprType(dFn,
                                            com.legend.compiler.element.type
                                                    .Multiplicity.Bounded
                                                    .ONE))));
                    continue;
                }
                throw new NotImplementedException("graph ->subType(@"
                        + node.subTypeFqn() + "): carrier column '"
                        + col + "' is not on the row (non-union"
                        + " subtype mapping) — not built yet");
            }
            TypedSpec read = new TypedPropertyAccess(
                    toRow.apply(null), col,
                    new ExprType(rc.type(), rc.multiplicity()));
            var pFn = new Type.FunctionType(
                    List.of(new Type.Param(rowType,
                            com.legend.compiler.element.type
                                    .Multiplicity.Bounded.ONE)),
                    new Type.Param(rc.type(), rc.multiplicity()));
            patch.add(new TypedFuncCol(keyOf(sub),
                    new TypedLambda(List.of(rowVar), List.of(read),
                            new ExprType(pFn,
                                    com.legend.compiler.element.type
                                            .Multiplicity.Bounded.ONE))));
        }
        String mcol = com.legend.model.ClassMapping.subTypeColumn(
                java.util.Objects.requireNonNull(node.subTypeFqn(),
                        "subtype node without a class"),
                com.legend.model.ClassMapping.memberWitness());
        Type.Column mrc = rowType.columns().stream()
                .filter(c -> c.name().equals(mcol))
                .findFirst().orElseThrow(() ->
                        new NotImplementedException("graph ->subType(@"
                                + node.subTypeFqn() + "): membership"
                                + " witness '" + mcol
                                + "' is not on the row"));
        TypedSpec mread = new TypedPropertyAccess(
                toRow.apply(null), mcol,
                new ExprType(mrc.type(), mrc.multiplicity()));
        var mFn = new Type.FunctionType(
                List.of(new Type.Param(rowType,
                        com.legend.compiler.element.type
                                .Multiplicity.Bounded.ONE)),
                new Type.Param(mrc.type(), mrc.multiplicity()));
        TypedFuncCol member = new TypedFuncCol(mcol,
                new TypedLambda(List.of(rowVar), List.of(mread),
                        new ExprType(mFn,
                                com.legend.compiler.element.type
                                        .Multiplicity.Bounded.ONE)));
        return new TypedSerializeGraph.SubTypePatch(
                java.util.Objects.requireNonNull(node.subTypeFqn(), "node.subTypeFqn()"),
                patch, member, patchChildren);
    }

    /** The supported serialize-config surface: includeType (+ typeKeyName,
     * fullyQualifiedTypePath) emit the type key; every OTHER envelope-
     * changing flag walls loudly — never a silently-ignored config. */
    record SerializeTypeConfig(@com.legend.Nullable String typeKey,
            boolean fq, boolean includeEnumType, boolean removeNull,
            boolean removeEmpty, boolean includeObjectReference) {
    }

    static @com.legend.Nullable SerializeTypeConfig serializeTypeConfig(TypedSpec cfg) {
        // the alloyConfig ctor family (graphFetch.pure:126-171): decode
        // the CALL positionally by arity into the same flag surface
        if (cfg instanceof TypedNativeCall cc
                && "meta::pure::graphFetch::execution::alloyConfig"
                        .equals(cc.callee().qualifiedName())) {
            List<TypedSpec> a = cc.args();
            int n = a.size();
            boolean includeType = boolArg(a.get(0));
            boolean includeEnumType = boolArg(a.get(1));
            int rn = n == 8 ? 3 : 2;
            String key = n >= 6 ? strArg(a.get(n == 8 ? 5 : 4), "@type")
                    : "@type";
            boolean fq = n >= 6 ? boolArg(a.get(n == 8 ? 6 : 5)) : true;
            boolean ior = n == 5 ? boolArg(a.get(4))
                    : n >= 7 ? boolArg(a.get(n - 1)) : false;
            boolean removeNull = boolArg(a.get(rn));
            boolean removeEmpty = boolArg(a.get(rn + 1));
            return includeType || includeEnumType || removeNull
                    || removeEmpty || ior
                    ? new SerializeTypeConfig(includeType ? key : null,
                            includeType && fq, includeEnumType,
                            removeNull, removeEmpty, ior)
                    : null;
        }
        if (!(cfg instanceof com.legend.compiler.spec.typed.TypedNewInstance ni)
                || !"meta::pure::graphFetch::execution::AlloySerializationConfig"
                        .equals(ni.classFqn())) {
            throw new NotImplementedException("serialize config of shape "
                    + cfg.getClass().getSimpleName() + " is not supported yet");
        }
        boolean includeType = false;
        boolean includeEnumType2 = false;
        boolean fq = false;
        String key = "@type";
        for (var e : ni.properties().entrySet()) {
            TypedSpec v = e.getValue();
            switch (e.getKey()) {
                case "typeKeyName" -> {
                    if (v instanceof com.legend.compiler.spec.typed.TypedCString cs) {
                        key = cs.value();
                    }
                }
                case "includeType" -> includeType =
                        v instanceof com.legend.compiler.spec.typed
                                .TypedCBoolean b && b.value();
                case "includeEnumType" -> includeEnumType2 =
                        v instanceof com.legend.compiler.spec.typed
                                .TypedCBoolean b && b.value();
                case "fullyQualifiedTypePath" -> fq =
                        v instanceof com.legend.compiler.spec.typed
                                .TypedCBoolean b && b.value();
                default -> {
                    boolean nop = v instanceof com.legend.compiler.spec.typed
                            .TypedCBoolean b && !b.value();
                    if (!nop) {
                        throw new NotImplementedException("serialize config"
                                + " flag '" + e.getKey() + "' is not"
                                + " supported yet");
                    }
                }
            }
        }
        return includeType || includeEnumType2
                ? new SerializeTypeConfig(includeType ? key : null,
                        includeType && fq, includeEnumType2, false, false,
                        false)
                : null;
    }

    private static boolean boolArg(TypedSpec v) {
        return v instanceof com.legend.compiler.spec.typed.TypedCBoolean b
                && b.value();
    }

    private static String strArg(TypedSpec v, String dflt) {
        return v instanceof com.legend.compiler.spec.typed.TypedCString cs
                ? cs.value() : dflt;
    }

    /** Stamp the type-key config on EVERY node of a built envelope
     * (nested + patch children) — one application at the serialize arm.
     * includeEnumType wraps ENUM-typed leaf bodies with the enum's FQN
     * prefix ('pkg::Enum.VALUE' — the engine's includeEnumType form);
     * {@code plusCallee} is the registered String+String overload. */
    static TypedSerializeGraph withTypeKey(TypedSerializeGraph g,
            SerializeTypeConfig c,
            com.legend.compiler.element.TypedFunction plusCallee) {
        return withTypeKey(g, c, plusCallee, null);
    }

    /** {@code objectRefPrefix} stamps the ROOT node only (children never
     * carry the ASOR channel). */
    static TypedSerializeGraph withTypeKey(TypedSerializeGraph g,
            SerializeTypeConfig c,
            com.legend.compiler.element.TypedFunction plusCallee,
            @com.legend.Nullable String objectRefPrefix) {
        List<TypedFuncCol> leaves = c.includeEnumType()
                ? g.leaves().stream().map(l ->
                        enumPrefixed(l, plusCallee)).toList()
                : g.leaves();
        return new TypedSerializeGraph(g.source(), g.rowVar(), leaves,
                g.nested().stream().map(ch -> new TypedSerializeGraph.Child(
                        ch.property(), withTypeKey(ch.node(), c, plusCallee)))
                        .toList(),
                g.arrayWrap(), g.bareValue(), g.classFqn(), g.info(),
                g.inlineChild(),
                g.subTypePatches().stream().map(p ->
                        new TypedSerializeGraph.SubTypePatch(p.subTypeFqn(),
                                c.includeEnumType()
                                        ? p.leaves().stream().map(l ->
                                                enumPrefixed(l, plusCallee))
                                                .toList()
                                        : p.leaves(),
                                p.member(),
                                p.children().stream().map(ch ->
                                        new TypedSerializeGraph.Child(
                                                ch.property(),
                                                withTypeKey(ch.node(), c,
                                                        plusCallee)))
                                        .toList()))
                        .toList(),
                g.orderKeys(), c.typeKey(), c.fq(),
                g.checkedConstraints(), c.removeNull(), c.removeEmpty(),
                objectRefPrefix);
    }

    /** The ASOR objectReference STATIC prefix (engine store-object-
     * reference protocol, decoded from the goldens): '001:010:' +
     * len10-prefixed segments — store kind, the DEFINING (include-aware)
     * mapping, the mangled set id twice, the canonical test-H2
     * connection protocol JSON. The per-row pk segment appends in SQL. */
    static String asorPrefix(ModelContext mc, ClassSource cs) {
        // F3.4: the protocol lives ONCE in AsorRef; this keeps only the
        // model-derived inputs (defining mapping, set id)
        String defining = definingMapping(mc, cs.mappingFqn(), cs.classFqn());
        String setId = cs.classFqn().replace("::", "_");
        return AsorRef.prefix(defining, setId, setId);
    }

    /** The include-closure mapping that DEFINES the class binding (the
     * engine's reference names the OWNING mapping, not the queried one):
     * own declarations win, else the first include that defines it. */
    private static String definingMapping(ModelContext mc, String mappingFqn,
            String classFqn) {
        String r = definingMapping0(mc, mappingFqn, classFqn);
        return r == null ? mappingFqn : r;
    }

    private static @com.legend.Nullable String definingMapping0(
            ModelContext mc, String mappingFqn, String classFqn) {
        var m = mc.findMapping(mappingFqn).orElse(null);
        if (m == null) {
            return null;
        }
        for (var inc : m.includes()) {
            String r = definingMapping0(mc, inc.mappingPath(), classFqn);
            if (r != null) {
                return r;
            }
        }
        return m.classBindings().stream()
                .anyMatch(cb -> cb.classFqn().equals(classFqn))
                ? mappingFqn : null;
    }

    /** The String+String plus overload — the includeEnumType prefix. */
    static com.legend.compiler.element.TypedFunction stringPlusCallee(
            ModelContext mc) {
        return mc.findFunction("meta::pure::functions::math::plus").stream()
                .filter(f -> f.parameters().size() == 2
                        && f.parameters().get(0).type()
                                == Type.Primitive.STRING)
                .findFirst().orElseThrow(() -> new IllegalStateException(
                        "resolver bug: no String plus registration"));
    }

    /** An ENUM-typed leaf body concat-prefixed with its enumeration FQN
     * (a NULL enum rides concat's null-skip — acceptable until the
     * removeNull sub-slice owns null keys). Non-enum leaves unchanged. */
    private static TypedFuncCol enumPrefixed(TypedFuncCol l,
            com.legend.compiler.element.TypedFunction plusCallee) {
        if (!(l.fn() instanceof TypedLambda lam)
                || lam.body().isEmpty()) {
            return l;
        }
        TypedSpec body = lam.body().get(lam.body().size() - 1);
        if (!(body.info().type() instanceof Type.EnumType et)) {
            return l;
        }
        var one = com.legend.compiler.element.type.Multiplicity.Bounded.ONE;
        TypedSpec prefixed = new TypedNativeCall(plusCallee, List.of(
                new com.legend.compiler.spec.typed.TypedCString(
                        et.fqn() + ".",
                        new ExprType(Type.Primitive.STRING, one)),
                body),
                new ExprType(Type.Primitive.STRING,
                        body.info().multiplicity()));
        List<TypedSpec> nb = new ArrayList<>(lam.body());
        nb.set(nb.size() - 1, prefixed);
        return new TypedFuncCol(l.name(), new TypedLambda(lam.parameters(),
                nb, lam.info()));
    }

    /** β-substitute free variables by name over the derived-body
     * vocabulary (shadowing lambdas drop their params). */
    private static TypedSpec substVars(TypedSpec n, Map<String, TypedSpec> sub) {
        if (sub.isEmpty()) {
            return n;
        }
        return switch (n) {
            case TypedVariable v ->
                    sub.getOrDefault(v.name(), v);
            case TypedPropertyAccess pa -> new TypedPropertyAccess(
                    substVars(pa.source(), sub), pa.property(), pa.info());
            case com.legend.compiler.spec.typed.TypedMilestonedAccess ma ->
                    new com.legend.compiler.spec.typed.TypedMilestonedAccess(
                            substVars(ma.source(), sub), ma.property(),
                            ma.dates().stream().map(d2 -> substVars(d2, sub))
                                    .toList(),
                            ma.sweep(), ma.info());
            case TypedNativeCall c -> c.withChildren(c.args().stream().map(a -> substVars(a, sub)).toList());
            case TypedFilter f -> new TypedFilter(substVars(f.source(), sub),
                    (TypedLambda) substVars(f.predicate(), sub), f.info());
            case TypedLambda l -> {
                Map<String, TypedSpec> inner = new java.util.LinkedHashMap<>(sub);
                l.parameters().forEach(inner::remove);
                yield new TypedLambda(l.parameters(),
                        l.body().stream().map(b2 -> substVars(b2, inner))
                                .toList(), l.info());
            }
            case com.legend.compiler.spec.typed.TypedIf i ->
                    new com.legend.compiler.spec.typed.TypedIf(
                            substVars(i.condition(), sub),
                            substVars(i.thenBranch(), sub),
                            i.elseBranch().map(e -> substVars(e, sub)),
                            i.info());
            case com.legend.compiler.spec.typed.TypedCollection tc ->
                    new com.legend.compiler.spec.typed.TypedCollection(
                            tc.elements().stream()
                                    .map(e -> substVars(e, sub)).toList(),
                            tc.info());
            default -> n;
        };
    }

    /** The serialized key: the tree alias when given, else the
     * property name. */
    private static String keyOf(TypedGraphTree node) {
        if (node.alias() != null) {
            return node.alias();
        }
        return node.args().isEmpty() ? node.property() : callKey(node);
    }

    private static boolean containsVar(TypedSpec n, String var) {
        return com.legend.compiler.spec.typed.VarUse.reads(n, var);
    }
}
