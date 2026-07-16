// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

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
        List<TypedGraphTree> tree = new ArrayList<>();
        for (Map.Entry<String, TypedSpec> e : cs.bindings().entrySet()) {
            TypedSpec inner = e.getValue();
            if (inner instanceof TypedNativeCall c
                    && c.args().size() == 1
                    && c.callee().qualifiedName().equals("meta::pure::functions::multiplicity::toOne")) {
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
        String strat = temporal.temporalStrategy(cs.classFqn());
        if (strat != null) {
            if (!"processingtemporal".equals(strat)
                    && !cs.bindings().containsKey("businessDate")) {
                tree.add(new TypedGraphTree("businessDate", List.of()));
            }
            if (!"businesstemporal".equals(strat)
                    && !cs.bindings().containsKey("processingDate")) {
                tree.add(new TypedGraphTree("processingDate", List.of()));
            }
        }
        return tree;
    }

    /** The generated date's VALUE for an envelope leaf: the fetch context
     * date when one exists (point fetch), else the row's own
     * validity-start milestone column (version sweep); {@code null} when
     * the property is not a generated date here. */
    TypedSpec generatedDateLeaf(ClassSource cs, String prop,
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
                ? "genProcessingDate" : "genBusinessDate");
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
        var rowType = (Type.RelationType)
                pipeline.info().type();
        UnaryOperator<TypedSpec> toRow = v -> new TypedVariable(
                rowVar, new ExprType(rowType,
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
        List<TypedFuncCol> leaves = new ArrayList<>();
        List<TypedSerializeGraph.Child> children = new ArrayList<>();
        for (TypedGraphTree node : tree) {
            if (!node.children().isEmpty()
                    || (!cs.bindings().containsKey(node.property())
                            && ctx.findAssociationOf(cs.classFqn(), node.property())
                                    .isPresent())) {
                children.add(graphChild(cs, node, context, rowVar, rowType));
                continue;
            }
            TypedSpec binding = cs.bindings().get(node.property());
            if (binding == null) {
                // GENERATED temporal-context property on the envelope:
                // point fetch = the context date constant; VERSION SWEEP =
                // each row's own validity-start column (engine: the
                // property maps to BUS_FROM / PROCESSING_IN / snapshot)
                TypedSpec gen = generatedDateLeaf(cs, node.property(),
                        rowType, rowVar);
                if (gen == null) {
                    throw new MappingResolutionException("property '"
                            + node.property() + "' of class '" + cs.classFqn()
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
                leaves.add(new TypedFuncCol(node.property(),
                        new TypedLambda(List.of(rowVar), List.of(gen),
                                new ExprType(genFn,
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ONE))));
                continue;
            }
            TypedSpec inner = binding;
            if (inner instanceof TypedNativeCall c
                    && c.args().size() == 1
                    && c.callee().qualifiedName().equals("meta::pure::functions::multiplicity::toOne")) {
                inner = c.args().get(0);
            }
            // A TO-MANY PRIMITIVE leaf ($row.<slot>.COL through a to-many
            // navigate slot): reading it as a joined scalar EXPLODES the
            // parent rows — the engine serializes ["abc","def"] per parent.
            // Emit a bare-value child: the slot target correlated on the
            // parent row, aggregating the raw column values.
            if (inner instanceof TypedPropertyAccess colPa
                    && colPa.source() instanceof TypedPropertyAccess slotPa
                    && slotPa.source() instanceof TypedVariable slotV
                    && slotV.name().equals(cs.rowVar())
                    // multiplicity of the RAW read (a toOne conformance
                    // wrapper on the binding would mask the [*])
                    && !(colPa.info().multiplicity()
                            instanceof com.legend.compiler.element.type
                                    .Multiplicity.Bounded b1
                            && Integer.valueOf(1).equals(b1.upper()))) {
                var navPrim = Pipelines.navSteps(cs.pipeline()).get(slotPa.property());
                if (navPrim != null) {
                    children.add(primitiveArrayChild(node.property(),
                            navPrim.target(), navPrim.predicate(),
                            colPa, rowVar, rowType));
                    continue;
                }
                var slotPrim = Pipelines.joinSlots(cs.pipeline()).get(slotPa.property());
                if (slotPrim != null) {
                    children.add(primitiveArrayChild(node.property(),
                            slotPrim.target(), slotPrim.condition(),
                            colPa, rowVar, rowType));
                    continue;
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
                    slotPrefixes, stripped, toRow);
            var fnType = new Type.FunctionType(
                    List.of(new Type.Param(rowType,
                            com.legend.compiler.element.type.Multiplicity.Bounded.ONE)),
                    new Type.Param(
                            body.info().type(), body.info().multiplicity()));
            leaves.add(new TypedFuncCol(node.property(),
                    new TypedLambda(List.of(rowVar), List.of(body),
                            new ExprType(fnType,
                                    com.legend.compiler.element.type.Multiplicity.Bounded.ONE))));
        }
        return new TypedSerializeGraph(pipeline, rowVar, leaves, children,
                arrayWrap, info);
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
        Type.RelationType targetRow =
                (Type.RelationType) targetPipeline.info().type();
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
                targetPipeline.info());
        String childVar = "_r" + freshVar.getAsInt();
        TypedSpec value = new TypedPropertyAccess(
                new TypedVariable(childVar,
                        new ExprType(targetRow,
                                com.legend.compiler.element.type
                                        .Multiplicity.Bounded.ONE)),
                colRead.property(), colRead.info());
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
            Type.RelationType parentRowType) {
        if (node.children().isEmpty()) {
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
            // Unwrap the M2M cast (^Target($src.assocProp)) and toOne.
            if (inner instanceof TypedNewInstanceCast nic) {
                inner = nic.source();
            }
            if (inner instanceof TypedNativeCall c1
                    && c1.args().size() == 1
                    && c1.callee().qualifiedName().equals("meta::pure::functions::multiplicity::toOne")) {
                inner = c1.args().get(0);
            }
            if (inner instanceof TypedNewInstanceCast nic2) {
                inner = nic2.source();
            }
            if (inner instanceof TypedPropertyAccess pa
                    && pa.source() instanceof TypedVariable v
                    && v.name().equals(cs.rowVar())
                    && v.info().type() instanceof Type.ClassType srcCls
                    && ctx.findAssociationOf(srcCls.fqn(), pa.property()).isPresent()) {
                return m2mAssocChild(cs, node, srcCls.fqn(), pa.property(),
                        context, parentRowVar, parentRowType);
            }
            // A NAVIGATE-SLOT read ($row.<alias>, the relational
            // association injected into the source pipeline): the slot's
            // TypedNavigate carries the raw target and the join predicate.
            if (inner instanceof TypedPropertyAccess pa2
                    && pa2.source() instanceof TypedVariable v2
                    && v2.name().equals(cs.rowVar())) {
                var navSteps = Pipelines.navSteps(cs.pipeline());
                var nav = navSteps.get(pa2.property());
                if (nav != null) {
                    return navSlotChild(cs, node, nav,
                            b0 instanceof TypedNewInstanceCast nic0
                                    ? nic0.classFqn() : null,
                            context, parentRowVar, parentRowType);
                }
            }
            throw new NotImplementedException("graph child '" + node.property()
                    + "' of class '" + cs.classFqn() + "' is mapped as an"
                    + " embedded/join-slot/otherwise/M2M binding — only"
                    + " association children are supported yet (H4b/H5c)");
        }
        AssociationJoins.AssocJoin aj = assocMaterial.associationJoin(temporal, cs, node.property(), context, /*forExists*/ true);
        var assoc = ctx.findAssociationOf(cs.classFqn(), node.property()).orElseThrow();
        var end = assoc.property1().propertyName().equals(node.property())
                ? assoc.property1() : assoc.property2();
        boolean toMany = !end.isToOne();
        return correlatedGraphChild(aj.target(), aj.targetPipeline(), aj.targetRow(),
                aj.condition(), toMany, node, parentRowVar, parentRowType, context);
    }

    /**
     * A graph child through a NAVIGATE SLOT: the slot's raw target composes
     * into the child's declared (possibly M2M) class; the slot predicate
     * λ(sourceRow, targetRow) correlates them.
     */
    TypedSerializeGraph.Child navSlotChild(ClassSource cs, TypedGraphTree node,
            TypedNavigate nav, String castClassFqn,
            StoreResolver.Context context, String parentRowVar,
            Type.RelationType parentRowType) {
        String key = (context.explicitMapping() == null ? "" : context.explicitMapping())
                + '\u0000'
                + (context.runtimeFqn() == null ? "" : context.runtimeFqn());
        String rawTarget = ((TypedGetAll) nav.target()).classFqn();
        var prop = ctx.findProperty(cs.classFqn(), node.property()).orElseThrow(
                () -> new IllegalStateException("resolver bug: graph child '"
                        + node.property() + "' is not a property of '"
                        + cs.classFqn() + "'"));
        String childClass = castClassFqn != null ? castClassFqn
                : prop.type() instanceof Type.ClassType cc
                        ? cc.fqn() : null;
        if (childClass == null) {
            throw new IllegalStateException("resolver bug: navigate-slot graph child '"
                    + node.property() + "' is not class-typed");
        }
        boolean toMany = !(prop.multiplicity()
                instanceof com.legend.compiler.element.type.Multiplicity.Bounded bm
                && Integer.valueOf(1).equals(bm.upper()));
        ClassSource child = childClass.equals(rawTarget)
                ? sources.get(dispatch.apply(context, rawTarget), rawTarget,
                        target -> dispatch.apply(context, target), key)
                : sources.get(cs.mappingFqn(), childClass,
                        target -> dispatch.apply(context, target), key);
        // The slot predicate's right side reads the RAW TARGET's physical
        // columns — the child's composed pipeline must bottom at that same
        // row or the correlation filters the wrong relation (audit: the
        // m2mAssocChild guard, applied to this sibling too).
        if (!childClass.equals(rawTarget)) {
            ClassSource rawSource = sources.get(dispatch.apply(context, rawTarget), rawTarget,
                    target -> dispatch.apply(context, target), key);
            if (!child.rowVar().equals(rawSource.rowVar())) {
                throw new NotImplementedException("navigate-slot graph child '"
                        + node.property() + "': the child class '" + childClass
                        + "' does not compose the slot target '" + rawTarget
                        + "' — cross-source children are not supported yet");
            }
        }
        Pipelines.Materialized cMat = Pipelines.materialize(
                child.pipeline(), Set.of(), childClass);
        return correlatedGraphChild(child, cMat.pipeline(),
                (Type.RelationType)
                        cMat.pipeline().info().type(),
                nav.predicate(), toMany, node, parentRowVar, parentRowType, context);
    }

    /**
     * An M2M child through the SOURCE class's association: the upstream
     * association supplies the condition and the RAW target; the child
     * serialized is the node property's DECLARED M2M class, whose composed
     * pipeline bottoms at that same raw target row (M2M composition
     * preserves the inner pipeline and row var — the correlation aligns by
     * construction, asserted loudly).
     */
    TypedSerializeGraph.Child m2mAssocChild(ClassSource cs, TypedGraphTree node,
            String srcClassFqn, String assocProp, StoreResolver.Context context,
            String parentRowVar,
            Type.RelationType parentRowType) {
        String key = (context.explicitMapping() == null ? "" : context.explicitMapping())
                + '\u0000'
                + (context.runtimeFqn() == null ? "" : context.runtimeFqn());
        ClassSource rawParent = sources.get(dispatch.apply(context, srcClassFqn), srcClassFqn,
                target -> dispatch.apply(context, target), key);
        AssociationJoins.AssocJoin aj = assocMaterial.associationJoin(temporal, rawParent, assocProp, context, /*forExists*/ true);
        var prop = ctx.findProperty(cs.classFqn(), node.property()).orElseThrow(
                () -> new IllegalStateException("resolver bug: graph child '"
                        + node.property() + "' is not a property of '"
                        + cs.classFqn() + "'"));
        // Cardinality from the DECLARED property — the spec the consumer
        // typed against — not the source association's end (consistent with
        // navSlotChild; audit).
        boolean toMany = !(prop.multiplicity()
                instanceof com.legend.compiler.element.type.Multiplicity.Bounded bm
                && Integer.valueOf(1).equals(bm.upper()));
        if (!(prop.type() instanceof Type.ClassType childCls)) {
            throw new IllegalStateException("resolver bug: M2M graph child '"
                    + node.property() + "' is not class-typed");
        }
        ClassSource child = sources.get(cs.mappingFqn(), childCls.fqn(),
                target -> dispatch.apply(context, target), key);
        if (!child.rowVar().equals(aj.target().rowVar())) {
            throw new NotImplementedException("M2M graph child '" + node.property()
                    + "': the child class '" + childCls.fqn() + "' does not compose"
                    + " the association target '" + aj.target().classFqn()
                    + "' — cross-source M2M children are not supported yet");
        }
        Pipelines.Materialized cMat = Pipelines.materialize(
                child.pipeline(), Set.of(), childCls.fqn());
        return correlatedGraphChild(child, cMat.pipeline(),
                (Type.RelationType)
                        cMat.pipeline().info().type(),
                aj.condition(), toMany, node, parentRowVar, parentRowType, context);
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
                targetPipeline.info());
        Set<String> childParams = new LinkedHashSet<>();
        for (TypedSpec b : target.bindings().values()) {
            StoreResolver.collectLambdaParams(b, childParams);
        }
        childParams.addAll(cond.parameters());
        String childVar;
        do {
            childVar = "_r" + freshVar.getAsInt();
        } while (childParams.contains(childVar));
        var childInfo = new ExprType(
                new Type.ClassType(target.classFqn()),
                toMany ? com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_MANY
                        : com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_ONE);
        TypedSerializeGraph child = buildGraphNode(target, childRel, Map.of(),
                Pipelines.slotAliases(target.pipeline()), childVar,
                node.children(), context, toMany, childInfo);
        return new TypedSerializeGraph.Child(node.property(), child);
    }
}
