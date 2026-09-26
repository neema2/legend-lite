// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.Property;
import com.legend.compiler.element.TypedClass;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCDate;
import com.legend.compiler.spec.typed.TypedCDecimal;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedConcatenate;
import com.legend.compiler.spec.typed.TypedEnumValue;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedDistinct;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedJoinSlot;
import com.legend.error.NotImplementedException;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNavigate;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.MappingResolutionException;
import com.legend.model.ClassMapping;
import com.legend.model.KeyThread;
import com.legend.model.MappingDefinition;
import com.legend.model.MappingInclude;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * THE STACK (legacy routes as composition, design §9 / §11): the class
 * source of an {@code Operation} binding, built from its ARMS. The
 * binding's function is {@code m1() -> concatenate(m2()) …}: each call is a
 * set's own function, resolved under the queried mapping, and each arm's
 * {@link ClassSource} is taken as it is — a relational set, a
 * {@code ~func} set, a model-to-model set, a nested stack. The law: every
 * operation on the stack is that operation per arm, stacked.
 *
 * <p>The ROW: the class's scalar properties by each arm's bindings (a typed
 * NULL where an arm has none), the embedded constructors' leaves per arm
 * and the constructor rebuilt above, the subtype-dispatch columns and
 * membership witness per arm from the arm's class, every arm's own primary
 * key ({@code <col>_<ordinal>}, the shared table key once), and the source
 * columns of the lifts.
 *
 * <p>THE LIFTS: for every class-typed property some arm navigates, ONE
 * navigate step above the concatenate composed from the arms' own steps.
 * Arm steps of one SHAPE (same target set's function, same condition with
 * the target reads erased and the source reads by bare path) become one
 * route sharing one key, so uniform arms join on one equality; different
 * shapes keep their routes and OR (the five-shape pass, §9). Each arm
 * projects the source columns of its own shape group ({@code __s_…}),
 * NULL for the others. Three rules from the engine ride as lift rules:
 * (a) MERGED — a stack target covered by one single-hop route per arm,
 * every route reading the same target PROPERTY columns: the relational
 * path cross-matches by value, so the predicate is one equality of
 * coalesces over the groups' keys, and the strict per-group OR rides as
 * the paired variant for graph children; (b) keys are shared across arms
 * only when their route shapes agree (the union-to-union trap row); (c) a
 * NON-stack target reached by every arm through the same join into
 * distinct private sets routes to the LAST arm's set only. Nothing here
 * reads the mapping text: the arms' steps are the facts.
 */
final class StackBuilder {

    static final String ROW_VAR = "u_row";

    private final ModelContext ctx;
    private final ClassSources sources;
    private final Callees callees;
    /** stack source -> its LEAF arms (a nested stack flattens to its leaves). */
    private final IdentityHashMap<ClassSource, List<ClassSource>> leaves = new IdentityHashMap<>();

    StackBuilder(ModelContext ctx, ClassSources sources) {
        this.callees = new Callees(ctx);
        this.ctx = ctx;
        this.sources = sources;
    }

    /** The leaf arms of a stack source; null for a non-stack source. */
    @com.legend.base.Nullable List<ClassSource> leavesOf(ClassSource cs) {
        return leaves.get(cs);
    }

    /** An arm's pipeline WITHOUT its own navigate steps: those steps are the
     * facts the lifts compose above the stack, never joins inside the arm
     * (a same-named inner step would be demanded by the outer alias and
     * materialize the lifted join inside the arm). Join slots stay: the
     * arm's bindings read them. Spine only (the steps sit on it). */
    static TypedSpec withoutNavSteps(TypedSpec pipe) {
        if (pipe instanceof TypedNavigate nav && nav.form() == TypedNavigate.Form.PRE_MAP) {
            return withoutNavSteps(nav.source());
        }
        List<TypedSpec> kids = pipe.children();
        if (kids.isEmpty()) {
            return pipe;
        }
        TypedSpec first = withoutNavSteps(kids.get(0));
        if (first == kids.get(0)) {
            return pipe;
        }
        return rechild0(pipe, first);
    }

    /** {@code node} over another first child, RETYPED by the node's KIND:
     * a step that adds its slot column (a navigate, a join slot) types as
     * the new child's row plus that column; a row-preserving step (filter,
     * distinct, sort) as the new child's row; a projection keeps its own
     * output. No column-name arithmetic. */
    static TypedSpec rechild0(TypedSpec node, TypedSpec new0) {
        List<TypedSpec> kids = node.children();
        List<TypedSpec> out = new ArrayList<>(kids);
        out.set(0, new0);
        TypedSpec re = node.withChildren(out);
        Type.RelationType newBase = rowOf(new0.info().type());
        Type.RelationType oldRow = rowOf(node.info().type());
        if (newBase == null || oldRow == null) {
            return re;
        }
        String slot = switch (node) {
            case TypedNavigate nav -> nav.alias().orElse(null);
            case TypedJoinSlot js -> js.alias();
            default -> null;
        };
        Type.RelationType row;
        if (slot != null) {
            Type.Column sc = columnOf(oldRow, slot);
            if (sc == null) {
                throw new IllegalStateException("resolver bug: step '" + slot
                        + "' does not carry its own slot column");
            }
            List<Type.Column> cols = new ArrayList<>(newBase.columns());
            cols.removeIf(c -> c.name().equals(slot));
            cols.add(sc);
            row = new Type.RelationType(cols);
        } else if (node instanceof TypedFilter || node instanceof TypedDistinct
                || node instanceof com.legend.compiler.spec.typed.TypedSortBy) {
            row = newBase;
        } else {
            return re;   // a projection (or any other node) keeps its own output
        }
        Type t = node.info().type() instanceof Type.RelationType ? row : Type.relation(row);
        return re.withInfo(new ExprType(t, node.info().multiplicity()));
    }

    /** A copy of a stack source (re-scoped) keeps the stack's leaves. */
    void alias(ClassSource original, ClassSource copy) {
        List<ClassSource> ls = leaves.get(original);
        if (ls != null) {
            leaves.put(copy, ls);
        }
    }

    /** One arm of a stack: its source (a leaf set, or a nested stack), the
     * pipeline it projects (its own, or a route's rows re-rooted onto it),
     * and the route it came from (null for an operation's own member). */
    record Arm(ClassSource src, TypedSpec pipe,
            com.legend.compiler.spec.typed.TypedNavigate.@com.legend.base.Nullable Route route) {
    }

    ClassSource build(String mappingFqn, String classFqn, MappingDefinition mapping,
            MappingDefinition.ClassBinding.Operation op,
            @com.legend.base.Nullable java.util.function.BiFunction<String, String, String> upstreamMapping,
            String contextKey) {
        List<Arm> arms = new ArrayList<>(op.memberSetIds().size());
        for (String setId : op.memberSetIds()) {
            MappingDefinition.ClassBinding cb = sources.findBindingBySetId(mapping, setId,
                    new LinkedHashSet<>());
            if (cb == null) {
                throw new MappingResolutionException("operation '" + classFqn + "' names member set '"
                        + setId + "', which mapping '" + mappingFqn
                        + "' and its includes do not bind", classFqn);
            }
            if (cb.classFqn().equals(classFqn) && cb.setId() == null) {
                throw new IllegalStateException("resolver bug: operation '" + classFqn
                        + "' names its own class-level binding '" + setId + "'");
            }
            ClassSource arm = sources.get(mappingFqn, cb.classFqn(), cb.setId(), upstreamMapping,
                    contextKey, null);
            arms.add(new Arm(arm, withoutNavSteps(arm.pipeline()), null));
        }
        return stackOf(mappingFqn, classFqn, mapping, arms);
    }

    // ------------------------------------------------------------------
    // the row
    // ------------------------------------------------------------------

    /** One column of the stack row: its name and type, and per arm the
     * value the arm projects (absent = a typed NULL). */
    private record Col(String name, Type type, Multiplicity mult, Map<Integer, TypedSpec> perArm) {
        Col(String name, Type type, Multiplicity mult) {
            this(name, type, mult, new LinkedHashMap<>());
        }
    }

    /** THE ONE UNION BUILDER: an operation's arms, or a routed navigate's
     * arms (one per route and leaf, the route's rows on the leaf's
     * pipeline, the route's target reads projected as its keys). */
    /** A DEMANDED column of the stack row beyond the class's own: its name,
     * type, and per arm the value the arm projects (absent = a typed NULL)
     * — the per-member child-route keys of a mixed union, the columns a
     * later reader demands. */
    record Extra(String name, Type type, Map<Integer, TypedSpec> perArm) {
    }

    ClassSource stackOf(String mappingFqn, String classFqn, MappingDefinition mapping,
            List<Arm> arms) {
        return stackOf(mappingFqn, classFqn, mapping, arms, List.of(), true);
    }

    /** {@code extras}: demanded columns beyond the class's own; {@code lifts}
     * false = the arms' navigate steps stay INSIDE the arms (a mixed union's
     * per-member child dispatch reads them there). */
    ClassSource stackOf(String mappingFqn, String classFqn, MappingDefinition mapping,
            List<Arm> arms, List<Extra> extras, boolean liftsWanted) {
        var one = Multiplicity.Bounded.ONE;
        var optional = Multiplicity.Bounded.ZERO_ONE;
        var many = Multiplicity.Bounded.ZERO_MANY;
        List<ClassSource> srcs = new ArrayList<>(arms.size());
        List<Type.RelationType> armRows = new ArrayList<>(arms.size());
        for (Arm a : arms) {
            srcs.add(a.src());
            armRows.add(Type.requireRelationSchema(a.pipe().info().type()));
        }
        List<Col> cols = new ArrayList<>();
        Map<String, Col> byName = new LinkedHashMap<>();
        // A3 — the scalar properties the arms bind, first appearance first
        for (int i = 0; i < arms.size(); i++) {
            ClassSource a = srcs.get(i);
            for (var e : a.bindings().entrySet()) {
                if (ClassMapping.isSubTypeColumn(e.getKey())
                        || ClassMapping.isPrimaryKeyBinding(e.getKey())) {
                    continue;
                }
                Property p = ctx.findProperty(classFqn, e.getKey()).orElse(null);
                if (p == null || Type.asClassType(p.type()) instanceof Type.ClassType) {
                    continue;
                }
                Col c = byName.get(e.getKey());
                if (c == null) {
                    c = new Col(e.getKey(), p.type(), p.multiplicity());
                    byName.put(c.name(), c);
                    cols.add(c);
                }
                c.perArm().put(i, coerce(e.getValue(), p.type()));
            }
        }
        // A4 — embedded constructors' leaves per arm
        Embedded emb = collectEmbedded(classFqn, srcs);
        for (var pe : emb.subs().entrySet()) {
            String path = pe.getKey();
            String innerFqn = emb.inner().get(path);
            for (String sub : pe.getValue()) {
                Property lp = innerFqn == null ? null
                        : ctx.findProperty(innerFqn, sub).orElse(null);
                Col c = null;
                for (int i = 0; i < arms.size(); i++) {
                    TypedNewInstance ctor = ctorAtPath(srcs.get(i).bindings(), path);
                    TypedSpec v = ctor == null ? null : ctor.properties().get(sub);
                    if (v == null) {
                        continue;
                    }
                    v = Pipelines.unwrapToOne(v);
                    if (c == null) {
                        Type t = lp != null ? lp.type() : v.info().type();
                        c = new Col(embCol(path, sub), t, optional);
                        byName.put(c.name(), c);
                        cols.add(c);
                    }
                    c.perArm().put(i, coerce(v, c.type()));
                }
            }
        }
        // A5 — subtype dispatch columns and the membership witness
        addSubtypeCols(classFqn, srcs, cols, byName);
        // A11 — every arm's own primary key; the shared table key once
        for (KeyThread t : keyThreads(mappingFqn, classFqn)) {
            Col c = byName.get(t.name());
            if (c == null) {
                Type kt = threadType(t, srcs, armRows);
                c = new Col(t.name(), kt, optional);
                byName.put(c.name(), c);
                cols.add(c);
            }
            for (int i = 0; i < arms.size(); i++) {
                boolean mine = t.shared() ? overTable(mapping, srcs.get(i), t.store(), t.table())
                        : t.ordinal() == i;
                Type.Column ac = columnOf(armRows.get(i), t.column());
                if (mine && ac != null) {
                    c.perArm().put(i, new TypedPropertyAccess(
                            new TypedVariable(srcs.get(i).rowVar(), new ExprType(armRows.get(i), one)),
                            ac.name(), new ExprType(ac.type(), ac.multiplicity())));
                }
            }
        }
        // THE ROUTE KEYS of a routed navigate's union: each route's target
        // reads, projected under the route's key names by every arm that
        // came from it and carries the column (a typed NULL elsewhere: the
        // engine's un-routed thread never matches)
        // (routes of one SHAPE share a key NAME: every arm of every such
        // route projects its own read under it)
        Map<String, String> keyReadMissing = new LinkedHashMap<>();
        for (int i = 0; i < arms.size(); i++) {
            var r = arms.get(i).route();
            if (r == null) {
                continue;
            }
            for (int k = 0; k < r.keyNames().size(); k++) {
                String name = r.keyNames().get(k);
                String read = r.targetReads().get(k);
                Type kt = pathType(armRows.get(i), read);
                Col c = byName.get(name);
                if (c == null && kt == null) {
                    keyReadMissing.putIfAbsent(name, read);
                    continue;
                }
                if (c == null) {
                    c = new Col(name, java.util.Objects.requireNonNull(kt), optional);
                    byName.put(name, c);
                    cols.add(c);
                    keyReadMissing.remove(name);
                }
                if (kt != null) {
                    c.perArm().put(i, pathRead(
                            new TypedVariable(srcs.get(i).rowVar(), new ExprType(armRows.get(i), one)),
                            armRows.get(i), read, new ExprType(c.type(), optional)));
                }
            }
        }
        for (var km : keyReadMissing.entrySet()) {
            if (byName.get(km.getKey()) == null) {
                throw new MappingResolutionException("route condition reads '" + km.getValue()
                        + "', which no target set's rows carry (class '" + classFqn
                        + "', mapping '" + mappingFqn + "')", classFqn);
            }
        }
        // the DEMANDED extras, per arm
        for (Extra x : extras) {
            Col c = byName.get(x.name());
            if (c == null) {
                c = new Col(x.name(), x.type(), optional);
                byName.put(c.name(), c);
                cols.add(c);
            }
            for (var pe : x.perArm().entrySet()) {
                if (c.perArm().containsKey(pe.getKey())) {
                    throw new IllegalStateException("resolver bug: arm " + pe.getKey()
                            + " projects extra '" + x.name() + "' twice");
                }
                c.perArm().put(pe.getKey(), pe.getValue());
            }
        }
        // A6 / A9 — the lifts: their source columns join the row
        List<Lift> lifts = liftsWanted
                ? collectLifts(mappingFqn, classFqn, mapping, srcs, armRows, emb) : List.of();
        for (Lift l : lifts) {
            for (Col c : l.srcCols()) {
                byName.put(c.name(), c);
                cols.add(c);
            }
        }
        // a SINGLE-TABLE hierarchy scans its table once: every arm's reads
        // re-root onto the first arm's row (collapseOntoOneScan)
        int emitted = arms.size();
        if (collapsedTable(mapping, classFqn, arms) != null) {
            collapseOntoOneScan(srcs, armRows, cols, byName, classFqn, emb);
            emitted = 1;
        }
        // the arms projected onto one row, concatenated in member order
        List<Type.Column> rowCols = new ArrayList<>(cols.size());
        for (Col c : cols) {
            rowCols.add(new Type.Column(c.name(), c.type(), c.mult()));
        }
        Type.RelationType rowType = new Type.RelationType(rowCols);
        TypedSpec union = null;
        for (int i = 0; i < emitted; i++) {
            ClassSource a = srcs.get(i);
            Type.RelationType aRow = armRows.get(i);
            List<TypedFuncCol> pcols = new ArrayList<>(cols.size());
            for (Col c : cols) {
                TypedSpec v = c.perArm().get(i);
                if (v == null) {
                    v = new TypedCollection(List.of(), new ExprType(c.type(), optional));
                }
                pcols.add(funcCol(c.name(), v, aRow, a.rowVar()));
            }
            TypedSpec arm = new TypedProject(arms.get(i).pipe(), pcols,
                    new ExprType(Type.relation(rowType), many));
            union = union == null ? arm
                    : new TypedConcatenate(union, arm, new ExprType(Type.relation(rowType), many));
        }
        TypedSpec pipeline = java.util.Objects.requireNonNull(union, "a stack with no arms");
        // the lifted navigations sit ABOVE the concatenate, one slot each
        List<Type.Column> withSlots = new ArrayList<>(rowCols);
        for (Lift l : lifts) {
            Type.RelationType srcRow = new Type.RelationType(withSlots);
            withSlots.add(new Type.Column(l.alias(), l.targetType(), one));
            pipeline = l.step(pipeline, srcRow, new Type.RelationType(withSlots));
        }
        Type.RelationType fullRow = new Type.RelationType(withSlots);
        // the bindings: scalar reads, rebuilt embedded constructors, the
        // lifted slots, the subtype and witness pseudo-bindings
        ExprType rowInfo = new ExprType(fullRow, one);
        TypedVariable row = new TypedVariable(ROW_VAR, rowInfo);
        Map<String, TypedSpec> bindings = new LinkedHashMap<>();
        for (Col c : cols) {
            if (ClassMapping.isSubTypeColumn(c.name())) {
                bindings.put(c.name(), new TypedPropertyAccess(row, c.name(),
                        new ExprType(c.type(), c.mult())));
                continue;
            }
            if (ctx.findProperty(classFqn, c.name()).isPresent()
                    && !c.name().startsWith("emb__") && !c.name().startsWith("__s_")) {
                bindings.put(c.name(), new TypedPropertyAccess(row, c.name(),
                        new ExprType(c.type(), c.mult())));
            }
        }
        for (Lift l : lifts) {
            bindings.put(l.alias(), new TypedPropertyAccess(row, l.alias(),
                    new ExprType(l.targetType(), one)));
        }
        for (String top : emb.tops()) {
            bindings.put(top, rebuildCtor(top, emb, row));
        }
        ClassSource out = new ClassSource(mappingFqn, classFqn, ClassSource.UNION_SET_ID, pipeline,
                ROW_VAR, bindings, fullRow);
        List<ClassSource> ls = new ArrayList<>();
        for (ClassSource a : srcs) {
            List<ClassSource> al = leaves.get(a);
            if (al != null) {
                ls.addAll(al);
            } else {
                ls.add(a);
            }
        }
        leaves.put(out, ls);
        return out;
    }

    /** THE SINGLE-TABLE HIERARCHY (the engine's cast semantics; the corpus
     * row {@code inheritanceWithEmbedded}): the arms of the class's
     * INHERITANCE operation (the binding fact) that ALL sit on one BARE
     * table — no filter, distinct, group or projection between the table
     * and the arm — are that table's rows ONCE, each row cast per arm; a
     * stack would thread every physical row once per member. The shared
     * table, or null when the arms are not that shape. */
    private com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedTableReference collapsedTable(
            MappingDefinition mapping, String classFqn, List<Arm> arms) {
        if (arms.size() < 2) {
            return null;
        }
        MappingDefinition.ClassBinding cb = findBinding(mapping, classFqn);
        if (!(cb instanceof MappingDefinition.ClassBinding.Operation op) || !op.inheritance()) {
            return null;
        }
        com.legend.compiler.spec.typed.TypedTableReference shared = null;
        for (Arm a : arms) {
            if (!(a.pipe() instanceof com.legend.compiler.spec.typed.TypedTableReference tr)) {
                return null;
            }
            if (shared == null) {
                shared = tr;
            } else if (!tr.store().equals(shared.store()) || !tr.table().equals(shared.table())) {
                return null;
            }
        }
        return shared;
    }

    /** The collapse: every column's per-arm reads re-rooted onto the FIRST
     * arm's row; the arms agreeing (one structural read) keep it on arm 0;
     * a class property (or an embedded leaf) the arms map DIFFERENTLY binds
     * nowhere on the base — a bare read is loud, a cast reads the
     * subtype's own column (the engine's rule; the normalizer's former
     * "identical property mappings hoist"); any other disagreement
     * (subtype, key, route, extra) is a builder bug. */
    private void collapseOntoOneScan(List<ClassSource> srcs, List<Type.RelationType> armRows,
            List<Col> cols, Map<String, Col> byName, String classFqn, Embedded emb) {
        String v0 = srcs.get(0).rowVar();
        ExprType info0 = new ExprType(armRows.get(0), Multiplicity.Bounded.ONE);
        Set<String> droppedTops = new LinkedHashSet<>();
        for (Iterator<Col> it = cols.iterator(); it.hasNext();) {
            Col c = it.next();
            if (ClassMapping.isSubTypeColumn(c.name())
                    && c.name().endsWith(ClassMapping.memberWitness())) {
                // every row of the one scan is every arm's: membership is
                // TOTAL, a cast is a same-row read, never a filtered head
                it.remove();
                byName.remove(c.name());
                continue;
            }
            TypedSpec chosen = null;
            boolean conflict = false;
            for (var pe : c.perArm().entrySet()) {
                TypedSpec v = pe.getKey() == 0 ? pe.getValue()
                        : Pipelines.rewriteRowReads(pe.getValue(), srcs.get(pe.getKey()).rowVar(),
                                Map.of(), Set.of(), x -> new TypedVariable(v0, info0));
                if (chosen == null) {
                    chosen = v;
                } else if (!chosen.equals(v)) {
                    conflict = true;
                }
            }
            if (chosen == null) {
                continue;   // a column no arm carries (a dead key) stays NULL
            }
            if (!conflict) {
                c.perArm().clear();
                c.perArm().put(0, chosen);
                continue;
            }
            String top = embeddedTopOf(c.name(), emb);
            if (top != null) {
                droppedTops.add(top);
            } else if (!ClassMapping.isSubTypeColumn(c.name())
                    && !ctx.findProperty(classFqn, c.name()).isPresent()) {
                throw new IllegalStateException("resolver bug: the arms of the single-table"
                        + " hierarchy '" + classFqn + "' read '" + c.name() + "' differently");
            }
            // (a subtype column the arms disagree on is a cast to an ANCESTOR
            // they share — bound nowhere, like the base's own property)
            it.remove();
            byName.remove(c.name());
        }
        // an embedded property mapped differently drops WHOLE (its other
        // leaves with it): the base binds it nowhere
        for (String top : droppedTops) {
            emb.tops().remove(top);
            for (Iterator<Col> it = cols.iterator(); it.hasNext();) {
                Col c = it.next();
                if (top.equals(embeddedTopOf(c.name(), emb)) || c.name().startsWith(embCol(top, ""))) {
                    byName.remove(c.name());
                    it.remove();
                }
            }
        }
    }

    /** The top embedded property an {@code emb__} column belongs to, or
     * null for a plain column. */
    private static @com.legend.base.Nullable String embeddedTopOf(String col, Embedded emb) {
        for (String top : emb.tops()) {
            if (col.startsWith(embCol(top, ""))) {
                return top;
            }
        }
        return null;
    }

    /** {@code value} coerced to the declared kind where the database would
     * not unite the arms otherwise: a String property reads as text on
     * every arm (the union's schema contract). */
    private static TypedSpec coerce(TypedSpec value, Type declared) {
        if (declared == Type.Primitive.STRING && value.info().type() != Type.Primitive.STRING
                && !(value instanceof TypedCollection tc && tc.elements().isEmpty())) {
            return new TypedCast(value, Type.Primitive.STRING,
                    new ExprType(Type.Primitive.STRING, value.info().multiplicity()), false);
        }
        return value;
    }

    private static TypedFuncCol funcCol(String name, TypedSpec value, Type.RelationType armRow,
            String rowVar) {
        var one = Multiplicity.Bounded.ONE;
        var lFn = new Type.FunctionType(List.of(new Type.Param(armRow, one)),
                new Type.Param(value.info().type(), value.info().multiplicity()));
        return new TypedFuncCol(name, new TypedLambda(List.of(rowVar), List.of(value),
                new ExprType(lFn, one)));
    }

    static @com.legend.base.Nullable Type.Column columnOf(Type.RelationType row, String name) {
        for (Type.Column c : row.columns()) {
            if (c.name().equals(name)) {
                return c;
            }
        }
        return null;
    }

    // ------------------------------------------------------------------
    // A4 — embedded
    // ------------------------------------------------------------------

    /** The embedded distribution: dotted ctor path -> projectable leaves,
     * path -> the ctor class the stack rebuilds, the top properties, and
     * path -> class-typed same-name reads (navigations the lifts serve). */
    record Embedded(Map<String, LinkedHashSet<String>> subs, Map<String, String> inner,
            LinkedHashSet<String> tops, Map<String, LinkedHashSet<String>> navSubs) {
    }

    private Embedded collectEmbedded(String classFqn, List<ClassSource> arms) {
        Map<String, LinkedHashSet<String>> subs = new LinkedHashMap<>();
        Map<String, String> inner = new LinkedHashMap<>();
        Map<String, LinkedHashSet<String>> pathClasses = new LinkedHashMap<>();
        Map<String, LinkedHashSet<String>> navSubs = new LinkedHashMap<>();
        Set<String> poisoned = new LinkedHashSet<>();
        for (ClassSource a : arms) {
            for (var e : a.bindings().entrySet()) {
                if (ctx.findProperty(classFqn, e.getKey()).isEmpty()) {
                    continue;   // a subtype-only field belongs to the subtype dispatch
                }
                if (Pipelines.unwrapToOne(e.getValue()) instanceof TypedNewInstance ni) {
                    collectLeaves(e.getKey(), e.getKey(), ni, a.rowVar(), subs, inner, poisoned,
                            pathClasses, navSubs);
                }
            }
        }
        // arms disagreeing on a path's ctor class rebuild the DECLARED class
        // with its own leaves only
        for (var pce : pathClasses.entrySet()) {
            if (pce.getValue().size() < 2) {
                continue;
            }
            String path = pce.getKey();
            String decl = classFqn;
            for (String seg : path.split("\\.")) {
                Property p = decl == null ? null : ctx.findProperty(decl, seg).orElse(null);
                decl = p != null && Type.asClassType(p.type()) instanceof Type.ClassType ct ? ct.fqn() : null;
            }
            if (decl == null) {
                poisoned.add(path.contains(".") ? path.substring(0, path.indexOf('.')) : path);
                continue;
            }
            inner.put(path, decl);
            final String decl0 = decl;
            LinkedHashSet<String> lv = subs.get(path);
            if (lv != null) {
                lv.removeIf(leaf -> ctx.findProperty(decl0, leaf).isEmpty());
                if (lv.isEmpty()) {
                    subs.remove(path);
                }
            }
            LinkedHashSet<String> nv = navSubs.get(path);
            if (nv != null) {
                nv.removeIf(leaf -> ctx.findProperty(decl0, leaf).isEmpty());
                if (nv.isEmpty()) {
                    navSubs.remove(path);
                }
            }
            java.util.function.Predicate<String> off = k -> k.startsWith(path + ".")
                    && ctx.findProperty(decl0, k.substring(path.length() + 1).split("\\.")[0]).isEmpty();
            inner.keySet().removeIf(off);
            subs.keySet().removeIf(off);
            navSubs.keySet().removeIf(off);
        }
        for (String bad : poisoned) {
            subs.keySet().removeIf(k -> k.equals(bad) || k.startsWith(bad + "."));
            inner.keySet().removeIf(k -> k.equals(bad) || k.startsWith(bad + "."));
            navSubs.keySet().removeIf(k -> k.equals(bad) || k.startsWith(bad + "."));
        }
        LinkedHashSet<String> tops = new LinkedHashSet<>();
        for (String k : subs.keySet()) {
            tops.add(k.contains(".") ? k.substring(0, k.indexOf('.')) : k);
        }
        for (String k : navSubs.keySet()) {
            tops.add(k.contains(".") ? k.substring(0, k.indexOf('.')) : k);
        }
        return new Embedded(subs, inner, tops, navSubs);
    }

    private void collectLeaves(String top, String path, TypedNewInstance ni, String rowVar,
            Map<String, LinkedHashSet<String>> subs, Map<String, String> inner, Set<String> poisoned,
            Map<String, LinkedHashSet<String>> pathClasses,
            Map<String, LinkedHashSet<String>> navSubs) {
        inner.putIfAbsent(path, ni.classFqn());
        pathClasses.computeIfAbsent(path, k -> new LinkedHashSet<>()).add(ni.classFqn());
        for (var pe : ni.properties().entrySet()) {
            TypedSpec v = Pipelines.unwrapToOne(pe.getValue());
            if (v instanceof TypedNewInstance sub) {
                collectLeaves(top, path + "." + pe.getKey(), sub, rowVar, subs, inner, poisoned,
                        pathClasses, navSubs);
                continue;
            }
            Property p = ctx.findProperty(ni.classFqn(), pe.getKey()).orElse(null);
            boolean sameNameRead = v instanceof TypedPropertyAccess pa
                    && pa.source() instanceof TypedVariable rv && rv.name().equals(rowVar)
                    && pa.property().equals(pe.getKey());
            if (sameNameRead && p != null && Type.asClassType(p.type()) instanceof Type.ClassType) {
                navSubs.computeIfAbsent(path, k -> new LinkedHashSet<>()).add(pe.getKey());
                continue;
            }
            if (projectable(v, rowVar)) {
                subs.computeIfAbsent(path, k -> new LinkedHashSet<>()).add(pe.getKey());
            } else {
                poisoned.add(top);
            }
        }
    }

    /** THREAD-PROJECTABLE: reads of the arm's row ({@code $row.col} or
     * {@code $row.slot.col}), literals, and functions of those. A deeper
     * read or a whole instance is not. */
    static boolean projectable(TypedSpec v, String rowVar) {
        return switch (v) {
            case TypedPropertyAccess pa -> pa.source() instanceof TypedVariable rv
                    ? rv.name().equals(rowVar)
                    : pa.source() instanceof TypedPropertyAccess in
                            && in.source() instanceof TypedVariable rv2 && rv2.name().equals(rowVar);
            case TypedNativeCall f -> f.args().stream().allMatch(x -> projectable(x, rowVar));
            case TypedCast c -> projectable(c.source(), rowVar);
            case TypedIf i -> projectable(i.condition(), rowVar) && projectable(i.thenBranch(), rowVar)
                    && i.elseBranch().map(e -> projectable(e, rowVar)).orElse(true);
            case TypedCollection c -> c.elements().stream().allMatch(x -> projectable(x, rowVar));
            case TypedCString ignored -> true;
            case TypedCInteger ignored -> true;
            case TypedCFloat ignored -> true;
            case TypedCDecimal ignored -> true;
            case TypedCBoolean ignored -> true;
            case TypedCDate ignored -> true;
            case TypedEnumValue ignored -> true;
            default -> false;
        };
    }

    private static @com.legend.base.Nullable TypedNewInstance ctorAtPath(Map<String, TypedSpec> bindings,
            String path) {
        String[] segs = path.split("\\.");
        TypedSpec b = bindings.get(segs[0]);
        TypedNewInstance ni = b == null ? null
                : Pipelines.unwrapToOne(b) instanceof TypedNewInstance n ? n : null;
        for (int i = 1; ni != null && i < segs.length; i++) {
            TypedSpec sub = ni.properties().get(segs[i]);
            ni = sub == null ? null : Pipelines.unwrapToOne(sub) instanceof TypedNewInstance n ? n : null;
        }
        return ni;
    }

    static String embCol(String path, String sub) {
        return "emb__" + path.replace(".", "__") + "__" + sub;
    }

    private TypedSpec rebuildCtor(String path, Embedded emb, TypedVariable row) {
        var one = Multiplicity.Bounded.ONE;
        var optional = Multiplicity.Bounded.ZERO_ONE;
        Type.RelationType rowType = java.util.Objects.requireNonNull(rowOf(row.info().type()));
        String innerFqn = java.util.Objects.requireNonNull(emb.inner().get(path),
                "resolver bug: an embedded path without its ctor class");
        Map<String, TypedSpec> fields = new LinkedHashMap<>();
        for (String sub : emb.subs().getOrDefault(path, new LinkedHashSet<>())) {
            Type.Column c = java.util.Objects.requireNonNull(columnOf(rowType, embCol(path, sub)));
            fields.put(sub, new TypedPropertyAccess(row, c.name(), new ExprType(c.type(), optional)));
        }
        // class-typed subs are reads of the LIFTED navigation of that name
        for (String sub : emb.navSubs().getOrDefault(path, new LinkedHashSet<>())) {
            Type.Column c = columnOf(rowType, sub);
            if (c != null) {
                fields.put(sub, new TypedPropertyAccess(row, c.name(),
                        new ExprType(c.type(), c.multiplicity())));
            }
        }
        for (String k : emb.inner().keySet()) {
            if (k.startsWith(path + ".") && k.indexOf('.', path.length() + 1) < 0) {
                fields.put(k.substring(path.length() + 1), rebuildCtor(k, emb, row));
            }
        }
        return new TypedNewInstance(innerFqn, fields, new ExprType(new Type.ClassType(innerFqn), one));
    }

    // ------------------------------------------------------------------
    // A5 — subtype dispatch
    // ------------------------------------------------------------------

    /** The cast targets an arm of a strict subclass serves: its class and
     * every ancestor strictly below the stack's class. */
    private List<String> ancestorsBelow(String armClass, String root) {
        List<String> out = new ArrayList<>();
        ArrayDeque<String> work = new ArrayDeque<>();
        Set<String> seen = new LinkedHashSet<>();
        work.add(armClass);
        while (!work.isEmpty()) {
            String c = work.poll();
            if (!seen.add(c) || c.equals(root) || !ctx.isSubtype(c, root)) {
                continue;
            }
            out.add(c);
            TypedClass tc = ctx.findClass(c).orElse(null);
            if (tc != null) {
                work.addAll(tc.superClassFqns());
            }
        }
        return out;
    }

    private void addSubtypeCols(String classFqn, List<ClassSource> arms, List<Col> cols,
            Map<String, Col> byName) {
        var one = Multiplicity.Bounded.ONE;
        var optional = Multiplicity.Bounded.ZERO_ONE;
        LinkedHashSet<String> targets = new LinkedHashSet<>();
        for (int i = 0; i < arms.size(); i++) {
            ClassSource a = arms.get(i);
            if (a.classFqn().equals(classFqn) || !ctx.isSubtype(a.classFqn(), classFqn)) {
                continue;
            }
            for (String target : ancestorsBelow(a.classFqn(), classFqn)) {
                targets.add(target);
                for (var e : a.bindings().entrySet()) {
                    if (ClassMapping.isSubTypeColumn(e.getKey())
                            || ClassMapping.isPrimaryKeyBinding(e.getKey())) {
                        continue;
                    }
                    Property p = ctx.findProperty(target, e.getKey()).orElse(null);
                    if (p == null) {
                        continue;
                    }
                    TypedSpec v = Pipelines.unwrapToOne(e.getValue());
                    if (!(Type.asClassType(p.type()) instanceof Type.ClassType)) {
                        addStc(cols, byName, ClassMapping.subTypeColumn(target, e.getKey()),
                                p.type(), i, coerce(e.getValue(), p.type()));
                    } else if (v instanceof TypedNewInstance ctor) {
                        // an EMBEDDED subtype field distributes each
                        // projectable ctor leaf as a flat column
                        for (var pe : ctor.properties().entrySet()) {
                            TypedSpec lv = Pipelines.unwrapToOne(pe.getValue());
                            if (lv instanceof TypedNewInstance || !projectable(lv, a.rowVar())) {
                                continue;
                            }
                            Property lp = ctx.findProperty(ctor.classFqn(), pe.getKey()).orElse(null);
                            Type lt = lp != null ? lp.type() : lv.info().type();
                            addStc(cols, byName, ClassMapping.subTypeColumn(target,
                                    e.getKey() + "__" + pe.getKey()), lt, i, coerce(lv, lt));
                        }
                    }
                }
            }
        }
        // MEMBERSHIP WITNESS: a cast target some arm does not conform to
        // restricts rows at to-many navigation positions — TRUE on the
        // conforming arms, NULL elsewhere; a total-membership target none
        for (String target : targets) {
            boolean partial = false;
            for (ClassSource a : arms) {
                if (!a.classFqn().equals(classFqn) && !ctx.isSubtype(a.classFqn(), target)) {
                    partial = true;
                }
            }
            if (!partial) {
                continue;
            }
            String name = ClassMapping.subTypeColumn(target, ClassMapping.memberWitness());
            for (int i = 0; i < arms.size(); i++) {
                if (ctx.isSubtype(arms.get(i).classFqn(), target)) {
                    addStc(cols, byName, name, Type.Primitive.BOOLEAN, i,
                            new TypedCBoolean(true, new ExprType(Type.Primitive.BOOLEAN, one)));
                } else if (byName.get(name) == null) {
                    // register the column even when no conforming arm came first
                    Col c = new Col(name, Type.Primitive.BOOLEAN, optional);
                    byName.put(name, c);
                    cols.add(c);
                }
            }
        }
    }

    private static void addStc(List<Col> cols, Map<String, Col> byName, String name, Type type,
            int arm, TypedSpec value) {
        Col c = byName.get(name);
        if (c == null) {
            c = new Col(name, type, Multiplicity.Bounded.ZERO_ONE);
            byName.put(name, c);
            cols.add(c);
        }
        if (c.perArm().containsKey(arm)) {
            throw new IllegalStateException("resolver bug: arm " + arm + " projects '" + name
                    + "' twice");
        }
        c.perArm().put(arm, value);
    }

    // ------------------------------------------------------------------
    // A11 — primary keys
    // ------------------------------------------------------------------

    /** The key-thread fact of the operation: on this mapping, else on the
     * include that defines the operation. */
    private List<KeyThread> keyThreads(String mappingFqn, String classFqn) {
        ArrayDeque<String> work = new ArrayDeque<>();
        Set<String> seen = new LinkedHashSet<>();
        work.add(mappingFqn);
        while (!work.isEmpty()) {
            String m = work.poll();
            if (!seen.add(m)) {
                continue;
            }
            List<KeyThread> ts = ctx.unionKeyThreads(m, classFqn);
            if (ts != null) {
                return ts;
            }
            ctx.findMapping(m).ifPresent(md -> {
                for (MappingInclude inc : md.includes()) {
                    work.add(inc.mappingPath());
                }
            });
        }
        return List.of();
    }

    private static Type threadType(KeyThread t, List<ClassSource> arms, List<Type.RelationType> rows) {
        for (Type.RelationType r : rows) {
            Type.Column c = columnOf(r, t.column());
            if (c != null) {
                return c.type();
            }
        }
        throw new MappingResolutionException("union key thread '" + t.name() + "' reads column '"
                + t.column() + "', which no arm's row carries", arms.get(0).classFqn());
    }

    /** Whether the arm's set is over the (store, table) of a shared key:
     * the binding's main-table fact (a constructed scope puts inline rows
     * in the table's place; the fact survives), else the pipeline's root
     * scan. */
    private boolean overTable(MappingDefinition mapping, ClassSource arm,
            @com.legend.base.Nullable String store, @com.legend.base.Nullable String table) {
        if (store == null || table == null) {
            return false;
        }
        MappingDefinition.ClassBinding cb = arm.setId() == null ? null
                : sources.findBinding(mapping, arm.classFqn(), arm.setId(), new LinkedHashSet<>());
        if (cb instanceof MappingDefinition.ClassBinding.Relational rb
                && rb.source() instanceof MappingDefinition.RelationalSource.Table st) {
            return st.database().equals(store)
                    && bareTable(st.table()).equalsIgnoreCase(bareTable(table));
        }
        TypedTableReference tr = ClassSources.rootTableOf(arm.pipeline());
        if (tr == null) {
            return false;
        }
        return tr.store().equals(store) && bareTable(tr.table()).equalsIgnoreCase(bareTable(table));
    }

    private static String bareTable(String t) {
        return t.substring(t.lastIndexOf('.') + 1);
    }

    // ------------------------------------------------------------------
    // A6 / A9 — the lifts
    // ------------------------------------------------------------------

    /** One arm's contribution to a lift: the arm, the target it names (a
     * set's function call or a class extent), the rows the condition reads
     * (a routed step's route rows; the class extent for a plain step), the
     * condition, its target reads and source reads (paths). */
    /** {@code sourceKeys}: the SQL-path source key per read (a modeled read
     * is the stack's property column, else the per-entry column);
     * {@code pairSourceKeys}: the per-entry column always (graph fetch
     * pairs each arm with its own route). */
    private record Entry(int arm, TypedSpec target, TypedSpec rows, TypedLambda cond,
            List<String> targetReads, List<String> sourceReads, List<String> sourceKeys,
            List<String> pairSourceKeys) {
    }

    /** One shape GROUP = one route: the entries of one target identity and
     * one condition shape; the source columns it projects and the key names
     * its target reads project under. */
    /** One target SET (or extent) of the routed union: its entries, and per
     * distinct target read the key it is projected under — the SQL-path
     * name (a modeled column's property, shared by every arm; else the
     * per-group key) and the per-group key graph fetch pairs on. */
    private record Group(TypedSpec target, TypedSpec rows, List<Entry> entries,
            Map<String, String> keyByRead, Map<String, String> pairKeyByRead) {
    }

    /** One lifted navigation: its slot alias (a property, or a subtype
     * dispatch key), target class, source columns, routes, and the
     * predicate builders over (stack row, routed union row). */
    private record Lift(String alias, Type targetType, TypedSpec target, List<Col> srcCols,
            List<TypedNavigate.Route> routes, Type.RelationType urow,
            java.util.function.Function<Type.RelationType, TypedLambda> predicate,
            java.util.function.@com.legend.base.Nullable Function<Type.RelationType, TypedLambda> paired) {
        TypedSpec step(TypedSpec source, Type.RelationType srcRow, Type.RelationType outRow) {
            var one = Multiplicity.Bounded.ONE;
            return new TypedNavigate(source, Optional.of(alias), target, predicate.apply(srcRow),
                    paired == null ? Optional.empty() : Optional.of(paired.apply(srcRow)),
                    null, TypedNavigate.Form.PRE_MAP, new ExprType(Type.relation(outRow), one),
                    routes);
        }
    }

    private List<Lift> collectLifts(String mappingFqn, String classFqn, MappingDefinition mapping,
            List<ClassSource> arms, List<Type.RelationType> armRows, Embedded emb) {
        // alias -> per arm the step it navigates under that alias
        Map<String, Map<Integer, TypedNavigate>> byAlias = new LinkedHashMap<>();
        Map<String, String> targetClassByAlias = new LinkedHashMap<>();
        for (int i = 0; i < arms.size(); i++) {
            ClassSource a = arms.get(i);
            var steps = Pipelines.navSteps(a.pipeline());
            if (steps.isEmpty()) {
                continue;
            }
            boolean strictSub = !a.classFqn().equals(classFqn) && ctx.isSubtype(a.classFqn(), classFqn);
            for (var e : a.bindings().entrySet()) {
                if (ClassMapping.isSubTypeColumn(e.getKey())
                        || ClassMapping.isPrimaryKeyBinding(e.getKey())) {
                    continue;
                }
                String slot = InnerDemand.navSlotAlias(e.getValue(), a.rowVar(), steps.keySet());
                TypedNavigate st = slot == null ? null : steps.get(slot);
                if (st == null || st.form() != TypedNavigate.Form.PRE_MAP
                        || !(st.target() instanceof TypedGetAll tg)) {
                    continue;
                }
                if (ctx.findProperty(classFqn, e.getKey()).isPresent()) {
                    byAlias.computeIfAbsent(e.getKey(), k -> new LinkedHashMap<>()).put(i, st);
                    targetClassByAlias.putIfAbsent(e.getKey(), tg.classFqn());
                } else if (strictSub) {
                    // a SUBTYPE-ONLY navigation lifts under the dispatch key of
                    // every cast target that declares it
                    for (String target : ancestorsBelow(a.classFqn(), classFqn)) {
                        if (ctx.findProperty(target, e.getKey()).isPresent()) {
                            String key = ClassMapping.subTypeColumn(target, e.getKey());
                            byAlias.computeIfAbsent(key, k -> new LinkedHashMap<>()).put(i, st);
                            targetClassByAlias.putIfAbsent(key, tg.classFqn());
                        }
                    }
                }
            }
        }
        // a class-typed field INSIDE an embedded constructor ($row.employees
        // under ^Bridge(...)) is the arm's navigation too: it lifts under its
        // own name and the rebuilt constructor reads the lifted slot
        for (int i = 0; i < arms.size(); i++) {
            ClassSource a = arms.get(i);
            var steps = Pipelines.navSteps(a.pipeline());
            for (var ne : emb.navSubs().entrySet()) {
                TypedNewInstance ctor = ctorAtPath(a.bindings(), ne.getKey());
                if (ctor == null) {
                    continue;
                }
                for (String sub : ne.getValue()) {
                    TypedSpec v = ctor.properties().get(sub);
                    String slot = v == null ? null
                            : InnerDemand.navSlotAlias(v, a.rowVar(), steps.keySet());
                    TypedNavigate st = slot == null ? null : steps.get(slot);
                    if (st == null || st.form() != TypedNavigate.Form.PRE_MAP
                            || !(st.target() instanceof TypedGetAll tg)) {
                        continue;
                    }
                    byAlias.computeIfAbsent(sub, k -> new LinkedHashMap<>()).put(i, st);
                    targetClassByAlias.putIfAbsent(sub, tg.classFqn());
                }
            }
        }
        List<Lift> out = new ArrayList<>();
        int liftIx = 0;
        for (var ae : byAlias.entrySet()) {
            Lift l = liftOf(ae.getKey(), liftIx++,
                    java.util.Objects.requireNonNull(targetClassByAlias.get(ae.getKey())), mapping,
                    ae.getValue(), arms, armRows);
            if (l != null) {
                out.add(l);
            }
        }
        return out;
    }

    private @com.legend.base.Nullable Lift liftOf(String alias, int liftIx, String targetClass,
            MappingDefinition mapping, Map<Integer, TypedNavigate> steps, List<ClassSource> arms,
            List<Type.RelationType> armRows) {
        var one = Multiplicity.Bounded.ONE;
        var optional = Multiplicity.Bounded.ZERO_ONE;
        // R-key (engine receipt): with an arm that has no route every key is
        // per set (avoidModeledProperties); otherwise a column an arm's set
        // maps as a scalar property is MODELED — the stack's own property
        // column, shared by every arm — and any other column is per entry
        boolean everyArmRoutes = steps.size() == arms.size();
        // the entries: every arm step's routes, a plain step as one entry
        // over the class extent
        List<Entry> entries = new ArrayList<>();
        for (var se : steps.entrySet()) {
            int arm = se.getKey();
            TypedNavigate st = se.getValue();
            Map<String, String> modeledSrc = everyArmRoutes
                    ? modeledOf(arms.get(arm), classFqnOf(arms)) : Map.of();
            if (st.routes().isEmpty()) {
                TypedLambda pred = st.predicate();
                TypedSpec body = pred.body().get(pred.body().size() - 1);
                List<String> tReads = new ArrayList<>();
                collectReads(body, pred.parameters().get(1), tReads);
                List<String> sReads = new ArrayList<>();
                collectReads(body, pred.parameters().get(0), sReads);
                entries.add(new Entry(arm, st.target(), st.target(), pred, tReads, sReads,
                        sourceKeys(liftIx, entries.size(), sReads, modeledSrc),
                        sourceKeys(liftIx, entries.size(), sReads, Map.of())));
            } else {
                for (TypedNavigate.Route r : st.routes()) {
                    TypedSpec body = r.cond().body().get(r.cond().body().size() - 1);
                    List<String> sReads = new ArrayList<>();
                    collectReads(body, r.cond().parameters().get(0), sReads);
                    entries.add(new Entry(arm, r.target(), r.rows(), r.cond(), r.targetReads(),
                            sReads, sourceKeys(liftIx, entries.size(), sReads, modeledSrc),
                            sourceKeys(liftIx, entries.size(), sReads, Map.of())));
                }
            }
        }
        // R-target (engine receipt, docs/LEG2_STACK_AUDIT_2026_09_14.md): the
        // arms' pins resolve under the QUERIED mapping — ONE distinct pinned
        // set resolves to that set, root or not; several resolve to the
        // target class's ROOT here (an operation's members, or the root/sole
        // set), and a pin outside them is a dead route (the engine's
        // un-routed thread never matches: the inclusive unions)
        MappingDefinition.ClassBinding tb = findBinding(mapping, targetClass);
        boolean targetIsStack = tb instanceof MappingDefinition.ClassBinding.Operation;
        if (ctx.findProperty(classFqnOf(arms), alias).isPresent()) {
            Set<String> distinct = new LinkedHashSet<>();
            for (Entry e : entries) {
                distinct.addAll(pinsOf(mapping, arms.get(e.arm()), alias));
            }
            if (distinct.size() > 1) {
                Set<String> leaves = leafSetIds(mapping, targetClass);
                if (leaves != null) {
                    entries.removeIf(e -> {
                        List<String> pins = pinsOf(mapping, arms.get(e.arm()), alias);
                        return !pins.isEmpty() && pins.stream().noneMatch(leaves::contains);
                    });
                }
            }
            // a pinned arm's class-extent route (its pin was the root under
            // the DEFINING mapping) names the pinned set itself here: the
            // target union is the pinned sets, each once
            entries = retargetPinned(entries, mapping, targetClass, alias, arms);
        }
        if (entries.isEmpty()) {
            return null;
        }
        // rule (c): a NON-stack target reached by every arm through the same
        // join into distinct private sets routes to the LAST arm's set
        if (!targetIsStack && entries.size() >= 2 && steps.size() == entries.size()) {
            Set<String> ids = new LinkedHashSet<>();
            Set<TypedSpec> shapes = new LinkedHashSet<>();
            boolean allSets = true;
            for (Entry e : entries) {
                String sid = targetSetIdOf(mapping, e.target());
                if (sid == null) {
                    allSets = false;
                    break;
                }
                ids.add(sid);
                shapes.add(condShape(e.cond()));
            }
            if (allSets && ids.size() == entries.size() && shapes.size() == 1) {
                entries = List.of(entries.get(entries.size() - 1));
            }
        }
        // SHARED KEYS: when every entry is ONE condition shape and the entries
        // are exactly every (arm, target set) pair, a source row fills only
        // its own arm's keys and a union row only its own set's — so the
        // per-entry and per-set keys can be ONE column each: the repointed
        // conditions coincide and the OR below collapses to one condition
        // (an equi-join the database hashes, where the OR of per-pair
        // equalities was a nested loop — 105 terms for inferredType's
        // 5 x 21 sets). Same answers: the OR's only live term for a row pair
        // is that pair's own entry, and it exists iff both are in the rectangle.
        boolean sharedKeys = sharedKeys(entries);
        if (sharedKeys) {
            List<Entry> shared = new ArrayList<>(entries.size());
            for (Entry e : entries) {
                List<String> keys = new ArrayList<>(e.sourceKeys());
                for (int k = 0; k < keys.size(); k++) {
                    if (keys.get(k).equals(e.pairSourceKeys().get(k))) {   // not a modeled column
                        keys.set(k, "__s_" + liftIx + "_all_" + k);
                    }
                }
                shared.add(new Entry(e.arm(), e.target(), e.rows(), e.cond(), e.targetReads(),
                        e.sourceReads(), keys, e.pairSourceKeys()));
            }
            entries = shared;
        }
        // the groups: one per target SET (or extent), each once — the OR
        // runs over every entry's condition
        Map<Object, Group> groups = new LinkedHashMap<>();
        for (Entry e : entries) {
            Object id = targetIdentity(e.target());
            Group g = groups.get(id);
            if (g == null) {
                g = new Group(e.target(), e.rows(), new ArrayList<>(), new LinkedHashMap<>(),
                        new LinkedHashMap<>());
                groups.put(id, g);
            }
            g.entries().add(e);
        }
        int gi = 0;
        for (Group g : groups.values()) {
            Map<String, String> modeled = everyArmRoutes
                    ? modeledColumns(mapping, targetClass, g.target()) : Map.of();
            for (Entry e : g.entries()) {
                List<String> order = targetReadOrder(e);
                for (String read : e.targetReads()) {
                    if (!g.pairKeyByRead().containsKey(read)) {
                        String pair = "__route" + gi + "_" + g.pairKeyByRead().size();
                        g.pairKeyByRead().put(read, pair);
                        String prop = modeled.get(read);
                        g.keyByRead().put(read, prop != null ? prop
                                : sharedKeys ? "__route_all_" + order.indexOf(read) : pair);
                    }
                }
            }
            gi++;
        }
        // the source columns: each entry's non-modeled reads, projected by
        // its arm alone (a modeled read is the stack's property column)
        List<Col> srcCols = new ArrayList<>();
        Map<String, Col> byName = new LinkedHashMap<>();
        for (Entry e : entries) {
            Type.RelationType aRow = armRows.get(e.arm());
            for (int k = 0; k < e.sourceReads().size(); k++) {
                String path = e.sourceReads().get(k);
                String name = e.pairSourceKeys().get(k);
                Type t = pathType(aRow, path);
                if (t == null) {
                    throw new MappingResolutionException("lift '" + alias + "': arm "
                            + arms.get(e.arm()).classFqn() + "[" + arms.get(e.arm()).setId()
                            + "] reads '" + path + "', which its rows do not carry (row "
                            + aRow.columns().stream().map(Type.Column::name).toList() + ")",
                            targetClass);
                }
                Col c = byName.get(name);
                if (c == null) {
                    c = new Col(name, t, optional);
                    byName.put(name, c);
                    srcCols.add(c);
                }
                if (c.perArm().containsKey(e.arm())) {
                    throw new IllegalStateException("resolver bug: arm " + e.arm()
                            + " projects source key '" + name + "' twice");
                }
                c.perArm().put(e.arm(), pathRead(
                        new TypedVariable(arms.get(e.arm()).rowVar(), new ExprType(aRow, one)),
                        aRow, path, new ExprType(t, optional)));
            }
        }
        // the shared source keys: one column per read position, each arm
        // projecting its own read (the same path in every entry of the arm)
        if (sharedKeys) {
            for (Entry e : entries) {
                Type.RelationType aRow = armRows.get(e.arm());
                for (int k = 0; k < e.sourceReads().size(); k++) {
                    String name = e.sourceKeys().get(k);
                    if (!name.startsWith("__s_" + liftIx + "_all_")) {
                        continue;   // a modeled read: the stack's own column
                    }
                    String path = e.sourceReads().get(k);
                    Type t = java.util.Objects.requireNonNull(pathType(aRow, path));
                    Col c = byName.get(name);
                    if (c == null) {
                        c = new Col(name, t, optional);
                        byName.put(name, c);
                        srcCols.add(c);
                    }
                    if (!c.perArm().containsKey(e.arm())) {
                        c.perArm().put(e.arm(), pathRead(
                                new TypedVariable(arms.get(e.arm()).rowVar(), new ExprType(aRow, one)),
                                aRow, path, new ExprType(t, optional)));
                    }
                }
            }
        }
        // ONE group into ONE plain set (the class's root or sole set): a
        // plain step over the set's own rows, reading the raw target
        // columns — no union, no keys (the engine's shape; a one-arm
        // projection would only be re-rooted away by the flatten path)
        if (!targetIsStack && onePlainTarget(mapping, groups.values())) {
            List<Group> gs0 = new ArrayList<>(groups.values());
            Entry e0 = gs0.get(0).entries().get(0);
            Type.RelationType tRow0 = rowOf(e0.cond().functionType().params().get(1).type());
            if (tRow0 != null) {
                java.util.function.Function<Type.RelationType, TypedLambda> plain = srcRow ->
                        plainPredicate(gs0, srcRow, tRow0);
                TypedSpec target = java.util.Objects.requireNonNull(steps.get(e0.arm())).target();
                return new Lift(alias, new Type.ClassType(targetClass), target, srcCols,
                        List.of(), tRow0, plain, null);
            }
        }
        // the routed union's row: the groups' keys, typed by the reads
        List<Type.Column> keyCols = new ArrayList<>();
        Set<String> keyNames = new LinkedHashSet<>();
        List<TypedNavigate.Route> routes = new ArrayList<>();
        for (Group g : groups.values()) {
            Entry e0 = g.entries().get(0);
            List<String> reads = new ArrayList<>();
            List<String> names = new ArrayList<>();
            for (Entry e : g.entries()) {
                Type.RelationType tRow = rowOf(e.cond().functionType().params().get(1).type());
                for (String read : e.targetReads()) {
                    Type kt = tRow == null ? null : pathType(tRow, read);
                    if (kt == null) {
                        throw new MappingResolutionException("lift '" + alias + "': the route"
                                + " condition reads '" + read + "', which the target rows do not"
                                + " carry (condition typed " + e.cond().info().type().typeName()
                                + ")", targetClass);
                    }
                    for (String name : List.of(g.keyByRead().get(read), g.pairKeyByRead().get(read))) {
                        if (keyNames.add(name)) {
                            keyCols.add(new Type.Column(name, kt, optional));
                        }
                        if (!names.contains(name)) {
                            names.add(name);
                            reads.add(read);
                        }
                    }
                }
            }
            routes.add(new TypedNavigate.Route(g.target(), g.rows(), e0.cond(), reads, names));
        }
        Type.RelationType urow = new Type.RelationType(keyCols);
        List<Group> gs = new ArrayList<>(groups.values());
        java.util.function.Function<Type.RelationType, TypedLambda> strict = srcRow ->
                strictPredicate(gs, srcRow, urow, false);
        java.util.function.Function<Type.RelationType, TypedLambda> paired = srcRow ->
                strictPredicate(gs, srcRow, urow, true);
        Entry first = entries.get(0);
        TypedSpec target = java.util.Objects.requireNonNull(steps.get(first.arm())).target();
        return new Lift(alias, new Type.ClassType(targetClass), target, srcCols, routes, urow,
                strict, paired);
    }

    /** The source keys of an entry's reads: a modeled read is its property
     * (the stack's column), any other a per-entry column. */
    private static List<String> sourceKeys(int liftIx, int entryIx, List<String> reads,
            Map<String, String> modeled) {
        List<String> out = new ArrayList<>(reads.size());
        for (int k = 0; k < reads.size(); k++) {
            String prop = modeled.get(reads.get(k));
            out.add(prop != null ? prop : "__s_" + liftIx + "_" + entryIx + "_" + k);
        }
        return out;
    }

    /** The pins {@code arm}'s binding declares for {@code alias}. */
    private List<String> pinsOf(MappingDefinition mapping, ClassSource arm, String alias) {
        MappingDefinition.ClassBinding ab = arm.setId() == null ? null
                : sources.findBinding(mapping, arm.classFqn(), arm.setId(), new LinkedHashSet<>());
        return ab instanceof MappingDefinition.ClassBinding.Relational rb
                ? rb.propertyPins().getOrDefault(alias, List.of()) : List.of();
    }

    /** Every class-extent entry of a PINNED arm retargeted to the pinned
     * set's function (one pin per extent entry: the emitter turned a root
     * pin into the extent); an unpinned extent entry stays the extent. */
    private List<Entry> retargetPinned(List<Entry> entries, MappingDefinition mapping,
            String targetClass, String alias, List<ClassSource> arms) {
        List<Entry> out = new ArrayList<>(entries.size());
        for (Entry e : entries) {
            List<String> pins = e.target() instanceof TypedGetAll
                    ? pinsOf(mapping, arms.get(e.arm()), alias) : List.of();
            if (pins.size() != 1) {
                out.add(e);
                continue;
            }
            MappingDefinition.ClassBinding pb = sources.findBinding(mapping, targetClass,
                    pins.get(0), new LinkedHashSet<>());
            if (pb == null) {
                // a pin the queried mapping does not bind by that id (a
                // class-level binding under its default id, a set of another
                // mapping): the engine's lookup misses and falls back to the
                // class's root — the extent stays
                out.add(e);
                continue;
            }
            var fns = ctx.findFunction(pb.functionFqn());
            if (fns.size() != 1) {
                throw new IllegalStateException("resolver bug: set function '" + pb.functionFqn()
                        + "' has " + fns.size() + " registrations");
            }
            TypedFunction fn = fns.get(0);
            TypedSpec call = new TypedUserCall(fn, List.of(),
                    new ExprType(fn.returnType(), fn.returnMultiplicity()));
            out.add(new Entry(e.arm(), call, call, e.cond(), e.targetReads(), e.sourceReads(),
                    e.sourceKeys(), e.pairSourceKeys()));
        }
        return out;
    }

    /** The target set's MODELED columns: physical column -> the scalar
     * property that reads it directly (a set's function or the class extent
     * — its root/sole set — resolved under the queried mapping). */
    private Map<String, String> modeledColumns(MappingDefinition mapping, String targetClass,
            TypedSpec target) {
        ClassSource set;
        if (target instanceof TypedUserCall uc) {
            MappingDefinition.ClassBinding cb = sources.findBindingByFunction(mapping,
                    uc.callee().qualifiedName(), new LinkedHashSet<>());
            if (cb == null || cb instanceof MappingDefinition.ClassBinding.Operation) {
                return Map.of();
            }
            set = sources.get(mapping.qualifiedName(), cb.classFqn(), cb.setId(), null, "", null);
        } else if (target instanceof TypedGetAll ga) {
            MappingDefinition.ClassBinding cb = findBinding(mapping, ga.classFqn());
            if (cb == null || cb instanceof MappingDefinition.ClassBinding.Operation) {
                return Map.of();
            }
            set = sources.get(mapping.qualifiedName(), ga.classFqn(), null);
        } else {
            return Map.of();
        }
        return modeledOf(set, targetClass);
    }

    /** {@code set}'s MODELED columns: physical column -> the scalar property
     * of {@code classFqn} whose binding reads it directly. */
    private Map<String, String> modeledOf(ClassSource set, String classFqn) {
        Map<String, String> out = new LinkedHashMap<>();
        for (var b : set.bindings().entrySet()) {
            // the [1] conformance wrap (trustOne) is not a column read
            if (Pipelines.unwrapToOne(b.getValue()) instanceof TypedPropertyAccess pa
                    && pa.source() instanceof TypedVariable v && v.name().equals(set.rowVar())
                    && ctx.findProperty(classFqn, b.getKey()).isPresent()) {
                out.putIfAbsent(pa.property(), b.getKey());
            }
        }
        return out;
    }

    /** {@code (s, u) | OR over routes of the route's condition}: its source
     * reads left on the parent row (renamed to {@code s}), its target reads
     * re-pointed at the union row's keys by position — the union heads'
     * condition (each branch a route). */
    TypedLambda orOverRoutes(List<TypedNavigate.Route> routes, Type.RelationType srcRow,
            Type.RelationType urow) {
        var one = Multiplicity.Bounded.ONE;
        var boolOne = new ExprType(Type.Primitive.BOOLEAN, one);
        TypedVariable s = new TypedVariable("s", new ExprType(srcRow, one));
        TypedVariable u = new TypedVariable("u", new ExprType(urow, one));
        TypedSpec or = null;
        Set<TypedSpec> seen = new LinkedHashSet<>();
        for (TypedNavigate.Route r : routes) {
            TypedLambda c = r.cond();
            TypedSpec body = c.body().get(c.body().size() - 1);
            Map<String, String> tMap = new LinkedHashMap<>();
            for (int k = 0; k < r.targetReads().size(); k++) {
                tMap.put(r.targetReads().get(k), r.keyNames().get(k));
            }
            TypedSpec re = renameReads(body, c.parameters().get(1), tMap, u, urow);
            re = renameVar(re, c.parameters().get(0), s);
            if (!seen.add(re)) {
                continue;
            }
            or = or == null ? re : new TypedNativeCall(orFn(), List.of(or, re), boolOne, null);
        }
        return lambda(s, u, java.util.Objects.requireNonNull(or), srcRow, urow);
    }

    /** {@code (s, u) | OR over groups of the group's condition with its
     * source reads re-pointed at the stack's columns and its target reads
     * at the union row's keys}. */
    private TypedLambda strictPredicate(List<Group> gs, Type.RelationType srcRow,
            Type.RelationType urow, boolean perPair) {
        var one = Multiplicity.Bounded.ONE;
        var boolOne = new ExprType(Type.Primitive.BOOLEAN, one);
        TypedVariable s = new TypedVariable("s", new ExprType(srcRow, one));
        TypedVariable u = new TypedVariable("u", new ExprType(urow, one));
        TypedSpec or = null;
        Set<TypedSpec> seen = new LinkedHashSet<>();
        for (Group g : gs) {
            for (Entry e : g.entries()) {
                TypedSpec re = repoint(e, g, s, u, srcRow, urow, perPair);
                if (!seen.add(re)) {
                    continue;   // two routes spelling one conjunct (a self-join's arms)
                }
                or = or == null ? re : new TypedNativeCall(orFn(), List.of(or, re), boolOne, null);
            }
        }
        return lambda(s, u, java.util.Objects.requireNonNull(or), srcRow, urow);
    }

    /** Every group lands on the SAME plain set (the class's root or sole
     * set, or its extent) over the set's own rows: the plain form applies,
     * OR-ed over the groups' conditions. */
    private boolean onePlainTarget(MappingDefinition mapping, Collection<Group> groups) {
        Object identity = null;
        for (Group g : groups) {
            Entry e0 = g.entries().get(0);
            boolean plain = e0.target() instanceof TypedGetAll
                    || (e0.target() instanceof TypedUserCall uc
                            && rootOrSole(mapping, uc.callee().qualifiedName()));
            if (!plain || Pipelines.containsSlot(g.rows())) {
                return false;   // a route joining its mids inside is not the set's own rows
            }
            Object id = targetIdentity(e0.target());
            if (identity != null && !identity.equals(id)) {
                return false;
            }
            identity = id;
        }
        return identity != null;
    }

    /** The PLAIN form (the groups into one plain set): each group's
     * condition with its source reads on the stack's columns and its
     * target reads left on the set's own row, OR-ed across the groups. */
    private TypedLambda plainPredicate(List<Group> gs, Type.RelationType srcRow,
            Type.RelationType tRow) {
        var one = Multiplicity.Bounded.ONE;
        var boolOne = new ExprType(Type.Primitive.BOOLEAN, one);
        TypedVariable s = new TypedVariable("s", new ExprType(srcRow, one));
        TypedVariable u = new TypedVariable("u", new ExprType(tRow, one));
        TypedSpec or = null;
        Set<TypedSpec> seen = new LinkedHashSet<>();
        for (Group g : gs) {
            for (Entry e : g.entries()) {
                TypedLambda c = e.cond();
                TypedSpec body = c.body().get(c.body().size() - 1);
                Map<String, String> sMap = new LinkedHashMap<>();
                for (int k = 0; k < e.sourceReads().size(); k++) {
                    sMap.put(e.sourceReads().get(k), e.sourceKeys().get(k));
                }
                TypedSpec re = renameReads(body, c.parameters().get(0), sMap, s, srcRow);
                re = renameVar(re, c.parameters().get(1), u);
                if (!seen.add(re)) {
                    continue;
                }
                or = or == null ? re : new TypedNativeCall(orFn(), List.of(or, re), boolOne, null);
            }
        }
        return lambda(s, u, java.util.Objects.requireNonNull(or), srcRow, tRow);
    }

    private static TypedSpec renameVar(TypedSpec n, String from, TypedVariable to) {
        if (n instanceof TypedVariable v && v.name().equals(from)) {
            return to;
        }
        List<TypedSpec> kids = n.children();
        if (kids.isEmpty()) {
            return n;
        }
        return n.withChildren(kids.stream().map(k -> renameVar(k, from, to)).toList());
    }

    /** Whether {@code functionFqn} realizes the ROOT or SOLE set of its class
     * (the class-level lookup lands on it). */
    private boolean rootOrSole(MappingDefinition mapping, String functionFqn) {
        MappingDefinition.ClassBinding cb = sources.findBindingByFunction(mapping, functionFqn,
                new LinkedHashSet<>());
        if (cb == null) {
            return false;
        }
        MappingDefinition.ClassBinding classLevel = findBinding(mapping, cb.classFqn());
        return classLevel != null && classLevel.functionFqn().equals(functionFqn);
    }

    private static TypedLambda lambda(TypedVariable s, TypedVariable u, TypedSpec body,
            Type.RelationType srcRow, Type.RelationType urow) {
        var one = Multiplicity.Bounded.ONE;
        return new TypedLambda(List.of(s.name(), u.name()), List.of(body),
                new ExprType(new Type.FunctionType(
                        List.of(new Type.Param(srcRow, one), new Type.Param(urow, one)),
                        new Type.Param(Type.Primitive.BOOLEAN, one)), one));
    }

    /** The entry's condition over (stack row, union row): source reads by
     * path -> the group's source columns, target reads by path -> the keys. */
    private static TypedSpec repoint(Entry e, Group g, TypedVariable s, TypedVariable u,
            Type.RelationType srcRow, Type.RelationType urow, boolean perPair) {
        TypedLambda c = e.cond();
        TypedSpec body = c.body().get(c.body().size() - 1);
        String sv = c.parameters().get(0);
        String tv = c.parameters().get(1);
        Map<String, String> sMap = new LinkedHashMap<>();
        for (int k = 0; k < e.sourceReads().size(); k++) {
            sMap.put(e.sourceReads().get(k),
                    (perPair ? e.pairSourceKeys() : e.sourceKeys()).get(k));
        }
        Map<String, String> tMap = new LinkedHashMap<>();
        for (int k = 0; k < e.targetReads().size(); k++) {
            String read = e.targetReads().get(k);
            tMap.put(read, java.util.Objects.requireNonNull(
                    (perPair ? g.pairKeyByRead() : g.keyByRead()).get(read)));
        }
        TypedSpec re = renameReads(body, sv, sMap, s, srcRow);
        return renameReads(re, tv, tMap, u, urow);
    }

    private static TypedSpec renameReads(TypedSpec n, String var, Map<String, String> byPath,
            TypedVariable to, Type.RelationType row) {
        String path = readPath(n, var);
        if (path != null) {
            String col = byPath.get(path);
            if (col == null) {
                throw new IllegalStateException("resolver bug: lift read '" + path + "' not collected");
            }
            Type.Column c = java.util.Objects.requireNonNull(columnOf(row, col),
                    "resolver bug: lift column '" + col + "' missing from its row");
            return new TypedPropertyAccess(to, c.name(), new ExprType(c.type(), c.multiplicity()));
        }
        if (n instanceof TypedVariable v && v.name().equals(var)) {
            return to;
        }
        List<TypedSpec> kids = n.children();
        if (kids.isEmpty()) {
            return n;
        }
        return n.withChildren(kids.stream().map(k -> renameReads(k, var, byPath, to, row)).toList());
    }

    /** The read path of {@code n} off {@code var}: {@code col} or
     * {@code slot.col} (one joined sub-row deep); null otherwise. */
    static @com.legend.base.Nullable String readPath(TypedSpec n, String var) {
        if (n instanceof TypedPropertyAccess pa) {
            if (pa.source() instanceof TypedVariable v && v.name().equals(var)) {
                return pa.property();
            }
            if (pa.source() instanceof TypedPropertyAccess inner
                    && inner.source() instanceof TypedVariable v2 && v2.name().equals(var)) {
                return inner.property() + "." + pa.property();
            }
        }
        return null;
    }

    static void collectReads(TypedSpec n, String var, List<String> out) {
        String path = readPath(n, var);
        if (path != null) {
            if (!out.contains(path)) {
                out.add(path);
            }
            return;
        }
        for (TypedSpec c : n.children()) {
            collectReads(c, var, out);
        }
    }

    /** The condition's SHAPE: target reads erased to a placeholder, source
     * reads reduced to their bare paths, the variables normalized. */
    /** Whether the entries share their keys (see liftOf): two or more, ONE
     * condition shape, every (arm, target set) pair exactly once, and within
     * each target set the same target reads in the same positions (a key
     * column is one target column per set). */
    private static boolean sharedKeys(List<Entry> entries) {
        if (entries.size() < 2) {
            return false;
        }
        TypedSpec shape = null;
        Set<Integer> armIds = new LinkedHashSet<>();
        Map<Object, List<String>> readsBySet = new LinkedHashMap<>();
        Set<List<Object>> pairs = new LinkedHashSet<>();
        for (Entry e : entries) {
            TypedSpec sh = condShape(e.cond());
            if (shape == null) {
                shape = sh;
            } else if (!shape.equals(sh)) {
                return false;
            }
            Object set = targetIdentity(e.target());
            List<String> order = targetReadOrder(e);
            if (!order.containsAll(e.targetReads())) {
                return false;
            }
            List<String> prior = readsBySet.putIfAbsent(set, order);
            if (prior != null && !prior.equals(order)) {
                return false;
            }
            armIds.add(e.arm());
            if (!pairs.add(List.of(e.arm(), set))) {
                return false;
            }
        }
        return pairs.size() == armIds.size() * readsBySet.size();
    }

    /** An entry's target reads in the order its condition reads them (the
     * order condShape's erased positions follow). */
    private static List<String> targetReadOrder(Entry e) {
        TypedSpec body = e.cond().body().get(e.cond().body().size() - 1);
        List<String> out = new ArrayList<>();
        collectReads(body, e.cond().parameters().get(1), out);
        return out;
    }

    /** A condition's SHAPE: its body with the target reads erased and the
     * source reads normalized — a typed node whose structural equality is
     * the identity (two routes of one shape read the same source paths
     * against some target column). */
    private static TypedSpec condShape(TypedLambda cond) {
        TypedSpec body = cond.body().get(cond.body().size() - 1);
        return erase(body, cond.parameters().get(1), cond.parameters().get(0));
    }

    private static TypedSpec erase(TypedSpec n, String tVar, String sVar) {
        String tp = readPath(n, tVar);
        if (tp != null && n instanceof TypedPropertyAccess pa) {
            return new TypedPropertyAccess(new TypedVariable("?", pa.info()), "?", pa.info());
        }
        String sp = readPath(n, sVar);
        if (sp != null && n instanceof TypedPropertyAccess pa) {
            return new TypedPropertyAccess(new TypedVariable("s", pa.info()), sp, pa.info());
        }
        List<TypedSpec> kids = n.children();
        if (kids.isEmpty()) {
            return n;
        }
        List<TypedSpec> erased = kids.stream().map(c -> erase(c, tVar, sVar)).toList();
        if (n instanceof TypedNativeCall nc) {
            return new TypedNativeCall(nc.callee(), erased, nc.info(), null);
        }
        return n.withChildren(erased);
    }

    /** A route target's IDENTITY: the set function it calls, the class it
     * extends over, or the relation node itself (structural equality). */
    private static Object targetIdentity(TypedSpec target) {
        return switch (target) {
            case TypedUserCall uc -> uc.callee().qualifiedName();
            case TypedGetAll ga -> ga.classFqn();
            default -> target;
        };
    }

    /** A row type from either spelling: a lambda parameter is typed as the
     * bare row, a relation value as {@code Relation<row>}. */
    static Type.@com.legend.base.Nullable RelationType rowOf(Type t) {
        Type.RelationType r = Type.relationSchema(t);
        return r != null ? r : t instanceof Type.RelationType bare ? bare : null;
    }

    static @com.legend.base.Nullable Type pathType(Type.RelationType row, String path) {
        Type.RelationType at = row;
        Type found = null;
        String[] parts = path.split("\\.");
        for (int i = 0; i < parts.length; i++) {
            Type.Column c = columnOf(at, parts[i]);
            if (c == null) {
                return null;
            }
            found = c.type();
            if (i + 1 < parts.length) {
                Type.RelationType sub = rowOf(c.type());
                if (sub == null) {
                    return null;
                }
                at = sub;
            }
        }
        return found;
    }

    static TypedSpec pathRead(TypedVariable m, Type.RelationType row, String path, ExprType leafInfo) {
        String[] parts = path.split("\\.");
        if (parts.length == 1) {
            return new TypedPropertyAccess(m, path, leafInfo);
        }
        Type.Column slot = java.util.Objects.requireNonNull(columnOf(row, parts[0]));
        TypedSpec sub = new TypedPropertyAccess(m, parts[0], new ExprType(slot.type(), slot.multiplicity()));
        return new TypedPropertyAccess(sub, parts[1], leafInfo);
    }

    // rule (a) and (c) helpers — binding facts only, no source resolved
    // (a target's source may be mid-construction: mutual navigations)

    /** The class-level binding — {@link ClassSources#findBinding}, THE rule
     * (engine R1: own beats the includes', a later include beats an earlier
     * one, the root wins among a class's sets, a rootless multi-set class
     * has none). This used to be a second implementation: a breadth-first
     * walk taking the SHALLOWEST include and accepting any rootless binding,
     * which disagreed with the rule that resolves the arm it is deciding
     * about — and the answer decides which union arms are DEAD and read as
     * typed NULLs (audit 2026-09-15 P2-3). */
    private MappingDefinition.@com.legend.base.Nullable ClassBinding findBinding(MappingDefinition mapping,
            String classFqn) {
        return sources.findBinding(mapping, classFqn, null, new LinkedHashSet<>());
    }

    private static String classFqnOf(List<ClassSource> arms) {
        return arms.get(0).classFqn();
    }

    /** The set ids of the LEAF sets a class resolves to under the queried
     * mapping: its root binding's members (operations expanded), or the
     * root/sole set itself; null when the class has no such binding. */
    @com.legend.base.Nullable Set<String> leafSetIds(MappingDefinition mapping,
            String classFqn) {
        MappingDefinition.ClassBinding tb = findBinding(mapping, classFqn);
        if (tb == null) {
            List<MappingDefinition.ClassBinding> own = mapping.bindings().ofClass(classFqn);
            if (own.size() != 1) {
                return null;
            }
            tb = own.get(0);
        }
        Set<String> out = new LinkedHashSet<>();
        collectLeafSetIds(mapping, tb, out, new LinkedHashSet<>());
        return out;
    }

    private void collectLeafSetIds(MappingDefinition mapping,
            MappingDefinition.ClassBinding cb, Set<String> out, Set<String> seen) {
        if (!seen.add(cb.functionFqn())) {
            return;
        }
        if (!(cb instanceof MappingDefinition.ClassBinding.Operation op)) {
            out.add(ClassSources.setIdOf(cb));
            return;
        }
        // the arms are the binding's FACT (member order)
        for (String setId : op.memberSetIds()) {
            MappingDefinition.ClassBinding m = sources.findBindingBySetId(mapping, setId,
                    new LinkedHashSet<>());
            if (m != null) {
                collectLeafSetIds(mapping, m, out, seen);
            }
        }
    }

    private @com.legend.base.Nullable String targetSetIdOf(MappingDefinition mapping, TypedSpec target) {
        if (!(target instanceof TypedUserCall uc)) {
            return null;
        }
        MappingDefinition.ClassBinding cb = sources.findBindingByFunction(mapping,
                uc.callee().qualifiedName(), new LinkedHashSet<>());
        return cb == null ? null : cb.setId() == null ? cb.classFqn() : cb.setId();
    }

    private TypedFunction orFn() {
        return callees.bool("or");
    }

    // ------------------------------------------------------------------
    // THE DEMAND SEAM (B6): a column a later reader demands of a union row
    // — a navigate slot a further hop reads, a physical key a condition
    // reads — is projected per arm from the arm's own row, NULL where an
    // arm lacks it. The same demand-driven projection every plain source
    // gets from Pipelines.materialize, applied to the stack's arms.
    // ------------------------------------------------------------------

    /**
     * JOIN-KEY COLLECTION over a UNION pipeline (engine: each member thread
     * of a union subselect carries the demanded join-key columns — the
     * {@code FirmID_0}-family columns in the partial-union goldens; this is
     * the shared-name form): a navigation join over a concatenate reads
     * source key columns the member projections dropped — re-add each key
     * to EVERY member projection, reading the member's own physical column.
     * No concatenate in the pipeline: unchanged. A member whose row lacks
     * the column is LOUD (the per-member suffixed/NULL-filled form is the
     * union-to-union rung).
     */
    /** {@link #demandForKeys} applied to the concatenate BENEATH
     * a pipeline's navigate / join-slot / filter steps (a union source
     * carrying hoisted steps): the steps rebuild over the widened union. */
    static TypedSpec demandBelow(TypedSpec pipeline, Set<String> cols) {
        if (cols.isEmpty()) {
            return pipeline;
        }
        return switch (pipeline) {
            case TypedConcatenate cat -> demandForKeys(cat, cols);
            case TypedFilter f -> {
                TypedSpec inner = demandBelow(f.source(), cols);
                yield inner == f.source() ? pipeline
                        : new TypedFilter(inner, f.predicate(),
                                new ExprType(inner.info().type(), Multiplicity.Bounded.ONE));
            }
            case TypedNavigate nav -> {
                TypedSpec inner = demandBelow(nav.source(), cols);
                yield inner == nav.source() ? pipeline : nav.withSource(inner, nav.info());
            }
            case TypedJoinSlot js -> {
                TypedSpec inner = demandBelow(js.source(), cols);
                yield inner == js.source() ? pipeline
                        : new TypedJoinSlot(inner, js.alias(), js.target(),
                                js.condition(), js.frameName(), js.info());
            }
            default -> pipeline;
        };
    }

    static TypedSpec demandForKeys(TypedSpec pipeline, Set<String> cols) {
        if (pipeline instanceof TypedFilter f) {
            TypedSpec inner = demandForKeys(f.source(), cols);
            if (inner == f.source()) {
                return pipeline;
            }
            return new TypedFilter(inner, f.predicate(),
                    new ExprType(inner.info().type(), Multiplicity.Bounded.ONE));
        }
        // a ONE-thread union (a lone projected member)
        if (pipeline instanceof TypedProject lone) {
            List<String> lmissing = missingOf(lone, cols);
            return lmissing.isEmpty() ? pipeline
                    : demandOnArm(lone, 0, List.of(lone), lmissing);
        }
        if (!(pipeline instanceof TypedConcatenate cat)) {
            return pipeline;
        }
        Type.RelationType row = Type.requireRelationSchema(cat.info().type());
        Set<String> have = new LinkedHashSet<>();
        for (Type.Column c : row.columns()) {
            have.add(c.name());
        }
        List<String> missing = new ArrayList<>();
        for (String c : cols) {
            if (!have.contains(c)) {
                missing.add(c);
            }
        }
        if (missing.isEmpty()) {
            return pipeline;
        }
        // Flatten the left-deep concatenate: member ordinal i = the i-th
        // thread = the engine's `<col>_<i>` key-column suffix.
        List<TypedSpec> members = new ArrayList<>();
        flattenConcatenate(cat, members);
        List<TypedSpec> widened = new ArrayList<>(members.size());
        for (int i = 0; i < members.size(); i++) {
            widened.add(demandOnArm(members.get(i), i, members, missing));
        }
        TypedSpec out = widened.get(0);
        for (int i = 1; i < widened.size(); i++) {
            out = new TypedConcatenate(out,
                    widened.get(i),
                    new ExprType(out.info().type(), Multiplicity.Bounded.ONE));
        }
        return out;
    }

    /** {@code pipe} widened for the key columns a condition reads off
     * its {@code targetParam}-th parameter's row — a union target's
     * threads project the link keys they publish, so this is a no-op
     * unless a consumer materialized the target without them. */
    static TypedSpec demandForCondition(TypedSpec pipe, @com.legend.base.Nullable TypedLambda cond,
            int targetParam) {
        if (cond == null || cond.parameters().size() <= targetParam) {
            return pipe;
        }
        Set<String> reads = new LinkedHashSet<>();
        for (TypedSpec b : cond.body()) {
            Pipelines.collectVarReads(b, cond.parameters().get(targetParam), reads);
        }
        return reads.isEmpty() ? pipe : demandForKeys(pipe, reads);
    }

    /** The demanded columns the relation does not carry. */
    private static List<String> missingOf(TypedSpec pipeline, Set<String> cols) {
        Type.RelationType row = Type.requireRelationSchema(pipeline.info().type());
        List<String> missing = new ArrayList<>();
        for (String c : cols) {
            if (row.columns().stream().noneMatch(x -> x.name().equals(c))) {
                missing.add(c);
            }
        }
        return missing;
    }

    private static void flattenConcatenate(TypedSpec n, List<TypedSpec> out) {
        if (n instanceof TypedConcatenate cat) {
            flattenConcatenate(cat.left(), out);
            flattenConcatenate(cat.right(), out);
        } else {
            out.add(n);
        }
    }

    /**
     * Append {@code missing} key columns to a union member's projection:
     * every member reads its own physical column (the shared-name form —
     * a link key a routed member publishes, a lift's own key thread); a
     * member whose row lacks the column contributes a typed NULL of a
     * sibling's kind (engine SQLNull padding, pureToSQLQuery_union.pure:
     * 682-691: un-routed threads must never match).
     */
    private static TypedSpec demandOnArm(TypedSpec side, int ordinal,
            List<TypedSpec> members, List<String> missing) {
        if (!(side instanceof TypedProject p)) {
            throw new NotImplementedException(
                    "a navigation join over this union demands key columns "
                    + missing + ", but a union member is a "
                    + side.getClass().getSimpleName()
                    + " — only projected members widen");
        }
        Type.RelationType srcRow = Type.requireRelationSchema(p.source().info().type());
        List<TypedFuncCol> newCols = new ArrayList<>(p.columns());
        List<Type.Column> outCols = new ArrayList<>(
                (Type.requireRelationSchema(p.info().type())).columns());
        String v = "u_k";
        TypedVariable row = new TypedVariable(v,
                new ExprType(srcRow, Multiplicity.Bounded.ONE));
        for (String c : missing) {
            TypedSpec body;
            Type colDeclType;
            Type.Column src = columnOf(srcRow, c);
            if (src != null) {
                body = read(row, src);
                colDeclType = src.type();
            } else {
                Type sibling = null;
                for (TypedSpec m : members) {
                    TypedSpec msrc = m instanceof TypedProject mp ? mp.source() : m;
                    if (Type.relationSchema(msrc.info().type()) instanceof Type.RelationType mr) {
                        Type.Column mc = columnOf(mr, c);
                        if (mc != null) {
                            sibling = mc.type();
                            break;
                        }
                    }
                }
                if (sibling == null) {
                    List<String> rows = new ArrayList<>();
                    for (TypedSpec m : members) {
                        TypedSpec msrc = m;
                        rows.add(Type.relationSchema(msrc.info().type()) instanceof Type.RelationType mr
                                ? mr.columns().stream().map(Type.Column::name).toList().toString()
                                : msrc.getClass().getSimpleName());
                    }
                    throw new NotImplementedException(
                            "a navigation join over this union demands key column '"
                            + c + "', which NO union member carries (members' rows: " + rows + ")");
                }
                body = new TypedCollection(List.of(),
                        new ExprType(sibling, Multiplicity.Bounded.ZERO_ONE));
                colDeclType = sibling;
            }
            var fnType = new Type.FunctionType(
                    List.of(new Type.Param(srcRow, Multiplicity.Bounded.ONE)),
                    new Type.Param(colDeclType, Multiplicity.Bounded.ZERO_ONE));
            newCols.add(new TypedFuncCol(c, new TypedLambda(List.of(v), List.of(body),
                    new ExprType(fnType, Multiplicity.Bounded.ONE))));
            outCols.add(new Type.Column(c, colDeclType, Multiplicity.Bounded.ZERO_ONE));
        }
        return new TypedProject(p.source(), newCols,
                new ExprType(Type.relation(new Type.RelationType(outCols)),
                        Multiplicity.Bounded.ONE));
    }

    private static TypedSpec read(TypedVariable row, Type.Column src) {
        return new TypedPropertyAccess(row, src.name(),
                new ExprType(src.type(), src.multiplicity()));
    }
}
