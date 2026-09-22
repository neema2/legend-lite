package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.ArrayList;
import java.util.List;

/**
 * The RESOLVED graph-serialize envelope (Phase H output; consumed only by
 * the lowerer) &mdash; a relation {@code source} plus the JSON shape to
 * project over it:
 *
 * <ul>
 *   <li>{@code leaves} &mdash; one {@code (property name, row lambda)} per
 *       scalar leaf of the fetch tree, exactly project-column-shaped; the
 *       envelope keys the {@code json_object} by these names. Column
 *       pruning is BY CONSTRUCTION: only tree leaves appear.</li>
 *   <li>{@code children} &mdash; one nested node per class-typed tree
 *       child. The child's {@code source} is the target class's pipeline
 *       FILTERED by the association condition with the PARENT row variable
 *       free ({@code rowVar}) &mdash; the same correlated shape as the
 *       EXISTS material, resolved through the lowerer's enclosing-scope
 *       channel. A to-many child sets {@code arrayWrap}.</li>
 *   <li>{@code arrayWrap} &mdash; aggregate the per-row objects into ONE
 *       JSON-array value ({@code json_group_array}): the SNAPSHOT root and
 *       every to-many child; a to-one child projects the bare object.</li>
 * </ul>
 *
 * <p>{@code info} stays CLASS-typed (the fetched class collection):
 * {@link com.legend.exec.ResultShape} classifies the root GRAPH, and the
 * executor reads the single {@code result} column as the JSON payload.
 *
 * @param source    the resolved relation pipeline supplying the rows
 * @param rowVar    the row variable the leaves bind and children correlate against
 * @param leaves    scalar leaf projections, fetch-tree order
 * @param nested    nested class-typed children, fetch-tree order
 * @param arrayWrap whether the objects aggregate into one JSON array
 * @param bareValue TO-MANY PRIMITIVE leaf mode: exactly one leaf, and the
 *                  aggregation collects the bare VALUES (a JSON array of
 *                  scalars), never json_object envelopes — the engine's
 *                  {@code otherNames: ["abc","def"]} shape
 * @param info      the fetched class collection's type (GRAPH shape)
 */
public record TypedSerializeGraph(TypedSpec source, String rowVar,
                                  List<TypedFuncCol> leaves, List<Child> nested,
                                  boolean arrayWrap, boolean bareValue,
                                  @com.legend.base.Nullable String classFqn,
                                  ExprType info,
                                  boolean inlineChild,
                                  List<SubTypePatch> subTypePatches,
                                  List<TypedFuncCol> orderKeys,
                                  @com.legend.base.Nullable String typeKeyName,
                                  boolean fqTypePath,
                                  @com.legend.base.Nullable List<CheckedConstraint> checkedConstraints,
                                  boolean removeNullKeys,
                                  boolean removeEmptySets,
                                  @com.legend.base.Nullable String objectRefPrefix)
        implements TypedSpec {

    /** The same graph over another row source (a resolver fusing two
     * terminals of one layout over the union of their sources). */
    public TypedSerializeGraph withSource(TypedSpec newSource) {
        return new TypedSerializeGraph(newSource, rowVar, leaves, nested, arrayWrap,
                bareValue, classFqn, info, inlineChild, subTypePatches, orderKeys,
                typeKeyName, fqTypePath, checkedConstraints, removeNullKeys,
                removeEmptySets, objectRefPrefix);
    }

    /** Config compat (no objectReference channel). */
    public TypedSerializeGraph(TypedSpec source, String rowVar,
            List<TypedFuncCol> leaves, List<Child> nested, boolean arrayWrap,
            boolean bareValue, @com.legend.base.Nullable String classFqn, ExprType info,
            boolean inlineChild, List<SubTypePatch> subTypePatches,
            List<TypedFuncCol> orderKeys,
            @com.legend.base.Nullable String typeKeyName,
            boolean fqTypePath,
            @com.legend.base.Nullable List<CheckedConstraint> checkedConstraints,
            boolean removeNullKeys, boolean removeEmptySets) {
        this(source, rowVar, leaves, nested, arrayWrap, bareValue, classFqn,
                info, inlineChild, subTypePatches, orderKeys, typeKeyName,
                fqTypePath, checkedConstraints, removeNullKeys,
                removeEmptySets, null);
    }

    /** Pre-config compat (removeNull/removeEmpty default off). */
    public TypedSerializeGraph(TypedSpec source, String rowVar,
            List<TypedFuncCol> leaves, List<Child> nested, boolean arrayWrap,
            boolean bareValue, @com.legend.base.Nullable String classFqn, ExprType info,
            boolean inlineChild, List<SubTypePatch> subTypePatches,
            List<TypedFuncCol> orderKeys,
            @com.legend.base.Nullable String typeKeyName,
            boolean fqTypePath,
            @com.legend.base.Nullable List<CheckedConstraint> checkedConstraints) {
        this(source, rowVar, leaves, nested, arrayWrap, bareValue, classFqn,
                info, inlineChild, subTypePatches, orderKeys, typeKeyName,
                fqTypePath, checkedConstraints, false, false);
    }

    /** This envelope with the array aggregate FORCED — the scalar
     * (snapshot) position needs the whole result as one value; all
     * other components carry over verbatim. */
    public TypedSerializeGraph asArrayWrapped() {
        return arrayWrap ? this : new TypedSerializeGraph(source, rowVar,
                leaves, nested, true, bareValue, classFqn, info,
                inlineChild, subTypePatches, orderKeys, typeKeyName,
                fqTypePath, checkedConstraints, removeNullKeys,
                removeEmptySets, objectRefPrefix);
    }

    /** Name prefix marking a PK DETERMINISM order key (ASC, best-effort at
     * lowering) — everything unprefixed in {@code orderKeys} is a union
     * WITNESS key (DESC, load-bearing). The prefix keeps the distinction in
     * typed HIR: the lowering layer must not consult the mapping model. */
    public static final String PK_ORDER_PREFIX = "pk_ord__";

    /** One class constraint riding a CHECKED envelope: the per-row defect
     * gate. Predicate/message are row lambdas (bindings already inlined);
     * the remaining fields are the defect object's constants. */
    public record CheckedConstraint(String id, TypedFuncCol predicate,
            TypedFuncCol message, String level, String definerFqn) {
    }

    /** Unchecked compat (the common envelope). */
    public TypedSerializeGraph(TypedSpec source, String rowVar,
            List<TypedFuncCol> leaves, List<Child> nested, boolean arrayWrap,
            boolean bareValue, @com.legend.base.Nullable String classFqn, ExprType info,
            boolean inlineChild, List<SubTypePatch> subTypePatches,
            List<TypedFuncCol> orderKeys,
            @com.legend.base.Nullable String typeKeyName,
            boolean fqTypePath) {
        this(source, rowVar, leaves, nested, arrayWrap, bareValue, classFqn,
                info, inlineChild, subTypePatches, orderKeys, typeKeyName,
                fqTypePath, null);
    }

    public TypedSerializeGraph {
        leaves = List.copyOf(leaves);
        nested = List.copyOf(nested);
        subTypePatches = subTypePatches == null ? List.of()
                : List.copyOf(subTypePatches);
        orderKeys = orderKeys == null ? List.of() : List.copyOf(orderKeys);
    }

    /** Type-key-free compat (includeType off — the common shape). */
    public TypedSerializeGraph(TypedSpec source, String rowVar,
            List<TypedFuncCol> leaves, List<Child> nested, boolean arrayWrap,
            boolean bareValue, @com.legend.base.Nullable String classFqn, ExprType info,
            boolean inlineChild, List<SubTypePatch> subTypePatches,
            List<TypedFuncCol> orderKeys) {
        this(source, rowVar, leaves, nested, arrayWrap, bareValue, classFqn,
                info, inlineChild, subTypePatches, orderKeys, null, false);
    }

    /** Order-free compat: envelope row order = scan order. */
    public TypedSerializeGraph(TypedSpec source, String rowVar,
            List<TypedFuncCol> leaves, List<Child> nested, boolean arrayWrap,
            boolean bareValue, @com.legend.base.Nullable String classFqn, ExprType info,
            boolean inlineChild, List<SubTypePatch> subTypePatches) {
        this(source, rowVar, leaves, nested, arrayWrap, bareValue, classFqn,
                info, inlineChild, subTypePatches, List.of());
    }

    /** A ->subType(@X){...} view: leaves reading the subtype member's
     * carrier columns, rendered as a JSON MERGE PATCH over the envelope —
     * NULL values (non-member rows) drop their keys (RFC 7386). */
    public record SubTypePatch(String subTypeFqn, List<TypedFuncCol> leaves,
            TypedFuncCol member, List<Child> children) {
        public SubTypePatch {
            children = children == null ? List.of() : List.copyOf(children);
        }
    }

    /** Patch-free compat (every pre-subType construction). */
    public TypedSerializeGraph(TypedSpec source, String rowVar,
            List<TypedFuncCol> leaves, List<Child> nested, boolean arrayWrap,
            boolean bareValue, @com.legend.base.Nullable String classFqn, ExprType info,
            boolean inlineChild) {
        this(source, rowVar, leaves, nested, arrayWrap, bareValue, classFqn,
                info, inlineChild, List.of());
    }

    /** Correlated node (the common case — an inline child reads the
     * PARENT row directly: embedded ctor bindings, no join/subquery). */
    public TypedSerializeGraph(TypedSpec source, String rowVar,
            List<TypedFuncCol> leaves, List<Child> nested,
            boolean arrayWrap, boolean bareValue,
            @com.legend.base.Nullable String classFqn,
            ExprType info) {
        this(source, rowVar, leaves, nested, arrayWrap, bareValue,
                classFqn, info, false);
    }

    /** Provenance-free compat (nested children, tests). */
    public TypedSerializeGraph(TypedSpec source, String rowVar,
            List<TypedFuncCol> leaves, List<Child> nested, boolean arrayWrap,
            boolean bareValue, ExprType info) {
        this(source, rowVar, leaves, nested, arrayWrap, bareValue, null, info);
    }

    /** The envelope form: objects keyed by leaves ({@code bareValue} = false). */
    public TypedSerializeGraph(TypedSpec source, String rowVar,
            List<TypedFuncCol> leaves, List<Child> nested, boolean arrayWrap,
            ExprType info) {
        this(source, rowVar, leaves, nested, arrayWrap, false, null, info);
    }

    /** One nested hop: the property name and the child's own envelope. */
    public record Child(String property, TypedSerializeGraph node) {
    }

    @Override
    public List<TypedSpec> children() {
        List<TypedSpec> out = new ArrayList<>();
        out.add(source);
        leaves.forEach(l -> out.add(l.fn()));
        nested.forEach(c -> out.add(c.node()));
        subTypePatches.forEach(p -> {
            p.leaves().forEach(l -> out.add(l.fn()));
            out.add(p.member().fn());
            p.children().forEach(c -> out.add(c.node()));
        });
        orderKeys.forEach(k -> out.add(k.fn()));
        if (checkedConstraints != null) {
            checkedConstraints.forEach(c -> {
                out.add(c.predicate().fn());
                out.add(c.message().fn());
            });
        }
        return out;
    }

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        int i = 1;
        java.util.List<TypedFuncCol> ls = new java.util.ArrayList<>(leaves.size());
        for (TypedFuncCol l : leaves) {
            ls.add(new TypedFuncCol(l.name(), (TypedLambda) kids.get(i++), l.documentation()));
        }
        java.util.List<Child> ns = new java.util.ArrayList<>(nested.size());
        for (Child c : nested) {
            ns.add(new Child(c.property(), (TypedSerializeGraph) kids.get(i++)));
        }
        java.util.List<SubTypePatch> sps = new java.util.ArrayList<>(subTypePatches.size());
        for (SubTypePatch p : subTypePatches) {
            java.util.List<TypedFuncCol> pl = new java.util.ArrayList<>(p.leaves().size());
            for (TypedFuncCol l : p.leaves()) {
                pl.add(new TypedFuncCol(l.name(), (TypedLambda) kids.get(i++), l.documentation()));
            }
            TypedFuncCol mem = new TypedFuncCol(p.member().name(),
                    (TypedLambda) kids.get(i++), p.member().documentation());
            java.util.List<Child> pc = new java.util.ArrayList<>(p.children().size());
            for (Child c : p.children()) {
                pc.add(new Child(c.property(), (TypedSerializeGraph) kids.get(i++)));
            }
            sps.add(new SubTypePatch(p.subTypeFqn(), pl, mem, pc));
        }
        java.util.List<TypedFuncCol> oks = new java.util.ArrayList<>(orderKeys.size());
        for (TypedFuncCol k : orderKeys) {
            oks.add(new TypedFuncCol(k.name(), (TypedLambda) kids.get(i++), k.documentation()));
        }
        java.util.List<CheckedConstraint> ccs = null;
        if (checkedConstraints != null) {
            ccs = new java.util.ArrayList<>(checkedConstraints.size());
            for (CheckedConstraint c : checkedConstraints) {
                TypedFuncCol pr = new TypedFuncCol(c.predicate().name(),
                        (TypedLambda) kids.get(i++), c.predicate().documentation());
                TypedFuncCol msg = new TypedFuncCol(c.message().name(),
                        (TypedLambda) kids.get(i++), c.message().documentation());
                ccs.add(new CheckedConstraint(c.id(), pr, msg, c.level(), c.definerFqn()));
            }
        }
        TypedSpec.expectChildren(kids, i, "TypedSerializeGraph");
        return new TypedSerializeGraph(kids.get(0), rowVar, ls, ns, arrayWrap,
                bareValue, classFqn, info, inlineChild, sps, oks, typeKeyName,
                fqTypePath, ccs, removeNullKeys, removeEmptySets,
                objectRefPrefix);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedSerializeGraph(source, rowVar, leaves, nested, arrayWrap, bareValue, classFqn, info, inlineChild, subTypePatches, orderKeys, typeKeyName, fqTypePath, checkedConstraints, removeNullKeys, removeEmptySets, objectRefPrefix);
    }
}
