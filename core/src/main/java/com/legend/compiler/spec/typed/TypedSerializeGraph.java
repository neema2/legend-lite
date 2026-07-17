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
                                  String classFqn,
                                  ExprType info) implements TypedSpec {

    public TypedSerializeGraph {
        leaves = List.copyOf(leaves);
        nested = List.copyOf(nested);
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
        return out;
    }
}
