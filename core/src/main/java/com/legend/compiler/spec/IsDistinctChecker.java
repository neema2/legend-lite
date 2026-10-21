package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.GraphFetchLiteral;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;
import java.util.ArrayList;
import java.util.List;

/**
 * Pure's {@code isDistinct}. The 1-arg collection form types generically
 * (its lowering is the group reducer COUNT(DISTINCT x) = COUNT(x)). The
 * 2-arg BY-TREE form {@code isDistinct(collection, #{T{a, b}}#)} — "no
 * duplicates comparing the elements by the tree's leaf properties" (engine
 * collectionExtension.pure declares it; its Pure body is
 * {@code fail('Not implemented!')} and only the generated-Java plan binding
 * implements it, IsDistinctFetchTreeCoder: an equality method over the
 * tree) — desugars to the 1-arg form over the row of those leaves:
 * {@code collection->map(e | tuple([$e.a, $e.b]))->isDistinct()}, with
 * {@code tuple} the internal struct value ({@link
 * com.legend.builtin.Pure.Lite#TUPLE}). A nested sub-tree is not a leaf and
 * walls loudly (the corpus spells leaves only: modelWithConstraints.pure
 * duplicateEmployee).
 */
final class IsDistinctChecker {

    private IsDistinctChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        if (af.parameters().size() != 2) {
            return t.applyGeneric(af, env);
        }
        ValueSpecification bound = env.resolveAlias(af.parameters().get(1));
        ValueSpecification tree = GraphFetchChecker.unwrapCompiledTree(bound);
        if (!(tree instanceof GraphFetchLiteral leaves)) {
            return t.applyGeneric(af, env);
        }
        if (!leaves.subTypeTrees().isEmpty()) {
            throw new com.legend.error.NotImplementedException(
                    "isDistinct by a tree with a ->subType view — leaves only");
        }
        Variable e = new Variable("e");
        List<ValueSpecification> reads = new ArrayList<>();
        for (GraphFetchLiteral.Node n : leaves.subTrees()) {
            if (!n.subTrees().isEmpty() || n.subType() != null || !n.subTypeTrees().isEmpty()) {
                throw new com.legend.error.NotImplementedException(
                        "isDistinct by a tree with a nested sub-tree (" + n.property()
                                + " {…}) — leaves only");
            }
            reads.add(new AppliedProperty(e, n.property()));
        }
        if (reads.size() < 2 || reads.size() > 6) {
            throw new com.legend.error.NotImplementedException(
                    "isDistinct by a tree of " + reads.size() + " leaves — 2 to 6 supported"
                            + " (one leaf: isDistinct over the property itself)");
        }
        // one tuple ARGUMENT per leaf (explicit arities): a collection literal
        // of mixed leaf types would lower as a text-cell list, not a row
        AppliedFunction mapped = Calls.platform("map", List.of(
                af.parameters().get(0),
                new LambdaFunction(List.of(e), List.of(Calls.exact(com.legend.builtin.Pure.Lite.TUPLE, reads)))));
        return t.synth(Calls.platform("isDistinct", List.of(mapped)), env);
    }
}
