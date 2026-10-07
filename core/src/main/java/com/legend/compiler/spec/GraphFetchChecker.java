package com.legend.compiler.spec;


import com.legend.platform.CoreFn;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.Property;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedGraphFetch;
import com.legend.compiler.spec.typed.TypedGraphTree;
import com.legend.compiler.spec.typed.TypedSerialize;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.GraphFetchLiteral;
import com.legend.protocol.spec.GraphFetchLiteral.Node;
import com.legend.protocol.spec.GraphFetchLiteral.SubTypeNode;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * {@code graphFetch(#{Class{…}}#)} and {@code serialize(#{…}#)} (engine
 * {@code GraphFetchChecker} / {@code SerializeChecker}). The tree is the
 * {@link GraphFetchLiteral} text and protocol JSON both read into; this checker
 * validates every property against its owner class <em>recursively</em>
 * (nesting requires a class-typed property) and reifies the typed tree.
 * {@code graphFetch} is a projection &mdash; the result is the SOURCE type
 * unchanged; {@code serialize} returns {@code String[1]} from its signature.
 */
final class GraphFetchChecker {

    private GraphFetchChecker() {
    }

    static TypedSpec graphFetch(Typer t, AppliedFunction af, Env env) {
        Checked c = checkTree(t, af, env, "graphFetch");
        return new TypedGraphFetch(c.source(), c.tree(), c.source().info());
    }

    /** {@code graphFetchChecked} &mdash; same tree validation; the result is
     * the registered signature's {@code Checked[*]} and the node carries
     * the CHECKED flag (the resolver's envelope adds per-row constraint
     * defects). */
    static TypedSpec graphFetchChecked(Typer t, AppliedFunction af, Env env) {
        Checked c = checkTree(t, af, env, "graphFetchChecked");
        var sigs = t.model().findFunction(
                CoreFn.GRAPH_FETCH_CHECKED.parseName());
        int arity = af.parameters().size();
        var sig = sigs.stream().filter(f -> f.parameters().size() == arity)
                .findFirst().orElseThrow(() -> new TypeInferenceException(
                        "no registered 'graphFetchChecked' overload accepts "
                        + arity + " argument(s)"));
        return new TypedGraphFetch(c.source(), c.tree(),
                new ExprType(sig.returnType(), sig.returnMultiplicity()),
                true);
    }

    static TypedSpec serialize(Typer t, AppliedFunction af, Env env) {
        Checked c = checkTree(t, af, env, "serialize");
        Optional<TypedSpec> config = af.parameters().size() > 2
                ? Optional.of(t.synth(af.parameters().get(2), env)) : Optional.empty();
        // String[1] — from the registered signature's return, never hardcoded.
        // ARITY-resolved (two serialize overloads are registered) via CoreFn,
        // not a magic string + blind get(0) (audit finding).
        var sigs = t.model().findFunction(CoreFn.SERIALIZE.parseName());
        int arity = af.parameters().size();
        var sig = sigs.stream().filter(f -> f.parameters().size() == arity).findFirst()
                .orElseThrow(() -> new TypeInferenceException(
                        "no registered 'serialize' overload accepts " + arity + " argument(s)"));
        return new TypedSerialize(c.source(), c.tree(), config,
                new ExprType(sig.returnType(), sig.returnMultiplicity()));
    }

    private record Checked(TypedSpec source, List<TypedGraphTree> tree) {
    }

    /**
     * The shared half: a class-collection source + a property tree validated
     * against it. (The signature's tree parameter is a RootGraphFetchTree, not
     * a column spec the generic deferred path routes, so validation walks the
     * class model directly.)
     */
    private static Checked checkTree(Typer t, AppliedFunction af, Env env, String fn) {
        // bind-once (family A): a let-bound tree parked by the statement
        // folds resolves through the alias channel — each use site gets
        // its own independent resolution (engine parallel: per-use
        // copyGenericType / use-site inScopeVars).
        ValueSpecification rawTree = af.parameters().size() < 2 ? null
                : af.parameters().get(1);
        ValueSpecification bound = rawTree == null ? null : env.resolveAlias(rawTree);
        ValueSpecification second = bound == null ? null : unwrapCompiledTree(bound);
        // a LET-BOUND tree literal CLOSES over the lets in scope (real pure
        // evaluates the literal at its let — the engine prints
        // `biTemporalClassification(2017-06-10, 2017-06-11)` for a tree
        // bound outside the query lambda); a tree spelled INSIDE the lambda
        // keeps its variable spellings (`classification($bd)` — the plan's
        // open variables). Batch 72b.
        if (rawTree instanceof com.legend.protocol.spec.Variable && bound != rawTree
                && second != null) {
            second = SourceSubst.substitute(second, env.aliases());
        }
        if (!(second instanceof GraphFetchLiteral tree)) {
            throw new TypeInferenceException(fn + " expects (classCollection, #{Class{…}}#)");
        }
        TypedSpec source = t.synth(af.parameters().get(0), env);
        // serialize over a CHECKED projection: the tree validates against
        // the FETCHED class (the Checked carrier is envelope-only)
        Type srcType = source instanceof TypedGraphFetch gf && gf.checked()
                ? gf.source().info().type() : source.info().type();
        if (!(srcType instanceof Type.ClassType ct)) {
            throw new TypeInferenceException(fn + " requires a class-typed source, got "
                    + srcType.typeName());
        }
        return new Checked(source, validate(t, ct.fqn(), tree.subTrees(), tree.subTypeTrees(), fn, env));
    }

    /** Validate one tree level against its owner class: its property nodes, then its subtype views. */
    private static List<TypedGraphTree> validate(Typer t, String classFqn, List<Node> nodes,
            List<SubTypeNode> subTypes, String fn, Env env) {
        List<TypedGraphTree> out = new ArrayList<>(nodes.size() + subTypes.size());
        for (Node n : nodes) {
            out.add(property(t, classFqn, n, fn, env));
        }
        for (SubTypeNode st : subTypes) {
            out.add(subTypeView(t, classFqn, st.subTypeClass(), st.subTrees(), fn, env));
        }
        return out;
    }

    /**
     * {@code ->subType(@Sub) { ... }}: the SUBTYPE VIEW — children validate
     * against the subtype class, which must extend the owner.
     */
    private static TypedGraphTree subTypeView(Typer t, String ownerFqn, String subFqn, List<Node> nodes,
            String fn, Env env) {
        if (t.model().findClass(subFqn).isEmpty()) {
            throw new TypeInferenceException(fn + " tree: ->subType"
                    + " requires a known class, got '" + subFqn + "'");
        }
        if (!t.model().isSubtype(subFqn, ownerFqn)) {
            throw new TypeInferenceException(fn + " tree: ->subType class '"
                    + subFqn + "' does not extend '" + ownerFqn + "'");
        }
        return new TypedGraphTree("->subType", validate(t, subFqn, nodes, List.of(), fn, env),
                null, List.of(), false, subFqn);
    }

    /** Whether a node carries a sub-tree: children, or a subtype view. */
    private static boolean hasSubTree(Node n) {
        return !n.subTrees().isEmpty() || n.subType() != null;
    }

    /** A node's sub-tree against {@code classFqn}; {@code prop->subType(@Sub) {...}} is the one subtype view. */
    private static List<TypedGraphTree> subTree(Typer t, String classFqn, Node n, String fn, Env env) {
        return n.subType() == null
                ? validate(t, classFqn, n.subTrees(), List.of(), fn, env)
                : List.of(subTypeView(t, classFqn, n.subType(), n.subTrees(), fn, env));
    }

    private static TypedGraphTree property(Typer t, String classFqn, Node n, String fn, Env env) {
        Property prop = t.model().findProperty(classFqn, n.property()).orElse(null);
        String propName = n.property();
        boolean sweep = false;
        // the SYNTHETIC milestoned sweep spelling: <base>AllVersions on
        // an end targeting a temporal class (real pure GENERATES it) —
        // the node resolves by the BASE property; the spelled name
        // becomes the envelope alias; the sweep serves the RAW extent
        if (prop == null && n.property().endsWith("AllVersions")) {
            String base = n.property().substring(0,
                    n.property().length() - "AllVersions".length());
            Property bp = t.model().findProperty(classFqn, base).orElse(null);
            if (bp != null && bp.type() instanceof Type.ClassType btc
                    && com.legend.compiler.element.Temporal
                            .strategyOf(t.model(), btc.fqn()) != null) {
                prop = bp;
                propName = base;
                sweep = true;
            }
        }
        if (prop == null) {
            // GENERATED milestoning members (businessDate/
            // processingDate/milestoning struct) serve graph trees
            // from the SAME registry as query-position typing
            com.legend.compiler.element.type.ExprType gen =
                    com.legend.compiler.element.Temporal
                            .generatedMember(t.model(), classFqn,
                                    n.property());
            if (gen != null) {
                if (!hasSubTree(n)) {
                    return new TypedGraphTree(n.property(), List.of(),
                            n.alias(), List.of(), false);
                }
                if (!(gen.type() instanceof Type.ClassType gc)) {
                    throw new TypeInferenceException(fn
                            + " tree: generated member '" + n.property()
                            + "' is not class-typed and cannot carry"
                            + " a sub-tree");
                }
                return new TypedGraphTree(n.property(), subTree(t, gc.fqn(), n, fn, env),
                        n.alias(), List.of(), false);
            }
            throw new TypeInferenceException(fn + " tree: class " + classFqn
                    + " has no property '" + n.property() + "'");
        }
        // qualifier CALL args type here and ride the tree (the
        // resolver inlines the derived body with them); non-derived
        // parenthesized args (milestoning dates) keep the historical
        // checker-drop — their feature owns its own threading
        List<TypedSpec> targs = List.of();
        if (!n.parameters().isEmpty()) {
            // typed for the ENVELOPE KEY (the engine serializes the
            // source call spelling — firm(2022-10-20T23:59:59+0000))
            // and for derived-body binding; milestoning CONTEXT still
            // flows through the temporal frame, not these args
            List<TypedSpec> ta = new ArrayList<>(n.parameters().size());
            for (var a : n.parameters()) {
                TypedSpec syn = t.synth(a, env);
                // a VARIABLE arg keeps its SOURCE spelling even when
                // let-inlining resolved its value — the engine key is
                // "customer($processingDate, $businessDate)" verbatim
                ta.add(a instanceof com.legend.protocol.spec.Variable v
                        && !(syn instanceof com.legend.compiler.spec
                                .typed.TypedVariable)
                        ? new com.legend.compiler.spec.typed
                                .TypedVariable(v.name(), syn.info())
                        : syn);
            }
            targs = ta;
        }
        String alias = n.alias() != null ? n.alias()
                : (sweep ? n.property() : null);
        if (!hasSubTree(n)) {
            return new TypedGraphTree(propName, List.of(), alias,
                    targs, sweep, null, n.qualified());
        }
        if (!(prop.type() instanceof Type.ClassType nestedClass)) {
            throw new TypeInferenceException(fn + " tree: property '" + n.property()
                    + "' is not class-typed and cannot carry a sub-tree");
        }
        return new TypedGraphTree(propName, subTree(t, nestedClass.fqn(), n, fn, env),
                alias, targs, sweep, null, n.qualified());
    }

    /**
     * The tree an argument denotes: the literal itself, or one built at runtime
     * from SOURCE TEXT —
     * {@code compileLegendValueSpecification('#{...}#')->cast(@RootGraphFetchTree<T>)}
     * (the subType-family spelling) — unwrapped to the tree the parser folded
     * ({@code QuotedTreeCall}): the cast strips. Any other shape returns the
     * ORIGINAL node so the loud arity message stands.
     */
    static ValueSpecification unwrapCompiledTree(ValueSpecification v) {
        if (v instanceof AppliedFunction c
                && com.legend.compiler.ResolvedNames.form(c).orElse(null) == CoreFn.CAST
                && !c.parameters().isEmpty()) {
            ValueSpecification inner = unwrapCompiledTree(c.parameters().get(0));
            return inner instanceof GraphFetchLiteral ? inner : v;
        }
        if (v instanceof com.legend.protocol.spec.QuotedTreeCall q) {
            // the parse-time quote/eval fold (SpecParser via QuotedSpecParser):
            // the carrier's pipeline face IS the parsed tree
            return q.tree();
        }
        return v;
    }
}
