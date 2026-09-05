package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.Property;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedGraphFetch;
import com.legend.compiler.spec.typed.TypedGraphTree;
import com.legend.compiler.spec.typed.TypedSerialize;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * {@code graphFetch(#{Class{…}}#)} and {@code serialize(#{…}#)} (engine
 * {@code GraphFetchChecker} / {@code SerializeChecker}). The parser encodes the
 * tree as a {@link ColSpecArray} (each node: {@code function1} = the generated
 * accessor lambda, {@code function2} wraps the nested sub-tree); this checker
 * validates every property against its owner class <em>recursively</em>
 * (nesting requires a class-typed property) and reifies the tree.
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
     * against it. (The tree's colspecs carry generated lambdas the generic
     * deferred path can't route &mdash; the signature's tree parameter is not a
     * {@code FuncColSpec} &mdash; so validation walks the class model directly.)
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
        if (!(second instanceof ColSpecArray tree)) {
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
        return new Checked(source, validate(t, ct.fqn(), tree, fn, env));
    }

    /** Validate one tree level against its owner class; recurse into class-typed sub-trees. */
    private static List<TypedGraphTree> validate(Typer t, String classFqn,
            ColSpecArray tree, String fn, Env env) {
        List<TypedGraphTree> out = new ArrayList<>(tree.colSpecs().size());
        for (ColSpec cs : tree.colSpecs()) {
            // ->subType(@Sub) { ... }: the SUBTYPE VIEW — children validate
            // against the subtype class, which must extend the owner
            if (cs.name().equals("->subType")) {
                String subFqn = cs.args().size() == 1
                        && cs.args().get(0)
                                instanceof com.legend.protocol.spec.TypeAnnotation.Named tn
                        && tn.type() instanceof com.legend.protocol.TypeExpression.NameRef nr
                        ? nr.name() : null;
                if (subFqn == null || t.model().findClass(subFqn).isEmpty()) {
                    throw new TypeInferenceException(fn + " tree: ->subType"
                            + " requires a known class, got '" + subFqn + "'");
                }
                if (!t.model().isSubtype(subFqn, classFqn)) {
                    throw new TypeInferenceException(fn + " tree: ->subType class '"
                            + subFqn + "' does not extend '" + classFqn + "'");
                }
                ColSpecArray subNested = nestedTree(cs);
                if (subNested == null) {
                    throw new TypeInferenceException(fn + " tree: ->subType"
                            + " requires a sub-tree of the subtype's properties");
                }
                out.add(new TypedGraphTree("->subType",
                        validate(t, subFqn, subNested, fn, env),
                        null, List.of(), false, subFqn));
                continue;
            }
            Property prop = t.model().findProperty(classFqn, cs.name()).orElse(null);
            String propName = cs.name();
            boolean sweep = false;
            // the SYNTHETIC milestoned sweep spelling: <base>AllVersions on
            // an end targeting a temporal class (real pure GENERATES it) —
            // the node resolves by the BASE property; the spelled name
            // becomes the envelope alias; the sweep serves the RAW extent
            if (prop == null && cs.name().endsWith("AllVersions")) {
                String base = cs.name().substring(0,
                        cs.name().length() - "AllVersions".length());
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
                                        cs.name());
                if (gen != null) {
                    ColSpecArray genNested = nestedTree(cs);
                    if (genNested == null) {
                        out.add(new TypedGraphTree(cs.name(), List.of(),
                                cs.alias(), List.of(), false));
                        continue;
                    }
                    if (!(gen.type() instanceof Type.ClassType gc)) {
                        throw new TypeInferenceException(fn
                                + " tree: generated member '" + cs.name()
                                + "' is not class-typed and cannot carry"
                                + " a sub-tree");
                    }
                    out.add(new TypedGraphTree(cs.name(),
                            validate(t, gc.fqn(), genNested, fn, env),
                            cs.alias(), List.of(), false));
                    continue;
                }
                throw new TypeInferenceException(fn + " tree: class " + classFqn
                        + " has no property '" + cs.name() + "'");
            }
            // qualifier CALL args type here and ride the tree (the
            // resolver inlines the derived body with them); non-derived
            // parenthesized args (milestoning dates) keep the historical
            // checker-drop — their feature owns its own threading
            List<TypedSpec> targs = List.of();
            if (!cs.args().isEmpty()) {
                // typed for the ENVELOPE KEY (the engine serializes the
                // source call spelling — firm(2022-10-20T23:59:59+0000))
                // and for derived-body binding; milestoning CONTEXT still
                // flows through the temporal frame, not these args
                List<TypedSpec> ta = new ArrayList<>(cs.args().size());
                for (var a : cs.args()) {
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
            String alias = cs.alias() != null ? cs.alias()
                    : (sweep ? cs.name() : null);
            ColSpecArray nested = nestedTree(cs);
            if (nested == null) {
                out.add(new TypedGraphTree(propName, List.of(), alias,
                        targs, sweep, null, cs.qualified()));
                continue;
            }
            if (!(prop.type() instanceof Type.ClassType nestedClass)) {
                throw new TypeInferenceException(fn + " tree: property '" + cs.name()
                        + "' is not class-typed and cannot carry a sub-tree");
            }
            out.add(new TypedGraphTree(propName,
                    validate(t, nestedClass.fqn(), nested, fn, env),
                    alias, targs, sweep, null, cs.qualified()));
        }
        return out;
    }

    /**
     * A tree built at runtime from SOURCE TEXT —
     * {@code compileLegendValueSpecification('#{...}#')->cast(@RootGraphFetchTree<T>)}
     * (the subType-family spelling) — unwraps to the PARSED tree literal:
     * the cast strips, the string-concat chain folds, and the platform
     * parser (the same island grammar) produces the ColSpecArray. Any
     * other shape (or a parse failure — e.g. the ->subType() paths the
     * grammar does not carry yet) returns the ORIGINAL node so the loud
     * arity message stands.
     */
    private static ValueSpecification unwrapCompiledTree(ValueSpecification v) {
        // the wire-facing literal carrier dissolves to its desugared tree on
        // first checker touch (same rule as PathLiteral in the resolver)
        if (v instanceof com.legend.protocol.spec.GraphFetchLiteral gf) {
            return gf.desugared();
        }
        if (v instanceof AppliedFunction c
                && (c.function().equals("cast")
                        || c.function().equals("meta::pure::functions::lang::cast"))
                && !c.parameters().isEmpty()) {
            ValueSpecification inner = unwrapCompiledTree(c.parameters().get(0));
            return inner instanceof ColSpecArray ? inner : v;
        }
        if (v instanceof com.legend.protocol.spec.QuotedTreeCall q) {
            // the parse-time quote/eval fold (SpecParser via QuotedSpecParser):
            // the carrier's pipeline face IS the parsed tree
            return q.tree();
        }
        return v;
    }


    /** The nested sub-tree a colspec's {@code function2} wraps, or {@code null} for a leaf. */
    private static @com.legend.Nullable ColSpecArray nestedTree(ColSpec cs) {
        if (cs.function2() == null || cs.function2().body().isEmpty()) {
            return null;
        }
        ValueSpecification body = cs.function2().body().get(0);
        return body instanceof ColSpecArray arr ? arr : null;
    }
}
