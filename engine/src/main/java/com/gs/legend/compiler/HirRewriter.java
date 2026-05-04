package com.gs.legend.compiler;

import com.gs.legend.compiler.typed.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Generic typed-HIR rewrite kernel: a capture-avoiding, identity-preserving
 * tree walker over {@link TypedSpec}.
 *
 * <p>This is the substitution-and-traversal machinery shared by every
 * abstraction-expansion pass — today {@link UserCallInliner}, in Phase 1b
 * also class-fetch inlining inside {@link MappingResolver}. The walker is
 * pure mechanical descent: it recurses through compound nodes, rebuilds
 * them when any child changes, and returns the input identity-unchanged
 * when no descendant rewrites.
 *
 * <p>Specialization is via {@link Hooks}. Hooks fire at the four
 * substitution-target node kinds —
 * {@link TypedVariable}, {@link TypedUserCall}, {@link TypedEval},
 * {@link TypedGetAll} — and at scope-introducing binders (lambda
 * parameters, {@link TypedLet}). Default hook implementations perform
 * ordinary descent without substitution; pass-specific subclasses
 * override what they care about.
 *
 * <p><strong>Capture-avoiding substitution</strong>: when entering a
 * binder that introduces a name shadowing an outer-scope binding (a
 * {@link TypedLet} in a {@link TypedBlock}, a {@link TypedLambda}
 * parameter), the kernel calls {@link Hooks#enterBinder} so the hook can
 * trim its scope. The standard impl in {@link Scope#trim} drops the
 * shadowed names from both the scalar and lambda binding maps.
 *
 * <p><strong>Tree identity</strong>: every helper returns the input list/
 * map/node when no descendant changed. Subtrees with no rewrites allocate
 * nothing.
 */
public final class HirRewriter {

    private final Hooks hooks;

    public HirRewriter(Hooks hooks) {
        this.hooks = hooks;
    }

    /** Substitution scope: param-name → bound actual. Immutable. */
    public record Scope(
            Map<String, TypedSpec> scalarBindings,
            Map<String, TypedLambda> lambdaBindings) {

        public static final Scope EMPTY = new Scope(Map.of(), Map.of());

        /** Return a Scope with the given names removed from both maps. */
        public Scope trim(List<String> names) {
            if (names.isEmpty()
                    || (scalarBindings.isEmpty() && lambdaBindings.isEmpty())) {
                return this;
            }
            boolean hits = false;
            for (String n : names) {
                if (scalarBindings.containsKey(n)
                        || lambdaBindings.containsKey(n)) {
                    hits = true;
                    break;
                }
            }
            if (!hits) return this;
            Map<String, TypedSpec> s = new LinkedHashMap<>(scalarBindings);
            Map<String, TypedLambda> l = new LinkedHashMap<>(lambdaBindings);
            for (String n : names) {
                s.remove(n);
                l.remove(n);
            }
            return new Scope(Map.copyOf(s), Map.copyOf(l));
        }
    }

    /**
     * Per-pass strategy. Each method's default impl is ordinary descent
     * with no substitution; passes override to inject behavior at the
     * substitution-target nodes.
     */
    public interface Hooks {

        /** Walk a {@link TypedVariable}. Default: identity. */
        default TypedSpec rewriteVariable(
                TypedVariable v, Scope scope, HirRewriter rec) {
            return v;
        }

        /**
         * Walk a {@link TypedUserCall}. Default: descend into args (no
         * inlining). Passes that inline override to splice the body.
         */
        default TypedSpec rewriteUserCall(
                TypedUserCall uc, Scope scope, HirRewriter rec) {
            List<TypedSpec> args = rec.rewriteList(uc.args(), scope);
            return args == uc.args() ? uc
                    : new TypedUserCall(uc.functionFqn(), args, uc.callee(), uc.info());
        }

        /**
         * Walk a {@link TypedEval}. Default: descend into args. Passes that
         * track lambda bindings override to β-reduce when the applicable
         * is bound.
         */
        default TypedSpec rewriteEval(
                TypedEval ev, Scope scope, HirRewriter rec) {
            List<TypedSpec> args = rec.rewriteList(ev.args(), scope);
            return args == ev.args() ? ev
                    : new TypedEval(ev.applicable(), args, ev.def(), ev.info());
        }

        /** Walk a {@link TypedGetAll}. Default: identity (leaf). */
        default TypedSpec rewriteGetAll(
                TypedGetAll ga, Scope scope, HirRewriter rec) {
            return ga;
        }

        /**
         * Hook invoked when entering a scope-introducing binder. Returns
         * the scope to use inside the binder's body. Default: trim shadowed
         * names from {@link Scope}.
         */
        default Scope enterBinder(List<String> boundNames, Scope scope) {
            return scope.trim(boundNames);
        }

        /**
         * Optional renaming of a binder's bound names. Returning a non-null
         * {@link RenamedBinder} causes the kernel to rebuild the binder's
         * declared names ({@link TypedLambda} parameters today) with the
         * supplied fresh names, and to descend into the binder's body
         * under the supplied inner scope. The hook is responsible for
         * threading any name→fresh-var substitution into that scope.
         *
         * <p>Default: {@code null} — no renaming, kernel uses
         * {@link #enterBinder} alone for capture-avoidance trimming.
         *
         * <p>Used by α-rename (see {@link HirRewriter#alphaRename}).
         */
        default RenamedBinder renameBinder(List<String> boundNames, Scope scope) {
            return null;
        }
    }

    /**
     * Result of {@link Hooks#renameBinder}: the fresh names to use in place
     * of the binder's declared names, and the scope to descend into the
     * body with. Order of {@code newNames} matches the order of the
     * binder's original names.
     */
    public record RenamedBinder(List<String> newNames, Scope innerScope) {}

    // ==================== Core rewrite ====================

    public TypedSpec rewrite(TypedSpec node, Scope scope) {
        if (node == null) return null;

        return switch (node) {
            // ---------- Substitution targets — delegate to hooks ----------
            case TypedVariable v -> hooks.rewriteVariable(v, scope, this);
            case TypedUserCall uc -> hooks.rewriteUserCall(uc, scope, this);
            case TypedEval ev -> hooks.rewriteEval(ev, scope, this);
            case TypedGetAll ga -> hooks.rewriteGetAll(ga, scope, this);

            // ---------- Compound nodes (rebuild iff any child changed) ----------
            case TypedFilter n -> {
                TypedSpec src = rewrite(n.source(), scope);
                TypedLambda pred = rewriteLambda(n.predicate(), scope);
                yield (src == n.source() && pred == n.predicate()) ? n
                        : new TypedFilter(src, pred, n.def(), n.info());
            }
            case TypedSort n -> {
                TypedSpec src = rewrite(n.source(), scope);
                List<TypedSortKey> keys = rewriteSortKeys(n.keys(), scope);
                yield (src == n.source() && keys == n.keys()) ? n
                        : new TypedSort(src, keys, n.def(), n.info());
            }
            case TypedSlice n -> {
                TypedSpec src = rewrite(n.source(), scope);
                yield src == n.source() ? n
                        : new TypedSlice(src, n.offset(), n.limit(), n.def(), n.info());
            }
            case TypedConcatenate n -> {
                TypedSpec l = rewrite(n.left(), scope);
                TypedSpec r = rewrite(n.right(), scope);
                yield (l == n.left() && r == n.right()) ? n
                        : new TypedConcatenate(l, r, n.def(), n.info());
            }
            case TypedDistinct n -> {
                TypedSpec src = rewrite(n.source(), scope);
                yield src == n.source() ? n
                        : new TypedDistinct(src, n.columns(), n.def(), n.info());
            }
            case TypedFlatten n -> {
                TypedSpec src = rewrite(n.source(), scope);
                yield src == n.source() ? n
                        : new TypedFlatten(src, n.column(), n.def(), n.info());
            }
            case TypedRename n -> {
                TypedSpec src = rewrite(n.source(), scope);
                yield src == n.source() ? n
                        : new TypedRename(src, n.renames(), n.def(), n.info());
            }
            case TypedSelect n -> {
                TypedSpec src = rewrite(n.source(), scope);
                yield src == n.source() ? n
                        : new TypedSelect(src, n.cols(), n.def(), n.info());
            }
            case TypedFold n -> {
                TypedSpec src = rewrite(n.source(), scope);
                TypedLambda red = rewriteLambda(n.reducer(), scope);
                TypedSpec init = rewrite(n.init(), scope);
                yield (src == n.source() && red == n.reducer() && init == n.init()) ? n
                        : new TypedFold(src, red, init, n.strategy(), n.def(), n.info());
            }
            case TypedMap n -> {
                TypedSpec src = rewrite(n.source(), scope);
                TypedLambda map = rewriteLambda(n.mapper(), scope);
                yield (src == n.source() && map == n.mapper()) ? n
                        : new TypedMap(src, map, n.def(), n.info());
            }
            case TypedZip n -> {
                List<TypedSpec> srcs = rewriteList(n.sources(), scope);
                yield srcs == n.sources() ? n
                        : new TypedZip(srcs, n.byKeys(), n.def(), n.info());
            }
            case TypedExtend n -> {
                TypedSpec src = rewrite(n.source(), scope);
                List<TypedExtendCol> exts = rewriteExtendCols(n.extensions(), scope);
                yield (src == n.source() && exts == n.extensions()) ? n
                        : new TypedExtend(src, n.traversalSpecs(), exts, n.def(), n.info());
            }
            case TypedProject n -> {
                TypedSpec src = rewrite(n.source(), scope);
                List<TypedProjectionCol> cols = rewriteProjectionCols(n.projections(), scope);
                yield (src == n.source() && cols == n.projections()) ? n
                        : new TypedProject(src, cols, n.def(), n.info());
            }
            case TypedJoin n -> {
                TypedSpec l = rewrite(n.left(), scope);
                TypedSpec r = rewrite(n.right(), scope);
                TypedLambda cond = rewriteLambda(n.condition(), scope);
                yield (l == n.left() && r == n.right() && cond == n.condition()) ? n
                        : new TypedJoin(l, r, cond, n.joinType(), n.renames(), n.def(), n.info());
            }
            case TypedAsOfJoin n -> {
                TypedSpec l = rewrite(n.left(), scope);
                TypedSpec r = rewrite(n.right(), scope);
                TypedLambda match = rewriteLambda(n.matchCondition(), scope);
                Optional<TypedLambda> key = n.keyCondition().map(k -> rewriteLambda(k, scope));
                yield (l == n.left() && r == n.right() && match == n.matchCondition()
                        && key.equals(n.keyCondition())) ? n
                        : new TypedAsOfJoin(l, r, match, key, n.renames(), n.def(), n.info());
            }
            case TypedGroupBy n -> {
                TypedSpec src = rewrite(n.source(), scope);
                List<TypedGroupKey> keys = rewriteGroupKeys(n.keys(), scope);
                List<TypedAggCall> aggs = rewriteAggCalls(n.aggs(), scope);
                yield (src == n.source() && keys == n.keys() && aggs == n.aggs()) ? n
                        : new TypedGroupBy(src, keys, aggs, n.def(), n.info());
            }
            case TypedAggregate n -> {
                TypedSpec src = rewrite(n.source(), scope);
                List<TypedAggCall> aggs = rewriteAggCalls(n.aggs(), scope);
                yield (src == n.source() && aggs == n.aggs()) ? n
                        : new TypedAggregate(src, aggs, n.def(), n.info());
            }
            case TypedPivot n -> {
                TypedSpec src = rewrite(n.source(), scope);
                List<TypedAggCall> aggs = rewriteAggCalls(n.aggs(), scope);
                yield (src == n.source() && aggs == n.aggs()) ? n
                        : new TypedPivot(src, n.pivotColumns(), aggs, n.def(), n.info());
            }
            case TypedFrom n -> {
                TypedSpec src = rewrite(n.source(), scope);
                yield src == n.source() ? n
                        : new TypedFrom(src, n.mapping(), n.runtime(), n.def(), n.info());
            }
            case TypedSerialize n -> {
                TypedSpec src = rewrite(n.source(), scope);
                yield src == n.source() ? n
                        : new TypedSerialize(src, n.format(), n.children(), n.def(), n.info());
            }
            case TypedWrite n -> {
                TypedSpec src = rewrite(n.source(), scope);
                TypedSpec dst = rewrite(n.destination(), scope);
                yield (src == n.source() && dst == n.destination()) ? n
                        : new TypedWrite(src, dst, n.def(), n.info());
            }
            case TypedGraphFetch n -> {
                TypedSpec src = rewrite(n.source(), scope);
                yield src == n.source() ? n
                        : new TypedGraphFetch(src, n.children(), n.def(), n.info());
            }
            case TypedNativeCall n -> {
                List<TypedSpec> args = rewriteList(n.args(), scope);
                yield args == n.args() ? n
                        : new TypedNativeCall(n.func(), args, n.info());
            }
            case TypedPropertyAccess n -> {
                TypedSpec src = rewrite(n.source(), scope);
                yield src == n.source() ? n
                        : new TypedPropertyAccess(src, n.property(), n.associationPath(),
                                n.physicalColumn(), n.info());
            }
            case TypedStructExtract n -> {
                TypedSpec src = rewrite(n.source(), scope);
                yield src == n.source() ? n
                        : new TypedStructExtract(src, n.field(), n.info());
            }
            case TypedNewInstance n -> {
                Map<String, TypedSpec> values = rewriteMap(n.values(), scope);
                yield values == n.values() ? n
                        : new TypedNewInstance(n.className(), values, n.info());
            }
            case TypedIf n -> {
                TypedSpec c = rewrite(n.condition(), scope);
                TypedSpec t = rewrite(n.thenBranch(), scope);
                TypedSpec e = rewrite(n.elseBranch(), scope);
                yield (c == n.condition() && t == n.thenBranch() && e == n.elseBranch()) ? n
                        : new TypedIf(c, t, e, n.info());
            }
            case TypedLet n -> {
                TypedSpec v = rewrite(n.value(), scope);
                yield v == n.value() ? n : new TypedLet(n.name(), v, n.info());
            }
            case TypedBlock n -> rewriteBlock(n, scope);
            case TypedMatch n -> {
                TypedSpec subj = rewrite(n.subject(), scope);
                List<TypedLambda> cases = rewriteLambdaList(n.cases(), scope);
                yield (subj == n.subject() && cases == n.cases()) ? n
                        : new TypedMatch(subj, cases, n.info());
            }
            case TypedCast n -> {
                TypedSpec e = rewrite(n.expr(), scope);
                yield e == n.expr() ? n : new TypedCast(e, n.targetType(), n.info());
            }
            case TypedLambda n -> rewriteLambda(n, scope);
            case TypedCollection n -> {
                List<TypedSpec> vs = rewriteList(n.values(), scope);
                yield vs == n.values() ? n : new TypedCollection(vs, n.info());
            }
            case TypedSerializeImplicit n -> {
                TypedSpec src = rewrite(n.source(), scope);
                yield src == n.source() ? n
                        : new TypedSerializeImplicit(src, n.children());
            }

            // ---------- Leaves: no children, no rewrite ----------
            case TypedTableReference n -> n;
            case TypedTdsLiteral n -> n;
            case TypedSourceUrl n -> n;
            case TypedPackageableRef n -> n;
            case TypedCInteger n -> n;
            case TypedCFloat n -> n;
            case TypedCDecimal n -> n;
            case TypedCString n -> n;
            case TypedCBoolean n -> n;
            case TypedCDateTime n -> n;
            case TypedCStrictDate n -> n;
            case TypedCStrictTime n -> n;
            case TypedCLatestDate n -> n;
            case TypedCByteArray n -> n;
            case TypedEnumValue n -> n;
        };
    }

    // ==================== Binder-aware helpers ====================

    /**
     * Rewrite a lambda. The lambda's parameters are bound names that shadow
     * any outer scope entries; the kernel calls {@link Hooks#enterBinder}
     * (or {@link Hooks#renameBinder} when α-renaming) to let the strategy
     * adjust scope, then walks the body.
     */
    public TypedLambda rewriteLambda(TypedLambda lam, Scope scope) {
        if (lam == null) return null;
        List<String> oldNames = lam.parameters().stream()
                .map(TypedParam::name).toList();

        // Optional α-rename: hook may return fresh names + a pre-built
        // inner scope that maps oldName → freshVarRef so body recursion
        // rewrites references via rewriteVariable.
        RenamedBinder renamed = hooks.renameBinder(oldNames, scope);
        Scope inner = renamed != null ? renamed.innerScope()
                : hooks.enterBinder(oldNames, scope);

        List<TypedParam> newParams = lam.parameters();
        if (renamed != null) {
            List<String> newNames = renamed.newNames();
            if (newNames.size() != oldNames.size()) {
                throw new IllegalStateException(
                        "[hir-rewriter] renameBinder returned " + newNames.size()
                                + " names for " + oldNames.size() + " params");
            }
            List<TypedParam> rebuilt = new ArrayList<>(oldNames.size());
            for (int i = 0; i < oldNames.size(); i++) {
                TypedParam p = lam.parameters().get(i);
                rebuilt.add(new TypedParam(newNames.get(i), p.type(), p.multiplicity()));
            }
            newParams = rebuilt;
        }

        var body = lam.body();
        List<TypedSpec> rewritten = null;
        for (int i = 0; i < body.size(); i++) {
            TypedSpec s = rewrite(body.get(i), inner);
            if (s != body.get(i) && rewritten == null) {
                rewritten = new ArrayList<>(body);
            }
            if (rewritten != null) rewritten.set(i, s);
        }
        if (rewritten == null && newParams == lam.parameters()) return lam;
        return new TypedLambda(newParams,
                rewritten != null ? rewritten : body, lam.info());
    }

    /**
     * Rewrite a block, threading let-introduced names through the scope so
     * later statements see the shadowing.
     */
    private TypedSpec rewriteBlock(TypedBlock n, Scope scope) {
        List<TypedSpec> stmts = n.stmts();
        List<TypedSpec> out = null;
        Scope cur = scope;
        for (int i = 0; i < stmts.size(); i++) {
            TypedSpec s = stmts.get(i);
            TypedSpec rewritten = rewrite(s, cur);
            if (rewritten != s && out == null) out = new ArrayList<>(stmts);
            if (out != null) out.set(i, rewritten);
            if (s instanceof TypedLet let) {
                cur = hooks.enterBinder(List.of(let.name()), cur);
            }
        }
        return out == null ? n : new TypedBlock(out, n.info());
    }

    // ==================== List / map helpers ====================

    public List<TypedSpec> rewriteList(List<TypedSpec> in, Scope scope) {
        List<TypedSpec> out = null;
        for (int i = 0; i < in.size(); i++) {
            TypedSpec s = rewrite(in.get(i), scope);
            if (s != in.get(i) && out == null) out = new ArrayList<>(in);
            if (out != null) out.set(i, s);
        }
        return out == null ? in : out;
    }

    public List<TypedLambda> rewriteLambdaList(List<TypedLambda> in, Scope scope) {
        List<TypedLambda> out = null;
        for (int i = 0; i < in.size(); i++) {
            TypedLambda lam = rewriteLambda(in.get(i), scope);
            if (lam != in.get(i) && out == null) out = new ArrayList<>(in);
            if (out != null) out.set(i, lam);
        }
        return out == null ? in : out;
    }

    public Map<String, TypedSpec> rewriteMap(Map<String, TypedSpec> in, Scope scope) {
        Map<String, TypedSpec> out = null;
        for (var e : in.entrySet()) {
            TypedSpec rewritten = rewrite(e.getValue(), scope);
            if (rewritten != e.getValue() && out == null) {
                out = new LinkedHashMap<>(in);
            }
            if (out != null) out.put(e.getKey(), rewritten);
        }
        return out == null ? in : out;
    }

    public List<TypedSortKey> rewriteSortKeys(List<TypedSortKey> in, Scope scope) {
        List<TypedSortKey> out = null;
        for (int i = 0; i < in.size(); i++) {
            TypedSortKey k = in.get(i);
            TypedSortKey nk = switch (k) {
                case TypedColumnSortKey c -> c;
                case TypedExpressionSortKey e -> {
                    TypedLambda lam = rewriteLambda(e.keyFn(), scope);
                    yield lam == e.keyFn() ? e
                            : new TypedExpressionSortKey(lam, e.direction());
                }
            };
            if (nk != k && out == null) out = new ArrayList<>(in);
            if (out != null) out.set(i, nk);
        }
        return out == null ? in : out;
    }

    public List<TypedGroupKey> rewriteGroupKeys(List<TypedGroupKey> in, Scope scope) {
        List<TypedGroupKey> out = null;
        for (int i = 0; i < in.size(); i++) {
            TypedGroupKey k = in.get(i);
            TypedGroupKey nk = switch (k) {
                case TypedColumnGroupKey c -> c;
                case TypedAssociationGroupKey a -> a;
                case TypedExpressionGroupKey e -> {
                    TypedLambda lam = rewriteLambda(e.keyFn(), scope);
                    yield lam == e.keyFn() ? e
                            : new TypedExpressionGroupKey(lam, e.alias());
                }
            };
            if (nk != k && out == null) out = new ArrayList<>(in);
            if (out != null) out.set(i, nk);
        }
        return out == null ? in : out;
    }

    public List<TypedAggCall> rewriteAggCalls(List<TypedAggCall> in, Scope scope) {
        List<TypedAggCall> out = null;
        for (int i = 0; i < in.size(); i++) {
            TypedAggCall a = in.get(i);
            TypedLambda fn1 = rewriteLambda(a.fn1(), scope);
            TypedLambda fn2 = rewriteLambda(a.fn2(), scope);
            List<TypedSpec> extra = rewriteList(a.extraArgs(), scope);
            boolean changed = fn1 != a.fn1() || fn2 != a.fn2() || extra != a.extraArgs();
            if (changed) {
                if (out == null) out = new ArrayList<>(in);
                out.set(i, new TypedAggCall(a.alias(), a.func(), fn1, fn2,
                        extra, a.returnType(), a.castType()));
            }
        }
        return out == null ? in : out;
    }

    public List<TypedProjectionCol> rewriteProjectionCols(
            List<TypedProjectionCol> in, Scope scope) {
        List<TypedProjectionCol> out = null;
        for (int i = 0; i < in.size(); i++) {
            TypedProjectionCol c = in.get(i);
            TypedLambda lam = rewriteLambda(c.expression(), scope);
            if (lam != c.expression()) {
                if (out == null) out = new ArrayList<>(in);
                out.set(i, new TypedProjectionCol(c.alias(), lam, c.associationPath()));
            }
        }
        return out == null ? in : out;
    }

    public List<TypedExtendCol> rewriteExtendCols(
            List<TypedExtendCol> in, Scope scope) {
        List<TypedExtendCol> out = null;
        for (int i = 0; i < in.size(); i++) {
            TypedExtendCol c = in.get(i);
            TypedExtendCol nc = switch (c) {
                case TypedScalarExtendCol s -> {
                    TypedLambda lam = rewriteLambda(s.expression(), scope);
                    yield lam == s.expression() ? s
                            : new TypedScalarExtendCol(s.alias(), lam, s.returnType());
                }
                case TypedTraverseExtendCol t -> {
                    TypedLambda lam = rewriteLambda(t.expression(), scope);
                    yield lam == t.expression() ? t
                            : new TypedTraverseExtendCol(t.alias(), t.hops(), lam);
                }
                case TypedWindowExtendCol w -> {
                    List<TypedSpec> args = rewriteList(w.funcArgs(), scope);
                    Optional<TypedLambda> reducer = w.reducer().map(l -> rewriteLambda(l, scope));
                    Optional<TypedWindowExtendCol.OuterWrapper> outer = w.outerWrapper().map(ow -> {
                        TypedSpec rew = rewrite(ow.expr(), scope);
                        return rew == ow.expr() ? ow
                                : new TypedWindowExtendCol.OuterWrapper(rew, ow.holeName());
                    });
                    boolean changed = args != w.funcArgs()
                            || !reducer.equals(w.reducer())
                            || !outer.equals(w.outerWrapper());
                    yield changed
                            ? new TypedWindowExtendCol(w.alias(), w.func(), w.rowParamName(),
                                    args, reducer, outer, w.over(), w.returnType(), w.castType())
                            : w;
                }
                case TypedAssociationExtendCol a -> a;
                case TypedEmbeddedExtendCol e -> e;
            };
            if (nc != c) {
                if (out == null) out = new ArrayList<>(in);
                out.set(i, nc);
            }
        }
        return out == null ? in : out;
    }

    // ==================== α-rename utility ====================

    /**
     * Capture-avoiding deep clone with bound-variable renaming. Walks
     * {@code body} and:
     * <ul>
     *   <li>For every {@link TypedLambda}, rebuilds its parameters with
     *       fresh names obtained from {@code freshNamer}, and rewrites
     *       all references to those parameters inside the body to the
     *       fresh names. The rename is local to each lambda's scope.</li>
     *   <li>Free variables (not bound by any enclosing lambda within
     *       {@code body}) pass through unchanged.</li>
     * </ul>
     *
     * <p>The result is a fresh subtree where every bound variable name is
     * unique and disjoint from any name in the surrounding context. This
     * is the standard α-conversion operation and is the safety net for
     * splicing memoized templates (e.g., class-fetch synth bodies) into
     * different parts of a tree without risk of variable capture.
     *
     * <p>Tree identity: subtrees containing no lambdas are returned
     * unchanged by reference. Lambdas with no parameters are also returned
     * unchanged. Renaming only allocates when a lambda actually has
     * parameters and at least one reference to one of them.
     *
     * <p><strong>Scope</strong>: this implementation renames
     * {@link TypedLambda} parameters only. {@link TypedLet} introduces a
     * name as well, but synthetic class-fetch bodies (the primary client
     * today) do not use {@code let} bindings; lets pass through unchanged
     * with their original names.
     *
     * @param body       the subtree to rename
     * @param freshNamer maps an old bound name to a fresh, globally-unique
     *                   name. Typical impl: {@code old -> old + "$" + ctr.getAndIncrement()}
     *                   with an {@code AtomicLong} counter shared across
     *                   all alpha-rename calls in the same compilation.
     */
    public static TypedSpec alphaRename(
            TypedSpec body,
            java.util.function.UnaryOperator<String> freshNamer) {
        return new HirRewriter(new AlphaRenameHooks(freshNamer))
                .rewrite(body, Scope.EMPTY);
    }

    /**
     * Hooks impl for {@link #alphaRename}. Variable references look up
     * the rename map (carried in {@link Scope#scalarBindings} as
     * {@code TypedVariable} stubs whose name carries the fresh name);
     * if found, the variable is rewritten to a fresh {@link TypedVariable}
     * preserving the original's role and info. Lambda binders generate
     * fresh names and seed the rename map for body recursion.
     */
    private static final class AlphaRenameHooks implements Hooks {
        private final java.util.function.UnaryOperator<String> freshNamer;

        AlphaRenameHooks(java.util.function.UnaryOperator<String> freshNamer) {
            this.freshNamer = freshNamer;
        }

        @Override
        public TypedSpec rewriteVariable(
                TypedVariable v, Scope scope, HirRewriter rec) {
            TypedSpec mapped = scope.scalarBindings().get(v.name());
            if (mapped instanceof TypedVariable stub) {
                // Preserve original v's role + info; only the name changes.
                return new TypedVariable(stub.name(), v.role(), v.info());
            }
            return v;
        }

        @Override
        public RenamedBinder renameBinder(List<String> boundNames, Scope scope) {
            if (boundNames.isEmpty()) return null;
            List<String> newNames = new ArrayList<>(boundNames.size());
            Map<String, TypedSpec> bindings =
                    new LinkedHashMap<>(scope.scalarBindings());
            for (String oldName : boundNames) {
                String newName = freshNamer.apply(oldName);
                newNames.add(newName);
                // The stub's role/info are unread at variable-rewrite time;
                // rewriteVariable above takes role + info from the actual
                // variable reference being rewritten, not from this stub.
                bindings.put(oldName, new TypedVariable(
                        newName, com.gs.legend.compiler.typed.Role.LAMBDA_PARAM, null));
            }
            return new RenamedBinder(newNames,
                    new Scope(Map.copyOf(bindings), scope.lambdaBindings()));
        }
    }
}
