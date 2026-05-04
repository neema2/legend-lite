package com.gs.legend.compiler;

import com.gs.legend.compiler.typed.*;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Phase 2 of the MappingResolver rewrite plan: populates
 * {@link TypedPropertyAccess#physicalColumn} for direct (non-association-path)
 * property accesses by rewriting the typed HIR.
 *
 * <p>Walks the rewritten HIR tracking variable→store bindings. When entering a
 * relational operator with lambdas (filter, extend, project, etc.), binds each
 * lambda's parameters to the operator's source-store (looked up via the
 * sidecar). When seeing a {@link TypedPropertyAccess} on a bound variable,
 * resolves the property's physical column via {@code store.columnFor(prop)}
 * and rewrites the access to carry the resolved name on the AST.
 *
 * <p>After this pass, {@link com.gs.legend.plan.lowering.scalar.PropertyAccessLowering}
 * reads the field directly instead of recomputing via {@code storeFor +
 * columnFor}. The lowering-side fallback survives only as a transitional safety
 * net; both fallbacks (lowering-side and source-recursion in {@code storeOf})
 * disappear with the sidecar in Phase 5.
 *
 * <p>Path-bearing accesses (associationPath non-empty) are intentionally left
 * untouched — Phase 3 rewrites those into {@link TypedJoin} subtrees with
 * direct property accesses on the join target alias, and the populator handles
 * those through the same machinery.
 */
final class PropertyAccessPopulator {

    private final IdentityHashMap<TypedSpec, StoreResolution> resolutions;

    PropertyAccessPopulator(IdentityHashMap<TypedSpec, StoreResolution> resolutions) {
        this.resolutions = resolutions;
    }

    TypedSpec populate(TypedSpec root) {
        return walk(root, Map.of());
    }

    /**
     * Forward the sidecar stamp from {@code original} to {@code rebuilt}
     * when rebuilding a node whose children changed. The kernel's
     * identity-preserving rewrite naturally drops sidecar entries on
     * rebuild (since the new instance is identity-distinct), but
     * downstream lowering uses the stamp to resolve the node's output
     * store. Without forwarding, schema-changing ops (Project, GroupBy,
     * Aggregate, Pivot, Select, Rename) lose their TDS-shaped output
     * store and the source-recursion fallback in
     * {@link com.gs.legend.plan.lowering.LoweringContext#storeFor}
     * returns the upstream class store instead — which mis-renders
     * sort-key / extend column references against that operator's
     * output. Pass-through ops (Filter, Sort, Slice, etc.) are equally
     * happy with explicit forwarding or the recursion fallback; we
     * forward unconditionally for simplicity.
     */
    private TypedSpec forwardStamp(TypedSpec original, TypedSpec rebuilt) {
        if (rebuilt == original) return rebuilt;
        StoreResolution stamp = resolutions.get(original);
        if (stamp != null) resolutions.put(rebuilt, stamp);
        return rebuilt;
    }

    /* =================================================================
     * Walk: dispatch over every TypedSpec variant. Mirrors HirRewriter.
     * Threads {@code env} (variable→store bindings) through binders.
     * ================================================================= */

    private TypedSpec walk(TypedSpec node, Map<String, StoreResolution> env) {
        if (node == null) return null;
        return switch (node) {
            // ---------- Property access — the target ----------
            case TypedPropertyAccess pa -> rewriteAccess(pa, env);

            // ---------- Relational ops with lambdas — bind params ----------
            case TypedFilter n -> {
                TypedSpec src = walk(n.source(), env);
                StoreResolution s = storeOf(src, env);
                TypedLambda pred = walkLambda(n.predicate(), s, env);
                yield (src == n.source() && pred == n.predicate()) ? n
                        : forwardStamp(n, new TypedFilter(src, pred, n.def(), n.info()));
            }
            case TypedSort n -> {
                TypedSpec src = walk(n.source(), env);
                StoreResolution s = storeOf(src, env);
                List<TypedSortKey> keys = walkSortKeys(n.keys(), s, env);
                yield (src == n.source() && keys == n.keys()) ? n
                        : forwardStamp(n, new TypedSort(src, keys, n.def(), n.info()));
            }
            case TypedExtend n -> {
                TypedSpec src = walk(n.source(), env);
                StoreResolution s = storeOf(src, env);
                List<TypedExtendCol> exts = walkExtendCols(n.extensions(), s, env);
                yield (src == n.source() && exts == n.extensions()) ? n
                        : forwardStamp(n, new TypedExtend(src, n.traversalSpecs(),
                                exts, n.def(), n.info()));
            }
            case TypedProject n -> {
                TypedSpec src = walk(n.source(), env);
                StoreResolution s = storeOf(src, env);
                List<TypedProjectionCol> cols = walkProjectionCols(n.projections(), s, env);
                yield (src == n.source() && cols == n.projections()) ? n
                        : forwardStamp(n, new TypedProject(src, cols, n.def(), n.info()));
            }
            case TypedFold n -> {
                TypedSpec src = walk(n.source(), env);
                StoreResolution s = storeOf(src, env);
                TypedLambda red = walkLambda(n.reducer(), s, env);
                TypedSpec init = walk(n.init(), env);
                yield (src == n.source() && red == n.reducer() && init == n.init()) ? n
                        : forwardStamp(n, new TypedFold(src, red, init, n.strategy(), n.def(), n.info()));
            }
            case TypedMap n -> {
                TypedSpec src = walk(n.source(), env);
                StoreResolution s = storeOf(src, env);
                TypedLambda m = walkLambda(n.mapper(), s, env);
                yield (src == n.source() && m == n.mapper()) ? n
                        : forwardStamp(n, new TypedMap(src, m, n.def(), n.info()));
            }
            case TypedJoin n -> {
                TypedSpec l = walk(n.left(), env);
                TypedSpec r = walk(n.right(), env);
                // Two-source binder: param[0]→left store, param[1]→right store.
                StoreResolution ls = storeOf(l, env);
                StoreResolution rs = storeOf(r, env);
                TypedLambda cond = walkBiLambda(n.condition(), ls, rs, env);
                yield (l == n.left() && r == n.right() && cond == n.condition()) ? n
                        : forwardStamp(n, new TypedJoin(l, r, cond, n.joinType(), n.renames(), n.def(), n.info()));
            }
            case TypedAsOfJoin n -> {
                TypedSpec l = walk(n.left(), env);
                TypedSpec r = walk(n.right(), env);
                StoreResolution ls = storeOf(l, env);
                StoreResolution rs = storeOf(r, env);
                TypedLambda match = walkBiLambda(n.matchCondition(), ls, rs, env);
                Optional<TypedLambda> key = n.keyCondition()
                        .map(k -> walkBiLambda(k, ls, rs, env));
                yield (l == n.left() && r == n.right() && match == n.matchCondition()
                        && key.equals(n.keyCondition())) ? n
                        : forwardStamp(n, new TypedAsOfJoin(l, r, match, key, n.renames(), n.def(), n.info()));
            }
            case TypedGroupBy n -> {
                TypedSpec src = walk(n.source(), env);
                StoreResolution s = storeOf(src, env);
                List<TypedGroupKey> keys = walkGroupKeys(n.keys(), s, env);
                List<TypedAggCall> aggs = walkAggCalls(n.aggs(), s, env);
                yield (src == n.source() && keys == n.keys() && aggs == n.aggs()) ? n
                        : forwardStamp(n, new TypedGroupBy(src, keys, aggs, n.def(), n.info()));
            }
            case TypedAggregate n -> {
                TypedSpec src = walk(n.source(), env);
                StoreResolution s = storeOf(src, env);
                List<TypedAggCall> aggs = walkAggCalls(n.aggs(), s, env);
                yield (src == n.source() && aggs == n.aggs()) ? n
                        : forwardStamp(n, new TypedAggregate(src, aggs, n.def(), n.info()));
            }
            case TypedPivot n -> {
                TypedSpec src = walk(n.source(), env);
                StoreResolution s = storeOf(src, env);
                List<TypedAggCall> aggs = walkAggCalls(n.aggs(), s, env);
                yield (src == n.source() && aggs == n.aggs()) ? n
                        : forwardStamp(n, new TypedPivot(src, n.pivotColumns(), aggs, n.def(), n.info()));
            }

            // ---------- Relational ops without lambdas — pass-through ----------
            case TypedSlice n -> {
                TypedSpec src = walk(n.source(), env);
                yield src == n.source() ? n
                        : forwardStamp(n, new TypedSlice(src, n.offset(), n.limit(), n.def(), n.info()));
            }
            case TypedDistinct n -> {
                TypedSpec src = walk(n.source(), env);
                yield src == n.source() ? n
                        : forwardStamp(n, new TypedDistinct(src, n.columns(), n.def(), n.info()));
            }
            case TypedFlatten n -> {
                TypedSpec src = walk(n.source(), env);
                yield src == n.source() ? n
                        : forwardStamp(n, new TypedFlatten(src, n.column(), n.def(), n.info()));
            }
            case TypedRename n -> {
                TypedSpec src = walk(n.source(), env);
                yield src == n.source() ? n
                        : forwardStamp(n, new TypedRename(src, n.renames(), n.def(), n.info()));
            }
            case TypedSelect n -> {
                TypedSpec src = walk(n.source(), env);
                yield src == n.source() ? n
                        : forwardStamp(n, new TypedSelect(src, n.cols(), n.def(), n.info()));
            }
            case TypedConcatenate n -> {
                TypedSpec l = walk(n.left(), env);
                TypedSpec r = walk(n.right(), env);
                yield (l == n.left() && r == n.right()) ? n
                        : forwardStamp(n, new TypedConcatenate(l, r, n.def(), n.info()));
            }
            case TypedZip n -> {
                List<TypedSpec> srcs = walkList(n.sources(), env);
                yield srcs == n.sources() ? n
                        : new TypedZip(srcs, n.byKeys(), n.def(), n.info());
            }
            case TypedFrom n -> {
                TypedSpec src = walk(n.source(), env);
                yield src == n.source() ? n
                        : forwardStamp(n, new TypedFrom(src, n.mapping(), n.runtime(), n.def(), n.info()));
            }
            case TypedSerialize n -> {
                TypedSpec src = walk(n.source(), env);
                yield src == n.source() ? n
                        : new TypedSerialize(src, n.format(), n.children(), n.def(), n.info());
            }
            case TypedSerializeImplicit n -> {
                TypedSpec src = walk(n.source(), env);
                yield src == n.source() ? n
                        : new TypedSerializeImplicit(src, n.children());
            }
            case TypedWrite n -> {
                TypedSpec src = walk(n.source(), env);
                TypedSpec dst = walk(n.destination(), env);
                yield (src == n.source() && dst == n.destination()) ? n
                        : new TypedWrite(src, dst, n.def(), n.info());
            }
            case TypedGraphFetch n -> {
                TypedSpec src = walk(n.source(), env);
                yield src == n.source() ? n
                        : new TypedGraphFetch(src, n.children(), n.def(), n.info());
            }

            // ---------- Generic compound nodes — recurse on children ----------
            case TypedNativeCall n -> {
                List<TypedSpec> args = walkList(n.args(), env);
                yield args == n.args() ? n
                        : new TypedNativeCall(n.func(), args, n.info());
            }
            case TypedStructExtract n -> {
                TypedSpec src = walk(n.source(), env);
                yield src == n.source() ? n
                        : new TypedStructExtract(src, n.field(), n.info());
            }
            case TypedNewInstance n -> {
                Map<String, TypedSpec> values = walkMap(n.values(), env);
                yield values == n.values() ? n
                        : new TypedNewInstance(n.className(), values, n.info());
            }
            case TypedIf n -> {
                TypedSpec c = walk(n.condition(), env);
                TypedSpec t = walk(n.thenBranch(), env);
                TypedSpec e = walk(n.elseBranch(), env);
                yield (c == n.condition() && t == n.thenBranch() && e == n.elseBranch()) ? n
                        : new TypedIf(c, t, e, n.info());
            }
            case TypedLet n -> {
                TypedSpec v = walk(n.value(), env);
                yield v == n.value() ? n : new TypedLet(n.name(), v, n.info());
            }
            case TypedBlock n -> {
                List<TypedSpec> stmts = walkList(n.stmts(), env);
                yield stmts == n.stmts() ? n : new TypedBlock(stmts, n.info());
            }
            case TypedMatch n -> {
                TypedSpec subj = walk(n.subject(), env);
                List<TypedLambda> cases = walkLambdaList(n.cases(), null, env);
                yield (subj == n.subject() && cases == n.cases()) ? n
                        : new TypedMatch(subj, cases, n.info());
            }
            case TypedCast n -> {
                TypedSpec e = walk(n.expr(), env);
                yield e == n.expr() ? n : new TypedCast(e, n.targetType(), n.info());
            }
            case TypedCollection n -> {
                List<TypedSpec> vs = walkList(n.values(), env);
                yield vs == n.values() ? n : new TypedCollection(vs, n.info());
            }
            case TypedLambda n -> walkLambda(n, null, env);

            // ---------- Pass-through (no rewrite needed at this phase) ----------
            case TypedGetAll ga -> ga; // post-MR: shouldn't appear, but harmless.
            case TypedUserCall uc -> uc; // post-UCI: shouldn't appear, but harmless.
            case TypedEval ev -> ev;
            case TypedVariable v -> v;

            // ---------- Leaves ----------
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

    /* =================================================================
     * Property access rewrite — the only place that mutates the AST.
     * ================================================================= */

    private TypedSpec rewriteAccess(TypedPropertyAccess pa, Map<String, StoreResolution> env) {
        // Recurse into source so nested non-variable sources (chained accesses,
        // function calls returning rows) get their inner property accesses
        // populated. Variables don't have substructure to walk.
        TypedSpec newSrc = pa.source() instanceof TypedVariable
                ? pa.source()
                : walk(pa.source(), env);
        boolean alreadyResolved = pa.physicalColumn().isPresent();
        boolean pathBearing = pa.associationPath().isPresent()
                && !pa.associationPath().get().isEmpty();
        if (alreadyResolved || pathBearing) {
            // Phase-3 territory or already done — preserve as-is, modulo
            // child rewrite.
            return rebuild(pa, newSrc, pa.physicalColumn());
        }
        StoreResolution store = pa.source() instanceof TypedVariable v
                ? env.get(v.name())
                : storeOf(newSrc, env);
        if (store == null) {
            return rebuild(pa, newSrc, pa.physicalColumn());
        }
        String physical = store.columnFor(pa.property());
        if (physical == null) {
            // The store doesn't map this property — leave physicalColumn
            // empty so lowering's fallback still kicks in. Common for derived
            // properties that get materialized via M2M extends and reference
            // the alias directly.
            return rebuild(pa, newSrc, pa.physicalColumn());
        }
        return rebuild(pa, newSrc, Optional.of(physical));
    }

    private static TypedSpec rebuild(TypedPropertyAccess pa, TypedSpec newSrc,
                                     Optional<String> physicalColumn) {
        if (newSrc == pa.source() && physicalColumn.equals(pa.physicalColumn())) {
            return pa;
        }
        return new TypedPropertyAccess(newSrc, pa.property(), pa.associationPath(),
                physicalColumn, pa.info());
    }

    /* =================================================================
     * Store lookup — sidecar with source-recursion fallback.
     * ================================================================= */

    private StoreResolution storeOf(TypedSpec node, Map<String, StoreResolution> env) {
        if (node == null) return null;
        if (node instanceof TypedVariable v) return env.get(v.name());
        StoreResolution s = resolutions.get(node);
        if (s != null) return s;
        return switch (node) {
            case TypedFilter n -> storeOf(n.source(), env);
            case TypedExtend n -> storeOf(n.source(), env);
            case TypedProject n -> storeOf(n.source(), env);
            case TypedSort n -> storeOf(n.source(), env);
            case TypedSlice n -> storeOf(n.source(), env);
            case TypedDistinct n -> storeOf(n.source(), env);
            case TypedFlatten n -> storeOf(n.source(), env);
            case TypedRename n -> storeOf(n.source(), env);
            case TypedSelect n -> storeOf(n.source(), env);
            case TypedFrom n -> storeOf(n.source(), env);
            default -> null;
        };
    }

    /* =================================================================
     * Lambda walks — bind params to their source store(s).
     * ================================================================= */

    private TypedLambda walkLambda(TypedLambda lam, StoreResolution paramStore,
                                   Map<String, StoreResolution> env) {
        if (lam == null) return null;
        Map<String, StoreResolution> bodyEnv = new LinkedHashMap<>(env);
        for (TypedParam p : lam.parameters()) {
            if (paramStore != null) bodyEnv.put(p.name(), paramStore);
            else bodyEnv.remove(p.name()); // shadow; param has unknown store
        }
        List<TypedSpec> newBody = walkList(lam.body(), bodyEnv);
        if (newBody == lam.body()) return lam;
        return new TypedLambda(lam.parameters(), newBody, lam.info());
    }

    /**
     * Bi-source lambda: TypedJoin / TypedAsOfJoin condition. param[0] binds to
     * {@code leftStore}, param[1] to {@code rightStore}. Extra params (if any)
     * stay unbound.
     */
    private TypedLambda walkBiLambda(TypedLambda lam, StoreResolution leftStore,
                                     StoreResolution rightStore,
                                     Map<String, StoreResolution> env) {
        if (lam == null) return null;
        Map<String, StoreResolution> bodyEnv = new LinkedHashMap<>(env);
        List<TypedParam> ps = lam.parameters();
        for (int i = 0; i < ps.size(); i++) {
            String name = ps.get(i).name();
            StoreResolution s = i == 0 ? leftStore : i == 1 ? rightStore : null;
            if (s != null) bodyEnv.put(name, s);
            else bodyEnv.remove(name);
        }
        List<TypedSpec> newBody = walkList(lam.body(), bodyEnv);
        if (newBody == lam.body()) return lam;
        return new TypedLambda(lam.parameters(), newBody, lam.info());
    }

    private List<TypedLambda> walkLambdaList(List<TypedLambda> in,
                                             StoreResolution paramStore,
                                             Map<String, StoreResolution> env) {
        List<TypedLambda> out = null;
        for (int i = 0; i < in.size(); i++) {
            TypedLambda lam = walkLambda(in.get(i), paramStore, env);
            if (lam != in.get(i)) {
                if (out == null) out = new ArrayList<>(in);
                out.set(i, lam);
            }
        }
        return out == null ? in : out;
    }

    /* =================================================================
     * List / Map / sub-record walks — keep identity when unchanged.
     * ================================================================= */

    private List<TypedSpec> walkList(List<TypedSpec> in, Map<String, StoreResolution> env) {
        List<TypedSpec> out = null;
        for (int i = 0; i < in.size(); i++) {
            TypedSpec s = walk(in.get(i), env);
            if (s != in.get(i)) {
                if (out == null) out = new ArrayList<>(in);
                out.set(i, s);
            }
        }
        return out == null ? in : out;
    }

    private Map<String, TypedSpec> walkMap(Map<String, TypedSpec> in,
                                           Map<String, StoreResolution> env) {
        Map<String, TypedSpec> out = null;
        for (var e : in.entrySet()) {
            TypedSpec v = walk(e.getValue(), env);
            if (v != e.getValue()) {
                if (out == null) out = new LinkedHashMap<>(in);
                out.put(e.getKey(), v);
            }
        }
        return out == null ? in : out;
    }

    private List<TypedExtendCol> walkExtendCols(List<TypedExtendCol> in,
                                                StoreResolution srcStore,
                                                Map<String, StoreResolution> env) {
        List<TypedExtendCol> out = null;
        for (int i = 0; i < in.size(); i++) {
            TypedExtendCol c = in.get(i);
            TypedExtendCol nc = switch (c) {
                case TypedScalarExtendCol s -> {
                    TypedLambda lam = walkLambda(s.expression(), srcStore, env);
                    yield lam == s.expression() ? s
                            : new TypedScalarExtendCol(s.alias(), lam, s.returnType());
                }
                // Window / traverse / association / embedded extends carry
                // their own structure; their inner lambdas (if any) are
                // resolved at MIR time and don't flow through here. Pass
                // them through unchanged.
                default -> c;
            };
            if (nc != c) {
                if (out == null) out = new ArrayList<>(in);
                out.set(i, nc);
            }
        }
        return out == null ? in : out;
    }

    private List<TypedProjectionCol> walkProjectionCols(List<TypedProjectionCol> in,
                                                        StoreResolution srcStore,
                                                        Map<String, StoreResolution> env) {
        List<TypedProjectionCol> out = null;
        for (int i = 0; i < in.size(); i++) {
            TypedProjectionCol c = in.get(i);
            TypedLambda lam = walkLambda(c.expression(), srcStore, env);
            if (lam != c.expression()) {
                if (out == null) out = new ArrayList<>(in);
                out.set(i, new TypedProjectionCol(c.alias(), lam, c.associationPath()));
            }
        }
        return out == null ? in : out;
    }

    private List<TypedSortKey> walkSortKeys(List<TypedSortKey> in,
                                            StoreResolution srcStore,
                                            Map<String, StoreResolution> env) {
        List<TypedSortKey> out = null;
        for (int i = 0; i < in.size(); i++) {
            TypedSortKey k = in.get(i);
            TypedSortKey nk = switch (k) {
                case TypedColumnSortKey c -> c;
                case TypedExpressionSortKey e -> {
                    TypedLambda lam = walkLambda(e.keyFn(), srcStore, env);
                    yield lam == e.keyFn() ? e
                            : new TypedExpressionSortKey(lam, e.direction());
                }
            };
            if (nk != k) {
                if (out == null) out = new ArrayList<>(in);
                out.set(i, nk);
            }
        }
        return out == null ? in : out;
    }

    private List<TypedGroupKey> walkGroupKeys(List<TypedGroupKey> in,
                                              StoreResolution srcStore,
                                              Map<String, StoreResolution> env) {
        List<TypedGroupKey> out = null;
        for (int i = 0; i < in.size(); i++) {
            TypedGroupKey k = in.get(i);
            TypedGroupKey nk = switch (k) {
                case TypedColumnGroupKey c -> c;
                case TypedAssociationGroupKey a -> a;
                case TypedExpressionGroupKey e -> {
                    TypedLambda lam = walkLambda(e.keyFn(), srcStore, env);
                    yield lam == e.keyFn() ? e
                            : new TypedExpressionGroupKey(lam, e.alias());
                }
            };
            if (nk != k) {
                if (out == null) out = new ArrayList<>(in);
                out.set(i, nk);
            }
        }
        return out == null ? in : out;
    }

    private List<TypedAggCall> walkAggCalls(List<TypedAggCall> in,
                                            StoreResolution srcStore,
                                            Map<String, StoreResolution> env) {
        List<TypedAggCall> out = null;
        for (int i = 0; i < in.size(); i++) {
            TypedAggCall a = in.get(i);
            TypedLambda fn1 = walkLambda(a.fn1(), srcStore, env);
            TypedLambda fn2 = walkLambda(a.fn2(), srcStore, env);
            List<TypedSpec> extra = walkList(a.extraArgs(), env);
            if (fn1 != a.fn1() || fn2 != a.fn2() || extra != a.extraArgs()) {
                if (out == null) out = new ArrayList<>(in);
                out.set(i, new TypedAggCall(a.alias(), a.func(), fn1, fn2, extra,
                        a.returnType(), a.castType()));
            }
        }
        return out == null ? in : out;
    }
}
