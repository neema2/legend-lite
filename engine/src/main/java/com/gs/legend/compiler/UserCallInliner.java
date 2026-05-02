package com.gs.legend.compiler;

import com.gs.legend.compiler.typed.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Inlines {@link TypedUserCall} nodes into the typed HIR by substituting
 * formal parameters with actual arguments inside the callee body.
 *
 * <p><strong>Pass position:</strong> runs at the head of
 * {@link MappingResolver#resolve()}, before the resolution walk. After this
 * pass, no {@link TypedUserCall} remains in the resolved HIR — the body has
 * been spliced into the call site with formals bound to actuals. Downstream
 * layers (resolution walk, {@code Lowerer}, {@code ResultFormat},
 * {@code PlanGenerator}, executor) see one unified shape and never need to
 * "look through" call wrappers.
 *
 * <p><strong>Substitution semantics</strong>:
 * <ul>
 *   <li><b>Scalar / relational actuals</b>: a {@link TypedVariable} whose
 *       {@code name} matches a bound formal is replaced by the actual
 *       {@link TypedSpec}. (The TypeChecker stamps function and lambda
 *       parameters with the same {@link com.gs.legend.compiler.typed.Role#LAMBDA_PARAM}
 *       role today, so the discriminator is by name + lexical scope, not
 *       by role.)</li>
 *   <li><b>Lambda actuals</b>: when the body uses a function-typed param
 *       via {@code TypedEval(applicable=$f, args=[...])}, the eval is
 *       expanded to the lambda's body, with the lambda's own parameters
 *       bound (recursively substituted) to the eval's args.</li>
 *   <li><b>Lexical scoping (capture-avoidance)</b>: when entering a
 *       binder that introduces a name shadowing an outer formal
 *       (a {@link TypedLet} in a {@link TypedBlock}, or a {@link TypedLambda}
 *       parameter), the shadowed name is removed from the substitution map
 *       for the inner scope. The outer-binder's binding then handles the
 *       inner reference at lowering time. This is the standard
 *       capture-avoiding substitution rule.</li>
 *   <li><b>Recursion</b>: nested {@link TypedUserCall}s in either the body
 *       or the actuals are inlined transitively. {@link com.gs.legend.compiled.CompiledFunction}
 *       forbids self-recursion at compile time, so termination is
 *       guaranteed.</li>
 * </ul>
 *
 * <p><strong>Tree identity</strong>: when no substitution is active and no
 * children change, {@link #rewrite} returns the original node — so the
 * common case (subtrees with no user-calls) allocates nothing. When any
 * descendant substitutes, ancestors rebuild their compound nodes with the
 * new children.
 */
public final class UserCallInliner {

    private UserCallInliner() {}

    /**
     * Pipeline entry point: inline all {@link TypedUserCall}s in the given
     * compiled expression. Returns the same {@link com.gs.legend.compiled.CompiledExpression}
     * instance if no calls were present (identity-preservation), or a fresh
     * one wrapping the rewritten HIR with the original dependencies otherwise.
     */
    public static com.gs.legend.compiled.CompiledExpression inline(
            com.gs.legend.compiled.CompiledExpression unit) {
        TypedSpec rewritten = inline(unit.hir());
        return rewritten == unit.hir()
                ? unit
                : new com.gs.legend.compiled.CompiledExpression(rewritten, unit.dependencies());
    }

    /** Substitution scope: function-param-name → bound actual. */
    private record Ctx(
            Map<String, TypedSpec> scalarBindings,
            Map<String, TypedLambda> lambdaBindings) {

        static final Ctx EMPTY = new Ctx(Map.of(), Map.of());
    }

    /**
     * Top-level entry: inline all {@link TypedUserCall}s in the HIR. If no
     * user-calls are present, returns the input identity-unchanged.
     */
    static TypedSpec inline(TypedSpec root) {
        return rewrite(root, Ctx.EMPTY);
    }

    // ==================== Core rewrite ====================

    private static TypedSpec rewrite(TypedSpec node, Ctx ctx) {
        if (node == null) return null;

        return switch (node) {
            // ---------- Substitution targets ----------
            case TypedVariable v -> rewriteVariable(v, ctx);
            case TypedUserCall uc -> inlineCall(uc, ctx);
            case TypedEval ev -> rewriteEval(ev, ctx);

            // ---------- Compound nodes (rebuild if any child changed) ----------
            case TypedFilter n -> {
                TypedSpec src = rewrite(n.source(), ctx);
                TypedLambda pred = rewriteLambda(n.predicate(), ctx);
                yield (src == n.source() && pred == n.predicate()) ? n
                        : new TypedFilter(src, pred, n.def(), n.info());
            }
            case TypedSort n -> {
                TypedSpec src = rewrite(n.source(), ctx);
                List<TypedSortKey> keys = rewriteSortKeys(n.keys(), ctx);
                yield (src == n.source() && keys == n.keys()) ? n
                        : new TypedSort(src, keys, n.def(), n.info());
            }
            case TypedSlice n -> {
                TypedSpec src = rewrite(n.source(), ctx);
                yield src == n.source() ? n
                        : new TypedSlice(src, n.offset(), n.limit(), n.def(), n.info());
            }
            case TypedConcatenate n -> {
                TypedSpec l = rewrite(n.left(), ctx);
                TypedSpec r = rewrite(n.right(), ctx);
                yield (l == n.left() && r == n.right()) ? n
                        : new TypedConcatenate(l, r, n.def(), n.info());
            }
            case TypedDistinct n -> {
                TypedSpec src = rewrite(n.source(), ctx);
                yield src == n.source() ? n
                        : new TypedDistinct(src, n.columns(), n.def(), n.info());
            }
            case TypedFlatten n -> {
                TypedSpec src = rewrite(n.source(), ctx);
                yield src == n.source() ? n
                        : new TypedFlatten(src, n.column(), n.def(), n.info());
            }
            case TypedRename n -> {
                TypedSpec src = rewrite(n.source(), ctx);
                yield src == n.source() ? n
                        : new TypedRename(src, n.renames(), n.def(), n.info());
            }
            case TypedSelect n -> {
                TypedSpec src = rewrite(n.source(), ctx);
                yield src == n.source() ? n
                        : new TypedSelect(src, n.cols(), n.def(), n.info());
            }
            case TypedFold n -> {
                TypedSpec src = rewrite(n.source(), ctx);
                TypedLambda red = rewriteLambda(n.reducer(), ctx);
                TypedSpec init = rewrite(n.init(), ctx);
                yield (src == n.source() && red == n.reducer() && init == n.init()) ? n
                        : new TypedFold(src, red, init, n.strategy(), n.def(), n.info());
            }
            case TypedMap n -> {
                TypedSpec src = rewrite(n.source(), ctx);
                TypedLambda map = rewriteLambda(n.mapper(), ctx);
                yield (src == n.source() && map == n.mapper()) ? n
                        : new TypedMap(src, map, n.def(), n.info());
            }
            case TypedZip n -> {
                List<TypedSpec> srcs = rewriteList(n.sources(), ctx);
                yield srcs == n.sources() ? n
                        : new TypedZip(srcs, n.byKeys(), n.def(), n.info());
            }
            case TypedExtend n -> {
                TypedSpec src = rewrite(n.source(), ctx);
                List<TypedExtendCol> exts = rewriteExtendCols(n.extensions(), ctx);
                yield (src == n.source() && exts == n.extensions()) ? n
                        : new TypedExtend(src, n.traversalSpecs(), exts, n.def(), n.info());
            }
            case TypedProject n -> {
                TypedSpec src = rewrite(n.source(), ctx);
                List<TypedProjectionCol> cols = rewriteProjectionCols(n.projections(), ctx);
                yield (src == n.source() && cols == n.projections()) ? n
                        : new TypedProject(src, cols, n.def(), n.info());
            }
            case TypedJoin n -> {
                TypedSpec l = rewrite(n.left(), ctx);
                TypedSpec r = rewrite(n.right(), ctx);
                TypedLambda cond = rewriteLambda(n.condition(), ctx);
                yield (l == n.left() && r == n.right() && cond == n.condition()) ? n
                        : new TypedJoin(l, r, cond, n.joinType(), n.renames(), n.def(), n.info());
            }
            case TypedAsOfJoin n -> {
                TypedSpec l = rewrite(n.left(), ctx);
                TypedSpec r = rewrite(n.right(), ctx);
                TypedLambda match = rewriteLambda(n.matchCondition(), ctx);
                Optional<TypedLambda> key = n.keyCondition().map(k -> rewriteLambda(k, ctx));
                yield (l == n.left() && r == n.right() && match == n.matchCondition()
                        && key.equals(n.keyCondition())) ? n
                        : new TypedAsOfJoin(l, r, match, key, n.renames(), n.def(), n.info());
            }
            case TypedGroupBy n -> {
                TypedSpec src = rewrite(n.source(), ctx);
                List<TypedGroupKey> keys = rewriteGroupKeys(n.keys(), ctx);
                List<TypedAggCall> aggs = rewriteAggCalls(n.aggs(), ctx);
                yield (src == n.source() && keys == n.keys() && aggs == n.aggs()) ? n
                        : new TypedGroupBy(src, keys, aggs, n.def(), n.info());
            }
            case TypedAggregate n -> {
                TypedSpec src = rewrite(n.source(), ctx);
                List<TypedAggCall> aggs = rewriteAggCalls(n.aggs(), ctx);
                yield (src == n.source() && aggs == n.aggs()) ? n
                        : new TypedAggregate(src, aggs, n.def(), n.info());
            }
            case TypedPivot n -> {
                TypedSpec src = rewrite(n.source(), ctx);
                List<TypedAggCall> aggs = rewriteAggCalls(n.aggs(), ctx);
                yield (src == n.source() && aggs == n.aggs()) ? n
                        : new TypedPivot(src, n.pivotColumns(), aggs, n.def(), n.info());
            }
            case TypedFrom n -> {
                TypedSpec src = rewrite(n.source(), ctx);
                yield src == n.source() ? n
                        : new TypedFrom(src, n.mapping(), n.runtime(), n.def(), n.info());
            }
            case TypedSerialize n -> {
                TypedSpec src = rewrite(n.source(), ctx);
                yield src == n.source() ? n
                        : new TypedSerialize(src, n.format(), n.children(), n.def(), n.info());
            }
            case TypedWrite n -> {
                TypedSpec src = rewrite(n.source(), ctx);
                TypedSpec dst = rewrite(n.destination(), ctx);
                yield (src == n.source() && dst == n.destination()) ? n
                        : new TypedWrite(src, dst, n.def(), n.info());
            }
            case TypedGraphFetch n -> {
                TypedSpec src = rewrite(n.source(), ctx);
                yield src == n.source() ? n
                        : new TypedGraphFetch(src, n.children(), n.def(), n.info());
            }
            case TypedNativeCall n -> {
                List<TypedSpec> args = rewriteList(n.args(), ctx);
                yield args == n.args() ? n
                        : new TypedNativeCall(n.func(), args, n.info());
            }
            case TypedPropertyAccess n -> {
                TypedSpec src = rewrite(n.source(), ctx);
                yield src == n.source() ? n
                        : new TypedPropertyAccess(src, n.property(), n.associationPath(), n.info());
            }
            case TypedStructExtract n -> {
                TypedSpec src = rewrite(n.source(), ctx);
                yield src == n.source() ? n
                        : new TypedStructExtract(src, n.field(), n.info());
            }
            case TypedNewInstance n -> {
                Map<String, TypedSpec> values = rewriteMap(n.values(), ctx);
                yield values == n.values() ? n
                        : new TypedNewInstance(n.className(), values, n.info());
            }
            case TypedIf n -> {
                TypedSpec c = rewrite(n.condition(), ctx);
                TypedSpec t = rewrite(n.thenBranch(), ctx);
                TypedSpec e = rewrite(n.elseBranch(), ctx);
                yield (c == n.condition() && t == n.thenBranch() && e == n.elseBranch()) ? n
                        : new TypedIf(c, t, e, n.info());
            }
            case TypedLet n -> {
                TypedSpec v = rewrite(n.value(), ctx);
                yield v == n.value() ? n : new TypedLet(n.name(), v, n.info());
            }
            case TypedBlock n -> rewriteBlock(n, ctx);
            case TypedMatch n -> {
                TypedSpec subj = rewrite(n.subject(), ctx);
                List<TypedLambda> cases = rewriteLambdaList(n.cases(), ctx);
                yield (subj == n.subject() && cases == n.cases()) ? n
                        : new TypedMatch(subj, cases, n.info());
            }
            case TypedCast n -> {
                TypedSpec e = rewrite(n.expr(), ctx);
                yield e == n.expr() ? n : new TypedCast(e, n.targetType(), n.info());
            }
            case TypedLambda n -> rewriteLambda(n, ctx);
            case TypedCollection n -> {
                List<TypedSpec> vs = rewriteList(n.values(), ctx);
                yield vs == n.values() ? n : new TypedCollection(vs, n.info());
            }
            case TypedSerializeImplicit n -> {
                TypedSpec src = rewrite(n.source(), ctx);
                yield src == n.source() ? n
                        : new TypedSerializeImplicit(src, n.children());
            }

            // ---------- Leaves: no children, no rewrite ----------
            case TypedGetAll n -> n;
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

    // ==================== Substitution targets ====================

    /**
     * Substitute a {@link TypedVariable} when it references a function
     * parameter that is bound in the current context. Other roles
     * ({@code LAMBDA_PARAM}, {@code LET_BINDING}) are passthrough — they
     * never participate in user-call inlining.
     */
    private static TypedSpec rewriteVariable(TypedVariable v, Ctx ctx) {
        TypedSpec bound = ctx.scalarBindings().get(v.name());
        if (bound != null) return bound;
        return v;
    }

    /**
     * Inline a user-call. The body is rewritten under a fresh context that
     * binds each formal to its (already-rewritten-in-the-outer-context)
     * actual. Args are rewritten under the outer ctx so any user-calls or
     * variable references in them resolve in the call site's scope.
     */
    private static TypedSpec inlineCall(TypedUserCall uc, Ctx outer) {
        var formals = uc.callee().parameters();
        var actuals = uc.args();
        if (formals.size() != actuals.size()) {
            throw new IllegalStateException(
                    "[user-call-inliner] arity mismatch for " + uc.functionFqn()
                            + ": formals=" + formals.size() + " actuals=" + actuals.size());
        }
        Map<String, TypedSpec> scalar = new LinkedHashMap<>();
        Map<String, TypedLambda> lambdas = new LinkedHashMap<>();
        for (int i = 0; i < formals.size(); i++) {
            String name = formals.get(i).name();
            TypedSpec actual = rewrite(actuals.get(i), outer);
            if (actual instanceof TypedLambda lam) {
                lambdas.put(name, lam);
            } else {
                scalar.put(name, actual);
            }
        }
        Ctx inner = new Ctx(Map.copyOf(scalar), Map.copyOf(lambdas));
        return rewrite(uc.callee().body().hir(), inner);
    }

    /**
     * Rewrite a {@link TypedEval}. If the applicable is a function-param
     * variable bound to a lambda actual, expand the eval to the lambda's
     * body, with the lambda's own parameters bound to the eval args.
     * Otherwise, ordinary descent.
     */
    private static TypedSpec rewriteEval(TypedEval ev, Ctx ctx) {
        TypedVariable applicable = ev.applicable();
        TypedLambda lam = ctx.lambdaBindings().get(applicable.name());
        if (lam != null) {
            return expandLambdaApplication(lam, ev.args(), ctx);
        }
        // Default descent: applicable is a TypedVariable (bound elsewhere or
        // not a function-param). Rewrite args; applicable cannot become a
        // non-variable here, so reuse it.
        List<TypedSpec> args = rewriteList(ev.args(), ctx);
        return args == ev.args() ? ev
                : new TypedEval(applicable, args, ev.def(), ev.info());
    }

    /**
     * Expand {@code lambda($args)} into the lambda's body, with the lambda's
     * formal parameters substituted by the (rewritten-in-outer-ctx) eval
     * args. The lambda's own params are FUNCTION_PARAM-equivalent for the
     * purposes of this substitution: we treat them as a fresh scalar binding
     * scope. This matches the eval-site semantics that
     * {@code ControlFlowLowering.lower(TypedEval)} previously implemented at
     * lowering time.
     */
    private static TypedSpec expandLambdaApplication(
            TypedLambda lam, List<TypedSpec> args, Ctx outer) {
        var params = lam.parameters();
        if (params.size() != args.size()) {
            throw new IllegalStateException(
                    "[user-call-inliner] lambda arity mismatch: params=" + params.size()
                            + " args=" + args.size());
        }
        Map<String, TypedSpec> scalar = new LinkedHashMap<>(outer.scalarBindings());
        for (int i = 0; i < params.size(); i++) {
            scalar.put(params.get(i).name(), rewrite(args.get(i), outer));
        }
        Ctx inner = new Ctx(Map.copyOf(scalar), outer.lambdaBindings());
        // Lambda body is a List<TypedSpec>. Single-statement bodies return
        // their sole stmt; multi-statement bodies wrap in TypedBlock so the
        // result is a single TypedSpec carrying the lambda's value.
        var body = lam.body();
        if (body.size() == 1) return rewrite(body.get(0), inner);
        List<TypedSpec> rewritten = new ArrayList<>(body.size());
        for (var stmt : body) rewritten.add(rewrite(stmt, inner));
        return new TypedBlock(rewritten, lam.info());
    }

    // ==================== Helpers for compound sub-records ====================

    private static TypedLambda rewriteLambda(TypedLambda lam, Ctx ctx) {
        if (lam == null) return null;
        // Lambda parameters introduce fresh names that shadow any outer
        // formals of the same name. Strip those bindings before rewriting
        // the body so capture-avoiding substitution holds.
        Ctx inner = trim(ctx, lam.parameters().stream().map(TypedParam::name).toList());
        var body = lam.body();
        List<TypedSpec> rewritten = null;
        for (int i = 0; i < body.size(); i++) {
            TypedSpec s = rewrite(body.get(i), inner);
            if (s != body.get(i) && rewritten == null) {
                rewritten = new ArrayList<>(body);
            }
            if (rewritten != null) rewritten.set(i, s);
        }
        return rewritten == null ? lam : new TypedLambda(lam.parameters(), rewritten, lam.info());
    }

    /**
     * Rewrite a {@link TypedBlock}. Walks statements left-to-right; each
     * {@link TypedLet} introduces a let-bound name that shadows outer
     * formals of the same name for SUBSEQUENT statements (the let's own
     * value RHS still uses the outer scope). The let-binding's lookup at
     * lowering time will then resolve {@code $name} to the let value.
     */
    private static TypedSpec rewriteBlock(TypedBlock n, Ctx ctx) {
        List<TypedSpec> stmts = n.stmts();
        List<TypedSpec> out = null;
        Ctx cur = ctx;
        for (int i = 0; i < stmts.size(); i++) {
            TypedSpec s = stmts.get(i);
            TypedSpec rewritten = rewrite(s, cur);
            if (rewritten != s && out == null) out = new ArrayList<>(stmts);
            if (out != null) out.set(i, rewritten);
            // After rewriting a let, shadow its name for following stmts.
            if (s instanceof TypedLet let) {
                cur = trim(cur, List.of(let.name()));
            }
        }
        return out == null ? n : new TypedBlock(out, n.info());
    }

    /** Return a Ctx with the given names removed from both binding maps. */
    private static Ctx trim(Ctx ctx, List<String> names) {
        if (names.isEmpty() || ctx.scalarBindings().isEmpty() && ctx.lambdaBindings().isEmpty()) {
            return ctx;
        }
        boolean hits = false;
        for (String n : names) {
            if (ctx.scalarBindings().containsKey(n) || ctx.lambdaBindings().containsKey(n)) {
                hits = true;
                break;
            }
        }
        if (!hits) return ctx;
        Map<String, TypedSpec> s = new LinkedHashMap<>(ctx.scalarBindings());
        Map<String, TypedLambda> l = new LinkedHashMap<>(ctx.lambdaBindings());
        for (String n : names) { s.remove(n); l.remove(n); }
        return new Ctx(Map.copyOf(s), Map.copyOf(l));
    }

    private static List<TypedSpec> rewriteList(List<TypedSpec> in, Ctx ctx) {
        List<TypedSpec> out = null;
        for (int i = 0; i < in.size(); i++) {
            TypedSpec s = rewrite(in.get(i), ctx);
            if (s != in.get(i) && out == null) {
                out = new ArrayList<>(in);
            }
            if (out != null) out.set(i, s);
        }
        return out == null ? in : out;
    }

    private static List<TypedLambda> rewriteLambdaList(List<TypedLambda> in, Ctx ctx) {
        List<TypedLambda> out = null;
        for (int i = 0; i < in.size(); i++) {
            TypedLambda lam = rewriteLambda(in.get(i), ctx);
            if (lam != in.get(i) && out == null) {
                out = new ArrayList<>(in);
            }
            if (out != null) out.set(i, lam);
        }
        return out == null ? in : out;
    }

    private static Map<String, TypedSpec> rewriteMap(Map<String, TypedSpec> in, Ctx ctx) {
        Map<String, TypedSpec> out = null;
        for (var e : in.entrySet()) {
            TypedSpec rewritten = rewrite(e.getValue(), ctx);
            if (rewritten != e.getValue() && out == null) {
                out = new LinkedHashMap<>(in);
            }
            if (out != null) out.put(e.getKey(), rewritten);
        }
        return out == null ? in : out;
    }

    private static List<TypedSortKey> rewriteSortKeys(List<TypedSortKey> in, Ctx ctx) {
        List<TypedSortKey> out = null;
        for (int i = 0; i < in.size(); i++) {
            TypedSortKey k = in.get(i);
            TypedSortKey nk = switch (k) {
                case TypedColumnSortKey c -> c;
                case TypedExpressionSortKey e -> {
                    TypedLambda lam = rewriteLambda(e.keyFn(), ctx);
                    yield lam == e.keyFn() ? e
                            : new TypedExpressionSortKey(lam, e.direction());
                }
            };
            if (nk != k && out == null) out = new ArrayList<>(in);
            if (out != null) out.set(i, nk);
        }
        return out == null ? in : out;
    }

    private static List<TypedGroupKey> rewriteGroupKeys(List<TypedGroupKey> in, Ctx ctx) {
        List<TypedGroupKey> out = null;
        for (int i = 0; i < in.size(); i++) {
            TypedGroupKey k = in.get(i);
            TypedGroupKey nk = switch (k) {
                case TypedColumnGroupKey c -> c;
                case TypedAssociationGroupKey a -> a;
                case TypedExpressionGroupKey e -> {
                    TypedLambda lam = rewriteLambda(e.keyFn(), ctx);
                    yield lam == e.keyFn() ? e
                            : new TypedExpressionGroupKey(lam, e.alias());
                }
            };
            if (nk != k && out == null) out = new ArrayList<>(in);
            if (out != null) out.set(i, nk);
        }
        return out == null ? in : out;
    }

    private static List<TypedAggCall> rewriteAggCalls(List<TypedAggCall> in, Ctx ctx) {
        List<TypedAggCall> out = null;
        for (int i = 0; i < in.size(); i++) {
            TypedAggCall a = in.get(i);
            TypedLambda fn1 = rewriteLambda(a.fn1(), ctx);
            TypedLambda fn2 = rewriteLambda(a.fn2(), ctx);
            List<TypedSpec> extra = rewriteList(a.extraArgs(), ctx);
            boolean changed = fn1 != a.fn1() || fn2 != a.fn2() || extra != a.extraArgs();
            if (changed) {
                if (out == null) out = new ArrayList<>(in);
                out.set(i, new TypedAggCall(a.alias(), a.func(), fn1, fn2,
                        extra, a.returnType(), a.castType()));
            }
        }
        return out == null ? in : out;
    }

    private static List<TypedProjectionCol> rewriteProjectionCols(
            List<TypedProjectionCol> in, Ctx ctx) {
        List<TypedProjectionCol> out = null;
        for (int i = 0; i < in.size(); i++) {
            TypedProjectionCol c = in.get(i);
            TypedLambda lam = rewriteLambda(c.expression(), ctx);
            if (lam != c.expression()) {
                if (out == null) out = new ArrayList<>(in);
                out.set(i, new TypedProjectionCol(c.alias(), lam, c.associationPath()));
            }
        }
        return out == null ? in : out;
    }

    private static List<TypedExtendCol> rewriteExtendCols(
            List<TypedExtendCol> in, Ctx ctx) {
        List<TypedExtendCol> out = null;
        for (int i = 0; i < in.size(); i++) {
            TypedExtendCol c = in.get(i);
            TypedExtendCol nc = switch (c) {
                case TypedScalarExtendCol s -> {
                    TypedLambda lam = rewriteLambda(s.expression(), ctx);
                    yield lam == s.expression() ? s
                            : new TypedScalarExtendCol(s.alias(), lam, s.returnType());
                }
                case TypedTraverseExtendCol t -> {
                    TypedLambda lam = rewriteLambda(t.expression(), ctx);
                    yield lam == t.expression() ? t
                            : new TypedTraverseExtendCol(t.alias(), t.hops(), lam);
                }
                case TypedWindowExtendCol w -> {
                    List<TypedSpec> args = rewriteList(w.funcArgs(), ctx);
                    Optional<TypedLambda> reducer = w.reducer().map(l -> rewriteLambda(l, ctx));
                    Optional<TypedWindowExtendCol.OuterWrapper> outer = w.outerWrapper().map(ow -> {
                        TypedSpec rew = rewrite(ow.expr(), ctx);
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
}
