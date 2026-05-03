package com.gs.legend.compiler;

import com.gs.legend.compiler.typed.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
 * <p><strong>Implementation</strong>: this class is a thin strategy on top
 * of {@link HirRewriter} — it provides the per-pass {@link HirRewriter.Hooks}
 * for the four substitution-target node kinds. The capture-avoiding tree
 * walker, scope discipline, and compound-node descent all live in
 * {@link HirRewriter}; this class owns only the user-call substitution
 * semantics:
 * <ul>
 *   <li><b>Scalar / relational actuals</b>: a {@link TypedVariable} whose
 *       {@code name} matches a bound formal is replaced by the actual
 *       {@link TypedSpec}.</li>
 *   <li><b>Lambda actuals</b>: when the body uses a function-typed param
 *       via {@code TypedEval(applicable=$f, args=[...])}, the eval is
 *       expanded to the lambda's body, with the lambda's own parameters
 *       bound (recursively substituted) to the eval's args.</li>
 *   <li><b>Lexical scoping (capture-avoidance)</b>: handled by the
 *       kernel's {@link HirRewriter.Hooks#enterBinder} default impl —
 *       lambda params and let names trim shadowed bindings from the
 *       inner scope.</li>
 *   <li><b>Recursion</b>: nested {@link TypedUserCall}s in either the body
 *       or the actuals are inlined transitively. {@code CompiledFunction}
 *       forbids self-recursion at compile time, so termination is
 *       guaranteed.</li>
 * </ul>
 *
 * <p><strong>Tree identity</strong>: when no substitution is active and no
 * children change, the kernel returns the original node — so the common
 * case (subtrees with no user-calls) allocates nothing.
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

    /**
     * Top-level entry: inline all {@link TypedUserCall}s in the HIR. If no
     * user-calls are present, returns the input identity-unchanged.
     */
    static TypedSpec inline(TypedSpec root) {
        return new HirRewriter(HOOKS).rewrite(root, HirRewriter.Scope.EMPTY);
    }

    // ==================== Hooks ====================

    /**
     * The user-call substitution strategy plugged into {@link HirRewriter}.
     * Stateless — all per-call-site state lives in the threaded
     * {@link HirRewriter.Scope}.
     */
    private static final HirRewriter.Hooks HOOKS = new HirRewriter.Hooks() {

        @Override
        public TypedSpec rewriteVariable(
                TypedVariable v, HirRewriter.Scope scope, HirRewriter rec) {
            TypedSpec bound = scope.scalarBindings().get(v.name());
            return bound != null ? bound : v;
        }

        @Override
        public TypedSpec rewriteUserCall(
                TypedUserCall uc, HirRewriter.Scope outer, HirRewriter rec) {
            var formals = uc.callee().parameters();
            var actuals = uc.args();
            if (formals.size() != actuals.size()) {
                throw new IllegalStateException(
                        "[user-call-inliner] arity mismatch for " + uc.functionFqn()
                                + ": formals=" + formals.size()
                                + " actuals=" + actuals.size());
            }
            Map<String, TypedSpec> scalar = new LinkedHashMap<>();
            Map<String, TypedLambda> lambdas = new LinkedHashMap<>();
            for (int i = 0; i < formals.size(); i++) {
                String name = formals.get(i).name();
                TypedSpec actual = rec.rewrite(actuals.get(i), outer);
                if (actual instanceof TypedLambda lam) {
                    lambdas.put(name, lam);
                } else {
                    scalar.put(name, actual);
                }
            }
            HirRewriter.Scope inner = new HirRewriter.Scope(
                    Map.copyOf(scalar), Map.copyOf(lambdas));
            return rec.rewrite(uc.callee().body().hir(), inner);
        }

        @Override
        public TypedSpec rewriteEval(
                TypedEval ev, HirRewriter.Scope scope, HirRewriter rec) {
            TypedVariable applicable = ev.applicable();
            TypedLambda lam = scope.lambdaBindings().get(applicable.name());
            if (lam != null) {
                return expandLambdaApplication(lam, ev.args(), scope, rec);
            }
            // Default descent: applicable is a TypedVariable bound elsewhere
            // or not a function-param. Rewrite args; applicable cannot become
            // a non-variable here, so reuse it.
            List<TypedSpec> args = rec.rewriteList(ev.args(), scope);
            return args == ev.args() ? ev
                    : new TypedEval(applicable, args, ev.def(), ev.info());
        }
    };

    /**
     * Expand {@code lambda($args)} into the lambda's body, with the lambda's
     * formal parameters substituted by the (rewritten-in-outer-scope) eval
     * args. The lambda's own params become a fresh scalar binding scope.
     * This matches the eval-site semantics that
     * {@code ControlFlowLowering.lower(TypedEval)} previously implemented at
     * lowering time.
     */
    private static TypedSpec expandLambdaApplication(
            TypedLambda lam, List<TypedSpec> args,
            HirRewriter.Scope outer, HirRewriter rec) {
        var params = lam.parameters();
        if (params.size() != args.size()) {
            throw new IllegalStateException(
                    "[user-call-inliner] lambda arity mismatch: params="
                            + params.size() + " args=" + args.size());
        }
        Map<String, TypedSpec> scalar = new LinkedHashMap<>(outer.scalarBindings());
        for (int i = 0; i < params.size(); i++) {
            scalar.put(params.get(i).name(), rec.rewrite(args.get(i), outer));
        }
        HirRewriter.Scope inner = new HirRewriter.Scope(
                Map.copyOf(scalar), outer.lambdaBindings());
        var body = lam.body();
        if (body.size() == 1) return rec.rewrite(body.get(0), inner);
        List<TypedSpec> rewritten = new ArrayList<>(body.size());
        for (var stmt : body) rewritten.add(rec.rewrite(stmt, inner));
        return new TypedBlock(rewritten, lam.info());
    }
}
