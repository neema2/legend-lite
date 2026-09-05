package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.TypedParameter;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Phase G &mdash; the whole-function compile <strong>driver</strong>
 * (PHASE_G_SPEC_COMPILER.md §2/§6): demand-driven, memoized compilation of
 * function bodies into {@link CompiledFunction}s. Expression-level type-checking
 * lives in the {@link Typer} (bidirectional synth/check + {@link CoreFn}
 * dispatch); the pure type machinery lives in the {@link InferenceKernel}.
 *
 * <p>The driver's one typing rule: a call is typed from the callee's
 * <em>signature</em>, never by recursing into its body &mdash; so compilation of a
 * function graph needs no cycle guard, only the worklist in
 * {@link #compileReachable}.
 */
public final class SpecCompiler {

    private final Typer typer;

    /**
     * Demand memo (G.4): one {@link CompiledFunction} per function body, keyed by
     * the F-cached {@link TypedFunction} <strong>identity</strong> (refined G-γ).
     * It is a field of this compiler, so it is bound to the immutable
     * {@link ModelContext} snapshot &mdash; a new model means a new compiler and an
     * empty memo, never a stale hit. It also doubles as the visited-set for
     * {@link #compileReachable}, which is why no cycle guard is needed.
     */
    private final Map<TypedFunction, CompiledFunction> memo = new IdentityHashMap<>();

    private final ModelContext ctx;

    public SpecCompiler(ModelContext ctx) {
        this.ctx = ctx;
        this.typer = new Typer(ctx, new InferenceKernel(ctx));
    }

    /** The model this compiler compiles against (the literal unroll's
     * class-hierarchy oracle). */
    public ModelContext ctx() {
        return ctx;
    }

    /**
     * The public entry: type-check {@code fn}'s body, demand-driven and memoized
     * &mdash; compiled once per (F-cached) {@link TypedFunction} and cached for the
     * snapshot's lifetime (G-γ). This is the single-function compile a consumer
     * (or lowering) asks for; {@link #compileReachable} is the eager whole-graph
     * variant built on top of it.
     */
    public CompiledFunction compile(TypedFunction fn) {
        CompiledFunction cached = memo.get(fn);
        if (cached != null) {
            return cached;
        }
        CompiledFunction cf;
        try {
            cf = check(fn);   // flat: check() never re-enters compile()
        } catch (TypeInferenceException e) {
            // Positions stopgap: name the ENCLOSING FUNCTION (synth FQNs
            // encode owner+property, e.g. test::Person$prop$age) — the
            // expression-level [line:col] is the deferred big lift.
            // -Dlegend.spec.trace=<name fragment> prints WHO demanded the body
            String trace = System.getProperty("legend.spec.trace");
            if (trace != null && fn.qualifiedName().contains(trace)) {
                new Exception("[spec] compile of " + fn.qualifiedName() + " failed: " + e.getMessage())
                        .printStackTrace(System.err);
            }
            throw new TypeInferenceException(
                    "in function '" + fn.qualifiedName() + "': " + e.getMessage(), e);
        }
        memo.put(fn, cf);
        return cf;
    }

    /**
     * Eagerly compile {@code root} and every function its (transitive) typed body
     * references &mdash; each <strong>exactly once</strong>. This is whole-graph
     * validation done demand-driven: only functions reachable from {@code root}
     * are compiled.
     *
     * <p><strong>No cycle guard.</strong> The traversal lives in this worklist,
     * not in {@code check} (which types calls against signatures and never
     * recurses into a callee's body). The {@link #memo} doubles as the
     * visited-set, so a cycle {@code A→B→A} simply finds {@code A} already
     * compiled and stops.
     */
    public List<CompiledFunction> compileReachable(TypedFunction root) {
        List<CompiledFunction> compiled = new ArrayList<>();
        Deque<TypedFunction> work = new ArrayDeque<>();
        work.add(root);
        while (!work.isEmpty()) {
            TypedFunction fn = work.poll();
            if (memo.containsKey(fn)) {
                continue;                       // already compiled — dedup + cycle break
            }
            CompiledFunction cf = compile(fn);  // compiles fn's body (flat, signature-based)
            compiled.add(cf);
            for (TypedSpec stmt : cf.body()) {
                callees(stmt).forEach(work::add);   // enqueue the user functions it references
            }
        }
        return compiled;
    }

    /**
     * Every user-function callee a typed node (transitively) references &mdash; a
     * pure query over the HIR's one traversal spine ({@link TypedSpec#children()});
     * each node declares its own children, so this driver knows nothing about
     * node structure.
     */
    private static Stream<TypedFunction> callees(TypedSpec node) {
        Stream<TypedFunction> own = node instanceof TypedUserCall uc ? Stream.of(uc.callee()) : Stream.empty();
        return Stream.concat(own, node.children().stream().flatMap(SpecCompiler::callees));
    }

    /**
     * Type-check a whole function body against its declared signature (engine
     * {@code check(PureFunction)}) &mdash; the un-memoized worker behind
     * {@link #compile}. The parameters seed the environment, body statements are
     * checked in sequence, and the <strong>last</strong> statement is checked
     * (bidirectional {@code Check} mode, §2) against the declared return type.
     * Throws if the body is absent (native / dependency) or does not conform.
     *
     * <p>Single-statement bodies (the usual derived-property / constraint case)
     * are the common path; earlier statements in a multi-statement body are
     * inferred.
     */
    private CompiledFunction check(TypedFunction fn) {
        List<ValueSpecification> body = fn.body().orElseThrow(() -> new TypeInferenceException(
                "cannot type-check '" + fn.qualifiedName() + "': it has no body (native or dependency)"));
        if (body.isEmpty()) {
            throw new TypeInferenceException("function '" + fn.qualifiedName() + "' has an empty body");
        }

        Env scope = Env.empty();
        // generated-source escape (pure NewValidator parity): a
        // mapping-synth ctor body ($class$ marker — the engine's own
        // generated-FQN convention; '$' cannot appear in user pure
        // identifiers) constructs partial instances by design
        if (fn.qualifiedName().contains("$class$")) {
            scope = scope.withLenientNew();
        }
        for (TypedParameter p : fn.parameters()) {
            scope = scope.with(p.name(), new ExprType(p.type(), p.multiplicity()));
        }
        ExprType declaredReturn = new ExprType(fn.returnType(), fn.returnMultiplicity());

        List<TypedSpec> typed = new ArrayList<>(body.size());
        int last = body.size() - 1;
        for (int i = 0; i < body.size(); i++) {
            // bind-once (family A): deferred-kind let rhs parks (same
            // rule as the query fold below — never the last statement)
            com.legend.protocol.spec.CString dn;
            if (i < last
                    && (dn = SourceSubst.letName(body.get(i))) != null
                    && Typer.deferredLetRhs(
                            ((com.legend.protocol.spec.AppliedFunction) body.get(i))
                                    .parameters().get(1))) {
                scope = scope.withDeferred(dn.value(),
                        ((com.legend.protocol.spec.AppliedFunction) body.get(i))
                                .parameters().get(1));
                continue;
            }
            TypedSpec stmt = typeBody(body.get(i), scope, Expected.infer());
            if (i == last) {
                // The return-position conformance names the CONTRACT: the
                // generic "expected X, got Y" reads like an inner expression
                // error, and callers (the corpus included) grep for the
                // declared-return wording.
                try {
                    typer.requireConforms(stmt.info(), declaredReturn);
                } catch (TypeInferenceException e) {
                    throw new TypeInferenceException("declares return type "
                            + simpleName(declaredReturn.type().typeName())
                            + " but body returns " + simpleName(stmt.info().type().typeName())
                            + " (" + e.getMessage() + ")");
                }
            }
            typed.add(stmt);
            if (stmt instanceof TypedLet let) {
                // bind for the following statements — WITH the let's syntax
                // (the Env.exprAlias channel: match branches through a
                // let-bound variable). The TypedLet fact PROVES the protocol
                // shape (LetChecker rejects anything but letFunction(name,
                // value)) — no name string compare (dispatch freeze).
                scope = body.get(i) instanceof com.legend.protocol.spec.AppliedFunction paf
                        && paf.parameters().size() == 2
                        ? scope.withLet(let.name(), let.value().info(),
                                paf.parameters().get(1))
                        : scope.with(let.name(), let.value().info());
            }
        }
        return new CompiledFunction(fn, typed);
    }

    /**
     * Type-check a standalone query expression against the model snapshot &mdash;
     * the public expression entry the {@link com.legend.Compiler} façade drives
     * (an empty environment, inference mode).
     */
    public TypedSpec typeExpression(ValueSpecification query) {
        List<TypedSpec> body = typeQueryBody(query);
        return body.get(body.size() - 1);
    }

    /** Type a bare expression under explicit variable bindings — a mapping
     * specification lambda body ({@code $this.isCancelled} with {@code
     * this} bound to the class; {@code $mapped->sum()} with the map's
     * result bound), typed once by the router. */
    public TypedSpec typeExpression(ValueSpecification expr,
            java.util.Map<String, com.legend.compiler.element.type.ExprType> bindings) {
        Env env = Env.empty();
        for (var e : bindings.entrySet()) {
            env = env.with(e.getKey(), e.getValue());
        }
        return typer.typeBody(expr, env, Expected.infer());
    }

    /**
     * Type a standalone query as a STATEMENT SEQUENCE (the corpus/engine
     * convention: queries arrive as zero-param lambdas — "|let a = ...; $a;"
     * — with lets binding forward). The full typed sequence is returned so
     * the lowering can thread let bindings; the query's value is the last
     * statement.
     */
    public List<TypedSpec> typeQueryBody(ValueSpecification query) {
        if (query instanceof com.legend.protocol.spec.LambdaFunction lf
                && lf.parameters().isEmpty()) {
            Env scope = Env.empty();
            List<TypedSpec> body = new ArrayList<>();
            int lastStmt = lf.body().size() - 1;
            for (int si = 0; si < lf.body().size(); si++) {
                ValueSpecification stmt = lf.body().get(si);
                // bind-once (family A): a let whose rhs is a DEFERRED
                // kind parks the raw syntax instead of dying typing it;
                // consuming checkers resolve through the alias channel.
                // Never the last statement — the query's value stays loud.
                com.legend.protocol.spec.CString dn;
                if (si < lastStmt
                        && (dn = SourceSubst.letName(stmt)) != null
                        && Typer.deferredLetRhs(
                                ((com.legend.protocol.spec.AppliedFunction) stmt)
                                        .parameters().get(1))) {
                    scope = scope.withDeferred(dn.value(),
                            ((com.legend.protocol.spec.AppliedFunction) stmt)
                                    .parameters().get(1));
                    continue;
                }
                TypedSpec typed = typer.typeBody(stmt, scope, Expected.infer());
                body.add(typed);
                if (typed instanceof TypedLet let) {
                    // same syntax-carrying bind as the function-body fold
                    // (TypedLet proves the letFunction(name, value) shape)
                    scope = stmt instanceof com.legend.protocol.spec.AppliedFunction paf
                            && paf.parameters().size() == 2
                            ? scope.withLet(let.name(), let.value().info(),
                                    selfAliasResolved(scope, let.name(),
                                            paf.parameters().get(1)))
                            : scope.with(let.name(), let.value().info());
                }
            }
            if (body.isEmpty()) {
                throw new TypeInferenceException("empty query lambda");
            }
            return body;
        }
        return List.of(typer.typeBody(query, Env.empty(), Expected.infer()));
    }

    /**
     * A STATEMENT-level self-alias — {@code let query = $query} spelled
     * over a name already let-bound in the same statement scope (an
     * inlined helper's parameter re-bound under its caller's let name;
     * real pure would refuse the duplicate let outright) — re-binds the
     * OUTER alias, never the bare variable: a self-referential alias made
     * every later structural consumer (generateTestData's checker) see a
     * Variable and wall (batch 72a, 2026-09-05). Lambda-LOCAL lets are
     * untouched — the plan printer's injected {@code let v = $v}
     * Allocation lets shadow on purpose (Typer's lambda fold).
     */
    private static com.legend.protocol.spec.ValueSpecification selfAliasResolved(
            Env scope, String name, com.legend.protocol.spec.ValueSpecification rhs) {
        return rhs instanceof com.legend.protocol.spec.Variable v
                && v.name().equals(name)
                && scope.exprAlias(name).isPresent()
                ? scope.resolveAlias(rhs) : rhs;
    }

    private static String simpleName(String typeName) {
        int idx = typeName.lastIndexOf("::");
        return idx < 0 ? typeName : typeName.substring(idx + 2);
    }

    /**
     * Type-check a single expression &mdash; delegates to the {@link Typer}.
     * Package-private: the workhorse of {@link #check}, exercised directly by
     * in-package tests; not part of the public surface.
     */
    TypedSpec typeBody(ValueSpecification vs, Env env, Expected expected) {
        return typer.typeBody(vs, env, expected);
    }
}
