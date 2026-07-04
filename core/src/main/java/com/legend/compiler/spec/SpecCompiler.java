package com.legend.compiler.spec;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.TypedParameter;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.parser.spec.ValueSpecification;

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

    public SpecCompiler(ModelContext ctx) {
        this.typer = new Typer(ctx, new InferenceKernel(ctx));
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
        CompiledFunction cf = check(fn);   // flat: check() never re-enters compile()
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
        for (TypedParameter p : fn.parameters()) {
            scope = scope.with(p.name(), new ExprType(p.type(), p.multiplicity()));
        }
        ExprType declaredReturn = new ExprType(fn.returnType(), fn.returnMultiplicity());

        List<TypedSpec> typed = new ArrayList<>(body.size());
        int last = body.size() - 1;
        for (int i = 0; i < body.size(); i++) {
            Expected expected = (i == last) ? Expected.check(declaredReturn) : Expected.infer();
            TypedSpec stmt = typeBody(body.get(i), scope, expected);
            typed.add(stmt);
            if (stmt instanceof TypedLet let) {
                scope = scope.with(let.name(), let.value().info());   // bind for the following statements
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
        return typer.typeBody(query, Env.empty(), Expected.infer());
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
