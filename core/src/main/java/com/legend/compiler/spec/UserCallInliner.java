package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedAggCol;
import com.legend.compiler.spec.typed.TypedAggColSpec;
import com.legend.compiler.spec.typed.TypedAggColSpecArray;
import com.legend.compiler.spec.typed.TypedAggregate;
import com.legend.compiler.spec.typed.TypedAsOfJoin;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCDate;
import com.legend.compiler.spec.typed.TypedCDecimal;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCLatestDate;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCTime;
import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedColSpec;
import com.legend.compiler.spec.typed.TypedColSpecArray;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedConcatenate;
import com.legend.compiler.spec.typed.TypedDistinct;
import com.legend.compiler.spec.typed.TypedDrop;
import com.legend.compiler.spec.typed.TypedEnumValue;
import com.legend.compiler.spec.typed.TypedEval;
import com.legend.compiler.spec.typed.TypedExtend;
import com.legend.compiler.spec.typed.TypedExtendAgg;
import com.legend.compiler.spec.typed.TypedExtendWindow;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedFlatten;
import com.legend.compiler.spec.typed.TypedFold;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedFuncColSpec;
import com.legend.compiler.spec.typed.TypedFuncColSpecArray;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedGraphFetch;
import com.legend.compiler.spec.typed.TypedGroupBy;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedJoin;
import com.legend.compiler.spec.typed.TypedJoinSlot;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedMatch;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNavigate;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedNewInstanceCast;
import com.legend.compiler.spec.typed.TypedOver;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedPivot;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedRename;
import com.legend.compiler.spec.typed.TypedSelect;
import com.legend.compiler.spec.typed.TypedSerialize;
import com.legend.compiler.spec.typed.TypedSerializeGraph;
import com.legend.compiler.spec.typed.TypedSlice;
import com.legend.compiler.spec.typed.TypedSort;
import com.legend.compiler.spec.typed.TypedSortBy;
import com.legend.compiler.spec.typed.TypedSortInfo;
import com.legend.compiler.spec.typed.TypedSourceUrl;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.compiler.spec.typed.TypedTds;
import com.legend.compiler.spec.typed.TypedTypeRef;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.compiler.spec.typed.TypedWrite;
import com.legend.error.NotImplementedException;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Phase G&frac12; &mdash; whole-program &beta;-reduction of user-defined
 * function calls (the monomorphization family: SQL has no call frame, so
 * every {@link TypedUserCall} must be spliced into its caller BEFORE the
 * store resolver and the lowerer see the tree). After this pass no
 * {@code TypedUserCall} exists, and H's demand analysis sees THROUGH
 * function boundaries by construction (a callee navigating
 * {@code $p.address.street} contributes join demand like inline code).
 *
 * <p>Rules:
 * <ul>
 *   <li><b>&beta;</b> &mdash; parameter occurrences are replaced by the
 *       (already-rewritten) argument expressions; every node keeps its
 *       G-computed {@link com.legend.compiler.element.type.ExprType}
 *       (arguments conform by overload resolution; narrower replacements
 *       are conservative &mdash; the no-restamp discipline).</li>
 *   <li><b>lets reduce</b> &mdash; a callee's intermediate
 *       {@code let x = e;} statements substitute forward (Pure lets are
 *       single-assignment), so a call becomes ONE expression. Query-level
 *       lets are untouched (the lowerer owns those).</li>
 *   <li><b>&alpha;-hygiene</b> &mdash; INSIDE an inlined body every binder
 *       (lambda parameter, let name, match parameter) is renamed to a fresh
 *       {@code _i&lt;N&gt;}, unconditionally: an argument's free variables can
 *       never be captured. (Occurrences keep their own infos.)</li>
 *   <li><b>recursion is loud</b> &mdash; a call cycle throws naming the
 *       path ({@code f/1 -> g/2 -> f/1}); SQL cannot express it.</li>
 *   <li><b>eval of a literal lambda</b> &mdash; after substitution a
 *       higher-order parameter becomes a lambda literal;
 *       {@code $f-&gt;eval($x)} then &beta;-reduces the same way. Eval of
 *       anything else passes through (its own wall downstream).</li>
 * </ul>
 */
public final class UserCallInliner {

    private final SpecCompiler specs;
    private final ArrayDeque<String> stack = new ArrayDeque<>();
    private int fresh;

    public UserCallInliner(SpecCompiler specs) {
        this.specs = Objects.requireNonNull(specs, "specs");
    }

    /** Inline every user call in a query body (statements = lets + result). */
    public List<TypedSpec> inlineBody(List<TypedSpec> body) {
        List<TypedSpec> out = new ArrayList<>(body.size());
        for (TypedSpec stmt : body) {
            out.add(rewrite(stmt, Map.of()));
        }
        return out;
    }

    // =====================================================================
    // The call frame
    // =====================================================================

    private TypedSpec inlineCall(TypedUserCall call, Map<String, TypedSpec> env) {
        List<TypedSpec> args = new ArrayList<>(call.args().size());
        for (TypedSpec a : call.args()) {
            args.add(rewrite(a, env));
        }
        String key = call.callee().qualifiedName() + "/" + call.callee().parameters().size();
        if (stack.contains(key)) {
            List<String> path = new ArrayList<>(stack);
            java.util.Collections.reverse(path);
            throw new NotImplementedException("recursion cycle involving " + key
                    + " (" + String.join(" -> ", path) + " -> " + key
                    + ") — recursive functions cannot lower to SQL");
        }
        stack.push(key);
        try {
            List<TypedSpec> body = specs.compile(call.callee()).body();
            Map<String, TypedSpec> callEnv = new LinkedHashMap<>();
            for (int i = 0; i < call.callee().parameters().size(); i++) {
                callEnv.put(call.callee().parameters().get(i).name(), args.get(i));
            }
            return reduceStatements(body, callEnv);
        } finally {
            stack.pop();
        }
    }

    /**
     * A statement list under an environment: intermediate lets substitute
     * FORWARD (their values see the bindings so far); the last statement is
     * the value. One expression comes out.
     */
    private TypedSpec reduceStatements(List<TypedSpec> body, Map<String, TypedSpec> env) {
        Map<String, TypedSpec> scope = new LinkedHashMap<>(env);
        for (int i = 0; i < body.size() - 1; i++) {
            if (!(body.get(i) instanceof TypedLet let)) {
                throw new NotImplementedException("a non-let intermediate statement ("
                        + body.get(i).getClass().getSimpleName()
                        + ") in an inlined function body is not supported");
            }
            scope.put(let.name(), rewrite(let.value(), scope));
        }
        return rewrite(body.get(body.size() - 1), scope);
    }

    /** &beta;-reduce {@code eval(<literal lambda>, args)}. */
    private TypedSpec reduceEval(TypedEval ev, List<TypedSpec> args, TypedLambda lam) {
        if (lam.parameters().size() != args.size()) {
            throw new IllegalStateException("eval arity mismatch after inlining: "
                    + lam.parameters().size() + " parameter(s), " + args.size()
                    + " argument(s) — G should have rejected this");
        }
        Map<String, TypedSpec> env = new LinkedHashMap<>();
        for (int i = 0; i < args.size(); i++) {
            env.put(lam.parameters().get(i), args.get(i));
        }
        return reduceStatements(lam.body(), env);
    }

    // =====================================================================
    // The rewriter — exhaustive over the sealed vocabulary (javac-enforced)
    // =====================================================================

    private TypedSpec rewrite(TypedSpec n, Map<String, TypedSpec> env) {
        return switch (n) {
            case TypedUserCall uc -> inlineCall(uc, env);

            case TypedEval ev -> {
                TypedSpec fn = rewrite(ev.fn(), env);
                List<TypedSpec> args = list(ev.args(), env);
                yield fn instanceof TypedLambda lam
                        ? reduceEval(ev, args, lam)
                        : new TypedEval(fn, args, ev.info());
            }

            case TypedVariable v -> {
                TypedSpec r = env.get(v.name());
                if (r == null) {
                    yield v;
                }
                // An α-rename preserves the OCCURRENCE's own info; a real
                // substitution splices the argument/let expression verbatim.
                yield r instanceof TypedVariable rv
                        ? new TypedVariable(rv.name(), v.info())
                        : r;
            }

            // BINDERS — α-fresh inside inlined bodies (env non-empty),
            // untouched at the query's own level.
            case TypedLambda l -> lambda(l, env);
            case TypedMatch m -> {
                TypedSpec input = rewrite(m.input(), env);
                Map<String, TypedSpec> inner = new LinkedHashMap<>(env);
                String p = bind(m.param(), inner, input.info());
                TypedSpec mBody = rewrite(m.body(), inner);
                Optional<String> extraParam = m.extraParam();
                Optional<TypedSpec> extra = m.extra();
                if (extraParam.isPresent()) {
                    Map<String, TypedSpec> inner2 = new LinkedHashMap<>(env);
                    String p2 = bind(extraParam.get(), inner2, input.info());
                    extraParam = Optional.of(p2);
                    extra = extra.map(e -> rewrite(e, inner2));
                }
                yield new TypedMatch(input, p, mBody, extraParam, extra, m.info());
            }
            case TypedLet let -> {
                // Reached only for QUERY-LEVEL lets (callee lets reduce in
                // reduceStatements) and lets inside lambda bodies, which
                // lambda() handles statement-wise. env must not know it.
                if (env.containsKey(let.name())) {
                    throw new IllegalStateException("resolver bug: a let name '"
                            + let.name() + "' collided with an inlining binding");
                }
                yield new TypedLet(let.name(), rewrite(let.value(), env), let.info());
            }

            // Literals and leaves.
            case TypedCInteger c -> c;
            case TypedCString c -> c;
            case TypedCBoolean c -> c;
            case TypedCFloat c -> c;
            case TypedCDecimal c -> c;
            case TypedCDate c -> c;
            case TypedCTime c -> c;
            case TypedCLatestDate c -> c;
            case TypedEnumValue c -> c;
            case TypedColSpec c -> c;
            case TypedColSpecArray c -> c;
            case TypedSortInfo c -> c;
            case TypedTypeRef c -> c;
            case TypedTds c -> c;
            case TypedSourceUrl c -> c;
            case TypedTableReference c -> c;
            case TypedPackageableRef c -> c;
            case TypedGetAll g -> g.milestoning().isEmpty() ? g
                    : new TypedGetAll(g.classFqn(), list(g.milestoning(), env), g.info());

            // Expressions.
            case TypedPropertyAccess p -> new TypedPropertyAccess(
                    rewrite(p.source(), env), p.property(), p.info());
            case TypedNativeCall c -> new TypedNativeCall(c.callee(),
                    list(c.args(), env), c.info());
            case TypedCollection c -> new TypedCollection(list(c.elements(), env), c.info());
            case TypedIf i -> new TypedIf(rewrite(i.condition(), env),
                    rewrite(i.thenBranch(), env),
                    i.elseBranch().map(e -> rewrite(e, env)), i.info());
            case TypedCast c -> new TypedCast(rewrite(c.source(), env), c.target(), c.info());
            case TypedNewInstance ni -> {
                Map<String, TypedSpec> props = new LinkedHashMap<>();
                ni.properties().forEach((k, v) -> props.put(k, rewrite(v, env)));
                yield new TypedNewInstance(ni.classFqn(), props, ni.info());
            }
            case TypedNewInstanceCast nc -> new TypedNewInstanceCast(nc.classFqn(),
                    rewrite(nc.source(), env), nc.info());
            case TypedFold f -> new TypedFold(rewrite(f.source(), env),
                    lambda(f.reducer(), env), rewrite(f.init(), env),
                    f.strategy(), f.info());
            case TypedFuncColSpec fc -> new TypedFuncColSpec(
                    funcCol(fc.col(), env), fc.info());
            case TypedFuncColSpecArray fa -> new TypedFuncColSpecArray(
                    fa.cols().stream().map(c -> funcCol(c, env)).toList(), fa.info());
            case TypedAggColSpec ac -> new TypedAggColSpec(aggCol(ac.col(), env), ac.info());
            case TypedAggColSpecArray aa -> new TypedAggColSpecArray(
                    aa.cols().stream().map(c -> aggCol(c, env)).toList(), aa.info());

            // Relation operators.
            case TypedFilter f -> new TypedFilter(rewrite(f.source(), env),
                    lambda(f.predicate(), env), f.info());
            case TypedMap m -> new TypedMap(rewrite(m.source(), env),
                    lambda(m.mapper(), env), m.info());
            case TypedProject p -> new TypedProject(rewrite(p.source(), env),
                    funcCols(p.columns(), env), p.info());
            case TypedExtend e -> new TypedExtend(rewrite(e.source(), env),
                    funcCols(e.columns(), env), e.info());
            case TypedExtendAgg e -> new TypedExtendAgg(rewrite(e.source(), env),
                    aggCols(e.aggs(), env), e.info());
            case TypedExtendWindow w -> new TypedExtendWindow(rewrite(w.source(), env),
                    (TypedOver) rewrite(w.window(), env),
                    funcCols(w.columns(), env), aggCols(w.aggs(), env), w.info());
            case TypedOver o -> new TypedOver(o.partitions(), o.sortKeys(),
                    o.frame().map(f -> rewrite(f, env)), o.info());
            case TypedGroupBy g -> new TypedGroupBy(rewrite(g.source(), env),
                    g.keys().stream().map(k -> new TypedGroupBy.GroupKey(k.column(),
                            k.fn().map(f -> lambda(f, env)))).toList(),
                    aggCols(g.aggs(), env), g.info());
            case TypedAggregate a -> new TypedAggregate(rewrite(a.source(), env),
                    aggCols(a.aggs(), env), a.info());
            case TypedSort s -> new TypedSort(rewrite(s.source(), env), s.keys(), s.info());
            case TypedSortBy sb -> new TypedSortBy(rewrite(sb.source(), env),
                    lambda(sb.key(), env), sb.ascending(), sb.info());
            case TypedSelect s -> new TypedSelect(rewrite(s.source(), env),
                    s.columns(), s.info());
            case TypedRename r -> new TypedRename(rewrite(r.source(), env),
                    r.renames(), r.info());
            case TypedDistinct d -> new TypedDistinct(rewrite(d.source(), env),
                    d.columns(), d.info());
            case TypedConcatenate c -> new TypedConcatenate(rewrite(c.left(), env),
                    rewrite(c.right(), env), c.info());
            case TypedLimit l -> new TypedLimit(rewrite(l.source(), env),
                    rewrite(l.count(), env), l.info());
            case TypedDrop d -> new TypedDrop(rewrite(d.source(), env),
                    rewrite(d.count(), env), d.info());
            case TypedSlice s -> new TypedSlice(rewrite(s.source(), env),
                    rewrite(s.start(), env), rewrite(s.stop(), env), s.info());
            case TypedFlatten f -> new TypedFlatten(rewrite(f.source(), env),
                    f.column(), f.info());
            case TypedPivot p -> new TypedPivot(rewrite(p.source(), env),
                    p.pivotColumns(), aggCols(p.aggs(), env), p.info());
            case TypedJoin j -> new TypedJoin(rewrite(j.left(), env),
                    rewrite(j.right(), env), j.kind(),
                    lambda(j.condition(), env), j.prefix(), j.info());
            case TypedAsOfJoin j -> new TypedAsOfJoin(rewrite(j.left(), env),
                    rewrite(j.right(), env), lambda(j.match(), env),
                    j.condition().map(c -> lambda(c, env)), j.prefix(), j.info());
            case TypedJoinSlot js -> new TypedJoinSlot(rewrite(js.source(), env),
                    js.alias(), rewrite(js.target(), env),
                    lambda(js.condition(), env), js.info());
            case TypedNavigate nav -> new TypedNavigate(rewrite(nav.source(), env),
                    nav.alias(), rewrite(nav.target(), env),
                    lambda(nav.predicate(), env), nav.form(), nav.info());
            case TypedFrom f -> new TypedFrom(rewrite(f.source(), env),
                    f.mapping(), f.runtime(), f.info());
            case TypedWrite w -> new TypedWrite(rewrite(w.source(), env),
                    w.destination().map(d -> rewrite(d, env)), w.info());
            case TypedGraphFetch gf -> new TypedGraphFetch(rewrite(gf.source(), env),
                    gf.tree(), gf.info());
            case TypedSerialize sz -> new TypedSerialize(rewrite(sz.source(), env),
                    sz.tree(), sz.config().map(c -> rewrite(c, env)), sz.info());
            // Resolver OUTPUT vocabulary — never present pre-H, but the
            // switch stays total so a pipeline reordering fails loud here
            // rather than silently skipping bodies.
            case TypedSerializeGraph sg -> throw new IllegalStateException(
                    "TypedSerializeGraph reached the inliner — it runs BEFORE the store resolver");
        };
    }

    // =====================================================================
    // Binders and carriers
    // =====================================================================

    /**
     * A lambda under {@code env}: inside an inlined body (non-empty env)
     * every parameter α-renames to a fresh name and body LETS bind
     * statement-wise (renamed too); at the query's own level parameters and
     * let names stay.
     */
    private TypedLambda lambda(TypedLambda l, Map<String, TypedSpec> env) {
        if (env.isEmpty()) {
            List<TypedSpec> body = new ArrayList<>(l.body().size());
            for (TypedSpec stmt : l.body()) {
                body.add(rewrite(stmt, env));
            }
            return new TypedLambda(l.parameters(), body, l.info());
        }
        Map<String, TypedSpec> inner = new LinkedHashMap<>(env);
        var fnType = (com.legend.compiler.element.type.Type.FunctionType) l.info().type();
        List<String> params = new ArrayList<>(l.parameters().size());
        for (int i = 0; i < l.parameters().size(); i++) {
            var p = fnType.params().get(i);
            params.add(bind(l.parameters().get(i), inner,
                    new com.legend.compiler.element.type.ExprType(
                            p.type(), p.multiplicity())));
        }
        List<TypedSpec> body = new ArrayList<>(l.body().size());
        for (TypedSpec stmt : l.body()) {
            if (stmt instanceof TypedLet let) {
                TypedSpec value = rewrite(let.value(), inner);
                String renamed = bind(let.name(), inner, let.value().info());
                body.add(new TypedLet(renamed, value, let.info()));
                continue;
            }
            body.add(rewrite(stmt, inner));
        }
        return new TypedLambda(params, body, l.info());
    }

    /** Fresh-bind {@code name} into {@code scope}; returns the fresh name. */
    private String bind(String name, Map<String, TypedSpec> scope,
            com.legend.compiler.element.type.ExprType info) {
        String renamed = "_i" + fresh++;
        scope.put(name, new TypedVariable(renamed, info));
        return renamed;
    }

    private List<TypedSpec> list(List<TypedSpec> ns, Map<String, TypedSpec> env) {
        List<TypedSpec> out = new ArrayList<>(ns.size());
        for (TypedSpec n : ns) {
            out.add(rewrite(n, env));
        }
        return out;
    }

    private TypedFuncCol funcCol(TypedFuncCol c, Map<String, TypedSpec> env) {
        return new TypedFuncCol(c.name(), lambda(c.fn(), env));
    }

    private List<TypedFuncCol> funcCols(List<TypedFuncCol> cs, Map<String, TypedSpec> env) {
        return cs.stream().map(c -> funcCol(c, env)).toList();
    }

    private TypedAggCol aggCol(TypedAggCol c, Map<String, TypedSpec> env) {
        return new TypedAggCol(c.name(), lambda(c.map(), env), lambda(c.reduce(), env));
    }

    private List<TypedAggCol> aggCols(List<TypedAggCol> cs, Map<String, TypedSpec> env) {
        return cs.stream().map(c -> aggCol(c, env)).toList();
    }
}
