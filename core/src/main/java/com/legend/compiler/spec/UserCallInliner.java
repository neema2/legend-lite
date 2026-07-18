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
    private final java.util.function.UnaryOperator<TypedSpec> hook;
    private final ArrayDeque<String> stack = new ArrayDeque<>();
    private final ArrayDeque<String> names = new ArrayDeque<>();
    private int fresh;

    public UserCallInliner(SpecCompiler specs) {
        this(specs, null);
    }

    /**
     * {@code hook}: an OPTIONAL per-node pre-rewrite (the statement
     * executor's result-frame splice rides this walker instead of
     * duplicating the vocabulary switch). Fired before the standard
     * rewrite at every node; returning a DIFFERENT node replaces it and
     * the rewrite recurses into the replacement — the hook must return
     * the argument itself (same reference) when it does not apply.
     */
    public UserCallInliner(SpecCompiler specs,
            java.util.function.UnaryOperator<TypedSpec> hook) {
        this.specs = Objects.requireNonNull(specs, "specs");
        this.hook = hook;
    }

    /** Inline every user call in a query body (statements = lets + result). */
    public List<TypedSpec> inlineBody(List<TypedSpec> body) {
        // The fresh namespace must clear every user-written _i<N> — a query
        // variable literally named _i0 would otherwise be CAPTURED by an
        // α-renamed callee binder (audit). Callee bodies are closed, so the
        // query body is the only collision source.
        for (TypedSpec stmt : body) {
            reserveFreshNames(stmt);
        }
        // QUERY-level lets β-reduce exactly like callee lets — binders die
        // in the one substitution pass. A relation-typed let ($t = #TDS…#)
        // splices its pipeline into every use; downstream phases never see
        // a let. A TRAILING let IS its value (real pure: the let statement
        // yields it). KNOWN TRADE (audit): a let used twice EVALUATES twice
        // in SQL — for a non-deterministic row set (limit with no total
        // order) the two splices may disagree where real pure's
        // single-evaluation binding could not; CTE sharing is the future fix.
        Map<String, TypedSpec> scope = new LinkedHashMap<>();
        for (int i = 0; i < body.size() - 1; i++) {
            if (!(body.get(i) instanceof TypedLet let)) {
                throw new NotImplementedException(
                        "only let statements may precede the query expression");
            }
            scope.put(let.name(), rewrite(let.value(), scope));
        }
        TypedSpec last = body.get(body.size() - 1);
        TypedSpec root = last instanceof TypedLet let
                ? rewrite(let.value(), scope)
                : rewrite(last, scope);
        return List.of(root);
    }

    private void reserveFreshNames(TypedSpec n) {
        switch (n) {
            case TypedVariable v -> bumpPast(v.name());
            case TypedLet let -> bumpPast(let.name());
            case TypedLambda l -> l.parameters().forEach(this::bumpPast);
            default -> { }
        }
        for (TypedSpec c : n.children()) {
            reserveFreshNames(c);
        }
    }

    private void bumpPast(String name) {
        if (name.startsWith("_i")) {
            try {
                fresh = Math.max(fresh, Integer.parseInt(name.substring(2)) + 1);
            } catch (NumberFormatException ignored) {
                // _iFoo is outside the fresh namespace
            }
        }
    }

    // =====================================================================
    // The call frame
    // =====================================================================

    private TypedSpec inlineCall(TypedUserCall call, Map<String, TypedSpec> env) {
        List<TypedSpec> args = new ArrayList<>(call.args().size());
        for (TypedSpec a : call.args()) {
            args.add(rewrite(a, env));
        }
        // signatureKey identifies the OVERLOAD — name/arity conflated two
        // same-arity overloads into a false recursion (audit).
        String key = call.callee().signatureKey();
        String shown = call.callee().qualifiedName() + "/"
                + call.callee().parameters().size();
        if (stack.contains(key)) {
            List<String> path = new ArrayList<>(names);
            java.util.Collections.reverse(path);
            throw new NotImplementedException("recursion cycle involving " + shown
                    + " (" + String.join(" -> ", path) + " -> " + shown
                    + ") — recursive functions cannot lower to SQL");
        }
        stack.push(key);
        names.push(shown);
        try {
            List<TypedSpec> body = specs.compile(call.callee()).body();
            Map<String, TypedSpec> callEnv = new LinkedHashMap<>();
            // A relation param accepts a SUPERSET schema (covariant call);
            // the spliced body then carries the caller's extra columns in
            // SQL while the call site is typed by the DECLARED return —
            // conform by EMISSION with a select down to the declared columns.
            boolean widened = false;
            for (int i = 0; i < call.callee().parameters().size(); i++) {
                callEnv.put(call.callee().parameters().get(i).name(), args.get(i));
                if (rowType(call.callee().parameters().get(i).type()) instanceof
                            com.legend.compiler.element.type.Type.RelationType dp
                        && rowType(args.get(i).info().type()) instanceof
                            com.legend.compiler.element.type.Type.RelationType ap
                        && ap.columns().size() > dp.columns().size()) {
                    widened = true;
                }
            }
            TypedSpec reduced = reduceStatements(body, callEnv);
            if (widened && call.info().type()
                    instanceof com.legend.compiler.element.type.Type.RelationType rt) {
                reduced = new com.legend.compiler.spec.typed.TypedSelect(reduced,
                        rt.columns().stream()
                                .map(com.legend.compiler.element.type.Type.Column::name)
                                .toList(),
                        call.info());
            }
            return reduced;
        } finally {
            stack.pop();
            names.pop();
        }
    }

    /** The row type of a relation-valued type: bare RelationType, or Relation<(...)>. */
    private static com.legend.compiler.element.type.Type rowType(
            com.legend.compiler.element.type.Type t) {
        if (t instanceof com.legend.compiler.element.type.Type.GenericType g
                && g.rawFqn().equals("meta::pure::metamodel::relation::Relation")
                && g.arguments().size() == 1) {
            return g.arguments().get(0);
        }
        return t;
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
        // A TRAILING let IS its value (real pure: the let statement yields
        // it) — `{ let r = $x + 100 }` returns the sum, and no let node
        // survives into H/I.
        TypedSpec last = body.get(body.size() - 1);
        return last instanceof TypedLet let
                ? rewrite(let.value(), scope)
                : rewrite(last, scope);
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
        if (hook != null) {
            TypedSpec h = hook.apply(n);
            if (h != n) {
                return rewrite(h, env);
            }
        }
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
            // match is STATICALLY DISPATCHED (the checker picked the branch)
            // — the node IS a β-redex: substitute the input (and the extra
            // argument) into the chosen body and the match disappears; the
            // lowerer never needs a match arm.
            case TypedMatch m -> {
                TypedSpec input = rewrite(m.input(), env);
                Optional<TypedSpec> extra = m.extra().map(e -> rewrite(e, env));
                Map<String, TypedSpec> inner = new LinkedHashMap<>(env);
                inner.put(m.param(), input);
                if (m.extraParam().isPresent()) {
                    inner.put(m.extraParam().get(), extra.orElse(input));
                }
                yield rewrite(m.body(), inner);
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
            case com.legend.compiler.spec.typed.TypedMilestonedAccess ma ->
                    new com.legend.compiler.spec.typed.TypedMilestonedAccess(
                            rewrite(ma.source(), env), ma.property(),
                            list(ma.dates(), env), ma.sweep(), ma.info());
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
            case com.legend.compiler.spec.typed.TypedCopyInstance cp -> {
                Map<String, TypedSpec> ov = new LinkedHashMap<>();
                cp.overrides().forEach((k, v) -> ov.put(k, rewrite(v, env)));
                yield new com.legend.compiler.spec.typed.TypedCopyInstance(
                        rewrite(cp.source(), env), cp.classFqn(), ov, cp.info());
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
            case com.legend.compiler.spec.typed.TypedCollectionRelation cr ->
                    new com.legend.compiler.spec.typed.TypedCollectionRelation(
                            rewrite(cr.value(), env), cr.column(), cr.info());
            case TypedFlatten f -> new TypedFlatten(rewrite(f.source(), env),
                    f.column(), f.info());
            case TypedPivot p -> new TypedPivot(rewrite(p.source(), env),
                    p.pivotColumns(),
                    p.values().stream().map(v -> rewrite(v, env)).toList(),
                    aggCols(p.aggs(), env), p.info());
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
