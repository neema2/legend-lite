// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;


import com.legend.platform.CoreFn;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.KeyExpression;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.NewInstance;
import com.legend.protocol.spec.NewInstanceCast;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * SOURCE-level β-substitution over {@link ValueSpecification} trees — the
 * compiler-side sibling of the harness's inliner. Pure lets are
 * non-recursive value bindings, so substituting a let's value for its
 * variable preserves semantics exactly; shadowing lambda parameters stop
 * substitution.
 *
 * <p>CAPTURE-AVOIDING (rebuild W0.6 push 1): a binder (a lambda parameter,
 * a lambda-body let) that is free in the value of an entry its scope reads
 * is renamed to {@code b_<k>}, the smallest {@code k} whose name no value
 * mentions, the scope does not read and no binder inside it spells. No
 * counter and no state: the same input gives the same names. A binder is
 * renamed only on a real hazard.
 */
public final class SourceSubst {

    private SourceSubst() {
    }

    /**
     * Fold a multi-statement lambda {@code [let*, final]} into a
     * single-expression lambda by inlining each let into everything after
     * it. Null when any non-terminal statement is not a let — the caller
     * keeps its loud wall (never a silently dropped statement).
     */
    static @com.legend.base.Nullable LambdaFunction inlineLets(LambdaFunction lam) {
        Map<String, ValueSpecification> env = new LinkedHashMap<>();
        for (int i = 0; i < lam.body().size() - 1; i++) {
            CString name = letName(lam.body().get(i));
            if (name == null) {
                return null;
            }
            env.put(name.value(), substitute(
                    ((AppliedFunction) lam.body().get(i)).parameters().get(1),
                    env));
        }
        return new LambdaFunction(lam.parameters(),
                List.of(substitute(lam.body().get(lam.body().size() - 1), env)));
    }

    /** bind-once (family E): the view an INLINE call site would present,
     * for checkers that consume their arguments STRUCTURALLY (the
     * test-data-generation and CSV-census folds): each argument resolves
     * through the env's let-alias channel, and a resolved lambda CLOSES
     * over the remaining in-scope aliases (its body may reference outer
     * lets — trees, refs). Referentially transparent, same soundness as
     * {@link Env#withLet}; evaluation semantics untouched (the consumers
     * fold at check time and never re-evaluate the binding). */
    static List<ValueSpecification> resolveStructuralArgs(
            List<ValueSpecification> params, Env env) {
        Map<String, ValueSpecification> aliases = env.aliases();
        List<ValueSpecification> out = new java.util.ArrayList<>(params.size());
        for (ValueSpecification p : params) {
            ValueSpecification r = env.resolveAlias(p);
            if (r != p && r instanceof com.legend.protocol.spec.PureCollection
                    && !aliases.isEmpty()) {
                // a let-bound COLLECTION of hoisted constructor lets
                // ([$_s2_hoisted, $_s3_hoisted]) is its values
                r = substitute(r, aliases);
            }
            if ((r instanceof LambdaFunction || r != p && tdgCtorShape(r))
                    && !aliases.isEmpty()) {
                // lambdas close over remaining aliases; TDG
                // data-constructor shapes adopt DEEP for the same
                // reason — their inner args may be let-bound too
                // (let ids = createRowIdentifier(...); let tri =
                // createTableRowIdentifiers($db, ..., $ids); ...)
                r = substitute(r, aliases);
            } else if (r != p && !(r instanceof LambdaFunction
                    || tdgCtorShape(r)
                    || r instanceof com.legend.protocol.spec
                            .PackageableElementPtr)) {
                // adopt only the shapes these checkers consume
                // structurally; anything else keeps its variable (and
                // the walk's existing channels)
                r = p;
            }
            out.add(r);
        }
        return out;
    }

    /** The TDG data VALUES — exactly the shapes
     * {@code TestDataGenerationNatives.classifyArg} consumes: instance
     * literals of the engine's row-identifier classes (the constructors
     * are PROGRAMS the statement inliner expands; their values arrive
     * through lets — {@code let tri = ^TableRowIdentifiers(table =
     * getTable(...), rowIdentifiers = $_s1_hoisted)}), the milestoning-
     * dates constructor call (a value function, spelled at the site),
     * and collections of them. EXACT-FQN identification (the standing
     * doctrine); the resolver has run by check time. */
    private static final java.util.Set<String> TDG_VALUE_CLASSES = java.util.Set.of(
            "meta::relational::testDataGeneration::TableRowIdentifiers",
            "meta::relational::testDataGeneration::RowIdentifier",
            "meta::relational::testDataGeneration::TemporalMilestoningDates");

    private static final String CREATE_TEMPORAL_MILESTONING_DATES =
            "meta::relational::testDataGeneration::createTemporalMilestoningDates";

    private static boolean tdgCtorShape(ValueSpecification v) {
        if (v instanceof com.legend.protocol.spec.PureCollection pc) {
            return !pc.values().isEmpty()
                    && pc.values().stream().allMatch(SourceSubst::tdgCtorShape);
        }
        // the parser spells ^Class(...) as new(<class ptr>, NewInstance)
        com.legend.protocol.spec.NewInstance ni = instanceOf(v);
        if (ni != null) {
            return TDG_VALUE_CLASSES.contains(ni.className());
        }
        return v instanceof AppliedFunction af
                && CREATE_TEMPORAL_MILESTONING_DATES.equals(af.function());
    }

    /** The instance literal {@code v} spells: a bare {@code NewInstance} or
     * the parser's {@code new(<class ptr>, NewInstance)} wrapper. */
    public static com.legend.protocol.spec.@com.legend.base.Nullable NewInstance instanceOf(
            ValueSpecification v) {
        if (v instanceof com.legend.protocol.spec.NewInstance ni) {
            return ni;
        }
        if (v instanceof AppliedFunction af
                && com.legend.compiler.ResolvedNames.form(af).orElse(null) == CoreFn.NEW
                && af.parameters().size() == 2
                && af.parameters().get(1) instanceof com.legend.protocol.spec.NewInstance ni2) {
            return ni2;
        }
        return null;
    }

    /** The ONE let-shape recognizer (protocol encoding, not user
     * vocabulary): {@code letFunction(<name>, <value>)} — shared by the
     * fold and the lambda-local shadow-stop so the spelling lives once. */
    public static @com.legend.base.Nullable CString letName(ValueSpecification st) {
        return st instanceof AppliedFunction lf
                && com.legend.compiler.ResolvedNames.form(lf).orElse(null) == CoreFn.LET
                && lf.parameters().size() == 2
                && lf.parameters().get(0) instanceof CString name
                ? name : null;
    }

    /** The binders of ONE scope (a lambda's parameters, or a lambda-body
     * let over the statements after it) passing over the live entries.
     * A binder can capture only when some value mentions its name, so
     * that is asked first; the scope's own reads and binders are read
     * once, and only for a binder that can. */
    private static final class Binder {
        private final Call call;
        private final Map<String, ValueSpecification> scope;
        private final Map<String, String> renamed;
        private final java.util.Collection<String> taken;
        private final java.util.function.Supplier<java.util.Set<String>> readsOf;
        private final java.util.function.Supplier<java.util.Set<String>> insideOf;
        private java.util.@com.legend.base.Nullable Set<String> reads;

        Binder(Call call, Map<String, ValueSpecification> scope, Map<String, String> renamed,
                java.util.Collection<String> taken,
                java.util.function.Supplier<java.util.Set<String>> readsOf,
                java.util.function.Supplier<java.util.Set<String>> insideOf) {
            this.call = call;
            this.scope = scope;
            this.renamed = renamed;
            this.taken = java.util.List.copyOf(taken);
            this.readsOf = readsOf;
            this.insideOf = insideOf;
        }

        /** Pass under binder {@code b} (already removed from the scope
         * by shadowing); returns its name below, renamed when it would
         * capture. */
        String bind(String b) {
            if (!call.mentioned().contains(b) && !renamed.containsValue(b)) {
                return b;
            }
            java.util.Set<String> read = reads;
            if (read == null) {
                read = readsOf.get();
                reads = read;
            }
            boolean captures = false;
            for (String entry : scope.keySet()) {
                captures |= read.contains(entry) && call.freeOf(entry).contains(b);
            }
            for (Map.Entry<String, String> e : renamed.entrySet()) {
                captures |= read.contains(e.getKey()) && e.getValue().equals(b);
            }
            if (!captures) {
                return b;
            }
            java.util.Set<String> inside = insideOf.get();
            for (int k = 1;; k++) {
                String fresh = b + "_" + k;
                if (!call.mentioned().contains(fresh) && !read.contains(fresh)
                        && !inside.contains(fresh) && !taken.contains(fresh)
                        && !renamed.containsValue(fresh)) {
                    renamed.put(b, fresh);
                    return fresh;
                }
            }
        }
    }

    /** A lambda-body let as a binder over the statements after it. */
    private static ValueSpecification bindLet(ValueSpecification st, String name, Call call,
            Map<String, ValueSpecification> scope, Map<String, String> renamed,
            java.util.Collection<String> taken, java.util.List<ValueSpecification> rest) {
        String bound = new Binder(call, scope, renamed, taken, () -> {
            java.util.Set<String> reads = freeVarsOfBody(rest);
            reads.remove(name);
            return reads;
        }, () -> {
            java.util.Set<String> inside = new java.util.LinkedHashSet<>();
            rest.forEach(x -> inside.addAll(binders(x)));
            return inside;
        }).bind(name);
        if (bound.equals(name)) {
            return st;
        }
        // a post-fold may have replaced the statement: only a let is renamed
        if (!(st instanceof AppliedFunction let) || !(letName(st) instanceof CString spelled)) {
            throw new IllegalStateException("a let '" + name
                    + "' must be renamed but its statement is no longer a let");
        }
        return let.withParameters(List.of(spelled.withValue(bound), let.parameters().get(1)));
    }

    /** The names {@code v} reads that no binder inside it binds (binders:
     * lambda parameters; a lambda-body let, for the statements after it). */
    static java.util.Set<String> freeVars(ValueSpecification v) {
        java.util.Set<String> out = new java.util.LinkedHashSet<>();
        freeVars(v, java.util.Set.of(), out);
        return out;
    }

    /** The free variables of a statement sequence. */
    static java.util.Set<String> freeVarsOfBody(java.util.List<ValueSpecification> statements) {
        java.util.Set<String> out = new java.util.LinkedHashSet<>();
        freeVarsOfBody(statements, java.util.Set.of(), out);
        return out;
    }

    private static void freeVarsOfBody(java.util.List<ValueSpecification> statements,
            java.util.Set<String> bound, java.util.Set<String> out) {
        java.util.Set<String> scope = bound;
        for (ValueSpecification st : statements) {
            freeVars(st, scope, out);
            CString ln = letName(st);
            if (ln != null && !scope.contains(ln.value())) {
                scope = new java.util.LinkedHashSet<>(scope);
                scope.add(ln.value());
            }
        }
    }

    private static void freeVars(ValueSpecification v, java.util.Set<String> bound,
            java.util.Set<String> out) {
        if (v instanceof Variable var) {
            if (!bound.contains(var.name())) {
                out.add(var.name());
            }
            return;
        }
        if (v instanceof LambdaFunction lf) {
            java.util.Set<String> inner = new java.util.LinkedHashSet<>(bound);
            lf.parameters().forEach(p -> inner.add(p.name()));
            freeVarsOfBody(lf.body(), inner, out);
            return;
        }
        for (ValueSpecification c : v.children()) {
            freeVars(c, bound, out);
        }
    }

    /** Every name a binder beneath {@code v} introduces. */
    private static java.util.Set<String> binders(ValueSpecification v) {
        java.util.Set<String> out = new java.util.LinkedHashSet<>();
        java.util.ArrayDeque<ValueSpecification> work = new java.util.ArrayDeque<>();
        work.add(v);
        while (!work.isEmpty()) {
            ValueSpecification n = work.poll();
            if (n instanceof LambdaFunction lf) {
                lf.parameters().forEach(p -> out.add(p.name()));
            }
            CString ln = letName(n);
            if (ln != null) {
                out.add(ln.value());
            }
            work.addAll(n.children());
        }
        return out;
    }

    /** F3.2c: the driver-injected POST-FOLD hook, offered every
     * substituted node post-order. Two chartered uses today, both
     * corpus-driver wiring: the METAPROGRAMMING fold (a quote-native's
     * argument becomes a literal only AFTER substitution; the payload
     * grammar is each native's own CONTRACT —
     * compileLegendValueSpecification = engine grammar per the engine's
     * LegendCompile.java:57 — never ambient context, so this layer needs
     * no dialect anywhere) and the harness's TDSNull wire-sentinel.
     * Null hook = plain substitution (product compiles; a dynamic
     * quote string stays an opaque call and walls at lowering — the
     * compiled platform folds statically-known code only). */
    @FunctionalInterface
    public interface PostFold {
        @com.legend.base.Nullable ValueSpecification fold(ValueSpecification substituted);
    }

    public static ValueSpecification substitute(ValueSpecification v,
            Map<String, ValueSpecification> env) {
        return substitute(v, env, null);
    }

    public static ValueSpecification substitute(ValueSpecification v,
            Map<String, ValueSpecification> env,
            @com.legend.base.Nullable PostFold folder) {
        if (env.isEmpty() && folder == null) {
            return v;
        }
        return subst(v, env, Map.of(), new Call(env, folder));
    }

    /** One {@link #substitute} call: the entries it started with and the
     * names their values mention free, read ONCE and only when the walk
     * first meets a binder (most substitutions pass no binder that any
     * value mentions, and then no value is ever read). */
    private static final class Call {
        private final Map<String, ValueSpecification> entries;
        private final @com.legend.base.Nullable PostFold folder;
        private final Map<String, java.util.Set<String>> free = new java.util.HashMap<>();
        private java.util.@com.legend.base.Nullable Set<String> mentioned;

        Call(Map<String, ValueSpecification> entries, @com.legend.base.Nullable PostFold folder) {
            this.entries = entries;
            this.folder = folder;
        }

        /** The free variables of one entry's value. */
        java.util.Set<String> freeOf(String entry) {
            return free.computeIfAbsent(entry, e -> {
                ValueSpecification value = entries.get(e);
                return value == null ? java.util.Set.of() : freeVars(value);
            });
        }

        /** Every name some value mentions free. */
        java.util.Set<String> mentioned() {
            java.util.Set<String> m = mentioned;
            if (m == null) {
                m = new java.util.LinkedHashSet<>();
                for (String e : entries.keySet()) {
                    m.addAll(freeOf(e));
                }
                mentioned = m;
            }
            return m;
        }
    }

    /** {@code renames}: the binders renamed above this position (a read
     * of one keeps its own position and takes the new name). */
    private static ValueSpecification subst(ValueSpecification v,
            Map<String, ValueSpecification> env, Map<String, String> renames, Call call) {
        PostFold folder = call.folder;
        if (env.isEmpty() && renames.isEmpty() && folder == null) {
            return v;
        }
        ValueSpecification r = switch (v) {
            case Variable var -> {
                String to = renames.get(var.name());
                yield to != null ? var.renamed(to) : env.getOrDefault(var.name(), var);
            }
            case AppliedFunction af -> af.withParameters(
                    af.parameters().stream()
                            .map(p -> subst(p, env, renames, call))
                            .toList());
            // the owner class (older JSON's written detail) rides through; the position does not, as before
            case AppliedProperty ap -> new AppliedProperty(
                    subst(ap.receiver(), env, renames, call), ap.property(), null, ap.ownerClass());
            case LambdaFunction lf -> {
                Map<String, ValueSpecification> inner = new LinkedHashMap<>(env);
                Map<String, String> renamed = new LinkedHashMap<>(renames);
                lf.parameters().forEach(p -> {
                    inner.remove(p.name());
                    renamed.remove(p.name());
                });
                if (inner.isEmpty() && renamed.isEmpty()) {
                    yield lf;
                }
                Binder binder = new Binder(call, inner, renamed, renames.values(),
                        () -> freeVars(lf), () -> binders(lf));
                java.util.List<Variable> params =
                        new java.util.ArrayList<>(lf.parameters().size());
                for (Variable p : lf.parameters()) {
                    String name = binder.bind(p.name());
                    params.add(name.equals(p.name()) ? p : p.renamed(name));
                }
                // F3.2b: a LAMBDA-LOCAL let shadows the outer binding for
                // the statements BELOW it (real pure scoping — the
                // plan-printer's injected Allocation lets rely on it; the
                // harness engine had this right and the owner did not)
                java.util.List<ValueSpecification> body =
                        new java.util.ArrayList<>(lf.body().size());
                for (int i = 0; i < lf.body().size(); i++) {
                    ValueSpecification st = subst(lf.body().get(i), inner, renamed, call);
                    CString ln = letName(lf.body().get(i));
                    if (ln != null) {
                        inner.remove(ln.value());
                        renamed.remove(ln.value());
                        st = bindLet(st, ln.value(), call, inner, renamed, renames.values(),
                                lf.body().subList(i + 1, lf.body().size()));
                    }
                    body.add(st);
                }
                yield new LambdaFunction(params, body);
            }
            case PureCollection pc -> new PureCollection(pc.values().stream()
                    .map(x -> subst(x, env, renames, call)).toList());
            // LOSSLESS rebuild (F3.2c): the 5-arg ctor silently dropped
            // qualified/colType/stereotypes — a substituted ColSpec must
            // carry every component it arrived with
            case ColSpec cs -> new ColSpec(cs.name(),
                    cs.function1() == null ? null
                            : (LambdaFunction) subst(cs.function1(), env, renames, call),
                    cs.function2() == null ? null
                            : (LambdaFunction) subst(cs.function2(), env, renames, call),
                    cs.alias(),
                    cs.args().stream().map(a -> subst(a, env, renames, call))
                            .toList(),
                    cs.qualified(), cs.pos(), cs.colType(), cs.colTypeMult(),
                    cs.stereotypes(), cs.taggedValues());
            case ColSpecArray ca -> new ColSpecArray(ca.colSpecs().stream()
                    .map(c -> (ColSpec) subst(c, env, renames, call)).toList());
            case NewInstance ni -> {
                java.util.List<NewInstance.KeyBinding> props =
                        ni.properties().stream().map(b ->
                                new NewInstance.KeyBinding(b.key(),
                                        new KeyExpression(
                                                subst(b.expression()
                                                        .value(), env, renames, call),
                                                b.expression().isAdd(),
                                                b.expression().isLocal())))
                                .toList();
                yield new NewInstance(ni.className(), ni.typeArguments(), props);
            }
            case NewInstanceCast nc -> new NewInstanceCast(nc.className(),
                    nc.typeArguments(), subst(nc.src(), env, renames, call),
                    nc.targetSetId());
            // a folded quote/eval carrier is a CLOSED term (built from
            // literals — no free variables); substituting through it would
            // re-fold its own original
            case com.legend.protocol.spec.QuotedTreeCall q -> q;
            case com.legend.protocol.spec.QuotedGrammarCall q -> q;
            // leaves pass; any composite not special-cased above recurses
            default -> v.mapChildren(x -> subst(x, env, renames, call));
        };
        if (folder != null) {
            ValueSpecification f = folder.fold(r);
            if (f != null) {
                return f;
            }
        }
        return r;
    }
}
