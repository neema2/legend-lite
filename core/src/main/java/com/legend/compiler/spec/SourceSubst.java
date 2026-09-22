// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

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
                && CoreFn.of(af.function()).orElse(null) == CoreFn.NEW
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
                && CoreFn.of(lf.function()).orElse(null) == CoreFn.LET
                && lf.parameters().size() == 2
                && lf.parameters().get(0) instanceof CString name
                ? name : null;
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
        ValueSpecification r = switch (v) {
            case Variable var -> env.getOrDefault(var.name(), var);
            case AppliedFunction af -> af.withParameters(
                    af.parameters().stream()
                            .map(p -> substitute(p, env, folder))
                            .toList());
            case AppliedProperty ap -> new AppliedProperty(
                    substitute(ap.receiver(), env, folder), ap.property());
            case LambdaFunction lf -> {
                Map<String, ValueSpecification> inner = new LinkedHashMap<>(env);
                lf.parameters().forEach(p -> inner.remove(p.name()));
                if (inner.isEmpty()) {
                    yield lf;
                }
                // F3.2b: a LAMBDA-LOCAL let shadows the outer binding for
                // the statements BELOW it (real pure scoping — the
                // plan-printer's injected Allocation lets rely on it; the
                // harness engine had this right and the owner did not)
                java.util.List<ValueSpecification> body =
                        new java.util.ArrayList<>(lf.body().size());
                for (ValueSpecification st : lf.body()) {
                    body.add(substitute(st, inner, folder));
                    CString ln = letName(st);
                    if (ln != null) {
                        inner.remove(ln.value());
                    }
                }
                yield new LambdaFunction(lf.parameters(), body);
            }
            case PureCollection pc -> new PureCollection(pc.values().stream()
                    .map(x -> substitute(x, env, folder)).toList());
            // LOSSLESS rebuild (F3.2c): the 5-arg ctor silently dropped
            // qualified/colType/stereotypes — a substituted ColSpec must
            // carry every component it arrived with
            case ColSpec cs -> new ColSpec(cs.name(),
                    cs.function1() == null ? null
                            : (LambdaFunction) substitute(cs.function1(), env, folder),
                    cs.function2() == null ? null
                            : (LambdaFunction) substitute(cs.function2(), env, folder),
                    cs.alias(),
                    cs.args().stream().map(a -> substitute(a, env, folder))
                            .toList(),
                    cs.qualified(), cs.pos(), cs.colType(), cs.colTypeMult(),
                    cs.stereotypes(), cs.taggedValues());
            case ColSpecArray ca -> new ColSpecArray(ca.colSpecs().stream()
                    .map(c -> (ColSpec) substitute(c, env, folder)).toList());
            case NewInstance ni -> {
                java.util.List<NewInstance.KeyBinding> props =
                        ni.properties().stream().map(b ->
                                new NewInstance.KeyBinding(b.key(),
                                        new KeyExpression(
                                                substitute(b.expression()
                                                        .value(), env, folder),
                                                b.expression().isAdd(),
                                                b.expression().isLocal())))
                                .toList();
                yield new NewInstance(ni.className(), ni.typeArguments(), props);
            }
            case NewInstanceCast nc -> new NewInstanceCast(nc.className(),
                    nc.typeArguments(), substitute(nc.src(), env, folder),
                    nc.targetSetId());
            // a folded quote/eval carrier is a CLOSED term (built from
            // literals — no free variables); substituting through it would
            // re-fold its own original
            case com.legend.protocol.spec.QuotedTreeCall q -> q;
            case com.legend.protocol.spec.QuotedGrammarCall q -> q;
            // leaves pass; any composite not special-cased above recurses
            default -> v.mapChildren(x -> substitute(x, env, folder));
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
