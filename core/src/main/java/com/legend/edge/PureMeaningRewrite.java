// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.UserCallInliner;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.NotImplementedException;
import com.legend.model.ConnectionDefinition.DatabaseType;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * THE ENGINE REWRITE at legend-engine's door, {@code LEGEND_LITE_PURE_MEANING=engine} (A3; the user's ruling of
 * 2026-11-19 ~20:05, "let's do the right design for the rewrite part"; the design note,
 * docs/records/compat-homework/A3-engine-rewrite-design.md, approved to build by the independent reviewer).
 *
 * <p>legend-engine sends store-bound {@code substring} and {@code indexOf} to the database as written, so its answer
 * is the database's own function over legend-pure's arguments. Under {@code engine} the door gives the query that
 * answer: each call the detector flags becomes a call of the database's own function, built by {@link Substitute}
 * (the core's native, typed as the typer types it). The database computes it; nothing here computes a value.
 *
 * <p>One walk decides both what is flagged and what is rewritten (the reviewer's D-M1):
 * <ol>
 *   <li>The detector's walk over the query finds its store-bound roots: each statement that reads a store, and each
 *       let-bound lambda that store-bound code names.</li>
 *   <li>Each of those roots is inlined ON ITS OWN ({@link UserCallInliner}, the planner's own step), so its user
 *       functions and qualified properties become its own nodes. A host-side {@code let} is not inlined into them:
 *       inlining a whole body reduces its lets into the statements that read them, which would make a value
 *       legend-engine computes on the host (with Pure's meaning) store-bound code. The planner reduces it later,
 *       unrewritten.</li>
 *   <li>The detector's walk runs again over that body, recording each flagged call by identity.</li>
 *   <li>Exactly those calls are replaced, within those roots alone. The count replaced must equal the count flagged,
 *       or the request is refused.</li>
 * </ol>
 *
 * <p>Refused by name, never approximated:
 * <ul>
 *   <li>a database whose answer under {@code engine} is not measured ({@link PureText#engineAnswers});</li>
 *   <li>a call the walk could not follow ({@code unverified});</li>
 *   <li>a flagged call outside the query's own tree after inlining: in a mapping (an M2M chain's transforms, a
 *       {@code ~func}, which the store resolver reaches after the body exists) or in a function the inliner left as a
 *       call.</li>
 * </ul>
 */
final class PureMeaningRewrite {

    private PureMeaningRewrite() {
    }

    /** How one flagged call becomes legend-engine's: the database's own function over the same arguments, typed as the
     *  typer types it (the reviewer's D-S2: the callee is the registry's function, never hand-assembled). */
    interface Substitute {
        TypedNativeCall of(ModelContext ctx, PureMeaningCheck.Code code, TypedNativeCall flagged);
    }

    /** The door's substitute: the core's natives for the database's own functions, built by {@link EngineSubstitutes}
     *  (the typer's overload, checked against the registry). */
    static TypedNativeCall engine(ModelContext ctx, PureMeaningCheck.Code code, TypedNativeCall flagged) {
        return switch (code) {
            case SUBSTRING_2, SUBSTRING_3 -> EngineSubstitutes.substring(ctx, flagged);
            case INDEX_OF_2 -> EngineSubstitutes.indexOf(ctx, flagged);
            case CORRECTED_SUBSTRING_2, CORRECTED_SUBSTRING_3, SUBSTR_2, SUBSTR_3 -> EngineSubstitutes.substr(ctx, flagged);
            case UNVERIFIED -> throw new IllegalStateException("an unverified call is refused before it is rewritten");
        };
    }

    /** The rewritten query, and the calls it rewrote (the header's codes and counts). */
    record Rewritten(TypedQuery query, PureMeaningCheck.Result rewrote) {
    }

    /** {@code query} with each flagged call rewritten to legend-engine's meaning, or refused by name. A query the
     *  detector does not flag is returned as it is. */
    static Rewritten rewrite(TypedQuery query, ModelContext ctx, @com.legend.base.Nullable String runtime,
            @com.legend.base.Nullable String requestMapping, Substitute substitute) {
        PureMeaningCheck.Result first = PureMeaningCheck.forEngine(query, ctx, runtime, requestMapping);
        if (first.clean()) {
            return new Rewritten(query, first);
        }
        List<String> unverified = new ArrayList<>();
        for (PureMeaningCheck.Site s : first.sites()) {
            if (s.code() == PureMeaningCheck.Code.UNVERIFIED) {
                unverified.add(s.described());
            }
        }
        if (!unverified.isEmpty()) {
            throw new NotImplementedException(PureMeaningCheck.SETTING + "=engine: " + String.join(", ", unverified)
                    + ": a call the check could not follow cannot be rewritten, so this query is refused");
        }
        DatabaseType target = PureMeaningCheck.target(ctx, runtime);
        if (!PureText.engineAnswers(target)) {
            throw new NotImplementedException(PureMeaningCheck.SETTING + "=engine: legend-engine's answer is not"
                    + " measured on " + (target == null ? "a runtime with no single database" : target.name())
                    + ", so this query is refused (" + first.header() + ")");
        }
        // 2: each store-bound root inlined on its own; every other statement as written
        UserCallInliner inliner = new UserCallInliner(query.specs());
        List<TypedSpec> body = new ArrayList<>();
        for (TypedSpec stmt : query.body()) {
            body.add(isRoot(first, stmt) ? inlined(inliner, stmt) : stmt);
        }
        TypedQuery inlinedQuery = query.withBody(body);
        // 3: the one walk that decides what is rewritten
        PureMeaningCheck.Result flagged = PureMeaningCheck.forEngine(inlinedQuery, ctx, runtime, requestMapping);
        List<String> unreachable = new ArrayList<>();
        Map<TypedSpec, PureMeaningCheck.Code> nodes = new IdentityHashMap<>();
        for (PureMeaningCheck.Site s : flagged.sites()) {
            if (s.code() == PureMeaningCheck.Code.UNVERIFIED || s.node() == null) {
                unreachable.add(s.described());
            } else {
                nodes.put(s.node(), s.code());
            }
        }
        if (!unreachable.isEmpty()) {
            throw new NotImplementedException(PureMeaningCheck.SETTING + "=engine: " + String.join(", ", unreachable)
                    + " cannot be rewritten at this door (a call the check could not follow, or one reached in a"
                    + " mapping or a function the inliner keeps as a call), so this query is refused");
        }
        refuseSharedLetRoots(inlinedQuery.body(), flagged);
        // 4: exactly those nodes, within the roots the walk walked
        Set<TypedSpec> replaced = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
        java.util.function.UnaryOperator<TypedSpec> rewrite = new java.util.function.UnaryOperator<>() {
            @Override
            public TypedSpec apply(TypedSpec n) {
                TypedSpec rebuilt = n.mapChildren(this);
                PureMeaningCheck.Code code = nodes.get(n);
                if (code == null) {
                    return rebuilt;
                }
                replaced.add(n);
                return substitute.of(ctx, code, (TypedNativeCall) rebuilt);
            }
        };
        List<TypedSpec> out = new ArrayList<>();
        for (TypedSpec stmt : inlinedQuery.body()) {
            if (!isRoot(flagged, stmt)) {
                out.add(stmt);
            } else if (stmt instanceof TypedLet let) {
                out.add(let.mapChildren(c -> c == let.value() ? rewrite.apply(c) : c));
            } else {
                out.add(rewrite.apply(stmt));
            }
        }
        if (replaced.size() != nodes.size()) {
            throw new NotImplementedException(PureMeaningCheck.SETTING + "=engine: " + replaced.size() + " of "
                    + nodes.size() + " flagged calls were rewritten, so this query is refused");
        }
        return new Rewritten(inlinedQuery.withBody(out), flagged);
    }

    /** A lambda let the rewrite would rewrite in place (a root holding a flagged call) that a statement it does not
     *  rewrite also evaluates: that statement would get the database's meaning, where legend-engine computes it on the
     *  host with Pure's. Refused by name (the reviewer's E-S1; narrowed to lambda lets, R2-S2). The planner's inliner reduces each let-bound lambda a store-bound
     *  statement uses into that statement, so after inlining no such let is known to stay a root; this holds the
     *  rewrite to that, whatever a future inliner keeps. */
    static void refuseSharedLetRoots(List<TypedSpec> body, PureMeaningCheck.Result flagged) {
        Set<TypedSpec> nodes = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
        for (PureMeaningCheck.Site s : flagged.sites()) {
            if (s.node() != null) {
                nodes.add(s.node());
            }
        }
        for (TypedSpec root : flagged.roots()) {
            // a lambda let alone: a value let is computed once, its call in its own SQL as legend-engine writes it
            // (asked: `let d = ...->substring(1, 3); $d->toUpper()` answers "CRE"), so a later read of it is no
            // reason to refuse (the reviewer's R2-S2)
            if (!(root instanceof TypedLet let) || !(let.value() instanceof com.legend.compiler.spec.typed.TypedLambda)
                    || !holds(let.value(), nodes)) {
                continue;
            }
            for (TypedSpec stmt : body) {
                if (!isRoot(flagged, stmt) && reads(stmt, let.name())) {
                    throw new NotImplementedException(PureMeaningCheck.SETTING + "=engine: the let '" + let.name()
                            + "' is sent to the database and also read by a statement computed on the host, so its"
                            + " calls cannot be given the database's meaning for one and not the other; refused");
                }
            }
        }
    }

    private static boolean holds(TypedSpec n, Set<TypedSpec> nodes) {
        if (nodes.contains(n)) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (holds(c, nodes)) {
                return true;
            }
        }
        return false;
    }

    private static boolean reads(TypedSpec n, String name) {
        if (n instanceof com.legend.compiler.spec.typed.TypedVariable v && v.name().equals(name)) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (reads(c, name)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isRoot(PureMeaningCheck.Result r, TypedSpec stmt) {
        for (TypedSpec root : r.roots()) {
            if (root == stmt) {
                return true;
            }
        }
        return false;
    }

    /** One root inlined on its own: a let's value (the let kept, as the planner reduces it), else the statement. */
    private static TypedSpec inlined(UserCallInliner inliner, TypedSpec root) {
        if (root instanceof TypedLet let) {
            TypedSpec value = inliner.inlineBody(List.of(let.value())).get(0);
            return let.mapChildren(c -> c == let.value() ? value : c);
        }
        List<TypedSpec> one = inliner.inlineBody(List.of(root));
        if (one.size() != 1) {
            throw new IllegalStateException("inlining one statement gave " + one.size());
        }
        return one.get(0);
    }
}
