// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.TypedQuery;
import com.legend.builtin.NativeFn;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.StatementEffects;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.planner.LetPlanner;
import com.legend.planner.QueryDoor;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * THE KIND CENSUS of a body the router leaves on the walk (the test-is-the-product plan's M9, 2026-11-01): the test
 * kinds and the kinds planned another way that the body holds, each NAMED in the body-class rule's own vocabulary
 * ({@link BodyClass#anotherWay}, the query door's {@link QueryDoor.TestOnly}, the let kinds), sorted and joined. A
 * measurement carrier like {@code ProgramFacts.shape}: the corpus lanes attribute it per test and histogram it, so
 * each sub-step of M9 is aimed at the kinds that hold the most bodies back and the residue after each is named by
 * kind. It reads the typed body and decides nothing: a body's class is {@link BodyClass#of}'s alone.
 */
public final class BodyKinds {

    private BodyKinds() {
    }

    /** The kinds {@code query} holds on {@code runtime}, sorted and comma-joined; {@code why} a name the caller adds
     *  for what kept the body on the walk beyond its kinds (the host checks' own refusal of an assert's shape), or
     *  null. */
    public static String of(TypedQuery query, String runtime, @com.legend.base.Nullable String why) {
        ModelContext ctx = query.context();
        Set<String> tags = new TreeSet<>();
        if (why != null) {
            tags.add(why);
        }
        if (QueryDoor.declaresTestData(runtime, ctx)) {
            tags.add("declares-test-data");
        }
        Set<String> systemClasses = BodyClass.systemClasses(ctx);
        QueryDoor.Scan scan = new QueryDoor.Scan(query, runtime);
        List<TypedSpec> body = query.body();
        // the frames and the lets, as the rule keeps them: the census reads a statement where the rule reads it (a
        // frame let's query argument alone; a handle let and an alias nothing; every other statement with its inline
        // executes as their queries), so a tag names what keeps the body on the walk, not a position the rule exempts
        Map<String, Boolean> frames = new HashMap<>();
        java.util.function.Predicate<String> relationFrame = n -> Boolean.TRUE.equals(frames.get(n));
        List<TypedSpec> lets = new java.util.ArrayList<>();
        ElementConsumption consumption = new ElementConsumption(body, systemClasses, ctx, query.specs());
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            boolean last = i == body.size() - 1;
            EnumSet<QueryDoor.TestOnly> kinds = scan.kinds(statement);
            for (QueryDoor.TestOnly k : kinds) {
                tags.add(k.name().toLowerCase(Locale.ROOT).replace('_', '-'));
            }
            if (StatementEffects.containsTdgGenerator(statement)) {
                tags.add("tdg");
            }
            if (StatementEffects.reaches(statement, BodyClass::readsActivities, query.specs(), new HashMap<>())) {
                tags.add("activities");
            }
            TypedSpec value = statement instanceof TypedLet l ? l.value() : statement;
            if (value instanceof TypedNativeCall call && NativeFn.Effect.isDbEffect(call.callee().id())) {
                if (statement instanceof TypedLet) {
                    tags.add("effect-let");
                }
                if (NativeFn.Effect.of(call.callee().id()).orElse(null) == NativeFn.Effect.LOAD_CSV_TO_DB_TABLE) {
                    tags.add("load-csv");
                } else if (!BodyClass.literalEffect(call, body.subList(0, i))) {
                    tags.add("effect-computed");
                }
                if (last) {
                    tags.add("last-effect");
                }
                continue;
            }
            TypedSpec read = BodyClass.framesAndHandlesAsTheirQueries(statement);
            boolean frameQuery = false;
            if (statement instanceof TypedLet let && !last) {
                if (LetPlanner.badAliasIndex(let.value(), relationFrame)) {
                    tags.add("alias-index");
                    continue;
                }
                LetPlanner.Kind kind = LetPlanner.kind(let, body, i, query.specs(), new HashMap<>(), relationFrame);
                if (kind == LetPlanner.Kind.FRAME && kinds.equals(EnumSet.of(QueryDoor.TestOnly.LET_EXECUTE))) {
                    TypedNativeCall call = (TypedNativeCall) LetPlanner.rhs(let);
                    if (StatementEffects.reaches(call.args().get(0), n -> n instanceof TypedVariable v
                            && frames.containsKey(v.name()), query.specs(), new HashMap<>())) {
                        tags.add("frame-reads-frame");
                    }
                    Boolean rooted = com.legend.planner.FramePlanner.relationRooted(call, lets, query.specs());
                    if (rooted == null) {
                        tags.add("frame-unbuildable");
                    }
                    frames.put(let.name(), Boolean.TRUE.equals(rooted));
                    read = call.args().get(0);
                    frameQuery = true;
                } else if (kind == LetPlanner.Kind.ALIAS && kinds.isEmpty()) {
                    frames.put(let.name(), true);
                    continue;
                } else if (kind == LetPlanner.Kind.VALUE && kinds.isEmpty()
                        && (com.legend.planner.BodyPlanner.handleType(let.value(), ctx) != null
                        || consumption.consumedElementLet(i))) {
                    // a handle's let, or a let binding system elements the body never reads as a value (M10c items 7,
                    // 7b): the program splices it, no read -- the class rules' own exclusion, so the tags name none
                    lets.add(let);
                    continue;
                } else {
                    if (kind != LetPlanner.Kind.VALUE) {
                        tags.add("let-" + kind.name().toLowerCase(Locale.ROOT).replace('_', '-'));
                    }
                    lets.add(let);
                }
            } else if (statement instanceof TypedLet) {
                tags.add("last-let");
            }
            // every kind the statement reaches where the rule reads it (the reach records each; its answer is not
            // read): a frame's query the kinds the product's lowering plans another way, prefixed "frame-"; an
            // assert or a value let the side kinds (BodyClass.sideKind)
            boolean ofAFrame = frameQuery;
            StatementEffects.reaches(read, n -> {
                BodyClass.Kind kind = ofAFrame ? BodyClass.anotherWay(n, systemClasses, ctx)
                        : BodyClass.sideKind(n, systemClasses, ctx);
                if (kind != null) {
                    tags.add(ofAFrame ? "frame-" + kind.tag() : kind.tag());
                }
                return false;
            }, query.specs(), new HashMap<>());
            if (statement instanceof TypedLet || kinds.contains(QueryDoor.TestOnly.ASSERT)) {
                continue;
            }
            if (statement instanceof com.legend.compiler.spec.typed.TypedMap tm
                    && kinds.contains(QueryDoor.TestOnly.EFFECT)) {
                // an effectful map (M10c item 7c): a seed form whose data folds over one effect the door writes moves;
                // any other is named for the residue histogram
                tags.add(BodyClass.seedFormEffectMap(tm, body.subList(0, i)) ? "effect-map-seed" : "effect-map-computed");
                continue;
            }
            if (!BodyClass.plansNoStep(statement, body.subList(0, i), ctx)) {
                tags.add(last ? "last-query" : "check");
            }
        }
        return String.join(",", tags);
    }
}
