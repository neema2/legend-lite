// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.spec.typed.FoldStrategy;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.lowering.Resolvers.ColumnResolver;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlType;
import com.legend.sql.TypeFact;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Lambda-parameter scope binding at the LOWERING boundary
 * (CodeShapeGuardrail seam split from {@link Lowerer}, M4 re-land).
 *
 * <p>{@link #lowerNativeArgs} is the M4 capability that replaced the
 * parked branch's LambdaWire ThreadLocal (compensation #1): the
 * UNARY-LAMBDA BINDING CONVENTION. A one-parameter lambda argument of
 * a value-lane native ranges over the nearest PRECEDING collection
 * argument, so its body lowers with the parameter column STAMPED as
 * that collection's element — the body's own dispatch (pureToString's
 * Any arm, equality emission) then reads the carrier from the tree at
 * construction, never from a side channel (witness
 * testPctRemoveDuplicatesBy: a toString key over a LITERAL-carried
 * mix must print spellings, not variant-extract them). Comparator
 * lambdas bind at their consumer sites (Dedup — §3.2); fold's
 * two-parameter lambda (element, ACCUMULATOR) never matches the unary
 * rule; a lambda with no preceding definite-array argument lowers
 * unstamped exactly as before.
 */
final class LambdaBinding {

    private LambdaBinding() {
    }

    /** The COMPARATOR-CONVENTION natives (M4 §3.2 + post-landing
     * audit): a two-parameter lambda whose params BOTH stamp as the
     * function's ONE list's element, so comparator BODIES lower
     * element-stamped (dispatch is frozen at construction — witness
     * the NonStandardFunction toString comparators). ROSTER = the
     * exhaustive same-element (T,T)->_ signature sweep of both oracle
     * trees (2026-08-25): removeDuplicates, sort, and contains — the
     * audit's found regression. contains' convention is
     * eval($value, $x) — needle FIRST — and its param-0 element
     * stamp is honest ONLY because the rule substitutes a SPELLED,
     * LITERAL-marked needle there (MixedEncoding.markedNeedle;
     * witness ComparatorConventionTest incl. the kind-honest eq pin
     * that caught the raw-needle text collision). Keys are signature
     * keys, the rule table's own dispatch identity. NOT here: fold
     * (second param = ACCUMULATOR); relation join/asOfJoin (their
     * lambdas span TWO relations); removeAll (engine-core pure
     * composition, never lowered); and min/max — their comparator is
     * STRUCTURALLY RECOGNIZED at the rule (Comparators pattern-match,
     * the body never lowers as a body), and stamping its params broke
     * the recognizer's structural equality (gate-caught: G9
     * chB-std testMax/testMin, 'must apply the SAME key'). */
    private static final java.util.Set<String> COMPARATOR_NATIVES =
            java.util.stream.Stream.of("removeDuplicates", "sort",
                            "contains")
                    .flatMap(nm -> com.legend.builtin.Pure
                            .nativeKeysAt(nm).stream())
                    .collect(java.util.stream.Collectors
                            .toUnmodifiableSet());

    /** Inner-lambda scope: ALL its parameters shadow; everything else
     * resolves outward through the enclosing resolver. */
    static ColumnResolver lambdaResolver(
            List<String> params, ColumnResolver outer) {
        return (var, prop) -> {
            if (var == null || !params.contains(var)) {
                return outer.resolve(var, prop);
            }
            return prop == null ? SqlExpr.Column.derived(null, var)
                    : SqlExpr.Column.derived(var, prop);
        };
    }

    /** THE FOLD BINDING DOOR (§4bZ-U leg 2): a fold lambda's params
     * are {@code (element, accumulator)} — the element stamps as the
     * source collection's element (the {@link SqlExpr.Column#param}
     * door), the accumulator as the INIT's stored fact (the per-step
     * accumulator of an agreeing fold; a type-CHANGING fold's body
     * then types differently from init and {@code foldType} stays
     * honestly UNKNOWN). Bare refs only — property reads resolve
     * outward through {@code inner}. */
    static ColumnResolver foldResolver(String elemParam, SqlExpr collection,
            String accParam, SqlExpr init, ColumnResolver inner) {
        return (var, prop) -> {
            if (var != null && prop == null) {
                if (var.equals(elemParam)) {
                    return SqlExpr.Column.param(var, collection);
                }
                if (var.equals(accParam)
                        && init.type() instanceof TypeFact.Typed it) {
                    return new SqlExpr.Column(null, var, it,
                            com.legend.sql.OutputCol.Origin.DERIVED);
                }
            }
            // a PROPERTY read over a stamped struct param ($p.lastName
            // — the same qualified-column emission, now with the fact:
            // §4bZ-U, the fold-tree receipts' blind leaf)
            if (var != null && prop != null) {
                if (var.equals(elemParam)) {
                    SqlExpr.Column c = structFieldRead(var, prop,
                            elementOf(collection.type()));
                    if (c != null) {
                        return c;
                    }
                }
                if (var.equals(accParam)) {
                    SqlExpr.Column c = structFieldRead(var, prop,
                            init.type());
                    if (c != null) {
                        return c;
                    }
                }
            }
            return inner.resolve(var, prop);
        };
    }

    /** The struct-field read door: {@code var.prop} stamped as the
     * field's declared type when the param's fact is a Struct claiming
     * the field — identical emission (qualified column), supplied
     * fact. Null = no claim (the caller's plain resolution stands). */
    private static SqlExpr.@com.legend.base.Nullable Column structFieldRead(
            String var, String prop, TypeFact paramFact) {
        if (paramFact instanceof TypeFact.Typed t
                && t.type() instanceof SqlType.Struct st) {
            for (SqlType.Struct.Field f : st.fields()) {
                if (f.name().equals(prop)) {
                    // §E3: an absent optional property IS a NULL field
                    // (the StructLit declared-slot arm) — presence not
                    // provable, may-be-null (the structGetType rule)
                    return SqlExpr.Column.of(var, prop, f.type(), true,
                            com.legend.sql.OutputCol.Origin.DERIVED);
                }
            }
        }
        return null;
    }

    /** The collection fact's element fact (Array(T) -> Typed(T));
     * §E3: element presence not provable from Array(T) — may-be-null
     * (the {@link SqlExpr.Column#param} doctrine). */
    private static TypeFact elementOf(TypeFact collectionFact) {
        return collectionFact instanceof TypeFact.Typed t
                && t.type() instanceof SqlType.Array at
                ? new TypeFact.Typed(at.element(), true)
                : collectionFact;
    }

    /** fold in PURE conventions ({@code (element, accumulator)}
     * lambda); the Phase-G strategy collapses to logical facts
     * (Concatenation = list concat; MapReduce pre-transforms;
     * {@code accIsList} rides for the dialect). NOTHING here knows how
     * any backend folds. (Moved from {@link Lowerer} at the 3,500-line
     * shape guard — the binding-door owner hosts the fold lowering.) */
    static SqlExpr lowerFold(Lowerer lw,
            com.legend.compiler.spec.typed.TypedFold f,
            ColumnResolver columns) {
        // Phase 1c: a column-collect fold lowers as its per-row MAP
        TypedSpec collect = Fold.columnCollectAsMap(f);
        if (collect != null) {
            return lw.scalar(collect, columns);
        }
        // C1: the source conforms by EMISSION (asList, stamp-read); an
        // empty/NULL source additionally casts to its PURE element's
        // array (§4bZ-U leg 2 — the typedList door: empty-list folds
        // then bind and type through)
        SqlExpr source = PureSql.typedList(
                PureSql.asList(lw.scalar(f.source(), columns),
                        many(f.source())),
                f.source().info().type());
        SqlExpr rawInit = lw.scalar(f.init(), columns);
        // the acc-binding strategies ride the same door on the INIT: an
        // empty/NULL initial collection casts to its pure element's
        // array, so the accumulator param can stamp and the body types.
        // Concatenation keeps the RAW init — its own asList wrap owns
        // the shape (the door there double-wrapped: T[][] vs T[]).
        SqlExpr init = PureSql.typedList(rawInit, f.init().info().type());
        List<String> ps = f.reducer().parameters();
        return switch (f.strategy()) {
            // TO-ONE init concatenates as a singleton list; list-shaped
            // values and NULL (=[] to DuckDB list_concat) pass through.
            case FoldStrategy.Concatenation c ->
                    new SqlExpr.Call(com.legend.sql.SqlFn.LIST_CONCAT,
                            List.of(PureSql.asList(rawInit, many(f.init())),
                                    source));
            case FoldStrategy.SameType st ->
                    new SqlExpr.FoldCall(source,
                            new SqlExpr.Lambda(ps,
                                    lw.scalar(Lowerer.last(f.reducer()),
                                            foldResolver(ps.get(0), source,
                                                    ps.get(1), init,
                                                    lambdaResolver(ps, columns)))),
                            init, many(f.init()), true);
            case FoldStrategy.MapReduce mr -> {
                // every name derives from the strategy lambdas' OWN
                // parameters — the lambdas are CLOSED (MapReduce javadoc),
                // so whatever α-renaming the inliner applied is already
                // consistent between binder and body by construction
                String elem = mr.transform().parameters().get(0);
                SqlExpr.Lambda transform = new SqlExpr.Lambda(List.of(elem),
                        lw.scalar(Lowerer.last(mr.transform()),
                                mapElemResolver(elem, source, false,
                                        lambdaResolver(List.of(elem), columns))));
                SqlExpr transformed = new SqlExpr.Call(
                        com.legend.sql.SqlFn.LIST_TRANSFORM,
                        List.of(source, transform));
                // The transform makes source elements accumulator-typed.
                List<String> rps = mr.reducer().parameters();
                yield new SqlExpr.FoldCall(transformed,
                        new SqlExpr.Lambda(rps,
                                lw.scalar(Lowerer.last(mr.reducer()),
                                        foldResolver(rps.get(0), transformed,
                                                rps.get(1), init,
                                                lambdaResolver(rps, columns)))),
                        init, many(f.init()), true);
            }
            case FoldStrategy.CollectionBuild cb ->
                    new SqlExpr.FoldCall(source,
                            new SqlExpr.Lambda(ps,
                                    lw.scalar(Lowerer.last(f.reducer()),
                                            foldResolver(ps.get(0), source,
                                                    ps.get(1), init,
                                                    lambdaResolver(ps, columns)))),
                            init, many(f.init()), false);
        };
    }

    private static boolean many(TypedSpec spec) {
        return spec.info().multiplicity().requireBounded("lowering").isMany();
    }

    /** The collection-map MAPPER lowering (the Lowerer's TypedMap
     * collection arm delegates here — method-size guard): a
     * one-parameter lambda mapper lowers under the element door;
     * anything else lowers plain. */
    static SqlExpr mapMapper(Lowerer lw,
            com.legend.compiler.spec.typed.TypedMap m, SqlExpr mSrc,
            boolean mToOne, ColumnResolver columns) {
        if (m.mapper() instanceof TypedLambda mml
                && mml.parameters().size() == 1) {
            return new SqlExpr.Lambda(mml.parameters(),
                    lw.scalar(Lowerer.last(mml),
                            mapElemResolver(mml.parameters().get(0),
                                    mSrc, mToOne,
                                    lambdaResolver(mml.parameters(),
                                            columns))));
        }
        return lw.scalar(m.mapper(), columns);
    }

    /** The collection-map element door (§4bZ-U leg 2): the map
     * lambda's one param stamps as the SOURCE's element — the array's
     * element for a many source, the value's own type for a to-one
     * source (ListEncodings.map wraps it as one element). A many
     * source typed as a non-array carrier (variant JSON) stays
     * unstamped — its element is the carrier's own business. */
    static ColumnResolver mapElemResolver(String param, SqlExpr source,
            boolean srcToOne, ColumnResolver inner) {
        SqlType elem = source.type() instanceof TypeFact.Typed t
                ? t.type() instanceof com.legend.sql.SqlType.Array at
                        ? at.element()
                        : srcToOne ? t.type() : null
                : null;
        return (var, prop) -> {
            if (param.equals(var) && elem != null) {
                if (prop == null) {
                    // §E3: element read — may-be-null (param doctrine)
                    return SqlExpr.Column.of(null, param, elem, true,
                            com.legend.sql.OutputCol.Origin.DERIVED);
                }
                // property read over the stamped element (same door as
                // foldResolver's — §4bZ-U fold-tree receipts)
                SqlExpr.Column c = structFieldRead(param, prop,
                        new TypeFact.Typed(elem, true));
                if (c != null) {
                    return c;
                }
            }
            return inner.resolve(var, prop);
        };
    }

    /** Native-call argument lowering under the unary-lambda binding
     * convention (see class doc). {@code scalarFn} is the enclosing
     * Lowerer's scalar lane. */
    static List<SqlExpr> lowerNativeArgs(TypedNativeCall n,
            ColumnResolver columns,
            BiFunction<TypedSpec, ColumnResolver, SqlExpr> scalarFn) {
        // dispatch identity is the SIGNATURE KEY (the same key the
        // Scalars rule table uses — audit 22a: never the bare FQN)
        boolean comparator = COMPARATOR_NATIVES.contains(
                n.callee().signatureKey());
        List<SqlExpr> out = new ArrayList<>(n.args().size());
        for (TypedSpec a : n.args()) {
            SqlExpr coll;
            if (a instanceof TypedLambda l
                    && (l.parameters().size() == 1
                            || (comparator && l.parameters().size() == 2))
                    && (coll = lastDefiniteArray(out)) != null) {
                // unary: the param IS the element; comparator natives
                // (§3.2): BOTH params are elements of the ONE list
                List<String> ps = l.parameters();
                ColumnResolver inner = lambdaResolver(ps, columns);
                ColumnResolver stamped = (var, prop) ->
                        var != null && ps.contains(var) && prop == null
                                ? SqlExpr.Column.param(var, coll)
                                : inner.resolve(var, prop);
                out.add(new SqlExpr.Lambda(ps,
                        scalarFn.apply(Lowerer.last(l), stamped)));
                continue;
            }
            if (a instanceof com.legend.compiler.spec.typed.TypedCollection run
                    && run.operatorRun() && StoreLane.sqlLane(run.elements())) {
                // the infix run a + b (+ …) over STORE reads (StoreLane): the
                // operands go into the SQL verbatim — an optional column
                // null-propagates exactly as the engine's arithmetic does —
                // never through the value collection's empty-element
                // compaction. A run holding a possibly-empty PURE value keeps
                // pure's rule and lowers as the value collection below.
                out.add(Lowerer.listLiteral(run, e -> scalarFn.apply(e, columns)));
                continue;
            }
            out.add(scalarFn.apply(a, columns));
        }
        return out;
    }

    private static @com.legend.base.Nullable SqlExpr lastDefiniteArray(
            List<SqlExpr> lowered) {
        for (int i = lowered.size() - 1; i >= 0; i--) {
            if (lowered.get(i).type() instanceof TypeFact.Typed t
                    && t.type() instanceof SqlType.Array) {
                return lowered.get(i);
            }
        }
        return null;
    }
}
