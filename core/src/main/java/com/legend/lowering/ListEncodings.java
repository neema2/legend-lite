// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.PlatformTypes;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;

import java.util.List;

/**
 * The COLLECTION-value list encodings (extracted from {@link Lowerer} at
 * the file guardrail): map/slice over values that travel as DuckDB
 * lists. Relation-typed sources never come here — they take the
 * relation arms.
 */
final class ListEncodings {

    private ListEncodings() {
    }

    /**
     * {@code map} over a collection VALUE. WIRE SHAPE FOLLOWS THE TYPE
     * (tenet: types drive construction): a TO-ONE/[0..1] source cannot
     * feed {@code list_transform} bare — a BINDER error even in a dead
     * CASE arm (DuckDB type-checks both arms; assertEquals' many-path
     * over a {@code head()} actual — Phase 4 channel B, 12 essential
     * tests) — so a SCALAR-shaped source wraps as its singleton list
     * (one already list-shaped — a one-element collection literal —
     * passes through); and when the map RESULT is itself to-one/[0..1],
     * the value unwraps back to the scalar wire. A [0..1] source
     * null-guards both ways: map over EMPTY is EMPTY, never
     * {@code f(NULL)}.
     */
    /** The scalar-channel {@code TypedMap} lowering — moved from the
     * Lowerer at the shape limit (the numbered-seam split): the
     * wire-shape policy's own front door. {@code []->map(f) == []}
     * (Part-1 fix, 2026-08-26): a [0..0] source is the EMPTY value —
     * NullLit, the TypedCollection-empty convention (mapping over it
     * tripped the ONE-STAMP/LIST-SHAPE invariant). The mapper's param
     * stamps as the source's element (§4bZ-U leg 2,
     * LambdaBinding.mapMapper). */
    /** The scalar-channel {@code TypedSortBy} lowering (TDG lane S1):
     * collection sortBy = the SAME stable {k,i,v} struct-sort encoding
     * as the 3-arg native sort rule (Scalars: key first, index second
     * so equal keys stay stable, unwrap {@code .v}), with the key
     * lambda lowered under the map ELEMENT door. The database executes
     * the sort — never host Java (charter anti-pattern 1). */
    static SqlExpr lowerSortBy(Lowerer lw,
            com.legend.compiler.spec.typed.TypedSortBy sb,
            com.legend.lowering.Resolvers.ColumnResolver columns) {
        var mult = sb.source().info().multiplicity();
        if (mult instanceof com.legend.compiler.element.type.Multiplicity
                .Bounded z && z.upper() != null && z.upper() <= 1) {
            // sort over <=1 values IS the operand (the native rule's
            // stamp-read identity)
            return lw.scalar(sb.source(), columns);
        }
        if (sb.key().parameters().size() != 1) {
            throw new com.legend.error.NotImplementedException(
                    "collection sortBy expects a one-parameter key lambda");
        }
        SqlExpr src = lw.scalar(sb.source(), columns);
        String param = sb.key().parameters().get(0);
        SqlExpr keyBody = lw.scalar(LambdaBinding.last(sb.key()),
                LambdaBinding.mapElemResolver(param, src, false,
                        LambdaBinding.lambdaResolver(
                                sb.key().parameters(), columns)));
        // ELEMENT-carried indices (list_zip with 1..n) instead of
        // LIST_GET(src, i) inside the lambdas: a source that is itself a
        // scalar SUBQUERY (a JSON tree read off an executed frame) cannot
        // be referenced inside a lambda body (DuckDB: "subqueries in
        // lambda expressions are not supported"); the outer arguments
        // may be anything. The (k, i, v) struct sort keeps the stable
        // index tie-break exactly as before.
        SqlExpr range = SqlExpr.Call.of(SqlFn.RANGE_FN,
                new SqlExpr.IntLit(1),
                SqlExpr.Call.of(SqlFn.PLUS,
                        SqlExpr.Call.of(SqlFn.LIST_LENGTH, src),
                        new SqlExpr.IntLit(1)));
        SqlExpr zipped = SqlExpr.Call.of(SqlFn.LIST_ZIP, src, range);
        SqlExpr z = SqlExpr.Column.param("_sb_z", zipped);
        SqlExpr valAt = new SqlExpr.StructGet(z, "1");
        SqlExpr i = new SqlExpr.StructGet(z, "2");
        SqlExpr keyExpr = Scalars.substituteRef(keyBody, param, valAt);
        SqlExpr idxField = sb.ascending() ? i
                : SqlExpr.Call.of(SqlFn.MINUS, new SqlExpr.IntLit(0), i);
        SqlExpr pairs = SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, zipped,
                new SqlExpr.Lambda(List.of("_sb_z"),
                        new SqlExpr.StructLit(List.of(
                                new SqlExpr.StructLit.Field("k", keyExpr),
                                new SqlExpr.StructLit.Field("i", idxField),
                                new SqlExpr.StructLit.Field("v", valAt)))));
        SqlExpr sorted = new SqlExpr.Call(
                sb.ascending() ? SqlFn.LIST_SORT : SqlFn.LIST_SORT_DESC,
                List.of(pairs));
        return SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, sorted,
                new SqlExpr.Lambda(List.of("_sb_e"),
                        new SqlExpr.StructGet(
                                SqlExpr.Column.param("_sb_e", sorted), "v")));
    }

    static SqlExpr lowerMap(Lowerer lw,
            com.legend.compiler.spec.typed.TypedMap m,
            com.legend.lowering.Resolvers.ColumnResolver columns) {
        var mult = m.source().info().multiplicity();
        if (mult instanceof com.legend.compiler.element.type.Multiplicity
                .Bounded z && z.upper() != null && z.upper() == 0) {
            return new SqlExpr.NullLit();
        }
        SqlExpr mSrc = lw.scalar(m.source(), columns);
        // requireBounded: a VAR stamp surviving to lowering stays LOUD
        // (the Lowerer.isMany contract this call replaced)
        boolean mToOne = !mult.requireBounded("lowering").isMany();
        return map(mSrc,
                LambdaBinding.mapMapper(lw, m, mSrc, mToOne, columns),
                mToOne,
                mult instanceof com.legend.compiler.element.type
                        .Multiplicity.Bounded sb && sb.lower() == 0,
                m.info().multiplicity() instanceof com.legend.compiler
                        .element.type.Multiplicity.Bounded rb
                        && rb.upper() != null && rb.upper() == 1,
                ValueCollections.isCollectionMapper(m.mapper()));
    }

    static SqlExpr map(SqlExpr src, SqlExpr lam, boolean srcOne,
            boolean srcNullable, boolean resultOne, boolean collectionMapper) {
        boolean alreadyList = src instanceof SqlExpr.ArrayLit
                || src instanceof SqlExpr.NullLit;
        boolean srcOptional = srcOne && srcNullable && !alreadyList;
        SqlExpr listSrc = src;
        if (srcOne && !alreadyList) {
            SqlExpr singleton = new SqlExpr.ArrayLit(List.of(src));
            listSrc = srcOptional && !resultOne
                    ? new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                            SqlExpr.Call.of(SqlFn.IS_NULL, src),
                            new SqlExpr.ArrayLit(List.of()))), singleton)
                    : singleton;
        }
        SqlExpr transformed = SqlExpr.Call.of(SqlFn.LIST_TRANSFORM,
                listSrc, SqlExpr.Lambda.bind(lam, listSrc));
        if (srcOne && resultOne) {
            SqlExpr applied = SqlExpr.Call.of(SqlFn.LIST_GET, transformed,
                    new SqlExpr.IntLit(1));
            return srcOptional
                    ? new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                            SqlExpr.Call.of(SqlFn.IS_NULL, src),
                            new SqlExpr.NullLit())), applied)
                    : applied;
        }
        return collectionMapper
                ? SqlExpr.Call.of(SqlFn.LIST_FLATTEN, transformed)
                : transformed;
    }

    /** The concatenate RULE registration (seam split from
     * {@link Scalars} — the encoding's owner; the relation overload is
     * the TypedConcatenate set-op and never reaches scalar lowering).
     * A MIXED concatenation rides the VARIANT carrier — T solved to
     * Any, and ALSO mixed CLASS kinds under an ancestor LUB (§4bZ-U,
     * probed 1.5.0: DuckDB FIELD-UNIONS unequal struct arrays —
     * {name:'x'} ++ {place:'y'} delivers STRUCT(name, place) rows with
     * NULL-fill, SMEARING class identity; pure keeps per-element kinds
     * — testConcatenateTypeInference types the result as the common
     * superclass). One value, one carrier, whichever spelling built it
     * — the hetero-literal arm's doctrine. */
    static void registerConcatenate(java.util.Map<String, Scalars.Rule> rules) {
        for (String f : com.legend.builtin.Pure.nativeKeysAt("concatenate")) {
            rules.put(f, (n, args) -> {
                // scalar-encoded sides wrap null-guarded (concatSide)
                List<SqlExpr> args2 = new java.util.ArrayList<>(args.size());
                for (int i = 0; i < args.size(); i++) {
                    args2.add(concatSide(
                            Scalars.isToOne(n.args().get(i)), args.get(i)));
                }
                args = args2;
                boolean mixedStructs = args.size() == 2
                        && args.get(0).type()
                                instanceof com.legend.sql.TypeFact.Typed t0
                        && t0.type() instanceof SqlType.Array a0
                        && a0.element() instanceof SqlType.Struct s0
                        && args.get(1).type()
                                instanceof com.legend.sql.TypeFact.Typed t1
                        && t1.type() instanceof SqlType.Array a1
                        && a1.element() instanceof SqlType.Struct s1
                        && !s0.equals(s1);
                if (!PlatformTypes.isAny(n.info().type()) && !mixedStructs) {
                    return new SqlExpr.Call(SqlFn.LIST_CONCAT, args);
                }
                List<SqlExpr> wrapped = new java.util.ArrayList<>(args.size());
                for (int i = 0; i < args.size(); i++) {
                    if (PlatformTypes.isAny(n.args().get(i).info().type())) {
                        wrapped.add(args.get(i));
                    } else {
                        wrapped.add(SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, args.get(i),
                                new SqlExpr.Lambda(List.of("_cv"),
                                        SqlExpr.Call.of(SqlFn.TO_VARIANT,
                                                SqlExpr.Column.param("_cv",
                                                        args.get(i))))));
                    }
                }
                return new SqlExpr.Call(SqlFn.LIST_CONCAT, wrapped);
            });
        }
    }

    /** The zip RULE registration (seam split from {@link Scalars} at
     * the 3,500-line shape guard — the encoding's owner registers its
     * own rule, the ScalarStats.register idiom). Empty/NULL sides
     * conform to their pure element's array (§4bZ-U leg 2 — the
     * typedList door: the pair-struct chain then types through
     * LIST_GET/StructLit/LIST_TRANSFORM). zip's c1-literal sides box
     * (DEEP_AUDIT §3). */
    static void registerZip(java.util.Map<String, Scalars.Rule> rules) {
        for (String f : com.legend.builtin.Pure.nativeKeysAt("zip")) {
            rules.put(f, (n, args) -> zip(
                    PureSql.typedList(PureSql.asList(args.get(0),
                            !CollectionLanes.c1Literal(n.args().get(0))),
                            n.args().get(0).info().type()),
                    PureSql.typedList(PureSql.asList(args.get(1),
                            !CollectionLanes.c1Literal(n.args().get(1))),
                            n.args().get(1).info().type())));
        }
    }

    /** {@code zip(a, b)}: pairwise {first, second} structs up to the
     * SHORTER side — DuckDB's own positional {@code list_zip(a, b,
     * truncate)} (probed 1.5.0: a NULL or empty side zips to {@code []},
     * a scalar-subquery side is fine), re-spelled from its unnamed
     * struct into the Pair layout every Pair read expects. The former
     * {@code list_get(a, i)} spelling put the sides INSIDE the lambda,
     * which DuckDB rejects for a subquery side ("subqueries in lambda
     * expressions are not supported" — a collected relation). */
    static SqlExpr zip(SqlExpr a, SqlExpr b) {
        SqlExpr zipped = SqlExpr.Call.of(SqlFn.LIST_ZIP, a, b, new SqlExpr.BoolLit(true));
        SqlExpr.Column x = SqlExpr.Column.param("_zip_e", zipped);
        SqlExpr body = new SqlExpr.StructLit(List.of(
                new SqlExpr.StructLit.Field("first", new SqlExpr.StructGet(x, "1")),
                new SqlExpr.StructLit.Field("second", new SqlExpr.StructGet(x, "2"))));
        return SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, zipped,
                new SqlExpr.Lambda(List.of("_zip_e"), body));
    }

    /** {@code slice(start, stop)}: 0-based exclusive-stop → 1-based
     * inclusive array_slice; NEGATIVE bounds clamp to the list head
     * (PCT — DuckDB reads a negative bound FROM THE END), and inverted
     * bounds RAISE real pure's message, in the database. */
    static SqlExpr slice(SqlExpr src, SqlExpr start, SqlExpr stop) {
        SqlExpr lo = clamp0(start);
        SqlExpr hi = clamp0(stop);
        SqlExpr sliced = SqlExpr.Call.of(SqlFn.LIST_SLICE, src,
                onePlus(lo), hi);
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.GREATER, lo, hi),
                SqlExpr.Call.of(SqlFn.ERROR,
                        SqlExpr.Call.of(SqlFn.CONCAT,
                                SqlExpr.Call.of(SqlFn.CONCAT,
                                        SqlExpr.Call.of(SqlFn.CONCAT,
                                                new SqlExpr.StringLit("The low bound ("),
                                                new SqlExpr.Cast(lo, SqlType.Scalar.VARCHAR)),
                                        new SqlExpr.StringLit(") can't be higher than the high bound (")),
                                SqlExpr.Call.of(SqlFn.CONCAT,
                                        new SqlExpr.Cast(hi, SqlType.Scalar.VARCHAR),
                                        new SqlExpr.StringLit(") in a slice operation")))))),
                sliced);
    }

    /**
     * FIRST-OCCURRENCE dedup (real removeDuplicates semantics — its PCT
     * asserts order without sorting). LIST_DISTINCT is UNORDERED in DuckDB;
     * keep element x at 1-based index i iff its first position is i.
     */
    static SqlExpr orderedDedup(SqlExpr list) {
        // The list appears TWICE (once as the filter source, once inside
        // the position lambda). A SUBQUERY-carried list inside the
        // lambda is a DuckDB binder error ("subqueries in lambda
        // expressions are not supported" — sql-exec burn 2026-08-30,
        // value-lane distinct probe), so it hoists through a one-row
        // carrier and the lambda references the COLUMN.
        if (hasSubquery(list)
                && list.type() instanceof com.legend.sql.TypeFact.Typed t) {
            SqlExpr l = SqlExpr.Column.of("_ddc", "l", t.type(),
                    t.nullable(), com.legend.sql.OutputCol.Origin.DERIVED);
            com.legend.sql.SqlSelect carry = new com.legend.sql.SqlSelect(
                    List.of(new com.legend.sql.SqlSelect.Projection(list, "l",
                            new com.legend.sql.OutputCol("l", t.type(),
                                    t.nullable()))),
                    false, new com.legend.sql.SqlSource.Dual(), null,
                    List.of(), null, null, List.of(), null, null, List.of());
            return new SqlExpr.ScalarSubquery(new com.legend.sql.SqlSelect(
                    List.of(new com.legend.sql.SqlSelect.Projection(
                            dedupFilter(l), "v",
                            new com.legend.sql.OutputCol("v", t.type(),
                                    t.nullable()))),
                    false, new com.legend.sql.SqlSource.Subselect(carry,
                            "_ddc", null),
                    null, List.of(), null, null, List.of(), null, null,
                    List.of()));
        }
        return dedupFilter(list);
    }

    private static SqlExpr dedupFilter(SqlExpr list) {
        return new SqlExpr.Call(SqlFn.LIST_FILTER, List.of(list,
                new SqlExpr.Lambda(List.of("_ddx", "_ddi"),
                        new SqlExpr.Call(SqlFn.EQUAL, List.of(
                                SqlExpr.Call.of(SqlFn.LIST_POSITION, list,
                                        SqlExpr.Column.derived(null, "_ddx")),
                                SqlExpr.Column.derived(null, "_ddi"))))));
    }

    private static boolean hasSubquery(SqlExpr e) {
        if (e instanceof SqlExpr.ScalarSubquery
                || e instanceof SqlExpr.Exists) {
            return true;
        }
        for (SqlExpr c : e.children()) {
            if (hasSubquery(c)) {
                return true;
            }
        }
        return false;
    }

    /** A concatenate SIDE: scalar encodings (TO-ONE stamps, many-
     * stamped CASE optionals) wrap null-guarded — SQL NULL is pure's
     * EMPTY, so the side contributes [], never [NULL]. Many-stamped
     * lists pass; the STAMP decides ({@code toOne} = the caller's
     * Stamps.toOne read). Moved from Scalars (file-length guard). */
    static SqlExpr concatSide(boolean toOne, SqlExpr e) {
        if (e instanceof SqlExpr.NullLit
                || !(toOne || e instanceof SqlExpr.Case)) {
            return e;
        }
        return new SqlExpr.Case(
                List.of(new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.IS_NULL, e),
                        new SqlExpr.ArrayLit(List.of()))),
                new SqlExpr.ArrayLit(List.of(e)));
    }

    /** Clamp a (possibly negative) index to zero — PCT's slice/drop/take edge semantics. */
    static SqlExpr clamp0(SqlExpr e) {
        return e instanceof SqlExpr.IntLit i
                ? new SqlExpr.IntLit(Math.max(0, i.value()))
                : SqlExpr.Call.of(SqlFn.GREATEST, e, new SqlExpr.IntLit(0));
    }

    /** 0-based → 1-based shift, constant-folded for literals. */
    static SqlExpr onePlus(SqlExpr e) {
        return e instanceof SqlExpr.IntLit i
                ? new SqlExpr.IntLit(i.value() + 1)
                : SqlExpr.Call.of(SqlFn.PLUS, e, new SqlExpr.IntLit(1));
    }
}
