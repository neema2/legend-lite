// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.RaiseKind;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;

import java.util.function.Supplier;

/**
 * THE LOWERING'S TWO-FORM CHOICES (the audit's K5, 2026-11-17, the orchestrator's ruling: one two-form kind,
 * {@code PlanStyle.twoForm}): what the product writes in its own canonical form and the compatibility edge in
 * upstream's. The product never branches on an upstream flag: it asks its form, and {@link #PRODUCT} answers the
 * product's way; the edge's forms (in {@code com.legend.edge}) answer legend-engine's.
 *
 * <p>Six forms that were identical in both implementations ({@code view}, {@code wireCast}, {@code relations},
 * {@code keyedCounts}, {@code computedPruning}, {@code unionFrame}) were deleted by the edition's E0c, 2026-11-25
 * (docs/records/edition-census-2026-11-25.md).
 *
 * <ul>
 * <li>{@link #raise}: a check the plan writes -- the product writes every one ({@code checked}); legend-engine's pure/v1
 * route writes none for its census class A, the plain operation, the database's own error passing through.</li>
 * <li>{@link #executeValues}: a serialize's values where legend-engine's cardinality rule could apply -- the root of an
 * engine {@code execute(...)}'s values read (the audit's J3, 2026-11-17) and a serialize in scalar position (the lens
 * audit's L6, 2026-11-18: until then folded unconditionally) -- the product's stays the array, lite's own (the user's
 * ruling of 2026-11-03: "the right clean thing in lite, the backward-compatible thing in compatibility mode");
 * legend-engine's JsonBuilder writes exactly one object bare, the edge folding the wrapped array itself.</li>
 * <li>{@link #existsJoin} (the edge audit's J19, the user's ruling of 2026-11-17, "Yes to the rule"; register S367): a
 * filter's correlated {@code EXISTS} over a to-many join -- legend-engine's SQL writes its "second exists emission", a
 * join to the distinct keys ({@code ExistsJoinForm}), the edge's forms; the product's form the EXISTS as lowered, the
 * leanest SQL, a dialect that cannot correlate one decomposing it at render (ClickHouse, {@code sql.ExistsJoins}).</li>
 * <li>{@link #isNumeric}: the relational isNumeric -- the product's a real numeric check, the same on every database
 * ({@code SqlFn.IS_NUMERIC}; the user's ruling of 2026-11-17 23:05, census J20); legend-engine's H2 compares
 * {@code lower(s) = upper(s)}, the edge's.</li>
 * </ul>
 */
public interface LoweringForms {

    /** A raise of {@code kind}: the guarded operation ({@code checked}) or the plain one ({@code plain}). */
    SqlExpr raise(RaiseKind kind, Supplier<SqlExpr> checked, SqlExpr plain);

    /** A serialize's values as the product answers them ({@code own}: the object where the graph is to-one, the array
     *  where it is many -- the product's multiplicity fact): the product's form returns them unchanged; legend-engine's
     *  writes its JsonBuilder cardinality rule over them, in the edge's own code (the orchestrator's ruling of 2026-11-18,
     *  L-A.LoweringForms: the product hands the form its own answer and nothing it never uses). */
    SqlSelect executeValues(SqlSelect own);

    /** The relational isNumeric of {@code s}. */
    SqlExpr isNumeric(SqlExpr s);

    /** An object reference's connection segment (the audit's J12, 2026-11-17): the product's names no connection, the
     *  empty text; legend-engine's its H2 test-connection JSON, whatever the database. */
    String objectReferenceConnection();

    /** A column's text parsed to a Decimal ({@code parseDecimal}'s one-argument form; the audit's J1, RULED by the user
     *  2026-11-17): the product's an exact decimal at its Decimal's precision; legend-engine's H2 its fixed
     *  {@code DECIMAL(5,2)}. */
    SqlExpr parsedDecimal(SqlExpr text);

    /** A text parsed to JSON ({@code parseJSON}; the audit's J2, RULED by the user 2026-11-17): the product's the text as
     *  written; legend-engine's with its parser's one tolerance, {@code '}{'} read as {@code '},{'}. */
    SqlExpr parsedJson(SqlExpr text);

    /** A filtered select ({@code filtered}) whose {@code EXISTS} conjuncts are kept as lowered, or written as joins to the
     *  distinct keys over the statement's {@code sites} (the lens audit's item 1, 2026-11-18: the lowering hands its
     *  sites, never the rewrite; the edge's form calls the product's building block, sql.ExistsJoins). */
    SqlSelect existsJoin(SqlSelect filtered, ExistsSites sites);

    /** A fresh statement's WHERE merges (L10): the product's appends each predicate and keeps no zones; legend-engine's
     *  orders the conjuncts by zone and keeps each split for its exists form ({@link WhereZones}). */
    WhereZones whereZones();
}
