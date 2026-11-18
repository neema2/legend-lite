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
 * <ul>
 * <li>{@link #raise}: a check the plan writes -- the product writes every one ({@code checked}); legend-engine's pure/v1
 * route writes none for its census class A, the plain operation, the database's own error passing through.</li>
 * <li>{@link #view}: a view at relation position -- the product lowers its body flat (a name is not a nesting);
 * legend-engine's SQL nests it as an aliased derived table ({@code ViewSelectSQLQuery}).</li>
 * <li>{@link #wireCast}: a mapping's wire coercion -- the product writes the SQL cast (DuckDB does not wire-convert,
 * audit 19 F7); legend-engine's SQL leaves it to its reader, which converts on the wire.</li>
 * <li>{@link #executeValues}: the root of an engine {@code execute(...)}'s values read as a serialize -- the product's
 * stays the array, lite's own root (the user's ruling of 2026-11-03: "the right clean thing in lite, the
 * backward-compatible thing in compatibility mode"); legend-engine's JsonBuilder writes exactly one object bare (the
 * audit's J3, 2026-11-17: until then applied ungated).</li>
 * <li>{@link #relations}, {@link #keyedCounts}, {@link #computedPruning} (the edge audit's J23, 2026-11-17, the
 * orchestrator's ruling (a): until then three LoweringShape flags only the edge set false): the product writes lean SQL --
 * a relation read more than once written once, a count a declared key makes one folded to 1, an unread computed
 * projection dropped; legend-engine's SQL does none of them.</li>
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

    /** A view's relation: its body lowered ({@code body}), named {@code view}; {@code alias} mints an alias if one is
     *  written. */
    SqlSelect view(SqlSelect body, Supplier<String> alias, String view);

    /** A mapping's wire coercion of {@code value}: the cast written ({@code written}) or the value as read. */
    SqlExpr wireCast(SqlExpr value, Supplier<SqlExpr> written);

    /** The root of an engine {@code execute(...)}'s serialized values: lite's array ({@code array}), or one object bare
     *  and zero or several as the array ({@code oneBare}, legend-engine's JsonBuilder cardinality rule). */
    SqlSelect executeValues(Supplier<SqlSelect> array, Supplier<SqlSelect> oneBare);

    /** The relational isNumeric of {@code s}. */
    SqlExpr isNumeric(SqlExpr s);

    /** An object reference's connection segment (the audit's J12, 2026-11-17): the product's names no connection, the
     *  empty text; legend-engine's its H2 test-connection JSON, whatever the database. */
    String objectReferenceConnection();

    /** A statement whose relations read more than once are written once ({@code shared}), or at each read
     *  ({@code unshared}). */
    com.legend.sql.SqlQuery relations(Supplier<com.legend.sql.SqlQuery> shared,
            Supplier<com.legend.sql.SqlQuery> unshared);

    /** {@code q} with the counts a declared key makes one folded to 1 ({@code fold}), or as lowered. */
    com.legend.sql.SqlQuery keyedCounts(com.legend.sql.SqlQuery q,
            java.util.function.UnaryOperator<com.legend.sql.SqlQuery> fold);

    /** A statement with its unread computed projections dropped ({@code pruned}), or kept ({@code kept}); unread columns
     *  are dropped either way. */
    com.legend.sql.SqlQuery computedPruning(Supplier<com.legend.sql.SqlQuery> pruned,
            Supplier<com.legend.sql.SqlQuery> kept);

    /** A column's text parsed to a Decimal ({@code parseDecimal}'s one-argument form; the audit's J1, RULED by the user
     *  2026-11-17): the product's an exact decimal at its Decimal's precision; legend-engine's H2 its fixed
     *  {@code DECIMAL(5,2)}. */
    SqlExpr parsedDecimal(SqlExpr text);

    /** A text parsed to JSON ({@code parseJSON}; the audit's J2, RULED by the user 2026-11-17): the product's the text as
     *  written; legend-engine's with its parser's one tolerance, {@code '}{'} read as {@code '},{'}. */
    SqlExpr parsedJson(SqlExpr text);

    /** A filtered select ({@code filtered}) whose {@code EXISTS} conjuncts are kept as lowered, or written as joins to the
     *  distinct keys ({@code joinForm}). */
    SqlSelect existsJoin(SqlSelect filtered, java.util.function.UnaryOperator<SqlSelect> joinForm);
}
