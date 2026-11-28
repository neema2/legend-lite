// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.builtin.Pure;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

import java.util.List;
import java.util.Map;

/**
 * The multiplicity-COERCION rule family — user {@code toOne}, the
 * synthesized {@code trustOne} conformance, and {@code toOneMany} —
 * split from {@link Scalars} at the shape limit (the Comparators
 * precedent: registration doctrine unchanged, size only). The C2
 * provenance split and the 2026-08-26 value-lane ruling ("if not
 * relational, follow the type system literally") both live here.
 */
final class Coercions {

    private Coercions() {
    }

    static void register(Map<com.legend.model.FunctionId, Scalars.Rule> rules) {
        // toOne erases in SQL (MUST-honor: multiplicity narrowing is a
        // no-op value-wise). C2 (STAMP_DISCIPLINE_PROGRAM) MEASURED the
        // blanket unwrap alternative and it REGRESSED milestoning −16 /
        // union −23: the list-shaped operands here are mostly resolver-
        // SYNTHESIZED conformance toOnes over values-reader subqueries
        // whose LIST downstream genuinely consumes — the [1] stamp is
        // the lie, not the shape. The C2 fix is provenance-split
        // (user toOne = unwrap; synthesized conformance = ride-through
        // by design; values-reader stamps fixed at their producer),
        // recorded in the program doc — not a blanket emission.
        // USER toOne is CHECKED on BOTH bounds (multiplicity audit
        // slice 3): pure raises 'Cannot cast a collection of size N to
        // multiplicity [1]' for N != 1 — the old default arm DELETED
        // the call (navigation/column/variable operands were never
        // guarded; two rows flowed silently). The carrier follows the
        // operand's STAMP: many = list-checked, [0..1] = null-checked
        // scalar, [1..1] = already exactly one (identity). The
        // SYNTHESIZED conformance population spells trustOne (below)
        // and stays unguarded BY NAME — the C2 provenance split.
        for (com.legend.model.FunctionId f : Pure.AT_MULTIPLICITY_TO_ONE) {
            rules.put(f, (Scalars.StyledRule) (n, args, styled) -> {
                // AGG-STRIP (stamp C2): a LIST-collecting subquery
                // operand becomes the NATIVE scalar subquery — SQL's
                // own checked toOne (row-lane flow convention,
                // ADJUDICATED: NULL cell == empty; compacted carriers
                // strip through their wrapper).
                SqlExpr stripped = Scalars.aggStrip(
                        args.get(0) instanceof SqlExpr.CompactList cpl
                                ? cpl.list() : args.get(0));
                if (stripped != null) {
                    return stripped;
                }
                Multiplicity.Bounded m = n.args().get(0).info()
                        .multiplicity().requireBounded("toOne operand");
                // STATICALLY empty ([] literal): pure raises — size 0
                if (m.upper() != null && m.upper() == 0) {
                    return SqlExpr.Call.of(SqlFn.ERROR, SqlExpr.RaiseText.placed(
                            com.legend.sql.RaiseKind.SIZE_NOT_MULTIPLICITY, "[1]"));
                }
                // VALUE-LANE collections raise pure's size error in the
                // database. Lane from the TYPED OPERAND (CollectionLanes
                // — audit §1a); the guard counts the COMPACTED carrier;
                // scalar-carried ifs FLOW (no list to count).
                if (m.isMany() && CollectionLanes.valueLane(n.args().get(0))
                        && !CollectionLanes.scalarCarriedIf(
                                n.args().get(0))) {
                    return new SqlExpr.CheckedOne(
                            new SqlExpr.CompactList(args.get(0)), false, false,
                            Scalars.exactSize(n.args().get(0)));
                }
                // a LIST-producing call over the relation lane (a raw grid's
                // rows.values — getH2Versions): the SQL value IS a list, so
                // toOne is its one element, checked
                if (m.isMany() && args.get(0) instanceof SqlExpr.Call lc
                        && lc.fn().producesList()) {
                    return new SqlExpr.CheckedOne(
                            new SqlExpr.CompactList(args.get(0)), false, false,
                            Scalars.exactSize(n.args().get(0)));
                }
                // VALUE-LANE [0..1] (user ruling 2026-08-26, "if not
                // relational, follow the type system literally"): a
                // store-free maybe-empty — []->first(), a value
                // expression's optional — NULL-checks with pure's own
                // size-0 raise (the interpreted lane is the referee for
                // store-free expressions; the flow arm below is the
                // RELATIONAL lane's, where the engine's own goldens
                // bless flowing NULLs). Same lane discriminator as the
                // many-arm above (audit §1a; scalar-carried ifs keep
                // their adjudicated flow).
                if (!m.isMany() && m.lower() == 0
                        && CollectionLanes.valueLane(n.args().get(0))
                        && !CollectionLanes.scalarCarriedIf(
                                n.args().get(0))) {
                    return Scalars.guarded(n,
                            new SqlExpr.Call(SqlFn.IS_NULL,
                                    List.of(args.get(0))),
                            SqlExpr.RaiseText.placed(com.legend.sql.RaiseKind.SIZE_NOT_MULTIPLICITY, "[1]"),
                            args.get(0), styled);
                }
                // Everything else — ROW-LANE [0..1] scalar reads AND
                // many-stamped ROW-LANE collections (correlated
                // navigations, window reads) — is the engine's
                // relational lane: its own compilation of toOne is
                // processNoOp, and SQL cannot tell a NULL cell from an
                // empty. Flow (ADJUDICATED vs audit §3, with the engine
                // as the reference; the milestoned-qualifier corpus row
                // is the witness).
                return args.get(0);
            });
        }
        // trustOne — the SQL-lane conformance wrap (Lite.TRUST_ONE):
        // IDENTITY, no guard; SQL null-propagates (the engine's
        // processNoOp / no-guard qualifier behavior). This is the
        // synthesized population the C2 provenance split names.
        for (com.legend.model.FunctionId f : Pure.AT_LEGEND_LITE_TRUST_ONE) {
            rules.put(f, (n, args) -> args.get(0));
        }
        // cellText -- a TDS cell's text (Lite.CELL_TEXT): the product's empty where the cell is; a compatibility
        // door's row names the rule that writes legend-engine's word there (NamedRule.EMPTY_CELL_TEXT_AS_TDS_NULL;
        // phase D's D1.3, 2026-11-15, and the edition program's step 8, 2026-11-28)
        // typeRaise -- a run-time type check's failure (Lite.TYPE_RAISE, W5 of docs/records/d-plan-2026-11-16.md): the
        // product's raise of the kind its first argument names, the two types its operands; typed as the position it
        // stands in, as fail's is
        for (com.legend.model.FunctionId f : Pure.AT_LEGEND_LITE_TYPE_RAISE) {
            rules.put(f, (n, args) -> {
                if (!(args.get(0) instanceof com.legend.sql.SqlExpr.StringLit kind)
                        || !(args.get(1) instanceof com.legend.sql.SqlExpr.StringLit from)
                        || !(args.get(2) instanceof com.legend.sql.SqlExpr.StringLit to)) {
                    throw new IllegalStateException("typeRaise's arguments are the resolver's literals: " + args);
                }
                com.legend.sql.RaiseKind k = com.legend.sql.RaiseKind.valueOf(kind.value());
                // its operands: the two types, or a match's value's type alone
                com.legend.sql.SqlExpr raised = PureSql.raise(new com.legend.sql.SqlExpr.RaiseText(k,
                        List.of(from, to)), n.pos());
                return n.info().type() instanceof com.legend.compiler.element.type.Type.Primitive p && p != com.legend.compiler.element.type.Type.Primitive.BOOLEAN
                        ? new com.legend.sql.SqlExpr.Cast(raised, PureSql.type(p), com.legend.sql.SqlExpr.Reason.RAISE)
                        : raised;
            });
        }
        // assertRaise -- an in-body assert's failure with no message of its caller's (Lite.ASSERT_RAISE, item 3's S6,
        // 2026-11-23): the raise of the kind its argument names, no operand (the catalog words it); typed as the position
        // it stands in, as fail's is
        for (com.legend.model.FunctionId f : Pure.AT_LEGEND_LITE_ASSERT_RAISE) {
            rules.put(f, (n, args) -> {
                com.legend.sql.RaiseKind k = args.get(0) instanceof com.legend.sql.SqlExpr.StringLit kind
                        ? com.legend.sql.RaiseKind.valueOf(kind.value()) : null;
                if (k != com.legend.sql.RaiseKind.ASSERT_TRUE && k != com.legend.sql.RaiseKind.ASSERT_FALSE
                        && k != com.legend.sql.RaiseKind.ASSERT_NOT_EMPTY) {
                    throw new IllegalStateException("assertRaise's argument is the guard's assert kind: " + args);
                }
                com.legend.sql.SqlExpr raised = PureSql.raise(new com.legend.sql.SqlExpr.RaiseText(k, List.of()),
                        n.pos());
                return n.info().type() instanceof com.legend.compiler.element.type.Type.Primitive p && p != com.legend.compiler.element.type.Type.Primitive.BOOLEAN
                        ? new com.legend.sql.SqlExpr.Cast(raised, PureSql.type(p), com.legend.sql.SqlExpr.Reason.RAISE)
                        : raised;
            });
        }
        for (com.legend.model.FunctionId f : Pure.AT_LEGEND_LITE_CELL_TEXT) {
            rules.put(f, (n, args) -> com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.CELL_TEXT, args.get(0)));
        }
        // toOneMany narrows [*] to [1..*]: at-least-one is CHECKED
        // (audit slice 3 — it was an unconditional no-op). A to-one
        // operand additionally re-carriers to the LIST the [1..*]
        // stamp promises downstream.
        for (com.legend.model.FunctionId f : Pure.AT_MULTIPLICITY_TO_ONE_MANY) {
            rules.put(f, (n, args) -> {
                Multiplicity.Bounded m = n.args().get(0).info()
                        .multiplicity().requireBounded("toOneMany operand");
                if (m.upper() != null && m.upper() == 0) {
                    return SqlExpr.Call.of(SqlFn.ERROR, SqlExpr.RaiseText.placed(
                            com.legend.sql.RaiseKind.SIZE_NOT_MULTIPLICITY, "[1..*]"));
                }
                // TYPED-operand lane — exactly as toOne above
                if (m.isMany() && CollectionLanes.valueLane(n.args().get(0))
                        && !CollectionLanes.scalarCarriedIf(
                                n.args().get(0))) {
                    return new SqlExpr.CheckedOne(
                            new SqlExpr.CompactList(args.get(0)), false,
                            true /* at least one */, Scalars.exactSize(n.args().get(0)));
                }
                if (m.isMany()) {
                    return args.get(0);   // row-lane collection: flow
                }
                // to-one operands re-carrier to the LIST the [1..*]
                // stamp promises; the row-lane [0..1] flows (see toOne)
                return new SqlExpr.ArrayLit(List.of(args.get(0)));
            });
        }
    }
}
