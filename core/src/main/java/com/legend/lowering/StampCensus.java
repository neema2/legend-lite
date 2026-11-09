// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlType;

/**
 * THE MULTIPLICITY-STAMP INVARIANT (stamp program; flipped from the
 * original measurement instrument 2026-08-20 at census zero on the
 * full corpus and all five PCT suites): at the scalar lowering funnel,
 * compare each expression's PURE multiplicity stamp against the
 * PROVABLE shape of the SQL it lowered to — a provable stamp/shape lie
 * THROWS, always on, build-breaking. {@code LL_STAMP_COUNT=1}
 * downgrades to print-and-continue census mode for measurement sweeps.
 *
 * <p><strong>Honest scope</strong> (multiplicity audit
 * docs/MULTIPLICITY_AUDIT_2026_08_20.md §2 — adopted): this check is
 * CONSISTENCY, not soundness. Lowering picks the SQL shape FROM the
 * stamp at many sites, so a stamp that is wrong but consistently
 * propagated fires nothing; the checker-side defects the audit
 * catalogues (§1) are invisible here by construction. Only PROVABLE
 * shapes fire (a definite list value under a to-one stamp; a definite
 * scalar literal under a many stamp) and the DESIGNED (stamp, carrier)
 * table below names every adjudicated convention. Absence is not proof
 * of health, but firing IS proof of a lie — that hedge is the whole
 * claim, nothing stronger.
 */
public final class StampCensus {

    private StampCensus() {
    }

    /** Census mode: print-and-continue (measurement sweeps). Without
     * it the invariant THROWS — the permanent build-breaking check. */
    private static final boolean COUNT_MODE =
            System.getenv("LL_STAMP_COUNT") != null;

    // (the test label is base's one, Attribution.now(): 2026-11-09, one label for every census)


    /** The scalar-funnel hook: spec's stamp vs the lowered expression's
     * provable shape. */
    static void check(TypedSpec spec, SqlExpr e) {
        Multiplicity m = spec.info().multiplicity();
        if (!(m instanceof Multiplicity.Bounded b)) {
            // a VARIABLE stamp surviving to lowering is its own finding —
            // lowering should only ever see concrete bounds
            fire("VAR-STAMP-AT-LOWERING " + digest(spec)
                    + " test=" + com.legend.base.Attribution.now());
            return;
        }
        boolean scalarStamp = b.upper() != null && b.upper() <= 1;
        // NullLit is shape-AMBIGUOUS by the carrier convention (SQL NULL
        // carries pure's EMPTY in scalar AND list positions — ListShapes
        // classifies it list-shaped for list consumers, but under a
        // scalar stamp it is the designed [0..1] empty): never a lie.
        // A CASE whose every branch is NULL-only is that value under a
        // condition (2026-10-14, the tenth line's find: an all-NULL CASE,
        // reached from both islands, fired the invariant below).
        if (nullOnly(e)) {
            return;
        }
        // DESIGNED CARRIER TABLE — positive one-arm EXPECTATIONS, never
        // blanket skips (user ruling: "no way to abuse"; each row states
        // how ONE value of a COMPOSITE type is physically represented —
        // the same kind of sentence as "one String is a VARCHAR"):
        // 1. ONE relation value: the row-collect list OR the
        //    single-column cell collapse — BOTH designed, so only the
        //    scalar-stamp/list-shape arm is satisfied by type; a
        //    MANY-stamped relation node with provably-scalar SQL still
        //    fires below.
        // 2. ONE instance (ctor): the struct/canonical-layout ArrayLit
        //    (STRUCT_VALUES design) — satisfied only for the exact
        //    (ctor node, ArrayLit) pair.
        // 3. ONE platform List<T> object: the SQL array (engine List
        //    semantics) — satisfied only on the scalar-stamp arm; a
        //    many-stamped List node with provably-scalar SQL fires.
        // (A former row 4 — many-stamped property reads with scalar
        // SQL — is DELETED: its one producer was scalarMapAsProject
        // copying the collection mult onto the u_map__ COLUMN; the
        // column now declares the per-cell mult and property reads are
        // checked in BOTH arms like every other node.)
        boolean designedListCarrier =
                com.legend.compiler.element.type.Type
                        .schemaView(spec.info().type()) != null
                || (spec instanceof com.legend.compiler.spec.typed.TypedNewInstance
                        && e instanceof SqlExpr.ArrayLit)
                || com.legend.compiler.element.type.PlatformTypes
                        .isListCarrier(spec.info().type());
        if (scalarStamp && !designedListCarrier && listShaped(e)) {
            fire("ONE-STAMP/LIST-SHAPE mult=[" + b.lower() + ".."
                    + b.upper() + "] sql=" + e.getClass().getSimpleName()
                    + " " + digest(spec) + " test=" + com.legend.base.Attribution.now());
            return;
        }
        if (!scalarStamp && definitelyScalar(e)) {
            fire("MANY-STAMP/SCALAR-SHAPE mult=[" + b.lower() + ".."
                    + (b.upper() == null ? "*" : b.upper()) + "] sql="
                    + e.getClass().getSimpleName() + " " + digest(spec)
                    + " test=" + com.legend.base.Attribution.now());
        }
    }

    /** The NULL value, bare or under a CASE whose every branch is (no
     * ELSE is SQL's NULL): shape-ambiguous by the carrier convention. */
    static boolean nullOnly(SqlExpr e) {
        return e instanceof SqlExpr.NullLit
                || (e instanceof SqlExpr.Case cs
                        && cs.whens().stream().allMatch(w -> nullOnly(w.then()))
                        && (cs.otherwise() == null || nullOnly(cs.otherwise())));
    }

    /** Count mode prints; invariant mode THROWS — a provable stamp lie
     * is a compiler bug, never a value to compute with. */
    private static void fire(String line) {
        if (COUNT_MODE) {
            System.err.println("[stamp] " + line);
            return;
        }
        throw new IllegalStateException(
                "MULTIPLICITY-STAMP INVARIANT VIOLATED (stamp program,"
                + " docs/STAMP_DISCIPLINE_PROGRAM.md): " + line);
    }

    /** The value is DEFINITELY list-shaped at SQL level — the
     * invariant's own evidence procedure (absorbed from the dissolved
     * ListShapes). Audit correction: the old claim here that
     * "production code never consults shape" was FALSE — a handful of
     * pre-dialect shape-sniffs still decide behaviour (audit §2 lists
     * them); they are debt, tracked in the audit program, and this
     * helper is not their license. */
    private static boolean listShaped(SqlExpr e) {
        return e instanceof SqlExpr.ArrayLit || e instanceof SqlExpr.NullLit
                || e instanceof SqlExpr.CompactList
                || (e instanceof SqlExpr.ScalarSubquery sq
                        && listValuedSubquery(sq))
                || (e instanceof SqlExpr.Cast ct
                        && ct.target() instanceof com.legend.sql.SqlType.Array)
                || (e instanceof SqlExpr.Call c && c.fn().producesList())
                // a CASE is list-shaped when every branch is (audit §1a
                // stopgap: slice's bound-check wrapper and lowered ifs
                // were the census's blind spot — shared with the old
                // toOne rule, which now decides from the TYPED node)
                || (e instanceof SqlExpr.Case cs
                        && cs.whens().stream()
                                .allMatch(w -> listShaped(w.then()))
                        && cs.otherwise() != null
                        && listShaped(cs.otherwise()));
    }

    /** A scalar subquery whose single projection is a LIST-building
     * aggregate (the values-collection reader) — its VALUE is a list. */
    static boolean listValuedSubquery(SqlExpr.ScalarSubquery sq) {
        return sq.subquery() instanceof com.legend.sql.SqlSelect ss
                && ss.projections().size() == 1
                && (ss.projections().get(0).expr()
                                instanceof com.legend.sql.SqlAgg.Reducer r
                            && r.fn() == com.legend.sql.SqlAgg.Fn.LIST
                        || ss.projections().get(0).expr()
                                instanceof SqlExpr.OrderedListAgg);
    }

    /** PROVABLY a single scalar value — literals and scalar-typed casts
     * only (a column or call could carry a list; never guessed). */
    private static boolean definitelyScalar(SqlExpr e) {
        return switch (e) {
            case SqlExpr.IntLit ignored -> true;
            case SqlExpr.FloatLit ignored -> true;
            case SqlExpr.DecimalLit ignored -> true;
            case SqlExpr.StringLit ignored -> true;
            case SqlExpr.DatePartLit ignored -> true;
            case SqlExpr.JsonKindLit ignored -> true;
            case SqlExpr.TypeNameLit ignored -> true;
            case SqlExpr.BoolLit ignored -> true;
            case SqlExpr.DateLit ignored -> true;
            case SqlExpr.TimestampLit ignored -> true;
            case SqlExpr.TimeLit ignored -> true;
            case SqlExpr.Cast c -> c.target() instanceof SqlType.Scalar;
            default -> false;
        };
    }

    /** Cause-classification digest: the node kind plus the ONE detail
     * that names its origin (callee FQN, property, variable). */
    private static String digest(TypedSpec spec) {
        StringBuilder sb = new StringBuilder("node=")
                .append(spec.getClass().getSimpleName());
        if (spec instanceof TypedNativeCall c) {
            sb.append(" callee=").append(c.callee().qualifiedName());
            if (!c.args().isEmpty()) {
                TypedSpec a0 = c.args().get(0);
                sb.append(" arg0=").append(a0.getClass().getSimpleName());
                if (a0 instanceof TypedNativeCall ic) {
                    sb.append('(').append(ic.callee().qualifiedName()
                            .substring(ic.callee().qualifiedName()
                                    .lastIndexOf(':') + 1)).append(')');
                } else if (a0 instanceof TypedPropertyAccess ip) {
                    sb.append('(').append(ip.property()).append(')');
                }
            }
        } else if (spec instanceof TypedPropertyAccess p) {
            sb.append(" prop=").append(p.property());
        } else if (spec instanceof TypedVariable v) {
            sb.append(" name=").append(v.name());
        }
        return sb.toString();
    }
}
