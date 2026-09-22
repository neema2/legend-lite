// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

/**
 * R0's SCALAR-channel canonical render over wire values
 * (docs/CANONICAL_FORM_SPEC.md §2) — the HOST reference implementation.
 * Charter (R1): this is the referee's half of the byte channel — the
 * divergence instrument computes {@code render(e) == render(a)} NEXT TO
 * {@code PureAsserts.equal(e, a)} across the full corpus; R2's cutovers
 * move the render of record into SQL (Render), per family, against this
 * spec. It is NOT a verdict path: production verdicts never call it.
 *
 * <p>Every rule here cites an H-table row of
 * docs/CANONICAL_RENDER_HOMEWORK.md; a value outside the claimed domain
 * (§4: non-finite floats, -0.0, unmodeled kinds) returns
 * {@link Result.Residue}, never a guessed spelling.
 */
public final class CanonicalForm {

    private CanonicalForm() {
    }

    /** A render outcome: canonical text, or a named residue (out of the
     * byte channel's claimed domain). */
    public sealed interface Result {
        record Text(String value) implements Result {
        }

        record Residue(String reason) implements Result {
        }
    }

    /** Canonical text of one scalar wire value (spec §2). */
    public static Result render(@com.legend.base.Nullable Object v) {
        return switch (v) {
            case null -> new Result.Residue("null-scalar");
            // Integer: bare decimal, no decoration (H1)
            case Byte b -> new Result.Text(String.valueOf(b));
            case Short s -> new Result.Text(String.valueOf(s));
            case Integer i -> new Result.Text(String.valueOf(i));
            case Long l -> new Result.Text(String.valueOf(l));
            case BigInteger bi -> new Result.Text(bi.toString());
            case Boolean b -> new Result.Text(b.toString());
            // String: verbatim bytes (H1; toRepresentation quoting is a
            // DIFFERENT channel). The temporal string-carrier bridge is
            // the PROBE's pairwise concern, not this per-value rule.
            case String s -> new Result.Text(s);
            case Float f -> renderFloat(f.doubleValue());
            case Double d -> renderFloat(d);
            // Decimal: SCALE-PRESERVING (X2 — engine equality is
            // getValue().equals; the scale IS identity)
            case BigDecimal bd -> new Result.Text(bd.toPlainString());
            // TEMPORAL: PureDateLiteral is THE wire carrier (D-arc) —
            // written precision survives, so the render is exact per H1:
            // date-only precisions print bare, time-bearing forms take
            // the GMT-normalized +0000 suffix. A java.sql or java.time
            // temporal reaching here is a fetch-seam LEAK and reports
            // as unmodeled-kind residue, never grows an arm.
            case com.legend.values.PureDateLiteral d ->
                    new Result.Text(d.precision().atLeast(
                            com.legend.values.PureDateLiteral.Precision.HOUR)
                            ? d.toEngineString() + "+0000"
                            : d.toEngineString());
            default -> new Result.Residue(
                    "unmodeled-kind:" + v.getClass().getSimpleName());
        };
    }

    /** A collection side: one element renders as the scalar; otherwise
     * pure's list form {@code [a, b, c]} (H1 testListToString). Empty
     * renders {@code []}. Any element residue poisons the side. */
    public static Result renderSide(List<Object> side) {
        if (side.size() == 1) {
            return render(side.get(0));
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < side.size(); i++) {
            switch (render(side.get(i))) {
                case Result.Text t -> sb.append(i > 0 ? ", " : "")
                        .append(t.value());
                case Result.Residue r -> {
                    return r;
                }
            }
        }
        return new Result.Text(sb.append(']').toString());
    }

    /**
     * Float: fixed-point ALWAYS (pure never prints exponent notation —
     * H1 testFloatToString×4; DuckDB's bare CAST diverges on small
     * magnitudes, the R1 finding). {@code BigDecimal.valueOf} rides
     * {@code Double.toString}'s shortest-representation, which already
     * collapses trailing zeros and keeps {@code .0} on integral floats;
     * {@code toPlainString} removes the exponent. Non-finite and -0.0
     * are OUT of the claimed domain (spec §4 — ZERO witnesses, H6).
     */
    private static Result renderFloat(double d) {
        if (!Double.isFinite(d)) {
            return new Result.Residue("non-finite-float");
        }
        if (d == 0.0) {
            // ZEROS UNIFY (spec §3; witness parseFloat('-000.000') —
            // pure grants 0.0 == -0.0, so the byte channel must render
            // both '0.0'; the old negative-zero residue predated the
            // witness)
            return new Result.Text("0.0");
        }
        String plain = BigDecimal.valueOf(d).toPlainString();
        // integral doubles keep .0 (Double.toString gives "17.0");
        // BigDecimal.valueOf(17.0) has scale 1 so toPlainString keeps it
        return new Result.Text(plain.contains(".") ? plain : plain + ".0");
    }
}
