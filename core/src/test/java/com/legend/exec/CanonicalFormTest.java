// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import org.junit.jupiter.api.Test;

import com.legend.values.PureDateLiteral;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * R0 spec pins (docs/CANONICAL_FORM_SPEC.md §2) — each row cites its
 * H-table witness from docs/CANONICAL_RENDER_HOMEWORK.md. These pin the
 * HOST reference render the R1 divergence instrument compares with;
 * R2's SQL renders are held to the same rows.
 */
// reads process-wide counters (CanonicalDivergence, Census) that every query bumps: runs alone if JUnit ever runs
// classes in parallel, so no other test's calls land between its reset and its assert (Bazel workplan P3-04, A9)
@org.junit.jupiter.api.parallel.Isolated
class CanonicalFormTest {

    private static String text(Object v) {
        return assertInstanceOf(CanonicalForm.Result.Text.class,
                CanonicalForm.render(v)).value();
    }

    private static String residue(Object v) {
        return assertInstanceOf(CanonicalForm.Result.Residue.class,
                CanonicalForm.render(v)).reason();
    }

    @Test
    void integersAndBooleans() {
        assertEquals("1", text(1L));
        assertEquals("-1", text(-1));
        assertEquals("true", text(true));
        assertEquals("false", text(false));
    }

    /** H1 testFloatToString×4: fixed-point always, shortest form,
     * integral keeps .0, leading zero enforced. */
    @Test
    void floatsNeverExponent() {
        assertEquals("3.14", text(3.14));
        assertEquals("17.0", text(17.000));
        assertEquals("134210000.0", text(1.3421e8));
        assertEquals("0.000000013421", text(134.21e-10));
        assertEquals("0.01", text(.01));
    }

    /** X2 (VERDICT_RULE_AUDIT): SCALE-PRESERVING — engine Decimal
     * equality is getValue().equals, so scale IS identity and the
     * canon keeps it. */
    @Test
    void decimalsScalePreserving() {
        assertEquals("8.00", text(new BigDecimal("8.00")));
        assertEquals("3.80", text(new BigDecimal("3.80")));
        assertEquals("800", text(new BigDecimal("8.00E+2")));
    }

    /** Spec §4: non-finite floats and -0.0 are OUT of the claimed
     * domain — named residue, never a guessed spelling. */
    @Test
    void residues() {
        assertEquals("non-finite-float", residue(Double.NaN));
        assertEquals("non-finite-float", residue(Double.POSITIVE_INFINITY));
        // zeros UNIFY (spec §3 — pure grants 0.0 == -0.0; witness
        // parseFloat('-000.000') from the PCT relational lane)
        assertEquals("0.0", text(-0.0));
        assertEquals("0.0", text(0.0));
    }

    /** H1 temporals over THE wire carrier (PureDateLiteral, D-arc):
     * date-only precisions print bare; time-bearing take +0000;
     * WRITTEN precision survives (.000 ≠ none — the wire limit the
     * java.time carrier had is GONE). A java temporal is residue. */
    @Test
    void temporals() {
        assertEquals("2014", text(PureDateLiteral.parse("2014")));
        assertEquals("2014-01", text(PureDateLiteral.parse("2014-01")));
        assertEquals("2014-01-01", text(PureDateLiteral.parse("2014-01-01")));
        assertEquals("2014-01-01T00:00+0000",
                text(PureDateLiteral.parse("2014-01-01T00:00")));
        assertEquals("2014-01-01T10:01:35.231+0000",
                text(PureDateLiteral.parse("2014-01-01T10:01:35.231")));
        assertEquals("2014-01-01T10:01:35.000+0000",
                text(PureDateLiteral.parse("2014-01-01T10:01:35.000")));
        // GMT normalization: -0500 input prints shifted +0000 (H1)
        assertEquals("2014-01-01T15:01+0000",
                text(PureDateLiteral.parse("2014-01-01T10:01-0500")));
        // fetch-seam leak detector: a java.time temporal is RESIDUE
        assertEquals("unmodeled-kind:LocalDate",
                residue(java.time.LocalDate.of(2014, 1, 1)));
    }

    /** H1 testListToString: multi-element sides take the list form;
     * a single element renders as the scalar. */
    @Test
    void sides() {
        assertEquals("[1, 2, 3]", assertInstanceOf(
                CanonicalForm.Result.Text.class,
                CanonicalForm.renderSide(List.of(1L, 2L, 3L))).value());
        assertEquals("a", assertInstanceOf(
                CanonicalForm.Result.Text.class,
                CanonicalForm.renderSide(List.of((Object) "a"))).value());
        assertEquals("[]", assertInstanceOf(
                CanonicalForm.Result.Text.class,
                CanonicalForm.renderSide(List.of())).value());
    }

    /** The KIND-QUALIFIED byte compare (spec §3 amendment): the render
     * is not injective across kinds — String "8" vs Integer 8 must NOT
     * byte-agree, and the numeric tower shares one kind class so
     * Integer 8 vs Decimal 8D still does. */
    @Test
    void kindQualifiedByteCompare() {
        CanonicalDivergence.reset();
        // cross-kind same-text: lattice false, byte false — AGREE
        CanonicalDivergence.probeEqual("assertEquals",
                List.of((Object) "8"), List.of((Object) 8L), false);
        // string vs temporal: a type mismatch on both channels now
        // (the string-carrier bridge died with the D-arc cutover)
        CanonicalDivergence.probeEqual("assertEquals",
                List.of((Object) "2014-01-01"),
                List.of((Object) PureDateLiteral.parse("2014-01-01")), false);
        // X1: cross-kind numeric is FALSE on BOTH channels now
        CanonicalDivergence.probeEqual("assertEquals",
                List.of((Object) 8L),
                List.of((Object) new BigDecimal("8.00")), false);
        CanonicalDivergence.probeEqual("assertEquals",
                List.of((Object) 1L), List.of((Object) 2L), false);
        assertEquals("agree=4 disagree=0 residue=0 | sql-verdict"
                + " agree=0 disagree=0 declined=0 ulp-policy=0"
                + " row-order-canon=0",
                CanonicalDivergence.summary());
        CanonicalDivergence.reset();
    }
}
