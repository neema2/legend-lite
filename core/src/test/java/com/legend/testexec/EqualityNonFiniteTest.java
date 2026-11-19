// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A NON-FINITE CARRIER UNDER A DECIMAL DECLARATION IS JUDGED, NEVER A NumberFormatException (the PR #118 fix batch's
 * audit, JUDGES-WORDS-6, "suspected", confirmed here 2026-11-19 on branch claude/numbers-q1): a Decimal property mapped
 * to a DOUBLE column holding NaN or an infinity reaches the host judge as a {@link Double} under the declared kind
 * {@code Decimal}, and the DECIMAL branch read it as {@code new BigDecimal("NaN")}, which throws -- an assert errored
 * where a verdict was due. A Decimal holds no NaN and no infinity, so such a pair is compared as the double it is,
 * as the FLOAT branch compares one: NaN equal to nothing, an infinity equal to itself, never to a number. The host's
 * sort (assertSameElements) read every number the same way and threw on a NaN Float.
 */
class EqualityNonFiniteTest {

    private static boolean same(Object e, String ek, Object a, String ak) {
        return Equality.same(new Equality.Typed(e, ek), new Equality.Typed(a, ak), HostRules.LITE);
    }

    @Test
    void aDecimalDeclaredNonFiniteCarrierIsJudged() {
        assertFalse(same(Double.NaN, "Decimal", Double.NaN, "Decimal"), "NaN equals nothing, NaN included");
        assertTrue(same(Double.POSITIVE_INFINITY, "Decimal", Double.POSITIVE_INFINITY, "Decimal"));
        assertFalse(same(Double.NEGATIVE_INFINITY, "Decimal", Double.POSITIVE_INFINITY, "Decimal"));
        // a finite Decimal against a non-finite one, either side: not equal, judged
        assertFalse(same(new BigDecimal("1.5"), "Decimal", Double.NaN, "Decimal"));
        assertFalse(same(Double.POSITIVE_INFINITY, "Decimal(38,18)", new BigDecimal("1.5"), "Decimal(38,18)"));
        // a finite double under a Decimal declaration is still compared by its value
        assertTrue(same(1.5d, "Decimal", new BigDecimal("1.50"), "Decimal"));
    }

    @Test
    void theHostsSortOrdersNonFiniteFloats() {
        List<Equality.Typed> sorted = Equality.sorted(Equality.Typed.all(
                List.of(Double.NaN, 2.0d, Double.NEGATIVE_INFINITY, 1.5d, Double.POSITIVE_INFINITY), "Float"));
        // Java's order of doubles (Double.compare): the infinities at the ends, NaN after every number
        assertEquals(List.of(Double.NEGATIVE_INFINITY, 1.5d, 2.0d, Double.POSITIVE_INFINITY, Double.NaN),
                sorted.stream().map(Equality.Typed::value).toList());
        // finite numbers keep their exact order across carriers (a Decimal beside a Float and an Integer)
        List<Equality.Typed> mixed = Equality.sorted(Equality.Typed.all(
                List.of(new BigDecimal("0.30000000000000001"), 0.3d, 1L), null));
        assertEquals(List.of(0.3d, new BigDecimal("0.30000000000000001"), 1L),
                mixed.stream().map(Equality.Typed::value).toList());
    }
}
