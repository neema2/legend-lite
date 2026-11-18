// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

/**
 * THE DATABASE JUDGE'S STATIC GATE for an assert (J41, 2026-11-18: until then an enum whose two engine-text constants
 * the product read; the edge's engine-text arm now answers its own gate, {@link EngineTestText#gate}): whether the
 * database judge plans the assert's verdict without reading a side at planning ({@link Kind#PLANS}), raises at
 * planning ({@link Kind#RAISES}), or does not plan it ({@link Kind#UNPLANNED}, the body is given back) -- and, for a
 * planned one, whether the arm plans under the style's text collector ({@code collects}: a side the verdict row
 * computes) and whether its verdict is APPEALED at its step ({@code appealed}: the appeal reads the body's store, so a
 * system-routed body does not run it). Data, nothing evaluated.
 *
 * @param kind     the gate's answer
 * @param collects whether the arm plans under the style's text collector ({@code EngineTestText.batchTexts})
 * @param appealed whether the verdict may be appealed at its step, reading the body's store
 */
public record DatabaseGate(Kind kind, boolean collects, boolean appealed) {

    /** The gate's three answers. */
    public enum Kind { PLANS, RAISES, UNPLANNED }

    public static final DatabaseGate PLANS = new DatabaseGate(Kind.PLANS, false, false);
    public static final DatabaseGate RAISES = new DatabaseGate(Kind.RAISES, false, false);
    public static final DatabaseGate UNPLANNED = new DatabaseGate(Kind.UNPLANNED, false, false);

    /** A planned verdict that may be appealed at its step; {@code collects} where the arm plans under the collector. */
    public static DatabaseGate appealed(boolean collects) {
        return new DatabaseGate(Kind.PLANS, collects, true);
    }

    /** Two planned elements' gate, one quantified assert's: planned, collecting or appealed where either is. */
    DatabaseGate and(DatabaseGate other) {
        return new DatabaseGate(Kind.PLANS, collects || other.collects, appealed || other.appealed);
    }
}
