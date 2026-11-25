// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * THE PRESENTER (Phase 3 of docs/records/total-wall-edge-program-2026-11-16.md, 2026-11-16). Its print of a value,
 * {@code legendPure(Object, String)}, and that print's tests were deleted by B2b's design audit of 2026-11-25: no
 * caller since the catalog's sentences (BE, 84a063611), and a second legend-pure Float printer beside
 * {@link PureValueText} (G1), which writes a Float by legend-pure's own DecimalFormat ({@code PureRelationTextTest}).
 */
class PresenterTest {

    @Test
    void anEmptyTdsCellIsLegendEnginesTdsNull() {
        assertEquals("TDSNull", Presenter.TDS_NULL);
    }
}
