// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

/**
 * LEGEND-ENGINE'S NULL CELL WORD IN A RENDERED GOLDEN ({@code TDSNull}, tds.pure:131-133; census C1, the orchestrator's
 * ruling of 2026-11-17): the edge's form of the two-form {@link com.legend.compiler.spec.GoldenNullWord} kind.
 */
public enum UpstreamGoldenNullWord implements com.legend.compiler.spec.GoldenNullWord {
    INSTANCE;

    @Override
    public boolean isNullWord(String cell) {
        return cell.equals(Presenter.TDS_NULL);
    }
}
