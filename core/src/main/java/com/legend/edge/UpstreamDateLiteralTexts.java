// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

/**
 * A DATE LITERAL'S TEXT AS LEGEND-PURE WRITES IT ({@code toRepresentation}: '%' and its written precision, the zone
 * where a minute is written; census C1, the orchestrator's ruling of 2026-11-17): the edge's form of the two-form
 * {@link com.legend.testplan.DateLiteralTexts} kind.
 */
public enum UpstreamDateLiteralTexts implements com.legend.testplan.DateLiteralTexts {
    INSTANCE;

    @Override
    public String text(com.legend.values.PureDateLiteral d) {
        return "%" + com.legend.testexecutionplan.Verdicts.pureText(d);
    }
}
