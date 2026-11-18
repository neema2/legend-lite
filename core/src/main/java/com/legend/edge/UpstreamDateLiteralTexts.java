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
        return "%" + pureText(d);
    }

    /** legend-pure's text of a date, as its {@code toString} writes one: the literal body at its written precision,
     *  with {@code +0000} from a minute on ({@code DateFormat.append} writes it inside its minute branch; an
     *  hour-only date has none: {@code 2014-03-10T16} in its {@code TestDateFormat}). Moved from the product's
     *  {@code testexecutionplan.Verdicts} (census C5's Verdicts row, 2026-11-17: its last reader). */
    static String pureText(com.legend.values.PureDateLiteral d) {
        return d.precision().atLeast(com.legend.values.PureDateLiteral.Precision.MINUTE)
                ? d.toEngineString() + "+0000" : d.toEngineString();
    }
}
