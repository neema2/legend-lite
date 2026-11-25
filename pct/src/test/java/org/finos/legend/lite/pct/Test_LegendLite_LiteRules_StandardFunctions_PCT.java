// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct;

import static org.finos.legend.engine.test.shared.framework.PureTestHelperFramework.wrapSuite;

import junit.framework.Test;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.finos.legend.pure.m3.pct.reports.config.PCTReportConfiguration;
import org.finos.legend.pure.m3.pct.reports.config.exclusion.ExclusionSpecification;
import org.finos.legend.pure.m3.pct.reports.model.Adapter;
import org.finos.legend.pure.m3.pct.shared.model.ReportScope;
import org.finos.legend.pure.runtime.java.interpreted.testHelper.PureTestBuilderInterpreted;

/**
 * Standard Functions under LITE's own rules, channel A (the LITE-PCT lane, 2026-11-19): {@link LiteRulesLedger} --
 * this suite's COMPAT expected failures with lite's own rows in their place.
 */
public class Test_LegendLite_LiteRules_StandardFunctions_PCT extends PCTReportConfiguration {

    /** This suite's COMPAT ledger: its scope and its rows, lite's own in their place. */
    private static final Test_LegendLite_StandardFunctions_PCT COMPAT = new Test_LegendLite_StandardFunctions_PCT();
    private static final ReportScope reportScope = COMPAT.getReportScope();
    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    private static final MutableList<ExclusionSpecification> expectedFailures = LiteRulesLedger.under(COMPAT.expectedFailures(),
            // (the two "legend-pure's error words" rows, bitShiftLeft and bitShiftRight's MoreThan62Bits, went 2026-11-21
            // with stage B's BE, the one catalog: lite's sentence is legend-pure's, so both pass under lite's rules)
            // B3 step 5 (the one equality; the user's ruling of 2026-11-24): a comparison of two values whose types are
            // known and differ is a compile error under lite's own rules -- legend-pure's PCT answers it at its door
            one("meta::pure::functions::collection::tests::in::testInNonPrimitive_Function_1__Boolean_1_", "\"in compares Integer with meta::pure::functions::collection::tests::in::Firm: two values are equal only when they have the same type and the same value, so this comparison is a mistake -- write both sides in one type (1.0 for 1, toFloat(), toString(), an enumeration's own value)\""),
            one("meta::pure::functions::math::tests::max::testMax_Numbers_Function_1__Boolean_1_", "\"max compares Float with Integer: two values are equal only when they have the same type and the same value, so this comparison is a mistake -- write both sides in one type (1.0 for 1, toFloat(), toString(), an enumeration's own value)\""),
            one("meta::pure::functions::math::tests::min::testMin_Numbers_Function_1__Boolean_1_", "\"min compares Float with Integer: two values are equal only when they have the same type and the same value, so this comparison is a mistake -- write both sides in one type (1.0 for 1, toFloat(), toString(), an enumeration's own value)\""),
            // B3 step 5a's review (M4, 2026-11-25): max and min over two Dates read by the same rule
            one("meta::pure::functions::date::tests::max::testMax_Date_Function_1__Boolean_1_", "\"max compares DateTime with StrictDate: two values are equal only when they have the same type and the same value, so this comparison is a mistake -- write both sides in one type (1.0 for 1, toFloat(), toString(), an enumeration's own value)\""),
            one("meta::pure::functions::date::tests::min::testMin_Date_Function_1__Boolean_1_", "\"min compares DateTime with StrictDate: two values are equal only when they have the same type and the same value, so this comparison is a mistake -- write both sides in one type (1.0 for 1, toFloat(), toString(), an enumeration's own value)\"")
            );

    public static Test suite() {
        LiteRulesLedger.requireLite("Standard");
        return PctCensusGate.wrap("Standard", wrapSuite(
                () -> true,
                () -> PureTestBuilderInterpreted.buildPCTTestSuite(reportScope, expectedFailures, adapter),
                () -> false,
                Lists.mutable.empty()));
    }

    @Override
    public MutableList<ExclusionSpecification> expectedFailures() {
        return expectedFailures;
    }

    @Override
    public ReportScope getReportScope() {
        return reportScope;
    }

    @Override
    public Adapter getAdapter() {
        return adapter;
    }

    @Override
    public String getPlatform() {
        return platform;
    }
}
