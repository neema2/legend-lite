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
 * Grammar Functions under LITE's own rules, channel A (the LITE-PCT lane, 2026-11-19): {@link LiteRulesLedger} --
 * this suite's COMPAT expected failures with lite's own rows in their place.
 */
public class Test_LegendLite_LiteRules_GrammarFunctions_PCT extends PCTReportConfiguration {

    /** This suite's COMPAT ledger: its scope and its rows, lite's own in their place. */
    private static final Test_LegendLite_GrammarFunctions_PCT COMPAT = new Test_LegendLite_GrammarFunctions_PCT();
    private static final ReportScope reportScope = COMPAT.getReportScope();
    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    private static final MutableList<ExclusionSpecification> expectedFailures = LiteRulesLedger.under(COMPAT.expectedFailures(),
            // legend-pure's error words: the test asserts upstream's sentence and lite raises its own (lite's rules word a raise; the edge words upstream's) (2026-11-19)
            one("meta::pure::functions::collection::tests::range::testRangeStepError_Function_1__Boolean_1_", "\"Execution error message mismatch.\nThe actual message was \"range step is zero\"\nwhere the expected message was:\"range step must not be 0\"\""));

    public static Test suite() {
        LiteRulesLedger.requireLite("Grammar");
        return PctCensusGate.wrap("Grammar", wrapSuite(
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
