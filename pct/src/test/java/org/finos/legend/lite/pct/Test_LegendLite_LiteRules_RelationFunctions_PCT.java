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
 * Relation Functions under LITE's own rules, channel A (the LITE-PCT lane, 2026-11-19): {@link LiteRulesLedger} --
 * this suite's COMPAT expected failures with lite's own rows in their place.
 */
public class Test_LegendLite_LiteRules_RelationFunctions_PCT extends PCTReportConfiguration {

    /** This suite's COMPAT ledger: its scope and its rows, lite's own in their place. */
    private static final Test_LegendLite_RelationFunctions_PCT COMPAT = new Test_LegendLite_RelationFunctions_PCT();
    private static final ReportScope reportScope = COMPAT.getReportScope();
    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    // lite's own words for a compile error under lite's own rules (stage B's B1, 2026-11-21, PR #130, approved by the
    // user: "land B1 with the 4 rows"; the lane chooses its words once, PctPlanRoute.Audience -- until then a compile
    // error crossed in legend-pure's words on this lane too): the inverted window frame, M4's kind, in lite's sentence.
    // TEMPORARY, UNTIL BE: lite's message becomes legend-pure's sentence wherever legend-pure has one (the user's
    // ruling of 2026-11-21), and these four rows go with it
    private static final MutableList<ExclusionSpecification> expectedFailures = LiteRulesLedger.under(COMPAT.expectedFailures(),
            one("meta::pure::functions::relation::tests::over::testRows_InvalidWindowFrameBoundary_Function_1__Boolean_1_", "\"Execution error message mismatch.\nThe actual message was \"window frame starts after it ends\"\nwhere the expected message was:\"Invalid window frame boundary - lower bound of window frame cannot be greater than the upper bound!\"\""),
            one("meta::pure::functions::relation::tests::over::testRange_InvalidWindowFrameBoundary_Function_1__Boolean_1_", "\"Execution error message mismatch.\nThe actual message was \"window frame starts after it ends\"\nwhere the expected message was:\"Invalid window frame boundary - lower bound of window frame cannot be greater than the upper bound!\"\""),
            one("meta::pure::functions::relation::tests::reduce::testRows_InvalidWindowFrameBoundary_Function_1__Boolean_1_", "\"Execution error message mismatch.\nThe actual message was \"window frame starts after it ends\"\nwhere the expected message was:\"Invalid window frame boundary - lower bound of window frame cannot be greater than the upper bound!\"\""),
            one("meta::pure::functions::relation::tests::reduce::testRange_InvalidWindowFrameBoundary_Function_1__Boolean_1_", "\"Execution error message mismatch.\nThe actual message was \"window frame starts after it ends\"\nwhere the expected message was:\"Invalid window frame boundary - lower bound of window frame cannot be greater than the upper bound!\"\""));

    public static Test suite() {
        LiteRulesLedger.requireLite("Relation");
        return PctCensusGate.wrap("Relation", wrapSuite(
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
