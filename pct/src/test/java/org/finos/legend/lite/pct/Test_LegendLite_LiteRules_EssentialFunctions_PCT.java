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
 * Essential Functions under LITE's own rules, channel A (the LITE-PCT lane, 2026-11-19): {@link LiteRulesLedger} --
 * this suite's COMPAT expected failures with lite's own rows in their place.
 */
public class Test_LegendLite_LiteRules_EssentialFunctions_PCT extends PCTReportConfiguration {

    /** This suite's COMPAT ledger: its scope and its rows, lite's own in their place. */
    private static final Test_LegendLite_EssentialFunctions_PCT COMPAT = new Test_LegendLite_EssentialFunctions_PCT();
    private static final ReportScope reportScope = COMPAT.getReportScope();
    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    private static final MutableList<ExclusionSpecification> expectedFailures = LiteRulesLedger.under(COMPAT.expectedFailures(),
            // (the nine "legend-pure's error words" rows -- rem, at, slice, squareRoot, the date parts' hour, minute,
            // second and dayOfMonth, and date()'s newDate -- went 2026-11-21 with stage B's BE, the one catalog: lite's
            // sentence is legend-pure's, so each test answers as its COMPAT row says)
            // legend-pure's print of a value (a Float's toString, a DateTime's toString and format): lite's own text, the product's (S118; the user's ruling of 2026-11-10 on a Float's text) (2026-11-19)
            one("meta::pure::functions::string::tests::toString::testDateTimeToString_Function_1__Boolean_1_", "\"\nexpected: '2014-01-01T00:00:00.000+0000'\nactual:   '2014-01-01T00:00:00.000000000Z'\""),
            one("meta::pure::functions::string::tests::format::testSimpleFormatDate_Function_1__Boolean_1_", "\"\nexpected: 'the quick brown fox jumps over the lazy 2014-01-01T00:00:00.000+0000'\nactual:   'the quick brown fox jumps over the lazy 2014-01-01T00:00:00.000000000Z'\""),
            one("meta::pure::functions::string::tests::toString::testDateTimeWithTimezoneToString_Function_1__Boolean_1_", "\"\nexpected: '2014-01-01T00:00:00.0000+0000'\nactual:   '2014-01-01T00:00:00.000000000Z'\""),
            one("meta::pure::functions::string::tests::toString::testSimpleDateToString_Function_1__Boolean_1_", "\"\nexpected: '2014-01-02T01:54:27.352+0000'\nactual:   '2014-01-02T01:54:27.352000000Z'\""),
            // (testFloatToStringWithExcessTrailingZeros and testFloatToStringWithPositiveExponent left 2026-11-24: a whole
            // Float keeps one fraction digit, S311 amended by the user's ruling of 2026-11-23, "I think we do it as 2.0"
            // (B2a); NegativeExponent's text 0 became 0.0)
            one("meta::pure::functions::string::tests::toString::testFloatToStringWithNegativeExponent_Function_1__Boolean_1_", "\"\nexpected: '0.000000013421'\nactual:   '0.0'\""),
            // (2026-11-20, the stage-A batch: A3 piece 2 gave substring Pure's 0-based meaning in the core, so under lite's
            // rules substring::testStart, testStartEnd, sort::testSimpleSortWithKey and testSimpleSortWithFunctionVariables
            // pass and their 4 rows left; a guard moving down only: approved by the user 2026-11-19 ~20:45 ("Approved to move
            // guards down"))
            // legend-pure's text refused by name: an instance's toString, format's %r and %05d are the compatibility edge's (2026-11-19)
            one("meta::pure::functions::string::tests::toString::testComplexClassToString_Function_1__Boolean_1_", "\"toString of an instance is legend-pure's text, the compatibility edge's: the product writes an object by graph fetch\""),   // a COMPAT row, failing with lite's text
            one("meta::pure::functions::string::tests::format::testFormatRepr_Function_1__Boolean_1_", "format's %r is legend-pure's text, the compatibility edge's: the product's format writes %s, %d, %f and %t{}"),
            one("meta::pure::functions::string::tests::format::testFormatIntegerWithZeroPadding_Function_1__Boolean_1_", "format's %05d is legend-pure's text, the compatibility edge's: the product's format writes %s, %d, %f and %t{}"),
            // B3 step 5 (the one equality; the user's ruling of 2026-11-24): a comparison of two values whose types are
            // known and differ is a compile error under lite's own rules -- legend-pure's PCT answers it at its door
            one("meta::pure::functions::collection::tests::contains::testContainsNonPrimitive_Function_1__Boolean_1_", "\"contains compares meta::pure::functions::collection::tests::model::CO_Firm with Integer: two values are equal only when they have the same type and the same value, so this comparison is a mistake -- write both sides in one type (1.0 for 1, toFloat(), toString(), an enumeration's own value)\""));

    public static Test suite() {
        LiteRulesLedger.requireLite("Essential");
        return PctCensusGate.wrap("Essential", wrapSuite(
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
