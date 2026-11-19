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
            // legend-pure's error words: the test asserts upstream's sentence and lite raises its own (lite's rules word a raise; the edge words upstream's) (2026-11-19)
            one("meta::pure::functions::math::tests::rem::testRemError_Function_1__Boolean_1_", "\"Execution error message mismatch.\nThe actual message was \"division by zero\"\nwhere the expected message was:\"Cannot divide 5 by zero\"\""),
            one("meta::pure::functions::date::tests::testHourError_Function_1__Boolean_1_", "\"Execution error message mismatch.\nThe actual message was \"the date has no such part: it is coarser\"\nwhere the expected message was:\"Cannot get hour for 2017\"\""),   // a COMPAT row, failing with lite's text
            one("meta::pure::functions::date::tests::testDayOfMonthError_Function_1__Boolean_1_", "\"Execution error message mismatch.\nThe actual message was \"the date has no such part: it is coarser\"\nwhere the expected message was:\"Cannot get day of month for 2017\"\""),   // a COMPAT row, failing with lite's text
            one("meta::pure::functions::date::tests::testMinuteError_Function_1__Boolean_1_", "\"Execution error message mismatch.\nThe actual message was \"the date has no such part: it is coarser\"\nwhere the expected message was:\"Cannot get minute for 2017\"\""),   // a COMPAT row, failing with lite's text
            one("meta::pure::functions::collection::tests::at::testAtError_Function_1__Boolean_1_", "\"Execution error message mismatch.\nThe actual message was \"index out of bounds\"\nwhere the expected message was:\"The system is trying to get an element at offset 2 where the collection is of size 2\"\""),   // a COMPAT row, failing with lite's text
            one("meta::pure::functions::date::tests::testSecondError_Function_1__Boolean_1_", "\"Execution error message mismatch.\nThe actual message was \"the date has no such part: it is coarser\"\nwhere the expected message was:\"Cannot get second for 2017\"\""),   // a COMPAT row, failing with lite's text
            one("meta::pure::functions::collection::tests::slice::testSliceError_Function_1__Boolean_1_", "\"Execution error message mismatch.\nThe actual message was \"slice start is after its end\"\nwhere the expected message was:\"The low bound (3) can't be higher than the high bound (2) in a slice operation\"\""),
            one("meta::pure::functions::math::tests::testSquareRootError_Function_1__Boolean_1_", "\"Execution error message mismatch.\nThe actual message was \"square root of a negative number\"\nwhere the expected message was:\"Unable to compute sqrt of -1.0\"\""),
            one("meta::pure::functions::date::tests::testNewDateError_Function_1__Boolean_1_", "\"Execution error message mismatch.\nThe actual message was \"invalid date component\"\nwhere the expected message was:\"Invalid month: 13\"\""),   // a COMPAT row, failing with lite's text
            // legend-pure's print of a value (a Float's toString, a DateTime's toString and format): lite's own text, the product's (S118; the user's ruling of 2026-11-10 on a Float's text) (2026-11-19)
            one("meta::pure::functions::string::tests::toString::testDateTimeToString_Function_1__Boolean_1_", "\"\nexpected: '2014-01-01T00:00:00.000+0000'\nactual:   '2014-01-01T00:00:00.000000Z'\""),
            one("meta::pure::functions::string::tests::toString::testFloatToStringWithExcessTrailingZeros_Function_1__Boolean_1_", "\"\nexpected: '17.0'\nactual:   '17'\""),
            one("meta::pure::functions::string::tests::toString::testFloatToStringWithPositiveExponent_Function_1__Boolean_1_", "\"\nexpected: '134210000.0'\nactual:   '134210000'\""),
            one("meta::pure::functions::string::tests::format::testSimpleFormatDate_Function_1__Boolean_1_", "\"\nexpected: 'the quick brown fox jumps over the lazy 2014-01-01T00:00:00.000+0000'\nactual:   'the quick brown fox jumps over the lazy 2014-01-01T00:00:00.000000Z'\""),
            one("meta::pure::functions::string::tests::toString::testDateTimeWithTimezoneToString_Function_1__Boolean_1_", "\"\nexpected: '2014-01-01T00:00:00.0000+0000'\nactual:   '2014-01-01T00:00:00.000000Z'\""),
            one("meta::pure::functions::string::tests::toString::testSimpleDateToString_Function_1__Boolean_1_", "\"\nexpected: '2014-01-02T01:54:27.352+0000'\nactual:   '2014-01-02T01:54:27.352000Z'\""),
            one("meta::pure::functions::string::tests::toString::testFloatToStringWithNegativeExponent_Function_1__Boolean_1_", "\"\nexpected: '0.000000013421'\nactual:   '0'\""),
            // lite's substring indexing (the compatibility program's R4, not built: the PCT lanes' COMPAT rules plan CORRECT_SQL_SUBSTRING_INDEXING; LITE runs the product's default features) (2026-11-19)
            one("meta::pure::functions::string::tests::substring::testStart_Function_1__Boolean_1_", "\"\nexpected: 'he quick brown fox jumps over the lazy dog'\nactual:   'the quick brown fox jumps over the lazy dog'\""),
            one("meta::pure::functions::string::tests::substring::testStartEnd_Function_1__Boolean_1_", "\"\nexpected: 'he quick brown fox jumps over the lazy do'\nactual:   'the quick brown fox jumps over the lazy do'\""),
            one("meta::pure::functions::collection::tests::sort::testSimpleSortWithKey_Function_1__Boolean_1_", "\"\nexpected: ['Smith', 'Doe', 'Branche']\nactual:   ['Branche', 'Doe', 'Smith']\""),
            one("meta::pure::functions::collection::tests::sort::testSimpleSortWithFunctionVariables_Function_1__Boolean_1_", "\"\nexpected: ['Smith', 'Doe', 'Branche']\nactual:   ['Branche', 'Doe', 'Smith']\""),
            // legend-pure's text refused by name: an instance's toString, format's %r and %05d are the compatibility edge's (2026-11-19)
            one("meta::pure::functions::string::tests::toString::testComplexClassToString_Function_1__Boolean_1_", "\"toString of an instance is legend-pure's text, the compatibility edge's: the product writes an object by graph fetch\""),   // a COMPAT row, failing with lite's text
            one("meta::pure::functions::string::tests::format::testFormatRepr_Function_1__Boolean_1_", "format's %r is legend-pure's text, the compatibility edge's: the product's format writes %s, %d, %f and %t{}"),
            one("meta::pure::functions::string::tests::format::testFormatIntegerWithZeroPadding_Function_1__Boolean_1_", "format's %05d is legend-pure's text, the compatibility edge's: the product's format writes %s, %d, %f and %t{}"));

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
