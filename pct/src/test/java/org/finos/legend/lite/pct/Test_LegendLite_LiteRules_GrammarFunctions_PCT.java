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

    // (testRangeStepError gone 2026-11-21, stage B's BE, the one catalog: lite raises legend-pure's "range step must not
    // be 0"; a down move)
    private static final MutableList<ExclusionSpecification> expectedFailures = LiteRulesLedger.under(COMPAT.expectedFailures(),
            // B3 step 5 (the one equality; the user's ruling of 2026-11-24): a comparison of two values whose types are
            // known and differ is a compile error under lite's own rules -- legend-pure's PCT answers it at its door
            one("meta::pure::functions::boolean::tests::conjunctions::and::testShortCircuitSimple_Function_1__Boolean_1_", "\"> compares Float with Integer: two values are equal only when they have the same type and the same value, so this comparison is a mistake -- write both sides in one type (1.0 for 1, toFloat(), toString(), an enumeration's own value)\""),
            one("meta::pure::functions::boolean::tests::conjunctions::or::testShortCircuitSimple_Function_1__Boolean_1_", "\"> compares Float with Integer: two values are equal only when they have the same type and the same value, so this comparison is a mistake -- write both sides in one type (1.0 for 1, toFloat(), toString(), an enumeration's own value)\""),
            one("meta::pure::functions::boolean::tests::equality::eq::testEqEnum_Function_1__Boolean_1_", "\"eq compares meta::pure::functions::boolean::tests::equalitymodel::TestEnum1 with meta::pure::functions::boolean::tests::equalitymodel::TestEnum2: two values are equal only when they have the same type and the same value, so this comparison is a mistake -- write both sides in one type (1.0 for 1, toFloat(), toString(), an enumeration's own value)\""),
            // eq's and equal's NonPrimitive (step 5a's review M4, measured 2026-11-25): LeftClass and RightClass share a
            // subclass in legend-pure's model (BottomClass extends both), so one value can be of both and they are no
            // crossing -- channel B, which compiles the whole model, answers them; channel A's route hands lite the
            // classes the query names, without BottomClass, so lite refuses them by name there
            one("meta::pure::functions::boolean::tests::equality::eq::testEqNonPrimitive_Function_1__Boolean_1_", "\"eq compares meta::pure::functions::boolean::tests::equalitymodel::LeftClass with meta::pure::functions::boolean::tests::equalitymodel::RightClass: two values are equal only when they have the same type and the same value, so this comparison is a mistake -- write both sides in one type (1.0 for 1, toFloat(), toString(), an enumeration's own value)\""),
            one("meta::pure::functions::boolean::tests::equality::eq::testEqPrimitiveExtension_Function_1__Boolean_1_", "\"eq compares meta::pure::functions::boolean::tests::equalitymodel::SideClass with Integer: two values are equal only when they have the same type and the same value, so this comparison is a mistake -- write both sides in one type (1.0 for 1, toFloat(), toString(), an enumeration's own value)\""),
            one("meta::pure::functions::boolean::tests::equality::equal::testEqualEnum_Function_1__Boolean_1_", "\"== compares meta::pure::functions::boolean::tests::equalitymodel::TestEnum1 with meta::pure::functions::boolean::tests::equalitymodel::TestEnum2: two values are equal only when they have the same type and the same value, so this comparison is a mistake -- write both sides in one type (1.0 for 1, toFloat(), toString(), an enumeration's own value)\""),
            one("meta::pure::functions::boolean::tests::equality::equal::testEqualNonPrimitive_Function_1__Boolean_1_", "\"== compares meta::pure::functions::boolean::tests::equalitymodel::LeftClass with meta::pure::functions::boolean::tests::equalitymodel::RightClass: two values are equal only when they have the same type and the same value, so this comparison is a mistake -- write both sides in one type (1.0 for 1, toFloat(), toString(), an enumeration's own value)\""),
            one("meta::pure::functions::boolean::tests::equality::equal::testEqualPrimitiveExtension_Function_1__Boolean_1_", "\"== compares meta::pure::functions::boolean::tests::equalitymodel::SideClass with Integer: two values are equal only when they have the same type and the same value, so this comparison is a mistake -- write both sides in one type (1.0 for 1, toFloat(), toString(), an enumeration's own value)\""),
            one("meta::pure::functions::lang::tests::compare::testCompareDecimalAndLongTypes_Function_1__Boolean_1_", "\"compare compares Decimal with Integer: two values are equal only when they have the same type and the same value, so this comparison is a mistake -- write both sides in one type (1.0 for 1, toFloat(), toString(), an enumeration's own value)\""),
            one("meta::pure::functions::lang::tests::compare::testCompareMixedTypes_Function_1__Boolean_1_", "\"compare compares Integer with String: two values are equal only when they have the same type and the same value, so this comparison is a mistake -- write both sides in one type (1.0 for 1, toFloat(), toString(), an enumeration's own value)\""),
            one("meta::pure::functions::lang::tests::compare::testDateCompare_Function_1__Boolean_1_", "\"compare compares StrictDate with DateTime: two values are equal only when they have the same type and the same value, so this comparison is a mistake -- write both sides in one type (1.0 for 1, toFloat(), toString(), an enumeration's own value)\""),
            one("meta::pure::functions::lang::tests::compare::testNumberCompare_Function_1__Boolean_1_", "\"compare compares Integer with Float: two values are equal only when they have the same type and the same value, so this comparison is a mistake -- write both sides in one type (1.0 for 1, toFloat(), toString(), an enumeration's own value)\"")
            );

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
