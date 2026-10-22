// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct;

import junit.framework.Test;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.finos.legend.pure.m3.PlatformCodeRepositoryProvider;
import org.finos.legend.pure.m3.pct.reports.config.PCTReportConfiguration;
import org.finos.legend.pure.m3.pct.reports.config.exclusion.ExclusionSpecification;
import org.finos.legend.pure.m3.pct.reports.model.Adapter;
import org.finos.legend.pure.m3.pct.shared.model.ReportScope;
import org.finos.legend.pure.runtime.java.interpreted.testHelper.PureTestBuilderInterpreted;
import static org.finos.legend.engine.test.shared.framework.PureTestHelperFramework.wrapSuite;

/**
 * The essential PCT suite on MongoDB (docs/MONGODB_PLAN_2026_10_16.md §6): the SAME tests as the DuckDB lane's, through the
 * same adapter, on the embedded MongoDB 8.0 ({@code LEGENDLITE_PCT_BACKEND=mongodb}, pct/BUILD.bazel), channel A
 * through the plan path (PctPlanRoute: every result crosses as Pure's TDS text), with MongoDB's OWN expected
 * failures -- every one named and pinned by the text it fails with, and a pinned test that starts passing fails the
 * run, so a fix is recorded by deleting its row.
 */
public class Test_LegendLite_MongoDB_EssentialFunctions_PCT extends PCTReportConfiguration {

    private static final ReportScope reportScope = PlatformCodeRepositoryProvider.essentialFunctions;
    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    // Pinned by a STABLE part of the message (the runner matches by containment): no per-run id, no source line.
    // First measured 2026-10-17 (MongoDB 8.0.17, the pinned server; channel A through the plan path, PctPlanRoute).
    // Re-measured 2026-10-17 after the burn-down (docs/MONGODB_PLAN_2026_10_16.md §11.4: windows, quantified
    // comparisons, the Variant's JSON text, lateral and as-of joins, the statistics, the long tail; the adapter
    // reading the value relation's cells by the function's return type).
    // Re-measured 2026-10-17 after the cross-lane pass (§11.4: base64 and the string distances composed, a value
    // root's date precision, the day truncation typed DATE).
    // Re-measured 2026-10-18 after the text carrier (docs/PLAN_PATH_PROGRAM_2026_10_17.md §4, step 3): a class
    // instance, a List, a Pair, a Map, an Any value and a Type cross as Pure's own literal grammar; what stays
    // pinned is the identity tests (instances compared by reference, as on the DuckDB lane), a user element
    // referenced by its simple name, and an instance nested past the layout's depth.
    private static final MutableList<ExclusionSpecification> expectedFailures = Lists.mutable.with(
            // THE PLATFORM'S OWN (pinned on the DuckDB lane too, by the same cause)
            one("meta::pure::functions::collection::tests::at::testAtError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 37\""),
            // 2026-10-20, plan-route parity (b0d5e435, measured by the step-9 lane session): testFold, testFoldToMany
            // and testFoldFiltering pass -- the adapter prints a copy with its updated values (CopyWithUpdate)
            one("meta::pure::functions::date::tests::testDayOfMonthError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 36\""),
            one("meta::pure::functions::date::tests::testHourError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 36\""),
            one("meta::pure::functions::date::tests::testMinuteError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 36\""),
            one("meta::pure::functions::date::tests::testNewDateError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 29\""),
            one("meta::pure::functions::date::tests::testSecondError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 36\""),
            one("meta::pure::functions::lang::tests::match::testMatchWithMixedReturnType_Function_1__Boolean_1_", "\"scalar lowering not yet implemented for TypedDeactivate\""),
            // NOTE 2026-10-20 (the parity phase): testRemError and testSquareRootError unpinned -- a raising operator's
            // literal operand is read through the row, so the server's optimizer no longer raises before the guard
            // (MongoDb.unfolded)
            one("meta::pure::functions::math::tests::trigonometry::testArcCosineError_Function_1__Boolean_1_", "\"Execution error message mismatch.\nThe actual message was \"Character N is neither a decimal digit number, decimal point, nor \"e\" notation exponential mark.\"\nwhere the expected message was:\"Unable to compute acos of 2.0\"\""),
            one("meta::pure::functions::math::tests::trigonometry::testArcSineError_Function_1__Boolean_1_", "\"Execution error message mismatch.\nThe actual message was \"Character N is neither a decimal digit number, decimal point, nor \"e\" notation exponential mark.\"\nwhere the expected message was:\"Unable to compute asin of 2.0\"\""),
            // NOTE 2026-10-20 (plan-route parity, the instance carrier): testConcatenateTypeInference passes again: an instance
            // meeting its siblings at an ancestor with no layout carries its own class in its JSON (__type,
            // MixedEncoding.withClass), and the text carrier spells the subclass it names
            // (LiteralSpelling.jsonInstanceText).
            // VALUE DIFFERENCES, measured (step 9, 2026-10-19: testRemWithDecimals unpinned, its Decimal read by the
            // wire header's type since the DuckDB lane's landing)
            one("meta::pure::functions::collection::tests::indexof::testIndexOfOneElement_Function_1__Boolean_1_", "\"\nexpected: 0\nactual:   1\""),
            one("meta::pure::functions::date::tests::testDatePart_Function_1__Boolean_1_", "\"a date-time finer than a millisecond (2015-08-29T22:22:22.9914234) has no BSON date: refused, never cut\""),
            // 2026-10-20, pinned again (the user's rule: as many tests as possible without hacks): a Float is a double
            // on the database, which holds no 20 digits; the batch-1 rule that kept a Float computed from literals
            // alone as a DECIMAL (LiteralSpelling.exactFloatConstant) was a special case -- the same expression over a
            // column was a double -- and is removed
            one("meta::pure::functions::math::tests::abs::testBigFloatAbs_Function_1__Boolean_1_", "\"\nexpected: 123456789123456789.99\nactual:   123456789123456780.0\""),
            one("meta::pure::functions::string::tests::indexOf::testFromIndex_Function_1__Boolean_1_", "\"\nexpected: 1\nactual:   2\""),
            one("meta::pure::functions::string::tests::indexOf::testSimple_Function_1__Boolean_1_", "\"\nexpected: 4\nactual:   5\""),
            // 2026-10-20 (object identity): every object carries its __id now, so the default arm prints it (@_o1); the
            // class's own toString() qualifier is still not in the model the adapter hands over, as on the DuckDB lane
            one("meta::pure::functions::string::tests::toString::testComplexClassToString_Function_1__Boolean_1_", "actual:   '@_o1'"),
            // MONGODB'S OWN, the server's error
            one("meta::pure::functions::collection::tests::find::testFindInstance_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::model::CO_Person"),
            one("meta::pure::functions::collection::tests::find::testFindUsingVarForFunction_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::model::CO_Person"),
            one("meta::pure::functions::collection::tests::head::testHeadComplex_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::model::CO_Person"),
            one("meta::pure::functions::date::tests::testAdjustByDaysBigNumber_Function_1__Boolean_1_", "\"Command execution failed on MongoDB server with error 18537 (Location18537): 'PlanExecutor error during aggregation :: caused by :: Could not convert date to string: date component was outside the supported range of 0-9999: 33803336'"),
            one("meta::pure::functions::date::tests::testAdjustByHoursBigNumber_Function_1__Boolean_1_", "\"Command execution failed on MongoDB server with error 18537 (Location18537): 'PlanExecutor error during aggregation :: caused by :: Could not convert date to string: date component was outside the supported range of 0-9999: 1410404'"),
            one("meta::pure::functions::date::tests::testAdjustByMinutesBigNumber_Function_1__Boolean_1_", "\"Command execution failed on MongoDB server with error 18537 (Location18537): 'PlanExecutor error during aggregation :: caused by :: Could not convert date to string: date component was outside the supported range of 0-9999: 25489'"),
            one("meta::pure::functions::date::tests::testAdjustByMonthsBigNumber_Function_1__Boolean_1_", "\"Command execution failed on MongoDB server with error 5976500 (Location5976500): 'Failed to optimize pipeline :: caused by :: invalid dateAdd 'amount' parameter value: 9600000000 month'"),
            one("meta::pure::functions::date::tests::testAdjustByWeeksBigNumber_Function_1__Boolean_1_", "\"Command execution failed on MongoDB server with error 18537 (Location18537): 'PlanExecutor error during aggregation :: caused by :: Could not convert date to string: date component was outside the supported range of 0-9999: 236611261'"),
            // MONGODB'S OWN, refused by name
            one("meta::pure::functions::date::tests::testAdjustByMicrosecondsBigNumber_Function_1__Boolean_1_", "\"a MICROSECOND unit reached MongoDb, which has no such date unit\""),
            one("meta::pure::functions::date::tests::testAdjustByMicroseconds_Function_1__Boolean_1_", "\"a MICROSECOND unit reached MongoDb, which has no such date unit\"")
    );

    public static Test suite() {
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
