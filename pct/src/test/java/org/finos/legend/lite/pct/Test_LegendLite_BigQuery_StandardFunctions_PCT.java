// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct;

import junit.framework.Test;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.finos.legend.pure.code.core.CoreStandardFunctionsCodeRepositoryProvider;
import org.finos.legend.pure.m3.pct.reports.config.PCTReportConfiguration;
import org.finos.legend.pure.m3.pct.reports.config.exclusion.ExclusionSpecification;
import org.finos.legend.pure.m3.pct.reports.model.Adapter;
import org.finos.legend.pure.m3.pct.shared.model.ReportScope;
import org.finos.legend.pure.runtime.java.interpreted.testHelper.PureTestBuilderInterpreted;
import static org.finos.legend.engine.test.shared.framework.PureTestHelperFramework.wrapSuite;

/**
 * The standard PCT suite on BigQuery (docs/CLOUD_BACKENDS_PCT_2026_10_11.md): the SAME tests as
 * {@link Test_LegendLite_StandardFunctions_PCT}, through the same adapter, on a real account
 * ({@code LEGENDLITE_PCT_BACKEND=bigquery}, pct/BUILD.bazel; its coordinates and credentials from
 * {@code LEGENDLITE_PCT_CLOUD_PROPERTIES}), with BigQuery's OWN expected failures -- legend-engine's shape, as
 * {@link Test_LegendLite_Postgres_StandardFunctions_PCT}: every expected failure named and pinned by the text it
 * fails with, and a pinned test that starts passing fails the run, so a fix is recorded by deleting its row.
 */
public class Test_LegendLite_BigQuery_StandardFunctions_PCT extends PCTReportConfiguration {

    private static final ReportScope reportScope = CoreStandardFunctionsCodeRepositoryProvider.standardFunctions;
    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    // Pinned by a STABLE part of the message (the runner matches by containment): no per-run id.
    // First measured 2026-10-11 on the account (BigQuery, 206 tests); burned down the same day: every
    // row is the platform's own (pinned on the DuckDB lane too) or a measured limit of BigQuery.
    private static final MutableList<ExclusionSpecification> expectedFailures = Lists.mutable.with(
            // 2026-11-11, the user's ruling on round 34 ("I guess refuse for now and put this on feature list for after
            // total separation complete"): refused, never cut (S181): BigQuery keeps 6; pending the nanosecond type (design
            // §9). The test's date-time literal has 7 to 9 digits of a second
            one("meta::pure::functions::date::tests::timeBucket::dateTime::testTimeBucketSeconds_Function_1__Boolean_1_", "fraction digits; BigQuery keeps 6: refused, never cut (S181)"),
            one("meta::pure::functions::date::tests::timeBucket::dateTime::testTimeBucketMinutes_Function_1__Boolean_1_", "fraction digits; BigQuery keeps 6: refused, never cut (S181)"),
            one("meta::pure::functions::date::tests::timeBucket::dateTime::testTimeBucketHours_Function_1__Boolean_1_", "fraction digits; BigQuery keeps 6: refused, never cut (S181)"),
            one("meta::pure::functions::date::tests::timeBucket::dateTime::testTimeBucketDays_Function_1__Boolean_1_", "fraction digits; BigQuery keeps 6: refused, never cut (S181)"),
            one("meta::pure::functions::date::tests::timeBucket::dateTime::testTimeBucketWeeks_Function_1__Boolean_1_", "fraction digits; BigQuery keeps 6: refused, never cut (S181)"),
            one("meta::pure::functions::date::tests::timeBucket::dateTime::testTimeBucketMonths_Function_1__Boolean_1_", "fraction digits; BigQuery keeps 6: refused, never cut (S181)"),
            one("meta::pure::functions::date::tests::timeBucket::dateTime::testTimeBucketYears_Function_1__Boolean_1_", "fraction digits; BigQuery keeps 6: refused, never cut (S181)")
    );

    public static Test suite() {
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
