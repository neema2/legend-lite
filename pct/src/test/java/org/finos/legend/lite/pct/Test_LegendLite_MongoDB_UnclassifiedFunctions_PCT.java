// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct;

import junit.framework.Test;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.finos.legend.pure.code.core.CoreUnclassifiedFunctionsCodeRepositoryProvider;
import org.finos.legend.pure.m3.pct.reports.config.PCTReportConfiguration;
import org.finos.legend.pure.m3.pct.reports.config.exclusion.ExclusionSpecification;
import org.finos.legend.pure.m3.pct.reports.model.Adapter;
import org.finos.legend.pure.m3.pct.shared.model.ReportScope;
import org.finos.legend.pure.runtime.java.interpreted.testHelper.PureTestBuilderInterpreted;
import static org.finos.legend.engine.test.shared.framework.PureTestHelperFramework.wrapSuite;

/**
 * The unclassified PCT suite on MongoDB (docs/MONGODB_PLAN_2026_10_16.md §6): the SAME tests as the DuckDB lane's, through the
 * same adapter, on the embedded MongoDB 8.0 ({@code LEGENDLITE_PCT_BACKEND=mongodb}, pct/BUILD.bazel), channel A
 * through the plan path (PctPlanRoute: every result crosses as Pure's TDS text), with MongoDB's OWN expected
 * failures -- every one named and pinned by the text it fails with, and a pinned test that starts passing fails the
 * run, so a fix is recorded by deleting its row.
 */
public class Test_LegendLite_MongoDB_UnclassifiedFunctions_PCT extends PCTReportConfiguration {

    private static final ReportScope reportScope = CoreUnclassifiedFunctionsCodeRepositoryProvider.unclassifiedFunctions;
    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    // Pinned by a STABLE part of the message (the runner matches by containment): no per-run id, no source line.
    // First measured 2026-10-17 (MongoDB 8.0.17, the pinned server; channel A through the plan path, PctPlanRoute).
    // Re-measured 2026-10-17 after the burn-down (docs/MONGODB_PLAN_2026_10_16.md §11.4: windows, quantified
    // comparisons, the Variant's JSON text, lateral and as-of joins, the statistics, the long tail; the adapter
    // reading the value relation's cells by the function's return type).
    // Re-measured 2026-10-17 after the cross-lane pass (§11.4: base64 and the string distances composed, a value
    // root's date precision, the day truncation typed DATE).
    private static final MutableList<ExclusionSpecification> expectedFailures = Lists.mutable.with(
            // MONGODB'S OWN, refused by name
            one("meta::pure::functions::hash::tests::testMD5Hash_Function_1__Boolean_1_", "\"MD5 reached MongoDb, which has no digest operator\""),
            one("meta::pure::functions::hash::tests::testSHA1Hash_Function_1__Boolean_1_", "\"SHA1 reached MongoDb, which has no digest operator\""),
            one("meta::pure::functions::hash::tests::testSHA256Hash_Function_1__Boolean_1_", "\"SHA256 reached MongoDb, which has no digest operator\""),
            one("meta::pure::functions::runtime::currentUserId::testCurrentUserId_Function_1__Boolean_1_", "\"currentUserId reached MongoDb, whose pipeline names no user\"")
    );

    public static Test suite() {
        return PctCensusGate.wrap("Unclassified", wrapSuite(
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
