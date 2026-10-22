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
 * The unclassified PCT suite on MySQL (docs/CLOUD_BACKENDS_PCT_2026_10_11.md §14): the SAME tests as
 * {@link Test_LegendLite_UnclassifiedFunctions_PCT}, through the same adapter, on the embedded MySQL 8.4
 * ({@code LEGENDLITE_PCT_BACKEND=mysql}, pct/BUILD.bazel), with MySQL's OWN expected failures -- legend-engine's
 * shape, as {@link Test_LegendLite_Postgres_UnclassifiedFunctions_PCT}: every expected failure named and pinned by the text
 * it fails with, and a pinned test that starts passing fails the run, so a fix is recorded by deleting its row.
 */
public class Test_LegendLite_MySQL_UnclassifiedFunctions_PCT extends PCTReportConfiguration {

    private static final ReportScope reportScope = CoreUnclassifiedFunctionsCodeRepositoryProvider.unclassifiedFunctions;
    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    // Pinned by a STABLE part of the message (the runner matches by containment): no per-run id.
    // First measured 2026-10-11 (MySQL 8.4.11, the pinned server).
    // NOTE 2026-10-20 (the parity phase, second batch): testJaroWinklerSimilarityEqual and
    // testJaroWinklerSimilarityNotEqual unpinned -- the similarity is a recursive walk over the texts' characters,
    // measured bit-equal to DuckDB's jaro_winkler_similarity (MariaDb.jaroWinkler). The ledger is empty: MySQL
    // answers every Unclassified test.
    private static final MutableList<ExclusionSpecification> expectedFailures = Lists.mutable.with();

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
