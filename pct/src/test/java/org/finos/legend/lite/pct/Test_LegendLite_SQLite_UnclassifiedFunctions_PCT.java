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
 * The unclassified PCT suite on SQLite (docs/CLOUD_BACKENDS_PCT_2026_10_11.md §12): the SAME tests as
 * {@link Test_LegendLite_UnclassifiedFunctions_PCT}, through the same adapter, on an in-memory SQLite
 * ({@code LEGENDLITE_PCT_BACKEND=sqlite}, pct/BUILD.bazel), with SQLite's OWN expected failures -- legend-engine's
 * shape, as {@link Test_LegendLite_Postgres_UnclassifiedFunctions_PCT}: every expected failure named and pinned by the text
 * it fails with, and a pinned test that starts passing fails the run, so a fix is recorded by deleting its row.
 */
public class Test_LegendLite_SQLite_UnclassifiedFunctions_PCT extends PCTReportConfiguration {

    private static final ReportScope reportScope = CoreUnclassifiedFunctionsCodeRepositoryProvider.unclassifiedFunctions;
    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    // Pinned by a STABLE part of the message (the runner matches by containment): no per-run id.
    // First measured 2026-10-11 (SQLite 3.47.1, the driver's own build; 93 tests); burned down the same day: every
    // row is the platform's own (pinned on the DuckDB lane too), or a measured limit of SQLite.
    private static final MutableList<ExclusionSpecification> expectedFailures = Lists.mutable.with(
            // SQLITE'S OWN LIMIT: no users (an embedded database has no session user)
            one("meta::pure::functions::runtime::currentUserId::testCurrentUserId_Function_1__Boolean_1_", "\"CURRENT_USER_FN reached SQLite: SQLite has no users\"")
            // NOTE 2026-10-20 (the parity phase, second batch): testMD5Hash, testSHA1Hash and testSHA256Hash unpinned
            // -- each digest is written in SQLite's own SQL, recursive walks over the text's UTF-8 bytes, measured
            // bit-equal to the JDK's (SqliteDigests)
            // NOTE 2026-10-20 (the parity phase, second batch): testJaroWinklerSimilarityEqual and
            // testJaroWinklerSimilarityNotEqual unpinned -- the similarity is a recursive walk over the texts'
            // characters, measured bit-equal to DuckDB's jaro_winkler_similarity (SQLite.jaroWinkler)
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
