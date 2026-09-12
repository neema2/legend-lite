// Copyright 2026 Legend Lite Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct;

import junit.framework.Test;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.finos.legend.pure.code.core.RelationCodeRepositoryProvider;
import org.finos.legend.pure.m3.pct.reports.config.PCTReportConfiguration;
import org.finos.legend.pure.m3.pct.reports.config.exclusion.ExclusionSpecification;
import org.finos.legend.pure.m3.pct.reports.model.Adapter;
import org.finos.legend.pure.m3.pct.shared.model.ReportScope;
import org.finos.legend.pure.runtime.java.interpreted.testHelper.PureTestBuilderInterpreted;

import static org.finos.legend.engine.test.shared.framework.PureTestHelperFramework.wrapSuite;

/**
 * PCT tests for Legend-Lite's relation function support.
 * 
 * This test class integrates with legend-engine's PCT framework to run
 * actual PCT test functions. The tests are dispatched through our adapter
 * function which calls legend-lite's QueryService via native function bridge.
 * 
 * Pattern following: legend-engine-xt-python-reversePCT-legendQL
 */
public class Test_LegendLite_RelationFunctions_PCT extends PCTReportConfiguration {

    private static final ReportScope reportScope = RelationCodeRepositoryProvider.relationFunctions;
    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    // Expected failures — HARNESS representation limits, not semantic gaps.
    private static final MutableList<ExclusionSpecification> expectedFailures = Lists.mutable.with(
            // The result wire (TDS parser, null literals ["", "null"]) cannot
            // represent an EMPTY STRING cell; joinStrings over the empty
            // collection is '' — right in SQL, unrepresentable on the wire.
            // Pin is the FULL expected+actual text, verbatim the official
            // legend-engine DuckDB PCT's pin for the SAME failure — a loose
            // fragment would keep matching if the test regressed elsewhere.
            // 2026-09-10 (upstream boundary batch 1): variant cells spell
            // \'…\' since the 5.92.0 TDS reader quotes with the Pure string
            // form (Render.pctCell); the failure is the SAME (the empty-
            // string cell), only its spelling moved.
            one("meta::pure::functions::relation::tests::composition::testVariantArrayColumn_joinStrings_Function_1__Boolean_1_", "\"\nexpected: '#TDS\n   id,payload,joined\n   1,\\'[1,2,3]\\',1,2,3\n   2,\\'[4,5,6]\\',4,5,6\n   3,\\'[7,8,9]\\',7,8,9\n   4,\\'null\\',\n#'\nactual:   '#TDS\n   id,payload,joined\n   1,\\'[1,2,3]\\',1,2,3\n   2,\\'[4,5,6]\\',4,5,6\n   3,\\'[7,8,9]\\',7,8,9\n   4,\\'null\\',null\n#'\"")
            // 4.145.0 (batch 8): the relation universe grew 350 -> 469; every
            // new-function row it opened LANDED 2026-09-12 (the quantification
            // family, the sort null forms, the group-lambda aggregates, the
            // variant emptiness conversions). The ONE row above is the
            // reference adapters' own verdict at 4.145.0 (their manifests
            // pin the same expected/actual text).
    );

    /**
     * JUnit 3 test suite entry point.
     * Uses PureTestBuilderInterpreted to build the test suite from PCT tests.
     */
    public static Test suite() {
        // M4 §3.4: the census gate pins this JVM's SqlTypeCensus
        // invariants at suite teardown (PctCensusGate)
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
