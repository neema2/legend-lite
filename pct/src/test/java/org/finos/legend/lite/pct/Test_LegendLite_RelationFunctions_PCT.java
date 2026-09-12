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
            one("meta::pure::functions::relation::tests::composition::testVariantArrayColumn_joinStrings_Function_1__Boolean_1_", "\"\nexpected: '#TDS\n   id,payload,joined\n   1,\\'[1,2,3]\\',1,2,3\n   2,\\'[4,5,6]\\',4,5,6\n   3,\\'[7,8,9]\\',7,8,9\n   4,\\'null\\',\n#'\nactual:   '#TDS\n   id,payload,joined\n   1,\\'[1,2,3]\\',1,2,3\n   2,\\'[4,5,6]\\',4,5,6\n   3,\\'[7,8,9]\\',7,8,9\n   4,\\'null\\',null\n#'\""),
            // 4.145.0 (batch 8): the relation universe grew 350 -> 469. The
            // joinStrings / sort / extend / size additions are NEW upstream
            // functions the platform does not implement yet. Each row is a
            // leg; the fragment is the compiler's refusal. (The quantification
            // family — the ten quantified comparisons, relation `in`, the
            // two-argument `exists`, 90 rows — LANDED 2026-09-12:
            // NativeFn.RelationQuantifier + RelationPredicates.)
            one("meta::pure::functions::relation::tests::composition::testVariantColumn_filterOnIsEmptyOfModelConversion_Function_1__Boolean_1_", "not supported yet"),
            one("meta::pure::functions::relation::tests::composition::testVariantColumn_filterOnIsNotEmptyOfModelConversion_Function_1__Boolean_1_", "not supported yet"),
            one("meta::pure::functions::relation::tests::joinStrings::testJoinStrings_Aggregate_Ascending_Function_1__Boolean_1_", "no overload of 'meta::pure::functions::relation::aggregate' matches 2 argument(s)"),
            one("meta::pure::functions::relation::tests::joinStrings::testJoinStrings_Aggregate_EmptyLast_Function_1__Boolean_1_", "no overload of 'meta::pure::functions::relation::aggregate' matches 2 argument(s)"),
            one("meta::pure::functions::relation::tests::joinStrings::testJoinStrings_GroupBy_Ascending_Function_1__Boolean_1_", "no overload of 'meta::pure::functions::relation::groupBy' matches 3 argument(s)"),
            one("meta::pure::functions::relation::tests::joinStrings::testJoinStrings_GroupBy_Descending_Function_1__Boolean_1_", "no overload of 'meta::pure::functions::relation::groupBy' matches 3 argument(s)"),
            one("meta::pure::functions::relation::tests::joinStrings::testJoinStrings_GroupBy_EmptyFirst_Function_1__Boolean_1_", "no overload of 'meta::pure::functions::relation::groupBy' matches 3 argument(s)"),
            one("meta::pure::functions::relation::tests::joinStrings::testJoinStrings_GroupBy_ForcingSubSelect_Function_1__Boolean_1_", "no overload of 'meta::pure::functions::relation::groupBy' matches 3 argument(s)"),
            one("meta::pure::functions::relation::tests::joinStrings::testJoinStrings_GroupBy_MultipleSortColumns_Function_1__Boolean_1_", "no overload of 'meta::pure::functions::relation::groupBy' matches 3 argument(s)"),
            one("meta::pure::functions::relation::tests::joinStrings::testJoinStrings_GroupBy_RowFunction_Function_1__Boolean_1_", "no overload of 'meta::pure::functions::relation::groupBy' matches 3 argument(s)"),
            one("meta::pure::functions::relation::tests::joinStrings::testJoinStrings_GroupBy_SortColumnNotAggregated_Function_1__Boolean_1_", "no overload of 'meta::pure::functions::relation::groupBy' matches 3 argument(s)"),
            one("meta::pure::functions::relation::tests::joinStrings::testJoinStrings_GroupBy_Unordered_Function_1__Boolean_1_", "no overload of 'meta::pure::functions::relation::groupBy' matches 3 argument(s)"),
            one("meta::pure::functions::relation::tests::size::testSize_GroupBy_Function_1__Boolean_1_", "no overload of 'meta::pure::functions::relation::groupBy' matches 3 argument(s)")
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
