// Copyright 2026 Legend Contributors
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
 * The relation PCT suite on MySQL (docs/CLOUD_BACKENDS_PCT_2026_10_11.md §14): the SAME tests as
 * {@link Test_LegendLite_RelationFunctions_PCT}, through the same adapter, on the embedded MySQL 8.4
 * ({@code LEGENDLITE_PCT_BACKEND=mysql}, pct/BUILD.bazel), with MySQL's OWN expected failures -- legend-engine's
 * shape, as {@link Test_LegendLite_Postgres_RelationFunctions_PCT}: every expected failure named and pinned by the text
 * it fails with, and a pinned test that starts passing fails the run, so a fix is recorded by deleting its row.
 */
public class Test_LegendLite_MySQL_RelationFunctions_PCT extends PCTReportConfiguration {

    private static final ReportScope reportScope = RelationCodeRepositoryProvider.relationFunctions;
    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    // Pinned by a STABLE part of the message (the runner matches by containment): no per-run id.
    // First measured 2026-10-11 (MySQL 8.4.11, the pinned server).
    private static final MutableList<ExclusionSpecification> expectedFailures = Lists.mutable.with(
            // THE PLATFORM'S OWN (each pinned on the DuckDB lane too, by the same cause): name resolution, lowering
            // and harness limits, the 0-based indexOf, error positions and messages
            one("meta::pure::functions::relation::tests::composition::testVariantArrayColumn_joinStrings_Function_1__Boolean_1_", "\"\nexpected: '#TDS\n   id,payload,joined\n   1,\\'[1,2,3]\\',1,2,3\n   2,\\'[4,5,6]\\',4,5,6\n   3,\\'[7,8,9]\\',7,8,9\n   4,\\'null\\',\n#'\nactual:   '#TDS\n   id,payload,joined\n   1,\\'[1,2,3]\\',1,2,3\n   2,\\'[4,5,6]\\',4,5,6\n   3,\\'[7,8,9]\\',7,8,9\n   4,\\'null\\',null\n#'\""),
            // NOTE 2026-10-20 (the parity phase): testExtendLeadAdjustDerivedOffset unpinned -- its dates are all 2026;
            // MySQL evaluated the interval guard's raise on every row of an expression over a window call, and the
            // dialect now computes the window below the expression (WindowsComputedBelow)
            // NOTE 2026-10-20 (the parity phase): the eleven windowed string aggregations unpinned -- the dialect
            // writes a window call of an aggregate it has no window form of as that aggregate over the frame's rows,
            // a correlated subquery over the FROM numbered (WindowAggregatesAsCorrelated)
            // MYSQL'S OWN LIMIT: a JSON value is stored parsed, an object's keys sorted (by length, then bytes): the
            // values are right, the printed key order is not the document's
            one("meta::pure::functions::relation::tests::composition::testVariantMapColumn_values_LateralFlatten_Function_1__Boolean_1_", "\"#TDS\n   id:Integer[0..1],payload:meta::pure::metamodel::variant::Variant[0..1],firstName:String[0..1]\n   1,'{\"a\":{\"firstName\":\"John\",\"lastName\":\"Smith\"},\"b\":{\"firstName\":\"Jane\",\"lastName\":\"Smith\"}}',Jane\n   1,'{\"a\":{\"firstName\":\"John\",\"lastName\":\"Smith\"},\"b\":{\"firstName\":\"Jane\",\"lastName\":\"Smith\"}}',John\n   2,'{\"c\":{\"firstName\":\"Juan\",\"lastName\":\"Del Pueblo\"}}',Juan\n#\n is not equivalent to:\n#TDS\n   id:Integer[0..1],payload:meta::pure::metamodel::variant::Variant[0..1],firstName:String[0..1]\n   1,'{\"a\":{\"lastName\":\"Smith\",\"firstName\":\"John\"},\"b\":{\"lastName\":\"Smith\",\"firstName\":\"Jane\"}}',Jane\n   1,'{\"a\":{\"lastName\":\"Smith\",\"firstName\":\"John\"},\"b\":{\"lastName\":\"Smith\",\"firstName\":\"Jane\"}}',John\n   2,'{\"c\":{\"lastName\":\"Del Pueblo\",\"firstName\":\"Juan\"}}',Juan\n#\""),
            one("meta::pure::functions::relation::tests::extend::testVariantColumn_keyExtraction_Function_1__Boolean_1_", "\"\nexpected: '#TDS\n   id,payload,booleanKey,integerKey,stringKey\n   1,\\'{\"boolean\":true,\"integer\":1,\"string\":\"hello\"}\\',\\'true\\',\\'1\\',\\'\"hello\"\\'\n   2,\\'{\"boolean\":false,\"integer\":2,\"string\":\"world\"}\\',\\'false\\',\\'2\\',\\'\"world\"\\'\n   3,\\'{\"boolean\":true,\"integer\":3,\"string\":\"world\"}\\',\\'true\\',\\'3\\',\\'\"world\"\\'\n#'\nactual:   '#TDS\n   id,payload,booleanKey,integerKey,stringKey\n   1,\\'{\"string\":\"hello\",\"boolean\":true,\"integer\":1}\\',\\'true\\',\\'1\\',\\'\"hello\"\\'\n   2,\\'{\"string\":\"world\",\"boolean\":false,\"integer\":2}\\',\\'false\\',\\'2\\',\\'\"world\"\\'\n   3,\\'{\"string\":\"world\",\"boolean\":true,\"integer\":3}\\',\\'true\\',\\'3\\',\\'\"world\"\\'\n#'\""),
            one("meta::pure::functions::relation::tests::filter::testVariantColumn_filterOnKeyExtractionValue_Function_1__Boolean_1_", "\"\nexpected: '#TDS\n   id,payload\n   1,\\'{\"boolean\":true,\"integer\":1,\"string\":\"hello\"}\\'\n#'\nactual:   '#TDS\n   id,payload\n   1,\\'{\"string\":\"hello\",\"boolean\":true,\"integer\":1}\\'\n#'\"")
            // the ten uncast dynamic pivots answer since the probe plan (2026-10-20, KeyedPivot: the keys read
            // first, the body written for them on every database); their pins of step 9 (2026-10-19) removed
            // with the run that passed them
    );

    public static Test suite() {
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
