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
 * The relation PCT suite on H2 2.4.240 — gate 7. The SAME tests as
 * {@link Test_LegendLite_RelationFunctions_PCT}, through the same adapter; the
 * database is chosen by the target ({@code LEGENDLITE_PCT_BACKEND=h2},
 * pct/BUILD.bazel), and this class carries H2's OWN expected failures.
 *
 * <p>The shape is legend-engine's: one configuration per database per suite,
 * every expected failure named and pinned by the text it fails with, and a
 * pinned test that starts PASSING fails the run ("expected to fail ... but now
 * passes") — so a fix is recorded by deleting its row. It replaces a count
 * ratchet (at most 1 failure and 26 errors) that could not say which tests
 * failed, nor notice one failure swapping for another.
 *
 * <p>Measured 2026-09-23 against legend-engine's own H2 manifest at the pinned
 * release (pct-manifests/relational-h2/RelationFunctions_manifest.json): 21 of
 * these 27 fail there too — H2 has no list type, no UNNEST, no LATERAL — and 6
 * fail only here (marked). The engine fails 37 relation tests on H2 that lite
 * passes (pivots, as-of joins, flatten).
 */
public class Test_LegendLite_H2_RelationFunctions_PCT extends PCTReportConfiguration {

    private static final ReportScope reportScope = RelationCodeRepositoryProvider.relationFunctions;
    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    // Pinned by a STABLE part of the message: several carry a per-run
    // execution trace id, and the runner matches by containment.
    private static final MutableList<ExclusionSpecification> expectedFailures = Lists.mutable.with(
            // H2 has no list type: the list functions have no encoding on this dialect
            one("meta::pure::functions::relation::variant::tests::flatten::testFlatten_LateralJoin_Nested_Extend_Function_1__Boolean_1_", "LIST_FILTER reached a dialect without a list encoding"),
            one("meta::pure::functions::relation::tests::extend::testVariantColumn_filter_Function_1__Boolean_1_", "LIST_FILTER reached a dialect without a list encoding"),
            one("meta::pure::functions::relation::tests::filter::testVariantColumn_filterOutputFromLambda_Function_1__Boolean_1_", "LIST_FILTER reached a dialect without a list encoding"),
            one("meta::pure::functions::relation::tests::composition::testVariantColumn_functionComposition_Function_1__Boolean_1_", "LIST_FILTER reached a dialect without a list encoding"),
            one("meta::pure::functions::relation::tests::composition::testCoalesceInPreFilter_Function_1__Boolean_1_", "LIST_FILTER reached a dialect without a list encoding"),   // lite only: the engine passes it on H2
            one("meta::pure::functions::relation::tests::composition::testVariantColumn_distinct_removeDuplicates_Function_1__Boolean_1_", "LIST_SORT reached a dialect without a list encoding"),
            one("meta::pure::functions::relation::tests::composition::testVariantArrayColumn_sort_Function_1__Boolean_1_", "LIST_SORT reached a dialect without a list encoding"),
            one("meta::pure::functions::relation::tests::composition::testVariantColumn_slice_Function_1__Boolean_1_", "LIST_SLICE reached a dialect without a list encoding"),
            one("meta::pure::functions::relation::tests::extend::testVariantColumn_map_Function_1__Boolean_1_", "LIST_TRANSFORM reached a dialect without a list encoding"),
            one("meta::pure::functions::relation::tests::composition::testVariantArrayColumn_reverse_Function_1__Boolean_1_", "LIST_REVERSE reached a dialect without a list encoding"),
            one("meta::pure::functions::relation::tests::composition::testVariantColumn_indexOf_Function_1__Boolean_1_", "LIST_POSITION reached a dialect without a list encoding"),
            one("meta::pure::functions::relation::tests::composition::testVariantColumn_contains_Function_1__Boolean_1_", "collection membership reached a dialect without a list encoding"),
            one("meta::pure::functions::relation::tests::composition::testVariantArrayColumn_joinStrings_Function_1__Boolean_1_", "collection reduction 'STRING_AGG' reached a dialect without a list encoding"),
            one("meta::pure::functions::relation::tests::extend::testVariantColumn_fold_Function_1__Boolean_1_", "fold reached a dialect without a fold encoding"),
            // H2 has no UNNEST placement for a flatten
            one("meta::pure::functions::relation::variant::tests::flatten::testFlatten_LateralJoin_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::relation::variant::tests::flatten::testFlatten_LateralJoin_Nested_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::relation::tests::composition::testVariantMapColumn_keys_LateralFlatten_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::relation::tests::composition::testVariantMapColumn_values_LateralFlatten_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::relation::tests::project::testSimpleProjectWithEmpty_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),   // lite only: the engine passes it on H2
            // H2 has no LATERAL: the SQL lite renders is refused by the database
            one("meta::pure::functions::relation::tests::lateral::testLateralJoin_Function_1__Boolean_1_", "Function \"LATERAL\" not found"),
            one("meta::pure::functions::relation::tests::lateral::testLateralJoin_Chained_Function_1__Boolean_1_", "Function \"LATERAL\" not found"),
            one("meta::pure::functions::relation::tests::lateral::testLateralJoinAreInnerJoins_Function_1__Boolean_1_", "Function \"LATERAL\" not found"),
            one("meta::pure::functions::relation::tests::lateral::testLateralJoin_CorrelatedColumnNameAlsoOnInner_Function_1__Boolean_1_", "Function \"LATERAL\" not found"),
            one("meta::pure::functions::relation::tests::project::testSimpleProject_Function_1__Boolean_1_", "Function \"LATERAL\" not found"),   // lite only: the engine passes it on H2
            one("meta::pure::functions::relation::tests::project::testSimpleProjectList_Function_1__Boolean_1_", "Function \"LATERAL\" not found"),   // lite only: the engine passes it on H2
            one("meta::pure::functions::relation::tests::composition::testFilterPostProject_Function_1__Boolean_1_", "Function \"LATERAL\" not found"),   // lite only: the engine passes it on H2
            // a lite defect on H2: a self-referencing exists renders two columns named id
            one("meta::pure::functions::relation::tests::exists::testExistsSelfReferencing_Function_1__Boolean_1_", "Duplicate column name \"id\"")   // lite only: the engine passes it on H2
    );

    public static Test suite() {
        // the census gate keys its H2 pins on the backend (PctCensusGate)
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
