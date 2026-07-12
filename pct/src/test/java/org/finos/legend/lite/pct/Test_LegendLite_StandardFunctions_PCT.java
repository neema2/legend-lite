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
 * PCT tests for Standard Functions (extended standard library).
 */
public class Test_LegendLite_StandardFunctions_PCT extends PCTReportConfiguration {

    private static final ReportScope reportScope = CoreStandardFunctionsCodeRepositoryProvider.standardFunctions;
    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    // OFFICIAL-PARITY exclusions: every test here is ALSO an expected
    // failure of the reference legend-engine DuckDB adapter (its
    // Test_Relational_DuckDB_*_PCT exclusion set) — mixed-Number /
    // mixed-Date element identity through SQL type promotion, Decimal
    // value surfaces, in() over non-primitives, and friends. The
    // reference LEDGERS these rather than building a carrier; matching
    // its set IS the parity target. Pins are OUR full actual messages
    // (contains-matched), so any regression that changes the failure
    // shape — or a fix that makes one pass — fails loudly.
    private static final MutableList<ExclusionSpecification> expectedFailures = Lists.mutable.with(
            one("meta::pure::functions::collection::tests::in::testInIsEmpty_Function_1__Boolean_1_", "\"in call to 'meta::pure::functions::collection::in', argument 1: expected at least one value, got none ([0])\""),
            one("meta::pure::functions::collection::tests::in::testInNonPrimitive_Function_1__Boolean_1_", "\"Unimplemented type for cast (INTEGER -> STRUCT(legalName VARCHAR))\n\nLINE 1: SELECT 3 IN ({'legalName': 'f1'}, {'legalName': 'f2'}) AS value\n               ^\""),
            one("meta::pure::functions::math::hashCode::tests::testHashCodeAggregate_Function_1__Boolean_1_", "\"Not supported data type :'DOUBLE' for Pure type: 'Integer'\""),
            one("meta::pure::functions::math::tests::covarSample::testCovarSample_Function_1__Boolean_1_", "\"Attempting to execute an unsuccessful or closed pending query result\nError: Binder Error: No function matches the given name and argument types 'len(INTEGER_LITERAL)'. You might need to add explicit type casts.\n	Candidate functions:\n	len(VARCHAR) -> BIGINT\n	len(BIT) -> BIGINT\n	len(ANY[]) -> BIGINT\n\n\nLINE 1: SELECT CASE WHEN len(1) <> len(10) THEN error('covarSample: the two value...\n                         ^\""),
            one("meta::pure::functions::math::tests::percentile::testPercentile_Function_1__Boolean_1_", "\"\nexpected: [10]\nactual:   []\""));

    public static Test suite() {
        return wrapSuite(
                () -> true,
                () -> PureTestBuilderInterpreted.buildPCTTestSuite(reportScope, expectedFailures, adapter),
                () -> false,
                Lists.mutable.empty());
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
