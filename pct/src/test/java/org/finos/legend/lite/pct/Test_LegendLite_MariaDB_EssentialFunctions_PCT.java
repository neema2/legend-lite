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
 * The essential PCT suite on MariaDB (docs/CLOUD_BACKENDS_PCT_2026_10_11.md §13): the SAME tests as
 * {@link Test_LegendLite_EssentialFunctions_PCT}, through the same adapter, on the embedded MariaDB 11.4
 * ({@code LEGENDLITE_PCT_BACKEND=mariadb}, pct/BUILD.bazel), with MariaDB's OWN expected failures -- legend-engine's
 * shape, as {@link Test_LegendLite_Postgres_EssentialFunctions_PCT}: every expected failure named and pinned by the text
 * it fails with, and a pinned test that starts passing fails the run, so a fix is recorded by deleting its row.
 */
public class Test_LegendLite_MariaDB_EssentialFunctions_PCT extends PCTReportConfiguration {

    private static final ReportScope reportScope = PlatformCodeRepositoryProvider.essentialFunctions;
    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    // Pinned by a STABLE part of the message (the runner matches by containment): no per-run id.
    // First measured 2026-10-11 (MariaDB 11.4.13, the pinned server).
    private static final MutableList<ExclusionSpecification> expectedFailures = Lists.mutable.with(
            // INSTANCE IDENTITY (as on DuckDB): the asserts want the interpreter's ORIGINAL instances back, or eq/==
            // by reference; the harness inlines captured instances BY VALUE, so identity is erased before the wire
            one("meta::pure::functions::collection::tests::find::testFindInstance_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::model::CO_Person"),
            one("meta::pure::functions::collection::tests::find::testFindUsingVarForFunction_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::model::CO_Person"),
            one("meta::pure::functions::collection::tests::head::testHeadComplex_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::model::CO_Person"),
            // THE PLATFORM'S OWN (each pinned on the DuckDB lane too, by the same cause): name resolution, lowering
            // and harness limits, the 0-based indexOf, error positions and messages
            one("meta::pure::functions::collection::tests::at::testAtError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 37\""),
            // NOTE 2026-10-20: testFoldFiltering and testFoldToMany unpinned -- a lambda parameter's list field is read
            // as the list it is (MariaDb.structFields types the read; JSON_VALUE read it as a scalar, NULL, and the
            // accumulator kept its last name alone)
            // NOTE 2026-10-20, plan-route parity (b0d5e435, measured by the step-9 lane session): testFold unpinned -- the
            // adapter prints a copy with its updated values (CopyWithUpdate)
            one("meta::pure::functions::collection::tests::indexof::testIndexOfOneElement_Function_1__Boolean_1_", "\"\nexpected: 0\nactual:   1\""),
            one("meta::pure::functions::date::tests::testAdjustByDaysBigNumber_Function_1__Boolean_1_", "\"a date past the year 9999 has no MariaDB value\""),
            one("meta::pure::functions::date::tests::testAdjustByHoursBigNumber_Function_1__Boolean_1_", "\"a date past the year 9999 has no MariaDB value\""),
            one("meta::pure::functions::date::tests::testAdjustByMonthsBigNumber_Function_1__Boolean_1_", "\"a date past the year 9999 has no MariaDB value\""),
            one("meta::pure::functions::date::tests::testAdjustByWeeksBigNumber_Function_1__Boolean_1_", "\"a date past the year 9999 has no MariaDB value\""),
            one("meta::pure::functions::date::tests::testDayOfMonthError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 36\""),
            one("meta::pure::functions::date::tests::testHourError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 36\""),
            one("meta::pure::functions::date::tests::testMinuteError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 36\""),
            one("meta::pure::functions::date::tests::testNewDateError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 29\""),
            one("meta::pure::functions::date::tests::testSecondError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 36\""),
            one("meta::pure::functions::lang::tests::match::testMatchWithMixedReturnType_Function_1__Boolean_1_", "\"scalar lowering not yet implemented for TypedDeactivate\""),
            // 2026-10-20, pinned again (the user's rule: as many tests as possible without hacks): a Float is a double
            // on the database, which holds no 20 digits; the batch-1 rule that kept a Float computed from literals
            // alone as a DECIMAL (LiteralSpelling.exactFloatConstant) was a special case -- the same expression over a
            // column was a double -- and is removed
            one("meta::pure::functions::math::tests::abs::testBigFloatAbs_Function_1__Boolean_1_", "\"\nexpected: 123456789123456789.99\nactual:   123456789123456780.0\""),
            one("meta::pure::functions::math::tests::trigonometry::testArcCosineError_Function_1__Boolean_1_", "\"Execution error message mismatch.\nThe actual message was \"ACOS is undefined outside [-1,1]\"\nwhere the expected message was:\"Unable to compute acos of 2.0\"\""),
            one("meta::pure::functions::math::tests::trigonometry::testArcSineError_Function_1__Boolean_1_", "\"Execution error message mismatch.\nThe actual message was \"ASIN is undefined outside [-1,1]\"\nwhere the expected message was:\"Unable to compute asin of 2.0\"\""),
            one("meta::pure::functions::string::tests::indexOf::testFromIndex_Function_1__Boolean_1_", "\"\nexpected: 1\nactual:   2\""),
            one("meta::pure::functions::string::tests::indexOf::testSimple_Function_1__Boolean_1_", "\"\nexpected: 4\nactual:   5\""),
            // 2026-10-20 (object identity): every object carries its __id now, so the default arm prints it (@_o1); the
            // class's own toString() qualifier is still not in the model the adapter hands over, as on the DuckDB lane
            one("meta::pure::functions::string::tests::toString::testComplexClassToString_Function_1__Boolean_1_", "actual:   '@_o1'"),
            // MARIADB'S OWN LIMIT: a DATE or DATETIME is of the years 1000-9999 (a five-digit year has no value;
            // raised where an interval is added, a wrong comparison where a literal is one)
            one("meta::pure::functions::date::tests::testAdjustByMinutesBigNumber_Function_1__Boolean_1_", "\"a date past the year 9999 has no MariaDB value\""),
            // MARIADB'S OWN LIMIT: no named time zone without the server's zone tables (loaded from the host's
            // zoneinfo, which a hermetic server does not read)
            one("meta::pure::functions::string::tests::format::testFormatDate_Function_1__Boolean_1_", "\"TIMEZONE reached MariaDB: MariaDB knows no named time zone but UTC without its zone tables\"")
            // NOTE 2026-10-20 (the parity phase): testRemoveDuplicatesPrimitiveStandardFunctionExplicit unpinned -- an
            // ordered derived table takes a LIMIT here, so the server keeps its ORDER BY (MariaDb.subselectSource)
            // step 9, 2026-10-19, the plan route: a heterogeneous class collection meeting at an abstract ancestor crosses
            // as JSON without its class, which the text carrier has no form for; the instance carrier (as on DuckDB)
            // NOTE 2026-10-20 (plan-route parity, the instance carrier): testConcatenateTypeInference passes again: an instance
            // meeting its siblings at an ancestor with no layout carries its own class in its JSON (__type,
            // MixedEncoding.withClass), and the text carrier spells the subclass it names
            // (LiteralSpelling.jsonInstanceText).
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
