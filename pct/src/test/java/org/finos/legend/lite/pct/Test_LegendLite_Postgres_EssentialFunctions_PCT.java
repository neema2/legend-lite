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
 * The essential PCT suite on Postgres 16 (leg P2, docs/POSTGRES_DIALECT_HOMEWORK_2026_10_01.md Q5): the
 * SAME tests as {@link Test_LegendLite_EssentialFunctions_PCT}, through the same adapter, on this JVM's
 * embedded Postgres ({@code LEGENDLITE_PCT_BACKEND=postgres}, pct/BUILD.bazel), with Postgres's OWN
 * expected failures -- legend-engine's shape, as {@link Test_LegendLite_H2_RelationFunctions_PCT}: every
 * expected failure named and pinned by the text it fails with, and a pinned test that starts passing fails
 * the run, so a fix is recorded by deleting its row.
 *
 * <p>First measured 2026-10-02 on Postgres 16.15 (103 rows): the rows are grouped by why -- wrong answers
 * (dialect defects, fixed first), Postgres's own errors, the collections and Variant of leg P4, and
 * constructs the dialect refuses by name.
 */
public class Test_LegendLite_Postgres_EssentialFunctions_PCT extends PCTReportConfiguration {

    private static final ReportScope reportScope = PlatformCodeRepositoryProvider.essentialFunctions;
    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    // Pinned by a STABLE part of the message (the runner matches by containment): no per-run id.
    private static final MutableList<ExclusionSpecification> expectedFailures = Lists.mutable.with(
            // INSTANCE IDENTITY (as on DuckDB): the asserts want the interpreter's ORIGINAL instances back; the
            // harness inlines captured instances BY VALUE, so identity is erased before the wire
            one("meta::pure::functions::collection::tests::find::testFindInstance_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::model::CO_Person"),
            one("meta::pure::functions::collection::tests::find::testFindUsingVarForFunction_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::model::CO_Person"),
            one("meta::pure::functions::collection::tests::head::testHeadComplex_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::model::CO_Person"),
            // Float is a double (numeric charter Rule 2) -- deliberate, on every dialect
            one("meta::pure::functions::collection::tests::indexof::testIndexOfOneElement_Function_1__Boolean_1_", "expected: 0\nactual:   1"),
            // 2026-10-20, pinned again (the user's rule: as many tests as possible without hacks): a Float is a double
            // on the database, which holds no 20 digits; the batch-1 rule that kept a Float computed from literals
            // alone as a DECIMAL (LiteralSpelling.exactFloatConstant) was a special case -- the same expression over a
            // column was a double -- and is removed
            // 2026-10-27, the result boundary program (typed rows): the double crosses as itself and Pure prints its shortest
            // digits, no longer Postgres's fifteen-digit text of it (measured on this lane by its own session)
            // a Float literal's twenty digits: the interpreter holds a BigDecimal, lite a double (register S120, outcome)
            one("meta::pure::functions::math::tests::abs::testBigFloatAbs_Function_1__Boolean_1_", "expected: 123456789123456789.99\nactual:   123456789123456780.0"),
            one("meta::pure::functions::string::tests::indexOf::testFromIndex_Function_1__Boolean_1_", "expected: 1\nactual:   2"),
            one("meta::pure::functions::string::tests::indexOf::testSimple_Function_1__Boolean_1_", "expected: 4\nactual:   5"),
            // Postgres raises differently (its own error, or a different position or wording)
            one("meta::pure::functions::collection::tests::at::testAtError_Function_1__Boolean_1_", "Execution error column mismatch. Actual: 23 where expected: 37"),
            one("meta::pure::functions::date::tests::testAdjustByDaysBigNumber_Function_1__Boolean_1_", "ERROR: interval out of range"),
            one("meta::pure::functions::date::tests::testAdjustByHoursBigNumber_Function_1__Boolean_1_", "ERROR: interval out of range"),
            one("meta::pure::functions::date::tests::testAdjustByMinutesBigNumber_Function_1__Boolean_1_", "ERROR: timestamp out of range"),
            one("meta::pure::functions::date::tests::testAdjustByMonthsBigNumber_Function_1__Boolean_1_", "ERROR: interval out of range"),
            one("meta::pure::functions::date::tests::testAdjustByWeeksBigNumber_Function_1__Boolean_1_", "ERROR: interval out of range"),
            one("meta::pure::functions::date::tests::testDayOfMonthError_Function_1__Boolean_1_", "Execution error column mismatch. Actual: 23 where expected: 36"),
            one("meta::pure::functions::date::tests::testHourError_Function_1__Boolean_1_", "Execution error column mismatch. Actual: 23 where expected: 36"),
            one("meta::pure::functions::date::tests::testMinuteError_Function_1__Boolean_1_", "Execution error column mismatch. Actual: 23 where expected: 36"),
            one("meta::pure::functions::date::tests::testNewDateError_Function_1__Boolean_1_", "Execution error column mismatch. Actual: 23 where expected: 29"),
            one("meta::pure::functions::date::tests::testSecondError_Function_1__Boolean_1_", "Execution error column mismatch. Actual: 23 where expected: 36"),
            // step 9, 2026-10-19, the plan route: the NaN cell spelled NaN by the text carrier, refused by Pure's
            // parseFloat (the legacy route's Java BigDecimal said "Infinite or NaN"; the DuckDB lane's pin, the same cause)
            // 2026-10-27, the result boundary program (the PCT reads typed rows, measured on this lane by its own
            // session): the database's NaN reaches the bridge as a double, refused by name (Pure's Float is a decimal),
            // no longer parsed from its text
            one("meta::pure::functions::math::tests::trigonometry::testArcCosineError_Function_1__Boolean_1_", "Execution error message mismatch.\nThe actual message was \"the database answered NaN for a Float, which has no NaN in Pure\"\nwhere the expected message was:\"Unable to compute acos of 2.0\""),
            one("meta::pure::functions::math::tests::trigonometry::testArcSineError_Function_1__Boolean_1_", "Execution error message mismatch.\nThe actual message was \"the database answered NaN for a Float, which has no NaN in Pure\"\nwhere the expected message was:\"Unable to compute asin of 2.0\""),
            // 2026-10-29 (track L, the result boundary program; PR #80's pct_postgres lanes on the L1 commit): the two
            // removeDuplicates pins ("UNNEST over Unknown[] reached Postgres", a comparator dedup's CASE losing the
            // LITERAL label) are RETIRED -- a mixed list is a list of tagged values (SqlExpr.TaggedList) whose dedup
            // keeps its positions in typed channels, so no unknown-typed list reaches the dialect: both tests pass.
            // NOTE 2026-10-20 (plan-route parity, the instance carrier): testConcatenateTypeInference passes again: an instance
            // meeting its siblings at an ancestor with no layout carries its own class in its JSON (__type,
            // MixedEncoding.withClass), and the text carrier spells the subclass it names
            // (LiteralSpelling.jsonInstanceText).
            // refused by name: a construct the Postgres dialect does not spell yet
            // 2026-10-20: the folds' copies print their values now (the adapter's CopyWithUpdate); their accumulator, a
            // list of objects, has no form on Postgres (a list accumulator of anything but scalars is the jsonb carrier)
            one("meta::pure::functions::collection::tests::fold::testFoldFiltering_Function_1__Boolean_1_", "a list accumulator of anything but scalars is the jsonb carrier"),
            one("meta::pure::functions::collection::tests::fold::testFoldToMany_Function_1__Boolean_1_", "a list accumulator of anything but scalars is the jsonb carrier"),
            one("meta::pure::functions::collection::tests::fold::testFold_Function_1__Boolean_1_", "a list accumulator of anything but scalars is the jsonb carrier"),
            one("meta::pure::functions::lang::tests::match::testMatchWithMixedReturnType_Function_1__Boolean_1_", "scalar lowering not yet implemented for TypedDeactivate"),
            // 2026-10-20 (object identity): every object carries its __id now, so the default arm prints it (@_o1); the
            // class's own toString() qualifier is still not in the model the adapter hands over, as on the DuckDB lane
            one("meta::pure::functions::string::tests::toString::testComplexClassToString_Function_1__Boolean_1_", "actual:   '@_o1'")
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
