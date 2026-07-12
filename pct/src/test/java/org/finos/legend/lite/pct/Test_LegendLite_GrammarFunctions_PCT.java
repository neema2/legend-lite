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
 * PCT tests for Grammar Functions (boolean, comparison, basic operators).
 */
public class Test_LegendLite_GrammarFunctions_PCT extends PCTReportConfiguration {

    private static final ReportScope reportScope = PlatformCodeRepositoryProvider.grammarFunctions;
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
            one("meta::pure::functions::boolean::tests::equality::eq::testEqEnum_Function_1__Boolean_1_", "\"Assert failed\""),
            one("meta::pure::functions::boolean::tests::equality::eq::testEqNonPrimitive_Function_1__Boolean_1_", "\"Assert failed\""),
            one("meta::pure::functions::boolean::tests::equality::eq::testEqPrimitiveExtension_Function_1__Boolean_1_", "\"unknown type 'meta::pure::functions::boolean::tests::equalitymodel::ExtendedInteger' in @meta::pure::functions::boolean::tests::equalitymodel::ExtendedInteger\""),
            one("meta::pure::functions::boolean::tests::equality::equal::testEqualEnum_Function_1__Boolean_1_", "\"equality between an enum value and a non-matching type is not lowered (enum values render as name strings; cross-type equality would be silently wrong)\""),
            one("meta::pure::functions::boolean::tests::equality::equal::testEqualNonPrimitive_Function_1__Boolean_1_", "\"Assert failed\""),
            one("meta::pure::functions::boolean::tests::equality::equal::testEqualPrimitiveExtension_Function_1__Boolean_1_", "\"unknown type 'meta::pure::functions::boolean::tests::equalitymodel::ExtendedInteger' in @meta::pure::functions::boolean::tests::equalitymodel::ExtendedInteger\""),
            one("meta::pure::functions::collection::tests::filter::testFilterInstance_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::model::CO_Person"),
            one("meta::pure::functions::collection::tests::first::testFirstComplex_Function_1__Boolean_1_", "\"Cannot find property 'lastName' on meta::pure::functions::collection::Pair\""),
            one("meta::pure::functions::collection::tests::getAll::testBasic_Function_1__Boolean_1_", "\"class query under TypedNativeCall is not resolvable yet (H2 vocabulary)\""),
            one("meta::pure::functions::collection::tests::map::testMapRelationshipFromManyToMany_Function_1__Boolean_1_", "\"expected meta::pure::functions::collection::tests::map::model::M_GeographicEntityType, got meta::pure::functions::collection::tests::map::model::M_GeographicEntityType\""),
            one("meta::pure::functions::collection::tests::map::testMapRelationshipFromManyToOne_Function_1__Boolean_1_", "\"expected meta::pure::functions::collection::tests::map::model::M_GeographicEntityType, got meta::pure::functions::collection::tests::map::model::M_GeographicEntityType\""),
            one("meta::pure::functions::collection::tests::map::testMapRelationshipFromOneToOne_Function_1__Boolean_1_", "\"unbound variable '$address'\""),
            one("meta::pure::functions::collection::tests::range::testRangeStepError_Function_1__Boolean_1_", "\"Execution error message mismatch.\nThe actual message was \"No error was thrown\"\nwhere the expected message was:\"range step must not be 0\"\""),
            one("meta::pure::functions::lang::tests::letFn::testLetChainedWithAnotherFunction_Function_1__Boolean_1_", "\"'meta::pure::functions::lang::tests::letFn::TestClass' is not a known class, mapping, runtime, connection, or database\""),
            one("meta::pure::functions::math::tests::minus::testDecimalMinus_Function_1__Boolean_1_", "\"\nexpected: -4.0D\nactual:   -4.0\""),
            one("meta::pure::functions::math::tests::minus::testSingleMinusType_Function_1__Boolean_1_", "\"Cast exception: String cannot be cast to Type\""),
            one("meta::pure::functions::math::tests::plus::testDecimalPlus_Function_1__Boolean_1_", "\"\nexpected: 6.0D\nactual:   6.0\""),
            one("meta::pure::functions::math::tests::plus::testSinglePlusType_Function_1__Boolean_1_", "\"Cast exception: String cannot be cast to Type\""),
            one("meta::pure::functions::string::tests::plus::testMultiPlusWithPropertyExpressions_Function_1__Boolean_1_", "\"expected meta::pure::functions::string::tests::plus::model::P_GeographicEntityType, got meta::pure::functions::string::tests::plus::model::P_GeographicEntityType\""));

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
