// Copyright 2026 Legend Lite Contributors
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
 * The essential PCT suite on H2 (lite's one H2, 2.5.252), channel A: the SAME tests as
 * {@link Test_LegendLite_EssentialFunctions_PCT}, through the same adapter; the database is chosen by the target
 * ({@code LEGENDLITE_PCT_BACKEND=h2}, {@code //pct:pct_h2_essential}), and this class carries H2's OWN expected failures --
 * legend-engine's shape, as {@link Test_LegendLite_H2_RelationFunctions_PCT}: every expected failure named and pinned
 * by the text it fails with, and a pinned test that starts passing fails the run, so a fix is recorded by deleting its
 * row. Added 2026-11-26 (the user's question "Should we run full h2 pct like duck?"; not yet in any gate).
 */
public class Test_LegendLite_H2_EssentialFunctions_PCT extends PCTReportConfiguration {

    private static final ReportScope reportScope = PlatformCodeRepositoryProvider.essentialFunctions;
    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    // Pinned by a STABLE part of the message (the runner matches by containment): no per-run id.
    // First measured 2026-11-26 (H2 2.5.252; 345 tests, 84 pinned), from what H2 answered: nothing burned down.
    private static final MutableList<ExclusionSpecification> expectedFailures = Lists.mutable.with(
            // H2 HAS NO LIST TYPE: the list functions, unnest, fold and the list predicates have no encoding on this dialect, refused by name (as on the relation suite)
            one("meta::pure::functions::collection::tests::add::testAddWithOffset_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::add::testAdd_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::at::testAtError_Function_1__Boolean_1_", "LIST_FILTER reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::at::testAtOtherScenario_Function_1__Boolean_1_", "LIST_FILTER reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::contains::testContainsNonPrimitive_Function_1__Boolean_1_", "collection membership reached a dialect without a list encoding [collection: Call LIST_TRANSFORM]"),
            one("meta::pure::functions::collection::tests::drop::testDropNegativeOnEmptyList_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::drop::testDropNegativeOnNonEmptyList_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::exists::testExistsInSelect_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::exists::testExists_Function_1__Boolean_1_", "collection exists reached a dialect without a list-predicate encoding"),
            one("meta::pure::functions::collection::tests::find::testFindInstance_Function_1__Boolean_1_", "LIST_GET reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::find::testFindLiteralFromVar_Function_1__Boolean_1_", "LIST_GET reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::find::testFindLiteral_Function_1__Boolean_1_", "LIST_GET reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::find::testFindUsingVarForFunction_Function_1__Boolean_1_", "LIST_GET reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::fold::testFoldCollectionAccumulator_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::fold::testFoldEmptyListAndEmptyIdentity_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::fold::testFoldEmptyListAndNonEmptyIdentity_Function_1__Boolean_1_", "fold reached a dialect without a fold encoding"),
            one("meta::pure::functions::collection::tests::fold::testFoldFiltering_Function_1__Boolean_1_", "fold reached a dialect without a fold encoding"),
            one("meta::pure::functions::collection::tests::fold::testFoldMixedAccumulatorTypes_Function_1__Boolean_1_", "fold reached a dialect without a fold encoding"),
            one("meta::pure::functions::collection::tests::fold::testFoldToMany_Function_1__Boolean_1_", "fold reached a dialect without a fold encoding"),
            one("meta::pure::functions::collection::tests::fold::testFold_Function_1__Boolean_1_", "fold reached a dialect without a fold encoding"),
            one("meta::pure::functions::collection::tests::fold::testIntegerSum_Function_1__Boolean_1_", "fold reached a dialect without a fold encoding"),
            one("meta::pure::functions::collection::tests::fold::testStringSum_Function_1__Boolean_1_", "fold reached a dialect without a fold encoding"),
            one("meta::pure::functions::collection::tests::forall::testforAllOnEmptySet_Function_1__Boolean_1_", "collection forAll reached a dialect without a list-predicate encoding"),
            one("meta::pure::functions::collection::tests::forall::testforAllOnNonEmptySetIsFalse_Function_1__Boolean_1_", "collection forAll reached a dialect without a list-predicate encoding"),
            one("meta::pure::functions::collection::tests::forall::testforAllOnNonEmptySetIsTrue_Function_1__Boolean_1_", "collection forAll reached a dialect without a list-predicate encoding"),
            one("meta::pure::functions::collection::tests::get::testGet_Function_1__Boolean_1_", "LIST_GET reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::head::testHeadOnEmptySet_Function_1__Boolean_1_", "LIST_GET reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::indexof::testIndexOf_Function_1__Boolean_1_", "LIST_POSITION reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::keys::testKeys_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::removeDuplicates::testRemoveDuplicatesEmptyListExplicit_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::removeDuplicates::testRemoveDuplicatesPrimitiveNonStandardFunction_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::removeDuplicates::testRemoveDuplicatesPrimitiveStandardFunctionExplicit_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::removeDuplicates::testRemoveDuplicatesPrimitiveStandardFunctionMixedTypes_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::reverse::testReverse_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::slice::testSliceError_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::slice::testSliceOutOfBounds_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::sort::testMixedSortNoComparator_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::sort::testSimpleSortNoComparator_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::sort::testSimpleSortReversed_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::sort::testSimpleSortWithFunctionVariables_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::sort::testSimpleSortWithKey_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::sort::testSimpleSort_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::take::testTakeNegativeOnEmptyList_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::take::testTakeNegativeOnNonEmptyList_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::values::testValues_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::string::tests::joinStrings::testJoinStringsNoStrings_Function_1__Boolean_1_", "collection reduction 'STRING_AGG' reached a dialect without a list encoding"),
            one("meta::pure::functions::string::tests::split::testSplitWithNoSplit_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::string::tests::split::testSplit_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::string::tests::toString::testPairCollectionToString_Function_1__Boolean_1_", "collection reduction 'STRING_AGG' reached a dialect without a list encoding"),
            // H2 HAS NO MAP TYPE: an empty map is refused by name
            one("meta::pure::functions::collection::tests::put::testPut_emptyMap_Function_1__Boolean_1_", "an empty map reached H2, which has no map type"),
            one("meta::pure::functions::collection::tests::putAll::testPutAll_emptyInputMap_Function_1__Boolean_1_", "an empty map reached H2, which has no map type"),
            one("meta::pure::functions::collection::tests::putAll::testPutAll_emptyPutEntries_Function_1__Boolean_1_", "an empty map reached H2, which has no map type"),
            // H2 HAS NO SUCH FUNCTION: lite's H2 dialect refuses by name
            one("meta::pure::functions::date::tests::testDateFromSecond_Function_1__Boolean_1_", "a date-time constructor reached H2, which has no make_timestamp()"),
            one("meta::pure::functions::date::tests::testDateFromSubSecond_Function_1__Boolean_1_", "a date-time constructor reached H2, which has no make_timestamp()"),
            one("meta::pure::functions::string::tests::format::testFormatDate_Function_1__Boolean_1_", "minimal-fraction date format reached a dialect without a trim-zeros token"),
            one("meta::pure::functions::string::tests::format::testFormatFloatWithRounding_Function_1__Boolean_1_", "format specifier '%.' reached H2, which has no printf — only %d / %s concatenate"),
            one("meta::pure::functions::string::tests::format::testFormatFloatWithTruncation_Function_1__Boolean_1_", "format specifier '%.' reached H2, which has no printf — only %d / %s concatenate"),
            one("meta::pure::functions::string::tests::format::testFormatFloatWithZeroPadding_Function_1__Boolean_1_", "format specifier '%.' reached H2, which has no printf — only %d / %s concatenate"),
            // H2'S OWN LIMIT: DATEADD's count is an INT, so a count past 32 bits is refused by H2 (DuckDB and Postgres refuse it too)
            one("meta::pure::functions::date::tests::testAdjustByDaysBigNumber_Function_1__Boolean_1_", "Invalid value \"12345678912\" for parameter \"DATEADD count\""),
            one("meta::pure::functions::date::tests::testAdjustByHoursBigNumber_Function_1__Boolean_1_", "Invalid value \"12345678912\" for parameter \"DATEADD count\""),
            one("meta::pure::functions::date::tests::testAdjustByMinutesBigNumber_Function_1__Boolean_1_", "Invalid value \"12345678912\" for parameter \"DATEADD count\""),
            one("meta::pure::functions::date::tests::testAdjustByMonthsBigNumber_Function_1__Boolean_1_", "Invalid value \"9600000000\" for parameter \"DATEADD count\""),
            one("meta::pure::functions::date::tests::testAdjustBySecondsBigNumber_Function_1__Boolean_1_", "Invalid value \"12345678912\" for parameter \"DATEADD count\""),
            one("meta::pure::functions::date::tests::testAdjustByWeeksBigNumber_Function_1__Boolean_1_", "Invalid value \"12345678912\" for parameter \"DATEADD count\""),
            // THE PLATFORM'S OWN (each pinned on the DuckDB lane too, by the same cause): error positions, lowering, instance identity, a Float past 2^53
            one("meta::pure::functions::collection::tests::head::testHeadComplex_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::model::CO_Person"),
            one("meta::pure::functions::date::tests::testDayOfMonthError_Function_1__Boolean_1_", "Execution error column mismatch. Actual: 23 where expected: 36"),
            one("meta::pure::functions::date::tests::testHourError_Function_1__Boolean_1_", "Execution error column mismatch. Actual: 23 where expected: 36"),
            one("meta::pure::functions::date::tests::testMinuteError_Function_1__Boolean_1_", "Execution error column mismatch. Actual: 23 where expected: 36"),
            one("meta::pure::functions::date::tests::testNewDateError_Function_1__Boolean_1_", "Execution error column mismatch. Actual: 23 where expected: 29"),
            one("meta::pure::functions::date::tests::testSecondError_Function_1__Boolean_1_", "Execution error column mismatch. Actual: 23 where expected: 36"),
            one("meta::pure::functions::lang::tests::match::testMatchWithMixedReturnType_Function_1__Boolean_1_", "scalar lowering not yet implemented for TypedDeactivate"),
            one("meta::pure::functions::math::tests::abs::testBigFloatAbs_Function_1__Boolean_1_", "expected: 123456789123456789.99\nactual:   123456789123456780.0"),
            // as on DuckDB (pinned there by its actual, '@_o1'); H2 spells the identity text '@_"o1"'
            one("meta::pure::functions::string::tests::toString::testComplexClassToString_Function_1__Boolean_1_", "expected: '// Warning: Good for gin -- Sad times no tonic'\nactual:   '@_\"o1\"'"),
            // ==== DEFECTS OF LITE'S H2 DIALECT, TO BE FIXED (not limits of H2; found 2026-11-26, each passes on DuckDB) ====
            // (a) the dialect writes a DuckDB function H2 does not have, so H2 answers 'Function "X" not found': the fix is
            // H2's own spelling, or a refusal by name (invariant 4). Delete the row with the fix.
            one("meta::pure::functions::collection::tests::put::testPut_addsEntry_Function_1__Boolean_1_", "Function \"MAP_CONCAT\" not found"),
            one("meta::pure::functions::collection::tests::put::testPut_overridesEntry_Function_1__Boolean_1_", "Function \"MAP_CONCAT\" not found"),
            one("meta::pure::functions::collection::tests::putAll::testPutAll_overridesExistingAndAddNew_Function_1__Boolean_1_", "Function \"MAP_CONCAT\" not found"),
            one("meta::pure::functions::date::tests::testDateFromDay_Function_1__Boolean_1_", "Function \"MAKE_DATE\" not found"),
            one("meta::pure::functions::math::tests::testCubeRoot_Function_1__Boolean_1_", "Function \"CBRT\" not found"),
            one("meta::pure::functions::string::tests::reverse::testReverseString_Function_1__Boolean_1_", "Function \"REVERSE\" not found"),
            // (b) a wrong answer, or SQL H2 cannot read: the value differs from Pure's. Delete the row with the fix.
            one("meta::pure::functions::math::tests::rem::testRemWithMixedIntegersAndFloats_Function_1__Boolean_1_", "expected: 2.5\nactual:   3.0"),
            one("meta::pure::functions::string::tests::format::testFormatBoolean_Function_1__Boolean_1_", "expected: 'the quick brown fox jumps over the lazy true'\nactual:   'the quick brown fox jumps over the lazy TRUE'"),
            one("meta::pure::functions::string::tests::parseDate::testParseDateWithTimezone_Function_1__Boolean_1_", "Cannot parse \"TIMESTAMP WITH TIME ZONE\" constant \"2014-02-27T10:01:35.231-0500\""),
            one("meta::pure::functions::string::tests::toString::testFloatToStringWithNegativeExponent_Function_1__Boolean_1_", "expected: '0.000000013421'\nactual:   '1.3421E-8'"),
            one("meta::pure::functions::string::tests::toString::testFloatToStringWithPositiveExponent_Function_1__Boolean_1_", "expected: '134210000.0'\nactual:   '1.3421E8'")
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
