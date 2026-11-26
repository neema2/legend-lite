// Copyright 2026 Legend Lite Contributors
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
 * The standard PCT suite on H2 (lite's one H2, 2.5.252), channel A: the SAME tests as
 * {@link Test_LegendLite_StandardFunctions_PCT}, through the same adapter; the database is chosen by the target
 * ({@code LEGENDLITE_PCT_BACKEND=h2}, {@code //pct:pct_h2_standard}), and this class carries H2's OWN expected failures --
 * legend-engine's shape, as {@link Test_LegendLite_H2_RelationFunctions_PCT}: every expected failure named and pinned
 * by the text it fails with, and a pinned test that starts passing fails the run, so a fix is recorded by deleting its
 * row. Added 2026-11-26 (the user's question "Should we run full h2 pct like duck?"; not yet in any gate).
 */
public class Test_LegendLite_H2_StandardFunctions_PCT extends PCTReportConfiguration {

    private static final ReportScope reportScope = CoreStandardFunctionsCodeRepositoryProvider.standardFunctions;
    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    // Pinned by a STABLE part of the message (the runner matches by containment): no per-run id.
    // First measured 2026-11-26 (H2 2.5.252; 205 tests, 90 pinned), from what H2 answered: nothing burned down.
    private static final MutableList<ExclusionSpecification> expectedFailures = Lists.mutable.with(
            // LITE'S H2 DIALECT DOES NOT WRITE THESE LIST OPERATIONS (not 'H2 has no list type': corrected 2026-11-26, measured on
            // H2 2.5.252). H2 has arrays -- ARRAY[...], CARDINALITY, a[i], ARRAY_CONTAINS, ARRAY_SLICE, ||, and ARRAY_AGG(x ORDER BY x),
            // MAX, MEDIAN, MODE, STDDEV_SAMP, PERCENTILE_CONT, CORR over FROM UNNEST(ARRAY[...]) [WITH ORDINALITY] answer Pure's values --
            // but lite's H2 dialect carries a list as JSON and refuses these by name (H2.listCall). What H2 truly lacks: LATERAL, a
            // correlated UNNEST, lambdas over an array, a map type; a comparator or fold lambda, a list of class instances and a list of
            // mixed types need more than an array (the split per test: docs/records/h2-pct-full-2026-11-26.md)
            one("meta::pure::functions::collection::tests::greatest::testGreatest_Boolean_Function_1__Boolean_1_", "LIST_MAX reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::greatest::testGreatest_DateTime_Function_1__Boolean_1_", "LIST_MAX reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::greatest::testGreatest_Date_Function_1__Boolean_1_", "LIST_POSITION reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::greatest::testGreatest_Empty_Function_1__Boolean_1_", "LIST_MAX reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::greatest::testGreatest_Float_Function_1__Boolean_1_", "LIST_MAX reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::greatest::testGreatest_Integer_Function_1__Boolean_1_", "LIST_MAX reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::greatest::testGreatest_Number_Function_1__Boolean_1_", "LIST_POSITION reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::greatest::testGreatest_StrictDate_Function_1__Boolean_1_", "LIST_MAX reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::greatest::testGreatest_String_Function_1__Boolean_1_", "LIST_MAX reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::in::testInNonPrimitive_Function_1__Boolean_1_", "collection membership reached a dialect without a list encoding [collection: Call LIST_TRANSFORM]"),
            one("meta::pure::functions::collection::tests::least::testLeast_Boolean_Function_1__Boolean_1_", "LIST_MIN reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::least::testLeast_DateTime_Function_1__Boolean_1_", "LIST_MIN reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::least::testLeast_Date_Function_1__Boolean_1_", "LIST_POSITION reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::least::testLeast_Empty_Function_1__Boolean_1_", "LIST_MIN reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::least::testLeast_Float_Function_1__Boolean_1_", "LIST_MIN reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::least::testLeast_Integer_Function_1__Boolean_1_", "LIST_MIN reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::least::testLeast_Number_Function_1__Boolean_1_", "LIST_POSITION reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::least::testLeast_StrictDate_Function_1__Boolean_1_", "LIST_MIN reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::least::testLeast_String_Function_1__Boolean_1_", "LIST_MIN reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::max::testMax_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::min::testMin_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::date::tests::max::testMax_DateArray_Function_1__Boolean_1_", "LIST_MAX reached a dialect without a list encoding"),
            one("meta::pure::functions::date::tests::max::testMax_DateTimeArray_Function_1__Boolean_1_", "LIST_MAX reached a dialect without a list encoding"),
            one("meta::pure::functions::date::tests::max::testMax_Date_Function_1__Boolean_1_", "LIST_POSITION reached a dialect without a list encoding"),
            one("meta::pure::functions::date::tests::max::testMax_StrictDateArray_Function_1__Boolean_1_", "LIST_MAX reached a dialect without a list encoding"),
            one("meta::pure::functions::date::tests::min::testMin_DateArray_Function_1__Boolean_1_", "LIST_MIN reached a dialect without a list encoding"),
            one("meta::pure::functions::date::tests::min::testMin_DateTimeArray_Function_1__Boolean_1_", "LIST_MIN reached a dialect without a list encoding"),
            one("meta::pure::functions::date::tests::min::testMin_Date_Function_1__Boolean_1_", "LIST_POSITION reached a dialect without a list encoding"),
            one("meta::pure::functions::date::tests::min::testMin_StrictDateArray_Function_1__Boolean_1_", "LIST_MIN reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::average::testAverage_Floats_Function_1__Boolean_1_", "LIST_AVG reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::average::testAverage_Integers_Function_1__Boolean_1_", "LIST_AVG reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::average::testAverage_Numbers_Function_1__Boolean_1_", "LIST_AVG reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::corr::testCorr_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::math::tests::covarPopulation::testCovarPopulation_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::math::tests::covarSample::testCovarSample_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::math::tests::max::testMax_FloatsArray_Function_1__Boolean_1_", "LIST_MAX reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::max::testMax_IntegersArray_Function_1__Boolean_1_", "LIST_MAX reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::max::testMax_NumbersArray_Function_1__Boolean_1_", "LIST_MAX reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::max::testMax_Numbers_Function_1__Boolean_1_", "LIST_POSITION reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::maxBy::testMaxBy_Function_1__Boolean_1_", "LIST_GET reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::median::testMedian_Floats_Function_1__Boolean_1_", "LIST_MEDIAN reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::median::testMedian_Integers_Function_1__Boolean_1_", "LIST_MEDIAN reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::median::testMedian_Numbers_Function_1__Boolean_1_", "LIST_MEDIAN reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::min::testMin_FloatsArray_Function_1__Boolean_1_", "LIST_MIN reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::min::testMin_IntegersArray_Function_1__Boolean_1_", "LIST_MIN reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::min::testMin_NumbersArray_Function_1__Boolean_1_", "LIST_MIN reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::min::testMin_Numbers_Function_1__Boolean_1_", "LIST_POSITION reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::minBy::testMinBy_Function_1__Boolean_1_", "LIST_GET reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::mode::testMode_Float_Function_1__Boolean_1_", "LIST_MODE reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::mode::testMode_Integer_Function_1__Boolean_1_", "LIST_MODE reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::mode::testMode_Number_Function_1__Boolean_1_", "LIST_POSITION reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::percentile::testPercentile_Function_1__Boolean_1_", "collection reduction 'QUANTILE_CONT' reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::stdDev::testFloatStdDev_Function_1__Boolean_1_", "collection reduction 'STDDEV_SAMP' reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::stdDev::testIntStdDev_Function_1__Boolean_1_", "collection reduction 'STDDEV_SAMP' reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::stdDev::testMixedStdDev_Function_1__Boolean_1_", "collection reduction 'STDDEV_SAMP' reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::stdDev::testNegativeNumberStdDev_Function_1__Boolean_1_", "collection reduction 'STDDEV_SAMP' reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::stdDev::testPopulationStandardDeviation_Function_1__Boolean_1_", "collection reduction 'STDDEV_POP' reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::variance::testVariancePopulation_Function_1__Boolean_1_", "collection reduction 'VAR_POP' reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::variance::testVarianceSample_Function_1__Boolean_1_", "collection reduction 'VAR_SAMP' reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::variance::testVariance_Population_Function_1__Boolean_1_", "collection reduction 'VAR_POP' reached a dialect without a list encoding"),
            one("meta::pure::functions::math::tests::variance::testVariance_Sample_Function_1__Boolean_1_", "collection reduction 'VAR_SAMP' reached a dialect without a list encoding"),
            // LITE'S H2 DIALECT DOES NOT WRITE H2'S OWN FORM (B: H2 answers Pure's value, measured 2026-11-26 on H2 2.5.252; not a
            // limit of H2): bit operations (BITAND, BITOR, BITXOR, BITNOT, LSHIFT, RSHIFT), a date-time from its parts (a DATEADD chain
            // from DATE '1970-01-01'), '%.Nf' (TO_CHAR(x, 'FM9999990.00')), a time bucket (DATEADD of FLOOR(DATEDIFF / n) * n from the
            // epoch, weeks from 1969-12-29), and a count past 32 bits (DATE + n * INTERVAL '1' DAY, DATEADD(YEAR, n / 12, ..) for months)
            one("meta::pure::functions::date::tests::timeBucket::dateTime::testTimeBucketDays_Function_1__Boolean_1_", "a time bucket reached H2, which has no time_bucket()"),
            one("meta::pure::functions::date::tests::timeBucket::dateTime::testTimeBucketHours_Function_1__Boolean_1_", "a time bucket reached H2, which has no time_bucket()"),
            one("meta::pure::functions::date::tests::timeBucket::dateTime::testTimeBucketMinutes_Function_1__Boolean_1_", "a time bucket reached H2, which has no time_bucket()"),
            one("meta::pure::functions::date::tests::timeBucket::dateTime::testTimeBucketMonths_Function_1__Boolean_1_", "a time bucket reached H2, which has no time_bucket()"),
            one("meta::pure::functions::date::tests::timeBucket::dateTime::testTimeBucketSeconds_Function_1__Boolean_1_", "a time bucket reached H2, which has no time_bucket()"),
            one("meta::pure::functions::date::tests::timeBucket::dateTime::testTimeBucketWeeks_Function_1__Boolean_1_", "a time bucket reached H2, which has no time_bucket()"),
            one("meta::pure::functions::date::tests::timeBucket::dateTime::testTimeBucketYears_Function_1__Boolean_1_", "a time bucket reached H2, which has no time_bucket()"),
            one("meta::pure::functions::date::tests::timeBucket::strictDate::testTimeBucketDays_Function_1__Boolean_1_", "a time bucket reached H2, which has no time_bucket()"),
            one("meta::pure::functions::date::tests::timeBucket::strictDate::testTimeBucketMonths_Function_1__Boolean_1_", "a time bucket reached H2, which has no time_bucket()"),
            one("meta::pure::functions::date::tests::timeBucket::strictDate::testTimeBucketWeeks_Function_1__Boolean_1_", "a time bucket reached H2, which has no time_bucket()"),
            one("meta::pure::functions::date::tests::timeBucket::strictDate::testTimeBucketYears_Function_1__Boolean_1_", "a time bucket reached H2, which has no time_bucket()"),
            one("meta::pure::functions::math::tests::bitAnd::testBitAnd_LargeNumbers_Function_1__Boolean_1_", "BIT_AND reached a dialect without bit-op support"),
            one("meta::pure::functions::math::tests::bitAnd::testBitAnd_NegativeNumbers_Function_1__Boolean_1_", "BIT_AND reached a dialect without bit-op support"),
            one("meta::pure::functions::math::tests::bitAnd::testBitAnd_SmallNumbers_Function_1__Boolean_1_", "BIT_AND reached a dialect without bit-op support"),
            one("meta::pure::functions::math::tests::bitNot::testBitNot_LargeNumbers_Function_1__Boolean_1_", "bitNot reached H2, which has no xor()"),
            one("meta::pure::functions::math::tests::bitNot::testBitNot_NegativeNumbers_Function_1__Boolean_1_", "bitNot reached H2, which has no xor()"),
            one("meta::pure::functions::math::tests::bitNot::testBitNot_SmallNumbers_Function_1__Boolean_1_", "bitNot reached H2, which has no xor()"),
            one("meta::pure::functions::math::tests::bitOr::testBitOr_LargeNumbers_Function_1__Boolean_1_", "BIT_OR reached a dialect without bit-op support"),
            one("meta::pure::functions::math::tests::bitOr::testBitOr_NegativeNumbers_Function_1__Boolean_1_", "BIT_OR reached a dialect without bit-op support"),
            one("meta::pure::functions::math::tests::bitOr::testBitOr_SmallNumbers_Function_1__Boolean_1_", "BIT_OR reached a dialect without bit-op support"),
            one("meta::pure::functions::math::tests::bitShiftLeft::testBitShiftLeft_UpTo62Bits_Function_1__Boolean_1_", "BIT_SHIFT_LEFT reached a dialect without bit-op support"),
            one("meta::pure::functions::math::tests::bitShiftRight::testBitShiftRight_UpTo62Bits_Function_1__Boolean_1_", "BIT_SHIFT_RIGHT reached a dialect without bit-op support"),
            one("meta::pure::functions::math::tests::bitXor::testBitXor_LargeNumbers_Function_1__Boolean_1_", "BIT_XOR reached a dialect without bit-op support"),
            one("meta::pure::functions::math::tests::bitXor::testBitXor_NegativeNumbers_Function_1__Boolean_1_", "BIT_XOR reached a dialect without bit-op support"),
            one("meta::pure::functions::math::tests::bitXor::testBitXor_SmallNumbers_Function_1__Boolean_1_", "BIT_XOR reached a dialect without bit-op support"),
            // NOT SETTLED (C, 2026-11-26): H2 has no MAP type (a map as JSON: JSON_OBJECT() answers {}, but H2 has no JSON merge for
            // put/putAll); a signed 64-bit hashCode (H2 has HASH and ORA_HASH, not Pure's); the minimal-fraction date token (FORMATDATETIME
            // answered the other patterns of testFormatDate, not probed for this one). Lite refuses each by name
            one("meta::pure::functions::hashCode::tests::testHashCode_Function_1__Boolean_1_", "signed 64-bit hashCode reached a dialect without a spelling"),
            one("meta::pure::functions::math::hashCode::tests::testHashCodeAggregate_Function_1__Boolean_1_", "signed 64-bit hashCode reached a dialect without a spelling"),
            // ==== DEFECTS OF LITE'S H2 DIALECT, TO BE FIXED (not limits of H2; found 2026-11-26, each passes on DuckDB) ====
            // (a) the dialect writes a DuckDB function H2 does not have, so H2 answers 'Function "X" not found': the fix is
            // H2's own spelling, or a refusal by name (invariant 4). Delete the row with the fix.
            one("meta::pure::functions::math::tests::maxBy::testSimpleGroupByMaxBy_Function_1__Boolean_1_", "Function \"ARG_MAX\" not found"),
            one("meta::pure::functions::math::tests::minBy::testSimpleGroupByMinBy_Function_1__Boolean_1_", "Function \"ARG_MIN\" not found")
    );

    public static Test suite() {
        return PctCensusGate.wrap("Standard", wrapSuite(
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
