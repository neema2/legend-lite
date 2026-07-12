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
 * PCT tests for Essential Functions (core collection, string, math functions).
 */
public class Test_LegendLite_EssentialFunctions_PCT extends PCTReportConfiguration {

    private static final ReportScope reportScope = PlatformCodeRepositoryProvider.essentialFunctions;
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
            // COLUMN-precision limit: message and LINE now match real
            // pure exactly (SQL-raised guards + call-stack source info),
            // but the failing call executes in DuckDB — its column within
            // the test expression cannot be recovered without source-span
            // plumbing through the wire. The reference adapter excludes
            // these too.
            one("meta::pure::functions::collection::tests::at::testAtError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 37\""),
            one("meta::pure::functions::date::tests::testDayOfMonthError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 36\""),
            one("meta::pure::functions::date::tests::testHourError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 36\""),
            one("meta::pure::functions::date::tests::testMinuteError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 36\""),
            one("meta::pure::functions::date::tests::testNewDateError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 29\""),
            one("meta::pure::functions::date::tests::testSecondError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 36\""),
            one("meta::pure::functions::collection::tests::concatenate::testConcatenateTypeInference_Function_1__Boolean_1_", "\"Cast exception: String cannot be cast to CO_GeographicEntity\""),
            one("meta::pure::functions::collection::tests::contains::testContainsWithFunction_Function_1__Boolean_1_", "\"[1:369] expected ')' to close argument list\""),
            one("meta::pure::functions::collection::tests::find::testFindInstance_Function_1__Boolean_1_", ""),
            one("meta::pure::functions::collection::tests::find::testFindUsingVarForFunction_Function_1__Boolean_1_", ""),
            one("meta::pure::functions::collection::tests::fold::testFoldFiltering_Function_1__Boolean_1_", "\"'otherNames' is not a known class, mapping, runtime, connection, or database — user elements in a query need a fully qualified name\""),
            one("meta::pure::functions::collection::tests::fold::testFoldToMany_Function_1__Boolean_1_", "\"'otherNames' is not a known class, mapping, runtime, connection, or database — user elements in a query need a fully qualified name\""),
            one("meta::pure::functions::collection::tests::fold::testFold_Function_1__Boolean_1_", "\"'lastName' is not a known class, mapping, runtime, connection, or database — user elements in a query need a fully qualified name\""),
            one("meta::pure::functions::collection::tests::head::testHeadComplex_Function_1__Boolean_1_", ""),
            one("meta::pure::functions::collection::tests::removeDuplicates::testRemoveDuplicatesPrimitiveNonStandardFunction_Function_1__Boolean_1_", "\"\nexpected: [1, 2, '3']\nactual:   [1, 2, '1', '3', 3]\""),
            one("meta::pure::functions::collection::tests::removeDuplicates::testRemoveDuplicatesPrimitiveStandardFunctionExplicit_Function_1__Boolean_1_", "\"in call to 'meta::pure::functions::collection::removeDuplicates', argument 2: expected at most one value, got many ([*])\""),
            one("meta::pure::functions::date::tests::testAdjustByDaysBigNumber_Function_1__Boolean_1_", "\"Type INT64 with value 12345678912 can't be cast because the value is out of range for the destination type INT32\""),
            one("meta::pure::functions::date::tests::testAdjustByHoursBigNumber_Function_1__Boolean_1_", "\"Interval value 12345678912 hours out of range\""),
            one("meta::pure::functions::date::tests::testAdjustByMicrosecondsBigNumber_Function_1__Boolean_1_", "\"\nexpected: %2021-06-21T09:37:37.4990000+0000\nactual:   %2021-06-21T09:37:37.499+0000\""),
            one("meta::pure::functions::date::tests::testAdjustByMinutesBigNumber_Function_1__Boolean_1_", "\"\nexpected: %-21457-01-08T20:48:00+0000\nactual:   %21458-01-08T20:48:00+0000\""),
            one("meta::pure::functions::date::tests::testAdjustByMonthsBigNumber_Function_1__Boolean_1_", "\"Type INT64 with value 9600000000 can't be cast because the value is out of range for the destination type INT32\""),
            one("meta::pure::functions::date::tests::testAdjustByWeeksBigNumber_Function_1__Boolean_1_", "\"Type INT64 with value 12345678912 can't be cast because the value is out of range for the destination type INT32\""),
            one("meta::pure::functions::date::tests::testMonthNumber_Function_1__Boolean_1_", "\"Attempting to execute an unsuccessful or closed pending query result\nError: Binder Error: Could not choose a best candidate function for the function call \"date_part(STRING_LITERAL, STRING_LITERAL)\". In order to select one, please add explicit type casts.\n	Candidate functions:\n	date_part(VARCHAR, INTERVAL) -> BIGINT\n	date_part(VARCHAR, TIME) -> BIGINT\n	date_part(VARCHAR, TIMESTAMP) -> BIGINT\n	date_part(VARCHAR, TIME WITH TIME ZONE) -> BIGINT\n	date_part(VARCHAR, TIME_NS) -> BIGINT\n	date_part(VARCHAR, TIMESTAMP WITH TIME ZONE) -> BIGINT\n	date_part(VARCHAR, DATE) -> BIGINT\n\n\nLINE 1: SELECT date_part('month', '2015-04') AS value\n               ^\""),
            one("meta::pure::functions::date::tests::testYear_Function_1__Boolean_1_", "\"Attempting to execute an unsuccessful or closed pending query result\nError: Binder Error: Could not choose a best candidate function for the function call \"date_part(STRING_LITERAL, STRING_LITERAL)\". In order to select one, please add explicit type casts.\n	Candidate functions:\n	date_part(VARCHAR, INTERVAL) -> BIGINT\n	date_part(VARCHAR, TIME) -> BIGINT\n	date_part(VARCHAR, TIMESTAMP) -> BIGINT\n	date_part(VARCHAR, TIME WITH TIME ZONE) -> BIGINT\n	date_part(VARCHAR, TIME_NS) -> BIGINT\n	date_part(VARCHAR, TIMESTAMP WITH TIME ZONE) -> BIGINT\n	date_part(VARCHAR, DATE) -> BIGINT\n\n\nLINE 1: SELECT date_part('year', '2015') AS value\n               ^\""),
            // deactivate() reflects the EXPRESSION (a ValueSpecification metamodel
            // object) — legend-lite compiles to SQL and holds no expression tree at
            // run time; metamodel reflection is out of vocabulary.
            one("meta::pure::functions::lang::tests::match::testMatchWithMixedReturnType_Function_1__Boolean_1_", "\"unknown function 'meta::pure::functions::meta::deactivate'\""),
            one("meta::pure::functions::string::tests::format::testFormatDate_Function_1__Boolean_1_", "\"unsupported date-format token 'E' in pattern '[EST]yyyy-MM-dd HH:mm:ss.SSSZ'\""),
            one("meta::pure::functions::string::tests::format::testFormatIntegerWithZeroPadding_Function_1__Boolean_1_", "\"\nexpected: 'the quick brown fox jumps over the lazy -00003'\nactual:   'the quick brown fox jumps over the lazy -0003'\""),
            one("meta::pure::functions::string::tests::format::testFormatList_Function_1__Boolean_1_", "\"class 'meta::pure::functions::collection::List' has no property 'values'\""),
            one("meta::pure::functions::string::tests::format::testFormatRepr_Function_1__Boolean_1_", "\"Invalid type specifier \"r\" for formatting a value of type string\""),
            one("meta::pure::functions::string::tests::format::testSimpleFormatDate_Function_1__Boolean_1_", "\"\nexpected: 'the quick brown fox jumps over the lazy 2014-01-01T00:00:00.000+0000'\nactual:   'the quick brown fox jumps over the lazy 2014-01-01 00:00:00'\""),
            one("meta::pure::functions::string::tests::parseDate::testParseDateTypes_Function_1__Boolean_1_", "\"expected %2014-02-27 to be an instance of DateTime, actual: StrictDate\""),
            one("meta::pure::functions::string::tests::substring::testStartEnd_Function_1__Boolean_1_", "\"\nexpected: 'he quick brown fox jumps over the lazy do'\nactual:   'he quick brown fox jumps over the lazy dog'\""),
            one("meta::pure::functions::string::tests::toString::testClassToString_Function_1__Boolean_1_", "\"'meta::pure::functions::string::tests::toString::STR_Person' is not a known class, mapping, runtime, connection, or database\""),
            one("meta::pure::functions::string::tests::toString::testComplexClassToString_Function_1__Boolean_1_", "expected: '// Warning: Good for gin -- Sad times no tonic'"),
            one("meta::pure::functions::string::tests::toString::testDateTimeToString_Function_1__Boolean_1_", "\"\nexpected: '2014-01-01T00:00:00.00+0000'\nactual:   '2014-01-01T00:00:00.000+0000'\""),
            one("meta::pure::functions::string::tests::toString::testDateTimeWithTimezoneToString_Function_1__Boolean_1_", "\"\nexpected: '2014-01-01T00:00:00.0000+0000'\nactual:   '2014-01-01T00:00:00.000+0000'\""),
            one("meta::pure::functions::string::tests::toString::testEnumerationToString_Function_1__Boolean_1_", "\"'meta::pure::functions::string::tests::toString::STR_GeographicEntityType' is not a known class, mapping, runtime, connection, or database\""),
            one("meta::pure::functions::string::tests::toString::testListToString_Function_1__Boolean_1_", "\"class 'meta::pure::functions::collection::List' has no property 'values'\""));

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
