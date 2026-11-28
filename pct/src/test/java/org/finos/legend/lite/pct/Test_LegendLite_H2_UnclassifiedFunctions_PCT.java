// Copyright 2026 Legend Lite Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct;

import junit.framework.Test;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.finos.legend.pure.code.core.CoreUnclassifiedFunctionsCodeRepositoryProvider;
import org.finos.legend.pure.m3.pct.reports.config.PCTReportConfiguration;
import org.finos.legend.pure.m3.pct.reports.config.exclusion.ExclusionSpecification;
import org.finos.legend.pure.m3.pct.reports.model.Adapter;
import org.finos.legend.pure.m3.pct.shared.model.ReportScope;
import org.finos.legend.pure.runtime.java.interpreted.testHelper.PureTestBuilderInterpreted;

import static org.finos.legend.engine.test.shared.framework.PureTestHelperFramework.wrapSuite;

/**
 * The unclassified PCT suite on H2 (lite's one H2, 2.5.252), channel A: the SAME tests as
 * {@link Test_LegendLite_UnclassifiedFunctions_PCT}, through the same adapter; the database is chosen by the target
 * ({@code LEGENDLITE_PCT_BACKEND=h2}, {@code //pct:pct_h2_unclassified}), and this class carries H2's OWN expected failures --
 * legend-engine's shape, as {@link Test_LegendLite_H2_RelationFunctions_PCT}: every expected failure named and pinned
 * by the text it fails with, and a pinned test that starts passing fails the run, so a fix is recorded by deleting its
 * row. Added 2026-11-26 (the user's question "Should we run full h2 pct like duck?"; in gate 7, `//gates:pct_h2`, since the same day).
 */
public class Test_LegendLite_H2_UnclassifiedFunctions_PCT extends PCTReportConfiguration {

    private static final ReportScope reportScope = CoreUnclassifiedFunctionsCodeRepositoryProvider.unclassifiedFunctions;
    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    // Pinned by a STABLE part of the message (the runner matches by containment): no per-run id.
    // First measured 2026-11-26 (H2 2.5.252; 93 tests, 20 pinned), from what H2 answered: nothing burned down.
    private static final MutableList<ExclusionSpecification> expectedFailures = Lists.mutable.with(
            // LITE'S H2 DIALECT DOES NOT WRITE THESE LIST OPERATIONS (not 'H2 has no list type': corrected 2026-11-26, measured on
            // H2 2.5.252). H2 has arrays -- ARRAY[...], CARDINALITY, a[i], ARRAY_CONTAINS, ARRAY_SLICE, ||, and ARRAY_AGG(x ORDER BY x),
            // MAX, MEDIAN, MODE, STDDEV_SAMP, PERCENTILE_CONT, CORR over FROM UNNEST(ARRAY[...]) [WITH ORDINALITY] answer Pure's values --
            // but lite's H2 dialect carries a list as JSON and refuses these by name (H2.listCall). What H2 truly lacks: LATERAL, a
            // correlated UNNEST, lambdas over an array, a map type; a comparator or fold lambda, a list of class instances and a list of
            // mixed types need more than an array (the split per test: docs/records/h2-pct-full-2026-11-26.md)
            one("meta::pure::functions::string::tests::regexpExtract::testRegexpExtractAll_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::string::tests::regexpExtract::testRegexpExtract_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::string::tests::regexpIndexOf::testRegexpIndexOf_Function_1__Boolean_1_", "LIST_GET reached a dialect without a list encoding"),
            one("meta::pure::functions::string::tests::regexpIndexOf::testRegexpIndexOf_GroupNumber_Function_1__Boolean_1_", "LIST_GET reached a dialect without a list encoding"),
            one("meta::pure::functions::string::tests::regexpIndexOf::testRegexpIndexOf_NoMatch_Function_1__Boolean_1_", "LIST_GET reached a dialect without a list encoding"),
            // H2 TRULY CANNOT (A, measured 2026-11-26): no base64 function (TO_BASE64, BASE64_ENCODE: not found); lite refuses by name
            one("meta::pure::functions::string::tests::base64::testDecodeBase64NoPadding_Function_1__Boolean_1_", "decodeBase64 reached H2, which has no from_base64()"),
            one("meta::pure::functions::string::tests::base64::testDecodeBase64RoundTrip_Function_1__Boolean_1_", "decodeBase64 reached H2, which has no from_base64()"),
            one("meta::pure::functions::string::tests::base64::testDecodeBase64_Function_1__Boolean_1_", "decodeBase64 reached H2, which has no from_base64()"),
            one("meta::pure::functions::string::tests::base64::testEncodeBase64RoundTrip_Function_1__Boolean_1_", "encodeBase64 reached H2, which has no to_base64()"),
            one("meta::pure::functions::string::tests::base64::testEncodeBase64_Function_1__Boolean_1_", "encodeBase64 reached H2, which has no to_base64()"),
            // ==== DEFECTS OF LITE'S H2 DIALECT, TO BE FIXED (not limits of H2; found 2026-11-26, each passes on DuckDB) ====
            // (a) the dialect writes a DuckDB function H2 does not have, so H2 answers 'Function "X" not found': the fix is
            // H2's own spelling, or a refusal by name (invariant 4). Delete the row with the fix.
            one("meta::pure::functions::string::tests::jaroWinklerSimilarity::testJaroWinklerSimilarityEqual_Function_1__Boolean_1_", "Function \"JARO_WINKLER_SIMILARITY\" not found"),
            one("meta::pure::functions::string::tests::jaroWinklerSimilarity::testJaroWinklerSimilarityNotEqual_Function_1__Boolean_1_", "Function \"JARO_WINKLER_SIMILARITY\" not found"),
            one("meta::pure::functions::string::tests::levenshteinDistance::testLevenshteinDistanceEqual_Function_1__Boolean_1_", "Function \"LEVENSHTEIN\" not found"),
            one("meta::pure::functions::string::tests::levenshteinDistance::testLevenshteinDistanceNotEqual_Function_1__Boolean_1_", "Function \"LEVENSHTEIN\" not found"),
            one("meta::pure::functions::string::tests::regexpCount::testRegexpCount_CaseInsensitive_Function_1__Boolean_1_", "Function \"REGEXP_EXTRACT_ALL\" not found"),
            one("meta::pure::functions::string::tests::regexpCount::testRegexpCount_Function_1__Boolean_1_", "Function \"REGEXP_EXTRACT_ALL\" not found"),
            // (b) a wrong answer, or SQL H2 cannot read: the value differs from Pure's. Delete the row with the fix.
            one("meta::pure::functions::string::tests::ascii::testAsciiEmptyChar_Function_1__Boolean_1_", "Cannot cast a collection of size 0 to multiplicity [1]"),
            one("meta::pure::functions::string::tests::regexpReplace::testregexpReplaceAll_Function_1__Boolean_1_", "Invalid value \"g\" for parameter {1}"),
            one("meta::pure::functions::string::tests::regexpReplace::testregexpReplace_Function_1__Boolean_1_", "expected: 'aXc'\nactual:   'aXX'"),
            one("meta::pure::functions::string::tests::rpad::testRpadMultiChar_Function_1__Boolean_1_", "expected: 'ppxox'\nactual:   'ppxxx'")
    );

    public static Test suite() {
        return PctCensusGate.wrap("Unclassified", wrapSuite(
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
