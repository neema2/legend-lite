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
 * row. Added 2026-11-26 (the user's question "Should we run full h2 pct like duck?"; not yet in any gate).
 */
public class Test_LegendLite_H2_UnclassifiedFunctions_PCT extends PCTReportConfiguration {

    private static final ReportScope reportScope = CoreUnclassifiedFunctionsCodeRepositoryProvider.unclassifiedFunctions;
    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    // Pinned by a STABLE part of the message (the runner matches by containment): no per-run id.
    // First measured 2026-11-26 (H2 2.5.252; 93 tests, 20 pinned), from what H2 answered: nothing burned down.
    private static final MutableList<ExclusionSpecification> expectedFailures = Lists.mutable.with(
            // H2 HAS NO LIST TYPE: the list functions, unnest, fold and the list predicates have no encoding on this dialect, refused by name (as on the relation suite)
            one("meta::pure::functions::string::tests::regexpExtract::testRegexpExtractAll_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::string::tests::regexpExtract::testRegexpExtract_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::string::tests::regexpIndexOf::testRegexpIndexOf_Function_1__Boolean_1_", "LIST_GET reached a dialect without a list encoding"),
            one("meta::pure::functions::string::tests::regexpIndexOf::testRegexpIndexOf_GroupNumber_Function_1__Boolean_1_", "LIST_GET reached a dialect without a list encoding"),
            one("meta::pure::functions::string::tests::regexpIndexOf::testRegexpIndexOf_NoMatch_Function_1__Boolean_1_", "LIST_GET reached a dialect without a list encoding"),
            // LITE ON H2 (a finding, 2026-11-26): the H2 dialect writes a function H2 does not have, so H2 answers 'not found' instead of lite refusing by name or spelling H2's own
            one("meta::pure::functions::string::tests::jaroWinklerSimilarity::testJaroWinklerSimilarityEqual_Function_1__Boolean_1_", "Function \"JARO_WINKLER_SIMILARITY\" not found"),
            one("meta::pure::functions::string::tests::jaroWinklerSimilarity::testJaroWinklerSimilarityNotEqual_Function_1__Boolean_1_", "Function \"JARO_WINKLER_SIMILARITY\" not found"),
            one("meta::pure::functions::string::tests::levenshteinDistance::testLevenshteinDistanceEqual_Function_1__Boolean_1_", "Function \"LEVENSHTEIN\" not found"),
            one("meta::pure::functions::string::tests::levenshteinDistance::testLevenshteinDistanceNotEqual_Function_1__Boolean_1_", "Function \"LEVENSHTEIN\" not found"),
            one("meta::pure::functions::string::tests::regexpCount::testRegexpCount_CaseInsensitive_Function_1__Boolean_1_", "Function \"REGEXP_EXTRACT_ALL\" not found"),
            one("meta::pure::functions::string::tests::regexpCount::testRegexpCount_Function_1__Boolean_1_", "Function \"REGEXP_EXTRACT_ALL\" not found"),
            // H2 HAS NO SUCH FUNCTION: lite's H2 dialect refuses by name
            one("meta::pure::functions::string::tests::base64::testDecodeBase64NoPadding_Function_1__Boolean_1_", "decodeBase64 reached H2, which has no from_base64()"),
            one("meta::pure::functions::string::tests::base64::testDecodeBase64RoundTrip_Function_1__Boolean_1_", "decodeBase64 reached H2, which has no from_base64()"),
            one("meta::pure::functions::string::tests::base64::testDecodeBase64_Function_1__Boolean_1_", "decodeBase64 reached H2, which has no from_base64()"),
            one("meta::pure::functions::string::tests::base64::testEncodeBase64RoundTrip_Function_1__Boolean_1_", "encodeBase64 reached H2, which has no to_base64()"),
            one("meta::pure::functions::string::tests::base64::testEncodeBase64_Function_1__Boolean_1_", "encodeBase64 reached H2, which has no to_base64()"),
            // A WRONG ANSWER ON H2 (a finding, 2026-11-26: passes on DuckDB; a lite bug on H2, not H2's limit)
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
