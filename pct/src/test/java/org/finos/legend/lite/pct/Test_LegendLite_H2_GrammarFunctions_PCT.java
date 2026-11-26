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
 * The grammar PCT suite on H2 (lite's one H2, 2.5.252), channel A: the SAME tests as
 * {@link Test_LegendLite_GrammarFunctions_PCT}, through the same adapter; the database is chosen by the target
 * ({@code LEGENDLITE_PCT_BACKEND=h2}, {@code //pct:pct_h2_grammar}), and this class carries H2's OWN expected failures --
 * legend-engine's shape, as {@link Test_LegendLite_H2_RelationFunctions_PCT}: every expected failure named and pinned
 * by the text it fails with, and a pinned test that starts passing fails the run, so a fix is recorded by deleting its
 * row. Added 2026-11-26 (the user's question "Should we run full h2 pct like duck?"; not yet in any gate).
 */
public class Test_LegendLite_H2_GrammarFunctions_PCT extends PCTReportConfiguration {

    private static final ReportScope reportScope = PlatformCodeRepositoryProvider.grammarFunctions;
    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    // Pinned by a STABLE part of the message (the runner matches by containment): no per-run id.
    // First measured 2026-11-26 (H2 2.5.252; 137 tests, 18 pinned), from what H2 answered: nothing burned down.
    private static final MutableList<ExclusionSpecification> expectedFailures = Lists.mutable.with(
            // H2 HAS NO LIST TYPE: the list functions, unnest, fold and the list predicates have no encoding on this dialect, refused by name (as on the relation suite)
            one("meta::pure::functions::boolean::tests::equality::equal::testEqualNonPrimitive_Function_1__Boolean_1_", "LIST_TRANSFORM reached a dialect without a list encoding"),
            one("meta::pure::functions::boolean::tests::equality::equal::testEqualVarIdentity_Function_1__Boolean_1_", "LIST_TRANSFORM reached a dialect without a list encoding"),
            one("meta::pure::functions::collection::tests::map::testMapInstance_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::range::testRangeStepError_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::range::testRangeWithStartStopEqual_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::range::testRangeWithStep_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::range::testRangeWithVariables_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::range::testRange_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::range::testReverseRangeWithPositiveStep_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::range::testReverseRangeWithStep_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::collection::tests::range::testReverseRange_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::string::tests::plus::testPlusInCollect_Function_1__Boolean_1_", "UNNEST reached a dialect without an unnest placement"),
            one("meta::pure::functions::string::tests::plus::testPlusInIterate_Function_1__Boolean_1_", "fold reached a dialect without a fold encoding"),
            // THE PLATFORM'S OWN (each pinned on the DuckDB lane too, by the same cause): error positions, lowering, instance identity, a Float past 2^53
            one("meta::pure::functions::collection::tests::filter::testFilterInstance_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::model::CO_Person"),
            one("meta::pure::functions::collection::tests::first::testFirstComplex_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::model::CO_Person"),
            one("meta::pure::functions::collection::tests::map::testMapRelationshipFromManyToMany_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::map::model::M_Location"),
            one("meta::pure::functions::collection::tests::map::testMapRelationshipFromManyToOne_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::map::model::M_Address"),
            one("meta::pure::functions::collection::tests::map::testMapRelationshipFromOneToOne_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::map::model::M_Address")
    );

    public static Test suite() {
        return PctCensusGate.wrap("Grammar", wrapSuite(
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
