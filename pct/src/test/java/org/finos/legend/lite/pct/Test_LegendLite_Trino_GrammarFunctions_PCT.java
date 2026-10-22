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
 * The grammar PCT suite on Trino (docs/CLOUD_BACKENDS_PCT_2026_10_11.md §18): the SAME tests as
 * {@link Test_LegendLite_GrammarFunctions_PCT}, through the same adapter, on the embedded Trino 483
 * ({@code LEGENDLITE_PCT_BACKEND=trino}, pct/BUILD.bazel), with Trino's OWN expected failures -- legend-engine's
 * shape, as {@link Test_LegendLite_Postgres_GrammarFunctions_PCT}: every expected failure named and pinned by the text
 * it fails with, and a pinned test that starts passing fails the run, so a fix is recorded by deleting its row.
 */
public class Test_LegendLite_Trino_GrammarFunctions_PCT extends PCTReportConfiguration {

    private static final ReportScope reportScope = PlatformCodeRepositoryProvider.grammarFunctions;
    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    // Pinned by a STABLE part of the message (the runner matches by containment): no per-run id.
    // First measured 2026-10-12 (Trino 483, the pinned server).
    private static final MutableList<ExclusionSpecification> expectedFailures = Lists.mutable.with(
            // INSTANCE IDENTITY (as on DuckDB): the asserts want the interpreter's ORIGINAL instances back, or eq/==
            // by reference; the harness inlines captured instances BY VALUE, so identity is erased before the wire
            one("meta::pure::functions::collection::tests::filter::testFilterInstance_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::model::CO_Person"),
            one("meta::pure::functions::collection::tests::first::testFirstComplex_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::model::CO_Person"),
            one("meta::pure::functions::collection::tests::map::testMapRelationshipFromManyToMany_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::map::model::M_Location"),
            one("meta::pure::functions::collection::tests::map::testMapRelationshipFromManyToOne_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::map::model::M_Address"),
            // 2026-10-20, object identity (docs/OBJECT_IDENTITY_2026_10_20.md, b0d5e435, measured by the step-9 lane
            // session): eq is identity and equal a keyed class's keys, and the adapter binds each captured object once
            // by a let -- testEqNonPrimitive, testEqualNonPrimitive and the two primitive-extension tests pass
            // THE PLATFORM'S OWN (each pinned on the DuckDB lane too, by the same cause): name resolution, lowering
            // and harness limits, the 0-based indexOf, error positions and messages
            // OneToOne: the captured $address reaches the text since 2026-10-20 (the adapter binds captured objects by
            // lets, object identity); its assertIs then needs the interpreter's own object back (the ids it prints vary
            // per run, so the pin is the class), as on the DuckDB lane
            one("meta::pure::functions::collection::tests::map::testMapRelationshipFromOneToOne_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::map::model::M_Address")
            // NOTE 2026-10-20 (the parity phase): the instance tests whose unset property was a NULL of no type
            // unpinned -- the field takes the slot its builder declared (Trino.structLit)
            // NOTE 2026-10-20 (the parity phase, second batch): testBasic unpinned -- on a database with no temporary
            // table the system metamodel store's tables are the query's own rows, inline (PlanMaker.inlineRows)
            // NOTE 2026-10-20 (plan-route parity, a type value as its path): testLetChainedWithAnotherFunction passes again: a type
            // value carries its path (PlatformTypes.typeValuePath), so the text carrier's reader resolves TestClass
            // whatever its namesakes; a printer takes the name.
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
