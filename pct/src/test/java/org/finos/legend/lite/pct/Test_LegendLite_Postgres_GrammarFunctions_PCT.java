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
 * The grammar PCT suite on Postgres 16 (leg P2, docs/POSTGRES_DIALECT_HOMEWORK_2026_10_01.md Q5): the
 * SAME tests as {@link Test_LegendLite_GrammarFunctions_PCT}, through the same adapter, on this JVM's
 * embedded Postgres ({@code LEGENDLITE_PCT_BACKEND=postgres}, pct/BUILD.bazel), with Postgres's OWN
 * expected failures -- legend-engine's shape, as {@link Test_LegendLite_H2_RelationFunctions_PCT}: every
 * expected failure named and pinned by the text it fails with, and a pinned test that starts passing fails
 * the run, so a fix is recorded by deleting its row.
 *
 * <p>First measured 2026-10-02 on Postgres 16.15 (24 rows): the rows are grouped by why -- wrong answers
 * (dialect defects, fixed first), Postgres's own errors, the collections and Variant of leg P4, and
 * constructs the dialect refuses by name.
 */
public class Test_LegendLite_Postgres_GrammarFunctions_PCT extends PCTReportConfiguration {

    private static final ReportScope reportScope = PlatformCodeRepositoryProvider.grammarFunctions;
    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    // Pinned by a STABLE part of the message (the runner matches by containment): no per-run id.
    private static final MutableList<ExclusionSpecification> expectedFailures = Lists.mutable.with(
            // INSTANCE IDENTITY (as on DuckDB): eq/== on class instances is REFERENCE equality in real pure;
            // the harness inlines captured instances BY VALUE, so eq($x,$x) and eq($x,$y) arrive as identical
            // text -- identity is erased before the wire
            one("meta::pure::functions::boolean::tests::equality::eq::testEqNonPrimitive_Function_1__Boolean_1_", "Assert failed"),
            one("meta::pure::functions::boolean::tests::equality::equal::testEqualNonPrimitive_Function_1__Boolean_1_", "Assert failed"),
            // Postgres raises differently (its own error, or a different position or wording)
            one("meta::pure::functions::collection::tests::filter::testFilterInstance_Function_1__Boolean_1_", "ERROR: missing FROM-clause entry for table \"p"),
            // refused by name: a construct the Postgres dialect does not spell yet
            one("meta::pure::functions::boolean::tests::equality::eq::testEqPrimitiveExtension_Function_1__Boolean_1_", "unknown type 'meta::pure::functions::boolean::tests::equalitymodel::ExtendedInteger' in @meta::pure::functions::boolean::tests::equalitymodel::ExtendedInteger"),
            one("meta::pure::functions::boolean::tests::equality::equal::testEqualPrimitiveExtension_Function_1__Boolean_1_", "unknown type 'meta::pure::functions::boolean::tests::equalitymodel::ExtendedInteger' in @meta::pure::functions::boolean::tests::equalitymodel::ExtendedInteger"),
            one("meta::pure::functions::collection::tests::first::testFirstComplex_Function_1__Boolean_1_", "no typed conversion for org.postgresql.util.PGobject (type=ClassType[fqn=meta::pure::functions::collection::tests::model::CO_Firm])"),
            // 2026-10-04 C3b: the same refusal, now by the session owner (Sessions.openPrivate) and by the declared type
            one("meta::pure::functions::collection::tests::getAll::testBasic_Function_1__Boolean_1_", "no in-memory instance of database type 'Postgres'"),
            one("meta::pure::functions::collection::tests::map::testMapRelationshipFromManyToMany_Function_1__Boolean_1_", "no typed conversion for org.postgresql.util.PGobject (type=ClassType[fqn=meta::pure::functions::collection::tests::map::model::M_Location])"),
            one("meta::pure::functions::collection::tests::map::testMapRelationshipFromManyToOne_Function_1__Boolean_1_", "no typed conversion for org.postgresql.util.PGobject (type=ClassType[fqn=meta::pure::functions::collection::tests::map::model::M_Address])"),
            one("meta::pure::functions::collection::tests::map::testMapRelationshipFromOneToOne_Function_1__Boolean_1_", "unbound variable '$address'"),
            one("meta::pure::functions::lang::tests::letFn::testAssignNewInstance_Function_1__Boolean_1_", "no typed conversion for org.postgresql.util.PGobject (type=ClassType[fqn=meta::pure::functions::lang::tests::model::LA_Person])")
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
