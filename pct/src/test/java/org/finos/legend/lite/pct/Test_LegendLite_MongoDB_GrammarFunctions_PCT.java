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
 * The grammar PCT suite on MongoDB (docs/MONGODB_PLAN_2026_10_16.md §6): the SAME tests as the DuckDB lane's, through the
 * same adapter, on the embedded MongoDB 8.0 ({@code LEGENDLITE_PCT_BACKEND=mongodb}, pct/BUILD.bazel), channel A
 * through the plan path (PctPlanRoute: every result crosses as Pure's TDS text), with MongoDB's OWN expected
 * failures -- every one named and pinned by the text it fails with, and a pinned test that starts passing fails the
 * run, so a fix is recorded by deleting its row.
 */
public class Test_LegendLite_MongoDB_GrammarFunctions_PCT extends PCTReportConfiguration {

    private static final ReportScope reportScope = PlatformCodeRepositoryProvider.grammarFunctions;
    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    // Pinned by a STABLE part of the message (the runner matches by containment): no per-run id, no source line.
    // First measured 2026-10-17 (MongoDB 8.0.17, the pinned server; channel A through the plan path, PctPlanRoute).
    // Re-measured 2026-10-17 after the burn-down (docs/MONGODB_PLAN_2026_10_16.md §11.4: windows, quantified
    // comparisons, the Variant's JSON text, lateral and as-of joins, the statistics, the long tail; the adapter
    // reading the value relation's cells by the function's return type).
    // Re-measured 2026-10-17 after the cross-lane pass (§11.4: base64 and the string distances composed, a value
    // root's date precision, the day truncation typed DATE).
    // Re-measured 2026-10-18 after the text carrier (docs/PLAN_PATH_PROGRAM_2026_10_17.md §4, step 3): a class
    // instance, a List, a Pair, a Map, an Any value and a Type cross as Pure's own literal grammar; what stays
    // pinned is the identity tests (instances compared by reference, as on the DuckDB lane), a user element
    // referenced by its simple name, and an instance nested past the layout's depth.
    private static final MutableList<ExclusionSpecification> expectedFailures = Lists.mutable.with(
            // 2026-10-20, object identity (docs/OBJECT_IDENTITY_2026_10_20.md, b0d5e435, measured by the step-9 lane
            // session): eq is identity and equal a keyed class's keys, and the adapter binds each captured object once
            // by a let -- testEqNonPrimitive, testEqualNonPrimitive and the two primitive-extension tests pass
            // NOTE (step 9, 2026-10-19): testDecimalMinus, testDecimalPlus, testDecimalTimes unpinned, a Decimal read
            // by the wire header's type since the DuckDB lane's landing
            // NOTE 2026-10-20 (the parity phase, second batch): testDecimalDivide unpinned -- a divide's operands are
            // doubles, as on every other dialect (MongoDb's DIVIDE)
            // THE PLATFORM'S OWN (pinned on the DuckDB lane too, by the same cause)
            // OneToOne: the captured $address reaches the text since 2026-10-20 (the adapter binds captured objects by
            // lets, object identity); its assertIs then needs the interpreter's own object back (the ids it prints vary
            // per run, so the pin is the class), as on the DuckDB lane
            one("meta::pure::functions::collection::tests::map::testMapRelationshipFromOneToOne_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::map::model::M_Address"),
            // NOTE 2026-10-20 (the parity phase): testRangeStepError unpinned -- a raising operator's literal operand
            // is read through the row (MongoDb.unfolded)
            // THE IDENTITY TESTS: instances compared by reference, which no wire carries (the program's §4)
            one("meta::pure::functions::collection::tests::filter::testFilterInstance_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::model::CO_Person"),
            one("meta::pure::functions::collection::tests::first::testFirstComplex_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::model::CO_Person"),
            one("meta::pure::functions::collection::tests::map::testMapRelationshipFromManyToMany_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::map::model::M_Location"),
            one("meta::pure::functions::collection::tests::map::testMapRelationshipFromManyToOne_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::map::model::M_Address"),
            // 2026-10-20 (the parity phase): two dates without a time compare on their day numbers in any year now
            // (MixedEncoding.dayNumberOrNull), so the test reaches its sub-millisecond date-times, which a BSON date
            // has no form for: MongoDB's own limit
            one("meta::pure::functions::lang::tests::compare::testDateCompare_Function_1__Boolean_1_", "\"a date-time finer than a millisecond (2014-02-13T13:15:19.14231) has no BSON date: refused, never cut\"")
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
