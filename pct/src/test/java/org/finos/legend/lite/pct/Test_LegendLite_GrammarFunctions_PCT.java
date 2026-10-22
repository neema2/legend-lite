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
            // NOTE 2026-10-20 (object identity, docs/OBJECT_IDENTITY_2026_10_20.md): testEqNonPrimitive passes -- every
            // object carries its identity, and the adapter binds each captured object once by a let (capturedObjects,
            // objectLets), so eq(, ) compares two objects and eq(, ) one.
            // NOTE 2026-10-20 (the user's rule: as many tests as possible without hacks): testEqualNonPrimitive,
            // testEqPrimitiveExtension and testEqualPrimitiveExtension pass -- the model the adapter hands over keeps
            // <<equality.Key>> and the test's own Primitive (ModelPacker), and the plan's lowering compares a keyed
            // class by its keys (Lowerer.withInstanceKeys on the plan path, D91; only the legacy executor armed it).
            one("meta::pure::functions::collection::tests::filter::testFilterInstance_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::model::CO_Person"),
            // testFirstComplex: nested-struct reconstruction now REBUILDS
            // the CO_Person correctly (classPropertyTypeOf); the assert
            // then requires the interpreter's ORIGINAL instance back —
            // reference identity, unfixable over the wire (the reference
            // adapter excludes it too, failing even earlier)
            one("meta::pure::functions::collection::tests::first::testFirstComplex_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::model::CO_Person"),

            // (getAll::testBasic FIXED 2026-08-28, metamodel-store leg:
            // Class.all() is a mapped-class query over the seeded
            // metamodel.classes table — the B1-era wall entry removed,
            // the ledger failed loudly on the fix by design)
            // INSTANCE IDENTITY through the wire: these asserts require the
            // ORIGINAL let-bound instances back (assertIs / address-equal) —
            // reference identity cannot cross a value serialization boundary;
            // the reference adapter excludes this family too.
            one("meta::pure::functions::collection::tests::map::testMapRelationshipFromManyToMany_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::map::model::M_Location"),   // F13b(a) 2026-08-23: the flatten fix decodes the collection correctly (Channel B PASSES this test); the reference INTERPRETER still fails on REFERENCE IDENTITY — DB-round-tripped instances mint fresh interpreter ids (@_x vs @_y), inherent to the adapter seam
            one("meta::pure::functions::collection::tests::map::testMapRelationshipFromManyToOne_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::map::model::M_Address"),
            // OneToOne: the captured $address reaches the text since 2026-10-20 (the adapter binds captured objects by
            // lets, object identity); its assertIs then needs the interpreter's own object back, as the two above
            one("meta::pure::functions::collection::tests::map::testMapRelationshipFromOneToOne_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::map::model::M_Address")
            // NOTE 2026-10-20 (plan-route parity, a type value as its path): testLetChainedWithAnotherFunction passes again: a type
            // value carries its path (PlatformTypes.typeValuePath), so the text carrier's reader resolves TestClass
            // whatever its namesakes; a printer takes the name.
    );

    public static Test suite() {
        // M4 §3.4: the census gate pins this JVM's SqlTypeCensus
        // invariants at suite teardown (PctCensusGate)
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
