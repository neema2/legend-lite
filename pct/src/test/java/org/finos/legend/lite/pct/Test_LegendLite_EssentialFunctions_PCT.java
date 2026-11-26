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
            // HARNESS SERIALIZATION LOSS: the wire serializes ^$x(prop = expr)
            // as copy('', prop) — the override VALUE never reaches the text we
            // compile. The ^$var(...) copy-with-update feature itself works
            // (TypedCopyInstance; probed); these tests are unrunnable from the
            // serialized form.
            // 2026-10-20, pinned again (the user's rule: as many tests as possible without hacks): a Float is a double
            // on the database, which holds no 20 digits; the batch-1 rule that kept a Float computed from literals
            // alone as a DECIMAL (LiteralSpelling.exactFloatConstant) was a special case -- the same expression over a
            // column was a double -- and is removed
            // 2026-10-27, the result boundary program's phase 4 (the PCT reads typed rows): the double crosses as
            // itself and Pure prints its shortest digits, no longer the database's text of it
            // a Float literal's twenty digits: the interpreter holds a BigDecimal, lite a double (register S120, outcome)
            one("meta::pure::functions::math::tests::abs::testBigFloatAbs_Function_1__Boolean_1_", "\"\nexpected: 123456789123456789.99\nactual:   123456789123456780.0\""),
            // NOTE 2026-10-20 (plan-route parity, the instance carrier): testConcatenateTypeInference passes again: an instance
            // meeting its siblings at an ancestor with no layout carries its own class in its JSON (__type,
            // MixedEncoding.withClass), and the text carrier spells the subclass it names
            // (LiteralSpelling.jsonInstanceText).
            // NOTE 2026-10-20 (the user's rule: as many tests as possible without hacks): testFold, testFoldFiltering
            // and testFoldToMany pass -- the grammar printer printed a copy with updated properties without the values
            // (copy('', lastName)); the adapter prints it itself (CopyWithUpdate, pct_adapter.pure).

            // RELATIONAL substring is the engine's VERBATIM 1-based SQL
            // passthrough (testFilterUsingParseIntegerFunction golden +
            // rows); platform pure is 0-based — the reference DuckDB
            // adapter LEDGERS the same divergence for these exact tests
            // (its exclusions pin the identical off-by-one). The two
            // key-sorts fail through the SAME cause: their sort keys ARE
            // substrings (the comparator machinery itself is pinned
            // green in ValueSortComparatorTest).
            // G5 toString wall: a class instance's toString cannot lower
            // to SQL (Java orchestrates, the database executes) — the wall
            // REPLACED a silently-fabricated VARCHAR cast, so this is a
            // designed refusal, not a wrong answer. The reference DuckDB
            // adapter excludes this same test ("type not supported").
            // 2026-08-27 leg 6a: the B lane PASSES this row (the class's
            // own toString() qualifier dispatches — B-FIXES-A); the A
            // lane's adapter-composed model drops the qualifier AND the
            // __id slot, so the default-id arm fails honestly on the
            // missing struct key (was the loud not-modeled wall)
            // 2026-10-20 (object identity): every object carries its __id now, so the default arm prints it (@_o1); the class's
            // own toString() qualifier is still not in the model the adapter hands over (ModelPacker rebuilds classes
            // property by property)
            one("meta::pure::functions::string::tests::toString::testComplexClassToString_Function_1__Boolean_1_", "actual:   '@_o1'"),
            // 2026-11-19, A3 of the compatibility program, piece 2 (the user's word, 2026-11-19: "approved: push both A3
            // branches and make piece 2's moves"): indexOf is Pure's in the core, 0-based, -1 on a miss (S134), so
            // indexOf::testSimple, testFromIndex and indexof::testIndexOfOneElement pass and leave (measured on this lane).
            // They were here since C1.5c, which gave indexOf legend-engine's 1-based locate(); that meaning is the
            // compatibility edge's now. substring and sort-by-key: Pure's in the core too (S130), so the adapter sets no
            // CORRECT_SQL_SUBSTRING_INDEXING (the 2026-09-12 deviation from the reference adapters, reversed); substr was
            // always corrected (SubstrIndexingTest).

            // INSTANCE IDENTITY through the wire (assertIs against the original
            // instance — addresses are nondeterministic, so the STABLE fragment
            // pins; the reference adapter excludes these too):
            one("meta::pure::functions::collection::tests::find::testFindInstance_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::model::CO_Person"),
            one("meta::pure::functions::collection::tests::find::testFindUsingVarForFunction_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::model::CO_Person"),
            // testHeadComplex: nested-struct reconstruction now REBUILDS
            // the CO_Person correctly (classPropertyTypeOf); the assert
            // then requires the interpreter's ORIGINAL instance back —
            // reference identity, unfixable over the wire (the reference
            // adapter excludes it too, failing even earlier)
            one("meta::pure::functions::collection::tests::head::testHeadComplex_Function_1__Boolean_1_", "instanceOf meta::pure::functions::collection::tests::model::CO_Person"),


            one("meta::pure::functions::collection::tests::at::testAtError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 37\""),
            one("meta::pure::functions::date::tests::testDayOfMonthError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 36\""),
            one("meta::pure::functions::date::tests::testHourError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 36\""),
            one("meta::pure::functions::date::tests::testMinuteError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 36\""),
            one("meta::pure::functions::date::tests::testNewDateError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 29\""),
            one("meta::pure::functions::date::tests::testSecondError_Function_1__Boolean_1_", "\"Execution error column mismatch. Actual: 23 where expected: 36\""),
            // TIMESTAMP-DOMAIN limit: these expected results live at years
            // 1.4M-800M — beyond DuckDB's TIMESTAMP range entirely (290309 BC
            // to 294246 AD). Representing them would mean a string-domain
            // calendar reimplementation; ledgered as a carrier-domain limit
            // (the in-domain BigNumber adjusts — MINUTES, MICROSECONDS — pass).
            // B7 (RaisedErrors): NATIVE database errors now surface with
            // their honest envelope — the class-blind strip is dead. The
            // four BigNumber pins carry the SAME spellings upstream's own
            // DuckDB manifest records for these tests (receipt: census
            // §5c B7 — 'java.sql.SQLException: Invalid Input Error: …'),
            // so the ledger is now byte-comparable to the reference
            // target's.
            one("meta::pure::functions::date::tests::testAdjustByDaysBigNumber_Function_1__Boolean_1_", "\"Invalid Input Error: Type INT64 with value 12345678912 can't be cast because the value is out of range for the destination type INT32\""),
            one("meta::pure::functions::date::tests::testAdjustByHoursBigNumber_Function_1__Boolean_1_", "\"Out of Range Error: Interval value 12345678912 hours out of range\""),
            one("meta::pure::functions::date::tests::testAdjustByMonthsBigNumber_Function_1__Boolean_1_", "\"Invalid Input Error: Type INT64 with value 9600000000 can't be cast because the value is out of range for the destination type INT32\""),
            one("meta::pure::functions::date::tests::testAdjustByWeeksBigNumber_Function_1__Boolean_1_", "\"Invalid Input Error: Type INT64 with value 12345678912 can't be cast because the value is out of range for the destination type INT32\""),
            // deactivate() reflects the EXPRESSION (a ValueSpecification metamodel
            // object) — legend-lite compiles to SQL and holds no expression tree at
            // run time; metamodel reflection is out of vocabulary.
            // 2026-08-27 leg 3b: the B lane PASSES this row (deactivate
            // folds at TYPE time and the verdict reads the TypedTypeRef —
            // B-FIXES-A); the A lane's composition EXECUTES the let
            // eagerly, so the un-lowerable reflection carrier walls
            // loudly at the Lowerer (correct: deactivate is compile-time
            // and must never reach MIR)
            one("meta::pure::functions::lang::tests::match::testMatchWithMixedReturnType_Function_1__Boolean_1_", "scalar lowering not yet implemented for TypedDeactivate"));
            // RELATIONAL DOMAIN SEMANTICS (batch 61, 2026-09-04): the Pure
            // interpreter raises "Unable to compute acos of 2.0"; the
            // engine's relational spec cell is the bare acos(%s), whose
            // out-of-domain answer on H2 is NaN (the relational corpus
            // asserts the row DROPS — testFilterUsingArcCosFunction). Every
            // engine relational PCT adapter ledgers these two tests as
            // expected failures (relational-h2 EssentialFunctions_manifest:
            // "No error was thrown"; duckdb/postgres: the database's own
            // error). Ours: the NaN cell cannot be read back as a Float -- on the plan route (step 9, 2026-10-18)
            // the text carrier spells it NaN and Pure's parseFloat refuses the character (the legacy route's Java
            // BigDecimal said "Infinite or NaN").
            // 2026-10-27, phase 4 (typed rows): the database's NaN reaches the bridge as a double, refused by name
            // (Pure's Float is a decimal), no longer parsed from its text
            // (testArcCosineError and testArcSineError left this list 2026-11-26: acos and asin outside [-1, 1] raise
            // as legend-pure does, "Unable to compute acos of 2.0" -- the user's ruling of 2026-11-25, register S119)

    public static Test suite() {
        // M4 §3.4: the census gate (its SqlTypeCensus pins deleted 2026-11-09: the census had no feeder)
        // invariants at suite teardown (PctCensusGate)
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
