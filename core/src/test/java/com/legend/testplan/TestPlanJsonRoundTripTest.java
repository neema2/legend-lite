// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.legend.edge.EffectTexts;
import com.legend.edge.RefereeLoad;
import com.legend.executionplan.DatabaseType;
import com.legend.executionplan.ExecutionPlan;
import com.legend.edge.Appeal;
import com.legend.testexecutionplan.Assertion;
import com.legend.testexecutionplan.Extension;
import com.legend.testexecutionplan.Failure;
import com.legend.testexecutionplan.HostJudgment;
import com.legend.testexecutionplan.KeyTree;
import com.legend.testexecutionplan.KindClass;
import com.legend.testexecutionplan.RootLoop;
import com.legend.testexecutionplan.SideShape;
import com.legend.testexecutionplan.TestPlan;
import com.legend.testexecutionplan.TestPlanJson;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/**
 * THE TEST PLAN'S JSON, EVERY RECORD (the slice 2 audit, 2026-11-08): the lanes round-trip every plan they plan
 * ({@link TestPlanJson#requireRoundTrip}), which covers only the kinds a lane happens to plan; this plan holds one of
 * each -- every host judgment, every step, the root loop's rows and decisions, the agreement, an appeal of each read
 * form, and the edge's two records -- and must read back equal to itself.
 */
class TestPlanJsonRoundTripTest {

    /** A typed row's report (item 3's S4: every database row is typed). */
    private static final RootLoop.Report CONDITION = new RootLoop.Report(RootLoop.Family.CONDITION, true, null, null,
            null, -1);

    private static final ExecutionPlan.Multiplicity ONE = new ExecutionPlan.Multiplicity(1, 1);
    private static final Failure FAILURE = new Failure(Failure.Kind.NOT_IMPLEMENTED, "refused", null, null, null);

    /** A planned failure's record round-trips, and no message rides it (stage B's BE, 2026-11-21, the one error model):
     *  a compile error inside two frames at a position, and a file's size against a load's limit. */
    private static final Failure COMPILE_FAILURE = new Failure(Failure.Kind.MODEL, null,
            com.legend.error.LegendCompileException.Phase.TYPE, null, null, new com.legend.error.ErrorRecord(
                    com.legend.error.CompileKind.WINDOW_FRAME_INVERTED, List.of(), "[2:3]", List.of(
                            new com.legend.error.ErrorRecord.Frame(com.legend.error.FrameKind.INLINED_VIA,
                                    List.of("a", "b")),
                            new com.legend.error.ErrorRecord.Frame(com.legend.error.FrameKind.IN_FUNCTION,
                                    List.of("f")))));
    private static final Failure FILE_FAILURE = new Failure(Failure.Kind.DATA, null, null, null, null,
            com.legend.error.ErrorRecord.of(com.legend.error.ErrorKind.FILE_TOO_LARGE,
                    List.of("/big.csv", "1048577", "1048577", "1")));
    /** A planned assert failure's record (item 3, 2026-11-21): its kind and values, an element's inside its index's
     *  frame; no message rides it. */
    private static final Failure ASSERT_FAILURE = new Failure(Failure.Kind.ASSERT_FAILED, null, null, null, null,
            com.legend.error.AssertKind.EQUALS.record(java.util.Map.of("expected", "1", "actual", "2")).framed(
                    new com.legend.error.ErrorRecord.Frame(com.legend.error.FrameKind.AT_INDEX, List.of("0"))));
    private static final Assertion ASSERTION = new Assertion("assertEquals", "equality", true, null);

    /** A planned assert failure is raised at its turn as the assert's failure over the same record, its message the
     *  catalog's sentence (item 3, 2026-11-21). */
    @Test
    void aPlannedAssertFailureIsRaisedAsItsRecord() {
        RuntimeException e = com.legend.testexecutionplan.Failures.raised(ASSERT_FAILURE);
        org.junit.jupiter.api.Assertions.assertTrue(e instanceof com.legend.error.AssertFailed, e.getClass().getName());
        org.junit.jupiter.api.Assertions.assertEquals(ASSERT_FAILURE.record(),
                ((com.legend.error.AssertFailed) e).record());
        org.junit.jupiter.api.Assertions.assertEquals("expected: 1, actual: 2 at index 0", e.getMessage());
        org.junit.jupiter.api.Assertions.assertEquals(ASSERT_FAILURE, com.legend.testexecutionplan.Failures.of(
                (com.legend.error.AssertFailed) e, "the test"));
    }

    /** A failure refuses a record not its kind's code, and a record beside a decline's reason (the S1 audit's L5). */
    @Test
    void anAssertFailureRefusesARecordNotAnAssertsOrBesideADecline() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> new Failure(
                Failure.Kind.ASSERT_FAILED, null, null, null, null, com.legend.error.ErrorRecord.of(
                        com.legend.error.ErrorKind.DIVIDE_BY_ZERO, List.of())));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> new Failure(
                Failure.Kind.ASSERT_FAILED, null, null, null, "a decline", ASSERT_FAILURE.record()));
    }

    /** A plan whose assert record holds other than its sentence's slots is refused as it is read (the S1 audit's L5). */
    @Test
    void anAssertRecordOfTheWrongArityIsRefusedOnRead() {
        TestPlan.Verdict v = new TestPlan.Verdict(1, "v1", "arm", List.of(constant()), null, null, false,
                new TestPlan.Host(ASSERTION, new HostJudgment.Raise(ASSERT_FAILURE), List.of(shape())));
        TestPlan plan = new TestPlan(List.of(), List.of(new TestPlan.Verdicts(List.of(v), null, null)),
                TestPlan.Judge.HOST, Map.of(), Map.of(), false, anyTarget(),
                new com.legend.testexecutionplan.BodyFacts(true, false, true, "A"));
        String json = com.legend.testexecutionplan.TestPlanJson.write(plan);
        String one = json.replaceFirst("\\[\"1\",\\s*\"2\"\\]", "[\"1\"]");
        org.junit.jupiter.api.Assertions.assertNotEquals(json, one, "the record's two values were found");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> com.legend.testexecutionplan.TestPlanJson.read(one));
    }

    @Test
    void everyRecordReadsBackEqual() {
        List<HostJudgment> judgments = judgments();
        Set<String> covered = new TreeSet<>();
        for (HostJudgment j : judgments) {
            covered.add(j.getClass().getSimpleName());
        }
        Set<String> declared = new TreeSet<>();
        for (Class<?> c : HostJudgment.class.getPermittedSubclasses()) {
            declared.add(c.getSimpleName());
        }
        assertEquals(declared, covered, "a host judgment this test does not round-trip");

        List<TestPlan.Verdict> verdicts = new ArrayList<>();
        int id = 1;
        for (HostJudgment j : judgments) {
            verdicts.add(new TestPlan.Verdict(id, "v" + id, "arm", List.of(constant()), null, null, false,
                    new TestPlan.Host(ASSERTION, j, List.of(shape()))));
            id++;
        }
        RootLoop roots = new RootLoop(List.of(
                // (a refused row and a refused message went 2026-11-21, stage B's BE: a dialect's refusal refuses the
                // body at planning, D)
                new RootLoop.Root("assertEquals", List.of(new RootLoop.Row("row", 1, CONDITION), new RootLoop.Row("second", 2, CONDITION)),
                        new RootLoop.Message(constant(), true)),
                new RootLoop.Root("assert", List.of(new RootLoop.Judged(), new RootLoop.Decided(FAILURE, true)),
                        new RootLoop.Message(constant(), false)),
                // a caller's message whose computing raises: its error rides the root, no message (item 3, 2026-11-21)
                new RootLoop.Root("assertSize", List.of(new RootLoop.Row("third", 3, CONDITION)), null, FILE_FAILURE)),
                Map.of("fragment", "reason"));
        TestPlan.Verdicts group = new TestPlan.Verdicts(verdicts, null, null, roots, null);
        TestPlan.Branch branch = new TestPlan.Branch(constant(), List.of(), List.of());
        Map<Integer, List<Extension>> byStep = new LinkedHashMap<>();
        byStep.put(0, List.of(new EffectTexts(List.of(new EffectTexts.Statement("insert", false),
                new EffectTexts.Statement("select", true))), refereeLoad()));
        Map<Integer, List<Extension>> byVerdict = new LinkedHashMap<>();
        byVerdict.put(1, List.of(rowsAppeal(), new Appeal(2, "contract", Appeal.Kind.CONTRACT, "words",
                null)));
        TestPlan plan = new TestPlan(List.of(), List.of(group, branch), TestPlan.Judge.DATABASE, byStep,
                byVerdict, true, anyTarget(), new com.legend.testexecutionplan.BodyFacts(true, false, true, "FLAXEO"));

        assertNull(TestPlanJson.roundTripDifference(plan));
    }

    private static List<HostJudgment> judgments() {
        HostJudgment.SideFacts e = new HostJudgment.SideFacts("my::C", new KindClass.Instance("my::C"), true, false,
                false, "C", "C");
        HostJudgment.SideFacts a = new HostJudgment.SideFacts(null, KindClass.Primitive.NUMERIC, false, true, true,
                "Integer", "Number");
        KeyTree nested = new KeyTree("my::D", List.of(new KeyTree.Key("id", false, null, false, null)));
        KeyTree tree = new KeyTree("my::C", List.of(new KeyTree.Key("d", true, nested, true, "my::D")));
        HostJudgment.Keys keys = new HostJudgment.Keys(tree, null, true, false, Map.of("my::D", nested));
        HostJudgment.Part part = new HostJudgment.Part(ASSERTION, new HostJudgment.Size(true), 2);
        return List.of(
                new HostJudgment.AllRows("allRows", true, com.legend.error.AssertKind.MESSAGE.record(
                        java.util.Map.of("message", "a caller's message"))),
                new HostJudgment.AllRows("allRows", false, com.legend.error.AssertKind.FALSE.record()),
                new HostJudgment.Rows("rows", true, List.of(new HostJudgment.RowArg.Whole(),
                        new HostJudgment.RowArg.Field("f"), new HostJudgment.RowArg.Literal("Long", "1")),
                        "my::C", null, new HostJudgment.Pairing.Zip(new HostJudgment.Pairing.Arm(0),
                                new HostJudgment.Pairing.Arm(1))),
                new HostJudgment.Unrolled(List.of(part)),
                new HostJudgment.TdsEquivalent(true),
                new HostJudgment.GridPair("grid", true, false),
                new HostJudgment.Size(false),
                new HostJudgment.Contains(e, new HostJudgment.SideFacts(null, new KindClass.Enum("my::E"), false,
                        false, false, "E", "E"), keys),
                new HostJudgment.Tolerance(),
                new HostJudgment.Condition("condition", false),
                new HostJudgment.Subset("subset", true),
                new HostJudgment.Empty(true),
                new HostJudgment.Eq(e, a, keys),
                new HostJudgment.InstanceOf("my::C", Set.of("my::C", "my::B"), "my::C"),
                new HostJudgment.Is("is"),
                new HostJudgment.Decided(true, null),
                new HostJudgment.Decided(false, com.legend.error.AssertKind.EQUALS.record(
                        java.util.Map.of("expected", "a::A", "actual", "a::B"))),
                new HostJudgment.SameElements(e, new HostJudgment.SideFacts("my::F", new KindClass.Element("my::F"),
                        false, false, false, "F", "F"), true, keys),
                new HostJudgment.Equals("equals", true, false, true, e, a, keys,
                        new HostJudgment.FlatFacts(true, false, false, true)),
                new HostJudgment.Json(true),
                new HostJudgment.Rendered("rendered", false, "form", null, "aForm", true, true),
                new HostJudgment.Raise(FAILURE),
                new HostJudgment.Raise(COMPILE_FAILURE),
                new HostJudgment.Raise(FILE_FAILURE),
                new HostJudgment.Raise(ASSERT_FAILURE),
                new HostJudgment.EdgeJudged(new com.legend.edge.EngineTextJudgment("text", "golden", "ours", false, "decline"), 1,
                        "origin", true),
                new HostJudgment.AssertError("expected", 3L, null, FAILURE, part),
                new HostJudgment.Messaged(new HostJudgment.Size(true), 1, FILE_FAILURE),
                new HostJudgment.Messaged(new HostJudgment.Size(true), 1),
                new HostJudgment.Negated(new HostJudgment.Size(false), "NOT_SIZE"));
    }

    /** Any database's target: the round trip does not depend on which. */
    private static ExecutionPlan.Target anyTarget() {
        return new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(DatabaseType.values()[0], null), List.of(),
                List.of(), null, null, null);
    }

    private static Appeal rowsAppeal() {
        Appeal.Rows rows = new Appeal.Rows(new Appeal.Read(constant(), shape(), null),
                new Appeal.Read(null, null, null, new Appeal.Unlisted("DateTimeParseException", "bad text")),
                "select 1", "my::Mapping", "my::C", true, false, List.of("k"), "plan", Map.of("p", List.of("1")),
                List.of(new Appeal.TempTable("temp", "Integer", List.of("1", "2"))),
                new Appeal.InListSeed("String", List.of("a")),
                anyTarget(),
                new Appeal.Enums(List.of("my::E"), Map.of("my::E", Map.of("1", "A")), Map.of("kind", "my::E")));
        return new Appeal(1, "rows", Appeal.Kind.ROWS, null, rows);
    }

    private static TestPlan.Side constant() {
        return new TestPlan.Side.Constant(List.of(new ExecutionPlan.Column("v", "String", ONE, null, null, null)),
                List.of(List.of("text")), false);
    }

    private static SideShape shape() {
        return new SideShape(SideShape.Kind.TABULAR, "Relation", List.of(new SideShape.Column("v", "String", ONE)),
                List.of(new SideShape.Column("sum", "Integer", null)), true, false, 1, "[1]", false, true, false);
    }

    /** A seed's referee's copy (B2b): a statement, the rows as a content blob, a file by its reference read by the
     *  referee's own statements. */
    private static RefereeLoad refereeLoad() {
        com.legend.setup.LoadSteps rows = com.legend.setup.LoadSteps.blob(List.of("INSERT INTO T VALUES (1)"),
                "T");
        List<com.legend.executionplan.ExecutionPlan.LoadStep> steps = new java.util.ArrayList<>();
        steps.add(new com.legend.executionplan.ExecutionPlan.LoadStep.Statement("create table t"));
        steps.addAll(rows.steps());
        steps.add(new com.legend.executionplan.ExecutionPlan.LoadStep.File(new com.legend.executionplan.DataRef(
                com.legend.executionplan.DataRef.Namespace.RESOURCE, "people.csv", "0".repeat(64), 3,
                com.legend.executionplan.DataRef.Csv.DEFAULT), new com.legend.executionplan.ExecutionPlan.LoadStep
                .Delivery.Reads(List.of("SET @legend_csv = ?", "INSERT INTO T SELECT 1"), List.of(
                "SET @legend_csv = NULL"), List.of()), 500));
        return new RefereeLoad(steps, rows.content());
    }
}
