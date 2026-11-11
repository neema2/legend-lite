// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.legend.edge.EffectTexts;
import com.legend.edge.SeedTexts;
import com.legend.executionplan.DatabaseType;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testexecutionplan.Agreement;
import com.legend.testexecutionplan.Appeal;
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

    private static final ExecutionPlan.Multiplicity ONE = new ExecutionPlan.Multiplicity(1, 1);
    private static final Failure FAILURE = new Failure(Failure.Kind.ILLEGAL_STATE, "refused", null, null, null, "here");
    private static final Assertion ASSERTION = new Assertion("assertEquals", "equality", true, null);

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
                new RootLoop.Root("assertEquals", List.of(new RootLoop.Row("row", 1, null, FAILURE, 1, "foreign-dialect:DB2"),
                        new RootLoop.Row("refused", RootLoop.NO_VERDICT, FAILURE, null, RootLoop.NO_APPEAL, null)),
                        new RootLoop.Message(constant(), null, true)),
                new RootLoop.Root("assert", List.of(new RootLoop.Decided(FAILURE, true)),
                        new RootLoop.Message(null, FAILURE, false))),
                Map.of("fragment", "reason"));
        TestPlan.Verdicts group = new TestPlan.Verdicts(verdicts, null, null,
                TestPlan.JudgingRules.COMPILED_PURE_COMPAT, roots, new Agreement(1, 2));
        TestPlan.Branch branch = new TestPlan.Branch(constant(), List.of(), List.of());
        Map<Integer, List<Extension>> byStep = new LinkedHashMap<>();
        byStep.put(0, List.of(new EffectTexts(List.of(new EffectTexts.Statement("insert", false),
                new EffectTexts.Statement("select", true))), new SeedTexts(List.of("create table t"))));
        // a live source's load: the edge records none of it (B2a, 2026-11-11)
        byStep.put(1, List.of(new com.legend.edge.LiveSourceLoad()));
        Map<Integer, List<Extension>> byVerdict = new LinkedHashMap<>();
        byVerdict.put(1, List.of(rowsAppeal(), new Appeal(2, "contract", Appeal.Kind.CONTRACT, "words", null,
                new Appeal.Batched("golden", null, false), 1), new Appeal(3, "tdg", Appeal.Kind.TDG, null, null, null,
                RootLoop.NO_APPEAL)));
        TestPlan plan = new TestPlan(List.of(), List.of(group, branch), TestPlan.Judge.AGREEMENT, byStep, byVerdict,
                true, anyTarget(), new TestPlan.Release(List.of("DROP TABLE IF EXISTS temp.legend_data_0123456789abcdef"),
                anyTarget()));

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
                new HostJudgment.AllRows("allRows", true, "message"),
                new HostJudgment.Rows("rows", true, List.of(new HostJudgment.RowArg.Whole(),
                        new HostJudgment.RowArg.Field("f"), new HostJudgment.RowArg.Literal("Long", "1")),
                        "my::C", null, new HostJudgment.Pairing.Zip(new HostJudgment.Pairing.Arm(0),
                                new HostJudgment.Pairing.Arm(1))),
                new HostJudgment.Unrolled(List.of(part)),
                new HostJudgment.TdsEquivalent(true),
                new HostJudgment.GridPair("grid", true, false),
                new HostJudgment.Size(false),
                new HostJudgment.Contains(e, new HostJudgment.SideFacts(null, new KindClass.Enum("my::E"), false,
                        false, false, "E", "E")),
                new HostJudgment.Tolerance(),
                new HostJudgment.Condition("condition", false),
                new HostJudgment.Subset("subset", true),
                new HostJudgment.Empty(true),
                new HostJudgment.Eq(e, a, keys),
                new HostJudgment.InstanceOf("my::C", Set.of("my::C", "my::B")),
                new HostJudgment.Is("is"),
                new HostJudgment.Decided(true, "held"),
                new HostJudgment.SameElements(e, new HostJudgment.SideFacts("my::F", new KindClass.Element("my::F"),
                        false, false, false, "F", "F"), true, keys),
                new HostJudgment.Equals("equals", true, false, true, e, a, keys,
                        new HostJudgment.FlatFacts(true, false, false, true)),
                new HostJudgment.Json(true),
                new HostJudgment.Rendered("rendered", false, "form", null, "aForm", true, true),
                new HostJudgment.Raise(FAILURE),
                new HostJudgment.EngineText("text", "golden", "ours", false, "decline", "origin", 1, true),
                new HostJudgment.AssertError("expected", 3L, null, FAILURE, part),
                new HostJudgment.Messaged(new HostJudgment.Size(true), 1, "unplanned"));
    }

    /** Any database's target: the round trip does not depend on which. */
    private static ExecutionPlan.Target anyTarget() {
        return new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(DatabaseType.values()[0], null), List.of(),
                List.of());
    }

    private static Appeal rowsAppeal() {
        Appeal.Rows rows = new Appeal.Rows(new Appeal.Read(constant(), shape(), null),
                new Appeal.Read(null, null, null, new Appeal.Unlisted("DateTimeParseException", "bad text")),
                "select 1", "my::Mapping", "my::C", true, false, List.of("k"), "plan", Map.of("p", List.of("1")),
                List.of(new Appeal.TempTable("temp", "Integer", List.of("1", "2"))),
                new Appeal.InListSeed("String", List.of("a")),
                anyTarget(),
                new Appeal.Enums(List.of("my::E"), Map.of("my::E", Map.of("1", "A")), Map.of("kind", "my::E")));
        return new Appeal(1, "rows", Appeal.Kind.ROWS, null, rows, null, RootLoop.NO_APPEAL);
    }

    private static TestPlan.Side constant() {
        return new TestPlan.Side.Constant(List.of(new ExecutionPlan.Column("v", "String", ONE, null, null, null)),
                List.of(List.of("text")), false);
    }

    private static SideShape shape() {
        return new SideShape(SideShape.Kind.TABULAR, "Relation", List.of(new SideShape.Column("v", "String", ONE)),
                List.of(new SideShape.Column("sum", "Integer", null)), true, false, 1, "[1]", false, true, false);
    }
}
