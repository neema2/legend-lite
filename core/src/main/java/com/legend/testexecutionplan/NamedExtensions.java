// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import com.legend.executionplan.Extension;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE EDGE RECORDS A TEST PLAN'S VERDICTS NAME, READ FROM THE PLAN ALONE (the slice 2 invariant audit, 2026-11-08;
 * census C2, 2026-11-17: the appeal type moved to the edge, its numbers stay plan data): the numbers its verdicts name,
 * and the check that each is carried.
 */
public final class NamedExtensions {

    private NamedExtensions() {
    }

    /** The appeal numbers {@code plan}'s verdicts name, by verdict id: an engine text's (through the wrappers and parts
     *  that hold one) and a database-judged root loop's rows. */
    public static Map<Integer, List<Integer>> named(TestPlan plan) {
        Map<Integer, List<Integer>> out = new LinkedHashMap<>();
        walk(plan.steps(), out);
        return out;
    }

    /** {@code plan} is self-contained: every verdict naming the edge's records carries one per number it names, in
     *  order (the plan's {@code extensionsByVerdict}, built so) -- refused by name otherwise. */
    public static void requireCarried(TestPlan plan) {
        named(plan).forEach((verdict, numbers) -> {
            List<Extension> carried = plan.extensionsByVerdict().getOrDefault(verdict, List.of());
            if (carried.size() != numbers.size()) {
                throw new IllegalStateException("a test plan whose verdict " + verdict + " names the edge's records "
                        + numbers + ", and carries " + carried.size());
            }
        });
    }

    private static void walk(List<? extends TestPlan.TestStep> steps, Map<Integer, List<Integer>> out) {
        for (TestPlan.TestStep step : steps) {
            switch (step) {
                // each judge's plan carries its own appeals, checked when it runs
                case TestPlan.Isolated i -> {
                }
                case TestPlan.Verdicts group -> {
                    for (TestPlan.Verdict v : group.verdicts()) {
                        TestPlan.Host host = v.host();
                        if (host != null) {
                            for (int n : numbers(host.judgment(), new ArrayList<>())) {
                                add(out, v.id(), n);
                            }
                        }
                    }
                    RootLoop roots = group.roots();
                    if (roots != null) {
                        for (RootLoop.Root r : roots.roots()) {
                            for (RootLoop.Step s : r.steps()) {
                                if (s instanceof RootLoop.Row row && row.appeal() != RootLoop.NO_APPEAL
                                        && row.verdict() != RootLoop.NO_VERDICT) {
                                    add(out, row.verdict(), row.appeal());
                                }
                            }
                        }
                    }
                }
                case TestPlan.Branch b -> {
                    walk(b.then(), out);
                    walk(b.otherwise(), out);
                }
                case TestPlan.Run r -> {
                }
                case TestPlan.FrameRun f -> {
                }
            }
        }
    }
    private static void add(Map<Integer, List<Integer>> out, int verdict, int number) {
        List<Integer> at = out.computeIfAbsent(verdict, k -> new ArrayList<>());
        if (!at.contains(number)) {
            at.add(number);
        }
    }
    /** The appeal numbers a judgment names: an engine text's, through the wrappers and parts that hold one. */
    private static List<Integer> numbers(HostJudgment j, List<Integer> out) {
        switch (j) {
            case HostJudgment.EdgeJudged e -> {
                // an edge-judged comparison under lite's own rules names no appeal (nothing hears one)
                if (e.appeal() != RootLoop.NO_APPEAL) {
                    out.add(e.appeal());
                }
            }
            case HostJudgment.Messaged m -> numbers(m.inner(), out);
            case HostJudgment.Negated n -> numbers(n.inner(), out);
            case HostJudgment.Unrolled u -> u.parts().forEach(p -> numbers(p.judgment(), out));
            case HostJudgment.AssertError ae -> {
                if (ae.inner() != null) {
                    numbers(ae.inner().judgment(), out);
                }
            }
            // no other judgment holds an appeal: each named, so a new one is decided here (the slice 2 audit, 2026-11-08)
            case HostJudgment.AllRows x -> {
            }
            case HostJudgment.Rows x -> {
            }
            case HostJudgment.TdsEquivalent x -> {
            }
            case HostJudgment.GridPair x -> {
            }
            case HostJudgment.Size x -> {
            }
            case HostJudgment.Contains x -> {
            }
            case HostJudgment.Tolerance x -> {
            }
            case HostJudgment.Condition x -> {
            }
            case HostJudgment.Subset x -> {
            }
            case HostJudgment.Empty x -> {
            }
            case HostJudgment.Eq x -> {
            }
            case HostJudgment.InstanceOf x -> {
            }
            case HostJudgment.Is x -> {
            }
            case HostJudgment.Decided x -> {
            }
            case HostJudgment.SameElements x -> {
            }
            case HostJudgment.Equals x -> {
            }
            case HostJudgment.Json x -> {
            }
            case HostJudgment.Rendered x -> {
            }
            case HostJudgment.Raise x -> {
            }
        }
        return out;
    }
}
