// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.testexecutionplan.Appeal;
import com.legend.testexecutionplan.Extension;
import com.legend.testexecutionplan.HostJudgment;
import com.legend.testexecutionplan.RootLoop;
import com.legend.testexecutionplan.TestPlan;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE APPEALS A TEST PLAN'S VERDICTS NAME (the plan-as-data design's step 8b, 2026-11-08): each engine-text judgment's
 * appeal and each database root's row's, by the verdict that names it -- the plan's {@code extensionsByVerdict}, built
 * from the appeals the arms planned; and, at run, the appeals the plan carries by their numbers.
 */
final class AppealsByVerdict {

    private AppealsByVerdict() {
    }

    /** The appeals {@code plan}'s verdicts name, from {@code planned}, by verdict id. */
    static Map<Integer, List<Extension>> of(TestPlan plan, EngineTextAppeals planned) {
        Map<Integer, List<Extension>> out = new LinkedHashMap<>();
        walk(plan.steps(), planned, out);
        return out;
    }

    private static void walk(List<? extends TestPlan.TestStep> steps, EngineTextAppeals planned,
            Map<Integer, List<Extension>> out) {
        for (TestPlan.TestStep step : steps) {
            switch (step) {
                case TestPlan.Verdicts group -> {
                    for (TestPlan.Verdict v : group.verdicts()) {
                        TestPlan.Host host = v.host();
                        if (host != null) {
                            for (int n : numbers(host.judgment(), new ArrayList<>())) {
                                add(out, v.id(), planned.of(n));
                            }
                        }
                    }
                    RootLoop roots = group.roots();
                    if (roots != null) {
                        for (RootLoop.Root r : roots.roots()) {
                            for (RootLoop.Step s : r.steps()) {
                                if (s instanceof RootLoop.Row row && row.appeal() != RootLoop.NO_APPEAL
                                        && row.verdict() != RootLoop.NO_VERDICT) {
                                    add(out, row.verdict(), planned.of(row.appeal()));
                                }
                            }
                        }
                    }
                }
                case TestPlan.Branch b -> {
                    walk(b.then(), planned, out);
                    walk(b.otherwise(), planned, out);
                }
                case TestPlan.Run r -> {
                }
                case TestPlan.FrameRun f -> {
                }
            }
        }
    }

    private static void add(Map<Integer, List<Extension>> out, int verdict, Appeal a) {
        List<Extension> at = out.computeIfAbsent(verdict, k -> new ArrayList<>());
        if (at.stream().noneMatch(e -> e instanceof Appeal x && x.number() == a.number())) {
            at.add(a);
        }
    }

    /** The appeal numbers a judgment names: an engine text's, through the wrappers and parts that hold one. */
    private static List<Integer> numbers(HostJudgment j, List<Integer> out) {
        switch (j) {
            case HostJudgment.EngineText e -> out.add(e.appeal());
            case HostJudgment.Messaged m -> numbers(m.inner(), out);
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

    /** The stores whose fixtures {@code plan}'s appeals ask the harness to provide before the run (step 8c: the plan
     *  names them; until then the harness's registry was filled while the asserts were planned). */
    static java.util.Set<String> stores(TestPlan plan) {
        java.util.Set<String> out = new java.util.LinkedHashSet<>();
        for (Appeal a : byNumber(plan).values()) {
            Appeal.Rows r = a.rows();
            if (r != null) {
                out.addAll(r.stores());
            }
        }
        return out;
    }

    /** The appeals {@code plan} carries, by number, in their numbers' order -- the order their arms were planned, so
     *  the stores they name are provided in that order (the slice 2 audit, 2026-11-08: the harness's provision never
     *  overlays, so its order is the fixtures' order; by verdict id it was the plan's walk's). */
    static Map<Integer, Appeal> byNumber(TestPlan plan) {
        Map<Integer, Appeal> found = new java.util.HashMap<>();
        int last = 0;
        for (List<Extension> at : plan.extensionsByVerdict().values()) {
            for (Extension e : at) {
                if (e instanceof Appeal a) {
                    found.put(a.number(), a);
                    last = Math.max(last, a.number());
                }
            }
        }
        Map<Integer, Appeal> out = new LinkedHashMap<>();
        for (int n = 1; n <= last; n++) {
            Appeal a = found.get(n);
            if (a != null) {
                out.put(n, a);
            }
        }
        return out;
    }

    /** The appeal numbered {@code n} the plan carries: one heard that the plan does not carry is refused by name. */
    static Appeal of(Map<Integer, Appeal> planned, int n) {
        Appeal a = planned.get(n);
        if (a == null) {
            throw new IllegalStateException("an appeal heard (number " + n + ") that the test plan does not carry");
        }
        return a;
    }
}
