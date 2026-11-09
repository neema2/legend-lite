// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE APPEALS A TEST PLAN NAMES AND CARRIES, READ FROM THE PLAN ALONE (the slice 2 invariant audit, 2026-11-08: the
 * run's reads and the self-containment check, moved from the test planner's {@code AppealsByVerdict}): the numbers its
 * verdicts name, the appeals it carries by number, the stores they name, and the check that every number named is
 * carried.
 */
public final class PlannedAppeals {

    private PlannedAppeals() {
    }

    /** The appeal numbers {@code plan}'s verdicts name, by verdict id: an engine text's (through the wrappers and parts
     *  that hold one) and a database-judged root loop's rows. */
    public static Map<Integer, List<Integer>> named(TestPlan plan) {
        Map<Integer, List<Integer>> out = new LinkedHashMap<>();
        walk(plan.steps(), out);
        return out;
    }

    /** {@code plan} is self-contained: every appeal its verdicts name, it carries -- refused by name otherwise. */
    public static void requireCarried(TestPlan plan) {
        Map<Integer, Appeal> carried = byNumber(plan);
        named(plan).forEach((verdict, numbers) -> {
            for (int n : numbers) {
                if (!carried.containsKey(n)) {
                    throw new IllegalStateException("a test plan whose verdict " + verdict + " names appeal " + n
                            + ", which the plan does not carry");
                }
            }
        });
    }

    private static void walk(List<? extends TestPlan.TestStep> steps, Map<Integer, List<Integer>> out) {
        for (TestPlan.TestStep step : steps) {
            switch (step) {
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
            case HostJudgment.EngineText e -> {
                // an engine text compared under lite's own rules names no appeal (nothing hears one)
                if (e.appeal() != RootLoop.NO_APPEAL) {
                    out.add(e.appeal());
                }
            }
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
    public static java.util.Set<String> stores(TestPlan plan) {
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
    public static Map<Integer, Appeal> byNumber(TestPlan plan) {
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
    public static Appeal of(Map<Integer, Appeal> planned, int n) {
        Appeal a = planned.get(n);
        if (a == null) {
            throw new IllegalStateException("an appeal heard (number " + n + ") that the test plan does not carry");
        }
        return a;
    }
}
