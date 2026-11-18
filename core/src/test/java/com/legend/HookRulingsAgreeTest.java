// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE TWO HOOK REGISTRIES AGREE (the GUARD stream, step 2's item 1; the edge audit of 2026-11-17, section C.2:
 * "HOOKS_RULED cross-checked against HookRulingTest.RULED"). {@link EdgeSeparationTest#HOOKS_RULED} takes an entry out of
 * the count because its hook is ruled; {@link HookRulingTest#RULED} is where a ruled hook's four criteria are PROVEN
 * (signatures only, refused by the product by name, implemented in the edge alone, pinned). Until now the two were kept
 * by hand, apart: an entry could leave the count naming a hook nothing proves. Now:
 * <ul>
 *   <li>every HOOKS_RULED entry's hook (its target, or its source where it "stands in a product package") is a hook
 *   {@code RULED} proves, or the entry is one of the {@link #RULED_NOT_HOOKS}, pinned here by the exact entry with the
 *   test that pins it (a new dependency on either class is no such entry);</li>
 *   <li>every HOOKS_RULED entry names its ruling ("<line> HOOK-RULED <date>");</li>
 *   <li>every hook {@code RULED} proves is either a hook HOOKS_RULED takes out of the count, or one of the
 *   {@link #RULED_UNCOUNTED} -- product hooks no map ever counted, pinned here by name.</li>
 * </ul>
 */
@Tag("guardrail")
class HookRulingsAgreeTest {

    /** The HOOKS_RULED entries whose class is not a hook, each by its EXACT entry (the class standing in a product
     *  package, and nothing that depends on it) with its ruling and the test that pins it in place of HookRulingTest's
     *  four criteria. */
    static final Map<String, String> RULED_NOT_HOOKS = Map.of(
            // the closed list of kind types (a final class of one field): HookRulingTest.theFacetRegistryHoldsNothingButItsList
            "com.legend.testplan.PlanStyleFacets -> (stands in a product package)", "C4 HOOK-RULED 2026-11-17, the"
                    + " registry: HookRulingTest.theFacetRegistryHoldsNothingButItsList",
            // the listed facade, forwarding alone: JavaEvalLedgerTest's pin of 189 (193 until the dead defer overload
            // went, 2026-11-17), shrink-only
            "com.legend.testplan.EngineTextPlanning -> (stands in a product package)", "C4 HOOK-RULED 2026-11-17, the"
                    + " listed facade: JavaEvalLedgerTest (EngineTextPlanning, 189)");

    /** Hooks RULED proves that no map ever counted (no product class names them as compatibility): each by name, with
     *  its ruling. */
    static final Map<String, String> RULED_UNCOUNTED = Map.of(
            "com.legend.testexec.EdgeRun", "C2 HOOK-RULED 2026-11-17 (NoEdgeRun, the product's refusal)",
            "com.legend.testexec.EdgeHearing", "C2 HOOK-RULED 2026-11-17 (NoEdgeRun.Hearing, the product's refusal)",
            "com.legend.testplan.TextCollector", "C4 HOOK-RULED 2026-11-17 (handed out by EngineTestText alone)",
            "com.legend.testplan.BatchTexts", "C4 HOOK-RULED 2026-11-17 (handed out by EngineTestText alone)",
            // ruled after this list was first written (2026-11-17, merged from D's d-int3): legend-pure's words at run,
            // the hearing's member (C5 step B), and the ledger's recorder typed (census C1) -- each refused by the product
            "com.legend.testexec.PureWords", "C5 HOOK-RULED 2026-11-17 (C5 step B: NoEdgeRun's hearing refuses it by name)",
            "com.legend.testexecutionplan.Edge", "C1 HOOK-RULED 2026-11-17 (the ledger's recorder: NoEdge refuses each ask)");

    private static final String STANDS = "(stands in a product package)";

    /** The hook an entry takes out of the count: its target, or its source where it stands in a product package. */
    static String hookOf(String entry) {
        String[] p = entry.split(" -> ");
        return p[1].equals(STANDS) ? p[0] : p[1];
    }

    /** What is wrong between {@code hooksRuled} (entry -> ruling) and {@code ruled} (the hooks proven). */
    static List<String> disagreements(Map<String, String> hooksRuled, Set<String> ruled) {
        List<String> out = new ArrayList<>();
        Set<String> counted = new TreeSet<>();
        hooksRuled.forEach((entry, ruling) -> {
            String hook = hookOf(entry);
            counted.add(hook);
            if (!ruled.contains(hook) && !RULED_NOT_HOOKS.containsKey(entry)) {
                out.add(entry + ": " + hook + " is no hook HookRulingTest.RULED proves");
            }
            if (!ruling.matches("[A-Z][0-9]+ HOOK-RULED \\d{4}-\\d{2}-\\d{2}.*")) {
                out.add(entry + ": its ruling '" + ruling + "' names no line, HOOK-RULED and date");
            }
        });
        for (String hook : ruled) {
            if (!counted.contains(hook) && !RULED_UNCOUNTED.containsKey(hook)) {
                out.add(hook + ": proven by HookRulingTest.RULED, but neither HOOKS_RULED's nor pinned as uncounted");
            }
        }
        for (String hook : RULED_UNCOUNTED.keySet()) {
            if (!ruled.contains(hook)) {
                out.add(hook + ": pinned as an uncounted ruled hook, but HookRulingTest.RULED does not prove it");
            }
        }
        return out;
    }

    private static Set<String> ruledHooks() {
        Set<String> out = new TreeSet<>();
        HookRulingTest.RULED.keySet().forEach(c -> out.add(c.getName()));
        return out;
    }

    @Test
    void hooksRuledNamesOnlyProvenHooks() {
        assertEquals(List.of(), disagreements(EdgeSeparationTest.HOOKS_RULED, ruledHooks()),
                "EdgeSeparationTest.HOOKS_RULED and HookRulingTest.RULED disagree");
    }

    /** An entry naming an unproven hook, a new dependency on a pinned non-hook, an entry with no ruling, a proven hook
     *  neither counted nor pinned, and a pinned uncounted hook RULED no longer proves are each found. */
    @Test
    void theCrossCheckBites() {
        Map<String, String> planted = new LinkedHashMap<>(EdgeSeparationTest.HOOKS_RULED);
        planted.put("com.legend.lowering.Scalars -> com.legend.lowering.Repr", "C5 HOOK-RULED 2026-11-17");
        planted.put("com.legend.lowering.Lowerer -> com.legend.testplan.EngineTextPlanning", "C4 HOOK-RULED 2026-11-17");
        planted.put("com.legend.lowering.Lowerer -> com.legend.lowering.PureOnlyTexts", "ruled, trust me");
        Set<String> ruled = new TreeSet<>(ruledHooks());
        ruled.add("com.legend.planner.PlantedHook");
        ruled.remove("com.legend.testexec.EdgeRun");
        List<String> found = disagreements(planted, ruled);
        for (String want : List.of("com.legend.lowering.Scalars -> com.legend.lowering.Repr: com.legend.lowering.Repr is"
                + " no hook", "com.legend.lowering.Lowerer -> com.legend.testplan.EngineTextPlanning: com.legend"
                + ".testplan.EngineTextPlanning is no hook", "com.legend.lowering.Lowerer -> com.legend.lowering"
                + ".PureOnlyTexts: its ruling", "com.legend.planner.PlantedHook: proven by HookRulingTest.RULED",
                "com.legend.testexec.EdgeRun: pinned as an uncounted ruled hook")) {
            assertTrue(found.stream().anyMatch(f -> f.startsWith(want)), "the cross-check bites: " + want + " in "
                    + found);
        }
        assertEquals(5, found.size(), "exactly the five planted disagreements: " + found);
    }
}
