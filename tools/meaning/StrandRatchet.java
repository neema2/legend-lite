// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.meaning;

import com.legend.tools.meaning.ClassFiles.ClassFile;
import com.legend.tools.meaning.ClassFiles.Insn;
import com.legend.tools.meaning.ClassFiles.Method;
import com.legend.tools.meaning.ClassFiles.Ref;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * THE STRAND RATCHET'S MEASURE (B5 of the compatibility program, designed and prototyped by census B2, 2026-11-19; not
 * turned on): how far the compatibility strands' hook types reach into the CORE, read from its bytecode. Its numbers
 * are H8 (a)'s (docs/records/compat-homework/H8-phase0-measures.md §(a.1), §(a.3)), so the ratchet starts where 0.4
 * measured it and ends at 0 when PlanStyle dies (slice 4.1).
 *
 * <p>The scope: every class of {@code //core:product_jars} outside {@code com/legend/edge/} (the edge is the strands'
 * home; the ratchet counts their reach into the product), and outside the hook type's own nest.
 *
 * <p>The measures, for each hook type T ({@link #TYPES}):
 * <ul>
 *   <li>{@code T.parameters}: parameters of type T on declared methods and constructors (bridges and synthetic
 *       methods, lambdas' bodies among them, left out);</li>
 *   <li>{@code T.fields}: fields of type T (a record's component is a field too);</li>
 *   <li>{@code T.calls}: invoke instructions whose owner is T, and method references to T's methods (a MethodHandle
 *       constant);</li>
 *   <li>{@code T.carrying}: invoke instructions whose target takes a T parameter (the hook passed along);</li>
 *   <li>{@code T.classes}: classes naming T anywhere in their constant pool.</li>
 * </ul>
 * Feature is an enum carried in {@code Set<Feature>}, which erasure hides from descriptors, so it is measured by its
 * own facts: {@code Feature.classes}, {@code Feature.signatures} (generic signature strings naming it, per class),
 * {@code Feature.loads} (getstatic of a member) and {@code Feature.flagReads} (reads of the Lowerer's two flag fields).
 * PlanStyleFacets is a pair of closed lists, so it is measured as {@code PlanStyleFacets.kinds} (the class constants
 * its initializer loads: KINDS and TWO_FORMS together) and {@code PlanStyleFacets.reads} (reads of either list);
 * and the style's keyed hooks as {@code hooks.facet} and {@code hooks.twoForm}, their call sites in the product.
 */
public final class StrandRatchet {

    /** The hook types, by internal name, and the measure's name for each. */
    public static final Map<String, String> TYPES = Map.of(
            "com/legend/database/PlanStyle", "PlanStyle",
            "com/legend/lowering/LoweringForms", "LoweringForms",
            "com/legend/testplan/PlanStyleFacets", "PlanStyleFacets",
            "com/legend/platform/Feature", "Feature");

    private static final String FEATURE = "com/legend/platform/Feature";
    private static final String FACETS = "com/legend/testplan/PlanStyleFacets";
    private static final String STYLE = "com/legend/database/PlanStyle";

    private StrandRatchet() {
    }

    /** One measure's count, and where it was counted (a class per hit, for the message that names what grew). */
    public record Count(int n, List<String> where) {
    }

    /** The product's classes: {@code all} without the edge. */
    public static Map<String, ClassFile> product(Map<String, ClassFile> all) {
        Map<String, ClassFile> out = new TreeMap<>();
        all.forEach((n, c) -> {
            if (!n.startsWith("com/legend/edge/")) {
                out.put(n, c);
            }
        });
        return out;
    }

    /** Every measure over {@code product}, in a stable order. */
    public static Map<String, Count> measure(Map<String, ClassFile> product) {
        Map<String, List<String>> hits = new TreeMap<>();
        for (String t : List.of(STYLE, "com/legend/lowering/LoweringForms", FACETS, FEATURE)) {
            String name = TYPES.get(t);
            for (String m : List.of("parameters", "fields", "calls", "carrying", "classes")) {
                hits.put(name + "." + m, new ArrayList<>());
            }
        }
        for (String m : List.of("Feature.signatures", "Feature.loads", "Feature.flagReads", "PlanStyleFacets.kinds",
                "PlanStyleFacets.reads", "hooks.facet", "hooks.twoForm")) {
            hits.put(m, new ArrayList<>());
        }
        for (ClassFile c : product.values()) {
            for (Map.Entry<String, String> e : TYPES.entrySet()) {
                String t = e.getKey();
                String name = e.getValue();
                String desc = "L" + t + ";";
                if (c.name().equals(t) || c.name().startsWith(t + "$")) {
                    continue;
                }
                if (c.utf8s().stream().anyMatch(s -> s.contains(desc) || s.equals(t))) {
                    hits.get(name + ".classes").add(c.name());
                }
                c.fields().stream().filter(f -> f.descriptor().equals(desc))
                        .forEach(f -> hits.get(name + ".fields").add(c.name() + "." + f.name()));
                for (ClassFiles.Constant k : c.pool()) {
                    if (k != null && k.tag() == 15 && c.ref(k.b()).owner().equals(t)) {
                        hits.get(name + ".calls").add(c.name() + " ::" + c.ref(k.b()).name());
                    }
                }
                for (Method m : c.methods()) {
                    if (!m.isSynthetic() && !m.isBridge()) {
                        for (String p : ClassFiles.params(m.descriptor())) {
                            if (p.equals(desc)) {
                                hits.get(name + ".parameters").add(c.name() + "#" + m.name());
                            }
                        }
                    }
                    for (Insn in : m.code()) {
                        if (in.op() >= 0xb6 && in.op() <= 0xb9) {
                            Ref r = c.ref(in.operand());
                            if (r.owner().equals(t)) {
                                hits.get(name + ".calls").add(c.name() + "#" + m.name() + ":" + m.lineAt(in.pc()));
                            }
                            if (ClassFiles.params(r.descriptor()).contains(desc)) {
                                hits.get(name + ".carrying").add(c.name() + "#" + m.name() + ":" + m.lineAt(in.pc()));
                            }
                        }
                    }
                }
            }
            // Feature's own facts
            for (String s : new TreeSet<>(c.utf8s())) {
                if (!c.name().startsWith(FEATURE) && (s.contains("<L" + FEATURE + ";>") || s.contains("<+L" + FEATURE + ";>"))) {
                    hits.get("Feature.signatures").add(c.name());
                }
            }
            for (Method m : c.methods()) {
                List<Insn> code = m.code();
                for (int i = 0; i < code.size(); i++) {
                    Insn in = code.get(i);
                    if (in.op() == 0xb2) {
                        Ref r = c.ref(in.operand());
                        if (r.owner().equals(FEATURE) && !c.name().startsWith(FEATURE)) {
                            hits.get("Feature.loads").add(c.name() + "#" + m.name() + ":" + m.lineAt(in.pc()) + " " + r.name());
                        }
                        if (r.owner().equals(FACETS) && !c.name().startsWith(FACETS)) {
                            hits.get("PlanStyleFacets.reads").add(c.name() + "#" + m.name() + ":" + m.lineAt(in.pc()));
                        }
                    }
                    if (in.op() == 0xb4) {
                        Ref r = c.ref(in.operand());
                        if (r.owner().equals("com/legend/lowering/Lowerer")
                                && (r.name().equals("legacyNullUnsafeEquals") || r.name().equals("features"))) {
                            hits.get("Feature.flagReads").add(c.name() + "#" + m.name() + ":" + m.lineAt(in.pc()));
                        }
                    }
                    if ((in.op() == 0x12 || in.op() == 0x13) && c.name().equals(FACETS) && m.name().equals("<clinit>")) {
                        ClassFiles.Constant k = c.pool()[in.operand()];
                        if (k != null && k.tag() == 7) {
                            hits.get("PlanStyleFacets.kinds").add(c.className(in.operand()));
                        }
                    }
                    if ((in.op() == 0xb6 || in.op() == 0xb9) && !c.name().startsWith(STYLE)) {
                        Ref r = c.ref(in.operand());
                        if (r.owner().equals(STYLE) && (r.name().equals("facet") || r.name().equals("twoForm"))) {
                            hits.get("hooks." + r.name()).add(c.name() + "#" + m.name() + ":" + m.lineAt(in.pc())
                                    + " " + keyBefore(c, code, i));
                        }
                    }
                }
            }
        }
        Map<String, Count> out = new LinkedHashMap<>();
        hits.forEach((k, v) -> out.put(k, new Count(v.size(), List.copyOf(v))));
        return out;
    }

    /** The class constant loaded by the nearest ldc before instruction {@code i} (within 12 instructions), or "?". */
    private static String keyBefore(ClassFile c, List<Insn> code, int i) {
        for (int j = i - 1; j >= 0 && j >= i - 12; j--) {
            Insn in = code.get(j);
            if (in.op() == 0x12 || in.op() == 0x13) {
                ClassFiles.Constant k = c.pool()[in.operand()];
                if (k != null && k.tag() == 7) {
                    String n = c.className(in.operand());
                    return n.substring(n.lastIndexOf('/') + 1);
                }
            }
        }
        return "?";
    }

    /** Standalone: {@code StrandRatchet <jar list> [--where]}, a list of rlocation-style jar paths under bazel-bin. */
    public static void main(String[] args) throws Exception {
        List<Path> jars = new ArrayList<>();
        for (String l : Files.readAllLines(Path.of(args[0]))) {
            if (!l.isBlank()) {
                jars.add(Path.of("bazel-bin", l.strip().replaceFirst("^_main/", "")));
            }
        }
        boolean where = args.length > 1 && args[1].equals("--where");
        measure(product(ClassFiles.ofJars(jars))).forEach((k, v) -> {
            System.out.println(k + "\t" + v.n());
            if (where) {
                v.where().forEach(w -> System.out.println("    " + w));
            }
        });
    }
}
