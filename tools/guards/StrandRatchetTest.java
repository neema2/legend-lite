// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.guards;

import com.legend.testing.Runfile;
import com.legend.tools.meaning.ClassFiles;
import com.legend.tools.meaning.ClassFiles.ClassFile;
import com.legend.tools.meaning.StrandRatchet;

import org.junit.jupiter.api.Test;

import java.lang.constant.ClassDesc;
import java.lang.constant.ConstantDescs;
import java.lang.constant.MethodTypeDesc;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE STRAND RATCHET, A PROTOTYPE (B5 of the compatibility program; designed by census B2, 2026-11-19; NOT TURNED ON:
 * it lands with stage B, on the user's word). The reviewer's ruling: the ratchet is the BYTECODE count of the
 * compatibility strands' reach into the core -- PlanStyle's parameters, fields and calls (49 parameters in 19 classes,
 * 4 fields, 50 calls at main b75d6c73a), with LoweringForms, PlanStyleFacets and upstream's Feature flags beside it --
 * not ModeBranchCensusTest. {@link StrandRatchet} measures; this test holds each measure to its pin.
 *
 * <p>SHRINK-ONLY, AND EXACT. A measure above its pin is the strand growing: refused, naming what grew. A measure below
 * it is progress: refused too, until the pin is lowered to the measure with a dated note, so the gain is locked in and
 * cannot be spent later. A pin moves only down and only with a dated justification (AGENTS.md); {@code PINS} is a
 * constant {@code tools/guards/shrink_only.py} can watch.
 *
 * <p>Until {@link #TURNED_ON}, the measure is REPORTED, never failed on. What does fail, on or off, is the ratchet going
 * blind: a planted class with a new PlanStyle parameter and a new PlanStyle call must be seen
 * ({@link #itBitesAPlantedParameterAndACall}).
 */
class StrandRatchetTest {

    /** Whether a breach fails the build: false until stage B lands, on the user's word. */
    static final boolean TURNED_ON = false;

    /** Each measure's pin, at main b75d6c73a (2026-11-19, census B2's H8 (a.1) and (a.3), the starting counts). */
    static final Map<String, Integer> PINS = Map.ofEntries(
            // PlanStyle: the plan style's reach (H8 (a.1), 2026-11-19); it ends at 0 when PlanStyle dies (slice 4.1)
            Map.entry("PlanStyle.parameters", 49),
            Map.entry("PlanStyle.fields", 4),
            Map.entry("PlanStyle.calls", 50),
            Map.entry("PlanStyle.carrying", 112),
            Map.entry("PlanStyle.classes", 36),
            // LoweringForms: the lowering's two-form choices (H8 (a.1), 2026-11-19)
            Map.entry("LoweringForms.parameters", 6),
            Map.entry("LoweringForms.fields", 1),
            Map.entry("LoweringForms.calls", 25),
            Map.entry("LoweringForms.carrying", 11),
            Map.entry("LoweringForms.classes", 10),
            // PlanStyleFacets: the closed lists (5 facet kinds, 16 two-forms) and the keyed hooks' call sites
            // (2026-11-19)
            Map.entry("PlanStyleFacets.kinds", 21),
            Map.entry("PlanStyleFacets.reads", 0),
            Map.entry("PlanStyleFacets.parameters", 0),
            Map.entry("PlanStyleFacets.fields", 0),
            Map.entry("PlanStyleFacets.calls", 0),
            Map.entry("PlanStyleFacets.carrying", 0),
            Map.entry("PlanStyleFacets.classes", 0),
            Map.entry("hooks.facet", 16),
            Map.entry("hooks.twoForm", 16),
            // upstream's Feature flags in the core (H8 (a.3), the independent review's strand, 2026-11-19)
            Map.entry("Feature.classes", 20),
            Map.entry("Feature.signatures", 62),
            Map.entry("Feature.loads", 4),
            Map.entry("Feature.flagReads", 12),
            Map.entry("Feature.parameters", 1),
            Map.entry("Feature.fields", 0),
            Map.entry("Feature.calls", 1),
            Map.entry("Feature.carrying", 1));

    /** The breaches of {@code measured} against {@link #PINS}: a measure above its pin (it grew), below it (lower the
     *  pin), or unpinned (a measure the ratchet does not hold yet). */
    static List<String> breaches(Map<String, StrandRatchet.Count> measured) {
        List<String> out = new ArrayList<>();
        measured.forEach((k, v) -> {
            Integer pin = PINS.get(k);
            if (pin == null) {
                out.add(k + ": unpinned, measured " + v.n());
            } else if (v.n() > pin) {
                out.add(k + ": GREW " + pin + " -> " + v.n() + ": " + v.where());
            } else if (v.n() < pin) {
                out.add(k + ": shrank " + pin + " -> " + v.n() + ": lower the pin with a dated note");
            }
        });
        PINS.keySet().stream().filter(k -> !measured.containsKey(k)).forEach(k -> out.add(k + ": pinned, not measured"));
        return out;
    }

    private static Map<String, ClassFile> product() throws Exception {
        return StrandRatchet.product(ClassFiles.ofJars(Runfile.listed("meaning.product.jars")));
    }

    @Test
    void theStrandsReachNoFurtherThanTheirPins() throws Exception {
        long t0 = System.nanoTime();
        Map<String, ClassFile> product = product();
        long read = System.nanoTime();
        Map<String, StrandRatchet.Count> measured = StrandRatchet.measure(product);
        long done = System.nanoTime();
        measured.forEach((k, v) -> System.out.println("[strand-ratchet] " + k + "\t" + v.n() + "\t(pin " + PINS.get(k) + ")"));
        System.out.println("[strand-ratchet] " + product.size() + " product classes; read " + (read - t0) / 1_000_000
                + " ms, measured " + (done - read) / 1_000_000 + " ms");
        List<String> breaches = breaches(measured);
        breaches.forEach(b -> System.out.println("[strand-ratchet] BREACH " + b));
        if (TURNED_ON) {
            assertEquals(List.of(), breaches, "the compatibility strands' reach into the core moved");
        } else {
            System.out.println("[strand-ratchet] REPORT-ONLY (TURNED_ON = false): " + breaches.size() + " breach(es)");
        }
    }

    /** A class the product never had, planted beside it: a static method taking a PlanStyle (a new parameter) and a
     *  LoweringForms (another), calling PlanStyle.judgesEngineText (a new call) and reading a Feature member. */
    static byte[] plant() {
        ClassDesc style = ClassDesc.ofInternalName("com/legend/database/PlanStyle");
        ClassDesc forms = ClassDesc.ofInternalName("com/legend/lowering/LoweringForms");
        ClassDesc feature = ClassDesc.ofInternalName("com/legend/platform/Feature");
        return java.lang.classfile.ClassFile.of().build(ClassDesc.of("com.legend.planted.ratchet.PlantedStrandUser"),
                cb -> cb.withFlags(java.lang.classfile.ClassFile.ACC_PUBLIC | java.lang.classfile.ClassFile.ACC_FINAL)
                        .withMethodBody("use", MethodTypeDesc.of(ConstantDescs.CD_void, style, forms),
                                java.lang.classfile.ClassFile.ACC_PUBLIC | java.lang.classfile.ClassFile.ACC_STATIC,
                                code -> code
                                        .aload(0)
                                        .invokeinterface(style, "judgesEngineText",
                                                MethodTypeDesc.of(ConstantDescs.CD_boolean))
                                        .pop()
                                        .getstatic(feature, "LEGACY_SQL_NULL_UNSAFE_EQUALS", feature)
                                        .pop()
                                        .return_()));
    }

    @Test
    void itBitesAPlantedParameterAndACall() throws Exception {
        Map<String, ClassFile> planted = new TreeMap<>(product());
        ClassFile c = ClassFiles.read(plant());
        planted.put(c.name(), c);
        List<String> breaches = breaches(StrandRatchet.measure(planted));
        breaches.forEach(b -> System.out.println("[strand-ratchet] planted BREACH " + b));
        Map<String, Integer> pins = PINS;
        for (String grew : List.of(
                "PlanStyle.parameters: GREW " + pins.get("PlanStyle.parameters") + " -> " + (pins.get("PlanStyle.parameters") + 1),
                "PlanStyle.calls: GREW " + pins.get("PlanStyle.calls") + " -> " + (pins.get("PlanStyle.calls") + 1),
                "PlanStyle.classes: GREW " + pins.get("PlanStyle.classes") + " -> " + (pins.get("PlanStyle.classes") + 1),
                "LoweringForms.parameters: GREW " + pins.get("LoweringForms.parameters") + " -> " + (pins.get("LoweringForms.parameters") + 1),
                "Feature.loads: GREW " + pins.get("Feature.loads") + " -> " + (pins.get("Feature.loads") + 1))) {
            assertTrue(breaches.stream().anyMatch(b -> b.startsWith(grew)), "the ratchet must see the plant: " + grew
                    + "\n  breaches: " + breaches);
        }
        assertTrue(breaches.stream().anyMatch(b -> b.contains("com/legend/planted/ratchet/PlantedStrandUser")),
                "a breach names what grew");
    }
}
