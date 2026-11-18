// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tngtech.archunit.core.domain.JavaCall;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaCodeUnit;
import com.tngtech.archunit.core.importer.ClassFileImporter;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * G6 OF THE PRESENTATION PROGRAM, REPORT-ONLY (S0, 2026-11-18, docs/records/presentation-program-design-2026-11-18.md
 * §6): no code may branch on which mode a plan is in outside the mode records. Until S1m makes the modes data, this
 * census counts today's branches on "which edge style" (§2.7) by their call sites in the product's and the edge's
 * jars, and writes them to {@code mode_branch_census.txt} in the test's outputs: {@code PureText.isEdgeStyle},
 * {@code PlanStyle.judgesEngineText}, {@code PureText.recording}'s template check, and the {@code
 * Boolean.getBoolean} switches (the engine scan order's among them). It fails only when it cannot see a planted
 * branch; it blocks from S7, when every branch is gone.
 */
@Tag("guardrail")
class ModeBranchCensusTest {

    /** Today's branches, by the method each call site calls (owner and name). */
    static final List<String> BRANCHES = List.of(
            "com.legend.edge.PureText.isEdgeStyle",
            "com.legend.database.PlanStyle.judgesEngineText",
            "com.legend.edge.PureText.recording",
            "java.lang.Boolean.getBoolean");

    /** Each branch's call sites ({@code Class.method(line)}) in {@code classes}, the planted class's left out. */
    static Map<String, List<String>> sites(JavaClasses classes) {
        Map<String, List<String>> out = new TreeMap<>();
        for (String b : BRANCHES) {
            out.put(b, new ArrayList<>());
        }
        for (JavaClass c : classes) {
            for (JavaCodeUnit u : c.getCodeUnits()) {
                for (JavaCall<?> call : u.getCallsFromSelf()) {
                    List<String> at = out.get(call.getTargetOwner().getName() + "." + call.getName());
                    if (at != null) {
                        at.add(u.getOwner().getName() + "." + u.getName() + "(" + call.getLineNumber() + ")");
                    }
                }
            }
        }
        return out;
    }

    private static JavaClasses productAndEdge() {
        List<java.util.jar.JarFile> jars = new ArrayList<>();
        try {
            for (String property : List.of("legend.product.jars", "legend.edge.jars")) {
                for (Path jar : com.legend.testing.Runfile.listed(property)) {
                    jars.add(new java.util.jar.JarFile(jar.toFile()));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new ClassFileImporter().importJars(jars);
    }

    @Test
    void todaysBranchesAreCounted() throws IOException {
        Map<String, List<String>> sites = sites(productAndEdge());
        StringBuilder report = new StringBuilder("# G6 (report-only, S0): today's branches on which edge style, by call"
                + " site\n");
        int total = 0;
        for (Map.Entry<String, List<String>> e : sites.entrySet()) {
            report.append(e.getKey()).append('\t').append(e.getValue().size()).append('\n');
            for (String site : e.getValue()) {
                report.append("  ").append(site).append('\n');
            }
            total += e.getValue().size();
        }
        report.append("total\t").append(total).append('\n');
        System.out.println("[G6] " + total + " call sites of today's mode branches (report-only until S7)");
        String dir = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        if (dir != null) {
            Files.writeString(Path.of(dir, "mode_branch_census.txt"), report, StandardCharsets.UTF_8);
        }
        assertFalse(sites.get("com.legend.edge.PureText.isEdgeStyle").isEmpty(),
                "the census sees today's isEdgeStyle callers: a census that sees none"
                        + " reads the wrong jars");
    }

    /** A planted branch: the census must see it. */
    static final class PlantedBranch {
        static boolean branches(com.legend.database.PlanStyle style) {
            return com.legend.edge.PureText.isEdgeStyle(style);
        }
    }

    @Test
    void aPlantedBranchIsSeen() {
        Map<String, List<String>> sites = sites(new ClassFileImporter().importClasses(PlantedBranch.class));
        assertTrue(sites.get("com.legend.edge.PureText.isEdgeStyle").stream().anyMatch(s -> s.contains("PlantedBranch")),
                sites.toString());
    }
}
