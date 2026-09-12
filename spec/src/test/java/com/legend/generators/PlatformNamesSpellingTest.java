// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.rcorpus.Corpus;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Every distinguished {@code meta::…} spelling in {@code PlatformTypes} is an
 * upstream DECLARATION — a class, enum, profile, association, measure or
 * function the pinned checkouts declare (or a package that holds one), or one
 * of the platform's own system-metamodel elements. The constant's NAME is ours;
 * its VALUE is upstream's, and this is the whole-set parity for it (the old
 * {@code PlatformTypesDriftTest} pinned 6 of 127 by containment in our own
 * prelude — batch 5 audit, 2026-09-11). There is nothing to GENERATE here:
 * the spelling IS the FQN, so existence in the spec is the only derivable fact.
 */
class PlatformNamesSpellingTest {

    private static final Pattern CONSTANT = Pattern.compile("\"(meta::[A-Za-z0-9_:]+)\"");
    private static final Pattern DECL = Pattern.compile(
            "^\\s*(?:native\\s+function|function|Class|Enum|Profile|Association|Measure|Primitive)\\s+"
            + "(?:<<[^>]*>>\\s*)?(?:\\{[^}]*\\}\\s*)?(meta::[A-Za-z0-9_:]+)\\b", Pattern.MULTILINE);
    /** m3.pure's BOOTSTRAP declarations are M3 instances:
     *  {@code ^Root.children[…].children[Class] Any @Root.children[meta].children[pure]…} —
     *  the name after the instance's classifier, the package from the {@code @Root} path. */
    private static final Pattern M3_INSTANCE = Pattern.compile(
            "^\\^Root(?:\\.children\\[\\w+\\])+\\.children\\[(\\w+)\\] (\\w+)(?: @Root((?:\\.children\\[\\w+\\])+)\\.children)?\\s*$", Pattern.MULTILINE);
    private static final Pattern M3_SEGMENT = Pattern.compile("children\\[(\\w+)\\]");

    static Set<String> declaredIn(List<Path> roots) throws IOException {
        Set<String> out = new HashSet<>();
        for (Path root : roots) {
            try (Stream<Path> walk = Files.walk(root)) {
                for (Path p : walk.filter(x -> x.toString().endsWith(".pure")).toList()) {
                    String text = Files.readString(p, StandardCharsets.UTF_8);
                    Matcher m = DECL.matcher(text);
                    while (m.find()) {
                        out.add(m.group(1));
                    }
                    Matcher inst = M3_INSTANCE.matcher(text);
                    while (inst.find()) {
                        String kind = inst.group(1);
                        String name = inst.group(2);
                        String at = inst.group(3);
                        StringBuilder pkg = new StringBuilder();
                        if (at == null) {
                            // m3.pure's UN-ANNOTATED bootstrap declarations — the
                            // Package class and the primitive types — sit at
                            // upstream's ROOT (M3Paths.Package = "Package"); the
                            // prelude generator RE-HOMES them under
                            // meta::pure::metamodel (Package) / ::type (primitives),
                            // the platform's canonical spelling for the whole
                            // catalog. The same rule applies here, so the
                            // constant is held against the generator's spelling
                            // (docs/BATCH_1_5_AUDIT_2026_09_11.md, finding 5.4).
                            pkg.append("Class".equals(kind) ? "meta::pure::metamodel" : "meta::pure::metamodel::type");
                        } else {
                            Matcher seg = M3_SEGMENT.matcher(at);
                            while (seg.find()) {
                                pkg.append(pkg.length() == 0 ? "" : "::").append(seg.group(1));
                            }
                        }
                        out.add(pkg + "::" + name);
                    }
                }
            }
        }
        return out;
    }

    @Test
    @DisplayName("every PlatformTypes spelling is declared upstream (or is a system-metamodel element)")
    void everySpellingIsDeclared() throws IOException {
        Path engine = Corpus.ENGINE_ROOT;
        Path pure = PreludeGeneratorTest.pureRoot();
        Assumptions.assumeTrue(Files.isDirectory(engine), "legend-engine checkout not present");
        Assumptions.assumeTrue(Files.isDirectory(pure), "legend-pure checkout not present");
        List<Path> roots = new ArrayList<>();
        roots.add(pure);
        for (String r : PreludeGeneratorTest.ENGINE_SPEC_ROOTS) {
            roots.add(engine.resolve(r));
        }
        Set<String> declared = declaredIn(roots);
        declared.addAll(com.legend.builtin.SystemMetamodel.elementFqns());
        String src = Files.readString(CoreTree.main("com/legend/compiler/element/type/PlatformTypes.java"),
                StandardCharsets.UTF_8);
        Matcher m = CONSTANT.matcher(src);
        List<String> missing = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        int count = 0;
        while (m.find()) {
            String fqn = m.group(1);
            if (!seen.add(fqn)) {
                continue;
            }
            count++;
            boolean ok = fqn.endsWith("::")
                    ? declared.stream().anyMatch(d -> d.startsWith(fqn))
                    : declared.contains(fqn) || declared.stream().anyMatch(d -> d.startsWith(fqn + "::"));
            if (!ok) {
                missing.add(fqn);
            }
        }
        assertEquals(List.of(), missing, "PlatformTypes spells " + missing.size() + " of " + count
                + " names the pinned checkouts do not declare");
    }
}
