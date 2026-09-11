package com.legend.tools;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.builtin.NativeFn;
import com.legend.builtin.Pure;
import com.legend.builtin.Subsumed;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The {@link Subsumed} contract, pinned: every subsumed engine program is
 * NOT declared by the platform, its VALUE is dead (no main-tree code names
 * the FQN), and its upstream body is CITED and present at the pinned
 * checkout. The count is a shrink-only ratchet.
 */
class SubsumedRegistryTest {

    /** Shrink-only. MEASURED 2026-09-10: createDbConfig (the batch-4b census;
     *  USER: a stub that types and is never consumed is not an implementation). */
    static final int SUBSUMED_MAX = 2;

    static final Path MAIN = CoreTree.CORE.resolve("src/main/java");

    @Test
    @DisplayName("subsumed programs: not declared here, value dead, body cited — count shrink-only")
    void contract() throws IOException {
        List<String> problems = new ArrayList<>();
        for (Subsumed s : Subsumed.values()) {
            // 1. not declared here
            if (!Pure.nativeFunctionsAt(s.fqn()).isEmpty()) {
                problems.add(s + ": Pure.java declares " + s.fqn());
            }
            for (var fam : NativeFn.families().entrySet()) {
                for (NativeFn.Member m : fam.getValue()) {
                    if (m.fqn().equals(s.fqn())) {
                        problems.add(s + ": NativeFn." + fam.getKey() + " registers " + s.fqn());
                    }
                }
            }
            if (com.legend.compiler.element.type.PlatformTypes.isPlatformOwnedFunction(s.fqn())) {
                problems.add(s + ": platform-owned name — the corpus definition must be the typing source");
            }
            // 2. dead value: no main-tree source names the FQN outside Subsumed.java
            try (Stream<Path> walk = Files.walk(MAIN)) {
                for (Path f : walk.filter(p -> p.toString().endsWith(".java")).sorted().toList()) {
                    if (f.endsWith("Subsumed.java")) {
                        continue;
                    }
                    String text = Files.readString(f, StandardCharsets.UTF_8);
                    if (text.contains(s.fqn())) {
                        problems.add(s + ": " + f + " names " + s.fqn() + " (a consumer of a dead value?)");
                    }
                }
            }
            // 3. cited: the body is where the citation says, at the pinned checkout
            Path file = PreludeGeneratorTest.engineRoot().resolve(s.engineFile());
            if (!Files.isRegularFile(file)) {
                problems.add(s + ": cited file missing at the pinned engine checkout: " + file);
                continue;
            }
            List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            boolean declared = false;
            for (int i = s.fromLine(); i <= s.toLine() && i <= lines.size(); i++) {
                if (lines.get(i - 1).contains("function " + s.fqn() + "(")) {
                    declared = true;
                }
            }
            if (!declared) {
                problems.add(s + ": " + s.engineFile() + ":" + s.fromLine() + "-" + s.toLine()
                        + " does not declare " + s.fqn());
            }
        }
        assertTrue(problems.isEmpty(), () -> String.join("\n", problems));
        System.out.println("[subsumed] " + Subsumed.values().length + " engine programs (pin " + SUBSUMED_MAX + ")");
        assertTrue(Subsumed.values().length <= SUBSUMED_MAX,
                "Subsumed grew past its pin " + SUBSUMED_MAX + " — a new member is a decision with a written reason");
    }
}
