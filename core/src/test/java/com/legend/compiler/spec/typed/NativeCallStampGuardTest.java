// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec.typed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.SourceFiles;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * A PASS THAT REBUILDS A NATIVE CALL KEEPS ITS STAMP (the edition program's step 2, 2026-11-26;
 * docs/records/edition-plan-to-finish-stage-b-2026-11-25.md section 2.3). {@code TypedNativeCall.outsideStore} is a field
 * of the call so that it rides along when a pass rebuilds the call; a pass that builds the call again by hand must pass
 * the stamp of the call it was given ({@code x.outsideStore()}, the constructor's fifth argument), or the lowering's
 * edition table reads the wrong column for it.
 *
 * <p>The guard reads every {@code new TypedNativeCall(...)} in core's main sources. One whose statement names
 * {@code outsideStore} carries a stamp. Every other is counted per file and pinned: when the pins were measured
 * (2026-11-26, with the independent audit's scan, {@code docs/records/edition-homework/r3/audit-step2-3.md}) each was a
 * NEW call a pass makes, never a copy of one it was given. A file's count may fall freely. A count that rises is a new
 * construction: if it rebuilds a call it was given, pass the stamp and the count does not move; if it makes a new call,
 * move the pin with a dated note saying so.
 */
@Tag("guardrail")
class NativeCallStampGuardTest {

    private static final Pattern CONSTRUCTION =
            Pattern.compile("new\\s+(?:com\\.legend\\.compiler\\.spec\\.typed\\.)?TypedNativeCall\\(");

    /** How many lines after a construction's own its statement is read through. */
    private static final int STATEMENT_LINES = 11;

    /** File (under core's main sources) to its constructions that carry no stamp, measured 2026-11-26. */
    private static final Map<String, Integer> NEW_CALLS = Map.ofEntries(
            Map.entry("compiler/spec/AssertMessages.java", 3),
            Map.entry("compiler/spec/CallNodes.java", 1),
            Map.entry("compiler/spec/GeneratorSplice.java", 1),
            Map.entry("compiler/spec/LambdaBodies.java", 2),
            Map.entry("compiler/spec/LineageTreeLines.java", 1),
            Map.entry("compiler/spec/LiteralUnroll.java", 1),
            Map.entry("compiler/spec/MatchChecker.java", 2),
            Map.entry("compiler/spec/NavigateChecker.java", 1),
            Map.entry("compiler/spec/ReadChecker.java", 1),
            Map.entry("compiler/spec/TdsDesugars.java", 1),
            Map.entry("compiler/spec/Typer.java", 1),
            Map.entry("compiler/spec/UserCallInliner.java", 3),
            Map.entry("compiler/spec/VerdictQueries.java", 7),
            Map.entry("compiler/spec/typed/TypedNativeCall.java", 1),
            Map.entry("edge/EngineSubstitutes.java", 1),
            Map.entry("resolver/AssociationJoins.java", 4),
            Map.entry("resolver/CastReRoot.java", 2),
            Map.entry("resolver/ChainDispatch.java", 6),
            Map.entry("resolver/ChainNormalizer.java", 3),
            Map.entry("resolver/ClassSources.java", 2),
            Map.entry("resolver/ClassWrites.java", 2),
            Map.entry("resolver/CorrelatedSubselects.java", 1),
            Map.entry("resolver/ElementReferences.java", 1),
            Map.entry("resolver/FlattenOps.java", 1),
            Map.entry("resolver/GraphEmission.java", 3),
            Map.entry("resolver/ImportDataFlowAppend.java", 1),
            Map.entry("resolver/JsonSourceFrame.java", 2),
            Map.entry("resolver/ObjectReferenceArms.java", 2),
            Map.entry("resolver/ObjectReferenceDecode.java", 1),
            Map.entry("resolver/Pipelines.java", 1),
            Map.entry("resolver/StackBuilder.java", 3),
            Map.entry("resolver/StoreResolver.java", 4),
            Map.entry("resolver/Substitution.java", 18),
            Map.entry("resolver/SyntheticHeads.java", 3),
            Map.entry("resolver/TemporalFrame.java", 6));

    @Test
    void aConstructionCarriesAStampOrIsAPinnedNewCall() throws IOException {
        String root = "core/src/main/java/com/legend";
        List<Path> files = SourceFiles.under(root).stream().filter(p -> p.toString().endsWith(".java")).toList();
        assertTrue(!files.isEmpty(), "core's sources are not declared: " + root);
        Map<String, Integer> found = new TreeMap<>();
        for (Path f : files) {
            List<String> lines = Files.readAllLines(f);
            for (int i = 0; i < lines.size(); i++) {
                String text = lines.get(i);
                String code = text.strip();
                if (code.startsWith("//") || code.startsWith("*") || code.startsWith("/*")) {
                    continue;
                }
                Matcher m = CONSTRUCTION.matcher(text);
                while (m.find()) {
                    StringBuilder window = new StringBuilder(text.substring(m.start()));
                    for (int j = i + 1; j < Math.min(lines.size(), i + 1 + STATEMENT_LINES); j++) {
                        window.append('\n').append(lines.get(j));
                    }
                    int end = window.indexOf(";");
                    String statement = end < 0 ? window.toString() : window.substring(0, end);
                    if (!statement.contains("outsideStore")) {
                        String path = SourceFiles.path(f);
                        found.merge(path.substring(path.indexOf(root) + root.length() + 1), 1, Integer::sum);
                    }
                }
            }
        }
        List<String> grew = found.entrySet().stream()
                .filter(e -> e.getValue() > NEW_CALLS.getOrDefault(e.getKey(), 0))
                .map(e -> e.getKey() + ": " + e.getValue() + " > " + NEW_CALLS.getOrDefault(e.getKey(), 0)).toList();
        assertEquals(List.of(), grew, "a native call built with no stamp: a pass that rebuilds a call it was given passes"
                + " its stamp (x.outsideStore()); a call that is new moves this file's pin, with a dated note");
        List<String> shrank = NEW_CALLS.entrySet().stream()
                .filter(e -> found.getOrDefault(e.getKey(), 0) < e.getValue())
                .map(e -> e.getKey() + ": " + found.getOrDefault(e.getKey(), 0) + " < " + e.getValue()).sorted().toList();
        if (!shrank.isEmpty()) {
            System.out.println("[native-call-stamp] fewer new calls than pinned -- tighten NEW_CALLS: " + shrank);
        }
    }
}
