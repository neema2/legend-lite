// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * GENERATES engine-handlers.tsv — THE ENGINE SURFACE BY BARE NAME, from the
 * pinned engine's own registry alone (untangle step 4b; the build rebuild's
 * Phase 2). Legend-engine resolves a query's function by its bare name against
 * {@code Handlers.java}'s map FIRST, and only then through the import group
 * ({@code CompileContext.resolveFunctionBuilder}); every handler entry names the
 * exact signature ids it stands for ({@code h("<id>", "<name>", ...)}). This
 * table is that map, verbatim from the pinned text: each bare name with its ids,
 * names sorted, ids in the registry's order.
 *
 * <p>It reads Handlers.java and nothing of ours, so only an upgrade moves it.
 * {@code EngineHandlers} joins it with the platform's declarations when it loads
 * (an id's FQN, or none) and appends the platform's own surface.
 *
 * <p>THE EXTENSIONS' REGISTRATIONS (2026-11-14, the Studio roundtrip's
 * DSL_ExecutionEnvironment cases): the engine's map also holds what each loaded
 * compiler extension registers through its handler hooks
 * ({@code getExtraFunctionExpressionBuilderRegistrationInfoCollectors},
 * {@code getExtraFunctionHandlerRegistrationInfoCollectors}), spelled with the same
 * {@code handlers.h("<id>", "<name>", ...)} entries. Each extension file named after
 * Handlers.java is read the same way, its pairs merged after the registry's: the
 * service extension (ServiceCompilerExtensionImpl) is the one read today, so bare
 * {@code get}, {@code getWithRuntime} and the service {@code from} resolve as they do
 * in the engine.
 *
 * <p>Columns: {@code name \t id}.
 */
public final class EngineHandlersGenerator {

    private EngineHandlersGenerator() {
    }

    /** {@code h("<id>", "<name>"} inside a group, or a direct {@code register("<id>", "<name>", ...)}: the two entry forms. */
    private static final Pattern ENTRY = Pattern.compile("\\b(?:h|register)\\(\\s*\"([^\"]+)\"\\s*,\\s*\"([^\"]+)\"");

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException(
                    "usage: EngineHandlersGenerator <Handlers.java> [<extension.java> ...] <output>");
        }
        List<String> texts = new ArrayList<>();
        for (int i = 0; i < args.length - 1; i++) {
            texts.add(Files.readString(Path.of(args[i]), StandardCharsets.UTF_8));
        }
        String text = generate(texts);
        Files.writeString(Path.of(args[args.length - 1]), text, StandardCharsets.UTF_8);
        System.out.println("[engine-handlers] generated " + (text.split("\n").length - 2) + " rows");
    }

    /** The engine's (name, id) pairs, in the registry's order, deduplicated. */
    public static Map<String, List<String>> entries(String handlersJava) {
        Map<String, List<String>> byName = new LinkedHashMap<>();
        Matcher m = ENTRY.matcher(handlersJava);
        while (m.find()) {
            List<String> ids = byName.computeIfAbsent(m.group(2), k -> new ArrayList<>());
            if (!ids.contains(m.group(1))) {
                ids.add(m.group(1));
            }
        }
        if (byName.isEmpty()) {
            throw new IllegalStateException("Handlers.java carries no h(\"id\", \"name\") entries — upstream moved them");
        }
        return byName;
    }

    /** The (name, id) pairs of the registry, then of each extension in turn, deduplicated: an extension's pairs
     *  under a name follow the registry's. */
    public static Map<String, List<String>> entries(List<String> sources) {
        Map<String, List<String>> byName = new LinkedHashMap<>();
        for (String src : sources) {
            for (var e : entries(src).entrySet()) {
                List<String> ids = byName.computeIfAbsent(e.getKey(), k -> new ArrayList<>());
                for (String id : e.getValue()) {
                    if (!ids.contains(id)) {
                        ids.add(id);
                    }
                }
            }
        }
        return byName;
    }

    static String generate(List<String> sources) {
        StringBuilder sb = new StringBuilder();
        sb.append("# engine-handlers.tsv — THE ENGINE SURFACE BY BARE NAME: legend-engine's Handlers.java registry")
          .append(" (pinned) and the registrations of the compiler extensions read beside it (the service")
          .append(" extension), each bare name with the signature ids it stands for, in the registry's order. Upstream")
          .append(" only: EngineHandlers joins it with the platform's declarations when it loads. Generated by")
          .append(" EngineHandlersGenerator; regenerate: bazel run //:update_generated.\n");
        sb.append("name\tid\n");
        for (var e : new TreeMap<>(entries(sources)).entrySet()) {
            for (String id : e.getValue()) {
                sb.append(e.getKey()).append('\t').append(id).append('\n');
            }
        }
        return sb.toString();
    }
}
