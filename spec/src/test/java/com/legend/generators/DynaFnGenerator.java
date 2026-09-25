// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * GENERATES {@code DynaFn.java}'s member block from the pinned legend-engine
 * tree's two dynafunction registries — every {@code dynaFnToSql('name', …)} in
 * {@code extensionDefaults.pure} and the dialect extensions, with its registering
 * dialects, and whether {@code getDynaFunctionTypeInferenceMap} knows it. An
 * existing member keeps its resolution and Lite constant; a new engine name
 * lands UNSUPPORTED. Checked by {@link DynaFnRegistryTest}.
 *
 * <pre>
 *   DynaFnGenerator &lt;legend-engine root&gt; &lt;DynaFn.java&gt; &lt;output&gt;
 * </pre>
 */
public final class DynaFnGenerator {

    private DynaFnGenerator() {}

    private static final Pattern DYNA = Pattern.compile("dynaFnToSql\\('([A-Za-z0-9_]+)'");
    private static final Pattern INFERENCE_ENTRY = Pattern.compile("pair\\(\\s*\\n\\s*'([A-Za-z0-9_]+)',");
    private static final String INFERENCE_MAP = "getDynaFunctionTypeInferenceMap():";

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            throw new IllegalArgumentException("usage: DynaFnGenerator <legend-engine root> <DynaFn.java> <output>");
        }
        String generated = generate(upstream(Path.of(args[0])),
                Files.readString(Path.of(args[1]), StandardCharsets.UTF_8));
        Files.writeString(Path.of(args[2]), generated, StandardCharsets.UTF_8);
    }

    /** One upstream name's facts: registering dialects + inference-map membership. */
    public record Upstream(TreeSet<String> dialects, boolean inferred) {
    }

    static String dialectOf(Path p) {
        String s = p.toString().replace('\\', '/');
        Matcher m = Pattern.compile("core_relational_(\\w+)/").matcher(s);
        if (m.find()) {
            return m.group(1).toUpperCase();
        }
        m = Pattern.compile("dbSpecific/(\\w+)/").matcher(s);
        if (m.find()) {
            return m.group(1).toUpperCase();
        }
        return s.endsWith("extensionDefaults.pure") ? "DEFAULT" : "OTHER";
    }

    /** name → facts, read from every registry file in the checkout. */
    public static TreeMap<String, Upstream> upstream(Path engineRoot) throws IOException {
        TreeMap<String, Upstream> out = new TreeMap<>();
        try (Stream<Path> walk = Files.walk(engineRoot)) {
            for (Path p : walk.filter(x -> x.toString().endsWith(".pure")).toList()) {
                String text = Files.readString(p, StandardCharsets.UTF_8);
                if (text.contains("dynaFnToSql(")) {
                    String dialect = dialectOf(p);
                    Matcher m = DYNA.matcher(text);
                    while (m.find()) {
                        out.computeIfAbsent(m.group(1), k -> new Upstream(new TreeSet<>(), false))
                                .dialects().add(dialect);
                    }
                }
                int at = text.indexOf(INFERENCE_MAP);
                if (at >= 0) {
                    Matcher m = INFERENCE_ENTRY.matcher(text.substring(at));
                    while (m.find()) {
                        Upstream u = out.get(m.group(1));
                        out.put(m.group(1), new Upstream(u == null ? new TreeSet<>() : u.dialects(), true));
                    }
                }
            }
        }
        return out;
    }

    /** The catalog FQNs a PURE dynafunction name resolves to, spelled as the
     *  member's list literal: every user-resolvable native of that bare name. */
    static String pureFqns(String dynaName) {
        List<String> out = new ArrayList<>();
        for (String fqn : com.legend.builtin.Pure.userResolvableFunctionFqns()) {
            if (fqn.substring(fqn.lastIndexOf(':') + 1).equals(dynaName) && !out.contains(fqn)) {
                out.add(fqn);
            }
        }
        StringBuilder sb = new StringBuilder();
        for (String f : out) {
            sb.append(sb.length() == 0 ? "" : ", ").append('"').append(f).append('"');
        }
        return sb.toString();
    }

    /** DynaFn.java's text with its member block rewritten from {@code up},
     *  keeping each existing member's resolution and Lite constant. */
    public static String generate(TreeMap<String, Upstream> up, String text) {
        Map<String, String[]> existing = new TreeMap<>();
        Matcher m = Pattern.compile("^    ([A-Z_0-9]+)\\(\"(\\w+)\", Resolution\\.(\\w+), List\\.of\\(([^)]*)\\), Inference\\.\\w+", Pattern.MULTILINE).matcher(text);
        while (m.find()) {
            existing.put(m.group(2), new String[] {m.group(3), m.group(4)});
        }
        List<String> lines = new ArrayList<>();
        for (Map.Entry<String, Upstream> e : up.entrySet()) {
            String[] kept = existing.getOrDefault(e.getKey(), new String[] {"UNSUPPORTED", ""});
            // the FQN column: a PURE name's declarations are DERIVED from the
            // catalog (every user-resolvable native of that bare name — the
            // engine's operator IS pure's function, wherever the platform
            // declares it); a SHIM keeps its spelled Lite constant; the rest none
            String[] keep = kept[0].equals("PURE")
                    ? new String[] {kept[0], pureFqns(e.getKey())}
                    : new String[] {kept[0], kept[0].equals("SHIM") ? kept[1] : ""};
            String member = e.getKey().replaceAll("([a-z0-9])([A-Z])", "$1_$2").toUpperCase();
            StringBuilder ds = new StringBuilder();
            for (String d : e.getValue().dialects()) {
                ds.append(", Dialect.").append(d);
            }
            lines.add("    " + member + "(\"" + e.getKey() + "\", Resolution." + keep[0] + ", List.of(" + keep[1]
                    + "), Inference." + (e.getValue().inferred() ? "MAPPED" : "NONE") + ds + "),");
        }
        String last = lines.get(lines.size() - 1);
        lines.set(lines.size() - 1, last.substring(0, last.length() - 1) + ";");
        int start = text.indexOf("public enum DynaFn {\n") + "public enum DynaFn {\n".length();
        Matcher end = Pattern.compile("^    [A-Z_0-9]+\\(.*\\);\\n", Pattern.MULTILINE).matcher(text);
        if (!end.find(start)) {
            throw new IllegalStateException("member block not found");
        }
        System.out.println("[dynafn] generated " + lines.size() + " members");
        return text.substring(0, start) + String.join("\n", lines) + "\n" + text.substring(end.end());
    }
}
