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
 * GENERATES {@code core/src/main/java/com/legend/builtin/DynaFn.java} whole, from the pinned legend-engine alone: every
 * relational "dynafunction" its dialect extensions register ({@code dynaFnToSql('name', …)}), each with the dialects
 * that register it (from the file path) and whether the engine's type-inference map knows it. The platform's decision
 * for each name is not here: {@code DynaFnDecisions}, written by hand, which DynaFn asks; a name it does not list is
 * UNSUPPORTED, so a new engine name arrives unsupported. The committed file is held to this output by its diff test;
 * the decisions by {@code DynaFnRegistryTest}.
 *
 * <pre>
 *   DynaFnGenerator &lt;legend-engine root&gt; &lt;output&gt;
 * </pre>
 */
public final class DynaFnGenerator {

    private DynaFnGenerator() {}

    private static final Pattern DYNA = Pattern.compile("dynaFnToSql\\('([A-Za-z0-9_]+)'");
    private static final Pattern INFERENCE_ENTRY = Pattern.compile("pair\\(\\s*\\n\\s*'([A-Za-z0-9_]+)',");
    private static final String INFERENCE_MAP = "getDynaFunctionTypeInferenceMap():";

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: DynaFnGenerator <legend-engine root> <output>");
        }
        TreeMap<String, Upstream> up = upstream(Path.of(args[0]));
        Files.writeString(Path.of(args[1]), render(up), StandardCharsets.UTF_8);
        System.out.println("[dynafn] generated " + up.size() + " members");
    }

    /** One upstream name's facts: registering dialects + inference-map membership. */
    public record Upstream(TreeSet<String> dialects, boolean inferred) {
    }

    static String dialectOf(Path p) {
        String s = p.toString().replace('\\', '/');
        Matcher m = Pattern.compile("core_relational_(\\w+)/").matcher(s);
        if (m.find()) {
            return m.group(1).toUpperCase(java.util.Locale.ROOT);
        }
        m = Pattern.compile("dbSpecific/(\\w+)/").matcher(s);
        if (m.find()) {
            return m.group(1).toUpperCase(java.util.Locale.ROOT);
        }
        if (s.endsWith("extensionDefaults.pure")) {
            return "DEFAULT";
        }
        throw new IllegalStateException("a dynaFnToSql registry at a path no dialect rule names: " + s);
    }

    /** name → facts, read from every registry file in the checkout. */
    public static TreeMap<String, Upstream> upstream(Path engineRoot) throws IOException {
        TreeMap<String, Upstream> out = new TreeMap<>();
        try (Stream<Path> walk = Files.walk(engineRoot)) {
            for (Path p : walk.filter(x -> x.toString().endsWith(".pure")).toList()) {
                String text = Files.readString(p, StandardCharsets.UTF_8);
                if (text.contains("dynaFnToSql(")) {
                    // the dialect only for a file that registers a name (dbExtension.pure declares the function
                    // itself, at a path no dialect rule names, and registers none)
                    Matcher m = DYNA.matcher(text);
                    String dialect = null;
                    while (m.find()) {
                        if (dialect == null) {
                            dialect = dialectOf(p);
                        }
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

    /** DynaFn.java's text: the members and the Dialect enum from {@code up}, around the fixed template. */
    public static String render(TreeMap<String, Upstream> up) {
        if (up.isEmpty()) {
            throw new IllegalStateException("no dynaFnToSql registration found — upstream moved them");
        }
        TreeSet<String> dialects = new TreeSet<>();
        List<String> members = new ArrayList<>();
        for (Map.Entry<String, Upstream> e : up.entrySet()) {
            dialects.addAll(e.getValue().dialects());
            String member = e.getKey().replaceAll("([a-z0-9])([A-Z])", "$1_$2").toUpperCase(java.util.Locale.ROOT);
            StringBuilder ds = new StringBuilder();
            for (String d : e.getValue().dialects()) {
                ds.append(", Dialect.").append(d);
            }
            members.add("    " + member + "(\"" + e.getKey() + "\", Inference." + (e.getValue().inferred() ? "MAPPED" : "NONE")
                    + ds + ")");
        }
        return TEMPLATE.replace("__MEMBERS__", String.join(",\n", members))
                .replace("__DIALECTS__", String.join(", ", dialects));
    }

    private static final String TEMPLATE = """
            // GENERATED by //spec:gen_dynafn from the pinned legend-engine's dynafunction registries -- do not edit.
            // Regenerate: bazel run //:update_generated. The platform's decisions are DynaFnDecisions.java's.
            package com.legend.builtin;

            import java.util.EnumSet;
            import java.util.List;
            import java.util.Map;
            import java.util.Optional;

            /**
             * THE ENGINE'S DYNAFUNCTION REGISTRY, as data: every operator name a relational
             * mapping expression may use ({@code hash(col)}, {@code isDistinct(a, b)},
             * {@code concat(…)}), read from the pinned legend-engine checkout's SQL rendering
             * registries — {@code dynaFnToSql('<name>', …)} in {@code extensionDefaults.pure}
             * and every dialect extension — plus the engine's relational type-inference map
             * ({@code getDynaFunctionTypeInferenceMap}, relationalExtension.pure; some names
             * exist only there). Generated whole by {@code DynaFnGenerator}; how THIS platform
             * resolves each name is {@link DynaFnDecisions}' decision, written by hand:
             * <ul>
             *   <li>{@link Resolution#PURE}: passes through to the Pure native(s) the row's
             *       {@link #fqns()} name — the engine's operator IS pure's function;</li>
             *   <li>{@link Resolution#SHIM}: an engine-only operator with no pure signature
             *       (or a shape pure's differs from) — its {@link Pure.Lite} identity;</li>
             *   <li>{@link Resolution#TRANSLATED}: the mapping translator ({@code RelOpTranslator})
             *       rewrites the call into pure's own spelling and NOTHING passes through — a
             *       shape no arm rewrites is an error;</li>
             *   <li>{@link Resolution#UNSUPPORTED}: registered by the engine, handled by nothing
             *       here yet — a mapping using it fails LOUD naming the operator.</li>
             * </ul>
             * A PURE name may ALSO carry a translator arm for the engine's extra shape
             * ({@code and}/{@code or} with more than two operands, {@code parseDate} with a
             * format): {@code DynaFnArms.ARMS} lists every name with an arm, TRANSLATED or PURE.
             * Never a name set anywhere else: {@link #of(String)} is the one lookup.
             */
            public enum DynaFn {
            __MEMBERS__;

                /** How the platform resolves an engine dynafunction. */
                public enum Resolution { PURE, SHIM, TRANSLATED, UNSUPPORTED }

                /** Whether the engine's relational TYPE-INFERENCE map
                 *  ({@code getDynaFunctionTypeInferenceMap} in relationalExtension.pure) has
                 *  a rule for the name — the engine's second registry of dynafunction names. */
                public enum Inference { MAPPED, NONE }

                /** The engine dialect extension files that register a name. */
                public enum Dialect { __DIALECTS__ }

                private final String name;
                private final Inference inference;
                private final EnumSet<Dialect> dialects;

                DynaFn(String name, Inference inference, Dialect... dialects) {
                    this.name = name;
                    this.inference = inference;
                    this.dialects = EnumSet.noneOf(Dialect.class);
                    this.dialects.addAll(List.of(dialects));
                }

                /** Whether the engine's type-inference map has a rule for the name. */
                public Inference inference() {
                    return inference;
                }

                /** The engine's spelling of the operator. */
                public String dynaName() {
                    return name;
                }

                /** The platform's decision for the name ({@link DynaFnDecisions}; UNSUPPORTED unless it says otherwise). */
                public Resolution resolution() {
                    return DynaFnDecisions.resolution(this);
                }

                /** The dialects whose rendering registry declares this name (empty for a
                 *  name the engine knows only in its type-inference map). */
                public java.util.Set<Dialect> dialects() {
                    return java.util.Collections.unmodifiableSet(dialects);
                }

                /** THE DECLARATIONS this name resolves to: a PURE name's catalog FQNs (the engine
                 *  surface's for the name, or the declared residue: {@link DynaFnDecisions}), a SHIM's
                 *  one {@link Pure.Lite} FQN; empty for TRANSLATED and UNSUPPORTED. The translator
                 *  mints a PURE call carrying these as its candidates, so the typer never resolves a
                 *  dynafunction by a bare spelling (untangle 4b.2). */
                public List<String> fqns() {
                    return DynaFnDecisions.fqns(this);
                }

                /** The {@link Pure.Lite} identity a SHIM resolves to. */
                public String liteFqn() {
                    List<String> fqns = fqns();
                    if (resolution() != Resolution.SHIM || fqns.size() != 1) {
                        throw new IllegalStateException(name + " is not a SHIM");
                    }
                    return fqns.get(0);
                }

                private static final Map<String, DynaFn> BY_NAME;

                static {
                    Map<String, DynaFn> m = new java.util.HashMap<>();
                    for (DynaFn d : values()) {
                        m.put(d.name, d);
                    }
                    BY_NAME = Map.copyOf(m);
                }

                /** The registry entry for an engine operator name, or empty when the engine
                 *  registers no such dynafunction (the name is then a plain Pure function
                 *  the mapping expression calls, resolved like any other). */
                public static Optional<DynaFn> of(String dynaName) {
                    return Optional.ofNullable(BY_NAME.get(dynaName));
                }

                /** Every member of one resolution kind. */
                public static List<DynaFn> withResolution(Resolution r) {
                    return java.util.Arrays.stream(values()).filter(d -> d.resolution() == r).toList();
                }
            }
            """;
}
