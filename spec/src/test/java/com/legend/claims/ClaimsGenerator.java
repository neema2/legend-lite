// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.claims;

import com.legend.builtin.Pure;
import com.legend.generators.SourceTree;
import com.legend.model.NativeFunctionDefinition;
import com.legend.protocol.TypeExpression;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * GENERATES native-claims.tsv — the implemented surface, one row per Pure.java
 * overload, from {@link Claims} over the COMPILED registries. The core it runs on
 * is the one the other generators produced (spec/BUILD.bazel: :gen_claims runs on
 * //core:core_next), and it reads core's main sources as text for the
 * {@code also} column. Checked by {@link ClaimRegistryTest}.
 *
 * <pre>
 *   ClaimsGenerator &lt;core/src/main/java&gt; &lt;output&gt; [&lt;relative path&gt;=&lt;replacement&gt; ...]
 * </pre>
 */
public final class ClaimsGenerator {

    private ClaimsGenerator() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException(
                    "usage: ClaimsGenerator <core/src/main/java> <output> [<relative path>=<replacement> ...]");
        }
        Map<String, Path> overrides = new LinkedHashMap<>();
        for (int i = 2; i < args.length; i++) {
            String[] kv = args[i].split("=", 2);
            overrides.put(kv[0], Path.of(kv[1]));
        }
        List<String> lines = ledger(new SourceTree(Path.of(args[0]), overrides));
        // '\n', never the platform's separator: a committed file's bytes do not
        // depend on the machine that generated it (Files.write(lines) wrote CRLF on
        // Windows — the first Bazel CI runs, 2026-09-23)
        Files.writeString(Path.of(args[1]), String.join("\n", lines) + "\n", StandardCharsets.UTF_8);
        System.out.println("[claims] generated " + lines.size() + " lines");
    }

    /** constant name(s) per overload, by reflection over {@link Pure}'s
     *  fields — the catalog's own naming, never a parse of the source. */
    static Map<NativeFunctionDefinition, List<String>> constants() throws IllegalAccessException {
        Map<NativeFunctionDefinition, List<String>> out = new LinkedHashMap<>();
        for (Field f : Pure.class.getFields()) {
            if (Modifier.isStatic(f.getModifiers()) && f.getType() == NativeFunctionDefinition.class) {
                out.computeIfAbsent((NativeFunctionDefinition) f.get(null), k -> new ArrayList<>())
                        .add(f.getName());
            }
        }
        return out;
    }

    /** Every main source file: simple class name → text. Four simple names
     *  repeat across packages (Json, Multiplicity, PkInference, package-info);
     *  the LAST in sorted path order is the one kept — deterministic, where the
     *  old unsorted walk left it to the file system's order. */
    static Map<String, String> mainSources(SourceTree main) throws IOException {
        Map<String, String> out = new TreeMap<>();
        for (String p : main.files()) {
            if (!p.endsWith(".java")) {
                continue;
            }
            String name = p.substring(p.lastIndexOf('/') + 1);
            out.put(name.substring(0, name.length() - 5), main.read(p));
        }
        return out;
    }

    private static final java.util.Set<String> NOT_ALSO = java.util.Set.of(
            "Pure", "Claims", "NativeFn");

    /** {@code Pure.<constant>} as a WHOLE identifier — {@code Pure.PI} is
     *  not a reference inside {@code Pure.PIVOT__…} (the readers column
     *  once listed a pivot reader under pi). */
    static boolean namesConstant(String text, String constant) {
        String needle = "Pure." + constant;
        int at = text.indexOf(needle);
        while (at >= 0) {
            int end = at + needle.length();
            if (end >= text.length() || !Character.isJavaIdentifierPart(text.charAt(end))) {
                return true;
            }
            at = text.indexOf(needle, end);
        }
        return false;
    }

    /** The files (simple names) that name the overload — one of its
     *  constants or its FQN — beyond the registry and the family enums. */
    static List<String> also(Map<String, String> sources, NativeFunctionDefinition d, List<String> consts) {
        List<String> out = new ArrayList<>();
        for (var e : sources.entrySet()) {
            if (NOT_ALSO.contains(e.getKey())) {
                continue;
            }
            String text = e.getValue();
            boolean hit = text.contains("\"" + d.qualifiedName() + "\"");
            for (int i = 0; !hit && i < consts.size(); i++) {
                hit = namesConstant(text, consts.get(i));
            }
            if (hit) {
                out.add(e.getKey());
            }
        }
        return out;
    }

    /** Canonical signature rendering — the ledger's readable identity
     *  (the {@code signatureKey} the registries use is exact but prints
     *  parser nodes). */
    public static String signature(NativeFunctionDefinition d) {
        StringBuilder s = new StringBuilder();
        if (!d.typeParameters().isEmpty() || !d.multiplicityParameters().isEmpty()) {
            s.append('<').append(String.join(",", d.typeParameters()));
            if (!d.multiplicityParameters().isEmpty()) {
                s.append('|').append(String.join(",", d.multiplicityParameters()));
            }
            s.append('>');
        }
        s.append('(');
        for (int i = 0; i < d.parameters().size(); i++) {
            var p = d.parameters().get(i);
            if (i > 0) {
                s.append(", ");
            }
            s.append(p.name()).append(':').append(renderType(p.type())).append(p.multiplicity());
        }
        return s.append("):").append(renderType(d.returnType()))
                .append(d.returnMultiplicity()).toString();
    }

    private static String renderType(TypeExpression t) {
        return switch (t) {
            case TypeExpression.NameRef n -> n.name();
            case TypeExpression.Generic g -> g.name() + "<"
                    + String.join(",", g.arguments().stream()
                            .map(ClaimsGenerator::renderType).toList()) + ">";
            case TypeExpression.FunctionType f -> "{"
                    + String.join(",", f.parameters().stream()
                            .map(pp -> renderType(pp.type()) + pp.multiplicity()).toList())
                    + "->" + renderType(f.result().type()) + f.result().multiplicity() + "}";
            case TypeExpression.RelationType r -> "("
                    + String.join(",", r.columns().stream()
                            .map(c -> c.name() + ":" + renderType(c.type())).toList()) + ")";
            case TypeExpression.SchemaAlgebra a ->
                    renderType(a.left()) + a.op() + renderType(a.right());
        };
    }

    /** native-claims.tsv's lines, from the COMPILED registries (the core on
     *  this program's classpath) and core's main sources as text. */
    public static List<String> ledger(SourceTree main) throws Exception {
        Map<NativeFunctionDefinition, List<String>> consts = constants();
        Map<String, String> sources = mainSources(main);
        List<String> rows = new ArrayList<>();
        rows.add("# native-claims.tsv — THE IMPLEMENTED SURFACE: one row per Pure.java overload,"
                + " generated by ClaimsGenerator from Claims (docs/CLAIM_REGISTRY_DESIGN_2026_09_10.md)."
                + " kinds = every registration (a function lowers differently by position)."
                + " Regenerate: bazel run //:update_generated; the diff is the review.");
        rows.add("fqn\tsignature\tconstant\tkinds\towners\talso");
        TreeMap<String, String> sorted = new TreeMap<>();
        for (var e : Claims.all().entrySet()) {
            NativeFunctionDefinition d = e.getKey();
            List<Claims.Claim> cs = e.getValue();
            List<String> names = consts.getOrDefault(d, List.of("?"));
            TreeSet<String> kinds = new TreeSet<>();
            TreeSet<String> owners = new TreeSet<>();
            for (Claims.Claim c : cs) {
                kinds.add(c.kind().name());
                owners.add(c.owner());
            }
            String sig = signature(d);
            String row = d.qualifiedName() + "\t" + sig + "\t" + String.join("|", names)
                    + "\t" + (kinds.isEmpty() ? "UNCLAIMED" : String.join("|", kinds))
                    + "\t" + String.join("|", owners)
                    + "\t" + String.join(";", also(sources, d, names));
            sorted.put(d.qualifiedName() + " " + sig, row);
        }
        rows.addAll(sorted.values());
        return rows;
    }
}
