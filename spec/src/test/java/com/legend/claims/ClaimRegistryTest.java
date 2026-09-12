// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.claims;

import com.legend.builtin.Pure;
import com.legend.model.NativeFunctionDefinition;
import com.legend.protocol.TypeExpression;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

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
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE IMPLEMENTED SURFACE, as a committed ledger (upstream boundary batch 3;
 * docs/CLAIM_REGISTRY_DESIGN_2026_09_10.md).
 *
 * <p>{@code native-claims.tsv} — one row per {@link Pure} overload:
 * {@code fqn, signature, constant, kinds, owners, also} — is regenerated from
 * {@link Claims} every run and asserted byte-equal to the committed resource
 * (the {@code prelude.pure} contract). Any change to what the platform
 * implements is a reviewed diff. {@code kinds} lists EVERY registration of the
 * overload (a function lowers differently by position — scalar rule AND
 * reducer AND window); UNCLAIMED rows are visible in git until batch 4 empties
 * them, and their count is the shrink-only ratchet.
 *
 * <p>{@code also} is MEASURED, never typed: every file under
 * {@code src/main/java} (other than {@code Pure}, {@code Claims} and the
 * family enums) that names one of the overload's constants ({@code Pure.X})
 * or its FQN string — the sites that touch the function beyond its
 * registrations. Evidence for a reviewer, not a claim.
 */
public class ClaimRegistryTest {

    static final Path RESOURCE = com.legend.generators.CoreTree.resource("com/legend/builtin/native-claims.tsv");
    static final Path MAIN = com.legend.generators.CoreTree.CORE.resolve("src/main/java");

    /** Shrink-only: UNCLAIMED overloads in the ledger. MEASURED at the batch-3
     *  landing (2026-09-10): 133 overloads / 94 FQNs — Pure.java entries no
     *  registry, CoreFn name, wall, or NativeFn family enum (which absorbed
     *  the executor kinds table in batch 4b)
     *  backs (window frames, lateral, reduce, instanceOf, dynamicNew, the
     *  post-processors, createDbConfig, toCSV, the reflection natives, …).
     *  Batch 4 adjudicates every one: register it (a family enum or a rule)
     *  or move it to the prelude; the pin reaches 0 there. */
    // 133 -> 91 (batch 4a, 2026-09-10): 38 FQNs / 42 overloads that no code
    // dispatches on left Pure.java for the prelude (bodies carried where
    // upstream has one, respelled natives where it does not). Kept, for 4b:
    // 6 census "constant-only" rows dispatched through PlatformTypes
    // predicates (fetchDb*MetaData x4, createTableStatement, createDbConfig),
    // and `_range` (7 overloads) whose frame VALUE the over() checker
    // consumes by type — a name-grep census cannot see that (102 PCT tests
    // went red when it left, and came back).
    // 91 -> 67 (batch 4b group 1, 2026-09-10): NativeFn.Frame (rows, _range,
    // unbounded) and NativeFn.LowererForm (lateral, reduce, zScore, the two
    // row-mappers) registered; assertTdsEquivalent + toCSV joined
    // NativeFn.Verdict; `offset` and `is` left Pure.java (the census's only
    // "handler" for them was the SQL keyword list in the lexicon).
    // 67 -> 19 (batch 4b group 2): eight front-end families registered in
    // NativeFn (LiteralForm, ContextOption, PlanWrapper, ObjectReference,
    // SubtypeForm, ResolverForm, LiteDesugar, TyperForm — 43 overloads);
    // convertTimeZone, averageRank, newUnit, sourceInformation left
    // Pure.java (typed or parsed, never lowered). Group 3 (batch 4b landing):
    // the executor kinds left PlatformTypes for NativeFn (JavaRoutine, Handle,
    // Effect, Carrier, ContextOwner) with DdlStatement and TyperForm.UNION;
    // createDbConfig (7 overloads) LEFT Pure.java: it is a SUBSUMED engine
    // program (com.legend.builtin.Subsumed, SubsumedRegistryTest) — an engine
    // program the platform replaces, typed by the corpus's own declaration,
    // its value dead. MEASURED 2026-09-10: 826 overloads, 0 unclaimed.
    static final int UNCLAIMED_MAX = 0;

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

    /** Every main source file: simple class name → text. */
    static Map<String, String> mainSources() throws IOException {
        Map<String, String> out = new TreeMap<>();
        try (Stream<Path> s = Files.walk(MAIN)) {
            for (Path p : s.filter(x -> x.toString().endsWith(".java")).toList()) {
                String name = p.getFileName().toString();
                out.put(name.substring(0, name.length() - 5), Files.readString(p, StandardCharsets.UTF_8));
            }
        }
        return out;
    }

    private static final java.util.Set<String> NOT_ALSO = java.util.Set.of(
            "Pure", "Claims", "NativeFn");

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
                hit = text.contains("Pure." + consts.get(i));
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
    static String signature(NativeFunctionDefinition d) {
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
                            .map(ClaimRegistryTest::renderType).toList()) + ">";
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

    static List<String> ledger() throws Exception {
        Map<NativeFunctionDefinition, List<String>> consts = constants();
        Map<String, String> sources = mainSources();
        List<String> rows = new ArrayList<>();
        rows.add("# native-claims.tsv — THE IMPLEMENTED SURFACE: one row per Pure.java overload,"
                + " generated by ClaimRegistryTest from Claims (docs/CLAIM_REGISTRY_DESIGN_2026_09_10.md)."
                + " kinds = every registration (a function lowers differently by position)."
                + " Regenerate with -Dclaims.generate=1; the diff is the review.");
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

    @Test
    @DisplayName("the implemented surface is the committed ledger (native-claims.tsv), byte-equal")
    void ledgerIsCurrent() throws Exception {
        List<String> actual = ledger();
        Files.createDirectories(Path.of("target"));
        Files.write(Path.of("target/native-claims.tsv"), actual, StandardCharsets.UTF_8);
        if ("1".equals(System.getProperty("claims.generate"))) {
            Files.createDirectories(RESOURCE.getParent());
            Files.write(RESOURCE, actual, StandardCharsets.UTF_8);
            System.out.println("[claims] wrote " + RESOURCE + " (" + actual.size() + " lines)");
        }
        assertTrue(Files.exists(RESOURCE), "native-claims.tsv missing — run with -Dclaims.generate=1");
        List<String> expected = Files.readAllLines(RESOURCE, StandardCharsets.UTF_8);
        assertEquals(expected, actual,
                "the implemented surface moved — review target/native-claims.tsv against the"
                + " committed ledger; regenerate with -Dclaims.generate=1 for a DELIBERATE change");
    }

    @Test
    @DisplayName("no Pure.java overload is unclaimed beyond the shrink-only ratchet")
    void unclaimedIsRatcheted() throws Exception {
        List<String> unclaimed = new ArrayList<>();
        java.util.Map<String, Integer> byKind = new TreeMap<>();
        int total = 0;
        for (var e : Claims.all().entrySet()) {
            total++;
            NativeFunctionDefinition d = e.getKey();
            if (e.getValue().isEmpty()) {
                unclaimed.add(d.qualifiedName() + " " + signature(d));
            }
            for (Claims.Claim c : e.getValue()) {
                byKind.merge(c.kind().name(), 1, Integer::sum);
            }
        }
        TreeSet<String> unclaimedFqns = new TreeSet<>();
        for (String u : unclaimed) {
            unclaimedFqns.add(u.substring(0, u.indexOf(' ') < 0 ? u.length() : u.indexOf(' ')));
        }
        System.out.println("[claims] overloads=" + total + " unclaimed=" + unclaimed.size()
                + " (" + unclaimedFqns.size() + " FQNs) by-kind=" + byKind);
        Files.createDirectories(Path.of("target"));
        Files.write(Path.of("target/unclaimed-natives.txt"), unclaimed, StandardCharsets.UTF_8);
        assertTrue(unclaimed.size() <= UNCLAIMED_MAX, "UNCLAIMED overloads GREW: " + unclaimed.size()
                + " > " + UNCLAIMED_MAX + " — a Pure.java entry nothing implements; register it or"
                + " move it to the prelude (target/unclaimed-natives.txt)");
        if (unclaimed.size() < UNCLAIMED_MAX) {
            System.out.println("[claims] UNCLAIMED shrank to " + unclaimed.size()
                    + " (pin " + UNCLAIMED_MAX + ") — tighten UNCLAIMED_MAX");
        }
    }
}
