// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.claims;

import com.legend.testing.Repo;
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

    @Test
    @DisplayName("the implemented surface is the committed ledger (native-claims.tsv), byte-equal")
    void ledgerIsCurrent() throws Exception {
        List<String> actual = ClaimsGenerator.ledger(com.legend.generators.SourceTree.of(MAIN));
        Files.createDirectories(Repo.outDir());
        Files.write(Repo.out("native-claims.tsv"), actual, StandardCharsets.UTF_8);
        assertTrue(Files.exists(RESOURCE), "native-claims.tsv missing — regenerate: bazel run //:update_generated");
        List<String> expected = Files.readAllLines(RESOURCE, StandardCharsets.UTF_8);
        assertEquals(expected, actual,
                "the implemented surface moved — review target/native-claims.tsv against the"
                + " committed ledger; regenerate (bazel run //:update_generated) for a DELIBERATE change");
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
                unclaimed.add(d.qualifiedName() + " " + ClaimsGenerator.signature(d));
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
        Files.createDirectories(Repo.outDir());
        Files.write(Repo.out("unclaimed-natives.txt"), unclaimed, StandardCharsets.UTF_8);
        assertTrue(unclaimed.size() <= UNCLAIMED_MAX, "UNCLAIMED overloads GREW: " + unclaimed.size()
                + " > " + UNCLAIMED_MAX + " — a Pure.java entry nothing implements; register it or"
                + " move it to the prelude (target/unclaimed-natives.txt)");
        if (unclaimed.size() < UNCLAIMED_MAX) {
            System.out.println("[claims] UNCLAIMED shrank to " + unclaimed.size()
                    + " (pin " + UNCLAIMED_MAX + ") — tighten UNCLAIMED_MAX");
        }
    }
}
