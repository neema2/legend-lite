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
        Files.createDirectories(com.legend.testing.TestOutputs.dir());
        Files.write(com.legend.testing.TestOutputs.file("unclaimed-natives.txt"), unclaimed, StandardCharsets.UTF_8);
        assertTrue(unclaimed.size() <= UNCLAIMED_MAX, "UNCLAIMED overloads GREW: " + unclaimed.size()
                + " > " + UNCLAIMED_MAX + " — a Pure.java entry nothing implements; register it or"
                + " move it to the prelude (target/unclaimed-natives.txt)");
        if (unclaimed.size() < UNCLAIMED_MAX) {
            System.out.println("[claims] UNCLAIMED shrank to " + unclaimed.size()
                    + " (pin " + UNCLAIMED_MAX + ") — tighten UNCLAIMED_MAX");
        }
    }

    /** The edge's claimers of {@code substringAsGiven}: legend-engine's door's edition table, whose rows name it.
     *  (2026-11-26, the edition program's step 2: {@code EngineTable} in {@code EngineSubstitutes}' place -- the door's
     *  rewrite of the typed query, whose substitutes that class built, is deleted, and the lowering's table names the
     *  native instead; one edge claimer before and after, no product claimer joins.) */
    static final java.util.Set<String> SUBSTRING_AS_GIVEN_EDGE_CLAIMERS = java.util.Set.of("EngineTable");

    /**
     * NO PRODUCT CLAIMER OF {@code substringAsGiven} BUT {@code Scalars} (the pin the user ruled, 2026-11-19 ~21:10, "Yes
     * to all three for substring", relayed by the compat owner; the reviewer's D-S1 as corrected, ca8ea4a5). The native is
     * the legend-engine door's alone, and no user may name it (Pure.COMPILER_OWN). So in the committed ledger its
     * {@code also} column -- the files naming one of its overload constants, its id group or its quoted name -- holds the
     * lowering's rule and the door's table ({@code edge.EngineTable}, the one place that names it), and no other.
     * A product frontend that wrote it would be named here.
     * Named limit: the column does not read a use through the {@code Pure.Lite} name constant alone.
     */
    @Test
    @DisplayName("substringAsGiven's one product claimer is Scalars, its namer the door's EngineTable")
    void substringAsGivensOneProductClaimerIsScalars() throws IOException {
        List<String[]> rows = new ArrayList<>();
        for (String line : Files.readAllLines(RESOURCE, StandardCharsets.UTF_8)) {
            String[] cells = line.split("\t", -1);
            if (cells.length >= 6 && cells[0].equals(Pure.Lite.SUBSTRING_AS_GIVEN)) {
                rows.add(cells);
            }
        }
        assertEquals(2, rows.size(), "substringAsGiven's two overloads");
        for (String[] row : rows) {
            assertEquals("", strayClaimers(row[5]), row[2]);
            assertEquals(java.util.Set.of("Scalars", "EngineTable"), java.util.Set.of(row[5].split(";")),
                    row[2] + ": its lowering's rule and the door's table");
        }
        // the pin bites: a planted product claimer is named
        assertEquals("CypherToPure", strayClaimers("CypherToPure;EngineTable;Scalars"));
    }

    /**
     * NO PRODUCT CLAIMER OF {@code substrAsGiven} BUT {@code Scalars} (A3, 2026-11-20; the user's word ~07:45, "Okay 1",
     * relayed by the compat owner): substringAsGiven's pin, for its sibling. The native is the legend-engine door's
     * alone (its table's row for {@code substr} and the corrected {@code substring}), and no user may name
     * it (Pure.COMPILER_OWN), so its {@code also} column holds the lowering's rule and the door's table.
     */
    @Test
    @DisplayName("substrAsGiven's one product claimer is Scalars, its namer the door's EngineTable")
    void substrAsGivensOneProductClaimerIsScalars() throws IOException {
        List<String[]> rows = new ArrayList<>();
        for (String line : Files.readAllLines(RESOURCE, StandardCharsets.UTF_8)) {
            String[] cells = line.split("\t", -1);
            if (cells.length >= 6 && cells[0].equals(Pure.Lite.SUBSTR_AS_GIVEN)) {
                rows.add(cells);
            }
        }
        assertEquals(2, rows.size(), "substrAsGiven's two overloads");
        for (String[] row : rows) {
            assertEquals("", strayClaimers(row[5]), row[2]);
            assertEquals(java.util.Set.of("Scalars", "EngineTable"), java.util.Set.of(row[5].split(";")),
                    row[2] + ": its lowering's rule and the door's table");
        }
    }

    /** The claimers in {@code also} that are neither Scalars nor the edge's, joined; empty when there are none. */
    private static String strayClaimers(String also) {
        return String.join(";", java.util.Arrays.stream(also.split(";"))
                .filter(c -> !c.isEmpty() && !c.equals("Scalars") && !SUBSTRING_AS_GIVEN_EDGE_CLAIMERS.contains(c))
                .toList());
    }
}
