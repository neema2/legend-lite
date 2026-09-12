// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.claims;

import com.legend.builtin.NativeFn;
import com.legend.builtin.Pure;
import com.legend.compiler.spec.CoreFn;
import com.legend.lowering.RegistryKeys;
import com.legend.model.NativeFunctionDefinition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE CLAIM REGISTRY — the platform's implemented surface as ONE computed fact
 * (docs/CLAIM_REGISTRY_DESIGN_2026_09_10.md, docs/UPSTREAM_BOUNDARY_PROGRAM.md
 * workstream D1).
 *
 * <p>An entry in {@link Pure} is a semantic CLAIM: "the platform lowers this."
 * A {@link Claim} is the fact that backs it — <i>this overload is implemented
 * by this owner, in this way</i>. One overload can carry SEVERAL claims,
 * because the same function lowers differently by POSITION: {@code max} is a
 * scalar rule in scalar position and a reducer under groupBy; {@code first}
 * is scalar, reducer and window function (measured 2026-09-10: 112 of 881
 * overloads). Every kind is recorded; the invariant is NONE UNCLAIMED
 * ({@code ClaimRegistryTest}: a shrink-only ratchet on the UNCLAIMED rows of
 * the committed ledger {@code native-claims.tsv}).
 *
 * <p><b>Derived claims</b> cost nothing to write: the four catalog-keyed
 * lowering registries ({@link Scalars#ruleKeys}, {@link Aggregates#reducerKeys},
 * {@link Windows#fnKeys}, {@link Windows#aggregateKeys}), the {@link CoreFn}
 * parse names (every overload whose bare name is a CoreFn name is dispatched
 * there), {@link Pure#walledNativeFqns} and
 * {@link PlatformTypes#IMPLEMENTATION_KIND} are all claims already; this class
 * reads them. The three families that used to dispatch on a string switch
 * ({@code CalendarAgg}, {@code AssertVerdicts}, {@code RowGetters}) are now
 * CLOSED TYPES — {@code CalendarFn}, {@code AssertFn}, {@code RowGetter}: an
 * enum whose constants carry their catalog overloads and whose switches are
 * switch EXPRESSIONS with no default, so a new member fails to COMPILE until
 * it is handled. The enum is the set and the dispatch key: nothing beside the
 * code to keep in sync, and no hand-maintained table anywhere in this class.
 *
 * <p>TEST SCOPE, deliberately: the registry is a MEASUREMENT over main code,
 * not a runtime service — main never dispatches through it (the enums and the
 * registries are the dispatch), so it sits beside its test, outside the
 * architecture's dependency rules for production packages. Its main-scope
 * artifact is the generated ledger {@code native-claims.tsv}, which batch 5's
 * generator reads. Every source is referenced explicitly (no reflection, no
 * service loading), so what is claimed does not depend on load order.
 */
public final class Claims {

    private Claims() {
    }

    /** How an overload is implemented. */
    public enum Kind {
        /** a key in {@code Scalars.RULES} — a SQL expression rule */
        SCALAR_RULE,
        /** a key in {@code Aggregates.REDUCERS} — a SQL aggregate */
        REDUCER,
        /** a key in {@code Windows.FNS} — a window function */
        WINDOW_FN,
        /** a key in {@code Windows.AGGREGATES} — a window-only aggregate */
        WINDOW_AGG,
        /** a {@link CoreFn} parse name — a language form with its own checker and HIR node */
        CORE_FN,
        /** {@code Pure.WALLED_NATIVES} — refused by decision, with a reason */
        WALL,
        /** a closed family type — an enum whose constants carry their overloads and
         *  whose switches are exhaustive ({@code CalendarFn}, {@code AssertFn}, {@code RowGetter}) */
        FAMILY
    }

    /** One claim: the overload, how it is implemented, and by whom.
     *  {@code owner} is the registry's name for derived claims and the owning
     *  class's simple name for arms. */
    public record Claim(NativeFunctionDefinition overload, Kind kind, String owner) {
    }

    private static final Map<NativeFunctionDefinition, List<Claim>> BY_OVERLOAD = new LinkedHashMap<>();

    private static void claim(NativeFunctionDefinition overload, Kind kind, String owner) {
        BY_OVERLOAD.computeIfAbsent(overload, k -> new ArrayList<>()).add(new Claim(overload, kind, owner));
    }

    static {
        for (NativeFunctionDefinition d : Pure.all()) {
            BY_OVERLOAD.put(d, new ArrayList<>());
            String key = d.signatureKey();
            String fqn = d.qualifiedName();
            String bare = fqn.substring(fqn.lastIndexOf(':') + 1);
            if (RegistryKeys.scalarRules().contains(key)) {
                claim(d, Kind.SCALAR_RULE, "Scalars.RULES");
            }
            if (RegistryKeys.reducers().contains(key)) {
                claim(d, Kind.REDUCER, "Aggregates.REDUCERS");
            }
            if (RegistryKeys.windowFunctions().contains(key)) {
                claim(d, Kind.WINDOW_FN, "Windows.FNS");
            }
            if (RegistryKeys.windowAggregates().contains(key)) {
                claim(d, Kind.WINDOW_AGG, "Windows.AGGREGATES");
            }
            CoreFn fn = CoreFn.parseNames().get(bare);
            if (fn != null) {
                claim(d, Kind.CORE_FN, "CoreFn." + fn.name());
            }
            if (Pure.walledNativeFqns().contains(fqn)) {
                claim(d, Kind.WALL, "Pure.WALLED_NATIVES");
            }
        }

        // ---- THE FAMILIES: NativeFn — closed types (enums, the CoreFn pattern)
        // whose owning switches are exhaustive at compile time; the enum is the
        // set and the dispatch key, so the claim is read straight off it
        for (var fam : NativeFn.families().entrySet()) {
            for (NativeFn.Member m : fam.getValue()) {
                for (NativeFunctionDefinition o : m.overloads()) {
                    claim(o, Kind.FAMILY, "NativeFn." + fam.getKey());
                }
            }
        }
    }

    /** Every claim, in catalog order, one list per overload (empty = UNCLAIMED). */
    public static Map<NativeFunctionDefinition, List<Claim>> all() {
        return Collections.unmodifiableMap(BY_OVERLOAD);
    }

    /** The claims on one overload (empty when unclaimed). */
    public static List<Claim> of(NativeFunctionDefinition overload) {
        return BY_OVERLOAD.getOrDefault(overload, List.of());
    }

    /** Whether the platform claims ANY overload of {@code fqn} — the prelude's
     *  exclusion rule keys on this (batch 4). */
    public static boolean claimsFqn(String fqn) {
        for (var e : BY_OVERLOAD.entrySet()) {
            if (e.getKey().qualifiedName().equals(fqn) && !e.getValue().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /** The BARE names of every claimed overload — the prelude generator's
     *  exclusion rule (batch 4): a library body under a name the platform
     *  implements stays out of the prelude (else a bare call resolves to the
     *  library's FQN through the core imports and bypasses the platform's
     *  form — batch 169). Keyed on CLAIMS, not on "a signature exists". */
    public static java.util.Set<String> claimedBareNames() {
        java.util.Set<String> out = new java.util.TreeSet<>();
        for (var e : BY_OVERLOAD.entrySet()) {
            if (!e.getValue().isEmpty()) {
                String fqn = e.getKey().qualifiedName();
                out.add(fqn.substring(fqn.lastIndexOf(':') + 1));
            }
        }
        return out;
    }

    /** Whether ANY claim backs the overload. */
    public static boolean claimed(NativeFunctionDefinition overload) {
        return !of(overload).isEmpty();
    }
}
