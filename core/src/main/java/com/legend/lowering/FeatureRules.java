// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.builtin.Pure;
import com.legend.compiler.spec.typed.Feature;
import com.legend.error.NotImplementedException;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The engine's execution feature flags ({@link Feature}) as the LOWERING
 * consumes them: a flag SELECTS a scalar rule in place of {@link Scalars}'
 * plain rule for the same signature key — an emission, never a different
 * typed tree. Consumers by flag:
 * <ul>
 *   <li>CORRECT_SQL_SUBSTRING_INDEXING — the {@code substring} overloads
 *       (the engine's processSubstr: Pure's 0-based, end-exclusive indexes
 *       become SQL's 1-based start and length; a literal index folds
 *       ({@code substr(s, 4, 9) -> substring(s, 5, 5)}), a computed one
 *       becomes plus / minus on the tree);</li>
 *   <li>LEGACY_SQL_NULL_UNSAFE_EQUALS — the Lowerer's equality form
 *       (NullSemantics.verbatim);</li>
 *   <li>PUSH_DOWN_ENUM_TRANSFORM — a documented no-op: the platform only
 *       ever transforms enums in SQL;</li>
 *   <li>any other member is LOUD when set ({@link #requireConsumed}) — a
 *       flag the engine honours and the platform silently ignored would be
 *       a false pass.</li>
 * </ul>
 */
final class FeatureRules {

    /** Per flag, the rules that win over Scalars' for the same key. */
    static final Map<Feature, Map<String, Scalars.Rule>> UNDER = new EnumMap<>(Feature.class);

    /** Flags with a consumer that is not a rule here. */
    private static final Set<Feature> LOWERER_CONSUMED = Set.of(
            Feature.LEGACY_SQL_NULL_UNSAFE_EQUALS, Feature.PUSH_DOWN_ENUM_TRANSFORM);

    private FeatureRules() {
    }

    static {
        for (String f : Pure.nativeKeysAt("substring")) {
            under(Feature.CORRECT_SQL_SUBSTRING_INDEXING, f, (n, args) -> {
                SqlExpr str = args.get(0);
                SqlExpr start = args.get(1);
                SqlExpr start1 = start instanceof SqlExpr.IntLit s
                        ? new SqlExpr.IntLit(s.value() + 1)
                        : SqlExpr.Call.of(SqlFn.PLUS, start, new SqlExpr.IntLit(1));
                if (args.size() == 2) {
                    return new SqlExpr.Call(SqlFn.SUBSTRING, List.of(str, start1));
                }
                SqlExpr end = args.get(2);
                SqlExpr length = start instanceof SqlExpr.IntLit s && end instanceof SqlExpr.IntLit e
                        ? new SqlExpr.IntLit(e.value() - s.value())
                        : SqlExpr.Call.of(SqlFn.MINUS, end, start);
                return new SqlExpr.Call(SqlFn.SUBSTRING, List.of(str, start1, length));
            });
        }
    }

    private static void under(Feature flag, String key, Scalars.Rule rule) {
        UNDER.computeIfAbsent(flag, k -> new HashMap<>()).put(key, rule);
    }

    /** The rule {@code features} select for {@code key}, or null for the plain one. */
    static Scalars.@com.legend.Nullable Rule select(String key, Set<Feature> features) {
        Scalars.Rule rule = null;
        for (Feature f : features) {
            Scalars.Rule flagged = UNDER.getOrDefault(f, Map.of()).get(key);
            if (flagged != null) {
                if (rule != null) {
                    throw new IllegalStateException("two feature flags select a rule for '" + key + "'");
                }
                rule = flagged;
            }
        }
        return rule;
    }

    /** Loud for a flag nothing in the lowering consumes. */
    static void requireConsumed(Set<Feature> features) {
        for (Feature f : features) {
            if (!LOWERER_CONSUMED.contains(f) && !UNDER.containsKey(f)) {
                throw new NotImplementedException("feature flag " + f
                        + " is not implemented by the platform (no lowering consumes it)");
            }
        }
    }
}
