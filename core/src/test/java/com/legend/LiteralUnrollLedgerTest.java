// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.Repo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * WORLD_MAP §4 / TENET_CHARTER C6.2 — the unroll COMPARES, it never
 * COMPUTES. This pins the exact set of natives {@code LiteralUnroll} folds
 * over literals: every entry decides something visible in the program text
 * (list shape over a spelled list, identity of two spelled scalars of one
 * kind, a short-circuit on a spelled boolean). Adding a native that
 * produces a NEW value (toLower, plus, arithmetic, cross-kind equality) is
 * a tenet violation, not an optimization — the database computes it
 * through a residual CASE. Moving this set is a reviewed design decision.
 */
class LiteralUnrollLedgerTest {

    private static final Set<String> COMPARE_ONLY = Set.of(
            "instanceOf", "equal", "eq", "not", "and", "or", "in",
            "isEmpty", "isNotEmpty", "at",
            "toOne", "toOneMany", "first", "last",
            // the tail of a spelled list is list shape (as at/first/last);
            // so is the concatenation of two spelled lists (WORLD_MAP §4
            // names it; batch 55b: preOrderTraversal over a spelled tree)
            "tail", "init", "concatenate",
            // zip over two spelled lists is the spelled list of their pairs
            "zip",
            // batch 54 (WORLD_MAP §4 list shape / spelled maps): the size of
            // a spelled collection, same-kind membership (as `in`), and a
            // spelled map's pairs and lookup by a spelled key — structure
            // only, no new scalar value is computed
            "size", "contains", "keyValues", "get", "defaultIfEmpty",
            // a spelled-true assert is a no-op; an enumeration's values are
            // its declaration; dynamicNew over spelled keys is the instance literal
            "assert", "enumValues", "dynamicNew", "isTrue",
            // assertInstanceOf over a conforming literal = assert(true)
            "assertInstanceOf",
            // spelled-integer compares (same-kind identity, as equal/eq)
            "greaterThan", "lessThan", "greaterThanEqual", "lessThanEqual",
            // a spelled pair(a, b) IS an instance literal (first/second)
            "pair",
            // batch 74: sqlNull is a SHAPE test inside the equality fold
            // (is the operand the TDS null carrier?) — it produces no value
            "sqlNull");

    @Test
    @DisplayName("LiteralUnroll folds compare-only natives (the pinned set)")
    void foldSetIsCompareOnly() throws Exception {
        String src = Files.readString(Repo.module("src/main/java/com/legend/compiler/spec/LiteralUnroll.java"));
        Matcher m = Pattern.compile("is\\(c, \"(\\w+)\"\\)").matcher(src);
        Set<String> found = new TreeSet<>();
        while (m.find()) {
            found.add(m.group(1));
        }
        // the TYPED form (upstream boundary batch 4b): a fold keyed on a
        // NativeFn family member — resolve the constant to its bare name
        // through the enum itself, never a second spelling
        Matcher t = Pattern.compile("NativeFn\\.(\\w+)\\.of\\(c\\.callee\\(\\)\\.qualifiedName\\(\\)\\)"
                + "\\.orElse\\(null\\) == com\\.legend\\.builtin\\.NativeFn\\.(\\w+)\\.(\\w+)").matcher(src);
        while (t.find()) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            Enum<?> member = Enum.valueOf((Class<Enum>) Class.forName(
                    "com.legend.builtin.NativeFn$" + t.group(2)), t.group(3));
            found.add(((com.legend.builtin.NativeFn.Member) member).bareName());
        }
        assertEquals(new TreeSet<>(COMPARE_ONLY), found,
                "LiteralUnroll's fold set moved — WORLD_MAP §4: the unroll compares,"
                        + " the database computes; a value-producing fold is not admitted");
    }
}
