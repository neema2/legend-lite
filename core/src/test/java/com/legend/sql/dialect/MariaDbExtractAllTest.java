// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MARIADB'S (AND MYSQL'S) {@code regexp_extract_all} READS A PATTERN OF ONE MATCH LENGTH ALONE (the independent audit of
 * 2026-11-26). Its k-th match is found by asking whether the text splits into k matches, which is the left-to-right
 * scan's k-th only where every match has one length: {@code 'abc'->chunk(5)}, pattern {@code .{1,5}}, answered
 * {@code ['abc', 'c', 'c']}. A pattern whose matches may differ in length is refused by name; the check errs toward
 * refusing.
 */
class MariaDbExtractAllTest {

    @Test
    void aPatternOfOneMatchLengthIsRead_andEveryOtherIsNot() {
        for (String one : List.of(".{1,1}", ".", "[ab]c", "\\d{3}", "(?i)ab", "(?:ab)c", "a{2,2}", "\\+\\*", "[*+?|]x")) {
            assertTrue(MariaDb.oneMatchLength(one), one);
        }
        for (String varies : List.of(".{1,5}", "a+", "a*", "ab?", "ab|c", "a{2,}", "a{1,3}", "(?:ab)+")) {
            assertFalse(MariaDb.oneMatchLength(varies), varies);
        }
    }
}
