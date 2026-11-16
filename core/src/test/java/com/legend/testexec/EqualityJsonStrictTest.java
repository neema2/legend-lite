// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;


import com.legend.sql.Json;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/** {@code assertJsonStringsEqual}: an object never equals a one-element array holding it (json-simple's equals,
 *  CompiledSupport.jsonStringEquals; the user's ruling, 2026-11-03: the host's {@code [x] ≡ x} root unwrap is
 *  deleted) -- under the ordered compare and under an unsorted chain's root multiset alike
 *  ({@link EqualityJsonUnorderedRootTest}). */
class EqualityJsonStrictTest {

    private static final String B = "{\"legalName\":\"Firm B\",\"employees\":[{\"lastName\":\"Bala\"},{\"lastName\":\"Raman\"}]}";

    @Test
    @DisplayName("an object never equals a one-element array holding it")
    void anObjectIsNoArray() {
        assertNotNull(Equality.pureJson(Json.parse(B), Json.parse("[" + B + "]")));
        assertNotNull(Equality.pureJsonUnorderedRoot(Json.parse(B), Json.parse("[" + B + "]")));
        assertNotNull(Equality.pureJsonUnorderedRoot(Json.parse("[" + B + "]"), Json.parse(B)));
    }

    /** The service-test EqualToJson compares numbers as exact decimals, as the engine's JsonNodeComparator does
     *  (decimalValue()): two doubles one ULP apart differ, and two spellings of one decimal are equal (E-Java's
     *  ledger audit D1, 2026-11-16: an unconditional 2-ULP leniency had passed the first pair). */
    @Test
    @DisplayName("EqualToJson: numbers as exact decimals, no float leniency")
    void serviceJsonComparesExactDecimals() {
        assertNotNull(com.legend.test.TestAssertions.equalToJson(Json.parse("{\"x\": 1.0}"),
                Json.parse("{\"x\": 1.0000000000000002}")));
        org.junit.jupiter.api.Assertions.assertNull(com.legend.test.TestAssertions.equalToJson(
                Json.parse("{\"x\": [1, 2.50]}"), Json.parse("{\"x\": [2.5, 1.0]}")));
    }
}
