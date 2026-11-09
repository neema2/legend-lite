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
}
