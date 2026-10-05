// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

/**
 * A mapping's tests as upstream prints them: the legacy {@code MappingTests} and the {@code testSuites}
 * ({@code HelperMappingGrammarComposer.renderMappingTest} and {@code renderMappingTestSuite}).
 */
final class MappingTestComposer {

    private MappingTestComposer() {
    }

    static String legacyTest(Json.Obj test) {
        throw Composing.refused("no composer rule yet for a mapping's legacy tests (_type 'mapping', tests)");
    }

    static String testSuite(Json.Obj suite) {
        throw Composing.refused("no composer rule yet for a mapping's test suites (_type 'mapping', testSuites)");
    }
}
