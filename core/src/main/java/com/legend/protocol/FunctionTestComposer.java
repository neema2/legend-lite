// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

/**
 * A function's test suites, as upstream prints them after its body
 * ({@code HelperDomainGrammarComposer.renderFunctionTestSuites}).
 */
final class FunctionTestComposer {

    private FunctionTestComposer() {
    }

    static String testSuites(Json.Obj function) {
        if (Composing.items(function, "tests").isEmpty()) {
            return "";
        }
        throw Composing.refused("no composer rule yet for a function's test suites (_type 'function', tests)");
    }
}
