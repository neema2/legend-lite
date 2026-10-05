// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import static com.legend.protocol.Composing.TAB;

/**
 * A test assertion ({@code id: Kind #{ ... }#}) as upstream prints it in a mapping's or a service's test
 * suites ({@code HelperTestAssertionGrammarComposer}): {@code EqualTo}, {@code EqualToJson}, {@code Relation}.
 */
final class TestAssertionComposer {

    private TestAssertionComposer() {
    }

    /** {@code composeTestAssertion} at the context's indentation {@code i}. */
    static String compose(Json.Obj assertion, String i) {
        String indented = i + TAB;
        String inner = indented + TAB;
        String type = Composing.type(assertion);
        String keyword;
        String content;
        if ("equalTo".equals(type)) {
            keyword = "EqualTo";
            content = inner + "expected:\n" + inner + TAB + Composing.valueSpecification(assertion.get("expected"), inner + TAB) + ";";
        } else if ("equalToJson".equals(type)) {
            keyword = "EqualToJson";
            content = inner + "expected:\n" + EmbeddedDataComposer.compose(assertion.getObj("expected"), inner + TAB) + ";";
        } else if ("equalToRelation".equals(type)) {
            keyword = "Relation";
            content = EmbeddedDataComposer.alignedRelation(assertion.getObj("expected"), inner, false);
        } else {
            throw Composing.refused("no composer rule for a test assertion of _type '" + type + "'");
        }
        return i + Composing.convertIdentifier(assertion.getString("id")) + ":\n"
                + indented + keyword + "\n" + indented + "#{\n" + content + "\n" + indented + "}#";
    }
}
