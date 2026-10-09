// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import static com.legend.protocol.Composing.TAB;

/**
 * A test assertion ({@code id: Kind #{ ... }#}) as upstream prints it in a mapping's or a service's test
 * suites ({@code HelperTestAssertionGrammarComposer}): {@code EqualTo}, {@code EqualToJson}, {@code Relation} -- over
 * the record ({@link Protocol.PTestAssertion}; the protocol program's leg 2, step 3).
 */
final class TestAssertionComposer {

    private TestAssertionComposer() {
    }

    /** {@code composeTestAssertion} at the context's indentation {@code i}. */
    static String compose(Protocol.PTestAssertion assertion, String i) {
        String indented = i + TAB;
        String inner = indented + TAB;
        String keyword;
        String content;
        switch (assertion.expected()) {
            case Protocol.PEqualToValue v -> {
                keyword = "EqualTo";
                content = inner + "expected:\n" + inner + TAB + Composing.valueSpecification(v.value(), inner + TAB) + ";";
            }
            case Protocol.PExternalFormatData e -> {
                keyword = "EqualToJson";
                content = inner + "expected:\n" + EmbeddedDataComposer.compose(e, inner + TAB) + ";";
            }
            case Protocol.PRelationElement r -> {
                keyword = "Relation";
                content = EmbeddedDataComposer.alignedRelation(r, inner, false);
            }
        }
        return i + Composing.convertIdentifier(assertion.id()) + ":\n"
                + indented + keyword + "\n" + indented + "#{\n" + content + "\n" + indented + "}#";
    }

    /** {@link #compose(Protocol.PTestAssertion, String)} of the JSON, read first. */
    static String compose(Json.Obj assertion, String i) {
        return compose(EmbeddedDataReader.assertion(assertion), i);
    }
}
