// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertPath;

/**
 * {@code ###Data}'s data element as upstream prints it ({@code CorePureGrammarComposer.renderDataElement}) -- over
 * the record ({@link Protocol.PDataElement}; the protocol program's leg 2, step 3).
 */
final class DataElementComposer {

    private DataElementComposer() {
    }

    static String dataElement(Protocol.PDataElement e) {
        StringBuilder b = new StringBuilder(DomainComposer.declarationPrefix("Data", "", e.stereotypes(), e.taggedValues()))
                .append(Composing.elementPath(e.pkg(), e.name())).append("\n{\n");
        if (e.body().value() != null) {
            b.append(EmbeddedDataComposer.compose(e.body().value(), TAB)).append("\n");
        }
        List<String> resolvers = new ArrayList<>();
        for (Protocol.PDataResolver r : e.body().resolvers()) {
            resolvers.add(resolver(r));
        }
        if (!resolvers.isEmpty()) {
            b.append(String.join("\n", resolvers)).append("\n");
        }
        return b.append("}").toString();
    }

    /** {@link #dataElement(Protocol.PDataElement)} of the JSON, read first. */
    static String dataElement(Json.Obj e) {
        return dataElement(Composing.element(e, Protocol.PDataElement.class));
    }

    /** A reference resolver ({@code path;}) carries no data; a base resolver its data block. */
    private static String resolver(Protocol.PDataResolver r) {
        String path = convertPath(r.elementPointer().path());
        Protocol.PEmbeddedDataValue data = r.data();
        return data == null ? TAB + path + ";" : TAB + path + ":\n" + EmbeddedDataComposer.compose(data, TAB + TAB) + ";";
    }
}
