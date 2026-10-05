// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertPath;
import static com.legend.protocol.Composing.elementPath;
import static com.legend.protocol.Composing.objOr;
import static com.legend.protocol.Composing.objs;

/**
 * {@code ###Data}'s data element as upstream prints it ({@code CorePureGrammarComposer.renderDataElement}).
 */
final class DataElementComposer {

    private DataElementComposer() {
    }

    static String dataElement(Json.Obj e) {
        StringBuilder b = new StringBuilder(DomainComposer.declarationPrefix("Data", "", e)).append(elementPath(e)).append("\n{\n");
        Json.Obj data = objOr(e, "data");
        if (data != null) {
            b.append(EmbeddedDataComposer.compose(data, TAB)).append("\n");
        }
        List<String> resolvers = new ArrayList<>();
        for (Json.Obj r : objs(e, "dataResolvers")) {
            resolvers.add(resolver(r));
        }
        if (!resolvers.isEmpty()) {
            b.append(String.join("\n", resolvers)).append("\n");
        }
        return b.append("}").toString();
    }

    private static String resolver(Json.Obj r) {
        String path = convertPath(r.getObj("elementPointer").getString("path"));
        String type = Composing.type(r);
        if ("referenceDataResolver".equals(type)) {
            return TAB + path + ";";
        }
        if ("baseDataResolver".equals(type)) {
            return TAB + path + ":\n" + EmbeddedDataComposer.compose(r.getObj("data"), TAB + TAB) + ";";
        }
        throw Composing.refused("no composer rule for a data resolver of _type '" + type + "'");
    }
}
