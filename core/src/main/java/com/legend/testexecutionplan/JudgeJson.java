// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import com.legend.base.Nullable;

/**
 * THE PRODUCT'S FORM OF {@link JsonGrammar} (register S144, the user's ruling of 2026-11-07, option 2): lite's own runs
 * read {@code assertJsonStringsEqual}'s two texts as strict JSON -- a text that is not JSON is refused. Upstream's
 * json-simple grammar is the edge's form ({@code com.legend.edge.JsonSimpleGrammar}; census C1, 2026-11-17).
 */
public enum JudgeJson implements JsonGrammar {
    STRICT;

    @Override
    public @Nullable Object read(String text) {
        try {
            com.legend.json.Json.parse(text);
        } catch (IllegalArgumentException notJson) {
            throw new IllegalStateException("not JSON: " + notJson.getMessage(), notJson);
        }
        return com.legend.sql.Json.parse(text);
    }
}
