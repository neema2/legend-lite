// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import com.legend.base.Nullable;

/**
 * THE JUDGES' JSON GRAMMAR (register S144, the user's ruling of 2026-11-07, option 2): {@code assertJsonStringsEqual}'s
 * two texts are read as strict JSON -- a text that is not JSON is refused. On every route since B3 step 2 (2026-11-24):
 * upstream's json-simple grammar, the edge's form until then ({@code JsonSimpleGrammar}, census C1, 2026-11-17), is
 * deleted, and the two goldens it read past are cleaned at the edge's test-body door ({@code edge.UpstreamTestBodies}).
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
