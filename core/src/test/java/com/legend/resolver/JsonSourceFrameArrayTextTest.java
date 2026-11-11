// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedSourceUrl;
import com.legend.error.NotImplementedException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The JSON source frame is its payload's {@code data:} source relation (B2a, 2026-11-11,
 * docs/DATA_DELIVERY_DESIGN_2026_11_10.md §5): the resolver writes no row of the payload into the query -- the plan's
 * load makes a session table of its records ({@code planner.DataSources}, whose cut of the payload is pinned by
 * {@code DataSourcesRecordsTest}); until then, the frame's E3 pins cut the payload here, into a VALUES relation.
 */
@DisplayName("JsonSourceFrame: the payload's data: source (B2a)")
class JsonSourceFrameArrayTextTest {

    private static final String MODEL = "Class m::C { a: Integer[1]; b: String[0..1]; }";

    @Test
    @DisplayName("the frame reads the payload's data: source, each record's text and its place; no row in the query")
    void theFrameIsThePayloadsDataSource() {
        ModelContext ctx = Compiler.compileModel(MODEL);
        String url = "data:application/json,[{\"a\":1},{\"a\":2}]";
        ClassSource source = JsonSourceFrame.frame(ctx, "m::M", "m::C", url);
        assertTrue(source.pipeline() instanceof TypedSourceUrl u && u.url().equals(url),
                String.valueOf(source.pipeline()));
        assertEquals(List.of("data", TypedSourceUrl.ORDINAL),
                source.rowType().columns().stream().map(c -> c.name()).toList());
        // the class's properties are its bindings; the place is never one
        assertEquals(List.of("a", "b"), List.copyOf(source.bindings().keySet()));
    }

    @Test
    @DisplayName("a connection that is not a data:application/json literal is refused by name")
    void aRemoteUrlIsRefusedByName() {
        ModelContext ctx = Compiler.compileModel(MODEL);
        var e = assertThrows(NotImplementedException.class,
                () -> JsonSourceFrame.frame(ctx, "m::M", "m::C", "http://example.com/people.json"));
        assertTrue(e.getMessage().contains("is not a data:application/json literal"), e.getMessage());
    }
}
