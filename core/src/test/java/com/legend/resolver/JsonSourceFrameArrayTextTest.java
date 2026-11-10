// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSourceUrl;
import com.legend.error.NotImplementedException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The JSON source frame's payload is the plan's data, never literal rows (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §4, A5):
 * the frame is the payload's {@code sourceUrl} relation -- its JSON text and each element's place, the frame's row
 * identity ({@link TypedSourceUrl#ORDINAL}) -- which the planner's load makes a session table of. Until A5 the frame cut
 * the payload into objects itself ({@code objectTexts}) and wrote them as the statement's literal rows; the split is the
 * planner's one splitter now ({@code planner.DataSourcesElementsTest} carries this class's split cases).
 */
@DisplayName("JsonSourceFrame: the payload's relation (A5)")
class JsonSourceFrameArrayTextTest {

    private static final String MODEL = "###Pure\nClass m::C { a: Integer[1]; b: String[0..1]; }\n";

    @Test
    @DisplayName("the frame reads its payload as a sourceUrl relation of the JSON text and the element's place")
    void theFrameIsThePayloadsRelation() {
        ModelContext ctx = Compiler.compileModel(MODEL);
        String url = "data:application/json,[{\"a\":1},{\"a\":2}]";
        ClassSource frame = JsonSourceFrame.sourceUrlFrame(ctx, "m::M", "m::C", url);
        TypedSourceUrl source = assertInstanceOf(TypedSourceUrl.class, frame.pipeline());
        assertEquals(url, source.url(), "the payload rides the source, whole");
        Type.RelationType row = Type.requireRelationSchema(source.info().type());
        assertEquals(List.of("data", TypedSourceUrl.ORDINAL), row.columns().stream().map(Type.Column::name).toList());
        assertEquals(java.util.Set.of("a", "b"), frame.bindings().keySet(), "each scalar property a typed extraction");
    }

    @Test
    @DisplayName("a payload that is no data:application/json literal is refused by name")
    void anotherSourceIsRefused() {
        ModelContext ctx = Compiler.compileModel(MODEL);
        var e = assertThrows(NotImplementedException.class,
                () -> JsonSourceFrame.sourceUrlFrame(ctx, "m::M", "m::C", "http://example.com/x.json"));
        assertTrue(e.getMessage().contains("not a data:application/json literal"), e.getMessage());
    }
}
