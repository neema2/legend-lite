// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedJsonResult;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.List;

/**
 * legend-engine's JSON-BUILDER ENVELOPE around a serialized root's text (train 17 item 2, 2026-11-18: moved from the
 * compiler's VALUES kind and the lowering's ResultEnvelopes.values): the edge's form of the planner's
 * {@link com.legend.planner.ExecutedValues} kind -- {@code {"builder":{"_type":"json"},"values":} + the text +
 * {@code }}, as legend-engine's result serializer writes it.
 */
enum EngineExecutedValues implements com.legend.planner.ExecutedValues {
    INSTANCE;

    @Override
    public TypedSpec envelope(TypedSpec chain, ModelContext model) {
        ExprType str = ExprType.one(Type.Primitive.STRING);
        TypedSpec text = com.legend.compiler.spec.ExecuteChainAssembly.concatenated(model, List.of(
                new TypedCString("{\"builder\":{\"_type\":\"json\"},\"values\":", str), chain,
                new TypedCString("}", str)));
        return new TypedJsonResult(text, TypedJsonResult.Kind.STRING_ENTRY, null, str);
    }
}
