// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * The string entry's ({@code meta::legend::executeLegendQuery}) value, and legend-pure's toJSON documents of a
 * relation, BY EMISSION over the query chain, lowered as ONE scalar subquery over the chain's rows. {@code recorded}
 * is the text the plan style recorded when it planned a STYLED value (null where none, and on every other kind).
 */
public record TypedJsonResult(TypedSpec chain, Kind kind,
        @com.legend.base.Nullable String recorded, ExprType info) implements TypedSpec {

    /** TDS_JSON: the bare {@code toJSON(tds)} document — {@code {"columns":[{name,type, metaType}],"rows":[{"values":
     *  [..]}]}} (toJSON.pure's TabularDataSet arm: type = the column type's path, metaType = PrimitiveType /
     *  Enumeration / InvalidType, empty when the type is unknown). TDS_JSON_KV: the bare
     *  {@code tdsToJSONKeyValueObjectString(tds)} document — {@code [{"col":value,...},...]} (toJSON.pure:231
     *  tdsRowToJSONKeyValueObject: one object per row keyed by column name; {@code []} when empty). STRING_ENTRY: the
     *  text a plan style wrote around a serialized root, {@code chain} the whole text (train 17 item 2, 2026-11-18).
     *  STYLED: a value the plan style writes over the root ({@code lowering.ResultEnvelopes}: what to read of the root
     *  and the envelope are its; L-B.TypedJsonResult, the orchestrator's ruling (A) of 2026-11-18 -- legend-engine's
     *  TDS and CLASS envelopes and their activity SQL were product kinds until then). */
    public enum Kind { TDS_JSON, TDS_JSON_KV, STRING_ENTRY, STYLED }

    @Override
    public List<TypedSpec> children() {
        return List.of(chain);
    }

    @Override
    public TypedSpec withChildren(List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 1, "TypedJsonResult");
        return new TypedJsonResult(kids.get(0), kind, recorded, info);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedJsonResult(chain, kind, recorded, info);
    }
}
