// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * The engine's RESULT JSON of a TDS- or class-rooted query — the string
 * entry's ({@code meta::legend::executeLegendQuery}) value for those
 * roots, BY EMISSION over the query chain: {@code {"builder":{"_type":
 * "tdsBuilder","columns":[{name,type}…]},"activities":[{"_type":
 * "relational","sql":…}],"result":{"columns":[…],"rows":[{"values":
 * [...]}…]}}} (engine TDSResult serialization), or the classBuilder /
 * {@code objects} form for a class root. {@code sql} is the activity's
 * SQL text (the engine-style render of the same chain), null when no
 * render exists — the activities array is then empty. Lowered as ONE
 * scalar subquery aggregating the chain's rows.
 */
public record TypedJsonResult(TypedSpec chain, Kind kind,
        @com.legend.base.Nullable String sql, ExprType info) implements TypedSpec {

    /** TDS / CLASS: the executeLegendQuery result envelope; TDS_JSON: the
     * bare {@code toJSON(tds)} document — {@code {"columns":[{name,type,
     * metaType}],"rows":[{"values":[..]}]}} (toJSON.pure's TabularDataSet
     * arm: type = the column type's path, metaType = PrimitiveType /
     * Enumeration / InvalidType, empty when the type is unknown). */
    /** TDS_JSON_KV: the bare {@code tdsToJSONKeyValueObjectString(tds)}
     * document — {@code [{"col":value,...},...]} (toJSON.pure:231
     * tdsRowToJSONKeyValueObject: one object per row keyed by column
     * name; {@code []} when empty). */
    public enum Kind { TDS, CLASS, TDS_JSON, TDS_JSON_KV }

    @Override
    public List<TypedSpec> children() {
        return List.of(chain);
    }

    @Override
    public TypedSpec withChildren(List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 1, "TypedJsonResult");
        return new TypedJsonResult(kids.get(0), kind, sql, info);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedJsonResult(chain, kind, sql, info);
    }
}
