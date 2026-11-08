// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.base.Nullable;
import com.legend.json.Json;
import com.legend.testexecutionplan.Extension;
import com.legend.testexecutionplan.ExtensionCodec;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The JSON of the compatibility edge's records (docs/PLAN_AS_DATA_2026_11_07.md 2.7), registered for the test plan's
 * format through {@code META-INF/services}: each record an object tagged by its kind.
 */
public final class EdgeCodec implements ExtensionCodec {

    private static final String TAG = "_type";

    /** The tags of the edge's records, one spelling for the writer and the reader. */
    private enum Tag { effectTexts, seedTexts }

    @Override
    public @Nullable Map<String, Object> write(Extension e) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (e) {
            case EffectTexts t -> {
                o.put(TAG, Tag.effectTexts.name());
                List<Object> statements = new ArrayList<>(t.statements().size());
                for (EffectTexts.Statement s : t.statements()) {
                    Map<String, Object> so = new LinkedHashMap<>();
                    so.put("text", s.text());
                    so.put("query", s.query());
                    statements.add(so);
                }
                o.put("statements", statements);
            }
            case SeedTexts t -> {
                o.put(TAG, Tag.seedTexts.name());
                o.put("texts", t.texts());
            }
            default -> {
                return null;   // another codec's record
            }
        }
        return o;
    }

    @Override
    public @Nullable Extension read(String tag, Json.Obj o) {
        Tag t = null;
        for (Tag each : Tag.values()) {
            if (each.name().equals(tag)) {
                t = each;
            }
        }
        if (t == null) {
            return null;   // another codec's tag
        }
        return switch (t) {
            case effectTexts -> {
                List<EffectTexts.Statement> statements = new ArrayList<>();
                for (Json.Node n : o.getArr("statements").items()) {
                    Json.Obj so = (Json.Obj) n;
                    statements.add(new EffectTexts.Statement(so.getString("text"), so.getBool("query")));
                }
                yield new EffectTexts(statements);
            }
            case seedTexts -> new SeedTexts(o.getStringArray("texts"));
        };
    }
}
