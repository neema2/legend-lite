// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.base.Nullable;
import com.legend.json.Json;
import com.legend.executionplan.Extension;
import com.legend.executionplan.ExtensionCodec;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The JSON of the compatibility edge's records (docs/PLAN_AS_DATA_2026_11_07.md 2.7), registered for the plans' formats
 * (the test plan's, and a product plan's raise sites since S1 of the presentation program, 2026-11-19) through
 * {@code META-INF/services}: each record an object tagged by its kind.
 */
public final class EdgeCodec implements ExtensionCodec {

    /** The codec the edge hands a plan's writer where the classpath's loader finds none (WebAssembly's: the host
     *  halves' plans carry their sites' words; S1 of the presentation program, 2026-11-19). */
    static final EdgeCodec INSTANCE = new EdgeCodec();

    private static final String TAG = "_type";

    /** The tags of the edge's records, one spelling for the writer and the reader. */
    private enum Tag { effectTexts, refereeLoad, appeal, engineText, siteWords }

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
            case RefereeLoad r -> {
                o.put(TAG, Tag.refereeLoad.name());
                List<Object> steps = new ArrayList<>(r.steps().size());
                for (com.legend.executionplan.ExecutionPlan.LoadStep step : r.steps()) {
                    steps.add(com.legend.executionplan.PlanJson.loadStepJson(step));
                }
                o.put("steps", steps);
                // the bundle inline, sorted by digest: the edge's data, one blob of inserts per table
                o.put("content", new java.util.TreeMap<String, Object>(r.content()));
            }
            case Appeal a -> {
                // an engine-text assert's appeal (step 8b; moved to the edge with C2, 2026-11-17)
                o.put(TAG, Tag.appeal.name());
                o.putAll(AppealJson.wAppeal(a));
            }
            case EngineTextJudgment j -> {
                // an engine-text assert's comparison, held by its verdict's EdgeJudged judgment (census C3, 2026-11-17)
                o.put(TAG, Tag.engineText.name());
                o.putAll(AppealJson.wEngineTextJudgment(j));
            }
            case SiteWords w -> {
                // an upstream mode's words of a raise site, planned, the site's own record (K1, S1, 2026-11-19)
                o.put(TAG, Tag.siteWords.name());
                o.put("words", w.words());
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
            case refereeLoad -> {
                List<com.legend.executionplan.ExecutionPlan.LoadStep> steps = new ArrayList<>();
                for (Json.Node n : o.getArr("steps").items()) {
                    steps.add(com.legend.executionplan.PlanJson.loadStepOf((Json.Obj) n));
                }
                yield new RefereeLoad(steps, com.legend.executionplan.PlanJson.contentOf(o.getObj("content"),
                        "the referee's copy of a load"));
            }
            case appeal -> AppealJson.rAppeal(o);
            case engineText -> AppealJson.rEngineTextJudgment(o);
            case siteWords -> new SiteWords(o.getString("words"));
        };
    }
}
