// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.database.PlanStyle;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.dialect.SqlDialect;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * ONE PLANNING CALL'S RECORDER OF LEGEND-PURE'S WORDS OF ITS RAISE SITES (K10b of the edge audit, the orchestrator's
 * ruling (A) of 2026-11-17). Its template's style ({@link PureText#UPSTREAM} or {@link PureText#PURE_V1}) in every
 * answer; each statement the planner offers ({@link #siteWords}) is recorded with each of its sites' words, keyed by
 * {@code (statement text, n)}. THE STATE IS THE EDGE'S ALONE: minted per planning call by the door that reads it
 * ({@link PureText#recording}), never held in a static, a long-lived field or a cache; read by that door EXACTLY ONCE
 * ({@link #read}), after which it is SEALED and a later offer or read is refused by name.
 *
 * <p>Two offers of one key with different words are refused by name at planning (never overwritten); equal words are
 * one record. A site whose operands are not literals the edge words at planning (a product dialect's site under the
 * edge's style: an edge dialect writes such a raise's sentence in SQL, {@link PureMessages#wordable}) is recorded with
 * no words: its run's lookup is refused by name ({@code Appeals#raisedWords}).
 */
public final class SiteWordsRecorder implements PlanStyle {

    private final PlanStyle template;
    /** Each offered statement's text to its sites' words by number, in the order offered. */
    private final Map<String, Map<Integer, String>> table = new LinkedHashMap<>();
    /** Set by the door's one read: no offer or read after it. */
    private final java.util.concurrent.atomic.AtomicBoolean sealed = new java.util.concurrent.atomic.AtomicBoolean();

    SiteWordsRecorder(PlanStyle template) {
        this.template = java.util.Objects.requireNonNull(template, "the recorder's template style");
    }

    /** Records legend-pure's words of each of {@code sites}, the sites of the statement {@code text}. */
    @Override
    public void siteWords(String text, List<com.legend.sql.dialect.RaiseSite> sites) {
        java.util.Objects.requireNonNull(text, "the offered statement's text");
        if (sealed.get()) {
            throw new IllegalStateException("a raise site's words offered to a recorder its door has already read"
                    + " (sealed): a planning call's recorder is read once, after its planning");
        }
        for (com.legend.sql.dialect.RaiseSite site : sites) {
            String words = PureMessages.wordsOrNull(site.kind(), site.operands());
            if (words == null) {
                continue;   // not worded at planning: the run's lookup of it is refused by name
            }
            Map<Integer, String> byN = table.computeIfAbsent(text, t -> new LinkedHashMap<>());
            String held = byN.get(site.n());
            if (held != null && !held.equals(words)) {
                throw new IllegalStateException("two raise sites keyed (statement text, " + site.n() + ") are worded"
                        + " differently ('" + held + "' and '" + words + "'): one key, one site's words -- of the"
                        + " statement: " + text);
            }
            byN.put(site.n(), words);
        }
    }

    /** THE DOOR'S ONE READ: each offered statement's text to its sites' words by number, in the order offered; the
     *  recorder sealed. */
    public Map<String, Map<Integer, String>> read() {
        if (!sealed.compareAndSet(false, true)) {
            throw new IllegalStateException("a recorder of raise sites' words read twice: its door reads it once,"
                    + " after its planning");
        }
        Map<String, Map<Integer, String>> out = new LinkedHashMap<>();
        table.forEach((text, byN) -> out.put(text, java.util.Collections.unmodifiableMap(new LinkedHashMap<>(byN))));
        return java.util.Collections.unmodifiableMap(out);
    }

    @Override
    public SqlDialect dialect(DatabaseType type) {
        return template.dialect(type);
    }

    @Override
    public @com.legend.base.Nullable TraceCell traceCell() {
        return template.traceCell();
    }

    @Override
    public <F> F twoForm(Class<F> kind, F productForm) {
        return template.twoForm(kind, productForm);
    }

    @Override
    public String graphKeyDate(String written) {
        return template.graphKeyDate(written);
    }

    @Override
    public String graphKeyString(String value) {
        return template.graphKeyString(value);
    }

    @Override
    public @com.legend.base.Nullable String errorWords(RuntimeException e) {
        return template.errorWords(e);
    }

    @Override
    public <F> F facet(Class<F> kind) {
        return template.facet(kind);
    }

    @Override
    public boolean judgesEngineText() {
        return template.judgesEngineText();
    }

    @Override
    public <F> F facet(Class<F> kind, java.util.function.Supplier<? extends RuntimeException> refusal) {
        return template.facet(kind, refusal);
    }
}
