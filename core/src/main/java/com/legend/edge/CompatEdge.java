// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.dialect.RawSqlBoundary;
import com.legend.testexecutionplan.Edge;
import com.legend.testexecutionplan.Extension;

import java.util.ArrayList;
import java.util.List;

/**
 * THE COMPATIBILITY EDGE (docs/PLAN_AS_DATA_2026_11_07.md 2.7, the design's step 8): what a run under the compatibility
 * rules hands the test road, holding the referee's ledger ({@link RawSqlBoundary.Recorder}). At planning it plans each
 * effect's and seed's ledger texts into the test plan ({@link EffectTexts}, {@link SeedTexts}); at run it records them
 * as each step's events say ({@link #seeded}, {@link #effectRan}). Lite's own runs hand no edge, and plan nothing for it.
 */
public final class CompatEdge implements Edge {

    private final RawSqlBoundary.Recorder recorder;

    public CompatEdge(RawSqlBoundary.Recorder recorder) {
        this.recorder = recorder;
    }

    /** The ledger this edge records into: the referee replays the goldens over it. */
    public RawSqlBoundary.Recorder recorder() {
        return recorder;
    }

    @Override
    public List<Extension> effect(List<String> texts, List<Boolean> queries) {
        if (texts.size() != queries.size()) {
            throw new IllegalArgumentException("an effect's ledger texts and their query marks differ in number");
        }
        List<EffectTexts.Statement> statements = new ArrayList<>(texts.size());
        for (int i = 0; i < texts.size(); i++) {
            statements.add(new EffectTexts.Statement(texts.get(i), queries.get(i)));
        }
        return List.of(new EffectTexts(statements));
    }

    @Override
    public List<Extension> seed(List<String> texts) {
        return List.of(new SeedTexts(texts));
    }

    /** {@inheritDoc} Recorded as nothing: the referee replays legend-engine's text, which reads no table of lite's. */
    @Override
    public List<Extension> liveSource() {
        return List.of(new LiveSourceLoad());
    }

    @Override
    public void seeded(List<Extension> planned, List<Boolean> answeredRows) {
        if (planned.equals(List.of(new LiveSourceLoad()))) {
            return;   // a live source's session table: nothing of it is the referee's ledger
        }
        SeedTexts texts = seedOf(planned);
        if (answeredRows.size() > texts.texts().size()) {
            throw new IllegalStateException("a seed step ran " + answeredRows.size() + " statement(s) with "
                    + texts.texts().size() + " ledger text(s) planned at it");
        }
        for (int i = 0; i < answeredRows.size(); i++) {
            recorder.recordExecuted(texts.texts().get(i), answeredRows.get(i));
        }
    }

    @Override
    public void effectRan(List<Extension> planned) {
        for (EffectTexts.Statement s : effectOf(planned).statements()) {
            recorder.recordExecuted(s.text(), s.query());
        }
    }

    /** The one seed record planned at a step: a step that ran with none planned is refused by name. */
    private static SeedTexts seedOf(List<Extension> planned) {
        SeedTexts found = null;
        for (Extension e : planned) {
            if (e instanceof SeedTexts t) {
                if (found != null) {
                    throw new IllegalStateException("two seed ledger records planned at one step");
                }
                found = t;
            }
        }
        if (found == null) {
            throw new IllegalStateException("a seed step ran with no ledger texts planned at it");
        }
        return found;
    }

    /** The one effect record planned at a step: a step that ran with none planned is refused by name. */
    private static EffectTexts effectOf(List<Extension> planned) {
        EffectTexts found = null;
        for (Extension e : planned) {
            if (e instanceof EffectTexts t) {
                if (found != null) {
                    throw new IllegalStateException("two effect ledger records planned at one step");
                }
                found = t;
            }
        }
        if (found == null) {
            throw new IllegalStateException("an effect step ran with no ledger texts planned at it");
        }
        return found;
    }
}
