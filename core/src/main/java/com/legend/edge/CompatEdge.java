// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.executionplan.DataRef;
import com.legend.executionplan.DataStores;
import com.legend.executionplan.ExecutionPlan;
import com.legend.sql.dialect.RawSqlBoundary;
import com.legend.testexecutionplan.Edge;
import com.legend.testexecutionplan.Extension;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * THE COMPATIBILITY EDGE (docs/PLAN_AS_DATA_2026_11_07.md 2.7, the design's step 8): what a run under the compatibility
 * rules hands the test road, holding the referee's ledger ({@link RawSqlBoundary.Recorder}). At planning it plans each
 * effect's ledger texts and each seed's referee's copy into the test plan ({@link EffectTexts}, {@link RefereeLoad});
 * at run it records them as each step's events say ({@link #seeded}, {@link #effectRan}). Lite's own runs hand no edge,
 * and plan nothing for it.
 */
public final class CompatEdge implements Edge {

    private final RawSqlBoundary.Recorder recorder;
    private final DataStores stores;

    /** {@code stores}: the run's own, which resolve a file the plan's loads read by its reference (the referee reads
     *  the same verified bytes, B2b). */
    public CompatEdge(RawSqlBoundary.Recorder recorder, DataStores stores) {
        this.recorder = recorder;
        this.stores = stores;
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
    public List<Extension> seed(List<ExecutionPlan.LoadStep> steps, Map<String, String> content) {
        return List.of(new RefereeLoad(steps, content));
    }

    @Override
    public void seeded(List<Extension> planned, List<Boolean> answeredRows) {
        RefereeLoad copy = seedOf(planned);
        if (answeredRows.size() > copy.steps().size()) {
            throw new IllegalStateException("a seed's load ran " + answeredRows.size() + " step(s) with "
                    + copy.steps().size() + " step(s) of the referee's copy planned at it");
        }
        // the referee's copy of each step that ran, in order: a statement as it answered, the rows' blob each insert, a
        // file the referee's own statements over the bytes the plan's load read
        for (int i = 0; i < answeredRows.size(); i++) {
            switch (copy.steps().get(i)) {
                case ExecutionPlan.LoadStep.Statement s -> recorder.recordExecuted(s.sql(), answeredRows.get(i));
                case ExecutionPlan.LoadStep.Statements s -> {
                    for (String insert : DataRef.Statements.decode(java.util.Objects.requireNonNull(
                            copy.content().get(s.ref().digest()), "the referee's copy holds every blob it reads"))) {
                        recorder.recordExecuted(insert, false);
                    }
                }
                case ExecutionPlan.LoadStep.File f -> {
                    switch (f.delivery()) {
                        case ExecutionPlan.LoadStep.Delivery.Reads reads -> recorder.recordRead(
                                new RawSqlBoundary.FileRead(bytes(f, copy), reads.statements(), reads.after()));
                    }
                }
                // a host's statement, bound from its own store (B3's Q3): a HOST_SESSION load's alone, never a seed's
                case ExecutionPlan.LoadStep.Bound b -> throw new IllegalStateException("the referee's copy binds a"
                        + " statement from a host's store, which no seed holds: " + b.sql());
                // a host's snapshot stream or attached value: HOST_SESSION steps, never a seed's
                case ExecutionPlan.LoadStep.Stream s -> throw new IllegalStateException("the referee's copy streams the"
                        + " remote plane " + s.plane() + ", which no seed holds");
                case ExecutionPlan.LoadStep.Attach a -> throw new IllegalStateException("the referee's copy attaches"
                        + " the host's value " + a.held() + ", which no seed holds");
            }
        }
    }

    /** The verified bytes of the file {@code f} names: a content text from the copy's bundle, any other through the
     *  run's stores, its digest the reference's -- the bytes the plan's load read, refused by name if they are not. */
    private byte[] bytes(ExecutionPlan.LoadStep.File f, RefereeLoad copy) {
        DataRef ref = f.ref();
        byte[] bytes = ref.namespace() == DataRef.Namespace.CONTENT
                ? java.util.Objects.requireNonNull(copy.content().get(ref.digest()),
                        "the referee's copy holds every content text it reads").getBytes(
                        java.nio.charset.StandardCharsets.UTF_8)
                : stores.read(ref.namespace(), ref.name());
        if (bytes.length != ref.size() || !DataStores.sha256(bytes).equals(ref.digest())) {
            throw new IllegalStateException("the referee's copy of the file '" + ref.name() + "' is not the data the"
                    + " plan's load read: " + bytes.length + " bytes of digest " + DataStores.sha256(bytes)
                    + ", where the plan names " + ref.size() + " bytes of digest " + ref.digest());
        }
        return bytes;
    }

    @Override
    public void effectRan(List<Extension> planned) {
        for (EffectTexts.Statement s : effectOf(planned).statements()) {
            recorder.recordExecuted(s.text(), s.query());
        }
    }

    /** The one referee's copy planned at a step: a step that ran with none planned is refused by name. */
    private static RefereeLoad seedOf(List<Extension> planned) {
        RefereeLoad found = null;
        for (Extension e : planned) {
            if (e instanceof RefereeLoad t) {
                if (found != null) {
                    throw new IllegalStateException("two referee's copies planned at one seed step");
                }
                found = t;
            }
        }
        if (found == null) {
            throw new IllegalStateException("a seed step ran with no referee's copy planned at it");
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
