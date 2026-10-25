// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.cache.Hash;
import com.legend.cache.HeldStore;
import com.legend.compiler.element.ModelContext;

import java.util.function.Function;

/**
 * THE MODELS A HOST HOLDS (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §9, §10): a model compiled once and planned against
 * many times, for as long as a planner over it lives. A host that holds models -- the tab's module, Python's native
 * library -- makes one of these and owns it; a host that holds nothing (the server: a request has no holder, so it
 * compiles once per request) makes none.
 *
 * <p>Keyed by the model's content ({@link Hash} of its text, Invariant 3): the same text is the same entry, an edited
 * model a new one -- the rule {@code exec.PlanRunner}'s sessions follow. The text is hashed once, when it is held; each
 * plan names the model by its handle.
 *
 * <p>A held {@link ModelContext} is a function of its model alone -- what planning types on demand is kept in it, and no
 * query writes into it; a plan's runtime and connection go through {@code withExecutionOverlay}, a view (the Compiler
 * Rewrite line, 2026-10-10). Its typing caches are plain maps ("a single compile is single-threaded"), so {@link #with}
 * runs each use under the model's own lock: one thread at a time per model, however many of the host's threads plan.
 */
public final class HeldModels {

    private final HeldStore<ModelContext> models = new HeldStore<>();

    /**
     * Hold {@code model}: compiled the first time its text is held (a model that does not compile is refused, as
     * {@link Compiler#compileModel} refuses it, and nothing is held), and held once more each time after. The answer
     * names it to {@link #with} until it is released as many times as it was held.
     */
    public int hold(String model) {
        return models.hold(Hash.ofUtf8(model), () -> Compiler.compileModel(model));
    }

    /** Let go of {@code handle} once; its model goes with its last holder. */
    public void release(int handle) {
        models.release(handle);
    }

    /**
     * {@code use} of the model held under {@code handle}, under that model's own lock; a handle no longer held is refused
     * by name, and its holder holds the model again.
     */
    public <T> T with(int handle, Function<ModelContext, T> use) {
        return models.with(handle, use);
    }

    /**
     * {@code use} of the model {@code model} (its text): the one held, under its lock, when some planner holds that text
     * -- compiled no more -- else compiled for this use alone, holding nothing. How a host's {@code pure/v1} route answers
     * a request whose model the host holds already (the trader screens, 2026-10-25: each grid's query typed by
     * {@code lambdaRelationType} compiled the 2.3 MB trading model again, 2.7 s each, in a tab that held it).
     */
    public <T> T withText(String model, Function<ModelContext, T> use) {
        return models.withKeyOr(Hash.ofUtf8(model), use, () -> use.apply(Compiler.compileModel(model)));
    }

    /** How many models are held now: for tests and a host's diagnostics. */
    public int size() {
        return models.size();
    }
}
