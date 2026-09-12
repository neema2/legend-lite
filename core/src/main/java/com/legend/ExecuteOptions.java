// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

/**
 * The execute OPTIONS a caller passes with one execution — they ride the
 * request and the result, never a static slot (the PctRenderOption
 * thread-local died here, batch 122).
 *
 * @param pctRender the PCT adapter's wire render: a relation-rooted query
 *                  lowers through the PCT-TDS root mode and the result is an
 *                  {@link com.legend.exec.ExecutionResult.TdsText}
 * @param recorder  the raw-SQL ledger the executor appends to (null = none)
 * @param resources the test-input resource resolver (path → text; null = none)
 */
public record ExecuteOptions(boolean pctRender,
        com.legend.sql.dialect.RawSqlBoundary.@com.legend.Nullable Recorder recorder,
        java.util.function.@com.legend.Nullable Function<String, String> resources,
        java.util.Set<com.legend.compiler.spec.typed.Feature> features) {
    public static final ExecuteOptions NONE = new ExecuteOptions(false, null, null, java.util.Set.of());
    public static final ExecuteOptions PCT_RENDER = new ExecuteOptions(true, null, null, java.util.Set.of());

    /** The runner's DEFAULT feature flags — merged into every execute
     *  call's own ({@code ExecutionContext.features}); the one ambient
     *  source (the engine's testable runner has the same knob). */
    public ExecuteOptions withFeatures(java.util.Set<com.legend.compiler.spec.typed.Feature> f) {
        return new ExecuteOptions(pctRender, recorder, resources, java.util.Set.copyOf(f));
    }

    /** The raw-SQL ledger this execution appends to (Phase 2b) and the
     * test-input resource resolver its CSV loads read through (Phase 2d;
     * path → text, loud when absent — the reference checkout stays spec,
     * never runtime): the caller owns both, the executor keeps neither. */
    public static ExecuteOptions recording(com.legend.sql.dialect.RawSqlBoundary.Recorder r,
            java.util.function.@com.legend.Nullable Function<String, String> resources) {
        return new ExecuteOptions(false, r, resources, java.util.Set.of());
    }
}
