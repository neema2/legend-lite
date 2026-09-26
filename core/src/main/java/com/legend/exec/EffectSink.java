// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import java.util.ArrayList;
import java.util.List;

/**
 * AN EFFECT SEGMENT under construction (block-compiler stage 3, 2026-09-21): the
 * statements the effect natives would have SENT, collected instead. The arms keep
 * doing their compile work (splitting and adapting raw text, rendering DDL from the
 * model, spelling CSV rows as inserts); with a sink on the environment their one
 * run-time act — the send — lands here, and the segment goes to the database as ONE
 * script when the compiler closes it. Each entry carries the text executed and the
 * text the referee's ledger records for it (null: not recorded, as today), and its
 * kind decided statically by the boundary's first-keyword rule.
 */
public final class EffectSink {

    public record Entry(String executed, @com.legend.base.Nullable String recorded, boolean query) {
    }

    private final List<Entry> entries = new ArrayList<>();

    public void add(String executed, @com.legend.base.Nullable String recorded) {
        entries.add(new Entry(executed, recorded, com.legend.sql.RawSql.isSingleQuery(executed)));
        Census.inc(Census.Key.EFFECTS_IN_SCRIPT);
    }

    public List<Entry> entries() {
        return List.copyOf(entries);
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

}
