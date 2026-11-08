// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.testexecutionplan.Extension;

import java.util.List;

/**
 * A seed step's ledger texts, planned by the compatibility edge at the step (docs/PLAN_AS_DATA_2026_11_07.md 2.7): one
 * per statement, in order; whether each answered rows is the run's to say.
 */
public record SeedTexts(List<String> texts) implements Extension {

    public SeedTexts {
        texts = List.copyOf(texts);
    }
}
