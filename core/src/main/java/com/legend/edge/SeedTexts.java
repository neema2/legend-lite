// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.testexecutionplan.Extension;

import java.util.List;

/**
 * A seed step's ledger texts, planned by the compatibility edge at the step (docs/PLAN_AS_DATA_2026_11_07.md 2.7): one
 * per statement, in order; whether each answered rows is the run's to say.
 *
 * <p>UNUSED since B2b (2026-11-11): the edge plans {@link RefereeLoad}, the referee's copy as a load, and nothing makes,
 * writes or reads this record. Its deletion (census M3) waits on the user's word: this session did not create the
 * file (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §6, B2b).
 */
public record SeedTexts(List<String> texts) implements Extension {

    public SeedTexts {
        texts = List.copyOf(texts);
    }
}
