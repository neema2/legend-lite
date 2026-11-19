// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.base.Nullable;
import com.legend.testexec.EdgeHearing;
import com.legend.testexec.EdgeRun;
import com.legend.testexecutionplan.AssertListener;
import com.legend.testexecutionplan.TestPlan;

/**
 * THE COMPATIBILITY EDGE'S RUN (census C2, 2026-11-17): a body's engine-text asserts judged and their appeals heard,
 * each held appeal decided after the run by the harness's {@link SqlReplayReferee}, or by none (a contract appeal needs
 * no referee; a rows appeal with none declines by name).
 */
public final class AppealRun implements EdgeRun {

    private final @Nullable SqlReplayReferee referee;

    private AppealRun(@Nullable SqlReplayReferee referee) {
        this.referee = referee;
    }

    /** The edge's run deciding its appeals by {@code referee} (none: the harness registered no referee). */
    public static AppealRun with(@Nullable SqlReplayReferee referee) {
        return new AppealRun(referee);
    }

    @Override
    public EdgeHearing open(TestPlan plan, @Nullable AssertListener callers, java.util.UUID execution) {
        return new Appeals(plan, callers, execution, referee);
    }

    @Override
    public boolean wordsAlike() {
        // the compatibility rules: the words are the host's alone (upstream's), the verdicts compared
        return false;
    }
}
