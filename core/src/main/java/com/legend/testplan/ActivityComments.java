// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

/**
 * THE ACTIVITY COMMENT a body's planning reserves (slice 5, 2026-11-09): under a verdict batch, the activity row of a
 * frame built with no stamp of its own (an inline {@code execute(...)} an assert reads) names the trace comment of the
 * body's fused statement, the statement that will run it -- one per body, shared by every copy of the body's
 * environment. Planning's own (until slice 5 it borrowed exec's execution trace).
 *
 * <p>DERIVED, NEVER MINTED (G10, the C checkpoint's follow-up, 2026-11-12): until then it was a
 * {@code UUID.randomUUID()} minted at planning, so the same body planned twice wrote two plans. It is a planning-time
 * handle, not an execution's id: no statement is sent under a comment since C1 (2026-11-12), so the comment lives only
 * in the activity rows the body reads, and the frames a let builds under the batch bind their segment's stamp, the run's
 * id, instead ({@code BodyDriver.plannedFrame}). So it is derived from a fixed name ({@code UUID.nameUUIDFromBytes}: the
 * same comment for identical input, its shape the execution-trace comment's), and planning makes no value of its own
 * ({@code ArchitectureTest.planningMakesNoValueOfItsOwn}).
 */
final class ActivityComments {

    /** The reserved comment: an execution-trace comment of the name-derived id of the body's fused statement. */
    private static final String RESERVED = com.legend.executionplan.ExecutionPlan.traceComment(
            java.util.UUID.nameUUIDFromBytes("legend-lite: a body's fused statement".getBytes(
                    java.nio.charset.StandardCharsets.UTF_8)));

    /** The comment reserved for the body's fused statement: the same for every body, every planning. */
    String reserve() {
        return RESERVED;
    }
}
