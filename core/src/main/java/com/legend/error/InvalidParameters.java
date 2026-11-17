package com.legend.error;

/**
 * A RUN'S PARAMETER VALUES ITS DECLARED TYPES DO NOT TAKE (lite's API v1 spec §3, kind {@code parameters}; the audit's
 * J18, 2026-11-17): each value its parameter refuses, named with the parameter's type and multiplicity, every one at
 * once. Its words are the run's ({@code exec.PlanParameters.check}); a typed failure so that every binding classifies it
 * as the caller's values -- neither the data's nor a defect -- as it classifies {@link MissingParameters}. The
 * compatibility edge names it as legend-engine's clients read it ({@code edge.PureMessages.typeName}).
 */
public final class InvalidParameters extends IllegalArgumentException {

    public InvalidParameters(String message) {
        super(message);
    }
}
