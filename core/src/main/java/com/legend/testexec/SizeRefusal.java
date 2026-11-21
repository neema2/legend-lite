// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.Operand;
import com.legend.sql.RaiseKind;

/**
 * A REQUIRED SIDE'S, OR ANSWER'S, REFUSAL OF NO ROWS, AS A RECORD (stage B's B1, 2026-11-20, errors as records; until then its words were
 * planned in the plan's style, {@code SideShape.sizeWords} and {@code TestPlan.answerSizeWords}): the one site of the
 * run's size check, kind {@link RaiseKind#SIZE_NOT_MULTIPLICITY}, placed by the declared bound, its operand the bound
 * -- the shape of a placed raise site ({@code RaiseText.placed}). Its message is the product's sentence; an audience's
 * words read the site ({@link ExecutionPlan.RaisedAtSite}). An {@link IllegalStateException}, as the refusal was: an
 * {@code assertError} does not take it.
 */
final class SizeRefusal extends IllegalStateException implements ExecutionPlan.RaisedAtSite {
    private final transient ExecutionPlan.Site site;

    SizeRefusal(String bound) {
        this(new ExecutionPlan.Site(1, RaiseKind.SIZE_NOT_MULTIPLICITY, java.util.List.of(new Operand.Text(bound))));
    }

    private SizeRefusal(ExecutionPlan.Site site) {
        super(com.legend.result.RaiseWords.sentence(site));
        this.site = site;
    }

    @Override
    public ExecutionPlan.Site site() {
        return site;
    }
}
