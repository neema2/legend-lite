// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.testexecutionplan.Extension;

import java.util.List;
import java.util.Objects;

/**
 * An effect step's ledger texts, planned by the compatibility edge at the step (docs/PLAN_AS_DATA_2026_11_07.md 2.7):
 * each statement's text as the referee's ledger records it and whether it is a query; recorded once the step ran (a
 * step that fails records nothing: its failure is the database's own error).
 */
public record EffectTexts(List<Statement> statements) implements Extension {

    public EffectTexts {
        statements = List.copyOf(statements);
    }

    /** One statement's ledger text. */
    public record Statement(String text, boolean query) {
        public Statement {
            Objects.requireNonNull(text, "text");
        }
    }
}
