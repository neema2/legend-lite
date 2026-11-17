// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

/** The database judge's text collector ({@link #batchTexts}). */
public interface BatchTexts {
    /** {@code at} with this collector its engine-text collector: the environment the arm is planned under. */
    BodyDriver.ExecEnv under(BodyDriver.ExecEnv at);

    /** The arm has answered: the appeal it built reads what it needs from here on. */
    void endPlanning();
}
