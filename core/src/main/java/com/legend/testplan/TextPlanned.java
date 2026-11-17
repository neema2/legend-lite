// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

/** The arm's answer for one assert: opaque to everyone but the test planner. */
public final class TextPlanned {
    final HostSides.Check check;

    TextPlanned(HostSides.Check check) {
        this.check = check;
    }
}
