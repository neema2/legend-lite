// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.error;

/**
 * AN ERROR LITE RAISES, WITH ITS RECORD (stage B's BE, the one error model, 2026-11-21): the exception's message is
 * its record's sentence in lite's words; an audience that words errors its own way reads the record, never the message.
 * Null for an error whose site does not yet state one (the sites move to records family by family; the design's §5).
 */
public interface LegendError {

    /** The error as data; null where its site states no record yet. */
    @com.legend.base.Nullable ErrorRecord record();
}
