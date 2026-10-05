// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

/**
 * The documentation tag, {@code meta::pure::profiles::doc.doc}: a {@code '''...'''} block before a
 * declaration is sugar for it (legend-pure 5.99.0 / legend-engine 4.145.0). Spelled once, here, for
 * the parser that reads the block and the model printer that writes it back.
 */
public final class Documentation {

    /** The doc profile's full path. */
    public static final String PROFILE = "meta::pure::profiles::doc";
    /** The doc profile's one tag, also the profile's bare name. */
    public static final String TAG = "doc";

    private Documentation() {
    }
}
