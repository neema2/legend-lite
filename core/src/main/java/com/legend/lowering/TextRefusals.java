// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;

/**
 * THE PRODUCT'S REFUSALS OF A TEXT ONLY LEGEND-PURE WRITES (the separation census's B8 (i), 2026-11-17): the
 * product's own sentences, at lowering (phase LOWER), naming what was asked and the product's way instead. The
 * compatibility edge answers the same calls ({@link PureOnlyTexts}); the product's style throws these.
 */
final class TextRefusals {

    private TextRefusals() {
    }

    /** {@code format}'s {@code %0<width>d}. */
    static ModelException formatPadded(long width) {
        return refused("format's %0" + width + "d", "the product's format writes %s, %d, %f and %t{}");
    }

    /** A user's {@code toRepresentation}. */
    static ModelException representation() {
        return refused("toRepresentation", "the product writes a value's text by toString and format's %s, %d, %f and"
                + " %t{}");
    }

    private static ModelException refused(String what, String instead) {
        return new ModelException(LegendCompileException.Phase.LOWER,
                what + " is legend-pure's text, the compatibility edge's: " + instead);
    }
}
