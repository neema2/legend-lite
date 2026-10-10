// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.PortableText;

import java.math.BigDecimal;

/**
 * A protocol value's double, from its text and to it, as the JDK converts them -- the same on the JVM and in the tab
 * ({@link PortableText}: TeaVM's own conversions can land a digit or a unit in the last place off). The parser reads a
 * literal's double here, so its dependency surface stays the protocol's (ArchitectureTest, invariant 7c); the
 * protocol's readers and writers use it as well.
 */
public final class NumberText {

    private NumberText() {
    }

    /** {@code Double.parseDouble(text)} for a decimal. */
    public static double doubleOf(String text) {
        return PortableText.doubleOf(text);
    }

    /** {@code Double.toString(value)}. */
    public static String text(double value) {
        return PortableText.doubleText(value);
    }

    /** {@code BigDecimal.valueOf(value)}: the decimal of the double's text. */
    public static BigDecimal decimal(double value) {
        return new BigDecimal(PortableText.doubleText(value));
    }
}
