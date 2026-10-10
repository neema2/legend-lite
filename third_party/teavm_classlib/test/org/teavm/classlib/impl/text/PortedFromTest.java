// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.teavm.classlib.impl.text;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import org.junit.jupiter.api.Test;

/**
 * The corrected classes replace TeaVM's WHOLE, so they must be the version they were ported from: a TeaVM bump that
 * left them as they are would compile 0.15.0's TDouble, TFloat, TAbstractStringBuilder and TBigDecimal into the newer
 * library. This fails on a bump until the files are re-ported (README.md) and this version moves with them.
 */
class PortedFromTest {

    private static final String PORTED_FROM = "teavm-classlib-0.15.0.jar";

    @Test
    void theClassLibraryIsTheVersionTheCorrectedFilesWerePortedFrom() {
        // a class this package does not replace, so it is found in TeaVM's own jar
        URL url = PortedFromTest.class.getClassLoader().getResource("org/teavm/classlib/impl/text/DoubleAnalyzer.class");
        assertNotNull(url, "TeaVM's class library is on the class path");
        assertTrue(url.toString().contains(PORTED_FROM), "TeaVM's class library is " + url
                + ", but the corrected classes were ported from " + PORTED_FROM + ": re-port them (README.md)");
    }
}
