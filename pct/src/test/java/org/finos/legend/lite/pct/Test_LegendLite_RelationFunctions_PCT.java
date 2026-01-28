// Copyright 2026 Legend Lite Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.junit.Ignore;

/**
 * PCT tests for Legend-Lite's relation function support.
 * 
 * IMPORTANT: This test class is currently DISABLED because it requires
 * legend-engine's
 * Pure-to-Java compilation infrastructure which generates distributed metadata.
 * Legend-lite takes a different approach - we execute Pure directly through our
 * QueryService without compiling Pure to Java.
 * 
 * For now, PCT validation is done via:
 * - PureCompatibilityExecutionTest (end-to-end execution tests)
 * - PureCompatibilitySmokeTest (basic connectivity tests)
 * - TdsLiteralTests in RelationApiIntegrationTest (TDS literal support)
 * 
 * TODO: Implement a custom PCT runner that:
 * 1. Parses PCT test definitions from pure files directly
 * 2. Executes tests through PureCompatibilityExecution
 * 3. Validates results without needing compiled metadata
 */
@Ignore("Disabled: requires legend-engine Pure compilation infrastructure - see class javadoc")
public class Test_LegendLite_RelationFunctions_PCT {

    /**
     * JUnit 3 test suite entry point.
     * Returns empty suite since this is disabled.
     */
    public static Test suite() {
        // Return empty test suite - the full PCT infrastructure is not available
        return new TestSuite("Legend-Lite PCT (disabled pending custom runner)");
    }
}
