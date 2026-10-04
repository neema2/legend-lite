package com.legend.tools.junit.fixtures;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/** RunnerTest's flat JUnit 3 suite, built at discovery: the vintage engine can filter its top level. */
public class FlatSuiteTest {
    public static Test suite() {
        TestSuite suite = new TestSuite("pure::flat");
        for (int i = 1; i <= 12; i++) {
            suite.addTest(new TestCase("testFn_" + i) {
                @Override protected void runTest() { }
            });
        }
        return suite;
    }
}
