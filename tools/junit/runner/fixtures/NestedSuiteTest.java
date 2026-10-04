package com.legend.tools.junit.fixtures;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/** RunnerTest's PCT-shaped JUnit 3 suite: nested suites inside a TestSetup (PureTestBuilder, wrapped by
 *  PureTestHelperFramework.wrapSuite). The vintage engine cannot filter inside it. */
public class NestedSuiteTest {
    public static Test suite() {
        TestSuite suite = new TestSuite("pure::nested");
        for (int g = 0; g < 3; g++) {
            TestSuite group = new TestSuite("pure::nested::group" + g);
            for (int i = 1; i <= 4; i++) {
                group.addTest(new TestCase("testFn_" + g + "_" + i) {
                    @Override protected void runTest() { }
                });
            }
            suite.addTest(group);
        }
        return new TestSetup(suite);
    }
}
