package com.legend.tools.junit.fixtures;

import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

/** RunnerTest's failing pass, for JUnitAction: run only through it, never selected on its own. */
public class FailingTest {
    @Test void passes() { }

    @Test void fails() {
        fail("the fixture's planted failure");
    }
}
