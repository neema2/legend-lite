package com.legend.tools.junit.pins;

import static org.junit.jupiter.api.Assertions.assertEquals;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.util.List;
import java.nio.file.Path;
import java.util.Locale;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;

/**
 * Every JVM test runs with one clock, locale, encoding and temp directory, whatever the host has
 * (tools/junit/defs.bzl and JUnitMain; Bazel workplan P0-10). Removing a pin fails here.
 */
class PinsTest {

    /** Each pin is on the command line, so a host whose defaults happen to match cannot hide a removed pin. */
    @Test
    void everyPinIsPassedExplicitly() {
        List<String> args = ManagementFactory.getRuntimeMXBean().getInputArguments();
        for (String pin : List.of("-Duser.timezone=GMT", "-Duser.language=en", "-Duser.country=US", "-Dfile.encoding=UTF-8")) {
            assertTrue(args.contains(pin), pin + " is not on the test JVM's command line: " + args);
        }
    }

    @Test
    void theTestJvmSeesThePinnedEnvironmentNotTheHosts() {
        assertEquals("GMT", TimeZone.getDefault().getID(), "user.timezone");
        assertEquals(Locale.US, Locale.getDefault(), "user.language / user.country");
        assertEquals("UTF-8", Charset.defaultCharset().name(), "file.encoding");
        assertEquals(Path.of(System.getenv("TEST_TMPDIR")).toAbsolutePath().normalize(),
                Path.of(System.getProperty("java.io.tmpdir")).toAbsolutePath().normalize(),
                "java.io.tmpdir is the test's own TEST_TMPDIR (JUnitMain), never the host's");
    }
}
