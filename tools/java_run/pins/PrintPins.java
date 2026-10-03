package com.legend.tools.javarun.pins;

import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.util.List;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.TimeZone;

/** What a java_run action sees (tools/java_run/defs.bzl pins it; Bazel workplan P0-10): one line each, diff-tested. */
public final class PrintPins {
    private PrintPins() {}

    public static void main(String[] args) throws Exception {
        Path tmp = Path.of(System.getProperty("java.io.tmpdir"));
        // each pin as passed on the command line: a host whose defaults happen to match cannot hide a removed pin
        List<String> given = ManagementFactory.getRuntimeMXBean().getInputArguments();
        StringBuilder flags = new StringBuilder();
        for (String pin : List.of("-Duser.timezone=GMT", "-Duser.language=en", "-Duser.country=US", "-Dfile.encoding=UTF-8")) {
            flags.append(pin).append(given.contains(pin) ? " given\n" : " MISSING\n");
        }
        String text = flags + "timezone=" + TimeZone.getDefault().getID() + "\n"
                + "locale=" + Locale.getDefault() + "\n"
                + "encoding=" + Charset.defaultCharset().name() + "\n"
                // the action's own declared scratch directory (<target>_tmp), never the host's /tmp
                + "tmpdir=" + tmp.getFileName() + "\n";
        Files.writeString(Path.of(args[0]), text, StandardCharsets.UTF_8);
    }
}
