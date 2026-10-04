package com.legend.testing;

import java.nio.file.Path;

/**
 * Where a test finds the pinned legend-engine and legend-pure source trees.
 *
 * <p>The build says where they are, and nothing else does. Bazel fetches the pinned
 * release archives. A test target with {@code upstream = True} passes the runfiles path of each
 * tree's root {@code pom.xml} ({@code tools/junit/defs.bzl}), resolved here through the runfiles
 * library; a generator action passes the tree's exec path ({@code tools/generators/defs.bzl}), read
 * as it is, until Bazel workplan P3-33 gives generators explicit arguments and deletes this class. Each
 * test used to repeat
 * a {@code ~/legend/…} default of its own, so a JVM started without the property
 * read whatever checkout happened to sit there, at whatever commit — the
 * off-pin-checkout failure mode. A missing property is now an error naming it
 * (AGENTS.md invariant 4).
 */
public final class Upstream {

    private Upstream() {}

    /** The legend-engine source tree. */
    public static Path engine() {
        return root("legend.engine.root");
    }

    /** The legend-pure source tree. */
    public static Path pure() {
        return root("legend.pure.root");
    }

    private static Path root(String property) {
        String value = System.getProperty(property);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException("-D" + property + " is not set — run the test"
                    + " through Bazel: a test target with upstream = True, or a generator action,"
                    + " sets it");
        }
        return value.endsWith("/pom.xml") ? Runfile.of(value).getParent() : Path.of(value);
    }
}
