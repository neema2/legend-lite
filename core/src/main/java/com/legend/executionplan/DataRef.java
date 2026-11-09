// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.executionplan;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A REFERENCE TO DATA in a plan (docs/DATA_IN_THE_PLAN_2026_11_09.md 2.1, the user's go of 2026-11-09): where the name
 * is meaningful to any runner ({@code namespace}), the name inside it, and the SHA-256 digest and size of the bytes the
 * plan was planned against, so the same plan loads the same data on every machine. Never a machine's path: an absolute
 * name, a drive letter, a backslash, an empty segment or a {@code .}/{@code ..} segment is refused here, so no plan can
 * hold one.
 *
 * @param namespace where {@code name} resolves (a runner's store for it: {@link DataStores})
 * @param name      a relative, {@code /}-separated name inside the namespace
 * @param digest    the bytes' SHA-256, lowercase hex
 * @param size      the bytes' length
 * @param format    how the bytes are read
 */
public record DataRef(Namespace namespace, String name, String digest, long size, Format format) {

    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");

    public DataRef {
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(format, "format");
        requireName(name);
        Objects.requireNonNull(digest, "digest");
        if (!SHA256.matcher(digest).matches()) {
            throw new IllegalArgumentException("a data reference's digest is a SHA-256 in lowercase hex: " + digest);
        }
        if (size < 0) {
            throw new IllegalArgumentException("a data reference's size is never negative: " + size);
        }
    }

    /** Where a reference's name resolves. More arrive with their slices (the design's 2.1: content, bundle, url). */
    public enum Namespace {
        /** A repository's test resources, by their name under its resource root. */
        RESOURCE
    }

    /** How the referenced bytes are read. */
    public enum Format {
        /** Comma-separated text, UTF-8, its first line a header. */
        CSV
    }

    /** {@code name} refused unless it is a relative, {@code /}-separated name with no empty, {@code .} or {@code ..}
     *  segment, no backslash and no drive letter: never a machine's path (the design's P5). */
    public static String requireName(String name) {
        Objects.requireNonNull(name, "name");
        if (name.isEmpty() || name.startsWith("/") || name.indexOf('\\') >= 0 || name.indexOf(':') >= 0
                || name.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("a data reference's name is relative to its namespace, never a"
                    + " machine's path: '" + name + "'");
        }
        for (String segment : name.split("/", -1)) {
            if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
                throw new IllegalArgumentException("a data reference's name has an empty, '.' or '..' segment: '"
                        + name + "'");
            }
        }
        return name;
    }
}
