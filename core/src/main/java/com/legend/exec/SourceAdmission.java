// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.error.DataError;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/**
 * THE FILES A RUN'S LIVE SOURCES MAY READ (docs/DATA_IN_THE_PLAN_2026_11_09.md L6, the user's ruling of 2026-11-10:
 * "why not pointers to the files? ... I'm not so sure it's any different from a database table"): a model's
 * {@code file:} source is a pointer, as a table is, named in the plan as the model declares it and read by the database
 * at the run -- live data, no digest, no copy, upstream's behaviour. ANY FILE BY DEFAULT, as upstream reads any
 * (the user's ruling on docs/DATA_DELIVERY_DESIGN_2026_11_10.md, A5: "any file by default, with an opt-in fence, default
 * off"): {@link #any()}. A host may FENCE the files to listed directories ({@link #of}), the analogue of a database's
 * permissions, a location outside them refused by name at the run; every scheme but {@code file:} is refused (decision
 * 4's {@code url}, not built). Like {@code DataStores} it is configuration, never part of a plan (it lives with exec, its
 * one reader).
 *
 * @param fence each admitted directory, absolute and normalized, or null for no fence: any file
 */
public record SourceAdmission(@com.legend.base.Nullable List<Path> fence) {

    public SourceAdmission {
        fence = fence == null ? null : fence.stream().map(d -> Objects.requireNonNull(d, "directory").toAbsolutePath()
                .normalize()).toList();
    }

    /** No fence: any file a {@code file:} source names (the default, upstream's). */
    public static SourceAdmission any() {
        return new SourceAdmission(null);
    }

    /** A fence of no directory: every live source refused by name (a host that admits none). */
    public static SourceAdmission none() {
        return new SourceAdmission(List.of());
    }

    /** A fence: {@code directories} and everything under them. */
    public static SourceAdmission of(Path... directories) {
        return new SourceAdmission(List.of(directories));
    }

    /**
     * The admission a host's configuration names (the A checkpoint's tenets audit, 11, 2026-11-10: until then no host
     * could build a fence): absent or blank, any file (the default, unchanged: the user's ruling of A5); {@code none},
     * none; else the directories it lists, separated by the platform's path separator ({@code :}, {@code ;} on
     * Windows), each a fence's. A listed directory that is not one is refused by name, never dropped.
     */
    public static SourceAdmission configured(@com.legend.base.Nullable String value) {
        if (value == null || value.isBlank()) {
            return any();
        }
        if (value.strip().equals("none")) {
            return none();
        }
        List<Path> dirs = new java.util.ArrayList<>();
        for (String d : value.split(java.util.regex.Pattern.quote(java.io.File.pathSeparator))) {
            if (d.isBlank()) {
                continue;
            }
            Path p = Path.of(d.strip());
            if (!java.nio.file.Files.isDirectory(p)) {
                throw new IllegalArgumentException("the admitted sources' directory '" + d.strip() + "' is no"
                        + " directory");
            }
            dirs.add(p);
        }
        return new SourceAdmission(dirs);
    }

    /**
     * The path the database reads for {@code location}, a source's location as its model declares it: refused by name
     * ({@link DataError}) unless it is a {@code file:} URL this machine reads and, under a fence, names a file under an
     * admitted directory (its real path, links followed, so no link leads out).
     */
    public String admit(String location) {
        if (!location.startsWith("file:")) {
            throw new DataError("the source '" + location + "': only a file: source is read (a url source is not"
                    + " built)");
        }
        Path file;
        try {
            file = Path.of(java.net.URI.create(location)).toAbsolutePath().normalize();
        } catch (IllegalArgumentException | java.nio.file.FileSystemNotFoundException e) {
            throw new DataError("the source '" + location + "' is not a file: URL this machine reads", e);
        }
        Path real = real(file);
        if (fence == null) {
            // no fence: any file, as upstream reads any (forward slashes: a database reads them on every system)
            return real.toString().replace(java.io.File.separatorChar, '/');
        }
        for (Path d : fence) {
            if (real.startsWith(real(d))) {
                // forward slashes: a database reads them on every system (Windows CI, 2026-09-09)
                return real.toString().replace(java.io.File.separatorChar, '/');
            }
        }
        throw new DataError("the source '" + location + "' is not under a directory this run admits"
                + (fence.isEmpty() ? " (it admits none)" : ": " + fence));
    }

    /** {@code p}'s real path, links followed; a path that does not resolve (no such file, or a directory admitted
     *  that does not exist) is refused by name, never compared as written. */
    private static Path real(Path p) {
        try {
            return p.toRealPath();
        } catch (java.io.IOException e) {
            throw new DataError("the path '" + p + "' does not resolve on this machine: " + e.getMessage(), e);
        }
    }
}
