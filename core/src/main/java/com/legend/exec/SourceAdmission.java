// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.error.DataError;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/**
 * THE DIRECTORIES A RUN'S LIVE SOURCES MAY READ (docs/DATA_IN_THE_PLAN_2026_11_09.md L6, the user's ruling of
 * 2026-11-10: "why not pointers to the files? ... I'm not so sure it's any different from a database table"): a model's
 * {@code file:} source is a pointer, as a table is, named in the plan as the model declares it and read by the database
 * at the run -- live data, no digest, no copy, upstream's behaviour. This is its guard, the analogue of a database's
 * permissions: whoever runs lists the directories a plan may read, and a location outside them is refused by name at
 * the run, as is every scheme but {@code file:} (decision 4's {@code url}, not built). Like {@code DataStores} it is
 * configuration, never part of a plan (it lives with exec, its one reader); the default, {@link #none()}, admits nothing -- a server admits only what it is
 * configured with.
 *
 * @param directories each admitted directory, absolute and normalized
 */
public record SourceAdmission(List<Path> directories) {

    public SourceAdmission {
        directories = directories.stream().map(d -> Objects.requireNonNull(d, "directory").toAbsolutePath()
                .normalize()).toList();
    }

    /** Admits nothing: every live source refused by name. */
    public static SourceAdmission none() {
        return new SourceAdmission(List.of());
    }

    /** Admits {@code directories} and everything under them. */
    public static SourceAdmission of(Path... directories) {
        return new SourceAdmission(List.of(directories));
    }

    /**
     * The path the database reads for {@code location}, a source's location as its model declares it: refused by name
     * ({@link DataError}) unless it is a {@code file:} URL naming a file under an admitted directory (its real path,
     * links followed, so no link leads out).
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
        for (Path d : directories) {
            if (real.startsWith(real(d))) {
                // forward slashes: a database reads them on every system (Windows CI, 2026-09-09)
                return real.toString().replace(java.io.File.separatorChar, '/');
            }
        }
        throw new DataError("the source '" + location + "' is not under a directory this run admits"
                + (directories.isEmpty() ? " (it admits none)" : ": " + directories));
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
