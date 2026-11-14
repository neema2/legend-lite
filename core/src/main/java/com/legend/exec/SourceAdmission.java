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
 * @param host  what a refusal under the fence says of the host that drew it, after the source: empty for a library's
 *              own (no host to name), the server's words for the server's ({@link #onTheServer}); never the folders
 */
public record SourceAdmission(@com.legend.base.Nullable List<Path> fence, String host) {

    public SourceAdmission {
        fence = fence == null ? null : fence.stream().map(d -> Objects.requireNonNull(d, "directory").toAbsolutePath()
                .normalize()).toList();
        Objects.requireNonNull(host, "host");
    }

    /** {@code fence} with no host's words. */
    public SourceAdmission(@com.legend.base.Nullable List<Path> fence) {
        this(fence, "");
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
     * could build a fence): absent, blank or {@code any}, any file (the library's default, unchanged: the user's ruling
     * of A5); {@code none}, none; else the directories it lists, separated by the platform's path separator ({@code :},
     * {@code ;} on Windows), each a fence's. A listed directory that is not one is refused by name, never dropped: a
     * host validates its configuration with this once, where it starts (the server: the A delta audit's 5, 2026-11-11).
     */
    public static SourceAdmission configured(@com.legend.base.Nullable String value) {
        if (value == null || value.strip().isEmpty() || value.strip().equals("any")) {
            return any();
        }
        if (value.strip().equals("none")) {
            return none();
        }
        List<Path> dirs = new java.util.ArrayList<>();
        for (String d : value.split(java.util.regex.Pattern.quote(java.io.File.pathSeparator))) {
            if (d.strip().isEmpty()) {
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

    /** This admission as the server's (the user's ruling of 2026-11-10): a refusal under its fence says how the server
     *  names its folders, never which they are (the A delta audit's 4 and 13, 2026-11-11). */
    public SourceAdmission onTheServer() {
        return fence == null ? this : new SourceAdmission(fence, fence.isEmpty()
                ? " (the server reads no file: source outside its fenced folders; none are configured: start it with"
                        + " --sources DIR)"
                : " (the server reads a file: source only inside the folders its --sources DIR or"
                        + " LEGEND_LITE_SOURCE_DIRS name)");
    }

    /**
     * The path the database reads for {@code location}, a source's location as its model declares it: refused by name
     * ({@link DataError}) unless it is a {@code file:} URL this machine reads and, under a fence, names a file under an
     * admitted directory -- as written (the path normalized, before anything on the machine is asked of it) and as it
     * resolves (its real path, links followed, so no link leads out). Under a fence every refusal is ONE message, naming
     * the source alone (the A delta audit's 4, 2026-11-11: until then a caller learned whether a path outside the fence
     * existed, and the refusal echoed the fence's folders).
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
        if (fence == null) {
            // no fence: any file, as upstream reads any (forward slashes: a database reads them on every system)
            return slashed(real(file));
        }
        DataError refused = new DataError("the source '" + location + "' is not a file this run admits" + host);
        // as written first: a path outside the fence is refused before the machine is asked whether it exists (an
        // admitted folder as written or as it resolves: asking the host's own folders tells the caller nothing)
        boolean written = false;
        for (Path d : fence) {
            if (file.startsWith(d)) {
                written = true;
                break;
            }
            try {
                if (file.startsWith(d.toRealPath())) {
                    written = true;
                    break;
                }
            } catch (java.io.IOException gone) {
                continue;   // an admitted folder gone since the host started: as written alone
            }
        }
        if (!written) {
            throw refused;
        }
        Path real;
        try {
            real = file.toRealPath();
        } catch (java.io.IOException e) {
            throw refused;
        }
        for (Path d : fence) {
            Path admitted;
            try {
                admitted = d.toRealPath();
            } catch (java.io.IOException e) {
                continue;   // an admitted folder gone since the host started admits nothing
            }
            if (real.startsWith(admitted)) {
                return slashed(real);
            }
        }
        throw refused;
    }

    /** {@code p} with forward slashes: a database reads them on every system (Windows CI, 2026-09-09). */
    private static String slashed(Path p) {
        return p.toString().replace(java.io.File.separatorChar, '/');
    }

    /** {@code p}'s real path, links followed; a path that does not resolve is refused by name, never compared as
     *  written (no fence: any file, so naming the path tells the caller nothing it could not read). */
    private static Path real(Path p) {
        try {
            return p.toRealPath();
        } catch (java.io.IOException e) {
            throw new DataError("the path '" + p + "' does not resolve on this machine: " + e.getMessage(), e);
        }
    }
}
