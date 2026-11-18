// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package planoracle;

import java.nio.file.Path;

/** A path argument, relative to where the command was typed: under {@code bazel run} the process runs in its runfiles
 *  tree and Bazel names the caller's directory in {@code BUILD_WORKING_DIRECTORY} (as {@code perf.Cwd}). An absolute
 *  path, and any run outside Bazel, is unchanged. */
final class Cwd
{
    private Cwd()
    {
    }

    static Path resolve(String path)
    {
        Path p = Path.of(path);
        String cwd = System.getenv("BUILD_WORKING_DIRECTORY");
        return cwd == null || p.isAbsolute() ? p : Path.of(cwd).resolve(p);
    }
}
