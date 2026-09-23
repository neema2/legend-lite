package perf;

import java.nio.file.Path;
import java.nio.file.Paths;

/** A path argument, relative to where the command was TYPED: under `bazel run` the
 *  process runs in its runfiles tree, and Bazel names the caller's directory in
 *  BUILD_WORKING_DIRECTORY. Absolute paths, and any run outside Bazel, are unchanged. */
final class Cwd
{
    private Cwd()
    {
    }

    static Path of(String first, String... more)
    {
        Path p = Paths.get(first, more);
        String cwd = System.getenv("BUILD_WORKING_DIRECTORY");
        return cwd == null || p.isAbsolute() ? p : Paths.get(cwd).resolve(p);
    }
}
