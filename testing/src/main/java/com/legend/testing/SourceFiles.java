package com.legend.testing;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The source files a static-analysis guard reads, as its test target DECLARES them (Bazel workplan P3-27): never a
 * walk of a directory. The BUILD file passes {@code -Dlegend.sources=$(rlocationpath :<name>_sources)}, a list written
 * by {@code file_list} (tools/jars/defs.bzl) with one runfiles path per line, and every file it names rides in the
 * test's runfiles. Each file is resolved through Bazel's runfiles library ({@link Runfile}), so this works the same
 * under a runfiles tree and under a manifest alone (Windows).
 *
 * <p>A guard asks for the files under a repository directory ({@link #under}) and reports a file by its path from
 * that directory ({@link #rel}), computed from the declared path, never from where the file happens to sit on disk.
 */
public final class SourceFiles {

    /** The system property the BUILD file sets. */
    public static final String PROPERTY = "legend.sources";

    private SourceFiles() {}

    private static final class Holder {
        /** repository path ({@code core/src/main/java/...}) -> the file */
        static final Map<String, Path> FILES;
        /** the file -> its repository path */
        static final Map<Path, String> PATHS;

        static {
            Path list = Runfile.property(PROPERTY);
            List<String> lines;
            try {
                lines = Files.readAllLines(list, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new UncheckedIOException("cannot read the declared source list " + list, e);
            }
            Map<String, Path> files = new TreeMap<>();
            Map<Path, String> paths = new HashMap<>();
            for (String rlocationpath : lines) {
                if (rlocationpath.isBlank()) {
                    continue;
                }
                // <repository>/<path in it>: a guard reads the main repository's files by their repository paths
                String path = rlocationpath.substring(rlocationpath.indexOf('/') + 1);
                Path file = Runfile.of(rlocationpath);
                files.put(path, file);
                paths.put(file, path);
            }
            FILES = Collections.unmodifiableMap(files);
            PATHS = Collections.unmodifiableMap(paths);
        }
    }

    /** Every declared file under the repository directory {@code dir} ({@code core/src/main/java}), and under each of
     *  {@code more}, in path order within each. */
    public static List<Path> under(String dir, String... more) {
        List<Path> out = new ArrayList<>();
        for (String d : prepend(dir, more)) {
            String prefix = d.endsWith("/") ? d : d + "/";
            for (Map.Entry<String, Path> e : Holder.FILES.entrySet()) {
                if (e.getKey().startsWith(prefix)) {
                    out.add(e.getValue());
                }
            }
        }
        return out;
    }

    private static List<String> prepend(String first, String... more) {
        List<String> all = new ArrayList<>();
        all.add(first);
        all.addAll(List.of(more));
        return all;
    }

    /** The files directly in the repository directory {@code dir}, not in its subdirectories, in path order. */
    public static List<Path> in(String dir) {
        String prefix = dir.endsWith("/") ? dir : dir + "/";
        List<Path> out = new ArrayList<>();
        for (Map.Entry<String, Path> e : Holder.FILES.entrySet()) {
            if (e.getKey().startsWith(prefix) && e.getKey().indexOf('/', prefix.length()) < 0) {
                out.add(e.getValue());
            }
        }
        return out;
    }

    /** The declared file at the repository path {@code path}; fails when the test target does not declare it. */
    public static Path file(String path) {
        Path file = Holder.FILES.get(path);
        if (file == null) {
            throw new IllegalArgumentException(path + " is not a declared source (-D" + PROPERTY + ")");
        }
        return file;
    }

    /** Whether the repository path {@code path} is a declared source (a guard that a file stays deleted asks this). */
    public static boolean has(String path) {
        return Holder.FILES.containsKey(path);
    }

    /** {@code file}'s repository path ({@code core/src/main/java/com/legend/X.java}); fails for an undeclared file. */
    public static String path(Path file) {
        String path = Holder.PATHS.get(file);
        if (path == null) {
            throw new IllegalArgumentException(file + " is not a declared source (-D" + PROPERTY + ")");
        }
        return path;
    }

    /**
     * {@code file}'s path from the repository directory {@code dir}, with a leading '/' and '/' separators
     * ({@code /com/legend/X.java} from {@code core/src/main/java}): the form a path-string guard compares, the same
     * on every platform and wherever the checkout lives.
     */
    public static String rel(String dir, Path file) {
        String path = path(file);
        String prefix = dir.endsWith("/") ? dir : dir + "/";
        if (!path.startsWith(prefix)) {
            throw new IllegalArgumentException(path + " is not under " + dir);
        }
        return "/" + path.substring(prefix.length());
    }
}
