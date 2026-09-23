package com.legend.testing;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * The entries of this JVM's class path, as the JVM itself sees them: every
 * {@code java.class.path} entry, and every jar a jar's manifest names in its
 * {@code Class-Path}. Bazel's launcher on Windows packs a long class path into
 * ONE manifest-only "classpath jar" (the command line has a length limit), so a
 * scan of {@code java.class.path} alone finds a single jar there and none of the
 * engine's — the first Bazel CI run on Windows (2026-09-23): the fixture harvest
 * found no tests-jars and the protocol roster came out empty. Elsewhere this is
 * {@code java.class.path}, split. A manifest entry that names no existing file
 * is skipped, as the JVM skips it.
 */
public final class Classpath {

    private Classpath() {
    }

    /** This JVM's class path entries, manifest {@code Class-Path}s expanded. */
    public static List<String> entries() {
        return expand(System.getProperty("java.class.path", ""));
    }

    /** {@code classpath}'s entries (split on the platform separator), each jar's
     *  manifest {@code Class-Path} expanded after it. */
    static List<String> expand(String classpath) {
        LinkedHashSet<String> out = new LinkedHashSet<>();
        for (String entry : classpath.split(File.pathSeparator)) {
            if (entry.isEmpty()) {
                continue;
            }
            out.add(entry);
            if (entry.endsWith(".jar") && Files.isRegularFile(Path.of(entry))) {
                out.addAll(manifestClassPath(Path.of(entry)));
            }
        }
        return List.copyOf(out);
    }

    /** The existing files {@code jar}'s manifest names in {@code Class-Path}:
     *  space-separated URLs, relative ones resolved against the jar's own location. */
    private static List<String> manifestClassPath(Path jar) {
        try (JarFile file = new JarFile(jar.toFile())) {
            Manifest manifest = file.getManifest();
            String classPath = manifest == null ? null
                    : manifest.getMainAttributes().getValue(Attributes.Name.CLASS_PATH);
            if (classPath == null || classPath.isBlank()) {
                return List.of();
            }
            URI base = jar.toAbsolutePath().toUri();
            List<String> out = new ArrayList<>();
            for (String url : classPath.trim().split("\\s+")) {
                Path named = Path.of(base.resolve(url));
                if (Files.exists(named)) {
                    out.add(named.toString());
                }
            }
            return out;
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read the manifest of " + jar, e);
        }
    }
}
