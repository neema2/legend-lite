package com.legend.testing;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The Windows launcher's shape, built here: a manifest-only "classpath jar" whose
 * {@code Class-Path} names the real jars. Scans of the class path must see those
 * jars, not the one that names them.
 */
class ClasspathTest {

    private static Path jar(Path at, String classPath) throws Exception {
        Files.createDirectories(at.getParent());
        Manifest m = new Manifest();
        m.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        if (classPath != null) {
            m.getMainAttributes().put(Attributes.Name.CLASS_PATH, classPath);
        }
        try (OutputStream out = Files.newOutputStream(at); JarOutputStream j = new JarOutputStream(out, m)) {
            j.flush();
        }
        return at;
    }

    @Test
    @DisplayName("a classpath jar's Class-Path expands: relative, absolute, a space; a missing file is skipped")
    void classpathJarExpands(@TempDir Path dir) throws Exception {
        Path relative = jar(dir.resolve("lib/legend-engine-a.jar"), null);
        Path absolute = jar(dir.resolve("elsewhere/legend-engine-b.jar"), null);
        Path spaced = jar(dir.resolve("with space/legend-engine-c.jar"), null);
        Path launcher = jar(dir.resolve("launcher-classpath.jar"), String.join(" ",
                "lib/legend-engine-a.jar",
                absolute.toUri().toString(),
                "with%20space/legend-engine-c.jar",
                "lib/missing.jar"));
        assertEquals(List.of(launcher.toString(), relative.toString(), absolute.toString(),
                spaced.toString()), Classpath.expand(launcher.toString()));
    }

    @Test
    @DisplayName("an ordinary class path is itself, split")
    void plainClasspath(@TempDir Path dir) throws Exception {
        Path a = jar(dir.resolve("a.jar"), null);
        Path b = dir.resolve("classes");
        Files.createDirectories(b);
        assertEquals(List.of(a.toString(), b.toString()),
                Classpath.expand(a + File.pathSeparator + b));
    }
}
