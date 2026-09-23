package com.legend.equivalence.harvest;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Stream;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

/**
 * THE FIXTURE HARVEST, as code both callers share: the engine's own grammar and
 * compiler tests run under the recording shims (every fixture they assemble lands
 * in {@link FixtureRecorder}'s dump), then deduped into the snapshot the corpus
 * reads. Tier 1 runs the published tests-jars; tier 2 compiles the checkout's
 * EXTENSION grammar test sources (unpublished) and runs those.
 *
 * <p>DETERMINISTIC: classes and test methods run in NAME order. The dump keeps the
 * first fixture of each source text, so the order decides which engine test is
 * credited with a shared fixture — and {@code getMethods()} has no defined order.
 * Before this, re-harvesting the same release reordered the snapshot and re-credited
 * fixtures (measured 2026-09-22: a no-op bump rewrote 774 lines of the snapshot and
 * 672 of the corpus manifest, whose ids number the fixtures in file order).
 */
public final class FixtureHarvest {

    private FixtureHarvest() {}

    /** Tier 2's modules: the checkout's extension grammar tests, which upstream
     *  publishes no tests-jar for (engine-relative). */
    public static final List<String> TIER2_MODULES = List.of(
            "legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-grammar",
            "legend-engine-xts-service/legend-engine-language-pure-dsl-service",
            "legend-engine-xts-persistence/legend-engine-xt-persistence-grammar");

    /** A class's public no-argument JUnit 4 tests, in name order. */
    static List<Method> testMethods(Class<?> c) {
        List<Method> out = new ArrayList<>();
        for (Method m : c.getMethods()) {
            if (m.getAnnotation(org.junit.Test.class) != null && m.getParameterCount() == 0) {
                out.add(m);
            }
        }
        out.sort(Comparator.comparing(Method::getName));
        return out;
    }

    /** Runs {@code classNames} (in the order given) under the shims; returns the receipt. */
    static String run(List<String> classNames, ClassLoader loader) {
        int classes = 0;
        int methods = 0;
        int invoked = 0;
        Map<String, Integer> failures = new TreeMap<>();
        Map<String, String> firstCause = new TreeMap<>();
        for (String cls : classNames) {
            if (!cls.substring(cls.lastIndexOf('.') + 1).startsWith("Test")) {
                continue;
            }
            Class<?> c;
            try {
                c = Class.forName(cls, false, loader);
            } catch (Throwable t) {
                failures.merge("load", 1, Integer::sum);
                continue;
            }
            if (c.isInterface() || Modifier.isAbstract(c.getModifiers())) {
                continue;
            }
            Object instance;
            try {
                instance = c.getDeclaredConstructor().newInstance();
            } catch (Throwable t) {
                failures.merge("instantiate", 1, Integer::sum);
                continue;
            }
            classes++;
            for (Method m : testMethods(c)) {
                methods++;
                try {
                    m.invoke(instance);
                    invoked++;
                } catch (Throwable t) {
                    // the shims recorded BEFORE any failure; a throw here just
                    // means the engine test went on to assert something the
                    // shim skipped
                    failures.merge("invoke-threw", 1, Integer::sum);
                    firstCause.putIfAbsent("invoke-threw", rootCause(t));
                }
            }
        }
        return "classes: " + classes + "; test methods: " + methods + "; completed: " + invoked
                + "; failures: " + failures + (firstCause.isEmpty() ? "" : "; first causes: " + firstCause);
    }

    private static String rootCause(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null && c.getCause() != c) {
            c = c.getCause();
        }
        return c.getClass().getName() + ": " + c.getMessage();
    }

    /** Tier 1: every test class of the given tests-jars, jar by jar, each jar's
     *  classes in name order. */
    public static String tier1(List<String> testJars, ClassLoader loader) throws IOException {
        List<String> names = new ArrayList<>();
        for (String jarPath : testJars) {
            List<String> inJar = new ArrayList<>();
            try (JarFile jar = new JarFile(jarPath)) {
                Enumeration<JarEntry> es = jar.entries();
                while (es.hasMoreElements()) {
                    String name = es.nextElement().getName();
                    if (name.endsWith(".class") && !name.contains("$")) {
                        inJar.add(name.substring(0, name.length() - 6).replace('/', '.'));
                    }
                }
            }
            inJar.sort(null);
            names.addAll(inJar);
        }
        return run(names, loader);
    }

    /** Tier 2: the compiled extension test classes under {@code root}, in name order,
     *  loaded ABOVE the shims' class loader so every test(...) records. */
    public static String tier2(Path root, ClassLoader parent) throws IOException {
        List<String> names = new ArrayList<>();
        try (Stream<Path> s = Files.walk(root)) {
            for (Path p : s.filter(f -> f.toString().endsWith(".class"))
                    .filter(f -> !f.toString().contains("$")).toList()) {
                names.add(root.relativize(p).toString().replace(java.io.File.separatorChar, '/')
                        .replace(".class", "").replace('/', '.'));
            }
        }
        names.sort(null);
        try (URLClassLoader loader = new URLClassLoader(new java.net.URL[] {root.toUri().toURL()}, parent)) {
            return run(names, loader);
        }
    }

    /**
     * Compiles tier 2's test sources into {@code out}: each module WHOLE, and when
     * that fails, each {@code Test*.java} file on its own (a file that does not
     * compile is skipped and named — a vanished class shows in the origin census).
     * The options are the ones the bump always used.
     */
    public static void compileTier2(Path engineRoot, Path out, String classpath, PrintStream log)
            throws IOException {
        JavaCompiler javac = ToolProvider.getSystemJavaCompiler();
        if (javac == null) {
            throw new IllegalStateException("no system Java compiler — tier 2 needs a JDK");
        }
        Files.createDirectories(out);
        for (String mod : TIER2_MODULES) {
            Path src = engineRoot.resolve(mod).resolve("src/test/java");
            if (!Files.isDirectory(src)) {
                throw new IllegalStateException("tier-2 module has no test sources: " + mod);
            }
            String name = mod.substring(mod.lastIndexOf('/') + 1);
            List<String> all;
            try (Stream<Path> s = Files.walk(src)) {
                all = s.filter(f -> f.toString().endsWith(".java")).map(Path::toString).sorted().toList();
            }
            List<String> base = List.of("-nowarn", "-proc:none", "-encoding", "UTF-8", "-d", out.toString(),
                    "-cp", classpath, "-sourcepath", src.toString());
            if (compile(javac, base, all)) {
                log.println("   tier 2 " + name + ": compiled whole");
                continue;
            }
            int ok = 0;
            Set<String> bad = new LinkedHashSet<>();
            for (String f : all) {
                if (!Path.of(f).getFileName().toString().startsWith("Test")) {
                    continue;
                }
                if (compile(javac, base, List.of(f))) {
                    ok++;
                } else {
                    bad.add(Path.of(f).getFileName().toString());
                }
            }
            log.println("   tier 2 " + name + ": compiled per file, ok=" + ok + ", FAILED:"
                    + (bad.isEmpty() ? " none" : " " + String.join(" ", bad)));
        }
    }

    private static boolean compile(JavaCompiler javac, List<String> options, List<String> files) {
        List<String> args = new ArrayList<>(options);
        args.addAll(files);
        // diagnostics are the bump's per-file logs, not this build's output
        return javac.run(null, java.io.OutputStream.nullOutputStream(), java.io.OutputStream.nullOutputStream(),
                args.toArray(String[]::new)) == 0;
    }

    /** The committed snapshot from a dump: its header, then each source text's
     *  FIRST row (the reader's own rule), in dump order. */
    public static String snapshot(Path dump) throws IOException {
        var json = new com.fasterxml.jackson.databind.ObjectMapper();
        String header = null;
        StringBuilder rows = new StringBuilder();
        Set<String> seen = new LinkedHashSet<>();
        for (String l : Files.readAllLines(dump, StandardCharsets.UTF_8)) {
            if (l.startsWith("#")) {
                if (header == null) {
                    header = l;
                }
                continue;
            }
            if (l.isBlank()) {
                continue;
            }
            if (seen.add(json.readTree(l).get("source").asText())) {
                rows.append(l).append('\n');
            }
        }
        if (header == null) {
            throw new IllegalStateException("the harvest dump has no header line: " + dump);
        }
        return header + "\n" + rows;
    }
}
