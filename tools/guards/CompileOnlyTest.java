package com.legend.tools.guards;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.Runfile;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/**
 * THE BUILD IS ONLY COMPILES (docs/BUILD_REBUILD_DESIGN_2026_10_05.md, step 1). The build targets (//:java, //:web,
 * //:wasm, //:native) may perform only compiles and the file plumbing compiles need. A generator, a test, a
 * measurement or a validation that becomes part of one of them fails here, naming the target. The report lists every
 * action each build target's closure registers, by rule kind and mnemonic (compile_only.bzl, action_kinds_report).
 *
 * <p>An action that runs a program (it has a command line, as Bazel records) must match the per-tier allowlist of
 * (rule kind, mnemonic) PAIRS. One that runs no program (Bazel writes the file, link or tree itself) is plumbing
 * from any rule. The allowlist holds PAIRS. A target names its own mnemonic, but its rule kind
 * comes from Bazel, so a java_run calling itself "Javac" still fails. A pair joins only as a compile or as plumbing,
 * with its reason. Some pairs appear only on Linux or Windows (CppLink, JavaLauncherMaker): CI runs this test on all
 * three.
 */
class CompileOnlyTest {

    private static final Map<String, String> JAVA_LIBRARIES = Map.of(
            "java_library Javac", "compiles Java",
            "java_library Turbine", "a library's header jar, what its dependents compile against",
            "java_library JavaSourceJar", "a library's source jar, registered beside its compile",
            "java_library JavaResourceJar", "a library's resources, packed into its jar",
            "java_import JavaIjar", "Bazel's interface jar of a product jar (tools/deps/jars.bzl, http_jar)",
            "jvm_import StampJarManifest", "rules_jvm_external labels a Maven jar's manifest (its runtime jar)",
            "jvm_import CreateCompileJar", "rules_jvm_external's compile-only copy of a Maven jar");

    /** Tier -> "rule kind mnemonic" -> why it belongs in a build. Only actions that run a program are listed. */
    private static final Map<String, Map<String, String>> ALLOWED = Map.of(
            "java", with(JAVA_LIBRARIES, Map.ofEntries(
                    Map.entry("java_binary Javac", "a binary's own class jar (empty: the servers have no sources)"),
                    Map.entry("java_binary JavaSourceJar", "a binary's source jar, registered beside its compile"),
                    // a binary's packaging: registered, never built by //:java, which takes only the classpath jars
                    Map.entry("java_binary JavaDeployJar", "the deploy jar: registered, never built by //:java"),
                    Map.entry("java_binary JavaSingleJar", "the single-jar step: registered, never built by //:java"),
                    Map.entry("java_binary JavaLauncherMaker", "the Windows launcher: registered, never built by //:java"))),
            "wasm", with(JAVA_LIBRARIES, Map.of(
                    "_teavm_wasm TeaVM", "compiles Java to WebAssembly")),
            "native", with(JAVA_LIBRARIES, Map.ofEntries(
                    Map.entry("_native_image NativeImage", "compiles the database server to a native executable"),
                    Map.entry("cc_library CppCompile", "compiles zlib, which the native image links"),
                    Map.entry("cc_library CppArchive", "archives zlib for the image (libz.a)"),
                    Map.entry("cc_library CppLink", "zlib's shared library: registered on Linux and Windows, unused"),
                    Map.entry("cc_library CppModuleMap", "zlib's module map, registered beside its compile"))),
            "web", Map.ofEntries(
                    Map.entry("_esbuild_bundle Esbuild", "esbuild's native binary bundles TypeScript (tools/js/esbuild.bzl)"),
                    Map.entry("_copy_to_bin CopyFile", "a source file copied into the output tree for esbuild"),
                    Map.entry("js_library CopyFile", "a source file copied into the output tree"),
                    Map.entry("npm_package_store_internal NpmPackageExtract", "an npm package unpacked from its tarball")));

    /** Tier -> the compile it must show, so an empty or broken report cannot pass. */
    private static final Map<String, String> SIGNATURE = Map.of(
            "java", "java_library Javac",
            "web", "_esbuild_bundle Esbuild",
            "wasm", "_teavm_wasm TeaVM",
            "native", "_native_image NativeImage");

    private static Map<String, String> with(Map<String, String> a, Map<String, String> b) {
        Map<String, String> all = new TreeMap<>(a);
        all.putAll(b);
        return all;
    }

    @Test
    void theBuildTargetsOnlyCompile() throws IOException {
        List<String> report = Files.readAllLines(Runfile.of(System.getenv("BUILD_ACTION_KINDS")));
        Map<String, Set<String>> seen = new TreeMap<>();           // tier -> "kind mnemonic"
        Map<String, Set<String>> offenders = new TreeMap<>();      // "tier kind mnemonic" -> targets
        List<String> problems = new ArrayList<>();
        int execSkipped = 0;
        for (String line : report) {
            if (line.isBlank()) continue;
            String[] f = line.split("\t");
            if (f[1].equals("#exec-skipped")) {
                // the walk stops at exec-configuration targets (tools); with none skipped anywhere, the exec test no
                // longer matches Bazel's naming. A tier may skip none: //:web runs esbuild's downloaded binary.
                execSkipped += Integer.parseInt(f[2]);
                continue;
            }
            String pair = f[2] + " " + f[1];
            seen.computeIfAbsent(f[0], k -> new TreeSet<>()).add(pair);
            // an action with no command line runs no program (Bazel writes the file, link or tree itself): plumbing
            // from any rule, on any platform. Bazel records this (the report's last column); a mnemonic cannot fake it.
            // A validation line carries no such column: it always runs a program.
            boolean runsProgram = f.length < 5 || !f[4].equals("none");
            if (runsProgram && !ALLOWED.getOrDefault(f[0], Map.of()).containsKey(pair)) {
                offenders.computeIfAbsent(f[0] + " " + pair, k -> new TreeSet<>()).add(f[3]);
            }
        }
        if (execSkipped == 0) problems.add("no tier skipped an exec-configuration target: the exec test no longer matches");
        // NO NODE IN THE BUILD: esbuild runs natively (tools/js/esbuild.bzl). A js_binary is Node's launcher, so any
        // rule of that kind in a build target means Node is back.
        seen.forEach((tier, pairs) -> pairs.stream().filter(p -> p.startsWith("js_binary "))
                .forEach(p -> problems.add(tier + " runs Node: " + p)));
        SIGNATURE.forEach((tier, pair) -> {
            if (!seen.getOrDefault(tier, Set.of()).contains(pair)) problems.add(tier + " shows no " + pair);
        });
        String whole = "\nthe whole report:\n" + String.join("\n", report);
        assertTrue(problems.isEmpty(), "the report is not looking at the build: " + problems + whole);
        assertEquals(Map.of(), offenders,
                "the build targets perform actions that are not compiles (move them to a generator, test or check)"
                        + whole);
    }
}
