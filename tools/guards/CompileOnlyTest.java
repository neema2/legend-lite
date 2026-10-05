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
 * <p>An action that runs a program (it has a command line, as Bazel records) must match its tier's allowlist of
 * (rule kind, mnemonic) PAIRS. One that runs no program (Bazel writes the file, link or tree itself) is plumbing
 * from any rule. A target names its own mnemonic, so the pair includes the rule kind: an honest rule cannot hide
 * behind a mnemonic, and a java_run calling itself "Javac" fails. The rule kind is the name a rule is exported under,
 * so this catches mistakes, not a rule exported under an allowlisted name. A pair joins only as a compile or as
 * plumbing, with its reason. Some pairs appear only on Linux or Windows (CppLink, DefParser, JavaLauncherMaker): CI
 * runs this test on all three.
 *
 * <p>The fixture tier (tools/guards/BUILD.bazel) is what no build target may reach: a genrule, a java_run, a java_run
 * calling itself "Javac", and a target with a validation. The test expects exactly those flagged, so an upgrade of
 * Bazel or its rules that blinds the report fails here.
 */
class CompileOnlyTest {

    /** What every tier that compiles Java does for its libraries. */
    private static final Map<String, String> JAVA_LIBRARY = Map.of(
            "java_library Javac", "compiles Java",
            "java_library Turbine", "a library's header jar, what its dependents compile against",
            "java_library JavaSourceJar", "a library's source jar, registered beside its compile",
            "java_library JavaResourceJar", "a library's resources, packed into its jar");

    /** Tier -> "rule kind mnemonic" -> why it belongs in a build. Only actions that run a program are listed. */
    private static final Map<String, Map<String, String>> ALLOWED = Map.of(
            "java", with(JAVA_LIBRARY, Map.of(
                    "jvm_import CreateCompileJar", "rules_jvm_external's compile-only copy of a driver jar",
                    "jvm_import StampJarManifest", "rules_jvm_external labels a driver jar's manifest (its runtime jar)",
                    "java_binary Javac", "a binary's own class jar (empty: the servers have no sources)",
                    "java_binary JavaSourceJar", "a binary's source jar, registered beside its compile",
                    // a binary's packaging: registered, never built by //:java, which takes only the classpath jars
                    "java_binary JavaDeployJar", "the deploy jar: registered, never built by //:java",
                    "java_binary JavaSingleJar", "the single-jar step: registered, never built by //:java",
                    "java_binary JavaLauncherMaker", "the Windows launcher: registered, never built by //:java")),
            "wasm", with(JAVA_LIBRARY, Map.of(
                    "jvm_import CreateCompileJar", "rules_jvm_external's compile-only copy of a TeaVM jar",
                    "jvm_import StampJarManifest", "rules_jvm_external labels a TeaVM jar's manifest (its runtime jar)",
                    "_teavm_wasm TeaVM", "compiles Java to WebAssembly")),
            "native", with(JAVA_LIBRARY, Map.of(
                    "_native_image NativeImage", "compiles the database server to a native executable",
                    "cc_library CppCompile", "compiles zlib, which the native image links",
                    "cc_library CppArchive", "archives zlib for the image (libz.a)",
                    "cc_library CppLink", "zlib's shared library: registered on Linux and Windows, unused",
                    "cc_library DefParser", "zlib's DLL export list: registered on Windows (MSVC), unused")),
            "web", Map.of(
                    "_run_binary Esbuild", "bundles TypeScript for the browser",
                    "_copy_to_bin CopyFile", "a source file copied into the output tree for esbuild",
                    "js_library CopyFile", "a source file copied into the output tree",
                    "npm_package_store_internal NpmPackageExtract", "an npm package unpacked from its tarball"),
            // an honest compile beside the fixtures is not flagged
            "fixture", Map.of(
                    "java_library Javac", "the fixtures' empty library",
                    "java_library JavaSourceJar", "the fixtures' empty library"));

    /** Tier -> the compile it must show, so an empty or broken report cannot pass. */
    private static final Map<String, String> SIGNATURE = Map.of(
            "java", "java_library Javac",
            "web", "_run_binary Esbuild",
            "wasm", "_teavm_wasm TeaVM",
            "native", "_native_image NativeImage");

    /**
     * Tier -> a tool its walk must stop at, as an exec-configuration target. If the walk goes through it instead,
     * Bazel's naming of exec configurations changed. NullAway is every first-party library's javac plugin.
     */
    private static final Map<String, String> SKIPS = Map.of(
            "java", "//tools/nullaway:nullaway",
            "native", "//tools/nullaway:nullaway",
            "wasm", "//tools/nullaway:nullaway");

    /** The fixture tier's targets, and the pair each must be flagged for. */
    private static final Map<String, String> FIXTURES = Map.of(
            "//tools/guards:compile_only_fixture_genrule", "genrule Genrule",
            "//tools/guards:compile_only_fixture_generator", "_java_run JavaRun",
            "//tools/guards:compile_only_fixture_impostor", "_java_run Javac",
            "//tools/guards:compile_only_fixture_validated", "compile_only_validated_fixture Validation");

    private static Map<String, String> with(Map<String, String> a, Map<String, String> b) {
        Map<String, String> all = new TreeMap<>(a);
        all.putAll(b);
        return all;
    }

    /** The report, by tier: the pairs it shows, the offenders (pair -> targets), the exec targets it skipped. */
    private static final class Report {
        final List<String> lines;
        final Map<String, Set<String>> seen = new TreeMap<>();
        final Map<String, Map<String, Set<String>>> offenders = new TreeMap<>();
        final Map<String, Set<String>> skipped = new TreeMap<>();

        Report() throws IOException {
            lines = Files.readAllLines(Runfile.of(System.getenv("BUILD_ACTION_KINDS")));
            for (String line : lines) {
                if (line.isBlank()) continue;
                String[] f = line.split("\t");
                if (f[1].equals("#exec-skipped")) {
                    skipped.computeIfAbsent(f[0], k -> new TreeSet<>()).add(label(f[2]));
                    continue;
                }
                String pair = f[2] + " " + f[1];
                seen.computeIfAbsent(f[0], k -> new TreeSet<>()).add(pair);
                // an action with no command line runs no program (Bazel writes the file, link or tree itself):
                // plumbing from any rule, on any platform. Bazel records this (the report's last column); a mnemonic
                // cannot fake it. A validation line carries no such column: it always runs a program.
                boolean runsProgram = f.length < 5 || !f[4].equals("none");
                if (runsProgram && !ALLOWED.getOrDefault(f[0], Map.of()).containsKey(pair)) {
                    offenders.computeIfAbsent(f[0], k -> new TreeMap<>())
                            .computeIfAbsent(pair, k -> new TreeSet<>()).add(label(f[3]));
                }
            }
        }

        String whole() {
            return "\nthe whole report:\n" + String.join("\n", lines);
        }

        /** "@@//a:b" -> "//a:b": the main repository's labels as BUILD files write them. */
        private static String label(String s) {
            return s.startsWith("@@//") ? s.substring(2) : s;
        }
    }

    @Test
    void theBuildTargetsOnlyCompile() throws IOException {
        Report report = new Report();
        List<String> problems = new ArrayList<>();
        SKIPS.forEach((tier, tool) -> {
            if (!report.skipped.getOrDefault(tier, Set.of()).contains(tool)) {
                problems.add(tier + " did not stop at the tool " + tool + ": the exec test no longer matches");
            }
        });
        // the walk must never take the product for a tool: every first-party target it skipped is under //tools, or
        // a bundle's esbuild launcher (js_run_binary's <name>__js_binary, the npm route's tool)
        report.skipped.forEach((tier, labels) -> labels.stream()
                .filter(l -> l.startsWith("//") && !l.startsWith("//tools/") && !l.endsWith("__js_binary"))
                .forEach(l -> problems.add(tier + " skipped " + l + " as an exec-configuration tool")));
        // a java_binary is walked only to its runtime classpath (compile_only.bzl), which is what //:java takes; in
        // another tier its data and launcher would go unchecked
        report.seen.forEach((tier, pairs) -> pairs.stream()
                .filter(p -> p.startsWith("java_binary ") && !tier.equals("java") && !tier.equals("fixture"))
                .forEach(p -> problems.add(tier + " has a java_binary: " + p)));
        SIGNATURE.forEach((tier, pair) -> {
            if (!report.seen.getOrDefault(tier, Set.of()).contains(pair)) problems.add(tier + " shows no " + pair);
        });
        assertTrue(problems.isEmpty(), "the report is not looking at the build: " + problems + report.whole());
        Map<String, Map<String, Set<String>>> offenders = new TreeMap<>(report.offenders);
        offenders.remove("fixture");
        assertEquals(Map.of(), offenders,
                "the build targets perform actions that are not compiles (move them to a generator, test or check)"
                        + report.whole());
    }

    @Test
    void theFixturesAreFlaggedExactly() throws IOException {
        Report report = new Report();
        Map<String, Set<String>> expected = new TreeMap<>();
        FIXTURES.forEach((target, pair) -> expected.computeIfAbsent(pair, k -> new TreeSet<>()).add(target));
        assertEquals(expected, report.offenders.getOrDefault("fixture", Map.of()),
                "the guard no longer flags exactly what it exists to catch (did a Bazel or rules upgrade change what "
                        + "the report sees?)" + report.whole());
    }
}
