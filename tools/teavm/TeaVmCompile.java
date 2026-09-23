package com.legend.tools.teavm;

import org.teavm.tooling.ConsoleTeaVMToolLog;
import org.teavm.tooling.TeaVMProblemRenderer;
import org.teavm.tooling.TeaVMTargetType;
import org.teavm.tooling.TeaVMTool;
import org.teavm.tooling.TeaVMToolException;
import org.teavm.vm.TeaVMOptimizationLevel;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

/**
 * TeaVM as a build action: compiles one main class, and everything it reaches, to
 * a WebAssembly-GC module ({@code classes.wasm}), and writes TeaVM's host runtime
 * beside it. The {@code teavm_wasm} rule (defs.bzl) runs it; Bazel hands it the
 * program's jars, so the class path is the build graph's, never a guess.
 *
 * <p>Usage: {@code TeaVmCompile <out-dir> <main-class> @<classpath-file>}, the file
 * one jar per line (a Bazel param file — a class path outgrows a command line).
 *
 * <p>The settings are the ones {@code research/wasm/pom.xml} gave the TeaVM Maven
 * plugin (the plugin is a thin wrapper over {@link TeaVMTool}): ADVANCED
 * optimization, minified, no debug information, no incremental cache — an action
 * starts clean, and its output is a function of its inputs alone. The runtime is
 * the MODULAR, unminified {@code wasm-gc-module-runtime.js}, the one DataCube's
 * {@code WasmPlanner} and the differentials import.
 */
public final class TeaVmCompile {

    private static final String RUNTIME = "org/teavm/backend/wasm/wasm-gc-module-runtime.js";

    private TeaVmCompile() {
    }

    public static void main(String[] args) throws IOException, TeaVMToolException {
        if (args.length != 3 || !args[2].startsWith("@")) {
            throw new IllegalArgumentException(
                    "usage: TeaVmCompile <out-dir> <main-class> @<classpath-file>");
        }
        File out = new File(args[0]);
        String mainClass = args[1];
        List<File> classPath = new ArrayList<>();
        for (String line : Files.readAllLines(Path.of(args[2].substring(1)), StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                classPath.add(new File(line.strip()));
            }
        }
        Files.createDirectories(out.toPath());

        URL[] urls = new URL[classPath.size()];
        for (int i = 0; i < urls.length; i++) {
            urls[i] = classPath.get(i).toURI().toURL();
        }
        ConsoleTeaVMToolLog log = new ConsoleTeaVMToolLog(false);
        TeaVMTool tool = new TeaVMTool();
        try (URLClassLoader program = new URLClassLoader(urls, TeaVmCompile.class.getClassLoader())) {
            tool.setLog(log);
            tool.setTargetType(TeaVMTargetType.WEBASSEMBLY_GC);
            tool.setMainClass(mainClass);
            tool.setEntryPointName("main");
            tool.setTargetDirectory(out);
            tool.setClassLoader(program);
            tool.setClassPath(classPath);
            tool.setOptimizationLevel(TeaVMOptimizationLevel.ADVANCED);
            tool.setObfuscated(true);
            tool.setIncremental(false);
            tool.setCacheDirectory(null);
            tool.setDebugInformationGenerated(false);
            tool.setSourceMapsFileGenerated(false);
            tool.generate();
        }
        TeaVMProblemRenderer.describeProblems(
                tool.getDependencyInfo().getCallGraph(), tool.getProblemProvider(), log);
        if (!tool.getProblemProvider().getSevereProblems().isEmpty()) {
            // a severe problem is a method the class library does not carry, or a
            // class it cannot find: the module is not what the source says
            System.exit(1);
        }
        try (InputStream runtime = TeaVmCompile.class.getClassLoader().getResourceAsStream(RUNTIME)) {
            if (runtime == null) {
                throw new IllegalStateException(RUNTIME + " is not in teavm-core");
            }
            Files.copy(runtime, new File(out, "wasm-gc-module-runtime.js").toPath(),
                    StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
