package com.legend.tools.graalvm;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs native-image with the hermetic C toolchain's sysroot (Bazel workplan P1-09; patch
 * third_party/rules_graalvm_sysroot.patch). native-image calls the C compiler from its own temporary
 * directory, so the sysroot, an exec-root-relative path, is made absolute here, where the action starts.
 * No shell (G-03).
 *
 * <pre>sysroot_native_image &lt;native-image&gt; &lt;sysroot&gt; &lt;native-image arguments...&gt;</pre>
 */
public final class SysrootNativeImage {

    private SysrootNativeImage() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("usage: sysroot_native_image <native-image> <sysroot> <arguments...>");
        }
        List<String> command = new ArrayList<>();
        command.add(Path.of(args[0]).toAbsolutePath().toString());
        command.addAll(List.of(args).subList(2, args.length));
        command.add("-H:CCompilerOption=--sysroot=" + Path.of(args[1]).toAbsolutePath());
        Process p = new ProcessBuilder(command).inheritIO().start();
        System.exit(p.waitFor());
    }
}
