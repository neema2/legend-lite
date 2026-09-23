// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * GENERATES {@code NameResolver.java}'s {@code CORE_IMPORTS} list: the committed
 * file with the list between {@code CORE_IMPORTS = List.of(} and its {@code );}
 * replaced by the engine's {@code CompileContext.META_IMPORTS}, in the engine's
 * order (first-match resolution makes order semantic — see
 * {@link CoreImportsParityTest}).
 *
 * <pre>
 *   ImportsGenerator &lt;CompileContext.java&gt; &lt;NameResolver.java&gt; &lt;output&gt;
 * </pre>
 *
 * A build action (spec/BUILD.bazel, :gen_imports); the committed file is kept equal
 * to its output by //core:update_generated and its diff test.
 */
public final class ImportsGenerator {

    private static final String OPEN = "CORE_IMPORTS = List.of(";

    private ImportsGenerator() {}

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            throw new IllegalArgumentException(
                    "usage: ImportsGenerator <CompileContext.java> <NameResolver.java> <output>");
        }
        List<String> imports = metaImports(Files.readString(Path.of(args[0]), StandardCharsets.UTF_8));
        String generated = generate(imports, Files.readString(Path.of(args[1]), StandardCharsets.UTF_8));
        Files.writeString(Path.of(args[2]), generated, StandardCharsets.UTF_8);
        System.out.println("[imports] generated " + imports.size() + " packages");
    }

    /** {@code META_IMPORTS}' packages, in order, from CompileContext.java's text. */
    public static List<String> metaImports(String compileContextJava) {
        int at = compileContextJava.indexOf("META_IMPORTS");
        if (at < 0) {
            throw new IllegalStateException("CompileContext.META_IMPORTS not found — upstream moved it");
        }
        int end = compileContextJava.indexOf(");", at);
        Matcher m = Pattern.compile("\"(meta::[A-Za-z0-9_:]+)\"").matcher(compileContextJava.substring(at, end));
        List<String> out = new ArrayList<>();
        while (m.find()) {
            out.add(m.group(1));
        }
        return out;
    }

    /** NameResolver.java's text with its CORE_IMPORTS list replaced by {@code imports}. */
    public static String generate(List<String> imports, String nameResolverJava) {
        int start = nameResolverJava.indexOf(OPEN);
        if (start < 0) {
            throw new IllegalStateException("NameResolver.java has no '" + OPEN + "'");
        }
        start += OPEN.length();
        int end = nameResolverJava.indexOf(");", start);
        StringBuilder sb = new StringBuilder("\n");
        for (int k = 0; k < imports.size(); k++) {
            sb.append("            \"").append(imports.get(k)).append('"').append(k + 1 < imports.size() ? ",\n" : "");
        }
        return nameResolverJava.substring(0, start) + sb + nameResolverJava.substring(end);
    }
}
