// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import com.legend.generators.NativesGenerator.Decl;
import com.legend.generators.NativesGenerator.Row;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Every upstream declaration of a membership FQN, one per line (fqn, canonical key, canonical text, SOURCE FILE):
 * the input a re-keying leg and the provenance review read instead of scraping a test's report. A build output,
 * {@code bazel build //spec:native_declarations}; it was the test's {@code -Dnatives.dump=<file>}, which wrote
 * anywhere (Bazel workplan P2-17).
 *
 * <pre>
 *   NativeDeclarations &lt;legend-engine root&gt; &lt;legend-pure root&gt; &lt;native-membership.tsv&gt; &lt;output&gt;
 * </pre>
 */
public final class NativeDeclarations {

    private NativeDeclarations() {}

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            throw new IllegalArgumentException("usage: NativeDeclarations <legend-engine root> <legend-pure root>"
                    + " <native-membership.tsv> <output>");
        }
        List<Row> rows = NativesGenerator.readMembership(Path.of(args[2]));
        Map<String, Decl> upstream = NativesGenerator.upstreamDeclarations(rows, Path.of(args[0]), Path.of(args[1]));
        StringBuilder sb = new StringBuilder();
        for (Decl d : upstream.values()) {
            sb.append(d.fqn()).append('\t').append(d.key()).append('\t').append(d.text())
                    .append('\t').append(d.file().replace('\\', '/')).append('\n');
        }
        Files.writeString(Path.of(args[3]), sb.toString(), StandardCharsets.UTF_8);
    }
}
