// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import com.legend.builtin.Pure;
import com.legend.generators.NativesGenerator.Row;
import com.legend.model.NativeFunctionDefinition;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A DRAFT of {@code native-membership.tsv} from {@code Pure.java}'s constants as compiled: every public
 * native-function constant outside {@code Pure.Lite}, keyed by its own signature. The membership is hand-owned
 * (batch 5: membership is OURS); this is the starting point a person finishes, never a generated file:
 * {@code bazel run //core:draft_native_membership} writes it over the committed file, and {@code git diff} shows
 * what to keep. {@code NativeSignatureGeneratorTest} is the check. (It was the test's {@code -Dnatives.bootstrap},
 * which wrote into the source tree through the runfiles; Bazel workplan P2-17.)
 *
 * <pre>
 *   NativeMembershipDraft &lt;output&gt;
 * </pre>
 */
public final class NativeMembershipDraft {

    private NativeMembershipDraft() {}

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: NativeMembershipDraft <output>");
        }
        Files.writeString(Path.of(args[0]), draft(), StandardCharsets.UTF_8);
    }

    /** Pure.java's public native-function constants by name (reflection: the constant names are ours and the
     *  code references them). */
    public static Map<String, NativeFunctionDefinition> constants() {
        Map<String, NativeFunctionDefinition> out = new TreeMap<>();
        for (Field f : Pure.class.getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers()) && Modifier.isPublic(f.getModifiers())
                    && f.getType() == NativeFunctionDefinition.class) {
                try {
                    out.put(f.getName(), (NativeFunctionDefinition) f.get(null));
                } catch (IllegalAccessException ex) {
                    throw new IllegalStateException(ex);
                }
            }
        }
        return out;
    }

    /** The membership TSV derived from the catalog: sorted by fqn, then key. */
    static String draft() {
        StringBuilder sb = new StringBuilder("# native-membership.tsv — the platform's implemented"
                + " surface (upstream boundary program, batch 5).\n"
                + "# Membership is OURS; the signature text is generated from the pinned checkouts.\n"
                + "# constant\tfqn\tsignatureKey — sorted by fqn, then key.\n");
        List<Row> rows = new ArrayList<>();
        for (Map.Entry<String, NativeFunctionDefinition> c : constants().entrySet()) {
            NativeFunctionDefinition d = c.getValue();
            if (d.qualifiedName().startsWith(Pure.Lite.PKG)) {
                continue;
            }
            rows.add(new Row(c.getKey(), d.qualifiedName(), NativesGenerator.canonicalKey(d)));
        }
        rows.sort(Comparator.comparing(Row::fqn).thenComparing(Row::key));
        for (Row r : rows) {
            sb.append(r.constant()).append('\t').append(r.fqn()).append('\t').append(r.key()).append('\n');
        }
        return sb.toString();
    }
}
