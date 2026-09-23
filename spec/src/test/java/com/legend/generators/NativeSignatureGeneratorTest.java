package com.legend.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.builtin.Pure;
import com.legend.generators.NativesGenerator.Decl;
import com.legend.generators.NativesGenerator.Row;
import com.legend.compiler.NameResolver;
import com.legend.model.FunctionDefinition;
import com.legend.model.NativeFunctionDefinition;
import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;
import com.legend.parser.Dialect;
import com.legend.parser.ElementParser;
import com.legend.protocol.Multiplicity;
import com.legend.protocol.TypeExpression;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * UPSTREAM BOUNDARY, batch 5 (program §3 C / D4): {@code Pure.java}'s
 * signature TEXT is generated — membership is OURS ({@code
 * native-membership.tsv}: constant, fqn, signature key), the spelling is
 * UPSTREAM's (the pinned checkouts' {@code native function} and bodied
 * {@code function} declarations, resolved to FQNs and rendered
 * canonically). Drift between the two is impossible by construction: the
 * parity test regenerates the block between the markers and asserts it
 * byte-equal, exactly the {@code prelude.pure} contract.
 *
 * <p>{@link NativesGenerator} rewrites the generated block of {@code Pure.java}
 * ({@code bazel run //:update_generated}, printing the divergence receipt); this
 * test asserts the committed file is its current output.
 *
 * <p>What the generator refuses, loudly: a membership row no upstream
 * declaration matches (the signature key is the join — a row with no
 * partner is a claim the spec does not make: move it to the Lite section
 * or delete it), an upstream declaration whose type names do not resolve
 * (a spec root moved), and a rendered signature that does not round-trip
 * through the catalog's own parser.
 */
class NativeSignatureGeneratorTest {

    static final Path MEMBERSHIP = CoreTree.resource("com/legend/builtin/native-membership.tsv");
    static final Path PURE_JAVA = CoreTree.main("com/legend/builtin/Pure.java");
    @Test
    @DisplayName("every membership constant's signature text in Pure.java is the generator's current output (regenerate: bazel run //:update_generated)")
    void signatureTextIsCurrent() throws IOException {
        if ("1".equals(System.getProperty("natives.bootstrap"))) {
            Files.writeString(MEMBERSHIP, membershipFromCatalog(), StandardCharsets.UTF_8);
            System.out.println("[natives] bootstrapped " + MEMBERSHIP + " from the catalog");
            return;
        }
        List<Row> rows = NativesGenerator.readMembership(MEMBERSHIP);
        Map<String, Decl> upstream = NativesGenerator.upstreamDeclarations(rows,
                PreludeGeneratorTest.engineRoot(), PreludeGeneratorTest.pureRoot());
        String dump = System.getProperty("natives.dump");
        if (dump != null) {
            // every upstream declaration of a membership FQN, one per line
            // (fqn, canonical key, canonical text, SOURCE FILE) — the input a
            // re-keying leg and the provenance review read instead of scraping
            // this test's report
            StringBuilder sb = new StringBuilder();
            for (Decl d : upstream.values()) {
                sb.append(d.fqn()).append('\t').append(d.key()).append('\t').append(d.text())
                        .append('\t').append(d.file()).append('\n');
            }
            Files.writeString(Path.of(dump), sb.toString(), StandardCharsets.UTF_8);
            System.out.println("[natives] dumped " + upstream.size() + " upstream declarations to " + dump);
            return;
        }
        NativesGenerator.Result r = NativesGenerator.compute(rows, upstream,
                Files.readAllLines(PURE_JAVA, StandardCharsets.UTF_8), false);
        // DIVERGENT rows — membership rows whose signature is not upstream's:
        // a shrink-only pin (each leg of batch 5 adopts upstream's text for a
        // bucket and lowers it), never a ledger of reasons. Zero is the done
        // criterion (program §4 row 5, D4): then the pin goes and this is a
        // plain parity assert.
        NativesGenerator.refuseOrphans(r.orphans());
        assertTrue(r.missing().isEmpty(), () -> "membership constants with no declaration line in"
                + " Pure.java: " + r.missing() + " — regenerate: bazel run //:update_generated");
        NativesGenerator.printSummary(r);
        assertTrue(r.drift().isEmpty(), () -> "Pure.java's signature text drifted from the membership"
                + " + the pinned checkouts (" + r.drift().size() + " constants) — regenerate:"
                + " bazel run //:update_generated");
    }

    @Test
    @DisplayName("every generated constant is claimed (membership = the implemented surface) and every catalog native outside the block is a Lite invention")
    void membershipIsTheCatalog() throws IOException {
        Set<String> members = new LinkedHashSet<>();
        for (Row r : NativesGenerator.readMembership(MEMBERSHIP)) {
            members.add(r.constant());
        }
        List<String> stray = new ArrayList<>();
        for (Map.Entry<String, NativeFunctionDefinition> c : constants().entrySet()) {
            if (members.contains(c.getKey())) {
                continue;
            }
            if (!c.getValue().qualifiedName().startsWith(Pure.Lite.PKG)) {
                stray.add(c.getKey() + " = " + c.getValue().qualifiedName());
            }
        }
        assertTrue(stray.isEmpty(), () -> "Pure.java constants outside the membership that are"
                + " not Lite inventions (hand-typed spec claims — add the row, or move to Lite):\n  "
                + String.join("\n  ", stray));
    }

    // ------------------------------------------------------------------
    // membership
    // ------------------------------------------------------------------

    /** Pure.java's public native-function constants by name (reflection:
     *  the constant names are ours and the code references them). */
    static Map<String, NativeFunctionDefinition> constants() {
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

    /** Bootstrap helper (run once at the batch-5 landing, then never): the
     *  membership TSV derived from today's hand-typed catalog. */
    static String membershipFromCatalog() {
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
        rows.sort(java.util.Comparator.comparing(Row::fqn).thenComparing(Row::key));
        for (Row r : rows) {
            sb.append(r.constant()).append('\t').append(r.fqn()).append('\t').append(r.key()).append('\n');
        }
        return sb.toString();
    }
}
