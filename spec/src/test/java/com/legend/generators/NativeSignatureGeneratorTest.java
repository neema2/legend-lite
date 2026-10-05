package com.legend.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.builtin.Pure;
import com.legend.generators.NativesGenerator.Decl;
import com.legend.generators.NativesGenerator.Row;
import com.legend.model.NativeFunctionDefinition;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        List<Row> rows = NativesGenerator.readMembership(MEMBERSHIP);
        Map<String, Decl> upstream = NativesGenerator.upstreamDeclarations(rows,
                PreludeGeneratorTest.engineRoot(), PreludeGeneratorTest.pureRoot());
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
        for (Map.Entry<String, NativeFunctionDefinition> c : NativeMembershipDraft.constants().entrySet()) {
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
}
