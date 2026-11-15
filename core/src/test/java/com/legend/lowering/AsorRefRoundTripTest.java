// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.model.AsorRef;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * THE OBJECT REFERENCE ROUND TRIP (C5-J's last round, A4, 2026-11-15). An object reference ({@code ASOR:}, the engine's
 * store-object-reference protocol) is written by two encoders and read by one decoder: the database builds each row's
 * reference in SQL ({@code SnapshotEnvelope.asorWrap}: the static prefix, the row's primary-key map framed by its
 * length, {@code lpad}'s width {@link AsorRef#SEG_LEN_WIDTH}, base64 without padding), the Java side spells the same
 * reference ({@link AsorRef#ref}), and {@link AsorRef#decode} reads it back by its segment walk (the resolver's
 * {@code objectReferenceIn} reads a reference so). Since C5-J item 3 the protocol is the model's ({@code model.AsorRef},
 * below the lowering and the resolver both), and nothing held the SQL side to it.
 *
 * <p>Here, on each in-process database, a reference the database built is the Java side's byte for byte and decodes to
 * the mapping, the root set id, the set id and the primary-key map it was built from -- every segment -- or the
 * database's dialect refuses the encoding by name. Measured 2026-11-15: DuckDB (the platform's own engine) and SQLite
 * build it; H2 has no base64 in SQL, and its dialect refuses it ("encodeBase64 reached H2, which has no to_base64()"). The
 * platform's own database must build it, and each database's outcome is pinned ({@code asor-round-trip/outcomes.tsv}:
 * the re-check of 2026-11-15, so a database that stops building it fails here). The rows are the model's data, loaded by the plan on the server's road (a
 * project's data fills the project's database). Red on revert: a segment
 * width changed on one side alone (the SQL side's {@code lpad} width) fails it.
 */
class AsorRefRoundTripTest {

    private static final String MODEL = """
            ###Pure
            Class test::Item { id: Integer[1]; code: String[1]; name: String[1]; }

            ###Relational
            Database store::DB
            (
                Table T_ITEM (ID INTEGER PRIMARY KEY, CODE VARCHAR(20) PRIMARY KEY, NAME VARCHAR(50))
            )
            ###Mapping
            Mapping test::M
            (
                test::Item: Relational
                {
                    ~mainTable [store::DB] T_ITEM
                    id: [store::DB] T_ITEM.ID,
                    code: [store::DB] T_ITEM.CODE,
                    name: [store::DB] T_ITEM.NAME
                }
            )
            ###Connection
            RelationalDatabaseConnection store::Conn { store: store::DB; CONNECTION }
            ###Runtime
            Runtime test::RT { mappings: [ test::M ]; connections: [ store::DB: [ environment: store::Conn ] ]; }
            ###Data
            Data test::D
            {
              Relational
              #{
                default.T_ITEM:
                  'ID,CODE,NAME\\n1,A,one\\n22,bb,two\\n333,a longer code,three\\n';
              }#
            }
            """;

    /** Each item's primary key, as the reference's map spells it: the key columns in order, a string quoted -- the
     *  rows in the order a graph fetch reads them, by their key. */
    private static final List<String> KEYS = List.of(
            "{\"pk$_0\":1,\"pk$_1\":\"A\"}",
            "{\"pk$_0\":22,\"pk$_1\":\"bb\"}",
            "{\"pk$_0\":333,\"pk$_1\":\"a longer code\"}");

    private static final Pattern REFERENCE = Pattern.compile("\"objectReference\":\"([^\"]+)\"");

    @Test
    void aReferenceTheDatabaseBuildsIsJavasAndDecodesToEverySegment() {
        String setId = com.legend.model.SetId.defaultFor("test::Item");
        List<String> built = new ArrayList<>();
        List<String> refused = new ArrayList<>();
        for (TestBackend backend : TestBackends.inProcess()) {
            List<String> references;
            try {
                references = references(backend);
            } catch (com.legend.sql.DialectCapability e) {
                // a database that cannot encode base64 in SQL refuses by name, never builds a broken reference
                assertTrue(String.valueOf(e.getMessage()).startsWith("encodeBase64 reached "), e.getMessage());
                refused.add(backend.name());
                continue;
            }
            assertEquals(KEYS.size(), references.size(), backend.name() + ": " + references);
            for (int i = 0; i < KEYS.size(); i++) {
                String reference = references.get(i);
                // byte for byte the Java side's reference: the static prefix's segments (the store kind, the mapping,
                // the root set id, the set id, the connection) and the key's, each framed by its length
                assertEquals(AsorRef.ref("test::M", setId, setId, KEYS.get(i)), reference,
                        backend.name() + ": row " + i);
                AsorRef.Ref decoded = AsorRef.decode(reference);
                assertNotNull(decoded, backend.name() + ": " + reference + " does not decode");
                assertEquals(new AsorRef.Ref("test::M", setId, setId, KEYS.get(i)), decoded,
                        backend.name() + ": row " + i);
            }
            built.add(backend.name());
        }
        assertTrue(built.contains(TestBackends.platform().name()),
                "the platform's own database builds a reference: built on " + built + ", refused on " + refused);
        // the measured outcome on each database, pinned (the re-check, 2026-11-15): a database that stops building it,
        // or starts, moves the ledger
        java.util.Map<String, String> measured = new java.util.TreeMap<>();
        built.forEach(b -> measured.put(b, "built"));
        refused.forEach(b -> measured.put(b, "refused"));
        assertEquals(pinned(), measured, "the round trip's outcome on each in-process database moved");
    }

    /** The pinned outcome on each in-process database ({@code asor-round-trip/outcomes.tsv}): its name, built or
     *  refused. */
    private static java.util.Map<String, String> pinned() {
        java.util.Map<String, String> out = new java.util.TreeMap<>();
        try (java.io.InputStream in = AsorRefRoundTripTest.class.getResourceAsStream("/asor-round-trip/outcomes.tsv")) {
            String text = new String(java.util.Objects.requireNonNull(in, "the ledger is on the class path").readAllBytes(),
                    java.nio.charset.StandardCharsets.UTF_8);
            for (String line : text.split("\n")) {
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                String[] cells = line.split("\t");
                out.put(cells[0], cells[1]);
            }
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return out;
    }

    /** The object references a serialization of every item builds on {@code backend}'s database, in order: the
     *  server's road (the project's database, its data the model's, loaded by the plan). */
    private static List<String> references(TestBackend backend) {
        Lean.Result r = Lean.service(MODEL.replace("CONNECTION", backend.connectionBody()), """
                test::Item.all()
                    ->graphFetch(#{test::Item {name}}#)
                    ->serialize(#{test::Item {name}}#,
                        meta::pure::graphFetch::execution::alloyConfig(false, false, false, false, true))
                """, "test::RT");
        List<String> references = new ArrayList<>();
        Matcher m = REFERENCE.matcher(r.json());
        while (m.find()) {
            references.add(m.group(1));
        }
        return references;
    }
}
