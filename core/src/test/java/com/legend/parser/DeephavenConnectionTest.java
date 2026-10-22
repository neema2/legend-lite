// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser;

import com.legend.door.ModelFromText;
import com.legend.model.AuthenticationSpec;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionSpecification;
import com.legend.protocol.ProtocolEmitter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A Deephaven connection (docs/DEEPHAVEN_PLAN_2026_10_22.md §4): {@code type: Deephaven}, the {@code Deephaven}
 * specification and {@code PreSharedKey} authentication, each a DECLARED lite extension
 * (docs/OWN_CORPUS_DECISIONS.md LITE-DESIGN-deephaven-backend). lite's dialect reads them; legend-engine's refuses
 * each by name, at its token; the model door refuses any one of them without the other two; and, as SQLite's, they
 * have no legend-engine JSON.
 */
class DeephavenConnectionTest {

    private static String connection(String type, String specification, String auth) {
        return "###Connection\n"
                + "RelationalDatabaseConnection risk::live::Conn\n"
                + "{\n"
                + "  store: refdata::Ref;\n"
                + "  type: " + type + ";\n"
                + "  specification: " + specification + ";\n"
                + "  auth: " + auth + ";\n"
                + "}\n";
    }

    private static final String SPEC = "Deephaven { host: 'dh.internal'; port: 10000; useTls: true; }";
    private static final String AUTH = "PreSharedKey { keyVaultReference: 'deephaven.psk'; }";
    private static final String TEXT = connection("Deephaven", SPEC, AUTH);

    private static ParseException refused(String text, Dialect dialect) {
        return assertThrows(ParseException.class, () -> ModelFromText.parse(text, dialect));
    }

    private static void refusedNaming(String text, Dialect dialect, String message) {
        ParseException e = refused(text, dialect);
        assertTrue(String.valueOf(e.getMessage()).contains(message), e.getMessage());
    }

    @Test
    void litesDialectReadsTheConnection() {
        ConnectionDefinition c = (ConnectionDefinition) ModelFromText.parse(TEXT, Dialect.LEGEND_LITE).elements()
                .get(0);
        assertEquals(new ConnectionDefinition("risk::live::Conn", "refdata::Ref",
                ConnectionDefinition.DatabaseType.Deephaven,
                new ConnectionSpecification.Deephaven("dh.internal", 10000, true),
                new AuthenticationSpec.PreSharedKey("deephaven.psk")), c);
    }

    @Test
    void theEnginesDialectRefusesEachWordAtItsToken() {
        ParseException type = refused(TEXT, Dialect.LEGEND_ENGINE);
        assertTrue(String.valueOf(type.getMessage()).contains("Unknown database type 'Deephaven'"), type.getMessage());
        assertEquals(5, type.line());
        refusedNaming(connection("Postgres", SPEC, "Test"), Dialect.LEGEND_ENGINE,
                "unsupported datasource specification: Deephaven");
        refusedNaming(connection("Postgres", "Static { host: 'h'; port: 5432; name: 'n'; }", AUTH),
                Dialect.LEGEND_ENGINE, "unsupported auth strategy: PreSharedKey");
    }

    @Test
    void everySpecificationKeyIsRequired() {
        refusedNaming(connection("Deephaven", "Deephaven { port: 10000; useTls: true; }", AUTH), Dialect.LEGEND_LITE,
                "Deephaven needs host");
        refusedNaming(connection("Deephaven", "Deephaven { host: 'h'; useTls: true; }", AUTH), Dialect.LEGEND_LITE,
                "Deephaven needs port");
        refusedNaming(connection("Deephaven", "Deephaven { host: 'h'; port: 10000; }", AUTH), Dialect.LEGEND_LITE,
                "Deephaven needs useTls");
        refusedNaming(connection("Deephaven", SPEC, "PreSharedKey { }"), Dialect.LEGEND_LITE,
                "PreSharedKey needs keyVaultReference");
    }

    @Test
    void anUnknownOrRepeatedKeyIsRefusedByName() {
        refusedNaming(connection("Deephaven", "Deephaven { host: 'h'; port: 10000; useTls: true; tls: true; }", AUTH),
                Dialect.LEGEND_LITE, "unknown Deephaven key: tls");
        refusedNaming(connection("Deephaven", "Deephaven { host: 'h'; host: 'i'; port: 10000; useTls: true; }", AUTH),
                Dialect.LEGEND_LITE, "host");
        refusedNaming(connection("Deephaven", SPEC, "PreSharedKey { psk: 'the-key'; }"), Dialect.LEGEND_LITE,
                "unknown PreSharedKey key: psk");
    }

    @Test
    void theModelDoorTakesAllThreeWordsOrNone() {
        refusedNaming(connection("Deephaven", "Static { host: 'h'; port: 10000; name: 'n'; }", AUTH),
                Dialect.LEGEND_LITE, "a Deephaven connection's specification is Deephaven { host; port; useTls; }");
        refusedNaming(connection("Postgres", SPEC, "Test"), Dialect.LEGEND_LITE,
                "the Deephaven specification is a Deephaven connection's (type: Deephaven), not Postgres's");
        refusedNaming(connection("Deephaven", SPEC, "Test"), Dialect.LEGEND_LITE,
                "a Deephaven connection authenticates with PreSharedKey { keyVaultReference; }");
        refusedNaming(connection("Postgres", "Static { host: 'h'; port: 5432; name: 'n'; }", AUTH),
                Dialect.LEGEND_LITE, "PreSharedKey is a Deephaven connection's authentication (type: Deephaven)");
    }

    @Test
    void aPortIsATcpPort() {
        refusedNaming(connection("Deephaven", "Deephaven { host: 'h'; port: 0; useTls: false; }", AUTH),
                Dialect.LEGEND_LITE, "a Deephaven port is a TCP port (1 to 65535), not 0");
        refusedNaming(connection("Deephaven", "Deephaven { host: 'h'; port: 70000; useTls: false; }", AUTH),
                Dialect.LEGEND_LITE, "not 70000");
    }

    /** Each word refused by name when its records are written as legend-engine's JSON (the records, not the model:
     *  the model door's own check is not in the way). */
    @Test
    void itHasNoLegendEngineJson() {
        var spec = PmcdParser.parse(connection("Postgres", SPEC, "Test"), Dialect.LEGEND_LITE).model();
        IllegalStateException s = assertThrows(IllegalStateException.class, () -> ProtocolEmitter.emit(spec));
        assertEquals("Deephaven datasource specs have no engine wire shape (lite backend)", s.getMessage());
        var auth = PmcdParser.parse(connection("Postgres", "Static { host: 'h'; port: 5432; name: 'n'; }", AUTH),
                Dialect.LEGEND_LITE).model();
        IllegalStateException a = assertThrows(IllegalStateException.class, () -> ProtocolEmitter.emit(auth));
        assertEquals("PreSharedKey authentication has no engine wire shape (lite backend)", a.getMessage());
    }
}
