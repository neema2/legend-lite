// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.error.ModelException;
import com.legend.json.Json;
import com.legend.model.DatabaseDefinition;
import com.legend.model.RelationalDataType;
import com.legend.protocol.ModelComposer;
import com.legend.protocol.ModelReader;
import com.legend.protocol.Multiplicity;
import com.legend.protocol.Protocol;
import com.legend.protocol.ProtocolEmitter;
import com.legend.protocol.SourceInformation;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code ###Lakehouse}'s Ingest (docs/DESK_DATA_ROADMAP_2026_10_29.md A1; the declared extension LITE-DESIGN-lakehouse):
 * lite's dialect reads Legend's written form and lite's additions, the engine's refuses the section in its own words,
 * the records go to JSON and back and to text and back unchanged, Legend's own JSON (legend-studio's round-trip
 * fixtures, {@code src/test/resources/lakehouse}) reads and writes back as Legend wrote it, and a store that includes
 * an ingest reads its datasets as tables -- a dataset made by a read of another store's table taking that table's
 * columns -- with the design's checks refused by name.
 */
class LakehouseIngestTest {

    /** Legend's form, as Studio shows it written. */
    private static final String LEGENDS = """
            ###Lakehouse
            Ingest test::IngestDefinition Snapshot<CSV> owner=AppDir(production='297484')
            [
              TEST_TABLE( int_val: Int, varchar_val: Varchar(500) ) pk=[ int_val ];
            ]
            """;

    /** The desk's shapes: a stream from topics, latest by key and appended with a retention, a pull, a group. */
    private static final String DESK = """
            ###Relational
            Database desk::Topics
            (
              Table "tp.trades" (PRINT_ID VARCHAR(64) PRIMARY KEY, SYMBOL VARCHAR(16) NOT NULL, PRICE DOUBLE, QTY INTEGER)
              Table "tp.quotes" (QUOTE_ID VARCHAR(64) PRIMARY KEY, SYMBOL VARCHAR(16) NOT NULL, BID DOUBLE, ASK DOUBLE)
            )
            Database desk::SecMaster
            (
              Table INSTRUMENT_V (INSTRUMENT_ID VARCHAR(64) PRIMARY KEY, NAME VARCHAR(200))
            )

            ###Lakehouse
            Ingest desk::Feed Stream owner=AppDir(production='111111', prodParallel='222222')
            [
              MARKET_TRADE( |#>{desk::Topics."tp.trades"}# ) pk=[ PRINT_ID ] writeMode=AppendOnly retention=Last('PT8H');
              MARKET_QUOTE( |#>{desk::Topics."tp.quotes"}# ) pk=[ QUOTE_ID ] writeMode=LatestByKey;
              WORLD( item: Varchar(16)[1], state: Varchar(16), detail: Varchar(256) ) pk=[ item ] writeMode=LatestByKey;
            ]
            Ingest desk::Reference Pull<Daily('06:00', 'America/New_York')> writeMode=LatestByKey
            [
              INSTRUMENT( |#>{desk::SecMaster.INSTRUMENT_V}# ) pk=[ INSTRUMENT_ID ];
            ]
            Ingest desk::Limits Pull<Every('PT15M')> datasetGroup=RISK
            [
              RISK_LIMIT( |#>{desk::SecMaster.INSTRUMENT_V}# ) pk=[ INSTRUMENT_ID ] writeMode=LatestByKey;
            ]
            Ingest desk::Positions Delta<Parquet>
            [
              POSITION( POSITION_ID: Varchar(200)[1], QUANTITY: Double, OPENED: StrictDate, AT: Timestamp, OPEN: Boolean, COST: Decimal(18, 4) ) pk=[ POSITION_ID ] writeMode=AppendOnly retention=LastRows(200000) privacyClassification=DP10 storageLayoutClusterColumns=[ OPENED ];
            ]
            Ingest desk::Status Stream<Json>
            [
              STATUS( name: String[1], at: DateTime ) pk=[ name ] writeMode=LatestByKey;
            ]

            ###Relational
            Database desk::DB
            (
              include Ingest desk::Feed
              include Ingest desk::Reference
              include Ingest desk::Limits
              include Ingest desk::Positions
            )

            ###Connection
            RelationalDatabaseConnection desk::Conn { store: desk::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }

            ###Runtime
            Runtime desk::RT { mappings: []; connections: [ desk::DB: [ c: desk::Conn ] ]; }
            """;

    private static List<Protocol.Element> parse(String text) {
        List<Protocol.Element> out = new java.util.ArrayList<>();
        for (Protocol.Element e : PmcdParser.parse(text, Dialect.LEGEND_LITE).model().elements()) {
            if (!(e instanceof Protocol.PSectionIndex)) {
                out.add(e);
            }
        }
        return out;
    }

    private static Protocol.PIngest ingest(String text, String name) {
        for (Protocol.Element e : parse(text)) {
            if (e instanceof Protocol.PIngest i && i.qualifiedName().equals(name)) {
                return i;
            }
        }
        throw new AssertionError("no ingest " + name);
    }

    @Test
    void litesDialectReadsLegendsForm() {
        Protocol.PIngest i = ingest(LEGENDS, "test::IngestDefinition");
        assertEquals(new Protocol.PIngestFiles("Snapshot", "CSV"), i.readMode());
        assertEquals(new Protocol.PIngestOwner("297484", null), i.owner());
        Protocol.PIngestDataset d = i.datasets().get(0);
        assertEquals("TEST_TABLE", d.name());
        assertEquals(List.of("int_val"), d.primaryKey());
        Protocol.PIngestColumns cols = (Protocol.PIngestColumns) d.source();
        assertEquals(List.of("int_val", "varchar_val"), cols.columns().stream().map(c -> c.name()).toList());
        assertEquals(Multiplicity.range(0, 1), cols.columns().get(0).multiplicity(), "a relation column's [0..1]");
    }

    @Test
    void litesDialectReadsLitesAdditions() {
        Protocol.PIngest feed = ingest(DESK, "desk::Feed");
        assertEquals(new Protocol.PIngestStream(null), feed.readMode());
        assertEquals(new Protocol.PIngestOwner("111111", "222222"), feed.owner());
        assertEquals("append_only", feed.datasets().get(0).writeMode());
        assertEquals(new Protocol.PIngestLast("PT8H"), feed.datasets().get(0).retention());
        assertEquals("latest_by_key", feed.datasets().get(1).writeMode());
        assertTrue(feed.datasets().get(0).source() instanceof Protocol.PIngestFunction);
        assertEquals(new Protocol.PIngestPull(new Protocol.PIngestDaily("06:00", "America/New_York")),
                ingest(DESK, "desk::Reference").readMode());
        assertEquals("latest_by_key", ingest(DESK, "desk::Reference").writeMode());
        Protocol.PIngest limits = ingest(DESK, "desk::Limits");
        assertEquals(new Protocol.PIngestPull(new Protocol.PIngestEvery("PT15M")), limits.readMode());
        assertEquals("RISK", limits.datasetGroup());
        Protocol.PIngestDataset position = ingest(DESK, "desk::Positions").datasets().get(0);
        assertEquals(new Protocol.PIngestLastRows(200000), position.retention());
        assertEquals("DP10", position.privacyClassification());
        assertEquals(List.of("OPENED"), position.storageLayoutClusterColumns());
        assertEquals(new Protocol.PIngestStream("Json"), ingest(DESK, "desk::Status").readMode());
    }

    @Test
    void theEnginesDialectRefusesTheSectionInItsOwnWords() {
        ParseException e = assertThrows(ParseException.class,
                () -> PmcdParser.parse(LEGENDS, Dialect.LEGEND_ENGINE));
        assertTrue(String.valueOf(e.getMessage()).contains("'Lakehouse' is not a known section parser"),
                e.getMessage());
    }

    /** text -> records -> JSON -> records: the same JSON, with spans and without. And text -> records -> text ->
     *  records: the same records, spans aside (compared as their JSON without spans, which the emitter writes whole). */
    @Test
    void theRecordsGoToJsonAndTextAndBack() {
        for (String text : List.of(LEGENDS, DESK)) {
            for (Protocol.Element e : parse(text)) {
                String json = ProtocolEmitter.emitElement(e);
                Protocol.Element read = ModelReader.readElement(json);
                assertEquals(json, ProtocolEmitter.emitElement(read));
                String spanless = SourceInformation.stripAll(json);
                assertEquals(spanless, SourceInformation.stripAll(ProtocolEmitter.emitElement(
                        ModelReader.readElement(spanless))));
                if (e instanceof Protocol.PIngest) {
                    String printed = ModelComposer.element(e);
                    Protocol.Element again = ingest("###Lakehouse\n" + printed + "\n", Protocol.pathOf(e));
                    assertEquals(spanless, SourceInformation.stripAll(ProtocolEmitter.emitElement(again)), printed);
                }
            }
        }
    }

    private static String resource(String name) throws IOException {
        try (InputStream in = LakehouseIngestTest.class.getResourceAsStream("/lakehouse/" + name)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /** Legend's own JSON (legend-studio's TEST_DATA__IngestIncludeTransitiveRoundtrip): read, and written back as
     *  Legend wrote it, field for field. */
    @Test
    void legendsJsonReadsAndWritesBackAsLegendWroteIt() throws IOException {
        Json.Arr ingests = (Json.Arr) Json.parse(resource("studio-ingests.json"));
        assertEquals(2, ingests.items().size());
        for (Json.Node n : ingests.items()) {
            Protocol.PIngest read = (Protocol.PIngest) ModelReader.readElement(n);
            assertEquals(new Protocol.PIngestFiles("Delta", "Parquet"), read.readMode());
            assertEquals("batch_milestoned", read.writeMode());
            assertEquals(new Protocol.PIngestOwner("111111", "222222"), read.owner());
            String written = SourceInformation.stripAll(ProtocolEmitter.emitElement(read));
            assertEquals(n, Json.parse(written), written);
        }
        // the minimal one Studio's own test writes by hand says no read mode: refused by name, never given one
        Json.Arr minimal = (Json.Arr) Json.parse(resource("studio-ingest-minimal.json"));
        IllegalArgumentException noReadMode = assertThrows(IllegalArgumentException.class,
                () -> ModelReader.readElement(minimal.items().get(0)));
        assertTrue(noReadMode.getMessage().contains("readMode"), noReadMode.getMessage());
    }

    @Test
    void theReaderRefusesWhatLiteDoesNotCarryByName() {
        String base = "{\"_type\":\"ingestDefinition\",\"datasets\":[],\"name\":\"I\",\"package\":\"p\","
                + "\"readMode\":{\"_type\":\"Snapshot\",\"format\":{\"_type\":\"CSV\"}}";
        IllegalArgumentException suites = assertThrows(IllegalArgumentException.class,
                () -> ModelReader.readElement(base + ",\"testSuites\":[]}"));
        assertTrue(suites.getMessage().contains("test suites"), suites.getMessage());
        IllegalArgumentException mode = assertThrows(IllegalArgumentException.class,
                () -> ModelReader.readElement(base + ",\"writeMode\":{\"_type\":\"upsert\"}}"));
        assertTrue(mode.getMessage().contains("'upsert'"), mode.getMessage());
        IllegalArgumentException field = assertThrows(IllegalArgumentException.class,
                () -> ModelReader.readElement(base + ",\"somethingNew\":1}"));
        assertTrue(field.getMessage().contains("somethingNew"), field.getMessage());
    }

    @Test
    void theGrammarRefusesByName() {
        refused("Ingest a::I Weekly<CSV> [ T( c: Int ); ]", "unknown read mode 'Weekly'");
        refused("Ingest a::I Snapshot<Xml> [ T( c: Int ); ]", "unknown file format 'Xml'");
        refused("Ingest a::I Pull<Every('15 minutes')> [ T( c: Int ); ]", "an ISO 8601 duration");
        refused("Ingest a::I Pull<Daily('6am', 'America/New_York')> [ T( c: Int ); ]", "HH:MM");
        refused("Ingest a::I Pull<Daily('06:00', 'Eastern')> [ T( c: Int ); ]", "a named zone");
        refused("Ingest a::I Stream [ T( c: Int ) writeMode=Upsert; ]", "unknown write mode 'Upsert'");
        refused("Ingest a::I Stream [ T( c: Int ); T( d: Int ); ]", "declared twice");
        refused("Ingest a::I Stream [ ]", "declares no dataset");
        refused("Ingest a::I Stream owner=AppDir(production='12a') [ T( c: Int ); ]", "digits");
        refused("Ingest a::I Stream [ T( c: Int ) retention=LastRows(0); ]", "at least one row");
        refused("Ingest a::I Stream [ T( c: Int ) pk=[ c ] pk=[ c ]; ]", "pk");
    }

    private static void refused(String ingest, String message) {
        ParseException e = assertThrows(ParseException.class,
                () -> PmcdParser.parse("###Lakehouse\n" + ingest + "\n", Dialect.LEGEND_LITE));
        assertTrue(String.valueOf(e.getMessage()).contains(message), ingest + " -> " + e.getMessage());
    }

    /** A store including the ingests reads their datasets as tables: written columns as typed, a read's as the read
     *  table's, the dataset's key as the key, a dataset group as the schema. */
    @Test
    void aStoreIncludingAnIngestReadsItsDatasetsAsTables() {
        ModelContext ctx = Compiler.compileModel(DESK);
        DatabaseDefinition.TableDefinition trade = ctx.findTableDefinition("desk::DB", "MARKET_TRADE").orElseThrow();
        assertEquals(List.of("PRINT_ID", "SYMBOL", "PRICE", "QTY"), trade.columns().stream().map(c -> c.name()).toList());
        assertTrue(trade.columns().get(0).primaryKey());
        assertTrue(trade.columns().get(1).notNull(), "the read table's NOT NULL kept");
        assertEquals(new RelationalDataType.Varchar(16), trade.columns().get(1).dataType());
        DatabaseDefinition.TableDefinition world = ctx.findTableDefinition("desk::DB", "WORLD").orElseThrow();
        assertTrue(world.columns().get(0).notNull(), "[1] is NOT NULL");
        assertTrue(!world.columns().get(1).notNull(), "[0..1] is nullable");
        DatabaseDefinition.TableDefinition position = ctx.findTableDefinition("desk::DB", "POSITION").orElseThrow();
        assertEquals(List.of(new RelationalDataType.Varchar(200), new RelationalDataType.Double_(),
                        new RelationalDataType.Date_(), new RelationalDataType.Timestamp(), new RelationalDataType.Bit(),
                        new RelationalDataType.Decimal(18, 4)),
                position.columns().stream().map(c -> c.dataType()).toList());
        assertTrue(ctx.findTableDefinition("desk::DB", "RISK.RISK_LIMIT").isPresent(), "a dataset group's schema");
        // the model's layout: no schema recorded for the default one, every table in the flat list
        DatabaseDefinition feed = ctx.findDatabase("desk::Feed").orElseThrow();
        assertEquals(List.of(), feed.schemas());
        assertEquals(List.of("MARKET_TRADE", "MARKET_QUOTE", "WORLD"),
                feed.defaultSchemaTables().stream().map(t -> t.name()).toList());
        DatabaseDefinition limits = ctx.findDatabase("desk::Limits").orElseThrow();
        assertEquals(List.of("RISK"), limits.schemas().stream().map(s -> s.name()).toList());
        assertEquals(List.of(), limits.defaultSchemaTables());

        String sql = Compiler.query(ctx, "|#>{desk::DB.MARKET_TRADE}#->filter(r|$r.SYMBOL == 'AAPL')->select(~[PRICE])")
                .plan("desk::RT").sql();
        assertTrue(sql.contains("MARKET_TRADE") && sql.contains("SYMBOL"), sql);
    }

    @Test
    void theDesignsChecksAreRefusedByName() {
        String topics = """
                ###Relational
                Database t::Topics ( Table T (ID VARCHAR(8) PRIMARY KEY, V DOUBLE) )
                ###Lakehouse
                """;
        modelRefused(topics + "Ingest t::I Stream [ D( |#>{t::Topics.T}# ) writeMode=LatestByKey; ]",
                "names no key");
        modelRefused(topics + "Ingest t::I Stream [ D( |#>{t::Topics.T}# ) pk=[ ID ] retention=Last('PT1H'); ]",
                "only appended data has");
        modelRefused(topics + "Ingest t::I Pull<Every('PT1H')> [ D( ID: Varchar(8) ) pk=[ ID ]; ]",
                "a query of the source's store");
        modelRefused(topics + "Ingest t::I Stream [ D( |#>{t::Topics.T}# ) pk=[ KEY ]; ]",
                "'KEY' is not one of its columns");
        modelRefused(topics + "Ingest t::I Stream [ D( |#>{t::Topics.NOPE}# ); ]", "does not declare");
        modelRefused(topics + "Ingest t::I Stream [ D( |#>{t::Topics.T}#->filter(r|$r.V > 0) ); ]",
                "a materialized view");
        modelRefused(topics + "Ingest t::I Delta<Parquet> writeMode=BatchMilestoned [ D( ID: Varchar(8) ); ]",
                "batch milestoned");
        modelRefused(topics + "Ingest t::I Stream [ D( ID: Integer ); ]", "names no table type");
        modelRefused(topics + "Ingest t::I Stream [ D( ID: Varchar(8)[*] ); ]", "more than one value");
    }

    private static void modelRefused(String text, String message) {
        ModelException e = assertThrows(ModelException.class, () -> Compiler.compileModel(text));
        assertTrue(String.valueOf(e.getMessage()).contains(message), text + "\n -> " + e.getMessage());
    }
}
