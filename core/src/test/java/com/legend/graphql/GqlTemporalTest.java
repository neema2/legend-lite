// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.exec.ExecutionResult;
import com.legend.server.QueryService;
import com.legend.testbackend.TestBackends;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Temporal classes (register S119; docs/GRAPHQL_STUDY_2026_10_18.md §13.9) beyond the hardening fixture's
 * business-temporal one: a BITEMPORAL limit and a PROCESSING-temporal note per instrument. A root over a temporal
 * class and a field reaching one take its dates, the processing date first (Pure's {@code all(processingDate,
 * businessDate)}); no input object names a dated navigation; a temporal object answers the dates it was read as of.
 * Rows: AAPL's limit recorded as 5000 on 2024-01-01 and corrected to 8000 on 2024-02-01 (both holding from business
 * date 2024-01-01), MSFT's 3000; AAPL's notes "old" (recorded January only), "new" (from February), "kept".
 */
class GqlTemporalTest {

    static final String MODEL = """
            ###Pure
            Class demo::Instrument { ticker: String[1]; }
            Class <<temporal.bitemporal>> demo::Limit { ticker: String[1]; maxQty: Integer[1]; }
            Class <<temporal.processingtemporal>> demo::Note { ticker: String[1]; text: String[1]; }
            Association demo::Limits { instrument: demo::Instrument[0..1]; limit: demo::Limit[0..1]; }
            Association demo::Notes { noted: demo::Instrument[0..1]; notes: demo::Note[*]; }
            ###Relational
            Database demo::Db
            (
              Table INSTRUMENT (TICKER VARCHAR(10) PRIMARY KEY)
              Table LIMITS
              (
                milestoning
                (
                  business(BUS_FROM = FROM_Z, BUS_THRU = THRU_Z, INFINITY_DATE = %9999-12-31),
                  processing(PROCESSING_IN = IN_Z, PROCESSING_OUT = OUT_Z, INFINITY_DATE = %9999-12-31)
                )
                TICKER VARCHAR(10) PRIMARY KEY, FROM_Z DATE PRIMARY KEY, IN_Z DATE PRIMARY KEY, THRU_Z DATE, OUT_Z DATE,
                MAX_QTY INTEGER
              )
              Table NOTE
              (
                milestoning(processing(PROCESSING_IN = IN_Z, PROCESSING_OUT = OUT_Z, INFINITY_DATE = %9999-12-31))
                ID INTEGER PRIMARY KEY, IN_Z DATE PRIMARY KEY, TICKER VARCHAR(10), OUT_Z DATE, TXT VARCHAR(50)
              )
              Join InstrumentLimit (INSTRUMENT.TICKER = LIMITS.TICKER)
              Join InstrumentNote (INSTRUMENT.TICKER = NOTE.TICKER)
            )
            ###Mapping
            Mapping demo::Map
            (
              demo::Instrument: Relational { ~mainTable [demo::Db] INSTRUMENT ticker: [demo::Db] INSTRUMENT.TICKER }
              demo::Limit: Relational
              {
                ~mainTable [demo::Db] LIMITS
                ticker: [demo::Db] LIMITS.TICKER,
                maxQty: [demo::Db] LIMITS.MAX_QTY
              }
              demo::Note: Relational
              {
                ~mainTable [demo::Db] NOTE
                ticker: [demo::Db] NOTE.TICKER,
                text: [demo::Db] NOTE.TXT
              }
              demo::Limits: Relational { AssociationMapping ( instrument: [demo::Db] @InstrumentLimit,
                                                              limit: [demo::Db] @InstrumentLimit ) }
              demo::Notes: Relational { AssociationMapping ( noted: [demo::Db] @InstrumentNote,
                                                             notes: [demo::Db] @InstrumentNote ) }
            )
            ###Connection
            RelationalDatabaseConnection demo::Conn { store: demo::Db; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime demo::Runtime { mappings: [ demo::Map ]; connections: [ demo::Db: [ db: demo::Conn ] ]; }
            """;

    static Connection conn;
    static final QueryService qs = new QueryService();

    @BeforeAll
    static void open() throws SQLException {
        conn = TestBackends.platform().open();
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE INSTRUMENT (TICKER VARCHAR(10) PRIMARY KEY)");
            s.execute("CREATE TABLE LIMITS (TICKER VARCHAR(10), FROM_Z DATE, IN_Z DATE, THRU_Z DATE, OUT_Z DATE,"
                    + " MAX_QTY INTEGER)");
            s.execute("CREATE TABLE NOTE (ID INTEGER, IN_Z DATE, TICKER VARCHAR(10), OUT_Z DATE, TXT VARCHAR(50))");
            s.execute("INSERT INTO INSTRUMENT VALUES ('AAPL'), ('MSFT'), ('IBM')");
            s.execute("INSERT INTO LIMITS VALUES ('AAPL', DATE '2024-01-01', DATE '2024-01-01', DATE '9999-12-31',"
                    + " DATE '2024-02-01', 5000), ('AAPL', DATE '2024-01-01', DATE '2024-02-01', DATE '9999-12-31',"
                    + " DATE '9999-12-31', 8000), ('MSFT', DATE '2024-01-01', DATE '2024-01-01', DATE '9999-12-31',"
                    + " DATE '9999-12-31', 3000)");
            s.execute("INSERT INTO NOTE VALUES (1, DATE '2024-01-01', 'AAPL', DATE '2024-02-01', 'old'),"
                    + " (2, DATE '2024-02-01', 'AAPL', DATE '9999-12-31', 'new'),"
                    + " (3, DATE '2024-01-01', 'AAPL', DATE '9999-12-31', 'kept')");
        }
    }

    @AfterAll
    static void close() throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    static String gql(String document) throws SQLException {
        ExecutionResult r = qs.execute(MODEL, "#GQL{ " + document + " }#", "demo::Runtime", conn);
        assertInstanceOf(ExecutionResult.Graph.class, r, r.toString());
        return r.asGraph().json().replace(" ", "");
    }

    @Test
    void theSchemaAsksEachTemporalReadItsDates() {
        ModelContext ctx = Compiler.compileModel(MODEL);
        String sdl = com.legend.compiler.graphql.SdlPrinter.print(com.legend.compiler.graphql.GqlIslands.schemaOf(ctx));
        assertTrue(sdl.contains("Limit(processingDate: DateTime!, businessDate: Date!, where: LimitWhere"), sdl);
        assertTrue(sdl.contains("LimitAggregate(processingDate: DateTime!, businessDate: Date!, where: LimitWhere)"),
                sdl);
        assertTrue(sdl.contains("Note(processingDate: DateTime!, where: NoteWhere"), sdl);
        assertTrue(sdl.contains("limit(processingDate: DateTime!, businessDate: Date!): Limit"), sdl);
        // a dated list takes its date and no clause yet; no summary of it is offered
        assertTrue(sdl.contains("notes(processingDate: DateTime!): [Note!]!"), sdl);
        assertFalse(sdl.contains("notesAggregate") || sdl.contains("notesGroups"), sdl);
        // the dates a temporal object was read as of
        assertTrue(sdl.contains("businessDate: Date!") && sdl.contains("processingDate: DateTime!"), sdl);
        // no input object names a dated navigation
        int where = sdl.indexOf("input InstrumentWhere {");
        String instrumentWhere = sdl.substring(where, sdl.indexOf("}", where));
        assertFalse(instrumentWhere.contains("limit") || instrumentWhere.contains("notes"), instrumentWhere);
    }

    @Test
    void aBitemporalRootReadsTheVersionRecordedOnItsProcessingDate() throws SQLException {
        String january = gql("{ Limit(processingDate: \"2024-01-15T00:00:00\", businessDate: \"2024-06-01\","
                + " orderBy: [{ ticker: ASC }]) { ticker maxQty businessDate } }");
        assertTrue(january.contains("{\"ticker\":\"AAPL\",\"maxQty\":5000,\"businessDate\":\"2024-06-01\"},"
                + "{\"ticker\":\"MSFT\",\"maxQty\":3000"), january);
        String march = gql("{ Limit(processingDate: \"2024-03-01T00:00:00\", businessDate: \"2024-06-01\","
                + " orderBy: [{ ticker: ASC }]) { ticker maxQty } }");
        assertTrue(march.contains("{\"ticker\":\"AAPL\",\"maxQty\":8000}"), march);
        String before = gql("{ Limit(processingDate: \"2024-03-01T00:00:00\", businessDate: \"2023-06-01\") {"
                + " ticker } }");
        assertTrue(before.contains("[]"), "no limit held on business date 2023-06-01: " + before);
    }

    @Test
    void aFieldReachingATemporalClassTakesItsDates() throws SQLException {
        String json = gql("{ Instrument(orderBy: [{ ticker: ASC }]) { ticker limit(processingDate:"
                + " \"2024-03-01T00:00:00\", businessDate: \"2024-06-01\") { maxQty } notes(processingDate:"
                + " \"2024-01-15T00:00:00\") { text } } }");
        assertTrue(json.contains("{\"ticker\":\"AAPL\",\"limit\":{\"maxQty\":8000},\"notes\":["), json);
        assertTrue(json.contains("\"old\"") && json.contains("\"kept\"") && !json.contains("\"new\""), json);
        assertTrue(json.contains("{\"ticker\":\"IBM\",\"limit\":null,\"notes\":[]}"), json);
    }

    @Test
    void aTemporalReadWithoutItsDatesIsRefusedByName() {
        Exception e = org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
                () -> gql("{ Instrument { ticker limit(businessDate: \"2024-06-01\") { maxQty } } }"));
        assertTrue(String.valueOf(e.getMessage()).contains("processingDate"), e.getMessage());
    }
}
