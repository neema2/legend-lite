// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testing.LaneFloor;

import com.legend.testing.Lean;
import com.legend.server.QueryService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code graphFetchChecked} defect reporting witnesses (type audit D102).
 *
 * <p>The defect CASE must treat a SQL-NULL predicate as the engine's
 * generated checker treats a mid-evaluation throw: a required property
 * absent on the wire yields the catch-branch defect {@code Unable to
 * evaluate constraint [id]: data not available - check your mappings}
 * (referee: legendJavaPlatformBinding shared/constraints.pure
 * generateConstraintMethod). Before the fix, {@code NOT NULL} was
 * not-true and an object violating EVERY constraint reported
 * {@code "defects":[]}.
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class GraphFetchCheckedIntegrationTest {

    private Connection conn;
    private final QueryService qs = new QueryService();

    @BeforeEach
    void setUp() throws SQLException {
        conn = LaneFloor.backend().open();
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE T_EDGE (ID INTEGER PRIMARY KEY,"
                    + " S_STRING VARCHAR(100), S_INT INTEGER)");
            s.execute("INSERT INTO T_EDGE VALUES (1, 'plain', 5),"
                    + " (2, '', -3), (5, NULL, NULL)");
            // REDUCERS over a navigation inside a constraint: firms and their employees
            s.execute("CREATE TABLE T_FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100))");
            s.execute("CREATE TABLE T_PERSON (ID INTEGER PRIMARY KEY, FIRST_NAME VARCHAR(100),"
                    + " LAST_NAME VARCHAR(100), FIRM_ID INTEGER)");
            s.execute("INSERT INTO T_FIRM VALUES (1, 'Dupes'), (2, 'Clean'), (3, 'Empty')");
            s.execute("INSERT INTO T_PERSON VALUES (1, 'Ann', 'Smith', 1), (2, 'Ann', 'Smith', 1),"
                    + " (3, 'Bob', 'Jones', 1), (4, 'Ann', 'Smith', 2), (5, 'Ann', 'Stone', 2)");
            // NESTED constraints: persons -> firm (to-one) -> addresses (to-many)
            s.execute("CREATE TABLE T_NFIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100))");
            s.execute("CREATE TABLE T_NPERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100),"
                    + " FIRM_ID INTEGER)");
            s.execute("CREATE TABLE T_NADDRESS (ID INTEGER PRIMARY KEY, FIRM_ID INTEGER,"
                    + " STREET VARCHAR(100))");
            s.execute("INSERT INTO T_NFIRM VALUES (1, 'Good Firm'), (2, '')");
            s.execute("INSERT INTO T_NPERSON VALUES (1, 'Ann', 1), (2, 'Bob', 2), (3, 'Cid', NULL)");
            s.execute("INSERT INTO T_NADDRESS VALUES (1, 1, 'Main St'), (2, 1, ''), (3, 1, 'Side St')");
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    private static final String MODEL = """
            ###Pure
            Class test::Edge
            [
              posInt: $this.sInt > 0,
              nonEmptyStr: $this.sString != ''
            ]
            { id: Integer[1]; sString: String[1]; sInt: Integer[1]; }

            ###Relational
            Database store::DB
            (
                Table T_EDGE (ID INTEGER PRIMARY KEY, S_STRING VARCHAR(100), S_INT INTEGER)
            )
            ###Mapping
            Mapping test::M
            (
                test::Edge: Relational
                {
                    ~mainTable [store::DB] T_EDGE
                    id: [store::DB] T_EDGE.ID,
                    sString: [store::DB] T_EDGE.S_STRING,
                    sInt: [store::DB] T_EDGE.S_INT
                }
            )
            ###Connection
            RelationalDatabaseConnection store::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime test::RT { mappings: [ test::M ]; connections: [ store::DB: [ environment: store::Conn ] ]; }
            """;

    private String checkedJson() throws SQLException {
        Lean.Result r = Lean.run(MODEL, """
                test::Edge.all()
                    ->graphFetchChecked(#{test::Edge {id, sString, sInt}}#)
                    ->serialize(#{test::Edge {id, sString, sInt}}#)
                """, "test::RT", conn);
        assertEquals(com.legend.executionplan.ExecutionPlan.TdsResult.Shape.OBJECTS, r.shape());
        return r.json();
    }

    @Test
    void nullPredicateReportsUnableToEvaluateDefects() throws SQLException {
        String json = checkedJson();
        // the all-NULL row (id=5): the comparison constraint cannot
        // evaluate — the engine's catch-branch defect, NOT a clean
        // envelope (pure lane: [] > 0 raises; generated-Java lane:
        // unboxing NPE)
        assertTrue(json.contains("Unable to evaluate constraint"
                        + " [posInt]: data not available - check your"
                        + " mappings"),
                "NULL predicate must defect (posInt): " + json);
        // the EQUALITY constraint is pure-faithful null-safe: [] != ''
        // is TRUE, so the absent string SATISFIES nonEmptyStr — its
        // predicate is true, not NULL, and no defect is correct (both
        // referees agree: pure equal is size-aware, the engine's
        // generated equal is null-safe)
        assertFalse(json.contains("Unable to evaluate constraint"
                        + " [nonEmptyStr]"),
                "null-safe equality must not defect: " + json);
        assertFalse(json.contains("\"defects\":[],\"value\":{\"id\":5"),
                "the all-NULL object must not report clean: " + json);
    }

    @Test
    void falseAndTrueArmsKeepTheirSpellings() throws SQLException {
        String json = checkedJson();
        // clean row (id=1) keeps the empty defects array
        assertTrue(json.contains("\"defects\":[],\"value\":{\"id\":1"),
                "clean row must report zero defects: " + json);
        // the plainly-violating row (id=2) keeps the engine default
        // violation message and full defect shape
        assertTrue(json.contains(
                        "Constraint :[posInt] violated in the Class Edge"),
                "false predicate must keep the violation defect: " + json);
        assertTrue(json.contains(
                "Constraint :[nonEmptyStr] violated in the Class Edge"),
                "false predicate must keep the violation defect: " + json);
        assertTrue(json.contains("\"ruleDefinerPath\":\"test::Edge\"")
                        && json.contains("\"ruleType\":\"ClassConstraint\"")
                        && json.contains("\"enforcementLevel\":\"Error\""),
                "defect shape must stay engine-shaped: " + json);
        // exactly the all-NULL row's one unable-to-evaluate defect
        assertEquals(1, json.split("Unable to evaluate", -1).length - 1,
                "only the NULL row's comparison constraint carries"
                        + " unable-to-evaluate: " + json);
    }

    private static final String FIRM_MODEL = """
            ###Pure
            Class test::Person { firstName: String[1]; lastName: String[1]; }
            Class test::Firm
            [
              distinctFirstNames: $this.employees.firstName->isDistinct(),
              distinctByName: $this.employees->isDistinct(#{test::Person {firstName, lastName}}#),
              enoughStaff: $this.employees->map(e | $e.firstName)->size() != 1
            ]
            { legalName: String[1]; employees: test::Person[*]; }
            ###Relational
            Database store::FDB
            (
                Table T_FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100))
                Table T_PERSON (ID INTEGER PRIMARY KEY, FIRST_NAME VARCHAR(100), LAST_NAME VARCHAR(100), FIRM_ID INTEGER)
                Join FirmPerson (T_FIRM.ID = T_PERSON.FIRM_ID)
            )
            ###Mapping
            Mapping test::FM
            (
                test::Person: Relational
                {
                    ~mainTable [store::FDB] T_PERSON
                    firstName: [store::FDB] T_PERSON.FIRST_NAME,
                    lastName: [store::FDB] T_PERSON.LAST_NAME
                }
                test::Firm: Relational
                {
                    ~mainTable [store::FDB] T_FIRM
                    legalName: [store::FDB] T_FIRM.LEGAL_NAME,
                    employees: [store::FDB] @FirmPerson
                }
            )
            ###Connection
            RelationalDatabaseConnection store::FConn { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime test::FRT { mappings: [ test::FM ]; connections: [ store::FDB: [ environment: store::FConn ] ]; }
            """;

    /** A REDUCER over a to-many navigation inside a checked constraint —
     * by leaf path, by tree (pure's isDistinct(collection, tree)), and a
     * mapped reducer — evaluates as one correlated scalar aggregate per
     * object (NavReducer): the constraint's own value, on the database. */
    @Test
    void reducersOverNavigationsInConstraints() throws SQLException {
        Lean.Result r = Lean.run(FIRM_MODEL, """
                test::Firm.all()
                    ->graphFetchChecked(#{test::Firm {legalName}}#)
                    ->serialize(#{test::Firm {legalName}}#)
                """, "test::FRT", conn);
        String json = r.json();
        // Dupes: two 'Ann Smith' → both distinct constraints defect; three
        // employees → enoughStaff holds
        assertTrue(json.contains("\"id\":\"distinctFirstNames\"") && json.contains(
                "\"id\":\"distinctByName\""), "Dupes defects on both distinct forms: " + json);
        int dupesAt = json.indexOf("\"legalName\":\"Dupes\"");
        int cleanAt = json.indexOf("\"legalName\":\"Clean\"");
        int emptyAt = json.indexOf("\"legalName\":\"Empty\"");
        assertTrue(dupesAt > 0 && cleanAt > 0 && emptyAt > 0, "three firms: " + json);
        // Clean: 'Ann Smith' and 'Ann Stone' — first names repeat (defect),
        // full names distinct (no defect); two employees → enoughStaff holds
        String clean = json.substring(Math.max(0, cleanAt - 400), cleanAt);
        assertTrue(clean.contains("distinctFirstNames") && !clean.contains("distinctByName")
                        && !clean.contains("enoughStaff"),
                "Clean: first names repeat, full names distinct: " + clean);
        // Empty: no employees — []->isDistinct() is true, size 0 != 1 holds
        String empty = json.substring(Math.max(0, emptyAt - 200), emptyAt);
        assertTrue(empty.contains("\"defects\":[]"), "Empty firm: no defects: " + empty);
    }
    private static final String NESTED_MODEL = """
            ###Pure
            Class test::Address
            [
              hasStreet: $this.street != ''
            ]
            { street: String[1]; }
            Class test::Firm
            [
              named: $this.legalName != ''
            ]
            { legalName: String[1]; addresses: test::Address[*]; }
            Class test::Person
            [
              hasName: $this.name != ''
            ]
            { name: String[1]; firm: test::Firm[0..1]; }
            ###Relational
            Database store::NDB
            (
                Table T_NFIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100))
                Table T_NPERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), FIRM_ID INTEGER)
                Table T_NADDRESS (ID INTEGER PRIMARY KEY, FIRM_ID INTEGER, STREET VARCHAR(100))
                Join PersonFirm (T_NPERSON.FIRM_ID = T_NFIRM.ID)
                Join FirmAddress (T_NFIRM.ID = T_NADDRESS.FIRM_ID)
            )
            ###Mapping
            Mapping test::NM
            (
                test::Address: Relational
                {
                    ~mainTable [store::NDB] T_NADDRESS
                    street: [store::NDB] T_NADDRESS.STREET
                }
                test::Firm: Relational
                {
                    ~mainTable [store::NDB] T_NFIRM
                    legalName: [store::NDB] T_NFIRM.LEGAL_NAME,
                    addresses: [store::NDB] @FirmAddress
                }
                test::Person: Relational
                {
                    ~mainTable [store::NDB] T_NPERSON
                    name: [store::NDB] T_NPERSON.NAME,
                    firm: [store::NDB] @PersonFirm
                }
            )
            ###Connection
            RelationalDatabaseConnection store::NConn { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime test::NRT { mappings: [ test::NM ]; connections: [ store::NDB: [ environment: store::NConn ] ]; }
            """;

    /** NESTED-OBJECT constraints (corpus-zero cluster A, 2026-09-12; engine
     * createConstraintCheckingForTree_recurse): every class-typed node of
     * the fetched tree runs its own class's constraints, and a nested
     * defect is hoisted to the root with its path — {propertyName, index}
     * per hop, the element's 0-based index for a to-many, null for a
     * to-one. A person with no firm reports nothing for it. */
    @Test
    void nestedObjectConstraintsHoistWithPath() throws SQLException {
        Lean.Result r = Lean.run(NESTED_MODEL, """
                test::Person.all()
                    ->graphFetchChecked(#{test::Person {name, firm {legalName, addresses {street}}}}#)
                    ->serialize(#{test::Person {name, firm {legalName, addresses {street}}}}#)
                """, "test::NRT", conn);
        String json = r.json();
        // Bob's firm has an empty legal name: the firm's own constraint,
        // hoisted to Bob's envelope under the to-one path
        assertTrue(json.contains("\"id\":\"named\"") && json.contains(
                "\"path\":[{\"propertyName\":\"firm\",\"index\":null}]"),
                "to-one nested defect with its path: " + json);
        // Ann's firm has three addresses; the second has an empty street:
        // the address constraint hoisted through firm (to-one) then
        // addresses (to-many, index 1)
        assertTrue(json.contains("\"id\":\"hasStreet\"") && json.contains(
                "\"path\":[{\"propertyName\":\"firm\",\"index\":null},"
                        + "{\"propertyName\":\"addresses\",\"index\":1}]"),
                "to-many nested defect with its indexed path: " + json);
        // the nested values still serialize as plain objects/arrays
        assertTrue(json.contains("\"firm\":{\"legalName\":\"Good Firm\",\"addresses\":[{\"street\":\"Main St\"}"),
                "nested value shape: " + json);
        // Cid has no firm: no defect, firm serializes null
        assertTrue(json.contains("\"defects\":[],\"value\":{\"name\":\"Cid\",\"firm\":null}"),
                "a null to-one child is skipped: " + json);
    }

    /** A to-many property with NO values serializes as Pure's empty list, {@code []} (legend-engine's graph fetch
     *  serialization of an empty to-many), never as {@code null}: the homework README §9's finding ('lite's JSON
     *  writes an absent to-many property as null'), probed here at track F (2026-10-29) -- firm 2 has no address. */
    @Test
    void anEmptyToManyPropertySerializesAsAnEmptyList() throws SQLException {
        Lean.Result r = Lean.run(NESTED_MODEL, """
                test::Firm.all()
                    ->graphFetch(#{test::Firm {legalName, addresses {street}}}#)
                    ->serialize(#{test::Firm {legalName, addresses {street}}}#)
                """, "test::NRT", conn);
        String json = r.json();
        assertTrue(json.contains("{\"legalName\":\"\",\"addresses\":[]}"),
                "an empty to-many property is Pure's []: " + json);
        assertTrue(json.contains("\"legalName\":\"Good Firm\",\"addresses\":[{\"street\":\"Main St\"}"),
                "a to-many property's elements in order: " + json);
    }
}
