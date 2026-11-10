// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.testing.Lean;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A project's query on legend-lite's server answers its own Data rows (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C5):
 * the in-memory database the server keeps for the project's connection is made holding them, as a tab's and
 * Python's project database are -- so Query and Studio on the server show the rows a tab shows -- and an edited row is
 * a fresh database, never the earlier rows.
 */
class ProjectRowsTest {

    private static final String PARTY = """
            Class proj::Party { id: Integer[1]; name: String[1]; }
            ###Relational
            Database proj::PartyDB ( Schema PARTY ( Table PARTY (ID INTEGER PRIMARY KEY, NAME VARCHAR(200)) ) )
            ###Mapping
            Mapping proj::PartyMapping
            (
              *proj::Party: Relational
              {
                ~primaryKey ([proj::PartyDB] PARTY.PARTY.ID)
                ~mainTable [proj::PartyDB] PARTY.PARTY
                id: [proj::PartyDB] PARTY.PARTY.ID,
                name: [proj::PartyDB] PARTY.PARTY.NAME
              }
            )
            ###Connection
            RelationalDatabaseConnection proj::Conn { store: proj::PartyDB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime proj::RT { mappings: [ proj::PartyMapping ]; connections: [ proj::PartyDB: [ db: proj::Conn ] ]; }
            ###Data
            Data proj::PartyData
            {
              Relational
              #{
                PARTY.PARTY:
                  'ID,NAME\\n1,Meridian Capital\\n2,Halberd Securities\\n';
              }#
            }
            """;

    private static final String QUERY = "|proj::Party.all()->project(~[name: p|$p.name])->sort(~name->ascending())";

    private static List<Object> names(String model) throws Exception {
        return Lean.service(model, QUERY, "proj::RT").rows().stream().map(r -> r.get(0)).toList();
    }

    @Test
    void aProjectsQueryAnswersItsOwnRows() throws Exception {
        assertEquals(List.of("Halberd Securities", "Meridian Capital"), names(PARTY));
        // asked again: the same rows
        assertEquals(List.of("Halberd Securities", "Meridian Capital"), names(PARTY));
    }

    /** The project's connection, and the same connection on an in-memory database its opener names (legend-engine's
     *  test specification). */
    private static final String HELD = """
            RelationalDatabaseConnection proj::Conn { store: proj::PartyDB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            """;
    private static final String OPENER_NAMED = """
            RelationalDatabaseConnection proj::Conn { store: proj::PartyDB; type: H2; specification: LocalH2 { }; auth: DefaultH2; }
            """;

    /** The same project on an in-memory database its opener names (the named arm) answers its own rows too, and a
     *  refused model is refused there by name, never run on an empty database (the M1 tenets audit's finding 1,
     *  2026-11-09: only the held arm made the database until then). */
    @Test
    void aProjectOnAnOpenerNamedDatabaseAnswersItsOwnRows_andARefusedOneIsRefused() throws Exception {
        String named = PARTY.replace(HELD.strip(), OPENER_NAMED.strip());
        org.junit.jupiter.api.Assertions.assertNotEquals(PARTY, named);
        assertEquals(List.of("Halberd Securities", "Meridian Capital"), names(named));
        IllegalArgumentException refused = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> names(named.replace("PARTY.PARTY:\n", "PARTY.NOPE:\n")));
        assertEquals("the Data element proj::PartyData fills PARTY.NOPE, which no Database of the model declares",
                refused.getMessage());
    }

    /** A project whose connection declares test data making a table U (statements of its own, the declared test data's
     *  form), and the same project once that data is gone: two databases, never the first's table answering the second
     *  (the M1 correctness audit's C1, 2026-11-09: since L2 the connection no longer carried the test data, and the
     *  server's key named the project alone). */
    private static final String WITH_U = """
            RelationalDatabaseConnection proj::Conn { store: proj::PartyDB; type: H2; specification: LocalH2 { testDataSetupSqls: ['create table if not exists PARTY.U(ID INT)', 'delete from PARTY.U', 'insert into PARTY.U values (7)']; }; auth: DefaultH2; }
            """;

    @Test
    void twoEditionsWhoseTestDataDifferAreTwoDatabases() throws Exception {
        String withU = PARTY.replace(HELD.strip(), WITH_U.strip()).replace(
                "Database proj::PartyDB ( Schema PARTY ( Table PARTY (ID INTEGER PRIMARY KEY, NAME VARCHAR(200)) ) )",
                "Database proj::PartyDB ( Schema PARTY ( Table PARTY (ID INTEGER PRIMARY KEY, NAME VARCHAR(200))"
                        + " Table U (ID INTEGER PRIMARY KEY) ) )");
        org.junit.jupiter.api.Assertions.assertNotEquals(PARTY, withU);
        String withoutU = withU.replace(WITH_U.strip(), OPENER_NAMED.strip());
        org.junit.jupiter.api.Assertions.assertNotEquals(withU, withoutU);
        String readU = "|#>{proj::PartyDB.PARTY.U}#->select(~[ID])";
        // the two editions' project databases are named apart: the key is the whole target as a plan writes it, its
        // loads by identity
        var keyWith = com.legend.exec.PlanSessions.projectKey(com.legend.Compiler.query(
                com.legend.Compiler.compileModel(withU), readU).projectPlan("proj::RT").answerSql().target());
        var keyWithout = com.legend.exec.PlanSessions.projectKey(com.legend.Compiler.query(
                com.legend.Compiler.compileModel(withoutU), readU).projectPlan("proj::RT").answerSql().target());
        org.junit.jupiter.api.Assertions.assertNotEquals(keyWith, keyWithout);
        assertEquals(List.of(7L), com.legend.testing.Lean.service(withU, readU, "proj::RT").rows().stream()
                .map(r -> r.get(0)).toList());
        // the edition with no declared test data never made U: its own database refuses the read
        RuntimeException notMade = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                () -> com.legend.testing.Lean.service(withoutU, readU, "proj::RT"));
        assertTrue(String.valueOf(notMade.getMessage()).contains("\"U\" not found"), String.valueOf(notMade));
    }

    /** Two queries over one project share its one kept database, a query reading the system metamodel's tables too
     *  (the re-audit's A-4, 2026-11-09): the key names what the database holds -- its connection, its project, its ONCE
     *  loads -- never a query's own per-acquisition loads, which until then kept a database (with the whole fill) per
     *  query shape. */
    @Test
    void twoQueriesOverOneProjectShareItsDatabase_oneReadingTheMetamodel() {
        var ctx = com.legend.Compiler.compileModel(PARTY);
        var plain = com.legend.Compiler.query(ctx, QUERY).projectPlan("proj::RT").answerSql().target();
        var metamodel = com.legend.Compiler.query(ctx, "|meta::pure::metamodel::type::Class.all()->size()")
                .projectPlan("proj::RT").answerSql().target();
        org.junit.jupiter.api.Assertions.assertNotEquals(plain.loads(), metamodel.loads(),
                "the metamodel's query loads its tables on acquisition");
        assertEquals(com.legend.exec.PlanSessions.projectKey(plain), com.legend.exec.PlanSessions.projectKey(metamodel));
    }

    /** A source runs the loads its target names or refuses the target (the re-audit's A-3, 2026-11-09): a project's
     *  Data (its ONCE load) on a session the caller holds is refused by name, acquired or as it is, never skipped. */
    @Test
    void aProjectsDataOnACallersSessionIsRefused_neverSkipped() throws Exception {
        var target = com.legend.Compiler.query(com.legend.Compiler.compileModel(PARTY), QUERY).projectPlan("proj::RT")
                .answerSql().target();
        assertTrue(target.loads().stream().anyMatch(
                l -> l.cadence() == com.legend.executionplan.ExecutionPlan.Load.Cadence.ONCE), String.valueOf(target));
        try (java.sql.Connection c = com.legend.testbackend.TestBackends.platform().open()) {
            var session = com.legend.testbackend.TestSessions.over(c);
            var acquired = org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                    () -> com.legend.exec.PlanSessions.acquired(session, target));
            assertTrue(String.valueOf(acquired.getMessage()).contains("does not run"), acquired.getMessage());
            var given = org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                    () -> com.legend.exec.PlanSessions.of(session).open(target));
            assertTrue(String.valueOf(given.getMessage()).contains("does not run"), given.getMessage());
        }
    }

    /** A project's Data rows are the plan's one ONCE load (docs/DATA_IN_THE_PLAN_2026_11_09.md L3, S171): named by
     *  its target, written once in the plan's load section, and read back as written. */
    @Test
    void aProjectsDataIsItsTargetsOnceLoadWrittenOnce() {
        com.legend.executionplan.ExecutionPlan plan = com.legend.Compiler.query(com.legend.Compiler.compileModel(PARTY),
                QUERY).projectPlan("proj::RT");
        com.legend.executionplan.ExecutionPlan.Target target = plan.answerSql().target();
        List<com.legend.executionplan.ExecutionPlan.Load> once = target.loads().stream()
                .filter(l -> l.cadence() == com.legend.executionplan.ExecutionPlan.Load.Cadence.ONCE).toList();
        assertEquals(1, once.size(), String.valueOf(target.loads()));
        String json = com.legend.executionplan.PlanJson.write(plan);
        String fill = ((com.legend.executionplan.ExecutionPlan.LoadStep.Statement) once.get(0).steps().get(
                once.get(0).steps().size() - 1)).sql();
        String written = com.legend.json.Json.toCompact(fill);
        assertTrue(json.indexOf(written) >= 0, "the fill is written");
        assertEquals(json.indexOf(written), json.lastIndexOf(written), "the fill is written once");
        assertEquals(json, com.legend.executionplan.PlanJson.write(com.legend.executionplan.PlanJson.read(json)));
    }

    /** A project's Data above the inline threshold travels in the plan's bundle (L6c, 2026-11-10; census M1 DELETED):
     *  its ONCE load reads the rows by a CONTENT reference, no statement holds them, and the server's database answers
     *  every row. */
    @Test
    void aLargeFillTravelsInTheBundle() throws Exception {
        StringBuilder rows = new StringBuilder("ID,NAME\\n");
        for (int i = 1; i <= 2000; i++) {
            rows.append(i).append(",Party number ").append(i).append("\\n");
        }
        String large = PARTY.replace("'ID,NAME\\n1,Meridian Capital\\n2,Halberd Securities\\n'", "'" + rows + "'");
        com.legend.executionplan.ExecutionPlan plan = com.legend.Compiler.query(com.legend.Compiler.compileModel(large),
                QUERY).projectPlan("proj::RT");
        com.legend.executionplan.ExecutionPlan.Load once = plan.answerSql().target().loads().stream()
                .filter(l -> l.cadence() == com.legend.executionplan.ExecutionPlan.Load.Cadence.ONCE).findFirst()
                .orElseThrow();
        assertEquals(1, once.content().size(), "one blob: the table's rows");
        assertTrue(once.steps().stream().anyMatch(s -> s instanceof com.legend.executionplan.ExecutionPlan.LoadStep.File f
                && f.ref().namespace() == com.legend.executionplan.DataRef.Namespace.CONTENT), String.valueOf(once));
        com.legend.executionplan.PlanJson.Bundle bundle = com.legend.executionplan.PlanJson.writeBundle(plan);
        assertTrue(!bundle.json().contains("Party number 1999"), "no row in the plan's JSON");
        assertEquals(2000, names(large).size());
    }

    /** A Data element over a table no Database declares: every query on the model refused by name (the database the
     *  server made for it let go), where before C5 it ran on an empty database. */
    @Test
    void aDataElementOverAnUndeclaredTableIsRefusedByName() {
        String bad = PARTY.replace("PARTY.PARTY:\n", "PARTY.NOPE:\n");
        IllegalArgumentException refused = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> names(bad));
        assertEquals("the Data element proj::PartyData fills PARTY.NOPE, which no Database of the model declares",
                refused.getMessage());
    }

    @Test
    void anEditedRowIsAFreshDatabase() throws Exception {
        String edited = PARTY.replace("2,Halberd Securities", "3,Kestrel Partners");
        assertEquals(List.of("Halberd Securities", "Meridian Capital"), names(PARTY));
        assertEquals(List.of("Kestrel Partners", "Meridian Capital"), names(edited));
        assertEquals(List.of("Halberd Securities", "Meridian Capital"), names(PARTY));
    }
}
