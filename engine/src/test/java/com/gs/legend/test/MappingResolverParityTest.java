package com.gs.legend.test;

import com.gs.legend.plan.PlanGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Byte-identical SQL snapshot fixture pinning {@code MappingResolver} +
 * lowering output across a curated set of representative queries. Acts as
 * a regression net for the multi-phase MR refactor (see
 * {@code .windsurf/plans/mapping-resolver-as-rewrite.md}).
 *
 * <p><strong>How it works</strong>: each {@link Case} pairs a model + query
 * with a snapshot file at {@code engine/src/test/resources/parity/&lt;name&gt;.sql}.
 * The test runs {@link PlanGenerator#generate} and compares the resulting
 * SQL string against the snapshot byte-for-byte. Mismatch = test failure,
 * surfacing the diff via JUnit's expected/actual.
 *
 * <p><strong>Updating snapshots</strong>: pass
 * {@code -DupdateParitySnapshots=true} to the JVM. The test will (re)write
 * the snapshot file and assert against itself (always pass), so a refactor
 * with intentional SQL changes can be reviewed by inspecting the diff in
 * version control before commit. Without the flag, missing snapshots fail
 * loudly so we never accidentally commit a case without a baseline.
 *
 * <p><strong>Coverage targets</strong>: each case exercises a distinct
 * MR concern. As MR's responsibilities collapse into the rewriter (Phases
 * 1-5), each case keeps its baseline so regressions surface immediately.
 */
@DisplayName("MappingResolver parity fixture")
class MappingResolverParityTest {

    /** Set this system property to (re)write all snapshots: -DupdateParitySnapshots=true */
    private static final boolean UPDATE_SNAPSHOTS =
            Boolean.getBoolean("updateParitySnapshots");

    /** Snapshot directory relative to engine/. */
    private static final Path SNAPSHOT_DIR =
            Paths.get("src", "test", "resources", "parity");

    /** A parity case: a Pure model + query producing a SQL snapshot. */
    private record Case(String name, String description, String model, String query) {}

    /**
     * Wraps a model body with a Runtime block referencing the supplied DB.
     * Mirrors the helper in {@code RelationalMappingCompositionTest}.
     */
    private static String withRuntime(String body, String dbName, String mappingName) {
        return body + """

                RelationalDatabaseConnection store::Conn {
                    type: DuckDB; specification: InMemory { }; auth: NoAuth { };
                }
                Runtime test::RT {
                    mappings: [ %s ];
                    connections: [ %s: [ environment: store::Conn ] ];
                }
                """.formatted(mappingName, dbName);
    }

    // ==================== Cases ====================

    private static Stream<Case> cases() {
        return Stream.of(
                simpleRelational(),
                filterLocal(),
                projectAndSort(),
                associationOneHop(),
                associationTwoHop(),
                m2mChain(),
                extendScalar(),
                limitOnly(),
                multiOpChain()
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void parityCheck(Case c) throws IOException {
        String actualSql = PlanGenerator.generate(c.model(), c.query(), "test::RT").sql();
        Path snapshot = SNAPSHOT_DIR.resolve(c.name() + ".sql");

        if (UPDATE_SNAPSHOTS || !Files.exists(snapshot)) {
            Files.createDirectories(SNAPSHOT_DIR);
            Files.writeString(snapshot, actualSql, StandardCharsets.UTF_8);
            if (!UPDATE_SNAPSHOTS) {
                fail("Wrote initial snapshot for '" + c.name() + "' at "
                        + snapshot + " — re-run to verify it round-trips. "
                        + "(This failure is expected on first run.)");
            }
            // UPDATE mode: snapshot is now refreshed — assert trivially.
            assertEquals(actualSql, actualSql);
            return;
        }

        String expectedSql = Files.readString(snapshot, StandardCharsets.UTF_8);
        assertEquals(expectedSql, actualSql,
                "Parity drift in '" + c.name() + "'. "
                        + "If intentional, re-run with -DupdateParitySnapshots=true "
                        + "and review the diff in version control.\n"
                        + "Description: " + c.description());
    }

    // ==================== Case definitions ====================

    /** Simple relational mapping: project a single mapped column. */
    private static Case simpleRelational() {
        String model = withRuntime("""
                Class model::Person { name: String[1]; age: Integer[1]; }
                Database store::DB ( Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), AGE INTEGER) )
                Mapping model::M (
                    Person: Relational {
                        ~mainTable [store::DB] T_PERSON
                        name: [store::DB] T_PERSON.NAME,
                        age: [store::DB] T_PERSON.AGE
                    }
                )
                """, "store::DB", "model::M");
        return new Case("simple_relational",
                "Bare relational mapping: project a mapped column.",
                model,
                "Person.all()->project(~[name:p|$p.name])");
    }

    /** Filter on a local column then project. Should produce no joins. */
    private static Case filterLocal() {
        String model = withRuntime("""
                Class model::Person { name: String[1]; age: Integer[1]; }
                Database store::DB ( Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), AGE INTEGER) )
                Mapping model::M (
                    Person: Relational {
                        ~mainTable [store::DB] T_PERSON
                        name: [store::DB] T_PERSON.NAME,
                        age: [store::DB] T_PERSON.AGE
                    }
                )
                """, "store::DB", "model::M");
        return new Case("filter_local",
                "Filter + project on local mapped columns. No joins expected.",
                model,
                "Person.all()->filter({p|$p.age > 18})->project(~[name:p|$p.name, age:p|$p.age])");
    }

    /** Project + sort on local columns. */
    private static Case projectAndSort() {
        String model = withRuntime("""
                Class model::Person { name: String[1]; age: Integer[1]; }
                Database store::DB ( Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), AGE INTEGER) )
                Mapping model::M (
                    Person: Relational {
                        ~mainTable [store::DB] T_PERSON
                        name: [store::DB] T_PERSON.NAME,
                        age: [store::DB] T_PERSON.AGE
                    }
                )
                """, "store::DB", "model::M");
        return new Case("project_and_sort",
                "Project then sort by a mapped column.",
                model,
                "Person.all()->project(~[name:p|$p.name])->sort(~name->ascending())");
    }

    /** 1-hop association: Person.dept.name. Expect a single LEFT JOIN. */
    private static Case associationOneHop() {
        String model = withRuntime("""
                Class model::Person { name: String[1]; deptName: String[1]; }
                Database store::DB (
                    Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INTEGER)
                    Table T_DEPT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50))
                    Join Person_Dept(T_PERSON.DEPT_ID = T_DEPT.ID)
                )
                Mapping model::M (
                    Person: Relational {
                        ~mainTable [store::DB] T_PERSON
                        name: [store::DB] T_PERSON.NAME,
                        deptName: [store::DB] @Person_Dept | T_DEPT.NAME
                    }
                )
                """, "store::DB", "model::M");
        return new Case("association_one_hop",
                "1-hop association: project a join-chain property — emits one LEFT JOIN.",
                model,
                "Person.all()->project(~[name:p|$p.name, deptName:p|$p.deptName])");
    }

    /** 2-hop association: Person.dept.org.name. Expect 2 chained LEFT JOINs. */
    private static Case associationTwoHop() {
        String model = withRuntime("""
                Class model::Person { name: String[1]; orgName: String[1]; }
                Database store::DB (
                    Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INTEGER)
                    Table T_DEPT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50), ORG_ID INTEGER)
                    Table T_ORG (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))
                    Join Person_Dept(T_PERSON.DEPT_ID = T_DEPT.ID)
                    Join Dept_Org(T_DEPT.ORG_ID = T_ORG.ID)
                )
                Mapping model::M (
                    Person: Relational {
                        ~mainTable [store::DB] T_PERSON
                        name: [store::DB] T_PERSON.NAME,
                        orgName: [store::DB] @Person_Dept > @Dept_Org | T_ORG.NAME
                    }
                )
                """, "store::DB", "model::M");
        return new Case("association_two_hop",
                "2-hop association chain: emits 2 LEFT JOINs.",
                model,
                "Person.all()->project(~[name:p|$p.name, orgName:p|$p.orgName])");
    }

    /** M2M: Pure mapping that wraps a relational source. */
    private static Case m2mChain() {
        String model = withRuntime("""
                Class model::Raw { first: String[1]; last: String[1]; }
                Class model::Person { fullName: String[1]; }
                Database store::DB (
                    Table T_RAW (ID INTEGER PRIMARY KEY, FIRST VARCHAR(50), LAST VARCHAR(50))
                )
                Mapping model::M (
                    Raw: Relational {
                        ~mainTable [store::DB] T_RAW
                        first: [store::DB] T_RAW.FIRST,
                        last: [store::DB] T_RAW.LAST
                    }
                    Person: Pure {
                        ~src Raw
                        fullName: $src.first + ' ' + $src.last
                    }
                )
                """, "store::DB", "model::M");
        return new Case("m2m_chain",
                "M2M (Pure) mapping wrapping a relational source.",
                model,
                "Person.all()->project(~[fullName:p|$p.fullName])");
    }

    /** Extend with a computed scalar column. */
    private static Case extendScalar() {
        String model = withRuntime("""
                Class model::Person { name: String[1]; age: Integer[1]; }
                Database store::DB ( Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), AGE INTEGER) )
                Mapping model::M (
                    Person: Relational {
                        ~mainTable [store::DB] T_PERSON
                        name: [store::DB] T_PERSON.NAME,
                        age: [store::DB] T_PERSON.AGE
                    }
                )
                """, "store::DB", "model::M");
        return new Case("extend_scalar",
                "Project then extend with a computed scalar column on the TDS.",
                model,
                "Person.all()->project(~[name:p|$p.name])->extend(~upper:x|$x.name->toUpper())");
    }

    /** Limit clause. */
    private static Case limitOnly() {
        String model = withRuntime("""
                Class model::Person { name: String[1]; }
                Database store::DB ( Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100)) )
                Mapping model::M (
                    Person: Relational {
                        ~mainTable [store::DB] T_PERSON
                        name: [store::DB] T_PERSON.NAME
                    }
                )
                """, "store::DB", "model::M");
        return new Case("limit_only",
                "Project then limit. Tests slice lowering.",
                model,
                "Person.all()->project(~[name:p|$p.name])->limit(10)");
    }

    /** Multi-operation chain: filter + sort + limit + project. */
    private static Case multiOpChain() {
        String model = withRuntime("""
                Class model::Person { name: String[1]; age: Integer[1]; }
                Database store::DB ( Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), AGE INTEGER) )
                Mapping model::M (
                    Person: Relational {
                        ~mainTable [store::DB] T_PERSON
                        name: [store::DB] T_PERSON.NAME,
                        age: [store::DB] T_PERSON.AGE
                    }
                )
                """, "store::DB", "model::M");
        return new Case("multi_op_chain",
                "Filter + project + sort + limit. Tests operator composition.",
                model,
                "Person.all()->filter({p|$p.age > 18})->project(~[name:p|$p.name, age:p|$p.age])->sort(~name->ascending())->limit(5)");
    }
}
