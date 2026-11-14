// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedSpec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE CONSUMPTION OF ELEMENT BINDINGS (the test-is-the-product plan's M10c item 7b): a let binding a store element is
 * consumed when every read of its variable is in source form -- through a chain of consumed lets, through a helper's
 * parameter that the helper itself consumes (a planning-time effect's argument, an effect's connection) -- and not when
 * the element is navigated as a value, in the body or in the helper.
 */
class ElementConsumptionTest {

    private static final String MODEL = """
            Class x::Firm { name: String[1]; }
            function x::rt(db: meta::relational::metamodel::Database[1]): meta::core::runtime::Runtime[1]
            { ^meta::core::runtime::Runtime(connectionStores = ^meta::core::runtime::ConnectionStore(element = $db,
                connection = ^meta::external::store::relational::runtime::TestDatabaseConnection(
                  type = meta::relational::runtime::DatabaseType.H2))) }
            // the corpus's setupTestData: the store and the runtime handed to a helper that passes them to
            // connectionByElement (a planning-time effect) and the connection to executeInDb (the step's target)
            function x::setup(db: meta::relational::metamodel::Database[1], runtime: meta::core::runtime::Runtime[1]): Boolean[1]
            { let c = $runtime->meta::core::runtime::connectionByElement($db)->cast(
                @meta::external::store::relational::runtime::TestDatabaseConnection);
              meta::relational::metamodel::execute::executeInDb('insert into FIRM values (3, \\'Cyl\\')', $c); true; }
            // a helper that reads its parameter as a value: the store's tables
            function x::tables(db: meta::relational::metamodel::Database[1]): String[*] { $db.schemas.tables.name }
            function x::setupData(csvs: String[*], db: meta::relational::metamodel::Database[1], runtime: meta::core::runtime::Runtime[1]): Boolean[1]
            { let connection = $runtime.connectionByElement($db)->cast(@meta::external::store::relational::runtime::TestDatabaseConnection);
              let cfg = meta::relational::functions::sqlQueryToString::createDbConfig($connection.type);
              let sqls = $csvs->joinStrings('\\n-\\n')->meta::alloy::service::execution::setUpDataSQLs($db);
              $sqls->map(sql | meta::relational::metamodel::execute::executeInDb($sql, $connection));
              true; }
            // the engine's createDbConfig, SUBSUMED by name (builtin/Subsumed): an opaque value never evaluated -- its
            // body's assert (the engine's own asserts a DbExtension exists) runs on no road and is no kind of the body
            function meta::relational::functions::sqlQueryToString::createDbConfig(
                dbType: meta::relational::runtime::DatabaseType[1]): String[1] { assert(true, 'never run'); 'cfg'; }
            // a helper that hands its parameter on to a consuming helper
            function x::setupVia(db: meta::relational::metamodel::Database[1]): Boolean[1] { x::setup($db, x::rt($db)) }
            // the corpus's setupTestData whole: the CSV spelled as DDL and rows against the store (a planning-time
            // seed form), each statement sent through a map over executeInDb on the connection
            function x::seed(db: meta::relational::metamodel::Database[1], runtime: meta::core::runtime::Runtime[1]): Boolean[1]
            { let connection = $runtime->meta::core::runtime::connectionByElement($db)->cast(
                @meta::external::store::relational::runtime::TestDatabaseConnection);
              let sqls = 'default\\nFIRM\\nID,NAME\\n3,Cyl\\n'->meta::alloy::service::execution::setUpDataSQLs($db);
              $sqls->map(sql | meta::relational::metamodel::execute::executeInDb($sql, $connection)); true; }
            // the same with the DATA computed from the store: a navigation inside the seed form's value argument
            function x::seedComputed(db: meta::relational::metamodel::Database[1], runtime: meta::core::runtime::Runtime[1]): Boolean[1]
            { let connection = $runtime->meta::core::runtime::connectionByElement($db)->cast(
                @meta::external::store::relational::runtime::TestDatabaseConnection);
              let sqls = $db.schemas.tables.name->joinStrings(',')->meta::alloy::service::execution::setUpDataSQLs($db);
              $sqls->map(sql | meta::relational::metamodel::execute::executeInDb($sql, $connection)); true; }
            ###Relational
            Database x::DB ( Table FIRM (ID INTEGER PRIMARY KEY, NAME VARCHAR(32)) )
            ###Mapping
            Mapping x::M ( *x::Firm: Relational { ~mainTable [x::DB] FIRM name: [x::DB] FIRM.NAME } )
            ###Connection
            RelationalDatabaseConnection x::Conn { store: x::DB; type: H2; specification: LocalH2 { }; auth: DefaultH2; }
            ###Runtime
            Runtime x::RT { mappings: [x::M]; connections: [ x::DB: [ env: x::Conn ] ]; }
            """;
    private static final String QUERY = "|x::Firm.all()->project([f|$f.name], ['name'])";

    private static ElementConsumption of(String body) {
        ModelContext ctx = Compiler.compileModel(MODEL);
        TypedQuery q = Compiler.query(ctx, body);
        return new ElementConsumption(q.body(), BodyClass.systemClasses(ctx), ctx, q.specs());
    }

    @Test
    void aChainOfLetsEndingInAnOrchestrationArgumentIsConsumed() {
        ElementConsumption a = of("{| let r0 = x::rt(x::DB); let r = ^$r0(connectionStores = $r0.connectionStores);"
                + " let result = meta::pure::router::execute(" + QUERY + ", x::M, $r, []).values;"
                + " assertEquals(['Acme'], $result.rows.values);}");
        assertTrue(a.elementOnlyLet(0), "r0 builds a Runtime naming the store");
        assertTrue(a.elementOnlyLet(1), "r copies r0: the element carried on");
        assertTrue(a.consumedElementLet(0), "r0 is read only by r, which the execute consumes");
        assertTrue(a.consumedElementLet(1));
    }

    @Test
    void aChainReadAsAValueIsNotConsumed() {
        ElementConsumption a = of("{| let r0 = x::rt(x::DB); let r = ^$r0(connectionStores = $r0.connectionStores);"
                + " assertEquals(1, $r.connectionStores->size());}");
        assertTrue(a.elementOnlyLet(0));
        assertFalse(a.consumed(1), "r is read by the assert");
        assertFalse(a.consumed(0), "so r0's read by r is a read");
    }

    @Test
    void aParameterTheHelperConsumesConsumesTheArgument() {
        ElementConsumption a = of("{| let db = x::DB; let runtime = x::rt($db); x::setup($db, $runtime);"
                + " let result = meta::pure::router::execute(" + QUERY + ", x::M, $runtime, []).values;"
                + " assertEquals(3, $result.rows->size());}");
        assertTrue(a.elementOnlyLet(0), "db is the store element");
        assertTrue(a.elementOnlyLet(1), "runtime carries it (the parameter of rt flows into a construction)");
        assertTrue(a.consumedElementLet(1), "runtime: read by setup's consuming parameter and by the execute");
        assertTrue(a.consumedElementLet(0), "db: read by rt (a consumed carrier) and by setup's consuming parameter");
        // the boundary: a helper's RESULT carrying the parameter into another helper's argument (x::setup($db,
        // x::rt($db))) is not followed -- rt's last statement reads its parameter, and the rule does not ask where the
        // result flows (no corpus body has the shape; the corpus binds the runtime by a let first, as above)
        ElementConsumption b = of("{| let db = x::DB; x::setupVia($db); assertEquals(true, true);}");
        assertFalse(b.consumedElementLet(0), "a result carrying the parameter is a read of it");
    }

    /** The audit's HIGH-1: a let that only HANDS the element's variable to a planning-time native carries nothing --
     *  the plan read as a value is no read of the store, and the body is not system-routed by it. */
    @Test
    void aLetHandingTheElementToAPlanningTimeNativeCarriesNothing() {
        String body = "{| let rt = x::rt(x::DB); let plan = meta::pure::executionPlan::executionPlan(" + QUERY
                + ", x::M, $rt, []); assertNotEmpty($plan.rootExecutionNode.executionNodes);}";
        ElementConsumption a = of(body);
        assertTrue(a.consumedElementLet(0), "rt is consumed by the plan's orchestration argument");
        assertFalse(a.elementOnlyLet(1), "the plan carries no element: it is a value of its own");
        ModelContext ctx = Compiler.compileModel(MODEL);
        assertFalse(BodyClass.systemRouted(Compiler.query(ctx, body), "x::RT"), "no system read: " + body);
    }

    /** The corpus's setupTestData whole (the audit's MEDIUM-3): the store consumed by the seed form's store argument
     *  and by connectionByElement, the runtime by connectionByElement, the connection by executeInDb's target inside
     *  the map's lambda; a seed form whose DATA navigates the store reads it (MEDIUM-2). */
    @Test
    void theSeedHelperConsumesTheStoreAndTheRuntime() {
        ElementConsumption a = of("{| let db = x::DB; let runtime = x::rt($db); x::seed($db, $runtime);"
                + " let result = meta::pure::router::execute(" + QUERY + ", x::M, $runtime, []).values;"
                + " assertEquals(3, $result.rows->size());}");
        assertTrue(a.consumedElementLet(0), "db: the seed form's store argument, connectionByElement's");
        assertTrue(a.consumedElementLet(1), "runtime: connectionByElement's, the execute's");
        ElementConsumption b = of("{| let db = x::DB; let runtime = x::rt($db); x::seedComputed($db, $runtime);"
                + " let result = meta::pure::router::execute(" + QUERY + ", x::M, $runtime, []).values;"
                + " assertEquals(3, $result.rows->size());}");
        assertFalse(b.consumedElementLet(0), "db navigated inside the seed form's data argument: a read");
        assertTrue(b.consumedElementLet(1));
    }

    /** An effect's element argument is its target (the audit's MEDIUM-3): the store handed to dropAndCreateTableInDb
     *  is read at planning (by name), not as a row. */
    @Test
    void anEffectsElementArgumentIsItsTarget() {
        ElementConsumption a = of("{| let db = x::DB; let c = ^meta::external::store::relational::runtime::"
                + "TestDatabaseConnection(type = meta::relational::runtime::DatabaseType.H2);"
                + " meta::relational::functions::toDDL::dropAndCreateTableInDb($db, 'FIRM', $c); assertEquals(true, true);}");
        assertTrue(a.consumedElementLet(0));
    }

    /** The audit's LOW-4: a lambda a collection native RUNS evaluates where the let is spliced -- a navigation of the
     *  store inside it is a read, so the let carries nothing and the store's binding is not consumed. */
    @Test
    void aLambdaACollectionNativeRunsIsEvaluated() {
        ElementConsumption a = of("{| let db = x::DB; let t = [1]->map(i | $db.schemas.tables.name);"
                + " let result = meta::pure::router::execute(|x::Firm.all()->filter(f | $t->contains($f.name))"
                + "->project([f|$f.name], ['name']), x::M, x::RT.runtimeValue, []).values;"
                + " assertEquals([], $result.rows.values);}");
        assertFalse(a.elementOnlyLet(1), "the navigation inside the mapped lambda is a read");
        assertFalse(a.consumed(0));
    }

    /** Item 7c: the corpus's setup helper as the resolver unfolds it -- the connection picked by the runtime's qualified
     *  property (a handle by its type), read by a SUBSUMED call's argument (no read: never evaluated) and by the effect's
     *  target inside the map's lambda -- consumed, so the store and the runtime it carries are consumed too. */
    @Test
    void aSubsumedCallsArgumentIsNoRead() {
        ElementConsumption a = of("{| let db = x::DB; let runtime = x::rt($db);"
                + " let connection = $runtime.connectionByElement($db)->cast("
                + "@meta::external::store::relational::runtime::TestDatabaseConnection);"
                + " let cfg = meta::relational::functions::sqlQueryToString::createDbConfig($connection.type);"
                + " let sqls = 'default\\nFIRM\\nID,NAME\\n3,Cyl\\n'->meta::alloy::service::execution::setUpDataSQLs($db);"
                + " $sqls->map(sql | meta::relational::metamodel::execute::executeInDb($sql, $connection));"
                + " let result = meta::pure::router::execute(" + QUERY + ", x::M, $runtime, []).values;"
                + " assertEquals(['Cyl'], $result.rows.values);}");
        assertTrue(a.consumed(2), "connection: read by a subsumed call's argument and by executeInDb's target");
        assertTrue(a.consumedElementLet(1), "runtime: carried into the connection (a consumed handle) and the execute");
        assertTrue(a.consumedElementLet(0), "db: the runtime, the connection, the seed form's store argument");
    }

    /** Item 7c: the corpus's setup helper, unfolded by the resolver (the parity test runs it on both roads); here the
     *  kinds of the subsumed call's let -- the engine's createDbConfig body asserts, and that assert tagged every
     *  setupTestData body ASSERT, which kept them all on the walk -- and the body's class. */
    @Test
    void aSubsumedCallsBodyIsNoKindOfTheStatement() {
        ModelContext ctx = Compiler.compileModel(MODEL);
        String body = "{| let firmCsv = 'default\\nFIRM\\nID,NAME\\n3,Cyl\\n'; let db = x::DB; let runtime = x::rt($db);"
                + " x::setupData([$firmCsv], $db, $runtime);"
                + " let result = meta::pure::router::execute(" + QUERY + ", x::M, $runtime, []);"
                + " assertEquals(['Cyl'], $result.values.rows.values);}";
        com.legend.protocol.spec.ValueSpecification parsed = Compiler.parseQuery(body);
        java.util.List<com.legend.protocol.spec.ValueSpecification> statements =
                parsed instanceof com.legend.protocol.spec.LambdaFunction lf && lf.parameters().isEmpty()
                        ? lf.body() : java.util.List.of(parsed);
        TypedQuery q = Compiler.queryResolved(ctx, Compiler.resolveQuery(statements,
                new com.legend.model.ImportScope(java.util.List.of()), ctx));
        com.legend.planner.QueryDoor.Scan scan = new com.legend.planner.QueryDoor.Scan(q, "x::RT");
        boolean sawConfig = false;
        for (TypedSpec s : q.body()) {
            if (s instanceof TypedLet l && l.name().endsWith("_cfg")) {
                sawConfig = true;
                assertTrue(scan.kinds(s).isEmpty(), "a subsumed call's body is no kind: " + scan.kinds(s));
            }
        }
        assertTrue(sawConfig, "the helper was unfolded and its config let is in the body");
        assertEquals(BodyClass.FRAME_PROGRAM, BodyClass.of(q, "x::RT", true));
    }

    @Test
    void aParameterTheHelperNavigatesIsARead() {
        ElementConsumption a = of("{| let db = x::DB; let runtime = x::rt($db); assertEquals(['FIRM'], x::tables($db));"
                + " let result = meta::pure::router::execute(" + QUERY + ", x::M, $runtime, []).values;"
                + " assertEquals(2, $result.rows->size());}");
        assertTrue(a.elementOnlyLet(0));
        assertFalse(a.consumed(0), "tables navigates its parameter: the store's rows");
        // the navigation in the body itself
        ElementConsumption b = of("{| let db = x::DB; let t = $db.schemas.tables.name; assertEquals(['FIRM'], $t);}");
        assertFalse(b.elementOnlyLet(1), "a navigation is a read, not a carrier");
        assertFalse(b.consumed(0));
    }
}
