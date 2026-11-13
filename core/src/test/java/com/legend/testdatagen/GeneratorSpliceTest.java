// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testdatagen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.GeneratorSplice;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPlannedValue;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTestDataGen;
import com.legend.error.NotImplementedException;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.testbackend.TestBackends;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * THE GENERATOR'S READS SPLICED, BY THE COMPILER (B2d.2's audit, L2, 2026-11-11): {@link GeneratorSplice} over typed
 * bodies with a planner that answers fixed values -- what each read of the generator's result becomes, what the
 * comparison of its loads becomes, and what is refused by name -- with nothing planned or run.
 */
class GeneratorSpliceTest {

    /** The csv's statement the stub planner answers. */
    private static final SqlQuery CSV = SqlSelect.starOf(new SqlSource.Dual());
    private static final SqlQuery HOLDS = SqlSelect.starOf(new SqlSource.Dual());
    private static final SqlQuery DIFFERENCES = SqlSelect.starOf(new SqlSource.Dual());

    private static final GeneratorSplice.Generator STUB = new GeneratorSplice.Generator() {
        @Override
        public GeneratorSplice.Generated plan(TypedTestDataGen g) {
            return new GeneratorSplice.Generated() {
                @Override
                public List<String> sqls() {
                    return List.of("first fetch", "second fetch");
                }

                @Override
                public SqlQuery csv() {
                    return CSV;
                }
            };
        }

        @Override
        public GeneratorSplice.Same same(GeneratorSplice.Generated generated, String expected, String dbFqn,
                String lead) {
            return expected.isEmpty() ? new GeneratorSplice.Same.Fails(lead + "planned failure")
                    : new GeneratorSplice.Same.Compared(HOLDS, DIFFERENCES);
        }

        @Override
        public GeneratorSplice.Slice slice(GeneratorSplice.Generated generated, String dbFqn) {
            return new GeneratorSplice.Slice(Set.of(dbFqn), Map.of());
        }
    };

    @Test
    void theSqlsAreConstantsAndTheTextAPlannedValue() {
        List<TypedSpec> out = spliced("assertSize($testData.sqls, 2);\nassertEquals('x', $testData.dataCsvString);");
        assertFalse(out.stream().anyMatch(s -> s instanceof TypedLet l && "testData".equals(l.name())),
                "the generator's let is spliced away: " + out);
        List<TypedSpec> nodes = nodes(out);
        assertTrue(nodes.stream().anyMatch(n -> n instanceof TypedCollection c && c.elements().equals(List.of(
                new TypedCString("first fetch", c.elements().get(0).info()),
                new TypedCString("second fetch", c.elements().get(0).info())))), "the sqls a literal list: " + out);
        assertTrue(nodes.stream().anyMatch(n -> n instanceof TypedPlannedValue v && v.statement() == CSV),
                "the text the planned statement's cell: " + out);
    }

    @Test
    void theResultReadAnyOtherWayIsRefusedByName() {
        assertEquals("the test-data generator's result's relationTree: not planned (B2d answers its sqls and its"
                + " dataCsvString)", assertThrows(NotImplementedException.class,
                        () -> spliced("assertEquals(1, $testData.relationTree->size());")).getMessage());
    }

    @Test
    void theLoadsComparedAreTheDatabasesComparison() {
        String loads = "let s1 = meta::alloy::service::execution::setUpDataSQLs($testData.dataCsvString, t::DB);\n"
                + "let s2 = meta::alloy::service::execution::setUpDataSQLs('%s', t::DB);\n"
                + "assertSameElements($s1, $s2);";
        // the database's: assert(<holds' cell>, <differences' cell>), the loads' lets consumed
        List<TypedSpec> compared = spliced(loads.formatted("default\\nPERSON\\nID\\n2\\n"));
        TypedSpec last = compared.get(compared.size() - 1);
        assertTrue(last instanceof TypedNativeCall call && call.callee().qualifiedName().endsWith("::assert")
                && call.args().get(0) instanceof TypedPlannedValue h && h.statement() == HOLDS
                && call.args().get(1) instanceof TypedPlannedValue d && d.statement() == DIFFERENCES, compared.toString());
        assertFalse(compared.stream().anyMatch(s -> s instanceof TypedLet l && l.name().startsWith("s")), "" + compared);
        // decided at planning: assert(false, <the words>)
        List<TypedSpec> failed = spliced(loads.formatted(""));
        TypedSpec fails = failed.get(failed.size() - 1);
        assertTrue(fails instanceof TypedNativeCall call
                && call.args().get(1) instanceof TypedCString w && "planned failure".equals(w.value()), "" + failed);
    }

    @Test
    void aDatabaseEffectBetweenTheLetAndARead() {
        String effect = "let conn = t::RT.runtimeValue.connectionStores.connection->toOne()"
                + "->cast(@meta::external::store::relational::runtime::TestDatabaseConnection);\n"
                + "meta::relational::metamodel::execute::executeInDb('DELETE FROM BICYCLE', $conn);\n";
        assertTrue(assertThrows(NotImplementedException.class, () -> spliced(effect
                + "assertEquals('x', $testData.dataCsvString);")).getMessage().startsWith("a database effect between"
                        + " the test-data generator's let and a read of its result"));
        // the effect before the let: none between
        List<TypedSpec> out = spliced(effect + "assertEquals('x', $testData.dataCsvString);", true);
        assertSame(CSV, nodes(out).stream().filter(n -> n instanceof TypedPlannedValue).map(n ->
                ((TypedPlannedValue) n).statement()).findFirst().orElseThrow());
    }

    @Test
    void afterAReloadAStatementThatReachesTheDatabaseIsRefused() {
        String reload = "let conn = $runtime.connectionStores.connection->toOne()"
                + "->cast(@meta::external::store::relational::runtime::TestDatabaseConnection);\n"
                + "let loads = meta::alloy::service::execution::setUpDataSQLs($testData.dataCsvString, t::DB);\n"
                + "$loads->map(sql | meta::relational::metamodel::execute::executeInDb($sql, $conn));\n";
        assertTrue(assertThrows(NotImplementedException.class, () -> spliced(reload
                + "assertEquals(6, t::names()->size());")).getMessage().startsWith("a statement that reads or writes"
                        + " the database after the test-data generator's slice was reloaded"));
        // a statement that reaches nothing stays
        List<TypedSpec> out = spliced(reload + "assertEquals(2, t::two());");
        assertTrue(out.get(out.size() - 1) instanceof TypedNativeCall, out.toString());
        // reloaded on a connection that is not a runtime's
        assertTrue(assertThrows(NotImplementedException.class, () -> spliced(reload.replace(
                "$runtime.connectionStores.connection->toOne()", "$runtime.connectionStores.connection->at(0)")
                + "assertEquals(2, t::two());")).getMessage().startsWith("the test-data generator's slice reloaded on a"
                        + " connection that is not a runtime's"));
    }

    /** {@code statements} after the program test's lets, typed and spliced with the stub planner. */
    private static List<TypedSpec> spliced(String statements) {
        return spliced(statements, false);
    }

    /** {@link #spliced(String)}; {@code before}: the statements stand before the generator's let instead, the reads
     *  after it. */
    private static List<TypedSpec> spliced(String statements, boolean before) {
        ModelContext ctx = Compiler.compileModel(GeneratedTestDataProgramTest.model(TestBackends.inProcess().get(0)));
        String lets = GeneratedTestDataProgramTest.LETS;
        String body;
        if (before) {
            int read = statements.lastIndexOf("assertEquals(");
            int gen = lets.indexOf("let testData");
            body = lets.substring(0, gen) + statements.substring(0, read) + lets.substring(gen)
                    + statements.substring(read);
        } else {
            body = lets + statements;
        }
        TypedQuery typed = GeneratedTestDataProgramTest.typed(ctx, "{|" + body + "}");
        return GeneratorSplice.spliced(typed.body(), ctx, typed.specs(), STUB);
    }

    /** Every node of {@code body}. */
    private static List<TypedSpec> nodes(List<TypedSpec> body) {
        List<TypedSpec> out = new ArrayList<>();
        List<TypedSpec> todo = new ArrayList<>(body);
        while (!todo.isEmpty()) {
            TypedSpec n = todo.remove(todo.size() - 1);
            out.add(n);
            todo.addAll(n.children());
        }
        return out;
    }
}
