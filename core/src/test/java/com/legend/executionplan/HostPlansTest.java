package com.legend.executionplan;

import com.legend.database.Databases;
import com.legend.executionplan.ExecutionPlan.Column;
import com.legend.executionplan.ExecutionPlan.Database;
import com.legend.executionplan.ExecutionPlan.Multiplicity;
import com.legend.executionplan.ExecutionPlan.Parameter;
import com.legend.executionplan.ExecutionPlan.Sequence;
import com.legend.executionplan.ExecutionPlan.Servers;
import com.legend.executionplan.ExecutionPlan.SetupStep;
import com.legend.executionplan.ExecutionPlan.Slot;
import com.legend.executionplan.ExecutionPlan.Sql;
import com.legend.executionplan.ExecutionPlan.Target;
import com.legend.executionplan.ExecutionPlan.TdsResult;
import com.legend.executionplan.ExecutionPlan.Update;
import com.legend.model.AuthenticationSpec;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionSpecification;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** What a host runs, refused at planning by name (docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md D6, R2): a query's rows
 *  ({@link HostPlans#read}) and a program that writes ({@link HostPlans#program}) -- the refusals the tab's runner made
 *  at run time until 2026-11-02, each one here. */
class HostPlansTest {

    private static Target target(Servers servers, List<SetupStep> setup) {
        return new Target(new Database.Declared(new ConnectionDefinition("store::Conn", "store::DB", Databases.PLATFORM,
                new ConnectionSpecification.InMemory(), new AuthenticationSpec.TestAuth())), servers, List.of(), setup);
    }

    private static final Target HOSTS = target(new Servers.Every(), List.of());

    private static final Column COUNT = new Column("value", "Integer", new Multiplicity(1, 1), "BIGINT", null, null, false);

    private static TdsResult result(Target t, List<Slot> slots) {
        return new TdsResult(TdsResult.Shape.RELATION, List.of(COUNT), new Sql("select count(*) as value from T", slots, t, null));
    }

    private static ExecutionPlan write(UnaryOperator<Sql> update, TdsResult answer) {
        Sql insert = update.apply(new Sql("insert into T select * from S", List.of(), HOSTS, null));
        return new ExecutionPlan(List.of(), new Sequence(List.of(new Update(insert, "n"), answer)));
    }

    private static void refused(String why, Runnable planning) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, planning::run);
        assertTrue(e.getMessage().contains(why), e.getMessage());
    }

    @Test
    void aQuerysRowsAreOneTypedResultOverAStatementOfText() {
        ExecutionPlan rows = new ExecutionPlan(List.of(), result(HOSTS, List.of()));
        assertSame(rows, HostPlans.read(rows));
        // its setup steps are the server's: a host runs a query's rows on its own database, which holds its data
        ExecutionPlan setUp = new ExecutionPlan(List.of(), result(target(new Servers.Every(),
                List.of(new SetupStep.Statement("create table T(ID INT)"))), List.of()));
        assertSame(setUp, HostPlans.read(setUp));
        refused("this query's plan is not one (Sequence: a pivot whose columns the data names, or a program that writes)",
                () -> HostPlans.read(write(UnaryOperator.identity(), result(HOSTS, List.of()))));
        refused("this one binds values (1)", () -> HostPlans.read(new ExecutionPlan(List.of(), result(HOSTS,
                List.of(new Slot("p", new ExecutionPlan.Binding.One("VARCHAR", null)))))));
    }

    @Test
    void aProgramThatWritesIsWhatTheHostsRunnerRuns_andAnythingElseIsRefusedByName() {
        ExecutionPlan program = write(UnaryOperator.identity(), result(HOSTS, List.of()));
        assertSame(program, HostPlans.program(program));
        ExecutionPlan rows = new ExecutionPlan(List.of(), result(HOSTS, List.of()));
        assertSame(rows, HostPlans.program(rows));
        refused("takes parameters", () -> HostPlans.program(new ExecutionPlan(
                List.of(new Parameter("p", "String", new Multiplicity(1, 1), List.of())), result(HOSTS, List.of()))));
        refused("one typed result, not 0", () -> HostPlans.program(new ExecutionPlan(List.of(),
                new Sequence(List.of(new Update(new Sql("delete from T", List.of(), HOSTS, null), null))))));
        refused("run on one database: these name several", () -> HostPlans.program(write(UnaryOperator.identity(),
                result(target(new Servers.Versions(List.of("2.1")), List.of()), List.of()))));
        refused("some servers' versions alone", () -> HostPlans.program(new ExecutionPlan(List.of(),
                result(target(new Servers.Versions(List.of("2.1")), List.of()), List.of()))));
        refused("sets its database up first", () -> HostPlans.program(new ExecutionPlan(List.of(),
                result(target(new Servers.Every(), List.of(new SetupStep.Statement("create table T(ID INT)"))), List.of()))));
        refused("binds a list", () -> HostPlans.program(write(UnaryOperator.identity(), result(HOSTS,
                List.of(new Slot("ids", new ExecutionPlan.Binding.Array("INTEGER")))))));
        refused("takes its type from its value", () -> HostPlans.program(write(UnaryOperator.identity(), result(HOSTS,
                List.of(new Slot("t", new ExecutionPlan.Binding.One("TIMESTAMP", new ExecutionPlan.TypeHole(7,
                        java.util.Map.of(ExecutionPlan.ValueKind.DATE_TIME, new ExecutionPlan.TypeSpelling("TIMESTAMP",
                                ExecutionPlan.Digits.NONE)), ExecutionPlan.ValueKind.DATE_TIME))))))));
    }
}
