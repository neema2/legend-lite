// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testcases;

import com.legend.executionplan.ExecutionPlan;

import java.sql.Connection;
import java.util.List;


/**
 * The plan cases core's tests share (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 2's landing 2): queries with
 * parameters of every kind, each beside the query today's paths answer the same values with, the models they read, and
 * a plan run on a connection by the runner ({@code exec.PlanRunner}). {@code PlanMakerTest} runs them on
 * DuckDB and H2, {@code PostgresArmTest} on Postgres.
 */
public final class PlanCases {

    private PlanCases() {
    }

    /** A query with parameters, and the same query with each parameter a {@code let} of its value -- how the server
     *  binds a request's values today ({@code PureV1Api.boundParameters}) -- with the values as the runner takes them:
     *  each its Pure type's Java value ({@code exec.PlanParameters}), a list a List. */
    public record Parameterised(String parameters, String lets, String body, java.util.Map<String, Object> values) {

        /** The query with its parameters, as a plan is made from it. */
        public String withParameters() {
            return "{" + parameters + "|" + body + "}";
        }

        /** The query with each parameter a {@code let} of its value, as today's paths answer it. */
        public String withLets() {
            return "|" + lets + body + ";";
        }

    }

    /** The scalar cases over {@code table} (ID, NAME, PRICE; three rows). */
    public static List<Parameterised> scalars(String table) {
        return List.of(
            new Parameterised("n: Integer[1]", "let n = 1;",
                    "#>{s::DB." + table + "}#->filter(r|$r.ID > $n)->select(~[ID, NAME])->sort(~ID->ascending())",
                    java.util.Map.of("n", 1L)),
            new Parameterised("s: String[1]", "let s = 'O\\'Brien';",
                    "#>{s::DB." + table + "}#->filter(r|$r.NAME == $s)->select(~[ID, NAME])", java.util.Map.of("s", "O'Brien")),
            new Parameterised("p: Decimal[1]", "let p = 2.00D;",
                    "#>{s::DB." + table + "}#->filter(r|$r.PRICE < $p)->select(~[ID])->sort(~ID->ascending())",
                    java.util.Map.of("p", new java.math.BigDecimal("2.00"))),
            // one parameter written twice: compared, and added to a column
            new Parameterised("n: Integer[1]", "let n = 2;",
                    "#>{s::DB." + table + "}#->filter(r|$r.ID != $n)->extend(~plus: r|$r.ID + $n)->select(~[ID, plus])"
                            + "->sort(~ID->ascending())", java.util.Map.of("n", 2L)),
            new Parameterised("d: StrictDate[1]", "let d = %2024-01-02;",
                    "#>{s::DB." + table + "}#->extend(~d: r|$d)->select(~[ID, d])->sort(~ID->ascending())",
                    java.util.Map.of("d", java.time.LocalDate.of(2024, 1, 2))),
            new Parameterised("b: Boolean[1]", "let b = true;",
                    "#>{s::DB." + table + "}#->filter(r|$b)->select(~[ID])->sort(~ID->ascending())", java.util.Map.of("b", true)),
            // a Float is bound as a decimal (the numeric charter's Rule 1: a Float literal is a decimal in the database)
            new Parameterised("f: Float[1]", "let f = 1.1;",
                    "#>{s::DB." + table + "}#->extend(~x: r|$r.ID * $f)->select(~[ID, x])->sort(~ID->ascending())",
                    java.util.Map.of("f", 1.1d)),
            new Parameterised("f: Float[1]", "let f = 1.1;",
                    "#>{s::DB." + table + "}#->extend(~f: r|$f)->select(~[ID, f])->sort(~ID->ascending())",
                    java.util.Map.of("f", 1.1d)),
            // at an extreme magnitude a Float literal is a double (Rule 1), and so is its bound value
            new Parameterised("f: Float[1]", "let f = 1.5e15;",
                    "#>{s::DB." + table + "}#->extend(~f: r|$f)->select(~[ID, f])->sort(~ID->ascending())",
                    java.util.Map.of("f", 1.5e15d)),
            new Parameterised("f: Float[1]", "let f = 2.5e-7;",
                    "#>{s::DB." + table + "}#->extend(~f: r|$f)->select(~[ID, f])->sort(~ID->ascending())",
                    java.util.Map.of("f", 2.5e-7d)),
            new Parameterised("p: Decimal[1]", "let p = 2.50D;",
                    "#>{s::DB." + table + "}#->extend(~[p: r|$p, x: r|$r.ID * $p])->select(~[ID, p, x])->sort(~ID->ascending())",
                    java.util.Map.of("p", new java.math.BigDecimal("2.50"))),
            new Parameterised("t: DateTime[1]", "let t = %2024-01-02T10:30:00;",
                    "#>{s::DB." + table + "}#->filter(r|$r.ID < 3)->extend(~t: r|$t)->select(~[ID, t])->sort(~ID->ascending())",
                    java.util.Map.of("t", java.time.LocalDateTime.of(2024, 1, 2, 10, 30))),
            // to the nanosecond, as its literal keeps it (H2's cast is TIMESTAMP(9); DuckDB and Postgres keep
            // microseconds, the literal and the bound value alike)
            new Parameterised("t: DateTime[1]", "let t = %2024-01-02T10:30:00.123456789;",
                    "#>{s::DB." + table + "}#->filter(r|$r.ID < 3)->extend(~t: r|$t)->select(~[ID, t])->sort(~ID->ascending())",
                    java.util.Map.of("t", java.time.LocalDateTime.of(2024, 1, 2, 10, 30, 0, 123_456_789))),
            // a parameter whose value decides its type: bound as its value's kind
            new Parameterised("d: Date[1]", "let d = %2024-01-02;",
                    "#>{s::DB." + table + "}#->extend(~d: r|$d)->select(~[ID, d])->sort(~ID->ascending())",
                    java.util.Map.of("d", java.time.LocalDate.of(2024, 1, 2))),
            new Parameterised("d: Date[1]", "let d = %2024-01-02T10:30:00;",
                    "#>{s::DB." + table + "}#->extend(~d: r|$d)->select(~[ID, d])->sort(~ID->ascending())",
                    java.util.Map.of("d", java.time.LocalDateTime.of(2024, 1, 2, 10, 30))),
            // a Number: a whole one, and a decimal one (a Float's literal)
            new Parameterised("n: Number[1]", "let n = 1;",
                    "#>{s::DB." + table + "}#->filter(r|$r.ID > $n)->select(~[ID])->sort(~ID->ascending())",
                    java.util.Map.of("n", 1L)),
            // projected again (PARK-24 closed 2026-10-18: a whole Float's text has its .0 on every database)
            new Parameterised("n: Number[1]", "let n = 1.5;",
                    "#>{s::DB." + table + "}#->extend(~x: r|$r.ID * $n)->select(~[ID, x])->sort(~ID->ascending())",
                    java.util.Map.of("n", 1.5d)),
            // two parameters
            new Parameterised("lo: Integer[1], hi: Integer[1]", "let lo = 1; let hi = 3;",
                    "#>{s::DB." + table + "}#->filter(r|($r.ID > $lo) && ($r.ID < $hi))->select(~[ID, NAME])",
                    java.util.Map.of("lo", 1L, "hi", 3L)));
    }

    /** A query with an optional parameter {@code x}: run with a value, its plan answers as the query with that value as
     *  a {@code let}; run with none, as the query with {@code x} written empty ({@code []}), which lite lowers as the
     *  engine does (an equality with an empty side is a null check: pureToSQLQuery's nullSafeEqualsOperation). */
    public record OptionalCase(String parameter, String body, String let, Object value) {

        public String withParameter() {
            return "{" + parameter + "|" + body + "}";
        }

        public String withValue() {
            return "|" + let + body + ";";
        }

        public String withNone() {
            return "|" + body.replace("$x", "[]");
        }
    }

    /** The optional cases over {@code table} (ID, NAME, PRICE; three rows, one NAME absent). */
    public static List<OptionalCase> optionals(String table) {
        String t = "#>{s::DB." + table + "}#";
        return List.of(
                new OptionalCase("x: String[0..1]", t + "->filter(r|$r.NAME == $x)->select(~[ID, NAME])"
                        + "->sort(~ID->ascending())", "let x = 'a';", "a"),
                new OptionalCase("x: Integer[0..1]", t + "->filter(r|$r.ID != $x)->select(~[ID])"
                        + "->sort(~ID->ascending())", "let x = 1;", 1L),
                // a value-typed one: its absence a null of its absent kind (on H2, a cast to that kind's type)
                new OptionalCase("x: Float[0..1]", t + "->filter(r|$r.ID != $x)->select(~[ID])"
                        + "->sort(~ID->ascending())", "let x = 2.0;", 2.0d));
    }

    /** An account's status stored as a code: ACTIVE as 'A' or 'X' (one name, two codes), CLOSED as 'C'; one account
     *  has none, one a code the mapping does not know. {@code connection}: the connection's type, specification and
     *  authentication. */
    public static String enumModel(String connection, String table) {
        return """
                Enum s::Status { ACTIVE, CLOSED }
                Class s::Acct { id: Integer[1]; status: s::Status[0..1]; }
                ###Relational
                Database s::DB ( Table %2$s ( ID INTEGER PRIMARY KEY, ST VARCHAR(1) ) )
                ###Mapping
                Mapping s::M
                (
                  s::Status: EnumerationMapping St { ACTIVE: ['A', 'X'], CLOSED: 'C' }
                  *s::Acct: Relational { ~mainTable [s::DB] %2$s
                    id: [s::DB] %2$s.ID, status: EnumerationMapping St: [s::DB] %2$s.ST }
                )
                ###Connection
                RelationalDatabaseConnection s::Conn { store: s::DB; %1$s }
                ###Runtime
                Runtime s::RT { mappings: [s::M]; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """.formatted(connection, table);
    }

    /** {@link #enumModel(String, String)}'s rows, as a seed's CSV blocks. */
    public static String enumRows(String table) {
        return "default\n" + table + "\nID,ST\n1,A\n2,X\n3,C\n4,---null---\n5,Z\n";
    }

    /** The enumeration cases: compared with the mapped property (==, !=), and written as a value of its own. */
    public static List<Parameterised> enumerations() {
        return List.of(
                new Parameterised("st: s::Status[1]", "let st = s::Status.ACTIVE;",
                        "s::Acct.all()->filter(a|$a.status == $st)->project(~[id: a|$a.id])->sort(~id->ascending())",
                        java.util.Map.of("st", "ACTIVE")),
                new Parameterised("st: s::Status[1]", "let st = s::Status.CLOSED;",
                        "s::Acct.all()->filter(a|$a.status != $st)->project(~[id: a|$a.id])->sort(~id->ascending())",
                        java.util.Map.of("st", "CLOSED")),
                new Parameterised("st: s::Status[1]", "let st = s::Status.ACTIVE;",
                        "s::Acct.all()->filter(a|$a.id < 3)->project(~[id: a|$a.id, s: a|$st])->sort(~id->ascending())",
                        java.util.Map.of("st", "ACTIVE")));
    }

    /** The list cases over {@code table} (ID, NAME, PRICE; three rows): a list parameter bound as one array. */
    public static List<Parameterised> lists(String table) {
        String t = "#>{s::DB." + table + "}#";
        return List.of(
                new Parameterised("ns: Integer[*]", "let ns = [1, 3];",
                        t + "->filter(r|$r.ID->in($ns))->select(~[ID, NAME])->sort(~ID->ascending())",
                        java.util.Map.of("ns", List.of(1L, 3L))),
                new Parameterised("ns: Integer[*]", "let ns = [];",
                        t + "->filter(r|$r.ID->in($ns))->select(~[ID])->sort(~ID->ascending())",
                        java.util.Map.of("ns", List.of())),
                new Parameterised("ns: Integer[*]", "let ns = [1, 3];",
                        t + "->filter(r|!$r.ID->in($ns))->select(~[ID])->sort(~ID->ascending())",
                        java.util.Map.of("ns", List.of(1L, 3L))),
                new Parameterised("ns: Integer[*]", "let ns = [2, 3];",
                        t + "->filter(r|$ns->contains($r.ID))->select(~[ID])->sort(~ID->ascending())",
                        java.util.Map.of("ns", List.of(2L, 3L))),
                new Parameterised("ss: String[*]", "let ss = ['a', 'O\\'Brien'];",
                        t + "->filter(r|$r.ID->in([1, 2]) && $ss->contains($r.NAME->toOne()))->select(~[ID, NAME])"
                                + "->sort(~ID->ascending())", java.util.Map.of("ss", List.of("a", "O'Brien"))),
                // a DateTime list: its elements bound as timestamps
                new Parameterised("ts: DateTime[*]", "let ts = [%2024-01-02T10:30:00, %2024-01-05T00:00:00];",
                        t + "->filter(r|%2024-01-02T10:30:00->in($ts))->select(~[ID])->sort(~ID->ascending())",
                        java.util.Map.of("ts", List.of(java.time.LocalDateTime.of(2024, 1, 2, 10, 30),
                                java.time.LocalDateTime.of(2024, 1, 5, 0, 0)))),
                new Parameterised("ts: DateTime[*]", "let ts = [%2024-01-05T00:00:00];",
                        t + "->filter(r|%2024-01-02T10:30:00->in($ts))->select(~[ID])->sort(~ID->ascending())",
                        java.util.Map.of("ts", List.of(java.time.LocalDateTime.of(2024, 1, 5, 0, 0)))));
    }

    /** {@link #run(ExecutionPlan, Connection, java.util.Map)} for a plan of no parameters. */
    public static String run(ExecutionPlan plan, Connection c) throws java.io.IOException {
        return run(plan, c, java.util.Map.of());
    }

    /** {@code plan} run by the runner ({@code exec.PlanRunner}) on {@code c} with {@code values} (as legend-engine's
     *  execute API makes them), the target's setup run on {@code c} first: the text it writes. */
    public static String run(ExecutionPlan plan, Connection c, java.util.Map<String, ?> values)
            throws java.io.IOException {
        java.io.StringWriter out = new java.io.StringWriter();
        ExecutionPlan.Target target = switch (plan.root()) {
            case ExecutionPlan.TextResult text -> text.sql().target();
            case ExecutionPlan.Keyed keyed -> keyed.keys().target();
            case ExecutionPlan.TdsResult tds -> tds.sql().target();
            case ExecutionPlan.Sequence s -> throw new IllegalArgumentException("a plan of several steps");
        };
        com.legend.exec.PlanRunner.run(plan, values, com.legend.exec.PlanSessions.setUp(c, target), out);
        return out.toString();
    }

    /**
     * Today's path for {@code TypedQuery.Output.TDS_TEXT} (2026-10-17): the legacy route under
     * {@code ExecuteOptions.TDS_TEXT} answers a relation as its TDS text and a value as the value itself, so a
     * value's expected text is spelled here as the one-column relation {@code value} -- the header from the result's
     * type, each value's own text a row (loosely: {@code String.valueOf}; the plan path's TDS text is the judge of
     * record on the PCT lane, where every print form is pinned).
     */
    public static String todayTdsText(String model, String query, String runtime, Connection c) {
        com.legend.exec.ExecutionResult r = java.util.Objects.requireNonNull(com.legend.Execution.execute(model, query,
                (com.legend.model.ImportScope) null, runtime, c, com.legend.ExecuteOptions.TDS_TEXT));
        return switch (r) {
            case com.legend.exec.ExecutionResult.TdsText t -> t.text();
            case com.legend.exec.ExecutionResult.Scalar s -> "value:" + s.returnType().typeName() + "[1]\n"
                    + (s.value() == null ? "null" : String.valueOf(s.value()));
            case com.legend.exec.ExecutionResult.Collection col -> {
                StringBuilder sb = new StringBuilder("value:" + col.returnType().typeName() + "[1]");
                for (Object v : col.values()) {
                    sb.append('\n').append(v == null ? "null" : String.valueOf(v));
                }
                yield sb.toString();
            }
            case com.legend.exec.ExecutionResult.Tabular t -> throw new IllegalStateException(
                    "a tabular result under the TDS text option: the wrap missed a relation root");
            case com.legend.exec.ExecutionResult.Graph g -> throw new IllegalStateException(
                    "graph results have no TDS text");
        };
    }

    /**
     * The TDS text plan ({@code TypedQuery.Output.TDS_TEXT}) answers as today's path: its rows are today's rows, and
     * its header is the plan's own typed relation. The header is judged against the plan's relation and not the
     * let-query's text because a let-bound literal's type carries its digits ({@code Decimal(38,2)}) where a declared
     * parameter's does not ({@code Decimal}): each query's header is its own typed relation, and the rows are the
     * judge of the answer.
     */
    public static void assertTdsTextAnswersAsToday(String model, String query, String runtime, Connection today,
            ExecutionPlan plan, Connection planned, java.util.Map<String, ?> values, String what)
            throws java.io.IOException {
        String actual = run(plan, planned, values);
        if (plan.root() instanceof ExecutionPlan.Keyed) {
            // a dynamic pivot's probe plan (2026-10-20): its columns are its keys', named by the database, so the text
            // is judged whole against today's
            String expected = todayTdsText(model, query, runtime, today);
            if (!expected.equals(actual)) {
                throw new AssertionError(what + ": the TDS text\nexpected: " + expected + "\nactual: " + actual);
            }
            return;
        }
        ExecutionPlan.Relation relation = (ExecutionPlan.Relation) ((ExecutionPlan.TextResult) plan.root()).type();
        ExecutionPlan.Relation lets = (ExecutionPlan.Relation) ((ExecutionPlan.TextResult) com.legend.Compiler.query(
                com.legend.Compiler.compileModel(model), query)
                .executionPlan(runtime, com.legend.TypedQuery.Output.TDS_TEXT).root()).type();
        if (relation.equals(lets)) {
            String expected = todayTdsText(model, query, runtime, today);
            if (!expected.equals(actual)) {
                throw new AssertionError(what + ": the TDS text\nexpected: " + expected + "\nactual: " + actual);
            }
            return;
        }
        // the let-bound literal types its column more precisely than the declared parameter (Decimal(38,2) against
        // Decimal; StrictDate against Date), and the TDS text prints by the column's type: the two queries' texts
        // differ by design, so the plan's is judged by its header alone, the rows being the declared type's print
        // that no path with lets spells
        int nl = actual.indexOf('\n');
        String[] header = (nl < 0 ? actual : actual.substring(0, nl)).split(",");
        if (header.length != relation.columns().size()) {
            throw new AssertionError(what + ": the TDS header " + java.util.Arrays.toString(header) + " is not the"
                    + " plan's relation " + relation.columns());
        }
        for (int i = 0; i < header.length; i++) {
            ExecutionPlan.Column c = relation.columns().get(i);
            if (!header[i].endsWith(":" + c.type() + "[1]") && !header[i].endsWith(":" + c.type() + "[0..1]")) {
                throw new AssertionError(what + ": the TDS header column '" + header[i] + "' is not the plan's "
                        + c.name() + ":" + c.type());
            }
        }
    }

    /** A TDS text's rows: the lines after the header. */
    private static List<String> tdsRows(String tds) {
        int nl = tds.indexOf('\n');
        return nl < 0 ? List.of() : List.of(tds.substring(nl + 1).split("\n", -1));
    }
}
