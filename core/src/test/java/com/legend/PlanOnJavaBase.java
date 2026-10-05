package com.legend;

/**
 * THE PLANNER RUNS ON java.base ALONE: parse, type, resolve, lower and render a query in a JVM whose only module is
 * {@code java.base} ({@code java.sql} absent). That is what a WASM or slim-runtime planner needs, and it is invisible
 * otherwise: every build and test passes while it is broken. It was broken by ONE line, a
 * {@code catch (java.sql.SQLException)} in {@code Compiler}, which the verifier resolves when the class links. Checked by
 * RUNNING the planner, not by scanning source for JDBC names.
 *
 * <p>A test in its own right (Bazel workplan P3-19): {@code //core:planner_on_java_base_test} runs this program with
 * {@code --limit-modules=java.base}, and it exits non-zero when a check fails. No test starts a JVM of itself.
 */
public final class PlanOnJavaBase {

    private PlanOnJavaBase() {
    }

    public static void main(String[] args) {
        String model = """
                Class x::Firm { name: String[1]; size: Integer[1]; }
                ###Relational
                Database x::DB ( Table FIRM (ID INTEGER PRIMARY KEY, NAME VARCHAR(32), SIZE INTEGER) )
                ###Mapping
                Mapping x::M ( *x::Firm: Relational { ~mainTable [x::DB] FIRM
                    name: [x::DB] FIRM.NAME, size: [x::DB] FIRM.SIZE } )
                ###Connection
                RelationalDatabaseConnection x::DBDuckDB { store: x::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
                ###Runtime
                Runtime x::RT { mappings: [x::M]; connections: [ x::DB: [ c0: x::DBDuckDB ] ]; }
                """;
        check(ModuleLayer.boot().findModule("java.sql").isEmpty(), "java.sql is visible: run with --limit-modules=java.base");
        String sql = Compiler.query(Compiler.compileModel(model), "x::Firm.all()->filter(f|$f.size > 10)"
                + "->project(~[n: f|$f.name, s: f|$f.size])->groupBy(~[n], ~[t: x|$x.s: y|$y->plus()])"
                + "->sort(~n->ascending())").plan("x::RT").sql();
        System.out.println(sql);
        check(sql.contains("SELECT") && sql.contains("GROUP BY"), "no SQL planned: " + sql);
        // the Postgres dialect plans on java.base too (2026-10-01 W5.5/P1 Postgres
        // dialect): a runtime declaring Postgres, the browser planner's path
        String pg = """
                ###Relational
                Database x::PG ( Table FIRM (ID INTEGER PRIMARY KEY, NAME VARCHAR(32), SIZE INTEGER) )
                ###Connection
                RelationalDatabaseConnection x::PgConn { store: x::PG; type: Postgres;
                  specification: DuckDB { }; auth: Test; }
                ###Runtime
                Runtime x::PgRT { mappings: []; connections: [ x::PG: [ c1: x::PgConn ] ]; }
                """;
        String postgres = Compiler.query(Compiler.compileModel(pg), "#>{x::PG.FIRM}#->filter(r|$r.SIZE > 10)"
                + "->extend(over(~NAME, ~ID->ascending()), ~[rn:{p,w,r|$p->rowNumber($r)}])"
                + "->filter(r|$r.rn == 1)->groupBy(~[NAME], ~[t: r|$r.SIZE : y|$y->plus()])").plan("x::PgRT").sql();
        System.out.println("postgres: " + postgres);
        // quoted identifiers, and QUALIFY as a wrapping select
        check(postgres.startsWith("SELECT") && postgres.contains("FROM \"FIRM\"") && postgres.contains("\"qualify_src\""),
                "no Postgres SQL planned: " + postgres);
        System.out.println("the planner runs on java.base alone");
    }

    /** A failed check: said, and the run exits non-zero (the test's verdict). */
    private static void check(boolean ok, String failure) {
        if (!ok) {
            System.err.println("FAIL: " + failure);
            System.exit(1);
        }
    }
}
