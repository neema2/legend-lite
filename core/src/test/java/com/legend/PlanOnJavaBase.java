package com.legend;

/**
 * The program {@link PlannerRunsOnJavaBaseTest} runs in a JVM limited to
 * {@code java.base}: plan a class query, print the SQL. It names nothing outside
 * core and the JDK, so it loads where only java.base does.
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
        System.out.println("java.sql visible: " + ModuleLayer.boot().findModule("java.sql").isPresent());
        System.out.println(Compiler.plan(model, "x::Firm.all()->filter(f|$f.size > 10)"
                + "->project(~[n: f|$f.name, s: f|$f.size])->groupBy(~[n], ~[t: x|$x.s: y|$y->plus()])"
                + "->sort(~n->ascending())", "x::RT").sql());
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
        System.out.println("postgres: " + Compiler.plan(pg, "#>{x::PG.FIRM}#->filter(r|$r.SIZE > 10)"
                + "->extend(over(~NAME, ~ID->ascending()), ~[rn:{p,w,r|$p->rowNumber($r)}])"
                + "->filter(r|$r.rn == 1)->groupBy(~[NAME], ~[t: r|$r.SIZE : y|$y->plus()])", "x::PgRT").sql());
    }
}
