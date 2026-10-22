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
        // every dialect plans on java.base too (2026-10-01 W5.5/P1, the Postgres dialect, the browser planner's path;
        // every database the platform executes on since the dialect program's Phase 6, 2026-10-16, a relational
        // connection declares since main's merge, 2026-10-22): a runtime declaring each database, the same query
        for (com.legend.model.ConnectionDefinition.DatabaseType type : com.legend.database.Databases.relational()) {
            String declared = """
                    ###Relational
                    Database x::DB ( Table FIRM (ID INTEGER PRIMARY KEY, NAME VARCHAR(32), SIZE INTEGER) )
                    ###Connection
                    RelationalDatabaseConnection x::Conn { store: x::DB; type: %s;
                      specification: DuckDB { }; auth: Test; }
                    ###Runtime
                    Runtime x::DbRT { mappings: []; connections: [ x::DB: [ c1: x::Conn ] ]; }
                    """.formatted(type.name());
            String planned = Compiler.query(Compiler.compileModel(declared), "#>{x::DB.FIRM}#->filter(r|$r.SIZE > 10)"
                    + "->extend(over(~NAME, ~ID->ascending()), ~[rn:{p,w,r|$p->rowNumber($r)}])"
                    + "->filter(r|$r.rn == 1)->groupBy(~[NAME], ~[t: r|$r.SIZE : y|$y->plus()])").plan("x::DbRT").sql();
            System.out.println(type + ": " + planned.replace('\n', ' '));
            check(planned.toUpperCase(java.util.Locale.ROOT).startsWith("SELECT"), "no " + type + " SQL planned: "
                    + planned);
            // the table read as the dialect stores the name (Postgres folds a bare one, decision D8), and the window's
            // filter a QUALIFY or the select that wraps it
            String table = com.legend.database.Databases.dialect(type).storedName("FIRM", false);
            check(planned.contains("FROM " + table), "no " + type + " read of " + table + ": " + planned);
            check(planned.contains("QUALIFY") || planned.contains("qualify_src"), "no " + type + " QUALIFY: " + planned);
        }
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
