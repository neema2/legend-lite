// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compatspike;

import com.legend.Compiler;
import com.legend.Execution;
import com.legend.TypedQuery;
import com.legend.model.ConnectionDefinition;
import com.legend.result.Collector;
import com.legend.result.Output;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * H5, SCRATCH: measurement (c) -- after the ENGINE translator's rewrite, do a compile error and a raise still point at
 * the user's ORIGINAL source? Planted cases over one small H2 model, each run with the translator off (the core's Pure
 * meaning alone) and on; every outcome is printed for the record, nothing is asserted but that the probe ran.
 * Run: {@code bazel test //spec:h5_translator_probe --test_output=all}.
 */
class TranslatorProbeTest {

    static final String MODEL = String.join("\n",
            "###Relational",                                                                         // line 1
            "Database s::DB ( Table T ( ID INTEGER PRIMARY KEY, NAME VARCHAR(20) NOT NULL ) )",      // 2
            "###Connection",                                                                         // 3
            "RelationalDatabaseConnection s::Conn",                                                  // 4
            "{",                                                                                     // 5
            "    store: s::DB;",                                                                     // 6
            "    type: H2;",                                                                         // 7
            "    specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID,NAME\\n1,Credit\\n2,FX\\n'; };", // 8
            "    auth: DefaultH2;",                                                                  // 9
            "}",                                                                                     // 10
            "###Runtime",                                                                            // 11
            "Runtime s::RT { mappings: []; connections: [ s::DB: [ c1: s::Conn ] ]; }",               // 12
            "###Pure",                                                                               // 13
            "function s::first3(x: String[1]): String[1]",                                           // 14
            "{",                                                                                     // 15
            "    $x->substring(1, 3)",                                                               // 16
            "}",                                                                                     // 17
            "function s::bad(x: Integer[1]): String[1]",                                             // 18
            "{",                                                                                     // 19
            "    $x->substring(1, 3)",                                                               // 20
            "}",                                                                                     // 21
            "function s::raising(x: String[1]): String[1]",                                          // 22
            "{",                                                                                     // 23
            "    $x->substring(1, $x->parseInteger())",                                              // 24
            "}",                                                                                     // 25
            "");

    static final String[][] QUERIES = {
        // a rewritten call in the query, valid: the answer moves to legend-engine's (H2: 'Cre' at start 1, length 3)
        {"q.ok", "|#>{s::DB.T}#->extend(~r: r|$r.NAME->substring(1, 3))->select(~r)->sort(~r->ascending())->from(s::RT)"},
        // a compile error AT a rewritten call (an Integer receiver)
        {"q.typeError", "|#>{s::DB.T}#->extend(~r: r|$r.ID->substring(1, 3))->select(~r)->from(s::RT)"},
        // a raise INSIDE an argument of a rewritten call (rem by zero)
        {"q.raiseInArgument", "|#>{s::DB.T}#->extend(~r: r|$r.NAME->substring(1, rem($r.ID, 0)))->select(~r)->from(s::RT)"},
        // a RUN-TIME raise inside an argument of a rewritten call (parseInteger of 'Credit')
        {"q.runRaiseInArgument", "|#>{s::DB.T}#->extend(~r: r|$r.NAME->substring(1, $r.NAME->parseInteger()))->select(~r)->from(s::RT)"},
        // a run-time raise over a rewritten call's result
        {"q.runRaiseOverResult", "|#>{s::DB.T}#->extend(~r: r|$r.NAME->substring(1, 3)->parseInteger())->select(~r)->from(s::RT)"},
        // a run-time raise inside a model function's rewritten body
        {"q.runRaiseInModelFunction", "|#>{s::DB.T}#->extend(~r: r|s::raising($r.NAME))->select(~r)->from(s::RT)"},
        // indexOf rewritten: legend-engine's 1-based locate
        {"q.indexOf", "|#>{s::DB.T}#->extend(~r: r|$r.NAME->indexOf('e'))->select(~r)->sort(~r->ascending())->from(s::RT)"},
        // a model function reached from the query (EVERYWHERE rewrites its body; QUERIES does not reach it)
        {"q.modelFunction", "|#>{s::DB.T}#->extend(~r: r|s::first3($r.NAME))->select(~r)->sort(~r->ascending())->from(s::RT)"},
        // a compile error inside a model function's rewritten body
        {"q.modelFunctionTypeError", "|#>{s::DB.T}#->extend(~r: r|s::bad($r.ID))->select(~r)->from(s::RT)"},
    };

    @Test
    void probe() throws Exception {
        List<String> lines = new ArrayList<>();
        for (String mode : List.of("off", "everywhere")) {
            Compiler.ModelSource src = new Compiler.ModelSource("probe.pure", MODEL);
            List<Compiler.ModelSource> sources = new ArrayList<>(List.of(src));
            ConnectionDefinition h2 = new ConnectionDefinition("s::Conn", "s::DB",
                    com.legend.model.ConnectionDefinition.DatabaseType.H2,
                    new com.legend.model.ConnectionSpecification.InMemory(),
                    new com.legend.model.AuthenticationSpec.NoAuth());
            boolean on = mode.equals("everywhere");
            if (on) {
                sources.add(EngineTranslator.library(h2));
            }
            long t0 = System.nanoTime();
            Compiler.ParsedModule parsed = Compiler.parseSources(sources);
            EngineTranslator.Report report = new EngineTranslator.Report();
            if (on) {
                parsed = EngineTranslator.translate(parsed, EngineTranslator.Scope.EVERYWHERE, report);
            }
            Compiler.BuiltModule built = Compiler.buildModule(parsed.model());
            lines.add(mode + "\tmodel built ms=" + (System.nanoTime() - t0) / 1_000_000 + " walls=" + built.walls()
                    + " " + report.summary());
            for (String[] q : QUERIES) {
                String out;
                try {
                    com.legend.protocol.spec.ValueSpecification vs = Compiler.parseQuery(q[1]);
                    if (on) {
                        vs = EngineTranslator.translateQuery(vs, report);
                    }
                    TypedQuery typed = Compiler.query(built.context(), vs);
                    if (q[0].startsWith("q.runRaise")) {
                        // the plan's raise sites: where each says it is
                        var plan = typed.projectPlan(null);
                        lines.add(mode + "\t" + q[0] + "\tSITES\t" + plan.answerSql().sites()
                                + " SQLCHARS=" + plan.answerSql().statement().length());
                    }
                    Collector rows = new Collector();
                    Execution.runOnProjectDatabases(typed, null, new Output.Rows(rows));
                    out = "200\t" + rows.collected().rows();
                } catch (com.legend.error.LegendCompileException e) {
                    out = "COMPILE\t" + e.getClass().getSimpleName() + " phase=" + e.phase() + " position="
                            + e.position() + " element=" + e.element() + " :: " + oneLine(e.getMessage());
                } catch (Throwable e) {
                    out = "ERR\t" + e.getClass().getSimpleName() + " :: " + oneLine(e.getMessage());
                }
                lines.add(mode + "\t" + q[0] + "\t" + out);
            }
        }
        // DEBUG: H3's path exactly -- one text, compileModel(String), the compat FQN written in the query
        String lib = EngineTranslator.library(new ConnectionDefinition("s::Conn", "s::DB",
                com.legend.model.ConnectionDefinition.DatabaseType.H2,
                new com.legend.model.ConnectionSpecification.InMemory(),
                new com.legend.model.AuthenticationSpec.NoAuth())).text();
        for (String path : List.of("compileModelText", "parseSourcesBuildModel", "parseSourcesBuildModule")) {
            String out;
            try {
                com.legend.compiler.element.ModelContext ctx = switch (path) {
                    case "compileModelText" -> Compiler.compileModel(MODEL + "\n" + lib);
                    case "parseSourcesBuildModel" -> Compiler.buildModel(Compiler.parseSources(List.of(
                            new Compiler.ModelSource("probe.pure", MODEL), new Compiler.ModelSource("lib.pure", lib))).model());
                    default -> Compiler.buildModule(Compiler.parseSources(List.of(
                            new Compiler.ModelSource("probe.pure", MODEL), new Compiler.ModelSource("lib.pure", lib))).model()).context();
                };
                TypedQuery typed = Compiler.query(ctx, "|#>{s::DB.T}#->extend(~r: r|$r.NAME->meta::legend::compat::engine::substring(1, 3))->select(~r)->sort(~r->ascending())->from(s::RT)");
                Collector rows = new Collector();
                Execution.runOnProjectDatabases(typed, null, new Output.Rows(rows));
                out = "200\t" + rows.collected().rows();
            } catch (Throwable e) {
                out = "ERR\t" + e.getClass().getSimpleName() + " :: " + oneLine(e.getMessage());
            }
            lines.add("debug\t" + path + "\t" + out);
        }
        {
            var a = Compiler.parseQuery("|$x->substring(1, 3)");
            var b = Compiler.parseQuery("|$x->meta::legend::compat::engine::substring(1, 3)");
            lines.add("debug\trenamed\t" + EngineTranslator.translateQuery(a, new EngineTranslator.Report()));
            lines.add("debug\twritten\t" + b);
        }
        System.out.println("=== H5 probe (c) ===");
        lines.forEach(System.out::println);
        System.out.println("=== end ===");
    }

    private static String oneLine(String s) {
        return String.valueOf(s).replace('\n', ' ');
    }
}
