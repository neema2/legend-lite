// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.SqlExpr;

/**
 * LEGEND-PURE'S SENTENCES OF A RAISE, WORDED IN JAVA AFTER THE RUN (stage B's B1, errors as records: the user's rulings
 * of 2026-11-20 ~16:40, "we should do B to keep the PCT", and ~17:00, the plan carries the error's facts and no words):
 * a raised site's kind and its operands ({@code executionplan.Operand}, the literals the statement holds) worded as
 * legend-pure words them -- byte for byte where every printed operand is a literal (PCT's {@code assertError}s compare
 * legend-pure's text), and a computed operand {@code <computed>} where legend-pure prints a value only the database knew
 * (D1, register S389). Until B1 the edge wrote these sentences into its dialects' SQL where an operand was computed,
 * and recorded them at planning where none was ({@code SiteWords}); both are gone: every statement raises the
 * product's site token, on every door. Each arm cites its upstream line in the D0 inventory's reads
 * ({@code docs/compat-inventory-homework/upstream-reads.txt}).
 */
public final class PureMessages {

    private PureMessages() {
    }

    /** A checked graph's defect cell, {@code CONSTRAINT_UNEVALUABLE}: not a raise but an ANSWER cell the edge's dialects
     *  write as legend-engine's sentence (its milestoning/constraint words). Stage B's B2 moves it to the edge's
     *  output formatting; until then it is planned text, as before. */
    public static SqlExpr defectCell(SqlExpr.RaiseText r) {
        if (r.kind() != com.legend.sql.RaiseKind.CONSTRAINT_UNEVALUABLE
                || !(r.operands().get(0) instanceof SqlExpr.StringLit id)) {
            throw new IllegalStateException("a defect cell is a constraint's, its id a text; this one is " + r.kind());
        }
        return new SqlExpr.StringLit("Unable to evaluate constraint [" + id.value()
                + "]: data not available - check your mappings");
    }

    /** A raised site's sentence: the one catalog's ({@code result.RaiseWords}, the user's ruling of 2026-11-21: lite
     *  says legend-pure's sentence wherever legend-pure has one), the same the run says. */
    public static String worded(com.legend.executionplan.ExecutionPlan.Site site) {
        return com.legend.result.RaiseWords.sentence(site);
    }

    /**
     * LEGEND-PURE'S AND LEGEND-ENGINE'S SENTENCE OF A COMPILE ERROR (phase D's D1.6, W7 of
     * docs/records/d-plan-2026-11-16.md, 2026-11-16): {@code e}'s message as upstream words it, where an upstream
     * client reads it (pure/v1's routes, PCT, the corpus) -- since the one catalog (BE, 2026-11-21) the product's own
     * words too ({@link com.legend.error.CompileKind#sentence}, upstream's sentence). An error with no kind is its own message; the driver's
     * {@code [line:col]} stays in front.
     */
    public static String compile(Throwable e) {
        String message = String.valueOf(e.getMessage());
        for (Throwable t = e; t != null; t = t.getCause()) {
            // by its record's type, never exec's class (the edge's WebAssembly planner holds no java.sql)
            if (t instanceof com.legend.executionplan.ExecutionPlan.RaisedAtSite r && r.site() != null) {
                // a raise of a planned site: legend-pure's sentence, worded from the site's record (stage B's B1,
                // 2026-11-20; until then pure/v1 answered the kind's name, 'Raised: SIZE_NOT_MULTIPLICITY', as an
                // upstream reader had read it since PR1, and the mark and the where never reached the answer -- the
                // golden net, 2026-11-16)
                return worded(r.site());
            }
        }
        if (message.length() > 1 && message.charAt(0) == com.legend.sql.RaiseKind.MARK) {
            // a product raise's carriage (MARK kind) copied off its record, so no site to word: its kind's
            // name, as main's edge answered it -- PRE-EXISTING, kept (B1's audits, 2026-11-20: a residue of the one
            // error model, which carries the record everywhere); the mark is lite's carriage, never upstream's text (the
            // golden net, 2026-11-16; the second mark, before a where, went with BE's item 2). It answers, never
            // throws: a refusal by name is for a site whose operands its kind's sentence lacks (worded)
            return message.substring(1);
        }
        if (e instanceof com.legend.error.MissingParameters m) {
            // legend-engine's parameter validation, its exact join (4.145.0,
            // FunctionParametersParametersValidation.java:52: makeString(","); F11, 2026-11-16)
            // the sentence ends where upstream's does: lite's rest (a value no parameter takes) is the product's
            // sentence alone (error.MissingParameters; the orchestrator, 2026-11-17)
            return "Missing external parameter(s): " + String.join(",", m.missing());
        }
        if (e instanceof com.legend.error.InvalidParameters v) {
            // legend-engine's text of a parameter value its parameter does not take (moved from the product's
            // exec.PlanParameters, J8 of the edge audit, 2026-11-17): the problems in brackets, joined by "; "
            return "Invalid provided parameter(s): [" + String.join("; ", v.problems()) + "]";
        }
        // an error with its record (stage B's BE, 2026-11-21, the one error model): legend-pure's words of its code,
        // inside lite's frames, after its position -- live, or rebuilt from a planned failure's record, the same
        // function. The chain's first lite error decides: one with no record keeps its own words (a wrapper that states
        // only its text is never looked through)
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof com.legend.error.LegendError le) {
                if (le.record() != null) {
                    return upstream(le.record());
                }
                break;
            }
        }
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof com.legend.base.DataSize.TooLarge big) {
                // a size refusal no load wrapped (the base library's own: it has no record)
                return tooLarge(big.path(), big.characters(), big.limitMegabytes());
            }
            if (t instanceof com.legend.testdatagen.TestDataPlanner.NoRowIdentifiers n) {
                // legend-pure's assert, worded from the generator's facts (J7/K12 of the edge audit, 2026-11-17)
                return noRowIdentifiers(n);
            }
            if (t instanceof com.legend.testdatagen.TestDataPlanner.RowIdentifiersRepeated r) {
                // legend-pure's assert, worded from the generator's facts (J7 of the edge audit, 2026-11-18):
                // getRowIdentifiersForTable, TDG:487 (getQualifiedTableName)
                return "Multiple table row identifiers passed for the table: "
                        + com.legend.testdatagen.TestDataPlanner.physical(r.schema(), r.table());
            }
        }
        return message;
    }

    /** legend-pure's and legend-engine's sentence of an error's record (stage B's BE, 2026-11-21, the one error
     *  model): its code in upstream's words where an upstream test or client reads them, inside lite's frames as lite
     *  words them, after its position. A code upstream has no sentence for is worded by lite's catalog, ONE ruled
     *  mapping (Q3 = B, the user, 2026-11-20 ~16:40: compile errors keep lite's words where upstream has none). */
    public static String upstream(com.legend.error.ErrorRecord record) {
        return record.sentence((code, values) -> switch (code) {
            case com.legend.error.CompileKind k -> upstream(k, values);
            // a run's error with no site: the catalog's sentence (a load's refused file legend-engine's, FILE_TOO_LARGE)
            case com.legend.error.ErrorKind k -> k.sentence(values);
            // a failed assert: lite's catalog on every route (the user's Q15 = A, 2026-11-20; item 3, 2026-11-21)
            case com.legend.error.AssertKind k -> k.sentence(values);
            case com.legend.error.FrameKind f -> throw new IllegalStateException("an error's code is never a frame's: "
                    + f);
        });
    }

    /** The name an upstream answer gives an uncaught error's type (pure/v1's {@code "<type>: <message>"}): a parameter
     *  given no value is legend-engine's {@code IllegalArgumentException} (its PlanExecutor; lite's kind,
     *  {@code MissingParameters}, is the product's; the P2 checkpoint audit's M3/A-M4, 2026-11-16); any other error its
     *  own class's name. */
    public static String typeName(Throwable e) {
        // lite's typed refusals of a run's values (MissingParameters, and InvalidParameters since J18, 2026-11-17):
        // legend-engine's clients read an IllegalArgumentException, as they did before either was typed
        if (e instanceof com.legend.error.MissingParameters || e instanceof com.legend.error.InvalidParameters) {
            return "IllegalArgumentException";
        }
        return uncaught(e) ? "IllegalStateException" : e.getClass().getSimpleName();
    }

    /** A compile kind upstream raises at run as an uncaught error, never as a refusal of the text: a TDS row's cell
     *  offset (legend-pure's {@code at}, an {@code IllegalStateException} -- the product's answer until W7 made it a
     *  kind, restored at the edge by the P2 checkpoint audit's A-S2, 2026-11-16). */
    public static boolean uncaught(Throwable e) {
        // the record's own code, framed or not (the BE audit's nit 10, 2026-11-21: kind() is null under a frame)
        return e instanceof com.legend.error.LegendCompileException c && c.record() != null
                && c.record().code() == com.legend.error.CompileKind.ELEMENT_AT_OFFSET;
    }

    /** {@code kind}'s sentence over {@code o}: upstream's words are the kind's own (one catalog, the user's ruling of
     *  2026-11-21; each kind's source cited on {@code error.CompileKind}), so the edge says what the product says. */
    private static String upstream(com.legend.error.CompileKind kind, java.util.List<String> o) {
        return kind.sentence(o);
    }

    /** legend-pure's sentence of a load's file larger than the load admits (J25 of the edge audit, 2026-11-17: moved
     *  from base.DataSize, whose refusal is the product's kind and facts): {@code CsvReader.readCsv}'s inner message, the
     *  text's length in megabytes against the limit, wrapped by its outer "Unable to read the CSV file" (read 2026-11-09
     *  at 5.99.0; the inner exception carries no source information, so "Execution error at ??, " and the info quoted:
     *  legend-pure's PureException.printMessage; the M1 correctness audit's C4, 2026-11-09). */
    static String tooLarge(String path, long characters, int limitMegabytes) {
        // the catalog's one copy of legend-engine's sentence (the BE audit 2's nit 6, 2026-11-21: until then written
        // here a second time): [the file, bytes, characters, limit], the bytes unread by the sentence
        return com.legend.error.ErrorKind.FILE_TOO_LARGE.sentence(java.util.List.of(path, String.valueOf(characters),
                String.valueOf(characters), String.valueOf(limitMegabytes)));
    }

    /** legend-pure's sentence of a test-data generation's starting table no identifier names (J7/K12 of the edge audit,
     *  2026-11-17: moved from testdatagen.TestDataPlanner, whose refusal is the product's kind and facts): its
     *  {@code getRowIdentifiersForTable} assert (TDG:482-518; a view's, TDG:510, :558) -- {@link #unidentified}, then
     *  {@code Example:\n} and the table's first 5 keys written as {@code createTableRowIdentifiers(...)} code. Planning
     *  reads no row, so the example's list is empty (the user's ruling R3, 2026-11-11; register S300). */
    static String noRowIdentifiers(com.legend.testdatagen.TestDataPlanner.NoRowIdentifiers n) {
        return unidentified(n) + "Example:\n"
                + "  meta::relational::testDataGeneration::createTableRowIdentifiers(" + n.database() + ", '"
                + n.schema() + "', '" + n.table() + "', [\n\n  ])\n";
    }

    /** Upstream's no-identifiers text up to its example, the function's and the server routes' error node's
     *  (J7/K12 of the edge audit, 2026-11-17): {@code Row Identifers should be provided for the root table[ of the view
     *  (V)]: <db>.<[schema.]table> [<key>:<type>,...]\n}. */
    static String unidentified(com.legend.testdatagen.TestDataPlanner.NoRowIdentifiers n) {
        String keys = n.keys().stream().map(c -> c.name() + ":" + PlanText.spell(c.dataType()))
                .collect(java.util.stream.Collectors.joining(","));
        return "Row Identifers should be provided for the root table" + (n.view() == null ? "" : " of the view ("
                + n.view() + ")") + ": " + n.database() + "."
                + com.legend.testdatagen.TestDataPlanner.physical(n.schema(), n.table()) + " [" + keys + "]\n";
    }
}
