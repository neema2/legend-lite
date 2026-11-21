// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.SqlExpr;

/**
 * LEGEND-PURE'S SENTENCES OF A RAISE, WORDED IN JAVA AFTER THE RUN (stage B's B1, errors as records: the user's rulings
 * of 2026-11-20 ~16:40, "we should do B to keep the PCT", and ~17:00, the plan carries the error's facts and no words):
 * a raised site's kind and its operands ({@code executionplan.Operand}, the literals the statement holds) worded as
 * legend-pure words them -- byte for byte where every printed operand is a literal (PCT's {@code assertError}s compare
 * legend-pure's text), and a computed operand {@link #WITHHELD} where legend-pure prints a value only the database knew
 * (D1, register S389). Until B1 the edge wrote these sentences into its dialects' SQL where an operand was computed,
 * and recorded them at planning where none was ({@code SiteWords}); both are gone: every statement raises the
 * product's site token, on every door. Each arm cites its upstream line in the D0 inventory's reads
 * ({@code docs/compat-inventory-homework/upstream-reads.txt}).
 */
public final class PureMessages {

    private PureMessages() {
    }

    /** What stands where legend-pure's sentence prints an operand whose value the plan does not carry (the database
     *  computed it): register S389, the user's D1 of 2026-11-20 ~17:00. */
    public static final String WITHHELD = "<computed>";

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

    /** legend-pure's sentence of a raised site, from its record ({@code executionplan.ExecutionPlan.Site}). */
    public static String worded(com.legend.executionplan.ExecutionPlan.Site site) {
        return worded(site.kind(), site.operands());
    }

    /** legend-pure's sentence of a site of {@code kind} built from {@code o}, each operand at the place legend-pure's
     *  sentence prints it. */
    public static String worded(com.legend.sql.RaiseKind kind, java.util.List<com.legend.executionplan.Operand> o) {
        return switch (kind) {
            case USER_FAIL -> throw new IllegalStateException("a user's fail raises its own message, never a site");
            // rem's and quotient's, the dividend its operand: 'Cannot divide 5 by zero' (legend-pure's
            // CompiledSupport:1485); Integer / Integer's, no operand: the interpreter's BigDecimal 'Division by zero'
            case DIVIDE_BY_ZERO -> o.isEmpty() ? "Division by zero" : "Cannot divide " + str(o, 0) + " by zero";
            // engine's bitShiftLeft/Right.pure
            case SHIFT_OUT_OF_RANGE -> "Unsupported number of bits to shift - max bits allowed is 62";
            // [the part's label, the date]: legend-pure's 'Cannot get <part> for <date>'
            case DATE_PART_ABSENT -> "Cannot get " + str(o, 0) + " for " + date(o, 1);
            // [the offset, the size]: legend-pure's at() out of bounds
            case INDEX_OUT_OF_BOUNDS -> "The system is trying to get an element at offset " + str(o, 0)
                    + " where the collection is of size " + str(o, 1);
            // lite's own sentence (no upstream text: the D0 inventory's 1.6), kept for the compatibility road
            case INSERT_OUT_OF_BOUNDS -> "add(set, index, value): index out of bounds";
            // [the value]: legend-pure's 'Unable to compute sqrt of <x>', <x> as Pure prints a Float (K2)
            case SQRT_OF_NEGATIVE -> "Unable to compute sqrt of " + asFloat(o, 0);
            // [the bound] (a size known empty) or [the bound, the size]
            case SIZE_NOT_MULTIPLICITY -> o.size() == 1
                    ? "Cannot cast a collection of size 0 to multiplicity " + str(o, 0)
                    : "Cannot cast a collection of size " + str(o, 1) + " to multiplicity " + str(o, 0);
            // [low, high]: legend-pure's slice
            case SLICE_BOUNDS_INVERTED -> "The low bound (" + str(o, 0) + ") can't be higher than the high bound ("
                    + str(o, 1) + ") in a slice operation";
            case DATE_COMPONENT_INVALID -> dateComponent(o);
            // lite's own refusals (S138, S181, the CSV width): no upstream text; the compatibility road writes the
            // product's former sentence, its parts the operands, as main's edge did (PCT's expected failures pin them)
            case VALUE_NOT_HELD, FRACTION_NOT_KEPT, CSV_TOO_WIDE -> {
                StringBuilder parts = new StringBuilder();
                for (int i = 0; i < o.size(); i++) {
                    parts.append(str(o, i));
                }
                yield parts.toString();
            }
            // [the source type's name, the target's]
            case CAST_IMPOSSIBLE -> "Cast exception: " + str(o, 0) + " cannot be cast to " + str(o, 1);
            // [the value's run-time type's path, the needed type's]
            case INSTANCE_NOT_OF -> "Cast exception: " + str(o, 0) + " is not a " + str(o, 1);
            // legend-pure's Match.java, a value no branch takes; the resolver's run-time dispatch, its arms named
            case MATCH_FAILURE -> str(o, 1).isEmpty() ? "Match failure: " + str(o, 0) + " matches no branch"
                    : "Match failure: no arm of [" + str(o, 1) + "] accepts the row's run-time type";
            case RANGE_STEP_ZERO -> "range step must not be 0";
            // legend-pure's rows/_range assert, verbatim (#81's frame guard)
            case WINDOW_FRAME_INVERTED -> "Invalid window frame boundary - lower bound of window"
                    + " frame cannot be greater than the upper bound!";
            // [the start, the end, the length]: legend-pure's String.substring raise, Java's own words (S383)
            case SUBSTRING_OUT_OF_RANGE -> "Range [" + str(o, 0) + ", " + str(o, 1) + ") out of bounds for length "
                    + str(o, 2);
            // lite's own sentence (S170), kept for the compatibility road
            case WINDOW_FRAME_OFFSET_INVALID -> "Invalid window frame boundary - a window frame"
                    + " bound written -(n) is n before the current row and any other n after it, so a computed n must"
                    + " be neither negative nor empty (docs/SEMANTICS_REGISTER.md S170)";
            // [the native's name]: lite's own sentence, kept for the compatibility road
            case LIST_LENGTHS_DIFFER -> str(o, 0) + ": the two value lists differ in length";
            // [the constraint's id]: the engine's sentence (a raise of it; its defect cell is defectCell's)
            case CONSTRAINT_UNEVALUABLE -> "Unable to evaluate constraint [" + str(o, 0)
                    + "]: data not available - check your mappings";
            // DuckDB's own words, which the reference database's other dialects raise (the D0 inventory's K3)
            case MAP_KEYS_MISALIGNED -> "The map key list does not align with the map value list.";
            case MAP_KEY_NULL -> "Map keys can not be NULL.";
            case MAP_KEYS_NOT_UNIQUE -> "Map keys must be unique.";
            // [the text]: DuckDB's cast errors
            case TEXT_NOT_INTEGER -> "Could not convert string '" + str(o, 0) + "' to INT64";
            case TEXT_NOT_FLOAT -> "Could not convert string '" + str(o, 0) + "' to DOUBLE";
            case TEXT_NOT_DATE -> "Could not parse '" + str(o, 0) + "' as a date";
        };
    }

    /** date(...)'s component out of range: [the component, its literal] as legend-pure writes it ('Invalid month: 13', a
     *  fractional second's plain digits), [day, year, month, day] its attempted date ('Invalid day: 2016-12-32', the
     *  month unpadded). */
    private static String dateComponent(java.util.List<com.legend.executionplan.Operand> o) {
        if (o.size() == 4) {
            return "Invalid day: " + str(o, 1) + "-" + str(o, 2) + "-" + str(o, 3);
        }
        String head = "Invalid " + str(o, 0) + ": ";
        return switch (o.get(1)) {
            case com.legend.executionplan.Operand.Flt f -> head + java.math.BigDecimal.valueOf(f.value()).toPlainString();
            case com.legend.executionplan.Operand.Dec d -> head + d.value().toPlainString();
            case com.legend.executionplan.Operand.Text t -> head + t.value();
            case com.legend.executionplan.Operand.Int n -> head + n.value();
            case com.legend.executionplan.Operand.Bool b -> head + b.value();
            case com.legend.executionplan.Operand.Day d -> head + d.iso();
            case com.legend.executionplan.Operand.Instant t -> head + t.iso();
            case com.legend.executionplan.Operand.Computed c -> head + WITHHELD;
        };
    }

    /** Operand {@code i} as legend-pure's sentence prints it: a text itself, an Integer's digits, a date's ISO day, a
     *  Float as legend-pure prints one, a Decimal's plain digits, a Boolean's word, a date-time as the statement holds
     *  it; a computed one {@link #WITHHELD}. */
    private static String str(java.util.List<com.legend.executionplan.Operand> o, int i) {
        if (i >= o.size()) {
            throw new IllegalStateException("a raise site built from " + o.size() + " operands, and its sentence prints"
                    + " operand " + i);
        }
        return switch (o.get(i)) {
            case com.legend.executionplan.Operand.Text x -> x.value();
            case com.legend.executionplan.Operand.Int n -> Long.toString(n.value());
            case com.legend.executionplan.Operand.Day d -> d.iso();
            case com.legend.executionplan.Operand.Flt f -> Presenter.legendPure(f.value(), "Float");
            case com.legend.executionplan.Operand.Dec d -> d.value().toPlainString();
            case com.legend.executionplan.Operand.Bool b -> Boolean.toString(b.value());
            case com.legend.executionplan.Operand.Instant ts -> ts.iso();
            case com.legend.executionplan.Operand.Computed c -> WITHHELD;
        };
    }

    /** Operand {@code i}, a date, as legend-pure's {@code DateFormat.append} prints one: a date LITERAL (the lowering
     *  hands its text as written) at its own precision, {@code +0000} from a minute on ({@link EdgePureWords#pureText});
     *  any other as {@link #str} prints it. */
    private static String date(java.util.List<com.legend.executionplan.Operand> o, int i) {
        return i < o.size() && o.get(i) instanceof com.legend.executionplan.Operand.Text x
                ? EdgePureWords.pureText(com.legend.values.PureDateLiteral.parse(x.value())) : str(o, i);
    }

    /** Operand {@code i} as legend-pure prints a Float: a numeric literal's double; a computed one withheld. */
    private static String asFloat(java.util.List<com.legend.executionplan.Operand> o, int i) {
        return switch (o.get(i)) {
            case com.legend.executionplan.Operand.Flt f -> Presenter.legendPure(f.value(), "Float");
            case com.legend.executionplan.Operand.Int n -> Presenter.legendPure((double) n.value(), "Float");
            case com.legend.executionplan.Operand.Dec d -> Presenter.legendPure(d.value().doubleValue(), "Float");
            case com.legend.executionplan.Operand.Text t -> t.value();
            case com.legend.executionplan.Operand.Bool b -> Boolean.toString(b.value());
            case com.legend.executionplan.Operand.Day d -> d.iso();
            case com.legend.executionplan.Operand.Instant t -> t.iso();
            case com.legend.executionplan.Operand.Computed c -> WITHHELD;
        };
    }

    /**
     * LEGEND-PURE'S AND LEGEND-ENGINE'S SENTENCE OF A COMPILE ERROR (phase D's D1.6, W7 of
     * docs/records/d-plan-2026-11-16.md, 2026-11-16): {@code e}'s message as upstream words it, where an upstream
     * client reads it (pure/v1's routes, PCT, the corpus) -- the product words its kind its own way
     * ({@link com.legend.error.CompileKind#sentence}). An error with no kind is its own message; the driver's
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
            // a product raise's carriage (MARK kind [MARK where]) copied off its record, so no site to word: its kind's
            // name, as main's edge answered it -- PRE-EXISTING, kept (B1's audits, 2026-11-20: a residue of the one
            // error model, which carries the record everywhere); the mark and the where are lite's carriage, never
            // upstream's text (the golden net, 2026-11-16). It answers, never throws: a refusal by name is for a site
            // whose operands its kind's sentence lacks (worded), a defect of the record
            int end = message.indexOf(com.legend.sql.RaiseKind.MARK, 1);
            return end < 0 ? message.substring(1) : message.substring(1, end);
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
            case com.legend.error.ErrorKind k when k == com.legend.error.ErrorKind.FILE_TOO_LARGE ->
                    tooLarge(values.get(0), Long.parseLong(values.get(2)), Integer.parseInt(values.get(3)));
            // a run's error with no site, upstream's words none: lite's (the ruled mapping)
            case com.legend.error.ErrorKind k -> k.sentence(values);
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
        return "Unable to read the CSV file '" + path + "' Execution error at ??, \"File is too large, file was "
                + String.format(java.util.Locale.ROOT, "%.2f", characters * 1.0 / (1024 * 1024L)) + " Mb, limit is "
                + limitMegabytes + " Mb\"";
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
