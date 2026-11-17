// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.SqlExpr;

/**
 * LEGEND-PURE'S SENTENCES OF A RAISE, IN SQL (phase D's D1.1, the user's ruling of 2026-11-10: "Pure's full sentence
 * with the value is spelled at the edge, for PCT and pure/v1"): the message the edge's dialects write for a raise of
 * each kind, built from its typed operands exactly as the product's lowering built it until its site moved here
 * (byte for byte: PCT's {@code assertError}s compare legend-pure's text). Each arm cites its upstream line in the D0
 * inventory's reads ({@code docs/compat-inventory-homework/upstream-reads.txt}). A kind whose sites have not moved
 * yet is refused by name: nothing raises it.
 */
public final class PureMessages {

    private PureMessages() {
    }

    /** legend-pure's message of {@code r}, as SQL the edge's dialect writes. */
    public static SqlExpr sentence(SqlExpr.RaiseText r) {
        return switch (r.kind()) {
            // the user's own message, as the product writes it
            case USER_FAIL -> r.operands().get(0);
            // rem's and quotient's, the dividend its operand: 'Cannot divide 5 by zero' (legend-pure's
            // CompiledSupport:1485); Integer / Integer's, no operand: the interpreter's BigDecimal 'Division by zero'
            // (its NumericAccumulator; the D0 inventory's 1.2)
            case DIVIDE_BY_ZERO -> r.operands().isEmpty() ? new SqlExpr.StringLit("Division by zero")
                    : cat(new SqlExpr.StringLit("Cannot divide "), str(r.operands().get(0)),
                            new SqlExpr.StringLit(" by zero"));
            // engine's bitShiftLeft/Right.pure
            case SHIFT_OUT_OF_RANGE -> new SqlExpr.StringLit("Unsupported number of bits to shift - max bits allowed is 62");
            // [the part's label, the date]: legend-pure's 'Cannot get <part> for <date>'
            case DATE_PART_ABSENT -> cat(new SqlExpr.StringLit("Cannot get " + text(r, 0) + " for "),
                    str(r.operands().get(1)));
            // [the offset, the size]: legend-pure's at() out of bounds
            case INDEX_OUT_OF_BOUNDS -> cat(new SqlExpr.StringLit("The system is trying to get an element at offset "),
                    str(r.operands().get(0)), new SqlExpr.StringLit(" where the collection is of size "),
                    str(r.operands().get(1)));
            // lite's own sentence until D1.1 (no upstream text: the D0 inventory's 1.6), kept for the compatibility road
            case INSERT_OUT_OF_BOUNDS -> new SqlExpr.StringLit("add(set, index, value): index out of bounds");
            // [the value]: legend-pure's 'Unable to compute sqrt of <x>', <x> as Pure prints a Float (K2)
            case SQRT_OF_NEGATIVE -> cat(new SqlExpr.StringLit("Unable to compute sqrt of "),
                    SqlExpr.Call.of(com.legend.sql.SqlFn.FLOAT_SHORTEST_TEXT, r.operands().get(0)));
            // [the bound] (a size known empty) or [the bound, the size]: 'Cannot cast a collection of size <n> to
            // multiplicity <bound>'
            case SIZE_NOT_MULTIPLICITY -> r.operands().size() == 1
                    ? new SqlExpr.StringLit("Cannot cast a collection of size 0 to multiplicity " + text(r, 0))
                    : new SqlExpr.Call(com.legend.sql.SqlFn.CONCAT, java.util.List.of(
                            new SqlExpr.StringLit("Cannot cast a collection of size "), str(r.operands().get(1)),
                            new SqlExpr.StringLit(" to multiplicity " + text(r, 0))));
            // [low, high]: legend-pure's slice
            case SLICE_BOUNDS_INVERTED -> SqlExpr.Call.of(com.legend.sql.SqlFn.CONCAT,
                    SqlExpr.Call.of(com.legend.sql.SqlFn.CONCAT,
                            SqlExpr.Call.of(com.legend.sql.SqlFn.CONCAT, new SqlExpr.StringLit("The low bound ("),
                                    str(r.operands().get(0))),
                            new SqlExpr.StringLit(") can't be higher than the high bound (")),
                    SqlExpr.Call.of(com.legend.sql.SqlFn.CONCAT, str(r.operands().get(1)),
                            new SqlExpr.StringLit(") in a slice operation")));
            case DATE_COMPONENT_INVALID -> dateComponent(r);
            // lite's own refusals (S138, S181, the CSV width): no upstream text; the compatibility road writes the
            // product's former sentence, its parts the operands (a text written as it is, a value as its text)
            case VALUE_NOT_HELD, FRACTION_NOT_KEPT, CSV_TOO_WIDE -> parts(r);
            // [the source type's name, the target's]
            case CAST_IMPOSSIBLE -> new SqlExpr.StringLit("Cast exception: " + text(r, 0) + " cannot be cast to "
                    + text(r, 1));
            // [the value's run-time type's path, the needed type's] (W5: the resolver's chain dispatch, until then a
            // fail of this sentence in the product)
            case INSTANCE_NOT_OF -> new SqlExpr.StringLit("Cast exception: " + text(r, 0) + " is not a " + text(r, 1));
            // legend-pure's Match.java, a value no branch takes ('Match failure: Integer[1] matches no branch'); the
            // resolver's run-time dispatch over a row, its arms named (W7, 2026-11-16: until then the product's fail text)
            case MATCH_FAILURE -> new SqlExpr.StringLit(text(r, 1).isEmpty()
                    ? "Match failure: " + text(r, 0) + " matches no branch"
                    : "Match failure: no arm of [" + text(r, 1) + "] accepts the row's run-time type");
            case RANGE_STEP_ZERO -> new SqlExpr.StringLit("range step must not be 0");
            // legend-pure's rows/_range assert, verbatim (#81's frame guard, merged 2026-11-14)
            case WINDOW_FRAME_INVERTED -> new SqlExpr.StringLit("Invalid window frame boundary - lower bound of window"
                    + " frame cannot be greater than the upper bound!");
            // lite's own sentence (S170: a bound's direction read from its shape), kept for the compatibility road
            case WINDOW_FRAME_OFFSET_INVALID -> new SqlExpr.StringLit("Invalid window frame boundary - a window frame"
                    + " bound written -(n) is n before the current row and any other n after it, so a computed n must"
                    + " be neither negative nor empty (docs/SEMANTICS_REGISTER.md S170)");
            // [the native's name]: lite's own sentence, kept for the compatibility road
            case LIST_LENGTHS_DIFFER -> new SqlExpr.StringLit(text(r, 0) + ": the two value lists differ in length");
            // [the constraint's id]: the engine's milestoning/constraint sentence, in a checked graph's defect row
            case CONSTRAINT_UNEVALUABLE -> new SqlExpr.StringLit("Unable to evaluate constraint [" + text(r, 0)
                    + "]: data not available - check your mappings");
            // DuckDB's own words, which the reference database's other dialects raise (the D0 inventory's K3)
            case MAP_KEYS_MISALIGNED -> new SqlExpr.StringLit("The map key list does not align with the map value list.");
            case MAP_KEY_NULL -> new SqlExpr.StringLit("Map keys can not be NULL.");
            case MAP_KEYS_NOT_UNIQUE -> new SqlExpr.StringLit("Map keys must be unique.");
            // [the text]: DuckDB's cast errors
            case TEXT_NOT_INTEGER -> SqlExpr.Call.of(com.legend.sql.SqlFn.CONCAT,
                    new SqlExpr.StringLit("Could not convert string '"), r.operands().get(0),
                    new SqlExpr.StringLit("' to INT64"));
            case TEXT_NOT_FLOAT -> SqlExpr.Call.of(com.legend.sql.SqlFn.CONCAT,
                    new SqlExpr.StringLit("Could not convert string '"), r.operands().get(0),
                    new SqlExpr.StringLit("' to DOUBLE"));
            case TEXT_NOT_DATE -> SqlExpr.Call.of(com.legend.sql.SqlFn.CONCAT, new SqlExpr.StringLit("Could not parse '"),
                    r.operands().get(0), new SqlExpr.StringLit("' as a date"));
        };
    }

    /**
     * Whether the edge words {@code r}'s upstream sentence AT PLANNING (phase D's Phase 2, raises as SITES, 2026-11-16):
     * a raise (not a checked graph's defect text, not a user's {@code fail}) whose operands are all literals the
     * sentence prints exactly as the SQL sentence did ({@link #words}). Any other is the INTERIM: its sentence stays in
     * SQL ({@link #sentence}), no site, until the user rules on a computed operand (the design note's §5.3).
     */
    public static boolean wordable(SqlExpr.RaiseText r) {
        return r.asData() == null && r.kind() != com.legend.sql.RaiseKind.USER_FAIL
                && wordsOrNull(r.kind(), r.operands()) != null;
    }

    /** legend-pure's sentence of a site of {@code kind} raised from {@code operands}, worded in Java at planning, byte
     *  for byte as {@link #sentence} spells it in SQL; an operand it cannot word refused by name (the edge's dialect
     *  writes such a site's sentence in SQL instead, {@link #wordable}). */
    public static String words(com.legend.sql.RaiseKind kind, java.util.List<SqlExpr> operands) {
        String words = wordsOrNull(kind, operands);
        if (words == null) {
            throw new IllegalStateException("legend-pure's sentence of a " + kind + " site is worded at planning from"
                    + " literal operands, and this site's are " + operands + ": the edge's dialect writes it in SQL");
        }
        return words;
    }

    /** {@link #words}, or null where an operand is not a literal printed exactly. Each arm mirrors {@link #sentence}'s,
     *  its operand at the same place: a text the planner knew as it is ({@code text}), a value as the database's cast
     *  to text writes it ({@code str}: an Integer's digits, a date's ISO day, a text itself), a Float as legend-pure
     *  prints it ({@code FLOAT_SHORTEST_TEXT}). */
    private static @com.legend.base.Nullable String wordsOrNull(com.legend.sql.RaiseKind kind,
            java.util.List<SqlExpr> o) {
        for (SqlExpr x : o) {
            if (!(x instanceof SqlExpr.StringLit || x instanceof SqlExpr.IntLit || x instanceof SqlExpr.FloatLit
                    || x instanceof SqlExpr.DecimalLit || x instanceof SqlExpr.DateLit
                    || x instanceof SqlExpr.TimestampLit)) {
                return null;   // a computed operand: the interim
            }
        }
        return switch (kind) {
            case USER_FAIL -> null;
            case DIVIDE_BY_ZERO -> o.isEmpty() ? "Division by zero" : join("Cannot divide ", str(o, 0), " by zero");
            case SHIFT_OUT_OF_RANGE -> "Unsupported number of bits to shift - max bits allowed is 62";
            case DATE_PART_ABSENT -> join("Cannot get ", text(o, 0), " for ", str(o, 1));
            case INDEX_OUT_OF_BOUNDS -> join("The system is trying to get an element at offset ", str(o, 0),
                    " where the collection is of size ", str(o, 1));
            case INSERT_OUT_OF_BOUNDS -> "add(set, index, value): index out of bounds";
            case SQRT_OF_NEGATIVE -> o.get(0) instanceof SqlExpr.FloatLit f
                    ? "Unable to compute sqrt of " + Presenter.legendPure(f.value(), "Float") : null;
            case SIZE_NOT_MULTIPLICITY -> o.size() == 1
                    ? join("Cannot cast a collection of size 0 to multiplicity ", text(o, 0))
                    : join("Cannot cast a collection of size ", str(o, 1), " to multiplicity ", text(o, 0));
            case SLICE_BOUNDS_INVERTED -> join("The low bound (", str(o, 0), ") can't be higher than the high bound (",
                    str(o, 1), ") in a slice operation");
            case DATE_COMPONENT_INVALID -> dateComponentWords(o);
            case VALUE_NOT_HELD, FRACTION_NOT_KEPT, CSV_TOO_WIDE -> {
                StringBuilder parts = new StringBuilder();
                for (int i = 0; i < o.size(); i++) {
                    String part = o.get(i) instanceof SqlExpr.StringLit t ? t.value() : str(o, i);
                    if (part == null) {
                        break;
                    }
                    parts.append(part);
                }
                yield allWorded(o) ? parts.toString() : null;
            }
            case CAST_IMPOSSIBLE -> join("Cast exception: ", text(o, 0), " cannot be cast to ", text(o, 1));
            case INSTANCE_NOT_OF -> join("Cast exception: ", text(o, 0), " is not a ", text(o, 1));
            case MATCH_FAILURE -> matchFailure(text(o, 0), text(o, 1));
            case RANGE_STEP_ZERO -> "range step must not be 0";
            case WINDOW_FRAME_INVERTED -> "Invalid window frame boundary - lower bound of window"
                    + " frame cannot be greater than the upper bound!";
            case WINDOW_FRAME_OFFSET_INVALID -> "Invalid window frame boundary - a window frame"
                    + " bound written -(n) is n before the current row and any other n after it, so a computed n must"
                    + " be neither negative nor empty (docs/SEMANTICS_REGISTER.md S170)";
            case LIST_LENGTHS_DIFFER -> join(text(o, 0), ": the two value lists differ in length");
            case CONSTRAINT_UNEVALUABLE -> join("Unable to evaluate constraint [", text(o, 0),
                    "]: data not available - check your mappings");
            case MAP_KEYS_MISALIGNED -> "The map key list does not align with the map value list.";
            case MAP_KEY_NULL -> "Map keys can not be NULL.";
            case MAP_KEYS_NOT_UNIQUE -> "Map keys must be unique.";
            case TEXT_NOT_INTEGER -> join("Could not convert string '", text(o, 0), "' to INT64");
            case TEXT_NOT_FLOAT -> join("Could not convert string '", text(o, 0), "' to DOUBLE");
            case TEXT_NOT_DATE -> join("Could not parse '", text(o, 0), "' as a date");
        };
    }

    /** {@code MATCH_FAILURE}'s words: the value's type, and the arms where the resolver's dispatch read them. */
    private static @com.legend.base.Nullable String matchFailure(@com.legend.base.Nullable String type,
            @com.legend.base.Nullable String arms) {
        if (arms == null) {
            return null;
        }
        return arms.isEmpty() ? join("Match failure: ", type, " matches no branch")
                : "Match failure: no arm of [" + arms + "] accepts the row's run-time type";
    }

    /** Whether every one of {@code o} is worded exactly as a part ({@code parts}' rule: a text, or {@link #str}'s). */
    private static boolean allWorded(java.util.List<SqlExpr> o) {
        for (int i = 0; i < o.size(); i++) {
            if (!(o.get(i) instanceof SqlExpr.StringLit) && str(o, i) == null) {
                return false;
            }
        }
        return true;
    }

    /** {@link #dateComponent}'s words in Java, or null where its value is the database's text of a computed one. */
    private static @com.legend.base.Nullable String dateComponentWords(java.util.List<SqlExpr> o) {
        if (o.size() == 4) {
            return o.get(1) instanceof SqlExpr.IntLit y && o.get(2) instanceof SqlExpr.IntLit m
                    && o.get(3) instanceof SqlExpr.IntLit d ? "Invalid day: " + y.value() + "-" + m.value() + "-"
                    + d.value() : null;
        }
        String head = text(o, 0) == null ? null : "Invalid " + text(o, 0) + ": ";
        if (head == null) {
            return null;
        }
        return switch (o.get(1)) {
            case SqlExpr.IntLit lit -> head + lit.value();
            case SqlExpr.FloatLit fl -> head + java.math.BigDecimal.valueOf(fl.value()).toPlainString();
            case SqlExpr.DecimalLit dl -> head + dl.value().toPlainString();
            default -> join(head, str(o, 1));
        };
    }

    /** {@code parts} as one text, or null where any is (an operand not worded exactly). */
    private static @com.legend.base.Nullable String join(@com.legend.base.Nullable String... parts) {
        StringBuilder out = new StringBuilder();
        for (String p : parts) {
            if (p == null) {
                return null;
            }
            out.append(p);
        }
        return out.toString();
    }

    /** Operand {@code i}, a text the planner knew; null where it is not one. */
    private static @com.legend.base.Nullable String text(java.util.List<SqlExpr> o, int i) {
        return i < o.size() && o.get(i) instanceof SqlExpr.StringLit t ? t.value() : null;
    }

    /** Operand {@code i}'s text as the database's cast to text writes it, where Java writes the same: a text, an
     *  Integer's digits, a date's ISO day; null for any other (a Float's or a decimal's or a timestamp's text is the
     *  database's own, so its site stays in SQL). */
    private static @com.legend.base.Nullable String str(java.util.List<SqlExpr> o, int i) {
        if (i >= o.size()) {
            return null;
        }
        return switch (o.get(i)) {
            case SqlExpr.StringLit t -> t.value();
            case SqlExpr.IntLit n -> Long.toString(n.value());
            case SqlExpr.DateLit d -> d.iso();
            default -> null;
        };
    }

    /** date(...)'s component out of range: [the component, its literal] spelled at planning as legend-pure writes it
     *  ('Invalid month: 13', a fractional second's plain digits), [day, year, month, day] its attempted date
     *  ('Invalid day: 2016-12-32', the month unpadded), [the component, a value] the database's text of it. */
    private static SqlExpr dateComponent(SqlExpr.RaiseText r) {
        java.util.List<SqlExpr> o = r.operands();
        if (o.size() == 4) {
            return new SqlExpr.StringLit("Invalid day: " + ((SqlExpr.IntLit) o.get(1)).value() + "-"
                    + ((SqlExpr.IntLit) o.get(2)).value() + "-" + ((SqlExpr.IntLit) o.get(3)).value());
        }
        String head = "Invalid " + text(r, 0) + ": ";
        if (o.get(1) instanceof SqlExpr.IntLit lit) {
            return new SqlExpr.StringLit(head + lit.value());
        }
        if (o.get(1) instanceof SqlExpr.FloatLit fl) {
            return new SqlExpr.StringLit(head + java.math.BigDecimal.valueOf(fl.value()).toPlainString());
        }
        if (o.get(1) instanceof SqlExpr.DecimalLit dl) {
            return new SqlExpr.StringLit(head + dl.value().toPlainString());
        }
        // a value the database holds: its text
        return cat(new SqlExpr.StringLit(head), str(o.get(1)));
    }

    /** {@code r}'s operands as one text, in order: a text the planner knew as it is, a value as its text. */
    private static SqlExpr parts(SqlExpr.RaiseText r) {
        java.util.List<SqlExpr> o = r.operands();
        SqlExpr[] parts = new SqlExpr[o.size()];
        for (int i = 0; i < parts.length; i++) {
            parts[i] = o.get(i) instanceof SqlExpr.StringLit ? o.get(i) : str(o.get(i));
        }
        return parts.length == 1 ? parts[0] : cat(parts);
    }

    /** Operand {@code i}, a text the planner knew (a label, a type's name, a bound). */
    private static String text(SqlExpr.RaiseText r, int i) {
        return ((SqlExpr.StringLit) r.operands().get(i)).value();
    }

    /** {@code parts} as one text, in order ({@code CONCAT}, as the lowering's own {@code cat} builds it). */
    static SqlExpr cat(SqlExpr... parts) {
        SqlExpr out = parts[0];
        for (int i = 1; i < parts.length; i++) {
            out = SqlExpr.Call.of(com.legend.sql.SqlFn.CONCAT, out, parts[i]);
        }
        return out;
    }

    /** {@code x}'s text as the database casts it ({@code VARCHAR}), as the lowering's own {@code str} does. */
    static SqlExpr str(SqlExpr x) {
        return new SqlExpr.Cast(x, com.legend.sql.SqlType.Scalar.VARCHAR, SqlExpr.Reason.PRINT);
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
        if (message.length() > 1 && message.charAt(0) == com.legend.sql.RaiseKind.MARK) {
            // a product raise's text (MARK kind [MARK where]): its kind's name, as an upstream reader has read it since
            // PR1 (pure/v1's answer); the mark and the where are lite's carriage, never upstream's text -- the golden
            // net, 2026-11-16, found the mark leaking into the answer
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
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof com.legend.base.DataSize.TooLarge big) {
                return tooLarge(big);
            }
            if (t instanceof com.legend.testdatagen.TestDataPlanner.NoRowIdentifiers n) {
                // legend-pure's assert, worded from the generator's facts (J7/K12 of the edge audit, 2026-11-17)
                return noRowIdentifiers(n);
            }
        }
        // the kinded error itself, or the one a wrapper's message holds ("in function 'f': " + its message, and after
        // it the inliner's " [inlined via ...]": the P2 checkpoint audit's A-S1, 2026-11-16 -- spliced where it stands)
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof com.legend.error.LegendCompileException c && c.kind() != null) {
                String own = String.valueOf(c.getMessage());
                int at = message.lastIndexOf(own);
                if (at >= 0) {
                    return message.substring(0, at) + upstream(c) + message.substring(at + own.length());
                }
            }
        }
        return message;
    }

    /** The name an upstream answer gives an uncaught error's type (pure/v1's {@code "<type>: <message>"}): a parameter
     *  given no value is legend-engine's {@code IllegalArgumentException} (its PlanExecutor; lite's kind,
     *  {@code MissingParameters}, is the product's; the P2 checkpoint audit's M3/A-M4, 2026-11-16); any other error its
     *  own class's name. */
    public static String typeName(Throwable e) {
        if (e instanceof com.legend.error.MissingParameters || e instanceof com.legend.error.InvalidParameters) {
            return "IllegalArgumentException";
        }
        return uncaught(e) ? "IllegalStateException" : e.getClass().getSimpleName();
    }

    /** A compile kind upstream raises at run as an uncaught error, never as a refusal of the text: a TDS row's cell
     *  offset (legend-pure's {@code at}, an {@code IllegalStateException} -- the product's answer until W7 made it a
     *  kind, restored at the edge by the P2 checkpoint audit's A-S2, 2026-11-16). */
    public static boolean uncaught(Throwable e) {
        return e instanceof com.legend.error.LegendCompileException c
                && c.kind() == com.legend.error.CompileKind.ELEMENT_AT_OFFSET;
    }

    /** {@code c}'s sentence as upstream words its kind, after the driver's {@code [line:col]}. */
    private static String upstream(com.legend.error.LegendCompileException c) {
        java.util.List<String> o = c.operands();
        String upstream = switch (java.util.Objects.requireNonNull(c.kind(), "a kinded error")) {
            // PureModel.getConnection, getMapping, getRuntime, getStore, getAssociation (legend-engine)
            case MISSING -> "Can't find " + o.get(0) + " '" + o.get(1) + "'";
            // PureModel.getProfile
            case MISSING_PROFILE -> "Can't find the profile '" + o.get(0) + "'";
            // HelperModelBuilder.resolveStereotype, resolveTag
            case MISSING_PROFILE_MEMBER -> "Can't find " + o.get(0) + " '" + o.get(1) + "' in profile '" + o.get(2) + "'";
            // ClassMappingSecondPassBuilder
            case MISSING_CLASS_MAPPING -> "Can't find class mapping '" + o.get(0) + "' in mapping '" + o.get(1) + "'";
            // RelationalCompilerExtension's main table rule, its exact text (legend-engine 4.145.0,
            // RelationalCompilerExtension.java:350; F12, 2026-11-16: the tables the mappings span are lite's own words)
            case MAIN_TABLE_UNNAMED -> "Can't find the main table for class '" + o.get(0)
                    + "'. Please specify a main table using the ~mainTable directive.";
            // ClassCompilerExtension
            case INVALID_SUPERTYPE -> "Invalid supertype: '" + o.get(0) + "' cannot extend '" + o.get(1)
                    + "' as it is not a class.";
            // legend-pure's GenericType validation
            case TYPE_ARGUMENTS_COUNT -> "Wrong " + o.get(0) + " arguments count (" + o.get(1) + ") for type: " + o.get(2);
            case TYPE_VARIABLES_COUNT -> "Wrong type variables count (" + o.get(0) + ") for type: " + o.get(1);
            // legend-pure's collection at()
            case ELEMENT_AT_OFFSET -> "The system is trying to get an element at offset " + o.get(0)
                    + " where the collection is of size " + o.get(1);
            // legend-pure's rows()/_range() assert (the P2 checkpoint audit's M4, 2026-11-16)
            case WINDOW_FRAME_INVERTED -> "Invalid window frame boundary - lower bound of window frame cannot be greater"
                    + " than the upper bound!";
            // lite's own refusal (register S170), as the edge has spelled it
            case WINDOW_FRAME_COMPUTED_INVERTED -> "Invalid window frame boundary - a computed bound's direction is read"
                    + " from how it is written (-(n) is n before the current row, any other n after it), and this frame"
                    + " then starts after where it ends: write the bound before the current row as -(n)"
                    + " (docs/SEMANTICS_REGISTER.md S170)";
            // J10 of the edge audit, 2026-11-17: each the product's sentence until then, byte for byte
            // MappingCompilerExtension (legend-engine)
            case DUPLICATE_CLASS_MAPPING_IDS -> "Duplicated class mappings found with ID " + o.get(0) + " in mapping '"
                    + o.get(1) + "'";
            case DUPLICATE_MAPPING_INCLUDE -> "Duplicated mapping include '" + o.get(0) + "' in mapping '" + o.get(1)
                    + "'";
            // CompileContext.resolveStore (legend-engine)
            case MISSING_STORE -> "The store '" + o.get(0) + "' can't be found.";
            // PureModelBuilder (legend-engine)
            case DUPLICATE_ELEMENT -> "Duplicated element '" + o.get(0) + "'";
            // legend-engine's class and enumeration compilation
            case DUPLICATE_PROPERTY -> "Found duplicated property '" + o.get(0) + "' in class '" + o.get(1) + "'";
            case DUPLICATE_ENUM_VALUE -> "Found duplicated value '" + o.get(0) + "' in enumeration '" + o.get(1) + "'";
            // legend-pure's PropertyValidator
            case DEFAULT_ON_OPTIONAL -> "Default values are supported only for mandatory fields, and property '"
                    + o.get(0) + "' is optional.";
            // legend-engine's "can't be found" form of a store reference, lite's condition named after it
            case MISSING_STORE_COLUMN -> "The column '" + o.get(0) + "' can't be found in the " + o.get(1) + " '"
                    + o.get(2) + "' (" + o.get(3) + " of database '" + o.get(4) + "')";
            case MISSING_STORE_TABLE -> "The table '" + o.get(0) + "' can't be found in the database '" + o.get(1)
                    + "' (" + o.get(2) + ")";
        };
        return c.position() == null ? upstream : c.position() + " " + upstream;
    }

    /** legend-pure's sentence of a load's file larger than the load admits (J25 of the edge audit, 2026-11-17: moved
     *  from base.DataSize, whose refusal is the product's kind and facts): {@code CsvReader.readCsv}'s inner message, the
     *  text's length in megabytes against the limit, wrapped by its outer "Unable to read the CSV file" (read 2026-11-09
     *  at 5.99.0; the inner exception carries no source information, so "Execution error at ??, " and the info quoted:
     *  legend-pure's PureException.printMessage; the M1 correctness audit's C4, 2026-11-09). */
    static String tooLarge(com.legend.base.DataSize.TooLarge big) {
        return "Unable to read the CSV file '" + big.path() + "' Execution error at ??, \"File is too large, file was "
                + String.format(java.util.Locale.ROOT, "%.2f", big.characters() * 1.0 / (1024 * 1024L)) + " Mb, limit is "
                + big.limitMegabytes() + " Mb\"";
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
