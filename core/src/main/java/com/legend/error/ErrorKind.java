// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.error;

/**
 * THE KIND OF A PRODUCT ERROR RAISED IN A STATEMENT, AND ITS SENTENCE (phase D's D1.1): the error layer's own
 * vocabulary, beside the typed error ({@link DataError#kind}) -- exec parses a raise into its kind and decides nothing
 * more. One name for each of the plan's raise kinds ({@code sql.RaiseKind}, the MIR's bare enum: the two held equal by
 * name, both ways, by {@code ErrorKindTest}).
 *
 * <p>ONE CATALOG, ITS WORDS UPSTREAM'S (the user's ruling of 2026-11-21 ~02:40, replacing Q6 of 2026-11-10's "the
 * product's own wording"): a kind's sentence is legend-pure's (or legend-engine's) wherever upstream has one, the
 * values in it the site's operands as the kind's {@link Form} prints them -- a literal as the program wrote it, a value
 * only the database computed {@code <computed>} (D1, register S389; the SQL never reads it back, the lean-SQL rule).
 * Every audience says the same words: lite's API, the hosts (generated from these forms), legend-pure's PCT and
 * legend-engine's door (which adds its own {@code Raised:} wrapper). Lite's own words only where upstream has none:
 * {@code INSERT_OUT_OF_BOUNDS}, {@code LIST_LENGTHS_DIFFER}, the S170 window frame, and the refusals of a value a
 * database cannot hold (S138, S181, a load's width): each its own kind and sentence, its facts the operands -- a
 * database's name, a function's, a count (stage B's BE-P, 2026-11-21: until then three kinds whose operand was the
 * place's prose). And an in-body assert's failure ({@code ASSERT_TRUE}, {@code ASSERT_FALSE}, {@code ASSERT_NOT_EMPTY}):
 * the assert catalog's own sentences where legend-pure says "Assert failed" (item 3's S6; the user's Q15 = A, lite's
 * assert-failure words on every route, and the tenet of 2026-11-23: lite's own where it is better, the compatibility
 * edge re-spelling upstream's only where a test needs it).
 */
public enum ErrorKind implements ErrorCode {
    /** rem's and quotient's, the dividend its operand (legend-pure's CompiledSupport); Integer / Integer's, no operand:
     *  the interpreter's BigDecimal. */
    DIVIDE_BY_ZERO(Form.of(0, "Division by zero"), Form.any("Cannot divide %1$s by zero")),
    /** legend-engine's bitShiftLeft/Right.pure. */
    SHIFT_OUT_OF_RANGE(Form.any("Unsupported number of bits to shift - max bits allowed is 62")),
    /** [the part's label, the date]: legend-pure's date part accessors. */
    DATE_PART_ABSENT(Form.any("Cannot get %1$s for %2$s", Slot.TEXT, Slot.DATE)),
    /** [the offset, the size]: legend-pure's {@code at} native. */
    INDEX_OUT_OF_BOUNDS(Form.any("The system is trying to get an element at offset %1$s where the collection is of"
            + " size %2$s")),
    /** Lite's own (no upstream text: the D0 inventory's 1.6), as PCT's expected failures pin it. */
    INSERT_OUT_OF_BOUNDS(Form.any("add(set, index, value): index out of bounds")),
    /** [the value]: legend-pure's, the value as Pure prints a Float (K2). */
    SQRT_OF_NEGATIVE(Form.any("Unable to compute sqrt of %1$s", Slot.FLOAT)),
    /** [the bound] (a size known empty) or [the bound, the size]: legend-pure's toOne/cast. */
    SIZE_NOT_MULTIPLICITY(Form.of(1, "Cannot cast a collection of size 0 to multiplicity %1$s"),
            Form.any("Cannot cast a collection of size %2$s to multiplicity %1$s")),
    /** [low, high]: legend-pure's slice. */
    SLICE_BOUNDS_INVERTED(Form.any("The low bound (%1$s) can't be higher than the high bound (%2$s) in a slice"
            + " operation")),
    /** [day, year, month, day] its attempted date (the month unpadded), or [the component, its literal] (a
     *  fractional second's whole part, as legend-pure validates it): legend-pure's date(). */
    DATE_COMPONENT_INVALID(Form.of(4, "Invalid day: %2$s-%3$s-%4$s"),
            Form.any("Invalid %1$s: %2$s", Slot.TEXT, Slot.WHOLE)),
    /** [the row's class's name, the target's]: legend-pure's cast; the row's class {@code <computed>} where the
     *  database alone knows it (a root with mapped subtypes; S389). */
    CAST_IMPOSSIBLE(Form.any("Cast exception: %1$s cannot be cast to %2$s")),
    /** [the value's run-time class's name -- {@code <computed>} where only the database knows it, a class with
     *  subclasses (S389) -- the needed class's]: legend-pure's cast (Cast.java's sentence of a failed
     *  cast, the classes' names as GenericType.print writes them; the BE audit 2's S2, 2026-11-21: until then "is not
     *  a", which neither upstream says; the audit 3's M3: the names, until then the paths). */
    INSTANCE_NOT_OF(Form.any("Cast exception: %1$s cannot be cast to %2$s")),
    /** [the value as legend-pure prints it (CompiledSupport.matchFailure: "42 instanceOf Integer", a collection's
     *  "[..., ...]"; a value no plan holds {@code <computed>}), the arms]: legend-pure's where no branch takes the value
     *  (no arms named; the BE audit 3's M4, 2026-11-21: until then its type and multiplicity); the resolver's
     *  run-time dispatch, its arms named, where the value is the row only the database held, so legend-pure's
     *  sentence prints it {@code <computed>} (D1, register S389; the BE audit 2's S9, 2026-11-21: until then lite's own
     *  "no arm of [...] accepts the row's run-time type", where upstream has a sentence). */
    MATCH_FAILURE(Form.whenEmpty(1, "Match failure: %1$s"),
            Form.any("Match failure: <computed>")),
    /** legend-pure's Range native. */
    RANGE_STEP_ZERO(Form.any("range step must not be 0")),
    /** [the constraint's id]: legend-engine's. */
    CONSTRAINT_UNEVALUABLE(Form.any("Unable to evaluate constraint [%1$s]: data not available - check your mappings")),
    /** [the native's name]: lite's own (no upstream text). */
    LIST_LENGTHS_DIFFER(Form.any("%1$s: the two value lists differ in length")),
    /** legend-pure has none (its maps take any key); legend-engine's on its reference database, DuckDB's own words,
     *  which lite raises on every database (the D0 inventory's K3). */
    MAP_KEYS_MISALIGNED(Form.any("The map key list does not align with the map value list.")),
    MAP_KEY_NULL(Form.any("Map keys can not be NULL.")),
    MAP_KEYS_NOT_UNIQUE(Form.any("Map keys must be unique.")),
    /** [the text]: legend-engine's on its reference database, DuckDB's cast errors (legend-pure's are Java's own
     *  NumberFormatException text), which lite raises on every database. */
    TEXT_NOT_INTEGER(Form.any("Could not convert string '%1$s' to INT64")),
    TEXT_NOT_FLOAT(Form.any("Could not convert string '%1$s' to DOUBLE")),
    TEXT_NOT_DATE(Form.any("Could not parse '%1$s' as a date")),
    /** [the function (acos, asin)]: lite's own (S138: the database holds no NaN, the engine's answer). */
    INVERSE_TRIG_NAN(Form.of(1, "%1$s of a value outside [-1, 1] is NaN, which this database cannot hold")),
    /** Lite's own (S138), parseFloat of the text NaN. */
    PARSED_NAN_NOT_HELD(Form.of(0, "parseFloat of NaN is NaN, which this database cannot hold")),
    /** [the database]: lite's own (S138), a text read as a Float that is NaN or an infinity (the BE audit 2's S7,
     *  2026-11-21: until then the place alone, "NaN or an infinity"). */
    NOT_FINITE_NOT_HELD(Form.of(1, "NaN or an infinity has no %1$s value")),
    /** [the last year, the database] or [the first year, the last, the database]: lite's own, a date the arithmetic
     *  carries past the years the database holds. */
    DATE_PAST_YEARS(Form.of(2, "a date past the year %1$s has no %2$s value"),
            Form.of(3, "a date past the years %1$s-%2$s has no %3$s value")),
    /** [the first year, the last, the database]: lite's own, a date outside the years a database holds, which clamps
     *  one to the end it passed (ClickHouse). */
    DATE_OUTSIDE_YEARS(Form.of(3, "a date outside the years %1$s to %2$s has no %3$s value (it clamps one to the end it"
            + " passed)")),
    /** [the database]: lite's own, an Integer past 64 bits. */
    INTEGER_PAST_64_BITS(Form.of(1, "an integer past 64 bits has no %1$s value")),
    /** Lite's own: a mixed list's DOUBLE rank of an Integer beyond 2^53. */
    MIXED_RANK_INEXACT(Form.of(0, "an Integer beyond 2^53 in a mixed list's order: its DOUBLE rank does not hold it"
            + " exactly (refused until the kinds-kept carrier ranks numbers exactly)")),
    /** [Date or DateTime, the parameter's name, the database, the digits it keeps]: lite's own (S181, never cut;
     *  the unnamed form went 2026-11-21, the BE audit 4's M1: every site names its parameter). */
    PARAMETER_FRACTION_NOT_KEPT(Form.of(4, "the %1$s parameter '%2$s' has more fraction digits than %3$s keeps (%4$s):"
            + " refused, never cut (S181)")),
    /** [the second, the digits kept]: lite's own (S181), a date()'s fractional second. */
    SECOND_FRACTION_NOT_KEPT(Form.of(2, "the second %1$s has more than %2$s fraction digits, which a date()'s fractional"
            + " second keeps: refused, never cut (S181)")),
    /** [the database, the digits it reads], or [the database, the first day, the last day] it holds to the
     *  nanosecond: lite's own (S181, S399), a date-time an assert compares. */
    JUDGED_INSTANT_NOT_HELD(Form.of(2, "a date-time an assert compares has more fraction digits than %1$s reads in it"
            + " (%2$s): refused, never cut (S181)"), Form.of(3, "a date-time an assert compares to the nanosecond is"
            + " outside the days %1$s holds to the nanosecond (%2$s to %3$s): refused (S181)")),
    /** []: lite's own, an Any value an assert compares that holds a JSON object or array. */
    JUDGED_TREE_NOT_TYPED(Form.of(0, "an Any value an assert compares holds a JSON object or array, which the database"
            + " judge compares by no typed value: refused")),
    /** [the table's column count]: lite's own, a CSV record wider than its table (a load). */
    CSV_TOO_WIDE(Form.of(1, "a record of the CSV file has more cells than its table's %1$s column(s)")),
    /** legend-engine's rows()/_range() assert, verbatim (#81's frame guard). */
    WINDOW_FRAME_INVERTED(Form.any("Invalid window frame boundary - lower bound of window frame cannot be greater than"
            + " the upper bound!")),
    /** Lite's own (S170). */
    WINDOW_FRAME_OFFSET_INVALID(Form.any("Invalid window frame boundary - a window frame bound written -(n) is n before"
            + " the current row and any other n after it, so a computed n must be neither negative nor empty"
            + " (docs/SEMANTICS_REGISTER.md S170)")),
    /** [the start, the end, the length]: legend-pure's String.substring raise, Java's own words (S383). */
    SUBSTRING_OUT_OF_RANGE(Form.any("Range [%1$s, %2$s) out of bounds for length %3$s")),
    /** An in-body assert's failure with no message of its caller's (item 3's S6): the one catalog's sentence of the
     *  assert's kind (lite's words, the user's Q15 = A and Q-S6 -- one catalog for an assert's failure wherever it
     *  fails). No values: a raise carries facts fixed at planning, never a value the database computed. */
    ASSERT_TRUE(Form.of(0, AssertKind.TRUE.template())),
    /** {@code assertFalse}'s, as {@link #ASSERT_TRUE}. */
    ASSERT_FALSE(Form.of(0, AssertKind.FALSE.template())),
    /** {@code assertNotEmpty}'s, as {@link #ASSERT_TRUE}. */
    ASSERT_NOT_EMPTY(Form.of(0, AssertKind.NOT_EMPTY.template())),
    /** {@code fail(message)}: the user's own message is the error's text. ({@code fail()} with none raises
     *  {@link #ASSERT_TRUE} since item 3's S6; the bare form stays for a plan that names the kind with no operand.) */
    USER_FAIL(Form.of(0, "fail"), Form.any("fail: %1$s")),
    /** A failure the database raised with no site of lite's (phase D's Phase 2, item 4, 2026-11-16; the user's ruling:
     *  "I'm good with database only giving the database message too"): its message is the database's own, verbatim,
     *  carried as the database's ({@link DataError#databaseMessage}), its SQLSTATE where the driver gives one. Not a
     *  raise kind: no statement writes it. */
    DATABASE_ERROR(Form.of(0, "the database refused the statement"),
            Form.any("the database refused the statement: %1$s")),
    /** A session that could not be opened, or a connection lost (a driver's SQLSTATE class 08): the database's own
     *  message, as {@link #DATABASE_ERROR}'s. Not a raise kind. */
    CONNECTION_ERROR(Form.of(0, "the connection to the database failed"),
            Form.any("the connection to the database failed: %1$s")),
    /** A load's file larger than the load admits (J25 of the edge audit, 2026-11-17): [the file, its bytes, its
     *  characters, the limit in megabytes], legend-pure's CsvReader.readCsv words ({@link Form#fileSize}: the text's
     *  length in megabytes to two places). The run refuses it before any statement reads it. Not a raise kind. */
    FILE_TOO_LARGE(Form.fileSize()),
    /** A load's statement the database refused: [the table it fills, the database's own message], lite's own words
     *  (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §3 rule 5: a refusal names its data). Not a raise kind (stage B's BE,
     *  2026-11-21: until then the plan carried "the data of the table ..." as a load step's prose). */
    LOAD_REFUSED(Form.of(2, "the data of the table %1$s: %2$s")),
    /** A statement binding a data: payload the database refused: [the payload digest's head, its characters, the
     *  database's own message], lite's own words (the B checkpoint's tenets audit, LOW 4). Not a raise kind (stage B's
     *  BE, 2026-11-21: until then the plan carried the payload's label as prose). */
    PAYLOAD_REFUSED(Form.of(3, "the statement reading the data: payload %1$s (%2$s characters) was refused: %3$s"));

    /** How a form prints one value of a site's operands (the site's record's values are these prints). */
    public enum Slot {
        /** As the program wrote it: a text itself, an Integer's digits, a Float as Pure prints one, a Decimal's plain
         *  digits, a Boolean's word, a date's ISO text. */
        TEXT,
        /** A date literal at the precision it was written, {@code +0000} from a minute on (legend-pure's
         *  DateFormat.append); any other value as {@link #TEXT}. */
        DATE,
        /** A number as Pure prints a Float (an Integer's too: {@code 4.0}); any other value as {@link #TEXT}. */
        FLOAT,
        /** A number's whole part, truncated toward zero, as legend-pure's date() validates a fractional second
         *  ({@code second.intValue()}; the BE audit 2's S2, 2026-11-21: until then its plain digits); any other as
         *  {@link #TEXT}. */
        WHOLE
    }

    /**
     * One of a kind's sentences: chosen by the number of values (any, where {@code arity} is negative) and, where
     * {@code emptySlot} is not negative, by that value being empty; the first form of the kind that takes the values
     * says them. {@code template} is a format over the values, positional ({@code %1$s}, ...); {@code slots} how each is
     * printed ({@link Slot#TEXT} past the last).
     */
    public record Form(int arity, int emptySlot, String template, java.util.List<Slot> slots, Shape shape) {

        /** How a form says its values. */
        public enum Shape {
            /** {@code template} over the values. */
            TEMPLATE,
            /** A load's file larger than its limit: [the file, bytes, characters, limit in megabytes]. */
            FILE_SIZE
        }

        public Form {
            slots = java.util.List.copyOf(slots);
        }

        static Form of(int arity, String template, Slot... slots) {
            return new Form(arity, -1, template, java.util.List.of(slots), Shape.TEMPLATE);
        }

        static Form any(String template, Slot... slots) {
            return of(-1, template, slots);
        }

        static Form whenEmpty(int slot, String template, Slot... slots) {
            return new Form(-1, slot, template, java.util.List.of(slots), Shape.TEMPLATE);
        }

        static Form fileSize() {
            return new Form(4, -1, "Unable to read the CSV file '%1$s' Execution error at ??, \"File is too large, file"
                    + " was %2$s Mb, limit is %3$s Mb\"", java.util.List.of(), Shape.FILE_SIZE);
        }

        /** How value {@code i} is printed. */
        public Slot slot(int i) {
            return i < slots.size() ? slots.get(i) : Slot.TEXT;
        }

        boolean takes(int size, java.util.function.IntPredicate empty) {
            return (arity < 0 || arity == size) && size >= named()
                    && (emptySlot < 0 || emptySlot < size && empty.test(emptySlot));
        }

        /** The values the template names: its highest {@code %n$s} (the BE audit 2's S5, 2026-11-21: a site with fewer
         *  operands is no form's, refused by name, never a format's own error). */
        int named() {
            int n = 0;
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("%(\\d+)\\$s").matcher(template);
            while (m.find()) {
                n = Math.max(n, Integer.parseInt(m.group(1)));
            }
            return n;
        }

        String say(java.util.List<String> values) {
            return switch (shape) {
                case TEMPLATE -> String.format(java.util.Locale.ROOT, template, values.toArray());
                case FILE_SIZE -> String.format(java.util.Locale.ROOT, template, values.get(0),
                        String.format(java.util.Locale.ROOT, "%.2f",
                                Long.parseLong(values.get(2)) * 1.0 / (1024 * 1024L)), values.get(3));
            };
        }
    }

    /** Whether the kind is the database's own failure ({@link #DATABASE_ERROR}, {@link #CONNECTION_ERROR}), never a
     *  raise a statement writes. */
    public boolean isDatabases() {
        return this == DATABASE_ERROR || this == CONNECTION_ERROR;
    }

    /** Whether a statement raises the kind ({@code sql.RaiseKind} names it): every kind but the database's own
     *  failures, a load's refused file ({@link #FILE_TOO_LARGE}, refused by the run before a statement) and a load's or
     *  a payload's statement the database refused ({@link #LOAD_REFUSED}, {@link #PAYLOAD_REFUSED}: its words around
     *  the database's). */
    public boolean isRaise() {
        return !isDatabases() && this != FILE_TOO_LARGE && this != LOAD_REFUSED && this != PAYLOAD_REFUSED;
    }

    /** A value the database computed and no plan holds, as every audience prints it (D1, register S389). */
    public static final String COMPUTED = "<computed>";

    private final java.util.List<Form> forms;

    ErrorKind(Form... forms) {
        this.forms = java.util.List.of(forms);
    }

    /** The kind's forms, in the order they are tried (the hosts' tables are generated from them). */
    public java.util.List<Form> forms() {
        return forms;
    }

    /** The form that says {@code size} values, {@code empty} telling which are empty; refused by name where none does
     *  (a site built otherwise than its kind's sentence reads it: a defect of the site). */
    public Form form(int size, java.util.function.IntPredicate empty) {
        for (Form f : forms) {
            if (f.takes(size, empty)) {
                return f;
            }
        }
        throw new IllegalStateException("no sentence of " + name() + " says " + size + " values");
    }

    @Override
    public String family() {
        return "run";
    }

    /** The kind's sentence over {@code values}, each printed as its form's slot says. */
    @Override
    public String sentence(java.util.List<String> values) {
        return form(values.size(), i -> values.get(i).isEmpty()).say(values);
    }
}
