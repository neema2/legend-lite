// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.base.Nullable;
import com.legend.builtin.Pure;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.NotImplementedException;
import com.legend.lowering.EditionTable;
import com.legend.lowering.NamedRule;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.model.FunctionId;
import com.legend.planner.Edition;
import com.legend.platform.Feature;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * LEGEND-ENGINE'S EDITION AND ITS TABLE (the edition program's step 2, 2026-11-25;
 * docs/records/edition-plan-to-finish-stage-b-2026-11-25.md): what legend-engine's door hands the planner
 * ({@link Edition}: {@link #DOOR}) and, through it, the lowering ({@link EditionTable}). Each row names a rule of the
 * product's by what it writes ({@link NamedRule}); none of legend-engine's SQL is here.
 *
 * <p>Inside a store query legend-engine sends these to the database as it has them (register S130, S134, S383, S384):
 * <ul>
 *   <li>{@code substring}: the database's own SUBSTRING over the arguments as given ({@code SUBSTRING_AS_GIVEN}), or,
 *       under {@code CORRECT_SQL_SUBSTRING_INDEXING}, over {@code start + 1} and {@code end - start}
 *       ({@code SUBSTR_AS_GIVEN}: legend-engine's processSubstr);</li>
 *   <li>{@code substr}: always corrected ({@code SUBSTR_AS_GIVEN});</li>
 *   <li>the two-argument String {@code indexOf}: the database's position ({@code SQL_POSITION});</li>
 *   <li>{@code toOne} of an operand that is always empty: no check, the operand's SQL null ({@code TRUST_ONE}:
 *       legend-engine's processNoOp);</li>
 *   <li>{@code round}: the database's own ROUND, which on DuckDB and H2 sends a half away from zero
 *       ({@code ROUND_AS_GIVEN}; legend-engine's {@code transformRoundForDuckDB} and H2's {@code round}
 *       write the bare function; the edition program's step 5, 2026-11-26, a delegation the user ruled matched,
 *       amendment 22). Pure's {@code round} sends a half to the even neighbour;</li>
 *   <li>{@code toDecimal}: the database's own DECIMAL with no precision written ({@code CAST_TO_BARE_DECIMAL};
 *       legend-engine's {@code cast(%s as decimal)}), where its row answers ({@link PureText#ENGINE_BARE_DECIMAL_READ}):
 *       no fraction digit on H2, where {@code toDecimal(3.8)} is 4 (register S423; ruled matched, amendment 22).</li>
 *   <li>{@code parseInteger} and {@code parseFloat}: the database's own cast of the text, its refusal the database's
 *       own conversion error ({@code TEXT_CAST_TO_INTEGER}, {@code TEXT_CAST_TO_FLOAT}; legend-engine's SQL casts,
 *       ruled plain inside a store query: the edition design's S-2), on every database legend-engine has. Until
 *       2026-11-26 a pass of the edge's dialect unguarded them in every compatibility style and on every database,
 *       PCT's legend-pure run and a host-side {@code let} included.</li>
 *   <li>{@code dateDiff}: the database's own date difference for every unit ({@code DATE_DIFF_AS_GIVEN}), where
 *       Pure counts weeks by Sunday boundaries and hours, minutes and seconds as elapsed whole units (register
 *       S425);</li>
 *   <li>{@code splitPart}: the database's own {@code split_part} over the index plus one
 *       ({@code SPLIT_PART_AS_GIVEN}), on the databases where legend-engine writes that function (register S426);</li>
 *   <li>the relational {@code isNumeric}: legend-engine's own form on each database, a row for each form (a plain
 *       decimal's text where its dialect translation writes a regular expression, no cased letter where it writes
 *       {@code lower = upper}), where the product's is one numeric check on every database (register S370).</li>
 *   <li>{@code toString} of a Float: the database's own cast to text ({@code FLOAT_TEXT_BY_CAST}, and
 *       {@code FLOAT_TEXT_BY_CAST_OF_A_DOUBLE} where legend-engine's literal processor reads a Float literal as a
 *       double), on every database legend-engine has, where the product's is six decimals (the edition program's
 *       step 8, 2026-11-26; the user's ruling that day, "agreed on float/date text for engine").</li>
 *   <li>{@code toString} of a DateTime that is no literal: the database's own cast to text
 *       ({@code DATE_TIME_TEXT_BY_CAST}), on the databases whose text of it has been run, where the product's is
 *       ISO 8601 with {@code Z} (the same step and ruling).</li>
 * </ul>
 * Outside any store query (the planner's stamp) legend-engine's own Java computes them, with Pure's meaning: no row.
 * That is a store-free {@code let}, and a model-to-model mapping's transforms under a graph fetch or over a model
 * connection ({@link #transformsOutsideStore}).
 *
 * <p>legend-engine's answer for a row is the database's own function's, so each row carries the databases it answers
 * on ({@link PureText}'s sets: where a lane has read legend-engine's SQL and run it beside the rule). On any other
 * database a call a row would answer is refused by name, never answered with a guess (the user's ruling of 2026-11-20
 * ~07:55), while the statement is planned, before anything is sent. A call legend-engine writes as the database's own
 * function on some databases only ({@link #WHERE_WRITTEN}) is a row there and the call's own rule everywhere else.
 */
final class EngineTable implements EditionTable, Edition {

    /** legend-engine's door's edition: what the door's style hands the planner. As a table it answers nothing: the
     *  planner makes the table of a statement's database from it ({@link #table}). */
    static final EngineTable DOOR = new EngineTable(false, null, true, false);

    /** legend-engine's own tests' door's edition (the relational corpus): legend-engine's meaning inside the queries a
     *  test's {@code execute()}s run, as at its API, and Pure's in the test's own code, which legend-pure runs --
     *  every call of a test body outside those queries is stamped ({@link #stampsTestBodies}). */
    static final EngineTable TESTS = new EngineTable(false, null, true, true);

    /** PCT's legend-engine run's edition: a PCT case reaches lite as the one expression legend-engine's own PCT adapter
     *  sends to the database, so all of it is a store query's code -- nothing is stamped, and every row answers
     *  ({@link #stampsOutsideStore}). */
    static final EngineTable PCT = new EngineTable(false, null, false, false);

    /** A row: the rule that lowers the call, what a refusal calls the call, and the databases the row answers on --
     *  those where the rule gives legend-engine's own answer, read from its SQL and run on the database (a set of the
     *  edge's database owner, {@link PureText}: this table names no database). */
    private record Row(NamedRule as, String named, Set<DatabaseType> on) {
        Row(NamedRule as, String named) {
            this(as, named, PureText.ENGINE_FUNCTIONS_READ);
        }

        boolean answersOn(@Nullable DatabaseType database) {
            return database != null && on.contains(database);
        }
    }

    private static final FunctionId TO_ONE = FunctionId.of(Pure.TO_ONE__T_MANY);
    private static final List<Row> TO_ONE_OF_EMPTY = List.of(new Row(NamedRule.TRUST_ONE, "toOne(empty)"));

    /** {@code toString} of a Float: the database's own cast to text ({@code cast(%s as varchar)}, legend-engine's
     *  {@code toString} dynaFunction on every database it has: each lane's reading,
     *  docs/records/edition-homework/lanes/engine-templates-2026-11-26.md), a Float literal as its literal processor
     *  writes it -- read as a double first where its processor casts it ({@link PureText#ENGINE_FLOAT_LITERAL_AS_DOUBLE}),
     *  bare everywhere else. The user's ruling of 2026-11-26, "agreed on float/date text for engine": a row of this
     *  table, not presentation. On a database legend-engine does not have there is no row and the call keeps its own
     *  rule, lite's text. Until that day a pass of the edge's dialects wrote this cast for every Float text of every
     *  compatibility plan ({@code PurePrintForms}); the three tests it holds are PCT's legend-engine
     *  lane's {@code testFloatToStringWithNegativeExponent} (DuckDB) and
     *  {@code testFloatToStringWithPositiveExponent} (H2, {@code 1.3421E8}, failing as legend-engine's H2 does) and
     *  the corpus's validation showcase ({@code Quantity not in range: 1000000}), measured
     *  (docs/records/edition-homework/step8/). The row answers a call whose operand's declared type is Float; a
     *  Float reached through a Number or an Any has no row and keeps lite's text. */
    private static final FunctionId TO_STRING = FunctionId.of(Pure.TO_STRING__ANY_1);
    /** {@code toString} of a DateTime that is no literal, inside a store query: legend-engine writes
     *  {@code cast(%s as varchar)} for every type (legend-engine 4.145.0, each database's extension), so the
     *  database's own text of the timestamp is its answer ({@code 2014-01-02 10:20:30.25}); the user's ruling of
     *  2026-11-26, "agreed on float/date text for engine". Where the database's text of that cast has been read
     *  ({@link PureText#ENGINE_DATE_TIME_CAST_READ}); on any other database there is no row and the call keeps its
     *  own rule. A LITERAL has no row: its text stays its written text and {@code +0000}, the constant the edge's
     *  lowering writes, until the user rules on it (PCT's legend-engine lane passes three {@code toString} tests
     *  with it that legend-engine itself fails: docs/records/edition-homework/step8/moves-if-date-text-is-lites.md).
     *  No recorded test holds the row. */
    private static final List<Row> DATE_TIME_TO_STRING = List.of(
            new Row(NamedRule.DATE_TIME_TEXT_BY_CAST, "toString(DateTime)", PureText.ENGINE_DATE_TIME_CAST_READ));
    private static final List<Row> FLOAT_TO_STRING = List.of(
            new Row(NamedRule.FLOAT_TEXT_BY_CAST_OF_A_DOUBLE, "toString(Float)", PureText.ENGINE_FLOAT_LITERAL_AS_DOUBLE),
            new Row(NamedRule.FLOAT_TEXT_BY_CAST, "toString(Float)", PureText.ENGINE_DATABASES));

    /** Each call to its rows, in order: the first that answers on the statement's database lowers the call. One row
     *  for most; one per form where legend-engine writes the function its own way on each database. */
    private static final Map<FunctionId, List<Row>> AS_GIVEN;
    private static final Map<FunctionId, List<Row>> CORRECTED;

    /** Rows that answer only where legend-engine's SQL is the database's own function, and are no row on any other
     *  database: the call keeps its own rule there, as before the row, and is never refused. For a call legend-engine
     *  answers elsewhere by a function of its own Java, whose meaning is Pure's or near it ({@code splitPart} on H2:
     *  {@code legend_h2_extension_split_part}, Apache's {@code StringUtils.split}, which drops empty parts as Pure
     *  does), or has no database for at all (the plain parses). */
    private static final Map<FunctionId, Row> WHERE_WRITTEN;

    static {
        Map<FunctionId, List<Row>> given = new java.util.LinkedHashMap<>();
        each(given, Pure.AT_STRING_SUBSTRING, NamedRule.SUBSTRING_AS_GIVEN, "substring/2", "substring/3");
        each(given, Pure.AT_STRING_SUBSTR, NamedRule.SUBSTR_AS_GIVEN, "substr/2", "substr/3");
        given.put(FunctionId.of(Pure.INDEX_OF__STRING_1__STRING_1),
                List.of(new Row(NamedRule.SQL_POSITION, "indexOf/2")));
        each(given, Pure.AT_MATH_ROUND, NamedRule.ROUND_AS_GIVEN, "round/2", "round/2", "round/1");
        // isNumeric: the regular expression legend-engine's dialect translation writes (toPostgresModel.pure:367,
        // through sqlDialectDefaults.pure:894-897, 966-970: a plain decimal's text), or its lower(s) = upper(s)
        // (duckdbExtension.pure:238); read and run 2026-11-26 (docs/records/edition-homework/r3/
        // measured-step5-facts.md, C9). Until that day the edge's forms wrote lower = upper on every database
        // (register S370, corrected). On any other database legend-engine has no SQL for it: refused by name
        given.put(Pure.AT_LEGEND_LITE_IS_NUMERIC.get(0), List.of(
                new Row(NamedRule.TEXT_IS_PLAIN_DECIMAL, "isNumeric/1", PureText.ENGINE_IS_NUMERIC_BY_PATTERN),
                new Row(NamedRule.TEXT_HAS_NO_CASED_LETTER, "isNumeric/1", PureText.ENGINE_IS_NUMERIC_BY_CASE)));
        // dateDiff: the database's own function for every unit (legend-engine's datediff(unit, a, b) on both:
        // duckdbExtension.pure:213, toPostgresModel.pure:397; ruled 2026-11-26, "Yes all units"; register S425)
        each(given, Pure.AT_DATE_DATE_DIFF, NamedRule.DATE_DIFF_AS_GIVEN, "dateDiff/3", "dateDiff/3");
        Map<FunctionId, List<Row>> flagged = new java.util.LinkedHashMap<>();
        each(flagged, Pure.AT_STRING_SUBSTRING, NamedRule.SUBSTR_AS_GIVEN, "substring(corrected)/2",
                "substring(corrected)/3");
        // lookup tables, never iterated; unmodifiable in declaration order all the same (NoSaltedIterationOrderTest)
        AS_GIVEN = java.util.Collections.unmodifiableMap(given);
        CORRECTED = java.util.Collections.unmodifiableMap(flagged);
        // splitPart: the database's own split_part over the index plus one (pureToSQLQuery.pure:3197-3204,
        // duckdbExtension.pure:276; ruled 2026-11-26, "Yes match split fully"; register S426)
        // the plain parses: the database's own cast wherever legend-engine has a database and so writes one (its
        // template on each, read by the lanes); on a database it does not have (SQLite, MariaDB, MySQL, MongoDB) no
        // row -- the call's own checked parse, because a cast there can answer a value for a text that is no number
        // (SQLite's CAST('abc' AS INTEGER) is 0; the independent review of 2026-11-26)
        // toDecimal: the bare DECIMAL where its row answers, and the call's own rule everywhere else, never refused
        // (on DuckDB legend-engine writes the same cast, but its adapter spells the value another way: PureText's
        // ENGINE_BARE_DECIMAL_READ; a refusal there failed two PCT tests legend-engine passes, measured 2026-11-26)
        WHERE_WRITTEN = Map.of(Pure.AT_STRING_SPLIT_PART.get(0),
                new Row(NamedRule.SPLIT_PART_AS_GIVEN, "splitPart/3", PureText.ENGINE_SPLIT_PART_READ),
                Pure.AT_MATH_TO_DECIMAL.get(0),
                new Row(NamedRule.CAST_TO_BARE_DECIMAL, "toDecimal/1", PureText.ENGINE_BARE_DECIMAL_READ),
                Pure.AT_STRING_PARSE_INTEGER.get(0),
                new Row(NamedRule.TEXT_CAST_TO_INTEGER, "parseInteger/1", PureText.ENGINE_DATABASES),
                Pure.AT_STRING_PARSE_FLOAT.get(0),
                new Row(NamedRule.TEXT_CAST_TO_FLOAT, "parseFloat/1", PureText.ENGINE_DATABASES));
    }

    /** Whether this is a statement's table (made by {@link #table}); the door's edition is not. */
    private final boolean table;
    /** The database this table's statement runs on; null where its planning names no single one. */
    private final @Nullable DatabaseType database;

    /** Whether code outside any store query is stamped (legend-engine's own Java runs it, with Pure's meaning): at
     *  its API and in its own tests; not in its PCT run, where every case is sent to the database whole. */
    private final boolean stamps;

    /** Whether a test body's own code is stamped: legend-engine's own tests' door's. */
    private final boolean testBodies;

    private EngineTable(boolean table, @Nullable DatabaseType database, boolean stamps, boolean testBodies) {
        this.table = table;
        this.database = database;
        this.stamps = stamps;
        this.testBodies = testBodies;
    }

    /** Each overload of {@code from} lowered by {@code rule}, named in a refusal by the name at its place: the
     *  overloads are listed in their declared order (builtin.Pure). */
    private static void each(Map<FunctionId, List<Row>> rows, List<FunctionId> from, NamedRule rule,
            String... names) {
        if (from.size() != names.length) {
            throw new IllegalStateException("the engine table names each overload of a row: " + from + " and "
                    + List.of(names));
        }
        for (int i = 0; i < from.size(); i++) {
            rows.put(from.get(i), List.of(new Row(rule, names[i])));
        }
    }

    // ---- the lowering's table ---------------------------------------------------------------------------------------

    @Override
    public @Nullable NamedRule lowersAs(TypedNativeCall call, Set<Feature> features) {
        if (!table) {
            throw new IllegalStateException("legend-engine's edition was asked for a row: the planner makes the table"
                    + " of a statement's database from it (Edition.table)");
        }
        if (call.outsideStore()) {
            return null;
        }
        List<Row> rows = rows(call, features);
        if (rows == null) {
            return null;
        }
        for (Row row : rows) {
            if (row.answersOn(database)) {
                return row.as();
            }
        }
        // never guessed: legend-engine's answer on another database is checked first (the database lanes)
        throw new NotImplementedException("legend-engine's answer for " + rows.get(0).named()
                + (call.pos() == null ? "" : " at " + call.pos().startLine() + ":" + call.pos().startColumn())
                + " is not yet checked on " + (database == null ? "a runtime with no single database"
                        : database.name()) + ", so this query is refused");
    }

    /** legend-engine's SQL writes no check for the kinds of its census class A ({@link PureText#ENGINE_UNCHECKED}):
     *  inside a store query the database decides. Outside one (the planner's stamp) legend-engine's own Java computes
     *  the call and raises as Pure does, so the check is written. A check that is no call's own (a cast that can never
     *  succeed, a slice) carries no stamp. */
    @Override
    public boolean checks(com.legend.sql.RaiseKind kind, @Nullable TypedNativeCall call) {
        if (!table) {
            throw new IllegalStateException("legend-engine's edition was asked about a check: the planner makes the"
                    + " table of a statement's database from it (Edition.table)");
        }
        if (call == null) {
            // no call's own: at legend-engine's API it is read as inside a store query (B7). At its tests' door the
            // node cannot say which side it is on, and the test's own code must raise as Pure does, so the check is
            // written on both sides, as it was before that door had a table (the independent audit of 2026-11-26,
            // finding 3)
            return testBodies || !PureText.ENGINE_UNCHECKED.contains(kind);
        }
        return call.outsideStore() || !PureText.ENGINE_UNCHECKED.contains(kind);
    }

    private @Nullable List<Row> rows(TypedNativeCall call, Set<Feature> features) {
        FunctionId key = call.callee().id();
        if (TO_ONE.equals(key)) {
            return alwaysEmpty(call.args().get(0)) ? TO_ONE_OF_EMPTY : null;
        }
        if (TO_STRING.equals(key)) {
            // a Float's, where legend-engine has the database, and a DateTime's that is no literal, where its
            // database's cast text has been read: no row is the call's own rule
            com.legend.compiler.spec.typed.TypedSpec operand = call.args().get(0);
            com.legend.compiler.element.type.Type type = operand.info().type();
            if (type == com.legend.compiler.element.type.Type.Primitive.FLOAT) {
                return PureText.ENGINE_DATABASES.contains(database) ? FLOAT_TO_STRING : null;
            }
            if (type == com.legend.compiler.element.type.Type.Primitive.DATE_TIME
                    && !(operand instanceof com.legend.compiler.spec.typed.TypedCDate)) {
                return PureText.ENGINE_DATE_TIME_CAST_READ.contains(database) ? DATE_TIME_TO_STRING : null;
            }
            return null;
        }
        Row written = WHERE_WRITTEN.get(key);
        if (written != null) {
            return written.answersOn(database) ? List.of(written) : null;
        }
        if (features.contains(Feature.CORRECT_SQL_SUBSTRING_INDEXING)) {
            List<Row> flagged = CORRECTED.get(key);
            if (flagged != null) {
                return flagged;
            }
        }
        return AS_GIVEN.get(key);
    }

    /** Whether the typer gives {@code operand} no value at all ({@code [0]}: {@code []}, {@code []->cast(@T)}): a fact
     *  of the program's types, never a value. */
    private static boolean alwaysEmpty(TypedSpec operand) {
        return operand.info().multiplicity() instanceof Multiplicity.Bounded b && b.upper() != null && b.upper() == 0;
    }

    // ---- the planner's edition --------------------------------------------------------------------------------------

    @Override
    public boolean stampsOutsideStore() {
        return stamps;
    }

    @Override
    public boolean stampsTestBodies() {
        return testBodies;
    }

    /** legend-engine checks a class's constraints in its own Java, with Pure's meaning. */
    @Override
    public boolean constraintsOutsideStore() {
        return true;
    }

    /** legend-engine runs a model-to-model mapping's transforms in its own Java (Pure's meaning) under a graph fetch
     *  ({@code storeContract.pure:89-91}) and over a model connection with no chain to a store; in a TDS query over a
     *  chain whose source is a store it pushes them into the source query ({@code chain.pure:111-116}), the
     *  database's meaning. */
    @Override
    public boolean transformsOutsideStore(boolean fetchesAGraph, boolean chained) {
        return fetchesAGraph || !chained;
    }

    /** legend-engine's plan generation finds a query's {@code withFeatureFlags} calls and adds their flags to its
     *  context ({@code executionPlan_generation.pure}). The one a row reads is the corrected substring's; the door
     *  reads no other, as before the table (legend-engine's other flags written in a query are not honoured here). */
    @Override
    public Set<Feature> writtenFlagsRead() {
        return Set.of(Feature.CORRECT_SQL_SUBSTRING_INDEXING);
    }

    /** Each row's answer is the database's own function's, checked per database. */
    @Override
    public boolean rowsByDatabase() {
        return true;
    }

    @Override
    public EditionTable table(@Nullable DatabaseType onDatabase) {
        return new EngineTable(true, onDatabase, stamps, testBodies);
    }
}
