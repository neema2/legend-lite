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
 *       ({@code ROUND_HALF_AWAY_FROM_ZERO}; legend-engine's {@code transformRoundForDuckDB} and H2's {@code round}
 *       write the bare function; the edition program's step 5, 2026-11-26, a delegation the user ruled matched,
 *       amendment 22). Pure's {@code round} sends a half to the even neighbour.</li>
 * </ul>
 * Outside any store query (the planner's stamp) legend-engine's own Java computes them, with Pure's meaning: no row.
 * That is a store-free {@code let}, and a model-to-model mapping's transforms under a graph fetch or over a model
 * connection ({@link #transformsOutsideStore}).
 *
 * <p>legend-engine's answer for a row is the database's own function's, so it is checked per database
 * ({@link PureText#engineAnswers}). On a database where it is not yet checked, a call a row would answer is refused by
 * name, never answered with a guess (the user's ruling of 2026-11-20 ~07:55). The refusal is raised while the statement
 * is planned, before anything is sent.
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

    /** A row: the rule that lowers the call, and what a refusal calls the call. */
    private record Row(NamedRule as, String named) {
    }

    private static final FunctionId TO_ONE = FunctionId.of(Pure.TO_ONE__T_MANY);
    private static final Row TO_ONE_OF_EMPTY = new Row(NamedRule.TRUST_ONE, "toOne(empty)");

    private static final Map<FunctionId, Row> AS_GIVEN;
    private static final Map<FunctionId, Row> CORRECTED;

    static {
        Map<FunctionId, Row> given = new java.util.LinkedHashMap<>();
        each(given, Pure.AT_STRING_SUBSTRING, NamedRule.SUBSTRING_AS_GIVEN, "substring/2", "substring/3");
        each(given, Pure.AT_STRING_SUBSTR, NamedRule.SUBSTR_AS_GIVEN, "substr/2", "substr/3");
        given.put(FunctionId.of(Pure.INDEX_OF__STRING_1__STRING_1), new Row(NamedRule.SQL_POSITION, "indexOf/2"));
        each(given, Pure.AT_MATH_ROUND, NamedRule.ROUND_HALF_AWAY_FROM_ZERO, "round/2", "round/2", "round/1");
        Map<FunctionId, Row> flagged = new java.util.LinkedHashMap<>();
        each(flagged, Pure.AT_STRING_SUBSTRING, NamedRule.SUBSTR_AS_GIVEN, "substring(corrected)/2",
                "substring(corrected)/3");
        // lookup tables, never iterated; unmodifiable in declaration order all the same (NoSaltedIterationOrderTest)
        AS_GIVEN = java.util.Collections.unmodifiableMap(given);
        CORRECTED = java.util.Collections.unmodifiableMap(flagged);
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
    private static void each(Map<FunctionId, Row> rows, List<FunctionId> from, NamedRule rule, String... names) {
        if (from.size() != names.length) {
            throw new IllegalStateException("the engine table names each overload of a row: " + from + " and "
                    + List.of(names));
        }
        for (int i = 0; i < from.size(); i++) {
            rows.put(from.get(i), new Row(rule, names[i]));
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
        Row row = row(call, features);
        if (row == null) {
            return null;
        }
        if (!PureText.engineAnswers(database)) {
            // never guessed: legend-engine's answer on another database is checked first (the database lanes)
            throw new NotImplementedException("legend-engine's answer for " + row.named()
                    + (call.pos() == null ? "" : " at " + call.pos().startLine() + ":" + call.pos().startColumn())
                    + " is not yet checked on " + (database == null ? "a runtime with no single database"
                            : database.name()) + ", so this query is refused");
        }
        return row.as();
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

    private static @Nullable Row row(TypedNativeCall call, Set<Feature> features) {
        FunctionId key = call.callee().id();
        if (TO_ONE.equals(key)) {
            return alwaysEmpty(call.args().get(0)) ? TO_ONE_OF_EMPTY : null;
        }
        if (features.contains(Feature.CORRECT_SQL_SUBSTRING_INDEXING)) {
            Row flagged = CORRECTED.get(key);
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
