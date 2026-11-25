// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.builtin.Pure;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.NotImplementedException;
import com.legend.lowering.EditionTable;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.model.FunctionId;
import com.legend.platform.Feature;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * LEGEND-ENGINE'S TABLE (the edition program's step 2, 2026-11-25; docs/records/edition-plan-to-finish-stage-b-2026-11-25.md):
 * the rows legend-engine's door hands the lowering ({@link EditionTable}). Each row names a product native by what it
 * is; none of legend-engine's SQL is here.
 *
 * <p>Inside a store query legend-engine sends these to the database as it has them (register S130, S134, S383, S384):
 * <ul>
 *   <li>{@code substring}: the database's own SUBSTRING over the arguments as given ({@code substringAsGiven}), or,
 *       under {@code CORRECT_SQL_SUBSTRING_INDEXING}, over {@code start + 1} and {@code end - start}
 *       ({@code substrAsGiven}: legend-engine's processSubstr);</li>
 *   <li>{@code substr}: always corrected ({@code substrAsGiven});</li>
 *   <li>the two-argument String {@code indexOf}: the database's position ({@code sqlPosition});</li>
 *   <li>{@code toOne} of an operand that is always empty: no check, the operand's SQL null ({@code trustOne}:
 *       legend-engine's processNoOp).</li>
 * </ul>
 * Outside any store query (the stamp) legend-engine's own Java computes them, with Pure's meaning: no row.
 *
 * <p>legend-engine's answer for a row is the database's own function's, so it is checked per database
 * ({@link PureText#engineAnswers}). On a database where it is not yet checked, a call a row would answer is refused by
 * name, never answered with a guess (the user's ruling of 2026-11-20 ~07:55). The refusal is raised while the statement
 * is planned, before anything is sent.
 */
final class EngineTable implements EditionTable {

    /** The table as a door's style hands it over: bound to its statement's database by the planner ({@link #on})
     *  before the lowering asks it anything. */
    static final EngineTable INSTANCE = new EngineTable(() -> {
        throw new IllegalStateException("legend-engine's table was asked for a row before the planner bound it to its"
                + " statement's database (EditionTable.on)");
    });

    private static final FunctionId TO_ONE = FunctionId.of(Pure.TO_ONE__T_MANY);

    private static final Map<FunctionId, FunctionId> AS_GIVEN;
    private static final Map<FunctionId, FunctionId> CORRECTED;

    static {
        Map<FunctionId, FunctionId> given = new java.util.LinkedHashMap<>();
        byPosition(given, Pure.AT_STRING_SUBSTRING, Pure.AT_LEGEND_LITE_SUBSTRING_AS_GIVEN);
        byPosition(given, Pure.AT_STRING_SUBSTR, Pure.AT_LEGEND_LITE_SUBSTR_AS_GIVEN);
        given.put(FunctionId.of(Pure.INDEX_OF__STRING_1__STRING_1), Pure.AT_LEGEND_LITE_SQL_POSITION.get(0));
        Map<FunctionId, FunctionId> flagged = new java.util.LinkedHashMap<>();
        byPosition(flagged, Pure.AT_STRING_SUBSTRING, Pure.AT_LEGEND_LITE_SUBSTR_AS_GIVEN);
        // lookup tables, never iterated; unmodifiable in declaration order all the same (NoSaltedIterationOrderTest)
        AS_GIVEN = java.util.Collections.unmodifiableMap(given);
        CORRECTED = java.util.Collections.unmodifiableMap(flagged);
    }

    private final Supplier<@com.legend.base.Nullable DatabaseType> database;

    private EngineTable(Supplier<@com.legend.base.Nullable DatabaseType> database) {
        this.database = database;
    }

    /** Each overload of {@code from} to the overload of {@code to} at the same place: the two lists are declared in
     *  the same order, the two-argument overload then the three-argument one (builtin.Pure). */
    private static void byPosition(Map<FunctionId, FunctionId> rows, List<FunctionId> from, List<FunctionId> to) {
        if (from.size() != to.size()) {
            throw new IllegalStateException("the engine table pairs overloads by position: " + from + " and " + to);
        }
        for (int i = 0; i < from.size(); i++) {
            rows.put(from.get(i), to.get(i));
        }
    }

    @Override
    public @com.legend.base.Nullable FunctionId lowersAs(TypedNativeCall call, Set<Feature> features) {
        if (call.outsideStore()) {
            return null;
        }
        FunctionId row = row(call, features);
        if (row == null) {
            return null;
        }
        DatabaseType on = database.get();
        if (!PureText.engineAnswers(on)) {
            // never guessed: legend-engine's answer on another database is checked first (the database lanes)
            throw new NotImplementedException("legend-engine's answer for " + named(call, features)
                    + " is not yet checked on " + (on == null ? "a runtime with no single database" : on.name())
                    + ", so this query is refused");
        }
        return row;
    }

    private static @com.legend.base.Nullable FunctionId row(TypedNativeCall call, Set<Feature> features) {
        FunctionId key = call.callee().id();
        if (TO_ONE.equals(key)) {
            return alwaysEmpty(call.args().get(0)) ? Pure.AT_LEGEND_LITE_TRUST_ONE.get(0) : null;
        }
        if (features.contains(Feature.CORRECT_SQL_SUBSTRING_INDEXING)) {
            FunctionId flagged = CORRECTED.get(key);
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

    /** What a refusal calls {@code call}: its function and argument count, and its place when the request carried one;
     *  never the query's text. */
    private static String named(TypedNativeCall call, Set<Feature> features) {
        FunctionId key = call.callee().id();
        String name;
        if (TO_ONE.equals(key)) {
            name = "toOne(empty)";
        } else {
            String simple = call.callee().qualifiedName();
            simple = simple.substring(simple.lastIndexOf(':') + 1);
            boolean corrected = features.contains(Feature.CORRECT_SQL_SUBSTRING_INDEXING) && CORRECTED.containsKey(key);
            name = simple + (corrected ? "(corrected)" : "") + "/" + call.args().size();
        }
        return call.pos() == null ? name : name + " at " + call.pos().startLine() + ":" + call.pos().startColumn();
    }

    @Override
    public boolean stampsOutsideStore() {
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

    @Override
    public EditionTable on(Supplier<@com.legend.base.Nullable DatabaseType> onDatabase) {
        return new EngineTable(onDatabase);
    }
}
