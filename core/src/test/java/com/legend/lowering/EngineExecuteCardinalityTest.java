// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.ResultEnvelopeSplice;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSerialize;
import com.legend.compiler.spec.typed.TypedSerializeGraph;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The engine {@code execute()}'s JsonBuilder cardinality rule at a graph ROOT (compatibility only; the user's ruling,
 * 2026-11-03: "the right clean thing in lite, the backward-compatible thing in compatibility mode"): a serialize read
 * as {@code execute(...).values} is marked by the result-envelope splice, the resolver carries the mark to its graph,
 * and the lowerer folds that root as the scalar position does ({@link SnapshotEnvelope#fold}: exactly one object bare,
 * zero or several the array). Lite's own serialize root stays the array, and the streaming root never folds. Planning
 * only: no database is opened.
 */
class EngineExecuteCardinalityTest {

    private static final String MODEL = """
            Class s::Item { id: Integer[1]; name: String[0..1]; }
            ###Relational
            Database s::DB ( Table T ( ID INTEGER PRIMARY KEY, NAME VARCHAR(20) ) )
            ###Mapping
            Mapping s::M ( *s::Item: Relational { ~mainTable [s::DB] T
                id: [s::DB] T.ID, name: [s::DB] T.NAME } )
            ###Connection
            RelationalDatabaseConnection s::Conn { store: s::DB; type: H2; specification: LocalH2 { }; auth: DefaultH2; }
            ###Runtime
            Runtime s::RT { mappings: [s::M]; connections: [ s::DB: [ c1: s::Conn ] ]; }
            """;

    private static final String GRAPH = "|s::Item.all()->graphFetch(#{s::Item{id, name}}#)"
            + "->serialize(#{s::Item{id, name}}#)";

    @Test
    void aGraphReadAsAnEngineExecutesValuesFoldsAtTheRoot_liteKeepsTheArray() {
        ModelContext ctx = Compiler.compileModel(MODEL);
        TypedQuery q = Compiler.query(ctx, GRAPH);
        TypedSpec chain = q.body().get(q.body().size() - 1);
        assertFalse(serialize(chain).engineExecuteCardinality(), "lite's own serialize is never marked");

        // lite's own root: the array aggregate, unmarked
        Compiler.LoweredQuery own = com.legend.planner.Lowered.lower(q, "s::RT", false);
        assertFalse(graph(own.root()).engineExecuteCardinality());
        assertInstanceOf(SqlExpr.JsonArrayAgg.class, top(own.plan()));

        // the same chain as the values of an engine execute(...): the splice marks it, the resolver carries the mark
        // to the graph, the root folds over the same aggregate
        TypedSpec spliced = valuesRead(chain);
        assertTrue(serialize(spliced).engineExecuteCardinality(), "the splice marks the execute()'s serialize");
        Compiler.LoweredQuery compat = com.legend.planner.Lowered.lower(q.withBody(List.of(spliced)), "s::RT", false);
        assertTrue(graph(compat.root()).engineExecuteCardinality(), "the resolver carries the mark to its graph");
        SqlExpr folded = top(compat.plan());
        assertInstanceOf(SqlExpr.Case.class, folded);
        assertNotNull(SnapshotEnvelope.foldedAggregate(folded), "the singleton fold over the array aggregate");

        // the streaming root (one object per row) never folds
        SqlExpr streamed = top(com.legend.planner.Lowered.lower(q.withBody(List.of(spliced)), "s::RT", true).plan());
        assertNull(SnapshotEnvelope.foldedAggregate(streamed));
        assertFalse(streamed instanceof SqlExpr.JsonArrayAgg);
    }

    @Test
    void theSpliceMarksOnlyASerializeUnderItsFromEnvelopes() {
        ModelContext ctx = Compiler.compileModel(MODEL);
        TypedSpec chain = Compiler.query(ctx, GRAPH).body().get(0);
        TypedSpec framed = new TypedFrom(chain, Optional.empty(), Optional.empty(), chain.info());
        TypedFrom read = assertInstanceOf(TypedFrom.class, valuesRead(framed));
        assertTrue(read.executedExtent(), "the from() envelope is kept as the splice made it");
        assertTrue(assertInstanceOf(TypedSerialize.class, read.source()).engineExecuteCardinality());
        // any other chain stands as it is: the same instance
        TypedSpec relation = Compiler.query(ctx, "|#>{s::DB.T}#->select(~[ID])").body().get(0);
        assertSame(relation, ResultEnvelopeSplice.hook(frames(relation, true))
                .apply(values(relation), Set.of()));
    }

    /** {@code $r.values} over a frame {@code r} whose chain is {@code chain}, through the splice. */
    private static TypedSpec valuesRead(TypedSpec chain) {
        return ResultEnvelopeSplice.hook(frames(chain, false)).apply(values(chain), Set.of());
    }

    private static TypedSpec values(TypedSpec chain) {
        return new TypedPropertyAccess(new TypedVariable("r", chain.info()), "values", chain.info());
    }

    private static ResultEnvelopeSplice.Frames frames(TypedSpec chain, boolean relationRooted) {
        return new ResultEnvelopeSplice.Frames() {
            @Override
            public ResultEnvelopeSplice.@com.legend.base.Nullable View frame(String name) {
                return "r".equals(name) ? new ResultEnvelopeSplice.View(chain, relationRooted, null) : null;
            }

            @Override
            public ResultEnvelopeSplice.View inlineExecute(TypedNativeCall ec, boolean eager) {
                throw new AssertionError("no inline execute in this test");
            }

            @Override
            public @com.legend.base.Nullable String relationalActivitySql(String frameName, long activityNumber) {
                return null;
            }

            @Override
            public @com.legend.base.Nullable String relationalActivitySql(TypedNativeCall ec) {
                return null;
            }
        };
    }

    private static TypedSerialize serialize(TypedSpec n) {
        while (n instanceof TypedFrom f) {
            n = f.source();
        }
        return assertInstanceOf(TypedSerialize.class, n);
    }

    private static TypedSerializeGraph graph(TypedSpec n) {
        while (n instanceof TypedFrom f) {
            n = f.source();
        }
        return assertInstanceOf(TypedSerializeGraph.class, n);
    }

    /** The root's one projection, under the egress casts (and an empty-array COALESCE, as VerdictSql reads it). */
    private static SqlExpr top(SqlQuery plan) {
        SqlSelect s = assertInstanceOf(SqlSelect.class, plan);
        SqlExpr e = s.projections().get(0).expr();
        while (true) {
            if (e instanceof SqlExpr.Cast c) {
                e = c.value();
            } else if (e instanceof SqlExpr.Call k && k.fn() == SqlFn.COALESCE && !k.args().isEmpty()) {
                e = k.args().get(0);
            } else {
                return e;
            }
        }
    }
}
