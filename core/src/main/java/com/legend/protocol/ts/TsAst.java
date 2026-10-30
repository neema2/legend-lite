// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.ts;

import com.legend.protocol.sql.SqlAst;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@code #TS{...}#} query as {@link TsParser} reads it (docs/TIMESERIES_LANGUAGE_2026_10_29.md §3, R1): its WITH
 * sources, its items, its FROM sources and its FILTER. Every expression in it is the SQL parser's ({@link SqlAst}),
 * read by Postgres's grammar where the island's text has it; what the clauses mean is the compiler's island seam's.
 */
public final class TsAst {

    private TsAst() {
    }

    /** A query: its WITH sources, its items, its FROM sources, its periods ({@code SAMPLE BY}; none: null), how lines
     *  meet ({@code FILL}; none: null), its FILTER (none: null), where {@code all} stands as a window
     *  ({@code sum(x, all)}, read as a column of the name {@link TsParser#ALL}), and each span it writes ({@code 3mo},
     *  read as a column of its place's). */
    public record Query(List<With> withs, List<Item> items, List<From> sources, @com.legend.base.Nullable Span sample,
            @com.legend.base.Nullable Fill fill, SqlAst.@com.legend.base.Nullable Expr filter, Set<SqlAst.Pos> alls,
            Map<SqlAst.Pos, Span> spans) {
        public Query {
            withs = List.copyOf(withs);
            items = List.copyOf(items);
            sources = List.copyOf(sources);
            alls = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(alls));
            spans = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(spans));
        }
    }

    /** A span of time (§5): {@code n} of {@code unit} ({@code d}, {@code w}, {@code mo}, {@code q}, {@code y}, {@code h},
     *  {@code min}, {@code s}), where it is written. */
    public record Span(int n, String unit, SqlAst.Pos pos) {
    }

    /** How lines meet on dates one has and another has not (R6): {@code PREV}, {@code STEP} (gs-quant's: PREV, and a
     *  line's first value before it starts), {@code LINEAR}, {@code NULL}, {@code ZERO} ({@code FILL(0)}), where it is
     *  written. */
    public record Fill(String how, SqlAst.Pos pos) {
    }

    /** {@code WITH name AS (source)}: a source named for the query, a flat subquery or a filtered source. */
    public record With(SqlAst.Ident name, Source body) {
    }

    /** A select item: its expression (null for {@code *}, a subquery's only), its name ({@code AS name}; none: null),
     *  where it starts. */
    public record Item(SqlAst.@com.legend.base.Nullable Expr expr, SqlAst.@com.legend.base.Nullable Ident alias,
            SqlAst.Pos pos) {
    }

    /** A FROM source: what it reads, its alias (none: null), its date column ({@code ON}) and its instrument column
     *  ({@code BY}; none: null), each a column or a class's dotted path. */
    public record From(Source source, SqlAst.@com.legend.base.Nullable Ident alias, SqlAst.ColumnRef on,
            SqlAst.@com.legend.base.Nullable ColumnRef by, SqlAst.Pos pos) {
    }

    /** What a source reads. */
    public sealed interface Source permits Named, Variable, Filtered, Flat {
        SqlAst.Pos pos();
    }

    /** A class, a table or a WITH source by its name as written: {@code MarketQuote}, {@code marketdata::MarketQuote},
     *  {@code prices}, {@code demo::Db.schema.prices}. */
    public record Named(String name, SqlAst.Pos pos) implements Source {
    }

    /** A relation of the enclosing Pure: {@code $quotes}. */
    public record Variable(String name, SqlAst.Pos pos) implements Source {
    }

    /** {@code (source WHERE condition)}: a source's rows that meet the condition. */
    public record Filtered(Source of, SqlAst.Expr where, SqlAst.Pos pos) implements Source {
    }

    /** {@code (SELECT items FROM source [WHERE condition])}: a flat source, a class's paths named once. */
    public record Flat(List<Item> items, Source from, SqlAst.@com.legend.base.Nullable Expr where, SqlAst.Pos pos)
            implements Source {
        public Flat {
            items = List.copyOf(items);
        }
    }
}
