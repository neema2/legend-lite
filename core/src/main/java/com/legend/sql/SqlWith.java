// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

import java.util.List;

/**
 * A WITH-prefixed query: named common table expressions ahead of a body
 * query that references them as tables. Minted by the CTE-extraction
 * post-processor ({@code SqlPostProcessors.extractSubqueriesAsCtes} — the
 * engine's cteExtractionPostProcessor: every subselect in the FROM tree
 * becomes {@code subquery_cte_<level>_<index>}); the body's outputs are
 * the query's.
 */
public record SqlWith(List<Cte> ctes, SqlQuery body, boolean recursive) implements SqlQuery {

    public SqlWith {
        ctes = List.copyOf(ctes);
        if (ctes.isEmpty()) {
            throw new IllegalArgumentException("a WITH without expressions is its body");
        }
    }

    public SqlWith(List<Cte> ctes, SqlQuery body) {
        this(ctes, body, false);
    }

    /** The same WITH over other expressions and body: {@code recursive} carried (a rebuilding pass must not drop it:
     *  a recursive expression reads itself, and without the keyword its own name is unknown). */
    public SqlWith with(List<Cte> ctes, SqlQuery body) {
        return new SqlWith(ctes, body, recursive);
    }

    /** The name a relation the statement reads more than once is written once under (the lowering's
     *  {@code SharedRelations}, 2026-11-04): {@code __let<n>}, unique within the statement that lowered it. A dialect
     *  with no common table expressions writes each such expression back where it is read (MongoDB's pipeline). */
    public static final String SHARED_PREFIX = "__let";

    /** One {@code name AS (query)}; {@code materialized} asks the database
     * to evaluate it ONCE however many times it is referenced (leg 3.4: a
     * frame every assert of a body reads — every side sees the same rows
     * of a nondeterministically ordered result). Dialects without the
     * keyword (H2) spell a plain CTE. */
    public record Cte(String name, SqlQuery query, boolean materialized) {
        public Cte(String name, SqlQuery query) {
            this(name, query, false);
        }
    }

    /** {@code ctes} defined ahead of {@code q}'s own (a WITH merges; any
     * other query becomes the body). */
    public static SqlQuery prepend(List<Cte> ctes, SqlQuery q) {
        return prepend(ctes, q, false);
    }

    /** {@link #prepend}, the WITH {@code recursive} when {@code ctes} need it
     * (or {@code q}'s own did). */
    public static SqlQuery prepend(List<Cte> ctes, SqlQuery q, boolean recursive) {
        if (ctes.isEmpty()) {
            return q;
        }
        if (q instanceof SqlWith w) {
            List<Cte> all = new java.util.ArrayList<>(ctes);
            all.addAll(w.ctes());
            return new SqlWith(all, w.body(), recursive || w.recursive());
        }
        return new SqlWith(ctes, q, recursive);
    }

    @Override
    public List<OutputCol> outputs() {
        return body.outputs();
    }
}
