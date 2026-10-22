// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import java.util.ArrayList;
import java.util.List;

/**
 * THE ONE PLACE A DIALECT WRITES SQL (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §10, E): text, in order, and each bound
 * parameter at the place it is written ({@link #bind}), so the statement's parameters are listed in the order their
 * {@code ?} placeholders appear — by construction, as jOOQ, Calcite and Hibernate write SQL. A render method writes into
 * the writer and returns that writer, so a dispatching switch stays an expression whose cases javac checks
 * ({@code return switch (c.fn()) { case SQRT -> writer.append("sqrt(").expr(a, 0).append(")"); ... }}); a piece written
 * twice is written twice, its parameter with it.
 *
 * <p>Every dialect writes all of its SQL here (E, complete 2026-10-09): every query, its expressions and composing
 * helpers, the legacy engine-text printer's text, and DML's rows. DDL spells only names, types and keywords, so it is
 * text, as any spelling is.
 */
final class SqlWriter {

    /** How the writer's dialect writes an expression into a writer ({@code AnsiSqlRenderer.expr}): the writer is made
     *  by its dialect, so {@link #expr} writes a sub-expression in that dialect's spelling, in place. */
    @FunctionalInterface
    interface Expressions {
        void write(SqlWriter writer, com.legend.sql.SqlExpr e, int parentPrec);
    }

    /** A piece of SQL that writes itself — what a helper wraps when it does not build it itself (a value read out of
     *  jsonb, a list whose elements it ranges over): written where the helper writes it, as often as it does, its
     *  parameters with it (jOOQ's QueryPart). */
    @FunctionalInterface
    interface Piece {
        void writeTo(SqlWriter writer);
    }

    private final StringBuilder sql = new StringBuilder();
    private final List<RenderedStatement.Bind> binds = new ArrayList<>();
    private final List<RenderedStatement.Region> regions = new ArrayList<>();
    private final Expressions expressions;
    /** The per-key region being written (none, or the one: a region holds no other). */
    private final java.util.ArrayDeque<OpenRegion> open = new java.util.ArrayDeque<>();

    /** A per-key region while it is written: where it starts, its first placeholder, its counters so far. */
    private record OpenRegion(int start, int firstBind, List<Integer> counters) {
    }

    /** How deep the writer is inside the arguments of a set function the database evaluates when it plans
     *  ({@link #planned}): Postgres estimates generate_series's rows from its arguments, raises and all
     *  (2026-10-17, the Cypher island's range of a bound that may be null, docs/CYPHER_STUDY_2026_10_15.md §12.73). */
    private int planned;

    SqlWriter(Expressions expressions) {
        this.expressions = java.util.Objects.requireNonNull(expressions, "expressions");
    }

    SqlWriter append(String text) {
        sql.append(text);
        return this;
    }

    SqlWriter append(char c) {
        sql.append(c);
        return this;
    }

    SqlWriter append(long n) {
        sql.append(n);
        return this;
    }

    /** Writes {@code e} here, in the writer's dialect, at {@code parentPrec}. */
    SqlWriter expr(com.legend.sql.SqlExpr e, int parentPrec) {
        expressions.write(this, e, parentPrec);
        return this;
    }

    /** Writes the expressions joined by {@code separator}, each at {@code parentPrec}. */
    SqlWriter join(List<com.legend.sql.SqlExpr> es, String separator, int parentPrec) {
        for (int i = 0; i < es.size(); i++) {
            if (i > 0) {
                sql.append(separator);
            }
            expr(es.get(i), parentPrec);
        }
        return this;
    }

    /** Writes the expressions, comma-separated. */
    SqlWriter list(List<com.legend.sql.SqlExpr> es) {
        return join(es, ", ", 0);
    }

    /** Writes {@code name(args...)}, the arguments comma-separated. */
    SqlWriter function(String name, List<com.legend.sql.SqlExpr> args) {
        return append(name).append("(").list(args).append(")");
    }

    /** Writes each item as {@code each} writes it, joined by {@code separator}. */
    <T> SqlWriter join(List<T> items, String separator, java.util.function.BiConsumer<SqlWriter, T> each) {
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) {
                sql.append(separator);
            }
            each.accept(this, items.get(i));
        }
        return this;
    }

    /** Writes {@code piece} here. */
    SqlWriter piece(Piece piece) {
        piece.writeTo(this);
        return this;
    }

    /** Writes {@code piece} here as an argument the database evaluates when it plans: a raise within it is written
     *  so no planner can evaluate it before a row asks for it ({@link #inPlanned}). */
    SqlWriter planned(Piece piece) {
        planned++;
        try {
            piece.writeTo(this);
        } finally {
            planned--;
        }
        return this;
    }

    /** Whether the writer is inside an argument the database evaluates when it plans ({@link #planned}). */
    boolean inPlanned() {
        return planned > 0;
    }

    /** Writes a placeholder and records the parameter it binds, at this place in the statement. */
    SqlWriter bind(RenderedStatement.Bind bind) {
        sql.append('?');
        binds.add(bind);
        return this;
    }

    /** Writes {@code placeholder} as this parameter's place and records the bind, as {@link #bind} does with {@code ?}:
     *  a target whose statement is not SQL spells a placeholder in its own language (MongoDb's marker document;
     *  2026-10-16, docs/MONGODB_PLAN_2026_10_16.md D5), bound by its place among the placeholders, as {@code ?} is: a
     *  per-key region's copies are copies of its text (2026-10-20). */
    SqlWriter bind(String placeholder, RenderedStatement.Bind bind) {
        sql.append(placeholder);
        binds.add(bind);
        return this;
    }

    /** How many parameters are bound so far: the index the next placeholder takes. */
    int bound() {
        return binds.size();
    }

    /**
     * Writes {@code body} as a PER-KEY REGION ({@link RenderedStatement.Region}): text the runner writes once per key of
     * the plan's keys probe, {@code lead} before the first copy and {@code separator} between copies, nothing with no
     * key ({@code empty} in its place, or the statement refused when that is null). Inside it, {@link #counter} marks
     * where a copy's key position goes and {@link #bindKey} binds the copy's key; a region holds no other region.
     */
    SqlWriter region(String lead, String separator, @com.legend.base.Nullable String empty, Piece body) {
        if (!open.isEmpty()) {
            throw new IllegalStateException("a per-key region inside another per-key region");
        }
        OpenRegion r = new OpenRegion(sql.length(), binds.size(), new ArrayList<>());
        open.push(r);
        body.writeTo(this);
        open.pop();
        regions.add(new RenderedStatement.Region(r.start(), sql.length(), lead, separator, empty, r.firstBind(),
                binds.size() - r.firstBind(), r.counters()));
        return this;
    }

    /** The key's position in the probe, written here in each copy of the per-key region being written (a per-key
     *  name's: {@code "_pk3__u0"}); refused outside a region, where there is no key. */
    SqlWriter counter() {
        OpenRegion r = open.peek();
        if (r == null) {
            throw new IllegalStateException("a per-key name outside a per-key region: no key to name it by");
        }
        r.counters().add(sql.length());
        return this;
    }

    /** A placeholder binding the probe row's {@code cell} of each copy's key; refused outside a per-key region. */
    SqlWriter bindKey(int cell) {
        return bindKey("?", cell);
    }

    /** {@link #bindKey(int)} with the placeholder spelled in the target's own language, as {@link #bind(String,
     *  RenderedStatement.Bind)} (MongoDb's marker document). */
    SqlWriter bindKey(String placeholder, int cell) {
        if (open.isEmpty()) {
            throw new IllegalStateException("a key's cell outside a per-key region: no key to bind");
        }
        sql.append(placeholder);
        binds.add(new RenderedStatement.Bind("", new RenderedStatement.Binding.Key(cell)));
        return this;
    }

    /** The placeholder bound last is typed by its value, its type written HERE when the value is known: records the
     *  hole at this place in the statement, with the spelling of each kind of value. */
    SqlWriter typeHole(java.util.Map<com.legend.sql.ValueKind, RenderedStatement.TypeSpelling> types,
            com.legend.sql.ValueKind absent) {
        if (!open.isEmpty()) {
            throw new IllegalStateException("a value-typed placeholder inside a per-key region: the runner fills a"
                    + " type hole once, and a region is written once per key");
        }
        if (binds.isEmpty() || !(binds.get(binds.size() - 1).binding() instanceof RenderedStatement.Binding.One one)
                || one.hole() != null) {
            throw new IllegalStateException("a type hole follows a placeholder of one value with none: " + binds);
        }
        RenderedStatement.Bind last = binds.get(binds.size() - 1);
        binds.set(binds.size() - 1, new RenderedStatement.Bind(last.parameter(), new RenderedStatement.Binding.One(
                one.nullType(), new RenderedStatement.TypeHole(sql.length(), types, absent))));
        return this;
    }

    /** The statement written, with its parameters in placeholder order. */
    RenderedStatement statement() {
        return new RenderedStatement(sql.toString(), binds, regions);
    }

    /** The text written, for a caller that takes SQL as text: refused when a parameter was bound, which only a
     *  statement ({@link #statement}) can carry. */
    String text() {
        if (!regions.isEmpty()) {
            throw new IllegalStateException("a statement with per-key regions was asked for as text: render it as a"
                    + " statement, which the runner writes once per key");
        }
        if (!binds.isEmpty()) {
            throw new IllegalStateException("a statement with bound parameters " + binds
                    + " was asked for as text: render it as a statement");
        }
        return sql.toString();
    }
}
