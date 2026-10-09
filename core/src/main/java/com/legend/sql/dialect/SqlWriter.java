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
    private final Expressions expressions;

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

    /** Writes a placeholder and records the parameter it binds, at this place in the statement. */
    SqlWriter bind(RenderedStatement.Bind bind) {
        sql.append('?');
        binds.add(bind);
        return this;
    }

    /** The statement written, with its parameters in placeholder order. */
    RenderedStatement statement() {
        return new RenderedStatement(sql.toString(), binds);
    }

    /** The text written, for a caller that takes SQL as text: refused when a parameter was bound, which only a
     *  statement ({@link #statement}) can carry. */
    String text() {
        if (!binds.isEmpty()) {
            throw new IllegalStateException("a statement with bound parameters " + binds
                    + " was asked for as text: render it as a statement");
        }
        return sql.toString();
    }
}
