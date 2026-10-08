// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import java.util.ArrayList;
import java.util.List;

/**
 * THE ONE PLACE A DIALECT WRITES SQL (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §10, E): text, in order, and each bound
 * parameter at the place it is written ({@link #bind}), so the statement's parameters are listed in the order their
 * {@code ?} placeholders appear — by construction, as jOOQ, Calcite and Hibernate write SQL. A render method writes into
 * the writer and returns nothing; a piece written twice is written twice, its parameter with it.
 *
 * <p>E-1: the clause layer (queries, selects, sources) writes here; expressions still render as strings through a bridge
 * ({@link #text}), where a bound parameter is refused, as before.
 */
final class SqlWriter {

    private final StringBuilder sql = new StringBuilder();
    private final List<RenderedStatement.Bind> binds = new ArrayList<>();

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
