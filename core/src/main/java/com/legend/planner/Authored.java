// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.executionplan.ExecutionPlan;

import java.util.List;

/**
 * A STATEMENT A PERSON WROTE, AS A PLAN (the user's ruling of 2026-11-12, "Yes do 5 and 6": Studio's SQL playground goes
 * through the planner): the one statement of the person's text, carried as the author's SQL the way executeInDb's is
 * (the data-mechanism census's M7-authored: SQL the user wrote, staying -- a program's input, never the platform's),
 * on the database the playground points at. The planner neither parses, rewrites nor splits it: one text, one
 * statement, sent as written; a text the database cannot take is the database's own error. Its answer is the
 * relation the database reports (a raw grid's: the columns its result says), and the plan says it may write -- a
 * person's INSERT puts rows in a table -- since no reading of the text is made to tell.
 */
public final class Authored {

    private Authored() {
    }

    /** {@code text}, the person's one statement, as a plan on {@code type}'s database as {@code dialect} writes its
     *  target. A blank text is refused by name: there is no statement to carry. */
    public static ExecutionPlan plan(com.legend.sql.dialect.SqlDialect dialect, com.legend.executionplan.DatabaseType type,
            String text) {
        if (text == null || text.strip().isEmpty()) {
            throw new IllegalArgumentException("an authored statement is the person's text, and this one is empty");
        }
        ExecutionPlan.Target target = Targets.of(new ExecutionPlan.Database.Platform(type, null), dialect);
        return ExecutionPlan.writing(List.of(), new ExecutionPlan.TdsResult(ExecutionPlan.TdsResult.Shape.RELATION,
                List.of(), ExecutionPlan.Sql.of(text, target)));
    }
}
