// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.executionplan;

import java.util.List;
import java.util.Objects;

/**
 * THE ONE TEXT EXEC SENDS A DATABASE (the C checkpoint's MUST-FIX 1, 2026-11-12: the user's "are we provably totally
 * separated?"; the hosts' {@code PlannedSql}, engine-client/src/planned-sql.ts, in Java): a statement a PLAN carries,
 * exactly as the plan holds it. Every method of a session's statements that sends text ({@code exec.Statements}:
 * {@code execute}, {@code query}, {@code update}, {@code columns}, {@code describe}, {@code readsFile}, {@code rows},
 * {@code reportedRows}) takes this type and no other, so a runner that writes its own text -- a comment in front of a
 * statement, a statement of its own -- does not compile: a {@code String} is not a {@code PlannedSql}.
 *
 * <p>It is minted where a plan is READ, and nowhere else: the plan's own records hand out the text they hold
 * ({@link ExecutionPlan.Sql#planned}, a target's session statements and error read-back, a load's statements and a
 * file's reading, a script's bracket and abort; the test plan's savepoints), and the runner's keyed expansion writes a
 * keyed body's regions once per key (the charter's A3(a), the one sanctioned writing at run: {@code PlanRunner.expand}).
 * {@code ArchitectureTest.onlyThePlansReadersMintPlannedSql} holds {@link #fromPlan} to those callers; a test's own
 * text is a test's, outside the product's classes.
 */
public final class PlannedSql {

    private final String text;

    private final List<ExecutionPlan.Site> sites;

    private PlannedSql(String text, List<ExecutionPlan.Site> sites) {
        this.text = Objects.requireNonNull(text, "text");
        this.sites = List.copyOf(sites);
    }

    /** {@code text}, as the plan it was READ from carries it: called by the plan's records and the keyed expansion
     *  alone (the guard holds it so). The text is never edited, only typed. It declares no raise site. */
    public static PlannedSql fromPlan(String text) {
        return new PlannedSql(text, List.of());
    }

    /** {@code text} with the raise sites its plan declares for it (phase D's Phase 2, 2026-11-16): a token
     *  {@code LG#<n>} it raises is read back through them ({@code exec.RaisedErrors}). */
    public static PlannedSql fromPlan(String text, List<ExecutionPlan.Site> sites) {
        return new PlannedSql(text, sites);
    }

    /** The raise sites the plan declares for this statement, numbered 1..k: what a token it raises means. */
    public List<ExecutionPlan.Site> sites() {
        return sites;
    }

    /** Each of {@code texts}, as {@link #fromPlan} types one. */
    public static List<PlannedSql> allFromPlan(List<String> texts) {
        return texts.stream().map(PlannedSql::fromPlan).toList();
    }

    /** The statement's text, as the plan holds it: what the session sends. */
    public String text() {
        return text;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof PlannedSql p && p.text.equals(text);
    }

    @Override
    public int hashCode() {
        return text.hashCode();
    }

    @Override
    public String toString() {
        return text;
    }
}
