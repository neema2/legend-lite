// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.setup;

import com.legend.executionplan.DataRef;
import com.legend.executionplan.ExecutionPlan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A load's steps as the planner writes them, with THE BUNDLE they read (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §1.1
 * and §2, A2): statements as they are, and each table's rows as a {@link ExecutionPlan.LoadStep.Statements} step whose
 * content blob -- the rows' inserts, {@link DataRef.Statements} -- is in {@code content} by its digest. One form
 * whatever the size: where the blob's bytes travel is the plan writer's ({@code PlanJson.writeBundle}). Planning-side
 * knowledge, never plan data: a {@code Load} carries the steps and the bundle.
 *
 * @param steps   the steps, in order
 * @param content the blobs the steps read, by digest
 */
public record LoadSteps(List<ExecutionPlan.LoadStep> steps, Map<String, String> content) {

    /** No step. */
    public static final LoadSteps NONE = new LoadSteps(List.of(), Map.of());

    public LoadSteps {
        steps = List.copyOf(steps);
        content = Collections.unmodifiableMap(new LinkedHashMap<>(content));
    }

    /** Steps that read no blob. */
    public static LoadSteps of(List<ExecutionPlan.LoadStep> steps) {
        return new LoadSteps(steps, Map.of());
    }

    /** {@code statements} as one step reading them from a content blob (none for no statement): the form all model
     *  data takes, named by the blob's SHA-256; {@code of} names the data, as a refusal of a statement names it. */
    public static LoadSteps blob(List<String> statements, String of) {
        if (statements.isEmpty()) {
            return NONE;
        }
        String json = DataRef.Statements.encode(statements);
        // the portable SHA-256 (the planner runs on WebAssembly, which has no MessageDigest)
        String digest = com.legend.cache.Hash.ofUtf8(json).hex();
        DataRef ref = new DataRef(DataRef.Namespace.CONTENT, "statements/" + digest + ".json", digest,
                json.getBytes(java.nio.charset.StandardCharsets.UTF_8).length, DataRef.Statements.JSON);
        return new LoadSteps(List.of(new ExecutionPlan.LoadStep.Statements(ref, of)), Map.of(digest, json));
    }

    /**
     * A host's own file (B3, 2026-11-11: an upload, Studio's file put), read by its database's {@code reads} -- each
     * statement's one {@code ?} the file as the host locates it, never written -- with no platform limit on its size:
     * the host holds its bytes already, and the reference names its digest.
     */
    public static LoadSteps hostFile(DataRef file, List<String> reads) {
        if (file.namespace() != DataRef.Namespace.HOST) {
            throw new IllegalArgumentException("a host's file is named in the host's namespace, not " + file.namespace());
        }
        return of(List.of(new ExecutionPlan.LoadStep.File(file, new ExecutionPlan.LoadStep.Delivery.Reads(reads,
                List.of()), Integer.MAX_VALUE)));
    }

    /**
     * Each of {@code statements} as a statement whose {@code ?}s the host binds (B3's Q3, 2026-11-11): every statement
     * binds {@code binds}, in order, {@code reach} saying where its effect lives -- a secret's (the database's) or a
     * session variable's (the connection's). The binds name what the host holds, never its value.
     */
    public static LoadSteps bound(List<String> statements, List<ExecutionPlan.LoadStep.Bind> binds,
            ExecutionPlan.Reach reach) {
        List<ExecutionPlan.LoadStep> steps = new java.util.ArrayList<>();
        for (String sql : statements) {
            steps.add(new ExecutionPlan.LoadStep.Bound(sql, binds, reach));
        }
        return of(steps);
    }

    /** A value of the plan's own, bound to a host's statement: never a credential (a secret's scope). */
    public static ExecutionPlan.LoadStep.Bind value(String text) {
        return new ExecutionPlan.LoadStep.Bind.Value(text);
    }

    /** A value the host holds under {@code name}, its field {@code field}: named, never written. */
    public static ExecutionPlan.LoadStep.Bind held(String name, String field) {
        return new ExecutionPlan.LoadStep.Bind.Held(name, field);
    }

    /** {@code load}'s rows as {@code dialect} writes them, in one blob ({@link #blob}) named as the table's data: none
     *  for a load of no row. */
    public static LoadSteps rows(RowLoad load, com.legend.sql.dialect.SqlDialect dialect) {
        return rows(load, dialect, "the data of the table " + load.where());
    }

    /** {@link #rows(RowLoad, com.legend.sql.dialect.SqlDialect)} named {@code of} (a payload's elements, which their
     *  session table only holds). */
    public static LoadSteps rows(RowLoad load, com.legend.sql.dialect.SqlDialect dialect, String of) {
        return blob(load.statements(dialect), of);
    }

    /** These steps, then {@code next}'s, reading both bundles. */
    public LoadSteps then(LoadSteps next) {
        if (next.steps.isEmpty()) {
            return this;
        }
        if (steps.isEmpty()) {
            return next;
        }
        List<ExecutionPlan.LoadStep> all = new ArrayList<>(steps);
        all.addAll(next.steps);
        Map<String, String> bundle = new LinkedHashMap<>(content);
        bundle.putAll(next.content);
        return new LoadSteps(all, bundle);
    }

    /** No step at all. */
    public boolean isEmpty() {
        return steps.isEmpty();
    }

    /** Steps in order, each with the bundle it reads, as one: the planner's accumulator. */
    public static final class Builder {
        private final List<ExecutionPlan.LoadStep> steps = new ArrayList<>();
        private final Map<String, String> content = new LinkedHashMap<>();

        public Builder statement(String sql) {
            steps.add(new ExecutionPlan.LoadStep.Statement(sql));
            return this;
        }

        public Builder add(LoadSteps more) {
            steps.addAll(more.steps);
            content.putAll(more.content);
            return this;
        }

        public LoadSteps build() {
            return new LoadSteps(steps, content);
        }
    }
}
