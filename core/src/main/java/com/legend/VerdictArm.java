// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend;

import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.exec.ExecutionResult;

import java.util.List;

/**
 * ONE ARM OF THE VERDICT SEAM (cleanup move 2b, 2026-09-21): the router
 * ({@link AssertVerdicts}) classifies a statement-root assert (arity, the text and
 * rendered-text routes, the static identity gate, the order view) and hands the
 * sides to the arm the run's judge mode names ONCE per adjudication: the host arm
 * (both sides executed, Java compares the rows; the host verdict of record) or the
 * database arm (both sides planned, one verdict statement, the database returns
 * the row). One method per assert family; a null return means "not this shape,
 * the generic path continues", exactly as the inline bodies did.
 */
interface VerdictArm {

    /** Null = a non-tabular shape (the host arm): fall through, loud later. */
    @com.legend.base.Nullable ExecutionResult tdsEquivalent(String name, List<TypedSpec> targs, List<TypedSpec> letPrefix,
            SpecCompiler specs, StatementExecutor.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook);

    ExecutionResult size(String name, List<TypedSpec> args, List<TypedSpec> letPrefix,
            SpecCompiler specs, StatementExecutor.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook);

    @com.legend.base.Nullable ExecutionResult contains(String name, List<TypedSpec> args, List<TypedSpec> letPrefix,
            SpecCompiler specs, StatementExecutor.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook);

    ExecutionResult tolerance(String name, List<TypedSpec> args, List<TypedSpec> letPrefix,
            SpecCompiler specs, StatementExecutor.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook);

    /** {@code assert(cond)} / {@code assertFalse(cond)}. */
    ExecutionResult condition(String name, TypedSpec cond, boolean wantTrue, List<TypedSpec> letPrefix,
            SpecCompiler specs, StatementExecutor.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook);

    ExecutionResult empty(String name, TypedSpec arg, boolean wantEmpty, List<TypedSpec> letPrefix,
            SpecCompiler specs, StatementExecutor.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook);

    /** {@code assertEq(e, a)}. */
    ExecutionResult eq(String name, List<TypedSpec> args, List<TypedSpec> letPrefix,
            SpecCompiler specs, StatementExecutor.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook);

    /** {@code assertInstanceOf(v, type)}; null = a shape this arm leaves to the generic path. */
    @com.legend.base.Nullable ExecutionResult instanceOf(String name, List<TypedSpec> args, List<TypedSpec> letPrefix,
            SpecCompiler specs, StatementExecutor.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook);

    /** {@code assertIs(a, b)} once the static identity gate declined; null = not adjudicable here. */
    @com.legend.base.Nullable ExecutionResult is(String name, List<TypedSpec> args, List<TypedSpec> letPrefix,
            SpecCompiler specs, StatementExecutor.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook);

    /** {@code assertSameElements(e, a)}; {@code gridPair}: a side is statically tabular. */
    ExecutionResult sameElements(String name, List<TypedSpec> args, boolean gridPair, List<TypedSpec> letPrefix,
            SpecCompiler specs, StatementExecutor.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook);

    /** A quantified assert's predicate VECTOR ({@code xs->map(x | pred)}): every element true / false. */
    ExecutionResult quantified(String fqn, TypedSpec predMap, boolean wantTrue, String message, List<TypedSpec> letPrefix,
            SpecCompiler specs, StatementExecutor.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook);

    /** A quantified assert that meets THE VECTOR CONTRACT (VerdictQueries.vectorContract): the
     * per-element assert is the predicate it means over a row source. The database arm plans the
     * vector and judges it in the fused statement; the host arm returns null (it fetches and
     * unrolls by design). 2026-09-22. */
    @com.legend.base.Nullable ExecutionResult quantifiedVector(String fqn, TypedSpec predMap, List<TypedSpec> letPrefix,
            SpecCompiler specs, StatementExecutor.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook);

    /** The sorted flat-cells idiom of {@code assertEquals} ({@code $r.values.rows.values->sort()}
     * against a pool): the CELL-POOL multiset. Null (the host arm) = neither side is a grid,
     * the ordinary equality continues. */
    @com.legend.base.Nullable ExecutionResult cellPool(String name, TypedSpec cellsE, TypedSpec cellsA,
            List<TypedSpec> letPrefix, SpecCompiler specs, StatementExecutor.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook);

    /** {@code assertEquals} / {@code assertNotEquals} after the text, rendered-text, cell-pool
     * and grid-pair routes declined; {@code incidental}: the actual side's order is SQL arrival
     * order (the judgment is order-insensitive); {@code gridPair}: a side is statically tabular. */
    ExecutionResult equals(String name, boolean wantEqual, List<TypedSpec> args, boolean incidental,
            boolean gridPair, List<TypedSpec> letPrefix, SpecCompiler specs, StatementExecutor.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook);

    /** {@code assertJsonStringsEqual(golden, actual)}; null = a non-string shape (host arm). */
    @com.legend.base.Nullable ExecutionResult jsonStringsEqual(String name, List<TypedSpec> args,
            List<TypedSpec> letPrefix, SpecCompiler specs, StatementExecutor.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook);

    /** The RENDERED-TEXT pair (a toCSV / toString / joinStrings render of a grid against its
     * golden): {@code form} the render grammar, {@code rendered} the rendered side,
     * {@code eForm} / {@code aForm} which sides render, {@code orderedForm} whether the
     * assert is ordered. */
    ExecutionResult rendered(String name, boolean wantEqual, List<TypedSpec> args, String form,
            TypedSpec rendered, @com.legend.base.Nullable String eForm, @com.legend.base.Nullable String aForm,
            boolean orderedForm, List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook);

    /** The static identity gate decided {@code assertIs} at compile time (a census fact). */
    void staticallyDecided(String name);
}
