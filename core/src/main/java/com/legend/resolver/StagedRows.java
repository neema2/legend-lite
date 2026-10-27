// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedTableReference;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The let of a program's staged rows (lite's {@code stage}, docs/WRITES_DESIGN_2026_10_18.md §8, leg 6): what the
 * statements after {@code let s = rows->stage()} read for {@code $s}, once execution has computed the rows into a
 * temporary table of the session -- that table, a relation of the staged rows' columns. A physical reference,
 * minted here with the resolver's other table reads; execution names the table and the store.
 */
public final class StagedRows {

    private StagedRows() {
    }

    /** {@code let name} bound to the session's temporary table {@code table}, a relation of the staged rows' columns
     *  ({@code rows}), each named as its projection named it (delimited, so its case holds on a database that folds
     *  an undelimited name); {@code store} is the store the table is read under, {@code letInfo} the let's type. */
    public static TypedLet boundTo(String name, String table, ExprType rows, String store, ExprType letInfo) {
        Set<String> columns = new LinkedHashSet<>(columns(rows));
        return new TypedLet(name, new TypedTableReference(store, table, rows, true, null, columns, false, Map.of(),
                false, false), letInfo);
    }

    /** The temporary table of the session a staged let's rows are held in: the let's name under a prefix of the
     *  platform's own, so no table of a store is named. */
    public static String table(String let) {
        return "legend_stage_" + let;
    }

    /** The rows {@code let s = rows->stage()} stages, or null for any other statement. */
    public static com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedSpec rowsOf(
            com.legend.compiler.spec.typed.TypedSpec statement) {
        if (!(statement instanceof TypedLet let)) {
            return null;
        }
        com.legend.compiler.spec.typed.TypedSpec rhs = let.value();
        while (rhs instanceof com.legend.compiler.spec.typed.TypedFrom rf) {
            rhs = rf.source();
        }
        return rhs instanceof com.legend.compiler.spec.typed.TypedNativeCall sc
                && com.legend.builtin.Pure.Lite.STAGE.equals(sc.callee().qualifiedName()) ? sc.args().get(0) : null;
    }

    /** The store a staged let's table is read under: the one store its rows read ({@code resolved}, the rows' body),
     *  else (rows of no table) the one {@code runtime} binds; the session's temporary table is that store's
     *  connection's. */
    public static String storeOf(java.util.List<com.legend.compiler.spec.typed.TypedSpec> resolved,
            com.legend.compiler.element.ModelContext ctx, @com.legend.base.Nullable String runtime) {
        Set<String> stores = new java.util.TreeSet<>();
        java.util.ArrayDeque<com.legend.compiler.spec.typed.TypedSpec> work = new java.util.ArrayDeque<>(resolved);
        while (!work.isEmpty()) {
            com.legend.compiler.spec.typed.TypedSpec n = work.poll();
            if (n instanceof TypedTableReference tr) {
                stores.add(tr.store());
            }
            work.addAll(n.children());
        }
        if (stores.size() == 1) {
            return stores.iterator().next();
        }
        Set<String> bound = runtime == null ? Set.of()
                : ctx.findRuntime(runtime).map(rt -> rt.connectionBindings().keySet()).orElse(Set.of());
        if (stores.isEmpty() && bound.size() == 1) {
            return bound.iterator().next();
        }
        throw new com.legend.error.NotImplementedException("rows staged (stage) that read " + stores + " under a"
                + " runtime binding " + bound + ": a staged let's table is one store's");
    }

    /**
     * A program's lets checked against its writes (PR #67's review, 2026-10-22). A let's value is computed where the
     * let is read, if it is read at all (the body compiler and the plan maker inline it), where a staged let's rows
     * are computed at its turn. So, by name:
     * <ul>
     *   <li>a let whose value writes a store table is refused: its write would run where the let is read, or never
     *       ({@code let k = rows->write(t); ...} had written nothing);</li>
     *   <li>a let whose value reads a table a later write changes, read after that write, is refused: it would read
     *       the rows as the write left them, not as they were at the let; staged rows are those.</li>
     * </ul>
     * A read of a class, a class write and a user function's write are taken to touch every table (their tables are
     * the mapping's, known after the store resolver).
     */
    public static void refuseLetsTheWritesChange(java.util.List<com.legend.compiler.spec.typed.TypedSpec> body,
            com.legend.compiler.spec.SpecCompiler specs) {
        java.util.Map<String, PlainLet> lets = new java.util.HashMap<>();
        java.util.List<Touched> writes = new java.util.ArrayList<>();
        java.util.Map<com.legend.model.FunctionId, Boolean> memo = new java.util.HashMap<>();
        for (int j = 0; j < body.size(); j++) {
            com.legend.compiler.spec.typed.TypedSpec stmt = body.get(j);
            boolean plainLet = stmt instanceof TypedLet && rowsOf(stmt) == null && j < body.size() - 1;
            if (plainLet && stmt instanceof TypedLet let) {
                if (com.legend.compiler.spec.StatementEffects.writesStore(let.value(), specs, memo)) {
                    throw new com.legend.error.NotImplementedException("the let '" + let.name() + "' has a write for"
                            + " its value: a let's value is computed where the let is read, if it is read at all, so"
                            + " its write would run there or never; write it as a statement of its own");
                }
                lets.put(let.name(), new PlainLet(j, touched(let.value(), specs, new java.util.HashMap<>(), false),
                        com.legend.compiler.spec.typed.FreeVars.of(let.value())));
                writes.add(Touched.NONE);
                continue;
            }
            // a read site: each let it reads, and the lets their values read, computed here
            java.util.ArrayDeque<String> work = new java.util.ArrayDeque<>(
                    com.legend.compiler.spec.typed.FreeVars.of(stmt));
            Set<String> seen = new java.util.HashSet<>();
            while (!work.isEmpty()) {
                String name = work.poll();
                PlainLet read = lets.get(name);
                if (read == null || !seen.add(name)) {
                    continue;
                }
                for (int k = read.at() + 1; k < j; k++) {
                    if (writes.get(k).meets(read.reads())) {
                        throw new com.legend.error.NotImplementedException("the let '" + name + "' reads rows the"
                                + " write at statement " + (k + 1) + " changes, and statement " + (j + 1) + " reads it"
                                + " after that write: a let's value is computed where the let is read, so it would"
                                + " read the rows as the write left them; stage them (let " + name + " = rows"
                                + "->meta::legend::lite::stage()) for the rows as they were at the let");
                    }
                }
                work.addAll(read.uses());
            }
            writes.add(com.legend.compiler.spec.StatementEffects.writesStore(stmt, specs, memo)
                    ? touched(stmt, specs, new java.util.HashMap<>(), true) : Touched.NONE);
        }
    }

    /** A let whose value is inlined where it is read: its statement's place, the tables its value reads, and the
     *  variables its value reads (the lets before it). */
    private record PlainLet(int at, Touched reads, Set<String> uses) {
    }

    /** The tables a term reads ({@code writes} false) or writes ({@code writes} true), each {@code store.table};
     *  {@code any} when they are not known before the store resolver (a class, a class write, a user function's). */
    private record Touched(Set<String> tables, boolean any) {
        static final Touched NONE = new Touched(Set.of(), false);

        boolean meets(Touched other) {
            boolean touches = any || !tables.isEmpty();
            boolean otherTouches = other.any || !other.tables.isEmpty();
            return (any && otherTouches) || (other.any && touches)
                    || tables.stream().anyMatch(other.tables::contains);
        }
    }

    private static Touched touched(com.legend.compiler.spec.typed.TypedSpec term,
            com.legend.compiler.spec.SpecCompiler specs,
            java.util.Map<com.legend.model.FunctionId, Touched> memo, boolean writes) {
        Set<String> tables = new java.util.TreeSet<>();
        boolean any = false;
        java.util.ArrayDeque<com.legend.compiler.spec.typed.TypedSpec> work = new java.util.ArrayDeque<>();
        work.add(term);
        while (!work.isEmpty()) {
            com.legend.compiler.spec.typed.TypedSpec n = work.poll();
            if (writes) {
                if (n instanceof com.legend.compiler.spec.typed.TypedWrite w && w.destination().isPresent()
                        && w.destination().get() instanceof TypedTableReference tr) {
                    tables.add(tr.store() + "." + tr.table());
                } else if (n instanceof com.legend.compiler.spec.typed.TypedStoreWrite sw) {
                    tables.add(sw.table().store() + "." + sw.table().table());
                } else if (com.legend.compiler.spec.StatementEffects.isStoreWrite(n)) {
                    any = true;   // a class write: its table is its mapping's
                }
            } else if (n instanceof TypedTableReference tr) {
                tables.add(tr.store() + "." + tr.table());
            } else if (n instanceof com.legend.compiler.spec.typed.TypedGetAll) {
                any = true;   // a class's rows: its tables are its mapping's
            }
            if (n instanceof com.legend.compiler.spec.typed.TypedUserCall uc
                    && com.legend.builtin.Subsumed.of(uc.callee().qualifiedName()).isEmpty()) {
                Touched callee = memo.get(uc.callee().id());
                if (callee == null) {
                    memo.put(uc.callee().id(), new Touched(Set.of(), true));   // in progress: a cycle touches any
                    Set<String> inner = new java.util.TreeSet<>();
                    boolean innerAny = false;
                    try {
                        for (com.legend.compiler.spec.typed.TypedSpec s : specs.compile(uc.callee()).body()) {
                            Touched t = touched(s, specs, memo, writes);
                            inner.addAll(t.tables());
                            innerAny |= t.any();
                        }
                    } catch (com.legend.compiler.spec.TypeInferenceException e) {
                        innerAny = true;   // an un-typeable callee cannot run; taken to touch any table
                    }
                    callee = new Touched(inner, innerAny);
                    memo.put(uc.callee().id(), callee);
                }
                tables.addAll(callee.tables());
                any |= callee.any();
            }
            work.addAll(n.children());
        }
        return new Touched(tables, any);
    }

    /** The staged rows' columns ({@code rows}, a relation), in order, as their projection names them: the session's
     *  temporary table's, which the statements after read by these names (writes leg 4, 2026-10-20: a dialect whose
     *  database folds a stored column's name names the table's columns so). */
    public static java.util.List<String> columns(ExprType rows) {
        return Objects.requireNonNull(Type.schemaView(rows.type()), "staged rows are a relation").columns().stream()
                .map(Type.Column::name).toList();
    }
}
