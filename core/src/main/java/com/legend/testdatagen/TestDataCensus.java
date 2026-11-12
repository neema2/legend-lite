// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testdatagen;

import com.legend.compiler.element.ModelContext;
import com.legend.lineage.ScanRelations;
import com.legend.model.DatabaseDefinition;
import com.legend.model.RelationalOperation;
import com.legend.protocol.spec.LambdaFunction;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * THE TEST-DATA CENSUS AS A PLANNING CONSTANT (B2d.3 of the total-wall program, 2026-11-11; the design's §3.2): upstream's
 * {@code getRelationalCSVDataFromQuery(query, mapping)} ({@code testDataGeneration.pure:753-777}) is a fact of the MODEL
 * -- the tables a query reads through its mapping and, per table, the columns a test's data for it needs -- so it is
 * computed at planning (the charter's "build-time model text -> typed constants") and the carrier is replaced by the
 * instance it is (typed in the compiler layer): a {@code RelationalCSVData} whose {@code tables} are the reached tables sorted by schema and name, each
 * a {@code RelationalCSVTable} whose {@code values} are its column names joined by commas -- its primary key sorted by
 * name, then the columns the query reads of it sorted by name (the values its mapping reads and both sides of every
 * join), then its milestoning date columns sorted by name, without repeats. Upstream reads the columns through its
 * property tree ({@code scanProperties}, {@code scanColumns}); lite through its one lineage, the relation tree
 * ({@link ScanRelations#relTree}): a column on which the two disagree is posted, never patched. A view's columns are
 * the view's, not a table's: upstream keeps tables only. Nothing is executed.
 */
public final class TestDataCensus {

    private TestDataCensus() {
    }

    /** One table of the census: its schema, its name and its column names in upstream's order. */
    public record Table(String schema, String table, List<String> columns) {
        public Table {
            columns = List.copyOf(columns);
        }
    }

    /** The census of {@code query} over {@code mappingFqn}. */
    public static List<Table> of(ModelContext ctx, LambdaFunction query, String mappingFqn) {
        Map<String, Reached> reached = new LinkedHashMap<>();
        for (ScanRelations.Rel r : ScanRelations.relTree(ctx, query, mappingFqn)) {
            walk(ctx, r, null, reached);
        }
        List<Table> out = new ArrayList<>(reached.size());
        for (Reached t : reached.values()) {
            DatabaseDefinition.TableDefinition def = t.loc().def();
            Set<String> milestoning = milestoning(def);
            List<String> columns = new ArrayList<>();
            def.columns().stream().filter(DatabaseDefinition.ColumnDefinition::primaryKey)
                    .map(DatabaseDefinition.ColumnDefinition::name).sorted().forEach(columns::add);
            t.read().stream().filter(c -> !milestoning.contains(c)).forEach(columns::add);
            new TreeSet<>(milestoning).forEach(columns::add);
            out.add(new Table(t.loc().schema(), t.table(), List.copyOf(new LinkedHashSet<>(columns))));
        }
        out.sort(java.util.Comparator.comparing(t -> t.schema() + t.table()));
        return out;
    }

    /** {@link #of} as the rows of the instance upstream returns: each table's schema, name and column names joined by
     *  commas (the compiler types it: {@code GeneratorSplice.relationalCsvData}). */
    public static List<List<String>> rows(ModelContext ctx, LambdaFunction query, String mappingFqn) {
        List<List<String>> out = new ArrayList<>();
        for (Table t : of(ctx, query, mappingFqn)) {
            out.add(List.of(t.schema(), t.table(), String.join(",", t.columns())));
        }
        return out;
    }

    /** A table the query reaches: where it is and the columns read of it, sorted by name. */
    private record Reached(String table, TestDataGenerator.Located loc, TreeSet<String> read) {
    }

    private static void walk(ModelContext ctx, ScanRelations.Rel rel, ScanRelations.@com.legend.base.Nullable Rel parent,
            Map<String, Reached> reached) {
        if (!ScanRelations.isView(ctx, rel.db(), rel.table())) {
            TestDataGenerator.Located loc = TestDataGenerator.locate(ctx, rel.db(), rel.table());
            String table = java.util.Objects.requireNonNull(rel.table(), "rel.table()");
            Reached t = reached.computeIfAbsent(loc.schema() + "\n" + table,
                    k -> new Reached(table, loc, new TreeSet<>()));
            Set<String> known = new LinkedHashSet<>();
            loc.def().columns().forEach(c -> known.add(c.name()));
            TreeSet<String> read = new TreeSet<>();
            for (String c : rel.cols()) {
                read.add(c);
            }
            if (rel.cond() != null || rel.joinName() != null) {
                RelationalOperation op = rel.cond() != null ? rel.cond()
                        : TestDataGenerator.findJoin(ctx, rel.joinName(), rel.db(),
                                parent == null ? rel.db() : parent.db()).operation();
                TestDataGenerator.collectTableCols(op, table, read, known);
            }
            for (ScanRelations.Rel child : rel.children()) {
                RelationalOperation op = child.cond() != null ? child.cond()
                        : TestDataGenerator.findJoin(ctx, child.joinName(), child.db(), rel.db()).operation();
                TestDataGenerator.collectTableCols(op, table, read, known);
            }
            // a read name is the model's: the declared column of that exact name (TestDataGenerator.column)
            for (String c : read) {
                t.read().add(TestDataGenerator.column(loc.def(), c).name());
            }
        }
        for (ScanRelations.Rel child : rel.children()) {
            walk(ctx, child, rel, reached);
        }
    }

    /** The milestoning date columns {@code def} declares. */
    private static Set<String> milestoning(DatabaseDefinition.TableDefinition def) {
        Set<String> out = new LinkedHashSet<>();
        var ms = def.milestoning();
        if (ms == null) {
            return out;
        }
        if (ms.business() != null) {
            addIf(out, ms.business().from());
            addIf(out, ms.business().thru());
            addIf(out, ms.business().snapshotDate());
        }
        if (ms.processing() != null) {
            addIf(out, ms.processing().in());
            addIf(out, ms.processing().out());
            addIf(out, ms.processing().snapshotDate());
        }
        return out;
    }

    private static void addIf(Set<String> out, @com.legend.base.Nullable String c) {
        if (c != null) {
            out.add(c);
        }
    }
}
