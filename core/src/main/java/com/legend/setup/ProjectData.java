// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.setup;

import com.legend.compiler.element.ModelContext;
import com.legend.model.DataDefinition;
import com.legend.model.DatabaseDefinition;
import com.legend.protocol.Protocol;
import com.legend.sql.dialect.HostOperations;
import com.legend.sql.dialect.SqlDialect;

import java.util.ArrayList;
import java.util.List;

/**
 * A PROJECT'S OWN DATABASE (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C5): where a project version's {@code ###Data} rows
 * live on a host -- a database of their own inside the host's database (the tab's DuckDB, Python's, the server's), its
 * schemas and tables made exactly as the model declares them, so the planner's SQL runs there unchanged, and never
 * beside a person's own tables. A host makes it ({@link #open}), points a connection of its own at it ({@link #use}),
 * fills it ({@link #fill}), queries there, and drops it when idle ({@link #close}); it is rebuilt from the model the
 * next time, so dropping early is never wrong. The concept is the platform's: how a database makes one is its own
 * ({@link HostOperations}), and the rows are written by that database's dialect, as the server seeds its own
 * ({@link CsvSeed}). A database without the operations refuses by name.
 *
 * @param name  the database's name, from its content alone ({@link #contentKey}: the model's Databases and Data rows),
 *              never a caller's label -- so an edited row or table is a fresh database, a project whose rows are
 *              unchanged keeps its own whatever else changed, and a version republished under the same coordinates
 *              (a SNAPSHOT) with new rows is never answered with the old
 * @param open  the statements that make it, empty
 * @param use   the statements that point a connection of its own at it
 * @param fill  the statements, run on that connection, that make every table a relational Data element fills and put
 *              its rows in: in the model's order, each table the planner's seed for it ({@link CsvSeed#sqls})
 * @param close the statements that drop it with everything in it
 */
public record ProjectData(String name, List<String> open, List<String> use, List<String> fill, List<String> close) {

    /** The prefix of every project's database: no store schema spells it, so none can capture a model's name. */
    public static final String PREFIX = "__project_";

    public ProjectData {
        open = List.copyOf(open);
        use = List.copyOf(use);
        fill = List.copyOf(fill);
        close = List.copyOf(close);
    }

    /** The project's database for the compiled model {@code ctx} on {@code dialect}'s database. */
    public static ProjectData of(ModelContext ctx, SqlDialect dialect) {
        HostOperations host = HostOperations.of(dialect);
        String name = PREFIX + com.legend.cache.Hash.ofUtf8(contentKey(ctx)).hex().substring(0, 16);
        return new ProjectData(name, host.openDatabase(name), host.useDatabase(name), fill(ctx, dialect),
                host.closeDatabase(name));
    }

    /**
     * What a project's database holds, as text: every Database the model declares (its tables' shapes, which the fill
     * makes them with) and its Data rows ({@link #rowsKey}). Two models that agree on both hold the same database.
     */
    public static String contentKey(ModelContext ctx) {
        return storesKey(ctx) + "--\n" + rowsKey(ctx);
    }

    /** What the model's stores are, as text: every Database it declares (its tables' shapes). The platform's own
     *  database is keyed by this alone: it is filled with nothing. */
    public static String storesKey(ModelContext ctx) {
        StringBuilder out = new StringBuilder();
        ctx.databases().sorted(java.util.Comparator.comparing(DatabaseDefinition::qualifiedName))
                .forEach(db -> out.append(db).append('\n'));
        return out.toString();
    }

    /**
     * What {@code ctx}'s Data rows are, as text: every relational Data element's tables (schema, table, rows), in
     * order -- empty when it has none. A host keeping a database filled from them keys it by this (legend-lite's server:
     * an edited row is a fresh database).
     */
    public static String rowsKey(ModelContext ctx) {
        StringBuilder out = new StringBuilder();
        for (DataDefinition data : ctx.dataElements().toList()) {
            if (data.body().value() instanceof Protocol.PRelationalCsvData csv) {
                for (Protocol.PRelationalCsvTable t : csv.tables()) {
                    out.append(t.schema()).append('\n').append(t.table()).append('\n')
                            .append(t.values() == null ? "" : t.values()).append("\n-\n");
                }
            }
        }
        return out.toString();
    }

    /**
     * Why {@code ctx}'s Data rows cannot make a project's database, or null where they can: a relational Data element
     * with no rows, not even a header, or a table it fills that the model's Databases declare none or more than once of
     * ({@link #declaring}). A plan carries it as data (the plan-as-data design's slice 5, 2026-11-09): the server
     * refuses it where it makes the project's database, as it did before hole 7, and a query whose session is not the
     * project's -- a test's, on its own database -- never meets it.
     */
    public static @com.legend.base.Nullable String refusal(ModelContext ctx) {
        for (DataDefinition data : ctx.dataElements().toList()) {
            if (!(data.body().value() instanceof Protocol.PRelationalCsvData csv)) {
                continue;
            }
            for (Protocol.PRelationalCsvTable t : csv.tables()) {
                if (t.values() == null) {
                    return "the Data element " + data.qualifiedName() + " fills " + t.schema() + "." + t.table()
                            + " with no rows, not even a header";
                }
                List<String> declaring = declaringAll(ctx, t.schema(), t.table());
                if (declaring.size() != 1) {
                    return "the Data element " + data.qualifiedName() + " fills " + t.schema() + "." + t.table()
                            + (declaring.isEmpty() ? ", which no Database of the model declares"
                                    : ", which more than one Database declares " + declaring);
                }
            }
        }
        return null;
    }

    /** Every table a relational Data element fills, made and filled by {@code dialect}'s statements, in order; refused
     *  by name where the rows cannot make the database ({@link #refusal}). */
    /** {@link #fillSteps} as statement text: the host's own project database's ({@link #of}, census M14, until L7);
     *  a plan's fill never takes this text (L6c, 2026-11-10: census M1 DELETED). */
    public static List<String> fill(ModelContext ctx, SqlDialect dialect) {
        return fillSteps(ctx, dialect).stream().map(st -> st.text(dialect)).toList();
    }

    /** The seed steps that fill a project's database, the rows still rows: a plan's fill takes each table's rows inline,
     *  or from its bundle above the inline threshold ({@code planner.ContentRows}, L6c). */
    public static List<CsvSeed.Step> fillSteps(ModelContext ctx, SqlDialect dialect) {
        String refused = refusal(ctx);
        if (refused != null) {
            throw new IllegalArgumentException(refused);
        }
        List<CsvSeed.Step> out = new ArrayList<>();
        for (DataDefinition data : ctx.dataElements().toList()) {
            // only rows a relational store holds: a model store's or an external format's data is not a table's
            if (!(data.body().value() instanceof Protocol.PRelationalCsvData csv)) {
                continue;
            }
            for (Protocol.PRelationalCsvTable t : csv.tables()) {
                String database = declaringAll(ctx, t.schema(), t.table()).get(0);
                out.addAll(CsvSeed.steps(t.schema() + "\n" + t.table() + "\n" + t.values(), database, ctx, dialect));
            }
        }
        return out;
    }

    /**
     * The Databases of the model that themselves declare {@code schema.table} ({@code default}: a table of its default
     * schema). A fill takes the one; none, or more than one, is {@link #refusal}'s: which one would be a guess -- a test
     * names its store, and a project's own database has no test to ask.
     */
    private static List<String> declaringAll(ModelContext ctx, String schema, String table) {
        List<String> declaring = new ArrayList<>();
        for (DatabaseDefinition db : ctx.databases().toList()) {
            List<DatabaseDefinition.TableDefinition> tables = "default".equals(schema) ? db.defaultSchemaTables()
                    : db.schemas().stream().filter(s -> s.name().equals(schema))
                            .flatMap(s -> s.tables().stream()).toList();
            if (tables.stream().anyMatch(d -> d.name().equals(table))) {
                declaring.add(db.qualifiedName());
            }
        }
        return declaring;
    }
}
