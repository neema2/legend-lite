// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.executionplan.ExecutionPlan;
import com.legend.model.DatabaseDefinition;
import com.legend.protocol.Protocol;
import com.legend.sql.dialect.HostOperation;
import com.legend.sql.dialect.HostOperations;
import com.legend.sql.dialect.SqlDialect;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * INGEST PLANS (docs/DESK_DATA_ROADMAP_2026_10_29.md C0; the user's go of 2026-11-14): how a dataset's rows land in a
 * host's database, planned ONCE from the ingest's own declaration and run by the host for every batch -- the platform's
 * answer to main's total wall (data enters a database only through a planned load) for rows that keep arriving. Three
 * HOST_SESSION loads, each a plan:
 *
 * <ul>
 *   <li><b>make</b>: the table as the ingest declares it, empty (the seed's statements for its header alone, as a host's
 *   file and a synthesized table are made: {@link LoadPlanner#hostFile}, {@link Synthesize});</li>
 *   <li><b>land</b>: one batch -- the value the host holds under {@code held}, attached by name as the planner's own
 *   relation (the host holds it, the plan never does), put in by the dataset's write mode ({@code append_only} an insert
 *   from it, {@code batch_milestoned} kept current an upsert: each key's last row of the batch replacing the table's
 *   version, its milestoning columns computed by the database, a row the ingest's delete indicator marks removing its
 *   key; docs/DEPLOYMENT_DESIGN_2026_11_14.md §5), the attached copy dropped before and after;</li>
 *   <li><b>keep</b>, where the dataset keeps a retention: {@code LastRows(n)} the rows older than the latest n by arrival
 *   deleted, run when a tenth of n has arrived since the last ({@link Plans#keepEvery}, data the host reads).</li>
 * </ul>
 *
 * The host writes no statement and knows no write mode. A table no ingest makes, a write mode or a retention the
 * database cannot land, is refused by name.
 */
public final class IngestPlanner {

    private IngestPlanner() {
    }

    /** The three plans of one dataset's landing, and how many rows arrive between two runs of {@code keep} (null with
     *  it, for a dataset that keeps everything). */
    public record Plans(ExecutionPlan make, ExecutionPlan land, @com.legend.base.Nullable ExecutionPlan keep,
            @com.legend.base.Nullable Long keepEvery) {
        public Plans {
            if ((keep == null) != (keepEvery == null)) {
                throw new IllegalArgumentException("a keep plan runs at a cadence, and a cadence is a keep plan's");
            }
        }
    }

    /** The relation a batch is attached as, before it is put in: the planner's own name, one a table. */
    static String landingOf(String table) {
        return "__land_" + table;
    }

    /**
     * The plans that land rows in {@code table.schema().table()} of {@code table.database()} -- a table an ingest's
     * dataset makes, reached from that database through its includes ({@code include Ingest x::Y}) -- on a database
     * {@code dialect} writes, each batch attached under {@code held}.
     */
    public static Plans plans(SqlDialect dialect, com.legend.executionplan.DatabaseType type,
            LoadPlanner.DeclaredTable table, String held) {
        return plans(dialect, type, table, held, null);
    }

    /**
     * {@link #plans}, what the table keeps decided by {@code deployment} (docs/DEPLOYMENT_DESIGN_2026_11_14.md §8, step
     * 5): its live target's {@code keep} for the dataset, never the ingest's {@code retention}; a dataset the deployment
     * does not land refused by name. Null: the ingest's own retention.
     */
    public static Plans plans(SqlDialect dialect, com.legend.executionplan.DatabaseType type,
            LoadPlanner.DeclaredTable table, String held, @com.legend.base.Nullable String deployment) {
        String name = "default".equals(table.schema()) ? table.table() : table.schema() + "." + table.table();
        DatabaseDefinition.TableDefinition def = table.ctx().findTableDefinition(table.database(), name)
                .orElseThrow(() -> new IllegalArgumentException("an ingest's plans: the Database " + table.database()
                        + " declares no table " + table.schema() + "." + table.table()));
        com.legend.model.IngestDefinition ingest = ingestOf(table, name);
        Protocol.PIngestDataset dataset = ingest.ingest().datasets().stream()
                .filter(d -> table.table().equals(d.name())).findFirst()
                .orElseThrow(() -> new IllegalStateException("the ingest " + ingest.qualifiedName() + " owns the table "
                        + table.table() + " and has no dataset of its name"));
        HostOperations host = HostOperations.of(dialect);
        HostOperation.Name target = HostOperation.Name.declared(table.schema(), table.table());
        List<HostOperation.Column> columns = def.columns().stream()
                .map(c -> new HostOperation.Column(c.name(), c.name())).toList();
        // a batch's own columns: the milestoning columns are the landing's, never a delivery's
        List<String> lake = List.of(com.legend.protocol.IngestWords.LAKE_DIGEST,
                com.legend.protocol.IngestWords.LAKE_IN_ID, com.legend.protocol.IngestWords.LAKE_OUT_ID);
        List<HostOperation.Column> own = columns.stream().filter(c -> !lake.contains(c.target())).toList();
        ExecutionPlan.Target into = Targets.of(new ExecutionPlan.Database.Platform(type, null), dialect);

        // make: the table as declared, empty
        com.legend.setup.LoadSteps made = com.legend.setup.CsvSeed.planned(com.legend.setup.CsvSeed.steps(
                table.schema() + "\n" + table.table() + "\n" + String.join(",", columns.stream()
                        .map(HostOperation.Column::target).toList()),
                table.database(), table.ctx(), dialect), dialect);

        // land: the batch attached as the planner's relation, put in by the write mode, the relation dropped around it
        HostOperation.Name landing = HostOperation.Name.of(landingOf(table.table()));
        HostOperation.Source.Relation from = new HostOperation.Source.Relation(landing);
        String mode = ingest.writeModeOf(dataset);
        HostOperation put = switch (mode == null ? "" : mode) {
            case "append_only" -> new HostOperation.Insert(target, columns, from);
            case "batch_milestoned" -> {
                Protocol.PIngestDeleteIndicator d = ingest.ingest().readMode() == null ? null
                        : ingest.ingest().readMode().deleteIndicator();
                // the key the table declares: written, or the read table's (IngestDefinition.database)
                List<String> key = def.columns().stream().filter(DatabaseDefinition.ColumnDefinition::primaryKey)
                        .map(DatabaseDefinition.ColumnDefinition::name).toList();
                yield new HostOperation.Upsert(target, own, key, from,
                        new HostOperation.Milestones(com.legend.protocol.IngestWords.LAKE_DIGEST,
                                com.legend.protocol.IngestWords.LAKE_IN_ID, com.legend.protocol.IngestWords.LAKE_OUT_ID,
                                com.legend.protocol.IngestWords.OPEN_BATCH_ID),
                        d == null ? null : new HostOperation.DeleteWhen(d.deleteField(), d.deleteValues()));
            }
            default -> throw new IllegalArgumentException("the ingest " + ingest.qualifiedName() + "'s dataset "
                    + dataset.name() + " lands " + (mode == null ? "by no write mode" : "as " + mode)
                    + ": an ingest's plans land appended (AppendOnly) or keyed, each key's current version kept"
                    + " (BatchMilestoned)");
        };
        com.legend.setup.LoadSteps.Builder landed = new com.legend.setup.LoadSteps.Builder();
        List<String> drop = host.statements(new HostOperation.Drop(HostOperation.Kind.TABLE, landing));
        drop.forEach(landed::statement);
        landed.add(com.legend.setup.LoadSteps.attach(held, landingOf(table.table()), ExecutionPlan.Reach.DATABASE));
        host.statements(put).forEach(landed::statement);
        drop.forEach(landed::statement);

        // keep: the retention, by arrival -- the deployment's keep, or the ingest's own retention
        ExecutionPlan keep = null;
        Long every = null;
        Long kept = deployment == null ? null : DeploymentPlanner.keptRows(table.ctx(), deployment, ingest, dataset.name());
        if (kept != null || deployment == null && dataset.retention() != null) {
            long rows = kept != null ? kept : switch (java.util.Objects.requireNonNull(dataset.retention())) {
                case Protocol.PIngestLastRows r -> r.rows();
                case Protocol.PIngestLast l -> throw new IllegalArgumentException("the ingest "
                        + ingest.qualifiedName() + "'s dataset " + dataset.name() + " keeps Last('" + l.duration()
                        + "'), which reads when each row arrived, and a table here records no arrival time: keep its"
                        + " latest rows (LastRows(n))");
            };
            com.legend.setup.LoadSteps.Builder trimmed = new com.legend.setup.LoadSteps.Builder();
            host.statements(new HostOperation.Bound(target, rows))
                    .forEach(trimmed::statement);
            keep = ExecutionPlan.writing(List.of(), LoadPlanner.load(ExecutionPlan.Load.Cadence.HOST_SESSION,
                    trimmed.build().steps(), into));
            every = Math.max(1, rows / 10);
        }
        return new Plans(
                ExecutionPlan.writing(List.of(), LoadPlanner.load(ExecutionPlan.Load.Cadence.HOST_SESSION,
                        made.steps(), into, made.content())),
                ExecutionPlan.writing(List.of(), LoadPlanner.load(ExecutionPlan.Load.Cadence.HOST_SESSION,
                        landed.build().steps(), into)),
                keep, every);
    }

    /** The ingest whose database owns {@code name}, found from {@code table.database()} through its includes; a table
     *  of a database no ingest is refused by name. */
    private static com.legend.model.IngestDefinition ingestOf(LoadPlanner.DeclaredTable table, String name) {
        ArrayDeque<String> next = new ArrayDeque<>(List.of(table.database()));
        Set<String> seen = new HashSet<>();
        while (!next.isEmpty()) {
            String db = next.poll();
            if (!seen.add(db)) {
                continue;
            }
            if (table.ctx().findOwnTableDefinition(db, name).isPresent()) {
                return table.ctx().findIngest(db).orElseThrow(() -> new IllegalArgumentException("the table "
                        + table.table() + " is the Database " + db + "'s own, not an ingest's dataset: its rows land by"
                        + " no declaration (an ingest's plans land a dataset's rows)"));
            }
            table.ctx().findDatabase(db).ifPresent(d -> next.addAll(d.includes()));
        }
        throw new IllegalStateException("the table " + name + " is declared by " + table.database()
                + " and owned by none of its includes");
    }
}
