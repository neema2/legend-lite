// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.element.ModelContext;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionSpecification;
import com.legend.model.DeploymentDefinition;
import com.legend.model.IngestDefinition;
import com.legend.protocol.IngestWords;
import com.legend.protocol.Protocol;
import com.legend.sql.dialect.DeephavenLoad;
import com.legend.sql.dialect.HostOperation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A DEPLOYMENT, CHECKED AND RENDERED (docs/DEPLOYMENT_DESIGN_2026_11_14.md §8 and §9, step 1 of its order): what one
 * environment lands ({@code deploys}), from which sources ({@code sources}), on which targets ({@code live},
 * {@code history}), keeping how much ({@code keep}).
 *
 * <p>{@code deploys} names ingests and data products; a product lands the datasets it reads ({@link DataProducts}).
 * {@link #validate} names every reason the deployment cannot be landed: a name that is not what it should be (an
 * ingest or a data product deployed, a connection, a store a source feeds; a table a product reads that no ingest
 * makes); a topic whose store no source binds to one Kafka
 * connection; a dataset with no {@code keep} on a live target, or one that does not fit its write mode (an appended
 * dataset keeps rows, a span or all of them; a milestoned one its {@code current} version a key, or its
 * {@code versions}) -- the compiler assumes no default (AGENTS.md invariant 4); and what is not landed yet, each by
 * name (the history tier; a pull, a view; a live target other than Deephaven, the tab's step 5). A delivered ingest's
 * files are read at its source's drop location (a Files connection), one a dataset.
 * {@link #deephaven} renders a Deephaven live target's plan: data, for lite's fixed loader ({@link DeephavenLoad}),
 * never code. Nothing is applied here: installing is step 3's.
 */
public final class DeploymentPlanner {

    private DeploymentPlanner() {
    }

    /** Every reason {@code deployment} cannot be landed, in the order they are found; empty when it can. */
    public static List<String> validate(ModelContext ctx, String deployment) {
        return new Check(ctx, find(ctx, deployment)).run();
    }

    /** The plan for {@code deployment}'s live target, a Deephaven server: its tables as data for the fixed loader.
     *  Refused, with every reason, when the deployment does not validate. */
    public static DeephavenLoad.Plan deephaven(ModelContext ctx, String deployment) {
        DeploymentDefinition dp = find(ctx, deployment);
        List<String> problems = new Check(ctx, dp).run();
        if (!problems.isEmpty()) {
            throw new IllegalArgumentException("the deployment " + deployment + " cannot be landed: "
                    + String.join("; ", problems));
        }
        Protocol.PDeployment d = dp.deployment();
        Protocol.PDeploymentTarget live = java.util.Objects.requireNonNull(d.live());
        if (targetOf(ctx.findConnection(live.connection()).orElseThrow()) != Target.DEEPHAVEN) {
            throw new IllegalArgumentException("the deployment " + deployment + "'s live target " + live.connection()
                    + " is a database lite runs a host's loads on (a tab's own DuckDB): it lands by its ingests' plans,"
                    + " each asked with this deployment (its keep), not by a Deephaven server's plan");
        }
        Map<String, Protocol.PKeepRule> keep = keepByScope(live);
        List<DeephavenLoad.Table> tables = new ArrayList<>();
        for (Deployed deployed : deployed(ctx, d, new ArrayList<>())) {
            for (Landing t : deployed.landings(ctx)) {
                DeephavenLoad.Source source = t.topic() != null
                        ? new DeephavenLoad.Topic(t.topic(), kafka(ctx, d, t.store()).orElseThrow().bootstrapServers())
                        : new DeephavenLoad.File(fileUrl(files(ctx, d, t.store()).get(0), t));
                Protocol.PKeepRule rule = ruleOf(keep, t.table(), !t.key().isEmpty());
                DeephavenLoad.Keep k = switch (java.util.Objects.requireNonNull(rule)) {
                    case Protocol.PKeepRows r -> new DeephavenLoad.Rows(r.rows());
                    case Protocol.PKeepAll a -> new DeephavenLoad.All();
                    case Protocol.PKeepCurrent c -> new DeephavenLoad.Current(t.key(), new HostOperation.Milestones(
                            IngestWords.LAKE_DIGEST, IngestWords.LAKE_IN_ID, IngestWords.LAKE_OUT_ID,
                            IngestWords.OPEN_BATCH_ID), t.delete() == null ? null : new HostOperation.DeleteWhen(
                                    t.delete().deleteField(), t.delete().deleteValues()));
                    // the check refuses these on a live Deephaven target
                    case Protocol.PKeepFor f -> throw new IllegalStateException("validated: " + f);
                    case Protocol.PKeepVersions v -> throw new IllegalStateException("validated: " + v);
                };
                tables.add(new DeephavenLoad.Table(t.table(), source, t.format(),
                        t.columns().stream().map(c -> new DeephavenLoad.Column(c.name(), c.type())).toList(), k));
            }
        }
        return DeephavenLoad.plan(deployment, tables);
    }

    /**
     * How many rows {@code deployment}'s live target keeps of {@code ingest}'s dataset (docs/DEPLOYMENT_DESIGN_2026_11_14.md
     * §8, step 5: a tab's landing): its {@code LastRows(n)}, or null when it keeps every row ({@code all}) or a milestoned
     * dataset's current version a key. Refused, with every reason, when the deployment does not validate, and by name
     * when it does not land the dataset.
     */
    public static @com.legend.base.Nullable Long keptRows(ModelContext ctx, String deployment, IngestDefinition ingest,
            String dataset) {
        DeploymentDefinition dp = find(ctx, deployment);
        List<String> problems = new Check(ctx, dp).run();
        if (!problems.isEmpty()) {
            throw new IllegalArgumentException("the deployment " + deployment + " cannot be landed: "
                    + String.join("; ", problems));
        }
        // the ingest as the deployment holds it (the same record), and the dataset's landing there
        Landing landing = deployed(ctx, dp.deployment(), new ArrayList<>()).stream()
                .filter(d -> d.ingest().equals(ingest) && d.datasetNames().contains(dataset))
                .flatMap(d -> d.landings(ctx).stream()).filter(l -> l.table().equals(dataset)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("the deployment " + deployment + " lands no dataset "
                        + dataset + " of " + ingest.qualifiedName()));
        boolean milestoned = !landing.key().isEmpty();
        Protocol.PKeepRule rule = java.util.Objects.requireNonNull(ruleOf(keepByScope(
                java.util.Objects.requireNonNull(dp.deployment().live())), dataset, milestoned));
        return rule instanceof Protocol.PKeepRows r ? r.rows() : null;
    }

    /**
     * A table a runtime's server must hold when the runtime connects (docs/DEPLOYMENT_DESIGN_2026_11_14.md §9, step 3):
     * the server's connection, the deployment that lands the table there, the store of the runtime's that offers it,
     * and the table. Its {@link #query} reads it whole, so its plan checks each declared column against the server's
     * (DH16).
     */
    public record Held(String connection, String deployment, String store, String table) {
        /** The query that reads the table whole: {@code |#>{store.table}#}. */
        public String query() {
            String name = table.matches("[A-Za-z_][A-Za-z0-9_]*") ? table : "\"" + table + "\"";
            return "|#>{" + store + "." + name + "}#";
        }
    }

    /**
     * What {@code runtime}'s servers must hold when it connects: for each connection it binds that is a server whose
     * tables a plan makes at its start (Deephaven's), every table a deployment whose live target is that connection
     * lands there, each read through a store the runtime binds to such a server that offers it. A server's tables are
     * the deployment's, not the runtime's: no runtime knows what is on a server, so its host reads each before the
     * desk opens and refuses the desk naming every one missing or held otherwise. A table no store of the runtime's
     * offers is no table it reads, and is not asked for; a table the runtime reads that no deployment lands is refused
     * where a query reads it (DH16), naming it.
     */
    public static List<Held> onConnect(ModelContext ctx, String runtime) {
        com.legend.model.RuntimeDefinition rt = ctx.findRuntime(runtime).orElseThrow(() -> new IllegalArgumentException(
                "the model declares no Runtime " + runtime));
        // the runtime's stores bound to a server whose tables a plan makes, and those servers' connections
        List<String> stores = new ArrayList<>();
        java.util.Set<String> servers = new java.util.LinkedHashSet<>();
        rt.connectionBindings().forEach((store, connections) -> {
            for (String c : connections) {
                Optional<ConnectionDefinition> conn = ctx.findConnection(c);
                if (conn.isPresent() && targetOf(conn.get()) == Target.DEEPHAVEN) {
                    servers.add(c);
                    if (!stores.contains(store)) {
                        stores.add(store);
                    }
                }
            }
        });
        List<Held> held = new ArrayList<>();
        for (DeploymentDefinition dp : ctx.deployments().toList()) {
            Protocol.PDeploymentTarget live = dp.deployment().live();
            if (live == null || !servers.contains(live.connection())) {
                continue;
            }
            for (DeephavenLoad.Table t : deephaven(ctx, dp.qualifiedName()).tables()) {
                stores.stream().filter(s -> ctx.findTableDefinition(s, t.name()).isPresent()).findFirst()
                        .ifPresent(s -> held.add(new Held(live.connection(), dp.qualifiedName(), s, t.name())));
            }
        }
        return held;
    }

    private static DeploymentDefinition find(ModelContext ctx, String deployment) {
        return ctx.findDeployment(deployment).orElseThrow(() -> new IllegalArgumentException("the model declares no"
                + " Deployment " + deployment));
    }

    /** One dataset a deployment lands: its table, the store its source binds ({@code store}: a topic store, or the
     *  ingest its files are delivered to), its topic (null: a delivered file), its format, columns, key and delete
     *  indicator. */
    private record Landing(String table, String store, @com.legend.base.Nullable String topic, String format,
            List<IngestTopics.Column> columns, List<String> key,
            @com.legend.base.Nullable Protocol.PIngestDeleteIndicator delete) {
        /** Where its rows come from, said in a problem. */
        String from() {
            return topic != null ? "reads the topic " + topic + " of " + store : "is delivered to " + store + " as files";
        }
    }

    /** A delivered file's URL: its source's drop location, then the dataset and the format's extension
     *  ({@code file:///var/desk/positions/POSITION.csv}). */
    private static String fileUrl(ConnectionSpecification.Files drop, Landing t) {
        String at = drop.location();
        return at + (at.endsWith("/") ? "" : "/") + t.table() + "." + t.format().toLowerCase(java.util.Locale.ROOT);
    }

    /** An ingest a deployment lands, and which of its datasets: all of them ({@code datasets} null) when it is deployed
     *  itself, those a deployed data product reads otherwise. */
    private record Deployed(IngestDefinition ingest, java.util.@com.legend.base.Nullable Set<String> datasets) {
        /** Its datasets, those it lands: each a topic's records or a delivered file. */
        List<Landing> landings(ModelContext ctx) {
            List<Landing> out = new ArrayList<>();
            for (IngestTopics.Topic t : IngestTopics.ofIngest(ctx, ingest)) {
                out.add(new Landing(t.table(), t.store(), t.topic(), t.format(), t.columns(), t.key(), t.delete()));
            }
            for (IngestTopics.DeliveredFile f : IngestTopics.filesOf(ctx, ingest)) {
                out.add(new Landing(f.table(), f.store(), null, f.format(), f.columns(), f.key(), f.delete()));
            }
            return out.stream().filter(t -> datasets == null || datasets.contains(t.table())).toList();
        }

        List<String> datasetNames() {
            return ingest.ingest().datasets().stream().map(Protocol.PIngestDataset::name)
                    .filter(n -> datasets == null || datasets.contains(n)).toList();
        }
    }

    /** What {@code d} deploys, as ingests: each ingest it names, and the datasets each data product it names reads
     *  (DataProducts), in its order, an ingest named twice once; a name that is neither, and a table a product reads
     *  that no ingest makes, added to {@code problems}. */
    private static List<Deployed> deployed(ModelContext ctx, Protocol.PDeployment d, List<String> problems) {
        Map<String, Deployed> out = new LinkedHashMap<>();
        String where = "the deployment " + d.qualifiedName();
        for (String name : d.deploys()) {
            Optional<IngestDefinition> ingest = ctx.findIngest(name);
            Optional<com.legend.model.DataProductDefinition> product = ctx.findDataProduct(name);
            if (ingest.isPresent()) {
                out.put(name, new Deployed(ingest.get(), null));
            } else if (product.isPresent()) {
                DataProducts.Read read = DataProducts.datasets(ctx, product.get());
                read.unlanded().forEach(u -> problems.add(where + "'s data product " + name + " reads " + u));
                read.datasets().forEach((ing, sets) -> {
                    Deployed had = out.get(ing);
                    if (had == null || had.datasets() != null) {
                        java.util.Set<String> all = new java.util.LinkedHashSet<>();
                        if (had != null) {
                            all.addAll(java.util.Objects.requireNonNull(had.datasets()));
                        }
                        all.addAll(sets);
                        out.put(ing, new Deployed(ctx.findIngest(ing).orElseThrow(), all));
                    }
                });
            } else {
                problems.add(where + " deploys " + name + ", which is no Ingest or DataProduct the model declares");
            }
        }
        return new ArrayList<>(out.values());
    }

    /** A target's {@code keep}, by scope: a dataset's name, or {@code appended} / {@code milestoned}. */
    private static Map<String, Protocol.PKeepRule> keepByScope(Protocol.PDeploymentTarget t) {
        Map<String, Protocol.PKeepRule> m = new LinkedHashMap<>();
        for (Protocol.PKeep k : t.keep()) {
            m.put(k.scope(), k.rule());
        }
        return m;
    }

    private static final String APPENDED = "appended";
    private static final String MILESTONED = "milestoned";

    /** What a dataset keeps: its own rule, else its write mode's; null when neither is written. */
    private static Protocol.@com.legend.base.Nullable PKeepRule ruleOf(Map<String, Protocol.PKeepRule> keep,
            String dataset, boolean milestoned) {
        Protocol.PKeepRule own = keep.get(dataset);
        return own != null ? own : keep.get(milestoned ? MILESTONED : APPENDED);
    }

    /** The one Kafka connection {@code d}'s sources bind {@code store} to, if there is exactly one. */
    private static Optional<ConnectionSpecification.Kafka> kafka(ModelContext ctx, Protocol.PDeployment d, String store) {
        List<ConnectionSpecification.Kafka> found = kafkas(ctx, d, store);
        return found.size() == 1 ? Optional.of(found.get(0)) : Optional.empty();
    }

    /** What a live target is: a Deephaven server, or a database lite runs a host's loads on (a tab's own DuckDB). */
    private enum Target { DEEPHAVEN, TAB }

    /** {@code c}'s kind of live target, by its dialect: a server whose tables a plan makes at its start (Deephaven's), or
     *  one the platform runs plans on; null for any other. */
    private static @com.legend.base.Nullable Target targetOf(ConnectionDefinition c) {
        ConnectionDefinition.DatabaseType type = c.databaseType();
        boolean platform = com.legend.database.Databases.implemented().contains(type);
        if ((platform || com.legend.database.Databases.hostExecuted().contains(type))
                && com.legend.database.Databases.dialect(type).tablesMadeAtStartup()) {
            return Target.DEEPHAVEN;
        }
        return platform ? Target.TAB : null;
    }

    /** How many Simulated connections {@code d}'s sources bind {@code store} to. */
    private static int simulated(ModelContext ctx, Protocol.PDeployment d, String store) {
        int found = 0;
        for (Protocol.PDeploymentSource s : d.sources()) {
            if (s.stores().contains(store) && ctx.findConnection(s.connection())
                    .filter(c -> c.specification() instanceof ConnectionSpecification.Simulated).isPresent()) {
                found++;
            }
        }
        return found;
    }

    /** The Files connections {@code d}'s sources bind {@code store} to: its drop locations. */
    private static List<ConnectionSpecification.Files> files(ModelContext ctx, Protocol.PDeployment d, String store) {
        List<ConnectionSpecification.Files> found = new ArrayList<>();
        for (Protocol.PDeploymentSource s : d.sources()) {
            if (s.stores().contains(store)) {
                ctx.findConnection(s.connection()).ifPresent(c -> {
                    if (c.specification() instanceof ConnectionSpecification.Files f) {
                        found.add(f);
                    }
                });
            }
        }
        return found;
    }

    private static List<ConnectionSpecification.Kafka> kafkas(ModelContext ctx, Protocol.PDeployment d, String store) {
        List<ConnectionSpecification.Kafka> found = new ArrayList<>();
        for (Protocol.PDeploymentSource s : d.sources()) {
            if (s.stores().contains(store)) {
                ctx.findConnection(s.connection()).ifPresent(c -> {
                    if (c.specification() instanceof ConnectionSpecification.Kafka k) {
                        found.add(k);
                    }
                });
            }
        }
        return found;
    }

    /** One deployment's checks, their problems gathered. */
    private static final class Check {
        private final ModelContext ctx;
        private final Protocol.PDeployment d;
        private final String where;
        private final List<String> problems = new ArrayList<>();

        Check(ModelContext ctx, DeploymentDefinition dp) {
            this.ctx = ctx;
            this.d = dp.deployment();
            this.where = "the deployment " + dp.qualifiedName();
        }

        List<String> run() {
            List<Deployed> ingests = deployed(ctx, d, problems);
            sources();
            if (d.history() != null) {
                problems.add(where + " lands a history tier on " + d.history().connection() + ", which lite does not"
                        + " land yet (docs/DEPLOYMENT_DESIGN_2026_11_14.md §15: after the filtered-mapping fix)");
            }
            Protocol.PDeploymentTarget live = d.live();
            if (live == null) {
                if (d.history() == null) {
                    problems.add(where + " names no target (live: or history:)");
                }
                return problems;
            }
            Target target = liveTarget(live);
            if (target != null) {
                for (Deployed ingest : ingests) {
                    landed(ingest, live, target);
                }
                scopes(live, ingests);
            }
            return problems;
        }

        /** Each source a connection, each store it feeds a Database or an Ingest. */
        private void sources() {
            for (Protocol.PDeploymentSource s : d.sources()) {
                if (ctx.findConnection(s.connection()).isEmpty()) {
                    problems.add(where + "'s source " + s.connection() + " is no connection the model declares");
                }
                for (String store : s.stores()) {
                    if (ctx.findDatabase(store).isEmpty() && ctx.findIngest(store).isEmpty()) {
                        problems.add(where + "'s source " + s.connection() + " feeds " + store
                                + ", which is no Database or Ingest the model declares");
                    }
                }
            }
        }

        /** What the live target is -- a Deephaven server, or a tab's own DuckDB -- a problem named when it is anything
         *  else (null). */
        private @com.legend.base.Nullable Target liveTarget(Protocol.PDeploymentTarget live) {
            Optional<ConnectionDefinition> c = ctx.findConnection(live.connection());
            if (c.isEmpty()) {
                problems.add(where + "'s live target " + live.connection() + " is no connection the model declares");
                return null;
            }
            Target target = targetOf(c.get());
            if (target == null) {
                problems.add(where + "'s live target " + live.connection() + " is a " + c.get().databaseType()
                        + " connection: a live target is a Deephaven server (its plan for lite's loader) or a database"
                        + " lite runs a host's loads on, as a tab's own DuckDB (its ingests' plans)");
            }
            return target;
        }

        /** An ingest's datasets on the live Deephaven target: arriving on topics, each topic's store bound to one Kafka
         *  connection, or delivered as files, the ingest bound to one drop location (a Files connection); each dataset
         *  kept by a rule that fits its write mode. */
        private void landed(Deployed deployed, Protocol.PDeploymentTarget live, Target target) {
            IngestDefinition ingest = deployed.ingest();
            String of = where + "'s ingest " + ingest.qualifiedName();
            if (!IngestTopics.readsTopics(ingest) && !IngestTopics.receivesFiles(ingest)) {
                Protocol.PIngestReadMode read = ingest.ingest().readMode();
                problems.add(of + " lands " + (read == null ? "views alone" : "by " + read.kind()
                        + (read.format() == null ? "" : "<" + read.format() + ">")
                        + (ingest.ingest().schedule() == null ? "" : " on a schedule")) + ", which a Deephaven server"
                        + " does not land yet (a topic's records do, Delta<JSON>, and delivered files, Snapshot<CSV>;"
                        + " pulls and views are later steps)");
                return;
            }
            List<Landing> landings;
            try {
                landings = deployed.landings(ctx);
            } catch (IllegalArgumentException | IllegalStateException e) {
                problems.add(of + ": " + e.getMessage());
                return;
            }
            Map<String, Protocol.PKeepRule> keep = keepByScope(live);
            for (Landing t : landings) {
                // a tab's world publishes its topics and drops its files in the tab (a browser opens no Kafka
                // connection): every source a Simulated one
                int bound = target == Target.TAB ? simulated(ctx, d, t.store())
                        : t.topic() != null ? kafkas(ctx, d, t.store()).size() : files(ctx, d, t.store()).size();
                String kind = target == Target.TAB ? "Simulated" : t.topic() != null ? "Kafka" : "Files";
                if (target == Target.TAB && bound != 1) {
                    problems.add(of + "'s dataset " + t.table() + " " + t.from() + ", which the deployment's sources"
                            + " bind to " + (bound == 0 ? "no Simulated connection" : bound + " Simulated connections")
                            + ": a tab lands from the world simulated in it");
                } else if (bound != 1) {
                    problems.add(of + "'s dataset " + t.table() + " " + t.from() + ", which the deployment's sources"
                            + " bind to " + (bound == 0 ? "no " + kind + " connection" : bound + " " + kind
                                    + " connections") + ": " + (t.topic() != null ? "a topic's brokers are one Kafka"
                                    + " connection" : "a delivered ingest's files arrive at one drop location"));
                }
                boolean milestoned = !t.key().isEmpty();
                Protocol.PKeepRule rule = ruleOf(keep, t.table(), milestoned);
                String dataset = of + "'s dataset " + t.table();
                if (rule == null) {
                    problems.add(dataset + " keeps nothing on the live target: name its rule (" + t.table() + ": ... or "
                            + (milestoned ? MILESTONED + ": current" : APPENDED + ": LastRows(n)")
                            + "); the compiler assumes no default");
                    continue;
                }
                switch (rule) {
                    case Protocol.PKeepRows r -> appendedOnly(dataset, milestoned, "LastRows(" + r.rows() + ")");
                    case Protocol.PKeepAll a -> appendedOnly(dataset, milestoned, "all");
                    case Protocol.PKeepFor f -> problems.add(dataset + " keeps Last('" + f.duration() + "'): a span of"
                            + " time is not kept on a live target yet (keep its latest rows, LastRows(n))");
                    case Protocol.PKeepCurrent c -> {
                        if (!milestoned) {
                            problems.add(dataset + " is appended and keeps current: one row a key is a milestoned"
                                    + " dataset's (BatchMilestoned)");
                        }
                    }
                    case Protocol.PKeepVersions v -> problems.add(dataset + " keeps versions on the live target:"
                            + " every version is the history tier's (docs/DEPLOYMENT_DESIGN_2026_11_14.md §5); a live"
                            + " target keeps current");
                }
            }
        }

        private void appendedOnly(String dataset, boolean milestoned, String rule) {
            if (milestoned) {
                problems.add(dataset + " is milestoned and keeps " + rule + ": a milestoned dataset keeps current or"
                        + " versions");
            }
        }

        /** Each {@code keep} scope a write mode's or a deployed dataset's, its rule one that write mode keeps. */
        private void scopes(Protocol.PDeploymentTarget live, List<Deployed> ingests) {
            List<String> datasets = new ArrayList<>();
            for (Deployed ingest : ingests) {
                datasets.addAll(ingest.datasetNames());
            }
            for (Protocol.PKeep k : live.keep()) {
                boolean rows = !(k.rule() instanceof Protocol.PKeepCurrent || k.rule() instanceof Protocol.PKeepVersions);
                if (k.scope().equals(APPENDED) && !rows || k.scope().equals(MILESTONED) && rows) {
                    problems.add(where + " keeps " + k.scope() + " datasets by a rule of the other write mode"
                            + " (appended: LastRows(n), Last('PT..'), all; milestoned: current, versions)");
                } else if (!k.scope().equals(APPENDED) && !k.scope().equals(MILESTONED)
                        && !datasets.contains(k.scope())) {
                    problems.add(where + " keeps " + k.scope() + ", which is no dataset of an ingest it deploys");
                }
            }
        }
    }
}
