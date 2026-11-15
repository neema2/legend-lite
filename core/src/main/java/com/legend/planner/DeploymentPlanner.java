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
 * <p>{@link #validate} names every reason the deployment cannot be landed: a name that is not what it should be (an
 * ingest deployed, a connection, a store a source feeds); a topic whose store no source binds to one Kafka
 * connection; a dataset with no {@code keep} on a live target, or one that does not fit its write mode (an appended
 * dataset keeps rows, a span or all of them; a milestoned one its {@code current} version a key, or its
 * {@code versions}) -- the compiler assumes no default (AGENTS.md invariant 4); and what is not landed yet, each by
 * name (the history tier; a pull, a file, a view; a live target other than Deephaven, the tab's step 5).
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
        Map<String, Protocol.PKeepRule> keep = keepByScope(live);
        List<DeephavenLoad.Table> tables = new ArrayList<>();
        for (String name : d.deploys()) {
            IngestDefinition ingest = ctx.findIngest(name).orElseThrow();
            for (IngestTopics.Topic t : IngestTopics.ofIngest(ctx, ingest)) {
                ConnectionSpecification.Kafka kafka = kafka(ctx, d, t.store()).orElseThrow();
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
                tables.add(new DeephavenLoad.Table(t.table(), t.topic(), kafka.bootstrapServers(), t.format(),
                        t.columns().stream().map(c -> new DeephavenLoad.Column(c.name(), c.type())).toList(), k));
            }
        }
        return DeephavenLoad.plan(deployment, tables);
    }

    private static DeploymentDefinition find(ModelContext ctx, String deployment) {
        return ctx.findDeployment(deployment).orElseThrow(() -> new IllegalArgumentException("the model declares no"
                + " Deployment " + deployment));
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
            List<IngestDefinition> ingests = deployed();
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
            if (liveIsDeephaven(live)) {
                for (IngestDefinition ingest : ingests) {
                    landed(ingest, live);
                }
                scopes(live, ingests);
            }
            return problems;
        }

        /** Each name {@code deploys} lists, an ingest: the ingests, in its order. */
        private List<IngestDefinition> deployed() {
            List<IngestDefinition> ingests = new ArrayList<>();
            for (String name : d.deploys()) {
                Optional<IngestDefinition> ingest = ctx.findIngest(name);
                if (ingest.isEmpty()) {
                    problems.add(where + " deploys " + name + ", which is no Ingest the model declares");
                } else {
                    ingests.add(ingest.get());
                }
            }
            return ingests;
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

        /** Whether the live target is a Deephaven server; a problem named when it is anything else. */
        private boolean liveIsDeephaven(Protocol.PDeploymentTarget live) {
            Optional<ConnectionDefinition> c = ctx.findConnection(live.connection());
            if (c.isEmpty()) {
                problems.add(where + "'s live target " + live.connection() + " is no connection the model declares");
                return false;
            }
            ConnectionDefinition.DatabaseType type = c.get().databaseType();
            boolean planned = com.legend.database.Databases.implemented().contains(type)
                    || com.legend.database.Databases.hostExecuted().contains(type);
            if (!planned || !com.legend.database.Databases.dialect(type).tablesMadeAtStartup()) {
                problems.add(where + "'s live target " + live.connection() + " is a " + type + " connection: only a"
                        + " Deephaven server is rendered a deployment's plan yet (the tab's own DuckDB lands by its"
                        + " ingests' plans: the design's step 5)");
                return false;
            }
            return true;
        }

        /** An ingest's datasets on the live Deephaven target: arriving on topics, each topic's store bound to one Kafka
         *  connection, each dataset kept by a rule that fits its write mode. */
        private void landed(IngestDefinition ingest, Protocol.PDeploymentTarget live) {
            String of = where + "'s ingest " + ingest.qualifiedName();
            if (!IngestTopics.readsTopics(ingest)) {
                Protocol.PIngestReadMode read = ingest.ingest().readMode();
                problems.add(of + " lands " + (read == null ? "views alone" : "by " + read.kind()
                        + (read.format() == null ? "" : "<" + read.format() + ">")) + ", which a Deephaven server does"
                        + " not land yet (a topic's records do: Delta<JSON>; pulls, files and views are the design's"
                        + " steps 4 and later)");
                return;
            }
            List<IngestTopics.Topic> topics;
            try {
                topics = IngestTopics.ofIngest(ctx, ingest);
            } catch (IllegalArgumentException | IllegalStateException e) {
                problems.add(of + ": " + e.getMessage());
                return;
            }
            Map<String, Protocol.PKeepRule> keep = keepByScope(live);
            for (IngestTopics.Topic t : topics) {
                int bound = kafkas(ctx, d, t.store()).size();
                if (bound != 1) {
                    problems.add(of + "'s dataset " + t.table() + " reads the topic " + t.topic() + " of " + t.store()
                            + ", which the deployment's sources bind to " + (bound == 0 ? "no Kafka connection"
                                    : bound + " Kafka connections") + ": a topic's brokers are one Kafka connection");
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
                            + " time is not kept on Deephaven yet (keep its latest rows, LastRows(n))");
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
        private void scopes(Protocol.PDeploymentTarget live, List<IngestDefinition> ingests) {
            List<String> datasets = new ArrayList<>();
            for (IngestDefinition ingest : ingests) {
                ingest.ingest().datasets().forEach(x -> datasets.add(x.name()));
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
