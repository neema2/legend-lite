// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.TAB;

/**
 * {@code ###Lakehouse}'s Ingest printed as lite's grammar reads it ({@code LakehouseSectionGrammar}; the declared
 * extension LITE-DESIGN-lakehouse) -- over the record, {@link Protocol.PIngest}. Legend's own printer is closed, so
 * this is lite's: the form Studio shows, each other field by that form's conventions, a column's multiplicity
 * printed only where it is not the relation type's {@code [0..1]}.
 */
final class LakehouseComposer {

    private LakehouseComposer() {
    }

    /** A {@code ###Lakehouse} element: an Ingest or a Deployment. */
    static String element(Protocol.Element e, PureComposer.Style style) {
        return switch (e) {
            case Protocol.PIngest g -> ingest(g, style);
            case Protocol.PDeployment d -> deployment(d);
            default -> throw Composing.refused("no Lakehouse printer for a " + e.getClass().getSimpleName());
        };
    }

    /** {@code Deployment qn { deploys: [...]; sources: [...]; live: conn keep [...]; history: ...; }}. */
    static String deployment(Protocol.PDeployment d) {
        StringBuilder b = new StringBuilder("Deployment ")
                .append(DomainComposer.annotations(d.stereotypes(), d.taggedValues()))
                .append(Composing.elementPath(d.pkg(), d.name())).append("\n{\n");
        b.append(TAB).append("deploys: [ ").append(String.join(", ", d.deploys())).append(" ];\n");
        if (!d.sources().isEmpty()) {
            List<String> sources = new ArrayList<>();
            for (Protocol.PDeploymentSource s : d.sources()) {
                sources.add(s.connection() + ": [ " + String.join(", ", s.stores()) + " ]");
            }
            b.append(TAB).append("sources: [ ").append(String.join(", ", sources)).append(" ];\n");
        }
        if (d.live() != null) {
            b.append(TAB).append("live: ").append(target(d.live())).append(";\n");
        }
        if (d.history() != null) {
            b.append(TAB).append("history: ").append(target(d.history())).append(";\n");
        }
        return b.append('}').toString();
    }

    private static String target(Protocol.PDeploymentTarget t) {
        if (t.keep().isEmpty()) {
            return t.connection();
        }
        List<String> keep = new ArrayList<>();
        for (Protocol.PKeep k : t.keep()) {
            keep.add(k.scope() + ": " + switch (k.rule()) {
                case Protocol.PKeepRows r -> "LastRows(" + r.rows() + ")";
                case Protocol.PKeepFor f -> "Last(" + quoted(f.duration()) + ")";
                case Protocol.PKeepAll a -> "all";
                case Protocol.PKeepCurrent c -> "current";
                case Protocol.PKeepVersions v -> "versions";
            });
        }
        return t.connection() + " keep [ " + String.join(", ", keep) + " ]";
    }

    static String ingest(Protocol.PIngest g, PureComposer.Style style) {
        StringBuilder b = new StringBuilder("Ingest ")
                .append(DomainComposer.annotations(g.stereotypes(), g.taggedValues()))
                .append(Composing.elementPath(g.pkg(), g.name()));
        if (g.readMode() != null) {
            b.append(' ').append(g.readMode().kind());
            if (g.readMode().format() != null) {
                b.append('<').append(g.readMode().format()).append('>');
            }
        }
        if (g.owner() != null) {
            b.append(" owner=AppDir(production='").append(g.owner().production()).append('\'');
            if (g.owner().prodParallel() != null) {
                b.append(", prodParallel='").append(g.owner().prodParallel()).append('\'');
            }
            b.append(')');
        }
        if (g.datasetGroup() != null) {
            b.append(" datasetGroup=").append(g.datasetGroup());
        }
        if (g.writeMode() != null) {
            b.append(" writeMode=").append(writeMode(g.writeMode()));
        }
        if (g.readMode() != null && g.readMode().deleteIndicator() != null) {
            Protocol.PIngestDeleteIndicator d = g.readMode().deleteIndicator();
            List<String> values = new ArrayList<>();
            for (String v : d.deleteValues()) {
                values.add(quoted(v));
            }
            b.append(" deleteIndicator=DeleteIndicator(deleteField=").append(quoted(d.deleteField()))
                    .append(", deleteValues=[").append(String.join(", ", values)).append("])");
        }
        if (g.schedule() != null) {
            b.append(" schedule=").append(switch (g.schedule()) {
                case Protocol.PIngestDaily d -> "Daily(" + quoted(d.time()) + ", " + quoted(d.zone()) + ")";
                case Protocol.PIngestEvery e -> "Every(" + quoted(e.period()) + ")";
            });
        }
        b.append("\n[\n");
        for (Protocol.PIngestDataset d : g.datasets()) {
            b.append(TAB).append(dataset(d, style)).append('\n');
        }
        return b.append(']').toString();
    }

    private static String dataset(Protocol.PIngestDataset d, PureComposer.Style style) {
        StringBuilder b = new StringBuilder(d.name()).append("( ");
        switch (d.source()) {
            case Protocol.PIngestColumns cols -> {
                List<String> cs = new ArrayList<>();
                for (TypeExpression.Column c : cols.columns()) {
                    boolean optional = c.multiplicity().equals(Multiplicity.range(0, 1));
                    cs.add(c.name() + ": " + Composing.genericType(c.type())
                            + (optional ? "" : "[" + Composing.multiplicity(c.multiplicity()) + "]"));
                }
                b.append(String.join(", ", cs));
            }
            case Protocol.PIngestFunction f -> b.append(PureComposer.lambda(f.function(), style));
        }
        b.append(" )");
        if (!d.primaryKey().isEmpty()) {
            b.append(" pk=").append(columns(d.primaryKey()));
        }
        if (d.writeMode() != null) {
            b.append(" writeMode=").append(writeMode(d.writeMode()));
        }
        if (d.retention() != null) {
            b.append(" retention=").append(switch (d.retention()) {
                case Protocol.PIngestLast l -> "Last(" + quoted(l.duration()) + ")";
                case Protocol.PIngestLastRows r -> "LastRows(" + r.rows() + ")";
            });
        }
        if (d.privacyClassification() != null) {
            b.append(" privacyClassification=").append(d.privacyClassification());
        }
        if (!d.ingestPartitionColumns().isEmpty()) {
            b.append(" ingestPartitionColumns=").append(columns(d.ingestPartitionColumns()));
        }
        if (!d.storageLayoutPartitionColumns().isEmpty()) {
            b.append(" storageLayoutPartitionColumns=").append(columns(d.storageLayoutPartitionColumns()));
        }
        if (!d.storageLayoutClusterColumns().isEmpty()) {
            b.append(" storageLayoutClusterColumns=").append(columns(d.storageLayoutClusterColumns()));
        }
        return b.append(';').toString();
    }

    private static String writeMode(String wire) {
        String word = IngestWords.writeModeWord(wire);
        if (word == null) {
            throw Composing.refused("an ingest's write mode '" + wire + "'");
        }
        return word;
    }

    private static String columns(List<String> names) {
        return "[ " + String.join(", ", names) + " ]";
    }

    private static String quoted(String s) {
        return Composing.convertString(s, true);
    }
}
