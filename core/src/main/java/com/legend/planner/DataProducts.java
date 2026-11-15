// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.element.ModelContext;
import com.legend.model.DataProductDefinition;
import com.legend.model.IngestDefinition;
import com.legend.model.LegacyMappingDefinition;
import com.legend.protocol.Protocol;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * WHAT A DATA PRODUCT READS (docs/DEPLOYMENT_DESIGN_2026_11_14.md §7, step 2): the datasets a deployment of it lands --
 * upstream's deploy artifact's {@code dependencyDatasets} (legend-studio {@code V1_DataProductArtifact.ts}) -- each
 * an ingest's dataset: the tables its model groups' mappings map their classes onto (each relational class binding's
 * main table, through the mapping's includes, as the compiled mapping stamps it), and the tables its access points' functions read
 * ({@code #>{db.table}#}), each found among the ingests its store includes. A table no ingest makes is named, never
 * passed over: nothing would land it.
 */
public final class DataProducts {

    private DataProducts() {
    }

    /** What {@link #datasets} found: each ingest's datasets the product reads, in order, and every table it reads that
     *  no ingest makes (a problem a deployment names). */
    public record Read(Map<String, LinkedHashSet<String>> datasets, List<String> unlanded) {
    }

    /** The datasets {@code product} reads, by ingest. */
    public static Read datasets(ModelContext ctx, DataProductDefinition product) {
        Map<String, LinkedHashSet<String>> out = new LinkedHashMap<>();
        List<String> unlanded = new ArrayList<>();
        for (Protocol.PAccessPointGroup g : product.dataProduct().accessPointGroups()) {
            if (g.mapping() != null) {
                for (LegacyMappingDefinition.TableReference t : mainTables(ctx, g.mapping(), unlanded)) {
                    land(ctx, t.database(), t.table(), out, unlanded);
                }
            }
            for (Protocol.PLakehouseAccessPoint ap : g.accessPoints()) {
                List<IngestDefinition.TableRead> reads = new ArrayList<>();
                scan(ap.func(), reads);
                for (IngestDefinition.TableRead r : reads) {
                    land(ctx, r.database(), r.table(), out, unlanded);
                }
            }
        }
        return new Read(out, unlanded);
    }

    /** Each relational class binding's main table, through the mapping's includes, in order: the compiled mapping's
     *  stamped source (MappingDefinition.RelationalSource.Table, docs/LEGACY_MAPPING_REACHBACK_CENSUS.md's razor: a
     *  closed fact read verbatim from the binding, never from the parse). */
    private static List<LegacyMappingDefinition.TableReference> mainTables(ModelContext ctx, String mapping,
            List<String> unlanded) {
        Optional<com.legend.model.MappingDefinition> m = ctx.findMapping(mapping);
        if (m.isEmpty()) {
            unlanded.add("the mapping " + mapping + ", which the model does not declare");
            return List.of();
        }
        List<LegacyMappingDefinition.TableReference> tables = new ArrayList<>();
        for (com.legend.model.MappingDefinition.ClassBinding b : m.get().classBindingsWithIncludes(ctx::findMapping)) {
            if (b instanceof com.legend.model.MappingDefinition.ClassBinding.Relational r
                    && r.source() instanceof com.legend.model.MappingDefinition.RelationalSource.Table t) {
                LegacyMappingDefinition.TableReference ref = new LegacyMappingDefinition.TableReference(t.database(),
                        t.table());
                if (!tables.contains(ref)) {
                    tables.add(ref);
                }
            }
        }
        return tables;
    }

    /** The ingest whose dataset {@code table} of {@code database} is, found through the database's includes. */
    private static void land(ModelContext ctx, String database, String table, Map<String, LinkedHashSet<String>> out,
            List<String> unlanded) {
        ArrayDeque<String> next = new ArrayDeque<>(List.of(database));
        Set<String> seen = new HashSet<>();
        while (!next.isEmpty()) {
            String name = next.poll();
            if (!seen.add(name)) {
                continue;
            }
            Optional<IngestDefinition> ingest = ctx.findIngest(name);
            if (ingest.isPresent()) {
                if (ingest.get().ingest().datasets().stream().anyMatch(d -> d.name().equals(table))) {
                    out.computeIfAbsent(name, k -> new LinkedHashSet<>()).add(table);
                    return;
                }
                continue;
            }
            ctx.findDatabase(name).ifPresent(db -> next.addAll(db.includes()));
        }
        String what = "the table " + table + " of " + database + ", which no ingest makes";
        if (!unlanded.contains(what)) {
            unlanded.add(what);
        }
    }

    /** Every table a function reads ({@code #>{db.table}#}), in order. */
    private static void scan(ValueSpecification v, List<IngestDefinition.TableRead> out) {
        switch (v) {
            case AppliedFunction f -> {
                IngestDefinition.TableRead r = IngestDefinition.tableRead(f);
                if (r != null) {
                    out.add(r);
                }
                f.parameters().forEach(p -> scan(p, out));
            }
            case LambdaFunction l -> l.body().forEach(b -> scan(b, out));
            case PureCollection c -> c.values().forEach(x -> scan(x, out));
            case AppliedProperty p -> scan(p.receiver(), out);
            default -> { }
        }
    }
}
