// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql.hardening;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.legend.compiler.element.ModelContext;
import com.legend.protocol.Protocol;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * THE FUZZER OVER A REAL MODEL (gate 10's GraphQL lane, //core:stress_graphql; docs/GRAPHQL_STUDY_2026_10_18.md
 * §13.11): the hardening suite's seeded documents asked of the stress corpus's core domains -- products, reference
 * data, counterparties, the organisation, positions, trading -- on the platform's database seeded with the corpus's
 * own data, through the product's query route over the compiled model, each answer judged by {@link StressReference}
 * (the reference evaluator over the corpus's text and seed). A wrong answer fails at once; so does a refusal (the
 * reference asks only what the schema offers). {@code -Dfuzz.seeds=a..b} and {@code -Dfuzz.n=N} widen a campaign.
 */
@org.junit.jupiter.api.Tag("stress")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Stress corpus: the GraphQL fuzzer over its core domains (the platform's database)")
class StressFuzzTest {

    /** The mappings whose class mappings the reference reads (the corpus's core domains). */
    static final Set<String> MAPPINGS = Set.of("products::ProductsMapping", "refdata::RefdataMapping",
            "counterparty::CounterpartyMapping", "org::OrgMapping", "positions::PositionsMapping",
            "trading::TradingMapping");

    /** The mappings read for their enumeration and association mappings only. */
    static final Set<String> SHARED = Set.of("trading::Codes", "stress::AllMapping");

    static final int SEEDS = 3;
    static final int PER_SEED = 150;

    private Connection connection;
    private Harness harness;
    private StressReference.Built built;
    private ModelContext ctx;

    @BeforeAll
    void open() throws Exception {
        var module = com.legend.Compiler.parseSources(com.legend.integration.StressCorpus.sources(List.of()));
        ctx = com.legend.Compiler.buildModel(module.model());
        var data = ctx.findData("stress::TestData").orElseThrow().body().value();
        if (!(data instanceof Protocol.PRelationalCsvData csv)) {
            throw new IllegalStateException("stress::TestData is not relational CSV");
        }
        Map<String, String> tables = new java.util.LinkedHashMap<>();
        StringBuilder blocks = new StringBuilder();
        for (Protocol.PRelationalCsvTable t : csv.tables()) {
            String values = java.util.Objects.requireNonNull(t.values());
            if ("default".equals(t.schema())) {
                tables.put(t.table(), values);
            }
            if (blocks.length() > 0) {
                blocks.append("\n-\n");
            }
            blocks.append(t.schema()).append('\n').append(t.table()).append('\n').append(values);
        }
        var backend = com.legend.testbackend.TestBackends.platform();
        connection = backend.open();
        var dialect = backend.dialect();
        com.legend.exec.SetupRunner.run(com.legend.setup.CsvSeed.steps(blocks.toString(), "store::DB", ctx, dialect),
                connection, dialect, null);
        List<Map.Entry<String, String>> files = new ArrayList<>();
        for (var f : com.legend.integration.StressCorpus.files()) {
            files.add(Map.entry(f.name(), f.text()));
        }
        built = StressReference.build(ctx, files, MAPPINGS, SHARED, tables);
        harness = new Harness(ctx, "", "stress::RT", connection, built.scope());
    }

    @AfterAll
    void close() throws Exception {
        connection.close();
    }

    @Test
    void theReferenceReadsTheDomains() {
        int classes = built.model().classes.size();
        long relations = built.model().classes.values().stream()
                .flatMap(c -> c.fields.values().stream())
                .filter(f -> f instanceof ReferenceModel.ToOne || f instanceof ReferenceModel.ToMany).count();
        System.out.printf("[stress-fuzz] reference: %d classes, %d relations, scope %d classes; left out: %s%n",
                classes, relations, built.scope().size(), built.skipped());
        assertTrue(classes >= 20, "the reference lost classes: " + classes);
        assertTrue(relations >= 10, "the reference lost relations: " + relations);
    }

    /** DEVELOPER EXPLORER ({@code -Dfuzz.repro=FILE}): each document of the file answered by lite and by the
     *  reference, its Pure and SQL, to FILE-out.md. */
    @Test
    @org.junit.jupiter.api.condition.EnabledIfSystemProperty(named = "fuzz.repro", matches = ".+")
    void repro() throws Exception {
        String file = System.getProperty("fuzz.repro");
        StringBuilder out = new StringBuilder();
        var schema = com.legend.compiler.graphql.SchemaGenerator.generate(ctx,
                com.legend.compiler.graphql.Scope.of(built.scope()));
        for (String d : java.nio.file.Files.readAllLines(java.nio.file.Path.of(file))) {
            if (d.isBlank()) {
                continue;
            }
            out.append("### ").append(d).append("\n");
            out.append("lite: ").append(com.legend.json.Json.toCompact(harness.query(d))).append("\n");
            try {
                out.append("reference: ").append(OVal.render(Reference.answer(built.model(), d, null, null)))
                        .append("\n");
            } catch (RuntimeException e) {
                out.append("reference: ").append(e).append("\n");
            }
            out.append("verdict: ").append(Fuzz.verdict(harness, built.model(), d)).append("\n");
            try {
                var t = com.legend.protocol.graphql.GqlToPure.translate(
                        com.legend.parser.GqlParser.parseDocument(d, 1, 1), schema, null, Map.of());
                for (var root : t.roots()) {
                    out.append("pure: ").append(com.legend.protocol.PureComposer.valueSpecification(root.expression(),
                            com.legend.protocol.PureComposer.Style.STANDARD)).append("\n");
                    out.append("sql: ").append(com.legend.Compiler.query(ctx, root.asLambda()).plan("stress::RT").sql())
                            .append("\n");
                }
            } catch (RuntimeException e) {
                out.append("explain: ").append(e).append("\n");
            }
            out.append("\n");
        }
        java.nio.file.Files.writeString(java.nio.file.Path.of(file + "-out.md"), out.toString());
    }

    @Test
    @org.junit.jupiter.api.condition.DisabledIfSystemProperty(named = "fuzz.repro", matches = ".+")
    void theFuzzerAgreesWithTheReference() {
        long from = 1;
        long to = SEEDS;
        String seeds = System.getProperty("fuzz.seeds");
        if (seeds != null) {
            String[] ab = seeds.split("\\.\\.");
            from = Long.parseLong(ab[0]);
            to = Long.parseLong(ab[ab.length - 1]);
        }
        int n = Integer.getInteger("fuzz.n", PER_SEED);
        List<String> failures = new ArrayList<>();
        int agree = 0;
        for (long seed = from; seed <= to; seed++) {
            Fuzz f = new Fuzz(built.model(), seed, built.unique(), built.enums(), built.enumTypes());
            for (int i = 0; i < n; i++) {
                Fuzz.GDoc d = f.document();
                String v = Fuzz.verdict(harness, built.model(), d);
                if (v == null) {
                    agree++;
                } else {
                    failures.add("seed " + seed + ": " + d.render()
                            + (d.variables() == null ? "" : "  variables " + d.variables()) + "\n    " + v);
                }
            }
        }
        System.out.printf("[stress-fuzz] %d of %d documents agree%n", agree, agree + failures.size());
        if (!failures.isEmpty()) {
            fail(failures.size() + " of " + (agree + failures.size()) + " documents disagree:\n"
                    + String.join("\n", failures.subList(0, Math.min(20, failures.size()))));
        }
    }
}
