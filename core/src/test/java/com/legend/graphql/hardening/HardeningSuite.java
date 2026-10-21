// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql.hardening;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.legend.json.Json;
import com.legend.testbackend.TestBackend;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * THE GRAPHQL HARDENING SUITE (docs/GRAPHQL_STUDY_2026_10_18.md §13.7): lite's GraphQL answers, through the product
 * path, judged against the reference evaluator over the shop fixture's rows, on each database a subclass names
 * ({@link GraphQLHardeningTest}: DuckDB; {@link GraphQLHardeningPostgresTest}: Postgres 16).
 *
 * <ul>
 *   <li>The case corpus ({@code graphql-parity/hardening/shop-cases.graphql}): hand-written documents, one per use
 *       case -- every scalar kind, nesting to four levels, the many-to-many both ways, aliases, fragments,
 *       directives, variables, every filter operator and combinator, ordering, paging, aggregates and groups -- each
 *       answered without error and equal to the reference; the error cases refused with their message.</li>
 *   <li>The fuzzer: seeded random documents over the same schema. A wrong answer fails at once. A refusal fails
 *       unless it is a known gap below, each named with the work that removes it.</li>
 * </ul>
 */
@org.junit.jupiter.api.TestInstance(org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS)
abstract class HardeningSuite {

    /** The refusals the fuzzer may meet, each a core gap with its reason (shrink-only: an entry leaves when its gap
     *  closes; a new kind of refusal fails the run). */
    static final Map<String, String> KNOWN_GAPS = gaps();

    private static Map<String, String> gaps() {
        Map<String, String> m = new LinkedHashMap<>();
        return java.util.Collections.unmodifiableMap(m);
    }

    static final int SEEDS = 3;
    static final int PER_SEED = 150;

    private Connection connection;
    private Harness harness;

    /** The database the suite runs on. */
    abstract TestBackend backend();

    /** The reference read under the database's case folding (Unicode unless the lane says otherwise). */
    ReferenceModel reference() {
        return ShopReference.MODEL;
    }

    @BeforeAll
    void open() throws SQLException {
        TestBackend b = backend();
        connection = b.inProcess() ? b.open() : b.openWorkspace();
        ShopFixture.seed(connection);
        harness = new Harness(ShopFixture.model(b), ShopFixture.RUNTIME, connection);
    }

    @AfterAll
    void close() throws SQLException {
        connection.close();
    }

    /** One corpus case. */
    record Case(String name, String document, @com.legend.base.Nullable String variables,
            @com.legend.base.Nullable String operation, @com.legend.base.Nullable String error) {
        @Override
        public String toString() {
            return name;
        }
    }

    static List<Case> cases() {
        List<Case> out = new ArrayList<>();
        String text = Harness.resource("hardening/shop-cases.graphql");
        for (String block : text.split("\\n\\s*\\n")) {
            String name = null;
            String variables = null;
            String operation = null;
            String error = null;
            StringBuilder doc = new StringBuilder();
            for (String line : block.split("\\n")) {
                if (line.startsWith("# variables: ")) {
                    variables = line.substring("# variables: ".length());
                } else if (line.startsWith("# operation: ")) {
                    operation = line.substring("# operation: ".length());
                } else if (line.startsWith("# error: ")) {
                    error = line.substring("# error: ".length());
                } else if (line.startsWith("# ")) {
                    if (name == null) {
                        name = line.substring(2);
                    }
                } else if (!line.isBlank()) {
                    doc.append(line).append('\n');
                }
            }
            if (name != null && doc.length() > 0) {
                out.add(new Case(name, doc.toString(), variables, operation, error));
            }
        }
        return out;
    }

    @Test
    void theCorpusIsWhole() {
        List<Case> cs = cases();
        assertTrue(cs.size() >= 130, "the corpus lost cases: " + cs.size());
        assertEquals(cs.size(), cs.stream().map(Case::name).distinct().count(), "two cases share a name");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void theCaseAgreesWithTheReference(Case c) {
        Json.Obj answer = harness.query(c.document(), c.variables(), c.operation());
        Json.Node errors = answer.fields().get("errors");
        if (c.error() != null) {
            assertTrue(errors instanceof Json.Arr a && !a.items().isEmpty()
                    && Json.toCompact(errors).contains(c.error()),
                    c.name() + ": expected a refusal naming '" + c.error() + "', got " + Json.toCompact(answer));
            return;
        }
        if (errors != null) {
            fail(c.name() + ": refused: " + Json.toCompact(errors));
        }
        Json.Obj variables = c.variables() == null ? null : (Json.Obj) Json.parse(c.variables(), Harness.JSON);
        OVal expected = Reference.answer(reference(), c.document(), variables, c.operation());
        assertNull(OVal.diff(expected, answer.get("data"), "data"),
                c.name() + ": lite " + Json.toCompact(answer.get("data")));
    }

    @Test
    void theFuzzerAgreesWithTheReference() {
        List<String> failures = new ArrayList<>();
        Map<String, Integer> gapsMet = new LinkedHashMap<>();
        int agree = 0;
        for (long seed = 1; seed <= SEEDS; seed++) {
            Fuzz f = new Fuzz(reference(), seed, Map.of("Customer", "id", "Order", "id", "Line", "id",
                    "Product", "sku", "Tag", "id", "Payment", "id", "CardPayment", "id", "VoucherPayment", "id",
                    "Listing", "sku", "Stock", "bin"), Map.of("Customer.tier", List.of("GOLD", "SILVER", "BRONZE"),
                    "Order.status", List.of("OPEN", "SHIPPED", "CANCELLED"),
                    "Product.category", List.of("BOOKS", "TOYS", "GARDEN")),
                    Map.of("Customer.tier", "Tier", "Order.status", "Status", "Product.category", "Category"));
            for (int i = 0; i < PER_SEED; i++) {
                Fuzz.GDoc d = f.document();
                String doc = d.render() + (d.variables() == null ? "" : "  variables " + d.variables());
                String v = Fuzz.verdict(harness, reference(), d);
                if (v == null) {
                    agree++;
                    continue;
                }
                String gap = v.startsWith("ERROR: ") ? knownGap(v) : null;
                if (gap != null) {
                    gapsMet.merge(gap, 1, Integer::sum);
                } else {
                    failures.add("seed " + seed + ": " + doc + "\n    " + v);
                }
            }
        }
        if (!failures.isEmpty()) {
            fail(failures.size() + " of " + SEEDS * PER_SEED + " documents disagree (" + agree + " agree, known gaps "
                    + gapsMet + "):\n" + String.join("\n", failures.subList(0, Math.min(20, failures.size()))));
        }
    }

    private static @com.legend.base.Nullable String knownGap(String verdict) {
        for (String k : KNOWN_GAPS.keySet()) {
            if (verdict.contains(k)) {
                return k;
            }
        }
        return null;
    }
}
