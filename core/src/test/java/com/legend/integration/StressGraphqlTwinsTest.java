package com.legend.integration;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.compiler.element.ModelContext;
import com.legend.model.PackageableElement;
import com.legend.model.ServiceDefinition;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.test.ServiceTestRunner;
import com.legend.testbackend.TestBackends;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * GRAPHQL OVER A REAL MODEL (gate 10's GraphQL lane, //core:stress_graphql): the stress corpus's services -- a
 * 7,600-element financial model, its mappings (unions, filters, inheritance, joins through sets of other mappings,
 * enumeration mappings, embedded and otherwise sets, qualified properties), its seed with the corpus's adversarial
 * rows -- asked as the GraphQL a client would write for the same question, each answer judged by the service's own
 * expectation from the corpus's independent oracle (scripts/corpus/oracle.py).
 *
 * <ul>
 *   <li>A GRAPH-FETCH service ({@code C.all()->graphFetch(tree)->serialize(tree)}) is the list root {@code C} with
 *       the tree as its selection; the twin runs as the service with its query replaced, judged by the service's
 *       suite.</li>
 *   <li>A TDS service ({@code filter -> project -> [filter] -> [groupBy] -> sort -> [limit]} over to-one paths,
 *       {@link ServiceTwins}) is the list root with a {@code where}, an {@code orderBy}, a {@code limit} and the
 *       columns' paths as its selection, or the class's {@code Groups} root for a {@code groupBy}; the answer
 *       flattens back to the service's columns by path and is judged by the oracle's rows, its order by the sort
 *       column.</li>
 * </ul>
 *
 * <p>Each schema is generated for the twin's classes and the classes they reach (the model has same-named classes in
 * different packages: no single schema covers it). The counts judged equal may only grow: raise a floor with the leg
 * that raises the count, never lower it (as {@link StressServiceSuitesTest#MIN_PASS}). The verdicts, one line per
 * service, go to stress-graphql-*.txt in the test's outputs.
 */
@org.junit.jupiter.api.Tag("stress")
@DisplayName("Stress corpus: its services asked through GraphQL (DuckDB)")
class StressGraphqlTwinsTest {

    /** Graph-fetch twins judged equal to the oracle (first run 2026-10-21: 436 of 446 services; the ten others: 9
     *  whose service fails in lite too, and one {@link #NOT_TWINNED}). Shrink-proof. */
    static final int MIN_GRAPH = 436;

    /** TDS twins judged equal to the oracle, rows and order (first run 2026-10-21: 4,261 of 4,274 twinned, after the
     *  two resolver fixes this lane found, RoutedNavigationIntegrationTest; the thirteen others fail as their
     *  services do: provisioning over several stores, a multi-execution service, a JSON-mapped property, a Float
     *  difference past the sixth digit). Shrink-proof. */
    static final int MIN_TDS = 4261;

    /** Services whose GraphQL twin asks a different question than the service, by design, each with its reason. */
    static final Map<String, String> NOT_TWINNED = Map.of(
            "stress::CB_Otherwise", "a graph fetch of an Otherwise-embedded property reads the fallback set (the "
                    + "engine's testOtherwiseEmbeddedMapping, GraphEmission.graphChild); the projection reads the "
                    + "inline columns: the tree and the row are different answers",
            "stress::G2_MilestonedTree", "all(%latest): GraphQL names a version by its date, and there is no "
                    + "latest date literal",
            "stress::MU0_MonetaryTrade", "its class is in an excluded file (StressExclusions)");

    private static ModelContext ctx;
    private static List<PackageableElement> elements;

    private static synchronized void load() throws Exception {
        if (ctx == null) {
            var module = com.legend.Compiler.parseSources(StressCorpus.sources(List.of()));
            elements = module.model().elements();
            ctx = com.legend.Compiler.buildModel(module.model());
        }
    }

    /** The {@code stress::} services with suites, in name order, less the ones not twinned. */
    private static List<ServiceDefinition> services() {
        List<ServiceDefinition> out = new ArrayList<>();
        for (PackageableElement el : elements) {
            if (el instanceof ServiceDefinition svc && svc.testSuites() != null && !svc.testSuites().isEmpty()
                    && svc.qualifiedName().startsWith("stress::") && !NOT_TWINNED.containsKey(svc.qualifiedName())) {
                out.add(svc);
            }
        }
        out.sort(java.util.Comparator.comparing(ServiceDefinition::qualifiedName));
        return out;
    }

    private static ServiceDefinition withQuery(ServiceDefinition svc, LambdaFunction query) {
        return new ServiceDefinition(svc.qualifiedName(), svc.pattern(), query, svc.documentation(),
                svc.mappingRef(), svc.runtimeRef(), svc.testSuites(), svc.owners(), null, null, null);
    }

    @Test
    void graphFetchServicesAskedThroughGraphQL() throws Exception {
        load();
        List<String> agree = new ArrayList<>();
        List<String> disagree = new ArrayList<>();
        Map<String, Integer> notBuilt = new TreeMap<>();
        try (var runner = new ServiceTestRunner(ctx, TestBackends.platform()::open, ServiceTestRunner.Sessions.SHARED,
                TestBackends.platform().type())) {
            for (ServiceDefinition svc : services()) {
                if (ServiceTwins.graphFetchTree(svc.functionBody()) == null) {
                    continue;
                }
                ServiceTwins.GraphTwin twin;
                LambdaFunction query;
                try {
                    twin = ServiceTwins.graphTwin(ctx, svc.functionBody());
                    query = ServiceTwins.translate(ctx, twin.scope(), twin.document());
                } catch (ServiceTwins.Unsupported e) {
                    notBuilt.merge(String.valueOf(e.getMessage()), 1, Integer::sum);
                    continue;
                } catch (RuntimeException e) {
                    disagree.add(svc.qualifiedName() + " :: GRAPHQL " + e);
                    continue;
                }
                List<ServiceTestRunner.Result> rs = runner.run(withQuery(svc, query));
                if (!rs.isEmpty() && rs.stream().allMatch(ServiceTestRunner.Result::pass)) {
                    agree.add(svc.qualifiedName() + " :: " + twin.document());
                } else {
                    disagree.add(svc.qualifiedName() + " :: " + twin.document() + " :: "
                            + (rs.isEmpty() ? "no result" : rs.get(0).reason()));
                }
            }
        }
        report("graph", agree, disagree, notBuilt);
        assertTrue(agree.size() >= MIN_GRAPH, agree.size() + " graph-fetch twins agree with the oracle, below the"
                + " floor " + MIN_GRAPH + " -- see stress-graphql-graph-*.txt in the test's outputs; first: "
                + (disagree.isEmpty() ? "" : disagree.get(0)));
    }

    @Test
    void tdsServicesAskedThroughGraphQL() throws Exception {
        load();
        List<String> agree = new ArrayList<>();
        List<String> disagree = new ArrayList<>();
        Map<String, Integer> notBuilt = new TreeMap<>();
        Object[] captured = new Object[1];
        try (var runner = new ServiceTestRunner(ctx, TestBackends.platform()::open, ServiceTestRunner.Sessions.SHARED,
                TestBackends.platform().type(), r -> captured[0] = r.actual())) {
            for (ServiceDefinition svc : services()) {
                if (ServiceTwins.graphFetchTree(svc.functionBody()) != null) {
                    continue;
                }
                Object expected = expected(svc);
                ServiceTwins.Twin twin;
                String document;
                try {
                    if (expected == null) {
                        throw new ServiceTwins.Unsupported("no EqualToJson expectation");
                    }
                    twin = ServiceTwins.of(ctx, svc.functionBody());
                    document = ServiceTwins.named(ctx, twin);
                } catch (ServiceTwins.Unsupported e) {
                    notBuilt.merge(String.valueOf(e.getMessage()).replaceAll("[a-z_0-9]+::[A-Za-z_0-9:.]+", "X"), 1,
                            Integer::sum);
                    continue;
                }
                String verdict = verdict(runner, svc, twin, document, expected, captured);
                if (verdict == null) {
                    agree.add(svc.qualifiedName() + " :: " + document);
                } else {
                    disagree.add(svc.qualifiedName() + " :: " + document + " :: " + verdict);
                }
            }
        }
        report("tds", agree, disagree, notBuilt);
        assertTrue(agree.size() >= MIN_TDS, agree.size() + " TDS twins agree with the oracle, below the floor "
                + MIN_TDS + " -- see stress-graphql-tds-*.txt in the test's outputs; first: "
                + (disagree.isEmpty() ? "" : disagree.get(0)));
    }

    /** Null when the twin's answer is the oracle's rows in the oracle's order, else what differs. */
    private static @com.legend.base.Nullable String verdict(ServiceTestRunner runner, ServiceDefinition svc,
            ServiceTwins.Twin twin, String document, Object expected, Object[] captured) {
        LambdaFunction query;
        try {
            query = ServiceTwins.translate(ctx, ServiceTwins.scope(ctx, twin), document);
        } catch (RuntimeException e) {
            return "GRAPHQL " + e;
        }
        captured[0] = null;
        List<ServiceTestRunner.Result> rs = runner.run(withQuery(svc, query));
        Object actual = captured[0];
        if (actual == null) {
            return "EXECUTION " + (rs.isEmpty() ? "no result" : rs.get(0).reason());
        }
        List<Map<String, Object>> rows;
        try {
            Object answer = twin.kind() == ServiceTwins.Kind.TREE ? ((Map<?, ?>) actual).get("values")
                    : ServiceTwins.arranged((List<?>) actual);
            rows = ServiceTwins.rows(twin, answer);
        } catch (RuntimeException e) {
            return "SHAPE " + e + " of " + actual;
        }
        String diff = com.legend.test.TestAssertions.equalToJson(
                twin.kind() == ServiceTwins.Kind.TREE ? ServiceTwins.graphChannel(expected) : expected, rows);
        if (diff != null) {
            return "DATA " + diff;
        }
        if (!twin.sortColumns().isEmpty()) {
            // the order the oracle's rows are in, by the sort's keys (ties may come in either order)
            List<Object> want = new ArrayList<>();
            for (Object r : (List<?>) expected) {
                List<Object> key = new ArrayList<>();
                for (String c : twin.sortColumns()) {
                    key.add(((Map<?, ?>) r).get(c));
                }
                want.add(key);
            }
            List<Object> got = new ArrayList<>();
            for (Map<String, Object> r : rows) {
                List<Object> key = new ArrayList<>();
                for (String c : twin.sortColumns()) {
                    key.add(r.get(c));
                }
                got.add(key);
            }
            if (!String.valueOf(want).equals(String.valueOf(got))) {
                return "ORDER by " + twin.sortColumns() + ": want " + want + ", got " + got;
            }
        }
        return null;
    }

    /** The first test's EqualToJson expectation as a JSON tree, or null. */
    private static @com.legend.base.Nullable Object expected(ServiceDefinition svc) {
        for (var suite : svc.testSuites()) {
            for (var test : suite.tests()) {
                for (var a : test.assertions()) {
                    if (a.expected() instanceof com.legend.protocol.Protocol.PExternalFormatData d) {
                        return com.legend.sql.Json.parse(d.data());
                    }
                }
            }
        }
        return null;
    }

    private static void report(String kind, List<String> agree, List<String> disagree, Map<String, Integer> notBuilt)
            throws java.io.IOException {
        var dir = com.legend.testing.TestOutputs.dir();
        Files.createDirectories(dir);
        Files.write(dir.resolve("stress-graphql-" + kind + "-agree.txt"), agree);
        Files.write(dir.resolve("stress-graphql-" + kind + "-disagree.txt"), disagree);
        List<String> nb = new ArrayList<>();
        notBuilt.forEach((why, n) -> nb.add(n + "\t" + why));
        Files.write(dir.resolve("stress-graphql-" + kind + "-not-twinned.txt"), nb);
        System.out.printf("[stress-graphql] %s: %d agree, %d disagree, not twinned %s%n", kind, agree.size(),
                disagree.size(), notBuilt);
        for (String d : disagree.subList(0, Math.min(10, disagree.size()))) {
            System.out.println("[stress-graphql]   " + (d.length() > 600 ? d.substring(0, 600) + "..." : d));
        }
    }
}
