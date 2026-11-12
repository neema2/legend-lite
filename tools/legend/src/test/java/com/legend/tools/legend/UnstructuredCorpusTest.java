package com.legend.tools.legend;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.result.Collector;
import com.legend.result.Output;
import com.legend.server.ConnectionResolver;
import com.legend.testing.Runfile;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The unstructured study's model over its loaded corpus (//projects/unstructured:corpus_test, manual). Compiles
 * model.pure and runs Pure queries on DuckDB through the server's own route. The corpus is a pointer corpus, rebuilt
 * locally, so a checkout without it FAILS here and names the rebuild; this test never skips.
 */
class UnstructuredCorpusTest {

    private static final String DECLARED_PATH = "projects/unstructured/corpus/unstructured.duckdb";
    private static final String RUNTIME = "runtime::DuckDb";
    private static final String REBUILD = """
            Rebuild it from the repository root:
              EDGAR_USER_AGENT="Name email@example.org" python tools/edgar-corpus/fetch.py
              python tools/edgar-corpus/extract.py
              python tools/unstructured-corpus/load.py""";

    private static Path project;
    private static ModelContext context;

    @BeforeAll
    static void compileTheModelOverTheLoadedFile() throws IOException {
        // The target runs unsandboxed (tag "local"), and the runfile is a link into the source tree: its real path says
        // where the corpus is. The model's connection names the file from the repository root, which a test's working
        // directory is not, so the path alone is made absolute; everything else is the model as committed.
        Path model = Runfile.envList("UNSTRUCTURED_MODEL").get(0).toRealPath();
        Path directory = Objects.requireNonNull(model.getParent(), "model.pure has no parent directory");
        project = directory;
        Path database = directory.resolve("corpus/unstructured.duckdb");
        if (!Files.isRegularFile(database)) {
            throw new IllegalStateException("no corpus database at " + database + ".\n" + REBUILD);
        }
        String text = Files.readString(model);
        String declared = "path: '" + DECLARED_PATH + "'";
        assertTrue(text.contains(declared), "model.pure no longer declares " + declared);
        context = Compiler.compileModel(text.replace(declared, "path: '" + database + "'"));
    }

    private static List<List<Object>> run(String query) throws IOException {
        Collector collector = new Collector();
        ConnectionResolver.RUNNER.run(Compiler.query(context, query), RUNTIME, new Output.Rows(collector));
        return collector.collected().rows();
    }

    private static Map<String, Long> counts(List<List<Object>> rows) {
        Map<String, Long> out = new TreeMap<>();
        for (List<Object> row : rows) {
            out.put((String) row.get(0), ((Number) row.get(1)).longValue());
        }
        return out;
    }

    @Test
    void filingsByForm() throws IOException {
        Map<String, Long> byForm = counts(run("edgar::Filing.all()"
                + "->project(~[form:x|$x.form, accession:x|$x.accession])"
                + "->groupBy(~form, ~n:x|$x.accession:y|$y->count())"));
        System.out.println("filings by form: " + byForm);
        assertEquals(Map.of("10-K", 2L, "10-Q", 3L, "13F-HR", 3L, "8-K", 5L), byForm);
    }

    @Test
    void holdingsValueTotalPer13fEqualsItsCoverPage() throws IOException {
        Map<String, Long> totals = counts(run("edgar::Holding.all()"
                + "->project(~[accession:x|$x.filing.accession, value:x|$x.value])"
                + "->groupBy(~accession, ~total:x|$x.value:y|$y->sum())"));
        assertEquals(3, totals.size(), totals.toString());
        for (Map.Entry<String, Long> total : totals.entrySet()) {
            long cover = coverPageTotal(total.getKey());
            System.out.println(total.getKey() + ": holdings " + total.getValue() + ", cover page " + cover);
            assertEquals(cover, total.getValue(), total.getKey());
        }
    }

    /** One ground-truth table's rows as maps: these CSVs hold ids, numbers and dates, so no field is quoted. */
    private static List<Map<String, String>> groundTruth(String corpus, String table) throws IOException {
        List<String> lines = Files.readAllLines(project.resolve("corpus/" + corpus + "/ground_truth/" + table + ".csv"));
        String[] header = lines.get(0).split(",", -1);
        List<Map<String, String>> rows = new ArrayList<>();
        for (String line : lines.subList(1, lines.size())) {
            assertFalse(line.contains("\""), "a quoted field in " + table + ": " + line);
            String[] values = line.split(",", -1);
            Map<String, String> row = new TreeMap<>();
            for (int i = 0; i < header.length; i++) {
                row.put(header[i], values[i]);
            }
            rows.add(row);
        }
        return rows;
    }

    /** Requests the desk has answered, so the denominator is what it had the chance to fill. The rate is the database's. */
    @Test
    void allocationFillRatePerClient() throws IOException {
        List<List<Object>> rows = run("sales::AllocationRequest.all()"
                + "->filter(r|$r.status != sales::AllocationStatus.Requested)"
                + "->project(~[client:x|$x.client.clientId, requested:x|$x.requestedQtyUsd, allocated:x|$x.allocatedQtyUsd])"
                + "->groupBy(~client, ~[requested:x|$x.requested:y|$y->sum(), allocated:x|$x.allocated:y|$y->sum()])"
                + "->extend(~rate:x|$x.allocated / $x.requested)");
        Map<String, BigDecimal[]> expected = new TreeMap<>();
        for (Map<String, String> row : groundTruth("emails", "SA_ALLOCATION_REQUEST")) {
            if (!"Requested".equals(row.get("STATUS"))) {
                BigDecimal[] sums = expected.computeIfAbsent(row.get("CLIENT_ID"), k -> new BigDecimal[] {BigDecimal.ZERO, BigDecimal.ZERO});
                sums[0] = sums[0].add(new BigDecimal(row.get("REQUESTED_QTY_USD")));
                sums[1] = sums[1].add(new BigDecimal(row.get("ALLOCATED_QTY_USD")));
            }
        }
        assertEquals(expected.size(), rows.size(), rows.toString());
        for (List<Object> row : rows) {
            BigDecimal[] sums = Objects.requireNonNull(expected.get((String) row.get(0)), () -> "no such client: " + row);
            BigDecimal requested = new BigDecimal(row.get(1).toString());
            BigDecimal allocated = new BigDecimal(row.get(2).toString());
            double rate = ((Number) row.get(3)).doubleValue();
            System.out.println(row.get(0) + ": requested " + requested + ", allocated " + allocated + ", fill rate " + rate);
            assertEquals(0, sums[0].compareTo(requested), row.toString());
            assertEquals(0, sums[1].compareTo(allocated), row.toString());
            assertEquals(sums[1].doubleValue() / sums[0].doubleValue(), rate, 1e-9, row.toString());
        }
    }

    @Test
    void covenantsPerAgreementWithTheirStepDowns() throws IOException {
        List<List<Object>> rows = run("lending::Covenant.all()"
                + "->project(~[agreement:x|$x.agreement.agreementId, covenant:x|$x.covenantId, kind:x|$x.kind, clause:x|$x.clauseRef,"
                + " cure:x|$x.equityCureAllowed, from:x|$x.steps.effectiveFrom, level:x|$x.steps.threshold])");
        Map<String, List<Map<String, String>>> stepsByCovenant = new TreeMap<>();
        for (Map<String, String> step : groundTruth("loans", "LN_COVENANT_STEP")) {
            stepsByCovenant.computeIfAbsent(step.get("COVENANT_ID"), k -> new ArrayList<>()).add(step);
        }
        Set<String> expected = new TreeSet<>();
        for (Map<String, String> covenant : groundTruth("loans", "LN_COVENANT")) {
            String head = covenant.get("AGREEMENT_ID") + "|" + covenant.get("COVENANT_ID") + "|" + covenant.get("KIND") + "|"
                    + covenant.get("CLAUSE_REF") + "|" + covenant.get("EQUITY_CURE_ALLOWED");
            List<Map<String, String>> steps = stepsByCovenant.get(covenant.get("COVENANT_ID"));
            if (steps == null) {
                expected.add(head + "|null|null");
            } else {
                for (Map<String, String> step : steps) {
                    expected.add(head + "|" + step.get("EFFECTIVE_FROM") + "|" + new BigDecimal(step.get("THRESHOLD")).stripTrailingZeros().toPlainString());
                }
            }
        }
        Set<String> actual = new TreeSet<>();
        Map<String, Set<String>> covenants = new TreeMap<>();
        Map<String, Set<String>> stepped = new TreeMap<>();
        Map<String, Set<String>> cured = new TreeMap<>();
        for (List<Object> row : rows) {
            String agreement = (String) row.get(0);
            String covenant = (String) row.get(1);
            Object level = row.get(6);
            actual.add(agreement + "|" + covenant + "|" + row.get(2) + "|" + row.get(3) + "|" + row.get(4) + "|" + row.get(5) + "|"
                    + (level == null ? "null" : new BigDecimal(level.toString()).stripTrailingZeros().toPlainString()));
            covenants.computeIfAbsent(agreement, k -> new TreeSet<>()).add(covenant);
            if (level != null) {
                stepped.computeIfAbsent(agreement, k -> new TreeSet<>()).add(covenant);
            }
            if (Boolean.TRUE.equals(row.get(4))) {
                cured.computeIfAbsent(agreement, k -> new TreeSet<>()).add(covenant);
            }
        }
        assertEquals(expected, actual);
        assertEquals(12, covenants.size(), covenants.toString());
        covenants.forEach((agreement, ids) -> {
            assertTrue(ids.size() >= 4 && ids.size() <= 7, agreement + ": " + ids);
            Set<String> withStepDown = Objects.requireNonNull(stepped.get(agreement), agreement + " has no step-down");
            Set<String> withCure = Objects.requireNonNull(cured.get(agreement), agreement + " has no equity cure");
            System.out.println(agreement + ": " + ids.size() + " covenants, " + withStepDown.size() + " with a step-down, "
                    + withCure.size() + " with an equity cure");
        });
    }

    private static long coverPageTotal(String accession) throws IOException {
        try (DirectoryStream<Path> filings = Files.newDirectoryStream(project.resolve("corpus/edgar/13F-HR"), "*_" + accession)) {
            Path filing = filings.iterator().next();
            Matcher matcher = Pattern.compile("(?m)tableValueTotal: (\\d+)$").matcher(Files.readString(filing.resolve("cover.txt")));
            assertTrue(matcher.find(), "no tableValueTotal in " + filing);
            return Long.parseLong(matcher.group(1));
        }
    }

    @Test
    void passagesPerSection() throws IOException {
        Map<String, Long> bySection = counts(run("corpus::Passage.all()"
                + "->filter(p|$p.source == corpus::CorpusSource.EDGAR)"
                + "->project(~[section:x|$x.sectionOrDoc, sha:x|$x.sha256])"
                + "->groupBy(~section, ~n:x|$x.sha:y|$y->count())"));
        bySection.forEach((section, n) -> System.out.println(section + ": " + n + " passages"));
        assertEquals(Set.of("cover", "item_1A", "item_2", "item_2.02", "item_3", "item_7", "item_7A", "item_8.01"), bySection.keySet());
        bySection.forEach((section, n) -> {
            assertFalse(section.contains("/"), "a section must read as a section, not a path: " + section);
            assertTrue(n > 0, section);
        });
    }
}
