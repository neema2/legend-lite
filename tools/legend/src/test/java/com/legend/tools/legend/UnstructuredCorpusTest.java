package com.legend.tools.legend;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.result.Collector;
import com.legend.server.ConnectionResolver;
import com.legend.testing.Runfile;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
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
        ConnectionResolver.RUNNER.run(Compiler.query(context, query), RUNTIME, plan -> collector);
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
