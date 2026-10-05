package com.legend.tools.guards;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.Runfile;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** G17 (Bazel workplan P6-17): no test, in any package, reads a Markdown file of this repository at run time
 *  (every package's guard_markdown report, markdown.bzl). */
class MarkdownInputsTest {

    @Test
    void noMarkdownFileIsATestInput() throws IOException {
        List<String> reports = List.of(System.getenv("MARKDOWN_REPORTS").split(" "));
        assertTrue(reports.size() > 20, "only " + reports.size() + " reports: the guard is not looking");
        List<String> markdown = new ArrayList<>();
        for (String r : reports) {
            if (r.isBlank()) continue;
            Files.readAllLines(Runfile.of(r)).stream().filter(l -> !l.isBlank()).forEach(markdown::add);
        }
        assertEquals(List.of(), markdown, "tests that read a Markdown file: prose must not change a verdict");
    }
}
