package com.legend.tools.guards;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.testing.Runfile;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.junit.jupiter.api.Test;

/** G17 (Bazel workplan P6-17): no test, in any package, reads a Markdown file (//tools/guards:markdown_inputs). */
class MarkdownInputsTest {

    @Test
    void noMarkdownFileIsATestInput() throws IOException {
        List<String> markdown = Files.readAllLines(Runfile.property("markdown.inputs")).stream()
                .filter(l -> !l.isBlank()).toList();
        assertEquals(List.of(), markdown, "Markdown files some test reads: prose must not change a verdict");
    }
}
