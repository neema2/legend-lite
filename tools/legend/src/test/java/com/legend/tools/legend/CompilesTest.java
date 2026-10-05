package com.legend.tools.legend;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.testing.Runfile;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A legend_library compiles (Bazel workplan P3-23, tools/legend/defs.bzl): its own .pure files and its dependencies'
 * closure, and nothing else, through legend-lite's {@link Compiler#compileModel(List)}. A project that compiles only
 * beside some project it does not declare fails here, which is the undeclared-dependency defect projects/CONTRACT.md
 * exists to catch. Each file is its own source, named by its runfiles path, so an error names the file.
 *
 * <p>A QUARANTINED library (legend_library's {@code quarantine}, a dated row with its finding) is held to its known
 * failure instead: it must still fail with that message, so the day legend-lite is fixed this turns red and the row
 * comes out.
 */
class CompilesTest {

    @Test
    void theLibraryCompilesWithItsDeclaredClosure() throws IOException {
        List<Path> files = Runfile.envList("LEGEND_LIBRARY_SRCS");
        assertFalse(files.isEmpty(), "LEGEND_LIBRARY_SRCS names no files");
        List<Compiler.ModelSource> sources = new ArrayList<>();
        for (Path file : files) {
            sources.add(new Compiler.ModelSource(file.toString(), Files.readString(file, StandardCharsets.UTF_8)));
        }
        String quarantine = System.getenv("LEGEND_LIBRARY_QUARANTINE");
        if (quarantine == null || quarantine.isEmpty()) {
            Compiler.compileModel(sources);
            System.out.println("compiles: " + files.size() + " files");
            return;
        }
        RuntimeException known = assertThrows(RuntimeException.class, () -> Compiler.compileModel(sources),
                "quarantined, but it compiles now: drop its quarantine row (and close the finding)");
        assertTrue(String.valueOf(known.getMessage()).contains(quarantine),
                "quarantined for \"" + quarantine + "\", but it fails otherwise: " + known.getMessage());
        System.out.println("quarantined, and still fails as recorded: " + known.getMessage());
    }
}
