// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.Compiler;
import com.legend.result.Collector;
import com.legend.result.Output;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The server fences a model's {@code file:} sources to the folders its startup settings name, none when they name none
 * (the user's ruling of 2026-11-10, the A checkpoint's tenets audit, 11): {@code --sources DIR}, repeatable, else
 * {@code LEGEND_LITE_SOURCE_DIRS}, else no folder -- every {@code file:} read refused by name, saying how to name one;
 * {@code any} widens it to every file. The library's door ({@link ConnectionResolver#RUNNER}) reads any file.
 */
class ServerSourceFenceTest {

    private static final String MODEL = """
            ###Relational
            Database x::DB ( Table T (ID INTEGER PRIMARY KEY, NAME VARCHAR(20)) )
            ###Connection
            RelationalDatabaseConnection x::Conn { store: x::DB; %s }
            ###Runtime
            Runtime x::RT { mappings: [ ]; connections: [ x::DB: [ c: x::Conn ] ]; }
            """.formatted(TestBackends.platform().connectionBody());

    private static final String FROM_JSON = "->extend(~[ID: r|cast(get($r.data, 'ID'), "
            + "@meta::pure::metamodel::type::Integer)])->select(~[ID])->sort(~ID->ascending())->from(x::RT)";

    @Test
    void theStartupSettingsNameTheFence_noneGivenFencesToNoFolder() {
        assertEquals("none", LegendHttpServer.sources(new String[] {"8080"}, null));
        assertEquals("none", LegendHttpServer.sources(new String[0], "  "));
        assertEquals("/a" + File.pathSeparator + "/b",
                LegendHttpServer.sources(new String[] {"--sources", "/a", "9000", "--sources", "/b"}, "/env"));
        assertEquals("/env", LegendHttpServer.sources(new String[] {"--query-store", "/q"}, "/env"));
        assertEquals("any", LegendHttpServer.sources(new String[] {"--sources", "/a", "--sources", "any"}, null));
        assertTrue(LegendHttpServer.USAGE.contains("--sources DIR"), LegendHttpServer.USAGE);
    }

    /** A folder the settings name that is no directory refuses the start, by name, never every query after it (the A
     *  delta audit's 5, 2026-11-11). */
    @Test
    void aFolderThatIsNoDirectoryRefusesTheStart(@TempDir Path dir) throws Exception {
        Path file = Files.writeString(dir.resolve("not-a-folder.txt"), "x");
        var refused = assertThrows(IllegalArgumentException.class, () -> new LegendHttpServer(0,
                java.net.InetAddress.getLoopbackAddress(), LegendHttpServer.Origins.LOOPBACK, null,
                dir + File.pathSeparator + file));
        assertTrue(refused.getMessage().contains("'" + file + "'") && refused.getMessage().contains("does not start"),
                refused.getMessage());
        LegendHttpServer.validateSources(dir.toString());
        LegendHttpServer.validateSources("any");
        LegendHttpServer.validateSources(LegendHttpServer.NO_SOURCES);
    }

    @Test
    void aFileSourceOutsideTheFenceIsRefusedByName_insideItIsRead(@TempDir Path dir) throws Exception {
        Path rows = Files.writeString(dir.resolve("rows.json"), "[{\"ID\":2},{\"ID\":1}]");
        var query = Compiler.query(Compiler.compileModel(MODEL), "|sourceUrl('" + rows.toUri() + "')" + FROM_JSON);

        var refused = assertThrows(Exception.class, () -> ConnectionResolver.runner(LegendHttpServer.NO_SOURCES)
                .run(query, "x::RT", new Output.Rows(new Collector())));
        assertTrue(refused.getMessage().contains("none are configured: start it with --sources DIR"),
                refused.getMessage());

        for (String fence : new String[] {dir.toString(), "any"}) {
            Collector read = new Collector();
            ConnectionResolver.runner(fence).run(query, "x::RT", new Output.Rows(read));
            assertEquals(2, read.collected().rows().size(), fence);
        }
        Collector library = new Collector();
        ConnectionResolver.RUNNER.run(query, "x::RT", new Output.Rows(library));
        assertEquals(2, library.collected().rows().size());
    }
}
